#!/usr/bin/env python3
"""Standalone USD-M account-event listener. Never places or cancels orders."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import json
import logging
import math
import os
import random
import re
import sqlite3
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request

from . import binance_network

ROOT = Path(os.environ.get("V9711_DEPLOY_ROOT", str(Path(__file__).resolve().parents[1]))).expanduser().resolve()
LOG = logging.getLogger("binance.account_stream")
WS_BASE = "wss://fstream.binance.com/private/ws"


def now_ms():
    return int(time.time() * 1000)


def number(value, default=0.0):
    try:
        value = float(value)
        return value if math.isfinite(value) else default
    except (TypeError, ValueError):
        return default


def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def atomic_json(path, data):
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2, allow_nan=False)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def read_account_snapshot(directory, max_age_seconds=15):
    """Local reader API. Stale/disconnected/replay snapshots must not drive trades."""
    try:
        payload = json.loads((Path(directory) / "account_snapshot.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RuntimeError("account snapshot missing or invalid") from exc
    status = payload.get("status", {})
    age = now_ms() - int(payload.get("published_at_ms", 0))
    if (not status.get("ready") or not status.get("connected") or status.get("mode") != "live"
            or not 0 <= age <= max_age_seconds * 1000):
        raise RuntimeError("account snapshot is not ready or has expired")
    return payload


def read_events(directory, after_seq=0, limit=500):
    """Read durable event cursor using a read-only SQLite connection."""
    path = (Path(directory) / "events.sqlite3").resolve()
    db = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=5)
    try:
        rows = db.execute("SELECT seq,received_ms,payload FROM events WHERE seq>? ORDER BY seq LIMIT ?",
                          (int(after_seq), min(max(1, int(limit)), 10000))).fetchall()
        return [{"seq": seq, "received_ms": received, "event": json.loads(raw)} for seq, received, raw in rows]
    finally:
        db.close()


def load_env(path):
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def safe_error(exc, secrets=()):
    text = f"{type(exc).__name__}: {exc}"
    for secret in secrets:
        if secret:
            text = text.replace(secret, "***")
    text = re.sub(r"(?i)(signature|listenKey|apiKey)=([^&\s]+)", r"\1=***", text)
    return text[:900]


class InstanceLock:
    def __init__(self, path):
        self.path, self.handle = Path(path), None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+b")
        self.handle.seek(0, 2)
        if not self.handle.tell():
            self.handle.write(b"0")
            self.handle.flush()
        self.handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self.handle.close()
            raise RuntimeError("another listener holds this output directory") from exc
        return self

    def __exit__(self, *_):
        if self.handle:
            if os.name == "nt":
                import msvcrt
                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            self.handle.close()


class AccountStore:
    """Single writer; SQLite WAL is canonical, JSON is a replaceable read cache."""
    def __init__(self, directory, account_id="default", mode="live"):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.directory / "events.sqlite3", timeout=10)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT, fingerprint TEXT UNIQUE,
                received_ms INTEGER, event_type TEXT, event_ms INTEGER,
                transaction_ms INTEGER, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS positions (
                key TEXT PRIMARY KEY, version_ms INTEGER, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS balances (
                key TEXT PRIMARY KEY, version_ms INTEGER, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS orders (
                key TEXT PRIMARY KEY, version_ms INTEGER, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS fills (
                key TEXT PRIMARY KEY, transaction_ms INTEGER, payload TEXT NOT NULL);
        """)
        stored = self._meta("account_id")
        if stored and stored != account_id:
            self.db.close()
            raise ValueError("output directory belongs to another account-id")
        self.status(account_id=account_id, mode=mode, connected=False, ready=False,
                    state="STARTING", started_at_ms=now_ms(), last_error=None,
                    needs_order_history_reconciliation=True)

    def _meta(self, key, default=None):
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def _put_meta(self, key, value):
        self.db.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (key, encoded(value)))

    def status(self, **values):
        with self.db:
            for key, value in values.items():
                self._put_meta(key, value)

    def _merge(self, table, key, version, fields):
        assert table in {"positions", "balances", "orders"}
        row = self.db.execute(f"SELECT version_ms,payload FROM {table} WHERE key=?", (key,)).fetchone()
        if row and row[0] > version:
            return False
        payload = json.loads(row[1]) if row else {}
        if table == "orders" and row:
            # NEW and TRADE can share the same millisecond. Do not regress a
            # filled order when the delayed NEW update arrives afterwards.
            terminal = {"FILLED", "CANCELED", "CANCELLED", "EXPIRED", "REJECTED", "EXPIRED_IN_MATCH"}
            if (number(fields.get("executed_quantity")) < number(payload.get("executed_quantity"))
                    or (payload.get("status") in terminal and fields.get("status") not in terminal)):
                return False
        payload.update(fields)
        self.db.execute(f"INSERT OR REPLACE INTO {table} VALUES (?,?,?)", (key, version, encoded(payload)))
        return True

    def seed(self, positions, balances, snapshot_started_ms):
        """Bootstrap after WS is already receiving; queued deltas replay afterwards.

        snapshot_started_ms is an exchange-clock request fence for symbols absent
        from the full REST result. Per-position updateTime wins over older deltas.
        """
        if not isinstance(positions, list) or not isinstance(balances, list):
            raise ValueError("invalid account bootstrap response")
        with self.db:
            self.db.execute("DELETE FROM positions")
            self.db.execute("DELETE FROM balances")
            self._put_meta("snapshot_fence_ms", snapshot_started_ms)
            for row in positions:
                key = f"{row['symbol']}:{row.get('positionSide', 'BOTH')}"
                version = int(row.get("updateTime", 0) or 0)
                self._merge("positions", key, version, {
                    "symbol": row["symbol"], "position_side": row.get("positionSide", "BOTH"),
                    "quantity": number(row.get("positionAmt")),
                    "entry_price": number(row.get("entryPrice")),
                    "break_even_price": number(row.get("breakEvenPrice")),
                    "unrealized_pnl": number(row.get("unRealizedProfit")),
                    "unrealized_pnl_asof_ms": snapshot_started_ms,
                    "mark_price": number(row.get("markPrice")),
                    "liquidation_price": number(row.get("liquidationPrice")),
                    "mark_price_asof_ms": snapshot_started_ms,
                    "liquidation_price_asof_ms": snapshot_started_ms,
                    "source": "REST_BOOTSTRAP", "updated_at_ms": version,
                })
            for row in balances:
                self._merge("balances", row["asset"], int(row.get("updateTime", 0) or 0), {
                    "asset": row["asset"], "wallet_balance": number(row.get("balance")),
                    "cross_wallet_balance": number(row.get("crossWalletBalance")),
                    "available_balance": number(row.get("availableBalance")),
                    "available_balance_asof_ms": snapshot_started_ms,
                    "updated_at_ms": int(row.get("updateTime", 0) or 0),
                    "source": "REST_BOOTSTRAP",
                })
            self._put_meta("snapshot_at_ms", now_ms())
            self._put_meta("last_snapshot_positions", len(positions))

    def apply(self, event):
        if not isinstance(event, dict) or not event.get("e"):
            raise ValueError("expected a Binance user-data event with e field")
        # listenKeyExpired contains the credential; never persist it.
        event = {k: v for k, v in event.items() if k not in {"listenKey", "apiKey", "signature"}}
        kind = event["e"]
        stamp = int(event.get("T", event.get("E", 0)) or 0)
        raw = encoded(event)
        fingerprint = hashlib.sha256(raw.encode()).hexdigest()
        with self.db:
            cursor = self.db.execute(
                "INSERT OR IGNORE INTO events(fingerprint,received_ms,event_type,event_ms,transaction_ms,payload) VALUES (?,?,?,?,?,?)",
                (fingerprint, now_ms(), kind, int(event.get("E", 0) or 0), stamp, raw))
            if not cursor.rowcount:
                return None
            self._put_meta("last_event_received_ms", now_ms())
            self._put_meta("last_event_type", kind)
            if kind == "ACCOUNT_UPDATE":
                data = event.get("a", {})
                reason = data.get("m", "UNKNOWN")
                fence = self._meta("snapshot_fence_ms", 0)
                for row in data.get("P", []):
                    key = f"{row['s']}:{row.get('ps', 'BOTH')}"
                    if stamp < fence and not self.db.execute("SELECT 1 FROM positions WHERE key=?", (key,)).fetchone():
                        continue
                    self._merge("positions", key, stamp, {
                        "symbol": row["s"], "position_side": row.get("ps", "BOTH"),
                        "quantity": number(row.get("pa")), "entry_price": number(row.get("ep")),
                        "break_even_price": number(row.get("bep")), "unrealized_pnl": number(row.get("up")),
                        "unrealized_pnl_asof_ms": stamp, "margin_type": row.get("mt"),
                        "isolated_wallet": number(row.get("iw")), "updated_at_ms": stamp,
                        "reason": reason, "source": "ACCOUNT_UPDATE",
                    })
                for row in data.get("B", []):
                    self._merge("balances", row["a"], stamp, {
                        "asset": row["a"], "wallet_balance": number(row.get("wb")),
                        "cross_wallet_balance": number(row.get("cw")),
                        "balance_change": number(row.get("bc")), "updated_at_ms": stamp,
                        "reason": reason, "source": "ACCOUNT_UPDATE",
                    })
            elif kind == "ORDER_TRADE_UPDATE":
                row = event.get("o", {})
                client_id = str(row.get("c", ""))
                liquidation = client_id.startswith("autoclose-") or row.get("x") == "CALCULATED"
                adl = client_id.startswith("adl_autoclose")
                order = {
                    "symbol": row.get("s"), "position_side": row.get("ps", "BOTH"),
                    "order_id": row.get("i"), "client_order_id": client_id,
                    "order_side": row.get("S"), "order_type": row.get("o"),
                    "status": row.get("X"), "execution_type": row.get("x"),
                    "original_quantity": number(row.get("q")), "executed_quantity": number(row.get("z")),
                    "last_quantity": number(row.get("l")), "last_price": number(row.get("L")),
                    "average_price": number(row.get("ap")), "trade_id": row.get("t"),
                    "realized_pnl": number(row.get("rp")), "commission": number(row.get("n")),
                    "commission_asset": row.get("N"), "reduce_only": row.get("R"),
                    "close_position": row.get("cp"), "liquidation": liquidation and not adl, "adl": adl,
                    "updated_at_ms": stamp,
                }
                key = f"{row.get('s')}:{row.get('i')}"
                self._merge("orders", key, stamp, order)
                if row.get("x") in {"TRADE", "CALCULATED"} and number(row.get("l")) > 0:
                    trade_id = row.get("t")
                    if trade_id is not None and str(trade_id) != "-1":
                        fill_key = f"{key}:{trade_id}"
                    else:
                        fill_key = f"{key}:{row.get('x')}:{row.get('z')}"
                    self.db.execute("INSERT OR IGNORE INTO fills VALUES (?,?,?)", (fill_key, stamp, encoded(order)))
            elif kind == "listenKeyExpired":
                self._put_meta("ready", False)
                self._put_meta("state", "LISTEN_KEY_EXPIRED")
        return kind

    def export(self):
        meta = {k: json.loads(v) for k, v in self.db.execute("SELECT key,value FROM meta")}
        positions = {key: json.loads(raw) for key, raw in self.db.execute("SELECT key,payload FROM positions")}
        balances = {key: json.loads(raw) for key, raw in self.db.execute("SELECT key,payload FROM balances")}
        active = {key: value for key, value in positions.items() if value["quantity"] != 0}
        seq = self.db.execute("SELECT COALESCE(MAX(seq),0) FROM events").fetchone()[0]
        payload = {
            "schema_version": 1, "published_at_ms": now_ms(), "last_event_seq": seq,
            "status": meta, "positions": active, "balances": balances,
            "position_count": len(active),
            "pnl_note": "unrealized_pnl is event/snapshot-asof, NOT a live mark-to-market feed; missing stream events require REST reconciliation",
        }
        atomic_json(self.directory / "account_snapshot.json", payload)
        return payload

    def close(self):
        self.db.close()


class RestClient:
    # This allowlist deliberately excludes order, cancel, leverage and margin APIs.
    ALLOWED = {("POST", "/fapi/v1/listenKey"), ("PUT", "/fapi/v1/listenKey"),
               ("GET", "/fapi/v1/time"), ("GET", "/fapi/v3/positionRisk"),
               ("GET", "/fapi/v3/balance")}

    def __init__(self, key, secret, base="https://fapi.binance.com", timeout=10):
        self.key, self.secret, self.base, self.timeout = key, secret, base.rstrip("/"), timeout
        self.offset = 0

    def request(self, method, path, signed=False):
        if (method, path) not in self.ALLOWED:
            raise ValueError("listener REST endpoint is not allowed")
        params = {}
        if signed:
            params = {"timestamp": now_ms() + self.offset, "recvWindow": 10000}
            query = urlencode(params)
            params["signature"] = hmac.new(self.secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        url = self.base + path + (("?" + urlencode(params)) if params else "")
        request = Request(url, headers={"X-MBX-APIKEY": self.key}, method=method,
                          data=b"" if method in {"POST", "PUT"} else None)
        try:
            with binance_network.urlopen(request, timeout=self.timeout) as response:
                result = json.loads(response.read())
        except HTTPError as exc:
            try:
                data = json.loads(exc.read())
                code = data.get("code", "unknown")
                msg = str(data.get("msg", ""))[:180]
            except (ValueError, AttributeError):
                code, msg = "non_json", "non-JSON upstream response"
            # Do not include the signed URL or urllib exception repr.
            raise RuntimeError(f"{method} {path} HTTP={exc.code} code={code} msg={msg}") from None
        if isinstance(result, dict) and number(result.get("code"), 0) < 0:
            raise RuntimeError(f"{method} {path} code={result['code']}")
        return result

    def bootstrap(self):
        before = now_ms()
        server = self.request("GET", "/fapi/v1/time")
        self.offset = int(server["serverTime"]) - (before + now_ms()) // 2
        started = now_ms() + self.offset
        positions = self.request("GET", "/fapi/v3/positionRisk", signed=True)
        balances = self.request("GET", "/fapi/v3/balance", signed=True)
        return positions, balances, started


class Listener:
    def __init__(self, rest, store, *, ws_base=WS_BASE, resync_seconds=900, heartbeat_seconds=5):
        self.rest, self.store, self.ws_base = rest, store, ws_base.rstrip("/")
        self.resync_seconds, self.heartbeat_seconds = resync_seconds, heartbeat_seconds
        self.listen_key = ""

    def error(self, exc):
        return safe_error(exc, (self.rest.key, self.rest.secret, self.listen_key))

    async def connection(self):
        from websockets.asyncio.client import connect
        reply = await asyncio.to_thread(self.rest.request, "POST", "/fapi/v1/listenKey")
        self.listen_key = reply.get("listenKey", "")
        if not self.listen_key:
            raise RuntimeError("listenKey creation returned no key")
        async with connect(self.ws_base + "/" + self.listen_key, open_timeout=20,
                           ping_interval=20, ping_timeout=20, close_timeout=5,
                           max_size=2**22, max_queue=1024,
                           **binance_network.websocket_connect_kwargs()) as ws:
            queue = asyncio.Queue(maxsize=20000)

            async def receiver():
                async for raw in ws:
                    queue.put_nowait(json.loads(raw))
                raise ConnectionError("account websocket closed")

            async def keepalive():
                while True:
                    await asyncio.sleep(1800)
                    reply = await asyncio.to_thread(self.rest.request, "PUT", "/fapi/v1/listenKey")
                    if reply.get("listenKey", self.listen_key) != self.listen_key:
                        raise RuntimeError("listenKey changed; reconnecting")
                    LOG.info("listenKey keepalive OK")

            async def consumer():
                # Start receiver BEFORE REST so the startup gap is buffered.
                self.store.status(connected=True, ready=False, state="SYNCING", last_error=None)
                self.store.export()
                positions, balances, started = await asyncio.to_thread(self.rest.bootstrap)
                self.store.seed(positions, balances, started)
                while not queue.empty():
                    self.handle(queue.get_nowait())
                self.store.status(ready=True, state="LIVE", connected_at_ms=now_ms())
                self.store.export()
                LOG.info("account stream ready positions=%d; no trading endpoints enabled", len(positions))
                last_resync = time.monotonic()
                connected = last_resync
                while True:
                    try:
                        event = await asyncio.wait_for(queue.get(), timeout=self.heartbeat_seconds)
                        self.handle(event)
                    except asyncio.TimeoutError:
                        pass
                    self.store.status(heartbeat_at_ms=now_ms())
                    self.store.export()
                    if time.monotonic() - connected > 23 * 3600:
                        raise ConnectionError("scheduled websocket rotation")
                    if self.resync_seconds and time.monotonic() - last_resync >= self.resync_seconds:
                        self.store.status(ready=False, state="RESYNCING")
                        self.store.export()
                        positions, balances, started = await asyncio.to_thread(self.rest.bootstrap)
                        self.store.seed(positions, balances, started)
                        while not queue.empty():
                            self.handle(queue.get_nowait())
                        self.store.status(ready=True, state="LIVE")
                        self.store.export()
                        last_resync = time.monotonic()

            tasks = [asyncio.create_task(fn()) for fn in (receiver, keepalive, consumer)]
            try:
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    task.result()
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    def handle(self, event):
        kind = self.store.apply(event)
        if kind == "listenKeyExpired":
            raise ConnectionError("listenKey expired")
        if kind == "ORDER_TRADE_UPDATE":
            order = event.get("o", {})
            LOG.info("ORDER %s %s %s exec=%s filled=%s avg=%s realized=%s liquidation=%s adl=%s",
                     order.get("s"), order.get("ps"), order.get("X"), order.get("x"),
                     order.get("z"), order.get("ap"), order.get("rp"),
                     str(order.get("c", "")).startswith("autoclose-") or order.get("x") == "CALCULATED",
                     str(order.get("c", "")).startswith("adl_autoclose"))
        elif kind == "ACCOUNT_UPDATE":
            for row in event.get("a", {}).get("P", []):
                LOG.info("POSITION %s %s qty=%s entry=%s upnl_asof=%s reason=%s",
                         row.get("s"), row.get("ps"), row.get("pa"), row.get("ep"), row.get("up"),
                         event.get("a", {}).get("m"))
        elif kind == "MARGIN_CALL":
            LOG.warning("MARGIN_CALL received; margin warning is not proof of liquidation")

    async def run(self):
        failures = 0
        while True:
            self.store.status(connected=False, ready=False, state="CONNECTING")
            self.store.export()
            began = time.monotonic()
            try:
                await self.connection()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                detail = self.error(exc)
                failures = 1 if time.monotonic() - began > 120 else failures + 1
                delay = min(60, 2 ** min(failures, 6)) + random.random()
                self.store.status(connected=False, ready=False, state="RECONNECTING", last_error=detail,
                                  needs_order_history_reconciliation=True)
                self.store.export()
                LOG.warning("account stream disconnected retry_in=%.1fs error=%s", delay, detail)
                await asyncio.sleep(delay)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "var" / "binance_account_stream")
    parser.add_argument("--account-id", default="default", help="local label only, not an API credential")
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument("--rest-base", default="https://fapi.binance.com")
    parser.add_argument("--ws-base", default=WS_BASE)
    parser.add_argument("--resync-seconds", type=int, default=900, help="low-frequency REST reconciliation; 0 disables periodic resync")
    parser.add_argument("--replay", type=Path, help="offline JSONL event replay; no network or API credentials")
    args = parser.parse_args()
    if args.resync_seconds != 0 and args.resync_seconds < 60:
        parser.error("resync-seconds must be 0 or >= 60")
    if args.replay and args.output_dir.resolve() == (ROOT / "var" / "binance_account_stream").resolve():
        parser.error("offline replay needs a separate --output-dir")
    load_env(args.env_file)
    key, secret = os.environ.get("BINANCE_API_KEY", ""), os.environ.get("BINANCE_API_SECRET", "")
    if not args.replay and not (key and secret):
        parser.error("set BINANCE_API_KEY and BINANCE_API_SECRET in environment or --env-file")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)sZ %(levelname)s %(message)s",
                        datefmt="%Y-%m-%dT%H:%M:%S", handlers=[logging.StreamHandler(),
                        RotatingFileHandler(args.output_dir / "listener.log", maxBytes=10_000_000,
                                            backupCount=5, encoding="utf-8")])
    logging.Formatter.converter = time.gmtime
    # Avoid third-party debug logs exposing the private URL / listenKey.
    logging.getLogger("websockets").setLevel(logging.CRITICAL)
    with InstanceLock(args.output_dir / "listener.lock"):
        store = AccountStore(args.output_dir, args.account_id, mode="replay" if args.replay else "live")
        try:
            if args.replay:
                for line in args.replay.read_text(encoding="utf-8-sig").splitlines():
                    if line.strip():
                        store.apply(json.loads(line))
                store.status(state="REPLAY_COMPLETE", ready=False, connected=False)
                store.export()
                LOG.info("offline replay complete")
            else:
                LOG.info("listener starting account=%s network=direct output=%s", args.account_id,
                         args.output_dir)
                asyncio.run(Listener(RestClient(key, secret, args.rest_base), store,
                                     ws_base=args.ws_base, resync_seconds=args.resync_seconds).run())
        except KeyboardInterrupt:
            LOG.info("listener stopped")
        finally:
            try:
                store.status(connected=False, ready=False, state="STOPPED", stopped_at_ms=now_ms())
                store.export()
            finally:
                store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
