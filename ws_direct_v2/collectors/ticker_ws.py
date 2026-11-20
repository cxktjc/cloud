#!/usr/bin/env python3
"""High-frequency Binance USD-M aggTrade snapshot writer for v9711.

The strategy still consumes one atomic local JSON file. This process replaces
periodic REST ticker probes with Binance public WebSocket aggTrade streams,
subscribes only to the symbols in config/symbols.txt, and publishes a coalesced
snapshot every 100ms by default. Signal deduplicates unchanged price content,
so this does not force a full model evaluation on every heartbeat.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import signal
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import websockets
from websockets.exceptions import ConnectionClosed

from . import binance_network
from v9711_live import config as live_cfg

LOG = logging.getLogger("v9711.ticker_ws")
DEPLOY_ROOT = Path(os.environ.get("V9711_DEPLOY_ROOT", str(Path(__file__).resolve().parents[1]))).expanduser().resolve()
DEFAULT_SYMBOLS = DEPLOY_ROOT / "config" / "symbols.txt"
DEFAULT_OUTPUT = DEPLOY_ROOT / "var" / "binance_ticker_price" / "latest.json"
WS_BASE = "wss://fstream.binance.com/market/stream"
BOOK_WS_BASE = "wss://fstream.binance.com/public/stream"
DEFAULT_WS_BASES = (WS_BASE, BOOK_WS_BASE, "wss://fstream.binance.com/stream")
FIRST_DATA_TIMEOUT = 15.0
MAX_SILENCE_SECONDS = 30.0
STABLE_CONNECTION_SECONDS = 30.0


def endpoint_candidates(base: str, stream_kind='aggTrade') -> tuple[str, ...]:
    """Try alternatives only for Binance production paths, not custom hosts."""
    if base in DEFAULT_WS_BASES:
        if stream_kind == 'bookTicker':
            # Book quotes belong to /public; /market can handshake but never
            # deliver this stream. Do not cycle through tick-only endpoints.
            return (BOOK_WS_BASE,)
        return (base,) + tuple(value for value in DEFAULT_WS_BASES if value != base)
    return (base,)



def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, path)
    finally:
        try:
            os.unlink(name)
        except FileNotFoundError:
            pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_symbols(path: Path) -> list[str]:
    if not path.is_file():
        raise RuntimeError(f"symbols file does not exist: {path}")
    symbols = sorted({line.strip().upper() for line in path.read_text(encoding="utf-8").splitlines()
                      if line.strip() and not line.lstrip().startswith("#")})
    if not symbols:
        raise RuntimeError(f"symbols file is empty: {path}")
    for symbol in symbols:
        # Binance has listed non-ASCII symbol names; preserve them. Only
        # reject characters that would corrupt a combined-stream name.
        if any(ch.isspace() or ch in "/@" for ch in symbol):
            raise ValueError(f"invalid Binance symbol in {path}: {symbol!r}")
    return symbols


def chunks(items: list[str], size: int):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def stream_url(symbols: list[str], base: str, stream_kind='aggTrade') -> str:
    streams = "/".join(f"{symbol.lower()}@{stream_kind}" for symbol in symbols)
    return f"{base}?{urlencode({'streams': streams})}"


class SnapshotState:
    def __init__(self, symbols: list[str], output: Path, heartbeat: float, total_groups: int) -> None:
        self.symbols = tuple(symbols)
        self.allowed = set(symbols)
        self.output = output
        self.heartbeat = max(0.2, float(heartbeat))
        self.total_groups = max(1, int(total_groups))
        self.ready_groups: set[int] = set()
        self.group_errors: dict[int, str] = {}
        self.group_received_mono: dict[int, float] = {}
        self.group_received_epoch: dict[int, float] = {}
        self.prices: dict[str, dict[str, Any]] = {}
        self.dirty = False
        self.sequence = 0
        self.last_publish = 0.0
        self.last_publish_mono = 0.0
        self.last_write_mono = 0.0
        self.last_error = ""
        self.started = time.time()
        self._write("starting", error="")

    def update(self, symbol: str, price: str, exchange_time_ms: int) -> bool:
        symbol = symbol.upper()
        if symbol not in self.allowed:
            return False
        try:
            number = float(price)
            stamp = int(exchange_time_ms)
        except (TypeError, ValueError):
            return False
        if not math.isfinite(number) or number <= 0 or stamp <= 0:
            return False
        previous = self.prices.get(symbol)
        row = {"price": str(price), "exchange_time_ms": stamp}
        if previous == row:
            return True
        self.prices[symbol] = row
        self.dirty = True
        return True

    def _write(self, status: str, *, error: str = "") -> None:
        now = time.time()
        self.last_write_mono = time.monotonic()
        if status == "ok":
            self.last_error = ""
            self.sequence += 1
            # A file heartbeat is not a market-data receipt. Use the oldest
            # group's actual accepted message time; silence cannot renew TTL.
            self.last_publish = min(self.group_received_epoch.values())
            self.last_publish_mono = min(self.group_received_mono.values())
        payload = {
            "schema_version": 1,
            "source": "binance_futures_aggtrade_ws",
            "status": status,
            "sequence": self.sequence,
            "updated_at_utc": utc_now(),
            "last_success_epoch": self.last_publish if self.last_publish > 0 else None,
            "last_success_at_utc": (datetime.fromtimestamp(self.last_publish, timezone.utc).isoformat()
                                     if self.last_publish > 0 else None),
            "interval_seconds": 0.0,
            "consecutive_errors": 0 if status == "ok" else 1,
            "error": error,
            "count": len(self.prices),
            "prices": dict(self.prices),
            "groups_ready": len(self.ready_groups),
            "groups_total": self.total_groups,
            "group_errors": dict(self.group_errors),
        }
        atomic_json(self.output, payload)

    def group_connected(self, group_index: int) -> None:
        # Transport handshake alone does not establish data readiness.
        self.ready_groups.discard(int(group_index))
        self.group_received_mono.pop(int(group_index), None)
        self.group_received_epoch.pop(int(group_index), None)
        self.dirty = True

    def group_received(self, group_index: int) -> None:
        group_index = int(group_index)
        self.ready_groups.add(group_index)
        self.group_errors.pop(group_index, None)
        self.group_received_mono[group_index] = time.monotonic()
        self.group_received_epoch[group_index] = time.time()
        self.dirty = True

    def group_failed(self, group_index: int, message: str) -> None:
        self.ready_groups.discard(int(group_index))
        self.group_errors[int(group_index)] = str(message)[:500]
        self.last_error = self.group_errors[int(group_index)]
        try:
            self._write("error", error=self.last_error)
        except OSError:
            LOG.exception("failed to publish ticker error state")

    def _current_status(self) -> str:
        if not self.prices:
            return "starting"
        if len(self.ready_groups) < self.total_groups or self.group_errors:
            return "error"
        if any(time.monotonic() - receipt > MAX_SILENCE_SECONDS
               for receipt in self.group_received_mono.values()):
            return "error"
        return "ok"

    def publish_if_due(self, force: bool = False) -> bool:
        now = time.monotonic()
        if not force and not self.dirty and now - self.last_write_mono < self.heartbeat:
            return False
        status = self._current_status()
        self._write(status, error=self.last_error)
        self.dirty = False
        return status == "ok"


async def consume_group(
    group: list[str], state: SnapshotState, *, base: str, ping_interval: float,
    ping_timeout: float, read_timeout: float, reconnect_before: float,
    stop: asyncio.Event, group_index: int, stream_kind='aggTrade',
) -> None:
    retry = 1.0
    endpoints = endpoint_candidates(base, stream_kind)
    endpoint_index = 0
    allowed = set(group)
    while not stop.is_set():
        started = time.monotonic()
        selected_base = endpoints[endpoint_index % len(endpoints)]
        received = False
        connected_at = None
        raw_count = valid_count = 0
        try:
            url = stream_url(group, selected_base, stream_kind)
            LOG.info("market stream=%s group=%s connecting symbols=%s endpoint=%s proxy=disabled/direct",
                     stream_kind, group_index, len(group), selected_base)
            async with websockets.connect(
                url, ping_interval=ping_interval, ping_timeout=ping_timeout,
                open_timeout=15, close_timeout=3, max_queue=4096, compression=None,
                **binance_network.websocket_connect_kwargs(),
            ) as socket:
                state.group_connected(group_index)
                LOG.info("market group=%s handshake complete; waiting for first valid %s endpoint=%s proxy=disabled/direct",
                         group_index, stream_kind, selected_base)
                connected_at = time.monotonic()
                last_data_at = connected_at
                valid_count = 0
                raw_count = 0
                rejected = {}
                def reject(reason):
                    rejected[reason] = rejected.get(reason, 0) + 1
                while not stop.is_set():
                    now_mono = time.monotonic()
                    if now_mono - connected_at >= reconnect_before:
                        state.group_failed(group_index, "scheduled reconnect")
                        break
                    deadline = (last_data_at + min(read_timeout, MAX_SILENCE_SECONDS)
                                if received else connected_at + FIRST_DATA_TIMEOUT)
                    remaining = deadline - now_mono
                    if remaining <= 0:
                        raise TimeoutError(f"no valid {stream_kind}; received_messages={raw_count} valid={valid_count} rejected={rejected}")
                    try:
                        if hasattr(asyncio, 'timeout'):
                            # Python 3.11+: avoid creating a separate Task for
                            # every frame on high-rate bookTicker connections.
                            async with asyncio.timeout(remaining):
                                raw = await socket.recv()
                        else:
                            raw = await asyncio.wait_for(socket.recv(), timeout=remaining)
                    except asyncio.TimeoutError as exc:
                        raise TimeoutError(
                            f"no valid {stream_kind}; endpoint={selected_base} received_messages={raw_count} "
                            f"valid={valid_count} rejected={rejected} waiting_for={'next_data' if received else 'first_data'}") from exc
                    wire_ns = time.monotonic_ns()
                    raw_count += 1
                    if raw_count % 64 == 0:
                        # recv() may complete synchronously from a full buffer.
                        # Give other groups, pings and reconnects a fair turn.
                        await asyncio.sleep(0)
                    message = json.loads(raw)
                    if not isinstance(message, dict):
                        raise ValueError("expected JSON object from combined stream")
                    data = message.get("data", message)
                    if "code" in message:
                        raise ValueError(f"subscription error code={message.get('code')} msg={str(message.get('msg'))[:200]}")
                    if not isinstance(data, dict) or data.get("e") != stream_kind:
                        if raw_count == 1:
                            LOG.warning("market stream=%s group=%s unexpected first message keys=%s",
                                        stream_kind, group_index, list(message)[:8])
                        reject('unexpected_event')
                        continue
                    symbol = str(data.get("s", "")).upper()
                    if data.get('st', 1) != 1:
                        reject('non_um_symbol_type')
                        continue
                    try:
                        stamp = int(data.get("T", data.get("E", 0)))
                    except (ValueError, TypeError, OverflowError):
                        reject('invalid_timestamp')
                        continue
                    if symbol not in allowed:
                        reject('unexpected_symbol')
                        continue
                    if not -5 <= time.time() - stamp / 1000 <= live_cfg.PRICE_EXCHANGE_MAX_AGE_SECONDS:
                        reject('stale_or_future_timestamp')
                        continue
                    if hasattr(state, '_last_wire_ns'):
                        # Set immediately before update: another group may
                        # have run during the explicit fairness yield above.
                        state._last_wire_ns = wire_ns
                    try:
                        accepted = (state.update_book(symbol, data.get('b'), data.get('B'), stamp, data.get('u'))
                                    if stream_kind == 'bookTicker' else state.update(symbol, str(data.get('p', '')), stamp))
                    except BufferError as exc:
                        # Local overload is NOT a failed socket. The market
                        # revoked its old generation and blocks until drained.
                        # Reading must continue so TCP/ping buffers can drain.
                        LOG.warning('market stream=%s group=%s local overload; socket kept open: %s',
                                    stream_kind, group_index, exc)
                        last_data_at = time.monotonic()
                        continue
                    if not accepted:
                        reject('invalid_or_duplicate_quote')
                        continue
                    state.group_received(group_index)
                    last_data_at = time.monotonic()
                    valid_count += 1
                    if not received:
                        received = True
                        LOG.info("market stream=%s group=%s DATA READY endpoint=%s first_symbol=%s latency=%.3fs",
                                 stream_kind, group_index, selected_base, symbol, last_data_at - started)
        except asyncio.CancelledError:
            state.group_failed(group_index, "collector stopped")
            raise
        except Exception as exc:
            detail = f"{type(exc).__name__}: {exc}"[:700]
            lifetime = time.monotonic()-connected_at if connected_at is not None else 0.
            if received and lifetime >= STABLE_CONNECTION_SECONDS:
                retry = 1.0
            LOG.warning("market stream=%s group=%s disconnected endpoint=%s proxy=disabled/direct "
                        "lifetime=%.3fs messages=%d valid=%d retry=%.1fs error=%s",
                        stream_kind, group_index, selected_base, lifetime, raw_count, valid_count, retry, detail)
            state.group_failed(group_index, detail)
            if not received:
                endpoint_index += 1
            await asyncio.sleep(retry)
            retry = min(retry * 2.0, 30.0)


async def publish_loop(state: SnapshotState, interval: float, stop: asyncio.Event) -> None:
    interval = max(0.02, float(interval))
    while not stop.is_set():
        state.publish_if_due()
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass
    state.publish_if_due(force=True)


async def run(args: argparse.Namespace) -> int:
    symbols = load_symbols(args.symbols_file)
    if args.max_symbols and len(symbols) > args.max_symbols:
        raise RuntimeError(f"symbols file has {len(symbols)} symbols, exceeds --max-symbols={args.max_symbols}")
    state = SnapshotState(symbols, args.output, args.heartbeat, total_groups=math.ceil(len(symbols) / max(1, int(args.streams_per_connection))))
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            pass
    groups = list(chunks(symbols, max(1, int(args.streams_per_connection))))
    LOG.info("ticker WS source=aggTrade symbols=%s groups=%s publish_interval=%.3fs output=%s",
             len(symbols), len(groups), args.publish_interval, args.output)
    tasks = [asyncio.create_task(consume_group(
        group, state, base=args.ws_base, ping_interval=args.ping_interval,
        ping_timeout=args.ping_timeout, read_timeout=args.read_timeout,
        reconnect_before=args.reconnect_before, stop=stop, group_index=index + 1))
        for index, group in enumerate(groups)]
    tasks.append(asyncio.create_task(publish_loop(state, args.publish_interval, stop)))
    try:
        await stop.wait()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols-file", type=Path, default=Path(os.environ.get("SYMBOLS_FILE", str(DEFAULT_SYMBOLS))))
    parser.add_argument("--output", type=Path, default=Path(os.environ.get("PRICE_FILE", str(live_cfg.PRICE_FILE))))
    parser.add_argument("--ws-base", default=os.environ.get("TICKER_WS_BASE", WS_BASE))
    parser.add_argument("--streams-per-connection", type=int, default=int(os.environ.get("TICKER_STREAMS_PER_CONNECTION", "200")))
    parser.add_argument("--publish-interval", type=float, default=float(os.environ.get("TICKER_PUBLISH_INTERVAL", "0.1")))
    parser.add_argument("--heartbeat", type=float, default=float(os.environ.get("TICKER_HEARTBEAT", "5")))
    parser.add_argument("--ping-interval", type=float, default=float(os.environ.get("TICKER_PING_INTERVAL", "30")))
    parser.add_argument("--ping-timeout", type=float, default=float(os.environ.get("TICKER_PING_TIMEOUT", "20")))
    parser.add_argument("--read-timeout", type=float, default=float(os.environ.get("TICKER_READ_TIMEOUT", "30")))
    parser.add_argument("--reconnect-before", type=float, default=float(os.environ.get("TICKER_RECONNECT_BEFORE", str(23 * 3600))))
    parser.add_argument("--max-symbols", type=int, default=int(os.environ.get("TICKER_MAX_SYMBOLS", "1024")))
    args = parser.parse_args()
    if not 1 <= args.streams_per_connection <= 1024:
        parser.error("--streams-per-connection must be 1..1024")
    if args.publish_interval <= 0 or args.heartbeat <= 0:
        parser.error("publish interval and heartbeat must be positive")
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        return asyncio.run(run(args))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit('WS-direct trade owns the price output; run v9711_live.execution.runner instead')
