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

from . import binance_proxy

LOG = logging.getLogger("v9711.ticker_ws")
DEPLOY_ROOT = Path(os.environ.get("V9711_DEPLOY_ROOT", str(Path(__file__).resolve().parents[1]))).expanduser().resolve()
DEFAULT_SYMBOLS = DEPLOY_ROOT / "config" / "symbols.txt"
DEFAULT_OUTPUT = DEPLOY_ROOT / "var" / "binance_ticker_price" / "latest.json"
WS_BASE = "wss://fstream.binance.com/stream"


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


def stream_url(symbols: list[str], base: str) -> str:
    streams = "/".join(f"{symbol.lower()}@aggTrade" for symbol in symbols)
    return f"{base}?{urlencode({'streams': streams})}"


class SnapshotState:
    def __init__(self, symbols: list[str], output: Path, heartbeat: float) -> None:
        self.symbols = tuple(symbols)
        self.allowed = set(symbols)
        self.output = output
        self.heartbeat = max(0.2, float(heartbeat))
        self.prices: dict[str, dict[str, Any]] = {}
        self.dirty = False
        self.sequence = 0
        self.last_publish = 0.0
        self.last_publish_mono = 0.0
        self.last_error = ""
        self.started = time.time()
        self._write("starting", error="")

    def update(self, symbol: str, price: str, exchange_time_ms: int) -> None:
        symbol = symbol.upper()
        if symbol not in self.allowed:
            return
        try:
            number = float(price)
            stamp = int(exchange_time_ms)
        except (TypeError, ValueError):
            return
        if not math.isfinite(number) or number <= 0 or stamp <= 0:
            return
        previous = self.prices.get(symbol)
        row = {"price": str(price), "exchange_time_ms": stamp}
        if previous == row:
            return
        self.prices[symbol] = row
        self.dirty = True

    def _write(self, status: str, *, error: str = "") -> None:
        now = time.time()
        if status == "ok":
            self.last_error = ""
            self.sequence += 1
            self.last_publish = now
            self.last_publish_mono = time.monotonic()
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
        }
        atomic_json(self.output, payload)

    def publish_if_due(self, force: bool = False) -> bool:
        now = time.monotonic()
        if not force and not self.dirty and now - self.last_publish_mono < self.heartbeat:
            return False
        status = "ok" if self.prices else "starting"
        self._write(status, error=self.last_error)
        self.dirty = False
        return status == "ok"

    def error(self, message: str) -> None:
        self.last_error = str(message)[:500]
        try:
            self._write("error", error=self.last_error)
        except OSError:
            LOG.exception("failed to publish ticker error state")


async def consume_group(
    group: list[str], state: SnapshotState, *, base: str, ping_interval: float,
    ping_timeout: float, read_timeout: float, reconnect_before: float,
    stop: asyncio.Event, group_index: int,
) -> None:
    retry = 1.0
    while not stop.is_set():
        started = time.monotonic()
        try:
            url = stream_url(group, base)
            LOG.info("ticker group=%s connecting symbols=%s", group_index, len(group))
            async with websockets.connect(
                url,
                ping_interval=ping_interval,
                ping_timeout=ping_timeout,
                close_timeout=10,
                **binance_proxy.websocket_connect_kwargs(),
            ) as socket:
                retry = 1.0
                while not stop.is_set():
                    if time.monotonic() - started >= reconnect_before:
                        LOG.info("ticker group=%s reconnecting before connection limit", group_index)
                        break
                    try:
                        raw = await asyncio.wait_for(socket.recv(), timeout=read_timeout)
                    except asyncio.TimeoutError as exc:
                        raise TimeoutError(f"no ticker message for {read_timeout}s") from exc
                    message = json.loads(raw)
                    data = message.get("data", message)
                    if data.get("e") != "aggTrade":
                        continue
                    state.update(str(data.get("s", "")), str(data.get("p", "")), int(data.get("T", data.get("E", 0))))
        except asyncio.CancelledError:
            raise
        except (ConnectionClosed, TimeoutError, OSError, ValueError, json.JSONDecodeError) as exc:
            detail = f"{type(exc).__name__}: {exc}"[:500]
            LOG.warning("ticker group=%s disconnected retry=%.1fs error=%s", group_index, retry, detail)
            state.error(detail)
            await asyncio.sleep(retry)
            retry = min(retry * 2.0, 60.0)
        except Exception as exc:
            LOG.exception("ticker group=%s unexpected error", group_index)
            state.error(f"{type(exc).__name__}: {exc}")
            await asyncio.sleep(retry)
            retry = min(retry * 2.0, 60.0)


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
    state = SnapshotState(symbols, args.output, args.heartbeat)
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
    parser.add_argument("--output", type=Path, default=Path(os.environ.get("PRICE_FILE", str(DEFAULT_OUTPUT))))
    parser.add_argument("--ws-base", default=os.environ.get("TICKER_WS_BASE", WS_BASE))
    parser.add_argument("--streams-per-connection", type=int, default=int(os.environ.get("TICKER_STREAMS_PER_CONNECTION", "200")))
    parser.add_argument("--publish-interval", type=float, default=float(os.environ.get("TICKER_PUBLISH_INTERVAL", "0.1")))
    parser.add_argument("--heartbeat", type=float, default=float(os.environ.get("TICKER_HEARTBEAT", "5")))
    parser.add_argument("--ping-interval", type=float, default=float(os.environ.get("TICKER_PING_INTERVAL", "30")))
    parser.add_argument("--ping-timeout", type=float, default=float(os.environ.get("TICKER_PING_TIMEOUT", "20")))
    parser.add_argument("--read-timeout", type=float, default=float(os.environ.get("TICKER_READ_TIMEOUT", "300")))
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
    raise SystemExit(main())
