#!/usr/bin/env python3
"""
Hybrid startup/process manager for Binance futures klines.

Realtime sources:
  - WebSocket !ticker@arr for 24h top-N ranking
  - WebSocket !contractInfo for contract status / onboard updates

Repair/bootstrap sources:
  - REST exchangeInfo / klines for full snapshot and history backfill

Responsibilities:
  1. Maintain futures_rankings snapshots (top-N, default 50) every hour
  2. Maintain kline_watch from the current top-N set
  3. Backfill/repair klines_3m over a lookback window (default 365d)
  4. Aggregate local klines_3m into klines_15m

The existing binance_futures_3m_ws.py remains the live 3m incremental writer.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import socket
import ssl
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request

try:
    import pymysql
    from pymysql.connections import Connection
    from pymysql.cursors import DictCursor
except ImportError:
    pymysql = None
    Connection = Any  # type: ignore[misc,assignment]
    DictCursor = Any  # type: ignore[misc,assignment]

import websockets
from websockets.exceptions import ConnectionClosed

from . import binance_network
from v9711_live.environment import load_environment
from .binance_futures_3m_ws import KlineRow, TableColumn, build_insert_sql


DEFAULT_BASE_URL = "https://fapi.binance.com"
DEFAULT_WS_BASE_URL = "wss://fstream.binance.com/market/stream"
DEFAULT_MYSQL_HOST = "127.0.0.1"
DEFAULT_MYSQL_PORT = 3306
DEFAULT_MYSQL_USER = "root"
DEFAULT_MYSQL_DATABASE = "binance_futures"
DEFAULT_OUTPUT_DIR = Path("/Users/cxk/Documents/New project")

DEFAULT_TOP_LIMIT = 50
DEFAULT_LOOKBACK_DAYS = 365
DEFAULT_REPAIR_INTERVAL_SECONDS = 180
DEFAULT_SNAPSHOT_INTERVAL_SECONDS = 3600
DEFAULT_WATCH_DAYS = 365
DEFAULT_WORKERS = 5
DEFAULT_SYMBOL_FRESHNESS_HOURS = 24.0
DEFAULT_ANCHOR_SYMBOL = "BTCUSDT"
DEFAULT_AGGREGATION_LAG_SECONDS = 10 * 60

REQUEST_TIMEOUT_SECONDS = 30
DEFAULT_REST_RETRIES = 5
DEFAULT_RETRY_DELAY_SECONDS = 2.0
KLINE_MAX_LIMIT = 1500
DEFAULT_REQUEST_DELAY_SECONDS = 0.12
DEFAULT_REST_LAG_SECONDS = 8 * 60
WS_RECONNECT_DELAY_SECONDS = 3

INTERVAL_3M = "3m"
INTERVAL_3M_MS = 3 * 60 * 1000
INTERVAL_15M_MS = 15 * 60 * 1000
CANDLES_PER_15M = 5

WATCH_TABLE = "kline_watch"
KLINES_3M_TABLE = "klines_3m"
KLINES_15M_TABLE = "klines_15m"

PINNED_SYMBOLS = ["BTCUSDT"]
PINNED_DAYS = 36500
NEW_SYMBOL_LOG = DEFAULT_OUTPUT_DIR / "new_symbols.log"

WS_TICKER_STREAM = "!ticker@arr"
WS_CONTRACT_STREAM = "!contractInfo"

LOG = logging.getLogger("kline_startup_manager")
ASYNCIO_LOG = logging.getLogger("asyncio")


class AsyncioWebsocketsNoiseFilter(logging.Filter):
    """Hide a websockets 15 callback bug that is already handled by reconnect."""

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        if (
            record.name == "asyncio"
            and "Exception in callback Connection.connection_lost" in message
            and record.exc_info
            and "ClientConnection' object has no attribute 'recv_messages'" in logging.Formatter().formatException(record.exc_info)
        ):
            return False
        return True


def ws_connect(url: str):
    """
    Use websockets' asyncio client for these long-lived public streams.

    The deployment uses direct HTTPS/WSS connections for both streams and REST
    repair requests; no proxy or local forwarding port is configured.
    """
    return websockets.connect(
        url,
        ping_interval=20,
        ping_timeout=20,
        open_timeout=60,
        close_timeout=10,
        max_queue=1024,
        **binance_network.websocket_connect_kwargs(),
    )


WATCH_SCHEMA = """
CREATE TABLE IF NOT EXISTS kline_watch (
    symbol      VARCHAR(30)  NOT NULL,
    added_at    DATETIME(3)  NOT NULL,
    last_seen   DATETIME(3)  NOT NULL,
    expires_at  DATETIME(3)  NOT NULL,
    appearances INT UNSIGNED NOT NULL DEFAULT 1,
    PRIMARY KEY (symbol)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


SNAPSHOTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshots (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    collected_at_utc DATETIME(3) NOT NULL,
    source VARCHAR(255) NOT NULL,
    interval_seconds INT UNSIGNED NOT NULL,
    total_active_symbols INT UNSIGNED NOT NULL,
    inserted_rows INT UNSIGNED NOT NULL,
    PRIMARY KEY (id),
    KEY idx_snapshots_collected_at (collected_at_utc)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


FUTURES_RANKINGS_SCHEMA = """
CREATE TABLE IF NOT EXISTS futures_rankings (
    collected_at_utc DATETIME(3) NOT NULL,
    snapshot_id BIGINT UNSIGNED NOT NULL,
    rank_no INT UNSIGNED NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    pair VARCHAR(32) NOT NULL,
    base_asset VARCHAR(32) NOT NULL,
    quote_asset VARCHAR(32) NOT NULL,
    margin_asset VARCHAR(32) NOT NULL,
    last_price DECIMAL(38, 16) NOT NULL,
    price_change_percent DECIMAL(18, 8) NOT NULL,
    high_price_24h DECIMAL(38, 16) NOT NULL,
    low_price_24h DECIMAL(38, 16) NOT NULL,
    volume_24h DECIMAL(38, 8) NOT NULL,
    quote_volume_24h DECIMAL(38, 8) NOT NULL,
    open_time_ms BIGINT,
    close_time_ms BIGINT,
    trade_count BIGINT,
    PRIMARY KEY (collected_at_utc, symbol),
    UNIQUE KEY uq_futures_rankings_time_rank (collected_at_utc, rank_no),
    KEY idx_futures_rankings_symbol_time (symbol, collected_at_utc),
    KEY idx_futures_rankings_snapshot_rank (snapshot_id, rank_no),
    CONSTRAINT fk_futures_rankings_snapshot
        FOREIGN KEY (snapshot_id) REFERENCES snapshots(id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


LATEST_TOP_VIEW_SCHEMA = """
CREATE OR REPLACE VIEW latest_top30 AS
    SELECT r.*
    FROM futures_rankings r
    WHERE r.collected_at_utc = (
        SELECT MAX(collected_at_utc) FROM futures_rankings
    )
    ORDER BY r.rank_no ASC
"""


KLINES_15M_SCHEMA = """
CREATE TABLE IF NOT EXISTS klines_15m (
    symbol                  VARCHAR(32)     NOT NULL,
    open_time_ms            BIGINT          NOT NULL,
    open_time_utc           DATETIME(3)     NOT NULL,
    close_time_ms           BIGINT          NOT NULL,
    close_time_utc          DATETIME(3)     NOT NULL,
    open_price              DECIMAL(38, 16) NOT NULL,
    high_price              DECIMAL(38, 16) NOT NULL,
    low_price               DECIMAL(38, 16) NOT NULL,
    close_price             DECIMAL(38, 16) NOT NULL,
    volume                  DECIMAL(38, 8)  NOT NULL,
    quote_volume            DECIMAL(38, 8)  NOT NULL,
    trade_count             BIGINT          NOT NULL,
    taker_buy_volume        DECIMAL(38, 8)  NOT NULL,
    taker_buy_quote_volume  DECIMAL(38, 8)  NOT NULL,
    is_closed               TINYINT(1)      NOT NULL DEFAULT 1,
    PRIMARY KEY (symbol, open_time_ms),
    KEY idx_klines_15m_symbol_open (symbol, open_time_utc)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


@dataclass
class Contract:
    symbol: str
    pair: str
    base_asset: str
    quote_asset: str
    margin_asset: str
    onboard_ms: int | None
    status: str
    contract_type: str


@dataclass
class Ticker24h:
    symbol: str
    last_price: Decimal
    price_change_percent: Decimal
    high_price_24h: Decimal
    low_price_24h: Decimal
    volume_24h: Decimal
    quote_volume_24h: Decimal
    open_time_ms: int | None
    close_time_ms: int | None
    trade_count: int | None


@dataclass
class CoverageStats:
    min_ms: int | None
    max_ms: int | None
    count: int


@dataclass(frozen=True)
class SymbolRepairResult:
    symbol: str
    expected_start_ms: int
    onboard_ms: int | None
    gap_count: int
    backfilled_rows: int
    aggregated_rows: int


class SharedState:
    def __init__(self, top_limit: int) -> None:
        self.top_limit = top_limit
        self.lock = asyncio.Lock()
        self.contracts: dict[str, Contract] = {}
        self.tickers: dict[str, Ticker24h] = {}
        self.current_top_symbols: list[str] = []
        self.top_changed_event = asyncio.Event()

    def _eligible_contracts(self) -> set[str]:
        return {
            symbol
            for symbol, contract in self.contracts.items()
            if contract.contract_type == "PERPETUAL"
            and contract.quote_asset == "USDT"
            and contract.margin_asset == "USDT"
            and contract.status == "TRADING"
        }

    def _recompute_top(self) -> list[str]:
        eligible = self._eligible_contracts()
        ranked = sorted(
            (ticker for symbol, ticker in self.tickers.items() if symbol in eligible),
            key=lambda item: item.price_change_percent,
            reverse=True,
        )
        return [row.symbol for row in ranked[: self.top_limit]]

    async def bootstrap_from_rest(self, base_url: str) -> None:
        payload = fetch_json(base_url, "/fapi/v1/exchangeInfo")
        contracts: dict[str, Contract] = {}
        for row in payload.get("symbols", []):
            symbol = str(row.get("symbol", "")).upper()
            if not symbol:
                continue
            contracts[symbol] = Contract(
                symbol=symbol,
                pair=str(row.get("pair", symbol)),
                base_asset=str(row.get("baseAsset", "")),
                quote_asset=str(row.get("quoteAsset", "")),
                margin_asset=str(row.get("marginAsset", "")),
                onboard_ms=int(row["onboardDate"]) if row.get("onboardDate") is not None else None,
                status=str(row.get("status", "")),
                contract_type=str(row.get("contractType", "")),
            )
        ticker_payload = fetch_json(base_url, "/fapi/v1/ticker/24hr")
        tickers: dict[str, Ticker24h] = {}
        for row in ticker_payload:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol", "")).upper()
            if symbol not in contracts:
                continue
            tickers[symbol] = Ticker24h(
                symbol=symbol,
                last_price=to_decimal(row.get("lastPrice")),
                price_change_percent=to_decimal(row.get("priceChangePercent")),
                high_price_24h=to_decimal(row.get("highPrice")),
                low_price_24h=to_decimal(row.get("lowPrice")),
                volume_24h=to_decimal(row.get("volume")),
                quote_volume_24h=to_decimal(row.get("quoteVolume")),
                open_time_ms=to_optional_int(row.get("openTime")),
                close_time_ms=to_optional_int(row.get("closeTime")),
                trade_count=to_optional_int(row.get("count")),
            )
        async with self.lock:
            self.contracts = contracts
            self.tickers = tickers
            self.current_top_symbols = self._recompute_top()

    async def apply_ticker_rows(self, rows: list[dict[str, Any]]) -> None:
        changed = False
        async with self.lock:
            for row in rows:
                symbol = str(row.get("s", "")).upper()
                if not symbol:
                    continue
                self.tickers[symbol] = Ticker24h(
                    symbol=symbol,
                    last_price=to_decimal(row.get("c")),
                    price_change_percent=to_decimal(row.get("P")),
                    high_price_24h=to_decimal(row.get("h")),
                    low_price_24h=to_decimal(row.get("l")),
                    volume_24h=to_decimal(row.get("v")),
                    quote_volume_24h=to_decimal(row.get("q")),
                    open_time_ms=to_optional_int(row.get("O")),
                    close_time_ms=to_optional_int(row.get("C")),
                    trade_count=to_optional_int(row.get("n")),
                )
            new_top = self._recompute_top()
            if new_top != self.current_top_symbols:
                self.current_top_symbols = new_top
                changed = True
        if changed:
            self.top_changed_event.set()

    async def apply_contract_row(self, row: dict[str, Any]) -> None:
        symbol = str(row.get("s", "")).upper()
        if not symbol:
            return
        changed = False
        async with self.lock:
            previous = self.contracts.get(symbol)
            next_contract = Contract(
                symbol=symbol,
                pair=previous.pair if previous else symbol,
                base_asset=previous.base_asset if previous else "",
                quote_asset=previous.quote_asset if previous else "USDT",
                margin_asset=previous.margin_asset if previous else "USDT",
                onboard_ms=to_optional_int(row.get("ot")) or (previous.onboard_ms if previous else None),
                status=str(row.get("cs") or (previous.status if previous else "")),
                contract_type=previous.contract_type if previous else "PERPETUAL",
            )
            self.contracts[symbol] = next_contract
            new_top = self._recompute_top()
            if new_top != self.current_top_symbols:
                self.current_top_symbols = new_top
                changed = True
        if changed:
            self.top_changed_event.set()

    async def snapshot_top(self) -> tuple[list[str], dict[str, Contract], dict[str, Ticker24h]]:
        async with self.lock:
            symbols = list(self.current_top_symbols)
            contracts = {symbol: self.contracts[symbol] for symbol in symbols if symbol in self.contracts}
            tickers = {symbol: self.tickers[symbol] for symbol in symbols if symbol in self.tickers}
        return symbols, contracts, tickers

    async def current_onboard_ms(self, symbol: str) -> int | None:
        async with self.lock:
            contract = self.contracts.get(symbol)
            return contract.onboard_ms if contract else None


def require_pymysql() -> None:
    if pymysql is None:
        print("Missing dependency: pymysql. Run: python -m pip install pymysql", file=sys.stderr)
        raise SystemExit(2)


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def utc_iso(dt: datetime | None = None) -> str:
    dt = dt or utc_now()
    return dt.replace(tzinfo=timezone.utc).isoformat(timespec="seconds")


def dt_to_ms(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(tzinfo=None)


def ms_to_text(ms: int | None) -> str:
    if ms is None:
        return "-"
    return ms_to_dt(ms).strftime("%Y-%m-%d %H:%M:%S")


def to_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("-Infinity")


def to_decimal_zero(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


def to_optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


class RequestLimiter:
    def __init__(
        self,
        min_interval_seconds: float,
        max_retries: int = DEFAULT_REST_RETRIES,
        retry_delay: float = DEFAULT_RETRY_DELAY_SECONDS,
    ) -> None:
        self.min_interval_seconds = max(0.0, min_interval_seconds)
        self.max_retries = max(0, max_retries)
        self.retry_delay = max(0.0, retry_delay)
        self._lock = threading.Lock()
        self._next_allowed = 0.0

    def wait(self) -> None:
        if self.min_interval_seconds <= 0:
            return
        with self._lock:
            now = time.monotonic()
            wait_seconds = max(0.0, self._next_allowed - now)
            self._next_allowed = max(now, self._next_allowed) + self.min_interval_seconds
        if wait_seconds > 0:
            time.sleep(wait_seconds)


def floor_interval(ms: int, interval_ms: int) -> int:
    return (ms // interval_ms) * interval_ms


def ceil_interval(ms: int, interval_ms: int) -> int:
    return ((ms + interval_ms - 1) // interval_ms) * interval_ms


def is_retryable_http_error(exc: HTTPError) -> bool:
    return exc.code in {408, 418, 425, 429, 500, 502, 503, 504}


def retry_sleep_seconds(attempt: int, base_delay: float) -> float:
    return min(60.0, max(0.0, base_delay) * (2 ** max(0, attempt - 1)))


def fetch_json(
    base_url: str,
    path: str,
    params: dict[str, Any] | None = None,
    limiter: RequestLimiter | None = None,
    max_retries: int = DEFAULT_REST_RETRIES,
    retry_delay: float = DEFAULT_RETRY_DELAY_SECONDS,
) -> Any:
    if limiter is not None:
        max_retries = limiter.max_retries
        retry_delay = limiter.retry_delay
    url = f"{base_url.rstrip('/')}{path}"
    if params:
        url = f"{url}?{urlencode(params)}"
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "binance-hybrid-kline-manager/1.0",
        },
    )
    for attempt in range(1, max_retries + 2):
        if limiter is not None:
            limiter.wait()
        try:
            with binance_network.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                return json.load(response)
        except HTTPError as exc:
            if not is_retryable_http_error(exc) or attempt > max_retries:
                raise
            wait_seconds = retry_sleep_seconds(attempt, retry_delay)
            LOG.warning(
                "REST %s failed HTTP %s; retrying in %.1fs (%s/%s)",
                path,
                exc.code,
                wait_seconds,
                attempt,
                max_retries,
            )
            time.sleep(wait_seconds)
        except (URLError, socket.timeout, TimeoutError, ssl.SSLError) as exc:
            if attempt > max_retries:
                raise
            wait_seconds = retry_sleep_seconds(attempt, retry_delay)
            LOG.warning(
                "REST %s failed: %s; retrying in %.1fs (%s/%s)",
                path,
                exc,
                wait_seconds,
                attempt,
                max_retries,
            )
            time.sleep(wait_seconds)
    raise RuntimeError(f"REST request failed after retries: {url}")


def connect_mysql(args: argparse.Namespace, ensure_schema_objects: bool = True) -> Connection:
    require_pymysql()
    connection = pymysql.connect(
        host=args.mysql_host,
        port=args.mysql_port,
        user=args.mysql_user,
        password=args.mysql_password,
        database=args.mysql_database,
        charset="utf8mb4",
        cursorclass=DictCursor,
        autocommit=False,
    )
    if ensure_schema_objects:
        ensure_schema(connection)
    return connection


def ensure_schema(connection: Connection) -> None:
    statements = [
        WATCH_SCHEMA,
        SNAPSHOTS_SCHEMA,
        FUTURES_RANKINGS_SCHEMA,
        LATEST_TOP_VIEW_SCHEMA,
        KLINES_15M_SCHEMA,
    ]
    with connection.cursor() as cursor:
        for statement in statements:
            cursor.execute(statement)
    connection.commit()


def close_quietly(connection: Connection | None) -> None:
    if connection is None:
        return
    try:
        connection.close()
    except Exception:
        pass


def load_3m_columns(connection: Connection, database: str, table: str) -> dict[str, TableColumn]:
    def value(row: Any, key: str, index: int) -> Any:
        if isinstance(row, dict):
            return row[key]
        return row[index]

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
            """,
            (database, table),
        )
        rows = cursor.fetchall()
    if not rows:
        raise RuntimeError(f"MySQL table {database}.{table} does not exist")
    return {
        value(row, "COLUMN_NAME", 0): TableColumn(
            name=value(row, "COLUMN_NAME", 0),
            data_type=str(value(row, "DATA_TYPE", 1)).lower(),
            nullable=value(row, "IS_NULLABLE", 2) == "YES",
            has_default=value(row, "COLUMN_DEFAULT", 3) is not None,
            extra=str(value(row, "EXTRA", 4) or "").lower(),
        )
        for row in rows
    }


def expire_watch(connection: Connection) -> int:
    now = utc_now()
    with connection.cursor() as cursor:
        cursor.execute(f"DELETE FROM {WATCH_TABLE} WHERE expires_at < %s", (now,))
        deleted = cursor.rowcount
    connection.commit()
    return deleted


def upsert_watch(connection: Connection, symbols: list[str], days: int) -> set[str]:
    now = utc_now()
    expires = now + timedelta(days=days)
    new_entries: set[str] = set()
    with connection.cursor() as cursor:
        for symbol in symbols:
            cursor.execute(f"SELECT 1 FROM {WATCH_TABLE} WHERE symbol = %s", (symbol,))
            if cursor.fetchone() is None:
                new_entries.add(symbol)
            cursor.execute(
                f"""
                INSERT INTO {WATCH_TABLE}
                    (symbol, added_at, last_seen, expires_at, appearances)
                VALUES (%s, %s, %s, %s, 1)
                ON DUPLICATE KEY UPDATE
                    last_seen = VALUES(last_seen),
                    expires_at = VALUES(expires_at),
                    appearances = appearances + 1
                """,
                (symbol, now, now, expires),
            )
    connection.commit()
    return new_entries


def get_watch_rows(connection: Connection) -> list[dict[str, Any]]:
    now = utc_now()
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT symbol, added_at, last_seen, expires_at, appearances
            FROM {WATCH_TABLE}
            WHERE expires_at >= %s
            ORDER BY last_seen DESC, symbol ASC
            """,
            (now,),
        )
        return list(cursor.fetchall())


def get_all_3m_symbols(connection: Connection) -> list[str]:
    queries = [
        f"""
        SELECT DISTINCT symbol
        FROM {KLINES_3M_TABLE}
        WHERE symbol IS NOT NULL AND symbol <> ''
        """,
        f"""
        SELECT DISTINCT symbol
        FROM {KLINES_15M_TABLE}
        WHERE symbol IS NOT NULL AND symbol <> ''
        """,
        f"""
        SELECT DISTINCT symbol
        FROM {WATCH_TABLE}
        WHERE symbol IS NOT NULL AND symbol <> ''
          AND expires_at >= UTC_TIMESTAMP()
        """,
        """
        SELECT DISTINCT symbol
        FROM futures_rankings
        WHERE symbol IS NOT NULL AND symbol <> ''
        """,
    ]
    symbols: set[str] = set()
    with connection.cursor() as cursor:
        for query in queries:
            cursor.execute(query)
            for row in cursor.fetchall():
                symbol = str(row["symbol"]).strip().upper()
                if symbol:
                    symbols.add(symbol)
    return sorted(symbols)


def get_latest_closed_3m_ms(connection: Connection, anchor_symbol: str = DEFAULT_ANCHOR_SYMBOL) -> int | None:
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT open_time_ms
            FROM {KLINES_3M_TABLE}
            WHERE symbol = %s
              AND is_closed = 1
            ORDER BY open_time_ms DESC
            LIMIT 1
            """,
            (anchor_symbol.upper(),),
        )
        row = cursor.fetchone()
    return int(row["open_time_ms"]) if row and row["open_time_ms"] is not None else None


def filter_symbols_with_recent_3m_bars(connection: Connection, symbols: list[str], cutoff_ms: int) -> list[str]:
    if not symbols:
        return []
    placeholders = ",".join(["%s"] * len(symbols))
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT symbol
            FROM {KLINES_3M_TABLE}
            WHERE symbol IN ({placeholders})
              AND open_time_ms >= %s
              AND is_closed = 1
            GROUP BY symbol
            ORDER BY symbol ASC
            """,
            symbols + [cutoff_ms],
        )
        rows = cursor.fetchall()
    return [str(row["symbol"]).strip().upper() for row in rows if str(row["symbol"]).strip()]


def latest_ready_15m_from_3m(latest_3m_ms: int) -> int:
    return floor_interval(latest_3m_ms - (CANDLES_PER_15M - 1) * INTERVAL_3M_MS, INTERVAL_15M_MS)


def resolve_repair_symbols(connection: Connection, args: argparse.Namespace) -> list[str]:
    if args.symbols:
        return sorted({symbol.strip().upper() for symbol in args.symbols if symbol.strip()})

    symbols = get_all_3m_symbols(connection)
    for symbol in PINNED_SYMBOLS:
        if symbol not in symbols:
            symbols.append(symbol)

    latest_3m_ms = get_latest_closed_3m_ms(connection, args.anchor_symbol)
    if latest_3m_ms is None:
        return sorted(symbols)
    cutoff_ms = latest_3m_ms - int(args.symbol_freshness_hours * 60 * 60 * 1000)
    fresh_symbols = filter_symbols_with_recent_3m_bars(connection, sorted(symbols), cutoff_ms)
    for symbol in PINNED_SYMBOLS:
        if symbol not in fresh_symbols:
            fresh_symbols.append(symbol)
    if len(fresh_symbols) != len(symbols):
        LOG.info(
            "filtered repair symbols by recent 3m activity: before=%s after=%s anchor=%s latest_3m=%s freshness_hours=%s",
            len(symbols),
            len(fresh_symbols),
            args.anchor_symbol.upper(),
            ms_to_text(latest_3m_ms),
            args.symbol_freshness_hours,
        )
    return sorted(fresh_symbols)


def log_new_symbols(symbols: set[str]) -> None:
    if not symbols:
        return
    NEW_SYMBOL_LOG.parent.mkdir(parents=True, exist_ok=True)
    now = utc_now().strftime("%Y-%m-%d %H:%M:%S")
    with NEW_SYMBOL_LOG.open("a", encoding="utf-8") as handle:
        for symbol in sorted(symbols):
            handle.write(f"{now}  {symbol}\n")


def insert_snapshot(
    connection: Connection,
    contracts: dict[str, Contract],
    tickers: dict[str, Ticker24h],
    symbols: list[str],
    source: str,
    interval_seconds: int,
) -> int:
    collected_at = utc_now()
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO snapshots (
                collected_at_utc,
                source,
                interval_seconds,
                total_active_symbols,
                inserted_rows
            )
            VALUES (%s, %s, %s, %s, %s)
            """,
            (collected_at, source, interval_seconds, len(contracts), len(symbols)),
        )
        snapshot_id = int(cursor.lastrowid)

        rows: list[tuple[Any, ...]] = []
        for rank, symbol in enumerate(symbols, start=1):
            contract = contracts[symbol]
            ticker = tickers[symbol]
            rows.append(
                (
                    collected_at,
                    snapshot_id,
                    rank,
                    symbol,
                    contract.pair,
                    contract.base_asset,
                    contract.quote_asset,
                    contract.margin_asset,
                    ticker.last_price,
                    ticker.price_change_percent,
                    ticker.high_price_24h,
                    ticker.low_price_24h,
                    ticker.volume_24h,
                    ticker.quote_volume_24h,
                    ticker.open_time_ms,
                    ticker.close_time_ms,
                    ticker.trade_count,
                )
            )

        cursor.executemany(
            """
            INSERT INTO futures_rankings (
                collected_at_utc,
                snapshot_id,
                rank_no,
                symbol,
                pair,
                base_asset,
                quote_asset,
                margin_asset,
                last_price,
                price_change_percent,
                high_price_24h,
                low_price_24h,
                volume_24h,
                quote_volume_24h,
                open_time_ms,
                close_time_ms,
                trade_count
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                snapshot_id = VALUES(snapshot_id),
                rank_no = VALUES(rank_no),
                pair = VALUES(pair),
                base_asset = VALUES(base_asset),
                quote_asset = VALUES(quote_asset),
                margin_asset = VALUES(margin_asset),
                last_price = VALUES(last_price),
                price_change_percent = VALUES(price_change_percent),
                high_price_24h = VALUES(high_price_24h),
                low_price_24h = VALUES(low_price_24h),
                volume_24h = VALUES(volume_24h),
                quote_volume_24h = VALUES(quote_volume_24h),
                open_time_ms = VALUES(open_time_ms),
                close_time_ms = VALUES(close_time_ms),
                trade_count = VALUES(trade_count)
            """,
            rows,
        )
    connection.commit()
    return snapshot_id


def fetch_coverage_stats(
    connection: Connection,
    table: str,
    symbol: str,
    start_ms: int,
    end_ms: int,
) -> CoverageStats:
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT MIN(open_time_ms) AS min_ms,
                   MAX(open_time_ms) AS max_ms,
                   COUNT(*) AS cnt
            FROM {table}
            WHERE symbol = %s
              AND open_time_ms >= %s
              AND open_time_ms <= %s
            """,
            (symbol, start_ms, end_ms),
        )
        row = cursor.fetchone()
    return CoverageStats(
        min_ms=int(row["min_ms"]) if row and row["min_ms"] is not None else None,
        max_ms=int(row["max_ms"]) if row and row["max_ms"] is not None else None,
        count=int(row["cnt"] or 0) if row else 0,
    )


def expected_candle_count(start_ms: int, end_ms: int, interval_ms: int) -> int:
    if end_ms < start_ms:
        return 0
    return ((end_ms - start_ms) // interval_ms) + 1


def fetch_internal_gaps(
    connection: Connection,
    table: str,
    symbol: str,
    start_ms: int,
    end_ms: int,
    interval_ms: int,
) -> list[tuple[int, int]]:
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            WITH ordered AS (
                SELECT
                    open_time_ms,
                    LAG(open_time_ms) OVER (ORDER BY open_time_ms) AS prev_open_time_ms
                FROM {table}
                WHERE symbol = %s
                  AND open_time_ms >= %s
                  AND open_time_ms <= %s
            )
            SELECT
                prev_open_time_ms + %s AS gap_start,
                open_time_ms - %s AS gap_end
            FROM ordered
            WHERE prev_open_time_ms IS NOT NULL
              AND open_time_ms - prev_open_time_ms > %s
            ORDER BY gap_start ASC
            """,
            (symbol, start_ms, end_ms, interval_ms, interval_ms, interval_ms),
        )
        rows = cursor.fetchall()
    return [(int(row["gap_start"]), int(row["gap_end"])) for row in rows]


def build_missing_segments(
    connection: Connection,
    table: str,
    symbol: str,
    start_ms: int,
    end_ms: int,
    interval_ms: int,
) -> tuple[list[tuple[int, int]], CoverageStats]:
    stats = fetch_coverage_stats(connection, table, symbol, start_ms, end_ms)
    expected = expected_candle_count(start_ms, end_ms, interval_ms)
    if expected == 0:
        return [], stats
    if stats.count == 0:
        return [(start_ms, end_ms)], stats

    segments: list[tuple[int, int]] = []
    if stats.min_ms is not None and stats.min_ms > start_ms:
        segments.append((start_ms, stats.min_ms - interval_ms))
    if stats.max_ms is not None and stats.max_ms < end_ms:
        segments.append((stats.max_ms + interval_ms, end_ms))
    if stats.count != expected:
        try:
            segments.extend(fetch_internal_gaps(connection, table, symbol, start_ms, end_ms, interval_ms))
        except Exception:
            if not segments:
                segments = [(start_ms, end_ms)]

    normalized: list[tuple[int, int]] = []
    for seg_start, seg_end in sorted(segments):
        if seg_end < seg_start:
            continue
        if not normalized or seg_start > normalized[-1][1] + interval_ms:
            normalized.append((seg_start, seg_end))
        else:
            normalized[-1] = (normalized[-1][0], max(normalized[-1][1], seg_end))
    return normalized, stats


def fetch_klines_range(
    base_url: str,
    symbol: str,
    interval: str,
    interval_ms: int,
    start_ms: int,
    end_ms: int,
    limiter: RequestLimiter | None = None,
) -> list[list[Any]]:
    rows: list[list[Any]] = []
    cursor_ms = start_ms
    while cursor_ms <= end_ms:
        batch = fetch_json(
            base_url,
            "/fapi/v1/klines",
            params={
                "symbol": symbol,
                "interval": interval,
                "startTime": cursor_ms,
                "endTime": end_ms,
                "limit": KLINE_MAX_LIMIT,
            },
            limiter=limiter,
        )
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < KLINE_MAX_LIMIT:
            break
        cursor_ms = int(batch[-1][0]) + interval_ms
    return rows


def iter_klines_range(
    base_url: str,
    symbol: str,
    interval: str,
    interval_ms: int,
    start_ms: int,
    end_ms: int,
    limiter: RequestLimiter | None = None,
):
    cursor_ms = start_ms
    while cursor_ms <= end_ms:
        batch = fetch_json(
            base_url,
            "/fapi/v1/klines",
            params={
                "symbol": symbol,
                "interval": interval,
                "startTime": cursor_ms,
                "endTime": end_ms,
                "limit": KLINE_MAX_LIMIT,
            },
            limiter=limiter,
        )
        if not batch:
            break
        yield batch
        if len(batch) < KLINE_MAX_LIMIT:
            break
        cursor_ms = int(batch[-1][0]) + interval_ms


def discover_onboard_ms(
    base_url: str,
    symbol: str,
    fallback_start_ms: int,
    end_ms: int,
    limiter: RequestLimiter | None = None,
) -> int | None:
    rows = fetch_json(
        base_url,
        "/fapi/v1/klines",
        params={
            "symbol": symbol,
            "interval": INTERVAL_3M,
            "startTime": fallback_start_ms,
            "endTime": end_ms,
            "limit": 1,
        },
        limiter=limiter,
    )
    if rows:
        return int(rows[0][0])
    return None


async def resolve_expected_start_ms(
    base_url: str,
    state: SharedState,
    symbol: str,
    lookback_days: int,
    now_ms: int,
    limiter: RequestLimiter | None = None,
) -> tuple[int, int | None]:
    cutoff_ms = dt_to_ms(utc_now() - timedelta(days=lookback_days))
    onboard_ms = await state.current_onboard_ms(symbol)
    if onboard_ms is None:
        onboard_ms = discover_onboard_ms(base_url, symbol, cutoff_ms, now_ms, limiter)
    effective_start = max(cutoff_ms, onboard_ms or cutoff_ms)
    return ceil_interval(effective_start, INTERVAL_3M_MS), onboard_ms


def rest_kline_to_3m_row(symbol: str, row: list[Any]) -> KlineRow:
    open_ms = int(row[0])
    close_ms = int(row[6])
    now_ms = dt_to_ms(utc_now())
    is_closed = close_ms < now_ms
    return KlineRow(
        symbol=symbol,
        open_time=open_ms,
        close_time=close_ms,
        open_price=to_decimal_zero(row[1]),
        high_price=to_decimal_zero(row[2]),
        low_price=to_decimal_zero(row[3]),
        close_price=to_decimal_zero(row[4]),
        volume=to_decimal_zero(row[5]),
        quote_volume=to_decimal_zero(row[7]),
        trades=int(row[8]),
        first_trade_id=0,
        last_trade_id=0,
        taker_buy_base_volume=to_decimal_zero(row[9]),
        taker_buy_quote_volume=to_decimal_zero(row[10]),
        is_closed=is_closed,
        event_time=close_ms,
    )


def build_3m_param_list(
    table: str,
    columns: dict[str, TableColumn],
    rows: list[KlineRow],
) -> tuple[str, list[list[Any]]]:
    sql, params = build_insert_sql(table, columns, rows[0], "update")
    param_list = [params]
    for row in rows[1:]:
        _, values = build_insert_sql(table, columns, row, "update")
        param_list.append(values)
    return sql, param_list


def upsert_3m_rows(
    connection: Connection,
    database: str,
    table: str,
    symbol: str,
    rows: list[list[Any]],
    columns: dict[str, TableColumn] | None = None,
) -> int:
    if not rows:
        return 0
    columns = columns or load_3m_columns(connection, database, table)
    converted = [rest_kline_to_3m_row(symbol, row) for row in rows]
    sql, param_list = build_3m_param_list(table, columns, converted)
    with connection.cursor() as cursor:
        cursor.executemany(sql, param_list)
    connection.commit()
    return len(converted)


def repair_3m_segment_now(
    connection: Connection,
    database: str,
    base_url: str,
    symbol: str,
    start_ms: int,
    end_ms: int,
    limiter: RequestLimiter | None = None,
    columns: dict[str, TableColumn] | None = None,
) -> int:
    """Fetch and upsert a short closed-3m segment immediately."""
    rows = fetch_klines_range(
        base_url,
        symbol,
        INTERVAL_3M,
        INTERVAL_3M_MS,
        start_ms,
        end_ms,
        limiter,
    )
    if not rows:
        return 0
    return upsert_3m_rows(connection, database, KLINES_3M_TABLE, symbol, rows, columns)


def fetch_3m_candles(connection: Connection, symbol: str, from_ms: int, to_ms: int) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT
                open_time_ms,
                open_price, high_price, low_price, close_price,
                volume, quote_volume, trade_count,
                taker_buy_volume, taker_buy_quote_volume,
                close_time_ms, is_closed
            FROM {KLINES_3M_TABLE}
            WHERE symbol = %s
              AND open_time_ms >= %s
              AND open_time_ms <= %s
            ORDER BY open_time_ms ASC
            """,
            (symbol, from_ms, to_ms),
        )
        return list(cursor.fetchall())


def aggregate_to_15m(symbol: str, candles_3m: list[dict[str, Any]]) -> list[tuple]:
    if not candles_3m:
        return []
    windows: dict[int, list[dict[str, Any]]] = {}
    for candle in candles_3m:
        key = floor_interval(int(candle["open_time_ms"]), INTERVAL_15M_MS)
        windows.setdefault(key, []).append(candle)

    rows: list[tuple] = []
    for win_ms in sorted(windows):
        group = windows[win_ms]
        group.sort(key=lambda candle: int(candle["open_time_ms"]))
        first = group[0]
        last = group[-1]
        close_ms = int(last["close_time_ms"])
        is_closed = 1 if len(group) == CANDLES_PER_15M and int(last["is_closed"]) == 1 else 0
        rows.append(
            (
                symbol,
                win_ms,
                ms_to_dt(win_ms),
                close_ms,
                ms_to_dt(close_ms),
                to_decimal_zero(first["open_price"]),
                max(to_decimal_zero(candle["high_price"]) for candle in group),
                min(to_decimal_zero(candle["low_price"]) for candle in group),
                to_decimal_zero(last["close_price"]),
                sum(to_decimal_zero(candle["volume"]) for candle in group),
                sum(to_decimal_zero(candle["quote_volume"]) for candle in group),
                sum(int(candle["trade_count"]) for candle in group),
                sum(to_decimal_zero(candle["taker_buy_volume"]) for candle in group),
                sum(to_decimal_zero(candle["taker_buy_quote_volume"]) for candle in group),
                is_closed,
            )
        )
    return rows


def upsert_15m_rows(connection: Connection, rows: list[tuple]) -> int:
    if not rows:
        return 0
    with connection.cursor() as cursor:
        cursor.executemany(
            f"""
            INSERT INTO {KLINES_15M_TABLE} (
                symbol, open_time_ms, open_time_utc,
                close_time_ms, close_time_utc,
                open_price, high_price, low_price, close_price,
                volume, quote_volume, trade_count,
                taker_buy_volume, taker_buy_quote_volume,
                is_closed
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                close_time_ms          = VALUES(close_time_ms),
                close_time_utc         = VALUES(close_time_utc),
                open_price             = VALUES(open_price),
                high_price             = VALUES(high_price),
                low_price              = VALUES(low_price),
                close_price            = VALUES(close_price),
                volume                 = VALUES(volume),
                quote_volume           = VALUES(quote_volume),
                trade_count            = VALUES(trade_count),
                taker_buy_volume       = VALUES(taker_buy_volume),
                taker_buy_quote_volume = VALUES(taker_buy_quote_volume),
                is_closed              = VALUES(is_closed)
            """,
            rows,
        )
    connection.commit()
    return len(rows)


def ensure_symbol_3m_complete(
    connection: Connection,
    args: argparse.Namespace,
    symbol: str,
    expected_start_ms: int,
    now_ms: int,
    limiter: RequestLimiter,
) -> tuple[int, int, int | None]:
    rest_cutoff_ms = now_ms - (args.rest_lag_seconds * 1000)
    expected_end_ms = floor_interval(rest_cutoff_ms, INTERVAL_3M_MS) - INTERVAL_3M_MS
    if expected_end_ms < expected_start_ms:
        return 0, 0, None

    segments, _stats = build_missing_segments(
        connection,
        KLINES_3M_TABLE,
        symbol,
        expected_start_ms,
        expected_end_ms,
        INTERVAL_3M_MS,
    )
    if not segments:
        return 0, 0, None

    columns = load_3m_columns(connection, args.mysql_database, KLINES_3M_TABLE)
    total_rows = 0
    earliest_changed_ms: int | None = None
    for seg_start, seg_end in segments:
        for klines in iter_klines_range(
            args.base_url,
            symbol,
            INTERVAL_3M,
            INTERVAL_3M_MS,
            seg_start,
            seg_end,
            limiter,
        ):
            inserted = upsert_3m_rows(
                connection,
                args.mysql_database,
                KLINES_3M_TABLE,
                symbol,
                klines,
                columns,
            )
            total_rows += inserted
            first_open_ms = int(klines[0][0])
            if earliest_changed_ms is None or first_open_ms < earliest_changed_ms:
                earliest_changed_ms = first_open_ms
    return len(segments), total_rows, earliest_changed_ms


def ensure_symbol_15m_complete(
    connection: Connection,
    args: argparse.Namespace,
    symbol: str,
    expected_start_ms: int,
    now_ms: int,
    changed_from_3m_ms: int | None,
) -> int:
    start_15m_ms = floor_interval(expected_start_ms, INTERVAL_15M_MS)
    latest_3m_ms = get_latest_closed_3m_ms(connection, args.anchor_symbol)
    if latest_3m_ms is None:
        return 0
    lagged_now_ms = now_ms - int(max(0, args.aggregation_lag_seconds) * 1000)
    last_ready_15m_by_time = floor_interval(lagged_now_ms, INTERVAL_15M_MS) - INTERVAL_15M_MS
    last_ready_15m_by_3m = latest_ready_15m_from_3m(latest_3m_ms)
    current_15m_ms = min(last_ready_15m_by_time, last_ready_15m_by_3m)
    if current_15m_ms < start_15m_ms:
        return 0

    stats = fetch_coverage_stats(connection, KLINES_15M_TABLE, symbol, start_15m_ms, current_15m_ms)
    expected = expected_candle_count(start_15m_ms, current_15m_ms, INTERVAL_15M_MS)
    coverage_complete = (
        expected == 0
        or (
            stats.count == expected
            and stats.min_ms == start_15m_ms
            and stats.max_ms == current_15m_ms
        )
    )

    if changed_from_3m_ms is not None:
        aggregate_from_ms = floor_interval(changed_from_3m_ms, INTERVAL_15M_MS)
    elif coverage_complete and stats.max_ms is not None:
        aggregate_from_ms = max(start_15m_ms, stats.max_ms)
    else:
        aggregate_from_ms = start_15m_ms

    aggregate_to_ms = current_15m_ms + INTERVAL_15M_MS - INTERVAL_3M_MS
    candles_3m = fetch_3m_candles(connection, symbol, aggregate_from_ms, aggregate_to_ms)
    if not candles_3m:
        return 0
    return upsert_15m_rows(connection, aggregate_to_15m(symbol, candles_3m))


def repair_symbol_worker(
    args: argparse.Namespace,
    symbol: str,
    expected_start_ms: int,
    onboard_ms: int | None,
    now_ms: int,
    limiter: RequestLimiter,
) -> SymbolRepairResult:
    connection = connect_mysql(args, ensure_schema_objects=False)
    try:
        gap_count, backfilled_rows, changed_from_ms = ensure_symbol_3m_complete(
            connection,
            args,
            symbol,
            expected_start_ms,
            now_ms,
            limiter,
        )
        aggregated_rows = ensure_symbol_15m_complete(
            connection,
            args,
            symbol,
            expected_start_ms,
            now_ms,
            changed_from_ms,
        )
        return SymbolRepairResult(
            symbol=symbol,
            expected_start_ms=expected_start_ms,
            onboard_ms=onboard_ms,
            gap_count=gap_count,
            backfilled_rows=backfilled_rows,
            aggregated_rows=aggregated_rows,
        )
    finally:
        close_quietly(connection)


def run_repair_workers(
    args: argparse.Namespace,
    repair_plan: list[tuple[str, int, int | None]],
    now_ms: int,
    limiter: RequestLimiter,
) -> tuple[int, int]:
    total_3m_rows = 0
    total_15m_rows = 0
    completed = 0
    max_workers = max(1, args.workers)

    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="kline-repair") as executor:
        futures = {
            executor.submit(
                repair_symbol_worker,
                args,
                symbol,
                expected_start_ms,
                onboard_ms,
                now_ms,
                limiter,
            ): (symbol, expected_start_ms, onboard_ms)
            for symbol, expected_start_ms, onboard_ms in repair_plan
        }

        for future in as_completed(futures):
            symbol, expected_start_ms, onboard_ms = futures[future]
            completed += 1
            try:
                result = future.result()
            except Exception as exc:
                LOG.exception(
                    "[%s/%s] %s repair failed start=%s onboard=%s: %s",
                    completed,
                    len(futures),
                    symbol,
                    ms_to_text(expected_start_ms),
                    ms_to_text(onboard_ms),
                    exc,
                )
                continue

            total_3m_rows += result.backfilled_rows
            total_15m_rows += result.aggregated_rows
            LOG.info(
                "[%s/%s] %s start=%s onboard=%s gaps=%s 3m=%s 15m=%s",
                completed,
                len(futures),
                result.symbol,
                ms_to_text(result.expected_start_ms),
                ms_to_text(result.onboard_ms),
                result.gap_count,
                result.backfilled_rows,
                result.aggregated_rows,
            )

    return total_3m_rows, total_15m_rows


async def run_repair_cycle(
    connection: Connection,
    args: argparse.Namespace,
    state: SharedState,
) -> None:
    top_symbols, _contracts, _tickers = await state.snapshot_top()
    watch_symbols = top_symbols + [symbol for symbol in PINNED_SYMBOLS if symbol not in top_symbols]
    expired = expire_watch(connection)
    new_symbols = upsert_watch(connection, watch_symbols, args.watch_days)
    upsert_watch(connection, PINNED_SYMBOLS, PINNED_DAYS)
    log_new_symbols(new_symbols)

    active_symbols = resolve_repair_symbols(connection, args)
    now_ms = dt_to_ms(utc_now())
    limiter = RequestLimiter(
        args.request_delay,
        max_retries=args.rest_retries,
        retry_delay=args.retry_delay,
    )

    LOG.info(
        "repair cycle start top=%s db_symbols=%s new_watch=%s expired_watch=%s days=%s workers=%s request_delay=%ss rest_lag=%ss retries=%s",
        len(top_symbols),
        len(active_symbols),
        len(new_symbols),
        expired,
        args.days,
        args.workers,
        args.request_delay,
        args.rest_lag_seconds,
        args.rest_retries,
    )

    if not active_symbols:
        LOG.warning("repair cycle skipped: no symbols found in %s", KLINES_3M_TABLE)
        return

    repair_plan: list[tuple[str, int, int | None]] = []
    for symbol in active_symbols:
        try:
            expected_start_ms, onboard_ms = await resolve_expected_start_ms(
                args.base_url,
                state,
                symbol,
                args.days,
                now_ms,
                limiter,
            )
            repair_plan.append((symbol, expected_start_ms, onboard_ms))
        except Exception as exc:
            LOG.exception("%s start-time resolution failed: %s", symbol, exc)

    total_3m_rows, total_15m_rows = await asyncio.to_thread(
        run_repair_workers,
        args,
        repair_plan,
        now_ms,
        limiter,
    )

    LOG.info(
        "repair cycle done symbols=%s 3m_rows=%s 15m_rows=%s",
        len(repair_plan),
        total_3m_rows,
        total_15m_rows,
    )


async def ticker_ws_loop(args: argparse.Namespace, state: SharedState, stop_event: asyncio.Event) -> None:
    url = f"{args.ws_base_url}?{urlencode({'streams': WS_TICKER_STREAM})}"
    while not stop_event.is_set():
        try:
            async with ws_connect(url) as ws:
                LOG.info("ticker websocket connected")
                while not stop_event.is_set():
                    raw = await ws.recv()
                    message = json.loads(raw)
                    data = message.get("data", message)
                    rows = data if isinstance(data, list) else [data]
                    await state.apply_ticker_rows([row for row in rows if isinstance(row, dict)])
        except ConnectionClosed as exc:
            LOG.warning("ticker websocket closed code=%s reason=%s", getattr(exc, "code", None), str(exc) or "-")
        except asyncio.TimeoutError:
            LOG.warning("ticker websocket opening handshake timed out; reconnecting")
        except Exception as exc:
            LOG.exception("ticker websocket error: %s", exc)
        if not stop_event.is_set():
            await asyncio.sleep(WS_RECONNECT_DELAY_SECONDS)


async def contract_ws_loop(args: argparse.Namespace, state: SharedState, stop_event: asyncio.Event) -> None:
    url = f"{args.ws_base_url}?{urlencode({'streams': WS_CONTRACT_STREAM})}"
    while not stop_event.is_set():
        try:
            async with ws_connect(url) as ws:
                LOG.info("contractInfo websocket connected")
                while not stop_event.is_set():
                    raw = await ws.recv()
                    message = json.loads(raw)
                    data = message.get("data", message)
                    rows = data if isinstance(data, list) else [data]
                    for row in rows:
                        if isinstance(row, dict):
                            await state.apply_contract_row(row)
        except ConnectionClosed as exc:
            LOG.warning("contractInfo websocket closed code=%s reason=%s", getattr(exc, "code", None), str(exc) or "-")
        except asyncio.TimeoutError:
            LOG.warning("contractInfo websocket opening handshake timed out; reconnecting")
        except Exception as exc:
            LOG.exception("contractInfo websocket error: %s", exc)
        if not stop_event.is_set():
            await asyncio.sleep(WS_RECONNECT_DELAY_SECONDS)


async def hourly_snapshot_loop(args: argparse.Namespace, state: SharedState, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        try:
            symbols, contracts, tickers = await state.snapshot_top()
            if symbols:
                connection = connect_mysql(args)
                try:
                    snapshot_id = insert_snapshot(
                        connection,
                        contracts,
                        tickers,
                        symbols,
                        source="WS:!ticker@arr",
                        interval_seconds=args.snapshot_interval,
                    )
                    LOG.info("saved hourly top%s snapshot #%s", len(symbols), snapshot_id)
                finally:
                    close_quietly(connection)
        except Exception as exc:
            LOG.exception("hourly snapshot failed: %s", exc)

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=args.snapshot_interval)
        except asyncio.TimeoutError:
            pass


async def repair_loop(args: argparse.Namespace, state: SharedState, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        connection: Connection | None = None
        try:
            connection = connect_mysql(args)
            await run_repair_cycle(connection, args, state)
        except Exception as exc:
            LOG.exception("repair loop failed: %s", exc)
        finally:
            close_quietly(connection)

        if args.once:
            stop_event.set()
            break
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=args.repair_interval)
        except asyncio.TimeoutError:
            pass


def parse_args(argv: list[str]) -> argparse.Namespace:
    load_environment()
    parser = argparse.ArgumentParser(description="Hybrid startup manager for Binance futures rankings/watchlist/klines.")
    parser.add_argument("--mysql-host", default=os.environ.get("MYSQL_HOST", DEFAULT_MYSQL_HOST))
    parser.add_argument("--mysql-port", type=int, default=int(os.environ.get("MYSQL_PORT", str(DEFAULT_MYSQL_PORT))))
    parser.add_argument("--mysql-user", default=os.environ.get("MYSQL_USER", DEFAULT_MYSQL_USER))
    parser.add_argument("--mysql-password", default=os.environ.get("MYSQL_PASSWORD", ""))
    parser.add_argument("--mysql-database", default=os.environ.get("MYSQL_DATABASE", DEFAULT_MYSQL_DATABASE))
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--ws-base-url", default=DEFAULT_WS_BASE_URL)
    parser.add_argument("--top", type=int, default=DEFAULT_TOP_LIMIT, help="Top-N ranking size. Default: 50")
    parser.add_argument("--days", type=int, default=DEFAULT_LOOKBACK_DAYS, help="History window in days. Default: 365")
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=None,
        help="Only repair these symbols. Default: all symbols already present in klines_3m.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Concurrent symbol repair workers. Default: {DEFAULT_WORKERS}",
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        default=DEFAULT_REQUEST_DELAY_SECONDS,
        help=(
            "Minimum seconds between Binance REST requests across all workers. "
            f"Default: {DEFAULT_REQUEST_DELAY_SECONDS}"
        ),
    )
    parser.add_argument(
        "--rest-lag-seconds",
        type=int,
        default=DEFAULT_REST_LAG_SECONDS,
        help=(
            "Only REST-repair closed 3m candles older than this lag, "
            f"leaving recent candles for websocket first. Default: {DEFAULT_REST_LAG_SECONDS}"
        ),
    )
    parser.add_argument(
        "--aggregation-lag-seconds",
        type=int,
        default=DEFAULT_AGGREGATION_LAG_SECONDS,
        help=(
            "Only synthesize 15m candles older than this lag, and only after the anchor latest 3m "
            f"covers the 15m window's final 3m. Default: {DEFAULT_AGGREGATION_LAG_SECONDS}"
        ),
    )
    parser.add_argument(
        "--rest-retries",
        type=int,
        default=DEFAULT_REST_RETRIES,
        help=f"Retry failed Binance REST requests this many times. Default: {DEFAULT_REST_RETRIES}",
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=DEFAULT_RETRY_DELAY_SECONDS,
        help=f"Initial retry backoff seconds for REST requests. Default: {DEFAULT_RETRY_DELAY_SECONDS}",
    )
    parser.add_argument("--watch-days", type=int, default=DEFAULT_WATCH_DAYS, help="Watchlist expiry in days. Default: 365")
    parser.add_argument(
        "--repair-interval",
        type=int,
        default=DEFAULT_REPAIR_INTERVAL_SECONDS,
        help="Seconds between repair/completeness cycles. Default: 180",
    )
    parser.add_argument(
        "--symbol-freshness-hours",
        type=float,
        default=DEFAULT_SYMBOL_FRESHNESS_HOURS,
        help=(
            "Default repair mode keeps only symbols with closed 3m bars within this many hours "
            f"of --anchor-symbol's latest 3m. Default: {DEFAULT_SYMBOL_FRESHNESS_HOURS}"
        ),
    )
    parser.add_argument(
        "--anchor-symbol",
        default=DEFAULT_ANCHOR_SYMBOL,
        help=f"Symbol used for cheap latest-3m probes. Default: {DEFAULT_ANCHOR_SYMBOL}",
    )
    parser.add_argument(
        "--snapshot-interval",
        type=int,
        default=DEFAULT_SNAPSHOT_INTERVAL_SECONDS,
        help="Seconds between ranking snapshots stored to DB. Default: 3600",
    )
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--once", action="store_true", help="Run one repair cycle after bootstrap, then exit.")
    return parser.parse_args(argv)


async def main_async(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ASYNCIO_LOG.addFilter(AsyncioWebsocketsNoiseFilter())

    require_pymysql()
    bootstrap_connection = connect_mysql(args)
    close_quietly(bootstrap_connection)

    state = SharedState(top_limit=args.top)
    await state.bootstrap_from_rest(args.base_url)
    LOG.info("bootstrapped contracts=%s tickers=%s top=%s", len(state.contracts), len(state.tickers), len(state.current_top_symbols))

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    tasks = [
        asyncio.create_task(ticker_ws_loop(args, state, stop_event)),
        asyncio.create_task(contract_ws_loop(args, state, stop_event)),
        asyncio.create_task(hourly_snapshot_loop(args, state, stop_event)),
        asyncio.create_task(repair_loop(args, state, stop_event)),
    ]

    try:
        await stop_event.wait()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    return 0


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    raise SystemExit(main())
