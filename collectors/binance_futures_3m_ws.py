#!/usr/bin/env python3
"""
Subscribe Binance USD-M futures 3m kline streams and persist them into MySQL.

The deployment initializes the table separately with sql/001_schema.sql.
This process reads the local strategy symbol file and writes only completed 3m rows;
its retention worker deletes rows older than the configured one-day window.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import signal
import ssl
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlencode

import aiomysql
import websockets
from websockets.exceptions import ConnectionClosed

from . import binance_network
from v9711_live.environment import load_environment, MYSQL_DEFAULT_PASSWORD
from v9711_live import config as live_cfg
from .kline_status import write_3m_status


DEFAULT_MYSQL_HOST = "127.0.0.1"
DEFAULT_MYSQL_PORT = 3306
DEFAULT_MYSQL_USER = "root"
DEFAULT_MYSQL_DATABASE = "binance_futures"

TABLE_NAME = "klines_3m"
INTERVAL = "3m"
WS_BASE_URL = "wss://fstream.binance.com/market/stream"
MAX_STREAMS_PER_CONNECTION = 100
RECONNECT_BEFORE_SECONDS = 23 * 60 * 60
MYSQL_RETRY_SECONDS = 5
DEFAULT_PING_INTERVAL_SECONDS = 30
DEFAULT_PING_TIMEOUT_SECONDS = 20
DEFAULT_READ_TIMEOUT_SECONDS = 300
BATCH_LOG_MAX_DELAY_SECONDS = 90.0
STATUS_MIN_CONNECTION_RATIO = 0.90
DEPLOY_ROOT = Path(os.environ.get('V9711_DEPLOY_ROOT', str(Path(__file__).resolve().parents[1])))
DEFAULT_SYMBOLS_FILE = str(DEPLOY_ROOT / 'config' / 'symbols.txt')
DEFAULT_AGGREGATOR_TRIGGER_FILE = os.path.join(DEPLOY_ROOT, "var", "kline_aggregator_15m.trigger")
DEFAULT_3M_STATUS_FILE = os.path.join(DEPLOY_ROOT, "var", "klines_3m_status.json")

OPEN_TIME_COLUMNS = {
    "open_time",
    "open_timestamp",
    "start_time",
    "timestamp",
    "time",
    "open_time_ms",
    "start_time_ms",
    "timestamp_ms",
    "time_ms",
    "open_time_utc",
    "start_time_utc",
    "open_datetime",
    "open_date",
    "datetime",
    "date_time",
    "dt",
    "date",
}
KEY_COLUMNS = OPEN_TIME_COLUMNS | {"symbol", "interval"}


LOG = logging.getLogger("binance_futures_3m_ws")


@dataclass(frozen=True)
class MySQLConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    charset: str = "utf8mb4"


@dataclass(frozen=True)
class TableColumn:
    name: str
    data_type: str
    nullable: bool
    has_default: bool
    extra: str


@dataclass(frozen=True)
class KlineRow:
    symbol: str
    open_time: int
    close_time: int
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: Decimal
    quote_volume: Decimal
    trades: int
    first_trade_id: int
    last_trade_id: int
    taker_buy_base_volume: Decimal
    taker_buy_quote_volume: Decimal
    is_closed: bool
    event_time: int


class GracefulShutdown:
    def __init__(self) -> None:
        self.event = asyncio.Event()

    def install(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.event.set)
            except NotImplementedError:
                signal.signal(sig, lambda *_: self.event.set())


class ClosedBatchStatusTracker:
    """Publish after every websocket connection persists its symbol quorum."""

    def __init__(
        self,
        symbols: list[str],
        path: str,
        *,
        symbol_groups: Iterable[Iterable[str]] | None = None,
        min_group_ratio: float = STATUS_MIN_CONNECTION_RATIO,
    ) -> None:
        self.expected = frozenset(
            symbol.strip().upper() for symbol in symbols if symbol.strip()
        )
        raw_groups = symbol_groups if symbol_groups is not None else (self.expected,)
        self.groups = tuple(
            frozenset(str(symbol).strip().upper() for symbol in group if str(symbol).strip())
            for group in raw_groups
        )
        self.groups = tuple(group for group in self.groups if group)
        self.min_group_ratio = min(1.0, max(0.0, float(min_group_ratio)))
        self.path = Path(path) if path else None
        self.batches: dict[int, set[str]] = {}
        self.latest_written = -1

    def _group_progress(self, received: set[str]) -> list[tuple[int, int]]:
        return [
            (
                len(group.intersection(received)),
                max(1, math.ceil(len(group) * self.min_group_ratio)),
            )
            for group in self.groups
        ]

    def record(self, row: KlineRow) -> bool:
        if (
            self.path is None
            or not row.is_closed
            or row.open_time <= self.latest_written
        ):
            return False
        received = self.batches.setdefault(row.open_time, set())
        received.add(row.symbol)
        actual = received.intersection(self.expected)
        progress = self._group_progress(actual)
        if not self.expected or not progress or any(count < required for count, required in progress):
            return False
        complete = self.expected.issubset(actual)
        write_3m_status(
            open_time_ms=row.open_time,
            symbols=list(actual),
            row_count=len(actual),
            source=(
                "binance_futures_3m_ws_complete_batch"
                if complete
                else "binance_futures_3m_ws_per_connection_90pct"
            ),
            path=self.path,
        )
        self.latest_written = row.open_time
        self.batches = {
            open_time: symbols
            for open_time, symbols in self.batches.items()
            if open_time > self.latest_written
        }
        LOG.info(
            "published 3m status open_time=%s symbols=%s/%s groups=%s ratio=%.2f path=%s",
            time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(row.open_time / 1000)),
            len(actual),
            len(self.expected),
            ",".join(f"{count}/{required}" for count, required in progress),
            self.min_group_ratio,
            self.path,
        )
        return True


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


def parse_args() -> argparse.Namespace:
    load_environment()
    parser = argparse.ArgumentParser(
        description="Subscribe Binance USD-M futures 3m klines and write to existing MySQL klines_3m table.",
    )
    parser.add_argument("--mysql-host", default=env("MYSQL_HOST", DEFAULT_MYSQL_HOST))
    parser.add_argument("--mysql-port", type=int, default=int(env("MYSQL_PORT", str(DEFAULT_MYSQL_PORT))))
    parser.add_argument("--mysql-user", default=env("MYSQL_USER", DEFAULT_MYSQL_USER))
    parser.add_argument("--mysql-password", default=env("MYSQL_PASSWORD", MYSQL_DEFAULT_PASSWORD))
    parser.add_argument("--mysql-database", default=env("MYSQL_DATABASE", DEFAULT_MYSQL_DATABASE))
    parser.add_argument("--symbol-column", default=env("SYMBOL_COLUMN", "symbol"))
    parser.add_argument("--symbols-file", default=env("SYMBOLS_FILE", DEFAULT_SYMBOLS_FILE),
                        help="Local strategy symbol list; avoids coupling subscriptions to old database contents.")
    parser.add_argument(
        "--time-column",
        default=env("TIME_COLUMN", ""),
        help="Existing klines_3m column that stores kline open time, if it uses a custom name.",
    )
    parser.add_argument("--table", default=env("KLINE_TABLE", TABLE_NAME))
    parser.add_argument("--retention-hours", type=float,
                        default=float(env("KLINE_RETENTION_HOURS", "24")),
                        help="Delete persisted 3m rows older than this many hours; default 24.")
    parser.add_argument(
        "--write-open-klines",
        action="store_true",
        default=env("WRITE_OPEN_KLINES", "0").lower() in {"1", "true", "yes"},
        help="Also write unfinished klines. By default only closed klines are persisted.",
    )
    parser.add_argument(
        "--duplicate-policy",
        default=env("DUPLICATE_POLICY", "update"),
        choices=("ignore", "update"),
        help="Use ignore to preserve existing rows, or update to refresh duplicate rows. Default: update.",
    )
    parser.add_argument(
        "--refresh-symbols-seconds",
        type=int,
        default=int(env("REFRESH_SYMBOLS_SECONDS", "300")),
        help="Reload symbols from MySQL after this many seconds. Set 0 to disable.",
    )
    parser.add_argument(
        "--streams-per-connection",
        type=int,
        default=int(env("STREAMS_PER_CONNECTION", str(MAX_STREAMS_PER_CONNECTION))),
        help=f"Max combined streams per websocket connection. Default: {MAX_STREAMS_PER_CONNECTION}.",
    )
    parser.add_argument(
        "--ping-interval",
        type=float,
        default=float(env("WS_PING_INTERVAL", str(DEFAULT_PING_INTERVAL_SECONDS))),
        help=f"Websocket ping interval seconds. Default: {DEFAULT_PING_INTERVAL_SECONDS}.",
    )
    parser.add_argument(
        "--ping-timeout",
        type=float,
        default=float(env("WS_PING_TIMEOUT", str(DEFAULT_PING_TIMEOUT_SECONDS))),
        help=f"Websocket ping timeout seconds. Default: {DEFAULT_PING_TIMEOUT_SECONDS}.",
    )
    parser.add_argument(
        "--read-timeout",
        type=float,
        default=float(env("WS_READ_TIMEOUT", str(DEFAULT_READ_TIMEOUT_SECONDS))),
        help=f"Reconnect if no message is received for this many seconds. Default: {DEFAULT_READ_TIMEOUT_SECONDS}.",
    )
    parser.add_argument(
        "--aggregator-trigger-file",
        default=env("KLINE_AGGREGATOR_TRIGGER_FILE", DEFAULT_AGGREGATOR_TRIGGER_FILE),
        help=(
            "Write this local trigger file after websocket errors/reconnects so "
            "kline_aggregator_15m.py can wake and run a check. Set empty to disable."
        ),
    )
    parser.add_argument(
        "--status-file",
        default=env("KLINE_3M_STATUS_FILE", str(live_cfg.STATUS_FILE)),
        help=(
            "Atomically publish this file after every subscribed symbol has "
            "persisted its closed 3m bar. Set empty to disable."
        ),
    )
    parser.add_argument(
        "--log-level",
        default=env("LOG_LEVEL", "INFO"),
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
    )
    return parser.parse_args()


def quote_ident(identifier: str) -> str:
    if not identifier or "`" in identifier:
        raise ValueError(f"Invalid MySQL identifier: {identifier!r}")
    return f"`{identifier}`"


def chunked(items: list[str], size: int) -> Iterable[list[str]]:
    for index in range(0, len(items), size):
        yield items[index : index + size]


async def create_pool(config: MySQLConfig) -> aiomysql.Pool:
    return await aiomysql.create_pool(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        db=config.database,
        charset=config.charset,
        autocommit=True,
        minsize=1,
        maxsize=10,
    )


async def load_columns(pool: aiomysql.Pool, database: str, table: str) -> dict[str, TableColumn]:
    sql = """
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
    """
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql, (database, table))
            rows = await cur.fetchall()

    if not rows:
        raise RuntimeError(f"MySQL table {database}.{table} does not exist or has no columns")

    return {
        row["COLUMN_NAME"]: TableColumn(
            name=row["COLUMN_NAME"],
            data_type=row["DATA_TYPE"].lower(),
            nullable=row["IS_NULLABLE"] == "YES",
            has_default=row["COLUMN_DEFAULT"] is not None,
            extra=(row["EXTRA"] or "").lower(),
        )
        for row in rows
    }


def read_symbols_file(path: str) -> list[str]:
    file_path = Path(path).expanduser()
    if not file_path.is_file():
        raise RuntimeError(f"symbols file does not exist: {file_path}")
    symbols = sorted({line.strip().upper() for line in file_path.read_text(encoding="utf-8").splitlines()
                      if line.strip() and not line.lstrip().startswith("#")})
    if not symbols:
        raise RuntimeError(f"symbols file is empty: {file_path}")
    return symbols


async def load_symbols(pool: aiomysql.Pool, table: str, symbol_column: str,
                       symbols_file: str = "") -> list[str]:
    if symbols_file:
        return read_symbols_file(symbols_file)
    sql = (
        f"SELECT DISTINCT {quote_ident(symbol_column)} AS symbol "
        f"FROM {quote_ident(table)} "
        f"WHERE {quote_ident(symbol_column)} IS NOT NULL AND {quote_ident(symbol_column)} <> ''"
    )
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql)
            rows = await cur.fetchall()

    symbols = sorted({str(row[0]).strip().upper() for row in rows if row and str(row[0]).strip()})
    if not symbols:
        raise RuntimeError(f"No symbols found in {table}.{symbol_column}")
    return symbols


def parse_kline_message(raw: str) -> KlineRow | None:
    message = json.loads(raw)
    data = message.get("data", message)
    if data.get("e") != "kline":
        return None

    kline = data["k"]
    return KlineRow(
        symbol=str(kline["s"]).upper(),
        open_time=int(kline["t"]),
        close_time=int(kline["T"]),
        open_price=Decimal(kline["o"]),
        high_price=Decimal(kline["h"]),
        low_price=Decimal(kline["l"]),
        close_price=Decimal(kline["c"]),
        volume=Decimal(kline["v"]),
        quote_volume=Decimal(kline["q"]),
        trades=int(kline["n"]),
        first_trade_id=int(kline["f"]),
        last_trade_id=int(kline["L"]),
        taker_buy_base_volume=Decimal(kline["V"]),
        taker_buy_quote_volume=Decimal(kline["Q"]),
        is_closed=bool(kline["x"]),
        event_time=int(data["E"]),
    )


def candidate_values(row: KlineRow) -> dict[str, Any]:
    open_dt = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(row.open_time / 1000))
    close_dt = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(row.close_time / 1000))
    event_dt = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(row.event_time / 1000))
    updated_at = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

    return {
        "symbol": row.symbol,
        "interval": INTERVAL,
        "open_time": row.open_time,
        "open_timestamp": row.open_time,
        "start_time": row.open_time,
        "timestamp": row.open_time,
        "time": row.open_time,
        "open_time_ms": row.open_time,
        "start_time_ms": row.open_time,
        "timestamp_ms": row.open_time,
        "time_ms": row.open_time,
        "close_time": row.close_time,
        "end_time": row.close_time,
        "close_time_ms": row.close_time,
        "end_time_ms": row.close_time,
        "open_datetime": open_dt,
        "open_date": open_dt,
        "datetime": open_dt,
        "date_time": open_dt,
        "dt": open_dt,
        "date": open_dt,
        "open_time_utc": open_dt,
        "start_time_utc": open_dt,
        "close_datetime": close_dt,
        "close_time_utc": close_dt,
        "end_time_utc": close_dt,
        "event_time": row.event_time,
        "event_time_ms": row.event_time,
        "event_datetime": event_dt,
        "open": row.open_price,
        "open_price": row.open_price,
        "high": row.high_price,
        "high_price": row.high_price,
        "low": row.low_price,
        "low_price": row.low_price,
        "close": row.close_price,
        "close_price": row.close_price,
        "volume": row.volume,
        "base_volume": row.volume,
        "quote_volume": row.quote_volume,
        "quote_asset_volume": row.quote_volume,
        "number_of_trades": row.trades,
        "trade_count": row.trades,
        "trades": row.trades,
        "first_trade_id": row.first_trade_id,
        "first_trade": row.first_trade_id,
        "last_trade_id": row.last_trade_id,
        "last_trade": row.last_trade_id,
        "taker_buy_base_volume": row.taker_buy_base_volume,
        "taker_buy_volume": row.taker_buy_base_volume,
        "taker_buy_quote_volume": row.taker_buy_quote_volume,
        "taker_buy_quote_asset_volume": row.taker_buy_quote_volume,
        "is_closed": int(row.is_closed),
        "closed": int(row.is_closed),
        "created_at": updated_at,
        "create_time": updated_at,
        "created_time": updated_at,
        "updated_at": updated_at,
        "update_time": updated_at,
    }


def coerce_value(value: Any, column: TableColumn) -> Any:
    if value is None:
        return None
    if column.data_type in {"datetime", "timestamp"} and isinstance(value, int):
        return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(value / 1000))
    if column.data_type == "date" and isinstance(value, str):
        return value[:10]
    if column.data_type in {"tinyint", "smallint", "mediumint", "int", "bigint"} and isinstance(value, bool):
        return int(value)
    return value


def kline_open_time_value(row: KlineRow, column: TableColumn) -> Any:
    open_dt = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(row.open_time / 1000))
    if column.data_type in {"datetime", "timestamp"}:
        return open_dt
    if column.data_type == "date":
        return open_dt[:10]
    return row.open_time


def build_insert_sql(
    table: str,
    columns: dict[str, TableColumn],
    row: KlineRow,
    duplicate_policy: str,
    time_column: str = "",
) -> tuple[str, list[Any]]:
    values = candidate_values(row)
    if time_column and time_column in columns and time_column not in values:
        values[time_column] = kline_open_time_value(row, columns[time_column])

    insert_columns: list[str] = []
    params: list[Any] = []

    for name, column in columns.items():
        if "auto_increment" in column.extra:
            continue
        if name not in values:
            continue
        insert_columns.append(name)
        params.append(coerce_value(values[name], column))

    if "symbol" not in insert_columns:
        raise RuntimeError("klines_3m must have a symbol column to receive websocket data")
    time_columns = OPEN_TIME_COLUMNS | ({time_column} if time_column else set())
    if not any(name in insert_columns for name in time_columns):
        raise RuntimeError(
            "klines_3m must have a kline open-time column. "
            "If your table uses a custom name, run with --time-column YOUR_COLUMN. "
            f"Detected columns: {', '.join(columns)}"
        )

    quoted_columns = ", ".join(quote_ident(name) for name in insert_columns)
    placeholders = ", ".join(["%s"] * len(insert_columns))
    sql = f"INSERT IGNORE INTO {quote_ident(table)} ({quoted_columns}) VALUES ({placeholders})"
    if duplicate_policy == "update":
        update_columns = [
            name
            for name in insert_columns
            if name not in KEY_COLUMNS | ({time_column} if time_column else set())
        ]
        if update_columns:
            update_clause = ", ".join(
                f"{quote_ident(name)} = new_values.{quote_ident(name)}" for name in update_columns
            )
        else:
            update_clause = f"{quote_ident(insert_columns[0])} = {quote_ident(insert_columns[0])}"
        sql = (
            f"INSERT INTO {quote_ident(table)} ({quoted_columns}) "
            f"VALUES ({placeholders}) AS new_values "
            f"ON DUPLICATE KEY UPDATE {update_clause}"
        )
    return sql, params


class KlineWriter:
    def __init__(
        self,
        pool: aiomysql.Pool,
        table: str,
        columns: dict[str, TableColumn],
        duplicate_policy: str,
        time_column: str = "",
        retention_hours: float = 24.0,
    ) -> None:
        self.pool = pool
        self.table = table
        self.columns = columns
        self.duplicate_policy = duplicate_policy
        self.time_column = time_column
        self.retention_hours = max(1.0, float(retention_hours))
        self.retention_column = time_column if time_column and time_column in columns else "open_time_ms"
        self._cleanup_lock = asyncio.Lock()
        self._next_cleanup_mono = 0.0

    async def _maybe_prune(self) -> None:
        if self.retention_column not in self.columns or time.monotonic() < self._next_cleanup_mono:
            return
        async with self._cleanup_lock:
            now_mono = time.monotonic()
            if now_mono < self._next_cleanup_mono:
                return
            self._next_cleanup_mono = now_mono + 60.0
            cutoff = int((time.time() - self.retention_hours * 3600.0) * 1000)
            try:
                async with self.pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(
                            f"DELETE FROM {quote_ident(self.table)} "
                            f"WHERE {quote_ident(self.retention_column)} < %s LIMIT 10000", (cutoff,))
                        deleted = int(cur.rowcount or 0)
                if deleted:
                    LOG.info("retention cleanup table=%s deleted=%s cutoff_ms=%s", self.table, deleted, cutoff)
            except Exception:
                LOG.exception("retention cleanup failed table=%s", self.table)

    async def write(self, row: KlineRow) -> int:
        sql, params = build_insert_sql(self.table, self.columns, row, self.duplicate_policy, self.time_column)
        while True:
            try:
                async with self.pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(sql, params)
                        affected = int(cur.rowcount or 0)
                if row.is_closed:
                    await self._maybe_prune()
                return affected
            except Exception:
                LOG.exception("MySQL write failed for %s %s; retrying in %ss", row.symbol, row.open_time, MYSQL_RETRY_SECONDS)
                await asyncio.sleep(MYSQL_RETRY_SECONDS)


def build_ws_url(symbols: list[str]) -> str:
    streams = "/".join(f"{symbol.lower()}@kline_{INTERVAL}" for symbol in symbols)
    return f"{WS_BASE_URL}?{urlencode({'streams': streams})}"


def describe_connection_closed(exc: ConnectionClosed) -> str:
    details = [
        f"type={type(exc).__name__}",
        f"code={getattr(exc, 'code', None)}",
        f"reason={getattr(exc, 'reason', '') or '-'}",
        f"rcvd={getattr(exc, 'rcvd', None)}",
        f"sent={getattr(exc, 'sent', None)}",
        f"order={getattr(exc, 'rcvd_then_sent', None)}",
    ]
    return " ".join(details)


def write_aggregator_trigger(path: str, event: str, stream_name: str, detail: str = "") -> None:
    if not path:
        return
    try:
        parent = os.path.dirname(os.path.abspath(path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        payload = {
            "source": "binance_futures_3m_ws",
            "event": event,
            "stream": stream_name,
            "detail": detail,
            "time_ms": int(time.time() * 1000),
            "time_utc": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
        }
        tmp_path = f"{path}.tmp.{os.getpid()}.{stream_name}"
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, separators=(",", ":"))
            fh.write("\n")
        os.replace(tmp_path, path)
    except Exception:
        LOG.warning("failed to write aggregator trigger file=%s", path, exc_info=True)


def _record_batch_update(
    batches: dict[int, dict[str, Any]],
    row: KlineRow,
    affected: int,
    monotonic_now: float,
    wall_now: float,
) -> None:
    stats = batches.get(row.open_time)
    if stats is None:
        stats = {
            "first_seen": monotonic_now,
            "last_seen": monotonic_now,
            "first_saved_wall": wall_now,
            "last_saved_wall": wall_now,
            "symbols": set(),
            "rows": 0,
            "affected": 0,
            "closed": 0,
            "latest_event_time": row.event_time,
            "latest_close_time": row.close_time,
        }
        batches[row.open_time] = stats
    stats["last_seen"] = monotonic_now
    stats["last_saved_wall"] = wall_now
    stats["latest_event_time"] = max(int(stats["latest_event_time"]), row.event_time)
    stats["latest_close_time"] = max(int(stats["latest_close_time"]), row.close_time)
    stats["rows"] = int(stats["rows"]) + 1
    stats["affected"] = int(stats["affected"]) + int(affected)
    stats["closed"] = int(stats["closed"]) + (1 if row.is_closed else 0)
    stats["symbols"].add(row.symbol)


def _flush_batch_logs(
    name: str,
    batches: dict[int, dict[str, Any]],
    duplicate_policy: str,
    expected_symbols: int,
    force: bool = False,
) -> None:
    now = time.monotonic()
    wall_now = time.time()
    for open_time, stats in list(batches.items()):
        symbol_count = len(stats["symbols"])
        complete = expected_symbols > 0 and symbol_count >= expected_symbols
        delayed = now - float(stats["first_seen"]) >= BATCH_LOG_MAX_DELAY_SECONDS
        if not (force or complete or delayed):
            continue
        last_saved_wall = float(stats["last_saved_wall"])
        latest_event_s = int(stats["latest_event_time"]) / 1000.0
        latest_close_s = int(stats["latest_close_time"]) / 1000.0
        event_lag = max(0.0, last_saved_wall - latest_event_s)
        close_lag = max(0.0, last_saved_wall - latest_close_s)
        flush_delay = max(0.0, wall_now - last_saved_wall)
        LOG.info(
            "%s saved batch open_time=%s symbols=%s/%s rows=%s closed=%s affected=%s "
            "close_lag=%.2fs event_lag=%.2fs flush_delay=%.2fs policy=%s",
            name,
            time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(open_time / 1000)),
            symbol_count,
            expected_symbols,
            stats["rows"],
            stats["closed"],
            stats["affected"],
            close_lag,
            event_lag,
            flush_delay,
            duplicate_policy,
        )
        batches.pop(open_time, None)


async def stream_worker(
    name: str,
    symbols: list[str],
    writer: KlineWriter,
    shutdown: GracefulShutdown,
    write_open_klines: bool,
    ping_interval: float,
    ping_timeout: float,
    read_timeout: float,
    aggregator_trigger_file: str,
    status_tracker: ClosedBatchStatusTracker,
) -> None:
    ssl_context = ssl.create_default_context()
    retry_delay = 1
    connect_count = 0

    while not shutdown.event.is_set():
        url = build_ws_url(symbols)
        started_at = time.monotonic()
        last_message_at = started_at
        batch_logs: dict[int, dict[str, Any]] = {}
        try:
            LOG.info("%s connecting with %s symbols", name, len(symbols))
            async with websockets.connect(
                url,
                ssl=ssl_context,
                ping_interval=max(1.0, ping_interval),
                ping_timeout=max(1.0, ping_timeout),
                close_timeout=10,
                max_queue=4096,
                compression=None,
                **binance_network.websocket_connect_kwargs(),
            ) as websocket:
                LOG.info("%s connected", name)
                connect_count += 1
                if connect_count > 1:
                    write_aggregator_trigger(
                        aggregator_trigger_file,
                        "ws_reconnected",
                        name,
                        f"symbols={len(symbols)}",
                    )
                retry_delay = 1
                while not shutdown.event.is_set():
                    if time.monotonic() - started_at > RECONNECT_BEFORE_SECONDS:
                        LOG.info("%s reconnecting before Binance 24h connection limit", name)
                        break

                    try:
                        raw = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        _flush_batch_logs(name, batch_logs, writer.duplicate_policy, len(symbols))
                        if time.monotonic() - last_message_at >= max(1.0, read_timeout):
                            raise
                        continue
                    last_message_at = time.monotonic()
                    row = parse_kline_message(raw)
                    if row is None:
                        _flush_batch_logs(name, batch_logs, writer.duplicate_policy, len(symbols))
                        continue
                    if row.is_closed or write_open_klines:
                        affected = await writer.write(row)
                        _record_batch_update(batch_logs, row, affected, time.monotonic(), time.time())
                        status_tracker.record(row)
                    _flush_batch_logs(name, batch_logs, writer.duplicate_policy, len(symbols))
        except asyncio.TimeoutError:
            LOG.warning("%s websocket read timeout; reconnecting", name)
            write_aggregator_trigger(aggregator_trigger_file, "ws_read_timeout", name)
        except ConnectionClosed as exc:
            detail = describe_connection_closed(exc)
            LOG.warning("%s websocket closed %s", name, detail)
            write_aggregator_trigger(aggregator_trigger_file, "ws_closed", name, detail)
        except Exception as exc:
            LOG.exception("%s websocket error", name)
            write_aggregator_trigger(aggregator_trigger_file, "ws_error", name, repr(exc))
        finally:
            _flush_batch_logs(name, batch_logs, writer.duplicate_policy, len(symbols), force=True)

        if not shutdown.event.is_set():
            LOG.info("%s reconnecting in %ss", name, retry_delay)
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)


async def run_once_for_symbols(
    symbols: list[str],
    writer: KlineWriter,
    shutdown: GracefulShutdown,
    restart_event: asyncio.Event,
    write_open_klines: bool,
    streams_per_connection: int,
    ping_interval: float,
    ping_timeout: float,
    read_timeout: float,
    aggregator_trigger_file: str,
    status_file: str,
) -> None:
    chunk_size = max(1, int(streams_per_connection))
    symbol_groups = list(chunked(symbols, chunk_size))
    status_tracker = ClosedBatchStatusTracker(
        symbols,
        status_file,
        symbol_groups=symbol_groups,
    )
    workers = [
        asyncio.create_task(
            stream_worker(
                f"ws-{index + 1}",
                batch,
                writer,
                shutdown,
                write_open_klines,
                ping_interval,
                ping_timeout,
                read_timeout,
                aggregator_trigger_file,
                status_tracker,
            )
        )
        for index, batch in enumerate(symbol_groups)
    ]
    try:
        wait_shutdown = asyncio.create_task(shutdown.event.wait())
        wait_restart = asyncio.create_task(restart_event.wait())
        done, pending = await asyncio.wait(
            {wait_shutdown, wait_restart},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        if wait_restart in done:
            restart_event.clear()
    finally:
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)


async def symbol_refresh_loop(
    pool: aiomysql.Pool,
    table: str,
    symbol_column: str,
    symbols_file: str,
    refresh_seconds: int,
    state: dict[str, list[str]],
    restart_event: asyncio.Event,
    shutdown: GracefulShutdown,
) -> None:
    if refresh_seconds <= 0:
        return

    while not shutdown.event.is_set():
        try:
            await asyncio.wait_for(shutdown.event.wait(), timeout=refresh_seconds)
        except asyncio.TimeoutError:
            try:
                symbols = await load_symbols(pool, table, symbol_column, symbols_file)
            except Exception:
                LOG.exception("failed to refresh symbols; will retry")
                continue
            if symbols != state["symbols"]:
                LOG.info("symbol set changed from %s to %s; restarting subscriptions", len(state["symbols"]), len(symbols))
                state["symbols"] = symbols
                restart_event.set()


async def main_async() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    mysql_config = MySQLConfig(
        host=args.mysql_host,
        port=args.mysql_port,
        user=args.mysql_user,
        password=args.mysql_password,
        database=args.mysql_database,
    )

    shutdown = GracefulShutdown()
    shutdown.install()

    pool = await create_pool(mysql_config)
    try:
        columns = await load_columns(pool, mysql_config.database, args.table)
        if args.symbol_column not in columns:
            raise RuntimeError(f"{args.table} does not have symbol column {args.symbol_column!r}")

        symbols = await load_symbols(pool, args.table, args.symbol_column, args.symbols_file)
        LOG.info("loaded %s symbols from %s.%s", len(symbols), mysql_config.database, args.table)
        LOG.info("detected writable columns: %s", ", ".join(columns))
        LOG.info(
            "websocket config streams_per_connection=%s ping_interval=%.1fs ping_timeout=%.1fs read_timeout=%.1fs aggregator_trigger=%s",
            args.streams_per_connection,
            args.ping_interval,
            args.ping_timeout,
            args.read_timeout,
            args.aggregator_trigger_file or "disabled",
        )
        LOG.info("3m complete-batch status file=%s", args.status_file or "disabled")

        writer = KlineWriter(pool, args.table, columns, args.duplicate_policy, args.time_column, args.retention_hours)
        state = {"symbols": symbols}
        restart_event = asyncio.Event()
        refresh_task = asyncio.create_task(
            symbol_refresh_loop(
                pool,
                args.table,
                args.symbol_column,
                args.symbols_file,
                args.refresh_symbols_seconds,
                state,
                restart_event,
                shutdown,
            )
        )
        try:
            while not shutdown.event.is_set():
                await run_once_for_symbols(
                    state["symbols"],
                    writer,
                    shutdown,
                    restart_event,
                    args.write_open_klines,
                    args.streams_per_connection,
                    args.ping_interval,
                    args.ping_timeout,
                    args.read_timeout,
                    args.aggregator_trigger_file,
                    args.status_file,
                )
        finally:
            refresh_task.cancel()
            await asyncio.gather(refresh_task, return_exceptions=True)
    finally:
        pool.close()
        await pool.wait_closed()


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
