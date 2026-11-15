#!/usr/bin/env python3
"""
Build klines_15m from local klines_3m.

This version is scheduled by 15m candle boundaries:

1. Check the latest 3m/15m synthesis status every --check-interval seconds.
2. Compute the next 15m window from the latest fully-covered klines_15m
   timestamp.
3. When that window is due, wait until the latest closed 3m reaches the
   window's final 3m candle.
4. Then check per-symbol coverage; if incomplete, REST-repair missing 3m rows
   immediately and retry.
5. Once complete, aggregate that window and then wait for the next one.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

try:
    import pymysql
    from pymysql.connections import Connection
except ImportError:
    pymysql = None
    Connection = Any  # type: ignore[misc,assignment]

from .kline_startup_manager import (
    DEFAULT_BASE_URL,
    DEFAULT_REQUEST_DELAY_SECONDS,
    DEFAULT_REST_RETRIES,
    DEFAULT_RETRY_DELAY_SECONDS,
    KLINES_3M_TABLE,
    RequestLimiter,
    fetch_klines_range,
    load_3m_columns,
    upsert_3m_rows,
)
from .kline_status import write_15m_status
from v9711_live.environment import load_environment, MYSQL_DEFAULT_PASSWORD


INTERVAL_3M_MS = 3 * 60 * 1000
INTERVAL_15M_MS = 15 * 60 * 1000
CANDLES_PER_15M = 5

DEFAULT_CHECK_INTERVAL_SECONDS = 1
DEFAULT_STATUS_PRINT_INTERVAL_SECONDS = 15.0
DEFAULT_RETRY_INTERVAL_SECONDS = 5
DEFAULT_SYMBOL_FRESHNESS_HOURS = 24
DEFAULT_REPAIR_WORKERS = 8
DEFAULT_REPAIR_GRACE_SECONDS = 45.0
DEFAULT_REPAIR_TASK_COOLDOWN_SECONDS = 60
REPAIR_CACHE_MAX_AGE_SECONDS = 24 * 60 * 60
DEFAULT_MID_REPAIR_PROBE_INTERVAL_SECONDS = 2.0
DEFAULT_EARLY_3M_REPAIR_DELAY_SECONDS = 90.0
DEFAULT_STARTUP_CHECK_WINDOWS = 80
DEFAULT_TRIGGER_POLL_INTERVAL_SECONDS = 0.2
DEPLOY_ROOT = os.environ.get("V9711_DEPLOY_ROOT", str(__import__("pathlib").Path(__file__).resolve().parents[1]))
DEFAULT_SYMBOLS_FILE = os.path.join(DEPLOY_ROOT, "config", "symbols.txt")
DEFAULT_AGGREGATOR_TRIGGER_FILE = os.path.join(DEPLOY_ROOT, "var", "kline_aggregator_15m.trigger")


# In-process dedup: maps (symbol, start_ms, end_ms) → monotonic timestamp of
# last attempt.  Lets the coverage check loop run every retry_interval while
# each REST repair task only fires once per cooldown window.
_REPAIR_ATTEMPTS: dict[tuple[str, int, int], float] = {}


def _filter_repair_tasks(
    tasks: list[tuple[str, int, int]],
    cooldown_seconds: float,
    now: float,
) -> tuple[list[tuple[str, int, int]], int]:
    """Return (pending tasks not in cooldown, skipped count).

    Tasks never seen before are always pending — we deliberately probe the
    cache by membership instead of treating an absent entry as "attempted at
    monotonic time 0", which on a fresh process would falsely mark them
    cooled down for the first ``cooldown_seconds`` of uptime.
    """
    pending: list[tuple[str, int, int]] = []
    skipped = 0
    for task in tasks:
        last = _REPAIR_ATTEMPTS.get(task)
        if last is not None and now - last < cooldown_seconds:
            skipped += 1
            continue
        pending.append(task)
    return pending, skipped


def _record_repair_attempts(tasks: list[tuple[str, int, int]], now: float) -> None:
    for task in tasks:
        _REPAIR_ATTEMPTS[task] = now


def _gc_repair_cache(now: float) -> None:
    """Drop entries older than ``REPAIR_CACHE_MAX_AGE_SECONDS`` to bound memory."""
    cutoff = now - REPAIR_CACHE_MAX_AGE_SECONDS
    if not _REPAIR_ATTEMPTS:
        return
    expired = [key for key, ts in _REPAIR_ATTEMPTS.items() if ts < cutoff]
    for key in expired:
        _REPAIR_ATTEMPTS.pop(key, None)

DEFAULT_MYSQL_HOST = "127.0.0.1"
DEFAULT_MYSQL_PORT = 3306
DEFAULT_MYSQL_USER = "root"
DEFAULT_MYSQL_DATABASE = "binance_futures"


@dataclass(frozen=True)
class Coverage:
    total: int
    complete: int
    missing: list[str]
    required_times: list[int] = field(default_factory=list)
    covered_by_time: dict[int, int] = field(default_factory=dict)
    bar_count_distribution: dict[int, int] = field(default_factory=dict)
    present_by_symbol: dict[str, set[int]] = field(default_factory=dict)

    @property
    def complete_all(self) -> bool:
        return self.total > 0 and self.complete >= self.total and not self.missing


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def utc_iso(dt: datetime | None = None) -> str:
    dt = dt or utc_now()
    return dt.replace(tzinfo=timezone.utc).isoformat(timespec="milliseconds")


def dt_to_ms(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(tzinfo=None)


def ms_label(ms: int | None) -> str:
    return "-" if ms is None else ms_to_dt(ms).strftime("%Y-%m-%d %H:%M:%S")


def seconds_text(seconds: float) -> str:
    if seconds <= 0:
        return "due now"
    return f"{seconds:.0f}s"


def coverage_detail_text(coverage: Coverage) -> str:
    if not coverage.required_times:
        return "bars=- counts=-"
    bars = ",".join(f"{ms_label(open_ms)[11:16]}={coverage.covered_by_time.get(open_ms, 0)}" for open_ms in coverage.required_times)
    counts = ",".join(
        f"{bar_count}bars:{symbol_count}"
        for bar_count, symbol_count in sorted(coverage.bar_count_distribution.items())
    )
    return f"bars={bars} counts={counts or '-'}"


def to_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


def floor_15m(ms: int) -> int:
    return (ms // INTERVAL_15M_MS) * INTERVAL_15M_MS


def require_pymysql() -> None:
    if pymysql is None:
        print("Missing dependency: PyMySQL. Install with `python3 -m pip install pymysql`.", file=sys.stderr)
        raise SystemExit(2)


def connect_mysql(args: argparse.Namespace) -> Connection:
    require_pymysql()
    conn = pymysql.connect(
        host=args.mysql_host,
        port=args.mysql_port,
        user=args.mysql_user,
        password=args.mysql_password,
        database=args.mysql_database,
        charset="utf8mb4",
        autocommit=False,
    )
    ensure_schema(conn)
    return conn


def close_quietly(conn: Connection | None) -> None:
    if conn is None:
        return
    try:
        conn.close()
    except Exception:
        pass


def ensure_schema(conn: Connection) -> None:
    sql = """
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
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def _in_clause(values: list[str]) -> tuple[str, list[str]]:
    if not values:
        return "(NULL)", []
    return "(" + ",".join(["%s"] * len(values)) + ")", values


def get_latest_closed_3m_ms(conn: Connection, anchor_symbol: str = "BTCUSDT") -> int | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT open_time_ms
            FROM klines_3m
            WHERE symbol = %s
              AND is_closed = 1
            ORDER BY open_time_ms DESC
            LIMIT 1
            """,
            (anchor_symbol.upper(),),
        )
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def get_all_symbols(conn: Connection) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT symbol FROM klines_3m ORDER BY symbol ASC")
        rows = cur.fetchall()
    return [str(row[0]).upper() for row in rows]


def filter_symbols_with_recent_bars(conn: Connection, symbols: list[str], cutoff_ms: int) -> list[str]:
    if not symbols:
        return []
    clause, params = _in_clause(symbols)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT symbol
            FROM klines_3m
            WHERE symbol IN {clause}
              AND open_time_ms >= %s
              AND is_closed = 1
            GROUP BY symbol
            ORDER BY symbol ASC
            """,
            params + [cutoff_ms],
        )
        rows = cur.fetchall()
    return [str(row[0]).upper() for row in rows]


def read_symbols_file(path: str) -> list[str]:
    file_path = __import__("pathlib").Path(path).expanduser()
    if not file_path.is_file():
        raise RuntimeError(f"symbols file does not exist: {file_path}")
    symbols = sorted({line.strip().upper() for line in file_path.read_text(encoding="utf-8").splitlines()
                      if line.strip() and not line.lstrip().startswith("#")})
    if not symbols:
        raise RuntimeError(f"symbols file is empty: {file_path}")
    return symbols


def get_target_symbols(conn: Connection, args: argparse.Namespace) -> list[str]:
    if getattr(args, "symbols_file", ""):
        return read_symbols_file(args.symbols_file)
    if args.symbols:
        return sorted({symbol.upper() for symbol in args.symbols})

    all_symbols = get_all_symbols(conn)
    if args.all_symbols:
        return all_symbols

    latest_3m_ms = get_latest_closed_3m_ms(conn, args.anchor_symbol)
    if latest_3m_ms is None:
        return []
    cutoff_ms = latest_3m_ms - int(args.symbol_freshness_hours * 60 * 60 * 1000)
    return filter_symbols_with_recent_bars(conn, all_symbols, cutoff_ms)


def latest_complete_15m_ms(
    conn: Connection,
    symbols: list[str],
    min_open_ms: int | None = None,
) -> int | None:
    if not symbols:
        return None
    clause, params = _in_clause(symbols)
    min_filter = ""
    query_params: list[Any] = list(params)
    if min_open_ms is not None:
        min_filter = "AND open_time_ms >= %s"
        query_params.append(int(min_open_ms))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT open_time_ms, COUNT(DISTINCT symbol) AS symbol_count
            FROM klines_15m
            WHERE symbol IN {clause}
              {min_filter}
              AND is_closed = 1
            GROUP BY open_time_ms
            HAVING symbol_count = %s
            ORDER BY open_time_ms DESC
            LIMIT 1
            """,
            query_params + [len(symbols)],
        )
        row = cur.fetchone()
    return int(row[0]) if row else None


def latest_any_15m_ms(
    conn: Connection,
    symbols: list[str],
    min_open_ms: int | None = None,
) -> int | None:
    if not symbols:
        return None
    clause, params = _in_clause(symbols)
    min_filter = ""
    query_params: list[Any] = list(params)
    if min_open_ms is not None:
        min_filter = "AND open_time_ms >= %s"
        query_params.append(int(min_open_ms))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT MAX(open_time_ms)
            FROM klines_15m
            WHERE symbol IN {clause}
              {min_filter}
              AND is_closed = 1
            """,
            query_params,
        )
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def next_window_ms(conn: Connection, symbols: list[str], anchor_symbol: str = "BTCUSDT") -> int | None:
    latest_complete = latest_complete_15m_ms(conn, symbols)
    if latest_complete is not None:
        return latest_complete + INTERVAL_15M_MS

    latest_any = latest_any_15m_ms(conn, symbols)
    if latest_any is not None:
        return latest_any

    latest_3m = get_latest_closed_3m_ms(conn, anchor_symbol)
    if latest_3m is None:
        return None
    return floor_15m(latest_3m)


def coverage_for_window(conn: Connection, symbols: list[str], window_ms: int) -> Coverage:
    if not symbols:
        return Coverage(total=0, complete=0, missing=[])
    clause, params = _in_clause(symbols)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT symbol, open_time_ms
            FROM klines_3m
            WHERE symbol IN {clause}
              AND open_time_ms >= %s
              AND open_time_ms < %s
              AND is_closed = 1
            ORDER BY symbol ASC, open_time_ms ASC
            """,
            params + [window_ms, window_ms + INTERVAL_15M_MS],
        )
        rows = cur.fetchall()

    required_times = [window_ms + index * INTERVAL_3M_MS for index in range(CANDLES_PER_15M)]
    required_set = set(required_times)
    by_symbol: dict[str, set[int]] = {symbol: set() for symbol in symbols}
    covered_by_time = {open_ms: 0 for open_ms in required_times}
    for row in rows:
        symbol = str(row[0]).upper()
        open_ms = int(row[1])
        if symbol not in by_symbol or open_ms not in required_set:
            continue
        if open_ms not in by_symbol[symbol]:
            covered_by_time[open_ms] += 1
        by_symbol[symbol].add(open_ms)

    complete_symbols: set[str] = set()
    bar_count_distribution: dict[int, int] = {}
    for symbol, open_times in by_symbol.items():
        bar_count = len(open_times)
        bar_count_distribution[bar_count] = bar_count_distribution.get(bar_count, 0) + 1
        if open_times >= required_set:
            complete_symbols.add(symbol)

    missing = sorted(set(symbols) - complete_symbols)
    return Coverage(
        total=len(symbols),
        complete=len(complete_symbols),
        missing=missing,
        required_times=required_times,
        covered_by_time=covered_by_time,
        bar_count_distribution=bar_count_distribution,
        present_by_symbol=by_symbol,
    )


def missing_symbols_for_3m(conn: Connection, symbols: list[str], open_ms: int) -> list[str]:
    if not symbols:
        return []
    clause, params = _in_clause(symbols)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT symbol
            FROM klines_3m
            WHERE symbol IN {clause}
              AND open_time_ms = %s
              AND is_closed = 1
            GROUP BY symbol
            """,
            params + [int(open_ms)],
        )
        present = {str(row[0]).upper() for row in cur.fetchall()}
    return sorted(set(symbols) - present)


def repair_missing_3m_bar(
    conn: Connection,
    args: argparse.Namespace,
    symbols: list[str],
    open_ms: int,
    limiter: RequestLimiter,
) -> int:
    missing = missing_symbols_for_3m(conn, symbols, open_ms)
    if not missing:
        return 0

    all_tasks = [(symbol, int(open_ms), int(open_ms)) for symbol in missing]
    now = time.monotonic()
    _gc_repair_cache(now)
    cooldown_seconds = float(getattr(args, "repair_task_cooldown", DEFAULT_REPAIR_TASK_COOLDOWN_SECONDS))
    tasks, cooled = _filter_repair_tasks(all_tasks, cooldown_seconds, now)
    if not tasks:
        print(
            f"{utc_iso()} early 3m repair skipped: all {len(all_tasks)} task(s) in cooldown "
            f"({cooldown_seconds:.0f}s window) for {ms_label(open_ms)}",
            flush=True,
        )
        return 0
    _record_repair_attempts(tasks, now)

    columns = load_3m_columns(conn, args.mysql_database, KLINES_3M_TABLE)
    workers = min(max(1, int(getattr(args, "repair_workers", DEFAULT_REPAIR_WORKERS))), len(tasks))
    print(
        f"{utc_iso()} early repairing missing 3m open={ms_label(open_ms)} "
        f"symbols={len(missing)} tasks={len(tasks)} skipped_cooled={cooled}/{len(all_tasks)} workers={workers}",
        flush=True,
    )

    def _do_task(symbol: str, start_ms: int, end_ms: int) -> tuple[str, list[list[Any]], Exception | None]:
        try:
            rows = fetch_klines_range(
                args.base_url, symbol, "3m", INTERVAL_3M_MS, start_ms, end_ms, limiter,
            )
            return symbol, rows, None
        except Exception as exc:
            return symbol, [], exc

    inserted_total = 0
    errors = 0
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="agg-early-repair") as pool:
        future_map = {pool.submit(_do_task, *task): task for task in tasks}
        for future in as_completed(future_map):
            symbol, rows, err = future.result()
            if err is not None:
                errors += 1
                print(
                    f"{utc_iso()} early repair fetch failed {symbol} {ms_label(open_ms)}: {err}",
                    file=sys.stderr,
                    flush=True,
                )
                continue
            if rows:
                inserted_total += upsert_3m_rows(conn, args.mysql_database, KLINES_3M_TABLE, symbol, rows, columns)
    print(
        f"{utc_iso()} early repair done open={ms_label(open_ms)} rows={inserted_total} errors={errors}",
        flush=True,
    )
    return inserted_total


def missing_segments_for_symbol(open_times: set[int], required_times: list[int]) -> list[tuple[int, int]]:
    missing_times = [open_ms for open_ms in required_times if open_ms not in open_times]
    if not missing_times:
        return []
    segments: list[tuple[int, int]] = []
    start = prev = missing_times[0]
    for open_ms in missing_times[1:]:
        if open_ms == prev + INTERVAL_3M_MS:
            prev = open_ms
            continue
        segments.append((start, prev))
        start = prev = open_ms
    segments.append((start, prev))
    return segments


def repair_missing_3m_for_coverage(
    conn: Connection,
    args: argparse.Namespace,
    coverage: Coverage,
    window_ms: int,
    limiter: RequestLimiter,
) -> tuple[int, bool]:
    """Return (rows_inserted, coverage_recovered).

    ``coverage_recovered`` is True when the time-based probe observed full
    coverage during the run — the caller should skip its retry sleep and
    re-check coverage immediately so the 15m status file is written without
    the usual retry-interval delay.
    """
    if not coverage.missing or not coverage.required_times:
        return 0, False
    columns = load_3m_columns(conn, args.mysql_database, KLINES_3M_TABLE)
    all_tasks: list[tuple[str, int, int]] = []
    for symbol in coverage.missing:
        open_times = coverage.present_by_symbol.get(symbol, set())
        for start_ms, end_ms in missing_segments_for_symbol(open_times, coverage.required_times):
            all_tasks.append((symbol, start_ms, end_ms))
    if not all_tasks:
        return 0, False

    now = time.monotonic()
    _gc_repair_cache(now)
    cooldown_seconds = float(getattr(args, "repair_task_cooldown", DEFAULT_REPAIR_TASK_COOLDOWN_SECONDS))
    tasks, cooled = _filter_repair_tasks(all_tasks, cooldown_seconds, now)
    if not tasks:
        print(
            f"{utc_iso()} repair skipped: all {len(all_tasks)} task(s) in cooldown "
            f"({cooldown_seconds:.0f}s window) for {ms_label(window_ms)}",
            flush=True,
        )
        return 0, False

    _record_repair_attempts(tasks, now)

    workers = max(1, int(getattr(args, "repair_workers", DEFAULT_REPAIR_WORKERS)))
    workers = min(workers, len(tasks))
    print(
        f"{utc_iso()} repairing missing 3m in parallel tasks={len(tasks)} "
        f"(skipped_cooled={cooled}/{len(all_tasks)}) "
        f"symbols={len(coverage.missing)} workers={workers}",
        flush=True,
    )

    probe_enabled = bool(getattr(args, "mid_repair_abort", True))
    probe_interval = float(getattr(args, "mid_repair_probe_interval", DEFAULT_MID_REPAIR_PROBE_INTERVAL_SECONDS))

    # Shared cancellation state between the time-based probe thread and the
    # REST worker threads:
    #   abort_event        — workers bail (set by probe on recovery OR by
    #                        the outer finally on normal exit)
    #   coverage_recovered — set ONLY by probe on coverage recovery; lets the
    #                        caller distinguish "WS filled in" from "we
    #                        finished naturally" and skip its retry sleep
    #   missing_set        — symbols still incomplete; workers skip if filled
    # The probe thread is the sole authority on missing_set updates so the
    # main thread can do its work (upserts) without holding the lock.
    abort_event = threading.Event()
    coverage_recovered = threading.Event()
    missing_lock = threading.Lock()
    missing_set: set[str] = set(coverage.missing)

    def _probe_loop() -> None:
        # Independent timer probe.  Crucially, uses its OWN connection so it
        # bypasses the parent transaction's REPEATABLE READ snapshot and can
        # actually see writes the WS process has committed while our REST
        # workers are stuck in retries.
        probe_conn: Connection | None = None
        try:
            try:
                probe_conn = connect_mysql(args)
            except Exception as exc:
                print(
                    f"{utc_iso()} mid-repair probe connect failed: {exc}",
                    file=sys.stderr,
                    flush=True,
                )
                return
            while not abort_event.is_set():
                if abort_event.wait(probe_interval):
                    return
                try:
                    probe_conn.rollback()
                    with missing_lock:
                        snapshot = list(missing_set)
                    if not snapshot:
                        abort_event.set()
                        return
                    fresh = coverage_for_window(probe_conn, snapshot, window_ms)
                except Exception as probe_err:
                    print(
                        f"{utc_iso()} mid-repair probe failed: {probe_err}",
                        file=sys.stderr,
                        flush=True,
                    )
                    continue
                if fresh.complete_all:
                    with missing_lock:
                        recovered = len(missing_set)
                        missing_set.clear()
                    coverage_recovered.set()
                    abort_event.set()
                    print(
                        f"{utc_iso()} probe: coverage recovered for {ms_label(window_ms)} "
                        f"({recovered} symbols newly complete); aborting REST workers",
                        flush=True,
                    )
                    return
                with missing_lock:
                    missing_set.intersection_update(fresh.missing)
        finally:
            close_quietly(probe_conn)

    probe_thread: threading.Thread | None = None
    if probe_enabled:
        probe_thread = threading.Thread(target=_probe_loop, daemon=True, name="agg-probe")
        probe_thread.start()

    def _do_task(
        symbol: str, start_ms: int, end_ms: int
    ) -> tuple[str, int, int, list[list[Any]], Exception | None, str]:
        if abort_event.is_set():
            return symbol, start_ms, end_ms, [], None, "aborted"
        with missing_lock:
            in_missing = symbol in missing_set
        if not in_missing:
            return symbol, start_ms, end_ms, [], None, "filled"
        try:
            rows = fetch_klines_range(
                args.base_url, symbol, "3m", INTERVAL_3M_MS, start_ms, end_ms, limiter,
            )
            return symbol, start_ms, end_ms, rows, None, "ok"
        except Exception as exc:
            return symbol, start_ms, end_ms, [], exc, "error"

    total_inserted = 0
    completed = 0
    succeeded = 0
    errors = 0
    skipped_aborted = 0
    skipped_filled = 0
    last_progress_at = time.monotonic()
    progress_log_interval = 5.0

    pool: ThreadPoolExecutor | None = None
    try:
        pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="agg-repair")
        future_map = {
            pool.submit(_do_task, symbol, start_ms, end_ms): (symbol, start_ms, end_ms)
            for symbol, start_ms, end_ms in tasks
        }
        pending = set(future_map)
        while pending:
            if coverage_recovered.is_set():
                skipped_aborted += len(tasks) - completed
                print(
                    f"{utc_iso()} repair aborting wait for {ms_label(window_ms)}: "
                    f"coverage already recovered; pending={len(tasks) - completed}",
                    flush=True,
                )
                break
            done, pending = wait(
                pending,
                timeout=max(0.2, probe_interval),
                return_when=FIRST_COMPLETED,
            )
            if not done:
                continue
            for future in done:
                if coverage_recovered.is_set():
                    skipped_aborted += len(tasks) - completed
                    print(
                        f"{utc_iso()} repair aborting wait for {ms_label(window_ms)}: "
                        f"coverage already recovered; pending={len(tasks) - completed}",
                        flush=True,
                    )
                    pending.clear()
                    break
                symbol, start_ms, end_ms, rows, err, status = future.result()
                completed += 1
                if status == "aborted":
                    skipped_aborted += 1
                elif status == "filled":
                    skipped_filled += 1
                elif status == "error":
                    errors += 1
                    print(
                        f"{utc_iso()} repair fetch failed {symbol} {ms_label(start_ms)}..{ms_label(end_ms)} "
                        f"({completed}/{len(tasks)}): {err}",
                        file=sys.stderr,
                        flush=True,
                    )
                elif rows:
                    inserted = upsert_3m_rows(
                        conn, args.mysql_database, KLINES_3M_TABLE, symbol, rows, columns,
                    )
                    total_inserted += inserted
                    succeeded += 1

                now_t = time.monotonic()
                if completed == len(tasks) or now_t - last_progress_at >= progress_log_interval:
                    print(
                        f"{utc_iso()} repair progress {completed}/{len(tasks)} for {ms_label(window_ms)} "
                        f"ok={succeeded} rows={total_inserted} errors={errors} "
                        f"skipped_filled={skipped_filled} skipped_aborted={skipped_aborted}",
                        flush=True,
                    )
                    last_progress_at = now_t
    finally:
        abort_event.set()
        if pool is not None:
            pool.shutdown(wait=not coverage_recovered.is_set(), cancel_futures=True)
        if probe_thread is not None:
            probe_thread.join(timeout=5.0)

    return total_inserted, coverage_recovered.is_set()


def fetch_3m_candles_for_window(conn: Connection, symbol: str, window_ms: int) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                open_time_ms,
                open_price, high_price, low_price, close_price,
                volume, quote_volume, trade_count,
                taker_buy_volume, taker_buy_quote_volume,
                close_time_ms, is_closed
            FROM klines_3m
            WHERE symbol = %s
              AND open_time_ms >= %s
              AND open_time_ms < %s
            ORDER BY open_time_ms ASC
            """,
            (symbol, window_ms, window_ms + INTERVAL_15M_MS),
        )
        return cur.fetchall()


def fetch_3m_candles_for_window_many(conn: Connection, symbols: list[str], window_ms: int) -> dict[str, list[tuple]]:
    if not symbols:
        return {}
    clause, params = _in_clause(symbols)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                symbol,
                open_time_ms,
                open_price, high_price, low_price, close_price,
                volume, quote_volume, trade_count,
                taker_buy_volume, taker_buy_quote_volume,
                close_time_ms, is_closed
            FROM klines_3m
            WHERE symbol IN {clause}
              AND open_time_ms >= %s
              AND open_time_ms < %s
            ORDER BY symbol ASC, open_time_ms ASC
            """,
            params + [window_ms, window_ms + INTERVAL_15M_MS],
        )
        rows = cur.fetchall()

    out: dict[str, list[tuple]] = {symbol.upper(): [] for symbol in symbols}
    for row in rows:
        symbol = str(row[0]).upper()
        out.setdefault(symbol, []).append(tuple(row[1:]))
    return out


def aggregate_to_15m(symbol: str, candles_3m: list[tuple]) -> list[tuple]:
    if not candles_3m:
        return []

    windows: dict[int, list[tuple]] = {}
    for candle in candles_3m:
        key = floor_15m(int(candle[0]))
        windows.setdefault(key, []).append(candle)

    rows: list[tuple] = []
    for win_ms in sorted(windows):
        group = sorted(windows[win_ms], key=lambda row: int(row[0]))
        first = group[0]
        last = group[-1]

        open_p = to_decimal(first[1])
        high_p = max(to_decimal(row[2]) for row in group)
        low_p = min(to_decimal(row[3]) for row in group)
        close_p = to_decimal(last[4])

        volume = sum(to_decimal(row[5]) for row in group)
        q_volume = sum(to_decimal(row[6]) for row in group)
        trades = sum(int(row[7]) for row in group)
        tb_vol = sum(to_decimal(row[8]) for row in group)
        tb_qvol = sum(to_decimal(row[9]) for row in group)

        close_ms = int(last[10])
        is_closed = 1 if len(group) == CANDLES_PER_15M and int(last[11]) == 1 else 0

        rows.append(
            (
                symbol,
                win_ms,
                ms_to_dt(win_ms),
                close_ms,
                ms_to_dt(close_ms),
                open_p,
                high_p,
                low_p,
                close_p,
                volume,
                q_volume,
                trades,
                tb_vol,
                tb_qvol,
                is_closed,
            )
        )
    return rows


def upsert_15m_rows(conn: Connection, rows: list[tuple]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO klines_15m (
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
    conn.commit()
    return len(rows)


def aggregate_window(conn: Connection, symbols: list[str], window_ms: int) -> int:
    started = time.monotonic()
    candles_by_symbol = fetch_3m_candles_for_window_many(conn, symbols, window_ms)
    fetch_elapsed = time.monotonic() - started

    rows_15m: list[tuple] = []
    for symbol in symbols:
        rows_15m.extend(aggregate_to_15m(symbol, candles_by_symbol.get(symbol.upper(), [])))

    upsert_started = time.monotonic()
    total = upsert_15m_rows(conn, rows_15m)
    print(
        f"{utc_iso()} aggregated {len(symbols)}/{len(symbols)} symbols for {ms_label(window_ms)} "
        f"rows={total} fetch={fetch_elapsed:.2f}s upsert={time.monotonic() - upsert_started:.2f}s",
        flush=True,
    )
    return total


def trigger_file_mtime(path: str) -> float:
    if not path:
        return 0.0
    try:
        return os.path.getmtime(path)
    except OSError:
        return 0.0


def trigger_file_summary(path: str) -> str:
    if not path:
        return "disabled"
    try:
        with open(path, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except Exception:
        return os.path.basename(path)
    event = str(payload.get("event") or "trigger")
    stream = str(payload.get("stream") or "-")
    detail = str(payload.get("detail") or "")
    return f"{event} stream={stream}" + (f" detail={detail}" if detail else "")


def sleep_interruptibly(
    seconds: float,
    should_stop,
    trigger_file: str = "",
    trigger_state: dict[str, float] | None = None,
    trigger_poll_interval: float = DEFAULT_TRIGGER_POLL_INTERVAL_SECONDS,
) -> bool:
    deadline = time.monotonic() + max(0.0, seconds)
    while not should_stop() and time.monotonic() < deadline:
        time.sleep(min(max(0.05, trigger_poll_interval), deadline - time.monotonic()))
        if trigger_file and trigger_state is not None:
            mtime = trigger_file_mtime(trigger_file)
            if mtime > trigger_state.get("mtime", 0.0):
                trigger_state["mtime"] = mtime
                print(
                    f"{utc_iso()} aggregator trigger received: "
                    f"{trigger_file_summary(trigger_file)}; checking now",
                    flush=True,
                )
                return True
    return False


def wait_for_coverage_and_aggregate(
    args: argparse.Namespace,
    symbols: list[str],
    window_ms: int,
    should_stop,
) -> bool:
    required_last_3m_ms = window_ms + (CANDLES_PER_15M - 1) * INTERVAL_3M_MS
    limiter = RequestLimiter(
        args.request_delay,
        max_retries=args.rest_retries,
        retry_delay=args.retry_delay,
    )
    while not should_stop():
        conn: Connection | None = None
        try:
            conn = connect_mysql(args)
            latest_3m_ms = get_latest_closed_3m_ms(conn, args.anchor_symbol)
            if latest_3m_ms is None or latest_3m_ms < required_last_3m_ms:
                print(
                    f"{utc_iso()} now={utc_now():%Y-%m-%d %H:%M:%S} "
                    f"waiting for latest 3m before coverage check: "
                    f"latest_3m={ms_label(latest_3m_ms)} required={ms_label(required_last_3m_ms)} "
                    f"for 15m {ms_label(window_ms)}; retry in {args.retry_interval}s",
                    flush=True,
                )
                close_quietly(conn)
                conn = None
                sleep_interruptibly(
                    args.retry_interval,
                    should_stop,
                    args.trigger_file,
                    args._trigger_state,
                    args.trigger_poll_interval,
                )
                continue

            coverage = coverage_for_window(conn, symbols, window_ms)
            if coverage.complete_all:
                total = aggregate_window(conn, symbols, window_ms)
                write_15m_status(
                    open_time_ms=window_ms,
                    symbols=symbols,
                    row_count=total,
                    source="kline_aggregator_15m",
                )
                print(
                    f"{utc_iso()} now={utc_now():%Y-%m-%d %H:%M:%S} "
                    f"window {ms_label(window_ms)} complete; "
                    f"upserted {total} rows",
                    flush=True,
                )
                return True

            sample = ",".join(coverage.missing[:10])
            suffix = "..." if len(coverage.missing) > 10 else ""
            print(
                f"{utc_iso()} now={utc_now():%Y-%m-%d %H:%M:%S} "
                f"waiting for 3m coverage {coverage.complete}/{coverage.total} "
                f"for {ms_label(window_ms)}; missing={sample}{suffix}; "
                f"{coverage_detail_text(coverage)}; "
                f"retry in {args.retry_interval}s",
                flush=True,
            )
            repaired_rows = 0
            if args.repair_missing_3m:
                repair_grace_seconds = max(0.0, float(getattr(args, "repair_grace_seconds", DEFAULT_REPAIR_GRACE_SECONDS)))
                seconds_since_due = (dt_to_ms(utc_now()) - (window_ms + INTERVAL_15M_MS)) / 1000.0
                if seconds_since_due < repair_grace_seconds:
                    wait_seconds = min(float(args.retry_interval), repair_grace_seconds - seconds_since_due)
                    print(
                        f"{utc_iso()} repair grace active for {ms_label(window_ms)}: "
                        f"since_due={seconds_since_due:.1f}s grace={repair_grace_seconds:.1f}s; "
                        f"wait {wait_seconds:.1f}s before REST repair",
                        flush=True,
                    )
                    close_quietly(conn)
                    conn = None
                    sleep_interruptibly(
                        wait_seconds,
                        should_stop,
                        args.trigger_file,
                        args._trigger_state,
                        args.trigger_poll_interval,
                    )
                    continue
                repaired_rows, coverage_recovered = repair_missing_3m_for_coverage(
                    conn, args, coverage, window_ms, limiter,
                )
                if repaired_rows > 0:
                    print(
                        f"{utc_iso()} repaired missing 3m rows={repaired_rows}; rechecking {ms_label(window_ms)}",
                        flush=True,
                    )
                    continue
                if coverage_recovered:
                    # Probe detected WS/peer process filled the gap.  Skip
                    # the retry sleep and re-check coverage immediately so
                    # the 15m status file is written without delay.
                    print(
                        f"{utc_iso()} coverage recovered during repair for {ms_label(window_ms)}; "
                        f"rechecking immediately",
                        flush=True,
                    )
                    continue
                # repaired_rows == 0 and probe did not signal recovery.  Cause
                # is either "all tasks were cooled down" (already logged inside
                # the call) or "REST returned no new rows".  Either way, sleep
                # retry_interval and let the next check pick up either fresh
                # websocket data or expired cooldowns.
                print(
                    f"{utc_iso()} no new 3m rows from repair for {ms_label(window_ms)}; "
                    f"retry in {args.retry_interval}s",
                    flush=True,
                )
        except Exception as exc:
            print(f"{utc_iso()} coverage/aggregation failed: {exc}", file=sys.stderr, flush=True)
        finally:
            close_quietly(conn)

        sleep_interruptibly(
            args.retry_interval,
            should_stop,
            args.trigger_file,
            args._trigger_state,
            args.trigger_poll_interval,
        )
    return False


def should_print_status(last_print_at: float, interval_seconds: float) -> bool:
    return last_print_at <= 0 or time.monotonic() - last_print_at >= max(0.0, interval_seconds)


def check_status(conn: Connection, symbols: list[str], anchor_symbol: str) -> tuple[int | None, int | None, int | None]:
    latest_3m = get_latest_closed_3m_ms(conn, anchor_symbol)
    latest_15m = latest_complete_15m_ms(conn, symbols)
    next_ms = next_window_ms(conn, symbols, anchor_symbol)
    return latest_3m, latest_15m, next_ms


def due_early_3m_opens(window_ms: int, now_ms: int, delay_seconds: float) -> list[int]:
    delay_ms = int(max(0.0, delay_seconds) * 1000)
    out: list[int] = []
    for index in range(CANDLES_PER_15M - 1):
        open_ms = int(window_ms) + index * INTERVAL_3M_MS
        close_due_ms = open_ms + INTERVAL_3M_MS
        if now_ms >= close_due_ms + delay_ms:
            out.append(open_ms)
    return out


def prune_old_rows(conn: Connection, args: argparse.Namespace, latest_3m_ms: int | None = None) -> None:
    """Keep both aggregate and source tables within the one-day deployment window."""
    now_mono = time.monotonic()
    if now_mono < float(getattr(args, "_next_prune_mono", 0.0)):
        return
    args._next_prune_mono = now_mono + 60.0
    if latest_3m_ms is None:
        latest_3m_ms = get_latest_closed_3m_ms(conn, args.anchor_symbol)
    if latest_3m_ms is None:
        return
    cutoff = int(latest_3m_ms) - int(max(1.0, float(args.retention_hours)) * 3600 * 1000)
    with conn.cursor() as cur:
        for table in ("klines_3m", "klines_15m"):
            try:
                cur.execute(f"DELETE FROM `{table}` WHERE open_time_ms < %s LIMIT 10000", (cutoff,))
            except Exception:
                # The 3m collector owns its own cleanup; 15m is created by this process.
                if table == "klines_15m":
                    raise
    conn.commit()


def startup_min_15m_ms(latest_3m_ms: int, args: argparse.Namespace) -> int | None:
    if args.startup_check_all:
        return None
    windows = max(1, int(args.startup_check_windows))
    latest_window = floor_15m(latest_3m_ms)
    retention_min = latest_window - int(max(1.0, float(args.retention_hours)) * 3600 * 1000)
    bounded_min = latest_window - (windows - 1) * INTERVAL_15M_MS
    return max(0, retention_min, bounded_min)


def initialize_cursor(
    conn: Connection,
    args: argparse.Namespace,
) -> tuple[list[str], int | None, int | None, int | None]:
    """Find the startup cursor, bounded to recent windows unless requested."""
    symbols = get_target_symbols(conn, args)
    if not symbols:
        return [], None, None, None

    latest_3m = get_latest_closed_3m_ms(conn, args.anchor_symbol)
    if latest_3m is None:
        return symbols, None, None, None

    min_open_ms = startup_min_15m_ms(latest_3m, args)
    latest_complete = latest_complete_15m_ms(conn, symbols, min_open_ms=min_open_ms)
    if latest_complete is not None:
        next_after_complete = latest_complete + INTERVAL_15M_MS
        lookback_windows = max(0, int(args.startup_lookback_windows))
        target_ms = max(0, next_after_complete - lookback_windows * INTERVAL_15M_MS)
        if min_open_ms is not None:
            target_ms = max(min_open_ms, target_ms)
        return symbols, latest_3m, latest_complete, target_ms

    latest_any = latest_any_15m_ms(conn, symbols, min_open_ms=min_open_ms)
    if latest_any is not None:
        return symbols, latest_3m, None, latest_any

    return symbols, latest_3m, None, min_open_ms if min_open_ms is not None else floor_15m(latest_3m)


def parse_args(argv: list[str]) -> argparse.Namespace:
    load_environment()
    parser = argparse.ArgumentParser(
        description="Schedule 3m to 15m aggregation from local MySQL data."
    )
    parser.add_argument("--mysql-host", default=os.environ.get("MYSQL_HOST", DEFAULT_MYSQL_HOST))
    parser.add_argument("--mysql-port", type=int, default=int(os.environ.get("MYSQL_PORT", str(DEFAULT_MYSQL_PORT))))
    parser.add_argument("--mysql-user", default=os.environ.get("MYSQL_USER", DEFAULT_MYSQL_USER))
    parser.add_argument("--mysql-password", default=os.environ.get("MYSQL_PASSWORD", MYSQL_DEFAULT_PASSWORD))
    parser.add_argument("--mysql-database", default=os.environ.get("MYSQL_DATABASE", DEFAULT_MYSQL_DATABASE))
    parser.add_argument("--symbols-file", default=os.environ.get("SYMBOLS_FILE", DEFAULT_SYMBOLS_FILE),
                        help="Local strategy symbol list; default is config/symbols.txt.")
    parser.add_argument("--retention-hours", type=float, default=float(os.environ.get("KLINE_RETENTION_HOURS", "24")),
                        help="Only inspect and retain this many recent hours; default 24.")
    parser.add_argument("--base-url", default=os.environ.get("BINANCE_BASE_URL", DEFAULT_BASE_URL))
    parser.add_argument("--check-interval", type=int, default=DEFAULT_CHECK_INTERVAL_SECONDS)
    parser.add_argument(
        "--status-print-interval",
        type=float,
        default=DEFAULT_STATUS_PRINT_INTERVAL_SECONDS,
        help=(
            "Seconds between pre-due scheduler status prints. Checks still run "
            f"every --check-interval. Default: {DEFAULT_STATUS_PRINT_INTERVAL_SECONDS}."
        ),
    )
    parser.add_argument("--retry-interval", type=int, default=DEFAULT_RETRY_INTERVAL_SECONDS)
    parser.add_argument("--request-delay", type=float, default=DEFAULT_REQUEST_DELAY_SECONDS)
    parser.add_argument(
        "--repair-workers",
        type=int,
        default=DEFAULT_REPAIR_WORKERS,
        help=f"Concurrent REST fetch workers for missing 3m repair. Default: {DEFAULT_REPAIR_WORKERS}",
    )
    parser.add_argument(
        "--repair-grace-seconds",
        type=float,
        default=DEFAULT_REPAIR_GRACE_SECONDS,
        help=(
            "Wait this many seconds after the 15m close before starting REST "
            "repair for missing 3m rows, giving websocket/database writes time "
            f"to finish. Default: {DEFAULT_REPAIR_GRACE_SECONDS}."
        ),
    )
    parser.add_argument(
        "--repair-task-cooldown",
        type=float,
        default=DEFAULT_REPAIR_TASK_COOLDOWN_SECONDS,
        help=(
            "Per-(symbol, start_ms, end_ms) cooldown in seconds.  After a repair "
            "attempt, the same task is skipped until this many seconds have passed, "
            "so the coverage check loop stays fast and repairs don't pile up. "
            f"Default: {DEFAULT_REPAIR_TASK_COOLDOWN_SECONDS}."
        ),
    )
    parser.add_argument(
        "--mid-repair-abort",
        dest="mid_repair_abort",
        action="store_true",
        default=True,
        help=(
            "While repair tasks are in flight, periodically re-check 3m "
            "coverage; if WS/peer process has filled the gap, cancel all "
            "remaining pending REST tasks immediately.  Default: on."
        ),
    )
    parser.add_argument(
        "--no-mid-repair-abort",
        dest="mid_repair_abort",
        action="store_false",
        help="Disable the mid-repair coverage abort (always run all submitted tasks).",
    )
    parser.add_argument(
        "--mid-repair-probe-interval",
        type=float,
        default=DEFAULT_MID_REPAIR_PROBE_INTERVAL_SECONDS,
        help=(
            "Minimum seconds between mid-repair coverage probes.  "
            f"Default: {DEFAULT_MID_REPAIR_PROBE_INTERVAL_SECONDS}."
        ),
    )
    parser.add_argument(
        "--early-3m-repair-delay",
        type=float,
        default=DEFAULT_EARLY_3M_REPAIR_DELAY_SECONDS,
        help=(
            "Before a 15m window is due, check each closed child 3m bar after "
            "this many seconds and REST-repair missing symbols early. Set <0 "
            f"to disable. Default: {DEFAULT_EARLY_3M_REPAIR_DELAY_SECONDS}."
        ),
    )
    parser.add_argument("--rest-retries", type=int, default=DEFAULT_REST_RETRIES)
    parser.add_argument("--retry-delay", type=float, default=DEFAULT_RETRY_DELAY_SECONDS)
    parser.add_argument("--symbol-freshness-hours", type=float, default=DEFAULT_SYMBOL_FRESHNESS_HOURS)
    parser.add_argument(
        "--startup-lookback-windows",
        type=int,
        default=2,
        help=(
            "On startup, re-check/re-aggregate at most this many recent 15m "
            "windows before the next unconfirmed window.  After startup the "
            "scheduler advances from its in-memory cursor instead of running "
            "full-history progress checks. Default: 2."
        ),
    )
    parser.add_argument(
        "--startup-check-windows",
        type=int,
        default=96,
        help=(
            "Limit the startup klines_15m progress scan to this many recent "
            "15m windows. This bounds restart cost; use --startup-check-all "
            f"for the old full-history scan. Default: 96 (24h)."
        ),
    )
    parser.add_argument(
        "--startup-check-all",
        action="store_true",
        help="On startup, scan all historical klines_15m progress before choosing the cursor.",
    )
    parser.add_argument(
        "--trigger-file",
        default=os.environ.get("KLINE_AGGREGATOR_TRIGGER_FILE", DEFAULT_AGGREGATOR_TRIGGER_FILE),
        help=(
            "Local file watched during sleeps. When binance_futures_3m_ws.py "
            "updates it after a reconnect/error, this process wakes and runs "
            "one scheduler check. Set empty to disable."
        ),
    )
    parser.add_argument(
        "--trigger-poll-interval",
        type=float,
        default=DEFAULT_TRIGGER_POLL_INTERVAL_SECONDS,
        help=(
            "Seconds between trigger-file probes while sleeping. "
            f"Default: {DEFAULT_TRIGGER_POLL_INTERVAL_SECONDS}."
        ),
    )
    parser.add_argument("--anchor-symbol", default="BTCUSDT", help="Symbol used for cheap latest-3m probes.")
    parser.add_argument("--all-symbols", action="store_true", help="Use every historical symbol in klines_3m.")
    parser.add_argument("--symbols", nargs="+", default=None, metavar="SYMBOL")
    parser.add_argument(
        "--repair-missing-3m",
        dest="repair_missing_3m",
        action="store_true",
        default=True,
        help="Immediately REST-repair missing 3m rows before retrying aggregation. Default: on.",
    )
    parser.add_argument(
        "--no-repair-missing-3m",
        dest="repair_missing_3m",
        action="store_false",
        help="Only wait for websocket/database coverage; do not REST-repair missing 3m rows.",
    )
    parser.add_argument("--once", action="store_true", help="Run one due aggregation cycle and exit.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    stopped = False
    args._trigger_state = {"mtime": trigger_file_mtime(args.trigger_file)}

    def handle_stop(_signum: int, _frame: Any) -> None:
        nonlocal stopped
        stopped = True

    def should_stop() -> bool:
        return stopped

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    print(
        f"Kline aggregator scheduler (3m to 15m) mysql://{args.mysql_user}@"
        f"{args.mysql_host}:{args.mysql_port}/{args.mysql_database} "
        f"check={args.check_interval}s retry={args.retry_interval}s "
        f"startup_lookback={args.startup_lookback_windows} "
        f"startup_check={'all' if args.startup_check_all else str(args.startup_check_windows)} "
        f"trigger={args.trigger_file or 'disabled'}",
        flush=True,
    )

    symbols: list[str] = []
    latest_complete_15m: int | None = None
    target_window_ms: int | None = None
    early_checked_3m: set[int] = set()
    last_status_print_at = 0.0
    conn: Connection | None = None
    try:
        conn = connect_mysql(args)
        symbols, latest_3m, latest_complete_15m, target_window_ms = initialize_cursor(conn, args)
        prune_old_rows(conn, args, latest_3m)
        startup_scan_from = (
            "all"
            if args.startup_check_all
            else ms_label(startup_min_15m_ms(latest_3m, args) if latest_3m is not None else None)
        )
        print(
            f"{utc_iso()} startup cursor "
            f"symbols={len(symbols)} latest_3m={ms_label(latest_3m)} "
            f"startup_scan_from={startup_scan_from} "
            f"latest_complete_15m={ms_label(latest_complete_15m)} "
            f"next_window={ms_label(target_window_ms)}",
            flush=True,
        )
    except Exception as exc:
        print(f"{utc_iso()} startup cursor failed: {exc}", file=sys.stderr, flush=True)
    finally:
        close_quietly(conn)

    while not stopped:
        conn: Connection | None = None
        try:
            if not symbols:
                print(f"{utc_iso()} no target symbols; retry in {args.check_interval}s", flush=True)
                wait_seconds = args.check_interval
            else:
                conn = connect_mysql(args)
                latest_3m = get_latest_closed_3m_ms(conn, args.anchor_symbol)
                prune_old_rows(conn, args, latest_3m)
                if target_window_ms is None:
                    print(f"{utc_iso()} no 3m data yet; retry in {args.check_interval}s", flush=True)
                    wait_seconds = args.check_interval
                else:
                    due_ms = target_window_ms + INTERVAL_15M_MS
                    now_ms = dt_to_ms(utc_now())
                    seconds_until_due = (due_ms - now_ms) / 1000.0
                    if seconds_until_due <= 0 or should_print_status(last_status_print_at, args.status_print_interval):
                        print(
                            f"{utc_iso()} now={utc_now():%Y-%m-%d %H:%M:%S} "
                            f"symbols={len(symbols)} latest_3m={ms_label(latest_3m)} "
                            f"latest_complete_15m={ms_label(latest_complete_15m)} "
                            f"next_window={ms_label(target_window_ms)} due={ms_label(due_ms)} "
                            f"until_due={seconds_text(seconds_until_due)}",
                            flush=True,
                        )
                        last_status_print_at = time.monotonic()

                    if seconds_until_due > 0:
                        early_delay = float(getattr(args, "early_3m_repair_delay", DEFAULT_EARLY_3M_REPAIR_DELAY_SECONDS))
                        if early_delay >= 0:
                            limiter = RequestLimiter(
                                args.request_delay,
                                max_retries=args.rest_retries,
                                retry_delay=args.retry_delay,
                            )
                            for child_open_ms in due_early_3m_opens(target_window_ms, now_ms, early_delay):
                                if child_open_ms in early_checked_3m:
                                    continue
                                if latest_3m is not None and int(latest_3m) < int(child_open_ms):
                                    continue
                                early_checked_3m.add(child_open_ms)
                                try:
                                    repair_missing_3m_bar(conn, args, symbols, child_open_ms, limiter)
                                except Exception as exc:
                                    print(
                                        f"{utc_iso()} early 3m repair failed open={ms_label(child_open_ms)}: {exc}",
                                        file=sys.stderr,
                                        flush=True,
                                    )
                        wait_seconds = min(float(args.check_interval), seconds_until_due)
                    else:
                        close_quietly(conn)
                        conn = None
                        ok = wait_for_coverage_and_aggregate(args, symbols, target_window_ms, should_stop)
                        if args.once:
                            break
                        if not ok:
                            break
                        latest_complete_15m = target_window_ms
                        target_window_ms += INTERVAL_15M_MS
                        early_checked_3m = {value for value in early_checked_3m if value >= target_window_ms}
                        continue
        except KeyboardInterrupt:
            stopped = True
            wait_seconds = 0.0
        except Exception as exc:
            print(f"{utc_iso()} check failed: {exc}", file=sys.stderr, flush=True)
            wait_seconds = args.retry_interval
        finally:
            close_quietly(conn)

        if args.once or stopped:
            break
        sleep_interruptibly(
            wait_seconds,
            should_stop,
            args.trigger_file,
            args._trigger_state,
            args.trigger_poll_interval,
        )

    print("Stopped.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
