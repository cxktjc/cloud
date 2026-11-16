"""Market data readers: local 3m status, incremental MySQL bars, price JSON."""
from __future__ import annotations

import math
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from . import config as cfg
from .local_poll import CachedJsonFile


@dataclass(frozen=True)
class Bar:
    """One completed 3m bar. open_time_ms is open time, not close time."""
    open_time_ms: int
    open: float
    high: float
    low: float
    close: float
    quote_volume: float

    @property
    def available_time_ms(self) -> int:
        return self.open_time_ms + cfg.BAR_MS

    @property
    def available_time_utc(self) -> str:
        return datetime.fromtimestamp(self.available_time_ms / 1000, tz=timezone.utc).isoformat(timespec='seconds')


@dataclass(frozen=True)
class KlineStatus:
    open_time_ms: int
    symbol_count: int
    row_count: int
    symbols: tuple[str, ...]
    updated_at: str
    source: str

    @property
    def signature(self) -> tuple[int, int, int, str, str]:
        return (self.open_time_ms, self.symbol_count, self.row_count, self.updated_at, self.source)

    @property
    def updated_epoch(self) -> float:
        try:
            return datetime.fromisoformat(self.updated_at.replace('Z', '+00:00')).timestamp()
        except (ValueError, AttributeError):
            return float('nan')


@dataclass(frozen=True)
class PriceQuote:
    symbol: str
    price: float
    exchange_time_ms: int
    snapshot_epoch: float
    snapshot_sequence: int


@dataclass(frozen=True)
class PriceSnapshot:
    sequence: int
    updated_at_utc: str
    last_success_epoch: float
    status: str
    prices: Mapping[str, PriceQuote]


class StatusReader:
    """Read the atomic local 3m status JSON written by the kline collector."""

    def __init__(self, path: Path = cfg.STATUS_FILE):
        self.path = Path(path)
        self._json = CachedJsonFile(self.path)
        self._parsed_revision = -1
        self._current: KlineStatus | None = None
        self.last_signature: tuple[int, int, int, str, str] | None = None
        self.last_status: KlineStatus | None = None

    def read(self, *, only_new: bool = False) -> KlineStatus | None:
        data = self._json.read()
        if self._json.revision != self._parsed_revision:
            self._parsed_revision = self._json.revision
            self._current = None
            try:
                if not isinstance(data, dict) or int(data.get('schema_version', 0) or 0) != 1:
                    return None
                symbols = tuple(sorted({str(s).strip().upper() for s in data.get('symbols', []) if str(s).strip()}))
                self._current = KlineStatus(
                    open_time_ms=int(data['open_time_ms']),
                    symbol_count=int(data.get('symbol_count') or len(symbols)),
                    row_count=int(data.get('row_count') or 0),
                    symbols=symbols,
                    updated_at=str(data.get('updated_at') or ''),
                    source=str(data.get('source') or 'unknown'),
                )
            except (KeyError, TypeError, ValueError, OverflowError):
                return None
        status = self._current
        if status is None:
            return None
        if (status.signature == self.last_signature and self.last_status is not None
                and status.symbols == self.last_status.symbols):
            return None if only_new else status
        self.last_signature = status.signature
        self.last_status = status
        return status


class PriceReader:
    """Read the local ticker snapshot and de-duplicate by price/timestamp content.

    The collector may advance its sequence or refresh JSON metadata while all
    symbol prices remain identical.  Those writes are healthy but are not new
    market input, so ``read(only_new=True)`` returns ``None`` until at least one
    symbol's price or exchange timestamp changes.  ``last_read_healthy`` and
    ``last_success_epoch`` remain available to the control loop for fail-closed
    freshness checks and display.
    """

    def __init__(self, path: Path = cfg.PRICE_FILE,
                 max_age_seconds: float = cfg.PRICE_MAX_AGE_SECONDS,
                 error_grace_seconds: float = cfg.PRICE_ERROR_GRACE_SECONDS):
        self.path = Path(path)
        self._json = CachedJsonFile(self.path)
        self._parsed_revision = -1
        self._parsed_snapshot: PriceSnapshot | None = None
        self._parsed_signature: tuple | None = None
        self._parse_error = ''
        self.max_age_seconds = float(max_age_seconds)
        self.error_grace_seconds = float(error_grace_seconds)
        self.last_sequence = -1
        self.last_snapshot: PriceSnapshot | None = None
        self.last_content_signature: tuple[tuple[str, float, int], ...] | None = None
        self.last_read_healthy = False
        self.last_read_error = ''
        self.last_success_epoch = float('nan')

    def read(self, *, only_new: bool = False) -> PriceSnapshot | None:
        self.last_read_healthy = False
        data = self._json.read()
        if self._json.revision != self._parsed_revision:
            self._parsed_revision = self._json.revision
            self._parsed_snapshot = None
            self._parse_error = self._json.error
            try:
                self._parse_snapshot(data)
            except (TypeError, ValueError, OverflowError) as exc:
                self._parse_error = f'{type(exc).__name__}: {exc}'
        snapshot = self._parsed_snapshot
        if snapshot is None:
            self.last_read_error = self._parse_error or 'invalid price snapshot'
            return None
        # Time-based safety must be checked even for identical cached bytes.
        age = time.time() - snapshot.last_success_epoch
        healthy = snapshot.status == 'ok'
        limit = self.max_age_seconds if healthy else min(self.max_age_seconds, self.error_grace_seconds)
        if (not math.isfinite(snapshot.last_success_epoch) or age < -5.0 or age > limit):
            self.last_read_error = f'stale/invalid snapshot age={age:.3f}s status={snapshot.status}'
            return None
        self.last_read_healthy = True
        self.last_read_error = ''
        changed = self._parsed_signature != self.last_content_signature
        self.last_snapshot = snapshot
        self.last_sequence = snapshot.sequence
        self.last_content_signature = self._parsed_signature
        return snapshot if changed or not only_new else None

    def _parse_snapshot(self, data: Any) -> None:
        if not isinstance(data, dict) or int(data.get('schema_version', 0) or 0) != 1:
            self._parse_error = 'invalid price snapshot schema'
            return
        sequence = int(data.get('sequence', -1) or -1)
        success_epoch = _finite(data.get('last_success_epoch'), float('nan'))
        self.last_success_epoch = success_epoch
        raw_prices = data.get('prices')
        if not isinstance(raw_prices, dict) or not raw_prices:
            self._parse_error = 'price snapshot is empty'
            return
        prices: dict[str, PriceQuote] = {}
        for symbol_raw, row in raw_prices.items():
            symbol = str(symbol_raw).strip().upper()
            if not symbol or not isinstance(row, dict):
                continue
            price = _finite(row.get('price'), float('nan'))
            exchange_ms = int(_finite(row.get('exchange_time_ms'), 0.0))
            if math.isfinite(price) and price > 0.0 and exchange_ms > 0:
                prices[symbol] = PriceQuote(symbol, price, exchange_ms, success_epoch, sequence)
        if not prices:
            self._parse_error = 'price snapshot has no valid rows'
            return
        self._parsed_signature = tuple(sorted(
            (symbol, quote.price, quote.exchange_time_ms) for symbol, quote in prices.items()))
        self._parsed_snapshot = PriceSnapshot(sequence, str(data.get('updated_at_utc') or ''),
                                              success_epoch, str(data.get('status') or ''), prices)


class BarStore:
    """Bootstrap a small rolling window, then fetch only newly reported bars."""

    def __init__(self, *, history_bars: int = cfg.HISTORY_BARS,
                 batch_size: int = cfg.MYSQL_BATCH_SIZE):
        if history_bars < cfg.QV_WINDOW + cfg.QV_MIN_PERIODS:
            raise ValueError('history_bars must cover qv20 plus reconciliation')
        self.history_bars = int(history_bars)
        self.batch_size = int(batch_size)
        self.bars: dict[str, list[Bar]] = defaultdict(list)
        self.last_open_ms = 0
        self._has_closed_column: bool | None = None
        self.revision = 0

    def connect(self):
        import pymysql
        return pymysql.connect(
            host=cfg.MYSQL_HOST, port=cfg.MYSQL_PORT,
            user=cfg.MYSQL_USER, password=cfg.MYSQL_PASSWORD,
            database=cfg.MYSQL_DATABASE, charset='utf8mb4',
            connect_timeout=15, read_timeout=300,
        )

    def _closed_filter(self, cursor) -> str:
        if self._has_closed_column is None:
            cursor.execute('SHOW COLUMNS FROM klines_3m LIKE %s', ('is_closed',))
            self._has_closed_column = cursor.fetchone() is not None
        return ' AND is_closed = 1' if self._has_closed_column else ''

    def update(self, status: KlineStatus, *, now_epoch: float | None = None) -> int:
        """Read only completed bars up to status bar, returning rows merged."""
        target_ms = int(status.open_time_ms)
        if target_ms % cfg.BAR_MS:
            raise ValueError('3m status open_time_ms is not aligned')
        symbols = status.symbols
        if not symbols:
            return 0
        # The status writer publishes only after the closed batch has persisted.
        # Status write time is checked against open_time + 3m to reject a future
        # or not-yet-completed bar; the local clock gets a 5s skew allowance.
        now_epoch = time.time() if now_epoch is None else float(now_epoch)
        status_epoch = status.updated_epoch
        available_epoch = target_ms / 1000 + cfg.BAR_MS / 1000
        if math.isfinite(status_epoch) and status_epoch < available_epoch:
            return 0
        if now_epoch < available_epoch - 5.0:
            return 0
        start_ms = target_ms - (self.history_bars - 1) * cfg.BAR_MS
        # Per-symbol cursors let a late symbol catch up after the first status
        # batch, without re-reading the full universe or skipping missing rows.
        groups: dict[int, list[str]] = defaultdict(list)
        for symbol in symbols:
            series = self.bars.get(symbol, ())
            from_ms = max(start_ms, series[-1].open_time_ms + cfg.BAR_MS) if series else start_ms
            if from_ms <= target_ms:
                groups[from_ms].append(symbol)
        if not groups:
            return 0
        conn = self.connect()
        rows_added = 0
        try:
            with conn.cursor() as cur:
                closed_sql = self._closed_filter(cur)
                for from_ms, group_symbols in sorted(groups.items()):
                    for p0 in range(0, len(group_symbols), self.batch_size):
                        batch = group_symbols[p0:p0 + self.batch_size]
                        placeholders = ','.join(['%s'] * len(batch))
                        query = (
                            'SELECT symbol, open_time_ms, open_price, high_price, low_price, '
                            'close_price, quote_volume FROM klines_3m FORCE INDEX (PRIMARY) '
                            f'WHERE symbol IN ({placeholders}) AND open_time_ms >= %s '
                            f'AND open_time_ms <= %s{closed_sql} '
                            'ORDER BY symbol, open_time_ms'
                        )
                        cur.execute(query, tuple(batch) + (from_ms, target_ms))
                        for symbol, open_ms, op, hi, lo, cl, qv in cur.fetchall():
                            bar = Bar(
                                int(open_ms), _finite(op, float('nan')), _finite(hi, float('nan')),
                                _finite(lo, float('nan')), _finite(cl, float('nan')), _finite(qv, 0.0))
                            if not math.isfinite(bar.close) or bar.open_time_ms % cfg.BAR_MS:
                                continue
                            series = self.bars[str(symbol).upper()]
                            if series and series[-1].open_time_ms >= bar.open_time_ms:
                                if series[-1].open_time_ms == bar.open_time_ms:
                                    series[-1] = bar
                                continue
                            series.append(bar)
                            rows_added += 1
        finally:
            conn.close()
        cutoff = start_ms
        for symbol, series in list(self.bars.items()):
            trimmed = [bar for bar in series if bar.open_time_ms >= cutoff]
            if trimmed:
                self.bars[symbol] = trimmed
            else:
                del self.bars[symbol]
        self.last_open_ms = max(self.last_open_ms, target_ms)
        if rows_added:
            self.revision += 1
        return rows_added

    def read_window(self, symbols: Iterable[str], start_ms: int, end_ms: int) -> dict[str, list[Bar]]:
        """Bounded restart catch-up, without disturbing the current rolling cache."""
        universe = sorted({str(s).upper() for s in symbols})
        result: dict[str, list[Bar]] = defaultdict(list)
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                closed_sql = self._closed_filter(cur)
                for p0 in range(0, len(universe), self.batch_size):
                    batch = universe[p0:p0 + self.batch_size]
                    placeholders = ','.join(['%s'] * len(batch))
                    cur.execute(
                        'SELECT symbol, open_time_ms, open_price, high_price, low_price, '
                        'close_price, quote_volume FROM klines_3m FORCE INDEX (PRIMARY) '
                        f'WHERE symbol IN ({placeholders}) AND open_time_ms >= %s '
                        f'AND open_time_ms <= %s{closed_sql} ORDER BY symbol, open_time_ms',
                        tuple(batch) + (int(start_ms), int(end_ms)))
                    for symbol, ms, op, hi, lo, cl, qv in cur.fetchall():
                        if int(ms) % cfg.BAR_MS:
                            continue
                        result[str(symbol).upper()].append(Bar(
                            int(ms), _finite(op, float('nan')), _finite(hi, float('nan')),
                            _finite(lo, float('nan')), _finite(cl, float('nan')), _finite(qv, 0)))
        finally:
            conn.close()
        return dict(result)

    @staticmethod
    def qv20(series: Iterable[Bar], target_open_ms: int) -> float:
        """Past-only rolling mean: bars open < target, min_periods=10, zero fill."""
        values = [bar.quote_volume if math.isfinite(bar.quote_volume) else 0.0
                  for bar in series if bar.open_time_ms < target_open_ms]
        values = values[-cfg.QV_WINDOW:]
        if len(values) < cfg.QV_MIN_PERIODS:
            return float('nan')
        return sum(values) / len(values)

    def latest_open(self, symbol: str, up_to_ms: int) -> Bar | None:
        for bar in reversed(self.bars.get(symbol.upper(), ())):
            if bar.open_time_ms <= up_to_ms:
                return bar
        return None


def _finite(value: Any, default: float) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default
