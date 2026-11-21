"""Signal/execution process boundary contracts.

Signal is the only writer of desired_orders.json. Execution is the only writer
of execution_feedback.json, execution_ledger.json and execution_intents.json.
Signal also owns signal_portfolio.json and theoretical target_positions.
Only execution may place/cancel exchange orders or report actual exchange fills.
"""
from __future__ import annotations

import json
import math
import os
import tempfile
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from . import config as cfg

SCHEMA_VERSION = 1


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds')


def parse_epoch(value: Any) -> float:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str) and value.strip():
        text = value.strip().replace('Z', '+00:00')
        try:
            return datetime.fromisoformat(text).timestamp()
        except ValueError:
            return float('nan')
    return float('nan')


def atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    atomic_write_text(path, json.dumps(payload, ensure_ascii=False, separators=(',', ':'), allow_nan=False))


def atomic_write_text(path: Path, encoded: str) -> None:
    """Readers see either the previous or the next complete document.

    Windows may transiently deny replacing a JSON file that a reader, editor,
    antivirus scanner or another process has just opened. Retry the exact same
    atomic replace briefly; only a persistent denial is raised.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f'.{path.name}.', suffix='.tmp', dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as handle:
            # Encode once in memory and issue one buffered write. json.dump's
            # Python chunk iterator is expensive for a long execution ledger.
            # The flush/fsync/atomic-replace durability boundary is unchanged.
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        for attempt in range(8):
            try:
                os.replace(name, path)
                name = ''
                break
            except PermissionError as exc:
                if attempt >= 7:
                    raise
                time.sleep(0.05 * (attempt + 1))
    finally:
        if name:
            try:
                os.unlink(name)
            except (FileNotFoundError, PermissionError):
                pass


def read_json(path: Path) -> dict[str, Any] | None:
    try:
        with path.open('r', encoding='utf-8') as handle:
            data = json.load(handle)
    except (OSError, ValueError, TypeError):
        return None
    return data if isinstance(data, dict) else None


@dataclass(frozen=True)
class DesiredOrder:
    """One desired LONG limit buy; execution owns the real exchange order id."""
    symbol: str
    order_bar_open_ms: int
    order_bar_available_ms: int
    reference_bar_open_ms: int
    reference_bar_available_ms: int
    limit_price: float
    notional: float
    qv20: float
    observed_price: float
    observed_exchange_time_ms: int
    gap: float
    exit_due_epoch: float
    exit_due_ms: int
    exit_due_utc: str
    reason: str = 'within_place_gap'

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        # Base-asset target quantity; exchange tick/step rounding belongs to execution.
        row['quantity'] = self.notional / self.limit_price
        return row


@dataclass(frozen=True)
class Decision:
    symbol: str
    action: str  # place | keep | cancel(gap only) | withdraw | expire | entry_confirmed
    reason: str
    limit_price: float | None = None
    observed_price: float | None = None
    gap: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class BarStats:
    """Per-bar plan events; safety withdrawals and model expiry stay separate."""
    bar_open_ms: int = 0
    bar_available_ms: int = 0
    candidates: int = 0
    place: int = 0
    keep: int = 0
    cancel: int = 0  # signal price cancellation is disabled; trade rotation is separate
    expire: int = 0
    withdraw: int = 0
    entry_confirmed: int = 0
    pending_confirmation: int = 0
    touched_limit: int = 0
    stale_price_symbols: int = 0
    near_place_candidates: int = 0
    unresolved_capacity: int = 0
    desired_orders: int = 0
    signal_entries: int = 0
    signal_exits: int = 0
    decisions: list[Decision] = field(default_factory=list)

    def reset(self, bar_open_ms: int) -> None:
        self.bar_open_ms = int(bar_open_ms)
        self.bar_available_ms = int(bar_open_ms + 180_000)
        self.candidates = 0
        self.place = 0
        self.keep = 0
        self.cancel = self.expire = self.withdraw = self.entry_confirmed = 0
        self.pending_confirmation = 0
        self.touched_limit = 0
        self.stale_price_symbols = 0
        self.near_place_candidates = 0
        self.unresolved_capacity = 0
        self.desired_orders = 0
        self.signal_entries = self.signal_exits = 0
        self.decisions.clear()

    def add(self, decision: Decision) -> None:
        self.decisions.append(decision)
        if len(self.decisions) > 200:
            del self.decisions[:-200]
        if decision.action == 'place':
            self.place += 1
        elif decision.action == 'keep':
            self.keep += 1
        elif decision.action == 'cancel':
            self.cancel += 1
        elif decision.action in {'expire', 'withdraw', 'entry_confirmed'}:
            setattr(self, decision.action, getattr(self, decision.action) + 1)

    def to_dict(self) -> dict[str, Any]:
        return {
            'bar_open_ms': self.bar_open_ms,
            'bar_available_ms': self.bar_available_ms,
            'candidates': self.candidates,
            'place': self.place,
            'keep': self.keep,
            'cancel': self.cancel,
            'expire': self.expire, 'withdraw': self.withdraw,
            'entry_confirmed': self.entry_confirmed,
            'pending_confirmation': self.pending_confirmation,
            'touched_limit': self.touched_limit,
            'stale_price_symbols': self.stale_price_symbols,
            'near_place_candidates': self.near_place_candidates,
            'unresolved_capacity': self.unresolved_capacity,
            'desired_orders': self.desired_orders,
            'signal_entries': self.signal_entries, 'signal_exits': self.signal_exits,
            'decisions': [d.to_dict() for d in self.decisions[-100:]],
        }


@dataclass(frozen=True)
class ExecutionFeedback:
    """Read-only view of executor-owned state for terminal statistics."""
    positions: dict[str, dict[str, Any]]
    open_orders: dict[str, dict[str, Any]]
    realized_balance: float
    cumulative_pnl: float
    current_unrealized_pnl: float
    updated_at_epoch: float
    fresh: bool
    mode: str = 'unknown'

    @classmethod
    def load(cls, path: Path, max_age_seconds: float, *, now: float | None = None) -> 'ExecutionFeedback':
        payload = read_json(path)
        now_epoch = time.time() if now is None else float(now)
        if payload is None:
            return cls({}, {}, cfg.EQUITY0, 0.0, 0.0, float('nan'), False)
        updated = parse_epoch(payload.get('updated_at_epoch', payload.get('updated_at_utc')))
        fresh = math.isfinite(updated) and 0.0 <= now_epoch - updated <= max_age_seconds
        mode = str(payload.get('mode') or 'unknown')
        if mode not in {'live', 'dry_run'}:
            mode = 'unknown'
        if not fresh:
            # A stale executor snapshot is not current account state.  In
            # particular, never present test/simulation leftovers as live
            # positions or let them block signal candidates.
            return cls({}, {}, cfg.EQUITY0, 0.0, 0.0, updated, False, mode)
        positions = _normalize_rows(payload.get('positions'))
        orders = _normalize_rows(payload.get('open_orders'))
        balance = _finite(payload.get('realized_balance'), cfg.EQUITY0)
        cumulative = balance - cfg.EQUITY0
        current = 0.0
        for row in positions.values():
            price = _finite(row.get('mark_price'), float('nan'))
            entry = _finite(row.get('entry_price'), float('nan'))
            qty = _finite(row.get('quantity'), 0.0)
            if math.isfinite(price) and math.isfinite(entry) and qty:
                current += (price - entry) * qty
        return cls(positions, orders, balance, cumulative, current, updated, fresh, mode)


def _finite(value: Any, default: float) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def _normalize_rows(raw: Any) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    if isinstance(raw, dict):
        rows: Any = raw.values()
    elif isinstance(raw, list):
        rows = raw
    else:
        rows = ()
    for item in rows:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get('symbol', '') or '').strip().upper()
        if symbol:
            out[symbol] = dict(item)
    return out
