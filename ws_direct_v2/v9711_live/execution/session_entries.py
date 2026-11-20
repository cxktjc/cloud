"""Read-only, process-local accounting of signal entries without exact trade fills.

A new LiveTrade owns a fresh tracker. Never restore this state from a ledger or
feedback file. Entry time is the signal's completed-bar confirmation time, not an
invented intrabar fill time. Positive partial fills count as having opened.
"""
from __future__ import annotations

import math
import time
from collections.abc import Mapping
from typing import Any

from .. import config as cfg
from .position_identity import is_manual_position
from .entry_timing import EntryTimings


def positive(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return 0.0
    return number if math.isfinite(number) and number > 0 else 0.0


class SessionEntryStats:
    """Observe only validated signal snapshots and executor-owned fill evidence.

    ACKs, aggregate account positions, quotes and model touches are NOT fills.
    A fill with a position_id matches that exact identity; otherwise symbol +
    order bar matches the signal entry bar. Never match by symbol alone.
    """

    def __init__(self, *, started_at: float | None = None):
        self.started_at = time.time() if started_at is None else float(started_at)
        self._entries: dict[tuple[str, str], dict[str, Any]] = {}
        self._active_keys: set[tuple[str, str]] = set()
        self._filled_ids: set[tuple[str, str]] = set()
        self._filled_bars: set[tuple[str, int]] = set()
        self._cache: dict[str, Any] | None = None
        self._cache_limit = -1
        self.timings = EntryTimings()
        self._cache_timing_revision = -1

    def observe_fill(self, symbol: str, row: Mapping[str, Any], *, quantity: Any) -> None:
        """Caller supplies a booked, positive, exact-owned ENTRY fill only."""
        symbol = str(symbol).upper().strip()
        if is_manual_position(row) or not symbol or not positive(quantity):
            return
        identity = (row.get('entry_client_order_id') or row.get('client_order_id')
                    or row.get('entry_order_id') or row.get('order_id'))
        if not identity or str(identity) == '0':
            return
        position_id = str(row.get('position_id') or '')
        bar = int(positive(row.get('order_bar_open_ms') or row.get('entry_bar_open_ms')))
        if position_id:
            key, evidence = (symbol, position_id), self._filled_ids
        elif bar:
            key, evidence = (symbol, bar), self._filled_bars
        else:
            return
        if key not in evidence:
            evidence.add(key)
            self._cache = None

    def observe_owned_state(self, positions: Mapping[str, Any],
                            tracked_orders: Mapping[str, Any]) -> None:
        """Seed/recheck booked evidence without treating a submitted order as filled."""
        for symbol, row in positions.items():
            if isinstance(row, Mapping):
                self.observe_fill(symbol, row, quantity=row.get('quantity'))
        for symbol, row in tracked_orders.items():
            if isinstance(row, Mapping):
                self.observe_fill(symbol, row, quantity=row.get('accounted_entry_qty'))

    @staticmethod
    def _entry(row: Any, session_id: str) -> tuple[tuple[str, str], dict[str, Any]] | None:
        if not isinstance(row, Mapping) or not positive(row.get('quantity')):
            return None
        symbol = str(row.get('symbol') or '').upper().strip()
        bar = int(positive(row.get('entry_bar_open_ms')))
        available = int(positive(row.get('entry_available_ms'))) or bar + cfg.BAR_MS
        if not symbol or not bar or available != bar + cfg.BAR_MS:
            return None
        position_id = str(row.get('position_id') or '')
        key = (session_id, position_id or f'{symbol}:{bar}')
        return key, dict(symbol=symbol, position_id=position_id,
                         signal_session_id=session_id, entry_bar_open_ms=bar,
                         entry_available_ms=available,
                         exit_due_ms=int(positive(row.get('exit_due_ms'))),
                         exit_available_ms=int(positive(row.get('exit_available_ms'))))

    def observe_signal(self, snapshot: Mapping[str, Any], *, status: str, now: float) -> None:
        """Invalid/stale/unready input never creates entries or ends existing ones.

        Pre-start confirmations (including warmup) are excluded, even when first
        received after start. recent_trades recovers entries that both opened and
        closed between two reads, within the publisher's retained history.
        """
        if status != 'valid' or not snapshot.get('signal_ready'):
            return
        book = snapshot.get('portfolio') or {}
        if not isinstance(book, Mapping):
            return
        session_id = str(snapshot.get('signal_session_id') or book.get('session_id') or '')
        targets = snapshot.get('target_positions')
        if not isinstance(targets, list) or not session_id:
            return
        recent = book.get('recent_trades') or []
        if not isinstance(recent, list):
            recent = []
        observed: dict[tuple[str, str], dict[str, Any]] = {}
        # Active targets win over an inconsistent duplicate in recent_trades.
        for rows, active in ((recent, False), (targets, True)):
            for row in rows:
                parsed = self._entry(row, session_id)
                if parsed is None:
                    continue
                key, entry = parsed
                if not self.started_at * 1000 < entry['entry_available_ms'] <= now * 1000:
                    continue
                if not active and (not entry['exit_available_ms']
                                   or entry['exit_available_ms'] > now * 1000):
                    continue
                entry['signal_active'] = active
                observed[key] = entry
        for key, entry in observed.items():
            if self._entries.get(key) != entry:
                self._entries[key] = entry
                self._cache = None
        # This is a complete valid target snapshot, not a quote or short read gap.
        # Absence can mean close OR a signal data correction; do not invent a fill.
        active_keys = {key for key, entry in observed.items() if entry['signal_active']}
        for key in self._active_keys - active_keys:
            if self._entries[key]['signal_active']:
                self._entries[key]['signal_active'] = False
                self._cache = None
        self._active_keys = active_keys

    def _has_fill(self, entry: Mapping[str, Any]) -> bool:
        symbol = entry['symbol']
        return ((bool(entry['position_id'])
                 and (symbol, entry['position_id']) in self._filled_ids)
                or (symbol, entry['entry_bar_open_ms']) in self._filled_bars)

    def snapshot(self, *, max_rows: int = 12) -> dict[str, Any]:
        """Stable, cached display data; no clocks/ages that force periodic redraws."""
        limit = max(0, int(max_rows))
        timing_revision = self.timings.revision
        if (self._cache is not None and self._cache_limit == limit
                and self._cache_timing_revision == timing_revision):
            return self._cache
        missing = [entry for entry in self._entries.values() if not self._has_fill(entry)]
        active = sum(bool(entry['signal_active']) for entry in missing)
        missing.sort(key=lambda row: (row['entry_available_ms'], row['symbol'], row['position_id']),
                     reverse=True)
        self._cache = {
            'started_at_epoch': self.started_at,
            'scope': 'signal_entry_confirmation_after_trade_start; exact_owned_positive_fill',
            'signal_entries': len(self._entries),
            'trade_filled_entries': len(self._entries) - len(missing),
            'unfilled_entries': len(missing),
            'active_unfilled_entries': active,
            'ended_unfilled_entries': len(missing) - active,
            'recent_unfilled_entries': [dict(entry, first_order_timing=self.timings.first_for(entry))
                                       for entry in missing[:limit]],
            'details_truncated': len(missing) > limit,
        }
        self._cache_limit = limit
        self._cache_timing_revision = timing_revision
        return self._cache
