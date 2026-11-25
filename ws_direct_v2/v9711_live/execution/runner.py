"""Persistent v9711 live trade executor.

Reads:
    state/desired_orders.json          signal-owned desired near-limit candidates
    var/binance_ticker_price/latest.json  local high-frequency price snapshot
    var/binance_account_stream/*       exact order fills / statuses (read-only)
Writes (execution-owned only):
    state/execution_intents.json       exact registered order identities
    state/execution_ledger.json        tracked orders, owned positions, balances
    state/execution_feedback.json      signal terminal's read-only view

Safety:
    - no aggregate positionRisk ownership inference;
    - only exact ``cfg.ORDER_CLIENT_PREFIX_*`` client/order identities are cancelled or closed;
    - eight ordinary pending entries plus two signal-follow entries; ordinary
      entries use the three-position limit and final pre-exit bar exception;
      signal-follow entries stop and cancel at six owned positions;
    - timed QUEUE close starts 1s before the signal-provided ``exit_due_epoch``;
      up to twelve 5s rounds, each cancelled and confirmed before replacement,
      then MARKET for the owned residual;
    - a symbol is blocked from reopening for 60s after a close;
    - persistent live mode polls signal/local fills on the state-owner thread;
      independent broker I/O runs on bounded workers, never under a state lock.
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import time
from datetime import datetime, timezone
from typing import Any, Mapping

from .. import config as cfg
from ..contracts import atomic_write_json, parse_epoch, utc_now_iso
from ..local_poll import CachedJsonFile, LocalChangePoller, probe_sleep
from ..market_data import PriceReader
from .client import DisabledExchange, ExchangeClient, OwnershipLedger
from .logic import select_physical_orders
from .pending_rotation import PendingRotationMixin
from ..entry_policy import GapPolicy
from .close_ladder import CloseLongMixin
from . import close_ladder as close_policy
from .stats import compute_comparison
from .cancel_pacing import ENTRY_CANCEL_MIN_AGE_SECONDS, exact_entry_cancel_key
from .session_entries import SessionEntryStats
from .nonblocking_io import NetworkTradeMixin
from .submit_errors import (require_submit_ack, submit_error_is_ambiguous,
                            EntryQuoteUnavailable, EntrySendGateRejection)
from .entry_diagnostics import submission_diagnostic
from .order_identity import matches_order_identity, exact_entry_fill_time_ms
from .entry_quotes import QUOTE_FIELDS
from .stream import StreamUnavailable
from .terminal import TradeTerminalView, MarketConsoleFilter
from .forced_close import ForcedCloseMixin
from .manual_close import ManualCloseMixin

LOG = logging.getLogger('v9711_live_trade')

TERMINAL = {'FILLED', 'CANCELED', 'CANCELLED', 'EXPIRED',
            'EXPIRED_IN_MATCH', 'REJECTED'}


class LiveTrade(ManualCloseMixin, ForcedCloseMixin, PendingRotationMixin, CloseLongMixin, NetworkTradeMixin):
    def __init__(self, *, once: bool = False, enabled: bool | None = None,
                 dry_run: bool | None = None, nonblocking_io: bool | None = None):
        if (cfg.STATE_DIR / '.manual_signal_sync_pending.json').exists():
            raise RuntimeError('manual signal ledger sync interrupted; rerun scripts.sync_signal_to_trade --apply')
        self.session_entries = SessionEntryStats(started_at=time.time())
        cfg.STATE_DIR.mkdir(parents=True, exist_ok=True)
        self.once = bool(once)
        self._network_io = None
        self._io_signal = ({}, 'unreadable')
        self._entry_plan_buffer = None
        self.client = ExchangeClient(
            enabled=cfg.TRADE_ENABLED if enabled is None else enabled,
            dry_run=cfg.DRY_RUN if dry_run is None else dry_run,
            account_stream_dir=cfg.ACCOUNT_STREAM_DIR,
            exchange_info_cache_file=cfg.EXCHANGE_INFO_CACHE_FILE,
            price_file=cfg.PRICE_FILE)
        self.client.entry_timings = self.session_entries.timings
        self.ledger = OwnershipLedger(
            path=cfg.EXECUTION_INTENT_FILE,
            state_path=cfg.EXECUTION_LEDGER_FILE)
        self.ledger.state.setdefault('entry_submit_rejections', {})
        self._entry_gate_retries: dict[str, tuple[int, int, float]] = {}
        self.ledger.state.setdefault('entry_recovery', {})
        self.ledger.state.setdefault('pending_rotation', None)
        self.ledger.state.setdefault('rotated_out_plans', {})
        self.session_entries.observe_owned_state(self.positions, self.tracked)
        self.price = PriceReader(cfg.PRICE_FILE)
        self.view = TradeTerminalView(
            min_refresh_seconds=cfg.TradeViewConfig().min_refresh_seconds,
            heartbeat_seconds=cfg.ViewConfig().heartbeat_seconds,
            max_rows=cfg.ViewConfig().max_orders_display)
        self._desired_reader = CachedJsonFile(cfg.DESIRED_FILE)
        self.last_desired_key: tuple[Any, ...] | None = None
        self.last_valid_snapshot: dict[str, Any] | None = None
        self.last_valid_desired_mono = float('-inf')
        self.cycles = 0
        self._preflight_bar = 0
        self._rejected_preflight: set[tuple] = set()
        self.stats = {'place': 0, 'cancel': 0, 'close': 0, 'fill': 0,
                      'rotation_defer': 0, 'foreign_ignored': 0,
                      'entry_rejected': 0, 'close_rejected': 0, 'entry_retries': 0}
        if nonblocking_io is None:
            nonblocking_io = (not self.once and self.client.enabled and not self.client.dry_run
                and os.environ.get('V9711_LIVE_NONBLOCKING_IO', '1').strip().lower()
                in {'1', 'true', 'yes', 'on'})
        if nonblocking_io:
            self.enable_nonblocking_io()

    @property
    def tracked(self) -> dict[str, dict[str, Any]]:
        return self.ledger.state['tracked_orders']

    def _ordinary_pending_count(self) -> int:
        return sum(not row.get('signal_follow') and not row.get('cancel_requested')
                   for row in self.tracked.values())

    def _entry_client_id(self, row: Mapping[str, Any]) -> str:
        return self.ledger.client_id(str(row.get('symbol') or '').upper(),
                                    int(self._finite(row.get('order_bar_open_ms'))), 'entry')

    @property
    def positions(self) -> dict[str, dict[str, Any]]:
        return self.ledger.state['owned_positions']

    def _book_initial_equity(self) -> float:
        value = self._finite(self.ledger.state.get('book_initial_equity'), cfg.EQUITY0)
        return value if value > 0 else cfg.EQUITY0

    @staticmethod
    def _finite(value: Any, default: float = 0.0) -> float:
        try:
            out = float(value)
        except (TypeError, ValueError):
            return default
        return out if math.isfinite(out) else default

    def desired_snapshot(self, *, now: float) -> tuple[dict[str, Any], str]:
        """Read the complete signal-authoritative target snapshot."""
        if getattr(self, '_desired_reader', None) is None:
            self._desired_reader = CachedJsonFile(cfg.DESIRED_FILE)
        payload = self._desired_reader.read()
        if not isinstance(payload, dict) or self._finite(payload.get('schema_version')) != 1:
            return self._fresh_fallback(), 'unreadable'
        generated = self._finite(payload.get('generated_at_epoch'), float('nan'))
        expires = self._finite(payload.get('expires_at_epoch'), float('nan'))
        if (not math.isfinite(generated) or not math.isfinite(expires)
                or now - generated > cfg.DESIRED_TTL_SECONDS or now > expires):
            return self._fresh_fallback(), 'expired'
        rows = payload.get('desired')
        contract = payload.get('execution_contract') or {}
        if (not isinstance(rows, list) or not isinstance(contract, dict)
                or self._finite(payload.get('contract_version')) < 2
                or payload.get('state_authority') != 'signal'
                or contract.get('positions_authority') != 'target_positions'):
            return self._fresh_fallback(), 'incompatible_contract'
        self.last_valid_snapshot = dict(payload)
        self.last_valid_desired_mono = time.monotonic()
        return dict(payload), 'valid'

    def _fresh_fallback(self) -> dict[str, Any]:
        """Keep the last complete target snapshot through a short read gap."""
        if (self.last_valid_snapshot
                and time.monotonic() - self.last_valid_desired_mono
                <= min(cfg.DESIRED_TTL_SECONDS, 45.0)):
            return dict(self.last_valid_snapshot)
        return {'signal_ready': False, 'desired': [], 'target_positions': [],
                'portfolio': {}, 'execution_contract': {}}

    def refresh_prices(self, *, now: float | None = None) -> dict[str, float]:
        now = time.time() if now is None else float(now)
        snapshot = self.price.read()
        if snapshot is None:
            return {}
        return {symbol: q.price for symbol, q in snapshot.prices.items()
                if -5 <= now - q.exchange_time_ms / 1000 <= cfg.PRICE_EXCHANGE_MAX_AGE_SECONDS}

    def refresh_entry_prices(self, *, now=None):
        return self.refresh_prices(now=now)

    def reconcile_orders(self, *, now: float) -> None:
        """Advance exact-order state from the local account stream + own fallback."""
        for symbol, row in list(self.tracked.items()):
            query = getattr(self.client, 'query_exact_order', None)
            if callable(query):
                order, known = query(
                    self.ledger, symbol=symbol, order_id=row.get('order_id'),
                    client_order_id=str(row.get('client_order_id') or ''), now=now)
            else:
                legacy = self.client.exact_order(
                    self.ledger, symbol=symbol, order_id=row.get('order_id'),
                    client_order_id=str(row.get('client_order_id') or ''))
                order, known = legacy, legacy is not None
            if not known:
                continue
            if not order:
                self._note_entry_absence(symbol, row, now=now)
                self._retire_order(symbol, row, 'confirmed_absent')
                continue
            if not matches_order_identity(order, symbol=symbol,
                    client_order_id=row.get('client_order_id'), order_id=row.get('order_id')):
                LOG.warning('ignore mismatched entry evidence; identity retained symbol=%s', symbol)
                continue
            # Recover the acknowledgement of an ambiguous placement before a
            # cancel. Subsequent cancellation can use its exact exchange ID and
            # must not run an extra UID lookup every executor cycle.
            recovered_id = str(order.get('orderId') or '')
            if recovered_id and (recovered_id != str(row.get('order_id') or '')
                                 or row.get('fate_unknown')):
                row['order_id'] = recovered_id
                row['fate_unknown'] = False
                self.ledger.register(
                    symbol=symbol, client_order_id=str(row.get('client_order_id') or ''),
                    order_id=recovered_id, kind='entry',
                    limit_price=self._finite(row.get('limit_price')),
                    qty=self._finite(row.get('qty')),
                    exit_due_epoch=self._finite(row.get('exit_due_epoch')))
            status = str(order.get('status') or '').upper()
            if row.get('signal_follow') and not self._follow_fill_evidence(row, order):
                continue
            if status in {'NEW', 'PARTIALLY_FILLED'} and recovered_id:
                self._entry_cancel_delay(row)
            qty = self._finite(order.get('executedQty'))
            avg = self._finite(order.get('avgPrice'))
            row['last_status'] = status
            row['confirmed_filled_qty'] = qty
            self.session_entries.timings.outcome(str(row.get('client_order_id') or ''),
                order_id=str(order.get('orderId') or ''), status=status, executed_qty=qty)
            if qty > 0.0:
                if not self._register_fill(symbol, row, order, qty, avg):
                    continue
            if status in TERMINAL:
                self._retire_order(symbol, row, status)
            elif qty > 0.0 and status in {'NEW', 'PARTIALLY_FILLED'}:
                # Partial fill: cancel the exact remainder, retain ownership
                # until the stream reports a terminal status.
                self._request_cancel(symbol, row, 'partial_fill')
        self.ledger.save()

    def _register_fill(self, symbol: str, row: Mapping[str, Any],
                       order: Mapping[str, Any], qty: float, avg: float) -> bool:
        """Book cumulative exact entry fills BEFORE retiring a cancelled order.

        A partial fill keeps its order identity until the remainder is terminal.
        Actual fills beyond the position cap are recorded, never silently lost.
        """
        previous_qty = self._finite(row.get('accounted_entry_qty'))
        previous_quote = self._finite(row.get('accounted_entry_quote'))
        if qty <= previous_qty + 1e-12:
            self.session_entries.observe_fill(symbol, row, quantity=previous_qty)
            return True
        if not math.isfinite(avg) or avg <= 0:
            LOG.warning('entry fill price missing; retain exact identity symbol=%s qty=%s', symbol, qty)
            return False
        delta = qty - previous_qty
        delta_quote = qty * avg - previous_quote
        if not math.isfinite(delta_quote) or delta_quote <= 0:
            return False
        pos = self.positions.get(symbol)
        if pos is not None:
            same_entry = (str(pos.get('entry_client_order_id') or '') == str(row.get('client_order_id') or '')
                          and str(row.get('client_order_id') or ''))
            if not same_entry:
                LOG.error('entry identity conflict; retain order symbol=%s', symbol)
                return False
            old_qty = self._finite(pos.get('quantity'))
            pos['entry_price'] = (old_qty * pos['entry_price'] + delta_quote) / (old_qty + delta)
            pos['quantity'] = old_qty + delta
        else:
            if len(self.positions) >= cfg.SLOTS:
                LOG.warning('actual over-capacity fill recorded symbol=%s qty=%s', symbol, qty)
            self.positions[symbol] = {
                'symbol': symbol, 'side': 'LONG', 'entry_price': delta_quote / delta,
                'quantity': delta, 'entry_epoch': time.time(),
                'entry_fill_time_ms': exact_entry_fill_time_ms(order, symbol=symbol,
                    client_order_id=str(row.get('client_order_id') or ''),
                    order_id=row.get('order_id'), allow_update_time=True),
                **{k: row[k] for k in ('sizing_weight', 'sizing_position_count', 'sizing_balance', 'sizing_raw_notional') if k in row},
                'exit_due_epoch': self._finite(row.get('exit_due_epoch')),
                'exit_due_ms': int(self._finite(row.get('exit_due_ms'))),
                'exit_due_utc': str(row.get('exit_due_utc') or ''),
                'entry_order_id': row.get('order_id'),
                'entry_client_order_id': row.get('client_order_id'),
                'position_id': str(row.get('position_id') or ''),
                'reference_bar_open_ms': int(self._finite(row.get('reference_bar_open_ms'))),
                'order_bar_open_ms': int(self._finite(row.get('order_bar_open_ms'))),
            }
            self.stats['fill'] += 1
        row['accounted_entry_qty'] = qty
        row['accounted_entry_quote'] = qty * avg
        timing = self.session_entries.timings.order(str(row.get('client_order_id') or '')) or row.get('entry_timing')
        if timing:
            self.positions[symbol]['entry_timing'] = timing
        self.session_entries.observe_fill(symbol, row, quantity=qty)
        self.ledger.save()
        self._publish_entry_gate()
        return True

    def _retire_order(self, symbol: str, row: Mapping[str, Any], reason: str) -> None:
        self.session_entries.timings.outcome(str(row.get('client_order_id') or ''), retired_reason=reason)
        self._note_rotation_terminal(symbol, row, reason)
        getattr(self, '_cancel_attempts', {}).pop((symbol, str(row.get('client_order_id') or '')), None)
        self.tracked.pop(symbol, None)
        self.ledger.forget(symbol=symbol,
                           client_order_id=str(row.get('client_order_id') or ''),
                           order_id=row.get('order_id'))
        LOG.info('retire tracked order symbol=%s reason=%s', symbol, reason)

    def _entry_cancel_delay(self, row: dict[str, Any]) -> float:
        if not self.client.enabled or self.client.dry_run:
            return 0.
        now = time.time()
        confirmed = self._finite(row.get('entry_confirmed_at_epoch'))
        if confirmed <= 0 or confirmed > now:
            timing = row.get('entry_timing') or {}
            response = self._finite(timing.get('response_epoch_ms')) / 1000
            confirmed = response if 0 < response <= now else now
            row['entry_confirmed_at_epoch'] = confirmed
        return max(0., confirmed + ENTRY_CANCEL_MIN_AGE_SECONDS - now)

    def _request_cancel(self, symbol: str, row: dict[str, Any], reason: str) -> bool:
        cid = str(row.get('client_order_id') or '')
        oid = row.get('order_id')
        if not self.ledger.owns_order(symbol, oid, cid):
            self.stats['foreign_ignored'] += 1
            LOG.warning('refusing to cancel unregistered exact order symbol=%s reason=%s', symbol, reason)
            return False
        self.session_entries.timings.outcome(cid, cancel_reason=reason)
        if row.get('fate_unknown') and oid in (None, '', 0, '0'):
            # No acknowledged ID exists to cancel yet. The single throttled
            # reconciler resolves this UID; do not bypass its cooldown with a
            # second broker lookup from the cancel path. Keep the slot reserved.
            row['cancel_requested'] = True
            row['cancel_requested_epoch'] = time.time()
            row['cancel_reason'] = reason
            self._publish_entry_gate()
            if row.get('unacked_cancel_reason_logged') != reason:
                row['unacked_cancel_reason_logged'] = reason
                LOG.info('cancel marked without exchange order id symbol=%s id=%s reason=%s', symbol, cid, reason)
            return False
        attempts = getattr(self, '_cancel_attempts', None)
        if attempts is None:
            attempts = self._cancel_attempts = {}
        attempt_key = (symbol, cid)
        # Retry pacing is process-local: a persisted cancel flag after restart
        # is not evidence that this process has already enqueued its request.
        if (row.get('cancel_requested') and attempt_key in attempts
                and time.time() - attempts[attempt_key] < 10.0):
            return False
        # Reserve cancellation on the owner thread before scheduling I/O.
        row['cancel_requested'] = True
        row['cancel_requested_epoch'] = time.time()
        row['cancel_reason'] = reason
        delay = self._entry_cancel_delay(row)
        self.ledger.save()
        if delay > 0 and self._network_io is None:
            return False
        attempts[attempt_key] = time.time()
        def complete(result, error):
            current = self.tracked.get(symbol)
            if (not current or current.get('client_order_id') != cid
                    or str(current.get('order_id') or '') != str(oid or '')):
                return
            if isinstance(result, Mapping) and result.get('_cancel_not_sent'):
                return  # exact entry filled/retired while its cancel was still queued
            if error is not None:
                LOG.warning('cancel uncertain symbol=%s reason=%s err=%s', symbol, reason, error)
                return
            result = result if isinstance(result, Mapping) else {}
            if not matches_order_identity(result, symbol=symbol, client_order_id=cid, order_id=oid):
                LOG.warning('ignore mismatched cancel evidence; identity retained symbol=%s', symbol)
                return
            if current.get('signal_follow') and not self._follow_fill_evidence(current, result):
                return
            filled_qty = self._finite(result.get('executedQty'))
            current['confirmed_filled_qty'] = max(filled_qty, self._finite(current.get('confirmed_filled_qty')))
            if filled_qty + 1e-12 < self._finite(current.get('accounted_entry_qty')):
                LOG.warning('older cancel snapshot retained; wait for exact latest fill symbol=%s', symbol)
                return
            self.session_entries.timings.outcome(cid, order_id=str(oid or ''),
                status=str(result.get('status') or '').upper(), executed_qty=filled_qty)
            if filled_qty > 0 and not self._register_fill(
                    symbol, current, result, filled_qty, self._finite(result.get('avgPrice'))):
                self.ledger.save()
                return
            if str(result.get('status', '')).upper() in TERMINAL:
                self._retire_order(symbol, current, reason)
            self.stats['cancel'] += 1
            LOG.info('cancel requested symbol=%s order=%s reason=%s', symbol, oid or cid, reason)
        self._run_network(exact_entry_cancel_key(symbol, cid, oid),
            lambda: self.client.cancel(symbol=symbol, order_id=oid, client_order_id=cid),
            complete, priority=30, entry_cancel=True, entry_cancel_delay=delay)
        return True

    @staticmethod
    def _completed_3m_edge_epoch(*, now: float) -> float:
        return math.floor(now / (cfg.BAR_MS / 1000.0)) * (cfg.BAR_MS / 1000.0)

    def _over_capacity_reduction(self, *, now: float,
                                 target_positions: Mapping[str, Mapping[str, Any]]) -> None:
        """Prefer normal fixed-bar exits; only trim excess at a new 3m edge."""
        # Only positions whose normal fixed-bar close is still pending count as
        # "wait for normal exit". Once an exact close is in flight, over-capacity
        # trimming waits for that identity to become terminal.
        normal_due = [s for s, p in self.positions.items()
                      if (not p.get('close_client_order_id'))
                      and math.isfinite(self._finite(p.get('exit_due_epoch'), float('nan')))
                      and now >= self._finite(p.get('exit_due_epoch'), float('inf'))
                      and now - self._finite(p.get('exit_due_epoch'), 0.0)
                      < cfg.BAR_MS / 1000.0]
        if normal_due:
            LOG.info('defer over-capacity trim; %d normal fixed-bar exits are due first',
                     len(normal_due))
            return
        # Wait one completed 3m bar after the last fixed-bar exit before trimming
        # an over-capacity residual. This keeps normal exits first.
        last_due = max((self._finite(p.get('exit_due_epoch'), 0.0)
                        for p in self.positions.values()
                        if not p.get('close_client_order_id')), default=0.0)
        if last_due > 0.0 and now - last_due < cfg.BAR_MS / 1000.0:
            return
        excess = len(self.positions) - cfg.SLOTS
        if excess <= 0:
            return
        edge = self._completed_3m_edge_epoch(now=now)
        last = self._finite(self.ledger.state.get('last_over_capacity_edge'))
        if last == edge:
            return
        self.ledger.state['last_over_capacity_edge'] = edge
        # Only exact-owned positions are touched.  Prefer owned positions that
        # are no longer in the signal target set; otherwise close the oldest.
        victims = sorted(self.positions,
                         key=lambda s: (s in target_positions,
                                        self._finite(self.positions[s].get('entry_epoch')), s))[:excess]
        for symbol in victims:
            self._close_position(symbol, 'over_capacity_3m_edge', now=now)

    def _timed_exits(self, *, now: float) -> None:
        for symbol, pos in list(self.positions.items()):
            due = self._finite(pos.get('exit_due_epoch'), float('nan'))
            if math.isfinite(due) and now >= due - close_policy.TIMED_EXIT_LEAD_SECONDS:
                self._close_position(symbol, 'fixed_bar_exit', now=now)

    def _in_final_pre_exit_bar(self, *, now: float) -> bool:
        """Allow new pending entries during the final 3m before any timed exit."""
        horizon = cfg.BAR_MS / 1000.0
        return any(
            math.isfinite(self._finite(pos.get('exit_due_epoch'), float('nan')))
            and 0.0 < self._finite(pos.get('exit_due_epoch')) - now <= horizon
            for pos in self.positions.values()
        )

    def _cancel_all_for_full_capacity(self, *, now: float) -> None:
        if len(self.positions) < cfg.SLOTS or self._in_final_pre_exit_bar(now=now):
            return
        for symbol, row in list(self.tracked.items()):
            if row.get('signal_follow'):
                continue
            self._request_cancel(symbol, row, 'position_capacity_full_cancel_all')

    def _entry_plan_signature(self, row: Mapping[str, Any]) -> list[Any]:
        """Ignore quote changes; only a different executable signal plan retries."""
        return [str(row.get('symbol') or '').upper(),
                int(self._finite(row.get('order_bar_open_ms'))),
                int(self._finite(row.get('reference_bar_open_ms'))),
                self._finite(row.get('limit_price')), self._finite(row.get('notional')),
                self._finite(row.get('exit_due_epoch')),
                int(self._finite(row.get('exit_due_ms'), self._finite(row.get('exit_due_epoch'))*1000.0))]

    def _note_entry_absence(self, symbol: str, row: Mapping[str, Any], *, now: float) -> None:
        """A retry needs exact negative evidence AND a live desired plan later."""
        bar_ms = int(self._finite(row.get('order_bar_open_ms')))
        if not bar_ms > 0 or not bar_ms <= now * 1000 < bar_ms + cfg.BAR_MS:
            return
        cid = str(row.get('client_order_id') or '')
        records = self.ledger.state['entry_recovery']
        count = int(self._finite((records.get(cid) or {}).get('confirmed_absences'))) + 1
        records[cid] = dict(symbol=symbol, order_bar_open_ms=bar_ms,
                            confirmed_absences=count, last_absence_epoch=now)
        count = self._entry_recovery_count(row)
        if count > cfg.ENTRY_RECOVERY_MAX_RETRIES:
            LOG.warning('entry recovery limit reached symbol=%s id=%s retries=%d; '
                        'no more sends for this order bar', symbol, cid, cfg.ENTRY_RECOVERY_MAX_RETRIES)
        else:
            LOG.info('entry absence confirmed; retry eligible only if current desired '
                     'and capacity allow symbol=%s id=%s retry=%d/%d',
                     symbol, cid, count, cfg.ENTRY_RECOVERY_MAX_RETRIES)

    def _entry_recovery_count(self, row: Mapping[str, Any]) -> int:
        symbol = str(row.get('symbol') or '').upper()
        bar = int(self._finite(row.get('order_bar_open_ms')))
        return sum(int(self._finite(record.get('confirmed_absences')))
                   for record in self.ledger.state['entry_recovery'].values()
                   if record.get('symbol') == symbol
                   and int(self._finite(record.get('order_bar_open_ms'))) == bar)

    def _entry_submission_blocked(self, row: Mapping[str, Any]) -> bool:
        symbol = str(row.get('symbol') or '').upper()
        if self._forced_entry_blocked(symbol, now=time.time()):
            return True
        signature = self._entry_plan_signature(row)
        if row.get('signal_follow'):
            # Follow entries already advance their own durable generation and
            # retry confirmed rejects after a cooldown while signal is active.
            cid = self._entry_client_id(row)
            previous = self.ledger.state['entry_submit_rejections'].get(cid) or {}
            recovery = self.ledger.state['entry_recovery'].get(cid) or {}
            return (previous.get('plan_signature') == signature
                    or self._finite(recovery.get('confirmed_absences')) > cfg.ENTRY_RECOVERY_MAX_RETRIES)
        retry = self._entry_gate_retries.get(symbol)
        if retry and retry[0] == signature[1] and time.monotonic() < retry[2]:
            return True
        # Guard the signal plan, not a physical send ID. New IDs must not reset
        # rotation suppression, definitive rejection, or bounded absence retry.
        cache = getattr(self, '_entry_block_cache', None)
        if cache is None:
            cache = self._entry_block_cache = {}
        previous = cache.get(symbol)
        if previous:
            name, key, record = previous
            if (self.ledger.state.get(name, {}).get(key) is record
                    and not record.get('retryable') and record.get('symbol') == symbol
                    and record.get('plan_signature') == signature):
                return True
            cache.pop(symbol, None)
        for name in ('rotated_out_plans', 'entry_submit_rejections'):
            for key, record in self.ledger.state.get(name, {}).items():
                if not record.get('retryable') and record.get('symbol') == symbol and record.get('plan_signature') == signature:
                    cache[symbol] = (name, key, record)
                    return True
        if self._entry_recovery_count(row) > cfg.ENTRY_RECOVERY_MAX_RETRIES:
            return True
        return False

    def _prune_entry_rejections(self, bar_ms: int) -> None:
        # Keep current-bar physical-ID evidence across restart, while bounding
        # history and preserving plan-level suppression across new send IDs.
        if bar_ms > 0:
            self._entry_gate_retries = {s: r for s, r in self._entry_gate_retries.items()
                                        if r[0] >= bar_ms}
            for name in ('entry_submit_rejections', 'entry_recovery', 'rotated_out_plans'):
                records = self.ledger.state[name]
                for cid, record in list(records.items()):
                    if int(self._finite(record.get('order_bar_open_ms'))) < bar_ms:
                        records.pop(cid, None)

    def _reject_entry_submission(self, row: Mapping[str, Any], cid: str,
                                 exc: BaseException) -> None:
        symbol = str(row.get('symbol') or '').upper()
        current = self.tracked.get(symbol)
        bar_ms = int(self._finite(row.get('order_bar_open_ms')))
        now = time.time()
        gate_retry = (isinstance(exc, EntrySendGateRejection)
                      and (exc.reason in EntrySendGateRejection.RECOVERABLE_ORDINARY or
                           (exc.reason == 'entry_cancel_pending_at_send' and
                            exc.cancel_reason in ('entry_gap_too_far', 'position_capacity_full',
                                                  'position_capacity_full_cancel_all')))
                      and not (row.get('signal_follow') or (current or {}).get('signal_follow'))
                      and bar_ms > 0 and bar_ms <= now*1000 < bar_ms + cfg.BAR_MS)
        retryable = isinstance(exc, EntryQuoteUnavailable) or gate_retry
        if gate_retry:
            # Only errors proven to be before any wire attempt may use this
            # path. Slow down repeated gate flaps without blacklisting the bar.
            prev = self._entry_gate_retries.get(symbol)
            failures = min(5, prev[1] + 1) if prev and prev[0] == bar_ms else 0
            self._entry_gate_retries[symbol] = (bar_ms, failures, time.monotonic() + .25 * 2**failures)
        diagnostic = submission_diagnostic(exc, rejected=True, disabled=isinstance(exc, DisabledExchange))
        self.session_entries.timings.outcome(cid, status='SUBMIT_REJECTED', retryable=retryable, **diagnostic)
        activity = getattr(self.client, 'request_activity', None)
        if activity is not None:
            activity.entry_outcome(diagnostic['submit_error_category'],
                                   diagnostic.get('submit_error_reason'))
        if current and str(current.get('client_order_id') or '') == cid:
            self._note_rotation_terminal(symbol, current, 'confirmed_submission_rejected')
            origin = current.get('rotation_origin')
            if isinstance(origin, Mapping) and not self.ledger.state.get('pending_rotation'):
                self.ledger.state['pending_rotation'] = dict(origin)
                LOG.info('replacement rejected; restore original rotation gates symbol=%s', symbol)
        self.tracked.pop(symbol, None)
        if isinstance(exc, EntryQuoteUnavailable):
            retry_rejected = getattr(self, '_entry_retry_rejected', None)
            if retry_rejected is not None:
                retry_rejected(symbol, exc)
        self.ledger.state['entry_submit_rejections'][cid] = {
            'symbol': symbol, 'client_order_id': cid,
            'order_bar_open_ms': bar_ms,
            'plan_signature': self._entry_plan_signature(row),
            'rejected_at_epoch': time.time(), 'error': str(exc),
            'retryable': retryable,
        }
        self.stats['entry_rejected'] += 1
        # Only called for a newly registered submission known not to be accepted.
        # Never forget a previously ambiguous or acknowledged order here.
        self.ledger.forget(symbol=symbol, client_order_id=cid)
        LOG.warning('entry rejected; slot released symbol=%s id=%s err=%s; %s', symbol, cid, exc,
                    'temporary send gate; eligible for same-bar reevaluation after backoff' if gate_retry
                    else 'fresh qualifying quote may retry' if retryable
                    else 'same signal plan suppressed until changed/new bar')

    def _place_order(self, row: Mapping[str, Any]) -> None:
        with self.ledger.batch_saves():
            return self._place_order_recorded(row)

    def _record_entry_skip(self, symbol, row, reason):
        if not hasattr(self, '_submit_skip_reasons'):
            self._submit_skip_reasons = {}
        self._submit_skip_reasons[symbol or '<missing-symbol>'] = reason
        self.session_entries.timings.skipped(symbol, row, (self._io_signal[0] or {}).get('signal_session_id'), reason)

    def _place_order_recorded(self, row: Mapping[str, Any]) -> None:
        if hasattr(self, '_entry_sizing_row'):
            row = self._entry_sizing_row(row)
        symbol = str(row.get('symbol') or '').upper()
        def skipped(reason: str) -> None:
            self._record_entry_skip(symbol, row, reason)

        if not symbol or symbol in self.tracked or symbol in self.positions:
            return skipped('missing_symbol_or_already_tracked_or_held')
        submit_now = time.time()
        following = bool(row.get('signal_follow'))
        bar_ms = int(self._finite(row.get('order_bar_open_ms')))
        if not following and bar_ms > 0 and not bar_ms <= submit_now * 1000 < bar_ms + cfg.BAR_MS:
            return skipped('order_bar_expired_before_submit')
        reopen_until = self._finite(
            self.ledger.state['reopen_until_epoch'].get(symbol))
        if submit_now < reopen_until:
            return skipped('post_close_reopen_cooldown')
        final_exit_bar = self._in_final_pre_exit_bar(now=submit_now)
        from .signal_follow import FOLLOW_ORDER_CAP, FOLLOW_POSITION_CAP
        if following:
            if (len(self.positions) >= FOLLOW_POSITION_CAP
                    or sum(bool(p.get('signal_follow')) for p in self.tracked.values()) >= FOLLOW_ORDER_CAP):
                return skipped('signal_follow_capacity_full')
        elif ((len(self.positions) >= cfg.SLOTS and not final_exit_bar)
                or self._ordinary_pending_count() >= cfg.MAX_PENDING_ORDERS):
            return skipped('position_or_pending_capacity_full')
        plan_id = self._entry_client_id(row)
        if self.ledger.owns_client(plan_id) or self._entry_submission_blocked(row):
            return skipped('exact_client_identity_already_used_or_plan_rejected')
        price = self._finite(row.get('limit_price'))
        notional = self._finite(row.get('notional'))
        if price <= 0.0 or notional <= 0.0:
            return skipped('invalid_price_or_notional')
        prepared = self.client.prepare_limit(symbol, price, notional) \
            if hasattr(self.client, 'prepare_limit') else None
        if prepared is None:
            price_text = self.client.price_text(symbol, price)
            qty_text = self.client.qty_text(symbol, notional, price)
            prepared = {'ok': True, 'price': price_text, 'qty': qty_text,
                        'reason': 'legacy_adapter'}
        if not prepared.get('ok'):
            LOG.warning('skip order symbol=%s reason=%s price=%s qty=%s notional=%.8g',
                        symbol, prepared.get('reason'), prepared.get('price'),
                        prepared.get('qty'), notional)
            return skipped('preflight: ' + str(prepared.get('reason')))
        qty = self._finite(prepared.get('qty'))
        price_text = str(prepared.get('price') or '')
        qty_text = str(prepared.get('qty') or '')
        # Preserve signal identity fields used by _order_changed. Actual entry
        # sizing can exceed the plan because of minNotional/minQty/stepSize.
        sizing_details = {
            'submitted_limit_price': self._finite(prepared.get('price'), price),
            'submitted_notional': self._finite(
                prepared.get('actual_notional'), qty * self._finite(prepared.get('price'), price)),
            'sizing_target_notional': self._finite(prepared.get('target_notional'), notional),
        }
        client_id = plan_id if following else self.ledger.reserve_entry_client_id(symbol, bar_ms)
        self.session_entries.timings.register(symbol, client_id, dict(row, **sizing_details),
            session_id=(self._io_signal[0] or {}).get('signal_session_id'))
        # Persist BOTH exact identity and its occupied physical slot before I/O.
        # A process interruption between send and ACK must not lose this slot.
        self.tracked[symbol] = {
            'symbol': symbol, 'client_order_id': client_id, 'order_id': '',
            'limit_price': price, 'notional': notional, 'qty': qty,
            **sizing_details,
            **{k: row[k] for k in ('sizing_weight', 'sizing_position_count', 'sizing_balance', 'sizing_raw_notional') if k in row},
            'exit_due_epoch': self._finite(row.get('exit_due_epoch')),
            'exit_due_ms': int(self._finite(row.get('exit_due_ms'))),
            'exit_due_utc': str(row.get('exit_due_utc') or ''),
            'reference_bar_open_ms': int(self._finite(row.get('reference_bar_open_ms'))),
            'order_bar_open_ms': int(self._finite(row.get('order_bar_open_ms'))),
            'position_id': str(row.get('position_id') or ''),
            **{k: row[k] for k in ('signal_follow', 'signal_follow_position_id') if k in row},
            'observed_price': self._finite(row.get('observed_price'), price),
            'gap': self._finite(row.get('gap'), 0.0),
            'placed_at_epoch': submit_now, 'last_status': 'SUBMITTING',
            'entry_identity_version': 2,
            **{k: row[k] for k in QUOTE_FIELDS if k in row},
            'entry_gap_ceiling': row.get('entry_gap_ceiling', GapPolicy().admission_gap(self._ordinary_pending_count())),
            'fate_unknown': True,
        }
        origin = row.get('_rotation_origin')
        if isinstance(origin, Mapping):
            self.tracked[symbol]['rotation_origin'] = dict(origin)
            # Persist reservation consumption together with the exact new slot,
            # before send. A confirmed later rejection restores these gates.
            self.ledger.state['pending_rotation'] = None
        self.ledger.register(symbol=symbol, client_order_id=client_id,
                             kind='entry', limit_price=price, qty=qty,
                             exit_due_epoch=self._finite(row.get('exit_due_epoch')))
        # One grouped durability barrier contains sequence, exact intent, new
        # slot and any replacement/cancel reservations. Nothing is sent first.
        self.ledger.flush_for_dispatch()
        self.session_entries.timings.mark(client_id, 'registered')
        LOG.info('entry registered symbol=%s id=%s 满足=%s tick=%s depth_bid0=%s gap_ceiling=%s',
                 symbol, client_id, self.tracked[symbol].get('entry_qualified_by', 'unknown'),
                 row.get('entry_tick'), row.get('entry_bid0'), row.get('entry_gap_ceiling'))
        if self._entry_recovery_count(row):
            self.stats['entry_retries'] += 1
        source_row = dict(row)
        def send():
            self.session_entries.timings.mark(client_id, 'worker')
            guarded_place = getattr(self.client, 'place_limit_for_bar', None)
            kwargs = dict(symbol=symbol, client_order_id=client_id,
                          limit_price=price, notional=notional)
            if callable(guarded_place):
                return guarded_place(order_bar_open_ms=bar_ms, **kwargs)
            return self.client.place_limit(**kwargs)

        def complete(place_result, error):
            current = self.tracked.get(symbol)
            if not current or current.get('client_order_id') != client_id:
                return  # exact stream evidence already settled this identity
            try:
                if error is not None:
                    raise error
                place_result = require_submit_ack(
                    place_result, symbol=symbol, client_order_id=client_id)
                if not matches_order_identity(place_result, symbol=symbol,
                        client_order_id=client_id, order_id=current.get('order_id')):
                    raise RuntimeError('entry ACK conflicts with exact known order identity')
            except Exception as exc:
                # A stream ACK/fill may beat the background result. Never erase
                # known exchange ownership because an older call failed.
                if current.get('order_id') or self._finite(current.get('accounted_entry_qty')) > 0:
                    LOG.warning('late submit failure after exact evidence; retain symbol=%s err=%s', symbol, exc)
                    return
                if isinstance(exc, DisabledExchange) or not submit_error_is_ambiguous(exc):
                    self._reject_entry_submission(source_row, client_id, exc)
                    return
                LOG.warning('place uncertain symbol=%s err=%s; exact identity retained', symbol, exc)
                current.update(last_status='UNKNOWN', submit_failed_at_epoch=time.time())
                self.session_entries.timings.outcome(client_id, status='UNKNOWN',
                    **submission_diagnostic(exc, rejected=False))
                self.ledger.save()
                return
            order_id = str(place_result.get('orderId') or '')
            self.session_entries.timings.mark(client_id, 'owner_ack')
            send_quote = place_result.get('_entry_send_quote')
            if isinstance(send_quote, Mapping) and send_quote.get('entry_qualified_by'):
                current['entry_send_quote'] = {k: send_quote[k] for k in QUOTE_FIELDS if k in send_quote}
            current.update(order_id=order_id, fate_unknown=False)
            if order_id:
                self._entry_cancel_delay(current)
            if self._finite(place_result.get('executedQty')) >= self._finite(current.get('confirmed_filled_qty')):
                current['last_status'] = str(place_result.get('status') or 'NEW').upper()
            self.session_entries.timings.outcome(client_id, order_id=order_id,
                status=str(current.get('last_status') or 'NEW').upper(),
                executed_qty=max(self._finite(place_result.get('executedQty')),
                                 self._finite(current.get('confirmed_filled_qty'))))
            current['entry_timing'] = self.session_entries.timings.order(client_id)
            self.ledger.register(symbol=symbol, client_order_id=client_id,
                                 order_id=order_id, kind='entry',
                                 limit_price=price, qty=qty,
                                 exit_due_epoch=self._finite(row.get('exit_due_epoch')))
            if not current.get('submission_ack_recorded'):
                self.stats['place'] += 1
                current['submission_ack_recorded'] = True
            if isinstance(place_result, Mapping):
                filled_qty = self._finite(place_result.get('executedQty'))
                if filled_qty > 0 and self._register_fill(
                        symbol, self.tracked[symbol], place_result, filled_qty,
                        self._finite(place_result.get('avgPrice'))):
                    if str(place_result.get('status') or '').upper() in TERMINAL:
                        self._retire_order(symbol, self.tracked[symbol], 'entry_response_terminal')
            LOG.info('place exact order symbol=%s signal_price=%.10g order_price=%.10g '
                     'signal_notional=%.8g order_notional=%.8g qty=%s id=%s/%s 满足=%s',
                     symbol, price, sizing_details['submitted_limit_price'],
                     notional, sizing_details['submitted_notional'], qty_text, client_id, order_id,
                     (current.get('entry_send_quote') or current).get('entry_qualified_by', 'unknown'))
            timing = current['entry_timing']
            LOG.info('entry timing symbol=%s id=%s quote_received_ms=%s first_send_ms=%s '
                     'response_ms=%s owner_ack_ms=%s transport=%s latency_ms=%s',
                     symbol, client_id, timing.get('received_epoch_ms'), timing.get('send_epoch_ms'),
                     timing.get('response_epoch_ms'), timing.get('owner_ack_epoch_ms'),
                     timing.get('transport'), timing.get('latencies_ms'))
            self.ledger.save()
        self._run_network(('entry', symbol, client_id), send, complete, priority=1,
            entry_request=None if following else dict(symbol=symbol, client_order_id=client_id, limit_price=price,
                                                     notional=notional, order_bar_open_ms=bar_ms))

    def _target_positions(self, snapshot: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
        rows = snapshot.get('target_positions')
        out: dict[str, dict[str, Any]] = {}
        if isinstance(rows, list):
            for row in rows:
                if not isinstance(row, Mapping):
                    continue
                symbol = str(row.get('symbol') or '').upper().strip()
                if symbol:
                    out[symbol] = dict(row)
        return out

    def _apply_signal_targets(self, snapshot: Mapping[str, Any]) -> None:
        """Record signal-authoritative targets; trade implements physical state."""
        if not snapshot.get('signal_ready'):
            return
        targets = self._target_positions(snapshot)
        self.ledger.state['signal_session_id'] = snapshot.get('signal_session_id')
        self.ledger.state['last_signal_revision'] = snapshot.get('signal_revision')
        # The signal terminal is authoritative for correct model positions/PnL.
        # Execution feedback exposes these fields alongside exact-owned actual
        # positions; it never invents an exchange position from a target.
        self.ledger.state['signal_target_positions'] = targets
        portfolio = dict(snapshot.get('portfolio') or {})
        self.ledger.state['signal_portfolio'] = portfolio

    def _comparison(self, *, snapshot: Mapping[str, Any], price_map: Mapping[str, float],
                    signal_desired_count: int, priced_desired_count: int,
                    near_place_count: int, positions: Mapping[str, Any],
                    open_orders: Mapping[str, Any]) -> dict[str, Any]:
        return compute_comparison(
            actual_positions=positions,
            open_orders=open_orders,
            signal_portfolio=snapshot.get('portfolio') or {},
            target_positions=self._target_positions(snapshot),
            price_map=price_map,
            realized_balance=self._finite(
                self.ledger.state.get('realized_balance'), cfg.EQUITY0),
            signal_desired_count=signal_desired_count,
            priced_desired_count=priced_desired_count,
            near_place_count=near_place_count,
            stats=self.stats,
            equity0=self._book_initial_equity(),
            imported_baseline=bool(self.ledger.state.get('manual_signal_sync')),
            max_pending_orders=cfg.MAX_PENDING_ORDERS,
            max_positions=cfg.SLOTS)

    def write_feedback(self, *, now: float, price_map: Mapping[str, float],
                       comparison: Mapping[str, Any], stream_health: Mapping[str, Any]) -> None:
        positions: dict[str, dict[str, Any]] = {}
        current = 0.0
        for symbol, pos in self.positions.items():
            mark = self._finite(price_map.get(symbol), float('nan'))
            entry = self._finite(pos.get('entry_price'))
            qty = self._finite(pos.get('quantity'))
            if not math.isfinite(mark):
                mark = entry
            row = dict(pos)
            row['mark_price'] = mark
            row['unrealized_pnl'] = (mark - entry) * qty
            current += row['unrealized_pnl']
            positions[symbol] = row
        orders = {}
        for symbol, row in self.tracked.items():
            orders[symbol] = {
                'symbol': symbol,
                'order_id': row.get('order_id'),
                'client_order_id': row.get('client_order_id'),
                'limit_price': row.get('limit_price'),
                'notional': row.get('notional'),
                'qty': row.get('qty'),
                'status': row.get('last_status', 'NEW'),
                'signal_follow': bool(row.get('signal_follow')),
                'cancel_requested': bool(row.get('cancel_requested')),
                'cancel_reason': row.get('cancel_reason'),
                'signal_follow_position_id': row.get('signal_follow_position_id'),
                **{k: row[k] for k in (*QUOTE_FIELDS, 'entry_send_quote') if k in row},
            }
        payload = {
            'schema_version': 1,
            'component': 'execution',
            'mode': 'live' if self.client.enabled and not self.client.dry_run else 'dry_run',
            'updated_at_utc': utc_now_iso(),
            'updated_at_epoch': now,
            'realized_balance': self._finite(
                self.ledger.state.get('realized_balance'), cfg.EQUITY0),
            'initial_equity': self._book_initial_equity(),
            'cumulative_pnl': self._finite(
                self.ledger.state.get('realized_balance'), cfg.EQUITY0) - self._book_initial_equity(),
            'current_unrealized_pnl': current,
            'positions': positions,
            'open_orders': orders,
            'close_orders': self._close_order_view(),
            'target_positions': self._target_positions(self.last_valid_snapshot or {}),
            'signal_session_id': self.ledger.state.get('signal_session_id'),
            'signal_revision': self.ledger.state.get('last_signal_revision'),
            'signal_portfolio': self.ledger.state.get('signal_portfolio') or {},
            'capital_mode': self.ledger.state.get('capital_mode', 'execution_ledger'),
            'manual_signal_sync': self.ledger.state.get('manual_signal_sync') or {},
            'manual_close_history': dict(self.ledger.state.get('manual_close_history', {})),
            'comparison': dict(comparison),
            'session_entry_stats': self.session_entries.snapshot(
                max_rows=cfg.ViewConfig().max_orders_display),
            'stats': dict(self.stats),
            'entry_submit_rejections': dict(self.ledger.state['entry_submit_rejections']),
            'entry_recovery': dict(self.ledger.state['entry_recovery']),
            'entry_gap_policy': GapPolicy().describe(),
            'current_admission_gap': self._current_admission_gap(),
            'pending_rotation': self.ledger.state.get('pending_rotation'),
            'rotated_out_plans': dict(self.ledger.state.get('rotated_out_plans') or {}),
            'reopen_until_epoch': dict(self.ledger.state['reopen_until_epoch']),
            'forced_close_history': dict(self.ledger.state.get('forced_close_history', {})),
            'forced_close_cleanup': dict(self.ledger.state.get('forced_close_cleanup', {})),
            'entry_diagnostics': getattr(self, '_entry_diagnostics', {}),
            'stream_health': dict(stream_health),
            'network_io': self._network_io.health() if self._network_io else {'enabled': False},
            'note': ('Operator-imported signal baseline plus subsequent execution accounting; '
                     'imported positions are not claimed as exchange entry fills.'
                     if self.ledger.state.get('manual_signal_sync') else
                     'Executor-owned exact-identity state. Aggregate account '
                     'positions are never adopted as v9711 positions.'),
        }
        self._write_feedback_payload(payload)

    def _write_feedback_payload(self, payload):
        atomic_write_json(cfg.EXECUTION_FEEDBACK_FILE, payload)

    def _stream_health(self) -> dict[str, Any]:
        try:
            health = self.client.stream.health()
            return {'fresh': True, 'age_seconds': health.age_seconds,
                    'last_event_seq': health.last_event_seq}
        except StreamUnavailable as exc:
            return {'fresh': False, 'reason': str(exc)}

    def _render_trade(self, *, comparison: Mapping[str, Any],
                      orders: Mapping[str, Mapping[str, Any]],
                      stream_health: Mapping[str, Any], now: float) -> None:
        self.view.render(
            force=self.once,
            clock_utc=datetime.now(timezone.utc).isoformat(timespec='seconds'),
            enabled=self.client.enabled,
            dry_run=self.client.dry_run,
            signal_session_id=self.ledger.state.get('signal_session_id'),
            signal_revision=self.ledger.state.get('last_signal_revision'),
            updated_at_epoch=now,
            comparison=dict(comparison),
            session_entry_stats=self.session_entries.snapshot(
                max_rows=cfg.ViewConfig().max_orders_display),
            open_orders={symbol: dict(row) for symbol, row in orders.items()},
            close_orders=self._close_order_view(),
            network_io=self._network_io.health() if self._network_io else {'enabled': False},
            pending_rotation=self.ledger.state.get('pending_rotation'),
            capital_mode=self.ledger.state.get('capital_mode', 'execution_ledger'),
            manual_signal_sync=dict(self.ledger.state.get('manual_signal_sync') or {}),
            manual_close_history=dict(self.ledger.state.get('manual_close_history', {})),
            forced_close_history=dict(self.ledger.state.get('forced_close_history', {})),
            forced_close_cleanup=dict(self.ledger.state.get('forced_close_cleanup', {})),
            entry_diagnostics=getattr(self, '_entry_diagnostics', {}),
            entry_rejections=dict(self.ledger.state['entry_submit_rejections']),
            stream_health=dict(stream_health))

    def step(self, *, now: float | None = None) -> None:
        with self.entry_cancel_plan():
            self._step(now=now)

    def _step(self, *, now: float | None = None) -> None:
        checkpoint = getattr(self, '_maintenance_checkpoint', lambda stage: None)
        admission_turn = getattr(self, '_maintenance_admission_turn', lambda: None)
        self.cycles += 1
        self._submit_skip_reasons = {}
        now = time.time() if now is None else float(now)
        snapshot, status = self.desired_snapshot(now=now)
        self._io_signal = (snapshot, status)
        self._publish_entry_gate()
        # Capture exact booked fills before close reconciliation removes positions.
        self.session_entries.observe_owned_state(self.positions, self.tracked)
        self._drain_network()
        checkpoint('signal_callbacks')
        self.reconcile_orders(now=now)
        checkpoint('orders_exact')
        admission_turn()
        self._reconcile_forced_exits(now=now)
        checkpoint('forced_exits')
        self._cleanup_forced_close_orders(now=now)
        checkpoint('forced_cleanup')
        admission_turn()
        self._publish_entry_gate()
        for symbol in list(self.positions):
            self._reconcile_position_close(symbol, now=now)
        checkpoint('positions_close')
        admission_turn()
        self._reconcile_forced_exits(now=now)
        self._reconcile_manual_flats(now=now)
        checkpoint('manual_flats')
        admission_turn()
        # Admission turns can observe a new signal file. Never apply an older
        # local snapshot over the newer worker gate/targets after resuming.
        snapshot, status = self.desired_snapshot(now=now)
        self._io_signal = snapshot, status
        self._apply_signal_targets(snapshot)
        self.session_entries.observe_signal(snapshot, status=status, now=now)
        checkpoint('positions_manual')
        admission_turn()
        snapshot, status = self.desired_snapshot(now=now)
        self._io_signal = snapshot, status
        targets = self._target_positions(snapshot)
        self._over_capacity_reduction(now=now, target_positions=targets)
        self._timed_exits(now=now)
        final_exit_bar = self._in_final_pre_exit_bar(now=now)
        self._cancel_all_for_full_capacity(now=now)
        checkpoint('close_capacity')
        admission_turn()
        snapshot, status = self.desired_snapshot(now=now)
        self._io_signal = snapshot, status
        targets = self._target_positions(snapshot)
        final_exit_bar = self._in_final_pre_exit_bar(now=now)
        desired_rows, live_desired, price_map, plan = self._execute_entries(
            snapshot, status, now=now, targets=targets, final_exit_bar=final_exit_bar)
        checkpoint('entry_plan')
        admission_turn()
        for row in (*self.tracked.values(), *self.positions.values()):
            timing = self.session_entries.timings.order(str(row.get('client_order_id') or row.get('entry_client_order_id') or ''))
            if timing:
                row['entry_timing'] = timing
        self.session_entries.observe_owned_state(self.positions, self.tracked)
        self.ledger.save()
        checkpoint('ledger')
        admission_turn()
        if not getattr(self, '_presentation_due', lambda: True)():
            checkpoint('presentation_deferred')
            return  # Exact accounting, entries and close timers already ran.
        stream_health = self._stream_health()
        comparison = self._comparison(
            snapshot=snapshot, price_map=price_map,
            signal_desired_count=len(desired_rows),
            priced_desired_count=len(live_desired),
            near_place_count=sum(math.isfinite(self._finite(item.get('gap'), float('nan')))
                                 and self._finite(item.get('gap'), float('nan')) <= self._current_admission_gap()
                                 for item in live_desired),
            positions=self.positions, open_orders=self.tracked)
        checkpoint('comparison')
        admission_turn()
        self.write_feedback(now=now, price_map=price_map,
                            comparison=comparison, stream_health=stream_health)
        self._render_trade(comparison=comparison, orders=self.tracked,
                           stream_health=stream_health, now=now)
        if self.cycles == 1 or self.once or status != getattr(self, '_last_logged_desired_status', None):
            LOG.info('cycle=%s desired_status=%s signal_ready=%s targets=%d desired=%d tracked=%d positions=%d actions=%d',
                     self.cycles, status, bool(snapshot.get('signal_ready')),
                     len(targets), len(live_desired), len(self.tracked),
                     len(self.positions), len(plan.actions))
        self._last_logged_desired_status = status

    def _execute_entries(self, snapshot, status, *, now, targets, final_exit_bar):
        """Shared admission/rotation rules, called directly by WS events."""
        self._submit_skip_reasons = {}
        desired_rows = snapshot.get('desired')
        if not isinstance(desired_rows, list):
            desired_rows = []
        price_map = self.refresh_entry_prices(now=now)
        self._cancel_prices = price_map
        bar_ms = int(self._finite(snapshot.get('bar_open_ms')))
        plan_ready = (status == 'valid' and bool(snapshot.get('signal_ready'))
                      and bar_ms > 0 and bar_ms / 1000 <= now < (bar_ms + cfg.BAR_MS) / 1000)
        if bar_ms != self._preflight_bar:
            self._rejected_preflight.clear()
            self._preflight_bar = bar_ms
            self._prune_entry_rejections(bar_ms)
        live_desired = []
        filtered_entries = {}
        for raw in desired_rows:
            if not isinstance(raw, Mapping):
                continue
            item = dict(raw)
            symbol = str(item.get('symbol', '')).upper().strip()
            price = self._finite(price_map.get(symbol), float('nan'))
            limit = self._finite(item.get('limit_price'), float('nan'))
            row_bar = int(self._finite(item.get('order_bar_open_ms')))
            if (math.isfinite(price) and price > 0 and math.isfinite(limit) and limit > 0
                    and row_bar == bar_ms):
                item['observed_price'] = price
                item['gap'] = (price - limit) / limit
                if symbol not in self.tracked and (symbol in self.positions or item['gap'] > cfg.CANCEL_GAP):
                    continue
                if symbol not in self.tracked and self._entry_submission_blocked(item):
                    filtered_entries[symbol] = 'submission_blocked_for_current_plan'
                    continue  # Do not rotate for or re-send a known rejected plan.
                # Never cancel a valid incumbent for a challenger that cannot
                # meet tick/step/min-notional constraints. Preparation is local.
                prepare = getattr(self.client, 'prepare_limit', None)
                if plan_ready and symbol not in self.tracked and callable(prepare):
                    try:
                        prepared = prepare(symbol, limit, self._finite(item.get('notional')))
                    except Exception as exc:
                        prepared = {'ok': False, 'reason': str(exc)}
                    if not prepared.get('ok'):
                        key = (symbol, limit, item.get('notional'), str(prepared.get('reason')))
                        if key not in self._rejected_preflight:
                            self._rejected_preflight.add(key)
                            self._record_entry_skip(symbol, item, 'preflight: ' + str(prepared.get('reason')))
                            LOG.warning('candidate not executable; no rotation symbol=%s reason=%s',
                                        symbol, prepared.get('reason'))
                        filtered_entries[symbol] = 'preflight: ' + str(prepared.get('reason'))
                        continue
                live_desired.append(item)
            else:
                filtered_entries[symbol or '<missing-symbol>'] = (
                    'order_bar_mismatch' if row_bar != bar_ms else 'missing_or_stale_price_or_invalid_limit')
        tracked_view = {}
        pending_victim = (self.ledger.state.get('pending_rotation') or {}).get('victim')
        for symbol, old in self.tracked.items():
            if old.get('signal_follow'):
                continue
            if (getattr(self, '_fast_tick', False) and self._network_io is not None and old.get('cancel_requested')
                    and symbol != pending_victim):
                # Already paced independently. Keep the exact identity in
                # self.tracked and reconcile/retry in maintenance; it cannot
                # compete for an active entry slot on every fresh quote.
                continue
            item = dict(old)
            price = self._finite(price_map.get(symbol), float('nan'))
            limit = self._finite(item.get('limit_price'), float('nan'))
            item['_quote_fresh'] = (math.isfinite(price) and price > 0
                                    and math.isfinite(limit) and limit > 0)
            if item['_quote_fresh']:
                item['observed_price'] = price
                item['gap'] = (price - limit) / limit
            tracked_view[symbol] = item
        plan = select_physical_orders(
            live_desired, tracked_view, held=self.positions,
            free_position_slots=cfg.SLOTS - len(self.positions),
            ignore_position_capacity=final_exit_bar,
            signal_ready=plan_ready, now=now,
            max_orders=cfg.MAX_PENDING_ORDERS, place_gap=cfg.PLACE_GAP,
            switch_tolerance=cfg.SWITCH_TOLERANCE,
            bootstrap_gap=cfg.BOOTSTRAP_PLACE_GAP, fill_gap=cfg.FILL_PLACE_GAP,
            bootstrap_orders=cfg.BOOTSTRAP_PENDING_ORDERS,
            pending_rotation=self.ledger.state.get('pending_rotation'))
        self._prepare_rotation(plan)
        cancel_reasons = {a.symbol: a.reason for a in plan.actions if a.action == 'cancel'}
        # Release active admission capacity in memory before dispatching new
        # entries, but retain every old exact identity. Cancel I/O comes later.
        for symbol in plan.cancel:
            row = self.tracked.get(symbol)
            if row is not None:
                if not row.get('cancel_requested'):
                    row['cancel_requested_epoch'] = time.time()
                row.update(cancel_requested=True, cancel_reason=cancel_reasons[symbol])
                self.ledger.save()
        for action in plan.actions:
            if action.action == 'defer' and plan.rotation_action == 'reserve':
                self.stats['rotation_defer'] += 1
        with self.entry_plan():
            for row in plan.place:
                raw = {
                    'symbol': row.symbol, 'limit_price': row.limit_price,
                    'notional': row.notional, 'observed_price': row.observed_price,
                    'gap': row.gap, 'order_bar_open_ms': row.order_bar_open_ms,
                    'reference_bar_open_ms': row.reference_bar_open_ms,
                    'exit_due_epoch': row.exit_due_epoch,
                    'exit_due_ms': row.exit_due_ms,
                    'exit_due_utc': row.exit_due_utc,
                    'position_id': (targets.get(row.symbol) or {}).get('position_id', ''),
                    'entry_gap_ceiling': GapPolicy().admission_gap(self._ordinary_pending_count()),
                }
                if plan.rotation_placement == row.symbol:
                    raw['_rotation_origin'] = dict(self.ledger.state.get('pending_rotation') or {})
                    origin = raw['_rotation_origin']
                    raw['entry_gap_ceiling'] = min(cfg.CANCEL_GAP, self._finite(origin.get('victim_gap'))
                        - GapPolicy().minimum_improvement(int(self._finite(origin.get('original_order_count')))))
                self._place_order(raw)
        # The new entries have been registered and dispatched. The existing
        # paced cancel lane retains its 1.5s interval and exact-ID recovery.
        if plan.cancel:
            self.ledger.flush_for_dispatch()
        for symbol in plan.cancel:
            row = self.tracked.get(symbol)
            if row is not None:
                self._request_cancel(symbol, row, cancel_reasons[symbol])
        self._consume_rotation(plan)
        filtered_entries.update(self._submit_skip_reasons)
        blockers = []
        if status != 'valid':
            blockers.append('desired_' + status)
        if not snapshot.get('signal_ready'):
            blockers.append('signal_not_ready')
        if not (bar_ms > 0 and bar_ms / 1000 <= now < (bar_ms + cfg.BAR_MS) / 1000):
            blockers.append('outside_order_bar')
        if len(self.positions) >= cfg.SLOTS and not final_exit_bar:
            blockers.append('position_capacity_full')
        self._entry_diagnostics = {
            'desired_file': getattr(self, '_desired_path_display', None) or str(cfg.DESIRED_FILE.resolve()),
            'desired_status': status, 'signal_ready': bool(snapshot.get('signal_ready')),
            'plan_ready': plan_ready, 'bar_open_ms': bar_ms,
            'published': len(desired_rows), 'priced_executable': len(live_desired),
            'selected_to_place': [row.symbol for row in plan.place],
            'tracked': sorted(self.tracked), 'blockers': blockers,
            'filtered': filtered_entries,
            'actions': [{'symbol': a.symbol, 'action': a.action, 'reason': a.reason}
                        for a in plan.actions],
            'admission_gap': self._current_admission_gap(),
        }
        if not getattr(self, '_fast_tick', False) and LOG.isEnabledFor(logging.DEBUG):
            diagnostic_key = json.dumps(self._entry_diagnostics, sort_keys=True)
            if diagnostic_key != getattr(self, '_last_entry_diagnostic_key', None):
                LOG.debug('entry diagnostics %s', diagnostic_key)
                self._last_entry_diagnostic_key = diagnostic_key
        return desired_rows, live_desired, self.refresh_prices(now=now), plan

    def run(self) -> int:
        try:
            return self._run_loop()
        finally:
            self.shutdown_io()

    def _run_loop(self) -> int:
        LOG.info('start enabled=%s dry_run=%s max_orders=%d max_positions=%d desired=%s',
                 self.client.enabled, self.client.dry_run,
                 cfg.MAX_PENDING_ORDERS, cfg.SLOTS, cfg.DESIRED_FILE)
        LOG.info('input paths root=%s state=%s desired=%s price=%s account=%s feedback=%s log=%s',
                 cfg.ROOT.resolve(), cfg.STATE_DIR.resolve(), cfg.DESIRED_FILE.resolve(),
                 cfg.PRICE_FILE.resolve(), cfg.ACCOUNT_STREAM_DIR.resolve(),
                 cfg.EXECUTION_FEEDBACK_FILE.resolve(), cfg.TRADE_LOG_FILE.resolve())
        LOG.info('network I/O nonblocking=%s workers=%s; main loop owns state and signal polling',
                 self._network_io is not None, self._network_io.worker_count if self._network_io else 0)
        LOG.info('entry gaps: first5<=%.2f%% fill6..8<=%.2f%%; '
                 'rotate at any count >=%.2fpp closer, at >=5 orders >=%.2fpp closer; '
                 'replacement bypasses admission tiers; cancel gap>%.2f%%',
                 cfg.BOOTSTRAP_PLACE_GAP*100, cfg.FILL_PLACE_GAP*100,
                 cfg.SWITCH_TOLERANCE*100, cfg.CROWDED_SWITCH_TOLERANCE*100, cfg.CANCEL_GAP*100)
        LOG.info('entry plans: immediate individual WS by default; optional V9711_LIVE_ENTRY_BATCH=1 REST chunks<=5; entry cancels independently paced')
        LOG.info('entry cancels: nearest absolute relative gap first; one at a time; '
                 '1.5s after previous response; exit/QUEUE cancels and other I/O bypass this lane')
        LOG.info('close policy=MARKET at exit_due_epoch; timed_exit_lead=%.1fs; '
                 'legacy_QUEUE=confirm_cancel_then_market_residual; unknown_fate=query_only',
                 close_policy.TIMED_EXIT_LEAD_SECONDS)
        if self.once:
            self.step()
            return 0
        # Only stat probes run at high frequency. Maintenance keeps exact-fill
        # reconciliation, worker results, TTL checks and timed exits alive even
        # when signal/prices stop changing; it is NOT a broker-request timer.
        poller = LocalChangePoller((cfg.DESIRED_FILE, cfg.PRICE_FILE),
                                   maintenance_seconds=cfg.TRADE_MAINTENANCE_SECONDS)
        LOG.info('local signal/price stat probes=%.3fs; reconciliation/safety maintenance=%.3fs',
                 cfg.TRADE_POLL_SECONDS, cfg.TRADE_MAINTENANCE_SECONDS)
        while True:
            started = time.monotonic()
            try:
                if poller.due(now=started):
                    self.step()
            except KeyboardInterrupt:
                LOG.info('stopped by user')
                return 0
            except Exception:
                LOG.exception('trade step failed; retrying')
            time.sleep(probe_sleep(cfg.TRADE_POLL_SECONDS, started))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--paper', action='store_true', help='Use isolated var/paper_state; paths do not read .env.')
    parser.add_argument('--enable-live', action='store_true',
                        help='Allow the exchange adapter to connect and submit exact orders.')
    parser.add_argument('--no-dry-run', action='store_true',
                        help='Disable local simulation; requires --enable-live.')
    parser.add_argument('--sync-signal-ledger', action='store_true',
                        help='MANUAL one-time signal funds/positions import, with backup, then EXIT; no orders. Stop signal/trade first.')
    parser.add_argument('--sync-price', action='append', default=[], metavar='SYMBOL=PRICE',
                        help='With --sync-signal-ledger: override imported entry price, preserving notional.')
    parser.add_argument('--sync-symbol', action='append', default=None, metavar='SYMBOL',
                        help='With --sync-signal-ledger: import only selected signal positions (repeatable).')
    parser.add_argument('--log-level', default='INFO')
    parser.add_argument('--check-inputs', action='store_true', help='Read-only WS-direct paths and contract diagnostics.')
    args = parser.parse_args()
    if args.paper and args.no_dry_run:
        parser.error('--paper cannot be combined with --no-dry-run')
    if (args.sync_price or args.sync_symbol is not None) and not args.sync_signal_ledger:
        parser.error('--sync-price/--sync-symbol require --sync-signal-ledger')
    if args.sync_signal_ledger:
        from .manual_sync import parse_prices, sync
        try:
            result = sync(cfg.STATE_DIR, prices=parse_prices(args.sync_price), symbols=args.sync_symbol,
                          apply=True, paper=args.paper)
        except (OSError, RuntimeError, ValueError, KeyError, TypeError) as exc:
            parser.error(str(exc))
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0  # Deliberately do NOT instantiate an exchange client or start trade.
    if args.check_inputs:
        from .ws_direct import check_inputs
        print(json.dumps(check_inputs(), indent=2, ensure_ascii=False))
        return 0
    if not (args.enable_live and args.no_dry_run) and not cfg.PAPER_MODE:
        parser.error('v2 simulation requires --paper; do not write paper history into live state')
    if args.enable_live and args.no_dry_run and cfg.PAPER_MODE:
        parser.error('v2 live must not use --paper')
    cfg.STATE_DIR.mkdir(parents=True, exist_ok=True)
    from .async_logs import AsyncTradeLogs
    log_writer = AsyncTradeLogs(cfg.TRADE_LOG_FILE,
        getattr(logging, str(args.log_level).upper(), logging.INFO), MarketConsoleFilter())
    enabled = bool(args.enable_live)
    dry_run = not bool(args.no_dry_run)
    if enabled and dry_run:
        LOG.warning('live adapter connected with dry_run=True')
    # Reuse the collector's standard-library, OS-released single-writer lock.
    # Another trade process must not replace the same execution state files.
    from ..storage import SingleWriter
    try:
        from contextlib import ExitStack
        from pathlib import Path
        from ..contracts import read_json
        with ExitStack() as leases:
            leases.enter_context(SingleWriter(cfg.TRADE_LOCK_FILE))
            leases.enter_context(SingleWriter(cfg.PRICE_FILE.with_suffix('.writer.lock')))
            if enabled and not dry_run:
                from .startup_v2 import prepare_live_state, legacy_locations
                receipt = prepare_live_state(cfg.STATE_DIR, legacy_locations(cfg.ROOT), leases)
                LOG.info('live execution ledger ready mode=%s source=%s destination=%s; signal ledger untouched',
                         receipt.get('mode', 'previous_migration'), receipt.get('source'), cfg.STATE_DIR)
                if receipt.get('backup'):
                    LOG.warning('prior simulated/empty execution files backed up at %s; signal ledger unchanged',
                                receipt['backup'])
            from .ws_direct import DirectTrade
            return DirectTrade(once=args.once, enabled=enabled,
                               dry_run=dry_run).run()
    except RuntimeError as exc:
        LOG.error('WS-direct startup/runtime failed; check state/price locks and configuration: %s', exc)
        return 1
    finally:
        log_writer.close()


if __name__ == '__main__':
    raise SystemExit(main())
