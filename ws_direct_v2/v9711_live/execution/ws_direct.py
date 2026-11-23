"""Event-driven trade entrypoint. Signal publishes full-bar templates, not ticks."""
from __future__ import annotations
import heapq
import hashlib
import logging
import queue
import sys
import threading
import time
from pathlib import Path
from .. import config as cfg
from .runner import LiveTrade
from .ws_market import DirectMarket, DualMarket
from .metadata_sync import MetadataSync
from .position_identity import CAPITAL_MODE
from .sizing import CountSizingMixin
from .signal_follow import SignalFollowMixin
from .entry_quotes import qualify_quote, QUOTE_FIELDS
from .submit_errors import ConfirmedSubmitRejection
from .owner_work import OwnerWork
from .feedback_writer import FeedbackWriter
from .background_account import BackgroundAccountView
from .background_signal import BackgroundSignal
from .background_terminal import BackgroundTerminal
from .stream import AccountStreamView
from ..entry_policy import GapPolicy, at_most
from collectors.ticker_ws import load_symbols

LOG = logging.getLogger('v9711.ws_direct')


def check_inputs():
    from .startup_v2 import legacy_locations
    return dict(legacy_trade_lock_dirs=[str(p) for p in legacy_locations(cfg.ROOT)],
                version=cfg.WS_DIRECT_MODE, root=str(cfg.ROOT), state=str(cfg.STATE_DIR.resolve()),
                plans=str(cfg.DESIRED_FILE.resolve()), price_output=str(cfg.PRICE_FILE.resolve()),
                bars=str(cfg.STATUS_FILE.resolve()), account=str(cfg.ACCOUNT_STREAM_DIR.resolve()),
                symbols=str(cfg.SYMBOLS_FILE.resolve()), filters=str(cfg.EXCHANGE_INFO_CACHE_FILE.resolve()),
                decision_input='internal WS memory only; never latest.json',
                order='receive -> owner reservation/snapshot/dispatch -> mirror; worker waits durable receipt before send')


class DirectTrade(SignalFollowMixin, CountSizingMixin, LiveTrade):
    def _network_wake(self):
        self.market._wake()  # Worker only enqueues; the owner applies all results.

    def _account_read_wake(self):
        self._account_changed.set()
        self.market._wake()

    def _signal_read_wake(self):
        self._signal_changed.set()
        self.market._wake()

    def __init__(self, **kwargs):
        self.market = DualMarket(load_symbols(cfg.SYMBOLS_FILE), cfg.PRICE_FILE)
        self._fast_tick = False
        self._account_deadline = 0.0
        self._template_revision = -1
        self._template_file_revision = -1
        self._templates = {}
        self._queue_targets = frozenset()
        self._batch_event_contexts = {}
        self._eligible = {}
        self._rank_heap = []
        self._quote_versions = {}
        self._active_price_symbols = None
        self._cycle_price_maps = None
        self._quote_signal_gate = None
        self._submit_skip_reasons = {}
        self._skipped_far_events = 0
        self._skipped_noncompetitive_events = 0
        self._owner_work = OwnerWork()
        self._quote_routes = {}
        self._feedback_writer = None
        self._account_reader = None
        self._signal_reader = None
        self._signal_changed = threading.Event()
        self._account_changed = threading.Event()
        self._background_terminal = None
        self._servicing_quotes = False
        self._maintenance_mark = None
        self._last_presentation_mono = None
        self._deferred_presentations = 0
        self._previous_switch_interval = None
        self._live_switch_interval = None
        self._runtime_source = dict(build='20261123-frozen-entry-market-exit-v13', source=str(Path(__file__).resolve()),
                                    source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12])
        super().__init__(**kwargs)
        self._desired_path_display = str(cfg.DESIRED_FILE.resolve())
        self.client.entry_account_guard = self._account_send_allowed
        self.client.entry_quote_guard = self._entry_quote_send_allowed
        self.client.entry_quote_snapshot = self._entry_quote_send_snapshot
        self._refresh_precision_readiness()

    def enable_nonblocking_io(self):
        interval = None
        if self.client.enabled and not self.client.dry_run and self._previous_switch_interval is None:
            interval = cfg.env_float('V9711_LIVE_GIL_SWITCH_SECONDS', .001)
            if not .0005 <= interval <= .005:
                raise ValueError('V9711_LIVE_GIL_SWITCH_SECONDS must be in [0.0005, 0.005]')
        super().enable_nonblocking_io()
        if self.client.enabled and not self.client.dry_run:
            self.ledger.enable_background(incremental=True)
            self.market.events.enable_ingress_screening()
            if self._previous_switch_interval is None:
                # CPython's default 5ms GIL interval can be paid repeatedly
                # between the quote owner, disk writer and socket loop.
                self._previous_switch_interval = sys.getswitchinterval()
                self._live_switch_interval = interval
                sys.setswitchinterval(interval)
        self._network_io.callback_limit = 8
        self._network_io.callback_budget_seconds = .001
        if self._feedback_writer is None:
            self._feedback_writer = FeedbackWriter(cfg.EXECUTION_FEEDBACK_FILE)
        if (self._account_reader is None and self.client.enabled and not self.client.dry_run
                and type(self.client.stream) is AccountStreamView):
            self._account_reader = BackgroundAccountView(self.client.stream, owner_wake=self._account_read_wake)
            self.client.stream = self._account_reader
        if self._background_terminal is None:
            self._background_terminal = BackgroundTerminal(self.view)
            self.view = self._background_terminal

    def enable_signal_reader(self):
        if self._signal_reader is None:
            self._signal_reader = BackgroundSignal(self._desired_reader, owner_wake=self._signal_read_wake)
            self._desired_reader = self._signal_reader

    def shutdown_io(self):
        try:
            if self._signal_reader is not None:
                self._signal_reader.close()
            super().shutdown_io()
        finally:
            try:
                self.ledger.close_background()
            finally:
                if self._feedback_writer is not None:
                    self._feedback_writer.close()
                if self._account_reader is not None:
                    self._account_reader.close()
                    self.client.stream = self._account_reader.source
                if self._background_terminal is not None:
                    self._background_terminal.close()
                if self._previous_switch_interval is not None and sys.getswitchinterval() == self._live_switch_interval:
                    sys.setswitchinterval(self._previous_switch_interval)

    def _write_feedback_payload(self, payload):
        if self._feedback_writer is None:
            return super()._write_feedback_payload(payload)
        self._feedback_writer.submit(payload)

    def _refresh_precision_readiness(self):
        # Only startup/maintenance reads the cache; no extra file read per tick.
        metadata = self.client.refresh_metadata_snapshot()
        self._precision_ready = isinstance(metadata, dict) and bool(metadata.get('symbols'))

    def _account_send_allowed(self):
        return (not self.client.enabled or self.client.dry_run
                or (time.time() <= self._account_deadline
                    and (self._signal_reader is None or self._signal_reader.healthy())))

    def _entry_quote_send_allowed(self, symbol, cid, limit):
        return self._entry_quote_send_snapshot(symbol, cid, limit)['entry_qualified_by'] != 'none'

    def _entry_quote_send_snapshot(self, symbol, cid, limit):
        # Exact admission evidence, frozen by the owner. No WS read, freshness
        # test or distance comparison after this entry has been admitted.
        detail = self.client.entry_gate.get('admitted_entry_quotes', {}).get(cid)
        if (not detail or detail.get('symbol') != symbol
                or detail.get('entry_qualified_by') not in {'tick', 'depth', 'all', 'signal_follow'}):
            raise ConfirmedSubmitRejection('exact entry admission missing before send')
        return {key: detail[key] for key in QUOTE_FIELDS if key in detail}

    def step(self, *, now=None):
        started = self._maintenance_mark = time.perf_counter_ns()
        self._maintenance_segments = {}
        try:
            stamp = time.time() if now is None else now
            health = self._stream_health()
            if health.get('fresh'):
                self._account_deadline = stamp + cfg.ACCOUNT_STREAM_MAX_AGE_SECONDS - float(health.get('age_seconds', 0))
            else:
                self._account_deadline = 0
            self._maintenance_checkpoint('account_health')
            self._refresh_precision_readiness()
            self._maintenance_checkpoint('metadata')
            # Reconciliation may update many exact records. Coalesce routine
            # saves; _run_network still flushes entry/close identities before
            # dispatch, including when maintenance schedules an urgent close.
            with self.ledger.batch_saves():
                return super().step(now=now)
        finally:
            self._maintenance_checkpoint('remaining')
            self._owner_work.finish('maintenance', started, self._maintenance_segments)
            self._maintenance_mark = None

    def _maintenance_checkpoint(self, stage):
        if self._maintenance_mark is not None:
            now = time.perf_counter_ns()
            self._maintenance_segments[stage] = (now-self._maintenance_mark)/1e6
            self._maintenance_mark = now

    def _maintenance_admission_turn(self):
        # Only called at explicit state-consistent phase boundaries. The same
        # owner still reserves capacity; no worker can mutate tracked/positions.
        if self._maintenance_mark is not None and not self._servicing_quotes:
            self._servicing_quotes = True
            started = time.perf_counter_ns()
            try:
                self._service_pending_quotes(limit=32, budget_seconds=.001)
            finally:
                elapsed = time.perf_counter_ns()-started
                self._maintenance_segments['admission_turn'] = self._maintenance_segments.get('admission_turn', 0)+elapsed/1e6
                self._maintenance_mark += elapsed
                self._servicing_quotes = False

    def _render_trade(self, **kwargs):
        try:
            return super()._render_trade(**kwargs)
        finally:
            self._maintenance_checkpoint('render')

    def _presentation_due(self):
        # Presentation is optional work, not exact accounting or close timers.
        # Give a burst a chance to drain before another full feedback scan;
        # continuous load still receives at least one update per second.
        now = time.monotonic()
        if (not self.once and self.market.events.qsize() > 32
                and self._last_presentation_mono is not None
                and now-self._last_presentation_mono < 1.):
            self._deferred_presentations += 1
            return False
        self._last_presentation_mono = now
        return True

    def _execute_entries(self, snapshot, status, **kwargs):
        with self.ledger.dispatch_group(), self.ledger.batch_saves():
            return self._execute_entries_batched(snapshot, status, **kwargs)

    def _execute_entries_batched(self, snapshot, status, **kwargs):
        # Reuse one read within this decision only. No owner state or quote
        # view is cached across events, and worker send guards stay live.
        symbols = self._active_price_symbols if self._fast_tick else None
        previous = self._cycle_price_maps
        self._cycle_price_maps = self.market.price_maps(symbols)
        try:
            # A planner pass reserves one exact replacement. Finish other
            # independent replacements in this owner turn, rather than waiting
            # for another quote/maintenance tick. All reservations share the
            # surrounding durability receipt; paced cancels remain asynchronous.
            # Only the fast shortlist is repeated, never maintenance's full
            # universe. Stop immediately when no exact new identity was made.
            for _ in range(cfg.MAX_PENDING_ORDERS):
                before = {row.get('client_order_id') for row in self.tracked.values()}
                result = self._execute_entries_with_prices(snapshot, status, **kwargs)
                if self._network_io is None or not self._fast_tick:
                    break
                plan = result[3]
                after = {row.get('client_order_id') for row in self.tracked.values()}
                if (not plan.rotation_placement
                        or not after - before or self.ledger.state.get('pending_rotation')):
                    break
            return result
        finally:
            self._cycle_price_maps = previous

    def _execute_entries_with_prices(self, snapshot, status, **kwargs):
        snapshot = dict(snapshot)
        snapshot['_signal_snapshot_valid'] = status == 'valid'
        if status == 'valid':
            if not self.market.healthy():
                status = 'ws_not_ready'
            elif self.client.enabled and not self.client.dry_run and time.time() > self._account_deadline:
                status = 'account_not_ready'
        if status == 'valid' and not self._precision_ready:
            status = 'precision_cache_not_ready'
        snapshot = dict(snapshot)
        snapshot['desired'] = [self._entry_sizing_row(row)
                               for row in snapshot.get('desired', []) if isinstance(row, dict)]
        result = super()._execute_entries(snapshot, status, **kwargs)
        self._entry_diagnostics['template_total'] = len(self._templates)
        symbols = {s for s, row in self.tracked.items()
                   if not self._fast_tick or self._network_io is None or not row.get('cancel_requested')}
        symbols.update(self._entry_diagnostics.get('selected_to_place', []))
        symbols.update(row.get('symbol') for row in snapshot.get('desired', [])[:3] if isinstance(row, dict))
        quotes = {}
        for symbol in sorted(s for s in symbols if s):
            quote = self.market.entry_quote(symbol)
            limit = self._finite((self.tracked.get(symbol) or self._templates.get(symbol) or {}).get('limit_price'))
            quotes[symbol] = dict(quote, **{key+'_gap': (quote[key]-limit)/limit
                if quote.get(key) is not None and limit > 0 else None for key in ('tick', 'bid0', 'selected')})
        self._entry_diagnostics['entry_quotes'] = quotes
        for symbol, reason in self._entry_diagnostics['filtered'].items():
            if reason == 'missing_or_stale_price_or_invalid_limit':
                limit = self._finite(self._templates.get(symbol, {}).get('limit_price'))
                self._entry_diagnostics['filtered'][symbol] = (
                    'invalid_limit' if limit <= 0 else self.market.quote_state(symbol))
        return result

    def _place_order(self, row):
        symbol = str(row.get('symbol') or '').upper()
        ceiling = row.get('entry_gap_ceiling', GapPolicy().admission_gap(self._ordinary_pending_count()))
        context = dict(decision_epoch_ms=time.time()*1000, _decision_ns=time.monotonic_ns())
        quote = self.market.entry_quote(symbol)
        context.update(self.market.entry_timing(symbol, quote.get('source')))
        context.update(self._batch_event_contexts.get(symbol) or getattr(self, '_event_timing_context', {}) or {})
        detail = qualify_quote(quote, self._finite(row.get('limit_price')), ceiling,
                               following=bool(row.get('signal_follow')))
        if detail['entry_qualified_by'] == 'none':
            self._submit_skip_reasons[symbol] = 'admission_quote_not_qualified'
            return
        detail['entry_quote_policy'] = 'admission_frozen'
        return super()._place_order(dict(row, **detail, _entry_timing_context=context))

    def desired_snapshot(self, *, now):
        payload, status = super().desired_snapshot(now=now)
        if status != 'valid':
            return payload, status
        if payload.get('execution_mode') != cfg.WS_DIRECT_MODE or not isinstance(payload.get('candidate_templates'), list):
            return self._fresh_fallback(), 'incompatible_ws_direct_contract'
        payload['signal_discovery_desired'] = payload.get('desired') or []
        payload['desired'] = payload['candidate_templates']
        if self._template_file_revision != self._desired_reader.revision:
            templates = {row['symbol']:dict(row) for row in payload['candidate_templates']
                         if isinstance(row, dict) and row.get('symbol')}
            if templates != self._templates:
                self._templates = templates
                self._eligible.clear()
                self._rank_heap.clear()
                for symbol, price in self.market.entry_prices().items():
                    self._update_rank(symbol, price)
                self._template_revision += 1
            self._template_file_revision = self._desired_reader.revision
        self._queue_targets = frozenset(self._target_positions(payload))
        self.market.events.set_policy(self._templates,
            set(self._eligible) | set(self.tracked) | set(self.positions) | self._queue_targets)
        self._promote_signal_follow_orders(payload, now=now)
        return payload, status

    def refresh_prices(self, *, now=None):
        if self._cycle_price_maps is not None:
            return self._cycle_price_maps[0]
        return self.market.prices(self._active_price_symbols if self._fast_tick else None)

    def refresh_entry_prices(self, *, now=None):
        if self._cycle_price_maps is not None:
            return self._cycle_price_maps[2]
        return self.market.entry_prices(self._active_price_symbols if self._fast_tick else None)

    def _publish_entry_gate(self):
        self.market.events.set_policy(self._templates,
            set(self._eligible) | set(self.tracked) | set(self.positions) | self._queue_targets)
        snapshot, status = self._io_signal
        # Market readiness is checked when admitting new entries. Once a CID
        # is reserved, publication must not reinstate a secondary WS gate.
        if self.client.enabled and not self.client.dry_run and time.time() > self._account_deadline:
            self._io_signal = (snapshot, 'account_not_ready')
        try:
            super()._publish_entry_gate()
        finally:
            self._io_signal = snapshot, status

    def _update_rank(self, symbol, price):
        row = self._templates.get(symbol)
        version = self._quote_versions.get(symbol, 0)+1
        self._quote_versions[symbol] = version
        limit = self._finite((row or {}).get('limit_price'))
        gap = (price-limit)/limit if limit > 0 else float('inf')
        self._eligible.pop(symbol, None)
        if row and gap <= cfg.CANCEL_GAP and not self._unowned_rank_blocked(symbol, time.time()):
            item = (gap, symbol, version)
            self._eligible[symbol] = item
            heapq.heappush(self._rank_heap, item)
        if len(self._rank_heap) > max(64, 4*len(self._eligible)):
            self._rank_heap = list(self._eligible.values())
            heapq.heapify(self._rank_heap)
        self.market.events.watch(symbol, symbol in self._eligible or symbol in self.tracked
                                 or symbol in self.positions or symbol in self._queue_targets)

    def _unowned_rank_blocked(self, symbol, now):
        if symbol in self.tracked or symbol in self.positions:
            return False
        row = self._templates.get(symbol)
        return bool(row and (now < self._finite(self.ledger.state['reopen_until_epoch'].get(symbol))
                            or self._entry_submission_blocked(self._entry_sizing_row(row))))

    def _fast_snapshot(self, snapshot, tick, now):
        effective = self.market.entry_prices([tick.symbol]).get(tick.symbol, float('inf'))
        self._update_rank(tick.symbol, effective)
        target_symbols = set(self._target_positions(snapshot))
        selected, restore = set(self.tracked) | target_symbols, []
        valid = 0
        while self._rank_heap and valid < cfg.MAX_PENDING_ORDERS:
            item = heapq.heappop(self._rank_heap)
            gap, symbol, version = item
            if self._eligible.get(symbol) != item:
                continue
            if not self.market.order_allowed(symbol):
                self._eligible.pop(symbol, None)
                continue
            effective = self.market.entry_prices([symbol]).get(symbol, float('inf'))
            limit = float(self._templates[symbol]['limit_price'])
            if (effective-limit)/limit != gap:
                self._update_rank(symbol, effective)
                continue
            restore.append(item)
            row = self._entry_sizing_row(self._templates[symbol])
            if (symbol in target_symbols or symbol in self.positions or symbol in self.tracked
                    or now < self._finite(self.ledger.state['reopen_until_epoch'].get(symbol))
                    or self._entry_submission_blocked(row)):
                continue
            selected.add(symbol)
            valid += 1
        for item in restore:
            heapq.heappush(self._rank_heap, item)
        working = dict(snapshot)
        pending_victim = (self.ledger.state.get('pending_rotation') or {}).get('victim')
        working['desired'] = [self._templates[symbol] for symbol in sorted(selected)
                              if symbol in self._templates and
                              (self._network_io is None or not (self.tracked.get(symbol) or {}).get('cancel_requested')
                               or symbol == pending_victim)]
        # Keep ALL tracked prices for the paced cancel lane's distance ranking,
        # even though retired-from-admission rows do not enter the new plan.
        self._active_price_symbols = selected
        return working

    def _best_unowned_gap(self, excluded):
        """Peek a fresh challenger, keeping owned ranks for later retirement.

        A low-priced incumbent cannot replace another incumbent. Including it
        in this bound forces the full planner on every unrelated quote.
        """
        restore = []
        try:
            while self._rank_heap:
                item = self._rank_heap[0]
                gap, symbol, version = item
                if self._eligible.get(symbol) != item:
                    heapq.heappop(self._rank_heap)
                    continue
                if symbol in excluded:
                    restore.append(heapq.heappop(self._rank_heap))
                    continue
                if self._unowned_rank_blocked(symbol, time.time()):
                    heapq.heappop(self._rank_heap)
                    self._eligible.pop(symbol, None)
                    continue
                effective = self.market.entry_prices([symbol]).get(symbol)
                if effective is None:
                    heapq.heappop(self._rank_heap)
                    self._eligible.pop(symbol, None)
                    continue
                current = (effective-float(self._templates[symbol]['limit_price']))/float(self._templates[symbol]['limit_price'])
                if current != gap:
                    heapq.heappop(self._rank_heap)
                    self._update_rank(symbol, effective)
                    continue
                return gap
            return float('inf')
        finally:
            for item in restore:
                heapq.heappush(self._rank_heap, item)

    def _no_entry_action(self, tick, targets, now):
        """Conservative no-action check for every ordinary order count.

        File changes and completions bypass this method. Owned/target quotes
        must still pass cancel/follow/capacity checks before skipping work.
        Time-based reconciliation still runs in maintenance.
        """
        ordinary = {s: r for s, r in self.tracked.items()
                    if not r.get('signal_follow') and not r.get('cancel_requested')}
        self._quote_route = 'full_follow'
        if tick.symbol in targets and tick.symbol not in self.positions:
            follow = self.tracked.get(tick.symbol) or {}
            if (not follow.get('signal_follow') or follow.get('cancel_requested')
                    or follow.get('signal_follow_position_id') != targets[tick.symbol].get('position_id')):
                return False  # a newly available signal target must be evaluated
        if any(r.get('signal_follow') and not r.get('cancel_requested')
               and (s in self.positions or len(self.positions) >= cfg.SIGNAL_FOLLOW_POSITION_STOP
                    or self._forced_entry_blocked(s, now=now) or s not in targets)
               for s, r in self.tracked.items()):
            return False
        pending = self.ledger.state.get('pending_rotation')
        self._quote_route = 'full_rotation_state'
        if pending:
            victim = str(pending.get('victim') or '')
            old = self.tracked.get(victim)
            if (now*1000 >= self._finite(pending.get('expires_at_ms'))
                    or pending.get('victim_filled') or victim in self.positions
                    or (old and (not old.get('cancel_requested')
                        or self._finite(old.get('accounted_entry_qty')) > 0
                        or str(old.get('client_order_id') or '') != str(pending.get('victim_client_order_id') or '')))
                    or (old is None and not pending.get('victim_terminal'))):
                return False  # preserve all exact-state transitions/unknown reservations
        self._quote_route = 'full_order_count'
        if len(ordinary) > cfg.MAX_PENDING_ORDERS:
            return False
        quote = self.market.entry_quote(tick.symbol)['selected']
        self._update_rank(tick.symbol, quote if quote is not None else float('inf'))
        # Full positions still need one full pass to mark resting ordinary
        # entries for cancellation. Once marked, unrelated ticks cannot admit.
        if len(self.positions) >= cfg.SLOTS and not self._in_final_pre_exit_bar(now=now):
            return not ordinary and not pending
        weight = cfg.SLOT_WEIGHTS[min(len(self.positions), 2)]
        self._quote_route = 'full_sizing_or_bar'
        if any(s in self.positions or r.get('sizing_weight') != weight
               or not int(r.get('order_bar_open_ms') or 0) <= now*1000 < int(r.get('order_bar_open_ms') or 0)+cfg.BAR_MS
               for s, r in ordinary.items()):
            return False
        prices = self.market.entry_prices(ordinary)
        gaps = [(prices[s]-float(r['limit_price']))/float(r['limit_price'])
                for s, r in ordinary.items() if s in prices and float(r.get('limit_price') or 0) > 0]
        self._quote_route = 'full_far_order'
        if (gaps and not at_most(max(gaps), cfg.CANCEL_GAP)):
            return False
        excluded = set(self.tracked) | set(self.positions) | set(targets)
        if pending:
            self._quote_route = 'full_reserved_replacement'
            excluded.add(str(pending.get('victim') or ''))
        best = self._best_unowned_gap(excluded)
        policy, count = GapPolicy(), len(ordinary)
        if pending:
            # A rejected replacement may leave a durable reservation while
            # quotes rebound. Retain its original gate without running the
            # entire planner on every nonqualifying event.
            if count >= cfg.MAX_PENDING_ORDERS:
                return True
            if policy.replacement_allowed(self._finite(pending.get('victim_gap'), float('nan')), best,
                    ceiling=cfg.CANCEL_GAP, count=int(self._finite(pending.get('original_order_count')))):
                return False
            return not at_most(best, policy.admission_gap(count))
        self._quote_route = 'full_vacancy'
        if count < cfg.MAX_PENDING_ORDERS and at_most(best, policy.admission_gap(count)):
            return False
        # Missing incumbent quotes retain their physical slots, but cannot
        # authorize distance cancellation or rotation. The full planner also
        # requires ALL active incumbents to be ranked before replacing one.
        # Replanning every healthy quote during an unrelated outage creates
        # a backlog; still evaluate genuine vacancies and recovery crossings.
        self._quote_route = 'full_competitive_replacement'
        return not (len(gaps) == len(ordinary) and gaps and policy.replacement_allowed(max(gaps), best,
                    ceiling=cfg.CANCEL_GAP, count=count))

    def on_tick(self, tick):
        started = self._quote_mark = time.perf_counter_ns()
        feed = self.market.book if tick.source == 'bid0' else self.market
        dequeued = getattr(feed, '_dequeued_context', None)
        dequeued_ns, dequeued_ms = ((dequeued[1], dequeued[2]) if dequeued and dequeued[0] is tick
                                   else (time.monotonic_ns(), time.time()*1000))
        self._event_timing_context = dict(event_received_epoch_ms=tick.received_epoch_ms,
            _event_received_ns=tick.received_ns, dequeued_epoch_ms=dequeued_ms,
            _dequeued_ns=dequeued_ns, trigger_source=tick.source, trigger_symbol=tick.symbol)
        self._quote_segments, self._quote_route, self._quote_tail = {}, 'full', 'screening'
        try:
            with self.ledger.batch_saves():
                return self._on_tick(tick)
        finally:
            self._batch_event_contexts.clear()
            self._event_timing_context = None
            self._quote_checkpoint(self._quote_tail)
            self._quote_routes[self._quote_route] = self._quote_routes.get(self._quote_route, 0)+1
            self._owner_work.finish('quote', started, self._quote_segments, route=self._quote_route)

    def _quote_checkpoint(self, stage):
        now = time.perf_counter_ns()
        self._quote_segments[stage] = (now-self._quote_mark)/1e6
        self._quote_mark = now

    def _collect_distinct_candidates(self, tick):
        if self._network_io is None:
            return
        excluded = set(self._eligible) | set(self.tracked) | set(self.positions) | self._queue_targets | {tick.symbol}
        for _ in range(cfg.MAX_PENDING_ORDERS-1):
            candidate = self.market.events.take_distinct_candidate(excluded)
            if candidate is None:
                break
            excluded.add(candidate.symbol)
            def accept(event):
                feed = self.market.book if event.source == 'bid0' else self.market
                _, stamp_ns, stamp_ms = feed._dequeued_context
                self._batch_event_contexts[event.symbol] = dict(
                    event_received_epoch_ms=event.received_epoch_ms, _event_received_ns=event.received_ns,
                    dequeued_epoch_ms=stamp_ms, _dequeued_ns=stamp_ns,
                    trigger_source=event.source, trigger_symbol=event.symbol)
                price = self.market.entry_prices([event.symbol]).get(event.symbol, float('inf'))
                self._update_rank(event.symbol, price)
                return False
            try:
                self.market.decide(candidate, accept)
            finally:
                self.market.events.task_done()

    def _on_tick(self, tick):
        now = time.time()
        previous_revision = self._template_file_revision
        snapshot, status = self.desired_snapshot(now=now)
        self._quote_checkpoint('signal')
        self._io_signal = snapshot, status
        completed = self._drain_network()
        changed = completed and getattr(self._network_io, 'last_drain_urgent', True)
        self._quote_checkpoint('callbacks')
        signal_gate = (status, bool(snapshot.get('signal_ready')))
        if signal_gate != self._quote_signal_gate:
            self._quote_signal_gate = signal_gate
            if status != 'valid' or not signal_gate[1]:
                self._publish_entry_gate()
        if ((status != 'valid' or not signal_gate[1]) and not changed
                and tick.symbol not in self.tracked):
            # No new entry can be admitted. Keep committing every quote for
            # signal/valuation; reconcile resting orders on their own quotes
            # and periodic maintenance, not on every unrelated market event.
            self._quote_route = 'signal_not_ready'
            return False
        previous_ids = {row.get('client_order_id') for row in self.tracked.values()}
        targets = self._target_positions(snapshot)
        # A new far quote cannot enter the shortlist, change any incumbent's
        # distance, or become a follow order. Still process every crossing and
        # every update of an existing eligible/owned/target symbol. Periodic
        # maintenance handles all time/state changes independently of ingress.
        if (status == 'valid' and previous_revision == self._template_file_revision and not changed
                and tick.symbol not in self.tracked and tick.symbol not in self.positions and tick.symbol not in targets
                and tick.symbol not in self._eligible and not self.ledger.state.get('pending_rotation')):
            template = self._templates.get(tick.symbol) or {}
            limit = self._finite(template.get('limit_price'))
            quote = self.market.entry_quote(tick.symbol)['selected']
            if not template or (quote is not None and limit > 0 and (quote-limit)/limit > cfg.CANCEL_GAP):
                self._update_rank(tick.symbol, quote if quote is not None else float('inf'))
                self._skipped_far_events += 1
                self._quote_route = 'far'
                return False
        # Check genuine unowned challengers at partial/full order counts and
        # while position capacity is closed, not just uniform empty/full books.
        if (status == 'valid' and previous_revision == self._template_file_revision and not changed
                and self._no_entry_action(tick, targets, now)):
            self._skipped_noncompetitive_events += 1
            self._quote_route = 'no_action'
            return False
        # A far quote changed no signal/order/position state. Rebuilding the
        # immutable worker gate for it cannot change admission; workers still
        # check the frozen admission identity, account state, TTL and capacity.
        # Maintenance and every state-changing path continue publishing it.
        self._quote_checkpoint('screening')
        self._collect_distinct_candidates(tick)
        self._publish_entry_gate()
        self._quote_checkpoint('gate')
        working = self._fast_snapshot(snapshot, tick, now)
        self._quote_checkpoint('ranking')
        self._quote_tail = 'plan_dispatch'
        # All mutations, rankings and capacity reservations have one owner.
        self._fast_tick = True
        try:
            with self.entry_cancel_plan():
                self._execute_entries(working, status, now=now, targets=targets,
                                      final_exit_bar=self._in_final_pre_exit_bar(now=now))
                self._entry_diagnostics['template_total'] = len(self._templates)
        finally:
            self._fast_tick = False
        submitted = bool({row.get('client_order_id') for row in self.tracked.values()} - previous_ids)
        if submitted and self._network_io is not None:
            # The writer/sender have just been made runnable. Yield this
            # quantum instead of immediately taking more owner/ingress GIL
            # turns. No disk, exchange response or timed sleep is awaited.
            time.sleep(0)
        return submitted

    def write_feedback(self, **kwargs):
        self._maintenance_checkpoint('pre_feedback')
        # Exposed in the persistent diagnostics snapshot, not printed per tick.
        self._entry_diagnostics = dict(getattr(self, '_entry_diagnostics', {}),
                                       ws_direct=self.market.metrics())
        self._entry_diagnostics['ws_direct']['skipped_far_events'] = self._skipped_far_events
        self._entry_diagnostics['ws_direct']['skipped_noncompetitive_events'] = self._skipped_noncompetitive_events
        self._entry_diagnostics['ws_direct']['deferred_presentations'] = self._deferred_presentations
        self._entry_diagnostics['ws_direct']['owner_work'] = self._owner_work.snapshot()
        self._entry_diagnostics['ws_direct']['quote_routes'] = dict(self._quote_routes)
        self._entry_diagnostics['ws_direct']['runtime'] = self._runtime_source
        self._entry_diagnostics['ws_direct']['entry_latency'] = self.session_entries.timings.summary()
        if self.ledger._background is not None:
            self._entry_diagnostics['ws_direct']['durability_writer'] = self.ledger._background.health()
            self._entry_diagnostics['ws_direct']['durability_writer']['gil_switch_ms'] = sys.getswitchinterval()*1000
        if self._account_reader is not None:
            self._entry_diagnostics['ws_direct']['account_reader'] = self._account_reader.diagnostics()
        self._entry_diagnostics['ws_direct']['feedback_writer'] = (
            self._feedback_writer.health() if self._feedback_writer is not None else {'enabled': False})
        try:
            super().write_feedback(**kwargs)
        finally:
            self._maintenance_checkpoint('feedback')

    def _service_pending_quotes(self, *, limit=32, budget_seconds=.002):
        """Give queued entry opportunities a bounded turn before maintenance."""
        started, count = time.perf_counter(), 0
        while count < limit and time.perf_counter()-started < budget_seconds:
            try:
                tick = self.market.events.get_nowait()
            except queue.Empty:
                break
            try:
                if tick is None:
                    self._drain_network()
                    self._publish_entry_gate()
                else:
                    self.market.decide(tick, self.on_tick)
            finally:
                self.market.events.task_done()
            count += 1
        return count

    def _next_exit_wake_delay(self, *, now):
        deadlines = [max(self._finite(pos.get('exit_due_epoch'), float('inf')),
                         self._finite(pos.get('close_retry_after_epoch')))
                     for pos in self.positions.values() if not pos.get('close_client_order_id')]
        return max(0., min(deadlines, default=float('inf'))-now)

    def _run_loop(self):
        LOG.info('WS DIRECT runtime=%s mode=%s inputs=%s', self._runtime_source, cfg.WS_DIRECT_MODE, check_inputs())
        if self.client.enabled and not self.client.dry_run:
            from ..contracts import read_json
            marker = read_json(cfg.STATE_DIR / 'WS_MIGRATION.json') or {}
            if ((cfg.STATE_DIR / '.migration_in_progress').exists()
                    or (cfg.STATE_DIR / '.manual_signal_sync_pending.json').exists() or not marker.get('completed')
                    or not cfg.EXECUTION_INTENT_FILE.is_file() or not cfg.EXECUTION_LEDGER_FILE.is_file()):
                raise RuntimeError('live execution startup not prepared; launch execution.runner for automatic ledger detection; never delete existing ledgers')
        if self.once:
            self.step()
            return 0
        if self.client.enabled and not self.client.dry_run:
            self.enable_signal_reader()
        metadata_sync = MetadataSync(cfg.EXCHANGE_INFO_CACHE_FILE)
        metadata_sync.start()
        next_maintenance = time.monotonic()
        try:
            self.market.start()
            while True:
                if self._signal_changed.is_set():
                    self._signal_changed.clear()
                    next_maintenance = 0
                if self._account_changed.is_set():
                    self._account_changed.clear()
                    self._last_forced_probe = self._last_manual_flat_probe = None
                    next_maintenance = 0  # Apply changed exact evidence without a polling-cycle wait.
                now_mono = time.monotonic()
                # Wake at a pending position's due time rather than waiting
                # for the ordinary 250ms maintenance interval. Existing exact
                # close IDs continue normal reconciliation, without spinning.
                next_maintenance = min(next_maintenance,
                    now_mono+self._next_exit_wake_delay(now=time.time()))
                if now_mono >= next_maintenance:
                    # Exact fill/forced exit/close timers remain bounded even
                    # under continuous WS load; never wait for an exchange ACK.
                    try:
                        self._service_pending_quotes()
                        self.step()
                    except Exception:
                        self._account_deadline = 0
                        self._publish_entry_gate()
                        LOG.exception('maintenance failed; new entries suspended until recovery')
                    next_maintenance = time.monotonic()+min(.25, cfg.TRADE_MAINTENANCE_SECONDS)
                try:
                    tick = self.market.events.get(timeout=max(0, next_maintenance-time.monotonic()))
                except queue.Empty:
                    continue
                try:
                    if tick is None:
                        if self._drain_network() and self._network_io.last_drain_urgent:
                            next_maintenance = 0  # Reconcile/replacement without a 250ms polling wait.
                        self._publish_entry_gate()
                    else:
                        self.market.decide(tick, self.on_tick)
                except Exception:
                    self._account_deadline = 0
                    self._publish_entry_gate()
                    LOG.exception('WS decision failed; tick not committed to price mirror')
                    next_maintenance = 0
                finally:
                    self.market.events.task_done()
        except KeyboardInterrupt:
            LOG.info('WS direct stopped')
            return 0
        finally:
            metadata_sync.stop()
            self.market.close()
