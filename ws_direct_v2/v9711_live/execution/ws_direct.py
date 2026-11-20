"""Event-driven trade entrypoint. Signal publishes full-bar templates, not ticks."""
from __future__ import annotations
import heapq
import logging
import queue
import time
from pathlib import Path
from .. import config as cfg
from .runner import LiveTrade
from .ws_market import DirectMarket, DualMarket
from .metadata_sync import MetadataSync
from .position_identity import CAPITAL_MODE
from .sizing import CountSizingMixin
from .signal_follow import SignalFollowMixin
from .entry_quotes import qualify_quote
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
                order='receive -> decision/durable registration/dispatch -> mirror commit -> asynchronous file')


class DirectTrade(SignalFollowMixin, CountSizingMixin, LiveTrade):
    def _network_wake(self):
        self.market._wake()  # Worker only enqueues; the owner applies all results.

    def __init__(self, **kwargs):
        self.market = DualMarket(load_symbols(cfg.SYMBOLS_FILE), cfg.PRICE_FILE)
        self._fast_tick = False
        self._account_deadline = 0.0
        self._template_revision = -1
        self._template_file_revision = -1
        self._templates = {}
        self._eligible = {}
        self._rank_heap = []
        self._quote_versions = {}
        self._active_price_symbols = None
        self._cycle_price_maps = None
        self._skipped_far_events = 0
        self._skipped_noncompetitive_events = 0
        super().__init__(**kwargs)
        self._desired_path_display = str(cfg.DESIRED_FILE.resolve())
        self.client.market_guard = self._market_send_allowed
        self.client.entry_quote_guard = self._entry_quote_send_allowed
        self.client.entry_quote_snapshot = self._entry_quote_send_snapshot
        self._refresh_precision_readiness()

    def _refresh_precision_readiness(self):
        # Only startup/maintenance reads the cache; no extra file read per tick.
        metadata = self.client.refresh_metadata_snapshot()
        self._precision_ready = isinstance(metadata, dict) and bool(metadata.get('symbols'))

    def _market_send_allowed(self, symbol):
        account_ready = (not self.client.enabled or self.client.dry_run
                         or time.time() <= self._account_deadline)
        return account_ready and self.market.send_quote(symbol)['selected'] is not None

    def _entry_quote_send_allowed(self, symbol, cid, limit):
        return self._entry_quote_send_snapshot(symbol, cid, limit)['entry_qualified_by'] != 'none'

    def _entry_quote_send_snapshot(self, symbol, cid, limit):
        gate = self.client.entry_gate  # immutable owner snapshot; no worker ledger access
        ceiling = gate.get('entry_gap_limits', {}).get(cid)
        return qualify_quote(self.market.send_quote(symbol), limit, ceiling,
                             following=cid in gate.get('follow_entries', {}))

    def step(self, *, now=None):
        stamp = time.time() if now is None else now
        health = self._stream_health()
        if health.get('fresh'):
            self._account_deadline = stamp + cfg.ACCOUNT_STREAM_MAX_AGE_SECONDS - float(health.get('age_seconds', 0))
        else:
            self._account_deadline = 0
        self._refresh_precision_readiness()
        return super().step(now=now)

    def _execute_entries(self, snapshot, status, **kwargs):
        # Reuse one read within this decision only. No owner state or quote
        # view is cached across events, and worker send guards stay live.
        symbols = self._active_price_symbols if self._fast_tick else None
        previous = self._cycle_price_maps
        self._cycle_price_maps = self.market.price_maps(symbols)
        try:
            return self._execute_entries_with_prices(snapshot, status, **kwargs)
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
        symbols = set(self.tracked) | set(self._entry_diagnostics.get('selected_to_place', []))
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
        context = dict(decision_epoch_ms=time.time()*1000, _decision_ns=time.monotonic_ns())
        quote = self.market.entry_quote(str(row.get('symbol') or '').upper())
        context.update(self.market.entry_timing(str(row.get('symbol') or '').upper(), quote.get('source')))
        ceiling = row.get('entry_gap_ceiling', GapPolicy().admission_gap(self._ordinary_pending_count()))
        detail = qualify_quote(quote, self._finite(row.get('limit_price')), ceiling,
                               following=bool(row.get('signal_follow')))
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
        snapshot, status = self._io_signal
        if not self.market.healthy() or (self.client.enabled and not self.client.dry_run
                                         and time.time() > self._account_deadline):
            self._io_signal = (snapshot, 'market_not_ready')
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
        if row and gap <= cfg.CANCEL_GAP:
            item = (gap, symbol, version)
            self._eligible[symbol] = item
            heapq.heappush(self._rank_heap, item)
        if len(self._rank_heap) > max(64, 4*len(self._eligible)):
            self._rank_heap = list(self._eligible.values())
            heapq.heapify(self._rank_heap)

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
        working['desired'] = [self._templates[symbol] for symbol in sorted(selected)
                              if symbol in self._templates]
        self._active_price_symbols = selected
        return working

    def on_tick(self, tick):
        now = time.time()
        previous_revision = self._template_file_revision
        snapshot, status = self.desired_snapshot(now=now)
        self._io_signal = snapshot, status
        completed = self._drain_network()
        previous_ids = {row.get('client_order_id') for row in self.tracked.values()}
        targets = self._target_positions(snapshot)
        # A new far quote cannot enter the shortlist, change any incumbent's
        # distance, or become a follow order. Still process every crossing and
        # every update of an existing eligible/owned/target symbol. Periodic
        # maintenance handles all time/state changes independently of ingress.
        if (status == 'valid' and previous_revision == self._template_file_revision and not completed
                and tick.symbol not in self.tracked and tick.symbol not in self.positions and tick.symbol not in targets
                and tick.symbol not in self._eligible and not self.ledger.state.get('pending_rotation')):
            template = self._templates.get(tick.symbol) or {}
            limit = self._finite(template.get('limit_price'))
            quote = self.market.entry_quote(tick.symbol)['selected']
            if not template or (quote is not None and limit > 0 and (quote-limit)/limit > cfg.CANCEL_GAP):
                self._update_rank(tick.symbol, quote if quote is not None else float('inf'))
                self._skipped_far_events += 1
                return False
        # Empty books cannot admit a quote outside the first tier; full books
        # cannot rotate without the required improvement. A conservative heap
        # bound avoids a full plan but never drops a crossing or incumbent event.
        ordinary = {s: r for s, r in self.tracked.items() if not r.get('signal_follow')}
        if (status == 'valid' and previous_revision == self._template_file_revision and not completed
                and len(ordinary) in (0, cfg.MAX_PENDING_ORDERS)
                and tick.symbol not in self.tracked and tick.symbol not in self.positions and tick.symbol not in targets
                and len(self.positions) < cfg.SLOTS and not self.ledger.state.get('pending_rotation')
                and all(not r.get('cancel_requested') and
                        int(r.get('order_bar_open_ms') or 0) <= now*1000 < int(r.get('order_bar_open_ms') or 0)+cfg.BAR_MS
                        for r in ordinary.values())):
            quote = self.market.entry_quote(tick.symbol)['selected']
            self._update_rank(tick.symbol, quote if quote is not None else float('inf'))
            while self._rank_heap and self._eligible.get(self._rank_heap[0][1]) != self._rank_heap[0]:
                heapq.heappop(self._rank_heap)
            best = self._rank_heap[0][0] if self._rank_heap else float('inf')
            if not ordinary and not at_most(best, GapPolicy().admission_gap(0)):
                self._skipped_noncompetitive_events += 1
                return False
            prices = self.market.entry_prices(ordinary)
            gaps = [(prices[s]-float(r['limit_price']))/float(r['limit_price'])
                    for s, r in ordinary.items() if s in prices and float(r.get('limit_price') or 0) > 0]
            if ordinary and len(gaps) == len(ordinary) and at_most(max(gaps), cfg.CANCEL_GAP):
                if not GapPolicy().replacement_allowed(max(gaps), best,
                        ceiling=cfg.CANCEL_GAP, count=len(ordinary)):
                    self._skipped_noncompetitive_events += 1
                    return False
        # A far quote changed no signal/order/position state. Rebuilding the
        # immutable worker gate for it cannot change admission; workers still
        # check live market health, quote distance, TTL and capacity at send.
        # Maintenance and every state-changing path continue publishing it.
        self._publish_entry_gate()
        working = self._fast_snapshot(snapshot, tick, now)
        # All mutations, rankings and capacity reservations have one owner.
        self._fast_tick = True
        try:
            with self.entry_cancel_plan():
                self._execute_entries(working, status, now=now, targets=targets,
                                      final_exit_bar=self._in_final_pre_exit_bar(now=now))
                self._entry_diagnostics['template_total'] = len(self._templates)
        finally:
            self._fast_tick = False
        return bool({row.get('client_order_id') for row in self.tracked.values()} - previous_ids)

    def write_feedback(self, **kwargs):
        # Exposed in the persistent diagnostics snapshot, not printed per tick.
        self._entry_diagnostics = dict(getattr(self, '_entry_diagnostics', {}),
                                       ws_direct=self.market.metrics())
        self._entry_diagnostics['ws_direct']['skipped_far_events'] = self._skipped_far_events
        self._entry_diagnostics['ws_direct']['skipped_noncompetitive_events'] = self._skipped_noncompetitive_events
        super().write_feedback(**kwargs)

    def _run_loop(self):
        LOG.info('WS DIRECT mode=%s inputs=%s', cfg.WS_DIRECT_MODE, check_inputs())
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
        metadata_sync = MetadataSync(cfg.EXCHANGE_INFO_CACHE_FILE)
        metadata_sync.start()
        next_maintenance = time.monotonic()
        try:
            self.market.start()
            while True:
                now_mono = time.monotonic()
                if now_mono >= next_maintenance:
                    # Exact fill/forced exit/close timers remain bounded even
                    # under continuous WS load; never wait for an exchange ACK.
                    try:
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
                        if self._drain_network():
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
