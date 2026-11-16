"""Event-driven trade entrypoint. Signal publishes full-bar templates, not ticks."""
from __future__ import annotations
import heapq
import logging
import queue
import time
from pathlib import Path
from .. import config as cfg
from .runner import LiveTrade
from .ws_market import DirectMarket
from collectors.ticker_ws import load_symbols

LOG = logging.getLogger('v9711.ws_direct')


def check_inputs():
    return dict(version=cfg.WS_DIRECT_MODE, root=str(cfg.ROOT), state=str(cfg.STATE_DIR.resolve()),
                plans=str(cfg.DESIRED_FILE.resolve()), price_output=str(cfg.PRICE_FILE.resolve()),
                bars=str(cfg.STATUS_FILE.resolve()), account=str(cfg.ACCOUNT_STREAM_DIR.resolve()),
                symbols=str(cfg.SYMBOLS_FILE.resolve()), filters=str(cfg.EXCHANGE_INFO_CACHE_FILE.resolve()),
                decision_input='internal WS memory only; never latest.json',
                order='receive -> decision/durable registration/dispatch -> mirror commit -> asynchronous file')


class DirectTrade(LiveTrade):
    def __init__(self, **kwargs):
        self.market = DirectMarket(load_symbols(cfg.SYMBOLS_FILE), cfg.PRICE_FILE)
        self._fast_tick = False
        self._account_deadline = 0.0
        self._template_revision = -1
        self._templates = {}
        self._eligible = {}
        self._rank_heap = []
        self._quote_versions = {}
        self._active_price_symbols = None
        super().__init__(**kwargs)
        self._desired_path_display = str(cfg.DESIRED_FILE.resolve())
        self.client.market_guard = self._market_send_allowed

    def _market_send_allowed(self, symbol):
        account_ready = (not self.client.enabled or self.client.dry_run
                         or time.time() <= self._account_deadline)
        return account_ready and self.market.order_allowed(symbol)

    def step(self, *, now=None):
        stamp = time.time() if now is None else now
        health = self._stream_health()
        if health.get('fresh'):
            self._account_deadline = stamp + cfg.ACCOUNT_STREAM_MAX_AGE_SECONDS - float(health.get('age_seconds', 0))
        else:
            self._account_deadline = 0
        return super().step(now=now)

    def _execute_entries(self, snapshot, status, **kwargs):
        if status == 'valid':
            if not self.market.healthy():
                status = 'ws_not_ready'
            elif self.client.enabled and not self.client.dry_run and time.time() > self._account_deadline:
                status = 'account_not_ready'
        return super()._execute_entries(snapshot, status, **kwargs)

    def desired_snapshot(self, *, now):
        payload, status = super().desired_snapshot(now=now)
        if status != 'valid':
            return payload, status
        if payload.get('execution_mode') != cfg.WS_DIRECT_MODE or not isinstance(payload.get('candidate_templates'), list):
            return self._fresh_fallback(), 'incompatible_ws_direct_contract'
        payload['signal_discovery_desired'] = payload.get('desired') or []
        payload['desired'] = payload['candidate_templates']
        if self._template_revision != self._desired_reader.revision:
            self._templates = {row['symbol']:dict(row) for row in payload['candidate_templates']
                               if isinstance(row, dict) and row.get('symbol')}
            self._eligible.clear()
            self._rank_heap.clear()
            for symbol, price in self.market.prices().items():
                self._update_rank(symbol, price)
            self._template_revision = self._desired_reader.revision
        return payload, status

    def refresh_prices(self, *, now=None):
        return self.market.prices(self._active_price_symbols if self._fast_tick else None)

    def _publish_entry_gate(self):
        if getattr(self, '_network_io', None) is None:
            return
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
        if row and gap <= cfg.BOOTSTRAP_PLACE_GAP:
            item = (gap, symbol, version)
            self._eligible[symbol] = item
            heapq.heappush(self._rank_heap, item)
        if len(self._rank_heap) > max(64, 4*len(self._eligible)):
            self._rank_heap = list(self._eligible.values())
            heapq.heapify(self._rank_heap)

    def _fast_snapshot(self, snapshot, tick, now):
        self._update_rank(tick.symbol, float(tick.price))
        selected, restore = set(self.tracked), []
        valid = 0
        while self._rank_heap and valid < cfg.MAX_PENDING_ORDERS:
            item = heapq.heappop(self._rank_heap)
            gap, symbol, version = item
            if self._eligible.get(symbol) != item:
                continue
            if not self.market.order_allowed(symbol):
                self._eligible.pop(symbol, None)
                continue
            restore.append(item)
            row = self._templates[symbol]
            if (symbol in self.positions or symbol in self.tracked
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
        snapshot, status = self.desired_snapshot(now=now)
        self._io_signal = snapshot, status
        self._publish_entry_gate()
        self._drain_network()
        previous_ids = {row.get('client_order_id') for row in self.tracked.values()}
        targets = self._target_positions(snapshot)
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
        super().write_feedback(**kwargs)

    def _run_loop(self):
        LOG.info('WS DIRECT mode=%s inputs=%s', cfg.WS_DIRECT_MODE, check_inputs())
        if self.client.enabled and not self.client.dry_run:
            from ..contracts import read_json
            marker = read_json(cfg.STATE_DIR / 'WS_MIGRATION.json') or {}
            if ((cfg.STATE_DIR / '.migration_in_progress').exists() or not marker.get('completed')
                    or not cfg.EXECUTION_INTENT_FILE.is_file() or not cfg.EXECUTION_LEDGER_FILE.is_file()):
                raise RuntimeError('live WS-direct requires completed stopped-ledger migration; run scripts.migrate_state --source <legacy-state> --apply; never delete existing ledgers')
        if self.once:
            self.step()
            return 0
        self.market.start()
        next_maintenance = time.monotonic()
        try:
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
            self.market.close()
