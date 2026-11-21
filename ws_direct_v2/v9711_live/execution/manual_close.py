"""Reconcile a user/external manual close using account stream evidence."""
from __future__ import annotations
import logging
from .stream import StreamUnavailable
from .. import config as cfg
from .position_identity import position_identity
from .order_identity import exact_entry_fill_time_ms

LOG = logging.getLogger('v9711_live_trade')

class ManualCloseMixin:
    def _reconcile_manual_flats(self, *, now: float) -> None:
        if not self.positions or not self.client.enabled or self.client.dry_run:
            return
        try:
            health = self.client.stream.health()
        except StreamUnavailable:
            return
        key = (health.last_event_seq, int(now // 2), tuple(sorted(
            (symbol, position_identity(pos), pos.get('quantity'))
            for symbol, pos in self.positions.items())))
        if key == getattr(self, '_last_manual_flat_probe', None):
            return
        pending = False
        for symbol, pos in list(self.positions.items()):
            if pos.get('side') != 'LONG':
                continue
            fence = max(self._finite(pos.get('order_bar_open_ms')),
                        self._finite(pos.get('external_evidence_since_ms')))
            entry_time = self._finite(pos.get('entry_fill_time_ms')) or self._finite(pos.get('entry_epoch'))*1000
            since_ms = int(max(fence, entry_time))
            own_close = str(pos.get('close_client_order_id') or '')
            try:
                diagnostics = {}
                evidence = self.client.stream.manual_flat_evidence(
                    symbol=symbol, since_ms=since_ms,
                    owned_qty=self._finite(pos.get('quantity')),
                    excluded_client_ids=(own_close,), diagnostics=diagnostics)
                if (not evidence and not pos.get('entry_fill_time_ms')
                        and diagnostics.get('flat_candidate_ms', 0) > 0):
                    # Upgrade legacy delayed-booking positions only from this
                    # exact entry's retained BUY trades. Do not weaken the
                    # adoption/sync fence or infer an entry from symbol totals.
                    entry = self.client.stream.lookup_order(symbol=symbol,
                        order_id=pos.get('entry_order_id'),
                        client_order_id=str(pos.get('entry_client_order_id') or ''))
                    actual = exact_entry_fill_time_ms(entry, symbol=symbol,
                        order_id=pos.get('entry_order_id'),
                        client_order_id=str(pos.get('entry_client_order_id') or ''))
                    corrected = int(max(fence, actual))
                    if actual > 0 and corrected < since_ms:
                        evidence = self.client.stream.manual_flat_evidence(
                            symbol=symbol, since_ms=corrected, owned_qty=self._finite(pos.get('quantity')),
                            excluded_client_ids=(own_close,), diagnostics=diagnostics)
                        if evidence:
                            evidence = dict(evidence, recovered_entry_fill_time_ms=actual,
                                            previous_since_ms=since_ms, corrected_since_ms=corrected)
                if not evidence and diagnostics.get('flat_candidate_ms', 0) > 0:
                    self._close_warning(symbol, 'manual-flat evidence pending: '+str(diagnostics), now=now)
            except StreamUnavailable as exc:
                pending = True
                self._close_warning(symbol, str(exc), now=now)
                continue
            if evidence:
                self._settle_manual_flat(symbol, evidence, now=now)
        if not pending:
            self._last_manual_flat_probe = key

    def _settle_manual_flat(self, symbol, evidence, *, now):
        pos = self.positions.get(symbol)
        if not pos:
            return
        state = self.ledger.state
        identity = position_identity(pos)
        if not identity:
            return
        history = state.setdefault('manual_close_history', {})
        if identity in history:
            return
        quantity = self._finite(pos.get('quantity'))
        entry = self._finite(pos.get('entry_price'))
        exit_price = self._finite(evidence.get('average_price'))
        if quantity <= 0 or entry <= 0 or exit_price <= 0:
            return
        pnl = quantity * (exit_price-entry) - quantity * (entry+exit_price) * cfg.TRADE_FEE_RATE
        history[identity] = dict(evidence, entry_client_order_id=pos.get('entry_client_order_id'),
            position_identity=identity, position_origin=pos.get('position_origin', 'exact_fill'),
            owned_entry_price=entry, booked_pnl=pnl, processed_at_epoch=now,
            pnl_basis='external SELL fill VWAP/owned entry; configured entry+exit fee estimate')
        close_cid = str(pos.get('close_client_order_id') or '')
        if close_cid:
            state.setdefault('forced_close_cleanup', {})[symbol] = dict(
                symbol=symbol, client_order_id=close_cid, order_id=pos.get('close_order_id'),
                next_query_epoch=0, reason='confirmed_manual_flat')
        state['realized_balance'] = max(1e-9, self._finite(
            state.get('realized_balance'), cfg.EQUITY0) + pnl)
        self.positions.pop(symbol, None)
        until = max(now + cfg.REOPEN_PROTECTION_SECONDS,
                    self._finite(pos.get('exit_due_epoch')))
        state.setdefault('forced_close_blocks', {})[symbol] = dict(
            until_epoch=until, entry_client_order_id=identity,
            reason='manual_exit_no_replenishment')
        state.setdefault('reopen_until_epoch', {})[symbol] = until
        self.stats['manual_close'] = self.stats.get('manual_close', 0) + 1
        self._close_stat('close')
        self.ledger.save()
        self._publish_entry_gate()
        tracked = self.tracked.get(symbol)
        if tracked is not None:
            self._request_cancel(symbol, tracked, 'manual_exit_no_replenishment')
        LOG.info('manual close confirmed from account flat + SELL fills symbol=%s qty=%s '
                 'exit_vwap=%s pnl_estimate=%s; position retired, no replenishment',
                 symbol, quantity, exit_price, pnl)

