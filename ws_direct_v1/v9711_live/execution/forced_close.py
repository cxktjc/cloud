"""Reconcile explicit exchange forced exits against existing strategy ownership.

Never infer a fill from mark price, aggregate absence, or an unknown own close.
Full forced closure retires the owned position even when an earlier close is
UNKNOWN. That old exact intent is isolated for query/cancel cleanup, not a
phantom position. No compensating BUY or new closing SELL is submitted here.
"""
from __future__ import annotations
import logging
from .stream import StreamUnavailable
from .. import config as cfg

LOG = logging.getLogger('v9711_live_trade')


class ForcedCloseMixin:
    def _forced_entry_blocked(self, symbol: str, *, now: float) -> bool:
        state = self.ledger.state
        return (symbol in state.get('forced_close_cleanup', {}) or
                now < self._finite((state.get('forced_close_blocks', {}).get(symbol) or {}).get('until_epoch')))

    def _reconcile_forced_exits(self, *, now: float) -> None:
        if not self.positions or not self.client.enabled or self.client.dry_run:
            return
        stream = self.client.stream
        try:
            health = stream.health()
        except StreamUnavailable:
            return
        key = (health.last_event_seq, int(now // 5), tuple(sorted((symbol, pos.get('entry_client_order_id'),
                pos.get('quantity')) for symbol, pos in self.positions.items())))
        if key == getattr(self, '_last_forced_probe', None):
            return
        for symbol, pos in list(self.positions.items()):
            cid = str(pos.get('entry_client_order_id') or '')
            if not cid.startswith(cfg.ORDER_CLIENT_PREFIX + '_') or pos.get('side') != 'LONG':
                continue
            since_ms = int(max(self._finite(pos.get('order_bar_open_ms')),
                               self._finite(pos.get('entry_epoch')) * 1000))
            try:
                evidence = stream.forced_flat_evidence(symbol=symbol, since_ms=since_ms,
                                                       owned_qty=self._finite(pos.get('quantity')))
            except StreamUnavailable as exc:
                self._close_warning(symbol, str(exc), now=now)
                return  # retry even without a new event after transient DB error
            if evidence:
                # Consume any already-persisted own close fills first. An
                # external exit must not book quantities closed by our order.
                if pos.get('close_client_order_id'):
                    try:
                        own = stream.lookup_order(symbol=symbol, order_id=pos.get('close_order_id'),
                                                  client_order_id=pos['close_client_order_id'])
                    except StreamUnavailable as exc:
                        self._close_warning(symbol, str(exc), now=now)
                        return
                    if own:
                        self._settle_close_confirmation(symbol, own, now=now)
                if symbol in self.positions:
                    self._settle_forced_flat(symbol, evidence, now=now)
        self._last_forced_probe = key

    def _settle_forced_flat(self, symbol, evidence, *, now):
        pos = self.positions.get(symbol)
        if not pos:
            return
        state = self.ledger.state
        audit = state.setdefault('forced_close_history', {})
        identity = str(pos['entry_client_order_id'])
        if identity in audit:
            return
        quantity = self._finite(pos['quantity'])
        entry = self._finite(pos['entry_price'])
        exit_price = self._finite(evidence['average_price'])
        pnl = quantity * (exit_price-entry) - quantity * (entry+exit_price) * cfg.TRADE_FEE_RATE
        record = dict(evidence, entry_client_order_id=identity, closed_owned_qty=quantity,
                      owned_entry_price=entry, booked_pnl=pnl, processed_at_epoch=now,
                      previous_close_status=pos.get('close_status'),
                      pnl_basis='owned_entry/forced_average; configured entry+exit fee estimate')
        audit[identity] = record
        close_cid = str(pos.get('close_client_order_id') or '')
        if close_cid:
            state.setdefault('forced_close_cleanup', {})[symbol] = dict(
                symbol=symbol, client_order_id=close_cid, order_id=pos.get('close_order_id'),
                next_query_epoch=0, reason='confirmed_forced_flat')
        state['realized_balance'] = max(1e-9, self._finite(state.get('realized_balance'), cfg.EQUITY0) + pnl)
        self.positions.pop(symbol)
        until = max(now + cfg.REOPEN_PROTECTION_SECONDS, self._finite(pos.get('exit_due_epoch')))
        state.setdefault('forced_close_blocks', {})[symbol] = dict(until_epoch=until,
            entry_client_order_id=identity, reason='forced_exit_no_replenishment')
        state['reopen_until_epoch'][symbol] = until
        self.stats['forced_close'] = self.stats.get('forced_close', 0) + 1
        self._close_stat('close')
        # Audit, balance and removal share the atomic state-file checkpoint.
        self.ledger.save()
        self._publish_entry_gate()
        tracked = self.tracked.get(symbol)
        if tracked is not None:
            self._request_cancel(symbol, tracked, 'forced_exit_no_replenishment')
        LOG.warning('forced close confirmed symbol=%s qty=%s exit=%s booked_pnl=%s '
                    'previous_close=%s; position retired; no replenishment', symbol,
                    quantity, exit_price, pnl, pos.get('close_status'))

    def _cleanup_forced_close_orders(self, *, now: float) -> None:
        cleanup = self.ledger.state.get('forced_close_cleanup', {})
        for symbol, row in list(cleanup.items()):
            if now < self._finite(row.get('next_query_epoch')):
                continue
            row['next_query_epoch'] = now + 5
            cid = row['client_order_id']
            result, known = self.client.query_exact_order(self.ledger, symbol=symbol,
                order_id=row.get('order_id'), client_order_id=cid, now=now)
            if not known:
                continue  # keep only the orphan intent, never recreate a position
            status = str((result or {}).get('status') or '').upper()
            if result is None or status in {'FILLED', 'CANCELED', 'CANCELLED', 'EXPIRED',
                                           'EXPIRED_IN_MATCH', 'REJECTED'}:
                cleanup.pop(symbol, None)
                self.ledger.save()
                self.ledger.forget(symbol=symbol, client_order_id=cid, order_id=row.get('order_id'))
                continue
            def complete(value, error, symbol=symbol, cid=cid):
                current = self.ledger.state.get('forced_close_cleanup', {}).get(symbol)
                if current is None or current['client_order_id'] != cid:
                    return
                if error is not None:
                    LOG.warning('forced-flat old close cancel pending symbol=%s err=%s', symbol, error)
                # The next exact query confirms terminal state; ACK is not assumed.
            self._run_network(('forced-close-cancel', symbol, cid),
                lambda symbol=symbol, row=dict(row): self.client.cancel(symbol=symbol,
                    order_id=row.get('order_id'), client_order_id=row['client_order_id']),
                complete, priority=0)
