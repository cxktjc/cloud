"""Durable, non-blocking LONG close ladder, adapted from trade1026/0930.

Normal exits: SELL LIMIT/GTC priceMatch=QUEUE -> wait -> exact cancel
confirmation -> MARKET for the still-owned residual. Unknown order fate never
permits a second close. No strategy decisions, exchange imports or background
threads live here. Existing pre-ladder market close identities remain valid.
"""
from __future__ import annotations

import logging
import math
import os
import time
from typing import Any, Mapping

from .. import config as cfg
from .client import DisabledExchange
from .submit_errors import require_submit_ack, submit_error_is_ambiguous

LOG = logging.getLogger('v9711_live_trade')
LIMIT_ENABLED = os.environ.get('V9711_LIVE_CLOSE_LIMIT_ENABLED', '1').strip().lower() in {
    '1', 'true', 'yes', 'on'}
LIMIT_SECONDS = cfg.env_float('V9711_LIVE_CLOSE_LIMIT_SECONDS', 60.0)
CANCEL_RETRY_SECONDS = cfg.env_float('V9711_LIVE_CLOSE_CANCEL_RETRY_SECONDS', 5.0)
if not (math.isfinite(LIMIT_SECONDS) and 1.0 <= LIMIT_SECONDS <= 600.0
        and math.isfinite(CANCEL_RETRY_SECONDS) and 1.0 <= CANCEL_RETRY_SECONDS <= 60.0):
    raise ValueError('Invalid QUEUE close wait / cancel retry seconds')
QUEUE_REASONS = frozenset({'fixed_bar_exit', 'over_capacity_3m_edge'})
TERMINAL = frozenset({'FILLED', 'CANCELED', 'CANCELLED', 'EXPIRED',
                      'EXPIRED_IN_MATCH', 'REJECTED'})
QUEUE = 'LIMIT_QUEUE'
MARKET = 'MARKET'


class CloseLongMixin:
    """Uses LiveTrade's existing exact ledger, positions, client and counters."""

    def _close_stat(self, key: str) -> None:
        self.stats[key] = int(self.stats.get(key, 0)) + 1

    def _close_warning(self, symbol: str, message: str, *, now: float) -> None:
        pos = self.positions.get(symbol)
        if pos is None:
            return
        previous = self._finite(pos.get('close_warning_epoch'), float('-inf'))
        if pos.get('close_warning_message') != message or now - previous >= 60.0:
            LOG.warning('close retained symbol=%s: %s', symbol, message)
            pos.update(close_warning_epoch=now, close_warning_message=message)

    def _clear_close_order(self, symbol: str, *, next_phase: str,
                           retry_after: float = 0.0) -> None:
        """Checkpoint residual/accounting BEFORE removing the old exact intent."""
        pos = self.positions[symbol]
        cid, oid = str(pos.get('close_client_order_id') or ''), pos.get('close_order_id')
        control = {k: pos[k] for k in ('close_started_epoch', 'close_deadline_epoch',
                                       'close_reason') if k in pos}
        attempt = int(self._finite(pos.get('close_attempt'))) + 1
        for key in tuple(pos):
            if key.startswith('close_'):
                pos.pop(key, None)
        pos.update(control)
        pos.update(close_attempt=attempt, close_next_phase=next_phase,
                   close_retry_after_epoch=retry_after)
        # The intent/state files are separate atomic writes. Keep the old intent
        # until the residual and next attempt have reached the position file.
        self.ledger.save()
        self.ledger.forget(symbol=symbol, client_order_id=cid, order_id=oid)

    def _settle_close_confirmation(self, symbol: str, result: Mapping[str, Any], *, now: float) -> None:
        """Book cumulative exact fills once; an ACK/cancel is not a fill."""
        pos = self.positions.get(symbol)
        if not pos or not pos.get('close_client_order_id'):
            return
        cid, oid = str(pos['close_client_order_id']), str(pos.get('close_order_id') or '')
        response_symbol = str(result.get('symbol') or '').upper()
        response_cid = str(result.get('clientOrderId') or result.get('origClientOrderId') or '')
        response_oid = str(result.get('orderId') or '')
        if ((response_symbol and response_symbol != symbol.upper())
                or (response_cid and response_cid != cid)
                or (response_oid and oid and response_oid != oid)):
            self._close_warning(symbol, 'exact response identity mismatch', now=now)
            return
        if response_oid and not oid:
            pos['close_order_id'] = response_oid
            self.ledger.register(symbol=symbol, client_order_id=cid, order_id=response_oid,
                                 kind='close', qty=self._finite(pos.get('close_quantity')))
        if (pos.get('close_phase') == QUEUE and pos.get('close_timer_pending')
                and (response_oid or pos.get('close_order_id'))):
            # A queued network job may sit behind another request. Do not use
            # local queue residence as its 60s exchange resting interval.
            pos.update(close_timer_pending=False, close_ack_epoch=now,
                       close_deadline_epoch=now if pos.get('close_urgent') else now + LIMIT_SECONDS)
        requested = self._finite(pos.get('close_quantity'))
        accounted = self._finite(pos.get('close_accounted_qty'))
        filled = self._finite(result.get('executedQty'), float('nan'))
        status = str(result.get('status') or '').upper()
        epsilon = max(1e-12, requested * 1e-9)
        # A terminal response without a measurable cumulative fill cannot
        # authorize fallback: it might conceal a fill racing with cancellation.
        if (not math.isfinite(filled) or filled < accounted - epsilon
                or filled > requested + epsilon or requested <= 0
                or (status == 'FILLED' and filled <= 0)):
            self._close_warning(symbol, 'missing/invalid cumulative fill; wait for exact evidence', now=now)
            return
        avg = self._finite(result.get('avgPrice'))
        if filled > accounted + epsilon:
            if avg <= 0.0 and result.get('_simulated'):
                avg = self.client.mark_price(symbol)
                if not math.isfinite(avg) or avg <= 0.0:
                    avg = self._finite(pos.get('entry_price'))
            if avg <= 0.0:
                self._close_warning(symbol, 'fill price unavailable; no residual fallback yet', now=now)
                return
            delta = filled - accounted
            quote = filled * avg
            delta_quote = quote - self._finite(pos.get('close_accounted_quote'))
            if delta_quote <= 0.0:
                self._close_warning(symbol, 'invalid cumulative fill quote', now=now)
                return
            entry = self._finite(pos.get('entry_price'))
            fee = (entry * delta + delta_quote) * cfg.TRADE_FEE_RATE
            pnl = delta_quote - entry * delta - fee
            balance = self._finite(self.ledger.state.get('realized_balance'), cfg.EQUITY0)
            self.ledger.state['realized_balance'] = max(1e-9, balance + pnl)
            pos['quantity'] = max(0.0, self._finite(pos.get('quantity')) - delta)
            pos['close_accounted_qty'] = filled
            pos['close_accounted_quote'] = quote
        pos['close_status'] = status or 'UNKNOWN'
        if self._finite(pos.get('quantity')) <= epsilon:
            self.positions.pop(symbol, None)
            self.ledger.state['reopen_until_epoch'][symbol] = now + cfg.REOPEN_PROTECTION_SECONDS
            self._close_stat('close')
            self.ledger.save()
            self.ledger.forget(symbol=symbol, client_order_id=cid,
                               order_id=pos.get('close_order_id'))
            LOG.info('close fill confirmed symbol=%s phase=%s qty=%.8g',
                     symbol, pos.get('close_phase', MARKET), filled)
        elif status in TERMINAL:
            phase = str(pos.get('close_phase') or MARKET)
            LOG.info('close terminal with residual symbol=%s phase=%s status=%s remaining=%.8g',
                     symbol, phase, status, self._finite(pos.get('quantity')))
            self._clear_close_order(symbol, next_phase=MARKET)
        else:
            self.ledger.save()

    def _cancel_queue_close(self, symbol: str, *, now: float) -> None:
        pos = self.positions.get(symbol)
        if not pos or pos.get('close_phase') != QUEUE:
            return
        # An unacknowledged placement must first recover its exact exchange ID.
        # A negative lookup is NOT proof of absence for a close order.
        oid = pos.get('close_order_id')
        if oid in (None, '', 0, '0'):
            self._close_warning(symbol, 'QUEUE timeout with unknown submission; query only, no duplicate close', now=now)
            return
        if now < self._finite(pos.get('close_cancel_retry_after_epoch')):
            return
        cid = str(pos['close_client_order_id'])
        pos.update(close_cancel_requested_epoch=pos.get('close_cancel_requested_epoch', now),
                   close_cancel_retry_after_epoch=now + CANCEL_RETRY_SECONDS)
        self.ledger.save()  # crash-safe before the cancel reaches the exchange
        def complete(result, error):
            current = self.positions.get(symbol)
            if not current or current.get('close_client_order_id') != cid:
                return
            event_now = time.time() if self._network_io else now
            if error is not None:
                self._close_warning(symbol, f'QUEUE cancel uncertain ({error}); no market until confirmed', now=event_now)
                self.ledger.save()
                return
            self._close_stat('close_limit_cancel')
            if isinstance(result, Mapping):
                self._settle_close_confirmation(symbol, result, now=event_now)
        self._run_network(('cancel', symbol, cid),
            lambda: self.client.cancel(symbol=symbol, order_id=oid, client_order_id=cid),
            complete, priority=0)

    def _reconcile_position_close(self, symbol: str, *, now: float) -> None:
        pos = self.positions.get(symbol)
        if not pos or not pos.get('close_client_order_id'):
            return
        query = getattr(self.client, 'query_exact_order', None)
        if callable(query):
            result, known = query(
                self.ledger, symbol=symbol, order_id=pos.get('close_order_id'),
                client_order_id=str(pos['close_client_order_id']), now=now)
        else:
            result = self.client.exact_order(
                self.ledger, symbol=symbol, order_id=pos.get('close_order_id'),
                client_order_id=str(pos['close_client_order_id']))
            known = result is not None
        if known and result:
            self._settle_close_confirmation(symbol, result, now=now)
        elif known:
            self._close_warning(symbol, 'negative close lookup is not terminal proof; identity retained', now=now)
        pos = self.positions.get(symbol)
        if (pos and pos.get('close_client_order_id') and pos.get('close_phase') == QUEUE
                and now >= self._finite(pos.get('close_deadline_epoch'), float('inf'))):
            self._cancel_queue_close(symbol, now=now)

    def _reject_close_submission(self, symbol: str, cid: str,
                                 exc: BaseException, *, now: float) -> None:
        pos = self.positions[symbol]
        phase = str(pos.get('close_phase') or MARKET)
        pos['last_close_rejection'] = {'client_order_id': cid,
            'rejected_at_epoch': now, 'error': str(exc), 'phase': phase}
        self._close_stat('close_rejected')
        # Definitive QUEUE rejection -> market on next loop. A market rejection
        # retries with a fresh ID after the existing 10s cooldown. No resends
        # are allowed for transport/ambiguous failures.
        delay = 10.0 if phase == MARKET or isinstance(exc, DisabledExchange) else 0.0
        self._clear_close_order(symbol, next_phase=MARKET, retry_after=now + delay)
        self.positions[symbol]['close_status'] = 'REJECTED_RETRY'
        self.ledger.save()
        LOG.warning('close rejected; real position retained symbol=%s phase=%s id=%s '
                    'qty=%s retry_in=%.1fs err=%s', symbol, phase, cid,
                    self.positions[symbol].get('quantity'), delay, exc)

    def _close_position(self, symbol: str, reason: str, *, now: float) -> None:
        pos = self.positions.get(symbol)
        if not pos:
            return
        if pos.get('close_client_order_id'):
            if pos.get('close_phase') == QUEUE and reason not in QUEUE_REASONS:
                # A future emergency caller must not wait out a normal QUEUE
                # deadline. Still confirm exact cancellation before market.
                pos['close_urgent'] = True
                pos['close_deadline_epoch'] = min(
                    self._finite(pos.get('close_deadline_epoch'), now), now)
                self.ledger.save()
            self._reconcile_position_close(symbol, now=now)
            pos = self.positions.get(symbol)
            if not pos or pos.get('close_client_order_id'):
                return
        if now < self._finite(pos.get('close_retry_after_epoch')):
            return
        phase = str(pos.get('close_next_phase') or
                    (QUEUE if LIMIT_ENABLED and reason in QUEUE_REASONS else MARKET))
        attempt = int(self._finite(pos.get('close_attempt')))
        prefix = 'clq' if phase == QUEUE else 'close'
        kind = prefix if attempt == 0 else f'{prefix}{attempt}'
        cid = self.ledger.client_id(symbol, int(self._finite(pos.get('exit_due_ms'))), kind)
        already_registered = self.ledger.owns_client(cid)
        pos.setdefault('close_started_epoch', now)
        if phase == QUEUE:
            pos.setdefault('close_deadline_epoch', now + LIMIT_SECONDS)
            if self._network_io:
                pos.update(close_timer_pending=True, close_enqueued_epoch=now)
        pos.update(close_phase=phase, close_client_order_id=cid, close_order_id='',
                   close_quantity=self._finite(pos.get('quantity')), close_accounted_qty=0.0,
                   close_accounted_quote=0.0, close_requested_epoch=now, close_reason=reason,
                   close_status='SUBMITTING')
        self.ledger.register(symbol=symbol, client_order_id=cid, kind='close',
                             qty=pos['close_quantity'])
        if already_registered:
            self._reconcile_position_close(symbol, now=now)
            return
        quantity = pos['close_quantity']
        place = self.client.place_queue_close if phase == QUEUE else self.client.place_market_close
        def complete(result, error):
            pos = self.positions.get(symbol)
            if not pos or pos.get('close_client_order_id') != cid:
                return
            event_now = time.time() if self._network_io else now
            try:
                if error is not None:
                    raise error
                result = require_submit_ack(result, symbol=symbol, client_order_id=cid)
            except Exception as exc:
                if pos.get('close_order_id') or self._finite(pos.get('close_accounted_qty')) > 0:
                    self._close_warning(symbol, f'late close failure after exact evidence ({exc}); retained', now=event_now)
                    return
                if isinstance(exc, DisabledExchange) or not submit_error_is_ambiguous(exc):
                    self._reject_close_submission(symbol, cid, exc, now=event_now)
                    return
                pos.update(close_status='UNKNOWN', close_submit_failed_at_epoch=event_now)
                self.ledger.save()
                self._close_warning(symbol, f'{phase} submit uncertain ({exc}); exact identity retained', now=event_now)
                return
            self._close_stat('close_limit' if phase == QUEUE else 'close_market')
            if phase == MARKET and 'close_deadline_epoch' in pos:
                self._close_stat('close_fallback')
            pos['close_order_id'] = str(result.get('orderId') or '')
            pos['close_status'] = str(result.get('status') or 'UNKNOWN').upper()
            self.ledger.register(symbol=symbol, client_order_id=cid,
                                 order_id=pos['close_order_id'], kind='close', qty=pos['close_quantity'])
            LOG.info('close submitted symbol=%s phase=%s qty=%.8g id=%s deadline=%s reason=%s',
                     symbol, phase, pos['close_quantity'], cid,
                     pos.get('close_deadline_epoch') if phase == QUEUE else '-', reason)
            self._settle_close_confirmation(symbol, result, now=event_now)
        self._run_network(('close', symbol, cid),
            lambda: place(symbol=symbol, qty=quantity, client_order_id=cid), complete, priority=0)

    def _close_order_view(self) -> dict[str, dict[str, Any]]:
        """Separate close orders from the six-entry-order capacity accounting."""
        return {s: {'symbol': s, 'client_order_id': p['close_client_order_id'],
                    'order_id': p.get('close_order_id'),
                    'phase': p.get('close_phase', MARKET), 'status': p.get('close_status'),
                    'remaining_owned_qty': p.get('quantity'),
                    'deadline_epoch': p.get('close_deadline_epoch'),
                    'cancel_requested': bool(p.get('close_cancel_requested_epoch'))}
                for s, p in self.positions.items() if p.get('close_client_order_id')}
