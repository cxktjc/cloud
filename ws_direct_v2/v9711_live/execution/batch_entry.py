"""Per-plan LIMIT batch adapter, using trade1026's underlying REST primitive.

Only 2..5 actual sendable entries use batchOrders. One sendable member uses the
ordinary single-order adapter. Each result/error is attributed to its own CID;
transport uncertainty never triggers a blind whole-batch or single retry.
"""
from __future__ import annotations

import os
import logging
import time
from typing import Any, Mapping

from .submit_errors import ConfirmedSubmitRejection, EntryQuoteUnavailable, require_submit_ack
from .signal_follow import follow_send_allowed

LOG = logging.getLogger('v9711_live_trade.entry_quotes')


class BatchEntryMixin:
    def _checked_entry_quote(self, symbol, cid, limit):
        capture = getattr(self, 'entry_quote_snapshot', None)
        if callable(capture):
            detail = capture(symbol, cid, limit)
            if detail['entry_qualified_by'] == 'none':
                raise EntryQuoteUnavailable('neither tick nor bid0 qualifies before send')
            return detail
        guard = getattr(self, 'entry_quote_guard', None)
        if callable(guard) and not guard(symbol, cid, limit):
            raise EntryQuoteUnavailable('neither tick nor bid0 qualifies before send')
        return {}

    @staticmethod
    def _log_entry_quote(symbol, cid, detail):
        if detail:
            LOG.info('entry pre-send symbol=%s id=%s 满足=%s tick=%s depth_bid0=%s '
                     'tick_gap=%s depth_gap=%s gap_ceiling=%s', symbol, cid,
                     detail['entry_qualified_by'], detail['entry_tick'], detail['entry_bid0'],
                     detail['entry_tick_gap'], detail['entry_depth_gap'], detail['entry_gap_ceiling'])

    def _post_batch_orders(self, orders: list[dict[str, Any]]) -> Any:
        # Runtime-only import of the existing reusable primitive, NOT either
        # live launcher. Its function supports LIMIT, GTC and explicit prices.
        from ..exchange.binance_futures_trade_ws import rest_place_order_batch
        key, secret = os.environ.get('BINANCE_API_KEY', ''), os.environ.get('BINANCE_API_SECRET', '')
        if not key or not secret:
            raise ConfirmedSubmitRejection('batch credentials unavailable; request never sent')
        return rest_place_order_batch(key, secret, orders, recv_window=5000, request_timeout=5)

    def _entry_send_check(self, row: Mapping[str, Any]) -> dict:
        symbol, cid = str(row['symbol']).upper(), str(row['client_order_id'])
        bar = int(row['order_bar_open_ms'])
        market_guard = getattr(self, 'market_guard', None)
        if callable(market_guard) and not market_guard(symbol):
            raise EntryQuoteUnavailable('internal WS quotes unavailable before batch send')
        following = follow_send_allowed(getattr(self, 'entry_gate', {}), symbol, cid, bar, now=time.time())
        if self.network_io is not None:
            following = self.network_io.check_entry(symbol, cid, bar)
        detail = self._checked_entry_quote(symbol, cid, float(row['limit_price']))
        from .. import config as cfg
        if not following and not bar <= time.time() * 1000 < bar + cfg.BAR_MS:
            raise ConfirmedSubmitRejection('entry order bar expired/not active before batch send')
        if time.monotonic() < self._broker_rate_limit_until_mono:
            raise ConnectionError('trade broker rate limit cooldown; no batch request sent')
        return detail

    def place_limit_batch(self, requests: list[dict[str, Any]]) -> dict[str, tuple[Any, BaseException | None]]:
        if not 1 <= len(requests) <= 5:
            raise ValueError('One batch accepts 1..5 entries')
        ids = [str(r['client_order_id']) for r in requests]
        if len(set(ids)) != len(ids) or len({str(r['symbol']).upper() for r in requests}) != len(requests):
            raise ConfirmedSubmitRejection('duplicate batch identity/symbol; request never sent')
        outcome = {}
        sendable = []
        timings = getattr(self, 'entry_timings', None)
        for row in requests:
            cid = str(row['client_order_id'])
            if timings is not None:
                timings.mark(cid, 'worker')
            try:
                self._entry_send_check(row)
                prepared = self.prepare_limit(str(row['symbol']), float(row['limit_price']), float(row['notional']))
                if not prepared['ok']:
                    raise ConfirmedSubmitRejection(str(prepared['reason']))
                # Metadata/preparation is local, but may have taken time.
                self._entry_send_check(row)
                wire = dict(symbol=str(row['symbol']).upper(),side='BUY',type='LIMIT',
                    timeInForce='GTC',positionSide='LONG',price=str(prepared['price']),
                    quantity=str(prepared['qty']),newClientOrderId=cid)
                sendable.append((row,wire))
            except Exception as exc:
                outcome[cid] = (None,exc)
        # Refilter immediately before sending; canceled/expired members do not
        # poison the valid subset, and a remaining singleton is NOT a batch.
        current=[]
        quotes={}
        for row,wire in sendable:
            try:
                quotes[str(row['client_order_id'])] = self._entry_send_check(row)
                current.append((row,wire))
            except Exception as exc:
                outcome[str(row['client_order_id'])]=(None,exc)
        if not current:
            return outcome
        if len(current)==1:
            row,wire=current[0]
            cid=str(row['client_order_id'])
            try:
                value=self.place_limit_for_bar(**row)
                outcome[cid]=(value,None)
            except Exception as exc:
                outcome[cid]=(None,exc)
            return outcome
        for row,wire in current:
            self._log_entry_quote(wire['symbol'], wire['newClientOrderId'], quotes[wire['newClientOrderId']])
        if self.dry_run or not self.enabled:
            replies=[dict(_simulated=True,symbol=w['symbol'],clientOrderId=w['newClientOrderId'],
                orderId='simbatch-'+w['newClientOrderId'],status='NEW',executedQty='0',avgPrice='0',
                price=w['price'],origQty=w['quantity']) for _,w in current]
        else:
            try:
                if timings is not None:
                    for row, wire in current:
                        timings.mark(wire['newClientOrderId'], 'send', transport='REST_BATCH')
                replies=self._post_batch_orders([w for _,w in current])
                if timings is not None:
                    for row, wire in current:
                        timings.mark(wire['newClientOrderId'], 'response')
            except Exception as exc:
                self._record_rate_limit(exc)
                for row,wire in current:
                    outcome[str(row['client_order_id'])]=(None,exc)
                return outcome
        for i,(row,wire) in enumerate(current):
            cid=wire['newClientOrderId']
            try:
                if not isinstance(replies,list):
                    raise RuntimeError('batch response is not a list; exact fate unknown')
                matched=[r for r in replies if isinstance(r,Mapping)
                    and str(r.get('clientOrderId') or r.get('origClientOrderId') or '')==cid]
                if len(matched)>1:
                    raise RuntimeError('duplicate batch response identity; exact fate unknown')
                if matched:
                    result=matched[0]
                elif len(replies)==len(current) and isinstance(replies[i],Mapping) and not (
                        replies[i].get('orderId') or replies[i].get('clientOrderId') or replies[i].get('origClientOrderId')):
                    # Binance returns per-request errors in request order.
                    result=replies[i]
                else:
                    raise RuntimeError('batch member identity/result missing; exact fate unknown')
                self._record_rate_limit(result)
                result=require_submit_ack(result,symbol=wire['symbol'],client_order_id=cid)
                if quotes[cid]:
                    result=dict(result, _entry_send_quote=quotes[cid])
                outcome[cid]=(result,None)
            except Exception as exc:
                outcome[cid]=(None,exc)
        return outcome
