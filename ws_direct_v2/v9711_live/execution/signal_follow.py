"""Two extra exact-owned entries following active signal positions across bars."""
from __future__ import annotations

import hashlib
import math
import time
from typing import Mapping

from .. import config as cfg
from .submit_errors import EntryQuoteUnavailable, is_agreement_rejection

FOLLOW_ORDER_CAP = cfg.SIGNAL_FOLLOW_MAX_PENDING_ORDERS
FOLLOW_POSITION_CAP = cfg.SIGNAL_FOLLOW_POSITION_STOP


def follow_send_allowed(gate, symbol, cid, bar_ms, *, now):
    """Use only the owner's immutable snapshot at the actual broker send."""
    return bool(
        gate.get('ready') and cid in gate.get('allowed', ())
        and gate.get('follow_entries', {}).get(cid) == (symbol.upper(), bar_ms)
        and gate.get('held', FOLLOW_POSITION_CAP) < FOLLOW_POSITION_CAP
        and now <= gate.get('expires', 0)
        and 0 <= now - gate.get('generated', 0) <= gate.get('ttl', 0)
        and 0 <= now - gate.get('refreshed', 0) <= gate.get('ttl', 0))


class SignalFollowMixin:
    def _follow_fill_evidence(self, row, result):
        qty = self._finite(result.get('executedQty'), float('nan'))
        cid = str(result.get('clientOrderId') or result.get('origClientOrderId') or '')
        oid = str(result.get('orderId') or '')
        return (math.isfinite(qty) and qty >= self._finite(row.get('accounted_entry_qty'))
                and qty <= self._finite(row.get('qty')) + 1e-9
                and (str(result.get('status') or '').upper() != 'FILLED' or qty > 0)
                and (not result.get('symbol') or str(result['symbol']).upper() == row['symbol'])
                and (cid or oid)
                and (not cid or cid == str(row.get('client_order_id') or ''))
                and (not oid or not row.get('order_id') or oid == str(row['order_id'])))

    def _follow_targets(self, snapshot, *, now):
        out = {}
        for symbol, raw in self._target_positions(snapshot).items():
            row = dict(raw)
            identity = str(row.get('position_id') or '')
            bar = int(self._finite(row.get('entry_bar_open_ms')))
            available = int(self._finite(row.get('entry_available_ms'), bar + cfg.BAR_MS))
            price = self._finite(row.get('limit_price'), self._finite(row.get('entry_price')))
            due = self._finite(row.get('exit_due_epoch'), self._finite(row.get('exit_due_ms')) / 1000)
            if (not identity or bar <= 0 or bar % cfg.BAR_MS
                    or available != bar + cfg.BAR_MS or available > now * 1000
                    or self._finite(row.get('quantity')) <= 0 or price <= 0 or due <= 0):
                continue
            row.update(symbol=symbol, position_id=identity, limit_price=price,
                       order_bar_open_ms=bar, exit_due_epoch=due,
                       exit_due_ms=int(due * 1000), signal_follow=True)
            out[symbol] = row
        return out

    def _follow_snapshot_complete(self, snapshot, targets):
        rows = snapshot.get('target_positions')
        return (bool(snapshot.get('signal_ready')) and isinstance(rows, list)
                and len(rows) == len(targets))

    def _promote_signal_follow_orders(self, snapshot, *, now):
        """Keep the original exchange identity when signal confirms that entry."""
        if not snapshot.get('signal_ready') or len(self.positions) >= FOLLOW_POSITION_CAP:
            return
        targets = self._follow_targets(snapshot, now=now)
        if not self._follow_snapshot_complete(snapshot, targets):
            return
        count = sum(bool(r.get('signal_follow')) for r in self.tracked.values())
        changed = False
        for symbol, target in sorted(targets.items(), key=lambda p: (p[1]['order_bar_open_ms'], p[0])):
            old = self.tracked.get(symbol)
            if (not old or old.get('signal_follow') or symbol in self.positions
                    or count >= FOLLOW_ORDER_CAP):
                continue
            if (int(self._finite(old.get('order_bar_open_ms'))) != target['order_bar_open_ms']
                    or not math.isclose(self._finite(old.get('limit_price')), target['limit_price'], rel_tol=1e-12)
                    or (old.get('position_id') and old['position_id'] != target['position_id'])):
                continue
            old.update(signal_follow=True, signal_follow_position_id=target['position_id'],
                       position_id=target['position_id'],
                       exit_due_epoch=target['exit_due_epoch'], exit_due_ms=target['exit_due_ms'])
            old.pop('rotation_origin', None)
            count += 1
            changed = True
        if changed:
            pending = self.ledger.state.get('pending_rotation')
            if pending and (pending.get('victim') in targets or pending.get('challenger') in targets):
                self.ledger.state['pending_rotation'] = None
            self.ledger.save()

    def _entry_client_id(self, row):
        if not row.get('signal_follow'):
            return super()._entry_client_id(row)
        identity = str(row['position_id'])
        generation = int(self.ledger.state.get('signal_follow_generations', {}).get(identity, 0))
        digest = hashlib.sha1(identity.encode()).hexdigest()[:6]
        return self.ledger.client_id(row['symbol'], int(row['order_bar_open_ms']), f'sf{generation}_{digest}')

    def _advance_signal_follow(self, row):
        if not row.get('signal_follow') or row.get('signal_follow_generation_advanced'):
            return
        identity = str(row.get('signal_follow_position_id') or row.get('position_id') or '')
        generations = self.ledger.state.setdefault('signal_follow_generations', {})
        generations[identity] = int(generations.get(identity, 0)) + 1
        row['signal_follow_generation_advanced'] = True
        self.ledger.save()  # durable before the exact intent is retired

    def _retire_order(self, symbol, row, reason):
        self._advance_signal_follow(row)
        return super()._retire_order(symbol, row, reason)

    def _reject_entry_submission(self, row, cid, exc):
        current = self.tracked.get(str(row.get('symbol') or '').upper())
        if current and current.get('signal_follow'):
            self._advance_signal_follow(current)
            identity = str(current.get('signal_follow_position_id') or current.get('position_id'))
            retry_after = self.ledger.state.setdefault('signal_follow_retry_after', {})
            if isinstance(exc, EntryQuoteUnavailable):
                # Known not sent: the next fresh quote can restore a follow
                # immediately. Keep the advanced identity, never reuse its ID.
                retry_after.pop(identity, None)
            elif is_agreement_rejection(exc):
                self.ledger.state.setdefault('signal_follow_permission_blocks', {})[identity] = dict(
                    symbol=row.get('symbol'), error=str(exc)[:300], rejected_epoch=time.time())
            else:
                retry_after[identity] = time.time() + 10.0
        return super()._reject_entry_submission(row, cid, exc)

    def _follow_gate_entries(self, snapshot, *, now):
        targets = self._follow_targets(snapshot, now=now)
        if not self._follow_snapshot_complete(snapshot, targets):
            return {}
        allowed = {}
        for symbol, row in self.tracked.items():
            target = targets.get(symbol)
            if (len(allowed) < FOLLOW_ORDER_CAP and row.get('signal_follow') and target
                    and symbol not in self.positions and not row.get('cancel_requested')
                    and row.get('signal_follow_position_id') == target['position_id']
                    and not self._forced_entry_blocked(symbol, now=now)):
                allowed[str(row.get('client_order_id'))] = (symbol, int(row['order_bar_open_ms']))
        return allowed

    def _execute_entries(self, snapshot, status, *, now, **kwargs):
        fresh_signal = status == 'valid' or snapshot.get('_signal_snapshot_valid', False)
        targets = self._follow_targets(snapshot, now=now)
        complete = self._follow_snapshot_complete(snapshot, targets)
        if fresh_signal and complete:
            blocks = self.ledger.state.get('signal_follow_permission_blocks', {})
            active = {row['position_id'] for row in targets.values()}
            removed = [identity for identity in blocks if identity not in active]
            for identity in removed:
                blocks.pop(identity)
            if removed:
                self.ledger.save()
            self._promote_signal_follow_orders(snapshot, now=now)
        follows = [(s, r) for s, r in self.tracked.items() if r.get('signal_follow')]
        for index, (symbol, row) in enumerate(follows):
            target = targets.get(symbol)
            ended = (fresh_signal and complete
                     and (not target or row.get('signal_follow_position_id') != target['position_id']))
            forced = self._forced_entry_blocked(symbol, now=now)
            if (ended or symbol in self.positions or len(self.positions) >= FOLLOW_POSITION_CAP
                    or index >= FOLLOW_ORDER_CAP or forced):
                reason = ('signal_follow_ended' if ended else 'signal_follow_filled' if symbol in self.positions
                          else 'signal_follow_position_cap' if len(self.positions) >= FOLLOW_POSITION_CAP
                          else 'signal_follow_forced_block' if forced
                          else 'signal_follow_order_cap')
                self._request_cancel(symbol, row, reason)
        if status == 'valid' and complete and len(self.positions) < FOLLOW_POSITION_CAP:
            prices = self.refresh_entry_prices(now=now)
            for symbol, target in sorted(targets.items(), key=lambda p: (p[1]['order_bar_open_ms'], p[0])):
                if sum(bool(r.get('signal_follow')) for r in self.tracked.values()) >= FOLLOW_ORDER_CAP:
                    break  # unresolved cancels also occupy an extra slot
                if symbol in self.positions:
                    continue
                old = self.tracked.get(symbol)
                if old:
                    if not old.get('signal_follow'):
                        self._request_cancel(symbol, old, 'signal_follow_different_entry')
                    continue
                if now < self._finite(self.ledger.state.get('signal_follow_retry_after', {}).get(target['position_id'])):
                    continue
                if target['position_id'] in self.ledger.state.get('signal_follow_permission_blocks', {}):
                    self._submit_skip_reasons[symbol] = 'signal_follow_agreement_rejected_for_position'
                    continue
                price = self._finite(prices.get(symbol))
                if price <= 0:
                    continue
                row = dict(target, observed_price=price, gap=(price - target['limit_price']) / target['limit_price'],
                           signal_follow_position_id=target['position_id'])
                self._place_order(row)
        working = dict(snapshot)
        protected = set(self._target_positions(snapshot)) | {s for s, r in self.tracked.items() if r.get('signal_follow')}
        working['desired'] = [r for r in snapshot.get('desired', [])
                              if isinstance(r, Mapping) and str(r.get('symbol') or '').upper() not in protected]
        result = super()._execute_entries(working, status, now=now, **kwargs)
        self._entry_diagnostics['signal_follow'] = dict(
            pending=sum(bool(r.get('signal_follow')) for r in self.tracked.values()),
            max_pending=FOLLOW_ORDER_CAP, position_stop=FOLLOW_POSITION_CAP,
            symbols=sorted(s for s, r in self.tracked.items() if r.get('signal_follow')))
        return result
