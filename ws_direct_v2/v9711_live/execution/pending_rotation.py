"""Durable exact replacement qualification; unrelated free slots remain live."""
from __future__ import annotations

import logging
from typing import Any, Mapping

from .. import config as cfg
from ..entry_policy import GapPolicy

LOG = logging.getLogger('v9711_live_trade')


class PendingRotationMixin:
    def _current_admission_gap(self) -> float:
        policy = GapPolicy()
        count = self._ordinary_pending_count()
        ceiling = policy.rotation_gap(count, cfg.MAX_PENDING_ORDERS) if count >= cfg.MAX_PENDING_ORDERS else policy.admission_gap(count)
        pending = self.ledger.state.get('pending_rotation')
        if pending:
            ceiling = policy.rotation_gap(int(self._finite(pending.get('original_order_count'))), cfg.MAX_PENDING_ORDERS)
        return ceiling

    def _prepare_rotation(self, plan) -> None:
        if plan.rotation_action == 'clear':
            if self.ledger.state.get('pending_rotation'):
                LOG.info('pending rotation cleared (bar expiry, fill, or identity change)')
            self.ledger.state['pending_rotation'] = None
        elif plan.rotation_action == 'reserve':
            self.ledger.state['pending_rotation'] = dict(plan.rotation)
            victim = self.tracked[plan.rotation['victim']]
            cid = str(victim.get('client_order_id') or '')
            self.ledger.state.setdefault('rotated_out_plans', {})[cid] = dict(
                symbol=plan.rotation['victim'], client_order_id=cid,
                order_bar_open_ms=int(self._finite(victim.get('order_bar_open_ms'))),
                plan_signature=self._entry_plan_signature(victim),
                reason='rotated_out_do_not_refill_same_plan')
            # Durable reservation must precede the first cancel request/ACK.
            self.ledger.save()
            row = plan.rotation
            LOG.info('reserve pending rotation old=%s new=%s old_gap=%.4f%% new_gap=%.4f%% '
                     'count=%s base_improvement=%.4fpp selected_improvement=%.4fpp '
                     'similar_order_rule=%s similar_order_band=%.4fpp max_new_gap=%.4f%%',
                     row['victim'], row['challenger'],
                     row.get('victim_gap', float('nan')) * 100,
                     row.get('challenger_gap', float('nan')) * 100,
                     row['original_order_count'], row.get('base_minimum_improvement', 0) * 100,
                     row.get('minimum_improvement', 0) * 100,
                     row.get('similar_order_rule', False), row.get('similar_order_band', 0) * 100,
                     row['admission_gap'] * 100)

    def _note_rotation_terminal(self, symbol: str, row: Mapping[str, Any], reason: str) -> None:
        pending = self.ledger.state.get('pending_rotation')
        if (not pending or pending.get('victim') != symbol
                or str(pending.get('victim_client_order_id') or '')
                != str(row.get('client_order_id') or '')):
            return
        pending['victim_terminal'] = True
        pending['victim_terminal_reason'] = reason
        pending['victim_filled'] = max(self._finite(row.get('accounted_entry_qty')),
                                       self._finite(row.get('confirmed_filled_qty'))) > 0

    def _consume_rotation(self, plan) -> None:
        if plan.rotation_action != 'consume':
            return
        # A preflight/reopen/confirmed-submit rejection may leave no identity.
        # In that case keep the original gates, rather than silently treating
        # the cancelled order as an ordinary free slot on the next step.
        for row in plan.place:
            if row.symbol in self.tracked or row.symbol in self.positions:
                self.ledger.state['pending_rotation'] = None
                LOG.info('pending rotation replacement reserved/sent symbol=%s', row.symbol)
                break
