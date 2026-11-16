"""Count-based sizing; eight competing pending orders are NOT eight filled slots.

Concurrency caveat: already-resting orders may fill before cancellations arrive.
Never rewrite those fills or pretend count-based order sizing is a 1x risk cap.
"""
from .. import config as cfg


def weight_for_count(count):
    if count < 0:
        raise ValueError('negative position count')
    # Preserve v1's explicitly requested final-pre-exit-bar exception: use the
    # smallest tier, not a new 50% cycle, if temporarily holding >=3 positions.
    return cfg.SLOT_WEIGHTS[min(count, len(cfg.SLOT_WEIGHTS)-1)]


class CountSizingMixin:
    def _entry_sizing_row(self, row):
        item = dict(row)
        count = len(self.positions)  # partial fills / pending closes still count
        weight = weight_for_count(count)
        old = self.tracked.get(str(item.get('symbol', '')).upper()) or {}
        same_plan = old and all(old.get(k) == item.get(k) for k in
            ('order_bar_open_ms', 'reference_bar_open_ms', 'limit_price'))
        if same_plan and old.get('sizing_weight') == weight:
            for k in ('notional','sizing_weight','sizing_position_count','sizing_balance','sizing_raw_notional'):
                if k in old:
                    item[k] = old[k]
            return item  # do not churn on every capital movement
        balance = self._finite(self.ledger.state.get('realized_balance'), 0.0)
        raw = balance * weight
        item.update(notional=raw, sizing_raw_notional=raw, sizing_balance=balance,
                    sizing_weight=weight, sizing_position_count=count)
        return item

    def _execute_entries(self, snapshot, status, **kwargs):
        weight = weight_for_count(len(self.positions))
        for symbol, row in list(self.tracked.items()):
            if (symbol not in self.positions and row.get('sizing_weight') != weight
                    and not row.get('cancel_requested')):
                # Persist the reason before async cancellation. UNKNOWN retains
                # its identity; no replacement until exact terminal evidence.
                row['sizing_resize_pending'] = True
                self.ledger.save()
                self._request_cancel(symbol, row, 'position_count_sizing_changed')
        result = super()._execute_entries(snapshot, status, **kwargs)
        self._entry_diagnostics['sizing'] = self._sizing_view()
        return result

    def _retire_order(self, symbol, row, reason):
        # Save generation BEFORE removing the old exact identity. A restart in
        # between cannot reuse the old client ID or lose an unsettled order.
        if (row.get('sizing_resize_pending') and not row.get('sizing_generation_advanced')
                and self._finite(row.get('accounted_entry_qty')) == 0
                and symbol not in self.positions):
            key = f'{symbol}:{int(self._finite(row.get("order_bar_open_ms")))}'
            generations = self.ledger.state.setdefault('sizing_generations', {})
            generations[key] = int(generations.get(key, 0)) + 1
            row['sizing_generation_advanced'] = True
            self.ledger.save()
        return super()._retire_order(symbol, row, reason)

    def _sizing_view(self):
        count = len(self.positions)
        balance = self._finite(self.ledger.state.get('realized_balance'), 0.0)
        return dict(policy=cfg.SIZING_POLICY, positions=count,
                    weight=weight_for_count(count), balance=balance,
                    raw_notional=balance*weight_for_count(count),
                    minimum_note='5U/minNotional/minQty may increase executable size',
                    simultaneous_fills_may_exceed_target=True)

    def write_feedback(self, **kwargs):
        self._entry_diagnostics = dict(getattr(self, '_entry_diagnostics', {}), sizing=self._sizing_view())
        return super().write_feedback(**kwargs)
