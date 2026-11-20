"""Below-limit candidates rank first in diagnostics and physical admission."""
import unittest

from tests import test_book_ticker_v2 as fixtures
from tests.test_entry_tiers_v2 import row
from v9711_live import config as cfg
from v9711_live.execution.entry_quotes import qualify_quote
from v9711_live.execution.logic import select_physical_orders
from v9711_live.signal_diagnostics import nearest_candidates, nearest_candidates_line
from v9711_live.signal_logic import Candidate, SignalInput, select_orders


def candidate(symbol):
    return Candidate(symbol=symbol, order_bar_open_ms=180000, order_bar_available_ms=360000,
                     reference_bar_open_ms=0, reference_bar_available_ms=180000,
                     limit_price=100., qv20=1., notional=5., reference_price=102.,
                     exit_due_epoch=1080., exit_due_ms=1080000, exit_due_utc='test')


class SignedGapTests(unittest.TestCase):
    def test_diagnostic_ranks_negative_before_zero_and_positive(self):
        gaps = {'ZDEEP': -.08, 'YNEG': -.02, 'AFLAT': 0., 'BPOS': .001, 'CPOS': .01}
        candidates = [candidate(s) for s in gaps]
        prices = {s: SignalInput(100*(1+g), 180001, g) for s, g in gaps.items()}
        plan = select_orders(candidates, prices)
        rows = nearest_candidates(candidates, prices, desired_symbols=[r.symbol for r in plan.desired])
        self.assertEqual([r.symbol for r in rows], ['ZDEEP', 'YNEG', 'AFLAT'])
        self.assertEqual([r.status for r in rows[:2]], ['below_limit_desired']*2)
        text = nearest_candidates_line(rows, candidate_count=len(candidates), price_healthy=True)
        self.assertIn('按价差从小到大，低于限价优先', text)
        self.assertNotIn('按绝对价差', text)

    def test_signal_publishes_all_negative_gaps_without_absolute_distance_filter(self):
        gaps = {'ZDEEP': -.20, 'YNEG': -.02, 'ANEAR': .001, 'FAR': .03}
        candidates = [candidate(s) for s in gaps]
        prices = {s: SignalInput(100*(1+g), 180001, g) for s, g in gaps.items()}
        plan = select_orders(candidates, prices)
        self.assertEqual({r.symbol for r in plan.desired}, {'ZDEEP', 'YNEG', 'ANEAR'})
        self.assertEqual([r.symbol for r in plan.decisions if r.action == 'place'], ['ZDEEP', 'YNEG', 'ANEAR'])

    def test_trade_selects_lowest_signed_gaps_first_at_eight_order_cap(self):
        gaps = [.001, .002, .003, .004, .005, .006, .007, -.02, -.08]
        plan = select_physical_orders([row(f'N{i}', gap) for i, gap in enumerate(gaps)], {})
        self.assertEqual([r.symbol for r in plan.place[:2]], ['N8', 'N7'])
        self.assertEqual(len(plan.place), 8)
        self.assertNotIn('N6', [r.symbol for r in plan.place])

    def test_full_book_rotation_uses_signed_improvement_even_when_both_negative(self):
        tracked = {f'OLD{i}': row(f'OLD{i}', -.001) for i in range(8)}
        plan = select_physical_orders([*tracked.values(), row('NEW', -.0032)], tracked)
        self.assertEqual(plan.cancel, ('OLD7',))
        self.assertEqual(plan.rotation['challenger'], 'NEW')
        self.assertEqual([r.symbol for r in plan.place], ['NEW'])

    def test_tick_and_bid_send_qualification_accept_negative_gaps(self):
        for tick, bid, expected in ((.8, 1.03, 'tick'), (1.03, .8, 'depth'), (.8, .9, 'all')):
            with self.subTest(expected=expected):
                result = qualify_quote(dict(tick=tick, bid0=bid, source='test'), 1., .0165)
                self.assertEqual(result['entry_qualified_by'], expected)


class OwnerSignedGapTests(unittest.TestCase):
    setUp = fixtures.DualAdmissionTests.setUp
    target = fixtures.DualAdmissionTests.target
    make_trade = fixtures.DualAdmissionTests.make_trade
    refresh_quotes = fixtures.DualAdmissionTests.refresh_quotes
    place = fixtures.DualAdmissionTests.place
    cancel = fixtures.DualAdmissionTests.cancel
    publish = fixtures.DualAdmissionTests.publish

    def test_live_shortlist_and_dispatch_prioritize_deep_negative_gap(self):
        self.targets = []
        self.templates = [dict(symbol=f'N{i}USDT', limit_price=1., notional=5.,
            order_bar_open_ms=self.bar, reference_bar_open_ms=self.bar-cfg.BAR_MS,
            exit_due_epoch=self.now+900) for i in range(9)]
        self.publish()
        gaps = [.001, .002, .003, .004, .005, .006, .007, -.02, -.08]
        for item, gap in zip(self.templates, gaps):
            self.trade.market.update(item['symbol'], str(1+gap), int(self.now*1000))
            tick = self.trade.market.events.get_nowait()
            self.trade.market.decide(tick, lambda event: None)
        snapshot, status = self.trade.desired_snapshot(now=self.now)
        working = self.trade._fast_snapshot(snapshot, tick, self.now)
        self.assertEqual(len(working['desired']), 8)
        self.assertIn('N8USDT', [r['symbol'] for r in working['desired']])
        self.assertNotIn('N6USDT', [r['symbol'] for r in working['desired']])
        self.trade.market.update('N8USDT', '.92', int(self.now*1000))
        self.trade.market.decide(self.trade.market.events.get_nowait(), self.trade.on_tick)
        self.assertEqual([r['symbol'] for r in self.submits[:2]], ['N8USDT', 'N7USDT'])
        self.assertEqual(len(self.submits), 8)
        self.assertNotIn('N6USDT', self.trade.tracked)


if __name__ == '__main__':
    unittest.main()
