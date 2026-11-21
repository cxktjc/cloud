"""Decision-local quote reuse must never bypass live send or state guards."""
import time
import threading
import unittest
from unittest.mock import patch

from tests import test_book_ticker_v2 as fixtures
from v9711_live.execution import ws_market as module


class PriceViewTests(unittest.TestCase):
    setUp = fixtures.DualMarketTests.setUp
    drain = fixtures.DualMarketTests.drain
    quote = fixtures.DualMarketTests.quote

    def test_price_maps_keep_tick_valuation_and_independent_source_validity(self):
        self.quote(tick=1.03, bid=1.01)
        self.assertEqual(self.market.price_maps(), ({'XUSDT': 1.03}, {'XUSDT': 1.01}, {'XUSDT': 1.01}))
        self.market.book.group_failed(1, 'disconnected')
        self.assertEqual(self.market.price_maps(), ({'XUSDT': 1.03}, {}, {'XUSDT': 1.03}))
        self.market.group_failed(1, 'disconnected')
        self.assertEqual(self.market.price_maps(), ({}, {}, {}))
        self.market.book.group_connected(1)
        self.quote(tick=None, bid=1.02, update_id=2)
        self.assertEqual(self.market.price_maps(), ({}, {'XUSDT': 1.02}, {'XUSDT': 1.02}))
        self.quote(tick=None, bid=1.02, quantity=0, update_id=3)
        self.assertEqual(self.market.price_maps(), ({}, {}, {}))

    def test_batch_clock_is_shared_but_freshness_is_rechecked_next_call(self):
        self.quote()
        with patch.object(self.market, '_quote_clock', wraps=self.market._quote_clock) as clock:
            self.assertTrue(self.market.entry_prices())
            clock.assert_called_once()
        with patch('time.time', return_value=time.time()+120):
            self.assertEqual(self.market.price_maps(), ({}, {}, {}))
        self.assertTrue(self.market.entry_prices())

    def test_metrics_scan_dual_quotes_once_and_keep_source_counts(self):
        self.quote(tick=None)
        with patch.object(self.market, 'price_maps', wraps=self.market.price_maps) as capture:
            metrics = self.market.metrics()
            capture.assert_called_once_with()
        self.assertEqual((metrics['fresh_symbols'], metrics['bid0_symbols'], metrics['entry_symbols']), (0, 1, 1))

    def test_diagnostic_sort_does_not_hold_the_ingress_lock(self):
        self.quote()
        started, release, accepted = threading.Event(), threading.Event(), threading.Event()
        errors = []
        def slow_sort(values, **kwargs):
            started.set()
            if not release.wait(2):
                raise AssertionError('diagnostic sort not released')
            return sorted(values, **kwargs)
        def capture_metrics():
            try:
                self.market.metrics()
            except Exception as exc:
                errors.append(exc)
        with patch.object(module, 'sorted', side_effect=slow_sort, create=True):
            metrics = threading.Thread(target=capture_metrics)
            metrics.start()
            try:
                self.assertTrue(started.wait(1))
                def ingress():
                    self.market.update('XUSDT', '1.03', int(time.time()*1000))
                    accepted.set()
                receiver = threading.Thread(target=ingress)
                receiver.start()
                self.assertTrue(accepted.wait(.5), 'diagnostic sort blocked WS ingestion')
                receiver.join(1)
            finally:
                release.set()
                metrics.join(2)
        self.assertFalse(metrics.is_alive())
        self.assertEqual(errors, [])


class OwnerPriceViewTests(unittest.TestCase):
    setUp = fixtures.DualAdmissionTests.setUp
    target = fixtures.DualAdmissionTests.target
    make_trade = fixtures.DualAdmissionTests.make_trade
    refresh_quotes = fixtures.DualAdmissionTests.refresh_quotes
    place = fixtures.DualAdmissionTests.place
    cancel = fixtures.DualAdmissionTests.cancel
    publish = fixtures.DualAdmissionTests.publish
    event = fixtures.DualAdmissionTests.event
    held = fixtures.DualAdmissionTests.held
    ordinary_plan = fixtures.DualAdmissionTests.ordinary_plan
    plans = fixtures.DualAdmissionTests.plans
    tick = fixtures.DualAdmissionTests.tick
    bid = fixtures.DualAdmissionTests.bid

    def test_owner_view_does_not_let_worker_use_disconnected_bid(self):
        self.plans()
        self.tick(1.03)
        self.bid(1.01)
        row = self.trade.tracked['N0USDT']
        self.assertTrue(self.trade._entry_quote_send_allowed('N0USDT', row['client_order_id'], 1.))
        def disconnect_during_decision(*args, **kwargs):
            self.assertEqual(self.trade.refresh_entry_prices()['N0USDT'], 1.01)
            self.trade.market.book.group_failed(1, 'disconnected during planning')
            self.assertFalse(self.trade._entry_quote_send_allowed('N0USDT', row['client_order_id'], 1.))
        with patch.object(self.trade, '_execute_entries_with_prices', side_effect=disconnect_during_decision):
            self.trade._execute_entries({}, 'valid')
        self.assertIsNone(self.trade._cycle_price_maps)
        self.assertEqual(self.trade.refresh_entry_prices()['N0USDT'], 1.03)

    def test_owner_view_clears_when_planning_raises(self):
        with patch.object(self.trade, '_execute_entries_with_prices', side_effect=RuntimeError('failed')):
            with self.assertRaisesRegex(RuntimeError, 'failed'):
                self.trade._execute_entries({}, 'valid')
        self.assertIsNone(self.trade._cycle_price_maps)

    def test_far_price_does_not_rebuild_gate_but_crossing_does(self):
        self.plans()
        self.tick(1.03)
        with patch.object(self.trade, '_publish_entry_gate', wraps=self.trade._publish_entry_gate) as publish:
            self.tick(1.04)
            publish.assert_not_called()
            self.tick(1.018)
            self.assertTrue(publish.called)
        self.assertIn('N0USDT', self.trade.tracked)

    def test_far_price_with_network_completion_still_runs_planner(self):
        self.plans()
        self.tick(1.03)
        with patch.object(self.trade, '_drain_network', return_value=1), \
                patch.object(self.trade, '_execute_entries', wraps=self.trade._execute_entries) as plan:
            self.tick(1.04)
            plan.assert_called_once()

    def test_expired_signal_revokes_gate_even_for_far_price(self):
        self.plans()
        self.tick(1.03)
        self.now += 31
        self.tick(1.04)
        self.assertFalse(self.trade.client.entry_gate['ready'])


if __name__ == '__main__':
    unittest.main()
