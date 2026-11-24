"""Ordinary entries use fresh ingress even when woken by a different symbol."""
from dataclasses import replace
import time
import unittest
from unittest.mock import patch

from tests import test_book_ticker_v2 as fixtures
from v9711_live import config as cfg


class AdmissionFreshnessTests(unittest.TestCase):
    def setUp(self):
        self.case = fixtures.DualAdmissionTests('test_tick_only_qualifies')
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.case.plans(2)
        self.trade = self.case.trade
        self.market = self.trade.market
        self.symbol = 'N0USDT'
        self.stamp = int(time.time()*1000)
        budget = patch.object(cfg, 'WS_MAX_QUEUE_AGE_SECONDS', .5)
        budget.start()
        self.addCleanup(budget.stop)

    def tick_ingress(self, price):
        self.assertTrue(self.market.update(self.symbol, str(price), self.stamp))
        return self.market.latest_ingress[self.symbol]

    def bid_ingress(self, price, update_id=1):
        self.assertTrue(self.market.update_book(self.symbol, str(price), '10', self.stamp, update_id))
        return self.market.book.latest_ingress[self.symbol]

    def age_ingress(self, feed):
        tick = feed.latest_ingress[self.symbol]
        feed.latest_ingress[self.symbol] = replace(tick, received_ns=tick.received_ns-1_645_000_000,
                                                   received_epoch_ms=tick.received_epoch_ms-1645)

    def drain_only(self):
        while not self.market.events.empty():
            self.market.decide(self.market.events.get_nowait(), lambda tick: None)
            self.market.events.task_done()

    def turn(self):
        now = time.time()
        snapshot, status = self.trade.desired_snapshot(now=now)
        self.trade._io_signal = snapshot, status
        self.trade._execute_entries(snapshot, status, now=now, targets={}, final_exit_bar=False)

    def test_old_low_quote_does_not_admit_and_valuation_stays_available(self):
        self.tick_ingress(1.009)
        self.drain_only()
        self.age_ingress(self.market)
        self.bid_ingress(1.03)  # Fresh but outside the admission band.
        self.turn()
        self.assertNotIn(self.symbol, self.trade.tracked)
        self.assertEqual(self.market.prices()[self.symbol], 1.009)
        self.assertEqual(self.market.admission_quote(self.symbol)['selected'], 1.03)

    def test_both_old_wait_for_new_same_symbol_evidence(self):
        self.tick_ingress(1.009)
        self.bid_ingress(1.009)
        self.drain_only()
        self.age_ingress(self.market)
        self.age_ingress(self.market.book)
        self.turn()
        self.assertEqual(self.trade._entry_diagnostics['filtered'][self.symbol], 'admission_quote_expired')
        self.assertNotIn(self.symbol, self.trade.tracked)
        new_bid = self.bid_ingress(1.009, 2)
        self.assertEqual(self.market.events.qsize(), 1)  # unchanged price regained eligibility
        self.market.decide(self.market.events.get_nowait(), self.trade.on_tick)
        self.market.events.task_done()
        row = self.trade.tracked[self.symbol]
        self.assertEqual(row['entry_qualified_by'], 'depth')
        timing = self.trade.session_entries.timings.order(row['client_order_id'])
        self.assertEqual(timing['received_epoch_ms'], new_bid.received_epoch_ms)

    def test_uses_latest_unprocessed_prices_and_matching_timestamp(self):
        self.tick_ingress(1.03)
        self.drain_only()
        self.market.book._last_wire_ns = time.monotonic_ns() - 5_000_000
        self.bid_ingress(1.012)
        newest = self.tick_ingress(1.012)
        self.turn()  # planner did not consume either queued quote
        row = self.trade.tracked[self.symbol]
        self.assertEqual(row['entry_qualified_by'], 'all')
        self.assertEqual(row['entry_quote_source'], 'tick')  # equal prices, use newer evidence
        timing = self.trade.session_entries.timings.order(row['client_order_id'])
        self.assertEqual(timing['received_epoch_ms'], newest.received_epoch_ms)
        self.assertEqual(self.market.prices()[self.symbol], 1.03)


if __name__ == '__main__':
    unittest.main()
