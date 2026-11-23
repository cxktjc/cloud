"""Receive-side screening must preserve actionable transitions across feeds."""
import queue
from dataclasses import replace
from pathlib import Path
import tempfile
import time
import unittest

from v9711_live.execution.ws_market import DualMarket


class IngressTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.market = DualMarket(['XUSDT', 'YUSDT'], Path(self.tmp.name)/'prices.json')
        for feed in (self.market, self.market.book):
            feed.group_connected(1)
        self.drain()
        self.market.events.enable_ingress_screening()
        self.market.events.set_policy({'XUSDT': {'limit_price': 1.}}, set())
        self.stamp = int(time.time()*1000)

    def drain(self):
        result = []
        while not self.market.events.empty():
            tick = self.market.events.get_nowait()
            if tick is not None:
                result.append((tick.source, tick.price))
                self.market.decide(tick, lambda event: None)
            self.market.events.task_done()
        return result

    def test_far_prices_finish_without_owner_but_valuation_and_health_refresh(self):
        for price in ('1.03', '1.04', '1.05'):
            self.market.update('XUSDT', price, self.stamp)
        self.assertTrue(self.market.events.empty())
        self.assertEqual(self.market.processed['XUSDT'].price, '1.05')
        self.assertEqual(self.market.committed['XUSDT'].price, '1.05')
        self.assertEqual(self.market.counts['screened_far'], 3)

    def test_crossing_and_rebound_from_other_source_cannot_overtake_or_disappear(self):
        self.market.update('XUSDT', '1.04', self.stamp)
        self.market.update_book('XUSDT', '1.01', '2', self.stamp, 1)
        self.market.update('XUSDT', '1.05', self.stamp)
        self.market.update_book('XUSDT', '1.06', '2', self.stamp, 2)
        self.assertEqual(self.drain(), [('bid0', '1.01'), ('tick', '1.05'), ('bid0', '1.06')])

    def test_inflight_symbol_is_protected_until_callback_finishes(self):
        self.market.update('XUSDT', '1.01', self.stamp)
        tick = self.market.events.get_nowait()
        def callback(event):
            self.market.update('XUSDT', '1.08', self.stamp)
            self.assertEqual(self.market.processed['XUSDT'].price, '1.01')
            self.assertEqual(self.market.events.qsize(), 1)
        self.market.decide(tick, callback)
        self.market.events.task_done()
        self.assertEqual(self.drain(), [('tick', '1.08')])

    def test_policy_update_replays_completed_quote_without_waiting_for_another_ws_message(self):
        self.market.update('XUSDT', '1.04', self.stamp)
        self.market.events.set_policy({'XUSDT': {'limit_price': 1.04}}, set())
        self.assertEqual(self.drain(), [('tick', '1.04')])
        self.assertEqual(self.market.events.replayed['tick'], 1)

    def test_watched_symbol_unknown_template_and_zero_quantity_invalidation_always_queue(self):
        self.market.events.watch('XUSDT', True)
        self.market.update('XUSDT', '1.2', self.stamp)
        self.market.update('YUSDT', '1.2', self.stamp)
        self.market.update_book('XUSDT', '1.2', '0', self.stamp, 1)
        self.assertEqual(len(self.drain()), 3)
        self.assertNotIn('XUSDT', self.market.book.processed)

    def test_unchanged_book_refresh_is_used_when_new_template_replays_old_far_price(self):
        self.market.update_book('XUSDT', '1.04', '2', self.stamp, 1)
        key = 'XUSDT', 'bid0'
        old = self.market.events._screened[key]
        self.market.events._screened[key] = replace(old, received_ns=old.received_ns-2_000_000_000)
        self.market.update_book('XUSDT', '1.04', '2', self.stamp, 2)
        newest = self.market.book.latest_ingress['XUSDT']
        self.market.events.set_policy({'XUSDT': {'limit_price': 1.04}}, set())
        tick = self.market.events.get_nowait()
        self.assertIs(tick, newest)
        self.assertTrue(self.market.decide(tick, lambda event: None))
        self.market.events.task_done()

    def test_completed_far_quote_does_not_hide_a_source_invalidation(self):
        self.market.update_book('XUSDT', '1.04', '2', self.stamp, 1)
        self.market.update_book('XUSDT', '1.04', '0', self.stamp, 2)
        self.market.events.set_policy({'XUSDT': {'limit_price': 1.04}}, set())
        result = self.drain()
        self.assertEqual(result, [('bid0', '0')])
        self.assertNotIn('XUSDT', self.market.book.processed)

    def test_replay_waits_when_bounded_queue_is_full_and_is_not_lost(self):
        self.market.events.maxsize = 1
        self.market.update('XUSDT', '1.04', self.stamp)
        self.market.update('YUSDT', '1.2', self.stamp)
        self.market.events.set_policy({'XUSDT': {'limit_price': 1.04}}, set())
        self.assertEqual(self.market.events.qsize(), 1)
        self.assertEqual(self.drain(), [('tick', '1.2'), ('tick', '1.04')])
        self.assertFalse(self.market.events._rescreen)

    def test_failed_callback_releases_inflight_protection_without_publishing_crossing(self):
        self.market.update('XUSDT', '1.01', self.stamp)
        tick = self.market.events.get_nowait()
        with self.assertRaises(RuntimeError):
            self.market.decide(tick, lambda event: (_ for _ in ()).throw(RuntimeError('failed')))
        self.market.events.task_done()
        self.assertNotIn('XUSDT', self.market.events._inflight)
        self.assertNotIn('XUSDT', self.market.committed)

    def test_reconnected_generation_cannot_replay_old_completed_far_quote(self):
        self.market.update('XUSDT', '1.04', self.stamp)
        self.market.group_connected(1)
        self.market.events.set_policy({'XUSDT': {'limit_price': 1.04}}, set())
        observed = []
        while not self.market.events.empty():
            tick = self.market.events.get_nowait()
            if tick is not None:
                self.assertFalse(self.market.decide(tick, lambda event: observed.append(event)))
            self.market.events.task_done()
        self.assertFalse(observed)


if __name__ == '__main__':
    unittest.main()
