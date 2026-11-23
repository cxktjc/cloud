"""Known-unsent rejection cannot recycle the same owner/ingress evidence."""
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from tests import test_recheck_v15 as tiers


class RetryLoopTests(unittest.TestCase):
    setUp = tiers.DurableTieredTests.setUp
    finish = tiers.DurableTieredTests.finish
    wait_done = tiers.DurableTieredTests.wait_done
    block_disk = tiers.DurableTieredTests.block_disk
    first_far_entry = tiers.DurableTieredTests.first_far_entry

    def reject_rebound(self):
        started = self.block_disk()
        old = self.first_far_entry()
        self.assertTrue(started.wait(1))
        self.trade.market.update('N0USDT', '1.019', int(self.case.now*1000))
        self.release.set()
        self.wait_done()
        self.assertFalse(self.sent)
        return old

    def plan_again(self, count=1):
        snapshot, status = self.trade.desired_snapshot(now=self.case.now)
        for _ in range(count):
            self.trade._execute_entries(snapshot, status, now=self.case.now, targets={}, final_exit_bar=False)
            self.wait_done()

    def test_repeating_maintenance_cannot_allocate_or_reject_same_quote_again(self):
        self.reject_rebound()
        sequence = self.trade.ledger.state['entry_submission_sequence']
        with patch('v9711_live.execution.runner.LOG.warning') as warning:
            self.plan_again(20)
        warning.assert_not_called()
        self.assertEqual(self.trade.stats['entry_rejected'], 1)
        self.assertEqual(self.trade.ledger.state['entry_submission_sequence'], sequence)
        self.assertEqual(len(self.trade.ledger.state['entry_submit_rejections']), 1)
        self.assertFalse(self.sent)
        self.assertFalse(self.trade.tracked)

    def test_new_near_opportunity_retries_immediately_without_secondary_read(self):
        self.reject_rebound()
        self.trade._service_pending_quotes(limit=64, budget_seconds=.05)
        with patch.object(self.trade.market, 'send_quote', side_effect=AssertionError('near retry WS recheck')):
            self.trade.market.update('N0USDT', '1.01', int(self.case.now*1000))
            self.trade._service_pending_quotes(limit=64, budget_seconds=.05)
            self.wait_done()
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(self.trade.stats['entry_rejected'], 1)
        self.assertEqual(self.trade.ledger.state['entry_submission_sequence'], 2)

    def test_old_queued_near_crossing_cannot_rearm_after_newer_rebound_rejected(self):
        started = self.block_disk()
        self.first_far_entry()
        self.assertTrue(started.wait(1))
        self.trade.market.update('N0USDT', '1.01', int(self.case.now*1000))
        self.trade.market.update('N0USDT', '1.019', int(self.case.now*1000))
        self.release.set()
        self.wait_done()
        with patch.object(self.trade.market, 'send_quote', side_effect=AssertionError('old quote retried')):
            self.trade._service_pending_quotes(limit=64, budget_seconds=.05)
            self.wait_done()
        self.assertEqual(self.trade.ledger.state['entry_submission_sequence'], 1)
        self.assertEqual(self.trade.stats['entry_rejected'], 1)
        self.assertFalse(self.sent)

    def test_new_quote_arriving_before_owner_handles_rejection_is_not_lost(self):
        started = self.block_disk()
        self.first_far_entry()
        self.assertTrue(started.wait(1))
        self.trade.market.update('N0USDT', '1.019', int(self.case.now*1000))
        rejected_sequence = self.trade.market.latest_ingress['N0USDT'].receive_seq
        self.release.set()
        deadline = time.perf_counter()+2
        while self.trade._network_io.health()['completed'] < 1 and time.perf_counter() < deadline:
            time.sleep(.001)
        self.assertEqual(self.trade._network_io.health()['completed'], 1)
        self.trade.market.update('N0USDT', '1.01', int(self.case.now*1000))
        self.trade._drain_network()
        self.assertEqual(self.trade.market._entry_reject_cutoffs['N0USDT']['tick'], rejected_sequence)
        self.trade._service_pending_quotes(limit=64, budget_seconds=.05)
        self.wait_done()
        self.assertEqual(len(self.sent), 1)

    def test_signal_reader_not_ready_does_not_repeat_account_rejections(self):
        started = self.block_disk()
        self.first_far_entry('1.01')
        self.assertTrue(started.wait(1))
        snapshot, status = self.trade.desired_snapshot(now=self.case.now)
        self.trade._signal_reader = SimpleNamespace(healthy=lambda: False)
        try:
            self.release.set()
            self.wait_done()
            for _ in range(20):
                self.trade._execute_entries(snapshot, status, now=self.case.now, targets={}, final_exit_bar=False)
                self.wait_done()
            self.assertEqual(self.trade.stats['entry_rejected'], 1)
            self.assertEqual(self.trade.ledger.state['entry_submission_sequence'], 1)
            self.assertEqual(self.trade.market._entry_reject_cutoffs, {})
        finally:
            self.trade._signal_reader = None
        self.plan_again()
        self.assertEqual(len(self.sent), 1)


class QuoteEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.case = tiers.TieredQuoteTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.market = self.case.trade.market

    def process_tick(self, price):
        self.market.update('N0USDT', str(price), int(self.case.case.now*1000))
        self.market.decide(self.market.latest_ingress['N0USDT'], lambda event: None)

    def process_bid(self, price, update_id):
        self.market.update_book('N0USDT', str(price), '5', int(self.case.case.now*1000), update_id)
        self.market.decide(self.market.book.latest_ingress['N0USDT'], lambda event: None)

    def test_receive_sequence_distinguishes_quotes_with_identical_timestamps(self):
        with patch('time.monotonic_ns', return_value=1_000_000_000):
            self.process_tick(1.018)
            first = self.market.processed['N0USDT']
            self.market.reject_entry_quotes('N0USDT')
            self.process_tick(1.01)
            second = self.market.processed['N0USDT']
            self.assertEqual(first.received_ns, second.received_ns)
            self.assertGreater(second.receive_seq, first.receive_seq)
            self.assertEqual(self.market.entry_quote('N0USDT')['selected'], 1.01)

    def test_old_tick_is_valuation_only_and_new_bid_can_qualify_independently(self):
        self.process_tick(1.01)
        self.market.update('N0USDT', '1.03', int(self.case.case.now*1000))
        self.market.reject_entry_quotes('N0USDT', self.market.send_quote('N0USDT')['observed_watermarks'])
        self.process_bid(1.018, 1)
        quote = self.market.entry_quote('N0USDT')
        self.assertIsNone(quote['tick'])
        self.assertEqual(quote['selected'], 1.018)
        self.assertEqual(quote['source'], 'bid0')
        ticks, bids, entries = self.market.price_maps(['N0USDT'])
        self.assertEqual(ticks['N0USDT'], 1.01)
        self.assertEqual(entries['N0USDT'], 1.018)
        self.assertEqual(self.market.prices(['N0USDT'])['N0USDT'], 1.01)

    def test_unchanged_book_refresh_can_recover_without_price_change_or_new_queue_event(self):
        self.process_bid(1.018, 1)
        self.market.reject_entry_quotes('N0USDT')
        self.assertIsNone(self.market.entry_quote('N0USDT')['selected'])
        size = self.market.events.qsize()
        self.market.update_book('N0USDT', '1.018', '5', int(self.case.case.now*1000), 2)
        self.assertEqual(self.market.events.qsize(), size)
        self.assertEqual(self.market.entry_quote('N0USDT')['selected'], 1.018)

    def test_other_symbols_keep_admission_and_repeated_cutoff_cannot_move_backwards(self):
        self.process_tick(1.018)
        latest = self.market.latest_ingress['N0USDT'].receive_seq
        self.market.reject_entry_quotes('N0USDT')
        self.market.reject_entry_quotes('N0USDT', dict(tick=latest-1, bid0=0))
        self.assertEqual(self.market._entry_reject_cutoffs['N0USDT']['tick'], latest)
        self.assertIsNone(self.market.entry_quote('N0USDT')['selected'])
        self.assertTrue(self.market.order_allowed('N1USDT'))
        self.assertEqual(self.market.quote_state('N0USDT'), 'quote_waiting_fresh_after_send_rejection')


if __name__ == '__main__':
    unittest.main()
