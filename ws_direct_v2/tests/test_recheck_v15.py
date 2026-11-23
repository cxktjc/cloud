"""Distance-tiered send checks, exact retry and no near-entry WS reread."""
import time
import unittest
from unittest.mock import Mock, patch

from tests import test_book_ticker_v2 as fixtures
from tests import test_durable_writer_v2 as durable
from v9711_live.execution.client import ExchangeClient
from v9711_live.execution.entry_quotes import admission_gap, SEND_RECHECK_GAP
from v9711_live.execution.submit_errors import ConfirmedSubmitRejection, EntryQuoteUnavailable
from v9711_live.execution.terminal import entry_timing_lines


class TieredQuoteTests(unittest.TestCase):
    def setUp(self):
        self.case = fixtures.DualAdmissionTests('test_tick_only_qualifies')
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.trade = self.case.trade
        self.index = 0

    def seed(self, tick=1.018, bid=None, *, actual_limit=1., ceiling=.0185, following=False, symbol='N0USDT'):
        self.index += 1
        cid = f'test-{self.index}'
        detail = dict(symbol=symbol, submitted_limit_price=actual_limit,
            entry_tick=tick, entry_bid0=bid, entry_qualified_by='signal_follow' if following else 'tick',
            entry_tick_gap=None, entry_depth_gap=None, entry_gap_ceiling=ceiling,
            entry_quote_source='tick', entry_quote_policy='tiered_admission')
        self.trade.client.entry_gate = dict(admitted_entry_quotes={cid: detail},
            entry_gap_limits={cid: ceiling}, follow_entries={cid: {}} if following else {})
        self.trade.session_entries.timings.register(symbol, cid, dict(limit_price=1.))
        return cid

    def snapshot(self, cid):
        return self.trade._entry_quote_send_snapshot('N0USDT', cid, 1.)

    def test_near_negative_and_exact_1p5_percent_never_read_latest_quotes(self):
        for price in (.90, 1., 1.01499, 1.015):
            with self.subTest(price=price):
                cid = self.seed(price)
                with patch.object(self.trade.market, 'send_quote', side_effect=AssertionError('near WS read')):
                    result = self.snapshot(cid)
                self.assertEqual(result['entry_send_check'], 'near_skipped')
                self.assertEqual(result['entry_tick'], price)

    def test_above_1p5_rechecks_once_and_retains_admission_tier_after_price_drops(self):
        cid = self.seed(1.0150001)
        with patch.object(self.trade.market, 'send_quote',
                          return_value=dict(tick=1.01, bid0=None, source='tick')) as capture:
            result = self.snapshot(cid)
        capture.assert_called_once_with('N0USDT')
        self.assertEqual(result['entry_send_check'], 'far_passed')
        self.assertGreater(result['entry_recheck_gap'], SEND_RECHECK_GAP)
        self.assertEqual(result['entry_tick'], 1.01)

    def test_or_admission_with_one_near_source_skips_recheck(self):
        cid = self.seed(1.018, 1.0149)
        with patch.object(self.trade.market, 'send_quote', side_effect=AssertionError('near OR WS read')):
            self.assertEqual(self.snapshot(cid)['entry_send_check'], 'near_skipped')

    def test_far_latest_or_accepts_bid_when_tick_has_rebounded(self):
        cid = self.seed()
        with patch.object(self.trade.market, 'send_quote',
                          return_value=dict(tick=1.04, bid0=1.0185, source='bid0')):
            result = self.snapshot(cid)
        self.assertEqual(result['entry_send_check'], 'far_passed')
        self.assertEqual(result['entry_qualified_by'], 'depth')

    def test_far_rebound_or_both_invalid_is_retryable_and_audited(self):
        for tick, bid in ((1.019, 1.019), (None, None)):
            with self.subTest(tick=tick, bid=bid):
                cid = self.seed()
                with patch.object(self.trade.market, 'send_quote',
                                  return_value=dict(tick=tick, bid0=bid, source=None)):
                    with self.assertRaises(EntryQuoteUnavailable):
                        self.trade.client._checked_entry_quote('N0USDT', cid, 1.)
                audit = self.trade.session_entries.timings.order(cid)
                self.assertEqual(audit['entry_send_check'], 'far_rejected')
                self.assertIn('二次行情检查拒绝', '\n'.join(entry_timing_lines(audit)))

    def test_exchange_rounded_limit_controls_1p5_tier(self):
        cid = self.seed(1.01501, actual_limit=1.)
        with patch.object(self.trade.market, 'send_quote', return_value=dict(tick=1.0186, bid0=None, source='tick')) as read:
            self.assertEqual(self.trade._entry_quote_send_snapshot('N0USDT', cid, 1.0001)['entry_send_check'], 'far_rejected')
        read.assert_called_once()

    def test_far_replacement_uses_its_saved_ceiling_not_ordinary_fill_ceiling(self):
        cid = self.seed(1.0188, ceiling=.0188)
        with patch.object(self.trade.market, 'send_quote', return_value=dict(tick=1.0187, bid0=None, source='tick')):
            self.assertEqual(self.snapshot(cid)['entry_send_check'], 'far_passed')
        with patch.object(self.trade.market, 'send_quote', return_value=dict(tick=1.0188001, bid0=None, source='tick')):
            self.assertEqual(self.snapshot(cid)['entry_send_check'], 'far_rejected')

    def test_follow_keeps_original_rule_and_does_not_gain_an_ordinary_gap_cap(self):
        cid = self.seed(1.20, following=True)
        with patch.object(self.trade.market, 'send_quote', side_effect=AssertionError('follow WS read')):
            self.assertEqual(self.snapshot(cid)['entry_send_check'], 'follow_skipped')

    def test_promoted_follow_identity_also_skips_far_recheck(self):
        cid = self.seed(1.018)
        self.trade.client.entry_gate['follow_entries'][cid] = {}
        with patch.object(self.trade.market, 'send_quote', side_effect=AssertionError('promoted follow WS read')):
            self.assertEqual(self.snapshot(cid)['entry_send_check'], 'follow_skipped')

    def test_missing_exact_admission_cannot_be_replaced_with_a_qualifying_live_quote(self):
        self.seed()
        with patch.object(self.trade.market, 'send_quote', side_effect=AssertionError('missing CID WS read')):
            with self.assertRaises(ConfirmedSubmitRejection):
                self.snapshot('missing')

    def test_admission_gap_missing_or_nonfinite_prices_is_not_falsely_near(self):
        self.assertIsNone(admission_gap(dict(entry_tick=float('nan'), entry_bid0=None), 1.))
        self.assertIsNone(admission_gap(dict(entry_tick=1.), 0.))


class DurableTieredTests(unittest.TestCase):
    setUp = durable.OwnerDurabilityTests.setUp
    finish = durable.OwnerDurabilityTests.finish
    wait_done = durable.OwnerDurabilityTests.wait_done
    block_disk = durable.OwnerDurabilityTests.block_disk

    def first_far_entry(self, price='1.018'):
        self.case.templates = self.case.templates[:1]
        self.case.publish()
        self.trade.market.update('N0USDT', price, int(self.case.now*1000))
        self.trade.market.decide(self.trade.market.events.get_nowait(), self.trade.on_tick)
        return dict(self.trade.tracked['N0USDT'])

    def test_far_rejection_after_disk_wait_releases_slot_and_fresh_near_quote_retries_once(self):
        started = self.block_disk()
        old = self.first_far_entry()
        self.assertTrue(started.wait(1))
        self.trade.market.update('N0USDT', '1.019', int(self.case.now*1000))
        self.release.set()
        self.wait_done()
        self.assertFalse(self.sent)
        self.assertNotIn('N0USDT', self.trade.tracked)
        self.assertFalse(self.trade._entry_submission_blocked(self.case.templates[0]))
        self.assertTrue(self.trade.ledger.state['entry_submit_rejections'][old['client_order_id']]['retryable'])
        self.trade._service_pending_quotes(limit=64, budget_seconds=.05)
        self.trade.market.update('N0USDT', '1.01', int(self.case.now*1000))
        self.trade._service_pending_quotes(limit=64, budget_seconds=.05)
        self.wait_done()
        self.assertEqual(len(self.sent), 1)
        self.assertNotEqual(self.sent[0], old['client_order_id'])
        self.trade.market.update('N0USDT', '1.01', int(self.case.now*1000))
        self.trade._service_pending_quotes(limit=64, budget_seconds=.05)
        self.wait_done()
        self.assertEqual(len(self.sent), 1)

    def test_far_valid_depth_is_checked_after_socket_readiness(self):
        stages = []
        original = self.trade.client._broker_call
        def broker(*args, before_send, **kwargs):
            stages.append('socket_ready')
            return original(*args, before_send=before_send, **kwargs)
        self.trade.client._broker_call = broker
        original_quote = self.trade.market.send_quote
        def read(symbol):
            stages.append('quote_check')
            return original_quote(symbol)
        self.trade.market.update_book('N0USDT', '1.018', '5', int(self.case.now*1000), 1)
        self.trade.market.decide(self.trade.market.events.get_nowait(), lambda event: None)
        with patch.object(self.trade.market, 'send_quote', side_effect=read):
            self.first_far_entry()
            self.wait_done()
        self.assertEqual(stages, ['socket_ready', 'quote_check'])
        self.assertEqual(len(self.sent), 1)

    def test_actual_stale_ingress_blocks_far_entries(self):
        started = self.block_disk()
        old = self.first_far_entry()
        self.assertTrue(started.wait(1))
        future_ns = time.monotonic_ns()+600_000_000
        with patch('time.monotonic_ns', return_value=future_ns):
            self.release.set()
            self.wait_done()
        self.assertFalse(self.sent)
        self.assertEqual(self.trade.session_entries.timings.order(old['client_order_id'])['entry_send_check'], 'far_rejected')

    def assert_booked_fill_is_not_missing(self, price):
        self.first_far_entry(price)
        self.wait_done()
        self.assertEqual(len(self.sent), 1)
        row = self.trade.tracked['N0USDT']
        signal_entry = dict(symbol='N0USDT', position_id=f'session:N0USDT:{self.case.bar}',
            quantity=row['qty'], entry_bar_open_ms=self.case.bar, entry_available_ms=self.case.bar+180000)
        stats = self.trade.session_entries
        stats.observe_signal(dict(signal_ready=True, signal_session_id='session', target_positions=[signal_entry]),
                             status='valid', now=self.case.bar/1000+181)
        self.assertEqual(stats.snapshot()['unfilled_entries'], 1)  # ACK is not a fill.
        self.assertTrue(self.trade._register_fill('N0USDT', row,
            dict(symbol='N0USDT', clientOrderId=row['client_order_id'], orderId=row['order_id'],
                 status='FILLED', updateTime=self.case.now*1000), row['qty'], 1.))
        self.trade._retire_order('N0USDT', row, 'FILLED')
        self.assertEqual(stats.snapshot()['trade_filled_entries'], 1)
        self.assertEqual(stats.snapshot()['unfilled_entries'], 0)

    def test_near_booked_fill_is_counted_after_order_retirement(self):
        self.assert_booked_fill_is_not_missing('1.01')

    def test_far_booked_fill_is_counted_after_order_retirement(self):
        self.assert_booked_fill_is_not_missing('1.018')


class BatchTieredTests(unittest.TestCase):
    setUp = TieredQuoteTests.setUp
    seed = TieredQuoteTests.seed

    def requests(self):
        far = self.seed(1.018)
        gate = self.trade.client.entry_gate
        near = self.seed(1.01, symbol='N1USDT')
        self.trade.client.entry_gate['admitted_entry_quotes'].update(gate['admitted_entry_quotes'])
        self.trade.client.entry_gate['entry_gap_limits'].update(gate['entry_gap_limits'])
        client = self.trade.client
        client.enabled, client.dry_run = True, False
        self.trade._account_deadline = self.case.now+100
        return [dict(symbol=symbol, client_order_id=cid, order_bar_open_ms=self.case.bar,
                     limit_price=1., notional=5.) for symbol, cid in [('N0USDT', far), ('N1USDT', near)]]

    def test_batch_rechecks_only_far_member_once_at_final_send(self):
        rows = self.requests()
        client = self.trade.client
        client._post_batch_orders = Mock(return_value=[dict(symbol=r['symbol'],
            clientOrderId=r['client_order_id'], orderId=str(i+10), status='NEW') for i, r in enumerate(rows)])
        with patch.object(self.trade.market, 'send_quote',
                          return_value=dict(tick=1.017, bid0=None, source='tick')) as read:
            result = client.place_limit_batch(rows)
        read.assert_called_once_with('N0USDT')
        self.assertTrue(all(result[row['client_order_id']][1] is None for row in rows))
        self.assertEqual(result[rows[0]['client_order_id']][0]['_entry_send_quote']['entry_send_check'], 'far_passed')
        self.assertEqual(result[rows[1]['client_order_id']][0]['_entry_send_quote']['entry_send_check'], 'near_skipped')

    def test_far_batch_reject_does_not_poison_near_singleton(self):
        rows = self.requests()
        client = self.trade.client
        client.place_limit_for_bar = ExchangeClient.place_limit_for_bar.__get__(client)
        sent = []
        def wire(method, symbol, side, qty, *, client_order_id, before_send, **kwargs):
            before_send()
            sent.append(client_order_id)
            return dict(symbol=symbol, clientOrderId=client_order_id, orderId='100', status='NEW')
        client._broker_call = wire
        with patch.object(self.trade.market, 'send_quote',
                          return_value=dict(tick=1.02, bid0=None, source='tick')) as read:
            result = client.place_limit_batch(rows)
        read.assert_called_once_with('N0USDT')
        self.assertIsInstance(result[rows[0]['client_order_id']][1], EntryQuoteUnavailable)
        self.assertEqual(sent, [rows[1]['client_order_id']])
        self.assertIsNone(result[rows[1]['client_order_id']][1])
