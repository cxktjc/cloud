"""Full-book owner/send guards and pending-cancel visibility, offline only."""
import unittest
from copy import deepcopy
from unittest.mock import Mock

from tests import test_signal_follow_v2 as fixtures
from v9711_live import config as cfg
from v9711_live.execution.client import ExchangeClient
from v9711_live.execution.nonblocking_io import NetworkIO
from v9711_live.execution.submit_errors import ConfirmedSubmitRejection, EntrySendGateRejection


class CapacityGateTests(unittest.TestCase):
    setUp = fixtures.SignalFollowTests.setUp
    target = fixtures.SignalFollowTests.target
    make_trade = fixtures.SignalFollowTests.make_trade
    refresh_quotes = fixtures.SignalFollowTests.refresh_quotes
    place = fixtures.SignalFollowTests.place
    cancel = fixtures.SignalFollowTests.cancel
    publish = fixtures.SignalFollowTests.publish
    event = fixtures.SignalFollowTests.event
    held = fixtures.SignalFollowTests.held

    def ordinary_plan(self):
        self.targets = []
        self.templates = [dict(symbol=f'N{i}USDT', limit_price=1., notional=5.,
            order_bar_open_ms=self.bar, reference_bar_open_ms=self.bar-cfg.BAR_MS,
            exit_due_epoch=self.now+900) for i in range(8)]
        self.publish()

    def worker_gate(self):
        io = object.__new__(NetworkIO)
        io._entry_gate, io._closed = self.trade.client.entry_gate, False
        return io

    def test_worker_gate_distinguishes_bar_end_plan_rollover_and_sizing_change(self):
        self.ordinary_plan()
        self.event('N0USDT')
        io = self.worker_gate()
        cid = self.trade.tracked['N0USDT']['client_order_id']
        gate = io._entry_gate
        self.assertFalse(io.check_entry('N0USDT', cid, self.bar))
        for now, update, reason in (
            ((self.bar + cfg.BAR_MS)/1000, {'bar_ms': self.bar+cfg.BAR_MS}, 'entry_bar_ended_at_send'),
            (self.now, {'bar_ms': self.bar+cfg.BAR_MS}, 'entry_plan_bar_changed'),
            (self.now, {'held': cfg.SLOTS}, 'entry_position_cap_at_send'),
        ):
            with self.subTest(reason=reason):
                prior, self.now = self.now, now
                try:
                    io._entry_gate = dict(gate, **update)
                    with self.assertRaises(EntrySendGateRejection) as rejection:
                        io.check_entry('N0USDT', cid, self.bar)
                    self.assertEqual(rejection.exception.reason, reason)
                finally:
                    self.now = prior
        self.trade.tracked['N0USDT']['sizing_weight'] = cfg.SLOT_WEIGHTS[1]
        self.trade._publish_entry_gate()
        with self.assertRaises(EntrySendGateRejection) as rejection:
            self.worker_gate().check_entry('N0USDT', cid, self.bar)
        self.assertEqual(rejection.exception.reason, 'entry_sizing_changed_at_send')

    def test_three_positions_outside_final_bar_block_all_new_ordinary_orders(self):
        self.held(3)
        self.ordinary_plan()
        self.event('N0USDT')
        self.assertFalse(self.trade.tracked)
        self.assertFalse(self.submits)
        self.assertIn('position_capacity_full', self.trade._entry_diagnostics['blockers'])

    def test_third_fill_immediately_blocks_queued_single_and_batch_sends(self):
        self.held(2)
        self.ordinary_plan()
        self.event('N0USDT')
        self.assertEqual(len(self.submits), 8)
        first = self.trade.tracked['N0USDT']
        self.assertTrue(self.trade._register_fill('N0USDT', first, {}, first['qty'], 1.))
        self.assertEqual(len(self.trade.positions), 3)
        remaining = deepcopy(self.trade.tracked['N1USDT'])
        io = self.worker_gate()
        with self.assertRaises(ConfirmedSubmitRejection):
            io.check_entry('N1USDT', remaining['client_order_id'], self.bar)
        self.trade.client.enabled, self.trade.client.dry_run = True, False
        self.trade._account_deadline = self.now+100
        self.trade.client.network_io = io
        sent = []
        def dispatch(*args, before_send=None, **kwargs):
            before_send()
            sent.append(kwargs)
        self.trade.client._broker_call = Mock(side_effect=dispatch)
        request = dict(symbol='N1USDT', client_order_id=remaining['client_order_id'],
                       limit_price=1., notional=5., order_bar_open_ms=self.bar)
        with self.assertRaises(ConfirmedSubmitRejection):
            ExchangeClient.place_limit_for_bar(self.trade.client, **request)
        self.assertFalse(sent)
        self.trade.client._post_batch_orders = Mock(side_effect=AssertionError('must not send batch'))
        other = dict(request, symbol='N2USDT', client_order_id=self.trade.tracked['N2USDT']['client_order_id'])
        result = self.trade.client.place_limit_batch([request, other])
        self.assertTrue(all(isinstance(error, ConfirmedSubmitRejection) for _, error in result.values()))
        self.trade.client._post_batch_orders.assert_not_called()

    def test_full_capacity_requests_cancel_but_retains_unknown_order_until_confirmed(self):
        self.held(2)
        self.ordinary_plan()
        self.event('N0USDT')
        self.held(3)
        self.cancel_error = TimeoutError('cancel response lost')
        self.event('N0USDT')
        self.assertEqual(len(self.trade.tracked), 8)
        self.assertTrue(all(row['cancel_requested'] for row in self.trade.tracked.values()))
        self.assertTrue(all(row['cancel_reason'] == 'position_capacity_full' for row in self.trade.tracked.values()))
        self.assertEqual(len(self.submits), 8)
        self.event('N0USDT')
        self.assertEqual(len(self.submits), 8)

    def test_final_window_boundaries_allow_then_stop_ordinary_entries(self):
        self.held(3)
        self.ordinary_plan()
        self.trade.positions['H0']['exit_due_epoch'] = self.now+180.001
        self.event('N0USDT')
        self.assertFalse(self.trade.tracked)
        self.trade.positions['H0']['exit_due_epoch'] = self.now+180
        self.event('N0USDT')
        self.assertEqual(self.trade._ordinary_pending_count(), 8)
        self.assertTrue(all(row['sizing_weight'] == .2 for row in self.trade.tracked.values()))
        self.trade.positions['H0']['exit_due_epoch'] = self.now
        self.event('N0USDT')
        self.assertFalse(self.trade.tracked)
        self.assertEqual(len(self.submits), 8)

    def test_full_capacity_preserves_two_follow_orders_and_cancels_ordinary_only(self):
        self.held(2)
        self.ordinary_plan()
        self.targets = [self.target(s) for s in ('AUSDT', 'BUSDT')]
        self.publish()
        self.event('N0USDT')
        self.assertEqual(len(self.trade.tracked), 10)
        self.held(3)
        self.event('N0USDT')
        self.assertEqual(set(self.trade.tracked), {'AUSDT', 'BUSDT'})
        self.assertTrue(all(row['signal_follow'] for row in self.trade.tracked.values()))
        self.assertEqual(len(self.submits), 10)

    def test_owner_submission_rechecks_capacity_even_if_caller_requests_entry(self):
        self.held(3)
        self.ordinary_plan()
        self.trade._place_order(self.templates[0])
        self.assertFalse(self.trade.tracked)
        self.assertFalse(self.submits)
        self.assertEqual(self.trade._submit_skip_reasons['N0USDT'], 'position_or_pending_capacity_full')

    def test_price_event_cancels_far_ordinary_entry_but_preserves_signal_follow(self):
        self.ordinary_plan()
        self.targets = [self.target('AUSDT')]
        self.publish()
        self.event('N0USDT')
        self.assertEqual(len(self.trade.tracked), 9)
        self.assertTrue(self.trade.tracked['AUSDT']['signal_follow'])
        self.trade.market.update('N0USDT', '1.021001', int(self.now*1000))
        self.trade.market.decide(self.trade.market.events.get_nowait(), self.trade.on_tick)
        self.assertNotIn('N0USDT', self.trade.tracked)
        self.assertIn('AUSDT', self.trade.tracked)
        self.assertEqual([r['symbol'] for r in self.cancels], ['N0USDT'])
        self.assertEqual(len(self.submits), 9)

    def test_full_ordinary_book_replaces_above_discovery_gate_with_follow_excluded(self):
        self.ordinary_plan()
        self.templates.append(dict(self.templates[0], symbol='N8USDT'))
        self.targets = [self.target('AUSDT')]
        self.publish()
        self.event('N0USDT')
        self.assertEqual(self.trade._ordinary_pending_count(), 8)
        self.assertEqual(len(self.trade.tracked), 9)
        def quote(symbol, price):
            self.trade.market.update(symbol, str(price), int(self.now*1000))
            self.trade.market.decide(self.trade.market.events.get_nowait(), self.trade.on_tick)
        quote('N8USDT', 1.0188)
        self.assertFalse(self.cancels)
        quote('N7USDT', 1.021)
        self.assertEqual([r['symbol'] for r in self.cancels], ['N7USDT'])
        self.assertEqual(self.trade.tracked['N8USDT']['rotation_origin']['original_order_count'], 8)
        self.assertIn('N8USDT', self.trade.tracked)
        quote('N8USDT', 1.0188)
        self.assertIn('N8USDT', self.trade.tracked)
        self.assertAlmostEqual(self.trade.tracked['N8USDT']['gap'], .0188)
        self.assertIn('AUSDT', self.trade.tracked)
        self.assertIsNone(self.trade.ledger.state['pending_rotation'])
        self.assertEqual(self.trade._ordinary_pending_count(), 8)

    def test_four_ordinary_plus_two_follow_orders_still_use_sparse_margin(self):
        self.ordinary_plan()
        self.templates = self.templates[:4]
        self.targets = [self.target(s) for s in ('AUSDT', 'BUSDT')]
        self.publish()
        self.event('N0USDT')
        self.assertEqual(len(self.trade.tracked), 6)
        self.trade.market.update('N3USDT', '1.020', int(self.now*1000))
        self.trade.market.decide(self.trade.market.events.get_nowait(), self.trade.on_tick)
        self.templates.append(dict(self.templates[0], symbol='N8USDT'))
        self.publish()
        self.trade.market.update('N8USDT', '1.0178', int(self.now*1000))
        self.trade.market.decide(self.trade.market.events.get_nowait(), self.trade.on_tick)
        # 0.22pp is insufficient at FOUR ordinary orders: fill the free fifth
        # slot instead. Counting two follow orders would incorrectly rotate.
        self.assertFalse(self.cancels)
        self.assertIn('N3USDT', self.trade.tracked)
        self.assertIn('N8USDT', self.trade.tracked)
        self.assertEqual(self.trade._ordinary_pending_count(), 5)

    def test_worker_final_window_permission_expires_without_another_owner_update(self):
        self.held(3)
        self.ordinary_plan()
        self.trade.positions['H0']['exit_due_epoch'] = self.now+1
        self.event('N0USDT')
        row = self.trade.tracked['N0USDT']
        io = self.worker_gate()
        self.assertFalse(io.check_entry('N0USDT', row['client_order_id'], self.bar))
        self.now += 1
        with self.assertRaises(ConfirmedSubmitRejection):
            io.check_entry('N0USDT', row['client_order_id'], self.bar)

    def test_eight_real_queued_cancels_need_10p5_seconds_of_pacing(self):
        self.held(2)
        self.ordinary_plan()
        self.event('N0USDT')
        self.cancel_mono = 0.
        io = NetworkIO(workers=2, cancel_clock=lambda: self.cancel_mono)
        self.trade._network_io = self.trade.client.network_io = io
        self.addCleanup(self.trade.shutdown_io)
        self.held(3)
        self.event('N0USDT')
        self.assertEqual(len(self.trade.tracked), 8)
        self.assertTrue(all(row['cancel_requested'] for row in self.trade.tracked.values()))
        for index in range(8):
            self.cancel_mono = index*1.5
            with io._condition:
                io._condition.notify_all()
                self.assertTrue(io._condition.wait_for(lambda: any(
                    job.entry_cancel and job.state == 'done' for job in io._jobs.values()), timeout=2))
            self.trade._drain_network()
            self.assertEqual(len(self.trade.tracked), 7-index)
            self.assertEqual(len(self.submits), 8)
            if index == 6:
                self.assertEqual(self.cancel_mono, 9.)
                self.assertEqual(len(self.trade.tracked), 1)
        self.assertEqual(self.cancel_mono, 10.5)
        self.assertEqual(len(self.cancels), 8)
