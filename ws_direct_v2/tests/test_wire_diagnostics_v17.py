"""Offline evidence for wake attribution and real request/WS failure correlation."""
import asyncio
import copy
import json
import time
import unittest
from unittest.mock import AsyncMock, Mock, patch

from tests import test_signal_follow_v2 as follows
from tests.test_trade_ws_keepalive import FakeSocket
from v9711_live.execution.client import ExchangeClient
from v9711_live.execution.entry_timing import EntryTimings, EntrySendProbe
from v9711_live.execution.request_activity import RequestActivity
from v9711_live.execution.submit_errors import submit_error_is_ambiguous, is_agreement_rejection
from v9711_live.execution.terminal import entry_timing_lines
from v9711_live.execution.ws_market import DualMarket
from v9711_live.exchange.binance_futures_trade_ws import BinanceFuturesTradeWS, APIError


class ActivityTests(unittest.TestCase):
    def test_windows_expire_on_monotonic_time_and_keep_lifetime_counts(self):
        stats = RequestActivity()
        with patch('time.monotonic', return_value=100.2):
            first = stats.begin('entry', 'XUSDT')
            stats.finish(first)
            stats.begin('cancel', 'YUSDT')
            stats.observe_loop_lag(.025)
            snap = stats.snapshot()
        self.assertEqual(snap['windows']['1']['entry'], 1)
        self.assertEqual(snap['windows']['10']['loop_lag_max_ms'], 25.)
        self.assertEqual(snap['inflight'], 1)
        with patch('time.monotonic', return_value=111.):
            self.assertNotIn('entry', stats.snapshot()['windows']['10'])
            self.assertEqual(stats.snapshot()['windows']['60']['entry'], 1)
        with patch('time.monotonic', return_value=161.):
            self.assertNotIn('entry', stats.snapshot()['windows']['60'])
            self.assertEqual(stats.snapshot()['totals']['entry'], 1)

    def test_batch_counts_one_wire_request_and_independent_order_members(self):
        stats = RequestActivity()
        token = stats.begin('entry', members=2, symbols=['XUSDT', 'YUSDT'], transport='REST_BATCH')
        stats.finish(token)
        stats.member_reject(-1015)
        row = stats.snapshot()['windows']['10']
        self.assertEqual(row['wire_requests'], 1)
        self.assertEqual(row['entry'], 2)
        self.assertEqual(row['member_rejected'], 1)
        self.assertEqual(row['rate_limited'], 1)
        self.assertEqual(row['top_symbols'], {'XUSDT': 1, 'YUSDT': 1})

    def test_worker_copies_share_one_activity_object(self):
        client = ExchangeClient(enabled=False)
        self.assertIs(copy.copy(client).request_activity, client.request_activity)

    def test_quote_log_runs_after_wire_call_instead_of_inside_guard(self):
        client = ExchangeClient(enabled=True, dry_run=False)
        client.prepare_limit = Mock(return_value=dict(ok=True, qty='5', price='1'))
        client._checked_entry_quote = Mock(return_value=dict(entry_qualified_by='tick'))
        client._check_entry_market = Mock()
        client._log_entry_quote = Mock()
        def send(method, *args, before_send, **kwargs):
            before_send()
            client._log_entry_quote.assert_not_called()
            return dict(orderId=1)
        client._broker_call = send
        client.place_limit(symbol='XUSDT', client_order_id='cid', limit_price=1., notional=5.)
        client._log_entry_quote.assert_called_once()

    def test_failure_snapshot_is_frozen_and_local_overflow_is_not_socket_disconnect(self):
        market = DualMarket(['XUSDT'], 'unused.json')
        stats = RequestActivity()
        market.book.request_activity = stats
        stats.begin('entry', 'XUSDT')
        stats.begin('cancel', 'XUSDT')
        stats.observe_loop_lag(.02)
        market.book.group_failed(1, 'ConnectionClosedError: no close frame received or sent')
        before = market.book.error_history[-1]['activity_before_failure']
        stats.begin('entry', 'XUSDT')
        self.assertEqual(before['entry'], 1)
        self.assertEqual(before['cancel'], 1)
        self.assertEqual(before['loop_lag_max_ms'], 20.)
        self.assertEqual(market.book.socket_failures, 1)
        market.book.group_failed(1, 'WS owner queue overflow; old generation revoked; socket kept open')
        self.assertEqual(market.book.socket_failures, 1)

    def test_own_quote_age_and_wake_latency_are_not_merged_or_faked(self):
        timings = EntryTimings()
        timings.register('XUSDT', 'cid', dict(limit_price=1., signal_follow=True,
            _entry_timing_context=dict(admission_price=1.02, trigger_symbol='ETHUSDT',
                trigger_source='tick', trigger_price=1675., _received_ns=1_000_000,
                _event_received_ns=146_000_000, _dequeued_ns=147_000_000, _decision_ns=152_000_000)))
        timings.mark_at('cid', 'send', 204_000_000, 204.)
        row = timings.order('cid')
        self.assertEqual(row['quote_symbol'], 'XUSDT')
        self.assertIsNone(row['trigger_gap'])
        self.assertEqual(row['latencies_ms']['quote_age_at_wake'], 145.)
        self.assertEqual(row['latencies_ms']['trigger_to_send'], 58.)
        self.assertEqual(row['latencies_ms']['quote_to_send'], 203.)
        panel = '\n'.join(entry_timing_lines(row))
        self.assertIn('币对=XUSDT 本币准入价=1.02 挂单价=1', panel)
        self.assertIn('准入价差=+2.0000%', panel)
        self.assertIn('唤醒行情→发送=58.000ms', panel)
        self.assertIn('依据报价→发送=203.000ms', panel)
        self.assertNotIn('触发价差=', panel)


    def test_latency_maxima_report_the_actual_orders_and_missing_wake(self):
        timings = EntryTimings()
        timings.register('OLDUSDT', 'older-quote', dict(_entry_timing_context=dict(
            _received_ns=1_000_000, _decision_ns=1_650_000_000)))
        timings.register('WAKEUSDT', 'slow-wake', dict(_entry_timing_context=dict(
            _received_ns=1_600_000_000, _event_received_ns=1_620_000_000,
            _dequeued_ns=1_680_000_000, _decision_ns=1_700_000_000)))
        timings.mark_at('older-quote', 'send', 1_677_000_000, 1677.)
        timings.mark_at('slow-wake', 'send', 1_761_000_000, 1761.)
        summary = timings.summary()
        self.assertEqual(summary['max_ms'], 1676.)
        self.assertEqual(summary['max_order']['symbol'], 'OLDUSDT')
        self.assertEqual(summary['max_order']['latencies_ms']['decision_to_send'], 27.)
        self.assertNotIn('trigger_to_send', summary['max_order']['latencies_ms'])
        self.assertEqual(summary['trigger']['max_order']['symbol'], 'WAKEUSDT')
        self.assertEqual(summary['trigger']['max_order']['latencies_ms']['queue_wait'], 60.)
        self.assertEqual(summary['trigger']['max_ms'], 141.)
        self.assertEqual(timings.summary(), summary)
        timings.mark_at('older-quote', 'persisted', 1_660_000_000, 1660.)
        self.assertIsNot(timings.summary(), summary)


class WireTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.broker = BinanceFuturesTradeWS('fake-key', 'fake-secret')
        self.addAsyncCleanup(self.broker.close)
        self.broker._ensure_connected = AsyncMock()
        self.broker._ws = FakeSocket()
        self.stats = self.broker.request_activity = RequestActivity()

    async def test_place_close_cancel_and_query_count_actual_wire_calls(self):
        for method, params in [('order.place', {'symbol': 'XUSDT', 'side': 'BUY'}),
                               ('order.place', {'symbol': 'XUSDT', 'side': 'SELL'}),
                               ('order.cancel', {'symbol': 'XUSDT'}), ('order.status', {'symbol': 'XUSDT'})]:
            await self.broker._request(method, params, signed=True)
        row = self.stats.snapshot()['windows']['10']
        for name in ('entry', 'close', 'cancel', 'query'):
            self.assertEqual(row[name], 1)
        self.assertEqual(row['wire_requests'], 4)
        self.assertEqual(row['replied'], 4)
        self.assertEqual(self.stats.snapshot()['inflight'], 0)

    async def test_guard_failure_is_not_counted_as_wire_request(self):
        def reject():
            raise ValueError('expired')
        with self.assertRaises(ValueError):
            await self.broker._request('order.place', {}, signed=True, before_send=reject)
        row = self.stats.snapshot()['windows']['10']
        self.assertEqual(row['local_rejected'], 1)
        self.assertNotIn('wire_requests', row)
        self.assertEqual(self.broker._ws.sent, [])

    async def test_explicit_api_rejection_counts_code_without_retaining_payload(self):
        async def response(identity):
            return dict(id=identity, status=400, error=dict(code=-1015, msg='Too many new orders'))
        self.broker._read_response = response
        with self.assertRaises(APIError):
            await self.broker._request('order.place', {'symbol': 'XUSDT', 'side': 'BUY'}, signed=True)
        row = self.stats.snapshot()['windows']['10']
        self.assertEqual(row['entry'], 1)
        self.assertEqual(row['failed'], 1)
        self.assertEqual(row['rate_limited'], 1)
        self.assertEqual(row['errors'], {'API:-1015': 1})
        self.assertNotIn('fake-secret', json.dumps(self.stats.snapshot()))

    async def test_send_failure_and_cancelled_request_release_inflight_counter(self):
        for error in (OSError('wire lost'), asyncio.CancelledError()):
            self.broker._ws = FakeSocket()
            self.broker._ws.send = AsyncMock(side_effect=error)
            with self.assertRaises(type(error)):
                await self.broker._request('order.cancel', {'symbol': 'XUSDT'}, signed=True)
            self.assertEqual(self.stats.snapshot()['inflight'], 0)
        self.assertEqual(self.stats.snapshot()['totals']['failed'], 2)

    async def test_malformed_ack_stays_an_error_and_does_not_leak_inflight(self):
        self.broker._read_response = AsyncMock(return_value=dict(status='malformed'))
        with self.assertRaises(ValueError):
            await self.broker._request('order.place', {'symbol': 'XUSDT', 'side': 'BUY'}, signed=True)
        self.assertEqual(self.stats.snapshot()['inflight'], 0)
        self.assertEqual(self.stats.snapshot()['totals']['failed'], 1)

    @patch('time.monotonic_ns', time.perf_counter_ns)
    async def test_socket_wait_is_visible_before_send(self):
        timings = EntryTimings()
        timings.register('XUSDT', 'cid', dict(limit_price=1.))
        probe = EntrySendProbe(lambda: None, timings, 'cid')
        await self.broker._lock.acquire()
        work = asyncio.create_task(self.broker._request('order.place', {}, signed=True, before_send=probe))
        await asyncio.sleep(.03)
        self.broker._lock.release()
        await work
        delays = timings.order('cid')['latencies_ms']
        self.assertGreaterEqual(delays['socket_wait'], 10.)
        self.assertIn('send_guard', delays)
        self.assertIn('sign_encode', delays)


class PermissionTests(unittest.TestCase):
    setUp = follows.SignalFollowTests.setUp
    make_trade = follows.SignalFollowTests.make_trade
    refresh_quotes = follows.SignalFollowTests.refresh_quotes
    target = follows.SignalFollowTests.target
    publish = follows.SignalFollowTests.publish
    event = follows.SignalFollowTests.event
    place = follows.SignalFollowTests.place
    cancel = follows.SignalFollowTests.cancel

    def test_agreement_refusal_does_not_repeat_follow_every_ten_seconds_or_restart(self):
        self.submit_error = RuntimeError('Please sign TradFi-Perps agreement contract fapi.')
        self.event()
        self.assertEqual(len(self.submits), 1)
        self.assertFalse(self.trade.tracked)
        self.now += 11
        self.publish()
        self.make_trade()
        self.event()
        self.assertEqual(len(self.submits), 1)
        self.targets[0]['position_id'] += ':new'
        self.submit_error = None
        self.publish()
        self.event()
        self.assertEqual(len(self.submits), 2)

    def test_transport_chain_stays_unknown_even_with_agreement_phrase(self):
        error = RuntimeError('Please sign TradFi-Perps agreement contract fapi.')
        error.__cause__ = OSError('connection lost')
        self.assertTrue(submit_error_is_ambiguous(error))
        self.assertFalse(is_agreement_rejection(error))

    def test_follow_cross_wake_keeps_own_symbol_price_and_limit(self):
        self.event('N0USDT')
        row = self.trade.tracked['AUSDT']
        timing = self.trade.session_entries.timings.order(row['client_order_id'])
        self.assertEqual(timing['trigger_symbol'], 'N0USDT')
        self.assertEqual(timing['quote_symbol'], 'AUSDT')
        self.assertEqual(timing['admission_price'], 1.2)
        self.assertEqual(timing['order_limit_price'], 1.)


if __name__ == '__main__':
    unittest.main()
