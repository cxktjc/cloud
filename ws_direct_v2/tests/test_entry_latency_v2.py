"""Offline latency attribution, exact first-send matching and worker warmup."""
import asyncio
import threading
import unittest
from unittest.mock import AsyncMock, Mock, patch

from tests.test_trade_ws_keepalive import FakeSocket
from tests import test_book_ticker_v2 as fixtures
from v9711_live import config as cfg
from v9711_live.exchange.binance_futures_trade_ws import BinanceFuturesTradeWS
from v9711_live.execution.entry_timing import EntrySendProbe, EntryTimings
from v9711_live.execution.nonblocking_io import NetworkIO
from v9711_live.execution.session_entries import SessionEntryStats
from v9711_live.execution.terminal import entry_timing_lines


class EntryLatencyTests(unittest.TestCase):
    def setUp(self):
        self.bar = 1800000
        self.now = self.bar/1000+39
        self.nano = 1_000_000_000
        for target, clock in (('time.time', lambda: self.now), ('time.monotonic_ns', lambda: self.nano)):
            p = patch(target, side_effect=clock)
            p.start()
            self.addCleanup(p.stop)
        self.stats = SessionEntryStats(started_at=self.bar/1000)
        self.timing = self.stats.timings
        self.target = dict(symbol='THEUSDT', quantity=1, position_id='book:THE',
                           entry_bar_open_ms=self.bar, entry_available_ms=self.bar+cfg.BAR_MS)
        self.entry = dict(self.target, signal_session_id='book')

    def register(self, cid='first', session='book', bar=None, position_id=''):
        self.timing.register('THEUSDT', cid, dict(order_bar_open_ms=bar or self.bar,
            position_id=position_id, _entry_timing_context=dict(quote_source='tick',
            received_epoch_ms=self.now*1000, exchange_time_ms=self.now*1000-8,
            _received_ns=self.nano, decision_epoch_ms=self.now*1000, _decision_ns=self.nano)),
            session_id=session)

    def advance(self, ms):
        self.now += ms/1000
        self.nano += int(ms*1e6)

    def observe_signal(self):
        self.stats.observe_signal(dict(signal_ready=True, signal_session_id='book', portfolio={},
            target_positions=[self.target]), status='valid', now=self.bar/1000+181)
        return self.stats.snapshot()['recent_unfilled_entries'][0]

    def test_first_send_39_and_reply_41_are_separate_millisecond_stamps(self):
        self.register()
        for stage, ms in (('registered', 4), ('worker', 1), ('send', 10), ('response', 2000), ('owner_ack', 3)):
            self.advance(ms)
            self.timing.mark('first', stage)
        timing = self.observe_signal()['first_order_timing']
        self.assertAlmostEqual(timing['send_epoch_ms'], self.bar+39015)
        self.assertAlmostEqual(timing['response_epoch_ms'], self.bar+41015)
        self.assertEqual(timing['latencies_ms']['send_to_response'], 2000)
        self.assertEqual(timing['latencies_ms']['response_to_owner'], 3)
        panel = '\n'.join(entry_timing_lines(timing))
        self.assertIn('首次下单=', panel)
        self.assertIn('收到行情=', panel)
        self.assertIn('.015+00:00', panel)
        self.assertEqual(self.stats.snapshot()['trade_filled_entries'], 0)  # ACK is not a fill.

    def test_stats_cache_updates_after_worker_stage_without_new_signal(self):
        self.register()
        self.timing.mark('first', 'registered')
        before = self.observe_signal()['first_order_timing']
        self.assertNotIn('send_epoch_ms', before)
        self.advance(10)
        self.timing.mark('first', 'send')
        after = self.stats.snapshot()['recent_unfilled_entries'][0]['first_order_timing']
        self.assertIn('send_epoch_ms', after)

    def test_retry_and_follow_do_not_replace_the_first_actual_send(self):
        self.register('rejected')  # Local pre-send rejection must not become first send.
        self.advance(5)
        self.register('first')
        self.timing.mark('first', 'send')
        first = self.timing.first_for(self.entry)
        self.advance(1000)
        self.register('follow', position_id='book:THE')
        self.timing.mark('follow', 'send')
        self.timing.mark('first', 'send')  # Duplicate hook cannot overwrite first send.
        self.assertEqual(self.timing.first_for(self.entry), first)

    def test_other_session_bar_or_exact_position_cannot_supply_times(self):
        self.register('other-session', session='old')
        self.register('other-bar', bar=self.bar-cfg.BAR_MS)
        self.register('other-position', position_id='book:OTHER')
        for cid in ('other-session', 'other-bar', 'other-position'):
            self.timing.mark(cid, 'send')
        self.assertEqual(self.timing.first_for(self.entry), {})
        self.assertIn('首次下单=未记录', entry_timing_lines({})[0])

    def test_elapsed_durations_use_monotonic_clock_when_wall_clock_changes(self):
        self.register()
        self.timing.mark('first', 'send')
        self.now -= 3600
        self.nano += 25_000_000
        self.timing.mark('first', 'response')
        self.assertEqual(self.timing.order('first')['latencies_ms']['send_to_response'], 25)


class WireProbeTests(unittest.IsolatedAsyncioTestCase):
    async def test_wire_hooks_run_after_connect_and_guard_and_before_socket_send(self):
        timings = EntryTimings()
        timings.register('THEUSDT', 'cid', dict(order_bar_open_ms=180000))
        broker = BinanceFuturesTradeWS('fake-key', 'fake-secret')
        self.addAsyncCleanup(broker.close)
        stages = []
        class Socket(FakeSocket):
            async def send(inner, wire):
                self.assertIn('send_epoch_ms', timings.order('cid'))
                self.assertNotIn('response_epoch_ms', timings.order('cid'))
                stages.append('send')
                await super().send(wire)
        broker._ws = Socket()
        async def connected():
            stages.append('connected')
        broker._ensure_connected = AsyncMock(side_effect=connected)
        def guard():
            stages.append('guard')
            self.assertNotIn('send_epoch_ms', timings.order('cid'))
        result = await broker._request('order.place', {}, signed=True,
                                      before_send=EntrySendProbe(guard, timings, 'cid'))
        self.assertEqual(stages, ['connected', 'guard', 'send'])
        self.assertEqual(result['orderId'], 123)
        self.assertEqual(timings.order('cid')['transport'], 'WS')
        self.assertIn('response_epoch_ms', timings.order('cid'))

    async def test_failed_guard_does_not_record_a_send(self):
        timings = EntryTimings()
        timings.register('THEUSDT', 'cid', dict(order_bar_open_ms=180000))
        broker = BinanceFuturesTradeWS('fake-key', 'fake-secret')
        self.addAsyncCleanup(broker.close)
        broker._ws = FakeSocket()
        broker._ensure_connected = AsyncMock()
        def reject():
            raise ValueError('expired')
        with self.assertRaisesRegex(ValueError, 'expired'):
            await broker._request('order.place', {}, signed=True,
                                  before_send=EntrySendProbe(reject, timings, 'cid'))
        self.assertEqual(broker._ws.sent, [])
        self.assertNotIn('send_epoch_ms', timings.order('cid'))


class WorkerLatencyTests(unittest.TestCase):
    def make_io(self, **kwargs):
        io = NetworkIO(**kwargs)
        self.addCleanup(io.shutdown)
        return io

    def test_all_worker_lanes_warm_before_work_and_reuse_their_threads(self):
        lock, warmed = threading.Lock(), threading.Event()
        identities = set()
        def warm():
            with lock:
                identities.add(threading.get_ident())
                if len(identities) == 2:
                    warmed.set()
        io = self.make_io(workers=2, worker_start=warm)
        self.assertTrue(warmed.wait(2))
        worked = threading.Event()
        def work():
            self.assertIn(threading.get_ident(), identities)
            worked.set()
        io.submit(('entry', 'THEUSDT', 'cid'), work, priority=10)
        self.assertTrue(worked.wait(2))

    def test_failed_warmup_does_not_lose_work_or_mutate_owner_state(self):
        wake = threading.Event()
        with self.assertLogs('v9711_live_trade.io', level='WARNING'):
            io = self.make_io(workers=1, worker_start=Mock(side_effect=ConnectionError('offline')),
                              owner_wake=wake.set)
            results = []
            io.submit(('entry', 'THEUSDT', 'cid'), lambda: 7, priority=10,
                      done=lambda value, error: results.append((value, error)))
            self.assertTrue(wake.wait(2))
        self.assertEqual(results, [])  # Worker only wakes, owner applies result.
        self.assertEqual(io.drain(), 1)
        self.assertEqual(results, [(7, None)])
        self.assertEqual(io.health()['transport_warmup']['failed'], 1)

    def test_callback_failure_remains_retryable_and_idle_drains_do_not_reapply(self):
        wake = threading.Event()
        io = self.make_io(workers=1, owner_wake=wake.set)
        calls = []
        def done(value, error):
            calls.append(value)
            if len(calls) == 1:
                raise OSError('save failed')
        io.submit(('entry', 'THEUSDT', 'cid'), lambda: 3, priority=10, done=done)
        self.assertTrue(wake.wait(2))
        with self.assertRaisesRegex(OSError, 'save failed'):
            io.drain()
        self.assertEqual(io.drain(), 1)
        for _ in range(100):
            self.assertEqual(io.drain(), 0)
        self.assertEqual(calls, [3, 3])

    def test_completion_arriving_during_an_owner_callback_is_not_lost(self):
        wake, release_b = threading.Event(), threading.Event()
        self.addCleanup(release_b.set)
        io = self.make_io(workers=2, owner_wake=wake.set)
        results = []
        def b():
            release_b.wait(2)
            return 'b'
        def done_a(value, error):
            results.append(value)
            wake.clear()
            release_b.set()
            self.assertTrue(wake.wait(2))
        io.submit(('entry', 'A', 'a'), lambda: 'a', priority=10, done=done_a)
        io.submit(('entry', 'B', 'b'), b, priority=10, done=lambda value, error: results.append(value))
        self.assertTrue(wake.wait(2))
        self.assertEqual(io.drain(), 1)
        self.assertEqual(io.drain(), 1)
        self.assertEqual(results, ['a', 'b'])

    def test_new_query_is_pruned_even_when_valid_identities_did_not_change(self):
        io = self.make_io(workers=1)
        io.prune_queries(set())
        key = ('query', 'get_order', 'THEUSDT', 'old')
        io.submit(key, lambda: None, priority=20)
        self.assertIn(key, io._query_keys)
        io.prune_queries(set())
        self.assertNotIn(key, io._jobs)
        self.assertFalse(io._query_keys)


class BatchLatencyTests(unittest.TestCase):
    setUp = fixtures.DualAdmissionTests.setUp
    target = fixtures.DualAdmissionTests.target
    make_trade = fixtures.DualAdmissionTests.make_trade
    refresh_quotes = fixtures.DualAdmissionTests.refresh_quotes
    place = fixtures.DualAdmissionTests.place
    cancel = fixtures.DualAdmissionTests.cancel
    publish = fixtures.DualAdmissionTests.publish
    ordinary_plan = fixtures.DualAdmissionTests.ordinary_plan
    plans = fixtures.DualAdmissionTests.plans
    tick = fixtures.DualAdmissionTests.tick

    def test_batch_marks_each_exact_members_own_quote_and_request_times(self):
        fixtures.DualAdmissionTests.test_batch_send_retains_each_members_own_qualification(self)
        for symbol, source in (('N0USDT', 'tick'), ('N1USDT', 'depth')):
            cid = self.trade.tracked[symbol]['client_order_id']
            timing = self.trade.session_entries.timings.order(cid)
            self.assertEqual(timing['quote_source'], source)
            self.assertEqual(timing['transport'], 'REST_BATCH')
            self.assertIn('send_epoch_ms', timing)
            self.assertIn('response_epoch_ms', timing)
            self.assertIn('received_epoch_ms', timing)


if __name__ == '__main__':
    unittest.main()
