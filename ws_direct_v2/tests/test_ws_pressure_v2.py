"""Overload recovery keeps sockets open and never trades skipped generations."""
import asyncio
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from collectors import ticker_ws
from tests import test_book_ticker_v2 as fixtures
from v9711_live.execution.ws_market import DualMarket


class IngressPressureTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.market = DualMarket(['XUSDT'], Path(self.tmp.name)/'prices.json', queue_size=4)
        self.market.group_connected(1)
        self.market.book.group_connected(1)
        self.drain()

    def drain(self, callback=lambda event: None):
        while not self.market.events.empty():
            event = self.market.events.get_nowait()
            self.market.decide(event, callback)

    def bid(self, price, uid, quantity=2, stamp=None):
        return self.market.update_book('XUSDT', str(price), str(quantity), stamp or int(time.time()*1000), uid)

    def test_12000_quantity_only_updates_need_one_queue_slot(self):
        for uid in range(1, 12001):
            self.assertTrue(self.bid(1.01, uid, quantity=uid))
        self.assertEqual(self.market.events.qsize(), 1)
        self.assertEqual(self.market.book.counts['received'], 12000)
        self.assertEqual(self.market.book.counts['unchanged'], 11999)
        self.assertEqual(self.market.book.counts['overflow'], 0)

    def test_short_lived_threshold_crossing_and_return_are_all_queued(self):
        for uid,price in enumerate((1.03, 1.03, 1.01, 1.01, 1.04, 1.04), 1):
            self.bid(price, uid)
        observed = []
        self.drain(lambda event: observed.append(float(event.price)))
        self.assertEqual(observed, [1.03, 1.01, 1.04])

    def test_zero_quantity_revocation_is_not_deduplicated(self):
        self.bid(1.01, 1)
        self.drain()
        self.assertTrue(self.market.order_allowed('XUSDT'))
        self.bid(1.01, 2, quantity=0)
        self.drain()
        self.assertFalse(self.market.order_allowed('XUSDT'))
        self.bid(1.01, 3)
        self.drain()
        self.assertTrue(self.market.order_allowed('XUSDT'))

    def test_same_price_refreshes_timestamp_but_not_reconnect_generation(self):
        now = time.time()
        self.bid(1.01, 1)
        self.drain()
        with patch('time.time', return_value=now+65):
            self.assertFalse(self.market.order_allowed('XUSDT'))
            self.bid(1.01, 2, quantity=9, stamp=int((now+65)*1000))
            self.assertTrue(self.market.events.empty())
            self.assertTrue(self.market.order_allowed('XUSDT'))
            self.assertEqual(self.market.entry_quote('XUSDT')['bid_quantity'], 9)
            self.market.book.group_connected(1)
            self.drain()
            self.assertFalse(self.market.order_allowed('XUSDT'))
            self.bid(1.01, 3, stamp=int((now+65)*1000))
            self.assertFalse(self.market.events.empty())
            self.assertFalse(self.market.order_allowed('XUSDT'))

    def test_overflow_revokes_old_events_until_fresh_owner_decision(self):
        self.bid(1.01, 1)
        self.drain()
        self.market.update('XUSDT', '1.04', int(time.time()*1000))
        self.drain()
        old_generation = self.market.book.generation[1]
        for uid in range(2, 6):
            self.bid(1+uid/100, uid)
        with self.assertRaises(BufferError):
            self.bid(1.01, 6)
        self.assertEqual(self.market.book.generation[1], old_generation+1)
        self.assertIsNone(self.market.entry_quote('XUSDT')['bid0'])
        self.assertEqual(self.market.entry_quote('XUSDT')['tick'], 1.04)
        self.bid(.9, 7)
        self.assertEqual(self.market.book.counts['overload_discarded'], 2)
        self.assertIsNone(self.market.entry_quote('XUSDT')['bid0'])
        self.drain(lambda event: self.fail('revoked generation ran decision'))
        self.bid(1.01, 8)
        self.assertIsNone(self.market.entry_quote('XUSDT')['bid0'])
        self.drain()
        self.assertEqual(self.market.entry_quote('XUSDT')['bid0'], 1.01)

    def test_same_price_can_recover_after_stale_queue_rejection(self):
        self.bid(1.01, 1)
        event = self.market.events.get_nowait()
        with patch('time.monotonic_ns', return_value=event.received_ns+2_000_000_000):
            self.assertFalse(self.market.decide(event, lambda event: None))
        self.assertTrue(self.market.book.overloaded)
        self.bid(1.01, 2)
        self.assertFalse(self.market.events.empty())
        self.drain()
        self.assertTrue(self.market.order_allowed('XUSDT'))


class FarQuoteTests(unittest.TestCase):
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

    def test_far_quotes_skip_full_planner_but_entry_crossing_is_immediate(self):
        self.plans()
        self.tick(1.03)
        original = self.trade._execute_entries
        with patch.object(self.trade, '_execute_entries', wraps=original) as planner:
            for index in range(100):
                self.tick(1.03+(index % 2)*.001)
            planner.assert_not_called()
            self.assertEqual(self.trade._skipped_far_events, 100)
            self.tick(1.018)
            planner.assert_called_once()
        self.assertIn('N0USDT', self.trade.tracked)

    def test_far_incumbent_and_new_plan_still_run_full_decision(self):
        self.plans()
        self.tick(1.01)
        self.tick(1.03)
        self.assertEqual(len(self.cancels), 1)
        self.templates[0]['limit_price'] = 1.02
        self.publish()
        self.tick(1.03)
        self.assertEqual(len(self.submits), 2)


class ReceiverRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def consume(self, state, connect, stop):
        with patch.object(ticker_ws.websockets, 'connect', side_effect=connect):
            await ticker_ws.consume_group(['XUSDT'], state, base=ticker_ws.BOOK_WS_BASE,
                stream_kind='bookTicker', ping_interval=30, ping_timeout=20,
                read_timeout=30, reconnect_before=3600, stop=stop, group_index=1)

    def frame(self):
        return json.dumps(dict(e='bookTicker', s='XUSDT', b='1.01', B='2', u=7, T=int(time.time()*1000)))

    async def test_local_overflow_keeps_reading_same_socket(self):
        stop, state = asyncio.Event(), Mock()
        state.update_book.side_effect = [BufferError('local queue full'), True]
        frames, connections = [], []
        class Socket:
            async def recv(inner):
                frames.append(1)
                if len(frames) == 2:
                    stop.set()
                return self.frame()
        class Connection:
            async def __aenter__(self): return Socket()
            async def __aexit__(self, *args): pass
        def connect(*args, **kwargs):
            connections.append(1)
            return Connection()
        await self.consume(state, connect, stop)
        self.assertEqual(len(connections), 1)
        self.assertEqual(len(frames), 2)
        state.group_failed.assert_not_called()

    async def test_repeated_short_connections_use_exponential_backoff(self):
        await self.check_backoff(stable_second=False, expected=[1., 2., 4.])

    async def test_python310_timeout_fallback_still_receives(self):
        stop, state = asyncio.Event(), Mock()
        class Socket:
            async def recv(inner):
                stop.set()
                return self.frame()
        class Connection:
            async def __aenter__(self): return Socket()
            async def __aexit__(self, *args): pass
        with patch.dict(ticker_ws.asyncio.__dict__):
            ticker_ws.asyncio.__dict__.pop('timeout', None)
            await self.consume(state, lambda *args, **kwargs: Connection(), stop)
        state.update_book.assert_called_once()

    async def test_buffered_socket_yields_to_other_connections_and_pings(self):
        stop, state = asyncio.Event(), Mock()
        frames, witnessed = [0], []
        class Socket:
            async def recv(inner):
                frames[0] += 1
                if frames[0] == 128:
                    stop.set()
                return self.frame()
        class Connection:
            async def __aenter__(self): return Socket()
            async def __aexit__(self, *args): pass
        async def witness():
            witnessed.append((frames[0], state.update_book.call_count))
        task = asyncio.create_task(witness())
        await self.consume(state, lambda *args, **kwargs: Connection(), stop)
        await task
        self.assertEqual(len(witnessed), 1)
        read, processed = witnessed[0]
        self.assertLess(read, 128)
        self.assertEqual(read, processed)
        self.assertEqual(state.update_book.call_count, 128)

    async def test_stable_connection_resets_backoff(self):
        await self.check_backoff(stable_second=True, expected=[1., 1., 2.])

    async def check_backoff(self, *, stable_second, expected):
        stop, state = asyncio.Event(), Mock()
        delays, connections, clock = [], [], [100.]
        class Socket:
            sent = False
            async def recv(inner):
                if not inner.sent:
                    inner.sent = True
                    return self.frame()
                if stable_second and len(connections) == 2:
                    clock[0] += 31
                raise ConnectionResetError('peer reset')
        class Connection:
            async def __aenter__(self): return Socket()
            async def __aexit__(self, *args): pass
        def connect(*args, **kwargs):
            connections.append(1)
            return Connection()
        async def backoff(seconds):
            if seconds:
                delays.append(seconds)
                if len(delays) == 3:
                    stop.set()
        with patch.object(ticker_ws.asyncio, 'sleep', side_effect=backoff), \
                patch.object(ticker_ws.time, 'monotonic', side_effect=lambda: clock[0]):
            await self.consume(state, connect, stop)
        self.assertEqual(delays, expected)
