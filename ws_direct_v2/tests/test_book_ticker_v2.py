"""Offline tick/bid0 OR admission, independent feed health and exact sends."""
import asyncio
import json
import random
import tempfile
import time
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from unittest.mock import Mock, patch

from collectors import ticker_ws
from tests import test_capacity_gate_v2 as fixtures
from v9711_live import config as cfg
from v9711_live.execution.client import ExchangeClient
from v9711_live.execution.logic import select_physical_orders
from v9711_live.execution.submit_errors import EntryQuoteUnavailable
from v9711_live.execution.ws_market import DualMarket


class DualMarketTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.market = DualMarket(['XUSDT'], Path(self.tmp.name)/'prices.json')
        for feed in (self.market, self.market.book):
            feed.group_connected(1)
        self.drain()

    def drain(self, callback=lambda event: None):
        while not self.market.events.empty():
            self.market.decide(self.market.events.get_nowait(), callback)

    def quote(self, tick=1.03, bid=1.01, quantity=2, update_id=1):
        stamp = int(time.time()*1000)
        if tick is not None:
            self.market.update('XUSDT', str(tick), stamp)
        if bid is not None:
            self.market.update_book('XUSDT', str(bid), str(quantity), stamp, update_id)
        self.drain()

    def test_minimum_entry_quote_and_tick_only_mirror_and_valuation(self):
        self.quote()
        self.assertEqual(self.market.entry_quote('XUSDT')['source'], 'bid0')
        self.assertEqual(self.market.entry_prices(), {'XUSDT': 1.01})
        self.assertEqual(self.market.prices(), {'XUSDT': 1.03})
        self.assertEqual(float(self.market.snapshot()['prices']['XUSDT']['price']), 1.03)
        self.assertEqual(self.market.entry_quote('XUSDT')['bid_quantity'], 2)

    def test_single_feed_disconnect_does_not_block_the_other(self):
        self.quote()
        self.market.group_failed(1, 'tick disconnected')
        self.assertTrue(self.market.order_allowed('XUSDT'))
        self.assertEqual(self.market.entry_prices(), {'XUSDT': 1.01})
        self.assertEqual(self.market.prices(), {})
        self.assertEqual(self.market.snapshot()['status'], 'error')
        self.market.book.group_failed(1, 'book disconnected')
        self.assertFalse(self.market.order_allowed('XUSDT'))
        self.assertEqual(self.market.entry_prices(), {})

    def test_bid_only_startup_does_not_wait_for_tick(self):
        self.quote(tick=None)
        self.assertTrue(self.market.healthy())
        self.assertEqual(self.market.entry_quote('XUSDT')['source'], 'bid0')

    def test_zero_quantity_invalidates_bid_and_duplicate_update_is_rejected(self):
        self.quote()
        self.assertFalse(self.market.update_book('XUSDT', '0.5', '2', int(time.time()*1000), 1))
        self.quote(tick=None, bid=1, quantity=0, update_id=2)
        self.assertEqual(self.market.entry_quote('XUSDT')['source'], 'tick')
        self.assertIsNone(self.market.entry_quote('XUSDT')['bid0'])

    def test_book_reconnect_rejects_queued_old_generation(self):
        self.quote()
        self.market.update_book('XUSDT', '0.9', '2', int(time.time()*1000), 2)
        stale = self.market.events.get_nowait()
        self.market.book.group_connected(1)
        self.assertFalse(self.market.decide(stale, lambda event: self.fail('old book event used')))
        self.assertEqual(self.market.entry_quote('XUSDT')['source'], 'tick')

    def test_per_symbol_staleness_falls_back_then_expires_both(self):
        self.quote()
        with patch('time.time', return_value=time.time()+cfg.PRICE_EXCHANGE_MAX_AGE_SECONDS+6):
            self.assertFalse(self.market.order_allowed('XUSDT'))
            self.assertEqual(self.market.entry_prices(), {})

    def test_book_mirror_stays_tick_only_during_decision(self):
        self.quote(tick=1.03, bid=None)
        self.market.update_book('XUSDT', '1.01', '2', int(time.time()*1000), 1)
        def decision(event):
            self.assertEqual(self.market.entry_prices()['XUSDT'], 1.01)
            self.assertEqual(float(self.market.snapshot()['prices']['XUSDT']['price']), 1.03)
        self.drain(decision)


class DualAdmissionTests(unittest.TestCase):
    setUp = fixtures.CapacityGateTests.setUp
    target = fixtures.CapacityGateTests.target
    make_trade = fixtures.CapacityGateTests.make_trade
    refresh_quotes = fixtures.CapacityGateTests.refresh_quotes
    place = fixtures.CapacityGateTests.place
    cancel = fixtures.CapacityGateTests.cancel
    publish = fixtures.CapacityGateTests.publish
    event = fixtures.CapacityGateTests.event
    held = fixtures.CapacityGateTests.held
    ordinary_plan = fixtures.CapacityGateTests.ordinary_plan

    def plans(self, count=1):
        self.ordinary_plan()
        self.templates = self.templates[:count]
        self.publish()

    def tick(self, price, symbol='N0USDT'):
        self.trade.market.update(symbol, str(price), int(self.now*1000))
        self.trade.market.decide(self.trade.market.events.get_nowait(), self.trade.on_tick)

    def bid(self, price, symbol='N0USDT', update_id=1):
        self.trade.market.update_book(symbol, str(price), '10', int(self.now*1000), update_id)
        self.trade.market.decide(self.trade.market.events.get_nowait(), self.trade.on_tick)

    def test_bid_only_qualifies_and_two_events_do_not_duplicate(self):
        self.plans()
        self.tick(1.03)
        self.assertFalse(self.submits)
        self.bid(1.0185)
        self.assertEqual(len(self.submits), 1)
        self.assertEqual(self.trade.tracked['N0USDT']['entry_quote_source'], 'bid0')
        self.tick(1.03)
        self.bid(1.018, update_id=2)
        self.assertEqual(len(self.submits), 1)
        self.assertFalse(self.cancels)
        self.assertEqual(self.trade.refresh_prices()['N0USDT'], 1.03)

    def test_tick_only_qualifies(self):
        self.plans()
        self.tick(1.04)
        self.bid(1.03)
        self.assertFalse(self.submits)
        self.tick(1.01)
        self.assertEqual(len(self.submits), 1)
        self.assertEqual(self.trade.tracked['N0USDT']['entry_quote_source'], 'tick')

    def test_distance_cancel_needs_all_valid_quotes_to_be_outside(self):
        self.plans()
        self.tick(1.01)
        self.bid(1.02)
        self.tick(1.03)
        self.assertFalse(self.cancels)
        self.bid(1.022, update_id=2)
        self.assertEqual(len(self.cancels), 1)

    def test_both_feeds_missing_do_not_trigger_distance_cancel(self):
        self.plans()
        self.tick(1.01)
        self.trade.market.group_failed(1, 'tick down')
        self.trade.step(now=self.now)
        self.assertIn('N0USDT', self.trade.tracked)
        self.assertFalse(self.cancels)

    def test_full_book_replacement_uses_both_sources_for_old_and_new(self):
        self.plans(8)
        self.event('N0USDT')
        self.templates.append(dict(self.templates[0], symbol='N8USDT'))
        self.publish()
        self.tick(1.03, symbol='N8USDT')
        self.tick(1.021, symbol='N7USDT')
        self.bid(1.0205, symbol='N7USDT')
        self.bid(1.0188, symbol='N8USDT')
        self.assertFalse(self.cancels)  # old's bid is closer: advantage <0.22pp
        self.bid(1.021, symbol='N7USDT', update_id=2)
        self.assertEqual([r['symbol'] for r in self.cancels], ['N7USDT'])
        self.assertNotIn('N8USDT', self.trade.tracked)
        self.bid(1.0188, symbol='N8USDT', update_id=2)
        row = self.trade.tracked['N8USDT']
        self.assertAlmostEqual(row['entry_gap_ceiling'], .0188)
        self.assertTrue(self.trade._entry_quote_send_allowed('N8USDT', row['client_order_id'], 1.))
        self.trade.market.update_book('N8USDT', '1.0188001', '2', int(self.now*1000), 3)
        self.trade.market.decide(self.trade.market.events.get_nowait(), lambda event: None)
        self.assertFalse(self.trade._entry_quote_send_allowed('N8USDT', row['client_order_id'], 1.))

    def test_bid_can_admit_with_tick_connection_down(self):
        self.plans()
        self.trade.market.group_failed(1, 'tick down')
        while not self.trade.market.events.empty():
            self.trade.market.events.get_nowait()
        self.bid(1.01)
        self.assertIn('N0USDT', self.trade.tracked)

    def test_follow_order_retains_size_when_bid_moves_far(self):
        self.event()
        original = dict(self.trade.tracked['AUSDT'])
        self.held(2)
        self.bid(1.5, symbol='AUSDT')
        self.assertFalse(self.cancels)
        self.assertEqual(self.trade.tracked['AUSDT']['qty'], original['qty'])

    def test_send_rechecks_current_or_quotes_for_single_and_batch(self):
        self.plans()
        self.tick(1.01)
        row = self.trade.tracked['N0USDT']
        # Update owner market without running the order planner: emulate a
        # quote change while a prepared request is waiting for actual send.
        self.trade.market.update('N0USDT', '1.02', int(self.now*1000))
        self.trade.market.decide(self.trade.market.events.get_nowait(), lambda event: None)
        client = self.trade.client
        client.enabled, client.dry_run = True, False
        self.trade._account_deadline = self.now+100
        def dispatch(*args, before_send=None, **kw):
            before_send()
            return dict(symbol='N0USDT', clientOrderId=row['client_order_id'], orderId='wire', status='NEW')
        client._broker_call = Mock(side_effect=dispatch)
        request = dict(symbol='N0USDT', client_order_id=row['client_order_id'],
                       order_bar_open_ms=self.bar, limit_price=1., notional=5.)
        with self.assertRaises(EntryQuoteUnavailable):
            ExchangeClient.place_limit_for_bar(client, **request)
        with self.assertRaises(EntryQuoteUnavailable):
            client._entry_send_check(request)
        self.trade.market.update_book('N0USDT', '1.018', '2', int(self.now*1000), 1)
        self.trade.market.decide(self.trade.market.events.get_nowait(), lambda event: None)
        ExchangeClient.place_limit_for_bar(client, **request)
        client._entry_send_check(request)

    def test_transient_quote_reject_does_not_block_returning_candidate(self):
        self.plans()
        self.trade.client.place_limit_for_bar = Mock(side_effect=EntryQuoteUnavailable('quote moved before send'))
        self.tick(1.01)
        self.assertFalse(self.trade.tracked)
        self.trade.client.place_limit_for_bar = Mock(side_effect=self.place)
        self.tick(1.01)
        self.assertIn('N0USDT', self.trade.tracked)

    def test_incremental_ranking_matches_full_selection_after_book_disconnect(self):
        self.plans(8)
        self.templates.append(dict(self.templates[0], symbol='N8USDT'))
        self.publish()
        rng = random.Random(20261120)
        symbols = [r['symbol'] for r in self.templates]
        for scenario in range(24):
            self.trade.tracked.clear()
            self.trade.market.book.group_connected(1)
            while not self.trade.market.events.empty():
                self.trade.market.events.get_nowait()
            for symbol in symbols:
                self.trade.market.update(symbol, str(1+rng.uniform(-.01,.035)), int(self.now*1000))
                self.trade.market.decide(self.trade.market.events.get_nowait(), lambda event: None)
                self.trade.market.update_book(symbol, str(1+rng.uniform(-.01,.035)), '2', int(self.now*1000), scenario+1)
                event = self.trade.market.events.get_nowait()
                self.trade.market.decide(event, lambda event: None)
            snapshot, status = self.trade.desired_snapshot(now=self.now)
            self.trade._eligible.clear()
            self.trade._rank_heap.clear()
            for symbol, price in self.trade.market.entry_prices().items():
                self.trade._update_rank(symbol, price)
            if scenario % 3 == 0:
                self.trade.market.book.group_failed(1, 'disconnected after ranking')
            prices = self.trade.market.entry_prices()
            for row in rng.sample(self.templates, rng.randrange(9)):
                symbol = row['symbol']
                self.trade.tracked[symbol] = dict(row, client_order_id='owned_'+symbol,
                    _quote_fresh=True, observed_price=prices[symbol], gap=prices[symbol]-1)
            working = self.trade._fast_snapshot(snapshot, event, self.now)
            def select(rows):
                return select_physical_orders([dict(row, observed_price=prices[row['symbol']],
                    gap=prices[row['symbol']]-1) for row in rows], self.trade.tracked, now=self.now)
            self.assertEqual(select(snapshot['desired']), select(working['desired']), str(scenario))


class BookReceiverTests(unittest.IsolatedAsyncioTestCase):
    async def test_dual_receiver_uses_identical_groups_for_both_streams(self):
        with tempfile.TemporaryDirectory() as directory:
            market = DualMarket(['AUSDT', 'BUSDT', 'CUSDT'], Path(directory)/'prices.json', group_size=2)
            calls = []
            async def consume(group, state, **kw):
                calls.append((kw['stream_kind'], tuple(group), kw['base']))
                if len(calls) == 4:
                    kw['stop'].set()
                await kw['stop'].wait()
            with patch.object(ticker_ws, 'consume_group', side_effect=consume):
                await market._receive()
            self.assertEqual([g for s,g,b in calls if s == 'aggTrade'],
                             [g for s,g,b in calls if s == 'bookTicker'])
            self.assertTrue(all('/public/' in b for s,g,b in calls if s == 'bookTicker'))

    async def test_real_parser_subscribes_same_symbols_and_reads_best_bid(self):
        stop = asyncio.Event()
        state = Mock()
        state.update_book.return_value = True
        urls = []
        class Socket:
            async def recv(self):
                stop.set()
                return json.dumps(dict(data=dict(e='bookTicker', s='XUSDT', b='1.01', B='4',
                                                 a='1.02', A='5', u=7, T=int(time.time()*1000))))
        class Connection:
            async def __aenter__(self): return Socket()
            async def __aexit__(self, *args): pass
        def connect(url, **kw):
            urls.append(url)
            return Connection()
        with patch.object(ticker_ws.websockets, 'connect', side_effect=connect):
            await ticker_ws.consume_group(['XUSDT'], state,
                base='wss://fstream.binance.com/public/stream', stream_kind='bookTicker',
                ping_interval=30, ping_timeout=20, read_timeout=30, reconnect_before=3600,
                stop=stop, group_index=1)
        self.assertEqual(parse_qs(urlparse(urls[0]).query)['streams'], ['xusdt@bookTicker'])
        self.assertEqual(state.update_book.call_args.args[:3], ('XUSDT', '1.01', '4'))
        self.assertEqual(state.update_book.call_args.args[-1], 7)
