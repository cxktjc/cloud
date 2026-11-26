"""Offline tick/bid0 OR admission, independent feed health and exact sends."""
import asyncio
import json
import os
import random
import tempfile
import time
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from unittest.mock import AsyncMock, Mock, patch

from collectors import ticker_ws
from tests import test_capacity_gate_v2 as fixtures
from v9711_live import config as cfg
from v9711_live.execution.client import ExchangeClient
from v9711_live.execution.logic import select_physical_orders
from v9711_live.execution.submit_errors import EntryQuoteUnavailable
from v9711_live.execution.ws_market import DualMarket
from v9711_live.execution.entry_quotes import qualify_quote
from v9711_live.execution.terminal import compact_positions, TradeTerminalView


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

    def test_recent_error_survives_reconnect_and_marks_only_valid_data_recovered(self):
        self.market.book.group_failed(1, 'ConnectionClosedError: no close frame received or sent')
        error = self.market.metrics()['recent_errors'][0]
        self.assertEqual((error['source'], error['group'], error['count']), ('depth', 1, 1))
        self.assertFalse(error['recovered'])
        self.market.book.group_connected(1)
        self.assertFalse(self.market.metrics()['recent_errors'][0]['recovered'])
        self.quote(tick=None)
        error = self.market.metrics()['recent_errors'][0]
        self.assertTrue(error['recovered'])
        self.assertIn('ConnectionClosedError', error['error'])
        self.assertFalse(self.market.metrics()['depth_errors'])

    def test_error_history_merges_repeats_and_remains_bounded(self):
        for index in range(10):
            self.market.book.group_failed(1, f'error {index}')
        self.market.book.group_failed(1, 'error 9')
        self.market.book.group_failed(1, 'error 9')
        self.assertEqual(len(self.market.book.error_history), 5)
        self.assertEqual(self.market.book.error_history[-1]['count'], 3)
        self.assertEqual(self.market.book.error_history[-1]['error'], 'error 9')
        self.market.book.group_failed(1, 'scheduled reconnect')
        self.market.book.group_failed(1, 'collector stopped')
        self.assertEqual(len(self.market.book.error_history), 5)
        self.assertNotIn('collector stopped', str(self.market.book.error_history))


class WSRecoveryHistoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.elapsed = 0.
        self.epoch = time.time()
        for target, clock in (('time.monotonic', lambda: self.elapsed),
                              ('time.time', lambda: self.epoch+self.elapsed)):
            patcher = patch(target, side_effect=clock)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.market = DualMarket(['XUSDT', 'YUSDT'], Path(self.tmp.name)/'prices.json', group_size=1)
        self.feed = self.market.book
        self.feed.group_failed(1, 'disconnected')

    def receive(self, elapsed, group=1):
        self.elapsed = elapsed
        symbol = 'XUSDT' if group == 1 else 'YUSDT'
        self.assertTrue(self.feed.update(symbol, '1', int(time.time()*1000)))

    def keep_receiving(self, start, end, group=1):
        elapsed = start
        while elapsed < end:
            self.receive(elapsed, group)
            elapsed += 20
        self.receive(end, group)

    def test_recovered_error_retires_at_ten_minutes_of_valid_data(self):
        self.keep_receiving(0, 599.999)
        self.assertTrue(self.feed.recent_errors()[0]['recovered'])
        self.receive(600)
        self.assertEqual(self.feed.recent_errors(), [])
        self.assertEqual(self.feed.error_history, [])
        self.feed.group_failed(1, 'another disconnect')
        self.assertEqual([row['error'] for row in self.feed.recent_errors()], ['another disconnect'])

    def test_recurrence_resets_timer_and_merges_count(self):
        self.keep_receiving(0, 500)
        self.feed.group_failed(1, 'disconnected')
        self.assertFalse(self.feed.recent_errors()[0]['recovered'])
        self.keep_receiving(510, 1109)
        row = self.feed.recent_errors()[0]
        self.assertEqual(row['count'], 2)
        self.assertTrue(row['recovered'])
        self.receive(1110)
        self.assertEqual(self.feed.recent_errors(), [])

    def test_reconnect_and_invalid_data_do_not_start_recovery_timer(self):
        self.elapsed = 2000
        self.feed.group_connected(1)
        self.assertFalse(self.feed.update('XUSDT', '1', int((time.time()-120)*1000)))
        self.assertFalse(self.feed.recent_errors()[0]['recovered'])
        self.keep_receiving(2001, 2600)
        self.assertTrue(self.feed.recent_errors()[0]['recovered'])
        self.receive(2601)
        self.assertEqual(self.feed.recent_errors(), [])

    def test_scheduled_reconnect_restarts_continuous_recovery(self):
        self.keep_receiving(0, 500)
        self.feed.group_failed(1, 'scheduled reconnect')
        self.feed.group_connected(1)
        self.keep_receiving(510, 1109)
        self.assertEqual(len(self.feed.recent_errors()), 1)
        self.receive(1110)
        self.assertEqual(self.feed.recent_errors(), [])

    def test_silent_gap_restarts_timer_even_without_panel_refresh(self):
        for refresh_during_gap in (False, True):
            with self.subTest(refresh_during_gap=refresh_during_gap):
                self.feed.group_failed(1, 'disconnected')
                start = self.elapsed+1
                self.keep_receiving(start, start+500)
                self.elapsed = start+531
                if refresh_during_gap:
                    self.assertFalse(self.feed.recent_errors()[0]['recovered'])
                self.keep_receiving(start+531, start+1130)
                self.assertTrue(self.feed.recent_errors()[0]['recovered'])
                self.receive(start+1131)
                self.assertEqual(self.feed.recent_errors(), [])

    def test_other_groups_and_sources_cannot_expire_unrecovered_errors(self):
        self.feed.group_failed(2, 'depth group 2 down')
        self.market.group_failed(1, 'tick group 1 down')
        self.keep_receiving(0, 600)
        rows = self.market.metrics()['recent_errors']
        self.assertEqual({(row['source'], row['group']) for row in rows}, {('depth', 2), ('tick', 1)})
        self.assertTrue(all(not row['recovered'] for row in rows))
        self.elapsed = 10000
        self.assertEqual(len(self.market.metrics()['recent_errors']), 2)

    def test_wall_clock_jump_does_not_expire_recovered_error(self):
        self.keep_receiving(0, 300)
        self.epoch += 86400
        self.keep_receiving(320, 599)
        self.assertTrue(self.feed.recent_errors()[0]['recovered'])
        self.receive(600)
        self.assertEqual(self.feed.recent_errors(), [])


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

    def test_tick_depth_all_labels_use_actual_admission_and_rotation_ceiling(self):
        cases = [
            (1.0185, 1.019, .0185, 'tick'),
            (1.019, 1.0185, .0185, 'depth'),
            (1.0185, 1.018, .0185, 'all'),
            (1.0165, 1.017, .0165, 'tick'),
            (1.018, 1.0188, .0188, 'all'),  # replacement above fill threshold
            (1.017, 1.017, .0165, 'none'),
            (None, 1.0185, .0185, 'depth'),
            (1.0185, None, .0185, 'tick'),
            (None, None, .0185, 'none'),
        ]
        for tick, bid, ceiling, expected in cases:
            with self.subTest(tick=tick, bid=bid, ceiling=ceiling):
                detail = qualify_quote(dict(tick=tick, bid0=bid, source='bid0'), 1., ceiling)
                self.assertEqual(detail['entry_qualified_by'], expected)

    def test_changed_book_after_admission_does_not_overwrite_frozen_source(self):
        self.plans()
        client = self.trade.client
        client.enabled, client.dry_run = True, False
        self.trade._account_deadline = self.now+100
        def dispatch(*args, before_send=None, **kw):
            before_send()  # first check: tick only
            self.trade.market.update_book('N0USDT', '1.012', '2', int(self.now*1000), 1)
            self.trade.market.decide(self.trade.market.events.get_nowait(), lambda event: None)
            before_send()  # final socket check: both
            return dict(symbol='N0USDT', clientOrderId=kw['client_order_id'], orderId='wire', status='NEW')
        client._broker_call = Mock(side_effect=dispatch)
        client.place_limit_for_bar = lambda **req: ExchangeClient.place_limit_for_bar(client, **req)
        with self.assertLogs('v9711_live_trade.entry_quotes', level='INFO') as logs:
            self.tick(1.01)
        row = self.trade.tracked['N0USDT']
        self.assertEqual(row['entry_qualified_by'], 'tick')
        self.assertEqual(row['entry_send_quote']['entry_qualified_by'], 'tick')
        self.assertIn('准入满足=tick', compact_positions(self.trade.tracked, 12))
        self.assertIn('满足=tick', '\n'.join(logs.output))
        self.assertNotIn('满足=all', '\n'.join(logs.output))
        saved = json.loads(cfg.EXECUTION_LEDGER_FILE.read_text())
        self.assertEqual(saved['state']['tracked_orders']['N0USDT']['entry_send_quote']['entry_qualified_by'], 'tick')

    def test_batch_send_retains_each_members_own_qualification(self):
        self.plans(2)
        self.trade.market.update('N1USDT', '1.03', int(self.now*1000))
        self.trade.market.decide(self.trade.market.events.get_nowait(), lambda event: None)
        self.trade.market.update_book('N1USDT', '1.01', '2', int(self.now*1000), 1)
        self.trade.market.decide(self.trade.market.events.get_nowait(), lambda event: None)
        self.tick(1.01)
        requests = [dict(symbol=s, client_order_id=row['client_order_id'],
            order_bar_open_ms=self.bar, limit_price=1., notional=5.) for s,row in self.trade.tracked.items()]
        self.trade.client.enabled, self.trade.client.dry_run = True, False
        self.trade._account_deadline = self.now+100
        def post(wires):
            return [dict(symbol=w['symbol'], clientOrderId=w['newClientOrderId'], orderId=str(i), status='NEW')
                    for i,w in enumerate(wires, 1)]
        self.trade.client._post_batch_orders = Mock(side_effect=post)
        replies = self.trade.client.place_limit_batch(requests)
        labels = {s: replies[row['client_order_id']][0]['_entry_send_quote']['entry_qualified_by']
                  for s,row in self.trade.tracked.items()}
        self.assertEqual(labels, {'N0USDT': 'tick', 'N1USDT': 'depth'})
        self.trade.client._post_batch_orders.assert_called_once()

    def test_follow_and_legacy_orders_do_not_claim_ordinary_threshold_qualification(self):
        self.event()
        row = self.trade.tracked['AUSDT']
        self.assertEqual(row['entry_qualified_by'], 'signal_follow')
        self.assertIn('准入满足=signal_follow', compact_positions(self.trade.tracked, 12))
        legacy = dict(symbol='OLDUSDT', client_order_id='old', quantity=1)
        self.assertIn('登记满足=unknown', compact_positions([legacy], 12))

    def test_panel_keeps_recovered_ws_error_above_positions_on_later_refresh(self):
        self.trade.market.book.group_failed(1, 'ConnectionClosedError: no close frame received or sent')
        self.trade.market.book.group_connected(1)
        while not self.trade.market.events.empty():
            self.trade.market.events.get_nowait()
        self.bid(1.01)
        self.trade.step(now=self.now)
        comparison = self.trade._render_trade.call_args.kwargs['comparison']
        fields = dict(comparison=comparison, clock_utc='test', entry_diagnostics=self.trade._entry_diagnostics,
                      entry_gap_policy={'fill_gap': .0165}, current_admission_gap=.0185)
        view = TradeTerminalView()
        first = view._format(fields)
        self.assertIn('[已恢复] ConnectionClosedError', first)
        self.assertLess(first.index('WS 最近报错'), first.index('当前实际持仓'))
        self.now += 1
        self.trade.step(now=self.now)
        fields['entry_diagnostics'] = self.trade._entry_diagnostics
        self.assertIn('[已恢复] ConnectionClosedError', view._format(fields))

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
        self.assertIn('N8USDT', self.trade.tracked)
        # Full-book replacements send before the victim's cancel ACK.
        # Maintenance must not submit the replacement a second time.
        placements = len(self.submits)
        self.trade.step(now=self.now)
        self.assertEqual(len(self.submits), placements)
        row = self.trade.tracked['N8USDT']
        self.assertAlmostEqual(row['entry_gap_ceiling'], .0188)
        self.assertTrue(self.trade._entry_quote_send_allowed('N8USDT', row['client_order_id'], 1.))
        self.trade.market.update_book('N8USDT', '1.0188001', '2', int(self.now*1000), 3)
        self.trade.market.decide(self.trade.market.events.get_nowait(), lambda event: None)
        self.assertFalse(self.trade._entry_quote_send_allowed('N8USDT', row['client_order_id'], 1.))
        self.trade.market.update_book('N8USDT', '1.0188', '2', int(self.now*1000), 4)
        self.assertTrue(self.trade._entry_quote_send_allowed('N8USDT', row['client_order_id'], 1.))

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

    def test_single_and_batch_retain_admitted_quote_after_price_changes(self):
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
        with patch.object(self.trade.market, 'send_quote', side_effect=AssertionError('secondary WS read')):
            result = ExchangeClient.place_limit_for_bar(client, **request)
            detail = client._entry_send_check(request)
        self.assertEqual(result['_entry_send_quote']['entry_tick'], 1.01)
        self.assertEqual(detail['entry_qualified_by'], 'tick')
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


class SubscribeReceiverTests(unittest.IsolatedAsyncioTestCase):
    async def consume(self, state, stop, *, base=ticker_ws.BOOK_WS_BASE, stream='bookTicker', symbols=None,
                      startup_done=None):
        await ticker_ws.consume_group(symbols or ['XUSDT'], state, base=base, stream_kind=stream,
            ping_interval=30, ping_timeout=20, read_timeout=30, reconnect_before=3600,
            stop=stop, group_index=3, subscribe=True, startup_done=startup_done)

    async def test_startup_waits_until_subscription_send_finishes(self):
        stop, state = asyncio.Event(), Mock()
        sending, release, startup_done = asyncio.Event(), asyncio.Event(), asyncio.Event()
        class Socket:
            async def send(self, raw):
                sending.set()
                await release.wait()
            async def recv(self):
                stop.set()
                return json.dumps(dict(e='bookTicker', s='XUSDT', b='1.01', B='4', u=7,
                                       T=int(time.time()*1000)))
        class Connection:
            async def __aenter__(self): return Socket()
            async def __aexit__(self, *args): pass
        with patch.object(ticker_ws.websockets, 'connect', return_value=Connection()):
            task = asyncio.create_task(self.consume(state, stop, startup_done=startup_done))
            try:
                await asyncio.wait_for(sending.wait(), 1)
                self.assertFalse(startup_done.is_set())
                state.group_received.assert_not_called()
                release.set()
                await asyncio.wait_for(task, 1)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        self.assertTrue(startup_done.is_set())

    async def test_failed_handshake_releases_startup_but_does_not_mark_ready(self):
        stop, state, startup_done = asyncio.Event(), Mock(), asyncio.Event()
        async def backoff(seconds): stop.set()
        with patch.object(ticker_ws.websockets, 'connect', side_effect=OSError('handshake failed')), \
                patch.object(ticker_ws.asyncio, 'sleep', side_effect=backoff):
            await self.consume(state, stop, startup_done=startup_done)
        self.assertTrue(startup_done.is_set())
        state.group_received.assert_not_called()
        state.group_failed.assert_called_once()

    async def test_real_socket_sends_subscription_and_parses_both_event_formats(self):
        for stream in ('aggTrade', 'bookTicker'):
            for combined in (False, True):
                with self.subTest(stream=stream, combined=combined):
                    stop, state, requests = asyncio.Event(), Mock(), []
                    def accepted(*args):
                        stop.set()
                        return True
                    state.update.side_effect = state.update_book.side_effect = accepted
                    async def server(socket):
                        requests.append((socket.request.path, json.loads(await socket.recv())))
                        await socket.send(json.dumps(dict(result=None, id=3)))
                        data = dict(e=stream, s='XUSDT', p='1.01', b='1.01', B='4', u=7,
                                    T=int(time.time()*1000))
                        await socket.send(json.dumps(dict(data=data) if combined else data))
                        await socket.wait_closed()
                    async with ticker_ws.websockets.serve(server, '127.0.0.1', 0) as listener:
                        port = listener.sockets[0].getsockname()[1]
                        await asyncio.wait_for(self.consume(state, stop,
                            base=f'ws://127.0.0.1:{port}/stream', stream=stream,
                            symbols=['XUSDT', '币安人生USDT']), timeout=3)
                    self.assertEqual(requests, [('/stream', dict(method='SUBSCRIBE',
                        params=[f'xusdt@{stream}', f'币安人生usdt@{stream}'], id=3))])
                    state.group_received.assert_called_once_with(3)
                    (state.update_book if stream == 'bookTicker' else state.update).assert_called_once()
                    state.group_failed.assert_not_called()

    async def test_ack_without_data_does_not_mark_ready(self):
        stop, state = asyncio.Event(), Mock()
        class Socket:
            send = AsyncMock()
            async def recv(self):
                if not getattr(self, 'sent_ack', False):
                    self.sent_ack = True
                    return json.dumps(dict(result=None, id=3))
                state.group_received.assert_not_called()
                raise asyncio.TimeoutError()
        class Connection:
            async def __aenter__(self): return Socket()
            async def __aexit__(self, *args): pass
        async def backoff(seconds): stop.set()
        with patch.object(ticker_ws.websockets, 'connect', return_value=Connection()), \
                patch.object(ticker_ws.asyncio, 'sleep', side_effect=backoff):
            await self.consume(state, stop)
        state.group_received.assert_not_called()
        state.update_book.assert_not_called()
        self.assertIn('waiting_for=first_data', state.group_failed.call_args.args[1])

    async def test_reconnect_resubscribes_before_receiving(self):
        stop, state, requests = asyncio.Event(), Mock(), []
        class Socket:
            async def send(self, raw): requests.append(json.loads(raw))
            async def recv(self):
                if len(requests) == 1:
                    raise ConnectionResetError('peer reset')
                stop.set()
                return json.dumps(dict(e='bookTicker', s='XUSDT', b='1.01', B='4', u=7,
                                       T=int(time.time()*1000)))
        class Connection:
            async def __aenter__(self): return Socket()
            async def __aexit__(self, *args): pass
        async def backoff(seconds): pass
        with patch.object(ticker_ws.websockets, 'connect', return_value=Connection()) as connect, \
                patch.object(ticker_ws.asyncio, 'sleep', side_effect=backoff):
            await self.consume(state, stop)
        self.assertEqual(requests, [dict(method='SUBSCRIBE', params=['xusdt@bookTicker'], id=3)]*2)
        self.assertEqual([call.args[0] for call in connect.call_args_list], [ticker_ws.BOOK_WS_BASE]*2)
        state.group_received.assert_called_once_with(3)

    async def test_subscription_errors_fail_without_authorizing_quotes(self):
        for response in (dict(code=2, msg='Invalid request', id=3), dict(result=False, id=3)):
            with self.subTest(response=response):
                stop, state = asyncio.Event(), Mock()
                class Socket:
                    send = AsyncMock()
                    async def recv(self): return json.dumps(response)
                class Connection:
                    async def __aenter__(self): return Socket()
                    async def __aexit__(self, *args): pass
                async def backoff(seconds): stop.set()
                with patch.object(ticker_ws.websockets, 'connect', return_value=Connection()), \
                        patch.object(ticker_ws.asyncio, 'sleep', side_effect=backoff):
                    await self.consume(state, stop)
                state.group_received.assert_not_called()
                state.update_book.assert_not_called()
                self.assertIn('ValueError', state.group_failed.call_args.args[1])


class BookReceiverTests(unittest.IsolatedAsyncioTestCase):
    async def test_book_retry_never_switches_to_market_endpoint(self):
        self.assertEqual(ticker_ws.endpoint_candidates(ticker_ws.BOOK_WS_BASE, 'bookTicker'),
                         (ticker_ws.BOOK_WS_BASE,))
        self.assertEqual(ticker_ws.endpoint_candidates(ticker_ws.WS_BASE, 'bookTicker'),
                         (ticker_ws.BOOK_WS_BASE,))
        custom = 'wss://example.invalid/test'
        self.assertEqual(ticker_ws.endpoint_candidates(custom, 'bookTicker'), (custom,))

    async def test_dual_receiver_uses_identical_groups_for_both_streams(self):
        with tempfile.TemporaryDirectory() as directory:
            symbols = [f'TEST{i:04d}USDT' for i in range(647)]
            market = DualMarket(symbols, Path(directory)/'prices.json')
            self.assertEqual([len(group) for group in market.groups], [100]*6+[47])
            self.assertEqual(market.groups, market.book.groups)
            calls = []
            async def consume(group, state, **kw):
                self.assertIs(kw['subscribe'], True)
                self.assertIsNone(kw['read_timeout'])
                self.assertIsNone(kw['reconnect_before'])
                calls.append((kw['stream_kind'], tuple(group), kw['base']))
                kw['startup_done'].set()
                if len(calls) == 14:
                    kw['stop'].set()
                await kw['stop'].wait()
            with patch.object(ticker_ws, 'consume_group', side_effect=consume), \
                    patch.object(market, '_startup_pause', new_callable=AsyncMock) as pause:
                await market._receive()
            self.assertEqual(pause.await_count, 13)
            self.assertEqual([s for s,g,b in calls], ['bookTicker']*7+['aggTrade']*7)
            self.assertEqual([g for s,g,b in calls if s == 'aggTrade'],
                             [g for s,g,b in calls if s == 'bookTicker'])
            for stream in ('aggTrade', 'bookTicker'):
                self.assertEqual([symbol for kind, group, base in calls if kind == stream
                                  for symbol in group], symbols)
            self.assertTrue(all('/public/' in b for s,g,b in calls if s == 'bookTicker'))

    async def test_next_connection_waits_one_second_after_previous_subscription(self):
        with tempfile.TemporaryDirectory() as directory:
            market = DualMarket(['XUSDT'], Path(directory)/'prices.json')
            started, release = asyncio.Event(), asyncio.Event()
            calls, sent = [], []
            async def consume(group, state, **kw):
                calls.append((kw['stream_kind'], time.perf_counter()))
                if len(calls) == 1:
                    started.set()
                    await release.wait()  # Handshake/subscription still pending.
                sent.append(time.perf_counter())
                kw['startup_done'].set()
                if len(calls) == 2:
                    kw['stop'].set()
                await kw['stop'].wait()
            with patch.object(ticker_ws, 'consume_group', side_effect=consume):
                task = asyncio.create_task(market._receive())
                try:
                    await asyncio.wait_for(started.wait(), 1)
                    self.assertEqual(len(calls), 1)
                    release.set()
                    await asyncio.wait_for(task, 3)
                finally:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
            self.assertEqual([s for s,t in calls], ['bookTicker', 'aggTrade'])
            self.assertGreaterEqual(calls[1][1]-sent[0], .99)

    async def test_stop_during_handshake_or_gap_does_not_start_later_groups(self):
        for during_pause in (False, True):
            with self.subTest(during_pause=during_pause), tempfile.TemporaryDirectory() as directory:
                market = DualMarket(['XUSDT'], Path(directory)/'prices.json')
                waiting, blocked = asyncio.Event(), asyncio.Event()
                calls, cancelled = [], []
                async def consume(group, state, **kw):
                    calls.append(kw['stream_kind'])
                    try:
                        if during_pause:
                            kw['startup_done'].set()
                        else:
                            waiting.set()
                        await blocked.wait()
                    finally:
                        cancelled.append(kw['stream_kind'])
                async def pause():
                    waiting.set()
                    await blocked.wait()
                with patch.object(ticker_ws, 'consume_group', side_effect=consume), \
                        patch.object(market, '_startup_pause', side_effect=pause):
                    task = asyncio.create_task(market._receive())
                    try:
                        await asyncio.wait_for(waiting.wait(), 1)
                        market._async_stop.set()
                        await asyncio.wait_for(task, 1)
                    finally:
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                self.assertEqual(calls, ['bookTicker'])
                self.assertEqual(cancelled, ['bookTicker'])

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
            self.assertIsNone(kw['proxy'])
            return Connection()
        with patch.object(ticker_ws.websockets, 'connect', side_effect=connect), patch.dict(os.environ,
                {'HTTPS_PROXY': 'http://127.0.0.1:12334', 'ALL_PROXY': 'http://127.0.0.1:12334'}):
            await ticker_ws.consume_group(['XUSDT'], state,
                base='wss://fstream.binance.com/public/stream', stream_kind='bookTicker',
                ping_interval=30, ping_timeout=20, read_timeout=30, reconnect_before=3600,
                stop=stop, group_index=1)
        self.assertEqual(parse_qs(urlparse(urls[0]).query)['streams'], ['xusdt@bookTicker'])
        self.assertEqual(state.update_book.call_args.args[:3], ('XUSDT', '1.01', '4'))
        self.assertEqual(state.update_book.call_args.args[-1], 7)

    async def test_timeout_reports_stream_endpoint_direct_mode_and_rejected_timestamps(self):
        stop, state = asyncio.Event(), Mock()
        class Socket:
            async def recv(self):
                if not getattr(self, 'sent', False):
                    self.sent = True
                    return json.dumps(dict(e='bookTicker', s='XUSDT', b='1', B='4', u=7,
                                           T=int((time.time()-3600)*1000)))
                raise asyncio.TimeoutError()
        class Connection:
            async def __aenter__(self): return Socket()
            async def __aexit__(self, *args): pass
        async def backoff(seconds):
            stop.set()
        with patch.object(ticker_ws.websockets, 'connect', return_value=Connection()), \
                patch.object(ticker_ws.asyncio, 'sleep', side_effect=backoff), \
                self.assertLogs('v9711.ticker_ws', level='WARNING') as logs:
            await ticker_ws.consume_group(['XUSDT'], state, base=ticker_ws.BOOK_WS_BASE,
                stream_kind='bookTicker', ping_interval=30, ping_timeout=20, read_timeout=30,
                reconnect_before=3600, stop=stop, group_index=1)
        text = '\n'.join(logs.output)
        self.assertIn('stream=bookTicker', text)
        self.assertIn('proxy=disabled/direct', text)
        self.assertIn("'stale_or_future_timestamp': 1", text)
        self.assertIn('/public/stream', text)
        state.update_book.assert_not_called()
