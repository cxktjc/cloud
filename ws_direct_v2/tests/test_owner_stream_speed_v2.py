"""Real account DB, hot owned quotes and useful owner throughput evidence."""
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from collectors.binance_account_stream import AccountStore
from tests import test_flash_speed_v2 as fixtures
from v9711_live import config as cfg
from v9711_live.execution import stream as stream_module
from v9711_live.execution.stream import AccountStreamView, StreamUnavailable
from v9711_live.execution.owner_work import OwnerWork
from v9711_live.execution.ws_market import DualMarket


class IndexedAccountTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.store = AccountStore(self.root)
        self.addCleanup(self.store.close)
        self.store.status(mode='live', ready=True, connected=True)
        self.store.export()
        self.view = AccountStreamView(self.root)
        self.stamp = int(time.time()*1000)
        self.cid = cfg.ORDER_CLIENT_PREFIX+'_entry_owned'

    def fill(self, oid=1, cid=None, symbol='XUSDT', qty='1', trade_id=1):
        self.store.apply(dict(e='ORDER_TRADE_UPDATE', E=self.stamp, T=self.stamp,
            o=dict(s=symbol, i=oid, c=cid or self.cid, ps='LONG', S='BUY', x='TRADE', X='PARTIALLY_FILLED',
                   q='3', z=qty, l=qty, ap='2', L='2', t=trade_id, n='.01', N='USDT')))

    def test_exact_lookup_never_decodes_unrelated_order_or_fill_history(self):
        self.fill()
        foreign = json.dumps(dict(foreign_marker=True, symbol='OTHERUSDT'))
        self.store.db.executemany('INSERT INTO orders VALUES (?,?,?)',
            [(f'OTHERUSDT:{i}', self.stamp, foreign) for i in range(100)])
        self.store.db.executemany('INSERT INTO fills VALUES (?,?,?)',
            [(f'OTHERUSDT:{i}:1', self.stamp, foreign) for i in range(100)])
        self.store.db.commit()
        loads = json.loads
        def decode(raw, *args, **kwargs):
            self.assertNotIn('foreign_marker', raw)
            return loads(raw, *args, **kwargs)
        with patch.object(stream_module.json, 'loads', side_effect=decode):
            result = self.view.lookup_order(symbol='XUSDT', order_id=1, client_order_id=self.cid)
        self.assertEqual(result['executedQty'], 1)
        self.assertEqual(result['clientOrderId'], self.cid)

    def test_fill_prefix_does_not_mix_order_one_with_order_ten(self):
        self.fill(oid=1, qty='1')
        self.fill(oid=10, qty='2', cid=self.cid+'_ten')
        result = self.view.lookup_order(symbol='XUSDT', order_id=1, client_order_id=self.cid)
        self.assertEqual(result['executedQty'], 1)
        self.assertEqual(len(result['_user_fills']), 1)
        self.assertIsNone(self.view.lookup_order(symbol='XUSDT', order_id=1, client_order_id='foreign'))

    def test_cid_only_recovery_selects_matching_symbol_and_latest_exact_order(self):
        self.fill(oid=1)
        self.fill(oid=2, cid=self.cid+'_different')
        self.fill(oid=3, symbol='XUSDT0')
        result = self.view.lookup_order(symbol='XUSDT', client_order_id=self.cid)
        self.assertEqual(result['orderId'], 1)
        self.assertEqual(result['executedQty'], 1)

    def test_standard_and_lite_are_deduplicated_after_indexed_query(self):
        self.fill()
        for tid, qty in ((1, '1'), (2, '2')):
            self.store.apply(dict(e='TRADE_LITE', E=self.stamp, T=self.stamp,
                s='XUSDT', i=1, c=self.cid, S='BUY', L='2', l=qty, t=tid))
        result = self.view.lookup_order(symbol='XUSDT', order_id=1, client_order_id=self.cid)
        self.assertEqual(result['executedQty'], 3)
        self.assertEqual(result['status'], 'FILLED')
        self.assertEqual(len(result['_user_fills']), 2)
        standard = next(f for f in result['_user_fills'] if str(f['tradeId']) == '1')
        self.assertEqual(standard['commission'], .01)

    def test_cached_health_still_expires_and_invalid_replacement_revokes_it(self):
        self.view.health()
        revision = self.view._health_reader.revision
        self.view.health()
        self.assertEqual(self.view._health_reader.revision, revision)
        with patch('time.time', return_value=time.time()+self.view.max_age_seconds+1):
            with self.assertRaises(StreamUnavailable):
                self.view.health()
        self.view.snapshot_path.write_text('invalid')
        with self.assertRaises(StreamUnavailable):
            self.view.health()


class OwnerEvidenceTests(unittest.TestCase):
    def test_rates_separate_unchanged_receipts_from_actual_queue_work(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch('time.monotonic', return_value=10):
                market = DualMarket(['XUSDT'], Path(tmp)/'prices')
            market.book.counts.update(received=100, unchanged=90, enqueued=10, decided=8, rejected_backlog=2)
            with patch('time.monotonic', return_value=11):
                metrics = market.metrics()
            self.assertEqual(metrics['receive_per_second'], 100)
            self.assertEqual(metrics['enqueue_per_second'], 10)
            self.assertEqual(metrics['decide_per_second'], 8)
            self.assertEqual(metrics['unchanged_per_second'], 90)
            self.assertEqual(metrics['expired_per_second'], 2)

    def test_owner_work_history_is_bounded_and_labels_slow_stages(self):
        work = OwnerWork()
        with patch('time.perf_counter_ns', return_value=100_000_000):
            for _ in range(600):
                work.finish('quote', 0, {'callbacks': 90, 'screening': 10}, route='full')
        snapshot = work.snapshot()
        self.assertEqual(snapshot['quote']['count'], 600)
        self.assertEqual(snapshot['quote']['samples'], 512)
        self.assertEqual(len(snapshot['recent_slow']), 5)
        self.assertEqual(snapshot['recent_slow'][-1]['stage'], 'callbacks')


class HotOwnedQuotesTests(unittest.TestCase):
    setUp = fixtures.FlashOwnerTests.setUp
    plans, event = fixtures.FlashOwnerTests.plans, fixtures.FlashOwnerTests.event
    place, cancel = fixtures.FlashOwnerTests.place, fixtures.FlashOwnerTests.cancel
    target, make_trade = fixtures.FlashOwnerTests.target, fixtures.FlashOwnerTests.make_trade
    refresh_quotes = fixtures.FlashOwnerTests.refresh_quotes
    publish, held = fixtures.FlashOwnerTests.publish, fixtures.FlashOwnerTests.held

    def test_missing_owned_quote_does_not_replan_every_other_quote(self):
        self.plans(9)
        self.event('N0USDT', 1.01)
        market = self.trade.market
        market.processed.pop('N0USDT')
        market.latest_ingress.pop('N0USDT')
        self.assertIsNone(market.entry_quote('N0USDT')['selected'])
        with patch.object(self.trade, '_execute_entries', side_effect=AssertionError('missing quote repeats full plan')):
            for price in (1.0101, 1.0099, 1.0101):
                self.event('N8USDT', price)
        self.assertFalse(self.cancels)
        self.assertEqual(len(self.trade.tracked), 8)
        # Recovery must immediately restore ordinary distance/replacement checks.
        self.event('N0USDT', 1.022)
        self.assertNotIn('N0USDT', self.trade.tracked)

    def test_missing_owned_quote_still_allows_fresh_candidate_in_vacancy(self):
        self.plans(9)
        for index in range(3, 9):
            self.trade.market.update(f'N{index}USDT', '1.02', int(self.now*1000))
            self.trade.market.decide(self.trade.market.events.get_nowait(), lambda event: None)
        self.event('N0USDT', 1.01)
        self.assertEqual(len(self.trade.tracked), 3)
        self.trade.market.processed.pop('N0USDT')
        self.trade.market.latest_ingress.pop('N0USDT')
        with patch.object(self.trade, '_execute_entries', side_effect=AssertionError('no vacancy candidate qualifies')):
            self.event('N8USDT', 1.0201)
        self.event('N8USDT', 1.0185)
        self.assertIn('N8USDT', self.trade.tracked)
        self.assertIn('N0USDT', self.trade.tracked)

    def test_missing_quote_does_not_suppress_other_owned_distance_cancel(self):
        self.plans(9)
        self.event('N0USDT', 1.01)
        self.trade.market.processed.pop('N0USDT')
        self.trade.market.latest_ingress.pop('N0USDT')
        self.event('N1USDT', 1.022)
        self.assertNotIn('N1USDT', self.trade.tracked)
        self.assertIn('N0USDT', self.trade.tracked)

    def test_owned_price_changes_without_action_do_not_repeat_full_plan(self):
        self.plans(9)
        self.event('N0USDT', 1.01)
        with patch.object(self.trade, '_execute_entries', side_effect=AssertionError('hot owned quote')):
            for price in (1.0101, 1.0099, 1.0101):
                self.event('N0USDT', price)
        self.event('N0USDT', 1.022)
        self.assertNotIn('N0USDT', self.trade.tracked)

    def test_owned_price_crossing_can_enable_replacement(self):
        self.plans(9)
        self.event('N0USDT', 1.01)
        self.event('N0USDT', 1.02)
        self.assertIn('N8USDT', self.trade.tracked)

    def test_live_target_quote_is_skipped_only_after_follow_is_owned(self):
        self.targets = [self.target('AUSDT')]
        self.templates = []
        self.publish()
        self.event('AUSDT', 1.2)
        self.assertTrue(self.trade.tracked['AUSDT']['signal_follow'])
        with patch.object(self.trade, '_execute_entries', side_effect=AssertionError('stable follow quote')):
            self.event('AUSDT', 1.21)
        self.held(6)
        self.event('AUSDT', 1.22)
        self.assertNotIn('AUSDT', self.trade.tracked)
