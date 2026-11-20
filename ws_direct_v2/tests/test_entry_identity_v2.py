"""Regression coverage for duplicate entry ownership and durable physical IDs."""
import json
import sqlite3
import unittest
from contextlib import closing
from copy import deepcopy
from unittest.mock import Mock

from tests import test_signal_follow_v2 as fixtures
from v9711_live import config as cfg
from v9711_live.execution.client import ExchangeClient


class EntryIdentityTests(unittest.TestCase):
    setUp = fixtures.SignalFollowTests.setUp
    target = fixtures.SignalFollowTests.target
    make_trade = fixtures.SignalFollowTests.make_trade
    refresh_quotes = fixtures.SignalFollowTests.refresh_quotes
    place = fixtures.SignalFollowTests.place
    cancel = fixtures.SignalFollowTests.cancel
    publish = fixtures.SignalFollowTests.publish
    event = fixtures.SignalFollowTests.event

    def ordinary(self):
        self.targets = []
        self.templates = [dict(symbol='N0USDT', limit_price=1., notional=5.,
            order_bar_open_ms=self.bar, reference_bar_open_ms=self.bar-cfg.BAR_MS,
            exit_due_epoch=self.now+900)]
        self.publish()

    def quote(self, symbol, price):
        self.trade.market.update(symbol, str(price), int(self.now*1000))
        self.trade.market.decide(self.trade.market.events.get_nowait(), self.trade.on_tick)

    def real_query(self):
        self.trade.client.query_exact_order = ExchangeClient.query_exact_order.__get__(self.trade.client)

    def old_terminal_stream(self, terminal, *, stamp=None):
        directory = self.trade.client.stream.directory
        directory.mkdir(parents=True, exist_ok=True)
        stamp = int(self.now*1000) if stamp is None else stamp
        (directory/'account_snapshot.json').write_text(json.dumps(dict(
            published_at_ms=int(self.now*1000), last_event_seq=1,
            status=dict(mode='live', ready=True, connected=True, account_id=cfg.ACCOUNT_ID))),
            encoding='utf-8')
        payload = dict(symbol='N0USDT', order_id=terminal['orderId'],
            client_order_id=terminal['clientOrderId'], status='CANCELED',
            executed_quantity=0., average_price=0., original_quantity=float(terminal['origQty']),
            updated_at_ms=stamp, order_side='BUY', position_side='LONG')
        with closing(sqlite3.connect(directory/'events.sqlite3')) as db:
            db.execute('CREATE TABLE orders(key TEXT PRIMARY KEY, version_ms INTEGER, payload TEXT)')
            db.execute('CREATE TABLE fills(key TEXT PRIMARY KEY, transaction_ms INTEGER, payload TEXT)')
            db.execute('CREATE TABLE events(seq INTEGER PRIMARY KEY, event_type TEXT, payload TEXT)')
            db.execute('INSERT INTO orders VALUES (?,?,?)', ('N0USDT:'+str(terminal['orderId']), stamp, json.dumps(payload)))
            db.commit()

    def test_gap_cancel_reentry_uses_new_durable_identity_even_after_restart(self):
        self.ordinary()
        self.event('N0USDT')
        first = deepcopy(self.trade.tracked['N0USDT'])
        self.quote('N0USDT', 1.022)
        self.assertNotIn('N0USDT', self.trade.tracked)
        self.make_trade()
        self.event('N0USDT')
        second = deepcopy(self.trade.tracked['N0USDT'])
        self.assertNotEqual(first['client_order_id'], second['client_order_id'])
        self.assertLessEqual(len(second['client_order_id']), 36)
        self.assertEqual(self.trade.ledger.state['entry_submission_sequence'], 2)
        self.assertFalse(self.trade.ledger.owns_client(first['client_order_id']))
        self.assertTrue(self.trade.ledger.owns_client(second['client_order_id']))
        self.make_trade()
        self.event('N0USDT')
        self.assertEqual(self.trade.tracked['N0USDT']['client_order_id'], second['client_order_id'])
        self.assertEqual(len(self.submits), 2)

    def test_old_real_sqlite_terminal_cannot_retire_ack_lost_replacement(self):
        self.ordinary()
        self.event('N0USDT')
        first = deepcopy(self.trade.tracked['N0USDT'])
        self.quote('N0USDT', 1.022)
        self.old_terminal_stream(self.orders[first['client_order_id']])
        self.submit_error = TimeoutError('accepted remotely, ACK lost')
        self.quote('N0USDT', 1.01)
        second = deepcopy(self.trade.tracked['N0USDT'])
        self.assertNotEqual(first['client_order_id'], second['client_order_id'])
        self.assertEqual(second['order_id'], '')
        self.assertEqual(self.orders[second['client_order_id']]['status'], 'NEW')
        self.real_query()
        self.trade.reconcile_orders(now=self.now)
        self.assertEqual(self.trade.tracked['N0USDT']['client_order_id'], second['client_order_id'])
        self.assertTrue(self.trade.ledger.owns_client(second['client_order_id']))
        # Next-bar candidates cannot send while the accepted old attempt is
        # still unresolved. Its UNKNOWN slot remains occupied after restart too.
        self.now += cfg.BAR_MS/1000
        self.bar += cfg.BAR_MS
        self.submit_error = None
        self.ordinary()
        self.refresh_quotes()
        self.event('N0USDT')
        self.assertEqual(len(self.submits), 2)
        self.assertEqual(self.trade.tracked['N0USDT']['client_order_id'], second['client_order_id'])
        live = [r for r in self.orders.values() if r['status'] == 'NEW']
        self.assertEqual(len(live), 1)
        self.make_trade()
        self.real_query()
        self.event('N0USDT')
        self.assertEqual(len(self.submits), 2)

    def test_conflicting_stream_identity_cannot_overwrite_acknowledged_order(self):
        self.ordinary()
        self.event('N0USDT')
        original = deepcopy(self.trade.tracked['N0USDT'])
        for cid in ('old-client-id', original['client_order_id']):
            self.trade.client.query_exact_order = Mock(return_value=(dict(
                symbol='N0USDT', clientOrderId=cid, orderId='old-order-id',
                status='CANCELED', executedQty='0'), True))
            self.trade.reconcile_orders(now=self.now)
            self.assertEqual(self.trade.tracked['N0USDT']['client_order_id'], original['client_order_id'])
            self.assertEqual(self.trade.tracked['N0USDT']['order_id'], original['order_id'])
            self.assertTrue(self.trade.ledger.owns_client(original['client_order_id']))

    def test_wrong_order_id_in_query_cache_is_discarded(self):
        self.ordinary()
        self.event('N0USDT')
        row = self.trade.tracked['N0USDT']
        bad = dict(symbol='N0USDT', clientOrderId=row['client_order_id'],
                   orderId='old-order-id', status='CANCELED', executedQty='0')
        client = self.trade.client
        client.stream.lookup_order = Mock(return_value=None)
        client.stream.health = Mock()
        state = dict(confirmed_terminal=bad, confirmed_snapshot=bad,
                     next_query_epoch=self.now+60, failures=0, not_found=0)
        client._order_queries['c:'+row['client_order_id']] = state
        self.real_query()
        self.trade.reconcile_orders(now=self.now)
        self.assertNotIn('confirmed_terminal', state)
        self.assertNotIn('confirmed_snapshot', state)
        self.assertIn('N0USDT', self.trade.tracked)
        self.assertEqual(row['order_id'], '1')

    def test_delayed_wrong_ack_cannot_overwrite_stream_ack(self):
        self.ordinary()
        callbacks = []
        self.trade._run_network = lambda key, work, done, **kw: callbacks.append(done)
        self.event('N0USDT')
        row = self.trade.tracked['N0USDT']
        row.update(order_id='stream-confirmed-order', fate_unknown=False)
        self.trade.ledger.register(symbol='N0USDT', client_order_id=row['client_order_id'],
                                   order_id=row['order_id'], kind='entry')
        callbacks[0](dict(symbol='N0USDT', clientOrderId=row['client_order_id'],
                          orderId='wrong-old-order', status='NEW', executedQty='0'), None)
        self.assertEqual(row['order_id'], 'stream-confirmed-order')
        self.assertTrue(self.trade.ledger.owns_order('N0USDT', 'stream-confirmed-order'))
        self.assertFalse(self.trade.ledger.owns_order('N0USDT', 'wrong-old-order'))

    def test_mismatched_cancel_terminal_does_not_release_the_slot(self):
        self.ordinary()
        self.event('N0USDT')
        row = deepcopy(self.trade.tracked['N0USDT'])
        self.trade.client.cancel = Mock(return_value=dict(symbol='N0USDT',
            clientOrderId=row['client_order_id'], orderId='wrong-old-order',
            status='CANCELED', executedQty='0'))
        self.quote('N0USDT', 1.022)
        self.assertEqual(self.trade.tracked['N0USDT']['order_id'], row['order_id'])
        self.quote('N0USDT', 1.01)
        self.assertEqual(len(self.submits), 1)

    def test_pre_upgrade_unacknowledged_attempt_ignores_older_terminal(self):
        self.ordinary()
        self.submit_error = TimeoutError('ACK lost')
        self.event('N0USDT')
        row = self.trade.tracked['N0USDT']
        # Legacy reuse has the same CID, so also require temporal evidence.
        row.pop('entry_identity_version')
        old = dict(self.orders[row['client_order_id']], orderId='older-order-id')
        self.old_terminal_stream(old, stamp=int(self.now*1000)-1000)
        self.real_query()
        self.trade.reconcile_orders(now=self.now)
        self.assertIn('N0USDT', self.trade.tracked)
        self.assertEqual(row['order_id'], '')

    def test_new_send_ids_do_not_reset_confirmed_absence_retry_limit(self):
        self.ordinary()
        broker = Mock(side_effect=TimeoutError('transport uncertainty; not accepted'))
        self.trade.client.place_limit_for_bar = broker
        identities = []
        for _ in range(cfg.ENTRY_RECOVERY_MAX_RETRIES+1):
            self.event('N0USDT')
            row = self.trade.tracked['N0USDT']
            identities.append(row['client_order_id'])
            self.trade.client.query_exact_order = Mock(return_value=({}, True))
            self.trade.reconcile_orders(now=self.now)
            self.assertNotIn('N0USDT', self.trade.tracked)
        self.assertEqual(len(set(identities)), len(identities))
        self.event('N0USDT')
        self.assertNotIn('N0USDT', self.trade.tracked)
        self.assertEqual(broker.call_count, cfg.ENTRY_RECOVERY_MAX_RETRIES+1)
        self.make_trade()
        self.event('N0USDT')
        self.assertNotIn('N0USDT', self.trade.tracked)
        self.assertFalse(self.submits)

    def test_definitive_rejection_suppresses_same_plan_after_restart(self):
        self.ordinary()
        broker = Mock(side_effect=RuntimeError('Filter failure: LOT_SIZE'))
        self.trade.client.place_limit_for_bar = broker
        self.event('N0USDT')
        self.event('N0USDT')
        self.assertEqual(broker.call_count, 1)
        self.assertNotIn('N0USDT', self.trade.tracked)
        self.make_trade()
        self.event('N0USDT')
        self.assertFalse(self.submits)

    def test_rotated_out_plan_is_not_readmitted_with_a_new_send_id(self):
        self.ordinary()
        self.event('N0USDT')
        self.templates.append(dict(self.templates[0], symbol='N1USDT'))
        self.publish()
        self.quote('N1USDT', 1.006)
        self.assertNotIn('N0USDT', self.trade.tracked)
        self.quote('N1USDT', 1.006)
        self.assertIn('N1USDT', self.trade.tracked)
        self.quote('N0USDT', 1.01)
        self.assertNotIn('N0USDT', self.trade.tracked)
        self.assertEqual(len(self.submits), 2)
