"""Delayed entry booking must not hide a later exchange manual exit."""
import unittest

from tests import test_forced_close as fixtures
from v9711_live.execution.order_identity import exact_entry_fill_time_ms


class ManualCloseTimingTests(unittest.TestCase):
    setUp = fixtures.ForcedCloseTests.setUp
    manual_sell = fixtures.ForcedCloseTests.manual_sell
    zero = fixtures.ForcedCloseTests.zero

    def position(self):
        self.trade.positions.clear()
        self.trade.positions['KITEUSDT'] = dict(symbol='KITEUSDT', side='LONG',
            quantity=38., entry_price=.132, entry_epoch=self.now,
            order_bar_open_ms=int((self.now-300)*1000), entry_order_id=8888,
            entry_client_order_id='v9712l_entry_kite', exit_due_epoch=self.now+300)

    def buy(self, cid='v9712l_entry_kite'):
        stamp = int((self.now-30)*1000)
        self.store.apply(dict(e='ORDER_TRADE_UPDATE', E=stamp, T=stamp, o=dict(
            s='KITEUSDT', ps='LONG', S='BUY', c=cid, i=8888, x='TRADE', X='FILLED',
            q='38', z='38', l='38', ap='.132', L='.132', t=455)))
        self.store.export()
        return stamp

    def test_legacy_delayed_booking_recovers_only_exact_entry_time(self):
        self.position()
        actual = self.buy()
        self.manual_sell(quantity='38', stamp=int((self.now-10)*1000))
        self.trade._reconcile_manual_flats(now=self.now)
        self.assertNotIn('KITEUSDT', self.trade.positions)
        history = self.trade.ledger.state['manual_close_history']['v9712l_entry_kite']
        self.assertEqual(history['recovered_entry_fill_time_ms'], actual)
        self.assertLess(history['corrected_since_ms'], history['previous_since_ms'])
        balance = self.trade.ledger.state['realized_balance']
        self.trade._reconcile_manual_flats(now=self.now+3)
        self.assertEqual(self.trade.ledger.state['realized_balance'], balance)
        self.wire.assert_not_called()

    def test_new_delayed_fill_books_exchange_time_before_manual_close(self):
        self.trade.positions.clear()
        actual = self.buy()
        order = self.trade.client.stream.lookup_order(symbol='KITEUSDT', order_id=8888,
                                                      client_order_id='v9712l_entry_kite')
        row = dict(symbol='KITEUSDT', client_order_id='v9712l_entry_kite', order_id=8888,
                   order_bar_open_ms=int((self.now-300)*1000))
        self.assertTrue(self.trade._register_fill('KITEUSDT', row, order, 38., .132))
        self.assertEqual(self.trade.positions['KITEUSDT']['entry_fill_time_ms'], actual)
        self.manual_sell(quantity='38', stamp=int((self.now-10)*1000))
        self.trade._reconcile_manual_flats(now=self.now)
        self.assertNotIn('KITEUSDT', self.trade.positions)
        self.wire.assert_not_called()

    def test_foreign_entry_cannot_move_legacy_time_lower_bound(self):
        self.position()
        self.buy(cid='some_other_entry')
        self.manual_sell(quantity='38', stamp=int((self.now-10)*1000))
        self.trade._reconcile_manual_flats(now=self.now)
        self.assertIn('KITEUSDT', self.trade.positions)
        self.assertFalse(self.trade.ledger.state.get('manual_close_history'))
        self.wire.assert_not_called()

    def test_sync_fence_cannot_be_weakened_by_older_exact_fill(self):
        self.position()
        self.buy()
        self.trade.positions['KITEUSDT']['external_evidence_since_ms'] = int(self.now*1000)
        self.manual_sell(quantity='38', stamp=int((self.now-10)*1000))
        self.trade._reconcile_manual_flats(now=self.now)
        self.assertIn('KITEUSDT', self.trade.positions)
        self.wire.assert_not_called()

    def test_pending_diagnostics_distinguish_flat_before_time_cutoff(self):
        self.position()
        self.manual_sell(quantity='38', stamp=int((self.now-10)*1000))
        info = {}
        evidence = self.trade.client.stream.manual_flat_evidence(symbol='KITEUSDT',
            since_ms=int(self.now*1000), owned_qty=38., diagnostics=info)
        self.assertIsNone(evidence)
        self.assertEqual(info['reason'], 'flat_before_entry_lower_bound')
        self.assertEqual(info['account_quantity'], 0)

    def test_canceled_update_time_and_foreign_fill_do_not_become_entry_time(self):
        reply = dict(symbol='KITEUSDT', clientOrderId='owned', orderId=1,
                     status='CANCELED', executedQty=38, updateTime=123456)
        self.assertEqual(exact_entry_fill_time_ms(reply, symbol='KITEUSDT',
            client_order_id='owned', allow_update_time=True), 0)
        reply['_user_fills'] = [dict(symbol='KITEUSDT', clientOrderId='foreign', orderId=2,
                                     side='BUY', time=1000, qty=38, price=.132)]
        self.assertEqual(exact_entry_fill_time_ms(reply, symbol='KITEUSDT',
            client_order_id='owned', order_id=1), 0)
