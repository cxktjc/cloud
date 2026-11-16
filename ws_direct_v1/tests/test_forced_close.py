from __future__ import annotations
import sys
import time
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import Mock, patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from collectors.binance_account_stream import AccountStore
from v9711_live import config as cfg
from v9711_live.execution.runner import LiveTrade
from v9711_live.execution.client import OwnershipLedger
from v9711_live.execution.stream import StreamUnavailable


class ForcedCloseTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.root = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        for name, value in {'STATE_DIR': self.root/'state',
                'EXECUTION_INTENT_FILE': self.root/'intents.json',
                'EXECUTION_LEDGER_FILE': self.root/'ledger.json',
                'ACCOUNT_STREAM_DIR': self.root/'account'}.items():
            self.stack.enter_context(patch.object(cfg, name, value))
        self.store = AccountStore(cfg.ACCOUNT_STREAM_DIR)
        self.addCleanup(self.store.close)
        self.store.status(mode='live', ready=True, connected=True)
        self.trade = LiveTrade(enabled=False, nonblocking_io=False)
        self.trade.client.enabled = True
        self.trade.client.dry_run = False
        self.wire = self.stack.enter_context(patch.object(self.trade.client, '_broker_call',
                                                         side_effect=AssertionError('no broker calls')))
        self.now = time.time()
        self.ms = int((self.now-10)*1000)
        self.trade.positions['BULLAUSDT'] = dict(symbol='BULLAUSDT', side='LONG',
            quantity=192., entry_price=.02615, entry_epoch=self.now-1000,
            order_bar_open_ms=int((self.now-1100)*1000),
            entry_client_order_id='v9711l_entry_test', exit_due_epoch=self.now+300,
            close_client_order_id='v9711l_close_test', close_order_id='',
            close_phase='MARKET', close_quantity=192., close_status='UNKNOWN')
        self.trade.ledger.register(symbol='BULLAUSDT', client_order_id='v9711l_close_test',
                                   kind='close', qty=192.)
        self.zero()

    def zero(self, quantity='0', stamp=None):
        stamp = self.ms+1 if stamp is None else stamp
        self.store.apply({'e':'ACCOUNT_UPDATE','E':stamp,'T':stamp,
                          'a':{'m':'ORDER','P':[{'s':'BULLAUSDT','ps':'LONG','pa':quantity,'ep':'0'}]}})
        self.store.export()

    def forced(self, quantity='192', **overrides):
        order = dict(s='BULLAUSDT', ps='LONG', S='SELL', c='autoclose-test', i=9001,
                     x='CALCULATED', X='FILLED', z=quantity, l=quantity, ap='0.01962',
                     L='0.01962', t=123, rp='-1.25376', n='0', N='USDT')
        order.update(overrides)
        self.store.apply({'e':'ORDER_TRADE_UPDATE','E':self.ms,'T':self.ms,'o':order})
        self.store.export()

    def test_unknown_position_settles_from_history_once_and_survives_restart(self):
        self.forced()
        self.trade._reconcile_forced_exits(now=self.now)
        self.assertNotIn('BULLAUSDT', self.trade.positions)
        expected = 15 + 192*(.01962-.02615)-192*(.01962+.02615)*cfg.TRADE_FEE_RATE
        self.assertAlmostEqual(self.trade.ledger.state['realized_balance'], expected)
        self.assertEqual(self.trade._close_order_view(), {})
        self.assertIn('BULLAUSDT', self.trade.ledger.state['forced_close_cleanup'])
        self.assertTrue(self.trade._entry_submission_blocked({'symbol':'BULLAUSDT'}))
        self.trade.ledger = OwnershipLedger(path=cfg.EXECUTION_INTENT_FILE,
                                           state_path=cfg.EXECUTION_LEDGER_FILE)
        self.trade._reconcile_forced_exits(now=self.now+1)
        self.assertEqual(len(self.trade.ledger.state['forced_close_history']), 1)
        self.assertAlmostEqual(self.trade.ledger.state['realized_balance'], expected)
        self.wire.assert_not_called()

    def test_post_upgrade_replays_event_created_before_executor_restart(self):
        self.forced()
        # Original UNKNOWN position is checkpointed before constructing updated executor.
        self.trade.ledger.save()
        recovered = LiveTrade(enabled=False, nonblocking_io=False)
        recovered.client.enabled, recovered.client.dry_run = True, False
        recovered._reconcile_forced_exits(now=self.now)
        self.assertNotIn('BULLAUSDT', recovered.positions)
        self.assertEqual(recovered._close_order_view(), {})

    def test_zero_alone_is_not_a_forced_fill(self):
        self.trade._reconcile_forced_exits(now=self.now)
        self.assertIn('BULLAUSDT', self.trade.positions)
        self.assertEqual(self.trade.ledger.state['realized_balance'], 15)

    def test_partial_forced_evidence_does_not_erase_residual(self):
        self.forced(quantity='100')
        self.trade._reconcile_forced_exits(now=self.now)
        self.assertEqual(self.trade.positions['BULLAUSDT']['quantity'], 192)

    def test_nonzero_or_older_flat_snapshot_cannot_retire_position(self):
        self.forced()
        self.zero(quantity='12')
        self.trade._reconcile_forced_exits(now=self.now)
        self.assertIn('BULLAUSDT', self.trade.positions)

    def test_pre_entry_forced_event_is_not_reused(self):
        self.forced()
        self.trade.positions['BULLAUSDT']['entry_epoch'] = self.now
        self.trade._reconcile_forced_exits(now=self.now)
        self.assertIn('BULLAUSDT', self.trade.positions)

    def test_wrong_side_or_manual_close_not_accepted(self):
        self.forced(S='BUY')
        self.trade._reconcile_forced_exits(now=self.now)
        self.assertIn('BULLAUSDT', self.trade.positions)
        self.forced(i=9002, c='manual-order', x='TRADE')
        self.trade._reconcile_forced_exits(now=self.now)
        self.assertIn('BULLAUSDT', self.trade.positions)

    def test_account_restart_full_snapshot_absence_with_historical_forced_fill(self):
        self.forced()
        self.store.seed([], [], self.ms+500)
        self.store.export()
        self.trade._reconcile_forced_exits(now=self.now)
        self.assertNotIn('BULLAUSDT', self.trade.positions)

    def test_orphan_close_cleanup_never_recreates_a_holding(self):
        self.forced()
        self.trade._reconcile_forced_exits(now=self.now)
        self.trade.client.query_exact_order = Mock(return_value=(None, False))
        self.trade._cleanup_forced_close_orders(now=self.now)
        self.assertNotIn('BULLAUSDT', self.trade.positions)
        self.trade.client.query_exact_order.return_value = ({'status':'CANCELED'}, True)
        self.trade._cleanup_forced_close_orders(now=self.now+6)
        self.assertNotIn('BULLAUSDT', self.trade.ledger.state['forced_close_cleanup'])
        self.assertFalse(self.trade.ledger.owns_client('v9711l_close_test'))
        self.assertTrue(self.trade._entry_submission_blocked({'symbol':'BULLAUSDT'}))
        self.wire.assert_not_called()

    def manual_sell(self, quantity='192', price='0.13368', stamp=None, **overrides):
        stamp = int((self.now+0.5)*1000) if stamp is None else stamp
        order = dict(s='KITEUSDT', ps='LONG', S='SELL', c='web_manual_1', i=9101,
                     x='TRADE', X='FILLED', z=quantity, l=quantity, ap=price,
                     L=price, t=456, rp='0.063', n='0.001', N='USDT')
        order.update(overrides)
        self.store.apply({'e':'ORDER_TRADE_UPDATE','E':stamp,'T':stamp,'o':order})
        self.store.apply({'e':'ACCOUNT_UPDATE','E':stamp+1,'T':stamp+1,
                          'a':{'m':'ORDER','P':[{'s':'KITEUSDT','ps':'LONG','pa':'0','ep':'0'}]}})
        self.store.export()

    def test_manual_web_close_retires_exact_owned_position_from_stream(self):
        self.trade.positions['KITEUSDT'] = dict(symbol='KITEUSDT', side='LONG',
            quantity=38., entry_price=.132, entry_epoch=self.now-1000,
            order_bar_open_ms=int((self.now-1100)*1000),
            entry_client_order_id='v9711l_entry_kite', exit_due_epoch=self.now+300)
        self.manual_sell()
        self.trade._reconcile_manual_flats(now=self.now+1)
        self.assertNotIn('KITEUSDT', self.trade.positions)
        row = self.trade.ledger.state['manual_close_history']['v9711l_entry_kite']
        self.assertEqual(row['closed_owned_qty'],38)
        self.assertAlmostEqual(row['average_price'],.13368)
        self.assertGreater(row['booked_pnl'],0)

    def test_manual_flat_requires_actual_sell_fills_and_long_zero(self):
        self.trade.positions['KITEUSDT'] = dict(symbol='KITEUSDT', side='LONG',
            quantity=38., entry_price=.132, entry_epoch=self.now-1000,
            order_bar_open_ms=int((self.now-1100)*1000),
            entry_client_order_id='v9711l_entry_kite')
        stamp=int((self.now+.5)*1000)
        self.zero(quantity='0', stamp=stamp)
        self.trade._reconcile_manual_flats(now=self.now+1)
        self.assertIn('KITEUSDT', self.trade.positions)
        self.manual_sell(quantity='10', stamp=stamp+10)
        self.trade._reconcile_manual_flats(now=self.now+2)
        self.assertIn('KITEUSDT', self.trade.positions)

    def test_manual_sell_without_explicit_flat_does_not_retire_position(self):
        self.trade.positions['KITEUSDT'] = dict(symbol='KITEUSDT', side='LONG',
            quantity=38., entry_price=.132, entry_epoch=self.now-1000,
            order_bar_open_ms=int((self.now-1100)*1000),
            entry_client_order_id='v9711l_entry_kite')
        stamp=int((self.now+.5)*1000)
        order=dict(s='KITEUSDT', ps='LONG', S='SELL', c='web_manual_1', i=9101,
                   x='TRADE', X='FILLED', z='38', l='38', ap='.13368', L='.13368',
                   t=456, rp='.063', n='.001', N='USDT')
        self.store.apply({'e':'ORDER_TRADE_UPDATE','E':stamp,'T':stamp,'o':order})
        self.store.export()
        self.trade._reconcile_manual_flats(now=self.now+1)
        self.assertIn('KITEUSDT', self.trade.positions)

    def test_executor_own_close_fill_not_double_counted_as_manual(self):
        self.trade.positions['KITEUSDT'] = dict(symbol='KITEUSDT', side='LONG',
            quantity=38., entry_price=.132, entry_epoch=self.now-1000,
            order_bar_open_ms=int((self.now-1100)*1000),
            entry_client_order_id='v9711l_entry_kite', close_client_order_id='v9711l_close_kite')
        self.manual_sell(c='v9711l_close_kite')
        self.trade._reconcile_manual_flats(now=self.now+1)
        self.assertIn('KITEUSDT', self.trade.positions)

    def test_adl_forced_exit_is_also_recognized(self):
        self.forced(c='adl_autoclose-test', x='TRADE')
        self.trade._reconcile_forced_exits(now=self.now)
        self.assertNotIn('BULLAUSDT', self.trade.positions)

    def test_own_partial_close_is_booked_before_forced_residual(self):
        self.forced()
        self.trade.client.stream.lookup_order = Mock(return_value={
            'symbol':'BULLAUSDT', 'clientOrderId':'v9711l_close_test',
            'status':'PARTIALLY_FILLED', 'executedQty':'50', 'avgPrice':'0.025'})
        self.trade._reconcile_forced_exits(now=self.now)
        history = self.trade.ledger.state['forced_close_history']['v9711l_entry_test']
        self.assertEqual(history['closed_owned_qty'], 142)
        self.assertNotIn('BULLAUSDT', self.trade.positions)

    def test_read_failure_does_not_commit_or_remove_unknown(self):
        self.forced()
        self.trade.client.stream.forced_flat_evidence = Mock(side_effect=StreamUnavailable('DB busy'))
        self.trade._reconcile_forced_exits(now=self.now)
        self.assertEqual(self.trade.positions['BULLAUSDT']['close_status'], 'UNKNOWN')
        self.assertEqual(self.trade.ledger.state['realized_balance'], 15)

    def test_full_step_does_not_close_or_replenish_the_forced_flat_position(self):
        self.forced()
        self.trade.client.query_exact_order = Mock(return_value=(None, False))
        bar = int(self.now*1000)//cfg.BAR_MS*cfg.BAR_MS
        self.trade.desired_snapshot = Mock(return_value=({'signal_ready':True,
            'bar_open_ms':bar, 'target_positions':[], 'desired':[dict(symbol='BULLAUSDT',
            limit_price=.02, notional=5, order_bar_open_ms=bar,
            exit_due_epoch=self.now+300)]}, 'valid'))
        self.trade.refresh_prices = Mock(return_value={'BULLAUSDT':.01962})
        self.trade.write_feedback = Mock()
        self.trade._render_trade = Mock()
        self.trade._step(now=self.now)
        self.assertNotIn('BULLAUSDT', self.trade.positions)
        self.assertEqual(self.trade._entry_diagnostics['filtered']['BULLAUSDT'],
                         'submission_blocked_for_current_plan')
        self.assertEqual(self.trade._entry_diagnostics['selected_to_place'], [])
        self.assertEqual(self.trade._close_order_view(), {})
        self.wire.assert_not_called()

    def test_background_entry_gate_rejects_prequeued_replenishment(self):
        self.forced()
        self.trade._reconcile_forced_exits(now=self.now)
        self.trade.tracked['BULLAUSDT'] = dict(symbol='BULLAUSDT', client_order_id='v9711l_entry_pending')
        io = Mock()
        self.trade._network_io = io
        self.trade._publish_entry_gate()
        self.assertNotIn('v9711l_entry_pending', io.set_entry_gate.call_args.args[0]['allowed'])
        self.trade._network_io = None

    def test_stale_account_feed_cannot_retire_position(self):
        self.forced()
        self.store.status(ready=False)
        self.store.export()
        self.trade._reconcile_forced_exits(now=self.now)
        self.assertIn('BULLAUSDT', self.trade.positions)


if __name__ == '__main__':
    unittest.main()
