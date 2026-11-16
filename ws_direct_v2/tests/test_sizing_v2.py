import json
import tempfile
import unittest
from pathlib import Path
from contextlib import ExitStack
from unittest.mock import patch, Mock
from types import SimpleNamespace
from tests import test_ws_direct as fixtures
from v9711_live import config as cfg
from v9711_live.execution.sizing import weight_for_count
from v9711_live.execution.ws_direct import DirectTrade
from v9711_live.execution.startup_v2 import prepare_live_state
from v9711_live.signal_portfolio import SignalPortfolio


class SizingTests(unittest.TestCase):
    setUp = fixtures.DirectAdmissionTests.setUp
    save_plan = fixtures.DirectAdmissionTests.save_plan
    event = fixtures.DirectAdmissionTests.event

    def test_counts_zero_one_two_and_overcap_exception(self):
        self.assertEqual([weight_for_count(n) for n in range(6)], [.5,.3,.2,.2,.2,.2])
        for n, expected in ((0,50),(1,30),(2,20),(3,20)):
            self.trade.positions.clear()
            self.trade.positions.update({f'P{i}':{} for i in range(n)})
            self.trade.ledger.state['realized_balance']=100
            self.assertEqual(self.trade._entry_sizing_row(self.payload['candidate_templates'][0])['notional'],expected)

    def test_uses_trade_not_signal_capital_without_import(self):
        self.trade.ledger.state['realized_balance']=100
        self.payload['candidate_templates'][0]['notional']=999
        self.save_plan();self.event()
        self.assertEqual(self.trade.tracked['TESTUSDT']['notional'],50)

    def test_minimum_5u_and_exchange_minimum(self):
        self.trade.positions['OTHER']={}
        self.event()
        row=self.trade.tracked['TESTUSDT']
        self.assertEqual(row['notional'],4.5)
        self.assertGreaterEqual(row['submitted_notional'],5)
        self.assertEqual(row['sizing_weight'],.3)

    def test_same_tier_capital_change_does_not_resize_incumbent(self):
        self.event();old=self.trade.tracked['TESTUSDT']['client_order_id']
        self.trade.ledger.state['realized_balance']=100
        self.event()
        self.assertEqual(self.trade.tracked['TESTUSDT']['notional'],7.5)
        self.assertEqual(self.trade.tracked['TESTUSDT']['client_order_id'],old)

    def test_cancel_then_new_identity_on_count_change(self):
        self.event();old=dict(self.trade.tracked['TESTUSDT'])
        self.trade.positions['OTHER']={'quantity':1}
        self.trade.client.cancel=Mock(return_value=dict(symbol='TESTUSDT',clientOrderId=old['client_order_id'],
            orderId=old['order_id'],status='CANCELED',executedQty='0',avgPrice='0'))
        self.event();self.event()
        new=self.trade.tracked['TESTUSDT']
        self.assertNotEqual(new['client_order_id'],old['client_order_id'])
        self.assertEqual(new['notional'],4.5)
        self.trade.client.cancel.assert_called_once()
        restored=DirectTrade(enabled=False,nonblocking_io=False)
        self.assertEqual(restored.tracked['TESTUSDT']['client_order_id'],new['client_order_id'])
        self.assertEqual(restored.ledger.state['sizing_generations'],self.trade.ledger.state['sizing_generations'])

    def test_unknown_cancel_keeps_old_identity_no_replacement(self):
        self.event();old=self.trade.tracked['TESTUSDT']['client_order_id']
        self.trade.positions['OTHER']={}
        self.trade.client.cancel=Mock(side_effect=TimeoutError('lost cancel ack'))
        self.event();self.event()
        self.assertEqual(self.trade.tracked['TESTUSDT']['client_order_id'],old)
        self.assertEqual(self.trade.tracked['TESTUSDT']['notional'],7.5)
        self.assertFalse(self.trade.ledger.state.get('sizing_generations'))

    def test_partial_fill_preserves_actual_size_no_topup(self):
        self.event();row=self.trade.tracked['TESTUSDT'];oid=row['client_order_id']
        self.assertTrue(self.trade._register_fill('TESTUSDT',row,{},10,.017))
        self.assertEqual(self.trade.positions['TESTUSDT']['quantity'],10)
        self.assertEqual(self.trade.positions['TESTUSDT']['sizing_weight'],.5)
        self.trade.client.cancel=Mock(return_value=dict(symbol='TESTUSDT',clientOrderId=oid,
            orderId=row['order_id'],status='CANCELED',executedQty='10',avgPrice='.017'))
        self.event()
        self.assertEqual(self.trade.positions['TESTUSDT']['quantity'],10)
        self.assertNotIn('TESTUSDT',self.trade.tracked)

    def test_pending_close_counts_until_confirmed_flat(self):
        self.trade.positions['X']={'close_status':'UNKNOWN','quantity':10}
        self.assertEqual(self.trade._entry_sizing_row(self.payload['candidate_templates'][0])['sizing_weight'],.3)

    def test_same_batch_candidates_share_current_count_not_pending_count(self):
        self.trade.tracked['X']={'notional':7.5}
        self.assertEqual(self.trade._entry_sizing_row(self.payload['candidate_templates'][0])['sizing_weight'],.5)

    def test_capital_zero_does_not_floor_into_a_new_order(self):
        self.trade.ledger.state['realized_balance']=0
        self.event();self.assertFalse(self.trade.tracked)

    def test_worker_gate_excludes_old_tier_immediately_after_fill(self):
        self.event()
        self.trade.positions['OTHER']={}
        self.trade._network_io=Mock()
        self.trade._io_signal=(self.payload,'valid')
        self.trade._publish_entry_gate()
        gate=self.trade._network_io.set_entry_gate.call_args.args[0]
        self.assertNotIn(self.trade.tracked['TESTUSDT']['client_order_id'],gate['allowed'])
        self.trade._network_io=None


class NativeStartupTests(unittest.TestCase):
    def test_fresh_native_and_restart_preserve_capital(self):
        with tempfile.TemporaryDirectory() as d, ExitStack() as leases:
            p=Path(d)
            prepare_live_state(p,[],leases)
            file=p/'execution_ledger.json';data=json.loads(file.read_bytes())
            data['state']['realized_balance']=12.34;file.write_text(json.dumps(data))
            prepare_live_state(p,[],leases)
            self.assertEqual(json.loads(file.read_bytes())['state']['realized_balance'],12.34)

    def test_stale_v1_positions_do_not_block_or_get_adopted(self):
        with tempfile.TemporaryDirectory() as d, ExitStack() as leases:
            p=Path(d);old=p/'old';old.mkdir();dest=p/'new';dest.mkdir()
            source=old/'execution_ledger.json'
            source.write_text(json.dumps({'state':{'owned_positions':{'X':{'quantity':1}}}}))
            before=source.read_bytes()
            prepare_live_state(dest,[old],leases)
            self.assertEqual(source.read_bytes(),before)
            state=json.loads((dest/'execution_ledger.json').read_bytes())['state']
            self.assertEqual(state['owned_positions'],{})
            self.assertEqual(state['realized_balance'],cfg.EQUITY0)

    def test_paper_ledger_not_adopted(self):
        with tempfile.TemporaryDirectory() as d, ExitStack() as leases:
            p=Path(d);prepare_live_state(p,[],leases)
            f=p/'execution_intents.json';f.write_text(json.dumps({'intents':{'c:x':{'order_id':'sim-1'}}}))
            with self.assertRaises(RuntimeError):prepare_live_state(p,[],leases)

    def test_incomplete_ledger_not_reset(self):
        with tempfile.TemporaryDirectory() as d, ExitStack() as leases:
            p=Path(d);f=p/'execution_intents.json';f.write_text('{"intents":{}}')
            before=f.read_bytes()
            with self.assertRaises(RuntimeError):prepare_live_state(p,[],leases)
            self.assertEqual(f.read_bytes(),before)


class ModelSizingTests(unittest.TestCase):
    def test_three_entries_in_one_bar_are_503020_and_replay_safe(self):
        with tempfile.TemporaryDirectory() as d:
            book=SignalPortfolio(Path(d)/'signal.json')
            bar=180000*100
            symbols=['AAAUSDT','BBBUSDT','CCCUSDT','DDDUSDT']
            bars={s:[SimpleNamespace(open_time_ms=bar-180000,open=1,high=1.01,low=.98,close=1,quote_volume=1),
                     SimpleNamespace(open_time_ms=bar,open=1,high=1,low=.96,close=.99,quote_volume=1)] for s in symbols}
            with patch('v9711_live.signal_portfolio.BarStore.qv20',return_value=1):
                book.advance(bars,through_bar_open_ms=bar,symbols=symbols)
                self.assertEqual([p['notional'] for p in book.positions.values()],[7.5,4.5,3])
                self.assertEqual(len(book.positions),3)
                book.advance(bars,through_bar_open_ms=bar,symbols=symbols)
                self.assertEqual(book.state['total_entries'],3)
            loaded=SignalPortfolio(book.path)
            self.assertEqual(loaded.positions,book.positions)

