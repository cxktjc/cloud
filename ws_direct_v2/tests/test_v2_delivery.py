"""v2 sizing transitions and isolated startup failure/recovery scenarios."""
import json
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import Mock, patch

from tests import test_ws_direct as fixtures
from v9711_live import config as cfg
from v9711_live.contracts import atomic_write_json
from v9711_live.execution import startup_v2 as startup
from v9711_live.execution.ws_direct import DirectTrade


class SizingTransitions(unittest.TestCase):
    setUp = fixtures.DirectAdmissionTests.setUp
    save_plan = fixtures.DirectAdmissionTests.save_plan
    event = fixtures.DirectAdmissionTests.event

    def cancel_ack(self):
        def ack(**kwargs):
            row = self.trade.tracked[kwargs['symbol']]
            return dict(symbol=kwargs['symbol'], clientOrderId=row['client_order_id'],
                        orderId=row['order_id'], status='CANCELED', executedQty='0', avgPrice='0')
        self.trade.client.cancel = Mock(side_effect=ack)

    def test_closed_positions_step_back_to_30_then_50_without_resizing_holdings(self):
        self.trade.ledger.state['realized_balance'] = 100
        self.trade.positions.update(A=dict(quantity=1), B=dict(quantity=2))
        self.event()
        ids = [self.trade.tracked['TESTUSDT']['client_order_id']]
        self.assertEqual(self.trade.tracked['TESTUSDT']['notional'], 20)
        self.cancel_ack()
        self.trade.positions.pop('A')
        self.event()
        self.event()
        ids.append(self.trade.tracked['TESTUSDT']['client_order_id'])
        self.assertEqual(self.trade.tracked['TESTUSDT']['notional'], 30)
        self.assertEqual(self.trade.positions['B'], dict(quantity=2))
        self.trade.positions.pop('B')
        self.event()
        self.event()
        ids.append(self.trade.tracked['TESTUSDT']['client_order_id'])
        self.assertEqual(self.trade.tracked['TESTUSDT']['notional'], 50)
        self.assertEqual(len(set(ids)), 3)

    def test_fill_racing_with_resize_cancel_keeps_fill_and_never_replaces_it(self):
        self.event()
        old = dict(self.trade.tracked['TESTUSDT'])
        self.trade.positions['OTHER'] = dict(quantity=1)
        self.trade.client.cancel = Mock(return_value=dict(symbol='TESTUSDT',
            clientOrderId=old['client_order_id'], orderId=old['order_id'],
            status='CANCELED', executedQty='100', avgPrice='.017'))
        self.event()
        self.event()
        self.assertEqual(self.trade.positions['TESTUSDT']['quantity'], 100)
        self.assertEqual(self.trade.positions['TESTUSDT']['sizing_weight'], .5)
        self.assertNotIn('TESTUSDT', self.trade.tracked)
        self.assertFalse(self.trade.ledger.state.get('sizing_generations'))
        restored = DirectTrade(enabled=False, nonblocking_io=False)
        self.assertEqual(restored.positions['TESTUSDT']['quantity'], 100)

    def test_invalid_capital_does_not_invent_default_capital_or_minimum_order(self):
        for balance in (None, 'invalid', float('nan'), float('inf'), -10):
            with self.subTest(balance=balance):
                self.trade.ledger.state['realized_balance'] = balance
                row = self.trade._entry_sizing_row(self.payload['candidate_templates'][0])
                self.assertLessEqual(row['notional'], 0)


class NativeStateSafety(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.root = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        self.dest = self.root / 'v2'

    def prepare(self, sources=()):
        return startup.prepare_live_state(self.dest, sources, self.stack)

    def test_signal_and_paper_books_are_untouched_on_fresh_start_and_restart(self):
        self.dest.mkdir()
        signal = self.dest / 'signal_portfolio.json'
        signal.write_text('{"model":"kept"}')
        paper = self.root / 'paper_state'
        paper.mkdir()
        (paper / 'execution_ledger.json').write_text('paper history')
        before = signal.read_bytes()
        self.prepare()
        self.prepare()
        self.assertEqual(signal.read_bytes(), before)
        self.assertEqual((paper / 'execution_ledger.json').read_text(), 'paper history')

    def test_old_orphan_exact_intent_does_not_block_or_modify_native_v2(self):
        old = self.root / 'old'
        old.mkdir()
        for name, value in startup._initial_pair().items():
            atomic_write_json(old / name, value)
        atomic_write_json(old / 'execution_intents.json', dict(intents={
            'c:old': dict(client_order_id='old', kind='entry', order_id='123')}))
        before = {p.name: p.read_bytes() for p in old.glob('*.json')}
        self.prepare([old])
        self.assertEqual(before, {p.name: p.read_bytes() for p in old.glob('*.json')})
        self.assertEqual(json.loads((self.dest / 'execution_intents.json').read_bytes())['intents'], {})

    def test_unpaired_old_intents_do_not_block_native_startup(self):
        old = self.root / 'old'
        old.mkdir()
        atomic_write_json(old / 'execution_intents.json', dict(intents={}))
        before = (old / 'execution_intents.json').read_bytes()
        self.prepare([old])
        self.assertEqual((old / 'execution_intents.json').read_bytes(), before)

    def test_malformed_old_ledger_is_not_read_or_repaired(self):
        old = self.root / 'old'
        old.mkdir()
        ledger = old / 'execution_ledger.json'
        ledger.write_text('unreadable old ledger; leave it alone')
        before = ledger.read_bytes()
        self.prepare([old])
        self.assertEqual(ledger.read_bytes(), before)

    def test_existing_v2_balance_and_positions_survive_changed_stale_old_book(self):
        old = self.root / 'old'
        old.mkdir()
        source = old / 'execution_ledger.json'
        source.write_text('{"state":{"owned_positions":{"OLD":{"quantity":99}}}}')
        with ExitStack() as leases:
            startup.prepare_live_state(self.dest, [old], leases)
        ledger = self.dest / 'execution_ledger.json'
        payload = json.loads(ledger.read_bytes())
        payload['state'].update(realized_balance=12.34, owned_positions={
            'NATIVE': dict(quantity=2, entry_price=1, entry_client_order_id=cfg.ORDER_CLIENT_PREFIX+'_owned')})
        atomic_write_json(ledger, payload)
        before = ledger.read_bytes()
        source.write_text('old file changed after manual exchange close')
        self.prepare([old])
        self.assertEqual(ledger.read_bytes(), before)

    def test_interrupted_native_pair_initialization_resumes_exact_empty_pair(self):
        def write(path, value):
            if Path(path).name == 'execution_ledger.json':
                raise OSError('interrupted write')
            atomic_write_json(path, value)
        with patch.object(startup, 'atomic_write_json', side_effect=write):
            with self.assertRaisesRegex(OSError, 'interrupted write'):
                self.prepare()
        self.assertTrue((self.dest / startup.INIT_JOURNAL).exists())
        receipt = self.prepare()
        self.assertEqual(receipt['kind'], 'execution_native_v2')
        self.assertFalse((self.dest / startup.INIT_JOURNAL).exists())
        state = json.loads((self.dest / 'execution_ledger.json').read_bytes())['state']
        self.assertEqual(state['realized_balance'], cfg.EQUITY0)

    def test_changed_file_during_interrupted_initialization_is_never_overwritten(self):
        self.dest.mkdir()
        atomic_write_json(self.dest / startup.INIT_JOURNAL,
                          dict(version=cfg.VERSION, targets=startup._initial_pair()))
        path = self.dest / 'execution_intents.json'
        atomic_write_json(path, dict(intents={'c:owned': dict(order_id='123')}))
        before = path.read_bytes()
        with self.assertRaisesRegex(RuntimeError, 'changed v2 initialization'):
            self.prepare()
        self.assertEqual(path.read_bytes(), before)

    def test_foreign_intent_cannot_be_hidden_in_an_otherwise_empty_v2_book(self):
        self.prepare()
        path = self.dest / 'execution_intents.json'
        atomic_write_json(path, dict(intents={'c:foreign': dict(client_order_id='v9711l_old')}))
        before = path.read_bytes()
        with self.assertRaisesRegex(RuntimeError, 'foreign exact intent'):
            self.prepare()
        self.assertEqual(path.read_bytes(), before)

    def test_nonfinite_balance_stops_startup_without_reset(self):
        self.prepare()
        path = self.dest / 'execution_ledger.json'
        value = json.loads(path.read_bytes())
        value['state']['realized_balance'] = float('nan')
        path.write_text(json.dumps(value))
        before = path.read_bytes()
        with self.assertRaisesRegex(RuntimeError, 'invalid v2 realized balance'):
            self.prepare()
        self.assertEqual(path.read_bytes(), before)

    def test_manual_sync_metadata_does_not_authorize_a_foreign_exchange_identity(self):
        self.prepare()
        path = self.dest / 'execution_ledger.json'
        value = json.loads(path.read_bytes())
        value['state'].update(manual_signal_sync={'sync_id': 'manual-once'},
                              owned_positions={'X': dict(entry_client_order_id='v9711l_foreign')})
        atomic_write_json(path, value)
        before = path.read_bytes()
        with self.assertRaisesRegex(RuntimeError, 'foreign order identity'):
            self.prepare()
        self.assertEqual(path.read_bytes(), before)

    def test_unversioned_old_balance_cannot_be_adopted_as_native_v2(self):
        self.prepare()
        path = self.dest / 'execution_ledger.json'
        value = json.loads(path.read_bytes())
        value['state'].pop('strategy_version')
        value['state']['realized_balance'] = 999
        atomic_write_json(path, value)
        before = path.read_bytes()
        with self.assertRaisesRegex(RuntimeError, 'not a v2 execution ledger'):
            self.prepare()
        self.assertEqual(path.read_bytes(), before)

    def test_manual_seed_remains_compatible_with_native_restart(self):
        from v9711_live.execution.position_identity import MANUAL_ORIGIN
        self.prepare()
        path = self.dest / 'execution_ledger.json'
        value = json.loads(path.read_bytes())
        value['state'].update(manual_signal_sync={'sync_id': 'manual-once'},
            owned_positions={'X': dict(position_origin=MANUAL_ORIGIN, position_seed_id='manual-once:X')})
        atomic_write_json(path, value)
        self.prepare()
        self.assertEqual(json.loads(path.read_bytes())['state']['owned_positions'],
                         value['state']['owned_positions'])


class ManualNativeSync(unittest.TestCase):
    def test_one_time_sync_ignores_stale_old_positions_and_preserves_old_book(self):
        from v9711_live.execution import manual_sync
        from v9711_live.signal_portfolio import SignalPortfolio
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            old, dest = root / 'v1', root / 'v2'
            with ExitStack() as leases:
                startup.prepare_live_state(old, [], leases)
                path = old / 'execution_ledger.json'
                value = json.loads(path.read_bytes())
                value['state']['realized_balance'] = 123
                value['state']['owned_positions'] = {'STALE': dict(quantity=99, close_status='UNKNOWN')}
                atomic_write_json(path, value)
                startup.prepare_live_state(dest, [old], leases)
            signal = SignalPortfolio(dest / cfg.SIGNAL_BOOK_FILENAME)
            signal.state.update(last_processed_bar_open_ms=cfg.BAR_MS * 100,
                                start_bar_open_ms=cfg.BAR_MS * 100, realized_balance=21)
            atomic_write_json(signal.path, signal.state)
            before_old = path.read_bytes()
            before_signal = signal.path.read_bytes()
            plan = manual_sync.sync(dest, candidates=[old], apply=True)
            self.assertTrue(plan['applied'])
            with ExitStack() as leases:
                startup.prepare_live_state(dest, [old], leases)
            state = json.loads((dest / 'execution_ledger.json').read_bytes())['state']
            self.assertEqual(state['realized_balance'], 21)
            self.assertEqual(path.read_bytes(), before_old)
            self.assertEqual(signal.path.read_bytes(), before_signal)


class ServiceIsolation(unittest.TestCase):
    def test_systemd_units_have_independent_v2_names(self):
        units = Path(__file__).resolve().parents[1] / 'deploy/systemd'
        self.assertEqual({p.name for p in units.glob('*.service')},
                         {'v9711-ws-v2-signal.service', 'v9711-ws-v2-trade.service'})
