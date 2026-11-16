"""Offline one-shot ledger imports. All paths are temporary; no broker I/O."""
from contextlib import ExitStack
import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from v9711_live import config as cfg
from v9711_live.contracts import atomic_write_json
from v9711_live.signal_portfolio import SignalPortfolio
from v9711_live.storage import SingleWriter
from v9711_live.execution import manual_sync as ms
from v9711_live.execution.client import OwnershipLedger
from v9711_live.execution.runner import LiveTrade
from v9711_live.execution.position_identity import CAPITAL_MODE, MANUAL_ORIGIN
from v9711_live.execution.session_entries import SessionEntryStats
from v9711_live.execution.startup_state import prepare_live_state


class ManualSyncTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.state = self.root/'state'
        self.state.mkdir()
        self.now = time.time()
        self.bar = int(self.now*1000)//cfg.BAR_MS*cfg.BAR_MS - cfg.BAR_MS
        self.signal = SignalPortfolio(self.state/cfg.SIGNAL_BOOK_FILENAME)
        self.signal.state.update(last_processed_bar_open_ms=self.bar,
            start_bar_open_ms=self.bar, revision=4, realized_balance=14.9)
        self.signal.positions['TESTUSDT'] = self.model_position('TESTUSDT')
        atomic_write_json(self.signal.path, self.signal.state)
        atomic_write_json(self.state/'execution_intents.json', dict(schema_version=1, component='execution', intents={}))
        self.old_state = dict(owned_positions={'KITEUSDT': dict(symbol='KITEUSDT', side='LONG',
            quantity=38, entry_price=.13, entry_client_order_id='v9712l_entry_old')},
            tracked_orders={}, realized_balance=12.3, reopen_until_epoch={},
            forced_close_history={'bulla':dict(symbol='BULLAUSDT', booked_pnl=-1.2)})
        self.save_old()
        atomic_write_json(self.state/'execution_feedback.json', dict(mode='live', realized_balance=12.3))
        self.originals = {name:(self.state/name).read_bytes() for name in ms.BACKUP_FILES if (self.state/name).exists()}

    def model_position(self, symbol):
        return dict(symbol=symbol, side='LONG', position_id=f'model:{symbol}:{self.bar}',
            entry_bar_open_ms=self.bar, reference_bar_open_ms=self.bar-cfg.BAR_MS,
            entry_available_ms=self.bar+cfg.BAR_MS, entry_price=2., quantity=2.5, notional=5.,
            exit_bar_open_ms=self.bar+3*cfg.BAR_MS, exit_due_ms=self.bar+4*cfg.BAR_MS,
            exit_due_epoch=(self.bar+4*cfg.BAR_MS)/1000)

    def save_old(self):
        atomic_write_json(self.state/'execution_ledger.json', dict(schema_version=1, component='execution', state=self.old_state))

    def read_state(self):
        return json.loads((self.state/'execution_ledger.json').read_text())['state']

    def sync(self, **kwargs):
        return ms.sync(self.state, candidates=[], **kwargs)

    def assert_original(self):
        for name, raw in self.originals.items():
            self.assertEqual((self.state/name).read_bytes(), raw, name)

    def test_preview_never_writes_accounting_or_backups(self):
        plan = self.sync()
        self.assertFalse(plan['applied'])
        self.assertEqual(plan['realized_balance_after'], 14.9)
        self.assertEqual(set(plan['positions_after']), {'TESTUSDT'})
        self.assert_original()
        self.assertFalse((self.state/'execution_backups').exists())
        self.assertFalse((self.state/'WS_MIGRATION.json').exists())

    def test_apply_imports_funds_positions_and_due_without_modifying_signal(self):
        plan = self.sync(apply=True)
        state = self.read_state()
        self.assertEqual(state['realized_balance'], 14.9)
        self.assertEqual(state['book_initial_equity'], 15.)
        self.assertEqual(state['capital_mode'], CAPITAL_MODE)
        self.assertEqual(set(state['owned_positions']), {'TESTUSDT'})
        pos = state['owned_positions']['TESTUSDT']
        self.assertEqual(pos['quantity'], 2.5)
        self.assertEqual(pos['entry_price'], 2.)
        self.assertEqual(pos['exit_due_epoch'], self.signal.positions['TESTUSDT']['exit_due_epoch'])
        self.assertEqual(pos['position_origin'], MANUAL_ORIGIN)
        self.assertIsNone(pos['entry_client_order_id'])
        self.assertEqual(pos['entry_identity_basis'], 'operator_seed_not_exchange_order')
        self.assertGreaterEqual(pos['external_evidence_since_ms'], int(self.now*1000))
        self.assertEqual((self.state/cfg.SIGNAL_BOOK_FILENAME).read_bytes(), self.originals[cfg.SIGNAL_BOOK_FILENAME])
        self.assertEqual(state['forced_close_history'], self.old_state['forced_close_history'])
        self.assertEqual(json.loads((self.state/'execution_intents.json').read_text())['intents'], {})
        self.assertTrue(plan['applied'])
        self.assertFalse((self.state/ms.JOURNAL).exists())
        feedback = json.loads((self.state/'execution_feedback.json').read_text())
        self.assertEqual(feedback['realized_balance'], 14.9)
        self.assertEqual(feedback['positions']['TESTUSDT']['quantity'], 2.5)
        self.assertEqual(feedback['status'], 'manual_sync_applied_waiting_for_trade')
        for name, raw in self.originals.items():
            self.assertEqual((Path(plan['backup'])/name).read_bytes(), raw, name)

    def test_price_override_preserves_notional_and_recalculates_quantity(self):
        plan = self.sync(prices={'TESTUSDT':1.25}, apply=True)
        pos = self.read_state()['owned_positions']['TESTUSDT']
        self.assertEqual(pos['entry_price'], 1.25)
        self.assertEqual(pos['quantity'], 4.)
        self.assertEqual(pos['notional'], 5.)
        self.assertEqual(plan['manual_prices'], {'TESTUSDT':1.25})

    def test_invalid_or_unselected_price_is_refused_without_accounting_writes(self):
        for prices in ({'TESTUSDT':float('nan')}, {'TESTUSDT':0}, {'ABSENTUSDT':1}):
            with self.assertRaises(ValueError):
                self.sync(prices=prices, apply=True)
            self.assert_original()

    def test_price_parser_requires_unique_positive_prices(self):
        self.assertEqual(ms.parse_prices(['testusdt=2.3']), {'TESTUSDT':2.3})
        for values in (['TESTUSDT=nan'], ['TESTUSDT=inf'], ['TESTUSDT=-1'], ['wrong'], ['A=1','a=2']):
            with self.assertRaises(ValueError):
                ms.parse_prices(values)

    def test_over_cap_requires_explicit_symbols_instead_of_silent_truncation(self):
        for index in range(cfg.SLOTS):
            symbol = f'OTHER{index}USDT'
            self.signal.positions[symbol] = self.model_position(symbol)
        atomic_write_json(self.signal.path, self.signal.state)
        with self.assertRaisesRegex(ValueError, 'use --symbol'):
            self.sync(apply=True)
        plan = self.sync(symbols=['TESTUSDT'], apply=True)
        self.assertEqual(set(self.read_state()['owned_positions']), {'TESTUSDT'})
        self.assertEqual(len(plan['excluded_signal_symbols']), cfg.SLOTS)

    def test_empty_signal_clears_old_book_only_on_explicit_apply(self):
        self.signal.positions.clear()
        atomic_write_json(self.signal.path, self.signal.state)
        self.sync(apply=True)
        self.assertEqual(self.read_state()['owned_positions'], {})
        self.assertEqual(self.read_state()['realized_balance'], 14.9)

    def test_unknown_close_is_never_overwritten(self):
        self.old_state['owned_positions']['KITEUSDT'].update(close_client_order_id='v9712l_close_old', close_status='UNKNOWN')
        self.save_old()
        before = (self.state/'execution_ledger.json').read_bytes()
        with self.assertRaisesRegex(ValueError, 'UNKNOWN close'):
            self.sync(apply=True)
        self.assertEqual((self.state/'execution_ledger.json').read_bytes(), before)

    def test_pending_entry_or_rotation_is_refused(self):
        for field, value in (('tracked_orders', {'TESTUSDT':{'fate_unknown':True}}), ('pending_rotation', {'victim':'TESTUSDT'})):
            old = copy.deepcopy(self.old_state)
            self.old_state[field] = value
            self.save_old()
            with self.assertRaisesRegex(ValueError, 'entry/rotation pending'):
                self.sync(apply=True)
            self.old_state = old

    def test_orphan_exact_order_intent_is_preserved_and_refused(self):
        path = self.state/'execution_intents.json'
        raw = json.dumps({'intents':{'c:unknown':{'client_order_id':'unknown'}}}).encode()
        path.write_bytes(raw)
        with self.assertRaisesRegex(ValueError, 'unresolved exact-order intent'):
            self.sync(apply=True)
        self.assertEqual(path.read_bytes(), raw)

    def test_unrelated_forced_close_cleanup_is_preserved_without_becoming_a_holding(self):
        self.old_state['forced_close_cleanup'] = {'BULLAUSDT':dict(client_order_id='v9712l_close_bulla')}
        self.save_old()
        atomic_write_json(self.state/'execution_intents.json', {'intents':{
            'c:v9712l_close_bulla':dict(client_order_id='v9712l_close_bulla', kind='close')}})
        self.sync(apply=True)
        self.assertIn('BULLAUSDT', self.read_state()['forced_close_cleanup'])
        self.assertNotIn('BULLAUSDT', self.read_state()['owned_positions'])
        self.assertIn('c:v9712l_close_bulla', json.loads((self.state/'execution_intents.json').read_text())['intents'])

    def test_same_symbol_old_close_cleanup_blocks_import(self):
        self.old_state['forced_close_cleanup'] = {'TESTUSDT':dict(client_order_id='v9712l_close_unknown')}
        self.save_old()
        with self.assertRaisesRegex(ValueError, 'unresolved old close'):
            self.sync(apply=True)

    def test_signal_or_trade_lock_conflict_prevents_writes(self):
        for name in ('signal.lock', 'trade.lock'):
            with SingleWriter(self.state/name):
                with self.assertRaisesRegex(RuntimeError, 'another process'):
                    self.sync(apply=True)
            self.assert_original()

    def test_legacy_writer_conflict_prevents_writes(self):
        legacy = self.root/'legacy'
        with SingleWriter(legacy/'trade.lock'):
            with self.assertRaisesRegex(RuntimeError, 'another process'):
                ms.sync(self.state, candidates=[legacy], apply=True)
        self.assert_original()

    def test_all_legacy_writers_are_locked_after_normal_live_restart(self):
        sources = [self.root/'legacy1', self.root/'legacy2']
        for path in sources:
            path.mkdir()
            for name in ms.FILES[:2]:
                (path/name).write_bytes((self.state/name).read_bytes())
        plan = ms.sync(self.state, candidates=sources, apply=True)
        state_bytes = (self.state/'execution_ledger.json').read_bytes()
        with ExitStack() as leases:
            receipt = prepare_live_state(self.state, sources, leases)
            self.assertEqual(receipt['kind'], ms.KIND)
            self.assertEqual(receipt['sync_id'], self.read_state()['manual_signal_sync']['sync_id'])
            for path in sources:
                with self.assertRaisesRegex(RuntimeError, 'another process'):
                    with SingleWriter(path/'trade.lock'):
                        pass
        self.assertEqual((self.state/'execution_ledger.json').read_bytes(), state_bytes)
        for index, path in enumerate(sources):
            for name in ms.FILES[:2]:
                self.assertEqual((Path(plan['backup'])/f'legacy/{index}'/name).read_bytes(), (path/name).read_bytes())

    def test_conflicting_unknown_legacy_book_is_not_overwritten_or_hidden(self):
        legacy = self.root/'legacy'
        legacy.mkdir()
        (legacy/'execution_intents.json').write_text('{"intents":{}}')
        (legacy/'execution_ledger.json').write_text('{"state":{"owned_positions":{},"tracked_orders":{},"realized_balance":9}}')
        with self.assertRaisesRegex(ValueError, 'conflicting legacy execution ledger'):
            ms.sync(self.state, candidates=[legacy], apply=True)
        self.assert_original()
        self.assertFalse((self.state/'execution_backups').exists())

    def test_existing_takeover_receipt_allows_unchanged_legacy_book_with_old_orders(self):
        legacy = self.root/'legacy'
        legacy.mkdir()
        (legacy/'execution_intents.json').write_text('{"intents":{"c:old":{"client_order_id":"old","kind":"entry"}}}')
        (legacy/'execution_ledger.json').write_text('{"state":{"owned_positions":{},"tracked_orders":{"X":{"last_status":"UNKNOWN"}}}}')
        hashes = {name:hashlib.sha256((legacy/name).read_bytes()).hexdigest() for name in ms.FILES[:2]}
        atomic_write_json(self.state/'WS_MIGRATION.json', dict(completed=True, source=str(legacy.resolve()), source_sha256=hashes))
        ms.sync(self.state, candidates=[legacy], apply=True)
        with ExitStack() as leases:
            receipt = prepare_live_state(self.state, [legacy], leases)
            self.assertEqual(receipt['kind'], ms.KIND)
        self.assertEqual(json.loads((legacy/'execution_ledger.json').read_text())['state']['tracked_orders']['X']['last_status'], 'UNKNOWN')

    def test_partial_or_simulated_live_pair_is_refused(self):
        path = self.state/'execution_ledger.json'
        self.old_state['owned_positions']['KITEUSDT']['entry_order_id'] = 'sim-123'
        self.save_old()
        with self.assertRaisesRegex(ValueError, 'simulated execution history'):
            self.sync(apply=True)
        path.unlink()
        with self.assertRaisesRegex(ValueError, 'incomplete execution ledger pair'):
            self.sync(apply=True)

    def test_explicit_reimport_is_once_again_but_normal_restart_never_reimports(self):
        self.sync(apply=True)
        first_id = self.read_state()['manual_signal_sync']['sync_id']
        state = self.read_state()
        state['realized_balance'] = 16.5
        state['owned_positions'] = {}
        atomic_write_json(self.state/'execution_ledger.json', {'state':state})
        self.signal.state['realized_balance'] = 50.
        atomic_write_json(self.signal.path, self.signal.state)
        with ExitStack() as leases:
            prepare_live_state(self.state, [], leases)
        self.assertEqual(self.read_state()['realized_balance'], 16.5)
        self.assertEqual(self.read_state()['owned_positions'], {})
        self.assertEqual(self.read_state()['manual_signal_sync']['sync_id'], first_id)
        second = self.sync(apply=True)
        self.assertEqual(self.read_state()['realized_balance'], 50.)
        self.assertNotEqual(self.read_state()['manual_signal_sync']['sync_id'], first_id)
        self.assertTrue(Path(second['backup']).exists())

    def test_changed_legacy_ledger_after_sync_is_not_silently_accepted(self):
        legacy = self.root/'legacy'
        legacy.mkdir()
        for name in ms.FILES[:2]:
            (legacy/name).write_bytes((self.state/name).read_bytes())
        ms.sync(self.state, candidates=[legacy], apply=True)
        (legacy/'execution_ledger.json').write_text('{"state":{}}')
        with ExitStack() as leases:
            with self.assertRaisesRegex(RuntimeError, 'changed after manual sync'):
                prepare_live_state(self.state, [legacy], leases)

    def test_interrupted_apply_blocks_startup_and_resumes_exact_targets(self):
        real = ms._atomic_bytes
        def fail_feedback(path, raw):
            if path.name == 'execution_feedback.json':
                raise OSError('interrupted write')
            real(path, raw)
        with patch.object(ms, '_atomic_bytes', side_effect=fail_feedback):
            with self.assertRaisesRegex(OSError, 'interrupted write'):
                self.sync(apply=True)
        self.assertTrue((self.state/ms.JOURNAL).exists())
        with ExitStack() as leases:
            with self.assertRaisesRegex(RuntimeError, 'sync interrupted'):
                prepare_live_state(self.state, [], leases)
        with patch.object(cfg, 'STATE_DIR', self.state):
            with self.assertRaisesRegex(RuntimeError, 'sync interrupted'):
                LiveTrade(enabled=False)
        preview = self.sync()
        self.assertFalse(preview['applied'])
        self.assertTrue(preview['resumed'])
        self.assertTrue((self.state/ms.JOURNAL).exists())
        plan = self.sync(apply=True)
        self.assertTrue(plan['applied'])
        self.assertTrue(plan['resumed'])
        self.assertFalse((self.state/ms.JOURNAL).exists())
        self.assertEqual(self.read_state()['realized_balance'], 14.9)
        self.assertEqual((self.state/cfg.SIGNAL_BOOK_FILENAME).read_bytes(), self.originals[cfg.SIGNAL_BOOK_FILENAME])
        with ExitStack() as leases:
            self.assertEqual(prepare_live_state(self.state, [], leases)['kind'], ms.KIND)

    def test_interrupted_apply_refuses_unrelated_execution_edits(self):
        with patch.object(ms, '_atomic_bytes', side_effect=OSError('interrupted')):
            with self.assertRaises(OSError):
                self.sync(apply=True)
        (self.state/'execution_ledger.json').write_text('{"state":{"realized_balance":999}}')
        before = (self.state/'execution_ledger.json').read_bytes()
        with self.assertRaisesRegex(ValueError, 'execution file changed'):
            self.sync(apply=True)
        self.assertEqual((self.state/'execution_ledger.json').read_bytes(), before)

    def test_imported_position_is_not_a_fabricated_fill_for_session_stats(self):
        self.sync(apply=True)
        tracker = SessionEntryStats(started_at=self.bar/1000)
        tracker.observe_owned_state(self.read_state()['owned_positions'], {})
        tracker.observe_signal(dict(signal_ready=True, signal_session_id='model', portfolio={},
            target_positions=[self.signal.positions['TESTUSDT']]), status='valid', now=self.now)
        self.assertEqual(tracker.snapshot()['trade_filled_entries'], 0)

    def test_after_import_signal_changes_do_not_replace_trade_and_close_books_own_pnl(self):
        self.sync(apply=True)
        with ExitStack() as patches:
            for name, value in dict(STATE_DIR=self.state,
                EXECUTION_INTENT_FILE=self.state/'execution_intents.json',
                EXECUTION_LEDGER_FILE=self.state/'execution_ledger.json',
                EXECUTION_FEEDBACK_FILE=self.state/'execution_feedback.json').items():
                patches.enter_context(patch.object(cfg, name, value))
            trade = LiveTrade(enabled=False, nonblocking_io=False)
            owned_before = copy.deepcopy(trade.positions)
            for balance in (1000., 4.):
                trade._apply_signal_targets(dict(signal_ready=True, signal_session_id='new-model',
                    signal_revision=99, target_positions=[], portfolio={'realized_balance':balance}))
                self.assertEqual(trade.positions, owned_before)
                self.assertEqual(trade.ledger.state['realized_balance'], 14.9)
            pos = trade.positions['TESTUSDT']
            pos.update(close_client_order_id='v9712l_close_test', close_order_id='999',
                close_quantity=pos['quantity'], close_phase='MARKET', close_accounted_qty=0., close_accounted_quote=0.)
            trade.ledger.register(symbol='TESTUSDT', client_order_id='v9712l_close_test', order_id='999', kind='close', qty=pos['quantity'])
            trade._settle_close_confirmation('TESTUSDT', dict(status='FILLED', executedQty=2.5, avgPrice=2.2), now=self.now+1)
            expected = 14.9 + (2.2-2.)*2.5 - (2.2+2.)*2.5*cfg.TRADE_FEE_RATE
            self.assertAlmostEqual(trade.ledger.state['realized_balance'], expected)
            self.assertNotIn('TESTUSDT', trade.positions)
            restarted = OwnershipLedger(path=cfg.EXECUTION_INTENT_FILE, state_path=cfg.EXECUTION_LEDGER_FILE)
            self.assertAlmostEqual(restarted.state['realized_balance'], expected)
            self.assertEqual(restarted.state['owned_positions'], {})

    def test_adopted_manual_flat_uses_only_post_sync_account_evidence(self):
        self.sync(apply=True)
        with ExitStack() as patches:
            for name, value in dict(STATE_DIR=self.state, EXECUTION_INTENT_FILE=self.state/'execution_intents.json',
                                   EXECUTION_LEDGER_FILE=self.state/'execution_ledger.json').items():
                patches.enter_context(patch.object(cfg, name, value))
            trade = LiveTrade(enabled=False, nonblocking_io=False)
            trade.client.enabled, trade.client.dry_run = True, False
            stream = Mock()
            stream.health.return_value.last_event_seq = 2
            stream.manual_flat_evidence.return_value = dict(average_price=2.1, event_time_ms=int((self.now+1)*1000))
            trade.client.stream = stream
            sync_ms = trade.positions['TESTUSDT']['external_evidence_since_ms']
            trade._reconcile_manual_flats(now=self.now+2)
            self.assertEqual(stream.manual_flat_evidence.call_args.kwargs['since_ms'], sync_ms)
            self.assertEqual(trade.positions, {})
            self.assertTrue(any(key.startswith('manual:') for key in trade.ledger.state['manual_close_history']))

    def test_adopted_forced_flat_is_supported_without_fake_exchange_entry_id(self):
        self.sync(apply=True)
        with ExitStack() as patches:
            for name, value in dict(STATE_DIR=self.state, EXECUTION_INTENT_FILE=self.state/'execution_intents.json',
                                   EXECUTION_LEDGER_FILE=self.state/'execution_ledger.json').items():
                patches.enter_context(patch.object(cfg, name, value))
            trade = LiveTrade(enabled=False, nonblocking_io=False)
            trade.client.enabled, trade.client.dry_run = True, False
            stream = Mock()
            stream.health.return_value.last_event_seq = 2
            stream.forced_flat_evidence.return_value = dict(average_price=1.5, event_time_ms=int((self.now+1)*1000))
            trade.client.stream = stream
            trade._reconcile_forced_exits(now=self.now+2)
            self.assertEqual(trade.positions, {})
            audit = next(value for key,value in trade.ledger.state['forced_close_history'].items() if key.startswith('manual:'))
            self.assertIsNone(audit['entry_client_order_id'])
            self.assertEqual(audit['position_origin'], MANUAL_ORIGIN)


class ManualSyncEntrypointTests(unittest.TestCase):
    def test_runner_sync_flag_applies_once_and_exits_without_client_or_trade(self):
        from v9711_live.execution import runner
        with patch.object(sys, 'argv', ['trade', '--sync-signal-ledger', '--sync-price', 'TESTUSDT=1.25',
                                      '--sync-symbol', 'TESTUSDT']), \
             patch('v9711_live.execution.manual_sync.sync', return_value={'applied':True}) as sync, \
             patch.object(runner, 'ExchangeClient') as client, \
             patch('v9711_live.execution.ws_direct.DirectTrade') as trade, \
             patch('builtins.print'):
            self.assertEqual(runner.main(), 0)
        sync.assert_called_once_with(cfg.STATE_DIR, prices={'TESTUSDT':1.25},
                                     symbols=['TESTUSDT'], apply=True, paper=False)
        client.assert_not_called()
        trade.assert_not_called()

    def test_cli_default_only_previews_and_never_connects_to_exchange(self):
        from scripts import sync_signal_to_trade as cli
        with patch.object(sys, 'argv', ['sync']), \
             patch.object(cli, 'sync', return_value={'applied':False}) as sync, \
             patch('builtins.print'):
            self.assertEqual(cli.main(), 0)
        sync.assert_called_once_with(cfg.STATE_DIR, prices={}, symbols=None, apply=False, paper=False)


if __name__ == '__main__':
    unittest.main()
