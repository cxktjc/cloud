import hashlib
import json
import sys
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from v9711_live.execution.startup_state import prepare_live_state, FILES, AUTO_KIND
from v9711_live.storage import SingleWriter

class StartupStateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.source, self.dest = self.root/'old', self.root/'new'
        self.source.mkdir(); self.dest.mkdir()
        self.write_pair(self.source)
        self.signal = self.dest/'signal_portfolio.json'
        self.signal.write_bytes(b'{"new_signal_book":true}')
        self.before = self.signal.read_bytes()

    def write_pair(self, directory, quantity=192):
        (directory/FILES[0]).write_text(json.dumps({'intents':{'c:exact':{'kind':'close', 'order_id':'123'}}}))
        (directory/FILES[1]).write_text(json.dumps({'state':{'tracked_orders':{},
            'owned_positions':{'BULLAUSDT':{'quantity':quantity,'close_status':'UNKNOWN'}}}}))

    def test_auto_takeover_preserves_signal_and_exact_unknown_state(self):
        before = {name:(self.source/name).read_bytes() for name in FILES}
        with ExitStack() as leases:
            result = prepare_live_state(self.dest,[self.source],leases)
            self.assertTrue(result['completed'])
            with self.assertRaises(RuntimeError):
                with SingleWriter(self.source/'trade.lock'): pass
        for name, raw in before.items():
            self.assertEqual((self.dest/name).read_bytes(),raw)
            self.assertEqual((self.source/name).read_bytes(),raw)
        self.assertEqual(self.signal.read_bytes(),self.before)

    def test_native_valid_execution_pair_does_not_need_manual_marker(self):
        self.write_pair(self.dest)
        with ExitStack() as leases:
            result=prepare_live_state(self.dest,[],leases)
        self.assertEqual(result['mode'],'existing_native')
        self.assertEqual(self.signal.read_bytes(),self.before)

    def test_previous_receipt_relocks_old_trade_on_restart(self):
        with ExitStack() as leases:
            prepare_live_state(self.dest,[self.source],leases)
        with SingleWriter(self.source/'trade.lock'), ExitStack() as leases:
            with self.assertRaisesRegex(RuntimeError,'another process'):
                prepare_live_state(self.dest,[self.source],leases)

    def test_running_legacy_trade_prevents_any_copy(self):
        with SingleWriter(self.source/'trade.lock'), ExitStack() as leases:
            with self.assertRaisesRegex(RuntimeError,'another process'):
                prepare_live_state(self.dest,[self.source],leases)
        self.assertFalse((self.dest/FILES[0]).exists())

    def test_conflicting_sources_fail_without_writing(self):
        other=self.root/'other'; other.mkdir(); self.write_pair(other,quantity=99)
        with ExitStack() as leases:
            with self.assertRaisesRegex(RuntimeError,'conflicting legacy'):
                prepare_live_state(self.dest,[self.source,other],leases)
        self.assertFalse((self.dest/FILES[0]).exists())

    def test_conflicting_destination_is_never_overwritten(self):
        self.write_pair(self.dest,quantity=99)
        before=(self.dest/FILES[1]).read_bytes()
        with ExitStack() as leases:
            with self.assertRaisesRegex(RuntimeError,'conflicts'):
                prepare_live_state(self.dest,[self.source],leases)
        self.assertEqual((self.dest/FILES[1]).read_bytes(),before)

    def test_partial_destination_pair_is_never_overwritten(self):
        (self.dest/FILES[0]).write_bytes((self.source/FILES[0]).read_bytes())
        with ExitStack() as leases:
            with self.assertRaisesRegex(RuntimeError,'incomplete execution'):
                prepare_live_state(self.dest,[self.source],leases)
        self.assertFalse((self.dest/FILES[1]).exists())

    def test_paper_directory_is_never_a_source(self):
        paper=self.root/'paper_state'; paper.mkdir(); self.write_pair(paper)
        with ExitStack() as leases:
            with self.assertRaisesRegex(RuntimeError,'no real execution'):
                prepare_live_state(self.dest,[paper],leases)

    def test_simulated_identity_is_never_imported(self):
        path=self.source/FILES[0]
        path.write_text(path.read_text().replace('123','sim-123'))
        with ExitStack() as leases:
            with self.assertRaisesRegex(RuntimeError,'simulated order history'):
                prepare_live_state(self.dest,[self.source],leases)
        self.assertFalse((self.dest/FILES[0]).exists())

    def test_dry_run_feedback_is_never_imported(self):
        (self.source/'execution_feedback.json').write_text('{"mode":"dry_run"}')
        with ExitStack() as leases:
            with self.assertRaisesRegex(RuntimeError,'dry-run execution'):
                prepare_live_state(self.dest,[self.source],leases)

    def test_interrupted_own_journal_resumes_matching_bytes(self):
        from v9711_live.execution import startup_state as startup
        original=startup._complete
        with patch.object(startup,'_complete',side_effect=OSError('crash')), ExitStack() as leases:
            with self.assertRaises(OSError):
                prepare_live_state(self.dest,[self.source],leases)
        self.assertTrue((self.dest/'.migration_in_progress').exists())
        with ExitStack() as leases:
            result=prepare_live_state(self.dest,[self.source],leases)
        self.assertTrue(result['completed'])
        self.assertFalse((self.dest/'.migration_in_progress').exists())
        self.assertEqual(self.signal.read_bytes(),self.before)

    def test_foreign_incomplete_journal_is_not_removed(self):
        path=self.dest/'.migration_in_progress'; path.write_text('{"kind":"other"}')
        with ExitStack() as leases:
            with self.assertRaisesRegex(RuntimeError,'unrecognized incomplete'):
                prepare_live_state(self.dest,[self.source],leases)
        self.assertTrue(path.exists())

    def test_missing_history_never_uses_signal_as_exchange_position(self):
        with ExitStack() as leases:
            with self.assertRaisesRegex(RuntimeError,'signal model positions are not proof'):
                prepare_live_state(self.dest,[],leases)
        self.assertEqual(self.signal.read_bytes(),self.before)

    def test_real_cli_prepares_state_before_constructing_executor(self):
        import logging
        from unittest.mock import Mock
        from v9711_live import config as cfg
        from v9711_live.execution import runner, ws_direct, startup_state
        with ExitStack() as stack:
            for name, value in {'STATE_DIR':self.dest,
                    'TRADE_LOCK_FILE':self.dest/'trade.lock',
                    'PRICE_FILE':self.dest/'latest.json'}.items():
                stack.enter_context(patch.object(cfg,name,value))
            stack.enter_context(patch.object(sys,'argv',['trade','--enable-live','--no-dry-run']))
            stack.enter_context(patch.object(runner.logging,'FileHandler',return_value=logging.NullHandler()))
            stack.enter_context(patch.object(startup_state,'legacy_locations',return_value=[self.source]))
            def construct(**kwargs):
                self.assertTrue((self.dest/'WS_MIGRATION.json').exists())
                self.assertEqual((self.dest/FILES[1]).read_bytes(),(self.source/FILES[1]).read_bytes())
                self.assertEqual(self.signal.read_bytes(),self.before)
                return Mock(run=Mock(return_value=0))
            stack.enter_context(patch.object(ws_direct,'DirectTrade',side_effect=construct))
            self.assertEqual(runner.main(),0)

if __name__=='__main__': unittest.main()
