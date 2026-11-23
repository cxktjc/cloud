"""Actual fsync receipts, alternating-file recovery and maintenance readers."""
import json
import tempfile
import subprocess
import sys
from contextlib import ExitStack
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from v9711_live.execution import durable_writer as module
from v9711_live.execution.client import OwnershipLedger
from v9711_live.execution.startup_state import _read_pair
from v9711_live.execution.manual_sync import _read
from v9711_live import config as cfg


class DispatchSlotsTests(unittest.TestCase):
    def test_offline_migration_exports_current_complete_bundle_without_slot_dependency(self):
        from scripts.migrate_state import migrate
        cid, ticket = self.reserve()
        ticket.result(2)
        with tempfile.TemporaryDirectory() as destination:
            root = Path(destination)
            migrate(self.ledger.path.parent, root, apply=True)
            self.assertNotIn('execution_dispatch_store', json.loads((root/'execution_ledger.json').read_text()))
            self.assertFalse(any(path.exists() for path in module.store_paths(root/'execution_ledger.json')))
            self.assertTrue(OwnershipLedger(root/'execution_intents.json', root/'execution_ledger.json').owns_client(cid))

    def test_admission_group_commits_all_slots_once_and_cannot_release_early(self):
        before = self.writer.health()['submitted']
        tickets, identities = [], []
        with self.ledger.dispatch_group():
            for symbol in ('XUSDT', 'YUSDT', 'ZUSDT'):
                cid, ticket = self.reserve(symbol)
                tickets.append(ticket)
                identities.append((symbol, cid))
                self.assertFalse(ticket.done())
                self.assertEqual(self.writer.health()['submitted'], before)
        self.assertTrue(all(ticket is tickets[0] for ticket in tickets))
        self.assertEqual(self.writer.health()['submitted'], before+1)
        receipt = tickets[0].result(2)
        self.assertTrue(set(identities) <= receipt.reservations)
        restored = self.load()
        self.assertTrue(all(restored.owns_client(cid) for _, cid in identities))

    def test_group_failure_is_visible_to_every_ticket_without_a_positive_receipt(self):
        with patch.object(module, 'commit_snapshot_slot', side_effect=OSError('group disk failure')):
            with self.ledger.dispatch_group():
                _, one = self.reserve('XUSDT')
                _, two = self.reserve('YUSDT')
            self.assertIs(one, two)
            with self.assertRaises(OSError):
                one.result(2)
            self.assertFalse(self.load().state['tracked_orders'])
        self.ledger.flush()

    def test_failed_initial_seed_releases_writer_and_retry_can_initialize(self):
        self.ledger.close_background()
        ledger = self.load()
        self.addCleanup(ledger.close_background)
        with patch.object(module, 'commit_snapshot_slot', side_effect=OSError('initial disk error')):
            with self.assertRaises(OSError):
                ledger.enable_background(persistent_slots=True)
        self.assertIsNone(ledger._background)
        self.assertNotIn('execution_dispatch_store', json.loads(ledger.state_path.read_text()))
        ledger.enable_background(persistent_slots=True)
        ledger.flush()
        self.assertTrue(ledger._background.health()['persistent_slots'])

    def test_abrupt_process_exit_recovers_exact_entry_and_close_without_shutdown_exports(self):
        root = self.ledger.path.parent/'abrupt'
        code = '''
import os, sys
from pathlib import Path
from v9711_live.execution.client import OwnershipLedger
from v9711_live import config as cfg
root = Path(sys.argv[1])
ledger = OwnershipLedger(root/'execution_intents.json', root/'execution_ledger.json')
ledger.state['strategy_version'] = cfg.VERSION
ledger.flush()
ledger.enable_background(persistent_slots=True)
with ledger.batch_saves():
    entry = ledger.reserve_entry_client_id('XUSDT', 180000)
    ledger.state['tracked_orders']['XUSDT'] = dict(symbol='XUSDT', client_order_id=entry)
    ledger.register(symbol='XUSDT', client_order_id=entry, kind='entry')
    close = cfg.ORDER_CLIENT_PREFIX+'_clq_abrupt'
    ledger.state['owned_positions']['YUSDT'] = dict(symbol='YUSDT', quantity=1, close_client_order_id=close,
        entry_client_order_id=cfg.ORDER_CLIENT_PREFIX+'_entry_before_crash')
    ledger.register(symbol='YUSDT', client_order_id=close, kind='close')
receipt = ledger.flush_pending().result(3)
assert {('XUSDT', entry), ('YUSDT', close)} <= receipt.reservations
print(entry, flush=True)
os._exit(0)
'''
        result = subprocess.run([sys.executable, '-B', '-c', code, str(root)],
            capture_output=True, text=True, timeout=10, cwd=Path(__file__).resolve().parents[1])
        self.assertEqual(result.returncode, 0, result.stderr)
        cid = result.stdout.strip()
        self.assertIn('execution_dispatch_store', json.loads((root/'execution_ledger.json').read_text()))
        restored = OwnershipLedger(root/'execution_intents.json', root/'execution_ledger.json')
        self.assertTrue(restored.owns_client(cid))
        close = cfg.ORDER_CLIENT_PREFIX+'_clq_abrupt'
        self.assertTrue(restored.owns_client(close))
        self.assertEqual(restored.state['tracked_orders']['XUSDT']['client_order_id'], cid)
        self.assertEqual(restored.state['owned_positions']['YUSDT']['close_client_order_id'], close)
        pair = _read_pair(root)
        self.assertIn('c:'+cid, json.loads(pair['execution_intents.json'])['intents'])
        from v9711_live.execution.startup_v2 import prepare_live_state
        from v9711_live.storage import SingleWriter
        with ExitStack() as leases:
            leases.enter_context(SingleWriter(root/'trade.lock'))
            self.assertTrue(prepare_live_state(root, [], leases)['completed'])
        restored.enable_background(persistent_slots=True)
        restored.close_background()
        self.assertTrue(OwnershipLedger(restored.path, restored.state_path).owns_client(cid))

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.ledger = OwnershipLedger(root/'execution_intents.json', root/'execution_ledger.json')
        self.ledger.flush()
        self.ledger.enable_background(persistent_slots=True)
        self.writer = self.ledger._background
        self.addCleanup(self.ledger.close_background)

    def load(self):
        return OwnershipLedger(self.ledger.path, self.ledger.state_path)

    def reserve(self, symbol='XUSDT'):
        with self.ledger.batch_saves():
            cid = self.ledger.reserve_entry_client_id(symbol, 180000)
            self.ledger.state['tracked_orders'][symbol] = dict(symbol=symbol, client_order_id=cid)
            self.ledger.register(symbol=symbol, client_order_id=cid, kind='entry')
        return cid, self.ledger.flush_pending()

    def test_reuses_open_files_without_rename_and_readers_see_current_exact_reservation(self):
        handles = tuple(handle.fileno() for handle in self.writer._handles)
        marker = self.ledger.state_path.read_bytes()
        with patch.object(module, 'atomic_write_text', wraps=module.atomic_write_text) as write:
            cid, receipt = self.reserve()
            self.assertIn(('XUSDT', cid), receipt.result(2).reservations)
            self.assertFalse(write.called)
        self.assertEqual(marker, self.ledger.state_path.read_bytes())
        self.assertEqual(handles, tuple(handle.fileno() for handle in self.writer._handles))
        self.assertTrue(self.load().owns_client(cid))
        self.assertEqual(_read(self.ledger.state_path)['state']['tracked_orders']['XUSDT']['client_order_id'], cid)
        pair = _read_pair(self.ledger.path.parent)
        self.assertIn('c:'+cid, json.loads(pair['execution_intents.json'])['intents'])

    def test_no_positive_receipt_before_slot_sync_returns(self):
        started, release = threading.Event(), threading.Event()
        original = module.commit_snapshot_slot
        def blocked(handle, record):
            started.set()
            if not release.wait(2):
                raise AssertionError('test write not released')
            original(handle, record)
        try:
            with patch.object(module, 'commit_snapshot_slot', side_effect=blocked):
                cid, receipt = self.reserve()
                self.assertTrue(started.wait(1))
                self.assertFalse(receipt.done())
                self.assertFalse(self.load().owns_client(cid))
                release.set()
                self.assertIn(('XUSDT', cid), receipt.result(2).reservations)
        finally:
            release.set()

    def test_torn_inactive_write_keeps_previous_acknowledged_snapshot(self):
        first, receipt = self.reserve()
        receipt.result(2)
        def torn(handle, record):
            handle.seek(0)
            handle.write(record[:module._SLOT.size+7])
            raise OSError('power lost during inactive-slot write')
        with patch.object(module, 'commit_snapshot_slot', side_effect=torn):
            second, receipt = self.reserve('YUSDT')
            with self.assertRaises(OSError):
                receipt.result(2)
            restored = self.load()
            self.assertTrue(restored.owns_client(first))
            self.assertFalse(restored.owns_client(second))
        self.ledger.flush()
        self.assertTrue(self.load().owns_client(second))

    def test_mismatched_epoch_and_missing_slots_never_fall_back_to_seed_body(self):
        marker = self.ledger.state_path.read_bytes()
        payload = json.loads(marker)
        payload['execution_dispatch_store'] = '0'*32
        self.ledger.state_path.write_text(json.dumps(payload), encoding='utf-8')
        try:
            with self.assertRaisesRegex(RuntimeError, 'no intact'):
                self.load()
            with self.assertRaises(RuntimeError):
                _read_pair(self.ledger.path.parent)
        finally:
            self.ledger.state_path.write_bytes(marker)

    def test_graceful_close_exports_standalone_bundle_and_restart_changes_epoch_safely(self):
        cid, receipt = self.reserve()
        receipt.result(2)
        epoch = self.writer._epoch
        self.ledger.close_background()
        self.assertNotIn('execution_dispatch_store', json.loads(self.ledger.state_path.read_text()))
        restored = self.load()
        self.assertTrue(restored.owns_client(cid))
        restored.enable_background(persistent_slots=True)
        try:
            self.assertNotEqual(restored._background._epoch, epoch)
            self.assertTrue(self.load().owns_client(cid))
        finally:
            restored.close_background()

    def test_partial_open_failure_closes_handles_and_can_retry_without_issuing_receipt(self):
        self.ledger.close_background()
        writer = module.SlotWriter(self.ledger.path, self.ledger.state_path)
        self.addCleanup(writer.close)
        original = Path.open
        opened = []
        def broken(path, *args, **kwargs):
            if str(path).endswith('.dispatch1'):
                raise OSError('second slot unavailable')
            handle = original(path, *args, **kwargs)
            if str(path).endswith('.dispatch0'):
                opened.append(handle)
            return handle
        snapshot = module.Snapshot('{}', '{"tracked_orders":{}}', frozenset())
        with patch.object(Path, 'open', broken):
            with self.assertRaises(OSError):
                writer.submit(snapshot).result(2)
        self.assertTrue(opened[0].closed)
        self.assertEqual(writer._handles, [])
        writer.submit(snapshot).result(2)
        self.assertEqual(len(writer._handles), 2)
