"""One-file exact commits, crash recovery and legacy/tool compatibility."""
import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from v9711_live.execution import durable_writer as module
from v9711_live.execution.client import OwnershipLedger
from v9711_live.execution.startup_state import _read_pair


class AtomicBundleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.ledger = OwnershipLedger(self.root/'execution_intents.json', self.root/'execution_ledger.json')
        self.ledger.flush()
        self.ledger.enable_background(atomic_bundle=True)
        self.addCleanup(self.ledger.close_background)

    def reserve(self):
        with self.ledger.batch_saves():
            cid = self.ledger.reserve_entry_client_id('XUSDT', 180000)
            self.ledger.state['tracked_orders']['XUSDT'] = dict(symbol='XUSDT', client_order_id=cid)
            self.ledger.register(symbol='XUSDT', client_order_id=cid, kind='entry')
        return cid, self.ledger.flush_pending()

    def test_one_atomic_write_certifies_both_identity_and_slot_and_stale_export_cannot_hide_them(self):
        with patch.object(module, 'atomic_write_text', wraps=module.atomic_write_text) as write:
            cid, receipt = self.reserve()
            self.assertIn(('XUSDT', cid), receipt.result(2).reservations)
            self.assertEqual([call.args[0] for call in write.call_args_list], [self.ledger.state_path])
        self.assertFalse(json.loads(self.ledger.path.read_text())['intents'])
        restored = OwnershipLedger(self.ledger.path, self.ledger.state_path)
        self.assertTrue(restored.owns_client(cid))
        self.assertEqual(restored.state['tracked_orders']['XUSDT']['client_order_id'], cid)
        pair = _read_pair(self.root)
        self.assertIn('c:'+cid, json.loads(pair['execution_intents.json'])['intents'])

    def test_failure_does_not_certify_and_previous_bundle_remains_recoverable(self):
        cid, receipt = self.reserve()
        receipt.result(2)
        before = self.ledger.state_path.read_bytes()
        with patch.object(module, 'atomic_write_text', side_effect=OSError('fsync failed')):
            self.ledger.state['realized_balance'] = 123
            with self.assertRaises(OSError):
                self.ledger.flush_for_dispatch().result(2)
            self.assertEqual(self.ledger.state_path.read_bytes(), before)
            self.assertTrue(OwnershipLedger(self.ledger.path, self.ledger.state_path).owns_client(cid))
        self.ledger.flush()

    def test_synchronous_flush_exports_legacy_intents_and_close_is_idempotent(self):
        cid, receipt = self.reserve()
        receipt.result(2)
        self.ledger.flush()
        self.assertIn('c:'+cid, json.loads(self.ledger.path.read_text())['intents'])
        self.ledger.close_background()
        self.ledger.close_background()

    def test_old_export_cannot_resurrect_an_identity_removed_in_the_bundle(self):
        cid, receipt = self.reserve()
        receipt.result(2)
        self.ledger.flush()
        self.ledger.state['tracked_orders'].pop('XUSDT')
        self.ledger.forget(symbol='XUSDT', client_order_id=cid)
        self.ledger.flush_pending().result(2)
        self.assertIn('c:'+cid, json.loads(self.ledger.path.read_text())['intents'])
        restored = OwnershipLedger(self.ledger.path, self.ledger.state_path)
        self.assertFalse(restored.owns_client(cid))
        self.assertFalse(restored.state['tracked_orders'])

    def test_invalid_bundle_never_falls_back_to_a_stale_legacy_file(self):
        self.ledger.close_background()
        self.ledger.state_path.write_text(json.dumps(dict(execution_bundle_version=1, state={}, intents=None)))
        with self.assertRaises(RuntimeError):
            OwnershipLedger(self.ledger.path, self.ledger.state_path)
        with self.assertRaises(RuntimeError):
            _read_pair(self.root)
