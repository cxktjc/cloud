"""Incremental persistence: real files, fsync barriers and abrupt recovery."""
from contextlib import ExitStack
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from v9711_live.execution.client import OwnershipLedger
from v9711_live.execution import journal_store as journal
from v9711_live.execution.startup_state import _read_pair
from v9711_live.execution.manual_sync import _read
from v9711_live.storage import SingleWriter


class JournalTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.ledger = OwnershipLedger(root/'execution_intents.json', root/'execution_ledger.json')
        self.ledger.enable_background(incremental=True)
        self.addCleanup(self.ledger.close_background)

    def load(self):
        return OwnershipLedger(self.ledger.path, self.ledger.state_path)

    def reserve(self, symbol='XUSDT'):
        with self.ledger.batch_saves():
            cid = self.ledger.reserve_entry_client_id(symbol, 180000)
            self.ledger.state['tracked_orders'][symbol] = dict(symbol=symbol, client_order_id=cid)
            self.ledger.register(symbol=symbol, client_order_id=cid, kind='entry')
        return cid, self.ledger.flush_pending()

    def test_exact_pair_visible_after_positive_receipt_and_maintenance_readers_work(self):
        cid, future = self.reserve()
        self.assertIn(('XUSDT', cid), future.result(3).reservations)
        self.assertTrue(self.load().owns_client(cid))
        self.assertEqual(self.load().state['tracked_orders']['XUSDT']['client_order_id'], cid)
        pair = _read_pair(self.ledger.path.parent)
        self.assertIn('c:'+cid, json.loads(pair['execution_intents.json'])['intents'])
        self.assertIn('XUSDT', _read(self.ledger.state_path)['state']['tracked_orders'])

    def test_changed_branches_only_and_external_mutable_aliases_are_observed(self):
        padding = '0123456789abcdef' * 65536
        self.ledger.state['large_unchanged_audit'] = {'blob': padding}
        row = {'status': 'NEW', 'nested': {'values': [1, 2]}}
        self.ledger.state['retained_row'] = row
        self.ledger.flush()
        row['nested']['values'].append(3)
        row['status'] = 'FILLED'
        self.ledger.flush()
        self.assertLess(self.ledger._background.health()['record_bytes'], 1024)
        self.assertEqual(self.load().state['retained_row'], row)
        self.assertEqual(self.load().state['large_unchanged_audit']['blob'], padding)

    def test_older_bundle_reader_rejects_journal_marker_instead_of_using_seed(self):
        marker = json.loads(self.ledger.state_path.read_text())
        self.assertEqual(marker['execution_bundle_version'], 2)
        with self.assertRaises(RuntimeError):
            journal.embedded_intents(marker)
        self.ledger.close_background()
        self.assertEqual(json.loads(self.ledger.state_path.read_text())['execution_bundle_version'], 1)

    def test_pending_deltas_are_not_overwritten_and_deletes_restore_exactly(self):
        entered, release = threading.Event(), threading.Event()
        original = journal.commit_journal
        def blocked(*args):
            entered.set()
            self.assertTrue(release.wait(3))
            return original(*args)
        with patch.object(journal, 'commit_journal', side_effect=blocked):
            _, first = self.reserve('XUSDT')
            self.assertTrue(entered.wait(2))
            _, second = self.reserve('YUSDT')
            self.ledger.state['tracked_orders'].pop('XUSDT')
            self.ledger.state['test_delete'] = {'a': 1}
            self.ledger.save()
            self.ledger.state.pop('test_delete')
            self.ledger.save()
            last = self.ledger.flush_pending()
            self.assertFalse(first.done())
            self.assertFalse(second.done())
            release.set()
            last.result(3)
        self.assertNotIn('XUSDT', self.load().state['tracked_orders'])
        self.assertIn('YUSDT', self.load().state['tracked_orders'])
        self.assertNotIn('test_delete', self.load().state)

    def test_disk_failure_never_grants_receipt_and_poisoned_writer_cannot_append(self):
        with patch.object(journal, 'commit_journal', side_effect=OSError('simulated disk failure')):
            _, future = self.reserve()
            with self.assertRaises(OSError):
                future.result(3)
        self.assertFalse(self.load().state['tracked_orders'])
        with self.assertRaises(OSError):
            self.ledger.flush_for_dispatch().result(1)
        # The close path still joins both threads after a failed flush.
        self.ledger._background.close()
        self.ledger._background_closed = True

    def test_checkpoint_rotation_and_gc_can_reconstruct_current_state(self):
        writer = self.ledger._background
        writer._checkpoint_every = 2
        writer._segment_bytes = 300
        for value in range(35):
            self.ledger.state['revision'] = value
            self.ledger.state['history'] = {'last': value, 'blob': 'x'*200}
            self.ledger.flush()
        deadline = time.monotonic() + 3
        while writer.health()['checkpoint_sequence'] < 34 and time.monotonic() < deadline:
            time.sleep(.002)
        self.assertGreaterEqual(writer.health()['checkpoint_sequence'], 34)
        self.assertEqual(self.load().state['revision'], 34)
        segments = list(self.ledger.path.parent.glob('*.journal.*.[0-9]*'))
        self.assertLess(len(segments), 6)

    def test_missing_or_corrupt_checkpoint_stops_without_using_seed_marker(self):
        self.reserve()[1].result(3)
        checkpoint = self.ledger._background._checkpoint_path
        raw = checkpoint.read_bytes()
        try:
            checkpoint.write_text('{}')
            with self.assertRaises(RuntimeError):
                self.load()
        finally:
            checkpoint.write_bytes(raw)

    def test_checksum_failure_of_complete_record_is_not_treated_as_partial_tail(self):
        self.reserve()[1].result(3)
        path = self.ledger._background._segment_path
        raw = path.read_bytes()
        corrupted = bytearray(raw)
        corrupted[-1] ^= 1
        try:
            path.write_bytes(corrupted)
            with self.assertRaises(RuntimeError):
                self.load()
        finally:
            path.write_bytes(raw)

    def test_corrupt_sequence_in_checkpoint_covered_prefix_is_not_silently_skipped(self):
        self.reserve()[1].result(3)
        self.ledger.state['revision'] = 2
        self.ledger.flush()
        writer = self.ledger._background
        journal._checkpoint(writer._checkpoint_path, writer._epoch, writer._sequence, writer._root)
        path = writer._segment_path
        raw = path.read_bytes()
        offset = journal._SEGMENT.size
        header = list(journal._FRAME.unpack(raw[offset:offset+journal._FRAME.size]))
        header[2] = 0  # Would otherwise be mistaken for an already covered frame.
        corrupted = raw[:offset] + journal._FRAME.pack(*header) + raw[offset+journal._FRAME.size:]
        try:
            path.write_bytes(corrupted)
            with self.assertRaises(RuntimeError):
                self.load()
        finally:
            path.write_bytes(raw)

    def test_actual_abrupt_exit_and_partial_tail_recover_before_native_restart(self):
        root = self.ledger.path.parent/'abrupt'
        code = '''
import os, sys
from pathlib import Path
from v9711_live.execution.client import OwnershipLedger
from v9711_live import config as cfg
root = Path(sys.argv[1])
ledger = OwnershipLedger(root/'execution_intents.json', root/'execution_ledger.json')
ledger.state['strategy_version'] = cfg.VERSION
ledger.enable_background(incremental=True)
with ledger.batch_saves():
    cid = ledger.reserve_entry_client_id('XUSDT', 180000)
    ledger.state['tracked_orders']['XUSDT'] = dict(symbol='XUSDT', client_order_id=cid)
    ledger.register(symbol='XUSDT', client_order_id=cid, kind='entry')
ledger.flush_pending().result(3)
path = ledger._background._segment_path
with path.open('ab') as handle:
    handle.write(b'V9711J')
    handle.flush()
    os.fsync(handle.fileno())
print(cid, flush=True)
os._exit(0)
'''
        result = subprocess.run([sys.executable, '-B', '-c', code, str(root)],
            capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        restored = OwnershipLedger(root/'execution_intents.json', root/'execution_ledger.json')
        self.assertTrue(restored.owns_client(result.stdout.strip()))
        from v9711_live.execution.startup_v2 import prepare_live_state
        with ExitStack() as leases:
            leases.enter_context(SingleWriter(root/'trade.lock'))
            self.assertTrue(prepare_live_state(root, [], leases)['completed'])
        restored.enable_background(incremental=True)
        restored.close_background()
        self.assertNotIn('execution_journal_store', json.loads(restored.state_path.read_text()))
        self.assertTrue(OwnershipLedger(restored.path, restored.state_path).owns_client(result.stdout.strip()))

    def test_real_exit_mid_append_preserves_previous_receipt_and_rejects_incomplete_entry(self):
        root = self.ledger.path.parent/'torn'
        code = '''
import os, sys
from pathlib import Path
from v9711_live.execution.client import OwnershipLedger
from v9711_live.execution import journal_store
root = Path(sys.argv[1])
ledger = OwnershipLedger(root/'execution_intents.json', root/'execution_ledger.json')
ledger.enable_background(incremental=True)
def reserve(symbol):
    with ledger.batch_saves():
        cid = ledger.reserve_entry_client_id(symbol, 180000)
        ledger.state['tracked_orders'][symbol] = dict(symbol=symbol, client_order_id=cid)
        ledger.register(symbol=symbol, client_order_id=cid, kind='entry')
    return ledger.flush_pending()
reserve('XUSDT').result(3)
def torn(handle, records):
    handle.write(records[:journal_store._FRAME.size+40])
    os.fsync(handle.fileno())
    os._exit(0)
journal_store.commit_journal = torn
reserve('YUSDT').result(3)
'''
        result = subprocess.run([sys.executable, '-B', '-c', code, str(root)],
            capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        restored = OwnershipLedger(root/'execution_intents.json', root/'execution_ledger.json')
        self.assertIn('XUSDT', restored.state['tracked_orders'])
        self.assertNotIn('YUSDT', restored.state['tracked_orders'])

    def test_failed_checkpoints_retain_segments_and_bounded_journal_eventually_blocks_dispatch(self):
        writer = self.ledger._background
        writer._checkpoint_every = 1
        with patch.object(journal, '_checkpoint', side_effect=OSError('checkpoint disk unavailable')):
            self.reserve()[1].result(3)
            deadline = time.monotonic()+2
            while not writer.health()['checkpoint_error'] and time.monotonic() < deadline:
                time.sleep(.001)
            self.assertIn('checkpoint disk unavailable', writer.health()['checkpoint_error'])
            self.assertIn('XUSDT', self.load().state['tracked_orders'])
            writer._max_journal_bytes = writer.health()['retained_bytes'] + 20
            _, future = self.reserve('YUSDT')
            with self.assertRaises(OSError):
                future.result(3)
        self.assertNotIn('YUSDT', self.load().state['tracked_orders'])
        writer.close()
        self.ledger._background_closed = True

    def test_checkpoint_encoding_does_not_hold_writer_or_mutate_shared_root(self):
        writer = self.ledger._background
        writer._checkpoint_every = 1
        entered, release = threading.Event(), threading.Event()
        original = journal._checkpoint
        def blocked(*args):
            entered.set()
            self.assertTrue(release.wait(3))
            return original(*args)
        with patch.object(journal, '_checkpoint', side_effect=blocked):
            self.ledger.state['nested'] = {'value': 1}
            self.ledger.flush()
            self.assertTrue(entered.wait(2))
            try:
                self.ledger.state['nested']['value'] = 2
                self.ledger.flush()
                self.assertEqual(self.load().state['nested']['value'], 2)
            finally:
                release.set()

    def test_ordinary_appends_do_not_wake_idle_checkpoint_thread(self):
        writer = self.ledger._background
        with patch.object(writer._checkpoint_event, 'set', wraps=writer._checkpoint_event.set) as wake:
            for value in range(4):
                self.ledger.state['value'] = value
                self.ledger.flush()
            wake.assert_not_called()

    def test_shutdown_with_pending_checkpoint_joins_both_threads(self):
        writer = self.ledger._background
        writer._checkpoint_every = 1
        entered, release = threading.Event(), threading.Event()
        original = journal._checkpoint
        def blocked(*args):
            entered.set()
            self.assertTrue(release.wait(3))
            return original(*args)
        with patch.object(journal, '_checkpoint', side_effect=blocked):
            self.ledger.state['value'] = 1
            self.ledger.flush()
            self.assertTrue(entered.wait(2))
            self.ledger.state['value'] = 2
            self.ledger.flush()
            closer = threading.Thread(target=writer.close, daemon=True)
            closer.start()
            release.set()
            closer.join(3)
            self.assertFalse(closer.is_alive())
        self.ledger._background_closed = True
        self.assertEqual(self.load().state['value'], 2)


if __name__ == '__main__':
    unittest.main()
