"""Causal checks: new entries precede cancel/feedback I/O; ownership persists."""
import io
import json
import logging
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from tests import test_flash_speed_v2 as fixtures
from v9711_live import config as cfg
from v9711_live.execution import client as client_module, feedback_writer as feedback_module
from v9711_live.execution.async_logs import AsyncTradeLogs
from v9711_live.execution.client import OwnershipLedger
from v9711_live.execution.feedback_writer import FeedbackWriter
from v9711_live.execution.nonblocking_io import NetworkIO
from v9711_live.execution.stream import Health, StreamUnavailable


class BatchDurabilityTests(unittest.TestCase):
    def test_grouped_registration_is_durable_before_dispatch_and_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ledger = OwnershipLedger(root/'intents', root/'state')
            with patch.object(client_module, 'atomic_write_json', wraps=client_module.atomic_write_json) as write:
                with ledger.batch_saves():
                    cid = ledger.reserve_entry_client_id('XUSDT', 180000)
                    ledger.state['tracked_orders']['XUSDT'] = {'client_order_id': cid}
                    ledger.register(symbol='XUSDT', client_order_id=cid, kind='entry')
                    write.assert_not_called()
                    ledger.flush()
                    self.assertEqual(write.call_count, 2)
                self.assertEqual(write.call_count, 2)
            restored = OwnershipLedger(ledger.path, ledger.state_path)
            self.assertTrue(restored.owns_client(cid))
            self.assertEqual(restored.state['tracked_orders']['XUSDT']['client_order_id'], cid)
            self.assertNotEqual(restored.reserve_entry_client_id('XUSDT', 180000), cid)


class EntryFirstTests(unittest.TestCase):
    setUp = fixtures.FlashOwnerTests.setUp
    plans, event = fixtures.FlashOwnerTests.plans, fixtures.FlashOwnerTests.event
    place, cancel = fixtures.FlashOwnerTests.place, fixtures.FlashOwnerTests.cancel
    target, make_trade = fixtures.FlashOwnerTests.target, fixtures.FlashOwnerTests.make_trade
    refresh_quotes = fixtures.FlashOwnerTests.refresh_quotes
    publish, held = fixtures.FlashOwnerTests.publish, fixtures.FlashOwnerTests.held

    def test_backlog_defers_presentation_but_does_not_starve_it(self):
        with patch.object(self.trade.market.events, 'qsize', return_value=100), \
                patch('time.monotonic', return_value=100):
            self.assertTrue(self.trade._presentation_due())
            self.assertFalse(self.trade._presentation_due())
        with patch.object(self.trade.market.events, 'qsize', return_value=100), \
                patch('time.monotonic', return_value=101):
            self.assertTrue(self.trade._presentation_due())
        self.assertEqual(self.trade._deferred_presentations, 1)

    def test_presentation_deferral_keeps_entries_close_checks_and_durable_state(self):
        self.plans(1)
        with patch.object(self.trade, '_presentation_due', return_value=False), \
                patch.object(self.trade, '_comparison', side_effect=AssertionError('presentation delayed')), \
                patch.object(self.trade, '_timed_exits', wraps=self.trade._timed_exits) as closes, \
                patch.object(self.trade, 'write_feedback') as feedback:
            self.trade.step(now=self.now)
        closes.assert_called_once()
        feedback.assert_not_called()
        self.assertIn('N0USDT', self.trade.tracked)
        restored = OwnershipLedger(self.trade.ledger.path, self.trade.ledger.state_path)
        self.assertTrue(restored.owns_client(self.trade.tracked['N0USDT']['client_order_id']))

    def test_maintenance_coalesces_multiple_fill_saves(self):
        self.plans(3)
        self.event('N0USDT', 1.01)
        for row in self.trade.tracked.values():
            self.orders[row['client_order_id']].update(status='FILLED', executedQty=row['qty'], avgPrice='1')
        with patch.object(client_module, 'atomic_write_json', wraps=client_module.atomic_write_json) as write:
            self.trade.step(now=self.now)
        self.assertEqual(len(self.trade.positions), 3)
        self.assertEqual(write.call_count, 2)
        restored = OwnershipLedger(self.trade.ledger.path, self.trade.ledger.state_path)
        self.assertEqual(len(restored.state['owned_positions']), 3)

    def test_full_book_sends_after_one_grouped_barrier_before_cancel_calls(self):
        self.plans(8)
        self.event('N0USDT', 1.01)
        trace, writes = [], []
        original_write = client_module.atomic_write_json
        def write(path, payload):
            writes.append(path)
            original_write(path, payload)
        def place(**kwargs):
            restored = OwnershipLedger(self.trade.ledger.path, self.trade.ledger.state_path)
            self.assertTrue(restored.owns_client(kwargs['client_order_id']))
            self.assertEqual(restored.state['tracked_orders']['N8USDT']['client_order_id'], kwargs['client_order_id'])
            self.assertTrue(restored.state['tracked_orders']['N7USDT']['cancel_requested'])
            trace.append(('entry', len(writes)))
            return self.place(**kwargs)
        def cancel(**kwargs):
            trace.append(('cancel', len(writes)))
            return self.cancel(**kwargs)
        self.trade.client.place_limit_for_bar = Mock(side_effect=place)
        self.trade.client.cancel = Mock(side_effect=cancel)
        self.plans(9)
        with patch.object(client_module, 'atomic_write_json', side_effect=write):
            self.event('N8USDT', .99)
        self.assertEqual(trace[0], ('entry', 2))
        self.assertEqual(trace[1][0], 'cancel')

    def test_failed_durability_barrier_never_dispatches_new_order(self):
        self.plans(1)
        original_write = client_module.atomic_write_json
        def fail_state(path, payload):
            if path == self.trade.ledger.state_path:
                raise OSError('disk durability unavailable')
            original_write(path, payload)
        with patch.object(client_module, 'atomic_write_json', side_effect=fail_state):
            with self.assertRaises(OSError):
                self.event('N0USDT', .99)
        self.assertFalse(self.submits)
        self.trade.ledger.flush()
        restored = OwnershipLedger(self.trade.ledger.path, self.trade.ledger.state_path)
        row = restored.state['tracked_orders']['N0USDT']
        self.assertTrue(restored.owns_client(row['client_order_id']))

    def test_blocked_feedback_disk_does_not_block_next_actual_entry(self):
        self.plans(1)
        started, release, sent = threading.Event(), threading.Event(), threading.Event()
        original_write = feedback_module.atomic_write_text
        def slow_feedback(path, payload):
            started.set()
            if not release.wait(3):
                raise AssertionError('test feedback writer was not released')
            original_write(path, payload)
        def place(**kwargs):
            result = self.place(**kwargs)
            if kwargs['symbol'] == 'N1USDT':
                sent.set()
            return result
        self.trade.client.place_limit_for_bar = Mock(side_effect=place)
        with patch.object(feedback_module, 'atomic_write_text', side_effect=slow_feedback):
            self.trade.enable_nonblocking_io()
            try:
                self.trade.step(now=self.now)
                self.assertTrue(started.wait(1))
                self.plans(2)
                self.event('N1USDT', .99)
                # A prior ACK wake may be ahead of this quote, as in production.
                while not self.trade.market.events.empty():
                    event = self.trade.market.events.get_nowait()
                    if event is None:
                        self.trade._drain_network()
                    else:
                        self.trade.market.decide(event, self.trade.on_tick)
                self.assertTrue(sent.wait(.5), 'entry waited for blocked feedback disk I/O')
                self.assertFalse(release.is_set())
            finally:
                release.set()
                self.trade.shutdown_io()

    def test_queued_quote_gets_entry_turn_before_due_maintenance(self):
        self.plans(1)
        trace = []
        def place(**kwargs):
            trace.append('entry')
            return self.place(**kwargs)
        def maintenance(*args, **kwargs):
            trace.append('maintenance')
            raise KeyboardInterrupt()
        self.trade.client.place_limit_for_bar = Mock(side_effect=place)
        self.trade.market.update('N0USDT', '.99', int(self.now*1000))
        with patch.object(self.trade.market, 'start'), patch.object(self.trade.market, 'close'), \
                patch.object(self.trade, 'step', side_effect=maintenance), \
                patch('v9711_live.execution.ws_direct.MetadataSync'):
            self.assertEqual(self.trade._run_loop(), 0)
        self.assertEqual(trace[:2], ['entry', 'maintenance'])

    def test_quote_service_is_bounded_by_event_quota(self):
        self.plans(1)
        self.trade.market.update('N0USDT', '1.03', int(self.now*1000))
        self.trade.market.update('N0USDT', '1.031', int(self.now*1000))
        self.assertEqual(self.trade._service_pending_quotes(limit=1, budget_seconds=1), 1)
        self.assertFalse(self.trade.market.events.empty())

    def test_new_quote_during_reconciliation_is_sent_before_remaining_maintenance(self):
        self.plans(1)
        trace = []
        def reconcile(**kwargs):
            self.trade.market.update('N0USDT', '.99', int(self.now*1000))
        def remaining(**kwargs):
            trace.append('remaining')
            self.assertIn('N0USDT', self.trade.tracked)
        def place(**kwargs):
            trace.append('entry')
            return self.place(**kwargs)
        self.trade.client.place_limit_for_bar = Mock(side_effect=place)
        with patch.object(self.trade, 'reconcile_orders', side_effect=reconcile), \
                patch.object(self.trade, '_cleanup_forced_close_orders', side_effect=remaining):
            self.trade.step(now=self.now)
        self.assertEqual(trace[:2], ['entry', 'remaining'])

    def test_admission_turn_new_signal_is_not_overwritten_by_old_maintenance_snapshot(self):
        self.plans(1)
        def reconcile(**kwargs):
            self.payload['signal_ready'] = False
            cfg.DESIRED_FILE.write_text(json.dumps(self.payload))
            self.trade.market.update('N0USDT', '.99', int(self.now*1000))
        with patch.object(self.trade, 'reconcile_orders', side_effect=reconcile):
            self.trade.step(now=self.now)
        self.assertFalse(self.trade.tracked)
        self.assertFalse(self.trade.client.entry_gate['ready'])

    def test_concurrent_tick_and_depth_ingress_share_one_entry_reservation(self):
        self.plans(1)
        ready = threading.Barrier(3)
        def tick():
            ready.wait()
            self.trade.market.update('N0USDT', '.99', int(self.now*1000))
        def depth():
            ready.wait()
            self.trade.market.update_book('N0USDT', '.99', '2', int(self.now*1000), 1)
        threads = [threading.Thread(target=tick), threading.Thread(target=depth)]
        for thread in threads:
            thread.start()
        ready.wait()
        for thread in threads:
            thread.join(2)
            self.assertFalse(thread.is_alive())
        self.trade._service_pending_quotes(limit=20, budget_seconds=1)
        self.assertEqual(len(self.submits), 1)
        self.assertEqual(len(self.trade.tracked), 1)

    def test_pending_manual_evidence_retries_without_waiting_two_second_bucket(self):
        self.held(1)
        self.trade.positions['H0']['side'] = 'LONG'
        self.trade.client.enabled, self.trade.client.dry_run = True, False
        self.trade.client.stream.health = Mock(return_value=Health('default', 0, 7))
        probe = self.trade.client.stream.manual_flat_evidence = Mock(side_effect=[StreamUnavailable('read pending'), None])
        self.trade._reconcile_manual_flats(now=self.now)
        self.trade._reconcile_manual_flats(now=self.now)
        self.assertEqual(probe.call_count, 2)


class BackgroundOutputTests(unittest.TestCase):
    def test_feedback_detaches_rows_and_writes_latest_pending_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'feedback.json'
            started, release = threading.Event(), threading.Event()
            original_write = feedback_module.atomic_write_text
            calls = []
            def write(path, payload):
                revision = json.loads(payload)['revision']
                calls.append(revision)
                if revision == 1:
                    started.set()
                    self.assertTrue(release.wait(3))
                original_write(path, payload)
            with patch.object(feedback_module, 'atomic_write_text', side_effect=write):
                writer = FeedbackWriter(path)
                try:
                    writer.submit({'revision': 1})
                    self.assertTrue(started.wait(1))
                    writer.submit({'revision': 2})
                    payload = {'revision': 3, 'nested': {'quantity': 1}}
                    writer.submit(payload)
                    payload['nested']['quantity'] = 999
                finally:
                    release.set()
                    writer.close()
            self.assertEqual(calls, [1, 3])
            self.assertEqual(json.loads(path.read_text())['nested']['quantity'], 1)

    def test_failed_feedback_retries_latest_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'feedback.json'
            failed = threading.Event()
            original_write = feedback_module.atomic_write_text
            calls = []
            def write(path, payload):
                calls.append(json.loads(payload)['revision'])
                if len(calls) == 1:
                    failed.set()
                    raise OSError('temporary feedback failure')
                original_write(path, payload)
            with patch.object(feedback_module, 'atomic_write_text', side_effect=write):
                writer = FeedbackWriter(path)
                try:
                    writer.submit({'revision': 1})
                    self.assertTrue(failed.wait(1))
                    writer.submit({'revision': 2})
                finally:
                    writer.close()
            self.assertEqual(json.loads(path.read_text())['revision'], 2)
            self.assertEqual(writer.health()['failures'], 1)

    def test_log_file_io_does_not_block_the_logging_caller(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'trade.log'
            started, release = threading.Event(), threading.Event()
            emit = logging.FileHandler.emit
            def blocked(handler, record):
                started.set()
                self.assertTrue(release.wait(3))
                emit(handler, record)
            with patch('sys.stderr', new=io.StringIO()), patch.object(logging.FileHandler, 'emit', blocked):
                logs = AsyncTradeLogs(path, logging.INFO, logging.Filter())
                try:
                    logging.getLogger('entry-first').info('first')
                    self.assertTrue(started.wait(1))
                    logging.getLogger('entry-first').info('second')
                    self.assertFalse(release.is_set())
                finally:
                    release.set()
                    logs.close()
            self.assertIn('first', path.read_text())
            self.assertIn('second', path.read_text())

    def test_ready_entry_is_dispatched_before_paced_cancel(self):
        network = NetworkIO(workers=1)
        done, trace = threading.Event(), []
        def cancel():
            trace.append('cancel')
            done.set()
        try:
            with network._condition:
                network.submit(('cancel', 'OLD', 'cid', 'oid'), cancel, priority=30, entry_cancel=True)
                network.submit(('entry', 'NEW', 'cid'), lambda: trace.append('entry'), priority=10)
            self.assertTrue(done.wait(1))
        finally:
            network.shutdown()
        self.assertEqual(trace, ['entry', 'cancel'])
