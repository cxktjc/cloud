"""Message isolation, expiration and post-negative read barriers; no broker."""
import threading
import time
import unittest
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from v9711_live.execution.background_account import BackgroundAccountView
from v9711_live.execution.background_terminal import BackgroundTerminal
from v9711_live.execution.stream import Health, StreamUnavailable
from v9711_live.execution.client import ExchangeClient, OwnershipLedger
from v9711_live.execution.nonblocking_io import NetworkIO


class BackgroundAccountTests(unittest.TestCase):
    def setUp(self):
        self.started, self.release = threading.Event(), threading.Event()
        self.calls = []
        self.source = SimpleNamespace(max_age_seconds=30, health=lambda: Health('default', .1, 7))
        self.source.lookup_order = self.lookup
        self.worker = BackgroundAccountView(self.source, refresh_seconds=.002)
        self.addCleanup(self.worker.close)
        self.addCleanup(self.release.set)
        self.until(self.worker.health)

    def lookup(self, **kwargs):
        self.calls.append((threading.get_ident(), kwargs))
        self.started.set()
        if not self.release.wait(2):
            raise RuntimeError('test reader not released')
        return dict(status='FILLED', executedQty=1, symbol=kwargs['symbol'])

    def until(self, operation):
        deadline = time.monotonic()+2
        while time.monotonic() < deadline:
            try:
                return operation()
            except StreamUnavailable:
                threading.Event().wait(.002)
        self.fail('background read did not complete')

    def test_blocked_database_read_never_blocks_owner_and_returns_detached_result(self):
        with self.assertRaises(StreamUnavailable):
            self.worker.lookup_order(symbol='X', client_order_id='c')
        self.assertTrue(self.started.wait(1))
        for _ in range(10):
            with self.assertRaises(StreamUnavailable):
                self.worker.lookup_order(symbol='X', client_order_id='c')
        self.assertFalse(self.release.is_set())
        self.release.set()
        result = self.until(lambda: self.worker.lookup_order(symbol='X', client_order_id='c'))
        result['executedQty'] = 999
        self.assertEqual(self.worker.lookup_order(symbol='X', client_order_id='c')['executedQty'], 1)
        self.assertNotEqual(self.calls[0][0], threading.get_ident())

    def test_inflight_read_before_negative_response_cannot_pass_fresh_barrier(self):
        with self.assertRaises(StreamUnavailable):
            self.worker.lookup_order(symbol='X', client_order_id='c')
        self.assertTrue(self.started.wait(1))
        fence = time.perf_counter_ns()
        with self.assertRaises(StreamUnavailable):
            self.worker.lookup_order_after(after_ns=fence, symbol='X', client_order_id='c')
        self.release.set()
        result = self.until(lambda: self.worker.lookup_order_after(after_ns=fence, symbol='X', client_order_id='c'))
        self.assertEqual(result['status'], 'FILLED')
        self.assertGreaterEqual(len(self.calls), 2)

    def test_health_expires_while_reader_is_blocked(self):
        self.worker.result_max_age = .01
        with self.assertRaises(StreamUnavailable):
            self.worker.lookup_order(symbol='X')
        self.assertTrue(self.started.wait(1))
        threading.Event().wait(.025)
        with self.assertRaises(StreamUnavailable):
            self.worker.health()

    def test_manual_diagnostics_and_none_are_copied_without_fabricating_evidence(self):
        def manual(*, symbol, diagnostics):
            diagnostics.update(reason='no_post_entry_flat_snapshot')
            return None
        self.source.manual_flat_evidence = manual
        details = {}
        self.assertIsNone(self.until(lambda: self.worker.manual_flat_evidence(symbol='X', diagnostics=details)))
        self.assertEqual(details['reason'], 'no_post_entry_flat_snapshot')

    def test_changed_evidence_wakes_owner_without_waking_for_identical_refreshes(self):
        wake = threading.Event()
        self.worker._owner_wake = wake.set
        with self.assertRaises(StreamUnavailable):
            self.worker.lookup_order(symbol='X')
        self.assertTrue(self.started.wait(1))
        self.release.set()
        self.assertTrue(wake.wait(1))
        self.until(lambda: self.worker.lookup_order(symbol='X'))
        wake.clear()
        threading.Event().wait(.02)
        self.assertFalse(wake.is_set())


class BackgroundTerminalTests(unittest.TestCase):
    def test_slow_display_does_not_block_caller_and_never_observes_mutated_input(self):
        started, release = threading.Event(), threading.Event()
        seen = []
        def render(**fields):
            started.set()
            if not release.wait(2):
                raise RuntimeError('test display not released')
            seen.append(fields['value']['qty'])
        view = SimpleNamespace(min_refresh_seconds=0, render=render)
        worker = BackgroundTerminal(view)
        try:
            fields = dict(value=dict(qty=1))
            worker.render(**fields)
            self.assertTrue(started.wait(1))
            fields['value']['qty'] = 99
            worker.render(force=True, value=dict(qty=2))
            self.assertFalse(release.is_set())
        finally:
            release.set()
            worker.close()
        self.assertEqual(seen, [1, 2])


class AccountAbsenceBarrierTests(unittest.TestCase):
    def test_exact_ack_already_in_local_payload_prevents_absence_release(self):
        with tempfile.TemporaryDirectory() as folder:
            ledger = OwnershipLedger(Path(folder)/'intents', Path(folder)/'state')
            ledger.register(symbol='X', client_order_id='cid', kind='entry')
            client = ExchangeClient(enabled=False)
            client._can_confirm_unacknowledged_absence = Mock(return_value=True)
            row = dict(symbol='X', clientOrderId='cid', orderId='1', status='NEW', executedQty=0)
            client.stream = SimpleNamespace(lookup_order=lambda **kwargs: row, health=lambda: Health('default', 0, 1))
            state = dict(absence_check_after_ns=123, not_found=2)
            client._order_queries['c:cid'] = state
            self.assertEqual(client.query_exact_order(ledger, symbol='X', client_order_id='cid'), (row, True))
            self.assertNotIn('absence_check_after_ns', state)
            self.assertTrue(ledger.owns_client('cid'))

    def test_pending_local_read_never_confirms_absence_and_late_fill_wins(self):
        for filled in (False, True):
            with self.subTest(filled=filled), tempfile.TemporaryDirectory() as folder:
                ledger = OwnershipLedger(Path(folder)/'intents', Path(folder)/'state')
                cid = ledger.reserve_entry_client_id('X', 180000)
                ledger.register(symbol='X', client_order_id=cid, kind='entry')
                client = ExchangeClient(enabled=False)
                client._can_confirm_unacknowledged_absence = Mock(return_value=True)
                client._broker_call = Mock(side_effect=AssertionError('repeated remote query before local barrier'))
                class Barrier:
                    pending = True
                    def lookup_order(self, **kwargs):
                        return None
                    def health(self):
                        return Health('default', 0, 1)
                    def lookup_order_after(self, *, after_ns, **kwargs):
                        if self.pending:
                            raise StreamUnavailable('fresh local read pending')
                        return dict(symbol='X', clientOrderId=cid, orderId='1', status='FILLED',
                                    executedQty=1, avgPrice=1) if filled else None
                client.stream = Barrier()
                client._order_queries['c:'+cid] = dict(absence_check_after_ns=123, not_found=2)
                self.assertEqual(client.query_exact_order(ledger, symbol='X', client_order_id=cid), (None, False))
                self.assertTrue(ledger.owns_client(cid))
                client.stream.pending = False
                result, known = client.query_exact_order(ledger, symbol='X', client_order_id=cid)
                self.assertTrue(known)
                self.assertEqual(result.get('status'), 'FILLED' if filled else None)
                client._broker_call.assert_not_called()


class BoundedCallbackTests(unittest.TestCase):
    def test_completed_callbacks_remain_pending_after_owner_budget(self):
        io = NetworkIO(workers=2)
        try:
            io.callback_limit = 1
            applied = []
            for i in range(3):
                io.submit(('entry', str(i), str(i)), lambda: 1,
                          done=lambda value, error: applied.append(value), priority=0)
            deadline = time.monotonic()+2
            while io.health()['completed'] < 3 and time.monotonic() < deadline:
                threading.Event().wait(.002)
            self.assertEqual(io.health()['completed'], 3)
            self.assertEqual(io.drain(), 1)
            self.assertTrue(io._completed_callbacks.is_set())
            self.assertEqual(io.drain(), 1)
            self.assertEqual(io.drain(), 1)
            self.assertEqual(applied, [1, 1, 1])
        finally:
            io.shutdown()

    def test_shutdown_drains_all_callbacks_even_with_one_callback_turn_limit(self):
        io = NetworkIO(workers=2)
        io.callback_limit = 1
        applied = []
        for i in range(3):
            io.submit(('entry', str(i), str(i)), lambda: 1,
                      done=lambda value, error: applied.append((value, error)), priority=0)
        io.shutdown()
        self.assertEqual(len(applied), 3)
        self.assertFalse(io._jobs)
