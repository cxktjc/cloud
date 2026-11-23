"""No idle-worker broadcast during entry, completion or cancel-plan updates."""
import threading
from concurrent.futures import Future
import unittest
from unittest.mock import patch

from v9711_live.execution.nonblocking_io import NetworkIO
from v9711_live.execution.submit_errors import ConfirmedSubmitRejection


class WorkerWakeTests(unittest.TestCase):
    def test_default_pool_keeps_eight_workers_available_while_queries_wait_for_ack(self):
        release, query_ready, entry_ready = threading.Event(), threading.Event(), threading.Event()
        started = []
        lock = threading.Lock()
        io = NetworkIO(workers=19)
        def query():
            with lock:
                started.append(1)
                if len(started) == 11:
                    query_ready.set()
            release.wait(2)
        try:
            for index in range(19):
                io.submit(('query', 'status', str(index), 'cid'), query, priority=5)
            self.assertTrue(query_ready.wait(1))
            io.submit(('entry', 'NEW', 'cid'), entry_ready.set, priority=1)
            self.assertTrue(entry_ready.wait(1))
            with lock:
                self.assertEqual(len(started), 11)
        finally:
            release.set()
            io.shutdown()

    def test_cancel_and_query_backlogs_cannot_consume_mutating_request_capacity(self):
        release = threading.Event()
        io = NetworkIO(max_jobs=1, max_entry_cancels=2, workers=1, worker_start=lambda: release.wait(2))
        try:
            for symbol in ('A', 'B'):
                self.assertTrue(io.submit(('cancel', symbol, 'cid', 'oid'), lambda: None,
                    priority=30, entry_cancel=True))
            self.assertTrue(io.submit(('query', 'status', 'C', 'cid'), lambda: None, priority=5))
            self.assertTrue(io.submit(('entry', 'D', 'cid'), lambda: None, priority=1))
            with self.assertRaises(ConfirmedSubmitRejection):
                io.submit(('entry', 'E', 'cid'), lambda: None, priority=1)
            with self.assertRaises(ConfirmedSubmitRejection):
                io.submit(('query', 'status', 'F', 'cid'), lambda: None, priority=5)
            with self.assertRaises(ConfirmedSubmitRejection):
                io.submit(('cancel', 'G', 'cid', 'oid'), lambda: None, priority=30, entry_cancel=True)
        finally:
            release.set()
            io.shutdown()

    def test_new_zero_fill_ack_is_light_but_fills_cancels_errors_and_uncertainty_are_urgent(self):
        io = NetworkIO(workers=2)
        self.addCleanup(io.shutdown)
        cases = [('entry', dict(status='NEW', executedQty='0'), False),
                 ('entry', dict(status='PARTIALLY_FILLED', executedQty='.1'), True),
                 ('entry', dict(status='FILLED', executedQty='1'), True),
                 ('entry', dict(status='NEW'), True),
                 ('cancel', dict(status='CANCELED', executedQty='0'), True)]
        for index, (kind, value, urgent) in enumerate(cases):
            value = dict(value, symbol='XUSDT', clientOrderId=str(index), orderId='1')
            io.submit((kind, 'XUSDT', str(index)), lambda value=value: value, priority=10,
                      done=lambda value, error: self.assertIsNone(error))
            self.assertTrue(io._completed_callbacks.wait(1))
            self.assertEqual(io.drain(), 1)
            self.assertEqual(io.last_drain_urgent, urgent)
        def failed():
            raise TimeoutError('ACK uncertain')
        io.submit(('entry', 'XUSDT', 'failed'), failed, priority=10, done=lambda value, error: None)
        self.assertTrue(io._completed_callbacks.wait(1))
        io.drain()
        self.assertTrue(io.last_drain_urgent)

    def test_malformed_or_mismatched_new_ack_and_uninspected_tail_remain_urgent(self):
        io = NetworkIO(workers=2)
        self.addCleanup(io.shutdown)
        for value in (dict(status='NEW', executedQty='0'), dict(status='NEW', executedQty='0',
                       orderId='1', symbol='OTHER', clientOrderId='one')):
            io.submit(('entry', 'XUSDT', 'one'), lambda value=value: value, priority=10,
                      done=lambda value, error: None)
            self.assertTrue(io._completed_callbacks.wait(1))
            io.drain()
            self.assertTrue(io.last_drain_urgent)
        io.callback_limit = 1
        for cid in ('one', 'two'):
            value = dict(status='NEW', executedQty='0', orderId='1', symbol=cid+'USDT', clientOrderId=cid)
            io.submit(('entry', cid+'USDT', cid), lambda value=value: value, priority=10,
                      done=lambda value, error: None)
        # Work is trivial; the condition deadline also handles arbitrary worker scheduling.
        with io._condition:
            io._condition.wait_for(lambda: sum(j.state == 'done' for j in io._jobs.values()) == 2, timeout=1)
        self.assertEqual(io.drain(), 1)
        self.assertTrue(io.last_drain_urgent)
        self.assertEqual(io.drain(), 1)
        self.assertFalse(io.last_drain_urgent)

    def test_one_readiness_callback_per_shared_snapshot(self):
        io = NetworkIO(workers=4)
        self.addCleanup(io.shutdown)
        receipt = Future()
        sent = [threading.Event() for _ in range(4)]
        for index, event in enumerate(sent):
            io.submit(('entry', str(index), 'one'), event.set, priority=10, ready=receipt)
        self.assertEqual(len(receipt._done_callbacks), 1)
        self.assertEqual(io.health()['running'], 0)
        receipt.set_result(True)
        self.assertTrue(all(event.wait(1) for event in sent))

    def test_paced_cancel_in_quiet_period_does_not_wake_idle_workers_on_plan_refresh(self):
        io = NetworkIO(workers=2, cancel_clock=lambda: 0.)
        self.addCleanup(io.shutdown)
        io._entry_cancel_after = 10.
        key = ('cancel', 'XUSDT', 'one', '1')
        io.hold_entry_cancels()
        io.submit(key, lambda: self.fail('cancel ran inside quiet period'), priority=20, entry_cancel=True)
        with patch.object(io._condition, 'notify', side_effect=AssertionError('no runnable task')):
            io.update_entry_cancels({key: dict(price=1., limit_price=1.)})
            io.release_entry_cancels()

    def test_waiting_for_disk_does_not_occupy_worker_or_allow_same_symbol_overtake(self):
        io = NetworkIO(workers=1)
        self.addCleanup(io.shutdown)
        receipt = Future()
        sent, other, same = threading.Event(), threading.Event(), threading.Event()
        io.submit(('entry', 'XUSDT', 'one'), lambda: sent.set(), priority=10, ready=receipt,
                  done=lambda value, error: self.assertIsNone(error))
        io.submit(('entry', 'YUSDT', 'other'), lambda: other.set(), priority=10,
                  done=lambda value, error: self.assertIsNone(error))
        io.submit(('cancel', 'XUSDT', 'one'), lambda: same.set(), priority=20,
                  done=lambda value, error: self.assertIsNone(error))
        self.assertTrue(other.wait(1))
        self.assertFalse(sent.is_set())
        self.assertFalse(same.is_set())
        with io._condition:
            io._condition.wait_for(lambda: io._jobs[('entry', 'YUSDT', 'other')].state == 'done', timeout=1)
        io.drain()
        receipt.set_result(True)
        self.assertTrue(sent.wait(1))
        self.assertFalse(same.is_set())
        with io._condition:
            io._condition.wait_for(lambda: io._jobs[('entry', 'XUSDT', 'one')].state == 'done', timeout=1)
        io.drain()
        self.assertTrue(same.wait(1))

    def test_empty_plans_and_completions_do_not_broadcast(self):
        io = NetworkIO(workers=19)
        self.addCleanup(io.shutdown)
        finished = threading.Event()
        with patch.object(io._condition, 'notify_all', side_effect=AssertionError('idle broadcast')):
            io.hold_entry_cancels()
            io.update_entry_cancels({})
            io.release_entry_cancels()
            io.submit(('entry', 'XUSDT', 'one'), lambda: finished.set(), priority=10,
                      done=lambda value, error: self.assertIsNone(error))
            self.assertTrue(finished.wait(1))
            with io._condition:
                self.assertTrue(io._condition.wait_for(
                    lambda: io._jobs[('entry', 'XUSDT', 'one')].state == 'done', timeout=1))
            io.drain()
            self.assertFalse(io.health()['running'])

    def test_same_symbol_successor_waits_for_owner_ack_and_then_wakes(self):
        io = NetworkIO(workers=2)
        self.addCleanup(io.shutdown)
        first, second = threading.Event(), threading.Event()
        io.submit(('entry', 'XUSDT', 'one'), lambda: first.set(), priority=10,
                  done=lambda value, error: self.assertIsNone(error))
        io.submit(('entry', 'XUSDT', 'two'), lambda: second.set(), priority=10,
                  done=lambda value, error: self.assertIsNone(error))
        self.assertTrue(first.wait(1))
        self.assertFalse(second.is_set())
        with io._condition:
            io._condition.wait_for(lambda: io._jobs[('entry', 'XUSDT', 'one')].state == 'done', timeout=1)
        io.drain()
        self.assertTrue(second.wait(1))
