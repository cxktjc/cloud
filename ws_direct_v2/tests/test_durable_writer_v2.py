"""Disk waits must not own the quote loop; no send without exact durability."""
import json
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from tests import test_book_ticker_v2 as fixtures
from v9711_live.execution import durable_writer as module
from v9711_live.execution.client import ExchangeClient, OwnershipLedger


class WriterTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.ledger = OwnershipLedger(root/'intents', root/'state')
        self.ledger.enable_background()
        self.release = threading.Event()
        self.addCleanup(self.finish)

    def finish(self):
        self.release.set()
        self.ledger.close_background()

    def reserve(self, symbol):
        with self.ledger.batch_saves():
            cid = self.ledger.reserve_entry_client_id(symbol, 180000)
            self.ledger.state['tracked_orders'][symbol] = dict(symbol=symbol, client_order_id=cid)
            self.ledger.register(symbol=symbol, client_order_id=cid, kind='entry')
        return cid, self.ledger.flush_pending()

    def blocked_write(self):
        started = threading.Event()
        original = module.atomic_write_text
        def write(path, encoded):
            started.set()
            if not self.release.wait(3):
                raise AssertionError('disk barrier not released')
            original(path, encoded)
        context = patch.object(module, 'atomic_write_text', side_effect=write)
        context.start()
        self.addCleanup(context.stop)
        return started

    def test_snapshots_detach_and_newer_pending_snapshot_covers_all_reserved_ids(self):
        started = self.blocked_write()
        cid0, first = self.reserve('XUSDT')
        self.assertTrue(started.wait(1))
        cid1, second = self.reserve('YUSDT')
        cid2, third = self.reserve('ZUSDT')
        self.assertIs(second, third)  # Bounded pending snapshot, not an unbounded disk queue.
        self.assertFalse(first.done())
        self.assertFalse(second.done())
        # Change memory without submitting: the writer may never inspect it.
        self.ledger.state['tracked_orders']['ZUSDT']['quantity'] = 999
        self.release.set()
        self.assertIn(('XUSDT', cid0), first.result(1).reservations)
        receipt = third.result(1)
        self.assertTrue({('XUSDT', cid0), ('YUSDT', cid1), ('ZUSDT', cid2)} <= receipt.reservations)
        restored = OwnershipLedger(self.ledger.path, self.ledger.state_path)
        self.assertNotIn('quantity', restored.state['tracked_orders']['ZUSDT'])
        self.assertTrue(all(restored.owns_client(cid) for cid in (cid0, cid1, cid2)))
        self.assertEqual(restored.state['entry_submission_sequence'], 3)

    def test_partial_pair_failure_has_no_positive_receipt_and_retry_writes_missing_state(self):
        original = module.atomic_write_text
        failed = threading.Event()
        def write(path, encoded):
            if path == self.ledger.state_path and not failed.is_set():
                failed.set()
                raise OSError('simulated state fsync failure')
            return original(path, encoded)
        with patch.object(module, 'atomic_write_text', side_effect=write):
            cid, ticket = self.reserve('XUSDT')
            with self.assertRaises(OSError):
                ticket.result(1)
            self.assertTrue(json.loads(self.ledger.path.read_text())['intents'])
            self.assertFalse(self.ledger.state_path.exists())
            retry = self.ledger.flush_for_dispatch()
            self.assertIn(('XUSDT', cid), retry.result(1).reservations)
        restored = OwnershipLedger(self.ledger.path, self.ledger.state_path)
        self.assertEqual(restored.state['tracked_orders']['XUSDT']['client_order_id'], cid)

    def test_superseded_pending_identity_is_not_certified(self):
        started = self.blocked_write()
        self.reserve('XUSDT')
        self.assertTrue(started.wait(1))
        cid, ticket = self.reserve('YUSDT')
        self.ledger.state['tracked_orders'].pop('YUSDT')
        self.ledger.forget(symbol='YUSDT', client_order_id=cid)
        self.release.set()
        self.assertNotIn(('YUSDT', cid), ticket.result(1).reservations)

    def test_close_receipt_requires_both_intent_and_owned_position_close_slot(self):
        self.ledger.state['owned_positions']['XUSDT'] = dict(quantity=2, close_client_order_id='close-cid')
        self.ledger.register(symbol='XUSDT', client_order_id='close-cid', kind='close', qty=2)
        self.assertIn(('XUSDT', 'close-cid'), self.ledger.flush_pending().result(1).reservations)
        self.ledger.state['owned_positions']['XUSDT'].pop('close_client_order_id')
        self.assertNotIn(('XUSDT', 'close-cid'), self.ledger.flush_for_dispatch().result(1).reservations)

    def test_close_flushes_latest_state_and_is_idempotent(self):
        self.reserve('XUSDT')
        self.ledger.state['realized_balance'] = 123
        self.ledger.close_background()
        self.ledger.close_background()
        self.assertFalse(self.ledger._background._thread.is_alive())
        restored = OwnershipLedger(self.ledger.path, self.ledger.state_path)
        self.assertEqual(restored.state['realized_balance'], 123)


class OwnerDurabilityTests(unittest.TestCase):
    def setUp(self):
        self.case = fixtures.DualAdmissionTests('test_tick_only_qualifies')
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.release = threading.Event()
        self.case.targets = []
        self.case.templates = [dict(symbol=f'N{i}USDT', limit_price=1., notional=5.,
            order_bar_open_ms=self.case.bar, reference_bar_open_ms=self.case.bar-180000,
            exit_due_epoch=self.case.now+900) for i in range(8)]
        self.case.publish()
        self.trade = self.case.trade
        self.trade.client.enabled, self.trade.client.dry_run = True, False
        self.trade.client.stream = SimpleNamespace()
        self.trade.client.warm_worker_transport = lambda: None
        self.trade.enable_nonblocking_io()
        self.trade._account_deadline = self.case.now+100
        self.sent = []
        def wire(method, symbol, side, qty, *, client_order_id, before_send, **kw):
            before_send()
            restored = OwnershipLedger(self.trade.ledger.path, self.trade.ledger.state_path)
            if not restored.owns_client(client_order_id):
                raise AssertionError('send before intent durability')
            if restored.state['tracked_orders'][symbol]['client_order_id'] != client_order_id:
                raise AssertionError('send before slot durability')
            before_send.wire_sent()
            self.sent.append(client_order_id)
            result = dict(symbol=symbol, clientOrderId=client_order_id, orderId=str(len(self.sent)),
                          status='NEW', executedQty='0', origQty=qty, avgPrice='0')
            self.case.orders[client_order_id] = result
            return result
        self.trade.client._broker_call = wire
        self.trade.client.place_limit_for_bar = ExchangeClient.place_limit_for_bar.__get__(self.trade.client)
        self.addCleanup(self.finish)

    def finish(self):
        self.release.set()
        self.trade.shutdown_io()

    def wait_done(self):
        deadline = time.perf_counter()+2
        while time.perf_counter() < deadline:
            self.trade._drain_network()
            health = self.trade._network_io.health()
            if not health['running'] and not health['queued'] and not health['completed']:
                return
            threading.Event().wait(.001)
        self.fail('network jobs did not settle')

    def block_disk(self):
        original = module.atomic_write_text
        started = threading.Event()
        def write(path, encoded):
            started.set()
            if not self.release.wait(3):
                raise AssertionError('test disk not released')
            original(path, encoded)
        context = patch.object(module, 'atomic_write_text', side_effect=write)
        context.start()
        self.addCleanup(context.stop)
        return started

    def test_owner_continues_quotes_and_reserves_eight_while_disk_blocked_then_sends_exactly_once(self):
        started = self.block_disk()
        # The fixture initially has all eight prices eligible. One tick must
        # reserve all slots without waiting for the first disk completion.
        self.case.event('N0USDT')
        self.assertTrue(started.wait(1))
        self.assertEqual(len(self.trade.tracked), 8)
        self.assertFalse(self.sent)
        self.case.event('AUSDT')
        self.assertIn('AUSDT', self.trade.market.prices())
        self.assertFalse(self.release.is_set())
        self.release.set()
        self.wait_done()
        self.assertEqual(len(self.sent), 8)
        self.assertEqual(len(set(self.sent)), 8)

    def test_rebound_received_during_disk_wait_prevents_stale_send(self):
        self.case.templates = self.case.templates[:1]
        self.case.publish()
        started = self.block_disk()
        self.case.event('N0USDT')
        self.assertTrue(started.wait(1))
        self.trade.market.update('N0USDT', '1.03', int(self.case.now*1000))
        # The final guard reads live ingress even before the owner dequeues it.
        self.release.set()
        self.wait_done()
        self.assertFalse(self.sent)

    def test_failed_disk_never_calls_exchange(self):
        with patch.object(module, 'atomic_write_text', side_effect=OSError('simulated disk failure')):
            self.case.event('N0USDT')
            self.wait_done()
            self.assertFalse(self.sent)
            self.assertFalse(self.trade.tracked)  # Known-unsent disk failure is not UNKNOWN.
            try:
                self.trade.ledger.flush_pending().result(1)
            except OSError:
                pass  # Finish the deliberately failed background rejection save.
        self.trade.ledger.flush()  # Explicitly recover the test disk before cleanup.

    def test_signal_withdrawn_during_disk_wait_is_checked_again_at_send(self):
        self.case.templates = self.case.templates[:1]
        self.case.publish()
        started = self.block_disk()
        self.case.event('N0USDT')
        self.assertTrue(started.wait(1))
        self.trade._io_signal = ({}, 'expired')
        self.trade._publish_entry_gate()
        self.release.set()
        self.wait_done()
        self.assertFalse(self.sent)

    def test_network_ticket_cannot_send_an_identity_absent_from_coalesced_snapshot(self):
        self.case.templates = self.case.templates[:1]
        self.case.publish()
        started = self.block_disk()
        # Occupy the active write so the entry ticket belongs to the pending
        # group, which is then superseded before any positive receipt.
        self.trade.ledger.flush_for_dispatch()
        self.assertTrue(started.wait(1))
        self.case.event('N0USDT')
        row = self.trade.tracked.pop('N0USDT')
        cid = row['client_order_id']
        self.trade.ledger.forget(symbol='N0USDT', client_order_id=cid)
        self.trade._publish_entry_gate()
        self.release.set()
        self.wait_done()
        self.assertFalse(self.sent)
        self.trade.ledger.flush()
        restored = OwnershipLedger(self.trade.ledger.path, self.trade.ledger.state_path)
        self.assertFalse(restored.owns_client(cid))

    def test_sent_but_ack_lost_preserves_durable_exact_slot_for_restart(self):
        self.case.templates = self.case.templates[:1]
        self.case.publish()
        original = self.trade.client._broker_call
        def lost_ack(*args, **kwargs):
            original(*args, **kwargs)
            raise TimeoutError('simulated reply lost after actual send')
        self.trade.client._broker_call = lost_ack
        self.case.event('N0USDT')
        self.wait_done()
        self.assertEqual(len(self.sent), 1)
        self.trade.shutdown_io()  # Simulate restart only AFTER the old writer has exited.
        restored = OwnershipLedger(self.trade.ledger.path, self.trade.ledger.state_path)
        row = restored.state['tracked_orders']['N0USDT']
        self.assertEqual(row['last_status'], 'UNKNOWN')
        self.assertEqual(row['client_order_id'], self.sent[0])
        self.assertTrue(restored.owns_client(self.sent[0]))
        self.assertNotEqual(restored.reserve_entry_client_id('N0USDT', self.case.bar), self.sent[0])

    def test_shutdown_restores_process_thread_switch_interval(self):
        previous = self.trade._previous_switch_interval
        self.assertEqual(sys.getswitchinterval(), self.trade._live_switch_interval)
        self.trade.shutdown_io()
        self.assertEqual(sys.getswitchinterval(), previous)


if __name__ == '__main__':
    unittest.main()
