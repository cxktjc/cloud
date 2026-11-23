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
from unittest.mock import Mock

from tests import test_book_ticker_v2 as fixtures
from v9711_live.execution import durable_writer as module
from v9711_live.execution.client import ExchangeClient, OwnershipLedger
from v9711_live import config as cfg
from v9711_live.contracts import atomic_write_json


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
        self.assertTrue(all(not thread.is_alive() for thread in self.ledger._background._file_pool._threads))
        restored = OwnershipLedger(self.ledger.path, self.ledger.state_path)
        self.assertEqual(restored.state['realized_balance'], 123)

    def test_two_file_writes_start_together_but_receipt_waits_for_both(self):
        started = {path: threading.Event() for path in (self.ledger.path, self.ledger.state_path)}
        release_intents = threading.Event()
        original = module.atomic_write_text
        def write(path, encoded):
            started[path].set()
            gate = release_intents if path == self.ledger.path else self.release
            if not gate.wait(3):
                raise AssertionError('parallel file gate not released')
            original(path, encoded)
        try:
            with patch.object(module, 'atomic_write_text', side_effect=write):
                _, receipt = self.reserve('XUSDT')
                self.assertTrue(all(event.wait(1) for event in started.values()))
                release_intents.set()
                self.assertFalse(receipt.done())
                self.release.set()
                receipt.result(1)
        finally:
            release_intents.set()
            self.release.set()

    def test_failed_file_must_not_allow_next_snapshot_to_overtake_busy_sibling(self):
        old_intents_started, first_failed, newer_started = (threading.Event() for _ in range(3))
        original = module.atomic_write_text
        def write(path, encoded):
            payload = json.loads(encoded)
            if path == self.ledger.path:
                if len(payload['intents']) == 1:
                    old_intents_started.set()
                    if not self.release.wait(3):
                        raise AssertionError('old write not released')
                else:
                    newer_started.set()
            elif len(payload['state']['tracked_orders']) == 1:
                first_failed.set()
                raise OSError('first state failed while first intents still writing')
            original(path, encoded)
        with patch.object(module, 'atomic_write_text', side_effect=write):
            _, first = self.reserve('XUSDT')
            self.assertTrue(old_intents_started.wait(1))
            self.assertTrue(first_failed.wait(1))
            cid, second = self.reserve('YUSDT')
            self.assertFalse(newer_started.wait(.02))
            self.assertFalse(first.done())
            self.release.set()
            with self.assertRaises(OSError):
                first.result(1)
            self.assertIn(('YUSDT', cid), second.result(1).reservations)
        restored = OwnershipLedger(self.ledger.path, self.ledger.state_path)
        self.assertEqual(len(restored.state['tracked_orders']), 2)
        self.assertEqual(len(restored.intents), 2)


class NativeRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.intents, self.state = root/'intents', root/'state'
        self.cid = cfg.ORDER_CLIENT_PREFIX+'_entry_native123'
        self.payload = dict(strategy_version=cfg.VERSION, entry_submission_sequence=19,
            tracked_orders={'XUSDT': dict(symbol='XUSDT', client_order_id=self.cid,
                order_id='', qty=5, limit_price=1, placed_at_epoch=1000, fate_unknown=True)},
            owned_positions={})

    def save(self, intents=None):
        atomic_write_json(self.intents, dict(intents=intents or {}))
        atomic_write_json(self.state, dict(state=self.payload))

    def test_partial_pair_recovers_only_existing_native_cid_for_reconciliation(self):
        self.save()
        ledger = OwnershipLedger(self.intents, self.state)
        self.assertTrue(ledger.owns_client(self.cid))
        self.assertTrue(ledger.state['tracked_orders']['XUSDT']['fate_unknown'])
        self.assertEqual(ledger.state['entry_submission_sequence'], 19)
        self.assertEqual(json.loads(self.intents.read_text())['intents'], {})  # Pure read, not a second writer.
        ledger.flush()
        restored = OwnershipLedger(self.intents, self.state)
        self.assertTrue(restored.owns_client(self.cid))

    def test_foreign_strategy_or_foreign_cid_is_never_reconstructed(self):
        self.payload['strategy_version'] = 'old_strategy'
        self.save()
        self.assertFalse(OwnershipLedger(self.intents, self.state).owns_client(self.cid))
        self.payload['strategy_version'] = cfg.VERSION
        self.payload['tracked_orders']['XUSDT']['client_order_id'] = 'foreign_entry'
        self.save()
        self.assertFalse(OwnershipLedger(self.intents, self.state).owns_client('foreign_entry'))

    def test_partial_close_pair_keeps_existing_close_id_and_quantity(self):
        self.payload['tracked_orders'] = {}
        self.payload['owned_positions'] = {'XUSDT': dict(quantity=3,
            close_client_order_id=self.cid, close_quantity=3, close_phase='MARKET')}
        self.save()
        ledger = OwnershipLedger(self.intents, self.state)
        self.assertEqual(ledger.intents['c:'+self.cid]['kind'], 'close')
        self.assertEqual(ledger.intents['c:'+self.cid]['qty'], 3)
        self.assertEqual(ledger.state['owned_positions']['XUSDT']['quantity'], 3)

    def test_conflicting_order_alias_stops_recovery_and_preserves_files(self):
        self.payload['tracked_orders']['XUSDT']['order_id'] = '123'
        self.save({'o:XUSDT:123': dict(symbol='XUSDT', client_order_id='conflicting')})
        before = self.intents.read_bytes(), self.state.read_bytes()
        with self.assertRaises(RuntimeError):
            OwnershipLedger(self.intents, self.state)
        self.assertEqual(before, (self.intents.read_bytes(), self.state.read_bytes()))

    def test_recovered_native_slot_can_reconcile_exact_fill_without_sending(self):
        self.save()
        ledger = OwnershipLedger(self.intents, self.state)
        client = ExchangeClient(enabled=False, dry_run=True)
        fill = dict(symbol='XUSDT', clientOrderId=self.cid, orderId='123', status='FILLED',
                    executedQty='5', avgPrice='1', updateTime=1000001)
        client.stream = SimpleNamespace(lookup_order=lambda **kw: fill, health=lambda: None)
        client._broker_call = Mock(side_effect=AssertionError('recovery must not send'))
        actual, known = client.query_exact_order(ledger, symbol='XUSDT', client_order_id=self.cid, now=1001)
        self.assertTrue(known)
        self.assertEqual(actual, fill)
        client._broker_call.assert_not_called()

    def test_conflicting_slot_symbol_stops_recovery(self):
        self.payload['tracked_orders']['XUSDT']['symbol'] = 'YUSDT'
        self.save()
        with self.assertRaises(RuntimeError):
            OwnershipLedger(self.intents, self.state)


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
        from v9711_live.execution import journal_store
        original = journal_store.commit_journal
        started = threading.Event()
        def write(path, encoded):
            started.set()
            if not self.release.wait(3):
                raise AssertionError('test disk not released')
            original(path, encoded)
        context = patch.object(journal_store, 'commit_journal', side_effect=write)
        context.start()
        self.addCleanup(context.stop)
        return started

    def prime_distinct_batch(self):
        self.case.templates = self.case.templates[:3]
        for symbol in ('N0USDT', 'N1USDT', 'N2USDT'):
            self.trade.market.update(symbol, '1.03', int(self.case.now*1000))
            self.trade.market.decide(self.trade.market.events.get_nowait(), lambda event: None)
        self.case.publish()
        self.trade.desired_snapshot(now=self.case.now)

    def test_one_network_callback_turn_saves_all_ack_identities_in_one_snapshot(self):
        self.case.event('N0USDT')
        io = self.trade._network_io
        deadline = time.perf_counter()+2
        while io.health()['completed'] != 8 and time.perf_counter() < deadline:
            threading.Event().wait(.001)
        self.assertEqual(io.health()['completed'], 8)
        io.callback_limit = io.callback_budget_seconds = None
        before = self.trade.ledger._background.health()['submitted']
        self.assertEqual(self.trade._drain_network(), 8)
        self.trade.ledger.flush_pending().result(2)
        self.assertEqual(self.trade.ledger._background.health()['submitted'], before+1)
        restored = OwnershipLedger(self.trade.ledger.path, self.trade.ledger.state_path)
        self.assertTrue(all(row.get('order_id') for row in restored.state['tracked_orders'].values()))

    def test_distinct_simultaneous_crossings_use_one_plan_and_keep_each_trigger_timestamp(self):
        self.prime_distinct_batch()
        for symbol in ('N0USDT', 'N1USDT', 'N2USDT'):
            self.trade.market.update(symbol, '.99', int(self.case.now*1000))
        first = self.trade.market.events.get_nowait()
        with patch.object(self.trade, '_execute_entries', wraps=self.trade._execute_entries) as execute:
            self.trade.market.decide(first, self.trade.on_tick)
            self.assertEqual(execute.call_count, 1)
        self.trade.market.events.task_done()
        self.wait_done()
        self.assertEqual(len(self.sent), 3)
        for cid in self.sent:
            timing = self.trade.session_entries.timings.order(cid)
            self.assertEqual(timing['trigger_symbol'], timing['symbol'])
            self.assertEqual(timing['trigger_price'], .99)
            self.assertAlmostEqual(timing['trigger_gap'], -.01)
        self.assertFalse(self.trade._batch_event_contexts)

    def prime_full_book_replacements(self):
        self.case.event('N0USDT')
        self.wait_done()
        old = {s: r['client_order_id'] for s, r in self.trade.tracked.items()}
        challengers = ('AUSDT', 'BUSDT', 'CUSDT', 'N8USDT')
        self.case.templates.extend(dict(self.case.templates[0], symbol=s) for s in challengers)
        for symbol in (*old, *challengers):
            self.trade.market.update(symbol, '1.02' if symbol in old else '1.03', int(self.case.now*1000))
        while not self.trade.market.events.empty():
            event = self.trade.market.events.get_nowait()
            if event is not None:
                self.trade.market.decide(event, lambda event: None)
            self.trade.market.events.task_done()
        self.case.publish()
        self.trade.desired_snapshot(now=self.case.now)
        for symbol in challengers:
            self.trade.market.update(symbol, '.99', int(self.case.now*1000))
        return old, challengers

    def test_full_book_replaces_all_distinct_challengers_in_one_durable_turn(self):
        old, challengers = self.prime_full_book_replacements()
        started = self.block_disk()
        before = self.trade.ledger._background.health()['submitted']
        event = self.trade.market.events.get_nowait()
        self.trade.market.decide(event, self.trade.on_tick)
        self.trade.market.events.task_done()
        self.assertTrue(started.wait(1))
        self.assertEqual(self.trade.ledger._background.health()['submitted'], before+1)
        self.assertEqual(self.trade._ordinary_pending_count(), 8)
        self.assertEqual(len(self.trade.tracked), 12)
        self.assertEqual(len(self.sent), 8)  # New IDs still await their shared receipt.
        self.assertIsNone(self.trade.ledger.state.get('pending_rotation'))
        for symbol, cid in old.items():
            self.assertEqual(self.trade.tracked[symbol]['client_order_id'], cid)
        self.assertEqual(sum(bool(r.get('cancel_requested')) for r in self.trade.tracked.values()), 4)
        for symbol in challengers:
            self.assertEqual(self.trade.tracked[symbol]['rotation_origin']['original_order_count'], 8)
        self.release.set()
        deadline = time.perf_counter()+2
        while len(self.sent) < 12 and time.perf_counter() < deadline:
            self.trade._drain_network()
            threading.Event().wait(.001)
        self.assertEqual(len(set(self.sent)), 12)

    def test_replacement_without_registration_stops_replanning_and_keeps_reservation(self):
        self.prime_full_book_replacements()
        event = self.trade.market.events.get_nowait()
        with patch.object(self.trade, '_place_order') as place:
            self.trade.market.decide(event, self.trade.on_tick)
        self.trade.market.events.task_done()
        self.assertEqual(place.call_count, 1)
        self.assertEqual(len(self.trade.tracked), 8)
        self.assertIsNotNone(self.trade.ledger.state.get('pending_rotation'))

    def test_cancel_backlog_stays_owned_and_distance_ranked_without_competing_in_fast_plan(self):
        from v9711_live.execution import runner
        old, _ = self.prime_full_book_replacements()
        victims = list(old)[:3]
        seen = []
        original = runner.select_physical_orders
        def select(desired, tracked, **kwargs):
            seen.append(set(tracked))
            return original(desired, tracked, **kwargs)
        with self.trade.entry_cancel_plan():
            for symbol in victims:
                self.trade._request_cancel(symbol, self.trade.tracked[symbol], 'previous_cancel')
            event = self.trade.market.events.get_nowait()
            with patch.object(runner, 'select_physical_orders', side_effect=select):
                self.trade.market.decide(event, self.trade.on_tick)
            self.trade.market.events.task_done()
            self.assertTrue(seen)
            self.assertFalse(set(victims) & set.union(*seen))
            for symbol in victims:
                self.assertEqual(self.trade.tracked[symbol]['client_order_id'], old[symbol])
                self.assertEqual(self.trade._cancel_prices[symbol], 1.02)
            self.trade._refresh_entry_cancel_plan()
            rows = self.trade._network_io._entry_cancel_orders
            for symbol in victims:
                key = ('cancel', symbol, old[symbol], self.trade.tracked[symbol]['order_id'])
                self.assertEqual(rows[key]['price'], 1.02)

    def test_maintenance_full_universe_plans_once_even_with_multiple_replacements(self):
        self.prime_full_book_replacements()
        while not self.trade.market.events.empty():
            event = self.trade.market.events.get_nowait()
            if event is not None:
                self.trade.market.decide(event, lambda event: None)
            self.trade.market.events.task_done()
        snapshot, status = self.trade.desired_snapshot(now=self.case.now)
        self.assertFalse(self.trade._fast_tick)
        with patch.object(self.trade, '_execute_entries_with_prices',
                          wraps=self.trade._execute_entries_with_prices) as execute:
            self.trade._execute_entries(snapshot, status, now=self.case.now,
                                        targets={}, final_exit_bar=False)
        self.assertEqual(execute.call_count, 1)
        self.assertEqual(len(self.trade.tracked), 9)

    def test_optional_exit_milliseconds_use_same_identity_before_and_after_planner(self):
        from v9711_live.execution.logic import Row
        raw = dict(self.case.templates[0], observed_price=1.01, gap=.01)
        normalized = Row.from_dict(raw)
        canonical = dict(raw, exit_due_ms=normalized.exit_due_ms)
        self.assertEqual(self.trade._entry_plan_signature(raw), self.trade._entry_plan_signature(canonical))
        self.trade.ledger.state['rotated_out_plans']['old'] = dict(symbol=raw['symbol'],
            plan_signature=self.trade._entry_plan_signature(canonical), retryable=False)
        self.assertTrue(self.trade._entry_submission_blocked(raw))
        self.assertTrue(self.trade._entry_submission_blocked(canonical))

    def test_batch_preserves_rebound_but_already_admitted_entries_can_send(self):
        self.prime_distinct_batch()
        for symbol, price in (('N0USDT', '.99'), ('N1USDT', '.99'), ('N1USDT', '1.03')):
            self.trade.market.update(symbol, price, int(self.case.now*1000))
        first = self.trade.market.events.get_nowait()
        self.trade.market.decide(first, self.trade.on_tick)
        self.trade.market.events.task_done()
        self.wait_done()
        self.assertEqual(len(self.sent), 2)
        rebound = self.trade.market.events.get_nowait()
        self.assertEqual((rebound.symbol, rebound.price), ('N1USDT', '1.03'))
        self.trade.market.decide(rebound, self.trade.on_tick)
        self.trade.market.events.task_done()
        self.wait_done()
        self.assertEqual(len(self.sent), 2)
        self.assertEqual({self.trade.session_entries.timings.order(cid)['symbol'] for cid in self.sent},
                         {'N0USDT', 'N1USDT'})

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

    def test_rebound_received_during_disk_wait_keeps_frozen_admission(self):
        self.case.templates = self.case.templates[:1]
        self.case.publish()
        started = self.block_disk()
        self.case.event('N0USDT')
        self.assertTrue(started.wait(1))
        self.trade.market.update('N0USDT', '1.03', int(self.case.now*1000))
        # Sending must not read or compare another quote after admission.
        with patch.object(self.trade.market, 'send_quote', side_effect=AssertionError('secondary WS read')):
            self.release.set()
            self.wait_done()
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(self.trade.tracked['N0USDT']['entry_send_quote']['entry_tick'], 1.01)

    def test_failed_disk_never_calls_exchange(self):
        from v9711_live.execution import journal_store
        with patch.object(journal_store, 'commit_journal', side_effect=OSError('simulated disk failure')):
            self.case.event('N0USDT')
            self.wait_done()
            self.assertFalse(self.sent)
            self.assertFalse(self.trade.tracked)  # Known-unsent disk failure is not UNKNOWN.
            try:
                self.trade.ledger.flush_pending().result(1)
            except OSError:
                pass  # Finish the deliberately failed background rejection save.
        # A journal with an uncertain partial append is poisoned until restart.
        # It must not append behind that tail, even after the test disk recovers.
        with self.assertRaises(OSError):
            self.trade.ledger.flush()
        self.trade.ledger._background.close()
        self.trade.ledger._background_closed = True

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

    def test_expired_quote_after_admission_does_not_cancel_send_but_account_gate_still_runs(self):
        self.case.templates = self.case.templates[:1]
        self.case.publish()
        started = self.block_disk()
        self.case.event('N0USDT')
        self.assertTrue(started.wait(1))
        self.trade.market.group_failed(1, 'quote stream disconnected after admission')
        self.trade._publish_entry_gate()
        with patch.object(self.trade.market, 'send_quote', side_effect=AssertionError('secondary WS read')):
            self.release.set()
            self.wait_done()
        self.assertEqual(len(self.sent), 1)

    def test_account_expiry_still_blocks_an_admitted_entry_after_disk_wait(self):
        self.case.templates = self.case.templates[:1]
        self.case.publish()
        started = self.block_disk()
        self.case.event('N0USDT')
        self.assertTrue(started.wait(1))
        self.trade._account_deadline = self.case.now-1
        self.release.set()
        self.wait_done()
        self.assertFalse(self.sent)

    def test_stalled_background_signal_read_cannot_send_after_publication_expires(self):
        self.case.templates = self.case.templates[:1]
        self.case.publish()
        self.trade.enable_signal_reader()
        reader = self.trade._signal_reader
        deadline = time.perf_counter()+1
        while not reader.healthy() and time.perf_counter() < deadline:
            threading.Event().wait(.001)
        self.assertTrue(reader.healthy())
        started = self.block_disk()
        self.trade.market.update('N0USDT', '1.01', int(self.case.now*1000))
        while not self.trade.market.events.empty():
            event = self.trade.market.events.get_nowait()
            if event is not None:
                self.trade.market.decide(event, self.trade.on_tick)
            self.trade.market.events.task_done()
        self.assertTrue(started.wait(1))
        reading, resume = threading.Event(), threading.Event()
        original = reader.source.read
        def stalled():
            reading.set()
            resume.wait(2)
            return original()
        try:
            with patch.object(reader.source, 'read', side_effect=stalled):
                self.assertTrue(reading.wait(1))
                reader._max_age = .01
                threading.Event().wait(.02)
                self.assertFalse(reader.healthy())
                self.release.set()
                self.wait_done()
                self.assertFalse(self.sent)
        finally:
            resume.set()

    def test_network_ticket_cannot_send_an_identity_absent_from_coalesced_snapshot(self):
        self.case.templates = self.case.templates[:1]
        self.case.publish()
        started = self.block_disk()
        # Occupy the active write so the entry ticket belongs to the pending
        # group, which is then superseded before any positive receipt.
        self.trade.ledger.state['_test_changed_snapshot'] = True
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
