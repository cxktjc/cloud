"""Temporary-state regressions for flash quotes, cancel isolation and durability."""
import json
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from tests import test_book_ticker_v2 as fixtures
from tests import benchmark_flash_entry
from tests.test_entry_tiers_v2 import row
from v9711_live import config as cfg
from v9711_live.execution import client as client_module
from v9711_live.execution.client import ExchangeClient, OwnershipLedger
from v9711_live.execution.logic import select_physical_orders
from v9711_live.execution.nonblocking_io import NetworkIO


class DurableSaveTests(unittest.TestCase):
    def ledger(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        root = Path(folder.name)
        return OwnershipLedger(root/'intents.json', root/'ledger.json')

    def test_only_changed_document_is_rewritten(self):
        ledger = self.ledger()
        ledger.save()
        with patch.object(client_module, 'atomic_write_json', wraps=client_module.atomic_write_json) as write:
            ledger.save()
            write.assert_not_called()
            ledger.state['realized_balance'] += 1
            ledger.save()
            self.assertEqual([c.args[0] for c in write.call_args_list], [ledger.state_path])
            write.reset_mock()
            ledger.intents['c:one'] = {'symbol': 'XUSDT'}
            ledger.save()
            self.assertEqual([c.args[0] for c in write.call_args_list], [ledger.path])

    def test_failed_second_replace_is_retried_without_losing_intent(self):
        ledger = self.ledger()
        ledger.save()
        ledger.intents['c:one'] = {'symbol': 'XUSDT'}
        ledger.state['tracked_orders']['XUSDT'] = {'client_order_id': 'one'}
        original = client_module.atomic_write_json
        def interrupted(path, value):
            if path == ledger.state_path:
                raise OSError('interrupted second durable replace')
            original(path, value)
        with patch.object(client_module, 'atomic_write_json', side_effect=interrupted):
            with self.assertRaises(OSError):
                ledger.save()
        self.assertIn('c:one', json.loads(ledger.path.read_text())['intents'])
        with patch.object(client_module, 'atomic_write_json', wraps=original) as write:
            ledger.save()
            self.assertEqual([c.args[0] for c in write.call_args_list], [ledger.state_path])
        self.assertIn('XUSDT', json.loads(ledger.state_path.read_text())['state']['tracked_orders'])

    def test_reserved_client_id_is_never_reused_after_restart(self):
        ledger = self.ledger()
        first = ledger.reserve_entry_client_id('XUSDT', 180000)
        restarted = OwnershipLedger(ledger.path, ledger.state_path)
        self.assertNotEqual(first, restarted.reserve_entry_client_id('XUSDT', 180000))


class PrecisionSnapshotTests(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.path = Path(folder.name)/'filters.json'
        self.write('.001', '1')
        self.client = ExchangeClient(enabled=False, exchange_info_cache_file=self.path)

    def write(self, tick, step):
        self.path.write_text(json.dumps({'symbols': {'XUSDT': dict(
            tick=tick, step=step, min_qty=step, min_notional='5')}}))

    def test_hot_preflight_uses_memory_and_caller_cannot_mutate_cache(self):
        self.client.refresh_metadata_snapshot()
        first = self.client.prepare_limit('XUSDT', .0179, 5)
        first['qty'] = 'corrupted'
        with patch.object(self.client._metadata_reader, 'read', side_effect=AssertionError('file read on hot path')):
            for _ in range(100):
                self.assertNotEqual(self.client.prepare_limit('XUSDT', .0179, 5)['qty'], 'corrupted')
            self.assertGreater(float(self.client.prepare_limit('XUSDT', .0179, 10)['qty']),
                               float(self.client.prepare_limit('XUSDT', .0179, 5)['qty']))

    def test_metadata_revision_and_unreadable_snapshot_invalidate_cache(self):
        self.client.refresh_metadata_snapshot()
        self.assertEqual(self.client.prepare_limit('XUSDT', .0179, 5)['price'], '0.017')
        self.write('.01', '10')
        self.client.refresh_metadata_snapshot()
        changed = self.client.prepare_limit('XUSDT', .0179, 5)
        self.assertEqual(changed['price'], '0.01')
        self.assertEqual(float(changed['qty']) % 10, 0)
        self.path.write_text('{invalid')
        self.client.refresh_metadata_snapshot()
        with self.assertRaisesRegex(ValueError, 'metadata missing'):
            self.client.prepare_limit('XUSDT', .0179, 5)


class IndependentRotationTests(unittest.TestCase):
    def test_another_queued_cancel_cannot_block_relative_replacement(self):
        tracked = {f'OLD{i}': row(f'OLD{i}', .01) for i in range(6)}
        tracked['OLD0'] = dict(row('OLD0', .02), cancel_requested=True)
        tracked['OLD1'] = row('OLD1', .019)
        plan = select_physical_orders([*tracked.values(), row('NEW', .0167)], tracked)
        self.assertEqual(plan.cancel, ('OLD1',))
        self.assertEqual([r.symbol for r in plan.place], ['NEW'])
        self.assertEqual(plan.rotation_placement, 'NEW')
        self.assertGreater(plan.place[0].gap, cfg.FILL_PLACE_GAP)
        self.assertLessEqual(len(tracked)+len(plan.place), 8)

    def test_pruned_unsent_cancel_wakes_owner_and_releases_scheduler(self):
        io = NetworkIO(workers=2)
        self.addCleanup(io.shutdown)
        io._entry_cancel_after = time.monotonic()+60
        applied = []
        key = ('cancel', 'XUSDT', 'cid', 'oid')
        io.submit(key, lambda: self.fail('pruned cancel must not send'), priority=0,
                  done=lambda value, error: applied.append((value, error)), entry_cancel=True)
        io.update_entry_cancels({})
        self.assertEqual(io.drain(), 1)
        self.assertFalse(io.contains(key))
        self.assertEqual(applied, [({'_cancel_not_sent': True}, None)])

    def test_pending_rotation_cannot_block_another_free_slot(self):
        tracked = {f'OLD{i}': row(f'OLD{i}', .01) for i in range(8)}
        tracked['OLD7'] = row('OLD7', .0185)
        first = select_physical_orders([*tracked.values(), row('NEW', .0163)], tracked)
        tracked.pop('OLD0')  # A different order has confirmed terminal.
        waiting = select_physical_orders([*tracked.values(), row('NEW', .03), row('OTHER', .0164)],
                                         tracked, pending_rotation=first.rotation)
        self.assertEqual([r.symbol for r in waiting.place], ['OTHER'])
        self.assertEqual(waiting.rotation_placement, '')
        self.assertIn('OLD7', waiting.cancel)
        self.assertLessEqual(len(tracked)+len(waiting.place), 8)

    def test_legacy_five_to_four_reservation_keeps_original_improvement(self):
        tracked = {f'OLD{i}': row(f'OLD{i}', .01) for i in range(4)}
        pending = dict(victim='OLD4', victim_client_order_id='exact_OLD4',
            victim_terminal=True, victim_filled=False, victim_gap=.02,
            original_order_count=5, order_bar_open_ms=180000, expires_at_ms=360000,
            challenger='NEW')
        plan = select_physical_orders([*tracked.values(), row('NEW', .0178)], tracked, pending_rotation=pending)
        self.assertEqual([r.symbol for r in plan.place], ['NEW'])
        self.assertEqual(plan.rotation_placement, 'NEW')

    def test_missing_unknown_victim_still_reserves_its_own_slot(self):
        tracked = {f'OLD{i}': row(f'OLD{i}', .01) for i in range(8)}
        tracked['OLD7'] = row('OLD7', .02)
        pending = select_physical_orders([*tracked.values(), row('NEW', .0178)], tracked).rotation
        for symbol in ('OLD5', 'OLD6', 'OLD7'):
            tracked.pop(symbol)
        plan = select_physical_orders([*tracked.values(), row('NEW', .03),
            *[row(f'OTHER{i}', .01) for i in range(3)]], tracked, pending_rotation=pending)
        self.assertEqual(len(plan.place), 2)
        self.assertLessEqual(len(tracked)+len(plan.place)+1, 8)


class FlashOwnerTests(unittest.TestCase):
    setUp = fixtures.DualAdmissionTests.setUp
    target = fixtures.DualAdmissionTests.target
    make_trade = fixtures.DualAdmissionTests.make_trade
    refresh_quotes = fixtures.DualAdmissionTests.refresh_quotes
    place = fixtures.DualAdmissionTests.place
    cancel = fixtures.DualAdmissionTests.cancel
    publish = fixtures.DualAdmissionTests.publish

    def plans(self, count):
        self.targets = []
        self.templates = [dict(symbol=f'N{i}USDT', limit_price=1., notional=5.,
            order_bar_open_ms=self.bar, reference_bar_open_ms=self.bar-cfg.BAR_MS,
            exit_due_epoch=self.now+900) for i in range(count)]
        self.publish()

    def event(self, symbol, price):
        self.trade.market.update(symbol, str(price), int(self.now*1000))
        return self.trade.market.decide(self.trade.market.events.get_nowait(), self.trade.on_tick)

    def test_no_rank_rebuild_for_heartbeat_but_changed_template_rebuilds(self):
        self.plans(8)
        self.trade.desired_snapshot(now=self.now)
        revision = self.trade._template_revision
        payload = dict(self.payload, heartbeat_only='new-file-same-templates')
        cfg.DESIRED_FILE.write_text(json.dumps(payload))
        with patch.object(self.trade, '_update_rank', wraps=self.trade._update_rank) as update:
            self.trade.desired_snapshot(now=self.now)
            update.assert_not_called()
            self.templates[0]['limit_price'] = .9
            self.publish()
            self.trade.desired_snapshot(now=self.now)
            self.assertGreater(update.call_count, 0)
        self.assertGreater(self.trade._template_revision, revision)

    def test_noncompetitive_quote_skips_work_but_short_drop_is_not_skipped(self):
        self.plans(9)
        self.event('N0USDT', 1.01)
        with patch.object(self.trade, '_execute_entries', side_effect=AssertionError('unneeded full planner')):
            self.event('N8USDT', 1.0101)
        self.assertGreater(self.trade._skipped_noncompetitive_events, 0)
        self.event('N8USDT', .99)
        self.assertEqual(len(self.cancels), 1)
        self.event('N8USDT', .99)
        self.assertIn('N8USDT', self.trade.tracked)
        self.assertEqual(len(self.trade.tracked), 8)

    def test_empty_book_outside_tier_skips_work_but_inclusive_crossing_sends(self):
        self.plans(9)
        for template in self.templates:
            self.trade.market.update(template['symbol'], '1.02', int(self.now*1000))
            self.trade.market.decide(self.trade.market.events.get_nowait(), lambda event: None)
        self.trade.desired_snapshot(now=self.now)
        with patch.object(self.trade, '_execute_entries', side_effect=AssertionError('outside admission tier')):
            self.event('N8USDT', 1.0201)
        self.assertFalse(self.trade.tracked)
        self.event('N8USDT', 1.0185)
        self.assertIn('N8USDT', self.trade.tracked)

    def test_send_guard_sees_received_rebound_without_advancing_owner(self):
        market = self.trade.market
        self.event('N0USDT', .99)
        market.update('N0USDT', '1.03', int(self.now*1000))
        self.assertEqual(market.entry_quote('N0USDT')['selected'], .99)
        self.assertEqual(market.send_quote('N0USDT')['selected'], 1.03)
        self.assertEqual(market.processed['N0USDT'].price, '0.99')

    def test_send_guard_keeps_either_source_rule_and_zero_bid_revokes(self):
        market = self.trade.market
        market.book.group_connected(1)
        market.update('N0USDT', '1.03', int(self.now*1000))
        market.update_book('N0USDT', '.99', '2', int(self.now*1000), 1)
        self.assertEqual(market.send_quote('N0USDT')['selected'], .99)
        market.update_book('N0USDT', '.99', '0', int(self.now*1000), 2)
        self.assertIsNone(market.send_quote('N0USDT')['bid0'])
        self.assertEqual(market.send_quote('N0USDT')['selected'], 1.03)

    def test_send_guard_applies_queue_age_budget_after_registration(self):
        market = self.trade.market
        self.event('N0USDT', .99)
        clock = market._quote_clock()
        with patch.object(market, '_quote_clock', return_value=(clock[0], clock[1], clock[2]+1_000_000_000)):
            self.assertEqual(market.entry_quote('N0USDT')['selected'], .99)
            self.assertIsNone(market.send_quote('N0USDT')['selected'])

    def check_cancel_independence(self, *, cooling):
        self.plans(5)
        self.event('N0USDT', 1.01)
        self.event('N0USDT', 1.02)
        self.event('N1USDT', 1.02)
        release, started, sent, second_sent = (threading.Event() for _ in range(4))
        io = NetworkIO(workers=2)
        self.trade._network_io = self.trade.client.network_io = io
        def cancel(**kwargs):
            started.set()
            if not release.wait(2):
                raise AssertionError('test cancel was not released')
            return self.cancel(**kwargs)
        def place(**kwargs):
            value = self.place(**kwargs)
            if kwargs['symbol'] == 'N5USDT':
                sent.set()
            if kwargs['symbol'] == 'N6USDT':
                second_sent.set()
            return value
        self.trade.client.cancel = Mock(side_effect=cancel)
        self.trade.client.place_limit_for_bar = Mock(side_effect=place)
        self.trade._publish_entry_gate()
        if cooling:
            io._entry_cancel_after = time.monotonic()+60  # No real 1.5s sleep needed.
        try:
            self.plans(6)
            self.event('N5USDT', .99)
            self.assertTrue(sent.wait(.5), 'a free-slot entry waited behind cancellation')
            self.assertIn('N1USDT', self.trade.tracked)
            self.assertIn('N5USDT', self.trade.tracked)
            self.assertEqual(self.trade._ordinary_pending_count(), 6)
            if cooling:
                self.assertFalse(started.is_set())
            else:
                self.assertTrue(started.wait(.5))
            self.assertFalse(release.is_set())
            # A second replacement above the normal 1.65% tier must not be
            # blocked just because another victim is already queued/running.
            self.plans(7)
            self.event('N6USDT', 1.0177)
            self.assertTrue(second_sent.wait(.5))
            self.assertEqual(self.trade._ordinary_pending_count(), 7)
            self.assertTrue(all(self.trade.tracked[s].get('cancel_requested') for s in ('N0USDT', 'N1USDT')))
            self.assertGreater(self.trade.tracked['N6USDT']['entry_gap_ceiling'], cfg.FILL_PLACE_GAP)
        finally:
            release.set()
            self.trade.shutdown_io()

    def test_cancel_cooldown_cannot_block_free_slot_send(self):
        self.check_cancel_independence(cooling=True)

    def test_slow_cancel_ack_cannot_block_free_slot_send(self):
        self.check_cancel_independence(cooling=False)

    def test_default_first_send_does_not_wait_for_second_registration(self):
        self.plans(8)
        first_sent = threading.Event()
        io = NetworkIO(workers=2)
        self.trade._network_io = self.trade.client.network_io = io
        original_reserve = self.trade.ledger.reserve_entry_client_id
        def reserve(symbol, bar):
            if symbol == 'N1USDT':
                self.assertTrue(first_sent.wait(1), 'first send was held until the rest registered')
            return original_reserve(symbol, bar)
        def place(**kwargs):
            result = self.place(**kwargs)
            if kwargs['symbol'] == 'N0USDT':
                first_sent.set()
            return result
        self.trade.ledger.reserve_entry_client_id = reserve
        self.trade.client.place_limit_for_bar = Mock(side_effect=place)
        try:
            with patch.dict(os.environ):
                os.environ.pop('V9711_LIVE_ENTRY_BATCH', None)
                self.event('N0USDT', 1.01)
            self.assertTrue(first_sent.is_set())
            self.assertEqual(self.trade._ordinary_pending_count(), 8)
        finally:
            self.trade.shutdown_io()


class ReboundTransportTests(unittest.TestCase):
    def test_already_received_rebound_prevents_actual_adapter_dispatch(self):
        result = benchmark_flash_entry.probe(1, True, rebound=True)
        self.assertFalse(result['received_rebound_but_guard_used_old_quote'])
        self.assertEqual(result['sends'], [])
        self.assertEqual(result['pending_after_callbacks'], 0)
