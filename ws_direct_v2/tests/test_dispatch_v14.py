"""Durable append boundaries and immutable entry-price evidence."""
from copy import deepcopy
from pathlib import Path
import random
import tempfile
import threading
import unittest
from unittest.mock import patch

from v9711_live.execution import journal_store as journal
from v9711_live.execution.client import OwnershipLedger
from v9711_live.execution.entry_timing import EntryTimings
from v9711_live.execution.terminal import entry_timing_lines
from tests import test_book_ticker_v2 as fixtures


class CopyOnWriteTests(unittest.TestCase):
    def test_shared_prefix_updates_delete_and_ancestor_replacement_keep_prior_roots(self):
        old = dict(intents={'a': {'n': 1}, 'b': {'n': 2}}, state={'book': {'x': 1}, 'audit': [1]})
        original = deepcopy(old)
        replacement = {'new': {'x': 9}}
        operations = [[['intents', 'a', 'n'], 3], [['intents', 'b']],
            [['state', 'book', 'x'], 2], [['state', 'book'], replacement],
            [['state', 'book', 'new', 'x'], 10], [['state', 'book', 'second'], 11]]
        new = journal.apply_changes(old, operations)
        self.assertEqual(old, original)
        self.assertEqual(replacement, {'new': {'x': 9}})
        self.assertEqual(new['intents'], {'a': {'n': 3}})
        self.assertEqual(new['state']['book'], {'new': {'x': 10}, 'second': 11})
        self.assertIs(new['state']['audit'], old['state']['audit'])

    def test_transactions_match_sequential_reference_with_retained_checkpoints(self):
        rng = random.Random(14)
        root = dict(intents={}, state={'rows': {}})
        expected = deepcopy(root)
        for revision in range(100):
            prior, frozen = root, deepcopy(root)
            operations = []
            for _ in range(20):
                branch = rng.choice(['intents', 'state'])
                path = [branch] + (['rows'] if branch == 'state' else [])
                key = str(rng.randrange(20))
                target = expected[branch] if branch == 'intents' else expected['state']['rows']
                if key in target and rng.randrange(3) == 0:
                    del target[key]
                    operations.append([path+[key]])
                else:
                    value = dict(revision=revision, values=[rng.randrange(99)])
                    target[key] = deepcopy(value)
                    operations.append([path+[key], value])
            root = journal.apply_changes(root, operations)
            self.assertEqual(root, expected)
            self.assertEqual(prior, frozen)

    def test_invalid_parent_or_deletion_never_mutates_original_root(self):
        root = dict(intents={}, state={'x': 1})
        for operation in ([['state', 'x', 'n'], 2], [['intents', 'missing']]):
            with self.assertRaises(ValueError):
                journal.apply_changes(root, [[['state', 'first'], 9], operation])
            self.assertEqual(root, dict(intents={}, state={'x': 1}))


class JournalSyncTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.ledger = OwnershipLedger(root/'intents.json', root/'state.json')
        self.ledger.enable_background(incremental=True)
        self.addCleanup(self.ledger.close_background)

    def test_data_sync_blocks_positive_receipt_and_exports_stage_times(self):
        entered, release = threading.Event(), threading.Event()
        real_sync = journal.os.fsync
        def sync(fd):
            entered.set()
            if not release.wait(3):
                raise TimeoutError('test sync never released')
            real_sync(fd)
        with patch.object(journal.os, 'fdatasync', sync, create=True):
            self.ledger.state['revision'] = 1
            self.ledger.save()
            future = self.ledger.flush_pending()
            try:
                self.assertTrue(entered.wait(2))
                self.assertFalse(future.done())
            finally:
                release.set()
            receipt = future.result(3)
        self.assertLessEqual(receipt.write_started_ns, receipt.prepared_ns)
        self.assertLessEqual(receipt.prepared_ns, receipt.written_ns)
        self.assertLessEqual(receipt.written_ns, receipt.synced_ns)
        self.assertLessEqual(receipt.synced_ns, receipt.completed_ns)
        self.assertEqual(self.ledger._background.health()['sync_method'], 'fdatasync')
        self.assertEqual(OwnershipLedger(self.ledger.path, self.ledger.state_path).state['revision'], 1)

    def test_sync_error_grants_no_receipt_and_poisoned_writer_stays_closed(self):
        with patch.object(journal.os, 'fdatasync', side_effect=OSError('sync failed'), create=True):
            self.ledger.state['revision'] = 1
            self.ledger.save()
            with self.assertRaisesRegex(OSError, 'sync failed'):
                self.ledger.flush_pending().result(3)
        with self.assertRaises(OSError):
            self.ledger.flush_for_dispatch().result(1)
        self.ledger._background.close()
        self.ledger._background_closed = True

    def test_fsync_fallback_and_full_segment_sync_remain_available(self):
        with patch.object(journal.os, 'fdatasync', None, create=True):
            self.ledger.state['revision'] = 1
            self.ledger.flush()
            self.assertEqual(self.ledger._background.health()['sync_method'], 'fsync')
        with patch.object(journal.os, 'fdatasync', side_effect=AssertionError('header used data-only sync'), create=True):
            root = Path(self.tmp.name)/'new'
            writer = journal.JournalWriter(root/'intents.json', root/'state.json',
                initial=self.ledger._background._root)
            self.addCleanup(writer.close)


class GapEvidenceTests(unittest.TestCase):
    def register(self, *, trigger='XUSDT', price=1.01, limit=1., admission=1.005):
        timings = EntryTimings()
        timings.register('XUSDT', 'cid', dict(limit_price=1.0009, submitted_limit_price=limit,
            order_bar_open_ms=180000, _entry_timing_context=dict(trigger_symbol=trigger,
                trigger_source='bid0', trigger_price=price, admission_price=admission)))
        return timings

    def test_gap_uses_actual_rounded_limit_and_frozen_trigger_price(self):
        timings = self.register()
        row = timings.order('cid')
        self.assertAlmostEqual(row['trigger_gap'], .01)
        self.assertAlmostEqual(row['admission_gap'], .005)
        self.assertEqual(row['order_limit_price'], 1.)
        panel = '\n'.join(entry_timing_lines(row))
        self.assertIn('触发价=1.01 挂单价=1 触发价差=+1.0000%', panel)
        self.assertIn('准入价差=+0.5000%', panel)
        timings.register('XUSDT', 'cid', dict(limit_price=2.))
        self.assertEqual(timings.order('cid')['trigger_price'], 1.01)

    def test_cross_symbol_trigger_has_no_numerically_meaningless_gap(self):
        row = self.register(trigger='BTCUSDT', price=90000.).order('cid')
        self.assertIsNone(row['trigger_gap'])
        self.assertAlmostEqual(row['admission_gap'], .005)
        panel = '\n'.join(entry_timing_lines(row))
        self.assertIn('币对=XUSDT 本币准入价=1.005', panel)
        self.assertIn('跨币仅唤醒评估，不参与本币比价', panel)
        self.assertNotIn('触发价差=', panel)

    def test_negative_and_missing_quotes_are_distinguished(self):
        row = self.register(price=.99).order('cid')
        self.assertIn('触发价差=-1.0000%', '\n'.join(entry_timing_lines(row)))
        row = self.register(price=None, admission=None, limit=None).order('cid')
        self.assertIsNone(row['trigger_gap'])
        self.assertIn('触发价差=未记录', '\n'.join(entry_timing_lines(row)))

    def test_sync_substages_survive_public_snapshot_and_summary(self):
        timings = self.register()
        for stage, ms in [('write_started', 1), ('prepared', 2), ('written', 3),
                          ('synced', 14), ('persisted', 15), ('send', 16)]:
            timings.mark_at('cid', stage, ms*1_000_000, ms)
        row = timings.order('cid')
        self.assertEqual(row['latencies_ms']['journal_sync'], 11.)
        self.assertEqual(timings.summary()['stages']['journal_sync']['p95_ms'], 11.)
        self.assertIn('准备=1.000ms 写入=1.000ms 同步=11.000ms 回执=1.000ms',
                      '\n'.join(entry_timing_lines(row)))


class LiveGapContextTests(unittest.TestCase):
    setUp = fixtures.DualAdmissionTests.setUp
    target = fixtures.DualAdmissionTests.target
    make_trade = fixtures.DualAdmissionTests.make_trade
    refresh_quotes = fixtures.DualAdmissionTests.refresh_quotes
    place = fixtures.DualAdmissionTests.place
    cancel = fixtures.DualAdmissionTests.cancel
    publish = fixtures.DualAdmissionTests.publish
    event = fixtures.DualAdmissionTests.event
    held = fixtures.DualAdmissionTests.held
    ordinary_plan = fixtures.DualAdmissionTests.ordinary_plan
    plans = fixtures.DualAdmissionTests.plans
    tick = fixtures.DualAdmissionTests.tick
    bid = fixtures.DualAdmissionTests.bid

    def test_tick_and_bid_capture_event_price_without_post_send_price_reads(self):
        self.plans(1)
        self.tick(1.01)
        first_cid = self.trade.tracked['N0USDT']['client_order_id']
        self.templates.append(dict(self.templates[0], symbol='N1USDT'))
        self.publish()
        self.bid(.99, symbol='N1USDT')
        second_cid = self.trade.tracked['N1USDT']['client_order_id']
        for symbol, cid, source, price in [('N0USDT', first_cid, 'tick', 1.01), ('N1USDT', second_cid, 'bid0', .99)]:
            timing = self.trade.session_entries.timings.order(cid)
            self.assertEqual(timing['trigger_symbol'], symbol)
            self.assertEqual(timing['trigger_source'], source)
            self.assertEqual(timing['trigger_price'], price)
            self.assertEqual(timing['order_limit_price'], 1.)
            self.assertAlmostEqual(timing['trigger_gap'], price-1.)


if __name__ == '__main__':
    unittest.main()
