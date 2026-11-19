import json
import os
import subprocess
import sys
import unittest

from v9711_live import config as cfg
from v9711_live.entry_policy import GapPolicy
from v9711_live.execution.logic import select_physical_orders


def row(symbol, gap):
    return dict(symbol=symbol, limit_price=1, observed_price=1+gap, gap=gap,
                notional=5, order_bar_open_ms=180000, reference_bar_open_ms=0,
                exit_due_epoch=1080, client_order_id='exact_'+symbol, order_id=symbol)


def select(count, old_gap, new_gap):
    tracked = {f'OLD{i}': row(f'OLD{i}', old_gap) for i in range(count)}
    return select_physical_orders([*tracked.values(), row('NEW', new_gap)], tracked)


class EntryTierTests(unittest.TestCase):
    def test_first_five_admit_1p85_percent_inclusive(self):
        self.assertEqual([r.symbol for r in select(4, .01, .0185).place], ['NEW'])
        self.assertFalse(select(4, .01, .018501).place)

    def test_sixth_to_eighth_admit_1p65_percent(self):
        for count in (5, 6, 7):
            self.assertEqual([r.symbol for r in select(count, .01, .0165).place], ['NEW'])
            self.assertFalse(select(count, .01, .016501).place)

    def test_every_nonempty_count_rotates_at_inclusive_0p30pp(self):
        for count in range(1, 9):
            with self.subTest(count=count):
                plan = select(count, .020, .017)
                self.assertEqual(plan.cancel, (f'OLD{count-1}',))
                self.assertFalse(plan.place)

    def test_crowded_rotation_bypasses_all_old_admission_ceilings(self):
        for count in range(5, 9):
            with self.subTest(count=count):
                plan = select(count, .021, .0188)
                self.assertEqual(len(plan.cancel), 1)
                self.assertFalse(plan.place)
                self.assertEqual(plan.rotation['admission_gap'], .021)
                self.assertAlmostEqual(plan.rotation['minimum_improvement'], .0022)
                self.assertFalse(select(count, .021, .018801).cancel)

    def test_sparse_margin_is_0p30pp_not_0p22pp(self):
        for count in range(1, 5):
            with self.subTest(count=count):
                self.assertFalse(select(count, .020, .017001).cancel)
                self.assertFalse(select(count, .020, .0178).cancel)

    def test_old_exception_no_longer_overrides_minimum_improvement(self):
        policy = GapPolicy()
        self.assertFalse(policy.beats(.01651, .01449, count=8))
        self.assertFalse(select(8, .01651, .01449).cancel)

    def test_rotation_waits_for_exact_cancel_terminal_and_keeps_original_count(self):
        tracked = {f'OLD{i}': row(f'OLD{i}', .01) for i in range(5)}
        tracked['OLD4'] = row('OLD4', .020)
        new = row('NEW', .0178)
        first = select_physical_orders([*tracked.values(), new], tracked)
        pending = dict(first.rotation)
        self.assertEqual(first.cancel, ('OLD4',))
        self.assertFalse(first.place)
        self.assertFalse(select_physical_orders([new], tracked, pending_rotation=pending).place)
        tracked.pop('OLD4')
        unknown = select_physical_orders([*tracked.values(), new], tracked, pending_rotation=pending)
        self.assertFalse(unknown.place)
        self.assertIn('rotation_victim_fate_unknown', [a.reason for a in unknown.actions])
        pending['victim_terminal'] = True
        complete = select_physical_orders([*tracked.values(), new], tracked, pending_rotation=pending)
        self.assertEqual([r.symbol for r in complete.place], ['NEW'])
        self.assertEqual(complete.rotation_action, 'consume')
        # A worse fresh quote must not inherit the qualification at cancellation.
        self.assertFalse(select_physical_orders([*tracked.values(), row('NEW', .017801)],
            tracked, pending_rotation=pending).place)
        # Existing v6 reservations use the newly confirmed v7 rules on restart.
        pending['admission_gap'] = .0145
        pending.pop('policy_version', None)
        self.assertEqual([r.symbol for r in select_physical_orders([*tracked.values(), new],
            tracked, pending_rotation=pending).place], ['NEW'])
        pending['victim_filled'] = True
        filled = select_physical_orders([*tracked.values(), new], tracked, pending_rotation=pending)
        self.assertFalse(filled.place)
        self.assertEqual(filled.rotation_action, 'clear')

    def test_underfilled_rotation_precedes_vacancy_fill(self):
        plan = select(5, .0185, .0135)
        self.assertEqual(len(plan.cancel), 1)
        self.assertFalse(plan.place)
        self.assertEqual(plan.rotation['original_order_count'], 5)

    def test_ordinary_gap_cancel_is_strictly_beyond_2p1_percent(self):
        for gap, expected in ((.021, ()), (.021001, ('OLD',))):
            with self.subTest(gap=gap):
                old = row('OLD', gap)
                plan = select_physical_orders([old], {'OLD': old})
                self.assertEqual(plan.cancel, expected)
                if expected:
                    self.assertIn('entry_gap_too_far', [a.reason for a in plan.actions])

    def test_gap_cancel_uses_fresh_tracked_quote_without_signal(self):
        old = dict(row('OLD', .022), _quote_fresh=True)
        plan = select_physical_orders([], {'OLD': old}, signal_ready=False)
        self.assertEqual(plan.cancel, ('OLD',))
        old['_quote_fresh'] = False
        self.assertFalse(select_physical_orders([], {'OLD': old}, signal_ready=False).cancel)

    def test_legacy_environment_cannot_restore_old_gaps_or_short_history(self):
        env = dict(os.environ, V9711_LIVE_HISTORY_BARS='40',
                   V9711_LIVE_PLACE_GAP='.018', V9711_LIVE_FILL_PLACE_GAP='.020',
                   V9711_LIVE_BOOTSTRAP_PLACE_GAP='.022', V9711_LIVE_SWITCH_TOLERANCE='.005',
                   V9711_LIVE_CANCEL_GAP='.05')
        script = ('import json; from v9711_live import config as c; '
                  'print(json.dumps([c.HISTORY_BARS,c.BOOTSTRAP_PLACE_GAP,c.FILL_PLACE_GAP,c.PLACE_GAP,'
                  'c.SWITCH_TOLERANCE,c.CROWDED_SWITCH_TOLERANCE,c.CANCEL_GAP]))')
        result = subprocess.run([sys.executable, '-B', '-c', script], env=env,
                                check=True, capture_output=True, text=True)
        self.assertEqual(json.loads(result.stdout), [96, .0185, .0165, .0145, .003, .0022, .021])
