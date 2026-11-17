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
    def test_first_five_admit_2p15_percent_inclusive(self):
        self.assertEqual([r.symbol for r in select(4, .01, .0215).place], ['NEW'])
        self.assertFalse(select(4, .01, .021501).place)

    def test_sixth_to_eighth_admit_1p95_percent(self):
        for count in (5, 6, 7):
            self.assertEqual([r.symbol for r in select(count, .01, .0195).place], ['NEW'])
            self.assertFalse(select(count, .01, .019501).place)

    def test_full_book_1p75_ceiling_and_cancel_before_place(self):
        plan = select(8, .03, .0175)
        self.assertEqual(len(plan.cancel), 1)
        self.assertFalse(plan.place)
        self.assertEqual(plan.rotation['admission_gap'], .0175)
        self.assertFalse(select(8, .03, .017501).cancel)

    def test_exception_new_below_1p75_old_above_1p95_is_strict(self):
        policy = GapPolicy()
        self.assertTrue(policy.beats(.01951, .01749))
        self.assertFalse(policy.beats(.0195, .01749))
        self.assertFalse(policy.beats(.01951, .0175))

    def test_half_percentage_point_equality_still_does_not_rotate(self):
        policy = GapPolicy()
        self.assertFalse(policy.beats(.0225, .0175))
        self.assertTrue(policy.beats(.022501, .0175))

    def test_underfilled_rotation_precedes_vacancy_fill(self):
        plan = select(5, .0215, .0165)
        self.assertEqual(len(plan.cancel), 1)
        self.assertFalse(plan.place)
        self.assertEqual(plan.rotation['original_order_count'], 5)

    def test_tighter_policy_does_not_cancel_valid_incumbent_for_gap_alone(self):
        old = row('OLD', .05)
        plan = select_physical_orders([old], {'OLD': old})
        self.assertEqual(plan.keep, ('OLD',))
        self.assertFalse(plan.cancel)

    def test_legacy_environment_cannot_restore_old_gaps_or_short_history(self):
        env = dict(os.environ, V9711_LIVE_HISTORY_BARS='40',
                   V9711_LIVE_PLACE_GAP='.018', V9711_LIVE_FILL_PLACE_GAP='.020',
                   V9711_LIVE_BOOTSTRAP_PLACE_GAP='.022')
        script = ('import json; from v9711_live import config as c; '
                  'print(json.dumps([c.HISTORY_BARS,c.BOOTSTRAP_PLACE_GAP,c.FILL_PLACE_GAP,c.PLACE_GAP]))')
        result = subprocess.run([sys.executable, '-B', '-c', script], env=env,
                                check=True, capture_output=True, text=True)
        self.assertEqual(json.loads(result.stdout), [96, .0215, .0195, .0175])
