"""Tiered pending-entry admission; gaps are signed fractions of frozen limit.

Signal and trade keep independent three-position books. Trade owns eight
ordinary pending orders plus the separately managed signal-follow entries;
during the final pre-exit bar, ordinary admission ignores the position limit.
"""
from __future__ import annotations

import math
from dataclasses import dataclass

from . import config as cfg


def at_most(value: float, ceiling: float) -> bool:
    return value <= ceiling or math.isclose(value, ceiling, rel_tol=0, abs_tol=1e-12)


def strictly_above(value: float, floor: float) -> bool:
    return value > floor and not math.isclose(value, floor, rel_tol=0, abs_tol=1e-12)


@dataclass(frozen=True)
class GapPolicy:
    bootstrap_gap: float = cfg.BOOTSTRAP_PLACE_GAP
    fill_gap: float = cfg.FILL_PLACE_GAP
    strict_gap: float = cfg.PLACE_GAP
    bootstrap_orders: int = cfg.BOOTSTRAP_PENDING_ORDERS
    improvement: float = cfg.SWITCH_TOLERANCE

    def __post_init__(self):
        if not (all(math.isfinite(v) for v in (self.bootstrap_gap, self.fill_gap,
                 self.strict_gap, self.improvement))
                and 0 <= self.strict_gap <= self.fill_gap <= self.bootstrap_gap < 1
                and 0 <= self.improvement < 1 and self.bootstrap_orders >= 1):
            raise ValueError('Invalid tiered pending-order policy')

    def admission_gap(self, count: int) -> float:
        return self.bootstrap_gap if count < self.bootstrap_orders else self.fill_gap

    def rotation_gap(self, count: int, capacity: int) -> float:
        return self.strict_gap if count >= capacity else self.fill_gap

    def beats(self, old_gap: float, new_gap: float) -> bool:
        # Strict >0.5 percentage point; equality must NOT rotate. The exception
        # is strict on both ends too: new <1.6%, old >1.8%.
        exception = (strictly_above(self.strict_gap, new_gap)
                     and strictly_above(old_gap, self.fill_gap))
        return exception or strictly_above(old_gap - new_gap, self.improvement)

    def replacement_allowed(self, old_gap: float, new_gap: float, *, ceiling: float) -> bool:
        return at_most(new_gap, ceiling) and self.beats(old_gap, new_gap)

    def describe(self) -> dict:
        return dict(version=4, bootstrap_order_count=self.bootstrap_orders,
                    bootstrap_gap=self.bootstrap_gap, fill_gap=self.fill_gap,
                    full_rotation_gap=self.strict_gap,
                    improvement_strictly_greater_than=self.improvement,
                    exception_new_strictly_below=self.strict_gap,
                    exception_old_strictly_above=self.fill_gap,
                    underfilled_rotation_before_vacancy_fill=True,
                    cancel_terminal_before_replacement=True)
