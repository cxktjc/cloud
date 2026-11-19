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
    crowded_improvement: float = cfg.CROWDED_SWITCH_TOLERANCE
    cancel_gap: float = cfg.CANCEL_GAP

    def __post_init__(self):
        if not (all(math.isfinite(v) for v in (self.bootstrap_gap, self.fill_gap,
                 self.strict_gap, self.improvement, self.crowded_improvement, self.cancel_gap))
                and 0 <= self.strict_gap <= self.fill_gap <= self.bootstrap_gap < 1
                and 0 <= self.crowded_improvement <= self.improvement < 1
                and self.bootstrap_gap <= self.cancel_gap < 1 and self.bootstrap_orders >= 1):
            raise ValueError('Invalid tiered pending-order policy')

    def admission_gap(self, count: int) -> float:
        return self.bootstrap_gap if count < self.bootstrap_orders else self.fill_gap

    def rotation_gap(self, count: int, capacity: int) -> float:
        # Replacement is independent of initial/fill admission tiers.
        return self.cancel_gap

    def minimum_improvement(self, count: int) -> float:
        return self.crowded_improvement if count >= self.bootstrap_orders else self.improvement

    def beats(self, old_gap: float, new_gap: float, *, count: int = 0) -> bool:
        # Inclusive 0.30pp at every count; inclusive 0.22pp with >=5 orders.
        return at_most(self.minimum_improvement(count), old_gap - new_gap)

    def replacement_allowed(self, old_gap: float, new_gap: float, *, ceiling: float,
                            count: int = 0) -> bool:
        return at_most(new_gap, ceiling) and self.beats(old_gap, new_gap, count=count)

    def describe(self) -> dict:
        return dict(version=7, bootstrap_order_count=self.bootstrap_orders,
                    bootstrap_gap=self.bootstrap_gap, fill_gap=self.fill_gap,
                    full_rotation_gap=self.cancel_gap,
                    improvement_at_least=self.improvement,
                    crowded_improvement_at_least=self.crowded_improvement,
                    cancel_gap=self.cancel_gap, cancel_gap_strict=True,
                    rotation_ignores_admission_tiers=True,
                    underfilled_rotation_before_vacancy_fill=True,
                    cancel_terminal_before_replacement=True)
