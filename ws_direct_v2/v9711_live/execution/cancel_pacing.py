"""Distance-ranked entry cancellations without sleeps or worker reservation.

Only entry-order cancels opt in. Exit/QUEUE safety cancels, queries and entries
keep their own network lanes. One paced cancel is in flight at a time, then a
monotonic 1.5s quiet period starts AFTER its request returns (success or failure).
All shared scheduler metadata is protected by NetworkIO's condition lock.
"""
from __future__ import annotations

import heapq
import math
import time
from typing import Any, Callable, Mapping

ENTRY_CANCEL_INTERVAL_SECONDS = 1.5


def exact_entry_cancel_key(symbol: str, client_order_id: Any, order_id: Any) -> tuple:
    # A deterministic CID can be reused after confirmed absence. An old queued
    # cancel must not cancel a later exchange order that happens to reuse it.
    return ('cancel', symbol, str(client_order_id or ''), str(order_id or ''))


class EntryCancelPacing:
    """Small scheduler mixin; caller owns _jobs and _condition, not trade state."""

    def _init_entry_cancels(self, *, interval: float,
                            clock: Callable[[], float] | None = None) -> None:
        if not math.isfinite(interval) or interval < 0:
            raise ValueError('Entry cancel interval must be finite and nonnegative')
        self._entry_cancel_interval = float(interval)
        self._entry_cancel_clock = clock or time.monotonic
        self._entry_cancel_after = float('-inf')
        self._entry_cancel_hold = 0
        self._entry_cancel_orders: dict[tuple, dict[str, Any]] | None = None

    def hold_entry_cancels(self) -> None:
        self._assert_owner()
        with self._condition:
            self._entry_cancel_hold += 1

    def release_entry_cancels(self) -> None:
        self._assert_owner()
        with self._condition:
            if self._entry_cancel_hold <= 0:
                raise RuntimeError('Unbalanced entry cancel collection')
            self._entry_cancel_hold -= 1
            self._condition.notify_all()

    def update_entry_cancels(self, orders: Mapping[tuple, Mapping[str, Any]]) -> None:
        """Owner publishes immutable exact identities + already-fresh local quotes.

        Prune an unsent cancel if its entry has filled/retired/replaced. This is
        not a successful cancellation and never consumes the 1.5s rate budget.
        """
        self._assert_owner()
        with self._condition:
            self._entry_cancel_orders = {key: dict(row) for key, row in orders.items()}
            for job in self._jobs.values():
                if (job.entry_cancel and job.state == 'queued'
                        and job.key not in self._entry_cancel_orders):
                    job.state = 'done'
                    job.value = {'_cancel_not_sent': True}
            self._heap = [ticket for ticket in self._heap
                          if self._jobs.get(ticket[2]) is not None
                          and self._jobs[ticket[2]].state == 'queued']
            # Filtering a heap preserves its partial ordering, not necessarily
            # its array shape; rebuild before the dispatcher's next pop.
            heapq.heapify(self._heap)
            self._condition.notify_all()

    def _entry_cancel_distance(self, job) -> float:
        row = (self._entry_cancel_orders or {}).get(job.key) or {}
        try:
            price, limit = float(row['price']), float(row['limit_price'])
            if math.isfinite(price) and price > 0 and math.isfinite(limit) and limit > 0:
                return abs(price / limit - 1.0)
        except (KeyError, TypeError, ValueError, OverflowError):
            pass
        return float('inf')

    def _entry_cancel_key(self, job) -> tuple:
        return (self._entry_cancel_distance(job), str(job.key[1]), str(job.key[2]))

    def _next_entry_cancel(self):
        if self._entry_cancel_hold:
            return None
        if any(job.entry_cancel and job.state in {'running', 'done'}
               for job in self._jobs.values()):
            return None  # wait only this cancel lane, never unrelated network work
        if self._entry_cancel_clock() < self._entry_cancel_after:
            return None
        queued = [job for job in self._jobs.values()
                  if job.entry_cancel and job.state == 'queued']
        return min(queued, key=self._entry_cancel_key) if queued else None

    def _entry_cancel_timeout(self) -> float | None:
        if self._entry_cancel_hold or not any(
                job.entry_cancel and job.state == 'queued' for job in self._jobs.values()):
            return None
        if any(job.entry_cancel and job.state in {'running', 'done'}
               for job in self._jobs.values()):
            return None
        delay = self._entry_cancel_after - self._entry_cancel_clock()
        return delay if delay > 0 else None

    def _entry_cancel_completed(self, job) -> None:
        if job.entry_cancel:
            self._entry_cancel_after = self._entry_cancel_clock() + self._entry_cancel_interval

    def _entry_cancel_health(self) -> dict[str, Any]:
        queued = sorted((j for j in self._jobs.values()
                         if j.entry_cancel and j.state == 'queued'), key=self._entry_cancel_key)
        return {
            'interval_seconds': self._entry_cancel_interval,
            'policy': 'absolute_relative_distance; serial; interval_after_response',
            'active': [str(j.key[1]) for j in self._jobs.values()
                       if j.entry_cancel and j.state in {'running', 'done'}],
            'queued': [dict(symbol=str(j.key[1]), client_order_id=str(j.key[2]),
                            distance_pct=(100 * self._entry_cancel_distance(j)
                                          if math.isfinite(self._entry_cancel_distance(j)) else None))
                       for j in queued],
        }
