"""Pure tiered execution selection; exact-cancel-first rotation reservations.

The first five physical pending entries admit <=1.85%, orders 6-8 admit <=1.65%.
At any count a >=0.30pp improvement rotates BEFORE filling a free slot;
with >=5 orders the inclusive advantage is >=0.22pp. Replacements bypass the
admission tiers. Fresh quotes strictly beyond +2.1% retire ordinary entries.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from .. import config as cfg
from ..entry_policy import GapPolicy, at_most


@dataclass(frozen=True)
class Row:
    """Normalized desired order from signal."""
    symbol: str
    limit_price: float
    notional: float
    gap: float
    observed_price: float
    order_bar_open_ms: int
    reference_bar_open_ms: int
    exit_due_epoch: float
    exit_due_ms: int
    exit_due_utc: str

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> 'Row | None':
        symbol = str(raw.get('symbol', '') or '').upper().strip()
        price = _finite(raw.get('limit_price'), float('nan'))
        observed = _finite(raw.get('observed_price'), float('nan'))
        notional = _finite(raw.get('notional'), 0.0)
        gap = _finite(raw.get('gap'), float('nan'))
        due = _finite(raw.get('exit_due_epoch'), float('nan'))
        if (not symbol or not math.isfinite(price) or price <= 0.0
                or not math.isfinite(observed) or observed <= 0.0
                or notional <= 0.0 or not math.isfinite(gap)
                or not math.isfinite(due)):
            return None
        # Recompute gap from frozen limit and observed price; do not trust a
        # stale gap from a desired snapshot after the collector has refreshed.
        gap = (observed - price) / price
        return cls(
            symbol=symbol,
            limit_price=price,
            notional=notional,
            gap=gap,
            observed_price=observed,
            order_bar_open_ms=int(_finite(raw.get('order_bar_open_ms'), 0.0)),
            reference_bar_open_ms=int(_finite(raw.get('reference_bar_open_ms'), 0.0)),
            exit_due_epoch=due,
            exit_due_ms=int(_finite(raw.get('exit_due_ms'), due * 1000.0)),
            exit_due_utc=str(raw.get('exit_due_utc') or ''),
        )


@dataclass(frozen=True)
class Action:
    action: str  # place | cancel | keep | close | defer
    symbol: str
    reason: str
    limit_price: float | None = None
    notional: float | None = None
    gap: float | None = None
    observed_price: float | None = None
    order_id: Any = None

    def to_dict(self) -> dict[str, Any]:
        return dict(action=self.action, symbol=self.symbol, reason=self.reason,
                    limit_price=self.limit_price, notional=self.notional,
                    gap=self.gap, observed_price=self.observed_price,
                    order_id=self.order_id)


@dataclass(frozen=True)
class Plan:
    keep: tuple[str, ...]
    place: tuple[Row, ...]
    cancel: tuple[str, ...]
    actions: tuple[Action, ...]
    rotation: Mapping[str, Any] | None = None
    rotation_action: str = ''  # reserve | consume | clear; durable owner applies it


def select_physical_orders(
    desired: Iterable[Mapping[str, Any]],
    tracked: Mapping[str, Mapping[str, Any]],
    *,
    held: Iterable[str] = (),
    max_orders: int = cfg.MAX_PENDING_ORDERS,
    free_position_slots: int | None = None,
    signal_ready: bool = True,
    now: float | None = None,
    place_gap: float = cfg.PLACE_GAP,
    cancel_gap: float = cfg.CANCEL_GAP,
    switch_tolerance: float = cfg.SWITCH_TOLERANCE,
    bootstrap_gap: float = cfg.BOOTSTRAP_PLACE_GAP,
    fill_gap: float = cfg.FILL_PLACE_GAP,
    bootstrap_orders: int = cfg.BOOTSTRAP_PENDING_ORDERS,
    pending_rotation: Mapping[str, Any] | None = None,
    ignore_position_capacity: bool = False,
) -> Plan:
    """Select admissions or ONE cancel-first rotation, never a blind replacement.

    An unresolved rotation reserves the intended replacement, including when
    the book has spare physical slots. Its original order count survives the
    victim's retirement: a five-order rotation still uses the 0.22pp margin
    after cancellation leaves four orders. Fresh quotes also retire ordinary
    entries strictly beyond cancel_gap, independently of signal readiness.
    """
    policy = GapPolicy(bootstrap_gap, fill_gap, place_gap, bootstrap_orders,
                       switch_tolerance, cancel_gap=cancel_gap)
    if max_orders < 0:
        raise ValueError('Invalid pending order capacity')
    held_set = {str(s).upper().strip() for s in held if str(s).strip()}
    slots = cfg.SLOTS - len(held_set) if free_position_slots is None else free_position_slots
    slots = max(0, min(cfg.SLOTS, int(slots)))
    if ignore_position_capacity:
        slots = max_orders
    rows = {}
    for raw in desired:
        row = Row.from_dict(raw)
        if row is not None and row.symbol not in held_set:
            rows[row.symbol] = row
    actions: list[Action] = []
    cancel: list[str] = []
    kept: set[str] = set()
    ranked: dict[str, Row] = {}
    in_flight = False
    for symbol in sorted(tracked):
        old = tracked[symbol]
        row = rows.get(symbol)
        quote_row = row
        if quote_row is None and old.get('_quote_fresh'):
            quote_row = Row.from_dict(dict(old, symbol=symbol))
        old_bar = int(_finite(old.get('order_bar_open_ms')))
        end_ms = _finite(old.get('order_bar_available_ms'), old_bar + cfg.BAR_MS)
        reason = None
        if symbol in held_set:
            reason = 'already_held'
        elif slots == 0:
            reason = 'position_capacity_full'
        elif now is not None and old_bar > 0 and now * 1000 >= end_ms:
            reason = 'order_bar_expired'
        elif signal_ready and row is not None and _order_changed(old, row):
            reason = 'order_plan_changed'
        elif quote_row is not None and not at_most(quote_row.gap, cancel_gap):
            reason = 'entry_gap_too_far'
        if reason:
            cancel.append(symbol)
            actions.append(Action('cancel', symbol, reason, order_id=old.get('order_id')))
            continue
        kept.add(symbol)
        if old.get('cancel_requested'):
            in_flight = True
            actions.append(Action('keep', symbol, 'await_cancel_terminal', order_id=old.get('order_id')))
            continue
        row = quote_row
        if row is not None:
            ranked[symbol] = row
        actions.append(Action('keep', symbol, 'retained_for_order_bar',
                              gap=row.gap if row else None, order_id=old.get('order_id')))

    if len(tracked) > max_orders:
        excess = len(tracked) - max_orders - len(cancel)
        for symbol in sorted(kept, key=lambda s: (-ranked[s].gap if s in ranked else float('-inf'), s))[:max(0, excess)]:
            cancel.append(symbol)
            kept.remove(symbol)
            actions.append(Action('cancel', symbol, 'pending_order_overflow', order_id=tracked[symbol].get('order_id')))

    rotation_action = ''
    if pending_rotation and now is not None and now * 1000 >= _finite(pending_rotation.get('expires_at_ms')):
        pending_rotation = None
        rotation_action = 'clear'

    def result(placed=(), *, rotation=None, state=None):
        return Plan(tuple(sorted(kept)), tuple(placed), tuple(cancel), tuple(actions),
                    rotation, rotation_action if state is None else state)

    if slots == 0:
        return result(state='clear' if pending_rotation else rotation_action)
    # A rotation is a transaction, not a newly free book. Never add into other
    # vacancies while the exact victim cancellation/its accounting is unknown.
    if pending_rotation:
        victim = str(pending_rotation.get('victim') or '')
        current = tracked.get(victim)
        if (pending_rotation.get('victim_filled') or victim in held_set
                or (current and _finite(current.get('accounted_entry_qty')) > 0)):
            return result(state='clear')  # actual fills take precedence, no synthetic replacement
        if current is not None:
            if str(current.get('client_order_id') or '') != str(pending_rotation.get('victim_client_order_id') or ''):
                return result(state='clear')  # never cancel a different exact identity
            if victim not in cancel:
                cancel.append(victim)
                kept.discard(victim)
                actions[:] = [a for a in actions if a.symbol != victim]
                actions.append(Action('cancel', victim, 'replaced_by_closer', order_id=current.get('order_id')))
            actions.append(Action('defer', str(pending_rotation.get('challenger') or ''),
                                  'await_reserved_rotation_terminal'))
            return result()
        if not pending_rotation.get('victim_terminal'):
            # Missing local rows are not exact exchange negative evidence.
            actions.append(Action('defer', str(pending_rotation.get('challenger') or ''),
                                  'rotation_victim_fate_unknown'))
            return result()
        if not signal_ready or len(tracked) >= max_orders:
            return result()
        original_count = int(_finite(pending_rotation.get('original_order_count')))
        # Apply v7 even to an in-progress v6 reservation after restart. Do not
        # resurrect its obsolete 1.45/1.65% gate. Keep the original count so a
        # 5 -> 4 cancellation still completes with the agreed 0.22pp margin.
        ceiling = policy.rotation_gap(original_count, max_orders)
        old_gap = _finite(pending_rotation.get('victim_gap'), float('nan'))
        bar = int(_finite(pending_rotation.get('order_bar_open_ms')))
        challengers = sorted((r for r in rows.values()
            if r.symbol not in tracked and r.symbol != victim and r.order_bar_open_ms == bar
            and policy.replacement_allowed(old_gap, r.gap, ceiling=ceiling, count=original_count)),
            key=lambda r: (r.gap, r.symbol))
        if challengers:
            best = challengers[0]
            actions.append(Action('place', best.symbol, 'complete_reserved_replacement',
                                  best.limit_price, best.notional, best.gap, best.observed_price))
            return result((best,), state='consume')
        return result()  # wait for a fresh qualifying replacement or original bar expiry

    if not signal_ready:
        return result()
    challengers = sorted((r for s, r in rows.items() if s not in tracked),
                         key=lambda r: (r.gap, r.symbol))
    count = len(tracked)
    # Relative improvement takes priority at every nonempty order count.
    if (kept and not cancel and not in_flight
            and len(ranked) == len(kept)):
        worst = max(kept, key=lambda s: (ranked[s].gap, s))
        ceiling = policy.rotation_gap(count, max_orders)
        best = next((r for r in challengers if policy.replacement_allowed(
            ranked[worst].gap, r.gap, ceiling=ceiling, count=count)), None)
        if best is not None:
            old = tracked[worst]
            rotation = dict(victim=worst, victim_client_order_id=str(old.get('client_order_id') or ''),
                victim_order_id=old.get('order_id'), victim_gap=ranked[worst].gap,
                challenger=best.symbol, original_order_count=count, admission_gap=ceiling,
                minimum_improvement=policy.minimum_improvement(count), policy_version=7,
                order_bar_open_ms=best.order_bar_open_ms,
                expires_at_ms=best.order_bar_open_ms + cfg.BAR_MS,
                victim_terminal=False, victim_filled=False)
            cancel.append(worst)
            kept.remove(worst)
            actions[:] = [a for a in actions if a.symbol != worst]
            r = ranked[worst]
            actions.append(Action('cancel', worst, 'replaced_by_closer', r.limit_price,
                                  r.notional, r.gap, r.observed_price, old.get('order_id')))
            actions.append(Action('defer', best.symbol, 'await_cancel_terminal_before_replace',
                                  best.limit_price, best.notional, best.gap, best.observed_price))
            return result(rotation=rotation, state='reserve')

    # Normal (non-rotation) cancellation reserves only its own slot; preserve
    # independent admissions while unrelated I/O is pending.
    placed = []
    for r in challengers:
        current_count = count + len(placed)
        if current_count >= max_orders:
            break
        if placed and not cancel and not in_flight:
            hypothetical = dict(ranked, **{row.symbol: row for row in placed})
            if hypothetical and len(hypothetical) == len(kept) + len(placed):
                worst_gap = max(row.gap for row in hypothetical.values())
                if policy.replacement_allowed(worst_gap, r.gap,
                        ceiling=policy.rotation_gap(current_count, max_orders), count=current_count):
                    # Send this subset first, then rotate an exact tracked
                    # identity on the next owner step rather than add a slot.
                    break
        if at_most(r.gap, policy.admission_gap(current_count)):
            placed.append(r)
            actions.append(Action('place', r.symbol, 'fill_free_pending_slot',
                                  r.limit_price, r.notional, r.gap, r.observed_price))
    return result(placed)


def _order_changed(old: Mapping[str, Any], new: Row) -> bool:
    # Legacy sparse records used by offline adapters do not invent an identity.
    for field in ('order_bar_open_ms', 'reference_bar_open_ms', 'exit_due_ms'):
        if old.get(field) is not None and int(_finite(old[field])) != getattr(new, field):
            return True
    for field in ('limit_price', 'notional', 'exit_due_epoch'):
        if old.get(field) is not None and not math.isclose(_finite(old[field]), getattr(new, field), rel_tol=1e-12, abs_tol=1e-12):
            return True
    return False


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default
