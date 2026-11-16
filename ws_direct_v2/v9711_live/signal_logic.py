"""Pure signal decisions for the live v9711 long-limit port."""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from . import config as cfg
from .contracts import Decision, DesiredOrder


def _ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat(timespec='milliseconds')


@dataclass(frozen=True)
class Candidate:
    symbol: str
    order_bar_open_ms: int
    order_bar_available_ms: int
    reference_bar_open_ms: int
    reference_bar_available_ms: int
    limit_price: float
    qv20: float
    notional: float
    reference_price: float
    exit_due_epoch: float
    exit_due_ms: int
    exit_due_utc: str


@dataclass(frozen=True)
class SignalInput:
    observed_price: float
    exchange_time_ms: int
    gap: float


@dataclass(frozen=True)
class SignalPlan:
    candidates: tuple[Candidate, ...]
    desired: tuple[DesiredOrder, ...]
    decisions: tuple[Decision, ...]
    touched_limit: int
    stale_price_symbols: int
    unresolved_capacity: int


def long_gap(limit_price: float, observed_price: float) -> float:
    """Distance from observed price to the buy limit, normalized by limit.

    gap >= 0: observed is at/above the buy limit (approaching from above).
    gap <  0: observed is below the buy limit: still eligible for an immediate
              LIMIT buy at the frozen price. A quote is never proof of a fill.
    """
    if not (math.isfinite(limit_price) and limit_price > 0.0
            and math.isfinite(observed_price) and observed_price > 0.0):
        return float('nan')
    return (observed_price - limit_price) / limit_price


def build_candidates(bars_by_symbol: Mapping[str, Iterable[Any]], *,
                     reference_bar_open_ms: int, symbols: Iterable[str],
                     qv_threshold: float = cfg.QV_THRESHOLD,
                     notional: float = cfg.EQUITY0 / cfg.SIGNAL_SIZING_DIVISOR,
                     qv20_fn=None) -> tuple[Candidate, ...]:
    """Plan orders for the upcoming 3m bar using only completed data.

    The next bar's open is not knowable in advance. The completed bar's CLOSE is
    the last causal price available immediately before that bar opens, so it is
    the frozen reference for the buy limit. qv20 includes completed bars with
    open_time < the upcoming order bar, matching rolling().shift(1) semantics.
    No H/L/C from the unfinished order bar or any future data is used.
    """
    if qv20_fn is None:
        from .market_data import BarStore
        qv20_fn = BarStore.qv20
    order_bar_open_ms = int(reference_bar_open_ms) + cfg.BAR_MS
    # v9711 holds K bars from the entry bar.  The executor owns a monotonic
    # timer; this is the exact wall-clock due time fixed before the order is
    # submitted: entry_bar_open + K*3m + 3m (the exit bar's completed close).
    exit_due_ms = order_bar_open_ms + (cfg.K + 1) * cfg.BAR_MS
    out: list[Candidate] = []
    for symbol_raw in symbols:
        symbol = str(symbol_raw).upper().strip()
        if not symbol:
            continue
        series = list(bars_by_symbol.get(symbol, ()))
        if not series:
            continue
        bar = None
        for item in reversed(series):
            if int(item.open_time_ms) <= reference_bar_open_ms:
                bar = item
                break
        if (bar is None or int(bar.open_time_ms) != reference_bar_open_ms
                or not math.isfinite(float(bar.close)) or float(bar.close) <= 0.0):
            continue
        qv = float(qv20_fn(series, order_bar_open_ms))
        if not math.isfinite(qv) or qv <= 0.0 or qv > qv_threshold:
            continue
        out.append(Candidate(
            symbol=symbol,
            order_bar_open_ms=order_bar_open_ms,
            order_bar_available_ms=order_bar_open_ms + cfg.BAR_MS,
            reference_bar_open_ms=int(bar.open_time_ms),
            reference_bar_available_ms=int(bar.open_time_ms) + cfg.BAR_MS,
            limit_price=float(bar.close) * (1.0 - cfg.D),
            qv20=qv,
            notional=float(notional),
            reference_price=float(bar.close),
            exit_due_epoch=exit_due_ms / 1000.0,
            exit_due_ms=exit_due_ms,
            exit_due_utc=_ms_to_iso(exit_due_ms),
        ))
    out.sort(key=lambda row: row.symbol)
    return tuple(out)


def select_orders(candidates: Iterable[Candidate], prices: Mapping[str, SignalInput], *,
                  held_symbols: Iterable[str] = (),
                  incumbent_symbols: Iterable[str] = (),
                  place_gap: float = cfg.SIGNAL_DISCOVERY_GAP,
                  cancel_gap: float | None = None) -> SignalPlan:
    """Publish the complete <=2.2% discovery envelope for tiered trade admission.

    Signal does not cap pending intents at six and does not price-cancel a
    valid incumbent. Trade owns the eight physical slots and applies count-based
    admission plus cancel-first rotation (including underfilled 5..7 books).
    Signal never infers physical capacity from its unlimited model positions.
    Negative gaps remain eligible for the frozen LIMIT buy.
    The cancel_gap keyword is retained for compatibility and ignored.
    """
    if not (math.isfinite(place_gap) and 0.0 <= place_gap < 1.0):
        raise ValueError('Require 0 <= place_gap < 1')
    held = {str(s).upper().strip() for s in held_symbols if str(s).strip()}
    incumbents = {str(s).upper().strip() for s in incumbent_symbols if str(s).strip()}
    by_symbol = {c.symbol: c for c in candidates}
    decisions: list[Decision] = []
    stale = 0
    touched = 0
    desired: list[DesiredOrder] = []
    kept_symbols: set[str] = set()

    # Existing intents stay alive through the active planning bar, even if
    # the latest price moves beyond the discovery envelope. Missing/corrupt
    # input is the only data-side withdrawal; trade separately retains a physical incumbent
    # when a candidate drops out of the current signal list.
    for symbol in sorted(incumbents):
        candidate = by_symbol.get(symbol)
        quote = prices.get(symbol)
        if symbol in held:
            decisions.append(Decision(symbol, 'withdraw', 'already_held'))
            continue
        if candidate is None or quote is None:
            reason = 'candidate_gone' if candidate is None else 'price_unavailable'
            decisions.append(Decision(symbol, 'withdraw', reason))
            if candidate is not None and quote is None:
                stale += 1
            continue
        if not (all(math.isfinite(v) for v in
                (quote.gap, quote.observed_price, candidate.limit_price))
                and quote.observed_price > 0 and candidate.limit_price > 0):
            decisions.append(Decision(symbol, 'withdraw', 'price_unavailable',
                                      candidate.limit_price, quote.observed_price, quote.gap))
            stale += 1
            continue
        if quote.gap < 0.0:
            touched += 1
        kept_symbols.add(symbol)
        decisions.append(Decision(symbol, 'keep', 'retained_for_order_bar',
                                  candidate.limit_price, quote.observed_price, quote.gap))

    # New signal intents are unlimited; physical trade selection enforces eight.
    new_rows: list[tuple[Candidate, SignalInput]] = []
    for candidate in by_symbol.values():
        symbol = candidate.symbol
        if symbol in held or symbol in incumbents:
            continue
        quote = prices.get(symbol)
        if (quote is None or not all(math.isfinite(v) for v in
                (quote.gap, quote.observed_price, candidate.limit_price))
                or quote.observed_price <= 0 or candidate.limit_price <= 0):
            stale += 1
            continue
        if quote.gap < 0.0:
            touched += 1
        if quote.gap <= place_gap or math.isclose(quote.gap, place_gap, rel_tol=0, abs_tol=1e-12):
            new_rows.append((candidate, quote))

    for candidate, quote in sorted(new_rows, key=lambda pair: (pair[1].gap, pair[0].symbol)):
        symbol = candidate.symbol
        kept_symbols.add(symbol)
        decisions.append(Decision(symbol, 'place', 'within_place_gap',
                                  candidate.limit_price, quote.observed_price, quote.gap))

    for symbol in sorted(kept_symbols):
        candidate = by_symbol[symbol]
        quote = prices[symbol]
        desired.append(DesiredOrder(
            symbol=symbol,
            order_bar_open_ms=candidate.order_bar_open_ms,
            order_bar_available_ms=candidate.order_bar_available_ms,
            reference_bar_open_ms=candidate.reference_bar_open_ms,
            reference_bar_available_ms=candidate.reference_bar_available_ms,
            limit_price=candidate.limit_price,
            notional=candidate.notional,
            qv20=candidate.qv20,
            observed_price=quote.observed_price,
            observed_exchange_time_ms=quote.exchange_time_ms,
            gap=quote.gap,
            exit_due_epoch=candidate.exit_due_epoch,
            exit_due_ms=candidate.exit_due_ms,
            exit_due_utc=candidate.exit_due_utc,
            reason='incumbent_kept' if symbol in incumbents else 'within_place_gap',
        ))

    desired.sort(key=lambda row: row.symbol)
    return SignalPlan(tuple(by_symbol[s] for s in sorted(by_symbol)),
                      tuple(desired), tuple(decisions), touched, stale, 0)
