"""Pure trade-versus-signal PnL and capacity-difference calculations.

No exchange I/O and no writes. ``signal`` values come from the signal-owned
portfolio snapshot; ``actual`` values come only from executor-owned exact fills
and exact owned positions. The difference is reported with an explicit sign.
"""
from __future__ import annotations

import math
from typing import Any, Mapping

from .. import config as cfg


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def _as_int(value: Any, default: int = 0) -> int:
    out = _finite(value, float(default))
    return int(out) if math.isfinite(out) else default


def _pct(value: float, base: float) -> float:
    return 100.0 * value / base if base > 0.0 and math.isfinite(value) else 0.0


def _position_rows(raw: Any) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    if isinstance(raw, Mapping):
        rows: Any = raw.values()
    elif isinstance(raw, list):
        rows = raw
    else:
        rows = ()
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        symbol = str(row.get('symbol') or '').upper().strip()
        if symbol:
            out[symbol] = dict(row)
    return out


def _view_positions(raw: Any, price_map: Mapping[str, float], source: str) -> dict[str, dict[str, Any]]:
    """Normalize positions and attach a current mark/unrealized PnL."""
    out: dict[str, dict[str, Any]] = {}
    for symbol, row in _position_rows(raw).items():
        entry = _finite(row.get('entry_price'), float('nan'))
        if not math.isfinite(entry) or entry <= 0.0:
            entry = _finite(row.get('limit_price'), 0.0)
        if not math.isfinite(entry) or entry <= 0.0:
            entry = 0.0
        qty = _finite(row.get('quantity'), 0.0)
        notional = _finite(row.get('notional'), 0.0)
        if qty <= 0.0 and notional > 0.0 and entry > 0.0:
            qty = notional / entry
        if notional <= 0.0 and qty > 0.0 and entry > 0.0:
            notional = qty * entry
        mark = _finite(row.get('mark_price'), float('nan'))
        if not math.isfinite(mark) or mark <= 0.0:
            mark = _finite(price_map.get(symbol), float('nan'))
        if not math.isfinite(mark) or mark <= 0.0:
            mark = entry
        unrealized = _finite(row.get('unrealized_pnl'), float('nan'))
        if not math.isfinite(unrealized):
            unrealized = (mark - entry) * qty if entry > 0.0 and qty > 0.0 else 0.0
        out[symbol] = {
            'symbol': symbol,
            'source': source,
            'side': str(row.get('side') or 'LONG').upper(),
            'entry_price': entry,
            'mark_price': mark,
            'quantity': qty,
            'notional': notional,
            'unrealized_pnl': unrealized,
            'exit_due_ms': _as_int(row.get('exit_due_ms')),
            'position_id': str(row.get('position_id') or ''),
        }
    return out


def _signal_positions(signal_portfolio: Mapping[str, Any],
                      target_positions: Mapping[str, Any],
                      price_map: Mapping[str, float]) -> dict[str, dict[str, Any]]:
    raw = signal_portfolio.get('positions') if isinstance(signal_portfolio, Mapping) else None
    positions = _view_positions(raw or {}, price_map, 'signal_model')
    if not positions:
        positions = _view_positions(target_positions, price_map, 'signal_model')
    return positions


def _signal_pnl(signal_portfolio: Mapping[str, Any],
                positions: Mapping[str, Mapping[str, Any]],
                equity0: float) -> dict[str, Any]:
    initial = _finite(signal_portfolio.get('initial_equity'), equity0)
    if initial <= 0.0:
        initial = equity0
    balance = _finite(signal_portfolio.get('realized_balance'), initial)
    realized = _finite(signal_portfolio.get('cumulative_pnl'), balance - initial)
    unrealized = _finite(signal_portfolio.get('current_unrealized_pnl'),
                         sum(_finite(row.get('unrealized_pnl')) for row in positions.values()))
    total = _finite(signal_portfolio.get('total_pnl'), realized + unrealized)
    return {
        'position_count': len(positions),
        'initial_equity': initial,
        'realized_balance': balance,
        'realized_pnl': realized,
        'unrealized_pnl': unrealized,
        'total_pnl': total,
        'return_pct': _finite(signal_portfolio.get('return_pct'), _pct(total, initial)),
        'session_id': str(signal_portfolio.get('session_id') or ''),
        'revision': signal_portfolio.get('revision'),
        'pnl_kind': 'signal_model_not_exchange_fill',
    }


def _actual_pnl(positions: Mapping[str, Mapping[str, Any]],
                realized_balance: float, equity0: float) -> dict[str, Any]:
    realized_balance = _finite(realized_balance, equity0)
    realized = realized_balance - equity0
    unrealized = sum(_finite(row.get('unrealized_pnl')) for row in positions.values())
    total = realized + unrealized
    return {
        'position_count': len(positions),
        'initial_equity': equity0,
        'realized_balance': realized_balance,
        'realized_pnl': realized,
        'unrealized_pnl': unrealized,
        'total_pnl': total,
        'return_pct': _pct(total, equity0),
        'pnl_kind': 'exact_owned_exchange_fills_and_open_positions',
    }


def _difference_rows(signal_positions: Mapping[str, Mapping[str, Any]],
                     actual_positions: Mapping[str, Mapping[str, Any]],
                     limit: int) -> tuple[list[dict[str, Any]], dict[str, float]]:
    rows: list[dict[str, Any]] = []
    signal_only_pnl = 0.0
    trade_only_pnl = 0.0
    matched_gap = 0.0
    for symbol in sorted(set(signal_positions) | set(actual_positions)):
        signal_row = signal_positions.get(symbol)
        actual_row = actual_positions.get(symbol)
        s_pnl = _finite(signal_row.get('unrealized_pnl')) if signal_row else 0.0
        a_pnl = _finite(actual_row.get('unrealized_pnl')) if actual_row else 0.0
        diff = s_pnl - a_pnl
        if signal_row and not actual_row:
            kind = 'signal_only'
            signal_only_pnl += s_pnl
        elif actual_row and not signal_row:
            kind = 'trade_only'
            trade_only_pnl += a_pnl
        else:
            kind = 'matched'
            matched_gap += diff
        rows.append({
            'symbol': symbol,
            'kind': kind,
            'signal_unrealized_pnl': s_pnl,
            'trade_unrealized_pnl': a_pnl,
            'difference_signal_minus_trade': diff,
            'signal_notional': _finite(signal_row.get('notional')) if signal_row else 0.0,
            'trade_notional': _finite(actual_row.get('notional')) if actual_row else 0.0,
            'signal_mark_price': _finite(signal_row.get('mark_price')) if signal_row else 0.0,
            'trade_mark_price': _finite(actual_row.get('mark_price')) if actual_row else 0.0,
        })
    rows.sort(key=lambda row: (-abs(_finite(row['difference_signal_minus_trade'])),
                               str(row['symbol'])))
    totals = {
        'signal_only_unrealized_pnl': signal_only_pnl,
        'trade_only_unrealized_pnl': trade_only_pnl,
        'matched_unrealized_difference_signal_minus_trade': matched_gap,
    }
    return rows[:max(0, limit)], totals


def compute_comparison(*, actual_positions: Any, open_orders: Any,
                       signal_portfolio: Any, target_positions: Any,
                       price_map: Mapping[str, float], realized_balance: float,
                       signal_desired_count: int = 0,
                       priced_desired_count: int = 0,
                       near_place_count: int = 0,
                       stats: Mapping[str, Any] | None = None,
                       equity0: float = cfg.EQUITY0,
                       max_pending_orders: int = cfg.MAX_PENDING_ORDERS,
                       max_positions: int = cfg.SLOTS) -> dict[str, Any]:
    """Compute the executor's actual PnL and its difference from signal model."""
    signal_book = dict(signal_portfolio) if isinstance(signal_portfolio, Mapping) else {}
    signal_positions = _signal_positions(signal_book, target_positions or {}, price_map)
    actual = _view_positions(actual_positions, price_map, 'exact_owned_trade')
    signal = _signal_pnl(signal_book, signal_positions, equity0)
    trade = _actual_pnl(actual, realized_balance, equity0)

    largest_difference, part = _difference_rows(
        signal_positions, actual, max(1, max_pending_orders))
    signal_only = {
        symbol: dict(row) for symbol, row in signal_positions.items()
        if symbol not in actual}
    trade_only = {
        symbol: dict(row) for symbol, row in actual.items()
        if symbol not in signal_positions}
    matched = {
        symbol: dict(row) for symbol, row in actual.items()
        if symbol in signal_positions}
    largest_signal_only = sorted(
        signal_only.values(),
        key=lambda row: (-_finite(row.get('notional')), str(row.get('symbol'))))[
            :max(1, max_pending_orders)]

    realized_gap = signal['realized_pnl'] - trade['realized_pnl']
    unrealized_gap = signal['unrealized_pnl'] - trade['unrealized_pnl']
    total_gap = signal['total_pnl'] - trade['total_pnl']
    signal_desired_count = max(0, _as_int(signal_desired_count))
    priced_desired_count = max(0, _as_int(priced_desired_count))
    near_place_count = max(0, _as_int(near_place_count))
    pending_count = len(_position_rows(open_orders))

    difference = {
        'sign_convention': 'signal_minus_trade; positive means signal model is ahead',
        'signal_minus_trade': {
            'realized_pnl': realized_gap,
            'unrealized_pnl': unrealized_gap,
            'total_pnl': total_gap,
            'return_pct': _pct(total_gap, equity0),
        },
        'trade_minus_signal': {
            'realized_pnl': -realized_gap,
            'unrealized_pnl': -unrealized_gap,
            'total_pnl': -total_gap,
            'return_pct': _pct(-total_gap, equity0),
        },
        'difference_part_pnl': {
            'realized_difference_signal_minus_trade': realized_gap,
            'signal_only_unrealized_pnl': part['signal_only_unrealized_pnl'],
            'trade_only_unrealized_pnl': part['trade_only_unrealized_pnl'],
            'matched_unrealized_difference_signal_minus_trade':
                part['matched_unrealized_difference_signal_minus_trade'],
            'capacity_excluded_unrealized_pnl': part['signal_only_unrealized_pnl'],
            'capacity_excluded_return_pct': _pct(part['signal_only_unrealized_pnl'], equity0),
        },
        'signal_only_position_count': len(signal_only),
        'trade_only_position_count': len(trade_only),
        'matched_position_count': len(matched),
        'difference_position_count': len(signal_only) + len(trade_only) + len(matched),
        'largest_difference_positions': largest_difference,
    }

    capacity = {
        'max_pending_orders': int(max_pending_orders),
        'actual_position_cap': int(max_positions),
        'signal_target_count': len(signal_positions),
        'actual_position_count': len(actual),
        'pending_order_count': pending_count,
        'signal_desired_count': signal_desired_count,
        'priced_desired_count': priced_desired_count,
        'near_place_count': near_place_count,
        'signal_targets_over_position_cap': max(0, len(signal_positions) - int(max_positions)),
        'signal_targets_not_held': len(signal_only),
        'signal_desired_over_pending_cap': max(0, signal_desired_count - int(max_pending_orders)),
        'priced_desired_over_pending_cap': max(0, priced_desired_count - int(max_pending_orders)),
        'near_place_over_pending_cap': max(0, near_place_count - int(max_pending_orders)),
        'rotation_defer_count': _as_int((stats or {}).get('rotation_defer')),
        'largest_signal_only_targets': largest_signal_only,
    }

    return {
        'schema_version': 1,
        'basis': 'signal_contract_v2_model_vs_exact_owned_trade_state',
        'initial_equity': equity0,
        'actual': {**trade, 'pending_order_count': pending_count},
        'signal': signal,
        'difference': difference,
        'capacity': capacity,
        'positions': {
            'actual': actual,
            'signal_only_top': largest_signal_only,
            'trade_only': trade_only,
            'matched': matched,
            'largest_difference': largest_difference,
        },
        'stats': dict(stats or {}),
    }
