"""Signal-owned v9711 target book, independent of exchange execution feedback.

Completed 3m bars confirm touches of the SAME previous-close limit planned
for that bar. All eligible signals are retained; only trade controls actual
position capacity. Entries run before that bar's K-bar exits.
Unlike a finite backtest, unavailable FUTURE exit prices never reject entries.
These are strategy targets/model PnL, not claims of actual exchange fills.
"""
from __future__ import annotations

import copy
import hashlib
import json
import math
import uuid
from pathlib import Path
from typing import Any, Iterable, Mapping

from . import config as cfg
from .contracts import atomic_write_json, read_json, utc_now_iso
from .market_data import BarStore
from .signal_logic import build_candidates


class SignalStateError(RuntimeError):
    """An existing book must never silently reset unclosed strategy positions."""


def finite(value: Any, default: float = 0.0) -> float:
    try:
        value = float(value)
    except (TypeError, ValueError):
        return default
    return value if math.isfinite(value) else default


class SignalPortfolio:
    MODEL = 'v9712_previous_close_limit_count_503020'

    def __init__(self, path: Path):
        self.path = Path(path)
        self._dirty = False
        self.state: dict[str, Any] = {
            'schema_version': 1, 'component': 'signal_portfolio',
            'model': self.MODEL, 'parameters': self.parameters(),
            'session_id': uuid.uuid4().hex,
            'created_at_utc': utc_now_iso(), 'start_bar_open_ms': None,
            'last_processed_bar_open_ms': None,
            'initial_equity': cfg.EQUITY0, 'realized_balance': cfg.EQUITY0,
            'positions': {}, 'recent_trades': [],
            'total_entries': 0, 'total_exits': 0, 'revision': 0,
            'last_bar_checkpoint': None, 'last_bar_input_key': None,
        }
        if self.path.exists():
            payload = read_json(self.path)
            self._validate(payload)
            self.state = payload

    @staticmethod
    def parameters() -> dict[str, Any]:
        return {'d': cfg.D, 'k': cfg.K, 'sizing_divisor': cfg.SIGNAL_SIZING_DIVISOR,
                'price_reference': 'previous_completed_close', 'signal_position_limit': cfg.SLOTS, 'sizing_weights': list(cfg.SLOT_WEIGHTS),
                'qv_threshold': cfg.QV_THRESHOLD, 'fee_per_side': cfg.TRADE_FEE_RATE,
                'initial_equity': cfg.EQUITY0, 'bar_ms': cfg.BAR_MS}

    def _validate(self, data: Any) -> None:
        if (not isinstance(data, dict) or data.get('schema_version') != 1
                or data.get('component') != 'signal_portfolio'
                or data.get('model') != self.MODEL
                or data.get('parameters') != self.parameters()
                or not isinstance(data.get('positions'), dict)
                or not isinstance(data.get('recent_trades'), list)
                or not isinstance(data.get('session_id'), str)
                or finite(data.get('realized_balance'), -1) <= 0):
            raise SignalStateError(f'Invalid/incompatible signal book; retained without reset: {self.path}')
        last = data.get('last_processed_bar_open_ms')
        if last is not None and (not isinstance(last, int) or last % cfg.BAR_MS):
            raise SignalStateError('Invalid signal bar checkpoint; refusing to reset')
        identities = set()
        for symbol, row in data['positions'].items():
            identity = row.get('position_id') if isinstance(row, dict) else None
            if (not isinstance(row, dict) or symbol != row.get('symbol')
                    or not isinstance(identity, str) or not identity or identity in identities
                    or finite(row.get('quantity'), -1) <= 0
                    or finite(row.get('entry_price'), -1) <= 0
                    or finite(row.get('notional'), -1) <= 0
                    or not isinstance(row.get('exit_bar_open_ms'), int)):
                raise SignalStateError('Invalid target position; refusing to discard an unclosed position')
            identities.add(identity)

    @property
    def positions(self) -> dict[str, dict[str, Any]]:
        return self.state['positions']

    @property
    def balance(self) -> float:
        return float(self.state['realized_balance'])

    @property
    def last_bar(self) -> int | None:
        return self.state['last_processed_bar_open_ms']

    @property
    def revision(self) -> int:
        return int(self.state['revision'])

    @staticmethod
    def _index(bars: Mapping[str, Iterable[Any]]) -> dict[str, dict[int, Any]]:
        return {symbol: {int(b.open_time_ms): b for b in rows}
                for symbol, rows in bars.items()}

    def _save(self) -> None:
        if self._dirty:
            atomic_write_json(self.path, self.state)
            self._dirty = False

    def _settle(self, index: Mapping[str, Mapping[int, Any]], *, through_ms: int) -> list[dict[str, Any]]:
        """Only a known completed due-bar close settles a model position."""
        trades = []
        batch_pnl = 0.0
        for symbol, pos in sorted(self.positions.items()):
            due = int(pos['exit_bar_open_ms'])
            bar = index.get(symbol, {}).get(due)
            if due > through_ms or bar is None or finite(bar.close, -1) <= 0:
                continue
            price = float(bar.close)
            pnl = float(pos['notional']) * (price / float(pos['entry_price']) - 1.0
                                           - 2 * cfg.TRADE_FEE_RATE)
            trade = dict(pos, exit_price=price, exit_available_ms=due + cfg.BAR_MS,
                         pnl=pnl, reason='fixed_K_bar_completed_close',
                         source='signal_model_not_exchange_fill')
            trades.append(trade)
            batch_pnl += pnl
        if not trades:
            return []
        self.state['realized_balance'] = max(1e-9, self.balance + batch_pnl)
        for trade in trades:
            self.positions.pop(trade['symbol'])
            trade['balance_after_exit_batch'] = self.balance
        self.state['total_exits'] += len(trades)
        self.state['recent_trades'] = (self.state['recent_trades'] + trades)[-100:]
        checkpoint = self.state.get('last_bar_checkpoint')
        if checkpoint and checkpoint.get('last_processed_bar_open_ms') is not None:
            # A delayed price for an older exit is also prior to the last bar.
            # Preserve that settlement if a late current-bar row is replayed.
            older = [t for t in trades
                     if t['exit_bar_open_ms'] <= checkpoint['last_processed_bar_open_ms']
                     and t['symbol'] in checkpoint['positions']]
            if older:
                checkpoint['realized_balance'] = max(
                    1e-9, checkpoint['realized_balance'] + sum(t['pnl'] for t in older))
                for trade in older:
                    checkpoint['positions'].pop(trade['symbol'])
                checkpoint['total_exits'] += len(older)
                checkpoint['recent_trades'] = (checkpoint['recent_trades'] + older)[-100:]
        self.state['revision'] += 1
        self._dirty = True
        return trades

    _CHECKPOINT_FIELDS = ('start_bar_open_ms', 'last_processed_bar_open_ms',
                          'realized_balance', 'positions', 'recent_trades',
                          'total_entries', 'total_exits')

    def _input_key(self, bars, index, universe, bar_ms):
        rows = []
        checkpoint_positions = (self.state.get('last_bar_checkpoint') or {}).get('positions', {})
        for symbol in sorted(set(universe) | set(self.positions) | set(checkpoint_positions)):
            bar = index.get(symbol, {}).get(bar_ms)
            values = None if bar is None else [
                finite(getattr(bar, k), None) for k in ('open', 'high', 'low', 'close', 'quote_volume')]
            qv = finite(BarStore.qv20(bars.get(symbol, ()), bar_ms), None)
            previous = index.get(symbol, {}).get(bar_ms - cfg.BAR_MS)
            previous_close = None if previous is None else finite(previous.close, None)
            rows.append((symbol, values, qv, previous_close))
        # Existing held symbols may need an older missing due-bar close. That
        # recovery is handled by _settle even when the current inputs match.
        return hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()

    def advance(self, bars: Mapping[str, Iterable[Any]], *, through_bar_open_ms: int,
                symbols: Iterable[str]) -> list[dict[str, Any]]:
        """Incremental completed-bar replay; no terminal-window liquidation.

        First launch starts with the latest observed completed bar and 15 units.
        Earlier bars are indicator warmup only, not invented historical trades.
        Subsequent launches continue the persisted checkpoint and original book.
        """
        through = int(through_bar_open_ms)
        if through % cfg.BAR_MS:
            raise ValueError('Signal model requires aligned completed 3m bars')
        self._save()  # retry a previous atomic-save failure before more progress
        index = self._index(bars)
        # Recovery may append an older due candle after the rolling window.
        # Normalize once per completed-bar update so both exact previous-close
        # lookup and the causal last-20 qv window are ordered and deduplicated.
        bars = {symbol: [by_time[ms] for ms in sorted(by_time)]
                for symbol, by_time in index.items()}
        universe = sorted({str(s).strip().upper() for s in symbols if str(s).strip()})
        events: list[dict[str, Any]] = []
        replay_from = None
        if self.last_bar is not None and through >= self.last_bar:
            checkpoint = self.state.get('last_bar_checkpoint')
            if checkpoint and self.last_bar in {ms for rows in index.values() for ms in rows}:
                input_key = self._input_key(bars, index, universe, self.last_bar)
                if input_key != self.state.get('last_bar_input_key'):
                    # The 90%-ready collector can append late symbols in the
                    # same 3m batch. Recompute only this last committed bar from
                    # its before-bar book, avoiding missed targets/double PnL.
                    replay_from = self.last_bar
                    for field in self._CHECKPOINT_FIELDS:
                        self.state[field] = copy.deepcopy(checkpoint[field])
                    self._dirty = True
        if self.last_bar is not None:
            events.extend(dict(t, action='exit') for t in self._settle(index, through_ms=min(through, self.last_bar)))
        first = (replay_from if replay_from is not None else
                 through if self.last_bar is None else self.last_bar + cfg.BAR_MS)
        for bar_ms in range(first, through + 1, cfg.BAR_MS):
            # Never skip a whole missing bar and silently forget its entries.
            if not any(bar_ms in rows for rows in index.values()):
                break
            self.state['last_bar_checkpoint'] = {
                field: copy.deepcopy(self.state[field]) for field in self._CHECKPOINT_FIELDS}
            self.state['last_bar_input_key'] = self._input_key(bars, index, universe, bar_ms)
            if self.state['start_bar_open_ms'] is None:
                self.state['start_bar_open_ms'] = bar_ms
            balance_at_entry = self.balance
            notional = balance_at_entry * cfg.SLOT_WEIGHTS[min(len(self.positions), 2)]
            held_before = set(self.positions)
            # One shared planner is authoritative for both advance-of-bar
            # pending orders and completed-bar target confirmation. Never
            # substitute the entry bar's open for its previous-close limit.
            planned = build_candidates(
                bars, reference_bar_open_ms=bar_ms - cfg.BAR_MS,
                symbols=universe, notional=notional, qv20_fn=BarStore.qv20)
            for candidate in planned:
                if len(self.positions) >= cfg.SLOTS:
                    break
                symbol = candidate.symbol
                if symbol in held_before:
                    continue
                bar = index.get(symbol, {}).get(bar_ms)
                if bar is None:
                    continue
                hi, lo, cl = (finite(getattr(bar, name), float('nan'))
                              for name in ('high', 'low', 'close'))
                if (not all(math.isfinite(v) and v > 0 for v in (hi, lo, cl))
                        or not lo <= candidate.limit_price <= hi):
                    continue
                weight = cfg.SLOT_WEIGHTS[len(self.positions)]
                entry_notional = balance_at_entry * weight
                pos = {
                    'sizing_weight': weight, 'sizing_position_count': len(self.positions),
                    'symbol': symbol, 'side': 'LONG',
                    'position_id': f'{self.state["session_id"]}:{symbol}:{bar_ms}',
                    'entry_bar_open_ms': bar_ms, 'entry_available_ms': bar_ms + cfg.BAR_MS,
                    'reference_bar_open_ms': candidate.reference_bar_open_ms,
                    'reference_bar_available_ms': candidate.reference_bar_available_ms,
                    'reference_price': candidate.reference_price,
                    'limit_price': candidate.limit_price,
                    'entry_price': candidate.limit_price,
                    'quantity': entry_notional / candidate.limit_price,
                    'notional': entry_notional, 'entry_balance': balance_at_entry,
                    'entry_qv20': candidate.qv20,
                    'exit_bar_open_ms': candidate.exit_due_ms - cfg.BAR_MS,
                    'exit_due_ms': candidate.exit_due_ms,
                    'exit_due_epoch': candidate.exit_due_epoch,
                    'exit_due_utc': candidate.exit_due_utc,
                    'last_close': cl, 'last_close_available_ms': bar_ms + cfg.BAR_MS,
                    'source': 'signal_previous_close_limit_model',
                    'entry_timing': 'unknown_within_3m_confirmed_after_close',
                }
                self.positions[symbol] = pos
                self.state['total_entries'] += 1
                events.append(dict(pos, action='entry'))
            # Keep model admission before the completed-close exit so an
            # expiring position cannot release a slot earlier in its own bar.
            for symbol, pos in self.positions.items():
                bar = index.get(symbol, {}).get(bar_ms)
                if bar is not None and finite(bar.close, -1) > 0:
                    pos['last_close'] = float(bar.close)
                    pos['last_close_available_ms'] = bar_ms + cfg.BAR_MS
            events.extend(dict(t, action='exit') for t in self._settle(index, through_ms=bar_ms))
            self.state['last_processed_bar_open_ms'] = bar_ms
            self.state['revision'] += 1
            self._dirty = True
        self._save()
        return events

    def snapshot(self, prices: Mapping[str, Any] | None = None, *, now: float,
                 ready: bool = True) -> dict[str, Any]:
        marks = prices or {}
        positions: dict[str, dict[str, Any]] = {}
        unrealized = 0.0
        stale_marks = []
        for symbol, row in sorted(self.positions.items()):
            pos = dict(row)
            quote = marks.get(symbol)
            price = finite(getattr(quote, 'price', None), -1)
            stamp = finite(getattr(quote, 'exchange_time_ms', None), 0)
            age = now - stamp / 1000
            if quote is None or price <= 0 or not -5 <= age <= cfg.PRICE_EXCHANGE_MAX_AGE_SECONDS:
                price = float(pos['last_close'])
                pos['mark_source'] = 'latest_completed_close'
                stale_marks.append(symbol)
            else:
                pos['mark_source'] = 'local_ticker'
            pos['mark_price'] = price
            pos['unrealized_pnl'] = (price - float(pos['entry_price'])) * float(pos['quantity'])
            unrealized += pos['unrealized_pnl']
            positions[symbol] = pos
        cumulative = self.balance - float(self.state['initial_equity'])
        pending_exits = [s for s, p in self.positions.items()
                         if self.last_bar is not None and p['exit_bar_open_ms'] <= self.last_bar]
        return {
            'authority': 'signal', 'model': self.MODEL,
            'session_id': self.state['session_id'], 'revision': self.revision,
            'ready': bool(ready and self.last_bar is not None),
            'start_bar_open_ms': self.state['start_bar_open_ms'],
            'last_processed_bar_open_ms': self.last_bar,
            'initial_equity': float(self.state['initial_equity']),
            'positions': positions, 'realized_balance': self.balance,
            'cumulative_pnl': cumulative, 'current_unrealized_pnl': unrealized,
            'total_pnl': cumulative + unrealized,
            'return_pct': 100 * (cumulative + unrealized) / float(self.state['initial_equity']),
            'notional_per_order': self.balance * cfg.SLOT_WEIGHTS[min(len(self.positions), 2)],
            'sizing_divisor': cfg.SIGNAL_SIZING_DIVISOR,
            'signal_position_limit': cfg.SLOTS, 'sizing_weights': list(cfg.SLOT_WEIGHTS), 'position_capacity_owner': 'independent_signal_model_and_trade',
            'pnl_scope': 'three_position_count_weighted_model_not_actual_account',
            'total_entries': self.state['total_entries'], 'total_exits': self.state['total_exits'],
            'pending_exit_price_symbols': sorted(pending_exits),
            'stale_mark_symbols': stale_marks,
            'recent_trades': self.state['recent_trades'][-20:],
            'pnl_kind': 'signal_model_not_exchange_account',
        }
