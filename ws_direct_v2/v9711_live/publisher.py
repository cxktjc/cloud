"""Signal-authoritative target state with additive portfolio fields.

The legacy desired-order reader continues to consume schema_version=1;
contract_version=2 adds signal-owned target_positions and portfolio state.
"""
from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any, Iterable, Mapping

from . import config as cfg
from .entry_policy import GapPolicy
from .contracts import BarStats, DesiredOrder, atomic_write_json, utc_now_iso


class DesiredPublisher:
    def __init__(self, path: Path = cfg.DESIRED_FILE,
                 ttl_seconds: float = cfg.DESIRED_TTL_SECONDS,
                 heartbeat_seconds: float = cfg.DESIRED_HEARTBEAT_SECONDS):
        self.path = Path(path)
        self.ttl_seconds = float(ttl_seconds)
        self.heartbeat_seconds = float(heartbeat_seconds)
        self.content_key = ''
        self.last_write = 0.0

    def publish(self, *, orders: Iterable[DesiredOrder], bar_open_ms: int,
                bar_stats: BarStats, portfolio: Mapping[str, Any],
                status_signature: tuple | None, price_sequence: int,
                now: float | None = None, force: bool = False,
                candidate_templates: Iterable[Mapping[str, Any]] = ()) -> bool:
        now = time.time() if now is None else float(now)
        rows = sorted(orders, key=lambda row: row.symbol)
        templates = sorted((dict(row) for row in candidate_templates), key=lambda row: row['symbol'])
        positions = portfolio['positions']
        stable = {
            'bar_open_ms': int(bar_open_ms),
            'candidate_templates': templates,
            'orders': [[o.symbol, o.order_bar_open_ms, o.limit_price, o.notional] for o in rows],
            'session_id': portfolio['session_id'], 'revision': portfolio['revision'],
            'ready': portfolio['ready'], 'positions': positions,
            'realized_balance': portfolio['realized_balance'],
            'entry_gap_policy': GapPolicy().describe(),
        }
        key = hashlib.sha256(json.dumps(stable, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        if not (force or key != self.content_key or now - self.last_write >= self.heartbeat_seconds):
            return False
        payload = {
            'schema_version': 1, 'contract_version': 2,
            'execution_mode': cfg.WS_DIRECT_MODE,
            'candidate_templates': templates if portfolio['ready'] else [],
            'component': 'signal', 'state_authority': 'signal',
            'strategy_version': cfg.VERSION, 'side': 'LONG',
            'generated_at_utc': utc_now_iso(), 'generated_at_epoch': now,
            'expires_at_epoch': now + self.ttl_seconds,
            'bar_open_ms': int(bar_open_ms),
            'bar_available_ms': int(bar_open_ms) + cfg.BAR_MS if bar_open_ms else 0,
            'status_signature': list(status_signature) if status_signature else None,
            'price_sequence': int(price_sequence),
            'place_gap': cfg.SIGNAL_DISCOVERY_GAP, 'cancel_gap': None,
            'entry_gap_policy': GapPolicy().describe(),
            'price_gap_cancel_enabled': False,
            'fee_rate_per_side': cfg.TRADE_FEE_RATE,
            'signal_ready': bool(portfolio['ready']),
            'signal_session_id': portfolio['session_id'],
            'signal_revision': portfolio['revision'],
            'target_positions': [positions[s] for s in sorted(positions)],
            'held': sorted(positions), 'portfolio': dict(portfolio),
            'free_slots': None, 'position_slots_available': None,
            'actual_position_cap': cfg.SLOTS, 'target_positions_unlimited': False,
            'pending_order_limit': None,
            'ordinary_pending_order_cap': cfg.MAX_PENDING_ORDERS,
            'signal_follow_pending_order_cap': cfg.SIGNAL_FOLLOW_MAX_PENDING_ORDERS,
            'signal_follow_position_stop': cfg.SIGNAL_FOLLOW_POSITION_STOP,
            'physical_pending_order_cap': None,  # paced cancel backlog may overlap new entries
            'ordinary_pending_excludes_cancel_requested': True,
            'pending_order_policy': cfg.PENDING_ORDER_POLICY,
            'switch_tolerance': cfg.SWITCH_TOLERANCE,
            'crowded_switch_tolerance': cfg.CROWDED_SWITCH_TOLERANCE,
            'trade_cancel_gap': cfg.CANCEL_GAP,
            'desired': [row.to_dict() for row in rows] if portfolio['ready'] else [],
            'bar_stats': bar_stats.to_dict(),
            'execution_contract': {
                'required_schema': 1, 'contract_version': 2,
                'positions_authority': 'target_positions',
                'target_position_capacity': 'three_position_count_weighted_model_and_independent_trade',
                'pending_order_capacity': 'eight_active_ordinary_plus_two_signal_follow_and_cancel_backlog',
                'replacement_waits_for_cancel_ack': False,
                'pending_order_switch_rule': 'rotation_first_ge_0p003_or_count_ge_5_ge_0p0022_no_tier_ceiling',
                'pending_order_policy_version': 7,
                'ordinary_pending_cancel_gap_strictly_above': cfg.CANCEL_GAP,
                'reconcile_only_exact_strategy_owned_positions': True,
                'execution_feedback_changes_signal_state': False,
                'signal_target_count_limit': cfg.SLOTS, 'actual_position_cap_owner': 'trade',
                'model_entries': 'previous_completed_close*(1-D), low_touch, canonical_symbol_order, three_positions_503020',
                'pending_limit_reference': 'previous_completed_close*(1-D)',
            },
            'note': ('Signal targets and model PnL are independent of actual fills. '
                     'The legacy trade reader consumes desired[] and controls actual capacity; '
                     'it never writes actual fills back into this signal book.'),
        }
        atomic_write_json(self.path, payload)
        self.content_key = key
        self.last_write = now
        return True
