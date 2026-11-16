"""Changed-content terminal view of SIGNAL targets, not execution/account state."""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from . import config as cfg
from .contracts import DesiredOrder


def iso(ms: int | None) -> str:
    return '-' if not ms else datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat(timespec='seconds')


def compact_orders(rows: Mapping[str, Mapping[str, Any]], limit: int) -> str:
    if not rows:
        return '(空)'
    parts = []
    for symbol in sorted(rows)[:limit]:
        row = rows[symbol]
        price = row.get('limit_price', row.get('entry_price', row.get('price', '')))
        quantity = row.get('quantity')
        suffix = f' qty={quantity:.8g}' if isinstance(quantity, (float, int)) else ''
        if row.get('exit_due_ms') and 'entry_price' in row:
            suffix += f' exit={iso(row["exit_due_ms"])}'
        parts.append(f'{symbol}@{price}{suffix}')
    more = len(rows) - limit
    return ', '.join(parts) + (f' …+{more}' if more > 0 else '')


def desired_map(orders: Mapping[str, DesiredOrder] | Mapping[str, Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    return {symbol: row.to_dict() if hasattr(row, 'to_dict') else dict(row)
            for symbol, row in orders.items()}


def latest_completed_positions(trades: Iterable[Mapping[str, Any]]) -> tuple[Mapping[str, Any], ...]:
    """Latest completed-close batch by its actual model exit time, not append order.

    Delayed older exits can be appended after a newer exit. Same-time exits are
    displayed together, without using position capacity or a display-count cap.
    """
    rows = [row for row in trades if isinstance(row, Mapping) and row.get('exit_available_ms')]
    if not rows:
        return ()
    latest_ms = max(int(row['exit_available_ms']) for row in rows)
    return tuple(sorted((row for row in rows if int(row['exit_available_ms']) == latest_ms),
                        key=lambda row: str(row.get('symbol', ''))))


def completed_positions_block(trades: Iterable[Mapping[str, Any]]) -> list[str]:
    """Separate bottom panel; entry-bar time never pretends to be exact fill time."""
    rows = latest_completed_positions(trades)
    lines = ['上一次完成持仓（信号模型；时间UTC）:']
    if not rows:
        return lines + ['  暂无已完成持仓']
    lines.append(f'  退出时间={iso(int(rows[0]["exit_available_ms"]))}  共{len(rows)}个')
    for row in rows:
        lines.append(
            f'  {row["symbol"]} 入场bar={iso(row.get("entry_bar_open_ms"))} '
            f'确认时间={iso(row.get("entry_available_ms"))} '
            f'入场价={row["entry_price"]:.10g} 退出价={row["exit_price"]:.10g} '
            f'数量={row["quantity"]:.8g} 已实现盈亏={row["pnl"]:+.6f}')
    return lines


class TerminalView:
    def __init__(self, view_config: cfg.ViewConfig = cfg.SignalViewConfig()):
        self.view = view_config
        self.last_content = ''
        self.last_render = float('-inf')

    @staticmethod
    def _change_key(fields: Mapping[str, Any]) -> str:
        stable = {}
        for key, value in fields.items():
            if key in {'clock_utc', 'status_line', 'price_sequence',
                       'total_price_updates', 'total_evaluations', 'recent_decisions'}:
                continue
            if key == 'desired':
                stable[key] = {s: {k: r.get(k) for k in ('order_bar_open_ms', 'limit_price', 'notional')}
                               for s, r in value.items()}
            elif key == 'bar_stats':
                stable[key] = {k: value.get(k) for k in (
                    'bar_open_ms', 'candidates', 'place', 'cancel', 'expire', 'withdraw',
                    'entry_confirmed', 'pending_confirmation', 'touched_limit',
                    'stale_price_symbols', 'near_place_candidates', 'desired_orders',
                    'signal_entries', 'signal_exits')}
            else:
                stable[key] = value
        return json.dumps(stable, sort_keys=True, ensure_ascii=False, default=str)

    def render(self, *, now: float | None = None, force: bool = False, **fields: Any) -> bool:
        now = time.monotonic() if now is None else float(now)
        key = self._change_key(fields)
        # File liveness heartbeat must not force a screen clear/redraw.
        if not (force or key != self.last_content):
            return False
        if not force and now - self.last_render < self.view.min_refresh_seconds:
            return False
        self.last_content, self.last_render = key, now
        try:
            os.system('cls' if os.name == 'nt' else 'clear')
            sys.stdout.reconfigure(encoding='utf-8')
        except (AttributeError, OSError, ValueError):
            pass
        print(self._format(fields), flush=True)
        return True

    def _format(self, f: Mapping[str, Any]) -> str:
        book = f['portfolio']
        stats = f['bar_stats']
        positions = book['positions']
        desired = f['desired']
        lines = [
            '=' * 112,
            f'v9711 WS-DIRECT v2 SIGNAL  {f["clock_utc"]}  authority=signal',
            f'状态: {f["status_line"]}',
            f'参考bar(open/available): {iso(f["reference_bar_open_ms"])} / {iso(f["reference_bar_available_ms"])}',
            f'下单bar(open/available): {iso(f["bar_open_ms"])} / {iso(f["bar_available_ms"])}  price_seq={f["price_sequence"]}',
            f'账本起始bar: {iso(book["start_bar_open_ms"])}  已计算bar: {iso(book["last_processed_bar_open_ms"])}  revision={book["revision"]}',
            '-' * 112,
            f'信号应持仓({len(positions)}; trade实际仓位上限={cfg.SLOTS}): {compact_orders(positions, self.view.max_orders_display)}',
            f'信号累计收益(已实现): {book["cumulative_pnl"]:+.6f}  信号当前收益(浮盈): {book["current_unrealized_pnl"]:+.6f}',
            f'信号总收益: {book["total_pnl"]:+.6f} ({book["return_pct"]:+.4f}%)  信号已实现余额: {book["realized_balance"]:.6f}',
            f'compound_equity={book["realized_balance"]:.6f}  next_order_target_503020={book["notional_per_order"]:.6f}',
            f'信号应挂单({len(desired)}): {compact_orders(desired, self.view.max_orders_display)}',
            '-' * 112,
            f'本bar挂撤计划: candidates={stats["candidates"]} near_place={stats["near_place_candidates"]} '
            f'place={stats["place"]} keep={stats["keep"]} cancel={stats["cancel"]} '
            f'below_limit={stats["touched_limit"]} stale_price={stats["stale_price_symbols"]}',
            f'本bar订单归因: entry_confirmed={stats.get("entry_confirmed", 0)} '
            f'expire={stats.get("expire", 0)} withdraw={stats.get("withdraw", 0)} '
            f'pending_confirmation={stats.get("pending_confirmation", 0)}',
            f'本bar信号入/退出: entry={stats.get("signal_entries", 0)} exit={stats.get("signal_exits", 0)}  '
            f'累计信号入/退出: {book["total_entries"]}/{book["total_exits"]}  pending_limit=unlimited  signal_position_cap={cfg.SLOTS}  trade_position_cap={cfg.SLOTS}',
            f'累计挂撤计划: place={f["total_place"]} cancel={f["total_cancel"]} '
            f'entry_confirmed={f.get("total_entry_confirmed", 0)} expire={f.get("total_expire", 0)} '
            f'withdraw={f.get("total_withdraw", 0)} bars={f["total_bars"]} '
            f'price_updates={f["total_price_updates"]} evaluations={f["total_evaluations"]}',
            f'挂单策略: signal发布≤{100 * cfg.SIGNAL_DISCOVERY_GAP:g}%候选；trade不足5单准入≤{100 * cfg.BOOTSTRAP_PLACE_GAP:g}%，'
            f'第6～8单准入≤{100 * cfg.FILL_PLACE_GAP:g}%；5～7单优先换旧、不增单；满8替换准入≤{100 * cfg.PLACE_GAP:g}%；'
            f'通常优势>{100 * cfg.SWITCH_TOLERANCE:g}个百分点，或新<{100 * cfg.PLACE_GAP:g}%直接换旧>{100 * cfg.FILL_PLACE_GAP:g}%；'
            '先确认撤旧再挂新；不因价差扩大自动撤单；'
            'entry_confirmed=已发计划确认信号入仓；expire=到期未入仓；withdraw=数据/状态异常等撤回；pending_confirmation=等待完成bar。',
        ]
        if book['pending_exit_price_symbols']:
            lines.append('已到退出bar，等待对应已完成close，账本保留: ' + ', '.join(book['pending_exit_price_symbols']))
        if book['stale_mark_symbols']:
            lines.append('以下信号仓位暂用最近已完成close估值: ' + ', '.join(book['stale_mark_symbols']))
        if not book['ready']:
            lines.append('信号计算尚未追平有效3m状态；保留既有应持仓，暂不发布新挂单。')
        recent = f.get('recent_decisions', ())[-self.view.max_orders_display:]
        if recent:
            lines.append('最近信号动作: ' + ', '.join(recent))
        lines.append('以上收益按三仓50/30/20信号模型计算，不是交易所成交或账户收益。')
        lines.append('实际持仓数量由trade控制；执行反馈不反向决定信号。')
        lines.append('信号只读本地行情；以前根close预设限价、完成3m确认应持仓，发布 schema=1 / contract=2。')
        lines.append('-' * 112)
        lines.append(f.get('nearest_candidates_line', '最近3候选:\n  尚未计算价格距离'))
        lines.append('-' * 112)
        lines.extend(completed_positions_block(
            f.get('last_completed_positions', book.get('recent_trades', ()))))
        lines.append('=' * 112)
        return '\n'.join(lines)
