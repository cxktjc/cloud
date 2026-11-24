"""Trade-side terminal display: actual fills/PnL versus signal model.

Only changed content redraws the terminal. All values are computed by
``execution.stats`` from execution-owned state and signal-owned snapshots.
"""
from __future__ import annotations

import json
import logging
import math
import os
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Mapping

from .. import config as cfg


class MarketConsoleFilter(logging.Filter):
    """Collector chatter stays in trade.log; sticky errors live in the panel."""
    def filter(self, record):
        return not (record.name == 'v9711.ticker_ws' or record.name.startswith('v9711.ticker_ws.'))


def iso(ms: Any) -> str:
    value = 0.0
    try:
        value = float(ms)
    except (TypeError, ValueError):
        return '-'
    if not value:
        return '-'
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat(timespec='seconds')


def iso_ms(ms: Any) -> str:
    if not ms:
        return '未记录'
    return datetime.fromtimestamp(float(ms)/1000, tz=timezone.utc).isoformat(timespec='milliseconds')


def entry_timing_lines(timing):
    timing = timing or {}
    cross_symbol = bool(timing.get('trigger_symbol') and timing['trigger_symbol'] != timing.get('symbol'))
    line = (f'      首次下单={iso_ms(timing.get("send_epoch_ms"))} '
            f'收到行情={iso_ms(timing.get("received_epoch_ms"))} '
            f'来源={timing.get("quote_source", "未记录")} '
            f'通道={timing.get("transport", "未记录")} '
            f'行情时间={iso_ms(timing.get("exchange_time_ms"))}')
    labels = (('quote_to_decision', '收行情→判断'), ('decision_to_registered', '判断→登记'),
              ('registered_to_worker', '登记→线程'), ('worker_to_durable', '等待落盘'),
              ('durable_to_send', '落盘后→发送'), ('worker_to_send', '线程→发送'),
              ('send_to_response', '发送→回包'), ('response_to_owner', '回包→主线程确认'))
    delays = timing.get('latencies_ms') or {}
    if 'write_started_to_persisted' in delays:
        labels = (('quote_to_decision', '收行情→判断'), ('decision_to_registered', '判断→登记'),
                  ('registered_to_write_started', '保存排队'), ('write_started_to_persisted', '保存'),
                  ('persisted_to_durable', '保存后→线程'), ('durable_to_send', '线程→发送'),
                  ('send_to_response', '发送→回包'), ('response_to_owner', '回包→主线程确认'))
    details = ' '.join(f'{label}={delays[key]:.3f}ms' for key, label in labels if key in delays
                      and (key != 'worker_to_send' or 'durable_to_send' not in delays))
    if cross_symbol:
        line = line.replace('收到行情=', '本币报价接收=')
        details = details.replace('收行情→判断=', '本币报价→判断=')
    stages = (f'      该单登记={iso_ms(timing.get("registered_epoch_ms"))} '
              +(f'落盘完成={iso_ms(timing.get("persisted_epoch_ms") or timing["durable_epoch_ms"])} '
                if timing.get('persisted_epoch_ms') or timing.get('durable_epoch_ms') else '')
              +f'回包={iso_ms(timing.get("response_epoch_ms"))} '
              f'主线程确认={iso_ms(timing.get("owner_ack_epoch_ms"))}')
    extra = ' '.join(f'{label}={delays[key]:.3f}ms' for key, label in (
        ('queue_wait', '触发行情→出队'), ('dequeued_to_decision', '出队→选中'),
        ('trigger_to_send', '触发行情→发送'), ('quote_to_send', '依据报价→发送')) if key in delays)
    trigger = (f'触发={timing["trigger_symbol"]}/{timing.get("trigger_source")} '
               if timing.get('trigger_symbol') else '')
    if cross_symbol:
        extra = extra.replace('触发行情', '唤醒行情')
        trigger = f'评估唤醒={timing["trigger_symbol"]}/{timing.get("trigger_source")} '
    quote_lines = []
    if timing and ('order_limit_price' in timing or 'trigger_price' in timing):
        def number(value, *, percent=False):
            try:
                value = float(value)
                if math.isfinite(value) and (percent or value > 0):
                    return f'{value:+.4%}' if percent else f'{value:.10g}'
            except (TypeError, ValueError, OverflowError):
                pass
            return '未记录'
        trigger_gap = number(timing.get('trigger_gap'), percent=True)
        quote_lines = [f'      报价与挂单: {trigger}触发价={number(timing.get("trigger_price"))} '
                       f'挂单价={number(timing.get("order_limit_price"))} 触发价差={trigger_gap} '
                       f'准入价={number(timing.get("admission_price"))} '
                       f'准入价差={number(timing.get("admission_gap"), percent=True)}']
        if cross_symbol:
            quote_lines = [f'      报价与挂单: 币对={timing.get("symbol")} '
                           f'本币准入价={number(timing.get("admission_price"))} '
                           f'挂单价={number(timing.get("order_limit_price"))} '
                           f'准入价差={number(timing.get("admission_gap"), percent=True)}',
                           f'      {trigger}唤醒价={number(timing.get("trigger_price"))} '
                           '（跨币仅唤醒评估，不参与本币比价）']
        if 'quote_age_at_wake' in delays:
            quote_lines.append(f'      本币报价在唤醒时已存在={delays["quote_age_at_wake"]:.3f}ms（报价年龄，非排队耗时）')
    save_parts = ' '.join(f'{label}={delays[key]:.3f}ms' for key, label in (
        ('journal_prepare', '准备'), ('journal_write', '写入'),
        ('journal_sync', '同步'), ('journal_receipt', '回执')) if key in delays)
    checks = {'near_skipped': '准入距离≤1.5%，免二次行情检查',
              'follow_skipped': '补挂单沿用原规则',
              'far_passed': '准入距离>1.5%，二次行情检查通过',
              'far_rejected': '准入距离>1.5%，二次行情检查拒绝'}
    check = timing.get('entry_send_check')
    check_lines = [f'      发送检查: {checks.get(check, check)}'] if check else []
    prepare = ' '.join(f'{label}={delays[key]:.3f}ms' for key, label in (
        ('durable_to_socket', '续发→socket协程'), ('socket_wait', 'socket等待/连接'),
        ('send_guard', '发送检查'), ('sign_encode', '签名编码'), ('encoded_to_send', '编码后→发送')) if key in delays)
    return [line]+([stages] if timing else [])+([f'      分段耗时: {details}'] if details else [])+(
        [f'      保存细分: {save_parts}'] if save_parts else [])+(
        [f'      发送准备细分: {prepare}'] if prepare else [])+quote_lines+check_lines+(
        [f'      入场链路: {trigger}{extra}'] if extra else [])


def compact_positions(rows: Any, limit: int) -> str:
    if isinstance(rows, Mapping):
        source: Any = rows.values()
    elif isinstance(rows, list):
        source = rows
    else:
        source = ()
    parts = []
    for row in source:
        if not isinstance(row, Mapping):
            continue
        symbol = str(row.get('symbol') or '').upper()
        qty = row.get('quantity')
        mark = row.get('mark_price')
        pnl = row.get('unrealized_pnl')
        label = '[补挂]' if row.get('signal_follow') else ''
        if 'client_order_id' in row:
            sent = row.get('entry_send_quote') or {}
            stage = '发送' if sent else '登记'
            if (sent or row).get('entry_quote_policy') in {'admission_frozen', 'tiered_admission'}:
                stage = '准入'
            qualified = (sent or row).get('entry_qualified_by', 'unknown')
            label += f'[{stage}满足={qualified}]'
        parts.append(f'{symbol}{label} qty={qty} mark={mark} pnl={pnl}')
        if len(parts) >= limit:
            break
    if not parts:
        return '(空)'
    return ', '.join(parts)


class TradeTerminalView:
    def __init__(self, *, min_refresh_seconds: float = cfg.TradeViewConfig().min_refresh_seconds,
                 heartbeat_seconds: float = 60.0, max_rows: int = 12):
        self.min_refresh_seconds = float(min_refresh_seconds)
        self.heartbeat_seconds = float(heartbeat_seconds)
        self.max_rows = int(max_rows)
        self.last_key = ''
        self.last_render = float('-inf')

    def _key(self, fields: Mapping[str, Any]) -> str:
        stable = {key: value for key, value in fields.items()
                  if key not in {'clock_utc', 'updated_at_epoch', 'stream_health'}}
        health = fields.get('stream_health') or {}
        stable['stream_health'] = {key: health.get(key) for key in ('fresh', 'reason')}
        return json.dumps(stable, ensure_ascii=False, sort_keys=True, default=str)

    def render(self, *, now: float | None = None, force: bool = False,
               **fields: Any) -> bool:
        now = time.monotonic() if now is None else float(now)
        key = self._key(fields)
        if not (force or key != self.last_key):
            return False
        if not force and now - self.last_render < self.min_refresh_seconds:
            return False
        self.last_key, self.last_render = key, now
        try:
            os.system('cls' if os.name == 'nt' else 'clear')
            sys.stdout.reconfigure(encoding='utf-8')
        except (AttributeError, OSError, ValueError):
            pass
        print(self._format(fields), flush=True)
        return True

    def _format(self, f: Mapping[str, Any]) -> str:
        actual = f['comparison']['actual']
        signal = f['comparison']['signal']
        diff = f['comparison']['difference']
        capacity = f['comparison']['capacity']
        parts = diff['difference_part_pnl']
        gap = diff['signal_minus_trade']
        mode = 'live' if f.get('enabled') and not f.get('dry_run') else 'dry_run'
        lines = [
            '=' * 116,
            f'v9711 WS-DIRECT v2 TRADE  {f["clock_utc"]}  mode={mode}  authority=execution',
            f'signal_session={f.get("signal_session_id") or "-"}  '
            f'signal_revision={f.get("signal_revision") if f.get("signal_revision") is not None else "-"}  '
            f'trade_cap={cfg.SLOTS}  pending_order_cap={cfg.MAX_PENDING_ORDERS}',
            '-' * 116,
            ('当前trade持仓（含手动导入的signal账本仓位）: '
             if f.get('manual_signal_sync') else '当前实际持仓（只含精确归属成交）: ')
            + compact_positions(f['comparison']['positions']['actual'], self.max_rows),
            f'trade实际累计收益(已实现): {actual["realized_pnl"]:+.6f}  '
            f'trade当前收益(浮盈): {actual["unrealized_pnl"]:+.6f}  '
            f'trade总收益: {actual["total_pnl"]:+.6f} ({actual["return_pct"]:+.4f}%)',
            f'signal模型累计收益(已实现): {signal["realized_pnl"]:+.6f}  '
            f'signal当前收益(浮盈): {signal["unrealized_pnl"]:+.6f}  '
            f'signal总收益: {signal["total_pnl"]:+.6f} ({signal["return_pct"]:+.4f}%)',
            f'差异 signal-trade: 已实现={gap["realized_pnl"]:+.6f}  '
            f'浮动={gap["unrealized_pnl"]:+.6f}  总计={gap["total_pnl"]:+.6f} '
            f'({gap["return_pct"]:+.4f}%)',
            f'区别部分收益: signal-only浮动={parts["signal_only_unrealized_pnl"]:+.6f}  '
            f'trade-only浮动={parts["trade_only_unrealized_pnl"]:+.6f}  '
            f'匹配仓浮动差={parts["matched_unrealized_difference_signal_minus_trade"]:+.6f}',
            f'8有效挂单/3仓限制: signal目标={capacity["signal_target_count"]} '
            f'trade持仓={capacity["actual_position_count"]} '
            f'普通挂单={capacity["pending_order_count"]} '
            f'待撤普通={capacity.get("cancel_pending_order_count", 0)} '
            f'补挂单={capacity.get("signal_follow_order_count", 0)}/{cfg.SIGNAL_FOLLOW_MAX_PENDING_ORDERS} '
            f'signal应挂单={capacity["signal_desired_count"]} '
            f'有价可评估={capacity["priced_desired_count"]} '
            f'近阈值={capacity["near_place_count"]}',
            f'补挂规则: signal原入场限价，挂出后固定数量，不随仓数变化撤改；最多额外{cfg.SIGNAL_FOLLOW_MAX_PENDING_ORDERS}单；'
            f'trade持仓≥{cfg.SIGNAL_FOLLOW_POSITION_STOP}时撤补挂单；普通挂单数不含补挂单。',
            f'容量排除/缺口: 超出3仓目标={capacity["signal_targets_over_position_cap"]} '
            f'未实际持有signal目标={capacity["signal_targets_not_held"]} '
            f'应挂单超8={capacity["signal_desired_over_pending_cap"]} '
            f'有价候选超8={capacity["priced_desired_over_pending_cap"]} '
            f'近阈值超8={capacity["near_place_over_pending_cap"]} '
            f'rotation_defer={capacity["rotation_defer_count"]}',
            f'容量排除浮动收益估计: {parts["capacity_excluded_unrealized_pnl"]:+.6f} '
            f'({parts["capacity_excluded_return_pct"]:+.4f}%)  '
            '说明：这是signal-only当前目标按同价估算，不是已实现收益。',
            '-' * 116,
            '持仓浮盈差异（含双方持有；signal-trade，正=signal领先）:',
        ]
        rows = f['comparison']['positions']['largest_difference'][:self.max_rows]
        if not rows:
            lines.append('  （暂无差异）')
        for row in rows:
            kind = {'matched': '双方持有', 'signal_only': '仅signal持有', 'trade_only': '仅trade持有'}.get(row['kind'], row['kind'])
            lines.append(
                f'  {row["symbol"]} [{kind}] signal_pnl={row["signal_unrealized_pnl"]:+.6f} '
                f'trade_pnl={row["trade_unrealized_pnl"]:+.6f} '
                f'diff={row["difference_signal_minus_trade"]:+.6f}')
        lines.extend([
            '-' * 116,
            '当前挂单: ' + compact_positions(f.get('open_orders', {}), self.max_rows),
            'signal-only目标（按名义金额最大）: '
            + compact_positions(f['comparison']['positions']['signal_only_top'], self.max_rows),
            f'stream={f.get("stream_health", {}).get("fresh", False)}  '
            f'last_event_seq={f.get("stream_health", {}).get("last_event_seq", "-")}  '
            f'feedback_updated={f.get("updated_at_epoch")}',
            ('trade收益=手动导入的一次性账本基准+后续trade独立收益；导入仓位不代表实际入场成交。'
             if f.get('manual_signal_sync') else
             'trade收益仅来自精确归属订单成交/平仓和当前实际持仓；差异按同价对比signal模型，不是交易所账户总收益。'),
            '=' * 116,
        ])
        forced_count = len(f.get('forced_close_history') or {})
        manual_count = len(f.get('manual_close_history') or {})
        cleanup_count = len(f.get('forced_close_cleanup') or {})
        if forced_count or manual_count or cleanup_count:
            lines.insert(-1, f'Closed audit: forced={forced_count} manual/external={manual_count} '
                         f'old exact-order cleanup pending={cleanup_count} '
                         '(audit retained; not an open position)')
        diagnostics = f.get('entry_diagnostics') or {}
        ws = diagnostics.get('ws_direct') or {}
        if ws:
            lines.insert(-1, f'Internal WS: healthy={ws.get("healthy")} queue={ws.get("queue")} '
                         f'prices={ws.get("fresh_symbols", 0)}/{ws.get("subscribed", 0)} decided={ws.get("decided")} stale={ws.get("rejected_backlog")} '
                         f'overflow={ws.get("overflow")} p95_us={ws.get("receive_to_decision_us_p95")} '
                         '(decision/dispatch, not exchange ACK; mirror runs after decision) '
                         f'build={(ws.get("runtime") or {}).get("build", "-")}')
            lines.insert(-1, f'tick/bid0 OR准入: tick有效={ws.get("fresh_symbols", 0)} '
                         f'bid0有效={ws.get("bid0_symbols", 0)} 任一路有效={ws.get("entry_symbols", 0)}；估值仍用tick。')
            lines.insert(-1, f'行情负载: 接收={ws.get("receive_per_second", 0):.0f}/s '
                         f'入队={ws.get("enqueue_per_second", 0):.0f}/s '
                         f'决策={ws.get("decide_per_second", 0):.0f}/s 积压={ws.get("queue", 0)} '
                         f'峰值={ws.get("queue_peak", 0)} 买一未变={ws.get("unchanged", 0)} '
                         f'远价快速处理={ws.get("skipped_far_events", 0)} '
                         f'接收侧远价完成={ws.get("screened_far", 0)} '
                         f'过载丢弃={ws.get("overload_discarded", 0)} '
                         f'当前丢弃={ws.get("discarded_per_second", 0):.0f}/s')
            work = ws.get('owner_work') or {}
            latency = ws.get('entry_latency') or {}
            trigger_latency = latency.get('trigger') or {}
            if trigger_latency.get('samples'):
                lines.insert(-1, f'本机评估唤醒→发送(ms，含跨币唤醒): P50={trigger_latency.get("p50_ms")} '
                             f'P95={trigger_latency.get("p95_ms")} P99={trigger_latency.get("p99_ms")} '
                             f'最大={trigger_latency.get("max_ms")} ≤10ms={trigger_latency.get("within_10ms")}/{trigger_latency.get("samples")}')
            if latency.get('samples'):
                lines.insert(-1, f'本机依据报价→发送(ms，最近最多512次登记): P50={latency.get("p50_ms")} '
                             f'P95={latency.get("p95_ms")} P99={latency.get("p99_ms")} '
                             f'最大={latency.get("max_ms")} ≤10ms={latency.get("within_10ms")}/{latency.get("samples")}')
                stages = latency.get('stages') or {}
                if stages:
                    edges = (
                        ('queue_wait', '排队'), ('dequeued_to_decision', '选单'),
                        ('decision_to_registered', '登记快照'), ('registered_to_worker', 'worker调度'),
                        ('worker_to_durable', '等待落盘'), ('durable_to_send', '落盘后发送'))
                    if (stages.get('write_started_to_persisted') or {}).get('samples'):
                        edges = (('queue_wait', '排队'), ('dequeued_to_decision', '选单'),
                                 ('decision_to_registered', '登记快照'),
                                 ('registered_to_persisted', '保存等待'),
                                 ('persisted_to_durable', '保存后调度'), ('durable_to_send', '发送准备'))
                    parts = [f'{label}={(stages.get(key) or {}).get("p95_ms")}' for key, label in edges]
                    lines.insert(-1, '已发送订单分段P95(ms): '+ ' '.join(parts)+'；各段分位数不可相加')
                    if (stages.get('persisted_to_durable') or {}).get('samples'):
                        lines.insert(-1, '落盘等待细分P95(ms): 登记→保存回执就绪='
                                     f'{stages["registered_to_persisted"].get("p95_ms")} '
                                     f'回执就绪→worker续发={stages["persisted_to_durable"].get("p95_ms")}')
                    if (stages.get('write_started_to_persisted') or {}).get('samples'):
                        lines.insert(-1, '保存细分P95(ms): 登记→开始保存='
                                     f'{(stages.get("registered_to_write_started") or {}).get("p95_ms")} '
                                     f'开始保存→回执={stages["write_started_to_persisted"].get("p95_ms")}')
                    if (stages.get('journal_sync') or {}).get('samples'):
                        pieces = [f'{label}={(stages.get(key) or {}).get("p95_ms")}' for key, label in (
                            ('journal_prepare', '准备'), ('journal_write', '写入'),
                            ('journal_sync', '同步'), ('journal_receipt', '回执'))]
                        lines.insert(-1, '日志保存分段P95(ms): '+' '.join(pieces)+'；各段分位数不可相加')
                    if (stages.get('socket_wait') or {}).get('samples'):
                        pieces = [f'{label}={(stages.get(key) or {}).get("p95_ms")}' for key, label in (
                            ('durable_to_socket', '续发→socket协程'), ('socket_wait', 'socket等待/连接'),
                            ('send_guard', '发送检查'), ('sign_encode', '签名编码'), ('encoded_to_send', '编码后→发送'))]
                        lines.insert(-1, '发送准备分段P95(ms): '+' '.join(pieces)+'；各段分位数不可相加')
            activity = ws.get('request_activity') or {}
            windows = activity.get('windows') or {}
            if windows:
                parts = [f'{label}='+ '/'.join(str(windows.get(w, {}).get(key, 0)) for w in ('1', '10', '60'))
                         for key, label in (('entry', '入场'), ('close', '平仓'), ('cancel', '撤单'), ('query', '查询'))]
                ten, minute = windows.get('10', {}), windows.get('60', {})
                lines.insert(-1, '交易请求尝试（最近1/10/60秒，非成交数）: '+' '.join(parts)+
                             f' 在途={activity.get("inflight", 0)} 峰值在途={activity.get("peak_inflight", 0)}')
                lines.insert(-1, f'交易/接收诊断: 10秒请求失败={ten.get("failed", 0)} 批次成员拒绝={ten.get("member_rejected", 0)} '
                             f'本地未发拒绝={ten.get("local_rejected", 0)} '
                             f'限频回包={ten.get("rate_limited", 0)} 错误={ten.get("errors", {})} '
                             f'60秒请求峰值={minute.get("peak_requests_per_second", 0)}/s '
                             f'接收循环延迟最大={minute.get("loop_lag_max_ms", 0)}ms '
                             f'请求最多币对={minute.get("top_symbols", {})} '
                             f'累计socket故障={ws.get("socket_failures", {})}')
            if work:
                quote, maintenance = work.get('quote') or {}, work.get('maintenance') or {}
                lines.insert(-1, f'处理耗时P95/最大(ms): 行情={quote.get("p95_ms", 0):.3f}/{quote.get("max_ms", 0):.3f} '
                             f'维护={maintenance.get("p95_ms", 0):.3f}/{maintenance.get("max_ms", 0):.3f} '
                             f'未变买一={ws.get("unchanged_per_second", 0):.0f}/s '
                             f'过期拒绝={ws.get("expired_per_second", 0):.0f}/s')
                slow = (work.get('recent_slow') or [])[-1:]
                for item in slow:
                    lines.insert(-1, f'最近慢处理: {iso_ms(item.get("epoch", 0)*1000)} '
                                 f'{item.get("kind")} {item.get("elapsed_ms")}ms '
                                 f'主要阶段={item.get("stage")} {item.get("stage_ms")}ms')
            writer = ws.get('feedback_writer') or {}
            durability = ws.get('durability_writer') or {}
            if durability.get('enabled'):
                storage = ('双槽单次同步='+str((durability.get('file_write_ms') or {}).get('state', 0))+'ms '
                           if durability.get('persistent_slots') else
                           f'intents/state={(durability.get("file_write_ms") or {}).get("intents", 0):.3f}/'
                           f'{(durability.get("file_write_ms") or {}).get("state", 0):.3f}ms ')
                if durability.get('incremental_journal'):
                    storage = (f'增量日志={durability.get("record_bytes", 0)}B '
                               f'同步={durability.get("sync_method", "fsync")} '
                               f'准备/写入/同步={durability.get("prepare_ms", 0):.3f}/'
                               f'{durability.get("write_ms", 0):.3f}/{durability.get("sync_ms", 0):.3f}ms '
                               f'序号={durability.get("commit_sequence", 0)} '
                               f'检查点={durability.get("checkpoint_sequence", 0)} '
                               f'检查点错误={durability.get("checkpoint_error") or "无"} ')
                lines.insert(-1, f'订单账本后台写盘: 写入中={int(durability.get("writing", False))} '
                             f'待写={durability.get("pending", 0)} 合并={durability.get("coalesced", 0)} '
                             f'最近={durability.get("last_write_ms", 0):.3f}ms '
                             f'快照排队={durability.get("queue_wait_ms", 0):.3f}ms '
                             +storage+
                             f'失败={durability.get("failures", 0)} 错误={durability.get("error") or "无"}；落盘成功后才发单')
            if writer.get('enabled'):
                lines.insert(-1, f'反馈写盘: 后台待写={writer.get("pending", 0)} '
                             f'最近写盘={writer.get("last_write_ms", 0):.3f}ms '
                             f'失败={writer.get("failures", 0)} 当前错误={writer.get("error") or "无"}')
            lines.insert(-1, f'depth WS: endpoint={ws.get("depth_endpoint", "-")} proxy={ws.get("proxy", "-")}')
            for source in ('tick', 'depth'):
                for group, error in list((ws.get(source+'_errors') or {}).items())[:3]:
                    lines.insert(-1, f'  {source} group={group} error={error}')
            recent = ws.get('recent_errors') or []
            if recent:
                block = ['WS 最近报错（UTC，恢复后连续正常 10 分钟隐藏；相同错误合并计数）:']
                for row in recent[:5]:
                    status = '已恢复' if row.get('recovered') else '未恢复'
                    block.append(f'  {iso(row["last_epoch"]*1000)} {row["source"]} group={row["group"]} '
                                 f'次数={row["count"]} [{status}] {row["error"]}')
                    before = row.get('activity_before_failure')
                    if before is not None:
                        block.append(f'    断线前10秒: 入场={before.get("entry", 0)} 平仓={before.get("close", 0)} '
                                     f'撤单={before.get("cancel", 0)} 查询={before.get("query", 0)} '
                                     f'请求失败={before.get("failed", 0)} 限频={before.get("rate_limited", 0)} '
                                     f'接收循环延迟最大={before.get("loop_lag_max_ms", 0)}ms '
                                     f'在途={before.get("inflight", 0)} 行情积压={before.get("queue", 0)}')
                block.append(f'完整日志: {cfg.TRADE_LOG_FILE}')
                block.append('-' * 116)
                lines[4:4] = block
        for symbol, quote in list(diagnostics.get('entry_quotes', {}).items())[:3]:
            def shown(key):
                value = quote.get(key+'_gap')
                return f'{value*100:.4f}%' if value is not None else '无效'
            lines.insert(-1, f'  {symbol} tick距离={shown("tick")} bid0距离={shown("bid0")} '
                         f'判断来源={quote.get("source") or "无"}')
        sync = f.get('manual_signal_sync') or {}
        if sync:
            lines.insert(-1, '账本模式: 手动同步一次后trade独立维护；'
                         f'signal导入版本={sync.get("signal_revision", "-")} '
                         f'导入仓位={sync.get("imported_position_count", 0)}；不自动重新同步')
        if diagnostics:
            lines.insert(-1, 'Entry plan: status={desired_status} signal_ready={signal_ready} '
                         'plan_ready={plan_ready} published={published} executable={priced_executable}'
                         .format(**dict(diagnostics, published=diagnostics.get('template_total', diagnostics['published']))))
            lines.insert(-1, '  blockers=' + ','.join(diagnostics.get('blockers') or ['none'])
                         + ' selected=' + ','.join(diagnostics.get('selected_to_place') or ['none']))
            filtered = diagnostics.get('filtered') or {}
            quiet_reasons = {'quote_waiting_first_tick', 'quote_waiting_reconnect_tick',
                             'quote_stream_not_ready', 'quote_stale',
                             'missing_or_stale_price_or_invalid_limit'}
            waiting = sum(reason in {'quote_waiting_first_tick', 'quote_waiting_reconnect_tick'}
                          for reason in filtered.values())
            stale = sum(reason == 'quote_stale' for reason in filtered.values())
            if waiting or stale:
                lines.insert(-1, f'  quotes: waiting first/reconnect tick={waiting} stale={stale}')
            actionable = [(symbol, reason) for symbol, reason in filtered.items()
                          if reason not in quiet_reasons]
            for symbol, reason in actionable[:3]:
                lines.insert(-1, f'  entry skipped {symbol}: {reason}')
        rejections = sorted((f.get('entry_rejections') or {}).values(),
                            key=lambda row: row.get('rejected_at_epoch', 0), reverse=True)
        for row in rejections[:3]:
            lines.insert(-1, f'  ENTRY REJECTED {row.get("symbol")}: {row.get("error")}')
        session = f.get('session_entry_stats')
        if session is not None:
            block = [
                '-' * 116,
                '本次启动漏开统计（重启清零；时间UTC）:',
                f'  启动={iso(session["started_at_epoch"] * 1000)}  '
                f'signal累计开仓={session["signal_entries"]}  '
                f'trade已开仓（有成交）={session["trade_filled_entries"]}  '
                f'未见trade成交={session["unfilled_entries"]}',
                f'  未成交分项: signal仍持仓={session["active_unfilled_entries"]}  '
                f'signal已结束/撤回={session["ended_unfilled_entries"]}',
            ]
            rows = session['recent_unfilled_entries'][:self.max_rows]
            if rows:
                block.append(f'  未成交明细（最近{len(rows)}笔；按入场确认时间倒序）:')
                for row in rows:
                    state = 'signal仍持仓' if row['signal_active'] else 'signal已结束/撤回'
                    block.append(f'    {row["symbol"]} 入场bar={iso(row["entry_bar_open_ms"])} '
                                 f'确认={iso(row["entry_available_ms"])} [{state}]')
                    timing = row.get('first_order_timing')
                    if timing:
                        block.extend(entry_timing_lines(timing))
                    else:
                        block.append('      该笔signal尚无匹配的下单时间记录；不使用其他入场bar的记录代填。')
                    attempts = row.get('order_attempts') or {}
                    latest = attempts.get('latest') or {}
                    if latest:
                        reasons = {'order_bar_expired': '入场bar结束', 'entry_gap_too_far': '距离超过撤单门槛',
                                   'replaced_by_closer': '被更近候选替换', 'position_capacity_full': '持仓容量已满',
                                   'position_capacity_full_cancel_all': '持仓容量已满',
                                   'signal_follow_ended': 'signal持仓结束'}
                        reason = latest.get('cancel_reason') or latest.get('retired_reason') or ''
                        block.append(f'      委托跟踪：已登记={attempts.get("registered", 0)} 实际发送={attempts.get("sent", 0)} '
                                     f'最近状态={latest.get("status") or "待确认"} '
                                     f'累计成交量={latest.get("executed_qty", "未确认")} '
                                     f'撤销/终结原因={reasons.get(reason, reason) or "尚未记录"}')
                        if latest.get('submit_error'):
                            block.append(f'      最近提交异常：{latest["submit_error"]}')
                    current = row.get('same_symbol_trade')
                    if current:
                        reason = ('入场bar不同' if current.get('mismatch_reason') == 'different_entry_bar'
                                  else '入场身份未匹配')
                        block.append(f'      同币trade已有持仓：入场bar={iso(current.get("entry_bar_open_ms"))} '
                                     f'成交={iso_ms(current.get("entry_fill_time_ms"))}；{reason}，不计入该笔signal成交。')
                        if current.get('first_order_timing'):
                            block.append('      同币trade这笔持仓的下单记录（另笔入场）:')
                            block.extend(entry_timing_lines(current['first_order_timing']))
            else:
                block.append('  未成交明细: （暂无）')
            block.append('  口径: 只计启动后完成3m确认的signal入场；部分成交算已开仓；'
                         '延迟成交回报会修正未成交数，实际平仓不会重新算漏开。')
            lines[-1:-1] = block
        for symbol, row in list((f.get('open_orders') or {}).items())[:self.max_rows]:
            if (row.get('sizing_weight') is not None
                    and abs(Decimal(str(row.get('submitted_notional', 0)))
                            - Decimal(str(row.get('sizing_raw_notional', 0)))) > Decimal('0.5')):
                lines.insert(-1, f'  {symbol} 挂单资金档={row["sizing_weight"]:.0%} '
                             f'理论={row.get("sizing_raw_notional", 0):.6f}U '
                             f'实际委托={row.get("submitted_notional", 0):.6f}U')
        rotation = f.get('pending_rotation') or {}
        if rotation:
            lines.insert(-1, f'换单保留名额: 撤{rotation.get("victim")} → {rotation.get("challenger")} '
                         f'原挂单数={rotation.get("original_order_count")} '
                         f'旧单终态确认={rotation.get("victim_terminal", False)}；确认前不另占空位。')
        io = f.get('network_io') or {}
        if io.get('enabled'):
            lines.insert(-1, f'网络I/O: 后台运行={io.get("running", 0)} '
                         f'排队={io.get("queued", 0)} 待主循环确认={io.get("completed", 0)} '
                         '（信号轮询不等待网络响应）')
            warmup = io.get('transport_warmup') or {}
            if warmup:
                lines.insert(-1, f'交易连接启动预热: 就绪={warmup.get("ready", 0)} '
                             f'进行中={warmup.get("pending", 0)} 失败={warmup.get("failed", 0)}')
        pacing = io.get('entry_cancel_pacing') or {}
        if pacing:
            active = ', '.join(pacing.get('active', ())) or '(空)'
            queued = pacing.get('queued') or []
            lines.insert(-1, f'入场撤单队列: 执行中={active} 等待={len(queued)}；'
                         f'按绝对相对价差近→远，前笔返回后间隔{pacing["interval_seconds"]:g}s；'
                         '不阻塞下单/信号/平仓，平仓撤单不排此队列。')
            if queued:
                parts = [f'{row["symbol"]} 距离={row["distance_pct"]:.4f}%'
                         if row['distance_pct'] is not None else f'{row["symbol"]} 距离=未知'
                         for row in queued[:self.max_rows]]
                lines.insert(-1, '  待撤顺序: ' + ' → '.join(parts))
        closes = f.get('close_orders') or {}
        if closes:
            lines.insert(-1, '在途平多（不占8个入场挂单名额；确认成交前仍计持仓）:')
            for symbol, row in list(closes.items())[:self.max_rows]:
                deadline = iso(float(row.get('deadline_epoch') or 0) * 1000)
                lines.insert(-1, f'  {symbol} {row.get("phase")} status={row.get("status")} '
                             f'剩余={row.get("remaining_owned_qty")} 到期UTC={deadline} '
                             f'撤单确认中={row.get("cancel_requested", False)}')
        return '\n'.join(lines)
