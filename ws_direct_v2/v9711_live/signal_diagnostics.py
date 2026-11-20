"""Read-only distance diagnostics using the exact prices used for selection.

Never selects or limits orders. Held targets and invalid/stale quotes are not
ranked; the caller passes the already freshness-filtered SignalInput mapping.
"""
from __future__ import annotations

from dataclasses import dataclass
from heapq import nsmallest
import math
from typing import Iterable, Mapping

from . import config as cfg
from .signal_logic import Candidate, SignalInput


@dataclass(frozen=True)
class CandidateDistance:
    symbol: str
    observed_price: float
    limit_price: float
    gap: float
    status: str


def nearest_candidates(candidates: Iterable[Candidate], prices: Mapping[str, SignalInput], *,
                       held_symbols: Iterable[str] = (), desired_symbols: Iterable[str] = (),
                       limit: int = 3, place_gap: float = cfg.SIGNAL_DISCOVERY_GAP) -> tuple[CandidateDistance, ...]:
    """Fresh, non-held candidates by ascending signed gap, below-limit first.

    Below-limit prices are eligible for a buy intent, never proof of a fill.
    Using the exact selection inputs prevents stale quotes from appearing
    as inexplicably close candidates. O(n log 3), only on a price evaluation.
    """
    if limit <= 0:
        return ()
    held = {str(s).upper().strip() for s in held_symbols}
    desired = {str(s).upper().strip() for s in desired_symbols}
    rows = []
    for candidate in candidates:
        quote = prices.get(candidate.symbol)
        if candidate.symbol in held or quote is None:
            continue
        if (not all(math.isfinite(v) for v in (quote.observed_price, candidate.limit_price, quote.gap))
                or quote.observed_price <= 0 or candidate.limit_price <= 0):
            continue
        if candidate.symbol in desired:
            status = ('below_limit_desired' if quote.gap < 0 else
                      'desired' if quote.gap <= place_gap else 'kept')
        else:
            status = 'outside_place_gap' if quote.gap > place_gap else 'eligible_not_published'
        rows.append(CandidateDistance(candidate.symbol, quote.observed_price,
                                      candidate.limit_price, quote.gap, status))
    return tuple(nsmallest(limit, rows, key=lambda r: (r.gap, r.symbol)))


def nearest_candidates_line(rows: Iterable[CandidateDistance], *, candidate_count: int,
                            price_healthy: bool, place_gap: float = cfg.SIGNAL_DISCOVERY_GAP) -> str:
    """Stable display/log block: header, then one candidate per line."""
    prefix = f'最近3候选(按价差从小到大，低于限价优先；信号候选高出≤{100 * place_gap:.4f}%（含低于限价）):\n  '
    if not candidate_count:
        return prefix + '无有效候选'
    if not price_healthy:
        return prefix + '价格快照不可用或过期，无法比较'
    labels = {'below_limit_desired': '低于限价，应挂单', 'desired': '应挂单', 'kept': '保留已有挂单',
              'outside_place_gap': '未进挂单范围', 'eligible_not_published': '已进范围但未发布'}
    parts = []
    for row in rows:
        distance = ('高出' if row.gap >= 0 else '低于') + f'={abs(row.gap) * 100:.4f}%'
        parts.append(f'{row.symbol} 现价={row.observed_price:.10g} 挂单价={row.limit_price:.10g} '
                     f'{distance} [{labels[row.status]}]')
    return prefix + ('\n  '.join(parts) if parts else '无可比较的未持仓候选（已持仓/缺价/报价过期）')
