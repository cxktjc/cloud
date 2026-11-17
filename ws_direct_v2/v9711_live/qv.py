"""Causal calendar-bar QV20 and historical cross-sectional q90 smoothing.

Reconstructed from completed candles on every bar, including after restart.
Missing per-symbol grid cells are zero, as in the reference loader. A whole
missing market bar blocks readiness instead of compressing time across it.
No pandas/numpy runtime dependency is needed.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from . import config as cfg


def quantile(values, q=cfg.QV_QUANTILE):
    values = sorted(v for v in values if math.isfinite(v) and v > 0)
    if not values:
        return float('nan')
    rank = (len(values) - 1) * q
    lower = int(rank)
    upper = min(lower + 1, len(values) - 1)
    return values[lower] + (values[upper] - values[lower]) * (rank - lower)


@dataclass(frozen=True)
class QVSnapshot:
    order_bar_open_ms: int
    threshold: float
    raw_quantile: float
    quantile_count: int
    history_bars: int
    qv20: dict[str, float]
    ready: bool

    def describe(self):
        return dict(policy='cs_q90_trailing24', order_bar_open_ms=self.order_bar_open_ms,
                    threshold=self.threshold if math.isfinite(self.threshold) else None,
                    raw_quantile=self.raw_quantile if math.isfinite(self.raw_quantile) else None,
                    quantile_count=self.quantile_count, required_quantiles=cfg.QV_SMOOTH_BARS,
                    history_bars=self.history_bars, required_history_bars=cfg.QV_REQUIRED_BARS,
                    positive_symbols=sum(math.isfinite(v) and v > 0 for v in self.qv20.values()),
                    ready=self.ready)


class QVHistory:
    def __init__(self, bars, symbols):
        self.symbols = tuple(sorted({str(s).strip().upper() for s in symbols if str(s).strip()}))
        self.rows = {s: {int(b.open_time_ms): b for b in bars.get(s, ())
                         if int(b.open_time_ms) % cfg.BAR_MS == 0} for s in self.symbols}
        self.times = {ms for rows in self.rows.values() for ms in rows}
        self.start = min(self.times, default=0)
        self._features = {}
        self._raw = {}

    def features(self, target):
        if target not in self._features:
            start = max(self.start, target - cfg.QV_WINDOW * cfg.BAR_MS)
            times = range(start, target, cfg.BAR_MS)
            if len(times) < cfg.QV_MIN_PERIODS:
                result = {s: float('nan') for s in self.symbols}
            else:
                result = {}
                for s, rows in self.rows.items():
                    total = 0.0
                    for ms in times:
                        bar = rows.get(ms)
                        value = float(bar.quote_volume) if bar is not None else 0.0
                        total += value if math.isfinite(value) else 0.0
                    result[s] = total / len(times)
            self._features[target] = result
        return self._features[target]

    def snapshot(self, target):
        target = int(target)
        if target % cfg.BAR_MS:
            raise ValueError('QV requires an aligned 3m order bar')
        history = 0
        for i in range(1, cfg.QV_REQUIRED_BARS + 1):
            if target - i * cfg.BAR_MS not in self.times:
                break
            history += 1
        raw = []
        for t in range(target - (cfg.QV_SMOOTH_BARS - 1) * cfg.BAR_MS,
                       target + cfg.BAR_MS, cfg.BAR_MS):
            if t not in self._raw:
                self._raw[t] = quantile(self.features(t).values())
            raw.append(self._raw[t])
        valid = [v for v in raw if math.isfinite(v)]
        # Startup backfills a FULL smoothing window; never silently falls back
        # to a static month threshold or a shortened warmup statistic.
        ready = history == cfg.QV_REQUIRED_BARS and len(valid) == cfg.QV_SMOOTH_BARS
        threshold = math.fsum(valid) / len(valid) if ready else float('nan')
        return QVSnapshot(target, threshold, raw[-1], len(valid), history,
                          self.features(target), ready)
