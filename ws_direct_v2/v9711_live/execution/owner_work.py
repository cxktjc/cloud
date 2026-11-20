"""Bounded owner-work evidence; no I/O, account data or worker mutation."""
from collections import deque
import time


class OwnerWork:
    def __init__(self):
        self.samples = {'quote': deque(maxlen=512), 'maintenance': deque(maxlen=64)}
        self.counts = {'quote': 0, 'maintenance': 0}
        self.slow = deque(maxlen=5)

    def finish(self, kind, started_ns, segments, *, route=''):
        elapsed = (time.perf_counter_ns()-started_ns)/1e6
        self.samples[kind].append(elapsed)
        self.counts[kind] += 1
        if elapsed >= 50:
            stage = max(segments, key=segments.get) if segments else 'unknown'
            self.slow.append(dict(epoch=time.time(), kind=kind, route=route, elapsed_ms=round(elapsed, 3),
                                  stage=stage, stage_ms=round(segments.get(stage, elapsed), 3)))

    def snapshot(self):
        result = {}
        for kind, samples in self.samples.items():
            values = sorted(samples)
            result[kind] = dict(count=self.counts[kind], samples=len(values),
                p95_ms=round(values[int((len(values)-1)*.95)], 3) if values else 0.,
                max_ms=round(max(values, default=0.), 3))
        result['recent_slow'] = list(self.slow)
        return result
