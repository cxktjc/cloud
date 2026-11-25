"""Bounded process-local wire activity; no credentials, payloads or per-tick work."""
from collections import Counter
import threading
import time


class RequestActivity:
    def __init__(self):
        self._lock = threading.Lock()
        self._buckets = {}
        self._inflight = self._peak_inflight = 0
        self._totals = Counter()
        self._bars = {}
        self._bar_mutation_second = None
        self._bar_mutations = 0

    def _bucket(self, now):
        second = int(now)
        if second not in self._buckets:
            self._buckets[second] = dict(counts=Counter(), symbols=Counter(), errors=Counter(), lag_ms=0.)
            for key in tuple(self._buckets):
                if key < second-60:
                    del self._buckets[key]
        return self._buckets[second]

    def begin(self, kind, symbol='', *, transport='WS', members=1, symbols=()):
        now = time.monotonic()
        with self._lock:
            row = self._bucket(now)
            for counts in (row['counts'], self._totals):
                counts[kind] += members
                counts['wire_requests'] += 1
                counts[transport] += 1
            if kind in ('entry', 'cancel', 'close'):
                epoch = time.time()
                bar = self._bar(epoch)
                bar[kind] += members
                second = int(epoch)
                if second != self._bar_mutation_second:
                    self._bar_mutation_second, self._bar_mutations = second, 0
                self._bar_mutations += 1
                bar['mutation_requests_peak_second'] = max(bar['mutation_requests_peak_second'], self._bar_mutations)
            if symbol:
                row['symbols'][symbol] += members
            for member_symbol in symbols:
                row['symbols'][member_symbol] += 1
            self._inflight += 1
            self._peak_inflight = max(self._peak_inflight, self._inflight)
        return now

    def _bar(self, epoch):
        start = int(epoch) // 180 * 180
        if start not in self._bars:
            self._bars[start] = Counter()
            for old in tuple(self._bars):
                if old < start-59*180:
                    del self._bars[old]
        return self._bars[start]

    def market_pressure(self, kind, *, age_ms=None, now=None):
        with self._lock:
            row = self._bar(time.time() if now is None else now)
            row[kind] += 1
            if age_ms is not None:
                row['queue_delay_max_ms'] = max(row['queue_delay_max_ms'], round(age_ms, 3))

    def entry_outcome(self, category, reason=None):
        with self._lock:
            row = self._bar(time.time())
            row[category] += 1
            if category == 'local_not_sent' and reason:
                row['not_sent_'+reason] += 1

    def _bar_stats(self, now, queue_peaks, market_load):
        start = int(now) // 180 * 180
        rows = []
        for bar in sorted(set(self._bars) | set(queue_peaks) | set(market_load) | {start, start-180}):
            if not start-59*180 <= bar <= start:
                continue
            row = dict(self._bars.get(bar, {}), **market_load.get(bar, {}),
                       utc_start=bar, queue_peak=queue_peaks.get(bar, 0))
            row['entry_cancel_total'] = row.get('entry', 0)+row.get('cancel', 0)
            rows.append(row)
        peaks = {key: max(rows, key=lambda row: (row.get(key, 0), row['utc_start']))
                 for key in ('entry', 'cancel', 'entry_cancel_total', 'queue_peak', 'queue_delay_max_ms', 'overload_discarded')}
        return dict(current=rows[-1], previous=rows[-2], peaks=peaks, recent=rows,
                    retained_bar_count=60, process_local=True)

    def finish(self, started, *, error=None, code=None):
        now = time.monotonic()
        with self._lock:
            row = self._bucket(now)
            self._inflight -= 1
            name = 'failed' if error is not None or code is not None else 'replied'
            row['counts'][name] += 1
            self._totals[name] += 1
            if name == 'failed':
                # Codes/types only: never store request params or arbitrary exception text.
                key = 'API:'+str(code) if code is not None else type(error).__name__
                if len(row['errors']) < 12 or key in row['errors']:
                    row['errors'][key] += 1
            if str(code) in ('-1003', '-1015', '429', '418'):
                row['counts']['rate_limited'] += 1
                self._totals['rate_limited'] += 1
                self._bar(time.time())['rate_limited'] += 1

    def local_reject(self):
        with self._lock:
            self._bucket(time.monotonic())['counts']['local_rejected'] += 1
            self._totals['local_rejected'] += 1

    def member_reject(self, code):
        with self._lock:
            row = self._bucket(time.monotonic())
            row['counts']['member_rejected'] += 1
            self._totals['member_rejected'] += 1
            key = 'API:'+str(code)
            if len(row['errors']) < 12 or key in row['errors']:
                row['errors'][key] += 1
            if str(code) in ('-1003', '-1015', '429', '418'):
                row['counts']['rate_limited'] += 1
                self._totals['rate_limited'] += 1
                self._bar(time.time())['rate_limited'] += 1

    def observe_loop_lag(self, seconds):
        with self._lock:
            row = self._bucket(time.monotonic())
            row['lag_ms'] = max(row['lag_ms'], max(0., seconds)*1000)

    def snapshot(self, *, queue_peaks=None, market_load=None):
        now = time.monotonic()
        with self._lock:
            windows = {}
            for length in (1, 10, 60):
                counts, symbols, errors, lag = Counter(), Counter(), Counter(), 0.
                peak = 0
                for second, row in self._buckets.items():
                    if int(now)-length < second <= int(now):
                        counts.update(row['counts'])
                        symbols.update(row['symbols'])
                        errors.update(row['errors'])
                        peak = max(peak, row['counts']['wire_requests'])
                        lag = max(lag, row['lag_ms'])
                windows[str(length)] = dict(counts, peak_requests_per_second=peak,
                    loop_lag_max_ms=round(lag, 3), top_symbols=dict(symbols.most_common(3)),
                    errors=dict(errors.most_common(5)))
            return dict(windows=windows, totals=dict(self._totals), inflight=self._inflight,
                        peak_inflight=self._peak_inflight, window_clock='monotonic_second_buckets',
                        bars_3m=self._bar_stats(time.time(), queue_peaks or {}, market_load or {}))


def request_kind(method, params):
    if method in ('order.place', 'algoOrder.place'):
        return 'close' if str(params.get('side')).upper() == 'SELL' else 'entry'
    if 'cancel' in method.lower():
        return 'cancel'
    return 'query'
