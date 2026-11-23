"""Process-local entry latency evidence; workers never mutate execution ledgers."""
from __future__ import annotations

import threading
import time
import math
from collections import deque


class EntryTimings:
    def __init__(self):
        self._lock = threading.RLock()
        self._rows = {}
        self._plans = {}
        self._recent = deque(maxlen=512)
        self.revision = 0

    def register(self, symbol, cid, row, *, session_id=''):
        context = dict(row.get('_entry_timing_context') or {})
        limit = row.get('submitted_limit_price', row.get('limit_price'))
        def gap(price):
            try:
                price, limit_value = float(price), float(limit)
                if price > 0 and limit_value > 0 and math.isfinite(price) and math.isfinite(limit_value):
                    return (price-limit_value)/limit_value
            except (TypeError, ValueError, OverflowError):
                pass
            return None
        context.update(order_limit_price=limit, signal_limit_price=row.get('limit_price'),
            admission_gap=gap(context.get('admission_price')),
            trigger_gap=gap(context.get('trigger_price')) if context.get('trigger_symbol') == symbol else None)
        with self._lock:
            if cid in self._rows:
                return
            self._rows[cid] = dict(context, symbol=symbol, client_order_id=cid,
                signal_session_id=str(session_id or ''), position_id=str(row.get('position_id') or ''),
                order_bar_open_ms=int(row.get('order_bar_open_ms') or 0))
            key = (symbol, int(row.get('order_bar_open_ms') or 0), str(session_id or ''))
            self._plans.setdefault(key, []).append(cid)
            self._recent.append(cid)
            self.revision += 1

    def mark(self, cid, stage, *, transport=None):
        self.mark_at(cid, stage, time.monotonic_ns(), time.time()*1000, transport=transport)

    def mark_at(self, cid, stage, stamp_ns, epoch_ms, *, transport=None):
        with self._lock:
            row = self._rows.get(cid)
            if row is None or stage+'_epoch_ms' in row:
                return
            row[stage+'_epoch_ms'] = epoch_ms
            row['_'+stage+'_ns'] = stamp_ns
            if transport is not None:
                row['transport'] = transport
            self.revision += 1

    def outcome(self, cid, **fields):
        """Thread-safe diagnostics; status/fill updates remain owner-only."""
        with self._lock:
            row = self._rows.get(cid)
            if row is None:
                return
            if 'executed_qty' in fields:
                if float(fields['executed_qty'] or 0) < float(row.get('executed_qty') or 0):
                    fields.pop('status', None)  # A delayed ACK must not rewind newer fill evidence.
                fields['executed_qty'] = max(float(row.get('executed_qty') or 0), float(fields['executed_qty'] or 0))
            if all(row.get(key) == value for key, value in fields.items()):
                return
            row.update(fields, outcome_updated_epoch_ms=time.time()*1000)
            self.revision += 1

    def quote_check(self, cid, detail):
        """Worker-only quote audit; no status, fills or execution-ledger writes."""
        fields = {key: detail[key] for key in ('entry_send_check', 'entry_recheck_gap', 'entry_recheck_threshold')
                  if key in detail}
        if detail.get('entry_quote_policy') == 'distance_rechecked':
            fields.update(entry_recheck_tick_gap=detail.get('entry_tick_gap'),
                          entry_recheck_depth_gap=detail.get('entry_depth_gap'))
        self.outcome(cid, **fields)

    @staticmethod
    def _public(row):
        result = {k: v for k, v in row.items() if not k.startswith('_')}
        edges = (('quote_to_decision', 'received', 'decision'),
                 ('queue_wait', 'event_received', 'dequeued'),
                 ('dequeued_to_decision', 'dequeued', 'decision'),
                 ('trigger_to_send', 'event_received', 'send'),
                 ('quote_to_send', 'received', 'send'),
                 ('decision_to_registered', 'decision', 'registered'),
                 ('registered_to_worker', 'registered', 'worker'),
                 ('worker_to_durable', 'worker', 'durable'),
                 ('registered_to_persisted', 'registered', 'persisted'),
                 ('registered_to_write_started', 'registered', 'write_started'),
                 ('write_started_to_persisted', 'write_started', 'persisted'),
                 ('journal_prepare', 'write_started', 'prepared'),
                 ('journal_write', 'prepared', 'written'),
                 ('journal_sync', 'written', 'synced'),
                 ('journal_receipt', 'synced', 'persisted'),
                 ('persisted_to_durable', 'persisted', 'durable'),
                 ('durable_to_send', 'durable', 'send'),
                 ('worker_to_send', 'worker', 'send'),
                 ('send_to_response', 'send', 'response'),
                 ('response_to_owner', 'response', 'owner_ack'))
        result['latencies_ms'] = {name: round((row['_'+end+'_ns']-row['_'+start+'_ns'])/1e6, 3)
            for name, start, end in edges if '_'+start+'_ns' in row and '_'+end+'_ns' in row
            and row['_'+end+'_ns'] >= row['_'+start+'_ns']}
        return result

    def order(self, cid):
        with self._lock:
            row = self._rows.get(cid)
            return self._public(row) if row else {}

    def first_for(self, entry):
        with self._lock:
            matches = self._matches(entry)
            sent = [row for row in matches if '_send_ns' in row]
            if sent:
                return self._public(min(sent, key=lambda row: row['_send_ns']))
            return self._public(matches[0]) if matches else {}

    def _matches(self, entry):
        key = (entry['symbol'], entry['entry_bar_open_ms'], entry['signal_session_id'])
        return [self._rows[cid] for cid in self._plans.get(key, ())
                if (not self._rows[cid]['position_id'] or not entry['position_id']
                    or self._rows[cid]['position_id'] == entry['position_id'])]

    def attempts_for(self, entry):
        with self._lock:
            matches = self._matches(entry)
            return dict(registered=len(matches), sent=sum('_send_ns' in row for row in matches),
                        latest=self._public(matches[-1]) if matches else {})

    def summary(self):
        fields = ('received', 'event_received', 'dequeued', 'decision', 'registered', 'worker',
                  'write_started', 'prepared', 'written', 'synced', 'persisted', 'durable', 'send')
        keys = tuple('_'+name+'_ns' for name in fields)
        with self._lock:
            # Copy only bounded timestamp fields, then sort outside the lock.
            # Aggregation runs during feedback, never inside quote selection.
            rows = [tuple(r.get(key) for key in keys)
                    for r in (self._rows[cid] for cid in self._recent)]
        def summarize(start, end='send'):
            first, last = fields.index(start), fields.index(end)
            values = sorted((r[last]-r[first])/1e6 for r in rows
                            if r[-1] is not None and r[first] is not None
                            and r[last] is not None and r[last] >= r[first])
            return dict(samples=len(values), within_10ms=sum(v <= 10 for v in values),
                p50_ms=round(values[int((len(values)-1)*.5)], 3) if values else None,
                p95_ms=round(values[int((len(values)-1)*.95)], 3) if values else None,
                p99_ms=round(values[int((len(values)-1)*.99)], 3) if values else None,
                max_ms=round(max(values), 3) if values else None)
        result = summarize('received')
        result['trigger'] = summarize('event_received')
        result['stages'] = {name: summarize(start, end) for name, start, end in (
            ('queue_wait', 'event_received', 'dequeued'),
            ('dequeued_to_decision', 'dequeued', 'decision'),
            ('decision_to_registered', 'decision', 'registered'),
            ('registered_to_worker', 'registered', 'worker'),
            ('worker_to_durable', 'worker', 'durable'),
            ('registered_to_persisted', 'registered', 'persisted'),
            ('registered_to_write_started', 'registered', 'write_started'),
            ('write_started_to_persisted', 'write_started', 'persisted'),
            ('journal_prepare', 'write_started', 'prepared'),
            ('journal_write', 'prepared', 'written'),
            ('journal_sync', 'written', 'synced'),
            ('journal_receipt', 'synced', 'persisted'),
            ('persisted_to_durable', 'persisted', 'durable'),
            ('durable_to_send', 'durable', 'send'),
            ('worker_to_send', 'worker', 'send'))}
        return result


class EntrySendProbe:
    """Callable guard plus hooks invoked at the actual checked WS transport."""
    def __init__(self, guard, timings, cid):
        self.guard, self.timings, self.cid = guard, timings, cid

    def __call__(self):
        return self.guard()

    def wire_sent(self):
        if self.timings is not None:
            self.timings.mark(self.cid, 'send', transport='WS')

    def reply_received(self):
        if self.timings is not None:
            self.timings.mark(self.cid, 'response')
