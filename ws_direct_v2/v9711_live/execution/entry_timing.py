"""Process-local entry latency evidence; workers never mutate execution ledgers."""
from __future__ import annotations

import threading
import time
import math
from collections import OrderedDict, deque

_ERROR_FIELDS = ('symbol', 'client_order_id', 'signal_follow', 'submit_error_category',
                 'submit_error_reason', 'submit_error_code', 'submit_error', 'submit_error_epoch_ms')


class EntryTimings:
    def __init__(self):
        self._lock = threading.RLock()
        self._rows = OrderedDict()
        self._plans = {}
        self._recent = deque(maxlen=512)
        self._errors = OrderedDict()
        self._skips = OrderedDict()
        self._summary_cache = None
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
            quote_symbol=symbol, signal_follow=bool(row.get('signal_follow')),
            admission_gap=gap(context.get('admission_price')),
            trigger_gap=gap(context.get('trigger_price')) if context.get('trigger_symbol') == symbol else None)
        with self._lock:
            if cid in self._rows:
                return
            self._rows[cid] = dict(context, symbol=symbol, client_order_id=cid,
                signal_session_id=str(session_id or ''), position_id=str(row.get('position_id') or ''),
                order_bar_open_ms=int(row.get('order_bar_open_ms') or 0))
            key = (symbol, int(row.get('order_bar_open_ms') or 0), str(session_id or ''))
            self._plans.setdefault(key, deque()).append(cid)
            self._recent.append(cid)
            # The 512-sample panel is not the eviction queue for the 2048-row index.
            if len(self._rows) > 2048:
                _, evicted = self._rows.popitem(last=False)
                plan = (evicted['symbol'], evicted['order_bar_open_ms'], evicted['signal_session_id'])
                bucket = self._plans[plan]
                bucket.popleft()  # global oldest is also the oldest of this plan
                if not bucket:
                    del self._plans[plan]
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
            if fields.get('status') == 'REJECTED' and not fields.get('submit_error_category'):
                fields = dict(fields, submit_error_category='exchange_rejected', submit_error='订单状态 REJECTED')
            if fields.get('submit_error_category'):
                # Replace older error evidence, including optional fields such as code.
                for key in ('submit_error_code', 'submit_error_reason'):
                    row.pop(key, None)
                row.update({k: v for k, v in fields.items() if k.startswith('submit_error')})
                row['submit_error_epoch_ms'] = row['outcome_updated_epoch_ms']
                self._errors[cid] = {k: row[k] for k in _ERROR_FIELDS if k in row}
                self._errors.move_to_end(cid)
                if len(self._errors) > 64:
                    self._errors.popitem(last=False)
            self.revision += 1

    def quote_check(self, cid, detail):
        """Worker-only quote audit; no status, fills or execution-ledger writes."""
        fields = {key: detail[key] for key in ('entry_send_check', 'entry_recheck_gap', 'entry_recheck_threshold',
                                                'entry_check_signal_limit')
                  if key in detail}
        if detail.get('entry_quote_policy') == 'distance_rechecked':
            fields.update(entry_recheck_tick_gap=detail.get('entry_tick_gap'),
                          entry_recheck_depth_gap=detail.get('entry_depth_gap'))
        self.outcome(cid, **fields)

    @staticmethod
    def _public(row):
        result = {k: v for k, v in row.items() if not k.startswith('_')}
        edges = (('quote_to_decision', 'received', 'decision'),
                 ('quote_age_at_wake', 'received', 'event_received'),
                 ('queue_wait', 'event_received', 'dequeued'),
                 ('dequeued_to_decision', 'dequeued', 'decision'),
                 ('trigger_to_send', 'event_received', 'send'),
                 ('quote_to_send', 'received', 'send'),
                 ('decision_to_send', 'decision', 'send'),
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
                 ('durable_to_socket', 'durable', 'socket_entered'),
                 ('socket_wait', 'socket_entered', 'socket_ready'),
                 ('send_guard', 'socket_ready', 'checked'),
                 ('sign_encode', 'checked', 'encoded'),
                 ('encoded_to_send', 'encoded', 'send'),
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

    def received_monotonic_ns(self, cid):
        with self._lock:
            return (self._rows.get(cid) or {}).get('_received_ns')

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
            key = entry['symbol'], entry['entry_bar_open_ms'], entry['signal_session_id']
            latest_error = max((row for row in matches if row.get('submit_error_category')),
                               key=lambda row: row.get('submit_error_epoch_ms', 0), default=None)
            def by_kind(follow):
                rows = [row for row in matches if row['signal_follow'] == follow]
                sent = [row for row in rows if '_send_ns' in row]
                first_sent = min(sent, key=lambda row: row['_send_ns'], default={})
                error = max((row for row in rows if row.get('submit_error_category')),
                            key=lambda row: row.get('submit_error_epoch_ms', 0), default={})
                latest = rows[-1] if rows else {}
                return dict(registered=len(rows), sent=len(sent),
                            first_registration_ms=rows[0].get('registered_epoch_ms') if rows else None,
                            first_send_ms=first_sent.get('send_epoch_ms'),
                            latest_status=latest.get('status'), latest_id=latest.get('client_order_id'),
                            latest_end_reason=latest.get('cancel_reason') or latest.get('retired_reason'),
                            latest_executed_qty=latest.get('executed_qty'),
                            last_error={k: error[k] for k in _ERROR_FIELDS if k in error})
            return dict(registered=len(matches), sent=sum('_send_ns' in row for row in matches),
                        last_skip=self._skips.get((*key, entry['position_id'])) or self._skips.get((*key, '')),
                        latest=self._public(matches[-1]) if matches else {},
                        last_error={k: latest_error[k] for k in _ERROR_FIELDS if k in latest_error}
                        if latest_error else {},
                        ordinary=by_kind(False), follow=by_kind(True))

    def recent_errors(self, limit=6):
        if limit <= 0:
            return []
        with self._lock:
            errors = []
            for cid in reversed(self._errors):
                error = self._errors[cid]
                row = self._rows[cid]
                errors.append(dict(error, current_status=row.get('status'),
                                   executed_qty=row.get('executed_qty')))
                if len(errors) >= limit:
                    break
            return errors

    def skipped(self, symbol, row, session, reason):
        try:
            bar = int(row.get('order_bar_open_ms') or 0)
        except (ValueError, TypeError, OverflowError):
            return
        key = symbol, bar, session, str(row.get('position_id') or '')
        if not all(key[:3]):
            return
        with self._lock:
            if self._skips.get(key) == reason:
                return
            self._skips[key] = reason
            self._skips.move_to_end(key)
            if len(self._skips) > 8192:
                self._skips.popitem(last=False)
            self.revision += 1

    def summary(self):
        fields = ('received', 'event_received', 'dequeued', 'decision', 'registered', 'worker',
                  'write_started', 'prepared', 'written', 'synced', 'persisted', 'durable',
                  'socket_entered', 'socket_ready', 'checked', 'encoded', 'send')
        keys = tuple('_'+name+'_ns' for name in fields)
        with self._lock:
            revision = self.revision
            if self._summary_cache is not None and self._summary_cache[0] == revision:
                return self._summary_cache[1]
            # Copy only bounded timestamp fields, then sort outside the lock.
            # Aggregation runs during feedback, never inside quote selection.
            rows = [(*map(r.get, keys), r['client_order_id'])
                    for r in (self._rows[cid] for cid in self._recent)]
        send = fields.index('send')
        def summarize(start, end='send', *, identify_max=False):
            first, last = fields.index(start), fields.index(end)
            if identify_max:
                ranked = sorted((row[last]-row[first], row[-1]) for row in rows
                                if row[send] is not None and row[first] is not None and row[last] >= row[first])
                values = [span/1e6 for span, _ in ranked]
                worst_cid = ranked[-1][1] if ranked else None
            else:
                values = sorted((row[last]-row[first])/1e6 for row in rows
                                if row[send] is not None and row[first] is not None
                                and row[last] is not None and row[last] >= row[first])
                worst_cid = None
            metrics = dict(samples=len(values), within_10ms=sum(v <= 10 for v in values),
                p50_ms=round(values[int((len(values)-1)*.5)], 3) if values else None,
                p95_ms=round(values[int((len(values)-1)*.95)], 3) if values else None,
                p99_ms=round(values[int((len(values)-1)*.99)], 3) if values else None,
                max_ms=round(values[-1], 3) if values else None)
            if worst_cid is not None:
                detail = self.order(worst_cid)
                metrics['max_order'] = {key: detail[key] for key in (
                    'symbol', 'client_order_id', 'signal_follow', 'trigger_symbol', 'trigger_source',
                    'quote_source', 'received_epoch_ms', 'send_epoch_ms', 'latencies_ms') if key in detail}
            return metrics
        result = summarize('received', identify_max=True)
        result['trigger'] = summarize('event_received', identify_max=True)
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
            ('durable_to_socket', 'durable', 'socket_entered'),
            ('socket_wait', 'socket_entered', 'socket_ready'),
            ('send_guard', 'socket_ready', 'checked'),
            ('sign_encode', 'checked', 'encoded'),
            ('encoded_to_send', 'encoded', 'send'),
            ('worker_to_send', 'worker', 'send'))}
        with self._lock:
            if self.revision == revision:
                self._summary_cache = (revision, result)
        return result


class EntrySendProbe:
    """Callable guard plus hooks invoked at the actual checked WS transport."""
    def __init__(self, guard, timings, cid, activity=None):
        self.guard, self.timings, self.cid, self.activity = guard, timings, cid, activity

    def __call__(self):
        return self.guard()

    def stage(self, name):
        if self.timings is not None:
            self.timings.mark(self.cid, name)

    def wire_sent(self):
        stamp_ns, epoch_ms = time.monotonic_ns(), time.time()*1000
        row = {}
        if self.timings is not None:
            self.timings.mark_at(self.cid, 'send', stamp_ns, epoch_ms, transport='WS')
            row = self.timings.order(self.cid)
            received_ns = self.timings.received_monotonic_ns(self.cid)
        else:
            received_ns = None
        latency_ms = ((stamp_ns-received_ns)/1e6
                      if received_ns is not None and stamp_ns >= received_ns else None)
        if self.activity is not None:
            self.activity.entry_wire_send(row.get('symbol', ''), latency_ms, now=epoch_ms/1000)

    def reply_received(self):
        if self.timings is not None:
            self.timings.mark(self.cid, 'response')
