"""Process-local entry latency evidence; workers never mutate execution ledgers."""
from __future__ import annotations

import threading
import time


class EntryTimings:
    def __init__(self):
        self._lock = threading.RLock()
        self._rows = {}
        self._plans = {}
        self.revision = 0

    def register(self, symbol, cid, row, *, session_id=''):
        context = dict(row.get('_entry_timing_context') or {})
        with self._lock:
            if cid in self._rows:
                return
            self._rows[cid] = dict(context, symbol=symbol, client_order_id=cid,
                signal_session_id=str(session_id or ''), position_id=str(row.get('position_id') or ''),
                order_bar_open_ms=int(row.get('order_bar_open_ms') or 0))
            key = (symbol, int(row.get('order_bar_open_ms') or 0), str(session_id or ''))
            self._plans.setdefault(key, []).append(cid)
            self.revision += 1

    def mark(self, cid, stage, *, transport=None):
        with self._lock:
            row = self._rows.get(cid)
            if row is None or stage+'_epoch_ms' in row:
                return
            row[stage+'_epoch_ms'] = time.time()*1000
            row['_'+stage+'_ns'] = time.monotonic_ns()
            if transport is not None:
                row['transport'] = transport
            self.revision += 1

    @staticmethod
    def _public(row):
        result = {k: v for k, v in row.items() if not k.startswith('_')}
        edges = (('quote_to_decision', 'received', 'decision'),
                 ('decision_to_registered', 'decision', 'registered'),
                 ('registered_to_worker', 'registered', 'worker'),
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
            key = (entry['symbol'], entry['entry_bar_open_ms'], entry['signal_session_id'])
            matches = [self._rows[cid] for cid in self._plans.get(key, ())
                if (not self._rows[cid]['position_id'] or not entry['position_id']
                     or self._rows[cid]['position_id'] == entry['position_id'])]
            sent = [row for row in matches if '_send_ns' in row]
            if sent:
                return self._public(min(sent, key=lambda row: row['_send_ns']))
            return self._public(matches[0]) if matches else {}


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
