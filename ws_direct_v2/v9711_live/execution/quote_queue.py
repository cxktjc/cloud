"""Bounded quote queue with per-symbol ordering and an admission fast lane.

Actionable transitions are retained. Unrelated unowned far prices can finish
their stateless comparison on ingress. A symbol's queued/in-flight updates
always run before its crossing. Consumers retain all final admission guards.
"""
from collections import deque
import heapq
import itertools
import queue


class QuoteQueue(queue.Queue):
    def _init(self, maxsize):
        self.queue = {}
        self._fifo = []
        self._urgent = []
        self._crossings = []
        self._sequence = itertools.count()
        self._length = 0
        self.peak = 0
        self._templates = None
        self._thresholds = {}
        self._limits = {}
        self._watched = frozenset()
        self._enabled = False
        self._urgent_run = 0
        self._crossing_run = 0
        self.priority_taken = self.background_taken = 0
        self._screen_ingress = False
        self._inflight = {}
        self._screened = {}
        self._rescreen = {}
        self.replayed = {'tick': 0, 'bid0': 0}
        self.refresh_quotes = {}
        self.compacted_far = 0
        self._bar_peaks = {}
        self._bar = None

    def enable_ingress_screening(self):
        with self.mutex:
            self._screen_ingress = True

    def put_quote(self, item, complete_far, *, price=None):
        """Stateless comparison, atomic with policy and symbol ownership.

        Both sources share a symbol bucket. Pending/in-flight updates retain
        order; no crossing, rebound or owned-order update is filtered here.
        """
        with self.not_full:
            priority = self._priority(item, price=price)
            if (self._screen_ingress and self._enabled and self._thresholds and item.valid
                    and item.symbol not in self._watched
                    and not priority):
                bucket = self.queue.get(item.symbol)
                if bucket is None and item.symbol not in self._inflight and complete_far(item):
                    key = item.symbol, item.source
                    self._screened[key] = item
                    self._rescreen.pop(key, None)
                    return False
                if bucket is not None:
                    seq, old, near = bucket[0][-1]
                    if (not near and old.valid and old.source == item.source
                            and old.generation == item.generation):
                        # Only shrink an adjacent same-source far run. Keep every
                        # crossing, rebound boundary, invalidation and owned update.
                        bucket[0][-1] = (seq, item, False)
                        self.compacted_far += 1
                        self._observe_bar_peak(item)
                        return False
            if self.maxsize > 0 and self._qsize() >= self.maxsize:
                raise queue.Full
            self._put(item, priority=priority)
            self.unfinished_tasks += 1
            self.not_empty.notify()
            return True

    def finish(self, item):
        if item is None:
            return
        with self.mutex:
            count = self._inflight.get(item.symbol, 0)
            if count <= 1:
                self._inflight.pop(item.symbol, None)
            else:
                self._inflight[item.symbol] = count - 1
            if self._rescreen:
                self._replay_screened()

    def _replay_screened(self):
        # A template/watch change can make completed far quotes relevant.
        # Retain deferred replay when full; bounded by two quotes per symbol.
        for key, item in tuple(self._rescreen.items()):
            if item.symbol in self.queue or item.symbol in self._inflight:
                continue
            if self.maxsize > 0 and self._qsize() >= self.maxsize:
                break
            refresh = self.refresh_quotes.get(item.source)
            if refresh is not None:
                item = refresh(item.symbol, item)
            self._put(item)
            self.unfinished_tasks += 1
            self.replayed[item.source] += 1
            self.not_empty.notify()

    def _qsize(self):
        return self._length

    def _priority(self, item, *, price=None):
        if not self._enabled:
            return 0
        if item is None:
            return 1  # Exact-account/network completions must reach owner.
        threshold = self._thresholds.get(item.symbol)
        if threshold is None or not item.valid:
            return 0
        try:
            if price is None:
                price = float(item.price)
            return (2 if price <= self._limits[item.symbol] else 1) if 0 < price <= threshold else 0
        except (ValueError, TypeError):
            return 0

    def _near(self, item):
        return bool(self._priority(item))

    def _ticket(self, key, bucket):
        head = bucket[0][0][0]
        if key in self._watched:
            return head, head, key
        if bucket[1]:
            return bucket[1][0], head, key
        return None

    def _push_head(self, key):
        bucket = self.queue[key]
        heapq.heappush(self._fifo, (bucket[0][0][0], key))
        ticket = self._ticket(key, bucket)
        if ticket is not None:
            heapq.heappush(self._urgent, ticket)
        if bucket[2]:
            heapq.heappush(self._crossings, (bucket[2][0], bucket[0][0][0], key))

    def _compact(self):
        if max(len(self._fifo), len(self._urgent), len(self._crossings)) <= max(64, 4*len(self.queue)):
            return
        self._fifo, self._urgent, self._crossings = [], [], []
        for key in self.queue:
            self._push_head(key)

    def _put(self, item, *, priority=None):
        key = item.symbol if item is not None else None
        if item is not None:
            identity = item.symbol, item.source
            self._screened.pop(identity, None)
            self._rescreen.pop(identity, None)
        bucket = self.queue.get(key)
        if bucket is None:
            bucket = self.queue[key] = (deque(), deque(), deque())
        seq = next(self._sequence)
        if priority is None:
            priority = self._priority(item)
        bucket[0].append((seq, item, priority))
        if priority:
            bucket[1].append(seq)
        if priority == 2:
            bucket[2].append(seq)
        self._length += 1
        self.peak = max(self.peak, self._length)
        self._observe_bar_peak(item)
        if len(bucket[0]) == 1:
            self._push_head(key)
        else:
            if priority and len(bucket[1]) == 1 and key not in self._watched:
                heapq.heappush(self._urgent, self._ticket(key, bucket))
            if priority == 2 and len(bucket[2]) == 1:
                heapq.heappush(self._crossings, (seq, bucket[0][0][0], key))
        self._compact()

    def _observe_bar_peak(self, item):
        stamp = getattr(item, 'received_epoch_ms', 0)
        if stamp:
            bar = int(stamp) // 180000 * 180
            if bar != self._bar:
                if self._bar is None or bar > self._bar:
                    self._bar = bar
                    self._bar_peaks = {k: v for k, v in self._bar_peaks.items() if k >= bar-59*180}
                elif bar < self._bar-59*180:
                    return
            if self._length > self._bar_peaks.get(bar, 0):
                self._bar_peaks[bar] = self._length

    def bar_peaks(self):
        with self.mutex:
            return dict(self._bar_peaks)

    def _urgent_key(self):
        while self._urgent:
            ticket = self._urgent[0]
            bucket = self.queue.get(ticket[2])
            if bucket is not None and self._ticket(ticket[2], bucket) == ticket:
                return ticket[2]
            heapq.heappop(self._urgent)
        return None,  # Distinct from the real None/wake bucket.

    def _fifo_key(self):
        while self._fifo:
            seq, key = self._fifo[0]
            bucket = self.queue.get(key)
            if bucket is not None and bucket[0][0][0] == seq:
                return key
            heapq.heappop(self._fifo)
        raise RuntimeError('quote queue head missing')

    def _next_priority_key(self):
        if not self._enabled:
            return (None,), False
        if self._screen_ingress and None in self.queue:
            return None, False  # Account and exact wire completion wakes.
        if self._urgent_run >= 16:
            return (None,), False
        if self._screen_ingress and self._crossing_run < 8:
            while self._crossings:
                seq, head, key = self._crossings[0]
                bucket = self.queue.get(key)
                if bucket is not None and bucket[2] and (bucket[2][0], bucket[0][0][0]) == (seq, head):
                    return key, True
                heapq.heappop(self._crossings)
        return self._urgent_key(), False

    def _get(self):
        key, crossing = self._next_priority_key()
        self._crossing_run = self._crossing_run + 1 if crossing else 0
        if key == (None,):
            key = self._fifo_key()
            self._urgent_run = 0
            self.background_taken += 1
        else:
            self._urgent_run += 1
            self.priority_taken += 1
        bucket = self.queue[key]
        seq, item, priority = bucket[0].popleft()
        if item is not None:
            self._inflight[item.symbol] = self._inflight.get(item.symbol, 0) + 1
        if priority:
            bucket[1].popleft()
        if priority == 2:
            bucket[2].popleft()
        self._length -= 1
        if bucket[0]:
            self._push_head(key)
        else:
            del self.queue[key]
        self._compact()
        return item

    def set_policy(self, templates, watched):
        """Owner publishes immutable limits and the current protected symbols."""
        watched = frozenset(watched)
        with self.mutex:
            changed_limits = templates is not self._templates
            changed_watch = watched ^ self._watched
            if not changed_limits and not changed_watch and self._enabled:
                return
            self._enabled = True
            self._templates, self._watched = templates, watched
            if changed_limits:
                from .. import config as cfg
                self._limits = {s: float(row['limit_price'])
                    for s, row in templates.items() if float(row.get('limit_price') or 0) > 0}
                self._thresholds = {s: p*(1+cfg.CANCEL_GAP+1e-12) for s, p in self._limits.items()}
                for key, bucket in self.queue.items():
                    rows = deque((seq, item, self._priority(item)) for seq, item, _ in bucket[0])
                    self.queue[key] = (rows, deque(seq for seq, _, p in rows if p),
                                      deque(seq for seq, _, p in rows if p == 2))
                self._fifo, self._urgent, self._crossings = [], [], []
                for key in self.queue:
                    self._push_head(key)
            else:
                for key in changed_watch:
                    bucket = self.queue.get(key)
                    if bucket is not None and (ticket := self._ticket(key, bucket)) is not None:
                        heapq.heappush(self._urgent, ticket)
                self._compact()

            if self._screen_ingress:
                if changed_limits:
                    self._rescreen = {key: item for key, item in self._screened.items()
                        if self._near(item) or item.symbol in self._watched}
                else:
                    for symbol in changed_watch & watched:
                        for key in ((symbol, 'tick'), (symbol, 'bid0')):
                            if key in self._screened:
                                self._rescreen[key] = self._screened[key]
                self._replay_screened()

    def watch(self, symbol, active):
        # One owner publishes/changes the watch set; ingress only reads it.
        # Avoid contending for the queue mutex on every unchanged membership.
        if (symbol in self._watched) == active:
            return
        with self.mutex:
            if (symbol in self._watched) == active:
                return
            self._watched = (self._watched | {symbol}) if active else (self._watched - {symbol})
            bucket = self.queue.get(symbol)
            if bucket is not None and (ticket := self._ticket(symbol, bucket)) is not None:
                heapq.heappush(self._urgent, ticket)
            self._compact()

            if self._screen_ingress and active:
                for key in ((symbol, 'tick'), (symbol, 'bid0')):
                    if key in self._screened:
                        self._rescreen[key] = self._screened[key]
                self._replay_screened()

    def take_distinct_candidate(self, excluded):
        """Take only the next normal queue head, if it is a new near symbol.

        Never scan past a wake, a protected update, a far predecessor or a
        repeated symbol. A later A->B->A update stays for its ordinary turn.
        The owner must still validate generation/age and call task_done().
        """
        with self.not_empty:
            if not self._length or not self._enabled:
                return None
            key, _ = self._next_priority_key()
            if key == (None,):
                key = self._fifo_key()
            bucket = self.queue[key]
            _, item, near = bucket[0][0]
            if item is None or key in excluded or not near:
                return None
            item = self._get()
            self.not_full.notify()
            return item
