"""Read-only account worker; owner receives detached, expiring exact evidence.

No ledger, capacity or broker is accessible to this thread. A missing/pending
read is never evidence of absence. A negative remote recovery additionally
requires a local read STARTED after that remote response (lookup_order_after).
"""
from __future__ import annotations

import copy
import threading
import time
from .stream import Health, StreamUnavailable


class BackgroundAccountView:
    def __init__(self, source, *, refresh_seconds=.025, max_age_seconds=.25, max_keys=128, owner_wake=None):
        self.source = source
        self.refresh_seconds = refresh_seconds
        self.result_max_age = max_age_seconds
        self.max_keys = max_keys
        self._owner_wake = owner_wake
        self._condition = threading.Condition()
        self._closing = False
        self._requests = {}
        self._health = None
        self._health_error = 'account reader starting'
        self._health_mono = 0.
        self._reads = self._failures = 0
        self._last_read_ms = 0.
        self._thread = threading.Thread(target=self._loop, name='v9711-account-reader', daemon=True)
        self._thread.start()

    def health(self):
        with self._condition:
            health, error, checked = self._health, self._health_error, self._health_mono
            closed = self._closing
        elapsed = time.monotonic()-checked
        if closed or health is None or error or elapsed > self.result_max_age:
            raise StreamUnavailable(error or 'background account health expired')
        age = health.age_seconds+elapsed
        if age > self.source.max_age_seconds:
            raise StreamUnavailable('background account publication expired')
        return Health(health.account_id, age, health.last_event_seq)

    def _read(self, method, kwargs, *, after_ns=0, diagnostics=None):
        self.health()
        key = (method, tuple(sorted(kwargs.items())))
        now = time.monotonic()
        with self._condition:
            request = self._requests.get(key)
            if request is None:
                if len(self._requests) >= self.max_keys:
                    removable = [(r['requested'], k) for k, r in self._requests.items() if not r.get('running')]
                    if not removable:
                        raise StreamUnavailable('account reader request capacity full')
                    self._requests.pop(min(removable, key=lambda item: item[0])[1])
                request = dict(method=method, kwargs=kwargs, requested=now, started_ns=0, completed=0.,
                               min_start_ns=after_ns, next_read=0., value=None, error='read pending')
                self._requests[key] = request
            request['requested'] = now
            if after_ns > request['started_ns']:
                request['min_start_ns'] = max(after_ns, request.get('min_start_ns', 0))
                request['next_read'] = 0.
            self._condition.notify_all()
            if (request['error'] or now-request['completed'] > self.result_max_age
                    or request['started_ns'] < after_ns):
                raise StreamUnavailable(request['error'] or 'exact account read pending/expired')
            value, details = request['value']
        value, details = copy.deepcopy(value), dict(details)
        if diagnostics is not None:
            diagnostics.update(details)
        return value

    def lookup_order(self, **kwargs):
        return self._read('lookup_order', kwargs)

    def lookup_order_after(self, *, after_ns, **kwargs):
        return self._read('lookup_order', kwargs, after_ns=after_ns)

    def forced_flat_evidence(self, **kwargs):
        return self._read('forced_flat_evidence', kwargs)

    def manual_flat_evidence(self, *, diagnostics=None, **kwargs):
        return self._read('manual_flat_evidence', kwargs, diagnostics=diagnostics)

    def diagnostics(self):
        with self._condition:
            return dict(reads=self._reads, failures=self._failures, requests=len(self._requests),
                        last_read_ms=round(self._last_read_ms, 3), error=self._health_error)

    def _loop(self):
        next_health = 0.
        while True:
            now = time.monotonic()
            with self._condition:
                if self._closing:
                    return
            if now >= next_health:
                checked = time.monotonic()
                try:
                    health, error = self.source.health(), ''
                except Exception as exc:
                    health, error = None, str(exc)
                with self._condition:
                    self._health, self._health_error, self._health_mono = health, error, checked
                next_health = time.monotonic()+self.refresh_seconds
            with self._condition:
                active = [(r['next_read'], k, r) for k, r in self._requests.items()
                          if now-r['requested'] < 2. and r['next_read'] <= now]
                if not active:
                    self._condition.wait(timeout=self.refresh_seconds)
                    continue
                _, key, request = min(active, key=lambda item: item[0])
                request['running'] = True
            started = time.perf_counter_ns()
            details = {}
            try:
                kwargs = dict(request['kwargs'])
                if request['method'] == 'manual_flat_evidence':
                    kwargs['diagnostics'] = details
                value = getattr(self.source, request['method'])(**kwargs)
                value, error = (value, details), ''
            except Exception as exc:
                value, error = None, str(exc)
            completed = time.monotonic()
            with self._condition:
                changed = (request['error'] != error or request['value'] != value
                           or request['started_ns'] < request['min_start_ns'] <= started)
                request.update(value=value, error=error, started_ns=started, completed=completed,
                               running=False, next_read=completed+self.refresh_seconds)
                if started < request['min_start_ns']:
                    request['next_read'] = 0.
                self._reads += 1
                self._failures += bool(error)
                self._last_read_ms = (time.perf_counter_ns()-started)/1e6
            if changed and self._owner_wake is not None:
                self._owner_wake()  # Notification only; never apply account/ledger mutations here.

    def close(self):
        with self._condition:
            self._closing = True
            self._condition.notify_all()
        self._thread.join()
