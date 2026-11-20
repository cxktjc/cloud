"""Single-owner state + independent bounded background network requests.

The owner thread never waits for network I/O. Workers never touch ledgers,
positions, signal files or terminal output. Result callbacks run only in drain().
Exact operation keys deduplicate queued/running calls. Independent symbols and
plans do not wait for unrelated ACKs; only same-symbol actions are sequenced.
"""
from __future__ import annotations

from dataclasses import dataclass
from contextlib import contextmanager
import os
import heapq
import itertools
import logging
import threading
import time
from typing import Any, Callable

from .. import config as cfg
from .submit_errors import ConfirmedSubmitRejection
from .signal_follow import follow_send_allowed, FOLLOW_POSITION_CAP
from .cancel_pacing import (EntryCancelPacing, ENTRY_CANCEL_INTERVAL_SECONDS,
                            exact_entry_cancel_key)

LOG = logging.getLogger('v9711_live_trade.io')

class NetworkPending(Exception):
    """A background query is queued/running, NOT a timeout or API failure."""


@dataclass
class _Job:
    key: tuple
    work: Callable[[], Any]
    done: Callable[[Any, BaseException | None], None] | None
    priority: int
    symbols: frozenset[str] = frozenset()
    aliases: tuple[tuple, ...] = ()
    entry_cancel: bool = False
    state: str = 'queued'
    value: Any = None
    error: BaseException | None = None


class NetworkIO(EntryCancelPacing):
    def __init__(self, max_jobs: int = 64, workers: int | None = None, *,
                 cancel_interval_seconds: float = ENTRY_CANCEL_INTERVAL_SECONDS,
                 cancel_clock: Callable[[], float] | None = None,
                 worker_start: Callable[[], None] | None = None,
                 owner_wake: Callable[[], None] | None = None):
        workers = cfg.env_int('V9711_LIVE_IO_WORKERS', 19) if workers is None else workers
        if not 1 <= workers <= 19:
            raise ValueError('I/O workers must be between 1 and 19')
        self.worker_count = workers
        self.worker_exit = None
        self._worker_start = worker_start
        self._owner_wake = owner_wake
        self._warm_pending = workers if worker_start is not None else 0
        self._warm_ready = self._warm_failed = 0
        self._completed_callbacks = threading.Event()
        self._valid_query_keys = None
        self._query_keys = set()  # Only the owner adds/retires exact query identities.
        self._owner = threading.get_ident()
        self._condition = threading.Condition()
        self._jobs: dict[tuple, _Job] = {}
        self._aliases: dict[tuple, tuple] = {}
        self._heap: list[tuple[int, int, tuple]] = []
        self._sequence = itertools.count()
        self._closed = False
        self._max_jobs = max_jobs
        self._entry_gate: dict[str, Any] = {}
        self._init_entry_cancels(interval=cancel_interval_seconds, clock=cancel_clock)
        self._threads = [threading.Thread(target=self._loop, name=f'v9711-network-io-{i}',
                                          daemon=True) for i in range(workers)]
        self._thread = self._threads[0]  # backwards-compatible inspection hook
        for thread in self._threads:
            thread.start()

    def in_worker(self) -> bool:
        return threading.current_thread() in self._threads

    def _assert_owner(self) -> None:
        if threading.get_ident() != self._owner:
            raise RuntimeError('Only the trade main thread may mutate jobs/state')

    def contains(self, key: tuple) -> bool:
        with self._condition:
            return key in self._jobs or self._aliases.get(key) in self._jobs

    def submit(self, key: tuple, work: Callable[[], Any], *, priority: int,
               done: Callable[[Any, BaseException | None], None] | None = None,
               aliases: tuple[tuple, ...] = (), entry_cancel: bool = False) -> bool:
        self._assert_owner()
        with self._condition:
            if key in self._jobs or key in self._aliases:
                return False
            if any(a in self._jobs or a in self._aliases for a in aliases):
                raise ConfirmedSubmitRejection('duplicate exact batch member; request never sent')
            if self._closed or len(self._jobs) >= self._max_jobs:
                raise ConfirmedSubmitRejection('local I/O queue unavailable; request never sent')
            identities = aliases or (key,)
            symbols = frozenset(str(k[2] if k[0] == 'query' else k[1]) for k in identities)
            self._jobs[key] = _Job(key, work, done, priority, symbols, aliases,
                                   entry_cancel=bool(entry_cancel))
            if key[0] == 'query':
                self._valid_query_keys = None
                self._query_keys.add(key)
            for alias in aliases:
                self._aliases[alias] = key
            heapq.heappush(self._heap, (priority, next(self._sequence), key))
            self._condition.notify_all()
        return True

    def poll(self, key: tuple, work: Callable[[], Any], *, priority: int = 20) -> Any:
        self._assert_owner()
        with self._condition:
            job = self._jobs.get(key)
            if job is not None and job.state == 'done':
                self._jobs.pop(key)
                self._query_keys.discard(key)
                if job.error is not None:
                    raise job.error
                return job.value
        self.submit(key, work, priority=priority)
        raise NetworkPending()

    def drain(self) -> int:
        """Apply completed mutations on owner only; never wait for a worker."""
        self._assert_owner()
        if not self._completed_callbacks.is_set():
            return 0
        with self._condition:
            self._completed_callbacks.clear()
            ready = [job for job in self._jobs.values() if job.state == 'done' and job.done]
        count = 0
        for job in ready:
            # Retain the completion if accounting/save throws; cumulative fill
            # application is idempotent and can be retried next iteration.
            try:
                job.done(job.value, job.error)
            except BaseException:
                self._completed_callbacks.set()  # accounting/save must retry on the owner
                raise
            with self._condition:
                if self._jobs.get(job.key) is job:
                    self._jobs.pop(job.key)
                    for alias in job.aliases:
                        self._aliases.pop(alias, None)
                self._condition.notify_all()
            count += 1
        return count

    def prune_queries(self, valid: set[tuple]) -> None:
        self._assert_owner()
        if valid == self._valid_query_keys:
            return
        with self._condition:
            self._valid_query_keys = frozenset(valid)
            for key, job in list(self._jobs.items()):
                if key[0] == 'query' and key not in valid:
                    self._jobs.pop(key)
                    self._query_keys.discard(key)
            # Remove stale heap tickets so a stream-only workload cannot grow
            # an unbounded queue of canceled/retired query identities.
            self._heap = [ticket for ticket in self._heap if ticket[2] in self._jobs]
            heapq.heapify(self._heap)

    def set_entry_gate(self, gate: dict[str, Any]) -> None:
        self._assert_owner()
        # Replace a snapshot; no ledger/state lock spans any network call.
        self._entry_gate = gate

    def check_entry(self, symbol: str, cid: str, bar_ms: int) -> bool:
        gate = self._entry_gate
        now = time.time()
        if not self._closed and follow_send_allowed(gate, symbol, cid, bar_ms, now=now):
            return True
        # Match the owner's final-pre-exit-bar admission at the actual send
        # time. Use immutable due times, not a latched allow-full boolean:
        # queued work/reconnects must not extend the exception past its end.
        final_exit_bar = any(
            0.0 < due - now <= gate.get('bar_length_ms', 0) / 1000.0
            for due in gate.get('position_exit_due_epochs', ())
        )
        if (self._closed or not gate.get('ready') or gate.get('bar_ms') != bar_ms
                or not bar_ms <= now * 1000 < bar_ms + gate.get('bar_length_ms', 0)
                or now > gate.get('expires', 0) or now - gate.get('generated', 0) > gate.get('ttl', 0)
                or now - gate.get('refreshed', 0) > gate.get('ttl', 0)
                or (gate.get('held', 0) >= gate.get('position_cap', 0) and not final_exit_bar)
                or cid not in gate.get('allowed', ())):
            raise ConfirmedSubmitRejection('queued entry no longer valid/full/expired before send; request never sent')
        return False

    def health(self) -> dict[str, Any]:
        with self._condition:
            return {'enabled': True, 'queued': sum(j.state == 'queued' for j in self._jobs.values()),
                    'running': sum(j.state == 'running' for j in self._jobs.values()),
                    'completed': sum(j.state == 'done' for j in self._jobs.values()),
                    'workers': self.worker_count,
                    'transport_warmup': dict(pending=self._warm_pending, ready=self._warm_ready,
                                             failed=self._warm_failed),
                    'entry_cancel_pacing': self._entry_cancel_health()}

    def _take_ready_job(self) -> _Job | None:
        # Explicit workers=1 is the old serial fallback. Default parallel mode
        # has NO global ACK barrier; a slow plan cannot block a different one.
        if self.worker_count == 1 and any(
                j.state == 'done' and j.done is not None for j in self._jobs.values()):
            return None
        busy = set().union(*(j.symbols for j in self._jobs.values()
            if j.state == 'running' or j.state == 'done' and j.done is not None))
        deferred = []
        selected = None
        next_cancel = self._next_entry_cancel()
        while self._heap:
            ticket = heapq.heappop(self._heap)
            job = self._jobs.get(ticket[2])
            if job is None or job.state != 'queued':
                continue
            if job.entry_cancel and job is not next_cancel:
                deferred.append(ticket)
                continue
            if job.symbols & busy:
                deferred.append(ticket)
                continue
            selected = job
            break
        for ticket in deferred:
            heapq.heappush(self._heap, ticket)
        return selected

    def _loop(self) -> None:
        try:
            if self._worker_start is not None:
                try:
                    self._worker_start()
                except Exception:
                    with self._condition:
                        self._warm_failed += 1
                        first_failure = self._warm_failed == 1
                    if first_failure:
                        LOG.warning('trade WS startup warmup failed; normal exact requests may retry transport',
                                    exc_info=True)
                else:
                    with self._condition:
                        self._warm_ready += 1
                finally:
                    with self._condition:
                        self._warm_pending -= 1
            while True:
                with self._condition:
                    job = None
                    while not self._closed:
                        job = self._take_ready_job()
                        if job is not None:
                            break
                        self._condition.wait(timeout=self._entry_cancel_timeout())
                    if self._closed:
                        return
                    job.state = 'running'
                try:
                    value, error = job.work(), None
                except BaseException as exc:
                    value, error = None, exc
                with self._condition:
                    self._entry_cancel_completed(job)
                    job.value, job.error, job.state = value, error, 'done'
                    if job.done is not None:
                        self._completed_callbacks.set()
                    self._condition.notify_all()
                if job.done is not None and self._owner_wake is not None:
                    self._owner_wake()

        finally:
            if self.worker_exit is not None:
                self.worker_exit()

    def shutdown(self) -> None:
        """Do not release the single-writer process lock with a sender alive."""
        self._assert_owner()
        with self._condition:
            self._closed = True
            for job in self._jobs.values():
                if job.state == 'queued':
                    job.state = 'done'
                    job.error = ConfirmedSubmitRejection('trade stopping; queued request never sent')
                    if job.done is not None:
                        self._completed_callbacks.set()
            self._condition.notify_all()
        for thread in self._threads:
            thread.join()  # only shutdown waits for the bounded in-flight requests
        self.drain()


class NetworkTradeMixin:
    def enable_nonblocking_io(self) -> None:
        if self._network_io is not None:
            return
        self._network_io = NetworkIO(cancel_interval_seconds=ENTRY_CANCEL_INTERVAL_SECONDS,
                                     worker_start=self.client.warm_worker_transport,
                                     owner_wake=getattr(self, '_network_wake', None))
        self.client.network_io = self._network_io
        self._network_io.worker_exit = self.client.close_worker_transport
        self._publish_entry_gate()

    def _publish_entry_gate(self) -> None:
        io = self._network_io
        snapshot, status = self._io_signal
        now = time.time()
        follow_entries = (self._follow_gate_entries(snapshot, now=now)
                          if hasattr(self, '_follow_gate_entries') and len(self.positions) < FOLLOW_POSITION_CAP else {})
        gate = dict(ready=status == 'valid' and bool(snapshot.get('signal_ready')),
            bar_ms=int(self._finite(snapshot.get('bar_open_ms'))), bar_length_ms=cfg.BAR_MS,
            generated=self._finite(snapshot.get('generated_at_epoch')),
            expires=self._finite(snapshot.get('expires_at_epoch')), ttl=cfg.DESIRED_TTL_SECONDS,
            refreshed=now, held=len(self.positions), position_cap=cfg.SLOTS,
            follow_entries=follow_entries,
            entry_gap_limits={str(row.get('client_order_id')): row.get('entry_gap_ceiling', cfg.CANCEL_GAP)
                              for row in self.tracked.values() if not row.get('signal_follow')},
            # Only the owner reads positions. Workers receive a normalized,
            # immutable snapshot, never signal targets or a mutable ledger.
            position_exit_due_epochs=tuple(self._finite(pos.get('exit_due_epoch'))
                                           for pos in self.positions.values()),
            allowed=frozenset(follow_entries) | frozenset(str(p.get('client_order_id')) for p in self.tracked.values()
                              if not p.get('signal_follow') and not p.get('cancel_requested')
                              and (p.get('sizing_weight') is None
                                   or p.get('sizing_weight') == cfg.SLOT_WEIGHTS[min(len(self.positions), 2)])
                              and not self._forced_entry_blocked(str(p.get('symbol') or ''), now=now)))
        self.client.entry_gate = gate
        if io is not None:
            io.set_entry_gate(gate)

    @contextmanager
    def entry_cancel_plan(self):
        """Collect one owner cycle before allowing the nearest cancel to start."""
        io = self._network_io
        if io is None:
            yield
            return
        io.hold_entry_cancels()
        self._cancel_prices = {}
        try:
            yield
        finally:
            try:
                self._refresh_entry_cancel_plan()
            finally:
                io.release_entry_cancels()

    def _refresh_entry_cancel_plan(self) -> None:
        if self._network_io is None:
            return
        prices = getattr(self, '_cancel_prices', {})
        self._network_io.update_entry_cancels({
            exact_entry_cancel_key(symbol, row.get('client_order_id'), row.get('order_id')): {
                'price': prices.get(symbol),
                'limit_price': row.get('submitted_limit_price') or row.get('limit_price'),
            }
            for symbol, row in self.tracked.items() if row.get('cancel_requested')})

    @contextmanager
    def entry_plan(self):
        if self._network_io is None or os.environ.get('V9711_LIVE_ENTRY_BATCH', '1') == '0':
            yield
            return
        if self._entry_plan_buffer is not None:
            raise RuntimeError('Nested entry plan collection is not supported')
        self._entry_plan_buffer = []
        try:
            yield
        finally:
            items, self._entry_plan_buffer = self._entry_plan_buffer, None
            for start in range(0, len(items), 5):
                group = items[start:start + 5]
                if len(group) == 1:
                    key, work, done, request = group[0]
                    self._run_network(key, work, done, priority=10)
                else:
                    aliases = tuple(item[0] for item in group)
                    key = ('entry_batch', aliases)
                    LOG.info('dispatch entry plan transport=REST_BATCH n=%d symbols=%s; no previous-plan ACK wait',
                             len(group), ','.join(item[0][1] for item in group))
                    def send(group=group):
                        return self.client.place_limit_batch([item[3] for item in group])
                    def complete(values, error, group=group):
                        for member_key, work, done, request in group:
                            value, member_error = (None, error) if error else values.get(
                                member_key[2], (None, RuntimeError('batch member result missing; exact fate unknown')))
                            done(value, member_error)
                    self._run_network(key, send, complete, priority=10, aliases=aliases)

    def _run_network(self, key: tuple, work: Callable[[], Any],
                     done: Callable[[Any, BaseException | None], None], *, priority: int,
                     entry_request: dict[str, Any] | None = None,
                     aliases: tuple[tuple, ...] = (), entry_cancel: bool = False) -> None:
        if entry_request is not None and self._entry_plan_buffer is not None:
            self._entry_plan_buffer.append((key, work, done, dict(entry_request)))
            return
        self._publish_entry_gate()
        if self._network_io is None:
            try:
                result = work()
            except Exception as exc:
                done(None, exc)
            else:
                done(result, None)
            return
        def complete(value, error):
            try:
                done(value, error)
            finally:
                self._publish_entry_gate()
        try:
            self._network_io.submit(key, work, priority=priority, done=complete,
                                    aliases=aliases, entry_cancel=entry_cancel)
        except ConfirmedSubmitRejection as exc:
            complete(None, exc)

    def shutdown_io(self) -> None:
        if self._network_io is not None:
            self._network_io.shutdown()

    def _drain_network(self) -> int:
        if self._network_io is not None:
            completed = self._network_io.drain()
            if not self._network_io._query_keys:
                return completed
            valid = set()
            for row in list(self.tracked.values()) + list(self.positions.values()):
                symbol = str(row.get('symbol') or '')
                cid = str(row.get('close_client_order_id') or row.get('client_order_id') or '')
                oid = row.get('close_order_id') or row.get('order_id')
                if cid:
                    valid.add(('query', 'get_order_by_client_order_id', symbol, cid))
                if oid:
                    valid.add(('query', 'get_order', symbol, str(oid)))
            self._network_io.prune_queries(valid)
            return completed
        return 0
