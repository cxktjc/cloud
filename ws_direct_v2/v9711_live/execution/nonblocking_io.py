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
from .submit_errors import ConfirmedSubmitRejection, EntrySendGateRejection
from .entry_quotes import QUOTE_FIELDS
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
    ready: Any = None
    cancel_not_before: float = float('-inf')


class NetworkIO(EntryCancelPacing):
    def __init__(self, max_jobs: int = 64, workers: int | None = None, *,
                 max_entry_cancels: int = 1024,
                 cancel_interval_seconds: float = ENTRY_CANCEL_INTERVAL_SECONDS,
                 cancel_clock: Callable[[], float] | None = None,
                 worker_start: Callable[[], None] | None = None,
                 owner_wake: Callable[[], None] | None = None):
        workers = cfg.env_int('V9711_LIVE_IO_WORKERS', 19) if workers is None else workers
        if not 1 <= workers <= 19:
            raise ValueError('I/O workers must be between 1 and 19')
        self.worker_count = workers
        # With the default 19 workers, keep eight available for fresh entries
        # and urgent closes even if remote exact-order queries await replies.
        self._query_workers = workers-cfg.MAX_PENDING_ORDERS if workers > cfg.MAX_PENDING_ORDERS else workers
        self.worker_exit = None
        self._worker_start = worker_start
        self._owner_wake = owner_wake
        self._warm_pending = workers if worker_start is not None else 0
        self._warm_ready = self._warm_failed = 0
        self._completed_callbacks = threading.Event()
        self.callback_limit = None
        self.callback_budget_seconds = None
        self.last_drain_urgent = False
        self._valid_query_keys = None
        self._query_keys = set()  # Only the owner adds/retires exact query identities.
        self._owner = threading.get_ident()
        self._condition = threading.Condition()
        self._jobs: dict[tuple, _Job] = {}
        self._watched_ready = {}  # receipt -> number of dependent jobs
        self._aliases: dict[tuple, tuple] = {}
        self._heap: list[tuple[int, int, tuple]] = []
        self._sequence = itertools.count()
        self._closed = False
        self._max_jobs = max_jobs
        if max_jobs < 1 or max_entry_cancels < 1:
            raise ValueError('network queue capacities must be positive')
        self._max_entry_cancels = max_entry_cancels
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
               aliases: tuple[tuple, ...] = (), entry_cancel: bool = False,
               ready: Any = None, entry_cancel_delay: float = 0.) -> bool:
        self._assert_owner()
        with self._condition:
            if key in self._jobs or key in self._aliases:
                return False
            if any(a in self._jobs or a in self._aliases for a in aliases):
                raise ConfirmedSubmitRejection('duplicate exact batch member; request never sent')
            lane = 'cancel' if entry_cancel else 'query' if key[0] == 'query' else 'mutation'
            used = sum(('cancel' if j.entry_cancel else 'query' if j.key[0] == 'query' else 'mutation') == lane
                       for j in self._jobs.values())
            capacity = self._max_entry_cancels if entry_cancel else self._max_jobs
            if self._closed or used >= capacity:
                raise ConfirmedSubmitRejection('local I/O queue unavailable; request never sent')
            identities = aliases or (key,)
            symbols = frozenset(str(k[2] if k[0] == 'query' else k[1]) for k in identities)
            self._jobs[key] = _Job(key, work, done, priority, symbols, aliases,
                                   entry_cancel=bool(entry_cancel), ready=ready,
                                   cancel_not_before=(self._entry_cancel_clock() + entry_cancel_delay
                                                      if entry_cancel and entry_cancel_delay > 0 else float('-inf')))
            if key[0] == 'query':
                self._valid_query_keys = None
                self._query_keys.add(key)
            for alias in aliases:
                self._aliases[alias] = key
            heapq.heappush(self._heap, (priority, next(self._sequence), key))
            # One new job needs one idle worker. Waking all 19 on every entry
            # creates avoidable GIL/condition contention during a burst.
            if ready is None or ready.done():
                self._condition.notify(1)
            else:
                count = self._watched_ready.get(ready, 0)
                self._watched_ready[ready] = count + 1
                if not count:
                    ready.add_done_callback(self._job_ready)
        return True

    def _job_ready(self, future):
        with self._condition:
            count = self._watched_ready.pop(future, 0)
            if count and not self._closed:
                # Runs on the journal writer. Signal dependents without doing
                # a global scheduling pass before the next write can begin.
                # Workers still select under the same symbol/cancel/query gates.
                self._condition.notify(min(count, self.worker_count))

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
        self.last_drain_urgent = False
        if not self._completed_callbacks.is_set():
            return 0
        with self._condition:
            self._completed_callbacks.clear()
            ready = [job for job in self._jobs.values() if job.state == 'done' and job.done]
        count = 0
        started = time.perf_counter()
        for job in ready:
            if count and ((self.callback_limit is not None and count >= self.callback_limit)
                    or (self.callback_budget_seconds is not None
                        and time.perf_counter()-started >= self.callback_budget_seconds)):
                self._completed_callbacks.set()
                break
            # Retain the completion if accounting/save throws; cumulative fill
            # application is idempotent and can be retried next iteration.
            try:
                job.done(job.value, job.error)
            except BaseException:
                self._completed_callbacks.set()  # accounting/save must retry on the owner
                raise
            # NEW/zero-fill entry ACK only adds exchange identity evidence.
            # Its owner callback has already saved it and published the gate.
            # All fills, cancels, rejects and uncertain results remain urgent.
            value = job.value
            try:
                ordinary_ack = (job.key[0] == 'entry' and job.error is None
                    and isinstance(value, dict) and str(value.get('status')).upper() == 'NEW'
                    and str(value.get('orderId') or '') not in ('', '0')
                    and value.get('symbol') == job.key[1] and value.get('clientOrderId') == job.key[2]
                    and float(value.get('executedQty', 'nan')) == 0.)
            except (ValueError, TypeError, OverflowError):
                ordinary_ack = False
            self.last_drain_urgent |= not ordinary_ack
            with self._condition:
                if self._jobs.get(job.key) is job:
                    self._jobs.pop(job.key)
                    for alias in job.aliases:
                        self._aliases.pop(alias, None)
                self._wake_queued()
            count += 1
        # A time-budgeted tail may contain a fill/reject. Never classify an
        # uninspected completion as a harmless NEW acknowledgment.
        self.last_drain_urgent |= self._completed_callbacks.is_set()
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
        now_ms = now * 1000
        close_ms = bar_ms + gate.get('bar_length_ms', 0)
        if self._closed:
            raise EntrySendGateRejection('entry_worker_closed')
        if not gate.get('ready'):
            raise EntrySendGateRejection('account_gate' if gate.get('signal_status') == 'account_not_ready'
                                         else 'entry_signal_not_ready', gate.get('signal_status', ''))
        if not bar_ms <= now_ms < close_ms:
            if gate.get('admitted_entry_quotes', {}).get(cid, {}).get('signal_follow'):
                raise EntrySendGateRejection('entry_follow_not_allowed_at_send')
            raise EntrySendGateRejection('entry_bar_ended_at_send' if now_ms >= close_ms else 'entry_bar_not_started',
                                         f'order_bar_ms={bar_ms}, close_ms={close_ms}, check_ms={now_ms:.3f}')
        if gate.get('bar_ms') != bar_ms:
            raise EntrySendGateRejection('entry_plan_bar_changed',
                                         f'order_bar_ms={bar_ms}, plan_bar_ms={gate.get("bar_ms")}')
        ttl = gate.get('ttl', 0)
        if now > gate.get('expires', 0) or now - gate.get('generated', 0) > ttl or now - gate.get('refreshed', 0) > ttl:
            raise EntrySendGateRejection('entry_plan_stale_at_send')
        if gate.get('held', 0) >= gate.get('position_cap', 0) and not final_exit_bar:
            raise EntrySendGateRejection('entry_position_cap_at_send', f'held={gate.get("held", 0)}')
        if cid not in gate.get('allowed', ()):
            reason = gate.get('entry_blocks', {}).get(cid, 'entry_id_not_allowed')
            cancel_reason = gate.get('entry_cancel_reasons', {}).get(cid)
            raise EntrySendGateRejection(reason, f'cancel_reason={cancel_reason}' if cancel_reason else '',
                                         cancel_reason=cancel_reason)
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
        if not self._heap:
            return None
        busy = set().union(*(j.symbols for j in self._jobs.values()
            if j.state == 'running' or j.state == 'done' and j.done is not None
            or j.state == 'queued' and j.ready is not None and not j.ready.done()))
        deferred = []
        selected = None
        cancel_checked = False
        next_cancel = None
        query_slots = self._query_workers-sum(j.key[0] == 'query' and j.state == 'running'
                                            for j in self._jobs.values())
        while self._heap:
            ticket = heapq.heappop(self._heap)
            job = self._jobs.get(ticket[2])
            if job is None or job.state != 'queued':
                continue
            if job.ready is not None and not job.ready.done():
                deferred.append(ticket)
                continue
            if job.key[0] == 'query' and query_slots <= 0:
                deferred.append(ticket)
                continue
            if job.entry_cancel:
                if not cancel_checked:
                    next_cancel = self._next_entry_cancel()
                    cancel_checked = True
                if job is not next_cancel:
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

    def _wake_queued(self):
        # Called under _condition. A completed request/owner ACK must not
        # awaken 19 idle workers when there is no work. Broadcasts made the
        # quote owner and newly durable entries compete with empty scanners.
        queued = [job for job in self._jobs.values() if job.state == 'queued']
        if not queued:
            return
        busy = set().union(*(job.symbols for job in self._jobs.values()
            if job.state == 'running' or job.state == 'done' and job.done is not None
            or job.state == 'queued' and job.ready is not None and not job.ready.done()))
        next_cancel = self._next_entry_cancel()
        eligible = [job for job in queued if (job.ready is None or job.ready.done()) and not job.symbols & busy
                    and (not job.entry_cancel or job is next_cancel)]
        query_slots = max(0, self._query_workers-sum(j.key[0] == 'query' and j.state == 'running'
                                                  for j in self._jobs.values()))
        queries = sum(job.key[0] == 'query' for job in eligible)
        count = len(eligible)-queries+min(queries, query_slots)
        if count:
            self._condition.notify(min(count, self.worker_count))

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
                    self._wake_queued()
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
        while self.drain():
            pass  # Shutdown drains every result, irrespective of the live turn budget.


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
        allowed = set(follow_entries)
        blocked, cancel_reasons = {}, {}
        expected_weight = cfg.SLOT_WEIGHTS[min(len(self.positions), 2)]
        for row in self.tracked.values():
            if row.get('signal_follow'):
                continue
            cid = str(row.get('client_order_id'))
            if row.get('cancel_requested'):
                if row.get('cancel_reason'):
                    cancel_reasons[cid] = row['cancel_reason']
                blocked[cid] = ('entry_sizing_changed_at_send'
                                if row.get('cancel_reason') == 'position_count_sizing_changed'
                                else 'entry_cancel_pending_at_send')
            elif row.get('sizing_weight') not in (None, expected_weight):
                blocked[cid] = 'entry_sizing_changed_at_send'
            elif self._forced_entry_blocked(str(row.get('symbol') or ''), now=now):
                blocked[cid] = 'entry_forced_close_at_send'
            else:
                allowed.add(cid)
        gate = dict(ready=status == 'valid' and bool(snapshot.get('signal_ready')),
            signal_status=status,
            bar_ms=int(self._finite(snapshot.get('bar_open_ms'))), bar_length_ms=cfg.BAR_MS,
            generated=self._finite(snapshot.get('generated_at_epoch')),
            expires=self._finite(snapshot.get('expires_at_epoch')), ttl=cfg.DESIRED_TTL_SECONDS,
            refreshed=now, held=len(self.positions), position_cap=cfg.SLOTS,
            follow_entries=follow_entries,
            entry_gap_limits={str(row.get('client_order_id')): row.get('entry_gap_ceiling', cfg.CANCEL_GAP)
                              for row in self.tracked.values() if not row.get('signal_follow')},
            admitted_entry_quotes={str(row.get('client_order_id')):
                dict(symbol=symbol, signal_follow=bool(row.get('signal_follow')),
                     signal_limit_price=row.get('limit_price'),
                     **{key: row[key] for key in QUOTE_FIELDS if key in row})
                for symbol, row in self.tracked.items()},
            # Only the owner reads positions. Workers receive a normalized,
            # immutable snapshot, never signal targets or a mutable ledger.
            position_exit_due_epochs=tuple(self._finite(pos.get('exit_due_epoch'))
                                           for pos in self.positions.values()),
            entry_blocks=blocked, entry_cancel_reasons=cancel_reasons, allowed=frozenset(allowed))
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
        if self._network_io is None or os.environ.get('V9711_LIVE_ENTRY_BATCH', '0') == '0':
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
                    self._run_network(key, work, done, priority=1)
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
                    self._run_network(key, send, complete, priority=1, aliases=aliases)

    def _run_network(self, key: tuple, work: Callable[[], Any],
                     done: Callable[[Any, BaseException | None], None], *, priority: int,
                     entry_request: dict[str, Any] | None = None,
                     aliases: tuple[tuple, ...] = (), entry_cancel: bool = False,
                     entry_cancel_delay: float = 0.) -> None:
        if entry_request is not None and self._entry_plan_buffer is not None:
            self._entry_plan_buffer.append((key, work, done, dict(entry_request)))
            return
        barrier = self.ledger.flush_pending() if key[0] != 'cancel' else None
        self._publish_entry_gate()
        if barrier is not None:
            original_work = work
            identities = aliases or (key,)
            required = frozenset((item[1], item[2]) for item in identities if item[0] in {'entry', 'close'})
            timings = self.session_entries.timings
            def work():
                for item in identities:
                    if item[0] == 'entry':
                        timings.mark(item[2], 'worker')
                try:
                    receipt = barrier.result()
                except Exception:
                    receipt = None
                # Raise outside the handler: the existing conservative error
                # classifier treats a chained OSError as possibly sent. Here
                # original_work has provably never been called.
                if receipt is None:
                    raise ConfirmedSubmitRejection('durable registration failed; request never sent')
                if not required <= receipt.reservations:
                    raise ConfirmedSubmitRejection('durable reservation superseded; request never sent')
                for item in identities:
                    if item[0] == 'entry':
                        timings.mark(item[2], 'durable')
                try:
                    return original_work()
                finally:
                    for item in identities:
                        if item[0] != 'entry':
                            continue
                        if receipt.write_started_ns:
                            timings.mark_at(item[2], 'write_started', receipt.write_started_ns, receipt.write_started_epoch_ms)
                        if receipt.completed_ns:
                            timings.mark_at(item[2], 'persisted', receipt.completed_ns, receipt.completed_epoch_ms)
                        for stage in ('prepared', 'written', 'synced'):
                            stamp = getattr(receipt, stage+'_ns', 0)
                            if stamp:
                                epoch_ms = receipt.completed_epoch_ms-(receipt.completed_ns-stamp)/1e6
                                timings.mark_at(item[2], stage, stamp, epoch_ms)
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
                                    aliases=aliases, entry_cancel=entry_cancel, ready=barrier,
                                    entry_cancel_delay=entry_cancel_delay)
        except ConfirmedSubmitRejection as exc:
            complete(None, exc)

    def shutdown_io(self) -> None:
        if self._network_io is not None:
            self._network_io.shutdown()

    def _drain_network(self) -> int:
        if self._network_io is not None:
            # Apply one bounded callback turn as one owner save. Network wake
            # turns run outside maintenance's existing batch; serializing the
            # whole ledger after every NEW ACK needlessly delayed quote intake.
            with self.ledger.batch_saves():
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
