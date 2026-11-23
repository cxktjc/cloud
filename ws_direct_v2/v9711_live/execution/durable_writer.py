"""One ordered disk writer; immutable snapshots and positive send receipts.

Only the owner serializes state. At most one snapshot is writing and one is
pending. Replacing the pending snapshot shares its Future: a newer complete
snapshot covers older tickets only for identities actually present in it.
"""
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
import json
import hashlib
import logging
import os
from pathlib import Path
import struct
import threading
import time
import uuid

from ..contracts import atomic_write_text

LOG = logging.getLogger('v9711_live_trade.durability')


@dataclass(frozen=True)
class Snapshot:
    intents: str
    state: str
    reservations: frozenset
    submitted_ns: int = field(default_factory=lambda: time.perf_counter_ns(), compare=False)


@dataclass(frozen=True)
class Receipt:
    reservations: frozenset
    completed_ns: int = 0
    completed_epoch_ms: float = 0.
    write_started_ns: int = 0
    write_started_epoch_ms: float = 0.
    prepared_ns: int = 0
    written_ns: int = 0
    synced_ns: int = 0


class DurableWriter:
    def __init__(self, intent_path, state_path, *, saved=(None, None)):
        self.paths = (intent_path, state_path)
        self._saved = list(saved)
        self._condition = threading.Condition()
        self._pending = self._latest = self._latest_future = None
        self._closing = self._writing = False
        self._submitted = self._written = self._coalesced = self._failures = 0
        self._last_ms = 0.
        self._queue_wait_ms = 0.
        self._file_ms = [0., 0.]
        self._error = ''
        # The coordinator still orders complete snapshots. Independent files
        # in one snapshot can fsync in parallel; the next snapshot must wait
        # for BOTH file tasks, including when either one fails.
        # The coordinator writes the first file itself; only the other file
        # needs a worker. Avoid a third thread/extra wake on the receipt path.
        self._file_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix='v9711-ledger-file')
        self._thread = threading.Thread(target=self._loop, name='v9711-durable-ledger', daemon=True)
        self._thread.start()

    def submit(self, snapshot):
        with self._condition:
            if self._closing:
                raise RuntimeError('durable ledger writer is closed')
            if snapshot == self._latest and self._latest_future is not None:
                future = self._latest_future
                if not future.done() or (not future.cancelled() and future.exception() is None):
                    return future
            self._submitted += 1
            if self._pending is None:
                future = Future()
            else:
                _, future = self._pending
                self._coalesced += 1
            self._pending = (snapshot, future)
            self._latest, self._latest_future = snapshot, future
            self._condition.notify()
            return future

    def _loop(self):
        while True:
            with self._condition:
                self._condition.wait_for(lambda: self._pending is not None or self._closing)
                if self._pending is None:
                    return
                snapshot, future = self._pending
                self._pending, self._writing = None, True
            started = time.perf_counter()
            write_started_ns, write_started_epoch_ms = time.monotonic_ns(), time.time()*1000
            with self._condition:
                self._queue_wait_ms = (time.perf_counter_ns()-snapshot.submitted_ns)/1e6
            try:
                self._persist(snapshot)
            except Exception as exc:
                with self._condition:
                    self._failures += 1
                    self._error = f'{type(exc).__name__}: {exc}'
                LOG.error('durable ledger write failed; dependent requests will NOT send: %s', self._error)
                future.set_exception(exc)
            else:
                with self._condition:
                    self._written += 1
                    self._error = ''
                future.set_result(Receipt(snapshot.reservations, time.monotonic_ns(), time.time()*1000,
                    write_started_ns, write_started_epoch_ms))
            finally:
                with self._condition:
                    self._writing = False
                    self._last_ms = (time.perf_counter()-started)*1000

    def _persist(self, snapshot):
        changed = [(index, name, body) for index, (name, body) in enumerate(
            (('intents', snapshot.intents), ('state', snapshot.state))) if body != self._saved[index]]
        other = self._file_pool.submit(self._write_file, *changed[1]) if len(changed) == 2 else None
        errors = []
        for offset, (index, name, body) in enumerate(changed):
            try:
                elapsed = other.result() if offset else self._write_file(index, name, body)
            except Exception as exc:
                errors.append(exc)
            else:
                self._saved[index] = body
                with self._condition:
                    self._file_ms[index] = elapsed
        if errors:
            raise errors[0]

    def _write_file(self, index, name, body):
        started = time.perf_counter()
        encoded = ('{"schema_version":1,"component":"execution","updated_at_epoch":'
                   +json.dumps(time.time(), allow_nan=False)+',"'+name+'":'+body+'}')
        atomic_write_text(self.paths[index], encoded)
        return (time.perf_counter()-started)*1000

    def health(self):
        with self._condition:
            return dict(enabled=True, writing=self._writing, pending=int(self._pending is not None),
                submitted=self._submitted, written=self._written, coalesced=self._coalesced,
                failures=self._failures, error=self._error, last_write_ms=round(self._last_ms, 3),
                queue_wait_ms=round(self._queue_wait_ms, 3),
                file_write_ms=dict(zip(('intents', 'state'), (round(value, 3) for value in self._file_ms))))

    def close(self):
        with self._condition:
            self._closing = True
            self._condition.notify_all()
        self._thread.join()  # Never release the process lock with a writer alive.
        self._file_pool.shutdown(wait=True)


class BundleWriter(DurableWriter):
    """One atomic commit contains BOTH intent identities and physical slots.

    execution_intents.json is a compatibility export. Recovery always uses
    the embedded intents while a bundle is present, including after a crash.
    """
    def __init__(self, *args, **kwargs):
        self._bundle_written = False
        self._exported = None
        super().__init__(*args, **kwargs)

    def _persist(self, snapshot):
        if self._bundle_written and [snapshot.intents, snapshot.state] == self._saved:
            return
        started = time.perf_counter()
        encoded = ('{"schema_version":1,"component":"execution","execution_bundle_version":1,'
                   '"updated_at_epoch":'+json.dumps(time.time(), allow_nan=False)
                   +',"intents":'+snapshot.intents+',"state":'+snapshot.state+'}')
        atomic_write_text(self.paths[1], encoded)
        self._saved[:] = snapshot.intents, snapshot.state
        self._bundle_written = True
        with self._condition:
            self._file_ms[:] = 0., (time.perf_counter()-started)*1000

    def export_intents(self):
        # Called by the owner only after a synchronous flush/after join, never
        # by the dispatch path. The coordinator never writes this export.
        body = self._saved[0]
        if self._bundle_written and body != self._exported:
            self._write_file(0, 'intents', body)
            self._exported = body

    def health(self):
        return dict(super().health(), atomic_bundle=True)

    def close(self):
        super().close()
        self.export_intents()


def embedded_intents(payload):
    """Return canonical intents or None for an old two-file ledger."""
    version = payload.get('execution_bundle_version')
    if version is None:
        return None
    if version != 1 or not isinstance(payload.get('intents'), dict) or not isinstance(payload.get('state'), dict):
        raise RuntimeError('invalid/unsupported execution bundle; retained without fallback')
    if any(not isinstance(payload['state'].get(name, {}), dict) for name in ('owned_positions', 'tracked_orders')):
        raise RuntimeError('invalid execution bundle slots; retained without fallback')
    return payload['intents']


_SLOT = struct.Struct('>8s16sQQ32s')
_MAGIC = b'V9711DS1'
_MAX_SNAPSHOT = 128*1024*1024


def store_paths(state_path):
    path = Path(state_path)
    return tuple(path.with_name(path.name+'.dispatch'+str(index)) for index in (0, 1))


def commit_snapshot_slot(handle, record):
    """Reuse one inactive slot; the other slot is never touched by this commit."""
    handle.seek(0)
    remaining = memoryview(record)
    while remaining:
        count = handle.write(remaining)
        if not count:
            raise OSError('short durable slot write')
        remaining = remaining[count:]
    os.fsync(handle.fileno())


def resolve_store(payload, state_path):
    if 'execution_journal_store' in payload:
        from .journal_store import resolve_journal
        return resolve_journal(payload, state_path)
    epoch = payload.get('execution_dispatch_store')
    if epoch is None:
        return payload
    try:
        expected = uuid.UUID(hex=epoch).bytes
    except (ValueError, TypeError, AttributeError) as exc:
        raise RuntimeError('invalid durable store epoch; no stale-ledger fallback') from exc
    candidates = []
    for path in store_paths(state_path):
        try:
            with path.open('rb') as handle:
                magic, actual_epoch, sequence, size, digest = _SLOT.unpack(handle.read(_SLOT.size))
                if magic != _MAGIC or actual_epoch != expected or not 0 < size <= _MAX_SNAPSHOT:
                    continue
                body = handle.read(size)
            if len(body) != size or hashlib.sha256(body).digest() != digest:
                continue
            value = json.loads(body)
            if embedded_intents(value) is not None:
                candidates.append((sequence, value))
        except (OSError, ValueError, struct.error):
            continue
    if not candidates:
        raise RuntimeError('no intact durable dispatch slot; retained without stale-ledger fallback')
    return max(candidates, key=lambda item: item[0])[1]


class SlotWriter(BundleWriter):
    """Persistent alternating slots avoid file creation/rename on each send.

    Only the inactive slot is changed. SHA-256/length/epoch reject torn writes;
    the highest intact sequence restores both exact intents and owned state.
    """
    def __init__(self, *args, **kwargs):
        self._handles = []
        self._epoch = uuid.uuid4()
        self._slot_sequence = 0
        self._last_body = None
        self._snapshot_bytes = 0
        super().__init__(*args, **kwargs)

    def _persist(self, snapshot):
        if self._bundle_written and [snapshot.intents, snapshot.state] == self._saved:
            return
        started = time.perf_counter()
        body = ('{"schema_version":1,"component":"execution","execution_bundle_version":1,'
                '"updated_at_epoch":'+json.dumps(time.time(), allow_nan=False)
                +',"intents":'+snapshot.intents+',"state":'+snapshot.state+'}')
        encoded = body.encode('utf-8')
        if len(encoded) > _MAX_SNAPSHOT:
            raise ValueError('durable dispatch snapshot exceeds 128MiB; no send')
        if not self._handles:
            # Checkpoint BEFORE reusing an older epoch's slots. A crash at any
            # point in initialization still has a complete standalone bundle.
            atomic_write_text(self.paths[1], body)
            opened = []
            try:
                for path in store_paths(self.paths[1]):
                    opened.append(path.open('w+b', buffering=0))
            except BaseException:
                for handle in opened:
                    handle.close()
                raise
            self._handles = opened
        sequence = self._slot_sequence+1
        record = _SLOT.pack(_MAGIC, self._epoch.bytes, sequence, len(encoded), hashlib.sha256(encoded).digest())+encoded
        commit_snapshot_slot(self._handles[sequence % 2], record)
        if not self._bundle_written:
            marker = body[:-1]+',"execution_dispatch_store":'+json.dumps(self._epoch.hex)+'}'
            atomic_write_text(self.paths[1], marker)
        self._slot_sequence = sequence
        self._saved[:] = snapshot.intents, snapshot.state
        self._last_body, self._snapshot_bytes = body, len(encoded)
        self._bundle_written = True
        with self._condition:
            self._file_ms[:] = 0., (time.perf_counter()-started)*1000

    def health(self):
        return dict(super().health(), persistent_slots=True, snapshot_bytes=self._snapshot_bytes,
                    commit_sequence=self._slot_sequence)

    def close(self):
        try:
            super().close()
            if self._last_body is not None:
                # The coordinator has exited. Export a standalone complete
                # bundle last; old slot files then cease to be authoritative.
                atomic_write_text(self.paths[1], self._last_body)
        finally:
            for handle in self._handles:
                handle.close()
