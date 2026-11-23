"""Ordered incremental dispatch journal and immutable background checkpoints.

The owner encodes only changed JSON branches. No mutable owner objects cross
the writer boundary. A positive receipt follows an actual fsync. Checkpoints
use copy-on-write roots so encoding never races with journal application.
"""
from collections import deque
from concurrent.futures import Future
from dataclasses import dataclass, field
import hashlib
import json
import logging
import os
from pathlib import Path
import struct
import threading
import time
import uuid

from ..contracts import atomic_write_text
from .durable_writer import Receipt, embedded_intents

_FRAME = struct.Struct('>8s16sQQ32s')
_SEGMENT = struct.Struct('>8s16sQ')
_MAGIC = b'V9711JL1'
_SEG_MAGIC = b'V9711JG1'
_MAX_RECORD = 128 * 1024 * 1024
LOG = logging.getLogger('v9711_live_trade.durability')


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def changes(old, new, path=()):
    """C-level equality skips unchanged branches, including large audit maps.

    Rows of state maps and individual intents are the largest replacement
    unit. Nested lists and scalar values are immutable JSON replacements.
    This deliberately observes ordinary dict mutations and retained aliases;
    it does not require callers to use special tracked container classes.
    """
    if old == new:
        return []
    cutoff = 2 if path and path[0] == 'intents' else 3
    if isinstance(old, dict) and isinstance(new, dict) and len(path) < cutoff:
        result = [[list(path + (key,))] for key in old.keys() - new.keys()]
        for key, value in new.items():
            if key not in old:
                result.append([list(path + (key,)), value])
            else:
                result.extend(changes(old[key], value, path + (key,)))
        return result
    return [[list(path), new]]


def apply_changes(root, operations):
    """Copy-on-write only the dictionary paths touched by a transaction."""
    result = root
    for operation in operations:
        if not isinstance(operation, list) or len(operation) not in (1, 2):
            raise ValueError('invalid journal operation')
        path = operation[0]
        if (not isinstance(path, list) or not path or path[0] not in ('intents', 'state')
                or not all(isinstance(key, str) for key in path)):
            raise ValueError('invalid journal path')
        result = dict(result)
        target = result
        for key in path[:-1]:
            child = target.get(key)
            if not isinstance(child, dict):
                raise ValueError('journal parent is not a dictionary')
            target[key] = dict(child)
            target = target[key]
        if len(operation) == 1:
            if path[-1] not in target:
                raise ValueError('journal deletion has no predecessor')
            del target[path[-1]]
        else:
            target[path[-1]] = operation[1]
    return result


@dataclass(frozen=True)
class Delta:
    body: str
    reservations: frozenset
    submitted_ns: int = field(default_factory=time.perf_counter_ns, compare=False)


class DeltaCapture:
    def __init__(self, intents, state):
        self.root = json.loads(encode(dict(intents=intents, state=state)))

    def prepare(self, intents, state, reservations):
        operations = changes(self.root, dict(intents=intents, state=state))
        body = encode(operations)
        # Decode ONLY the changed branches, retaining immutable unchanged ones.
        root = apply_changes(self.root, json.loads(body)) if operations else self.root
        return Delta(body, frozenset(reservations)), root


def journal_prefix(state_path, epoch):
    return str(Path(state_path)) + '.journal.' + epoch


def artifacts(state_path):
    path = Path(state_path)
    return tuple(sorted(path.parent.glob(path.name + '.journal.*')))


def _bundle(root):
    return dict(schema_version=1, component='execution', execution_bundle_version=1,
                intents=root['intents'], state=root['state'])


def _checkpoint(path, epoch, sequence, root):
    body = encode(_bundle(root))
    envelope = dict(journal_checkpoint=1, epoch=epoch, sequence=sequence,
                    sha256=hashlib.sha256(body.encode()).hexdigest(), body=body)
    atomic_write_text(path, encode(envelope))
    sync_directory(Path(path).parent)


def sync_directory(path):
    # Linux requires the directory entry itself durable before we can drop
    # preceding segments or acknowledge the first append to a new segment.
    if os.name != 'nt':
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def _load_checkpoint(path, epoch):
    value = json.loads(path.read_text(encoding='utf-8-sig'))
    if (value.get('journal_checkpoint') != 1 or value.get('epoch') != epoch
            or not isinstance(value.get('sequence'), int) or value['sequence'] < 0
            or not isinstance(value.get('body'), str)
            or hashlib.sha256(value['body'].encode()).hexdigest() != value.get('sha256')):
        raise ValueError('invalid journal checkpoint')
    root = json.loads(value['body'])
    if embedded_intents(root) is None:
        raise ValueError('checkpoint has no canonical intent/slot pair')
    return value['sequence'], root


def resolve_journal(payload, state_path):
    epoch = payload.get('execution_journal_store')
    try:
        epoch_bytes = uuid.UUID(hex=epoch).bytes
    except (ValueError, TypeError, AttributeError) as exc:
        raise RuntimeError('invalid journal epoch; no stale-ledger fallback') from exc
    prefix = journal_prefix(state_path, epoch)
    checkpoint = Path(prefix + '.checkpoint')
    # A maintenance reader can race with a newer checkpoint and segment GC.
    # Retry from that NEW checkpoint; never fall back to the seed marker.
    for attempt in range(3):
        try:
            sequence, root = _load_checkpoint(checkpoint, epoch)
            files = sorted(p for p in Path(state_path).parent.glob(Path(prefix).name + '.[0-9]*')
                           if p.suffix[1:].isdigit())
            if not files:
                raise ValueError('journal has no active segment')
            for index, path in enumerate(files):
                with path.open('rb') as handle:
                    header = handle.read(_SEGMENT.size)
                    magic, actual_epoch, first = _SEGMENT.unpack(header)
                    if magic != _SEG_MAGIC or actual_epoch != epoch_bytes or first != int(path.suffix[1:]):
                        raise ValueError('invalid journal segment header')
                    last = index == len(files) - 1
                    frame_sequence = first
                    while True:
                        raw = handle.read(_FRAME.size)
                        if not raw:
                            break
                        if len(raw) < _FRAME.size:
                            if last:
                                break  # interrupted, never acknowledged tail
                            raise ValueError('truncated sealed journal header')
                        magic, actual_epoch, current, size, digest = _FRAME.unpack(raw)
                        if magic != _MAGIC or actual_epoch != epoch_bytes or not 0 < size <= _MAX_RECORD:
                            raise ValueError('invalid journal frame header')
                        if current != frame_sequence:
                            raise ValueError('non-contiguous journal segment')
                        body = handle.read(size)
                        if len(body) != size:
                            if last:
                                break
                            raise ValueError('truncated sealed journal frame')
                        if hashlib.sha256(body).digest() != digest:
                            raise ValueError('journal checksum mismatch')
                        frame_sequence += 1
                        if current <= sequence:
                            continue  # checkpoint already contains this prefix
                        if current != sequence + 1:
                            raise ValueError('journal sequence gap')
                        root = apply_changes(root, json.loads(body))
                        sequence = current
            if embedded_intents(root) is None:
                raise ValueError('invalid reconstructed journal bundle')
            return root
        except (OSError, ValueError, struct.error, KeyError) as exc:
            if attempt < 2:
                continue
            raise RuntimeError('invalid execution journal; retained without stale-ledger fallback') from exc


def commit_journal(handle, records):
    remaining = memoryview(records)
    while remaining:
        count = handle.write(remaining)
        if not count:
            raise OSError('short durable journal write')
        remaining = remaining[count:]
    os.fsync(handle.fileno())


class JournalWriter:
    def __init__(self, intent_path, state_path, *, initial, checkpoint_every=256,
                 segment_bytes=4 * 1024 * 1024, max_pending=1024,
                 max_journal_bytes=128 * 1024 * 1024):
        self.paths = (Path(intent_path), Path(state_path))
        self._epoch = uuid.uuid4().hex
        self._epoch_bytes = uuid.UUID(hex=self._epoch).bytes
        self._prefix = journal_prefix(state_path, self._epoch)
        self._checkpoint_path = Path(self._prefix + '.checkpoint')
        self._condition = threading.Condition()
        self._checkpoint_event = threading.Event()
        self._pending = deque()
        self._sealed = deque()
        self._closing = self._writing = False
        self._checkpoint_pending = None
        self._checkpoint_closing = False
        self._checkpoint_every = checkpoint_every
        self._segment_bytes = segment_bytes
        self._max_pending = max_pending
        self._max_journal_bytes = max_journal_bytes
        self._retained_bytes = 0
        self._root = json.loads(encode(initial))
        self._sequence = self._checkpoint_sequence = 0
        self._submitted = self._written = self._coalesced = self._failures = 0
        self._last_ms = self._queue_wait_ms = self._record_bytes = self._checkpoint_ms = 0.
        self._error = self._checkpoint_error = ''
        self._latest = self._latest_future = None
        self._fatal = None
        self._handle = None
        # Startup transaction: standalone export remains authoritative until
        # BOTH the initial checkpoint and fsynced segment exist.
        atomic_write_text(self.paths[1], encode(_bundle(self._root)))
        atomic_write_text(self.paths[0], encode(dict(schema_version=1,
            component='execution', intents=self._root['intents'])))
        _checkpoint(self._checkpoint_path, self._epoch, 0, self._root)
        self._open_segment(1)
        # An old binary must reject this marker, not mistake its seed for the
        # latest book. The resolver returns a normal version-1 complete bundle.
        marker = dict(_bundle(self._root), execution_bundle_version=2,
                      execution_journal_store=self._epoch)
        try:
            atomic_write_text(self.paths[1], encode(marker))
            sync_directory(self.paths[1].parent)
        except BaseException:
            self._handle.close()
            raise
        self._checkpoint_thread = threading.Thread(target=self._checkpoint_loop,
            name='v9711-journal-checkpoint', daemon=True)
        self._thread = threading.Thread(target=self._loop, name='v9711-journal-dispatch', daemon=True)
        self._checkpoint_thread.start()
        self._thread.start()

    def _open_segment(self, first):
        path = Path(self._prefix + f'.{first:020d}')
        temporary = path.with_name(path.name + '.creating')
        try:
            with temporary.open('xb', buffering=0) as handle:
                commit_journal(handle, _SEGMENT.pack(_SEG_MAGIC, self._epoch_bytes, first))
            os.replace(temporary, path)
            sync_directory(path.parent)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        handle = path.open('r+b', buffering=0)
        handle.seek(0, os.SEEK_END)
        self._handle, self._segment_path = handle, path
        with self._condition:
            self._retained_bytes += _SEGMENT.size

    def submit(self, delta):
        with self._condition:
            if self._closing:
                raise RuntimeError('journal writer closed; no send')
            if self._fatal is not None:
                future = Future()
                future.set_exception(self._fatal)
                return future
            if delta.body == '[]' and self._latest_future is not None:
                # The preceding transaction already contains this exact state.
                return self._latest_future
            if len(self._pending) >= self._max_pending:
                raise RuntimeError('journal writer queue full; no send')
            future = Future()
            self._submitted += 1
            self._pending.append((delta, future))
            self._latest, self._latest_future = delta, future
            self._condition.notify()
            return future

    def _loop(self):
        while True:
            with self._condition:
                self._condition.wait_for(lambda: self._pending or self._closing)
                if not self._pending:
                    return
                batch = tuple(self._pending)
                self._pending.clear()
                self._writing = True
            started = time.perf_counter()
            started_ns, started_ms = time.monotonic_ns(), time.time()*1000
            try:
                root, sequence, frames = self._root, self._sequence, []
                for delta, _ in batch:
                    body = delta.body.encode('utf-8')
                    if not 0 < len(body) <= _MAX_RECORD:
                        raise ValueError('journal delta exceeds limit')
                    root = apply_changes(root, json.loads(body))
                    sequence += 1
                    frames.append(_FRAME.pack(_MAGIC, self._epoch_bytes, sequence,
                        len(body), hashlib.sha256(body).digest()) + body)
                record = b''.join(frames)
                with self._condition:
                    if self._retained_bytes + len(record) > self._max_journal_bytes:
                        raise OSError('journal retention limit reached; checkpoint must recover before restart')
                commit_journal(self._handle, record)
                # Publish only AFTER durable commit; checkpoint sees immutable roots.
                self._root, self._sequence = root, sequence
                self._record_bytes = len(record)
                with self._condition:
                    self._retained_bytes += len(record)
                self._written += len(batch)
                self._coalesced += len(batch) - 1
                receipt = Receipt(batch[-1][0].reservations, time.monotonic_ns(), time.time()*1000,
                                  started_ns, started_ms)
                for _, future in batch:
                    future.set_result(receipt)
                if sequence - self._checkpoint_sequence >= self._checkpoint_every:
                    with self._condition:
                        self._checkpoint_pending = sequence, root
                    self._checkpoint_event.set()
                if self._handle.tell() >= self._segment_bytes:
                    previous_handle, previous_path = self._handle, self._segment_path
                    self._open_segment(sequence + 1)
                    previous_handle.close()
                    with self._condition:
                        self._sealed.append((previous_path, sequence))
            except Exception as exc:
                with self._condition:
                    self._fatal = exc
                    self._failures += 1
                    self._error = f'{type(exc).__name__}: {exc}'
                    failed = tuple(self._pending)
                    self._pending.clear()
                LOG.error('journal commit failed; writer stopped and dependent orders will NOT send: %s', self._error)
                for _, future in (*batch, *failed):
                    if not future.done():
                        future.set_exception(exc)
                return  # No appends after an uncertain or partial disk write.
            finally:
                with self._condition:
                    self._writing = False
                    self._last_ms = (time.perf_counter()-started)*1000
                    self._queue_wait_ms = (started_ns-batch[0][0].submitted_ns)/1e6

    def _checkpoint_loop(self):
        while True:
            with self._condition:
                if self._checkpoint_closing and self._checkpoint_pending is None:
                    return
            self._checkpoint_event.wait()
            with self._condition:
                self._checkpoint_event.clear()
                if self._checkpoint_pending is None:
                    if self._checkpoint_closing:
                        return
                    continue
                sequence, root = self._checkpoint_pending
                self._checkpoint_pending = None
            started = time.perf_counter()
            try:
                _checkpoint(self._checkpoint_path, self._epoch, sequence, root)
                with self._condition:
                    self._checkpoint_sequence = sequence
                    obsolete = []
                    while self._sealed and self._sealed[0][1] <= sequence:
                        obsolete.append(self._sealed.popleft()[0])
                for path in obsolete:
                    size = path.stat().st_size
                    path.unlink(missing_ok=True)
                    with self._condition:
                        self._retained_bytes -= size
                self._checkpoint_error = ''
            except Exception as exc:
                # Keep all segments when a checkpoint fails. Existing WAL
                # receipts remain durable; expose failure separately.
                message = f'{type(exc).__name__}: {exc}'
                if message != self._checkpoint_error:
                    LOG.warning('journal checkpoint failed; segments retained: %s', message)
                self._checkpoint_error = message
            self._checkpoint_ms = (time.perf_counter()-started)*1000

    def health(self):
        with self._condition:
            return dict(enabled=True, incremental_journal=True, atomic_bundle=True,
                writing=self._writing, pending=len(self._pending), submitted=self._submitted,
                written=self._written, coalesced=self._coalesced, failures=self._failures,
                error=self._error, last_write_ms=round(self._last_ms, 3),
                queue_wait_ms=round(self._queue_wait_ms, 3), record_bytes=self._record_bytes,
                retained_bytes=self._retained_bytes,
                commit_sequence=self._sequence, checkpoint_sequence=self._checkpoint_sequence,
                checkpoint_ms=round(self._checkpoint_ms, 3), checkpoint_error=self._checkpoint_error,
                file_write_ms=dict(intents=0., state=round(self._last_ms, 3)))

    def export_intents(self):
        # Only after a barrier/join; take one immutable committed root.
        atomic_write_text(self.paths[0], encode(dict(schema_version=1,
            component='execution', intents=self._root['intents'])))

    def close(self):
        with self._condition:
            self._closing = True
            self._condition.notify()
        self._thread.join()
        with self._condition:
            self._checkpoint_closing = True
        self._checkpoint_event.set()
        self._checkpoint_thread.join()
        self._handle.close()
        # Failed writes may have an unacknowledged complete tail. Preserve it
        # for conservative recovery instead of exporting a stale root.
        if self._fatal is None:
            self.export_intents()
            atomic_write_text(self.paths[1], encode(_bundle(self._root)))
            sync_directory(self.paths[1].parent)
            for path in artifacts(self.paths[1]):
                if path.name.startswith(Path(self._prefix).name + '.'):
                    path.unlink(missing_ok=True)
