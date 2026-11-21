"""One ordered disk writer; immutable snapshots and positive send receipts.

Only the owner serializes state. At most one snapshot is writing and one is
pending. Replacing the pending snapshot shares its Future: a newer complete
snapshot covers older tickets only for identities actually present in it.
"""
from concurrent.futures import Future
from dataclasses import dataclass
import json
import logging
import threading
import time

from ..contracts import atomic_write_text

LOG = logging.getLogger('v9711_live_trade.durability')


@dataclass(frozen=True)
class Snapshot:
    intents: str
    state: str
    reservations: frozenset


@dataclass(frozen=True)
class Receipt:
    reservations: frozenset


class DurableWriter:
    def __init__(self, intent_path, state_path, *, saved=(None, None)):
        self.paths = (intent_path, state_path)
        self._saved = list(saved)
        self._condition = threading.Condition()
        self._pending = self._latest = self._latest_future = None
        self._closing = self._writing = False
        self._submitted = self._written = self._coalesced = self._failures = 0
        self._last_ms = 0.
        self._error = ''
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
            try:
                for index, (name, body) in enumerate((('intents', snapshot.intents), ('state', snapshot.state))):
                    if body != self._saved[index]:
                        encoded = ('{"schema_version":1,"component":"execution","updated_at_epoch":'
                                   +json.dumps(time.time(), allow_nan=False)+',"'+name+'":'+body+'}')
                        atomic_write_text(self.paths[index], encoded)
                        # Cache only AFTER fsync and replace succeed, separately
                        # for each file. A partial pair failure must retry state.
                        self._saved[index] = body
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
                future.set_result(Receipt(snapshot.reservations))
            finally:
                with self._condition:
                    self._writing = False
                    self._last_ms = (time.perf_counter()-started)*1000

    def health(self):
        with self._condition:
            return dict(enabled=True, writing=self._writing, pending=int(self._pending is not None),
                submitted=self._submitted, written=self._written, coalesced=self._coalesced,
                failures=self._failures, error=self._error, last_write_ms=round(self._last_ms, 3))

    def close(self):
        with self._condition:
            self._closing = True
            self._condition.notify_all()
        self._thread.join()  # Never release the process lock with a writer alive.
