"""Single-writer latest feedback snapshots; never owns execution state."""
import copy
import logging
import threading
import time

from ..contracts import atomic_write_json

LOG = logging.getLogger('v9711_live_trade.feedback')


class FeedbackWriter:
    def __init__(self, path):
        self.path = path
        self._condition = threading.Condition()
        self._pending = None
        self._closing = self._writing = False
        self._submitted = self._written = self._failures = 0
        self._error = ''
        self._last_ms = 0.
        self._thread = threading.Thread(target=self._loop, name='v9711-feedback', daemon=True)
        self._thread.start()

    def submit(self, payload):
        # Detach nested timing/diagnostic rows while the owner still owns them.
        snapshot = copy.deepcopy(payload)
        with self._condition:
            if self._closing:
                raise RuntimeError('feedback writer is closed')
            self._submitted += 1
            self._pending = (self._submitted, snapshot)
            self._condition.notify_all()

    def health(self):
        with self._condition:
            return dict(enabled=True, pending=int(self._pending is not None)+int(self._writing),
                        submitted=self._submitted, written_revision=self._written,
                        failures=self._failures, error=self._error, last_write_ms=round(self._last_ms, 3))

    def _loop(self):
        while True:
            with self._condition:
                self._condition.wait_for(lambda: self._pending is not None or self._closing)
                if self._pending is None:
                    return
                sequence, payload = self._pending
                self._pending = None
                self._writing = True
            started, failed = time.perf_counter(), False
            try:
                atomic_write_json(self.path, payload)
            except Exception as exc:
                failed = True
                with self._condition:
                    self._failures += 1
                    message = f'{type(exc).__name__}: {exc}'
                    if message != self._error:
                        LOG.warning('feedback background write failed; latest snapshot will retry: %s', message)
                    self._error = message
                    if not self._closing and self._pending is None:
                        self._pending = (sequence, payload)
            else:
                with self._condition:
                    self._written = sequence
                    self._error = ''
            finally:
                with self._condition:
                    self._writing = False
                    self._last_ms = (time.perf_counter()-started)*1000
                    if failed and not self._closing:
                        self._condition.wait(timeout=.25)

    def close(self):
        with self._condition:
            self._closing = True
            self._condition.notify_all()
        self._thread.join()  # flush latest snapshot before releasing the process lock
