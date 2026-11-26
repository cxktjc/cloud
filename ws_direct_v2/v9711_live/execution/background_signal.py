"""Publish complete signal reads; quote owner never stats/parses a file."""
import threading
import time


class BackgroundSignal:
    def __init__(self, source, *, owner_wake=None, interval=.005, max_age=.1, name='v9711-signal-reader'):
        self.source = source
        self.path = source.path
        self.revision = 0  # Revision consumed by the owner, paired with read().
        self._wake = owner_wake
        self._interval, self._max_age = interval, max_age
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._published = (None, 0, None)
        self._thread = threading.Thread(target=self._loop, name=name, daemon=True)
        self._thread.start()

    def read(self):
        with self._lock:
            payload, revision, checked = self._published
        self.revision = revision
        if self._stop.is_set() or checked is None or time.monotonic()-checked > self._max_age:
            return None
        return payload  # Read-only, just like CachedJsonFile's contract.

    def healthy(self):
        with self._lock:
            payload, _, checked = self._published
        return (not self._stop.is_set() and payload is not None and checked is not None
                and time.monotonic()-checked <= self._max_age)

    def _loop(self):
        previous = None
        while not self._stop.is_set():
            try:
                payload = self.source.read()
            except Exception:
                payload = None
            revision = self.source.revision
            with self._lock:
                self._published = payload, revision, time.monotonic()
            stamp = revision, payload is not None
            if stamp != previous and self._wake is not None:
                self._wake()
            previous = stamp
            self._stop.wait(self._interval)

    def close(self):
        self._stop.set()
        self._thread.join()
