"""Latest-only detached display messages; no execution state is shared."""
import copy
import logging
import threading
import time

LOG = logging.getLogger('v9711_live_trade.terminal')


class BackgroundTerminal:
    def __init__(self, view):
        self.view = view
        self._condition = threading.Condition()
        self._pending = None
        self._closing = False
        self._next_submit = 0.
        self._thread = threading.Thread(target=self._loop, name='v9711-terminal', daemon=True)
        self._thread.start()

    def render(self, *, force=False, **fields):
        now = time.monotonic()
        if not force and now < self._next_submit:
            return False
        snapshot = copy.deepcopy(fields)
        with self._condition:
            if self._closing:
                return False
            self._pending = (force, snapshot)
            self._next_submit = now+max(.05, self.view.min_refresh_seconds)
            self._condition.notify_all()
        return True

    def _loop(self):
        while True:
            with self._condition:
                self._condition.wait_for(lambda: self._pending is not None or self._closing)
                if self._pending is None:
                    return
                force, fields = self._pending
                self._pending = None
            try:
                self.view.render(force=force, **fields)
            except Exception:
                LOG.exception('background display failed; execution continues')

    def close(self):
        with self._condition:
            self._closing = True
            self._condition.notify_all()
        self._thread.join()
