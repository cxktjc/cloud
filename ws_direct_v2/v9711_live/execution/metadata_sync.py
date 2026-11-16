"""Refresh public precision filters outside the tick/decision path."""
import logging
import threading
from scripts.refresh_exchange_info import refresh

LOG = logging.getLogger('v9711.ws_direct.filters')

class MetadataSync:
    def __init__(self, output):
        self.output = output
        self.stop_event = threading.Event()
        self.thread = None

    def refresh_once(self):
        try:
            payload = refresh(self.output)
            LOG.info('precision filters ready symbols=%s', len(payload['symbols']))
            return True
        except Exception as exc:
            LOG.warning('precision refresh failed; retain valid cache, retry in 60s: %s', exc)
            return False

    def start(self):
        def run():
            while not self.stop_event.is_set():
                success = self.refresh_once()
                if self.stop_event.wait(21600 if success else 60):
                    break
        self.thread = threading.Thread(target=run, name='ws-filter-sync', daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=0.2)
