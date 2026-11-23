import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from v9711_live.local_poll import CachedJsonFile
from v9711_live.execution.background_signal import BackgroundSignal


class BackgroundSignalTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)/'signal.json'
        self.path.write_text('{"value":1}')
        self.source = CachedJsonFile(self.path)
        self.wake = threading.Event()
        self.reader = BackgroundSignal(self.source, owner_wake=self.wake.set, interval=.001, max_age=.05)
        self.addCleanup(self.reader.close)
        self.until(lambda: self.reader.healthy())

    def until(self, predicate):
        end = time.perf_counter()+2
        while time.perf_counter() < end:
            if predicate():
                return
            threading.Event().wait(.001)
        self.fail('signal publication did not arrive')

    def test_owner_read_never_performs_file_io_and_pairs_its_consumed_revision(self):
        owner = threading.get_ident()
        original = self.source.read
        def background_only():
            self.assertNotEqual(threading.get_ident(), owner)
            return original()
        with patch.object(self.source, 'read', side_effect=background_only):
            for _ in range(1000):
                self.assertEqual(self.reader.read(), {'value': 1})
        revision = self.reader.revision
        self.path.write_text('{"value":222}')
        self.until(lambda: (self.reader.read() or {}).get('value') == 222)
        self.assertGreater(self.reader.revision, revision)

    def test_missing_invalid_and_restored_file_publish_complete_revisions_and_wake_owner(self):
        self.wake.clear()
        self.path.write_text('invalid JSON')
        self.until(lambda: self.reader.read() is None)
        self.assertTrue(self.wake.is_set())
        self.assertFalse(self.reader.healthy())
        self.path.unlink()
        self.until(lambda: self.reader.read() is None and self.source.error.startswith('local JSON'))
        self.path.write_text('{"value":3}')
        self.until(lambda: self.reader.read() == {'value': 3})

    def test_blocked_read_does_not_block_owner_and_expired_publication_fails_closed(self):
        started, release = threading.Event(), threading.Event()
        original = self.source.read
        def blocked():
            started.set()
            release.wait(2)
            return original()
        try:
            with patch.object(self.source, 'read', side_effect=blocked):
                self.assertTrue(started.wait(1))
                self.assertEqual(self.reader.read(), {'value': 1})
                threading.Event().wait(.06)
                self.assertIsNone(self.reader.read())
                self.assertFalse(self.reader.healthy())
        finally:
            release.set()
        self.until(lambda: self.reader.healthy())

    def test_close_stops_publication_and_disallows_cached_signal(self):
        self.reader.close()
        self.assertFalse(self.reader.healthy())
        self.assertIsNone(self.reader.read())


if __name__ == '__main__':
    unittest.main()
