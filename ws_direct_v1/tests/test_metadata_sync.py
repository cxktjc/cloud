import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from v9711_live.execution.metadata_sync import MetadataSync

class MetadataSyncTests(unittest.TestCase):
    def test_refresh_failure_retains_cache(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'filters.json'
            path.write_text('existing-cache')
            with patch('v9711_live.execution.metadata_sync.refresh', side_effect=OSError('offline')):
                self.assertFalse(MetadataSync(path).refresh_once())
            self.assertEqual(path.read_text(), 'existing-cache')

    def test_start_does_not_wait_for_network(self):
        entered, release = threading.Event(), threading.Event()
        def slow(output):
            entered.set()
            release.wait(3)
            return {'symbols': {'TESTUSDT': {}}}
        sync = MetadataSync(Path('unused'))
        with patch('v9711_live.execution.metadata_sync.refresh', side_effect=slow):
            try:
                sync.start()
                self.assertTrue(entered.wait(1))
                self.assertTrue(sync.thread.is_alive())
                self.assertFalse(release.is_set())
            finally:
                sync.stop_event.set()
                release.set()
                sync.stop()

if __name__ == '__main__':
    unittest.main()
