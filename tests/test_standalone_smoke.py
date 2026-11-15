from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from collectors.ticker_ws import SnapshotState
from v9711_live import config as cfg
from v9711_live.market_data import PriceReader


class StandaloneSmokeTests(unittest.TestCase):
    def test_paths_are_inside_deployment(self):
        root = Path(__file__).resolve().parents[1]
        self.assertEqual(cfg.ROOT, root.resolve())
        self.assertTrue(str(cfg.STATE_DIR).startswith(str(root.resolve())))
        self.assertFalse("deliverables" in str(cfg.ROOT))

    def test_ws_snapshot_matches_signal_reader_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "latest.json"
            state = SnapshotState(["ABCUSDT"], path, heartbeat=5)
            state.update("ABCUSDT", "1.2300", 1_700_000_000_000)
            self.assertTrue(state.publish_if_due())
            payload = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(payload["status"], "ok")
            reader = PriceReader(path, max_age_seconds=999999)
            snapshot = reader.read()
            self.assertIsNotNone(snapshot)
            self.assertAlmostEqual(snapshot.prices["ABCUSDT"].price, 1.23)

    def test_local_symbol_file_is_nonempty(self):
        symbols = [line.strip() for line in cfg.SYMBOLS_FILE.read_text(encoding="utf-8").splitlines()
                   if line.strip() and not line.startswith("#")]
        self.assertGreater(len(symbols), 100)
        self.assertEqual(symbols, sorted(set(symbols)))


if __name__ == "__main__":
    unittest.main()

