from __future__ import annotations

import json
import tempfile
import unittest
from unittest.mock import patch, Mock
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from collectors.ticker_ws import SnapshotState
from collectors import binance_network
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
            state = SnapshotState(["ABCUSDT"], path, heartbeat=5, total_groups=1)
            state.group_connected(1)
            state.update("ABCUSDT", "1.2300", 1_700_000_000_000)
            self.assertTrue(state.publish_if_due())
            payload = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(payload["status"], "ok")
            reader = PriceReader(path, max_age_seconds=999999)
            snapshot = reader.read()
            self.assertIsNotNone(snapshot)
            self.assertAlmostEqual(snapshot.prices["ABCUSDT"].price, 1.23)

    def test_partial_ws_group_is_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "latest.json"
            state = SnapshotState(["AUSDT", "BUSDT"], path, heartbeat=5, total_groups=2)
            state.group_connected(1)
            state.group_connected(2)
            state.update("AUSDT", "1", 1_700_000_000_000)
            state.update("BUSDT", "2", 1_700_000_000_000)
            state.group_failed(2, "test disconnect")
            self.assertFalse(state.publish_if_due())
            self.assertEqual(json.loads(path.read_text(encoding="utf-8"))["status"], "error")

    def test_binance_network_is_direct_only(self):
        from v9711_live.exchange import binance_network as trade_network
        poisoned = {"BINANCE_PROXY_URL": "http://invalid:1", "BINANCE_PROXY": "http://invalid:2",
                    "HTTP_PROXY": "http://invalid:3", "HTTPS_PROXY": "http://invalid:4",
                    "ALL_PROXY": "http://invalid:5", "https_proxy": "http://invalid:6"}
        for module in (binance_network, trade_network):
            with self.subTest(module=module.__name__), patch.dict(os.environ, poisoned):
                self.assertEqual(module.websocket_connect_kwargs(), {"proxy": None})
                opener = Mock()
                with patch.object(module.urllib.request, "ProxyHandler") as handler, \
                     patch.object(module.urllib.request, "build_opener", return_value=opener):
                    module.urlopen("https://example.invalid", timeout=7)
                    handler.assert_called_once_with({})
                    opener.open.assert_called_once_with("https://example.invalid", timeout=7)

    def test_local_symbol_file_is_nonempty(self):
        symbols = [line.strip() for line in cfg.SYMBOLS_FILE.read_text(encoding="utf-8").splitlines()
                   if line.strip() and not line.startswith("#")]
        self.assertGreater(len(symbols), 100)
        self.assertEqual(symbols, sorted(set(symbols)))


if __name__ == "__main__":
    unittest.main()

