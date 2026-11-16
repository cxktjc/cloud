from __future__ import annotations
import asyncio
import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from collectors import ticker_ws as ticker


class DataReadinessTests(unittest.TestCase):
    def test_handshake_is_not_ready(self):
        with tempfile.TemporaryDirectory() as directory:
            state = ticker.SnapshotState(["BTCUSDT"], Path(directory)/"latest.json", 5, 1)
            state.group_connected(1)
            state.update("BTCUSDT", "100", int(time.time()*1000))
            self.assertFalse(state.publish_if_due())
            self.assertEqual(state.ready_groups, set())

    def test_heartbeat_cannot_refresh_market_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"latest.json"
            state = ticker.SnapshotState(["BTCUSDT"], path, 5, 1)
            state.update("BTCUSDT", "100", int(time.time()*1000))
            with patch.object(ticker.time, "time", return_value=1000), patch.object(ticker.time, "monotonic", return_value=100):
                state.group_received(1)
                self.assertTrue(state.publish_if_due(force=True))
            with patch.object(ticker.time, "time", return_value=1005), patch.object(ticker.time, "monotonic", return_value=105):
                self.assertTrue(state.publish_if_due(force=True))
                self.assertEqual(json.loads(path.read_text())["last_success_epoch"], 1000)
            with patch.object(ticker.time, "time", return_value=1031), patch.object(ticker.time, "monotonic", return_value=131):
                self.assertFalse(state.publish_if_due(force=True))
                payload = json.loads(path.read_text())
                self.assertEqual(payload["status"], "error")
                self.assertEqual(payload["last_success_epoch"], 1000)

    def test_unicode_stream_and_custom_endpoint(self):
        from urllib.parse import urlparse, parse_qs
        self.assertEqual(ticker.endpoint_candidates("wss://example.invalid/stream"), ("wss://example.invalid/stream",))
        symbol = "\u5e01\u5b89\u4eba\u751fUSDT"
        url = ticker.stream_url([symbol], ticker.WS_BASE)
        self.assertEqual(parse_qs(urlparse(url).query)["streams"], [symbol.lower()+"@aggTrade"])


class EndpointRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_silent_handshake_switches_endpoint_until_real_trade(self):
        with tempfile.TemporaryDirectory() as directory:
            state = ticker.SnapshotState(["BTCUSDT"], Path(directory)/"latest.json", 5, 1)
            stop = asyncio.Event()
            urls = []
            class Socket:
                def __init__(self, index): self.index=index
                async def recv(self):
                    if self.index == 1:
                        await asyncio.sleep(100)
                    stop.set()
                    return json.dumps({"data":{"e":"aggTrade", "s":"BTCUSDT", "p":"100", "T":int(time.time()*1000)}})
            class Connection:
                def __init__(self, index): self.index=index
                async def __aenter__(self): return Socket(self.index)
                async def __aexit__(self,*args): pass
            def connect(url, **kwargs):
                self.assertEqual(kwargs["proxy"], None)
                urls.append(url)
                return Connection(len(urls))
            # No real network; shorten the backoff without mocking asyncio.sleep.
            with patch.object(ticker.websockets,"connect",side_effect=connect), patch.object(ticker,"FIRST_DATA_TIMEOUT",0.01):
                await ticker.consume_group(["BTCUSDT"],state,base=ticker.WS_BASE,ping_interval=30,
                    ping_timeout=20,read_timeout=30,reconnect_before=3600,stop=stop,group_index=1)
            self.assertEqual(len(urls),2)
            self.assertIn("/market/stream?", urls[0])
            self.assertIn("/public/stream?", urls[1])
            self.assertEqual(state.ready_groups,{1})
            self.assertTrue(state.publish_if_due())

if __name__ == "__main__":
    unittest.main()
