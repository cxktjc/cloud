from __future__ import annotations
import sys, tempfile, unittest, time
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from v9711_live import config as cfg
from v9711_live.input_diagnostics import status_reason, check_inputs
from v9711_live.market_data import KlineStatus, StatusReader
from collectors.kline_status import write_3m_status
from collectors import binance_futures_3m_ws

class InputPathsTests(unittest.TestCase):
    def test_server_templates_use_cloud_checkout(self):
        root=Path(__file__).resolve().parents[1]
        prefix="/opt/v9711-live/cloud"
        for service in (root/"deploy/systemd").glob("*.service"):
            text=service.read_text(encoding="utf-8")
            self.assertIn("WorkingDirectory="+prefix, text)
            self.assertIn("EnvironmentFile="+prefix+"/.env", text)
            self.assertIn("ExecStart="+prefix+"/.venv/bin/python", text)
            self.assertNotIn("/cloud/cloud", text)
        template=(root/".env.example").read_text(encoding="utf-8")
        self.assertIn("V9711_LIVE_3M_STATUS_FILE="+prefix+"/var/klines_3m_status.json", template)

    def test_collector_default_matches_signal_path(self):
        import os
        env=dict(os.environ); env.pop("KLINE_3M_STATUS_FILE",None)
        with patch.dict(os.environ,env,clear=True), patch.object(sys,"argv",["collector"]):
            self.assertEqual(Path(binance_futures_3m_ws.parse_args().status_file).resolve(),cfg.STATUS_FILE.resolve())

    def test_missing_status_diagnostic_is_read_only(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)
            with patch.object(cfg,"STATUS_FILE",path/"missing-status.json"), patch.object(cfg,"PRICE_FILE",path/"missing-price.json"):
                result=check_inputs()
                self.assertFalse(result["status"]["exists"])
                self.assertNotEqual(result["status"]["reason"],"ok")
                self.assertFalse(result["mysql"]["queried"])
                self.assertEqual(list(path.iterdir()),[])

    def test_published_bar_status_and_expiry(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"status.json"
            now=time.time(); current=int(now*1000)//cfg.BAR_MS*cfg.BAR_MS
            write_3m_status(open_time_ms=current-cfg.BAR_MS,symbols=["BTCUSDT"],row_count=1,source="test",path=path)
            status=StatusReader(path).read()
            self.assertEqual(status_reason(status,now=now),"ok")
            self.assertIn("expired",status_reason(status,now=current/1000+cfg.BAR_MS/1000))

if __name__ == "__main__": unittest.main()
