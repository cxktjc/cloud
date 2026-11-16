from __future__ import annotations
import sys, tempfile, unittest, time
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from v9711_live import config as cfg
from v9711_live.input_diagnostics import status_reason, check_inputs
from v9711_live.market_data import KlineStatus, StatusReader
from collectors.kline_status import write_3m_status

class InputPathsTests(unittest.TestCase):
    def test_server_templates_use_cloud_checkout(self):
        root=Path(__file__).resolve().parents[1]
        prefix="/opt/v9711-live/cloud/ws_direct_v2"
        for service in (root/"deploy/systemd").glob("*.service"):
            text=service.read_text(encoding="utf-8")
            self.assertIn("WorkingDirectory="+prefix, text)
            self.assertIn("EnvironmentFile="+prefix+"/.env", text)
            self.assertIn("ExecStart=/opt/v9711-live/cloud/.venv/bin/python", text)
            self.assertNotIn("/cloud/cloud", text)
        template=(root/".env.example").read_text(encoding="utf-8")
        self.assertNotIn("V9711_LIVE_3M_STATUS_FILE=", template)

    def _read_config(self, overrides=None, args=()):
        import json, os, subprocess
        env = {k: v for k, v in os.environ.items()
               if not k.startswith(("V9711_", "KLINE_"))}
        env.update(overrides or {})
        code = """
import json
from unittest.mock import patch
with patch('v9711_live.environment.load_environment'):
    from v9711_live import config as c
print(json.dumps({name: str(getattr(c, name).resolve()) for name in
    ('ROOT', 'SHARED_ROOT', 'STATUS_FILE', 'ACCOUNT_STREAM_DIR',
     'EXCHANGE_INFO_CACHE_FILE', 'PRICE_FILE', 'STATE_DIR')}))
"""
        result = subprocess.run([sys.executable, "-B", "-c", code, *args],
            cwd=Path(__file__).resolve().parents[1], env=env,
            check=True, capture_output=True, text=True)
        return {k: Path(v) for k, v in json.loads(result.stdout).items()}

    def test_default_shared_reads_and_isolated_writes(self):
        paths = self._read_config()
        root = Path(__file__).resolve().parents[1]
        self.assertEqual(paths['ROOT'], root)
        self.assertEqual(paths['SHARED_ROOT'], root.parent)
        shared = root.parent / 'var'
        self.assertEqual(paths['STATUS_FILE'], shared / 'klines_3m_status.json')
        self.assertEqual(paths['ACCOUNT_STREAM_DIR'], shared / 'binance_account_stream')
        self.assertEqual(paths['EXCHANGE_INFO_CACHE_FILE'], root / 'var/exchange_info_cache.json')
        self.assertEqual(paths['STATE_DIR'], root / 'var/state')
        self.assertEqual(paths['PRICE_FILE'], root / 'var/binance_ticker_price/latest.json')

    def test_stale_env_cannot_redirect_runtime_paths(self):
        names = ('V9711_DEPLOY_ROOT', 'V9711_WS_SHARED_ROOT',
            'V9711_LIVE_3M_STATUS_FILE', 'V9711_LIVE_ACCOUNT_STREAM_DIR',
            'V9711_LIVE_EXCHANGE_INFO_CACHE_FILE', 'V9711_LIVE_PRICE_FILE',
            'V9711_LIVE_SYMBOLS_FILE', 'V9711_LIVE_STATE_DIR')
        self.assertEqual(self._read_config({name: '/wrong/path' for name in names}),
                         self._read_config())

    def test_paper_flag_preserves_existing_paper_directory(self):
        root = Path(__file__).resolve().parents[1]
        paths = self._read_config(args=('--paper',))
        self.assertEqual(paths['STATE_DIR'], root / 'var/paper_state')

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
