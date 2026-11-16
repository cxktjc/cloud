from __future__ import annotations
import os
import sys
import tempfile
import unittest
import logging
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from v9711_live.environment import load_environment
from v9711_live import config as cfg
from v9711_live import run_signal
from collectors import binance_futures_3m_ws, kline_aggregator_15m, kline_startup_manager


class MysqlSettingsTests(unittest.TestCase):
    def test_private_env_and_environment_precedence(self):
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {}, clear=True):
            path=Path(folder)/".env"
            path.write_text("MYSQL_PASSWORD='test-only password'\nMYSQL_ADMIN_PASSWORD=test-admin\n",encoding="utf-8")
            load_environment(path)
            self.assertEqual(os.environ["MYSQL_PASSWORD"], "test-only password")
            os.environ["MYSQL_PASSWORD"]="external-test"
            load_environment(path)
            self.assertEqual(os.environ["MYSQL_PASSWORD"], "external-test")

    def test_all_collectors_accept_password_override(self):
        for module in (binance_futures_3m_ws, kline_aggregator_15m, kline_startup_manager):
            with self.subTest(module=module.__name__), patch.object(sys,"argv",["test","--mysql-password","test-cli"]):
                args=module.parse_args() if module is binance_futures_3m_ws else module.parse_args(sys.argv[1:])
                self.assertEqual(args.mysql_password,"test-cli")

    def test_cloud_collectors_share_default_without_private_env(self):
        from v9711_live.environment import MYSQL_DEFAULT_PASSWORD
        for module in (binance_futures_3m_ws, kline_aggregator_15m, kline_startup_manager):
            with self.subTest(module=module.__name__), patch.dict(os.environ, {}, clear=True), \
                 patch.object(module, "load_environment"), patch.object(sys,"argv",["test"]):
                args=module.parse_args() if module is binance_futures_3m_ws else module.parse_args([])
                self.assertEqual(args.mysql_password, MYSQL_DEFAULT_PASSWORD)

    def test_signal_override_reaches_db_config_before_construction(self):
        with tempfile.TemporaryDirectory() as folder, \
             patch.object(cfg,"STATE_DIR",Path(folder)), \
             patch.object(cfg,"MYSQL_PASSWORD","original-test"), \
             patch.object(sys,"argv",["signal","--mysql-password","test-cli","--once"]), \
             patch.object(run_signal.logging,"FileHandler",return_value=logging.NullHandler()), \
             patch("v9711_live.storage.SingleWriter"), \
             patch.object(run_signal,"LiveSignal") as signal:
            signal.return_value.run.return_value=0
            def build(**kwargs):
                self.assertEqual(cfg.MYSQL_PASSWORD,"test-cli")
                from unittest.mock import Mock
                obj=Mock(); obj.run.return_value=0
                return obj
            signal.side_effect=build
            self.assertEqual(run_signal.main(),0)

if __name__ == "__main__":
    unittest.main()
