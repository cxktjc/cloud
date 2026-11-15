from __future__ import annotations

import ast
import logging
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from v9711_live import config as cfg
from v9711_live.execution import runner


class TradeEntrypointTests(unittest.TestCase):
    def test_live_cli_reaches_executor_with_shared_lock(self):
        # Never instantiate the real exchange client or touch production state.
        with tempfile.TemporaryDirectory() as folder, \
             patch.object(cfg, 'STATE_DIR', Path(folder)), \
             patch.object(cfg, 'TRADE_LOCK_FILE', Path(folder) / 'trade.lock'), \
             patch.object(sys, 'argv', ['trade', '--enable-live', '--no-dry-run', '--once']), \
             patch.object(runner.logging, 'FileHandler', return_value=logging.NullHandler()), \
             patch('v9711_live.storage.SingleWriter') as lock, \
             patch.object(runner, 'LiveTrade') as trade:
            trade.return_value.run.return_value = 0
            self.assertEqual(runner.main(), 0)
            lock.assert_called_once_with(Path(folder) / 'trade.lock')
            trade.assert_called_once_with(once=True, enabled=True, dry_run=False)
            trade.return_value.run.assert_called_once_with()

    def test_duplicate_writer_does_not_start_executor(self):
        with tempfile.TemporaryDirectory() as folder, \
             patch.object(cfg, 'STATE_DIR', Path(folder)), \
             patch.object(sys, 'argv', ['trade', '--once']), \
             patch.object(runner.logging, 'FileHandler', return_value=logging.NullHandler()), \
             patch('v9711_live.storage.SingleWriter') as lock, \
             patch.object(runner, 'LiveTrade') as trade:
            lock.return_value.__enter__.side_effect = RuntimeError('locked')
            self.assertEqual(runner.main(), 1)
            trade.assert_not_called()

    def test_all_relative_module_imports_exist_in_standalone_tree(self):
        for package in ('v9711_live', 'collectors'):
            for source in (ROOT / package).rglob('*.py'):
                tree = ast.parse(source.read_text(encoding='utf-8-sig'))
                for node in ast.walk(tree):
                    if not isinstance(node, ast.ImportFrom) or not node.level or not node.module:
                        continue
                    base = source.parent
                    for _ in range(node.level - 1):
                        base = base.parent
                    target = base.joinpath(*node.module.split('.'))
                    with self.subTest(file=str(source.relative_to(ROOT)), line=node.lineno):
                        self.assertTrue(target.with_suffix('.py').is_file() or
                                        (target / '__init__.py').is_file(), str(target))


if __name__ == '__main__':
    unittest.main()
