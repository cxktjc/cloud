from __future__ import annotations

import ast
import logging
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, Mock
from contextlib import ExitStack
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from v9711_live import config as cfg
from v9711_live.execution import runner, ws_direct


class TradeEntrypointTests(unittest.TestCase):
    def test_live_cli_reaches_executor_with_shared_lock(self):
        # Never instantiate the real exchange client or touch production state.
        with tempfile.TemporaryDirectory() as folder, \
             patch.object(cfg, 'STATE_DIR', Path(folder)), \
             patch.object(cfg, 'TRADE_LOCK_FILE', Path(folder) / 'trade.lock'), \
             patch.object(sys, 'argv', ['trade', '--enable-live', '--no-dry-run', '--once']), \
             patch.object(runner.logging, 'FileHandler', return_value=logging.NullHandler()), \
             patch('v9711_live.storage.SingleWriter') as lock, \
             patch.object(ws_direct, 'DirectTrade') as trade, \
             patch('v9711_live.execution.startup_v2.prepare_live_state', return_value={'mode': 'test'}) as prepare:
            trade.return_value.run.return_value = 0
            self.assertEqual(runner.main(), 0)
            lock.assert_any_call(Path(folder) / 'trade.lock')
            self.assertEqual(lock.call_count, 2)
            trade.assert_called_once_with(once=True, enabled=True, dry_run=False)
            trade.return_value.run.assert_called_once_with()
            prepare.assert_called_once()

    def test_duplicate_writer_does_not_start_executor(self):
        with tempfile.TemporaryDirectory() as folder, \
             patch.object(cfg, 'STATE_DIR', Path(folder)), \
             patch.object(cfg, 'PAPER_MODE', True), \
             patch.object(sys, 'argv', ['trade', '--once', '--paper']), \
             patch.object(runner.logging, 'FileHandler', return_value=logging.NullHandler()), \
             patch('v9711_live.storage.SingleWriter') as lock, \
             patch.object(ws_direct, 'DirectTrade') as trade:
            lock.return_value.__enter__.side_effect = RuntimeError('locked')
            self.assertEqual(runner.main(), 1)
            trade.assert_not_called()

    def _diagnostic_step(self, *, quote=True, ready=True, preflight=True):
        with tempfile.TemporaryDirectory() as folder, ExitStack() as stack:
            root = Path(folder)
            for name, value in {'STATE_DIR': root, 'EXECUTION_INTENT_FILE': root / 'intents.json',
                                'EXECUTION_LEDGER_FILE': root / 'ledger.json',
                                'DESIRED_FILE': root / 'desired_orders.json'}.items():
                stack.enter_context(patch.object(cfg, name, value))
            client = stack.enter_context(patch.object(runner, 'ExchangeClient')).return_value
            client.prepare_limit.return_value = {'ok': preflight, 'reason': 'test_preflight_failure'}
            trade = runner.LiveTrade(enabled=False, nonblocking_io=False)
            trade.session_entries = Mock()
            for name in ('_publish_entry_gate', '_drain_network', 'reconcile_orders',
                         '_apply_signal_targets', '_over_capacity_reduction', '_timed_exits',
                         '_cancel_all_for_full_capacity', '_prepare_rotation', '_consume_rotation',
                         '_refresh_entry_cancel_plan', 'write_feedback', '_render_trade'):
                stack.enter_context(patch.object(trade, name))
            now = time.time()
            bar = int(now * 1000) // cfg.BAR_MS * cfg.BAR_MS
            snapshot = {'signal_ready': ready, 'bar_open_ms': bar,
                        'target_positions': [], 'desired': [dict(symbol='LYNUSDT',
                        limit_price=0.0170041, notional=5, order_bar_open_ms=bar,
                        exit_due_epoch=now+900)]}
            trade.desired_snapshot = Mock(return_value=(snapshot, 'valid'))
            trade.refresh_prices = Mock(return_value={'LYNUSDT': 0.01730} if quote else {})
            trade._comparison = Mock(return_value={})
            trade._place_order = Mock()
            trade._step(now=now)
            return dict(trade._entry_diagnostics), trade._place_order.call_count

    def test_lyn_with_fresh_price_and_free_capacity_is_selected(self):
        diagnostics, submissions = self._diagnostic_step()
        self.assertEqual(diagnostics['selected_to_place'], ['LYNUSDT'])
        self.assertEqual(submissions, 1)
        self.assertEqual(diagnostics['blockers'], [])

    def test_missing_quote_and_preflight_failure_are_visible(self):
        for kwargs, reason in (({'quote': False}, 'missing_or_stale_price_or_invalid_limit'),
                               ({'preflight': False}, 'preflight: test_preflight_failure')):
            diagnostics, submissions = self._diagnostic_step(**kwargs)
            self.assertEqual(diagnostics['filtered']['LYNUSDT'], reason)
            self.assertEqual(submissions, 0)

    def test_signal_not_ready_is_visible_without_submission(self):
        diagnostics, submissions = self._diagnostic_step(ready=False)
        self.assertIn('signal_not_ready', diagnostics['blockers'])
        self.assertEqual(submissions, 0)

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
