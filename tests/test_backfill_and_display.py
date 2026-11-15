from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from collectors import kline_aggregator_15m as agg
from v9711_live import config as cfg
from v9711_live.terminal_view import TerminalView


class BackfillTests(unittest.TestCase):
    def test_recent_windows_first_then_live(self):
        step = agg.INTERVAL_15M_MS
        scheduler = agg.NewestFirstWindows(10 * step, 14 * step + 1000, 24)
        seen = []
        for _ in range(4):
            window = scheduler.select(14 * step + 1000, 1)
            seen.append(window)
            scheduler.finish(window, True, 0)
        self.assertEqual(seen, [n * step for n in (13, 12, 11, 10)])
        self.assertEqual(scheduler.select(14 * step + 1000, 1), 14 * step)
        self.assertEqual(scheduler.select(15 * step + 1000, 1), 14 * step)

    def test_live_preempts_old_backfill_and_failures_retry(self):
        step = agg.INTERVAL_15M_MS
        scheduler = agg.NewestFirstWindows(10 * step, 14 * step + 1000, 24)
        scheduler.finish(13 * step, False, 20)
        self.assertEqual(scheduler.select(14 * step + 1000, 2), 12 * step)
        self.assertEqual(scheduler.select(15 * step + 1000, 2), 14 * step)
        scheduler.finish(14 * step, True, 0)
        self.assertEqual(scheduler.select(15 * step + 1000, 21), 13 * step)

    def test_retention_trims_expired_work(self):
        step = agg.INTERVAL_15M_MS
        scheduler = agg.NewestFirstWindows(0, 96 * step, 24)
        scheduler.select(96 * step + 1, 1)
        self.assertNotIn(0, scheduler.pending)
        self.assertTrue(all(key >= 1 for key in scheduler.pending))

    def test_backfill_does_not_regress_status(self):
        with patch.object(agg, 'read_15m_status', return_value=SimpleNamespace(open_time_ms=200)), \
             patch.object(agg, 'write_15m_status') as writer:
            agg.publish_aggregate_status(100, ['BTCUSDT'], 1)
            writer.assert_not_called()
            agg.publish_aggregate_status(300, ['BTCUSDT'], 1)
            writer.assert_called_once_with(open_time_ms=300, symbols=['BTCUSDT'], row_count=1,
                                           source='kline_aggregator_15m')

    def test_main_dispatches_latest_completed_window_first(self):
        step = agg.INTERVAL_15M_MS
        # The old cursor points far back; main must dispatch the newest window.
        with patch.object(agg, 'connect_mysql', return_value=Mock()), \
             patch.object(agg, 'initialize_cursor', return_value=(['BTCUSDT'], 100 * step, None, 10 * step)), \
             patch.object(agg, 'get_latest_closed_3m_ms', return_value=100 * step), \
             patch.object(agg, 'prune_old_rows'), \
             patch.object(agg, 'dt_to_ms', return_value=100 * step + 1000), \
             patch.object(agg.signal, 'signal'), patch('builtins.print'), \
             patch.object(agg, 'wait_for_coverage_and_aggregate', return_value=True) as run:
            self.assertEqual(agg.main(['--once']), 0)
            self.assertEqual(run.call_args.args[2], 99 * step)
            self.assertEqual(run.call_args.kwargs['max_attempts'], 2)

    def test_unavailable_anchor_yields_without_sleep(self):
        args = SimpleNamespace(request_delay=0, rest_retries=0, retry_delay=0,
                               anchor_symbol='BTCUSDT', retry_interval=5)
        with patch.object(agg, 'connect_mysql', return_value=Mock()), \
             patch.object(agg, 'get_latest_closed_3m_ms', return_value=None), \
             patch.object(agg, 'sleep_interruptibly') as sleep, patch('builtins.print'):
            self.assertFalse(agg.wait_for_coverage_and_aggregate(args, ['BTCUSDT'], 0,
                                                               lambda: False, max_attempts=2))
            sleep.assert_not_called()


class DisplayTests(unittest.TestCase):
    def test_signal_default_does_not_change_trade_display(self):
        self.assertEqual(TerminalView().view.min_refresh_seconds, cfg.SignalViewConfig().min_refresh_seconds)
        self.assertIsNot(TerminalView().view.__class__, cfg.ViewConfig)

    def test_changed_only_throttle_keeps_latest(self):
        view = TerminalView(cfg.ViewConfig(min_refresh_seconds=3))
        view._format = Mock(side_effect=lambda fields: str(fields['value']))
        with patch('builtins.print') as output, patch('os.system'), patch('sys.stdout'):
            self.assertTrue(view.render(now=0, value=1))
            self.assertFalse(view.render(now=0.1, value=2))
            self.assertFalse(view.render(now=2.9, value=3))
            self.assertTrue(view.render(now=3, value=4))
            self.assertFalse(view.render(now=20, value=4))
            self.assertEqual([call.args[0] for call in output.call_args_list], ['1', '4'])

    def test_clock_and_sequence_do_not_redraw(self):
        view = TerminalView(cfg.ViewConfig(min_refresh_seconds=3))
        view._format = Mock(return_value='frame')
        with patch('builtins.print'), patch('os.system'), patch('sys.stdout'):
            self.assertTrue(view.render(now=0, value=1, clock_utc='a', price_sequence=1))
            self.assertFalse(view.render(now=10, value=1, clock_utc='b', price_sequence=2))


if __name__ == '__main__':
    unittest.main()
