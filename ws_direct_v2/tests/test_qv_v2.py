import copy
import json
import math
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock, patch

from v9711_live import config as cfg
from v9711_live.contracts import atomic_write_json
from v9711_live.market_data import Bar, BarStore, KlineStatus
from v9711_live.qv import QVHistory, quantile
from v9711_live.signal_logic import build_candidates
from v9711_live.signal_portfolio import SignalPortfolio, SignalStateError

STEP = cfg.BAR_MS
# UTC month boundary: no month-specific branch may change the statistic.
TARGET = 1793491200000
SYMBOLS = tuple(f'S{i:02}USDT' for i in range(1, 12))


def candles(*, target=TARGET, count=96, rising=False, touch=False):
    return {s: [Bar(target + (i - count) * STEP, 100, 110,
                   60 if touch and i == count else 100, 100,
                   scale * (i + 1 if rising else 100))
                for i in range(count + 1)] for scale, s in enumerate(SYMBOLS, 1)}


class QVTests(unittest.TestCase):
    def test_linear_quantile_matches_reference_interpolation(self):
        self.assertAlmostEqual(quantile([10, 20, 30, 40]), 37)
        self.assertEqual(quantile([0, -1, float('nan'), float('inf'), 5]), 5)
        self.assertTrue(math.isnan(quantile([0, float('nan')])))

    def test_exact_20_bar_shift_q90_and_24_bar_mean(self):
        snap = QVHistory(candles(rising=True), SYMBOLS).snapshot(TARGET)
        self.assertTrue(snap.ready)
        self.assertEqual(snap.quantile_count, 24)
        self.assertAlmostEqual(snap.qv20[SYMBOLS[0]], 86.5)
        self.assertAlmostEqual(snap.raw_quantile, 865)
        self.assertAlmostEqual(snap.threshold, 750)

    def test_future_candles_and_current_bar_qv_do_not_change_threshold(self):
        bars = candles(rising=True)
        old = QVHistory(bars, SYMBOLS).snapshot(TARGET)
        for s in SYMBOLS:
            bars[s][-1] = replace(bars[s][-1], quote_volume=1e99, open=1e9, close=1e9)
            bars[s].append(Bar(TARGET+STEP, 1, 1, 1, 1, 1e99))
        new = QVHistory(bars, SYMBOLS).snapshot(TARGET)
        self.assertEqual(new, old)

    def test_restart_and_month_boundary_use_full_historical_quantiles(self):
        bars = candles(rising=True)
        full = QVHistory(bars, SYMBOLS).snapshot(TARGET)
        trimmed = {s: [b for b in rows if b.open_time_ms >= TARGET-43*STEP]
                   for s, rows in bars.items()}
        restarted = QVHistory(trimmed, SYMBOLS).snapshot(TARGET)
        self.assertEqual(full, restarted)
        self.assertNotEqual(full.threshold, full.raw_quantile)

    def test_missing_whole_bar_blocks_even_when_union_count_exceeds_43(self):
        bars = candles()
        for s in SYMBOLS:
            bars[s] = [b for b in bars[s] if b.open_time_ms != TARGET-30*STEP]
        snap = QVHistory(bars, SYMBOLS).snapshot(TARGET)
        self.assertFalse(snap.ready)
        self.assertEqual(snap.history_bars, 29)
        self.assertEqual(build_candidates(bars, reference_bar_open_ms=TARGET-STEP, symbols=SYMBOLS), ())

    def test_missing_symbol_cell_is_zero_on_calendar_grid_not_last_20_records(self):
        bars = candles()
        bars[SYMBOLS[0]] = [b for b in bars[SYMBOLS[0]] if b.open_time_ms != TARGET-5*STEP]
        snap = QVHistory(bars, SYMBOLS).snapshot(TARGET)
        self.assertTrue(snap.ready)
        self.assertEqual(snap.qv20[SYMBOLS[0]], 95)

    def test_invalid_volume_is_zero_and_zero_symbols_excluded_from_cross_section(self):
        bars = candles()
        bars['ZERO'] = [replace(b, quote_volume=float('nan')) for b in bars[SYMBOLS[0]]]
        snap = QVHistory(bars, SYMBOLS+('ZERO',)).snapshot(TARGET)
        self.assertEqual(snap.qv20['ZERO'], 0)
        self.assertEqual(snap.threshold, 1000)
        self.assertEqual(snap.describe()['positive_symbols'], 11)

    def test_min_periods_ten_and_full_smoothing_warmup_are_distinct(self):
        bars = candles(count=9)
        history = QVHistory(bars, SYMBOLS)
        self.assertTrue(math.isnan(history.features(TARGET)[SYMBOLS[0]]))
        bars = candles(count=10)
        history = QVHistory(bars, SYMBOLS)
        self.assertEqual(history.features(TARGET)[SYMBOLS[0]], 100)
        self.assertFalse(history.snapshot(TARGET).ready)

    def test_all_43_past_bars_required_no_static_fallback(self):
        self.assertFalse(QVHistory(candles(count=42), SYMBOLS).snapshot(TARGET).ready)
        self.assertTrue(QVHistory(candles(count=43), SYMBOLS).snapshot(TARGET).ready)

    def test_shared_snapshot_uses_full_universe_and_previous_close_d025(self):
        bars = candles()
        snap = QVHistory(bars, SYMBOLS).snapshot(TARGET)
        rows = build_candidates(bars, reference_bar_open_ms=TARGET-STEP,
                                symbols=SYMBOLS, qv_snapshot=snap)
        self.assertEqual([c.symbol for c in rows], list(SYMBOLS[:-1]))
        self.assertTrue(all(c.limit_price == 97.5 and c.reference_price == 100 for c in rows))
        self.assertEqual(rows[0].exit_due_ms, TARGET+5*STEP)
        with self.assertRaises(ValueError):
            build_candidates(bars, reference_bar_open_ms=TARGET, symbols=SYMBOLS, qv_snapshot=snap)

    def test_unaligned_target_rejected(self):
        with self.assertRaises(ValueError):
            QVHistory(candles(), SYMBOLS).snapshot(TARGET+1)


class BookTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.path = Path(self.folder.name)/'signal.json'

    def legacy(self, *, touch=True):
        book = SignalPortfolio(self.path)
        bars = candles(touch=touch)
        book.advance(bars, through_bar_open_ms=TARGET, symbols=SYMBOLS)
        state = copy.deepcopy(book.state)
        state.update(model=SignalPortfolio.LEGACY_MODEL, parameters=SignalPortfolio.legacy_parameters())
        state.pop('strategy_effective_bar_open_ms')
        state['realized_balance'] = 21.234
        atomic_write_json(self.path, state)
        return state, bars

    def test_model_and_planner_have_same_limit_qv_and_due(self):
        bars = candles(touch=True)
        planned = build_candidates(bars, reference_bar_open_ms=TARGET-STEP, symbols=SYMBOLS)
        book = SignalPortfolio(self.path)
        book.advance(bars, through_bar_open_ms=TARGET, symbols=SYMBOLS)
        for candidate in planned[:3]:
            pos = book.positions[candidate.symbol]
            self.assertEqual(pos['entry_price'], candidate.limit_price)
            self.assertEqual(pos['entry_qv20'], candidate.qv20)
            self.assertEqual(pos['exit_due_ms'], candidate.exit_due_ms)
        self.assertEqual([p['notional'] for p in book.positions.values()], [7.5, 4.5, 3])
        # Entry bar low is 60 (below a hypothetical 25% stop). User excluded
        # that stop; only the original fixed due bar may settle the position.
        self.assertEqual(book.state['total_exits'], 0)

    def test_upgrade_preserves_book_exactly_and_backup_is_original_bytes(self):
        legacy, bars = self.legacy()
        original = self.path.read_bytes()
        book = SignalPortfolio(self.path, now=(TARGET+STEP+5000)/1000)
        for field in ('session_id', 'positions', 'recent_trades', 'realized_balance',
                      'initial_equity', 'last_bar_checkpoint', 'total_entries', 'total_exits',
                      'start_bar_open_ms', 'last_processed_bar_open_ms'):
            self.assertEqual(book.state[field], legacy[field], field)
        self.assertEqual(self.path.with_name(self.path.name+'.pre_d025_cs_qv.bak').read_bytes(), original)
        self.assertEqual(book.state['strategy_effective_bar_open_ms'], TARGET+2*STEP)
        restored = SignalPortfolio(self.path, now=(TARGET+1000*STEP)/1000)
        self.assertEqual(restored.state, book.state)

    def test_legacy_bar_is_frozen_not_replayed_on_upgrade(self):
        legacy, bars = self.legacy()
        book = SignalPortfolio(self.path, now=(TARGET+STEP+5000)/1000)
        for s in SYMBOLS:
            bars[s][-1] = replace(bars[s][-1], low=1000, high=1001)
        book.advance(bars, through_bar_open_ms=TARGET, symbols=SYMBOLS)
        self.assertEqual(book.positions, legacy['positions'])
        self.assertEqual(book.balance, legacy['realized_balance'])

    def test_old_active_order_bar_keeps_price_and_new_bar_switches(self):
        self.legacy(touch=False)
        book = SignalPortfolio(self.path, now=(TARGET+STEP+5000)/1000)
        old = build_candidates(candles(), reference_bar_open_ms=TARGET, symbols=SYMBOLS,
                               **book.candidate_options(TARGET+STEP))
        self.assertEqual(old[0].limit_price, 97)
        bars = candles(target=TARGET+STEP)
        new = build_candidates(bars, reference_bar_open_ms=TARGET+STEP, symbols=SYMBOLS,
                               **book.candidate_options(TARGET+2*STEP))
        self.assertEqual(new[0].limit_price, 97.5)

    def test_catchup_crosses_upgrade_boundary_with_correct_entry_rule(self):
        self.legacy(touch=False)
        book = SignalPortfolio(self.path, now=(TARGET+STEP+5000)/1000)
        bars = candles()
        for s in SYMBOLS:
            bars[s].extend(Bar(TARGET+i*STEP, 100, 101, 97.3, 100, 100)
                           for i in (1, 2))
        book.advance(bars, through_bar_open_ms=TARGET+2*STEP, symbols=SYMBOLS)
        self.assertEqual(book.state['total_entries'], 3)
        for p in book.positions.values():
            self.assertEqual(p['entry_bar_open_ms'], TARGET+2*STEP)
            self.assertEqual(p['entry_price'], 97.5)
            self.assertEqual(p['exit_due_ms'], TARGET+7*STEP)

    def test_legacy_active_order_identity_is_kept_by_new_trade_selector(self):
        from dataclasses import asdict
        from v9711_live.execution.logic import select_physical_orders
        self.legacy(touch=False)
        book = SignalPortfolio(self.path, now=(TARGET+STEP+5000)/1000)
        candidate = build_candidates(candles(), reference_bar_open_ms=TARGET,
            symbols=SYMBOLS, **book.candidate_options(TARGET+STEP))[0]
        # Migration retains the old price/identity inside the new 2.1% boundary.
        row = dict(asdict(candidate), observed_price=98.5, gap=(98.5-97)/97)
        old = dict(row, client_order_id='v9712l_original', order_id=12345)
        plan = select_physical_orders([row], {candidate.symbol: old})
        self.assertEqual(plan.keep, (candidate.symbol,))
        self.assertFalse(plan.cancel)
        self.assertFalse(plan.place)
        far = dict(row, observed_price=100, gap=(100-97)/97)
        plan = select_physical_orders([far], {candidate.symbol: old})
        self.assertEqual(plan.cancel, (candidate.symbol,))
        self.assertEqual(old['client_order_id'], 'v9712l_original')
        self.assertEqual(old['order_id'], 12345)
        self.assertFalse(plan.place)

    def test_newly_processed_legacy_bar_still_accepts_late_candles(self):
        self.legacy(touch=False)
        book = SignalPortfolio(self.path, now=(TARGET+STEP+5000)/1000)
        bars = candles()
        for s in SYMBOLS:
            bars[s].append(Bar(TARGET+STEP, 100, 101, 100, 100, 100))
        book.advance(bars, through_bar_open_ms=TARGET+STEP, symbols=SYMBOLS)
        self.assertFalse(book.positions)
        for s in SYMBOLS:
            bars[s][-1] = replace(bars[s][-1], low=96)
        book.advance(bars, through_bar_open_ms=TARGET+STEP, symbols=SYMBOLS)
        self.assertEqual(book.state['total_entries'], 3)
        self.assertTrue(all(p['entry_price'] == 97 for p in book.positions.values()))

    def test_missing_warmup_does_not_consume_model_bar(self):
        book = SignalPortfolio(self.path)
        book.advance(candles(count=30, touch=True), through_bar_open_ms=TARGET, symbols=SYMBOLS)
        self.assertIsNone(book.last_bar)
        book.advance(candles(touch=True), through_bar_open_ms=TARGET, symbols=SYMBOLS)
        self.assertEqual(book.state['total_entries'], 3)

    def test_restart_latest_replay_idempotent_and_historical_qv_correction_seen(self):
        bars = candles()
        book = SignalPortfolio(self.path)
        book.advance(bars, through_bar_open_ms=TARGET, symbols=SYMBOLS)
        before = book.revision
        restored = SignalPortfolio(self.path)
        restored.advance(bars, through_bar_open_ms=TARGET, symbols=SYMBOLS)
        self.assertEqual(restored.revision, before)
        # Outside current qv20, but inside the smoothing window.
        bars[SYMBOLS[-2]][-36] = replace(bars[SYMBOLS[-2]][-36], quote_volume=1e6)
        restored.advance(bars, through_bar_open_ms=TARGET, symbols=SYMBOLS)
        self.assertGreater(restored.revision, before)
        self.assertEqual(restored.state['total_entries'], 0)

    def test_corrupt_or_unrecognized_legacy_book_is_never_reset(self):
        state, _ = self.legacy()
        state['parameters']['d'] = .04
        atomic_write_json(self.path, state)
        before = self.path.read_bytes()
        with self.assertRaises(SignalStateError):
            SignalPortfolio(self.path)
        self.assertEqual(self.path.read_bytes(), before)
        self.assertFalse(self.path.with_name(self.path.name+'.pre_d025_cs_qv.bak').exists())

    def test_conflicting_backup_preserves_original_and_stops(self):
        self.legacy()
        before = self.path.read_bytes()
        self.path.with_name(self.path.name+'.pre_d025_cs_qv.bak').write_bytes(b'conflict')
        with self.assertRaises(SignalStateError):
            SignalPortfolio(self.path)
        self.assertEqual(self.path.read_bytes(), before)

    def test_interrupted_upgrade_resumes_from_verified_original(self):
        self.legacy()
        before = self.path.read_bytes()
        with patch('v9711_live.signal_portfolio.atomic_write_json', side_effect=OSError('interrupted')):
            with self.assertRaises(OSError):
                SignalPortfolio(self.path, now=TARGET/1000)
        self.assertEqual(self.path.read_bytes(), before)
        book = SignalPortfolio(self.path, now=TARGET/1000)
        self.assertEqual(book.balance, 21.234)
        self.assertEqual(len(book.positions), 3)

    def test_bounded_multi_bar_catchup_equals_incremental_restart(self):
        bars = candles(touch=True)
        # Existing positions must preserve their due times across the cutoff.
        for s in SYMBOLS:
            bars[s].extend(Bar(TARGET+i*STEP, 100, 110, 100+i, 100+i, 100)
                           for i in range(1, 7))
        a = SignalPortfolio(self.path)
        b = SignalPortfolio(self.path.with_name('second.json'))
        for book in (a, b):
            book.advance(bars, through_bar_open_ms=TARGET, symbols=SYMBOLS)
        a.advance(bars, through_bar_open_ms=TARGET+6*STEP, symbols=SYMBOLS)
        for i in range(1, 7):
            b = SignalPortfolio(b.path)
            b.advance(bars, through_bar_open_ms=TARGET+i*STEP, symbols=SYMBOLS)
        self.assertEqual(a.balance, b.balance)
        self.assertEqual(a.state['total_exits'], 3)
        self.assertEqual(b.state['total_entries'], 3)


class LiveIntegrationTests(unittest.TestCase):
    def test_late_history_gap_is_retried_and_merged_in_bounded_window(self):
        from v9711_live.run_signal import LiveSignal
        from datetime import datetime, timezone
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            symbols = folder/'symbols.txt'
            symbols.write_text('\n'.join(SYMBOLS))
            with patch.object(cfg, 'STATE_DIR', folder), patch.object(cfg, 'SYMBOLS_FILE', symbols), \
                 patch.object(cfg, 'DESIRED_FILE', folder/'desired.json'):
                signal = LiveSignal()
            complete = candles()
            signal.bar_store.bars = {s: [b for b in rows if b.open_time_ms != TARGET-30*STEP]
                                     for s, rows in complete.items()}
            signal.bar_store.update = Mock(return_value=0)
            def read(universe, start, end):
                self.assertLessEqual((end-start)//STEP+1, 84)
                return {s: [b for b in complete[s] if start <= b.open_time_ms <= end]
                        for s in universe}
            signal.bar_store.read_window = Mock(side_effect=read)
            now = (TARGET+STEP+1000)/1000
            status = KlineStatus(TARGET, 11, 11, SYMBOLS,
                datetime.fromtimestamp(now, timezone.utc).isoformat(), 'test')
            signal._refresh_bar(status, now=now)
            self.assertFalse(signal.model_ready)
            signal._refresh_bar(status, now=now+5)
            self.assertTrue(signal.model_ready)
            self.assertTrue(signal.qv_diagnostics['ready'])
            self.assertEqual(len(signal.bar_store.bars[SYMBOLS[0]]), 96)
            self.assertGreater(signal.bar_store.revision, 0)

    def test_full_universe_threshold_with_partial_status_and_published_diagnostics(self):
        from v9711_live.run_signal import LiveSignal
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            symbols = folder/'symbols.txt'
            symbols.write_text('\n'.join(SYMBOLS))
            with patch.object(cfg, 'STATE_DIR', folder), patch.object(cfg, 'SYMBOLS_FILE', symbols), \
                 patch.object(cfg, 'DESIRED_FILE', folder/'desired.json'):
                signal = LiveSignal()
            signal.bar_store.bars = candles()
            signal.bar_store.revision = 1
            signal.bar_store.update = Mock(return_value=0)
            signal.bar_store.read_window = Mock(side_effect=AssertionError('unbounded/unnecessary SQL'))
            from datetime import datetime, timezone
            now = (TARGET+STEP+1000)/1000
            status = KlineStatus(TARGET, 1, 1, (SYMBOLS[0],),
                datetime.fromtimestamp(now, timezone.utc).isoformat(), 'test')
            signal.current_status = status
            signal._refresh_bar(status, now=now)
            self.assertTrue(signal.model_ready)
            self.assertEqual(signal.qv_diagnostics['positive_symbols'], 11)
            self.assertEqual(len(signal.candidates), 10)
            self.assertTrue(signal.candidates[-1].symbol not in status.symbols)
            signal._publish_heartbeat(now=now, force=True)
            payload = json.loads(signal.publisher.path.read_bytes())
            self.assertTrue(payload['signal_ready'])
            self.assertEqual(payload['portfolio']['qv']['threshold'], 1000)
            self.assertEqual(payload['portfolio']['qv']['quantile_count'], 24)
