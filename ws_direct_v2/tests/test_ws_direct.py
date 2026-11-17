from __future__ import annotations
import json
import queue
import sys
import tempfile
import threading
import time
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from v9711_live import config as cfg
from v9711_live.execution.ws_market import DirectMarket, Tick
from v9711_live.execution.ws_direct import DirectTrade
from v9711_live.market_data import PriceReader
from v9711_live.publisher import DesiredPublisher
from v9711_live.contracts import BarStats
from v9711_live.execution.logic import select_physical_orders
from scripts.migrate_state import migrate
from v9711_live.storage import SingleWriter


class MarketOrderingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)/'latest.json'
        self.market = DirectMarket(['TESTUSDT'], self.path)
        self.market.group_connected(1)
        self.market.events.get_nowait()  # connection wake

    def tick(self, price='1', stamp=None):
        self.assertTrue(self.market.update('TESTUSDT', price, stamp or int(time.time()*1000)))
        return self.market.events.get_nowait()

    def test_decision_and_dispatch_happen_before_price_is_visible_to_writer(self):
        tick = self.tick()
        order = []
        def decision(event):
            order.append('decision')
            self.market.write_mirror()
            self.assertNotIn('TESTUSDT', json.loads(self.path.read_text())['prices'])
            order.append('dispatch')
        self.market.decide(tick, decision)
        self.market.write_mirror()
        self.assertEqual(order, ['decision', 'dispatch'])
        payload = json.loads(self.path.read_text())
        self.assertEqual(payload['prices']['TESTUSDT']['price'], '1')
        self.assertTrue(payload['decision_before_mirror'])
        self.assertIsNotNone(PriceReader(self.path).read())

    def test_threshold_crossing_is_not_coalesced_away(self):
        stamp = int(time.time()*1000)
        for price in ('1.03', '1.01', '1.04'):
            self.market.update('TESTUSDT', price, stamp)
        observed = []
        while not self.market.events.empty():
            self.market.decide(self.market.events.get_nowait(), lambda tick: observed.append(tick.price))
        self.assertEqual(observed, ['1.03', '1.01', '1.04'])

    def test_blocked_file_writer_does_not_hold_decision_lock(self):
        entered, release = threading.Event(), threading.Event()
        def slow_write(*args):
            entered.set()
            release.wait(5)
        with patch('v9711_live.execution.ws_market.atomic_write_json', side_effect=slow_write):
            writer = threading.Thread(target=self.market.write_mirror)
            writer.start()
            self.assertTrue(entered.wait(2))
            called = []
            try:
                self.market.decide(self.tick(), lambda tick: called.append(tick.symbol))
                self.assertEqual(called, ['TESTUSDT'])
            finally:
                release.set()
                writer.join(2)

    def test_failed_decision_is_not_published(self):
        with self.assertRaisesRegex(RuntimeError, 'failed'):
            self.market.decide(self.tick(), lambda tick: (_ for _ in ()).throw(RuntimeError('failed')))
        self.assertEqual(self.market.snapshot()['prices'], {})

    def test_reconnect_invalidates_queued_old_generation(self):
        tick = self.tick()
        self.market.group_connected(1)
        self.assertFalse(self.market.decide(tick, lambda tick: self.fail('old generation executed')))
        self.assertFalse(self.market.order_allowed('TESTUSDT'))

    def test_disconnect_blocks_worker_even_when_quote_is_cached(self):
        self.market.decide(self.tick(), lambda tick: None)
        self.assertTrue(self.market.order_allowed('TESTUSDT'))
        self.market.group_failed(1, 'disconnected')
        self.assertFalse(self.market.order_allowed('TESTUSDT'))
        self.assertEqual(self.market.prices(), {})
        self.assertEqual(self.market.snapshot()['status'], 'error')

    def test_old_exchange_timestamp_is_rejected(self):
        stamp = int(time.time()*1000)
        self.tick(stamp=stamp)
        self.assertFalse(self.market.update('TESTUSDT', '2', stamp-1))

    def test_reconnect_does_not_rewind_exchange_time(self):
        stamp = int(time.time()*1000)
        self.tick(stamp=stamp)
        self.market.group_connected(1)
        self.assertFalse(self.market.update('TESTUSDT','2',stamp-1))

    def test_stale_queue_does_not_trade_historical_crossing(self):
        tick = Tick('TESTUSDT','1',int(time.time()*1000),time.monotonic_ns()-2_000_000_000,1,1)
        self.assertFalse(self.market.decide(tick, lambda tick: self.fail('stale event executed')))
        self.assertFalse(self.market.healthy())
        self.assertEqual(self.market.counts['rejected_backlog'], 1)

    def test_queue_overflow_fails_closed(self):
        market = DirectMarket(['TESTUSDT'], self.path, queue_size=1)
        market.group_connected(1)
        market.events.get_nowait()
        market.update('TESTUSDT','1',int(time.time()*1000))
        with self.assertRaises(BufferError):
            market.update('TESTUSDT','2',int(time.time()*1000))
        self.assertFalse(market.healthy())


class DirectAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.root = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        values = dict(STATE_DIR=self.root/'state', DESIRED_FILE=self.root/'plans.json',
                      EXECUTION_INTENT_FILE=self.root/'intents.json', EXECUTION_LEDGER_FILE=self.root/'ledger.json',
                      EXECUTION_FEEDBACK_FILE=self.root/'feedback.json',
                      SYMBOLS_FILE=self.root/'symbols.txt', EXCHANGE_INFO_CACHE_FILE=self.root/'filters.json',
                      PRICE_FILE=self.root/'latest.json', ACCOUNT_STREAM_DIR=self.root/'account')
        for key,value in values.items():
            self.stack.enter_context(patch.object(cfg,key,value))
        cfg.SYMBOLS_FILE.write_text('TESTUSDT\n')
        cfg.EXCHANGE_INFO_CACHE_FILE.write_text(json.dumps({'symbols':{'TESTUSDT':dict(tick='.000001',step='1',min_qty='1',min_notional='5')}}))
        self.trade = DirectTrade(enabled=False, nonblocking_io=False)
        self.trade.market.group_connected(1)
        self.trade.market.events.get_nowait()
        self.now = time.time()
        self.bar = int(self.now*1000)//cfg.BAR_MS*cfg.BAR_MS
        self.payload = dict(schema_version=1, contract_version=2, execution_mode=cfg.WS_DIRECT_MODE,
            state_authority='signal', generated_at_epoch=self.now, expires_at_epoch=self.now+30,
            signal_ready=True, bar_open_ms=self.bar, execution_contract={'positions_authority':'target_positions'},
            desired=[], target_positions=[], candidate_templates=[dict(symbol='TESTUSDT',
                order_bar_open_ms=self.bar, reference_bar_open_ms=self.bar-cfg.BAR_MS,
                limit_price=.0170041, notional=5, exit_due_epoch=self.now+900)])
        self.save_plan()
        self.trade.price.read = lambda **kwargs: self.fail('execution read local price file')

    def save_plan(self):
        cfg.DESIRED_FILE.write_text(json.dumps(self.payload))

    def event(self, price='.01734'):
        self.assertTrue(self.trade.market.update('TESTUSDT',price,int(time.time()*1000)))
        tick = self.trade.market.events.get_nowait()
        return self.trade.market.decide(tick,self.trade.on_tick)

    def test_full_plan_triggers_without_price_file_or_signal_near_order(self):
        self.assertFalse(cfg.PRICE_FILE.exists())
        self.assertEqual(self.payload['desired'], [])
        self.event()
        self.assertIn('TESTUSDT',self.trade.tracked)
        self.assertFalse(cfg.PRICE_FILE.exists())
        self.assertEqual(self.trade.tracked['TESTUSDT']['limit_price'],.0170041)

    def test_incremental_shortlist_matches_full_selector_with_rotation(self):
        symbols = ['S%02dUSDT'%i for i in range(30)]
        self.trade.market = DirectMarket(symbols,cfg.PRICE_FILE)
        self.trade.market.group_connected(1)
        self.trade.market.events.get_nowait()
        self.payload['candidate_templates'] = [dict(self.payload['candidate_templates'][0],
                    symbol=symbol,limit_price=1.) for symbol in symbols]
        self.save_plan()
        for i,symbol in enumerate(symbols):
            self.trade.market.update(symbol,str(1.001+i*.0005),int(time.time()*1000))
            tick = self.trade.market.events.get_nowait()
            self.trade.market.decide(tick,lambda tick:None)
        snapshot,status = self.trade.desired_snapshot(now=time.time())
        prices = self.trade.market.prices()
        for row in self.payload['candidate_templates'][-5:]:
            symbol = row['symbol']
            self.trade.tracked[symbol] = dict(row,client_order_id='test_'+symbol,
                _quote_fresh=True,observed_price=prices[symbol],gap=prices[symbol]-1)
        working = self.trade._fast_snapshot(snapshot,tick,time.time())
        def select(rows):
            return select_physical_orders([dict(row,observed_price=prices[row['symbol']],
                        gap=prices[row['symbol']]-1) for row in rows], self.trade.tracked,
                        free_position_slots=3,now=time.time())
        full,narrow = select(snapshot['desired']),select(working['desired'])
        self.assertEqual(full.actions,narrow.actions)
        self.assertEqual(full.rotation,narrow.rotation)
        self.assertLessEqual(len(working['desired']),13)

    def test_live_mode_requires_migration_before_starting_ws(self):
        from unittest.mock import Mock
        self.trade.client.enabled = True
        self.trade.client.dry_run = False
        self.trade.market.start = Mock()
        with self.assertRaisesRegex(RuntimeError,'startup not prepared'):
            self.trade._run_loop()
        self.trade.market.start.assert_not_called()

    def test_incremental_selector_parity_across_fifty_market_states(self):
        import random
        randomizer = random.Random(20261116)
        symbols = ['R%02dUSDT'%i for i in range(32)]
        for scenario in range(50):
            self.trade.market = DirectMarket(symbols,cfg.PRICE_FILE)
            self.trade.market.group_connected(1)
            self.trade.market.events.get_nowait()
            rows = [dict(self.payload['candidate_templates'][0],symbol=symbol,limit_price=1.)
                    for symbol in symbols]
            self.trade._templates = {row['symbol']:row for row in rows}
            self.trade._eligible.clear()
            self.trade._rank_heap.clear()
            prices = {}
            for symbol in symbols:
                price = round(randomizer.uniform(.99,1.035),6)
                prices[symbol] = price
                self.trade.market.update(symbol,str(price),int(time.time()*1000))
                tick = self.trade.market.events.get_nowait()
                self.trade.market.decide(tick,lambda tick:None)
                self.trade._update_rank(symbol,price)
            self.trade.tracked.clear()
            for row in randomizer.sample(rows,randomizer.randint(0,8)):
                symbol = row['symbol']
                self.trade.tracked[symbol] = dict(row,client_order_id='test_'+symbol,
                    _quote_fresh=True,observed_price=prices[symbol],gap=prices[symbol]-1,
                    cancel_requested=randomizer.random()<.2)
            self.trade.positions.clear()
            for symbol in randomizer.sample(symbols,randomizer.randint(0,3)):
                self.trade.positions[symbol] = dict(symbol=symbol,quantity=1)
            final_bar = bool(randomizer.randint(0,1))
            working = self.trade._fast_snapshot(dict(desired=rows),tick,time.time())
            def select(candidates):
                return select_physical_orders([dict(row,observed_price=prices[row['symbol']],
                    gap=prices[row['symbol']]-1) for row in candidates],self.trade.tracked,
                    held=self.trade.positions,free_position_slots=3-len(self.trade.positions),
                    ignore_position_capacity=final_bar,now=time.time())
            expected,actual = select(rows),select(working['desired'])
            self.assertEqual(expected,actual,'scenario '+str(scenario))

    def test_repeated_ticks_do_not_duplicate_exact_entry(self):
        self.event()
        client_id = self.trade.tracked['TESTUSDT']['client_order_id']
        self.event()
        self.assertEqual(len(self.trade.tracked),1)
        self.assertEqual(self.trade.tracked['TESTUSDT']['client_order_id'],client_id)
        self.assertEqual(self.trade.stats['place'],1)

    def test_old_signal_contract_cannot_drive_new_executor(self):
        self.payload.pop('execution_mode')
        self.save_plan()
        self.event()
        self.assertEqual(self.trade.tracked,{})

    def test_expired_plan_cannot_open_even_with_live_quote(self):
        self.payload['expires_at_epoch'] = self.now-1
        self.save_plan()
        self.event()
        self.assertEqual(self.trade.tracked,{})

    def test_future_bar_and_unready_plan_are_blocked(self):
        self.payload['bar_open_ms'] += cfg.BAR_MS
        self.save_plan()
        self.event()
        self.assertEqual(self.trade.tracked,{})

    def test_below_limit_price_is_an_entry_intent_not_a_model_fill(self):
        self.event('.016')
        self.assertIn('TESTUSDT',self.trade.tracked)
        self.assertEqual(self.trade.positions,{})

    def test_ws_feed_loss_blocks_new_admissions(self):
        self.trade.market.group_failed(1,'lost')
        self.payload['signal_ready'] = False
        self.save_plan()
        # A new packet alone does not override signal readiness.
        self.event()
        self.assertEqual(self.trade.tracked,{})

    def test_first_quote_wait_has_distinct_diagnostic(self):
        snapshot, status = self.trade.desired_snapshot(now=self.now)
        self.trade._execute_entries(snapshot, status, now=self.now, targets={}, final_exit_bar=False)
        self.assertEqual(self.trade._entry_diagnostics['filtered']['TESTUSDT'],
                         'quote_waiting_first_tick')
        self.assertEqual(self.trade.tracked, {})

    def test_missing_precision_cache_is_global_blocker_not_per_symbol_spam(self):
        cfg.EXCHANGE_INFO_CACHE_FILE.unlink()
        self.trade._refresh_precision_readiness()
        with patch('v9711_live.execution.runner.LOG.warning') as warning:
            self.event()
        self.assertEqual(self.trade.tracked, {})
        self.assertIn('desired_precision_cache_not_ready',
                      self.trade._entry_diagnostics['blockers'])
        warning.assert_not_called()

    def test_precision_readiness_adds_no_cache_read_to_tick_path(self):
        # Outside admission range, no local precision preflight is necessary.
        with patch.object(self.trade.client._metadata_reader, 'read',
                          side_effect=AssertionError('extra cache read on tick')):
            self.event('.03')

    def test_plan_dump_is_not_info_log(self):
        self.event('.03')
        with patch('v9711_live.execution.runner.LOG.info') as info:
            self.trade.step()
        self.assertFalse(any('entry diagnostics' in str(call) for call in info.call_args_list))


    def test_panel_summarizes_waits_but_keeps_real_preflight_errors(self):
        with patch.object(self.trade.view, 'render') as render:
            self.trade.step()
        fields = render.call_args.kwargs
        fields['entry_diagnostics']['filtered'] = {
            'WAITUSDT': 'quote_waiting_first_tick', 'STALEUSDT': 'quote_stale',
            'BADUSDT': 'preflight: invalid precision'}
        text = self.trade.view._format(fields)
        self.assertNotIn('entry skipped WAITUSDT', text)
        self.assertNotIn('entry skipped STALEUSDT', text)
        self.assertIn('waiting first/reconnect tick=1 stale=1', text)
        self.assertIn('entry skipped BADUSDT: preflight: invalid precision', text)
        fields['forced_close_history'] = {'BULLA-entry': {
            'symbol': 'BULLAUSDT', 'closed_owned_qty': 192}}
        fields['forced_close_cleanup'] = {'BULLAUSDT': {'reason': 'confirmed_forced_flat'}}
        fields['manual_close_history'] = {}
        fields['manual_signal_sync'] = {'signal_revision': 42, 'imported_position_count': 1}
        text = self.trade.view._format(fields)
        self.assertNotIn('BULLAUSDT', text)
        self.assertIn('Closed audit: forced=1 manual/external=0 old exact-order cleanup pending=1', text)
        self.assertNotIn('Signal compounding capital synced', text)
        self.assertIn('signal\u5bfc\u5165\u7248\u672c=42', text)
        self.assertNotIn('????', text)

    def test_signal_updates_are_comparison_only_not_automatic_ledger_sync(self):
        self.trade.ledger.state['realized_balance'] = 12.3
        snapshot = {'signal_ready': True, 'signal_session_id': 'fresh-session',
            'signal_revision': 42, 'target_positions': [], 'portfolio': {
                'session_id': 'fresh-session', 'revision': 42, 'initial_equity': 15,
                'realized_balance': 14.9, 'notional_per_order': 14.9/3,
                'sizing_divisor': 3, 'positions': {}}}
        self.trade._apply_signal_targets(snapshot)
        self.assertNotIn('signal_capital_sync', self.trade.ledger.state)
        self.assertEqual(self.trade.positions, {})
        self.assertEqual(self.trade.ledger.state['realized_balance'], 12.3)
        self.assertEqual(self.trade.ledger.state['signal_portfolio']['revision'], 42)


    def test_manual_import_mode_sizes_new_tick_orders_from_trade_not_signal(self):
        self.trade.ledger.state.update(capital_mode='trade_owned_after_manual_signal_sync',
                                       realized_balance=21.)
        self.payload['candidate_templates'][0]['notional'] = 100.
        self.save_plan()
        self.event()
        self.assertEqual(self.trade.tracked['TESTUSDT']['notional'], 10.5)
        self.trade.ledger.state['realized_balance'] = 24.
        self.event()
        self.assertEqual(self.trade.tracked['TESTUSDT']['notional'], 10.5)
        self.assertFalse(self.trade.tracked['TESTUSDT'].get('cancel_requested'))

    def test_manual_import_mode_sizes_new_maintenance_orders_from_trade(self):
        self.trade.ledger.state.update(capital_mode='trade_owned_after_manual_signal_sync',
                                       realized_balance=24.)
        self.trade.market.update('TESTUSDT', '.01734', int(time.time()*1000))
        self.trade.market.decide(self.trade.market.events.get_nowait(), lambda event: None)
        with patch.object(self.trade.view, 'render'):
            self.trade.step()
        self.assertEqual(self.trade.tracked['TESTUSDT']['notional'], 12.)

    def test_trade_baseline_feedback_and_comparison_survive_signal_balance_changes(self):
        self.trade.ledger.state.update(capital_mode='trade_owned_after_manual_signal_sync',
            book_initial_equity=20., realized_balance=21.,
            manual_signal_sync={'sync_id':'manual-once', 'signal_revision':4})
        self.payload['portfolio'] = dict(initial_equity=15., realized_balance=1000., positions={})
        self.save_plan()
        with patch.object(self.trade.view, 'render') as render:
            self.trade.step()
        feedback = json.loads(cfg.EXECUTION_FEEDBACK_FILE.read_text())
        self.assertEqual(feedback['realized_balance'], 21.)
        self.assertEqual(feedback['cumulative_pnl'], 1.)
        self.assertEqual(feedback['comparison']['actual']['realized_pnl'], 1.)
        self.assertEqual(feedback['comparison']['actual']['initial_equity'], 20.)
        self.assertEqual(feedback['comparison']['actual']['pnl_kind'],
                         'operator_imported_baseline_plus_subsequent_execution')
        self.assertEqual(render.call_args.kwargs['manual_signal_sync']['sync_id'], 'manual-once')



class TemplateAndMigrationTests(unittest.TestCase):
    def test_full_candidate_contract_is_published_without_near_orders_or_price(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/'plans.json'
            publisher = DesiredPublisher(path)
            portfolio = dict(positions={},session_id='test',revision=1,ready=True,realized_balance=15)
            publisher.publish(orders=[],bar_open_ms=180000,bar_stats=BarStats(),portfolio=portfolio,
                status_signature=None,price_sequence=-1,now=time.time(),
                candidate_templates=[dict(symbol='FARUSDT',limit_price=1,notional=5)])
            data = json.loads(path.read_text())
            self.assertEqual(data['desired'],[])
            self.assertEqual(data['candidate_templates'][0]['symbol'],'FARUSDT')
            self.assertEqual(data['execution_mode'],cfg.WS_DIRECT_MODE)

    def test_matching_signal_exports_candidates_without_price_and_excludes_held(self):
        from types import SimpleNamespace
        from v9711_live.run_signal import LiveSignal
        from v9711_live.signal_logic import Candidate
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/'plans.json'
            signal = object.__new__(LiveSignal)
            signal.publisher = DesiredPublisher(path)
            signal.desired = {}
            signal.stats = BarStats()
            signal.current_status = signal.current_price = None
            signal.portfolio = SimpleNamespace(positions={'HELDUSDT':{}})
            signal._portfolio_snapshot = lambda **kwargs: dict(positions={},session_id='test',revision=1,
                                                                ready=True,realized_balance=15)
            signal.candidates = tuple(Candidate(symbol,180000,360000,0,180000,1.,100000.,5.,1/.97,
                                                1080.,1080000,'test') for symbol in ('FARUSDT','HELDUSDT'))
            signal._publish_heartbeat(now=time.time())
            data = json.loads(path.read_text())
            self.assertEqual([row['symbol'] for row in data['candidate_templates']],['FARUSDT'])
            self.assertEqual(data['desired'],[])

    def test_not_ready_contract_withholds_all_executable_templates(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/'plans.json'
            DesiredPublisher(path).publish(orders=[],bar_open_ms=0,bar_stats=BarStats(),
                portfolio=dict(positions={},session_id='test',revision=0,ready=False,realized_balance=15),
                status_signature=None,price_sequence=-1,candidate_templates=[dict(symbol='FARUSDT')])
            self.assertEqual(json.loads(path.read_text())['candidate_templates'],[])

    def test_original_model_and_selection_rules_are_source_identical(self):
        import hashlib
        root = Path(__file__).resolve().parents[1]
        baseline = json.loads((root/'BASELINE.json').read_text())['source_normalized_sha256']
        # Forced-exit reconciliation now also accepts explicitly adopted seeds;
        # its exact-fill behavior is covered by test_forced_close. The v2 close
        # ladder intentionally uses 12 x 5s rounds (test_close_ladder_v2).
        # Frozen signal model and entry selection sources remain unchanged.
        for name in ('v9711_live/signal_logic.py',
                     'v9711_live/entry_policy.py','v9711_live/execution/logic.py'):
            self.assertEqual(hashlib.sha256((root/name).read_bytes().replace(b'\r\n',b'\n')).hexdigest(),baseline[name],name)

    def ledgers(self, source):
        source.mkdir()
        (source/'execution_ledger.json').write_text(json.dumps({'state':{'owned_positions':{'BULLAUSDT':{'quantity':192,'close_status':'UNKNOWN'}}}}))
        (source/'execution_intents.json').write_text(json.dumps({'intents':{'c:original':{'kind':'close'}}}))
        (source/'signal_portfolio.json').write_text(json.dumps({'schema_version':1,'positions':{}}))

    def test_migration_copies_unknown_ledger_without_changing_source(self):
        with tempfile.TemporaryDirectory() as folder:
            source,dest = Path(folder)/'old',Path(folder)/'new'
            self.ledgers(source)
            original = {p.name:p.read_bytes() for p in source.glob('*.json')}
            self.assertFalse(migrate(source,dest)['applied'])
            self.assertFalse(dest.exists())
            self.assertTrue(migrate(source,dest,apply=True)['completed'])
            for name,raw in original.items():
                self.assertEqual((source/name).read_bytes(),raw)
                self.assertEqual((dest/name).read_bytes(),raw)
            self.assertTrue((dest/'WS_MIGRATION.json').exists())

    def test_migration_refuses_overwrite(self):
        with tempfile.TemporaryDirectory() as folder:
            source,dest = Path(folder)/'old',Path(folder)/'new'
            self.ledgers(source)
            dest.mkdir()
            (dest/'execution_ledger.json').write_text('existing')
            with self.assertRaisesRegex(ValueError,'refusing overwrite'):
                migrate(source,dest,apply=True)
            self.assertEqual((dest/'execution_ledger.json').read_text(),'existing')

    def test_live_startup_also_holds_legacy_executor_lock(self):
        from unittest.mock import Mock
        import logging
        from v9711_live.execution import runner, ws_direct
        with tempfile.TemporaryDirectory() as folder, ExitStack() as stack:
            root = Path(folder)
            source,dest = root/'old',root/'new'
            self.ledgers(source)
            migrate(source,dest,apply=True)
            for name,value in {'STATE_DIR':dest,'TRADE_LOCK_FILE':dest/'trade.lock',
                               'PRICE_FILE':root/'price.json'}.items():
                stack.enter_context(patch.object(cfg,name,value))
            stack.enter_context(patch.object(sys,'argv',['trade','--enable-live','--no-dry-run']))
            stack.enter_context(patch.object(runner.logging,'FileHandler',return_value=logging.NullHandler()))
            constructor = stack.enter_context(patch.object(ws_direct,'DirectTrade'))
            stack.enter_context(patch('v9711_live.execution.startup_v2.legacy_locations',return_value=[source]))
            with SingleWriter(source/'trade.lock'):
                self.assertEqual(runner.main(),1)
            constructor.assert_not_called()

    def test_running_legacy_writer_blocks_migration(self):
        with tempfile.TemporaryDirectory() as folder:
            source,dest = Path(folder)/'old',Path(folder)/'new'
            self.ledgers(source)
            with SingleWriter(source/'trade.lock'):
                with self.assertRaises(RuntimeError):
                    migrate(source,dest,apply=True)
            self.assertFalse((dest/'execution_ledger.json').exists())


if __name__ == '__main__':
    unittest.main()
