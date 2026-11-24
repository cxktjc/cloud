"""Offline latency attribution, exact first-send matching and worker warmup."""
import asyncio
import threading
import unittest
from unittest.mock import AsyncMock, Mock, patch

from tests.test_trade_ws_keepalive import FakeSocket
from tests import test_book_ticker_v2 as fixtures
from v9711_live import config as cfg
from v9711_live.exchange.binance_futures_trade_ws import APIError, BinanceFuturesTradeWS
from v9711_live.execution.entry_diagnostics import submission_diagnostic, unfilled_diagnosis
from v9711_live.execution.submit_errors import EntryQuoteUnavailable, require_submit_ack, submit_error_is_ambiguous
from v9711_live.execution.entry_timing import EntrySendProbe, EntryTimings
from v9711_live.execution.client import ExchangeClient
from v9711_live.execution.nonblocking_io import NetworkIO
from v9711_live.execution.session_entries import SessionEntryStats
from v9711_live.execution.stats import compute_comparison
from v9711_live.execution.terminal import entry_timing_lines, TradeTerminalView


class EntryLatencyTests(unittest.TestCase):
    def test_summary_only_visits_recent_512_but_old_exact_order_records_remain(self):
        timing = EntryTimings()
        for index in range(600):
            cid = str(index)
            timing.register('XUSDT', cid, dict(_entry_timing_context=dict(_received_ns=1_000_000,
                _event_received_ns=1_000_000)))
            timing.mark_at(cid, 'send', 2_000_000 if index else 10_000_000_000, self.now*1000)
        class NoFullScan(dict):
            def values(self):
                raise AssertionError('summary scanned full process history')
        timing._rows = NoFullScan(timing._rows)
        result = timing.summary()
        self.assertEqual(result['samples'], 512)
        self.assertEqual(result['max_ms'], 1)
        self.assertEqual(timing.order('0')['latencies_ms']['quote_to_send'], 9999)

    def setUp(self):
        self.bar = 1800000
        self.now = self.bar/1000+39
        self.nano = 1_000_000_000
        for target, clock in (('time.time', lambda: self.now), ('time.monotonic_ns', lambda: self.nano)):
            p = patch(target, side_effect=clock)
            p.start()
            self.addCleanup(p.stop)
        self.stats = SessionEntryStats(started_at=self.bar/1000)
        self.timing = self.stats.timings
        self.target = dict(symbol='THEUSDT', quantity=1, position_id='book:THE',
                           entry_bar_open_ms=self.bar, entry_available_ms=self.bar+cfg.BAR_MS)
        self.entry = dict(self.target, signal_session_id='book')

    def register(self, cid='first', session='book', bar=None, position_id=''):
        self.timing.register('THEUSDT', cid, dict(order_bar_open_ms=bar or self.bar,
            position_id=position_id, _entry_timing_context=dict(quote_source='tick',
            received_epoch_ms=self.now*1000, exchange_time_ms=self.now*1000-8,
            _received_ns=self.nano, decision_epoch_ms=self.now*1000, _decision_ns=self.nano)),
            session_id=session)

    def advance(self, ms):
        self.now += ms/1000
        self.nano += int(ms*1e6)

    def observe_signal(self):
        self.stats.observe_signal(dict(signal_ready=True, signal_session_id='book', portfolio={},
            target_positions=[self.target]), status='valid', now=self.bar/1000+181)
        return self.stats.snapshot()['recent_unfilled_entries'][0]

    def test_first_send_39_and_reply_41_are_separate_millisecond_stamps(self):
        self.register()
        for stage, ms in (('registered', 4), ('worker', 1), ('send', 10), ('response', 2000), ('owner_ack', 3)):
            self.advance(ms)
            self.timing.mark('first', stage)
        timing = self.observe_signal()['first_order_timing']
        self.assertAlmostEqual(timing['send_epoch_ms'], self.bar+39015)
        self.assertAlmostEqual(timing['response_epoch_ms'], self.bar+41015)
        self.assertEqual(timing['latencies_ms']['send_to_response'], 2000)
        self.assertEqual(timing['latencies_ms']['response_to_owner'], 3)
        panel = '\n'.join(entry_timing_lines(timing))
        self.assertIn('首次下单=', panel)
        self.assertIn('收到行情=', panel)
        self.assertIn('.015+00:00', panel)
        self.assertEqual(self.stats.snapshot()['trade_filled_entries'], 0)  # ACK is not a fill.

    def test_queue_wait_selection_and_send_are_separate_from_exchange_reply(self):
        self.timing.register('THEUSDT', 'first', dict(order_bar_open_ms=self.bar,
            _entry_timing_context=dict(_received_ns=self.nano, _event_received_ns=self.nano,
                _dequeued_ns=self.nano+100_000_000, _decision_ns=self.nano+102_000_000)))
        self.advance(105)
        self.timing.mark('first', 'send')
        self.advance(2000)
        self.timing.mark('first', 'response')
        timing = self.timing.order('first')
        self.assertEqual(timing['latencies_ms']['queue_wait'], 100)
        self.assertEqual(timing['latencies_ms']['dequeued_to_decision'], 2)
        self.assertEqual(timing['latencies_ms']['trigger_to_send'], 105)
        self.assertEqual(self.timing.summary()['p95_ms'], 105)
        self.assertEqual(self.timing.summary()['within_10ms'], 0)

    def test_stats_cache_updates_after_worker_stage_without_new_signal(self):
        self.register()
        self.timing.mark('first', 'registered')
        before = self.observe_signal()['first_order_timing']
        self.assertNotIn('send_epoch_ms', before)
        self.advance(10)
        self.timing.mark('first', 'send')
        after = self.stats.snapshot()['recent_unfilled_entries'][0]['first_order_timing']
        self.assertIn('send_epoch_ms', after)

    def test_stage_summary_counts_sent_orders_and_keeps_missing_trigger_absent(self):
        self.timing.register('THEUSDT', 'first', dict(order_bar_open_ms=self.bar,
            _entry_timing_context=dict(_received_ns=self.nano, _event_received_ns=self.nano,
                _dequeued_ns=self.nano+100_000_000, _decision_ns=self.nano+102_000_000)))
        for stage, ms in (('registered', 112), ('worker', 3), ('send', 5)):
            self.advance(ms)
            self.timing.mark('first', stage)
        self.register('maintenance')  # No triggering event for a maintenance entry.
        self.advance(2)
        self.timing.mark('maintenance', 'registered')
        self.advance(2)
        self.timing.mark('maintenance', 'worker')
        self.advance(2)
        self.timing.mark('maintenance', 'send')
        self.register('unsent')
        self.advance(5000)
        self.timing.mark('unsent', 'registered')  # Must not inflate sent-order latency.
        stages = self.timing.summary()['stages']
        self.assertEqual(stages['queue_wait']['samples'], 1)
        self.assertEqual(stages['queue_wait']['p95_ms'], 100)
        self.assertEqual(stages['dequeued_to_decision']['p95_ms'], 2)
        self.assertEqual(stages['decision_to_registered']['max_ms'], 10)
        self.assertEqual(stages['registered_to_worker']['max_ms'], 3)
        self.assertEqual(stages['worker_to_send']['max_ms'], 5)
        self.assertEqual(stages['worker_to_send']['samples'], 2)

    def test_retry_and_follow_do_not_replace_the_first_actual_send(self):
        self.register('rejected')  # Local pre-send rejection must not become first send.
        self.advance(5)
        self.register('first')
        self.timing.mark('first', 'send')
        first = self.timing.first_for(self.entry)
        self.advance(1000)
        self.register('follow', position_id='book:THE')
        self.timing.mark('follow', 'send')
        self.timing.mark('first', 'send')  # Duplicate hook cannot overwrite first send.
        self.assertEqual(self.timing.first_for(self.entry), first)

    def test_background_disk_wait_remains_visible_in_send_latency(self):
        self.register()
        for stage, ms in (('registered', 1), ('worker', 2), ('durable', 31), ('send', 3)):
            self.advance(ms)
            self.timing.mark('first', stage)
        result = self.timing.summary()
        self.assertEqual(result['p95_ms'], 37)
        self.assertEqual(result['stages']['decision_to_registered']['p95_ms'], 1)
        self.assertEqual(result['stages']['registered_to_worker']['p95_ms'], 2)
        self.assertEqual(result['stages']['worker_to_durable']['p95_ms'], 31)
        self.assertEqual(result['stages']['durable_to_send']['p95_ms'], 3)

    def test_actual_receipt_timestamp_separates_disk_completion_from_worker_resume(self):
        self.register()
        self.advance(1)
        self.timing.mark('first', 'registered')
        self.advance(2)
        self.timing.mark('first', 'worker')
        self.timing.mark_at('first', 'persisted', self.nano+15_000_000, self.now*1000+15)
        self.advance(20)
        self.timing.mark('first', 'durable')
        self.advance(3)
        self.timing.mark('first', 'send')
        stages = self.timing.summary()['stages']
        self.assertEqual(stages['registered_to_persisted']['p95_ms'], 17)
        self.assertEqual(stages['persisted_to_durable']['p95_ms'], 5)
        self.assertEqual(stages['worker_to_durable']['p95_ms'], 20)

    def test_other_session_bar_or_exact_position_cannot_supply_times(self):
        self.register('other-session', session='old')
        self.register('other-bar', bar=self.bar-cfg.BAR_MS)
        self.register('other-position', position_id='book:OTHER')
        for cid in ('other-session', 'other-bar', 'other-position'):
            self.timing.mark(cid, 'send')
        self.assertEqual(self.timing.first_for(self.entry), {})
        self.assertIn('首次下单=未记录', entry_timing_lines({})[0])

    def test_elapsed_durations_use_monotonic_clock_when_wall_clock_changes(self):
        self.register()
        self.timing.mark('first', 'send')
        self.now -= 3600
        self.nano += 25_000_000
        self.timing.mark('first', 'response')
        self.assertEqual(self.timing.order('first')['latencies_ms']['send_to_response'], 25)

    def test_older_same_symbol_position_explains_missing_without_borrowing_its_send_time(self):
        self.register('older', bar=self.bar-cfg.BAR_MS)
        self.timing.mark('older', 'send', transport='WS')
        position = dict(symbol='THEUSDT', quantity=1, entry_client_order_id='older',
                        order_bar_open_ms=self.bar-cfg.BAR_MS, entry_fill_time_ms=self.bar-1000)
        self.stats.observe_owned_state({'THEUSDT': position}, {})
        entry = self.observe_signal()
        self.assertEqual(self.stats.snapshot()['trade_filled_entries'], 0)
        self.assertEqual(entry['first_order_timing'], {})
        other = entry['same_symbol_trade']
        self.assertEqual(other['mismatch_reason'], 'different_entry_bar')
        self.assertIn('send_epoch_ms', other['first_order_timing'])
        # The table compares floating PnL even when both sides hold the symbol.
        comparison = compute_comparison(actual_positions={'THEUSDT': position}, open_orders={},
            signal_portfolio={}, target_positions=[self.target], price_map={}, realized_balance=15)
        output = TradeTerminalView()._format(dict(comparison=comparison, clock_utc='test',
            session_entry_stats=self.stats.snapshot()))
        self.assertIn('持仓浮盈差异（含双方持有', output)
        self.assertIn('[双方持有]', output)
        self.assertIn('入场bar不同，不计入该笔signal成交', output)
        self.assertIn('同币trade这笔持仓的下单记录（另笔入场）', output)

    def test_same_bar_different_explicit_position_id_remains_unmatched(self):
        self.stats.observe_owned_state({'THEUSDT': dict(quantity=1,
            entry_client_order_id='older', order_bar_open_ms=self.bar, position_id='book:OTHER')}, {})
        entry = self.observe_signal()
        self.assertEqual(self.stats.snapshot()['trade_filled_entries'], 0)
        self.assertEqual(entry['same_symbol_trade']['mismatch_reason'], 'entry_identity_not_matched')

    def test_same_entry_positive_fill_is_not_reported_as_missing(self):
        self.stats.observe_owned_state({'THEUSDT': dict(quantity=.1,
            entry_client_order_id='filled', order_bar_open_ms=self.bar)}, {})
        self.stats.observe_signal(dict(signal_ready=True, signal_session_id='book', portfolio={},
            target_positions=[self.target]), status='valid', now=self.bar/1000+181)
        self.assertEqual(self.stats.snapshot()['trade_filled_entries'], 1)
        self.assertEqual(self.stats.snapshot()['recent_unfilled_entries'], [])

    def test_existing_position_context_is_removed_when_position_closes(self):
        self.stats.observe_owned_state({'THEUSDT': dict(quantity=1,
            entry_client_order_id='older', order_bar_open_ms=self.bar-cfg.BAR_MS)}, {})
        self.assertIn('same_symbol_trade', self.observe_signal())
        self.stats.observe_owned_state({}, {})
        self.assertNotIn('same_symbol_trade', self.stats.snapshot()['recent_unfilled_entries'][0])

    def test_persisted_position_timing_can_be_shown_without_recreating_send_evidence(self):
        self.stats.observe_owned_state({'THEUSDT': dict(quantity=1, entry_client_order_id='older',
            order_bar_open_ms=self.bar-cfg.BAR_MS,
            entry_timing=dict(send_epoch_ms=self.bar-1000, transport='WS'))}, {})
        entry = self.observe_signal()
        self.assertEqual(entry['first_order_timing'], {})
        self.assertEqual(entry['same_symbol_trade']['first_order_timing']['send_epoch_ms'], self.bar-1000)
        self.assertEqual(self.timing.order('older'), {})

    def test_legacy_model_timing_text_is_not_treated_as_a_real_send(self):
        self.stats.observe_owned_state({'THEUSDT': dict(quantity=1, entry_client_order_id='older',
            order_bar_open_ms=self.bar-cfg.BAR_MS, entry_timing='unknown_within_3m_confirmed_after_close')}, {})
        self.assertEqual(self.observe_signal()['same_symbol_trade']['first_order_timing'], {})

    def test_outcome_explains_cancelled_order_without_fabricating_fill(self):
        self.register()
        self.timing.mark('first', 'send', transport='WS')
        first_send = self.timing.first_for(self.entry)['send_epoch_ms']
        self.timing.outcome('first', status='CANCELED', executed_qty=0, cancel_reason='replaced_by_closer')
        entry = self.observe_signal()
        self.assertEqual(entry['order_attempts']['sent'], 1)
        self.assertEqual(entry['order_attempts']['latest']['cancel_reason'], 'replaced_by_closer')
        self.assertEqual(entry['first_order_timing']['send_epoch_ms'], first_send)
        self.assertEqual(self.stats.snapshot()['trade_filled_entries'], 0)
        comparison = compute_comparison(actual_positions={}, open_orders={}, signal_portfolio={},
            target_positions=[self.target], price_map={}, realized_balance=15)
        output = TradeTerminalView()._format(dict(comparison=comparison, clock_utc='test',
            session_entry_stats=self.stats.snapshot()))
        self.assertIn('最近状态=CANCELED', output)
        self.assertIn('撤销/终结原因=被更近候选替换', output)

    def test_recent_error_is_visible_without_signal_and_remains_history_after_retry(self):
        self.register()
        self.timing.mark('first', 'send', transport='WS')
        self.timing.outcome('first', status='SUBMIT_REJECTED', **submission_diagnostic(
            APIError(-999, 'Please sign TradFi-Perps agreement contract fapi.'), rejected=True))
        snapshot = self.stats.snapshot()
        self.assertEqual(snapshot['recent_unfilled_entries'], [])
        self.assertEqual(snapshot['recent_entry_errors'][0]['submit_error_code'], -999)
        self.advance(20)
        self.register('retry')
        self.timing.outcome('retry', status='NEW', order_id='live', executed_qty=0)
        detail = self.observe_signal()
        self.assertEqual(detail['unfilled_reason'], 'resting_unfilled')
        comparison = compute_comparison(actual_positions={}, open_orders={}, signal_portfolio={},
            target_positions=[self.target], price_map={}, realized_balance=15)
        output = TradeTerminalView()._format(dict(comparison=comparison, clock_utc='test',
            session_entry_stats=self.stats.snapshot()))
        self.assertIn('最近入场异常', output)
        self.assertIn('账户未签署 TradFi 合约协议', output)
        self.assertIn('未成交归因：交易所已接单，尚未成交', output)

    def test_known_local_rejection_vs_unknown_api_fate_and_late_ack(self):
        self.register()
        self.timing.outcome('first', status='SUBMIT_REJECTED', **submission_diagnostic(
            EntryQuoteUnavailable('neither tick nor bid0 qualifies'), rejected=True))
        self.assertEqual(self.observe_signal()['unfilled_reason'], 'local_not_sent')
        self.advance(20)
        self.register('retry')
        self.timing.mark('retry', 'send', transport='WS')
        self.timing.outcome('retry', status='UNKNOWN', **submission_diagnostic(
            APIError(-1007, 'Timeout waiting for response; execution status unknown'), rejected=False))
        self.assertEqual(self.observe_signal()['unfilled_reason'], 'result_unknown')
        self.timing.outcome('retry', status='NEW', order_id='live', executed_qty=0)
        self.assertEqual(self.observe_signal()['unfilled_reason'], 'resting_unfilled')
        self.assertEqual(self.stats.snapshot()['recent_entry_errors'][0]['current_status'], 'NEW')

    def test_raw_batch_member_error_keeps_code_and_unknown_error_stays_unknown(self):
        for code, text, expected in ((-2019, 'Margin is insufficient.', 'exchange_rejected'),
                                     (-999, 'unexpected reply', 'result_unknown')):
            with self.subTest(code=code), self.assertRaises(RuntimeError) as failure:
                require_submit_ack(dict(code=code, msg=text), symbol='THEUSDT', client_order_id='first')
            error = failure.exception
            diagnostic = submission_diagnostic(error, rejected=not submit_error_is_ambiguous(error))
            self.assertEqual(diagnostic['submit_error_code'], code)
            self.assertEqual(diagnostic['submit_error_category'], expected)
        self.assertEqual(unfilled_diagnosis({'last_skip': 'signal_follow_order_cap'}), 'local_not_sent')

    def test_audit_revision_changes_only_when_outcome_changes_and_never_rewinds_fill(self):
        self.register()
        self.timing.outcome('first', status='PARTIALLY_FILLED', executed_qty=1)
        revision = self.timing.revision
        self.timing.outcome('first', status='PARTIALLY_FILLED', executed_qty=1)
        self.assertEqual(self.timing.revision, revision)
        self.timing.outcome('first', status='NEW', executed_qty=0)
        self.assertEqual(self.timing.order('first')['status'], 'PARTIALLY_FILLED')
        self.assertEqual(self.timing.order('first')['executed_qty'], 1)


class WireProbeTests(unittest.IsolatedAsyncioTestCase):
    async def test_wire_hooks_run_after_connect_and_guard_and_before_socket_send(self):
        timings = EntryTimings()
        timings.register('THEUSDT', 'cid', dict(order_bar_open_ms=180000))
        broker = BinanceFuturesTradeWS('fake-key', 'fake-secret')
        self.addAsyncCleanup(broker.close)
        stages = []
        class Socket(FakeSocket):
            async def send(inner, wire):
                self.assertIn('send_epoch_ms', timings.order('cid'))
                self.assertNotIn('response_epoch_ms', timings.order('cid'))
                stages.append('send')
                await super().send(wire)
        broker._ws = Socket()
        async def connected():
            stages.append('connected')
        broker._ensure_connected = AsyncMock(side_effect=connected)
        def guard():
            stages.append('guard')
            self.assertNotIn('send_epoch_ms', timings.order('cid'))
        result = await broker._request('order.place', {}, signed=True,
                                      before_send=EntrySendProbe(guard, timings, 'cid'))
        self.assertEqual(stages, ['connected', 'guard', 'send'])
        self.assertEqual(result['orderId'], 123)
        self.assertEqual(timings.order('cid')['transport'], 'WS')
        self.assertIn('response_epoch_ms', timings.order('cid'))

    async def test_failed_guard_does_not_record_a_send(self):
        timings = EntryTimings()
        timings.register('THEUSDT', 'cid', dict(order_bar_open_ms=180000))
        broker = BinanceFuturesTradeWS('fake-key', 'fake-secret')
        self.addAsyncCleanup(broker.close)
        broker._ws = FakeSocket()
        broker._ensure_connected = AsyncMock()
        def reject():
            raise ValueError('expired')
        with self.assertRaisesRegex(ValueError, 'expired'):
            await broker._request('order.place', {}, signed=True,
                                  before_send=EntrySendProbe(reject, timings, 'cid'))
        self.assertEqual(broker._ws.sent, [])
        self.assertNotIn('send_epoch_ms', timings.order('cid'))


class WorkerLatencyTests(unittest.TestCase):
    def make_io(self, **kwargs):
        io = NetworkIO(**kwargs)
        self.addCleanup(io.shutdown)
        return io

    def test_all_worker_lanes_warm_before_work_and_reuse_their_threads(self):
        lock, warmed = threading.Lock(), threading.Event()
        identities = set()
        def warm():
            with lock:
                identities.add(threading.get_ident())
                if len(identities) == 2:
                    warmed.set()
        io = self.make_io(workers=2, worker_start=warm)
        self.assertTrue(warmed.wait(2))
        worked = threading.Event()
        def work():
            self.assertIn(threading.get_ident(), identities)
            worked.set()
        io.submit(('entry', 'THEUSDT', 'cid'), work, priority=10)
        self.assertTrue(worked.wait(2))

    def test_failed_warmup_does_not_lose_work_or_mutate_owner_state(self):
        wake = threading.Event()
        with self.assertLogs('v9711_live_trade.io', level='WARNING'):
            io = self.make_io(workers=1, worker_start=Mock(side_effect=ConnectionError('offline')),
                              owner_wake=wake.set)
            results = []
            io.submit(('entry', 'THEUSDT', 'cid'), lambda: 7, priority=10,
                      done=lambda value, error: results.append((value, error)))
            self.assertTrue(wake.wait(2))
        self.assertEqual(results, [])  # Worker only wakes, owner applies result.
        self.assertEqual(io.drain(), 1)
        self.assertEqual(results, [(7, None)])
        self.assertEqual(io.health()['transport_warmup']['failed'], 1)

    def test_callback_failure_remains_retryable_and_idle_drains_do_not_reapply(self):
        wake = threading.Event()
        io = self.make_io(workers=1, owner_wake=wake.set)
        calls = []
        def done(value, error):
            calls.append(value)
            if len(calls) == 1:
                raise OSError('save failed')
        io.submit(('entry', 'THEUSDT', 'cid'), lambda: 3, priority=10, done=done)
        self.assertTrue(wake.wait(2))
        with self.assertRaisesRegex(OSError, 'save failed'):
            io.drain()
        self.assertEqual(io.drain(), 1)
        for _ in range(100):
            self.assertEqual(io.drain(), 0)
        self.assertEqual(calls, [3, 3])

    def test_completion_arriving_during_an_owner_callback_is_not_lost(self):
        wake, release_b = threading.Event(), threading.Event()
        self.addCleanup(release_b.set)
        io = self.make_io(workers=2, owner_wake=wake.set)
        results = []
        def b():
            release_b.wait(2)
            return 'b'
        def done_a(value, error):
            results.append(value)
            wake.clear()
            release_b.set()
            self.assertTrue(wake.wait(2))
        io.submit(('entry', 'A', 'a'), lambda: 'a', priority=10, done=done_a)
        io.submit(('entry', 'B', 'b'), b, priority=10, done=lambda value, error: results.append(value))
        self.assertTrue(wake.wait(2))
        self.assertEqual(io.drain(), 1)
        self.assertEqual(io.drain(), 1)
        self.assertEqual(results, ['a', 'b'])

    def test_new_query_is_pruned_even_when_valid_identities_did_not_change(self):
        io = self.make_io(workers=1)
        io.prune_queries(set())
        key = ('query', 'get_order', 'THEUSDT', 'old')
        io.submit(key, lambda: None, priority=20)
        self.assertIn(key, io._query_keys)
        io.prune_queries(set())
        self.assertNotIn(key, io._jobs)
        self.assertFalse(io._query_keys)


class BatchLatencyTests(unittest.TestCase):
    setUp = fixtures.DualAdmissionTests.setUp
    target = fixtures.DualAdmissionTests.target
    make_trade = fixtures.DualAdmissionTests.make_trade
    refresh_quotes = fixtures.DualAdmissionTests.refresh_quotes
    place = fixtures.DualAdmissionTests.place
    cancel = fixtures.DualAdmissionTests.cancel
    publish = fixtures.DualAdmissionTests.publish
    ordinary_plan = fixtures.DualAdmissionTests.ordinary_plan
    plans = fixtures.DualAdmissionTests.plans
    tick = fixtures.DualAdmissionTests.tick

    def test_checked_transport_does_not_read_ws_again_after_socket_is_ready(self):
        self.plans()
        self.tick(1.01)
        client = self.trade.client
        client.enabled, client.dry_run = True, False
        self.trade._account_deadline = self.now+100
        row = self.trade.tracked['N0USDT']
        stages = []
        class CheckedBroker:
            MANAGES_WS_LIFECYCLE = True
            def place_order_checked(inner, *args, before_send=None, **kw):
                stages.append('socket_ready')
                before_send()
                stages.append('send')
                return dict(symbol='N0USDT', clientOrderId=row['client_order_id'], orderId='wire', status='NEW')
        client._broker = CheckedBroker()
        with patch.object(self.trade.market, 'send_quote', side_effect=AssertionError('secondary WS read')) as reads:
            ExchangeClient.place_limit_for_bar(client, symbol='N0USDT', client_order_id=row['client_order_id'],
                order_bar_open_ms=self.bar, limit_price=1., notional=5.)
        reads.assert_not_called()
        self.assertEqual(stages, ['socket_ready', 'send'])

    def test_unchecked_legacy_transport_still_runs_guard_before_send(self):
        self.plans()
        self.tick(1.01)
        client = self.trade.client
        client.enabled, client.dry_run = True, False
        stages = []
        class LegacyBroker:
            MANAGES_WS_LIFECYCLE = True
            def place_order(inner, *args, **kw):
                stages.append('send')
                return {}
        client._broker = LegacyBroker()
        client._broker_call('place_order', 'N0USDT', 'BUY', '5', before_send=lambda: stages.append('guard'))
        self.assertEqual(stages, ['guard', 'send'])

    def test_exact_cancel_is_retained_in_entry_lifecycle_after_retirement(self):
        self.plans()
        self.tick(1.01)
        row = self.trade.tracked['N0USDT']
        cid = row['client_order_id']
        self.trade._request_cancel('N0USDT', row, 'order_bar_expired')
        audit = self.trade.session_entries.timings.order(cid)
        self.assertEqual(audit['status'], 'CANCELED')
        self.assertEqual(audit['cancel_reason'], 'order_bar_expired')
        self.assertEqual(audit['retired_reason'], 'order_bar_expired')
        self.assertFalse(self.trade.positions)

    def test_exact_fill_preserves_recorded_times_after_entry_order_is_retired(self):
        self.plans()
        self.tick(1.01)
        row = self.trade.tracked['N0USDT']
        cid = row['client_order_id']
        self.trade.session_entries.timings.mark(cid, 'send', transport='WS')
        self.assertTrue(self.trade._register_fill('N0USDT', row,
            dict(symbol='N0USDT', clientOrderId=cid, orderId=row['order_id'], updateTime=self.now*1000),
            row['qty'], 1.))
        self.trade._retire_order('N0USDT', row, 'FILLED')
        timing = self.trade.positions['N0USDT']['entry_timing']
        self.assertEqual(timing['client_order_id'], cid)
        self.assertIn('received_epoch_ms', timing)
        self.assertIn('send_epoch_ms', timing)
        self.assertNotIn('N0USDT', self.trade.tracked)

    def test_batch_marks_each_exact_members_own_quote_and_request_times(self):
        fixtures.DualAdmissionTests.test_batch_send_retains_each_members_own_qualification(self)
        for symbol, source in (('N0USDT', 'tick'), ('N1USDT', 'depth')):
            cid = self.trade.tracked[symbol]['client_order_id']
            timing = self.trade.session_entries.timings.order(cid)
            self.assertEqual(timing['quote_source'], source)
            self.assertEqual(timing['transport'], 'REST_BATCH')
            self.assertIn('send_epoch_ms', timing)
            self.assertIn('response_epoch_ms', timing)
            self.assertIn('received_epoch_ms', timing)


if __name__ == '__main__':
    unittest.main()
