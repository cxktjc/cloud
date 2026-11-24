"""Offline integration tests for the two extra signal-follow entry slots."""
import json
import tempfile
import time
import unittest
from contextlib import ExitStack
from copy import deepcopy
from pathlib import Path
from unittest.mock import Mock, patch

from v9711_live import config as cfg
from v9711_live.execution.client import ExchangeClient
from v9711_live.execution.nonblocking_io import NetworkIO
from v9711_live.execution.submit_errors import ConfirmedSubmitRejection, EntryQuoteUnavailable
from v9711_live.execution.ws_direct import DirectTrade


class SignalFollowTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.root = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        self.bar = int(time.time() * 1000) // cfg.BAR_MS * cfg.BAR_MS
        self.now = self.bar / 1000 + 30
        self.stack.enter_context(patch('time.time', side_effect=lambda: self.now))
        for key, value in dict(STATE_DIR=self.root/'state', DESIRED_FILE=self.root/'plans.json',
                EXECUTION_INTENT_FILE=self.root/'intents.json', EXECUTION_LEDGER_FILE=self.root/'ledger.json',
                EXECUTION_FEEDBACK_FILE=self.root/'feedback.json', PRICE_FILE=self.root/'prices.json',
                SYMBOLS_FILE=self.root/'symbols.txt', EXCHANGE_INFO_CACHE_FILE=self.root/'filters.json',
                ACCOUNT_STREAM_DIR=self.root/'account').items():
            self.stack.enter_context(patch.object(cfg, key, value))
        self.symbols = ['AUSDT', 'BUSDT', 'CUSDT'] + [f'N{i}USDT' for i in range(9)]
        cfg.SYMBOLS_FILE.write_text('\n'.join(self.symbols))
        cfg.EXCHANGE_INFO_CACHE_FILE.write_text(json.dumps({'symbols': {
            s: dict(tick='.001', step='1', min_qty='1', min_notional='5') for s in self.symbols}}))
        self.orders = {}
        self.submits = []
        self.cancels = []
        self.submit_error = None
        self.cancel_error = None
        self.cancel_response = None
        self.make_trade()
        self.targets = [self.target('AUSDT')]
        self.templates = []
        self.publish()

    def target(self, symbol):
        entry = self.bar - cfg.BAR_MS
        return dict(symbol=symbol, position_id=f'session:{symbol}:{entry}', side='LONG',
                    entry_bar_open_ms=entry, entry_available_ms=entry+cfg.BAR_MS,
                    reference_bar_open_ms=entry-cfg.BAR_MS, limit_price=1., entry_price=1.,
                    quantity=5., notional=5., exit_due_epoch=(entry+5*cfg.BAR_MS)/1000,
                    exit_due_ms=entry+5*cfg.BAR_MS)

    def test_missing_follow_quote_has_exact_signal_skip_evidence(self):
        self.trade.market.processed.pop('AUSDT')
        self.event('N0USDT')
        entry = dict(self.targets[0], signal_session_id='session')
        attempts = self.trade.session_entries.timings.attempts_for(entry)
        self.assertEqual(attempts['last_skip'], 'signal_follow_no_valid_quote')
        self.assertEqual(attempts['sent'], 0)
        for other in (dict(position_id='other'), dict(signal_session_id='other'),
                      dict(entry_bar_open_ms=self.bar)):
            self.assertIsNone(self.trade.session_entries.timings.attempts_for(dict(entry, **other))['last_skip'])

    def make_trade(self):
        self.trade = DirectTrade(enabled=False, nonblocking_io=False)
        self.trade.market.group_connected(1)
        self.trade.market.events.get_nowait()
        self.refresh_quotes()
        self.trade.client.place_limit_for_bar = Mock(side_effect=self.place)
        self.trade.client.cancel = Mock(side_effect=self.cancel)
        self.trade.client.query_exact_order = Mock(side_effect=lambda ledger, **kw:
            (deepcopy(self.orders.get(kw['client_order_id'])), kw['client_order_id'] in self.orders))
        self.trade._stream_health = Mock(return_value=dict(fresh=True, age_seconds=0))
        self.trade.price.read = Mock(side_effect=AssertionError('must use internal WS'))
        self.trade._render_trade = Mock()

    def refresh_quotes(self):
        for symbol in self.symbols:
            self.trade.market.update(symbol, '1.2' if symbol[0] in 'ABC' else '1.01', int(self.now*1000))
            self.trade.market.decide(self.trade.market.events.get_nowait(), lambda tick: None)

    def place(self, **kw):
        self.submits.append(dict(kw))
        prepared = self.trade.client.prepare_limit(kw['symbol'], kw['limit_price'], kw['notional'])
        result = dict(symbol=kw['symbol'], clientOrderId=kw['client_order_id'], orderId=str(len(self.submits)),
                      status='NEW', executedQty='0', avgPrice='0', origQty=prepared['qty'])
        self.orders[kw['client_order_id']] = deepcopy(result)
        if self.submit_error:
            raise self.submit_error
        return result

    def cancel(self, **kw):
        self.cancels.append(dict(kw))
        if self.cancel_error:
            raise self.cancel_error
        result = dict(self.orders[kw['client_order_id']], status='CANCELED')
        if self.cancel_response:
            result.update(self.cancel_response)
        self.orders[kw['client_order_id']] = deepcopy(result)
        return result

    def publish(self):
        self.payload = dict(schema_version=1, contract_version=2, execution_mode=cfg.WS_DIRECT_MODE,
            state_authority='signal', generated_at_epoch=self.now, expires_at_epoch=self.now+30,
            signal_ready=True, signal_session_id='session', bar_open_ms=int(self.now*1000)//cfg.BAR_MS*cfg.BAR_MS,
            execution_contract={'positions_authority': 'target_positions'}, desired=[],
            target_positions=self.targets, candidate_templates=self.templates)
        cfg.DESIRED_FILE.write_text(json.dumps(self.payload))

    def event(self, symbol='AUSDT'):
        self.trade.market.update(symbol, '1.2' if symbol[0] in 'ABC' else '1.01', int(self.now*1000))
        return self.trade.market.decide(self.trade.market.events.get_nowait(), self.trade.on_tick)

    def held(self, count):
        self.trade.positions.update({f'H{i}': dict(symbol=f'H{i}', quantity=1., entry_price=1.,
            exit_due_epoch=self.now+3600, entry_epoch=self.now-600) for i in range(count)})

    def test_uses_original_signal_price_without_normal_gap_gate(self):
        self.event()
        row = self.trade.tracked['AUSDT']
        self.assertTrue(row['signal_follow'])
        self.assertEqual(row['limit_price'], 1.)
        self.assertEqual(row['order_bar_open_ms'], self.targets[0]['entry_bar_open_ms'])
        self.assertEqual(row['position_id'], self.targets[0]['position_id'])
        self.assertEqual(row['exit_due_ms'], self.targets[0]['exit_due_ms'])
        self.assertEqual(self.trade._ordinary_pending_count(), 0)
        self.assertEqual(self.trade._current_admission_gap(), cfg.BOOTSTRAP_PLACE_GAP)

    def test_survives_bar_roll_quote_change_and_restart_without_resubmission(self):
        self.event()
        cid = self.trade.tracked['AUSDT']['client_order_id']
        self.now += 190
        self.publish()
        self.refresh_quotes()
        self.event()
        self.assertEqual(self.trade.tracked['AUSDT']['client_order_id'], cid)
        self.assertEqual(self.cancels, [])
        self.make_trade()
        self.event()
        self.assertEqual(self.trade.tracked['AUSDT']['client_order_id'], cid)
        self.assertEqual(len(self.submits), 1)

    def test_eight_ordinary_orders_plus_two_follow_orders_with_separate_counts(self):
        self.targets = [self.target(s) for s in ('AUSDT', 'BUSDT')]
        self.templates = [dict(symbol=f'N{i}USDT', limit_price=1., notional=5.,
            order_bar_open_ms=self.bar, reference_bar_open_ms=self.bar-cfg.BAR_MS,
            exit_due_epoch=self.now+900) for i in range(8)]
        self.publish()
        self.event()
        self.event()
        self.assertEqual(len(self.trade.tracked), 10)
        self.assertEqual(self.trade._ordinary_pending_count(), 8)
        compare = self.trade._comparison(snapshot=self.payload, price_map=self.trade.market.prices(),
            signal_desired_count=8, priced_desired_count=8, near_place_count=8,
            positions=self.trade.positions, open_orders=self.trade.tracked)
        self.assertEqual(compare['capacity']['pending_order_count'], 8)
        self.assertEqual(compare['capacity']['signal_follow_order_count'], 2)
        self.assertEqual(compare['capacity']['total_pending_order_count'], 10)
        self.assertFalse(any(self.trade.tracked[s].get('cancel_requested') for s in ('AUSDT', 'BUSDT')))

    def test_allows_three_through_five_positions_but_only_two_extras(self):
        self.held(5)
        self.targets = [self.target(s) for s in ('AUSDT', 'BUSDT', 'CUSDT')]
        self.publish()
        self.event()
        self.assertEqual(set(self.trade.tracked), {'AUSDT', 'BUSDT'})
        self.assertTrue(all(r['sizing_weight'] == .2 for r in self.trade.tracked.values()))
        self.assertEqual(self.trade._ordinary_pending_count(), 0)

    def test_six_positions_block_new_and_cancel_existing_follow_orders(self):
        self.targets = [self.target(s) for s in ('AUSDT', 'BUSDT')]
        self.publish()
        self.event()
        self.held(6)
        self.event()
        self.assertEqual(self.trade.tracked, {})
        self.assertEqual(len(self.cancels), 2)
        self.assertEqual(len(self.submits), 2)
        self.event()
        self.assertEqual(len(self.submits), 2)

    def test_sixth_fill_blocks_worker_and_cancels_other_extra(self):
        self.held(5)
        self.targets = [self.target(s) for s in ('AUSDT', 'BUSDT')]
        self.publish()
        self.event()
        a, b = deepcopy(self.trade.tracked['AUSDT']), deepcopy(self.trade.tracked['BUSDT'])
        self.orders[a['client_order_id']].update(status='FILLED', executedQty=str(a['qty']), avgPrice='1')
        self.trade.reconcile_orders(now=self.now)
        self.assertEqual(len(self.trade.positions), 6)
        gate = self.trade.client.entry_gate
        io = object.__new__(NetworkIO)
        io._entry_gate, io._closed = gate, False
        with self.assertRaises(ConfirmedSubmitRejection):
            io.check_entry('BUSDT', b['client_order_id'], b['order_bar_open_ms'])
        self.event()
        self.assertNotIn('BUSDT', self.trade.tracked)
        self.assertEqual(len(self.submits), 2)

    def test_signal_end_cancels_and_racing_partial_fill_is_booked_once(self):
        self.event()
        old = deepcopy(self.trade.tracked['AUSDT'])
        self.cancel_response = dict(executedQty='2', avgPrice='1.1')
        self.targets = []
        self.publish()
        self.event()
        self.assertNotIn('AUSDT', self.trade.tracked)
        self.assertEqual(self.trade.positions['AUSDT']['quantity'], 2.)
        self.assertEqual(self.trade.positions['AUSDT']['entry_price'], 1.1)
        self.assertEqual(self.trade.positions['AUSDT']['exit_due_ms'], old['exit_due_ms'])
        self.event()
        self.assertEqual(len(self.submits), 1)

    def test_unconfirmed_cancel_still_occupies_one_of_two_slots(self):
        self.targets = [self.target(s) for s in ('AUSDT', 'BUSDT')]
        self.publish()
        self.event()
        self.cancel_error = TimeoutError('unknown cancel')
        self.targets = [self.target(s) for s in ('BUSDT', 'CUSDT')]
        self.publish()
        self.event()
        self.assertTrue(self.trade.tracked['AUSDT']['cancel_requested'])
        self.assertNotIn('CUSDT', self.trade.tracked)
        self.now += 11
        self.cancel_error = None
        self.publish()
        self.refresh_quotes()
        self.event()
        self.assertEqual(set(self.trade.tracked), {'BUSDT', 'CUSDT'})

    def test_unknown_submit_retains_exact_identity_across_restart(self):
        self.submit_error = TimeoutError('ACK lost')
        self.event()
        cid = self.trade.tracked['AUSDT']['client_order_id']
        self.make_trade()
        self.event()
        self.assertEqual(self.trade.tracked['AUSDT']['client_order_id'], cid)
        self.assertEqual(len(self.submits), 1)
        self.assertTrue(self.trade.ledger.owns_client(cid))

    def test_missing_cumulative_cancel_evidence_does_not_release_follow_slot(self):
        self.event()
        cid = self.trade.tracked['AUSDT']['client_order_id']
        self.cancel_response = dict(executedQty=None)
        self.targets = []
        self.publish()
        self.event()
        self.assertEqual(self.trade.tracked['AUSDT']['client_order_id'], cid)
        self.assertTrue(self.trade.ledger.owns_client(cid))
        self.assertNotIn('AUSDT', self.trade.positions)

    def test_definitive_reject_retries_after_cooldown_with_durable_new_id(self):
        self.submit_error = RuntimeError('Filter failure: LOT_SIZE')
        self.event()
        first = self.submits[-1]['client_order_id']
        self.assertNotIn('AUSDT', self.trade.tracked)
        self.event()
        self.assertEqual(len(self.submits), 1)
        self.now += 11
        self.submit_error = None
        self.publish()
        self.refresh_quotes()
        self.make_trade()
        self.event()
        self.assertNotEqual(self.trade.tracked['AUSDT']['client_order_id'], first)

    def test_follow_quote_rejection_retries_next_fresh_quote_without_ten_second_wait(self):
        self.submit_error = EntryQuoteUnavailable('internal WS quotes unavailable before send')
        self.event()
        first = self.submits[-1]['client_order_id']
        self.assertNotIn('AUSDT', self.trade.tracked)
        self.submit_error = None
        self.event()
        self.assertEqual(len(self.submits), 2)
        self.assertNotEqual(self.trade.tracked['AUSDT']['client_order_id'], first)
        self.assertTrue(self.trade.tracked['AUSDT']['signal_follow'])

    def test_signal_read_gap_preserves_existing_follow_without_new_admission(self):
        self.event()
        cid = self.trade.tracked['AUSDT']['client_order_id']
        self.now += 31
        self.refresh_quotes()
        self.event()
        self.assertEqual(self.trade.tracked['AUSDT']['client_order_id'], cid)
        self.assertEqual(self.cancels, [])
        self.assertEqual(len(self.submits), 1)

    def test_reopen_and_forced_close_blocks_are_preserved(self):
        self.trade.ledger.state['reopen_until_epoch']['AUSDT'] = self.now+60
        self.event()
        self.assertEqual(self.submits, [])
        self.trade.ledger.state['reopen_until_epoch'].clear()
        self.trade.ledger.state['forced_close_blocks'] = {'AUSDT': {'until_epoch': self.now+60}}
        self.event()
        self.assertEqual(self.submits, [])

    def test_actual_sync_adapter_and_worker_accept_old_bar_only_for_active_follow(self):
        self.held(3)
        self.event()
        row = self.trade.tracked['AUSDT']
        client = self.trade.client
        client.enabled, client.dry_run = True, False
        self.trade._account_deadline = self.now+100
        self.trade._publish_entry_gate()
        sends = []
        def dispatch(method, *args, before_send=None, **kwargs):
            before_send()
            sends.append(kwargs)
            return dict(symbol='AUSDT', clientOrderId=row['client_order_id'], orderId='sync',
                        status='NEW', executedQty='0')
        client._broker_call = Mock(side_effect=dispatch)
        ExchangeClient.place_limit_for_bar(client, symbol='AUSDT', client_order_id=row['client_order_id'],
            limit_price=1., notional=5., order_bar_open_ms=row['order_bar_open_ms'])
        self.assertEqual(len(sends), 1)
        io = object.__new__(NetworkIO)
        io._entry_gate, io._closed = client.entry_gate, False
        self.assertTrue(io.check_entry('AUSDT', row['client_order_id'], row['order_bar_open_ms']))
        self.targets = []
        self.publish()
        snapshot, status = self.trade.desired_snapshot(now=self.now)
        self.trade._io_signal = snapshot, status
        self.trade._publish_entry_gate()
        io._entry_gate = client.entry_gate
        with self.assertRaises(ConfirmedSubmitRejection):
            io.check_entry('AUSDT', row['client_order_id'], row['order_bar_open_ms'])
        with self.assertRaises(ConfirmedSubmitRejection):
            ExchangeClient.place_limit_for_bar(client, symbol='AUSDT', client_order_id=row['client_order_id'],
                limit_price=1., notional=5., order_bar_open_ms=row['order_bar_open_ms'])
        self.assertEqual(len(sends), 1)

    def test_signal_confirmation_promotes_original_order_without_cancel_or_new_id(self):
        self.targets = []
        self.publish()
        original = dict(symbol='AUSDT', limit_price=1., notional=5., order_bar_open_ms=self.bar,
                        reference_bar_open_ms=self.bar-cfg.BAR_MS, exit_due_epoch=self.now+900)
        self.trade.market.update('AUSDT', '1.01', int(self.now*1000))
        self.trade.market.decide(self.trade.market.events.get_nowait(), lambda event: None)
        self.trade._place_order(original)
        cid = self.trade.tracked['AUSDT']['client_order_id']
        self.bar += cfg.BAR_MS
        self.now += cfg.BAR_MS / 1000
        self.targets = [self.target('AUSDT')]
        self.publish()
        self.refresh_quotes()
        self.trade.step(now=self.now)
        self.assertEqual(self.trade.tracked['AUSDT']['client_order_id'], cid)
        self.assertTrue(self.trade.tracked['AUSDT']['signal_follow'])
        self.assertEqual(self.trade._ordinary_pending_count(), 0)
        self.assertEqual(len(self.submits), 1)
        self.assertEqual(self.cancels, [])

    def test_malformed_target_snapshot_does_not_mean_signal_position_has_ended(self):
        self.event()
        cid = self.trade.tracked['AUSDT']['client_order_id']
        self.payload['target_positions'] = None
        cfg.DESIRED_FILE.write_text(json.dumps(self.payload))
        self.event()
        self.assertEqual(self.trade.tracked['AUSDT']['client_order_id'], cid)
        self.assertEqual(self.cancels, [])

    def test_fill_after_signal_end_is_closed_by_original_due_with_no_reentry(self):
        self.event()
        row = self.trade.tracked['AUSDT']
        self.cancel_response = dict(executedQty='2', avgPrice='1.1')
        self.targets = []
        self.publish()
        self.event()
        self.now = row['exit_due_epoch']
        close = Mock()
        self.trade._close_position = close
        self.trade._timed_exits(now=self.now)
        close.assert_called_once_with('AUSDT', 'fixed_bar_exit', now=self.now)
        self.assertEqual(len(self.submits), 1)

    def test_partial_fill_cancels_remainder_and_never_tops_up_held_position(self):
        self.event()
        cid = self.trade.tracked['AUSDT']['client_order_id']
        self.orders[cid].update(status='PARTIALLY_FILLED', executedQty='2', avgPrice='1.1')
        self.trade.reconcile_orders(now=self.now)
        self.assertEqual(self.trade.positions['AUSDT']['quantity'], 2.)
        self.assertNotIn('AUSDT', self.trade.tracked)
        self.event()
        self.assertEqual(self.trade.positions['AUSDT']['quantity'], 2.)
        self.assertEqual(len(self.submits), 1)

    def test_follow_order_freezes_price_and_quantity_across_tier_changes_and_restart(self):
        self.event()
        original = deepcopy(self.trade.tracked['AUSDT'])
        for count in (1, 2, 0, 3, 5):
            self.trade.positions.clear()
            self.held(count)
            self.trade.ledger.state['realized_balance'] = 100 + count
            self.event()
            row = self.trade.tracked['AUSDT']
            for field in ('client_order_id', 'order_id', 'limit_price', 'qty', 'notional', 'sizing_weight'):
                self.assertEqual(row[field], original[field])
            self.assertEqual(self.trade._ordinary_pending_count(), 0)
            # Direct sizing calls and the actual worker gate must agree too.
            raw = dict(self.target('AUSDT'), signal_follow=True,
                       signal_follow_position_id=self.targets[0]['position_id'])
            self.assertEqual(self.trade._entry_sizing_row(raw)['notional'], original['notional'])
            self.trade._publish_entry_gate()
            io = object.__new__(NetworkIO)
            io._entry_gate, io._closed = self.trade.client.entry_gate, False
            self.assertTrue(io.check_entry('AUSDT', row['client_order_id'], row['order_bar_open_ms']))
        self.make_trade()
        self.event()
        self.assertEqual(self.trade.tracked['AUSDT']['client_order_id'], original['client_order_id'])
        self.assertEqual(self.trade.tracked['AUSDT']['qty'], original['qty'])
        self.assertFalse(self.cancels)
        self.assertEqual(len(self.submits), 1)


if __name__ == '__main__':
    unittest.main()
