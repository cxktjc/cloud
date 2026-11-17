"""Offline regressions for v2's durable twelve-round, five-second close ladder."""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from v9711_live import config as cfg
from v9711_live.execution import close_ladder as policy
from v9711_live.execution.client import OwnershipLedger
from v9711_live.execution.runner import LiveTrade


SYMBOL = 'TESTUSDT'
REASON = 'fixed_bar_exit'


class OfflineExchange:
    """Exact per-ID exchange state; this fake has no transport or credentials."""
    def __init__(self):
        self.orders = {}
        self.queue_calls = []
        self.market_calls = []
        self.cancel_calls = []
        self.queue_error = None
        self.queue_reply = None
        self.cancel_error = None
        self.cancel_reply = None
        self.query_unknown = False
        self.query_negative = False
        self.next_id = 1

    def _place(self, phase, *, symbol, qty, client_order_id):
        calls = self.queue_calls if phase == policy.QUEUE else self.market_calls
        calls.append(dict(symbol=symbol, qty=qty, client_order_id=client_order_id))
        if phase == policy.QUEUE and self.queue_error is not None:
            raise self.queue_error
        result = dict(symbol=symbol, clientOrderId=client_order_id,
                      orderId=str(self.next_id), status='NEW', executedQty='0', avgPrice='0')
        self.next_id += 1
        if phase == policy.QUEUE and self.queue_reply is not None:
            result = deepcopy(self.queue_reply)
        self.orders[client_order_id] = deepcopy(result)
        return result

    def place_queue_close(self, **kwargs):
        return self._place(policy.QUEUE, **kwargs)

    def place_market_close(self, **kwargs):
        return self._place(policy.MARKET, **kwargs)

    def query_exact_order(self, ledger, *, symbol, order_id, client_order_id, now):
        if self.query_unknown:
            return None, False
        if self.query_negative:
            return None, True
        row = self.orders.get(client_order_id)
        return deepcopy(row), row is not None

    def cancel(self, *, symbol, order_id, client_order_id):
        self.cancel_calls.append(dict(symbol=symbol, order_id=order_id, client_order_id=client_order_id))
        if self.cancel_error is not None:
            raise self.cancel_error
        row = deepcopy(self.orders[client_order_id])
        row['status'] = 'CANCELED'
        if self.cancel_reply is not None:
            row = deepcopy(self.cancel_reply)
        self.orders[client_order_id] = deepcopy(row)
        return row


class OfflineTrade(policy.CloseLongMixin):
    """Use production close logic and real persistence without a live client."""
    _finite = staticmethod(LiveTrade._finite)
    _timed_exits = LiveTrade._timed_exits

    def __init__(self, directory, client=None):
        directory = Path(directory)
        self.ledger = OwnershipLedger(path=directory/'intents.json', state_path=directory/'ledger.json')
        self.client = client or OfflineExchange()
        self.stats = {}
        self._network_io = None
        self.defer_network = False
        self.pending_network = []

    @property
    def positions(self):
        return self.ledger.state['owned_positions']

    def _run_network(self, key, action, complete, *, priority):
        if self.defer_network:
            self.pending_network.append((key, action, complete))
            return
        try:
            result, error = action(), None
        except Exception as exc:
            result, error = None, exc
        complete(result, error)

    def complete_pending(self, epoch):
        _, action, complete = self.pending_network.pop(0)
        try:
            result, error = action(), None
        except Exception as exc:
            result, error = None, exc
        with patch.object(policy.time, 'time', return_value=epoch):
            complete(result, error)


class CloseLadderV2Tests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.trade = OfflineTrade(self.tmp.name)
        self.trade.positions[SYMBOL] = dict(symbol=SYMBOL, side='LONG', quantity=10.,
            entry_price=10., entry_epoch=1., entry_client_order_id='v9712l_entry_test',
            exit_due_epoch=103., exit_due_ms=103000)
        self.trade.ledger.state['realized_balance'] = 100.
        self.trade.ledger.save()
        self.enabled = patch.object(policy, 'LIMIT_ENABLED', True)
        self.enabled.start()
        self.addCleanup(self.enabled.stop)
        self.monotonic = patch.object(policy, '_monotonic', return_value=0.)
        self.monotonic.start()
        self.addCleanup(self.monotonic.stop)

    @property
    def pos(self):
        return self.trade.positions[SYMBOL]

    @property
    def exchange(self):
        return self.trade.client

    def close(self, now, reason=REASON):
        self.trade._close_position(SYMBOL, reason, now=now)

    def next_round(self, now):
        """Allow either same-turn or following-turn terminal resubmission."""
        previous = self.pos.get('close_client_order_id')
        self.close(now)
        if not self.pos.get('close_client_order_id'):
            self.close(now)
        self.assertNotEqual(self.pos.get('close_client_order_id'), previous)

    def seed_exact(self, *, rounds=None, attempt=0, phase=None, ack=100., requested=100., deadline=160.):
        phase = phase or policy.QUEUE
        cid = self.trade.ledger.client_id(SYMBOL, 103000, 'legacy_close')
        row = dict(symbol=SYMBOL, clientOrderId=cid, orderId='900', status='NEW',
                   executedQty='0', avgPrice='0')
        self.exchange.orders[cid] = row
        self.pos.update(close_phase=phase, close_client_order_id=cid, close_order_id='900',
                        close_quantity=10., close_accounted_qty=0., close_accounted_quote=0.,
                        close_requested_epoch=requested, close_deadline_epoch=deadline,
                        close_attempt=attempt, close_status='NEW')
        if ack is not None:
            self.pos['close_ack_epoch'] = ack
        if rounds is not None:
            self.pos['close_queue_rounds'] = rounds
        self.trade.ledger.register(symbol=SYMBOL, client_order_id=cid, order_id='900', kind='close', qty=10.)
        return cid

    def restart(self):
        self.trade.ledger.save()
        self.trade = OfflineTrade(self.tmp.name, self.exchange)

    def test_fixed_v2_timing_and_attempt_contract(self):
        self.assertEqual(policy.LIMIT_SECONDS, 5.0)
        self.assertEqual(policy.LIMIT_ATTEMPTS, 12)
        self.assertEqual(policy.TIMED_EXIT_LEAD_SECONDS, 1.0)

    def test_twelve_queue_rounds_then_exactly_one_market_with_unique_identities(self):
        self.close(100.)
        ids = [self.pos['close_client_order_id']]
        for round_number in range(1, 13):
            self.assertEqual(self.pos['close_phase'], policy.QUEUE)
            self.assertEqual(self.pos['close_queue_rounds'], round_number)
            deadline = self.pos['close_deadline_epoch']
            self.close(deadline-0.001)
            self.assertEqual(len(self.exchange.cancel_calls), round_number-1)
            self.next_round(deadline)
            ids.append(self.pos['close_client_order_id'])
            self.assertEqual(len(self.exchange.queue_calls), min(round_number+1, 12))
            self.assertEqual(len(self.exchange.market_calls), int(round_number == 12))
        self.assertEqual(len(set(ids)), 13)
        self.assertEqual(self.pos['close_phase'], policy.MARKET)
        self.assertEqual(self.pos['close_queue_rounds'], 12)
        self.close(1000.)
        self.assertEqual(len(self.exchange.queue_calls), 12)
        self.assertEqual(len(self.exchange.market_calls), 1)

    def test_partial_fills_and_cancel_race_book_only_cumulative_deltas_across_rounds(self):
        self.close(100.)
        first = self.pos['close_client_order_id']
        self.exchange.orders[first].update(status='PARTIALLY_FILLED', executedQty='4', avgPrice='11')
        self.close(101.)
        first_balance = self.trade.ledger.state['realized_balance']
        self.close(102.)
        self.assertEqual(self.pos['quantity'], 6.)
        self.assertEqual(self.trade.ledger.state['realized_balance'], first_balance)
        self.exchange.cancel_reply = dict(self.exchange.orders[first], status='CANCELED', executedQty='6', avgPrice='12')
        self.next_round(105.)
        self.assertEqual(self.pos['close_queue_rounds'], 2)
        self.assertAlmostEqual(self.exchange.queue_calls[-1]['qty'], 4.)
        self.assertAlmostEqual(self.trade.ledger.state['realized_balance'], 100.+12.-132.*cfg.TRADE_FEE_RATE)
        second = self.pos['close_client_order_id']
        self.exchange.orders[second].update(status='PARTIALLY_FILLED', executedQty='1', avgPrice='13')
        self.close(106.)
        self.exchange.cancel_reply = dict(self.exchange.orders[second], status='CANCELED', executedQty='2', avgPrice='14')
        self.next_round(110.)
        self.assertAlmostEqual(self.exchange.queue_calls[-1]['qty'], 2.)
        self.assertAlmostEqual(self.pos['quantity'], 2.)
        self.assertAlmostEqual(self.trade.ledger.state['realized_balance'], 100.+20.-180.*cfg.TRADE_FEE_RATE)
        # A delayed old-order response must not book a fill on the new identity.
        before = deepcopy(self.pos)
        self.trade._settle_close_confirmation(SYMBOL, self.exchange.cancel_reply, now=111.)
        self.assertEqual(self.pos['quantity'], before['quantity'])
        self.assertEqual(self.pos['close_client_order_id'], before['close_client_order_id'])
        self.assertEqual(self.exchange.market_calls, [])

    def test_full_fill_racing_cancel_finishes_owned_position_without_another_order(self):
        self.close(100.)
        cid = self.pos['close_client_order_id']
        self.exchange.cancel_reply = dict(self.exchange.orders[cid],status='FILLED',executedQty='10',avgPrice='11')
        self.close(105.)
        self.assertNotIn(SYMBOL, self.trade.positions)
        self.assertEqual(len(self.exchange.queue_calls), 1)
        self.assertEqual(self.exchange.market_calls, [])
        self.assertFalse(self.trade.ledger.owns_client(cid))

    def test_unknown_cancel_transport_failure_never_replaces_live_queue(self):
        self.close(100.)
        cid = self.pos['close_client_order_id']
        self.exchange.cancel_error = TimeoutError('cancel execution status unknown')
        self.close(105.)
        self.close(120.)
        self.assertEqual(self.pos['close_client_order_id'], cid)
        self.assertEqual(self.pos['close_queue_rounds'], 1)
        self.assertEqual(len(self.exchange.queue_calls), 1)
        self.assertEqual(self.exchange.market_calls, [])
        self.assertTrue(self.trade.ledger.owns_client(cid))

    def test_nonterminal_cancel_ack_never_authorizes_replacement(self):
        self.close(100.)
        cid = self.pos['close_client_order_id']
        self.exchange.cancel_reply = dict(self.exchange.orders[cid], status='NEW')
        self.close(105.)
        self.close(120.)
        self.assertEqual(self.pos['close_client_order_id'], cid)
        self.assertEqual(len(self.exchange.queue_calls), 1)
        self.assertEqual(self.exchange.market_calls, [])

    def test_terminal_cancel_without_cumulative_fill_keeps_exact_identity(self):
        self.close(100.)
        cid = self.pos['close_client_order_id']
        missing = dict(self.exchange.orders[cid], status='CANCELED')
        missing.pop('executedQty')
        self.exchange.cancel_reply = missing
        self.close(105.)
        self.close(120.)
        self.assertEqual(self.pos['close_client_order_id'], cid)
        self.assertEqual(self.pos['quantity'], 10.)
        self.assertEqual(len(self.exchange.queue_calls), 1)
        self.assertEqual(self.exchange.market_calls, [])

    def test_unknown_submission_reserves_one_round_without_duplicate_placement(self):
        self.exchange.queue_error = TimeoutError('submit timeout')
        self.exchange.query_unknown = True
        self.close(100.)
        cid = self.pos['close_client_order_id']
        self.restart()
        self.close(1000.)
        self.assertEqual(self.pos['close_client_order_id'], cid)
        self.assertEqual(self.pos['close_queue_rounds'], 1)
        self.assertEqual(self.pos['close_status'], 'UNKNOWN')
        self.assertEqual(len(self.exchange.queue_calls), 1)
        self.assertEqual(self.exchange.market_calls, [])

    def test_missing_submission_ack_is_unknown_and_never_a_new_round(self):
        self.exchange.queue_reply = {}
        self.close(100.)
        cid = self.pos['close_client_order_id']
        self.close(1000.)
        self.assertEqual(self.pos['close_client_order_id'], cid)
        self.assertEqual(self.pos['close_queue_rounds'], 1)
        self.assertEqual(len(self.exchange.queue_calls), 1)
        self.assertEqual(self.exchange.market_calls, [])

    def test_definitive_queue_rejection_falls_back_without_using_remaining_rounds(self):
        self.exchange.queue_error = RuntimeError('Filter failure: LOT_SIZE')
        self.close(100.)
        self.close(100.)
        self.assertEqual(len(self.exchange.queue_calls), 1)
        self.assertEqual(len(self.exchange.market_calls), 1)
        self.assertEqual(self.pos['close_queue_rounds'], 1)
        self.assertEqual(self.pos['close_phase'], policy.MARKET)

    def test_urgent_close_cancels_exact_queue_and_skips_remaining_queue_rounds(self):
        self.close(100.)
        self.close(101., 'operator_emergency')
        if not self.pos.get('close_client_order_id'):
            self.close(101., 'operator_emergency')
        self.assertEqual(len(self.exchange.cancel_calls), 1)
        self.assertEqual(len(self.exchange.queue_calls), 1)
        self.assertEqual(len(self.exchange.market_calls), 1)
        self.assertEqual(self.pos['close_phase'], policy.MARKET)

    def test_ack_queue_residence_does_not_consume_exchange_five_seconds(self):
        self.trade._network_io = object()
        self.trade.defer_network = True
        self.close(100.)
        self.assertEqual(self.pos['close_queue_rounds'], 1)
        self.assertTrue(self.pos['close_timer_pending'])
        # Placement has not yet been sent; no exchange ID authorizes a cancel.
        self.close(130.)
        self.assertEqual(len(self.trade.pending_network), 1)
        self.assertEqual(self.exchange.cancel_calls, [])
        self.trade.complete_pending(140.)
        self.assertEqual(self.pos['close_ack_epoch'], 140.)
        self.assertEqual(self.pos['close_deadline_epoch'], 145.)
        self.trade.defer_network = False
        self.close(144.999)
        self.assertEqual(self.exchange.cancel_calls, [])
        with patch.object(policy.time,'time',return_value=145.):
            self.close(145.)
        self.assertEqual(len(self.exchange.cancel_calls), 1)

    def test_restart_after_seventh_reserved_round_continues_at_eighth_not_first(self):
        self.seed_exact(rounds=7, attempt=23, deadline=105.)
        self.restart()
        self.next_round(105.)
        self.assertEqual(self.pos['close_queue_rounds'], 8)
        self.assertEqual(len(self.exchange.queue_calls), 1)
        self.assertEqual(self.exchange.market_calls, [])
        self.assertEqual(self.pos['close_attempt'], 24)

    def test_restart_after_twelfth_reserved_round_goes_market_without_thirteenth_queue(self):
        self.seed_exact(rounds=12, attempt=30, deadline=105.)
        self.restart()
        self.next_round(105.)
        self.assertEqual(self.pos['close_phase'], policy.MARKET)
        self.assertEqual(self.pos['close_queue_rounds'], 12)
        self.assertEqual(self.exchange.queue_calls, [])
        self.assertEqual(len(self.exchange.market_calls), 1)

    def test_legacy_queue_round_missing_restores_one_and_caps_old_sixty_second_deadline(self):
        cid = self.seed_exact(deadline=160., ack=100.)
        self.restart()
        self.close(104.)
        self.assertEqual(self.pos['close_client_order_id'], cid)
        self.assertEqual(self.pos['close_queue_rounds'], 1)
        self.assertEqual(self.pos['close_deadline_epoch'], 105.)
        self.assertEqual(self.exchange.cancel_calls, [])
        self.next_round(105.)
        self.assertEqual(self.pos['close_queue_rounds'], 2)

    def test_legacy_queue_without_ack_uses_requested_epoch_and_never_extends_earlier_deadline(self):
        self.seed_exact(deadline=160., ack=None, requested=100.)
        self.close(104.)
        self.assertEqual(self.pos['close_deadline_epoch'], 105.)
        self.pos['close_ack_epoch'] = 100.
        self.pos['close_deadline_epoch'] = 104.5
        self.close(104.1)
        self.assertEqual(self.pos['close_deadline_epoch'], 104.5)
        self.assertEqual(self.exchange.cancel_calls, [])

    def test_legacy_market_unknown_retains_its_identity_and_deadline_without_queue(self):
        cid = self.seed_exact(phase=policy.MARKET, deadline=160.)
        self.exchange.query_unknown = True
        self.restart()
        self.close(1000.)
        self.assertEqual(self.pos['close_client_order_id'], cid)
        self.assertEqual(self.pos['close_phase'], policy.MARKET)
        self.assertEqual(self.pos['close_deadline_epoch'], 160.)
        self.assertNotIn('close_queue_rounds', self.pos)
        self.assertEqual(self.exchange.queue_calls, [])
        self.assertEqual(self.exchange.market_calls, [])

    def test_timed_exit_starts_one_second_early_without_mutating_due_fields(self):
        original = (self.pos['exit_due_epoch'], self.pos['exit_due_ms'])
        self.trade._timed_exits(now=100.)
        self.assertEqual(self.exchange.queue_calls, [])
        self.trade._timed_exits(now=101.999)
        self.assertEqual(self.exchange.queue_calls, [])
        self.trade._timed_exits(now=102.)
        self.assertEqual(len(self.exchange.queue_calls), 1)
        self.assertEqual((self.pos['exit_due_epoch'], self.pos['exit_due_ms']), original)
        self.assertEqual(self.pos['close_requested_epoch'], 102.)
        self.assertEqual(self.pos['close_deadline_epoch'], 107.)

    def test_disabled_queue_after_restart_cannot_use_persisted_between_round_phase(self):
        cid = self.seed_exact(rounds=7, deadline=105.)
        self.trade._settle_close_confirmation(SYMBOL,
            dict(self.exchange.orders[cid],status='CANCELED'), now=105.)
        self.assertEqual(self.pos['close_next_phase'], policy.QUEUE)
        self.restart()
        with patch.object(policy, 'LIMIT_ENABLED', False):
            self.close(106.)
        self.assertEqual(self.pos['close_phase'], policy.MARKET)
        self.assertEqual(self.pos['close_queue_rounds'], 7)
        self.assertEqual(self.exchange.queue_calls, [])
        self.assertEqual(len(self.exchange.market_calls), 1)

    def test_known_negative_query_is_not_proof_that_unknown_submit_can_be_replaced(self):
        self.exchange.queue_error = TimeoutError('submission fate unknown')
        self.exchange.query_negative = True
        self.close(100.)
        cid = self.pos['close_client_order_id']
        self.close(1000.)
        self.restart()
        self.close(2000.)
        self.assertEqual(self.pos['close_client_order_id'], cid)
        self.assertEqual(self.pos['close_queue_rounds'], 1)
        self.assertEqual(len(self.exchange.queue_calls), 1)
        self.assertEqual(self.exchange.market_calls, [])
        self.assertTrue(self.trade.ledger.owns_client(cid))

    def test_twelfth_partial_cancel_race_markets_only_residual_and_full_fill_settles_once(self):
        old_cid = self.seed_exact(rounds=12, attempt=43, deadline=105.)
        self.exchange.orders[old_cid].update(status='PARTIALLY_FILLED',executedQty='4',avgPrice='11')
        self.close(101.)
        self.assertEqual(self.pos['quantity'], 6.)
        self.exchange.cancel_reply = dict(self.exchange.orders[old_cid],status='CANCELED',executedQty='6',avgPrice='12')
        self.next_round(105.)
        self.assertEqual(self.pos['close_phase'], policy.MARKET)
        self.assertEqual(self.pos['close_queue_rounds'], 12)
        self.assertEqual(self.exchange.queue_calls, [])
        self.assertEqual(len(self.exchange.market_calls), 1)

        self.assertEqual(self.exchange.market_calls[0]['qty'], 4.)
        self.assertAlmostEqual(self.trade.ledger.state['realized_balance'], 100.+12.-132.*cfg.TRADE_FEE_RATE)
        market_cid = self.pos['close_client_order_id']
        self.assertNotEqual(market_cid, old_cid)
        self.exchange.orders[market_cid].update(status='FILLED',executedQty='4',avgPrice='13')
        self.close(106.)
        final_balance = 100.+24.-224.*cfg.TRADE_FEE_RATE
        self.assertAlmostEqual(self.trade.ledger.state['realized_balance'], final_balance)
        self.assertNotIn(SYMBOL, self.trade.positions)
        self.assertFalse(self.trade.ledger.owns_client(old_cid))
        self.assertFalse(self.trade.ledger.owns_client(market_cid))
        self.close(2000.)
        self.assertAlmostEqual(self.trade.ledger.state['realized_balance'], final_balance)
        self.assertEqual(len(self.exchange.market_calls), 1)

    def test_synchronous_four_second_ack_delay_preserves_full_five_second_rest(self):
        with patch.object(policy, '_monotonic', side_effect=[100., 104.]):
            self.close(100.)
        self.assertEqual(self.pos['close_ack_epoch'], 104.)
        self.assertEqual(self.pos['close_deadline_epoch'], 109.)
        self.assertEqual(self.pos['close_queue_rounds'], 1)
        self.close(108.999)
        self.assertEqual(self.exchange.cancel_calls, [])
        self.next_round(109.)
        self.assertEqual(len(self.exchange.cancel_calls), 1)
        self.assertEqual(self.pos['close_queue_rounds'], 2)


if __name__ == '__main__':
    unittest.main(verbosity=2)
