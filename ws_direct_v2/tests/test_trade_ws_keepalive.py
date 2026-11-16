"""Keepalive/reconnect regressions; fake transports and local-only WS server."""
from __future__ import annotations

import asyncio
from concurrent.futures import TimeoutError as FutureTimeoutError
from contextlib import ExitStack
import json
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from websockets.asyncio.server import serve, ServerConnection
from websockets.frames import Frame, Opcode
from websockets.protocol import State
from v9711_live import config as cfg
from v9711_live.exchange import trade_ws_lifecycle as life
from v9711_live.exchange.binance_futures_trade_ws import BinanceFuturesTradeWS, BinanceWSBroker
from v9711_live.execution.client import ExchangeClient, OwnershipLedger
from v9711_live.execution.runner import LiveTrade
from v9711_live.execution.submit_errors import ConfirmedSubmitRejection


async def wait_until(predicate, timeout=2.):
    async def poll():
        while not predicate():
            await asyncio.sleep(.005)
    await asyncio.wait_for(poll(), timeout)


class FakeSocket:
    def __init__(self, *, ack=True):
        self.state = State.OPEN
        self.ack = ack
        self.sent = []
        self.responses = asyncio.Queue()
        self.close_count = 0

    async def send(self, raw):
        payload = json.loads(raw)
        self.sent.append(payload)
        if self.ack:
            self.reply(payload['id'])

    def reply(self, identity):
        self.responses.put_nowait(json.dumps(dict(id=identity, status=200,
            result=dict(orderId=123, status='NEW'))))

    async def recv(self):
        result = await self.responses.get()
        if isinstance(result, Exception):
            raise result
        return result

    async def close(self):
        self.state = State.CLOSED
        self.close_count += 1


class AsyncKeepaliveTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.object(life, 'MONITOR_INTERVAL_SECONDS', .01))
        self.stack.enter_context(patch.object(life, 'RETRY_MIN_SECONDS', .01))
        self.stack.enter_context(patch.object(life, 'RETRY_MAX_SECONDS', .04))
        self.sockets = []
        async def connect(*args, **kwargs):
            socket = FakeSocket()
            self.sockets.append(socket)
            return socket
        self.connect = self.stack.enter_context(patch.object(life.websockets, 'connect', side_effect=connect))
        self.broker = BinanceFuturesTradeWS('test-key', 'test-secret', request_timeout=.05)
        self.addAsyncCleanup(self.broker.close)
        await self.broker.connect()

    async def test_ping_pong_enabled_and_healthy_idle_does_not_reconnect(self):
        kwargs = self.connect.call_args.kwargs
        self.assertEqual(kwargs['ping_interval'], 20.)
        self.assertEqual(kwargs['ping_timeout'], 10.)
        self.assertIsNone(kwargs['proxy'])
        self.broker._connected_at -= 60.
        await asyncio.sleep(.04)
        await self.broker._call('order.status', {'symbol':'TESTUSDT'})
        self.assertEqual(self.connect.call_count, 1)
        self.assertEqual(self.sockets[0].close_count, 0)

    async def test_disconnect_recovers_without_waiting_for_a_new_order(self):
        self.sockets[0].state = State.CLOSED
        await wait_until(lambda: len(self.sockets) == 2)
        self.assertEqual(self.sockets[1].sent, [])
        self.assertTrue(self.broker._socket_open())

    async def test_transport_failures_retry_in_background_without_order_replay(self):
        attempts = 0
        async def flaky(*args, **kwargs):
            nonlocal attempts
            attempts += 1
            if attempts <= 2:
                raise ConnectionError('offline')
            socket = FakeSocket()
            self.sockets.append(socket)
            return socket
        self.connect.side_effect = flaky
        self.sockets[0].state = State.CLOSED
        await wait_until(lambda: len(self.sockets) == 2)
        self.assertEqual(attempts, 3)
        self.assertEqual(self.sockets[1].sent, [])

    async def test_session_rotation_waits_for_inflight_ack(self):
        socket = self.sockets[0]
        socket.ack = False
        self.broker.request_timeout = 1
        call = asyncio.create_task(self.broker._call('order.place', {'newClientOrderId':'exact-id'}))
        await wait_until(lambda: len(socket.sent) == 1)
        self.broker._connected_at -= life.RECONNECT_BEFORE_SECONDS+1
        await asyncio.sleep(.04)
        self.assertEqual(self.connect.call_count, 1)
        self.assertEqual(socket.close_count, 0)
        socket.reply(socket.sent[0]['id'])
        self.assertEqual((await call)['orderId'], 123)
        await wait_until(lambda: self.connect.call_count == 2)
        self.assertEqual(len(socket.sent), 1)
        self.assertEqual(self.sockets[1].sent, [])

    async def test_lost_ack_is_not_resent_after_successful_reconnect(self):
        socket = self.sockets[0]
        socket.ack = False
        with self.assertRaises(asyncio.TimeoutError):
            await self.broker.place_order('TESTUSDT', 'BUY', '5', price='1',
                position_side='LONG', client_order_id='v9712l_exact_test')
        await wait_until(lambda: len(self.sockets) == 2)
        self.assertEqual(len(socket.sent), 1)
        self.assertEqual(socket.sent[0]['params']['newClientOrderId'], 'v9712l_exact_test')
        self.assertEqual(self.sockets[1].sent, [])
        await self.broker.get_order_by_client_order_id('TESTUSDT', 'v9712l_exact_test')
        self.assertEqual([r['method'] for r in self.sockets[1].sent], ['order.status'])

    async def test_plan_is_rechecked_after_reconnect_before_any_send(self):
        await self.broker.close()
        # Reactivate with a closed socket, exactly as a transport recovered later.
        self.broker._stopping = False
        called = []
        def expired():
            called.append(len(self.sockets))
            raise ConfirmedSubmitRejection('expired; request never sent')
        with self.assertRaises(ConfirmedSubmitRejection):
            await self.broker.place_order('TESTUSDT', 'BUY', '5', price='1', position_side='LONG',
                                         client_order_id='v9712l_expired', before_send=expired)
        self.assertEqual(called, [2])
        self.assertEqual(self.sockets[1].sent, [])

    async def test_cancelled_waiting_request_never_sends_after_lock_released(self):
        await self.broker._lock.acquire()
        task = asyncio.create_task(self.broker._call('order.place', {'newClientOrderId':'must-not-send'}))
        await asyncio.sleep(.01)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.broker._lock.release()
        await asyncio.sleep(.02)
        self.assertEqual(self.sockets[0].sent, [])

    async def test_cancelled_inflight_request_invalidates_transport_without_replay(self):
        socket = self.sockets[0]
        socket.ack = False
        self.broker.request_timeout = 1
        task = asyncio.create_task(self.broker._call('order.place', {'newClientOrderId':'unknown-id'}))
        await wait_until(lambda: bool(socket.sent))
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await wait_until(lambda: len(self.sockets) == 2)
        self.assertEqual(len(socket.sent), 1)
        self.assertEqual(self.sockets[1].sent, [])

    async def test_close_cancels_background_recovery(self):
        task = self.broker._supervisor
        await self.broker.close()
        self.sockets[0].state = State.CLOSED
        await asyncio.sleep(.03)
        self.assertTrue(task.done())
        self.assertEqual(self.connect.call_count, 1)
        self.assertIsNone(self.broker._supervisor)


class LocalWireTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for key,value in dict(PING_INTERVAL_SECONDS=.02, PING_TIMEOUT_SECONDS=.1,
                              MONITOR_INTERVAL_SECONDS=.01, RETRY_MIN_SECONDS=.01,
                              RETRY_MAX_SECONDS=.04).items():
            self.stack.enter_context(patch.object(life, key, value))
        self.peers = []
        self.messages = []
        self.pings = []
        self.drop_next_ack = False
        self.drop_pongs = False
        owner = self
        class CountConnection(ServerConnection):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                original = self.protocol.send_frame
                def send_frame(frame):
                    if frame.opcode == Opcode.PONG and owner.drop_pongs:
                        return
                    return original(frame)
                self.protocol.send_frame = send_frame
            def process_event(self, event):
                if isinstance(event, Frame) and event.opcode == Opcode.PING:
                    owner.pings.append(event)
                super().process_event(event)
        async def handler(peer):
            self.peers.append(peer)
            try:
                async for raw in peer:
                    data = json.loads(raw)
                    self.messages.append(data)
                    if self.drop_next_ack:
                        self.drop_next_ack = False
                        await peer.close()
                        break
                    await peer.send(json.dumps(dict(id=data['id'],status=200,result={'orderId':321,'status':'NEW'})))
            except Exception:
                pass
        self.server = await serve(handler, '127.0.0.1', 0, ping_interval=None,
                                  create_connection=CountConnection)
        port = self.server.sockets[0].getsockname()[1]
        self.broker = BinanceFuturesTradeWS('local-test-key','local-test-secret',
            ws_url=f'ws://127.0.0.1:{port}', request_timeout=.2)
        self.addAsyncCleanup(self.cleanup)
        await self.broker.connect()

    async def cleanup(self):
        await self.broker.close()
        self.server.close()
        await self.server.wait_closed()

    async def test_real_ping_pong_works_with_no_application_messages(self):
        await wait_until(lambda: len(self.pings) >= 3)
        self.assertEqual(len(self.peers), 1)
        self.assertEqual(self.messages, [])
        self.assertEqual(self.broker._connect_count, 1)
        self.assertEqual((await self.broker.get_order('TESTUSDT', 321))['orderId'], 321)

    async def test_missing_pong_recovers_in_background_without_application_requests(self):
        self.drop_pongs = True
        await wait_until(lambda: len(self.peers) >= 2)
        self.drop_pongs = False
        self.assertGreaterEqual(len(self.pings), 1)
        self.assertEqual(self.messages, [])
        self.assertEqual((await self.broker.get_order('TESTUSDT', 321))['orderId'], 321)

    async def test_real_remote_close_is_recovered_without_order_trigger(self):
        await self.peers[0].close()
        await wait_until(lambda: len(self.peers) >= 2)
        self.assertEqual(self.messages, [])
        self.assertEqual((await self.broker.get_order('TESTUSDT', 321))['orderId'], 321)
        self.assertEqual(len(self.messages), 1)

    async def test_real_disconnect_after_send_never_resends_the_submission(self):
        self.drop_next_ack = True
        with self.assertRaises(Exception):
            await self.broker.place_order('TESTUSDT','SELL','5',position_side='LONG',
                                         client_order_id='v9712l_exact_close')
        await wait_until(lambda: len(self.peers) >= 2)
        self.assertEqual(len(self.messages), 1)
        self.assertEqual(self.messages[0]['method'], 'order.place')
        await self.broker.get_order_by_client_order_id('TESTUSDT','v9712l_exact_close')
        self.assertEqual([r['method'] for r in self.messages], ['order.place','order.status'])


class AdapterTests(unittest.TestCase):
    def test_old_idle_connection_is_not_forcibly_reconnected(self):
        client = ExchangeClient(enabled=True, dry_run=False)
        broker = Mock()
        client._broker = client._io_broker = broker
        client._broker_last_io_mono = time.monotonic()-3600
        self.assertIs(client._ready_broker(), broker)
        broker.reconnect.assert_not_called()

    def test_recovered_managed_broker_is_not_forcibly_reconnected_again(self):
        class Managed:
            MANAGES_WS_LIFECYCLE = True
            reconnect = Mock()
        client = ExchangeClient(enabled=True,dry_run=False)
        broker = Managed()
        client._broker = client._io_broker = broker
        client._broker_reconnect_needed = True
        self.assertIs(client._ready_broker(),broker)
        broker.reconnect.assert_not_called()

    def test_sync_wrapper_shuts_down_supervisor_loop_and_thread(self):
        async def connect(*args, **kwargs):
            return FakeSocket()
        with patch.object(life.websockets, 'connect', side_effect=connect):
            wrapper = BinanceWSBroker('local-test-key', 'local-test-secret')
            task = wrapper._broker._supervisor
            try:
                self.assertTrue(wrapper._thread.is_alive())
                self.assertFalse(task.done())
            finally:
                wrapper.close()
            self.assertTrue(task.done())
            self.assertFalse(wrapper._thread.is_alive())
            self.assertTrue(wrapper._loop.is_closed())

    def test_initial_connection_failure_leaves_no_broker_thread_or_background_task(self):
        before = {thread.ident for thread in threading.enumerate() if thread.name == 'BinanceWSBrokerLoop'}
        with patch.object(life.websockets, 'connect', side_effect=ConnectionError('offline')):
            with self.assertRaises(ConnectionError):
                BinanceWSBroker('local-test-key', 'local-test-secret')
        after = {thread.ident for thread in threading.enumerate() if thread.name == 'BinanceWSBrokerLoop'}
        self.assertEqual(after, before)

    def test_sync_timeout_cancels_pending_coroutine(self):
        wrapper = object.__new__(BinanceWSBroker)
        wrapper._loop = Mock()
        wrapper.request_timeout = 1
        future = Mock()
        future.result.side_effect = FutureTimeoutError()
        coroutine = asyncio.sleep(0)
        try:
            with patch('asyncio.run_coroutine_threadsafe', return_value=future):
                with self.assertRaises(FutureTimeoutError):
                    wrapper._run(coroutine, timeout=.01)
            future.cancel.assert_called_once()
        finally:
            coroutine.close()

    def test_market_close_wrapper_never_blindly_replays_via_rest(self):
        wrapper = object.__new__(BinanceWSBroker)
        wrapper.place_order_checked = Mock(side_effect=ConnectionError('ACK lost'))
        with patch('v9711_live.exchange.binance_futures_trade_ws._rest_place_market_order') as rest:
            with self.assertRaises(ConnectionError):
                wrapper.place_order('TESTUSDT','SELL','5',position_side='LONG')
        rest.assert_not_called()


class LostAckLedgerTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        root = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        values = dict(STATE_DIR=root/'state', EXECUTION_INTENT_FILE=root/'state/intents.json',
            EXECUTION_LEDGER_FILE=root/'state/ledger.json', EXCHANGE_INFO_CACHE_FILE=root/'filters.json',
            ACCOUNT_STREAM_DIR=root/'account', PRICE_FILE=root/'prices.json')
        for key,value in values.items():
            self.stack.enter_context(patch.object(cfg,key,value))
        cfg.EXCHANGE_INFO_CACHE_FILE.write_text(json.dumps({'symbols':{'TESTUSDT':dict(tick='.01',step='1',min_qty='1',min_notional='5')}}))
        self.trade = LiveTrade(enabled=True,dry_run=False,once=True,nonblocking_io=False)
        self.broker = self.make_broker()
        self.trade.client._broker = self.broker
        self.now = time.time()
        self.bar = int(self.now*1000)//cfg.BAR_MS*cfg.BAR_MS
        self.row = dict(symbol='TESTUSDT',limit_price=1.,notional=5.,order_bar_open_ms=self.bar,
                        reference_bar_open_ms=self.bar-cfg.BAR_MS,exit_due_epoch=self.now+900)

    def make_broker(self):
        class Managed:
            MANAGES_WS_LIFECYCLE = True
            def __init__(self):
                self.sent = []
                self.queries = []
                self.reconnect = Mock()
            def place_order_checked(self,*args,before_send=None,**kwargs):
                if before_send:
                    before_send()
                self.sent.append((args,kwargs))
                raise ConnectionError('accepted by exchange but response connection closed')
            def get_order_by_client_order_id(self,symbol,cid):
                self.queries.append(cid)
                return dict(symbol=symbol, clientOrderId=cid, orderId=777, status='FILLED',executedQty='5',avgPrice='1.1')
        return Managed()

    def restart(self):
        trade = LiveTrade(enabled=True,dry_run=False,once=True,nonblocking_io=False)
        trade.client._broker = self.broker
        return trade

    def test_entry_identity_survives_disconnect_and_restart_then_exact_query_books_fill(self):
        self.trade._place_order(self.row)
        cid = self.trade.tracked['TESTUSDT']['client_order_id']
        self.assertEqual(self.trade.tracked['TESTUSDT']['last_status'],'UNKNOWN')
        self.assertTrue(self.trade.ledger.owns_client(cid))
        self.assertEqual(len(self.broker.sent),1)
        self.trade._place_order(self.row)
        self.assertEqual(len(self.broker.sent),1)
        recovered = self.restart()
        self.assertEqual(recovered.tracked['TESTUSDT']['client_order_id'],cid)
        recovered.reconcile_orders(now=self.now+8)
        self.assertEqual(recovered.positions['TESTUSDT']['quantity'],5.)
        self.assertEqual(recovered.positions['TESTUSDT']['entry_client_order_id'],cid)
        self.assertNotIn('TESTUSDT',recovered.tracked)
        self.assertEqual(len(self.broker.sent),1)
        self.assertEqual(self.broker.queries,[cid])
        recovered.reconcile_orders(now=self.now+9)
        self.assertEqual(recovered.positions['TESTUSDT']['quantity'],5.)

    def test_cancel_lost_ack_retains_exact_order_and_books_racing_fill_after_restart(self):
        self.trade._place_order(self.row)
        row = self.trade.tracked['TESTUSDT']
        cid = row['client_order_id']
        row.update(order_id=777,fate_unknown=False,last_status='NEW')
        self.trade.ledger.register(symbol='TESTUSDT',client_order_id=cid,order_id=777,kind='entry',qty=5)
        self.broker.cancel_order = Mock(side_effect=ConnectionError('cancel ACK lost'))
        self.trade._request_cancel('TESTUSDT',row,'rotation')
        self.assertIn('TESTUSDT', self.trade.tracked)
        self.assertTrue(self.trade.ledger.owns_order('TESTUSDT',777,cid))
        self.assertTrue(row['cancel_requested'])
        recovered = self.restart()
        self.assertEqual(recovered.tracked['TESTUSDT']['client_order_id'], cid)
        recovered.client.query_exact_order = Mock(return_value=(dict(status='FILLED',orderId=777,
            clientOrderId=cid,executedQty='5',avgPrice='1.1'),True))
        recovered.reconcile_orders(now=self.now+8)
        self.assertEqual(recovered.positions['TESTUSDT']['quantity'], 5.)
        self.assertNotIn('TESTUSDT', recovered.tracked)
        self.broker.cancel_order.assert_called_once()
        self.assertEqual(len(self.broker.sent), 1)

    def test_unknown_close_remains_owned_then_query_confirms_flat_without_resend(self):
        self.trade.positions['TESTUSDT'] = dict(symbol='TESTUSDT',side='LONG',quantity=5.,entry_price=1.,
            entry_client_order_id='v9712l_entry_original',entry_epoch=self.now-600,exit_due_epoch=self.now)
        self.trade._close_position('TESTUSDT','fixed_bar_exit',now=self.now)
        pos = self.trade.positions['TESTUSDT']
        cid = pos['close_client_order_id']
        self.assertEqual(pos['close_status'],'UNKNOWN')
        self.assertTrue(self.trade.ledger.owns_client(cid))
        self.assertEqual(len(self.broker.sent),1)
        recovered = self.restart()
        self.assertEqual(recovered.positions['TESTUSDT']['quantity'],5.)
        self.assertEqual(recovered.positions['TESTUSDT']['close_client_order_id'],cid)
        recovered._reconcile_position_close('TESTUSDT',now=self.now+8)
        self.assertNotIn('TESTUSDT',recovered.positions)
        self.assertEqual(len(self.broker.sent),1)
        self.assertEqual(self.broker.queries,[cid])
        self.assertAlmostEqual(recovered.ledger.state['realized_balance'],15.+.5-5.*2.1*cfg.TRADE_FEE_RATE)


if __name__ == '__main__':
    unittest.main()
