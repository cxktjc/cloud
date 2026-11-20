"""Read-only diagnostics preserve socket failure evidence without trade state."""
import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from websockets.exceptions import ConnectionClosedError

from collectors import ws_connection_probe as diagnostic


class ProbeTests(unittest.IsolatedAsyncioTestCase):
    def connection(self, frames):
        class Socket:
            remote_address = ('192.0.2.1', 443)
            recv_exc = ConnectionResetError(104, 'Connection reset by peer')
            protocol = None
            sent = []
            async def send(self, payload):
                self.sent.append(json.loads(payload))
            async def recv(self):
                if frames:
                    return json.dumps(frames.pop(0))
                raise ConnectionClosedError(None, None) from self.recv_exc
        socket = Socket()
        class Connection:
            async def __aenter__(self): return socket
            async def __aexit__(self, *args): pass
        return Connection(), socket

    def quote(self):
        return dict(e='bookTicker', s='XUSDT', st=1, b='1', B='2', u=1)

    async def test_receipt_then_abnormal_close_preserves_root_exception_and_peer(self):
        connection, socket = self.connection([dict(data=self.quote())])
        with patch.object(diagnostic.websockets, 'connect', return_value=connection) as connect, \
                patch.object(diagnostic, 'emit'):
            result = await diagnostic.probe('depth', ['XUSDT'], diagnostic.BOOK_WS_BASE,
                                            'bookTicker', 20)
        self.assertTrue(result['connected'])
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['matching_events'], 1)
        self.assertEqual(result['peer'], socket.remote_address)
        self.assertIn('no close frame', result['exceptions'][0])
        self.assertIn('ConnectionResetError', result['exceptions'][1])
        self.assertEqual(result['transport_error'], result['exceptions'][1:])
        self.assertIsNone(connect.call_args.kwargs['proxy'])
        self.assertEqual(connect.call_args.kwargs['ping_interval'], 30)
        self.assertEqual(connect.call_args.kwargs['max_queue'], 4096)
        self.assertFalse(socket.sent)

    async def test_request_comparison_sends_only_one_public_subscribe(self):
        connection, socket = self.connection([
            dict(id='readonly-probe', result=None), self.quote()])
        with patch.object(diagnostic.websockets, 'connect', return_value=connection) as connect, \
                patch.object(diagnostic, 'emit'):
            result = await diagnostic.probe('depth', ['XUSDT'], diagnostic.BOOK_WS_BASE,
                                            'bookTicker', 20, subscribe=True)
        self.assertEqual(connect.call_args.args, (diagnostic.BOOK_WS_BASE,))
        self.assertEqual(socket.sent, [dict(method='SUBSCRIBE', params=['xusdt@bookTicker'],
                                           id='readonly-probe')])
        self.assertEqual(result['subscription_acks'], 1)
        self.assertEqual(result['matching_events'], 1)

    async def test_opening_failure_does_not_claim_data_or_connection(self):
        with patch.object(diagnostic.websockets, 'connect', side_effect=TimeoutError('opening')), \
                patch.object(diagnostic, 'emit'):
            result = await diagnostic.probe('depth', ['XUSDT'], diagnostic.BOOK_WS_BASE,
                                            'bookTicker', 20)
        self.assertFalse(result['connected'])
        self.assertEqual(result['matching_events'], 0)
        self.assertIsNone(result['lifetime_seconds'])
        self.assertEqual(result['exceptions'], ['TimeoutError: opening'])

    async def test_probe_timeout_is_observation_end_and_not_network_failure(self):
        connection, socket = self.connection([])
        async def wait_for_frame():
            await asyncio.Event().wait()
        socket.recv = wait_for_frame
        with patch.object(diagnostic.websockets, 'connect', return_value=connection), \
                patch.object(diagnostic, 'emit'):
            result = await diagnostic.probe('depth', ['XUSDT'], diagnostic.BOOK_WS_BASE,
                                            'bookTicker', .01)
        self.assertEqual(result['status'], 'observation_finished')
        self.assertEqual(result['exceptions'], [])
        self.assertEqual(result['messages'], 0)

    async def test_comparison_uses_same_group_and_runs_all_four_cases(self):
        calls = []
        async def probe(*args, **kwargs):
            calls.append((args, kwargs))
            await asyncio.sleep(0)
            return dict(case=args[0], matching_events=1, status='observation_finished')
        with patch.object(diagnostic, 'probe', side_effect=probe), patch.object(diagnostic, 'emit'):
            results = await diagnostic.run(['XUSDT', 'YUSDT'], 20)
        self.assertEqual(len(results), 4)
        self.assertEqual(calls[0][0][1], calls[2][0][1])
        self.assertEqual(calls[0][0][1], calls[3][0][1])
        self.assertEqual(calls[1][0][1], ['BTCUSDT'])
        self.assertEqual(calls[3][1], {'subscribe': True})


class ExceptionChainTests(unittest.TestCase):
    def test_transport_exception_supports_old_and_current_library_attributes(self):
        error = ConnectionResetError(104, 'Connection reset by peer')
        for socket in (SimpleNamespace(recv_exc=error), SimpleNamespace(connection_exc=error),
                       SimpleNamespace(connection_exc=None, recv_exc=error)):
            self.assertIs(diagnostic.transport_exception(socket), error)
        self.assertIsNone(diagnostic.transport_exception(None))

    def test_exception_chain_is_bounded_even_when_cyclic(self):
        error = OSError('lost')
        error.__cause__ = error
        self.assertEqual(diagnostic.exception_chain(error), ['OSError: lost'])
