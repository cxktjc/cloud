"""Read-only diagnostics preserve socket failure evidence without trade state."""
import asyncio
import json
import logging
import ssl
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from websockets.exceptions import ConnectionClosedError
from websockets.frames import Frame, Opcode

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

    async def test_heartbeat_comparison_pins_same_peer_and_changes_only_ping_interval(self):
        calls = []
        async def probe(*args, **kwargs):
            calls.append((args, kwargs))
            return dict(case=args[0], matching_events=1, status='observation_finished')
        with patch.object(diagnostic, 'probe', side_effect=probe), patch.object(diagnostic, 'emit'):
            await diagnostic.run(['XUSDT'], 20, heartbeat_compare=True, peer_ip='192.0.2.1')
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0][0][1:], calls[1][0][1:])
        self.assertEqual(calls[0][0][1], ['BTCUSDT'])
        self.assertEqual([kw['ping_interval'] for _, kw in calls], [30, 2])
        self.assertTrue(all(kw['peer_ip'] == '192.0.2.1' for _, kw in calls))

    async def test_pinned_peer_preserves_original_tls_hostname_and_validation(self):
        connection, _ = self.connection([self.quote()])
        with patch.object(diagnostic.websockets, 'connect', return_value=connection) as connect, \
                patch.object(diagnostic, 'emit'):
            result = await diagnostic.probe('depth', ['XUSDT'], diagnostic.BOOK_WS_BASE,
                'bookTicker', 20, ping_interval=2, peer_ip='192.0.2.1')
        options = connect.call_args.kwargs
        self.assertEqual(options['host'], '192.0.2.1')
        self.assertEqual(options['server_hostname'], 'fstream.binance.com')
        self.assertNotIn('ssl', options)
        self.assertEqual(result['ping_interval'], 2)
        self.assertFalse(result['observation_end_requested'])

    async def test_tls12_comparison_keeps_certificate_and_hostname_verification(self):
        connection, socket = self.connection([self.quote()])
        socket.transport = SimpleNamespace(get_extra_info=lambda name:
            SimpleNamespace(version=lambda: 'TLSv1.2') if name == 'ssl_object' else None)
        with patch.object(diagnostic.websockets, 'connect', return_value=connection) as connect, \
                patch.object(diagnostic, 'emit'):
            result = await diagnostic.probe('tls12', ['XUSDT'], diagnostic.BOOK_WS_BASE,
                                            'bookTicker', 20, tls12=True)
        context = connect.call_args.kwargs['ssl']
        self.assertEqual(context.minimum_version, ssl.TLSVersion.TLSv1_2)
        self.assertEqual(context.maximum_version, ssl.TLSVersion.TLSv1_2)
        self.assertTrue(context.check_hostname)
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        self.assertEqual(result['tls_version'], 'TLSv1.2')

    async def test_tls12_option_applies_to_every_comparison_socket(self):
        calls = []
        async def probe(*args, **kwargs):
            calls.append(kwargs)
            return dict(case=args[0], matching_events=1, status='observation_finished')
        with patch.object(diagnostic, 'probe', side_effect=probe), patch.object(diagnostic, 'emit'):
            await diagnostic.run(['XUSDT'], 20, tls12=True)
        self.assertEqual(len(calls), 4)
        self.assertTrue(all(call['tls12'] for call in calls))

    async def test_real_library_automatically_pongs_and_records_control_frames(self):
        from websockets.asyncio.server import serve
        confirmed = []
        async def handler(socket):
            pong = await socket.ping(b'probe-server-ping')
            await asyncio.wait_for(pong, 1)
            confirmed.append(True)
            await socket.close()
        async with serve(handler, '127.0.0.1', 0, ping_interval=None) as server:
            port = server.sockets[0].getsockname()[1]
            with patch.object(diagnostic, 'emit'):
                result = await diagnostic.probe('local', ['XUSDT'], f'ws://127.0.0.1:{port}/',
                                                'bookTicker', 2)
        self.assertEqual(confirmed, [True])
        self.assertEqual(result['ws_control_counts']['in_PING'], 1)
        self.assertEqual(result['ws_control_counts']['out_PONG'], 1)
        events = result['ws_control_events']
        ping = next(e for e in events if e['frame'] == 'PING')
        pong = next(e for e in events if e['frame'] == 'PONG')
        self.assertEqual(ping['payload_hex'], pong['payload_hex'])
        self.assertLessEqual(ping['elapsed_ms'], pong['elapsed_ms'])


class ExceptionChainTests(unittest.TestCase):
    def test_control_trace_works_through_logger_adapter_and_ignores_data_frames(self):
        trace = diagnostic.ControlTrace('unit')
        adapter = logging.LoggerAdapter(trace, {})
        with patch.object(Frame, '__str__', side_effect=AssertionError('frame formatting is forbidden')):
            for _ in range(1000):
                adapter.debug('< %s', Frame(Opcode.TEXT, b'market-data'))
            adapter.debug('< %s', Frame(Opcode.PING, b'payload'))
            adapter.debug('> %s', Frame(Opcode.PONG, b'payload'))
            adapter.debug('< EOF')
        self.assertEqual(dict(trace.counts), {'in_PING': 1, 'out_PONG': 1})
        self.assertEqual(len(trace.events), 3)

    def test_control_trace_bounds_retention_but_keeps_total_counts(self):
        trace = diagnostic.ControlTrace('bounded')
        for _ in range(100):
            trace.debug('< %s', Frame(Opcode.PING, b'x'))
        self.assertEqual(trace.counts['in_PING'], 100)
        self.assertEqual(len(trace.events), 32)

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


class CLIComparisonTests(unittest.TestCase):
    def test_100_symbol_cli_keeps_original_group_and_applies_count_to_all_group_sockets(self):
        symbols = [f'S{index:03d}USDT' for index in range(450)]
        calls = []
        async def probe(*args, **kwargs):
            calls.append((args, kwargs))
            return dict(status='observation_finished', matching_events=1)
        with patch('sys.argv', ['probe', '--group', '2', '--limit-symbols', '100', '--seconds', '20']), \
                patch.object(diagnostic, 'load_symbols', return_value=symbols), \
                patch.object(diagnostic, 'probe', side_effect=probe), patch.object(diagnostic, 'emit') as emit:
            self.assertEqual(diagnostic.main(), 0)
        self.assertEqual([args[1] for args, _ in calls],
                         [symbols[200:300], ['BTCUSDT'], symbols[200:300], symbols[200:300]])
        self.assertEqual(emit.call_args_list[0].kwargs['symbols'], 100)
        self.assertEqual(emit.call_args_list[0].kwargs['tls_policy'], 'system_default')
        self.assertTrue(all('peer_ip' not in kw and 'tls12' not in kw for _, kw in calls))
