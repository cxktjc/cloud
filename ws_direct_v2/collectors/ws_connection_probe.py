"""Read-only socket comparison: no account connection, orders or state writes."""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import ipaddress
import json
import logging
import ssl
import time
from collections import Counter, deque
from datetime import datetime, timezone
from functools import partial
from urllib.parse import urlsplit

import websockets

from .ticker_ws import DEFAULT_SYMBOLS, BOOK_WS_BASE, WS_BASE, load_symbols, stream_url


def exception_chain(error):
    result, seen = [], set()
    while error is not None and id(error) not in seen and len(result) < 8:
        seen.add(id(error))
        result.append(f'{type(error).__name__}: {error}')
        error = error.__cause__ or (None if error.__suppress_context__ else error.__context__)
    return result


def emit(**row):
    print(json.dumps(dict(utc=datetime.now(timezone.utc).isoformat(), **row),
                     ensure_ascii=False), flush=True)


def transport_exception(socket):
    # 17.4 renamed this private diagnostic attribute. Missing recv_exc must
    # not be mistaken for evidence that the transport had no exception.
    return getattr(socket, 'connection_exc', None) or getattr(socket, 'recv_exc', None)


class ControlTrace(logging.Logger):
    """Observe library control frames without formatting/logging market frames."""
    def __init__(self, name):
        super().__init__('ws-probe.'+name, logging.DEBUG)
        self.propagate = False
        self.started = time.monotonic()
        self.counts = Counter()
        self.events = deque(maxlen=32)

    def debug(self, message, *args, **kwargs):
        if message in ('< %s', '> %s') and args:
            frame = args[0]
            opcode = getattr(frame, 'opcode', None)
            if opcode not in (8, 9, 10):
                return
            kind = {8: 'CLOSE', 9: 'PING', 10: 'PONG'}[opcode]
            direction = 'in' if message.startswith('<') else 'out'
            self.counts[direction+'_'+kind] += 1
            self.events.append(dict(elapsed_ms=round((time.monotonic()-self.started)*1000, 3),
                direction=direction, frame=kind, payload_hex=bytes(frame.data).hex()))
        elif message in ('< EOF', '> EOF'):
            self.events.append(dict(elapsed_ms=round((time.monotonic()-self.started)*1000, 3),
                direction='in' if message.startswith('<') else 'out', frame='EOF'))

    def log(self, level, message, *args, **kwargs):
        # ClientConnection uses LoggerAdapter, which calls logger.log().
        if level == logging.DEBUG:
            self.debug(message, *args, **kwargs)
        else:
            super().log(level, message, *args, **kwargs)


async def probe(name, symbols, base, stream, seconds, *, subscribe=False,
                ping_interval=30, peer_ip=None, tls12=False):
    started = time.monotonic()
    connected = first = last = socket = None
    count = events = acknowledgements = 0
    peer, outcome, chain = None, 'observation_finished', []
    allowed = set(symbols)
    trace = ControlTrace(name)
    tls_version = None
    observation_end_requested = False
    connection_options = dict(logger=trace)
    if tls12:
        context = ssl.create_default_context()
        context.minimum_version = context.maximum_version = ssl.TLSVersion.TLSv1_2
        connection_options['ssl'] = context
    if peer_ip:
        connection_options.update(host=str(ipaddress.ip_address(peer_ip)),
                                  server_hostname=urlsplit(base).hostname)
    try:
        url = base if subscribe else stream_url(symbols, base, stream)
        async with websockets.connect(url, proxy=None, ping_interval=ping_interval, ping_timeout=20,
                open_timeout=10, close_timeout=3, max_queue=4096, compression=None,
                **connection_options) as socket:
            connected = time.monotonic()
            peer = socket.remote_address
            transport = getattr(socket, 'transport', None)
            tls_socket = transport.get_extra_info('ssl_object') if transport is not None else None
            tls_version = tls_socket.version() if tls_socket is not None else None
            emit(case=name, phase='connected', peer=peer, symbols=len(symbols),
                 connect_ms=round((connected-started)*1000, 3), ping_interval=ping_interval,
                 tls_version=tls_version)
            if subscribe:
                await socket.send(json.dumps(dict(method='SUBSCRIBE',
                    params=[f'{s.lower()}@{stream}' for s in symbols], id='readonly-probe')))
            deadline = connected + seconds
            while True:
                remaining = deadline-time.monotonic()
                if remaining <= 0:
                    observation_end_requested = True
                    break
                try:
                    if hasattr(asyncio, 'timeout'):
                        async with asyncio.timeout(remaining):
                            raw = await socket.recv()
                    else:
                        raw = await asyncio.wait_for(socket.recv(), remaining)
                except asyncio.TimeoutError:
                    observation_end_requested = True
                    break
                count += 1
                if count % 64 == 0:
                    await asyncio.sleep(0)
                message = json.loads(raw)
                if not isinstance(message, dict):
                    continue
                if 'code' in message:
                    raise ValueError(f'subscription error: {message}')
                if message.get('id') == 'readonly-probe' and 'result' in message:
                    acknowledgements += 1
                data = message.get('data', message)
                if (not isinstance(data, dict) or data.get('e') != stream
                        or data.get('s') not in allowed or data.get('st', 1) != 1):
                    continue
                events += 1
                last = time.monotonic()
                if first is None:
                    first = last
                    emit(case=name, phase='first_data', symbol=data['s'],
                         first_data_ms=round((first-connected)*1000, 3))
    except Exception as exc:
        outcome, chain = 'failed', exception_chain(exc)
    ended = time.monotonic()
    result = dict(case=name, phase='summary', status=outcome, peer=peer,
        connected=connected is not None, messages=count, matching_events=events,
        subscription_acks=acknowledgements, exceptions=chain,
        lifetime_seconds=round(ended-connected, 3) if connected is not None else None,
        total_seconds=round(ended-started, 3),
        ping_interval=ping_interval, observation_end_requested=observation_end_requested,
        tls_version=tls_version,
        ws_control_counts=dict(trace.counts), ws_control_events=list(trace.events),
        control_note='out means library generated frame; does not prove peer receipt',
        last_data_age_ms=round((ended-last)*1000, 3) if last is not None else None,
        transport_error=exception_chain(transport_exception(socket)),
        protocol_error=exception_chain(getattr(getattr(socket, 'protocol', None), 'parser_exc', None)))
    emit(**result)
    return result


async def run(symbols, seconds, *, heartbeat_compare=False, peer_ip=None, tls12=False):
    lag = [0.]
    async def watch():
        while True:
            began = time.monotonic()
            await asyncio.sleep(.1)
            lag[0] = max(lag[0], time.monotonic()-began-.1)
    watcher = asyncio.create_task(watch())
    run_probe = partial(probe, tls12=True) if tls12 else probe
    try:
        if heartbeat_compare:
            results = await asyncio.gather(
                run_probe('depth_BTC_ping30', ['BTCUSDT'], BOOK_WS_BASE, 'bookTicker', seconds,
                      ping_interval=30, peer_ip=peer_ip),
                run_probe('depth_BTC_ping2', ['BTCUSDT'], BOOK_WS_BASE, 'bookTicker', seconds,
                      ping_interval=2, peer_ip=peer_ip))
        else:
            results = await asyncio.gather(
                run_probe('depth_group_url', symbols, BOOK_WS_BASE, 'bookTicker', seconds),
                run_probe('depth_single_url', ['BTCUSDT'], BOOK_WS_BASE, 'bookTicker', seconds),
                run_probe('tick_group_url', symbols, WS_BASE, 'aggTrade', seconds),
                run_probe('depth_group_subscribe', symbols, BOOK_WS_BASE, 'bookTicker', seconds, subscribe=True))
    finally:
        watcher.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await watcher
    emit(phase='comparison_finished', event_loop_lag_max_ms=round(lag[0]*1000, 3),
         note='independent read-only sockets; no trade owner, account stream or state-file writer')
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seconds', type=float, default=20.)
    parser.add_argument('--group', type=int, default=2, help='original 200-symbol grouping retained for diagnostic comparisons')
    parser.add_argument('--limit-symbols', type=int, default=200,
                        help='use first N symbols within original group (1..200); do not shift group boundaries')
    parser.add_argument('--heartbeat-compare', action='store_true',
                        help='compare single-BTC sockets with 30s and 2s ping, retaining automatic pong')
    parser.add_argument('--peer-ip', help='pin both heartbeat cases to one IP; preserve original TLS hostname')
    parser.add_argument('--tls12', action='store_true',
                        help='diagnostic TLS 1.2 only; certificate/hostname verification remains enabled')
    args = parser.parse_args()
    if not 1 <= args.seconds <= 60 or args.group < 1:
        parser.error('seconds must be 1..60; group must be positive')
    if not 1 <= args.limit_symbols <= 200:
        parser.error('--limit-symbols must be 1..200')
    if args.peer_ip:
        if not args.heartbeat_compare:
            parser.error('--peer-ip requires --heartbeat-compare')
        try:
            ipaddress.ip_address(args.peer_ip)
        except ValueError:
            parser.error('--peer-ip must be a literal IP address')
    symbols = load_symbols(DEFAULT_SYMBOLS)[(args.group-1)*200:args.group*200][:args.limit_symbols]
    if not symbols:
        parser.error('group is outside configured symbols')
    emit(phase='start', group=None if args.heartbeat_compare else args.group,
         symbols=1 if args.heartbeat_compare else len(symbols), seconds=args.seconds,
         mode='heartbeat_compare' if args.heartbeat_compare else 'subscription_compare', peer_ip=args.peer_ip,
         tls_policy='TLSv1.2' if args.tls12 else 'system_default',
         websockets=websockets.__version__, proxy='disabled/direct')
    results = asyncio.run(run(symbols, args.seconds, heartbeat_compare=args.heartbeat_compare,
                             peer_ip=args.peer_ip, tls12=args.tls12))
    return int(any(row['status'] == 'failed' or not row['matching_events'] for row in results))


if __name__ == '__main__':
    raise SystemExit(main())
