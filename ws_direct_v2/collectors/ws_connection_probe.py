"""Read-only socket comparison: no account connection, orders or state writes."""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import time
from datetime import datetime, timezone

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


async def probe(name, symbols, base, stream, seconds, *, subscribe=False):
    started = time.monotonic()
    connected = first = last = socket = None
    count = events = acknowledgements = 0
    peer, outcome, chain = None, 'observation_finished', []
    allowed = set(symbols)
    try:
        url = base if subscribe else stream_url(symbols, base, stream)
        async with websockets.connect(url, proxy=None, ping_interval=30, ping_timeout=20,
                open_timeout=10, close_timeout=3, max_queue=4096, compression=None) as socket:
            connected = time.monotonic()
            peer = socket.remote_address
            emit(case=name, phase='connected', peer=peer, symbols=len(symbols),
                 connect_ms=round((connected-started)*1000, 3))
            if subscribe:
                await socket.send(json.dumps(dict(method='SUBSCRIBE',
                    params=[f'{s.lower()}@{stream}' for s in symbols], id='readonly-probe')))
            deadline = connected + seconds
            while True:
                remaining = deadline-time.monotonic()
                if remaining <= 0:
                    break
                try:
                    if hasattr(asyncio, 'timeout'):
                        async with asyncio.timeout(remaining):
                            raw = await socket.recv()
                    else:
                        raw = await asyncio.wait_for(socket.recv(), remaining)
                except asyncio.TimeoutError:
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
        last_data_age_ms=round((ended-last)*1000, 3) if last is not None else None,
        transport_error=exception_chain(getattr(socket, 'recv_exc', None)),
        protocol_error=exception_chain(getattr(getattr(socket, 'protocol', None), 'parser_exc', None)))
    emit(**result)
    return result


async def run(symbols, seconds):
    lag = [0.]
    async def watch():
        while True:
            began = time.monotonic()
            await asyncio.sleep(.1)
            lag[0] = max(lag[0], time.monotonic()-began-.1)
    watcher = asyncio.create_task(watch())
    try:
        results = await asyncio.gather(
            probe('depth_group_url', symbols, BOOK_WS_BASE, 'bookTicker', seconds),
            probe('depth_single_url', ['BTCUSDT'], BOOK_WS_BASE, 'bookTicker', seconds),
            probe('tick_group_url', symbols, WS_BASE, 'aggTrade', seconds),
            probe('depth_group_subscribe', symbols, BOOK_WS_BASE, 'bookTicker', seconds, subscribe=True))
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
    parser.add_argument('--group', type=int, default=2, help='same 200-symbol grouping as trade')
    args = parser.parse_args()
    if not 1 <= args.seconds <= 60 or args.group < 1:
        parser.error('seconds must be 1..60; group must be positive')
    symbols = load_symbols(DEFAULT_SYMBOLS)[(args.group-1)*200:args.group*200]
    if not symbols:
        parser.error('group is outside configured symbols')
    emit(phase='start', group=args.group, symbols=len(symbols), seconds=args.seconds,
         websockets=websockets.__version__, proxy='disabled/direct')
    results = asyncio.run(run(symbols, args.seconds))
    return int(any(row['status'] == 'failed' or not row['matching_events'] for row in results))


if __name__ == '__main__':
    raise SystemExit(main())
