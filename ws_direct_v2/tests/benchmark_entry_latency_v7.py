"""Real owner/temporary durable files; guarded FAKE socket, never Binance.

External perf_counter_ns stamps avoid coarse Windows monotonic_ns ticks.
Run from the version under test, with this script's absolute path if needed.
"""
import argparse
import asyncio
import json
import queue
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from tests.test_book_ticker_v2 import DualAdmissionTests
from v9711_live import config as cfg
from v9711_live.execution.client import ExchangeClient
from v9711_live.execution.stream import Health


def summary(values):
    values = sorted(values)
    return dict(samples=len(values), within_10ms=sum(v <= 10 for v in values),
        p50_ms=round(values[int((len(values)-1)*.5)], 3),
        p95_ms=round(values[int((len(values)-1)*.95)], 3),
        p99_ms=round(values[int((len(values)-1)*.99)], 3), max_ms=round(max(values), 3))


def run(samples=40, history=1000, blocked=False, signed_socket=False, burst=False):
    case = DualAdmissionTests('test_tick_only_qualifies')
    case.setUp()
    read_started, release_read = threading.Event(), threading.Event()
    producer = None
    try:
        case.symbols = [f'N{i}USDT' for i in range(800)]
        cfg.SYMBOLS_FILE.write_text('\n'.join(case.symbols))
        cfg.EXCHANGE_INFO_CACHE_FILE.write_text(json.dumps({'symbols': {
            s: dict(tick='.0001', step='1', min_qty='1', min_notional='5') for s in case.symbols}}))
        case.make_trade()
        trade, market = case.trade, case.trade.market
        for feed in (market, market.book):
            for group in feed.generation:
                feed.group_connected(group)
        while not market.events.empty():
            market.events.get_nowait()
        for symbol in case.symbols:
            market.update(symbol, '1.03', int(case.now*1000))
            market.decide(market.events.get_nowait(), lambda event: None)
        case.targets = []
        case.templates = [dict(symbol=s, limit_price=1., notional=5., order_bar_open_ms=case.bar,
            reference_bar_open_ms=case.bar-cfg.BAR_MS, exit_due_epoch=case.now+900) for s in case.symbols]
        case.publish()
        trade.step(now=case.now)
        for i in range(history):
            trade.ledger.intents['c:historical-'+str(i)] = dict(symbol='OLDUSDT', kind='entry',
                client_order_id='historical-'+str(i), order_id=str(i), limit_price=1., qty=5.,
                exit_due_epoch=case.now-1800, registered_at_epoch=case.now-3600)
        trade.ledger.flush()
        sends, received = [], {}
        def fake_socket(method, symbol, side, qty, *, client_order_id, before_send, **kwargs):
            before_send()
            sent = time.perf_counter_ns()
            before_send.wire_sent()
            sends.append(dict(symbol=symbol, ms=(sent-received[symbol])/1e6,
                              before_read_release=not release_read.is_set()))
            result = dict(symbol=symbol, clientOrderId=client_order_id, orderId=str(len(sends)),
                          status='NEW', executedQty='0', avgPrice='0', origQty=qty)
            case.orders[client_order_id] = result
            before_send.reply_received()
            return result
        trade.client._broker_call = fake_socket
        trade.client.place_limit_for_bar = ExchangeClient.place_limit_for_bar.__get__(trade.client)
        trade.client.warm_worker_transport = lambda: None
        if signed_socket:
            from websockets.protocol import State
            from v9711_live.exchange import trade_ws_lifecycle as lifecycle
            from v9711_live.exchange.binance_futures_trade_ws import BinanceWSBroker
            class Socket:
                def __init__(self):
                    self.state = State.OPEN
                    self.responses = asyncio.Queue()
                async def send(self, wire):
                    sent = time.perf_counter_ns()
                    payload = json.loads(wire)
                    params = payload['params']
                    assert payload['method'] == 'order.place' and params['apiKey'] == 'offline-key'
                    assert len(params['signature']) == 64
                    symbol, cid = params['symbol'], params['newClientOrderId']
                    sends.append(dict(symbol=symbol, ms=(sent-received[symbol])/1e6,
                                      before_read_release=not release_read.is_set()))
                    result = dict(symbol=symbol, clientOrderId=cid, orderId=str(len(sends)),
                                  status='NEW', executedQty='0', avgPrice='0', origQty=params['quantity'])
                    case.orders[cid] = result
                    self.responses.put_nowait(json.dumps(dict(id=payload['id'], status=200, result=result)))
                async def recv(self):
                    return await self.responses.get()
                async def close(self):
                    self.state = State.CLOSED
            async def connect(*args, **kwargs):
                return Socket()
            def require_broker(inner):
                if inner._broker is None:
                    inner._broker = BinanceWSBroker('offline-key', 'offline-secret', ws_url='wss://offline.invalid')
                return inner._broker
            case.stack.enter_context(patch.object(lifecycle.websockets, 'connect', side_effect=connect))
            case.stack.enter_context(patch.object(ExchangeClient, '_require_broker', require_broker))
            del trade.client.__dict__['_broker_call']
            del trade.client.__dict__['warm_worker_transport']
        # Avoid wrapping the default fixture's unused/missing database.
        trade.client.stream = SimpleNamespace()
        trade.client.enabled, trade.client.dry_run = True, False
        trade.enable_nonblocking_io()
        deadline = time.perf_counter()+5
        while trade._network_io.health()['transport_warmup']['pending'] and time.perf_counter() < deadline:
            threading.Event().wait(.001)
        if trade._network_io.health()['transport_warmup']['failed']:
            raise AssertionError('offline worker prewarm failed')
        trade._account_deadline = case.now+3600
        def pump_until(count):
            deadline = time.perf_counter()+3
            while len(sends) < count and time.perf_counter() < deadline:
                try:
                    event = market.events.get(timeout=.002)
                except queue.Empty:
                    continue
                if event is None:
                    trade._drain_network()
                else:
                    market.decide(event, trade.on_tick)
                market.events.task_done()
            if len(sends) != count:
                raise AssertionError(f'expected {count} fake sends, got {len(sends)}')
            while trade._network_io.health()['running'] or trade._network_io.health()['queued']:
                threading.Event().wait(.001)
            trade._drain_network()
        if blocked:
            # One resting exact order is queried while another symbol crosses.
            received['N0USDT'] = time.perf_counter_ns()
            market.update('N0USDT', '1.01', int(case.now*1000))
            pump_until(1)
            row = trade.tracked['N0USDT']
            class SlowAccount:
                max_age_seconds = 30
                def health(self):
                    return Health('default', 0, 1)
                def lookup_order(self, **kwargs):
                    read_started.set()
                    if not release_read.wait(2):
                        raise RuntimeError('producer failed to release account reader')
                    return case.orders[row['client_order_id']]
            source = SlowAccount()
            try:
                from v9711_live.execution.background_account import BackgroundAccountView
            except ImportError:
                trade.client.stream = source
            else:
                trade._account_reader = BackgroundAccountView(source)
                trade.client.stream = trade._account_reader
                deadline = time.perf_counter()+1
                while time.perf_counter() < deadline:
                    try:
                        trade.client.stream.health()
                        break
                    except Exception:
                        threading.Event().wait(.001)
            trade.client.query_exact_order = ExchangeClient.query_exact_order.__get__(trade.client)
            def produce():
                if not read_started.wait(2):
                    return
                received['N1USDT'] = time.perf_counter_ns()
                market.update('N1USDT', '.99', int(case.now*1000))
                threading.Event().wait(.1)
                release_read.set()
            producer = threading.Thread(target=produce, daemon=True)
            producer.start()
            trade.step(now=case.now)
            pump_until(2)
            values = [s['ms'] for s in sends if s['symbol'] == 'N1USDT']
        elif burst:
            for index in range(samples):
                symbol = case.symbols[index]
                received[symbol] = time.perf_counter_ns()
                market.update(symbol, '.99', int(case.now*1000))
            pump_until(samples)
            values = [s['ms'] for s in sends]
        else:
            for index in range(samples):
                symbol = case.symbols[index]
                received[symbol] = time.perf_counter_ns()
                market.update(symbol, '.99', int(case.now*1000))
                pump_until(index+1)
                row = trade.tracked[symbol]
                trade._retire_order(symbol, row, 'offline_benchmark_terminal')
                market.update(symbol, '1.03', int(case.now*1000))
                while not market.events.empty():
                    event = market.events.get_nowait()
                    if event is None:
                        trade._drain_network()
                    else:
                        market.decide(event, trade.on_tick)
                    market.events.task_done()
            values = [s['ms'] for s in sends]
        return dict(scope=('800 templates; real durable files/worker/broker loop/signing/JSON to FAKE socket.send; no TLS/network RTT'
                          if signed_socket else '800 templates; real temporary durable files; guarded FAKE socket, no signing/network/ACK RTT'),
            scenario='100ms_blocked_account' if blocked else 'burst_entries' if burst else 'sequential_entries', history_intents=history,
            latency=summary(values), sends=sends,
            account_worker=trade._account_reader is not None if hasattr(trade, '_account_reader') else False)
    finally:
        release_read.set()
        if producer:
            producer.join(3)
        case.trade.shutdown_io()
        case.doCleanups()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--samples', type=int, default=40)
    parser.add_argument('--history', type=int, default=1000)
    parser.add_argument('--blocked-account', action='store_true')
    parser.add_argument('--signed-socket', action='store_true')
    parser.add_argument('--burst', action='store_true')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    if not 1 <= args.samples <= 800 or args.history < 0:
        parser.error('samples in 1..800 and nonnegative history required')
    if args.burst and (args.samples > 8 or args.blocked_account):
        parser.error('burst requires samples <= 8 and no blocked-account')
    result = run(args.samples, args.history, args.blocked_account, args.signed_socket, args.burst)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2)+'\n', encoding='utf-8')
    print(json.dumps(result['latency']))
