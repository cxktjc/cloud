"""Real owner/temporary durable files; guarded FAKE socket, never Binance.

External perf_counter_ns stamps avoid coarse Windows monotonic_ns ticks.
Run from the version under test, with this script's absolute path if needed.
"""
import argparse
import asyncio
import json
import os
import sys
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
    if not values:
        return dict(samples=0, within_10ms=0, p50_ms=None, p95_ms=None, p99_ms=None, max_ms=None)
    return dict(samples=len(values), within_10ms=sum(v <= 10 for v in values),
        p50_ms=round(values[int((len(values)-1)*.5)], 3),
        p95_ms=round(values[int((len(values)-1)*.95)], 3),
        p99_ms=round(values[int((len(values)-1)*.99)], 3), max_ms=round(max(values), 3))


def restrict_cpus(count):
    """Only this disposable benchmark process; never change system settings."""
    if hasattr(os, 'sched_getaffinity'):
        available = sorted(os.sched_getaffinity(0))
        chosen = available[:count]
        os.sched_setaffinity(0, chosen)
        return chosen
    if os.name == 'nt':
        import ctypes
        from ctypes import wintypes
        api = ctypes.WinDLL('kernel32', use_last_error=True)
        api.GetCurrentProcess.restype = wintypes.HANDLE
        api.GetProcessAffinityMask.argtypes = (wintypes.HANDLE, ctypes.POINTER(ctypes.c_size_t), ctypes.POINTER(ctypes.c_size_t))
        api.SetProcessAffinityMask.argtypes = (wintypes.HANDLE, ctypes.c_size_t)
        process = api.GetCurrentProcess()
        mask, system = ctypes.c_size_t(), ctypes.c_size_t()
        if not api.GetProcessAffinityMask(process, ctypes.byref(mask), ctypes.byref(system)):
            raise ctypes.WinError(ctypes.get_last_error())
        chosen = [i for i in range(mask.value.bit_length()) if mask.value & (1 << i)][:count]
        if not api.SetProcessAffinityMask(process, sum(1 << i for i in chosen)):
            raise ctypes.WinError(ctypes.get_last_error())
        return chosen
    raise RuntimeError('CPU affinity unsupported on this platform')


def run(samples=40, history=1000, blocked=False, signed_socket=False, burst=False,
        pressure_rate=0, interval=1., changing=False, disk_delay_ms=0):
    case = DualAdmissionTests('test_tick_only_qualifies')
    case.setUp()
    read_started, release_read = threading.Event(), threading.Event()
    producer = None
    pressure_stop = threading.Event()
    pressure_errors = []
    try:
        case.symbols = [f'N{i}USDT' for i in range(800)]
        # Avoid per-frame Mock call recording in the pressure producer.
        case.stack.enter_context(patch('time.time', lambda: case.now))
        if pressure_rate:
            # Python 3.10 Windows monotonic_ns can tick in ~16ms units. All
            # event and stage clocks use the same high-resolution source here.
            case.stack.enter_context(patch('time.monotonic_ns', time.perf_counter_ns))
        cfg.SYMBOLS_FILE.write_text('\n'.join(case.symbols))
        cfg.EXCHANGE_INFO_CACHE_FILE.write_text(json.dumps({'symbols': {
            s: dict(tick='.0001', step='1', min_qty='1', min_notional='5') for s in case.symbols}}))
        case.make_trade()
        trade, market = case.trade, case.trade.market
        if disk_delay_ms:
            from v9711_live.execution import client as client_module
            original_json = client_module.atomic_write_json
            def slow_json(*args, **kwargs):
                threading.Event().wait(disk_delay_ms/1000)
                return original_json(*args, **kwargs)
            case.stack.enter_context(patch.object(client_module, 'atomic_write_json', side_effect=slow_json))
            try:
                from v9711_live.execution import durable_writer as durable_module
            except ImportError:
                pass  # Same benchmark can run against the archived synchronous baseline.
            else:
                original_text = durable_module.atomic_write_text
                def slow_text(*args, **kwargs):
                    threading.Event().wait(disk_delay_ms/1000)
                    return original_text(*args, **kwargs)
                case.stack.enter_context(patch.object(durable_module, 'atomic_write_text', side_effect=slow_text))
        for feed in (market, market.book):
            for group in feed.generation:
                feed.group_connected(group)
        while not market.events.empty():
            market.events.get_nowait()
        for symbol in case.symbols:
            market.update(symbol, '1.03', int(case.now*1000))
            market.decide(market.events.get_nowait(), lambda event: None)
            if pressure_rate:
                market.update_book(symbol, '1.03', '2', int(case.now*1000), 1)
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
        if pressure_rate:
            # User reported ~54KB state and <1KB intents. Do not benchmark an
            # empty ledger then attribute live delays to huge history files.
            trade.ledger.state['_offline_padding'] = 'x'*54000
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
        maintenance = time.perf_counter()+.25
        cycles = 0
        def pump_until(count):
            nonlocal maintenance, cycles
            deadline = time.perf_counter()+3
            while len(sends) < count and time.perf_counter() < deadline:
                if pressure_rate and time.perf_counter() >= maintenance:
                    trade._service_pending_quotes()
                    trade.step(now=case.now)
                    cycles += 1
                    maintenance = time.perf_counter()+.25
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
        pressure = None
        if pressure_rate:
            # Open-loop background load AND independently scheduled entries.
            # Trigger symbols are reserved so background frames cannot erase a
            # crossing before its explicitly scheduled one-second rebound.
            initial = market.metrics()
            started = time.perf_counter()
            duration = 2. if burst else samples*interval+1.
            background = case.symbols[samples:]
            # Build server-side payloads before measuring. The client should
            # pay JSON decoding cost, not the exchange's per-frame encoding.
            wires = {(symbol, change): json.dumps(dict(s=symbol, p=str(1.03+.0001*change),
                E=int(case.now*1000))) for symbol in background for change in (0, 1)}
            opportunities = []
            started = time.perf_counter()
            def produce_pressure():
                index, triggered = 0, 0
                rebounded = set()
                try:
                    while not pressure_stop.is_set():
                        elapsed = time.perf_counter()-started
                        if elapsed >= duration:
                            break
                        due = min(samples, samples if burst and elapsed >= .5 else
                                  max(0, int((elapsed-.5)/interval)+1) if elapsed >= .5 else 0)
                        while triggered < due:
                            symbol = case.symbols[triggered]
                            received[symbol] = time.perf_counter_ns()
                            opportunities.append(dict(symbol=symbol, received_ns=received[symbol]))
                            market.update(symbol, '.99', int(case.now*1000))
                            triggered += 1
                        for item in opportunities:
                            symbol = item['symbol']
                            if symbol not in rebounded and time.perf_counter_ns()-item['received_ns'] >= 1_000_000_000:
                                market.update(symbol, '1.03', int(case.now*1000))
                                rebounded.add(symbol)
                        target = int(elapsed*pressure_rate)
                        while index < target:
                            symbol = background[index % len(background)]
                            # 5/32 tick + 27/32 stable bid frames gives 15.625%
                            # effective events after initial book population.
                            change = index//len(background) % 2 if changing else 0
                            message = json.loads(wires[symbol, change])  # Decode on the client under the GIL.
                            try:
                                if index % 32 < 5:
                                    market.update(message['s'], message['p'], message['E'])
                                else:
                                    market.update_book(message['s'], message['p'], '2', message['E'], index+2)
                            except BufferError:
                                pass  # Explicit production overflow counters remain visible.
                            index += 1
                        pressure_stop.wait(.001)
                except BaseException as exc:
                    pressure_errors.append(repr(exc))
                finally:
                    pressure_stop.set()
            producer = threading.Thread(target=produce_pressure, daemon=True)
            producer.start()
            while not pressure_stop.is_set() or not market.events.empty():
                if time.perf_counter() >= maintenance:
                    trade._service_pending_quotes()
                    trade.step(now=case.now)
                    cycles += 1
                    maintenance = time.perf_counter()+.25
                try:
                    event = market.events.get(timeout=.005)
                except queue.Empty:
                    continue
                if event is None:
                    if trade._drain_network():
                        maintenance = 0
                    trade._publish_entry_gate()
                else:
                    market.decide(event, trade.on_tick)
                market.events.task_done()
            producer.join(2)
            # No artificial owner retirement: the eight slots fill normally;
            # rebounds use the real paced cancel path and fake cancel ACKs.
            end = time.perf_counter()+1
            while time.perf_counter() < end and (trade._network_io.health()['running'] or trade._network_io.health()['queued']):
                trade._drain_network()
                threading.Event().wait(.001)
            trade._drain_network()
            final = market.metrics()
            keys = ('received', 'enqueued', 'decided', 'overflow', 'overload_discarded', 'rejected_backlog')
            pressure = dict(target_raw_rate=pressure_rate, target_effective_rate=(pressure_rate if changing else pressure_rate*5/32),
                seconds=duration, actual_seconds=round(time.perf_counter()-started, 3),
                achieved_raw_rate=round((final.get('received', 0)-initial.get('received', 0))/duration, 1),
                achieved_effective_rate=round((final.get('enqueued', 0)-initial.get('enqueued', 0))/duration, 1),
                counters={k: final.get(k, 0)-initial.get(k, 0) for k in keys},
                queue_peak=final.get('queue_peak'), final_queue=market.events.qsize(),
                opportunities=len(opportunities), sent_symbols=len({s['symbol'] for s in sends}),
                unsent_symbols=[item['symbol'] for item in opportunities if item['symbol'] not in {s['symbol'] for s in sends}],
                duplicate_symbols=len(sends)-len({s['symbol'] for s in sends}),
                io_at_end=trade._network_io.health(),
                maintenance_cycles=cycles, errors=pressure_errors,
                decoded_json=True, state_bytes=cfg.EXECUTION_LEDGER_FILE.stat().st_size,
                disk_delay_ms_per_changed_file=disk_delay_ms,
                gil_switch_ms=sys.getswitchinterval()*1000,
                durability_writer=trade.ledger._background.health() if getattr(trade.ledger, '_background', None) else None,
                stages={name: None for name in ('queue_wait', 'dequeued_to_decision', 'decision_to_registered',
                                               'registered_to_worker', 'worker_to_durable', 'durable_to_send', 'worker_to_send')})
            # Timing rows survive retirement, use their exact CIDs for all sends.
            pressure['stages'] = {name: summary([row['latencies_ms'][name]
                for row in (trade.session_entries.timings.order(cid) for cid in case.orders)
                if name in row.get('latencies_ms', {})]) for name in pressure['stages']}
            values = [s['ms'] for s in sends]
        elif blocked:
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
            account_worker=trade._account_reader is not None if hasattr(trade, '_account_reader') else False,
            pressure=pressure)
    finally:
        pressure_stop.set()
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
    parser.add_argument('--pressure-rate', type=int, default=0)
    parser.add_argument('--interval', type=float, default=1.)
    parser.add_argument('--changing', action='store_true')
    parser.add_argument('--cpus', type=int, help='Restrict this process to N available logical CPUs')
    parser.add_argument('--disk-delay-ms', type=float, default=0., help='Offline added latency per changed durable file')
    args = parser.parse_args()
    if not 1 <= args.samples <= 800 or args.history < 0:
        parser.error('samples in 1..800 and nonnegative history required')
    if args.burst and (args.samples > 8 or args.blocked_account):
        parser.error('burst requires samples <= 8 and no blocked-account')
    if args.pressure_rate and (args.pressure_rate < 1 or args.samples > 8 or args.blocked_account or args.interval < .2):
        parser.error('pressure requires positive rate, samples <= 8, interval >= .2 and no blocked account')
    if args.cpus is not None and not 1 <= args.cpus <= 19:
        parser.error('cpus must be in 1..19')
    if not 0 <= args.disk_delay_ms <= 1000:
        parser.error('disk-delay-ms must be in 0..1000')
    affinity = restrict_cpus(args.cpus) if args.cpus else None
    result = run(args.samples, args.history, args.blocked_account, args.signed_socket, args.burst,
                 args.pressure_rate, args.interval, args.changing, args.disk_delay_ms)
    result['cpu_affinity'] = affinity
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2)+'\n', encoding='utf-8')
    print(json.dumps(result['latency']))
    if result['pressure']:
        evidence = result['pressure']
        raise SystemExit(int(bool(evidence['errors'] or evidence['unsent_symbols'] or evidence['duplicate_symbols']
            or evidence['counters']['overflow'] or evidence['counters']['overload_discarded']
            or evidence['counters']['rejected_backlog']
            or evidence['achieved_raw_rate'] < .9*args.pressure_rate)))
