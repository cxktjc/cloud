"""Offline load harness: real owner, temporary ledger/mirror, no exchange I/O.

Run from ws_direct_v2: python -B -m tests.benchmark_ws_pressure --rate 5000 --seconds 3
This exercises in-memory ingress; it does not measure remote WS/TLS bandwidth.
"""
import argparse
import json
import queue
import threading
import time
from pathlib import Path

from tests import test_book_ticker_v2 as fixtures
from v9711_live import config as cfg


def run(rate=5000, seconds=3, symbols=800, pattern='mixed'):
    case = fixtures.DualAdmissionTests('test_tick_only_qualifies')
    case.setUp()
    stop, errors = threading.Event(), []
    threads = []
    try:
        case.symbols = [f'S{i:04d}USDT' for i in range(symbols)]
        cfg.SYMBOLS_FILE.write_text('\n'.join(case.symbols))
        cfg.EXCHANGE_INFO_CACHE_FILE.write_text(json.dumps({'symbols': {
            s: dict(tick='.0001', step='1', min_qty='1', min_notional='5') for s in case.symbols}}))
        case.make_trade()
        market = case.trade.market
        for feed in (market, market.book):
            for group in feed.generation:
                feed.group_connected(group)
        while not market.events.empty():
            market.events.get_nowait()
        for index, symbol in enumerate(case.symbols):
            price = '1.01' if index < 8 else '1.03'
            market.update(symbol, price, int(case.now*1000))
            market.decide(market.events.get_nowait(), lambda event: None)
            market.update_book(symbol, price, '2', int(case.now*1000), index+1)
            market.decide(market.events.get_nowait(), lambda event: None)
        case.targets = []
        case.templates = [dict(symbol=s, limit_price=1., notional=5., order_bar_open_ms=case.bar,
            reference_bar_open_ms=case.bar-cfg.BAR_MS, exit_due_epoch=case.now+900) for s in case.symbols]
        case.publish()
        case.trade.step(now=case.now)
        initial = dict(market.counts)
        initial_book = dict(market.book.counts)
        frames = int(rate*seconds)
        started = time.perf_counter()
        def produce():
            try:
                for index in range(frames):
                    if index % 32 == 0:
                        stop.wait(max(0, started+index/rate-time.perf_counter()))
                    symbol = case.symbols[index % symbols]
                    if pattern == 'changing':
                        # All sources change price on every revisit, including
                        # incumbent orders; no quantity-only dedupe benefit.
                        base = 1.01 if index % symbols < 8 else 1.03
                        price = base+(index//symbols % 2)*.0001
                    else:
                        price = 1.01 if index % symbols < 8 else 1.03+(index % 2)*.001
                    try:
                        if index % 5 == 0:
                            market.update(symbol, str(price), int(case.now*1000))
                        else:
                            market.update_book(symbol, str(price), str(index+2), int(case.now*1000), index+symbols+1)
                    except BufferError:
                        pass  # explicit counter + revoked generation, just like consumer
            except BaseException as exc:
                errors.append(str(exc))
            finally:
                stop.set()
        def mirror():
            try:
                while not stop.wait(.1):
                    market.write_mirror()
            except BaseException as exc:
                errors.append(str(exc))
        for work in (produce, mirror):
            thread = threading.Thread(target=work, daemon=True)
            threads.append(thread)
            thread.start()
        maintenance, cycles = time.perf_counter()+.25, 0
        maintenance_ms, decision_ms = [], []
        while not stop.is_set() or not market.events.empty():
            if time.perf_counter() >= maintenance:
                cycle_started = time.perf_counter()
                case.trade.step(now=case.now)
                maintenance_ms.append((time.perf_counter()-cycle_started)*1000)
                maintenance = time.perf_counter()+.25
                cycles += 1
            try:
                event = market.events.get(timeout=.01)
            except queue.Empty:
                continue
            if market.decide(event, case.trade.on_tick):
                decision_ms.append((time.monotonic_ns()-event.received_ns)/1e6)
            market.events.task_done()
        for thread in threads:
            thread.join()
        result = market.metrics()
        counters = {key: result.get(key, 0)-initial.get(key, 0)-initial_book.get(key, 0)
                    for key in ('received', 'decided', 'enqueued', 'unchanged', 'overflow', 'rejected_backlog', 'overload_discarded')}
        maintenance_ms.sort()
        decision_ms.sort()
        percentile = lambda p: round(decision_ms[int((len(decision_ms)-1)*p)], 3) if decision_ms else None
        return dict(symbols=symbols, pattern=pattern, target_frames=frames, target_rate=rate,
            elapsed_seconds=round(time.perf_counter()-started, 3), counters=counters,
            queue_peak=result.get('queue_peak'), p95_receive_to_decision_ms=percentile(.95),
            p99_receive_to_decision_ms=percentile(.99), latency_scope='all decided events in this run; includes mirror commit',
            maintenance_p95_ms=round(maintenance_ms[int((len(maintenance_ms)-1)*.95)], 3) if maintenance_ms else None,
            pending=len(case.trade.tracked), simulated_submissions=len(case.submits),
            maintenance_cycles=cycles, skipped_far_events=getattr(case.trade, '_skipped_far_events', 0),
            errors=errors, scope='in-memory ingress with real owner/temporary durable files; mocked exchange')
    finally:
        stop.set()
        for thread in threads:
            thread.join()
        case.doCleanups()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--rate', type=int, default=5000)
    parser.add_argument('--seconds', type=float, default=3)
    parser.add_argument('--pattern', choices=('mixed', 'changing'), default='mixed')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    if args.rate <= 0 or args.seconds <= 0:
        parser.error('rate and seconds must be positive')
    result = run(args.rate, args.seconds, pattern=args.pattern)
    text = json.dumps(result, indent=2)
    print(text)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text+'\n', encoding='utf-8')
    raise SystemExit(int(bool(result['errors'] or result['counters']['overflow'] or result['counters']['rejected_backlog'])))
