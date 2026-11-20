"""Offline load harness: real owner, temporary ledger/mirror, no exchange I/O.

Run from ws_direct_v2: python -B -m tests.benchmark_ws_pressure --rate 5000 --seconds 3
This exercises in-memory ingress; it does not measure remote WS/TLS bandwidth.
"""
import argparse
import json
import queue
from contextlib import closing
import threading
import time
from pathlib import Path
from unittest.mock import patch

from tests import test_book_ticker_v2 as fixtures
from v9711_live import config as cfg


def run(rate=5000, seconds=3, symbols=800, pattern='mixed', nonblocking=False, near_symbols=8, near_gap=.01,
        scenario='steady', hot_symbols=None, account_history=0):
    case = fixtures.DualAdmissionTests('test_tick_only_qualifies')
    case.setUp()
    # Keep the fixture's stable bar clock without Mock call recording on each
    # quote. That test-only overhead can dominate busy-market measurements.
    case.stack.enter_context(patch('time.time', lambda: case.now))
    stop, errors = threading.Event(), []
    threads = []
    simulated_batch_ids = []
    try:
        case.symbols = [f'S{i:04d}USDT' for i in range(symbols)]
        cfg.SYMBOLS_FILE.write_text('\n'.join(case.symbols))
        cfg.EXCHANGE_INFO_CACHE_FILE.write_text(json.dumps({'symbols': {
            s: dict(tick='.0001', step='1', min_qty='1', min_notional='5') for s in case.symbols}}))
        case.make_trade()
        if nonblocking:
            original_batch = case.trade.client.place_limit_batch
            def batch(requests):
                replies = original_batch(requests)
                simulated_batch_ids.extend(cid for cid, (value, error) in replies.items()
                    if error is None and isinstance(value, dict) and value.get('_simulated'))
                return replies
            case.trade.client.place_limit_batch = batch
            case.trade.enable_nonblocking_io()  # Disabled exchange: simulation only.
        market = case.trade.market
        for feed in (market, market.book):
            for group in feed.generation:
                feed.group_connected(group)
        while not market.events.empty():
            market.events.get_nowait()
        bases = {s: 1+near_gap if i < near_symbols else 1.03 for i, s in enumerate(case.symbols)}
        if scenario == 'partial_book':
            bases = {s: 1.01 if i < 3 else 1.02 for i, s in enumerate(case.symbols)}
        if scenario == 'full_positions':
            case.held(3)
        for index, symbol in enumerate(case.symbols):
            price = str(bases[symbol])
            market.update(symbol, price, int(case.now*1000))
            market.decide(market.events.get_nowait(), lambda event: None)
            market.update_book(symbol, price, '2', int(case.now*1000), index+1)
            market.decide(market.events.get_nowait(), lambda event: None)
        case.targets = []
        case.templates = [dict(symbol=s, limit_price=1., notional=5., order_bar_open_ms=case.bar,
            reference_bar_open_ms=case.bar-cfg.BAR_MS, exit_due_epoch=case.now+900) for s in case.symbols]
        case.publish()
        case.trade.step(now=case.now)
        if scenario == 'owned_spread':
            symbol = sorted(case.trade.tracked)[0]
            bases[symbol] = .99
            market.update(symbol, '.99', int(case.now*1000))
            market.decide(market.events.get_nowait(), case.trade.on_tick)
            market.update_book(symbol, '.99', '2', int(case.now*1000), symbols+1)
            market.decide(market.events.get_nowait(), case.trade.on_tick)
        # Initial ACK wakes can precede the setup quotes. Consume every setup
        # quote before taking the counters, rather than count it in the run.
        while not market.events.empty():
            event = market.events.get_nowait()
            if event is None:
                case.trade._drain_network()
            else:
                market.decide(event, case.trade.on_tick)
            market.events.task_done()
        if account_history:
            from collectors.binance_account_stream import AccountStore
            from v9711_live.execution.client import ExchangeClient
            store = case.stack.enter_context(closing(AccountStore(cfg.ACCOUNT_STREAM_DIR)))
            store.status(mode='live', ready=True, connected=True)
            stamp = int(case.now*1000)
            orders, fills = [], []
            for index in range(account_history):
                payload = json.dumps(dict(symbol=f'FOREIGN{index}USDT', order_id=index+1,
                    client_order_id=f'foreign-{index}', status='FILLED', order_side='BUY',
                    executed_quantity=1, original_quantity=1, average_price=1,
                    last_price=1, last_quantity=1, trade_id=1, updated_at_ms=stamp))
                orders.append((f'FOREIGN{index}USDT:{index+1}', stamp, payload))
                fills.append((f'FOREIGN{index}USDT:{index+1}:1', stamp, payload))
            for symbol, row in case.trade.tracked.items():
                payload = dict(symbol=symbol, order_id=row['order_id'], client_order_id=row['client_order_id'],
                    status='NEW', order_side='BUY', original_quantity=row['qty'], executed_quantity=0,
                    average_price=0, updated_at_ms=stamp, price=row['limit_price'])
                orders.append((symbol+':'+str(row['order_id']), stamp, json.dumps(payload)))
            store.db.executemany('INSERT OR REPLACE INTO orders VALUES (?,?,?)', orders)
            store.db.executemany('INSERT OR REPLACE INTO fills VALUES (?,?,?)', fills)
            store.db.commit()
            store.export()
            case.trade.client.query_exact_order = ExchangeClient.query_exact_order.__get__(case.trade.client)
        unavailable = set()
        if scenario == 'quote_outage':
            # A resting order loses both quote sources while other groups
            # continue sending; no new event may silently recover this group.
            unavailable = set(market.groups[0])
            for feed in (market, market.book):
                feed.group_failed(1, 'offline benchmark quote outage')
        producer_symbols = [symbol for symbol in case.symbols if symbol not in unavailable]
        if hot_symbols:
            producer_symbols = producer_symbols[:hot_symbols]
        if not producer_symbols:
            raise ValueError('quote_outage requires symbols in another connection group')
        initial = dict(market.counts)
        initial_book = dict(market.book.counts)
        frames = int(rate*seconds)
        started = time.perf_counter()
        def produce():
            try:
                for index in range(frames):
                    if index % 32 == 0:
                        stop.wait(max(0, started+index/rate-time.perf_counter()))
                    symbol = producer_symbols[index % len(producer_symbols)]
                    if pattern in ('changing', 'changing_effective'):
                        # All sources change price on every revisit, including
                        # incumbent orders; no quantity-only dedupe benefit.
                        base = bases[symbol]
                        revisit = index//len(producer_symbols) % 2
                        price = base+(revisit*.0002-.0001 if pattern == 'changing_effective' else revisit*.0001)
                    else:
                        price = bases[symbol]
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
            if event is None:
                # Match the production owner: a completed network operation
                # can request immediate maintenance ahead of queued quotes.
                if case.trade._drain_network():
                    maintenance = 0
                case.trade._publish_entry_gate()
            elif market.decide(event, case.trade.on_tick):
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
        return dict(symbols=symbols, hot_symbols=hot_symbols, account_history=account_history,
            owner_work=case.trade._owner_work.snapshot() if hasattr(case.trade, '_owner_work') else None,
            near_symbols=near_symbols, near_gap=near_gap, scenario=scenario,
            positions=len(case.trade.positions), pattern=pattern, nonblocking_io=nonblocking,
            target_frames=frames, target_rate=rate,
            elapsed_seconds=round(time.perf_counter()-started, 3), counters=counters,
            queue_peak=result.get('queue_peak'), p95_receive_to_decision_ms=percentile(.95),
            p99_receive_to_decision_ms=percentile(.99), latency_scope='all decided events in this run; includes mirror commit',
            maintenance_p95_ms=round(maintenance_ms[int((len(maintenance_ms)-1)*.95)], 3) if maintenance_ms else None,
            pending=len(case.trade.tracked), simulated_submissions=len(case.submits)+len(simulated_batch_ids),
            maintenance_cycles=cycles, skipped_far_events=getattr(case.trade, '_skipped_far_events', 0),
            skipped_noncompetitive_events=getattr(case.trade, '_skipped_noncompetitive_events', 0),
            errors=errors, scope='in-memory ingress with real owner/temporary durable files; mocked exchange')
    finally:
        stop.set()
        for thread in threads:
            thread.join()
        case.trade.shutdown_io()
        case.doCleanups()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--rate', type=int, default=5000)
    parser.add_argument('--seconds', type=float, default=3)
    parser.add_argument('--pattern', choices=('mixed', 'changing', 'changing_effective'), default='mixed')
    parser.add_argument('--nonblocking', action='store_true')
    parser.add_argument('--near-symbols', type=int, default=8,
                        help='Number of 800 symbols near the limit; 800 exercises broad selloffs')
    parser.add_argument('--near-gap', type=float, default=.01,
                        help='Near price gap; .02 exercises an empty book outside the admission tier')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--hot-symbols', type=int, help='Concentrate measured events on the first N symbols')
    parser.add_argument('--account-history', type=int, default=0,
                        help='Use real account SQLite queries with N unrelated historical orders/fills')
    parser.add_argument('--scenario', choices=('steady', 'full_positions', 'partial_book', 'owned_spread', 'quote_outage'),
                        default='steady')
    args = parser.parse_args()
    if (args.rate <= 0 or args.seconds <= 0 or not 0 <= args.near_symbols <= 800 or not 0 <= args.near_gap <= .021
            or args.account_history < 0 or (args.hot_symbols is not None and not 1 <= args.hot_symbols <= 800)):
        parser.error('positive rate/seconds, near-symbols in 0..800 and near-gap in 0..0.021 required')
    result = run(args.rate, args.seconds, pattern=args.pattern, nonblocking=args.nonblocking,
                 near_symbols=args.near_symbols, near_gap=args.near_gap, scenario=args.scenario,
                 hot_symbols=args.hot_symbols, account_history=args.account_history)
    text = json.dumps(result, indent=2)
    print(text)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text+'\n', encoding='utf-8')
    raise SystemExit(int(bool(result['errors'] or result['counters']['overflow'] or result['counters']['rejected_backlog'])))
