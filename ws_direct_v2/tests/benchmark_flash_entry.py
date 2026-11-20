"""Offline audit of registration barriers and rebound visibility.

Temporary ledgers only. Both transport entry points and worker warmup are
replaced before enabling guarded adapter code: this cannot send exchange I/O.
The recorded boundary is a FAKE transport, not exchange arrival or an ACK RTT.
Run: python -B -m tests.benchmark_flash_entry --output var/validation_v2/flash_probe.json
"""
import argparse
import json
import threading
import time
from pathlib import Path
from unittest.mock import patch

from tests.test_book_ticker_v2 import DualAdmissionTests
from v9711_live import config as cfg
from v9711_live.execution.client import ExchangeClient


def setup(near):
    case = DualAdmissionTests('test_tick_only_qualifies')
    case.setUp()
    case.stack.enter_context(patch('time.time', lambda: case.now))
    try:
        case.symbols = [f'S{i:04d}USDT' for i in range(800)]
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
            price = '1.01' if index < near else '1.03'
            market.update(symbol, price, int(case.now*1000))
            market.decide(market.events.get_nowait(), lambda event: None)
            market.update_book(symbol, price, '2', int(case.now*1000), index+1)
            market.decide(market.events.get_nowait(), lambda event: None)
        case.targets = []
        case.templates = [dict(symbol=s, limit_price=1., notional=5., order_bar_open_ms=case.bar,
            reference_bar_open_ms=case.bar-cfg.BAR_MS, exit_due_epoch=case.now+900) for s in case.symbols]
        case.publish()
        return case
    except BaseException:
        case.doCleanups()
        raise


def install_transport(case, fsyncs):
    trade, market = case.trade, case.trade.market
    sends = []
    def response(symbol, cid, qty):
        value = dict(symbol=symbol, clientOrderId=cid, orderId='fake-'+cid,
                     status='NEW', executedQty='0', avgPrice='0', origQty=str(qty))
        case.orders[cid] = value
        return value
    def record(symbols, transport):
        # Capture already RECEIVED rebound separately from the owner's quote.
        symbol = symbols[0]
        sends.append(dict(at_ns=time.perf_counter_ns(), transport=transport,
            symbols=symbols, fsyncs_before_send=len(fsyncs),
            owner_quote=market.entry_quote(symbol)['selected'],
            raw_tick=float(market.latest_ingress[symbol].price),
            raw_bid=float(market.book.latest_ingress[symbol].price)))
    def single(method, *args, before_send=None, **kwargs):
        if method != 'place_order':
            raise AssertionError('Unexpected broker operation in offline audit')
        before_send()
        before_send.wire_sent()
        record([args[0]], 'FAKE_WS')
        value = response(args[0], kwargs['client_order_id'], args[2])
        before_send.reply_received()
        return value
    def batch(orders):
        record([row['symbol'] for row in orders], 'FAKE_REST_BATCH')
        return [response(row['symbol'], row['newClientOrderId'], row['quantity']) for row in orders]
    trade.client._broker_call = single
    trade.client._post_batch_orders = batch
    trade.client.warm_worker_transport = lambda: None
    trade.client.place_limit_for_bar = ExchangeClient.place_limit_for_bar.__get__(trade.client)
    trade.client.enabled, trade.client.dry_run = True, False
    trade.enable_nonblocking_io()
    trade._account_deadline = case.now+3600
    return sends


def wait_for_sends(case, sends, count):
    deadline = time.perf_counter()+3
    while sum(len(row['symbols']) for row in sends) < count and time.perf_counter() < deadline:
        # Deliberately don't drain: callbacks mustn't perturb registration timing.
        threading.Event().wait(.001)
    if sum(len(row['symbols']) for row in sends) != count:
        raise AssertionError(f'Expected {count} fake sends, got {sends}')


def probe(near, batch_enabled, rebound=False):
    case = setup(0 if rebound else near)
    rebound_thread = None
    try:
        trade, market = case.trade, case.trade.market
        # Initialize readers and signal authority before the timed admission.
        if rebound:
            trade.step(now=case.now)
        fsyncs = []
        import os
        original_fsync = os.fsync
        def counted_fsync(fd):
            started = time.perf_counter_ns()
            original_fsync(fd)
            fsyncs.append((time.perf_counter_ns()-started)/1e6)
        case.stack.enter_context(patch('v9711_live.contracts.os.fsync', side_effect=counted_fsync))
        case.stack.enter_context(patch.dict('os.environ', {'V9711_LIVE_ENTRY_BATCH': str(int(batch_enabled))}))
        sends = install_transport(case, fsyncs)
        registered, release = threading.Event(), threading.Event()
        rebound_ns = []
        if rebound:
            original_save = trade.ledger.save
            first = True
            def save():
                nonlocal first
                if first:
                    first = False
                    registered.set()
                    if not release.wait(2):
                        raise AssertionError('Rebound producer failed')
                original_save()
            trade.ledger.save = save
            def rebound_during_registration():
                if registered.wait(2):
                    # A deterministic 80ms owner stall models a slow durable
                    # write. Both newly received quotes are now OUTSIDE range.
                    threading.Event().wait(.08)
                    market.update(case.symbols[0], '1.03', int(case.now*1000))
                    market.update_book(case.symbols[0], '1.03', '3', int(case.now*1000), 9000)
                    rebound_ns.append(time.perf_counter_ns())
                release.set()
            rebound_thread = threading.Thread(target=rebound_during_registration, daemon=True)
            rebound_thread.start()
            market.update(case.symbols[0], '.99', int(case.now*1000))
            event = market.events.get_nowait()
        started = time.perf_counter_ns()
        if rebound:
            market.decide(event, trade.on_tick)
        else:
            trade.step(now=case.now)
        owner_ms = (time.perf_counter_ns()-started)/1e6
        wait_for_sends(case, sends, near)
        result = dict(near=near, batch_enabled=batch_enabled, rebound=rebound,
            injected_owner_stall_ms=80 if rebound else 0,
            owner_call_ms=round(owner_ms, 3), fsync_count_before_drain=len(fsyncs),
            fsync_total_ms=round(sum(fsyncs), 3),
            sends=[dict(row, at_ns=None, after_start_ms=round((row['at_ns']-started)/1e6, 3),
                        after_rebound_ms=round((row['at_ns']-rebound_ns[0])/1e6, 3) if rebound_ns else None)
                   for row in sends])
        if rebound:
            result['received_rebound_but_guard_used_old_quote'] = any(
                row['at_ns'] > rebound_ns[0] and row['raw_tick'] > 1.0185
                and row['raw_bid'] > 1.0185 and row['owner_quote'] <= 1.0185
                for row in sends) if rebound_ns else False
        # All exact-ACK callbacks still run on owner, before temporary cleanup.
        deadline = time.perf_counter()+3
        while trade._network_io._jobs and time.perf_counter() < deadline:
            trade._drain_network()
            threading.Event().wait(.001)
        if trade._network_io._jobs:
            raise AssertionError('Offline callbacks did not settle')
        return result
    finally:
        if rebound_thread is not None:
            rebound_thread.join(timeout=3)
        case.trade.shutdown_io()
        case.doCleanups()


def run():
    return dict(scope='offline fake transport, real temporary durable files; no exchange I/O',
        registration=[probe(near, batch) for near, batch in ((1, True), (8, True), (8, False))],
        rebound_visibility=probe(1, True, rebound=True))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    result = run()
    content = json.dumps(result, indent=2)
    print(content)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(content+'\n', encoding='utf-8')
