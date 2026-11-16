"""WS ingress -> owner decision -> committed mirror, never file -> decision.

Receiver and snapshot writer are independent of the execution state owner.
Every accepted tick is queued, not last-price-coalesced before admission.
Overflow, stale backlog, reconnect generations and silence fail closed.
"""
from __future__ import annotations
import asyncio
import logging
import math
import queue
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from .. import config as cfg
from ..contracts import atomic_write_json, utc_now_iso
from collectors import ticker_ws

LOG = logging.getLogger('v9711.ws_direct.market')


@dataclass(frozen=True)
class Tick:
    symbol: str
    price: str
    exchange_ms: int
    received_ns: int
    group: int
    generation: int


class DirectMarket:
    def __init__(self, symbols, output: Path, *, group_size=200, queue_size=None):
        self.symbols = tuple(symbols)
        self.output = Path(output)
        self.groups = [list(self.symbols[i:i+group_size]) for i in range(0, len(self.symbols), group_size)]
        if not self.groups:
            raise ValueError('WS symbol list is empty')
        self.group_for = {symbol: index for index, group in enumerate(self.groups, 1) for symbol in group}
        self.events = queue.Queue(maxsize=cfg.WS_QUEUE_SIZE if queue_size is None else queue_size)
        self._lock = threading.RLock()
        self.generation = {index:0 for index in range(1, len(self.groups)+1)}
        self.receipts = {}
        self.errors = {}
        self.latest_ingress = {}
        self.processed = {}  # owner thread only
        self.committed = {}
        self.committed_receipts = {}
        self.sequence = 0
        self.overloaded = False
        self.counts = dict(received=0, decided=0, rejected_old=0, rejected_backlog=0, overflow=0)
        self.latencies_us = []
        self.admission_latencies_us = []
        self._closing = threading.Event()
        self._threads = []
        self._loop = self._async_stop = None
        self._last_wire_ns = 0

    def _wake(self):
        try:
            self.events.put_nowait(None)
        except queue.Full:
            pass

    def group_connected(self, index):
        with self._lock:
            self.generation[index] += 1
            self.receipts.pop(index, None)
            self.errors[index] = 'waiting_for_first_data'
        self._wake()

    def group_failed(self, index, message):
        with self._lock:
            self.receipts.pop(index, None)
            self.errors[index] = str(message)
        self._wake()

    def group_received(self, index):
        pass  # update() validates/marks receipt before making its event visible

    def update(self, symbol, price, exchange_time_ms):
        received_ns = self._last_wire_ns or time.monotonic_ns()
        self._last_wire_ns = 0
        now = time.time()
        try:
            value, stamp = float(price), int(exchange_time_ms)
        except (ValueError, TypeError, OverflowError):
            return False
        if symbol not in self.group_for or not math.isfinite(value) or value <= 0 or not -5 <= now-stamp/1000 <= cfg.PRICE_EXCHANGE_MAX_AGE_SECONDS:
            return False
        group = self.group_for[symbol]
        with self._lock:
            generation = self.generation[group]
            old = self.latest_ingress.get(symbol)
            if old and stamp < old.exchange_ms:
                self.counts['rejected_old'] += 1
                return False
            self.receipts[group] = (time.monotonic(), now)
            self.errors.pop(group, None)
            tick = Tick(symbol, str(price), stamp, received_ns, group, generation)
            self.latest_ingress[symbol] = tick
            self.counts['received'] += 1
        try:
            self.events.put_nowait(tick)
        except queue.Full:
            self.counts['overflow'] += 1
            self.group_failed(group, 'WS owner queue overflow; admissions disabled')
            raise BufferError('WS decision queue overflow; reconnect without silent coalescing')
        return True

    def healthy(self):
        with self._lock:
            return (not self.overloaded and not self.errors and len(self.receipts) == len(self.groups)
                    and all(time.monotonic()-row[0] <= ticker_ws.MAX_SILENCE_SECONDS for row in self.receipts.values()))

    def order_allowed(self, symbol):
        with self._lock:
            tick = self.processed.get(symbol)
            return bool(self.healthy() and tick and self.generation[tick.group] == tick.generation
                        and -5 <= time.time()-tick.exchange_ms/1000 <= cfg.PRICE_EXCHANGE_MAX_AGE_SECONDS)

    def quote_state(self, symbol):
        with self._lock:
            tick = self.processed.get(symbol)
            if tick is None:
                return 'quote_waiting_first_tick'
            if tick.generation != self.generation[tick.group]:
                return 'quote_waiting_reconnect_tick'
            if not self.healthy():
                return 'quote_stream_not_ready'
            return 'quote_stale'

    def prices(self, symbols=None):
        if not self.healthy():
            return {}
        now = time.time()
        with self._lock:
            items = self.processed.items() if symbols is None else ((s,self.processed.get(s)) for s in symbols)
            return {symbol:float(tick.price) for symbol,tick in items if tick is not None
                    if self.generation[tick.group] == tick.generation
                    and -5 <= now-tick.exchange_ms/1000 <= cfg.PRICE_EXCHANGE_MAX_AGE_SECONDS}

    def decide(self, tick, callback):
        if tick is None:
            return False
        with self._lock:
            if tick.generation != self.generation[tick.group]:
                self.counts['rejected_old'] += 1
                return False
        if (time.monotonic_ns()-tick.received_ns)/1e9 > cfg.WS_MAX_QUEUE_AGE_SECONDS:
            self.counts['rejected_backlog'] += 1
            self.overloaded = True
            return False
        self.overloaded = False
        with self._lock:
            self.processed[tick.symbol] = tick
        # callback includes durable intent registration and network dispatch.
        # Do not expose this tick to the file writer until it returns.
        admitted = bool(callback(tick))
        elapsed = (time.monotonic_ns()-tick.received_ns)/1000
        with self._lock:
            self.counts['decided'] += 1
            self.latencies_us.append(elapsed)
            if admitted:
                self.admission_latencies_us.append(elapsed)
                if len(self.admission_latencies_us) > 4096:
                    del self.admission_latencies_us[:2048]
            if len(self.latencies_us) > 4096:
                del self.latencies_us[:2048]
            self.committed[tick.symbol] = tick
            self.committed_receipts[tick.group] = (tick.generation, time.time()-(time.monotonic_ns()-tick.received_ns)/1e9)
        return True

    def snapshot(self):
        with self._lock:
            ready = self.healthy() and all(index in self.committed_receipts
                    and self.committed_receipts[index][0] == generation
                    for index,generation in self.generation.items())
            success = min((row[1] for row in self.committed_receipts.values()), default=0)
            self.sequence += 1
            return dict(schema_version=1, source='trade_internal_aggTrade_after_decision',
                        status='ok' if ready else 'error', sequence=self.sequence,
                        updated_at_utc=utc_now_iso(), last_success_epoch=success or None,
                        prices={symbol:dict(price=tick.price, exchange_time_ms=tick.exchange_ms)
                                for symbol,tick in self.committed.items()
                                if tick.generation == self.generation[tick.group]},
                        groups_total=len(self.groups), groups_ready=len(self.receipts),
                        error='; '.join(self.errors.values()), decision_before_mirror=True)

    def write_mirror(self):
        atomic_write_json(self.output, self.snapshot())

    def metrics(self):
        with self._lock:
            values = sorted(self.latencies_us)
            percentile = lambda p: values[min(len(values)-1, int((len(values)-1)*p))] if values else None
            return dict(self.counts, queue=self.events.qsize(), healthy=self.healthy(),
                        subscribed=len(self.symbols), received_symbols=len(self.processed),
                        fresh_symbols=len(self.prices()),
                        receive_to_decision_us_p50=percentile(.5),
                        receive_to_decision_us_p95=percentile(.95),
                        receive_to_decision_us_p99=percentile(.99),
                        receive_to_decision_us_max=max(values) if values else None,
                        admission_dispatch_us_samples=list(self.admission_latencies_us[-64:]),
                        note='includes decision+durable intent+dispatch, not exchange ACK; rolling samples')

    async def _receive(self):
        self._loop = asyncio.get_running_loop()
        self._async_stop = asyncio.Event()
        tasks = [asyncio.create_task(ticker_ws.consume_group(group, self,
                 base=ticker_ws.WS_BASE, ping_interval=30, ping_timeout=20, read_timeout=30,
                 reconnect_before=23*3600, stop=self._async_stop, group_index=index))
                 for index,group in enumerate(self.groups, 1)]
        if self._closing.is_set():
            self._async_stop.set()
        try:
            await self._async_stop.wait()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    def start(self):
        def receive():
            try:
                asyncio.run(self._receive())
            except Exception:
                LOG.exception('market receiver stopped; admissions disabled')
                with self._lock:
                    self.errors[-1] = 'market receiver stopped'
                self._wake()
        def writer():
            while not self._closing.is_set():
                try:
                    self.write_mirror()
                except Exception:
                    LOG.exception('price mirror write failed; internal decision path remains independent')
                self._closing.wait(max(.02, cfg.WS_MIRROR_SECONDS))
        for name, target in (('direct-market-ws', receive), ('direct-price-mirror', writer)):
            thread = threading.Thread(target=target, name=name, daemon=True)
            self._threads.append(thread)
            thread.start()

    def close(self):
        self._closing.set()
        if self._loop and self._async_stop and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._async_stop.set)
        for thread in self._threads:
            thread.join(timeout=20)
        with self._lock:
            self.errors[-1] = 'trade stopped'
        try:
            self.write_mirror()
        except Exception:
            LOG.exception('cannot publish stopped market state')
