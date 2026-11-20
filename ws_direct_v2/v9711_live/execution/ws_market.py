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
    source: str = 'tick'
    valid: bool = True
    quantity: str | None = None


class DirectMarket:
    def __init__(self, symbols, output: Path, *, group_size=200, queue_size=None, source='tick'):
        self.source = source
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

    def update(self, symbol, price, exchange_time_ms, *, valid=True, quantity=None):
        received_ns = self._last_wire_ns or time.monotonic_ns()
        self._last_wire_ns = 0
        now = time.time()
        try:
            value, stamp = float(price), int(exchange_time_ms)
        except (ValueError, TypeError, OverflowError):
            return False
        if symbol not in self.group_for or not math.isfinite(value) or (valid and value <= 0) or not -5 <= now-stamp/1000 <= cfg.PRICE_EXCHANGE_MAX_AGE_SECONDS:
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
            tick = Tick(symbol, str(price), stamp, received_ns, group, generation, self.source, valid, quantity)
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
            if tick.valid:
                self.processed[tick.symbol] = tick
            else:
                self.processed.pop(tick.symbol, None)
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
            if tick.valid:
                self.committed[tick.symbol] = tick
            else:
                self.committed.pop(tick.symbol, None)
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

    def entry_prices(self, symbols=None):
        return self.prices(symbols)

    def entry_quote(self, symbol):
        price = self.entry_prices([symbol]).get(symbol)
        return dict(tick=price, bid0=None, selected=price, source='tick' if price is not None else None)


class DualMarket(DirectMarket):
    """Independent aggTrade/bookTicker feeds; only aggTrade values mark positions."""
    def __init__(self, symbols, output, **kwargs):
        super().__init__(symbols, output, **kwargs)
        self.book = DirectMarket(symbols, output, source='bid0', **kwargs)
        self.book.events = self.events  # one owner, distinct per-feed generations
        self.book.update_book = self.update_book
        self._book_ids = {}
        self.bid_quantities = {}

    def update_book(self, symbol, bid, quantity, stamp, update_id):
        try:
            bid, quantity, update_id = float(bid), float(quantity), int(update_id)
        except (ValueError, TypeError, OverflowError):
            return False
        if symbol not in self.group_for or update_id <= 0 or not all(math.isfinite(v) for v in (bid, quantity)):
            return False
        group = self.group_for[symbol]
        key = (self.book.generation[group], update_id)
        old = self._book_ids.get(symbol)
        if old and key[0] == old[0] and key[1] <= old[1]:
            return False
        valid = bid > 0 and quantity > 0
        accepted = self.book.update(symbol, str(bid if valid else 0), stamp, valid=valid, quantity=str(quantity))
        if accepted:
            self._book_ids[symbol] = key
        return accepted

    def _quote(self, feed, symbol, tick=None):
        with feed._lock:
            tick = feed.processed.get(symbol) if tick is None else tick
            if (self._closing.is_set() or feed.overloaded or -1 in self.errors or not tick or not tick.valid
                    or tick.group in feed.errors or tick.group not in feed.receipts
                    or tick.generation != feed.generation[tick.group]
                    or time.monotonic()-feed.receipts[tick.group][0] > ticker_ws.MAX_SILENCE_SECONDS
                    or not -5 <= time.time()-tick.exchange_ms/1000 <= cfg.PRICE_EXCHANGE_MAX_AGE_SECONDS
                    or (time.monotonic_ns()-tick.received_ns)/1e9 > cfg.PRICE_EXCHANGE_MAX_AGE_SECONDS):
                return None
            return float(tick.price)

    def healthy(self):
        return any(self._quote(feed, symbol) is not None
                   for symbol in self.symbols for feed in (self, self.book))

    def order_allowed(self, symbol):
        return self._quote(self, symbol) is not None or self._quote(self.book, symbol) is not None

    def prices(self, symbols=None):
        # Deliberately keep tick-only valuation and the signal price mirror.
        return {symbol: price for symbol in (self.symbols if symbols is None else symbols)
                if (price := self._quote(self, symbol)) is not None}

    def entry_quote(self, symbol):
        tick, bid = self._quote(self, symbol), self._quote(self.book, symbol)
        candidates = [(price, source) for price, source in ((tick, 'tick'), (bid, 'bid0')) if price is not None]
        selected, source = min(candidates, default=(None, None))
        return dict(tick=tick, bid0=bid, selected=selected, source=source,
                    bid_quantity=self.bid_quantities.get(symbol))

    def entry_prices(self, symbols=None):
        return {symbol: price for symbol in (self.symbols if symbols is None else symbols)
                if (price := self.entry_quote(symbol)['selected']) is not None}

    def quote_state(self, symbol):
        if self.order_allowed(symbol):
            return 'quote_ready'
        if symbol not in self.processed and symbol not in self.book.processed:
            return 'quote_waiting_first_tick'
        return 'quote_stale_or_disconnected_both_sources'

    def snapshot(self):
        result = super().snapshot()
        with self._lock:
            result['prices'] = {s: dict(price=tick.price, exchange_time_ms=tick.exchange_ms)
                                for s, tick in self.committed.items() if self._quote(self, s, tick) is not None}
            tick_ready = (not self.overloaded and not self.errors and len(self.receipts) == len(self.groups)
                          and all(time.monotonic()-r[0] <= ticker_ws.MAX_SILENCE_SECONDS for r in self.receipts.values()))
            result['status'] = 'ok' if tick_ready and result['status'] == 'ok' else 'error'
        return result

    def decide(self, tick, callback):
        if tick is not None and tick.source == 'bid0':
            def decide_book(event):
                self.bid_quantities[event.symbol] = float(event.quantity or 0)
                return callback(event)
            return self.book.decide(tick, decide_book)
        return super().decide(tick, callback)

    def metrics(self):
        result = super().metrics()
        values = sorted(self.latencies_us + self.book.latencies_us)
        for suffix, fraction in (('p50', .5), ('p95', .95), ('p99', .99)):
            result['receive_to_decision_us_'+suffix] = values[int((len(values)-1)*fraction)] if values else None
        result['receive_to_decision_us_max'] = max(values, default=None)
        for key in ('received', 'decided', 'rejected_old', 'rejected_backlog', 'overflow'):
            result[key] += self.book.counts[key]
        result.update(bid0_symbols=sum(self._quote(self.book, s) is not None for s in self.symbols),
                      entry_symbols=len(self.entry_prices()),
                      book_received=self.book.counts['received'], book_decided=self.book.counts['decided'],
                      book_rejected_backlog=self.book.counts['rejected_backlog'],
                      book_overflow=self.book.counts['overflow'])
        with self._lock:
            result['tick_errors'] = dict(self.errors)
        with self.book._lock:
            result['depth_errors'] = dict(self.book.errors)
        result['proxy'] = 'disabled/direct'
        result['depth_endpoint'] = ticker_ws.BOOK_WS_BASE
        return result

    async def _receive(self):
        self._loop = asyncio.get_running_loop()
        self._async_stop = asyncio.Event()
        tasks = []
        for feed, stream, base in ((self, 'aggTrade', ticker_ws.WS_BASE),
                                  (self.book, 'bookTicker', ticker_ws.BOOK_WS_BASE)):
            for index, group in enumerate(self.groups, 1):
                tasks.append(asyncio.create_task(ticker_ws.consume_group(group, feed,
                    base=base, stream_kind=stream, ping_interval=30, ping_timeout=20, read_timeout=30,
                    reconnect_before=23*3600, stop=self._async_stop, group_index=index)))
        if self._closing.is_set():
            self._async_stop.set()
        try:
            await self._async_stop.wait()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
