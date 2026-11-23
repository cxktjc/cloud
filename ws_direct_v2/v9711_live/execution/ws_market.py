"""WS ingress -> owner decision -> committed mirror, never file -> decision.

Receiver and snapshot writer are independent of the execution state owner.
Every price/validity change is queued, not last-price-coalesced before admission.
Identical consecutive book prices only refresh receipt/quantity freshness.
Overflow, stale backlog, reconnect generations and silence fail closed.
"""
from __future__ import annotations
import asyncio
import logging
import math
import queue
from .quote_queue import QuoteQueue
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from .. import config as cfg
from ..contracts import atomic_write_json, utc_now_iso
from collectors import ticker_ws

LOG = logging.getLogger('v9711.ws_direct.market')
RECOVERED_ERROR_HIDE_SECONDS = 10 * 60


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
    received_epoch_ms: float = 0.


class DirectMarket:
    def __init__(self, symbols, output: Path, *, group_size=100, queue_size=None, source='tick', dedupe_unchanged=False):
        self.source = source
        self.dedupe_unchanged = dedupe_unchanged
        self.symbols = tuple(symbols)
        self.output = Path(output)
        self.groups = [list(self.symbols[i:i+group_size]) for i in range(0, len(self.symbols), group_size)]
        if not self.groups:
            raise ValueError('WS symbol list is empty')
        self.group_for = {symbol: index for index, group in enumerate(self.groups, 1) for symbol in group}
        self.events = QuoteQueue(maxsize=cfg.WS_QUEUE_SIZE if queue_size is None else queue_size)
        self._lock = threading.RLock()
        self.generation = {index:0 for index in range(1, len(self.groups)+1)}
        self.receipts = {}
        self.errors = {}
        self.error_history = []
        self._recovered_since = {}
        self._overflow_groups = set()
        self.latest_ingress = {}
        self.processed = {}  # owner thread only
        self.committed = {}
        self.committed_receipts = {}
        self.sequence = 0
        self.overloaded = False
        self.counts = dict(received=0, decided=0, rejected_old=0, rejected_backlog=0, overflow=0,
                           enqueued=0, unchanged=0, overload_discarded=0)
        self._rate_keys = ('received', 'enqueued', 'decided', 'unchanged', 'rejected_backlog', 'rejected_old', 'overload_discarded')
        self._rate_sample = (time.monotonic(), {key: 0 for key in self._rate_keys})
        self._rates = {key: 0. for key in self._rate_keys}
        self.queue_peak = 0
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
            self._overflow_groups.discard(index)
            self.receipts.pop(index, None)
            self._recovered_since.pop(index, None)
            self.errors[index] = 'waiting_for_first_data'
        self._wake()

    def group_failed(self, index, message):
        with self._lock:
            self.receipts.pop(index, None)
            self._recovered_since.pop(index, None)
            self.errors[index] = str(message)
            if message not in ('scheduled reconnect', 'collector stopped'):
                detail = str(message)[:700]
                previous = next((row for row in self.error_history
                                 if row['group'] == index and row['error'] == detail), None)
                if previous is not None:
                    self.error_history.remove(previous)
                else:
                    previous = dict(group=index, error=detail, count=0, first_epoch=time.time())
                previous.update(count=previous['count']+1, last_epoch=time.time())
                self.error_history.append(previous)
                del self.error_history[:-5]
        self._wake()

    def recent_errors(self):
        with self._lock:
            now = time.monotonic()
            visible, retained = [], []
            for row in self.error_history:
                group = row['group']
                recovered = (group in self.receipts and group not in self.errors
                             and now-self.receipts[group][0] <= ticker_ws.MAX_SILENCE_SECONDS)
                if not recovered:
                    self._recovered_since.pop(group, None)
                since = self._recovered_since.get(group)
                if recovered and since is not None and now-since >= RECOVERED_ERROR_HIDE_SECONDS:
                    continue  # Retire panel history only; the full log remains on disk.
                retained.append(row)
                visible.append(dict(row, source='depth' if self.source == 'bid0' else 'tick',
                                    recovered=recovered))
            self.error_history[:] = retained
            return visible

    def group_received(self, index):
        pass  # update() validates/marks receipt before making its event visible

    def update(self, symbol, price, exchange_time_ms, *, valid=True, quantity=None):
        received_ns = self._last_wire_ns or time.monotonic_ns()
        self._last_wire_ns = 0
        clock_ns = time.monotonic_ns()
        now = time.time()
        received_epoch_ms = now*1000-(clock_ns-received_ns)/1e6
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
            tick = Tick(symbol, str(price), stamp, received_ns, group, generation, self.source, valid, quantity,
                        received_epoch_ms)
            self.latest_ingress[symbol] = tick
            self.counts['received'] += 1
            if group in self._overflow_groups:
                # The explicit overflow has revoked ALL queued prices of this
                # generation. Keep the socket draining, but admit no old work.
                watermark = min(256, self.events.maxsize//4) if self.events.maxsize else 256
                if self.events.qsize() > watermark:
                    self.counts['overload_discarded'] += 1
                    return True
                self._overflow_groups.discard(group)
                old = None  # the recovery quote must reach the owner
            receipt_time = time.monotonic()
            previous_receipt = self.receipts.get(group)
            if (group in self.errors or previous_receipt is None
                    or receipt_time-previous_receipt[0] > ticker_ws.MAX_SILENCE_SECONDS):
                self._recovered_since[group] = receipt_time
            else:
                self._recovered_since.setdefault(group, receipt_time)
            self.receipts[group] = (receipt_time, now)
            self.errors.pop(group, None)
            if (self.dedupe_unchanged and not self.overloaded and old and old.generation == generation
                    and old.valid == valid and float(old.price) == value):
                # Only consecutive identical decision inputs are redundant.
                # A->B->A is always three events; zero quantity is a change.
                self.counts['unchanged'] += 1
                return True
        try:
            self.events.put_nowait(tick)
        except queue.Full:
            with self._lock:
                self.counts['overflow'] += 1
                self.counts['overload_discarded'] += 1
                self.generation[group] += 1
                self._overflow_groups.add(group)
                self.group_failed(group, 'WS owner queue overflow; old generation revoked; socket kept open')
            raise BufferError('WS decision queue overflow; old generation revoked; draining without reconnect')
        self.counts['enqueued'] += 1
        self.queue_peak = max(self.queue_peak, self.events.qsize())
        return True

    def quote_tick(self, symbol, tick):
        """Refresh unchanged book timestamps without publishing a new price."""
        latest = self.latest_ingress.get(symbol)
        if (self.dedupe_unchanged and latest and tick and latest.generation == tick.generation
                and latest.valid == tick.valid and float(latest.price) == float(tick.price)):
            return latest
        return tick

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
        # Owner-only boundary, immediately after the caller takes the event.
        # Keep lock validation/screening out of the queue-wait measurement.
        self._dequeued_context = (tick, time.monotonic_ns(), time.time()*1000)
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

    def metrics(self, *, fresh_prices=None):
        with self._lock:
            now = time.monotonic()
            previous, counts = self._rate_sample
            if now-previous >= 1:
                self._rates = {key: (self.counts[key]-counts[key])/(now-previous) for key in self._rate_keys}
                self._rate_sample = (now, {key: self.counts[key] for key in self._rate_keys})
            counts, rates = dict(self.counts), dict(self._rates)
            samples, admissions = list(self.latencies_us), list(self.admission_latencies_us[-64:])
            received_symbols, peak = len(self.processed), self.queue_peak
        # Diagnostics must not hold the ingress lock while sorting samples or
        # evaluating the quote map. Receivers keep draining during this work.
        values = sorted(samples)
        percentile = lambda p: values[min(len(values)-1, int((len(values)-1)*p))] if values else None
        return dict(counts, queue=self.events.qsize(), queue_peak=peak,
                        receive_per_second=rates['received'], decide_per_second=rates['decided'],
                        enqueue_per_second=rates['enqueued'], unchanged_per_second=rates['unchanged'],
                        expired_per_second=rates['rejected_backlog'], old_per_second=rates['rejected_old'],
                        discarded_per_second=rates['overload_discarded'], healthy=self.healthy(),
                        subscribed=len(self.symbols), received_symbols=received_symbols,
                        fresh_symbols=len(self.prices() if fresh_prices is None else fresh_prices),
                        receive_to_decision_us_p50=percentile(.5),
                        receive_to_decision_us_p95=percentile(.95),
                        receive_to_decision_us_p99=percentile(.99),
                        receive_to_decision_us_max=max(values) if values else None,
                        admission_dispatch_us_samples=admissions,
                        note='includes decision+durable intent+dispatch, not exchange ACK; rolling samples')

    async def _receive(self):
        await self._receive_feeds(((self, 'aggTrade', ticker_ws.WS_BASE),))

    async def _startup_pause(self):
        await asyncio.sleep(1.0)

    async def _launch_receivers(self, feeds, tasks):
        specs = [(feed, stream, base, index, group)
                 for feed, stream, base in feeds for index, group in enumerate(feed.groups, 1)]
        for offset, (feed, stream, base, index, group) in enumerate(specs):
            if self._async_stop.is_set():
                break
            startup_done = asyncio.Event()
            tasks.append(asyncio.create_task(ticker_ws.consume_group(group, feed,
                base=base, stream_kind=stream, ping_interval=30, ping_timeout=20, read_timeout=30,
                reconnect_before=23*3600, stop=self._async_stop, group_index=index,
                subscribe=True, startup_done=startup_done)))
            await startup_done.wait()
            if offset+1 < len(specs) and not self._async_stop.is_set():
                await self._startup_pause()

    async def _receive_feeds(self, feeds):
        self._loop = asyncio.get_running_loop()
        self._async_stop = asyncio.Event()
        tasks = []
        if self._closing.is_set():
            self._async_stop.set()
        startup = asyncio.create_task(self._launch_receivers(feeds, tasks))
        stopping = asyncio.create_task(self._async_stop.wait())
        try:
            completed, _ = await asyncio.wait((startup, stopping), return_when=asyncio.FIRST_COMPLETED)
            if startup in completed:
                await startup  # Propagate unexpected launch errors; fail closed.
            await stopping
        finally:
            startup.cancel()
            stopping.cancel()
            await asyncio.gather(startup, stopping, return_exceptions=True)
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
        self.book = DirectMarket(symbols, output, source='bid0', dedupe_unchanged=True, **kwargs)
        self.book.events = self.events  # one owner, distinct per-feed generations
        self.book.update_book = self.update_book
        self._book_ids = {}

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

    @staticmethod
    def _quote_clock():
        return time.monotonic(), time.time(), time.monotonic_ns()

    def _quote(self, feed, symbol, tick=None, *, clock=None):
        mono, epoch, nano = self._quote_clock() if clock is None else clock
        with feed._lock:
            tick = feed.processed.get(symbol) if tick is None else tick
            tick = feed.quote_tick(symbol, tick)
            if (self._closing.is_set() or feed.overloaded or -1 in self.errors or not tick or not tick.valid
                    or tick.group in feed.errors or tick.group not in feed.receipts
                    or tick.generation != feed.generation[tick.group]
                    or mono-feed.receipts[tick.group][0] > ticker_ws.MAX_SILENCE_SECONDS
                    or not -5 <= epoch-tick.exchange_ms/1000 <= cfg.PRICE_EXCHANGE_MAX_AGE_SECONDS
                    or (nano-tick.received_ns)/1e9 > cfg.PRICE_EXCHANGE_MAX_AGE_SECONDS):
                return None
            return float(tick.price)

    def healthy(self):
        clock = self._quote_clock()
        return any(self._quote(feed, symbol, clock=clock) is not None
                   for symbol in self.symbols for feed in (self, self.book))

    def order_allowed(self, symbol):
        clock = self._quote_clock()
        return (self._quote(self, symbol, clock=clock) is not None
                or self._quote(self.book, symbol, clock=clock) is not None)

    def prices(self, symbols=None):
        # Deliberately keep tick-only valuation and the signal price mirror.
        clock = self._quote_clock()
        return {symbol: price for symbol in (self.symbols if symbols is None else symbols)
                if (price := self._quote(self, symbol, clock=clock)) is not None}

    def price_maps(self, symbols=None):
        """One owner-cycle view; send guards always re-read live feed health."""
        clock = self._quote_clock()
        ticks, bids, entries = {}, {}, {}
        for symbol in self.symbols if symbols is None else symbols:
            tick = self._quote(self, symbol, clock=clock)
            bid = self._quote(self.book, symbol, clock=clock)
            if tick is not None:
                ticks[symbol] = tick
            if bid is not None:
                bids[symbol] = bid
            selected = bid if tick is None else tick if bid is None else min(tick, bid)
            if selected is not None:
                entries[symbol] = selected
        return ticks, bids, entries

    def entry_quote(self, symbol):
        clock = self._quote_clock()
        tick = self._quote(self, symbol, clock=clock)
        bid = self._quote(self.book, symbol, clock=clock)
        candidates = [(price, source) for price, source in ((tick, 'tick'), (bid, 'bid0')) if price is not None]
        selected, source = min(candidates, default=(None, None))
        with self.book._lock:
            book_tick = self.book.quote_tick(symbol, self.book.processed.get(symbol))
            quantity = float(book_tick.quantity or 0) if book_tick and bid is not None else None
        return dict(tick=tick, bid0=bid, selected=selected, source=source,
                    bid_quantity=quantity)

    def send_quote(self, symbol):
        """Final guard reads ingress, without advancing ordered owner state.

        A rebound/invalidation already received must revoke an older decision.
        A quote must also remain within the owner's existing queue-age budget
        at send, including time spent registering the exact durable intent.
        """
        clock = self._quote_clock()
        prices, quantity = {}, None
        for source, feed in (('tick', self), ('bid0', self.book)):
            with feed._lock:
                latest = feed.latest_ingress.get(symbol)
                price = self._quote(feed, symbol, latest, clock=clock) if latest is not None else None
                if latest is not None and (clock[2]-latest.received_ns)/1e9 > cfg.WS_MAX_QUEUE_AGE_SECONDS:
                    price = None
                prices[source] = price
                if source == 'bid0' and price is not None:
                    quantity = float(latest.quantity or 0)
        selected, source = min(((p, s) for s, p in prices.items() if p is not None), default=(None, None))
        return dict(prices, selected=selected, source=source, bid_quantity=quantity)

    def entry_prices(self, symbols=None):
        return self.price_maps(symbols)[2]

    def entry_timing(self, symbol, source):
        feed = self.book if source == 'bid0' else self
        with feed._lock:
            tick = feed.quote_tick(symbol, feed.processed.get(symbol))
            if tick is None or self._quote(feed, symbol, tick) is None:
                return {}
            return dict(quote_source='depth' if source == 'bid0' else 'tick',
                        exchange_time_ms=tick.exchange_ms, received_epoch_ms=tick.received_epoch_ms,
                        _received_ns=tick.received_ns)

    def quote_state(self, symbol):
        if self.order_allowed(symbol):
            return 'quote_ready'
        if symbol not in self.processed and symbol not in self.book.processed:
            return 'quote_waiting_first_tick'
        return 'quote_stale_or_disconnected_both_sources'

    def snapshot(self):
        result = super().snapshot()
        clock = self._quote_clock()
        with self._lock:
            result['prices'] = {s: dict(price=tick.price, exchange_time_ms=tick.exchange_ms)
                                for s, tick in self.committed.items()
                                if self._quote(self, s, tick, clock=clock) is not None}
            tick_ready = (not self.overloaded and not self.errors and len(self.receipts) == len(self.groups)
                          and all(time.monotonic()-r[0] <= ticker_ws.MAX_SILENCE_SECONDS for r in self.receipts.values()))
            result['status'] = 'ok' if tick_ready and result['status'] == 'ok' else 'error'
        return result

    def decide(self, tick, callback):
        if tick is not None and tick.source == 'bid0':
            return self.book.decide(tick, callback)
        return super().decide(tick, callback)

    def metrics(self):
        ticks, bids, entries = self.price_maps()
        result = super().metrics(fresh_prices=ticks)
        values = sorted(self.latencies_us + self.book.latencies_us)
        for suffix, fraction in (('p50', .5), ('p95', .95), ('p99', .99)):
            result['receive_to_decision_us_'+suffix] = values[int((len(values)-1)*fraction)] if values else None
        result['receive_to_decision_us_max'] = max(values, default=None)
        book_metrics = self.book.metrics(fresh_prices=bids)
        for key in ('received', 'decided', 'rejected_old', 'rejected_backlog', 'overflow',
                    'enqueued', 'unchanged', 'overload_discarded'):
            result[key] += self.book.counts[key]
        result['queue_peak'] = max(self.queue_peak, self.book.queue_peak)
        for key in ('receive_per_second', 'decide_per_second', 'enqueue_per_second', 'unchanged_per_second',
                    'expired_per_second', 'old_per_second', 'discarded_per_second'):
            result[key] += book_metrics[key]
        result.update(bid0_symbols=len(bids), entry_symbols=len(entries),
                      book_received=self.book.counts['received'], book_decided=self.book.counts['decided'],
                      book_rejected_backlog=self.book.counts['rejected_backlog'],
                      book_overflow=self.book.counts['overflow'])
        with self._lock:
            result['tick_errors'] = dict(self.errors)
        with self.book._lock:
            result['depth_errors'] = dict(self.book.errors)
        result['proxy'] = 'disabled/direct'
        result['depth_endpoint'] = ticker_ws.BOOK_WS_BASE
        result['recent_errors'] = sorted(self.recent_errors()+self.book.recent_errors(),
                                         key=lambda row: row['last_epoch'], reverse=True)[:5]
        return result

    async def _receive(self):
        await self._receive_feeds(((self.book, 'bookTicker', ticker_ws.BOOK_WS_BASE),
                                   (self, 'aggTrade', ticker_ws.WS_BASE)))
