"""Exact-identity exchange adapter for the v9711 live trade process.

Only deterministic ``cfg.ORDER_CLIENT_PREFIX_*`` client IDs registered in the local intent
ledger are owned.  Aggregate account positions are never used to infer order
ownership, matching ``trade_ws_prepend_1026.py``.
"""
from __future__ import annotations

import hashlib
import copy
from contextlib import contextmanager
import threading
import json
import logging
import math
import os
import sys
import time
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, localcontext
from pathlib import Path
from typing import Any, Mapping

from .. import config as cfg
from ..contracts import atomic_write_json, read_json
from ..local_poll import CachedJsonFile
from .stream import AccountStreamView, StreamUnavailable
from .submit_errors import ConfirmedSubmitRejection, EntryQuoteUnavailable, is_transport_error
from .nonblocking_io import NetworkPending
from .batch_entry import BatchEntryMixin
from .signal_follow import follow_send_allowed
from .order_identity import matches_order_identity
from .entry_timing import EntrySendProbe

LOG = logging.getLogger('v9711_live_trade.client')

# Per-exact-identity query cooldown, independent of the initial unknown grace.
# Stream events are still consumed immediately while the network query backs off.
ORDER_QUERY_RETRY_SECONDS = 5.0
ORDER_QUERY_MAX_RETRY_SECONDS = 60.0
ORDER_QUERY_WARNING_SECONDS = 60.0
ORDER_ABSENCE_CONFIRMATIONS = 2
ORDER_ABSENCE_MAX_AGE_SECONDS = 86400.0
BROKER_RATE_LIMIT_COOLDOWN_SECONDS = 60.0



class DisabledExchange(RuntimeError):
    """No exchange client is configured or live trading is not enabled."""


class OwnershipLedger:
    """Durable exact-order intents and executor-owned position records."""

    def __init__(self, path: Path = cfg.EXECUTION_INTENT_FILE,
                 state_path: Path = cfg.EXECUTION_LEDGER_FILE):
        self.path = Path(path)
        self.state_path = Path(state_path)
        from .durable_writer import embedded_intents, resolve_store
        state_payload = resolve_store(read_json(self.state_path) or {}, self.state_path)
        canonical = embedded_intents(state_payload)
        self.intents = dict(canonical) if canonical is not None else self._load(self.path, 'intents', {})
        initial_state = {
            'schema_version': 1,
            'tracked_orders': {},
            'owned_positions': {},
            'reopen_until_epoch': {},
            'realized_balance': cfg.EQUITY0,
        }
        self.state = dict(state_payload['state']) if isinstance(state_payload.get('state'), Mapping) else initial_state
        self.state.setdefault('tracked_orders', {})
        self.state.setdefault('owned_positions', {})
        self.state.setdefault('reopen_until_epoch', {})
        self.state.setdefault('realized_balance', cfg.EQUITY0)
        self.state.setdefault('unknown_since_epoch', {})
        self.state.setdefault('signal_session_id', None)
        self.state.setdefault('last_signal_revision', None)
        self._recover_native_slot_intents()
        self._saved_intents = self._saved_state = None
        self._save_depth = 0
        self._save_pending = False
        self._background = None
        self._background_closed = False
        self._last_barrier = None
        self._dispatch_group = None
        self._delta_capture = None

    def _recover_native_slot_intents(self):
        # A crash between the two atomic file replacements can leave a native
        # occupied slot without its intent (including after a terminal forget).
        # Recover ONLY its existing exact CID for reconciliation, never create
        # a new order or infer ownership from an aggregate account position.
        if self.state.get('strategy_version') != cfg.VERSION:
            return
        for section, field, kind in (('tracked_orders', 'client_order_id', 'entry'),
                                     ('owned_positions', 'close_client_order_id', 'close')):
            for symbol, row in self.state.get(section, {}).items():
                if not isinstance(row, Mapping):
                    continue
                cid = str(row.get(field) or '')
                if not cid.startswith(cfg.ORDER_CLIENT_PREFIX+'_') or 'c:'+cid in self.intents:
                    continue
                if row.get('symbol') not in (None, '', symbol):
                    raise RuntimeError('conflicting native exact-order slot symbol; retained for inspection')
                oid = _text(row.get('order_id') if kind == 'entry' else row.get('close_order_id'))
                alias = self.intents.get(f'o:{symbol}:{oid}') if oid else None
                if alias and (alias.get('client_order_id') != cid or alias.get('symbol') != symbol):
                    raise RuntimeError('conflicting native exact-order slot/intent; retained for inspection')
                self.intents['c:'+cid] = dict(symbol=symbol, client_order_id=cid, order_id=oid,
                    kind=kind, limit_price=_finite(row.get('limit_price')), qty=_finite(
                        row.get('qty') if kind == 'entry' else row.get('close_quantity')),
                    exit_due_epoch=_finite(row.get('exit_due_epoch')),
                    registered_at_epoch=_finite(row.get('placed_at_epoch') if kind == 'entry'
                                                else row.get('close_requested_epoch')),
                    recovered_from_native_slot=True)
                if oid:
                    self.intents[f'o:{symbol}:{oid}'] = self.intents['c:'+cid]

    def unknown_since(self, key: str, *, now: float) -> float:
        existing = _finite(self.state['unknown_since_epoch'].get(key))
        if existing <= 0.0:
            self.state['unknown_since_epoch'][key] = float(now)
            self.save()
            existing = float(now)
        return existing

    def clear_unknown(self, key: str) -> None:
        if self.state['unknown_since_epoch'].pop(key, None) is not None:
            self.save()

    @staticmethod
    def _load(path: Path, key: str, default: dict[str, Any]) -> dict[str, Any]:
        payload = read_json(path) or {}
        value = payload.get(key)
        return dict(value) if isinstance(value, Mapping) else dict(default)

    def save(self) -> None:
        if self._save_depth:
            self._save_pending = True
            return
        self.flush_for_dispatch()

    @contextmanager
    def batch_saves(self):
        """Coalesce owner mutations; senders must flush before dispatch."""
        self._save_depth += 1
        try:
            yield
        finally:
            self._save_depth -= 1
            if not self._save_depth and self._save_pending:
                self.flush_for_dispatch()

    def flush(self) -> None:
        if self._background is not None:
            self.flush_for_dispatch().result()
            export = getattr(self._background, 'export_intents', None)
            if export is not None:
                export()
            return
        # A successful save is still durable before dispatch. Unchanged files
        # need no new fsync; cache each document ONLY after its replace succeeds
        # so an interrupted second write is retried on the next owner cycle.
        for name, path, value in (('intents', self.path, self.intents),
                                  ('state', self.state_path, self.state)):
            encoded = json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
            attr = '_saved_' + name
            if encoded != getattr(self, attr):
                atomic_write_json(path, {
                    'schema_version': 1, 'component': 'execution',
                    'updated_at_epoch': time.time(), name: value})
                setattr(self, attr, encoded)
        self._save_pending = False

    def flush_pending(self):
        if self._save_pending:
            return self.flush_for_dispatch()
        return self._last_barrier

    def enable_background(self, *, atomic_bundle=False, persistent_slots=False, incremental=False):
        if self._background is None:
            if incremental:
                from .journal_store import DeltaCapture, JournalWriter
                capture = DeltaCapture(self.intents, self.state)
                self._background = JournalWriter(self.path, self.state_path, initial=capture.root)
                self._delta_capture = capture
                return
            from .durable_writer import BundleWriter, DurableWriter, SlotWriter
            writer = SlotWriter if persistent_slots else BundleWriter if atomic_bundle else DurableWriter
            self._background = writer(self.path, self.state_path,
                saved=(self._saved_intents, self._saved_state))
            if persistent_slots:
                # Pre-open/seed both slots before WS ingress starts. No entry
                # is allowed to pay first-time initialization at its crossing.
                try:
                    self.flush_for_dispatch().result()
                except BaseException:
                    try:
                        self._background.close()
                    finally:
                        self._background = self._last_barrier = None
                    raise

    def flush_for_dispatch(self):
        if self._dispatch_group is not None:
            self._save_pending = True
            self._last_barrier = self._dispatch_group
            return self._dispatch_group
        if self._background is None:
            self.flush()
            return None
        from .durable_writer import Snapshot
        reservations = set()
        for name, field, kind in (('tracked_orders', 'client_order_id', 'entry'),
                                  ('owned_positions', 'close_client_order_id', 'close')):
            for symbol, row in self.state.get(name, {}).items():
                cid = str(row.get(field) or '')
                intent = self.intents.get('c:'+cid) or {}
                if cid and intent.get('symbol') == symbol and intent.get('kind') == kind and intent.get('client_order_id') == cid:
                    reservations.add((symbol, cid))
        if self._delta_capture is not None:
            delta, root = self._delta_capture.prepare(self.intents, self.state, reservations)
            barrier = self._background.submit(delta)
            self._delta_capture.root = root
            self._last_barrier = barrier
            self._save_pending = False
            return barrier
        encode = lambda value: json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
        self._last_barrier = self._background.submit(Snapshot(
            encode(self.intents), encode(self.state), frozenset(reservations)))
        self._save_pending = False
        return self._last_barrier

    @contextmanager
    def dispatch_group(self):
        """One owner admission plan, one complete snapshot, no early sender.

        Each request receives the group's unresolved positive receipt. Only
        after all slots/identities have been reserved does the owner serialize
        and submit the actual snapshot. Nested groups share that same barrier.
        """
        if self._background is None or self._dispatch_group is not None:
            yield
            return
        from concurrent.futures import Future
        group = self._dispatch_group = Future()
        try:
            yield
        finally:
            self._dispatch_group = None
            try:
                barrier = self.flush_for_dispatch()
            except BaseException as exc:
                group.set_exception(exc)
                raise
            else:
                def complete(saved):
                    try:
                        receipt = saved.result()
                    except BaseException as exc:
                        group.set_exception(exc)
                    else:
                        group.set_result(receipt)
                barrier.add_done_callback(complete)

    def close_background(self):
        if self._background is not None and not self._background_closed:
            try:
                self.flush()
            finally:
                self._background.close()
                self._background_closed = True

    def client_id(self, symbol: str, bar_open_ms: int, kind: str) -> str:
        generation = self.state.get('sizing_generations', {}).get(f'{symbol.upper()}:{int(bar_open_ms)}', 0) if kind == 'entry' else 0
        raw = f'{cfg.ORDER_CLIENT_PREFIX}:{kind}:{symbol.upper()}:{int(bar_open_ms)}:{generation}'
        digest = hashlib.sha1(raw.encode()).hexdigest()[:12]
        # Binance client order id has a 36-character limit.
        return f'{cfg.ORDER_CLIENT_PREFIX}_{kind}_{digest}'

    def reserve_entry_client_id(self, symbol: str, bar_open_ms: int) -> str:
        # Separate namespace from legacy deterministic symbol/bar IDs. Persist
        # before registration/send; a skipped number after interruption is safe.
        sequence = int(self.state.get('entry_submission_sequence', 0)) + 1
        self.state['entry_submission_sequence'] = sequence
        raw = f'{cfg.ORDER_CLIENT_PREFIX}:physical-entry-v2:{symbol.upper()}:{int(bar_open_ms)}:{sequence}'
        cid = f'{cfg.ORDER_CLIENT_PREFIX}_entry_{hashlib.sha1(raw.encode()).hexdigest()[:20]}'
        self.save()
        return cid

    def register(self, *, symbol: str, client_order_id: str, order_id: Any = None,
                 kind: str, limit_price: float = 0.0, qty: float = 0.0,
                 exit_due_epoch: float = 0.0) -> None:
        key = f'c:{client_order_id}'
        self.intents[key] = {
            'symbol': symbol.upper(), 'client_order_id': client_order_id,
            'order_id': _text(order_id), 'kind': kind,
            'limit_price': float(limit_price), 'qty': float(qty),
            'exit_due_epoch': float(exit_due_epoch),
            'registered_at_epoch': time.time(),
        }
        if _text(order_id):
            self.intents[f'o:{symbol.upper()}:{_text(order_id)}'] = self.intents[key]
        self.save()

    def owns_client(self, client_order_id: str) -> bool:
        return f'c:{_text(client_order_id)}' in self.intents

    def owns_order(self, symbol: str, order_id: Any, client_order_id: str = '') -> bool:
        if client_order_id:
            record = self.intents.get(f'c:{_text(client_order_id)}')
            if isinstance(record, Mapping) and _text(record.get('symbol')).upper() != _text(symbol).upper():
                return False
            if self.owns_client(client_order_id):
                return True
        return f'o:{_text(symbol).upper()}:{_text(order_id)}' in self.intents

    def forget(self, *, symbol: str, client_order_id: str = '', order_id: Any = '') -> None:
        keys = []
        if client_order_id:
            keys.append(f'c:{_text(client_order_id)}')
        if _text(order_id):
            keys.append(f'o:{_text(symbol).upper()}:{_text(order_id)}')
        for key in keys:
            self.intents.pop(key, None)
            self.state['unknown_since_epoch'].pop(key, None)
        self.save()


class ExchangeClient(BatchEntryMixin):
    """Thin adapter over the repository's synchronous Binance WS broker."""

    def __init__(self, *, enabled: bool = cfg.TRADE_ENABLED,
                 dry_run: bool = cfg.DRY_RUN,
                 account_stream_dir: Path = cfg.ACCOUNT_STREAM_DIR,
                 exchange_info_cache_file: Path = cfg.EXCHANGE_INFO_CACHE_FILE,
                 price_file: Path = cfg.PRICE_FILE):
        self.enabled = bool(enabled)
        self.dry_run = bool(dry_run)
        self.stream = AccountStreamView(account_stream_dir)
        self.exchange_info_cache_file = Path(exchange_info_cache_file)
        self._metadata_reader = CachedJsonFile(self.exchange_info_cache_file)
        self._metadata_lock = threading.RLock()
        self.price_file = Path(price_file)
        self.network_io = None
        self.entry_timings = None
        from .request_activity import RequestActivity
        self.request_activity = RequestActivity()  # Shared by shallow-copied worker lanes.
        self._transport_local = threading.local()
        self._transport_lock = threading.Lock()
        self._transport_lanes = {}
        self._broker: Any = None
        self._meta_cache: dict[str, dict[str, Any]] = {}
        self._metadata_snapshot_enabled = False
        self._metadata_payload = None
        self._metadata_publication = [None]  # Shared by shallow-copied worker lanes.
        self._meta_revision = -1
        self._prepared_cache = {}
        self._order_queries: dict[str, dict[str, Any]] = {}
        self._io_broker: Any = None
        self._broker_last_io_mono = 0.0
        self._broker_reconnect_needed = False
        self._broker_reconnect_after_mono = 0.0
        self._broker_rate_limit_until_mono = 0.0

    def _require_broker(self) -> Any:
        if not self.enabled:
            raise DisabledExchange('V9711_LIVE_TRADE_ENABLED=0; exact-order simulation only')
        if self._broker is None:
            from ..exchange import binance_futures_trade_ws as ws
            key = os.environ.get('BINANCE_API_KEY', '')
            secret = os.environ.get('BINANCE_API_SECRET', '')
            if not key or not secret:
                raise DisabledExchange('BINANCE_API_KEY/BINANCE_API_SECRET not available')
            self._broker = ws.BinanceWSBroker(key, secret, dry_run=self.dry_run)
            self._broker._broker.request_activity = self.request_activity
        return self._broker

    def _ready_broker(self) -> Any:
        """Reuse healthy sockets; managed brokers recover in the background, never replay."""
        now_mono = time.monotonic()
        if now_mono < self._broker_rate_limit_until_mono:
            raise ConnectionError('trade broker rate limit cooldown; no request sent')
        broker = self._require_broker()
        if broker is not self._io_broker:
            self._io_broker = broker
            self._broker_last_io_mono = now_mono
            self._broker_reconnect_needed = False
            self._broker_reconnect_after_mono = 0.0
        if getattr(type(broker), 'MANAGES_WS_LIFECYCLE', False) is True:
            # The core protects send/ACK and background reconnect with one lock.
            # Never force-close a connection its supervisor already recovered.
            self._broker_reconnect_needed = False
            return broker
        if self._broker_reconnect_needed:
            if now_mono < self._broker_reconnect_after_mono:
                raise ConnectionError('trade WS reconnect cooldown; no request sent')
            try:
                broker.reconnect()
            except Exception:
                self._broker_reconnect_needed = True
                self._broker_reconnect_after_mono = time.monotonic() + cfg.ORDER_WS_RECONNECT_RETRY_SECONDS
                raise
            LOG.info('trade WS reconnected reason=transport_error')
            self._broker_reconnect_needed = False
            self._broker_reconnect_after_mono = 0.0
            self._broker_last_io_mono = time.monotonic()
        return broker

    def _worker_transport(self):
        lane = getattr(self._transport_local, 'lane', None)
        if lane is None:
            # Same configuration and injectable test factory, but NEVER share
            # the broker's receive lock or reconnect state across workers.
            lane = copy.copy(self)
            lane.network_io = None
            lane._broker = lane._io_broker = None
            lane._broker_last_io_mono = 0.0
            lane._broker_reconnect_needed = False
            lane._broker_reconnect_after_mono = 0.0
            lane._broker_rate_limit_until_mono = 0.0
            self._transport_local.lane = lane
            with self._transport_lock:
                self._transport_lanes[threading.get_ident()] = lane
        return lane

    def close_worker_transport(self) -> None:
        lane = getattr(self._transport_local, 'lane', None)
        broker = getattr(lane, '_broker', None) if lane is not None else None
        close = getattr(broker, 'close', None)
        if callable(close):
            try:
                close()  # worker is exiting: no other request uses this lane
            except Exception:
                LOG.warning('trade transport close failed during shutdown', exc_info=True)
        if lane is not None:
            with self._transport_lock:
                self._transport_lanes.pop(threading.get_ident(), None)

    def warm_worker_transport(self) -> None:
        if self.enabled and not self.dry_run:
            self._worker_transport()._ready_broker()  # Connect/keepalive only, never send an order.

    def _record_rate_limit(self, value) -> None:
        code = str(value.get('code', '')) if isinstance(value, Mapping) else str(getattr(value, 'code', ''))
        status = str(getattr(value, 'status_code', getattr(value, 'status', '')))
        text = str(value).lower()
        if (code in ('-1003', '-1015') or status in ('429', '418')
                or any(marker in text for marker in ('too many requests', 'too many new orders', 'rate limit'))):
            with self._transport_lock:
                self._broker_rate_limit_until_mono = max(self._broker_rate_limit_until_mono,
                    time.monotonic() + BROKER_RATE_LIMIT_COOLDOWN_SECONDS)

    def _broker_call(self, method: str, *args: Any, before_send: Any = None,
                     ws_only: bool = False, **kwargs: Any) -> Any:
        if self.network_io is not None and not self.network_io.in_worker():
            if method not in {'get_order', 'get_order_by_client_order_id'}:
                raise RuntimeError('mutating broker I/O must be scheduled by the owner loop')
            key = ('query', method, str(args[0]), str(args[1]))
            return self.network_io.poll(key,
                lambda: self._broker_call(method, *args, before_send=before_send,
                                          ws_only=ws_only, **kwargs), priority=5)
        if self.network_io is not None and self.network_io.in_worker():
            if time.monotonic() < self._broker_rate_limit_until_mono:
                raise ConnectionError('trade broker rate limit cooldown; no request sent')
            lane = self._worker_transport()
            try:
                return lane._broker_call(method, *args, before_send=before_send,
                                         ws_only=ws_only, **kwargs)
            except Exception as exc:
                self._record_rate_limit(exc)
                raise
        broker = self._ready_broker()
        checked = getattr(type(broker), 'place_order_checked', None)
        checked_entry = method == 'place_order' and callable(checked)
        if before_send is not None and not checked_entry:
            before_send()  # A reconnect may have consumed the last seconds of a bar.
        try:
            if checked_entry:
                # Validate once INSIDE the socket lock after reconnect;
                # UNKNOWN responses never trigger an automatic REST resend.
                return broker.place_order_checked(*args, before_send=before_send, **kwargs)
            # Legacy/injected BinanceWSBroker.place_order may resend MARKET SELL through REST
            # after any WS exception. A close must NOT be resubmitted before
            # resolving its exact fate (a market order may already be filled).
            # Use the same broker's WS coroutine directly for close submissions;
            # keep reconnection/cooldown handling here and query recovery above.
            if ws_only and callable(getattr(type(broker), '_run', None)):
                core = getattr(broker, '_broker', None)
                direct = getattr(core, method, None)
                if callable(direct):
                    return broker._run(direct(*args, **kwargs))
            return getattr(broker, method)(*args, **kwargs)
        except Exception as exc:
            code = str(getattr(exc, 'code', ''))
            status = str(getattr(exc, 'status_code', getattr(exc, 'status', '')))
            if (code in ('-1003', '-1015') or status in ('429', '418')
                    or any(marker in str(exc).lower()
                           for marker in ('too many requests', 'too many new orders', 'rate limit'))):
                self._broker_rate_limit_until_mono = time.monotonic() + BROKER_RATE_LIMIT_COOLDOWN_SECONDS
            if is_transport_error(exc):
                self._broker_reconnect_needed = True
                # Query scheduling supplies the delay. A failing reconnect itself
                # has its own cooldown, separate from submission fate.
            raise
        finally:
            self._broker_last_io_mono = time.monotonic()

    def _metadata_view(self):
        publication = self._metadata_publication[0]
        if publication is None:
            payload = self._metadata_reader.read()
            revision = self._metadata_reader.revision
        else:
            payload, revision = publication
        if self._meta_revision != revision:
            self._meta_cache.clear()
            self._prepared_cache.clear()
            self._meta_revision = revision
        return payload

    def refresh_metadata_snapshot(self):
        """Owner maintenance publishes a revision; workers use memory only."""
        # Do not hold the worker's precision lock across stat/open/JSON reads.
        # Until the read completes, senders use the last coherent publication.
        payload = self._metadata_reader.read()
        return self.publish_metadata_snapshot(payload, self._metadata_reader.revision)

    def publish_metadata_snapshot(self, payload, revision):
        # Invalidate per-lane caches even if a stalled reader becomes unavailable
        # and later recovers with the SAME source revision.
        revision = (revision, payload is not None)
        with self._metadata_lock:
            self._metadata_publication[0] = (payload, revision)
            self._metadata_payload = payload
            self._metadata_snapshot_enabled = True
            return self._metadata_view()

    def _symbol_meta(self, symbol: str) -> dict[str, Any]:
        symbol = str(symbol).upper()
        with self._metadata_lock:
            payload = self._metadata_view()
            cached = self._meta_cache.get(symbol)
            if cached is not None:
                return cached
        symbols = payload.get('symbols') if isinstance(payload, dict) else None
        item = symbols.get(symbol) if isinstance(symbols, dict) else None
        if not isinstance(item, dict):
            raise ValueError(f'{symbol}: exchange metadata missing/invalid at '
                             f'{self.exchange_info_cache_file}; run '
                             'python -m scripts.refresh_exchange_info --output <cache-path>')
        meta = {}
        for name in ('tick', 'step', 'min_qty', 'min_notional'):
            value = _finite(item.get(name), float('nan'))
            if not math.isfinite(value) or value < 0 or (name in ('tick', 'step') and value <= 0):
                raise ValueError(f'{symbol}: invalid exchange metadata field {name} '
                                 f'at {self.exchange_info_cache_file}')
            meta[name] = value
        meta['tick_text'] = str(item['tick'])
        meta['step_text'] = str(item['step'])
        with self._metadata_lock:
            publication = self._metadata_publication[0]
            if payload is (publication[0] if publication is not None else self._metadata_reader.payload):
                self._meta_cache[symbol] = meta
        return meta

    @staticmethod
    def _align_down(value: float, step: float) -> float:
        if not math.isfinite(value) or value <= 0.0 or not math.isfinite(step) or step <= 0.0:
            return 0.0
        return math.floor(value / step + 1e-12) * step

    @staticmethod
    def _decimal_text(value: float) -> str:
        return format(value, '.12f').rstrip('0').rstrip('.') or '0'

    def price_text(self, symbol: str, price: float) -> str:
        meta = self._symbol_meta(symbol)
        return self._price_text(meta, price, symbol)

    @staticmethod
    def _price_text(meta, price, symbol):
        with localcontext() as context:
            context.prec = 40
            tick = Decimal(meta['tick_text'])
            aligned = (Decimal(str(price)) / tick).to_integral_value(rounding=ROUND_FLOOR) * tick
        if not aligned.is_finite() or aligned <= 0:
            raise ValueError(f'{symbol}: aligned price below tick size')
        text = format(aligned, 'f')
        return text.rstrip('0').rstrip('.') if '.' in text else text

    def qty_text(self, symbol: str, notional: float, price: float) -> str:
        meta = self._symbol_meta(symbol)
        if price <= 0:
            return '0'
        with localcontext() as context:
            context.prec = 40
            step = Decimal(meta['step_text'])
            quantity = (Decimal(str(notional)) / Decimal(str(price)) / step).to_integral_value(
                rounding=ROUND_FLOOR) * step
        text = format(quantity, 'f')
        return text.rstrip('0').rstrip('.') if '.' in text else text

    def validate_order(self, symbol: str, price: float, qty: float) -> tuple[bool, str]:
        """Validate exchange step, minimum quantity and minimum notional."""
        meta = self._symbol_meta(symbol)
        return self._validate_order(meta, price, qty)

    @classmethod
    def _validate_order(cls, meta, price, qty):
        step = _finite(meta.get('step'))
        min_qty = _finite(meta.get('min_qty'), step)
        min_notional = _finite(meta.get('min_notional'))
        if price <= 0.0 or qty <= 0.0:
            return False, 'non-positive price or quantity'
        aligned = cls._align_down(qty, step)
        if abs(aligned - qty) > max(1e-12, step * 1e-8):
            return False, f'quantity {qty} is not stepSize {step} aligned'
        if qty + max(1e-12, min_qty * 1e-9) < min_qty:
            return False, f'quantity {qty} is below minQty {min_qty}'
        if min_notional > 0.0 and qty * price + 1e-8 < min_notional:
            return False, f'notional {qty*price} is below minNotional {min_notional}'
        return True, 'ok'

    def prepare_limit(self, symbol: str, limit_price: float,
                      notional: float) -> dict[str, Any]:
        """Lift positive entry plans to all minimums; never top up a holding.

        Same round-up policy as the 1026 launcher's sizing chain: target the
        larger of the plan and exchange minNotional, respect minQty, then ceil
        to stepSize. This executor also enforces the user's 5 USDT entry floor.
        Price tick alignment happens first. Invalid/zero plans stay invalid.
        """
        requested = _finite(notional, float('nan'))
        price = _finite(limit_price, float('nan'))
        if not (math.isfinite(requested) and requested > 0.0
                and math.isfinite(price) and price > 0.0):
            return {'ok': False, 'reason': 'non-positive/non-finite entry price or notional'}
        meta = self._symbol_meta(symbol)
        cache_key = (str(symbol).upper(), price, requested, meta['tick_text'],
                     meta['step_text'], meta['min_qty'], meta['min_notional'])
        with self._metadata_lock:
            cached = self._prepared_cache.get(cache_key)
        if cached is not None:
            return dict(cached)
        step = _finite(meta.get('step'), float('nan'))
        min_qty = _finite(meta.get('min_qty'), step)
        min_notional = _finite(meta.get('min_notional'))
        if not (math.isfinite(step) and step > 0.0
                and math.isfinite(min_qty) and min_qty >= 0.0
                and math.isfinite(min_notional) and min_notional >= 0.0):
            return {'ok': False, 'reason': 'invalid step/minimum entry metadata'}
        price_text = self._price_text(meta, price, symbol)
        # Decimal ceil avoids binary rounding that can leave qty just below a
        # minimum, especially when minQty itself is not a stepSize multiple.
        with localcontext() as context:
            context.prec = 40
            aligned_price = Decimal(price_text)
            step_decimal = Decimal(meta['step_text'])
            requested_decimal = Decimal(str(requested))
            target = max(requested_decimal, Decimal('5'), Decimal(str(min_notional)))
            notional_units = (target / (aligned_price * step_decimal)).to_integral_value(
                rounding=ROUND_CEILING)
            minimum_units = (Decimal(str(min_qty)) / step_decimal).to_integral_value(
                rounding=ROUND_CEILING)
            quantity = max(notional_units, minimum_units) * step_decimal
            actual_notional = quantity * aligned_price
            qty_text = format(quantity, 'f')
            if '.' in qty_text:
                qty_text = qty_text.rstrip('0').rstrip('.')
            adjusted = actual_notional != requested_decimal
        ok, why = self._validate_order(meta, float(aligned_price), _finite(qty_text))
        result = {'ok': ok, 'reason': why, 'price': price_text, 'qty': qty_text,
                'requested_notional': requested, 'target_notional': float(target),
                'actual_notional': float(actual_notional), 'sizing_adjusted': adjusted,
                'min_qty': min_qty, 'min_notional': min_notional, 'step': step}
        with self._metadata_lock:
            if len(self._prepared_cache) >= 8192:
                self._prepared_cache.clear()
            self._prepared_cache[cache_key] = result
        return dict(result)

    def place_limit_for_bar(self, *, order_bar_open_ms: int, **kwargs: Any) -> dict[str, Any]:
        """Entry runner capability: recheck the same bar after WS reconnection."""
        return self.place_limit(order_bar_open_ms=order_bar_open_ms, **kwargs)

    def place_limit(self, *, symbol: str, client_order_id: str,
                    limit_price: float, notional: float,
                    order_bar_open_ms: int | None = None) -> dict[str, Any]:
        prepared = self.prepare_limit(symbol, limit_price, notional)
        if not prepared['ok']:
            raise ValueError(f'{symbol}: {prepared["reason"]}')
        qty_text = prepared['qty']
        price_text = prepared['price']
        if self.dry_run or not self.enabled:
            return {'_simulated': True, 'symbol': symbol.upper(),
                    'clientOrderId': client_order_id, 'orderId': f'sim-{int(time.time()*1000)}',
                    'status': 'NEW', 'price': price_text, 'origQty': qty_text,
                    'executedQty': '0'}
        send_quote = {}
        def check_bar() -> None:
            nonlocal send_quote
            self._check_entry_market(symbol.upper())
            if self.network_io is not None:
                following = self.network_io.check_entry(symbol, client_order_id, int(order_bar_open_ms or 0))
            else:
                following = follow_send_allowed(getattr(self, 'entry_gate', {}), symbol, client_order_id,
                                                int(order_bar_open_ms or 0), now=time.time())
            detail = self._checked_entry_quote(symbol.upper(), client_order_id, limit_price)
            if not following and order_bar_open_ms is not None and not (
                    order_bar_open_ms > 0
                    and order_bar_open_ms <= time.time() * 1000 < order_bar_open_ms + cfg.BAR_MS):
                raise ConfirmedSubmitRejection('entry order bar expired/not active before send')
            send_quote = detail
        result = self._broker_call(
            'place_order', symbol.upper(), 'BUY', qty_text,
            price=price_text, position_side='LONG',
            client_order_id=client_order_id,
            before_send=EntrySendProbe(check_bar, self.entry_timings, client_order_id, self.request_activity))
        self._log_entry_quote(symbol.upper(), client_order_id, send_quote)
        return dict(result, _entry_send_quote=send_quote) if send_quote and isinstance(result, Mapping) else result

    def cancel(self, *, symbol: str, order_id: Any = None,
               client_order_id: str = '') -> dict[str, Any]:
        if self.dry_run or not self.enabled:
            return {'_simulated': True, 'symbol': symbol.upper(),
                    'orderId': order_id, 'clientOrderId': client_order_id,
                    'status': 'CANCELED', 'executedQty': '0', 'avgPrice': '0'}
        if order_id in (None, '', 0, '0'):
            # Exact UID lookup is required: never cancel all symbol orders.
            order = self._broker_call('get_order_by_client_order_id', symbol, client_order_id)
            order_id = order.get('orderId')
            if order_id in (None, '', 0, '0'):
                raise RuntimeError(f'{symbol}: exact cancel lookup missing orderId; retain ownership')
        return self._broker_call('cancel_order', symbol.upper(), int(order_id))

    def place_queue_close(self, *, symbol: str, qty: float,
                          client_order_id: str) -> dict[str, Any]:
        """SELL the owned LONG at exchange QUEUE, without a client-side price.

        Do not inherit entry minNotional/minQty bumping: a close may only use
        the still-owned quantity, rounded DOWN by the existing LOT step.
        """
        step = self._symbol_meta(symbol)['step']
        aligned = self._align_down(qty, step) if step > 0.0 else qty
        if not math.isfinite(aligned) or aligned <= 0.0:
            raise ValueError(f'{symbol}: close quantity below step size')
        qty_text = self._decimal_text(aligned)
        if self.dry_run or not self.enabled:
            return {'_simulated': True, 'symbol': symbol.upper(),
                    'clientOrderId': client_order_id, 'orderId': f'simqueue-{time.time_ns()}',
                    'status': 'NEW', 'type': 'LIMIT', 'timeInForce': 'GTC',
                    'priceMatch': 'QUEUE', 'origQty': qty_text,
                    'executedQty': '0', 'avgPrice': '0'}
        return self._broker_call(
            'place_order', symbol.upper(), 'SELL', qty_text, price=None,
            price_match='QUEUE', reduce_only=True, position_side='LONG',
            client_order_id=client_order_id, ws_only=True)

    def place_market_close(self, *, symbol: str, qty: float,
                           client_order_id: str) -> dict[str, Any]:
        market_step = self._symbol_meta(symbol)['step']
        qty = self._align_down(qty, market_step) if market_step > 0.0 else qty
        qty_text = self._decimal_text(qty)
        if qty <= 0.0:
            raise ValueError(f'{symbol}: close quantity below step size')
        if self.dry_run or not self.enabled:
            return {'_simulated': True, 'symbol': symbol.upper(),
                    'clientOrderId': client_order_id, 'orderId': f'simclose-{int(time.time()*1000)}',
                    'status': 'FILLED', 'executedQty': qty_text, 'avgPrice': '0'}
        return self._broker_call(
            'place_order', symbol.upper(), 'SELL', qty_text, price=None,
            reduce_only=False, position_side='LONG',
            client_order_id=client_order_id, ws_only=True)

    @staticmethod
    def _can_confirm_unacknowledged_absence(
            ledger: OwnershipLedger, *, symbol: str, client_order_id: str,
            order_id: Any, now: float) -> bool:
        """Only a recent, never-acknowledged ENTRY can be inferred not accepted.

        Never discard an acknowledged order, old identity, actual position,
        partial fill, or close from a negative lookup. Those require an exact
        terminal event / human reconciliation, not an empty account snapshot.
        """
        if order_id not in (None, '', 0, '0') or not client_order_id:
            return False
        intent = ledger.intents.get(f'c:{client_order_id}') or {}
        row = ledger.state['tracked_orders'].get(symbol.upper()) or {}
        registered = _finite(intent.get('registered_at_epoch'))
        if (intent.get('kind') != 'entry'
                or _text(intent.get('symbol')).upper() != symbol.upper()
                or intent.get('order_id') not in (None, '', 0, '0')
                or row.get('client_order_id') != client_order_id
                or row.get('order_id') not in (None, '', 0, '0')
                or not row.get('fate_unknown')
                or str(row.get('last_status') or '').upper() not in ('', 'SUBMITTING', 'UNKNOWN')
                or not registered > 0.0
                or not 0.0 <= now - registered < ORDER_ABSENCE_MAX_AGE_SECONDS):
            return False
        if symbol.upper() in ledger.state['owned_positions']:
            return False
        return all(field not in row or _finite(row.get(field), float('nan')) == 0.0
                   for field in ('accounted_entry_qty', 'confirmed_filled_qty', 'executedQty'))

    def _fast_unknown_since(self, ledger: OwnershipLedger, *, symbol: str,
                            client_order_id: str, order_id: Any,
                            now: float) -> float | None:
        """Only actionable, unacknowledged entry/close attempts use fast queries."""
        intent = ledger.intents.get(f'c:{client_order_id}') or {}
        if intent.get('kind') == 'entry' and self._can_confirm_unacknowledged_absence(
                ledger, symbol=symbol, client_order_id=client_order_id,
                order_id=order_id, now=now):
            row = ledger.state['tracked_orders'].get(symbol.upper()) or {}
            bar_ms = _finite(row.get('order_bar_open_ms'))
            failed = _finite(row.get('submit_failed_at_epoch'),
                             _finite(intent.get('registered_at_epoch')))
            active_follow = row.get('signal_follow') and failed > 0 and now < failed + cfg.DESIRED_TTL_SECONDS
            if (active_follow or bar_ms > 0 and bar_ms <= now * 1000 < bar_ms + cfg.BAR_MS) and 0 < failed <= now:
                return failed
        elif (intent.get('kind') == 'close'
              and order_id in (None, '', 0, '0')
              and intent.get('order_id') in (None, '', 0, '0')):
            pos = ledger.state['owned_positions'].get(symbol.upper()) or {}
            failed = _finite(pos.get('close_submit_failed_at_epoch'),
                             _finite(pos.get('close_requested_epoch')))
            if (pos.get('close_client_order_id') == client_order_id
                    and pos.get('close_order_id') in (None, '', 0, '0')
                    and str(pos.get('close_status') or '').upper() in ('SUBMITTING', 'UNKNOWN')
                    and 0 < failed <= now < failed + cfg.BAR_MS / 1000.0):
                return failed
        return None

    @staticmethod
    def _queue_close_due_epoch(ledger: OwnershipLedger, *, symbol: str,
                               client_order_id: str, now: float) -> float | None:
        """After QUEUE timeout/cancel, stale NEW must not mask terminal fills."""
        intent = ledger.intents.get(f'c:{client_order_id}') or {}
        pos = ledger.state['owned_positions'].get(symbol.upper()) or {}
        if (intent.get('kind') != 'close' or pos.get('close_phase') != 'LIMIT_QUEUE'
                or pos.get('close_client_order_id') != client_order_id):
            return None
        due = _finite(pos.get('close_cancel_requested_epoch'),
                      _finite(pos.get('close_deadline_epoch')))
        return due if 0 < due <= now else None

    def query_exact_order(self, ledger: OwnershipLedger, *, symbol: str,
                          order_id: Any = None, client_order_id: str = '',
                          now: float | None = None) -> tuple[dict[str, Any] | None, bool]:
        """Local stream first; fast unknown/stable delayed broker query second.

        known=False retains the exact identity and its physical slot.
        ({}, True) is reserved for repeated negative queries of a recent
        entry whose placement was ambiguous and never acknowledged or filled.
        """
        if not ledger.owns_order(symbol, order_id, client_order_id):
            return None, False
        now = time.time() if now is None else float(now)
        key = (f'c:{_text(client_order_id)}' if client_order_id else
               f'o:{_text(symbol).upper()}:{_text(order_id)}')
        payload = None
        try:
            payload = self.stream.lookup_order(
                symbol=symbol, order_id=order_id, client_order_id=client_order_id)
            self.stream.health()
        except StreamUnavailable:
            pass
        intent = ledger.intents.get(f'c:{client_order_id}') or {}
        tracked = ledger.state['tracked_orders'].get(symbol.upper()) or {}
        def valid_evidence(result):
            if not matches_order_identity(result, symbol=symbol,
                                          client_order_id=client_order_id, order_id=order_id):
                return False
            # A pre-upgrade reused CID may still be awaiting its new ACK.
            # Its older terminal stream/cache must not retire this attempt.
            if (intent.get('kind') == 'entry' and order_id in (None, '', 0, '0')
                    and tracked.get('client_order_id') == client_order_id
                    and not tracked.get('entry_identity_version') and not tracked.get('signal_follow')):
                updated = _finite(result.get('updateTime'))
                placed = _finite(tracked.get('placed_at_epoch')) * 1000
                if 0 < updated < placed:
                    return False
            return True
        if payload and not valid_evidence(payload):
            payload = None
        terminal = {'FILLED', 'CANCELED', 'CANCELLED', 'EXPIRED', 'EXPIRED_IN_MATCH', 'REJECTED'}
        def complete_terminal(row):
            status = str(row.get('status') or '').upper()
            qty, avg = _finite(row.get('executedQty')), _finite(row.get('avgPrice'))
            return (status in terminal and (qty <= 0 or avg > 0)
                    and (status != 'FILLED' or qty > 0))
        state = self._order_queries.get(key)
        if state:
            for field in ('confirmed_terminal', 'confirmed_snapshot'):
                if state.get(field) and not valid_evidence(state[field]):
                    state.pop(field, None)
        if payload and complete_terminal(payload):
            self._order_queries.pop(key, None)
            ledger.clear_unknown(key)
            return payload, True
        if self.network_io is not None and any(self.network_io.contains((kind, symbol, client_order_id))
                for kind in ('entry', 'close')):
            # Queued is not submitted. Do NOT query a negative remote response
            # and free this identity before the original worker has sent it.
            return (payload, True) if payload else (None, False)
        if state and state.get('absence_check_after_ns'):
            # The read-only account worker must observe the database AFTER the
            # final negative exchange reply. A cached None is never enough.
            if payload and valid_evidence(payload):
                state.pop('absence_check_after_ns', None)
                state.pop('last_negative_epoch', None)
                state['not_found'] = 0
                return payload, True
            if not self._can_confirm_unacknowledged_absence(ledger, symbol=symbol,
                    client_order_id=client_order_id, order_id=order_id, now=now):
                state.pop('absence_check_after_ns', None)
                state['not_found'] = 0
            else:
                try:
                    latest = self.stream.lookup_order_after(after_ns=state['absence_check_after_ns'],
                        symbol=symbol, order_id=order_id, client_order_id=client_order_id)
                except StreamUnavailable:
                    return (payload, True) if payload else (None, False)
                if latest:
                    if not valid_evidence(latest):
                        return None, False
                    state['not_found'] = 0
                    state.pop('last_negative_epoch', None)
                    state.pop('absence_check_after_ns', None)
                    return latest, True
                self._order_queries.pop(key, None)
                ledger.clear_unknown(key)
                LOG.info('unacknowledged entry confirmed absent after fresh account barrier symbol=%s id=%s',
                         symbol, client_order_id)
                return {}, True
        # An old NEW/PARTIAL row is not proof that a MARKET close remains open.
        # Previously it cleared unknown_since forever and suppressed fallback.
        if state and state.get('confirmed_terminal'):
            cached = state['confirmed_terminal']
            if _finite(cached.get('executedQty')) >= _finite((payload or {}).get('executedQty')):
                return dict(cached), True
            # A later exact fill must never be replaced by an older cached
            # cancellation whose cumulative quantity was smaller.
            state.pop('confirmed_terminal', None)
        updated = _finite((payload or {}).get('updateTime')) / 1000.0
        seed = updated if 0 < updated <= now else now
        queue_due = self._queue_close_due_epoch(
            ledger, symbol=symbol, client_order_id=client_order_id, now=now)
        fast_since = queue_due if queue_due is not None else (self._fast_unknown_since(
            ledger, symbol=symbol, client_order_id=client_order_id,
            order_id=order_id, now=now) if not payload else None)
        if fast_since is not None:
            seed = fast_since
        started = ledger.unknown_since(key, now=seed)
        grace = (cfg.ORDER_UNKNOWN_FALLBACK_SECONDS if fast_since is not None
                 else cfg.ORDER_REST_FALLBACK_SECONDS)
        grace_start = fast_since if fast_since is not None else started
        if (payload and queue_due is None and updated > 0
                and -2 <= now - updated < cfg.ORDER_REST_FALLBACK_SECONDS):
            return payload, True
        if now - grace_start < grace or not self.enabled or self.dry_run:
            return (payload, True) if payload else (None, False)
        state = self._order_queries.setdefault(key, {
            'failures': 0, 'not_found': 0, 'next_query_epoch': 0.0,
            'last_warning_epoch': None, 'last_warning_kind': None})
        if queue_due is not None and not state.get('queue_urgent'):
            state['queue_urgent'] = True
            state['next_query_epoch'] = min(state['next_query_epoch'], now)
        if now < state['next_query_epoch']:
            cached = state.get('confirmed_snapshot')
            if cached and _finite(cached.get('executedQty')) >= _finite((payload or {}).get('executedQty')):
                return dict(cached), True
            return (payload, True) if payload else (None, False)
        try:
            if order_id not in (None, '', 0, '0'):
                result = self._broker_call('get_order', symbol, int(order_id))
            else:
                result = self._broker_call('get_order_by_client_order_id', symbol, client_order_id)
            if not isinstance(result, Mapping) or not result or not result.get('status'):
                raise RuntimeError('exact order query returned no order/status; ownership retained')
            response_symbol = _text(result.get('symbol')).upper()
            response_cid = _text(result.get('clientOrderId') or result.get('origClientOrderId'))
            response_oid = _text(result.get('orderId'))
            if ((response_symbol and response_symbol != symbol.upper())
                    or (response_cid and client_order_id and response_cid != client_order_id)
                    or (response_oid and order_id not in (None, '', 0, '0')
                        and response_oid != str(order_id))):
                raise RuntimeError('exact order query identity mismatch; ownership retained')
            if not valid_evidence(result):
                raise RuntimeError('exact order query returned stale/mismatched evidence; ownership retained')
            # Cache broker evidence across a missing terminal stream event.
            # Do not query again each cycle when the local row stays old NEW.
            state['failures'] = state['not_found'] = 0
            state['next_query_epoch'] = now + (ORDER_QUERY_RETRY_SECONDS
                if queue_due is not None else ORDER_QUERY_MAX_RETRY_SECONDS)
            state['confirmed_snapshot'] = dict(result)
            if complete_terminal(result):
                state['confirmed_terminal'] = dict(result)
                ledger.clear_unknown(key)
            return dict(result), True
        except NetworkPending:
            # Background scheduling is not a network failure: no warnings,
            # absence count or retry/backoff advancement until I/O completes.
            return (payload, True) if payload else (None, False)
        except Exception as exc:
            code = str(getattr(exc, 'code', ''))
            can_confirm = code == '-2013' and self._can_confirm_unacknowledged_absence(
                ledger, symbol=symbol, client_order_id=client_order_id,
                order_id=order_id, now=now)
            interval = (cfg.ORDER_UNKNOWN_QUERY_INTERVAL_SECONDS if fast_since is not None
                        else ORDER_QUERY_RETRY_SECONDS)
            if can_confirm:
                last_negative = state.get('last_negative_epoch')
                if last_negative is None or now - last_negative >= interval:
                    state['not_found'] += 1
                    state['last_negative_epoch'] = now
            else:
                state['not_found'] = 0
                state.pop('last_negative_epoch', None)
            settled = fast_since is None or now - fast_since >= cfg.ORDER_UNKNOWN_SETTLE_SECONDS
            if state['not_found'] >= ORDER_ABSENCE_CONFIRMATIONS and settled:
                # A local ACK/fill may have arrived DURING the network query.
                # Never retire the identity and retry before consuming that evidence.
                if callable(getattr(type(self.stream), 'lookup_order_after', None)):
                    state['absence_check_after_ns'] = time.perf_counter_ns()
                    try:
                        self.stream.lookup_order_after(after_ns=state['absence_check_after_ns'],
                            symbol=symbol, order_id=order_id, client_order_id=client_order_id)
                    except StreamUnavailable:
                        pass  # Start the read immediately; resolve on a later owner turn.
                    return (payload, True) if payload else (None, False)
                try:
                    latest = self.stream.lookup_order(
                        symbol=symbol, order_id=order_id, client_order_id=client_order_id)
                except StreamUnavailable:
                    return (payload, True) if payload else (None, False)
                if latest and valid_evidence(latest):
                    state['not_found'] = 0
                    state.pop('last_negative_epoch', None)
                    return latest, True
                self._order_queries.pop(key, None)
                ledger.clear_unknown(key)
                LOG.info('unacknowledged entry confirmed absent symbol=%s id=%s probes=%s; '
                         'release pending identity, not a fill/cancel',
                         symbol, client_order_id, state['not_found'])
                return {}, True
            state['failures'] += 1
            cap = (cfg.ORDER_UNKNOWN_MAX_RETRY_SECONDS if fast_since is not None
                   else ORDER_QUERY_MAX_RETRY_SECONDS)
            delay = min(cap, interval * 2 ** min(20, state['failures'] - 1))
            if can_confirm and fast_since is not None:
                # A valid negative query is not a network failure. Still allow a
                # minimum settle time and two distinct, spaced responses.
                delay = max(interval, cfg.ORDER_UNKNOWN_SETTLE_SECONDS - (now - fast_since))
            state['next_query_epoch'] = now + delay
            kind = (type(exc).__name__, code)
            last_warning = state['last_warning_epoch']
            if (last_warning is None or now - last_warning >= ORDER_QUERY_WARNING_SECONDS
                    or kind != state['last_warning_kind']):
                LOG.warning('own exact-order fallback failed symbol=%s id=%s code=%s '
                            'err=%s retry_in=%.1fs; ownership retained',
                            symbol, order_id or client_order_id, code or 'n/a', exc, delay)
                state['last_warning_epoch'] = now
                state['last_warning_kind'] = kind
            cached = state.get('confirmed_snapshot')
            if cached and _finite(cached.get('executedQty')) >= _finite((payload or {}).get('executedQty')):
                return dict(cached), True
            return (payload, True) if payload else (None, False)

    def exact_order(self, ledger: OwnershipLedger, *, symbol: str,
                     order_id: Any = None, client_order_id: str = '') -> dict[str, Any] | None:
        """Backward-compatible stream-only lookup."""
        payload, known = self.query_exact_order(
            ledger, symbol=symbol, order_id=order_id,
            client_order_id=client_order_id)
        return payload if known and payload else None

    def mark_price(self, symbol: str) -> float:
        """Use the latest local price only for displayed unrealized PnL."""
        try:
            payload = json.loads(self.price_file.read_text(encoding='utf-8'))
            row = (payload.get('prices') or {}).get(str(symbol).upper()) or {}
            return _finite(row.get('price'), float('nan'))
        except (OSError, ValueError, TypeError, AttributeError):
            return float('nan')


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def _text(value: Any) -> str:
    return str(value or '').strip()
