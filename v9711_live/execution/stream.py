"""Read-only local account-stream view for exact v9711 order identities.

Modeled on ``account_stream_view_0930``: this class never uses aggregate
positionRisk to infer ownership. Only intents registered by this executor are
looked up by exact client/order identity in the local SQLite/JSON stream.
"""
from __future__ import annotations

import json
import math
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .. import config as cfg


class StreamUnavailable(RuntimeError):
    """Local account stream cannot be trusted at this moment."""


def _text(value: Any) -> str:
    return str(value or '').strip()


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


@dataclass(frozen=True)
class Health:
    account_id: str
    age_seconds: float
    last_event_seq: int


class AccountStreamView:
    def __init__(self, directory: Path = cfg.ACCOUNT_STREAM_DIR, *,
                 account_id: str = cfg.ACCOUNT_ID,
                 max_age_seconds: float = cfg.ACCOUNT_STREAM_MAX_AGE_SECONDS):
        self.directory = Path(directory).resolve()
        self.account_id = _text(account_id) or 'default'
        self.max_age_seconds = max(1.0, float(max_age_seconds))
        self.snapshot_path = self.directory / 'account_snapshot.json'
        self.database_path = self.directory / 'events.sqlite3'
        self._lite_event_seq = 0
        self._lite_fills: dict[tuple[str, str, str], dict[str, dict[str, Any]]] = {}

    @property
    def fresh(self) -> bool:
        try:
            self.health()
            return True
        except StreamUnavailable:
            return False

    def health(self) -> Health:
        try:
            payload = json.loads(self.snapshot_path.read_text(encoding='utf-8'))
        except (OSError, ValueError, TypeError) as exc:
            raise StreamUnavailable('account snapshot missing or invalid') from exc
        status = payload.get('status') if isinstance(payload, Mapping) else None
        if not isinstance(status, Mapping):
            raise StreamUnavailable('account snapshot status missing')
        published = _finite(payload.get('published_at_ms'))
        age = time.time() - published / 1000.0
        if (str(status.get('mode', '')).lower() != 'live'
                or not bool(status.get('ready'))
                or not bool(status.get('connected'))
                or _text(status.get('account_id')) != self.account_id
                or published <= 0.0 or age < -2.0 or age > self.max_age_seconds):
            raise StreamUnavailable('account stream not live/ready/fresh')
        return Health(self.account_id, max(0.0, age),
                      int(_finite(payload.get('last_event_seq'))))

    def _connect(self) -> sqlite3.Connection:
        if not self.database_path.is_file():
            raise StreamUnavailable('account stream database missing')
        try:
            return sqlite3.connect(self.database_path.as_uri() + '?mode=ro',
                                   uri=True, timeout=2.0)
        except sqlite3.Error as exc:
            raise StreamUnavailable(f'account stream database unavailable: {exc}') from exc

    def forced_flat_evidence(self, *, symbol: str, since_ms: int,
                             owned_qty: float) -> dict[str, Any] | None:
        """Historical forced SELL + explicit subsequent account LONG zero.

        Zero/missing account positions alone never establish strategy ownership
        or prove a close. Only already-owned LONG positions use this evidence.
        Read canonical SQLite in one snapshot, including after executor restart.
        """
        self.health()
        if since_ms <= 0 or owned_qty <= 0:
            return None
        symbol = _text(symbol).upper()
        db = self._connect()
        try:
            db.execute('BEGIN')
            account = db.execute("SELECT value FROM meta WHERE key='account_id'").fetchone()
            if not account or json.loads(account[0]) != self.account_id:
                raise StreamUnavailable('forced-close account database identity mismatch')
            zero = db.execute('SELECT version_ms,payload FROM positions WHERE key=?',
                              (symbol + ':LONG',)).fetchone()
            fence_row = db.execute("SELECT value FROM meta WHERE key='snapshot_fence_ms'").fetchone()
            fence = int(_finite(json.loads(fence_row[0]))) if fence_row else 0
            if zero:
                position = json.loads(zero[1])
                if (_text(position.get('symbol')).upper() != symbol
                        or position.get('position_side') != 'LONG'
                        or _finite(position.get('quantity'), float('nan')) != 0):
                    return None
                flat_ms = int(zero[0])
                if position.get('source') == 'REST_BOOTSTRAP':
                    flat_ms = max(flat_ms, fence)
                flat_basis = 'explicit_account_LONG_zero'
            else:
                # A completed full REST bootstrap may omit flat symbols. This
                # is evidence only when paired with the exact forced orders.
                flat_ms, flat_basis = fence, 'subsequent_full_REST_snapshot_absence'
            if flat_ms < since_ms:
                return None
            orders = []
            for version, raw in db.execute(
                    'SELECT version_ms,payload FROM orders WHERE key>=? AND key<? '
                    'AND version_ms>=? ORDER BY version_ms ASC',
                    (symbol + ':', symbol + ';', since_ms)):
                row = json.loads(raw)
                cid = _text(row.get('client_order_id'))
                forced = (row.get('liquidation') is True or row.get('adl') is True
                          or row.get('execution_type') == 'CALCULATED'
                          or cid.startswith(('autoclose-', 'adl_autoclose')))
                quantity = _finite(row.get('executed_quantity'))
                price = _finite(row.get('average_price'))
                if price <= 0 and _finite(row.get('last_quantity')) == quantity:
                    price = _finite(row.get('last_price'))
                if (not forced or _text(row.get('symbol')).upper() != symbol
                        or row.get('position_side') != 'LONG' or row.get('order_side') != 'SELL'
                        or row.get('status') != 'FILLED' or quantity <= 0 or price <= 0
                        or not _text(row.get('order_id')) or int(version) > flat_ms):
                    continue
                orders.append(dict(order_id=_text(row['order_id']), client_order_id=cid,
                                   quantity=quantity, price=price, event_ms=int(version),
                                   exchange_realized_pnl=row.get('realized_pnl'),
                                   liquidation=not bool(row.get('adl')), adl=bool(row.get('adl'))))
            total = sum(row['quantity'] for row in orders)
            if not orders or total + max(1e-12, owned_qty * 1e-9) < owned_qty:
                return None  # partial or unrelated forced evidence cannot erase residual
            return dict(symbol=symbol, account_id=self.account_id, orders=orders,
                        total_forced_qty=total,
                        average_price=sum(row['quantity'] * row['price'] for row in orders) / total,
                        flat_version_ms=flat_ms, flat_basis=flat_basis)
        except (sqlite3.Error, ValueError, TypeError, KeyError) as exc:
            raise StreamUnavailable(f'forced-close evidence unavailable: {exc}') from exc
        finally:
            db.close()

    def lookup_order(self, *, symbol: str, order_id: Any = None,
                     client_order_id: str = '') -> dict[str, Any] | None:
        """Read exact cumulative order + deduplicated standard/LITE fills."""
        self.health()
        symbol = _text(symbol).upper()
        order_id_text = _text(order_id)
        client = _text(client_order_id)
        if not symbol or (not order_id_text and not client):
            return None
        db = self._connect()
        try:
            best: dict[str, Any] | None = None
            best_version = -1
            for version, raw in db.execute(
                    'SELECT version_ms,payload FROM orders ORDER BY version_ms DESC').fetchall():
                try:
                    row = json.loads(raw)
                except (TypeError, ValueError):
                    continue
                if not isinstance(row, Mapping) or _text(row.get('symbol')).upper() != symbol:
                    continue
                if order_id_text and _text(row.get('order_id')) != order_id_text:
                    continue
                if client and _text(row.get('client_order_id')) != client:
                    continue
                if int(_finite(version)) >= best_version:
                    best = dict(row)
                    best_version = int(_finite(version))
            fills = self._fills(db, symbol=symbol,
                                order_id=best.get('order_id') if best else order_id,
                                client_order_id=client)
            if best is None:
                if not fills:
                    return None
                # LITE contains last fill quantity, NOT original/cumulative qty.
                # Do not invent origQty/status from its q field or order absence.
                best = {'order_id': fills[0]['orderId'],
                        'client_order_id': fills[0].get('clientOrderId') or client,
                        'order_side': fills[0].get('side'), 'status': 'UNKNOWN'}
            fill_qty = sum(_finite(item.get('qty')) for item in fills)
            fill_quote = sum(_finite(item.get('quoteQty')) for item in fills)
            order_qty = _finite(best.get('executed_quantity'))
            order_avg = _finite(best.get('average_price'))
            # A retained fills table can be incomplete. Never shrink a more
            # complete cumulative order or price all its quantity at subset VWAP.
            if order_qty > fill_qty + max(1e-12, order_qty * 1e-9):
                qty, avg = order_qty, order_avg
            elif fill_qty > 0:
                qty, avg = fill_qty, fill_quote / fill_qty
            else:
                qty, avg = order_qty, order_avg
            original = _finite(best.get('original_quantity'))
            status = str(best.get('status') or 'UNKNOWN').upper()
            if original > 0 and qty >= original - max(1e-12, original * 1e-9):
                status = 'FILLED'
            elif qty > 0 and status in {'NEW', 'UNKNOWN', 'SUBMITTING', 'PENDING_NEW'}:
                status = 'PARTIALLY_FILLED'
            updated = max(int(_finite(best.get('updated_at_ms'))), best_version,
                          max((int(_finite(f.get('time'))) for f in fills), default=0))
            return {
                'symbol': symbol, 'orderId': best.get('order_id'),
                'clientOrderId': best.get('client_order_id', ''),
                'status': status, 'origQty': original, 'executedQty': qty,
                'avgPrice': avg,
                'price': _finite(best.get('price', best.get('limit_price'))),
                'side': str(best.get('order_side') or 'UNKNOWN').upper(),
                'positionSide': str(best.get('position_side') or 'UNKNOWN').upper(),
                'updateTime': updated, '_user_fills': fills, '_account_stream': True,
            }
        finally:
            db.close()

    def _refresh_lite_fills(self, db: sqlite3.Connection) -> None:
        """Incrementally read raw local TRADE_LITE events; never rewrite collector DB."""
        try:
            latest_seq = int(db.execute('SELECT COALESCE(MAX(seq),0) FROM events').fetchone()[0])
            if latest_seq < self._lite_event_seq:
                # Collector database rebuilt/truncated: don't reuse old rows.
                self._lite_event_seq = 0
                self._lite_fills.clear()
            events = db.execute(
                'SELECT seq,event_type,payload FROM events '
                'WHERE seq>? AND seq<=? AND event_type=? ORDER BY seq ASC',
                (self._lite_event_seq, latest_seq, 'TRADE_LITE')).fetchall()
        except sqlite3.OperationalError as exc:
            if 'no such table' in str(exc).lower():
                return  # compatibility with old local stream fixture/database
            raise StreamUnavailable(f'account stream events unavailable: {exc}') from exc
        self._lite_event_seq = latest_seq
        for seq, event_type, raw in events:
            if event_type != 'TRADE_LITE':
                continue
            try:
                row = json.loads(raw)
            except (TypeError, ValueError):
                continue
            if not isinstance(row, Mapping) or row.get('e') != 'TRADE_LITE':
                continue
            symbol, oid, cid = _text(row.get('s')).upper(), _text(row.get('i')), _text(row.get('c'))
            price, qty = _finite(row.get('L')), _finite(row.get('l'))
            trade_id = _text(row.get('t'))
            side = _text(row.get('S')).upper()
            # Prefix is only a cache filter; lookup still requires both exact
            # symbol/order/client matches, and ExchangeClient checks ownership.
            if (not symbol or oid in ('', '0') or not cid.startswith(cfg.ORDER_CLIENT_PREFIX + '_')
                    or price <= 0 or qty <= 0 or _finite(trade_id) <= 0
                    or side not in {'BUY', 'SELL'}):
                continue
            fills = self._lite_fills.setdefault((symbol, oid, cid), {})
            fills[trade_id] = {
                'symbol': symbol, 'orderId': row.get('i'), 'clientOrderId': cid,
                'price': price, 'qty': qty, 'quoteQty': price * qty,
                'commission': None, 'tradeId': row.get('t'), 'side': side,
                'time': int(_finite(row.get('T'), _finite(row.get('E')))),
                '_account_stream': True, '_trade_lite': True,
            }

    def _fills(self, db: sqlite3.Connection, *, symbol: str,
               order_id: Any, client_order_id: str = '') -> list[dict[str, Any]]:
        self._refresh_lite_fills(db)
        combined: dict[str, dict[str, Any]] = {}
        for (fill_symbol, fill_oid, cid), records in self._lite_fills.items():
            if (fill_symbol == symbol.upper()
                    and (order_id in (None, '', 0, '0') or fill_oid == _text(order_id))
                    and (not client_order_id or cid == client_order_id)):
                for trade_id, row in records.items():
                    combined[f'{fill_oid}:{trade_id}'] = dict(row)
        for transaction_ms, raw in db.execute(
                'SELECT transaction_ms,payload FROM fills ORDER BY transaction_ms ASC').fetchall():
            try:
                row = json.loads(raw)
            except (TypeError, ValueError):
                continue
            if not isinstance(row, Mapping):
                continue
            if (_text(row.get('symbol')).upper() != symbol.upper()
                    or (order_id not in (None, '', 0, '0') and _text(row.get('order_id')) != _text(order_id))
                    or (client_order_id and row.get('client_order_id')
                        and _text(row.get('client_order_id')) != client_order_id)
                    or (order_id in (None, '', 0, '0')
                        and _text(row.get('client_order_id')) != client_order_id)):
                continue
            price, quantity = _finite(row.get('last_price')), _finite(row.get('last_quantity'))
            if price <= 0 or quantity <= 0:
                continue
            oid, tid = _text(row.get('order_id')), _text(row.get('trade_id'))
            key = f'{oid}:{tid}' if tid not in ('', '0') else f'{oid}:{transaction_ms}:standard'
            # Standard TRADE has commission/position-side; prefer it over LITE
            # when both events refer to the exact same tradeId.
            combined[key] = {
                'symbol': symbol.upper(), 'orderId': row.get('order_id'),
                'clientOrderId': row.get('client_order_id'), 'price': price,
                'qty': quantity, 'quoteQty': price * quantity,
                'commission': _finite(row.get('commission')), 'tradeId': row.get('trade_id'),
                'side': _text(row.get('order_side')).upper(),
                'time': int(_finite(transaction_ms, _finite(row.get('updated_at_ms')))),
                '_account_stream': True,
            }
        return sorted(combined.values(), key=lambda f: (f['time'], _text(f.get('tradeId'))))

    def account_positions(self) -> dict[str, dict[str, Any]]:
        """Read aggregate positions only for display/over-capacity diagnostics."""
        self.health()
        db = self._connect()
        try:
            out: dict[str, dict[str, Any]] = {}
            for _, raw in db.execute(
                    'SELECT version_ms,payload FROM positions ORDER BY version_ms DESC').fetchall():
                try:
                    row = json.loads(raw)
                except (TypeError, ValueError):
                    continue
                if not isinstance(row, Mapping):
                    continue
                symbol = _text(row.get('symbol')).upper()
                side = _text(row.get('position_side')).upper()
                if not symbol:
                    continue
                key = f'{symbol}:{side or "BOTH"}'
                if key not in out:
                    out[key] = dict(row)
            return out
        finally:
            db.close()
