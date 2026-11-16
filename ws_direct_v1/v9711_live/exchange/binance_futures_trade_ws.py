#!/usr/bin/env python3
"""
WebSocket-first Binance USD-M Futures broker.

Trading/account requests are sent through Binance's WebSocket API first.
Transport recovery never replays uncertain order submissions. The executor
retains exact identities and reconciles them from account/query evidence. API keys
are loaded from .env or the environment:

  BINANCE_API_KEY=...
  BINANCE_API_SECRET=...

Supported through Binance USD-M Futures WebSocket API:
  balance, positions, info, orders, query, buy, sell, close, cancel

Unsupported by the official USD-M Futures WebSocket API:
  leverage, margin-type
"""

from __future__ import annotations

import argparse
import asyncio
from concurrent.futures import TimeoutError as FutureTimeoutError
import hashlib
import hmac
import json
import logging
import os
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request

from . import binance_network
from .trade_ws_lifecycle import TradeWSLifecycle


REST_BASE_URL = "https://fapi.binance.com"
WS_ENDPOINT = "wss://ws-fapi.binance.com/ws-fapi/v1"
WS_TESTNET_ENDPOINT = "wss://testnet.binancefuture.com/ws-fapi/v1"
USER_STREAM_WS_BASE = "wss://fstream.binance.com/private/ws"
RECV_WINDOW = 5000
# Tight timeout for small-payload WS / REST calls (positions, balance,
# place_order).  Trading prefers fast failover — 5s is well above Binance's
# usual <100ms response so it only triggers on genuine network issues.  The
# heavier exchangeInfo download (~1.5 MB) gets its own longer timeout in
# trade_runner.
REQUEST_TIMEOUT = 5
DEFAULT_TARGET_LEVERAGE = 2


def _load_dotenv(path: Path | None = None) -> None:
    deploy_root = Path(os.environ.get('V9711_DEPLOY_ROOT', str(Path(__file__).resolve().parents[2])))
    paths = [path] if path else [deploy_root / '.env', Path.cwd() / '.env']
    for env_file in paths:
        if not env_file or not env_file.exists():
            continue
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


_load_dotenv()


RST = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
GRN = "\033[32m"
RED = "\033[31m"
YEL = "\033[33m"
CYN = "\033[36m"


def c(text: Any, *codes: str) -> str:
    return "".join(codes) + str(text) + RST


def header(title: str) -> None:
    print(c(f"\n  -- {title} {'-' * max(0, 60 - len(title))}", BOLD, CYN))


def sep() -> None:
    print(c(f"  {'-' * 72}", DIM))


class AuthError(Exception):
    pass


class APIError(Exception):
    def __init__(self, code: int, msg: str):
        super().__init__(msg)
        self.code = code


class UnsupportedWebSocketAPI(Exception):
    pass


def _decimal_string(value: Any) -> str:
    if isinstance(value, Decimal):
        decimal_value = value
    else:
        try:
            decimal_value = Decimal(str(value))
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(f"Invalid decimal value: {value!r}") from exc
    return format(decimal_value.normalize(), "f")


def _bounded_client_order_id(value: Any) -> str:
    raw = str(value or "")
    if len(raw) < 36:
        return raw
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:4]
    return f"{raw[:30]}_{digest}"


def _normalize_param_value(value: Any) -> Any:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        return _decimal_string(value)
    return value


def _ws_sign(params: dict[str, Any], secret: str) -> str:
    signing_params = {k: _normalize_param_value(v) for k, v in params.items() if k != "signature"}
    # 必须用「原始值」拼接，不能用 urlencode：WS API 的参数是以 JSON 原样发送的，
    # 交易所端也按原始值参与签名。ASCII 符号下 urlencode 恰好是恒等变换，所以看不出
    # 问题；但符号名含非 ASCII（如 币安人生USDT）时 urlencode 会转成 %E5%B8%81...，
    # 而我们发出去的 JSON 里仍是原始字符，两边不一致 -> -1022 Signature is not valid.
    query = "&".join(f"{k}={v}" for k, v in sorted(signing_params.items()))
    return hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()


def _rest_signed_request(
    method: str,
    path: str,
    api_key: str,
    api_secret: str,
    params: dict[str, Any] | None = None,
    recv_window: int = RECV_WINDOW,
    request_timeout: int | None = None,
) -> dict[str, Any] | list[dict[str, Any]]:
    signed = {k: _normalize_param_value(v) for k, v in dict(params or {}).items() if v is not None}
    signed["timestamp"] = int(time.time() * 1000)
    signed["recvWindow"] = recv_window
    query = urlencode(signed)
    signed["signature"] = hmac.new(api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    signed_query = urlencode(signed)
    payload = None if method.upper() == "GET" else signed_query.encode()
    url = f"{REST_BASE_URL}{path}"
    if method.upper() == "GET":
        url = f"{url}?{signed_query}"

    req = Request(
        url,
        data=payload,
        method=method,
        headers={
            "X-MBX-APIKEY": api_key,
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    try:
        with binance_network.urlopen(req, timeout=request_timeout or REQUEST_TIMEOUT) as response:
            data = response.read()
            return json.loads(data) if data else {}
    except HTTPError as exc:
        try:
            data = json.loads(exc.read())
        except Exception:
            data = {"code": -1, "msg": str(exc)}
        raise APIError(int(data.get("code", -1)), str(data.get("msg", exc))) from exc
    except URLError as exc:
        raise ConnectionError(f"REST fallback network error: {exc.reason}") from exc


def mark_order_source(order: dict[str, Any] | None, source: str) -> dict[str, Any] | None:
    if order is None:
        return None
    if isinstance(order, dict):
        tagged = dict(order)
        tagged.setdefault("_source", source)
        return tagged
    return order


def _rest_position_rows(
    api_key: str,
    api_secret: str,
    *,
    recv_window: int,
    symbol: str | None = None,
    request_timeout: int | None = None,
) -> list[dict[str, Any]]:
    params = {"symbol": symbol.upper()} if symbol else {}
    result = _rest_signed_request(
        "GET",
        "/fapi/v2/positionRisk",
        api_key,
        api_secret,
        params,
        recv_window,
        request_timeout=request_timeout,
    )
    rows = result if isinstance(result, list) else [result]
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        tagged = dict(row)
        tagged.setdefault("_source", "REST")
        out.append(tagged)
    return out


def _unique_symbols(symbols: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in symbols:
        symbol = str(raw).strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    return out


def _parse_symbol_args(values: list[str] | None) -> list[str]:
    symbols: list[str] = []
    for value in values or []:
        symbols.extend(part.strip().upper() for part in str(value).split(",") if part.strip())
    return _unique_symbols(symbols)


def _leverage_arg(value: str) -> int:
    try:
        leverage = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("leverage must be an integer") from exc
    if leverage < 1 or leverage > 125:
        raise argparse.ArgumentTypeError("leverage must be between 1 and 125")
    return leverage


def _rest_place_market_order(
    api_key: str,
    api_secret: str,
    *,
    recv_window: int,
    symbol: str,
    side: str,
    quantity: float | Decimal | str,
    reduce_only: bool = False,
    position_side: str | None = None,
    client_order_id: str | None = None,
    request_timeout: int | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "symbol": symbol.upper(),
        "side": side.upper(),
        "type": "MARKET",
        "quantity": _decimal_string(quantity),
    }
    if position_side:
        params["positionSide"] = position_side.upper()
    if reduce_only and params.get("positionSide", "BOTH") == "BOTH":
        params["reduceOnly"] = "true"
    if client_order_id:
        params["newClientOrderId"] = _bounded_client_order_id(client_order_id)
    result = _rest_signed_request(
        "POST",
        "/fapi/v1/order",
        api_key,
        api_secret,
        params,
        recv_window,
        request_timeout=request_timeout,
    )
    return mark_order_source(result if isinstance(result, dict) else {}, "REST") or {}


def _ts(ms: Any) -> str:
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).strftime("%m-%d %H:%M:%S")
    except Exception:
        return "--"


def rest_place_order_batch(
    api_key: str,
    api_secret: str,
    orders: list[dict[str, Any]],
    *,
    recv_window: int,
    request_timeout: int,
) -> list[dict[str, Any]]:
    """Submit 1..5 independent LIMIT/MARKET orders through REST batchOrders.

    This is deliberately local to the standalone deployment and does not import
    any other strategy. Binance returns one response object per request member;
    the caller attributes every response by client order id.
    """
    if not orders:
        return []
    if len(orders) > 5:
        raise ValueError(f"batchOrders accepts at most 5 orders, got {len(orders)}")
    batch: list[dict[str, Any]] = []
    for order in orders:
        params: dict[str, Any] = {
            "symbol": str(order["symbol"]).upper(),
            "side": str(order["side"]).upper(),
            "type": str(order.get("type") or "MARKET").upper(),
            "quantity": str(order["quantity"]),
        }
        for source, target in (("timeInForce", "timeInForce"),
                               ("priceMatch", "priceMatch"),
                               ("positionSide", "positionSide")):
            value = order.get(source) or order.get(source[0].lower() + source[1:])
            if value:
                params[target] = str(value).upper()
        price = order.get("price")
        if price is not None and "priceMatch" not in params:
            params["price"] = str(price)
        reduce_only = bool(order.get("reduceOnly") or order.get("reduce_only"))
        if reduce_only and params.get("positionSide", "BOTH") == "BOTH":
            params["reduceOnly"] = "true"
        client_order_id = order.get("newClientOrderId") or order.get("client_order_id")
        if client_order_id:
            params["newClientOrderId"] = str(client_order_id)
        batch.append(params)
    result = _rest_signed_request(
        "POST", "/fapi/v1/batchOrders", api_key, api_secret,
        {"batchOrders": json.dumps(batch, separators=(",", ":"))},
        recv_window, request_timeout=request_timeout,
    )
    if not isinstance(result, list):
        raise RuntimeError(f"unexpected batchOrders response: {result!r}")
    return [row if isinstance(row, dict) else {"code": -1, "msg": str(row)}
            for row in result]


class BinanceFuturesTradeWS(TradeWSLifecycle):
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        dry_run: bool = False,
        ws_url: str = WS_ENDPOINT,
        recv_window: int = RECV_WINDOW,
        request_timeout: int = REQUEST_TIMEOUT,
        ssl_verify: bool = True,
    ) -> None:
        if not api_key or not api_secret:
            raise AuthError("API key and secret are required.")
        self.api_key = api_key
        self.api_secret = api_secret
        self.dry_run = dry_run
        self.ws_url = ws_url
        self.recv_window = recv_window
        self.request_timeout = request_timeout
        self.ssl_verify = ssl_verify
        self._lock = asyncio.Lock()
        self._init_lifecycle()

    async def __aenter__(self) -> "BinanceFuturesTradeWS":
        await self.connect()
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()

    async def _request(self, method, params, *, signed, before_send=None):
        async with self._lock:
            await self._ensure_connected()
            # Revalidate AFTER a reconnect/lock wait and BEFORE signing/sending.
            # An expired or withdrawn plan must not be sent from the old queue.
            if before_send is not None:
                before_send()
            request_params = {k:_normalize_param_value(v) for k,v in dict(params or {}).items() if v is not None}
            request_params.setdefault('apiKey', self.api_key)
            if signed:
                request_params['apiKey'] = self.api_key
                request_params['timestamp'] = int(time.time()*1000)
                request_params['recvWindow'] = self.recv_window
                request_params['signature'] = _ws_sign(request_params, self.api_secret)
            request_id = str(uuid.uuid4())
            payload = dict(id=request_id, method=method, params=request_params)
            try:
                await self._ws.send(json.dumps(payload, separators=(',', ':')))
                response = await asyncio.wait_for(self._read_response(request_id), timeout=self.request_timeout)
            except (Exception, asyncio.CancelledError):
                # Sending may already have succeeded. Recover the CONNECTION
                # only; exact intent/query/stream reconciliation resolves fate.
                await self._invalidate_locked()
                raise
        status = int(response.get('status', 0))
        if status not in (200, 201, 204):
            error = response.get('error') or {}
            raise APIError(int(error.get('code', -1)), str(error.get('msg', f'WS status={status}')))
        return response.get('result', {})

    async def _call(self, method: str, params: dict[str, Any] | None = None, *, before_send=None) -> Any:
        return await self._request(method, params, signed=True, before_send=before_send)

    async def _call_api_key_only(self, method: str, params: dict[str, Any] | None = None) -> Any:
        return await self._request(method, params, signed=False)

    async def _read_response(self, request_id: str) -> dict[str, Any]:
        while True:
            raw = await self._ws.recv()
            data = json.loads(raw)
            if data.get("id") == request_id:
                return data
            if data.get("id") is None and data.get("status") == 401:
                error = data.get("error") or {}
                raise APIError(int(error.get("code", -1)), str(error.get("msg", "API key revoked")))

    async def get_balance(self) -> list[dict[str, Any]]:
        rows = await self._call("account.balance", {})
        if not isinstance(rows, list):
            return []
        return [
            row
            for row in rows
            if float(row.get("balance", 0)) != 0 or float(row.get("crossUnPnl", 0)) != 0
        ]

    async def get_account_status(self) -> dict[str, Any]:
        result = await self._call("v2/account.status", {})
        return result if isinstance(result, dict) else {}

    async def get_position_rows(self, symbol: str | None = None) -> list[dict[str, Any]]:
        params = {"symbol": symbol.upper()} if symbol else {}
        rows = await self._call("v2/account.position", params)
        return rows if isinstance(rows, list) else []

    async def get_positions(self) -> list[dict[str, Any]]:
        rows = await self.get_position_rows()
        return [row for row in rows if float(row.get("positionAmt", 0)) != 0]

    async def get_position_info(self, symbol: str) -> dict[str, Any]:
        rows = await self.get_position_rows(symbol)
        non_zero = [row for row in rows if float(row.get("positionAmt", 0)) != 0]
        return (non_zero or rows or [{}])[0]

    async def hedge_mode(self, symbol: str | None = None) -> bool:
        rows = await self.get_position_rows(symbol)
        sides = {str(row.get("positionSide", "")).upper() for row in rows}
        if "LONG" in sides or "SHORT" in sides:
            return True
        return False

    async def get_open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        params = {"symbol": symbol.upper()} if symbol else {}
        result = await self._call("openOrders.status", params)
        return result if isinstance(result, list) else []

    async def start_user_data_stream(self) -> str:
        result = await self._call_api_key_only("userDataStream.start", {})
        listen_key = result.get("listenKey") if isinstance(result, dict) else None
        if not listen_key:
            raise APIError(-1, "userDataStream.start did not return listenKey")
        return str(listen_key)

    async def keepalive_user_data_stream(self) -> str:
        result = await self._call_api_key_only("userDataStream.ping", {})
        listen_key = result.get("listenKey") if isinstance(result, dict) else None
        return str(listen_key or "")

    async def stop_user_data_stream(self) -> None:
        await self._call_api_key_only("userDataStream.stop", {})

    async def get_order(self, symbol: str, order_id: int) -> dict[str, Any]:
        result = await self._call("order.status", {"symbol": symbol.upper(), "orderId": order_id})
        return result if isinstance(result, dict) else {}

    async def get_order_by_client_order_id(self, symbol: str, client_order_id: str) -> dict[str, Any]:
        result = await self._call(
            "order.status",
            {"symbol": symbol.upper(), "origClientOrderId": str(client_order_id)},
        )
        return result if isinstance(result, dict) else {}

    async def set_leverage(self, symbol: str, leverage: int) -> dict[str, Any]:
        if self.dry_run:
            print(c(f"  [DRY-RUN/REST] POST /fapi/v1/leverage symbol={symbol.upper()} leverage={leverage}", YEL))
            return {"_source": "REST"}
        result = _rest_signed_request(
            "POST",
            "/fapi/v1/leverage",
            self.api_key,
            self.api_secret,
            {"symbol": symbol.upper(), "leverage": leverage},
            self.recv_window,
        )
        return mark_order_source(result if isinstance(result, dict) else {}, "REST")

    async def set_margin_type(self, symbol: str, margin_type: str) -> dict[str, Any]:
        if self.dry_run:
            print(c(f"  [DRY-RUN/REST] POST /fapi/v1/marginType symbol={symbol.upper()} marginType={margin_type.upper()}", YEL))
            return {"_source": "REST"}
        result = _rest_signed_request(
            "POST",
            "/fapi/v1/marginType",
            self.api_key,
            self.api_secret,
            {"symbol": symbol.upper(), "marginType": margin_type.upper()},
            self.recv_window,
        )
        return mark_order_source(result if isinstance(result, dict) else {}, "REST")

    async def place_order(
        self,
        symbol: str,
        side: str,
        quantity: float | Decimal | str,
        price: float | Decimal | str | None = None,
        reduce_only: bool = False,
        position_side: str | None = None,
        price_match: str | None = None,
        client_order_id: str | None = None,
        before_send: Any = None,
    ) -> dict[str, Any]:
        """下单。price 与 price_match 互斥。

        price_match（如 "QUEUE" / "OPPONENT"）交给交易所按盘口定价，用于
        挂单不吃单的场景；此时不接受显式 price，type 仍是 LIMIT + GTC。
        """
        symbol = symbol.upper()
        side = side.upper()
        price_match = str(price_match).strip().upper() if price_match else None
        if price_match and price is not None:
            raise ValueError("price and price_match are mutually exclusive")
        order_type = "LIMIT" if (price is not None or price_match) else "MARKET"
        if position_side is None:
            position_side = "LONG" if (await self.hedge_mode(symbol)) and side == "BUY" else None
            if position_side is None and await self.hedge_mode(symbol):
                position_side = "SHORT"
            if position_side is None:
                position_side = "BOTH"

        params: dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "quantity": _decimal_string(quantity),
            "positionSide": position_side.upper(),
        }
        if price_match:
            params["priceMatch"] = price_match
            params["timeInForce"] = "GTC"
        elif price is not None:
            params["price"] = _decimal_string(price)
            params["timeInForce"] = "GTC"
        if reduce_only and params["positionSide"] == "BOTH":
            params["reduceOnly"] = "true"
        if client_order_id:
            params["newClientOrderId"] = _bounded_client_order_id(client_order_id)

        if self.dry_run:
            print(c(f"  [DRY-RUN/WS] order.place params={params}", YEL))
            return {"_source": "WS"}
        result = await self._call("order.place", params, before_send=before_send)
        return mark_order_source(result if isinstance(result, dict) else {}, "WS")

    async def place_stop_market_order(
        self,
        symbol: str,
        side: str,
        quantity: float | Decimal | str,
        stop_price: float | Decimal | str,
        order_type: str = "STOP_MARKET",
        reduce_only: bool = False,
        position_side: str | None = None,
    ) -> dict[str, Any]:
        symbol = symbol.upper()
        side = side.upper()
        if position_side is None:
            position_side = "LONG" if await self.hedge_mode(symbol) else "BOTH"

        params: dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "type": order_type.upper(),
            "stopPrice": _decimal_string(stop_price),
            "quantity": _decimal_string(quantity),
            "positionSide": position_side.upper(),
        }
        if reduce_only and params["positionSide"] == "BOTH":
            params["reduceOnly"] = "true"

        if self.dry_run:
            print(c(f"  [DRY-RUN/WS] algoOrder.place params={params}", YEL))
            return {"_source": "WS", "algoType": order_type.upper()}
        algo_params = {
            "symbol": symbol,
            "side": side,
            "quantity": _decimal_string(quantity),
            "positionSide": position_side.upper(),
            "algoType": "CONDITIONAL",
            "type": order_type.upper(),
            "triggerPrice": _decimal_string(stop_price),
        }
        if reduce_only and algo_params["positionSide"] == "BOTH":
            algo_params["reduceOnly"] = "true"
        result = await self._call("algoOrder.place", algo_params)
        return mark_order_source(result if isinstance(result, dict) else {}, "WS")

    async def cancel_order(self, symbol: str, order_id: int) -> dict[str, Any]:
        params = {"symbol": symbol.upper(), "orderId": order_id}
        if self.dry_run:
            print(c(f"  [DRY-RUN/WS] order.cancel params={params}", YEL))
            return {"_source": "WS"}
        result = await self._call("order.cancel", params)
        return mark_order_source(result if isinstance(result, dict) else {}, "WS")

    async def cancel_all_orders(self, symbol: str) -> dict[str, Any]:
        symbol = symbol.upper()
        orders = await self.get_open_orders(symbol)
        canceled: list[dict[str, Any]] = []
        for order in orders:
            order_id = int(order["orderId"])
            canceled.append(await self.cancel_order(symbol, order_id))
        return {"symbol": symbol, "count": len(canceled), "canceled": canceled, "_source": "WS"}


class BinanceWSBroker:
    """
    Synchronous compatibility wrapper for existing executor/broker.py callers.

    The public methods mirror broker.py. Trading/account requests are sent
    through WebSocket API; leverage and margin type use REST because Binance
    does not expose those setters through USD-M Futures WebSocket API.
    """

    CONNECT_TIMEOUT = 5
    MANAGES_WS_LIFECYCLE = True

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        dry_run: bool = False,
        ssl_verify: bool = True,
        ws_url: str = WS_ENDPOINT,
        request_timeout: int = REQUEST_TIMEOUT,
        recv_window: int = RECV_WINDOW,
    ) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.dry_run = dry_run
        self.ws_url = ws_url
        self.request_timeout = request_timeout
        self.recv_window = recv_window
        self._ssl_verify = ssl_verify
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="BinanceWSBrokerLoop")
        self._thread.start()
        try:
            self._broker: BinanceFuturesTradeWS = self._run(self._create_broker(), timeout=self.CONNECT_TIMEOUT)
        except Exception:
            if self._loop.is_running():
                self._loop.call_soon_threadsafe(self._loop.stop)
            if self._thread.is_alive():
                self._thread.join(timeout=5)
            raise
        self._hedge_mode: bool | None = None

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_forever()
        finally:
            # Cancel the keepalive supervisor and any timed-out request task
            # before releasing the loop/thread; no zombie coroutine may send.
            pending = asyncio.all_tasks(self._loop)
            for task in pending:
                task.cancel()
            if pending:
                self._loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            self._loop.close()

    async def _create_broker(self) -> BinanceFuturesTradeWS:
        broker = BinanceFuturesTradeWS(
            self.api_key,
            self.api_secret,
            dry_run=self.dry_run,
            ws_url=self.ws_url,
            recv_window=self.recv_window,
            request_timeout=self.request_timeout,
            ssl_verify=self._ssl_verify,
        )
        await broker.connect()
        return broker

    def _run(self, coro: Any, timeout: int | None = None) -> Any:
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            return future.result(timeout=timeout or self.request_timeout + 10)
        except FutureTimeoutError:
            future.cancel()  # Never allow a timed-out queued coroutine to send later.
            raise

    def close(self) -> None:
        try:
            if getattr(self, "_broker", None) is not None and self._loop.is_running():
                self._run(self._broker.close(), timeout=5)
        except Exception:
            pass
        if self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread.is_alive():
            self._thread.join(timeout=5)

    def reconnect(self) -> None:
        """Reconnect the underlying WS session without recreating this wrapper."""
        if not self._loop.is_running():
            raise ConnectionError("broker event loop is not running")
        self._run(self._broker.connect(), timeout=self.CONNECT_TIMEOUT)

    def place_order_checked(self, *args, before_send=None, **kwargs):
        """Exact submission ONCE; no transport-error REST replay."""
        return self._run(self._broker.place_order(*args, before_send=before_send, **kwargs))

    @property
    def hedge_mode(self) -> bool:
        if self._hedge_mode is None:
            self._hedge_mode = bool(self._run(self._broker.hedge_mode()))
        return self._hedge_mode

    def get_balance(self) -> list[dict[str, Any]]:
        return self._run(self._broker.get_balance())

    def get_positions(self) -> list[dict[str, Any]]:
        try:
            return self._run(self._broker.get_positions())
        except Exception as exc:
            logging.getLogger(__name__).warning("WS get_positions failed; using REST fallback: %s", exc)
            rows = _rest_position_rows(
                self.api_key,
                self.api_secret,
                recv_window=self.recv_window,
                request_timeout=self.request_timeout,
            )
            return [row for row in rows if float(row.get("positionAmt", 0)) != 0]

    def get_position_info(self, symbol: str) -> dict[str, Any]:
        try:
            return self._run(self._broker.get_position_info(symbol))
        except Exception as exc:
            logging.getLogger(__name__).warning("WS get_position_info failed; using REST fallback: %s", exc)
            rows = _rest_position_rows(
                self.api_key,
                self.api_secret,
                recv_window=self.recv_window,
                symbol=symbol,
                request_timeout=self.request_timeout,
            )
            non_zero = [row for row in rows if float(row.get("positionAmt", 0)) != 0]
            return (non_zero or rows or [{}])[0]

    def get_open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        return self._run(self._broker.get_open_orders(symbol))

    def start_user_data_stream(self) -> str:
        return self._run(self._broker.start_user_data_stream())

    def keepalive_user_data_stream(self) -> str:
        return self._run(self._broker.keepalive_user_data_stream())

    def stop_user_data_stream(self) -> None:
        self._run(self._broker.stop_user_data_stream())

    def get_order(self, symbol: str, order_id: int) -> dict[str, Any]:
        return self._run(self._broker.get_order(symbol, order_id))

    def get_order_by_client_order_id(self, symbol: str, client_order_id: str) -> dict[str, Any]:
        return self._run(self._broker.get_order_by_client_order_id(symbol, client_order_id))

    def set_leverage(self, symbol: str, leverage: int) -> dict[str, Any]:
        return self._run(self._broker.set_leverage(symbol, leverage))

    def set_margin_type(self, symbol: str, margin_type: str) -> dict[str, Any]:
        return self._run(self._broker.set_margin_type(symbol, margin_type))

    def place_order(
        self,
        symbol: str,
        side: str,
        quantity: float | Decimal | str,
        price: float | Decimal | str | None = None,
        reduce_only: bool = False,
        position_side: str | None = None,
        price_match: str | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        # A lost ACK may mean FILLED. Never blindly replay MARKET SELL via REST.
        return self.place_order_checked(symbol, side, quantity, price=price,
            reduce_only=reduce_only, position_side=position_side,
            price_match=price_match, client_order_id=client_order_id)

    def place_stop_market_order(
        self,
        symbol: str,
        side: str,
        quantity: float | Decimal | str,
        stop_price: float | Decimal | str,
        order_type: str = "STOP_MARKET",
        reduce_only: bool = False,
        position_side: str | None = None,
    ) -> dict[str, Any]:
        return self._run(
            self._broker.place_stop_market_order(
                symbol,
                side,
                quantity,
                stop_price,
                order_type=order_type,
                reduce_only=reduce_only,
                position_side=position_side,
            )
        )

    def cancel_order(self, symbol: str, order_id: int) -> dict[str, Any]:
        return self._run(self._broker.cancel_order(symbol, order_id))

    def cancel_all_orders(self, symbol: str) -> dict[str, Any]:
        return self._run(self._broker.cancel_all_orders(symbol))


BinanceBroker = BinanceWSBroker


def _position_side(row: dict[str, Any]) -> str:
    side = str(row.get("positionSide", "")).upper()
    if side in {"LONG", "SHORT"}:
        return side
    amount = float(row.get("positionAmt", 0))
    return "LONG" if amount > 0 else "SHORT"


def show_balance(rows: list[dict[str, Any]]) -> None:
    header("账户余额")
    print(c(f"  {'资产':<10}  {'余额':>16}  {'可用保证金':>16}  {'未实现盈亏':>16}  {'最大转出':>16}", DIM))
    sep()
    for row in rows:
        balance = float(row.get("balance", 0))
        available = float(row.get("availableBalance", 0))
        upnl = float(row.get("crossUnPnl", 0))
        withdraw = float(row.get("maxWithdrawAmount", row.get("withdrawAvailable", available)))
        upnl_color = GRN if upnl >= 0 else RED
        print(
            f"  {row.get('asset', ''):<10}  {balance:>16.4f}  {available:>16.4f}  "
            f"{c(f'{upnl:>+16.4f}', upnl_color)}  {withdraw:>16.4f}"
        )
    print()


def show_position_info(row: dict[str, Any]) -> None:
    if not row:
        print(c("  未找到该币对信息", YEL))
        print()
        return
    symbol = row.get("symbol", "")
    margin_type = row.get("marginType", "-")
    leverage = row.get("leverage", "-")
    amount = float(row.get("positionAmt", 0))
    entry = float(row.get("entryPrice", 0))
    mark = float(row.get("markPrice", 0))
    upnl = float(row.get("unRealizedProfit", 0))
    liq = float(row.get("liquidationPrice", 0))
    header(f"持仓设置  {symbol}")
    print(f"  仓位方向    {row.get('positionSide', '-')}")
    print(f"  保证金模式  {margin_type}")
    print(f"  杠杆倍数    {leverage}x")
    if amount != 0:
        upnl_color = GRN if upnl >= 0 else RED
        print(f"  当前持仓    {_position_side(row)}  {abs(amount):.6f}")
        print(f"  开仓均价    {entry:.4f}")
        print(f"  标记价格    {mark:.4f}")
        print(f"  强平价格    {liq:.4f}")
        print(f"  未实现盈亏  {c(f'{upnl:+.4f}', upnl_color)}")
    else:
        print("  当前持仓    （空仓）")
    print()


def show_positions(rows: list[dict[str, Any]]) -> None:
    header("当前持仓")
    if not rows:
        print(c("  （无持仓）", DIM))
        print()
        return
    print(c(
        f"  {'币对':<16}  {'方向':<6}  {'数量':>12}  {'开仓均价':>12}  "
        f"{'标记价格':>12}  {'未实现盈亏':>14}  {'杠杆':>4}  {'保证金模式':<10}",
        DIM,
    ))
    sep()
    for row in rows:
        amount = float(row.get("positionAmt", 0))
        entry = float(row.get("entryPrice", 0))
        mark = float(row.get("markPrice", 0))
        upnl = float(row.get("unRealizedProfit", 0))
        side = _position_side(row)
        side_color = GRN if side == "LONG" else RED
        upnl_color = GRN if upnl >= 0 else RED
        print(
            f"  {row.get('symbol', ''):<16}  {c(f'{side:<6}', side_color)}  {abs(amount):>12.6f}  "
            f"{entry:>12.4f}  {mark:>12.4f}  {c(f'{upnl:>+14.4f}', upnl_color)}  "
            f"{row.get('leverage', '-'):>4}x  {row.get('marginType', '-'):<10}"
        )
    print()


def show_orders(rows: list[dict[str, Any]]) -> None:
    header("挂单列表")
    if not rows:
        print(c("  （无挂单）", DIM))
        print()
        return
    print(c(
        f"  {'OrderID':>12}  {'币对':<16}  {'方向':<6}  {'类型':<14}  "
        f"{'数量':>12}  {'价格':>12}  {'已成交':>10}  {'状态':<14}  {'时间'}",
        DIM,
    ))
    sep()
    for row in rows:
        side = str(row.get("side", ""))
        side_color = GRN if side == "BUY" else RED
        print(
            f"  {row.get('orderId', ''):>12}  {row.get('symbol', ''):<16}  {c(f'{side:<6}', side_color)}  "
            f"{row.get('type', ''):<14}  {float(row.get('origQty', 0)):>12.6f}  "
            f"{float(row.get('price', 0)):>12.4f}  {float(row.get('executedQty', 0)):>10.6f}  "
            f"{row.get('status', ''):<14}  {_ts(row.get('time') or row.get('updateTime'))}"
        )
    print()


def show_order(row: dict[str, Any]) -> None:
    header(f"订单详情  {row.get('symbol', '')}  #{row.get('orderId', '')}")
    fields = [
        ("订单ID", row.get("orderId")),
        ("币对", row.get("symbol")),
        ("方向", row.get("side")),
        ("仓位方向", row.get("positionSide")),
        ("类型", row.get("type")),
        ("数量", row.get("origQty")),
        ("价格", row.get("price")),
        ("已成交", row.get("executedQty")),
        ("均价", row.get("avgPrice")),
        ("状态", row.get("status")),
        ("时间", _ts(row["time"]) if row.get("time") else "-"),
        ("更新时间", _ts(row["updateTime"]) if row.get("updateTime") else "-"),
    ]
    for key, value in fields:
        print(f"  {key:<10}  {value}")
    print()


def show_placed_order(row: dict[str, Any]) -> None:
    if not row:
        return
    side = row.get("side", "")
    side_color = GRN if side == "BUY" else RED
    print(c(
        f"\n  OK 下单成功  #{row.get('orderId')}  "
        f"{row.get('symbol')}  {side}  "
        f"qty={row.get('origQty')}  "
        f"price={row.get('price') or 'MARKET'}  "
        f"status={row.get('status')}",
        side_color,
        BOLD,
    ))
    print()


def confirm(prompt: str, yes: bool) -> bool:
    if yes:
        return True
    answer = input(f"  {prompt} [y/N] ").strip().lower()
    return answer in {"y", "yes"}


async def cmd_balance(broker: BinanceFuturesTradeWS, _args: argparse.Namespace) -> None:
    show_balance(await broker.get_balance())


async def cmd_positions(broker: BinanceFuturesTradeWS, _args: argparse.Namespace) -> None:
    show_positions(await broker.get_positions())


async def cmd_info(broker: BinanceFuturesTradeWS, args: argparse.Namespace) -> None:
    show_position_info(await broker.get_position_info(args.symbol))


async def cmd_orders(broker: BinanceFuturesTradeWS, args: argparse.Namespace) -> None:
    show_orders(await broker.get_open_orders(getattr(args, "symbol", None)))


async def cmd_query(broker: BinanceFuturesTradeWS, args: argparse.Namespace) -> None:
    show_order(await broker.get_order(args.symbol, args.order_id))


async def cmd_buy(broker: BinanceFuturesTradeWS, args: argparse.Namespace) -> None:
    price = getattr(args, "price", None)
    desc = f"市价买入 {args.qty} {args.symbol}" if price is None else f"限价买入 {args.qty} {args.symbol} @ {price}"
    print(c(f"\n  -> {desc}", YEL))
    if not confirm("确认下单?", args.yes):
        print("  已取消")
        return
    if broker.dry_run:
        position_side = args.position_side or "AUTO"
        order_type = "LIMIT" if price is not None else "MARKET"
        params = {
            "symbol": args.symbol.upper(),
            "side": "BUY",
            "type": order_type,
            "quantity": args.qty,
            "positionSide": position_side,
        }
        if price is not None:
            params["price"] = price
            params["timeInForce"] = "GTC"
        print(c(f"  [DRY-RUN/WS] order.place params={params}", YEL))
        return
    result = await broker.place_order(args.symbol, "BUY", args.qty, price, position_side=args.position_side)
    show_placed_order(result)


async def cmd_sell(broker: BinanceFuturesTradeWS, args: argparse.Namespace) -> None:
    price = getattr(args, "price", None)
    desc = f"市价卖出 {args.qty} {args.symbol}" if price is None else f"限价卖出 {args.qty} {args.symbol} @ {price}"
    print(c(f"\n  -> {desc}", YEL))
    if not confirm("确认下单?", args.yes):
        print("  已取消")
        return
    if broker.dry_run:
        position_side = args.position_side or "AUTO"
        order_type = "LIMIT" if price is not None else "MARKET"
        params = {
            "symbol": args.symbol.upper(),
            "side": "SELL",
            "type": order_type,
            "quantity": args.qty,
            "positionSide": position_side,
        }
        if price is not None:
            params["price"] = price
            params["timeInForce"] = "GTC"
        print(c(f"  [DRY-RUN/WS] order.place params={params}", YEL))
        return
    result = await broker.place_order(args.symbol, "SELL", args.qty, price, position_side=args.position_side)
    show_placed_order(result)


async def cmd_close(broker: BinanceFuturesTradeWS, args: argparse.Namespace) -> None:
    symbol = args.symbol.upper()
    positions = [row for row in await broker.get_position_rows(symbol) if float(row.get("positionAmt", 0)) != 0]
    if not positions:
        print(c(f"\n  {symbol} 无持仓\n", YEL))
        return

    if args.position_side:
        target_side = args.position_side.upper()
        pos = next((row for row in positions if str(row.get("positionSide", "")).upper() == target_side), None)
        if pos is None:
            print(c(f"\n  {symbol} 没有 {target_side} 持仓\n", YEL))
            return
    else:
        pos = positions[0]

    amount = float(pos.get("positionAmt", 0))
    pos_side = str(pos.get("positionSide", "BOTH")).upper()
    if pos_side == "LONG":
        side = "SELL"
    elif pos_side == "SHORT":
        side = "BUY"
    else:
        side = "SELL" if amount > 0 else "BUY"
    qty = args.qty or abs(amount)
    upnl = float(pos.get("unRealizedProfit", 0))
    upnl_color = GRN if upnl >= 0 else RED

    close_position_side = pos_side if pos_side in {"LONG", "SHORT"} else None
    reduce_only = close_position_side is None

    print(c(
        f"\n  -> 市价平仓 {symbol}  {_position_side(pos)}  数量={qty}  "
        f"开仓价={float(pos.get('entryPrice', 0))}  标记价={float(pos.get('markPrice', 0))}  "
        f"未实现={c(f'{upnl:+.4f}', upnl_color)}",
        YEL,
    ))
    if not confirm("确认平仓?", args.yes):
        print("  已取消")
        return
    result = await broker.place_order(symbol, side, qty, reduce_only=reduce_only, position_side=close_position_side)
    show_placed_order(result)


async def cmd_cancel(broker: BinanceFuturesTradeWS, args: argparse.Namespace) -> None:
    symbol = args.symbol.upper()
    if args.all:
        print(c(f"\n  -> 撤销 {symbol} 全部挂单", YEL))
        if not confirm("确认撤销?", args.yes):
            print("  已取消")
            return
        result = await broker.cancel_all_orders(symbol)
        if not broker.dry_run:
            print(c(f"  OK 已撤销 {result.get('count', 0)} 个挂单\n", GRN))
        return

    if not args.order_id:
        print(c("  需要 --order-id 或 --all", RED))
        return
    print(c(f"\n  -> 撤单 {symbol} #{args.order_id}", YEL))
    if not confirm("确认撤单?", args.yes):
        print("  已取消")
        return
    result = await broker.cancel_order(symbol, args.order_id)
    if not broker.dry_run:
        print(c(f"  OK 已撤单  #{result.get('orderId')}  status={result.get('status')}\n", GRN))


async def cmd_leverage(broker: BinanceFuturesTradeWS, args: argparse.Namespace) -> None:
    await broker.set_leverage(args.symbol, args.leverage)


async def cmd_leverage_all(broker: BinanceFuturesTradeWS, args: argparse.Namespace) -> None:
    symbols = _parse_symbol_args(getattr(args, "symbols", None))
    source = "命令行 --symbols"
    current_by_symbol: dict[str, int] = {}

    if not symbols:
        rows = _rest_position_rows(
            broker.api_key,
            broker.api_secret,
            recv_window=broker.recv_window,
            request_timeout=broker.request_timeout,
        )
        if args.only_open:
            rows = [row for row in rows if float(row.get("positionAmt", 0) or 0) != 0]
            source = "REST positionRisk 当前持仓"
        else:
            source = "REST positionRisk 全部币对"
        symbols = _unique_symbols([str(row.get("symbol", "")) for row in rows])
        for row in rows:
            symbol = str(row.get("symbol", "")).strip().upper()
            try:
                current_by_symbol[symbol] = int(row.get("leverage") or 0)
            except (TypeError, ValueError):
                pass

    if args.limit > 0:
        symbols = symbols[: args.limit]

    header(f"批量设置杠杆  {args.leverage}x")
    print(f"  来源        {source}", flush=True)
    print(f"  币对数量    {len(symbols)}", flush=True)
    print(f"  目标杠杆    {args.leverage}x", flush=True)
    if not symbols:
        print(c("  未找到可处理的币对", YEL), flush=True)
        print(flush=True)
        return

    if not broker.dry_run and not confirm(f"确认把 {len(symbols)} 个币对杠杆设置为 {args.leverage}x?", args.yes):
        print("  已取消")
        return

    ok: list[str] = []
    skipped: list[str] = []
    failed: list[tuple[str, str]] = []
    total = len(symbols)
    for index, symbol in enumerate(symbols, 1):
        if not args.force and current_by_symbol.get(symbol) == args.leverage:
            if not args.summary_only:
                print(c(f"  [{index:>3}/{total:<3}] SKIP    {symbol:<16} already {args.leverage}x", DIM), flush=True)
            skipped.append(symbol)
            continue

        try:
            result: dict[str, Any] = {}
            for attempt in range(1, args.retries + 2):
                try:
                    result = await broker.set_leverage(symbol, args.leverage)
                    break
                except Exception:
                    if attempt > args.retries:
                        raise
                    await asyncio.sleep(max(0.0, args.retry_sleep))
            returned_leverage = result.get("leverage", args.leverage)
            tag = "DRY-RUN" if broker.dry_run else "OK"
            color = YEL if broker.dry_run else GRN
            if not args.summary_only:
                print(c(f"  [{index:>3}/{total:<3}] {tag:<7} {symbol:<16} -> {returned_leverage}x", color), flush=True)
            ok.append(symbol)
        except APIError as exc:
            message = f"APIError {exc.code}: {exc}"
            if not args.summary_only:
                print(c(f"  [{index:>3}/{total:<3}] FAIL    {symbol:<16} {message}", RED), file=sys.stderr, flush=True)
            failed.append((symbol, message))
        except Exception as exc:
            message = f"{type(exc).__name__}: {exc}"
            if not args.summary_only:
                print(c(f"  [{index:>3}/{total:<3}] FAIL    {symbol:<16} {message}", RED), file=sys.stderr, flush=True)
            failed.append((symbol, message))

        if args.sleep > 0 and index < total:
            await asyncio.sleep(args.sleep)

    print(c(f"\n  完成：成功 {len(ok)}，跳过 {len(skipped)}，失败 {len(failed)}\n", GRN if not failed else YEL), flush=True)
    if failed:
        raise APIError(-1, f"{len(failed)} 个币对杠杆设置失败")


async def cmd_margin_type(broker: BinanceFuturesTradeWS, args: argparse.Namespace) -> None:
    await broker.set_margin_type(args.symbol, args.margin_type)


COMMANDS = {
    "balance": cmd_balance,
    "positions": cmd_positions,
    "info": cmd_info,
    "orders": cmd_orders,
    "query": cmd_query,
    "buy": cmd_buy,
    "sell": cmd_sell,
    "close": cmd_close,
    "cancel": cmd_cancel,
    "leverage": cmd_leverage,
    "leverage-all": cmd_leverage_all,
    "margin-type": cmd_margin_type,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Pure WebSocket Binance USD-M Futures broker",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--env-file", default="", help="Path to .env; default is script directory .env")
    parser.add_argument("--api-key", default=os.environ.get("BINANCE_API_KEY", ""), help="or set BINANCE_API_KEY in .env")
    parser.add_argument("--api-secret", default=os.environ.get("BINANCE_API_SECRET", ""), help="or set BINANCE_API_SECRET in .env")
    parser.add_argument("--dry-run", action="store_true", help="print order/cancel params, do not send trade requests")
    parser.add_argument("--yes", "-y", action="store_true", help="skip confirmation prompts")
    parser.add_argument("--testnet", action="store_true", help="use Binance Futures testnet WebSocket API")
    parser.add_argument("--ws-url", default="", help="override WebSocket API URL")
    parser.add_argument("--recv-window", type=int, default=RECV_WINDOW)
    parser.add_argument("--timeout", type=int, default=REQUEST_TIMEOUT)

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("balance", help="查账户余额")
    sub.add_parser("positions", help="查当前持仓")

    sp = sub.add_parser("orders", help="查挂单")
    sp.add_argument("symbol", nargs="?", default=None)

    sp = sub.add_parser("query", help="查单")
    sp.add_argument("symbol")
    sp.add_argument("--order-id", type=int, required=True)

    sp = sub.add_parser("buy", help="下买单（做多）")
    sp.add_argument("symbol")
    sp.add_argument("qty")
    sp.add_argument("--price", default=None)
    sp.add_argument("--position-side", choices=["LONG", "SHORT", "BOTH"], default=None)
    sp.add_argument("--yes", "-y", action="store_true")

    sp = sub.add_parser("sell", help="下卖单（做空）")
    sp.add_argument("symbol")
    sp.add_argument("qty")
    sp.add_argument("--price", default=None)
    sp.add_argument("--position-side", choices=["LONG", "SHORT", "BOTH"], default=None)
    sp.add_argument("--yes", "-y", action="store_true")

    sp = sub.add_parser("close", help="市价平仓")
    sp.add_argument("symbol")
    sp.add_argument("--qty", default=None)
    sp.add_argument("--position-side", choices=["LONG", "SHORT", "BOTH"], default=None)
    sp.add_argument("--yes", "-y", action="store_true")

    sp = sub.add_parser("cancel", help="撤单")
    sp.add_argument("symbol")
    sp.add_argument("--order-id", type=int, default=None)
    sp.add_argument("--all", action="store_true", help="用 WS 查询挂单后逐个撤销")
    sp.add_argument("--yes", "-y", action="store_true")

    sp = sub.add_parser("info", help="查询币对当前保证金模式和杠杆")
    sp.add_argument("symbol")

    sp = sub.add_parser("leverage", help="设置杠杆倍数（官方 WS API 不支持）")
    sp.add_argument("symbol")
    sp.add_argument("leverage", type=_leverage_arg)

    sp = sub.add_parser("leverage-all", help="批量设置杠杆倍数，默认 2x（官方 WS API 不支持）")
    sp.add_argument("leverage", nargs="?", type=_leverage_arg, default=DEFAULT_TARGET_LEVERAGE)
    sp.add_argument("--symbols", nargs="+", default=None, help="指定币对；支持空格或逗号分隔，例如 BTCUSDT ETHUSDT 或 BTCUSDT,ETHUSDT")
    sp.add_argument("--only-open", action="store_true", help="只处理当前有持仓的币对")
    sp.add_argument("--limit", type=int, default=0, help="只处理前 N 个币对，用于小范围验证")
    sp.add_argument("--sleep", type=float, default=0.05, help="每次 REST 请求之间的等待秒数")
    sp.add_argument("--retries", type=int, default=2, help="每个币对网络失败后的重试次数")
    sp.add_argument("--retry-sleep", type=float, default=0.5, help="重试前等待秒数")
    sp.add_argument("--force", action="store_true", help="即使当前已经是目标杠杆也重新发送设置请求")
    sp.add_argument("--summary-only", action="store_true", help="只打印汇总，不打印每个币对")
    sp.add_argument("--yes", "-y", action="store_true", help="跳过批量修改确认")

    sp = sub.add_parser("margin-type", help="设置保证金模式（官方 WS API 不支持）")
    sp.add_argument("symbol")
    sp.add_argument("margin_type", choices=["isolated", "crossed"])

    return parser


async def main_async(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv or sys.argv[1:])

    if args.env_file:
        _load_dotenv(Path(args.env_file))
        if not args.api_key:
            args.api_key = os.environ.get("BINANCE_API_KEY", "")
        if not args.api_secret:
            args.api_secret = os.environ.get("BINANCE_API_SECRET", "")

    ws_url = args.ws_url or (WS_TESTNET_ENDPOINT if args.testnet else WS_ENDPOINT)
    dry_run_without_connection = args.dry_run and args.command in {"buy", "sell"}
    rest_only_without_connection = args.command in {"leverage", "leverage-all", "margin-type"}

    try:
        if dry_run_without_connection or rest_only_without_connection:
            broker = BinanceFuturesTradeWS(
                args.api_key,
                args.api_secret,
                dry_run=args.dry_run,
                ws_url=ws_url,
                recv_window=args.recv_window,
                request_timeout=args.timeout,
            )
            await COMMANDS[args.command](broker, args)
            return 0

        async with BinanceFuturesTradeWS(
            args.api_key,
            args.api_secret,
            dry_run=args.dry_run,
            ws_url=ws_url,
            recv_window=args.recv_window,
            request_timeout=args.timeout,
        ) as broker:
            await COMMANDS[args.command](broker, args)
    except AuthError as exc:
        print(c(f"\n  认证错误: {exc}\n  请在 .env 设置 BINANCE_API_KEY / BINANCE_API_SECRET\n", RED), file=sys.stderr)
        return 1
    except UnsupportedWebSocketAPI as exc:
        print(c(f"\n  不支持: {exc}\n  按你的要求未使用 REST，所以该命令不会执行。\n", RED), file=sys.stderr)
        return 2
    except APIError as exc:
        print(c(f"\n  API 错误 [{exc.code}]: {exc}\n", RED), file=sys.stderr)
        return 1
    except (ConnectionError, TimeoutError, asyncio.TimeoutError) as exc:
        print(c(f"\n  网络错误: {exc}\n", RED), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print()
    return 0


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    raise SystemExit(main())
