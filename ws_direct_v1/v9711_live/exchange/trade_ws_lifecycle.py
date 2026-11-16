"""Keep established trading sockets alive and recover transport, never orders.

One broker event loop owns each socket. The same request lock protects socket
replacement and send/ACK reception: recovery never closes an in-flight request.
Only Ping/Pong keepalive and connection attempts run here; no order is replayed.
"""
from __future__ import annotations

import asyncio
from contextlib import suppress
import logging
import ssl
import time

import websockets
from websockets.protocol import State

from . import binance_network

LOG = logging.getLogger('v9711_live_trade.client')
PING_INTERVAL_SECONDS = 20.0
PING_TIMEOUT_SECONDS = 10.0
CONNECT_TIMEOUT_SECONDS = 5.0
CLOSE_TIMEOUT_SECONDS = 2.0
MONITOR_INTERVAL_SECONDS = 1.0
RECONNECT_BEFORE_SECONDS = 23 * 60 * 60
RETRY_MIN_SECONDS = 1.0
RETRY_MAX_SECONDS = 30.0


class TradeWSLifecycle:
    """Requires ws_url, ssl_verify and an asyncio _lock on the owning loop."""

    def _init_lifecycle(self):
        self._ws = None
        self._connected_at = 0.0
        self._stopping = False
        self._supervisor = None
        self._wake = asyncio.Event()
        self._connect_count = 0
        self._connect_failures = 0
        self._retry_at = 0.0
        self._last_connect_warning = float('-inf')

    def _socket_open(self):
        return self._ws is not None and self._ws.state == State.OPEN

    def _start_supervisor(self):
        if not self._stopping and (self._supervisor is None or self._supervisor.done()):
            self._supervisor = asyncio.create_task(self._supervise(), name='v9711-trade-ws-keepalive')

    async def _close_socket_locked(self):
        socket, self._ws = self._ws, None
        if socket is not None:
            with suppress(Exception):
                await socket.close()

    async def _connect_locked(self, reason):
        # Caller holds the same lock as send/recv. No in-flight ACK is discarded
        # by routine 23-hour rotation or background recovery.
        if self._stopping:
            raise ConnectionError('trade WS stopping; no request sent')
        await self._close_socket_locked()
        kwargs = dict(ping_interval=PING_INTERVAL_SECONDS, ping_timeout=PING_TIMEOUT_SECONDS,
                      open_timeout=CONNECT_TIMEOUT_SECONDS, close_timeout=CLOSE_TIMEOUT_SECONDS,
                      max_queue=1024)
        if self.ws_url.startswith('wss://') and not self.ssl_verify:
            kwargs['ssl'] = ssl._create_unverified_context()
        kwargs.update(binance_network.websocket_connect_kwargs())
        try:
            self._ws = await websockets.connect(self.ws_url, **kwargs)
        except Exception as exc:
            self._connect_failures += 1
            delay = min(RETRY_MAX_SECONDS, RETRY_MIN_SECONDS * 2**min(10, self._connect_failures-1))
            self._retry_at = time.monotonic()+delay
            if time.monotonic()-self._last_connect_warning >= 60:
                self._last_connect_warning = time.monotonic()
                LOG.warning('trade WS disconnected; background reconnect retry=%.1fs error=%s; '
                            'exact orders remain in ledger (no replay)', delay, exc)
            raise
        self._connected_at = time.monotonic()
        self._connect_count += 1
        self._connect_failures = 0
        self._retry_at = 0.0
        if self._connect_count > 1:
            LOG.info('trade WS reconnected reason=%s; exact orders retained, no automatic resend', reason)
        else:
            LOG.debug('trade WS connected keepalive=%.1fs pong_timeout=%.1fs',
                      PING_INTERVAL_SECONDS, PING_TIMEOUT_SECONDS)

    async def connect(self):
        async with self._lock:
            self._stopping = False
            await self._connect_locked('explicit_reconnect')
            self._start_supervisor()

    async def _ensure_connected(self):
        # Called INSIDE the request lock. Healthy idle connections stay intact.
        if self._stopping:
            raise ConnectionError('trade WS stopping; no request sent')
        self._start_supervisor()
        if not self._socket_open():
            if time.monotonic() < self._retry_at:
                raise ConnectionError('trade WS background reconnect cooldown; no request sent')
            await self._connect_locked('connection_closed' if self._connect_count else 'initial_connect')

    async def _invalidate_locked(self):
        await self._close_socket_locked()
        self._retry_at = 0.0  # Prompt background recovery, not a retry of the request.
        self._wake.set()

    async def _supervise(self):
        while not self._stopping:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=MONITOR_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                pass
            self._wake.clear()
            if self._stopping:
                break
            # Waiting here never blocks the trade owner or another broker lane.
            # It only defers replacement until this lane's current ACK completes.
            async with self._lock:
                if self._stopping:
                    break
                if self._socket_open() and time.monotonic()-self._connected_at < RECONNECT_BEFORE_SECONDS:
                    continue
                if time.monotonic() < self._retry_at:
                    continue
                reason = 'session_lifetime' if self._socket_open() else 'connection_closed'
                try:
                    await self._connect_locked(reason)
                except Exception:
                    pass  # Bounded backoff/warning are managed by _connect_locked.

    async def close(self):
        self._stopping = True
        self._wake.set()
        supervisor, self._supervisor = self._supervisor, None
        if supervisor is not None and supervisor is not asyncio.current_task():
            supervisor.cancel()
            with suppress(asyncio.CancelledError):
                await supervisor
        async with self._lock:
            await self._close_socket_locked()
