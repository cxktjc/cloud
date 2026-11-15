"""Shared Binance proxy helpers.

All Binance HTTP/WebSocket traffic in the live scripts defaults to the local
proxy at 127.0.0.1:12334. Set BINANCE_PROXY_URL to override it, or to "off" to
temporarily disable the forced proxy.
"""

from __future__ import annotations

import os
import urllib.request
from typing import Any


DEFAULT_BINANCE_PROXY_URL = "http://127.0.0.1:12334"
_DISABLE_VALUES = {"", "0", "false", "no", "off", "direct", "none"}


def proxy_url() -> str | None:
    raw = os.environ.get("BINANCE_PROXY_URL")
    if raw is None:
        raw = os.environ.get("BINANCE_PROXY", DEFAULT_BINANCE_PROXY_URL)
    value = str(raw).strip()
    if value.lower() in _DISABLE_VALUES:
        return None
    return value


def websocket_connect_kwargs() -> dict[str, Any]:
    url = proxy_url()
    return {"proxy": url} if url else {"proxy": None}


def urlopen(
    request: urllib.request.Request | str,
    timeout: float | None = None,
    **kwargs: Any,
) -> Any:
    url = proxy_url()
    if not url:
        return urllib.request.urlopen(request, timeout=timeout, **kwargs)
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({
            "http": url,
            "https": url,
        })
    )
    return opener.open(request, timeout=timeout, **kwargs)
