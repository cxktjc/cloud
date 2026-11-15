"""Direct Binance HTTP/WebSocket network helpers.

This deployment does not use a local or remote HTTP proxy. Outbound Binance
requests use the normal HTTPS/WSS connection directly.
"""
from __future__ import annotations

import urllib.request
from typing import Any


def websocket_connect_kwargs() -> dict[str, Any]:
    # Explicitly disable environment/system proxy discovery in websockets.
    return {"proxy": None}


def urlopen(request: urllib.request.Request | str,
            timeout: float | None = None, **kwargs: Any) -> Any:
    # An empty ProxyHandler prevents HTTP_PROXY/HTTPS_PROXY environment values
    # from silently routing traffic through a local port.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    return opener.open(request, timeout=timeout, **kwargs)
