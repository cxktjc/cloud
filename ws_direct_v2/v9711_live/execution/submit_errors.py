"""Pure, conservative submission-fate handling adapted from trade1026.

Reuse its distinction between exchange rejection and uncertain transport; never
import the live launcher (it installs wrappers). Unrecognized failures stay
unknown, unlike the older launcher's absence-of-transport-marker default.
No exchange imports, network access, credential access or runtime state here.
"""
from __future__ import annotations

import math
from typing import Any, Mapping


class ConfirmedSubmitRejection(RuntimeError):
    """An exact submission response explicitly rejected without any fills."""


class ExchangeSubmitRejected(ConfirmedSubmitRejection):
    """An explicit exchange REJECTED reply with zero executed quantity."""

    def __init__(self, message, code=None):
        super().__init__(message)
        self.code = code


class ExchangeSubmitResponseError(RuntimeError):
    """Raw exchange error reply; the message still determines rejection vs uncertain fate."""

    def __init__(self, message, code):
        super().__init__(message)
        self.code = code


class EntryQuoteUnavailable(ConfirmedSubmitRejection):
    """No request sent; a fresh qualifying quote may retry the same signal plan."""

    def __init__(self, message, *, retry_gate='quote', quote_watermarks=None):
        super().__init__(message)
        self.retry_gate = retry_gate
        self.quote_watermarks = dict(quote_watermarks) if quote_watermarks is not None else None


_TRANSPORT = (
    'network', 'ssl', 'eof', 'connection', 'timeout', 'timed out',
    'remote end closed', 'no close frame', 'broken pipe',
)
_UNCERTAIN = _TRANSPORT + (
    'execution status unknown', 'unknown execution',
    'status of the execution is unknown', 'duplicate', 'duplicated',
    'does not exist', 'not found', 'service unavailable', 'internal error',
)
# Only recognizable request/validation rejection messages. Never infer rejection
# from an arbitrary negative API code, HTTP 5xx, or the bare word "rejected".
_REJECTED = (
    'please sign tradfi-perps agreement contract',
    'price not increased by tick size', 'quantity not increased by step size',
    'quantity less than min quantity', 'quantity greater than max quantity',
    'price less than min price', 'price greater than max price',
    'filter failure: price_filter', 'filter failure: lot_size',
    'filter failure: market_lot_size', 'filter failure: min_notional',
    'filter failure: notional', 'margin is insufficient',
    'insufficient balance', 'account has insufficient balance',
    'precision is over the maximum', 'invalid symbol',
    'order would immediately trigger', 'reduceonly order is rejected',
    "order's position side does not match user's setting",
    'timestamp for this request is outside',
    'signature for this request is not valid',
    'api-key format invalid', 'invalid api-key, ip, or permissions',
    # These are raised locally by this package's adapter before submission.
    'aligned price below tick size', 'close quantity below step size',
)


def _exception_chain(exc: BaseException) -> list[BaseException]:
    chain = []
    seen = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        chain.append(current)
        current = current.__cause__ or current.__context__
    return chain


def is_transport_error(exc: BaseException) -> bool:
    """Reconnect the WS session, never re-send a submission automatically."""
    return any(isinstance(error, (TimeoutError, ConnectionError, OSError))
               or any(marker in str(error).lower() for marker in _TRANSPORT)
               for error in _exception_chain(exc))


def is_agreement_rejection(exc: BaseException) -> bool:
    # Explicit permission refusal only; a timeout/wrapper never proves absence.
    return (not is_transport_error(exc) and any(
        'please sign tradfi-perps agreement contract' in ' '.join(str(error).lower().split())
        for error in _exception_chain(exc)))


def submit_error_is_ambiguous(exc: BaseException) -> bool:
    """False only for recognizable rejection, never for an unknown exception.

    Transport failures in an exception chain take precedence over a misleading
    rejection-like wrapper. Duplicated IDs may refer to an accepted first send.
    Not-found is query evidence, not permission to retry a submission.
    DisabledExchange is handled explicitly by the caller (known before send).
    """
    chain = _exception_chain(exc)
    texts = [' '.join(str(error).lower().split()) for error in chain]
    if is_transport_error(exc):
        return True
    if any(marker in text for text in texts for marker in _UNCERTAIN):
        return True
    if isinstance(exc, ConfirmedSubmitRejection):
        return False
    return not any(marker in text for text in texts for marker in _REJECTED)


def require_submit_ack(result: Any, *, symbol: str,
                       client_order_id: str) -> Mapping[str, Any]:
    """Validate response identity; a malformed/empty ACK is ambiguous, not NEW.

    The repository broker normally raises API errors. Be conservative with a
    future adapter returning a raw error or a mismatched order response.
    """
    if not isinstance(result, Mapping) or not result:
        raise RuntimeError('submission response missing; exact fate unknown')
    response_symbol = str(result.get('symbol') or '').upper()
    response_cid = str(result.get('clientOrderId') or result.get('origClientOrderId') or '')
    if ((response_symbol and response_symbol != symbol.upper())
            or (response_cid and response_cid != client_order_id)):
        raise RuntimeError('submission response identity mismatch; exact fate unknown')
    status = str(result.get('status') or '').upper()
    if status == 'REJECTED':
        try:
            filled = float(result.get('executedQty', 0))
        except (TypeError, ValueError):
            filled = float('nan')
        if math.isfinite(filled) and filled == 0.0:
            raise ExchangeSubmitRejected(str(result.get('msg') or 'explicit REJECTED submission'), result.get('code'))
        raise RuntimeError('REJECTED submission has uncertain fills; exact fate unknown')
    if str(result.get('orderId') or '') in ('', '0'):
        # Raw API errors are classified by the same conservative message list.
        # Generic errors still retain exact identity; no synthetic order IDs.
        code = result.get('code')
        if isinstance(code, (int, str)) and str(code).startswith('-') and str(code)[1:].isdigit():
            raise ExchangeSubmitResponseError(str(result.get('msg') or 'submission response missing orderId; exact fate unknown'), code)
        raise RuntimeError(str(result.get('msg') or 'submission response missing orderId; exact fate unknown'))
    return result
