"""Exact exchange evidence checks shared by entry queries and callbacks."""
from collections.abc import Mapping
import math


def matches_order_identity(result, *, symbol, client_order_id='', order_id=None):
    if not isinstance(result, Mapping):
        return False
    cid = str(result.get('clientOrderId') or result.get('origClientOrderId') or '')
    oid = str(result.get('orderId') or '')
    actual_symbol = str(result.get('symbol') or '').upper()
    expected_oid = str(order_id or '')
    return bool(((cid and client_order_id) or (oid and expected_oid))
                and (not actual_symbol or actual_symbol == str(symbol).upper())
                and (not cid or not client_order_id or cid == str(client_order_id))
                and (not oid or not expected_oid or oid == expected_oid))


def exact_entry_fill_time_ms(result, *, symbol, client_order_id='', order_id=None, allow_update_time=False):
    """Use exchange BUY fill time, never the time the owner finally books it."""
    if not matches_order_identity(result, symbol=symbol, client_order_id=client_order_id, order_id=order_id):
        return 0
    def positive(value):
        try:
            value = float(value)
            return value if math.isfinite(value) and value > 0 else 0
        except (TypeError, ValueError, OverflowError):
            return 0
    stamps = [int(positive(fill.get('time'))) for fill in (result.get('_user_fills') or [])
              if isinstance(fill, Mapping) and str(fill.get('side') or '').upper() == 'BUY'
              and positive(fill.get('qty')) and positive(fill.get('price'))
              and matches_order_identity(fill, symbol=symbol, client_order_id=client_order_id, order_id=order_id)
              and positive(fill.get('time'))]
    if stamps:
        return min(stamps)
    # CANCELED order updateTime can be much later than its partial fills.
    if (allow_update_time and result.get('status') in ('FILLED', 'PARTIALLY_FILLED')
            and str(result.get('side') or 'BUY').upper() == 'BUY'
            and positive(result.get('executedQty'))):
        return int(positive(result.get('updateTime')))
    return 0
