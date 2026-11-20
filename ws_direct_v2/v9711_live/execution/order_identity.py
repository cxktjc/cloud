"""Exact exchange evidence checks shared by entry queries and callbacks."""
from collections.abc import Mapping


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
