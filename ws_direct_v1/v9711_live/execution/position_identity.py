"""Keep explicitly adopted accounting positions distinct from exchange fills."""
MANUAL_ORIGIN = 'manual_signal_ledger_sync'
CAPITAL_MODE = 'trade_owned_after_manual_signal_sync'


def is_manual_position(row):
    return row.get('position_origin') == MANUAL_ORIGIN


def position_identity(row):
    if is_manual_position(row):
        return str(row.get('position_seed_id') or '')
    return str(row.get('entry_client_order_id') or '')
