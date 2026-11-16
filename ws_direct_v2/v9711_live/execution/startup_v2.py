"""Native v2 execution state; never adopt another strategy's book."""
from pathlib import Path
import json
import math
from .. import config as cfg
from ..contracts import atomic_write_json, utc_now_iso
from ..storage import SingleWriter
from .startup_state import _json, _read_pair
from .position_identity import is_manual_position

INIT_JOURNAL = '.v2_initializing.json'


def legacy_locations(root):
    root = Path(root).resolve()
    return (root.parent / 'ws_direct_v1/var/state', root.parent / 'var/state',
            root.parent.parent / 'var/state')


def _initial_pair():
    return {
        'execution_intents.json': dict(schema_version=1, component='execution', intents={}),
        'execution_ledger.json': dict(schema_version=1, component='execution', state=dict(
            strategy_version=cfg.VERSION, realized_balance=cfg.EQUITY0,
            owned_positions={}, tracked_orders={}, reopen_until_epoch={})),
    }


def _initialize(destination):
    """Resume only a provable empty initialization after an interrupted write."""
    journal_path = destination / INIT_JOURNAL
    targets = _initial_pair()
    if journal_path.exists():
        journal = _json(journal_path)
        if journal != dict(version=cfg.VERSION, targets=targets):
            raise RuntimeError(f'conflicting v2 initialization retained at {journal_path}')
        for name, expected in targets.items():
            path = destination / name
            if path.exists() and _json(path) != expected:
                raise RuntimeError(f'changed v2 initialization file preserved: {path}')
    elif any((destination / name).exists() for name in targets):
        return  # Validate existing partial pairs; never complete them by guess.
    else:
        atomic_write_json(journal_path, dict(version=cfg.VERSION, targets=targets))
    for name, value in targets.items():
        if not (destination / name).exists():
            atomic_write_json(destination / name, value)
    journal_path.unlink()


def _check_legacy(old):
    ledger = old / 'execution_ledger.json'
    intents = old / 'execution_intents.json'
    if ledger.exists():
        state = _json(ledger).get('state')
        if not isinstance(state, dict):
            raise RuntimeError(f'invalid old ledger preserved: {ledger}')
        if any(state.get(k) for k in ('owned_positions', 'tracked_orders', 'forced_close_cleanup')):
            raise RuntimeError(f'old strategy has unresolved exposure at {ledger}; '
                               'finish with old trade, no automatic adoption')
    if ledger.exists() or intents.exists():
        pair = _read_pair(old)
        if json.loads(pair['execution_intents.json'])['intents']:
            raise RuntimeError(f'old strategy has unresolved exact intents at {intents}; '
                               'reconcile with old trade, no automatic adoption')


def prepare_live_state(destination, candidates, leases):
    # Caller already owns destination trade.lock and the price mirror lock.
    destination = Path(destination).resolve()
    if 'paper_state' in destination.parts:
        raise RuntimeError('v2 live cannot use paper_state')
    for name in ('.manual_signal_sync_pending.json', '.migration_in_progress'):
        if (destination / name).exists():
            raise RuntimeError(f'interrupted operation retained at {destination / name}; '
                               'resume the matching operation before trading')
    for old in dict.fromkeys(Path(p).resolve() for p in candidates):
        if old == destination or not old.exists():
            continue
        leases.enter_context(SingleWriter(old / 'trade.lock'))
        _check_legacy(old)
    destination.mkdir(parents=True, exist_ok=True)
    _initialize(destination)
    pair = _read_pair(destination)
    state = json.loads(pair['execution_ledger.json'])['state']
    if state.get('strategy_version') != cfg.VERSION:
        raise RuntimeError('not a v2 execution ledger; preserved without migration')
    try:
        balance = float(state['realized_balance'])
    except (KeyError, ValueError, TypeError) as exc:
        raise RuntimeError('invalid v2 realized balance; ledger preserved') from exc
    if not math.isfinite(balance):
        raise RuntimeError('invalid v2 realized balance; ledger preserved')
    for section in ('owned_positions', 'tracked_orders'):
        for row in state.get(section, {}).values():
            if not isinstance(row, dict):
                raise RuntimeError(f'invalid v2 {section} row; ledger preserved')
            cid = row.get('entry_client_order_id') or row.get('client_order_id') or ''
            if (section == 'owned_positions' and is_manual_position(row)
                    and state.get('manual_signal_sync') and row.get('position_seed_id') and not cid):
                continue  # Explicit operator seeds are distinct from exchange fills.
            if not str(cid).startswith(cfg.ORDER_CLIENT_PREFIX + '_'):
                raise RuntimeError('foreign order identity in v2 ledger; no automatic adoption')
    for row in json.loads(pair['execution_intents.json'])['intents'].values():
        if (not isinstance(row, dict) or not str(row.get('client_order_id', '')).startswith(
                cfg.ORDER_CLIENT_PREFIX + '_')):
            raise RuntimeError('foreign exact intent in v2 ledger; no automatic adoption')
    receipt = dict(completed=True, kind='execution_native_v2', mode='v2_native_independent',
                   source=None, destination=str(destination), prepared_at_utc=utc_now_iso(),
                   signal_book='untouched; owned by matching v2 signal')
    atomic_write_json(destination / 'WS_MIGRATION.json', receipt)
    return receipt
