"""Explicit one-shot accounting import from signal. No exchange I/O or orders.

Stop signal and all trade writers first. Default is preview; only apply=True
replaces execution accounting. Original files and frozen targets are backed up,
and interrupted writes must finish before any trade process may start.
"""
from __future__ import annotations

import base64
from contextlib import ExitStack
import copy
import hashlib
import json
import math
import os
from pathlib import Path
import time
import uuid

from .. import config as cfg
from ..contracts import atomic_write_json, utc_now_iso
from ..signal_portfolio import SignalPortfolio
from ..storage import SingleWriter
from .position_identity import CAPITAL_MODE, MANUAL_ORIGIN
from .startup_state import _atomic_bytes, _simulated
from .startup_v2 import legacy_locations

JOURNAL = '.manual_signal_sync_pending.json'
KIND = 'manual_signal_to_trade_v1'
# The receipt is committed last. A pending journal blocks ALL trade startups.
FILES = ('execution_intents.json', 'execution_ledger.json',
         'execution_feedback.json', 'WS_MIGRATION.json')
BACKUP_FILES = (*FILES, cfg.SIGNAL_BOOK_FILENAME)


def _encode(data):
    return json.dumps(data, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf-8')


def _hash(raw):
    return hashlib.sha256(raw).hexdigest()


def _read(path, default=None):
    if not path.exists():
        return copy.deepcopy(default)
    data = json.loads(path.read_text(encoding='utf-8-sig'))
    if not isinstance(data, dict):
        raise ValueError(f'invalid JSON object at {path}; no reset')
    return data


def _positive(value, name):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise ValueError(f'{name} must be a finite positive number')
    return number


def parse_prices(values):
    result = {}
    for item in values:
        symbol, sep, raw = item.partition('=')
        symbol = symbol.strip().upper()
        if not sep or not symbol or symbol in result:
            raise ValueError(f'use one --price SYMBOL=PRICE per position: {item}')
        result[symbol] = _positive(raw, f'{symbol} entry price')
    return result


def _positions(portfolio, prices, symbols, sync_id, now):
    positions = portfolio['positions']
    selected = set(positions) if symbols is None else set(symbols)
    if selected - set(positions):
        raise ValueError('selected symbols absent from signal: ' + ', '.join(sorted(selected-set(positions))))
    if set(prices) - selected:
        raise ValueError('entry-price override requires a selected signal position: ' +
                         ', '.join(sorted(set(prices)-selected)))
    if len(selected) > cfg.SLOTS:
        raise ValueError(f'signal has {len(selected)} selected positions, trade cap={cfg.SLOTS}; '
                         'use --symbol SYMBOL (repeatable) to select actual holdings; no automatic truncation')
    result = {}
    for symbol in sorted(selected):
        source = positions[symbol]
        if str(source.get('side', 'LONG')).upper() != 'LONG':
            raise ValueError(f'only LONG positions can be imported: {symbol}')
        entry = _positive(prices.get(symbol, source['entry_price']), f'{symbol} entry price')
        quantity = _positive(source['quantity'], f'{symbol} quantity')
        notional = _positive(source['notional'], f'{symbol} notional')
        if symbol in prices:
            quantity = notional / entry  # explicit override preserves signal entry notional
        elif not math.isclose(quantity*entry, notional, rel_tol=1e-9, abs_tol=1e-9):
            raise ValueError(f'inconsistent signal quantity/entry/notional: {symbol}')
        bar = int(source['entry_bar_open_ms'])
        due = _positive(source['exit_due_epoch'], f'{symbol} exit due')
        due_ms = int(source['exit_due_ms'])
        reference = int(source['reference_bar_open_ms'])
        if (bar <= 0 or bar % cfg.BAR_MS or reference != bar-cfg.BAR_MS
                or due_ms % cfg.BAR_MS or due_ms <= bar
                or abs(due * 1000-due_ms) > 1):
            raise ValueError(f'inconsistent entry/exit timestamps in signal position: {symbol}')
        result[symbol] = dict(symbol=symbol, side='LONG', quantity=quantity,
            entry_price=entry, notional=quantity*entry, entry_epoch=bar/1000,
            order_bar_open_ms=bar, entry_bar_open_ms=bar,
            reference_bar_open_ms=reference, position_id=source['position_id'],
            entry_client_order_id=None, entry_order_id=None,
            position_seed_id=f'manual:{sync_id}:{symbol}',
            entry_identity_basis='operator_seed_not_exchange_order',
            # Historical account exits must NOT erase a freshly adopted baseline.
            external_evidence_since_ms=int(now*1000),
            exit_due_epoch=due, exit_due_ms=due_ms,
            exit_due_utc=str(source.get('exit_due_utc') or ''),
            position_origin=MANUAL_ORIGIN, manual_sync_id=sync_id)
    return result


def _safe_to_replace(state, intents, imported):
    if not isinstance(state.get('owned_positions', {}), dict) or not isinstance(state.get('tracked_orders', {}), dict):
        raise ValueError('invalid current execution state; no overwrite')
    if state.get('tracked_orders') or state.get('pending_rotation'):
        raise ValueError('tracked entry/rotation pending; first cancel/reconcile exact orders')
    cleanup = state.get('forced_close_cleanup') or {}
    if not isinstance(cleanup, dict):
        raise ValueError('invalid exact-order cleanup state')
    for symbol, pos in state.get('owned_positions', {}).items():
        if not isinstance(pos, dict) or pos.get('close_client_order_id'):
            raise ValueError(f'active or UNKNOWN close on {symbol}; reconcile it before changing the ledger')
    if set(imported) & set(cleanup):
        raise ValueError('imported symbol has unresolved old close: ' + ', '.join(sorted(set(imported)&set(cleanup))))
    safe_ids = {row.get('client_order_id') for row in cleanup.values()
                if isinstance(row, dict) and row.get('client_order_id')}
    for row in intents.values():
        if not isinstance(row, dict) or row.get('client_order_id') not in safe_ids:
            raise ValueError('unresolved exact-order intent exists; never discard it during manual sync')


def _legacy_sources(state_dir, receipt, candidates):
    paths = [*candidates, *(receipt.get('sources') or [])]
    if receipt.get('source'):
        paths.append(receipt['source'])
    result = []
    for item in paths:
        path = Path(item).resolve()
        if path == state_dir or 'paper_state' in path.parts:
            continue
        if path.is_relative_to(state_dir) or state_dir.is_relative_to(path):
            raise ValueError(f'overlapping legacy execution directory: {path}')
        if any((path/name).exists() for name in ('trade.lock', *FILES[:2])) and path not in result:
            result.append(path)
    return result


def _source_hashes(sources):
    return {str(path): {name:_hash((path/name).read_bytes()) for name in FILES[:2]
                        if (path/name).exists()} for path in sources}


def _check_sources(sources, hashes):
    if _source_hashes(sources) != hashes:
        raise ValueError('legacy execution files changed during manual sync; preserve all files and reconcile')


def _backup_bytes(path, raw):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('xb') as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())


def _resume(state_dir, journal, leases, *, apply):
    if journal.get('kind') != KIND or journal.get('destination') != str(state_dir):
        raise ValueError('unrecognized manual sync journal; no overwrite')
    backup = Path(journal['backup']).resolve()
    parent = (state_dir/'execution_backups').resolve()
    if not parent.is_relative_to(state_dir) or not backup.is_relative_to(parent):
        raise ValueError('manual sync backup must stay inside execution state directory')
    if _read(backup/'manifest.json') != journal:
        raise ValueError('manual sync journal/backup manifest mismatch')
    sources = [Path(path).resolve() for path in journal['legacy_trade_sources']]
    for path in sources:
        if path == state_dir or path.is_relative_to(state_dir) or state_dir.is_relative_to(path):
            raise ValueError('invalid legacy execution lock in pending sync')
        leases.enter_context(SingleWriter(path/'trade.lock'))
    _check_sources(sources, journal['legacy_sha256'])
    for name, digest in journal['backup_sha256'].items():
        path = (backup/name).resolve()
        if not path.is_relative_to(backup) or _hash(path.read_bytes()) != digest:
            raise ValueError(f'manual sync backup integrity failure: {name}')
    targets = {name:base64.b64decode(value, validate=True) for name,value in journal['targets'].items()}
    if set(targets) != set(FILES):
        raise ValueError('invalid pending manual sync file set')
    originals = journal['original_sha256']
    for name in FILES:
        path = state_dir/name
        current = _hash(path.read_bytes()) if path.exists() else None
        if current not in {originals.get(name), _hash(targets[name])}:
            raise ValueError(f'execution file changed after manual sync began: {path}; preserve it')
    plan = dict(journal['plan'], applied=bool(apply), backup=str(backup), resumed=True)
    if apply:
        for name, raw in targets.items():
            _atomic_bytes(state_dir/name, raw)
        (state_dir/JOURNAL).unlink()
    return plan


def sync(state_dir, *, prices=None, symbols=None, apply=False, candidates=None, paper=False):
    state_dir = Path(state_dir).resolve()
    if not state_dir.is_dir():
        raise ValueError(f'signal state directory is missing: {state_dir}')
    prices = {str(symbol).upper().strip():_positive(value, f'{symbol} entry price')
              for symbol,value in (prices or {}).items()}
    if symbols is not None:
        cleaned = [str(symbol).upper().strip() for symbol in symbols]
        if not cleaned or any(not symbol for symbol in cleaned) or len(set(cleaned)) != len(cleaned):
            raise ValueError('use each --symbol SYMBOL once; omit it to import all signal positions')
        symbols = cleaned
    # Locks only; preview never writes/replaces any accounting or backup file.
    with ExitStack() as leases:
        leases.enter_context(SingleWriter(state_dir/'trade.lock'))
        leases.enter_context(SingleWriter(state_dir/'signal.lock'))
        for name in (*BACKUP_FILES, JOURNAL):
            if not (state_dir/name).resolve().is_relative_to(state_dir):
                raise ValueError(f'accounting path escapes state directory: {name}')
        pending = _read(state_dir/JOURNAL)
        if pending is not None:
            if ((prices and prices != pending['plan'].get('manual_prices'))
                    or (symbols is not None and sorted(symbols) != pending['plan'].get('selected_symbols'))):
                raise ValueError('finish pending manual sync before changing prices/symbols')
            return _resume(state_dir, pending, leases, apply=apply)
        if (state_dir/'.migration_in_progress').exists():
            raise ValueError('legacy takeover is incomplete; finish it before manual sync')
        signal_path = state_dir/cfg.SIGNAL_BOOK_FILENAME
        signal_raw = signal_path.read_bytes()
        portfolio = json.loads(signal_raw.decode('utf-8-sig'))
        validator = object.__new__(SignalPortfolio)
        validator.path = signal_path
        validator._validate(portfolio)
        if portfolio.get('last_processed_bar_open_ms') is None:
            raise ValueError('signal has no completed model checkpoint; no import')
        _positive(portfolio['initial_equity'], 'signal initial equity')
        pair_exists = [(state_dir/name).exists() for name in FILES[:2]]
        if any(pair_exists) and not all(pair_exists):
            raise ValueError('incomplete execution ledger pair; no reset')
        state_payload = _read(state_dir/FILES[1], {'schema_version':1, 'component':'execution', 'state':{}})
        intent_payload = _read(state_dir/FILES[0], {'schema_version':1, 'component':'execution', 'intents':{}})
        state, intents = state_payload.get('state'), intent_payload.get('intents')
        if not isinstance(state, dict) or not isinstance(intents, dict):
            raise ValueError('invalid current execution state/intents; no reset')
        if not paper and (_simulated(state_payload) or _simulated(intent_payload)):
            raise ValueError('simulated execution history; do not import it as live')
        now = time.time()
        sync_id = uuid.uuid4().hex
        imported = _positions(portfolio, prices, symbols, sync_id, now)
        _safe_to_replace(state, intents, imported)
        previous_receipt = _read(state_dir/'WS_MIGRATION.json', {})
        sources = _legacy_sources(state_dir, previous_receipt,
            legacy_locations(cfg.ROOT) if candidates is None else candidates)
        for path in sources:
            leases.enter_context(SingleWriter(path/'trade.lock'))
        legacy_hashes = _source_hashes(sources)
        for path in sources:
            hashes = legacy_hashes[str(path)]
            if previous_receipt.get('kind') == 'execution_native_v2':
                from .startup_v2 import _check_legacy
                _check_legacy(path)
                continue  # Independent v2 history need not equal a flat old book.
            if hashes and len(hashes) != 2:
                raise ValueError(f'incomplete legacy execution ledger pair: {path}')
            known = (previous_receipt.get('legacy_sha256') or {}).get(str(path))
            if known is None and previous_receipt.get('source') and Path(previous_receipt['source']).resolve() == path:
                known = previous_receipt.get('source_sha256')
            if known is not None and hashes != known:
                raise ValueError(f'legacy execution ledger changed since takeover: {path}')
            if hashes and known is None:
                current = {name:_hash((state_dir/name).read_bytes()) for name in FILES[:2]
                           if (state_dir/name).exists()}
                if hashes != current:
                    raise ValueError(f'conflicting legacy execution ledger without takeover receipt: {path}; no overwrite')
        plan = dict(applied=False, action='one_time_signal_ledger_import_no_exchange_orders',
            signal_file=str(signal_path), trade_file=str(state_dir/FILES[1]),
            signal_session_id=portfolio['session_id'], signal_revision=portfolio.get('revision'),
            signal_sha256=_hash(signal_raw), initial_equity_after=portfolio['initial_equity'],
            realized_balance_before=state.get('realized_balance', cfg.EQUITY0),
            realized_balance_after=portfolio['realized_balance'],
            positions_before=copy.deepcopy(state.get('owned_positions', {})),
            positions_after=imported, manual_prices=prices, selected_symbols=sorted(imported),
            excluded_signal_symbols=sorted(set(portfolio['positions'])-set(imported)),
            legacy_trade_sources=[str(path) for path in sources],
            warning='accounting import only; NO BUY/SELL. Adopt only verified actual holdings; quantities are not exchange-rounded',
            actual_position_cap=cfg.SLOTS, imported_position_count=len(imported),
            future_accounting='trade owns capital/positions independently; no automatic resync')
        if not apply:
            return plan
        new_state = copy.deepcopy(state)
        new_state.pop('signal_capital_sync', None)
        new_state.update(strategy_version=cfg.VERSION, owned_positions=imported, tracked_orders={},
            realized_balance=float(portfolio['realized_balance']),
            book_initial_equity=float(portfolio['initial_equity']), capital_mode=CAPITAL_MODE,
            pending_rotation=None, unknown_since_epoch={},
            manual_signal_sync=dict(sync_id=sync_id, synced_at_epoch=now,
                signal_session_id=portfolio['session_id'], signal_revision=portfolio.get('revision'),
                realized_balance_at_sync=float(portfolio['realized_balance']),
                realized_balance_before=state.get('realized_balance', cfg.EQUITY0),
                imported_position_count=len(imported), manual_prices=prices,
                selected_symbols=sorted(imported), signal_sha256=plan['signal_sha256'],
                position_basis='operator_seed_not_exchange_fill'),
            signal_session_id=portfolio['session_id'], last_signal_revision=portfolio.get('revision'))
        for name in ('reopen_until_epoch', 'forced_close_blocks'):
            records = new_state.setdefault(name, {})
            for symbol in imported:
                records.pop(symbol, None)
        parent = (state_dir/'execution_backups').resolve()
        if not parent.is_relative_to(state_dir):
            raise ValueError('backup path is outside execution state directory')
        backup = parent/f'manual_signal_sync_{sync_id}'
        receipt = dict(completed=True, kind=KIND, mode='manual_signal_ledger_sync',
            source=str(sources[0]) if sources else None, sources=[str(path) for path in sources],
            destination=str(state_dir), synced_at_utc=utc_now_iso(), sync_id=sync_id,
            legacy_sha256=legacy_hashes, backup=str(backup),
            signal_book='untouched; explicit operator import, not exchange entry fills')
        feedback = dict(schema_version=1, component='execution', mode='manual_sync',
            status='manual_sync_applied_waiting_for_trade', updated_at_epoch=now,
            initial_equity=new_state['book_initial_equity'], realized_balance=new_state['realized_balance'],
            cumulative_pnl=new_state['realized_balance']-new_state['book_initial_equity'],
            positions=imported, open_orders={}, close_orders={}, capital_mode=CAPITAL_MODE,
            manual_signal_sync=new_state['manual_signal_sync'])
        targets = {FILES[0]:_encode(intent_payload),
            FILES[1]:_encode(dict(schema_version=1, component='execution', updated_at_epoch=now, state=new_state)),
            FILES[2]:_encode(feedback), FILES[3]:_encode(receipt)}
        backup.mkdir(parents=True)
        originals, backup_hashes = {}, {}
        for name in BACKUP_FILES:
            path = state_dir/name
            if path.exists():
                raw = path.read_bytes()
                _backup_bytes(backup/name, raw)
                originals[name] = backup_hashes[name] = _hash(raw)
        for index, path in enumerate(sources):
            for name in FILES:
                if (path/name).exists():
                    raw = (path/name).read_bytes()
                    name_in_backup = f'legacy/{index}/{name}'
                    _backup_bytes(backup/name_in_backup, raw)
                    backup_hashes[name_in_backup] = _hash(raw)
        _check_sources(sources, legacy_hashes)
        journal = dict(kind=KIND, destination=str(state_dir), backup=str(backup),
            original_sha256=originals, backup_sha256=backup_hashes,
            targets={name:base64.b64encode(raw).decode('ascii') for name,raw in targets.items()},
            legacy_trade_sources=plan['legacy_trade_sources'], legacy_sha256=legacy_hashes, plan=plan)
        atomic_write_json(backup/'manifest.json', journal)
        atomic_write_json(state_dir/JOURNAL, journal)
        for name, raw in targets.items():
            _atomic_bytes(state_dir/name, raw)
        (state_dir/JOURNAL).unlink()
        return dict(plan, applied=True, backup=str(backup), resumed=False)
