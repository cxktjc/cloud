"""Automatic execution-only takeover. Never copy or replace the signal book."""
from pathlib import Path
import hashlib
import json
import os
import base64
import tempfile
import uuid
from ..contracts import atomic_write_json, utc_now_iso
from ..storage import SingleWriter

FILES = ('execution_intents.json', 'execution_ledger.json')
AUTO_KIND = 'execution_auto_v1'
SWITCH_KIND = 'execution_simulation_archive_v1'

class SimulatedHistory(RuntimeError):
    def __init__(self, directory, payloads):
        super().__init__(f'simulated order history at {directory}; never adopt paper as live')
        self.payloads = payloads

def legacy_locations(root):
    root = Path(root).resolve()
    return (root.parent / 'var/state', root.parent.parent / 'var/state')

def _json(path):
    try:
        value = json.loads(path.read_text(encoding='utf-8-sig'))
    except (OSError, ValueError) as exc:
        raise RuntimeError(f'invalid execution state at {path}: {exc}') from exc
    if not isinstance(value, dict):
        raise RuntimeError(f'expected execution state object at {path}')
    return value

def _simulated(value):
    if isinstance(value, dict):
        return bool(value.get('_simulated')) or any(_simulated(v) for v in value.values())
    if isinstance(value, list):
        return any(_simulated(v) for v in value)
    return isinstance(value, str) and value.startswith('sim-')

def _read_pair(directory):
    result = {}
    for name in FILES:
        path = directory / name
        if not path.is_file():
            raise RuntimeError(f'incomplete execution ledger pair at {directory}: missing {name}')
        raw = path.read_bytes()
        data = _json(path)
        key = 'intents' if name == FILES[0] else 'state'
        if not isinstance(data.get(key), dict):
            raise RuntimeError(f'invalid {key} object at {path}')
        if key == 'state':
            for field in ('owned_positions', 'tracked_orders'):
                if not isinstance(data[key].get(field, {}), dict):
                    raise RuntimeError(f'invalid {field} at {path}')
        result[name] = raw
    from .durable_writer import embedded_intents, resolve_store
    state_payload = resolve_store(json.loads(result[FILES[1]]), directory/FILES[1])
    if state_payload.get('execution_bundle_version'):
        result[FILES[1]] = json.dumps(state_payload, ensure_ascii=False, allow_nan=False).encode('utf-8')
    canonical = embedded_intents(state_payload)
    if canonical is not None:
        result[FILES[0]] = json.dumps(dict(schema_version=1, component='execution', intents=canonical),
                                    ensure_ascii=False, allow_nan=False).encode('utf-8')
    # Feedback is a display snapshot, not evidence that durable orders are fake.
    if any(_simulated(json.loads(raw)) for raw in result.values()):
        raise SimulatedHistory(directory, result)
    return result

def _hashes(payloads):
    return {name: hashlib.sha256(raw).hexdigest() for name, raw in payloads.items()}

def _complete(destination, source, hashes, mode):
    receipt = dict(completed=True, kind=AUTO_KIND, mode=mode,
        source=str(source) if source else None, destination=str(destination),
        migrated_at_utc=utc_now_iso(), source_sha256=hashes,
        signal_book='untouched; owned by the matching signal')
    atomic_write_json(destination / 'WS_MIGRATION.json', receipt)
    return receipt

def _pure_simulation(payloads):
    state = json.loads(payloads[FILES[1]])['state']
    if state.get('owned_positions'):
        return False  # Never erase positions just because a later run was dry-run.
    from .. import config as cfg
    if float(state.get('realized_balance', cfg.EQUITY0)) != cfg.EQUITY0:
        return False
    identities = {}
    anonymous = []
    def visit(value, inherited=''):
        if isinstance(value, dict):
            cid = str(value.get('client_order_id') or value.get('clientOrderId') or inherited)
            oid = value.get('order_id', value.get('orderId'))
            pending = (value.get('fate_unknown') or
                       value.get('status', value.get('last_status')) in ('UNKNOWN', 'SUBMITTING'))
            if oid not in (None, '', 0, '0') or value.get('_simulated') or pending or (
                    value.get('client_order_id') or value.get('clientOrderId')) or (
                    inherited and value.get('kind') in ('entry', 'close')):
                evidence = identities.setdefault(cid, set()) if cid else set()
                evidence.add('sim' if str(oid).startswith('sim-') or value.get('_simulated')
                             else 'real' if oid not in (None, '', 0, '0') else 'unknown')
                if not cid:
                    anonymous.append(evidence)
            for key, item in value.items():
                if str(key).startswith('o:') and ':sim-' not in str(key):
                    anonymous.append({'real'})
                visit(item, str(key)[2:] if str(key).startswith('c:') else cid)
        elif isinstance(value, list):
            for item in value:
                visit(item, inherited)
    for raw in payloads.values():
        visit(json.loads(raw))
    groups = list(identities.values()) + anonymous
    return bool(groups) and all('sim' in group and 'real' not in group for group in groups)

def _atomic_bytes(path, raw):
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix='.execution-startup-')
    try:
        with os.fdopen(fd, 'wb') as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)

def _finish_simulation_switch(destination, journal, leases, lease):
    backup = Path(journal['backup']).resolve()
    if not backup.is_relative_to(destination / 'execution_backups'):
        raise RuntimeError('invalid simulation backup path; no files replaced')
    for source_text, expected in journal.get('legacy_sha256', {}).items():
        source = Path(source_text).resolve()
        lease(source)
        try:
            current = _read_pair(source)
        except SimulatedHistory as exc:
            current = exc.payloads
        if _hashes(current) != expected:
            raise RuntimeError(f'legacy state changed during interrupted simulation switch: {source}')
    if _json(backup/'manifest.json') != journal:
        raise RuntimeError('simulation switch journal differs from backup manifest')
    targets = {name:base64.b64decode(value, validate=True)
               for name, value in journal['targets'].items()}
    if set(targets) != set(FILES):
        raise RuntimeError('invalid live execution target pair')
    for name, raw in targets.items():
        data = json.loads(raw)
        key = 'intents' if name == FILES[0] else 'state'
        if not isinstance(data, dict) or not isinstance(data.get(key), dict) or _simulated(data):
            raise RuntimeError('invalid or simulated live execution target; no files replaced')
    # Verify every backup/current file BEFORE replacing either execution file.
    for name, expected in journal['backup_sha256'].items():
        raw = (backup/name).read_bytes()
        if hashlib.sha256(raw).hexdigest() != expected:
            raise RuntimeError(f'simulation backup integrity error: {backup/name}')
        path = destination/name
        if path.exists():
            actual = hashlib.sha256(path.read_bytes()).hexdigest()
            allowed = {expected}
            if name in targets:
                allowed.add(hashlib.sha256(targets[name]).hexdigest())
            if name == 'WS_MIGRATION.json' and _json(path).get('backup') == str(backup):
                allowed.add(actual)  # receipt written just before an interrupted completion
            if actual not in allowed:
                raise RuntimeError(f'execution file changed since simulation backup: {path}')
    for name, raw in targets.items():
        _atomic_bytes(destination/name, raw)
    result = _complete(destination, journal.get('source'), _hashes(targets),
                       'live_after_simulation_archive')
    result['backup'] = str(backup)
    atomic_write_json(destination/'WS_MIGRATION.json', result)
    (destination/'.migration_in_progress').unlink()
    return result

def _archive_simulation(destination, old, target, source, source_pairs, leases, lease):
    parent = (destination/'execution_backups').resolve()
    if not parent.is_relative_to(destination):
        raise RuntimeError('execution backup directory must remain inside live state directory')
    backup = parent/uuid.uuid4().hex
    backup.mkdir(parents=True)
    originals = dict(old)
    for name in ('execution_feedback.json', 'WS_MIGRATION.json'):
        if (destination/name).exists():
            originals[name] = (destination/name).read_bytes()
    for name, raw in originals.items():
        with (backup/name).open('xb') as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    journal = dict(kind=SWITCH_KIND, destination=str(destination), backup=str(backup),
        backup_sha256=_hashes(originals), source=str(source) if source else None,
        legacy_sha256={str(path):_hashes(pair) for path,pair in source_pairs},
        targets={name:base64.b64encode(raw).decode('ascii') for name,raw in target.items()})
    atomic_write_json(backup/'manifest.json', journal)
    atomic_write_json(destination/'.migration_in_progress', journal)
    return _finish_simulation_switch(destination, journal, leases, lease)

def prepare_live_state(destination, candidates, leases):
    # Caller holds destination trade.lock. Source leases last the entire live run.
    destination = Path(destination).resolve()
    locked = set()
    def lease(path):
        path = Path(path).resolve()
        if path in locked:
            return
        if path == destination or 'paper_state' in path.parts:
            raise RuntimeError(f'invalid live takeover source: {path}')
        leases.enter_context(SingleWriter(path / 'trade.lock'))
        locked.add(path)
    if (destination / '.manual_signal_sync_pending.json').exists():
        raise RuntimeError('manual signal ledger sync interrupted; rerun scripts.sync_signal_to_trade --apply; no trade started')
    receipt_path = destination / 'WS_MIGRATION.json'
    journal_path = destination / '.migration_in_progress'
    journal = _json(journal_path) if journal_path.exists() else None
    if journal and journal.get('kind') == SWITCH_KIND:
        if journal.get('destination') != str(destination):
            raise RuntimeError('interrupted simulation switch destination mismatch')
        return _finish_simulation_switch(destination, journal, leases, lease)
    receipt = _json(receipt_path) if receipt_path.exists() else None
    if receipt is not None and not receipt.get('completed'):
        raise RuntimeError(f'incomplete live receipt at {receipt_path}; preserving files')
    if receipt and not journal_path.exists() and receipt.get('kind') == 'manual_signal_to_trade_v1':
        pair = _read_pair(destination)
        state = json.loads(pair[FILES[1]])['state']
        if ((state.get('manual_signal_sync') or {}).get('sync_id') != receipt.get('sync_id')
                or not receipt.get('sync_id')):
            raise RuntimeError('manual signal ledger receipt/state mismatch; no overwrite')
        paths = [*candidates, *receipt.get('sources', [])]
        if receipt.get('source'):
            paths.append(receipt['source'])
        recorded = receipt.get('legacy_sha256') or {}
        for path in dict.fromkeys(Path(p).resolve() for p in paths):
            if path == destination or 'paper_state' in path.parts:
                continue
            if path.is_relative_to(destination) or destination.is_relative_to(path):
                raise RuntimeError(f'overlapping execution directories: {path}, {destination}')
            present = any((path/name).exists() for name in FILES)
            if present or (path/'trade.lock').exists() or str(path) in recorded:
                lease(path)
                hashes = {name:hashlib.sha256((path/name).read_bytes()).hexdigest()
                          for name in FILES if (path/name).exists()}
                if hashes != recorded.get(str(path), {}):
                    raise RuntimeError(f'legacy execution ledger changed after manual sync: {path}; no overwrite')
        return receipt
    if receipt and not journal_path.exists():
        source = receipt.get('source')
        if source:
            source = Path(source).resolve()
            if source == destination or 'paper_state' in source.parts:
                raise RuntimeError(f'invalid live takeover source: {source}')
            lease(source)
        try:
            _read_pair(destination)
            return receipt
        except SimulatedHistory:
            pass  # Classify and archive only if all durable identities are simulated.
    journal = _json(journal_path) if journal_path.exists() else None
    if journal and (journal.get('kind') != AUTO_KIND or
                    journal.get('destination') != str(destination)):
        raise RuntimeError(f'unrecognized incomplete migration at {journal_path}; preserving files')
    own_exists = any((destination / name).exists() for name in FILES)
    own_simulated = None
    try:
        own = _read_pair(destination) if own_exists and not journal else None
    except SimulatedHistory as exc:
        if not _pure_simulation(exc.payloads):
            raise RuntimeError('mixed real/unknown and simulated execution history; preserve all records') from exc
        own, own_simulated = None, exc.payloads
    sources = []
    simulated_sources = []
    paths = list(candidates)
    if receipt and receipt.get('source'):
        paths.append(Path(receipt['source']))
    if receipt:
        paths.extend(Path(path) for path in receipt.get('sources', []))
    if journal:
        paths = [Path(journal['source'])]
    for item in dict.fromkeys(Path(p).resolve() for p in paths):
        if item == destination or 'paper_state' in item.parts:
            continue
        if item.is_relative_to(destination) or destination.is_relative_to(item):
            raise RuntimeError(f'overlapping execution directories: {item}, {destination}')
        # Also lock an old empty writer if it has started but not saved a ledger.
        present = any((item / name).exists() for name in FILES)
        if present or (item / 'trade.lock').exists():
            lease(item)
        if present:
            try:
                sources.append((item, _read_pair(item)))
            except SimulatedHistory as exc:
                if not _pure_simulation(exc.payloads):
                    raise RuntimeError(f'mixed real/unknown and simulated order history at {item}') from exc
                simulated_sources.append((item, exc.payloads))
    groups = {}
    for source, payloads in sources:
        groups.setdefault(tuple(sorted(_hashes(payloads).items())), []).append((source, payloads))
    if len(groups) > 1:
        raise RuntimeError('conflicting legacy execution ledgers detected automatically: ' +
                           ', '.join(str(row[0]) for row in sources) + '; no files overwritten')
    selected = sources[0] if sources else None
    if own_simulated is not None:
        if selected:
            source, target = selected
        else:
            from .. import config as cfg
            source = None
            target = {FILES[0]:json.dumps({'schema_version':1, 'component':'execution', 'intents':{}}).encode(),
                      FILES[1]:json.dumps({'schema_version':1, 'component':'execution', 'state':{
                          'owned_positions':{}, 'tracked_orders':{}, 'reopen_until_epoch':{},
                          'realized_balance':cfg.EQUITY0}}).encode()}
        return _archive_simulation(destination, own_simulated, target, source,
                                   sources+simulated_sources, leases, lease)
    if own is not None:
        if selected and _hashes(own) != _hashes(selected[1]):
            state = json.loads(own[FILES[1]])['state']
            from .. import config as cfg
            empty = (not json.loads(own[FILES[0]])['intents']
                     and not state.get('owned_positions') and not state.get('tracked_orders')
                     and not state.get('forced_close_history') and not state.get('unknown_since_epoch')
                     and float(state.get('realized_balance', cfg.EQUITY0)) == cfg.EQUITY0)
            if empty:
                return _archive_simulation(destination, own, selected[1], selected[0],
                                           sources+simulated_sources, leases, lease)
        if selected and _hashes(own) != _hashes(selected[1]):
            raise RuntimeError(f'current execution ledger {destination} conflicts with {selected[0]}; '
                               'signal book preserved; no execution files overwritten')
        return _complete(destination, selected[0] if selected else None, _hashes(own), 'existing_native')
    if selected is None:
        raise RuntimeError('no real execution ledger found automatically; checked: ' +
                           ', '.join(str(p) for p in (destination, *map(Path, candidates))) +
                           '; signal model positions are not proof of exchange fills; paper never imported')
    source, payloads = selected
    hashes = _hashes(payloads)
    if journal and (journal.get('source_sha256') != hashes or journal.get('source') != str(source)):
        raise RuntimeError('legacy ledger changed after interrupted takeover; preserving destination')
    for name, raw in payloads.items():
        path = destination / name
        if path.exists() and (not journal or path.read_bytes() != raw):
            raise RuntimeError(f'destination execution file already exists: {path}; no overwrite')
    if not journal:
        atomic_write_json(journal_path, dict(kind=AUTO_KIND, source=str(source),
            destination=str(destination), source_sha256=hashes))
    for name, raw in payloads.items():
        path = destination / name
        if not path.exists():
            with path.open('xb') as handle:
                handle.write(raw)
                handle.flush()
                os.fsync(handle.fileno())
    result = _complete(destination, source, hashes, 'automatic_execution_takeover')
    journal_path.unlink()
    return result
