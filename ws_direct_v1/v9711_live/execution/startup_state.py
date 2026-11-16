"""Automatic execution-only takeover. Never copy or replace the signal book."""
from pathlib import Path
import hashlib
import json
import os
from ..contracts import atomic_write_json, utc_now_iso
from ..storage import SingleWriter

FILES = ('execution_intents.json', 'execution_ledger.json')
AUTO_KIND = 'execution_auto_v1'

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
        if _simulated(data):
            raise RuntimeError(f'simulated order history at {directory}; never adopt paper as live')
        result[name] = raw
    feedback = directory / 'execution_feedback.json'
    if feedback.exists() and _json(feedback).get('mode') == 'dry_run':
        raise RuntimeError(f'dry-run execution history at {directory}; never adopt paper as live')
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

def prepare_live_state(destination, candidates, leases):
    # Caller holds destination trade.lock. Source leases last the entire live run.
    destination = Path(destination).resolve()
    receipt_path = destination / 'WS_MIGRATION.json'
    journal_path = destination / '.migration_in_progress'
    receipt = _json(receipt_path) if receipt_path.exists() else None
    if receipt is not None and not receipt.get('completed'):
        raise RuntimeError(f'incomplete live receipt at {receipt_path}; preserving files')
    if receipt and not journal_path.exists():
        source = receipt.get('source')
        if source:
            source = Path(source).resolve()
            if source == destination or 'paper_state' in source.parts:
                raise RuntimeError(f'invalid live takeover source: {source}')
            leases.enter_context(SingleWriter(source / 'trade.lock'))
        _read_pair(destination)
        return receipt
    journal = _json(journal_path) if journal_path.exists() else None
    if journal and (journal.get('kind') != AUTO_KIND or
                    journal.get('destination') != str(destination)):
        raise RuntimeError(f'unrecognized incomplete migration at {journal_path}; preserving files')
    own_exists = any((destination / name).exists() for name in FILES)
    own = _read_pair(destination) if own_exists and not journal else None
    sources = []
    paths = list(candidates)
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
            leases.enter_context(SingleWriter(item / 'trade.lock'))
        if present:
            sources.append((item, _read_pair(item)))
    groups = {}
    for source, payloads in sources:
        groups.setdefault(tuple(sorted(_hashes(payloads).items())), []).append((source, payloads))
    if len(groups) > 1:
        raise RuntimeError('conflicting legacy execution ledgers detected automatically: ' +
                           ', '.join(str(row[0]) for row in sources) + '; no files overwritten')
    selected = sources[0] if sources else None
    if own is not None:
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
