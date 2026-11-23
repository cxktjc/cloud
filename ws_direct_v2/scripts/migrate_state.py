"""Copy stopped legacy ledgers into new version, never delete/move source files.

Default is read-only preview. --apply takes both legacy and new process locks,
refuses existing destination ledgers and leaves an incomplete marker on error.
"""
from __future__ import annotations
import argparse
from contextlib import ExitStack
import hashlib
import json
from pathlib import Path
from v9711_live import config as cfg
from v9711_live.contracts import atomic_write_json, utc_now_iso
from v9711_live.storage import SingleWriter

FILES = ('execution_intents.json', 'execution_ledger.json', 'signal_portfolio.json')


def migrate(source, destination, *, apply=False):
    source, destination = Path(source).resolve(), Path(destination).resolve()
    if source == destination or source.is_relative_to(destination) or destination.is_relative_to(source):
        raise ValueError('source and destination must be disjoint state directories')
    for name in FILES[:2]:
        if not (source/name).is_file():
            raise ValueError('missing legacy ledger: '+str(source/name))
    existing = [name for name in FILES if (destination/name).exists()]
    if existing or (destination/'WS_MIGRATION.json').exists() or (destination/'.migration_in_progress').exists():
        raise ValueError('destination already has state/incomplete migration; refusing overwrite: '+str(existing))
    if not apply:
        return dict(applied=False, source=str(source), destination=str(destination),
                    files=[name for name in FILES if (source/name).exists()])
    with ExitStack() as stack:
        for directory in (source, destination):
            for name in ('signal.lock','trade.lock'):
                stack.enter_context(SingleWriter(directory/name))
        # Revalidate under locks; take a consistent stopped-process snapshot.
        payloads = {}
        for name in FILES:
            path = source/name
            if path.exists():
                raw = path.read_bytes()
                data = json.loads(raw.decode('utf-8-sig'))
                if not isinstance(data, dict):
                    raise ValueError('invalid ledger: '+str(path))
                payloads[name] = raw
        if not isinstance(json.loads(payloads['execution_ledger.json']).get('state'), dict):
            raise ValueError('legacy execution ledger has no state object')
        # Export the current complete bundle, never copy an epoch marker
        # without its slot files or revive the older compatibility intents.
        from v9711_live.execution.durable_writer import resolve_store, embedded_intents
        execution = resolve_store(json.loads(payloads['execution_ledger.json']), source/'execution_ledger.json')
        canonical = embedded_intents(execution)
        if canonical is not None:
            payloads['execution_ledger.json'] = json.dumps(execution, ensure_ascii=False, allow_nan=False).encode('utf-8')
            payloads['execution_intents.json'] = json.dumps(dict(schema_version=1, component='execution',
                intents=canonical), ensure_ascii=False, allow_nan=False).encode('utf-8')
        if any((destination/name).exists() for name in FILES):
            raise ValueError('destination changed during migration; refusing overwrite')
        marker = destination/'.migration_in_progress'
        marker.write_text(utc_now_iso(), encoding='utf-8')
        for name, raw in payloads.items():
            with (destination/name).open('xb') as handle:
                handle.write(raw)
                handle.flush()
                __import__('os').fsync(handle.fileno())
        result = dict(completed=True, applied=True, source=str(source), destination=str(destination),
                      migrated_at_utc=utc_now_iso(), source_sha256={name:hashlib.sha256(raw).hexdigest()
                                                                  for name,raw in payloads.items()})
        atomic_write_json(destination/'WS_MIGRATION.json', result)
        marker.unlink()
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    print(json.dumps(migrate(args.source, cfg.STATE_DIR, apply=args.apply), indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
