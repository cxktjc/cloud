"""Cheap local input detection; no exchange, database or strategy dependencies.

Probe file identity/timestamps at high frequency. Parse only changed revisions;
callers must still revalidate TTL/quote freshness against their current clock.
Periodic maintenance is independent of change detection so silence cannot stop
expiry, model catchup, exact-fill reconciliation or timed exits.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Iterable


def _stamp(stat: os.stat_result) -> tuple[int, ...]:
    return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)


def file_stamp(path: Path) -> tuple[int, ...] | None:
    try:
        return _stamp(os.stat(path))
    except OSError:
        return None


class CachedJsonFile:
    """Cache decoded JSON by file stamp, including atomic replacement identity.

    Never serve the old payload when the path disappears or changes to invalid
    JSON. Cache invalid content too (until its stamp changes); transient OS
    errors retry on the next read. A replacement during a read is retried rather
    than labelling old bytes with the new file's stamp. Payloads are read-only
    to callers; a consumer needing mutable nested values must copy them.
    """

    def __init__(self, path: Path):
        self.path = Path(path)
        self._stamp: object = object()
        self.payload: Any = None
        self.error = ''
        self.revision = 0

    def read(self) -> Any:
        stamp = file_stamp(self.path)
        if stamp == self._stamp:
            return self.payload
        self.revision += 1
        self.payload = None
        self.error = ''
        self._stamp = stamp
        if stamp is None:
            self.error = 'local JSON missing or inaccessible'
            return None
        try:
            with self.path.open('r', encoding='utf-8') as handle:
                payload = json.load(handle)
                opened_stamp = _stamp(os.fstat(handle.fileno()))
            if opened_stamp != stamp or file_stamp(self.path) != stamp:
                self._stamp = object()  # retry an atomic-replace/in-place-write race
                self.error = 'local JSON changed during read; retrying'
                return None
            self.payload = payload
        except (ValueError, TypeError, UnicodeError) as exc:
            self.error = f'{type(exc).__name__}: {exc}'
        except OSError as exc:
            self._stamp = object()  # permissions/sharing violations can be transient
            self.error = f'{type(exc).__name__}: {exc}'
        return self.payload


class LocalChangePoller:
    """Fast stat-only probes plus bounded, monotonic-clock maintenance wakes."""

    def __init__(self, paths: Iterable[Path], *, maintenance_seconds: float):
        self.paths = tuple(Path(path) for path in paths)
        self.maintenance_seconds = max(0.005, float(maintenance_seconds))
        self._stamps: tuple | None = None
        self._maintenance_at = float('-inf')

    def due(self, *, now: float | None = None) -> bool:
        now = time.monotonic() if now is None else float(now)
        stamps = tuple(file_stamp(path) for path in self.paths)
        changed = stamps != self._stamps
        self._stamps = stamps
        if now >= self._maintenance_at:
            self._maintenance_at = now + self.maintenance_seconds
            return True
        return changed


def probe_sleep(interval: float, started: float) -> float:
    """Do not restore the old 50ms floor or busy-spin on a slow iteration."""
    return max(0.001, max(0.005, float(interval)) - (time.monotonic() - started))
