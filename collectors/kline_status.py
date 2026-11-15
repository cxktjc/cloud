"""Small atomic file-based handoff from kline writers to live signal runners."""

from __future__ import annotations

import json
import zlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__import__("os").environ.get("V9711_DEPLOY_ROOT", str(Path(__file__).resolve().parents[1]))).expanduser().resolve()
DEFAULT_STATUS_FILE = REPO_ROOT / "var" / "klines_15m_status.json"
DEFAULT_3M_STATUS_FILE = REPO_ROOT / "var" / "klines_3m_status.json"


@dataclass(frozen=True)
class Kline15mStatus:
    open_time_ms: int
    open_time_utc: str
    symbol_count: int
    row_count: int
    symbols: tuple[str, ...]
    updated_at: str
    source: str

    @property
    def signature(self) -> tuple[int, int, int, int]:
        payload = "|".join(
            [
                str(self.open_time_ms),
                str(self.symbol_count),
                str(self.row_count),
                ",".join(self.symbols),
                self.updated_at,
                self.source,
            ]
        )
        checksum = zlib.crc32(payload.encode("utf-8")) & 0xFFFFFFFF
        return self.open_time_ms, self.symbol_count, self.row_count, checksum


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def ms_to_iso(open_time_ms: int) -> str:
    return datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc).isoformat(timespec="seconds")


def write_15m_status(
    *,
    open_time_ms: int,
    symbols: list[str],
    row_count: int,
    source: str,
    path: Path = DEFAULT_STATUS_FILE,
) -> Kline15mStatus:
    normalized_symbols = tuple(sorted({symbol.strip().upper() for symbol in symbols if symbol.strip()}))
    status = Kline15mStatus(
        open_time_ms=int(open_time_ms),
        open_time_utc=ms_to_iso(int(open_time_ms)),
        symbol_count=len(normalized_symbols),
        row_count=int(row_count),
        symbols=normalized_symbols,
        updated_at=utc_now_iso(),
        source=source,
    )
    payload: dict[str, Any] = {
        "schema_version": 1,
        "open_time_ms": status.open_time_ms,
        "open_time_utc": status.open_time_utc,
        "symbol_count": status.symbol_count,
        "row_count": status.row_count,
        "symbols": list(status.symbols),
        "updated_at": status.updated_at,
        "source": status.source,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)
    return status


def read_15m_status(path: Path = DEFAULT_STATUS_FILE) -> Kline15mStatus | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return None

    try:
        symbols = tuple(sorted({str(symbol).strip().upper() for symbol in payload.get("symbols", []) if str(symbol).strip()}))
        return Kline15mStatus(
            open_time_ms=int(payload["open_time_ms"]),
            open_time_utc=str(payload.get("open_time_utc") or ms_to_iso(int(payload["open_time_ms"]))),
            symbol_count=int(payload.get("symbol_count") or len(symbols)),
            row_count=int(payload.get("row_count") or 0),
            symbols=symbols,
            updated_at=str(payload.get("updated_at") or ""),
            source=str(payload.get("source") or "unknown"),
        )
    except (KeyError, TypeError, ValueError):
        return None


# The payload and completeness metadata are interval-independent.  Keep the
# existing 15m API stable and expose the 3m names expected by live runners.
Kline3mStatus = Kline15mStatus


def write_3m_status(
    *,
    open_time_ms: int,
    symbols: list[str],
    row_count: int,
    source: str,
    path: Path = DEFAULT_3M_STATUS_FILE,
) -> Kline3mStatus:
    return write_15m_status(
        open_time_ms=open_time_ms,
        symbols=symbols,
        row_count=row_count,
        source=source,
        path=path,
    )


def read_3m_status(path: Path = DEFAULT_3M_STATUS_FILE) -> Kline3mStatus | None:
    return read_15m_status(path)
