#!/usr/bin/env python3
"""Compatibility entry point: price collection is now WebSocket based.

The historical REST command accepted ``--interval``. The standalone server
ignores that legacy cadence and uses the WS collector's coalescing interval
(default 100ms); use ``--publish-interval`` for the new setting.
"""
from __future__ import annotations
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Keep old operational commands from failing while making clear that REST is
# no longer used. Drop only the obsolete REST cadence flag.
args = sys.argv[1:]
filtered: list[str] = []
i = 0
while i < len(args):
    if args[i] == "--interval":
        i += 2
        continue
    filtered.append(args[i])
    i += 1
sys.argv = [sys.argv[0], *filtered]

from collectors.ticker_ws import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
