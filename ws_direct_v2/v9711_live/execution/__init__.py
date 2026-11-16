"""Execution-side feedback reader and CLI helpers."""
from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any

from .. import config as cfg
from ..contracts import ExecutionFeedback, atomic_write_json, utc_now_iso

FEEDBACK_TEMPLATE: dict[str, Any] = {
    "schema_version": 1,
    "component": "execution",
    "updated_at_utc": "",
    "updated_at_epoch": 0,
    "realized_balance": cfg.EQUITY0,
    "positions": {},
    "open_orders": {},
    "note": "Executor-owned state: positions, exchange order ids, fills, PnL and cancel/place acknowledgements.",
}

def load_feedback(path: Path = cfg.EXECUTION_FEEDBACK_FILE) -> ExecutionFeedback:
    return ExecutionFeedback.load(Path(path), cfg.EXECUTION_FEEDBACK_MAX_AGE_SECONDS)

def write_template(path: Path = cfg.EXECUTION_FEEDBACK_FILE) -> None:
    payload = dict(FEEDBACK_TEMPLATE)
    payload["updated_at_utc"] = utc_now_iso()
    payload["updated_at_epoch"] = time.time()
    atomic_write_json(Path(path), payload)

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=Path, default=cfg.EXECUTION_FEEDBACK_FILE)
    parser.add_argument("--write-template", action="store_true")
    parser.add_argument("--show", action="store_true")
    args = parser.parse_args()
    if args.write_template:
        write_template(args.path)
    if args.show or not args.write_template:
        fb = load_feedback(args.path)
        print(json.dumps({"fresh": fb.fresh, "positions": len(fb.positions),
                          "open_orders": len(fb.open_orders),
                          "realized_balance": fb.realized_balance,
                          "cumulative_pnl": fb.cumulative_pnl,
                          "current_unrealized_pnl": fb.current_unrealized_pnl,
                          "updated_at_epoch": fb.updated_at_epoch if math.isfinite(fb.updated_at_epoch) else None},
                         ensure_ascii=False, indent=2))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
