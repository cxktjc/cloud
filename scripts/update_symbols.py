#!/usr/bin/env python3
"""Refresh the local perpetual-USDT symbol list from Binance exchangeInfo."""
from __future__ import annotations
import argparse, json, os, tempfile
from pathlib import Path
from urllib.request import Request
import sys

ROOT_PATH = Path(__file__).resolve().parents[1]
if str(ROOT_PATH) not in sys.path:
    sys.path.insert(0, str(ROOT_PATH))
from collectors import binance_network

ROOT = Path(os.environ.get("V9711_DEPLOY_ROOT", str(Path(__file__).resolve().parents[1]))).resolve()
URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=ROOT / "config" / "symbols.txt")
    parser.add_argument("--from-status", type=Path, help="Copy symbols from an existing klines_3m status JSON instead of exchangeInfo.")
    args = parser.parse_args()
    if args.from_status:
        symbols = json.loads(args.from_status.read_text(encoding="utf-8")).get("symbols", [])
    else:
        req = Request(URL, headers={"User-Agent": "v9711-live-symbol-sync/1.0", "Accept": "application/json"})
        with binance_network.urlopen(req, timeout=15) as response:
            data = json.loads(response.read())
        symbols = [row["symbol"] for row in data.get("symbols", [])
                   if row.get("status") == "TRADING" and row.get("contractType") == "PERPETUAL"
                   and row.get("quoteAsset") == "USDT"]
    symbols = sorted({str(x).strip().upper() for x in symbols if str(x).strip()})
    if not symbols:
        raise SystemExit("no symbols found")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=args.output.name + ".", dir=args.output.parent)
    with open(fd, "w", encoding="utf-8", closefd=True) as handle:
        handle.write("# v9711 live strategy universe; one Binance USD-M symbol per line\n")
        handle.write("\n".join(symbols) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, args.output)
    print(f"wrote {len(symbols)} symbols to {args.output}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
