"""Manual ONE-TIME signal-to-trade ledger import; never submits exchange orders.

Stop signal and trade. Preview first; add --apply to back up and replace the
trade accounting baseline. Restart normally WITHOUT this command afterwards.
"""
import argparse
import json
from v9711_live import config as cfg
from v9711_live.execution.manual_sync import parse_prices, sync


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true', help='Back up and import once; otherwise preview only.')
    parser.add_argument('--price', action='append', default=[], metavar='SYMBOL=PRICE',
                        help='Override imported entry price; preserve signal notional by recalculating quantity.')
    parser.add_argument('--symbol', action='append', default=None, metavar='SYMBOL',
                        help='Import only selected signal holdings (repeatable); default all, subject to trade cap.')
    parser.add_argument('--paper', action='store_true', help='Use isolated paper_state instead of live state.')
    args = parser.parse_args()
    try:
        result = sync(cfg.STATE_DIR, prices=parse_prices(args.price), symbols=args.symbol,
                      apply=args.apply, paper=args.paper)
    except (OSError, ValueError, RuntimeError, KeyError, TypeError) as exc:
        parser.error(str(exc))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
