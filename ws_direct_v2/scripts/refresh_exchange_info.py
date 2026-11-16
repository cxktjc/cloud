"""Download public USD-M symbol filters. Never reads account data or places orders."""
from __future__ import annotations
import argparse
import json
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.request import Request
from collectors import binance_network
from v9711_live import config as cfg
from v9711_live.contracts import atomic_write_json, utc_now_iso

URL = 'https://fapi.binance.com/fapi/v1/exchangeInfo'


def normalize(payload):
    rows = payload.get('symbols') if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        raise ValueError('invalid exchangeInfo: expected symbols list')
    result = {}
    for row in rows:
        if not isinstance(row, dict) or not row.get('symbol'):
            continue
        filters = {f['filterType']: f for f in row.get('filters', [])
                   if isinstance(f, dict) and f.get('filterType')}
        price, lot = filters.get('PRICE_FILTER', {}), filters.get('LOT_SIZE', {})
        notional = filters.get('MIN_NOTIONAL', {})
        if not price or not lot or not notional:
            continue
        values = dict(tick=price.get('tickSize'), step=lot.get('stepSize'),
                      min_qty=lot.get('minQty'), min_notional=notional.get('notional'))
        for name, value in values.items():
            try:
                decimal = Decimal(str(value))
            except InvalidOperation as exc:
                raise ValueError(f'{row["symbol"]}: invalid {name}') from exc
            if not decimal.is_finite() or decimal < 0 or (name in ('tick', 'step') and decimal <= 0):
                raise ValueError(f'{row["symbol"]}: invalid {name}')
            values[name] = str(value)
        result[str(row['symbol']).upper()] = values
    if not result:
        raise ValueError('exchangeInfo contains no usable symbol filters; previous cache retained')
    return {'schema_version': 1, 'source': URL, 'updated_at_utc': utc_now_iso(), 'symbols': result}


def refresh(output):
    request = Request(URL, headers={'User-Agent': 'v9711-filter-sync/1.0', 'Accept': 'application/json'})
    with binance_network.urlopen(request, timeout=20) as response:
        payload = normalize(json.loads(response.read()))
    atomic_write_json(output, payload)
    return payload


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=cfg.EXCHANGE_INFO_CACHE_FILE)
    args = parser.parse_args()
    payload = refresh(args.output)
    print(f'wrote {len(payload["symbols"])} symbol filters to {args.output.resolve()}')
    for symbol in ('AGTUSDT', 'DOSUSDT'):
        print(symbol, payload['symbols'].get(symbol, 'NOT FOUND'))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
