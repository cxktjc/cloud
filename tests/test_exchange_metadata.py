from __future__ import annotations
import json
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch, Mock
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from v9711_live.execution.client import ExchangeClient
from scripts import refresh_exchange_info as refresh


def filters(step='1', tick='0.00000100'):
    return {'symbols': [{'symbol': 'TESTUSDT', 'pricePrecision': 8, 'quantityPrecision': 6,
             'filters': [{'filterType': 'PRICE_FILTER', 'tickSize': tick},
                         {'filterType': 'LOT_SIZE', 'stepSize': step, 'minQty': '1'},
                         {'filterType': 'MIN_NOTIONAL', 'notional': '5'}]}]}


class ExchangeMetadataTests(unittest.TestCase):
    def test_missing_or_unknown_metadata_never_guesses(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'cache.json'
            client = ExchangeClient(enabled=False, exchange_info_cache_file=path)
            with self.assertRaisesRegex(ValueError, 'metadata missing'):
                client.prepare_limit('TESTUSDT', .01, 5)
            path.write_text(json.dumps({'symbols': {}}))
            with self.assertRaisesRegex(ValueError, 'metadata missing'):
                client.prepare_limit('TESTUSDT', .01, 5)

    def test_real_filters_override_precision_fields_and_integer_qty(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'cache.json'
            path.write_text(json.dumps(refresh.normalize(filters())))
            client = ExchangeClient(enabled=False, exchange_info_cache_file=path)
            prepared = client.prepare_limit('TESTUSDT', .0170041, 4.9)
            self.assertTrue(prepared['ok'])
            self.assertEqual(prepared['price'], '0.017004')
            self.assertNotIn('.', prepared['qty'])
            self.assertGreaterEqual(Decimal(prepared['qty']) * Decimal(prepared['price']), Decimal('5'))

    def test_decimal_alignment_and_cache_refresh(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'cache.json'
            path.write_text(json.dumps(refresh.normalize(filters(step='.005', tick='.000005'))))
            client = ExchangeClient(enabled=False, exchange_info_cache_file=path)
            self.assertEqual(client.price_text('TESTUSDT', .0170041), '0.017')
            prepared = client.prepare_limit('TESTUSDT', .0170041, 5)
            self.assertEqual(Decimal(prepared['qty']) % Decimal('.005'), 0)
            path.write_text(json.dumps(refresh.normalize(filters(step='1', tick='.001'))))
            self.assertEqual(client.price_text('TESTUSDT', .0170041), '0.017')
            self.assertEqual(client._symbol_meta('TESTUSDT')['step'], 1)

    def test_invalid_download_preserves_previous_cache(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'cache.json'
            path.write_text('previous-cache')
            response = Mock()
            response.read.return_value = b'{"code": -1, "msg": "failed"}'
            response.__enter__ = Mock(return_value=response)
            response.__exit__ = Mock(return_value=False)
            with patch.object(sys, 'argv', ['refresh', '--output', str(path)]), \
                 patch.object(refresh.binance_network, 'urlopen', return_value=response):
                with self.assertRaises(ValueError):
                    refresh.main()
            self.assertEqual(path.read_text(), 'previous-cache')


if __name__ == '__main__':
    unittest.main()
