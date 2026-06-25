from __future__ import annotations

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from engine.data.futures_instrument import is_futures_symbol, parse_futures_symbol


class FuturesInstrumentParserTests(unittest.TestCase):
    def test_parse_continuous_es_metadata(self) -> None:
        metadata = parse_futures_symbol("ES.c.0")

        self.assertIsNotNone(metadata)
        assert metadata is not None
        self.assertEqual(metadata.symbol, "ES.c.0")
        self.assertEqual(metadata.asset_class, "FUTURES")
        self.assertEqual(metadata.instrument_kind, "fut_continuous")
        self.assertEqual(metadata.root, "ES")
        self.assertEqual(metadata.exchange, "CME")
        self.assertEqual(metadata.multiplier, 50.0)
        self.assertEqual(metadata.tick_size, 0.25)
        self.assertEqual(metadata.tick_value, 12.50)
        self.assertEqual(metadata.price_ccy, "USD")
        self.assertEqual(metadata.continuous_alias, "ES.c.0")
        self.assertEqual(metadata.roll_method, "oi_volume")
        self.assertEqual(metadata.source, "parser")

    def test_parse_dated_es_metadata(self) -> None:
        metadata = parse_futures_symbol("ESZ26")

        self.assertIsNotNone(metadata)
        assert metadata is not None
        self.assertEqual(metadata.symbol, "ESZ26")
        self.assertEqual(metadata.instrument_kind, "fut_dated")
        self.assertEqual(metadata.root, "ES")
        self.assertIsNone(metadata.continuous_alias)

    def test_parse_cl_contract_specs(self) -> None:
        metadata = parse_futures_symbol("CL.c.0")

        self.assertIsNotNone(metadata)
        assert metadata is not None
        self.assertEqual(metadata.root, "CL")
        self.assertEqual(metadata.exchange, "NYMEX")
        self.assertEqual(metadata.multiplier, 1000.0)
        self.assertEqual(metadata.tick_size, 0.01)
        self.assertEqual(metadata.tick_value, 10.0)

    def test_bare_roots_do_not_parse(self) -> None:
        self.assertIsNone(parse_futures_symbol("ES"))
        self.assertIsNone(parse_futures_symbol("GC"))

    def test_non_futures_do_not_parse(self) -> None:
        for symbol in ("SPY", "EURUSD", "", None):
            with self.subTest(symbol=symbol):
                self.assertIsNone(parse_futures_symbol(symbol))
                self.assertFalse(is_futures_symbol(symbol))

    def test_is_futures_symbol_agrees_with_parser(self) -> None:
        for symbol in ("ES.c.0", "ESZ26", "CLM26", "MGCZ26", "SPY", "GC", None):
            with self.subTest(symbol=symbol):
                self.assertIs(is_futures_symbol(symbol), parse_futures_symbol(symbol) is not None)


if __name__ == "__main__":
    unittest.main()
