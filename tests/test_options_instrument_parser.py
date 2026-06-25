from __future__ import annotations

from datetime import date
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from engine.data import options_instrument
from engine.data.options_instrument import is_option_symbol, parse_option_symbol
from engine.execution.options_readiness import _OCC_COMPACT_RE


class OptionsInstrumentParserTests(unittest.TestCase):
    def test_compact_and_polygon_prefixed_symbols_parse_to_same_metadata(self) -> None:
        compact = parse_option_symbol("SPY240920C00450000")
        prefixed = parse_option_symbol("O:SPY240920C00450000")

        self.assertIsNotNone(compact)
        self.assertIsNotNone(prefixed)
        assert compact is not None
        assert prefixed is not None
        self.assertEqual(compact, prefixed)
        self.assertEqual(compact.underlying, "SPY")
        self.assertEqual(compact.right, "C")
        self.assertEqual(compact.strike, 450.0)
        self.assertEqual(compact.expiry, date(2024, 9, 20))
        self.assertEqual(compact.asset_class, "OPTION")
        self.assertEqual(compact.occ_symbol, "SPY240920C00450000")
        self.assertFalse(compact.contract_specs_verified)
        self.assertEqual(compact.multiplier_source, "parser_default_unverified")
        self.assertEqual(compact.contract_spec_source, "parser_default_unverified")

    def test_to_dict_keys_are_sorted_and_expiry_is_iso(self) -> None:
        metadata = parse_option_symbol("SPY240920C00450000")

        self.assertIsNotNone(metadata)
        assert metadata is not None
        payload = metadata.to_dict()
        self.assertEqual(list(payload), sorted(payload))
        self.assertEqual(payload["expiry"], "2024-09-20")
        self.assertEqual(payload["strike"], 450.0)
        self.assertEqual(payload["contract_specs_verified"], False)
        self.assertEqual(payload["multiplier_source"], "parser_default_unverified")
        self.assertEqual(payload["contract_spec_source"], "parser_default_unverified")

    def test_invalid_inputs_return_none_without_raising(self) -> None:
        for symbol in ("", None, "SPY", "EURUSD", "SPY240920C00ABC000", "SPY241320C00450000"):
            with self.subTest(symbol=symbol):
                self.assertIsNone(parse_option_symbol(symbol))
                self.assertFalse(is_option_symbol(symbol))

    def test_occ_symbol_round_trips(self) -> None:
        metadata = parse_option_symbol("O:SPY240920C00450000")

        self.assertIsNotNone(metadata)
        assert metadata is not None
        self.assertEqual(parse_option_symbol(metadata.occ_symbol), metadata)

    def test_occ_regex_matches_readiness_regex(self) -> None:
        self.assertEqual(options_instrument.OCC_COMPACT_RE.pattern, _OCC_COMPACT_RE.pattern)


if __name__ == "__main__":
    unittest.main()
