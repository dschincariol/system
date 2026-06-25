from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from engine.execution import options_readiness


class OptionsReadinessConsumesParserTests(unittest.TestCase):
    def test_existing_options_order_truth_table_is_unchanged(self) -> None:
        cases = (
            ({"option_symbol": "SPY240920C00450000"}, True),
            ({"contract_type": "call", "strike": 450}, True),
            ({"symbol": "SPY", "side": "buy", "qty": 1}, False),
            (["not", "a", "mapping"], False),
        )

        for order, expected in cases:
            with self.subTest(order=order):
                self.assertIs(options_readiness.is_options_order(order), expected)

    def test_polygon_prefixed_contract_is_now_recognized(self) -> None:
        self.assertTrue(options_readiness.is_options_order({"option_symbol": "O:SPY240920C00450000"}))

    def test_option_order_metadata_uses_parser_only_for_missing_fields(self) -> None:
        parsed = options_readiness.option_order_metadata({"option_symbol": "O:SPY240920C00450000"})
        self.assertEqual(parsed["underlying"], "SPY")
        self.assertEqual(parsed["expiration"], "2024-09-20")
        self.assertEqual(parsed["contract_type"], "call")
        self.assertEqual(parsed["strike"], 450.0)

        explicit = options_readiness.option_order_metadata(
            {
                "option_symbol": "O:SPY240920C00450000",
                "underlying": "QQQ",
                "expiration": "2025-01-17",
                "contract_type": "put",
                "strike": 300,
            }
        )
        self.assertEqual(explicit["underlying"], "QQQ")
        self.assertEqual(explicit["expiration"], "2025-01-17")
        self.assertEqual(explicit["contract_type"], "put")
        self.assertEqual(explicit["strike"], 300.0)

    def test_fail_closed_defaults_remain_unchanged(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(options_readiness.LIVE_OPTIONS_BROKER_ADAPTERS, frozenset({"tradier_options"}))
            self.assertEqual(options_readiness.options_instruments_mode(), "shadow")


if __name__ == "__main__":
    unittest.main()
