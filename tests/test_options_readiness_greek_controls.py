from __future__ import annotations

import importlib
from pathlib import Path
import sys
import unittest
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class OptionsReadinessGreekControlsTest(unittest.TestCase):
    def test_gamma_and_vega_controls_are_exposed_with_single_live_adapter(self) -> None:
        readiness = importlib.reload(importlib.import_module("engine.execution.options_readiness"))

        names = [control[0] for control in readiness.NUMERIC_CONTROLS]

        self.assertIn("OPTIONS_MAX_PORTFOLIO_GAMMA_ABS", names)
        self.assertIn("OPTIONS_MAX_PORTFOLIO_VEGA_ABS", names)
        self.assertEqual(readiness.LIVE_OPTIONS_BROKER_ADAPTERS, frozenset({"tradier_options"}))

    def test_is_options_order_semantics_remain_unchanged(self) -> None:
        readiness = importlib.reload(importlib.import_module("engine.execution.options_readiness"))

        self.assertTrue(
            readiness.is_options_order(
                {
                    "symbol": "SPY270115C00500000",
                    "option_contract": "SPY270115C00500000",
                    "instrument_type": "option",
                }
            )
        )
        self.assertFalse(readiness.is_options_order({"symbol": "SPY", "instrument_type": "equity"}))

    def test_greek_snapshot_reports_missing_multiplier_without_provenance(self) -> None:
        readiness = importlib.reload(importlib.import_module("engine.execution.options_readiness"))

        class ParsedWithoutProvenance:
            multiplier = 100.0
            multiplier_source = ""
            contract_specs_verified = False

        order = {
            "instrument_type": "option",
            "option_contract": "SPY270115C00500000",
            "underlying": "SPY",
            "expiration": "2027-01-15",
            "contract_type": "call",
            "strike": 500.0,
            "qty": 1,
            "delta": 0.5,
            "gamma": 0.01,
            "theta": -0.02,
            "vega": 0.2,
        }

        with patch.object(readiness, "parse_option_symbol", return_value=ParsedWithoutProvenance()):
            snapshot = readiness._option_order_greek_snapshot([order])

        self.assertEqual(snapshot["missing_multiplier"], ["SPY270115C00500000"])
        self.assertEqual(snapshot["by_symbol"], {})
        self.assertEqual(snapshot["net_delta"], 0.0)


if __name__ == "__main__":
    unittest.main()
