from __future__ import annotations

import unittest

from tools import pyright_money_path_gate as gate


def _diagnostic(
    *,
    file: str = "engine/execution/broker_router.py",
    line: int = 10,
    message: str = "sample diagnostic",
) -> dict[str, object]:
    return {
        "file": file,
        "severity": "error",
        "rule": "reportArgumentType",
        "line": line,
        "character": 4,
        "message": message,
    }


class PyrightMoneyPathGateTests(unittest.TestCase):
    def test_exact_baseline_passes(self) -> None:
        diagnostic = _diagnostic()

        new_diagnostics, resolved_diagnostics = gate._compare_to_baseline(
            {"diagnostics": [diagnostic]},
            [diagnostic],
        )

        self.assertEqual(new_diagnostics, [])
        self.assertEqual(resolved_diagnostics, [])

    def test_new_diagnostic_fails_comparison(self) -> None:
        existing = _diagnostic()
        added = _diagnostic(file="engine/risk/portfolio_risk_engine.py", line=22)

        new_diagnostics, resolved_diagnostics = gate._compare_to_baseline(
            {"diagnostics": [existing]},
            [existing, added],
        )

        self.assertEqual(resolved_diagnostics, [])
        self.assertEqual(len(new_diagnostics), 1)
        self.assertIn("engine/risk/portfolio_risk_engine.py:22", new_diagnostics[0])

    def test_resolved_diagnostic_requires_ratcheted_baseline(self) -> None:
        diagnostic = _diagnostic()

        new_diagnostics, resolved_diagnostics = gate._compare_to_baseline(
            {"diagnostics": [diagnostic]},
            [],
        )

        self.assertEqual(new_diagnostics, [])
        self.assertEqual(len(resolved_diagnostics), 1)
        self.assertIn("engine/execution/broker_router.py:10", resolved_diagnostics[0])

    def test_high_risk_exclusion_guard_blocks_broad_data_glob(self) -> None:
        violations = gate.high_risk_exclusion_violations({"exclude": ["**/data"]})

        self.assertTrue(any("engine/data" in violation for violation in violations))

    def test_high_risk_exclusion_guard_allows_top_level_artifact_data(self) -> None:
        violations = gate.high_risk_exclusion_violations({"exclude": ["data/**", "logs/**"]})

        self.assertEqual(violations, [])

    def test_target_scope_covers_money_path_roots(self) -> None:
        self.assertIn("engine/data/live_prices/ccxt_live.py", gate.TARGET_PATHS)
        self.assertIn("engine/execution/broker_router.py", gate.TARGET_PATHS)
        self.assertIn("engine/risk/portfolio_risk_engine.py", gate.TARGET_PATHS)
        self.assertIn("engine/runtime/prod_preflight.py", gate.TARGET_PATHS)
        self.assertIn("engine/strategy/portfolio.py", gate.TARGET_PATHS)


if __name__ == "__main__":
    unittest.main()
