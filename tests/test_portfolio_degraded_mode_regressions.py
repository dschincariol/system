from __future__ import annotations

import importlib
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class PortfolioDegradedModeRegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "portfolio_degraded.db"
        os.environ["DB_PATH"] = str(self.db_path)
        _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
        )

    def tearDown(self) -> None:
        try:
            storage = importlib.import_module("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception:
            pass
        self.tmp.cleanup()

    def _executescript(self, script: str) -> None:
        con = sqlite3.connect(str(self.db_path))
        try:
            con.executescript(script)
            con.commit()
        finally:
            con.close()

    def test_portfolio_meta_cache_isolated_by_database(self) -> None:
        _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
        )
        portfolio = importlib.reload(importlib.import_module("engine.strategy.portfolio"))

        other_db_path = Path(self.tmp.name) / "portfolio_other.db"
        for path in (self.db_path, other_db_path):
            con = sqlite3.connect(str(path))
            try:
                con.executescript(portfolio.SCHEMA)
                con.commit()
            finally:
                con.close()

        con_a = sqlite3.connect(str(self.db_path))
        con_b = sqlite3.connect(str(other_db_path))
        try:
            portfolio._set_meta(con_a, "last_rebalance_ts_ms", "111")
            con_a.commit()

            value_a = portfolio._get_meta(con_a, "last_rebalance_ts_ms")
            value_b = portfolio._get_meta(con_b, "last_rebalance_ts_ms")
        finally:
            con_a.close()
            con_b.close()

        self.assertEqual(value_a, "111")
        self.assertIsNone(value_b)

    def test_compute_rebalance_surfaces_degraded_phases(self) -> None:
        storage, portfolio, strategy_allocator, capital_guard, health = _reload_modules(
            "engine.runtime.storage",
            "engine.strategy.portfolio",
            "engine.runtime.strategy_allocator",
            "engine.strategy.capital_guard",
            "engine.runtime.health",
        )
        self._executescript(portfolio.SCHEMA)

        explain = json.dumps(
            {
                "model_id": "m1",
                "model_name": "model_one",
                "confidence": 0.9,
                "model_intent": {"score": 1.0},
            },
            separators=(",", ":"),
            sort_keys=True,
        )
        modules = {
            "s1": SimpleNamespace(
                build_desired=lambda alerts, now_ms: {
                    "AAPL": {
                        "symbol": "AAPL",
                        "side": "LONG",
                        "weight": 0.20,
                        "source_alert_id": 101,
                        "reason": {"confidence": 0.9, "score": 1.0, "expected_z": 1.2},
                        "confidence": 0.9,
                        "explain_json": explain,
                    }
                }
            )
        }

        with ExitStack() as stack:
            stack.enter_context(patch.object(capital_guard, "trading_allowed", return_value=True))
            stack.enter_context(
                patch.object(
                    strategy_allocator,
                    "compute_and_persist_strategy_allocations",
                    side_effect=RuntimeError("allocator exploded"),
                )
            )
            stack.enter_context(
                patch.object(
                    portfolio,
                    "_load_strategy_efficiency",
                    return_value={"s1": {"efficiency_score": 1.0}},
                )
            )
            stack.enter_context(patch.object(portfolio, "_load_recent_alert_candidates", return_value=[]))
            stack.enter_context(patch.object(portfolio, "_load_live_strategies", return_value=["s1"]))
            stack.enter_context(patch.object(portfolio, "_load_shadow_strategies", return_value=[]))
            stack.enter_context(patch.object(portfolio, "load_strategy_module", side_effect=lambda name: modules[str(name)]))
            stack.enter_context(patch.object(portfolio, "_optimize_capital_allocation", side_effect=lambda con, desired: desired))
            stack.enter_context(patch.object(portfolio, "_apply_impact_aware_sizing", side_effect=lambda con, desired: desired))
            stack.enter_context(
                patch.object(portfolio, "_apply_model_diversification_scoring", side_effect=lambda con, desired: (desired, {}))
            )
            stack.enter_context(
                patch.object(
                    portfolio,
                    "apply_portfolio_risk_engine",
                    side_effect=RuntimeError("risk engine exploded"),
                )
            )
            stack.enter_context(
                patch.object(portfolio, "apply_portfolio_risk_gate", side_effect=lambda con, desired, state, now_ms: (desired, {}))
            )
            stack.enter_context(patch.object(portfolio, "_apply_temporal_dampener", side_effect=lambda con, desired, now_ms: desired))
            stack.enter_context(patch.object(portfolio, "_apply_capital_at_risk_gate", side_effect=lambda desired: (desired, {})))
            stack.enter_context(
                patch.object(portfolio, "_apply_same_direction_exposure_netting", side_effect=lambda con, desired: (desired, {}))
            )
            stack.enter_context(
                patch.object(portfolio, "_apply_total_portfolio_risk_limit", side_effect=lambda con, desired: (desired, {}))
            )
            stack.enter_context(patch.object(portfolio, "_build_portfolio_correlation_diagnostics", return_value={}))
            stack.enter_context(patch.object(portfolio, "_persist_portfolio_correlation_diagnostics", return_value=None))
            stack.enter_context(patch.object(portfolio, "request_monte_carlo_refresh", return_value=None))
            stack.enter_context(patch.object(portfolio, "is_blacklisted", return_value=False))
            stack.enter_context(patch.object(portfolio, "PORTFOLIO_ALLOC_OPT", False))
            stack.enter_context(patch.object(portfolio, "PORTFOLIO_CORR_OPT", False))
            stack.enter_context(patch.object(portfolio, "PORTFOLIO_USE_VOL_TARGET", False, create=True))
            stack.enter_context(patch.object(portfolio, "PORTFOLIO_USE_STRESS_GATE", False))
            stack.enter_context(patch.object(portfolio, "PORTFOLIO_USE_SOCIAL_GATE", False))
            stack.enter_context(patch.object(portfolio, "PORTFOLIO_USE_VOV_GATE", False))
            stack.enter_context(patch.object(portfolio, "PORTFOLIO_USE_EXEC_REALISM", False))
            stack.enter_context(patch.object(portfolio, "PORTFOLIO_USE_EXEC_REGIME", False))

            result = portfolio.compute_rebalance()

        self.assertTrue(result.get("ok"), result)
        self.assertTrue(bool(result.get("execution_blocked")))
        self.assertIn("PORTFOLIO_STRATEGY_ALLOCATOR_FAILED", list(result.get("execution_blocked_codes") or []))
        self.assertIn("PORTFOLIO_RISK_ENGINE_FAILED", list(result.get("execution_blocked_codes") or []))
        self.assertEqual(int(result.get("orders_n") or 0), 0)
        self.assertEqual(list(result.get("changed") or []), [])
        diagnostics = dict(result.get("portfolio_diagnostics") or {})
        self.assertTrue(bool(diagnostics.get("degraded")))
        self.assertTrue(bool(diagnostics.get("execution_blocked")))
        reasons = list(diagnostics.get("degraded_reasons") or [])
        codes = {str(item.get("code") or "") for item in reasons}
        self.assertIn("PORTFOLIO_STRATEGY_ALLOCATOR_FAILED", codes)
        self.assertIn("PORTFOLIO_RISK_ENGINE_FAILED", codes)

        con = storage.connect(readonly=True)
        try:
            order_rows = con.execute("SELECT COUNT(*) FROM portfolio_orders").fetchone()
            state_rows = con.execute("SELECT COUNT(*) FROM portfolio_state").fetchone()
        finally:
            con.close()
        self.assertEqual(int(order_rows[0] or 0), 0)
        self.assertEqual(int(state_rows[0] or 0), 0)

        snapshot = health._portfolio_runtime_snapshot()
        self.assertTrue(bool(snapshot.get("degraded")))
        self.assertTrue(bool(snapshot.get("execution_blocked")))
        self.assertEqual(int(snapshot.get("orders_n") or 0), 0)
        self.assertIn("PORTFOLIO_STRATEGY_ALLOCATOR_FAILED", list(snapshot.get("execution_blocked_codes") or []))
        self.assertIn("PORTFOLIO_RISK_ENGINE_FAILED", list(snapshot.get("execution_blocked_codes") or []))

    def test_overlay_degraded_codes_short_circuit_rebalance_execution(self) -> None:
        (portfolio,) = _reload_modules("engine.strategy.portfolio")
        blocked_codes = [
            "PORTFOLIO_VOL_TARGET_FAILED",
            "PORTFOLIO_CAPITAL_AT_RISK_FAILED",
            "PORTFOLIO_TEMPORAL_DAMPENER_FAILED",
            "PORTFOLIO_EXPOSURE_NETTING_FAILED",
        ]

        with ExitStack() as stack:
            stack.enter_context(patch.object(portfolio, "_expire_stale_unconsumed_alerts", return_value=0))
            stack.enter_context(patch.object(portfolio, "_set_meta", return_value=None))
            stack.enter_context(patch.object(portfolio, "_persist_portfolio_runtime_health", return_value=None))

            for code in blocked_codes:
                with self.subTest(code=code):
                    ctx = portfolio._RebalanceContext(con=object(), now_ms=1_700_000_000_000)
                    ctx.desired = {"AAPL": {"symbol": "AAPL", "side": "LONG", "weight": 0.10}}
                    ctx.changed = ["AAPL"]
                    ctx.orders_n = 1
                    ctx.record_degraded_phase("unit_test", code, RuntimeError("injected overlay failure"))

                    result = portfolio._apply_rebalance_execution_block_stage(ctx)

                    self.assertIsNotNone(result)
                    self.assertTrue(bool(result.get("execution_blocked")), result)
                    self.assertIn(code, list(result.get("execution_blocked_codes") or []))
                    self.assertEqual(int(result.get("orders_n") or 0), 0)
                    self.assertEqual(list(result.get("changed") or []), [])

    def test_clean_rebalance_execution_block_stage_preserves_emitted_orders_control(self) -> None:
        (portfolio,) = _reload_modules("engine.strategy.portfolio")
        ctx = portfolio._RebalanceContext(con=object(), now_ms=1_700_000_000_000)
        ctx.desired = {"AAPL": {"symbol": "AAPL", "side": "LONG", "weight": 0.10}}
        ctx.changed = ["AAPL"]
        ctx.orders_n = 1

        result = portfolio._apply_rebalance_execution_block_stage(ctx)
        final = portfolio._build_rebalance_result(
            ctx,
            execution_blocked=False,
            execution_blocked_codes=[],
        )

        self.assertIsNone(result)
        self.assertFalse(bool(final.get("execution_blocked")), final)
        self.assertEqual(int(final.get("orders_n") or 0), 1)
        self.assertEqual(list(final.get("changed") or []), ["AAPL"])


if __name__ == "__main__":
    unittest.main()
