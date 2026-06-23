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


class PortfolioStagedRebalancePipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "portfolio_staged.db"
        self._prev_db_path = os.environ.get("DB_PATH")
        self._prev_storage_backend = os.environ.get("TS_STORAGE_BACKEND")
        os.environ["DB_PATH"] = str(self.db_path)
        os.environ["TS_STORAGE_BACKEND"] = "sqlite"
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
        if self._prev_db_path is None:
            os.environ.pop("DB_PATH", None)
        else:
            os.environ["DB_PATH"] = self._prev_db_path
        if self._prev_storage_backend is None:
            os.environ.pop("TS_STORAGE_BACKEND", None)
        else:
            os.environ["TS_STORAGE_BACKEND"] = self._prev_storage_backend
        try:
            _reload_modules(
                "engine.runtime.storage",
                "engine.strategy.portfolio",
            )
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

    def test_stage_order_is_the_production_rebalance_order(self) -> None:
        (portfolio,) = _reload_modules("engine.strategy.portfolio")

        self.assertEqual(
            portfolio._rebalance_stage_names(),
            [
                "input_loading",
                "allocator_loading",
                "target_construction",
                "normalization",
                "overlays",
                "risk_gates",
                "execution_blocking",
                "order_emission",
                "persistence",
            ],
        )

    def test_compute_rebalance_preserves_golden_order_decisions(self) -> None:
        storage, portfolio, strategy_allocator, capital_guard = _reload_modules(
            "engine.runtime.storage",
            "engine.strategy.portfolio",
            "engine.runtime.strategy_allocator",
            "engine.strategy.capital_guard",
        )
        alpha_shrinkage = importlib.import_module("engine.strategy.alpha_shrinkage")
        allocation_risk_overlay = importlib.import_module("engine.strategy.allocation_risk_overlay")
        broker_sim = importlib.import_module("engine.execution.broker_sim")
        global_risk_envelope = importlib.import_module("engine.runtime.global_risk_envelope")
        regime_size = importlib.import_module("engine.strategy.regime_size")
        regime_stack = importlib.import_module("engine.strategy.regime_stack")
        self._executescript(portfolio.SCHEMA)

        now_ms = 1_700_000_000_000
        old_opened_ms = now_ms - 3_600_000
        recent_opened_ms = now_ms - 60_000

        con = storage.connect()
        try:
            portfolio._write_state_row(con, "m1", "AAPL", "LONG", 0.10, old_opened_ms, old_opened_ms, None, "{}")
            portfolio._write_state_row(con, "m1", "MSFT", "SHORT", 0.05, old_opened_ms, old_opened_ms, None, "{}")
            portfolio._write_state_row(con, "m1", "TSLA", "LONG", 0.07, old_opened_ms, old_opened_ms, None, "{}")
            portfolio._write_state_row(con, "m1", "NVDA", "LONG", 0.09, recent_opened_ms, recent_opened_ms, None, "{}")
            con.commit()
        finally:
            con.close()

        def _target(symbol: str, side: str, weight: float) -> dict:
            explain = json.dumps(
                {
                    "model_id": "m1",
                    "model_name": "golden_model",
                    "confidence": 0.9,
                    "model_intent": {"score": 1.0},
                },
                separators=(",", ":"),
                sort_keys=True,
            )
            return {
                "symbol": symbol,
                "side": side,
                "weight": float(weight),
                "source_alert_id": 101,
                "reason": {"confidence": 0.9, "score": 1.0, "expected_z": 1.2},
                "confidence": 0.9,
                "explain_json": explain,
            }

        strategy_module = SimpleNamespace(
            build_desired=lambda alerts, now_ms: {
                "AAPL": _target("AAPL", "LONG", 0.20),
                "MSFT": _target("MSFT", "LONG", 0.08),
                "NVDA": _target("NVDA", "SHORT", 0.12),
                "GOOG": _target("GOOG", "LONG", 0.11),
                "AMZN": _target("AMZN", "FLAT", 0.00),
            }
        )

        with ExitStack() as stack:
            stack.enter_context(patch.object(portfolio, "_now_ms", return_value=now_ms))
            stack.enter_context(patch.object(portfolio, "PORTFOLIO_MIN_HOLD_S", 300))
            stack.enter_context(patch.object(capital_guard, "trading_allowed", return_value=True))
            stack.enter_context(
                patch.object(
                    strategy_allocator,
                    "compute_and_persist_strategy_allocations",
                    return_value={
                        "allocations": {"s1": 1.0},
                        "details": {},
                        "regime": {},
                        "regime_confidence": 0.0,
                        "reason": {},
                        "alpha_decay_runtime": {},
                        "portfolio_target_gross": 1.0,
                    },
                )
            )
            stack.enter_context(patch.object(portfolio, "_load_recent_alert_candidates", return_value=[]))
            stack.enter_context(patch.object(portfolio, "_load_live_strategies", return_value=["s1"]))
            stack.enter_context(patch.object(portfolio, "_load_shadow_strategies", return_value=[]))
            stack.enter_context(patch.object(portfolio, "_load_cached_competition_capital_plan", return_value={}))
            stack.enter_context(patch.object(portfolio, "_load_shadow_performance", return_value={}))
            stack.enter_context(patch.object(portfolio, "load_strategy_module", return_value=strategy_module))
            stack.enter_context(patch.object(portfolio, "is_blacklisted", return_value=False))
            stack.enter_context(patch.object(portfolio, "conformal_mode", return_value="off"))
            stack.enter_context(
                patch.object(
                    global_risk_envelope,
                    "compute_global_risk_envelope",
                    return_value={"global_scale": 1.0},
                )
            )
            stack.enter_context(
                patch.object(
                    regime_stack,
                    "compute_regime_vector",
                    return_value={"micro": {}, "macro": {}, "confidence": {"overall": 1.0}, "regimes": {}},
                )
            )
            stack.enter_context(patch.object(regime_stack, "regime_compatibility", return_value=1.0))
            stack.enter_context(
                patch.object(
                    alpha_shrinkage,
                    "apply_alpha_shrinkage_to_desired",
                    side_effect=lambda con, desired, now_ms: (desired, {}),
                )
            )
            stack.enter_context(patch.object(portfolio, "_apply_black_litterman_overlay", side_effect=lambda con, desired: desired))
            stack.enter_context(patch.object(portfolio, "_optimize_capital_allocation", side_effect=lambda con, desired: desired))
            stack.enter_context(patch.object(portfolio, "_apply_impact_aware_sizing", side_effect=lambda con, desired: desired))
            stack.enter_context(
                patch.object(portfolio, "_apply_model_diversification_scoring", side_effect=lambda con, desired: (desired, {}))
            )
            stack.enter_context(patch.object(portfolio, "apply_max_position_constraint", side_effect=lambda desired: desired))
            stack.enter_context(patch.object(portfolio, "_apply_allocation_mode", side_effect=lambda con, desired: desired))
            stack.enter_context(
                patch.object(portfolio, "apply_portfolio_risk_engine", side_effect=lambda con, desired, state, now_ms: (desired, {}))
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
            stack.enter_context(patch.object(portfolio, "_apply_flip_flop_penalty", side_effect=lambda con, desired, state: (desired, {})))
            stack.enter_context(patch.object(portfolio, "_build_portfolio_correlation_diagnostics", return_value={}))
            stack.enter_context(patch.object(portfolio, "_persist_portfolio_correlation_diagnostics", return_value=None))
            stack.enter_context(patch.object(portfolio, "request_monte_carlo_refresh", return_value=None))
            stack.enter_context(patch.object(portfolio, "PORTFOLIO_ALLOC_OPT", False))
            stack.enter_context(patch.object(portfolio, "PORTFOLIO_CORR_OPT", False))
            stack.enter_context(patch.object(portfolio, "PORTFOLIO_USE_VOL_TARGET", False, create=True))
            stack.enter_context(patch.object(portfolio, "PORTFOLIO_USE_STRESS_GATE", False))
            stack.enter_context(patch.object(portfolio, "PORTFOLIO_USE_SOCIAL_GATE", False))
            stack.enter_context(patch.object(portfolio, "PORTFOLIO_USE_VOV_GATE", False))
            stack.enter_context(patch.object(portfolio, "PORTFOLIO_USE_EXEC_REALISM", False))
            stack.enter_context(patch.object(portfolio, "PORTFOLIO_USE_EXEC_REGIME", False))
            stack.enter_context(patch.object(portfolio, "PORTFOLIO_EXPLORE_MIN_LABELS", 0))
            stack.enter_context(patch.object(regime_size, "regime_capital_scale", return_value={"final_mult": 1.0}))
            stack.enter_context(
                patch.object(
                    allocation_risk_overlay,
                    "apply_allocation_risk_overlays",
                    side_effect=lambda con, desired, gross_cap, now_ms: (desired, {}),
                )
            )
            stack.enter_context(patch.object(broker_sim, "broker_snapshot", return_value={"ok": True, "account": {}}))

            result = portfolio.compute_rebalance()

        self.assertTrue(result.get("ok"), result)
        self.assertFalse(result.get("execution_blocked"), result)
        self.assertEqual(int(result.get("orders_n") or 0), 5)

        con = storage.connect(readonly=True)
        try:
            rows = con.execute(
                """
                SELECT symbol, action, from_side, to_side, ROUND(from_weight, 4), ROUND(to_weight, 4)
                FROM portfolio_orders
                ORDER BY symbol ASC, action ASC
                """
            ).fetchall()
        finally:
            con.close()

        self.assertEqual(
            [tuple(row) for row in rows],
            [
                ("AAPL", "INCREASE", "LONG", "LONG", 0.1, 0.2),
                ("GOOG", "OPEN", "FLAT", "LONG", 0.0, 0.11),
                ("MSFT", "REVERSE", "SHORT", "LONG", 0.05, 0.08),
                ("NVDA", "HOLD", "LONG", "LONG", 0.09, 0.09),
                ("TSLA", "CLOSE", "LONG", "FLAT", 0.07, 0.0),
            ],
        )
        self.assertCountEqual(result.get("changed") or [], ["AAPL", "MSFT", "GOOG", "TSLA"])


if __name__ == "__main__":
    unittest.main()
