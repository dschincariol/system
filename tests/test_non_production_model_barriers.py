"""Regression tests for non-production model execution barriers."""

from __future__ import annotations

import importlib
import os
import sys
import tempfile
import unittest
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class NonProductionModelBarrierTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "barrier_test.db")
        _reload_modules("engine.runtime.db_guard", "engine.runtime.storage")

    def tearDown(self) -> None:
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception as e:
            sys.stderr.write(
                f"[test_non_production_model_barriers] close_pooled_connections_failed: {type(e).__name__}: {e}\n"
            )
        self.tmp.cleanup()

    def test_execution_guard_blocks_rl_and_llm_model_sources(self) -> None:
        (broker_apply_orders,) = _reload_modules("engine.execution.broker_apply_orders")

        allowed, blocked = broker_apply_orders._enforce_production_model_sources(
            [
                {"symbol": "AAPL", "model_id": "baseline", "model_name": "baseline"},
                {"symbol": "MSFT", "model_id": "rl_policy_v1", "model_name": "baseline"},
                {"symbol": "NVDA", "model_id": "baseline", "model_kind": "llm"},
                {"symbol": "TSLA", "model_id": "baseline", "explain": {"selector": {"rl_choice": "shadow_rl"}}},
            ]
        )

        self.assertEqual(len(allowed), 1)
        self.assertEqual(str(allowed[0]["symbol"]), "AAPL")
        self.assertEqual(len(blocked), 3)
        self.assertEqual(
            {str(item["symbol"]) for item in blocked},
            {"MSFT", "NVDA", "TSLA"},
        )

    @pytest.mark.requires_postgres
    def test_rebalance_selector_metadata_is_live_only(self) -> None:
        storage, portfolio = _reload_modules(
            "engine.runtime.storage",
            "engine.strategy.portfolio",
        )
        storage.init_db()

        con = storage.connect()
        try:
            portfolio._write_state_row(
                con,
                "baseline",
                "AAPL",
                "LONG",
                0.25,
                1,
                1,
                123,
                "{}",
            )
            portfolio._emit_order(
                con,
                "AAPL",
                "OPEN",
                "FLAT",
                "LONG",
                0.0,
                0.25,
                123,
                {
                    "strategy": {"name": "multi_strategy"},
                    "selector": {"mode": "live_registry_only", "strategy_name": "multi_strategy"},
                    "signal": {},
                    "tradability": {},
                    "model_id": "baseline",
                },
            )
            con.commit()

            row = con.execute(
                "SELECT explain_json FROM portfolio_orders ORDER BY id DESC LIMIT 1"
            ).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(row)
        self.assertIn("\"live_registry_only\"", str(row[0]))
        self.assertNotIn("\"rl_choice\"", str(row[0]))
        self.assertNotIn("\"rl_score\"", str(row[0]))
