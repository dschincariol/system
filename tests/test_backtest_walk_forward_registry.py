"""Regression tests for walk-forward model-registry integration."""

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

pytestmark = pytest.mark.requires_postgres


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class BacktestWalkForwardRegistryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.tmp.name) / "walk_forward_registry.db"
        os.environ["DB_PATH"] = str(self.db_path)
        for key in ("WF_MODEL_SELECTION", "WF_MODEL_NAME", "WF_MODEL_VERSION", "WF_REQUIRE_REGISTERED_MODEL"):
            os.environ.pop(key, None)
        _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.model_registry",
            "ops.backtest_walk_forward",
        )

    def tearDown(self) -> None:
        for key in ("WF_MODEL_SELECTION", "WF_MODEL_NAME", "WF_MODEL_VERSION", "WF_REQUIRE_REGISTERED_MODEL"):
            os.environ.pop(key, None)
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception as e:
            sys.stderr.write(
                f"[test_backtest_walk_forward_registry] close_pooled_connections_failed: {type(e).__name__}: {e}\n"
            )
        self.tmp.cleanup()

    def test_resolve_catalog_model_prefers_best_registered_version(self) -> None:
        (_, storage, registry) = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.model_registry",
        )
        storage.init_db()
        registry.register_model(
            symbol="SPY",
            model_name="temporal_predictor",
            model_kind="temporal",
            version="v1",
            training_data_window={"start_ts_ms": 1, "end_ts_ms": 10},
            performance_metrics={"quality_score": 0.41},
        )
        registry.register_model(
            symbol="SPY",
            model_name="temporal_predictor",
            model_kind="temporal",
            version="v2",
            training_data_window={"start_ts_ms": 11, "end_ts_ms": 20},
            performance_metrics={"quality_score": 0.73},
            is_active=True,
        )

        (backtest_walk_forward,) = _reload_modules("ops.backtest_walk_forward")
        rec = backtest_walk_forward._resolve_catalog_model("SPY")

        self.assertIsNotNone(rec)
        self.assertEqual(str(rec.get("model_name") or ""), "temporal_predictor")
        self.assertEqual(str(rec.get("version") or ""), "v2")

    def test_resolve_catalog_model_honors_active_and_explicit_selection(self) -> None:
        (_, storage, registry) = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.model_registry",
        )
        storage.init_db()
        registry.register_model(
            symbol="BTC",
            model_name="sequence_model",
            model_kind="temporal",
            version="v1",
            training_data_window={"start_ts_ms": 1, "end_ts_ms": 10},
            performance_metrics={"quality_score": 0.50},
            is_active=True,
        )
        registry.register_model(
            symbol="BTC",
            model_name="sequence_model",
            model_kind="temporal",
            version="v2",
            training_data_window={"start_ts_ms": 11, "end_ts_ms": 20},
            performance_metrics={"quality_score": 0.92},
        )

        os.environ["WF_MODEL_SELECTION"] = "active"
        os.environ["WF_MODEL_NAME"] = "sequence_model"
        (backtest_walk_forward,) = _reload_modules("ops.backtest_walk_forward")
        active_rec = backtest_walk_forward._resolve_catalog_model("BTC")

        self.assertIsNotNone(active_rec)
        self.assertEqual(str(active_rec.get("version") or ""), "v1")

        os.environ["WF_MODEL_SELECTION"] = "best"
        os.environ["WF_MODEL_VERSION"] = "v2"
        (backtest_walk_forward,) = _reload_modules("ops.backtest_walk_forward")
        explicit_rec = backtest_walk_forward._resolve_catalog_model("BTC")

        self.assertIsNotNone(explicit_rec)
        self.assertEqual(str(explicit_rec.get("version") or ""), "v2")

    def test_walk_forward_tables_include_model_registry_columns(self) -> None:
        (_, storage) = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
        )
        storage.init_db()
        con = storage.connect(readonly=True)
        try:
            runs_cols = {str(row[1] or "") for row in con.execute("PRAGMA table_info(walk_forward_runs)").fetchall() or []}
            scores_cols = {str(row[1] or "") for row in con.execute("PRAGMA table_info(walk_forward_scores)").fetchall() or []}
        finally:
            con.close()

        self.assertIn("model_selection_json", runs_cols)
        self.assertIn("model_name", scores_cols)
        self.assertIn("model_version", scores_cols)
        self.assertIn("model_kind", scores_cols)


if __name__ == "__main__":
    unittest.main()
