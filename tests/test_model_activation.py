"""Regression tests for canonical active-model resolution."""

from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class ModelActivationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.tmp.name) / "model_activation_test.db"
        os.environ["DB_PATH"] = str(self.db_path)
        os.environ["MODEL_CONFIG_JSON"] = json.dumps(
            [
                {
                    "model_name": "embed_regressor.active_core",
                    "family": "embed_regressor",
                    "horizons_s": [300],
                    "symbol_universe": ["AAPL"],
                    "feature_groups": ["base", "tech"],
                    "prediction_enabled": True,
                    "experimental": False,
                    "enabled": True,
                },
                {
                    "model_name": "temporal_predictor.experimental",
                    "family": "temporal_predictor",
                    "horizons_s": [300],
                    "symbol_universe": ["AAPL"],
                    "feature_groups": ["base"],
                    "prediction_enabled": False,
                    "experimental": True,
                    "enabled": True,
                },
            ],
            separators=(",", ":"),
            sort_keys=True,
        )

        _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.strategy.model_config",
            "engine.strategy.predictor",
        )

    def tearDown(self) -> None:
        os.environ.pop("MODEL_CONFIG_JSON", None)
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception as e:
            sys.stderr.write(f"[test_model_activation] close_pooled_connections_failed: {type(e).__name__}: {e}\n")
        self.tmp.cleanup()

    def test_resolve_active_model_ignores_inactive_registry_and_champion_candidates(self) -> None:
        model_config, predictor = _reload_modules(
            "engine.strategy.model_config",
            "engine.strategy.predictor",
        )

        self.assertEqual(
            model_config.active_model_names(symbol="AAPL", horizon_s=300),
            ["embed_regressor.active_core"],
        )

        with patch.object(predictor, "get_live_competition_champion_name", return_value="temporal_predictor.experimental"):
            with patch.object(predictor, "get_champion_assignment", return_value={"model_name": "temporal_predictor.experimental"}):
                with patch.object(predictor, "get_active_model_name", return_value="temporal_predictor.experimental"):
                    resolved = predictor._resolve_active_model("AAPL", 300)

        self.assertEqual(str(resolved.get("model_name") or ""), "embed_regressor.active_core")
        self.assertEqual(str(resolved.get("requested_model_name") or ""), "temporal_predictor.experimental")
        self.assertEqual(str(resolved.get("resolved_model_name") or ""), "embed_regressor.active_core")
        self.assertEqual(str(resolved.get("requested_model_family") or ""), "temporal_predictor")
        self.assertEqual(str(resolved.get("resolution_source") or ""), "env_default")
        self.assertTrue(bool(resolved.get("serve_fallback_active")))
        self.assertEqual(str(resolved.get("fallback_reason") or ""), "resolved_to_env_default")
        self.assertIn("temporal_predictor.experimental", list(resolved.get("candidate_names") or []))

    def test_predict_forced_model_rejects_inactive_model_name(self) -> None:
        (_, predictor) = _reload_modules(
            "engine.strategy.model_config",
            "engine.strategy.predictor",
        )

        with self.assertRaisesRegex(ValueError, "inactive_model:temporal_predictor.experimental"):
            predictor.predict_forced_model(
                np.asarray([0.0, 0.0], dtype=np.float32),
                symbol="AAPL",
                horizon_s=300,
                model_name="temporal_predictor.experimental",
            )

    def test_model_config_safe_int_ignores_missing_values_without_warning(self) -> None:
        (model_config,) = _reload_modules("engine.strategy.model_config")

        with patch.object(model_config, "_warn_nonfatal") as warn_nonfatal:
            self.assertEqual(model_config._safe_int(None, 7), 7)
            self.assertEqual(model_config._safe_int("", 9), 9)
            self.assertEqual(model_config._safe_int("   ", 11), 11)

        warn_nonfatal.assert_not_called()


if __name__ == "__main__":
    unittest.main()
