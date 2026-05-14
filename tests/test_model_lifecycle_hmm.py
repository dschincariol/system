from __future__ import annotations

import importlib
import os
import sys
import tempfile
import unittest
from pathlib import Path
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


class ModelLifecycleHMMTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.tmp.name) / "model_lifecycle_hmm.db"
        self._env_backup = {
            key: os.environ.get(key)
            for key in (
                "DB_PATH",
                "HMM_REGIME_ENABLED",
                "HMM_TRAIN_LOOKBACK_ROWS",
                "HMM_TRAIN_MIN_ROWS",
                "MODEL_V2_NAME",
            )
        }
        os.environ["DB_PATH"] = str(self.db_path)
        os.environ["HMM_REGIME_ENABLED"] = "1"
        os.environ["HMM_TRAIN_LOOKBACK_ROWS"] = "128"
        os.environ["HMM_TRAIN_MIN_ROWS"] = "32"
        os.environ["MODEL_V2_NAME"] = "regime_stats_v2"
        _, self.storage, self.lifecycle = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.strategy.model_lifecycle",
        )
        self.storage.init_db()

    def tearDown(self) -> None:
        for key, value in self._env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception:
            pass
        self.tmp.cleanup()

    def test_default_lifecycle_summary_includes_enabled_hmm_family(self) -> None:
        with patch.object(self.lifecycle, "detect_learning_signals", return_value={}):
            summary = self.lifecycle.get_lifecycle_summary()

        self.assertIn("hmm_regime", dict(summary.get("families") or {}))

    def test_create_training_plan_uses_hmm_training_scope_and_dataset(self) -> None:
        dataset_used = {
            "model_name": "hmm_regime",
            "lookback_rows": 128,
            "symbols": ["SPY"],
            "horizons": [],
            "feature_ids": ["macro.risk_off", "macro.vol_expansion"],
            "captured_ts_ms": 1234,
            "sources": {
                "prices": {
                    "table": "prices",
                    "row_count": 128,
                    "latest_ts_ms": 1234,
                    "symbol": "SPY",
                },
                "regime_vectors": {
                    "usable_rows": 128,
                    "required_min_rows": 32,
                },
            },
        }
        base_variation = {
            "model_name": "hmm_regime",
            "model_version": "hmm_regime-1234",
            "parent_version": None,
            "mutation_kind": "baseline_retrain",
            "train_scope": {"seed": "kept"},
            "trigger": {"reason": "unit_test"},
        }

        with patch.object(self.lifecycle, "_discover_hmm_training_scope", return_value=(["SPY"], [])), patch.object(
            self.lifecycle, "plan_training_variation", return_value=dict(base_variation)
        ), patch.object(
            self.lifecycle, "_build_hmm_dataset_snapshot", return_value=dict(dataset_used)
        ):
            plan = self.lifecycle.create_training_plan("hmm_regime")

        self.assertEqual(str(plan.get("job_name")), "train_hmm_regime")
        self.assertEqual(str(plan.get("module_name")), "engine.strategy.jobs.train_hmm_regime")
        self.assertEqual(list(plan.get("symbols") or []), ["SPY"])
        self.assertEqual(list(plan.get("horizons") or []), [])
        self.assertEqual(dict(plan.get("dataset_used") or {}), dataset_used)
        train_scope = dict(plan.get("train_scope") or {})
        self.assertEqual(train_scope.get("seed"), "kept")
        self.assertEqual(list(train_scope.get("symbols") or []), ["SPY"])
        self.assertEqual(list(train_scope.get("horizons") or []), [])
        self.assertEqual(int(train_scope.get("lookback_rows") or 0), 128)
        self.assertEqual(int(train_scope.get("min_rows") or 0), 32)
        self.assertEqual(dict(train_scope.get("dataset_used") or {}), dataset_used)


if __name__ == "__main__":
    unittest.main()
