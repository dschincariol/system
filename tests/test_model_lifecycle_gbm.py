from __future__ import annotations

import importlib
import json
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


class ModelLifecycleGBMTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.tmp.name) / "model_lifecycle_gbm.db"
        self._env_backup = {
            key: os.environ.get(key)
            for key in (
                "DB_PATH",
                "MODEL_CONFIG_JSON",
                "MODEL_V2_NAME",
            )
        }
        os.environ["DB_PATH"] = str(self.db_path)
        os.environ["MODEL_V2_NAME"] = "regime_stats_v2"
        os.environ["MODEL_CONFIG_JSON"] = json.dumps(
            [
                {
                    "model_name": "gbm_regressor.audit_shadow",
                    "family": "gbm_regressor",
                    "enabled": True,
                    "prediction_enabled": False,
                    "experimental": True,
                }
            ],
            separators=(",", ":"),
            sort_keys=True,
        )
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

    def test_default_lifecycle_summary_includes_enabled_gbm_variant(self) -> None:
        with patch.object(self.lifecycle, "detect_learning_signals", return_value={}):
            summary = self.lifecycle.get_lifecycle_summary()

        self.assertIn("gbm_regressor.audit_shadow", dict(summary.get("families") or {}))

    def test_default_lifecycle_job_includes_enabled_gbm_variant(self) -> None:
        with patch.object(self.lifecycle, "sync_registry_metrics", return_value=0), patch.object(
            self.lifecycle, "get_latest_version", return_value=None
        ), patch.object(self.lifecycle, "retire_underperforming_versions", return_value=[]), patch.object(
            self.lifecycle, "should_retrain", return_value={"should_retrain": False}
        ):
            result = self.lifecycle.run_model_lifecycle_job()

        self.assertTrue(bool(result.get("ok")))
        self.assertIn("gbm_regressor.audit_shadow", dict(result.get("families") or {}))
        self.assertEqual(
            dict(result.get("families") or {})["gbm_regressor.audit_shadow"]["dispatch"],
            None,
        )


if __name__ == "__main__":
    unittest.main()
