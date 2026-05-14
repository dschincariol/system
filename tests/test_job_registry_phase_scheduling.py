from __future__ import annotations

import importlib
import json
import os
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class JobRegistryPhaseSchedulingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.prev_env = {
            key: os.environ.get(key)
            for key in (
                "USE_TSFRESH_FEATURES",
                "USE_FINBERT_SENTIMENT",
                "HMM_REGIME_ENABLED",
                "PIT_UNIVERSE_BACKFILL_ENABLED",
                "DRIFT_RETRAIN_ENABLED",
                "CPCV_ENABLED",
                "USE_GBM_REGRESSOR",
                "MODEL_CONFIG_JSON",
            )
        }

    def tearDown(self) -> None:
        for key, value in self.prev_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def test_optional_phase_jobs_are_not_always_on_by_default(self) -> None:
        for key in self.prev_env:
            os.environ.pop(key, None)
        (job_registry,) = _reload_modules("engine.runtime.job_registry")

        self.assertNotIn("compute_tsfresh_snapshots", job_registry.INGESTION_DAEMON_JOBS)
        for job_name in (
            "backfill_universe_pit",
            "process_finbert_sentiment",
            "train_hmm_regime",
            "train_gbm_regressor",
            "drift_triggered_retrain",
            "backtest_cpcv",
        ):
            self.assertNotIn(job_name, job_registry.PIPELINE_ORDER)

    def test_enabled_phase_jobs_are_added_to_canonical_orders(self) -> None:
        os.environ["USE_TSFRESH_FEATURES"] = "1"
        os.environ["USE_FINBERT_SENTIMENT"] = "1"
        os.environ["HMM_REGIME_ENABLED"] = "1"
        os.environ["PIT_UNIVERSE_BACKFILL_ENABLED"] = "1"
        os.environ["DRIFT_RETRAIN_ENABLED"] = "1"
        os.environ["CPCV_ENABLED"] = "1"
        os.environ["USE_GBM_REGRESSOR"] = "1"
        os.environ["MODEL_CONFIG_JSON"] = json.dumps(
            [
                {
                    "family": "gbm_regressor",
                    "instance_name": "registry_test",
                    "horizon": "medium",
                    "feature_groups": ["base", "macro"],
                    "symbol_universe": ["*"],
                    "risk_profile": "balanced",
                    "training_window_days": 180,
                    "model_kind": "lightgbm",
                    "prediction_enabled": True,
                    "enabled": True,
                }
            ],
            separators=(",", ":"),
            sort_keys=True,
        )

        (job_registry,) = _reload_modules("engine.runtime.job_registry")
        pipeline = list(job_registry.PIPELINE_ORDER)

        self.assertIn("compute_tsfresh_snapshots", job_registry.INGESTION_DAEMON_JOBS)
        for job_name in (
            "backfill_universe_pit",
            "process_finbert_sentiment",
            "train_hmm_regime",
            "train_gbm_regressor",
            "drift_triggered_retrain",
            "backtest_cpcv",
        ):
            self.assertIn(job_name, pipeline)

        self.assertGreater(pipeline.index("backfill_universe_pit"), pipeline.index("update_universe"))
        self.assertLess(pipeline.index("backfill_universe_pit"), pipeline.index("ingest_options"))
        self.assertGreater(pipeline.index("process_finbert_sentiment"), pipeline.index("process_events"))
        self.assertGreater(pipeline.index("train_hmm_regime"), pipeline.index("compute_drift"))
        self.assertGreater(pipeline.index("train_gbm_regressor"), pipeline.index("shadow_metrics"))
        self.assertGreater(pipeline.index("drift_triggered_retrain"), pipeline.index("model_lifecycle_manager"))
        self.assertLess(pipeline.index("backtest_cpcv"), pipeline.index("validate_now"))

    def test_snapshot_equity_is_booted_as_background_daemon(self) -> None:
        (job_registry,) = _reload_modules("engine.runtime.job_registry")

        spec = job_registry.get_job_spec("snapshot_equity")

        self.assertIsNotNone(spec)
        self.assertEqual(spec[1], "daemon")
        self.assertIn("snapshot_equity", job_registry.JOB_ORDER)
        self.assertIn("snapshot_equity", job_registry.get_boot_jobs())

    def test_job_files_are_registered_or_explicitly_quarantined(self) -> None:
        (job_registry,) = _reload_modules("engine.runtime.job_registry")

        expected_quarantined: set[str] = set()

        discovered = job_registry._discover_repo_job_files(REPO_ROOT)
        registered = job_registry._registered_job_script_paths()
        quarantined = set(job_registry.QUARANTINED_JOB_FILES)

        self.assertEqual(expected_quarantined, quarantined)
        self.assertEqual(set(), discovered - registered - quarantined)
        self.assertEqual(set(), quarantined - discovered)
        self.assertEqual(set(), quarantined & registered)

    def test_registry_validation_reports_no_untracked_job_files(self) -> None:
        (job_registry,) = _reload_modules("engine.runtime.job_registry")

        validation = job_registry.validate_job_registry_paths(repo_root=REPO_ROOT)
        quarantine_errors = [
            error
            for error in (validation.get("errors") or [])
            if error.startswith("untracked_job_file:")
            or error.startswith("quarantined_job_missing_file:")
            or error.startswith("quarantined_job_registered:")
        ]

        self.assertEqual([], quarantine_errors)


if __name__ == "__main__":
    unittest.main()
