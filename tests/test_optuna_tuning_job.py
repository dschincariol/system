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
import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

pytestmark = pytest.mark.requires_postgres

MODEL_NAME = "gbm_regressor.unit"
FEATURE_IDS = [
    "base.source_credibility",
    "base.log_recency_hours",
]


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class FakeTrial:
    def __init__(self, number: int, suggestions: dict[str, float | int]) -> None:
        self.number = int(number)
        self._suggestions = dict(suggestions)
        self.user_attrs: dict[str, object] = {}
        self.value: float | None = None

    def suggest_int(self, name: str, low: int, high: int, log: bool = False, step: int | None = None) -> int:
        return int(self._suggestions[name])

    def suggest_float(self, name: str, low: float, high: float, log: bool = False) -> float:
        return float(self._suggestions[name])

    def set_user_attr(self, key: str, value: object) -> None:
        self.user_attrs[str(key)] = value


class FakeStudy:
    def __init__(self, study_name: str, trials: list[FakeTrial]) -> None:
        self.study_name = str(study_name)
        self._candidate_trials = list(trials)
        self.trials: list[FakeTrial] = []
        self.best_trial: FakeTrial | None = None

    def optimize(self, objective, n_trials: int, timeout: int | None = None) -> None:
        for trial in self._candidate_trials[: int(n_trials)]:
            trial.value = float(objective(trial))
            self.trials.append(trial)
            if self.best_trial is None or float(trial.value or 0.0) > float(self.best_trial.value or 0.0):
                self.best_trial = trial


class FakeOptuna:
    class samplers:
        class TPESampler:
            def __init__(self, seed: int | None = None) -> None:
                self.seed = seed

    def __init__(self, trials: list[FakeTrial]) -> None:
        self._trials = list(trials)

    def create_study(self, direction: str, study_name: str, sampler=None) -> FakeStudy:
        return FakeStudy(study_name=study_name, trials=list(self._trials))


class OptunaTuningJobTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.tmp.name) / "optuna_tuning.db"
        self._env_backup = {
            key: os.environ.get(key)
            for key in (
                "DB_PATH",
                "MODEL_CONFIG_JSON",
                "GBM_USE_TUNED_HYPERPARAMS",
            )
        }
        os.environ["DB_PATH"] = str(self.db_path)
        os.environ["GBM_USE_TUNED_HYPERPARAMS"] = "0"
        os.environ["MODEL_CONFIG_JSON"] = json.dumps(
            [
                {
                    "model_name": MODEL_NAME,
                    "family": "gbm_regressor",
                    "horizons_s": [300],
                    "feature_ids": list(FEATURE_IDS),
                    "symbol_universe": ["AAPL"],
                    "hyperparams": {
                        "num_leaves": 11,
                        "learning_rate": 0.07,
                        "n_estimators": 90,
                        "min_child_samples": 7,
                    },
                    "enabled": True,
                }
            ],
            separators=(",", ":"),
            sort_keys=True,
        )
        (
            self.db_guard,
            self.storage,
            self.model_config,
            self.feature_registry,
            self.gbm,
            self.optuna_tuner,
            self.job_registry,
        ) = self._reload_stack()
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

    def _reload_stack(self):
        return _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.strategy.model_config",
            "engine.strategy.feature_registry",
            "engine.strategy.gbm_regressor",
            "engine.strategy.optuna_tuner",
            "engine.runtime.job_registry",
        )

    def test_registry_overlay_is_opt_in(self) -> None:
        record_id = self.storage.record_model_hyperparameter_registry(
            model_name=MODEL_NAME,
            model_family="gbm_regressor",
            tuner="optuna_cpcv",
            objective="median_sharpe_minus_pbo_penalty",
            metric_value=1.10,
            params={
                "num_leaves": 23,
                "learning_rate": 0.03,
                "n_estimators": 180,
                "min_child_samples": 9,
            },
        )

        row = self.storage.fetch_latest_model_hyperparameters(
            model_name=MODEL_NAME,
            model_family="gbm_regressor",
            tuner="optuna_cpcv",
        )
        self.assertGreater(int(record_id), 0)
        self.assertEqual(int(row["params"]["num_leaves"]), 23)

        cfg_off = self.gbm._resolve_training_config({"model_name": MODEL_NAME})
        self.assertEqual(int(cfg_off["hyperparams"]["num_leaves"]), 11)
        self.assertFalse(bool(cfg_off["use_tuned_hyperparams"]))

        os.environ["GBM_USE_TUNED_HYPERPARAMS"] = "1"
        (
            self.db_guard,
            self.storage,
            self.model_config,
            self.feature_registry,
            self.gbm,
            self.optuna_tuner,
            self.job_registry,
        ) = self._reload_stack()
        cfg_on = self.gbm._resolve_training_config({"model_name": MODEL_NAME})
        self.assertEqual(int(cfg_on["hyperparams"]["num_leaves"]), 23)
        self.assertAlmostEqual(float(cfg_on["hyperparams"]["learning_rate"]), 0.03, places=6)
        self.assertTrue(bool(cfg_on["use_tuned_hyperparams"]))

    def test_optuna_tuning_job_records_best_params(self) -> None:
        fake_optuna = FakeOptuna(
            [
                FakeTrial(
                    0,
                    {
                        "num_leaves": 8,
                        "learning_rate": 0.10,
                        "n_estimators": 100,
                        "min_child_samples": 10,
                    },
                ),
                FakeTrial(
                    1,
                    {
                        "num_leaves": 32,
                        "learning_rate": 0.05,
                        "n_estimators": 140,
                        "min_child_samples": 6,
                    },
                ),
            ]
        )

        dataset = {
            "ok": True,
            "status": "loaded",
            "row_count": 12,
            "train_cfg": {
                "family": "gbm_regressor",
                "hyperparams": {
                    "num_leaves": 11,
                    "learning_rate": 0.07,
                    "n_estimators": 90,
                    "min_child_samples": 7,
                    "random_state": 42,
                    "n_jobs": 1,
                },
            },
            "runtime_symbols": ["AAPL"],
            "feature_ids": list(FEATURE_IDS),
            "horizon_s": 300,
            "lookback_days": 30,
            "X": np.ones((12, 2), dtype=np.float32),
            "y": np.ones(12, dtype=np.float32),
        }

        def fake_cpcv(_X, _y, model_factory, **kwargs):
            params = model_factory()
            num_leaves = int(params["num_leaves"])
            if num_leaves == 32:
                return {
                    "ok": True,
                    "status": "evaluated",
                    "n_paths": 4,
                    "mean_sharpe": 1.00,
                    "median_sharpe": 1.20,
                    "pbo": 0.10,
                    "paths": [{"returns": [0.1], "sharpe": 1.20}],
                    "diagnostics": {},
                }
            return {
                "ok": True,
                "status": "evaluated",
                "n_paths": 4,
                "mean_sharpe": 0.30,
                "median_sharpe": 0.40,
                "pbo": 0.60,
                "paths": [{"returns": [0.01], "sharpe": 0.40}],
                "diagnostics": {},
            }

        with patch.object(self.optuna_tuner, "_import_optuna", return_value=fake_optuna), patch.object(
            self.optuna_tuner,
            "_load_gbm_training_dataset",
            return_value=dataset,
        ), patch.object(
            self.optuna_tuner,
            "_make_lgbm_factory",
            side_effect=lambda hyperparams: (lambda: dict(hyperparams)),
        ), patch.object(
            self.optuna_tuner,
            "cpcv_backtest",
            side_effect=fake_cpcv,
        ):
            result = self.optuna_tuner.run_gbm_optuna_tuning_job(model_name=MODEL_NAME, n_trials=2)

        self.assertTrue(bool(result["ok"]))
        self.assertEqual(int(result["best_params"]["num_leaves"]), 32)
        self.assertGreater(int(result["registry_id"]), 0)
        row = self.storage.fetch_latest_model_hyperparameters(
            model_name=MODEL_NAME,
            model_family="gbm_regressor",
            tuner="optuna_cpcv",
        )
        self.assertEqual(int(row["params"]["num_leaves"]), 32)
        self.assertAlmostEqual(float(row["cpcv_pbo"]), 0.10, places=6)

    def test_job_registry_contains_optuna_tuning_job(self) -> None:
        self.assertIn("tune_gbm_regressor_optuna", self.job_registry.ALLOWED_JOBS)
        self.assertEqual(
            self.job_registry.ALLOWED_JOBS["tune_gbm_regressor_optuna"][0],
            "engine/strategy/jobs/tune_gbm_regressor_optuna.py",
        )


if __name__ == "__main__":
    unittest.main()
