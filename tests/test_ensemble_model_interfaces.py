from __future__ import annotations

import importlib
import os
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
from sklearn.ensemble import GradientBoostingRegressor

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class EnsembleModelInterfaceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self._env_backup = {
            key: os.environ.get(key)
            for key in (
                "DB_PATH",
                "TIMESCALE_ENABLED",
                "INFERENCE_ENSEMBLE_PARALLEL_WORKERS",
            )
        }
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "ensemble_model_interfaces.db")
        os.environ.pop("TIMESCALE_ENABLED", None)
        os.environ["INFERENCE_ENSEMBLE_PARALLEL_WORKERS"] = "4"
        (
            self.storage,
            self.feature_store,
            self.registry,
            self.base_model,
            self.gbm_model,
            self.online_model,
            self.inference,
        ) = _reload_modules(
            "engine.runtime.storage",
            "engine.data.feature_store",
            "engine.model_registry",
            "engine.strategy.models.base_model",
            "engine.strategy.models.gbm_model",
            "engine.strategy.models.online_model",
            "engine.inference_engine",
        )
        self.storage.init_db()

    def tearDown(self) -> None:
        for key, value in self._env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        try:
            self.storage.close_pooled_connections()
        except Exception:
            pass
        self.tmp.cleanup()

    def _store_feature_snapshot(self, symbol: str, *, scale: float = 1.0) -> dict[str, object]:
        now_ms = int(time.time() * 1000)
        feature_map = {
            name: float(scale * float(idx + 1))
            for idx, name in enumerate(self.feature_store.FEATURE_NAMES)
        }
        snapshot = {
            "symbol": str(symbol).upper(),
            "ts_ms": int(now_ms),
            "feature_set_tag": str(self.feature_store.FEATURE_SET_TAG),
            "feature_names": list(self.feature_store.FEATURE_NAMES),
            "point_count": 64,
            "source_timestamps": {"price_history_last_ts_ms": int(now_ms)},
            "features": feature_map,
        }
        return self.feature_store.store_features(str(symbol), snapshot)

    def _fit_gbm_wrapper(self, *, model_name: str, default_confidence: float = 0.82):
        feature_count = len(self.feature_store.FEATURE_NAMES)
        inputs = []
        targets = []
        for scale in (0.5, 0.8, 1.0, 1.3, 1.6, 2.0, 2.4, 2.8):
            row = [float(scale * float(idx + 1)) for idx in range(feature_count)]
            inputs.append(row)
            targets.append(float((row[0] * 0.02) + (row[1] * 0.01) - (row[2] * 0.005)))
        estimator = GradientBoostingRegressor(random_state=42)
        estimator.fit(np.asarray(inputs, dtype=np.float32), np.asarray(targets, dtype=np.float32))
        return self.gbm_model.GBMModel(
            estimator,
            model_name=str(model_name),
            feature_ids=list(self.feature_store.FEATURE_NAMES),
            feature_set_tag=str(self.feature_store.FEATURE_SET_TAG),
            backend="sklearn_gbm",
            default_confidence=float(default_confidence),
            training_metrics={"quality_score": float(default_confidence)},
        )

    def _fit_online_wrapper(self, *, model_name: str, default_confidence: float = 0.68):
        model = self.online_model.OnlineModel(
            model_name=str(model_name),
            feature_ids=list(self.feature_store.FEATURE_NAMES),
            feature_set_tag=str(self.feature_store.FEATURE_SET_TAG),
            default_confidence=float(default_confidence),
        )
        feature_count = len(self.feature_store.FEATURE_NAMES)
        for scale in (0.6, 0.9, 1.1, 1.4, 1.8, 2.2):
            row = [float(scale * float(idx + 1)) for idx in range(feature_count)]
            outcome = float((row[0] * 0.015) + (row[3] * 0.01) - (row[4] * 0.004))
            model.update(row, outcome)
        return model

    def test_online_model_uses_feature_store_inputs_and_registers_with_registry(self) -> None:
        self._store_feature_snapshot("AAPL")
        model = self.online_model.OnlineModel(
            model_name="sgd_live",
            feature_ids=list(self.feature_store.FEATURE_NAMES),
            feature_set_tag=str(self.feature_store.FEATURE_SET_TAG),
            default_confidence=0.6,
        )

        update_result = model.update("AAPL", 1.25)
        predict_result = model.predict("AAPL")
        record = model.register(
            symbol="AAPL",
            version="v1",
            artifact_uri=Path(self.tmp.name) / "sgd_live.pkl",
            performance_metrics={"quality_score": 0.64},
            is_active=True,
        )

        self.assertIn("prediction", update_result)
        self.assertIn("confidence", predict_result)
        self.assertLessEqual(
            abs(float(predict_result.get("prediction") or 0.0)),
            float(self.online_model.ONLINE_MODEL_MAX_ABS_PREDICTION),
        )
        self.assertIsNotNone(record)
        self.assertEqual(str(record.get("model_kind") or ""), "online")
        loaded = self.registry.load_model("AAPL", model_name="sgd_live", version="v1")
        self.assertIsNotNone(loaded)
        metadata = dict(loaded.get("metadata") or {})
        self.assertEqual(metadata.get("feature_set_tag"), str(self.feature_store.FEATURE_SET_TAG))
        self.assertTrue(bool(metadata.get("supports_online_update")))
        self.assertEqual(metadata.get("model_interface"), "BaseModel")

    def test_mixed_gbm_and_online_models_serve_side_by_side_in_ensemble(self) -> None:
        self._store_feature_snapshot("AMD", scale=1.4)
        gbm = self._fit_gbm_wrapper(model_name="gbm_alpha", default_confidence=0.83)
        online = self._fit_online_wrapper(model_name="online_alpha", default_confidence=0.69)

        gbm.register(
            symbol="AMD",
            version="v1",
            artifact_uri=Path(self.tmp.name) / "amd_gbm_alpha.pkl",
            performance_metrics={"quality_score": 0.83},
            is_active=True,
        )
        online.register(
            symbol="AMD",
            version="v1",
            artifact_uri=Path(self.tmp.name) / "amd_online_alpha.pkl",
            performance_metrics={"quality_score": 0.69},
            is_active=True,
        )

        result = self.inference.predict("AMD", persist=False, ensemble_method="weighted_average")

        self.assertEqual(str(result.get("status") or ""), "ok")
        self.assertFalse(bool(result.get("safe_output")))
        self.assertEqual(str(result.get("model_kind") or ""), "ensemble")
        self.assertEqual(int(result.get("ensemble_size") or 0), 2)
        self.assertEqual(
            {str(member.get("model_name") or "") for member in (result.get("ensemble_members") or [])},
            {"gbm_alpha", "online_alpha"},
        )
        self.assertGreaterEqual(float(result.get("confidence") or 0.0), 0.0)
        self.assertLessEqual(float(result.get("confidence") or 0.0), 1.0)

    def test_ensemble_parallel_workers_are_resolved_from_env_at_call_time(self) -> None:
        os.environ["INFERENCE_ENSEMBLE_PARALLEL_WORKERS"] = "6"
        self.assertEqual(int(self.inference._resolve_ensemble_parallel_workers()), 6)
        os.environ["INFERENCE_ENSEMBLE_PARALLEL_WORKERS"] = "invalid"
        self.assertEqual(
            int(self.inference._resolve_ensemble_parallel_workers()),
            int(self.inference.ENSEMBLE_PARALLEL_WORKERS),
        )

    def test_ensemble_member_predictions_execute_in_parallel(self) -> None:
        self._store_feature_snapshot("NVDA", scale=1.2)
        gbm = self._fit_gbm_wrapper(model_name="gbm_parallel", default_confidence=0.8)
        online = self._fit_online_wrapper(model_name="online_parallel", default_confidence=0.66)

        gbm.register(
            symbol="NVDA",
            version="v1",
            artifact_uri=Path(self.tmp.name) / "nvda_gbm_parallel.pkl",
            performance_metrics={"quality_score": 0.8},
            is_active=True,
        )
        online.register(
            symbol="NVDA",
            version="v1",
            artifact_uri=Path(self.tmp.name) / "nvda_online_parallel.pkl",
            performance_metrics={"quality_score": 0.66},
            is_active=True,
        )

        lock = threading.Lock()
        barrier = threading.Barrier(2, timeout=5.0)
        barrier_failures: list[str] = []
        current_concurrency = 0
        max_concurrency = 0
        original_predict = self.base_model.BaseModel.predict

        def instrumented_predict(model_self, features):
            nonlocal current_concurrency, max_concurrency
            with lock:
                current_concurrency += 1
                max_concurrency = max(max_concurrency, current_concurrency)
            try:
                try:
                    barrier.wait()
                except threading.BrokenBarrierError as exc:
                    with lock:
                        barrier_failures.append(type(exc).__name__)
                    raise AssertionError("ensemble member predictions did not overlap") from exc
                return original_predict(model_self, features)
            finally:
                with lock:
                    current_concurrency -= 1

        with patch.object(self.base_model.BaseModel, "predict", new=instrumented_predict):
            result = self.inference.predict("NVDA", persist=False, ensemble_method="weighted_average")

        self.assertEqual(barrier_failures, [])
        self.assertEqual(str(result.get("status") or ""), "ok")
        self.assertEqual(int(result.get("ensemble_size") or 0), 2)
        self.assertGreaterEqual(int(max_concurrency), 2)


if __name__ == "__main__":
    unittest.main()
