"""Regression tests for the centralized model catalog registry."""

from __future__ import annotations

import importlib
import os
import sys
import tempfile
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


class ModelRegistryCatalogTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.tmp.name) / "model_registry_catalog.db"
        os.environ["DB_PATH"] = str(self.db_path)
        os.environ.pop("ARTIFACT_STORE_MIRROR_ROOT", None)
        _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.model_registry",
        )

    def tearDown(self) -> None:
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception as e:
            sys.stderr.write(f"[test_model_registry_catalog] close_pooled_connections_failed: {type(e).__name__}: {e}\n")
        self.tmp.cleanup()

    def test_catalog_register_load_list_and_best_model(self) -> None:
        (_, storage, registry) = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.model_registry",
        )
        storage.init_db()

        registry.register_model(
            symbol="AAPL",
            model_name="temporal_predictor",
            model_kind="temporal",
            version="v1",
            training_data_window={"start_ts_ms": 100, "end_ts_ms": 200},
            performance_metrics={"quality_score": 0.61, "rmse": 0.20},
            metadata={"framework": "torch"},
        )
        latest = registry.register_model(
            symbol="AAPL",
            model_name="temporal_predictor",
            model_kind="temporal",
            version="v2",
            training_data_window={"start_ts_ms": 150, "end_ts_ms": 300},
            performance_metrics={"quality_score": 0.83, "rmse": 0.15},
            metadata={"framework": "torch", "features": ["price", "volume"]},
            is_active=True,
        )
        registry.register_model(
            symbol="AAPL",
            model_name="xgb_predictor",
            model_kind="xgboost",
            version="v1",
            training_data_window={"start_ts_ms": 120, "end_ts_ms": 280},
            performance_metrics={"quality_score": 0.79},
            metadata={"framework": "xgboost"},
        )

        self.assertIsNotNone(latest)
        self.assertEqual(str(latest.get("version") or ""), "v2")
        self.assertEqual(int(latest.get("training_start_ts_ms") or 0), 150)
        self.assertEqual(int(latest.get("training_end_ts_ms") or 0), 300)
        self.assertTrue(bool(latest.get("is_active")))

        loaded = registry.load_model("AAPL", model_name="temporal_predictor", version="v2")
        self.assertIsNotNone(loaded)
        self.assertEqual(str(loaded.get("model_kind") or ""), "temporal")
        self.assertEqual(dict(loaded.get("metadata") or {}).get("framework"), "torch")
        self.assertEqual(float(dict(loaded.get("performance_metrics") or {}).get("quality_score") or 0.0), 0.83)

        models = registry.list_models("AAPL")
        self.assertEqual(len(models), 3)
        self.assertEqual(
            {(str(model.get("model_name") or ""), str(model.get("version") or "")) for model in models},
            {
                ("temporal_predictor", "v1"),
                ("temporal_predictor", "v2"),
                ("xgb_predictor", "v1"),
            },
        )

        best = registry.get_best_model("AAPL")
        self.assertIsNotNone(best)
        self.assertEqual(str(best.get("model_name") or ""), "temporal_predictor")
        self.assertEqual(str(best.get("version") or ""), "v2")
        self.assertEqual(str(best.get("best_metric_name") or ""), "quality_score")

    def test_get_best_model_respects_lower_is_better_metric(self) -> None:
        (_, storage, registry) = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.model_registry",
        )
        storage.init_db()

        registry.register_model(
            symbol="MSFT",
            model_name="rmse_model",
            model_kind="linear",
            version="v1",
            training_data_window={"start_ts_ms": 1, "end_ts_ms": 10},
            performance_metrics={"rmse": 0.24},
            selection_metric_name="rmse",
            selection_metric_higher_is_better=False,
        )
        registry.register_model(
            symbol="MSFT",
            model_name="rmse_model",
            model_kind="linear",
            version="v2",
            training_data_window={"start_ts_ms": 11, "end_ts_ms": 20},
            performance_metrics={"rmse": 0.18},
            selection_metric_name="rmse",
            selection_metric_higher_is_better=False,
            is_active=True,
        )

        best = registry.get_best_model("MSFT")
        self.assertIsNotNone(best)
        self.assertEqual(str(best.get("version") or ""), "v2")
        self.assertEqual(str(best.get("best_metric_name") or ""), "rmse")
        self.assertFalse(bool(best.get("best_metric_higher_is_better")))

        best_by_metric_name = registry.get_best_model("MSFT", metric_name="rmse")
        self.assertIsNotNone(best_by_metric_name)
        self.assertEqual(str(best_by_metric_name.get("version") or ""), "v2")

    def test_catalog_register_normalizes_object_store_artifact_manifest(self) -> None:
        (_, storage, registry) = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.model_registry",
        )
        storage.init_db()

        record = registry.register_model(
            symbol="NVDA",
            model_name="temporal_predictor",
            model_kind="temporal",
            version="v3",
            artifact_uri="s3://model-artifacts/nvda/temporal_predictor_v3.pkl",
            metadata={
                "framework": "torch",
                "artifact_manifest": {
                    "version_id": "model-v3",
                    "sha256": "abc123def456",
                    "size_bytes": 2048,
                    "content_type": "application/octet-stream",
                },
            },
            performance_metrics={"quality_score": 0.91},
            is_active=True,
        )

        self.assertIsNotNone(record)
        manifest = dict(record.get("artifact_manifest") or {})
        self.assertEqual(str(record.get("artifact_uri") or ""), "s3://model-artifacts/nvda/temporal_predictor_v3.pkl")
        self.assertEqual(str(manifest.get("storage_backend") or ""), "object")
        self.assertEqual(str(manifest.get("bucket") or ""), "model-artifacts")
        self.assertEqual(str(manifest.get("key") or ""), "nvda/temporal_predictor_v3.pkl")
        self.assertEqual(str(manifest.get("version_id") or ""), "model-v3")
        self.assertTrue(bool(manifest.get("immutable")))

        loaded = registry.load_model("NVDA", model_name="temporal_predictor", version="v3")
        self.assertIsNotNone(loaded)
        loaded_manifest = dict(loaded.get("artifact_manifest") or {})
        self.assertEqual(str(loaded_manifest.get("sha256") or ""), "abc123def456")
        self.assertEqual(int(loaded_manifest.get("size_bytes") or 0), 2048)
        self.assertEqual(str(dict(loaded.get("metadata") or {}).get("framework") or ""), "torch")

    def test_object_store_registration_requires_immutable_identity(self) -> None:
        (_, storage, registry) = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.model_registry",
        )
        storage.init_db()

        with self.assertRaisesRegex(ValueError, "object_storage_artifact_requires_immutable_identity"):
            registry.register_model(
                symbol="NVDA",
                model_name="temporal_predictor",
                model_kind="temporal",
                version="v4",
                artifact_uri="s3://model-artifacts/nvda/temporal_predictor_v4.pkl",
                metadata={"framework": "torch"},
                performance_metrics={"quality_score": 0.92},
            )

    def test_legacy_stage_registry_registration_still_works(self) -> None:
        (_, storage, registry) = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.model_registry",
        )
        storage.init_db()

        registry.register_model(
            model_name="embed_regressor",
            model_kind="baseline",
            model_ts_ms=123456789,
            stage="challenger",
            metrics={"rmse": 0.11, "directional_acc": 0.57},
            regime="global",
        )

        latest = registry.get_stage_latest("embed_regressor", "challenger", regime="global")
        self.assertIsNotNone(latest)
        self.assertEqual(str(latest.get("model_kind") or ""), "baseline")
        self.assertEqual(int(latest.get("model_ts_ms") or 0), 123456789)
        self.assertEqual(float(latest.get("metrics", {}).get("rmse") or 0.0), 0.11)


if __name__ == "__main__":
    unittest.main()
