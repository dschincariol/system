"""Regression tests for the LightGBM structured-feature model family."""

from __future__ import annotations

import importlib
import importlib.util
import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

MODEL_NAME = "gbm_regressor_AAPL_1710000123456_abcdef1"
MODEL_VERSION = "gbm-20260411-test"
HORIZON_S = 300
FEATURE_IDS = [
    "base.source_credibility",
    "base.log_recency_hours",
    "base.normalized_text_len",
    "base.scheduled_flag",
]
LIGHTGBM_AVAILABLE = importlib.util.find_spec("lightgbm") is not None


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


@unittest.skipUnless(LIGHTGBM_AVAILABLE, "lightgbm not installed")
class GBMRegressorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.tmp.name) / "gbm_regressor.db"
        self._env_backup = {
            key: os.environ.get(key)
            for key in (
                "DB_PATH",
                "MODEL_CONFIG_JSON",
                "USE_GBM_REGRESSOR",
                "USE_EMBED_REGRESSOR",
                "GBM_NUM_LEAVES",
                "GBM_LEARNING_RATE",
                "GBM_N_ESTIMATORS",
                "GBM_MIN_CHILD_SAMPLES",
                "TS_ARTIFACTS_ROOT",
            )
        }
        self._configure_env(use_gbm=True)
        (
            _,
            self.storage,
            self.model_config,
            self.feature_registry,
            self.registry,
            self.lifecycle,
            self.gbm,
            self.predictor,
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
        except Exception as e:
            sys.stderr.write(f"[test_gbm_regressor] close_pooled_connections_failed: {type(e).__name__}: {e}\n")
        self.tmp.cleanup()

    def _configure_env(self, *, use_gbm: bool) -> None:
        os.environ["DB_PATH"] = str(self.db_path)
        os.environ["MODEL_CONFIG_JSON"] = json.dumps(
            [
                {
                    "model_name": MODEL_NAME,
                    "family": "gbm_regressor",
                    "horizons_s": [HORIZON_S],
                    "feature_ids": list(FEATURE_IDS),
                    "symbol_universe": ["AAPL"],
                    "model_kind": "lightgbm",
                    "prediction_enabled": True,
                    "experimental": False,
                    "enabled": True,
                }
            ],
            separators=(",", ":"),
            sort_keys=True,
        )
        os.environ["USE_GBM_REGRESSOR"] = "1" if use_gbm else "0"
        os.environ["USE_EMBED_REGRESSOR"] = "0"
        os.environ["GBM_NUM_LEAVES"] = "7"
        os.environ["GBM_LEARNING_RATE"] = "0.1"
        os.environ["GBM_N_ESTIMATORS"] = "16"
        os.environ["GBM_MIN_CHILD_SAMPLES"] = "1"
        os.environ["TS_ARTIFACTS_ROOT"] = str(Path(self.tmp.name) / "artifacts")

    def _reload_stack(self):
        return _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.strategy.model_config",
            "engine.strategy.feature_registry",
            "engine.model_registry",
            "engine.strategy.model_lifecycle",
            "engine.strategy.gbm_regressor",
            "engine.strategy.predictor",
        )

    def _event(self) -> dict[str, object]:
        return {
            "event_id": 101,
            "ts_ms": 1_710_000_000_000,
            "ref_ts_ms": 1_710_003_600_000,
            "title": "Fed CPI earnings schedule update",
            "body": "Macro surprise drives repricing across rates and equities.",
            "source": "rss:reuters",
        }

    def _training_hyperparams(self) -> dict[str, object]:
        return {
            "num_leaves": 7,
            "learning_rate": 0.1,
            "n_estimators": 16,
            "min_child_samples": 1,
            "random_state": 7,
            "n_jobs": 1,
        }

    def _training_dataset(self) -> tuple[np.ndarray, np.ndarray]:
        X = np.asarray(
            [
                [0.90, 0.05, 0.20, 1.0],
                [0.85, 0.10, 0.35, 1.0],
                [0.75, 0.25, 0.45, 0.0],
                [0.60, 0.45, 0.55, 0.0],
                [0.40, 0.65, 0.70, 0.0],
                [0.25, 0.90, 0.85, 0.0],
            ],
            dtype=np.float32,
        )
        y = np.asarray([1.20, 1.00, 0.55, 0.10, -0.45, -0.90], dtype=np.float32)
        return X, y

    def _train_blob(self) -> bytes:
        X, y = self._training_dataset()
        return self.gbm.train_gbm_model(
            X,
            y,
            feature_ids=list(FEATURE_IDS),
            hyperparams=self._training_hyperparams(),
        )

    def _persist_live_model(self) -> dict[str, object]:
        blob = self._train_blob()
        created_ts = 1_710_000_123_456
        feature_schema = {
            "feature_ids": list(FEATURE_IDS),
            "feature_set_tag": self.feature_registry.feature_set_tag_from_ids(list(FEATURE_IDS)),
            "feature_count": len(FEATURE_IDS),
            "ts_ms": created_ts,
        }
        training_metrics = {"n_train": 6, "rmse": 0.05, "directional_acc": 0.83}

        con = self.storage.connect()
        try:
            self.gbm.persist_gbm_model_record(
                con,
                model_name=MODEL_NAME,
                version=MODEL_VERSION,
                created_ts=created_ts,
                blob=blob,
                feature_schema=feature_schema,
                training_metrics=training_metrics,
            )
            con.commit()
        finally:
            con.close()

        cfg = self.model_config.get_model_config(MODEL_NAME)
        stage_metrics = self.model_config.build_model_registration_metadata(cfg)
        stage_metrics.update(
            {
                "model_version": MODEL_VERSION,
                "model_family": "gbm_regressor",
                "feature_schema": dict(feature_schema),
            }
        )
        self.registry.register_model(
            model_name=MODEL_NAME,
            model_kind="lightgbm",
            model_ts_ms=created_ts,
            stage="challenger",
            metrics=stage_metrics,
            regime="global",
        )
        self.lifecycle.register_model_version(
            model_name=MODEL_NAME,
            model_version=MODEL_VERSION,
            model_kind="lightgbm",
            stage="shadow",
            status="trained",
            live_ready=False,
            training_job_name="train_gbm_regressor",
            train_scope={
                "horizon_s": HORIZON_S,
                "feature_ids": list(FEATURE_IDS),
                "feature_set_tag": str(feature_schema["feature_set_tag"]),
            },
            meta=dict(stage_metrics),
        )
        from engine.runtime.runtime_meta import meta_set
        from engine.strategy.promotion_audit import record_statistical_evidence

        record_statistical_evidence(
            model_id=MODEL_NAME,
            test_name="white_reality_check",
            p_value=0.01,
            decision="pass",
            payload={"source": "gbm_regressor_fixture"},
        )
        replay_ts_ms = int(time.time() * 1000)
        replay_payload = {
            "ok": True,
            "updated_ts_ms": int(replay_ts_ms),
            "models": {
                f"{MODEL_NAME}|AAPL|{HORIZON_S}|global": {
                    "model_name": MODEL_NAME,
                    "model_id": MODEL_NAME,
                    "symbol": "AAPL",
                    "horizon_s": HORIZON_S,
                    "regime": "global",
                    "model_kind": "lightgbm",
                    "model_ts_ms": int(created_ts),
                    "approved": True,
                }
            },
        }
        replay_status = {"ok": True, "status": "ready", "updated_ts_ms": int(replay_ts_ms)}
        meta_set("competition_replay_validation", json.dumps(replay_payload, separators=(",", ":"), sort_keys=True))
        meta_set(
            "competition_replay_validation_status",
            json.dumps(replay_status, separators=(",", ":"), sort_keys=True),
        )
        self.registry.promote_to_champion(MODEL_NAME, "lightgbm", created_ts, regime="global")
        self.lifecycle.mark_version_live(MODEL_NAME, MODEL_VERSION, stage="champion")
        return {
            "blob": blob,
            "created_ts": created_ts,
            "feature_schema": feature_schema,
            "training_metrics": training_metrics,
        }

    def test_blob_round_trip_serialization_deserialization(self) -> None:
        blob = self._train_blob()

        model, schema = self.gbm.load_gbm_model(blob)

        self.assertIsNotNone(model)
        self.assertEqual(list(schema.get("feature_ids") or []), list(FEATURE_IDS))
        self.assertEqual(str(schema.get("feature_set_tag") or ""), self.feature_registry.feature_set_tag_from_ids(list(FEATURE_IDS)))

        prediction, diagnostics = self.gbm.predict_with_gbm_model(
            blob,
            {"features": {FEATURE_IDS[0]: 0.8, FEATURE_IDS[1]: 0.2, FEATURE_IDS[2]: 0.4, FEATURE_IDS[3]: 1.0}},
        )
        self.assertTrue(np.isfinite(prediction))
        self.assertEqual(list(diagnostics.get("missing_feature_ids") or []), [])
        self.assertAlmostEqual(float(diagnostics.get("feature_coverage") or 0.0), 1.0, places=6)

    def test_training_on_minimal_sample_dataset(self) -> None:
        X = np.asarray(
            [
                [0.95, 0.05, 0.15, 1.0],
                [0.70, 0.20, 0.40, 1.0],
                [0.35, 0.60, 0.70, 0.0],
                [0.10, 0.90, 0.95, 0.0],
            ],
            dtype=np.float32,
        )
        y = np.asarray([1.1, 0.6, -0.2, -0.8], dtype=np.float32)

        blob = self.gbm.train_gbm_model(
            X,
            y,
            feature_ids=list(FEATURE_IDS),
            hyperparams=self._training_hyperparams(),
        )

        preds = []
        for row in X:
            pred, _ = self.gbm.predict_with_gbm_model(blob, {"features": dict(zip(FEATURE_IDS, row.tolist()))})
            preds.append(float(pred))
        preds_arr = np.asarray(preds, dtype=np.float32)

        self.assertEqual(preds_arr.shape[0], X.shape[0])
        self.assertTrue(np.all(np.isfinite(preds_arr)))
        self.assertGreater(float(np.std(preds_arr)), 0.0)

    def test_registry_persistence(self) -> None:
        persisted = self._persist_live_model()

        record = self.gbm.load_gbm_model_record(MODEL_NAME, MODEL_VERSION)
        self.assertIsNotNone(record)
        self.assertEqual(str(record.get("model_name") or ""), MODEL_NAME)
        self.assertEqual(str(record.get("version") or ""), MODEL_VERSION)
        self.assertEqual(dict(record.get("feature_schema") or {}), dict(persisted["feature_schema"]))
        self.assertEqual(int(dict(record.get("training_metrics") or {}).get("n_train") or 0), 6)

        spec = self.registry.get_model_spec(MODEL_NAME, regime="global")
        self.assertEqual(str(spec.get("model_family") or ""), "gbm_regressor")
        self.assertEqual(str(spec.get("model_version") or ""), MODEL_VERSION)
        self.assertEqual(list(spec.get("feature_ids") or []), list(FEATURE_IDS))

        con = self.storage.connect(readonly=True)
        try:
            row = con.execute(
                """
                SELECT model_name, version, created_ts
                FROM gbm_models
                WHERE model_name=? AND version=?
                """,
                (MODEL_NAME, MODEL_VERSION),
            ).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(row)
        self.assertEqual(str(row[0]), MODEL_NAME)
        self.assertEqual(str(row[1]), MODEL_VERSION)
        self.assertEqual(int(row[2]), int(persisted["created_ts"]))

    def test_predictor_live_inference_uses_persisted_blob(self) -> None:
        persisted = self._persist_live_model()
        event = self._event()
        snapshot = self.feature_registry.build_feature_snapshot(
            event=event,
            symbol="AAPL",
            feature_ids=list(FEATURE_IDS),
        )
        expected_prediction, expected_diagnostics = self.gbm.predict_with_gbm_model(
            persisted["blob"],
            {"features": dict(snapshot)},
        )

        with patch.object(self.predictor, "_knn_raw", return_value=(0.0, 0.0, {})):
            with patch.object(
                self.predictor,
                "_blend_with_priors",
                side_effect=lambda symbol, horizon_s, pred_z, support: (float(pred_z), 0.73, {"prior": "none", "prior_n": 0}),
            ):
                with patch.object(self.predictor, "_track_prediction_output", return_value=None):
                    prediction, confidence, explain = self.predictor.predict_forced_model(
                        np.asarray([0.0], dtype=np.float32),
                        symbol="AAPL",
                        horizon_s=HORIZON_S,
                        model_name=MODEL_NAME,
                        event=event,
                    )

        self.assertAlmostEqual(float(prediction), float(expected_prediction), places=6)
        self.assertAlmostEqual(float(confidence), 0.73, places=6)
        self.assertEqual(str(explain.get("model") or ""), "gbm_regressor")
        self.assertEqual(str(explain.get("model_version") or ""), MODEL_VERSION)
        self.assertEqual(str(explain.get("model_family") or ""), "gbm_regressor")
        self.assertAlmostEqual(
            float(explain.get("feature_coverage") or 0.0),
            float(expected_diagnostics.get("feature_coverage") or 0.0),
            places=6,
        )
        self.assertEqual(list(explain.get("missing_feature_ids") or []), [])

    def test_disabled_path_backward_compatibility(self) -> None:
        self._configure_env(use_gbm=False)
        (
            _,
            self.storage,
            self.model_config,
            self.feature_registry,
            self.registry,
            self.lifecycle,
            self.gbm,
            self.predictor,
        ) = self._reload_stack()
        self.storage.init_db()
        self._persist_live_model()

        with patch.object(self.predictor, "_knn_raw", return_value=(0.12, 2.5, {"neighbors": []})):
            with patch.object(
                self.predictor,
                "_blend_with_priors",
                return_value=(0.12, 0.33, {"prior": "global", "prior_n": 3}),
            ):
                with patch.object(self.predictor, "_track_prediction_output", return_value=None):
                    prediction, confidence, explain = self.predictor.predict_forced_model(
                        np.asarray([0.0], dtype=np.float32),
                        symbol="AAPL",
                        horizon_s=HORIZON_S,
                        model_name=MODEL_NAME,
                        event=self._event(),
                    )

        self.assertAlmostEqual(float(prediction), 0.12, places=6)
        self.assertAlmostEqual(float(confidence), 0.33, places=6)
        self.assertEqual(str(explain.get("model_family") or ""), "gbm_regressor")
        self.assertEqual(
            dict(explain.get("serve_fallback") or {}).get("served_family"),
            "knn_prior",
        )
        self.assertEqual(
            dict(explain.get("serve_fallback") or {}).get("requested_family"),
            "gbm_regressor",
        )


if __name__ == "__main__":
    unittest.main()
