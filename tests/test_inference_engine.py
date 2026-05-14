from __future__ import annotations

import importlib
import os
import pickle
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


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class InferenceEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "inference_engine.db")
        os.environ.pop("TIMESCALE_ENABLED", None)
        os.environ.pop("ARTIFACT_STORE_MIRROR_ROOT", None)
        (
            self.storage,
            self.feature_store,
            self.registry,
            self.engine_ensemble,
            self.public_ensemble,
            self.engine_regime_detector,
            self.public_regime_detector,
            self.engine_inference,
            self.public_inference,
            self.predictor,
        ) = _reload_modules(
            "engine.runtime.storage",
            "engine.data.feature_store",
            "engine.model_registry",
            "engine.ensemble_engine",
            "ensemble_engine",
            "engine.regime_detector",
            "regime_detector",
            "engine.inference_engine",
            "inference_engine",
            "engine.strategy.predictor",
        )
        self.storage.init_db()

    def tearDown(self) -> None:
        try:
            self.public_regime_detector.shutdown_regime_detector(timeout_s=1.0)
        except Exception:
            pass
        try:
            self.storage.close_pooled_connections()
        except Exception:
            pass
        self.tmp.cleanup()

    def _store_feature_snapshot(
        self,
        symbol: str,
        *,
        scale: float = 1.0,
        overrides: dict[str, float] | None = None,
    ) -> dict[str, object]:
        now_ms = int(time.time() * 1000)
        feature_map = {
            name: float(scale * float(idx + 1))
            for idx, name in enumerate(self.feature_store.FEATURE_NAMES)
        }
        feature_map.update({str(key): float(value) for key, value in dict(overrides or {}).items()})
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

    def _register_linear_artifact(self, symbol: str, *, model_name: str = "rt_linear", version: str = "v1") -> Path:
        artifact_path = Path(self.tmp.name) / f"{symbol.lower()}_{model_name}_{version}.pkl"
        payload = {
            "weights": [0.01 for _ in self.feature_store.FEATURE_NAMES],
            "bias": 0.5,
            "confidence": 0.88,
        }
        with artifact_path.open("wb") as handle:
            pickle.dump(payload, handle)
        self.registry.register_model(
            symbol=str(symbol).upper(),
            model_name=str(model_name),
            model_kind="linear",
            version=str(version),
            artifact_uri=str(artifact_path),
            metadata={
                "model_id": f"{model_name}:{symbol.upper()}:{version}",
                "feature_ids": list(self.feature_store.FEATURE_NAMES),
                "horizon_s": 300,
            },
            performance_metrics={"quality_score": 0.88},
            is_active=True,
        )
        return artifact_path

    def _register_constant_artifact(
        self,
        symbol: str,
        *,
        model_name: str,
        version: str,
        prediction: float,
        confidence: float,
        performance_metrics: dict[str, float] | None = None,
        metadata: dict[str, object] | None = None,
        is_active: bool = True,
    ) -> Path:
        artifact_path = Path(self.tmp.name) / f"{symbol.lower()}_{model_name}_{version}_constant.pkl"
        payload = {
            "prediction": float(prediction),
            "confidence": float(confidence),
        }
        with artifact_path.open("wb") as handle:
            pickle.dump(payload, handle)
        model_metadata = {
            "model_id": f"{model_name}:{symbol.upper()}:{version}",
            "feature_ids": list(self.feature_store.FEATURE_NAMES),
            "horizon_s": 300,
        }
        model_metadata.update(dict(metadata or {}))
        self.registry.register_model(
            symbol=str(symbol).upper(),
            model_name=str(model_name),
            model_kind="constant",
            version=str(version),
            artifact_uri=str(artifact_path),
            metadata=model_metadata,
            performance_metrics=dict(performance_metrics or {}),
            is_active=bool(is_active),
        )
        return artifact_path

    def _register_object_store_artifact(
        self,
        symbol: str,
        *,
        model_name: str = "rt_object_store",
        version: str = "v1",
        payload: dict[str, object] | None = None,
    ) -> Path:
        os.environ["ARTIFACT_STORE_MIRROR_ROOT"] = str(Path(self.tmp.name) / "artifact_mirror")
        self.addCleanup(os.environ.pop, "ARTIFACT_STORE_MIRROR_ROOT", None)
        (artifact_store,) = _reload_modules("engine.runtime.artifact_store")
        artifact_uri = f"s3://model-artifacts/{symbol.lower()}/{model_name}_{version}.pkl"
        manifest = artifact_store.build_artifact_manifest(
            artifact_uri,
            {
                "artifact_manifest": {
                    "version_id": f"{model_name}-{version}",
                    "sha256": f"sha256-{symbol.lower()}-{model_name}-{version}",
                }
            },
        )
        self.assertIsNotNone(manifest)
        mirror_path = Path(str(manifest.get("local_mirror_path") or ""))
        mirror_path.parent.mkdir(parents=True, exist_ok=True)
        object_payload = dict(
            payload
            or {
                "weights": [0.02 for _ in self.feature_store.FEATURE_NAMES],
                "bias": 0.25,
                "confidence": 0.91,
            }
        )
        with mirror_path.open("wb") as handle:
            pickle.dump(object_payload, handle)
        self.registry.register_model(
            symbol=str(symbol).upper(),
            model_name=str(model_name),
            model_kind="linear",
            version=str(version),
            artifact_uri=artifact_uri,
            metadata={
                "model_id": f"{model_name}:{symbol.upper()}:{version}",
                "feature_ids": list(self.feature_store.FEATURE_NAMES),
                "horizon_s": 300,
                "artifact_manifest": {
                    "version_id": f"{model_name}-{version}",
                    "sha256": f"sha256-{symbol.lower()}-{model_name}-{version}",
                },
            },
            performance_metrics={"quality_score": 0.91},
            is_active=True,
        )
        return mirror_path

    def test_predict_scores_registered_model_and_persists_prediction(self) -> None:
        self._store_feature_snapshot("AAPL")
        self._register_linear_artifact("AAPL")

        result = self.public_inference.predict("AAPL")

        expected_prediction = 0.5 + (0.01 * sum(range(1, len(self.feature_store.FEATURE_NAMES) + 1)))
        self.assertEqual(result["status"], "ok")
        self.assertFalse(bool(result["safe_output"]))
        self.assertEqual(result["symbol"], "AAPL")
        self.assertAlmostEqual(float(result["prediction"]), float(expected_prediction), places=6)
        self.assertAlmostEqual(float(result["confidence"]), 0.88, places=6)
        self.assertEqual(str(result["model_name"]), "rt_linear")
        self.assertEqual(str(result["model_version"]), "v1")

        con = self.storage.connect(readonly=True)
        try:
            row = con.execute(
                """
                SELECT symbol, horizon_s, predicted_z, confidence, model_name, model_id, model_version
                FROM predictions
                ORDER BY ts_ms DESC, id DESC
                LIMIT 1
                """
            ).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(row)
        self.assertEqual(str(row[0]), "AAPL")
        self.assertEqual(int(row[1]), 300)
        self.assertAlmostEqual(float(row[2]), float(expected_prediction), places=6)
        self.assertAlmostEqual(float(row[3]), 0.88, places=6)
        self.assertEqual(str(row[4]), "rt_linear")
        self.assertEqual(str(row[6]), "v1")

    def test_predict_attaches_and_persists_market_regime(self) -> None:
        self._store_feature_snapshot(
            "AAPL",
            overrides={
                "volatility_20": 0.05,
                "volatility_60": 0.02,
                "atr_pct_14": 0.03,
                "momentum_1h": 0.01,
                "momentum_1d": 0.02,
                "rolling_return_1d": 0.02,
                "trend_strength_20": 1.8,
                "volume_rel_20": 1.5,
                "dollar_volume_rel_20": 1.7,
                "volume_nonzero_share_20": 0.95,
                "dollar_volume_last": 7_500_000.0,
            },
        )
        self._register_linear_artifact("AAPL")

        result = self.public_inference.predict("AAPL")
        self.assertEqual(str(result["volatility_regime"]), "unknown")
        self.assertEqual(str(result["trend_regime"]), "unknown")
        self.assertEqual(str(result["liquidity_regime"]), "unknown")
        self.assertTrue(bool(self.public_regime_detector.flush_regime_detector(2.0)))

        warmed = self.public_inference.predict("AAPL", persist=False)
        self.assertEqual(str(warmed["volatility_regime"]), "high")
        self.assertEqual(str(warmed["trend_regime"]), "bullish")
        self.assertEqual(str(warmed["liquidity_regime"]), "deep")

        con = self.storage.connect(readonly=True)
        try:
            prediction_row = con.execute(
                """
                SELECT regime_time_ms, volatility_regime, trend_regime, liquidity_regime
                FROM predictions
                ORDER BY ts_ms DESC, id DESC
                LIMIT 1
                """
            ).fetchone()
            regime_row = con.execute(
                """
                SELECT volatility_regime, trend_regime, liquidity_regime
                FROM regime_state
                WHERE symbol='AAPL'
                ORDER BY time DESC
                LIMIT 1
                """
            ).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(prediction_row)
        self.assertGreater(int(prediction_row[0] or 0), 0)
        self.assertEqual(tuple(str(value) for value in prediction_row[1:]), ("high", "bullish", "deep"))
        self.assertEqual(tuple(str(value) for value in regime_row), ("high", "bullish", "deep"))

    def test_predict_combines_multiple_active_models_with_weighted_ensemble(self) -> None:
        self._store_feature_snapshot("AMD")
        self._register_constant_artifact(
            "AMD",
            model_name="trend_follow",
            version="v1",
            prediction=1.2,
            confidence=0.92,
            performance_metrics={"accuracy": 0.90},
            metadata={"recent_performance": 0.80},
        )
        self._register_constant_artifact(
            "AMD",
            model_name="mean_revert",
            version="v1",
            prediction=-0.4,
            confidence=0.55,
            performance_metrics={"accuracy": 0.45},
            metadata={"recent_performance": 0.35},
        )

        expected = self.public_ensemble.combine_predictions(
            [
                {
                    "model_name": "trend_follow",
                    "model_version": "v1",
                    "prediction": 1.2,
                    "confidence": 0.92,
                    "performance_metrics": {"accuracy": 0.90},
                    "metadata": {"recent_performance": 0.80},
                },
                {
                    "model_name": "mean_revert",
                    "model_version": "v1",
                    "prediction": -0.4,
                    "confidence": 0.55,
                    "performance_metrics": {"accuracy": 0.45},
                    "metadata": {"recent_performance": 0.35},
                },
            ],
            method="weighted_average",
        )

        result = self.public_inference.predict(
            "AMD",
            persist=False,
            ensemble_enabled=True,
            ensemble_method="weighted_average",
        )

        self.assertEqual(result["status"], "ok")
        self.assertFalse(bool(result["safe_output"]))
        self.assertEqual(str(result["model_kind"]), "ensemble")
        self.assertEqual(str(result["model_name"]), "ensemble_weighted_average")
        self.assertEqual(int(result["ensemble_size"]), 2)
        self.assertAlmostEqual(float(result["prediction"]), float(expected["final_prediction"]), places=6)
        self.assertAlmostEqual(float(result["confidence"]), float(expected["aggregated_confidence"]), places=6)
        self.assertAlmostEqual(
            float(result["ensemble_output"]["final_prediction"]),
            float(result["prediction"]),
            places=6,
        )
        self.assertAlmostEqual(
            float(result["ensemble_output"]["aggregated_confidence"]),
            float(result["confidence"]),
            places=6,
        )
        self.assertFalse(bool(result["ensemble_output"]["fallback"]))
        self.assertEqual(len(result["ensemble_members"]), 2)
        self.assertEqual(str(result["ensemble_members"][0]["model_name"]), "trend_follow")

    def test_ensemble_persist_logs_model_version_and_component_vector(self) -> None:
        self._store_feature_snapshot("AMD")
        self._register_constant_artifact(
            "AMD",
            model_name="trend_follow",
            version="v1",
            prediction=1.2,
            confidence=0.92,
            performance_metrics={"accuracy": 0.90},
            metadata={"recent_performance": 0.80},
        )
        self._register_constant_artifact(
            "AMD",
            model_name="mean_revert",
            version="v2",
            prediction=-0.4,
            confidence=0.55,
            performance_metrics={"accuracy": 0.45},
            metadata={"recent_performance": 0.35},
        )
        result = self.public_inference.predict(
            "AMD",
            persist=False,
            ensemble_enabled=True,
            ensemble_method="weighted_average",
        )
        self.assertEqual(str(result["model_kind"]), "ensemble")
        self.assertTrue(str(result.get("model_version") or "").startswith("ensemble:weighted_average:"))

        stored = []
        logged = []

        def fake_store_prediction(*args, **kwargs):
            stored.append((args, kwargs))

        def fake_log_decision(**kwargs):
            logged.append(kwargs)

        with patch.object(self.engine_inference, "store_prediction", side_effect=fake_store_prediction):
            with patch("engine.strategy.decision_log.log_decision", side_effect=fake_log_decision):
                self.engine_inference._persist_prediction_output(result)

        self.assertEqual(len(stored), 1)
        self.assertEqual(len(logged), 1)
        self.assertEqual(logged[0]["model_version"], result["model_version"])
        self.assertEqual(set(logged[0]["component_vector"]["components"]), {"trend_follow", "mean_revert"})

    def test_predict_does_not_enable_ensemble_by_default(self) -> None:
        self._store_feature_snapshot("AMD")
        self._register_constant_artifact(
            "AMD",
            model_name="trend_follow",
            version="v1",
            prediction=1.2,
            confidence=0.92,
            performance_metrics={"accuracy": 0.90},
            metadata={"recent_performance": 0.80},
        )
        self._register_constant_artifact(
            "AMD",
            model_name="mean_revert",
            version="v1",
            prediction=-0.4,
            confidence=0.55,
            performance_metrics={"accuracy": 0.45},
            metadata={"recent_performance": 0.35},
        )

        result = self.public_inference.predict("AMD", persist=False)

        self.assertEqual(result["status"], "ok")
        self.assertFalse(bool(result["safe_output"]))
        self.assertNotEqual(str(result["model_kind"]), "ensemble")
        self.assertNotEqual(str(result["model_name"]), "ensemble_weighted_average")

    def test_predict_emits_latency_metric_and_updates_component_health(self) -> None:
        self._store_feature_snapshot("AAPL")
        self._register_linear_artifact("AAPL")
        metrics_store, observability, data_quality = _reload_modules(
            "engine.runtime.metrics_store",
            "engine.runtime.observability",
            "engine.runtime.data_quality",
        )

        result = self.public_inference.predict("AAPL", persist=False)

        self.assertEqual(result["status"], "ok")
        metrics = metrics_store.get_runtime_metrics(metric="inference_latency_ms")
        self.assertTrue(bool(metrics["ok"]))
        self.assertTrue(
            any(
                str(row["tags"].get("symbol") or "") == "AAPL"
                and float(row["value_num"] or 0.0) >= 0.0
                for row in (metrics.get("rows") or [])
            )
        )

        health = observability.get_component_health_snapshot("inference")
        self.assertTrue(bool(health.get("ok")))
        self.assertEqual(str(health.get("status") or ""), "ok")
        self.assertEqual(str(health.get("symbol") or ""), "AAPL")

        scoring = data_quality.get_scoring_pipeline_snapshot()
        self.assertTrue(bool(scoring.get("ok")))
        self.assertTrue(bool(scoring.get("model_loaded")))
        self.assertEqual(str(scoring.get("model_name") or ""), "rt_linear")
        self.assertGreater(int(scoring.get("last_success_ts_ms") or 0), 0)

    def test_predict_loads_object_store_artifact_from_local_mirror(self) -> None:
        self._store_feature_snapshot("IBM")
        self._register_object_store_artifact("IBM")

        result = self.public_inference.predict("IBM", persist=False)

        expected_prediction = 0.25 + (0.02 * sum(range(1, len(self.feature_store.FEATURE_NAMES) + 1)))
        self.assertEqual(result["status"], "ok")
        self.assertFalse(bool(result["safe_output"]))
        self.assertEqual(str(result["model_name"]), "rt_object_store")
        self.assertAlmostEqual(float(result["prediction"]), float(expected_prediction), places=6)
        self.assertAlmostEqual(float(result["confidence"]), 0.91, places=6)

        loaded = self.registry.load_model("IBM", model_name="rt_object_store", version="v1")
        self.assertIsNotNone(loaded)
        manifest = dict(loaded.get("artifact_manifest") or {})
        self.assertEqual(str(manifest.get("storage_backend") or ""), "object")
        self.assertTrue(bool(manifest.get("immutable")))
        self.assertEqual(str(manifest.get("bucket") or ""), "model-artifacts")

    def test_predict_returns_safe_default_when_model_registry_misses(self) -> None:
        self._store_feature_snapshot("MSFT")

        result = self.public_inference.predict("MSFT", persist=False)

        self.assertTrue(bool(result["safe_output"]))
        self.assertEqual(result["status"], "safe_default")
        self.assertEqual(result["symbol"], "MSFT")
        self.assertEqual(float(result["prediction"]), 0.0)
        self.assertEqual(float(result["confidence"]), 0.0)
        self.assertEqual(str(result["fallback_reason"]), "model_registry_miss")

    def test_predict_timeout_returns_safe_default(self) -> None:
        self._store_feature_snapshot("NVDA")
        self._register_linear_artifact("NVDA")
        now_ms = int(time.time() * 1000)

        def _slow_predict(*args, **kwargs):
            time.sleep(0.2)
            return {
                "symbol": "NVDA",
                "prediction": 1.0,
                "confidence": 0.9,
                "prediction_strength": 0.9,
                "horizon_s": 300,
                "model_name": "rt_linear",
                "model_id": "rt_linear:NVDA:v1",
                "model_version": "v1",
                "model_kind": "linear",
                "feature_ts_ms": int(now_ms),
                "feature_set_tag": str(self.feature_store.FEATURE_SET_TAG),
                "ts_ms": int(now_ms + 100),
                "timed_out": False,
                "safe_output": False,
                "fallback_reason": None,
                "status": "ok",
            }

        with patch.object(self.engine_inference, "_predict_blocking", side_effect=_slow_predict):
            result = self.public_inference.predict("NVDA", timeout_s=0.01, persist=False)

        self.assertTrue(bool(result["safe_output"]))
        self.assertTrue(bool(result["timed_out"]))
        self.assertEqual(result["status"], "safe_default")
        self.assertTrue(str(result["fallback_reason"]).startswith("timeout:"))
        self.assertEqual(int(result["ensemble_output"]["ensemble_size"]), 0)
        self.assertTrue(bool(result["ensemble_output"]["fallback"]))

    def test_predict_rejects_invalid_feature_snapshot_and_records_validation_failures(self) -> None:
        self._register_linear_artifact("AAPL")
        (data_quality,) = _reload_modules("engine.runtime.data_quality")
        now_ms = int(time.time() * 1000)
        missing_feature = str(self.feature_store.FEATURE_NAMES[0])
        bad_snapshot = {
            "symbol": "AAPL",
            "ts_ms": int(now_ms),
            "schema_version": int(getattr(self.feature_store, "FEATURE_SCHEMA_VERSION", 1)),
            "feature_set_tag": str(self.feature_store.FEATURE_SET_TAG),
            "feature_names": list(self.feature_store.FEATURE_NAMES),
            "vector": [float(idx + 1) for idx in range(len(self.feature_store.FEATURE_NAMES) - 1)],
            "point_count": 64,
            "source_timestamps": {"price_history_last_ts_ms": int(now_ms)},
            "features": {
                str(name): float(idx + 1)
                for idx, name in enumerate(self.feature_store.FEATURE_NAMES[1:], start=1)
            },
        }

        with patch.object(self.engine_inference, "read_online_feature_snapshot", return_value=bad_snapshot):
            result = self.public_inference.predict("AAPL", persist=False)

        self.assertTrue(bool(result["safe_output"]))
        self.assertEqual(str(result["fallback_reason"]), "feature_required_fields_missing")

        model_input = data_quality.get_model_input_validation_snapshot()
        self.assertFalse(bool(model_input.get("ok")))
        self.assertEqual(str(model_input.get("detail") or ""), "feature_required_fields_missing")
        self.assertIn(missing_feature, list(model_input.get("missing_feature_ids") or []))

        scoring = data_quality.get_scoring_pipeline_snapshot()
        self.assertFalse(bool(scoring.get("ok")))
        self.assertEqual(str(scoring.get("fallback_reason") or ""), "feature_required_fields_missing")
        self.assertEqual(int(scoring.get("invalid_input_count_total") or 0), 1)

    def test_project_feature_vector_uses_runtime_feature_contract_fallback(self) -> None:
        record = {
            "model_name": "runtime_contract_model",
            "metadata": {},
        }
        feature_snapshot = {
            "symbol": "AAPL",
            "ts_ms": int(time.time() * 1000),
            "feature_set_tag": "runtime.contract.v1",
            "vector": [2.0, 4.0],
            "features": {},
        }
        with patch.object(
            self.engine_inference,
            "get_online_feature_contract",
            return_value={
                "ok": True,
                "feature_names": ["alpha", "beta"],
                "feature_set_tag": "runtime.contract.v1",
                "schema_version": 1,
            },
        ):
            vector, feature_ids, coverage = self.engine_inference._project_feature_vector(feature_snapshot, record)

        self.assertEqual(list(feature_ids), ["alpha", "beta"])
        self.assertEqual(float(coverage), 1.0)
        self.assertEqual(list(vector.tolist()), [2.0, 4.0])

    def test_predict_degrades_to_single_member_when_an_ensemble_member_is_missing(self) -> None:
        self._store_feature_snapshot("QQQ")
        self._register_constant_artifact(
            "QQQ",
            model_name="survivor",
            version="v1",
            prediction=0.45,
            confidence=0.72,
            performance_metrics={"accuracy": 0.81},
            metadata={"recent_performance": 0.74},
        )
        self.registry.register_model(
            symbol="QQQ",
            model_name="missing_member",
            model_kind="constant",
            version="v1",
            artifact_uri=str(Path(self.tmp.name) / "qqq_missing_member.pkl"),
            metadata={
                "model_id": "missing_member:QQQ:v1",
                "feature_ids": list(self.feature_store.FEATURE_NAMES),
                "horizon_s": 300,
            },
            performance_metrics={"accuracy": 0.99},
            is_active=True,
        )

        result = self.public_inference.predict(
            "QQQ",
            persist=False,
            ensemble_enabled=True,
            ensemble_method="weighted_average",
        )

        self.assertEqual(result["status"], "ok")
        self.assertFalse(bool(result["safe_output"]))
        self.assertEqual(str(result["model_name"]), "survivor")
        self.assertEqual(str(result["model_kind"]), "constant")
        self.assertAlmostEqual(float(result["ensemble_output"]["final_prediction"]), float(result["prediction"]), places=6)
        self.assertAlmostEqual(
            float(result["ensemble_output"]["aggregated_confidence"]),
            float(result["confidence"]),
            places=6,
        )
        self.assertEqual(int(result["ensemble_output"]["ensemble_size"]), 1)
        self.assertTrue(bool(result["ensemble_output"]["fallback"]))
        self.assertEqual(
            str(result["ensemble_output"]["fallback_reason"]),
            "ensemble_degraded_to_single_member",
        )
        self.assertEqual(int(result["ensemble_output"]["attempted_size"]), 2)

    def test_predictor_live_hooks_delegate_non_breaking(self) -> None:
        self._store_feature_snapshot("AAPL")
        self._register_linear_artifact("AAPL")
        self._store_feature_snapshot("TSLA", scale=2.0)

        result = self.predictor.batch_predict_live_symbols(["AAPL", "TSLA"], persist=False)

        self.assertEqual(set(result.keys()), {"AAPL", "TSLA"})
        self.assertEqual(result["AAPL"]["status"], "ok")
        self.assertEqual(result["TSLA"]["status"], "safe_default")
        self.assertEqual(str(result["TSLA"]["fallback_reason"]), "model_registry_miss")

    def test_predict_runtime_event_adapts_realtime_results_to_legacy_shape(self) -> None:
        self._store_feature_snapshot("AAPL")
        self._register_linear_artifact("AAPL")

        preds = self.predictor.predict_runtime_event(
            query_vec=np.asarray([1.0, 2.0], dtype=np.float32),
            symbols=["AAPL"],
            horizons=[300],
            event={"event_id": 77, "ts_ms": 1_700_000_000_000},
        )

        self.assertEqual(set(preds.keys()), {("AAPL", 300)})
        expected_z, confidence, explain = preds[("AAPL", 300)]
        self.assertGreater(float(expected_z), 0.0)
        self.assertAlmostEqual(float(confidence), 0.88, places=6)
        self.assertEqual(str(explain["model_name"]), "rt_linear")
        self.assertEqual(str(explain["model_id"]), "rt_linear:AAPL:v1")
        self.assertEqual(str(explain["prediction_source"]), "realtime_inference_engine")
        self.assertIn("ensemble_output", explain)
        self.assertIn("feature_snapshot", explain)

    def test_predict_runtime_event_uses_legacy_predictor_only_when_explicitly_enabled(self) -> None:
        sentinel = {("AAPL", 300): (0.25, 0.5, {"model_name": "legacy"})}
        os.environ["REALTIME_INFERENCE_LEGACY_FALLBACK"] = "1"
        try:
            (self.predictor,) = _reload_modules("engine.strategy.predictor")
            with patch.object(self.predictor, "batch_predict_live_symbols", side_effect=RuntimeError("boom")):
                with patch.object(self.predictor, "predict_event", return_value=sentinel) as legacy_predict:
                    result = self.predictor.predict_runtime_event(
                        query_vec=np.asarray([1.0], dtype=np.float32),
                        symbols=["AAPL"],
                        horizons=[300],
                    )
            legacy_predict.assert_called_once()
            self.assertEqual(result, sentinel)
        finally:
            os.environ.pop("REALTIME_INFERENCE_LEGACY_FALLBACK", None)
            (self.predictor,) = _reload_modules("engine.strategy.predictor")

    def test_predict_runtime_event_avoids_legacy_db_fallback_by_default(self) -> None:
        original_mode = os.environ.get("ENGINE_MODE")
        os.environ["ENGINE_MODE"] = "safe"
        os.environ.pop("REALTIME_INFERENCE_LEGACY_FALLBACK", None)
        try:
            (self.predictor,) = _reload_modules("engine.strategy.predictor")
            with patch.object(self.predictor, "batch_predict_live_symbols", side_effect=RuntimeError("boom")):
                with patch.object(self.predictor, "predict_event") as legacy_predict:
                    result = self.predictor.predict_runtime_event(
                        query_vec=np.asarray([1.0], dtype=np.float32),
                        symbols=["AAPL"],
                        horizons=[300],
                        event={"event_id": 7},
                    )
            legacy_predict.assert_not_called()
            self.assertEqual(set(result.keys()), {("AAPL", 300)})
            expected_z, confidence, explain = result[("AAPL", 300)]
            self.assertEqual(float(expected_z), 0.0)
            self.assertEqual(float(confidence), 0.0)
            self.assertTrue(bool(explain.get("safe_output")))
            self.assertEqual(str(explain.get("prediction_source") or ""), "realtime_inference_safe_fallback")
            self.assertEqual(str(explain.get("fallback_reason") or ""), "realtime_inference_failed")
        finally:
            if original_mode is None:
                os.environ.pop("ENGINE_MODE", None)
            else:
                os.environ["ENGINE_MODE"] = str(original_mode)
            (self.predictor,) = _reload_modules("engine.strategy.predictor")
            (self.predictor,) = _reload_modules("engine.strategy.predictor")


if __name__ == "__main__":
    unittest.main()
