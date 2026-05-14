from __future__ import annotations

import concurrent.futures
import importlib
import os
import pickle
import sys
import tempfile
import time
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


class _FakeTrackingTimescaleClient:
    enabled = True

    def __init__(self, *, sleep_s: float = 0.0) -> None:
        self.sleep_s = float(max(0.0, sleep_s))
        self.model_registry_rows: list[dict[str, object]] = []
        self.prediction_rows: list[dict[str, object]] = []

    def enqueue_model_registry(self, rows, *, timeout_s=None) -> int:
        if self.sleep_s > 0.0:
            time.sleep(self.sleep_s)
        batch = [dict(row) for row in (rows or [])]
        self.model_registry_rows.extend(batch)
        return int(len(batch))

    def enqueue_predictions(self, rows, *, timeout_s=None) -> int:
        if self.sleep_s > 0.0:
            time.sleep(self.sleep_s)
        batch = [dict(row) for row in (rows or [])]
        self.prediction_rows.extend(batch)
        return int(len(batch))


class PredictionTrackingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "prediction_tracking.db")
        os.environ.pop("TIMESCALE_ENABLED", None)
        (
            self.storage,
            self.feature_store,
            self.engine_prediction_logger,
            self.public_prediction_logger,
            self.validation,
            self.engine_registry,
            self.public_registry,
            self.engine_ensemble,
            self.public_ensemble,
            self.engine_inference,
            self.public_inference,
        ) = _reload_modules(
            "engine.runtime.storage",
            "engine.data.feature_store",
            "engine.prediction_logger",
            "prediction_logger",
            "engine.strategy.validation",
            "engine.model_registry",
            "model_registry",
            "engine.ensemble_engine",
            "ensemble_engine",
            "engine.inference_engine",
            "inference_engine",
        )
        self.storage.init_db()

    def tearDown(self) -> None:
        try:
            self.public_prediction_logger.shutdown_prediction_tracking(timeout_s=2.0)
        except Exception:
            pass
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
        self.engine_registry.register_model(
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

    def test_model_registry_class_registers_and_lists_models(self) -> None:
        client = _FakeTrackingTimescaleClient()
        registry = self.public_registry.ModelRegistry()
        with patch.object(self.engine_prediction_logger, "get_timescale_client", return_value=client):
            row = registry.register_model("alpha_model", "v1", {"owner": "unit-test"})
            self.assertTrue(bool(registry.flush(2.0)))

        self.assertEqual(str(row["model_name"]), "alpha_model")
        self.assertEqual(str(row["version"]), "v1")
        self.assertEqual(dict(row["metadata"]), {"owner": "unit-test"})
        self.assertEqual(str(registry.get_model("alpha_model")["version"]), "v1")
        self.assertEqual(len(registry.list_models()), 1)
        self.assertEqual(len(client.model_registry_rows), 1)
        self.assertEqual(client.model_registry_rows[0]["model_name"], "alpha_model")
        self.assertEqual(client.model_registry_rows[0]["version"], "v1")

    def test_model_registry_survives_reload_after_flush(self) -> None:
        client = _FakeTrackingTimescaleClient()
        registry = self.public_registry.ModelRegistry()
        with patch.object(self.engine_prediction_logger, "get_timescale_client", return_value=client):
            registry.register_model("persistent_model", "v9", {"owner": "reload-test"})
            self.assertTrue(bool(registry.flush(2.0)))

        (_, _, _, _, _, _, reloaded_public_registry) = _reload_modules(
            "engine.runtime.storage",
            "engine.data.feature_store",
            "engine.prediction_logger",
            "prediction_logger",
            "engine.strategy.validation",
            "engine.model_registry",
            "model_registry",
        )
        reloaded = reloaded_public_registry.ModelRegistry().get_model("persistent_model", "v9")
        self.assertIsNotNone(reloaded)
        self.assertEqual(str(reloaded["version"]), "v9")
        self.assertEqual(dict(reloaded["metadata"]).get("owner"), "reload-test")

    def test_prediction_logger_does_not_block_caller_when_sink_is_slow(self) -> None:
        client = _FakeTrackingTimescaleClient(sleep_s=0.2)
        logger = self.public_prediction_logger.DEFAULT_PREDICTION_LOGGER
        with patch.object(self.engine_prediction_logger, "get_timescale_client", return_value=client):
            started = time.perf_counter()
            queued = logger.log_prediction_nowait(
                model_name="latency_probe",
                model_version="v1",
                symbol="SPY",
                timestamp=1_700_000_100_000,
                prediction=0.5,
                confidence=0.8,
                features_version="feature_set_v1",
            )
            elapsed_s = time.perf_counter() - started
            self.assertTrue(bool(queued))
            self.assertLess(elapsed_s, 0.1)
            self.assertTrue(bool(logger.flush(3.0)))

        self.assertEqual(len(client.prediction_rows), 1)
        self.assertEqual(client.prediction_rows[0]["model_name"], "latency_probe")
        self.assertEqual(client.prediction_rows[0]["features_version"], "feature_set_v1")

    def test_prediction_logger_handles_concurrent_models(self) -> None:
        client = _FakeTrackingTimescaleClient()
        logger = self.public_prediction_logger.DEFAULT_PREDICTION_LOGGER
        with patch.object(self.engine_prediction_logger, "get_timescale_client", return_value=client):
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                futures = [
                    executor.submit(
                        logger.log_prediction_nowait,
                        model_name=f"model_{idx}",
                        model_version=f"v{idx}",
                        symbol="QQQ",
                        timestamp=1_700_000_200_000 + idx,
                        prediction=float(idx),
                        confidence=0.5,
                        features_version="feature_set_v2",
                    )
                    for idx in range(16)
                ]
                results = [future.result(timeout=2.0) for future in futures]
            self.assertTrue(all(bool(result) for result in results))
            self.assertTrue(bool(logger.flush(3.0)))

        self.assertEqual(len(client.prediction_rows), 16)
        self.assertEqual(
            {str(row["model_name"]) for row in client.prediction_rows},
            {f"model_{idx}" for idx in range(16)},
        )

    def test_prediction_logger_retries_local_write_when_tracking_table_is_missing(self) -> None:
        logger = self.public_prediction_logger.DEFAULT_PREDICTION_LOGGER
        real_run_write_txn = self.engine_prediction_logger.run_write_txn
        attempts = {"tracked_predictions": 0}

        def flaky_run_write_txn(write_fn, *, table, operation):
            if str(table) == "tracked_predictions" and int(attempts["tracked_predictions"]) == 0:
                attempts["tracked_predictions"] += 1
                raise RuntimeError("no such table: tracked_predictions")
            return real_run_write_txn(write_fn, table=table, operation=operation)

        with patch.object(self.engine_prediction_logger, "run_write_txn", side_effect=flaky_run_write_txn):
            queued = logger.log_prediction_nowait(
                model_name="retry_probe",
                model_version="v1",
                symbol="IWM",
                timestamp=1_700_000_250_000,
                prediction=0.75,
                confidence=0.61,
                features_version="feature_set_retry",
            )
            self.assertTrue(bool(queued))
            self.assertTrue(bool(logger.flush(3.0)))

        self.assertEqual(int(attempts["tracked_predictions"]), 1)
        con = self.storage.connect(readonly=True)
        try:
            tracked = con.execute(
                """
                SELECT model_name, model_version, symbol
                FROM tracked_predictions
                WHERE model_name='retry_probe'
                ORDER BY ts_ms DESC, id DESC
                LIMIT 1
                """
            ).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(tracked)
        self.assertEqual(str(tracked[0]), "retry_probe")
        self.assertEqual(str(tracked[1]), "v1")
        self.assertEqual(str(tracked[2]), "IWM")

    def test_store_prediction_writes_canonical_linkage_record(self) -> None:
        self.validation.store_prediction(
            101,
            "SPY",
            300,
            1.25,
            0.82,
            model_name="linked_model",
            model_id="linked-model-id",
            model_version="v3",
            features_version="feature_set_v3",
            tracking_source="unit_test_validation",
        )
        self.assertTrue(bool(self.public_prediction_logger.flush_prediction_tracking(3.0)))

        con = self.storage.connect(readonly=True)
        try:
            prediction_id = int(
                con.execute(
                    """
                    SELECT id
                    FROM predictions
                    WHERE event_id=101 AND symbol='SPY' AND horizon_s=300
                    LIMIT 1
                    """
                ).fetchone()[0]
            )
            tracked = con.execute(
                """
                SELECT prediction_id, event_id, horizon_s, model_id, tracking_source, features_version
                FROM tracked_predictions
                WHERE prediction_id=?
                ORDER BY ts_ms DESC, id DESC
                LIMIT 1
                """,
                (int(prediction_id),),
            ).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(tracked)
        self.assertEqual(int(tracked[0]), prediction_id)
        self.assertEqual(int(tracked[1]), 101)
        self.assertEqual(int(tracked[2]), 300)
        self.assertEqual(str(tracked[3]), "linked-model-id")
        self.assertEqual(str(tracked[4]), "unit_test_validation")
        self.assertEqual(str(tracked[5]), "feature_set_v3")

    def test_predictor_tracking_carries_model_serving_diagnostics(self) -> None:
        (_, predictor) = _reload_modules(
            "engine.prediction_logger",
            "engine.strategy.predictor",
        )
        client = _FakeTrackingTimescaleClient()
        explain = {
            "model_name": "embed_regressor.live",
            "model_version": "v7",
            "model_id": "embed_regressor.live:SPY:v7",
            "model_kind": "ridge",
            "model_family": "embed_regressor",
            "requested_model_name": "temporal_predictor.live",
            "resolved_model_name": "embed_regressor.live",
            "resolution_source": "registry",
            "requested_model_family": "temporal_predictor",
            "served_model_family": "embed_regressor",
            "serve_fallback_active": True,
            "fallback_reason": "resolved_to_registry",
            "candidate_names": ["temporal_predictor.live", "embed_regressor.live"],
            "feature_ids": ["rolling_return_5m"],
            "feature_set_tag": "price_feature_store_v1",
            "signal_ts_ms": 1_700_000_000_000,
        }

        with patch.object(self.engine_prediction_logger, "get_timescale_client", return_value=client):
            predictor._track_prediction_output(
                symbol="SPY",
                horizon_s=300,
                prediction=0.75,
                confidence=0.66,
                explain=dict(explain),
                source="legacy_predictor",
            )
            self.assertTrue(bool(self.public_prediction_logger.flush_prediction_tracking(3.0)))

        self.assertEqual(len(client.prediction_rows), 1)
        prediction_meta = dict(client.prediction_rows[0]["metadata"])
        self.assertEqual(str(prediction_meta["requested_model_name"]), "temporal_predictor.live")
        self.assertEqual(str(prediction_meta["resolved_model_name"]), "embed_regressor.live")
        self.assertEqual(str(prediction_meta["resolution_source"]), "registry")
        self.assertEqual(str(prediction_meta["requested_model_family"]), "temporal_predictor")
        self.assertEqual(str(prediction_meta["served_model_family"]), "embed_regressor")
        self.assertTrue(bool(prediction_meta["serve_fallback_active"]))
        self.assertEqual(str(prediction_meta["fallback_reason"]), "resolved_to_registry")
        self.assertEqual(
            list(prediction_meta["candidate_names"]),
            ["temporal_predictor.live", "embed_regressor.live"],
        )

        self.assertEqual(len(client.model_registry_rows), 1)
        registry_meta = dict(client.model_registry_rows[0]["metadata"])
        self.assertEqual(str(registry_meta["requested_model_name"]), "temporal_predictor.live")
        self.assertEqual(str(registry_meta["served_model_family"]), "embed_regressor")

    def test_ensemble_inference_logs_member_outputs_and_final_output(self) -> None:
        client = _FakeTrackingTimescaleClient()
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

        with patch.object(self.engine_prediction_logger, "get_timescale_client", return_value=client):
            result = self.public_inference.predict(
                "AMD",
                persist=False,
                ensemble_enabled=True,
                ensemble_method="weighted_average",
            )
            self.assertEqual(result["status"], "ok")
            self.assertTrue(bool(self.public_prediction_logger.flush_prediction_tracking(3.0)))

        logged_names = [str(row["model_name"]) for row in client.prediction_rows]
        self.assertEqual(len(client.prediction_rows), 3)
        self.assertEqual(
            set(logged_names),
            {"trend_follow", "mean_revert", "ensemble_weighted_average"},
        )
        self.assertTrue(all(str(row["features_version"]) for row in client.prediction_rows))


if __name__ == "__main__":
    unittest.main()
