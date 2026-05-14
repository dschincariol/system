from __future__ import annotations

import asyncio
import importlib
import os
import pickle
import sys
import tempfile
import time
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


class ModelScoringTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "model_scoring_test.db")
        os.environ["MODEL_SCORING_ENABLED"] = "1"
        os.environ.pop("ARTIFACT_STORE_MIRROR_ROOT", None)
        (
            _db_guard,
            self.storage,
            self.feature_store,
            self.engine_prediction_logger,
            self.public_prediction_logger,
            self.validation,
            self.engine_registry,
            self.public_registry,
            self.engine_model_scoring,
            self.public_model_scoring,
            self.online_model,
        ) = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.data.feature_store",
            "engine.prediction_logger",
            "prediction_logger",
            "engine.strategy.validation",
            "engine.model_registry",
            "model_registry",
            "engine.model_scoring",
            "model_scoring",
            "engine.strategy.models.online_model",
        )
        self.storage.init_db()

    def tearDown(self) -> None:
        try:
            self.public_model_scoring.stop_model_scoring_service(timeout_s=1.0)
        except Exception:
            pass
        try:
            self.public_prediction_logger.shutdown_prediction_tracking(timeout_s=2.0)
        except Exception:
            pass
        try:
            self.storage.close_pooled_connections()
        except Exception:
            pass
        self.tmp.cleanup()

    def _store_feature_snapshot(self, symbol: str, *, ts_ms: int, scale: float = 1.0) -> dict[str, object]:
        feature_map = {
            name: float(scale * float(idx + 1))
            for idx, name in enumerate(self.feature_store.FEATURE_NAMES)
        }
        snapshot = {
            "symbol": str(symbol).upper(),
            "ts_ms": int(ts_ms),
            "feature_set_tag": str(self.feature_store.FEATURE_SET_TAG),
            "feature_names": list(self.feature_store.FEATURE_NAMES),
            "point_count": 64,
            "source_timestamps": {"price_history_last_ts_ms": int(ts_ms)},
            "features": feature_map,
        }
        return self.feature_store.store_features(str(symbol), snapshot)

    def _log_prediction(
        self,
        *,
        model_name: str,
        model_version: str,
        symbol: str,
        timestamp: int,
        prediction: float,
        confidence: float,
        horizon_s: int,
        model_id: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> None:
        queued = self.public_prediction_logger.DEFAULT_PREDICTION_LOGGER.log_prediction_nowait(
            model_name=str(model_name),
            model_version=str(model_version),
            symbol=str(symbol).upper(),
            timestamp=int(timestamp),
            prediction=float(prediction),
            confidence=float(confidence),
            features_version=str(self.feature_store.FEATURE_SET_TAG),
            horizon_s=int(horizon_s),
            model_id=(str(model_id) if model_id else None),
            tracking_source="unit_test_model_scoring",
            metadata=dict(metadata or {}),
        )
        self.assertTrue(bool(queued))
        self.assertTrue(bool(self.public_prediction_logger.flush_prediction_tracking(3.0)))

    def test_score_models_writes_historical_model_performance_from_predictions_table(self) -> None:
        scorer = self.public_model_scoring.ModelScorer(online_updates_enabled=False, batch_limit=10)
        self.validation.store_prediction(
            101,
            "AAPL",
            60,
            0.02,
            0.7,
            model_name="alpha_loop",
            model_id="alpha_loop:AAPL:v1",
            model_version="v1",
            features_version=str(self.feature_store.FEATURE_SET_TAG),
            tracking_metadata={},
        )
        self.validation.store_prediction(
            102,
            "AAPL",
            60,
            -0.01,
            0.6,
            model_name="alpha_loop",
            model_id="alpha_loop:AAPL:v1",
            model_version="v1",
            features_version=str(self.feature_store.FEATURE_SET_TAG),
            tracking_metadata={},
        )
        self.assertTrue(bool(self.public_prediction_logger.flush_prediction_tracking(3.0)))

        con = self.storage.connect(readonly=True)
        try:
            prediction_times = [
                int(row[0])
                for row in con.execute(
                    """
                    SELECT ts_ms
                    FROM predictions
                    WHERE symbol='AAPL' AND model_id='alpha_loop:AAPL:v1'
                    ORDER BY ts_ms ASC, id ASC
                    """
                ).fetchall()
            ]
        finally:
            con.close()

        self.assertEqual(len(prediction_times), 2)

        asyncio.run(scorer.record_outcome("AAPL", prediction_times[0] + 60_000, 0.03))
        asyncio.run(scorer.record_outcome("AAPL", prediction_times[1] + 60_000, -0.015))

        result = asyncio.run(scorer.score_models())
        self.assertTrue(bool(result["ok"]))
        self.assertEqual(int(result["scored_predictions"]), 2)

        rerun = asyncio.run(scorer.score_models())
        self.assertEqual(int(rerun.get("scored_predictions", 0)), 0)

        con = self.storage.connect(readonly=True)
        try:
            rows = con.execute(
                """
                SELECT prediction_id, prediction_time, "time", error, directional_accuracy, pnl_impact, rolling_score
                FROM model_performance
                ORDER BY prediction_time ASC
                """
            ).fetchall()
        finally:
            con.close()

        self.assertEqual(len(rows), 2)
        self.assertGreater(int(rows[0][0]), 0)
        self.assertEqual(int(rows[0][1]), int(prediction_times[0]))
        self.assertEqual(int(rows[0][2]), int(prediction_times[0] + 60_000))
        self.assertAlmostEqual(float(rows[0][3]), 0.01, places=6)
        self.assertEqual(int(rows[0][4]), 1)
        self.assertAlmostEqual(float(rows[0][5]), 0.03, places=6)
        self.assertIsNotNone(rows[0][6])

        self.assertGreater(int(rows[1][0]), 0)
        self.assertEqual(int(rows[1][1]), int(prediction_times[1]))
        self.assertEqual(int(rows[1][2]), int(prediction_times[1] + 60_000))
        self.assertAlmostEqual(float(rows[1][3]), 0.005, places=6)
        self.assertEqual(int(rows[1][4]), 1)
        self.assertAlmostEqual(float(rows[1][5]), 0.015, places=6)
        self.assertIsNotNone(rows[1][6])
        self.assertNotEqual(float(rows[0][6]), float(rows[1][6]))

    def test_score_models_updates_online_model_artifact(self) -> None:
        scorer = self.public_model_scoring.ModelScorer(online_updates_enabled=True, batch_limit=10)
        t0 = 1_700_100_000_000
        self._store_feature_snapshot("AMD", ts_ms=int(t0), scale=1.3)

        artifact_path = Path(self.tmp.name) / "amd_online_feedback.pkl"
        model = self.online_model.OnlineModel(
            model_name="online_feedback",
            feature_ids=list(self.feature_store.FEATURE_NAMES),
            feature_set_tag=str(self.feature_store.FEATURE_SET_TAG),
            default_confidence=0.62,
        )
        model.register(
            symbol="AMD",
            version="v1",
            artifact_uri=artifact_path,
            performance_metrics={"quality_score": 0.62},
            is_active=True,
        )

        self._log_prediction(
            model_name="online_feedback",
            model_version="v1",
            symbol="AMD",
            timestamp=int(t0),
            prediction=0.05,
            confidence=0.62,
            horizon_s=60,
            model_id="online_feedback:AMD:v1",
            metadata={"feature_ts_ms": int(t0)},
        )
        asyncio.run(scorer.record_outcome("AMD", t0 + 60_000, 0.75))

        result = asyncio.run(scorer.score_models())
        self.assertTrue(bool(result["ok"]))
        self.assertEqual(int(result["online_updates"]), 1)

        with artifact_path.open("rb") as handle:
            updated_model = pickle.load(handle)
        self.assertEqual(int(updated_model.n_updates), 1)
        self.assertGreaterEqual(float(updated_model.error_ema), 0.0)

    def test_score_models_skips_immutable_object_store_artifact_updates(self) -> None:
        scorer = self.public_model_scoring.ModelScorer(online_updates_enabled=True, batch_limit=10)
        t0 = 1_700_120_000_000
        self._store_feature_snapshot("AMD", ts_ms=int(t0), scale=1.1)
        os.environ["ARTIFACT_STORE_MIRROR_ROOT"] = str(Path(self.tmp.name) / "artifact_mirror")
        self.addCleanup(os.environ.pop, "ARTIFACT_STORE_MIRROR_ROOT", None)
        (artifact_store,) = _reload_modules("engine.runtime.artifact_store")

        model = self.online_model.OnlineModel(
            model_name="online_object_store",
            feature_ids=list(self.feature_store.FEATURE_NAMES),
            feature_set_tag=str(self.feature_store.FEATURE_SET_TAG),
            default_confidence=0.64,
        )
        artifact_uri = "s3://model-artifacts/amd/online_object_store_v1.pkl"
        manifest = artifact_store.build_artifact_manifest(
            artifact_uri,
            {
                "artifact_manifest": {
                    "version_id": "online-object-v1",
                    "sha256": "sha256-amd-online-object-v1",
                }
            },
        )
        self.assertIsNotNone(manifest)
        mirror_path = Path(str(manifest.get("local_mirror_path") or ""))
        mirror_path.parent.mkdir(parents=True, exist_ok=True)
        with mirror_path.open("wb") as handle:
            pickle.dump(model, handle)

        self.public_registry.register_model(
            symbol="AMD",
            model_name="online_object_store",
            model_kind="online",
            version="v1",
            artifact_uri=artifact_uri,
            metadata={
                "feature_ids": list(self.feature_store.FEATURE_NAMES),
                "feature_set_tag": str(self.feature_store.FEATURE_SET_TAG),
                "supports_online_update": True,
                "model_interface": "BaseModel",
                "artifact_manifest": {
                    "version_id": "online-object-v1",
                    "sha256": "sha256-amd-online-object-v1",
                },
            },
            performance_metrics={"quality_score": 0.64},
            is_active=True,
        )

        self._log_prediction(
            model_name="online_object_store",
            model_version="v1",
            symbol="AMD",
            timestamp=int(t0),
            prediction=0.03,
            confidence=0.64,
            horizon_s=60,
            model_id="online_object_store:AMD:v1",
            metadata={"feature_ts_ms": int(t0)},
        )
        asyncio.run(scorer.record_outcome("AMD", t0 + 60_000, 0.55))

        result = asyncio.run(scorer.score_models())
        self.assertTrue(bool(result["ok"]))
        self.assertEqual(int(result["online_updates"]), 0)
        self.assertGreaterEqual(int(result["skipped_online_updates"] or 0), 1)

        with mirror_path.open("rb") as handle:
            persisted_model = pickle.load(handle)
        self.assertEqual(int(persisted_model.n_updates), 0)

    def test_score_models_persists_regime_fields(self) -> None:
        scorer = self.public_model_scoring.ModelScorer(online_updates_enabled=False, batch_limit=10)
        t0 = 1_700_150_000_000
        con = self.storage.connect()
        try:
            con.execute(
                """
                INSERT INTO regime_state(time, symbol, volatility_regime, trend_regime, liquidity_regime)
                VALUES(?,?,?,?,?)
                """,
                (int(t0), "QQQ", "high", "bearish", "thin"),
            )
            con.commit()
        finally:
            con.close()

        self._log_prediction(
            model_name="regime_probe",
            model_version="v1",
            symbol="QQQ",
            timestamp=int(t0),
            prediction=-0.03,
            confidence=0.61,
            horizon_s=60,
            model_id="regime_probe:QQQ:v1",
            metadata={
                "feature_ts_ms": int(t0),
                "regime": {
                    "time": int(t0),
                    "symbol": "QQQ",
                    "volatility_regime": "unknown",
                    "trend_regime": "unknown",
                    "liquidity_regime": "unknown",
                },
            },
        )
        asyncio.run(scorer.record_outcome("QQQ", t0 + 60_000, -0.02))

        result = asyncio.run(scorer.score_models())
        self.assertTrue(bool(result["ok"]))
        self.assertEqual(int(result["scored_predictions"]), 1)

        con = self.storage.connect(readonly=True)
        try:
            row = con.execute(
                """
                SELECT regime_time_ms, volatility_regime, trend_regime, liquidity_regime
                FROM model_performance
                WHERE model_name='regime_probe'
                LIMIT 1
                """
            ).fetchone()
        finally:
            con.close()

        self.assertEqual(int(row[0]), int(t0))
        self.assertEqual(tuple(str(value) for value in row[1:]), ("high", "bearish", "thin"))

    def test_model_scoring_service_scores_in_background(self) -> None:
        t0 = 1_700_200_000_000
        self._log_prediction(
            model_name="service_probe",
            model_version="v1",
            symbol="MSFT",
            timestamp=int(t0),
            prediction=0.04,
            confidence=0.55,
            horizon_s=60,
            model_id="service_probe:MSFT:v1",
            metadata={"feature_ts_ms": int(t0)},
        )
        asyncio.run(self.public_model_scoring.DEFAULT_MODEL_SCORER.record_outcome("MSFT", t0 + 60_000, 0.02))

        snap = self.public_model_scoring.start_model_scoring_service(interval_s=0.1, enabled=True)
        self.assertTrue(bool(snap["ok"]))
        time.sleep(0.5)

        con = self.storage.connect(readonly=True)
        try:
            count = int(
                con.execute("SELECT COUNT(*) FROM model_performance WHERE model_name='service_probe'").fetchone()[0]
            )
        finally:
            con.close()

        self.assertEqual(count, 1)


if __name__ == "__main__":
    unittest.main()
