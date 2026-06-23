from __future__ import annotations

import importlib
import importlib.util
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

LIGHTGBM_AVAILABLE = importlib.util.find_spec("lightgbm") is not None


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


@unittest.skipUnless(LIGHTGBM_AVAILABLE, "lightgbm not installed")
class ShapExplainerGBMTests(unittest.TestCase):
    def setUp(self) -> None:
        self.gbm, self.shap_explainer = _reload_modules(
            "engine.strategy.gbm_regressor",
            "engine.strategy.shap_explainer",
        )
        self.feature_ids = [
            "feature_a",
            "feature_b",
            "feature_c",
            "feature_d",
        ]

    def test_supported_family_explanation_payload_structure(self) -> None:
        X = np.asarray(
            [
                [1.0, 0.0, 0.5, 1.0],
                [0.8, 0.2, 0.4, 1.0],
                [0.3, 0.7, 0.6, 0.0],
                [0.1, 0.9, 0.9, 0.0],
            ],
            dtype=np.float32,
        )
        y = np.asarray([1.2, 0.8, -0.2, -0.9], dtype=np.float32)
        blob = self.gbm.train_gbm_model(
            X,
            y,
            feature_ids=list(self.feature_ids),
            hyperparams={
                "num_leaves": 7,
                "learning_rate": 0.1,
                "n_estimators": 12,
                "min_child_samples": 1,
                "random_state": 7,
                "n_jobs": 1,
            },
        )

        payload = self.shap_explainer.explain_prediction(
            "gbm_regressor",
            blob,
            {
                "feature_ids": list(self.feature_ids),
                "features": {
                    "feature_a": 0.95,
                    "feature_b": 0.10,
                    "feature_c": 0.30,
                    "feature_d": 1.00,
                },
            },
            top_k=2,
        )

        self.assertTrue(bool(payload.get("available")))
        self.assertEqual(str(payload.get("explanation_type") or ""), "shap")
        self.assertTrue(bool(payload.get("is_shap")))
        self.assertTrue(bool(payload.get("supports_shap")))
        self.assertIsNotNone(payload.get("base_value"))
        self.assertEqual(len(list(payload.get("top_features") or [])), 2)

        rows = list(payload.get("top_features") or [])
        self.assertGreaterEqual(float(rows[0]["abs_attribution"]), float(rows[1]["abs_attribution"]))
        for idx, row in enumerate(rows, start=1):
            self.assertEqual(int(row["rank"]), idx)
            self.assertIn("feature_id", row)
            self.assertIn("value", row)
            self.assertIn("attribution", row)
            self.assertIn("abs_attribution", row)
            self.assertIn("direction", row)


class ShapExplainerFallbackTests(unittest.TestCase):
    def setUp(self) -> None:
        (self.shap_explainer,) = _reload_modules("engine.strategy.shap_explainer")

    def test_unsupported_family_graceful_fallback(self) -> None:
        payload = self.shap_explainer.explain_prediction(
            "temporal_predictor",
            None,
            {
                "feature_ids": ["feature_b", "feature_a"],
                "features": {
                    "feature_a": 1.5,
                    "feature_b": -2.0,
                },
                "explain_context": {
                    "model_kind": "temporal_mlp",
                    "regime_at_trade": "MID",
                },
            },
            top_k=1,
        )

        self.assertTrue(bool(payload.get("available")))
        self.assertEqual(str(payload.get("explanation_type") or ""), "feature_value_proxy")
        self.assertFalse(bool(payload.get("is_shap")))
        self.assertFalse(bool(payload.get("supports_shap")))
        rows = list(payload.get("top_features") or [])
        self.assertEqual(len(rows), 1)
        self.assertEqual(str(rows[0]["feature_id"]), "feature_b")
        self.assertEqual(str(payload.get("diagnostics", {}).get("proxy_basis") or ""), "raw_feature_value")

    def test_top_k_sorting_is_stable(self) -> None:
        payload = self.shap_explainer.normalize_explanation_payload(
            {
                "model_family": "embed_regressor",
                "explanation_type": "feature_value_proxy",
                "top_features": [
                    {"feature_id": "z_feature", "attribution": -2.0, "value": -2.0},
                    {"feature_id": "a_feature", "attribution": 2.0, "value": 2.0},
                    {"feature_id": "m_feature", "attribution": 0.5, "value": 0.5},
                ],
                "top_k": 2,
            },
            feature_ids=["z_feature", "a_feature", "m_feature"],
        )

        rows = list(payload.get("top_features") or [])
        self.assertEqual([str(row["feature_id"]) for row in rows], ["a_feature", "z_feature"])
        self.assertEqual(len(rows), 2)
        for idx, row in enumerate(rows, start=1):
            self.assertEqual(int(row["rank"]), idx)
            self.assertIn("direction", row)


class PredictionExplanationStorageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self._env_backup = os.environ.get("DB_PATH")
        self._storage_backend_backup = os.environ.get("TS_STORAGE_BACKEND")
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "prediction_explanations.db")
        os.environ["TS_STORAGE_BACKEND"] = "sqlite"
        (self.storage,) = _reload_modules("engine.runtime.storage")
        self.storage.init_db()

    def tearDown(self) -> None:
        if self._env_backup is None:
            os.environ.pop("DB_PATH", None)
        else:
            os.environ["DB_PATH"] = self._env_backup
        if self._storage_backend_backup is None:
            os.environ.pop("TS_STORAGE_BACKEND", None)
        else:
            os.environ["TS_STORAGE_BACKEND"] = self._storage_backend_backup
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception:
            pass
        self.tmp.cleanup()

    def test_prediction_explanations_write_read(self) -> None:
        row_id = self.storage.record_prediction_explanation(
            symbol="AAPL",
            ts=1_710_000_000_000,
            model_family="gbm_regressor",
            model_name="gbm_regressor.live",
            version="v1",
            explanation_type="shap",
            top_features=[
                {"feature_id": "feature_a", "attribution": 1.2, "abs_attribution": 1.2, "rank": 1},
                {"feature_id": "feature_b", "attribution": -0.4, "abs_attribution": 0.4, "rank": 2},
            ],
            base_value=0.15,
            diagnostics={"feature_set_tag": "feature_set_v1", "event_id": 101},
        )

        self.assertGreater(int(row_id), 0)
        row = self.storage.fetch_latest_prediction_explanation(
            symbol="AAPL",
            ts=1_710_000_000_000,
            model_family="gbm_regressor",
            model_name="gbm_regressor.live",
            version="v1",
        )

        self.assertIsNotNone(row)
        row = dict(row or {})
        self.assertEqual(int(row.get("id") or 0), int(row_id))
        self.assertEqual(str(row.get("explanation_type") or ""), "shap")
        self.assertAlmostEqual(float(row.get("base_value") or 0.0), 0.15, places=6)
        self.assertEqual(len(list(row.get("top_features") or [])), 2)
        self.assertEqual(int(row.get("diagnostics", {}).get("event_id") or 0), 101)


class PredictionExplanationLegacyMigrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self._env_backup = os.environ.get("DB_PATH")
        self._storage_backend_backup = os.environ.get("TS_STORAGE_BACKEND")
        self.db_path = Path(self.tmp.name) / "prediction_explanations_legacy.db"
        os.environ["DB_PATH"] = str(self.db_path)
        os.environ["TS_STORAGE_BACKEND"] = "sqlite"

    def tearDown(self) -> None:
        if self._env_backup is None:
            os.environ.pop("DB_PATH", None)
        else:
            os.environ["DB_PATH"] = self._env_backup
        if self._storage_backend_backup is None:
            os.environ.pop("TS_STORAGE_BACKEND", None)
        else:
            os.environ["TS_STORAGE_BACKEND"] = self._storage_backend_backup
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception:
            pass
        self.tmp.cleanup()

    def test_init_db_migrates_legacy_alert_and_tracking_prediction_tables(self) -> None:
        con = sqlite3.connect(str(self.db_path))
        try:
            con.executescript(
                """
                CREATE TABLE alerts (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  ts_ms INTEGER NOT NULL,
                  event_title TEXT NOT NULL,
                  symbol TEXT NOT NULL,
                  horizon_s INTEGER NOT NULL,
                  expected_z REAL NOT NULL,
                  confidence REAL NOT NULL,
                  severity TEXT NOT NULL,
                  rule_id TEXT NOT NULL,
                  explain_json TEXT,
                  dedupe_key TEXT,
                  title TEXT,
                  message TEXT,
                  source TEXT,
                  status TEXT,
                  detail_json TEXT,
                  updated_ts_ms INTEGER,
                  model_name TEXT,
                  model_id TEXT,
                  model_version TEXT,
                  event_id INTEGER
                );

                CREATE TABLE tracked_predictions (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  ts_ms INTEGER NOT NULL,
                  symbol TEXT NOT NULL,
                  model_name TEXT NOT NULL,
                  model_version TEXT NOT NULL,
                  prediction REAL NOT NULL,
                  confidence REAL NOT NULL,
                  features_version TEXT NOT NULL
                );

                CREATE TABLE execution_orders (
                  client_order_id TEXT PRIMARY KEY,
                  broker TEXT NOT NULL,
                  portfolio_orders_id INTEGER,
                  source_alert_id INTEGER,
                  symbol TEXT NOT NULL,
                  qty REAL NOT NULL,
                  submit_ts_ms INTEGER NOT NULL,
                  ref_px REAL,
                  broker_order_id TEXT,
                  status TEXT NOT NULL DEFAULT 'submitted',
                  extra_json TEXT
                );

                CREATE TABLE execution_fills (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  client_order_id TEXT NOT NULL,
                  fill_id TEXT,
                  broker TEXT,
                  symbol TEXT,
                  ts_ms INTEGER,
                  submit_ts_ms INTEGER,
                  fill_ts_ms INTEGER NOT NULL,
                  fill_qty REAL NOT NULL,
                  fill_px REAL NOT NULL,
                  expected_px REAL,
                  mid_px REAL,
                  bid_px REAL,
                  ask_px REAL,
                  spread_bps REAL,
                  slippage_bps REAL,
                  fill_latency_ms INTEGER,
                  fees REAL,
                  commission REAL,
                  liquidity TEXT,
                  raw_json TEXT,
                  extra_json TEXT
                );

                CREATE TABLE pnl_attribution (
                  ts_ms INTEGER NOT NULL,
                  source_alert_id INTEGER,
                  symbol TEXT NOT NULL,
                  realized_pnl REAL,
                  unrealized_pnl REAL,
                  net_pnl REAL,
                  fees REAL,
                  PRIMARY KEY (ts_ms, source_alert_id, symbol)
                );
                """
            )
            con.commit()
        finally:
            con.close()

        (storage,) = _reload_modules("engine.runtime.storage")
        storage.init_db()

        con = sqlite3.connect(str(self.db_path))
        try:
            alert_cols = [str(row[1]) for row in con.execute("PRAGMA table_info(alerts)").fetchall()]
            tracked_cols = [str(row[1]) for row in con.execute("PRAGMA table_info(tracked_predictions)").fetchall()]
            execution_order_cols = [str(row[1]) for row in con.execute("PRAGMA table_info(execution_orders)").fetchall()]
            execution_fill_cols = [str(row[1]) for row in con.execute("PRAGMA table_info(execution_fills)").fetchall()]
            pnl_cols = [str(row[1]) for row in con.execute("PRAGMA table_info(pnl_attribution)").fetchall()]
            alert_indexes = [str(row[1]) for row in con.execute("PRAGMA index_list(alerts)").fetchall()]
            tracked_indexes = [str(row[1]) for row in con.execute("PRAGMA index_list(tracked_predictions)").fetchall()]
            execution_order_indexes = [
                str(row[1]) for row in con.execute("PRAGMA index_list(execution_orders)").fetchall()
            ]
            execution_fill_indexes = [
                str(row[1]) for row in con.execute("PRAGMA index_list(execution_fills)").fetchall()
            ]
            pnl_indexes = [str(row[1]) for row in con.execute("PRAGMA index_list(pnl_attribution)").fetchall()]
        finally:
            con.close()

        self.assertIn("prediction_id", alert_cols)
        self.assertIn("prediction_id", tracked_cols)
        self.assertIn("metadata_json", tracked_cols)
        self.assertIn("prediction_id", execution_order_cols)
        self.assertIn("model_id", execution_order_cols)
        self.assertIn("model_id", execution_fill_cols)
        self.assertIn("prediction_id", pnl_cols)
        self.assertIn("idx_alerts_prediction_id", alert_indexes)
        self.assertIn("idx_tracked_predictions_prediction_id", tracked_indexes)
        self.assertIn("idx_execution_orders_prediction_submit_ts", execution_order_indexes)
        self.assertIn("idx_execution_fills_model_ts", execution_fill_indexes)
        self.assertIn("idx_pnl_attribution_prediction_ts", pnl_indexes)
        self.assertIn("idx_pnl_attribution_ts", pnl_indexes)
        self.assertIn("idx_pnl_attribution_model_ts", pnl_indexes)


class PredictorExplainabilityFlagTests(unittest.TestCase):
    def setUp(self) -> None:
        self._env_backup = {
            "SHAP_EXPLANATIONS_ENABLED": os.environ.get("SHAP_EXPLANATIONS_ENABLED"),
        }

    def tearDown(self) -> None:
        for key, value in self._env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def test_predictor_backward_compatibility_when_disabled(self) -> None:
        os.environ["SHAP_EXPLANATIONS_ENABLED"] = "0"
        (predictor,) = _reload_modules("engine.strategy.predictor")

        active_model = {
            "model_name": "embed_regressor.live",
            "model_id": "embed_regressor.live:AAPL:v1",
            "family": "embed_regressor",
            "model_family": "embed_regressor",
            "model_kind": "ridge",
            "model_version": "v1",
            "feature_ids": ["feature_a"],
            "feature_schema": {},
        }
        base_result = (
            0.25,
            0.60,
            {
                "model_name": "embed_regressor.live",
                "model_id": "embed_regressor.live:AAPL:v1",
                "model_family": "embed_regressor",
                "model_kind": "ridge",
                "model_version": "v1",
                "feature_ids": ["feature_a"],
            },
        )

        with patch.object(predictor, "_resolve_active_model", return_value=dict(active_model)):
            with patch.object(predictor, "_predict_resolved_model", return_value=base_result):
                with patch.object(predictor, "_track_prediction_output"):
                    with patch.object(predictor, "resolve_feature_ids", return_value=["feature_a"]):
                        with patch.object(predictor, "feature_set_tag", return_value="feature_set_v1"):
                            with patch.object(predictor, "build_feature_snapshot", return_value={"feature_a": 1.0}):
                                _, _, explain = predictor._predict_single_model(
                                    np.asarray([1.0], dtype=np.float32),
                                    "AAPL",
                                    300,
                                    top_k=8,
                                    event={"ts_ms": 1_710_000_000_000},
                                    forced_model_name="embed_regressor.live",
                                )

        self.assertIsInstance(explain, dict)
        self.assertEqual(str(explain.get("model_name") or ""), "embed_regressor.live")
        self.assertNotIn("prediction_explanation", explain)


if __name__ == "__main__":
    unittest.main()
