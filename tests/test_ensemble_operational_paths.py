from __future__ import annotations

import importlib
import json
import os
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


class EnsembleOperationalPathsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.tmp.name) / "ensemble_operational_paths.db"
        os.environ["DB_PATH"] = str(self.db_path)
        (self.storage,) = _reload_modules(
            "engine.runtime.storage",
        )
        _, _, self.api_read_advanced = _reload_modules(
            "engine.api.internal_access",
            "engine.api.api_read",
            "engine.api.api_read_advanced",
        )
        self.storage.init_db()

    def tearDown(self) -> None:
        for key in (
            "DB_PATH",
            "ENSEMBLE_META_MIN_ROWS",
            "ENSEMBLE_META_LOOKBACK_DAYS",
            "ENSEMBLE_META_MIN_FAMILIES",
        ):
            os.environ.pop(key, None)
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception:
            pass
        self.tmp.cleanup()

    def test_model_diagnostics_expose_ensemble_sections(self) -> None:
        con = self.storage.connect(readonly=False)
        try:
            con.execute(
                """
                INSERT INTO ensemble_blend_weights(created_ts, mode, regime, weights_json, meta_blob)
                VALUES (?,?,?,?,?)
                """,
                (1_700_000_000_000, "equal", None, json.dumps({"embed_regressor": 0.5, "temporal_predictor": 0.5}), None),
            )
            con.execute(
                """
                INSERT INTO ensemble_predictions(symbol, ts, blended_prediction, family_preds_json, weights_json, agreement)
                VALUES (?,?,?,?,?,?)
                """,
                (
                    "AAPL",
                    1_700_000_000_100,
                    0.33,
                    json.dumps({"embed_regressor": {"prediction": 0.3}, "temporal_predictor": {"prediction": 0.36}}),
                    json.dumps({"mode": "equal", "weights": {"embed_regressor": 0.5, "temporal_predictor": 0.5}}),
                    0.9,
                ),
            )
            con.execute(
                """
                INSERT INTO ensemble_family_performance(window_start_ts, window_end_ts, family, n_predictions, realized_sharpe, hit_rate)
                VALUES (?,?,?,?,?,?)
                """,
                (1_699_999_000_000, 1_700_000_000_000, "embed_regressor", 12, 1.1, 0.66),
            )
            con.commit()
        finally:
            con.close()

        out = self.api_read_advanced.get_model_diagnostics()

        self.assertIn("ensemble_current_weights", out)
        self.assertIn("ensemble_recent_predictions", out)
        self.assertIn("ensemble_family_performance", out)
        self.assertEqual(str(out["ensemble_current_weights"][0]["mode"]), "equal")
        self.assertEqual(str(out["ensemble_recent_predictions"][0]["symbol"]), "AAPL")
        self.assertEqual(str(out["ensemble_family_performance"][0]["family"]), "embed_regressor")

    def test_train_ensemble_meta_persists_stacked_weights_and_family_metrics(self) -> None:
        os.environ["ENSEMBLE_META_MIN_ROWS"] = "2"
        os.environ["ENSEMBLE_META_LOOKBACK_DAYS"] = "5000"
        os.environ["ENSEMBLE_META_MIN_FAMILIES"] = "2"
        (train_ensemble_meta,) = _reload_modules("engine.strategy.jobs.train_ensemble_meta")
        now_ts = int(time.time() * 1000)

        con = self.storage.connect(readonly=False)
        try:
            label_rows = [
                (101, "AAPL", 300, now_ts, 0.40),
                (102, "AAPL", 300, now_ts + 100, -0.20),
            ]
            for event_id, symbol, horizon_s, ts_ms, net_z in label_rows:
                con.execute(
                    """
                    INSERT INTO labels_exec(
                      event_id, symbol, horizon_s, ts_ms, source, realized, side, gross_ret, net_ret,
                      gross_z, net_z, mid_in, mid_out, spread_in, fees_bps, slippage_bps, spread_bps, total_cost_bps, extra_json
                    )
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        event_id,
                        symbol,
                        horizon_s,
                        ts_ms,
                        "test",
                        1,
                        1,
                        net_z,
                        net_z,
                        net_z,
                        net_z,
                        100.0,
                        100.1,
                        0.01,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        "{}",
                    ),
                )
            shadow_rows = [
                (now_ts, 101, "AAPL", 300, "embed_regressor.live", 0.50, 0.60),
                (now_ts, 101, "AAPL", 300, "temporal_predictor.live", 0.20, 0.70),
                (now_ts + 100, 102, "AAPL", 300, "embed_regressor.live", -0.30, 0.55),
                (now_ts + 100, 102, "AAPL", 300, "temporal_predictor.live", -0.10, 0.65),
            ]
            for ts_ms, event_id, symbol, horizon_s, model_name, predicted_z, confidence in shadow_rows:
                con.execute(
                    """
                    INSERT INTO shadow_predictions(
                      ts_ms, event_id, symbol, regime, horizon_s, model_name, model_kind, model_ts_ms,
                      predicted_z, confidence, cost_est, net_pred_z, extra_json
                    )
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        ts_ms,
                        event_id,
                        symbol,
                        "global",
                        horizon_s,
                        model_name,
                        "test",
                        1_700_000_000_000,
                        predicted_z,
                        confidence,
                        0.0,
                        predicted_z,
                        "{}",
                    ),
                )
            con.commit()
        finally:
            con.close()

        result = train_ensemble_meta.run()

        self.assertTrue(bool(result.get("ok")))
        self.assertEqual(str(result.get("status") or ""), "trained")
        self.assertGreaterEqual(int(result.get("family_count") or 0), 2)

        con = self.storage.connect(readonly=False)
        try:
            weight_row = con.execute(
                """
                SELECT mode, weights_json, meta_artifact_sha256, meta_artifact_alias
                FROM ensemble_blend_weights
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            perf_count = con.execute("SELECT COUNT(*) FROM ensemble_family_performance").fetchone()[0]
        finally:
            con.close()

        self.assertIsNotNone(weight_row)
        self.assertEqual(str(weight_row[0] or ""), "stacked")
        self.assertTrue(bool(json.loads(str(weight_row[1] or "{}"))))
        self.assertTrue(str(weight_row[2] or "").strip())
        self.assertTrue(str(weight_row[3] or "").strip())
        self.assertGreaterEqual(int(perf_count or 0), 2)

    def test_repair_schema_creates_ensemble_tables(self) -> None:
        con = self.storage.connect(readonly=False)
        try:
            for table in (
                "ensemble_blend_weights",
                "ensemble_predictions",
                "ensemble_family_performance",
                "insider_transactions",
                "congressional_trades",
            ):
                con.execute(f"DROP TABLE IF EXISTS {table}")
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS runtime_meta (
                  key TEXT PRIMARY KEY,
                  value TEXT,
                  updated_ts_ms INTEGER
                )
                """
            )
            con.execute(
                """
                INSERT OR REPLACE INTO runtime_meta(key, value, updated_ts_ms)
                VALUES ('schema_version', '2', 1)
                """
            )
            con.commit()
        finally:
            con.close()

        storage, repair_schema = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.jobs.repair_schema",
        )
        result = repair_schema.run()

        self.assertTrue(bool(result.get("ok")))
        self.assertEqual(int(result.get("expected_schema_version") or 0), int(repair_schema.SCHEMA_VERSION))
        self.assertEqual(int(repair_schema.SCHEMA_VERSION), int(storage.SCHEMA_VERSION))

        con = storage.connect(readonly=True)
        try:
            tables = {
                str(row[0] or "")
                for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
            runtime_meta_version = con.execute(
                "SELECT value FROM runtime_meta WHERE key='schema_version'"
            ).fetchone()
            latest_schema_version = con.execute(
                """
                SELECT version
                FROM schema_version
                WHERE status='applied'
                ORDER BY version DESC
                LIMIT 1
                """
            ).fetchone()
        finally:
            con.close()

        self.assertIn("ensemble_blend_weights", tables)
        self.assertIn("ensemble_predictions", tables)
        self.assertIn("ensemble_family_performance", tables)
        self.assertIn("insider_transactions", tables)
        self.assertIn("congressional_trades", tables)
        self.assertEqual(int(runtime_meta_version[0] or 0), int(storage.SCHEMA_VERSION))

    def test_repair_schema_backfills_execution_table_columns(self) -> None:
        con = self.storage.connect(readonly=False)
        try:
            con.execute("DROP TABLE IF EXISTS execution_orders")
            con.execute("DROP TABLE IF EXISTS execution_fills")
            con.execute(
                """
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
                )
                """
            )
            con.execute(
                """
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
                )
                """
            )
            con.commit()
        finally:
            con.close()

        _, repair_schema = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.jobs.repair_schema",
        )
        result = repair_schema.run()

        self.assertTrue(bool(result.get("ok")))

        con = self.storage.connect(readonly=True)
        try:
            execution_order_cols = {
                str(row[1] or "")
                for row in con.execute("PRAGMA table_info(execution_orders)").fetchall()
            }
            execution_fill_cols = {
                str(row[1] or "")
                for row in con.execute("PRAGMA table_info(execution_fills)").fetchall()
            }
        finally:
            con.close()

        self.assertIn("order_uid", execution_order_cols)
        self.assertIn("prediction_id", execution_order_cols)
        self.assertIn("model_id", execution_order_cols)
        self.assertIn("model_version", execution_order_cols)
        self.assertIn("model_id", execution_fill_cols)
        self.assertIn("model_version", execution_fill_cols)


if __name__ == "__main__":
    unittest.main()
