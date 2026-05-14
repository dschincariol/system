from __future__ import annotations

import importlib
import json
import math
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


class MetricsEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "metrics_engine_test.db")
        (
            self.storage,
            self.validation,
            self.alerts,
            self.execution_ledger,
            self.metrics_store,
            self.metrics_engine,
        ) = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.strategy.validation",
            "engine.runtime.alerts",
            "engine.execution.execution_ledger",
            "engine.runtime.metrics_store",
            "engine.metrics_engine",
        )[1:]
        self.storage.init_db()
        self.execution_ledger.init_execution_ledger()

    def tearDown(self) -> None:
        try:
            self.storage.close_pooled_connections()
        except Exception:
            pass
        self.tmp.cleanup()

    def test_refresh_feedback_loop_materializes_prediction_trade_links(self) -> None:
        con = self.storage.connect()
        try:
            con.execute(
                """
                INSERT INTO labels(
                  event_id, symbol, horizon_s, baseline_ret, realized_ret, impact_z, created_at_ms, vol_proxy, regime
                )
                VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (101, "AAPL", 300, None, 1.25, 1.25, 1, None, "global"),
            )
            con.commit()
        finally:
            con.close()

        self.validation.store_prediction(
            101,
            "AAPL",
            300,
            1.0,
            0.8,
            model_name="alpha_model",
            model_id="m1",
            model_version="v1",
        )

        alert_result = self.alerts.emit_alert(
            event_id=101,
            event_title="AAPL signal",
            symbol="AAPL",
            horizon_s=300,
            expected_z=1.0,
            confidence=0.8,
            explain={"model_name": "alpha_model", "model_id": "m1", "model_version": "v1"},
            return_details=True,
        )
        alert_id = int(alert_result["alert_id"] or 0)
        self.assertGreater(alert_id, 0)

        snapshot_ts_ms = int(time.time() * 1000)
        con = self.storage.connect()
        try:
            con.execute(
                """
                INSERT INTO execution_orders(
                  client_order_id, broker, source_alert_id, prediction_id, model_id, model_version,
                  symbol, qty, submit_ts_ms, extra_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "cid-1",
                    "sim",
                    int(alert_id),
                    None,
                    "m1",
                    "v1",
                    "AAPL",
                    1.0,
                    int(snapshot_ts_ms - 5_000),
                    json.dumps(
                        {"model_name": "alpha_model", "horizon_s": 300},
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                ),
            )
            con.execute(
                """
                INSERT INTO pnl_attribution(
                  ts_ms, source_alert_id, prediction_id, model_id, model_version, symbol, pnl, fees,
                  slippage_bps, position_size, avg_price, realized_pnl, unrealized_pnl, extra_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(snapshot_ts_ms),
                    int(alert_id),
                    None,
                    "m1",
                    "v1",
                    "AAPL",
                    15.0,
                    1.0,
                    2.0,
                    0.0,
                    100.0,
                    18.0,
                    0.0,
                    json.dumps(
                        {
                            "realized_trade_count": 1,
                            "realized_trade_client_order_ids": ["cid-1"],
                            "slippage_cost": 2.0,
                            "total_cost": 3.0,
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                ),
            )
            con.commit()
        finally:
            con.close()

        result = self.metrics_engine.refresh_feedback_loop(snapshot_ts_ms=int(snapshot_ts_ms))
        self.assertTrue(bool(result["ok"]))
        self.assertEqual(int(result["feedback"]["feedback_upserted"]), 1)

        con = self.storage.connect(readonly=True)
        try:
            prediction_id = int(
                con.execute(
                    """
                    SELECT id
                    FROM predictions
                    WHERE event_id=101 AND symbol='AAPL' AND horizon_s=300
                    LIMIT 1
                    """
                ).fetchone()[0]
            )
            alert_prediction_id = int(
                con.execute("SELECT prediction_id FROM alerts WHERE id=?", (int(alert_id),)).fetchone()[0]
            )
            order_prediction_id = int(
                con.execute(
                    "SELECT prediction_id FROM execution_orders WHERE client_order_id='cid-1'"
                ).fetchone()[0]
            )
            pnl_prediction_id = int(
                con.execute(
                    """
                    SELECT prediction_id
                    FROM pnl_attribution
                    WHERE ts_ms=? AND source_alert_id=?
                    """,
                    (int(snapshot_ts_ms), int(alert_id)),
                ).fetchone()[0]
            )
        finally:
            con.close()

        self.assertEqual(alert_prediction_id, prediction_id)
        self.assertEqual(order_prediction_id, prediction_id)
        self.assertEqual(pnl_prediction_id, prediction_id)

        feedback_rows = self.metrics_engine.list_prediction_feedback(model_id="m1")
        self.assertEqual(len(feedback_rows), 1)
        feedback = feedback_rows[0]
        self.assertEqual(int(feedback["prediction_id"]), prediction_id)
        self.assertEqual(int(feedback["source_alert_id"]), alert_id)
        self.assertAlmostEqual(float(feedback["realized_z"]), 1.25)
        self.assertAlmostEqual(float(feedback["realized_pnl"]), 18.0)
        self.assertAlmostEqual(float(feedback["net_pnl"]), 15.0)
        self.assertEqual(int(feedback["prediction_correct"]), 1)
        self.assertEqual(int(feedback["pnl_correct"]), 1)
        self.assertEqual(list(feedback["meta"]["client_order_ids"]), ["cid-1"])

        stats_rows = self.metrics_engine.get_model_performance_stats(scope="model", model_id="m1")
        self.assertEqual(len(stats_rows), 1)
        stats = stats_rows[0]
        self.assertAlmostEqual(float(stats["accuracy"]), 1.0)
        self.assertAlmostEqual(float(stats["sum_realized_pnl"]), 18.0)
        self.assertAlmostEqual(float(stats["sum_net_pnl"]), 15.0)
        self.assertAlmostEqual(float(stats["max_drawdown"]), 0.0)
        self.assertAlmostEqual(float(stats["sharpe"]), 0.0)

        metrics = self.metrics_store.get_runtime_metrics(metric="prediction_accuracy")
        self.assertTrue(bool(metrics["ok"]))
        self.assertTrue(
            any(
                str(row["tags"].get("model_id") or "") == "m1"
                and abs(float(row["value_num"] or 0.0) - 1.0) < 1e-9
                for row in (metrics.get("rows") or [])
            )
        )

    def test_compute_model_performance_stats_computes_sharpe_and_drawdown(self) -> None:
        con = self.storage.connect()
        try:
            rows = [
                (1, 1, 10, 201, None, "m1", "alpha_model", "v1", "AAPL", 300, 0.8, 0.6, 10.0, 10.0, 1, 1),
                (2, 2, 20, 202, None, "m1", "alpha_model", "v1", "AAPL", 300, -0.5, 0.7, -5.0, -5.0, 0, 0),
                (3, 3, 30, 203, None, "m1", "alpha_model", "v1", "AAPL", 300, 1.2, 0.9, 20.0, 20.0, 1, 1),
            ]
            for row in rows:
                con.execute(
                    """
                    INSERT INTO model_prediction_feedback(
                      prediction_id, prediction_ts_ms, resolution_ts_ms, event_id, source_alert_id,
                      model_id, model_name, model_version, symbol, horizon_s, predicted_z, confidence,
                      realized_pnl, net_pnl, trade_count, prediction_correct, pnl_correct, meta_json
                    )
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        int(row[0]),
                        int(row[1]),
                        int(row[2]),
                        int(row[3]),
                        row[4],
                        row[5],
                        row[6],
                        row[7],
                        row[8],
                        int(row[9]),
                        float(row[10]),
                        float(row[11]),
                        float(row[12]),
                        float(row[13]),
                        1,
                        int(row[14]),
                        int(row[15]),
                        "{}",
                    ),
                )
            con.commit()
        finally:
            con.close()

        result = self.metrics_engine.compute_model_performance_stats()
        self.assertTrue(bool(result["ok"]))

        stats_rows = self.metrics_engine.get_model_performance_stats(scope="model", model_id="m1")
        self.assertEqual(len(stats_rows), 1)
        stats = stats_rows[0]
        pnl_series = [10.0, -5.0, 20.0]
        mean = sum(pnl_series) / len(pnl_series)
        variance = sum((value - mean) ** 2 for value in pnl_series) / len(pnl_series)
        expected_sharpe = (mean / math.sqrt(variance)) * math.sqrt(len(pnl_series))

        self.assertEqual(int(stats["prediction_count"]), 3)
        self.assertAlmostEqual(float(stats["accuracy"]), 2.0 / 3.0, places=6)
        self.assertAlmostEqual(float(stats["sum_realized_pnl"]), 25.0)
        self.assertAlmostEqual(float(stats["sum_net_pnl"]), 25.0)
        self.assertAlmostEqual(float(stats["max_drawdown"]), 5.0, places=6)
        self.assertAlmostEqual(float(stats["sharpe"]), expected_sharpe, places=6)

        detail_rows = self.metrics_engine.get_model_performance_stats(
            scope="model_symbol_horizon",
            model_id="m1",
            symbol="AAPL",
            horizon_s=300,
        )
        self.assertEqual(len(detail_rows), 1)
        self.assertAlmostEqual(float(detail_rows[0]["max_drawdown"]), 5.0, places=6)


if __name__ == "__main__":
    unittest.main()
