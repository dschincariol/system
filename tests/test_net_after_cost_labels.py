from __future__ import annotations

import importlib
import json
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def test_net_after_cost_artifact_captures_prediction_execution_and_carry_costs():
    from engine.strategy.net_after_cost_labels import (
        build_net_after_cost_label,
        load_execution_trace,
        load_prediction_label_context,
        net_cost_evidence_summary,
        upsert_net_after_cost_label,
    )

    con = sqlite3.connect(":memory:")
    try:
        con.execute(
            """
            CREATE TABLE predictions (
              id INTEGER PRIMARY KEY,
              event_id INTEGER,
              symbol TEXT,
              horizon_s INTEGER,
              ts_ms INTEGER,
              predicted_z REAL,
              confidence REAL,
              confidence_raw REAL,
              prediction_strength REAL,
              model_name TEXT,
              model_id TEXT,
              model_version TEXT,
              volatility_regime TEXT,
              trend_regime TEXT,
              liquidity_regime TEXT
            )
            """
        )
        con.execute(
            """
            CREATE TABLE alerts (
              id INTEGER PRIMARY KEY,
              prediction_id INTEGER,
              event_id INTEGER,
              symbol TEXT,
              horizon_s INTEGER,
              ts_ms INTEGER,
              confidence REAL,
              explain_json TEXT,
              detail_json TEXT,
              model_name TEXT,
              model_id TEXT,
              model_version TEXT
            )
            """
        )
        con.execute(
            """
            CREATE TABLE execution_orders (
              client_order_id TEXT,
              symbol TEXT,
              source_alert_id INTEGER,
              prediction_id INTEGER,
              model_id TEXT,
              model_version TEXT,
              submit_ts_ms INTEGER,
              spread_bps REAL,
              extra_json TEXT
            )
            """
        )
        con.execute(
            """
            CREATE TABLE execution_fills (
              id INTEGER PRIMARY KEY,
              fill_id TEXT,
              client_order_id TEXT,
              symbol TEXT,
              prediction_id INTEGER,
              source_alert_id INTEGER,
              fill_ts_ms INTEGER,
              fill_qty REAL,
              fill_px REAL,
              expected_px REAL,
              mid_px REAL,
              bid_px REAL,
              ask_px REAL,
              spread_bps REAL,
              slippage_bps REAL,
              fees REAL,
              raw_json TEXT,
              extra_json TEXT
            )
            """
        )
        con.execute(
            """
            CREATE TABLE pnl_attribution (
              ts_ms INTEGER,
              symbol TEXT,
              prediction_id INTEGER,
              source_alert_id INTEGER,
              fees REAL,
              slippage_bps REAL,
              realized_pnl REAL,
              unrealized_pnl REAL,
              extra_json TEXT
            )
            """
        )
        con.execute(
            """
            INSERT INTO predictions(
              id, event_id, symbol, horizon_s, ts_ms, predicted_z, confidence, confidence_raw,
              prediction_strength, model_name, model_id, model_version, volatility_regime,
              trend_regime, liquidity_regime
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (101, 7, "AAPL", 300, 1_000_000, 1.4, 0.71, 0.82, 0.33, "patchtst_v1", "patchtst:aapl", "v3", "low_vol", "up", "liquid"),
        )
        con.execute(
            """
            INSERT INTO alerts(
              id, prediction_id, event_id, symbol, horizon_s, ts_ms, confidence,
              explain_json, detail_json, model_name, model_id, model_version
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                501,
                101,
                7,
                "AAPL",
                300,
                1_000_001,
                0.76,
                json.dumps({"regime": "risk_on", "carry": {"borrow_bps": 3.0, "financing_cost": 0.25}}),
                json.dumps({"model_intent": {"regime": "risk_on"}}),
                "patchtst_v1",
                "patchtst:aapl",
                "v3",
            ),
        )
        con.execute(
            """
            INSERT INTO execution_orders(client_order_id, symbol, source_alert_id, prediction_id, model_id, model_version, submit_ts_ms, spread_bps, extra_json)
            VALUES (?,?,?,?,?,?,?,?,?)
            """,
            ("order-1", "AAPL", 501, 101, "patchtst:aapl", "v3", 1_000_100, 5.0, json.dumps({"borrow_cost": 0.10})),
        )
        con.execute(
            """
            INSERT INTO execution_fills(
              fill_id, client_order_id, symbol, prediction_id, source_alert_id, fill_ts_ms,
              fill_qty, fill_px, expected_px, mid_px, bid_px, ask_px, spread_bps,
              slippage_bps, fees, raw_json, extra_json
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            ("fill-1", "order-1", "AAPL", 101, 501, 1_000_150, 10.0, 100.0, 99.9, 100.0, 99.95, 100.05, 5.0, 4.0, 0.20, "{}", "{}"),
        )
        con.execute(
            """
            INSERT INTO pnl_attribution(ts_ms, symbol, prediction_id, source_alert_id, fees, slippage_bps, realized_pnl, unrealized_pnl, extra_json)
            VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (1_100_000, "AAPL", 101, 501, 0.20, 4.0, 18.0, 0.0, json.dumps({"financing_bps": 2.5})),
        )

        context = load_prediction_label_context(con, event_id=7, symbol="AAPL", horizon_s=300)
        trace = load_execution_trace(
            con,
            event_id=7,
            symbol="AAPL",
            horizon_s=300,
            label_ts_ms=1_000_000,
            exit_ts_ms=1_300_000,
            prediction_id=context["prediction_id"],
            source_alert_id=context["source_alert_id"],
        )
        artifact = build_net_after_cost_label(
            event_id=7,
            symbol="AAPL",
            horizon_s=300,
            label_ts_ms=1_000_000,
            side=1,
            gross_return=0.020,
            net_return=0.018,
            realized_forward_return=0.020,
            source="broker_fills_v2",
            realized=1,
            entry_ts_ms=1_000_150,
            exit_ts_ms=1_300_000,
            costs={"fees_bps": 2.0, "slippage_bps": 4.0, "spread_bps": 5.0, "total_cost_bps": 11.0},
            context=context,
            execution_trace=trace,
        )
        upsert_net_after_cost_label(con, artifact)

        row = con.execute(
            """
            SELECT model_family, regime, confidence, gross_return, execution_cost_return, net_return,
                   borrow_bps, financing_bps, order_count, fill_count, label_metadata_json
            FROM net_after_cost_labels
            WHERE event_id=7 AND symbol='AAPL' AND horizon_s=300
            """
        ).fetchone()
        assert row is not None
        assert row[0] == "patchtst"
        assert row[1] == "risk_on"
        assert float(row[2]) == 0.76
        assert float(row[3]) == 0.020
        assert round(float(row[4]), 6) == 0.002
        assert float(row[5]) == 0.018
        assert float(row[6]) >= 3.0
        assert float(row[7]) >= 2.5
        assert int(row[8]) == 1
        assert int(row[9]) == 1
        metadata = json.loads(row[10])
        assert metadata["timestamp_safe"] is True
        assert metadata["cost_evidence"]["execution_trace_available"] is True

        evidence = net_cost_evidence_summary(con, min_ts_ms=0)
        assert evidence["available"] is True
        assert evidence["n"] == 1
    finally:
        con.close()


def test_promotion_metrics_require_net_cost_evidence():
    from engine.strategy.promotion_guard import promotion_allowed_by_metrics
    from engine.strategy.pipeline_train_and_eval import _beats_champion

    gross_only = {
        "n_eval": 500,
        "rmse": 0.8,
        "bias": 0.0,
        "dir_acc": 0.62,
        "gross_edge": 0.004,
    }
    champion = {"n_eval": 500, "rmse": 1.0, "bias": 0.0, "dir_acc": 0.55}
    assert promotion_allowed_by_metrics(gross_only, champion, 0.01, 0.02) is False

    net_evidence = {
        **gross_only,
        "net_edge": 0.002,
        "cost_drag": 0.002,
        "avg_total_cost_bps": 20.0,
        "net_cost_label_count": 12,
        "net_cost_evidence_available": True,
    }
    assert promotion_allowed_by_metrics(net_evidence, champion, 0.01, 0.10) is True

    ok, reason = _beats_champion(gross_only, {"metrics": champion})
    assert ok is False
    assert reason["missing_net_cost_evidence"] is True


def test_fill_cost_decomposition_uses_notional_and_adverse_slippage_sign():
    from ops.compute_exec_labels_from_fills import _cost_bps_from_trade

    buy_cost = _cost_bps_from_trade(
        {"fees_total": 1.0, "entry_notional": 1000.0, "ref_px": 99.0},
        px_in=100.0,
        px_out=102.0,
        side=1,
    )
    assert round(float(buy_cost["fees_bps"]), 6) == 10.0
    assert float(buy_cost["slippage_bps"]) > 0.0

    sell_cost = _cost_bps_from_trade(
        {"fees_total": 1.0, "entry_notional": 1000.0, "ref_px": 100.0},
        px_in=99.0,
        px_out=97.0,
        side=-1,
    )
    assert round(float(sell_cost["fees_bps"]), 6) == 10.0
    assert float(sell_cost["slippage_bps"]) > 0.0


def test_compute_exec_labels_is_timestamp_safe_for_unelapsed_horizon(monkeypatch):
    tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
    try:
        os.environ["DB_PATH"] = str(Path(tmp.name) / "labels.db")
        os.environ["TS_STORAGE_BACKEND"] = "sqlite"
        os.environ["TS_TESTING"] = "1"
        os.environ["ENGINE_SUPERVISED"] = "1"
        storage = importlib.reload(importlib.import_module("engine.runtime.storage"))
        compute_exec_labels = importlib.reload(importlib.import_module("ops.compute_exec_labels"))
        storage.init_db()

        label_ts_ms = int(time.time() * 1000) - 60_000
        horizon_s = 300
        con = storage.connect()
        try:
            con.execute(
                """
                INSERT INTO predictions(event_id, symbol, horizon_s, ts_ms, predicted_z, model_name, model_id, confidence, confidence_raw, prediction_strength)
                VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (9001, "AAPL", horizon_s, label_ts_ms, 1.0, "patchtst_v1", "patchtst:aapl", 0.7, 0.7, 0.5),
            )
            con.execute(
                "INSERT INTO prices(ts_ms, symbol, price, px, source) VALUES (?,?,?,?,?)",
                (label_ts_ms, "AAPL", 100.0, 100.0, "test"),
            )
            con.commit()
        finally:
            con.close()

        monkeypatch.setattr(compute_exec_labels, "_now_ms", lambda: label_ts_ms + 120_000)
        compute_exec_labels.main()

        con = storage.connect()
        try:
            labels_count = con.execute("SELECT COUNT(*) FROM labels_exec WHERE event_id=9001").fetchone()[0]
            artifact_count = con.execute("SELECT COUNT(*) FROM net_after_cost_labels WHERE event_id=9001").fetchone()[0]
        finally:
            con.close()

        assert int(labels_count) == 0
        assert int(artifact_count) == 0
    finally:
        os.environ.pop("ENGINE_SUPERVISED", None)
        os.environ.pop("TS_TESTING", None)
        os.environ.pop("TS_STORAGE_BACKEND", None)
        os.environ.pop("DB_PATH", None)
        tmp.cleanup()
