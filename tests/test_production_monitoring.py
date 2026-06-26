from __future__ import annotations

import json
import sqlite3

from engine.api.drift_explainer import build_drift_explainer_snapshot
from engine.api.model_performance_divergence import build_model_performance_divergence
from engine.strategy import production_monitoring as pm


NOW_MS = 1_800_000_000_000


def _init_source_schema(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        CREATE TABLE feature_distribution_drift (
          feature_id TEXT PRIMARY KEY,
          ts_ms INTEGER NOT NULL,
          recent_n INTEGER NOT NULL,
          baseline_n INTEGER NOT NULL,
          recent_mean REAL NOT NULL,
          baseline_mean REAL NOT NULL,
          baseline_std REAL NOT NULL,
          shift_z REAL NOT NULL,
          drift_score REAL NOT NULL,
          drift_flag INTEGER NOT NULL DEFAULT 0,
          meta_json TEXT
        );
        CREATE TABLE model_feature_snapshots (
          symbol TEXT NOT NULL,
          ts_ms INTEGER NOT NULL,
          feature_set_tag TEXT NOT NULL,
          snapshot_version INTEGER NOT NULL DEFAULT 1,
          feature_ids_json TEXT NOT NULL,
          vector_json TEXT NOT NULL,
          features_json TEXT NOT NULL,
          source_timestamps_json TEXT NOT NULL,
          availability_json TEXT NOT NULL,
          created_ts_ms INTEGER NOT NULL,
          PRIMARY KEY(symbol, ts_ms, feature_set_tag)
        );
        CREATE TABLE predictions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          event_id INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          horizon_s INTEGER NOT NULL,
          ts_ms INTEGER NOT NULL,
          predicted_z REAL NOT NULL,
          confidence REAL NOT NULL,
          model_name TEXT
        );
        CREATE TABLE labels (
          event_id INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          horizon_s INTEGER NOT NULL,
          impact_z REAL,
          created_at_ms INTEGER
        );
        CREATE TABLE decision_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          event_id INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          horizon_s INTEGER NOT NULL,
          predicted_z REAL NOT NULL,
          confidence REAL,
          model_name TEXT,
          explain_json TEXT
        );
        CREATE TABLE shadow_predictions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          event_id INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          horizon_s INTEGER NOT NULL,
          model_name TEXT NOT NULL,
          predicted_z REAL NOT NULL,
          confidence REAL NOT NULL,
          ts_ms INTEGER NOT NULL
        );
        CREATE TABLE labels_exec (
          event_id INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          horizon_s INTEGER NOT NULL,
          ts_ms INTEGER NOT NULL,
          net_ret REAL
        );
        CREATE TABLE model_lifecycle_runs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          model_name TEXT NOT NULL,
          model_version TEXT,
          parent_version TEXT,
          action TEXT NOT NULL,
          status TEXT NOT NULL,
          triggered_by TEXT,
          mutation_kind TEXT,
          details_json TEXT,
          created_ts_ms INTEGER NOT NULL,
          updated_ts_ms INTEGER NOT NULL
        );
        """
    )


def _seed_degraded_monitoring_inputs(con: sqlite3.Connection) -> None:
    con.execute(
        """
        INSERT INTO feature_distribution_drift(
          feature_id, ts_ms, recent_n, baseline_n, recent_mean, baseline_mean,
          baseline_std, shift_z, drift_score, drift_flag, meta_json
        ) VALUES('macro.credit', ?, 80, 320, 2.0, 0.1, 0.2, 9.5, 0.92, 1, '{}')
        """,
        (NOW_MS,),
    )
    for idx in range(10):
        con.execute(
            """
            INSERT INTO model_feature_snapshots(
              symbol, ts_ms, feature_set_tag, snapshot_version, feature_ids_json,
              vector_json, features_json, source_timestamps_json, availability_json,
              created_ts_ms
            ) VALUES('AAPL', ?, 'unit', 1, ?, ?, ?, '{}', ?, ?)
            """,
            (
                NOW_MS - idx,
                json.dumps(["a", "b"]),
                json.dumps([1.0, None]),
                json.dumps({"a": 1.0}),
                json.dumps({"price": True, "macro": False}),
                NOW_MS,
            ),
        )

    total = pm.RECENT_N + pm.BASELINE_N
    for idx in range(total):
        recent = idx < pm.RECENT_N
        event_id = idx + 1
        ts_ms = NOW_MS - idx * 1000
        pred = 3.0 if recent else (0.05 if idx % 2 == 0 else 0.15)
        target = -3.0 if recent else pred
        conf = 0.95 if recent else 0.55
        net_ret = -0.02 if recent else 0.02
        con.execute(
            """
            INSERT INTO predictions(event_id, symbol, horizon_s, ts_ms, predicted_z, confidence, model_name)
            VALUES(?, 'AAPL', 300, ?, ?, ?, 'live_model')
            """,
            (event_id, ts_ms, pred, conf),
        )
        con.execute(
            """
            INSERT INTO labels(event_id, symbol, horizon_s, impact_z, created_at_ms)
            VALUES(?, 'AAPL', 300, ?, ?)
            """,
            (event_id, target, ts_ms + 300_000),
        )
        con.execute(
            """
            INSERT INTO decision_log(ts_ms, event_id, symbol, horizon_s, predicted_z, confidence, model_name, explain_json)
            VALUES(?, ?, 'AAPL', 300, ?, ?, 'live_model', ?)
            """,
            (
                ts_ms,
                event_id,
                pred,
                conf,
                json.dumps(
                    {
                        "conformal": {
                            "available": True,
                            "interval_lower": -0.10,
                            "interval_upper": 0.10,
                            "alpha_target": 0.20,
                        }
                    }
                ),
            ),
        )
        if recent:
            con.execute(
                """
                INSERT INTO shadow_predictions(event_id, symbol, horizon_s, model_name, predicted_z, confidence, ts_ms)
                VALUES(?, 'AAPL', 300, 'shadow_model', -3.0, 0.80, ?)
                """,
                (event_id, ts_ms),
            )
        con.execute(
            """
            INSERT INTO labels_exec(event_id, symbol, horizon_s, ts_ms, net_ret)
            VALUES(?, 'AAPL', 300, ?, ?)
            """,
            (event_id, ts_ms, net_ret),
        )


def test_production_monitoring_computes_metrics_and_threshold_states() -> None:
    con = sqlite3.connect(":memory:")
    _init_source_schema(con)
    _seed_degraded_monitoring_inputs(con)

    result = pm.compute_and_store_production_monitoring(
        con=con,
        now_ms=NOW_MS,
        emit_signals=False,
    )

    metrics = {row["metric_name"]: row for row in result["metrics"]}
    assert result["status"]["severity"] == "CRIT"
    assert metrics["feature_drift"]["severity"] == "CRIT"
    assert metrics["missing_feature_rate"]["value"] >= 0.5
    assert metrics["prediction_drift"]["severity"] in {"WARN", "CRIT"}
    assert metrics["target_label_drift"]["labels_available"] is True
    assert metrics["calibration_ece"]["value"] > 0.50
    assert metrics["conformal_coverage"]["value"] == 0.0
    assert metrics["shadow_live_disagreement"]["details"]["sign_disagreement_rate"] == 1.0
    assert metrics["net_pnl_degradation"]["value"] > 0.03

    stored = pm.get_latest_production_monitoring_snapshot(con=con)
    assert stored["ok"] is True
    assert {row["metric_name"] for row in stored["metrics"]} >= set(metrics)


def test_conformal_risk_control_metric_uses_labeled_decision_payloads() -> None:
    con = sqlite3.connect(":memory:")
    _init_source_schema(con)
    total = pm.MIN_N
    for idx in range(total):
        event_id = idx + 10_000
        ts_ms = NOW_MS - idx * 1000
        target = -1.0 if idx < 3 else 1.0
        con.execute(
            """
            INSERT INTO labels(event_id, symbol, horizon_s, impact_z, created_at_ms)
            VALUES(?, 'AAPL', 300, ?, ?)
            """,
            (event_id, target, ts_ms + 300_000),
        )
        con.execute(
            """
            INSERT INTO decision_log(ts_ms, event_id, symbol, horizon_s, predicted_z, confidence, model_name, explain_json)
            VALUES(?, ?, 'AAPL', 300, 1.0, 0.8, 'live_model', ?)
            """,
            (
                ts_ms,
                event_id,
                json.dumps(
                    {
                        "conformal": {
                            "available": True,
                            "interval_lower": -2.0,
                            "interval_upper": 2.0,
                            "alpha_target": 0.20,
                            "risk_control": {
                                "enabled": True,
                                "available": True,
                                "loss_definition": "accepted_trade_loss",
                                "target_risk": 0.20,
                                "calibrated_loss_threshold": 0.50,
                                "recommended_action": "log_only",
                            },
                        }
                    }
                ),
            ),
        )

    metric = {row["metric_name"]: row for row in pm.compute_production_monitoring_metrics(con, now_ms=NOW_MS)}[
        "conformal_risk_control"
    ]

    assert metric["labels_available"] is True
    assert metric["sample_n"] == total
    assert metric["value"] == 3 / total
    assert abs(metric["baseline_value"] - 0.20) < 1.0e-12
    assert metric["details"]["loss_definitions"] == ["accepted_trade_loss"]


def test_monitoring_alerts_create_signals_without_auto_promotion() -> None:
    con = sqlite3.connect(":memory:")
    _init_source_schema(con)
    _seed_degraded_monitoring_inputs(con)

    result = pm.compute_and_store_production_monitoring(
        con=con,
        now_ms=NOW_MS,
        emit_signals=True,
        signal_cooldown_s=0,
    )

    actions = {row["action_taken"] for row in result["signals"]}
    assert "retrain_signal" in actions
    assert "shadow_review_signal" in actions

    event_rows = con.execute(
        "SELECT action_taken, outcome_status, candidate_version, diagnostics FROM drift_retrain_events"
    ).fetchall()
    assert event_rows
    assert all(row[1] == "signal_created" for row in event_rows)
    assert all(row[2] is None for row in event_rows)
    assert all(json.loads(row[3])["direct_promotion"] is False for row in event_rows)
    assert con.execute("SELECT COUNT(*) FROM model_lifecycle_runs").fetchone()[0] == 0


def test_monitoring_no_label_yet_does_not_emit_label_based_signals() -> None:
    con = sqlite3.connect(":memory:")
    _init_source_schema(con)
    con.execute("DELETE FROM labels")

    result = pm.compute_and_store_production_monitoring(
        con=con,
        now_ms=NOW_MS,
        emit_signals=True,
        signal_cooldown_s=0,
    )

    metrics = {row["metric_name"]: row for row in result["metrics"]}
    assert metrics["target_label_drift"]["state"] == "no_labels_yet"
    assert metrics["calibration_ece"]["state"] == "no_labels_yet"
    assert metrics["conformal_coverage"]["state"] == "no_labels_yet"
    assert result["signals"] == []


def test_model_performance_divergence_surfaces_production_monitoring_rows() -> None:
    payload = build_model_performance_divergence(
        shadow_payload=[],
        backtest_payload={},
        pnl_payload={},
        execution_metrics_payload={},
        execution_stats_payload={},
        execution_advisories_payload={},
        model_registry_payload={},
        production_monitoring_payload={
            "ok": True,
            "updated_ts_ms": NOW_MS,
            "metrics": [
                {
                    "metric_name": "shadow_live_disagreement",
                    "ts_ms": NOW_MS,
                    "value": 0.90,
                    "baseline_value": 0.0,
                    "threshold_value": 0.25,
                    "severity": "CRIT",
                    "state": "crit",
                    "action_signal": "shadow_review",
                    "details": {},
                }
            ],
        },
        now_ms=NOW_MS,
    )

    rows = {row["key"]: row for row in payload["comparisons"]}
    assert payload["status"]["state"] == "diverged"
    assert rows["shadow_live_disagreement"]["status"] == "diverged"
    assert "production_monitoring" in payload["sources"]


def test_drift_explainer_surfaces_production_monitoring_contributor() -> None:
    con = sqlite3.connect(":memory:")
    pm.ensure_production_monitoring_schema(con)
    con.execute(
        """
        INSERT INTO production_monitoring_metrics(
          metric_name, scope, dimension, ts_ms, value, baseline_value,
          threshold_value, severity, state, action_signal, labels_available,
          sample_n, details_json
        ) VALUES('calibration_ece', 'global', '', ?, 0.25, 0.05, 0.10, 'CRIT', 'crit', 'retrain', 1, 80, '{}')
        """,
        (NOW_MS,),
    )

    payload = build_drift_explainer_snapshot(con=con, now_ms=NOW_MS)

    assert payload["sources"]["production_monitoring"]["available"] is True
    assert any(row["source"] == "production_monitoring_metrics" for row in payload["contributors"])
    assert payload["status"]["severity"] == "CRIT"
