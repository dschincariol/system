from __future__ import annotations

import json
import sqlite3

from engine.strategy.feature_registry import feature_set_tag_from_ids


def test_log_decision_persists_feature_set_tag_from_training_schema(monkeypatch) -> None:
    chain_rows = []
    events = []

    def fake_append_chain_row(table_name, payload, con_arg):
        chain_rows.append((table_name, payload))

    def fake_append_event(**kwargs):
        events.append(kwargs)

    monkeypatch.setattr("engine.strategy.decision_log.append_chain_row", fake_append_chain_row)
    monkeypatch.setattr("engine.strategy.decision_log.append_event", fake_append_event)

    from engine.strategy.decision_log import log_decision

    feature_ids = ["price.last", "macro.cpi_yoy"]
    expected_tag = feature_set_tag_from_ids(feature_ids)

    log_decision(
        event_id=1,
        symbol="AAPL",
        horizon_s=300,
        predicted_z=0.25,
        confidence=0.9,
        model_name="lgbm_AAPL_1700000000007_abcdef8",
        model_kind="lgbm",
        model_ts_ms=1_700_000_000_007,
        features_hash="abc123",
        features_json={"price.last": 123.45, "macro.cpi_yoy": 3.1},
        explain_json={"feature_ids": feature_ids, "feature_schema": {"feature_ids": feature_ids}},
        con=sqlite3.connect(":memory:"),
    )

    row_payload = chain_rows[0][1]
    assert row_payload["feature_set_tag"] == expected_tag
    assert row_payload["feature_set_tag"]
    assert json.loads(row_payload["explain_json"])["feature_ids"] == feature_ids
    assert events[0]["payload"]["feature_set_tag"] == expected_tag


def test_log_decision_round_trips_auditability_columns() -> None:
    con = sqlite3.connect(":memory:")
    con.execute(
        """
        CREATE TABLE decision_log (
            id INTEGER PRIMARY KEY,
            ts_ms INTEGER NOT NULL,
            event_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            horizon_s INTEGER NOT NULL,
            predicted_z REAL NOT NULL,
            confidence REAL NOT NULL,
            model_name TEXT NOT NULL,
            model_kind TEXT,
            model_ts_ms INTEGER,
            model_version TEXT,
            features_hash TEXT,
            feature_set_tag TEXT,
            features_json TEXT,
            explain_json TEXT,
            extra_json TEXT,
            components_json TEXT,
            component_vector TEXT,
            prev_hash BLOB,
            row_hash BLOB NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE event_log (
            id INTEGER PRIMARY KEY,
            ts_ms INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            event_source TEXT NOT NULL,
            event_version INTEGER NOT NULL DEFAULT 1,
            entity_type TEXT,
            entity_id TEXT,
            correlation_id TEXT,
            payload_json TEXT NOT NULL
        )
        """
    )

    from engine.strategy.decision_log import log_decision

    component_vector = {
        "components": {
            "trend_follow": {"prediction": 0.4, "model_version": "trend:v1"},
            "mean_revert": {"prediction": -0.1, "model_version": "mean:v2"},
        },
        "weights": {"trend_follow": 0.7, "mean_revert": 0.3},
    }
    log_decision(
        event_id=2,
        symbol="MSFT",
        horizon_s=600,
        predicted_z=0.33,
        confidence=0.81,
        model_name="ensemble_weighted_average",
        model_kind="ensemble",
        model_ts_ms=1_700_000_000_111,
        model_version="ensemble:weighted_average:trend:v1+mean:v2",
        feature_set_tag="base.symbol_snapshot.v1",
        features_hash="hash-123",
        component_vector=component_vector,
        ts_ms=1_700_000_000_222,
        con=con,
    )

    row = con.execute(
        """
        SELECT feature_set_tag, model_version, components_json, component_vector
        FROM decision_log
        """
    ).fetchone()
    assert row is not None
    assert row[0] == "base.symbol_snapshot.v1"
    assert row[1] == "ensemble:weighted_average:trend:v1+mean:v2"
    assert json.loads(row[2]) == component_vector
    assert json.loads(row[3]) == component_vector

    event_payload = json.loads(con.execute("SELECT payload_json FROM event_log").fetchone()[0])
    assert event_payload["feature_set_tag"] == "base.symbol_snapshot.v1"
    assert event_payload["model_version"] == "ensemble:weighted_average:trend:v1+mean:v2"
    assert event_payload["component_vector"] == component_vector
