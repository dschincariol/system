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
