from __future__ import annotations

import sqlite3

from engine.strategy.discovery.base import CandidateFeature, EvaluationResult
from engine.strategy.discovery import registry as discovery_registry
from engine.strategy.discovery.registry import (
    FEATURE_STAGE_SHADOW,
    ensure_discovery_schema,
    list_evaluations,
    list_registered_features,
    record_candidate,
    record_evaluation,
    register_feature,
)


def test_discovery_registry_insert_dedup_and_read_round_trip() -> None:
    con = sqlite3.connect(":memory:")
    ensure_discovery_schema(con)
    candidate = CandidateFeature(
        source="unit",
        symbol="AAPL",
        expression="x0+x1",
        params={"feature_map": {"x0": "price.last", "x1": "price.rv_20"}},
    )

    first = record_candidate(candidate, con=con, ts=123)
    duplicate = record_candidate(candidate, con=con, ts=456)

    assert first.id == duplicate.id
    assert duplicate.ts == 123

    result = EvaluationResult(
        candidate_hash=candidate.hash,
        feature_id=candidate.feature_id,
        t_stat=4.2,
        p_value=0.001,
        q_value=0.004,
        oos_ic=0.35,
        decision="accepted",
        n_obs=64,
    )
    record_evaluation(first.id, result, con=con, ts=789)
    register_feature(candidate, candidate_id=first.id, stage=FEATURE_STAGE_SHADOW, con=con, ts=789)

    evaluations = list_evaluations(con=con)
    features = list_registered_features(con=con)

    assert len(evaluations) == 1
    assert evaluations[0]["decision"] == "accepted"
    assert evaluations[0]["q_value"] == 0.004
    assert len(features) == 1
    assert features[0].feature_id == candidate.feature_id
    assert features[0].stage == "shadow"
    assert features[0].hash == candidate.hash


def test_register_feature_invalidates_feature_registry_process_cache(monkeypatch) -> None:
    con = sqlite3.connect(":memory:")
    ensure_discovery_schema(con)
    candidate = CandidateFeature(
        source="unit",
        symbol="MSFT",
        expression="x0",
        params={"feature_map": {"x0": "price.last"}},
    )
    calls: list[object] = []

    import engine.strategy.feature_registry as feature_registry

    monkeypatch.setattr(feature_registry, "invalidate_feature_registry_cache", lambda: calls.append(True))

    discovery_registry.register_feature(
        candidate,
        candidate_id=1,
        stage=FEATURE_STAGE_SHADOW,
        con=con,
        ts=999,
    )

    assert calls == [True]


def test_discovery_schema_setup_runs_once_per_process_for_ready_db_path(tmp_path) -> None:
    class CountingConnection:
        def __init__(self, path) -> None:
            self._con = sqlite3.connect(path)
            self.schema_statements: list[str] = []

        def execute(self, sql, parameters=()):
            text = str(sql or "").lstrip().upper()
            if text.startswith("CREATE TABLE") or text.startswith("CREATE INDEX"):
                self.schema_statements.append(str(sql))
            return self._con.execute(sql, parameters)

        def close(self) -> None:
            self._con.close()

    db_path = tmp_path / "feature_registry.sqlite"
    discovery_registry.invalidate_discovery_schema_cache()
    first = CountingConnection(db_path)
    second = CountingConnection(db_path)
    try:
        discovery_registry.ensure_discovery_schema(first)
        discovery_registry.ensure_discovery_schema(first)
        discovery_registry.ensure_discovery_schema(second)

        assert len(first.schema_statements) == 6
        assert second.schema_statements == []
    finally:
        first.close()
        second.close()
        discovery_registry.invalidate_discovery_schema_cache()
