from __future__ import annotations

import sqlite3

from engine.strategy.discovery.base import CandidateFeature, EvaluationResult
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
