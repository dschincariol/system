import sqlite3

import pytest

from engine.strategy.ensemble.blender import EnsembleBlender, ensure_schema, persist_weights


@pytest.fixture(autouse=True)
def captured_fallback_metrics(monkeypatch):
    rows = []
    monkeypatch.setattr(
        "engine.strategy.ensemble.blender._emit_fallback_depth_metric",
        lambda **kwargs: rows.append(dict(kwargs)),
    )
    return rows


def _allow_families(con, families=("embed_regressor", "temporal_predictor")):
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS model_registry (
            model_name TEXT,
            stage TEXT,
            updated_ts_ms INTEGER
        )
        """
    )
    con.executemany(
        "INSERT INTO model_registry(model_name, stage, updated_ts_ms) VALUES (?, ?, ?)",
        [(family, "champion", 1) for family in families],
    )


def test_blender_falls_back_to_single_champion_without_weights():
    con = sqlite3.connect(":memory:")
    ensure_schema(con)

    result = EnsembleBlender(con=con, mode="blend").blend(
        symbol="AAPL",
        horizon=3600,
        ts=123,
        base_prediction=1.25,
        base_confidence=0.6,
        base_family="embed_regressor",
        predict_family=lambda family: None,
    )

    assert result.applied is False
    assert result.prediction == 1.25
    assert result.confidence == 0.6
    assert result.diagnostics["fallback_reason"] == "no_ensemble_weights"
    assert result.diagnostics["fallback_cascade_depth"] == 2


def test_blender_loads_weights_and_computes_weighted_sum(captured_fallback_metrics):
    con = sqlite3.connect(":memory:")
    ensure_schema(con)
    _allow_families(con)
    persist_weights(
        symbol="AAPL",
        horizon=3600,
        weights={"embed_regressor": 0.2, "temporal_predictor": 0.3},
        intercept=0.5,
        alpha=1.0,
        n_train_obs=20,
        val_metric=0.2,
        ts=2000,
        con=con,
    )

    components = {
        "embed_regressor": {"prediction": 2.0, "confidence": 0.5},
        "temporal_predictor": {"prediction": 4.0, "confidence": 0.7},
    }
    result = EnsembleBlender(con=con, mode="blend").blend(
        symbol="AAPL",
        horizon=3600,
        ts=3000,
        base_prediction=9.0,
        base_confidence=0.1,
        base_family="embed_regressor",
        predict_family=lambda family: components[family],
    )

    assert result.applied is True
    assert abs(result.prediction - 2.1) < 1e-12
    assert result.diagnostics["weights"] == {"embed_regressor": 0.2, "temporal_predictor": 0.3}
    assert set(result.diagnostics["components"]) == {"embed_regressor", "temporal_predictor"}
    assert result.diagnostics["fallback_cascade_depth"] == 0
    assert captured_fallback_metrics[-1]["depth"] == 0


def test_shadow_stage_family_is_excluded_from_weight_vector():
    con = sqlite3.connect(":memory:")
    ensure_schema(con)
    con.execute(
        """
        CREATE TABLE model_marketplace_scores (
            model_name TEXT,
            stage TEXT,
            symbol TEXT,
            horizon_s INTEGER,
            updated_ts_ms INTEGER
        )
        """
    )
    con.executemany(
        "INSERT INTO model_marketplace_scores(model_name, stage, symbol, horizon_s, updated_ts_ms) VALUES (?, ?, ?, ?, ?)",
        [
            ("embed_regressor", "champion", "AAPL", 3600, 10),
            ("temporal_predictor", "shadow", "AAPL", 3600, 10),
        ],
    )
    persist_weights(
        symbol="AAPL",
        horizon=3600,
        weights={"embed_regressor": 1.0, "temporal_predictor": 10.0},
        intercept=0.0,
        ts=2000,
        con=con,
    )

    result = EnsembleBlender(con=con, mode="blend").blend(
        symbol="AAPL",
        horizon=3600,
        ts=3000,
        base_prediction=0.0,
        base_confidence=0.5,
        base_family="embed_regressor",
        predict_family=lambda family: {"prediction": 3.0, "confidence": 0.5},
    )

    assert result.applied is True
    assert result.prediction == 3.0
    assert result.diagnostics["weights"] == {"embed_regressor": 1.0}
    assert result.diagnostics["excluded_families"] == {"temporal_predictor": "shadow_stage"}
    assert result.diagnostics["fallback_cascade_depth"] == 0


def test_blender_depth_one_renormalizes_when_some_components_missing(captured_fallback_metrics):
    con = sqlite3.connect(":memory:")
    ensure_schema(con)
    _allow_families(con)
    persist_weights(
        symbol="AAPL",
        horizon=3600,
        weights={"embed_regressor": 0.2, "temporal_predictor": 0.3},
        intercept=0.0,
        ts=2000,
        con=con,
    )

    result = EnsembleBlender(con=con, mode="blend").blend(
        symbol="AAPL",
        horizon=3600,
        ts=3000,
        base_prediction=9.0,
        base_confidence=0.1,
        base_family="embed_regressor",
        predict_family=lambda family: {"prediction": 2.0, "confidence": 0.5} if family == "embed_regressor" else None,
    )

    assert result.applied is True
    assert result.diagnostics["fallback_cascade_depth"] == 1
    assert result.diagnostics["fallback_reason"] == "partial_component_predictions"
    assert result.diagnostics["missing_families"] == ["temporal_predictor"]
    assert result.diagnostics["weights"] == {"embed_regressor": 0.5}
    assert result.prediction == 1.0
    assert captured_fallback_metrics[-1]["depth"] == 1


def test_blender_depth_two_when_no_eligible_families(captured_fallback_metrics):
    con = sqlite3.connect(":memory:")
    ensure_schema(con)
    persist_weights(
        symbol="AAPL",
        horizon=3600,
        weights={"embed_regressor": 1.0},
        intercept=0.0,
        ts=2000,
        con=con,
    )

    result = EnsembleBlender(con=con, mode="blend").blend(
        symbol="AAPL",
        horizon=3600,
        ts=3000,
        base_prediction=7.0,
        base_confidence=0.4,
        base_family="embed_regressor",
        predict_family=lambda family: {"prediction": 3.0, "confidence": 0.5},
    )

    assert result.applied is False
    assert result.prediction == 7.0
    assert result.diagnostics["fallback_cascade_depth"] == 2
    assert result.diagnostics["fallback_reason"] == "no_eligible_weights"
    assert captured_fallback_metrics[-1]["depth"] == 2


def test_blender_depth_three_uses_last_good_when_champion_missing(captured_fallback_metrics):
    con = sqlite3.connect(":memory:")
    ensure_schema(con)
    _allow_families(con, families=("embed_regressor",))
    persist_weights(
        symbol="AAPL",
        horizon=3600,
        weights={"embed_regressor": 1.0},
        intercept=0.0,
        ts=2000,
        con=con,
    )
    blender = EnsembleBlender(con=con, mode="blend")
    first = blender.blend(
        symbol="AAPL",
        horizon=3600,
        ts=3000,
        base_prediction=7.0,
        base_confidence=0.4,
        base_family="embed_regressor",
        predict_family=lambda family: {"prediction": 3.0, "confidence": 0.5},
    )

    assert first.prediction == 3.0

    empty = sqlite3.connect(":memory:")
    ensure_schema(empty)
    blender.con = empty
    second = blender.blend(
        symbol="AAPL",
        horizon=3600,
        ts=4000,
        base_prediction=float("nan"),
        base_confidence=0.0,
        base_family="embed_regressor",
        predict_family=lambda family: None,
    )

    assert second.applied is False
    assert second.prediction == 3.0
    assert second.diagnostics["fallback_cascade_depth"] == 3
    assert second.diagnostics["fallback_reason"] == "no_ensemble_weights_champion_missing"
    assert captured_fallback_metrics[-1]["depth"] == 3
