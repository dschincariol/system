import json
import sqlite3

import numpy as np
import pytest

from engine.causal.scores import CausalScoreRecord, ensure_causal_schema, upsert_causal_score
from engine.strategy.ensemble.blender import EnsembleBlender, ensure_schema as ensure_weight_schema
from engine.strategy.ensemble.blender import persist_weights
from engine.strategy.ensemble.oos_store import ensure_schema as ensure_oos_schema, read_oos_predictions, upsert_oos_predictions
from engine.strategy.jobs import train_ensemble
from engine.strategy.jobs.train_ensemble import fit_and_persist_group


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


def test_oos_refit_blend_and_decision_log_components(monkeypatch):
    con = sqlite3.connect(":memory:")
    ensure_oos_schema(con)
    ensure_weight_schema(con)
    _allow_families(con)

    rows = []
    for ts in range(1, 60):
        target = float(ts) / 100.0
        rows.extend(
            [
                {"symbol": "AAPL", "horizon": 3600, "family": "embed_regressor", "ts": ts, "prediction": target + 0.01, "target": target},
                {"symbol": "AAPL", "horizon": 3600, "family": "temporal_predictor", "ts": ts, "prediction": target - 0.01, "target": target},
            ]
        )
    upsert_oos_predictions(rows, con=con)
    train_rows = read_oos_predictions(symbol="AAPL", horizon=3600, require_target=True, con=con)

    persisted = fit_and_persist_group(
        train_rows,
        symbol="AAPL",
        horizon=3600,
        alpha=0.01,
        nonneg=True,
        con=con,
        ts=9999,
    )
    assert persisted is not None

    result = EnsembleBlender(con=con, mode="blend").blend(
        symbol="AAPL",
        horizon=3600,
        ts=10000,
        base_prediction=0.0,
        base_confidence=0.4,
        base_family="embed_regressor",
        predict_family=lambda family: {"prediction": 0.75 if family == "embed_regressor" else 0.70, "confidence": 0.6},
    )
    assert result.applied is True
    assert set(result.diagnostics["components"]) == {"embed_regressor", "temporal_predictor"}
    assert set(result.diagnostics["weights"]) == {"embed_regressor", "temporal_predictor"}

    chain_rows = []
    events = []

    def fake_append_chain_row(table_name, payload, con_arg):
        chain_rows.append((table_name, payload))

    def fake_append_event(**kwargs):
        events.append(kwargs)

    monkeypatch.setattr("engine.strategy.decision_log.append_chain_row", fake_append_chain_row)
    monkeypatch.setattr("engine.strategy.decision_log.append_event", fake_append_event)

    from engine.strategy.decision_log import log_decision

    log_decision(
        event_id=1,
        symbol="AAPL",
        horizon_s=3600,
        predicted_z=result.prediction,
        confidence=result.confidence,
        model_name="embed_regressor",
        model_version="abc123:1700000000000",
        ensemble_components=result.diagnostics["components"],
        ensemble_weights=result.diagnostics["weights"],
        component_vector=result.diagnostics,
        con=con,
    )

    explain_payload = json.loads(chain_rows[0][1]["explain_json"])
    components_payload = json.loads(chain_rows[0][1]["components_json"])
    assert chain_rows[0][1]["model_version"] == "abc123:1700000000000"
    assert set(explain_payload["ensemble_components"]) == {"embed_regressor", "temporal_predictor"}
    assert set(explain_payload["ensemble_weights"]) == {"embed_regressor", "temporal_predictor"}
    assert set(explain_payload["component_vector"]["components"]) == {"embed_regressor", "temporal_predictor"}
    assert set(components_payload["components"]) == {"embed_regressor", "temporal_predictor"}
    assert events[0]["payload"]["model_version"] == "abc123:1700000000000"
    assert set(events[0]["payload"]["components_json"]["weights"]) == {"embed_regressor", "temporal_predictor"}
    assert events[0]["payload"]["explain_json"]["ensemble_weights"] == result.diagnostics["weights"]


def test_train_ensemble_alpha_uses_catalog_with_env_override(monkeypatch):
    monkeypatch.delenv("ENSEMBLE_RIDGE_ALPHA", raising=False)

    def fake_fetch_latest_model_hyperparameters(**kwargs):
        return {"params": {"alpha": 0.25}, "model_name": kwargs.get("model_name")}

    monkeypatch.setattr(train_ensemble, "fetch_latest_model_hyperparameters", fake_fetch_latest_model_hyperparameters)
    assert train_ensemble.resolve_ridge_alpha(symbol="AAPL", horizon=3600, default=1.0) == 0.25

    monkeypatch.setenv("ENSEMBLE_RIDGE_ALPHA", "0.5")
    assert train_ensemble.resolve_ridge_alpha(symbol="AAPL", horizon=3600, default=1.0) == 0.5


def test_train_ensemble_derives_causal_prior_when_lambda_is_set(monkeypatch):
    con = sqlite3.connect(":memory:")
    ensure_oos_schema(con)
    ensure_weight_schema(con)
    ensure_causal_schema(con)
    _allow_families(con)
    now_ms = int(__import__("time").time() * 1000)
    rows = []
    for idx in range(1, 80):
        target = float(idx) / 100.0
        rows.extend(
            [
                {
                    "symbol": "AAPL",
                    "horizon": 3600,
                    "family": "embed_regressor",
                    "ts": now_ms + idx,
                    "prediction": target + 0.01,
                    "target": target,
                },
                {
                    "symbol": "AAPL",
                    "horizon": 3600,
                    "family": "temporal_predictor",
                    "ts": now_ms + idx,
                    "prediction": target - 0.01,
                    "target": target,
                },
            ]
        )
    upsert_oos_predictions(rows, con=con)
    upsert_causal_score(
        con,
        CausalScoreRecord(
            feature="embed_regressor",
            target="impact_z_3600",
            window="365d",
            ts=now_ms,
            granger_p=0.001,
            granger_lag=1,
            dowhy_effect=None,
            dowhy_p=None,
            score=0.9,
            decision="granger_only",
        ),
    )
    upsert_causal_score(
        con,
        CausalScoreRecord(
            feature="temporal_predictor",
            target="impact_z_3600",
            window="365d",
            ts=now_ms,
            granger_p=0.8,
            granger_lag=1,
            dowhy_effect=None,
            dowhy_p=None,
            score=0.1,
            decision="granger_only",
        ),
    )
    monkeypatch.setenv("ENSEMBLE_CAUSAL_PRIOR_LAMBDA", "5.0")
    monkeypatch.setenv("ENSEMBLE_REFIT_LOOKBACK_DAYS", "1")

    result = train_ensemble.run(con=con)

    prior = result["causal_prior_by_group"]["AAPL:3600"]
    assert prior["embed_regressor"] > prior["temporal_predictor"]
    assert result["persisted"][0]["causal_prior_weights"] == prior
    assert result["lambda_prior_by_group"]["AAPL:3600"] == pytest.approx(5.0)


def test_predictor_wrapper_serves_ridge_blend_and_explain(monkeypatch):
    con = sqlite3.connect(":memory:")
    ensure_weight_schema(con)
    _allow_families(con)
    persist_weights(
        symbol="AAPL",
        horizon=3600,
        weights={"embed_regressor": 0.4, "temporal_predictor": 0.6},
        intercept=0.1,
        alpha=0.01,
        n_train_obs=50,
        ts=999,
        con=con,
    )

    from engine.strategy import predictor

    monkeypatch.setattr(
        predictor,
        "RidgeStackBlender",
        lambda: EnsembleBlender(con=con, mode="blend"),
    )

    active_model = {
        "model_name": "embed_regressor.live",
        "model_id": "embed_regressor.live:AAPL:v1",
        "family": "embed_regressor",
        "model_family": "embed_regressor",
        "model_version": "v1",
        "model_kind": "ridge",
        "feature_ids": [],
        "feature_schema": {},
    }
    family_models = {
        "temporal_predictor": {
            "model_name": "temporal_predictor.live",
            "model_id": "temporal_predictor.live:AAPL:v1",
            "family": "temporal_predictor",
            "model_family": "temporal_predictor",
            "model_version": "v1",
            "model_kind": "temporal",
            "feature_ids": [],
            "feature_schema": {},
        }
    }

    def fake_resolve_family(symbol, horizon_s, family, primary_active_model=None):
        return dict(family_models.get(family) or {})

    def fake_predict_resolved_model(query_vec, sym, h, *, top_k, active_model, event=None):
        assert str(active_model.get("family")) == "temporal_predictor"
        return (
            2.0,
            0.8,
            {
                "model_name": "temporal_predictor.live",
                "model_family": "temporal_predictor",
                "model_version": "v1",
            },
        )

    monkeypatch.setattr(predictor, "_resolve_active_model_for_family", fake_resolve_family)
    monkeypatch.setattr(predictor, "_predict_resolved_model", fake_predict_resolved_model)

    prediction, confidence, explain = predictor._maybe_apply_stacked_ridge_ensemble(
        np.asarray([1.0], dtype=np.float32),
        "AAPL",
        3600,
        top_k=4,
        event={"ts_ms": 1234},
        active_model=dict(active_model),
        base_prediction=(
            1.0,
            0.5,
            {
                "model_name": "embed_regressor.live",
                "model_family": "embed_regressor",
                "model_version": "v1",
            },
        ),
    )

    assert prediction == pytest.approx(1.7)
    assert confidence == pytest.approx((0.4 * 0.5 + 0.6 * 0.8) / 1.0)
    assert explain["ensemble_output"]["fallback"] is False
    assert explain["ensemble_output"]["method"] == "ridge_stack"
    assert set(explain["ensemble_components"]) == {"embed_regressor", "temporal_predictor"}
    assert explain["ensemble_weights"] == {"embed_regressor": 0.4, "temporal_predictor": 0.6}
