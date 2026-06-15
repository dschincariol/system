from __future__ import annotations

import json
import math
import sqlite3

import numpy as np
import pytest

from engine.strategy.ensemble import hedge
from engine.strategy.ensemble.oos_store import ensure_schema as ensure_oos_schema, upsert_oos_predictions


def _marketplace_schema(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS model_marketplace_scores (
            model_id TEXT,
            model_name TEXT,
            symbol TEXT,
            horizon_s INTEGER,
            regime TEXT,
            stage TEXT,
            score REAL,
            trades INTEGER,
            wins INTEGER,
            losses INTEGER,
            net_pnl REAL,
            avg_confidence REAL,
            updated_ts_ms INTEGER,
            meta_json TEXT
        )
        """
    )


def _assignment_schema(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS champion_assignments (
            scope TEXT,
            symbol TEXT,
            horizon_s INTEGER,
            model_name TEXT,
            challenger_name TEXT,
            regime TEXT,
            state TEXT,
            assigned_ts_ms INTEGER,
            updated_ts_ms INTEGER,
            meta_json TEXT
        )
        """
    )


def _insert_marketplace(
    con: sqlite3.Connection,
    model_name: str,
    *,
    stage: str,
    symbol: str = "AAPL",
    horizon_s: int = 300,
    ts_ms: int = 1_000,
    score_source: str = "execution_fills",
) -> None:
    con.execute(
        """
        INSERT INTO model_marketplace_scores(
          model_id, model_name, symbol, horizon_s, regime, stage, score, trades,
          wins, losses, net_pnl, avg_confidence, updated_ts_ms, meta_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            model_name,
            model_name,
            symbol,
            int(horizon_s),
            "global",
            stage,
            1.0,
            10,
            6,
            4,
            1.0,
            0.7,
            int(ts_ms),
            json.dumps({"score_source": score_source, "model_kind": "ridge"}),
        ),
    )


def test_hedge_weights_sum_to_one_and_respect_floor() -> None:
    weights = hedge.compute_hedge_weights(
        {
            "champ": [0.01] * 60,
            "challenger_a": [0.04] * 60,
            "challenger_b": [0.09] * 60,
        },
        window=60,
        floor=0.02,
    )

    assert set(weights) == {"champ", "challenger_a", "challenger_b"}
    assert sum(weights.values()) == pytest.approx(1.0)
    assert min(weights.values()) >= 0.02
    assert weights["champ"] > weights["challenger_a"] > weights["challenger_b"]


def test_hedge_regret_and_weight_migration_on_switching_stream() -> None:
    losses = {"model_a": [], "model_b": []}
    blend_loss = 0.0
    model_a_loss = 0.0
    model_b_loss = 0.0
    snapshots: dict[int, dict[str, float]] = {}

    for idx in range(160):
        target = 1.0
        if idx < 80:
            preds = {"model_a": 1.0, "model_b": 0.0}
        else:
            preds = {"model_a": 0.0, "model_b": 1.0}
        weights = hedge.compute_hedge_weights(losses, window=60, floor=0.02)
        if not weights:
            weights = {"model_a": 0.5, "model_b": 0.5}
        blended = sum(weights[name] * preds[name] for name in preds)
        blend_loss += (blended - target) ** 2
        model_a_loss += (preds["model_a"] - target) ** 2
        model_b_loss += (preds["model_b"] - target) ** 2
        for name in preds:
            losses[name].append((preds[name] - target) ** 2)
        if idx in {70, 145}:
            snapshots[idx] = dict(hedge.compute_hedge_weights(losses, window=60, floor=0.02))

    assert snapshots[70]["model_a"] > 0.90
    assert snapshots[145]["model_b"] > 0.90
    assert blend_loss <= min(model_a_loss, model_b_loss) + 10.0


def test_refresh_hedge_weights_excludes_disqualified_model() -> None:
    con = sqlite3.connect(":memory:")
    ensure_oos_schema(con)
    _marketplace_schema(con)
    _insert_marketplace(con, "champ", stage="champion")
    _insert_marketplace(con, "challenger", stage="challenger")
    _insert_marketplace(con, "shadow_model", stage="shadow")

    rows = []
    for idx in range(80):
        target = math.sin(idx / 8.0)
        rows.extend(
            [
                {"symbol": "AAPL", "horizon": 300, "family": "champ", "ts": idx, "prediction": target + 0.10, "target": target},
                {"symbol": "AAPL", "horizon": 300, "family": "challenger", "ts": idx, "prediction": target + 0.01, "target": target},
                {"symbol": "AAPL", "horizon": 300, "family": "shadow_model", "ts": idx, "prediction": target, "target": target},
            ]
        )
    upsert_oos_predictions(rows, con=con)

    result = hedge.refresh_hedge_weights(con=con, symbols=["AAPL"], horizons=[300], now_ms=2_000)
    assert result["refreshed_count"] == 1
    weights = result["refreshed"][0]["weights"]
    assert set(weights) == {"champ", "challenger"}
    assert "shadow_model" not in weights
    assert weights["challenger"] > weights["champ"]


def test_commit_if_possible_raises_and_warns(monkeypatch) -> None:
    class CommitFails:
        def commit(self) -> None:
            raise RuntimeError("commit failed")

    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    monkeypatch.setattr(hedge, "_warn_nonfatal", lambda *args, **kwargs: calls.append((args, kwargs)))

    with pytest.raises(RuntimeError, match="commit failed"):
        hedge._commit_if_possible(CommitFails())

    assert calls
    assert calls[0][0][0] == "ENSEMBLE_HEDGE_COMMIT_FAILED"


def test_refresh_hedge_weights_reports_trigger_log_failure(monkeypatch) -> None:
    con = sqlite3.connect(":memory:")
    ensure_oos_schema(con)
    _marketplace_schema(con)
    _insert_marketplace(con, "champ", stage="champion")
    _insert_marketplace(con, "challenger", stage="challenger")
    rows = []
    for idx in range(80):
        target = math.sin(idx / 8.0)
        rows.extend(
            [
                {"symbol": "AAPL", "horizon": 300, "family": "champ", "ts": idx, "prediction": target + 0.10, "target": target},
                {"symbol": "AAPL", "horizon": 300, "family": "challenger", "ts": idx, "prediction": target + 0.01, "target": target},
            ]
        )
    upsert_oos_predictions(rows, con=con)
    monkeypatch.setattr(
        hedge,
        "effective_hedge_window",
        lambda *_args, **_kwargs: (30, {"triggered": True, "trigger_type": "bocpd"}),
    )
    monkeypatch.setattr(hedge, "effective_window_after_adwin", lambda *_args, **_kwargs: (30, {}))
    monkeypatch.setattr(hedge, "log_ensemble_trigger", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("audit down")))
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    monkeypatch.setattr(hedge, "_warn_nonfatal", lambda *args, **kwargs: calls.append((args, kwargs)))

    result = hedge.refresh_hedge_weights(con=con, symbols=["AAPL"], horizons=[300], now_ms=2_000)

    assert result["refreshed_count"] == 1
    assert result["trigger_log_failure_count"] == 1
    assert result["trigger_log_failures"][0]["error"] == "RuntimeError"
    assert calls[0][0][0] == "ENSEMBLE_HEDGE_TRIGGER_LOG_FAILED"


def test_qualified_pool_does_not_admit_assignment_only_challenger() -> None:
    con = sqlite3.connect(":memory:")
    _marketplace_schema(con)
    _assignment_schema(con)
    _insert_marketplace(con, "champ", stage="champion")
    con.execute(
        """
        INSERT INTO champion_assignments(
          scope, symbol, horizon_s, model_name, challenger_name, regime, state,
          assigned_ts_ms, updated_ts_ms, meta_json
        )
        VALUES ('global', 'AAPL', 300, 'champ', 'not_gate_passing', 'global', 'champion', 1000, 1000, '{}')
        """
    )

    assert hedge.qualified_model_pool(con, symbol="AAPL", horizon=300, asof_ts_ms=2_000) == ["champ"]

    _insert_marketplace(con, "gate_passing", stage="challenger")
    con.execute("UPDATE champion_assignments SET challenger_name='gate_passing'")
    assert hedge.qualified_model_pool(con, symbol="AAPL", horizon=300, asof_ts_ms=2_000) == ["champ", "gate_passing"]


def test_load_hedge_weights_filters_stale_disqualified_member() -> None:
    con = sqlite3.connect(":memory:")
    hedge.persist_hedge_weights(
        con,
        symbol="AAPL",
        horizon=300,
        weights={"champ": 0.2, "challenger": 0.3, "shadow_model": 0.5},
        ts_ms=1_000,
    )

    row = hedge.load_hedge_weights(
        con,
        symbol="AAPL",
        horizon=300,
        qualified_models=["champ", "challenger"],
    )

    assert row is not None
    assert set(row["weights"]) == {"champ", "challenger"}
    assert sum(row["weights"].values()) == pytest.approx(1.0)
    assert row["excluded_models"] == ["shadow_model"]


def test_predictor_hedge_mode_blends_qualified_models(monkeypatch) -> None:
    monkeypatch.setenv("PREDICTION_BLEND_MODE", "hedge")

    from engine.strategy import predictor

    def fake_resolve(symbol, horizon_s, forced_model_name=None):
        name = forced_model_name or "champ"
        return {
            "model_name": name,
            "model_id": name,
            "family": name,
            "model_family": name,
            "model_kind": "ridge",
            "feature_ids": [],
            "feature_schema": {},
        }

    def fake_predict_resolved(_query_vec, _sym, _h, *, top_k, active_model, event=None):
        name = str(active_model.get("model_name") or "")
        pred = 1.0 if name == "champ" else 3.0
        return pred, 0.5, {"model_name": name, "model_id": name, "model_kind": "ridge"}

    monkeypatch.setattr(predictor, "_resolve_active_model", fake_resolve)
    monkeypatch.setattr(predictor, "_predict_resolved_model", fake_predict_resolved)
    monkeypatch.setattr(predictor, "is_active_model_name", lambda model_name: True)
    monkeypatch.setattr(predictor, "_track_prediction_output", lambda **_kwargs: None)
    monkeypatch.setattr(predictor, "_cached_or_build_feature_snapshot", lambda **_kwargs: {})
    monkeypatch.setattr(
        predictor,
        "_maybe_attach_prediction_explanation",
        lambda *, symbol, horizon_s, event, explain, feature_snapshot: dict(explain),
    )
    monkeypatch.setattr(
        predictor.hedge_ensemble,
        "qualified_model_pool",
        lambda *_args, **_kwargs: ["champ", "challenger"],
    )
    monkeypatch.setattr(
        predictor.hedge_ensemble,
        "load_hedge_weights",
        lambda *_args, **_kwargs: {
            "ts_ms": 1_000,
            "regime": "AAPL:300",
            "weights": {"champ": 0.25, "challenger": 0.75},
            "excluded_models": [],
        },
    )
    monkeypatch.setattr(predictor, "connect", lambda **_kwargs: sqlite3.connect(":memory:"))

    pred, conf, explain = predictor._predict_single_model(
        np.asarray([1.0], dtype=np.float32),
        "AAPL",
        300,
        top_k=8,
        event={"ts_ms": 2_000},
    )

    assert pred == pytest.approx(2.5)
    assert conf == pytest.approx(0.5)
    assert explain["prediction_blend_mode"] == "hedge"
    assert explain["ensemble_weights"] == {"champ": 0.25, "challenger": 0.75}
    assert set(explain["component_vector"]["components"]) == {"champ", "challenger"}


def test_decision_log_uses_explain_component_vector_column(monkeypatch) -> None:
    con = sqlite3.connect(":memory:")
    captured = []
    events = []

    monkeypatch.setattr("engine.strategy.decision_log.append_chain_row", lambda table, payload, con_arg: captured.append(payload))
    monkeypatch.setattr("engine.strategy.decision_log.append_event", lambda **kwargs: events.append(kwargs))

    from engine.strategy.decision_log import log_decision

    component_vector = {
        "method": "hedge",
        "weights": {"champ": 0.4, "challenger": 0.6},
        "components": {"champ": {"prediction": 1.0}, "challenger": {"prediction": 2.0}},
    }
    log_decision(
        event_id=1,
        symbol="AAPL",
        horizon_s=300,
        predicted_z=1.6,
        confidence=0.7,
        model_name="champ",
        explain_json={"component_vector": component_vector},
        con=con,
    )

    assert json.loads(captured[0]["component_vector"]) == component_vector
    assert events[0]["payload"]["component_vector"] == component_vector
