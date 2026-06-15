from __future__ import annotations

import importlib.util
from typing import Any

import numpy as np
import pytest


LIGHTGBM_AVAILABLE = importlib.util.find_spec("lightgbm") is not None


@pytest.mark.skipif(not LIGHTGBM_AVAILABLE, reason="lightgbm not installed")
def test_init_model_refresh_beats_stale_model_on_drifted_holdout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RUNTIME_METRICS_BUFFER_ENABLED", "0")
    from engine.strategy.jobs.incremental_lgbm_refresh import compare_incremental_refresh
    from engine.strategy.models.lgbm_regressor import train_lgbm_regressor

    rng = np.random.default_rng(44)
    feature_ids = [
        "base.source_credibility",
        "base.log_recency_hours",
        "base.normalized_text_len",
        "base.scheduled_flag",
    ]
    hyperparams = {
        "n_estimators": 12,
        "learning_rate": 0.15,
        "num_leaves": 15,
        "min_child_samples": 1,
        "random_state": 11,
        "n_jobs": 1,
        "verbosity": -1,
        "deterministic": True,
        "force_col_wise": True,
    }
    X_train = rng.uniform(-1.0, 1.0, size=(300, len(feature_ids))).astype("float32")
    y_train = (X_train[:, 0] + 0.2 * X_train[:, 1]).astype("float32")
    stale = train_lgbm_regressor(
        X_train,
        y_train,
        feature_ids=list(feature_ids),
        hyperparams=hyperparams,
        model_name="lgbm_regressor.unit",
    )

    X_refresh = rng.uniform(-1.0, 1.0, size=(240, len(feature_ids))).astype("float32")
    y_refresh = (3.0 * X_refresh[:, 0] - 1.5 * X_refresh[:, 1] + 1.0).astype("float32")
    X_eval = rng.uniform(-1.0, 1.0, size=(120, len(feature_ids))).astype("float32")
    y_eval = (3.0 * X_eval[:, 0] - 1.5 * X_eval[:, 1] + 1.0).astype("float32")

    result = compare_incremental_refresh(
        stale,
        X_refresh,
        y_refresh,
        X_eval,
        y_eval,
        num_boost_round=35,
    )

    assert result["improved"] is True
    assert result["refreshed_metrics"]["rmse"] < result["stale_metrics"]["rmse"] * 0.5


@pytest.mark.skipif(not LIGHTGBM_AVAILABLE, reason="lightgbm not installed")
def test_shadow_mode_never_touches_serving_and_live_mode_audits(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RUNTIME_METRICS_BUFFER_ENABLED", "0")
    import engine.strategy.jobs.incremental_lgbm_refresh as refresh_module
    from engine.strategy.jobs.incremental_lgbm_refresh import route_incremental_refresh_result
    from engine.strategy.models.lgbm_regressor import train_lgbm_regressor

    feature_ids = [
        "base.source_credibility",
        "base.log_recency_hours",
        "base.normalized_text_len",
        "base.scheduled_flag",
    ]
    rng = np.random.default_rng(91)
    X = rng.uniform(-1.0, 1.0, size=(80, len(feature_ids))).astype("float32")
    y = (X[:, 0] - X[:, 1]).astype("float32")
    model = train_lgbm_regressor(
        X,
        y,
        feature_ids=list(feature_ids),
        hyperparams={
            "n_estimators": 8,
            "learning_rate": 0.2,
            "num_leaves": 7,
            "min_child_samples": 1,
            "random_state": 3,
            "n_jobs": 1,
            "verbosity": -1,
            "deterministic": True,
            "force_col_wise": True,
        },
        model_name="lgbm_regressor.unit",
    )

    assignment_calls: list[dict[str, Any]] = []
    audit_calls: list[dict[str, Any]] = []

    shadow = route_incremental_refresh_result(
        model,
        mode="shadow",
        symbol="AAPL",
        horizon_s=300,
        version="lgbm-incremental-shadow",
        metrics={"refreshed_metrics": {"rmse": 0.1}},
        register_artifact=False,
        set_assignment_fn=lambda **kwargs: assignment_calls.append(kwargs),
        audit_fn=lambda **kwargs: audit_calls.append(kwargs),
    )
    assert shadow["serving_updated"] is False
    assert assignment_calls == []
    assert audit_calls == []

    def _assignment(**kwargs: Any) -> dict[str, Any]:
        assignment_calls.append(dict(kwargs))
        return dict(kwargs)

    live = route_incremental_refresh_result(
        model,
        mode="live",
        symbol="AAPL",
        horizon_s=300,
        version="lgbm-incremental-live",
        metrics={"refreshed_metrics": {"rmse": 0.1}},
        register_artifact=False,
        set_assignment_fn=_assignment,
        audit_fn=lambda **kwargs: audit_calls.append(dict(kwargs)),
    )

    assert live["serving_updated"] is False
    assert live["ok"] is False
    assert live["error"] == "incremental_refresh_requires_competition_promotion"
    assert [call["state"] for call in assignment_calls] == ["challenger"]
    assert audit_calls
    assert audit_calls[0]["action"] == "block_incremental_lgbm_refresh"

    warn_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    monkeypatch.setattr(refresh_module, "_warn_nonfatal", lambda *args, **kwargs: warn_calls.append((args, kwargs)))
    assignment_calls.clear()

    failed_audit = route_incremental_refresh_result(
        model,
        mode="live",
        symbol="AAPL",
        horizon_s=300,
        version="lgbm-incremental-live-audit-fail",
        metrics={"refreshed_metrics": {"rmse": 0.1}},
        register_artifact=False,
        set_assignment_fn=_assignment,
        audit_fn=lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("audit failed")),
    )

    assert failed_audit["serving_updated"] is False
    assert failed_audit["ok"] is False
    assert failed_audit["error"] == "promotion_audit_failed"
    assert failed_audit["audit_recorded"] is False
    assert [call["state"] for call in assignment_calls] == ["challenger"]
    assert warn_calls[0][0][0] == "INCREMENTAL_LGBM_REFRESH_PROMOTION_AUDIT_FAILED"
