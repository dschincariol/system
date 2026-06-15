from __future__ import annotations

import numpy as np

from engine.strategy import cpcv
from engine.strategy import promotion_guard
from engine.strategy.gated_backtest import run_gated_backtest


class _EchoPredictionModel:
    def fit(self, features, labels):
        del labels
        return self

    def predict(self, features):
        return np.asarray(features, dtype=float)[:, 0]


def test_gated_backtest_is_deterministic_for_same_inputs() -> None:
    kwargs = {
        "predictions": [0.8, -0.4, 0.7, 0.2, -0.6],
        "realized_returns": [0.01, -0.02, 0.03, 0.04, -0.01],
        "sample_times_ms": [10, 10, 20, 20, 30],
        "symbols": ["AAA", "BBB", "AAA", "CCC", "BBB"],
        "cost_config": {
            "enabled": True,
            "asset_class": "US_EQUITY",
            "commission_bps": 1.0,
            "half_spread_bps": 2.0,
            "notional": 100_000.0,
            "adv": 10_000_000.0,
            "sigma_daily": 100.0,
            "participation": 0.10,
        },
        "max_positions": 3,
    }

    first = run_gated_backtest(**kwargs)
    second = run_gated_backtest(**kwargs)

    assert first["returns"] == second["returns"]
    assert first["costs"] == second["costs"]
    assert first["selected_symbols_by_ts"] == second["selected_symbols_by_ts"]
    assert first["diagnostics"] == second["diagnostics"]


def test_gated_path_applies_live_max_position_constraint() -> None:
    result = run_gated_backtest(
        [0.9, 0.8, 0.7, 0.1],
        [0.01, 0.01, 0.01, 0.50],
        sample_times_ms=[1000, 1000, 1000, 1000],
        symbols=["AAA", "BBB", "CCC", "DDD"],
        cost_config={"enabled": False},
        max_positions=3,
    )

    assert result["selected_symbols_by_ts"][0]["selected_symbols"] == ["AAA", "BBB", "CCC"]
    assert result["selected_symbols_by_ts"][0]["excluded_symbols"] == ["DDD"]
    assert np.allclose(result["frictionless_returns"], [0.53], atol=1e-12)
    assert np.allclose(result["returns"], [0.024], atol=1e-12)
    assert result["diagnostics"]["total_return_gap"] < 0.0


def test_cpcv_gated_mode_reports_gated_cost_adjusted_basis() -> None:
    predictions = np.asarray([1.0, 0.8, -0.9, -0.7, 0.6, 0.5, -0.4, -0.3, 0.2], dtype=float)
    realized = np.sign(predictions) * np.asarray([0.010, 0.011, 0.012, 0.013, 0.011, 0.012, 0.014, 0.015, 0.013])

    result = cpcv.cpcv_backtest(
        predictions.reshape(-1, 1),
        realized,
        model_factory=_EchoPredictionModel,
        n_splits=3,
        n_test_splits=1,
        embargo_pct=0.0,
        label_horizon=1,
        sample_times_ms=np.arange(9, dtype=int),
        symbols=[f"SYM{idx}" for idx in range(9)],
        cost_config={"enabled": False},
        gated_backtest=True,
    )

    assert result["ok"]
    assert result["diagnostics"]["metric_basis"] == "gated_cost_adjusted"
    assert result["diagnostics"]["gated_backtest"]["enabled"] is True
    assert result["paths"][0]["gated_backtest"]["enabled"] is True
    assert result["paths"][0]["returns"] == result["paths"][0]["cost_adjusted_returns"]


def test_promotion_guard_accepts_gated_cpcv_series(monkeypatch) -> None:
    run = {
        "id": 42,
        "created_ts": 123,
        "n_splits": 6,
        "n_test_splits": 2,
        "embargo_pct": 0.01,
        "n_paths": 2,
        "pbo": 0.10,
        "mean_sharpe": 0.80,
        "median_sharpe": 0.80,
        "diagnostics": {
            "metric_basis": "gated_cost_adjusted",
            "gated_backtest": {"enabled": True},
            "retrain_cadence_replay": {"enabled": True, "cadence_ms": 123},
        },
    }
    monkeypatch.setattr(promotion_guard, "fetch_latest_backtest_cpcv_run", lambda **_kwargs: dict(run))

    passed, diagnostics = promotion_guard.evaluate_cpcv_promotion_gate(
        model_name="candidate",
        candidate_version="v1",
        config={
            "cpcv": {
                "enabled": True,
                "n_splits": 6,
                "n_test_splits": 2,
                "embargo_pct": 0.01,
                "max_pbo": 0.50,
                "min_path_sharpe": 0.50,
                "costs_enabled": True,
                "retrain_cadence_replay": True,
                "retrain_cadence_ms": 123,
                "gated_backtest": True,
            }
        },
    )

    assert passed
    assert diagnostics["status"] == "evaluated"
    assert diagnostics["run_diagnostics"]["metric_basis"] == "gated_cost_adjusted"
