from __future__ import annotations

import math

import numpy as np

from engine.strategy import cpcv
from engine.strategy import promotion_guard


def test_transaction_costs_apply_on_position_changes_with_ac_temporary_impact() -> None:
    predictions = np.asarray([1.0, 1.0, -1.0, 0.0], dtype=float)
    realized = np.asarray([0.010, 0.020, -0.010, 0.030], dtype=float)
    cost_config = {
        "enabled": True,
        "asset_class": "US_EQUITY",
        "commission_bps": 1.0,
        "half_spread_bps": 2.0,
        "notional": 100.0,
        "adv": 10_000.0,
        "sigma_daily": 100.0,
        "participation": 0.10,
        # Deliberate borrow-free AC/turnover baseline; EQ-02 borrow math is tested separately.
        "borrow_enabled": False,
    }

    adjusted, meta = cpcv._apply_transaction_costs_to_returns(
        predictions,
        realized,
        cost_config=cost_config,
    )

    eta = 0.142
    temp_one = eta * 100.0 * math.sqrt(100.0 / 10_000.0)
    temp_flip = eta * 100.0 * math.sqrt(200.0 / 10_000.0)
    expected = np.asarray(
        [
            0.010 - ((1.0 + 2.0 + temp_one) / 10_000.0),
            0.020,
            0.010 - (2.0 * (1.0 + 2.0 + temp_flip) / 10_000.0),
            0.0 - ((1.0 + 2.0 + temp_one) / 10_000.0),
        ],
        dtype=float,
    )

    assert np.allclose(adjusted, expected, atol=1e-12)
    assert meta["turnover"] == [1.0, 0.0, 2.0, 1.0]
    assert float(meta["total_cost_return"]) > 0.0


def test_nonzero_costs_and_turnover_reduce_sharpe() -> None:
    predictions = np.asarray([1.0, 1.0, -1.0, -1.0, 1.0, 1.0], dtype=float)
    realized = np.asarray([0.010, 0.012, -0.011, -0.013, 0.012, 0.014], dtype=float)
    adjusted, meta = cpcv._apply_transaction_costs_to_returns(
        predictions,
        realized,
        cost_config={
            "enabled": True,
            "asset_class": "US_EQUITY",
            "commission_bps": 2.0,
            "half_spread_bps": 3.0,
            "notional": 100_000.0,
            "adv": 10_000_000.0,
            "sigma_daily": 100.0,
            "participation": 0.10,
            # Keep this Sharpe regression focused on transaction costs, not short borrow carry.
            "borrow_enabled": False,
        },
    )

    frictionless = np.asarray(meta["frictionless_returns"], dtype=float)
    assert sum(float(value) for value in meta["turnover"]) > 0.0
    assert cpcv._compute_sharpe(adjusted) < cpcv._compute_sharpe(frictionless)


class _EchoPredictionModel:
    def fit(self, features, labels):
        del labels
        self.fit_rows = int(len(features))
        return self

    def predict(self, features):
        return np.asarray(features, dtype=float)[:, 0]


def test_cpcv_reports_frictionless_and_cost_adjusted_metrics() -> None:
    predictions = np.asarray([1.0, 1.0, -1.0, -1.0, 1.0, 1.0, -1.0, -1.0, 1.0], dtype=float)
    realized = np.sign(predictions) * np.asarray([0.010, 0.011, 0.012, 0.013, 0.011, 0.012, 0.014, 0.015, 0.013])

    result = cpcv.cpcv_backtest(
        predictions.reshape(-1, 1),
        realized,
        model_factory=_EchoPredictionModel,
        n_splits=3,
        n_test_splits=1,
        embargo_pct=0.0,
        label_horizon=1,
        cost_config={
            "enabled": True,
            "asset_class": "US_EQUITY",
            "commission_bps": 2.0,
            "half_spread_bps": 3.0,
            "notional": 100_000.0,
            "adv": 10_000_000.0,
            "sigma_daily": 100.0,
            "participation": 0.10,
            # Preserve the existing CPCV metric golden as a borrow-free realism baseline.
            "borrow_enabled": False,
        },
    )

    assert result["ok"]
    assert result["diagnostics"]["metric_basis"] == "cost_adjusted"
    assert result["mean_sharpe"] < result["frictionless_mean_sharpe"]
    assert result["deflated_sharpe"]["raw_sharpe"] == result["median_sharpe"]
    assert "frictionless_returns" in result["paths"][0]
    assert result["paths"][0]["returns"] == result["paths"][0]["cost_adjusted_returns"]


def test_retrain_cadence_replay_fits_only_past_rows() -> None:
    features = np.arange(12, dtype=float).reshape(-1, 1)
    labels = np.ones(12, dtype=float) * 0.01

    result = cpcv.cpcv_backtest(
        features,
        labels,
        model_factory=_EchoPredictionModel,
        n_splits=3,
        n_test_splits=1,
        embargo_pct=0.0,
        label_horizon=1,
        sample_times_ms=np.arange(12, dtype=int),
        cost_config={"enabled": False},
        replay_retrain_cadence=True,
        retrain_cadence_ms=2,
    )

    fit_events = [
        event
        for path in result["paths"]
        for event in dict(path.get("retrain_replay") or {}).get("fit_events", [])
    ]
    assert fit_events
    assert any(int(dict(path.get("retrain_replay") or {}).get("fit_count") or 0) > 1 for path in result["paths"])
    assert all(int(event["train_max_ts_ms"]) < int(event["decision_ts_ms"]) for event in fit_events)


def test_promotion_gate_consumes_cost_adjusted_cpcv_series(monkeypatch) -> None:
    run = {
        "n_splits": 6,
        "n_test_splits": 2,
        "embargo_pct": 0.01,
        "n_paths": 2,
        "pbo": 0.10,
        "mean_sharpe": 0.20,
        "median_sharpe": 0.20,
        "diagnostics": {
            "metric_basis": "cost_adjusted",
            "frictionless": {"median_sharpe": 2.0},
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
                "retrain_cadence_replay": True,
                "retrain_cadence_ms": 123,
                "gated_backtest": False,
            }
        },
    )

    assert not passed
    assert diagnostics["status"] == "median_sharpe_below_threshold"
    assert diagnostics["latest_run"]["median_sharpe"] == 0.20
