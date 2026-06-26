from __future__ import annotations

import math
import sqlite3

import numpy as np

from engine.risk import covariance
from engine.risk import monte_carlo_risk_engine as mc
from engine.risk import portfolio_risk_engine
from engine.strategy import risk as strategy_risk


def _returns(n: int = 90) -> dict[str, list[float]]:
    rng = np.random.default_rng(42)
    base = rng.normal(0.0004, 0.0100, n)
    return {
        "AAA": [float(v) for v in base],
        "BBB": [float(0.985 * base[i] + rng.normal(0.0, 0.00025)) for i in range(n)],
        "CCC": [float(v) for v in rng.normal(-0.0001, 0.0080, n)],
    }


def _db_from_returns(series_by_symbol: dict[str, list[float]]) -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE prices(symbol TEXT, ts_ms INTEGER, price REAL, px REAL)")
    for symbol, returns in series_by_symbol.items():
        price = 100.0
        con.execute(
            "INSERT INTO prices(symbol, ts_ms, price, px) VALUES (?, ?, ?, ?)",
            (symbol, 1_000, price, price),
        )
        for idx, ret in enumerate(returns, start=1):
            price *= math.exp(float(ret))
            con.execute(
                "INSERT INTO prices(symbol, ts_ms, price, px) VALUES (?, ?, ?, ?)",
                (symbol, 1_000 + idx * 1_000, price, price),
            )
    return con


def test_shrinkage_covariance_used_with_sufficient_aligned_history(monkeypatch) -> None:
    monkeypatch.setenv("RISK_COVARIANCE_METHOD", "ledoit_wolf")
    monkeypatch.setenv("RISK_COVARIANCE_MIN_OBS", "20")
    con = _db_from_returns(_returns(90))

    estimate = covariance.estimate_covariance(con, ["AAA", "BBB", "CCC"], lookback=90)

    assert estimate.diagnostics["method"] == "ledoit_wolf"
    assert estimate.diagnostics["fallback_reason"] == ""
    assert estimate.diagnostics["n_obs"] == 90
    assert estimate.diagnostics["covered_symbols"] == ["AAA", "BBB", "CCC"]
    assert estimate.diagnostics["shrinkage"] is not None


def test_sample_fallback_has_clear_diagnostics_when_history_is_thin(monkeypatch) -> None:
    monkeypatch.setenv("RISK_COVARIANCE_METHOD", "ledoit_wolf")
    monkeypatch.setenv("RISK_COVARIANCE_MIN_OBS", "60")
    monkeypatch.setenv("RISK_COVARIANCE_FALLBACK", "sample")
    con = _db_from_returns(_returns(12))

    estimate = covariance.estimate_covariance(con, ["AAA", "BBB", "CCC"], lookback=20)

    assert estimate.diagnostics["method"] == "sample_aligned"
    assert estimate.diagnostics["fallback_reason"] == "insufficient_observations_for_shrinkage"
    assert estimate.diagnostics["shrinkage"] is None
    assert estimate.diagnostics["n_obs"] == 12


def test_covariance_is_psd_enough_for_cholesky(monkeypatch) -> None:
    monkeypatch.setenv("RISK_COVARIANCE_METHOD", "ledoit_wolf")
    monkeypatch.setenv("RISK_COVARIANCE_MIN_OBS", "20")
    con = _db_from_returns(_returns(90))

    estimate = covariance.estimate_covariance(con, ["AAA", "BBB", "CCC"], lookback=90)
    cov = np.asarray(estimate.covariance, dtype=np.float64)

    chol = np.linalg.cholesky(cov + np.eye(cov.shape[0]) * 1e-12)
    assert chol.shape == (3, 3)


def test_shrinkage_improves_condition_number_on_near_collinear_matrix(monkeypatch) -> None:
    monkeypatch.setenv("RISK_COVARIANCE_METHOD", "ledoit_wolf")
    monkeypatch.setenv("RISK_COVARIANCE_MIN_OBS", "20")
    rng = np.random.default_rng(123)
    base = rng.normal(0.0, 0.01, 120)
    matrix = np.column_stack(
        [
            base,
            base * 1.0001 + rng.normal(0.0, 0.000001, 120),
            rng.normal(0.0, 0.006, 120),
        ]
    )

    estimate = covariance.estimate_covariance_from_returns(["AAA", "BBB", "CCC"], matrix, lookback=120)

    assert estimate.diagnostics["method"] == "ledoit_wolf"
    assert float(estimate.diagnostics["condition_number"]) < float(estimate.diagnostics["sample_condition_number"])


def test_correlation_cluster_components_are_deterministic(monkeypatch) -> None:
    monkeypatch.setenv("RISK_COVARIANCE_METHOD", "ledoit_wolf")
    monkeypatch.setenv("RISK_COVARIANCE_MIN_OBS", "20")
    monkeypatch.setattr(portfolio_risk_engine, "CORR_LOOKBACK", 90)
    monkeypatch.setattr(portfolio_risk_engine, "CLUSTER_CORR_TH", 0.50)
    monkeypatch.setattr(portfolio_risk_engine, "USE_FX_CURRENCY_CLUSTERS", False)
    con = _db_from_returns(_returns(90))

    first = portfolio_risk_engine._corr_graph_components(con, ["CCC", "BBB", "AAA"])
    second = portfolio_risk_engine._corr_graph_components(con, ["CCC", "BBB", "AAA"])

    assert first[0] == second[0]
    assert first[1] == second[1]
    assert first[3]["method"] == "ledoit_wolf"
    assert first[0] == [["AAA", "BBB"]]


def test_single_asset_portfolio_realized_vol_is_unchanged(monkeypatch) -> None:
    monkeypatch.setenv("RISK_COVARIANCE_METHOD", "ledoit_wolf")
    monkeypatch.setenv("RISK_COVARIANCE_MIN_OBS", "2")
    con = _db_from_returns({"AAA": _returns(40)["AAA"]})
    desired = {"AAA": {"weight": 0.40, "side": "LONG"}}

    legacy_vol = strategy_risk.realized_vol_from_prices(con, "AAA", lookback=40)
    portfolio_vol = strategy_risk.portfolio_realized_vol(con, desired, lookback=40)

    assert legacy_vol is not None
    assert portfolio_vol == float(0.40 * legacy_vol)


def test_monte_carlo_and_portfolio_risk_share_covariance_method(monkeypatch) -> None:
    monkeypatch.setenv("RISK_COVARIANCE_METHOD", "oas")
    monkeypatch.setenv("RISK_COVARIANCE_MIN_OBS", "20")
    monkeypatch.setattr(mc, "MC_LOOKBACK", 90)
    monkeypatch.setattr(
        mc,
        "resolve_vol_forecast",
        lambda *_args, **_kwargs: {"vol": None, "fallback": True, "resolved_source": "trailing"},
    )
    monkeypatch.setattr(portfolio_risk_engine, "CORR_LOOKBACK", 90)
    monkeypatch.setattr(portfolio_risk_engine, "CLUSTER_CORR_TH", 0.50)
    monkeypatch.setattr(portfolio_risk_engine, "USE_FX_CURRENCY_CLUSTERS", False)
    con = _db_from_returns(_returns(90))
    desired = {
        "AAA": {"weight": 0.20},
        "BBB": {"weight": 0.20},
        "CCC": {"weight": 0.10},
    }

    _symbols, _weights, _vols, _drifts, _corr, vol_meta, input_meta = mc._build_inputs(con, desired)
    comps, _matrix, _fx_edges, portfolio_diag = portfolio_risk_engine._corr_graph_components(
        con,
        ["AAA", "BBB", "CCC"],
    )

    mc_diag = vol_meta["__covariance_diagnostics__"]
    assert input_meta["covariance"]["diagnostics"]["method"] == "oas"
    assert comps == [["AAA", "BBB"]]
    assert mc_diag["method"] == "oas"
    assert portfolio_diag["method"] == "oas"
