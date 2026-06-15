from __future__ import annotations

import copy
import importlib
import math
import sqlite3

from engine.strategy import har_rv


def _prices_table(con: sqlite3.Connection) -> None:
    con.execute("CREATE TABLE prices (ts_ms INTEGER, symbol TEXT, price REAL)")


def test_har_coefficients_recover_synthetic_structure() -> None:
    beta = (0.00001, 0.22, 0.31, 0.27)
    rv = [0.00010 + 0.00002 * math.sin(i / 3.0) + 0.000003 * (i % 5) for i in range(22)]
    while len(rv) < 160:
        idx = len(rv) - 1
        rv_next = (
            beta[0]
            + beta[1] * rv[idx]
            + beta[2] * (sum(rv[idx - 4: idx + 1]) / 5.0)
            + beta[3] * (sum(rv[idx - 21: idx + 1]) / 22.0)
        )
        rv.append(float(rv_next))

    fit = har_rv.fit_har_coefficients(rv, min_history=60)
    assert fit is not None
    assert fit.n_obs == len(rv)
    for actual, expected in zip(fit.coefficients, beta):
        assert actual == pytest_approx(expected, rel=1e-5, abs=1e-8)


def test_har_forecast_falls_back_to_trailing_when_history_short() -> None:
    con = sqlite3.connect(":memory:")
    _prices_table(con)
    rows = [(1_000 + i * 60_000, "AAA", 100.0 + i * 0.2) for i in range(12)]
    con.executemany("INSERT INTO prices(ts_ms, symbol, price) VALUES (?, ?, ?)", rows)

    payload = har_rv.forecast_har_for_symbol(con, "AAA", ts_ms=20_000, min_history=60)
    assert payload["fallback"] is True
    assert payload["source"] == "trailing_fallback"
    assert payload["n_obs"] == 0
    assert payload["forecast_vol_1d"] > 0.0


def test_candidate_symbols_warns_when_primary_source_query_fails(monkeypatch) -> None:
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE price_bars (not_symbol TEXT)")
    con.execute("CREATE TABLE prices (symbol TEXT)")
    con.execute("INSERT INTO prices(symbol) VALUES ('BBB')")
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    monkeypatch.setattr(har_rv, "_warn_nonfatal", lambda *args, **kwargs: calls.append((args, kwargs)))

    assert har_rv._candidate_symbols(con, 10) == ["BBB"]
    assert calls
    assert calls[0][0][0] == "HAR_RV_CANDIDATE_SYMBOLS_QUERY_FAILED"
    assert calls[0][1]["table"] == "price_bars"


def _har_forecast_connection(vol: float) -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    har_rv.ensure_har_rv_schema(con)
    har_rv.upsert_har_forecast(
        con,
        {
            "symbol": "AAA",
            "ts_ms": 1_000,
            "asof_ts_ms": 500,
            "rv": float(vol) * float(vol),
            "trailing_vol": 0.02,
            "forecast_rv_1d": float(vol) * float(vol),
            "forecast_vol_1d": float(vol),
            "forecast_ann_vol": float(vol) * math.sqrt(252.0),
            "forecast_ratio": float(vol) / 0.02,
            "n_obs": 80,
            "n_train": 58,
            "min_history": 60,
            "source": "garman_klass_daily_ohlc",
            "fallback": False,
            "diagnostics": {},
            "created_ts_ms": 1_000,
        },
    )
    return con


def test_vol_target_sizing_uses_higher_har_forecast_to_shrink(monkeypatch) -> None:
    monkeypatch.setenv("VOL_FORECAST_SOURCE", "har")
    monkeypatch.setenv("PORTFOLIO_RISK_VOL_TARGET", "0.02")
    monkeypatch.setenv("PORTFOLIO_RISK_VOL_FLOOR", "0.0001")
    monkeypatch.setenv("PORTFOLIO_RISK_VOL_CEIL", "1.0")

    import engine.risk.portfolio_risk_engine as pre

    pre = importlib.reload(pre)
    high = _har_forecast_connection(0.04)
    low = _har_forecast_connection(0.01)
    desired = {"AAA": {"weight": 1.0, "side": "LONG", "reason": {}}}

    high_out = pre._apply_portfolio_vol_target(high, copy.deepcopy(desired), {"ts_ms": 2_000})
    low_out = pre._apply_portfolio_vol_target(low, copy.deepcopy(desired), {"ts_ms": 2_000})

    assert high_out["AAA"]["weight"] < low_out["AAA"]["weight"]
    assert high_out["AAA"]["weight"] == pytest_approx(0.5)
    assert high_out["AAA"]["reason"]["portfolio_vol_target"]["vol_source"] == "har"


def test_har_tech_features_round_trip_through_model_snapshot() -> None:
    con = _har_forecast_connection(0.03)
    har_rv.upsert_har_forecast(
        con,
        {
            "symbol": "AAA",
            "ts_ms": 3_000,
            "asof_ts_ms": 2_500,
            "rv": 0.25,
            "trailing_vol": 0.2,
            "forecast_rv_1d": 0.25,
            "forecast_vol_1d": 0.5,
            "forecast_ann_vol": 0.5 * math.sqrt(252.0),
            "forecast_ratio": 2.5,
            "n_obs": 100,
            "n_train": 78,
            "min_history": 60,
            "source": "intraday_5m",
            "fallback": False,
            "diagnostics": {},
            "created_ts_ms": 3_000,
        },
    )

    from engine.strategy import feature_registry, model_feature_snapshots

    assert "tech.har_rv_forecast_1d" in feature_registry.registered_feature_ids()
    snap = model_feature_snapshots.build_model_feature_snapshot(
        symbol="AAA",
        ts_ms=2_000,
        feature_ids=["tech.har_rv_forecast_1d", "tech.har_rv_forecast_ratio"],
        con=con,
    )
    assert snap["feature_ids"] == ["tech.har_rv_forecast_1d", "tech.har_rv_forecast_ratio"]
    assert snap["features"]["tech.har_rv_forecast_1d"] == pytest_approx(0.03)
    assert snap["features"]["tech.har_rv_forecast_ratio"] == pytest_approx(1.5)
    assert snap["source_timestamps"]["tech"]["har_forecast_ts_ms"] == 1_000


def pytest_approx(value: float, **kwargs):
    import pytest

    return pytest.approx(value, **kwargs)
