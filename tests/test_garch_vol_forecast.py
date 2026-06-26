from __future__ import annotations

import math
import sqlite3
from typing import Any

import pytest

from engine.strategy import garch_vol, har_rv


def _market_tables(con: sqlite3.Connection) -> None:
    con.execute("CREATE TABLE price_bars (ts_ms INTEGER, symbol TEXT, tf_s INTEGER, o REAL, h REAL, l REAL, c REAL)")
    con.execute("CREATE TABLE prices (ts_ms INTEGER, symbol TEXT, price REAL, px REAL)")


def _seed_clustered_history(con: sqlite3.Connection, symbol: str = "AAA", n: int = 180) -> None:
    _market_tables(con)
    price = 100.0
    bars: list[tuple[int, str, int, float, float, float, float]] = []
    prices: list[tuple[int, str, float, float]] = []
    for idx in range(n):
        ts_ms = (idx + 1) * har_rv.MS_PER_DAY
        sigma = 0.006 if (idx // 25) % 2 == 0 else 0.026
        sign = -1.0 if idx % 2 == 0 else 1.0
        ret = sign * sigma * (0.35 + 0.65 * abs(math.sin(idx / 3.0)))
        open_px = price
        close_px = float(open_px * math.exp(ret))
        high_px = max(open_px, close_px) * (1.0 + sigma * 0.25)
        low_px = min(open_px, close_px) * (1.0 - sigma * 0.25)
        bars.append((ts_ms, symbol, 86_400, open_px, high_px, low_px, close_px))
        prices.append((ts_ms, symbol, close_px, close_px))
        price = close_px
    con.executemany("INSERT INTO price_bars(ts_ms, symbol, tf_s, o, h, l, c) VALUES (?, ?, ?, ?, ?, ?, ?)", bars)
    con.executemany("INSERT INTO prices(ts_ms, symbol, price, px) VALUES (?, ?, ?, ?)", prices)


class _FakeForecast:
    variance = [[4.0]]


class _FakeFit:
    convergence_flag = 0
    loglikelihood = -123.0
    aic = 252.0
    bic = 260.0

    def forecast(self, *, horizon: int, reindex: bool) -> _FakeForecast:
        assert horizon == 1
        assert reindex is False
        return _FakeForecast()


class _FakeModel:
    def fit(self, *, disp: str, show_warning: bool) -> _FakeFit:
        assert disp == "off"
        assert show_warning is False
        return _FakeFit()


def _fake_arch_model(values: Any, **kwargs: Any) -> _FakeModel:
    assert len(values) >= 60
    assert kwargs["vol"] == "GARCH"
    assert kwargs["p"] == 1
    assert kwargs["q"] == 1
    assert kwargs["o"] == 0
    assert kwargs["dist"] == "normal"
    return _FakeModel()


def _manual_garch_payload(symbol: str, ts_ms: int, vol: float, *, model_type: str = "garch") -> dict[str, Any]:
    return {
        "symbol": symbol,
        "ts_ms": int(ts_ms),
        "asof_ts_ms": int(ts_ms) - 1,
        "model_type": model_type,
        "distribution": "normal",
        "horizon_days": 1,
        "return_source": "unit",
        "trailing_vol": 0.02,
        "forecast_rv_1d": float(vol) * float(vol),
        "forecast_vol_1d": float(vol),
        "forecast_ann_vol": float(vol) * math.sqrt(252.0),
        "forecast_ratio": float(vol) / 0.02,
        "n_obs": 180,
        "n_train": 180,
        "min_history": 120,
        "converged": True,
        "convergence_status": "converged",
        "loglikelihood": -1.0,
        "aic": 2.0,
        "bic": 3.0,
        "fallback": False,
        "fallback_reason": None,
        "diagnostics": {"model": model_type, "distribution": "normal", "n_obs": 180},
        "created_ts_ms": int(ts_ms),
    }


def _manual_har_payload(symbol: str, ts_ms: int, vol: float) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "ts_ms": int(ts_ms),
        "asof_ts_ms": int(ts_ms) - 1,
        "rv": float(vol) * float(vol),
        "trailing_vol": 0.02,
        "forecast_rv_1d": float(vol) * float(vol),
        "forecast_vol_1d": float(vol),
        "forecast_ann_vol": float(vol) * math.sqrt(252.0),
        "forecast_ratio": float(vol) / 0.02,
        "n_obs": 180,
        "n_train": 158,
        "min_history": 60,
        "source": "unit",
        "fallback": False,
        "diagnostics": {"model": "har_rv_ols"},
        "created_ts_ms": int(ts_ms),
    }


def test_garch_forecast_persists_fake_arch_on_synthetic_clustered_data(monkeypatch: pytest.MonkeyPatch) -> None:
    con = sqlite3.connect(":memory:")
    _seed_clustered_history(con)
    monkeypatch.setattr(garch_vol, "_load_arch_model", lambda: (_fake_arch_model, None))

    payload = garch_vol.forecast_garch_for_symbol(con, "AAA", ts_ms=200 * har_rv.MS_PER_DAY, min_history=60, use_arch=True)
    assert payload["fallback"] is False
    assert payload["model_type"] == "garch"
    assert payload["distribution"] == "normal"
    assert payload["converged"] is True
    assert payload["forecast_vol_1d"] == pytest.approx(0.02)
    assert payload["forecast_rv_1d"] == pytest.approx(payload["forecast_vol_1d"] ** 2)
    assert payload["forecast_ann_vol"] == pytest.approx(0.02 * math.sqrt(252.0))
    assert payload["loglikelihood"] == pytest.approx(-123.0)
    assert payload["aic"] == pytest.approx(252.0)
    assert payload["bic"] == pytest.approx(260.0)

    garch_vol.upsert_garch_forecast(con, payload)
    row = garch_vol.latest_garch_forecast(con, "AAA", ts_ms=201 * har_rv.MS_PER_DAY)

    assert row is not None
    assert row["forecast_vol_1d"] == pytest.approx(0.02)
    assert row["diagnostics"]["model"] == "garch"
    assert row["diagnostics"]["forecast_ts_ms"] == 200 * har_rv.MS_PER_DAY


def test_garch_dependency_missing_falls_back_and_persists_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    con = sqlite3.connect(":memory:")
    _seed_clustered_history(con)
    monkeypatch.setattr(garch_vol, "_load_arch_model", lambda: (None, "No module named arch"))

    payload = garch_vol.forecast_garch_for_symbol(con, "AAA", ts_ms=200 * har_rv.MS_PER_DAY, min_history=60, use_arch=True)

    assert payload["fallback"] is True
    assert payload["fallback_reason"] == "arch_dependency_unavailable"
    assert payload["convergence_status"] == "dependency_unavailable"
    assert payload["forecast_vol_1d"] > 0.0
    garch_vol.upsert_garch_forecast(con, payload)
    row = garch_vol.latest_garch_forecast(con, "AAA", ts_ms=201 * har_rv.MS_PER_DAY)
    assert row is not None
    assert row["fallback"] is True
    assert row["fallback_reason"] == "arch_dependency_unavailable"


def test_garch_convergence_failure_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    con = sqlite3.connect(":memory:")
    _seed_clustered_history(con)

    class FailedFit(_FakeFit):
        convergence_flag = 4

    class FailedModel:
        def fit(self, *, disp: str, show_warning: bool) -> FailedFit:
            return FailedFit()

    monkeypatch.setattr(garch_vol, "_load_arch_model", lambda: (lambda _values, **_kwargs: FailedModel(), None))

    payload = garch_vol.forecast_garch_for_symbol(con, "AAA", ts_ms=200 * har_rv.MS_PER_DAY, min_history=60, use_arch=True)

    assert payload["fallback"] is True
    assert payload["fallback_reason"] == "garch_convergence_failed"
    assert payload["convergence_status"] == "convergence_failed"
    assert payload["forecast_rv_1d"] == pytest.approx(payload["forecast_vol_1d"] ** 2)


def test_asymmetric_garch_model_types_are_supported() -> None:
    egarch = garch_vol._arch_model_kwargs("egarch", "t")
    gjr = garch_vol._arch_model_kwargs("gjr_garch", "normal")

    assert egarch["vol"] == "EGARCH"
    assert egarch["o"] == 1
    assert egarch["p"] == 1
    assert egarch["q"] == 1
    assert egarch["dist"] == "t"
    assert gjr["vol"] == "GARCH"
    assert gjr["o"] == 1
    assert gjr["p"] == 1
    assert gjr["q"] == 1


def test_resolve_blend_combines_component_variances_and_reports_diagnostics(monkeypatch: pytest.MonkeyPatch) -> None:
    con = sqlite3.connect(":memory:")
    har_rv.ensure_har_rv_schema(con)
    garch_vol.ensure_garch_vol_schema(con)
    har_rv.upsert_har_forecast(con, _manual_har_payload("AAA", 1_000, 0.01))
    garch_vol.upsert_garch_forecast(con, _manual_garch_payload("AAA", 1_000, 0.03))
    monkeypatch.setattr(har_rv, "_trailing_fallback_vol", lambda _con, _symbol, lookback=240: 0.02)

    resolved = har_rv.resolve_vol_forecast(
        con,
        "AAA",
        ts_ms=2_000,
        source="blend",
        blend_weights={"trailing": 0.50, "har_rv": 0.25, "garch": 0.25},
    )

    expected_rv = (0.50 * 0.02 * 0.02) + (0.25 * 0.01 * 0.01) + (0.25 * 0.03 * 0.03)
    assert resolved["resolved_source"] == "blend"
    assert resolved["forecast_rv_1d"] == pytest.approx(expected_rv)
    assert resolved["forecast_vol_1d"] == pytest.approx(math.sqrt(expected_rv))
    assert resolved["diagnostics"]["blend_space"] == "variance"
    assert resolved["diagnostics"]["components"]["garch"]["forecast_vol_1d"] == pytest.approx(0.03)
    assert resolved["diagnostics"]["normalized_weights"]["har"] == pytest.approx(0.25)


def test_garch_resolver_uses_point_in_time_timestamp() -> None:
    con = sqlite3.connect(":memory:")
    garch_vol.ensure_garch_vol_schema(con)
    garch_vol.upsert_garch_forecast(con, _manual_garch_payload("AAA", 1_000, 0.02))
    garch_vol.upsert_garch_forecast(con, _manual_garch_payload("AAA", 5_000, 0.05))

    early = har_rv.resolve_vol_forecast(con, "AAA", ts_ms=3_000, source="garch")
    late = har_rv.resolve_vol_forecast(con, "AAA", ts_ms=6_000, source="garch")

    assert early["ts_ms"] == 1_000
    assert early["forecast_vol_1d"] == pytest.approx(0.02)
    assert late["ts_ms"] == 5_000
    assert late["forecast_vol_1d"] == pytest.approx(0.05)


def test_missing_garch_table_or_dependency_does_not_crash_risk_read(monkeypatch: pytest.MonkeyPatch) -> None:
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE prices (ts_ms INTEGER, symbol TEXT, price REAL)")
    rows = [(idx, "AAA", 100.0 + idx * 0.5) for idx in range(1, 20)]
    con.executemany("INSERT INTO prices(ts_ms, symbol, price) VALUES (?, ?, ?)", rows)
    monkeypatch.setattr(garch_vol, "_load_arch_model", lambda: (None, "missing"))

    resolved = har_rv.resolve_vol_forecast(con, "AAA", ts_ms=20, source="garch", trailing_lookback=20)

    assert resolved["resolved_source"] == "trailing"
    assert resolved["diagnostics"]["fallback_reason"] == "garch_forecast_unavailable"
    assert resolved["forecast_vol_1d"] is not None
    assert resolved["forecast_rv_1d"] == pytest.approx(resolved["forecast_vol_1d"] ** 2)


def test_monte_carlo_build_inputs_consumes_garch_source(monkeypatch: pytest.MonkeyPatch) -> None:
    con = sqlite3.connect(":memory:")
    _seed_clustered_history(con)
    garch_vol.ensure_garch_vol_schema(con)
    forecast_ts_ms = 179 * har_rv.MS_PER_DAY
    garch_vol.upsert_garch_forecast(con, _manual_garch_payload("AAA", forecast_ts_ms, 0.04))

    from engine.risk import monte_carlo_risk_engine as mc

    monkeypatch.setattr(mc, "VOL_FORECAST_SOURCE", "garch")
    monkeypatch.setattr(mc, "MC_LOOKBACK", 90)
    monkeypatch.setattr(mc, "_now_ms", lambda: 180 * har_rv.MS_PER_DAY)
    symbols, weights, vols, drifts, corr, vol_meta, input_meta = mc._build_inputs(con, {"AAA": {"weight": 1.0}})

    assert symbols == ["AAA"]
    assert weights == [1.0]
    assert vols[0] == pytest.approx(0.04)
    assert len(drifts) == 1
    assert corr == [[1.0]]
    assert input_meta["covariance"]["used"] is True
    assert vol_meta["AAA"]["source"] == "garch"
    assert vol_meta["AAA"]["forecast_ts_ms"] == forecast_ts_ms
