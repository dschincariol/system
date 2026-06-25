from __future__ import annotations

import importlib
import math

import pytest


def _tool():
    return importlib.import_module("tools.calibrate_edge_filter_min_net_abs_z")


def _row(symbol: str, slippage_bps: float, *, fees: float = 0.0, signal_json=None):
    return {
        "symbol": symbol,
        "ts_ms": 1_700_000_000_000,
        "slippage_bps": slippage_bps,
        "fees": fees,
        "signal_json": signal_json or {},
        "decision_json": {},
    }


def test_calibrator_recommends_percentile_from_realized_cost_z(monkeypatch: pytest.MonkeyPatch) -> None:
    calib = _tool()
    rows = [_row("SPY", float(idx)) for idx in range(1, 61)]
    monkeypatch.setattr(calib, "asset_class_for_symbol", lambda _symbol: "EQUITY")
    monkeypatch.setattr(calib, "realized_vol_from_prices", lambda _con, _symbol: 0.01)
    monkeypatch.setattr(calib, "PRICE_STEP_S", 60)

    payload = calib.calibrate(
        con=object(),
        rows=rows,
        min_fills=50,
        percentile=90.0,
        horizon_s=300,
        now_ms=1_700_000_000_000,
    )

    expected = [
        ((float(idx) / 1e4) / (0.01 * math.sqrt(5.0)))
        for idx in range(1, 61)
    ]
    assert payload["status"] == "ok"
    assert payload["n_fills"] == 60
    assert payload["n_usable"] == 60
    assert payload["recommended_min_net_abs_z"] == pytest.approx(calib._percentile(expected, 90.0))
    assert payload["cost_z_percentiles"]["p95"] == pytest.approx(calib._percentile(expected, 95.0))


def test_calibrator_returns_insufficient_data_without_fabricating(monkeypatch: pytest.MonkeyPatch) -> None:
    calib = _tool()
    rows = [_row("SPY", 1.0), _row("SPY", 2.0)]
    monkeypatch.setattr(calib, "asset_class_for_symbol", lambda _symbol: "EQUITY")
    monkeypatch.setattr(calib, "realized_vol_from_prices", lambda _con, _symbol: 0.01)

    payload = calib.calibrate(con=object(), rows=rows, min_fills=50, now_ms=1_700_000_000_000)

    assert payload["status"] == "insufficient_data"
    assert payload["n_usable"] == 2
    assert payload["recommended_min_net_abs_z"] is None


def test_calibrator_equity_scope_and_unknown_inclusion(monkeypatch: pytest.MonkeyPatch) -> None:
    calib = _tool()
    rows = [_row("SPY", 5.0), _row("XYZ", 5.0)]
    monkeypatch.setattr(calib, "asset_class_for_symbol", lambda symbol: "EQUITY" if symbol == "SPY" else "UNKNOWN")
    monkeypatch.setattr(calib, "realized_vol_from_prices", lambda _con, _symbol: 0.01)

    scoped = calib.calibrate(con=object(), rows=rows, min_fills=1, include_unknown=False, now_ms=1_700_000_000_000)
    assert scoped["status"] == "ok"
    assert scoped["n_fills"] == 1
    assert scoped["n_usable"] == 1
    assert scoped["skipped"]["out_of_scope"] == 1

    with_unknown = calib.calibrate(con=object(), rows=rows, min_fills=1, include_unknown=True, now_ms=1_700_000_000_000)
    assert with_unknown["status"] == "ok"
    assert with_unknown["n_fills"] == 2
    assert with_unknown["n_usable"] == 2
    assert with_unknown["asset_classes"]["UNKNOWN"] == 1


def test_calibrator_converts_fees_only_with_notional_context(monkeypatch: pytest.MonkeyPatch) -> None:
    calib = _tool()
    monkeypatch.setattr(calib, "asset_class_for_symbol", lambda _symbol: "EQUITY")
    monkeypatch.setattr(calib, "realized_vol_from_prices", lambda _con, _symbol: 0.01)
    monkeypatch.setattr(calib, "PRICE_STEP_S", 60)

    no_notional = calib.calibrate(
        con=object(),
        rows=[_row("SPY", 5.0, fees=2.0)],
        min_fills=1,
        now_ms=1_700_000_000_000,
    )
    assert no_notional["status"] == "insufficient_data"
    assert no_notional["n_usable"] == 0
    assert no_notional["skipped"]["fees_without_notional"] == 1

    with_notional = calib.calibrate(
        con=object(),
        rows=[_row("SPY", 5.0, fees=2.0, signal_json={"notional": 10_000.0})],
        min_fills=1,
        now_ms=1_700_000_000_000,
    )
    expected_cost_z = ((5.0 + 2.0) / 1e4) / (0.01 * math.sqrt(5.0))
    assert with_notional["status"] == "ok"
    assert with_notional["recommended_min_net_abs_z"] == pytest.approx(expected_cost_z)
