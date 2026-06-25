from __future__ import annotations

import math

import pytest


class _Conn:
    def close(self) -> None:
        pass


def _patch_cost_path(monkeypatch: pytest.MonkeyPatch, edge_filter, calls: list[str]) -> None:
    monkeypatch.setattr(edge_filter, "PRICE_STEP_S", 60)
    monkeypatch.setattr(edge_filter, "FEES_BPS", 0.0)
    monkeypatch.setattr(edge_filter, "SLIPPAGE_BPS", 0.0)
    monkeypatch.setattr(edge_filter, "realized_vol_from_prices", lambda _con, _sym: 0.01)
    monkeypatch.setattr(edge_filter, "estimate_cost_bps", lambda **_kwargs: {"total_cost_bps": 10.0})
    monkeypatch.setattr(edge_filter, "connect", lambda: calls.append("connect") or _Conn())


def _enable_edge_filter(
    monkeypatch: pytest.MonkeyPatch,
    *,
    min_net_abs_z: str = "0.0",
    asset_classes: str = "",
) -> None:
    monkeypatch.setenv("ALERT_USE_EXEC_COST_FILTER", "1")
    monkeypatch.setenv("ALERT_MIN_NET_ABS_Z", min_net_abs_z)
    if asset_classes:
        monkeypatch.setenv("ALERT_EXEC_COST_FILTER_ASSET_CLASSES", asset_classes)
    else:
        monkeypatch.delenv("ALERT_EXEC_COST_FILTER_ASSET_CLASSES", raising=False)


def test_edge_filter_env_set_after_import_activates_and_gates(monkeypatch: pytest.MonkeyPatch) -> None:
    import engine.data.asset_map as asset_map
    import engine.strategy.edge_filter as edge_filter

    calls: list[str] = []
    _patch_cost_path(monkeypatch, edge_filter, calls)
    monkeypatch.setattr(
        asset_map,
        "asset_class_for_symbol",
        lambda symbol: {"SPY": "EQUITY", "BTC": "CRYPTO"}.get(str(symbol).upper(), "UNKNOWN"),
    )
    monkeypatch.delenv("ALERT_USE_EXEC_COST_FILTER", raising=False)
    monkeypatch.delenv("ALERT_MIN_NET_ABS_Z", raising=False)
    monkeypatch.delenv("ALERT_EXEC_COST_FILTER_ASSET_CLASSES", raising=False)

    assert edge_filter.adjust_expected_z_for_costs(symbol="SPY", horizon_s=300, expected_z=1.0) is None
    assert calls == []

    _enable_edge_filter(monkeypatch, min_net_abs_z="0.02", asset_classes="EQUITY")
    assert edge_filter.adjust_expected_z_for_costs(symbol="BTC", horizon_s=300, expected_z=1.0) is None
    assert calls == []

    vol_horizon = 0.01 * math.sqrt(5.0)
    cost_z = (10.0 / 1e4) / vol_horizon
    result = edge_filter.adjust_expected_z_for_costs(
        symbol="SPY",
        horizon_s=300,
        expected_z=cost_z + 0.01,
    )

    assert result is not None
    assert math.isnan(result["expected_z_adj"])
    assert result["cost_z"] == pytest.approx(cost_z)
    assert calls == ["connect"]


def test_edge_filter_asset_class_scope_is_strict_superset(monkeypatch: pytest.MonkeyPatch) -> None:
    import engine.data.asset_map as asset_map
    import engine.strategy.edge_filter as edge_filter

    calls: list[str] = []
    _patch_cost_path(monkeypatch, edge_filter, calls)
    _enable_edge_filter(monkeypatch)
    monkeypatch.setattr(
        asset_map,
        "asset_class_for_symbol",
        lambda symbol: {"SPY": "EQUITY", "BTC": "CRYPTO"}.get(str(symbol).upper(), "UNKNOWN"),
    )

    monkeypatch.setenv("ALERT_EXEC_COST_FILTER_ASSET_CLASSES", "EQUITY")
    assert edge_filter.adjust_expected_z_for_costs(symbol="BTC", horizon_s=300, expected_z=1.0) is None
    assert calls == []

    result = edge_filter.adjust_expected_z_for_costs(symbol="SPY", horizon_s=300, expected_z=1.0)
    assert result is not None
    assert result["expected_z_adj"] == pytest.approx(1.0 - result["cost_z"])
    assert calls == ["connect"]

    calls.clear()
    monkeypatch.delenv("ALERT_EXEC_COST_FILTER_ASSET_CLASSES", raising=False)
    result = edge_filter.adjust_expected_z_for_costs(symbol="BTC", horizon_s=300, expected_z=1.0)
    assert result is not None
    assert calls == ["connect"]


def test_edge_filter_min_net_abs_z_hard_reject_math_is_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    import engine.strategy.edge_filter as edge_filter

    calls: list[str] = []
    _patch_cost_path(monkeypatch, edge_filter, calls)
    _enable_edge_filter(monkeypatch, min_net_abs_z="0.02")

    vol_horizon = 0.01 * math.sqrt(5.0)
    cost_z = (10.0 / 1e4) / vol_horizon
    result = edge_filter.adjust_expected_z_for_costs(
        symbol="SPY",
        horizon_s=300,
        expected_z=cost_z + 0.01,
    )

    assert result is not None
    assert math.isnan(result["expected_z_adj"])
    assert result["cost_z"] == pytest.approx(cost_z)
    assert result["cost_bps"] == pytest.approx(10.0)
    assert result["vol_step"] == pytest.approx(0.01)
    assert result["vol_horizon"] == pytest.approx(vol_horizon)
