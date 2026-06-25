from __future__ import annotations

import importlib

import pytest


def _reload_engine(monkeypatch, **env):
    defaults = {
        "PORTFOLIO_RISK_USE_MONTE_CARLO": "0",
        "PORTFOLIO_RISK_USE_ASSET_CLASS_BUDGETS": "1",
        "PORTFOLIO_RISK_USE_FX_LEVERAGE_CAPS": "0",
        "PORTFOLIO_RISK_USE_FUTURES_MARGIN_CAPS": "0",
        "PORTFOLIO_RISK_USE_STRATEGY_BUDGETS": "0",
        "PORTFOLIO_RISK_USE_ALPHA_DECAY_THROTTLE": "0",
        "PORTFOLIO_RISK_USE_VOL_CAPS": "0",
        "PORTFOLIO_RISK_USE_CORR_CLUSTERS": "0",
        "PORTFOLIO_RISK_MAX_GROSS": "100",
        "PORTFOLIO_RISK_MAX_NET": "100",
    }
    defaults.update({key: str(value) for key, value in env.items()})
    for key, value in defaults.items():
        monkeypatch.setenv(key, value)

    import engine.risk.portfolio_risk_engine as portfolio_risk_engine

    return importlib.reload(portfolio_risk_engine)


def _reload_gate(monkeypatch, **env):
    defaults = {
        "PORTFOLIO_USE_RISK_GATE": "1",
        "PORTFOLIO_USE_SLEEVE_CAPS": "1",
        "PORTFOLIO_SLEEVE_DEFAULT_MAX_GROSS": "100",
        "PORTFOLIO_SLEEVE_DEFAULT_MAX_NET": "100",
        "PORTFOLIO_MAX_NET_EXPOSURE": "100",
        "PORTFOLIO_MAX_TURNOVER": "100",
        "PORTFOLIO_GROSS_CAP": "100",
    }
    defaults.update({key: str(value) for key, value in env.items()})
    for key, value in defaults.items():
        monkeypatch.setenv(key, value)

    import engine.strategy.portfolio_risk_gate as portfolio_risk_gate

    return importlib.reload(portfolio_risk_gate)


def test_futures_budget_and_exposure_sums_use_multiplier(monkeypatch):
    engine = _reload_engine(monkeypatch)
    desired = {
        "ES.c.0": {"symbol": "ES.c.0", "weight": 0.02, "side": "LONG", "reason": {}},
        "SPY": {"symbol": "SPY", "weight": 0.02, "side": "LONG", "reason": {}},
    }

    snapshot = engine._exposure_snapshot(desired, None)

    assert "FUTURES" in engine._DEFAULT_ASSET_CLASS_BUDGETS
    assert snapshot["by_symbol"]["ES.c.0"]["gross"] == pytest.approx(1.0)
    assert snapshot["by_symbol"]["SPY"]["gross"] == pytest.approx(0.02)
    assert snapshot["by_asset_class"]["FUTURES"]["gross"] == pytest.approx(1.0)
    assert snapshot["by_asset_class"]["EQUITY"]["gross"] == pytest.approx(0.02)


def test_futures_sleeve_gross_and_net_use_multiplier(monkeypatch):
    gate = _reload_gate(monkeypatch)
    desired = {
        "ES.c.0": {"symbol": "ES.c.0", "weight": 0.02, "side": "LONG", "reason": {}},
        "SPY": {"symbol": "SPY", "weight": 0.02, "side": "LONG", "reason": {}},
    }

    assert gate._sleeve_gross(desired, "FUTURES") == pytest.approx(1.0)
    assert gate._sleeve_net(desired, "FUTURES") == pytest.approx(1.0)
    assert gate._sleeve_gross(desired, "EQUITY") == pytest.approx(0.02)
    assert gate._sleeve_net(desired, "EQUITY") == pytest.approx(0.02)


def test_weight_to_contracts_floors_without_oversizing():
    from engine.risk.futures_margin import weight_to_contracts

    assert weight_to_contracts(0.50, 100_000.0, 50.0, 5_000.0) == 0
    assert weight_to_contracts(2.60, 100_000.0, 50.0, 5_000.0) == 1
    assert weight_to_contracts(-5.10, 100_000.0, 50.0, 5_000.0) == -2
    assert weight_to_contracts(0.10, 100_000.0, 1.0, 100.0) == 100


def test_non_futures_exposure_sizing_unchanged(monkeypatch):
    engine = _reload_engine(monkeypatch)
    gate = _reload_gate(monkeypatch)
    desired = {
        "SPY": {"symbol": "SPY", "weight": 0.20, "side": "LONG", "reason": {}},
        "BTC": {"symbol": "BTC", "weight": 0.10, "side": "LONG", "reason": {}},
    }

    snapshot = engine._exposure_snapshot(desired, None)

    assert snapshot["gross"] == pytest.approx(0.30)
    assert snapshot["net"] == pytest.approx(0.30)
    assert snapshot["by_symbol"]["SPY"]["gross"] == pytest.approx(0.20)
    assert snapshot["by_symbol"]["BTC"]["gross"] == pytest.approx(0.10)
    assert gate._gross(desired) == pytest.approx(0.30)
    assert gate._net(desired) == pytest.approx(0.30)
