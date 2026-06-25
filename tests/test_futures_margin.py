from __future__ import annotations

import importlib

import pytest


def test_margin_engine_caps_contracts_at_min_reference_and_regulatory():
    from engine.risk.futures_margin import cap_contracts_by_margin, enforced_margin_per_contract

    assert enforced_margin_per_contract(15_000.0, 12_000.0) == pytest.approx(12_000.0)

    contracts, meta = cap_contracts_by_margin(
        5,
        capital=100_000.0,
        budget_weight=0.50,
        reference_margin=15_000.0,
        regulatory_or_broker_margin=12_000.0,
    )

    assert contracts == 4
    assert meta["enforced_margin_per_contract"] == pytest.approx(12_000.0)
    assert meta["initial_margin"] == pytest.approx(48_000.0)
    assert meta["budget"] == pytest.approx(50_000.0)
    assert meta["initial_margin"] <= meta["budget"]
    assert meta["clamped"] is True


def test_margin_engine_converts_non_usd_before_capping():
    from engine.risk.futures_margin import cap_contracts_by_margin, contract_notional, required_margin

    fx_rates = {"EURUSD": 1.25}
    margin = required_margin(
        2,
        reference_margin=10_000.0,
        regulatory_or_broker_margin=8_000.0,
        price_ccy="EUR",
        account_ccy="USD",
        fx_rates=fx_rates,
    )

    assert margin["fx_rate"] == pytest.approx(1.25)
    assert margin["initial_margin"] == pytest.approx(20_000.0)

    contracts, meta = cap_contracts_by_margin(
        4,
        capital=100_000.0,
        budget_weight=0.30,
        reference_margin=10_000.0,
        regulatory_or_broker_margin=8_000.0,
        price_ccy="EUR",
        account_ccy="USD",
        fx_rates=fx_rates,
    )

    assert contracts == 3
    assert meta["initial_margin"] == pytest.approx(30_000.0)
    assert contract_notional(3, 100.0, 10.0, price_ccy="EUR", account_ccy="USD", fx_rates=fx_rates) == pytest.approx(3_750.0)


def test_runtime_margin_stage_floors_and_caps_futures_contracts(monkeypatch):
    monkeypatch.setenv("PORTFOLIO_RISK_USE_FUTURES_MARGIN_CAPS", "1")
    monkeypatch.setenv("PORTFOLIO_RISK_ASSET_CLASS_BUDGETS_JSON", '{"FUTURES":0.50}')
    monkeypatch.setenv("FUTURES_MARGIN_REQUIREMENTS_JSON", '{"ES":12000}')

    import engine.risk.portfolio_risk_engine as portfolio_risk_engine

    engine = importlib.reload(portfolio_risk_engine)
    monkeypatch.setattr(engine, "_equity_reference", lambda _con: (100_000.0, "unit_test"))
    monkeypatch.setattr(engine, "_last_price", lambda _con, symbol: 5_000.0 if str(symbol) == "ES.c.0" else None)

    desired = {"ES.c.0": {"symbol": "ES.c.0", "weight": 12.0, "side": "LONG", "reason": {}}}
    info = {}

    out = engine._apply_futures_margin_caps(None, desired, info)

    futures = out["ES.c.0"]["futures"]
    assert futures["desired_contracts"] == 4
    assert futures["contracts"] == 4
    assert futures["margin"]["initial_margin"] == pytest.approx(48_000.0)
    assert futures["margin"]["budget"] == pytest.approx(50_000.0)
    assert out["ES.c.0"]["weight"] == pytest.approx(10.0)
    assert "futures_margin_hard_blocks" not in info
