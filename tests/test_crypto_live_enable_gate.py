from __future__ import annotations

import importlib

import pytest


def _reload_gate(monkeypatch, **env):
    defaults = {
        "PORTFOLIO_USE_RISK_GATE": "1",
        "EXEC_MAX_ABS_WEIGHT": "10",
        "EXEC_MAX_ABS_DELTA_WEIGHT": "10",
        "EXEC_MAX_ORDERS_PER_PASS": "50",
        "EXEC_PORTFOLIO_TOTAL_EXPOSURE_CAP": "100",
        "EXEC_PORTFOLIO_DIRECTION_CONCENTRATION_CAP": "100",
        "CRYPTO_NOTIONAL_CAP_USD": "10000",
    }
    defaults.update({key: str(value) for key, value in env.items()})
    for key, value in defaults.items():
        monkeypatch.setenv(key, value)

    import engine.strategy.portfolio_risk_gate as portfolio_risk_gate

    gate = importlib.reload(portfolio_risk_gate)
    monkeypatch.setattr(gate, "table_exists", lambda _con, _table: False)

    import engine.runtime.risk_state as risk_state

    monkeypatch.setattr(risk_state, "get_state", lambda _key, default="": default)
    return gate


@pytest.mark.safety_critical
def test_crypto_live_order_blocked_when_crypto_live_disabled(monkeypatch):
    gate = _reload_gate(monkeypatch, CRYPTO_LIVE_TRADING_ENABLED="0")
    orders = [{"symbol": "BTCUSD", "to_side": "LONG", "to_weight": 0.01}]

    routed, info = gate.apply_execution_risk_governor(
        None,
        orders,
        broker="ibkr",
        mode="live",
        equity_usd=100_000.0,
    )

    assert routed == []
    assert info["ok"] is False
    assert info["status"] == "blocked_crypto_live_trading_disabled"
    assert info["crypto_live_gate"]["env"]["CRYPTO_LIVE_TRADING_ENABLED"] == "0"


@pytest.mark.safety_critical
def test_crypto_live_order_blocked_when_notional_exceeds_cap(monkeypatch):
    gate = _reload_gate(
        monkeypatch,
        CRYPTO_LIVE_TRADING_ENABLED="1",
        CRYPTO_NOTIONAL_CAP_USD="1000",
    )
    orders = [{"symbol": "BTC", "to_side": "LONG", "to_weight": 0.02}]

    routed, info = gate.apply_execution_risk_governor(
        None,
        orders,
        broker="ibkr",
        mode="live",
        equity_usd=100_000.0,
    )

    assert routed == []
    assert info["ok"] is False
    assert info["status"] == "blocked_crypto_notional_cap"
    assert info["crypto_live_gate"]["breaches"][0]["reason"] == "crypto_notional_cap_exceeded"


@pytest.mark.safety_critical
def test_crypto_live_order_batch_notional_cap_is_aggregate(monkeypatch):
    gate = _reload_gate(
        monkeypatch,
        CRYPTO_LIVE_TRADING_ENABLED="1",
        CRYPTO_NOTIONAL_CAP_USD="2500",
    )
    orders = [
        {"symbol": "BTC", "to_side": "LONG", "to_weight": 0.015},
        {"symbol": "ETH", "to_side": "LONG", "to_weight": 0.015},
    ]

    routed, info = gate.apply_execution_risk_governor(
        None,
        orders,
        broker="ibkr",
        mode="live",
        equity_usd=100_000.0,
    )

    assert routed == []
    assert info["ok"] is False
    assert info["status"] == "blocked_crypto_notional_cap"
    assert info["crypto_live_gate"]["breaches"][0]["symbol"] == "BATCH"
    assert info["crypto_live_gate"]["total_crypto_notional_usd"] == pytest.approx(3_000.0)


@pytest.mark.safety_critical
def test_non_crypto_live_orders_unaffected_by_crypto_gate(monkeypatch):
    gate = _reload_gate(monkeypatch, CRYPTO_LIVE_TRADING_ENABLED="0")
    orders = [
        {"symbol": "SPY", "to_side": "LONG", "to_weight": 0.01},
        {"symbol": "EURUSD", "to_side": "LONG", "to_weight": 0.01},
    ]

    routed, info = gate.apply_execution_risk_governor(
        None,
        orders,
        broker="ibkr",
        mode="live",
        equity_usd=100_000.0,
    )

    assert info["ok"] is True
    assert info["crypto_live_gate"]["status"] == "no_crypto_orders"
    assert [order["symbol"] for order in routed] == ["SPY", "EURUSD"]


@pytest.mark.safety_critical
def test_crypto_gate_does_not_block_paper_mode(monkeypatch):
    gate = _reload_gate(monkeypatch, CRYPTO_LIVE_TRADING_ENABLED="0")
    orders = [{"symbol": "BTCUSD", "to_side": "LONG", "to_weight": 0.01}]

    routed, info = gate.apply_execution_risk_governor(
        None,
        orders,
        broker="sim",
        mode="paper",
        equity_usd=100_000.0,
    )

    assert info["ok"] is True
    assert info["crypto_live_gate"]["status"] == "not_live_mode"
    assert [order["symbol"] for order in routed] == ["BTCUSD"]
