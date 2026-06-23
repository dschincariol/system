from __future__ import annotations

import importlib
import json
import uuid

import pytest


class _DrawdownOK:
    ok = True
    drawdown = 0.0
    reason_code = "ok"

    def to_dict(self):
        return {"ok": True, "drawdown": 0.0, "reason_code": "ok"}


def _reload_engine(monkeypatch):
    env = {
        "PORTFOLIO_RISK_USE_MONTE_CARLO": "0",
        "PORTFOLIO_RISK_USE_ASSET_CLASS_BUDGETS": "0",
        "PORTFOLIO_RISK_USE_FX_LEVERAGE_CAPS": "1",
        "PORTFOLIO_RISK_USE_STRATEGY_BUDGETS": "0",
        "PORTFOLIO_RISK_USE_ALPHA_DECAY_THROTTLE": "0",
        "PORTFOLIO_RISK_USE_VOL_CAPS": "0",
        "PORTFOLIO_RISK_USE_CORR_CLUSTERS": "0",
        "PORTFOLIO_RISK_MAX_GROSS": "100",
        "PORTFOLIO_RISK_MAX_NET": "100",
        "PORTFOLIO_RISK_MAX_SYMBOL_GROSS": "100",
        "PORTFOLIO_RISK_SYMBOL_CAP_MAX_W": "100",
        "FX_LEVERAGE_JURISDICTION": "EU",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    import engine.risk.portfolio_risk_engine as portfolio_risk_engine

    return importlib.reload(portfolio_risk_engine)


def _fx_inst(symbol="EURUSD"):
    return {
        "asset_class": "FX",
        "base_ccy": "EUR",
        "quote_ccy": "USD",
        "pip_size": 0.0001,
        "contract_size": 100_000.0,
        "leverage_cap": 50.0,
        "symbol": symbol,
    }


def _patch_fast_path(monkeypatch, engine):
    captured_state = {}
    monkeypatch.setattr(engine, "evaluate_current_drawdown", lambda _con: _DrawdownOK())
    monkeypatch.setattr(
        engine,
        "_load_live_positions",
        lambda _con: ({}, {"source": "unit_test", "equity_ref": 100_000.0, "equity_ref_source": "unit_test"}),
    )
    monkeypatch.setattr(engine, "_equity_reference", lambda _con: (100_000.0, "unit_test"))
    monkeypatch.setattr(engine, "_fx_instrument", lambda _con, symbol: _fx_inst(str(symbol)))
    monkeypatch.setattr(engine, "set_state", lambda key, value: captured_state.__setitem__(key, value))
    monkeypatch.setattr(engine, "record_risk_block", lambda **_kwargs: None)
    monkeypatch.setattr(engine, "_persist_snapshot", lambda *_args, **_kwargs: None)
    return captured_state


@pytest.mark.safety_critical
def test_fx_leverage_stage_clamps_to_effective_cap_with_correct_math(monkeypatch):
    canary = f"canary-{uuid.uuid4()}"
    engine = _reload_engine(monkeypatch)
    captured_state = _patch_fast_path(monkeypatch, engine)
    monkeypatch.setattr(engine, "_last_price", lambda _con, _symbol: 1.08)
    desired = {"EURUSD": {"symbol": "EURUSD", "weight": 35.0, "side": "LONG", "reason": {}}}

    out, info = engine.apply_portfolio_risk_engine(None, desired, {}, now_ms=1_700_000_000_000)

    fx = out["EURUSD"]["fx"]
    assert out["EURUSD"]["weight"] == pytest.approx(30.0)
    assert fx["effective_leverage"] == pytest.approx(30.0)
    assert fx["effective_leverage_cap"] == pytest.approx(30.0)
    assert fx["base_notional"] == pytest.approx(3_000_000.0)
    assert fx["quote_notional"] == pytest.approx(3_240_000.0)
    assert fx["lots"] == pytest.approx(30.0)
    assert info["blocked"] is False
    assert captured_state["portfolio_risk_block"] == "0"
    assert canary not in json.dumps(info, sort_keys=True)


@pytest.mark.safety_critical
def test_fx_leverage_missing_pair_rate_fails_closed(monkeypatch):
    engine = _reload_engine(monkeypatch)
    captured_state = _patch_fast_path(monkeypatch, engine)
    monkeypatch.setattr(engine, "_last_price", lambda _con, _symbol: None)
    desired = {"EURUSD": {"symbol": "EURUSD", "weight": 0.10, "side": "LONG", "reason": {}}}

    out, info = engine.apply_portfolio_risk_engine(None, desired, {}, now_ms=1_700_000_000_000)

    assert out["EURUSD"]["weight"] == pytest.approx(0.10)
    assert info["blocked"] is True
    assert info["block_reason"]["type"] == "fx_leverage_hard_block"
    assert info["fx_leverage_hard_blocks"][0]["reason"] == "fx_pair_rate_unavailable"
    assert captured_state["portfolio_risk_block"] == "1"
