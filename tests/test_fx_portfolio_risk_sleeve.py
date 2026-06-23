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


def _reload_engine(monkeypatch, **env):
    defaults = {
        "PORTFOLIO_RISK_USE_MONTE_CARLO": "0",
        "PORTFOLIO_RISK_USE_FX_LEVERAGE_CAPS": "0",
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


def _fx_inst(symbol):
    base = {"EURUSD": "EUR", "GBPUSD": "GBP"}.get(symbol, "EUR")
    return {
        "asset_class": "FX",
        "base_ccy": base,
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
    monkeypatch.setattr(engine, "set_state", lambda key, value: captured_state.__setitem__(key, value))
    monkeypatch.setattr(engine, "record_risk_block", lambda **_kwargs: None)
    monkeypatch.setattr(engine, "_persist_snapshot", lambda *_args, **_kwargs: None)
    return captured_state


@pytest.mark.safety_critical
def test_fx_asset_class_sleeve_binds_fx_exposure(monkeypatch):
    canary = f"canary-{uuid.uuid4()}"
    engine = _reload_engine(monkeypatch, PORTFOLIO_RISK_USE_ASSET_CLASS_BUDGETS="1")
    captured_state = _patch_fast_path(monkeypatch, engine)
    monkeypatch.setattr(engine, "_fx_instrument", lambda _con, symbol: _fx_inst(str(symbol)))

    desired = {
        "EURUSD": {"symbol": "EURUSD", "weight": 0.40, "side": "LONG", "reason": {}},
        "GBPUSD": {"symbol": "GBPUSD", "weight": 0.40, "side": "LONG", "reason": {}},
    }

    out, info = engine.apply_portfolio_risk_engine(None, desired, {}, now_ms=1_700_000_000_000)

    assert info["asset_class_gross_post"]["FX"] <= 0.50 + 1e-12
    assert info["target_exposure_post"]["by_asset_class"]["FX"]["gross"] <= 0.50 + 1e-12
    assert out["EURUSD"]["weight"] == pytest.approx(0.25)
    assert out["GBPUSD"]["weight"] == pytest.approx(0.25)
    assert captured_state["portfolio_risk_block"] == "0"
    assert canary not in json.dumps(info, sort_keys=True)
