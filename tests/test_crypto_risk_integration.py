from __future__ import annotations

import importlib
import json

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
        "PORTFOLIO_RISK_USE_ASSET_CLASS_BUDGETS": "0",
        "PORTFOLIO_RISK_USE_FX_LEVERAGE_CAPS": "0",
        "PORTFOLIO_RISK_USE_CRYPTO_LEVERAGE_CAPS": "1",
        "PORTFOLIO_RISK_USE_FUTURES_MARGIN_CAPS": "0",
        "PORTFOLIO_RISK_USE_OPTIONS_GREEK_LIMITS": "0",
        "PORTFOLIO_RISK_USE_STRATEGY_BUDGETS": "0",
        "PORTFOLIO_RISK_USE_ALPHA_DECAY_THROTTLE": "0",
        "PORTFOLIO_RISK_USE_VOL_CAPS": "0",
        "PORTFOLIO_RISK_USE_CORR_CLUSTERS": "0",
        "PORTFOLIO_RISK_MAX_GROSS": "100",
        "PORTFOLIO_RISK_MAX_NET": "100",
        "PORTFOLIO_RISK_MAX_SYMBOL_GROSS": "100",
        "PORTFOLIO_RISK_SYMBOL_CAP_MAX_W": "100",
        "CRYPTO_VOL_TARGET": "0",
    }
    defaults.update({key: str(value) for key, value in env.items()})
    for key, value in defaults.items():
        monkeypatch.setenv(key, value)

    import engine.risk.portfolio_risk_engine as portfolio_risk_engine

    return importlib.reload(portfolio_risk_engine)


def _patch_fast_path(monkeypatch, engine):
    captured_state = {}
    monkeypatch.setattr(engine, "evaluate_current_drawdown", lambda _con: _DrawdownOK())
    monkeypatch.setattr(
        engine,
        "_load_live_positions",
        lambda _con: ({}, {"source": "unit_test", "equity_ref": 100_000.0, "equity_ref_source": "unit_test"}),
    )
    monkeypatch.setattr(engine, "_equity_reference", lambda _con: (100_000.0, "unit_test"))
    monkeypatch.setattr(engine, "_symbol_vol_input", lambda *_args, **_kwargs: {"vol": None, "source": "unit_test"})
    monkeypatch.setattr(engine, "_last_price", lambda _con, symbol: 50_000.0 if str(symbol).upper().startswith("BTC") else 1.08)
    monkeypatch.setattr(engine, "set_state", lambda key, value: captured_state.__setitem__(key, value))
    monkeypatch.setattr(engine, "record_risk_block", lambda **_kwargs: None)
    monkeypatch.setattr(engine, "_persist_snapshot", lambda *_args, **_kwargs: None)
    return captured_state


@pytest.mark.safety_critical
def test_crypto_leverage_cap_applies_only_to_crypto(monkeypatch):
    engine = _reload_engine(monkeypatch, CRYPTO_MAX_LEVERAGE="0.20")
    captured_state = _patch_fast_path(monkeypatch, engine)
    desired = {
        "BTCUSD": {"symbol": "BTCUSD", "weight": 0.50, "side": "LONG", "reason": {}},
        "SPY": {"symbol": "SPY", "weight": 0.50, "side": "LONG", "reason": {}},
    }

    out, info = engine.apply_portfolio_risk_engine(None, desired, {}, now_ms=1_700_000_000_000)

    assert out["BTCUSD"]["weight"] == pytest.approx(0.20)
    assert out["BTCUSD"]["crypto"]["fractional"] is True
    assert out["BTCUSD"]["reason"]["crypto"]["sizing"]["type"] == "crypto_leverage_cap"
    assert out["SPY"]["weight"] == pytest.approx(0.50)
    assert info["crypto_leverage_adjustments"]["BTCUSD"]["crypto"]["effective_leverage_cap"] == pytest.approx(0.20)
    assert info["blocked"] is False
    assert captured_state["portfolio_risk_block"] == "0"


@pytest.mark.safety_critical
def test_equity_and_fx_outputs_are_identical_with_crypto_caps_enabled(monkeypatch):
    desired = {
        "SPY": {"symbol": "SPY", "weight": 0.25, "side": "LONG", "reason": {"tag": "equity"}},
        "EURUSD": {"symbol": "EURUSD", "weight": 0.10, "side": "LONG", "reason": {"tag": "fx"}},
    }

    engine_off = _reload_engine(monkeypatch, PORTFOLIO_RISK_USE_CRYPTO_LEVERAGE_CAPS="0")
    _patch_fast_path(monkeypatch, engine_off)
    out_off, _info_off = engine_off.apply_portfolio_risk_engine(None, desired, {}, now_ms=1_700_000_000_000)

    engine_on = _reload_engine(monkeypatch, PORTFOLIO_RISK_USE_CRYPTO_LEVERAGE_CAPS="1")
    _patch_fast_path(monkeypatch, engine_on)
    out_on, _info_on = engine_on.apply_portfolio_risk_engine(None, desired, {}, now_ms=1_700_000_000_000)

    assert json.dumps(out_on, sort_keys=True, separators=(",", ":")) == json.dumps(
        out_off,
        sort_keys=True,
        separators=(",", ":"),
    )


@pytest.mark.safety_critical
def test_crypto_asset_class_budget_remains_035(monkeypatch):
    engine = _reload_engine(
        monkeypatch,
        PORTFOLIO_RISK_USE_ASSET_CLASS_BUDGETS="1",
        CRYPTO_MAX_LEVERAGE="5.0",
    )
    _patch_fast_path(monkeypatch, engine)
    desired = {"BTC": {"symbol": "BTC", "weight": 0.80, "side": "LONG", "reason": {}}}

    out, info = engine.apply_portfolio_risk_engine(None, desired, {}, now_ms=1_700_000_000_000)

    assert engine._DEFAULT_ASSET_CLASS_BUDGETS["CRYPTO"] == pytest.approx(0.35)
    assert out["BTC"]["weight"] == pytest.approx(0.35)
    assert info["asset_class_gross_post"]["CRYPTO"] <= 0.35 + 1e-12
    assert info["target_exposure_post"]["by_asset_class"]["CRYPTO"]["gross"] <= 0.35 + 1e-12
