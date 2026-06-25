from __future__ import annotations

import importlib
import json
import uuid

import pytest

pytestmark = pytest.mark.safety_critical


class _DrawdownOK:
    ok = True
    drawdown = 0.0
    reason_code = "ok"

    def to_dict(self):
        return {"ok": True, "drawdown": 0.0, "reason_code": "ok"}


def _reload_engine(monkeypatch: pytest.MonkeyPatch, **env):
    defaults = {
        "ENGINE_MODE": "live",
        "PORTFOLIO_USE_RISK_ENGINE": "1",
        "PORTFOLIO_RISK_USE_MONTE_CARLO": "1",
        "PORTFOLIO_RISK_MC_REQUIRED_IN_LIVE": "1",
        "PORTFOLIO_RISK_MC_MAX_AGE_S": "900",
        "PORTFOLIO_RISK_MC_VAR_95_BLOCK": "0",
        "PORTFOLIO_RISK_MC_VAR_99_BLOCK": "0",
        "PORTFOLIO_RISK_MC_CVAR_95_BLOCK": "0.05",
        "PORTFOLIO_RISK_MC_CVAR_99_BLOCK": "0",
        "PORTFOLIO_RISK_MC_DRAWDOWN_P95_BLOCK": "0",
        "PORTFOLIO_RISK_MC_WORST_DRAWDOWN_BLOCK": "0",
        "PORTFOLIO_RISK_USE_ASSET_CLASS_BUDGETS": "0",
        "PORTFOLIO_RISK_USE_SECTOR_BUDGETS": "0",
        "PORTFOLIO_RISK_USE_FX_LEVERAGE_CAPS": "0",
        "PORTFOLIO_RISK_USE_EQUITY_LEVERAGE_CAPS": "0",
        "PORTFOLIO_RISK_USE_CRYPTO_LEVERAGE_CAPS": "0",
        "PORTFOLIO_RISK_USE_FUTURES_MARGIN_CAPS": "0",
        "PORTFOLIO_RISK_USE_OPTIONS_GREEK_LIMITS": "0",
        "PORTFOLIO_RISK_USE_STRATEGY_BUDGETS": "0",
        "PORTFOLIO_RISK_USE_ALPHA_DECAY_THROTTLE": "0",
        "PORTFOLIO_RISK_USE_VOL_CAPS": "0",
        "PORTFOLIO_RISK_USE_CORR_CLUSTERS": "0",
        "PORTFOLIO_RISK_MAX_GROSS": "100",
        "PORTFOLIO_RISK_MAX_NET": "100",
        "PORTFOLIO_RISK_MAX_SYMBOL_GROSS": "100",
        "PORTFOLIO_RISK_VOL_HARD_BLOCK": "0",
    }
    defaults.update({key: str(value) for key, value in env.items()})
    for key, value in defaults.items():
        monkeypatch.setenv(key, value)

    import engine.risk.portfolio_risk_engine as portfolio_risk_engine

    return importlib.reload(portfolio_risk_engine)


def _patch_fast_apply_path(monkeypatch: pytest.MonkeyPatch, engine):
    captured_state: dict[str, str] = {}
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


def _set_mc_payload(monkeypatch: pytest.MonkeyPatch, engine, payload: dict[str, object], ts_ms: int) -> None:
    monkeypatch.setattr(
        engine,
        "get_state_row",
        lambda key, default: (json.dumps(payload, separators=(",", ":"), sort_keys=True), int(ts_ms)),
    )


def test_monte_carlo_cvar_block_still_blocks_apply_path(monkeypatch: pytest.MonkeyPatch) -> None:
    canary = f"secret-canary-{uuid.uuid4()}"
    engine = _reload_engine(monkeypatch)
    now_ms = 1_700_000_000_000
    _set_mc_payload(
        monkeypatch,
        engine,
        {
            "enabled": True,
            "ready": True,
            "status": "ok",
            "var_95": -0.01,
            "var_99": -0.02,
            "cvar_95": -0.055,
            "cvar_99": -0.07,
            "worst_simulated_drawdown": 0.01,
            "drawdown_percentiles": {"p95": 0.01, "p99": 0.02},
        },
        now_ms,
    )

    summary = engine._load_monte_carlo_risk_summary(now_ms)
    assert summary["blocked"] is True
    assert {row["type"] for row in summary["reasons"]} == {"monte_carlo_cvar_95_block"}

    captured_state = _patch_fast_apply_path(monkeypatch, engine)
    desired = {"SPY": {"symbol": "SPY", "weight": 0.10, "side": "LONG", "reason": {}}}
    _out, info = engine.apply_portfolio_risk_engine(None, desired, {}, now_ms=now_ms)

    assert info["blocked"] is True
    assert info["block_reason"]["type"] == "monte_carlo_risk_block"
    assert info["block_reason"]["monte_carlo"]["reasons"][0]["type"] == "monte_carlo_cvar_95_block"
    assert captured_state["portfolio_risk_block"] == "1"
    assert canary not in json.dumps(info, sort_keys=True)


def test_monte_carlo_staleness_still_fails_closed_when_required(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = _reload_engine(monkeypatch, PORTFOLIO_RISK_MC_CVAR_95_BLOCK="0")
    now_ms = 1_700_000_000_000
    _set_mc_payload(
        monkeypatch,
        engine,
        {
            "enabled": True,
            "ready": True,
            "status": "ok",
            "var_95": -0.01,
            "var_99": -0.02,
            "cvar_95": -0.01,
            "cvar_99": -0.02,
            "worst_simulated_drawdown": 0.01,
            "drawdown_percentiles": {"p95": 0.01, "p99": 0.02},
        },
        now_ms - 901_000,
    )

    summary = engine._load_monte_carlo_risk_summary(now_ms)

    assert summary["blocked"] is True
    assert summary["stale"] is True
    assert summary["reasons"][0]["type"] == "monte_carlo_risk_state_stale"
    assert summary["reasons"][0]["max_age_s"] == 900
