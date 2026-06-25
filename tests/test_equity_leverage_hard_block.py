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


def _reload_engine(monkeypatch: pytest.MonkeyPatch, **env):
    defaults = {
        "PORTFOLIO_RISK_USE_MONTE_CARLO": "0",
        "PORTFOLIO_RISK_USE_ASSET_CLASS_BUDGETS": "0",
        "PORTFOLIO_RISK_USE_FX_LEVERAGE_CAPS": "0",
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
        "PORTFOLIO_RISK_SYMBOL_CAP_MAX_W": "100",
        "PORTFOLIO_RISK_USE_EQUITY_LEVERAGE_CAPS": "1",
        "EQUITY_LEVERAGE_MODE": "cash",
        "EQUITY_LEVERAGE_CAPS_JSON": "",
        "DEPLOYABLE_EQUITY_MODE": "min_equity_bp",
        "DEPLOYABLE_BP_FACTOR": "0.50",
        "DEPLOYABLE_CASH_FACTOR": "1.00",
        "DEPLOYABLE_EQUITY_FACTOR": "1.00",
    }
    defaults.update({key: str(value) for key, value in env.items()})
    for key, value in defaults.items():
        monkeypatch.setenv(key, value)

    import engine.execution.deployable_capital as deployable_capital
    import engine.risk.equity_leverage_caps as equity_leverage_caps
    import engine.strategy.equity_sizing as equity_sizing
    import engine.risk.portfolio_risk_engine as portfolio_risk_engine

    importlib.reload(deployable_capital)
    importlib.reload(equity_leverage_caps)
    importlib.reload(equity_sizing)
    return importlib.reload(portfolio_risk_engine)


def _patch_fast_path(
    monkeypatch: pytest.MonkeyPatch,
    engine,
    *,
    account_equity: float = 100_000.0,
    buying_power=None,
):
    captured_state = {}
    monkeypatch.setattr(engine, "evaluate_current_drawdown", lambda _con: _DrawdownOK())
    monkeypatch.setattr(
        engine,
        "_load_live_positions",
        lambda _con: ({}, {"source": "unit_test", "equity_ref": account_equity, "equity_ref_source": "unit_test"}),
    )
    monkeypatch.setattr(engine, "_equity_reference", lambda _con: (float(account_equity), "unit_test"))
    monkeypatch.setattr(
        engine,
        "_buying_power_reference",
        lambda _con: (
            (float(buying_power) if buying_power is not None else None),
            ("unit_test" if buying_power is not None else "unavailable"),
        ),
    )
    monkeypatch.setattr(engine, "_asset_class_for", lambda _con, _symbol: "EQUITY")
    monkeypatch.setattr(engine, "_futures_multiplier_factor", lambda _con, _symbol: 1.0)
    monkeypatch.setattr(engine, "set_state", lambda key, value: captured_state.__setitem__(key, value))
    monkeypatch.setattr(engine, "record_risk_block", lambda **_kwargs: None)
    monkeypatch.setattr(engine, "_persist_snapshot", lambda *_args, **_kwargs: None)
    return captured_state


@pytest.mark.safety_critical
def test_equity_leverage_stage_clamps_aggregate_gross(monkeypatch: pytest.MonkeyPatch) -> None:
    canary = f"canary-{uuid.uuid4()}"
    engine = _reload_engine(monkeypatch)
    captured_state = _patch_fast_path(monkeypatch, engine)
    desired = {
        "AAPL": {"symbol": "AAPL", "weight": 1.0, "side": "LONG", "reason": {}},
        "MSFT": {"symbol": "MSFT", "weight": 0.6, "side": "LONG", "reason": {}},
    }

    out, info = engine.apply_portfolio_risk_engine(None, desired, {}, now_ms=1_700_000_000_000)

    assert out["AAPL"]["weight"] == pytest.approx(0.625)
    assert out["MSFT"]["weight"] == pytest.approx(0.375)
    assert info["equity_leverage_gross_pre"] == pytest.approx(1.6)
    assert info["equity_leverage_gross_post"] == pytest.approx(1.0)
    assert info["equity_leverage_allowed_gross_weight"] == pytest.approx(1.0)
    assert info["equity_leverage_adjustments"]["AAPL"]["clamp"]["type"] == "equity_leverage_cap"
    assert info["blocked"] is False
    assert captured_state["portfolio_risk_block"] == "0"
    assert canary not in json.dumps(info, sort_keys=True)


@pytest.mark.safety_critical
def test_reg_t_equity_leverage_missing_buying_power_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = _reload_engine(monkeypatch, EQUITY_LEVERAGE_MODE="reg_t")
    captured_state = _patch_fast_path(monkeypatch, engine, buying_power=None)
    monkeypatch.setattr(
        engine,
        "equity_deployable_base",
        lambda *_args, **_kwargs: pytest.fail("engine must hard-block before equity_deployable_base"),
    )
    desired = {"AAPL": {"symbol": "AAPL", "weight": 0.25, "side": "LONG", "reason": {}}}

    out, info = engine.apply_portfolio_risk_engine(None, desired, {}, now_ms=1_700_000_000_000)

    assert out["AAPL"]["weight"] == pytest.approx(0.25)
    assert info["blocked"] is True
    assert info["block_reason"]["type"] == "equity_leverage_hard_block"
    assert info["equity_leverage_hard_blocks"][0]["reason"] == "equity_buying_power_unavailable"
    assert captured_state["portfolio_risk_block"] == "1"


@pytest.mark.safety_critical
def test_equity_leverage_missing_account_equity_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = _reload_engine(monkeypatch)
    captured_state = _patch_fast_path(monkeypatch, engine, account_equity=0.0)
    desired = {"AAPL": {"symbol": "AAPL", "weight": 0.25, "side": "LONG", "reason": {}}}

    out, info = engine.apply_portfolio_risk_engine(None, desired, {}, now_ms=1_700_000_000_000)

    assert out["AAPL"]["weight"] == pytest.approx(0.25)
    assert info["blocked"] is True
    assert info["block_reason"]["type"] == "equity_leverage_hard_block"
    assert info["equity_leverage_hard_blocks"][0]["reason"] == "equity_account_reference_unavailable"
    assert captured_state["portfolio_risk_block"] == "1"
