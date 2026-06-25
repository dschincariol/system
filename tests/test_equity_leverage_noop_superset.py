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
        "DEPLOYABLE_EQUITY_MODE": "min_equity_bp",
        "DEPLOYABLE_BP_FACTOR": "0.50",
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


def _patch_fast_path(monkeypatch: pytest.MonkeyPatch, engine, *, asset_class: str):
    captured_state = {}
    monkeypatch.setattr(engine, "evaluate_current_drawdown", lambda _con: _DrawdownOK())
    monkeypatch.setattr(
        engine,
        "_load_live_positions",
        lambda _con: ({}, {"source": "unit_test", "equity_ref": 100_000.0, "equity_ref_source": "unit_test"}),
    )
    monkeypatch.setattr(engine, "_equity_reference", lambda _con: (100_000.0, "unit_test"))
    monkeypatch.setattr(engine, "_buying_power_reference", lambda _con: (None, "unavailable"))
    monkeypatch.setattr(engine, "_asset_class_for", lambda _con, _symbol: str(asset_class))
    monkeypatch.setattr(engine, "_futures_multiplier_factor", lambda _con, _symbol: 1.0)
    monkeypatch.setattr(engine, "set_state", lambda key, value: captured_state.__setitem__(key, value))
    monkeypatch.setattr(engine, "record_risk_block", lambda **_kwargs: None)
    monkeypatch.setattr(engine, "_persist_snapshot", lambda *_args, **_kwargs: None)
    return captured_state


def _stable(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


@pytest.mark.safety_critical
def test_equity_leverage_disabled_matches_identity_stage(monkeypatch: pytest.MonkeyPatch) -> None:
    desired = {"AAPL": {"symbol": "AAPL", "weight": 0.25, "side": "LONG", "reason": {"tag": "equity"}}}

    engine_identity = _reload_engine(monkeypatch, PORTFOLIO_RISK_USE_EQUITY_LEVERAGE_CAPS="1")
    _patch_fast_path(monkeypatch, engine_identity, asset_class="EQUITY")
    monkeypatch.setattr(engine_identity, "_apply_equity_leverage_caps", lambda _con, rows, _info: dict(rows or {}))
    out_identity, _info_identity = engine_identity.apply_portfolio_risk_engine(
        None,
        desired,
        {},
        now_ms=1_700_000_000_000,
    )

    engine_disabled = _reload_engine(monkeypatch, PORTFOLIO_RISK_USE_EQUITY_LEVERAGE_CAPS="0")
    _patch_fast_path(monkeypatch, engine_disabled, asset_class="EQUITY")
    out_disabled, info_disabled = engine_disabled.apply_portfolio_risk_engine(
        None,
        desired,
        {},
        now_ms=1_700_000_000_000,
    )

    assert _stable(out_disabled) == _stable(out_identity)
    assert not [key for key in info_disabled if str(key).startswith("equity_leverage_")]


@pytest.mark.safety_critical
def test_non_equity_outputs_match_identity_stage(monkeypatch: pytest.MonkeyPatch) -> None:
    desired = {"EURUSD": {"symbol": "EURUSD", "weight": 0.25, "side": "LONG", "reason": {"tag": "fx"}}}

    engine_identity = _reload_engine(monkeypatch)
    _patch_fast_path(monkeypatch, engine_identity, asset_class="FX")
    monkeypatch.setattr(engine_identity, "_apply_equity_leverage_caps", lambda _con, rows, _info: dict(rows or {}))
    out_identity, _info_identity = engine_identity.apply_portfolio_risk_engine(
        None,
        desired,
        {},
        now_ms=1_700_000_000_000,
    )

    engine_actual = _reload_engine(monkeypatch)
    _patch_fast_path(monkeypatch, engine_actual, asset_class="FX")
    out_actual, info_actual = engine_actual.apply_portfolio_risk_engine(
        None,
        desired,
        {},
        now_ms=1_700_000_000_000,
    )

    assert _stable(out_actual) == _stable(out_identity)
    assert not [key for key in info_actual if str(key).startswith("equity_leverage_")]
