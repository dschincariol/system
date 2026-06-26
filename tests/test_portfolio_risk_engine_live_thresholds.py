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


def _reload_required_mc_engine(monkeypatch, **env):
    monkeypatch.setenv("ENGINE_MODE", "live")
    monkeypatch.setenv("PORTFOLIO_RISK_USE_MONTE_CARLO", "1")
    monkeypatch.setenv("PORTFOLIO_RISK_MC_REQUIRED_IN_LIVE", "1")
    monkeypatch.setenv("PORTFOLIO_RISK_MC_MAX_AGE_S", "900")
    for key, value in env.items():
        monkeypatch.setenv(key, str(value))

    import engine.risk.portfolio_risk_engine as portfolio_risk_engine

    return importlib.reload(portfolio_risk_engine)


def _reload_live_overlay_engine(monkeypatch, **env):
    monkeypatch.setenv("ENGINE_MODE", "live")
    monkeypatch.setenv("EXECUTION_MODE", "live")
    monkeypatch.setenv("PORTFOLIO_RISK_USE_MONTE_CARLO", "0")
    monkeypatch.setenv("PORTFOLIO_RISK_MC_REQUIRED_IN_LIVE", "0")
    monkeypatch.setenv("PORTFOLIO_RISK_USE_ALPHA_DECAY_THROTTLE", "1")
    monkeypatch.setenv("PORTFOLIO_RISK_USE_VOL_CAPS", "1")
    monkeypatch.setenv("PORTFOLIO_RISK_USE_CORR_CLUSTERS", "1")
    monkeypatch.setenv("PORTFOLIO_RISK_DD_THROTTLE_START", "0.01")
    monkeypatch.setenv("PORTFOLIO_RISK_DD_THROTTLE_MIN_SCALE", "0.35")
    monkeypatch.setenv("PORTFOLIO_RISK_VOL_TARGET", "0.02")
    for key, value in env.items():
        monkeypatch.setenv(key, str(value))

    import engine.risk.portfolio_risk_engine as portfolio_risk_engine

    return importlib.reload(portfolio_risk_engine)


def _patch_fast_apply_path(monkeypatch, portfolio_risk_engine):
    captured_state = {}
    monkeypatch.setattr(portfolio_risk_engine, "evaluate_current_drawdown", lambda _con: _DrawdownOK())
    monkeypatch.setattr(
        portfolio_risk_engine,
        "_load_live_positions",
        lambda _con: ({}, {"source": "unit_test", "equity_ref": 100_000.0, "equity_ref_source": "unit_test"}),
    )
    monkeypatch.setattr(portfolio_risk_engine, "_apply_drawdown_throttle", lambda desired, _dd, _info: dict(desired or {}))
    monkeypatch.setattr(portfolio_risk_engine, "_apply_asset_class_budgets", lambda desired, _info: dict(desired or {}))
    monkeypatch.setattr(portfolio_risk_engine, "_apply_futures_margin_caps", lambda _con, desired, _info: dict(desired or {}))
    monkeypatch.setattr(portfolio_risk_engine, "_apply_fx_leverage_caps", lambda _con, desired, _info: dict(desired or {}))
    monkeypatch.setattr(portfolio_risk_engine, "_apply_equity_leverage_caps", lambda _con, desired, _info: dict(desired or {}))
    monkeypatch.setattr(portfolio_risk_engine, "_apply_crypto_leverage_caps", lambda _con, desired, _info: dict(desired or {}))
    monkeypatch.setattr(portfolio_risk_engine, "_apply_sector_budgets", lambda _con, desired, _info: dict(desired or {}))
    monkeypatch.setattr(portfolio_risk_engine, "_apply_strategy_budgets", lambda desired, _info: dict(desired or {}))
    monkeypatch.setattr(portfolio_risk_engine, "_apply_alpha_decay_throttle", lambda _con, desired, _info, _now_ms: dict(desired or {}))
    monkeypatch.setattr(portfolio_risk_engine, "_apply_symbol_vol_caps", lambda _con, desired, _info: dict(desired or {}))
    monkeypatch.setattr(portfolio_risk_engine, "_apply_corr_cluster_caps", lambda _con, desired, _info: dict(desired or {}))
    monkeypatch.setattr(portfolio_risk_engine, "_apply_portfolio_vol_target", lambda _con, desired, _info: dict(desired or {}))
    monkeypatch.setattr(portfolio_risk_engine, "_apply_portfolio_caps", lambda desired, _info: dict(desired or {}))
    monkeypatch.setattr(portfolio_risk_engine, "_persist_snapshot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(portfolio_risk_engine, "record_risk_block", lambda **_kwargs: None)
    monkeypatch.setattr(portfolio_risk_engine, "set_state", lambda key, value: captured_state.__setitem__(key, value))
    return captured_state


def _assert_apply_blocks_for_mc_reason(monkeypatch, portfolio_risk_engine, reason_type: str, now_ms: int):
    captured_state = _patch_fast_apply_path(monkeypatch, portfolio_risk_engine)
    desired = {"AAPL": {"symbol": "AAPL", "weight": 0.10, "side": "LONG", "reason": {}}}

    _out, info = portfolio_risk_engine.apply_portfolio_risk_engine(None, desired, {}, now_ms=int(now_ms))

    assert info["blocked"] is True
    assert info["block_reason"]["type"] == "monte_carlo_risk_block"
    assert captured_state["portfolio_risk_block"] == "1"
    reason_types = {str(row.get("type") or "") for row in info["monte_carlo_risk"]["reasons"]}
    assert reason_type in reason_types
    return info


def _set_mc_payload(monkeypatch, portfolio_risk_engine, payload, ts_ms):
    monkeypatch.setattr(
        portfolio_risk_engine,
        "get_state_row",
        lambda key, default: (json.dumps(payload, separators=(",", ":")), int(ts_ms)),
    )


def test_required_live_monte_carlo_missing_state_blocks_portfolio_approval(monkeypatch):
    portfolio_risk_engine = _reload_required_mc_engine(monkeypatch)
    now_ms = 1_700_000_000_000
    monkeypatch.setattr(portfolio_risk_engine, "get_state_row", lambda key, default: ("", 0))

    summary = portfolio_risk_engine._load_monte_carlo_risk_summary(now_ms)

    assert summary["blocked"] is True
    assert summary["required"] is True
    assert summary["reasons"][0]["type"] == "monte_carlo_risk_state_missing"
    _assert_apply_blocks_for_mc_reason(monkeypatch, portfolio_risk_engine, "monte_carlo_risk_state_missing", now_ms)


def test_required_live_monte_carlo_read_error_blocks_portfolio_approval(monkeypatch):
    portfolio_risk_engine = _reload_required_mc_engine(monkeypatch)
    now_ms = 1_700_000_000_000

    def _raise_read_error(_key, _default):
        raise RuntimeError("risk-state read failed")

    monkeypatch.setattr(portfolio_risk_engine, "get_state_row", _raise_read_error)

    summary = portfolio_risk_engine._load_monte_carlo_risk_summary(now_ms)

    assert summary["blocked"] is True
    assert summary["reasons"][0]["type"] == "monte_carlo_risk_state_read_error"
    assert summary["reasons"][0]["error_type"] == "RuntimeError"
    _assert_apply_blocks_for_mc_reason(monkeypatch, portfolio_risk_engine, "monte_carlo_risk_state_read_error", now_ms)


def test_required_live_monte_carlo_parse_error_blocks_portfolio_approval(monkeypatch):
    portfolio_risk_engine = _reload_required_mc_engine(monkeypatch)
    now_ms = 1_700_000_000_000
    monkeypatch.setattr(portfolio_risk_engine, "get_state_row", lambda key, default: ("{not-json", now_ms))

    summary = portfolio_risk_engine._load_monte_carlo_risk_summary(now_ms)

    assert summary["blocked"] is True
    assert summary["reasons"][0]["type"] == "monte_carlo_risk_state_parse_error"
    assert summary["reasons"][0]["error_type"] == "JSONDecodeError"
    _assert_apply_blocks_for_mc_reason(monkeypatch, portfolio_risk_engine, "monte_carlo_risk_state_parse_error", now_ms)


def test_required_live_monte_carlo_stale_state_blocks_portfolio_approval(monkeypatch):
    portfolio_risk_engine = _reload_required_mc_engine(monkeypatch)
    now_ms = 1_700_000_000_000
    payload = {
        "ready": True,
        "status": "ok",
        "var_95": -0.01,
        "var_99": -0.02,
        "cvar_95": -0.055,
        "cvar_99": -0.09,
        "worst_simulated_drawdown": 0.01,
        "drawdown_percentiles": {"p95": 0.01, "p99": 0.02},
    }
    _set_mc_payload(monkeypatch, portfolio_risk_engine, payload, now_ms - 901_000)

    summary = portfolio_risk_engine._load_monte_carlo_risk_summary(now_ms)

    assert summary["blocked"] is True
    assert summary["stale"] is True
    assert summary["reasons"][0]["type"] == "monte_carlo_risk_state_stale"
    _assert_apply_blocks_for_mc_reason(monkeypatch, portfolio_risk_engine, "monte_carlo_risk_state_stale", now_ms)


def test_required_live_monte_carlo_simulation_error_marker_blocks_portfolio_approval(monkeypatch):
    portfolio_risk_engine = _reload_required_mc_engine(monkeypatch)
    now_ms = 1_700_000_000_000
    payload = {
        "enabled": True,
        "ready": False,
        "pending": False,
        "status": "error",
        "error": "simulation failed",
        "ts_ms": now_ms,
    }
    _set_mc_payload(monkeypatch, portfolio_risk_engine, payload, now_ms)

    summary = portfolio_risk_engine._load_monte_carlo_risk_summary(now_ms)

    assert summary["blocked"] is True
    assert summary["reasons"][0]["type"] == "monte_carlo_risk_simulation_error"
    _assert_apply_blocks_for_mc_reason(monkeypatch, portfolio_risk_engine, "monte_carlo_risk_simulation_error", now_ms)


def test_monte_carlo_cvar_thresholds_block_when_tail_losses_breach(monkeypatch):
    portfolio_risk_engine = _reload_required_mc_engine(
        monkeypatch,
        PORTFOLIO_RISK_MC_CVAR_95_BLOCK="0.05",
        PORTFOLIO_RISK_MC_CVAR_99_BLOCK="0.08",
    )
    now_ms = 1_700_000_000_000
    payload = {
        "ready": True,
        "status": "ok",
        "var_95": -0.01,
        "var_99": -0.02,
        "cvar_95": -0.055,
        "cvar_99": -0.09,
        "worst_simulated_drawdown": 0.01,
        "drawdown_percentiles": {"p95": 0.01, "p99": 0.02},
    }
    _set_mc_payload(monkeypatch, portfolio_risk_engine, payload, now_ms)

    summary = portfolio_risk_engine._load_monte_carlo_risk_summary(now_ms)

    assert summary["blocked"] is True
    reason_types = {str(row.get("type") or "") for row in summary["reasons"]}
    assert "monte_carlo_cvar_95_block" in reason_types
    assert "monte_carlo_cvar_99_block" in reason_types
    _assert_apply_blocks_for_mc_reason(monkeypatch, portfolio_risk_engine, "monte_carlo_cvar_95_block", now_ms)


@pytest.mark.parametrize(
    ("overlay_name", "function_name"),
    [
        ("drawdown_throttle", "_apply_drawdown_throttle"),
        ("alpha_decay_throttle", "_apply_alpha_decay_throttle"),
        ("symbol_vol_caps", "_apply_symbol_vol_caps"),
        ("corr_cluster_caps", "_apply_corr_cluster_caps"),
        ("portfolio_vol_target", "_apply_portfolio_vol_target"),
    ],
)
def test_live_enabled_overlay_failure_blocks_portfolio_risk_state(monkeypatch, overlay_name, function_name):
    portfolio_risk_engine = _reload_live_overlay_engine(monkeypatch)
    captured_state = _patch_fast_apply_path(monkeypatch, portfolio_risk_engine)
    now_ms = 1_700_000_000_000
    desired = {"AAPL": {"symbol": "AAPL", "weight": 0.10, "side": "LONG", "reason": {}}}

    def _raise_overlay_failure(*_args, **_kwargs):
        raise RuntimeError(f"{overlay_name} injected failure")

    monkeypatch.setattr(portfolio_risk_engine, function_name, _raise_overlay_failure)

    _out, info = portfolio_risk_engine.apply_portfolio_risk_engine(None, desired, {}, now_ms=now_ms)

    assert info["blocked"] is True
    assert info["overlay_failed"] == overlay_name
    assert info["block_reason"]["type"] == "required_overlay_failed"
    assert info["block_reason"]["overlay"] == overlay_name
    assert info["post_checks"]["required_overlays_ok"] is False
    assert captured_state["portfolio_risk_block"] == "1"
    failures = list(info.get("required_overlay_failures") or [])
    assert any(str(row.get("name") or "") == overlay_name for row in failures)


def test_live_enabled_overlay_success_path_remains_clear(monkeypatch):
    portfolio_risk_engine = _reload_live_overlay_engine(monkeypatch)
    captured_state = _patch_fast_apply_path(monkeypatch, portfolio_risk_engine)
    now_ms = 1_700_000_000_000
    desired = {"AAPL": {"symbol": "AAPL", "weight": 0.10, "side": "LONG", "reason": {}}}

    out, info = portfolio_risk_engine.apply_portfolio_risk_engine(None, desired, {}, now_ms=now_ms)

    assert info["blocked"] is False
    assert "overlay_failed" not in info
    assert captured_state["portfolio_risk_block"] == "0"
    assert out["AAPL"]["weight"] == pytest.approx(0.10)
