from __future__ import annotations

import importlib
import os
from pathlib import Path
import sys
from unittest.mock import Mock

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload(name: str):
    if name in sys.modules:
        return importlib.reload(sys.modules[name])
    return importlib.import_module(name)


def _option_order() -> dict:
    return {
        "symbol": "AAPL270117C00150000",
        "instrument_type": "option",
        "underlying": "AAPL",
        "option_contract": "AAPL270117C00150000",
        "expiration": "2027-01-17",
        "contract_type": "call",
        "strike": 150.0,
        "side": "buy",
        "position_effect": "open",
        "qty": 1,
        "bid": 9.95,
        "ask": 10.05,
        "delta": 0.55,
        "gamma": 0.02,
        "theta": -0.01,
        "vega": 0.12,
        "open_interest": 500,
        "volume": 100,
        "margin_impact_fraction": 0.05,
        "assignment_exercise_policy": "monitor_and_manual_exercise",
    }


def _set_complete_live_options_controls(monkeypatch) -> None:
    monkeypatch.setenv("OPTIONS_INSTRUMENTS_MODE", "live")
    monkeypatch.setenv("OPTIONS_LIFECYCLE_ENABLED", "1")
    for name in (
        "OPTIONS_LIVE_GREEKS_READY",
        "OPTIONS_LIVE_LIQUIDITY_FILTERS_READY",
        "OPTIONS_LIVE_BID_ASK_QUALITY_READY",
        "OPTIONS_LIVE_ASSIGNMENT_EXERCISE_READY",
        "OPTIONS_LIVE_EXPIRATION_RISK_READY",
        "OPTIONS_LIVE_MARGIN_IMPACT_READY",
        "OPTIONS_LIVE_BROKER_SUPPORT_READY",
        "OPTIONS_LIVE_POSITION_LIMITS_READY",
        "OPTIONS_LIVE_KILL_SWITCH_INTEGRATION_READY",
    ):
        monkeypatch.setenv(name, "1")
    monkeypatch.setenv("OPTIONS_MIN_OPEN_INTEREST", "10")
    monkeypatch.setenv("OPTIONS_MIN_VOLUME", "5")
    monkeypatch.setenv("OPTIONS_MAX_SPREAD_BPS", "250")
    monkeypatch.setenv("OPTIONS_MIN_DTE_DAYS", "1")
    monkeypatch.setenv("OPTIONS_MAX_DTE_DAYS", "40000")
    monkeypatch.setenv("OPTIONS_MAX_POSITION_CONTRACTS", "10")
    monkeypatch.setenv("OPTIONS_MARGIN_IMPACT_MAX_FRACTION", "0.25")
    monkeypatch.setenv("OPTIONS_MAX_PORTFOLIO_DELTA_ABS", "200")
    monkeypatch.setenv("OPTIONS_MAX_PORTFOLIO_GAMMA_ABS", "200")
    monkeypatch.setenv("OPTIONS_MAX_PORTFOLIO_VEGA_ABS", "200")


def _patch_gate_predicates(monkeypatch, readiness, failing: str | None = None) -> None:
    def predicate(kind, _context):
        if kind == failing:
            return False, {"test_gate": kind, "healthy": False}
        return True, {"test_gate": kind, "healthy": True}

    monkeypatch.setattr(
        readiness,
        "_GATE_PREDICATES",
        {control: predicate for control, _blocker, _names in readiness.CONTROL_FLAG_GROUPS},
    )


def test_tradier_options_positive_readiness_requires_real_predicates(monkeypatch):
    _set_complete_live_options_controls(monkeypatch)
    readiness = _reload("engine.execution.options_readiness")
    _patch_gate_predicates(monkeypatch, readiness)

    state = readiness.live_options_readiness_snapshot(
        engine_mode="live",
        execution_mode="live",
        broker="tradier_options",
        orders=[_option_order()],
    )

    assert state["ok"] is True
    assert state["controls"]["greeks"]["detail"]["healthy"] is True
    assert readiness.live_options_order_block([_option_order()], broker="tradier_options") is None


def test_greeks_env_flag_is_not_enough_when_runtime_check_fails(monkeypatch):
    _set_complete_live_options_controls(monkeypatch)
    readiness = _reload("engine.execution.options_readiness")
    _patch_gate_predicates(monkeypatch, readiness, failing="greeks")

    state = readiness.live_options_readiness_snapshot(
        engine_mode="live",
        execution_mode="live",
        broker="tradier_options",
        orders=[_option_order()],
    )

    assert state["ok"] is False
    assert "options_live_greeks_gate_check_failed" in state["blockers"]


def test_kill_switch_env_flag_is_not_enough_when_runtime_check_fails(monkeypatch):
    _set_complete_live_options_controls(monkeypatch)
    readiness = _reload("engine.execution.options_readiness")
    _patch_gate_predicates(monkeypatch, readiness, failing="kill_switch_integration")

    state = readiness.live_options_readiness_snapshot(
        engine_mode="live",
        execution_mode="live",
        broker="tradier_options",
        orders=[_option_order()],
    )

    assert state["ok"] is False
    assert "options_live_kill_switch_integration_check_failed" in state["blockers"]


def test_tradier_options_removed_from_adapter_registry_blocks(monkeypatch):
    _set_complete_live_options_controls(monkeypatch)
    readiness = _reload("engine.execution.options_readiness")
    _patch_gate_predicates(monkeypatch, readiness)
    monkeypatch.setattr(readiness, "LIVE_OPTIONS_BROKER_ADAPTERS", frozenset())

    state = readiness.live_options_readiness_snapshot(
        engine_mode="live",
        execution_mode="live",
        broker="tradier_options",
        orders=[_option_order()],
    )

    assert state["ok"] is False
    assert "options_live_broker_adapter_missing:tradier_options" in state["blockers"]


def test_tradier_options_missing_token_is_terminal_without_network(monkeypatch):
    adapter = _reload("engine.execution.broker_tradier_options")
    monkeypatch.delenv("TRADIER_API_TOKEN", raising=False)
    monkeypatch.delenv("TRADIER_ACCOUNT_ID", raising=False)
    monkeypatch.setattr(adapter, "get_data_credential", lambda _name: "")
    post_order = Mock(side_effect=AssertionError("HTTP must not be called without credentials"))
    monkeypatch.setattr(adapter, "_post_order", post_order)

    result = adapter.apply_latest_portfolio_orders_live(dry_run=False, override_orders=[_option_order()])

    assert result["ok"] is False
    assert result["status"] == "missing_credentials"
    assert "TRADIER_API_TOKEN" in result["missing"]
    post_order.assert_not_called()


def test_default_remains_shadow_and_force_shadow_intent(monkeypatch):
    for key in list(os.environ):
        if key.startswith("OPTIONS_"):
            monkeypatch.delenv(key, raising=False)
    readiness = _reload("engine.execution.options_readiness")

    state = readiness.live_options_readiness_snapshot()
    intent = {"option_symbol": "AAPL270117C00150000", "instrument_type": "option", "execution_target": "real"}
    out = readiness.force_options_shadow_intent(intent)

    assert state["required"] is False
    assert state["shadow_only"] is True
    assert out["execution_target"] == "shadow"


def test_alpaca_still_blocks_with_exact_adapter_missing(monkeypatch):
    _set_complete_live_options_controls(monkeypatch)
    readiness = _reload("engine.execution.options_readiness")
    _patch_gate_predicates(monkeypatch, readiness)

    state = readiness.live_options_readiness_snapshot(
        engine_mode="live",
        execution_mode="live",
        broker="alpaca",
        orders=[_option_order()],
    )

    assert state["ok"] is False
    assert state["blockers"] == ["options_live_broker_adapter_missing:alpaca"]


def test_tradier_payload_uses_verified_single_leg_option_fields(monkeypatch):
    adapter = _reload("engine.execution.broker_tradier_options")
    payload = adapter.build_tradier_option_order_payload(_option_order(), override_order_id=123, index=0)

    assert payload["class"] == "option"
    assert payload["symbol"] == "AAPL"
    assert payload["option_symbol"] == "AAPL270117C00150000"
    assert payload["side"] == "buy_to_open"
    assert payload["quantity"] == 1
    assert payload["type"] == "market"
    assert payload["duration"] == "day"
    assert payload["tag"] == "ts-123-0"
