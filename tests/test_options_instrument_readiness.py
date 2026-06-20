from __future__ import annotations

import importlib
import sys
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload(name: str):
    if name in sys.modules:
        return importlib.reload(sys.modules[name])
    return importlib.import_module(name)


def _option_order() -> dict:
    return {
        "symbol": "AAPL240117C00150000",
        "instrument_type": "option",
        "underlying": "AAPL",
        "option_contract": "AAPL240117C00150000",
        "expiration": "2099-01-17",
        "contract_type": "call",
        "strike": 150.0,
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


def _set_live_broker_env(monkeypatch) -> None:
    monkeypatch.setenv("ENGINE_MODE", "live")
    monkeypatch.setenv("EXECUTION_MODE", "live")
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("BROKER_FAILOVER", "alpaca")
    monkeypatch.setenv("BROKER", "alpaca")
    monkeypatch.setenv("BROKER_NAME", "alpaca")
    monkeypatch.setenv("LIVE_BROKER", "alpaca")
    monkeypatch.setenv("ALPACA_BASE_URL", "https://api.alpaca.markets")
    monkeypatch.setenv("ALPACA_KEY_ID", "alpaca-key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "alpaca-secret")


def _set_complete_live_options_controls(monkeypatch) -> None:
    monkeypatch.setenv("OPTIONS_INSTRUMENTS_MODE", "live")
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
    monkeypatch.setenv("OPTIONS_MAX_PORTFOLIO_DELTA_ABS", "2.0")


def test_options_default_to_shadow_only_not_live_required(monkeypatch):
    monkeypatch.delenv("OPTIONS_INSTRUMENTS_MODE", raising=False)
    readiness = _reload("engine.execution.options_readiness")

    state = readiness.live_options_readiness_snapshot(
        engine_mode="live",
        execution_mode="live",
        broker="alpaca",
    )

    assert state["ok"] is True
    assert state["required"] is False
    assert state["shadow_only"] is True


def test_live_options_request_requires_all_controls_and_real_adapter(monkeypatch):
    monkeypatch.setenv("OPTIONS_INSTRUMENTS_MODE", "live")
    readiness = _reload("engine.execution.options_readiness")

    state = readiness.live_options_readiness_snapshot(
        engine_mode="live",
        execution_mode="live",
        broker="alpaca",
        orders=[_option_order()],
    )

    assert state["ok"] is False
    assert "options_live_greeks_gate_missing" in state["blockers"]
    assert "options_live_liquidity_filters_missing" in state["blockers"]
    assert "options_live_bid_ask_quality_missing" in state["blockers"]
    assert "options_live_assignment_exercise_missing" in state["blockers"]
    assert "options_live_expiration_risk_missing" in state["blockers"]
    assert "options_live_margin_impact_missing" in state["blockers"]
    assert "options_live_broker_support_missing" in state["blockers"]
    assert "options_live_position_limits_missing" in state["blockers"]
    assert "options_live_kill_switch_integration_missing" in state["blockers"]
    assert "options_live_broker_adapter_missing:alpaca" in state["blockers"]


def test_complete_control_flags_still_block_until_live_adapter_exists(monkeypatch):
    _set_complete_live_options_controls(monkeypatch)
    readiness = _reload("engine.execution.options_readiness")

    state = readiness.live_options_readiness_snapshot(
        engine_mode="live",
        execution_mode="live",
        broker="alpaca",
        orders=[_option_order()],
    )

    assert state["ok"] is False
    assert state["blockers"] == ["options_live_broker_adapter_missing:alpaca"]


def test_router_rejects_live_option_order_before_adapter(monkeypatch):
    _set_live_broker_env(monkeypatch)
    broker_router = _reload("engine.execution.broker_router")
    live_adapter = Mock(side_effect=AssertionError("live adapter must not receive option orders"))

    with ExitStack() as stack:
        for attr in (
            "emit_counter",
            "emit_timing",
            "record_rolling_rate",
            "record_component_health",
            "trace_event",
            "log_event",
        ):
            stack.enter_context(patch.object(broker_router, attr, return_value=None))
        stack.enter_context(patch.object(broker_router, "_execution_gate_snapshot", Mock(return_value={"ok": True, "allowed": True, "real_trading_allowed": True})))
        stack.enter_context(patch.object(broker_router, "_kill_switch_snapshot", Mock(return_value={"state": []})))
        stack.enter_context(patch.object(broker_router, "_get_execution_mode", Mock(return_value={"mode": "live", "armed": 1})))
        stack.enter_context(patch.object(broker_router, "_alpaca_apply", live_adapter))

        result = broker_router.apply_new_portfolio_orders_router(
            dry_run=False,
            override_orders=[_option_order()],
            override_order_id=42,
            override_ts_ms=123456,
        )

    assert result["ok"] is False
    assert result["status"] == "options_instruments_not_live_ready"
    assert result["fatal_options_readiness"] is True
    assert result["options_readiness"]["reason"] == "options_live_orders_disabled_shadow_only"
    live_adapter.assert_not_called()


def test_sim_route_allows_shadow_option_intents(monkeypatch):
    monkeypatch.setenv("ENGINE_MODE", "paper")
    monkeypatch.setenv("EXECUTION_MODE", "paper")
    monkeypatch.setenv("BROKER_FAILOVER", "sim")
    broker_router = _reload("engine.execution.broker_router")
    sim_adapter = Mock(return_value={"ok": True, "status": "shadow_preview", "broker": "sim"})

    with ExitStack() as stack:
        for attr in (
            "emit_counter",
            "emit_timing",
            "record_rolling_rate",
            "record_component_health",
            "trace_event",
            "log_event",
        ):
            stack.enter_context(patch.object(broker_router, attr, return_value=None))
        stack.enter_context(patch.object(broker_router, "_sim_apply", sim_adapter))

        result = broker_router.apply_new_portfolio_orders_router(
            dry_run=True,
            override_orders=[_option_order()],
            override_order_id=43,
            override_ts_ms=123456,
        )

    assert result["ok"] is True
    assert result["status"] == "shadow_preview"
    sim_adapter.assert_called_once()


def test_alpaca_adapter_direct_call_rejects_options_before_account_or_submit(monkeypatch):
    _set_live_broker_env(monkeypatch)
    alpaca = _reload("engine.execution.broker_alpaca_rest")

    class DummyCon:
        def close(self) -> None:
            return None

    with ExitStack() as stack:
        stack.enter_context(patch.object(alpaca, "_real_trading_gate", Mock(return_value={"ok": True, "real_trading_allowed": True})))
        stack.enter_context(patch.object(alpaca, "_alpaca_credentials_block", Mock(return_value=None)))
        stack.enter_context(patch.object(alpaca, "_prelive_reconcile_or_block", Mock(return_value=None)))
        stack.enter_context(patch.object(alpaca, "connect", Mock(return_value=DummyCon())))
        stack.enter_context(patch.object(alpaca, "apply_alpha_lifecycle", Mock(return_value=([_option_order()], {"ok": True}))))
        get_account = stack.enter_context(patch.object(alpaca, "get_account", Mock(side_effect=AssertionError("account read must not happen"))))
        submit_market = stack.enter_context(patch.object(alpaca, "_submit_market_order", Mock(side_effect=AssertionError("submit must not happen"))))

        result = alpaca.apply_latest_portfolio_orders_live(
            dry_run=False,
            override_orders=[_option_order()],
            override_order_id=44,
            override_ts_ms=123456,
        )

    assert result["ok"] is False
    assert result["status"] == "options_instruments_not_live_ready"
    get_account.assert_not_called()
    submit_market.assert_not_called()


def test_force_options_shadow_intent_normalizes_metadata():
    readiness = _reload("engine.execution.options_readiness")

    intent = {
        **_option_order(),
        "execution_target": "real",
        "competition": {"allowed": True, "blocked": False},
    }
    out = readiness.force_options_shadow_intent(intent)

    assert out["execution_target"] == "shadow"
    assert out["instrument_type"] == "option"
    assert out["options_instrument"]["underlying"] == "AAPL"
    assert out["options_instrument"]["greeks"]["delta"] == pytest.approx(0.55)
    assert out["competition"]["blocked"] is True
    assert out["competition"]["reason"] == "options_instruments_shadow_only"


def test_live_preflight_surfaces_options_instrument_blockers(monkeypatch):
    monkeypatch.setenv("OPTIONS_INSTRUMENTS_MODE", "live")
    preflight = _reload("engine.runtime.live_trading_preflight")

    monkeypatch.setattr(
        preflight,
        "live_environment_contract_snapshot",
        lambda **_kwargs: {
            "ok": True,
            "blockers": [],
            "confirmation": {"required": False},
            "broker_contract": {},
            "broker_preflight": {},
            "initial_kill_switch_hold": {},
        },
    )
    monkeypatch.setattr(preflight, "live_execution_disabled", lambda: False)
    monkeypatch.setattr(
        preflight,
        "prelive_reconcile_policy_snapshot",
        lambda **_kwargs: {"ok": True, "required": True, "blockers": []},
    )
    monkeypatch.setattr(
        preflight,
        "position_reconcile_evidence_snapshot",
        lambda **_kwargs: {"ok": True, "required": True, "blockers": []},
    )
    monkeypatch.setattr(
        preflight,
        "backup_restore_evidence_snapshot",
        lambda **_kwargs: {"ok": True, "required": True, "blockers": []},
    )
    monkeypatch.setattr(
        preflight,
        "_execution_arming_audit_snapshot",
        lambda **_kwargs: {"ok": True, "required": True, "blockers": []},
    )
    monkeypatch.setattr(
        preflight,
        "live_ai_safety_snapshot",
        lambda **_kwargs: {"ok": True, "required": True, "blockers": []},
    )

    state = preflight.live_trading_preflight(engine_mode="live", execution_mode="live")

    assert state["ok"] is False
    assert state["options_instruments"]["required"] is True
    assert "options_live_greeks_gate_missing" in state["blockers"]
