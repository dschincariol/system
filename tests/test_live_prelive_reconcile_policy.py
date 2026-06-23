from __future__ import annotations

import importlib
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

pytestmark = pytest.mark.safety_critical


def _live_disabled_reconcile_env() -> dict[str, str]:
    return {
        "ENGINE_MODE": "live",
        "DISABLE_LIVE_EXECUTION": "0",
        "EXECUTION_PRELIVE_RECONCILE": "0",
        "EXECUTION_PRELIVE_RECONCILE_BREAK_GLASS": "0",
        "EXECUTION_PRELIVE_RECONCILE_BREAK_GLASS_ACTOR": "",
        "EXECUTION_PRELIVE_RECONCILE_BREAK_GLASS_REASON": "",
    }


def test_prelive_break_glass_policy_records_audit_event(monkeypatch):
    monkeypatch.setenv("ENGINE_MODE", "live")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE_BREAK_GLASS", "1")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE_BREAK_GLASS_ACTOR", "ops@example.com")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE_BREAK_GLASS_REASON", "temporary audited incident response override")

    from engine.runtime import event_log
    from engine.runtime.live_execution_control import prelive_reconcile_policy_gate

    calls: list[dict] = []

    def _append_event(**kwargs):
        calls.append(dict(kwargs))
        return 123

    monkeypatch.setattr(event_log, "append_event", _append_event)

    block = prelive_reconcile_policy_gate(
        source="unit.test",
        engine_mode="live",
        broker="alpaca",
        audit_override=True,
        correlation_id="corr-1",
    )

    assert block is None
    assert len(calls) == 1
    assert calls[0]["event_type"] == "prelive_reconcile_break_glass"
    assert calls[0]["entity_id"] == "EXECUTION_PRELIVE_RECONCILE"
    assert calls[0]["payload"]["policy"]["audit"]["actor"] == "ops@example.com"
    assert calls[0]["correlation_id"] == "corr-1"


def test_alpaca_direct_submit_blocks_when_prelive_reconcile_disabled(monkeypatch):
    for key, value in _live_disabled_reconcile_env().items():
        monkeypatch.setenv(key, value)

    from engine.execution import broker_alpaca_rest

    broker_alpaca_rest = importlib.reload(broker_alpaca_rest)
    reconcile = Mock(side_effect=AssertionError("pre-live reconcile should not run after policy block"))
    audit = Mock(side_effect=AssertionError("broker audit should not run after policy block"))

    monkeypatch.setattr(
        broker_alpaca_rest,
        "_real_trading_gate",
        lambda: {"ok": True, "real_trading_allowed": True},
    )
    monkeypatch.setattr(broker_alpaca_rest, "_prelive_reconcile", reconcile)
    monkeypatch.setattr(broker_alpaca_rest, "record_broker_action_audit", audit)

    result = broker_alpaca_rest.submit_market_order("AAPL", 1.0, "cid-1")

    assert result["ok"] is False
    assert result["status"] == "prelive_reconcile_disabled_for_live"
    assert result["fatal_reconcile"] is True
    assert result["prelive_reconcile_policy"]["enabled"] is False
    reconcile.assert_not_called()
    audit.assert_not_called()


def test_alpaca_live_apply_missing_credentials_is_terminal(monkeypatch):
    monkeypatch.setenv("ENGINE_MODE", "live")
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.delenv("ALPACA_KEY_ID", raising=False)
    monkeypatch.delenv("ALPACA_SECRET_KEY", raising=False)

    from engine.execution import broker_alpaca_rest

    broker_alpaca_rest = importlib.reload(broker_alpaca_rest)
    monkeypatch.setattr(
        broker_alpaca_rest,
        "_real_trading_gate",
        lambda: {"ok": True, "real_trading_allowed": True},
    )

    result = broker_alpaca_rest.apply_latest_portfolio_orders_live(
        dry_run=False,
        override_orders=[],
    )

    assert result["ok"] is False
    assert result["status"] == "missing_credentials"
    assert result["broker"] == "alpaca"
    assert result["retryable"] is False
    assert result["stop_failover"] is True
    assert "ALPACA_KEY_ID" in result["credentials"]["missing"]
    assert "ALPACA_SECRET_KEY" in result["credentials"]["missing"]


def test_ibkr_direct_submit_blocks_when_prelive_reconcile_disabled(monkeypatch):
    for key, value in _live_disabled_reconcile_env().items():
        monkeypatch.setenv(key, value)

    from engine.execution import broker_ibkr_gateway

    broker_ibkr_gateway = importlib.reload(broker_ibkr_gateway)
    reconcile = Mock(side_effect=AssertionError("pre-live reconcile should not run after policy block"))
    audit = Mock(side_effect=AssertionError("broker audit should not run after policy block"))

    monkeypatch.setattr(
        broker_ibkr_gateway,
        "_real_trading_gate",
        lambda: {"ok": True, "real_trading_allowed": True},
    )
    monkeypatch.setattr(broker_ibkr_gateway, "_prelive_reconcile", reconcile)
    monkeypatch.setattr(broker_ibkr_gateway, "record_broker_action_audit", audit)

    result = broker_ibkr_gateway.submit_market_order("AAPL", 1.0, "cid-1")

    assert result["ok"] is False
    assert result["status"] == "prelive_reconcile_disabled_for_live"
    assert result["fatal_reconcile"] is True
    assert result["prelive_reconcile_policy"]["enabled"] is False
    reconcile.assert_not_called()
    audit.assert_not_called()


def test_ibkr_live_apply_missing_config_is_terminal(monkeypatch):
    monkeypatch.setenv("ENGINE_MODE", "live")
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.delenv("IBKR_HOST", raising=False)
    monkeypatch.delenv("IBKR_PORT", raising=False)
    monkeypatch.delenv("IBKR_CLIENT_ID", raising=False)

    from engine.execution import broker_ibkr_gateway

    broker_ibkr_gateway = importlib.reload(broker_ibkr_gateway)
    monkeypatch.setattr(
        broker_ibkr_gateway,
        "_real_trading_gate",
        lambda: {"ok": True, "real_trading_allowed": True},
    )

    result = broker_ibkr_gateway.apply_latest_portfolio_orders_live(
        dry_run=False,
        override_orders=[],
    )

    assert result["ok"] is False
    assert result["status"] == "missing_credentials"
    assert result["broker"] == "ibkr"
    assert result["retryable"] is False
    assert result["stop_failover"] is True
    assert result["credentials"]["required_explicit"] is True
    assert set(result["credentials"]["missing"]) == {"IBKR_HOST", "IBKR_PORT", "IBKR_CLIENT_ID"}
