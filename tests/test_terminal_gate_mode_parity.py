from __future__ import annotations

import time
from typing import Any
from urllib.parse import urlparse

import pytest

from engine.api import api_system
from engine.runtime import gates
from engine.runtime.live_execution_control import DISABLE_LIVE_EXECUTION_REASON
from engine.terminal.api import api_terminal_orders as terminal_orders


pytestmark = pytest.mark.safety_critical


def _stale_live_mode() -> dict[str, Any]:
    return {"mode": "live", "armed": 1, "source": "test_execution_mode_db"}


def _fresh_kill_switches() -> dict[str, Any]:
    return {
        "state": [],
        "loaded_ts_ms": int(time.time() * 1000),
        "max_age_ms": 60_000,
    }


def _risk_state(key: str, default: Any = "") -> Any:
    if key == "portfolio_risk_block":
        return "0"
    if key == "portfolio_risk_ts_ms":
        return "0"
    return default


def _quiet_degraded_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    empty = {
        "source": "test",
        "active": False,
        "severity": "WARNING",
        "reason": "",
        "reason_codes": [],
        "detail": {},
    }
    monkeypatch.setattr(gates, "_kill_switch_activation_failure_degraded_snapshot", lambda: dict(empty))
    monkeypatch.setattr(gates, "_event_bus_execution_degraded_snapshot", lambda: dict(empty))


def _set_mode(monkeypatch: pytest.MonkeyPatch, mode: str, *, disable_live_execution: str = "1") -> None:
    monkeypatch.setenv("ENGINE_MODE", mode)
    monkeypatch.setenv("EXECUTION_MODE", mode)
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", disable_live_execution)
    monkeypatch.delenv("KILL_SWITCH_GLOBAL", raising=False)
    monkeypatch.delenv("TRADING_KILL_SWITCH", raising=False)
    monkeypatch.delenv("KILL_SWITCH", raising=False)


def _execution_barrier_for_mode(mode_state_fn) -> dict[str, Any]:
    return gates.execution_gate_snapshot(
        get_execution_mode_fn=mode_state_fn,
        system_state={"state": "LIVE", "gate_severity": "WARNING"},
        kill_switches=_fresh_kill_switches(),
        risk_state_getter=_risk_state,
    )


def test_safe_terminal_order_reports_safe_mode_and_still_blocks(monkeypatch):
    _set_mode(monkeypatch, "safe")
    monkeypatch.setattr(terminal_orders, "_get_execution_mode", _stale_live_mode)

    result = terminal_orders.api_post_terminal_order(
        urlparse("/api/terminal/order"),
        {"symbol": "SPY", "side": "BUY", "qty": 1},
        {},
    )

    assert result["ok"] is False
    assert result["http_status"] == 403
    assert result["reason_code"] == DISABLE_LIVE_EXECUTION_REASON
    assert result["gate"]["mode"] == "safe"
    assert result["gate"]["reason"] == DISABLE_LIVE_EXECUTION_REASON
    assert result["gate"]["real_trading_allowed"] is False
    assert result["gate"]["allowed"] is False


@pytest.mark.parametrize("mode", ["safe", "shadow", "paper"])
def test_terminal_gate_mode_matches_execution_barrier_mode(monkeypatch, mode):
    _set_mode(monkeypatch, mode)
    _quiet_degraded_sources(monkeypatch)
    monkeypatch.setattr(terminal_orders, "_get_execution_mode", _stale_live_mode)

    terminal_response = terminal_orders._disabled_live_execution_response()
    assert terminal_response is not None
    terminal_gate = dict(terminal_response["gate"])

    barrier = _execution_barrier_for_mode(_stale_live_mode)
    monkeypatch.setattr(
        api_system,
        "_build_system_state_snapshot",
        lambda *_args, **_kwargs: {
            "ok": bool(barrier.get("allowed")),
            "status": "LIVE",
            "state": "LIVE",
            "mode": str(barrier.get("mode") or "unknown"),
            "execution_mode": str(barrier.get("mode") or "unknown"),
            "execution_allowed": bool(barrier.get("allowed")),
            "reasons": [barrier.get("reason")],
            "timestamps": {"ts_ms": barrier.get("ts_ms")},
            "ts_ms": barrier.get("ts_ms"),
            "execution_barrier": dict(barrier),
        },
    )
    api_barrier = api_system.api_get_execution_barrier({}, {})

    assert terminal_gate["mode"] == mode
    assert api_barrier["execution_barrier"]["mode"] == mode
    assert terminal_gate["mode"] == api_barrier["execution_barrier"]["mode"]
    assert terminal_gate["reason"] == DISABLE_LIVE_EXECUTION_REASON


def test_unknown_terminal_execution_mode_fails_closed(monkeypatch):
    monkeypatch.setenv("ENGINE_MODE", "not-a-real-mode")
    monkeypatch.delenv("EXECUTION_MODE", raising=False)
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.delenv("KILL_SWITCH_GLOBAL", raising=False)
    monkeypatch.delenv("TRADING_KILL_SWITCH", raising=False)
    monkeypatch.delenv("KILL_SWITCH", raising=False)
    monkeypatch.setattr(terminal_orders, "_get_execution_mode", _stale_live_mode)
    monkeypatch.setattr(terminal_orders, "_kill_switch_snapshot", _fresh_kill_switches)

    result = terminal_orders.api_post_terminal_order(
        urlparse("/api/terminal/order"),
        {"symbol": "SPY", "side": "BUY", "qty": 1},
        {},
    )

    assert result["ok"] is False
    assert result["http_status"] == 403
    assert result["reason_code"] == "invalid_execution_mode"
    assert result["gate"]["mode"] == "safe"
    assert result["gate"]["allowed"] is False
    assert result["gate"]["real_trading_allowed"] is False
