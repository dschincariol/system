from __future__ import annotations

from engine.runtime.live_trading_preflight import (
    DEFAULT_LIVE_CONFIRM_PHRASE,
    live_trading_preflight,
)


def test_live_trading_preflight_requires_token_and_confirmation():
    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="",
        live_confirm="",
    )

    assert state["ok"] is False
    assert "dashboard_api_token_required_for_live" in state["blockers"]
    assert "live_trading_confirmation_required" in state["blockers"]


def test_live_trading_preflight_accepts_explicit_live_acknowledgement():
    state = live_trading_preflight(
        engine_mode="live",
        dashboard_host="127.0.0.1",
        dashboard_api_token="secret",
        live_confirm=DEFAULT_LIVE_CONFIRM_PHRASE,
    )

    assert state["ok"] is True
    assert state["blockers"] == []


def test_live_trading_preflight_requires_token_for_remote_bind_even_when_safe():
    state = live_trading_preflight(
        engine_mode="safe",
        dashboard_host="0.0.0.0",
        dashboard_api_token="",
        live_confirm="",
    )

    assert state["ok"] is False
    assert state["reason"] == "dashboard_api_token_required_for_remote_bind"
