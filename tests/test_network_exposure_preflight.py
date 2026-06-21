from __future__ import annotations

import importlib
import os
from unittest.mock import patch

from engine.runtime.live_trading_preflight import (
    DEFAULT_PUBLIC_NETWORK_EXPOSURE_ACK_PHRASE,
    public_network_exposure_snapshot,
)


def _approved_exposure_env() -> dict[str, str]:
    return {
        "TRADING_PUBLIC_NETWORK_EXPOSURE_ACK": DEFAULT_PUBLIC_NETWORK_EXPOSURE_ACK_PHRASE,
        "TRADING_PUBLIC_NETWORK_EXPOSURE_OWNER": "ops@example.com",
        "TRADING_PUBLIC_NETWORK_EXPOSURE_REASON": "VPN-only maintenance window approved by change ticket CHG-1234",
    }


def test_public_network_exposure_defaults_to_loopback_with_container_internal_dashboard() -> None:
    state = public_network_exposure_snapshot(
        engine_mode="live",
        environ={
            "ENGINE_MODE": "live",
            "EXECUTION_MODE": "live",
            "ENV": "prod",
            "PROD_LOCK": "1",
            "DASHBOARD_HOST": "0.0.0.0",
            "DASHBOARD_BIND_CONTEXT": "container_internal",
        },
    )

    assert state["ok"] is True
    assert state["required"] is True
    assert state["public_services"] == []
    assert state["dashboard_process_bind"]["container_internal"] is True
    assert all(not item["public_host_bind"] for item in state["services"])


def test_public_network_exposure_rejects_timescale_wildcard_bind_in_live_without_ack() -> None:
    state = public_network_exposure_snapshot(
        engine_mode="live",
        environ={
            "ENGINE_MODE": "live",
            "EXECUTION_MODE": "live",
            "TIMESCALE_DANGEROUS_PUBLIC_BIND_HOST": "0.0.0.0",
            "TIMESCALE_PORT": "5432",
        },
    )

    assert state["ok"] is False
    assert "timescale_0_0_0_0_without_approved_exposure" in state["blockers"]
    assert "TIMESCALE_ALLOW_DANGEROUS_PUBLIC_BIND_required" in state["blockers"]
    assert "TRADING_PUBLIC_NETWORK_EXPOSURE_ACK_required" in state["blockers"]


def test_public_network_exposure_accepts_wildcard_bind_only_with_ack_and_service_allow() -> None:
    env = {
        "ENGINE_MODE": "live",
        "EXECUTION_MODE": "live",
        "TIMESCALE_DANGEROUS_PUBLIC_BIND_HOST": "0.0.0.0",
        "TIMESCALE_PORT": "5432",
        "TIMESCALE_ALLOW_DANGEROUS_PUBLIC_BIND": "1",
        **_approved_exposure_env(),
    }

    state = public_network_exposure_snapshot(engine_mode="live", environ=env)

    assert state["ok"] is True
    assert state["public_services"] == ["timescale"]
    timescale = next(item for item in state["services"] if item["service"] == "timescale")
    assert timescale["approved"] is True
    assert timescale["wildcard"] is True


def test_public_network_exposure_rejects_dashboard_host_process_wildcard_bind() -> None:
    state = public_network_exposure_snapshot(
        engine_mode="live",
        environ={
            "ENGINE_MODE": "live",
            "EXECUTION_MODE": "live",
            "DASHBOARD_HOST": "0.0.0.0",
        },
    )

    assert state["ok"] is False
    assert "dashboard_process_bind_without_approved_exposure" in state["blockers"]
    assert "dashboard_0_0_0_0_without_approved_exposure" in state["blockers"]


def test_prod_preflight_network_exposure_gate_fails_live_wildcard_bind() -> None:
    import engine.runtime.prod_preflight as prod_preflight

    prod_preflight = importlib.reload(prod_preflight)
    with patch.dict(
        os.environ,
        {
            "ENGINE_MODE": "live",
            "EXECUTION_MODE": "live",
            "REDIS_DANGEROUS_PUBLIC_BIND_HOST": "0.0.0.0",
            "REDIS_PORT": "6379",
        },
        clear=True,
    ):
        notes, warnings, errors, state = prod_preflight._network_exposure_gate()

    assert notes == []
    assert warnings == []
    assert errors
    assert "redis_0_0_0_0_without_approved_exposure" in errors[0]
    assert state["ok"] is False
