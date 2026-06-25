from __future__ import annotations

import json
import os
import uuid
from unittest.mock import patch

import pytest

pytestmark = pytest.mark.safety_critical

SAFETY = {"engine_mode": "live", "strict_runtime": True}
BASE_LIVE_RISK_ENV = {
    "PORTFOLIO_USE_RISK_ENGINE": "1",
    "PORTFOLIO_RISK_USE_MONTE_CARLO": "1",
    "PORTFOLIO_RISK_MC_REQUIRED_IN_LIVE": "1",
    "MODEL_AWARE_KILL_SWITCH": "1",
    "PORTFOLIO_RISK_MC_VAR_95_BLOCK": "0.04",
    "PORTFOLIO_RISK_MC_VAR_99_BLOCK": "0.06",
    "PORTFOLIO_RISK_MC_CVAR_95_BLOCK": "0.05",
    "PORTFOLIO_RISK_MC_CVAR_99_BLOCK": "0.08",
    "PORTFOLIO_RISK_MC_DRAWDOWN_P95_BLOCK": "0.10",
    "PORTFOLIO_RISK_MC_WORST_DRAWDOWN_BLOCK": "0.16",
    "PORTFOLIO_RISK_VOL_HARD_BLOCK": "0.12",
    "KILL_SWITCH_MODEL_MAX_DRAWDOWN": "5000",
    "KILL_SWITCH_MODEL_MAX_CONSECUTIVE_LOSSES": "4",
}


def _validate(env: dict[str, str]):
    from engine.runtime import config_schema

    with patch.dict(os.environ, env, clear=True):
        return config_schema.validate_live_risk_thresholds(SAFETY)


def _snapshot(env: dict[str, str]):
    from engine.runtime import config_schema

    with patch.dict(os.environ, env, clear=True):
        return config_schema.live_risk_threshold_validation_snapshot(SAFETY)


def test_live_required_edge_filter_rejects_disabled_filter_without_secret_leak() -> None:
    from engine.runtime.config_schema import ConfigError

    canary = f"secret-canary-{uuid.uuid4()}"
    env = {
        **BASE_LIVE_RISK_ENV,
        "EQUITY_EXEC_COST_FILTER_REQUIRED_IN_LIVE": "1",
        "ALERT_USE_EXEC_COST_FILTER": "0",
        "ALERT_MIN_NET_ABS_Z": "0.05",
        "UNRELATED_SECRET_CANARY": canary,
    }

    with pytest.raises(ConfigError) as ctx:
        _validate(env)

    assert "ALERT_USE_EXEC_COST_FILTER disabled" in str(ctx.value)
    assert canary not in str(ctx.value)

    snapshot = _snapshot(env)
    assert "ALERT_USE_EXEC_COST_FILTER" in snapshot["required_enabled_flags"]
    assert canary not in json.dumps(snapshot, sort_keys=True)


def test_live_required_edge_filter_rejects_zero_min_net_threshold() -> None:
    from engine.runtime.config_schema import ConfigError

    env = {
        **BASE_LIVE_RISK_ENV,
        "EQUITY_EXEC_COST_FILTER_REQUIRED_IN_LIVE": "1",
        "ALERT_USE_EXEC_COST_FILTER": "1",
        "ALERT_MIN_NET_ABS_Z": "0",
    }

    with pytest.raises(ConfigError) as ctx:
        _validate(env)

    assert "ALERT_MIN_NET_ABS_Z must be > 0" in str(ctx.value)


def test_live_required_edge_filter_accepts_enabled_positive_threshold() -> None:
    env = {
        **BASE_LIVE_RISK_ENV,
        "EQUITY_EXEC_COST_FILTER_REQUIRED_IN_LIVE": "1",
        "ALERT_USE_EXEC_COST_FILTER": "1",
        "ALERT_MIN_NET_ABS_Z": "0.025",
    }

    snapshot = _validate(env)

    assert snapshot["ok"] is True
    assert snapshot["cost_filter_required"] is True
    assert "ALERT_USE_EXEC_COST_FILTER" in snapshot["required_enabled_flags"]
    assert "ALERT_MIN_NET_ABS_Z" in snapshot["required_thresholds"]


def test_live_required_edge_filter_uses_existing_acceptance_override() -> None:
    env = {
        **BASE_LIVE_RISK_ENV,
        "EQUITY_EXEC_COST_FILTER_REQUIRED_IN_LIVE": "1",
        "ALERT_USE_EXEC_COST_FILTER": "0",
        "ALERT_MIN_NET_ABS_Z": "0",
        "LIVE_RISK_THRESHOLD_ACCEPTANCE_OVERRIDE": "1",
        "LIVE_RISK_THRESHOLD_ACCEPTANCE_ID": "risk-acceptance-123",
        "LIVE_RISK_THRESHOLD_ACCEPTANCE_OWNER": "risk-owner",
        "LIVE_RISK_THRESHOLD_ACCEPTANCE_REASON": "paper calibration rollout pending",
    }

    snapshot = _validate(env)

    assert snapshot["ok"] is True
    assert snapshot["override"] is True
    assert any("ALERT_USE_EXEC_COST_FILTER disabled" == issue for issue in snapshot["issues"])
    assert any("ALERT_MIN_NET_ABS_Z must be > 0" == issue for issue in snapshot["issues"])


def test_edge_filter_live_requirement_default_off_is_superset() -> None:
    snapshot = _validate(dict(BASE_LIVE_RISK_ENV))

    assert snapshot["ok"] is True
    assert snapshot["cost_filter_required"] is False
    assert "ALERT_USE_EXEC_COST_FILTER" not in snapshot["required_enabled_flags"]
    assert "ALERT_MIN_NET_ABS_Z" not in snapshot["required_thresholds"]
