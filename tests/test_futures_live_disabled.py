from __future__ import annotations

import importlib
import os
import sys
from contextlib import ExitStack
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

pytestmark = pytest.mark.safety_critical

CT = ZoneInfo("America/Chicago")


def _reload_router():
    return importlib.reload(importlib.import_module("engine.execution.broker_router"))


def _ms_ct(year: int, month: int, day: int, hour: int, minute: int = 0) -> int:
    return int(datetime(year, month, day, hour, minute, tzinfo=CT).timestamp() * 1000)


def _mute_router_side_effects(stack: ExitStack, broker_router) -> None:
    for attr in (
        "emit_counter",
        "emit_timing",
        "record_rolling_rate",
        "record_component_health",
        "trace_event",
        "log_event",
    ):
        stack.enter_context(patch.object(broker_router, attr, return_value=None))


def _live_ibkr_env(**extra: str) -> dict[str, str]:
    env = {
        "ENGINE_MODE": "live",
        "EXECUTION_MODE": "live",
        "BROKER_FAILOVER": "ibkr",
        "BROKER_ROUTER_RETRY_ATTEMPTS": "1",
        "LIVE_BROKER": "ibkr",
        "BROKER": "ibkr",
        "BROKER_NAME": "ibkr",
    }
    env.update(extra)
    return env


def test_futures_live_route_blocks_when_disable_live_execution_truthy() -> None:
    orders = [{"symbol": "ES.c.0", "qty": 1, "side": "BUY"}]
    with patch.dict(os.environ, _live_ibkr_env(DISABLE_LIVE_EXECUTION="yes"), clear=False):
        broker_router = _reload_router()
        ibkr_apply = Mock(side_effect=AssertionError("futures live adapter must not run when live execution is disabled"))

        with ExitStack() as stack:
            _mute_router_side_effects(stack, broker_router)
            stack.enter_context(patch.object(broker_router, "_ibkr_apply", ibkr_apply))
            result = broker_router.apply_new_portfolio_orders_router(
                dry_run=False,
                override_orders=orders,
                override_ts_ms=_ms_ct(2026, 1, 5, 15, 30),
            )

    assert result["ok"] is False
    assert result["status"] == "execution_blocked"
    assert result["gate"]["reason"] == "disable_live_execution_env"
    ibkr_apply.assert_not_called()


def test_futures_live_route_respects_live_mode_arming_boundary() -> None:
    orders = [{"symbol": "ES.c.0", "qty": 1, "side": "BUY"}]
    with patch.dict(
        os.environ,
        _live_ibkr_env(DISABLE_LIVE_EXECUTION="0", FUTURES_LIVE_TRADING_ENABLED="1"),
        clear=False,
    ):
        broker_router = _reload_router()
        gate_snapshot = Mock(return_value={"ok": True, "allowed": True, "real_trading_allowed": True})
        kill_switch_snapshot = Mock(return_value={"state": [], "loaded_ts_ms": int(_ms_ct(2026, 1, 5, 15, 30)), "max_age_ms": 60000})
        get_execution_mode = Mock(return_value={"mode": "live", "armed": 0, "source": "unit_test"})
        ibkr_apply = Mock(side_effect=AssertionError("futures live adapter must not run when live mode is unarmed"))

        with ExitStack() as stack:
            _mute_router_side_effects(stack, broker_router)
            stack.enter_context(patch.object(broker_router, "_execution_gate_snapshot", gate_snapshot))
            stack.enter_context(patch.object(broker_router, "_kill_switch_snapshot", kill_switch_snapshot))
            stack.enter_context(patch.object(broker_router, "_get_execution_mode", get_execution_mode))
            stack.enter_context(patch.object(broker_router, "_ibkr_apply", ibkr_apply))
            result = broker_router.apply_new_portfolio_orders_router(
                dry_run=False,
                override_orders=orders,
                override_ts_ms=_ms_ct(2026, 1, 5, 15, 30),
            )

    assert result["ok"] is False
    assert result["status"] == "execution_mode_blocked"
    assert result["reason"] == "live_not_armed"
    ibkr_apply.assert_not_called()
