from __future__ import annotations

import importlib
import os
import sys
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

pytestmark = pytest.mark.safety_critical


def _reload_router():
    return importlib.reload(importlib.import_module("engine.execution.broker_router"))


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
        "IBKR_HOST": "127.0.0.1",
        "IBKR_PORT": "7497",
        "IBKR_CLIENT_ID": "42",
    }
    env.update(extra)
    return env


def test_crypto_live_route_blocks_when_crypto_live_flag_unset() -> None:
    orders = [{"symbol": "BTC", "qty": 0.01, "side": "BUY"}]
    with patch.dict(os.environ, _live_ibkr_env(DISABLE_LIVE_EXECUTION="0"), clear=False):
        os.environ.pop("CRYPTO_LIVE_TRADING_ENABLED", None)
        broker_router = _reload_router()
        apply_one = Mock(side_effect=AssertionError("crypto live block must run before broker attempts"))

        with ExitStack() as stack:
            _mute_router_side_effects(stack, broker_router)
            stack.enter_context(patch.object(broker_router, "live_broker_environment_contract", Mock(return_value={"ok": True})))
            stack.enter_context(patch.object(broker_router, "_execution_gate_or_block", Mock(return_value=None)))
            stack.enter_context(patch.object(broker_router, "live_options_order_block", Mock(return_value=None)))
            stack.enter_context(patch.object(broker_router, "_apply_one", apply_one))

            result = broker_router.apply_new_portfolio_orders_router(dry_run=False, override_orders=orders)

    assert result["ok"] is False
    assert result["status"] == "crypto_live_disabled"
    assert result["reason"] == "crypto_live_trading_disabled_by_default"
    assert result["broker"] == "failover_chain"
    assert result["stop_failover"] is True
    assert result["retryable"] is False
    assert result["failover_attempts"] == []
    assert result["env"]["CRYPTO_LIVE_TRADING_ENABLED"] == "0"
    apply_one.assert_not_called()


def test_crypto_live_route_proceeds_when_crypto_live_flag_enabled() -> None:
    orders = [{"symbol": "BTC", "qty": 0.01, "side": "BUY"}]
    with patch.dict(
        os.environ,
        _live_ibkr_env(DISABLE_LIVE_EXECUTION="0", CRYPTO_LIVE_TRADING_ENABLED="1"),
        clear=False,
    ):
        broker_router = _reload_router()
        apply_one = Mock(return_value={"ok": True, "status": "ibkr_ok", "broker": "ibkr"})

        with ExitStack() as stack:
            _mute_router_side_effects(stack, broker_router)
            stack.enter_context(patch.object(broker_router, "live_broker_environment_contract", Mock(return_value={"ok": True})))
            stack.enter_context(patch.object(broker_router, "_execution_gate_or_block", Mock(return_value=None)))
            stack.enter_context(patch.object(broker_router, "live_options_order_block", Mock(return_value=None)))
            stack.enter_context(patch.object(broker_router, "_apply_one", apply_one))

            result = broker_router.apply_new_portfolio_orders_router(dry_run=False, override_orders=orders)

    assert result["ok"] is True
    assert result["status"] != "crypto_live_disabled"
    assert result["status"] == "ibkr_ok"
    assert result["broker"] == "ibkr"
    assert result["failover_attempts"][0]["broker"] == "ibkr"
    apply_one.assert_called_once()


def test_crypto_live_safety_block_does_not_block_dry_run() -> None:
    with patch.dict(os.environ, {"CRYPTO_LIVE_TRADING_ENABLED": "0"}, clear=False):
        broker_router = _reload_router()

        result = broker_router._crypto_order_safety_block(
            [{"symbol": "BTC", "qty": 0.01, "side": "BUY"}],
            dry_run=True,
            chain=["ibkr"],
        )

    assert result is None


def test_crypto_live_safety_block_ignores_non_crypto_batches() -> None:
    with patch.dict(os.environ, {"CRYPTO_LIVE_TRADING_ENABLED": "0"}, clear=False):
        broker_router = _reload_router()

        result = broker_router._crypto_order_safety_block(
            [{"symbol": "AAPL", "qty": 1.0, "side": "BUY"}],
            dry_run=False,
            chain=["ibkr"],
        )

    assert result is None
