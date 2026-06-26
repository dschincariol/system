from __future__ import annotations

import importlib
import os
import sys
import unittest
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


class FxBrokerRoutingTests(unittest.TestCase):
    def test_fx_capable_broker_detects_ibkr_alias(self) -> None:
        broker_router = _reload_router()

        self.assertEqual(broker_router._fx_capable_broker(["alpaca", "ib_gateway"]), "ibkr")
        self.assertIsNone(broker_router._fx_capable_broker(["alpaca", "sim"]))

    def test_fx_batch_prefers_ibkr_without_bypassing_gates(self) -> None:
        orders = [{"symbol": "EURUSD", "qty": 1.0, "side": "BUY"}]
        with patch.dict(os.environ, {"BROKER_FAILOVER": "alpaca,ibkr", "BROKER_ROUTER_RETRY_ATTEMPTS": "1"}, clear=False):
            broker_router = _reload_router()
            original_validate = broker_router.validate_live_failover_chain
            validate_chain = Mock(wraps=original_validate)
            execution_gate = Mock(return_value=None)
            real_gate = Mock(return_value=None)
            ibkr_apply = Mock(return_value={"ok": True, "status": "ibkr_ok"})
            alpaca_apply = Mock(side_effect=AssertionError("FX batch should prefer IBKR"))

            with ExitStack() as stack:
                _mute_router_side_effects(stack, broker_router)
                stack.enter_context(patch.object(broker_router, "validate_live_failover_chain", validate_chain))
                stack.enter_context(patch.object(broker_router, "_execution_gate_or_block", execution_gate))
                stack.enter_context(patch.object(broker_router, "_real_trading_gate_or_block", real_gate))
                stack.enter_context(patch.object(broker_router, "_ibkr_apply", ibkr_apply))
                stack.enter_context(patch.object(broker_router, "_alpaca_apply", alpaca_apply))

                result = broker_router.apply_new_portfolio_orders_router(dry_run=True, override_orders=orders)

        self.assertTrue(bool(result["ok"]))
        self.assertEqual(result["broker"], "ibkr")
        self.assertEqual(result["failover_attempts"][0]["broker"], "ibkr")
        validate_chain.assert_called_once()
        execution_gate.assert_called_once()
        real_gate.assert_called()
        ibkr_apply.assert_called_once()
        alpaca_apply.assert_not_called()

    def test_equity_batch_keeps_configured_order(self) -> None:
        orders = [{"symbol": "AAPL", "qty": 1.0, "side": "BUY"}]
        with patch.dict(os.environ, {"BROKER_FAILOVER": "alpaca,ibkr", "BROKER_ROUTER_RETRY_ATTEMPTS": "1"}, clear=False):
            broker_router = _reload_router()
            alpaca_apply = Mock(return_value={"ok": True, "status": "alpaca_ok"})
            ibkr_apply = Mock(side_effect=AssertionError("equity batch should keep Alpaca first"))

            with ExitStack() as stack:
                _mute_router_side_effects(stack, broker_router)
                stack.enter_context(patch.object(broker_router, "_execution_gate_or_block", Mock(return_value=None)))
                stack.enter_context(patch.object(broker_router, "_real_trading_gate_or_block", Mock(return_value=None)))
                stack.enter_context(patch.object(broker_router, "_alpaca_apply", alpaca_apply))
                stack.enter_context(patch.object(broker_router, "_ibkr_apply", ibkr_apply))

                result = broker_router.apply_new_portfolio_orders_router(dry_run=True, override_orders=orders)

        self.assertTrue(bool(result["ok"]))
        self.assertEqual(result["broker"], "alpaca")
        self.assertEqual(result["failover_attempts"][0]["broker"], "alpaca")
        alpaca_apply.assert_called_once()
        ibkr_apply.assert_not_called()

    def test_fx_batch_without_fx_capable_broker_fails_closed(self) -> None:
        orders = [{"symbol": "EURUSD", "qty": 1.0, "side": "BUY"}]
        with patch.dict(os.environ, {"BROKER_FAILOVER": "alpaca", "BROKER_ROUTER_RETRY_ATTEMPTS": "1"}, clear=False):
            broker_router = _reload_router()
            execution_gate = Mock(return_value=None)
            real_gate = Mock(side_effect=AssertionError("no broker-specific real gate exists when FX broker is absent"))
            alpaca_apply = Mock(return_value={"ok": True, "status": "alpaca_ok"})

            with ExitStack() as stack:
                _mute_router_side_effects(stack, broker_router)
                stack.enter_context(patch.object(broker_router, "_execution_gate_or_block", execution_gate))
                stack.enter_context(patch.object(broker_router, "_real_trading_gate_or_block", real_gate))
                stack.enter_context(patch.object(broker_router, "_alpaca_apply", alpaca_apply))

                result = broker_router.apply_new_portfolio_orders_router(dry_run=True, override_orders=orders)

        self.assertFalse(bool(result["ok"]))
        self.assertEqual(result["status"], "fx_broker_unavailable")
        self.assertEqual(result["broker"], "failover_chain")
        self.assertTrue(bool(result["stop_failover"]))
        self.assertEqual(result["failover_attempts"], [])
        self.assertEqual(result["required_broker"], "ibkr")
        execution_gate.assert_called_once()
        real_gate.assert_not_called()
        alpaca_apply.assert_not_called()

    def test_fx_live_ibkr_route_blocks_when_fx_live_flag_unset(self) -> None:
        orders = [{"symbol": "EURUSD", "qty": 1.0, "side": "BUY"}]
        with patch.dict(os.environ, _live_ibkr_env(DISABLE_LIVE_EXECUTION="0"), clear=False):
            os.environ.pop("FX_LIVE_TRADING_ENABLED", None)
            broker_router = _reload_router()
            ibkr_apply = Mock(side_effect=AssertionError("FX live adapter must not run when FX live is disabled"))

            with ExitStack() as stack:
                _mute_router_side_effects(stack, broker_router)
                stack.enter_context(patch.object(broker_router, "live_broker_environment_contract", Mock(return_value={"ok": True})))
                stack.enter_context(patch.object(broker_router, "_execution_gate_or_block", Mock(return_value=None)))
                stack.enter_context(patch.object(broker_router, "live_options_order_block", Mock(return_value=None)))
                stack.enter_context(patch.object(broker_router, "_ibkr_apply", ibkr_apply))

                result = broker_router.apply_new_portfolio_orders_router(dry_run=False, override_orders=orders)

        self.assertFalse(bool(result["ok"]))
        self.assertEqual(result["status"], "fx_live_trading_disabled_by_default")
        self.assertEqual(result["reason"], "fx_live_trading_disabled_by_default")
        self.assertEqual(result["broker"], "failover_chain")
        self.assertTrue(bool(result["stop_failover"]))
        self.assertEqual(result["env"]["FX_LIVE_TRADING_ENABLED"], "0")
        ibkr_apply.assert_not_called()

    def test_fx_live_ibkr_route_proceeds_when_fx_live_flag_enabled(self) -> None:
        orders = [{"symbol": "EURUSD", "qty": 1.0, "side": "BUY"}]
        with patch.dict(
            os.environ,
            _live_ibkr_env(DISABLE_LIVE_EXECUTION="0", FX_LIVE_TRADING_ENABLED="1"),
            clear=False,
        ):
            broker_router = _reload_router()
            ibkr_apply = Mock(return_value={"ok": True, "status": "ibkr_ok"})

            with ExitStack() as stack:
                _mute_router_side_effects(stack, broker_router)
                stack.enter_context(patch.object(broker_router, "live_broker_environment_contract", Mock(return_value={"ok": True})))
                stack.enter_context(patch.object(broker_router, "_execution_gate_or_block", Mock(return_value=None)))
                stack.enter_context(patch.object(broker_router, "_real_trading_gate_or_block", Mock(return_value=None)))
                stack.enter_context(patch.object(broker_router, "live_broker_mode_boundary_block", Mock(return_value=None)))
                stack.enter_context(patch.object(broker_router, "live_options_order_block", Mock(return_value=None)))
                stack.enter_context(patch.object(broker_router, "_prelive_reconcile_or_block", Mock(return_value=None)))
                stack.enter_context(patch.object(broker_router, "_adaptive_execute_orders", Mock(return_value={"status": "adaptive_disabled"})))
                stack.enter_context(patch.object(broker_router, "_apply_batch_entry_delay", Mock(return_value=0)))
                stack.enter_context(patch.object(broker_router, "_ibkr_apply", ibkr_apply))

                result = broker_router.apply_new_portfolio_orders_router(dry_run=False, override_orders=orders)

        self.assertTrue(bool(result["ok"]))
        self.assertEqual(result["status"], "ibkr_ok")
        self.assertEqual(result["broker"], "ibkr")
        ibkr_apply.assert_called_once()
