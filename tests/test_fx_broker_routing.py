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

    def test_fx_batch_without_fx_capable_broker_falls_back_safely(self) -> None:
        orders = [{"symbol": "EURUSD", "qty": 1.0, "side": "BUY"}]
        with patch.dict(os.environ, {"BROKER_FAILOVER": "alpaca", "BROKER_ROUTER_RETRY_ATTEMPTS": "1"}, clear=False):
            broker_router = _reload_router()
            alpaca_apply = Mock(return_value={"ok": True, "status": "alpaca_ok"})

            with ExitStack() as stack:
                _mute_router_side_effects(stack, broker_router)
                stack.enter_context(patch.object(broker_router, "_execution_gate_or_block", Mock(return_value=None)))
                stack.enter_context(patch.object(broker_router, "_real_trading_gate_or_block", Mock(return_value=None)))
                stack.enter_context(patch.object(broker_router, "_alpaca_apply", alpaca_apply))

                result = broker_router.apply_new_portfolio_orders_router(dry_run=True, override_orders=orders)

        self.assertTrue(bool(result["ok"]))
        self.assertEqual(result["broker"], "alpaca")
        self.assertEqual(result["failover_attempts"][0]["broker"], "alpaca")
