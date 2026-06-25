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


def test_ibkr_futures_contract_builds_future_and_contfuture_without_secret_leakage() -> None:
    gateway = importlib.reload(importlib.import_module("engine.execution.broker_ibkr_gateway"))
    canary = "CANARY_FUTURES_IBKR_SECRET_DO_NOT_LEAK"

    dated = gateway._mk_futures_contract("ESZ26")
    continuous = gateway._mk_contract_for_symbol("ES.c.0")
    order = gateway._mk_market_order(2)
    order_ref = gateway.validate_ibkr_order_ref("fut_test_order_123")

    assert dated.secType == "FUT"
    assert dated.symbol == "ES"
    assert dated.exchange == "CME"
    assert dated.currency == "USD"
    assert dated.lastTradeDateOrContractMonth == "202612"
    assert continuous.secType == "CONTFUT"
    assert continuous.symbol == "ES"
    assert continuous.exchange == "CME"
    assert float(order.totalQuantity) == 2.0
    assert order.action == "BUY"
    assert order_ref == "fut_test_order_123"

    payload = repr(vars(dated)) + repr(vars(continuous)) + repr(vars(order)) + order_ref
    assert canary not in payload


def test_futures_batch_prefers_ibkr_route_in_dry_run() -> None:
    orders = [{"symbol": "ES.c.0", "qty": 1, "side": "BUY"}]
    with patch.dict(os.environ, {"BROKER_FAILOVER": "alpaca,ibkr", "BROKER_ROUTER_RETRY_ATTEMPTS": "1"}, clear=False):
        broker_router = importlib.reload(importlib.import_module("engine.execution.broker_router"))
        ibkr_apply = Mock(return_value={"ok": True, "status": "ibkr_futures_preview"})
        alpaca_apply = Mock(side_effect=AssertionError("futures batch should prefer IBKR"))

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
            stack.enter_context(patch.object(broker_router, "_execution_gate_or_block", Mock(return_value=None)))
            stack.enter_context(patch.object(broker_router, "_real_trading_gate_or_block", Mock(return_value=None)))
            stack.enter_context(patch.object(broker_router, "_ibkr_apply", ibkr_apply))
            stack.enter_context(patch.object(broker_router, "_alpaca_apply", alpaca_apply))

            result = broker_router.apply_new_portfolio_orders_router(dry_run=True, override_orders=orders)

    assert result["ok"] is True
    assert result["broker"] == "ibkr"
    assert result["failover_attempts"][0]["broker"] == "ibkr"
    ibkr_apply.assert_called_once()
    alpaca_apply.assert_not_called()
