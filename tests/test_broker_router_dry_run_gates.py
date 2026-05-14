from __future__ import annotations

import importlib
import os
import sys
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import Mock, patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def _sample_orders() -> list[dict]:
    return [{"symbol": "AAPL", "action": "BUY", "qty": 1.0}]


def _case_permutations(value: str) -> list[str]:
    variants = [""]
    for ch in value:
        if ch.isalpha():
            variants = [prefix + ch.lower() for prefix in variants] + [
                prefix + ch.upper() for prefix in variants
            ]
        else:
            variants = [prefix + ch for prefix in variants]
    return variants


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


class BrokerRouterDryRunGateTests(unittest.TestCase):
    def test_rl_source_markers_block_all_casing_permutations(self) -> None:
        (broker_router,) = _reload_modules("engine.execution.broker_router")

        for marker in _case_permutations("rl.wrapper") + _case_permutations("rl_wrapper"):
            orders = [
                {"symbol": "AAPL", "source": marker},
                {"symbol": "AAPL", "source": None, "order_source": marker},
                {"symbol": "AAPL", "source": "   ", "order_source": marker},
            ]
            for order in orders:
                result = broker_router._rl_source_block([order])
                self.assertIsNotNone(result, msg=f"marker was not blocked: {order!r}")
                self.assertFalse(bool(result["ok"]))
                self.assertEqual(str(result["status"]), "rl_source_forbidden")

    def test_dry_run_blocks_real_execution(self) -> None:
        orders = _sample_orders()
        env = {
            "BROKER_FAILOVER": "alpaca",
            "BROKER_ROUTER_RETRY_ATTEMPTS": "1",
        }

        with patch.dict(os.environ, env, clear=False):
            (broker_router,) = _reload_modules("engine.execution.broker_router")
            live_adapter = Mock(return_value={"ok": True, "status": "dry_run_preview"})
            gate_snapshot = Mock(side_effect=AssertionError("execution gate should be bypassed in dry run"))
            kill_switch_snapshot = Mock(side_effect=AssertionError("kill switch lookup should be bypassed in dry run"))
            get_execution_mode = Mock(side_effect=AssertionError("execution mode lookup should be bypassed in dry run"))
            prelive_reconcile = Mock(side_effect=AssertionError("pre-live reconcile should be bypassed in dry run"))
            adaptive_execute = Mock(side_effect=AssertionError("adaptive live execution should be bypassed in dry run"))

            with ExitStack() as stack:
                _mute_router_side_effects(stack, broker_router)
                stack.enter_context(patch.object(broker_router, "_alpaca_apply", live_adapter))
                stack.enter_context(patch.object(broker_router, "_execution_gate_snapshot", gate_snapshot))
                stack.enter_context(patch.object(broker_router, "_kill_switch_snapshot", kill_switch_snapshot))
                stack.enter_context(patch.object(broker_router, "_get_execution_mode", get_execution_mode))
                stack.enter_context(patch.object(broker_router, "_prelive_reconcile", prelive_reconcile))
                stack.enter_context(patch.object(broker_router, "_adaptive_execute_orders", adaptive_execute))

                result = broker_router.apply_new_portfolio_orders_router(
                    dry_run=True,
                    override_orders=orders,
                    override_order_id=101,
                    override_ts_ms=1234,
                )

        self.assertTrue(bool(result["ok"]))
        self.assertEqual(result["status"], "dry_run_preview")
        self.assertEqual(result["broker"], "alpaca")
        self.assertEqual(result["failover_attempts"][0]["broker"], "alpaca")
        self.assertTrue(bool(live_adapter.call_args.kwargs["dry_run"]))
        self.assertEqual(live_adapter.call_args.kwargs["override_orders"], orders)
        live_adapter.assert_called_once()
        gate_snapshot.assert_not_called()
        kill_switch_snapshot.assert_not_called()
        get_execution_mode.assert_not_called()
        prelive_reconcile.assert_not_called()
        adaptive_execute.assert_not_called()

    def test_real_trading_gate_blocks_when_conditions_fail(self) -> None:
        orders = _sample_orders()
        gate = {
            "ok": True,
            "allowed": True,
            "real_trading_allowed": False,
            "reason": "unit_test_block",
        }
        env = {
            "BROKER_FAILOVER": "alpaca",
            "BROKER_ROUTER_RETRY_ATTEMPTS": "1",
        }

        with patch.dict(os.environ, env, clear=False):
            (broker_router,) = _reload_modules("engine.execution.broker_router")
            gate_snapshot = Mock(return_value=gate)
            kill_switch_snapshot = Mock(return_value={"global": False})
            get_execution_mode = Mock(return_value="live")
            live_adapter = Mock(side_effect=AssertionError("live adapter should not execute when real trading is blocked"))
            prelive_reconcile = Mock(side_effect=AssertionError("pre-live reconcile should not run when real trading is blocked"))

            with ExitStack() as stack:
                _mute_router_side_effects(stack, broker_router)
                stack.enter_context(patch.object(broker_router, "_execution_gate_snapshot", gate_snapshot))
                stack.enter_context(patch.object(broker_router, "_kill_switch_snapshot", kill_switch_snapshot))
                stack.enter_context(patch.object(broker_router, "_get_execution_mode", get_execution_mode))
                stack.enter_context(patch.object(broker_router, "_alpaca_apply", live_adapter))
                stack.enter_context(patch.object(broker_router, "_prelive_reconcile", prelive_reconcile))

                result = broker_router.apply_new_portfolio_orders_router(
                    dry_run=False,
                    override_orders=orders,
                )

        self.assertFalse(bool(result["ok"]))
        self.assertEqual(result["status"], "all_brokers_failed")
        self.assertEqual(len(result["failover_attempts"]), 1)
        self.assertEqual(result["failover_attempts"][0]["broker"], "alpaca")
        self.assertEqual(result["failover_attempts"][0]["status"], "real_trading_blocked")
        self.assertFalse(bool(result["failover_attempts"][0]["ok"]))
        self.assertEqual(gate_snapshot.call_count, 2)
        live_adapter.assert_not_called()
        prelive_reconcile.assert_not_called()

    def test_failover_path_triggers(self) -> None:
        orders = _sample_orders()
        call_sequence: list[tuple[str, bool]] = []
        env = {
            "BROKER_FAILOVER": "alpaca,sim",
            "BROKER_ROUTER_RETRY_ATTEMPTS": "1",
        }

        def _primary_apply(**kwargs):
            call_sequence.append(("alpaca", bool(kwargs["dry_run"])))
            return {"ok": False, "status": "temporary_failure"}

        def _fallback_apply(**kwargs):
            call_sequence.append(("sim", bool(kwargs["dry_run"])))
            return {"ok": True, "status": "fallback_ok"}

        with patch.dict(os.environ, env, clear=False):
            (broker_router,) = _reload_modules("engine.execution.broker_router")
            gate_snapshot = Mock(side_effect=AssertionError("execution gate should be bypassed in dry run failover"))
            prelive_reconcile = Mock(side_effect=AssertionError("pre-live reconcile should be bypassed in dry run failover"))

            with ExitStack() as stack:
                _mute_router_side_effects(stack, broker_router)
                stack.enter_context(patch.object(broker_router, "_execution_gate_snapshot", gate_snapshot))
                stack.enter_context(patch.object(broker_router, "_prelive_reconcile", prelive_reconcile))
                stack.enter_context(patch.object(broker_router, "_alpaca_apply", Mock(side_effect=_primary_apply)))
                stack.enter_context(patch.object(broker_router, "_sim_apply", Mock(side_effect=_fallback_apply)))

                result = broker_router.apply_new_portfolio_orders_router(
                    dry_run=True,
                    override_orders=orders,
                )

        self.assertTrue(bool(result["ok"]))
        self.assertEqual(result["broker"], "sim")
        self.assertEqual(
            call_sequence,
            [("alpaca", True), ("sim", True)],
        )
        self.assertEqual(
            [attempt["status"] for attempt in result["failover_attempts"]],
            ["temporary_failure", "fallback_ok"],
        )
        gate_snapshot.assert_not_called()
        prelive_reconcile.assert_not_called()

    def test_execution_gate_snapshot_used(self) -> None:
        orders = _sample_orders()
        gate = {
            "ok": True,
            "allowed": False,
            "real_trading_allowed": True,
            "reason": "snapshot_block",
        }
        env = {
            "BROKER_FAILOVER": "sim",
            "BROKER_ROUTER_RETRY_ATTEMPTS": "1",
        }

        with patch.dict(os.environ, env, clear=False):
            (broker_router,) = _reload_modules("engine.execution.broker_router")
            gate_snapshot = Mock(return_value=gate)
            kill_switch_snapshot = Mock(return_value={"global": False})
            get_execution_mode = Mock(return_value="live")
            sim_adapter = Mock(side_effect=AssertionError("broker adapter should not run when execution gate blocks"))

            with ExitStack() as stack:
                _mute_router_side_effects(stack, broker_router)
                stack.enter_context(patch.object(broker_router, "_execution_gate_snapshot", gate_snapshot))
                stack.enter_context(patch.object(broker_router, "_kill_switch_snapshot", kill_switch_snapshot))
                stack.enter_context(patch.object(broker_router, "_get_execution_mode", get_execution_mode))
                stack.enter_context(patch.object(broker_router, "_sim_apply", sim_adapter))

                result = broker_router.apply_new_portfolio_orders_router(
                    dry_run=False,
                    override_orders=orders,
                )

        self.assertFalse(bool(result["ok"]))
        self.assertEqual(result["status"], "execution_blocked")
        self.assertIs(result["gate"], gate)
        gate_snapshot.assert_called_once_with(
            get_execution_mode_fn=get_execution_mode,
            execution_degraded={"active": False, "detail": {}},
            kill_switches={"global": False},
        )
        sim_adapter.assert_not_called()


if __name__ == "__main__":
    unittest.main()
