from __future__ import annotations

import importlib
import os
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class EventRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "event_runtime.db")
        os.environ["ENV"] = "test"
        os.environ["EVENT_RUNTIME_ENABLED"] = "1"
        os.environ["EVENT_RUNTIME_EXECUTE_ENABLED"] = "1"
        os.environ["EVENT_RUNTIME_EXECUTE_UNSAFE_DIRECT_OPT_IN"] = "1"
        os.environ["EVENT_RUNTIME_DEBOUNCE_MS"] = "0"
        os.environ["EVENT_RUNTIME_SIGNAL_MIN_ABS_PREDICTION"] = "0.1"
        os.environ["EVENT_RUNTIME_SIGNAL_MIN_CONFIDENCE"] = "0.1"
        os.environ["EVENT_RUNTIME_EXEC_QTY"] = "2.0"
        (
            self.storage,
            self.event_bus,
            self.event_runtime,
        ) = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.event_bus",
            "engine.runtime.event_runtime",
        )
        self.storage.init_db()

    def tearDown(self) -> None:
        try:
            self.event_runtime.stop_event_runtime(timeout_s=2.0)
        except Exception:
            pass
        try:
            self.event_bus.shutdown_event_bus()
        except Exception:
            pass
        try:
            self.storage.close_pooled_connections()
        except Exception:
            pass
        self.tmp.cleanup()

    def test_price_tick_runs_full_event_chain_to_execution_submission(self) -> None:
        execution_result = {}
        execution_result_seen = threading.Event()
        broker_call = {}
        broker_called = threading.Event()

        def _capture_execution_result(event):
            payload = dict(event.get("payload") or {})
            execution_result.clear()
            execution_result.update(payload)
            execution_result_seen.set()

        def _fake_broker_router(*, dry_run, override_orders, override_order_id, override_ts_ms):
            broker_call["dry_run"] = bool(dry_run)
            broker_call["orders"] = [dict(order) for order in (override_orders or [])]
            broker_call["override_ts_ms"] = int(override_ts_ms or 0)
            broker_called.set()
            return {
                "ok": True,
                "status": "submitted",
                "broker": "sim",
            }

        self.event_bus.subscribe_event("execution_result", _capture_execution_result)

        prediction_payload = {
            "symbol": "AAPL",
            "prediction": 1.25,
            "confidence": 0.82,
            "horizon_s": 300,
            "model_name": "rt_linear",
            "model_id": "rt_linear:AAPL:v1",
            "model_version": "v1",
            "model_kind": "linear",
            "feature_ts_ms": 1_700_000_000_000,
            "feature_set_tag": "price_feature_store_v1",
            "feature_ids": ["rolling_return_5m"],
            "feature_coverage": 1.0,
            "safe_output": False,
            "timed_out": False,
            "status": "ok",
        }

        with patch.object(self.event_runtime, "predict", return_value=prediction_payload):
            with patch.object(self.event_runtime, "get_execution_mode", return_value="live"):
                with patch.object(self.event_runtime, "apply_execution_policy", side_effect=lambda orders, **_: list(orders)):
                    with patch.object(
                        self.event_runtime,
                        "apply_new_portfolio_orders_router",
                        side_effect=_fake_broker_router,
                    ):
                        started = self.event_runtime.start_event_runtime()
                        self.assertTrue(bool(started.get("ok")))
                        self.event_bus.publish_event(
                            "price_tick",
                            {
                                "symbol": "AAPL",
                                "price": 201.5,
                                "provider": "unit_test",
                                "source": "unit_test",
                                "ts_ms": 1_700_000_000_500,
                            },
                        )
                        self.assertTrue(broker_called.wait(timeout=3.0))
                        self.assertTrue(execution_result_seen.wait(timeout=3.0))

        self.assertFalse(bool(broker_call.get("dry_run")))
        self.assertGreater(int(broker_call.get("override_ts_ms") or 0), 0)
        self.assertEqual(len(broker_call.get("orders") or []), 1)
        order = dict((broker_call.get("orders") or [])[0])
        self.assertEqual(str(order.get("symbol") or ""), "AAPL")
        self.assertEqual(str(order.get("side") or ""), "BUY")
        self.assertGreater(float(order.get("qty") or 0.0), 0.0)
        self.assertGreater(int(order.get("signal_ts_ms") or 0), 0)
        self.assertAlmostEqual(float(order.get("confidence") or 0.0), 0.82, places=6)
        self.assertAlmostEqual(float(order.get("expected_z") or 0.0), 1.25, places=6)

        self.assertEqual(str(execution_result.get("symbol") or ""), "AAPL")
        self.assertEqual(str(execution_result.get("source") or ""), "event_runtime")
        self.assertEqual(str((execution_result.get("result") or {}).get("status") or ""), "submitted")


class EventRuntimeDefaultOffTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.prev_env = {
            key: os.environ.get(key)
            for key in (
                "DB_PATH",
                "ENV",
                "ENGINE_MODE",
                "EVENT_RUNTIME_ENABLED",
                "EVENT_RUNTIME_EXECUTE_ENABLED",
                "EVENT_RUNTIME_EXECUTE_UNSAFE_DIRECT_OPT_IN",
                "EVENT_RUNTIME_DEBOUNCE_MS",
            )
        }
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "event_runtime_default_off.db")
        os.environ.pop("EVENT_RUNTIME_ENABLED", None)
        os.environ.pop("EVENT_RUNTIME_EXECUTE_ENABLED", None)
        os.environ.pop("EVENT_RUNTIME_EXECUTE_UNSAFE_DIRECT_OPT_IN", None)
        os.environ["EVENT_RUNTIME_DEBOUNCE_MS"] = "0"
        (
            self.storage,
            self.event_bus,
            self.event_runtime,
        ) = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.event_bus",
            "engine.runtime.event_runtime",
        )
        self.storage.init_db()

    def tearDown(self) -> None:
        try:
            self.event_runtime.stop_event_runtime(timeout_s=2.0)
        except Exception:
            pass
        try:
            self.event_bus.shutdown_event_bus()
        except Exception:
            pass
        try:
            self.storage.close_pooled_connections()
        except Exception:
            pass
        for key, value in self.prev_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self.tmp.cleanup()

    def test_start_event_runtime_is_disabled_by_default(self) -> None:
        model_prediction_seen = threading.Event()

        def _capture_prediction(_event):
            model_prediction_seen.set()

        self.event_bus.subscribe_event("model_prediction", _capture_prediction)
        started = self.event_runtime.start_event_runtime()
        self.assertTrue(bool(started.get("ok")))
        self.assertFalse(bool(started.get("started")))
        self.assertFalse(bool(started.get("enabled")))
        self.assertFalse(bool(started.get("execute_enabled")))
        self.assertFalse(bool(started.get("execute_requested")))
        self.assertEqual(str(started.get("execute_block_reason") or ""), "event_runtime_direct_execution_not_requested")

        self.event_bus.publish_event(
            "price_tick",
            {
                "symbol": "AAPL",
                "price": 201.5,
                "provider": "unit_test",
                "source": "unit_test",
                "ts_ms": 1_700_000_000_500,
            },
        )
        self.assertFalse(model_prediction_seen.wait(timeout=0.5))


class EventRuntimeExecutionGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.prev_env = {
            key: os.environ.get(key)
            for key in (
                "DB_PATH",
                "ENV",
                "ENGINE_MODE",
                "EVENT_RUNTIME_ENABLED",
                "EVENT_RUNTIME_EXECUTE_ENABLED",
                "EVENT_RUNTIME_EXECUTE_UNSAFE_DIRECT_OPT_IN",
                "EVENT_RUNTIME_DEBOUNCE_MS",
            )
        }

    def tearDown(self) -> None:
        try:
            self.event_runtime.stop_event_runtime(timeout_s=2.0)
        except Exception:
            pass
        try:
            self.event_bus.shutdown_event_bus()
        except Exception:
            pass
        try:
            self.storage.close_pooled_connections()
        except Exception:
            pass
        for key, value in self.prev_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self.tmp.cleanup()

    def _reload_runtime(self) -> None:
        (
            self.storage,
            self.event_bus,
            self.event_runtime,
        ) = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.event_bus",
            "engine.runtime.event_runtime",
        )
        self.storage.init_db()

    def test_direct_execution_requires_explicit_dev_env(self) -> None:
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "event_runtime_guard.db")
        os.environ.pop("ENV", None)
        os.environ["EVENT_RUNTIME_ENABLED"] = "1"
        os.environ["EVENT_RUNTIME_EXECUTE_ENABLED"] = "1"
        os.environ["EVENT_RUNTIME_EXECUTE_UNSAFE_DIRECT_OPT_IN"] = "1"
        os.environ["EVENT_RUNTIME_DEBOUNCE_MS"] = "0"
        self._reload_runtime()

        started = self.event_runtime.start_event_runtime()
        self.assertTrue(bool(started.get("ok")))
        self.assertTrue(bool(started.get("started")))
        self.assertFalse(bool(started.get("execute_enabled")))
        self.assertTrue(bool(started.get("execute_requested")))
        self.assertEqual(
            str(started.get("execute_block_reason") or ""),
            "event_runtime_direct_execution_requires_explicit_dev_env",
        )

    def test_direct_execution_blocked_in_live_like_runtime(self) -> None:
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "event_runtime_live_like.db")
        os.environ["ENV"] = "test"
        os.environ["ENGINE_MODE"] = "shadow"
        os.environ["EVENT_RUNTIME_ENABLED"] = "1"
        os.environ["EVENT_RUNTIME_EXECUTE_ENABLED"] = "1"
        os.environ["EVENT_RUNTIME_EXECUTE_UNSAFE_DIRECT_OPT_IN"] = "1"
        os.environ["EVENT_RUNTIME_DEBOUNCE_MS"] = "0"
        self._reload_runtime()

        started = self.event_runtime.start_event_runtime()
        self.assertTrue(bool(started.get("ok")))
        self.assertTrue(bool(started.get("started")))
        self.assertFalse(bool(started.get("execute_enabled")))
        self.assertTrue(bool(started.get("execute_requested")))
        self.assertEqual(
            str(started.get("execute_block_reason") or ""),
            "event_runtime_direct_execution_live_like_blocked",
        )
        with patch.object(
            self.event_runtime,
            "apply_new_portfolio_orders_router",
            side_effect=AssertionError("direct execution should stay blocked"),
        ):
            self.event_runtime._on_execution_decision(
                {
                    "payload": {
                        "symbol": "AAPL",
                        "orders": [{"symbol": "AAPL", "side": "BUY", "signal_ts_ms": 1_700_000_000_500}],
                    },
                    "ts_ms": 1_700_000_000_500,
                }
            )


if __name__ == "__main__":
    unittest.main()
