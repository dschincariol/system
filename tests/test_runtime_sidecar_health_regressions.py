from __future__ import annotations

import queue
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class _AliveThread:
    def is_alive(self) -> bool:
        return True


class RuntimeSidecarHealthRegressionTests(unittest.TestCase):
    def test_timescale_snapshot_marks_backpressure_degraded_until_flush_recovers(self) -> None:
        from engine.runtime.timescale_client import TimescaleClient, TimescaleConfig

        client = TimescaleClient(
            TimescaleConfig(
                enabled=True,
                dsn="postgres://unit-test",
                schema_name="public",
                pool_min_size=1,
                pool_max_size=1,
                batch_size=8,
                flush_interval_s=0.1,
                queue_maxsize=16,
                retry_attempts=1,
                retry_base_s=0.01,
                retry_max_s=0.01,
                backpressure_timeout_s=0.1,
                start_timeout_s=0.1,
                connect_timeout_s=0.1,
                lock_timeout_s=0.1,
                command_timeout_s=1.0,
                idle_in_txn_timeout_s=0.1,
                application_name="unit-test",
            )
        )
        client._thread = _AliveThread()
        client._schema_ready = True

        client._note_backpressure()
        degraded = client.get_snapshot()

        self.assertFalse(bool(degraded["ok"]))
        self.assertTrue(bool(degraded["degraded"]))
        self.assertIn("queue_backpressure", degraded["degraded_reasons"])
        self.assertTrue(bool(degraded["metrics"]["backpressure_active"]))
        self.assertGreater(int(degraded["metrics"]["last_backpressure_ts_ms"]), 0)

        client._note_flush_success("price_data", 4)
        recovered = client.get_snapshot()

        self.assertTrue(bool(recovered["ok"]))
        self.assertFalse(bool(recovered["degraded"]))
        self.assertEqual(list(recovered["degraded_reasons"]), [])
        self.assertFalse(bool(recovered["metrics"]["backpressure_active"]))

    def test_feature_store_snapshot_surfaces_queue_pressure_and_drop_metrics(self) -> None:
        import engine.strategy.feature_store as feature_store_module
        from engine.strategy.feature_store import FeatureStore, FeatureStoreConfig

        store = FeatureStore(
            FeatureStoreConfig(
                enabled=True,
                dsn="postgres://unit-test",
                schema_name="public",
                batch_size=4,
                flush_interval_s=0.1,
                queue_maxsize=1,
                enqueue_timeout_s=0.1,
                retry_attempts=1,
                retry_base_s=0.01,
                retry_max_s=0.01,
                connect_timeout_s=0.1,
                command_timeout_s=1.0,
                application_name="unit-test",
            )
        )
        store._thread = _AliveThread()
        store._schema_ready = True
        store._queue = queue.Queue(maxsize=1)
        store._queue.put(
            types.SimpleNamespace(
                symbol="AAPL",
                timestamp=None,
                feature_version=1,
                features_json="{}",
            )
        )

        with patch.object(feature_store_module, "asyncpg", object()):
            with patch.object(store, "start", return_value=True):
                wrote = store.schedule_write(
                    "AAPL",
                    1_710_000_000_000,
                    {"feature": 1.0},
                    version=1,
                )
                snapshot = store.get_snapshot()

            self.assertFalse(bool(wrote))
            self.assertFalse(bool(snapshot["ok"]))
            self.assertTrue(bool(snapshot["degraded"]))
            self.assertIn("queue_backpressure", snapshot["degraded_reasons"])
            self.assertEqual(int(snapshot["metrics"]["queue_rejection_count"]), 1)
            self.assertTrue(bool(snapshot["metrics"]["queue_backpressure_active"]))

            store._note_flush_failure(3, dropped=True)
            dropped = store.get_snapshot()
            self.assertEqual(int(dropped["metrics"]["flush_failure_count"]), 1)
            self.assertEqual(int(dropped["metrics"]["flush_drop_count"]), 3)
            self.assertIn("flush_failures", dropped["degraded_reasons"])

            store._note_flush_success(3)
            store._queue = queue.Queue(maxsize=1)
            recovered = store.get_snapshot()

        self.assertTrue(bool(recovered["ok"]))
        self.assertFalse(bool(recovered["degraded"]))
        self.assertEqual(list(recovered["degraded_reasons"]), [])
        self.assertFalse(bool(recovered["metrics"]["queue_backpressure_active"]))

    def test_event_bus_stats_surface_normal_queue_overflow(self) -> None:
        from engine.runtime.event_bus import EventBus

        bus = EventBus(max_queue_size=32, handler_workers=1)
        for idx in range(33):
            bus.publish({"type": f"telemetry.{idx}", "ts_ms": idx})

        stats = bus.get_stats()

        self.assertTrue(bool(stats["degraded"]))
        self.assertIn("normal_queue_overflow", stats["degraded_reasons"])
        self.assertTrue(bool(stats["normal_overflow_active"]))
        self.assertEqual(int(stats["dropped_count"]), 1)
        self.assertIsNotNone(stats["last_normal_overflow_ts_ms"])

    def test_event_bus_stats_surface_critical_backpressure(self) -> None:
        from engine.runtime.event_bus import EventBus

        bus = EventBus(max_queue_size=32, handler_workers=1)
        for idx in range(17):
            bus.publish({"type": f"execution.signal.{idx}", "ts_ms": idx})

        stats = bus.get_stats()

        self.assertTrue(bool(stats["degraded"]))
        self.assertIn("critical_queue_backpressure", stats["degraded_reasons"])
        self.assertTrue(bool(stats["critical_backpressure_active"]))
        self.assertEqual(int(stats["critical_inline_dispatch_count"]), 1)
        self.assertEqual(int(stats["critical_backpressure_count"]), 1)
        self.assertIsNotNone(stats["last_critical_backpressure_ts_ms"])


if __name__ == "__main__":
    unittest.main()
