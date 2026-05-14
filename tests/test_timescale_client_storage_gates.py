from __future__ import annotations

import importlib
import sys
import threading
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class TimescaleClientStorageGateTests(unittest.TestCase):
    def test_snapshot_requires_schema_ready_before_reporting_ok(self) -> None:
        (timescale_client,) = _reload_modules("engine.runtime.timescale_client")
        config = timescale_client.TimescaleConfig(
            enabled=True,
            dsn="postgres://example",
            schema_name="public",
            pool_min_size=1,
            pool_max_size=1,
            batch_size=10,
            flush_interval_s=0.5,
            queue_maxsize=32,
            retry_attempts=2,
            retry_base_s=0.1,
            retry_max_s=1.0,
            backpressure_timeout_s=1.0,
            start_timeout_s=1.0,
            connect_timeout_s=1.0,
            lock_timeout_s=1.0,
            command_timeout_s=5.0,
            idle_in_txn_timeout_s=30.0,
            application_name="unit-test",
        )
        client = timescale_client.TimescaleClient(config=config)
        client._thread = threading.current_thread()

        snapshot = client.get_snapshot()

        self.assertFalse(bool(snapshot.get("ok")))
        self.assertIn("schema_not_ready", list(snapshot.get("degraded_reasons") or []))
        self.assertFalse(bool(snapshot.get("schema_ready")))

    def test_start_returns_snapshot_for_live_writer_without_lock_deadlock(self) -> None:
        (timescale_client,) = _reload_modules("engine.runtime.timescale_client")
        config = timescale_client.TimescaleConfig(
            enabled=True,
            dsn="postgres://example",
            schema_name="public",
            pool_min_size=1,
            pool_max_size=1,
            batch_size=10,
            flush_interval_s=0.5,
            queue_maxsize=32,
            retry_attempts=2,
            retry_base_s=0.1,
            retry_max_s=1.0,
            backpressure_timeout_s=1.0,
            start_timeout_s=1.0,
            connect_timeout_s=1.0,
            lock_timeout_s=1.0,
            command_timeout_s=5.0,
            idle_in_txn_timeout_s=30.0,
            application_name="unit-test",
        )
        client = timescale_client.TimescaleClient(config=config)
        client._thread = threading.current_thread()
        timescale_client.asyncpg = object()
        result: list[dict] = []
        errors: list[BaseException] = []

        def _call_start() -> None:
            try:
                result.append(client.start())
            except BaseException as exc:
                errors.append(exc)

        worker = threading.Thread(target=_call_start, name="timescale-start-regression", daemon=True)
        worker.start()
        worker.join(timeout=1.0)

        self.assertFalse(worker.is_alive(), "TimescaleClient.start() deadlocked while snapshotting an active writer")
        self.assertEqual([], errors)
        self.assertEqual(1, len(result))
        self.assertTrue(bool(result[0].get("enabled")))


if __name__ == "__main__":
    unittest.main()
