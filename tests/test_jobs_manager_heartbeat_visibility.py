from __future__ import annotations

import importlib
import os
import sys
import tempfile
import time
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


class JobManagerHeartbeatVisibilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self._prev_db_path = os.environ.get("DB_PATH")
        self._prev_storage_backend = os.environ.get("TS_STORAGE_BACKEND")
        self._prev_queue = os.environ.get("SQLITE_LIVENESS_QUEUE_ENABLED")
        self._prev_trace = os.environ.get("SQLITE_TRACE_REPORT_EVERY_S")
        self._prev_stall_after = os.environ.get("DAEMON_STALL_AFTER_MS")

        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "jobs_manager_heartbeat.db")
        os.environ["TS_STORAGE_BACKEND"] = "sqlite"
        os.environ["SQLITE_LIVENESS_QUEUE_ENABLED"] = "0"
        os.environ["SQLITE_TRACE_REPORT_EVERY_S"] = "0"
        os.environ["DAEMON_STALL_AFTER_MS"] = "120000"

        (storage,) = _reload_modules("engine.runtime.storage")
        storage.init_db()

    def tearDown(self) -> None:
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception:
            pass

        if self._prev_db_path is None:
            os.environ.pop("DB_PATH", None)
        else:
            os.environ["DB_PATH"] = str(self._prev_db_path)
        if self._prev_storage_backend is None:
            os.environ.pop("TS_STORAGE_BACKEND", None)
        else:
            os.environ["TS_STORAGE_BACKEND"] = str(self._prev_storage_backend)
        if self._prev_queue is None:
            os.environ.pop("SQLITE_LIVENESS_QUEUE_ENABLED", None)
        else:
            os.environ["SQLITE_LIVENESS_QUEUE_ENABLED"] = str(self._prev_queue)
        if self._prev_trace is None:
            os.environ.pop("SQLITE_TRACE_REPORT_EVERY_S", None)
        else:
            os.environ["SQLITE_TRACE_REPORT_EVERY_S"] = str(self._prev_trace)
        if self._prev_stall_after is None:
            os.environ.pop("DAEMON_STALL_AFTER_MS", None)
        else:
            os.environ["DAEMON_STALL_AFTER_MS"] = str(self._prev_stall_after)

        self.tmp.cleanup()

    def test_job_state_marks_supervised_daemon_running_from_fresh_job_heartbeat(self) -> None:
        storage, jobs_manager = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.jobs_manager",
        )

        storage.put_job_heartbeat("poll_prices", "test-owner", os.getpid())

        job = jobs_manager.JobState("poll_prices", "engine/data/poll_prices.py", "daemon", "ingestion")
        snapshot = job.to_dict()

        self.assertTrue(bool(snapshot["running"]))
        self.assertEqual(snapshot["status"], "RUNNING")
        self.assertEqual(int(snapshot["pid"]), int(os.getpid()))
        self.assertFalse(bool(snapshot["heartbeat_missing"]))
        self.assertEqual(snapshot["lock_owner"], "test-owner")
        self.assertEqual(snapshot["heartbeat_source"], "job_heartbeats")

    def test_job_state_does_not_mark_stale_heartbeat_as_running(self) -> None:
        storage, jobs_manager = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.jobs_manager",
        )

        storage.put_job_heartbeat("poll_prices", "test-owner", os.getpid())

        con = storage.connect_rw_direct()
        try:
            con.execute(
                "UPDATE job_heartbeats SET ts_ms=? WHERE job_name=?",
                (int(time.time() * 1000) - 300000, "poll_prices"),
            )
            con.commit()
        finally:
            con.close()

        job = jobs_manager.JobState("poll_prices", "engine/data/poll_prices.py", "daemon", "ingestion")
        snapshot = job.to_dict()

        self.assertFalse(bool(snapshot["running"]))
        self.assertEqual(snapshot["status"], "STOPPED")
        self.assertTrue(bool(snapshot["stale"]))
        self.assertEqual(snapshot["heartbeat_source"], "job_heartbeats")


if __name__ == "__main__":
    unittest.main()
