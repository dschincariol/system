from __future__ import annotations

import importlib
import subprocess
import sys
import threading
import time
from pathlib import Path


def _reload_module(name: str):
    module = sys.modules.get(name)
    if module is not None:
        return importlib.reload(module)
    return importlib.import_module(name)


def _ingestion_pipeline_health_row(ts_ms: int = 1_700_000_000_000):
    return (int(ts_ms), "poll_prices", 1, 12, 2, 1, int(ts_ms), None, "{}")


def _load_telemetry_for_shutdown_test(monkeypatch, tmp_path):
    monkeypatch.setenv("DB_PATH", str(Path(tmp_path) / "runtime_shutdown_drain.db"))
    monkeypatch.setenv("TELEMETRY_APPEND_BUFFER_SPOOL_PATH", str(Path(tmp_path) / "telemetry_spool.sqlite"))
    monkeypatch.setenv("TELEMETRY_APPEND_BUFFER_ENABLED", "1")
    monkeypatch.setenv("TELEMETRY_APPEND_BUFFER_FLUSH_INTERVAL_S", "60")
    monkeypatch.setenv("TELEMETRY_APPEND_BUFFER_MAX_BATCH", "8")
    monkeypatch.setenv("TELEMETRY_APPEND_BUFFER_MAX_ROWS", "8")
    monkeypatch.setenv("RUNTIME_METRICS_ENABLED", "0")
    storage_sqlite = _reload_module("engine.runtime.storage_sqlite")
    storage = _reload_module("engine.runtime.storage")
    telemetry = _reload_module("engine.runtime.telemetry_append_buffer")
    storage.init_db()
    with telemetry._BUFFER_LOCK:
        telemetry._BUFFER_THREAD = None
        telemetry._BUFFER_STOP.clear()
    return storage, storage_sqlite, telemetry


def _enqueue_spooled_ingestion_row(telemetry, ts_ms: int = 1_700_000_000_000) -> None:
    with telemetry._BUFFER_LOCK:
        telemetry._BUFFER_THREAD = None
        telemetry._BUFFER_STOP.clear()
    original_start = telemetry._ensure_buffer_thread_started
    try:
        telemetry._ensure_buffer_thread_started = lambda: None
        assert telemetry.enqueue_ingestion_pipeline_health(_ingestion_pipeline_health_row(ts_ms)) is True
    finally:
        telemetry._ensure_buffer_thread_started = original_start
    assert int(telemetry.get_telemetry_append_buffer_snapshot().get("spool_pending_rows") or 0) == 1


def _run_shutdown_in_thread(telemetry, *, timeout_s: float):
    result: dict[str, object] = {}

    def _target() -> None:
        try:
            result["snapshot"] = telemetry.shutdown_telemetry_append_buffers(timeout_s=timeout_s)
        except BaseException as exc:  # pragma: no cover - surfaced by assertion below
            result["error"] = exc

    thread = threading.Thread(target=_target, name="unit-telemetry-shutdown")
    started = time.monotonic()
    thread.start()
    return thread, result, started


def test_runtime_shutdown_drain_graceful_records_clean_result(monkeypatch):
    shutdown = _reload_module("engine.runtime.shutdown")
    calls: list[tuple[str, float]] = []
    recorded: list[dict] = []

    def _async(timeout_s: float):
        calls.append(("async", float(timeout_s)))
        return {"queue_rows": 0, "residual_spooled_rows": 0, "residual_dropped_rows": 0}

    def _telemetry(timeout_s: float):
        calls.append(("telemetry", float(timeout_s)))
        return {"buffered_rows": 0, "residual_spooled_rows": 0, "residual_dropped_rows": 0}

    monkeypatch.setattr(shutdown, "_drain_async_price_writer", _async)
    monkeypatch.setattr(shutdown, "_drain_telemetry_append_buffers", _telemetry)
    monkeypatch.setattr(shutdown, "_record_runtime_shutdown_drain", lambda payload: recorded.append(dict(payload)))

    result = shutdown.run_runtime_shutdown_drain(deadline_s=1.0, reason="unit_test")

    assert [name for name, _ in calls] == ["async", "telemetry"]
    assert result["ok"] is True
    assert result["residual_risk"]["status"] == "drained"
    assert result["residual_risk"]["residual_dropped_rows"] == 0
    assert recorded and recorded[0]["reason"] == "unit_test"


def test_telemetry_shutdown_contention_returns_without_unbounded_lock_wait(monkeypatch, tmp_path):
    _storage, storage_sqlite, telemetry = _load_telemetry_for_shutdown_test(monkeypatch, tmp_path)
    _enqueue_spooled_ingestion_row(telemetry, 1_700_000_000_101)

    assert storage_sqlite._WRITE_LOCK.acquire(timeout=1.0)
    thread = None
    try:
        thread, result, started = _run_shutdown_in_thread(telemetry, timeout_s=0.15)
        thread.join(timeout=1.0)
        elapsed_s = time.monotonic() - started
        assert not thread.is_alive()
        assert "error" not in result
        snapshot = dict(result["snapshot"])
        assert elapsed_s < 0.75
        assert int(snapshot.get("spool_pending_rows") or 0) == 1
        assert int(snapshot.get("residual_spooled_rows") or 0) == 1
        assert int(snapshot.get("residual_dropped_rows") or 0) == 0
        assert bool(snapshot.get("shutdown_deadline_exhausted")) is True
        assert int(snapshot.get("shutdown_drain_attempts") or 0) >= 1
        assert int(snapshot.get("shutdown_drain_failures") or 0) >= 1
    finally:
        storage_sqlite._WRITE_LOCK.release()
        if thread is not None and thread.is_alive():
            thread.join(timeout=1.0)


def test_telemetry_shutdown_delayed_contention_drains_after_retry(monkeypatch, tmp_path):
    storage, storage_sqlite, telemetry = _load_telemetry_for_shutdown_test(monkeypatch, tmp_path)
    _enqueue_spooled_ingestion_row(telemetry, 1_700_000_000_102)
    monkeypatch.setattr(telemetry, "_SHUTDOWN_DRAIN_WRITE_TIMEOUT_CAP_S", 0.05)
    monkeypatch.setattr(telemetry, "_SHUTDOWN_DRAIN_RETRY_SLEEP_S", 0.01)

    assert storage_sqlite._WRITE_LOCK.acquire(timeout=1.0)
    thread = None
    try:
        thread, result, _started = _run_shutdown_in_thread(telemetry, timeout_s=1.0)
        wait_until = time.monotonic() + 0.6
        observed_failures = 0
        while time.monotonic() < wait_until:
            with telemetry._BUFFER_LOCK:
                observed_failures = int(telemetry._BUFFER_STATE.get("shutdown_drain_failures") or 0)
            if observed_failures >= 1:
                break
            time.sleep(0.01)
        assert observed_failures >= 1
    finally:
        storage_sqlite._WRITE_LOCK.release()
    thread.join(timeout=1.0)

    assert not thread.is_alive()
    assert "error" not in result
    snapshot = dict(result["snapshot"])
    assert int(snapshot.get("spool_pending_rows") or 0) == 0
    assert int(snapshot.get("shutdown_drained_rows") or 0) == 1
    assert int(snapshot.get("deleted_rows") or 0) == 1
    assert int(snapshot.get("retry_count") or 0) >= 1
    assert int(snapshot.get("shutdown_drain_failures") or 0) >= 1
    con = storage.connect_ro_direct()
    try:
        row = con.execute(
            "SELECT COUNT(*) FROM ingestion_pipeline_health WHERE pipeline=?",
            ("poll_prices",),
        ).fetchone()
    finally:
        con.close()
    assert int(row[0] or 0) == 1


def test_telemetry_shutdown_timeout_retains_durable_residual(monkeypatch, tmp_path):
    _storage, _storage_sqlite, telemetry = _load_telemetry_for_shutdown_test(monkeypatch, tmp_path)
    _enqueue_spooled_ingestion_row(telemetry, 1_700_000_000_103)
    monkeypatch.setattr(telemetry, "_SHUTDOWN_DRAIN_ATTEMPTS", 2)
    monkeypatch.setattr(telemetry, "_SHUTDOWN_DRAIN_RETRY_SLEEP_S", 0.01)
    monkeypatch.setattr(
        telemetry,
        "_flush_rows",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("db down")),
    )

    snapshot = telemetry.shutdown_telemetry_append_buffers(timeout_s=0.5)

    assert int(snapshot.get("spool_pending_rows") or 0) == 1
    assert int(snapshot.get("residual_spooled_rows") or 0) == 1
    assert int(snapshot.get("residual_dropped_rows") or 0) == 0
    assert int(snapshot.get("shutdown_drain_attempts") or 0) == 2
    assert int(snapshot.get("shutdown_drain_failures") or 0) == 2
    assert int(snapshot.get("retry_count") or 0) == 2
    assert "db down" in str(snapshot.get("last_error") or "")


class _HungProcess:
    pid = 424242

    def __init__(self, events: list[str]) -> None:
        self._events = events
        self._killed = False

    def poll(self):
        return -9 if self._killed else None

    def terminate(self) -> None:
        self._events.append("terminate")

    def kill(self) -> None:
        self._events.append("kill")
        self._killed = True

    def wait(self, timeout=None):
        if self._killed:
            return -9
        raise subprocess.TimeoutExpired(cmd="unit", timeout=timeout)


def test_supervisor_stop_all_drains_before_sigkill_on_timeout():
    supervisor_mod = _reload_module("engine.runtime.supervisor")
    supervisor = supervisor_mod.RuntimeSupervisor()
    events: list[str] = []
    proc = _HungProcess(events)
    supervisor.register_job("unit_shutdown_job", "noop.py", daemon=True)
    supervisor._jobs["unit_shutdown_job"].process = proc

    def _drain(**kwargs):
        events.append("drain")
        assert proc.poll() is None
        assert kwargs["reason"] == "runtime_supervisor_stop_all_pre_sigkill"
        assert float(kwargs["deadline_s"]) > 0.0
        return {"ok": True, "residual_risk": {"status": "drained"}}

    result = supervisor.stop_all(drain_before_kill=_drain, drain_deadline_s=1.0)

    assert result["ok"] is True
    assert events[:3] == ["terminate", "drain", "kill"]


def test_jobs_manager_stop_all_drains_before_sigkill_on_timeout(monkeypatch):
    jobs_mod = _reload_module("engine.runtime.jobs_manager")
    events: list[str] = []
    proc = _HungProcess(events)
    job = jobs_mod.JobState("unit_shutdown_job", "noop.py", "daemon", "unit")
    job.proc = proc

    manager = jobs_mod.JobManager.__new__(jobs_mod.JobManager)
    manager._jobs = {"unit_shutdown_job": job}
    manager._lock = threading.Lock()
    manager._stop_event = threading.Event()
    manager._watchdog_thread = None

    monkeypatch.setattr(jobs_mod, "_runtime_lock_candidates", lambda name: [])
    monkeypatch.setattr(jobs_mod, "_write_job_history", lambda *args, **kwargs: None)
    monkeypatch.setattr(jobs_mod, "emit_counter", lambda *args, **kwargs: None)
    monkeypatch.setattr(jobs_mod, "trace_event", lambda *args, **kwargs: None)

    def _drain(**kwargs):
        events.append("drain")
        assert proc.poll() is None
        assert kwargs["reason"] == "jobs_manager_stop_all_pre_sigkill"
        assert float(kwargs["deadline_s"]) > 0.0
        return {"ok": True, "residual_risk": {"status": "drained"}}

    result = manager.stop_all(drain_before_kill=_drain, drain_deadline_s=1.0)

    assert result["ok"] is True
    assert events[:3] == ["terminate", "drain", "kill"]


def test_telemetry_shutdown_emits_residual_drop_metric_for_memory_rows(monkeypatch, tmp_path):
    monkeypatch.setenv("DB_PATH", str(Path(tmp_path) / "runtime_shutdown_drain.db"))
    monkeypatch.setenv("TELEMETRY_APPEND_BUFFER_SPOOL_PATH", str(Path(tmp_path) / "telemetry_spool.sqlite"))
    monkeypatch.setenv("TELEMETRY_APPEND_BUFFER_ENABLED", "1")
    monkeypatch.setenv("TELEMETRY_APPEND_BUFFER_FLUSH_INTERVAL_S", "60")
    telemetry = _reload_module("engine.runtime.telemetry_append_buffer")
    emitted: list[tuple[str, int, dict]] = []

    with telemetry._BUFFER_LOCK:
        telemetry._BUFFER_THREAD = None
        telemetry._BUFFER_STOP.clear()
        for table in telemetry._TABLE_ORDER:
            telemetry._BUFFER_PENDING[table] = []
        telemetry._BUFFER_PENDING["price_provider_health"] = [
            (1_700_000_000_000, "unit", 1, 2.5, 1, "", 1_700_000_000_000, 0)
        ]
        telemetry._BUFFER_STATE["buffered_rows"] = 1

    monkeypatch.setattr(
        telemetry,
        "emit_counter",
        lambda metric, value=1, **kwargs: emitted.append((str(metric), int(value), dict(kwargs))),
    )

    snapshot = telemetry.shutdown_telemetry_append_buffers(timeout_s=0.0)

    assert int(snapshot.get("residual_dropped_rows") or 0) >= 1
    assert int(snapshot.get("residual_loss_rows") or 0) >= 1
    assert any(metric == "telemetry_append_buffer_residual_dropped_rows" and value >= 1 for metric, value, _ in emitted)
    assert any(metric == "telemetry_append_buffer_residual_loss_rows" and value >= 1 for metric, value, _ in emitted)
