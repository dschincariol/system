from __future__ import annotations

import importlib
import threading
import time

import pytest


def _reload_module(name: str):
    return importlib.reload(importlib.import_module(name))


def _manager_with_stopped_job(jobs_mod):
    job = jobs_mod.JobState("unit_shutdown_job", "noop.py", "daemon", "unit")
    manager = jobs_mod.JobManager.__new__(jobs_mod.JobManager)
    manager._jobs = {"unit_shutdown_job": job}
    manager._lock = threading.Lock()
    manager._stop_event = threading.Event()
    manager._watchdog_thread = None
    return manager


def _patch_jobs_manager_side_effects(monkeypatch: pytest.MonkeyPatch, jobs_mod, warnings: list[tuple[str, str, dict]]) -> None:
    monkeypatch.setattr(jobs_mod, "_runtime_lock_candidates", lambda name: [f"job:{name}"])
    monkeypatch.setattr(jobs_mod, "_write_job_history", lambda *args, **kwargs: None)
    monkeypatch.setattr(jobs_mod, "emit_counter", lambda *args, **kwargs: None)
    monkeypatch.setattr(jobs_mod, "trace_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        jobs_mod,
        "_warn_nonfatal",
        lambda code, error, **extra: warnings.append((str(code), str(error), dict(extra))),
    )


def test_jobs_manager_stop_all_lock_release_raise_is_best_effort(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JOBS_MANAGER_LOCK_RELEASE_TIMEOUT_S", "0.05")
    jobs_mod = _reload_module("engine.runtime.jobs_manager")
    warnings: list[tuple[str, str, dict]] = []
    _patch_jobs_manager_side_effects(monkeypatch, jobs_mod, warnings)
    monkeypatch.setattr(
        jobs_mod,
        "_release_lock",
        lambda lock_name: (_ for _ in ()).throw(RuntimeError("SET LOCAL $1")),
    )
    manager = _manager_with_stopped_job(jobs_mod)

    started = time.monotonic()
    result = manager.stop_all(drain_before_kill=lambda **kwargs: {"ok": True}, drain_deadline_s=0.5)
    elapsed_s = time.monotonic() - started

    assert elapsed_s < 0.5
    assert result["ok"] is True
    assert result["stopped"] == ["unit_shutdown_job"]
    assert any(code == "JOBS_MANAGER_RUNTIME_LOCK_RELEASE_FAILED" for code, _, _ in warnings)


def test_jobs_manager_stop_all_lock_release_timeout_does_not_block(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JOBS_MANAGER_LOCK_RELEASE_TIMEOUT_S", "0.03")
    jobs_mod = _reload_module("engine.runtime.jobs_manager")
    warnings: list[tuple[str, str, dict]] = []
    release_gate = threading.Event()
    _patch_jobs_manager_side_effects(monkeypatch, jobs_mod, warnings)

    def _slow_release(_lock_name: str) -> None:
        release_gate.wait(timeout=2.0)

    monkeypatch.setattr(jobs_mod, "_release_lock", _slow_release)
    manager = _manager_with_stopped_job(jobs_mod)

    try:
        started = time.monotonic()
        result = manager.stop_all(drain_before_kill=lambda **kwargs: {"ok": True}, drain_deadline_s=0.25)
        elapsed_s = time.monotonic() - started
    finally:
        release_gate.set()

    assert elapsed_s < 0.25
    assert result["ok"] is True
    assert any(code == "JOBS_MANAGER_RUNTIME_LOCK_RELEASE_TIMEOUT" for code, _, _ in warnings)


def test_signal_handler_deadline_triggers_force_exit() -> None:
    shutdown = _reload_module("engine.startup.shutdown")
    runtime_gate = threading.Event()
    logs: list[tuple[str, dict]] = []
    flushed: list[str] = []

    class ForcedExit(Exception):
        def __init__(self, code: int) -> None:
            super().__init__(code)
            self.code = code

    try:
        with pytest.raises(ForcedExit) as exc_info:
            shutdown.handle_signal(
                15,
                watchdog_stop=threading.Event(),
                mark_clean_shutdown_loader=lambda: (lambda: None),
                terminate_ingestion=lambda: None,
                runtime_shutdown=lambda **kwargs: runtime_gate.wait(timeout=2.0),
                log_swallowed=lambda event, **extra: logs.append((str(event), dict(extra))),
                shutdown_deadline_s=0.05,
                force_exit=lambda code: (_ for _ in ()).throw(ForcedExit(int(code))),
                flush_logging_handlers=lambda: flushed.append("flush"),
            )
    finally:
        runtime_gate.set()

    assert exc_info.value.code == 0
    assert flushed == ["flush"]
    assert any(
        event == "SIGNAL_SHUTDOWN_DEADLINE_EXCEEDED" and extra.get("outstanding_step") == "runtime_shutdown"
        for event, extra in logs
    )


def test_signal_handler_force_exits_for_leftover_non_daemon_thread() -> None:
    shutdown = _reload_module("engine.startup.shutdown")
    stop = threading.Event()
    ready = threading.Event()
    logs: list[tuple[str, dict]] = []

    class ForcedExit(Exception):
        pass

    def _worker() -> None:
        ready.set()
        stop.wait(timeout=2.0)

    thread = threading.Thread(target=_worker, name="unit_non_daemon_shutdown_blocker", daemon=False)
    thread.start()
    assert ready.wait(timeout=1.0)
    try:
        with pytest.raises(ForcedExit):
            shutdown.handle_signal(
                15,
                watchdog_stop=threading.Event(),
                mark_clean_shutdown_loader=lambda: (lambda: None),
                terminate_ingestion=lambda: None,
                runtime_shutdown=lambda **kwargs: None,
                log_swallowed=lambda event, **extra: logs.append((str(event), dict(extra))),
                shutdown_deadline_s=0.05,
                force_exit=lambda code: (_ for _ in ()).throw(ForcedExit()),
                flush_logging_handlers=lambda: None,
            )
    finally:
        stop.set()
        thread.join(timeout=1.0)

    assert not thread.is_alive()
    assert any(
        event == "SIGNAL_SHUTDOWN_BACKGROUND_THREADS_STILL_RUNNING"
        and extra.get("outstanding_step") == "non_daemon_threads"
        and any("unit_non_daemon_shutdown_blocker" in item for item in extra.get("threads", []))
        for event, extra in logs
    )


def test_dashboard_bind_wait_thread_is_daemonized() -> None:
    dashboard = _reload_module("engine.startup.dashboard")
    captured: dict[str, object] = {}

    class FakeThread:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

        def start(self) -> None:
            captured["started"] = True

    dashboard.run_dashboard_server_post_bind_validation(
        lambda: None,
        mode="safe",
        host="127.0.0.1",
        port=8000,
        bind_wait_timeout_s=1.0,
        wait_for_bind=lambda **kwargs: True,
        start_startup_health_validation_async=lambda **kwargs: None,
        record_phase=lambda *args, **kwargs: None,
        record_first_failure=lambda *args, **kwargs: None,
        log_warning=lambda *args, **kwargs: None,
        log_swallowed=lambda *args, **kwargs: None,
        handle_late_startup_health_validation_failure=lambda *args, **kwargs: None,
        file_path=__file__,
        thread_factory=FakeThread,
    )

    assert captured["name"] == "startup_health_bind_wait"
    assert captured["daemon"] is True
    assert captured["started"] is True
