"""Startup shutdown and runtime side-effect helpers."""

import os
import sys
import threading
import time
from typing import Any, Callable, Optional


def shutdown_hard_deadline_s() -> float:
    """Return the whole-process SIGTERM/SIGINT shutdown deadline in seconds."""
    try:
        return max(0.05, min(300.0, float(os.environ.get("RUNTIME_SHUTDOWN_HARD_DEADLINE_S", "20") or 20.0)))
    except Exception:
        return 20.0


def _default_force_exit(code: int) -> None:
    os._exit(int(code))


def _flush_before_force_exit(flush_logging_handlers: Optional[Callable[[], None]] = None) -> None:
    try:
        if flush_logging_handlers is None:
            from engine.runtime.logging import flush_logging_handlers as _flush_logging_handlers

            flush_logging_handlers = _flush_logging_handlers
        flush_logging_handlers()
    except Exception:  # no-op-guard: allow - shutdown log flushing is best-effort before force exit.
        pass
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except Exception:  # no-op-guard: allow - stdio may already be closed during shutdown.
            pass


def _thread_label(thread: threading.Thread) -> str:
    try:
        ident = thread.ident
    except Exception:
        ident = None
    try:
        name = thread.name
    except Exception:
        name = "unknown"
    return f"{name}:{ident}"


def _non_daemon_threads(exclude_idents: set[int | None]) -> list[threading.Thread]:
    threads: list[threading.Thread] = []
    for thread in threading.enumerate():
        try:
            ident = thread.ident
            if ident in exclude_idents:
                continue
            if thread.daemon or not thread.is_alive():
                continue
            threads.append(thread)
        except Exception:
            continue
    return threads


def _join_non_daemon_threads_until(
    *,
    deadline: float,
    exclude_idents: set[int | None],
    log_swallowed: Callable[..., None],
) -> list[str]:
    for thread in _non_daemon_threads(exclude_idents):
        remaining_s = max(0.0, float(deadline) - time.monotonic())
        if remaining_s <= 0.0:
            break
        try:
            thread.join(timeout=remaining_s)
        except Exception as exc:
            log_swallowed(
                "SIGNAL_SHUTDOWN_THREAD_JOIN_FAILED",
                thread=_thread_label(thread),
                error=f"{type(exc).__name__}: {exc}",
            )
    return [_thread_label(thread) for thread in _non_daemon_threads(exclude_idents)]


def request_dashboard_runtime_stop(
    reason: str,
    *,
    watchdog_stop: Any,
    stop_server_loader: Callable[[], Callable[[], None]],
    runtime_shutdown: Callable[..., None],
    terminate_ingestion: Callable[[], None],
    log_swallowed: Callable[..., None],
) -> None:
    """Request every startup-owned runtime surface to stop."""
    stopped = False
    watchdog_stop.set()
    try:
        stop_server = stop_server_loader()
        stop_server()
        stopped = True
    except Exception as stop_err:
        log_swallowed("STARTUP_HEALTH_STOP_SERVER_FAILED", error=str(stop_err), reason=str(reason))

    try:
        runtime_shutdown(shutdown_reason=str(reason or "dashboard_runtime_stop"))
        stopped = True
    except Exception as shutdown_err:
        log_swallowed("STARTUP_HEALTH_RUNTIME_SHUTDOWN_FAILED", error=str(shutdown_err), reason=str(reason))

    try:
        terminate_ingestion()
        stopped = True
    except Exception as ingestion_err:
        log_swallowed("STARTUP_HEALTH_INGESTION_TERMINATE_FAILED", error=str(ingestion_err), reason=str(reason))

    if not stopped:
        log_swallowed("STARTUP_HEALTH_RUNTIME_STOP_NOT_CONFIRMED", reason=str(reason))


def handle_signal(
    signum: int,
    *,
    watchdog_stop: Any,
    mark_clean_shutdown_loader: Callable[[], Callable[[], None]],
    terminate_ingestion: Callable[[], None],
    runtime_shutdown: Callable[..., None],
    log_swallowed: Callable[..., None],
    shutdown_deadline_s: Optional[float] = None,
    force_exit: Callable[[int], None] = _default_force_exit,
    flush_logging_handlers: Optional[Callable[[], None]] = None,
) -> None:
    """Handle a process shutdown signal under a bounded whole-process deadline."""
    watchdog_stop.set()
    budget_s = shutdown_hard_deadline_s() if shutdown_deadline_s is None else max(0.05, float(shutdown_deadline_s))
    deadline = time.monotonic() + float(budget_s)
    finished = threading.Event()
    state: dict[str, Any] = {"step": "starting", "errors": []}

    def _shutdown_worker() -> None:
        try:
            state["step"] = "mark_clean_shutdown"
            try:
                mark_clean_shutdown = mark_clean_shutdown_loader()
                mark_clean_shutdown()
            except Exception as exc:
                state["errors"].append(f"mark_clean_shutdown:{type(exc).__name__}:{exc}")
                log_swallowed("MARK_CLEAN_SHUTDOWN_FAILED", signal=int(signum), error=str(exc))

            state["step"] = "terminate_ingestion"
            try:
                terminate_ingestion()
            except Exception as exc:
                state["errors"].append(f"terminate_ingestion:{type(exc).__name__}:{exc}")
                log_swallowed("TERMINATE_INGESTION_FAILED", signal=int(signum), error=str(exc))

            state["step"] = "runtime_shutdown"
            try:
                runtime_shutdown(shutdown_reason=f"signal:{int(signum)}")
            except Exception as exc:
                state["errors"].append(f"runtime_shutdown:{type(exc).__name__}:{exc}")
                log_swallowed("RUNTIME_SHUTDOWN_FAILED", signal=int(signum), error=str(exc))
        finally:
            state["step"] = "complete"
            finished.set()

    worker = threading.Thread(target=_shutdown_worker, name=f"signal_shutdown_{int(signum)}", daemon=True)
    try:
        worker.start()
    except Exception as exc:
        log_swallowed("SIGNAL_SHUTDOWN_THREAD_START_FAILED", signal=int(signum), error=str(exc))
        _flush_before_force_exit(flush_logging_handlers)
        force_exit(0)
        raise SystemExit(0)

    if not finished.wait(max(0.0, float(deadline) - time.monotonic())):
        outstanding = str(state.get("step") or "unknown")
        log_swallowed(
            "SIGNAL_SHUTDOWN_DEADLINE_EXCEEDED",
            signal=int(signum),
            deadline_s=float(budget_s),
            outstanding_step=outstanding,
        )
        _flush_before_force_exit(flush_logging_handlers)
        force_exit(0)
        raise SystemExit(0)

    exclude_idents = {threading.current_thread().ident, worker.ident}
    remaining_threads = _join_non_daemon_threads_until(
        deadline=deadline,
        exclude_idents=exclude_idents,
        log_swallowed=log_swallowed,
    )
    if remaining_threads:
        log_swallowed(
            "SIGNAL_SHUTDOWN_BACKGROUND_THREADS_STILL_RUNNING",
            signal=int(signum),
            deadline_s=float(budget_s),
            outstanding_step="non_daemon_threads",
            threads=remaining_threads[:20],
        )
        _flush_before_force_exit(flush_logging_handlers)
        force_exit(0)
        raise SystemExit(0)

    raise SystemExit(0)


def bootstrap_runtime_side_effects(
    *,
    watchdog_stop: Any,
    register_atexit: Callable[[Callable[[], None]], Any],
    register_signal: Callable[[Any, Callable[..., None]], Any],
    sigterm: Any,
    sigint: Any,
    handle_signal_fn: Callable[..., None],
    terminate_ingestion: Callable[[], None],
    cleanup_pid_file: Callable[[], None],
    write_pid_file: Callable[[], None],
    run_startup_db_repair: Callable[[], Any],
    log_exception: Callable[[str], None],
) -> None:
    """Install process side effects owned by startup bootstrap."""
    watchdog_stop.clear()
    register_atexit(terminate_ingestion)
    register_atexit(cleanup_pid_file)
    write_pid_file()

    try:
        register_signal(sigterm, handle_signal_fn)
        register_signal(sigint, handle_signal_fn)
    except Exception as e:
        log_exception("SIGNAL_HANDLER_REGISTRATION_FAILED")
        raise RuntimeError(f"signal_handler_registration_failed:{type(e).__name__}:{e}") from e

    try:
        run_startup_db_repair()
    except Exception:
        log_exception("DB_REPAIR_FAILED")
        raise
