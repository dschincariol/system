"""Startup shutdown and runtime side-effect helpers."""

from typing import Any, Callable


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
) -> None:
    """Handle a process shutdown signal with the existing fail-fast semantics."""
    watchdog_stop.set()
    try:
        mark_clean_shutdown = mark_clean_shutdown_loader()
        mark_clean_shutdown()
    except Exception:
        log_swallowed("MARK_CLEAN_SHUTDOWN_FAILED", signal=int(signum))
    terminate_ingestion()
    try:
        runtime_shutdown(shutdown_reason=f"signal:{int(signum)}")
    except Exception:
        log_swallowed("RUNTIME_SHUTDOWN_FAILED", signal=int(signum))
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
