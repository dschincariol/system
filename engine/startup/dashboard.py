"""Dashboard startup coordination helpers."""

import threading
import time
from typing import Any, Callable, Dict, Iterable, Optional


def wait_for_dashboard_bind(
    *,
    host: str,
    port: int,
    timeout_s: float,
    create_connection: Callable[..., Any],
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> bool:
    """Wait until the dashboard TCP listener accepts connections."""
    deadline = monotonic() + max(0.5, float(timeout_s))
    address = (str(host), int(port))
    while monotonic() < deadline:
        try:
            with create_connection(address, timeout=0.25):
                return True
        except OSError:
            sleep(0.25)
    return False


def coerce_ts_ms(value: Any) -> int:
    """Coerce a lifecycle timestamp value into an integer millisecond value."""
    return int(str(value or "0").strip() or "0")


def dashboard_stop_requested(
    *,
    dashboard_module: Any,
    log_swallowed: Optional[Callable[..., None]] = None,
) -> bool:
    """Return whether dashboard_server has requested a clean server stop."""
    try:
        event = getattr(dashboard_module, "_SERVER_STOP_EVENT", None) if dashboard_module is not None else None
        return bool(callable(getattr(event, "is_set", None)) and event.is_set())
    except Exception as e:
        if log_swallowed is not None:
            log_swallowed("DASHBOARD_STOP_REQUEST_CHECK_FAILED", error=str(e))
        return False


def dashboard_returned_after_clean_shutdown(
    lifecycle: Dict[str, Any],
    *,
    run_enter_ts_ms: int,
    stop_requested_at_enter: bool = False,
    stop_requested_now: bool = False,
    shutdown_states: Iterable[str] = ("SHUTTING_DOWN",),
    coerce_ts_ms_fn: Callable[[Any], int] = coerce_ts_ms,
) -> bool:
    """Return whether dashboard server exit corresponds to a clean shutdown."""
    state = str(lifecycle.get("state") or "").strip().upper()
    if state in {str(item).strip().upper() for item in shutdown_states}:
        return True

    if bool(stop_requested_now) and not bool(stop_requested_at_enter):
        return True

    clean_ts_ms = coerce_ts_ms_fn(lifecycle.get("last_clean_shutdown_ts_ms"))
    return bool(clean_ts_ms > 0 and clean_ts_ms >= int(run_enter_ts_ms or 0))


def run_dashboard_server(
    run_server: Callable[[], Any],
    *,
    mode: str,
    perform_startup_health_validation: Callable[..., Any],
) -> None:
    """Run synchronous startup validation before entering the dashboard server."""
    perform_startup_health_validation(mode=str(mode))
    run_server()


def run_dashboard_server_post_bind_validation(
    run_server: Callable[[], Any],
    *,
    mode: str,
    host: str,
    port: int,
    bind_wait_timeout_s: float,
    wait_for_bind: Callable[..., bool],
    start_startup_health_validation_async: Callable[..., Any],
    record_phase: Callable[..., None],
    record_first_failure: Callable[..., None],
    log_warning: Callable[..., None],
    log_swallowed: Callable[..., None],
    handle_late_startup_health_validation_failure: Callable[..., None],
    file_path: str,
    thread_factory: Callable[..., threading.Thread] = threading.Thread,
) -> None:
    """Run dashboard server while a background thread waits for bind then validates."""

    def _runner() -> None:
        try:
            record_phase(
                "STARTUP_HEALTH",
                status="started",
                detail="await_dashboard_bind_before_async_validation",
                extra={
                    "host": str(host),
                    "port": int(port),
                    "timeout_s": float(bind_wait_timeout_s),
                },
            )
            log_warning(
                "STARTUP_HEALTH_AWAIT_DASHBOARD_BIND host=%s port=%s timeout_s=%s",
                host,
                port,
                bind_wait_timeout_s,
            )
            if not wait_for_bind(
                host=str(host),
                port=int(port),
                timeout_s=float(bind_wait_timeout_s),
            ):
                raise TimeoutError(f"dashboard_bind_timeout:{host}:{port}")
            record_phase(
                "STARTUP_HEALTH",
                status="started",
                detail="dashboard_bound_async_validation_scheduled",
                extra={"host": str(host), "port": int(port)},
            )
            log_warning(
                "STARTUP_HEALTH_DASHBOARD_BOUND host=%s port=%s starting_async_validation",
                host,
                port,
            )
            start_startup_health_validation_async(mode=str(mode))
        except Exception as e:
            record_first_failure(
                "STARTUP_HEALTH",
                e,
                file_path=file_path,
                module="start_system.bind_wait",
            )
            record_phase(
                "STARTUP_HEALTH",
                status="failed",
                detail=str(e),
                extra={"host": str(host), "port": int(port)},
            )
            log_swallowed(
                "STARTUP_HEALTH_BIND_WAIT_FAILED",
                mode=str(mode),
                host=str(host),
                port=int(port),
                error=str(e),
            )
            handle_late_startup_health_validation_failure(
                e,
                mode=str(mode),
                scope="dashboard_bind_wait",
            )

    thread_factory(
        target=_runner,
        name="startup_health_bind_wait",
        daemon=True,
    ).start()
    run_server()
