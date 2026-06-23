"""Graceful runtime shutdown helpers for jobs, supervision, and SQLite state.

The shutdown path is intentionally fail-safe: it records runtime events, stops
child processes, checkpoints WAL state, and closes pooled connections without
raising cleanup-time exceptions back into the caller.
"""

from __future__ import annotations

import os
import threading
import time
from contextlib import nullcontext
from typing import Any, Callable, Dict, Optional

from engine.runtime.logging import flush_logging_handlers, get_logger

LOG = get_logger("runtime.shutdown")
_BROKER_SHUTDOWN_RISK_LOCK = threading.Lock()
_BROKER_SHUTDOWN_RISK_RESULT: Optional[Dict[str, Any]] = None
_BROKER_SHUTDOWN_COMMAND_ID = f"broker-risk-runtime-{os.getpid()}-{int(time.time() * 1000)}"


def _shutdown_storage_timeout_s() -> float:
    try:
        return max(0.05, float(os.environ.get("RUNTIME_SHUTDOWN_STORAGE_TIMEOUT_S", "0.5") or 0.5))
    except Exception:
        return 0.5


def _storage_timeout_ctx():
    try:
        from engine.runtime.storage_pool import storage_acquire_timeout_override

        return storage_acquire_timeout_override(_shutdown_storage_timeout_s())
    except Exception:
        return nullcontext()


def _broker_shutdown_timeout_s() -> float:
    try:
        return max(0.1, min(120.0, float(os.environ.get("BROKER_SHUTDOWN_TIMEOUT_S", "10") or 10.0)))
    except Exception:
        return 10.0


def _runtime_shutdown_drain_deadline_s() -> float:
    try:
        return max(0.0, min(120.0, float(os.environ.get("RUNTIME_SHUTDOWN_DRAIN_DEADLINE_S", "5.0") or 5.0)))
    except Exception:
        return 5.0


def _remaining_s(deadline: float) -> float:
    return max(0.0, float(deadline) - time.monotonic())


def _snapshot_int(snapshot: Dict[str, Any], *keys: str) -> int:
    for key in keys:
        try:
            value = snapshot.get(str(key))
        except Exception:
            value = None
        if value is None:
            continue
        try:
            return int(value or 0)
        except Exception:
            continue
    return 0


def _drain_async_price_writer(timeout_s: float) -> Dict[str, Any]:
    from engine.runtime.async_writer import shutdown_async_writer

    return dict(shutdown_async_writer(timeout_s=max(0.0, float(timeout_s))) or {})


def _drain_telemetry_append_buffers(timeout_s: float) -> Dict[str, Any]:
    from engine.runtime.telemetry_append_buffer import shutdown_telemetry_append_buffers

    return dict(shutdown_telemetry_append_buffers(timeout_s=max(0.0, float(timeout_s))) or {})


def _runtime_shutdown_residual_risk(
    *,
    async_price_writer: Dict[str, Any],
    telemetry_append_buffer: Dict[str, Any],
    deadline_exhausted: bool,
    errors: list[str],
) -> Dict[str, Any]:
    async_pending_rows = _snapshot_int(async_price_writer, "queue_rows", "spool_pending_rows")
    async_residual_spooled_rows = _snapshot_int(async_price_writer, "residual_spooled_rows")
    async_residual_dropped_rows = _snapshot_int(async_price_writer, "residual_loss_rows", "residual_dropped_rows")
    telemetry_pending_rows = _snapshot_int(telemetry_append_buffer, "buffered_rows", "queue_rows", "spool_pending_rows")
    telemetry_residual_spooled_rows = _snapshot_int(telemetry_append_buffer, "residual_spooled_rows")
    telemetry_residual_dropped_rows = _snapshot_int(telemetry_append_buffer, "residual_loss_rows", "residual_dropped_rows")
    residual_spooled_rows = int(async_residual_spooled_rows) + int(telemetry_residual_spooled_rows)
    residual_dropped_rows = int(async_residual_dropped_rows) + int(telemetry_residual_dropped_rows)
    pending_rows = int(async_pending_rows) + int(telemetry_pending_rows)
    loss_possible = bool(residual_dropped_rows > 0 or telemetry_residual_dropped_rows > 0)
    degraded = bool(deadline_exhausted or errors or pending_rows > 0 or residual_spooled_rows > 0 or loss_possible)
    if loss_possible:
        status = "residual_loss_possible"
    elif residual_spooled_rows > 0 or pending_rows > 0:
        status = "residual_retained_for_replay"
    elif deadline_exhausted:
        status = "deadline_exhausted"
    elif errors:
        status = "drain_errors"
    else:
        status = "drained"
    return {
        "ok": not degraded,
        "status": status,
        "deadline_exhausted": bool(deadline_exhausted),
        "pending_rows": int(pending_rows),
        "residual_spooled_rows": int(residual_spooled_rows),
        "residual_dropped_rows": int(residual_dropped_rows),
        "loss_possible": bool(loss_possible),
        "async_price_writer": {
            "pending_rows": int(async_pending_rows),
            "residual_spooled_rows": int(async_residual_spooled_rows),
            "residual_dropped_rows": int(async_residual_dropped_rows),
        },
        "telemetry_append_buffer": {
            "pending_rows": int(telemetry_pending_rows),
            "residual_spooled_rows": int(telemetry_residual_spooled_rows),
            "residual_dropped_rows": int(telemetry_residual_dropped_rows),
        },
        "errors": list(errors or []),
    }


def _record_runtime_shutdown_drain(payload: Dict[str, Any]) -> None:
    try:
        from engine.runtime.event_log import append_event

        with _storage_timeout_ctx():
            append_event(
                event_type="runtime_shutdown_drain",
                event_source="runtime.shutdown",
                entity_type="runtime",
                entity_id="shutdown",
                payload=dict(payload or {}),
                ts_ms=int(time.time() * 1000),
                best_effort=True,
            )
    except Exception:
        LOG.exception("runtime_shutdown_drain_event_failed")
    try:
        from engine.runtime.observability import record_component_health

        residual = dict((payload or {}).get("residual_risk") or {})
        ok = bool((payload or {}).get("ok")) and not bool(residual.get("loss_possible"))
        record_component_health(
            "runtime_shutdown_drain",
            ok=ok,
            status=("ok" if ok else "degraded"),
            detail=str(residual.get("status") or "runtime_shutdown_drain"),
            observed_ts_ms=int(time.time() * 1000),
            extra=dict(payload or {}),
        )
    except Exception:
        LOG.exception("runtime_shutdown_drain_health_failed")


def run_runtime_shutdown_drain(
    *,
    deadline_s: float | None = None,
    reason: str = "runtime_shutdown",
) -> Dict[str, Any]:
    started = time.perf_counter()
    budget_s = _runtime_shutdown_drain_deadline_s() if deadline_s is None else max(0.0, float(deadline_s))
    deadline = time.monotonic() + float(budget_s)
    errors: list[str] = []
    async_snapshot: Dict[str, Any] = {}
    telemetry_snapshot: Dict[str, Any] = {}

    try:
        async_snapshot = _drain_async_price_writer(_remaining_s(deadline))
    except Exception as exc:
        LOG.exception("runtime_shutdown_async_writer_failed")
        errors.append(f"async_price_writer:{type(exc).__name__}:{exc}")

    try:
        telemetry_snapshot = _drain_telemetry_append_buffers(_remaining_s(deadline))
    except Exception as exc:
        LOG.exception("runtime_shutdown_telemetry_append_buffer_failed")
        errors.append(f"telemetry_append_buffer:{type(exc).__name__}:{exc}")

    deadline_exhausted = bool(_remaining_s(deadline) <= 0.0)
    residual_risk = _runtime_shutdown_residual_risk(
        async_price_writer=async_snapshot,
        telemetry_append_buffer=telemetry_snapshot,
        deadline_exhausted=deadline_exhausted,
        errors=errors,
    )
    duration_ms = int(round((time.perf_counter() - started) * 1000.0))
    payload = {
        "ok": bool(residual_risk.get("ok")) and not errors,
        "reason": str(reason or "runtime_shutdown"),
        "deadline_s": float(budget_s),
        "duration_ms": int(duration_ms),
        "residual_risk": residual_risk,
        "async_price_writer": dict(async_snapshot or {}),
        "telemetry_append_buffer": dict(telemetry_snapshot or {}),
        "errors": list(errors),
        "ts_ms": int(time.time() * 1000),
    }
    _record_runtime_shutdown_drain(payload)
    return payload


def _call_stop_all(
    owner: Any,
    *,
    drain_before_kill: Callable[..., Dict[str, Any]],
    drain_deadline_s: float,
) -> Dict[str, Any]:
    stop_all = getattr(owner, "stop_all", None)
    if not callable(stop_all):
        return {"ok": True, "status": "stop_all_missing"}
    try:
        return dict(
            stop_all(
                drain_before_kill=drain_before_kill,
                drain_deadline_s=float(drain_deadline_s),
            )
            or {}
        )
    except TypeError as exc:
        message = str(exc)
        if (
            "drain_before_kill" not in message
            and "drain_deadline_s" not in message
            and "unexpected keyword" not in message
            and "got an unexpected" not in message
        ):
            raise
        # Older/test doubles without the drain hook still stop; the caller runs
        # the drain afterwards so residual risk is still recorded.
        return dict(stop_all() or {})


def _run_broker_shutdown_risk(*, shutdown_reason: str) -> Dict[str, Any]:
    global _BROKER_SHUTDOWN_RISK_RESULT
    with _BROKER_SHUTDOWN_RISK_LOCK:
        if _BROKER_SHUTDOWN_RISK_RESULT is not None:
            result = dict(_BROKER_SHUTDOWN_RISK_RESULT or {})
            result["duplicate_runtime_shutdown"] = True
            return result

        try:
            from engine.execution.broker_shutdown_risk import handle_broker_shutdown_risk

            result = dict(
                handle_broker_shutdown_risk(
                    policy=os.environ.get("BROKER_SHUTDOWN_POLICY"),
                    engine_mode=os.environ.get("ENGINE_MODE", "safe"),
                    timeout_s=_broker_shutdown_timeout_s(),
                    command_id=_BROKER_SHUTDOWN_COMMAND_ID,
                    actor=os.environ.get("BROKER_SHUTDOWN_ACTOR", "runtime_shutdown"),
                    reason=str(shutdown_reason or "runtime_shutdown"),
                    source="engine.runtime.shutdown",
                    require_explicit_live_policy=True,
                )
                or {}
            )
        except Exception as exc:
            LOG.exception("runtime_shutdown_broker_risk_failed")
            result = {
                "ok": False,
                "status": "runtime_shutdown_broker_risk_exception",
                "error": f"{type(exc).__name__}: {exc}",
            }
        _BROKER_SHUTDOWN_RISK_RESULT = dict(result or {})
        return result


def runtime_shutdown(
    *,
    JOBS: Optional[Any] = None,
    SUPERVISOR: Optional[Any] = None,
    shutdown_reason: str = "runtime_shutdown",
) -> None:
    shutdown_ts_ms = int(time.time() * 1000)
    drain_deadline_s = _runtime_shutdown_drain_deadline_s()
    drain_result: Dict[str, Any] = {}

    def _drain_once(
        *,
        reason: str = "runtime_shutdown_pre_sigkill",
        deadline_s: float | None = None,
    ) -> Dict[str, Any]:
        nonlocal drain_result
        if not drain_result:
            drain_result = run_runtime_shutdown_drain(
                deadline_s=(float(drain_deadline_s) if deadline_s is None else max(0.0, float(deadline_s))),
                reason=str(reason or shutdown_reason or "runtime_shutdown_pre_sigkill"),
            )
        return dict(drain_result or {})

    lifecycle = {}
    try:
        from engine.runtime.lifecycle_state import get_state
        with _storage_timeout_ctx():
            lifecycle = get_state() or {}
    except Exception:
        lifecycle = {}

    try:
        from engine.runtime.event_log import append_event

        with _storage_timeout_ctx():
            append_event(
                event_type="runtime_shutdown_start",
                event_source="runtime.shutdown",
                entity_type="runtime",
                entity_id="shutdown",
                payload={
                    "jobs_present": bool(JOBS is not None),
                    "supervisor_present": bool(SUPERVISOR is not None),
                    "dashboard_bound_ts_ms": str((lifecycle or {}).get("dashboard_bound_ts_ms") or ""),
                    "dashboard_bound_detail": str((lifecycle or {}).get("dashboard_bound_detail") or ""),
                    "lifecycle_state": str((lifecycle or {}).get("state") or ""),
                    "lifecycle_detail": str((lifecycle or {}).get("detail") or ""),
                    "ts_ms": int(shutdown_ts_ms),
                },
                ts_ms=int(shutdown_ts_ms),
                best_effort=True,
            )
    except Exception:
        LOG.exception("runtime_shutdown_start_event_failed")

    broker_risk_result = _run_broker_shutdown_risk(shutdown_reason=str(shutdown_reason or "runtime_shutdown"))
    try:
        from engine.runtime.event_log import append_event

        with _storage_timeout_ctx():
            append_event(
                event_type="runtime_shutdown_broker_risk",
                event_source="runtime.shutdown",
                entity_type="runtime",
                entity_id="shutdown",
                payload={
                    "shutdown_reason": str(shutdown_reason or "runtime_shutdown"),
                    "broker_risk": dict(broker_risk_result or {}),
                    "ts_ms": int(time.time() * 1000),
                },
                ts_ms=int(time.time() * 1000),
                best_effort=True,
            )
    except Exception:
        LOG.exception("runtime_shutdown_broker_risk_event_failed")

    # Stop jobs first so child processes release DB handles and background
    # activity before the storage layer is asked to checkpoint and close.
    # Whole-runtime stop paths accept a pre-SIGKILL drain hook. Generic older
    # owners still stop, then the post-stop drain below records residual risk.
    try:
        if JOBS is not None:
            try:
                _call_stop_all(
                    JOBS,
                    drain_before_kill=_drain_once,
                    drain_deadline_s=float(drain_deadline_s),
                )
            except Exception:
                LOG.exception("runtime_shutdown_jobs_stop_all_failed")
    except Exception:
        LOG.exception("runtime_shutdown_jobs_outer_failed")

    try:
        if SUPERVISOR is not None:
            try:
                _call_stop_all(
                    SUPERVISOR,
                    drain_before_kill=_drain_once,
                    drain_deadline_s=float(drain_deadline_s),
                )
            except Exception:
                LOG.exception("runtime_shutdown_supervisor_stop_all_failed")
    except Exception:
        LOG.exception("runtime_shutdown_supervisor_outer_failed")

    try:
        from engine.model_scoring import stop_model_scoring_service

        stop_model_scoring_service(timeout_s=2.0)
    except Exception:
        LOG.exception("runtime_shutdown_model_scoring_stop_failed")

    try:
        from engine.runtime.event_runtime import stop_event_runtime
    except Exception:
        LOG.exception("runtime_shutdown_event_runtime_import_failed")
        stop_event_runtime = None  # type: ignore

    if stop_event_runtime is not None:
        try:
            stop_event_runtime(timeout_s=2.0)
        except Exception:
            LOG.exception("runtime_shutdown_event_runtime_stop_failed")

    try:
        from engine.runtime.event_bus import shutdown_event_bus
    except Exception:
        LOG.exception("runtime_shutdown_event_bus_import_failed")
        shutdown_event_bus = None  # type: ignore

    if shutdown_event_bus is not None:
        try:
            shutdown_event_bus()
        except Exception:
            LOG.exception("runtime_shutdown_event_bus_failed")

    # Drain buffered persistence before storage pools close. Supported stop_all
    # paths invoke this hook before SIGKILL; older owners rely on this fallback.
    try:
        _drain_once(reason="runtime_shutdown_post_stop")
    except Exception:
        LOG.exception("runtime_shutdown_drain_failed")

    try:
        from engine.runtime.storage_pg_prices import shutdown_pg_price_storage
    except Exception:
        LOG.exception("runtime_shutdown_pg_price_storage_import_failed")
        shutdown_pg_price_storage = None  # type: ignore

    if shutdown_pg_price_storage is not None:
        try:
            shutdown_pg_price_storage()
        except Exception:
            LOG.exception("runtime_shutdown_pg_price_storage_failed")

    # Flush SQLite WAL + close pooled connections because runtime owns the
    # storage lifecycle. Postgres-backed storage does not understand SQLite
    # PRAGMAs, so skip that block for the Postgres facade.
    try:
        from engine.runtime.storage import connect, close_pooled_connections, shutdown_timeseries_storage  # type: ignore
    except Exception:
        LOG.exception("runtime_shutdown_storage_import_failed")
        connect = None  # type: ignore
        close_pooled_connections = None  # type: ignore
        shutdown_timeseries_storage = None  # type: ignore

    connect_module = str(getattr(connect, "__module__", "")) if connect is not None else ""
    if connect is not None and "storage_pg" not in connect_module:
        try:
            with _storage_timeout_ctx():
                con = connect(readonly=False)
                try:
                    con.execute("PRAGMA synchronous=FULL;")
                except Exception:
                    LOG.exception("runtime_shutdown_pragma_synchronous_failed")
                try:
                    con.execute("PRAGMA wal_checkpoint(RESTART);").fetchall()
                except Exception:
                    try:
                        con.execute("PRAGMA wal_checkpoint(TRUNCATE);").fetchall()
                    except Exception:
                        try:
                            con.execute("PRAGMA wal_checkpoint(PASSIVE);").fetchall()
                        except Exception:
                            LOG.exception("runtime_shutdown_wal_checkpoint_failed")
                try:
                    con.commit()
                except Exception:
                    try:
                        con.rollback()
                    except Exception:
                        LOG.exception("runtime_shutdown_db_rollback_failed")
                    LOG.exception("runtime_shutdown_db_commit_failed")
                try:
                    con.close()
                except Exception:
                    LOG.exception("runtime_shutdown_db_close_failed")
        except Exception:
            LOG.exception("runtime_shutdown_db_connect_failed")

    if close_pooled_connections is not None:
        try:
            close_pooled_connections()
        except Exception:
            LOG.exception("runtime_shutdown_close_pooled_connections_failed")

    if shutdown_timeseries_storage is not None:
        try:
            shutdown_timeseries_storage(timeout_s=5.0)
        except Exception:
            LOG.exception("runtime_shutdown_timeseries_shutdown_failed")

    try:
        from engine.runtime.event_log import append_event, shutdown_event_log_buffer

        with _storage_timeout_ctx():
            append_event(
                event_type="runtime_shutdown_complete",
                event_source="runtime.shutdown",
                entity_type="runtime",
                entity_id="shutdown",
                payload={
                    "ts_ms": int(time.time() * 1000),
                    "duration_ms": int(time.time() * 1000) - int(shutdown_ts_ms),
                    "drain": dict(drain_result or {}),
                },
                ts_ms=int(time.time() * 1000),
                best_effort=True,
            )
        shutdown_event_log_buffer(timeout_s=2.0)
    except Exception:
        LOG.exception("runtime_shutdown_complete_event_failed")

    try:
        flush_logging_handlers()
    except Exception:
        LOG.exception("runtime_shutdown_flush_logging_failed")
