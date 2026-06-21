"""Graceful runtime shutdown helpers for jobs, supervision, and SQLite state.

The shutdown path is intentionally fail-safe: it records runtime events, stops
child processes, checkpoints WAL state, and closes pooled connections without
raising cleanup-time exceptions back into the caller.
"""

from __future__ import annotations

import time
import os
import threading
from contextlib import nullcontext
from typing import Any, Dict, Optional

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
    # Stop process jobs first (best-effort)
    try:
        if JOBS is not None:
            try:
                JOBS.stop_all()
            except Exception:
                LOG.exception("runtime_shutdown_jobs_stop_all_failed")
    except Exception:
        LOG.exception("runtime_shutdown_jobs_outer_failed")

    try:
        if SUPERVISOR is not None:
            try:
                SUPERVISOR.stop_all()
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
        from engine.runtime.async_writer import shutdown_async_writer
    except Exception:
        LOG.exception("runtime_shutdown_async_writer_import_failed")
        shutdown_async_writer = None  # type: ignore

    if shutdown_async_writer is not None:
        try:
            shutdown_async_writer(timeout_s=5.0)
        except Exception:
            LOG.exception("runtime_shutdown_async_writer_failed")

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

    # Give children a moment to exit before DB flush so WAL checkpointing is
    # less likely to race with active writers.
    try:
        time.sleep(0.10)
    except Exception:
        LOG.exception("runtime_shutdown_sleep_failed")

    try:
        from engine.runtime.telemetry_append_buffer import shutdown_telemetry_append_buffers

        shutdown_telemetry_append_buffers(timeout_s=2.0)
    except Exception:
        LOG.exception("runtime_shutdown_telemetry_append_buffer_failed")

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
