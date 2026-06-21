"""
FILE: metrics_collector.py

Job entrypoint or scheduled task for `metrics_collector`.
"""

# engine/runtime/jobs/metrics_collector.py
import os
import time
from typing import Any, Dict
from engine.runtime.failure_diagnostics import log_failure

try:
    import psutil
except Exception:
    psutil = None

from engine.runtime.health import get_health_snapshot
from engine.runtime.locks import acquire_lock, heartbeat_lock, release_lock
from engine.runtime.logging import get_logger
from engine.runtime.metrics_store import init_runtime_metrics_db, write_runtime_snapshot
from engine.runtime.storage import DB_PATH, connect as _db_connect


JOB_NAME = "metrics_collector"
INTERVAL_S = float(os.environ.get("METRICS_COLLECTOR_INTERVAL_S", "30"))
LOCK_TTL_MS = int(os.environ.get("METRICS_COLLECTOR_LOCK_TTL_MS", "60000"))
LOG = get_logger("runtime.metrics_collector")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: Exception, *, once_key: str | None = None, **extra: Any) -> None:
    key = str(once_key or "")
    if key:
        if key in _WARNED_NONFATAL_KEYS:
            return
        _WARNED_NONFATAL_KEYS.add(key)
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.runtime.jobs.metrics_collector",
        extra=extra or None,
        include_health=False,
        persist=False,
    )


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        out = float(value)
        return out if out == out else float(default)
    except Exception as e:
        _warn_nonfatal("METRICS_COLLECTOR_FLOAT_PARSE_FAILED", e, once_key="float_parse")
        return float(default)


def _table_exists(con, table: str) -> bool:
    try:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (str(table),),
        ).fetchone()
        return bool(row)
    except Exception as e:
        _warn_nonfatal(
            "METRICS_COLLECTOR_TABLE_EXISTS_FAILED",
            e,
            once_key=f"table_exists_{table}",
            table=str(table),
        )
        return False


def _collect_snapshot() -> Dict[str, Any]:
    now_ms = int(time.time() * 1000)

    health: Dict[str, Any] = {}
    try:
        health_raw = get_health_snapshot()
        health = dict(health_raw) if isinstance(health_raw, dict) else {"ok": False}
    except Exception:
        health = {"ok": False}

    exec_sup_raw = health.get("execution_supervisor")
    exec_sup: Dict[str, Any] = dict(exec_sup_raw) if isinstance(exec_sup_raw, dict) else {}
    broker_conn_raw = health.get("broker_connection")
    broker_conn: Dict[str, Any] = dict(broker_conn_raw) if isinstance(broker_conn_raw, dict) else {}

    metrics = {
        "health_ok": 1.0 if bool(health.get("ok")) else 0.0,
        "prices_age_s": float(((health.get("prices") or {}).get("age_s")) or -1.0),
        "events_age_s": float(((health.get("events") or {}).get("age_s")) or -1.0),
        "stale_jobs": float(((health.get("job_summary") or {}).get("stale")) or 0.0),
        "job_total": float(((health.get("job_summary") or {}).get("total")) or 0.0),
        "providers_healthy": float(((health.get("providers") or {}).get("healthy")) or 0.0),
        "providers_total": float(((health.get("providers") or {}).get("total")) or 0.0),
        "execution_fills": float(((health.get("execution") or {}).get("n_fills")) or 0.0),
        "execution_last_fill_age_s": float(((health.get("execution") or {}).get("last_fill_age_s")) or -1.0),
        "execution_barrier_allowed": 1.0 if bool((health.get("execution_barrier") or {}).get("allowed")) else 0.0,
        "execution_supervisor_score": float(exec_sup.get("score") or 0.0),
        "execution_supervisor_open_due": float(exec_sup.get("open_due") or 0.0),
        "execution_supervisor_routing_failures": float(exec_sup.get("routing_failures") or 0.0),
        "execution_supervisor_oldest_open_order_age_ms": float(exec_sup.get("oldest_open_order_age_ms") or -1.0),
        "broker_connection_ok": 1.0 if bool(broker_conn.get("ok")) else 0.0,
        "broker_connection_latency_ms": float(broker_conn.get("latency_ms") or 0.0),
    }

    try:
        metrics["db_size_mb"] = round(DB_PATH.stat().st_size / (1024 * 1024), 4) if DB_PATH.exists() else 0.0
    except Exception:
        metrics["db_size_mb"] = 0.0

    if psutil is not None:
        try:
            p = psutil.Process(os.getpid())
            metrics["cpu_percent"] = float(psutil.cpu_percent(interval=0.1))
            metrics["memory_percent"] = float(psutil.virtual_memory().percent)
            metrics["process_rss_mb"] = round(p.memory_info().rss / (1024 * 1024), 4)
            metrics["thread_count"] = float(p.num_threads())
        except Exception as e:
            _warn_nonfatal("METRICS_COLLECTOR_PSUTIL_SNAPSHOT_FAILED", e, once_key="psutil_snapshot")

    con = _db_connect(readonly=True)
    try:
        if _table_exists(con, "alerts"):
            try:
                row = con.execute(
                    "SELECT COUNT(*) FROM alerts WHERE ts_ms >= ?",
                    (int(now_ms - 3600_000),),
                ).fetchone()
                metrics["alerts_last_hour"] = float((row or [0])[0] or 0.0)
            except Exception as e:
                _warn_nonfatal("METRICS_COLLECTOR_ALERTS_LAST_HOUR_FAILED", e, once_key="alerts_last_hour")

            try:
                row = con.execute(
                    "SELECT COUNT(*) FROM alerts WHERE severity = 'CRIT'",
                ).fetchone()
                metrics["critical_alerts_open"] = float((row or [0])[0] or 0.0)
            except Exception as e:
                _warn_nonfatal("METRICS_COLLECTOR_CRITICAL_ALERTS_FAILED", e, once_key="critical_alerts")

        if _table_exists(con, "execution_orders"):
            try:
                row = con.execute(
                    "SELECT COUNT(*) FROM execution_orders WHERE submit_ts_ms >= ?",
                    (int(now_ms - 3600_000),),
                ).fetchone()
                metrics["execution_orders_last_hour"] = float((row or [0])[0] or 0.0)
            except Exception as e:
                _warn_nonfatal("METRICS_COLLECTOR_EXECUTION_ORDERS_LAST_HOUR_FAILED", e, once_key="execution_orders_last_hour")

        if _table_exists(con, "execution_order_idempotency"):
            try:
                row = con.execute(
                    """
                    SELECT
                      SUM(CASE WHEN status='claimed' THEN 1 ELSE 0 END),
                      SUM(CASE WHEN status='submit_inflight_unknown' THEN 1 ELSE 0 END),
                      SUM(CASE WHEN status='submitted' THEN 1 ELSE 0 END),
                      SUM(CASE WHEN status='submission_unrecorded' THEN 1 ELSE 0 END),
                      MIN(CASE WHEN status='claimed' THEN claimed_ts_ms ELSE NULL END),
                      MIN(CASE WHEN status='submit_inflight_unknown' THEN updated_ts_ms ELSE NULL END),
                      MIN(CASE WHEN status='submission_unrecorded' THEN updated_ts_ms ELSE NULL END)
                    FROM execution_order_idempotency
                    """
                ).fetchone()
                metrics["execution_claimed_queue_depth"] = float((row or [0, 0, 0, 0, None, None, None])[0] or 0.0)
                metrics["execution_unknown_submit_depth"] = float((row or [0, 0, 0, 0, None, None, None])[1] or 0.0)
                metrics["execution_submitted_depth"] = float((row or [0, 0, 0, 0, None, None, None])[2] or 0.0)
                metrics["execution_submission_unrecorded_depth"] = float((row or [0, 0, 0, 0, None, None, None])[3] or 0.0)

                oldest_claimed_ts_ms = int((row or [0, 0, 0, 0, None, None, None])[4] or 0)
                oldest_unknown_ts_ms = int((row or [0, 0, 0, 0, None, None, None])[5] or 0)
                oldest_unrecorded_ts_ms = int((row or [0, 0, 0, 0, None, None, None])[6] or 0)

                metrics["execution_oldest_claimed_age_ms"] = float((now_ms - oldest_claimed_ts_ms) if oldest_claimed_ts_ms > 0 else -1.0)
                metrics["execution_oldest_unknown_submit_age_ms"] = float((now_ms - oldest_unknown_ts_ms) if oldest_unknown_ts_ms > 0 else -1.0)
                metrics["execution_oldest_submission_unrecorded_age_ms"] = float((now_ms - oldest_unrecorded_ts_ms) if oldest_unrecorded_ts_ms > 0 else -1.0)
            except Exception as e:
                _warn_nonfatal("METRICS_COLLECTOR_EXECUTION_IDEMPOTENCY_FAILED", e, once_key="execution_idempotency")

        if _table_exists(con, "alpha_decay_runtime_history"):
            try:
                row = con.execute(
                    """
                    SELECT min_throttle_mult, severe_count, warn_count
                    FROM alpha_decay_runtime_history
                    ORDER BY ts_ms DESC
                    LIMIT 1
                    """
                ).fetchone()
                metrics["alpha_decay_min_throttle_mult"] = _safe_float((row or [1.0, 0, 0])[0], 1.0)
                metrics["alpha_decay_severe_count"] = float((row or [1.0, 0, 0])[1] or 0.0)
                metrics["alpha_decay_warn_count"] = float((row or [1.0, 0, 0])[2] or 0.0)
            except Exception as e:
                _warn_nonfatal("METRICS_COLLECTOR_ALPHA_DECAY_FAILED", e, once_key="alpha_decay")

        if _table_exists(con, "broker_connection_health"):
            try:
                row = con.execute(
                    """
                    SELECT ok, latency_ms
                    FROM broker_connection_health
                    ORDER BY ts_ms DESC
                    LIMIT 1
                    """
                ).fetchone()
                metrics["broker_connection_ok_db"] = float((row or [0, 0.0])[0] or 0.0)
                metrics["broker_connection_latency_ms_db"] = float((row or [0, 0.0])[1] or 0.0)
            except Exception as e:
                _warn_nonfatal("METRICS_COLLECTOR_BROKER_CONNECTION_DB_FAILED", e, once_key="broker_connection_db")
    finally:
        con.close()

    return {
        "ts_ms": now_ms,
        "metrics": metrics,
        "tags": {
            "job": JOB_NAME,
        },
    }


def main():
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("metrics_collector must be launched by supervisor")
        raise SystemExit(1)

    init_runtime_metrics_db()

    if not acquire_lock(JOB_NAME, ttl_ms=LOCK_TTL_MS):
        raise SystemExit(2)

    try:
        while True:
            try:
                heartbeat_lock(JOB_NAME, ttl_ms=LOCK_TTL_MS)
                write_runtime_snapshot(_collect_snapshot())
            except Exception as e:
                try:
                    LOG.exception("metrics_collector_loop_failed", extra={"event": "metrics_collector_loop_failed"})
                except Exception as log_error:
                    _warn_nonfatal("METRICS_COLLECTOR_LOOP_EXCEPTION_LOG_FAILED", log_error, once_key="loop_exception_log")
                try:
                    write_runtime_snapshot(
                        {
                            "ts_ms": int(time.time() * 1000),
                            "metrics": {
                                "health_ok": 0.0,
                                "metrics_collector_error": 1.0,
                            },
                            "tags": {
                                "job": JOB_NAME,
                                "error": str(e),
                            },
                        }
                    )
                except Exception as snapshot_error:
                    _warn_nonfatal("METRICS_COLLECTOR_ERROR_SNAPSHOT_WRITE_FAILED", snapshot_error, once_key="error_snapshot_write")
            time.sleep(INTERVAL_S)
    finally:
        try:
            release_lock(JOB_NAME)
        except Exception as e:
            _warn_nonfatal("METRICS_COLLECTOR_RELEASE_LOCK_FAILED", e, once_key="release_lock")


if __name__ == "__main__":
    main()
