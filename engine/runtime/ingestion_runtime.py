"""
FILE: ingestion_runtime.py

Runtime subsystem module for `ingestion_runtime`.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, cast

try:
    import psutil
except Exception as e:
    psutil = None
    _PSUTIL_IMPORT_ERROR = e
else:
    _PSUTIL_IMPORT_ERROR = None

from engine.data.provider_registry import get_enabled_market_data_job_names
from engine.runtime.ingestion_shards import (
    canonical_shard_env,
    current_ingestion_shard,
    ingestion_shard_job_name,
    ingestion_state_key,
)
from engine.runtime.ipc import publish_channel_state, publish_message
from engine.runtime.feed_truth import annotate_provider_map_liveness
from engine.runtime.job_registry import ALLOWED_JOBS, INGESTION_DAEMON_JOBS
from engine.runtime.platform import default_local_log_dir
from engine.runtime.runtime_meta import meta_get
from engine.runtime.storage import (
    acquire_job_lock,
    connect_ro,
    init_db,
    put_job_heartbeat,
    release_job_lock,
    run_write_txn,
    touch_job_lock,
)
from services.data_source_manager import desired_ingestion_jobs, get_manager

from engine.runtime.alerts import emit_alert
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.lifecycle_state import set_state, WARMING_UP, DEGRADED
from engine.runtime.log_retention import rotate_log_if_needed
from engine.runtime.logging import get_logger, log_event
from engine.runtime.metrics import emit_counter, emit_gauge, emit_timing
from engine.runtime.observability import backoff_delay_s, record_component_health

JOB_NAME = "ingestion_runtime"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()
INGESTION_SHARD = current_ingestion_shard()
INGESTION_RUNTIME_JOB_LOCK_NAME = ingestion_shard_job_name(JOB_NAME, INGESTION_SHARD)
INGESTION_STATE_KEY = ingestion_state_key("ingestion_state", INGESTION_SHARD)
_SHARD_AWARE_CHILD_JOBS = {"poll_prices", "options_poll"}

CHILD_JOBS_CSV = str(os.environ.get("INGESTION_CHILD_JOBS", "")).strip()
_SAFE_NO_CREDENTIAL_CHILD_JOBS = {"poll_prices"}
CHILD_MAX_RESTARTS = int(os.environ.get("INGESTION_RUNTIME_CHILD_MAX_RESTARTS", "10"))
CHILD_RESTART_WINDOW_S = float(os.environ.get("INGESTION_RUNTIME_CHILD_RESTART_WINDOW_S", "300.0"))
_RESTART_GUARD_JOB_LOCK_PREFIX = "ingestion_restart_guard/v1"
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
RESTART_BASE_S = float(os.environ.get("INGESTION_RUNTIME_RESTART_BASE_S", "2.0"))
RESTART_MAX_S = float(os.environ.get("INGESTION_RUNTIME_RESTART_MAX_S", "60.0"))
HEARTBEAT_EVERY_S = float(os.environ.get("INGESTION_RUNTIME_HEARTBEAT_EVERY_S", "2.0"))
STATE_PUBLISH_EVERY_S = float(os.environ.get("INGESTION_RUNTIME_STATE_PUBLISH_EVERY_S", "2.0"))
PRICE_MAX_AGE_S = float(os.environ.get("PRICE_MAX_AGE_S", "15.0"))
CHILD_STARTUP_GRACE_S = float(os.environ.get("INGESTION_RUNTIME_CHILD_STARTUP_GRACE_S", "30.0"))
CONTROL_PLANE_REFRESH_S = float(os.environ.get("INGESTION_RUNTIME_CONTROL_PLANE_REFRESH_S", "5.0"))
SPAWN_RETRY_ATTEMPTS = max(1, int(os.environ.get("INGESTION_RUNTIME_SPAWN_RETRY_ATTEMPTS", "2")))
SPAWN_RETRY_BASE_S = max(0.0, float(os.environ.get("INGESTION_RUNTIME_SPAWN_RETRY_BASE_S", "0.1")))
SPAWN_RETRY_MAX_S = max(0.0, float(os.environ.get("INGESTION_RUNTIME_SPAWN_RETRY_MAX_S", "1.0")))
STARTUP_PRIORITY_CHILD_JOB = str(os.environ.get("INGESTION_RUNTIME_STARTUP_PRIORITY_CHILD_JOB", "poll_prices")).strip() or "poll_prices"
NONCRITICAL_CHILD_STARTUP_DELAY_S = max(
    0.0,
    float(os.environ.get("INGESTION_RUNTIME_NONCRITICAL_CHILD_STARTUP_DELAY_S", "60.0") or 60.0),
)
NONCRITICAL_CHILD_POST_FIRST_TICK_DELAY_S = max(
    0.0,
    float(os.environ.get("INGESTION_RUNTIME_NONCRITICAL_POST_FIRST_TICK_DELAY_S", "90.0") or 90.0),
)

log = get_logger("engine.runtime.ingestion_runtime")

_STOP = False
_PROVIDER_ALERT_STATE = {}
_CHILDREN_LOCK = threading.RLock()
_SUPERVISOR_SNAPSHOT_CACHE_LOCK = threading.RLock()
_SUPERVISOR_SNAPSHOT_CACHE: Dict[str, Dict[str, object]] = {}
_DATA_SOURCE_CONFIG_SNAPSHOT_NAMES = (
    "child_control_plane",
    "enabled_price_providers",
)
_LAST_DATA_SOURCES_RELOAD_TS_MS: Optional[int] = None


def _env_float_clamped(name: str, default: float, minimum: float, maximum: float) -> float:
    try:
        value = float(os.environ.get(str(name), str(default)) or default)
    except Exception:
        value = float(default)
    return max(float(minimum), min(float(maximum), float(value)))


SUPERVISOR_SNAPSHOT_CACHE_TTL_S = _env_float_clamped(
    "INGESTION_RUNTIME_SNAPSHOT_CACHE_TTL_S",
    1.0,
    0.0,
    5.0,
)

# GLOBAL SNAPSHOT STATE (REQUIRED)
_INGESTION_STATE = {
    "last_tick_ts_ms": 0,
    "last_publish_ts_ms": 0,
    "healthy_providers": 0,
    "running": False,
    "stale": True,
}


def _snapshot_copy(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _snapshot_copy(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_snapshot_copy(v) for v in value]
    if isinstance(value, tuple):
        return tuple(_snapshot_copy(v) for v in value)
    if isinstance(value, set):
        return {str(v) for v in value}
    return value


def invalidate_supervisor_snapshot_cache(*names: str) -> None:
    """Clear ingestion-supervisor process caches after local control-plane writes."""

    with _SUPERVISOR_SNAPSHOT_CACHE_LOCK:
        if not names:
            _SUPERVISOR_SNAPSHOT_CACHE.clear()
            return
        for name in names:
            _SUPERVISOR_SNAPSHOT_CACHE.pop(str(name), None)


def _supervisor_snapshot_cache_has_any(names: tuple[str, ...]) -> bool:
    with _SUPERVISOR_SNAPSHOT_CACHE_LOCK:
        return any(str(name) in _SUPERVISOR_SNAPSHOT_CACHE for name in names)


def _supervisor_snapshot_get_or_load(name: str, loader: Callable[[], Any]) -> Any:
    ttl_s = float(SUPERVISOR_SNAPSHOT_CACHE_TTL_S)
    if ttl_s <= 0.0:
        return loader()

    now = time.monotonic()
    key = str(name)
    with _SUPERVISOR_SNAPSHOT_CACHE_LOCK:
        entry = _SUPERVISOR_SNAPSHOT_CACHE.get(key)
        if entry and now < float(entry.get("expires_at") or 0.0):
            return _snapshot_copy(entry.get("value"))

    value = loader()
    with _SUPERVISOR_SNAPSHOT_CACHE_LOCK:
        _SUPERVISOR_SNAPSHOT_CACHE[key] = {
            "expires_at": float(now + ttl_s),
            "value": _snapshot_copy(value),
        }
    return _snapshot_copy(value)


def _read_data_sources_reload_ts_ms_uncached() -> int:
    con = None
    try:
        con = connect_ro()
        row = con.execute(
            "SELECT value FROM runtime_meta WHERE key=?",
            ("data_sources_reload_ts_ms",),
        ).fetchone()
        return _safe_int(_row_first_value(row), 0)
    except Exception as e:
        _warn_failure("INGESTION_RUNTIME_DATA_SOURCES_RELOAD_MARKER_LOOKUP_FAILED", e)
        return 0
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn_failure("INGESTION_RUNTIME_DATA_SOURCES_RELOAD_MARKER_CLOSE_FAILED", e)


def _invalidate_supervisor_snapshots_if_data_sources_changed() -> None:
    global _LAST_DATA_SOURCES_RELOAD_TS_MS

    marker_ts_ms = int(_read_data_sources_reload_ts_ms_uncached())
    if marker_ts_ms <= 0:
        return

    previous = _LAST_DATA_SOURCES_RELOAD_TS_MS
    _LAST_DATA_SOURCES_RELOAD_TS_MS = int(marker_ts_ms)
    if previous == int(marker_ts_ms):
        return

    if previous is not None or _supervisor_snapshot_cache_has_any(_DATA_SOURCE_CONFIG_SNAPSHOT_NAMES):
        invalidate_supervisor_snapshot_cache(*_DATA_SOURCE_CONFIG_SNAPSHOT_NAMES)


def _row_first_value(row: object) -> object:
    if row is None:
        return None
    try:
        return cast(Any, row)[0]  # storage row, tuple, list
    except Exception as e:
        _warn_failure("INGESTION_RUNTIME_ROW_FIRST_VALUE_FAILED", e, row_type=type(row).__name__)
        return row


def _dict_or_empty(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {str(k): v for k, v in value.items()}


def _list_or_empty(value: Any) -> List[Any]:
    return list(value) if isinstance(value, list) else []


def _has_first_price_tick() -> bool:
    return bool(str(meta_get("first_price_ts_ms") or "").strip())


def _first_price_tick_age_s(*, now_ts: Optional[float] = None) -> Optional[float]:
    raw = str(meta_get("first_price_ts_ms") or "").strip()
    if not raw:
        return None
    try:
        first_tick_s = int(raw) / 1000.0
    except Exception as e:
        _warn_failure(
            "INGESTION_RUNTIME_FIRST_TICK_AGE_PARSE_FAILED",
            e,
            raw_value=raw,
        )
        return None
    current_ts = float(now_ts if now_ts is not None else time.time())
    return max(0.0, current_ts - first_tick_s)


def _should_defer_child_start(
    job_name: str,
    *,
    startup_ts: float,
    now_ts: Optional[float] = None,
) -> bool:
    job_s = str(job_name or "").strip()
    if not job_s or job_s == str(STARTUP_PRIORITY_CHILD_JOB):
        return False
    current_ts = float(now_ts if now_ts is not None else time.time())
    if not _has_first_price_tick():
        if float(NONCRITICAL_CHILD_STARTUP_DELAY_S) <= 0.0:
            return False
        return (current_ts - float(startup_ts)) < float(NONCRITICAL_CHILD_STARTUP_DELAY_S)
    if float(NONCRITICAL_CHILD_POST_FIRST_TICK_DELAY_S) <= 0.0:
        return False
    first_tick_age_s = _first_price_tick_age_s(now_ts=current_ts)
    if first_tick_age_s is None:
        return False
    return float(first_tick_age_s) < float(NONCRITICAL_CHILD_POST_FIRST_TICK_DELAY_S)


def _safe_json_dict(raw: Any) -> Dict[str, Any]:
    try:
        parsed = json.loads(str(raw or "{}"))
    except Exception as e:
        _warn_failure(
            "INGESTION_RUNTIME_SAFE_JSON_DICT_FAILED",
            e,
            raw_type=type(raw).__name__,
        )
        return {}
    return _dict_or_empty(parsed)


def _safe_int(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return int(default)
    try:
        return int(value if value is not None else default)
    except Exception as e:
        _warn_failure(
            "INGESTION_RUNTIME_SAFE_INT_FAILED",
            e,
            value_type=type(value).__name__,
        )
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return float(default)
    try:
        return float(value if value is not None else default)
    except Exception as e:
        _warn_failure(
            "INGESTION_RUNTIME_SAFE_FLOAT_FAILED",
            e,
            value_type=type(value).__name__,
        )
        return float(default)


def _child_proc(info: Dict[str, object]) -> Optional[subprocess.Popen]:
    proc = info.get("proc")
    return proc if isinstance(proc, subprocess.Popen) else None


def _pid_is_running(pid: int) -> bool:
    try:
        pid = int(pid or 0)
    except Exception:
        pid = 0

    if pid <= 0:
        return False

    try:
        os.kill(int(pid), 0)
        return True
    except Exception as e:
        _warn_failure("INGESTION_RUNTIME_PID_RUNNING_CHECK_FAILED", e, pid=int(pid))
        return False


def _terminate_pid_tree(pid: int) -> bool:
    try:
        pid = int(pid or 0)
    except Exception:
        pid = 0

    if pid <= 0 or pid == PID:
        return False

    if not _pid_is_running(pid):
        return False

    try:
        os.kill(int(pid), signal.SIGTERM)
        return True
    except Exception as e:
        _warn_failure("INGESTION_RUNTIME_TERMINATE_PID_TREE_FAILED", e, pid=int(pid))
        return False


def _terminate_stale_child_processes(child_jobs: List[str]) -> None:
    child_names = [str(name).strip() for name in (child_jobs or []) if str(name).strip()]
    if not child_names:
        return
    liveness_names = list(dict.fromkeys(_child_liveness_job_name(name) for name in child_names if _child_liveness_job_name(name)))

    con = None
    stale_pids = set()
    try:
        con = connect_ro()
        placeholders = ",".join("?" for _ in liveness_names)
        hb_rows = con.execute(
            f"""
            SELECT pid
            FROM job_heartbeats
            WHERE job_name IN ({placeholders})
            """,
            tuple(liveness_names),
        ).fetchall() or []
        lock_rows = con.execute(
            f"""
            SELECT pid
            FROM job_locks
            WHERE job_name IN ({placeholders})
            """,
            tuple(liveness_names),
        ).fetchall() or []
        for row in list(hb_rows) + list(lock_rows):
            try:
                stale_pids.add(_safe_int(_row_first_value(row), 0))
            except Exception as e:
                _warn_failure("INGESTION_RUNTIME_STALE_CHILD_PID_PARSE_FAILED", e, row=repr(row))
                continue
    except Exception as e:
        stale_pids = set()
        _warn_failure(
            "INGESTION_RUNTIME_STALE_CHILDREN_LOOKUP_FAILED",
            e,
            jobs=[str(name) for name in child_names],
            liveness_jobs=[str(name) for name in liveness_names],
        )
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn_failure("INGESTION_RUNTIME_STALE_CHILDREN_CLOSE_FAILED", e)

    for job_name in child_names:
        for pid in _find_job_process_pids(job_name):
            if pid > 0:
                stale_pids.add(int(pid))

    for pid in sorted(stale_pids):
        if pid <= 0 or pid == PID:
            continue
        if not _pid_is_running(pid):
            continue
        if _terminate_pid_tree(pid):
            log_event(
                log,
                logging.WARNING,
                "ingestion_runtime_terminated_stale_child",
                component="engine.runtime.ingestion_runtime",
                extra={
                    "pid": int(pid),
                    "jobs": list(child_names),
                },
            )


def _find_job_process_pids(job_name: str) -> List[int]:
    if psutil is None:
        return []

    try:
        script_path = str(_resolve_child_script(job_name)).lower()
        script_name = Path(script_path).name.lower()
    except Exception as e:
        _warn_failure("INGESTION_RUNTIME_CHILD_SCRIPT_RESOLVE_FAILED", e, job=str(job_name))
        return []

    matches = set()
    for proc in psutil.process_iter(attrs=["pid", "cmdline"]):
        try:
            pid = int(proc.info.get("pid") or 0)
            if pid <= 0 or pid == PID:
                continue
            cmdline = proc.info.get("cmdline") or []
            joined = " ".join(str(part) for part in cmdline if part).lower()
            if not joined:
                continue
            if script_path in joined or script_name in joined:
                matches.add(pid)
        except Exception as e:
            _warn_failure("INGESTION_RUNTIME_PROCESS_ITERATION_FAILED", e, job=str(job_name))
            continue
    return sorted(matches)


def _existing_child_process_state(job_name: str) -> Dict[str, object]:
    # Heartbeat state is treated as authoritative only when it is both fresh and
    # tied to a currently running PID.
    con = None
    liveness_job_name = _child_liveness_job_name(job_name)
    try:
        con = connect_ro()
        row = con.execute(
            """
            SELECT pid, ts_ms, extra_json
            FROM job_heartbeats
            WHERE job_name = ?
            """,
            (str(liveness_job_name),),
        ).fetchone()
    except Exception as e:
        row = None
        _warn_failure(
            "INGESTION_RUNTIME_EXISTING_CHILD_STATE_LOOKUP_FAILED",
            e,
            job=str(job_name),
            liveness_job=str(liveness_job_name),
        )
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn_failure(
                "INGESTION_RUNTIME_EXISTING_CHILD_STATE_CLOSE_FAILED",
                e,
                job=str(job_name),
                liveness_job=str(liveness_job_name),
            )

    if not row:
        return {"active": False, "pid": 0, "ts_ms": 0, "age_ms": 10**12}

    try:
        pid = int(row[0] or 0)
    except Exception:
        pid = 0
    try:
        ts_ms = int(row[1] or 0)
    except Exception:
        ts_ms = 0

    extra = _safe_json_dict(row[2])

    age_ms = max(0, int(time.time() * 1000) - ts_ms) if ts_ms > 0 else 10**12
    stale_ms = _child_heartbeat_stale_ms(extra)
    active = bool(pid > 0 and _pid_is_running(pid) and age_ms < stale_ms)
    return {
        "active": active,
        "pid": pid,
        "ts_ms": ts_ms,
        "age_ms": age_ms,
        "stale_ms": stale_ms,
    }


def _children_snapshot(children: Optional[Dict[str, Dict[str, object]]]) -> Dict[str, Dict[str, object]]:
    # Heartbeats publish a copy of child state so external readers never touch
    # the mutable internal child map directly.
    snapshot: Dict[str, Dict[str, object]] = {}
    with _CHILDREN_LOCK:
        for name, info in (children or {}).items():
            snapshot[str(name)] = {
                "job": str(info.get("job") or name),
                "liveness_job": _child_liveness_job_name(str(info.get("job") or name)),
                "pid": _safe_int(info.get("pid"), 0),
                "running": bool(info.get("running")),
                "last_exit_rc": info.get("last_exit_rc"),
                "last_error": info.get("last_error"),
                "last_start_ts": info.get("last_start_ts"),
                "restart_disabled": bool(info.get("restart_disabled")),
                "restart_guard": dict(
                    info.get("restart_guard")
                    if isinstance(info.get("restart_guard"), dict)
                    else _restart_guard_snapshot(str(info.get("job") or name))
                ),
            }
    return snapshot


def _emit_ingestion_heartbeat(children: Optional[Dict[str, Dict[str, object]]]) -> None:
    # Ingestion runtime owns a dedicated heartbeat because health/lifecycle code
    # depends on it even when the main supervision loop is busy.
    writer_diagnostics = _ingestion_writer_diagnostics()
    extra_json = json.dumps(
        {
            "children": _children_snapshot(children),
            "market_state": dict(_INGESTION_STATE),
            "shard": INGESTION_SHARD.as_dict(),
            "liveness_job_name": str(INGESTION_RUNTIME_JOB_LOCK_NAME),
            "supervisor_owner_pid": _safe_int(os.environ.get("ENGINE_RUNTIME_OWNER_PID"), 0),
            "writer_diagnostics": writer_diagnostics,
            "heartbeat_every_s": float(HEARTBEAT_EVERY_S),
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    touch_job_lock(INGESTION_RUNTIME_JOB_LOCK_NAME, OWNER, PID, best_effort=True)
    put_job_heartbeat(INGESTION_RUNTIME_JOB_LOCK_NAME, OWNER, PID, extra_json=extra_json, best_effort=True)
    emit_gauge(
        "ingestion_running_children",
        int(
            sum(
                1
                for info in (children or {}).values()
                if isinstance(info, dict) and bool(info.get("running"))
            )
        ),
        component="engine.runtime.ingestion_runtime",
        job=JOB_NAME,
    )
    emit_gauge(
        "ingestion_healthy_providers",
        int(_INGESTION_STATE.get("healthy_providers") or 0),
        component="engine.runtime.ingestion_runtime",
        job=JOB_NAME,
    )
    emit_gauge(
        "ingestion_runtime_stale",
        1.0 if bool(_INGESTION_STATE.get("stale")) else 0.0,
        component="engine.runtime.ingestion_runtime",
        job=JOB_NAME,
    )


def _queue_pressure(snapshot: Dict[str, object], *, depth_key: str = "queue_depth", max_key: str = "queue_maxsize") -> bool:
    depth = _safe_int(snapshot.get(depth_key), 0)
    maximum = _safe_int(snapshot.get(max_key), 0)
    return bool(maximum > 0 and depth >= int(maximum * 0.80))


def _ingestion_writer_diagnostics() -> Dict[str, object]:
    diagnostics: Dict[str, object] = {
        "ok": True,
        "degraded": False,
        "degraded_reasons": [],
    }
    reasons: list[str] = []
    try:
        from engine.runtime.ingestion_tuning import ingestion_tuning_snapshot

        tuning = dict(ingestion_tuning_snapshot(pg_pool_role="ingestion") or {})
        tuning.pop("bounds", None)
        diagnostics["tuning"] = tuning
        if not bool(tuning.get("ok", True)):
            reasons.append("unsafe_tuning")
    except Exception as e:
        _warn_failure("INGESTION_RUNTIME_TUNING_SNAPSHOT_FAILED", e)
        diagnostics["tuning"] = {"ok": False, "error": f"{type(e).__name__}:{e}"}
        reasons.append("tuning_snapshot_failed")

    try:
        from engine.runtime.async_writer import get_async_writer

        async_snapshot = dict(get_async_writer().get_snapshot() or {})
        diagnostics["async_price_writer"] = async_snapshot
        if bool(async_snapshot.get("enabled")):
            emit_gauge(
                "ingestion_async_price_writer_queue_depth",
                _safe_int(async_snapshot.get("queue_depth"), 0),
                component="engine.runtime.ingestion_runtime",
                job=JOB_NAME,
            )
            emit_gauge(
                "ingestion_async_price_writer_spool_pending_bytes",
                _safe_int(async_snapshot.get("spool_pending_bytes"), 0),
                component="engine.runtime.ingestion_runtime",
                job=JOB_NAME,
            )
            if _queue_pressure(async_snapshot):
                reasons.append("async_price_writer_queue_pressure")
            if float(async_snapshot.get("spool_bytes_fill_ratio") or 0.0) >= 0.80:
                reasons.append("async_price_writer_spool_byte_pressure")
            if bool(async_snapshot.get("backpressure_active")):
                reasons.append("async_price_writer_backpressure")
            if _safe_int(async_snapshot.get("dropped_rows"), 0) > 0:
                reasons.append("async_price_writer_dropped_rows")
            residual_loss_rows = _safe_int(
                async_snapshot.get("residual_loss_rows", async_snapshot.get("residual_dropped_rows")),
                0,
            )
            if residual_loss_rows > 0:
                reasons.append("async_price_writer_residual_loss_rows")
            if _safe_int(async_snapshot.get("dead_letters"), 0) > 0:
                reasons.append("async_price_writer_dead_letters")
            if _safe_int(async_snapshot.get("spool_corruption_events"), 0) > 0:
                reasons.append("async_price_writer_spool_corruption")
    except Exception as e:
        _warn_failure("INGESTION_RUNTIME_ASYNC_WRITER_SNAPSHOT_FAILED", e)
        diagnostics["async_price_writer"] = {"ok": False, "error": f"{type(e).__name__}:{e}"}

    try:
        from engine.runtime.storage_pg_prices import get_price_storage

        price_storage = dict(get_price_storage().get_snapshot() or {})
        diagnostics["price_storage"] = price_storage
        if bool(price_storage.get("enabled")) and not bool(price_storage.get("ok", True)):
            reasons.append("price_storage_not_ok")
    except Exception as e:
        _warn_failure("INGESTION_RUNTIME_PRICE_STORAGE_SNAPSHOT_FAILED", e)
        diagnostics["price_storage"] = {"ok": False, "error": f"{type(e).__name__}:{e}"}

    try:
        from engine.runtime.telemetry_append_buffer import get_telemetry_append_buffer_snapshot

        telemetry = dict(get_telemetry_append_buffer_snapshot() or {})
        diagnostics["telemetry_append_buffer"] = telemetry
        if bool(telemetry.get("enabled")):
            emit_gauge(
                "ingestion_telemetry_append_buffer_queue_depth",
                _safe_int(telemetry.get("buffered_rows"), 0),
                component="engine.runtime.ingestion_runtime",
                job=JOB_NAME,
            )
            emit_gauge(
                "ingestion_telemetry_append_buffer_oldest_age_ms",
                _safe_int(telemetry.get("oldest_age_ms"), 0),
                component="engine.runtime.ingestion_runtime",
                job=JOB_NAME,
            )
            if _queue_pressure(telemetry, depth_key="buffered_rows", max_key="buffer_max_rows"):
                reasons.append("telemetry_append_buffer_pressure")
            if bool(telemetry.get("backpressure_active")):
                reasons.append("telemetry_append_buffer_backpressure")
            if _safe_int(telemetry.get("dropped_rows"), 0) > 0:
                reasons.append("telemetry_append_buffer_dropped_rows")
            if _safe_int(telemetry.get("flush_failures"), 0) > 0:
                reasons.append("telemetry_append_buffer_flush_failures")
    except Exception as e:
        _warn_failure("INGESTION_RUNTIME_TELEMETRY_BUFFER_SNAPSHOT_FAILED", e)
        diagnostics["telemetry_append_buffer"] = {"ok": False, "error": f"{type(e).__name__}:{e}"}

    try:
        from engine.runtime.ingestion_status import get_pipeline_status

        options_status = dict(get_pipeline_status("options_poll") or {})
        options_meta = _dict_or_empty(options_status.get("meta"))
        options_buffer = {
            "ok": bool(options_status.get("ok", True)),
            "updated_ts_ms": _safe_int(options_status.get("updated_ts_ms"), 0),
            "pending_rows": _safe_int(options_meta.get("durable_buffer_pending_rows"), 0),
            "pending_bytes": _safe_int(options_meta.get("durable_buffer_pending_bytes"), 0),
            "oldest_age_ms": _safe_int(options_meta.get("durable_buffer_oldest_age_ms"), 0),
            "rows_fill_ratio": float(options_meta.get("durable_buffer_rows_fill_ratio") or 0.0),
            "bytes_fill_ratio": float(options_meta.get("durable_buffer_bytes_fill_ratio") or 0.0),
            "backpressure_active": bool(options_meta.get("durable_buffer_backpressure_active")),
            "backpressure_events": _safe_int(options_meta.get("durable_buffer_backpressure_events"), 0),
            "rejected_rows": _safe_int(options_meta.get("durable_buffer_rejected_rows"), 0),
            "dropped_rows": _safe_int(options_meta.get("durable_buffer_dropped_rows"), 0),
            "enqueue_failures": _safe_int(options_meta.get("durable_buffer_enqueue_failures"), 0),
            "replay_failures": _safe_int(options_meta.get("durable_buffer_replay_failures"), 0),
            "delete_failures": _safe_int(options_meta.get("durable_buffer_delete_failures"), 0),
            "corrupt_payload_rows": _safe_int(options_meta.get("durable_buffer_corrupt_payload_rows"), 0),
            "last_error": str(options_meta.get("durable_buffer_last_error") or ""),
        }
        diagnostics["options_poll_durable_buffer"] = options_buffer
        emit_gauge(
            "ingestion_options_poll_durable_buffer_pending_rows",
            int(options_buffer["pending_rows"]),
            component="engine.runtime.ingestion_runtime",
            job=JOB_NAME,
        )
        emit_gauge(
            "ingestion_options_poll_durable_buffer_oldest_age_ms",
            int(options_buffer["oldest_age_ms"]),
            component="engine.runtime.ingestion_runtime",
            job=JOB_NAME,
        )
        if max(float(options_buffer["rows_fill_ratio"]), float(options_buffer["bytes_fill_ratio"])) >= 0.80:
            reasons.append("options_poll_durable_buffer_pressure")
        if bool(options_buffer["backpressure_active"]):
            reasons.append("options_poll_durable_buffer_backpressure")
        if _safe_int(options_buffer.get("rejected_rows"), 0) > 0:
            reasons.append("options_poll_durable_buffer_rejected_rows")
        if _safe_int(options_buffer.get("dropped_rows"), 0) > 0:
            reasons.append("options_poll_durable_buffer_dropped_rows")
        if _safe_int(options_buffer.get("enqueue_failures"), 0) > 0:
            reasons.append("options_poll_durable_buffer_enqueue_failures")
        if _safe_int(options_buffer.get("replay_failures"), 0) > 0:
            reasons.append("options_poll_durable_buffer_replay_failures")
        if _safe_int(options_buffer.get("delete_failures"), 0) > 0:
            reasons.append("options_poll_durable_buffer_delete_failures")
        if _safe_int(options_buffer.get("corrupt_payload_rows"), 0) > 0:
            reasons.append("options_poll_durable_buffer_corruption")
    except Exception as e:
        _warn_failure("INGESTION_RUNTIME_OPTIONS_DURABLE_BUFFER_SNAPSHOT_FAILED", e)
        diagnostics["options_poll_durable_buffer"] = {"ok": False, "error": f"{type(e).__name__}:{e}"}

    try:
        from engine.runtime.timescale_client import get_timescale_snapshot

        timescale = dict(get_timescale_snapshot() or {})
        diagnostics["timescale"] = timescale
        if bool(timescale.get("enabled")):
            emit_gauge(
                "ingestion_timescale_queue_depth",
                _safe_int(timescale.get("queue_depth"), 0),
                component="engine.runtime.ingestion_runtime",
                job=JOB_NAME,
            )
            if _queue_pressure(timescale):
                reasons.append("timescale_queue_pressure")
            metrics = _dict_or_empty(timescale.get("metrics"))
            if bool(metrics.get("backpressure_active")):
                reasons.append("timescale_backpressure")
            if _safe_int(metrics.get("flush_failure_count"), 0) > 0:
                reasons.append("timescale_flush_failures")
    except Exception as e:
        _warn_failure("INGESTION_RUNTIME_TIMESCALE_SNAPSHOT_FAILED", e)
        diagnostics["timescale"] = {"ok": False, "error": f"{type(e).__name__}:{e}"}

    unique_reasons = sorted(set(str(reason) for reason in reasons if str(reason).strip()))
    diagnostics["ok"] = not bool(unique_reasons)
    diagnostics["degraded"] = bool(unique_reasons)
    diagnostics["degraded_reasons"] = unique_reasons
    emit_gauge(
        "ingestion_writer_backpressure_degraded",
        1.0 if unique_reasons else 0.0,
        component="engine.runtime.ingestion_runtime",
        job=JOB_NAME,
    )
    return diagnostics


def _warn_failure(event: str, error: Exception, **extra) -> None:
    log_failure(
        log,
        event=str(event),
        code=str(event),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.runtime.ingestion_runtime",
        extra=extra or None,
        include_health=False,
        persist=True,
    )


if _PSUTIL_IMPORT_ERROR is not None:
    _warn_failure(
        "INGESTION_RUNTIME_PSUTIL_IMPORT_FAILED",
        _PSUTIL_IMPORT_ERROR,
        degradation="process_tree_checks_fall_back_to_pid_signals",
    )


def _load_enabled_price_providers() -> set[str]:
    try:
        manager = get_manager()
        manager.initialize()
        providers = {
            str(row.get("provider_name") or row.get("source_key") or "").strip()
            for row in (manager.list_sources() or [])
            if bool(row.get("enabled"))
            and str(row.get("source_type") or "").strip() == "price_provider"
            and str(row.get("provider_name") or row.get("source_key") or "").strip()
        }
        return providers
    except Exception as e:
        _warn_failure("INGESTION_RUNTIME_ENABLED_PRICE_PROVIDERS_FAILED", e)
        return set()


def _enabled_price_providers() -> set[str]:
    return set(
        _supervisor_snapshot_get_or_load(
            "enabled_price_providers",
            _load_enabled_price_providers,
        )
        or set()
    )


def _heartbeat_loop(children: Dict[str, Dict[str, object]]) -> None:
    # Heartbeat emission runs independently of child supervision so temporary
    # work spikes do not make the whole ingestion runtime look dead.
    next_hb = 0.0
    while not _STOP:
        now = time.time()
        if now >= next_hb:
            try:
                _emit_ingestion_heartbeat(children)
            except Exception as e:
                _warn_failure("INGESTION_RUNTIME_HEARTBEAT_UPDATE_FAILED", e)
            next_hb = now + max(0.5, float(HEARTBEAT_EVERY_S))
        time.sleep(0.25)


def _load_child_heartbeat_rows() -> Dict[str, Dict[str, object]]:
    child_heartbeat_rows: Dict[str, Dict[str, object]] = {}
    con = None
    try:
        con = connect_ro()
        rows = con.execute(
            """
            SELECT job_name, ts_ms, extra_json
            FROM job_heartbeats
            WHERE job_name != ?
            """
            ,
            (INGESTION_RUNTIME_JOB_LOCK_NAME,),
        ).fetchall() or []
    except Exception as e:
        rows = []
        _warn_failure("INGESTION_RUNTIME_POLLING_PRICE_MAX_AGE_LOOKUP_FAILED", e)
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn_failure("INGESTION_RUNTIME_CHILD_HEARTBEAT_CLOSE_FAILED", e)

    for job_name, ts_ms, extra_json in rows:
        parsed_extra = {}
        try:
            parsed_extra = _safe_json_dict(extra_json)
        except Exception:
            parsed_extra = {}
        child_heartbeat_rows[str(job_name)] = {
            "ts_ms": _safe_int(ts_ms, 0),
            "extra": parsed_extra,
        }

    return child_heartbeat_rows


def _child_heartbeat_rows_snapshot() -> Dict[str, Dict[str, object]]:
    cached = _supervisor_snapshot_get_or_load(
        "child_heartbeat_rows",
        _load_child_heartbeat_rows,
    )
    return {
        str(name): _dict_or_empty(row)
        for name, row in _dict_or_empty(cached).items()
    }


def _compute_polling_price_max_age_ms() -> int:
    base_ms = int(PRICE_MAX_AGE_S * 1000.0)

    derived_max_ms: int | None = None
    for row in _child_heartbeat_rows_snapshot().values():
        extra_json = _dict_or_empty(row).get("extra")
        try:
            extra = _dict_or_empty(extra_json)
            if not extra:
                continue
        except Exception as e:
            _warn_failure(
                "INGESTION_RUNTIME_POLLING_PRICE_MAX_AGE_EXTRA_PARSE_FAILED",
                e,
                extra_json=repr(extra_json),
            )
            continue

        telemetry = _child_telemetry_from_heartbeat_extra(extra)
        capabilities = _dict_or_empty(telemetry.get("capabilities"))
        is_streaming = bool(capabilities.get("streaming"))
        is_polling_only = bool(capabilities.get("polling")) and not is_streaming
        poll_seconds = max(0.0, _safe_float(extra.get("poll_seconds"), 0.0))
        if is_polling_only and poll_seconds > 0:
            derived_value = int(max(poll_seconds * 2.5, 45.0) * 1000.0)
            derived_max_ms = max(int(derived_max_ms or 0), int(derived_value))

    return int(derived_max_ms) if derived_max_ms is not None else int(base_ms)


def _polling_price_max_age_ms() -> int:
    return int(
        _supervisor_snapshot_get_or_load(
            "polling_price_max_age_ms",
            _compute_polling_price_max_age_ms,
        )
        or int(PRICE_MAX_AGE_S * 1000.0)
    )


def _handle_stop(signum, frame) -> None:
    global _STOP
    _STOP = True
    try:
        log_event(
            log,
            logging.INFO,
            "ingestion_runtime_stop_signal",
            component="engine.runtime.ingestion_runtime",
            extra={"signum": int(signum or 0)},
        )
    except Exception as e:
        _warn_failure("INGESTION_RUNTIME_STOP_SIGNAL_LOG_FAILED", e, signum=int(signum or 0))


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _job_log_dir() -> Path:
    log_dir = Path(
        os.environ.get("TRADING_LOGS")
        or os.environ.get("LOG_DIR")
        or str(default_local_log_dir().resolve())
    )
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def _child_log_paths(job_name: str) -> tuple[str, str]:
    safe = "".join(c if c.isalnum() or c in ("-", "_", ".") else "_" for c in str(job_name))
    log_dir = _job_log_dir()
    return (
        str((log_dir / f"{safe}.stdout.log").resolve()),
        str((log_dir / f"{safe}.stderr.log").resolve()),
    )


def _child_liveness_job_name(job_name: str) -> str:
    name = str(job_name or "").strip()
    if name in _SHARD_AWARE_CHILD_JOBS:
        return ingestion_shard_job_name(name, INGESTION_SHARD)
    return name


def _restart_guard_limit() -> int:
    return max(1, int(CHILD_MAX_RESTARTS))


def _restart_guard_window_ms() -> int:
    return max(1, int(float(CHILD_RESTART_WINDOW_S) * 1000.0))


def _restart_guard_row_prefix(job_name: str) -> str:
    liveness_name = str(_child_liveness_job_name(job_name) or job_name or "").strip()
    safe_job = "".join(c if c.isalnum() or c in ("-", "_", ".", ":") else "_" for c in liveness_name)
    return f"{_RESTART_GUARD_JOB_LOCK_PREFIX}::{safe_job}::"


def _restart_guard_status_from_expires(
    job_name: str,
    *,
    now_ms: int,
    expires_values: List[int],
    accounting_error: str = "",
) -> Dict[str, object]:
    active_expires = sorted(int(value) for value in expires_values if int(value or 0) > int(now_ms))
    count = int(len(active_expires))
    limit = int(_restart_guard_limit())
    suppressed = bool(count >= limit)
    suppressed_until_ts_ms = 0
    if suppressed and active_expires:
        expire_index = min(len(active_expires) - 1, max(0, count - limit))
        suppressed_until_ts_ms = int(active_expires[expire_index])
    status: Dict[str, object] = {
        "job": str(job_name),
        "liveness_job": _child_liveness_job_name(job_name),
        "count": int(count),
        "limit": int(limit),
        "remaining": max(0, int(limit - count)),
        "window_s": float(CHILD_RESTART_WINDOW_S),
        "suppressed": bool(suppressed),
        "suppressed_until_ts_ms": int(suppressed_until_ts_ms),
        "updated_ts_ms": int(now_ms),
    }
    if accounting_error:
        status["accounting_error"] = str(accounting_error)
    return status


def _restart_guard_snapshot(job_name: str, *, now: Optional[float] = None) -> Dict[str, object]:
    now_s = float(now if now is not None else time.time())
    now_ms = int(now_s * 1000.0)
    prefix = _restart_guard_row_prefix(job_name)
    con = None
    try:
        con = connect_ro()
        rows = con.execute(
            """
            SELECT expires_ms
            FROM job_locks
            WHERE job_name LIKE ?
              AND COALESCE(expires_ms, 0) > ?
            ORDER BY expires_ms ASC
            """,
            (prefix + "%", int(now_ms)),
        ).fetchall() or []
        return _restart_guard_status_from_expires(
            job_name,
            now_ms=int(now_ms),
            expires_values=[_safe_int(_row_first_value(row), 0) for row in rows],
        )
    except Exception as e:
        _warn_failure("INGESTION_RUNTIME_RESTART_GUARD_SNAPSHOT_FAILED", e, job=str(job_name))
        return _restart_guard_status_from_expires(
            job_name,
            now_ms=int(now_ms),
            expires_values=[],
            accounting_error=f"{type(e).__name__}: {e}",
        )
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn_failure("INGESTION_RUNTIME_RESTART_GUARD_SNAPSHOT_CLOSE_FAILED", e, job=str(job_name))


def _restart_guard_latest_attempt_ts_ms(job_name: str) -> int:
    prefix = _restart_guard_row_prefix(job_name)
    con = None
    try:
        con = connect_ro()
        row = con.execute(
            """
            SELECT MAX(acquired_ts_ms)
            FROM job_locks
            WHERE job_name LIKE ?
            """,
            (prefix + "%",),
        ).fetchone()
        return _safe_int(_row_first_value(row), 0)
    except Exception as e:
        _warn_failure("INGESTION_RUNTIME_RESTART_GUARD_LATEST_ATTEMPT_FAILED", e, job=str(job_name))
        return 0
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn_failure("INGESTION_RUNTIME_RESTART_GUARD_LATEST_ATTEMPT_CLOSE_FAILED", e, job=str(job_name))


def _record_child_restart_attempt(
    job_name: str,
    *,
    now: float,
    reason: str,
) -> Dict[str, object]:
    now_ms = int(float(now) * 1000.0)
    window_ms = int(_restart_guard_window_ms())
    prefix = _restart_guard_row_prefix(job_name)
    row_name = f"{prefix}{now_ms}:{time.time_ns()}:{int(PID)}"
    owner = f"{JOB_NAME}:{OWNER}"

    def _write(con) -> List[int]:
        con.execute(
            """
            DELETE FROM job_locks
            WHERE job_name LIKE ?
              AND COALESCE(expires_ms, 0) <= ?
            """,
            (prefix + "%", int(now_ms)),
        )
        con.execute(
            """
            INSERT INTO job_locks(job_name, owner, pid, acquired_ts_ms, heartbeat_ts_ms, expires_ms)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                str(row_name),
                str(owner),
                int(PID),
                int(now_ms),
                int(now_ms),
                int(now_ms + window_ms),
            ),
        )
        rows = con.execute(
            """
            SELECT expires_ms
            FROM job_locks
            WHERE job_name LIKE ?
              AND COALESCE(expires_ms, 0) > ?
            ORDER BY expires_ms ASC
            """,
            (prefix + "%", int(now_ms)),
        ).fetchall() or []
        return [_safe_int(_row_first_value(row), 0) for row in rows]

    try:
        expires_values = run_write_txn(
            _write,
            attempts=1,
            table="job_locks",
            operation="record_ingestion_restart_guard",
            direct=True,
            maintenance=False,
            timeout_s=0.5,
            busy_timeout_ms=500,
        ) or []
        status = _restart_guard_status_from_expires(
            job_name,
            now_ms=int(now_ms),
            expires_values=list(expires_values),
        )
    except Exception as e:
        _warn_failure(
            "INGESTION_RUNTIME_RESTART_GUARD_RECORD_FAILED",
            e,
            job=str(job_name),
            reason=str(reason),
        )
        status = _restart_guard_status_from_expires(
            job_name,
            now_ms=int(now_ms),
            expires_values=[],
            accounting_error=f"{type(e).__name__}: {e}",
        )

    _emit_restart_guard_metrics(job_name, status, reason=str(reason))
    return status


def _clear_child_restart_accounting(
    job_names: Optional[List[str]] = None,
    *,
    reason: str = "manual_override",
) -> Dict[str, object]:
    names = [str(name).strip() for name in list(job_names or []) if str(name).strip()]
    prefixes = [_restart_guard_row_prefix(name) for name in names] if names else [f"{_RESTART_GUARD_JOB_LOCK_PREFIX}::"]

    def _write(con) -> int:
        deleted = 0
        for prefix in prefixes:
            cur = con.execute(
                "DELETE FROM job_locks WHERE job_name LIKE ?",
                (str(prefix) + "%",),
            )
            deleted += max(0, int(getattr(cur, "rowcount", 0) or 0))
        return int(deleted)

    try:
        deleted_rows = int(
            run_write_txn(
                _write,
                attempts=1,
                table="job_locks",
                operation="clear_ingestion_restart_guard",
                direct=True,
                maintenance=False,
                timeout_s=0.5,
                busy_timeout_ms=500,
            )
            or 0
        )
        emit_counter(
            "ingestion_child_restart_guard_cleared_total",
            1,
            component="engine.runtime.ingestion_runtime",
            job=JOB_NAME,
            extra_tags={"reason": str(reason), "scope": "selected" if names else "all"},
        )
        return {
            "ok": True,
            "deleted_rows": int(deleted_rows),
            "job_names": names,
            "reason": str(reason),
        }
    except Exception as e:
        _warn_failure(
            "INGESTION_RUNTIME_RESTART_GUARD_CLEAR_FAILED",
            e,
            jobs=names,
            reason=str(reason),
        )
        return {
            "ok": False,
            "deleted_rows": 0,
            "job_names": names,
            "reason": str(reason),
            "error": f"{type(e).__name__}: {e}",
        }


def clear_child_restart_accounting(
    job_names: Optional[List[str]] = None,
    *,
    reason: str = "manual_override",
) -> Dict[str, object]:
    """Clear persisted ingestion child restart-storm accounting for operator overrides."""

    return _clear_child_restart_accounting(job_names, reason=reason)


def _emit_restart_guard_metrics(job_name: str, status: Dict[str, object], *, reason: str) -> None:
    try:
        emit_counter(
            "ingestion_child_restart_attempts_total",
            1,
            component="engine.runtime.ingestion_runtime",
            job=str(job_name),
            extra_tags={
                "reason": str(reason),
                "suppressed": str(bool(status.get("suppressed"))).lower(),
                "liveness_job": str(status.get("liveness_job") or ""),
            },
        )
        emit_gauge(
            "ingestion_child_restart_window_count",
            int(status.get("count") or 0),
            component="engine.runtime.ingestion_runtime",
            job=str(job_name),
            extra_tags={
                "liveness_job": str(status.get("liveness_job") or ""),
                "window_s": str(status.get("window_s") or ""),
            },
        )
    except Exception as e:
        _warn_failure("INGESTION_RUNTIME_RESTART_GUARD_METRICS_FAILED", e, job=str(job_name))


def _publish_child_restart_suppressed(
    job_name: str,
    status: Dict[str, object],
    *,
    reason: str,
    detail: Dict[str, object],
) -> None:
    payload = {
        "job": str(job_name),
        "liveness_job": str(status.get("liveness_job") or _child_liveness_job_name(job_name)),
        "reason": str(reason),
        "restarts": int(status.get("count") or 0),
        "limit": int(status.get("limit") or _restart_guard_limit()),
        "window_s": float(status.get("window_s") or CHILD_RESTART_WINDOW_S),
        "suppressed_until_ts_ms": int(status.get("suppressed_until_ts_ms") or 0),
        "shard": INGESTION_SHARD.as_dict(),
        **dict(detail or {}),
    }
    try:
        emit_counter(
            "ingestion_child_restart_suppressed_total",
            1,
            component="engine.runtime.ingestion_runtime",
            job=str(job_name),
            extra_tags={
                "reason": str(reason),
                "liveness_job": str(payload.get("liveness_job") or ""),
            },
        )
    except Exception as e:
        _warn_failure("INGESTION_RUNTIME_RESTART_SUPPRESSED_METRIC_FAILED", e, job=str(job_name))
    try:
        from engine.runtime.event_log import append_event

        append_event(
            event_type="ingestion_child_restart_suppressed",
            event_source=JOB_NAME,
            entity_type="ingestion_child",
            entity_id=str(payload.get("liveness_job") or job_name),
            payload=payload,
            best_effort=True,
        )
    except Exception as e:
        _warn_failure("INGESTION_RUNTIME_RESTART_SUPPRESSED_EVENT_FAILED", e, job=str(job_name))
    publish_message(
        "market_data",
        "child_restart_guard_triggered",
        payload,
        sender=JOB_NAME,
        best_effort=True,
    )


def _filter_child_candidates_for_shard(requested: List[str]) -> List[str]:
    if not INGESTION_SHARD.enabled:
        return list(requested or [])
    out: List[str] = []
    skipped: List[str] = []
    for raw_name in requested or []:
        name = str(raw_name or "").strip()
        if not name:
            continue
        if name in _SHARD_AWARE_CHILD_JOBS or int(INGESTION_SHARD.index) == 0:
            out.append(name)
        else:
            skipped.append(name)
    if skipped:
        try:
            log.info(
                "ingestion shard %s filtered singleton child jobs: %s",
                INGESTION_SHARD.label,
                sorted(set(skipped)),
            )
        except Exception as e:
            _warn_failure("INGESTION_RUNTIME_SHARD_CHILD_FILTER_LOG_FAILED", e)
    return list(dict.fromkeys(out))


def _compute_child_candidates() -> List[str]:
    requested: List[str] = []
    if CHILD_JOBS_CSV:
        requested = [str(x).strip() for x in CHILD_JOBS_CSV.split(",") if str(x).strip()]
    else:
        try:
            requested = list(
                dict.fromkeys(
                    list(
                        desired_ingestion_jobs(
                            default_jobs=list(INGESTION_DAEMON_JOBS or []) + list(get_enabled_market_data_job_names() or [])
                        )
                        or []
                    )
                )
            )
        except Exception as e:
            _warn_failure("INGESTION_RUNTIME_DESIRED_CHILDREN_LOOKUP_FAILED", e)
            requested = list(dict.fromkeys(list(INGESTION_DAEMON_JOBS or []) + list(get_enabled_market_data_job_names() or [])))

    requested = _filter_child_candidates_for_shard(_safe_no_credential_child_candidates(requested))

    out: List[str] = []
    seen = set()
    missing: List[str] = []

    for name in requested:
        if not name or name == JOB_NAME or name in seen:
            continue
        seen.add(name)
        spec = ALLOWED_JOBS.get(name)
        if not isinstance(spec, (tuple, list)) or len(spec) < 2:
            missing.append(f"{name}:unknown_job")
            continue
        if str(spec[1] or "").strip().lower() != "daemon":
            continue
        try:
            _resolve_child_script(name)
            out.append(name)
        except Exception as e:
            missing.append(f"{name}:{e}")

    if missing:
        try:
            log.warning("skipping unavailable ingestion child jobs: %s", missing)
        except Exception as e:
            _warn_failure("INGESTION_RUNTIME_LOG_MISSING_CHILDREN_FAILED", e, missing=list(missing))

    return out


def _load_child_control_plane_snapshot() -> Dict[str, object]:
    desired = _compute_child_candidates()
    config_hashes: Dict[str, str] = {}
    manager = None
    for job_name in desired:
        try:
            if manager is None:
                manager = get_manager()
            config_hashes[str(job_name)] = str(manager.config_hash_for_job(job_name) or "")
        except Exception as e:
            config_hashes[str(job_name)] = ""
            _warn_failure("INGESTION_RUNTIME_CHILD_CONFIG_HASH_LOOKUP_FAILED", e, job=str(job_name))
    return {
        "desired": [str(name) for name in desired],
        "config_hashes": config_hashes,
    }


def _child_control_plane_snapshot() -> Dict[str, object]:
    return _dict_or_empty(
        _supervisor_snapshot_get_or_load(
            "child_control_plane",
            _load_child_control_plane_snapshot,
        )
    )


def _child_candidates() -> List[str]:
    snapshot = _child_control_plane_snapshot()
    return [
        str(name)
        for name in _list_or_empty(snapshot.get("desired"))
        if str(name or "").strip()
    ]


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(str(name), "")
    if raw is None or str(raw).strip() == "":
        return bool(default)
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def _safe_no_credential_ingestion_mode() -> bool:
    try:
        from services.data_source_manager import safe_no_credential_market_data_mode

        if safe_no_credential_market_data_mode():
            return bool(
                _env_flag("YFINANCE_ENABLED", True)
                or _env_flag("SIMULATED_MARKET_DATA_ENABLED", True)
            )
    except Exception as e:
        _warn_failure(
            "INGESTION_RUNTIME_SAFE_NO_CREDENTIAL_MODE_CHECK_FAILED",
            e,
            degradation="safe_mode_env_fallback",
        )

    mode = str(os.environ.get("ENGINE_MODE") or "safe").strip().lower()
    execution_mode = str(os.environ.get("EXECUTION_MODE") or "safe").strip().lower()
    if mode != "safe" or execution_mode not in ("safe", "paper", "sim-paper", "sim_paper"):
        return False
    paid_or_credentialed_enabled = any(
        _env_flag(name, default)
        for name, default in (
            ("POLYGON_WS_ENABLED", True),
            ("POLYGON_REST_ENABLED", True),
            ("IBKR_ENABLED", False),
            ("CCXT_ENABLED", False),
            ("TRADIER_ENABLED", False),
        )
    )
    return bool(
        (_env_flag("YFINANCE_ENABLED", True) or _env_flag("SIMULATED_MARKET_DATA_ENABLED", False))
        and not paid_or_credentialed_enabled
    )


def _safe_no_credential_child_candidates(requested: List[str]) -> List[str]:
    if not _safe_no_credential_ingestion_mode():
        return list(requested or [])

    filtered = [
        str(name)
        for name in (requested or [])
        if str(name or "").strip() in _SAFE_NO_CREDENTIAL_CHILD_JOBS
    ]
    if (
        _env_flag("YFINANCE_ENABLED", True)
        or _env_flag("SIMULATED_MARKET_DATA_ENABLED", False)
    ) and "poll_prices" not in filtered:
        filtered.append("poll_prices")
    skipped = [
        str(name)
        for name in (requested or [])
        if str(name or "").strip() and str(name or "").strip() not in _SAFE_NO_CREDENTIAL_CHILD_JOBS
    ]
    if skipped:
        try:
            log.info("safe no-credential ingestion filtered child jobs: %s", sorted(set(skipped)))
        except Exception as e:
            _warn_failure("INGESTION_RUNTIME_SAFE_CHILD_FILTER_LOG_FAILED", e)
    return list(dict.fromkeys(filtered))


def _resolve_child_script(job_name: str) -> Path:
    spec = ALLOWED_JOBS.get(job_name)
    if not isinstance(spec, (tuple, list)) or len(spec) < 1:
        raise RuntimeError(f"unknown_child_job:{job_name}")

    script_rel = str(spec[0] or "").strip()
    if not script_rel:
        raise RuntimeError(f"missing_child_script:{job_name}")

    root = _repo_root()
    script_path = (root / script_rel).resolve()
    if script_path.exists():
        return script_path

    filename = Path(script_rel).name
    alt_path = (root / "engine" / "data" / "provider_sessions" / filename).resolve()
    if alt_path.exists():
        log.warning("AUTO-FIX script path for %s: %s -> %s", job_name, script_rel, alt_path)
        return alt_path

    raise RuntimeError(f"missing_child_script_path:{job_name}:{script_rel}")


def _script_module_name(script_path: Path) -> str:
    try:
        rel = script_path.resolve().relative_to(_repo_root().resolve())
    except Exception as e:
        _warn_failure("INGESTION_RUNTIME_SCRIPT_MODULE_NAME_FAILED", e, script_path=str(script_path))
        return ""

    if rel.suffix.lower() != ".py":
        return ""

    parts = list(rel.with_suffix("").parts)
    if not parts:
        return ""

    if any((not str(part).isidentifier()) for part in parts):
        return ""

    return ".".join(str(part) for part in parts)


def _child_pg_pool_env() -> Dict[str, str]:
    try:
        from engine.runtime.ingestion_tuning import env_bool, pg_pool_default_for_role, tuned_int

        if env_bool("INGESTION_CHILD_INHERIT_TS_PG_POOL_PROFILE", default=False):
            return {}
        pool_size = tuned_int(
            "INGESTION_CHILD_TS_PG_POOL_SIZE",
            pg_pool_default_for_role("jobs"),
            1,
            8,
        )
        pool_min_size = min(
            int(pool_size),
            tuned_int("INGESTION_CHILD_TS_PG_POOL_MIN_SIZE", 1, 1, 8),
        )
        timescale_pool_max = tuned_int("INGESTION_CHILD_TIMESCALE_POOL_MAX_SIZE", 2, 1, 8)
        price_pool_max = tuned_int("INGESTION_CHILD_TIMESCALE_PRICES_POOL_MAX_SIZE", 2, 1, 8)
        async_workers = min(
            int(price_pool_max),
            tuned_int("ASYNC_PRICE_WRITER_WORKERS", 4, 1, 16),
        )
        return {
            "ENGINE_PROCESS_ROLE": "ingestion_child",
            "TS_PROCESS_ROLE": "jobs",
            "TS_PG_POOL_PROFILE": "jobs",
            "TS_PG_POOL_SIZE": str(int(pool_size)),
            "TS_PG_POOL_MIN_SIZE": str(int(pool_min_size)),
            "TIMESCALE_POOL_MIN_SIZE": "1",
            "TIMESCALE_POOL_MAX_SIZE": str(int(timescale_pool_max)),
            "TIMESCALE_PRICES_POOL_MIN_SIZE": "1",
            "TIMESCALE_PRICES_POOL_MAX_SIZE": str(int(price_pool_max)),
            "ASYNC_PRICE_WRITER_WORKERS": str(int(async_workers)),
        }
    except Exception as e:
        _warn_failure("INGESTION_RUNTIME_CHILD_POOL_ENV_FAILED", e)
        return {
            "ENGINE_PROCESS_ROLE": "ingestion_child",
            "TS_PROCESS_ROLE": "jobs",
            "TS_PG_POOL_PROFILE": "jobs",
            "TS_PG_POOL_SIZE": "2",
            "TS_PG_POOL_MIN_SIZE": "1",
            "TIMESCALE_POOL_MIN_SIZE": "1",
            "TIMESCALE_POOL_MAX_SIZE": "2",
            "TIMESCALE_PRICES_POOL_MIN_SIZE": "1",
            "TIMESCALE_PRICES_POOL_MAX_SIZE": "2",
            "ASYNC_PRICE_WRITER_WORKERS": "2",
        }


def _spawn_child_once(job_name: str) -> subprocess.Popen:
    script_path = _resolve_child_script(job_name)
    env = dict(os.environ)
    env["ENGINE_SUPERVISED"] = "1"
    env["ENGINE_LAUNCHED_BY_SUPERVISOR"] = "1"
    env["ENGINE_JOB_NAME"] = str(job_name)
    env["ENGINE_INGESTION_CHILD"] = "1"

    existing_pp = env.get("PYTHONPATH", "")
    repo_pp = str(_repo_root())
    if existing_pp:
        parts = [p for p in existing_pp.split(os.pathsep) if p]
        if repo_pp not in parts:
            parts.insert(0, repo_pp)
        env["PYTHONPATH"] = os.pathsep.join(parts)
    else:
        env["PYTHONPATH"] = repo_pp

    try:
        env.update(get_manager().build_job_environment(job_name))
    except Exception as e:
        _warn_failure("INGESTION_RUNTIME_BUILD_JOB_ENV_FAILED", e, job=str(job_name))

    try:
        if _safe_no_credential_ingestion_mode():
            from services.data_source_manager import apply_safe_no_credential_runtime_environment

            apply_safe_no_credential_runtime_environment(env)
    except Exception as e:
        _warn_failure("INGESTION_RUNTIME_SAFE_ENV_SANITIZE_FAILED", e, job=str(job_name))

    env.update(_child_pg_pool_env())
    env.update(canonical_shard_env(INGESTION_SHARD))
    env["ENGINE_LIVENESS_JOB_NAME"] = _child_liveness_job_name(job_name)
    try:
        from engine.runtime.thread_policy import apply_cpu_thread_policy_to_env

        apply_cpu_thread_policy_to_env(
            env,
            role="ingestion_child",
            supervised_process_count=max(1, len(_child_candidates()) + 1),
        )
    except Exception as e:
        _warn_failure("INGESTION_RUNTIME_CHILD_THREAD_POLICY_FAILED", e, job=str(job_name))

    module_name = _script_module_name(script_path)
    if module_name:
        args = [sys.executable, "-u", "-m", module_name]
    else:
        args = [sys.executable, "-u", str(script_path)]

    stdout_path, stderr_path = _child_log_paths(_child_liveness_job_name(job_name))
    rotate_log_if_needed(stdout_path)
    rotate_log_if_needed(stderr_path)
    stdout_fh = open(stdout_path, "ab")
    stderr_fh = open(stderr_path, "ab")
    log_event(
        log,
        logging.INFO,
        "ingestion_runtime_child_spawn_started",
        component="engine.runtime.ingestion_runtime",
        extra={
            "job": str(job_name),
            "liveness_job": _child_liveness_job_name(job_name),
            "shard": INGESTION_SHARD.as_dict(),
            "args": list(args),
            "stdout_log": str(stdout_path),
            "stderr_log": str(stderr_path),
        },
    )
    try:
        return subprocess.Popen(
            args,
            cwd=str(_repo_root()),
            env=env,
            stdout=stdout_fh,
            stderr=stderr_fh,
            close_fds=(not sys.platform.startswith("win")),
            start_new_session=True,
            creationflags=(subprocess.CREATE_NO_WINDOW if sys.platform.startswith("win") else 0),
        )
    finally:
        try:
            stdout_fh.close()
        except Exception as e:
            _warn_failure("INGESTION_RUNTIME_STDOUT_CLOSE_FAILED", e, job=str(job_name), path=str(stdout_path))
        try:
            stderr_fh.close()
        except Exception as e:
            _warn_failure("INGESTION_RUNTIME_STDERR_CLOSE_FAILED", e, job=str(job_name), path=str(stderr_path))


def _spawn_child(job_name: str) -> subprocess.Popen:
    last_error: BaseException | None = None
    started = time.perf_counter()
    for attempt in range(1, int(SPAWN_RETRY_ATTEMPTS) + 1):
        try:
            proc = _spawn_child_once(job_name)
            latency_ms = int(round((time.perf_counter() - started) * 1000.0))
            emit_timing(
                "ingestion_child_spawn_latency_ms",
                int(latency_ms),
                component="engine.runtime.ingestion_runtime",
                job=str(job_name),
            )
            if attempt > 1:
                log_event(
                    log,
                    logging.INFO,
                    "ingestion_runtime_child_spawn_recovered",
                    component="engine.runtime.ingestion_runtime",
                    extra={
                        "job": str(job_name),
                        "attempt": int(attempt),
                        "latency_ms": int(latency_ms),
                        "pid": int(proc.pid or 0),
                    },
                )
            return proc
        except Exception as exc:
            last_error = exc
            retryable = attempt < int(SPAWN_RETRY_ATTEMPTS)
            log_event(
                log,
                logging.WARNING if retryable else logging.ERROR,
                "ingestion_runtime_child_spawn_failed",
                component="engine.runtime.ingestion_runtime",
                extra={
                    "job": str(job_name),
                    "attempt": int(attempt),
                    "retryable": bool(retryable),
                    "error": f"{type(exc).__name__}:{exc}",
                },
            )
            if retryable:
                emit_counter(
                    "retry_attempt",
                    1,
                    component="engine.runtime.ingestion_runtime",
                    job=str(job_name),
                    extra_tags={
                        "operation": "spawn_child",
                        "attempt": int(attempt),
                    },
                )
                time.sleep(
                    backoff_delay_s(
                        int(attempt),
                        base_s=float(SPAWN_RETRY_BASE_S),
                        max_s=float(SPAWN_RETRY_MAX_S),
                    )
                )
                _warn_failure(
                    "INGESTION_RUNTIME_CHILD_SPAWN_RETRYING",
                    exc,
                    job=str(job_name),
                    attempt=int(attempt),
                )
                continue
            break
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"ingestion_child_spawn_failed:{job_name}")


def _load_latest_price_source_rows() -> Dict[str, object]:
    effective_max_age_ms = _polling_price_max_age_ms()
    cutoff_ms = int(time.time() * 1000 - effective_max_age_ms)
    con = connect_ro()
    row = (0, 0, 0)
    latest_price_source = ""
    provider_rows: List[Dict[str, object]] = []
    try:
        row = con.execute(
            """
            SELECT COUNT(*), COUNT(DISTINCT symbol), MAX(ts_ms)
            FROM prices
            WHERE price IS NOT NULL
              AND ts_ms >= ?
            """,
            (int(cutoff_ms),),
        ).fetchone() or (0, 0, 0)
        try:
            source_row = con.execute(
                """
                SELECT source
                FROM prices
                WHERE price IS NOT NULL
                ORDER BY ts_ms DESC
                LIMIT 1
                """
            ).fetchone()
            latest_price_source = str((source_row or [""])[0] or "")
        except Exception:
            latest_price_source = ""

        try:
            raw_provider_rows = con.execute(
                """
                SELECT p.provider, p.ts_ms, p.ok, p.latency_ms, p.n_symbols, p.error
                FROM price_provider_health p
                INNER JOIN (
                  SELECT provider, MAX(ts_ms) AS max_ts_ms
                  FROM price_provider_health
                  GROUP BY provider
                ) latest
                  ON latest.provider = p.provider
                 AND latest.max_ts_ms = p.ts_ms
                """
            ).fetchall() or []
            provider_rows = [
                {
                    "provider": str(provider or ""),
                    "ts_ms": _safe_int(ts_ms, 0),
                    "ok": _safe_int(ok, 0),
                    "latency_ms": None if latency_ms is None else _safe_int(latency_ms, 0),
                    "n_symbols": _safe_int(n_symbols, 0),
                    "error": None if error is None else str(error),
                }
                for provider, ts_ms, ok, latency_ms, n_symbols, error in raw_provider_rows
            ]
        except Exception:
            provider_rows = []
    except Exception:
        row = (0, 0, 0)
        provider_rows = []
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_failure("INGESTION_RUNTIME_LATEST_PRICES_CLOSE_FAILED", e)

    return {
        "effective_max_age_ms": int(effective_max_age_ms),
        "cutoff_ms": int(cutoff_ms),
        "price_row": (
            _safe_int(row[0], 0),
            _safe_int(row[1], 0),
            _safe_int(row[2], 0),
        ),
        "latest_price_source": latest_price_source,
        "provider_rows": provider_rows,
    }


def _latest_price_source_rows_snapshot() -> Dict[str, object]:
    return _dict_or_empty(
        _supervisor_snapshot_get_or_load(
            "latest_price_source_rows",
            _load_latest_price_source_rows,
        )
    )


def _latest_provider_health_rows() -> List[Dict[str, object]]:
    return [
        _dict_or_empty(row)
        for row in _list_or_empty(
            _latest_price_source_rows_snapshot().get("provider_rows")
        )
    ]


def _latest_prices_state() -> Dict[str, object]:
    snapshot_rows = _latest_price_source_rows_snapshot()
    effective_max_age_ms = _safe_int(
        snapshot_rows.get("effective_max_age_ms"),
        _polling_price_max_age_ms(),
    )
    enabled_price_providers = _enabled_price_providers()
    raw_price_row = snapshot_rows.get("price_row")
    row = tuple(raw_price_row) if isinstance(raw_price_row, (list, tuple)) else (0, 0, 0)

    providers = {}
    healthy = 0
    now_ms = int(time.time() * 1000)
    for provider_row in _latest_provider_health_rows():
        provider_name = str(provider_row.get("provider") or "").strip()
        if not provider_name:
            continue
        if enabled_price_providers and provider_name not in enabled_price_providers:
            continue
        provider_ts_ms = _safe_int(provider_row.get("ts_ms"), 0)
        provider_age_ms = max(0, now_ms - provider_ts_ms) if provider_ts_ms > 0 else 10**12
        provider_health_ok = bool(_safe_int(provider_row.get("ok"), 0) == 1)
        provider_ok = bool(provider_health_ok and provider_age_ms < effective_max_age_ms)
        session_last_failure = {}
        session_fatal = {}
        try:
            session_last_failure = _safe_json_dict(meta_get(f"provider_session_{provider_name}_last_failure", ""))
        except Exception:
            session_last_failure = {}
        try:
            session_fatal = _safe_json_dict(meta_get(f"provider_session_{provider_name}_fatal", ""))
        except Exception:
            session_fatal = {}
        providers[provider_name] = {
            "last_ts_ms": provider_ts_ms,
            "age_ms": int(provider_age_ms),
            "latency_ms": provider_row.get("latency_ms"),
            "n_symbols": _safe_int(provider_row.get("n_symbols"), 0),
            "ok": provider_ok,
            "health_ok": provider_health_ok,
            "error": provider_row.get("error"),
            "session_last_failure": session_last_failure,
            "session_fatal": session_fatal,
            "failure_kind": str((session_last_failure or {}).get("failure_kind") or ""),
        }
        if provider_ok:
            healthy += 1

    last_price_ts_ms = _safe_int(row[2] if len(row) > 2 else 0, 0)
    age_ms = max(0, now_ms - last_price_ts_ms) if last_price_ts_ms > 0 else 10**12

    fresh_rows = _safe_int(row[0] if len(row) > 0 else 0, 0)
    fresh_symbols = _safe_int(row[1] if len(row) > 1 else 0, 0)
    latest_price_source = str(snapshot_rows.get("latest_price_source") or "")

    if healthy <= 0 and fresh_rows > 0 and last_price_ts_ms > 0 and age_ms < effective_max_age_ms:
        providers["derived_from_prices"] = {
            "last_ts_ms": int(last_price_ts_ms),
            "ok": True,
            "simulated": latest_price_source.lower() == "simulated",
            "source": latest_price_source,
        }
        healthy = 1
    truth = annotate_provider_map_liveness(
        providers,
        missing_credential_env_vars=[],
    )
    providers = truth["providers"]
    live_healthy = int(truth["live_healthy_providers"])

    provider_errors = {
        str(name): str(info.get("error") or "")
        for name, info in (providers or {}).items()
        if isinstance(info, dict) and str(info.get("error") or "").strip()
    }

    return {
        "fresh_rows": int(fresh_rows),
        "fresh_symbols": int(fresh_symbols),
        "last_price_ts_ms": int(last_price_ts_ms),
        "price_age_ms": int(age_ms),
        "providers": providers,
        "provider_errors": provider_errors,
        "raw_healthy_providers": int(healthy),
        "healthy_providers": int(live_healthy),
        "live_healthy_providers": int(live_healthy),
        "simulated_healthy_providers": int(truth["simulated_healthy_providers"]),
        "missing_credential_env_vars": list(truth["missing_credential_env_vars"]),
        "live_market_data_ok": bool(truth["live_market_data_ok"] and fresh_rows > 0),
        "live_feed_status": str(truth["live_feed_status"] or "degraded"),
        "updated_ts_ms": int(now_ms),
    }

def _check_provider_health(now_ts_ms: int) -> None:
    global _PROVIDER_ALERT_STATE

    cutoff_ms = int(now_ts_ms - _polling_price_max_age_ms())
    enabled_price_providers = _enabled_price_providers()

    healthy = False

    for row in _latest_provider_health_rows():
        provider = row.get("provider")
        ts_ms = row.get("ts_ms")
        ok = row.get("ok")
        if ts_ms is None:
            continue

        provider_s = str(provider)
        if enabled_price_providers and provider_s not in enabled_price_providers:
            continue
        ts_ms = int(ts_ms or 0)

        if int(ok or 0) == 1 and ts_ms >= cutoff_ms:
            healthy = True
            _PROVIDER_ALERT_STATE[provider_s] = False

        if ts_ms < cutoff_ms and not _PROVIDER_ALERT_STATE.get(provider_s):
            _PROVIDER_ALERT_STATE[provider_s] = True
            emit_alert(
                event_title=f"Provider stale: {provider_s}",
                symbol="",
                horizon_s=0,
                expected_z=0.0,
                confidence=1.0,
                explain={
                    "provider": provider_s,
                    "last_update_ts_ms": ts_ms,
                    "price_max_age_s": round(_polling_price_max_age_ms() / 1000.0, 1),
                },
            )

        elif ok == 0 and not _PROVIDER_ALERT_STATE.get(f"{provider_s}::fail"):
            _PROVIDER_ALERT_STATE[f"{provider_s}::fail"] = True
            emit_alert(
                event_title=f"Provider failing: {provider_s}",
                symbol="",
                horizon_s=0,
                expected_z=0.0,
                confidence=1.0,
                explain={
                    "provider": provider_s,
                    "last_update_ts_ms": ts_ms,
                    "type": "provider_fail",
                },
            )

        elif ok == 1:
            _PROVIDER_ALERT_STATE[f"{provider_s}::fail"] = False

    if not healthy:
        _INGESTION_STATE["stale"] = True
        emit_alert(
            event_title="All price feeds unhealthy",
            symbol="",
            horizon_s=0,
            expected_z=0.0,
            confidence=1.0,
            explain={
                "type": "feed_down",
                "action": "ingestion_runtime_expected_to_recover",
            },
        )

def _publish_market_state(children: Dict[str, Dict[str, object]]) -> None:
    snapshot = _latest_prices_state()
    state = {
        "owner_job": JOB_NAME,
        "children": children,
        "running": any(bool(info.get("running")) for info in children.values()),
        **snapshot,
    }
    publish_channel_state("market_data", state, owner=JOB_NAME, best_effort=True)

    # UPDATE GLOBAL STATE (for snapshot)
    try:
        _INGESTION_STATE["last_tick_ts_ms"] = int(state.get("last_price_ts_ms") or 0)
        _INGESTION_STATE["last_publish_ts_ms"] = int(time.time() * 1000)
        _INGESTION_STATE["healthy_providers"] = int(state.get("healthy_providers") or 0)
        _INGESTION_STATE["running"] = bool(state.get("running"))
        age_ms = int(state.get("price_age_ms") or 10**12)
        _INGESTION_STATE["stale"] = bool(age_ms > _polling_price_max_age_ms())
    except Exception as e:
        _warn_failure("INGESTION_RUNTIME_GLOBAL_STATE_UPDATE_FAILED", e)


def _new_child_info(job_name: str) -> Dict[str, object]:
    return {
        "job": job_name,
        "proc": None,
        "pid": 0,
        "running": False,
        "restart_delay_s": RESTART_BASE_S,
        "next_spawn_ts": 0.0,
        "last_start_ts": 0.0,
        "last_exit_rc": None,
        "last_error": None,
        "restart_disabled": False,
        "restart_guard": _restart_guard_snapshot(job_name),
        "config_hash": "",
    }


def _reconcile_child_control_plane(children: Dict[str, Dict[str, object]], now: float) -> List[str]:
    _invalidate_supervisor_snapshots_if_data_sources_changed()
    control_plane = _child_control_plane_snapshot()
    desired = [
        str(name)
        for name in _list_or_empty(control_plane.get("desired"))
        if str(name or "").strip()
    ]
    desired_set = set(desired)
    config_hashes = _dict_or_empty(control_plane.get("config_hashes"))
    config_reload_ts_ms = int(_LAST_DATA_SOURCES_RELOAD_TS_MS or 0)

    for job_name in desired:
        if job_name not in children:
            children[job_name] = _new_child_info(job_name)
        next_hash = str(config_hashes.get(str(job_name)) or "")
        current_hash = str(children[job_name].get("config_hash") or "")
        hash_changed = bool(next_hash and next_hash != current_hash)
        config_changed = bool(hash_changed and current_hash)
        persisted_attempt_ts_ms = (
            _restart_guard_latest_attempt_ts_ms(job_name)
            if config_reload_ts_ms > 0 and not config_changed
            else 0
        )
        persisted_config_changed = bool(
            config_reload_ts_ms > 0
            and persisted_attempt_ts_ms > 0
            and config_reload_ts_ms > persisted_attempt_ts_ms
        )
        if hash_changed:
            children[job_name]["config_hash"] = next_hash
        if config_changed or persisted_config_changed:
            if bool(children[job_name].get("running")):
                _terminate_child(job_name, _child_proc(children[job_name]))
                children[job_name]["proc"] = None
                children[job_name]["running"] = False
                children[job_name]["pid"] = 0
            children[job_name]["restart_disabled"] = False
            _clear_child_restart_accounting(
                [job_name],
                reason="config_changed" if config_changed else "config_reload_marker",
            )
            children[job_name]["restart_guard"] = _restart_guard_snapshot(job_name, now=now)
            children[job_name]["next_spawn_ts"] = float(now)
            children[job_name]["last_error"] = "config_changed_restart_requested"

    for job_name in [name for name in list(children.keys()) if name not in desired_set]:
        _terminate_child(job_name, _child_proc(children[job_name]))
        children.pop(job_name, None)

    return desired


def _terminate_child(child_job: Optional[str], child_proc: Optional[subprocess.Popen]) -> None:
    if child_proc is None:
        return
    try:
        if child_proc.poll() is None:
            log.info("stopping child job=%s pid=%s", child_job, child_proc.pid)
            try:
                killpg = getattr(os, "killpg", None)
                if callable(killpg):
                    killpg(int(child_proc.pid), signal.SIGTERM)
                else:
                    child_proc.terminate()
            except Exception as e:
                _warn_failure("INGESTION_RUNTIME_CHILD_TERMINATE_SIGNAL_FAILED", e, job=str(child_job), pid=int(child_proc.pid or 0))
                child_proc.terminate()
            try:
                child_proc.wait(timeout=10)
            except Exception as e:
                _warn_failure("INGESTION_RUNTIME_CHILD_TERMINATE_WAIT_FAILED", e, job=str(child_job), pid=int(child_proc.pid or 0))
                try:
                    killpg = getattr(os, "killpg", None)
                    sigkill = getattr(signal, "SIGKILL", signal.SIGTERM)
                    if callable(killpg):
                        killpg(int(child_proc.pid), sigkill)
                    else:
                        child_proc.kill()
                except Exception as kill_err:
                    _warn_failure("INGESTION_RUNTIME_CHILD_KILL_SIGNAL_FAILED", kill_err, job=str(child_job), pid=int(child_proc.pid or 0))
                    child_proc.kill()
                try:
                    child_proc.wait(timeout=5)
                except Exception as wait_err:
                    _warn_failure("INGESTION_RUNTIME_CHILD_KILL_WAIT_FAILED", wait_err, job=str(child_job), pid=int(child_proc.pid or 0))
    except Exception as e:
        _warn_failure("INGESTION_RUNTIME_TERMINATE_CHILD_FAILED", e, job=str(child_job))


def _restart_guard_disabled_child_still_suppressed(
    job_name: str,
    info: Dict[str, object],
    now: float,
) -> bool:
    if not bool(info.get("restart_disabled")):
        return False
    if not str(info.get("last_error") or "").startswith("restart_guard_triggered"):
        return True

    restart_guard = _restart_guard_snapshot(str(job_name), now=now)
    info["restart_guard"] = restart_guard
    if bool(restart_guard.get("suppressed")):
        return True

    info["restart_disabled"] = False
    info["last_error"] = None
    info["next_spawn_ts"] = float(now)
    return False


def _suppress_inactive_child_if_restart_guard_active(
    job_name: str,
    info: Dict[str, object],
    now: float,
) -> bool:
    if bool(info.get("running")):
        return False

    restart_guard = _restart_guard_snapshot(str(job_name), now=now)
    info["restart_guard"] = restart_guard
    if not bool(restart_guard.get("suppressed")):
        return False

    info["restart_disabled"] = True
    info["last_error"] = (
        "restart_guard_triggered "
        f"suppressed_until_ts_ms={int(restart_guard.get('suppressed_until_ts_ms') or 0)}"
    )
    _publish_child_restart_suppressed(
        str(job_name),
        restart_guard,
        reason="restart_window_active",
        detail={"action": "spawn_suppressed"},
    )
    return True


def _child_telemetry_from_heartbeat_extra(hb_extra: Dict[str, object]) -> Dict[str, object]:
    telemetry = _dict_or_empty(hb_extra.get("telemetry"))
    if telemetry:
        return telemetry

    providers = _dict_or_empty(hb_extra.get("providers"))
    provider_rows = [_dict_or_empty(row) for row in providers.values() if isinstance(row, dict)]
    if not provider_rows:
        return {}

    connected_rows = [row for row in provider_rows if bool(row.get("connected"))]
    age_candidates = []
    for row in connected_rows or provider_rows:
        try:
            age_candidates.append(int(row.get("last_msg_age_ms") or 10**12))
        except Exception:
            age_candidates.append(10**12)

    capabilities = {
        "streaming": any(bool((row.get("capabilities") or {}).get("streaming")) for row in provider_rows),
        "polling": any(bool((row.get("capabilities") or {}).get("polling")) for row in provider_rows),
    }

    manager_states = [
        str(row.get("manager_state") or "").strip().lower()
        for row in provider_rows
        if str(row.get("manager_state") or "").strip()
    ]

    manager_state = ""
    if any(state in {"failed", "error", "closed", "disconnected"} for state in manager_states):
        manager_state = next(
            state for state in manager_states if state in {"failed", "error", "closed", "disconnected"}
        )
    elif any(state == "healthy" for state in manager_states):
        manager_state = "healthy"
    elif manager_states:
        manager_state = manager_states[0]

    return {
        "connected": any(bool(row.get("connected")) for row in provider_rows),
        "last_msg_age_ms": min(age_candidates) if age_candidates else 10**12,
        "manager_state": manager_state,
        "capabilities": capabilities,
    }


def _child_heartbeat_stale_ms(hb_extra: Dict[str, object]) -> int:
    heartbeat_every_s = max(0.0, _safe_float(hb_extra.get("heartbeat_every_s"), 0.0))
    poll_seconds = max(0.0, _safe_float(hb_extra.get("poll_seconds"), 0.0))
    providers = _dict_or_empty(hb_extra.get("providers"))

    provider_dead_after_ms = 0
    for row in providers.values():
        if not isinstance(row, dict):
            continue
        provider_dead_after_ms = max(
            provider_dead_after_ms,
            _safe_int(row.get("manager_dead_after_ms"), 0),
        )

    stale_ms = 15000
    if heartbeat_every_s > 0:
        stale_ms = max(stale_ms, int(heartbeat_every_s * 3000.0))
    if poll_seconds > 0:
        stale_ms = max(stale_ms, int(max(poll_seconds * 1.5, 30.0) * 1000.0))
    if provider_dead_after_ms > 0:
        stale_ms = max(stale_ms, provider_dead_after_ms)
    return int(stale_ms)


def _restart_children_for_feed_stall(children: Dict[str, Dict[str, object]], latest_state: Dict[str, object], now: float) -> None:
    provider_errors = latest_state.get("provider_errors") if isinstance(latest_state, dict) else {}
    if not isinstance(provider_errors, dict):
        provider_errors = {}

    child_heartbeat_rows = _child_heartbeat_rows_snapshot()

    now_ts_ms = int(now * 1000)

    global_restart_reason = None
    global_price_max_age_ms = _polling_price_max_age_ms()
    # Warmup legitimately has no canonical prices yet; restarting the whole
    # ingestion fleet before the first price commit creates a self-sustaining
    # startup loop that starves poll_prices of a clean write window.
    first_price_seen = bool(_has_first_price_tick())
    if first_price_seen and _safe_int(latest_state.get("healthy_providers"), 0) <= 0:
        price_age_ms = _safe_int(latest_state.get("price_age_ms"), 10**12)
        if price_age_ms >= global_price_max_age_ms:
            global_restart_reason = f"feed_stalled price_age_ms={price_age_ms}"

    for job_name, info in children.items():
        if not bool(info.get("running")):
            continue
        if bool(info.get("restart_disabled")):
            continue

        proc = info.get("proc")
        if not isinstance(proc, subprocess.Popen):
            continue

        hb = child_heartbeat_rows.get(_child_liveness_job_name(str(job_name))) or {}
        hb_ts_ms = _safe_int(hb.get("ts_ms"), 0)
        hb_age_ms = max(0, now_ts_ms - hb_ts_ms) if hb_ts_ms > 0 else 10**12
        hb_extra = _dict_or_empty(hb.get("extra"))
        child_heartbeat_stale_ms = _child_heartbeat_stale_ms(hb_extra)
        telemetry = _child_telemetry_from_heartbeat_extra(hb_extra)
        manager_state = str(telemetry.get("manager_state") or "").strip().lower()
        last_msg_age_ms = _safe_int(telemetry.get("last_msg_age_ms"), 10**12)
        connected = bool(telemetry.get("connected"))
        capabilities = _dict_or_empty(telemetry.get("capabilities"))
        is_streaming = bool(capabilities.get("streaming"))
        is_polling_only = bool(capabilities.get("polling")) and not is_streaming
        poll_seconds = max(0.0, _safe_float(hb_extra.get("poll_seconds"), 0.0))
        effective_max_age_ms = int(PRICE_MAX_AGE_S * 1000.0)
        if is_polling_only and poll_seconds > 0:
            effective_max_age_ms = max(
                effective_max_age_ms,
                int(max(poll_seconds * 2.5, 45.0) * 1000.0),
            )
        child_age_s = max(0.0, now - _safe_float(info.get("last_start_ts"), 0.0))

        fatal_failure_kinds = {
            str((info or {}).get("failure_kind") or "").strip().lower()
            for info in _dict_or_empty(latest_state.get("providers")).values()
            if isinstance(info, dict)
        }
        fatal_provider_failure = any(kind in {"auth", "config"} for kind in fatal_failure_kinds)

        restart_reason = None
        if child_age_s < float(CHILD_STARTUP_GRACE_S):
            restart_reason = None
        elif hb_ts_ms <= 0:
            restart_reason = "child_missing_heartbeat"
        elif hb_age_ms >= child_heartbeat_stale_ms:
            restart_reason = f"child_stale_heartbeat age_ms={hb_age_ms}"
        elif fatal_provider_failure:
            restart_reason = None
        elif manager_state in {"failed", "error", "closed", "disconnected"}:
            restart_reason = f"child_manager_state={manager_state}"
        elif connected and not is_polling_only and last_msg_age_ms >= effective_max_age_ms:
            restart_reason = f"child_stale_data_flow age_ms={last_msg_age_ms}"
        elif global_restart_reason is not None and not (
            is_polling_only and _safe_int(latest_state.get("price_age_ms"), 10**12) < effective_max_age_ms
        ):
            restart_reason = global_restart_reason

        if restart_reason is None:
            continue

        err = str(provider_errors)[:400] if provider_errors else restart_reason
        if child_age_s >= 5.0:
            _clear_child_restart_accounting([str(job_name)], reason="stable_child_feed_stall")
        restart_guard = _record_child_restart_attempt(
            str(job_name),
            now=now,
            reason="feed_stall",
        )
        detail = {
            "restart_reason": restart_reason,
            "provider_errors": provider_errors,
            "heartbeat_age_ms": int(hb_age_ms),
            "manager_state": manager_state,
            "last_msg_age_ms": int(last_msg_age_ms),
            "child_age_s": float(child_age_s),
        }
        info["last_error"] = err
        info["running"] = False
        info["proc"] = None
        info["pid"] = 0
        info["last_exit_rc"] = None
        info["restart_guard"] = restart_guard
        if bool(restart_guard.get("suppressed")):
            info["restart_disabled"] = True
            info["last_error"] = f"restart_guard_triggered {restart_reason}"
            _publish_child_restart_suppressed(
                str(job_name),
                restart_guard,
                reason="feed_stall",
                detail=detail,
            )
        else:
            info["next_spawn_ts"] = now + _safe_float(info.get("restart_delay_s"), RESTART_BASE_S)
            info["restart_delay_s"] = min(
                RESTART_MAX_S,
                max(RESTART_BASE_S, _safe_float(info.get("restart_delay_s"), RESTART_BASE_S) * 2.0),
            )
            publish_message(
                "market_data",
                "child_restart_for_feed_stall",
                {
                    "job": str(job_name),
                    "reason": restart_reason,
                    **detail,
                },
                sender=JOB_NAME,
                best_effort=True,
            )
        _terminate_child(job_name, proc)


def _should_disable_restart_for_exit(job_name: str, latest_state: Dict[str, object]) -> bool:
    job = str(job_name or "").strip().lower()
    try:
        providers = _dict_or_empty(latest_state.get("providers")) if isinstance(latest_state, dict) else {}

        if job == "stream_prices_polygon_ws":
            provider_info = providers.get("polygon_ws") if isinstance(providers.get("polygon_ws"), dict) else {}
            failure_kind = str((provider_info or {}).get("failure_kind") or "").strip().lower()
            return failure_kind in {"auth", "config"}
    except Exception as e:
        _warn_failure("INGESTION_RUNTIME_PROVIDER_AUTH_FAILURE_CHECK_FAILED", e, job=str(job))
        return False

    return False


def _source_health_snapshot(latest_market_state: Optional[Dict[str, object]] = None) -> Dict[str, object]:
    try:
        from engine.runtime.health import (
            _build_ingestion_freshness_snapshot,
            _options_ingestion_snapshot,
        )
        from engine.runtime.ingestion_status import get_all_pipeline_statuses

        now_ms = int(time.time() * 1000)
        market_state = _dict_or_empty(latest_market_state or _latest_prices_state() or {})
        price_age_ms = _safe_int(market_state.get("price_age_ms"), 10**12)
        max_price_age_ms = int(_polling_price_max_age_ms())
        prices_snapshot = {
            "ok": bool(
                _safe_int(market_state.get("healthy_providers"), 0) > 0
                and price_age_ms <= max_price_age_ms
            ),
            "last_ts_ms": _safe_int(market_state.get("last_price_ts_ms"), 0),
        }
        ingestion_runtime_snapshot = {
            "last_publish_ts_ms": int(_INGESTION_STATE.get("last_publish_ts_ms") or 0),
            "last_tick_ts_ms": int(_INGESTION_STATE.get("last_tick_ts_ms") or 0),
            "healthy_providers": int(_INGESTION_STATE.get("healthy_providers") or 0),
            "running": bool(_INGESTION_STATE.get("running")),
            "stale": bool(_INGESTION_STATE.get("stale")),
        }
        freshness = _build_ingestion_freshness_snapshot(
            now_ms=int(now_ms),
            prices_snapshot=prices_snapshot,
            options_snapshot=_options_ingestion_snapshot(now_ms),
            ingestion_runtime_snapshot=ingestion_runtime_snapshot,
            pipeline_statuses=get_all_pipeline_statuses(),
        )
        sources = {}
        for source_name, row in _dict_or_empty(freshness.get("sources")).items():
            if not isinstance(row, dict):
                continue
            sources[str(source_name)] = {
                "critical": bool(row.get("critical")),
                "status": str(row.get("status") or ""),
                "stale": bool(row.get("stale")),
                "last_update_ts_ms": row.get("last_update_ts_ms"),
                "latest_update_ts_ms": row.get("latest_update_ts_ms"),
                "freshness_lag_s": row.get("freshness_lag_s"),
                "pipeline_names": _list_or_empty(row.get("pipeline_names")),
            }
        return {
            "updated_ts_ms": int(now_ms),
            "degraded": bool(freshness.get("degraded")),
            "critical_ok": bool(freshness.get("critical_ok")),
            "stale_sources": _list_or_empty(freshness.get("stale_sources")),
            "stale_critical_sources": _list_or_empty(freshness.get("stale_critical_sources")),
            "runtime_reason_codes": _list_or_empty(freshness.get("runtime_reason_codes")),
            "advisory_reason_codes": _list_or_empty(freshness.get("advisory_reason_codes")),
            "sources": sources,
        }
    except Exception as e:
        _warn_failure("INGESTION_RUNTIME_SOURCE_HEALTH_SNAPSHOT_FAILED", e)
        return {}


def _write_ingestion_state(children: Optional[Dict[str, Dict[str, object]]] = None, *, provider_status: str = "", last_error: str = "") -> None:
    try:
        now_ms = int(time.time() * 1000)
        market_state = _dict_or_empty(_latest_prices_state() or {})
        source_health = _source_health_snapshot(market_state)
        writer_diagnostics = _ingestion_writer_diagnostics()
        payload = {
            "running": bool(_INGESTION_STATE.get("running")),
            "pid": int(PID),
            "job": str(INGESTION_RUNTIME_JOB_LOCK_NAME),
            "base_job": str(JOB_NAME),
            "shard": INGESTION_SHARD.as_dict(),
            "provider_status": str(provider_status or market_state.get("status") or "unknown"),
            "last_event_ts_ms": _safe_int(market_state.get("last_ts_ms"), now_ms),
            "lag_ms": max(0, now_ms - _safe_int(market_state.get("last_ts_ms"), now_ms)),
            "market_state": market_state,
            "source_health": source_health,
            "writer_diagnostics": writer_diagnostics,
            "children": {
                name: {
                    "pid": _safe_int(info.get("pid"), 0),
                    "liveness_job": _child_liveness_job_name(str(name)),
                    "running": bool(info.get("running")),
                    "last_exit_rc": info.get("last_exit_rc"),
                    "last_error": info.get("last_error"),
                    "restart_disabled": bool(info.get("restart_disabled")),
                    "restart_guard": dict(
                        info.get("restart_guard")
                        if isinstance(info.get("restart_guard"), dict)
                        else _restart_guard_snapshot(str(name))
                    ),
                }
                for name, info in (children or {}).items()
            },
            "last_error": str(last_error or ""),
            "ts_ms": now_ms,
        }
        from engine.runtime.runtime_meta import meta_set
        meta_set(
            INGESTION_STATE_KEY,
            json.dumps(payload, separators=(",", ":"), sort_keys=True),
            best_effort=True,
        )
        critical_ok = bool(source_health.get("critical_ok"))
        writer_ok = bool(writer_diagnostics.get("ok", True))
        running = bool(_INGESTION_STATE.get("running"))
        healthy_providers = _safe_int(market_state.get("healthy_providers"), 0)
        status_name = str(provider_status or market_state.get("status") or ("running" if running else "stopped"))
        detail = (
            str(last_error or "")
            or ("critical_sources_stale" if not critical_ok else "")
            or ("ingestion_writer_backpressure" if not writer_ok else "")
            or ("no_healthy_providers" if running and healthy_providers <= 0 else "")
            or "ok"
        )
        emit_gauge(
            "ingestion_source_critical_ok",
            1.0 if critical_ok else 0.0,
            component="engine.runtime.ingestion_runtime",
            job=JOB_NAME,
        )
        record_component_health(
            "ingestion",
            ok=bool(running and critical_ok and writer_ok and healthy_providers > 0 and not str(last_error or "").strip()),
            status=str(status_name),
            detail=str(detail),
            observed_ts_ms=int(now_ms),
            extra={
                "running": bool(running),
                "healthy_providers": int(healthy_providers),
                "stale_critical_sources": _list_or_empty(source_health.get("stale_critical_sources")),
                "stale_sources": _list_or_empty(source_health.get("stale_sources")),
                "writer_degraded_reasons": _list_or_empty(writer_diagnostics.get("degraded_reasons")),
                "last_publish_ts_ms": int(_INGESTION_STATE.get("last_publish_ts_ms") or 0),
                "children_running": int(
                    sum(
                        1
                        for info in (children or {}).values()
                        if isinstance(info, dict) and bool(info.get("running"))
                    )
                ),
            },
        )
    except Exception as e:
        _warn_failure("INGESTION_RUNTIME_WRITE_STATE_FAILED", e, provider_status=str(provider_status), last_error=str(last_error or ""))

def get_ingestion_runtime_state() -> Dict[str, object]:
    try:
        return dict(_INGESTION_STATE)
    except Exception as e:
        _warn_failure("INGESTION_RUNTIME_GET_STATE_FAILED", e)
        return {}

def main() -> None:
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("ingestion_runtime must be launched by supervisor")
        raise SystemExit(1)

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    log_event(
        log,
        logging.INFO,
        "ingestion_runtime_boot_start",
        component="engine.runtime.ingestion_runtime",
        extra={
            "pid": int(PID),
            "owner": str(OWNER),
            "job": str(INGESTION_RUNTIME_JOB_LOCK_NAME),
            "shard": INGESTION_SHARD.as_dict(),
        },
    )

    init_db()
    try:
        get_manager().initialize()
        get_manager().apply_runtime_environment()
    except Exception as e:
        log.warning("data source manager init failed: %s", e)

    if not acquire_job_lock(INGESTION_RUNTIME_JOB_LOCK_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        log_event(
            log,
            logging.ERROR,
            "ingestion_runtime_lock_unavailable",
            component="engine.runtime.ingestion_runtime",
            extra={
                "job": str(INGESTION_RUNTIME_JOB_LOCK_NAME),
                "base_job": str(JOB_NAME),
                "owner": str(OWNER),
                "pid": int(PID),
                "shard": INGESTION_SHARD.as_dict(),
            },
        )
        raise SystemExit(2)

    child_candidates = _child_candidates()
    _terminate_stale_child_processes(child_candidates)
    children: Dict[str, Dict[str, object]] = {name: _new_child_info(name) for name in child_candidates}

    last_state = 0.0
    last_control_refresh = 0.0
    startup_started_at = time.time()

    try:
        _INGESTION_STATE["running"] = True
        _write_ingestion_state(children, provider_status="starting")
        publish_message(
            "market_data",
            "ingestion_runtime_started",
            {
                "pid": int(PID),
                "candidates": child_candidates,
                "job": str(INGESTION_RUNTIME_JOB_LOCK_NAME),
                "shard": INGESTION_SHARD.as_dict(),
            },
            sender=JOB_NAME,
            best_effort=True,
        )
        try:
            _publish_market_state({name: {k: v for k, v in info.items() if k != "proc"} for name, info in children.items()})
        except Exception as e:
            _warn_failure("INGESTION_RUNTIME_INITIAL_MARKET_STATE_PUBLISH_FAILED", e)
        heartbeat_thread = threading.Thread(
            target=_heartbeat_loop,
            args=(children,),
            name="ingestion_heartbeat",
            daemon=True,
        )
        heartbeat_thread.start()

        while not _STOP:
            now = time.time()
            if (now - last_control_refresh) >= max(1.0, float(CONTROL_PLANE_REFRESH_S)):
                try:
                    child_candidates = _reconcile_child_control_plane(children, now)
                except Exception as e:
                    log.warning("control plane reconcile failed: %s", e)
                last_control_refresh = now

            for job_name, info in list(children.items()):
                proc = info.get("proc")
                if proc is None and bool(info.get("running")):
                    existing = _existing_child_process_state(job_name)
                    if bool(existing.get("active")):
                        info["pid"] = _safe_int(existing.get("pid"), 0)
                        continue
                    info["running"] = False
                    info["pid"] = 0
                    info["next_spawn_ts"] = now + _safe_float(info.get("restart_delay_s"), RESTART_BASE_S)

                if isinstance(proc, subprocess.Popen):
                    rc = proc.poll()
                    if rc is not None:
                        child_age_s = max(0.0, now - _safe_float(info.get("last_start_ts"), 0.0))
                        if child_age_s >= 5.0:
                            _clear_child_restart_accounting([job_name], reason="stable_child_exit")
                        restart_guard = _record_child_restart_attempt(
                            job_name,
                            now=now,
                            reason="exit",
                        )
                        info["restart_guard"] = restart_guard

                        with _CHILDREN_LOCK:
                            info["proc"] = None
                            info["pid"] = 0
                            info["running"] = False
                            info["last_exit_rc"] = int(rc)
                        log_event(
                            log,
                            logging.WARNING,
                            "ingestion_runtime_child_exited",
                            component="engine.runtime.ingestion_runtime",
                            extra={
                                "job": str(job_name),
                                "rc": int(rc),
                                "age_s": float(child_age_s),
                            },
                        )
                        _write_ingestion_state(children, provider_status="child_exited", last_error=f"{job_name}:rc={int(rc)}")

                        latest_state = _latest_prices_state()
                        if _should_disable_restart_for_exit(job_name, latest_state):
                            with _CHILDREN_LOCK:
                                info["restart_disabled"] = True
                                info["last_error"] = "restart_disabled_due_to_fatal_provider_failure"
                            publish_message(
                                "market_data",
                                "child_restart_disabled",
                                {
                                    "job": str(job_name),
                                    "rc": int(rc),
                                    "reason": "fatal_provider_failure",
                                    "providers": latest_state.get("providers") if isinstance(latest_state, dict) else {},
                                },
                                sender=JOB_NAME,
                                best_effort=True,
                            )
                            continue

                        if bool(restart_guard.get("suppressed")):
                            with _CHILDREN_LOCK:
                                info["restart_disabled"] = True
                                info["last_error"] = f"restart_guard_triggered rc={int(rc)} age_s={child_age_s:.3f}"
                            _publish_child_restart_suppressed(
                                job_name,
                                restart_guard,
                                reason="exit",
                                detail={"rc": int(rc), "age_s": float(child_age_s)},
                            )
                        else:
                            with _CHILDREN_LOCK:
                                info["next_spawn_ts"] = now + _safe_float(info.get("restart_delay_s"), RESTART_BASE_S)
                                info["restart_delay_s"] = min(RESTART_MAX_S, max(RESTART_BASE_S, _safe_float(info.get("restart_delay_s"), RESTART_BASE_S) * 2.0))
                            publish_message(
                                "market_data",
                                "child_exit",
                                {"job": str(job_name), "rc": int(rc), "age_s": float(child_age_s)},
                                sender=JOB_NAME,
                                best_effort=True,
                            )

                if _restart_guard_disabled_child_still_suppressed(job_name, info, now):
                    continue

                if not bool(info.get("running")) and now >= _safe_float(info.get("next_spawn_ts"), 0.0):
                    if _suppress_inactive_child_if_restart_guard_active(job_name, info, now):
                        continue
                    if _should_defer_child_start(job_name, startup_ts=float(startup_started_at), now_ts=float(now)):
                        continue
                    existing = _existing_child_process_state(job_name)
                    if bool(existing.get("active")):
                        with _CHILDREN_LOCK:
                            info["proc"] = None
                            info["pid"] = _safe_int(existing.get("pid"), 0)
                            info["running"] = True
                            info["last_start_ts"] = now
                            info["last_error"] = None
                        continue
                    try:
                        _terminate_stale_child_processes([job_name])
                        proc = _spawn_child(job_name)
                        child_pid = int(proc.pid or 0)
                        if child_pid <= 0:
                            raise RuntimeError(f"child_pid_invalid:{job_name}:{child_pid}")

                        with _CHILDREN_LOCK:
                            info["proc"] = proc
                            info["pid"] = child_pid
                            info["running"] = True
                            info["last_start_ts"] = now
                            info["restart_delay_s"] = RESTART_BASE_S
                            info["last_error"] = None
                        log_event(
                            log,
                            logging.INFO,
                            "ingestion_runtime_child_started",
                            component="engine.runtime.ingestion_runtime",
                            extra={"job": str(job_name), "pid": int(child_pid)},
                        )
                        _write_ingestion_state(children, provider_status="child_started")
                        publish_message(
                            "market_data",
                            "child_start",
                            {"job": str(job_name), "pid": child_pid},
                            sender=JOB_NAME,
                            best_effort=True,
                        )
                    except Exception as e:
                        restart_guard = _record_child_restart_attempt(
                            job_name,
                            now=now,
                            reason="start_failed",
                        )
                        with _CHILDREN_LOCK:
                            info["restart_guard"] = restart_guard

                        err = f"{type(e).__name__}: {e}"
                        with _CHILDREN_LOCK:
                            info["proc"] = None
                            info["pid"] = 0
                            info["running"] = False
                            info["last_error"] = err
                        log_event(
                            log,
                            logging.ERROR,
                            "ingestion_runtime_child_start_failed",
                            component="engine.runtime.ingestion_runtime",
                            extra={
                                "job": str(job_name),
                                "error": str(err),
                                "restart_count": int(restart_guard.get("count") or 0),
                            },
                        )
                        _write_ingestion_state(children, provider_status="child_start_failed", last_error=f"{job_name}:{err}")
                        try:
                            publish_message(
                                "market_data",
                                "child_error",
                                {
                                    "job": str(job_name),
                                    "error": err,
                                },
                                sender=JOB_NAME,
                                best_effort=True,
                            )
                        except Exception as publish_err:
                            _warn_failure("INGESTION_RUNTIME_CHILD_ERROR_PUBLISH_FAILED", publish_err, job=str(job_name))

                        if bool(restart_guard.get("suppressed")):
                            with _CHILDREN_LOCK:
                                info["restart_disabled"] = True
                            _publish_child_restart_suppressed(
                                job_name,
                                restart_guard,
                                reason="start_failed",
                                detail={"error": err},
                            )
                        else:
                            with _CHILDREN_LOCK:
                                info["next_spawn_ts"] = now + _safe_float(info.get("restart_delay_s"), RESTART_BASE_S)
                                info["restart_delay_s"] = min(RESTART_MAX_S, max(RESTART_BASE_S, _safe_float(info.get("restart_delay_s"), RESTART_BASE_S) * 2.0))
                            publish_message(
                                "market_data",
                                "child_start_failed",
                                {"job": str(job_name), "error": err},
                                sender=JOB_NAME,
                                best_effort=True,
                            )

            running_children = [
                str(name)
                for name, info in children.items()
                if bool(info.get("running"))
            ]
            disabled_children = [
                str(name)
                for name, info in children.items()
                if bool(info.get("restart_disabled"))
            ]

            if running_children:
                try:
                    first_tick_seen = str(meta_get("first_price_ts_ms", "") or "").strip()
                    if not first_tick_seen:
                        set_state(WARMING_UP, "ingestion_runtime_running_awaiting_first_price_tick")
                except Exception as e:
                    _warn_failure("INGESTION_RUNTIME_SET_STATE_FAILED", e, scope="running_awaiting_first_price_tick")
            elif children and len(disabled_children) == len(children):
                fatal_payload = {
                    str(name): {
                        "last_exit_rc": info.get("last_exit_rc"),
                        "last_error": info.get("last_error"),
                    }
                    for name, info in children.items()
                }
                try:
                    publish_message(
                        "market_data",
                        "ingestion_runtime_failed",
                        {
                            "reason": "all_ingestion_children_restart_disabled",
                            "children": fatal_payload,
                        },
                        sender=JOB_NAME,
                        best_effort=True,
                    )
                except Exception as e:
                    _warn_failure("INGESTION_RUNTIME_FAILURE_PUBLISH_FAILED", e, scope="all_ingestion_children_restart_disabled")
                try:
                    set_state(DEGRADED, "all_ingestion_children_restart_disabled")
                except Exception as e:
                    _warn_failure("INGESTION_RUNTIME_SET_STATE_FAILED", e, scope="all_ingestion_children_restart_disabled")

            running_children = [
                str(name)
                for name, info in children.items()
                if bool(info.get("running"))
            ]
            disabled_children = [
                str(name)
                for name, info in children.items()
                if bool(info.get("restart_disabled"))
            ]

            if running_children:
                try:
                    if not str(meta_get("first_price_ts_ms", "") or "").strip():
                        set_state(WARMING_UP, "ingestion_runtime_running_awaiting_first_price_tick")
                except Exception as e:
                    _warn_failure("INGESTION_RUNTIME_SET_STATE_FAILED", e, scope="running_awaiting_first_price_tick_recheck")
            elif children and len(disabled_children) == len(children):
                try:
                    set_state(DEGRADED, "all_ingestion_children_restart_disabled")
                except Exception as e:
                    _warn_failure("INGESTION_RUNTIME_SET_STATE_FAILED", e, scope="all_ingestion_children_restart_disabled_recheck")

            if now - last_state >= STATE_PUBLISH_EVERY_S:
                try:
                    _publish_market_state({name: {k: v for k, v in info.items() if k != "proc"} for name, info in children.items()})
                    _write_ingestion_state(children, provider_status="running")
                    last_state = now
                except Exception as e:
                    _warn_failure("INGESTION_RUNTIME_STATE_PUBLISH_LOOP_FAILED", e)

            now_ts_ms = int(time.time() * 1000)

            try:
                _check_provider_health(now_ts_ms)
            except Exception as e:
                _warn_failure("INGESTION_RUNTIME_PROVIDER_HEALTH_CHECK_FAILED", e)

            try:
                _restart_children_for_feed_stall(children, _latest_prices_state(), now)
            except Exception as e:
                _warn_failure("INGESTION_RUNTIME_FEED_STALL_RESTART_CHECK_FAILED", e)

            time.sleep(0.25)

    finally:
        _INGESTION_STATE["running"] = False
        _write_ingestion_state(children, provider_status="stopped")
        for job_name, info in children.items():
            _terminate_child(job_name, _child_proc(info))
        try:
            release_job_lock(INGESTION_RUNTIME_JOB_LOCK_NAME, OWNER, PID)
        except Exception as e:
            _warn_failure(
                "INGESTION_RUNTIME_RELEASE_JOB_LOCK_FAILED",
                e,
                job=INGESTION_RUNTIME_JOB_LOCK_NAME,
                base_job=JOB_NAME,
                pid=int(PID),
            )


if __name__ == "__main__":
    main()
