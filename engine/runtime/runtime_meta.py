# NEW FILE: engine/runtime/runtime_meta.py
# CREATE THIS FILE EXACTLY:

import logging
import sys
import time
import os
import threading
from typing import Any, Optional

from engine.runtime import dbapi_compat as dbapi
from engine.runtime.startup_write_gate import (
    noncritical_startup_write_wait_s,
    should_defer_noncritical_startup_write,
)
from engine.runtime.storage import close_pooled_connections, connect as _db_connect, run_write_txn
from engine.runtime.state_cache import cache_get, cache_set, cache_invalidate_key, cache_invalidate_namespace

LOG = logging.getLogger("runtime_meta")


def _stderr_nonfatal(code: str, error: BaseException, **extra: Any) -> None:
    details = ", ".join(f"{k}={v}" for k, v in (extra or {}).items())
    suffix = f" ({details})" if details else ""
    try:
        sys.stderr.write(f"[engine.runtime.runtime_meta] {code}: {type(error).__name__}: {error}{suffix}\n")
        sys.stderr.flush()
    except Exception as stderr_err:
        logging.warning(
            "runtime_meta_stderr_nonfatal_write_failed code=%s error=%s",
            str(code),
            f"{type(stderr_err).__name__}: {stderr_err}",
        )
_BEST_EFFORT_SAME_VALUE_MIN_INTERVAL_MS = max(
    0,
    int(float(os.environ.get("RUNTIME_META_BEST_EFFORT_MIN_INTERVAL_S", "2.0") or 2.0) * 1000.0),
)
_BEST_EFFORT_BUFFER_ENABLED = str(os.environ.get("RUNTIME_META_BEST_EFFORT_BUFFER_ENABLED", "1")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
_BEST_EFFORT_BUFFER_FLUSH_INTERVAL_S = max(
    0.05,
    float(os.environ.get("RUNTIME_META_BEST_EFFORT_BUFFER_FLUSH_INTERVAL_S", "2.0") or 2.0),
)
_BEST_EFFORT_BUFFER_FLUSH_JITTER_RATIO = min(
    1.0,
    max(0.0, float(os.environ.get("RUNTIME_META_BEST_EFFORT_BUFFER_FLUSH_JITTER_RATIO", "0.5") or 0.5)),
)
_BEST_EFFORT_BUFFER_MAX_BATCH = max(
    1,
    int(os.environ.get("RUNTIME_META_BEST_EFFORT_BUFFER_MAX_BATCH", "64") or 64),
)
_BEST_EFFORT_BUFFER_MAX_KEYS = max(
    _BEST_EFFORT_BUFFER_MAX_BATCH,
    int(os.environ.get("RUNTIME_META_BEST_EFFORT_BUFFER_MAX_KEYS", "512") or 512),
)
_PREVIOUS_RUNTIME_META_DB_PATH = str(globals().get("_RUNTIME_META_DB_PATH") or "").strip()
_RUNTIME_META_DB_PATH = str(os.environ.get("DB_PATH") or "").strip()

_existing_meta_lock = globals().get("_BEST_EFFORT_META_LOCK")
_existing_meta_recent = globals().get("_BEST_EFFORT_META_RECENT")
_existing_buffer_lock = globals().get("_BEST_EFFORT_BUFFER_LOCK")
_existing_buffer_pending = globals().get("_BEST_EFFORT_BUFFER_PENDING")
_existing_buffer_inflight = globals().get("_BEST_EFFORT_BUFFER_INFLIGHT")
_existing_buffer_stop = globals().get("_BEST_EFFORT_BUFFER_STOP")
_existing_buffer_thread = globals().get("_BEST_EFFORT_BUFFER_THREAD")
_existing_buffer_state = globals().get("_BEST_EFFORT_BUFFER_STATE")
_RUNTIME_META_DB_PATH_CHANGED = bool(
    _PREVIOUS_RUNTIME_META_DB_PATH
    and _PREVIOUS_RUNTIME_META_DB_PATH != _RUNTIME_META_DB_PATH
)

if _RUNTIME_META_DB_PATH_CHANGED:
    if _existing_buffer_stop is not None:
        try:
            _existing_buffer_stop.set()
        except Exception as e:
            _stderr_nonfatal("RUNTIME_META_BEST_EFFORT_RELOAD_STOP_FAILED", e)
    if _existing_buffer_lock is not None:
        try:
            with _existing_buffer_lock:
                _existing_buffer_lock.notify_all()
        except Exception as e:
            _stderr_nonfatal("RUNTIME_META_BEST_EFFORT_RELOAD_NOTIFY_FAILED", e)
    if (
        isinstance(_existing_buffer_thread, threading.Thread)
        and _existing_buffer_thread.is_alive()
        and _existing_buffer_thread is not threading.current_thread()
    ):
        try:
            _existing_buffer_thread.join(timeout=1.0)
        except Exception as e:
            _stderr_nonfatal("RUNTIME_META_BEST_EFFORT_RELOAD_JOIN_FAILED", e)

_BEST_EFFORT_META_LOCK = (
    threading.Lock()
    if _RUNTIME_META_DB_PATH_CHANGED or _existing_meta_lock is None
    else _existing_meta_lock
)
_BEST_EFFORT_META_RECENT: dict[str, dict[str, Any]] = (
    {}
    if _RUNTIME_META_DB_PATH_CHANGED or not isinstance(_existing_meta_recent, dict)
    else _existing_meta_recent
)
_BEST_EFFORT_BUFFER_LOCK = (
    threading.Condition()
    if _RUNTIME_META_DB_PATH_CHANGED or _existing_buffer_lock is None
    else _existing_buffer_lock
)
_BEST_EFFORT_BUFFER_PENDING: dict[str, dict[str, Any]] = (
    {}
    if _RUNTIME_META_DB_PATH_CHANGED or not isinstance(_existing_buffer_pending, dict)
    else _existing_buffer_pending
)
_BEST_EFFORT_BUFFER_INFLIGHT: dict[str, dict[str, Any]] = (
    {}
    if _RUNTIME_META_DB_PATH_CHANGED or not isinstance(_existing_buffer_inflight, dict)
    else _existing_buffer_inflight
)
_BEST_EFFORT_BUFFER_STOP = (
    threading.Event()
    if _RUNTIME_META_DB_PATH_CHANGED or _existing_buffer_stop is None
    else _existing_buffer_stop
)
_BEST_EFFORT_BUFFER_THREAD: threading.Thread | None = (
    None
    if _RUNTIME_META_DB_PATH_CHANGED or not isinstance(_existing_buffer_thread, threading.Thread)
    else _existing_buffer_thread
)
_BEST_EFFORT_BUFFER_STATE: dict[str, Any] = (
    _existing_buffer_state if (not _RUNTIME_META_DB_PATH_CHANGED and isinstance(_existing_buffer_state, dict)) else {
        "buffered_keys": 0,
        "flush_batches": 0,
        "flushed_keys": 0,
        "dropped_keys": 0,
        "last_enqueue_ts_ms": 0,
        "last_flush_ts_ms": 0,
        "last_error": "",
        "last_error_ts_ms": 0,
    }
)

_BEST_EFFORT_BUFFER_STATE = dict(_BEST_EFFORT_BUFFER_STATE)
_BEST_EFFORT_BUFFER_STATE.update(
    {
        "buffered_keys": int(len(_BEST_EFFORT_BUFFER_PENDING)),
        "last_error": str(_BEST_EFFORT_BUFFER_STATE.get("last_error") or ""),
        "last_error_ts_ms": int(_BEST_EFFORT_BUFFER_STATE.get("last_error_ts_ms") or 0),
    }
)
_BEST_EFFORT_BUFFER_STATE: dict[str, Any] = {
    "buffered_keys": 0,
    **_BEST_EFFORT_BUFFER_STATE,
}


def _staggered_flush_interval_s(base_interval_s: float, jitter_ratio: float) -> float:
    base = max(0.05, float(base_interval_s))
    jitter = min(1.0, max(0.0, float(jitter_ratio)))
    if jitter <= 0.0:
        return float(base)
    # Spread best-effort control-plane writes across processes so volatile
    # runtime status keys do not synchronize into the same writer window.
    bucket = max(0, int(os.getpid()) % 17)
    return float(base * (1.0 + ((float(bucket) / 16.0) * jitter)))


def _best_effort_flush_backoff_s(consecutive_failures: int) -> float:
    failures = max(1, min(int(consecutive_failures), 5))
    base_interval_s = max(1.0, float(_BEST_EFFORT_BUFFER_EFFECTIVE_FLUSH_INTERVAL_S))
    return min(10.0, float(base_interval_s * (2 ** (failures - 1))))


_BEST_EFFORT_BUFFER_EFFECTIVE_FLUSH_INTERVAL_S = _staggered_flush_interval_s(
    _BEST_EFFORT_BUFFER_FLUSH_INTERVAL_S,
    _BEST_EFFORT_BUFFER_FLUSH_JITTER_RATIO,
)
_BEST_EFFORT_BUFFER_FLUSH_INTERVAL_MS = max(
    50,
    int(_BEST_EFFORT_BUFFER_EFFECTIVE_FLUSH_INTERVAL_S * 1000.0),
)

_VOLATILE_KEYS = {
    "first_price_ts_ms",
    "ingestion_state",
    "dashboard_bound_ts_ms",
    "dashboard_bound_detail",
    "warmup_started_ts_ms",
    "warmup_timeout_ts_ms",
    "lifecycle_state",
    "lifecycle_detail",
    "lifecycle_prev_state",
    "lifecycle_updated_ts_ms",
    "last_clean_shutdown_ts_ms",
    "last_crash_shutdown_ts_ms",
    "last_crash_reason",
}

_IMMEDIATE_BEST_EFFORT_KEYS = {
    # The ingestion watchdog and startup health gates poll this key directly
    # after process bring-up, so leaving it buffered can report a false
    # negative while the async writer is still pending.
    "ingestion_state",
}
_DATA_QUALITY_KEY_PREFIX = "data_quality::"


def _is_volatile_key(key: str) -> bool:
    key_s = str(key or "").strip()
    return (
        key_s in _VOLATILE_KEYS
        or key_s.startswith("lifecycle_")
        or key_s.startswith(_DATA_QUALITY_KEY_PREFIX)
    )


def _should_buffer_best_effort_key(key: str) -> bool:
    key_s = str(key or "").strip()
    if key_s in _IMMEDIATE_BEST_EFFORT_KEYS:
        return False
    if key_s.startswith(_DATA_QUALITY_KEY_PREFIX):
        # Health gates consume these keys across process boundaries. Buffering
        # them can make /api/health report stale feature/model-input state even
        # while the supervised inference probe is successfully refreshing it.
        return False
    return bool(
        _is_volatile_key(key_s)
        or key_s in {
            "price_provider_active",
            "price_provider_health_snapshot",
            "dashboard_boot_diagnostics",
            "import_smoke",
            "startup_health_validation",
            "startup_prebind_gates",
            "startup_trace",
        }
        or key_s.startswith("provider_session_")
    )


def _ensure_runtime_meta(con) -> None:
    # runtime_meta is the lightweight control-plane key/value store used for
    # boot traces, lifecycle breadcrumbs, and other cross-module status.
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS runtime_meta (
          key TEXT PRIMARY KEY,
          value TEXT,
          updated_ts_ms INTEGER
        )
        """
        )


def _runtime_meta_cache_key(key: str) -> str:
    db_path = str(os.environ.get("DB_PATH") or "").strip()
    return f"{db_path}|{str(key or '').strip()}"


def _note_best_effort_buffer_state(**updates: Any) -> None:
    with _BEST_EFFORT_BUFFER_LOCK:
        _BEST_EFFORT_BUFFER_STATE.update(dict(updates or {}))


def _current_runtime_meta_db_path() -> str:
    return str(os.environ.get("DB_PATH") or "").strip()


def _reset_best_effort_buffer_for_current_db_path_if_needed() -> None:
    global _RUNTIME_META_DB_PATH
    current = _current_runtime_meta_db_path()
    if current == _RUNTIME_META_DB_PATH:
        return
    with _BEST_EFFORT_META_LOCK:
        _BEST_EFFORT_META_RECENT.clear()
    with _BEST_EFFORT_BUFFER_LOCK:
        _BEST_EFFORT_BUFFER_PENDING.clear()
        _BEST_EFFORT_BUFFER_INFLIGHT.clear()
        _BEST_EFFORT_BUFFER_STATE.update(
            {
                "buffered_keys": 0,
                "last_enqueue_ts_ms": 0,
                "last_flush_ts_ms": 0,
                "last_error": "",
                "last_error_ts_ms": 0,
            }
        )
        _RUNTIME_META_DB_PATH = current
        _BEST_EFFORT_BUFFER_LOCK.notify_all()


def _get_pending_best_effort_value(key_s: str) -> tuple[bool, str | None]:
    _reset_best_effort_buffer_for_current_db_path_if_needed()
    key_name = str(key_s or "").strip()
    with _BEST_EFFORT_BUFFER_LOCK:
        pending = _BEST_EFFORT_BUFFER_PENDING.get(key_name)
        if pending is None:
            pending = _BEST_EFFORT_BUFFER_INFLIGHT.get(key_name)
        if pending is None:
            return False, None
        return True, str(pending.get("value", ""))


def _should_skip_best_effort_write(key_s: str, value_s: str) -> bool:
    if _BEST_EFFORT_SAME_VALUE_MIN_INTERVAL_MS <= 0:
        return False
    now_ms = int(time.time() * 1000)
    with _BEST_EFFORT_META_LOCK:
        recent = dict(_BEST_EFFORT_META_RECENT.get(str(key_s)) or {})
    if str(recent.get("value") or "") != str(value_s):
        return False
    written_ts_ms = int(recent.get("written_ts_ms") or 0)
    return bool(written_ts_ms > 0 and (now_ms - written_ts_ms) < _BEST_EFFORT_SAME_VALUE_MIN_INTERVAL_MS)


def _note_best_effort_write(key_s: str, value_s: str) -> None:
    with _BEST_EFFORT_META_LOCK:
        _BEST_EFFORT_META_RECENT[str(key_s)] = {
            "value": str(value_s),
            "written_ts_ms": int(time.time() * 1000),
        }


def _enqueue_best_effort_write(key_s: str, value_s: str) -> bool:
    if (not _BEST_EFFORT_BUFFER_ENABLED) or (not _should_buffer_best_effort_key(key_s)):
        return False
    _reset_best_effort_buffer_for_current_db_path_if_needed()
    now_ms = int(time.time() * 1000)
    with _BEST_EFFORT_BUFFER_LOCK:
        key_name = str(key_s)
        if key_name in _BEST_EFFORT_BUFFER_PENDING:
            _BEST_EFFORT_BUFFER_PENDING.pop(key_name, None)
        elif len(_BEST_EFFORT_BUFFER_PENDING) >= _BEST_EFFORT_BUFFER_MAX_KEYS:
            oldest_key = next(iter(_BEST_EFFORT_BUFFER_PENDING.keys()), None)
            if oldest_key is not None:
                _BEST_EFFORT_BUFFER_PENDING.pop(oldest_key, None)
                _BEST_EFFORT_BUFFER_STATE["dropped_keys"] = int(_BEST_EFFORT_BUFFER_STATE.get("dropped_keys") or 0) + 1
        _BEST_EFFORT_BUFFER_PENDING[key_name] = {
            "value": str(value_s),
            "enqueued_ts_ms": int(now_ms),
        }
        _BEST_EFFORT_BUFFER_STATE["buffered_keys"] = int(len(_BEST_EFFORT_BUFFER_PENDING))
        _BEST_EFFORT_BUFFER_STATE["last_enqueue_ts_ms"] = int(now_ms)
        _BEST_EFFORT_BUFFER_LOCK.notify_all()
    _ensure_best_effort_writer_thread()
    return True


def _drain_best_effort_rows(*, max_keys: int | None = None) -> list[tuple[str, str]]:
    with _BEST_EFFORT_BUFFER_LOCK:
        limit = max(1, int(max_keys or _BEST_EFFORT_BUFFER_MAX_BATCH))
        if not _BEST_EFFORT_BUFFER_PENDING:
            return []
        rows: list[tuple[str, str]] = []
        keys = list(_BEST_EFFORT_BUFFER_PENDING.keys())[:limit]
        for key_name in keys:
            payload = dict(_BEST_EFFORT_BUFFER_PENDING.pop(key_name, {}) or {})
            if not payload:
                continue
            _BEST_EFFORT_BUFFER_INFLIGHT[key_name] = payload
            rows.append((str(key_name), str(payload.get("value", ""))))
        _BEST_EFFORT_BUFFER_STATE["buffered_keys"] = int(len(_BEST_EFFORT_BUFFER_PENDING))
        return rows


def _clear_inflight_best_effort_rows(rows: list[tuple[str, str]]) -> None:
    if not rows:
        return
    with _BEST_EFFORT_BUFFER_LOCK:
        for key_name, _value in rows:
            _BEST_EFFORT_BUFFER_INFLIGHT.pop(str(key_name), None)


def _restore_inflight_best_effort_rows(rows: list[tuple[str, str]]) -> None:
    if not rows:
        return
    with _BEST_EFFORT_BUFFER_LOCK:
        for key_name, value_s in rows:
            payload = _BEST_EFFORT_BUFFER_INFLIGHT.pop(str(key_name), None)
            if str(key_name) in _BEST_EFFORT_BUFFER_PENDING:
                continue
            if payload is None:
                payload = {
                    "value": str(value_s),
                    "enqueued_ts_ms": int(time.time() * 1000),
                }
            _BEST_EFFORT_BUFFER_PENDING[str(key_name)] = dict(payload)
        _BEST_EFFORT_BUFFER_STATE["buffered_keys"] = int(len(_BEST_EFFORT_BUFFER_PENDING))
        _BEST_EFFORT_BUFFER_LOCK.notify_all()


def _flush_best_effort_rows(
    rows: list[tuple[str, str]],
    *,
    operation: str = "flush_runtime_meta_best_effort_buffer",
) -> int:
    if not rows:
        return 0

    def _txn(con) -> None:
        _ensure_runtime_meta(con)
        now_ms = int(time.time() * 1000)
        con.executemany(
            """
            INSERT INTO runtime_meta(key, value, updated_ts_ms)
            VALUES(?,?,?)
            ON CONFLICT(key) DO UPDATE SET
              value=excluded.value,
              updated_ts_ms=excluded.updated_ts_ms
            """,
            [
                (str(key_name), str(value_s), int(now_ms))
                for key_name, value_s in rows
            ],
        )

    _run_meta_write(
        _txn,
        attempts=1,
        timeout_s=0.25,
        busy_timeout_ms=250,
    )
    for key_name, value_s in rows:
        _note_best_effort_write(str(key_name), str(value_s))
    return int(len(rows))


def _best_effort_flush_ready() -> bool:
    pending_count = int(len(_BEST_EFFORT_BUFFER_PENDING))
    if pending_count <= 0:
        return False
    if pending_count >= _BEST_EFFORT_BUFFER_MAX_BATCH:
        return True
    now_ms = int(time.time() * 1000)
    oldest_enqueued_ts_ms = min(
        int((payload or {}).get("enqueued_ts_ms") or now_ms)
        for payload in _BEST_EFFORT_BUFFER_PENDING.values()
    )
    return bool((now_ms - oldest_enqueued_ts_ms) >= _BEST_EFFORT_BUFFER_FLUSH_INTERVAL_MS)


def _best_effort_writer_loop() -> None:
    consecutive_failures = 0
    while True:
        _reset_best_effort_buffer_for_current_db_path_if_needed()
        buffer_condition = _BEST_EFFORT_BUFFER_LOCK
        with buffer_condition:
            while True:
                if _BEST_EFFORT_BUFFER_STOP.is_set() and not _BEST_EFFORT_BUFFER_PENDING:
                    return
                if _best_effort_flush_ready():
                    break
                wait_s = float(_BEST_EFFORT_BUFFER_EFFECTIVE_FLUSH_INTERVAL_S)
                if _BEST_EFFORT_BUFFER_PENDING:
                    now_ms = int(time.time() * 1000)
                    oldest_enqueued_ts_ms = min(
                        int((payload or {}).get("enqueued_ts_ms") or now_ms)
                        for payload in _BEST_EFFORT_BUFFER_PENDING.values()
                    )
                    remaining_ms = max(
                        1,
                        int(oldest_enqueued_ts_ms + _BEST_EFFORT_BUFFER_FLUSH_INTERVAL_MS - now_ms),
                    )
                    wait_s = max(0.01, float(remaining_ms) / 1000.0)
                buffer_condition.wait(timeout=wait_s)
        if should_defer_noncritical_startup_write():
            _BEST_EFFORT_BUFFER_STOP.wait(timeout=float(noncritical_startup_write_wait_s()))
            continue
        rows = _drain_best_effort_rows()
        if not rows:
            continue
        try:
            flushed = _flush_best_effort_rows(rows)
            consecutive_failures = 0
            _clear_inflight_best_effort_rows(rows)
            _note_best_effort_buffer_state(
                flush_batches=int(_BEST_EFFORT_BUFFER_STATE.get("flush_batches") or 0) + 1,
                flushed_keys=int(_BEST_EFFORT_BUFFER_STATE.get("flushed_keys") or 0) + int(flushed),
                last_flush_ts_ms=int(time.time() * 1000),
                last_error="",
            )
        except Exception as e:
            consecutive_failures = min(consecutive_failures + 1, 5)
            _restore_inflight_best_effort_rows(rows)
            _note_best_effort_buffer_state(
                last_error=str(e),
                last_error_ts_ms=int(time.time() * 1000),
            )
            _stderr_nonfatal("RUNTIME_META_BEST_EFFORT_BUFFER_FLUSH_FAILED", e)
            _BEST_EFFORT_BUFFER_STOP.wait(timeout=_best_effort_flush_backoff_s(consecutive_failures))


def _ensure_best_effort_writer_thread() -> None:
    global _BEST_EFFORT_BUFFER_THREAD
    if not _BEST_EFFORT_BUFFER_ENABLED:
        return
    with _BEST_EFFORT_BUFFER_LOCK:
        if _BEST_EFFORT_BUFFER_THREAD is not None and _BEST_EFFORT_BUFFER_THREAD.is_alive():
            return
        _BEST_EFFORT_BUFFER_STOP.clear()
        _BEST_EFFORT_BUFFER_THREAD = threading.Thread(
            target=_best_effort_writer_loop,
            name="runtime-meta-best-effort-writer",
            daemon=True,
        )
        _BEST_EFFORT_BUFFER_THREAD.start()


def flush_best_effort_runtime_meta_buffer(*, max_batches: int | None = None) -> dict[str, Any]:
    _reset_best_effort_buffer_for_current_db_path_if_needed()
    batches = 0
    flushed = 0
    while True:
        if max_batches is not None and batches >= max(1, int(max_batches)):
            break
        rows = _drain_best_effort_rows(max_keys=_BEST_EFFORT_BUFFER_MAX_BATCH)
        if not rows:
            break
        try:
            flushed += int(_flush_best_effort_rows(rows, operation="manual_flush_runtime_meta_best_effort_buffer"))
            _clear_inflight_best_effort_rows(rows)
            batches += 1
        except Exception:
            _restore_inflight_best_effort_rows(rows)
            raise
    snapshot = runtime_meta_best_effort_buffer_snapshot()
    snapshot["manual_flush_batches"] = int(batches)
    snapshot["manual_flushed_keys"] = int(flushed)
    return snapshot


def shutdown_best_effort_runtime_meta_buffer(*, timeout_s: float = 2.0) -> dict[str, Any]:
    global _BEST_EFFORT_BUFFER_THREAD
    if not _BEST_EFFORT_BUFFER_ENABLED:
        return runtime_meta_best_effort_buffer_snapshot()
    _BEST_EFFORT_BUFFER_STOP.set()
    with _BEST_EFFORT_BUFFER_LOCK:
        _BEST_EFFORT_BUFFER_LOCK.notify_all()
        thread = _BEST_EFFORT_BUFFER_THREAD
    if thread is not None and thread.is_alive() and thread is not threading.current_thread():
        try:
            thread.join(timeout=max(0.1, float(timeout_s)))
        except Exception as e:
            _stderr_nonfatal("RUNTIME_META_BEST_EFFORT_BUFFER_JOIN_FAILED", e)
    _BEST_EFFORT_BUFFER_THREAD = None
    return flush_best_effort_runtime_meta_buffer()


def runtime_meta_best_effort_buffer_snapshot() -> dict[str, Any]:
    _reset_best_effort_buffer_for_current_db_path_if_needed()
    with _BEST_EFFORT_BUFFER_LOCK:
        state = dict(_BEST_EFFORT_BUFFER_STATE)
        state["buffered_keys"] = int(len(_BEST_EFFORT_BUFFER_PENDING))
        state["inflight_keys"] = int(len(_BEST_EFFORT_BUFFER_INFLIGHT))
        state["enabled"] = bool(_BEST_EFFORT_BUFFER_ENABLED)
        state["thread_alive"] = bool(_BEST_EFFORT_BUFFER_THREAD is not None and _BEST_EFFORT_BUFFER_THREAD.is_alive())
    state["flush_interval_s"] = float(_BEST_EFFORT_BUFFER_EFFECTIVE_FLUSH_INTERVAL_S)
    state["flush_interval_base_s"] = float(_BEST_EFFORT_BUFFER_FLUSH_INTERVAL_S)
    state["flush_jitter_ratio"] = float(_BEST_EFFORT_BUFFER_FLUSH_JITTER_RATIO)
    state["batch_size"] = int(_BEST_EFFORT_BUFFER_MAX_BATCH)
    state["buffer_max_keys"] = int(_BEST_EFFORT_BUFFER_MAX_KEYS)
    state["db_path"] = str(_RUNTIME_META_DB_PATH)
    return state


def _active_write_txn_connection():
    con = None
    try:
        con = _db_connect(readonly=False)
    except Exception as e:
        sys.stderr.write(f"[runtime_meta] active_write_txn_connection_failed: {type(e).__name__}: {e}\n")
        sys.stderr.flush()
        return None
    if bool(getattr(con, "in_transaction", False)):
        return con
    try:
        con.close()
    except Exception as e:
        _stderr_nonfatal("RUNTIME_META_ACTIVE_CONNECTION_CLOSE_FAILED", e)
    return None


def _upsert_runtime_meta(con, key_s: str, value_s: str) -> None:
    _ensure_runtime_meta(con)
    now = int(time.time() * 1000)
    con.execute(
        """
        INSERT INTO runtime_meta(key, value, updated_ts_ms)
        VALUES(?,?,?)
        ON CONFLICT(key) DO UPDATE SET
          value=excluded.value,
          updated_ts_ms=excluded.updated_ts_ms
        """,
        (key_s, value_s, int(now)),
    )


def _run_meta_write(
    fn,
    *,
    attempts: Optional[int] = None,
    timeout_s: Optional[float] = None,
    busy_timeout_ms: Optional[int] = None,
) -> None:
    try:
        run_write_txn(
            fn,
            attempts=attempts,
            table="runtime_meta",
            operation="runtime_meta_write",
            direct=True,
            maintenance=False,
            timeout_s=timeout_s,
            busy_timeout_ms=busy_timeout_ms,
        )
        return
    except Exception as e:
        if not (isinstance(e, dbapi.OperationalError) or dbapi.is_sqlite_error(e, "OperationalError")):
            raise
        if "write_transaction_already_active" not in str(e or ""):
            raise
    # A stale pooled write handle can survive helper boundaries in long-lived
    # ingestion loops. Drop pooled handles and retry once instead of letting
    # control-plane status writes silently fail.
    close_pooled_connections()
    run_write_txn(
        fn,
        attempts=attempts,
        table="runtime_meta",
        operation="runtime_meta_write",
        direct=True,
        maintenance=False,
        timeout_s=timeout_s,
        busy_timeout_ms=busy_timeout_ms,
    )


def meta_get(key: str, default: Optional[str] = None) -> Optional[str]:
    key_s = str(key)
    cache_key = _runtime_meta_cache_key(key_s)
    if not _is_volatile_key(key_s):
        cached = cache_get("runtime_meta", cache_key)
        if cached is not None:
            return str(cached) if cached is not None else default
    else:
        pending, pending_value = _get_pending_best_effort_value(key_s)
        if pending:
            return str(pending_value) if pending_value is not None else default

    # Reads prefer the cache because runtime_meta values are often polled by
    # dashboards and health endpoints.
    con = _db_connect(readonly=True)
    try:
        row = con.execute("SELECT value FROM runtime_meta WHERE key=?", (key_s,)).fetchone()
        value = str(row[0]) if row and row[0] is not None else default
        if not _is_volatile_key(key_s):
            cache_set("runtime_meta", cache_key, value, ttl_s=3600.0)
        return value
    finally:
        try:
            con.close()
        except Exception as e:
            try:
                from engine.runtime.failure_diagnostics import log_failure
                from engine.runtime.logging import get_logger

                log_failure(
                    get_logger("engine.runtime.runtime_meta"),
                    event="runtime_meta_connection_close_failed",
                    code="RUNTIME_META_CONNECTION_CLOSE_FAILED",
                    message="runtime_meta_connection_close_failed",
                    error=e,
                    level=30,
                    component="engine.runtime.runtime_meta",
                    extra={"key": str(key_s)},
                    persist=False,
                )
            except Exception:
                raise

def meta_set(key: str, value: str, *, best_effort: bool = False) -> None:
    key_s = str(key)
    value_s = str(value)
    cache_key = _runtime_meta_cache_key(key_s)

    def _txn(con) -> None:
        _upsert_runtime_meta(con, key_s, value_s)

    skipped_best_effort = False
    if bool(best_effort) and _should_skip_best_effort_write(key_s, value_s):
        skipped_best_effort = True

    active_con = _active_write_txn_connection()
    if not skipped_best_effort and active_con is not None:
        _upsert_runtime_meta(active_con, key_s, value_s)
        if bool(best_effort):
            _note_best_effort_write(key_s, value_s)
    elif not skipped_best_effort and bool(best_effort) and _enqueue_best_effort_write(key_s, value_s):
        pass
    elif not skipped_best_effort:
        _run_meta_write(
            _txn,
            attempts=(1 if bool(best_effort) else None),
            timeout_s=(0.25 if bool(best_effort) else None),
            busy_timeout_ms=(250 if bool(best_effort) else None),
        )
        if bool(best_effort):
            _note_best_effort_write(key_s, value_s)
    # Update cache immediately so readers see the new control-plane state
    # without waiting for the next DB round trip.
    if _is_volatile_key(key_s):
        cache_invalidate_key("runtime_meta", cache_key)
    else:
        cache_set("runtime_meta", cache_key, value_s, ttl_s=3600.0)
    cache_invalidate_namespace("lifecycle_state")


def meta_set_if_missing(key: str, value: str) -> bool:
    """
    Returns True if it set the value, False if already present.
    This is used for first-writer-wins markers where later boot stages should
    not overwrite the original source of truth.
    """
    key_s = str(key)
    value_s = str(value)
    cache_key = _runtime_meta_cache_key(key_s)

    if not _is_volatile_key(key_s):
        cached = cache_get("runtime_meta", cache_key)
        if cached is not None and str(cached) != "":
            return False

    result = {"set": False, "value": None}

    def _txn(con) -> None:
        _ensure_runtime_meta(con)
        row = con.execute("SELECT value FROM runtime_meta WHERE key=?", (key_s,)).fetchone()
        if row and row[0] is not None and str(row[0]) != "":
            result["set"] = False
            result["value"] = str(row[0])
            return

        now = int(time.time() * 1000)
        con.execute(
            """
            INSERT INTO runtime_meta(key, value, updated_ts_ms)
            VALUES(?,?,?)
            ON CONFLICT(key) DO UPDATE SET
              value=excluded.value,
              updated_ts_ms=excluded.updated_ts_ms
            """,
            (key_s, value_s, int(now)),
        )
        result["set"] = True
        result["value"] = value_s

    active_con = _active_write_txn_connection()
    if active_con is not None:
        _txn(active_con)
    else:
        _run_meta_write(_txn)

    if result["value"] is not None:
        if _is_volatile_key(key_s):
            cache_invalidate_key("runtime_meta", cache_key)
        else:
            cache_set("runtime_meta", cache_key, str(result["value"]), ttl_s=3600.0)
    if result["set"]:
        cache_invalidate_namespace("lifecycle_state")
    return bool(result["set"])
