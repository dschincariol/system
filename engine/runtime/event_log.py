"""Append-only runtime event log helpers used for audit and replay surfaces."""

import json
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.ingestion_tuning import env_bool, tuned_float, tuned_int
from engine.runtime.logging import get_logger
from engine.runtime.startup_write_gate import (
    noncritical_startup_write_wait_s,
    should_defer_noncritical_startup_write,
)
from engine.runtime.storage import connect, init_db, run_write_txn

LOG = get_logger("runtime.event_log")

SCHEMA = ""
_EVENT_LOG_BUFFER_ENABLED = env_bool("EVENT_LOG_BUFFER_ENABLED", default=True)
_EVENT_LOG_BUFFER_FLUSH_INTERVAL_S = tuned_float("EVENT_LOG_BUFFER_FLUSH_INTERVAL_S", 0.5, 0.05, 5.0)
_EVENT_LOG_BUFFER_FLUSH_JITTER_RATIO = tuned_float("EVENT_LOG_BUFFER_FLUSH_JITTER_RATIO", 0.25, 0.0, 1.0)
_EVENT_LOG_BUFFER_MAX_BATCH = tuned_int("EVENT_LOG_BUFFER_MAX_BATCH", 128, 1, 4096)
_EVENT_LOG_BUFFER_MAX_ROWS = max(
    _EVENT_LOG_BUFFER_MAX_BATCH,
    tuned_int("EVENT_LOG_BUFFER_MAX_ROWS", 2048, 1, 65536),
)
_EVENT_LOG_BUFFER_LOCK = threading.Condition()
_EVENT_LOG_BUFFER_PENDING: List[tuple[int, str, str, int, str | None, str | None, str | None, str]] = []
_EVENT_LOG_BUFFER_STOP = threading.Event()
_EVENT_LOG_BUFFER_THREAD: threading.Thread | None = None
_EVENT_LOG_BUFFER_STATE: dict[str, Any] = {
    "buffered_rows": 0,
    "inflight_rows": 0,
    "dropped_rows": 0,
    "flush_batches": 0,
    "flushed_rows": 0,
    "last_enqueue_ts_ms": 0,
    "last_flush_ts_ms": 0,
    "last_error": "",
    "last_error_ts_ms": 0,
}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _staggered_flush_interval_s(base_interval_s: float, jitter_ratio: float) -> float:
    base = max(0.05, float(base_interval_s))
    jitter = min(1.0, max(0.0, float(jitter_ratio)))
    if jitter <= 0.0:
        return float(base)
    # Spread periodic standalone event flushes across processes so bursty
    # dashboards and supervisors do not all contend on the same commit boundary.
    bucket = max(0, int(os.getpid()) % 17)
    return float(base * (1.0 + ((float(bucket) / 16.0) * jitter)))


def _event_log_flush_backoff_s(consecutive_failures: int) -> float:
    failures = max(1, min(int(consecutive_failures), 5))
    base_interval_s = max(1.0, float(_EVENT_LOG_EFFECTIVE_FLUSH_INTERVAL_S))
    return min(10.0, float(base_interval_s * (2 ** (failures - 1))))


_EVENT_LOG_EFFECTIVE_FLUSH_INTERVAL_S = _staggered_flush_interval_s(
    _EVENT_LOG_BUFFER_FLUSH_INTERVAL_S,
    _EVENT_LOG_BUFFER_FLUSH_JITTER_RATIO,
)


def _dump_json(payload: Optional[Dict[str, Any]]) -> str:
    try:
        return json.dumps(payload or {}, separators=(",", ":"), sort_keys=True, default=str)
    except Exception as e:
        log_failure(
            LOG,
            event="runtime_event_log_dump_json_failed",
            code="RUNTIME_EVENT_LOG_DUMP_JSON_FAILED",
            message="runtime_event_log_dump_json_failed",
            error=e,
            level=logging.WARNING,
            component="engine.runtime.event_log",
            persist=False,
        )
        return "{}"


def _event_log_row(
    *,
    ts_ms: int,
    event_type: str,
    event_source: str,
    event_version: int,
    entity_type: Optional[str],
    entity_id: Optional[str],
    correlation_id: Optional[str],
    payload: Optional[Dict[str, Any]],
) -> tuple[int, str, str, int, str | None, str | None, str | None, str]:
    return (
        int(ts_ms),
        str(event_type),
        str(event_source),
        int(event_version),
        (str(entity_type) if entity_type is not None else None),
        (str(entity_id) if entity_id is not None else None),
        (str(correlation_id) if correlation_id is not None else None),
        _dump_json(payload),
    )


def _event_log_buffer_snapshot() -> dict[str, Any]:
    with _EVENT_LOG_BUFFER_LOCK:
        thread_alive = bool(_EVENT_LOG_BUFFER_THREAD is not None and _EVENT_LOG_BUFFER_THREAD.is_alive())
        state = dict(_EVENT_LOG_BUFFER_STATE)
    state["enabled"] = bool(_EVENT_LOG_BUFFER_ENABLED)
    state["thread_alive"] = bool(thread_alive)
    state["buffer_max_rows"] = int(_EVENT_LOG_BUFFER_MAX_ROWS)
    state["flush_interval_s"] = float(_EVENT_LOG_EFFECTIVE_FLUSH_INTERVAL_S)
    state["flush_interval_base_s"] = float(_EVENT_LOG_BUFFER_FLUSH_INTERVAL_S)
    state["flush_jitter_ratio"] = float(_EVENT_LOG_BUFFER_FLUSH_JITTER_RATIO)
    state["batch_size"] = int(_EVENT_LOG_BUFFER_MAX_BATCH)
    return state


def _flush_event_log_rows(
    rows: List[tuple[int, str, str, int, str | None, str | None, str | None, str]],
    *,
    operation: str = "flush_event_log_buffer",
) -> int:
    if not rows:
        return 0

    def _write(conw) -> None:
        conw.executemany(
            """
            INSERT INTO event_log(
              ts_ms, event_type, event_source, event_version,
              entity_type, entity_id, correlation_id, payload_json
            )
            VALUES (?,?,?,?,?,?,?,?)
            """,
            rows,
        )

    run_write_txn(
        _write,
        attempts=1,
        table="event_log",
        operation=str(operation or "flush_event_log_buffer"),
        direct=True,
        maintenance=False,
        timeout_s=0.25,
        busy_timeout_ms=250,
    )
    return int(len(rows))


def _drain_event_log_buffer(
    *,
    max_rows: int | None = None,
    track_inflight: bool = False,
) -> List[tuple[int, str, str, int, str | None, str | None, str | None, str]]:
    with _EVENT_LOG_BUFFER_LOCK:
        limit = max(1, int(max_rows or _EVENT_LOG_BUFFER_MAX_BATCH))
        if not _EVENT_LOG_BUFFER_PENDING:
            return []
        rows = list(_EVENT_LOG_BUFFER_PENDING[:limit])
        del _EVENT_LOG_BUFFER_PENDING[:limit]
        _EVENT_LOG_BUFFER_STATE["buffered_rows"] = int(len(_EVENT_LOG_BUFFER_PENDING))
        if bool(track_inflight):
            _EVENT_LOG_BUFFER_STATE["inflight_rows"] = int(_EVENT_LOG_BUFFER_STATE.get("inflight_rows") or 0) + int(
                len(rows)
            )
        return rows


def _finish_event_log_inflight(row_count: int) -> None:
    rows = max(0, int(row_count or 0))
    if rows <= 0:
        return
    with _EVENT_LOG_BUFFER_LOCK:
        _EVENT_LOG_BUFFER_STATE["inflight_rows"] = max(
            0,
            int(_EVENT_LOG_BUFFER_STATE.get("inflight_rows") or 0) - rows,
        )
        _EVENT_LOG_BUFFER_LOCK.notify_all()


def _requeue_event_log_rows(
    rows: List[tuple[int, str, str, int, str | None, str | None, str | None, str]],
) -> None:
    if not rows:
        return
    with _EVENT_LOG_BUFFER_LOCK:
        room = max(0, int(_EVENT_LOG_BUFFER_MAX_ROWS) - int(len(_EVENT_LOG_BUFFER_PENDING)))
        kept = list(rows[:room])
        dropped = max(0, len(rows) - len(kept))
        if kept:
            _EVENT_LOG_BUFFER_PENDING[:0] = kept
        _EVENT_LOG_BUFFER_STATE["buffered_rows"] = int(len(_EVENT_LOG_BUFFER_PENDING))
        if dropped > 0:
            _EVENT_LOG_BUFFER_STATE["dropped_rows"] = int(_EVENT_LOG_BUFFER_STATE.get("dropped_rows") or 0) + int(
                dropped
            )
        _EVENT_LOG_BUFFER_LOCK.notify_all()


def _event_log_writer_loop() -> None:
    consecutive_failures = 0
    pending_since_s = 0.0
    while True:
        with _EVENT_LOG_BUFFER_LOCK:
            while True:
                if (not _EVENT_LOG_BUFFER_PENDING) and (not _EVENT_LOG_BUFFER_STOP.is_set()):
                    pending_since_s = 0.0
                    _EVENT_LOG_BUFFER_LOCK.wait(timeout=float(_EVENT_LOG_EFFECTIVE_FLUSH_INTERVAL_S))
                if _EVENT_LOG_BUFFER_STOP.is_set() and not _EVENT_LOG_BUFFER_PENDING:
                    return
                if _EVENT_LOG_BUFFER_PENDING and not _EVENT_LOG_BUFFER_STOP.is_set():
                    if should_defer_noncritical_startup_write():
                        break
                    # Coalesce short bursts of audit events so background flushes
                    # respect the configured interval instead of contending per row.
                    now_s = float(time.monotonic())
                    if pending_since_s <= 0.0:
                        pending_since_s = float(now_s)
                    wait_s = float(_EVENT_LOG_EFFECTIVE_FLUSH_INTERVAL_S) - (
                        float(now_s) - float(pending_since_s)
                    )
                    if wait_s > 0.0:
                        _EVENT_LOG_BUFFER_LOCK.wait(timeout=max(0.01, float(wait_s)))
                        continue
                break
        if should_defer_noncritical_startup_write():
            _EVENT_LOG_BUFFER_STOP.wait(timeout=float(noncritical_startup_write_wait_s()))
            continue
        rows = _drain_event_log_buffer(track_inflight=True)
        if not rows:
            continue
        try:
            flushed = _flush_event_log_rows(rows)
            consecutive_failures = 0
            now_ms = int(time.time() * 1000)
            with _EVENT_LOG_BUFFER_LOCK:
                _EVENT_LOG_BUFFER_STATE["flush_batches"] = int(_EVENT_LOG_BUFFER_STATE.get("flush_batches") or 0) + 1
                _EVENT_LOG_BUFFER_STATE["flushed_rows"] = int(_EVENT_LOG_BUFFER_STATE.get("flushed_rows") or 0) + int(
                    flushed
                )
                _EVENT_LOG_BUFFER_STATE["last_flush_ts_ms"] = int(now_ms)
                _EVENT_LOG_BUFFER_STATE["last_error"] = ""
        except Exception as e:
            consecutive_failures = min(consecutive_failures + 1, 5)
            _requeue_event_log_rows(rows)
            with _EVENT_LOG_BUFFER_LOCK:
                _EVENT_LOG_BUFFER_STATE["last_error"] = f"{type(e).__name__}:{e}"
                _EVENT_LOG_BUFFER_STATE["last_error_ts_ms"] = int(time.time() * 1000)
            log_failure(
                LOG,
                event="runtime_event_log_buffer_flush_failed",
                code="RUNTIME_EVENT_LOG_BUFFER_FLUSH_FAILED",
                message="runtime_event_log_buffer_flush_failed",
                error=e,
                level=logging.WARNING,
                component="engine.runtime.event_log",
                extra={"pending_rows": int(_event_log_buffer_snapshot().get("buffered_rows") or 0)},
                persist=False,
            )
            _EVENT_LOG_BUFFER_STOP.wait(timeout=_event_log_flush_backoff_s(consecutive_failures))
        finally:
            _finish_event_log_inflight(len(rows))


def _ensure_event_log_writer_started() -> None:
    global _EVENT_LOG_BUFFER_THREAD
    if not _EVENT_LOG_BUFFER_ENABLED:
        return
    with _EVENT_LOG_BUFFER_LOCK:
        if _EVENT_LOG_BUFFER_THREAD is not None and _EVENT_LOG_BUFFER_THREAD.is_alive():
            return
        _EVENT_LOG_BUFFER_STOP.clear()
        _EVENT_LOG_BUFFER_THREAD = threading.Thread(
            target=_event_log_writer_loop,
            name="runtime-event-log-buffer",
            daemon=True,
        )
        _EVENT_LOG_BUFFER_THREAD.start()


def _enqueue_event_log_rows(
    rows: List[tuple[int, str, str, int, str | None, str | None, str | None, str]],
) -> bool:
    if not rows:
        return True
    if not _EVENT_LOG_BUFFER_ENABLED:
        return False
    _ensure_event_log_writer_started()
    now_ms = int(time.time() * 1000)
    with _EVENT_LOG_BUFFER_LOCK:
        room = max(0, int(_EVENT_LOG_BUFFER_MAX_ROWS) - int(len(_EVENT_LOG_BUFFER_PENDING)))
        accepted = list(rows[:room])
        dropped = max(0, len(rows) - len(accepted))
        if accepted:
            _EVENT_LOG_BUFFER_PENDING.extend(accepted)
            _EVENT_LOG_BUFFER_STATE["buffered_rows"] = int(len(_EVENT_LOG_BUFFER_PENDING))
            _EVENT_LOG_BUFFER_STATE["last_enqueue_ts_ms"] = int(now_ms)
            _EVENT_LOG_BUFFER_LOCK.notify_all()
        if dropped > 0:
            _EVENT_LOG_BUFFER_STATE["dropped_rows"] = int(_EVENT_LOG_BUFFER_STATE.get("dropped_rows") or 0) + int(
                dropped
            )
            log_failure(
                LOG,
                event="runtime_event_log_buffer_overflow",
                code="RUNTIME_EVENT_LOG_BUFFER_OVERFLOW",
                message="runtime_event_log_buffer_overflow",
                error=RuntimeError(f"event_log_buffer_overflow:{dropped}"),
                level=logging.WARNING,
                component="engine.runtime.event_log",
                extra={"dropped_rows": int(dropped), "max_rows": int(_EVENT_LOG_BUFFER_MAX_ROWS)},
                persist=False,
            )
    return bool(accepted)


def flush_event_log_buffer(*, max_batches: int = 8, wait_inflight_s: float = 5.0) -> dict[str, Any]:
    flushed = 0
    batches = 0
    deadline_s = time.monotonic() + max(0.0, float(wait_inflight_s))
    while batches < max(1, int(max_batches)):
        rows = _drain_event_log_buffer(track_inflight=True)
        if not rows:
            with _EVENT_LOG_BUFFER_LOCK:
                pending = int(len(_EVENT_LOG_BUFFER_PENDING))
                inflight = int(_EVENT_LOG_BUFFER_STATE.get("inflight_rows") or 0)
                if pending <= 0 and inflight <= 0:
                    break
                remaining_s = float(deadline_s - time.monotonic())
                if remaining_s <= 0.0:
                    break
                _EVENT_LOG_BUFFER_LOCK.wait(timeout=min(0.05, remaining_s))
            continue
        try:
            flushed_now = int(_flush_event_log_rows(rows))
            flushed += int(flushed_now)
            batches += 1
            now_ms = int(time.time() * 1000)
            with _EVENT_LOG_BUFFER_LOCK:
                _EVENT_LOG_BUFFER_STATE["flush_batches"] = int(_EVENT_LOG_BUFFER_STATE.get("flush_batches") or 0) + 1
                _EVENT_LOG_BUFFER_STATE["flushed_rows"] = int(_EVENT_LOG_BUFFER_STATE.get("flushed_rows") or 0) + int(
                    flushed_now
                )
                _EVENT_LOG_BUFFER_STATE["last_flush_ts_ms"] = int(now_ms)
                _EVENT_LOG_BUFFER_STATE["last_error"] = ""
        except Exception as e:
            _requeue_event_log_rows(rows)
            with _EVENT_LOG_BUFFER_LOCK:
                _EVENT_LOG_BUFFER_STATE["last_error"] = f"{type(e).__name__}:{e}"
                _EVENT_LOG_BUFFER_STATE["last_error_ts_ms"] = int(time.time() * 1000)
            raise
        finally:
            _finish_event_log_inflight(len(rows))
    snapshot = _event_log_buffer_snapshot()
    snapshot["flushed"] = int(flushed)
    return snapshot


def shutdown_event_log_buffer(timeout_s: float = 2.0) -> dict[str, Any]:
    global _EVENT_LOG_BUFFER_THREAD
    with _EVENT_LOG_BUFFER_LOCK:
        thread = _EVENT_LOG_BUFFER_THREAD
        _EVENT_LOG_BUFFER_STOP.set()
        _EVENT_LOG_BUFFER_LOCK.notify_all()
    if thread is not None:
        thread.join(timeout=max(0.1, float(timeout_s)))
    flushed_snapshot = {}
    try:
        flushed_snapshot = flush_event_log_buffer(max_batches=64)
    except Exception:
        flushed_snapshot = _event_log_buffer_snapshot()
    with _EVENT_LOG_BUFFER_LOCK:
        _EVENT_LOG_BUFFER_THREAD = None
    return dict(flushed_snapshot or {})


def get_event_log_buffer_snapshot() -> dict[str, Any]:
    return _event_log_buffer_snapshot()


def _flush_event_log_buffer_before_read(*, scope: str) -> None:
    if not _EVENT_LOG_BUFFER_ENABLED:
        return
    if should_defer_noncritical_startup_write():
        return
    try:
        flush_event_log_buffer(max_batches=64)
    except Exception as e:
        log_failure(
            LOG,
            event="runtime_event_log_buffer_pre_read_flush_failed",
            code="RUNTIME_EVENT_LOG_BUFFER_PRE_READ_FLUSH_FAILED",
            message="runtime_event_log_buffer_pre_read_flush_failed",
            error=e,
            level=logging.WARNING,
            component="engine.runtime.event_log",
            extra={"scope": str(scope)},
            persist=False,
        )


def init_event_log(con=None) -> None:
    """Ensure the event-log schema is available through the shared DB bootstrap."""
    # Schema creation is delegated to init_db(); this helper exists so callers
    # can treat the event log as an always-available subsystem.
    if con is not None:
        return
    init_db()


def reindex_event_log_indexes(
    indexes: List[str],
    *,
    operation: str = "reindex_event_log_indexes",
) -> List[str]:
    requested = [str(name).strip() for name in list(indexes or []) if str(name).strip()]
    if not requested:
        return []

    def _write(conw):
        applied: List[str] = []
        for index_name in requested:
            conw.execute(f"REINDEX {index_name}")
            applied.append(str(index_name))
        return applied

    return list(
        run_write_txn(
            _write,
            table="event_log",
            operation=str(operation or "reindex_event_log_indexes"),
            context={"indexes": list(requested)},
        )
        or []
    )


def append_event(
    *,
    event_type: str,
    event_source: str,
    event_version: int = 1,
    entity_type: Optional[str] = None,
    entity_id: Optional[str] = None,
    correlation_id: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
    ts_ms: Optional[int] = None,
    con=None,
    best_effort: bool = False,
) -> Optional[int]:
    """Append one runtime event, optionally reusing the caller's transaction."""
    if con is None:
        init_event_log()

    now = int(ts_ms if ts_ms is not None else _now_ms())
    row = _event_log_row(
        ts_ms=int(now),
        event_type=str(event_type),
        event_source=str(event_source),
        event_version=int(event_version),
        entity_type=entity_type,
        entity_id=entity_id,
        correlation_id=correlation_id,
        payload=payload,
    )

    # Reuse the caller's transaction when available so event rows can commit
    # atomically with the state change they describe.
    if con is not None:
        cur = con.execute(
            """
            INSERT INTO event_log(
              ts_ms, event_type, event_source, event_version,
              entity_type, entity_id, correlation_id, payload_json
            )
            VALUES (?,?,?,?,?,?,?,?)
            """,
            row,
        )
        return int(cur.lastrowid)

    if _enqueue_event_log_rows([row]):
        return None
    if _EVENT_LOG_BUFFER_ENABLED and bool(best_effort):
        return None

    out: Dict[str, Any] = {"event_id": None}

    def _write(conw):
        cur = conw.execute(
            """
            INSERT INTO event_log(
              ts_ms, event_type, event_source, event_version,
              entity_type, entity_id, correlation_id, payload_json
            )
            VALUES (?,?,?,?,?,?,?,?)
            """,
            row,
        )
        out["event_id"] = int(cur.lastrowid)

    # Standalone writes use the centralized write transaction helper so event
    # logging follows the same DB locking and retry policy as the rest of runtime.
    run_write_txn(
        _write,
        table="event_log",
        operation="append_event",
        direct=True,
        maintenance=False,
        attempts=(1 if bool(best_effort) else None),
        timeout_s=(0.25 if bool(best_effort) else None),
        busy_timeout_ms=(250 if bool(best_effort) else None),
    )
    return int(out["event_id"])


def replay_events(
    *,
    after_event_id: int = 0,
    limit: int = 1000,
    event_type: Optional[str] = None,
    entity_type: Optional[str] = None,
    entity_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Replay append-only runtime events with optional filters."""
    init_event_log()
    _flush_event_log_buffer_before_read(scope="replay_events")

    # Replay is append-only by event id, which gives downstream consumers a
    # stable checkpoint mechanism without needing external offsets.
    con = connect(readonly=True)
    try:
        sql = """
        SELECT id, ts_ms, event_type, event_source, event_version,
               entity_type, entity_id, correlation_id, payload_json
        FROM event_log
        WHERE id > ?
        """
        params: List[Any] = [int(after_event_id)]

        if event_type is not None:
            sql += " AND event_type=?"
            params.append(str(event_type))
        if entity_type is not None:
            sql += " AND entity_type=?"
            params.append(str(entity_type))
        if entity_id is not None:
            sql += " AND entity_id=?"
            params.append(str(entity_id))

        sql += " ORDER BY id ASC LIMIT ?"
        params.append(int(limit))

        rows = con.execute(sql, tuple(params)).fetchall()

        out: List[Dict[str, Any]] = []
        for r in rows or []:
            payload = {}
            try:
                payload = json.loads(r[8] or "{}")
                if not isinstance(payload, dict):
                    payload = {"value": payload}
            except Exception:
                payload = {}

            out.append(
                {
                    "id": int(r[0]),
                    "ts_ms": int(r[1]),
                    "event_type": str(r[2]),
                    "event_source": str(r[3]),
                    "event_version": int(r[4] or 1),
                    "entity_type": (str(r[5]) if r[5] is not None else None),
                    "entity_id": (str(r[6]) if r[6] is not None else None),
                    "correlation_id": (str(r[7]) if r[7] is not None else None),
                    "payload": payload,
                }
            )
        return out
    finally:
        con.close()


def record_state_transition(
    *,
    namespace: str,
    state_key: str,
    state_value: str,
    payload: Optional[Dict[str, Any]] = None,
    event_type: str = "state_transition",
    event_source: str = "runtime.state",
    entity_type: Optional[str] = None,
    entity_id: Optional[str] = None,
    correlation_id: Optional[str] = None,
    ts_ms: Optional[int] = None,
    con=None,
) -> Optional[int]:
    """Record a deduplicated state transition plus the latest state snapshot."""
    if con is None:
        init_event_log()

    now = int(ts_ms if ts_ms is not None else _now_ms())

    # State transitions are deduped at the current-state table so callers can
    # safely publish idempotent updates without inflating the event stream.
    if con is not None:
        row = con.execute(
            """
            SELECT state_value
            FROM event_log_state
            WHERE namespace=? AND state_key=?
            """,
            (str(namespace), str(state_key)),
        ).fetchone()

        prev = None
        try:
            prev = None if row is None else row[0]
        except Exception:
            prev = None

        if (prev is not None) and str(prev) == str(state_value):
            return None

        state_payload = dict(payload or {})
        state_payload.setdefault("namespace", str(namespace))
        state_payload.setdefault("state_key", str(state_key))
        state_payload.setdefault("previous_state", (str(prev) if prev is not None else None))
        state_payload.setdefault("state", str(state_value))

        eid = append_event(
            event_type=str(event_type),
            event_source=str(event_source),
            entity_type=(str(entity_type) if entity_type is not None else str(namespace)),
            entity_id=(str(entity_id) if entity_id is not None else str(state_key)),
            correlation_id=(str(correlation_id) if correlation_id is not None else str(state_key)),
            payload=state_payload,
            ts_ms=int(now),
            con=con,
        )

        con.execute(
            """
            INSERT INTO event_log_state(namespace, state_key, state_value, updated_ts_ms, payload_json)
            VALUES (?,?,?,?,?)
            ON CONFLICT(namespace, state_key) DO UPDATE SET
              state_value=excluded.state_value,
              updated_ts_ms=excluded.updated_ts_ms,
              payload_json=excluded.payload_json
            """,
            (
                str(namespace),
                str(state_key),
                str(state_value),
                int(now),
                _dump_json(state_payload),
            ),
        )

        return eid

    out: Dict[str, Any] = {"event_id": None}

    def _write(conw):
        row = conw.execute(
            """
            SELECT state_value
            FROM event_log_state
            WHERE namespace=? AND state_key=?
            """,
            (str(namespace), str(state_key)),
        ).fetchone()

        prev = None
        try:
            prev = None if row is None else row[0]
        except Exception:
            prev = None

        if (prev is not None) and str(prev) == str(state_value):
            out["event_id"] = None
            return

        state_payload = dict(payload or {})
        state_payload.setdefault("namespace", str(namespace))
        state_payload.setdefault("state_key", str(state_key))
        state_payload.setdefault("previous_state", (str(prev) if prev is not None else None))
        state_payload.setdefault("state", str(state_value))

        out["event_id"] = append_event(
            event_type=str(event_type),
            event_source=str(event_source),
            entity_type=(str(entity_type) if entity_type is not None else str(namespace)),
            entity_id=(str(entity_id) if entity_id is not None else str(state_key)),
            correlation_id=(str(correlation_id) if correlation_id is not None else str(state_key)),
            payload=state_payload,
            ts_ms=int(now),
            con=conw,
        )

        conw.execute(
            """
            INSERT INTO event_log_state(namespace, state_key, state_value, updated_ts_ms, payload_json)
            VALUES (?,?,?,?,?)
            ON CONFLICT(namespace, state_key) DO UPDATE SET
              state_value=excluded.state_value,
              updated_ts_ms=excluded.updated_ts_ms,
              payload_json=excluded.payload_json
            """,
            (
                str(namespace),
                str(state_key),
                str(state_value),
                int(now),
                _dump_json(state_payload),
            ),
        )

    run_write_txn(_write)
    return out["event_id"]


def record_regime_change(
    *,
    symbol: str,
    regime: str,
    vol: Optional[float] = None,
    ts_ms: Optional[int] = None,
    con=None,
) -> Optional[int]:
    """Record a symbol-level regime change in the runtime event log."""
    payload: Dict[str, Any] = {
        "symbol": str(symbol),
        "regime": str(regime),
    }
    if vol is not None:
        try:
            payload["vol"] = float(vol)
        except Exception as e:
            try:
                from engine.runtime.failure_diagnostics import log_failure
                from engine.runtime.logging import get_logger

                log_failure(
                    get_logger("engine.runtime.event_log"),
                    event="event_log_vol_parse_failed",
                    code="EVENT_LOG_VOL_PARSE_FAILED",
                    message="event_log_vol_parse_failed",
                    error=e,
                    level=30,
                    component="engine.runtime.event_log",
                    extra={"symbol": str(symbol), "regime": str(regime)},
                    persist=False,
                )
            except Exception:
                raise

    return record_state_transition(
        namespace="regime",
        state_key=str(symbol),
        state_value=str(regime),
        payload=payload,
        event_type="regime_change",
        event_source="engine.strategy.model_v2",
        entity_type="symbol",
        entity_id=str(symbol),
        correlation_id=str(symbol),
        ts_ms=ts_ms,
        con=con,
    )


def record_risk_block(
    *,
    name: str,
    blocked: bool,
    info: Optional[Dict[str, Any]] = None,
    ts_ms: Optional[int] = None,
    con=None,
) -> Optional[int]:
    """Record one risk-block state transition."""
    payload = dict(info or {})
    payload["blocked"] = bool(blocked)

    return record_state_transition(
        namespace="risk_block",
        state_key=str(name),
        state_value=("blocked" if bool(blocked) else "clear"),
        payload=payload,
        event_type="risk_block",
        event_source="engine.strategy.portfolio_risk_engine",
        entity_type="risk_layer",
        entity_id=str(name),
        correlation_id=str(name),
        ts_ms=ts_ms,
        con=con,
    )


def record_allocator_decision(
    *,
    ts_ms: int,
    allocations: Dict[str, Any],
    details: Optional[Dict[str, Any]] = None,
    reason: Optional[Dict[str, Any]] = None,
    con=None,
) -> Optional[int]:
    """Record one allocator decision payload for audit and replay."""
    return append_event(
        event_type="allocator_decision",
        event_source="engine.runtime.strategy_allocator",
        event_version=1,
        entity_type="allocator",
        entity_id="strategy_allocator",
        correlation_id=str(int(ts_ms)),
        payload={
            "ts_ms": int(ts_ms),
            "allocations": dict(allocations or {}),
            "details": dict(details or {}),
            "reason": dict(reason or {}),
        },
        ts_ms=int(ts_ms),
        con=con,
    )


def record_order_decision(
    *,
    ts_ms: int,
    batch_id: Optional[int],
    payload_source: str,
    raw_payload: List[Dict[str, Any]],
    shaped_payload: List[Dict[str, Any]],
    mode: str,
    broker: str,
    con=None,
) -> Optional[int]:
    """Record the raw and shaped order-decision payloads for one batch."""
    correlation_id = str(int(batch_id)) if batch_id is not None else str(int(ts_ms))
    return append_event(
        event_type="order_decision",
        event_source="engine.execution.broker_apply_orders",
        event_version=1,
        entity_type="order_batch",
        entity_id=correlation_id,
        correlation_id=correlation_id,
        payload={
            "ts_ms": int(ts_ms),
            "batch_id": (int(batch_id) if batch_id is not None else None),
            "payload_source": str(payload_source),
            "mode": str(mode),
            "broker": str(broker),
            "raw_count": int(len(raw_payload or [])),
            "shaped_count": int(len(shaped_payload or [])),
            "raw_payload": list(raw_payload or []),
            "shaped_payload": list(shaped_payload or []),
        },
        ts_ms=int(ts_ms),
        con=con,
    )


def record_execution_block(
    *,
    ts_ms: int,
    layer: str,
    reason: str,
    mode: str,
    broker: str,
    payload: Optional[Dict[str, Any]] = None,
    correlation_id: Optional[str] = None,
    con=None,
) -> Optional[int]:
    """Record one execution gate or block decision."""
    return append_event(
        event_type="execution_block",
        event_source="engine.execution.broker_apply_orders",
        event_version=1,
        entity_type="execution_gate",
        entity_id=str(layer),
        correlation_id=(str(correlation_id) if correlation_id is not None else str(int(ts_ms))),
        payload={
            "ts_ms": int(ts_ms),
            "layer": str(layer),
            "reason": str(reason),
            "mode": str(mode),
            "broker": str(broker),
            **dict(payload or {}),
        },
        ts_ms=int(ts_ms),
        con=con,
    )


def record_lifecycle_event(
    *,
    event_type: str,
    state: str,
    detail: str = "",
    actor: str = "system",
    ts_ms: Optional[int] = None,
    con=None,
) -> Optional[int]:
    """Record one lifecycle event for startup, degradation, or shutdown traces."""
    now = int(ts_ms if ts_ms is not None else _now_ms())
    return append_event(
        event_type=str(event_type),
        event_source="engine.runtime.lifecycle",
        event_version=1,
        entity_type="lifecycle",
        entity_id=str(state),
        correlation_id=str(now),
        payload={
            "ts_ms": int(now),
            "state": str(state),
            "detail": str(detail or ""),
            "actor": str(actor or "system"),
        },
        ts_ms=int(now),
        con=con,
    )
