"""Buffered append-only telemetry writers for hot operational tables."""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Sequence

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.ingestion_tuning import env_bool, tuned_float, tuned_int
from engine.runtime.metrics import emit_counter, emit_gauge, emit_timing
from engine.runtime.startup_write_gate import (
    noncritical_startup_write_wait_s,
    should_defer_noncritical_startup_write,
)
from engine.runtime.storage import connect_ro, run_write_txn

LOG = logging.getLogger("engine.runtime.telemetry_append_buffer")

_BUFFER_ENABLED = env_bool("TELEMETRY_APPEND_BUFFER_ENABLED", default=True)
_BUFFER_FLUSH_INTERVAL_S = tuned_float("TELEMETRY_APPEND_BUFFER_FLUSH_INTERVAL_S", 0.5, 0.05, 5.0)
_BUFFER_FLUSH_JITTER_RATIO = tuned_float("TELEMETRY_APPEND_BUFFER_FLUSH_JITTER_RATIO", 0.25, 0.0, 1.0)
_BUFFER_MAX_BATCH = tuned_int("TELEMETRY_APPEND_BUFFER_MAX_BATCH", 128, 1, 4096)
_BUFFER_MAX_ROWS = max(
    _BUFFER_MAX_BATCH,
    tuned_int("TELEMETRY_APPEND_BUFFER_MAX_ROWS", 4096, 1, 65536),
)

_TABLE_SPECS: dict[str, dict[str, str]] = {
    "price_quotes_raw": {
        "sql": """
        INSERT INTO price_quotes_raw(
          ts_ms, symbol, provider, event_key, event_type, event_ts_ms,
          last, bid, ask, spread, volume,
          trade_ts_ms, quote_ts_ms, ingest_ts_ms, source
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol, provider, event_key) DO UPDATE SET
          ts_ms=excluded.ts_ms,
          event_type=excluded.event_type,
          event_ts_ms=excluded.event_ts_ms,
          last=excluded.last,
          bid=excluded.bid,
          ask=excluded.ask,
          spread=excluded.spread,
          volume=excluded.volume,
          trade_ts_ms=excluded.trade_ts_ms,
          quote_ts_ms=excluded.quote_ts_ms,
          ingest_ts_ms=excluded.ingest_ts_ms,
          source=excluded.source
        """,
        "operation": "flush_price_quotes_raw_buffer",
    },
    "price_provider_health": {
        "sql": """
        INSERT INTO price_provider_health(
          ts_ms, provider, ok, latency_ms, n_symbols, error, last_success_ts_ms, error_count
        )
        VALUES (?,?,?,?,?,?,?,?)
        ON CONFLICT(provider, ts_ms) DO UPDATE SET
          ok=excluded.ok,
          latency_ms=excluded.latency_ms,
          n_symbols=excluded.n_symbols,
          error=excluded.error,
          last_success_ts_ms=excluded.last_success_ts_ms,
          error_count=excluded.error_count
        """,
        "operation": "flush_price_provider_health_buffer",
    },
    "weather_provider_health": {
        "sql": """
        INSERT INTO weather_provider_health(ts_ms, provider, ok, latency_ms, error)
        VALUES (?,?,?,?,?)
        """,
        "operation": "flush_weather_provider_health_buffer",
    },
    "ingestion_pipeline_health": {
        "sql": """
        INSERT INTO ingestion_pipeline_health(
          ts_ms, pipeline, ok, latency_ms, raw_rows, event_rows,
          last_ingested_ts_ms, error, meta_json
        )
        VALUES (?,?,?,?,?,?,?,?,?)
        """,
        "operation": "flush_ingestion_pipeline_health_buffer",
    },
    "ingest_slippage": {
        "sql": """
        INSERT INTO ingest_slippage(
          ts_ms, symbol, provider,
          last, bid, ask, mid, spread,
          px_minus_mid, abs_px_minus_mid
        )
        VALUES (?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol, provider, ts_ms) DO UPDATE SET
          last=excluded.last,
          bid=excluded.bid,
          ask=excluded.ask,
          mid=excluded.mid,
          spread=excluded.spread,
          px_minus_mid=excluded.px_minus_mid,
          abs_px_minus_mid=excluded.abs_px_minus_mid
        """,
        "operation": "flush_ingest_slippage_buffer",
    },
}


def _uses_postgres_storage(con: Any) -> bool:
    raw = getattr(con, "raw", None)
    con_module = str(type(con).__module__ or "").lower()
    raw_module = str(type(raw).__module__ or "").lower()
    return con_module == "engine.runtime.storage_pg" or raw_module.startswith("psycopg")


def _sql_for_table(table: str, con: Any) -> str:
    spec = dict(_TABLE_SPECS.get(str(table)) or {})
    sql = str(spec.get("sql") or "").strip()
    if str(table) == "price_quotes_raw" and _uses_postgres_storage(con):
        sql = sql.replace(
            "ON CONFLICT(symbol, provider, event_key) DO UPDATE SET",
            "ON CONFLICT(symbol, provider, event_key, ts_ms) DO UPDATE SET",
        )
    return sql
_TABLE_ORDER: tuple[str, ...] = tuple(_TABLE_SPECS.keys())
_PREVIOUS_BUFFER_LOCK = globals().get("_BUFFER_LOCK")
_PREVIOUS_BUFFER_STOP = globals().get("_BUFFER_STOP")
_PREVIOUS_BUFFER_THREAD = globals().get("_BUFFER_THREAD")
if _PREVIOUS_BUFFER_STOP is not None:
    try:
        _PREVIOUS_BUFFER_STOP.set()
    except Exception:
        LOG.debug("telemetry_append_buffer_reload_stop_failed", exc_info=True)
if _PREVIOUS_BUFFER_LOCK is not None:
    try:
        with _PREVIOUS_BUFFER_LOCK:
            _PREVIOUS_BUFFER_LOCK.notify_all()
    except Exception:
        LOG.debug("telemetry_append_buffer_reload_notify_failed", exc_info=True)
if (
    _PREVIOUS_BUFFER_THREAD is not None
    and getattr(_PREVIOUS_BUFFER_THREAD, "is_alive", lambda: False)()
    and _PREVIOUS_BUFFER_THREAD is not threading.current_thread()
):
    try:
        _PREVIOUS_BUFFER_THREAD.join(timeout=1.0)
    except Exception:
        LOG.debug("telemetry_append_buffer_reload_join_failed", exc_info=True)
_BUFFER_LOCK = threading.Condition()
_BUFFER_PENDING: dict[str, list[tuple[Any, ...]]] = {name: [] for name in _TABLE_ORDER}
_BUFFER_STOP = threading.Event()
_BUFFER_THREAD: threading.Thread | None = None


def _empty_table_counters() -> dict[str, int]:
    return {name: 0 for name in _TABLE_ORDER}


_BUFFER_STATE: dict[str, Any] = {
    "accepted_rows": 0,
    "buffered_rows": 0,
    "dropped_rows": 0,
    "flush_batches": 0,
    "flush_failures": 0,
    "flushed_rows": 0,
    "retry_count": 0,
    "last_flush_latency_ms": 0,
    "total_flush_latency_ms": 0,
    "last_db_write_duration_ms": 0,
    "total_db_write_duration_ms": 0,
    "last_enqueue_ts_ms": 0,
    "last_flush_ts_ms": 0,
    "last_error": "",
    "last_error_ts_ms": 0,
    "last_rejected_reason": "",
    "last_rejected_table": "",
    "last_rejected_ts_ms": 0,
    "accepted_by_table": _empty_table_counters(),
    "dropped_by_table": _empty_table_counters(),
    "flushed_by_table": _empty_table_counters(),
}
_PRICE_PROVIDER_STATE_LOCK = threading.Lock()
_PRICE_PROVIDER_STATE: dict[str, dict[str, int]] = {}
_BUFFER_DB_PATH_KEY = str(os.environ.get("DB_PATH") or "").strip()


def _staggered_flush_interval_s(base_interval_s: float, jitter_ratio: float) -> float:
    base = max(0.05, float(base_interval_s))
    jitter = min(1.0, max(0.0, float(jitter_ratio)))
    if jitter <= 0.0:
        return float(base)
    bucket = max(0, int(os.getpid()) % 17)
    return float(base * (1.0 + ((float(bucket) / 16.0) * jitter)))


def _telemetry_append_flush_backoff_s(consecutive_failures: int) -> float:
    failures = max(1, min(int(consecutive_failures), 5))
    base_interval_s = max(1.0, float(_BUFFER_EFFECTIVE_FLUSH_INTERVAL_S))
    return min(10.0, float(base_interval_s * (2 ** (failures - 1))))


def _current_buffer_db_path_key() -> str:
    return str(os.environ.get("DB_PATH") or "").strip()


def _reset_buffer_for_current_db_path_if_needed() -> None:
    global _BUFFER_DB_PATH_KEY
    current = _current_buffer_db_path_key()
    if current == _BUFFER_DB_PATH_KEY:
        return
    with _BUFFER_LOCK:
        for table in _TABLE_ORDER:
            _BUFFER_PENDING[table] = []
        _BUFFER_STATE["buffered_rows"] = 0
        _BUFFER_STATE["last_error"] = ""
        _BUFFER_STATE["last_error_ts_ms"] = 0
        _BUFFER_DB_PATH_KEY = current
        _BUFFER_LOCK.notify_all()
    with _PRICE_PROVIDER_STATE_LOCK:
        _PRICE_PROVIDER_STATE.clear()


_BUFFER_EFFECTIVE_FLUSH_INTERVAL_S = _staggered_flush_interval_s(
    _BUFFER_FLUSH_INTERVAL_S,
    _BUFFER_FLUSH_JITTER_RATIO,
)


def _warn_nonfatal(code: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.runtime.telemetry_append_buffer",
        extra=dict(extra or {}) or None,
        persist=False,
    )


def _buffered_row_count_locked() -> int:
    return sum(int(len(rows)) for rows in _BUFFER_PENDING.values())


def _increment_table_counter_locked(field: str, table: str, amount: int) -> None:
    counters = dict(_BUFFER_STATE.get(str(field)) or {})
    counters[str(table)] = int(counters.get(str(table)) or 0) + int(amount)
    _BUFFER_STATE[str(field)] = counters


def _set_last_rejected_locked(*, table: str, reason: str, ts_ms: int) -> None:
    _BUFFER_STATE["last_rejected_table"] = str(table)
    _BUFFER_STATE["last_rejected_reason"] = str(reason)
    _BUFFER_STATE["last_rejected_ts_ms"] = int(ts_ms)


def _record_flush_success_locked(
    *,
    table: str,
    flushed_rows: int,
    ts_ms: int,
    flush_latency_ms: float | int | None = None,
    db_write_duration_ms: float | int | None = None,
) -> None:
    _BUFFER_STATE["flush_batches"] = int(_BUFFER_STATE.get("flush_batches") or 0) + 1
    _BUFFER_STATE["flushed_rows"] = int(_BUFFER_STATE.get("flushed_rows") or 0) + int(flushed_rows)
    _BUFFER_STATE["last_flush_ts_ms"] = int(ts_ms)
    _BUFFER_STATE["last_error"] = ""
    if flush_latency_ms is not None:
        latency_i = int(round(float(flush_latency_ms)))
        _BUFFER_STATE["last_flush_latency_ms"] = latency_i
        _BUFFER_STATE["total_flush_latency_ms"] = int(_BUFFER_STATE.get("total_flush_latency_ms") or 0) + latency_i
    if db_write_duration_ms is not None:
        db_i = int(round(float(db_write_duration_ms)))
        _BUFFER_STATE["last_db_write_duration_ms"] = db_i
        _BUFFER_STATE["total_db_write_duration_ms"] = int(_BUFFER_STATE.get("total_db_write_duration_ms") or 0) + db_i
    _increment_table_counter_locked("flushed_by_table", str(table), int(flushed_rows))


def _snapshot_locked() -> dict[str, Any]:
    state = dict(_BUFFER_STATE)
    state["accepted_by_table"] = dict(_BUFFER_STATE.get("accepted_by_table") or {})
    state["dropped_by_table"] = dict(_BUFFER_STATE.get("dropped_by_table") or {})
    state["flushed_by_table"] = dict(_BUFFER_STATE.get("flushed_by_table") or {})
    state["enabled"] = bool(_BUFFER_ENABLED)
    state["thread_alive"] = bool(_BUFFER_THREAD is not None and _BUFFER_THREAD.is_alive())
    state["queue_depth"] = int(state.get("buffered_rows") or 0)
    state["buffer_max_rows"] = int(_BUFFER_MAX_ROWS)
    state["batch_size"] = int(_BUFFER_MAX_BATCH)
    state["flush_interval_s"] = float(_BUFFER_EFFECTIVE_FLUSH_INTERVAL_S)
    state["flush_interval_base_s"] = float(_BUFFER_FLUSH_INTERVAL_S)
    state["flush_jitter_ratio"] = float(_BUFFER_FLUSH_JITTER_RATIO)
    state["db_path_key"] = str(_BUFFER_DB_PATH_KEY)
    state["pending_by_table"] = {
        name: int(len(_BUFFER_PENDING.get(name) or []))
        for name in _TABLE_ORDER
    }
    return state


def get_telemetry_append_buffer_snapshot() -> dict[str, Any]:
    with _BUFFER_LOCK:
        return _snapshot_locked()


def _write_rows(
    table: str,
    rows: Sequence[tuple[Any, ...]],
    *,
    attempts: int | None,
    timeout_s: float | None,
    busy_timeout_ms: int | None,
) -> int:
    if not rows:
        return 0
    spec = dict(_TABLE_SPECS.get(str(table)) or {})
    if not str(spec.get("sql") or "").strip():
        raise ValueError(f"unsupported_telemetry_table:{table}")

    def _write(con) -> None:
        sql = _sql_for_table(str(table), con)
        if not sql:
            raise ValueError(f"unsupported_telemetry_table:{table}")
        if str(table) == "price_quotes_raw":
            from engine.runtime.storage import _ensure_price_quotes_raw_schema

            _ensure_price_quotes_raw_schema(con)
        con.executemany(sql, list(rows))

    run_write_txn(
        _write,
        attempts=attempts,
        table=str(table),
        operation=str(spec.get("operation") or f"flush_{table}_buffer"),
        direct=True,
        maintenance=False,
        timeout_s=timeout_s,
        busy_timeout_ms=busy_timeout_ms,
    )
    return int(len(rows))


def _flush_rows(table: str, rows: Sequence[tuple[Any, ...]]) -> int:
    return int(
        _write_rows(
            str(table),
            rows,
            attempts=1,
            timeout_s=0.25,
            busy_timeout_ms=250,
        )
    )


def _append_rows_direct(table: str, rows: Sequence[tuple[Any, ...]]) -> int:
    return int(
        _append_rows_direct_with_policy(
            str(table),
            rows,
            attempts=None,
            timeout_s=None,
            busy_timeout_ms=None,
        )
    )


def _append_rows_direct_with_policy(
    table: str,
    rows: Sequence[tuple[Any, ...]],
    *,
    attempts: int | None,
    timeout_s: float | None,
    busy_timeout_ms: int | None,
) -> int:
    if not rows:
        return 0
    started = time.perf_counter()
    flushed = int(
        _write_rows(
            str(table),
            rows,
            attempts=attempts,
            timeout_s=timeout_s,
            busy_timeout_ms=busy_timeout_ms,
        )
    )
    latency_ms = float((time.perf_counter() - started) * 1000.0)
    now_ms = int(time.time() * 1000)
    with _BUFFER_LOCK:
        _record_flush_success_locked(
            table=str(table),
            flushed_rows=int(flushed),
            ts_ms=int(now_ms),
            flush_latency_ms=float(latency_ms),
            db_write_duration_ms=float(latency_ms),
        )
    emit_timing(
        "telemetry_append_buffer_flush_latency_ms",
        float(latency_ms),
        component="engine.runtime.telemetry_append_buffer",
        extra_tags={"table": str(table), "path": "direct"},
    )
    emit_timing(
        "telemetry_append_buffer_db_write_duration_ms",
        float(latency_ms),
        component="engine.runtime.telemetry_append_buffer",
        extra_tags={"table": str(table), "path": "direct"},
    )
    return int(flushed)


def _drain_rows_locked(
    *,
    max_rows: int,
    tables: Sequence[str] | None = None,
) -> tuple[str | None, list[tuple[Any, ...]]]:
    selected = [str(name) for name in (tables or _TABLE_ORDER) if str(name) in _TABLE_SPECS]
    if not selected:
        selected = list(_TABLE_ORDER)
    for table in selected:
        pending = _BUFFER_PENDING.get(table) or []
        if not pending:
            continue
        rows = list(pending[:max_rows])
        del pending[:max_rows]
        _BUFFER_STATE["buffered_rows"] = int(_buffered_row_count_locked())
        return str(table), rows
    return None, []


def _requeue_rows(table: str, rows: Sequence[tuple[Any, ...]]) -> None:
    if not rows:
        return
    dropped = 0
    with _BUFFER_LOCK:
        room = max(0, int(_BUFFER_MAX_ROWS) - int(_buffered_row_count_locked()))
        kept = list(rows[:room])
        dropped = max(0, len(rows) - len(kept))
        pending = _BUFFER_PENDING.setdefault(str(table), [])
        if kept:
            pending[:0] = kept
        _BUFFER_STATE["buffered_rows"] = int(_buffered_row_count_locked())
        if dropped > 0:
            _BUFFER_STATE["dropped_rows"] = int(_BUFFER_STATE.get("dropped_rows") or 0) + int(dropped)
            _increment_table_counter_locked("dropped_by_table", str(table), int(dropped))
        _BUFFER_LOCK.notify_all()
    if dropped > 0:
        emit_counter(
            "telemetry_append_buffer_dropped_rows",
            int(dropped),
            component="engine.runtime.telemetry_append_buffer",
            extra_tags={"table": str(table), "reason": "requeue_overflow"},
        )


def _buffer_writer_loop() -> None:
    consecutive_failures = 0
    pending_since_s = 0.0
    while True:
        _reset_buffer_for_current_db_path_if_needed()
        with _BUFFER_LOCK:
            while True:
                if _buffered_row_count_locked() <= 0 and not _BUFFER_STOP.is_set():
                    pending_since_s = 0.0
                    _BUFFER_LOCK.wait(timeout=float(_BUFFER_EFFECTIVE_FLUSH_INTERVAL_S))
                if _BUFFER_STOP.is_set() and _buffered_row_count_locked() <= 0:
                    return
                if _buffered_row_count_locked() > 0 and not _BUFFER_STOP.is_set():
                    if should_defer_noncritical_startup_write():
                        break
                    # Hold short-lived telemetry bursts until the configured
                    # interval elapses so control-plane writers batch together.
                    now_s = float(time.monotonic())
                    if pending_since_s <= 0.0:
                        pending_since_s = float(now_s)
                    wait_s = float(_BUFFER_EFFECTIVE_FLUSH_INTERVAL_S) - (
                        float(now_s) - float(pending_since_s)
                    )
                    if wait_s > 0.0:
                        _BUFFER_LOCK.wait(timeout=max(0.01, float(wait_s)))
                        continue
                break
        if should_defer_noncritical_startup_write():
            _BUFFER_STOP.wait(timeout=float(noncritical_startup_write_wait_s()))
            continue
        with _BUFFER_LOCK:
            table, rows = _drain_rows_locked(max_rows=int(_BUFFER_MAX_BATCH))
        if not table or not rows:
            continue
        try:
            flush_started = time.perf_counter()
            flushed = _flush_rows(str(table), rows)
            latency_ms = float((time.perf_counter() - flush_started) * 1000.0)
            consecutive_failures = 0
            now_ms = int(time.time() * 1000)
            with _BUFFER_LOCK:
                _record_flush_success_locked(
                    table=str(table),
                    flushed_rows=int(flushed),
                    ts_ms=int(now_ms),
                    flush_latency_ms=float(latency_ms),
                    db_write_duration_ms=float(latency_ms),
                )
            emit_timing(
                "telemetry_append_buffer_flush_latency_ms",
                float(latency_ms),
                component="engine.runtime.telemetry_append_buffer",
                extra_tags={"table": str(table), "path": "background"},
            )
            emit_timing(
                "telemetry_append_buffer_db_write_duration_ms",
                float(latency_ms),
                component="engine.runtime.telemetry_append_buffer",
                extra_tags={"table": str(table), "path": "background"},
            )
            emit_gauge(
                "telemetry_append_buffer_queue_depth",
                int(get_telemetry_append_buffer_snapshot().get("buffered_rows") or 0),
                component="engine.runtime.telemetry_append_buffer",
            )
        except Exception as e:
            consecutive_failures = min(consecutive_failures + 1, 5)
            _requeue_rows(str(table), rows)
            with _BUFFER_LOCK:
                _BUFFER_STATE["flush_failures"] = int(_BUFFER_STATE.get("flush_failures") or 0) + 1
                _BUFFER_STATE["retry_count"] = int(_BUFFER_STATE.get("retry_count") or 0) + 1
                _BUFFER_STATE["last_error"] = f"{type(e).__name__}:{e}"
                _BUFFER_STATE["last_error_ts_ms"] = int(time.time() * 1000)
            emit_counter(
                "telemetry_append_buffer_retries",
                1,
                component="engine.runtime.telemetry_append_buffer",
                extra_tags={"table": str(table), "failure_count": int(consecutive_failures)},
            )
            _warn_nonfatal(
                "TELEMETRY_APPEND_BUFFER_FLUSH_FAILED",
                e,
                table=str(table),
                pending_rows=int(get_telemetry_append_buffer_snapshot().get("buffered_rows") or 0),
            )
            _BUFFER_STOP.wait(timeout=_telemetry_append_flush_backoff_s(consecutive_failures))


def _ensure_buffer_thread_started() -> None:
    global _BUFFER_THREAD
    if not _BUFFER_ENABLED:
        return
    _reset_buffer_for_current_db_path_if_needed()
    with _BUFFER_LOCK:
        if _BUFFER_THREAD is not None and _BUFFER_THREAD.is_alive():
            return
        _BUFFER_STOP.clear()
        _BUFFER_THREAD = threading.Thread(
            target=_buffer_writer_loop,
            name="runtime-telemetry-append-buffer",
            daemon=True,
        )
        _BUFFER_THREAD.start()


def _enqueue_rows(table: str, rows: Sequence[tuple[Any, ...]]) -> bool:
    if not rows:
        return True
    _reset_buffer_for_current_db_path_if_needed()
    table_name = str(table)
    if table_name not in _TABLE_SPECS:
        now_ms = int(time.time() * 1000)
        with _BUFFER_LOCK:
            _set_last_rejected_locked(
                table=table_name,
                reason="unsupported_table",
                ts_ms=int(now_ms),
            )
        return False
    if not _BUFFER_ENABLED:
        now_ms = int(time.time() * 1000)
        with _BUFFER_LOCK:
            _set_last_rejected_locked(
                table=table_name,
                reason="buffer_disabled",
                ts_ms=int(now_ms),
            )
        return False
    _ensure_buffer_thread_started()
    now_ms = int(time.time() * 1000)
    accepted_count = 0
    dropped_count = 0
    with _BUFFER_LOCK:
        room = max(0, int(_BUFFER_MAX_ROWS) - int(_buffered_row_count_locked()))
        accepted = list(rows[:room])
        dropped = max(0, len(rows) - len(accepted))
        accepted_count = int(len(accepted))
        dropped_count = int(dropped)
        if accepted:
            _BUFFER_PENDING.setdefault(table_name, []).extend(accepted)
            _BUFFER_STATE["accepted_rows"] = int(_BUFFER_STATE.get("accepted_rows") or 0) + int(len(accepted))
            _increment_table_counter_locked("accepted_by_table", table_name, int(len(accepted)))
            _BUFFER_STATE["buffered_rows"] = int(_buffered_row_count_locked())
            _BUFFER_STATE["last_enqueue_ts_ms"] = int(now_ms)
            _BUFFER_LOCK.notify_all()
        if dropped > 0:
            _BUFFER_STATE["dropped_rows"] = int(_BUFFER_STATE.get("dropped_rows") or 0) + int(dropped)
            _increment_table_counter_locked("dropped_by_table", table_name, int(dropped))
            _set_last_rejected_locked(
                table=table_name,
                reason="buffer_overflow",
                ts_ms=int(now_ms),
            )
            _warn_nonfatal(
                "TELEMETRY_APPEND_BUFFER_OVERFLOW",
                RuntimeError(f"telemetry_append_buffer_overflow:{dropped}"),
                table=table_name,
                dropped_rows=int(dropped),
                max_rows=int(_BUFFER_MAX_ROWS),
            )
        elif not accepted:
            _set_last_rejected_locked(
                table=table_name,
                reason="buffer_full",
                ts_ms=int(now_ms),
            )
    if accepted_count > 0:
        emit_gauge(
            "telemetry_append_buffer_queue_depth",
            int(get_telemetry_append_buffer_snapshot().get("buffered_rows") or 0),
            component="engine.runtime.telemetry_append_buffer",
            extra_tags={"table": table_name},
        )
    if dropped_count > 0:
        emit_counter(
            "telemetry_append_buffer_dropped_rows",
            int(dropped_count),
            component="engine.runtime.telemetry_append_buffer",
            extra_tags={"table": table_name, "reason": "buffer_overflow"},
        )
    return bool(accepted)


def _read_price_provider_state_from_db(provider: str) -> dict[str, int]:
    con = None
    try:
        con = connect_ro()
        row = con.execute(
            """
            SELECT last_success_ts_ms, error_count
            FROM price_provider_health
            WHERE provider = ?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (str(provider),),
        ).fetchone()
    except Exception as e:
        _warn_nonfatal(
            "TELEMETRY_APPEND_BUFFER_PRICE_PROVIDER_STATE_READ_FAILED",
            e,
            provider=str(provider),
        )
        row = None
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn_nonfatal(
                "TELEMETRY_APPEND_BUFFER_PRICE_PROVIDER_STATE_CLOSE_FAILED",
                e,
                provider=str(provider),
            )
    return {
        "last_success_ts_ms": int((row or [0, 0])[0] or 0),
        "error_count": int((row or [0, 0])[1] or 0),
    }


def enqueue_price_provider_health(
    *,
    provider: str,
    ok: bool,
    latency_ms: int | None,
    n_symbols: int,
    error: str | None = None,
    ts_ms: int | None = None,
) -> bool:
    provider_name = str(provider or "").strip()
    if not provider_name:
        return False
    now_ms = int(ts_ms or (time.time() * 1000))
    with _PRICE_PROVIDER_STATE_LOCK:
        state = dict(_PRICE_PROVIDER_STATE.get(provider_name) or {})
        current = state or _read_price_provider_state_from_db(provider_name)
        last_success_ts_ms = int(now_ms) if bool(ok) else int(current.get("last_success_ts_ms") or 0)
        error_count = int(current.get("error_count") or 0) if bool(ok) else int(current.get("error_count") or 0) + 1
        row = (
            int(now_ms),
            provider_name,
            1 if bool(ok) else 0,
            (int(latency_ms) if latency_ms is not None else None),
            int(n_symbols or 0),
            (str(error) if error else None),
            int(last_success_ts_ms),
            int(error_count),
        )
        accepted = _enqueue_rows("price_provider_health", [row])
        if accepted:
            _PRICE_PROVIDER_STATE[provider_name] = {
                "last_success_ts_ms": int(last_success_ts_ms),
                "error_count": int(error_count),
            }
        return bool(accepted)


def append_price_provider_health(
    *,
    provider: str,
    ok: bool,
    latency_ms: int | None,
    n_symbols: int,
    error: str | None = None,
    ts_ms: int | None = None,
) -> bool:
    provider_name = str(provider or "").strip()
    if not provider_name:
        return False
    now_ms = int(ts_ms or (time.time() * 1000))
    with _PRICE_PROVIDER_STATE_LOCK:
        state = dict(_PRICE_PROVIDER_STATE.get(provider_name) or {})
        current = state or _read_price_provider_state_from_db(provider_name)
        last_success_ts_ms = int(now_ms) if bool(ok) else int(current.get("last_success_ts_ms") or 0)
        error_count = int(current.get("error_count") or 0) if bool(ok) else int(current.get("error_count") or 0) + 1
        row = (
            int(now_ms),
            provider_name,
            1 if bool(ok) else 0,
            (int(latency_ms) if latency_ms is not None else None),
            int(n_symbols or 0),
            (str(error) if error else None),
            int(last_success_ts_ms),
            int(error_count),
        )
        accepted = _enqueue_rows("price_provider_health", [row])
        if accepted:
            _PRICE_PROVIDER_STATE[provider_name] = {
                "last_success_ts_ms": int(last_success_ts_ms),
                "error_count": int(error_count),
            }
            return True
    flushed = _append_rows_direct("price_provider_health", [row])
    with _PRICE_PROVIDER_STATE_LOCK:
        _PRICE_PROVIDER_STATE[provider_name] = {
            "last_success_ts_ms": int(last_success_ts_ms),
            "error_count": int(error_count),
        }
    return bool(flushed)


def enqueue_weather_provider_health(
    *,
    provider: str,
    ok: bool,
    latency_ms: int | None,
    error: str | None = None,
    ts_ms: int | None = None,
) -> bool:
    row = (
        int(ts_ms or (time.time() * 1000)),
        str(provider or ""),
        1 if bool(ok) else 0,
        (int(latency_ms) if latency_ms is not None else None),
        (str(error) if error else None),
    )
    return _enqueue_rows("weather_provider_health", [row])


def append_weather_provider_health(
    *,
    provider: str,
    ok: bool,
    latency_ms: int | None,
    error: str | None = None,
    ts_ms: int | None = None,
) -> bool:
    row = (
        int(ts_ms or (time.time() * 1000)),
        str(provider or ""),
        1 if bool(ok) else 0,
        (int(latency_ms) if latency_ms is not None else None),
        (str(error) if error else None),
    )
    accepted = _enqueue_rows("weather_provider_health", [row])
    if accepted:
        return True
    return bool(_append_rows_direct("weather_provider_health", [row]))


def enqueue_ingestion_pipeline_health(
    row: tuple[int, str, int, int | None, int, int, int | None, str | None, str],
) -> bool:
    return _enqueue_rows("ingestion_pipeline_health", [row])


def append_ingestion_pipeline_health_row(
    row: tuple[int, str, int, int | None, int, int, int | None, str | None, str],
    *,
    prefer_buffer: bool = True,
    attempts: int | None = None,
    timeout_s: float | None = None,
    busy_timeout_ms: int | None = None,
) -> bool:
    if bool(prefer_buffer) and _enqueue_rows("ingestion_pipeline_health", [row]):
        return True
    return bool(
        _append_rows_direct_with_policy(
            "ingestion_pipeline_health",
            [row],
            attempts=attempts,
            timeout_s=timeout_s,
            busy_timeout_ms=busy_timeout_ms,
        )
    )


def enqueue_price_quotes_raw_rows(
    rows: Sequence[tuple[Any, ...]],
) -> bool:
    return _enqueue_rows("price_quotes_raw", rows)


def enqueue_ingest_slippage_rows(
    rows: Sequence[tuple[Any, ...]],
) -> bool:
    return _enqueue_rows("ingest_slippage", rows)


def flush_telemetry_append_buffers(
    *,
    max_batches: int = 8,
    tables: Sequence[str] | None = None,
) -> dict[str, Any]:
    _reset_buffer_for_current_db_path_if_needed()
    flushed = 0
    flush_batches = 0
    selected = [str(name) for name in (tables or []) if str(name) in _TABLE_SPECS] or None
    for _ in range(max(1, int(max_batches))):
        with _BUFFER_LOCK:
            table, rows = _drain_rows_locked(
                max_rows=int(_BUFFER_MAX_BATCH),
                tables=selected,
            )
        if not table or not rows:
            break
        started = time.perf_counter()
        flushed_now = int(_flush_rows(str(table), rows))
        latency_ms = float((time.perf_counter() - started) * 1000.0)
        flushed += int(flushed_now)
        flush_batches += 1
        now_ms = int(time.time() * 1000)
        with _BUFFER_LOCK:
            _record_flush_success_locked(
                table=str(table),
                flushed_rows=int(flushed_now),
                ts_ms=int(now_ms),
                flush_latency_ms=float(latency_ms),
                db_write_duration_ms=float(latency_ms),
            )
        emit_timing(
            "telemetry_append_buffer_flush_latency_ms",
            float(latency_ms),
            component="engine.runtime.telemetry_append_buffer",
            extra_tags={"table": str(table), "path": "manual"},
        )
        emit_timing(
            "telemetry_append_buffer_db_write_duration_ms",
            float(latency_ms),
            component="engine.runtime.telemetry_append_buffer",
            extra_tags={"table": str(table), "path": "manual"},
        )
    snapshot = get_telemetry_append_buffer_snapshot()
    snapshot["flushed"] = int(flushed)
    snapshot["manual_flush_batches"] = int(flush_batches)
    return snapshot


def shutdown_telemetry_append_buffers(timeout_s: float = 2.0) -> dict[str, Any]:
    global _BUFFER_THREAD
    with _BUFFER_LOCK:
        thread = _BUFFER_THREAD
        _BUFFER_STOP.set()
        _BUFFER_LOCK.notify_all()
    if thread is not None:
        thread.join(timeout=max(0.1, float(timeout_s)))
    try:
        snapshot = flush_telemetry_append_buffers(max_batches=64)
    except Exception:
        snapshot = get_telemetry_append_buffer_snapshot()
    with _BUFFER_LOCK:
        _BUFFER_THREAD = None
    return dict(snapshot or {})


__all__ = [
    "append_ingestion_pipeline_health_row",
    "append_price_provider_health",
    "append_weather_provider_health",
    "enqueue_ingest_slippage_rows",
    "enqueue_ingestion_pipeline_health",
    "enqueue_price_quotes_raw_rows",
    "enqueue_price_provider_health",
    "enqueue_weather_provider_health",
    "flush_telemetry_append_buffers",
    "get_telemetry_append_buffer_snapshot",
    "shutdown_telemetry_append_buffers",
]
