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
from engine.runtime.non_price_ingestion_spool import (
    NonPriceIngestionSpoolFullError,
    NonPriceIngestionSpoolUnavailableError,
    SQLiteNonPriceIngestionSpool,
)
from engine.runtime.observability import record_component_health
from engine.runtime.pg_durability import refetchable_pg_durability_snapshot
from engine.runtime.startup_write_gate import (
    noncritical_startup_write_wait_s,
    should_defer_noncritical_startup_write,
)
from engine.runtime.storage import connect_ro, run_refetchable_ingestion_telemetry_txn

LOG = logging.getLogger("engine.runtime.telemetry_append_buffer")

_BUFFER_ENABLED = env_bool("TELEMETRY_APPEND_BUFFER_ENABLED", default=True)
_BUFFER_FLUSH_INTERVAL_S = tuned_float("TELEMETRY_APPEND_BUFFER_FLUSH_INTERVAL_S", 0.5, 0.05, 5.0)
_BUFFER_FLUSH_JITTER_RATIO = tuned_float("TELEMETRY_APPEND_BUFFER_FLUSH_JITTER_RATIO", 0.25, 0.0, 1.0)
_BUFFER_MAX_BATCH = tuned_int("TELEMETRY_APPEND_BUFFER_MAX_BATCH", 128, 1, 4096)
_BUFFER_MAX_ROWS = max(
    _BUFFER_MAX_BATCH,
    tuned_int("TELEMETRY_APPEND_BUFFER_MAX_ROWS", 4096, 1, 65536),
)
_BUFFER_SPOOL_MAX_BYTES = tuned_int(
    "TELEMETRY_APPEND_BUFFER_SPOOL_MAX_BYTES",
    67108864,
    1048576,
    2147483648,
)
_BUFFER_SPOOL_BUSY_TIMEOUT_MS = tuned_int(
    "TELEMETRY_APPEND_BUFFER_SPOOL_BUSY_TIMEOUT_MS",
    50,
    10,
    60000,
)
_BUFFER_SPOOL_SYNCHRONOUS = str(
    os.environ.get("TELEMETRY_APPEND_BUFFER_SPOOL_SYNCHRONOUS") or "NORMAL"
).strip().upper()
if _BUFFER_SPOOL_SYNCHRONOUS not in {"FULL", "NORMAL", "EXTRA"}:
    _BUFFER_SPOOL_SYNCHRONOUS = "NORMAL"

_TABLE_SPECS: dict[str, dict[str, str]] = {
    "price_quotes_raw": {
        "sql": """
        INSERT INTO price_quotes_raw(
          ts_ms, symbol, provider, event_key, event_type, event_ts_ms,
          last, bid, ask, spread, volume,
          trade_ts_ms, quote_ts_ms, ingest_ts_ms, source
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol, provider, event_key, ts_ms) DO UPDATE SET
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
        ON CONFLICT(provider, ts_ms) DO UPDATE SET
          ok=excluded.ok,
          latency_ms=excluded.latency_ms,
          error=excluded.error
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
        ON CONFLICT(pipeline, ts_ms) DO UPDATE SET
          ok=excluded.ok,
          latency_ms=excluded.latency_ms,
          raw_rows=excluded.raw_rows,
          event_rows=excluded.event_rows,
          last_ingested_ts_ms=excluded.last_ingested_ts_ms,
          error=excluded.error,
          meta_json=excluded.meta_json
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


def _sql_for_table(table: str, con: Any) -> str:
    del con
    spec = dict(_TABLE_SPECS.get(str(table)) or {})
    return str(spec.get("sql") or "").strip()
_TABLE_ORDER: tuple[str, ...] = tuple(_TABLE_SPECS.keys())
_PREVIOUS_BUFFER_LOCK = globals().get("_BUFFER_LOCK")
_PREVIOUS_BUFFER_STOP = globals().get("_BUFFER_STOP")
_PREVIOUS_BUFFER_THREAD = globals().get("_BUFFER_THREAD")
_PREVIOUS_DURABLE_SPOOL = globals().get("_DURABLE_SPOOL")
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
if _PREVIOUS_DURABLE_SPOOL is not None:
    try:
        _PREVIOUS_DURABLE_SPOOL.close()
    except Exception:
        LOG.debug("telemetry_append_buffer_reload_spool_close_failed", exc_info=True)
_BUFFER_LOCK = threading.Condition()
_BUFFER_PENDING: dict[str, list[tuple[Any, ...]]] = {name: [] for name in _TABLE_ORDER}
_BUFFER_STOP = threading.Event()
_BUFFER_THREAD: threading.Thread | None = None
_DURABLE_SPOOL: SQLiteNonPriceIngestionSpool | None = None


def _empty_table_counters() -> dict[str, int]:
    return {name: 0 for name in _TABLE_ORDER}


_BUFFER_STATE: dict[str, Any] = {
    "accepted_rows": 0,
    "buffered_rows": 0,
    "dropped_rows": 0,
    "replayed_rows": 0,
    "committed_rows": 0,
    "deleted_rows": 0,
    "flush_batches": 0,
    "flush_failures": 0,
    "flushed_rows": 0,
    "retry_count": 0,
    "shutdown_drain_attempts": 0,
    "shutdown_drain_failures": 0,
    "shutdown_drained_batches": 0,
    "shutdown_drained_rows": 0,
    "residual_spooled_rows": 0,
    "residual_dropped_rows": 0,
    "residual_loss_rows": 0,
    "spooled_rows": 0,
    "last_shutdown_drain_duration_ms": 0,
    "last_shutdown_drain_deadline_ms": 0,
    "shutdown_deadline_exhausted": False,
    "spool_unavailable_count": 0,
    "spool_corrupt_rows": 0,
    "spool_corrupt_payload_rows": 0,
    "spool_corruption_events": 0,
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
    "oldest_age_ms": 0,
    "spool_oldest_age_ms": 0,
    "backpressure_active": False,
    "backpressure_events": 0,
    "last_backpressure_ts_ms": 0,
    "last_backpressure_reason": "",
    "backpressure_recovered_events": 0,
    "last_backpressure_recovered_ts_ms": 0,
    "last_backpressure_recovered_reason": "",
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
    global _BUFFER_DB_PATH_KEY, _DURABLE_SPOOL
    current = _current_buffer_db_path_key()
    if current == _BUFFER_DB_PATH_KEY:
        return
    with _BUFFER_LOCK:
        for table in _TABLE_ORDER:
            _BUFFER_PENDING[table] = []
        if _DURABLE_SPOOL is not None:
            try:
                _DURABLE_SPOOL.close()
            except Exception:
                LOG.debug("telemetry_append_buffer_db_path_spool_close_failed", exc_info=True)
            _DURABLE_SPOOL = None
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
_STEADY_STATE_FLUSH_ATTEMPTS = 1
_STEADY_STATE_WRITE_TIMEOUT_S = 0.25
_STEADY_STATE_BUSY_TIMEOUT_MS = 250
_SHUTDOWN_DRAIN_ATTEMPTS = 4
_SHUTDOWN_DRAIN_WRITE_TIMEOUT_CAP_S = 1.0
_SHUTDOWN_DRAIN_BUSY_TIMEOUT_CAP_MS = 2000
_SHUTDOWN_DRAIN_RETRY_SLEEP_S = 0.05


def _shutdown_drain_write_policy(remaining_s: float) -> dict[str, int | float]:
    remaining = max(0.0, float(remaining_s))
    if remaining <= 0.0:
        return {"attempts": 0, "write_timeout_s": 0.0, "busy_timeout_ms": 0}
    write_timeout_s = min(
        float(_SHUTDOWN_DRAIN_WRITE_TIMEOUT_CAP_S),
        max(0.001, float(remaining)),
    )
    busy_timeout_ms = min(
        int(_SHUTDOWN_DRAIN_BUSY_TIMEOUT_CAP_MS),
        max(1, int(float(write_timeout_s) * 1000.0)),
    )
    return {
        "attempts": 1,
        "write_timeout_s": float(write_timeout_s),
        "busy_timeout_ms": int(busy_timeout_ms),
    }


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


def _durable_spool() -> SQLiteNonPriceIngestionSpool:
    global _DURABLE_SPOOL
    if _DURABLE_SPOOL is None:
        _DURABLE_SPOOL = SQLiteNonPriceIngestionSpool(
            max_rows=int(_BUFFER_MAX_ROWS),
            max_bytes=int(_BUFFER_SPOOL_MAX_BYTES),
            busy_timeout_ms=int(_BUFFER_SPOOL_BUSY_TIMEOUT_MS),
            synchronous=str(_BUFFER_SPOOL_SYNCHRONOUS),
        )
    return _DURABLE_SPOOL


def _spool_stats() -> dict[str, Any]:
    try:
        return dict(_durable_spool().stats())
    except Exception as exc:
        return {
            "ok": False,
            "pending_rows": 0,
            "pending_batches": 0,
            "pending_bytes": 0,
            "file_bytes": 0,
            "max_rows": int(_BUFFER_MAX_ROWS),
            "max_bytes": int(_BUFFER_SPOOL_MAX_BYTES),
            "corruption_events": int(_BUFFER_STATE.get("spool_corruption_events") or 0),
            "synchronous": str(_BUFFER_SPOOL_SYNCHRONOUS),
            "error": f"{type(exc).__name__}:{exc}",
        }


def _spool_stats_by_table() -> dict[str, dict[str, Any]]:
    try:
        return dict(_durable_spool().stats_by_table())
    except Exception:
        return {}


def _legacy_pending_row_count_locked() -> int:
    return sum(int(len(rows)) for rows in _BUFFER_PENDING.values())


def _spooled_row_count_hint_locked() -> int:
    return max(0, int(_BUFFER_STATE.get("spooled_rows") or 0))


def _buffered_row_count_locked() -> int:
    legacy_rows = int(_legacy_pending_row_count_locked())
    if legacy_rows > 0:
        return int(legacy_rows) + int(_spooled_row_count_hint_locked())
    stats = _spool_stats()
    spooled_rows = max(0, int(stats.get("pending_rows") or 0))
    _BUFFER_STATE["spooled_rows"] = int(spooled_rows)
    return int(spooled_rows)


def _pending_by_table_locked() -> dict[str, int]:
    by_table = {
        name: int(len(_BUFFER_PENDING.get(name) or []))
        for name in _TABLE_ORDER
    }
    spool_tables = _spool_stats_by_table()
    for name in _TABLE_ORDER:
        spool_rows = int((spool_tables.get(name) or {}).get("pending_rows") or 0)
        by_table[name] = int(by_table.get(name) or 0) + int(spool_rows)
    return by_table


def _refresh_spool_state_locked(stats: dict[str, Any] | None = None) -> dict[str, Any]:
    spool = dict(stats or _spool_stats())
    spooled_rows = max(0, int(spool.get("pending_rows") or 0))
    pending_rows = int(spooled_rows) + int(_legacy_pending_row_count_locked())
    pending_bytes = int(spool.get("pending_bytes") or 0)
    oldest_age_ms = int(spool.get("oldest_age_ms") or 0)
    _BUFFER_STATE["buffered_rows"] = int(pending_rows)
    _BUFFER_STATE["spooled_rows"] = int(spooled_rows)
    _BUFFER_STATE["oldest_age_ms"] = int(oldest_age_ms)
    _BUFFER_STATE["spool_oldest_age_ms"] = int(oldest_age_ms)
    _BUFFER_STATE["spool_corruption_events"] = int(spool.get("corruption_events") or 0)
    now_ms = int(time.time() * 1000)
    if pending_rows >= int(_BUFFER_MAX_ROWS):
        _note_backpressure_locked(reason="queue_full", ts_ms=now_ms)
    elif pending_bytes >= int(_BUFFER_SPOOL_MAX_BYTES):
        _note_backpressure_locked(reason="spool_byte_limit", ts_ms=now_ms)
    else:
        _clear_backpressure_if_recovered_locked(ts_ms=now_ms)
    return spool


def _increment_table_counter_locked(field: str, table: str, amount: int) -> None:
    counters = dict(_BUFFER_STATE.get(str(field)) or {})
    counters[str(table)] = int(counters.get(str(table)) or 0) + int(amount)
    _BUFFER_STATE[str(field)] = counters


def _set_last_rejected_locked(*, table: str, reason: str, ts_ms: int) -> None:
    _BUFFER_STATE["last_rejected_table"] = str(table)
    _BUFFER_STATE["last_rejected_reason"] = str(reason)
    _BUFFER_STATE["last_rejected_ts_ms"] = int(ts_ms)


def _note_backpressure_locked(*, reason: str, ts_ms: int) -> None:
    if not bool(_BUFFER_STATE.get("backpressure_active")):
        _BUFFER_STATE["backpressure_events"] = int(_BUFFER_STATE.get("backpressure_events") or 0) + 1
    _BUFFER_STATE["backpressure_active"] = True
    _BUFFER_STATE["last_backpressure_reason"] = str(reason)
    _BUFFER_STATE["last_backpressure_ts_ms"] = int(ts_ms)


def _clear_backpressure_if_recovered_locked(*, ts_ms: int) -> None:
    if not bool(_BUFFER_STATE.get("backpressure_active")):
        return
    _BUFFER_STATE["backpressure_active"] = False
    _BUFFER_STATE["backpressure_recovered_events"] = (
        int(_BUFFER_STATE.get("backpressure_recovered_events") or 0) + 1
    )
    _BUFFER_STATE["last_backpressure_recovered_reason"] = "queue_below_limit"
    _BUFFER_STATE["last_backpressure_recovered_ts_ms"] = int(ts_ms)


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
    spool = _refresh_spool_state_locked()
    state = dict(_BUFFER_STATE)
    state["accepted_by_table"] = dict(_BUFFER_STATE.get("accepted_by_table") or {})
    state["dropped_by_table"] = dict(_BUFFER_STATE.get("dropped_by_table") or {})
    state["flushed_by_table"] = dict(_BUFFER_STATE.get("flushed_by_table") or {})
    state["enabled"] = bool(_BUFFER_ENABLED)
    state["thread_alive"] = bool(_BUFFER_THREAD is not None and _BUFFER_THREAD.is_alive())
    state["queue_depth"] = int(state.get("buffered_rows") or 0)
    state["queue_rows"] = int(state.get("buffered_rows") or 0)
    state["buffer_max_rows"] = int(_BUFFER_MAX_ROWS)
    state["queue_fill_ratio"] = float(
        int(state.get("buffered_rows") or 0) / float(max(1, int(_BUFFER_MAX_ROWS)))
    )
    state["batch_size"] = int(_BUFFER_MAX_BATCH)
    state["flush_interval_s"] = float(_BUFFER_EFFECTIVE_FLUSH_INTERVAL_S)
    state["flush_interval_base_s"] = float(_BUFFER_FLUSH_INTERVAL_S)
    state["flush_jitter_ratio"] = float(_BUFFER_FLUSH_JITTER_RATIO)
    state["steady_state_flush_attempts"] = int(_STEADY_STATE_FLUSH_ATTEMPTS)
    state["steady_state_write_timeout_s"] = float(_STEADY_STATE_WRITE_TIMEOUT_S)
    state["steady_state_busy_timeout_ms"] = int(_STEADY_STATE_BUSY_TIMEOUT_MS)
    state["shutdown_drain_max_attempts"] = int(_SHUTDOWN_DRAIN_ATTEMPTS)
    state["shutdown_drain_write_timeout_cap_s"] = float(_SHUTDOWN_DRAIN_WRITE_TIMEOUT_CAP_S)
    state["shutdown_drain_busy_timeout_cap_ms"] = int(_SHUTDOWN_DRAIN_BUSY_TIMEOUT_CAP_MS)
    state["db_path_key"] = str(_BUFFER_DB_PATH_KEY)
    state["pending_by_table"] = _pending_by_table_locked()
    state["write_path"] = "durable_sqlite_spool"
    state["spool"] = spool
    state["spool_path"] = str(spool.get("path") or "")
    state["spool_pending_batches"] = int(spool.get("pending_batches") or 0)
    state["spool_pending_rows"] = int(spool.get("pending_rows") or 0)
    state["spool_pending_bytes"] = int(spool.get("pending_bytes") or 0)
    state["spool_file_bytes"] = int(spool.get("file_bytes") or 0)
    state["spool_max_rows"] = int(spool.get("max_rows") or _BUFFER_MAX_ROWS)
    state["spool_max_bytes"] = int(spool.get("max_bytes") or _BUFFER_SPOOL_MAX_BYTES)
    state["spool_oldest_age_ms"] = int(spool.get("oldest_age_ms") or 0)
    state["oldest_age_ms"] = int(state.get("oldest_age_ms") or state.get("spool_oldest_age_ms") or 0)
    state["spool_bytes_fill_ratio"] = float(spool.get("bytes_fill_ratio") or 0.0)
    state["spool_synchronous"] = str(spool.get("synchronous") or _BUFFER_SPOOL_SYNCHRONOUS)
    state["durability"] = refetchable_pg_durability_snapshot()
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

    run_refetchable_ingestion_telemetry_txn(
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


def _flush_rows_with_policy(
    table: str,
    rows: Sequence[tuple[Any, ...]],
    *,
    attempts: int | None,
    timeout_s: float | None,
    busy_timeout_ms: int | None,
) -> int:
    return int(
        _write_rows(
            str(table),
            rows,
            attempts=attempts,
            timeout_s=timeout_s,
            busy_timeout_ms=busy_timeout_ms,
        )
    )


def _flush_rows(
    table: str,
    rows: Sequence[tuple[Any, ...]],
    *,
    attempts: int | None = _STEADY_STATE_FLUSH_ATTEMPTS,
    timeout_s: float | None = _STEADY_STATE_WRITE_TIMEOUT_S,
    busy_timeout_ms: int | None = _STEADY_STATE_BUSY_TIMEOUT_MS,
) -> int:
    return _flush_rows_with_policy(
        str(table),
        rows,
        attempts=attempts,
        timeout_s=timeout_s,
        busy_timeout_ms=busy_timeout_ms,
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
        _BUFFER_STATE["committed_rows"] = int(_BUFFER_STATE.get("committed_rows") or 0) + int(flushed)
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
) -> tuple[str | None, list[tuple[Any, ...]], list[int], str]:
    selected = [str(name) for name in (tables or _TABLE_ORDER) if str(name) in _TABLE_SPECS]
    if not selected:
        selected = list(_TABLE_ORDER)
    for table in selected:
        pending = _BUFFER_PENDING.get(table) or []
        if not pending:
            continue
        rows = list(pending[:max_rows])
        del pending[:max_rows]
        _refresh_spool_state_locked()
        return str(table), rows, [], "memory"
    try:
        records, corrupt = _durable_spool().select_batch(
            limit_rows=int(max_rows),
            tables=selected,
        )
    except NonPriceIngestionSpoolUnavailableError as exc:
        _BUFFER_STATE["spool_unavailable_count"] = int(_BUFFER_STATE.get("spool_unavailable_count") or 0) + 1
        _BUFFER_STATE["last_error"] = f"{type(exc).__name__}:{exc}"
        _BUFFER_STATE["last_error_ts_ms"] = int(time.time() * 1000)
        return None, [], [], "spool"
    if corrupt:
        corrupt_rows = int(sum(int(record.total_rows) for record in corrupt))
        corrupt_ids = [int(record.id) for record in corrupt]
        _BUFFER_STATE["spool_corrupt_rows"] = int(_BUFFER_STATE.get("spool_corrupt_rows") or 0) + len(corrupt)
        _BUFFER_STATE["spool_corrupt_payload_rows"] = (
            int(_BUFFER_STATE.get("spool_corrupt_payload_rows") or 0) + int(corrupt_rows)
        )
        _BUFFER_STATE["dropped_rows"] = int(_BUFFER_STATE.get("dropped_rows") or 0) + int(corrupt_rows)
        for record in corrupt:
            _increment_table_counter_locked("dropped_by_table", str(record.table), int(record.total_rows))
        try:
            _durable_spool().delete(corrupt_ids)
        except Exception as exc:
            _BUFFER_STATE["last_error"] = f"{type(exc).__name__}:{exc}"
            _BUFFER_STATE["last_error_ts_ms"] = int(time.time() * 1000)
        emit_counter(
            "telemetry_append_buffer_spool_corrupt_rows",
            int(len(corrupt)),
            component="engine.runtime.telemetry_append_buffer",
        )
        emit_counter(
            "telemetry_append_buffer_dropped_rows",
            int(corrupt_rows),
            component="engine.runtime.telemetry_append_buffer",
            extra_tags={"reason": "spool_payload_corrupt"},
        )
    if not records:
        _refresh_spool_state_locked()
        return None, [], [], "spool"
    table = str(records[0].table)
    rows = [tuple(row) for record in records for row in record.rows]
    ids = [int(record.id) for record in records]
    _BUFFER_STATE["replayed_rows"] = int(_BUFFER_STATE.get("replayed_rows") or 0) + int(len(rows))
    _refresh_spool_state_locked()
    return table, rows, ids, "spool"


def _delete_spool_records(ids: Sequence[int], *, table: str, row_count: int) -> int:
    if not ids:
        return 0
    deleted = int(_durable_spool().delete(ids))
    with _BUFFER_LOCK:
        _BUFFER_STATE["deleted_rows"] = int(_BUFFER_STATE.get("deleted_rows") or 0) + int(row_count)
        _refresh_spool_state_locked()
    emit_counter(
        "telemetry_append_buffer_spool_deleted_rows",
        int(row_count),
        component="engine.runtime.telemetry_append_buffer",
        extra_tags={"table": str(table)},
    )
    return int(deleted)


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
            _note_backpressure_locked(reason="requeue_overflow", ts_ms=int(time.time() * 1000))
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
        emit_counter(
            "telemetry_append_buffer_backpressure_events",
            1,
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
            table, rows, spool_ids, drain_source = _drain_rows_locked(max_rows=int(_BUFFER_MAX_BATCH))
        if not table or not rows:
            continue
        try:
            flush_started = time.perf_counter()
            flushed = _flush_rows(str(table), rows)
            if drain_source == "spool":
                _delete_spool_records(spool_ids, table=str(table), row_count=int(flushed))
            latency_ms = float((time.perf_counter() - flush_started) * 1000.0)
            consecutive_failures = 0
            now_ms = int(time.time() * 1000)
            with _BUFFER_LOCK:
                _BUFFER_STATE["committed_rows"] = int(_BUFFER_STATE.get("committed_rows") or 0) + int(flushed)
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
            emit_gauge(
                "telemetry_append_buffer_oldest_age_ms",
                int(get_telemetry_append_buffer_snapshot().get("oldest_age_ms") or 0),
                component="engine.runtime.telemetry_append_buffer",
                extra_tags={"table": str(table), "path": "background"},
            )
            record_component_health(
                "telemetry_append_buffer",
                ok=True,
                status="ok",
                detail="flush_ok",
                observed_ts_ms=int(now_ms),
                latency_ms=float(latency_ms),
                extra={
                    "table": str(table),
                    "rows": int(flushed),
                    "source": str(drain_source),
                    "queue_depth": int(get_telemetry_append_buffer_snapshot().get("buffered_rows") or 0),
                },
            )
        except Exception as e:
            consecutive_failures = min(consecutive_failures + 1, 5)
            if drain_source != "spool":
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
            record_component_health(
                "telemetry_append_buffer",
                ok=False,
                status="error",
                detail=f"{type(e).__name__}:{e}",
                observed_ts_ms=int(time.time() * 1000),
                extra={
                    "table": str(table),
                    "rows": int(len(rows)),
                    "source": str(drain_source),
                    "spool_rows_retained": bool(drain_source == "spool"),
                },
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
    reject_reason = ""
    with _BUFFER_LOCK:
        room = max(0, int(_BUFFER_MAX_ROWS) - int(_buffered_row_count_locked()))
        candidate_rows = [tuple(row) for row in list(rows[:room])]
        dropped_count = max(0, len(rows) - len(candidate_rows))
        remaining = list(candidate_rows)
        while remaining:
            chunk = remaining[: int(_BUFFER_MAX_BATCH)]
            try:
                stats = _durable_spool().enqueue(
                    table=table_name,
                    rows=chunk,
                    created_ts_ms=int(now_ms),
                )
            except NonPriceIngestionSpoolFullError:
                dropped_count += int(len(remaining))
                reject_reason = "buffer_overflow"
                break
            except NonPriceIngestionSpoolUnavailableError as exc:
                dropped_count += int(len(remaining))
                reject_reason = "spool_unavailable"
                _BUFFER_STATE["spool_unavailable_count"] = (
                    int(_BUFFER_STATE.get("spool_unavailable_count") or 0) + 1
                )
                _BUFFER_STATE["last_error"] = f"{type(exc).__name__}:{exc}"
                _BUFFER_STATE["last_error_ts_ms"] = int(now_ms)
                break
            accepted_count += int(len(chunk))
            remaining = remaining[int(_BUFFER_MAX_BATCH) :]
            _refresh_spool_state_locked(stats)
        if accepted_count > 0:
            _BUFFER_STATE["accepted_rows"] = int(_BUFFER_STATE.get("accepted_rows") or 0) + int(accepted_count)
            _increment_table_counter_locked("accepted_by_table", table_name, int(accepted_count))
            _BUFFER_STATE["last_enqueue_ts_ms"] = int(now_ms)
            _BUFFER_LOCK.notify_all()
        if dropped_count > 0:
            if not reject_reason:
                reject_reason = "buffer_overflow"
            _note_backpressure_locked(reason=str(reject_reason), ts_ms=int(now_ms))
            _BUFFER_STATE["dropped_rows"] = int(_BUFFER_STATE.get("dropped_rows") or 0) + int(dropped_count)
            _increment_table_counter_locked("dropped_by_table", table_name, int(dropped_count))
            _set_last_rejected_locked(
                table=table_name,
                reason=str(reject_reason),
                ts_ms=int(now_ms),
            )
            _warn_nonfatal(
                "TELEMETRY_APPEND_BUFFER_OVERFLOW",
                RuntimeError(f"telemetry_append_buffer_overflow:{dropped_count}"),
                table=table_name,
                dropped_rows=int(dropped_count),
                max_rows=int(_BUFFER_MAX_ROWS),
                reason=str(reject_reason),
            )
        elif accepted_count <= 0:
            _note_backpressure_locked(reason="buffer_full", ts_ms=int(now_ms))
            _set_last_rejected_locked(
                table=table_name,
                reason="buffer_full",
                ts_ms=int(now_ms),
            )
        _refresh_spool_state_locked()
    if accepted_count > 0:
        snapshot = get_telemetry_append_buffer_snapshot()
        emit_gauge(
            "telemetry_append_buffer_queue_depth",
            int(snapshot.get("buffered_rows") or 0),
            component="engine.runtime.telemetry_append_buffer",
            extra_tags={"table": table_name},
        )
        emit_gauge(
            "telemetry_append_buffer_spool_pending_bytes",
            int(snapshot.get("spool_pending_bytes") or 0),
            component="engine.runtime.telemetry_append_buffer",
            extra_tags={"table": table_name},
        )
        emit_gauge(
            "telemetry_append_buffer_oldest_age_ms",
            int(snapshot.get("oldest_age_ms") or 0),
            component="engine.runtime.telemetry_append_buffer",
            extra_tags={"table": table_name},
        )
    if dropped_count > 0:
        emit_counter(
            "telemetry_append_buffer_dropped_rows",
            int(dropped_count),
            component="engine.runtime.telemetry_append_buffer",
            extra_tags={"table": table_name, "reason": str(reject_reason or "buffer_overflow")},
        )
        emit_counter(
            "telemetry_append_buffer_backpressure_events",
            1,
            component="engine.runtime.telemetry_append_buffer",
            extra_tags={"table": table_name, "reason": str(reject_reason or "buffer_overflow")},
        )
    return bool(accepted_count > 0 and dropped_count <= 0)


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
        with _PRICE_PROVIDER_STATE_LOCK:
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
        with _PRICE_PROVIDER_STATE_LOCK:
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
    attempts: int | None = _STEADY_STATE_FLUSH_ATTEMPTS,
    write_timeout_s: float | None = _STEADY_STATE_WRITE_TIMEOUT_S,
    busy_timeout_ms: int | None = _STEADY_STATE_BUSY_TIMEOUT_MS,
    path: str = "manual",
) -> dict[str, Any]:
    _reset_buffer_for_current_db_path_if_needed()
    flushed = 0
    flush_batches = 0
    selected = [str(name) for name in (tables or []) if str(name) in _TABLE_SPECS] or None
    for _ in range(max(1, int(max_batches))):
        with _BUFFER_LOCK:
            table, rows, spool_ids, drain_source = _drain_rows_locked(
                max_rows=int(_BUFFER_MAX_BATCH),
                tables=selected,
            )
        if not table or not rows:
            break
        started = time.perf_counter()
        try:
            flushed_now = int(
                _flush_rows(
                    str(table),
                    rows,
                    attempts=attempts,
                    timeout_s=write_timeout_s,
                    busy_timeout_ms=busy_timeout_ms,
                )
            )
            if drain_source == "spool":
                _delete_spool_records(spool_ids, table=str(table), row_count=int(flushed_now))
        except Exception as exc:
            if drain_source != "spool":
                _requeue_rows(str(table), rows)
            with _BUFFER_LOCK:
                _BUFFER_STATE["flush_failures"] = int(_BUFFER_STATE.get("flush_failures") or 0) + 1
                _BUFFER_STATE["retry_count"] = int(_BUFFER_STATE.get("retry_count") or 0) + 1
                _BUFFER_STATE["last_error"] = f"{type(exc).__name__}:{exc}"
                _BUFFER_STATE["last_error_ts_ms"] = int(time.time() * 1000)
                _refresh_spool_state_locked()
            emit_counter(
                "telemetry_append_buffer_retries",
                1,
                component="engine.runtime.telemetry_append_buffer",
                extra_tags={"table": str(table), "path": str(path or "manual")},
            )
            raise
        latency_ms = float((time.perf_counter() - started) * 1000.0)
        flushed += int(flushed_now)
        flush_batches += 1
        now_ms = int(time.time() * 1000)
        with _BUFFER_LOCK:
            _BUFFER_STATE["committed_rows"] = int(_BUFFER_STATE.get("committed_rows") or 0) + int(flushed_now)
            _record_flush_success_locked(
                table=str(table),
                flushed_rows=int(flushed_now),
                ts_ms=int(now_ms),
                flush_latency_ms=float(latency_ms),
                db_write_duration_ms=float(latency_ms),
            )
            if str(path) == "shutdown":
                _BUFFER_STATE["shutdown_drained_batches"] = int(
                    _BUFFER_STATE.get("shutdown_drained_batches") or 0
                ) + 1
                _BUFFER_STATE["shutdown_drained_rows"] = int(
                    _BUFFER_STATE.get("shutdown_drained_rows") or 0
                ) + int(flushed_now)
        emit_timing(
            "telemetry_append_buffer_flush_latency_ms",
            float(latency_ms),
            component="engine.runtime.telemetry_append_buffer",
            extra_tags={"table": str(table), "path": str(path or "manual")},
        )
        emit_timing(
            "telemetry_append_buffer_db_write_duration_ms",
            float(latency_ms),
            component="engine.runtime.telemetry_append_buffer",
            extra_tags={"table": str(table), "path": str(path or "manual")},
        )
        emit_gauge(
            "telemetry_append_buffer_oldest_age_ms",
            int(get_telemetry_append_buffer_snapshot().get("oldest_age_ms") or 0),
            component="engine.runtime.telemetry_append_buffer",
            extra_tags={"table": str(table), "path": str(path or "manual")},
        )
    snapshot = get_telemetry_append_buffer_snapshot()
    snapshot["flushed"] = int(flushed)
    snapshot["manual_flush_batches"] = int(flush_batches)
    return snapshot


def _record_shutdown_residual_locked(*, deadline_exhausted: bool) -> dict[str, int | bool]:
    spool = _refresh_spool_state_locked()
    residual_spooled_rows = int(spool.get("pending_rows") or 0)
    residual_dropped_rows = int(_legacy_pending_row_count_locked())
    _BUFFER_STATE["shutdown_deadline_exhausted"] = bool(deadline_exhausted)
    if residual_spooled_rows > 0:
        _BUFFER_STATE["residual_spooled_rows"] = int(
            _BUFFER_STATE.get("residual_spooled_rows") or 0
        ) + int(residual_spooled_rows)
    if residual_dropped_rows > 0:
        _BUFFER_STATE["residual_dropped_rows"] = int(
            _BUFFER_STATE.get("residual_dropped_rows") or 0
        ) + int(residual_dropped_rows)
        _BUFFER_STATE["residual_loss_rows"] = int(_BUFFER_STATE.get("residual_loss_rows") or 0) + int(
            residual_dropped_rows
        )
        _BUFFER_STATE["dropped_rows"] = int(_BUFFER_STATE.get("dropped_rows") or 0) + int(residual_dropped_rows)
        for table, rows in _BUFFER_PENDING.items():
            if rows:
                _increment_table_counter_locked("dropped_by_table", str(table), int(len(rows)))
                _BUFFER_PENDING[table] = []
        _refresh_spool_state_locked()
    return {
        "residual_spooled_rows": int(residual_spooled_rows),
        "residual_dropped_rows": int(residual_dropped_rows),
        "deadline_exhausted": bool(deadline_exhausted),
    }


def _emit_shutdown_residual_metrics(residual: dict[str, int | bool]) -> None:
    residual_spooled_rows = int(residual.get("residual_spooled_rows") or 0)
    residual_dropped_rows = int(residual.get("residual_dropped_rows") or 0)
    if residual_spooled_rows > 0:
        emit_counter(
            "telemetry_append_buffer_residual_spooled_rows",
            int(residual_spooled_rows),
            component="engine.runtime.telemetry_append_buffer",
            extra_tags={"reason": "shutdown_deadline"},
        )
    if residual_dropped_rows > 0:
        emit_counter(
            "telemetry_append_buffer_residual_dropped_rows",
            int(residual_dropped_rows),
            component="engine.runtime.telemetry_append_buffer",
            extra_tags={"reason": "shutdown_deadline"},
        )
        emit_counter(
            "telemetry_append_buffer_residual_loss_rows",
            int(residual_dropped_rows),
            component="engine.runtime.telemetry_append_buffer",
            extra_tags={"reason": "shutdown_deadline"},
        )
    if residual_spooled_rows > 0 or residual_dropped_rows > 0:
        record_component_health(
            "telemetry_append_buffer",
            ok=bool(residual_dropped_rows == 0),
            status=("degraded" if residual_dropped_rows == 0 else "error"),
            detail="shutdown_deadline_residual",
            observed_ts_ms=int(time.time() * 1000),
            extra={
                "residual_spooled_rows": int(residual_spooled_rows),
                "residual_dropped_rows": int(residual_dropped_rows),
                "deadline_exhausted": bool(residual.get("deadline_exhausted")),
            },
        )


def shutdown_telemetry_append_buffers(timeout_s: float = 2.0) -> dict[str, Any]:
    global _BUFFER_THREAD, _DURABLE_SPOOL
    started = time.perf_counter()
    deadline_s = max(0.0, float(timeout_s))
    deadline = time.monotonic() + float(deadline_s)
    with _BUFFER_LOCK:
        thread = _BUFFER_THREAD
        _BUFFER_STOP.set()
        _BUFFER_LOCK.notify_all()
    if thread is not None and thread.is_alive():
        thread.join(timeout=max(0.0, float(deadline) - time.monotonic()))
    thread_alive = bool(thread is not None and thread.is_alive())
    shutdown_attempts = 0
    if not thread_alive:
        consecutive_failures = 0
        while True:
            remaining_s = max(0.0, float(deadline) - time.monotonic())
            with _BUFFER_LOCK:
                pending_rows = int(_buffered_row_count_locked())
            if pending_rows <= 0 or remaining_s <= 0.0:
                break
            if consecutive_failures >= int(_SHUTDOWN_DRAIN_ATTEMPTS):
                break
            policy = _shutdown_drain_write_policy(remaining_s)
            if int(policy.get("attempts") or 0) <= 0:
                break
            try:
                shutdown_attempts += 1
                flush_telemetry_append_buffers(
                    max_batches=1,
                    attempts=int(policy.get("attempts") or 1),
                    write_timeout_s=float(policy.get("write_timeout_s") or 0.001),
                    busy_timeout_ms=int(policy.get("busy_timeout_ms") or 1),
                    path="shutdown",
                )
                consecutive_failures = 0
            except Exception as exc:
                consecutive_failures += 1
                with _BUFFER_LOCK:
                    _BUFFER_STATE["shutdown_drain_failures"] = int(
                        _BUFFER_STATE.get("shutdown_drain_failures") or 0
                    ) + 1
                    _BUFFER_STATE["last_error"] = f"{type(exc).__name__}:{exc}"
                    _BUFFER_STATE["last_error_ts_ms"] = int(time.time() * 1000)
                sleep_s = min(
                    float(_SHUTDOWN_DRAIN_RETRY_SLEEP_S) * float(consecutive_failures),
                    max(0.0, float(deadline) - time.monotonic()),
                )
                if sleep_s <= 0.0:
                    break
                _BUFFER_STOP.wait(timeout=float(sleep_s))
    with _BUFFER_LOCK:
        pending_after = int(_buffered_row_count_locked())
        deadline_exhausted = bool(pending_after > 0 and (thread_alive or time.monotonic() >= float(deadline)))
        residual = _record_shutdown_residual_locked(deadline_exhausted=deadline_exhausted)
        _BUFFER_STATE["last_shutdown_drain_duration_ms"] = int(
            round((time.perf_counter() - started) * 1000.0)
        )
        _BUFFER_STATE["last_shutdown_drain_deadline_ms"] = int(round(float(deadline_s) * 1000.0))
        _BUFFER_STATE["shutdown_drain_attempts"] = int(
            _BUFFER_STATE.get("shutdown_drain_attempts") or 0
        ) + int(shutdown_attempts)
        snapshot = _snapshot_locked()
    _emit_shutdown_residual_metrics(residual)
    with _BUFFER_LOCK:
        if not thread_alive:
            _BUFFER_THREAD = None
        if _DURABLE_SPOOL is not None and not thread_alive:
            try:
                _DURABLE_SPOOL.close()
            except Exception:
                LOG.debug("telemetry_append_buffer_shutdown_spool_close_failed", exc_info=True)
            _DURABLE_SPOOL = None
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
