"""
FILE: metrics_store.py

Runtime subsystem module for `metrics_store`.
"""

# engine/runtime/metrics_store.py
import json
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.ingestion_tuning import env_bool, tuned_float, tuned_int
from engine.runtime.startup_write_gate import (
    noncritical_startup_write_wait_s,
    should_defer_noncritical_startup_write,
)
from engine.runtime import telemetry_read_router as _telemetry_read_router
from engine.runtime.storage import connect as _db_connect
from engine.runtime.storage import init_db as _init_db
from engine.runtime.storage import run_write_txn


_METRICS_INIT_LOCK = threading.Lock()
_METRICS_INIT_LOCAL = threading.local()
_METRICS_DB_READY = False
_METRICS_DB_READY_PATH = ""
LOG = logging.getLogger("engine.runtime.metrics_store")
_WARNED_NONFATAL_KEYS: set[str] = set()
_RUNTIME_METRICS_BUFFER_ENABLED = env_bool("RUNTIME_METRICS_BUFFER_ENABLED", default=True)
_RUNTIME_METRICS_FLUSH_INTERVAL_S = tuned_float("RUNTIME_METRICS_FLUSH_INTERVAL_S", 3.0, 0.05, 30.0)
_RUNTIME_METRICS_FLUSH_JITTER_RATIO = tuned_float("RUNTIME_METRICS_FLUSH_JITTER_RATIO", 0.5, 0.0, 1.0)
_RUNTIME_METRICS_BUFFER_MAX_BATCH = tuned_int("RUNTIME_METRICS_BUFFER_MAX_BATCH", 256, 1, 4096)
_RUNTIME_METRICS_BUFFER_MAX_ROWS = max(
    _RUNTIME_METRICS_BUFFER_MAX_BATCH,
    tuned_int("RUNTIME_METRICS_BUFFER_MAX_ROWS", 4096, 1, 65536),
)
MetricRow = tuple[int, str, float | None, str | None, str]


def _staggered_flush_interval_s(base_interval_s: float, jitter_ratio: float) -> float:
    base = max(0.05, float(base_interval_s))
    jitter = min(1.0, max(0.0, float(jitter_ratio)))
    if jitter <= 0.0:
        return float(base)
    # Spread periodic control-plane writers across processes so buffered metrics
    # do not re-contend in lockstep on the same SQLite writer.
    bucket = max(0, int(os.getpid()) % 17)
    return float(base * (1.0 + ((float(bucket) / 16.0) * jitter)))


def _runtime_metrics_flush_backoff_s(consecutive_failures: int) -> float:
    failures = max(1, min(int(consecutive_failures), 5))
    base_interval_s = max(1.0, float(_RUNTIME_METRICS_EFFECTIVE_FLUSH_INTERVAL_S))
    return min(10.0, float(base_interval_s * (2 ** (failures - 1))))


_RUNTIME_METRICS_EFFECTIVE_FLUSH_INTERVAL_S = _staggered_flush_interval_s(
    _RUNTIME_METRICS_FLUSH_INTERVAL_S,
    _RUNTIME_METRICS_FLUSH_JITTER_RATIO,
)


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.runtime.metrics_store",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _metrics_enabled() -> bool:
    return str(os.environ.get("RUNTIME_METRICS_ENABLED", "1")).strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }


class _RuntimeMetricsBuffer:
    def __init__(self) -> None:
        self._enabled = bool(_RUNTIME_METRICS_BUFFER_ENABLED)
        self._condition = threading.Condition()
        self._pending: List[MetricRow] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._state: dict[str, Any] = {
            "buffered_rows": 0,
            "dropped_rows": 0,
            "flush_batches": 0,
            "flushed_rows": 0,
            "last_enqueue_ts_ms": 0,
            "last_flush_ts_ms": 0,
            "last_error": "",
            "last_error_ts_ms": 0,
        }

    @property
    def enabled(self) -> bool:
        return bool(self._enabled)

    def note_state(self, **updates: Any) -> None:
        with self._condition:
            self._state.update(dict(updates or {}))

    def snapshot(self) -> dict[str, Any]:
        with self._condition:
            thread_alive = bool(self._thread is not None and self._thread.is_alive())
            state = dict(self._state)
            buffered_rows = int(len(self._pending))
        state["enabled"] = bool(self._enabled)
        state["thread_alive"] = bool(thread_alive)
        state["buffered_rows"] = int(buffered_rows)
        state["buffer_max_rows"] = int(_RUNTIME_METRICS_BUFFER_MAX_ROWS)
        state["flush_interval_s"] = float(_RUNTIME_METRICS_EFFECTIVE_FLUSH_INTERVAL_S)
        state["flush_interval_base_s"] = float(_RUNTIME_METRICS_FLUSH_INTERVAL_S)
        state["flush_jitter_ratio"] = float(_RUNTIME_METRICS_FLUSH_JITTER_RATIO)
        state["batch_size"] = int(_RUNTIME_METRICS_BUFFER_MAX_BATCH)
        return state

    def drain(self, *, max_rows: int | None = None) -> List[MetricRow]:
        with self._condition:
            limit = max(1, int(max_rows or _RUNTIME_METRICS_BUFFER_MAX_BATCH))
            if not self._pending:
                return []
            rows = list(self._pending[:limit])
            del self._pending[:limit]
            self._state["buffered_rows"] = int(len(self._pending))
            return rows

    def requeue(self, rows: List[MetricRow]) -> None:
        if not rows:
            return
        with self._condition:
            room = max(0, int(_RUNTIME_METRICS_BUFFER_MAX_ROWS) - int(len(self._pending)))
            kept = list(rows[:room])
            dropped = max(0, len(rows) - len(kept))
            if kept:
                self._pending[:0] = kept
            self._state["buffered_rows"] = int(len(self._pending))
            if dropped > 0:
                self._state["dropped_rows"] = int(self._state.get("dropped_rows") or 0) + int(dropped)
            self._condition.notify_all()

    def writer_loop(self) -> None:
        consecutive_failures = 0
        pending_since_s = 0.0
        condition = self._condition
        stop = self._stop
        while True:
            with condition:
                while True:
                    if (not self._pending) and (not stop.is_set()):
                        pending_since_s = 0.0
                        condition.wait(timeout=float(_RUNTIME_METRICS_EFFECTIVE_FLUSH_INTERVAL_S))
                    if stop.is_set() and not self._pending:
                        return
                    if self._pending and not stop.is_set():
                        if should_defer_noncritical_startup_write():
                            break
                        now_s = float(time.monotonic())
                        if pending_since_s <= 0.0:
                            pending_since_s = float(now_s)
                        wait_s = float(_RUNTIME_METRICS_EFFECTIVE_FLUSH_INTERVAL_S) - (
                            float(now_s) - float(pending_since_s)
                        )
                        if wait_s > 0.0:
                            condition.wait(timeout=max(0.01, float(wait_s)))
                            continue
                    break
            if should_defer_noncritical_startup_write():
                stop.wait(timeout=float(noncritical_startup_write_wait_s()))
                continue
            rows = self.drain()
            if not rows:
                continue
            try:
                flushed = _flush_runtime_metric_rows(rows)
                consecutive_failures = 0
                now_ms = int(time.time() * 1000)
                with condition:
                    self._state["flush_batches"] = int(self._state.get("flush_batches") or 0) + 1
                    self._state["flushed_rows"] = int(self._state.get("flushed_rows") or 0) + int(flushed)
                    self._state["last_flush_ts_ms"] = int(now_ms)
                    self._state["last_error"] = ""
            except Exception as e:
                consecutive_failures = min(consecutive_failures + 1, 5)
                self.requeue(rows)
                self.note_state(
                    last_error=f"{type(e).__name__}:{e}",
                    last_error_ts_ms=int(time.time() * 1000),
                )
                _warn_nonfatal(
                    "METRICS_STORE_BUFFER_FLUSH_FAILED",
                    e,
                    once_key="metrics_store_buffer_flush_failed",
                    pending_rows=int(self.snapshot().get("buffered_rows") or 0),
                )
                stop.wait(timeout=_runtime_metrics_flush_backoff_s(consecutive_failures))

    def ensure_started(self) -> None:
        if not self._enabled:
            return
        with self._condition:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self.writer_loop,
                name="runtime-metrics-buffer",
                daemon=True,
            )
            self._thread.start()

    def enqueue(self, rows: List[MetricRow]) -> bool:
        if not rows:
            return True
        if not self._enabled:
            return False
        now_ms = int(time.time() * 1000)
        with self._condition:
            room = max(0, int(_RUNTIME_METRICS_BUFFER_MAX_ROWS) - int(len(self._pending)))
            accepted = list(rows[:room])
            dropped = max(0, len(rows) - len(accepted))
            if accepted:
                self._pending.extend(accepted)
                self._state["buffered_rows"] = int(len(self._pending))
                self._state["last_enqueue_ts_ms"] = int(now_ms)
                self._condition.notify_all()
            if dropped > 0:
                self._state["dropped_rows"] = int(self._state.get("dropped_rows") or 0) + int(dropped)
                _warn_nonfatal(
                    "METRICS_STORE_BUFFER_OVERFLOW",
                    RuntimeError(f"runtime_metrics_buffer_overflow:{dropped}"),
                    once_key="metrics_store_buffer_overflow",
                    dropped_rows=int(dropped),
                    max_rows=int(_RUNTIME_METRICS_BUFFER_MAX_ROWS),
                )
        return bool(accepted)

    def flush(self, *, max_batches: int = 8) -> dict[str, Any]:
        flushed = 0
        for _ in range(max(1, int(max_batches))):
            rows = self.drain()
            if not rows:
                break
            flushed_now = int(_flush_runtime_metric_rows(rows))
            flushed += int(flushed_now)
            now_ms = int(time.time() * 1000)
            with self._condition:
                self._state["flush_batches"] = int(self._state.get("flush_batches") or 0) + 1
                self._state["flushed_rows"] = int(self._state.get("flushed_rows") or 0) + int(flushed_now)
                self._state["last_flush_ts_ms"] = int(now_ms)
                self._state["last_error"] = ""
        snapshot = self.snapshot()
        snapshot["flushed"] = int(flushed)
        return snapshot

    def shutdown(self, timeout_s: float = 2.0) -> dict[str, Any]:
        with self._condition:
            thread = self._thread
            self._stop.set()
            self._condition.notify_all()
        if thread is not None:
            thread.join(timeout=max(0.1, float(timeout_s)))
        try:
            snapshot = self.flush(max_batches=64)
        except Exception:
            snapshot = self.snapshot()
        thread_alive = bool(thread is not None and thread.is_alive())
        with self._condition:
            if not thread_alive:
                self._thread = None
        snapshot["thread_alive"] = bool(thread_alive)
        return dict(snapshot or {})


_existing_runtime_metrics_buffer = globals().get("_RUNTIME_METRICS_BUFFER")
if _existing_runtime_metrics_buffer is not None and callable(getattr(_existing_runtime_metrics_buffer, "shutdown", None)):
    try:
        _existing_runtime_metrics_buffer.shutdown(timeout_s=0.5)
    except Exception as e:
        _warn_nonfatal(
            "METRICS_STORE_BUFFER_RELOAD_SHUTDOWN_FAILED",
            e,
            once_key="metrics_store_buffer_reload_shutdown",
        )
_RUNTIME_METRICS_BUFFER = _RuntimeMetricsBuffer()


def _current_db_path_key() -> str:
    try:
        from engine.runtime.db_guard import resolve_db_path

        return str(resolve_db_path())
    except Exception as e:
        _warn_nonfatal(
            "METRICS_STORE_DB_PATH_RESOLVE_FAILED",
            e,
            once_key="metrics_store_db_path_resolve",
        )
        return ""


def _ensure_schema(con) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS runtime_metrics (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          metric TEXT NOT NULL,
          value_num REAL,
          value_text TEXT,
          tags_json TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_runtime_metrics_metric_ts
          ON runtime_metrics(metric, ts_ms);

        CREATE INDEX IF NOT EXISTS idx_runtime_metrics_ts
          ON runtime_metrics(ts_ms);
        """
    )


def _metrics_schema_present() -> bool:
    con = None
    try:
        con = _db_connect(readonly=True)
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='runtime_metrics' LIMIT 1"
        ).fetchone()
        return bool(row)
    except Exception as e:
        _warn_nonfatal(
            "METRICS_STORE_SCHEMA_PRESENCE_CHECK_FAILED",
            e,
            once_key="metrics_store_schema_presence_check",
        )
        return False
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn_nonfatal(
                "METRICS_STORE_SCHEMA_PRESENCE_CLOSE_FAILED",
                e,
                once_key="metrics_store_schema_presence_close",
            )


def init_runtime_metrics_db() -> bool:
    # Metrics tables are created lazily so instrumentation can be imported early
    # without requiring an explicit bootstrap ordering.
    global _METRICS_DB_READY, _METRICS_DB_READY_PATH
    if not _metrics_enabled():
        return False
    if bool(getattr(_METRICS_INIT_LOCAL, "active", False)):
        return False
    _METRICS_INIT_LOCAL.active = True
    try:
        return _init_runtime_metrics_db_inner()
    finally:
        _METRICS_INIT_LOCAL.active = False


def _init_runtime_metrics_db_inner() -> bool:
    global _METRICS_DB_READY, _METRICS_DB_READY_PATH

    db_path_key = _current_db_path_key()

    if _METRICS_DB_READY and _METRICS_DB_READY_PATH == db_path_key:
        return True

    if _metrics_schema_present():
        _METRICS_DB_READY = True
        _METRICS_DB_READY_PATH = db_path_key
        return True

    with _METRICS_INIT_LOCK:
        if _METRICS_DB_READY and _METRICS_DB_READY_PATH == db_path_key:
            return True
        if _metrics_schema_present():
            _METRICS_DB_READY = True
            _METRICS_DB_READY_PATH = db_path_key
            return True
        try:
            _init_db()
            run_write_txn(
                _ensure_schema,
                attempts=1,
                table="runtime_metrics",
                operation="init_runtime_metrics_db",
                direct=True,
                maintenance=False,
                timeout_s=0.25,
                busy_timeout_ms=250,
            )
        except Exception as e:
            _warn_nonfatal(
                "METRICS_STORE_INIT_DB_FAILED",
                e,
                once_key=f"metrics_store_init:{db_path_key}",
                db_path_key=str(db_path_key),
            )
            return False
        _METRICS_DB_READY = True
        _METRICS_DB_READY_PATH = db_path_key
        return True


def _as_float_or_none(val):
    try:
        if val is None:
            return None
        return float(val)
    except Exception as e:
        _warn_nonfatal(
            "METRICS_STORE_VALUE_FLOAT_PARSE_FAILED",
            e,
            once_key=f"metrics_store_float:{val}",
            raw_value=val,
        )
        return None


def _as_text_or_none(val):
    if val is None:
        return None
    try:
        return str(val)
    except Exception as e:
        _warn_nonfatal(
            "METRICS_STORE_VALUE_TEXT_PARSE_FAILED",
            e,
            once_key=f"metrics_store_text:{val}",
            raw_value=val,
        )
        return None


def _runtime_metric_row(
    metric: str,
    *,
    value_num=None,
    value_text=None,
    tags: Optional[Dict] = None,
    ts_ms: Optional[int] = None,
) -> tuple[int, str, float | None, str | None, str]:
    return (
        int(ts_ms or (time.time() * 1000)),
        str(metric or ""),
        _as_float_or_none(value_num),
        _as_text_or_none(value_text),
        json.dumps(tags or {}, separators=(",", ":"), sort_keys=True),
    )


def _note_buffer_state(**updates: Any) -> None:
    _RUNTIME_METRICS_BUFFER.note_state(**updates)


def _runtime_metrics_buffer_snapshot() -> dict[str, Any]:
    return _RUNTIME_METRICS_BUFFER.snapshot()


def _flush_runtime_metric_rows(
    rows: List[tuple[int, str, float | None, str | None, str]],
    *,
    operation: str = "flush_runtime_metrics_buffer",
) -> int:
    if not rows:
        return 0

    def _write(con) -> None:
        con.executemany(
            """
            INSERT INTO runtime_metrics(ts_ms, metric, value_num, value_text, tags_json)
            VALUES (?,?,?,?,?)
            """,
            rows,
        )

    run_write_txn(
        _write,
        attempts=1,
        table="runtime_metrics",
        operation=str(operation or "flush_runtime_metrics_buffer"),
        direct=True,
        maintenance=False,
        timeout_s=0.25,
        busy_timeout_ms=250,
    )
    return int(len(rows))


def _drain_runtime_metrics_buffer(*, max_rows: int | None = None) -> List[MetricRow]:
    return _RUNTIME_METRICS_BUFFER.drain(max_rows=max_rows)


def _requeue_runtime_metric_rows(rows: List[MetricRow]) -> None:
    _RUNTIME_METRICS_BUFFER.requeue(rows)


def _runtime_metrics_writer_loop() -> None:
    _RUNTIME_METRICS_BUFFER.writer_loop()


def _ensure_runtime_metrics_writer_started() -> None:
    _RUNTIME_METRICS_BUFFER.ensure_started()


def _enqueue_runtime_metric_rows(rows: List[MetricRow]) -> bool:
    if not rows:
        return True
    if not _RUNTIME_METRICS_BUFFER.enabled:
        return False
    _ensure_runtime_metrics_writer_started()
    return _RUNTIME_METRICS_BUFFER.enqueue(rows)


def flush_runtime_metrics_buffer(*, max_batches: int = 8) -> dict[str, Any]:
    return _RUNTIME_METRICS_BUFFER.flush(max_batches=max_batches)


def shutdown_runtime_metrics_buffer(timeout_s: float = 2.0) -> dict[str, Any]:
    return _RUNTIME_METRICS_BUFFER.shutdown(timeout_s=timeout_s)


def get_runtime_metrics_buffer_snapshot() -> dict[str, Any]:
    return _runtime_metrics_buffer_snapshot()


def write_runtime_metric(
    metric: str,
    value_num=None,
    value_text=None,
    tags: Optional[Dict] = None,
    ts_ms: Optional[int] = None,
) -> None:
    if not _metrics_enabled():
        return
    # Metrics are best-effort observability writes; callers should not assume
    # they are part of core transactional correctness.
    if not init_runtime_metrics_db():
        return
    row = _runtime_metric_row(
        metric,
        value_num=value_num,
        value_text=value_text,
        tags=tags,
        ts_ms=ts_ms,
    )
    if _enqueue_runtime_metric_rows([row]):
        return
    _flush_runtime_metric_rows([row], operation="write_runtime_metric")


def write_runtime_snapshot(snapshot: Dict, ts_ms: Optional[int] = None) -> None:
    if not _metrics_enabled():
        return
    if not init_runtime_metrics_db():
        return

    now_ms = int(ts_ms or snapshot.get("ts_ms") or (time.time() * 1000))
    metrics = dict(snapshot.get("metrics") or {})
    tags = dict(snapshot.get("tags") or {})

    rows = []
    for metric, raw_val in metrics.items():
        value_num = _as_float_or_none(raw_val)
        value_text = None if value_num is not None else _as_text_or_none(raw_val)
        rows.append(
            _runtime_metric_row(
                str(metric or ""),
                value_num=value_num,
                value_text=value_text,
                tags=tags,
                ts_ms=int(now_ms),
            )
        )
    if not rows:
        return
    if _enqueue_runtime_metric_rows(rows):
        return
    _flush_runtime_metric_rows(rows, operation="write_runtime_snapshot")


def get_runtime_metrics(metric: Optional[str] = None, since_ms: Optional[int] = None, limit: int = 500):
    # Reader API is intentionally generic so dashboards and debugging tools can
    # query one store instead of every metric-specific table.
    if not init_runtime_metrics_db():
        return {
            "ok": False,
            "metric": (str(metric) if metric else None),
            "since_ms": (int(since_ms) if since_ms is not None else None),
            "rows": [],
            "error": "runtime_metrics_db_init_failed",
        }

    if _RUNTIME_METRICS_BUFFER_ENABLED and not should_defer_noncritical_startup_write():
        try:
            flush_runtime_metrics_buffer(max_batches=64)
        except Exception as e:
            _warn_nonfatal(
                "METRICS_STORE_BUFFER_PRE_READ_FLUSH_FAILED",
                e,
                once_key="metrics_store_buffer_pre_read_flush_failed",
                metric=(str(metric) if metric else ""),
            )
    return _telemetry_read_router.fetch_runtime_metrics(metric=metric, since_ms=since_ms, limit=limit)
