"""
Sidecar TimescaleDB client for append-heavy time-series workloads.

SQLite remains the system of record for existing relational/runtime tables.
This module only manages TimescaleDB-backed hypertables and a background batch
writer for new time-series data paths.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import logging
import os
import random
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

try:
    import asyncpg
except Exception:  # pragma: no cover - optional dependency at runtime
    asyncpg = None  # type: ignore[assignment]


LOGGER = logging.getLogger(__name__)
TIMESCALE_SCHEMA_VERSION = 4
_CLIENT_LOCK = threading.Lock()
_CLIENT: "TimescaleClient | None" = None
_SCHEMA_LOCK_KEY = 761_112_019


def _asyncpg_pool_available() -> bool:
    return asyncpg is not None and callable(getattr(asyncpg, "create_pool", None))


def _asyncpg_connect_available() -> bool:
    return asyncpg is not None and callable(getattr(asyncpg, "connect", None))


_TIMESCALE_REQUIRED_TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "timescale_schema_version": ("version", "applied_at", "status", "notes"),
    "data_source_logs": ("sqlite_rowid", "time", "source_key", "level", "event_type", "message", "detail_json"),
    "event_log": (
        "sqlite_rowid",
        "time",
        "event_type",
        "event_source",
        "event_version",
        "entity_type",
        "entity_id",
        "correlation_id",
        "payload_json",
    ),
    "price_data": ("symbol", "timestamp", "open", "high", "low", "close", "volume"),
    "price_provider_health": (
        "sqlite_rowid",
        "time",
        "provider",
        "ok",
        "latency_ms",
        "n_symbols",
        "error",
        "last_success_ts_ms",
        "error_count",
    ),
    "feature_data": ("symbol", "timestamp", "feature_vector"),
    "ingestion_pipeline_health": (
        "sqlite_rowid",
        "time",
        "pipeline",
        "ok",
        "latency_ms",
        "raw_rows",
        "event_rows",
        "last_ingested_ts_ms",
        "error",
        "meta_json",
    ),
    "model_predictions": ("model_id", "symbol", "timestamp", "prediction", "confidence"),
    "trade_outcomes": ("trade_id", "timestamp", "pnl", "outcome"),
    "model_registry": ("model_name", "version", "created_at", "metadata"),
    "predictions": (
        "time",
        "symbol",
        "model_name",
        "model_version",
        "prediction",
        "confidence",
        "features_version",
        "model_id",
        "event_id",
        "horizon_s",
        "prediction_id",
        "source_alert_id",
        "tracking_source",
        "metadata",
    ),
    "runtime_metrics": ("sqlite_rowid", "time", "metric", "value_num", "value_text", "tags_json"),
    "weather_provider_health": ("sqlite_rowid", "time", "provider", "ok", "latency_ms", "error"),
}
_TIMESCALE_REQUIRED_INDEXES: tuple[str, ...] = (
    "timescale_schema_version_pkey",
    "data_source_logs_pkey",
    "event_log_pkey",
    "price_data_pkey",
    "price_provider_health_pkey",
    "feature_data_pkey",
    "ingestion_pipeline_health_pkey",
    "model_predictions_pkey",
    "trade_outcomes_pkey",
    "model_registry_pkey",
    "predictions_pkey",
    "runtime_metrics_pkey",
    "weather_provider_health_pkey",
    "idx_data_source_logs_source_time",
    "idx_event_log_time",
    "idx_event_log_type_time",
    "idx_ingestion_pipeline_health_pipeline_time",
    "idx_ingestion_pipeline_health_time",
    "idx_price_data_ts",
    "idx_price_provider_health_time",
    "idx_feature_data_ts",
    "idx_model_predictions_symbol_ts",
    "idx_trade_outcomes_ts",
    "idx_tracking_model_registry_created",
    "idx_tracking_predictions_symbol_time",
    "idx_tracking_predictions_model_time",
    "idx_tracking_predictions_prediction_id",
    "idx_tracking_predictions_event_lookup",
    "idx_runtime_metrics_metric_time",
    "idx_runtime_metrics_time",
    "idx_weather_provider_health_time",
)


class TimescaleError(RuntimeError):
    """Base exception for Timescale client failures."""

    pass


class TimescaleBackpressureError(TimescaleError):
    """Raised when the Timescale write queue cannot accept more work."""

    pass


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "")).strip().lower()
    if raw == "":
        return bool(default)
    return raw in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    raw = str(os.environ.get(name, "")).strip()
    if raw == "":
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _env_int(name: str, default: int) -> int:
    raw = str(os.environ.get(name, "")).strip()
    if raw == "":
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _quote_ident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _chunked(items: list[tuple[Any, ...]], chunk_size: int) -> Iterable[tuple[tuple[Any, ...], ...]]:
    step = max(1, int(chunk_size))
    for idx in range(0, len(items), step):
        yield tuple(items[idx : idx + step])


def _coalesce(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row.get(key) is not None:
            return row.get(key)
    return None


def _normalize_text(value: Any, *, field: str) -> str:
    text = str(value or "").strip()
    if text == "":
        raise ValueError(f"missing_required_field:{field}")
    return text


def _normalize_float(value: Any, *, field: str) -> float:
    if value is None or value == "":
        raise ValueError(f"missing_required_field:{field}")
    return float(value)


def _normalize_timestamp(value: Any, *, field: str = "timestamp") -> datetime:
    if value is None or value == "":
        raise ValueError(f"missing_required_field:{field}")
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        ts = float(value)
        if abs(ts) >= 10_000_000_000:
            ts = ts / 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    text = str(value).strip()
    if text == "":
        raise ValueError(f"missing_required_field:{field}")
    if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
        return _normalize_timestamp(int(text), field=field)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception as exc:
        raise ValueError(f"invalid_timestamp:{field}:{text}") from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalize_jsonb(value: Any, *, field: str) -> str:
    if value is None:
        raise ValueError(f"missing_required_field:{field}")
    if isinstance(value, str):
        raw = value.strip()
        if raw == "":
            raise ValueError(f"missing_required_field:{field}")
        json.loads(raw)
        return raw
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)


def _normalize_jsonb_or_empty(value: Any) -> str:
    try:
        if value in (None, ""):
            return "{}"
        return _normalize_jsonb(value, field="json")
    except Exception:
        return "{}"


@dataclass(frozen=True)
class TimescaleConfig:
    """Configure queueing, retry, and connection settings for the Timescale client."""

    enabled: bool
    dsn: str
    schema_name: str
    pool_min_size: int
    pool_max_size: int
    batch_size: int
    flush_interval_s: float
    queue_maxsize: int
    retry_attempts: int
    retry_base_s: float
    retry_max_s: float
    backpressure_timeout_s: float
    start_timeout_s: float
    connect_timeout_s: float
    lock_timeout_s: float
    command_timeout_s: float
    idle_in_txn_timeout_s: float
    application_name: str
    retention_days: int = 0
    compression_after_days: int = 0

    @classmethod
    def from_env(cls) -> "TimescaleConfig":
        """Build the Timescale client configuration from environment variables."""
        dsn = str(
            os.environ.get("TIMESCALE_DSN")
            or os.environ.get("TIMESCALE_URL")
            or os.environ.get("TIMESCALE_DATABASE_URL")
            or ""
        ).strip()
        enabled = _env_bool("TIMESCALE_ENABLED", default=bool(dsn))
        return cls(
            enabled=bool(enabled),
            dsn=dsn,
            schema_name=str(os.environ.get("TIMESCALE_SCHEMA", "public")).strip() or "public",
            pool_min_size=max(1, _env_int("TIMESCALE_POOL_MIN_SIZE", 1)),
            pool_max_size=max(1, _env_int("TIMESCALE_POOL_MAX_SIZE", 4)),
            batch_size=max(1, _env_int("TIMESCALE_BATCH_SIZE", 500)),
            flush_interval_s=max(0.05, _env_float("TIMESCALE_FLUSH_INTERVAL_S", 1.0)),
            queue_maxsize=max(1, _env_int("TIMESCALE_QUEUE_MAXSIZE", 1024)),
            retry_attempts=max(1, _env_int("TIMESCALE_RETRY_ATTEMPTS", 5)),
            retry_base_s=max(0.05, _env_float("TIMESCALE_RETRY_BASE_S", 0.25)),
            retry_max_s=max(0.1, _env_float("TIMESCALE_RETRY_MAX_S", 5.0)),
            backpressure_timeout_s=max(0.05, _env_float("TIMESCALE_BACKPRESSURE_TIMEOUT_S", 5.0)),
            start_timeout_s=max(0.1, _env_float("TIMESCALE_START_TIMEOUT_S", 5.0)),
            connect_timeout_s=max(0.1, _env_float("TIMESCALE_CONNECT_TIMEOUT_S", 5.0)),
            lock_timeout_s=max(0.05, _env_float("TIMESCALE_LOCK_TIMEOUT_S", 5.0)),
            command_timeout_s=max(1.0, _env_float("TIMESCALE_COMMAND_TIMEOUT_S", 30.0)),
            idle_in_txn_timeout_s=max(1.0, _env_float("TIMESCALE_IDLE_IN_TXN_TIMEOUT_S", 60.0)),
            application_name=str(os.environ.get("TIMESCALE_APPLICATION_NAME", "trading-system")).strip()
            or "trading-system",
            retention_days=max(0, _env_int("TIMESCALE_RETENTION_DAYS", 0)),
            compression_after_days=max(0, _env_int("TIMESCALE_COMPRESSION_AFTER_DAYS", 0)),
        )


@dataclass(frozen=True)
class _WriteEnvelope:
    table: str
    rows: tuple[tuple[Any, ...], ...]
    row_count: int
    enqueued_at: float


class TimescaleClient:
    """Background writer and schema manager for Timescale-backed time-series tables."""

    def __init__(self, config: TimescaleConfig | None = None):
        self._config = config or TimescaleConfig.from_env()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._queue: asyncio.Queue[_WriteEnvelope] | None = None
        self._thread: threading.Thread | None = None
        self._thread_started = threading.Event()
        self._state_lock = threading.Lock()
        self._metrics_lock = threading.Lock()
        self._pool: Any = None
        self._pool_lock: asyncio.Lock | None = None
        self._schema_lock: asyncio.Lock | None = None
        self._stop_event: asyncio.Event | None = None
        self._schema_ready = False
        self._schema_error: str | None = None
        self._schema_validation: dict[str, Any] = {
            "required_tables": sorted(_TIMESCALE_REQUIRED_TABLE_COLUMNS),
            "required_indexes": list(_TIMESCALE_REQUIRED_INDEXES),
            "missing_tables": [],
            "missing_columns": {},
            "missing_indexes": [],
        }
        self._policy_status: dict[str, Any] = {
            "retention_days": int(self._config.retention_days),
            "compression_after_days": int(self._config.compression_after_days),
            "applied": False,
            "last_error": "",
        }
        self._last_error: str | None = None
        self._last_error_ts_ms = 0
        self._last_connect_ts_ms = 0
        self._metrics: dict[str, Any] = {
            "backpressure_count": 0,
            "backpressure_active": False,
            "buffered_rows": 0,
            "consecutive_flush_failures": 0,
            "enqueue_failure_count": 0,
            "enqueued_rows": 0,
            "flush_failure_count": 0,
            "flushed_batches": 0,
            "flushed_rows": 0,
            "inflight_rows": 0,
            "last_backpressure_ts_ms": 0,
            "last_flush_failure_ts_ms": 0,
            "last_flush_ts_ms": 0,
            "retry_count": 0,
            "table_stats": {
                "data_source_logs": {"enqueued_rows": 0, "flushed_rows": 0},
                "event_log": {"enqueued_rows": 0, "flushed_rows": 0},
                "feature_data": {"enqueued_rows": 0, "flushed_rows": 0},
                "ingestion_pipeline_health": {"enqueued_rows": 0, "flushed_rows": 0},
                "model_registry": {"enqueued_rows": 0, "flushed_rows": 0},
                "model_predictions": {"enqueued_rows": 0, "flushed_rows": 0},
                "predictions": {"enqueued_rows": 0, "flushed_rows": 0},
                "price_data": {"enqueued_rows": 0, "flushed_rows": 0},
                "price_provider_health": {"enqueued_rows": 0, "flushed_rows": 0},
                "runtime_metrics": {"enqueued_rows": 0, "flushed_rows": 0},
                "trade_outcomes": {"enqueued_rows": 0, "flushed_rows": 0},
                "weather_provider_health": {"enqueued_rows": 0, "flushed_rows": 0},
            },
        }

    @property
    def enabled(self) -> bool:
        return bool(self._config.enabled)

    def start(self) -> dict[str, Any]:
        if not self.enabled:
            return self.get_snapshot()
        already_started = False
        with self._state_lock:
            if self._thread is not None and self._thread.is_alive():
                already_started = True
            else:
                if not _asyncpg_pool_available():
                    raise RuntimeError("timescaledb_enabled_but_asyncpg_is_not_installed")
                self._thread_started.clear()
                thread = threading.Thread(
                    target=self._thread_main,
                    name="timescale-writer",
                    daemon=True,
                )
                self._thread = thread
                thread.start()
        if already_started:
            return self.get_snapshot()
        if not self._thread_started.wait(timeout=self._config.start_timeout_s):
            raise RuntimeError("timescale_writer_start_timeout")
        self._schedule_schema_warmup()
        return self.get_snapshot()

    def close(self, timeout_s: float | None = None) -> dict[str, Any]:
        thread = None
        loop = None
        stop_event = None
        with self._state_lock:
            thread = self._thread
            loop = self._loop
            stop_event = self._stop_event
        if thread is None or loop is None or stop_event is None:
            return self.get_snapshot()
        loop.call_soon_threadsafe(stop_event.set)
        join_timeout = float(timeout_s or max(1.0, self._config.start_timeout_s))
        thread.join(timeout=join_timeout)
        if thread.is_alive():
            self._record_error(RuntimeError(f"timescale_shutdown_timeout:{join_timeout}"))
        return self.get_snapshot()

    def ensure_schema(self, timeout_s: float | None = None) -> dict[str, Any]:
        if not self.enabled:
            return self.get_snapshot()
        self.start()
        loop = self._loop
        if loop is None:
            raise RuntimeError("timescale_event_loop_unavailable")
        future = asyncio.run_coroutine_threadsafe(self._ensure_schema(), loop)
        future.result(timeout=timeout_s or self._config.command_timeout_s)
        return self.get_snapshot()

    def enqueue_price_data(self, rows: Iterable[Mapping[str, Any]], *, timeout_s: float | None = None) -> int:
        return self._enqueue("price_data", rows, timeout_s=timeout_s)

    def enqueue_runtime_metrics(self, rows: Iterable[Mapping[str, Any]], *, timeout_s: float | None = None) -> int:
        return self._enqueue("runtime_metrics", rows, timeout_s=timeout_s)

    def enqueue_event_log(self, rows: Iterable[Mapping[str, Any]], *, timeout_s: float | None = None) -> int:
        return self._enqueue("event_log", rows, timeout_s=timeout_s)

    def enqueue_ingestion_pipeline_health(self, rows: Iterable[Mapping[str, Any]], *, timeout_s: float | None = None) -> int:
        return self._enqueue("ingestion_pipeline_health", rows, timeout_s=timeout_s)

    def enqueue_price_provider_health(self, rows: Iterable[Mapping[str, Any]], *, timeout_s: float | None = None) -> int:
        return self._enqueue("price_provider_health", rows, timeout_s=timeout_s)

    def enqueue_weather_provider_health(self, rows: Iterable[Mapping[str, Any]], *, timeout_s: float | None = None) -> int:
        return self._enqueue("weather_provider_health", rows, timeout_s=timeout_s)

    def enqueue_data_source_logs(self, rows: Iterable[Mapping[str, Any]], *, timeout_s: float | None = None) -> int:
        return self._enqueue("data_source_logs", rows, timeout_s=timeout_s)

    def enqueue_feature_data(self, rows: Iterable[Mapping[str, Any]], *, timeout_s: float | None = None) -> int:
        return self._enqueue("feature_data", rows, timeout_s=timeout_s)

    def enqueue_model_predictions(self, rows: Iterable[Mapping[str, Any]], *, timeout_s: float | None = None) -> int:
        return self._enqueue("model_predictions", rows, timeout_s=timeout_s)

    def enqueue_model_registry(self, rows: Iterable[Mapping[str, Any]], *, timeout_s: float | None = None) -> int:
        return self._enqueue("model_registry", rows, timeout_s=timeout_s)

    def enqueue_predictions(self, rows: Iterable[Mapping[str, Any]], *, timeout_s: float | None = None) -> int:
        return self._enqueue("predictions", rows, timeout_s=timeout_s)

    def enqueue_trade_outcomes(self, rows: Iterable[Mapping[str, Any]], *, timeout_s: float | None = None) -> int:
        return self._enqueue("trade_outcomes", rows, timeout_s=timeout_s)

    def get_snapshot(self) -> dict[str, Any]:
        with self._metrics_lock:
            metrics = json.loads(json.dumps(self._metrics))
        queue_depth = 0
        loop_alive = False
        schema_validation = dict(self._schema_validation)
        with self._state_lock:
            if self._queue is not None:
                try:
                    queue_depth = int(self._queue.qsize())
                except Exception:
                    queue_depth = 0
            loop_alive = bool(self._thread is not None and self._thread.is_alive())
            policy_status = dict(self._policy_status)
        schema_ok = not (
            list(schema_validation.get("missing_tables") or [])
            or dict(schema_validation.get("missing_columns") or {})
            or list(schema_validation.get("missing_indexes") or [])
        )
        degraded_reasons: list[str] = []
        if self.enabled and not loop_alive:
            degraded_reasons.append("writer_stopped")
        if self.enabled and not bool(self._schema_ready):
            degraded_reasons.append("schema_not_ready")
        if self._schema_error is not None:
            degraded_reasons.append("schema_error")
        if self.enabled and not schema_ok:
            degraded_reasons.append("schema_invalid")
        if bool(metrics.get("backpressure_active")):
            degraded_reasons.append("queue_backpressure")
        if int(metrics.get("consecutive_flush_failures") or 0) > 0:
            degraded_reasons.append("flush_failures")
        if self.enabled and queue_depth >= int(self._config.queue_maxsize):
            degraded_reasons.append("queue_full")
        degraded = bool(degraded_reasons)
        return {
            "ok": (
                (not self.enabled)
                or (
                    loop_alive
                    and bool(self._schema_ready)
                    and self._schema_error is None
                    and bool(schema_ok)
                    and not degraded
                )
            ),
            "degraded": bool(degraded),
            "degraded_reasons": degraded_reasons,
            "enabled": bool(self.enabled),
            "dsn_configured": bool(self._config.dsn),
            "driver_available": _asyncpg_pool_available(),
            "queue_depth": int(queue_depth),
            "queue_maxsize": int(self._config.queue_maxsize),
            "backpressure_timeout_s": float(self._config.backpressure_timeout_s),
            "schema_name": str(self._config.schema_name),
            "schema_ready": bool(self._schema_ready),
            "schema_ok": bool(schema_ok),
            "schema_version": int(TIMESCALE_SCHEMA_VERSION if self._schema_ready else 0),
            "schema_error": self._schema_error,
            "schema_validation": schema_validation,
            "policy_status": policy_status,
            "started": bool(loop_alive),
            "connect_timeout_s": float(self._config.connect_timeout_s),
            "command_timeout_s": float(self._config.command_timeout_s),
            "lock_timeout_s": float(self._config.lock_timeout_s),
            "last_error": self._last_error,
            "last_error_ts_ms": int(self._last_error_ts_ms or 0),
            "last_connect_ts_ms": (int(self._last_connect_ts_ms) if self._last_connect_ts_ms > 0 else None),
            "metrics": metrics,
            "ts_ms": _now_ms(),
        }

    def _thread_main(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            self._loop = loop
            self._queue = asyncio.Queue(maxsize=self._config.queue_maxsize)
            self._pool_lock = asyncio.Lock()
            self._schema_lock = asyncio.Lock()
            self._stop_event = asyncio.Event()
            self._thread_started.set()
            loop.run_until_complete(self._run())
        except Exception as exc:
            LOGGER.exception("timescale writer loop crashed")
            self._record_error(exc)
            if not self._thread_started.is_set():
                self._thread_started.set()
        finally:
            try:
                pending = asyncio.all_tasks(loop=loop)
                for task in pending:
                    task.cancel()
                if pending:
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception:
                pass  # no-op-guard: allow best-effort async shutdown
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            except Exception:
                pass  # no-op-guard: allow best-effort async shutdown
            with self._state_lock:
                self._loop = None
                self._queue = None
                self._thread = None
                self._stop_event = None
            asyncio.set_event_loop(None)
            loop.close()

    async def _run(self) -> None:
        await self._warmup_schema()
        await self._writer_loop()
        await self._close_pool()

    async def _warmup_schema(self) -> None:
        try:
            await self._ensure_schema()
        except Exception as exc:
            self._schema_error = f"{type(exc).__name__}: {exc}"
            self._record_error(exc)
            LOGGER.warning("timescale schema warmup failed: %s", exc)

    def _schedule_schema_warmup(self) -> None:
        loop = self._loop
        if loop is None:
            return
        future = asyncio.run_coroutine_threadsafe(self._warmup_schema(), loop)

        def _ignore_result(done: concurrent.futures.Future[Any]) -> None:
            try:
                done.result()
            except Exception:
                return

        future.add_done_callback(_ignore_result)

    def _record_schema_validation(self, validation: Mapping[str, Any]) -> None:
        self._schema_validation = {
            "required_tables": list(validation.get("required_tables") or []),
            "required_indexes": list(validation.get("required_indexes") or []),
            "missing_tables": list(validation.get("missing_tables") or []),
            "missing_columns": dict(validation.get("missing_columns") or {}),
            "missing_indexes": list(validation.get("missing_indexes") or []),
        }

    async def _validate_schema(self, conn: Any) -> dict[str, Any]:
        table_rows = await conn.fetch(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = $1
            """,
            str(self._config.schema_name),
        )
        present_tables = {
            str(row["table_name"] or "").strip()
            for row in (table_rows or [])
            if str(row["table_name"] or "").strip()
        }
        required_tables = sorted(_TIMESCALE_REQUIRED_TABLE_COLUMNS)
        missing_tables = [table_name for table_name in required_tables if table_name not in present_tables]

        column_rows = await conn.fetch(
            """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = $1
            """,
            str(self._config.schema_name),
        )
        present_columns: dict[str, set[str]] = {}
        for row in (column_rows or []):
            table_name = str(row["table_name"] or "").strip()
            column_name = str(row["column_name"] or "").strip().lower()
            if not table_name or not column_name:
                continue
            present_columns.setdefault(table_name, set()).add(column_name)
        missing_columns: dict[str, list[str]] = {}
        for table_name, columns in _TIMESCALE_REQUIRED_TABLE_COLUMNS.items():
            if table_name in missing_tables:
                continue
            table_columns = present_columns.get(table_name, set())
            absent = [
                str(column)
                for column in columns
                if str(column).strip().lower() not in table_columns
            ]
            if absent:
                missing_columns[str(table_name)] = absent

        index_rows = await conn.fetch(
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = $1
            """,
            str(self._config.schema_name),
        )
        present_indexes = {
            str(row["indexname"] or "").strip()
            for row in (index_rows or [])
            if str(row["indexname"] or "").strip()
        }
        required_indexes = list(_TIMESCALE_REQUIRED_INDEXES)
        missing_indexes = [
            index_name
            for index_name in required_indexes
            if index_name not in present_indexes
        ]
        validation = {
            "required_tables": required_tables,
            "required_indexes": required_indexes,
            "missing_tables": missing_tables,
            "missing_columns": missing_columns,
            "missing_indexes": missing_indexes,
        }
        self._record_schema_validation(validation)
        if missing_tables or missing_columns or missing_indexes:
            raise TimescaleError(
                "timescale_schema_invalid:"
                f"missing_tables={missing_tables};"
                f"missing_columns={missing_columns};"
                f"missing_indexes={missing_indexes}"
            )
        return validation

    async def _ensure_pool(self) -> Any:
        if self._pool is not None:
            return self._pool
        if self._pool_lock is None:
            raise RuntimeError("timescale_pool_lock_uninitialized")
        async with self._pool_lock:
            if self._pool is not None:
                return self._pool
            if not _asyncpg_pool_available():
                raise RuntimeError("asyncpg_not_available")
            if not self._config.dsn:
                raise RuntimeError("timescale_dsn_not_configured")
            self._pool = await asyncpg.create_pool(
                dsn=self._config.dsn,
                min_size=int(self._config.pool_min_size),
                max_size=int(max(self._config.pool_min_size, self._config.pool_max_size)),
                command_timeout=float(self._config.command_timeout_s),
                timeout=float(self._config.connect_timeout_s),
                server_settings={
                    "application_name": self._config.application_name,
                    "statement_timeout": str(int(max(1.0, self._config.command_timeout_s) * 1000)),
                    "lock_timeout": str(int(max(1.0, self._config.lock_timeout_s) * 1000)),
                    "idle_in_transaction_session_timeout": str(
                        int(max(1.0, self._config.idle_in_txn_timeout_s) * 1000)
                    ),
                    "timezone": "UTC",
                },
            )
            self._last_connect_ts_ms = _now_ms()
            return self._pool

    async def _close_pool(self) -> None:
        pool = self._pool
        self._pool = None
        if pool is not None:
            try:
                await pool.close()
            except Exception as exc:
                self._record_error(exc)

    async def _reset_pool(self) -> None:
        await self._close_pool()

    async def _ensure_schema(self) -> None:
        if self._schema_ready:
            return
        if self._schema_lock is None:
            raise RuntimeError("timescale_schema_lock_uninitialized")
        async with self._schema_lock:
            if self._schema_ready:
                return
            try:
                pool = await self._ensure_pool()
                schema_name = self._config.schema_name
                async with pool.acquire() as conn:
                    await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_ident(schema_name)}")
                    await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")
                    await conn.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS {self._table_ref('timescale_schema_version')} (
                          version INTEGER PRIMARY KEY,
                          applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                          status TEXT NOT NULL,
                          notes TEXT
                        )
                        """
                    )
                    await conn.execute("SELECT pg_advisory_lock($1)", int(_SCHEMA_LOCK_KEY))
                    try:
                        current_version = int(
                            await conn.fetchval(
                                f"SELECT COALESCE(MAX(version), 0) FROM {self._table_ref('timescale_schema_version')}"
                            )
                            or 0
                        )
                        if current_version < 1:
                            async with conn.transaction():
                                await self._apply_migration_v1(conn)
                                await self._record_schema_version(
                                    conn,
                                    version=1,
                                    notes="initial_timescale_hypertables",
                                )
                        if current_version < 2:
                            async with conn.transaction():
                                await self._apply_migration_v2(conn)
                                await self._record_schema_version(
                                    conn,
                                    version=2,
                                    notes="prediction_tracking_tables",
                                )
                        if current_version < 3:
                            async with conn.transaction():
                                await self._apply_migration_v3(conn)
                                await self._record_schema_version(
                                    conn,
                                    version=3,
                                    notes="prediction_tracking_linkage_columns",
                                )
                        if current_version < 4:
                            async with conn.transaction():
                                await self._apply_migration_v4(conn)
                                await self._record_schema_version(
                                    conn,
                                    version=4,
                                    notes="telemetry_shadow_tables",
                                )
                        async with conn.transaction():
                            await self._apply_table_policies(conn, "data_source_logs")
                            await self._apply_table_policies(conn, "event_log")
                            await self._apply_table_policies(conn, "ingestion_pipeline_health")
                            await self._apply_table_policies(conn, "price_data")
                            await self._apply_table_policies(conn, "price_provider_health")
                            await self._apply_table_policies(conn, "feature_data")
                            await self._apply_table_policies(conn, "model_predictions")
                            await self._apply_table_policies(conn, "trade_outcomes")
                            await self._apply_table_policies(conn, "predictions")
                            await self._apply_table_policies(conn, "runtime_metrics")
                            await self._apply_table_policies(conn, "weather_provider_health")
                        self._record_policy_status(applied=True)
                        await self._validate_schema(conn)
                        self._schema_ready = True
                        self._schema_error = None
                    finally:
                        await conn.execute("SELECT pg_advisory_unlock($1)", int(_SCHEMA_LOCK_KEY))
            except Exception as exc:
                self._schema_ready = False
                self._schema_error = f"{type(exc).__name__}: {exc}"
                self._record_policy_status(applied=False, last_error=f"{type(exc).__name__}: {exc}")
                self._record_error(exc)
                raise

    async def _apply_migration_v1(self, conn: Any) -> None:
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._table_ref('price_data')} (
              symbol TEXT NOT NULL,
              "timestamp" TIMESTAMPTZ NOT NULL,
              "open" DOUBLE PRECISION NOT NULL,
              "high" DOUBLE PRECISION NOT NULL,
              "low" DOUBLE PRECISION NOT NULL,
              "close" DOUBLE PRECISION NOT NULL,
              volume DOUBLE PRECISION NOT NULL,
              PRIMARY KEY(symbol, "timestamp")
            )
            """
        )
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._table_ref('feature_data')} (
              symbol TEXT NOT NULL,
              "timestamp" TIMESTAMPTZ NOT NULL,
              feature_vector JSONB NOT NULL,
              PRIMARY KEY(symbol, "timestamp")
            )
            """
        )
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._table_ref('model_predictions')} (
              model_id TEXT NOT NULL,
              symbol TEXT NOT NULL,
              "timestamp" TIMESTAMPTZ NOT NULL,
              prediction DOUBLE PRECISION NOT NULL,
              confidence DOUBLE PRECISION NOT NULL,
              PRIMARY KEY(model_id, symbol, "timestamp")
            )
            """
        )
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._table_ref('trade_outcomes')} (
              trade_id TEXT NOT NULL,
              "timestamp" TIMESTAMPTZ NOT NULL,
              pnl DOUBLE PRECISION NOT NULL,
              outcome TEXT NOT NULL,
              PRIMARY KEY(trade_id, "timestamp")
            )
            """
        )
        await self._create_hypertable(conn, "price_data")
        await self._create_hypertable(conn, "feature_data")
        await self._create_hypertable(conn, "model_predictions")
        await self._create_hypertable(conn, "trade_outcomes")
        await self._apply_table_policies(conn, "price_data")
        await self._apply_table_policies(conn, "feature_data")
        await self._apply_table_policies(conn, "model_predictions")
        await self._apply_table_policies(conn, "trade_outcomes")
        await conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_price_data_ts ON {self._table_ref("price_data")} ("timestamp" DESC)'
        )
        await conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_feature_data_ts ON {self._table_ref("feature_data")} ("timestamp" DESC)'
        )
        await conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_model_predictions_symbol_ts ON {self._table_ref("model_predictions")} (symbol, "timestamp" DESC)'
        )
        await conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_trade_outcomes_ts ON {self._table_ref("trade_outcomes")} ("timestamp" DESC)'
        )

    async def _apply_migration_v2(self, conn: Any) -> None:
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._table_ref('model_registry')} (
              model_name TEXT NOT NULL,
              version TEXT NOT NULL,
              created_at TIMESTAMPTZ NOT NULL,
              metadata JSONB NOT NULL,
              PRIMARY KEY(model_name, version)
            )
            """
        )
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._table_ref('predictions')} (
              "time" TIMESTAMPTZ NOT NULL,
              symbol TEXT NOT NULL,
              model_name TEXT NOT NULL,
              model_version TEXT NOT NULL,
              prediction DOUBLE PRECISION NOT NULL,
              confidence DOUBLE PRECISION NOT NULL,
              features_version TEXT NOT NULL,
              model_id TEXT,
              event_id BIGINT,
              horizon_s INTEGER,
              prediction_id BIGINT,
              source_alert_id BIGINT,
              tracking_source TEXT,
              metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
              PRIMARY KEY(model_name, model_version, symbol, "time")
            )
            """
        )
        await self._create_hypertable(conn, "predictions", time_column="time")
        await self._apply_table_policies(conn, "predictions")
        await conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_tracking_model_registry_created ON {self._table_ref("model_registry")} (created_at DESC)'
        )
        await conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_tracking_predictions_symbol_time ON {self._table_ref("predictions")} (symbol, "time" DESC)'
        )
        await conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_tracking_predictions_model_time ON {self._table_ref("predictions")} (model_name, model_version, "time" DESC)'
        )

    async def _apply_migration_v3(self, conn: Any) -> None:
        predictions_table = self._table_ref("predictions")
        cols = {
            str(row.get("column_name") or "").strip().lower()
            for row in (
                await conn.fetch(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = $1 AND table_name = $2
                    """,
                    str(self._config.schema_name),
                    "predictions",
                )
                or []
            )
        }
        if "model_id" not in cols:
            await conn.execute(f"ALTER TABLE {predictions_table} ADD COLUMN model_id TEXT")
        if "event_id" not in cols:
            await conn.execute(f"ALTER TABLE {predictions_table} ADD COLUMN event_id BIGINT")
        if "horizon_s" not in cols:
            await conn.execute(f"ALTER TABLE {predictions_table} ADD COLUMN horizon_s INTEGER")
        if "prediction_id" not in cols:
            await conn.execute(f"ALTER TABLE {predictions_table} ADD COLUMN prediction_id BIGINT")
        if "source_alert_id" not in cols:
            await conn.execute(f"ALTER TABLE {predictions_table} ADD COLUMN source_alert_id BIGINT")
        if "tracking_source" not in cols:
            await conn.execute(f"ALTER TABLE {predictions_table} ADD COLUMN tracking_source TEXT")
        if "metadata" not in cols:
            await conn.execute(
                f"ALTER TABLE {predictions_table} ADD COLUMN metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb"
            )
        await conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_tracking_predictions_prediction_id ON {predictions_table} (prediction_id, "time" DESC)'
        )
        await conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_tracking_predictions_event_lookup ON {predictions_table} (event_id, symbol, horizon_s, "time" DESC)'
        )

    async def _apply_migration_v4(self, conn: Any) -> None:
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._table_ref('runtime_metrics')} (
              sqlite_rowid BIGINT NOT NULL,
              "time" TIMESTAMPTZ NOT NULL,
              metric TEXT NOT NULL,
              value_num DOUBLE PRECISION,
              value_text TEXT,
              tags_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
              PRIMARY KEY(sqlite_rowid, "time")
            )
            """
        )
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._table_ref('event_log')} (
              sqlite_rowid BIGINT NOT NULL,
              "time" TIMESTAMPTZ NOT NULL,
              event_type TEXT NOT NULL,
              event_source TEXT NOT NULL,
              event_version INTEGER NOT NULL,
              entity_type TEXT,
              entity_id TEXT,
              correlation_id TEXT,
              payload_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
              PRIMARY KEY(sqlite_rowid, "time")
            )
            """
        )
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._table_ref('ingestion_pipeline_health')} (
              sqlite_rowid BIGINT NOT NULL,
              "time" TIMESTAMPTZ NOT NULL,
              pipeline TEXT NOT NULL,
              ok SMALLINT NOT NULL,
              latency_ms INTEGER,
              raw_rows BIGINT NOT NULL DEFAULT 0,
              event_rows BIGINT NOT NULL DEFAULT 0,
              last_ingested_ts_ms BIGINT,
              error TEXT,
              meta_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
              PRIMARY KEY(sqlite_rowid, "time")
            )
            """
        )
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._table_ref('price_provider_health')} (
              sqlite_rowid BIGINT NOT NULL,
              "time" TIMESTAMPTZ NOT NULL,
              provider TEXT NOT NULL,
              ok SMALLINT NOT NULL,
              latency_ms INTEGER,
              n_symbols INTEGER,
              error TEXT,
              last_success_ts_ms BIGINT,
              error_count INTEGER,
              PRIMARY KEY(sqlite_rowid, "time")
            )
            """
        )
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._table_ref('weather_provider_health')} (
              sqlite_rowid BIGINT NOT NULL,
              "time" TIMESTAMPTZ NOT NULL,
              provider TEXT NOT NULL,
              ok SMALLINT NOT NULL,
              latency_ms INTEGER,
              error TEXT,
              PRIMARY KEY(sqlite_rowid, "time")
            )
            """
        )
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._table_ref('data_source_logs')} (
              sqlite_rowid BIGINT NOT NULL,
              "time" TIMESTAMPTZ NOT NULL,
              source_key TEXT NOT NULL,
              level TEXT NOT NULL,
              event_type TEXT NOT NULL,
              message TEXT,
              detail_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
              PRIMARY KEY(sqlite_rowid, "time")
            )
            """
        )
        await self._create_hypertable(conn, "runtime_metrics", time_column="time")
        await self._create_hypertable(conn, "event_log", time_column="time")
        await self._create_hypertable(conn, "ingestion_pipeline_health", time_column="time")
        await self._create_hypertable(conn, "price_provider_health", time_column="time")
        await self._create_hypertable(conn, "weather_provider_health", time_column="time")
        await self._create_hypertable(conn, "data_source_logs", time_column="time")
        await self._apply_table_policies(conn, "runtime_metrics")
        await self._apply_table_policies(conn, "event_log")
        await self._apply_table_policies(conn, "ingestion_pipeline_health")
        await self._apply_table_policies(conn, "price_provider_health")
        await self._apply_table_policies(conn, "weather_provider_health")
        await self._apply_table_policies(conn, "data_source_logs")
        await conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_runtime_metrics_time ON {self._table_ref("runtime_metrics")} ("time" DESC)'
        )
        await conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_runtime_metrics_metric_time ON {self._table_ref("runtime_metrics")} (metric, "time" DESC)'
        )
        await conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_event_log_time ON {self._table_ref("event_log")} ("time" DESC)'
        )
        await conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_event_log_type_time ON {self._table_ref("event_log")} (event_type, "time" DESC)'
        )
        await conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_ingestion_pipeline_health_time ON {self._table_ref("ingestion_pipeline_health")} ("time" DESC)'
        )
        await conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_ingestion_pipeline_health_pipeline_time ON {self._table_ref("ingestion_pipeline_health")} (pipeline, "time" DESC)'
        )
        await conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_price_provider_health_time ON {self._table_ref("price_provider_health")} ("time" DESC)'
        )
        await conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_weather_provider_health_time ON {self._table_ref("weather_provider_health")} ("time" DESC)'
        )
        await conn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_data_source_logs_source_time ON {self._table_ref("data_source_logs")} (source_key, "time" DESC)'
        )

    async def _record_schema_version(self, conn: Any, *, version: int, notes: str) -> None:
        await conn.execute(
            f"""
            INSERT INTO {self._table_ref('timescale_schema_version')}(
              version,
              applied_at,
              status,
              notes
            )
            VALUES($1, NOW(), 'applied', $2)
            ON CONFLICT(version) DO UPDATE SET
              applied_at = EXCLUDED.applied_at,
              status = EXCLUDED.status,
              notes = EXCLUDED.notes
            """,
            int(version),
            str(notes),
        )

    async def _create_hypertable(self, conn: Any, table_name: str, *, time_column: str = "timestamp") -> None:
        relation_name = f"{self._config.schema_name}.{table_name}"
        await conn.execute(
            """
            SELECT create_hypertable(
              $1::regclass,
              $2,
              if_not_exists => TRUE,
              migrate_data => TRUE
            )
            """,
            relation_name,
            str(time_column),
        )

    def _record_policy_status(self, *, applied: bool, last_error: str = "") -> None:
        with self._state_lock:
            self._policy_status = {
                "retention_days": int(self._config.retention_days),
                "compression_after_days": int(self._config.compression_after_days),
                "applied": bool(applied),
                "last_error": str(last_error or ""),
            }

    async def _apply_table_policies(self, conn: Any, table_name: str, *, segment_by: str | None = None) -> None:
        relation_name = f"{self._config.schema_name}.{table_name}"
        if segment_by is None:
            segment_by = {
                "data_source_logs": "source_key",
                "event_log": "event_type",
                "feature_data": "symbol",
                "ingestion_pipeline_health": "pipeline",
                "model_predictions": "symbol",
                "predictions": "symbol",
                "price_data": "symbol",
                "price_provider_health": "provider",
                "runtime_metrics": "metric",
                "trade_outcomes": "trade_id",
                "weather_provider_health": "provider",
            }.get(str(table_name), "symbol")
        if int(self._config.compression_after_days) > 0:
            await conn.execute(
                f"ALTER TABLE {self._table_ref(table_name)} SET (timescaledb.compress, timescaledb.compress_segmentby = '{segment_by}')"
            )
            await conn.execute(
                "SELECT add_compression_policy($1::regclass, $2::interval, if_not_exists => TRUE)",
                relation_name,
                f"{int(self._config.compression_after_days)} days",
            )
        if int(self._config.retention_days) > 0:
            await conn.execute(
                "SELECT add_retention_policy($1::regclass, $2::interval, if_not_exists => TRUE)",
                relation_name,
                f"{int(self._config.retention_days)} days",
            )

    async def _writer_loop(self) -> None:
        if self._queue is None or self._stop_event is None:
            raise RuntimeError("timescale_writer_not_initialized")
        pending: dict[str, list[tuple[Any, ...]]] = {
            "data_source_logs": [],
            "event_log": [],
            "feature_data": [],
            "ingestion_pipeline_health": [],
            "model_registry": [],
            "model_predictions": [],
            "predictions": [],
            "price_data": [],
            "price_provider_health": [],
            "runtime_metrics": [],
            "trade_outcomes": [],
            "weather_provider_health": [],
        }
        pending_since: dict[str, float] = {}
        while True:
            if self._stop_event.is_set() and self._queue.empty() and not any(pending.values()):
                break
            timeout_s = self._next_wait_timeout(pending, pending_since)
            envelope: _WriteEnvelope | None = None
            try:
                envelope = await asyncio.wait_for(self._queue.get(), timeout=timeout_s)
            except asyncio.TimeoutError:
                envelope = None
            if envelope is not None:
                pending[envelope.table].extend(envelope.rows)
                pending_since.setdefault(envelope.table, envelope.enqueued_at)
            due_tables = self._due_tables(pending, pending_since, stopping=self._stop_event.is_set())
            for table_name in due_tables:
                rows = pending.get(table_name) or []
                if not rows:
                    pending_since.pop(table_name, None)
                    continue
                flushed = await self._flush_with_retry(table_name, rows)
                if not flushed:
                    pending_since[table_name] = time.monotonic()
                    break
                pending[table_name] = []
                pending_since.pop(table_name, None)

    def _next_wait_timeout(self, pending: dict[str, list[tuple[Any, ...]]], pending_since: dict[str, float]) -> float:
        if not any(pending.values()):
            return float(self._config.flush_interval_s)
        now = time.monotonic()
        remaining = [
            max(0.0, float(self._config.flush_interval_s) - max(0.0, now - pending_since.get(table_name, now)))
            for table_name, rows in pending.items()
            if rows
        ]
        if not remaining:
            return 0.0
        return float(max(0.0, min(remaining)))

    def _due_tables(
        self,
        pending: dict[str, list[tuple[Any, ...]]],
        pending_since: dict[str, float],
        *,
        stopping: bool,
    ) -> list[str]:
        due: list[str] = []
        now = time.monotonic()
        for table_name in (
            "price_data",
            "runtime_metrics",
            "event_log",
            "ingestion_pipeline_health",
            "price_provider_health",
            "weather_provider_health",
            "data_source_logs",
            "feature_data",
            "model_predictions",
            "model_registry",
            "predictions",
            "trade_outcomes",
        ):
            rows = pending.get(table_name) or []
            if not rows:
                continue
            age_s = max(0.0, now - pending_since.get(table_name, now))
            if stopping or len(rows) >= int(self._config.batch_size) or age_s >= float(self._config.flush_interval_s):
                due.append(table_name)
        return due

    async def _flush_with_retry(self, table_name: str, rows: list[tuple[Any, ...]]) -> bool:
        if not rows:
            return True
        sql = self._insert_sql(table_name)
        self._set_inflight(len(rows))
        try:
            for attempt in range(1, int(self._config.retry_attempts) + 1):
                try:
                    await self._ensure_schema()
                    pool = await self._ensure_pool()
                    async with pool.acquire() as conn:
                        async with conn.transaction():
                            await conn.executemany(sql, rows)
                    self._note_flush_success(table_name, len(rows))
                    return True
                except Exception as exc:
                    self._record_error(exc)
                    self._note_retry()
                    if attempt >= int(self._config.retry_attempts):
                        self._note_flush_failure(table_name, len(rows))
                        LOGGER.warning(
                            "timescale flush failed table=%s rows=%s attempts=%s error=%s",
                            table_name,
                            len(rows),
                            attempt,
                            exc,
                        )
                        await self._reset_pool()
                        await asyncio.sleep(min(self._config.retry_max_s, self._config.flush_interval_s))
                        return False
                    await self._reset_pool()
                    delay_s = min(
                        float(self._config.retry_max_s),
                        float(self._config.retry_base_s) * (2 ** (attempt - 1)),
                    )
                    delay_s += random.uniform(0.0, min(0.25, float(self._config.retry_base_s)))
                    await asyncio.sleep(delay_s)
        finally:
            self._clear_inflight(len(rows))

    def _insert_sql(self, table_name: str) -> str:
        if table_name == "runtime_metrics":
            return (
                f'INSERT INTO {self._table_ref("runtime_metrics")}(sqlite_rowid, "time", metric, value_num, value_text, tags_json) '
                f'VALUES($1, $2, $3, $4, $5, $6::jsonb) '
                f'ON CONFLICT (sqlite_rowid, "time") DO UPDATE SET '
                f'metric = EXCLUDED.metric, '
                f'value_num = EXCLUDED.value_num, '
                f'value_text = EXCLUDED.value_text, '
                f'tags_json = EXCLUDED.tags_json'
            )
        if table_name == "event_log":
            return (
                f'INSERT INTO {self._table_ref("event_log")}(sqlite_rowid, "time", event_type, event_source, event_version, entity_type, entity_id, correlation_id, payload_json) '
                f'VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb) '
                f'ON CONFLICT (sqlite_rowid, "time") DO UPDATE SET '
                f'event_type = EXCLUDED.event_type, '
                f'event_source = EXCLUDED.event_source, '
                f'event_version = EXCLUDED.event_version, '
                f'entity_type = EXCLUDED.entity_type, '
                f'entity_id = EXCLUDED.entity_id, '
                f'correlation_id = EXCLUDED.correlation_id, '
                f'payload_json = EXCLUDED.payload_json'
            )
        if table_name == "ingestion_pipeline_health":
            return (
                f'INSERT INTO {self._table_ref("ingestion_pipeline_health")}(sqlite_rowid, "time", pipeline, ok, latency_ms, raw_rows, event_rows, last_ingested_ts_ms, error, meta_json) '
                f'VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb) '
                f'ON CONFLICT (sqlite_rowid, "time") DO UPDATE SET '
                f'pipeline = EXCLUDED.pipeline, '
                f'ok = EXCLUDED.ok, '
                f'latency_ms = EXCLUDED.latency_ms, '
                f'raw_rows = EXCLUDED.raw_rows, '
                f'event_rows = EXCLUDED.event_rows, '
                f'last_ingested_ts_ms = EXCLUDED.last_ingested_ts_ms, '
                f'error = EXCLUDED.error, '
                f'meta_json = EXCLUDED.meta_json'
            )
        if table_name == "price_provider_health":
            return (
                f'INSERT INTO {self._table_ref("price_provider_health")}(sqlite_rowid, "time", provider, ok, latency_ms, n_symbols, error, last_success_ts_ms, error_count) '
                f'VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9) '
                f'ON CONFLICT (sqlite_rowid, "time") DO UPDATE SET '
                f'provider = EXCLUDED.provider, '
                f'ok = EXCLUDED.ok, '
                f'latency_ms = EXCLUDED.latency_ms, '
                f'n_symbols = EXCLUDED.n_symbols, '
                f'error = EXCLUDED.error, '
                f'last_success_ts_ms = EXCLUDED.last_success_ts_ms, '
                f'error_count = EXCLUDED.error_count'
            )
        if table_name == "weather_provider_health":
            return (
                f'INSERT INTO {self._table_ref("weather_provider_health")}(sqlite_rowid, "time", provider, ok, latency_ms, error) '
                f'VALUES($1, $2, $3, $4, $5, $6) '
                f'ON CONFLICT (sqlite_rowid, "time") DO UPDATE SET '
                f'provider = EXCLUDED.provider, '
                f'ok = EXCLUDED.ok, '
                f'latency_ms = EXCLUDED.latency_ms, '
                f'error = EXCLUDED.error'
            )
        if table_name == "data_source_logs":
            return (
                f'INSERT INTO {self._table_ref("data_source_logs")}(sqlite_rowid, "time", source_key, level, event_type, message, detail_json) '
                f'VALUES($1, $2, $3, $4, $5, $6, $7::jsonb) '
                f'ON CONFLICT (sqlite_rowid, "time") DO UPDATE SET '
                f'source_key = EXCLUDED.source_key, '
                f'level = EXCLUDED.level, '
                f'event_type = EXCLUDED.event_type, '
                f'message = EXCLUDED.message, '
                f'detail_json = EXCLUDED.detail_json'
            )
        if table_name == "price_data":
            return (
                f'INSERT INTO {self._table_ref("price_data")}(symbol, "timestamp", "open", "high", "low", "close", volume) '
                f'VALUES($1, $2, $3, $4, $5, $6, $7) '
                f'ON CONFLICT (symbol, "timestamp") DO UPDATE SET '
                f'"open" = EXCLUDED."open", '
                f'"high" = EXCLUDED."high", '
                f'"low" = EXCLUDED."low", '
                f'"close" = EXCLUDED."close", '
                f'volume = EXCLUDED.volume'
            )
        if table_name == "feature_data":
            return (
                f'INSERT INTO {self._table_ref("feature_data")}(symbol, "timestamp", feature_vector) '
                f'VALUES($1, $2, $3::jsonb) '
                f'ON CONFLICT (symbol, "timestamp") DO UPDATE SET '
                f'feature_vector = EXCLUDED.feature_vector'
            )
        if table_name == "model_predictions":
            return (
                f'INSERT INTO {self._table_ref("model_predictions")}(model_id, symbol, "timestamp", prediction, confidence) '
                f'VALUES($1, $2, $3, $4, $5) '
                f'ON CONFLICT (model_id, symbol, "timestamp") DO UPDATE SET '
                f'prediction = EXCLUDED.prediction, '
                f'confidence = EXCLUDED.confidence'
            )
        if table_name == "model_registry":
            return (
                f'INSERT INTO {self._table_ref("model_registry")}(model_name, version, created_at, metadata) '
                f'VALUES($1, $2, $3, $4::jsonb) '
                f'ON CONFLICT (model_name, version) DO UPDATE SET '
                f'created_at = LEAST({self._table_ref("model_registry")}.created_at, EXCLUDED.created_at), '
                f'metadata = EXCLUDED.metadata'
            )
        if table_name == "predictions":
            return (
                f'INSERT INTO {self._table_ref("predictions")}("time", symbol, model_name, model_version, prediction, confidence, features_version, model_id, event_id, horizon_s, prediction_id, source_alert_id, tracking_source, metadata) '
                f'VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14::jsonb) '
                f'ON CONFLICT (model_name, model_version, symbol, "time") DO UPDATE SET '
                f'prediction = EXCLUDED.prediction, '
                f'confidence = EXCLUDED.confidence, '
                f'features_version = EXCLUDED.features_version, '
                f'model_id = EXCLUDED.model_id, '
                f'event_id = EXCLUDED.event_id, '
                f'horizon_s = EXCLUDED.horizon_s, '
                f'prediction_id = EXCLUDED.prediction_id, '
                f'source_alert_id = EXCLUDED.source_alert_id, '
                f'tracking_source = EXCLUDED.tracking_source, '
                f'metadata = EXCLUDED.metadata'
            )
        if table_name == "trade_outcomes":
            return (
                f'INSERT INTO {self._table_ref("trade_outcomes")}(trade_id, "timestamp", pnl, outcome) '
                f'VALUES($1, $2, $3, $4) '
                f'ON CONFLICT (trade_id, "timestamp") DO UPDATE SET '
                f'pnl = EXCLUDED.pnl, '
                f'outcome = EXCLUDED.outcome'
            )
        raise ValueError(f"unsupported_timescale_table:{table_name}")

    def _table_ref(self, table_name: str) -> str:
        return f"{_quote_ident(self._config.schema_name)}.{_quote_ident(table_name)}"

    def _prepare_rows(self, table_name: str, rows: Iterable[Mapping[str, Any]]) -> list[tuple[Any, ...]]:
        prepared: list[tuple[Any, ...]] = []
        for row in rows:
            if table_name == "runtime_metrics":
                prepared.append(
                    (
                        int(_coalesce(row, "sqlite_rowid", "rowid", "id")),
                        _normalize_timestamp(_coalesce(row, "time", "timestamp", "ts", "ts_ms"), field="time"),
                        _normalize_text(_coalesce(row, "metric"), field="metric"),
                        (float(_coalesce(row, "value_num")) if _coalesce(row, "value_num") not in (None, "") else None),
                        (str(_coalesce(row, "value_text")) if _coalesce(row, "value_text") not in (None, "") else None),
                        _normalize_jsonb_or_empty(_coalesce(row, "tags_json", "tags", "payload")),
                    )
                )
                continue
            if table_name == "event_log":
                prepared.append(
                    (
                        int(_coalesce(row, "sqlite_rowid", "rowid", "id")),
                        _normalize_timestamp(_coalesce(row, "time", "timestamp", "ts", "ts_ms"), field="time"),
                        _normalize_text(_coalesce(row, "event_type"), field="event_type"),
                        _normalize_text(_coalesce(row, "event_source"), field="event_source"),
                        int(_coalesce(row, "event_version", 1) or 1),
                        (str(_coalesce(row, "entity_type")) if _coalesce(row, "entity_type") not in (None, "") else None),
                        (str(_coalesce(row, "entity_id")) if _coalesce(row, "entity_id") not in (None, "") else None),
                        (str(_coalesce(row, "correlation_id")) if _coalesce(row, "correlation_id") not in (None, "") else None),
                        _normalize_jsonb_or_empty(_coalesce(row, "payload_json", "payload")),
                    )
                )
                continue
            if table_name == "ingestion_pipeline_health":
                prepared.append(
                    (
                        int(_coalesce(row, "sqlite_rowid", "rowid")),
                        _normalize_timestamp(_coalesce(row, "time", "timestamp", "ts", "ts_ms"), field="time"),
                        _normalize_text(_coalesce(row, "pipeline"), field="pipeline"),
                        1 if bool(int(_coalesce(row, "ok", 0) or 0)) else 0,
                        (int(_coalesce(row, "latency_ms")) if _coalesce(row, "latency_ms") not in (None, "") else None),
                        int(_coalesce(row, "raw_rows", 0) or 0),
                        int(_coalesce(row, "event_rows", 0) or 0),
                        (
                            int(_coalesce(row, "last_ingested_ts_ms"))
                            if _coalesce(row, "last_ingested_ts_ms") not in (None, "")
                            else None
                        ),
                        (str(_coalesce(row, "error")) if _coalesce(row, "error") not in (None, "") else None),
                        _normalize_jsonb_or_empty(_coalesce(row, "meta_json", "meta", "payload")),
                    )
                )
                continue
            if table_name == "price_provider_health":
                prepared.append(
                    (
                        int(_coalesce(row, "sqlite_rowid", "rowid")),
                        _normalize_timestamp(_coalesce(row, "time", "timestamp", "ts", "ts_ms"), field="time"),
                        _normalize_text(_coalesce(row, "provider"), field="provider"),
                        1 if bool(int(_coalesce(row, "ok", 0) or 0)) else 0,
                        (int(_coalesce(row, "latency_ms")) if _coalesce(row, "latency_ms") not in (None, "") else None),
                        (int(_coalesce(row, "n_symbols")) if _coalesce(row, "n_symbols") not in (None, "") else None),
                        (str(_coalesce(row, "error")) if _coalesce(row, "error") not in (None, "") else None),
                        (
                            int(_coalesce(row, "last_success_ts_ms"))
                            if _coalesce(row, "last_success_ts_ms") not in (None, "")
                            else None
                        ),
                        (int(_coalesce(row, "error_count")) if _coalesce(row, "error_count") not in (None, "") else None),
                    )
                )
                continue
            if table_name == "weather_provider_health":
                prepared.append(
                    (
                        int(_coalesce(row, "sqlite_rowid", "rowid")),
                        _normalize_timestamp(_coalesce(row, "time", "timestamp", "ts", "ts_ms"), field="time"),
                        _normalize_text(_coalesce(row, "provider"), field="provider"),
                        1 if bool(int(_coalesce(row, "ok", 0) or 0)) else 0,
                        (int(_coalesce(row, "latency_ms")) if _coalesce(row, "latency_ms") not in (None, "") else None),
                        (str(_coalesce(row, "error")) if _coalesce(row, "error") not in (None, "") else None),
                    )
                )
                continue
            if table_name == "data_source_logs":
                prepared.append(
                    (
                        int(_coalesce(row, "sqlite_rowid", "rowid", "id")),
                        _normalize_timestamp(_coalesce(row, "time", "timestamp", "ts", "ts_ms"), field="time"),
                        _normalize_text(_coalesce(row, "source_key"), field="source_key"),
                        _normalize_text(_coalesce(row, "level"), field="level"),
                        _normalize_text(_coalesce(row, "event_type"), field="event_type"),
                        (str(_coalesce(row, "message")) if _coalesce(row, "message") not in (None, "") else None),
                        _normalize_jsonb_or_empty(_coalesce(row, "detail_json", "detail", "payload")),
                    )
                )
                continue
            if table_name == "price_data":
                prepared.append(
                    (
                        _normalize_text(_coalesce(row, "symbol"), field="symbol"),
                        _normalize_timestamp(_coalesce(row, "timestamp", "ts", "ts_ms"), field="timestamp"),
                        _normalize_float(_coalesce(row, "open", "o"), field="open"),
                        _normalize_float(_coalesce(row, "high", "h"), field="high"),
                        _normalize_float(_coalesce(row, "low", "l"), field="low"),
                        _normalize_float(_coalesce(row, "close", "c"), field="close"),
                        _normalize_float(_coalesce(row, "volume", "v"), field="volume"),
                    )
                )
                continue
            if table_name == "feature_data":
                prepared.append(
                    (
                        _normalize_text(_coalesce(row, "symbol"), field="symbol"),
                        _normalize_timestamp(_coalesce(row, "timestamp", "ts", "ts_ms"), field="timestamp"),
                        _normalize_jsonb(
                            _coalesce(row, "feature_vector", "features", "feature_json", "payload"),
                            field="feature_vector",
                        ),
                    )
                )
                continue
            if table_name == "model_predictions":
                prepared.append(
                    (
                        _normalize_text(_coalesce(row, "model_id"), field="model_id"),
                        _normalize_text(_coalesce(row, "symbol"), field="symbol"),
                        _normalize_timestamp(_coalesce(row, "timestamp", "ts", "ts_ms"), field="timestamp"),
                        _normalize_float(_coalesce(row, "prediction", "value", "score"), field="prediction"),
                        _normalize_float(_coalesce(row, "confidence"), field="confidence"),
                    )
                )
                continue
            if table_name == "model_registry":
                prepared.append(
                    (
                        _normalize_text(_coalesce(row, "model_name", "name"), field="model_name"),
                        _normalize_text(_coalesce(row, "version", "model_version"), field="version"),
                        _normalize_timestamp(_coalesce(row, "created_at", "timestamp", "ts", "ts_ms"), field="created_at"),
                        _normalize_jsonb(_coalesce(row, "metadata", "metadata_json", "payload"), field="metadata"),
                    )
                )
                continue
            if table_name == "predictions":
                prepared.append(
                    (
                        _normalize_timestamp(_coalesce(row, "time", "timestamp", "ts", "ts_ms"), field="time"),
                        _normalize_text(_coalesce(row, "symbol"), field="symbol"),
                        _normalize_text(_coalesce(row, "model_name", "name"), field="model_name"),
                        _normalize_text(_coalesce(row, "model_version", "version"), field="model_version"),
                        _normalize_float(_coalesce(row, "prediction", "value", "score"), field="prediction"),
                        _normalize_float(_coalesce(row, "confidence"), field="confidence"),
                        _normalize_text(
                            _coalesce(row, "features_version", "feature_set_tag", "features_tag"),
                            field="features_version",
                        ),
                        (
                            _normalize_text(_coalesce(row, "model_id"), field="model_id")
                            if _coalesce(row, "model_id") not in (None, "")
                            else None
                        ),
                        (int(_coalesce(row, "event_id")) if _coalesce(row, "event_id") not in (None, "") else None),
                        (int(_coalesce(row, "horizon_s")) if _coalesce(row, "horizon_s") not in (None, "") else None),
                        (
                            int(_coalesce(row, "prediction_id"))
                            if _coalesce(row, "prediction_id") not in (None, "")
                            else None
                        ),
                        (
                            int(_coalesce(row, "source_alert_id"))
                            if _coalesce(row, "source_alert_id") not in (None, "")
                            else None
                        ),
                        (
                            _normalize_text(_coalesce(row, "tracking_source"), field="tracking_source")
                            if _coalesce(row, "tracking_source") not in (None, "")
                            else None
                        ),
                        _normalize_jsonb(_coalesce(row, "metadata", "metadata_json", "payload", "extra"), field="metadata"),
                    )
                )
                continue
            if table_name == "trade_outcomes":
                prepared.append(
                    (
                        _normalize_text(_coalesce(row, "trade_id"), field="trade_id"),
                        _normalize_timestamp(_coalesce(row, "timestamp", "ts", "ts_ms"), field="timestamp"),
                        _normalize_float(_coalesce(row, "pnl"), field="pnl"),
                        _normalize_text(_coalesce(row, "outcome"), field="outcome"),
                    )
                )
                continue
            raise ValueError(f"unsupported_timescale_table:{table_name}")
        return prepared

    def _enqueue(self, table_name: str, rows: Iterable[Mapping[str, Any]], *, timeout_s: float | None = None) -> int:
        rows_list = list(rows)
        if not rows_list:
            return 0
        if not self.enabled:
            return 0
        self.start()
        prepared_rows = self._prepare_rows(table_name, rows_list)
        if not prepared_rows:
            return 0
        loop = self._loop
        if loop is None:
            raise RuntimeError("timescale_event_loop_unavailable")
        deadline_s = float(timeout_s if timeout_s is not None else self._config.backpressure_timeout_s)
        total_rows = 0
        for chunk in _chunked(prepared_rows, self._config.batch_size):
            envelope = _WriteEnvelope(
                table=table_name,
                rows=chunk,
                row_count=len(chunk),
                enqueued_at=time.monotonic(),
            )
            future = asyncio.run_coroutine_threadsafe(self._async_enqueue(envelope), loop)
            try:
                future.result(timeout=deadline_s)
            except concurrent.futures.TimeoutError as exc:
                future.cancel()
                self._note_backpressure()
                raise TimescaleBackpressureError(
                    f"timescale_queue_backpressure_timeout:{table_name}:{deadline_s}s"
                ) from exc
            total_rows += len(chunk)
            self._note_enqueued(table_name, len(chunk))
        return total_rows

    async def _async_enqueue(self, envelope: _WriteEnvelope) -> None:
        if self._queue is None:
            raise RuntimeError("timescale_queue_unavailable")
        await self._queue.put(envelope)

    def _note_enqueued(self, table_name: str, row_count: int) -> None:
        with self._metrics_lock:
            self._metrics["enqueued_rows"] = int(self._metrics.get("enqueued_rows") or 0) + int(row_count)
            self._metrics["buffered_rows"] = int(self._metrics.get("buffered_rows") or 0) + int(row_count)
            table_stats = dict(self._metrics.get("table_stats") or {})
            table = dict(table_stats.get(table_name) or {})
            table["enqueued_rows"] = int(table.get("enqueued_rows") or 0) + int(row_count)
            table_stats[table_name] = table
            self._metrics["table_stats"] = table_stats

    def _note_flush_success(self, table_name: str, row_count: int) -> None:
        with self._metrics_lock:
            self._metrics["backpressure_active"] = False
            self._metrics["buffered_rows"] = max(0, int(self._metrics.get("buffered_rows") or 0) - int(row_count))
            self._metrics["consecutive_flush_failures"] = 0
            self._metrics["flushed_batches"] = int(self._metrics.get("flushed_batches") or 0) + 1
            self._metrics["flushed_rows"] = int(self._metrics.get("flushed_rows") or 0) + int(row_count)
            self._metrics["last_flush_ts_ms"] = _now_ms()
            table_stats = dict(self._metrics.get("table_stats") or {})
            table = dict(table_stats.get(table_name) or {})
            table["flushed_rows"] = int(table.get("flushed_rows") or 0) + int(row_count)
            table_stats[table_name] = table
            self._metrics["table_stats"] = table_stats

    def _set_inflight(self, row_count: int) -> None:
        with self._metrics_lock:
            self._metrics["inflight_rows"] = int(self._metrics.get("inflight_rows") or 0) + int(row_count)

    def _clear_inflight(self, row_count: int) -> None:
        with self._metrics_lock:
            self._metrics["inflight_rows"] = max(0, int(self._metrics.get("inflight_rows") or 0) - int(row_count))

    def _note_retry(self) -> None:
        with self._metrics_lock:
            self._metrics["retry_count"] = int(self._metrics.get("retry_count") or 0) + 1

    def _note_backpressure(self) -> None:
        with self._metrics_lock:
            self._metrics["backpressure_active"] = True
            self._metrics["backpressure_count"] = int(self._metrics.get("backpressure_count") or 0) + 1
            self._metrics["enqueue_failure_count"] = int(self._metrics.get("enqueue_failure_count") or 0) + 1
            self._metrics["last_backpressure_ts_ms"] = _now_ms()

    def _note_flush_failure(self, table_name: str, row_count: int) -> None:
        with self._metrics_lock:
            self._metrics["consecutive_flush_failures"] = int(
                self._metrics.get("consecutive_flush_failures") or 0
            ) + 1
            self._metrics["flush_failure_count"] = int(self._metrics.get("flush_failure_count") or 0) + 1
            self._metrics["last_flush_failure_ts_ms"] = _now_ms()
            table_stats = dict(self._metrics.get("table_stats") or {})
            table = dict(table_stats.get(table_name) or {})
            table["flush_failure_count"] = int(table.get("flush_failure_count") or 0) + 1
            table["last_flush_failure_rows"] = int(row_count)
            table_stats[table_name] = table
            self._metrics["table_stats"] = table_stats

    def _record_error(self, error: Exception) -> None:
        self._last_error = f"{type(error).__name__}: {error}"
        self._last_error_ts_ms = _now_ms()


def get_timescale_client() -> TimescaleClient:
    """Return the process-wide Timescale client singleton."""
    global _CLIENT
    client = _CLIENT
    if client is not None:
        return client
    with _CLIENT_LOCK:
        client = _CLIENT
        if client is None:
            client = TimescaleClient()
            _CLIENT = client
        return client


def init_timescale_client() -> dict[str, Any]:
    """Start the process-wide Timescale client and return its snapshot."""
    client = get_timescale_client()
    snapshot = client.start()
    if client.enabled:
        client._schedule_schema_warmup()
    return snapshot


def get_timescale_snapshot() -> dict[str, Any]:
    """Return a diagnostic snapshot of the process-wide Timescale client."""
    return get_timescale_client().get_snapshot()


def shutdown_timescale_client(timeout_s: float | None = None) -> dict[str, Any]:
    """Stop the process-wide Timescale client."""
    return get_timescale_client().close(timeout_s=timeout_s)
