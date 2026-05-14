"""Versioned TimescaleDB feature store for schema-driven feature snapshots.

This module is intentionally additive to the existing feature generation paths:

- reads are optional and explicit
- writes are fire-and-forget for live feature generation
- failures degrade open and never block the trading path
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import queue
import random
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

try:
    import asyncpg
except Exception:  # pragma: no cover - optional at runtime
    asyncpg = None  # type: ignore[assignment]

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.config import (
    FEATURE_STORE_ENABLED as GLOBAL_FEATURE_STORE_ENABLED,
    FEATURE_STORE_INIT_ON_STARTUP,
    FEATURE_STORE_VERSION as GLOBAL_FEATURE_STORE_VERSION,
)

LOG = get_logger("engine.strategy.feature_store")
_WARNED_NONFATAL_KEYS: set[str] = set()
_STORE_LOCK = threading.Lock()
_STORE: "FeatureStore | None" = None

FEATURE_STORE_VERSION = max(1, int(GLOBAL_FEATURE_STORE_VERSION))
FEATURE_STORE_TABLE = "feature_store"
FEATURE_STORE_SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE TABLE IF NOT EXISTS {table_ref} (
  "time" TIMESTAMPTZ NOT NULL,
  symbol TEXT NOT NULL,
  feature_version INTEGER NOT NULL,
  features JSONB NOT NULL,
  PRIMARY KEY(symbol, "time", feature_version)
);
SELECT create_hypertable(
  '{relation_name}'::regclass,
  'time',
  if_not_exists => TRUE,
  migrate_data => TRUE
);
CREATE INDEX IF NOT EXISTS idx_feature_store_symbol_time_desc
  ON {table_ref}(symbol, "time" DESC);
CREATE INDEX IF NOT EXISTS idx_feature_store_feature_version
  ON {table_ref}(feature_version);
"""


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.strategy.feature_store",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(str(once_key))


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


def _quote_ident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def _normalize_timestamp(value: Any) -> datetime:
    if value is None or value == "":
        raise ValueError("missing_timestamp")
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
        raise ValueError("missing_timestamp")
    if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
        return _normalize_timestamp(int(text))
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def _sanitize_json_value(value: Any, *, depth: int = 0) -> Any:
    if depth >= 8:
        return None
    if value is None:
        return None
    if isinstance(value, bool):
        return bool(value)
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        return _safe_float(value, 0.0)
    if isinstance(value, str):
        return str(value)
    if isinstance(value, datetime):
        return _normalize_timestamp(value).isoformat()
    if isinstance(value, Mapping):
        out: dict[str, Any] = {}
        for key, item in value.items():
            name = str(key or "").strip()
            if not name:
                continue
            out[name] = _sanitize_json_value(item, depth=depth + 1)
        return out
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_sanitize_json_value(item, depth=depth + 1) for item in value]
    try:
        return str(value)
    except Exception:
        return None


def _sanitize_feature_mapping(feature_dict: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(feature_dict, Mapping):
        return {}
    sanitized: dict[str, Any] = {}
    for key, value in feature_dict.items():
        name = str(key or "").strip()
        if not name:
            continue
        sanitized[name] = _sanitize_json_value(value)
    return sanitized


def _json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)


@dataclass(frozen=True)
class FeatureStoreConfig:
    """Configure the versioned strategy feature-store sidecar."""

    enabled: bool
    dsn: str
    schema_name: str
    batch_size: int
    flush_interval_s: float
    queue_maxsize: int
    enqueue_timeout_s: float
    retry_attempts: int
    retry_base_s: float
    retry_max_s: float
    connect_timeout_s: float
    command_timeout_s: float
    application_name: str

    @classmethod
    def from_env(cls) -> "FeatureStoreConfig":
        """Build the feature-store configuration from environment variables."""
        dsn = str(
            os.environ.get("FEATURE_STORE_DSN")
            or os.environ.get("TIMESCALE_DSN")
            or os.environ.get("TIMESCALE_URL")
            or os.environ.get("TIMESCALE_DATABASE_URL")
            or ""
        ).strip()
        return cls(
            enabled=_env_bool("FEATURE_STORE_ENABLED", default=bool(GLOBAL_FEATURE_STORE_ENABLED or bool(dsn))),
            dsn=dsn,
            schema_name=str(
                os.environ.get("FEATURE_STORE_SCHEMA")
                or os.environ.get("TIMESCALE_SCHEMA")
                or "public"
            ).strip()
            or "public",
            batch_size=max(1, _env_int("FEATURE_STORE_BATCH_SIZE", 256)),
            flush_interval_s=max(0.05, _env_float("FEATURE_STORE_FLUSH_INTERVAL_S", 0.5)),
            queue_maxsize=max(32, _env_int("FEATURE_STORE_QUEUE_MAXSIZE", 4096)),
            enqueue_timeout_s=max(0.05, _env_float("FEATURE_STORE_ENQUEUE_TIMEOUT_S", 5.0)),
            retry_attempts=max(1, _env_int("FEATURE_STORE_RETRY_ATTEMPTS", 3)),
            retry_base_s=max(0.05, _env_float("FEATURE_STORE_RETRY_BASE_S", 0.25)),
            retry_max_s=max(0.1, _env_float("FEATURE_STORE_RETRY_MAX_S", 2.0)),
            connect_timeout_s=max(0.1, _env_float("FEATURE_STORE_CONNECT_TIMEOUT_S", 5.0)),
            command_timeout_s=max(1.0, _env_float("FEATURE_STORE_COMMAND_TIMEOUT_S", 30.0)),
            application_name=str(
                os.environ.get("FEATURE_STORE_APPLICATION_NAME")
                or os.environ.get("TIMESCALE_APPLICATION_NAME")
                or "trading-system-feature-store"
            ).strip()
            or "trading-system-feature-store",
        )


@dataclass(frozen=True)
class _QueuedFeatureWrite:
    symbol: str
    timestamp: datetime
    feature_version: int
    features_json: str


class FeatureStore:
    """Async writer and read facade for versioned feature snapshots."""

    def __init__(self, config: FeatureStoreConfig | None = None) -> None:
        self._config = config or FeatureStoreConfig.from_env()
        self._metrics_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._queue: "queue.Queue[_QueuedFeatureWrite] | None" = None
        self._stop_event: threading.Event | None = None
        self._schema_ready = False
        self._last_error: str | None = None
        self._last_error_ts_ms = 0
        self._metrics: dict[str, Any] = {
            "consecutive_flush_failures": 0,
            "enqueued_rows": 0,
            "flush_drop_count": 0,
            "flush_failure_count": 0,
            "flushed_batches": 0,
            "flushed_rows": 0,
            "last_enqueue_failure_ts_ms": 0,
            "last_flush_failure_ts_ms": 0,
            "last_flush_ts_ms": 0,
            "queue_backpressure_active": False,
            "queue_rejection_count": 0,
            "queue_timeout_count": 0,
        }

    @property
    def enabled(self) -> bool:
        return bool(self._config.enabled and self._config.dsn and asyncpg is not None)

    def start(self) -> bool:
        if not self.enabled:
            return False
        with self._state_lock:
            if self._thread is not None and self._thread.is_alive():
                return True
            if self._queue is None:
                self._queue = queue.Queue(maxsize=int(self._config.queue_maxsize))
            self._stop_event = threading.Event()
            thread = threading.Thread(
                target=self._worker_main,
                name="feature-store-writer",
                daemon=True,
            )
            self._thread = thread
            thread.start()
        return True

    def get_snapshot(self) -> dict[str, Any]:
        queue_depth = 0
        started = False
        with self._metrics_lock:
            metrics = json.loads(json.dumps(self._metrics))
        with self._state_lock:
            started = bool(self._thread is not None and self._thread.is_alive())
            if self._queue is not None:
                try:
                    queue_depth = int(self._queue.qsize())
                except Exception:
                    queue_depth = 0
        degraded_reasons: list[str] = []
        if self.enabled and not started and self._last_error:
            degraded_reasons.append("writer_stopped")
        if bool(metrics.get("queue_backpressure_active")):
            degraded_reasons.append("queue_backpressure")
        if int(metrics.get("consecutive_flush_failures") or 0) > 0:
            degraded_reasons.append("flush_failures")
        if self.enabled and queue_depth >= int(self._config.queue_maxsize):
            degraded_reasons.append("queue_full")
        degraded = bool(degraded_reasons)
        return {
            "ok": bool((not self.enabled) or (started and self._schema_ready and not degraded)),
            "degraded": bool(degraded),
            "degraded_reasons": degraded_reasons,
            "enabled": bool(self.enabled),
            "dsn_configured": bool(self._config.dsn),
            "driver_available": asyncpg is not None,
            "started": bool(started),
            "schema_ready": bool(self._schema_ready),
            "queue_depth": int(queue_depth),
            "queue_maxsize": int(self._config.queue_maxsize),
            "enqueue_timeout_s": float(self._config.enqueue_timeout_s),
            "last_error": self._last_error,
            "last_error_ts_ms": int(self._last_error_ts_ms or 0),
            "feature_store_version": int(FEATURE_STORE_VERSION),
            "metrics": metrics,
            "ts_ms": int(time.time() * 1000),
        }

    def close(self, timeout_s: float | None = None) -> None:
        thread: threading.Thread | None = None
        stop_event: threading.Event | None = None
        with self._state_lock:
            thread = self._thread
            stop_event = self._stop_event
        if thread is None or stop_event is None:
            return
        stop_event.set()
        thread.join(timeout=float(timeout_s or max(1.0, self._config.command_timeout_s)))

    def _record_error(self, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
        self._last_error = f"{type(error).__name__}: {error}"
        self._last_error_ts_ms = int(time.time() * 1000)
        _warn_nonfatal(
            "FEATURE_STORE_ERROR",
            error,
            once_key=once_key,
            last_error=str(self._last_error),
            **extra,
        )

    def _prepare_row(
        self,
        symbol: str,
        timestamp: Any,
        feature_dict: Mapping[str, Any] | None,
        version: int | None,
    ) -> _QueuedFeatureWrite | None:
        symbol_key = _normalize_symbol(symbol)
        if not symbol_key:
            return None
        try:
            dt_value = _normalize_timestamp(timestamp)
        except Exception as exc:
            self._record_error(exc, once_key="feature_store_bad_timestamp", symbol=str(symbol_key))
            return None
        feature_version = max(1, int(version or FEATURE_STORE_VERSION))
        sanitized = _sanitize_feature_mapping(feature_dict)
        if not sanitized:
            return None
        return _QueuedFeatureWrite(
            symbol=str(symbol_key),
            timestamp=dt_value,
            feature_version=int(feature_version),
            features_json=_json_dumps(sanitized),
        )

    def schedule_write(
        self,
        symbol: str,
        timestamp: Any,
        feature_dict: Mapping[str, Any] | None,
        version: int | None = None,
    ) -> bool:
        row = self._prepare_row(symbol, timestamp, feature_dict, version)
        if row is None:
            return False
        if not self.start():
            return False
        if self._queue is None:
            return False
        try:
            self._queue.put_nowait(row)
            self._note_enqueue_success(1)
            return True
        except queue.Full as exc:
            self._note_queue_rejection(timed=False)
            self._record_error(
                exc,
                once_key="feature_store_queue_full",
                queue_maxsize=int(self._config.queue_maxsize),
            )
            return False

    async def write_features(
        self,
        symbol: str,
        timestamp: Any,
        feature_dict: Mapping[str, Any] | None,
        version: int | None = None,
    ) -> bool:
        row = self._prepare_row(symbol, timestamp, feature_dict, version)
        if row is None:
            return False
        if not self.start():
            return False
        if self._queue is None:
            return False
        try:
            await asyncio.to_thread(
                self._queue.put,
                row,
                True,
                float(self._config.enqueue_timeout_s),
            )
            self._note_enqueue_success(1)
            return True
        except queue.Full as exc:
            self._note_queue_rejection(timed=True)
            self._record_error(
                exc,
                once_key="feature_store_enqueue_timeout",
                queue_maxsize=int(self._config.queue_maxsize),
            )
            return False

    async def get_features(
        self,
        symbol: str,
        timestamp: Any,
        version: int | None = None,
    ) -> dict[str, Any] | None:
        if not self.enabled:
            return None
        symbol_key = _normalize_symbol(symbol)
        if not symbol_key:
            return None
        try:
            dt_value = _normalize_timestamp(timestamp)
        except Exception as exc:
            self._record_error(exc, once_key="feature_store_get_bad_timestamp", symbol=str(symbol_key))
            return None

        conn = None
        try:
            conn = await asyncpg.connect(  # type: ignore[union-attr]
                dsn=self._config.dsn,
                timeout=float(self._config.connect_timeout_s),
                command_timeout=float(self._config.command_timeout_s),
                server_settings={"application_name": self._config.application_name},
            )
            if version is None:
                row = await conn.fetchrow(
                    f"""
                    SELECT symbol, "time", feature_version, features
                    FROM {self._table_ref}
                    WHERE symbol = $1
                      AND "time" <= $2
                    ORDER BY "time" DESC, feature_version DESC
                    LIMIT 1
                    """,
                    str(symbol_key),
                    dt_value,
                )
            else:
                row = await conn.fetchrow(
                    f"""
                    SELECT symbol, "time", feature_version, features
                    FROM {self._table_ref}
                    WHERE symbol = $1
                      AND feature_version = $2
                      AND "time" <= $3
                    ORDER BY "time" DESC
                    LIMIT 1
                    """,
                    str(symbol_key),
                    int(max(1, int(version))),
                    dt_value,
                )
        except Exception as exc:
            self._record_error(
                exc,
                once_key="feature_store_get_failed",
                symbol=str(symbol_key),
                version=(None if version is None else int(version)),
            )
            return None
        finally:
            if conn is not None:
                try:
                    await conn.close()
                except Exception:
                    pass  # no-op-guard: allow best-effort cleanup

        if not row:
            return None
        raw_features = row["features"]
        if isinstance(raw_features, str):
            try:
                raw_features = json.loads(raw_features)
            except Exception:
                raw_features = {}
        if not isinstance(raw_features, Mapping):
            raw_features = {}
        row_time = row["time"]
        return {
            "symbol": _normalize_symbol(row["symbol"]),
            "timestamp": int(_normalize_timestamp(row_time).timestamp() * 1000),
            "version": int(row["feature_version"] or 0),
            "features": _sanitize_feature_mapping(dict(raw_features or {})),
        }

    async def ensure_ready(self) -> dict[str, Any]:
        if not self.enabled:
            return self.get_snapshot()
        self.start()
        conn = None
        try:
            conn = await asyncpg.connect(  # type: ignore[union-attr]
                dsn=self._config.dsn,
                timeout=float(self._config.connect_timeout_s),
                command_timeout=float(self._config.command_timeout_s),
                server_settings={"application_name": self._config.application_name},
            )
            await self._ensure_schema_on_connection(conn)
        except Exception as exc:
            self._record_error(exc, once_key="feature_store_ensure_ready_failed")
        finally:
            if conn is not None:
                try:
                    await conn.close()
                except Exception:
                    pass  # no-op-guard: allow best-effort cleanup
        return self.get_snapshot()

    def get_features_blocking(
        self,
        symbol: str,
        timestamp: Any,
        version: int | None = None,
    ) -> dict[str, Any] | None:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.get_features(symbol, timestamp, version=version))

        result: dict[str, Any] = {}
        error: dict[str, BaseException] = {}
        done = threading.Event()

        def _runner() -> None:
            try:
                result["value"] = asyncio.run(self.get_features(symbol, timestamp, version=version))
            except BaseException as exc:  # pragma: no cover - sync wrapper under active loop only
                error["error"] = exc
            finally:
                done.set()

        thread = threading.Thread(target=_runner, name="feature-store-sync-read", daemon=True)
        thread.start()
        done.wait()
        if "error" in error:
            raise error["error"]
        return result.get("value")

    def ensure_ready_blocking(self) -> dict[str, Any]:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return dict(asyncio.run(self.ensure_ready()))

        result: dict[str, Any] = {}
        error: dict[str, BaseException] = {}
        done = threading.Event()

        def _runner() -> None:
            try:
                result["value"] = asyncio.run(self.ensure_ready())
            except BaseException as exc:  # pragma: no cover - sync wrapper under active loop only
                error["error"] = exc
            finally:
                done.set()

        thread = threading.Thread(target=_runner, name="feature-store-sync-init", daemon=True)
        thread.start()
        done.wait()
        if "error" in error:
            raise error["error"]
        return dict(result.get("value") or {})

    @property
    def _table_ref(self) -> str:
        return f"{_quote_ident(self._config.schema_name)}.{_quote_ident(FEATURE_STORE_TABLE)}"

    @property
    def _relation_name(self) -> str:
        return f"{self._config.schema_name}.{FEATURE_STORE_TABLE}"

    async def _ensure_schema_on_connection(self, conn: Any) -> None:
        if self._schema_ready:
            return
        await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_ident(self._config.schema_name)}")
        for statement in (
            statement.strip()
            for statement in FEATURE_STORE_SCHEMA_SQL.format(
                table_ref=self._table_ref,
                relation_name=self._relation_name,
            ).split(";")
        ):
            if not statement:
                continue
            await conn.execute(statement)
        self._schema_ready = True

    async def _ensure_pool(self, pool: Any) -> Any:
        if pool is not None:
            return pool
        if asyncpg is None:
            raise RuntimeError("feature_store_asyncpg_unavailable")
        return await asyncpg.create_pool(  # type: ignore[union-attr]
            dsn=self._config.dsn,
            min_size=1,
            max_size=2,
            timeout=float(self._config.connect_timeout_s),
            command_timeout=float(self._config.command_timeout_s),
            server_settings={"application_name": self._config.application_name},
        )

    async def _flush_batch(self, pool: Any, rows: list[_QueuedFeatureWrite]) -> Any:
        if not rows:
            return pool
        last_error: BaseException | None = None
        for attempt in range(1, int(self._config.retry_attempts) + 1):
            try:
                pool = await self._ensure_pool(pool)
                async with pool.acquire() as conn:
                    await self._ensure_schema_on_connection(conn)
                    await conn.executemany(
                        f"""
                        INSERT INTO {self._table_ref} AS existing("time", symbol, feature_version, features)
                        VALUES($1, $2, $3, $4::jsonb)
                        ON CONFLICT (symbol, "time", feature_version) DO UPDATE
                        SET features = COALESCE(existing.features, '{{}}'::jsonb) || EXCLUDED.features
                        """,
                        [
                            (row.timestamp, row.symbol, row.feature_version, row.features_json)
                            for row in rows
                        ],
                    )
                self._note_flush_success(len(rows))
                return pool
            except Exception as exc:
                last_error = exc
                self._schema_ready = False
                self._record_error(
                    exc,
                    once_key="feature_store_flush_failed",
                    rows=int(len(rows)),
                    attempt=int(attempt),
                )
                if pool is not None:
                    try:
                        await pool.close()
                    except Exception:
                        pass  # no-op-guard: allow best-effort cleanup
                    pool = None
                if attempt >= int(self._config.retry_attempts):
                    break
                delay_s = min(
                    float(self._config.retry_max_s),
                    float(self._config.retry_base_s) * (2 ** (attempt - 1)),
                )
                delay_s += random.uniform(0.0, min(0.25, float(self._config.retry_base_s)))
                await asyncio.sleep(delay_s)
        if last_error is not None:
            self._note_flush_failure(len(rows), dropped=True)
            self._record_error(
                last_error,
                once_key="feature_store_flush_drop",
                rows=int(len(rows)),
            )
        return pool

    def _worker_main(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        pool = None
        batch: list[_QueuedFeatureWrite] = []
        last_flush = time.monotonic()
        try:
            while True:
                queue_ref = self._queue
                stop_event = self._stop_event
                if queue_ref is None or stop_event is None:
                    break
                timeout_s = float(self._config.flush_interval_s)
                if batch:
                    elapsed = max(0.0, time.monotonic() - last_flush)
                    timeout_s = max(0.01, float(self._config.flush_interval_s) - elapsed)
                try:
                    row = queue_ref.get(timeout=timeout_s)
                    batch.append(row)
                except queue.Empty:
                    row = None
                should_flush = bool(
                    batch
                    and (
                        len(batch) >= int(self._config.batch_size)
                        or (time.monotonic() - last_flush) >= float(self._config.flush_interval_s)
                        or (stop_event.is_set() and queue_ref.empty())
                    )
                )
                if should_flush:
                    pool = loop.run_until_complete(self._flush_batch(pool, batch))
                    batch = []
                    last_flush = time.monotonic()
                if stop_event.is_set() and queue_ref.empty() and not batch:
                    break
        except Exception as exc:
            self._record_error(exc, once_key="feature_store_worker_crash")
        finally:
            if batch:
                try:
                    pool = loop.run_until_complete(self._flush_batch(pool, batch))
                except Exception as exc:
                    self._record_error(exc, once_key="feature_store_final_flush_failed")
            if pool is not None:
                try:
                    loop.run_until_complete(pool.close())
                except Exception as exc:
                    self._record_error(exc, once_key="feature_store_pool_close_failed")
            with self._state_lock:
                self._thread = None
                self._queue = None
                self._stop_event = None
            asyncio.set_event_loop(None)
            loop.close()

    def _note_enqueue_success(self, row_count: int) -> None:
        with self._metrics_lock:
            self._metrics["enqueued_rows"] = int(self._metrics.get("enqueued_rows") or 0) + int(row_count)

    def _note_queue_rejection(self, *, timed: bool) -> None:
        with self._metrics_lock:
            self._metrics["queue_backpressure_active"] = True
            self._metrics["queue_rejection_count"] = int(self._metrics.get("queue_rejection_count") or 0) + 1
            if timed:
                self._metrics["queue_timeout_count"] = int(self._metrics.get("queue_timeout_count") or 0) + 1
            self._metrics["last_enqueue_failure_ts_ms"] = int(time.time() * 1000)

    def _note_flush_success(self, row_count: int) -> None:
        with self._metrics_lock:
            self._metrics["consecutive_flush_failures"] = 0
            self._metrics["flushed_batches"] = int(self._metrics.get("flushed_batches") or 0) + 1
            self._metrics["flushed_rows"] = int(self._metrics.get("flushed_rows") or 0) + int(row_count)
            self._metrics["last_flush_ts_ms"] = int(time.time() * 1000)
            self._metrics["queue_backpressure_active"] = False

    def _note_flush_failure(self, row_count: int, *, dropped: bool) -> None:
        with self._metrics_lock:
            self._metrics["consecutive_flush_failures"] = int(
                self._metrics.get("consecutive_flush_failures") or 0
            ) + 1
            self._metrics["flush_failure_count"] = int(self._metrics.get("flush_failure_count") or 0) + 1
            self._metrics["last_flush_failure_ts_ms"] = int(time.time() * 1000)
            if dropped:
                self._metrics["flush_drop_count"] = int(self._metrics.get("flush_drop_count") or 0) + int(row_count)


def get_feature_store() -> FeatureStore:
    """Return the process-wide strategy feature-store singleton."""
    global _STORE
    store = _STORE
    if store is not None:
        return store
    with _STORE_LOCK:
        store = _STORE
        if store is None:
            store = FeatureStore()
            _STORE = store
        return store


def enqueue_feature_write(
    symbol: str,
    timestamp: Any,
    feature_dict: Mapping[str, Any] | None,
    version: int | None = None,
) -> bool:
    """Queue one feature-snapshot write for asynchronous persistence."""
    try:
        return bool(
            get_feature_store().schedule_write(
                symbol=str(symbol),
                timestamp=timestamp,
                feature_dict=feature_dict,
                version=version,
            )
        )
    except Exception as exc:
        _warn_nonfatal(
            "FEATURE_STORE_ENQUEUE_FAILED",
            exc,
            once_key="feature_store_enqueue_failed",
            symbol=str(symbol or ""),
        )
        return False


def init_feature_store() -> dict[str, Any]:
    """Start the process-wide strategy feature store."""
    try:
        if not FEATURE_STORE_INIT_ON_STARTUP:
            return get_feature_store().get_snapshot()
        return dict(get_feature_store().ensure_ready_blocking())
    except Exception as exc:
        _warn_nonfatal(
            "FEATURE_STORE_INIT_FAILED",
            exc,
            once_key="feature_store_init_failed",
        )
        return {
            "ok": False,
            "enabled": False,
            "error": str(exc),
            "ts_ms": int(time.time() * 1000),
        }


def get_feature_store_snapshot() -> dict[str, Any]:
    """Return a diagnostic snapshot of the strategy feature store."""
    try:
        return dict(get_feature_store().get_snapshot())
    except Exception as exc:
        _warn_nonfatal(
            "FEATURE_STORE_SNAPSHOT_FAILED",
            exc,
            once_key="feature_store_snapshot_failed",
        )
        return {
            "ok": False,
            "enabled": False,
            "error": str(exc),
            "ts_ms": int(time.time() * 1000),
        }


def close_feature_store(timeout_s: float | None = None) -> None:
    """Close the process-wide strategy feature store."""
    try:
        get_feature_store().close(timeout_s=timeout_s)
    except Exception as exc:
        _warn_nonfatal(
            "FEATURE_STORE_CLOSE_FAILED",
            exc,
            once_key="feature_store_close_failed",
        )


__all__ = [
    "FEATURE_STORE_SCHEMA_SQL",
    "FEATURE_STORE_VERSION",
    "FeatureStore",
    "FeatureStoreConfig",
    "close_feature_store",
    "enqueue_feature_write",
    "get_feature_store_snapshot",
    "get_feature_store",
    "init_feature_store",
]
