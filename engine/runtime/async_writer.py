"""Durable SQLite-spooled async persistence for live price fanout."""

from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
from dataclasses import dataclass
from typing import Any

from engine.runtime.async_writer_spool import (
    CorruptPriceWriterSpoolRecord,
    PriceWriterSpoolFullError,
    PriceWriterSpoolRecord,
    SQLitePriceWriterSpool,
    default_spool_path,
)
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.ingestion_tuning import env_bool, tuned_float, tuned_int
from engine.runtime.logging import get_logger
from engine.runtime.metrics import emit_counter, emit_gauge, emit_timing
from engine.runtime.observability import backoff_delay_s, record_component_health
from engine.runtime.platform import default_local_log_dir
from engine.runtime.storage_pg_prices import get_price_storage

LOG = get_logger("runtime.async_writer")
_WRITER_LOCK = threading.Lock()
_WRITER: "AsyncPriceWriter | None" = None


def _env_bool(name: str, default: bool = False) -> bool:
    return env_bool(name, default=default)


def _json_default(value: Any) -> Any:
    try:
        return str(value)
    except Exception:
        return None


def _warn_nonfatal(code: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.runtime.async_writer",
        extra=dict(extra or {}) or None,
        persist=False,
    )


@dataclass(frozen=True)
class AsyncPriceWriterConfig:
    enabled: bool
    queue_maxsize: int
    batch_size: int
    flush_interval_s: float
    retry_attempts: int
    retry_base_s: float
    retry_max_s: float
    enqueue_timeout_s: float
    dead_letter_path: str
    high_watermark_ratio: float = 0.75
    shutdown_drain_max_s: float = 30.0
    spool_path: str = ""
    spool_max_bytes: int = 256 * 1024 * 1024
    spool_busy_timeout_ms: int = 50
    spool_synchronous: str = "FULL"

    @classmethod
    def from_env(cls) -> "AsyncPriceWriterConfig":
        enabled_default = bool(getattr(get_price_storage(), "enabled", False))
        dead_letter_path = str(
            os.environ.get("ASYNC_PRICE_WRITER_DEAD_LETTER_PATH")
            or os.path.join(str(default_local_log_dir().resolve()), "async_price_writer_dead_letter.jsonl")
        ).strip()
        enqueue_timeout_s = tuned_float("ASYNC_PRICE_WRITER_ENQUEUE_TIMEOUT_S", 0.05, 0.0, 5.0)
        return cls(
            enabled=_env_bool("ASYNC_PRICE_WRITER_ENABLED", default=enabled_default),
            queue_maxsize=tuned_int("ASYNC_PRICE_WRITER_QUEUE_MAXSIZE", 2048, 32, 32768),
            batch_size=tuned_int("ASYNC_PRICE_WRITER_BATCH_SIZE", 256, 1, 4096),
            flush_interval_s=tuned_float("ASYNC_PRICE_WRITER_FLUSH_INTERVAL_S", 0.5, 0.05, 5.0),
            retry_attempts=tuned_int("ASYNC_PRICE_WRITER_RETRY_ATTEMPTS", 4, 1, 10),
            retry_base_s=tuned_float("ASYNC_PRICE_WRITER_RETRY_BASE_S", 0.25, 0.01, 5.0),
            retry_max_s=tuned_float("ASYNC_PRICE_WRITER_RETRY_MAX_S", 5.0, 0.10, 30.0),
            enqueue_timeout_s=float(enqueue_timeout_s),
            dead_letter_path=dead_letter_path,
            high_watermark_ratio=tuned_float("ASYNC_PRICE_WRITER_HIGH_WATERMARK_RATIO", 0.75, 0.10, 1.0),
            shutdown_drain_max_s=tuned_float("ASYNC_PRICE_WRITER_SHUTDOWN_DRAIN_MAX_S", 30.0, 0.0, 300.0),
            spool_path=str(os.environ.get("ASYNC_PRICE_WRITER_SPOOL_PATH") or default_spool_path()).strip(),
            spool_max_bytes=tuned_int("ASYNC_PRICE_WRITER_SPOOL_MAX_BYTES", 268435456, 1048576, 8589934592),
            spool_busy_timeout_ms=tuned_int(
                "ASYNC_PRICE_WRITER_SPOOL_BUSY_TIMEOUT_MS",
                max(50, int(round(float(enqueue_timeout_s) * 1000.0))),
                10,
                60000,
            ),
            spool_synchronous=str(os.environ.get("ASYNC_PRICE_WRITER_SPOOL_SYNCHRONOUS") or "FULL").strip().upper(),
        )


@dataclass(frozen=True)
class PricePersistenceEnvelope:
    prices: tuple[dict[str, Any], ...]
    quotes: tuple[dict[str, Any], ...]
    raw: tuple[dict[str, Any], ...]
    source: str
    created_ts_ms: int


class AsyncPriceWriter:
    """SQLite-spooled batch writer for append-heavy market-data persistence."""

    def __init__(self, config: AsyncPriceWriterConfig | None = None):
        self._config = config or AsyncPriceWriterConfig.from_env()
        self._spool = SQLitePriceWriterSpool(
            path=str(self._config.spool_path or default_spool_path()),
            max_envelopes=max(1, int(self._config.queue_maxsize)),
            max_bytes=int(self._config.spool_max_bytes),
            busy_timeout_ms=int(self._config.spool_busy_timeout_ms),
            synchronous=str(self._config.spool_synchronous or "FULL"),
        )
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._state_lock = threading.RLock()
        self._metrics: dict[str, Any] = {
            "enqueued_batches": 0,
            "enqueued_rows": 0,
            "flushed_batches": 0,
            "flushed_rows": 0,
            "dead_letters": 0,
            "retry_count": 0,
            "backpressure_events": 0,
            "high_watermark_events": 0,
            "backpressure_active": False,
            "last_backpressure_ts_ms": 0,
            "last_backpressure_reason": "",
            "dropped_batches": 0,
            "dropped_rows": 0,
            "residual_dropped_batches": 0,
            "residual_dropped_rows": 0,
            "residual_spooled_batches": 0,
            "residual_spooled_rows": 0,
            "shutdown_drained_batches": 0,
            "shutdown_drained_rows": 0,
            "startup_pending_batches": 0,
            "startup_pending_rows": 0,
            "spool_deleted_batches": 0,
            "spool_delete_failures": 0,
            "spool_enqueue_failures": 0,
            "spool_corrupt_rows": 0,
            "spool_corruption_events": 0,
            "last_flush_latency_ms": 0,
            "total_flush_latency_ms": 0,
            "last_db_write_duration_ms": 0,
            "total_db_write_duration_ms": 0,
            "last_shutdown_drain_duration_ms": 0,
            "last_shutdown_drain_deadline_ms": 0,
            "inflight_batches": 0,
            "inflight_rows": 0,
            "last_enqueue_ts_ms": 0,
            "last_flush_ts_ms": 0,
            "last_error": "",
            "last_error_ts_ms": 0,
        }

    @property
    def enabled(self) -> bool:
        return bool(self._config.enabled)

    def start(self) -> dict[str, Any]:
        if not self.enabled:
            return self.get_snapshot()
        with self._state_lock:
            if self._thread is not None and self._thread.is_alive():
                return self.get_snapshot()
            self._spool.open()
            spool_stats = dict(self._spool.stats() or {})
            self._metrics["startup_pending_batches"] = int(spool_stats.get("pending_batches") or 0)
            self._metrics["startup_pending_rows"] = int(spool_stats.get("pending_rows") or 0)
            self._metrics["spool_corruption_events"] = int(spool_stats.get("corruption_events") or 0)
            self._stop.clear()
            self._thread = threading.Thread(target=self._run, name="async-price-writer", daemon=True)
            self._thread.start()
        self._emit_queue_depth(int(spool_stats.get("pending_batches") or 0), spool_stats=spool_stats)
        record_component_health(
            "async_price_writer",
            ok=True,
            status="ok",
            detail="writer_started",
            extra={
                "enabled": bool(self.enabled),
                "spool_path": str(spool_stats.get("path") or self._spool.path),
                "pending_batches": int(spool_stats.get("pending_batches") or 0),
                "pending_rows": int(spool_stats.get("pending_rows") or 0),
            },
        )
        return self.get_snapshot()

    def close(self, timeout_s: float = 2.0) -> dict[str, Any]:
        self._stop.set()
        drain_started = time.perf_counter()
        deadline_s = self._shutdown_drain_budget_s(timeout_s=timeout_s)
        deadline = time.monotonic() + float(deadline_s)
        with self._state_lock:
            thread = self._thread
        if thread is not None:
            thread.join(timeout=max(0.0, min(float(timeout_s), deadline - time.monotonic())))
        if int(self._spool_stats().get("pending_batches") or 0) > 0:
            self._drain_residual_until(deadline)
        self._record_residual_after_deadline(reason="shutdown_deadline")
        with self._state_lock:
            thread_alive = bool(self._thread is not None and self._thread.is_alive())
            if not thread_alive:
                self._thread = None
            self._metrics["last_shutdown_drain_duration_ms"] = int(
                round((time.perf_counter() - drain_started) * 1000.0)
            )
            self._metrics["last_shutdown_drain_deadline_ms"] = int(round(float(deadline_s) * 1000.0))
        snapshot = self.get_snapshot()
        self._spool.close()
        return snapshot

    def enqueue(
        self,
        *,
        prices: tuple[dict[str, Any], ...] = (),
        quotes: tuple[dict[str, Any], ...] = (),
        raw: tuple[dict[str, Any], ...] = (),
        source: str = "runtime",
    ) -> bool:
        if not self.enabled:
            return False
        if not prices and not quotes and not raw:
            return True
        self.start()
        envelope = PricePersistenceEnvelope(
            prices=tuple(dict(row or {}) for row in (prices or ())),
            quotes=tuple(dict(row or {}) for row in (quotes or ())),
            raw=tuple(dict(row or {}) for row in (raw or ())),
            source=str(source or "runtime"),
            created_ts_ms=int(time.time() * 1000),
        )
        row_count = self._row_count(envelope)
        try:
            spool_stats = self._spool.enqueue(
                source=envelope.source,
                created_ts_ms=int(envelope.created_ts_ms),
                prices=envelope.prices,
                quotes=envelope.quotes,
                raw=envelope.raw,
            )
            depth = int(spool_stats.get("pending_batches") or 0)
            with self._state_lock:
                self._metrics["enqueued_batches"] = int(self._metrics.get("enqueued_batches") or 0) + 1
                self._metrics["enqueued_rows"] = int(self._metrics.get("enqueued_rows") or 0) + row_count
                self._metrics["last_enqueue_ts_ms"] = int(envelope.created_ts_ms)
            self._emit_queue_depth(depth, spool_stats=spool_stats)
            if self._queue_at_high_watermark(depth, pending_bytes=int(spool_stats.get("pending_bytes") or 0)):
                self._note_backpressure(
                    "high_watermark",
                    row_count=row_count,
                    queue_depth=depth,
                    pending_bytes=int(spool_stats.get("pending_bytes") or 0),
                )
            else:
                self._clear_backpressure_if_recovered(
                    depth,
                    pending_bytes=int(spool_stats.get("pending_bytes") or 0),
                )
            return True
        except Exception as exc:
            reason = "spool_full" if isinstance(exc, PriceWriterSpoolFullError) else "spool_unavailable"
            depth = int(self._spool_stats().get("pending_batches") or 0)
            with self._state_lock:
                self._metrics["dropped_batches"] = int(self._metrics.get("dropped_batches") or 0) + 1
                self._metrics["dropped_rows"] = int(self._metrics.get("dropped_rows") or 0) + row_count
                self._metrics["spool_enqueue_failures"] = int(self._metrics.get("spool_enqueue_failures") or 0) + 1
                self._metrics["last_error"] = f"{type(exc).__name__}:{exc}"
                self._metrics["last_error_ts_ms"] = int(time.time() * 1000)
            self._note_backpressure(
                reason,
                row_count=row_count,
                queue_depth=depth,
                pending_bytes=int(self._spool_stats().get("pending_bytes") or 0),
            )
            emit_counter("async_price_writer_dropped_rows", row_count, component="engine.runtime.async_writer", extra_tags={"reason": reason})
            emit_counter("async_price_writer_spool_enqueue_failures", 1, component="engine.runtime.async_writer", extra_tags={"reason": reason})
            self._dead_letter(reason, [envelope], error=exc)
            return False

    def _run(self) -> None:
        while True:
            try:
                batch, corrupt = self._spool.select_batch(limit=int(self._config.batch_size))
            except Exception as exc:
                with self._state_lock:
                    self._metrics["last_error"] = f"{type(exc).__name__}:{exc}"
                    self._metrics["last_error_ts_ms"] = int(time.time() * 1000)
                _warn_nonfatal("ASYNC_PRICE_WRITER_SPOOL_SELECT_FAILED", exc)
                if self._stop.is_set():
                    return
                time.sleep(float(self._config.flush_interval_s))
                continue
            if corrupt:
                self._dead_letter_corrupt_spool_records(corrupt)
                self._delete_spool_rows((record.id for record in corrupt), reason="corrupt_payload")
            flushed = self._flush(batch) if batch else True
            if self._stop.is_set() and int(self._spool_stats().get("pending_batches") or 0) <= 0:
                return
            if batch and not flushed:
                time.sleep(backoff_delay_s(1, base_s=float(self._config.retry_base_s), max_s=float(self._config.retry_max_s)))
            elif not batch and not corrupt:
                time.sleep(float(self._config.flush_interval_s))

    def _flush(self, batch: list[PriceWriterSpoolRecord]) -> bool:
        combined_prices: list[dict[str, Any]] = []
        combined_quotes: list[dict[str, Any]] = []
        combined_raw: list[dict[str, Any]] = []
        spool_ids = [int(record.id) for record in batch]
        for record in batch:
            combined_prices.extend(list(record.prices))
            combined_quotes.extend(list(record.quotes))
            combined_raw.extend(list(record.raw))
        total_rows = int(len(combined_prices) + len(combined_quotes) + len(combined_raw))
        flush_started = time.perf_counter()
        with self._state_lock:
            self._metrics["inflight_batches"] = int(len(batch))
            self._metrics["inflight_rows"] = int(total_rows)
        last_error: BaseException | None = None
        for attempt in range(1, int(self._config.retry_attempts) + 1):
            try:
                storage = get_price_storage()
                write_started = time.perf_counter()
                write_result = storage.write_batch(prices=tuple(combined_prices), quotes=tuple(combined_quotes), raw=tuple(combined_raw))
                write_result_dict = dict(write_result) if isinstance(write_result, dict) else {}
                if write_result_dict and not bool(write_result_dict.get("enabled", True)):
                    raise RuntimeError("price_storage_write_not_committed:storage_disabled")
                if write_result_dict and not bool(write_result_dict.get("ok", True)):
                    raise RuntimeError(f"price_storage_write_not_committed:{write_result_dict}")
                deleted = self._delete_spool_rows(spool_ids, reason="flush_ok")
                db_write_duration_ms = float(write_result_dict.get("write_duration_ms") or ((time.perf_counter() - write_started) * 1000.0))
                flush_latency_ms = float((time.perf_counter() - flush_started) * 1000.0)
                now_ts_ms = int(time.time() * 1000)
                spool_stats = self._spool_stats()
                queue_depth = int(spool_stats.get("pending_batches") or 0)
                with self._state_lock:
                    self._metrics["flushed_batches"] = int(self._metrics.get("flushed_batches") or 0) + 1
                    self._metrics["flushed_rows"] = int(self._metrics.get("flushed_rows") or 0) + total_rows
                    self._metrics["last_flush_ts_ms"] = now_ts_ms
                    self._metrics["last_flush_latency_ms"] = int(round(flush_latency_ms))
                    self._metrics["total_flush_latency_ms"] = int(self._metrics.get("total_flush_latency_ms") or 0) + int(round(flush_latency_ms))
                    self._metrics["last_db_write_duration_ms"] = int(round(db_write_duration_ms))
                    self._metrics["total_db_write_duration_ms"] = int(self._metrics.get("total_db_write_duration_ms") or 0) + int(round(db_write_duration_ms))
                    self._metrics["last_error"] = ""
                    self._metrics["inflight_batches"] = 0
                    self._metrics["inflight_rows"] = 0
                emit_timing("async_price_writer_flush_latency_ms", flush_latency_ms, component="engine.runtime.async_writer")
                emit_timing("async_price_writer_db_write_duration_ms", db_write_duration_ms, component="engine.runtime.async_writer")
                self._emit_queue_depth(queue_depth, spool_stats=spool_stats)
                self._clear_backpressure_if_recovered(
                    queue_depth,
                    pending_bytes=int(spool_stats.get("pending_bytes") or 0),
                )
                record_component_health("async_price_writer", ok=True, status="ok", detail="flush_ok", observed_ts_ms=now_ts_ms, latency_ms=flush_latency_ms, extra={"enabled": bool(self.enabled), "rows": total_rows, "queue_depth": queue_depth, "spool_deleted_batches": int(deleted)})
                return True
            except Exception as exc:
                last_error = exc
                with self._state_lock:
                    self._metrics["retry_count"] = int(self._metrics.get("retry_count") or 0) + 1
                    self._metrics["last_error"] = f"{type(exc).__name__}:{exc}"
                    self._metrics["last_error_ts_ms"] = int(time.time() * 1000)
                emit_counter("async_price_writer_retries", 1, component="engine.runtime.async_writer", extra_tags={"attempt": int(attempt)})
                if attempt < int(self._config.retry_attempts):
                    time.sleep(backoff_delay_s(attempt, base_s=float(self._config.retry_base_s), max_s=float(self._config.retry_max_s)))
        with self._state_lock:
            self._metrics["inflight_batches"] = 0
            self._metrics["inflight_rows"] = 0
        emit_counter("async_price_writer_spool_write_failures", 1, component="engine.runtime.async_writer")
        if last_error is not None:
            record_component_health("async_price_writer", ok=False, status="error", detail=f"{type(last_error).__name__}:{last_error}", observed_ts_ms=int(time.time() * 1000), extra={"enabled": bool(self.enabled), "rows": total_rows, "spool_rows_retained": True})
        return False

    def _delete_spool_rows(self, ids: Iterable[int], *, reason: str) -> int:
        clean_ids = [int(row_id) for row_id in ids if int(row_id) > 0]
        if not clean_ids:
            return 0
        try:
            deleted = int(self._spool.delete(clean_ids))
            with self._state_lock:
                self._metrics["spool_deleted_batches"] = int(
                    self._metrics.get("spool_deleted_batches") or 0
                ) + int(deleted)
            emit_counter(
                "async_price_writer_spool_deleted_batches",
                int(deleted),
                component="engine.runtime.async_writer",
                extra_tags={"reason": str(reason)},
            )
            return int(deleted)
        except Exception as exc:
            with self._state_lock:
                self._metrics["spool_delete_failures"] = int(self._metrics.get("spool_delete_failures") or 0) + 1
                self._metrics["last_error"] = f"{type(exc).__name__}:{exc}"
                self._metrics["last_error_ts_ms"] = int(time.time() * 1000)
            emit_counter(
                "async_price_writer_spool_delete_failures",
                1,
                component="engine.runtime.async_writer",
                extra_tags={"reason": str(reason)},
            )
            raise

    def _row_count(self, envelope: PricePersistenceEnvelope | PriceWriterSpoolRecord) -> int:
        return int(len(envelope.prices) + len(envelope.quotes) + len(envelope.raw))

    def _batch_row_count(self, batch: list[PriceWriterSpoolRecord]) -> int:
        return int(sum(self._row_count(record) for record in list(batch or [])))

    def _high_watermark_depth(self) -> int:
        return max(1, int(math.ceil(max(1, int(self._config.queue_maxsize)) * float(self._config.high_watermark_ratio))))

    def _queue_at_high_watermark(self, depth: int | None = None, *, pending_bytes: int | None = None) -> bool:
        stats = self._spool_stats() if depth is None or pending_bytes is None else {}
        current = int((stats.get("pending_batches") or 0) if depth is None else depth)
        current_bytes = int((stats.get("pending_bytes") or 0) if pending_bytes is None else pending_bytes)
        byte_threshold = int(float(max(1, int(self._config.spool_max_bytes))) * 0.80)
        return bool(current >= self._high_watermark_depth() or current_bytes >= byte_threshold)

    def _emit_queue_depth(self, depth: int, *, spool_stats: dict[str, Any] | None = None) -> None:
        maximum = max(1, int(self._config.queue_maxsize))
        stats = dict(spool_stats or {})
        pending_bytes = int(stats.get("pending_bytes") or 0)
        max_bytes = max(1, int(stats.get("max_bytes") or self._config.spool_max_bytes))
        emit_gauge("async_price_writer_queue_depth", int(depth), component="engine.runtime.async_writer")
        emit_gauge("async_price_writer_queue_fill_ratio", float(depth / maximum), component="engine.runtime.async_writer")
        emit_gauge("async_price_writer_spool_pending_bytes", pending_bytes, component="engine.runtime.async_writer")
        emit_gauge("async_price_writer_spool_bytes_fill_ratio", float(pending_bytes / max_bytes), component="engine.runtime.async_writer")

    def _spool_stats(self) -> dict[str, Any]:
        try:
            stats = dict(self._spool.stats() or {})
            with self._state_lock:
                self._metrics["spool_corruption_events"] = int(stats.get("corruption_events") or 0)
            return stats
        except Exception as exc:
            with self._state_lock:
                self._metrics["last_error"] = f"{type(exc).__name__}:{exc}"
                self._metrics["last_error_ts_ms"] = int(time.time() * 1000)
            return {"ok": False, "pending_batches": 0, "pending_rows": 0, "pending_bytes": 0, "file_bytes": 0, "max_envelopes": int(self._config.queue_maxsize), "max_bytes": int(self._config.spool_max_bytes), "error": f"{type(exc).__name__}:{exc}"}

    def _note_backpressure(
        self,
        reason: str,
        *,
        row_count: int,
        queue_depth: int,
        pending_bytes: int = 0,
    ) -> None:
        now_ts_ms = int(time.time() * 1000)
        with self._state_lock:
            self._metrics["backpressure_events"] = int(self._metrics.get("backpressure_events") or 0) + 1
            if str(reason) == "high_watermark":
                self._metrics["high_watermark_events"] = int(self._metrics.get("high_watermark_events") or 0) + 1
            self._metrics["backpressure_active"] = True
            self._metrics["last_backpressure_ts_ms"] = now_ts_ms
            self._metrics["last_backpressure_reason"] = str(reason)
        emit_counter("async_price_writer_backpressure_events", 1, component="engine.runtime.async_writer", extra_tags={"reason": str(reason)})
        record_component_health(
            "async_price_writer",
            ok=False,
            status="backpressure",
            detail=str(reason),
            observed_ts_ms=now_ts_ms,
            extra={
                "enabled": bool(self.enabled),
                "row_count": row_count,
                "queue_depth": queue_depth,
                "queue_maxsize": int(self._config.queue_maxsize),
                "high_watermark_depth": int(self._high_watermark_depth()),
                "spool_pending_bytes": int(pending_bytes),
                "spool_max_bytes": int(self._config.spool_max_bytes),
            },
        )

    def _clear_backpressure_if_recovered(self, depth: int, *, pending_bytes: int | None = None) -> None:
        if self._queue_at_high_watermark(depth, pending_bytes=pending_bytes):
            return
        with self._state_lock:
            if bool(self._metrics.get("backpressure_active")):
                self._metrics["backpressure_active"] = False

    def _shutdown_drain_budget_s(self, *, timeout_s: float) -> float:
        queue_depth = int(self._spool_stats().get("pending_batches") or 0)
        batch_count = int(math.ceil(queue_depth / max(1, int(self._config.batch_size)))) if queue_depth > 0 else 0
        with self._state_lock:
            last_write_ms = int(self._metrics.get("last_db_write_duration_ms") or 0)
        per_batch_s = max(float(last_write_ms) / 1000.0, min(float(self._config.flush_interval_s), 0.25), 0.01)
        return min(max(0.0, float(self._config.shutdown_drain_max_s)), max(max(0.0, float(timeout_s)), batch_count * per_batch_s))

    def _drain_residual_until(self, deadline: float) -> None:
        while time.monotonic() < float(deadline):
            batch, corrupt = self._spool.select_batch(limit=int(self._config.batch_size))
            if corrupt:
                self._dead_letter_corrupt_spool_records(corrupt)
                self._delete_spool_rows((record.id for record in corrupt), reason="corrupt_payload")
            if not batch:
                return
            rows = self._batch_row_count(batch)
            if self._flush(batch):
                with self._state_lock:
                    self._metrics["shutdown_drained_batches"] = int(self._metrics.get("shutdown_drained_batches") or 0) + len(batch)
                    self._metrics["shutdown_drained_rows"] = int(self._metrics.get("shutdown_drained_rows") or 0) + rows
            else:
                return

    def _record_residual_after_deadline(self, *, reason: str) -> tuple[int, int]:
        stats = self._spool_stats()
        residual_batches = int(stats.get("pending_batches") or 0)
        residual_rows = int(stats.get("pending_rows") or 0)
        if not residual_batches and not residual_rows:
            return 0, 0
        with self._state_lock:
            self._metrics["residual_spooled_batches"] = int(self._metrics.get("residual_spooled_batches") or 0) + residual_batches
            self._metrics["residual_spooled_rows"] = int(self._metrics.get("residual_spooled_rows") or 0) + residual_rows
        emit_counter("async_price_writer_residual_spooled_rows", residual_rows, component="engine.runtime.async_writer", extra_tags={"reason": str(reason)})
        record_component_health("async_price_writer", ok=True, status="degraded", detail=str(reason), observed_ts_ms=int(time.time() * 1000), extra={"enabled": bool(self.enabled), "rows": residual_rows, "batches": residual_batches, "spool_path": str(stats.get("path") or "")})
        return residual_batches, residual_rows

    def _dead_letter(self, reason: str, batch: list[PricePersistenceEnvelope], *, error: BaseException | None = None) -> None:
        payload = {
            "ts_ms": int(time.time() * 1000),
            "reason": str(reason),
            "error": (f"{type(error).__name__}:{error}" if error is not None else ""),
            "batches": [{"source": envelope.source, "created_ts_ms": int(envelope.created_ts_ms), "prices": list(envelope.prices), "quotes": list(envelope.quotes), "raw": list(envelope.raw)} for envelope in batch],
        }
        try:
            path = str(self._config.dead_letter_path)
            directory = os.path.dirname(path)
            if directory:
                os.makedirs(directory, exist_ok=True)
            with open(path, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, separators=(",", ":"), default=_json_default) + "\n")
        except Exception as exc:
            _warn_nonfatal("ASYNC_PRICE_WRITER_DEAD_LETTER_FAILED", exc, reason=str(reason))
        with self._state_lock:
            self._metrics["dead_letters"] = int(self._metrics.get("dead_letters") or 0) + 1
            self._metrics["last_error"] = payload.get("error") or str(reason)
            self._metrics["last_error_ts_ms"] = int(payload["ts_ms"])
        if error is not None:
            _warn_nonfatal("ASYNC_PRICE_WRITER_ENQUEUE_FAILED", error, reason=str(reason), dead_letter_path=str(self._config.dead_letter_path))

    def _dead_letter_corrupt_spool_records(self, records: list[CorruptPriceWriterSpoolRecord]) -> None:
        if not records:
            return
        payload = {
            "ts_ms": int(time.time() * 1000),
            "reason": "spool_payload_corrupt",
            "spool_path": str(self._spool.path),
            "records": [{"id": int(record.id), "created_ts_ms": int(record.created_ts_ms), "payload_bytes": int(record.payload_bytes), "error": str(record.error), "payload_json": str(record.payload_json)} for record in records],
        }
        try:
            path = str(self._config.dead_letter_path)
            directory = os.path.dirname(path)
            if directory:
                os.makedirs(directory, exist_ok=True)
            with open(path, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, separators=(",", ":"), default=_json_default) + "\n")
        except Exception as exc:
            _warn_nonfatal("ASYNC_PRICE_WRITER_DEAD_LETTER_FAILED", exc, reason="spool_payload_corrupt")
        with self._state_lock:
            self._metrics["dead_letters"] = int(self._metrics.get("dead_letters") or 0) + 1
            self._metrics["spool_corrupt_rows"] = int(self._metrics.get("spool_corrupt_rows") or 0) + len(records)
            self._metrics["last_error"] = "spool_payload_corrupt"
            self._metrics["last_error_ts_ms"] = int(payload["ts_ms"])
        emit_counter("async_price_writer_spool_corrupt_rows", len(records), component="engine.runtime.async_writer")

    def get_snapshot(self) -> dict[str, Any]:
        spool_stats = self._spool_stats()
        queue_depth = int(spool_stats.get("pending_batches") or 0)
        queue_maxsize = max(1, int(self._config.queue_maxsize))
        high_watermark_depth = int(self._high_watermark_depth())
        with self._state_lock:
            metrics = dict(self._metrics)
            thread_alive = bool(self._thread is not None and self._thread.is_alive())
        backpressure_active = bool(metrics.get("backpressure_active")) or self._queue_at_high_watermark(
            queue_depth,
            pending_bytes=int(spool_stats.get("pending_bytes") or 0),
        )
        return {
            "ok": (not self.enabled) or (thread_alive and bool(spool_stats.get("ok", True)) and not str(metrics.get("last_error") or "").strip() and not backpressure_active),
            "enabled": bool(self.enabled),
            "thread_alive": thread_alive,
            "queue_depth": queue_depth,
            "queue_maxsize": queue_maxsize,
            "queue_fill_ratio": float(queue_depth / queue_maxsize),
            "high_watermark_ratio": float(self._config.high_watermark_ratio),
            "high_watermark_depth": high_watermark_depth,
            "batch_size": int(self._config.batch_size),
            "enqueued_batches": int(metrics.get("enqueued_batches") or 0),
            "enqueued_rows": int(metrics.get("enqueued_rows") or 0),
            "flushed_batches": int(metrics.get("flushed_batches") or 0),
            "flushed_rows": int(metrics.get("flushed_rows") or 0),
            "dead_letters": int(metrics.get("dead_letters") or 0),
            "retry_count": int(metrics.get("retry_count") or 0),
            "backpressure_active": bool(backpressure_active),
            "backpressure_events": int(metrics.get("backpressure_events") or 0),
            "high_watermark_events": int(metrics.get("high_watermark_events") or 0),
            "last_backpressure_reason": str(metrics.get("last_backpressure_reason") or ""),
            "last_backpressure_ts_ms": (int(metrics.get("last_backpressure_ts_ms") or 0) or None),
            "dropped_batches": int(metrics.get("dropped_batches") or 0),
            "dropped_rows": int(metrics.get("dropped_rows") or 0),
            "residual_dropped_batches": int(metrics.get("residual_dropped_batches") or 0),
            "residual_dropped_rows": int(metrics.get("residual_dropped_rows") or 0),
            "residual_spooled_batches": int(metrics.get("residual_spooled_batches") or 0),
            "residual_spooled_rows": int(metrics.get("residual_spooled_rows") or 0),
            "shutdown_drained_batches": int(metrics.get("shutdown_drained_batches") or 0),
            "shutdown_drained_rows": int(metrics.get("shutdown_drained_rows") or 0),
            "startup_pending_batches": int(metrics.get("startup_pending_batches") or 0),
            "startup_pending_rows": int(metrics.get("startup_pending_rows") or 0),
            "spool_path": str(spool_stats.get("path") or self._spool.path),
            "spool_pending_batches": int(spool_stats.get("pending_batches") or 0),
            "spool_pending_rows": int(spool_stats.get("pending_rows") or 0),
            "spool_pending_bytes": int(spool_stats.get("pending_bytes") or 0),
            "spool_file_bytes": int(spool_stats.get("file_bytes") or 0),
            "spool_max_envelopes": int(spool_stats.get("max_envelopes") or self._config.queue_maxsize),
            "spool_max_bytes": int(spool_stats.get("max_bytes") or self._config.spool_max_bytes),
            "spool_bytes_fill_ratio": float(spool_stats.get("bytes_fill_ratio") or 0.0),
            "spool_oldest_created_ts_ms": spool_stats.get("oldest_created_ts_ms"),
            "spool_newest_created_ts_ms": spool_stats.get("newest_created_ts_ms"),
            "spool_deleted_batches": int(metrics.get("spool_deleted_batches") or 0),
            "spool_delete_failures": int(metrics.get("spool_delete_failures") or 0),
            "spool_enqueue_failures": int(metrics.get("spool_enqueue_failures") or 0),
            "spool_corrupt_rows": int(metrics.get("spool_corrupt_rows") or 0),
            "spool_corruption_events": int(metrics.get("spool_corruption_events") or 0),
            "spool_last_quarantine_paths": list(spool_stats.get("last_quarantine_paths") or []),
            "spool_error": str(spool_stats.get("error") or ""),
            "last_flush_latency_ms": int(metrics.get("last_flush_latency_ms") or 0),
            "total_flush_latency_ms": int(metrics.get("total_flush_latency_ms") or 0),
            "last_db_write_duration_ms": int(metrics.get("last_db_write_duration_ms") or 0),
            "total_db_write_duration_ms": int(metrics.get("total_db_write_duration_ms") or 0),
            "last_shutdown_drain_duration_ms": int(metrics.get("last_shutdown_drain_duration_ms") or 0),
            "last_shutdown_drain_deadline_ms": int(metrics.get("last_shutdown_drain_deadline_ms") or 0),
            "inflight_batches": int(metrics.get("inflight_batches") or 0),
            "inflight_rows": int(metrics.get("inflight_rows") or 0),
            "last_enqueue_ts_ms": (int(metrics.get("last_enqueue_ts_ms") or 0) or None),
            "last_flush_ts_ms": (int(metrics.get("last_flush_ts_ms") or 0) or None),
            "last_error": str(metrics.get("last_error") or ""),
            "last_error_ts_ms": (int(metrics.get("last_error_ts_ms") or 0) or None),
            "dead_letter_path": str(self._config.dead_letter_path),
            "ts_ms": int(time.time() * 1000),
        }


def get_async_writer() -> AsyncPriceWriter:
    global _WRITER
    if _WRITER is None:
        with _WRITER_LOCK:
            if _WRITER is None:
                _WRITER = AsyncPriceWriter()
    return _WRITER


def init_async_writer() -> dict[str, Any]:
    try:
        return get_async_writer().start()
    except Exception as exc:
        _warn_nonfatal("ASYNC_PRICE_WRITER_INIT_FAILED", exc)
        return {"ok": False, "enabled": bool(AsyncPriceWriterConfig.from_env().enabled), "last_error": f"{type(exc).__name__}:{exc}", "ts_ms": int(time.time() * 1000)}


def shutdown_async_writer(timeout_s: float = 2.0) -> dict[str, Any]:
    global _WRITER
    with _WRITER_LOCK:
        writer = _WRITER
        _WRITER = None
    if writer is None:
        return {"ok": True, "enabled": False, "thread_alive": False, "queue_depth": 0, "detail": "async_writer_not_started", "ts_ms": int(time.time() * 1000)}
    snapshot = dict(writer.close(timeout_s=timeout_s) or {})
    snapshot["detail"] = "async_writer_stopped"
    return snapshot


def enqueue_price_persistence(
    *,
    prices: tuple[dict[str, Any], ...] = (),
    quotes: tuple[dict[str, Any], ...] = (),
    raw: tuple[dict[str, Any], ...] = (),
    source: str = "runtime",
) -> bool:
    return get_async_writer().enqueue(prices=prices, quotes=quotes, raw=raw, source=source)


__all__ = [
    "AsyncPriceWriter",
    "AsyncPriceWriterConfig",
    "enqueue_price_persistence",
    "get_async_writer",
    "init_async_writer",
    "shutdown_async_writer",
]
