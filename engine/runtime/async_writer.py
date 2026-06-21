"""Queue-based async persistence for live price fanout."""

from __future__ import annotations

import json
import logging
import math
import os
import queue
import threading
import time
from dataclasses import dataclass
from typing import Any

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
    """Configure queue sizing, retry policy, and dead-letter behavior."""

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

    @classmethod
    def from_env(cls) -> "AsyncPriceWriterConfig":
        enabled_default = bool(getattr(get_price_storage(), "enabled", False))
        dead_letter_path = str(
            os.environ.get("ASYNC_PRICE_WRITER_DEAD_LETTER_PATH")
            or os.path.join(str(default_local_log_dir().resolve()), "async_price_writer_dead_letter.jsonl")
        ).strip()
        return cls(
            enabled=_env_bool("ASYNC_PRICE_WRITER_ENABLED", default=enabled_default),
            queue_maxsize=tuned_int("ASYNC_PRICE_WRITER_QUEUE_MAXSIZE", 2048, 32, 32768),
            batch_size=tuned_int("ASYNC_PRICE_WRITER_BATCH_SIZE", 256, 1, 4096),
            flush_interval_s=tuned_float("ASYNC_PRICE_WRITER_FLUSH_INTERVAL_S", 0.5, 0.05, 5.0),
            retry_attempts=tuned_int("ASYNC_PRICE_WRITER_RETRY_ATTEMPTS", 4, 1, 10),
            retry_base_s=tuned_float("ASYNC_PRICE_WRITER_RETRY_BASE_S", 0.25, 0.01, 5.0),
            retry_max_s=tuned_float("ASYNC_PRICE_WRITER_RETRY_MAX_S", 5.0, 0.10, 30.0),
            enqueue_timeout_s=tuned_float("ASYNC_PRICE_WRITER_ENQUEUE_TIMEOUT_S", 0.05, 0.0, 5.0),
            dead_letter_path=dead_letter_path,
            high_watermark_ratio=tuned_float("ASYNC_PRICE_WRITER_HIGH_WATERMARK_RATIO", 0.75, 0.10, 1.0),
            shutdown_drain_max_s=tuned_float("ASYNC_PRICE_WRITER_SHUTDOWN_DRAIN_MAX_S", 30.0, 0.0, 300.0),
        )


@dataclass(frozen=True)
class PricePersistenceEnvelope:
    prices: tuple[dict[str, Any], ...]
    quotes: tuple[dict[str, Any], ...]
    raw: tuple[dict[str, Any], ...]
    source: str
    created_ts_ms: int


class AsyncPriceWriter:
    """Queue-backed batch writer for append-heavy market-data persistence."""

    def __init__(self, config: AsyncPriceWriterConfig | None = None):
        self._config = config or AsyncPriceWriterConfig.from_env()
        self._queue: queue.Queue[PricePersistenceEnvelope] = queue.Queue(maxsize=int(self._config.queue_maxsize))
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
            "shutdown_drained_batches": 0,
            "shutdown_drained_rows": 0,
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
            self._stop.clear()
            self._thread = threading.Thread(target=self._run, name="async-price-writer", daemon=True)
            self._thread.start()
        record_component_health("async_price_writer", ok=True, status="ok", detail="writer_started", extra={"enabled": True})
        return self.get_snapshot()

    def close(self, timeout_s: float = 2.0) -> dict[str, Any]:
        self._stop.set()
        drain_started = time.perf_counter()
        deadline_s = self._shutdown_drain_budget_s(timeout_s=timeout_s)
        deadline = time.monotonic() + float(deadline_s)
        with self._state_lock:
            thread = self._thread
        if thread is not None:
            join_budget_s = max(0.0, deadline - time.monotonic())
            if not self._queue.empty():
                join_budget_s = min(join_budget_s, max(0.0, float(timeout_s)))
            thread.join(timeout=join_budget_s)
        if not self._queue.empty():
            self._drain_residual_until(deadline)
        with self._state_lock:
            thread = self._thread
        if thread is not None and thread.is_alive():
            remaining = max(0.0, deadline - time.monotonic())
            if remaining > 0:
                thread.join(timeout=remaining)
        residual_batches, residual_rows = self._drop_residual_after_deadline(reason="shutdown_deadline")
        with self._state_lock:
            thread_alive = bool(self._thread is not None and self._thread.is_alive())
            if not thread_alive:
                self._thread = None
            self._metrics["last_shutdown_drain_duration_ms"] = int(round((time.perf_counter() - drain_started) * 1000.0))
            self._metrics["last_shutdown_drain_deadline_ms"] = int(round(deadline_s * 1000.0))
            inflight_rows = int(self._metrics.get("inflight_rows") or 0)
            if thread_alive and inflight_rows > 0:
                self._metrics["residual_dropped_rows"] = int(self._metrics.get("residual_dropped_rows") or 0) + inflight_rows
                self._metrics["last_error"] = "shutdown_deadline_inflight_rows"
                self._metrics["last_error_ts_ms"] = int(time.time() * 1000)
                emit_counter(
                    "async_price_writer_residual_dropped_rows",
                    inflight_rows,
                    component="engine.runtime.async_writer",
                    extra_tags={"reason": "shutdown_deadline_inflight"},
                )
            if residual_batches or residual_rows:
                self._metrics["last_error"] = "shutdown_deadline_residual_rows"
                self._metrics["last_error_ts_ms"] = int(time.time() * 1000)
        return self.get_snapshot()

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
            self._queue.put(envelope, timeout=float(self._config.enqueue_timeout_s))
            depth = int(self._queue.qsize())
            with self._state_lock:
                self._metrics["enqueued_batches"] = int(self._metrics.get("enqueued_batches") or 0) + 1
                self._metrics["enqueued_rows"] = int(self._metrics.get("enqueued_rows") or 0) + row_count
                self._metrics["last_enqueue_ts_ms"] = int(envelope.created_ts_ms)
            self._emit_queue_depth(depth)
            if self._queue_at_high_watermark(depth):
                self._note_backpressure("high_watermark", row_count=row_count, queue_depth=depth)
            else:
                self._clear_backpressure_if_recovered(depth)
            return True
        except queue.Full as exc:
            depth = int(self._queue.qsize())
            with self._state_lock:
                self._metrics["dropped_batches"] = int(self._metrics.get("dropped_batches") or 0) + 1
                self._metrics["dropped_rows"] = int(self._metrics.get("dropped_rows") or 0) + row_count
                self._metrics["last_error"] = f"{type(exc).__name__}:{exc}"
                self._metrics["last_error_ts_ms"] = int(time.time() * 1000)
            self._note_backpressure("queue_full", row_count=row_count, queue_depth=depth)
            emit_counter("async_price_writer_dropped_rows", row_count, component="engine.runtime.async_writer", extra_tags={"reason": "queue_full"})
            self._emit_queue_depth(depth)
            self._dead_letter("queue_full", [envelope], error=exc)
            record_component_health(
                "async_price_writer",
                ok=False,
                status="backpressure",
                detail="queue_full",
                extra={"enabled": bool(self.enabled), "row_count": row_count},
            )
            return False

    def _run(self) -> None:
        while True:
            batch = self._drain_batch()
            if batch:
                self._flush(batch)
            if self._stop.is_set() and self._queue.empty():
                return
            if not batch:
                continue

    def _drain_batch(self) -> list[PricePersistenceEnvelope]:
        batch: list[PricePersistenceEnvelope] = []
        try:
            first = self._queue.get(timeout=float(self._config.flush_interval_s))
        except queue.Empty:
            return batch
        batch.append(first)
        deadline = time.monotonic() + float(self._config.flush_interval_s)
        while len(batch) < int(self._config.batch_size):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                batch.append(self._queue.get(timeout=remaining))
            except queue.Empty:
                break
        return batch

    def _flush(self, batch: list[PricePersistenceEnvelope]) -> bool:
        combined_prices: list[dict[str, Any]] = []
        combined_quotes: list[dict[str, Any]] = []
        combined_raw: list[dict[str, Any]] = []
        for envelope in batch:
            combined_prices.extend(list(envelope.prices))
            combined_quotes.extend(list(envelope.quotes))
            combined_raw.extend(list(envelope.raw))
        total_rows = int(len(combined_prices) + len(combined_quotes) + len(combined_raw))
        last_error: BaseException | None = None
        flush_started = time.perf_counter()
        with self._state_lock:
            self._metrics["inflight_batches"] = int(len(batch))
            self._metrics["inflight_rows"] = int(total_rows)
        for attempt in range(1, int(self._config.retry_attempts) + 1):
            try:
                storage = get_price_storage()
                write_started = time.perf_counter()
                write_result = storage.write_batch(prices=tuple(combined_prices), quotes=tuple(combined_quotes), raw=tuple(combined_raw))
                write_result_dict = dict(write_result) if isinstance(write_result, dict) else {}
                db_write_duration_ms = float(write_result_dict.get("write_duration_ms") or ((time.perf_counter() - write_started) * 1000.0))
                flush_latency_ms = float((time.perf_counter() - flush_started) * 1000.0)
                now_ts_ms = int(time.time() * 1000)
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
                self._emit_queue_depth(int(self._queue.qsize()))
                self._clear_backpressure_if_recovered(int(self._queue.qsize()))
                record_component_health(
                    "async_price_writer",
                    ok=True,
                    status="ok",
                    detail="flush_ok",
                    observed_ts_ms=now_ts_ms,
                    latency_ms=flush_latency_ms,
                    extra={"enabled": bool(self.enabled), "rows": total_rows, "db_write_duration_ms": int(round(db_write_duration_ms)), "queue_depth": int(self._queue.qsize())},
                )
                return True
            except Exception as exc:
                last_error = exc
                with self._state_lock:
                    self._metrics["retry_count"] = int(self._metrics.get("retry_count") or 0) + 1
                    self._metrics["last_error"] = f"{type(exc).__name__}:{exc}"
                    self._metrics["last_error_ts_ms"] = int(time.time() * 1000)
                emit_counter("async_price_writer_retries", 1, component="engine.runtime.async_writer", extra_tags={"attempt": int(attempt)})
                if attempt >= int(self._config.retry_attempts):
                    break
                time.sleep(backoff_delay_s(int(attempt), base_s=float(self._config.retry_base_s), max_s=float(self._config.retry_max_s)))
        if last_error is not None:
            with self._state_lock:
                self._metrics["inflight_batches"] = 0
                self._metrics["inflight_rows"] = 0
            self._dead_letter("flush_failed", batch, error=last_error)
            record_component_health(
                "async_price_writer",
                ok=False,
                status="error",
                detail=f"{type(last_error).__name__}:{last_error}",
                observed_ts_ms=int(time.time() * 1000),
                extra={"enabled": bool(self.enabled), "rows": total_rows},
            )
        return False

    def _row_count(self, envelope: PricePersistenceEnvelope) -> int:
        return int(len(envelope.prices) + len(envelope.quotes) + len(envelope.raw))

    def _batch_row_count(self, batch: list[PricePersistenceEnvelope]) -> int:
        return int(sum(self._row_count(envelope) for envelope in list(batch or [])))

    def _high_watermark_depth(self) -> int:
        maximum = max(1, int(self._config.queue_maxsize))
        ratio = min(1.0, max(0.10, float(self._config.high_watermark_ratio)))
        return max(1, int(math.ceil(float(maximum) * ratio)))

    def _queue_at_high_watermark(self, depth: int | None = None) -> bool:
        current = int(self._queue.qsize() if depth is None else depth)
        return bool(current >= self._high_watermark_depth())

    def _emit_queue_depth(self, depth: int) -> None:
        maximum = max(1, int(self._config.queue_maxsize))
        emit_gauge("async_price_writer_queue_depth", int(depth), component="engine.runtime.async_writer")
        emit_gauge("async_price_writer_queue_fill_ratio", float(int(depth) / float(maximum)), component="engine.runtime.async_writer")

    def _note_backpressure(self, reason: str, *, row_count: int, queue_depth: int) -> None:
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
            extra={"enabled": bool(self.enabled), "row_count": row_count, "queue_depth": queue_depth, "queue_maxsize": int(self._config.queue_maxsize), "high_watermark_depth": int(self._high_watermark_depth())},
        )

    def _clear_backpressure_if_recovered(self, depth: int) -> None:
        if self._queue_at_high_watermark(depth):
            return
        with self._state_lock:
            if bool(self._metrics.get("backpressure_active")):
                self._metrics["backpressure_active"] = False

    def _shutdown_drain_budget_s(self, *, timeout_s: float) -> float:
        queue_depth = int(self._queue.qsize())
        batch_size = max(1, int(self._config.batch_size))
        batch_count = int(math.ceil(float(queue_depth) / float(batch_size))) if queue_depth > 0 else 0
        with self._state_lock:
            last_write_ms = int(self._metrics.get("last_db_write_duration_ms") or 0)
        per_batch_s = max(float(last_write_ms) / 1000.0, min(float(self._config.flush_interval_s), 0.25), 0.01)
        adaptive_s = float(batch_count) * per_batch_s
        requested_s = max(0.0, float(timeout_s))
        hard_cap_s = max(0.0, float(self._config.shutdown_drain_max_s))
        return min(hard_cap_s, max(requested_s, adaptive_s))

    def _drain_residual_until(self, deadline: float) -> None:
        while time.monotonic() < float(deadline):
            batch: list[PricePersistenceEnvelope] = []
            while len(batch) < int(self._config.batch_size) and time.monotonic() < float(deadline):
                try:
                    batch.append(self._queue.get_nowait())
                except queue.Empty:
                    break
            if not batch:
                return
            rows = self._batch_row_count(batch)
            if self._flush(batch):
                with self._state_lock:
                    self._metrics["shutdown_drained_batches"] = int(self._metrics.get("shutdown_drained_batches") or 0) + int(len(batch))
                    self._metrics["shutdown_drained_rows"] = int(self._metrics.get("shutdown_drained_rows") or 0) + rows
            else:
                return

    def _drop_residual_after_deadline(self, *, reason: str) -> tuple[int, int]:
        residual: list[PricePersistenceEnvelope] = []
        while True:
            try:
                residual.append(self._queue.get_nowait())
            except queue.Empty:
                break
        if not residual:
            return 0, 0
        residual_rows = self._batch_row_count(residual)
        now_ts_ms = int(time.time() * 1000)
        self._dead_letter(str(reason), residual)
        with self._state_lock:
            self._metrics["residual_dropped_batches"] = int(self._metrics.get("residual_dropped_batches") or 0) + int(len(residual))
            self._metrics["residual_dropped_rows"] = int(self._metrics.get("residual_dropped_rows") or 0) + residual_rows
            self._metrics["dropped_batches"] = int(self._metrics.get("dropped_batches") or 0) + int(len(residual))
            self._metrics["dropped_rows"] = int(self._metrics.get("dropped_rows") or 0) + residual_rows
            self._metrics["last_error"] = str(reason)
            self._metrics["last_error_ts_ms"] = now_ts_ms
        emit_counter("async_price_writer_residual_dropped_rows", residual_rows, component="engine.runtime.async_writer", extra_tags={"reason": str(reason)})
        emit_counter("async_price_writer_dropped_rows", residual_rows, component="engine.runtime.async_writer", extra_tags={"reason": str(reason)})
        record_component_health("async_price_writer", ok=False, status="error", detail=str(reason), observed_ts_ms=now_ts_ms, extra={"enabled": bool(self.enabled), "rows": residual_rows, "batches": int(len(residual))})
        return int(len(residual)), residual_rows

    def _dead_letter(self, reason: str, batch: list[PricePersistenceEnvelope], *, error: BaseException | None = None) -> None:
        now_ts_ms = int(time.time() * 1000)
        payload = {
            "ts_ms": now_ts_ms,
            "reason": str(reason),
            "error": (f"{type(error).__name__}:{error}" if error is not None else ""),
            "batches": [
                {"source": str(envelope.source), "created_ts_ms": int(envelope.created_ts_ms), "prices": list(envelope.prices), "quotes": list(envelope.quotes), "raw": list(envelope.raw)}
                for envelope in batch
            ],
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
            self._metrics["last_error_ts_ms"] = now_ts_ms
        if error is not None:
            _warn_nonfatal("ASYNC_PRICE_WRITER_FLUSH_FAILED", error, reason=str(reason), dead_letter_path=str(self._config.dead_letter_path))

    def get_snapshot(self) -> dict[str, Any]:
        queue_depth = int(self._queue.qsize())
        queue_maxsize = max(1, int(self._config.queue_maxsize))
        high_watermark_depth = int(self._high_watermark_depth())
        with self._state_lock:
            metrics = dict(self._metrics)
            thread_alive = bool(self._thread is not None and self._thread.is_alive())
        backpressure_active = bool(metrics.get("backpressure_active")) or bool(queue_depth >= high_watermark_depth)
        return {
            "ok": (not self.enabled) or (thread_alive and not str(metrics.get("last_error") or "").strip() and not bool(backpressure_active)),
            "enabled": bool(self.enabled),
            "thread_alive": thread_alive,
            "queue_depth": queue_depth,
            "queue_maxsize": queue_maxsize,
            "queue_fill_ratio": float(queue_depth / float(queue_maxsize)),
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
            "shutdown_drained_batches": int(metrics.get("shutdown_drained_batches") or 0),
            "shutdown_drained_rows": int(metrics.get("shutdown_drained_rows") or 0),
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
