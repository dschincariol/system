"""Queue-based async persistence for live price fanout."""

from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
from dataclasses import dataclass
from typing import Any, Mapping

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.observability import backoff_delay_s, record_component_health
from engine.runtime.storage_pg_prices import get_price_storage

LOG = get_logger("runtime.async_writer")
_WRITER_LOCK = threading.Lock()
_WRITER: "AsyncPriceWriter | None" = None


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

    @classmethod
    def from_env(cls) -> "AsyncPriceWriterConfig":
        """Build the async-writer configuration from environment variables."""
        enabled_default = bool(getattr(get_price_storage(), "enabled", False))
        dead_letter_path = str(
            os.environ.get("ASYNC_PRICE_WRITER_DEAD_LETTER_PATH")
            or os.path.join(os.getcwd(), "logs", "async_price_writer_dead_letter.jsonl")
        ).strip()
        return cls(
            enabled=_env_bool("ASYNC_PRICE_WRITER_ENABLED", default=enabled_default),
            queue_maxsize=max(32, _env_int("ASYNC_PRICE_WRITER_QUEUE_MAXSIZE", 2048)),
            batch_size=max(1, _env_int("ASYNC_PRICE_WRITER_BATCH_SIZE", 256)),
            flush_interval_s=max(0.05, _env_float("ASYNC_PRICE_WRITER_FLUSH_INTERVAL_S", 0.5)),
            retry_attempts=max(1, _env_int("ASYNC_PRICE_WRITER_RETRY_ATTEMPTS", 4)),
            retry_base_s=max(0.05, _env_float("ASYNC_PRICE_WRITER_RETRY_BASE_S", 0.25)),
            retry_max_s=max(0.10, _env_float("ASYNC_PRICE_WRITER_RETRY_MAX_S", 5.0)),
            enqueue_timeout_s=max(0.0, _env_float("ASYNC_PRICE_WRITER_ENQUEUE_TIMEOUT_S", 0.05)),
            dead_letter_path=dead_letter_path,
        )


@dataclass(frozen=True)
class PricePersistenceEnvelope:
    """One queued batch of price, quote, and raw rows awaiting persistence."""

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
            "dropped_batches": 0,
            "last_enqueue_ts_ms": 0,
            "last_flush_ts_ms": 0,
            "last_error": "",
            "last_error_ts_ms": 0,
        }

    @property
    def enabled(self) -> bool:
        return bool(self._config.enabled)

    def start(self) -> dict[str, Any]:
        """Start the background writer thread when the feature is enabled."""
        if not self.enabled:
            return self.get_snapshot()
        with self._state_lock:
            if self._thread is not None and self._thread.is_alive():
                return self.get_snapshot()
            self._stop.clear()
            self._thread = threading.Thread(target=self._run, name="async-price-writer", daemon=True)
            self._thread.start()
        record_component_health(
            "async_price_writer",
            ok=True,
            status="ok",
            detail="writer_started",
            extra={"enabled": bool(self.enabled)},
        )
        return self.get_snapshot()

    def close(self, timeout_s: float = 2.0) -> dict[str, Any]:
        """Stop the background writer and return its final snapshot."""
        self._stop.set()
        with self._state_lock:
            thread = self._thread
        if thread is not None:
            thread.join(timeout=max(0.1, float(timeout_s)))
        with self._state_lock:
            self._thread = None
        return self.get_snapshot()

    def enqueue(
        self,
        *,
        prices: tuple[dict[str, Any], ...] = (),
        quotes: tuple[dict[str, Any], ...] = (),
        raw: tuple[dict[str, Any], ...] = (),
        source: str = "runtime",
    ) -> bool:
        """Queue one batch of persistence rows for background flushing."""
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
        row_count = int(len(envelope.prices) + len(envelope.quotes) + len(envelope.raw))
        try:
            self._queue.put(envelope, timeout=float(self._config.enqueue_timeout_s))
            with self._state_lock:
                self._metrics["enqueued_batches"] = int(self._metrics.get("enqueued_batches") or 0) + 1
                self._metrics["enqueued_rows"] = int(self._metrics.get("enqueued_rows") or 0) + int(row_count)
                self._metrics["last_enqueue_ts_ms"] = int(envelope.created_ts_ms)
            return True
        except queue.Full as exc:
            with self._state_lock:
                self._metrics["dropped_batches"] = int(self._metrics.get("dropped_batches") or 0) + 1
                self._metrics["last_error"] = f"{type(exc).__name__}:{exc}"
                self._metrics["last_error_ts_ms"] = int(time.time() * 1000)
            self._dead_letter("queue_full", [envelope], error=exc)
            record_component_health(
                "async_price_writer",
                ok=False,
                status="backpressure",
                detail="queue_full",
                extra={"enabled": bool(self.enabled), "row_count": int(row_count)},
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

    def _flush(self, batch: list[PricePersistenceEnvelope]) -> None:
        combined_prices: list[dict[str, Any]] = []
        combined_quotes: list[dict[str, Any]] = []
        combined_raw: list[dict[str, Any]] = []
        for envelope in batch:
            combined_prices.extend(list(envelope.prices))
            combined_quotes.extend(list(envelope.quotes))
            combined_raw.extend(list(envelope.raw))

        total_rows = int(len(combined_prices) + len(combined_quotes) + len(combined_raw))
        last_error: BaseException | None = None
        for attempt in range(1, int(self._config.retry_attempts) + 1):
            try:
                storage = get_price_storage()
                storage.write_batch(
                    prices=tuple(combined_prices),
                    quotes=tuple(combined_quotes),
                    raw=tuple(combined_raw),
                )
                now_ts_ms = int(time.time() * 1000)
                with self._state_lock:
                    self._metrics["flushed_batches"] = int(self._metrics.get("flushed_batches") or 0) + 1
                    self._metrics["flushed_rows"] = int(self._metrics.get("flushed_rows") or 0) + int(total_rows)
                    self._metrics["last_flush_ts_ms"] = int(now_ts_ms)
                    self._metrics["last_error"] = ""
                record_component_health(
                    "async_price_writer",
                    ok=True,
                    status="ok",
                    detail="flush_ok",
                    observed_ts_ms=int(now_ts_ms),
                    extra={"enabled": bool(self.enabled), "rows": int(total_rows)},
                )
                return
            except Exception as exc:
                last_error = exc
                with self._state_lock:
                    self._metrics["retry_count"] = int(self._metrics.get("retry_count") or 0) + 1
                    self._metrics["last_error"] = f"{type(exc).__name__}:{exc}"
                    self._metrics["last_error_ts_ms"] = int(time.time() * 1000)
                if attempt >= int(self._config.retry_attempts):
                    break
                time.sleep(
                    backoff_delay_s(
                        int(attempt),
                        base_s=float(self._config.retry_base_s),
                        max_s=float(self._config.retry_max_s),
                    )
                )

        if last_error is not None:
            self._dead_letter("flush_failed", batch, error=last_error)
            record_component_health(
                "async_price_writer",
                ok=False,
                status="error",
                detail=f"{type(last_error).__name__}:{last_error}",
                observed_ts_ms=int(time.time() * 1000),
                extra={"enabled": bool(self.enabled), "rows": int(total_rows)},
            )

    def _dead_letter(self, reason: str, batch: list[PricePersistenceEnvelope], *, error: BaseException | None = None) -> None:
        now_ts_ms = int(time.time() * 1000)
        payload = {
            "ts_ms": int(now_ts_ms),
            "reason": str(reason),
            "error": (f"{type(error).__name__}:{error}" if error is not None else ""),
            "batches": [
                {
                    "source": str(envelope.source),
                    "created_ts_ms": int(envelope.created_ts_ms),
                    "prices": list(envelope.prices),
                    "quotes": list(envelope.quotes),
                    "raw": list(envelope.raw),
                }
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
            self._metrics["last_error_ts_ms"] = int(now_ts_ms)
        if error is not None:
            _warn_nonfatal("ASYNC_PRICE_WRITER_FLUSH_FAILED", error, reason=str(reason), dead_letter_path=str(self._config.dead_letter_path))

    def get_snapshot(self) -> dict[str, Any]:
        """Return queue depth, throughput counters, and error state."""
        with self._state_lock:
            metrics = dict(self._metrics)
            thread_alive = bool(self._thread is not None and self._thread.is_alive())
        return {
            "ok": (not self.enabled) or (thread_alive and not str(metrics.get("last_error") or "").strip()),
            "enabled": bool(self.enabled),
            "thread_alive": bool(thread_alive),
            "queue_depth": int(self._queue.qsize()),
            "queue_maxsize": int(self._config.queue_maxsize),
            "batch_size": int(self._config.batch_size),
            "enqueued_batches": int(metrics.get("enqueued_batches") or 0),
            "enqueued_rows": int(metrics.get("enqueued_rows") or 0),
            "flushed_batches": int(metrics.get("flushed_batches") or 0),
            "flushed_rows": int(metrics.get("flushed_rows") or 0),
            "dead_letters": int(metrics.get("dead_letters") or 0),
            "retry_count": int(metrics.get("retry_count") or 0),
            "dropped_batches": int(metrics.get("dropped_batches") or 0),
            "last_enqueue_ts_ms": (int(metrics.get("last_enqueue_ts_ms") or 0) or None),
            "last_flush_ts_ms": (int(metrics.get("last_flush_ts_ms") or 0) or None),
            "last_error": str(metrics.get("last_error") or ""),
            "last_error_ts_ms": (int(metrics.get("last_error_ts_ms") or 0) or None),
            "dead_letter_path": str(self._config.dead_letter_path),
            "ts_ms": int(time.time() * 1000),
        }


def get_async_writer() -> AsyncPriceWriter:
    """Return the process-wide async writer singleton."""
    global _WRITER
    if _WRITER is None:
        with _WRITER_LOCK:
            if _WRITER is None:
                _WRITER = AsyncPriceWriter()
    return _WRITER


def init_async_writer() -> dict[str, Any]:
    """Start the process-wide async writer and return its snapshot."""
    try:
        return get_async_writer().start()
    except Exception as exc:
        _warn_nonfatal("ASYNC_PRICE_WRITER_INIT_FAILED", exc)
        return {
            "ok": False,
            "enabled": bool(AsyncPriceWriterConfig.from_env().enabled),
            "last_error": f"{type(exc).__name__}:{exc}",
            "ts_ms": int(time.time() * 1000),
        }


def shutdown_async_writer(timeout_s: float = 2.0) -> dict[str, Any]:
    """Stop the process-wide async writer and return its final snapshot."""
    global _WRITER
    with _WRITER_LOCK:
        writer = _WRITER
        _WRITER = None
    if writer is None:
        return {
            "ok": True,
            "enabled": False,
            "thread_alive": False,
            "queue_depth": 0,
            "detail": "async_writer_not_started",
            "ts_ms": int(time.time() * 1000),
        }
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
    """Queue price-related persistence rows with the process-wide async writer."""
    return get_async_writer().enqueue(prices=prices, quotes=quotes, raw=raw, source=source)


__all__ = [
    "AsyncPriceWriter",
    "AsyncPriceWriterConfig",
    "enqueue_price_persistence",
    "get_async_writer",
    "init_async_writer",
    "shutdown_async_writer",
]
