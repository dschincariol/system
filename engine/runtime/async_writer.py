"""Durable SQLite-spooled async persistence for live price fanout."""

from __future__ import annotations

import json
import logging
import math
import os
import hashlib
import threading
import time
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

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
    worker_count: int = 4
    high_watermark_ratio: float = 0.75
    shutdown_drain_max_s: float = 30.0
    spool_path: str = ""
    spool_max_bytes: int = 256 * 1024 * 1024
    spool_busy_timeout_ms: int = 50
    spool_synchronous: str = "NORMAL"

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
            worker_count=tuned_int("ASYNC_PRICE_WRITER_WORKERS", 4, 1, 16),
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
            spool_synchronous=str(os.environ.get("ASYNC_PRICE_WRITER_SPOOL_SYNCHRONOUS") or "NORMAL").strip().upper(),
        )


@dataclass(frozen=True)
class PricePersistenceEnvelope:
    prices: tuple[Mapping[str, Any], ...]
    quotes: tuple[Mapping[str, Any], ...]
    raw: tuple[Mapping[str, Any], ...]
    source: str
    created_ts_ms: int
    shard_id: int = 0


class AsyncPriceWriter:
    """SQLite-spooled batch writer for append-heavy market-data persistence."""

    def __init__(self, config: AsyncPriceWriterConfig | None = None):
        self._config = config or AsyncPriceWriterConfig.from_env()
        self._spool = SQLitePriceWriterSpool(
            path=str(self._config.spool_path or default_spool_path()),
            max_envelopes=max(1, int(self._config.queue_maxsize)),
            max_bytes=int(self._config.spool_max_bytes),
            busy_timeout_ms=int(self._config.spool_busy_timeout_ms),
            synchronous=str(self._config.spool_synchronous or "NORMAL"),
        )
        self._startup_replay_high_watermark_id = 0
        self._threads: list[threading.Thread] = []
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
            "backpressure_recovered_events": 0,
            "last_backpressure_recovered_ts_ms": 0,
            "last_backpressure_recovered_reason": "",
            "dropped_batches": 0,
            "dropped_rows": 0,
            "rejected_batches": 0,
            "rejected_rows": 0,
            "spooled_batches": 0,
            "spooled_rows": 0,
            "residual_dropped_batches": 0,
            "residual_dropped_rows": 0,
            "residual_spooled_batches": 0,
            "residual_spooled_rows": 0,
            "replayed_batches": 0,
            "replayed_rows": 0,
            "shutdown_drained_batches": 0,
            "shutdown_drained_rows": 0,
            "startup_pending_batches": 0,
            "startup_pending_rows": 0,
            "startup_replay_high_watermark_id": 0,
            "spool_deleted_batches": 0,
            "spool_deleted_rows": 0,
            "spool_delete_failures": 0,
            "spool_enqueue_failures": 0,
            "spool_corrupt_rows": 0,
            "spool_corrupt_payload_rows": 0,
            "spool_corruption_events": 0,
            "last_flush_latency_ms": 0,
            "total_flush_latency_ms": 0,
            "last_db_write_duration_ms": 0,
            "total_db_write_duration_ms": 0,
            "write_failures": 0,
            "last_shutdown_drain_duration_ms": 0,
            "last_shutdown_drain_deadline_ms": 0,
            "row_copy_avoided_rows": 0,
            "row_copy_fallback_rows": 0,
            "inflight_batches": 0,
            "inflight_rows": 0,
            "last_enqueue_ts_ms": 0,
            "last_flush_ts_ms": 0,
            "last_error": "",
            "last_error_ts_ms": 0,
        }
        self._shard_metrics: list[dict[str, Any]] = [
            self._new_shard_metrics(shard_id)
            for shard_id in range(self._worker_count())
        ]

    @property
    def enabled(self) -> bool:
        return bool(self._config.enabled)

    def _worker_count(self) -> int:
        return max(1, int(getattr(self._config, "worker_count", 1) or 1))

    def _new_shard_metrics(self, shard_id: int) -> dict[str, Any]:
        return {
            "shard_id": int(shard_id),
            "enqueued_batches": 0,
            "enqueued_rows": 0,
            "flushed_batches": 0,
            "flushed_rows": 0,
            "retry_count": 0,
            "backpressure_events": 0,
            "last_backpressure_reason": "",
            "last_backpressure_ts_ms": 0,
            "dropped_batches": 0,
            "dropped_rows": 0,
            "inflight_batches": 0,
            "inflight_rows": 0,
            "last_batch_envelopes": 0,
            "last_batch_rows": 0,
            "last_enqueue_ts_ms": 0,
            "last_flush_ts_ms": 0,
            "last_flush_latency_ms": 0,
            "last_db_write_duration_ms": 0,
            "write_failures": 0,
            "last_error": "",
            "last_error_ts_ms": 0,
        }

    def _ensure_shard_metrics_locked(self) -> None:
        worker_count = self._worker_count()
        while len(self._shard_metrics) < worker_count:
            self._shard_metrics.append(self._new_shard_metrics(len(self._shard_metrics)))
        if len(self._shard_metrics) > worker_count:
            self._shard_metrics = self._shard_metrics[:worker_count]

    def _shard_metric_update(self, shard_id: int, **updates: Any) -> None:
        with self._state_lock:
            self._ensure_shard_metrics_locked()
            if not 0 <= int(shard_id) < len(self._shard_metrics):
                return
            self._shard_metrics[int(shard_id)].update(dict(updates))

    def _validate_price_storage_pool_capacity(self) -> None:
        worker_count = self._worker_count()
        if worker_count <= 1:
            return
        storage = get_price_storage()
        snapshot_fn = getattr(storage, "get_snapshot", None)
        if not callable(snapshot_fn):
            return
        snapshot = dict(snapshot_fn() or {})
        if not bool(snapshot.get("enabled")):
            return
        pool_max_size = int(snapshot.get("pool_max_size") or 0)
        if pool_max_size < worker_count:
            raise RuntimeError(
                "async_price_writer_pool_too_small:"
                f"TIMESCALE_PRICES_POOL_MAX_SIZE={pool_max_size};"
                f"ASYNC_PRICE_WRITER_WORKERS={worker_count}"
            )

    def start(self) -> dict[str, Any]:
        if not self.enabled:
            return self.get_snapshot()
        with self._state_lock:
            if any(thread.is_alive() for thread in self._threads):
                return self.get_snapshot()
            self._validate_price_storage_pool_capacity()
            self._spool.open()
            spool_stats = dict(self._spool.stats() or {})
            self._startup_replay_high_watermark_id = int(spool_stats.get("newest_id") or 0)
            self._metrics["startup_pending_batches"] = int(spool_stats.get("pending_batches") or 0)
            self._metrics["startup_pending_rows"] = int(spool_stats.get("pending_rows") or 0)
            self._metrics["startup_replay_high_watermark_id"] = int(self._startup_replay_high_watermark_id)
            self._metrics["spool_corruption_events"] = int(spool_stats.get("corruption_events") or 0)
            self._ensure_shard_metrics_locked()
            self._stop.clear()
            self._threads = []
            for shard_id in range(self._worker_count()):
                thread = threading.Thread(
                    target=self._run_shard,
                    args=(int(shard_id),),
                    name=f"async-price-writer-{int(shard_id)}",
                    daemon=True,
                )
                self._threads.append(thread)
                thread.start()
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
                "workers": int(self._worker_count()),
            },
        )
        return self.get_snapshot()

    def close(self, timeout_s: float = 2.0) -> dict[str, Any]:
        self._stop.set()
        drain_started = time.perf_counter()
        deadline_s = self._shutdown_drain_budget_s(timeout_s=timeout_s)
        deadline = time.monotonic() + float(deadline_s)
        with self._state_lock:
            threads = list(self._threads)
        for thread in threads:
            remaining_s = max(0.0, float(deadline) - time.monotonic())
            if remaining_s <= 0.0:
                break
            thread.join(timeout=remaining_s)
        with self._state_lock:
            live_threads = [thread for thread in self._threads if thread.is_alive()]
            if not live_threads:
                self._threads = []
        if not live_threads and int(self._spool_stats().get("pending_batches") or 0) > 0:
            self._drain_residual_until(deadline)
        self._record_residual_after_deadline(reason="shutdown_deadline")
        with self._state_lock:
            thread_alive = any(thread.is_alive() for thread in self._threads)
            if not thread_alive:
                self._threads = []
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
        prices: tuple[Mapping[str, Any], ...] = (),
        quotes: tuple[Mapping[str, Any], ...] = (),
        raw: tuple[Mapping[str, Any], ...] = (),
        source: str = "runtime",
    ) -> bool:
        if not self.enabled:
            return False
        if not prices and not quotes and not raw:
            return True
        self.start()
        created_ts_ms = int(time.time() * 1000)
        envelopes, row_copy_avoided_rows, row_copy_fallback_rows = self._sharded_envelopes(
            prices=prices,
            quotes=quotes,
            raw=raw,
            source=source,
            created_ts_ms=created_ts_ms,
        )
        accepted_all = True
        for envelope in envelopes:
            row_count = self._row_count(envelope)
            try:
                spool_stats = self._spool.enqueue(
                    source=envelope.source,
                    created_ts_ms=int(envelope.created_ts_ms),
                    shard_id=int(envelope.shard_id),
                    prices=envelope.prices,
                    quotes=envelope.quotes,
                    raw=envelope.raw,
                )
                depth = int(spool_stats.get("pending_batches") or 0)
                with self._state_lock:
                    self._metrics["enqueued_batches"] = int(self._metrics.get("enqueued_batches") or 0) + 1
                    self._metrics["enqueued_rows"] = int(self._metrics.get("enqueued_rows") or 0) + row_count
                    self._metrics["spooled_batches"] = int(self._metrics.get("spooled_batches") or 0) + 1
                    self._metrics["spooled_rows"] = int(self._metrics.get("spooled_rows") or 0) + row_count
                    self._metrics["last_enqueue_ts_ms"] = int(envelope.created_ts_ms)
                    self._ensure_shard_metrics_locked()
                    shard_metrics = self._shard_metrics[int(envelope.shard_id)]
                    shard_metrics["enqueued_batches"] = int(shard_metrics.get("enqueued_batches") or 0) + 1
                    shard_metrics["enqueued_rows"] = int(shard_metrics.get("enqueued_rows") or 0) + row_count
                    shard_metrics["last_enqueue_ts_ms"] = int(envelope.created_ts_ms)
                self._emit_queue_depth(depth, spool_stats=spool_stats)
                emit_counter(
                    "async_price_writer_spooled_rows",
                    row_count,
                    component="engine.runtime.async_writer",
                    extra_tags={"shard": int(envelope.shard_id)},
                )
                if self._queue_at_high_watermark(depth, pending_bytes=int(spool_stats.get("pending_bytes") or 0)):
                    self._note_backpressure(
                        "high_watermark",
                        row_count=row_count,
                        queue_depth=depth,
                        pending_bytes=int(spool_stats.get("pending_bytes") or 0),
                        shard_id=int(envelope.shard_id),
                    )
                else:
                    self._clear_backpressure_if_recovered(
                        depth,
                        pending_bytes=int(spool_stats.get("pending_bytes") or 0),
                    )
            except Exception as exc:
                accepted_all = False
                reason = "spool_full" if isinstance(exc, PriceWriterSpoolFullError) else "spool_unavailable"
                depth = int(self._spool_stats().get("pending_batches") or 0)
                with self._state_lock:
                    self._metrics["dropped_batches"] = int(self._metrics.get("dropped_batches") or 0) + 1
                    self._metrics["dropped_rows"] = int(self._metrics.get("dropped_rows") or 0) + row_count
                    self._metrics["rejected_batches"] = int(self._metrics.get("rejected_batches") or 0) + 1
                    self._metrics["rejected_rows"] = int(self._metrics.get("rejected_rows") or 0) + row_count
                    self._metrics["spool_enqueue_failures"] = int(self._metrics.get("spool_enqueue_failures") or 0) + 1
                    self._metrics["last_error"] = f"{type(exc).__name__}:{exc}"
                    self._metrics["last_error_ts_ms"] = int(time.time() * 1000)
                    self._ensure_shard_metrics_locked()
                    shard_metrics = self._shard_metrics[int(envelope.shard_id)]
                    shard_metrics["dropped_batches"] = int(shard_metrics.get("dropped_batches") or 0) + 1
                    shard_metrics["dropped_rows"] = int(shard_metrics.get("dropped_rows") or 0) + row_count
                    shard_metrics["last_error"] = f"{type(exc).__name__}:{exc}"
                    shard_metrics["last_error_ts_ms"] = int(time.time() * 1000)
                self._note_backpressure(
                    reason,
                    row_count=row_count,
                    queue_depth=depth,
                    pending_bytes=int(self._spool_stats().get("pending_bytes") or 0),
                    shard_id=int(envelope.shard_id),
                )
                emit_counter(
                    "async_price_writer_dropped_rows",
                    row_count,
                    component="engine.runtime.async_writer",
                    extra_tags={"reason": reason, "shard": int(envelope.shard_id)},
                )
                emit_counter(
                    "async_price_writer_rejected_rows",
                    row_count,
                    component="engine.runtime.async_writer",
                    extra_tags={"reason": reason, "shard": int(envelope.shard_id)},
                )
                emit_counter(
                    "async_price_writer_spool_enqueue_failures",
                    1,
                    component="engine.runtime.async_writer",
                    extra_tags={"reason": reason, "shard": int(envelope.shard_id)},
                )
                self._dead_letter(reason, [envelope], error=exc)
        if row_copy_avoided_rows or row_copy_fallback_rows:
            with self._state_lock:
                self._metrics["row_copy_avoided_rows"] = int(
                    self._metrics.get("row_copy_avoided_rows") or 0
                ) + int(row_copy_avoided_rows)
                self._metrics["row_copy_fallback_rows"] = int(
                    self._metrics.get("row_copy_fallback_rows") or 0
                ) + int(row_copy_fallback_rows)
            if row_copy_avoided_rows:
                emit_counter(
                    "async_price_writer_row_copies_avoided",
                    int(row_copy_avoided_rows),
                    component="engine.runtime.async_writer",
                )
            if row_copy_fallback_rows:
                emit_counter(
                    "async_price_writer_row_copy_fallback_rows",
                    int(row_copy_fallback_rows),
                    component="engine.runtime.async_writer",
                )
        return bool(accepted_all)

    def _row_mapping(self, row: Any) -> tuple[Mapping[str, Any], bool]:
        if isinstance(row, Mapping):
            return row, False
        return dict(row or {}), True

    def _row_shard_key(self, row: Mapping[str, Any]) -> str:
        symbol = str(row.get("symbol") or "").strip().upper()
        if symbol:
            return f"symbol:{symbol}"
        event_key = str(row.get("event_key") or "").strip()
        if event_key:
            provider = str(row.get("provider") or row.get("source") or "").strip().lower()
            return f"event:{provider}:{event_key}"
        provider = str(row.get("provider") or row.get("source") or "").strip().lower()
        sequence = str(row.get("sequence_number") or row.get("trade_id") or "").strip()
        timestamp = str(row.get("event_ts_ms") or row.get("ts_ms") or row.get("timestamp") or "").strip()
        return f"fallback:{provider}:{sequence}:{timestamp}"

    def _shard_for_row(self, row: Mapping[str, Any]) -> int:
        worker_count = self._worker_count()
        if worker_count <= 1:
            return 0
        key = self._row_shard_key(row)
        digest = hashlib.blake2b(key.encode("utf-8", errors="ignore"), digest_size=8).digest()
        return int(int.from_bytes(digest, "big", signed=False) % worker_count)

    def _sharded_envelopes(
        self,
        *,
        prices: tuple[Mapping[str, Any], ...],
        quotes: tuple[Mapping[str, Any], ...],
        raw: tuple[Mapping[str, Any], ...],
        source: str,
        created_ts_ms: int,
    ) -> tuple[list[PricePersistenceEnvelope], int, int]:
        groups: dict[int, dict[str, list[Mapping[str, Any]]]] = {}
        row_copy_avoided_rows = 0
        row_copy_fallback_rows = 0

        def _append(kind: str, row: Any) -> None:
            nonlocal row_copy_avoided_rows, row_copy_fallback_rows
            clean, copied = self._row_mapping(row)
            if copied:
                row_copy_fallback_rows += 1
            else:
                row_copy_avoided_rows += 1
            shard_id = int(self._shard_for_row(clean))
            bucket = groups.setdefault(shard_id, {"prices": [], "quotes": [], "raw": []})
            bucket[kind].append(clean)

        for row in prices or ():
            _append("prices", row)
        for row in quotes or ():
            _append("quotes", row)
        for row in raw or ():
            _append("raw", row)
        envelopes: list[PricePersistenceEnvelope] = []
        for shard_id in sorted(groups):
            bucket = groups[shard_id]
            envelopes.append(
                PricePersistenceEnvelope(
                    prices=tuple(bucket.get("prices") or ()),
                    quotes=tuple(bucket.get("quotes") or ()),
                    raw=tuple(bucket.get("raw") or ()),
                    source=str(source or "runtime"),
                    created_ts_ms=int(created_ts_ms),
                    shard_id=int(shard_id),
                )
            )
        return envelopes, row_copy_avoided_rows, row_copy_fallback_rows

    def _run_shard(self, shard_id: int) -> None:
        while True:
            try:
                select_shard_id = int(shard_id)
                if int(shard_id) == 0:
                    if self._legacy_spool_pending():
                        select_shard_id = -1
                elif self._legacy_spool_pending():
                    if self._stop.is_set():
                        return
                    if self._stop.wait(float(self._config.flush_interval_s)):
                        return
                    continue
                batch, corrupt = self._spool.select_batch(
                    limit=int(self._config.batch_size),
                    shard_id=int(select_shard_id),
                )
            except Exception as exc:
                with self._state_lock:
                    self._metrics["last_error"] = f"{type(exc).__name__}:{exc}"
                    self._metrics["last_error_ts_ms"] = int(time.time() * 1000)
                    self._ensure_shard_metrics_locked()
                    shard_metrics = self._shard_metrics[int(shard_id)]
                    shard_metrics["last_error"] = f"{type(exc).__name__}:{exc}"
                    shard_metrics["last_error_ts_ms"] = int(time.time() * 1000)
                _warn_nonfatal("ASYNC_PRICE_WRITER_SPOOL_SELECT_FAILED", exc)
                if self._stop.is_set():
                    return
                if self._stop.wait(float(self._config.flush_interval_s)):
                    return
                continue
            if corrupt:
                self._dead_letter_corrupt_spool_records(corrupt)
                self._delete_spool_rows(
                    (record.id for record in corrupt),
                    reason="corrupt_payload",
                    shard_id=int(shard_id),
                    row_count=sum(int(record.total_rows) for record in corrupt),
                )
            if self._stop.is_set():
                return
            flushed = self._flush(batch, shard_id=int(shard_id), honor_stop=True) if batch else True
            if (
                self._stop.is_set()
                and int(self._spool_stats(shard_id=int(shard_id)).get("pending_batches") or 0) <= 0
                and (int(shard_id) != 0 or not self._legacy_spool_pending())
            ):
                return
            if self._stop.is_set() and batch and not flushed:
                return
            if batch and not flushed:
                if self._stop.wait(
                    backoff_delay_s(1, base_s=float(self._config.retry_base_s), max_s=float(self._config.retry_max_s))
                ):
                    return
            elif not batch and not corrupt:
                if self._stop.wait(float(self._config.flush_interval_s)):
                    return

    def _flush(
        self,
        batch: list[PriceWriterSpoolRecord],
        *,
        shard_id: int,
        honor_stop: bool = False,
    ) -> bool:
        if bool(honor_stop) and self._stop.is_set():
            return False
        combined_prices: list[Mapping[str, Any]] = []
        combined_quotes: list[Mapping[str, Any]] = []
        combined_raw: list[Mapping[str, Any]] = []
        spool_ids = [int(record.id) for record in batch]
        for record in batch:
            combined_prices.extend(record.prices)
            combined_quotes.extend(record.quotes)
            combined_raw.extend(record.raw)
        total_rows = int(len(combined_prices) + len(combined_quotes) + len(combined_raw))
        flush_started = time.perf_counter()
        with self._state_lock:
            self._metrics["inflight_batches"] = int(self._metrics.get("inflight_batches") or 0) + int(len(batch))
            self._metrics["inflight_rows"] = int(self._metrics.get("inflight_rows") or 0) + int(total_rows)
            self._ensure_shard_metrics_locked()
            shard_metrics = self._shard_metrics[int(shard_id)]
            shard_metrics["inflight_batches"] = int(len(batch))
            shard_metrics["inflight_rows"] = int(total_rows)
            shard_metrics["last_batch_envelopes"] = int(len(batch))
            shard_metrics["last_batch_rows"] = int(total_rows)
        emit_gauge(
            "async_price_writer_shard_last_batch_envelopes",
            int(len(batch)),
            component="engine.runtime.async_writer",
            extra_tags={"shard": int(shard_id)},
        )
        emit_gauge(
            "async_price_writer_shard_last_batch_rows",
            int(total_rows),
            component="engine.runtime.async_writer",
            extra_tags={"shard": int(shard_id)},
        )
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
                deleted = self._delete_spool_rows(
                    spool_ids,
                    reason="flush_ok",
                    shard_id=int(shard_id),
                    row_count=int(total_rows),
                )
                replayed_batch_count = self._startup_replay_batch_count(batch)
                replayed_row_count = self._startup_replay_row_count(batch)
                db_write_duration_ms = float(write_result_dict.get("write_duration_ms") or ((time.perf_counter() - write_started) * 1000.0))
                flush_latency_ms = float((time.perf_counter() - flush_started) * 1000.0)
                now_ts_ms = int(time.time() * 1000)
                spool_stats = self._spool_stats()
                queue_depth = int(spool_stats.get("pending_batches") or 0)
                with self._state_lock:
                    self._metrics["flushed_batches"] = int(self._metrics.get("flushed_batches") or 0) + 1
                    self._metrics["flushed_rows"] = int(self._metrics.get("flushed_rows") or 0) + total_rows
                    self._metrics["replayed_batches"] = int(self._metrics.get("replayed_batches") or 0) + int(replayed_batch_count)
                    self._metrics["replayed_rows"] = int(self._metrics.get("replayed_rows") or 0) + int(replayed_row_count)
                    self._metrics["last_flush_ts_ms"] = now_ts_ms
                    self._metrics["last_flush_latency_ms"] = int(round(flush_latency_ms))
                    self._metrics["total_flush_latency_ms"] = int(self._metrics.get("total_flush_latency_ms") or 0) + int(round(flush_latency_ms))
                    self._metrics["last_db_write_duration_ms"] = int(round(db_write_duration_ms))
                    self._metrics["total_db_write_duration_ms"] = int(self._metrics.get("total_db_write_duration_ms") or 0) + int(round(db_write_duration_ms))
                    self._metrics["last_error"] = ""
                    self._metrics["inflight_batches"] = max(0, int(self._metrics.get("inflight_batches") or 0) - int(len(batch)))
                    self._metrics["inflight_rows"] = max(0, int(self._metrics.get("inflight_rows") or 0) - int(total_rows))
                    self._ensure_shard_metrics_locked()
                    shard_metrics = self._shard_metrics[int(shard_id)]
                    shard_metrics["flushed_batches"] = int(shard_metrics.get("flushed_batches") or 0) + 1
                    shard_metrics["flushed_rows"] = int(shard_metrics.get("flushed_rows") or 0) + int(total_rows)
                    shard_metrics["last_flush_ts_ms"] = now_ts_ms
                    shard_metrics["last_flush_latency_ms"] = int(round(flush_latency_ms))
                    shard_metrics["last_db_write_duration_ms"] = int(round(db_write_duration_ms))
                    shard_metrics["last_error"] = ""
                    shard_metrics["inflight_batches"] = 0
                    shard_metrics["inflight_rows"] = 0
                emit_timing("async_price_writer_flush_latency_ms", flush_latency_ms, component="engine.runtime.async_writer", extra_tags={"shard": int(shard_id)})
                emit_timing("async_price_writer_db_write_duration_ms", db_write_duration_ms, component="engine.runtime.async_writer", extra_tags={"shard": int(shard_id)})
                if replayed_row_count > 0:
                    emit_counter(
                        "async_price_writer_replayed_rows",
                        int(replayed_row_count),
                        component="engine.runtime.async_writer",
                        extra_tags={"shard": int(shard_id)},
                    )
                self._emit_queue_depth(queue_depth, spool_stats=spool_stats)
                self._clear_backpressure_if_recovered(
                    queue_depth,
                    pending_bytes=int(spool_stats.get("pending_bytes") or 0),
                )
                record_component_health("async_price_writer", ok=True, status="ok", detail="flush_ok", observed_ts_ms=now_ts_ms, latency_ms=flush_latency_ms, extra={"enabled": bool(self.enabled), "rows": total_rows, "queue_depth": queue_depth, "spool_deleted_batches": int(deleted), "shard": int(shard_id)})
                return True
            except Exception as exc:
                last_error = exc
                with self._state_lock:
                    self._metrics["retry_count"] = int(self._metrics.get("retry_count") or 0) + 1
                    self._metrics["last_error"] = f"{type(exc).__name__}:{exc}"
                    self._metrics["last_error_ts_ms"] = int(time.time() * 1000)
                    self._ensure_shard_metrics_locked()
                    shard_metrics = self._shard_metrics[int(shard_id)]
                    shard_metrics["retry_count"] = int(shard_metrics.get("retry_count") or 0) + 1
                    shard_metrics["last_error"] = f"{type(exc).__name__}:{exc}"
                    shard_metrics["last_error_ts_ms"] = int(time.time() * 1000)
                emit_counter("async_price_writer_retries", 1, component="engine.runtime.async_writer", extra_tags={"attempt": int(attempt), "shard": int(shard_id)})
                if bool(honor_stop) and self._stop.is_set():
                    break
                if attempt < int(self._config.retry_attempts):
                    delay_s = backoff_delay_s(
                        attempt,
                        base_s=float(self._config.retry_base_s),
                        max_s=float(self._config.retry_max_s),
                    )
                    if self._stop.wait(delay_s):
                        break
        with self._state_lock:
            self._metrics["write_failures"] = int(self._metrics.get("write_failures") or 0) + 1
            self._metrics["inflight_batches"] = max(0, int(self._metrics.get("inflight_batches") or 0) - int(len(batch)))
            self._metrics["inflight_rows"] = max(0, int(self._metrics.get("inflight_rows") or 0) - int(total_rows))
            self._ensure_shard_metrics_locked()
            shard_metrics = self._shard_metrics[int(shard_id)]
            shard_metrics["write_failures"] = int(shard_metrics.get("write_failures") or 0) + 1
            shard_metrics["inflight_batches"] = 0
            shard_metrics["inflight_rows"] = 0
        emit_counter("async_price_writer_spool_write_failures", 1, component="engine.runtime.async_writer", extra_tags={"shard": int(shard_id)})
        if last_error is not None:
            record_component_health("async_price_writer", ok=False, status="error", detail=f"{type(last_error).__name__}:{last_error}", observed_ts_ms=int(time.time() * 1000), extra={"enabled": bool(self.enabled), "rows": total_rows, "spool_rows_retained": True, "shard": int(shard_id)})
        return False

    def _delete_spool_rows(
        self,
        ids: Iterable[int],
        *,
        reason: str,
        shard_id: int | None = None,
        row_count: int = 0,
    ) -> int:
        clean_ids = [int(row_id) for row_id in ids if int(row_id) > 0]
        if not clean_ids:
            return 0
        try:
            deleted = int(self._spool.delete(clean_ids))
            with self._state_lock:
                self._metrics["spool_deleted_batches"] = int(
                    self._metrics.get("spool_deleted_batches") or 0
                ) + int(deleted)
                self._metrics["spool_deleted_rows"] = int(self._metrics.get("spool_deleted_rows") or 0) + int(row_count)
            emit_counter(
                "async_price_writer_spool_deleted_batches",
                int(deleted),
                component="engine.runtime.async_writer",
                extra_tags={"reason": str(reason), "shard": "" if shard_id is None else int(shard_id)},
            )
            emit_counter(
                "async_price_writer_spool_deleted_rows",
                int(row_count),
                component="engine.runtime.async_writer",
                extra_tags={"reason": str(reason), "shard": "" if shard_id is None else int(shard_id)},
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
                extra_tags={"reason": str(reason), "shard": "" if shard_id is None else int(shard_id)},
            )
            raise

    def _row_count(self, envelope: PricePersistenceEnvelope | PriceWriterSpoolRecord) -> int:
        return int(len(envelope.prices) + len(envelope.quotes) + len(envelope.raw))

    def _batch_row_count(self, batch: list[PriceWriterSpoolRecord]) -> int:
        return int(sum(self._row_count(record) for record in list(batch or [])))

    def _startup_replay_batch_count(self, batch: list[PriceWriterSpoolRecord]) -> int:
        high_watermark_id = int(self._startup_replay_high_watermark_id or 0)
        if high_watermark_id <= 0:
            return 0
        return int(sum(1 for record in list(batch or []) if int(record.id) <= high_watermark_id))

    def _startup_replay_row_count(self, batch: list[PriceWriterSpoolRecord]) -> int:
        high_watermark_id = int(self._startup_replay_high_watermark_id or 0)
        if high_watermark_id <= 0:
            return 0
        return int(
            sum(
                self._row_count(record)
                for record in list(batch or [])
                if int(record.id) <= high_watermark_id
            )
        )

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
        pending_rows = int(stats.get("pending_rows") or 0)
        pending_bytes = int(stats.get("pending_bytes") or 0)
        max_bytes = max(1, int(stats.get("max_bytes") or self._config.spool_max_bytes))
        now_ts_ms = int(time.time() * 1000)
        oldest_created_ts_ms = int(stats.get("oldest_created_ts_ms") or 0)
        oldest_age_ms = max(0, int(now_ts_ms) - oldest_created_ts_ms) if oldest_created_ts_ms > 0 else 0
        emit_gauge("async_price_writer_queue_depth", int(depth), component="engine.runtime.async_writer")
        emit_gauge("async_price_writer_queue_rows", pending_rows, component="engine.runtime.async_writer")
        emit_gauge("async_price_writer_queue_fill_ratio", float(depth / maximum), component="engine.runtime.async_writer")
        emit_gauge("async_price_writer_spool_pending_bytes", pending_bytes, component="engine.runtime.async_writer")
        emit_gauge("async_price_writer_spool_bytes_fill_ratio", float(pending_bytes / max_bytes), component="engine.runtime.async_writer")
        emit_gauge("async_price_writer_spool_oldest_age_ms", oldest_age_ms, component="engine.runtime.async_writer")

        for shard in self._spool_shard_stats():
            shard_id = int(shard.get("shard_id") or 0)
            shard_depth = int(shard.get("pending_batches") or 0)
            shard_pending_rows = int(shard.get("pending_rows") or 0)
            shard_pending_bytes = int(shard.get("pending_bytes") or 0)
            shard_oldest_created_ts_ms = int(shard.get("oldest_created_ts_ms") or 0)
            shard_pending_lag_ms = (
                max(0, int(now_ts_ms) - shard_oldest_created_ts_ms)
                if shard_oldest_created_ts_ms > 0
                else 0
            )
            emit_gauge(
                "async_price_writer_shard_queue_depth",
                shard_depth,
                component="engine.runtime.async_writer",
                extra_tags={"shard": shard_id},
            )
            emit_gauge(
                "async_price_writer_shard_queue_fill_ratio",
                float(shard_depth / maximum),
                component="engine.runtime.async_writer",
                extra_tags={"shard": shard_id},
            )
            emit_gauge(
                "async_price_writer_shard_queue_rows",
                shard_pending_rows,
                component="engine.runtime.async_writer",
                extra_tags={"shard": shard_id},
            )
            emit_gauge(
                "async_price_writer_shard_spool_pending_bytes",
                shard_pending_bytes,
                component="engine.runtime.async_writer",
                extra_tags={"shard": shard_id},
            )
            emit_gauge(
                "async_price_writer_shard_pending_lag_ms",
                shard_pending_lag_ms,
                component="engine.runtime.async_writer",
                extra_tags={"shard": shard_id},
            )

    def _spool_stats(self, *, shard_id: int | None = None) -> dict[str, Any]:
        try:
            stats = dict(self._spool.stats(shard_id=shard_id) or {})
            with self._state_lock:
                self._metrics["spool_corruption_events"] = int(stats.get("corruption_events") or 0)
            return stats
        except Exception as exc:
            with self._state_lock:
                self._metrics["last_error"] = f"{type(exc).__name__}:{exc}"
                self._metrics["last_error_ts_ms"] = int(time.time() * 1000)
            stats = {
                "ok": False,
                "pending_batches": 0,
                "pending_rows": 0,
                "pending_bytes": 0,
                "file_bytes": 0,
                "max_envelopes": int(self._config.queue_maxsize),
                "max_bytes": int(self._config.spool_max_bytes),
                "oldest_id": None,
                "newest_id": None,
                "synchronous": str(self._spool.synchronous),
                "error": f"{type(exc).__name__}:{exc}",
            }
            if shard_id is not None:
                stats["shard_id"] = int(shard_id)
            return stats

    def _spool_shard_stats(self) -> list[dict[str, Any]]:
        try:
            return list(self._spool.stats_by_shard(shard_count=self._worker_count()) or [])
        except Exception:
            return [
                {
                    "ok": False,
                    "shard_id": shard_id,
                    "pending_batches": 0,
                    "pending_rows": 0,
                    "pending_bytes": 0,
                    "max_envelopes": int(self._config.queue_maxsize),
                    "max_bytes": int(self._config.spool_max_bytes),
                }
                for shard_id in range(self._worker_count())
            ]

    def _legacy_spool_pending(self) -> bool:
        return bool(int(self._spool_stats(shard_id=-1).get("pending_batches") or 0) > 0)

    def _note_backpressure(
        self,
        reason: str,
        *,
        row_count: int,
        queue_depth: int,
        pending_bytes: int = 0,
        shard_id: int | None = None,
    ) -> None:
        now_ts_ms = int(time.time() * 1000)
        with self._state_lock:
            self._metrics["backpressure_events"] = int(self._metrics.get("backpressure_events") or 0) + 1
            if str(reason) == "high_watermark":
                self._metrics["high_watermark_events"] = int(self._metrics.get("high_watermark_events") or 0) + 1
            self._metrics["backpressure_active"] = True
            self._metrics["last_backpressure_ts_ms"] = now_ts_ms
            self._metrics["last_backpressure_reason"] = str(reason)
            if shard_id is not None:
                self._ensure_shard_metrics_locked()
                shard_metrics = self._shard_metrics[int(shard_id)]
                shard_metrics["backpressure_events"] = int(shard_metrics.get("backpressure_events") or 0) + 1
                shard_metrics["last_backpressure_reason"] = str(reason)
                shard_metrics["last_backpressure_ts_ms"] = int(now_ts_ms)
        emit_counter("async_price_writer_backpressure_events", 1, component="engine.runtime.async_writer", extra_tags={"reason": str(reason), "shard": "" if shard_id is None else int(shard_id)})
        emit_gauge("async_price_writer_backpressure_active", 1, component="engine.runtime.async_writer")
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
                "shard": (None if shard_id is None else int(shard_id)),
            },
        )

    def _clear_backpressure_if_recovered(self, depth: int, *, pending_bytes: int | None = None) -> None:
        if self._queue_at_high_watermark(depth, pending_bytes=pending_bytes):
            return
        recovered = False
        previous_reason = ""
        now_ts_ms = int(time.time() * 1000)
        with self._state_lock:
            if bool(self._metrics.get("backpressure_active")):
                recovered = True
                previous_reason = str(self._metrics.get("last_backpressure_reason") or "")
                self._metrics["backpressure_active"] = False
                self._metrics["backpressure_recovered_events"] = int(
                    self._metrics.get("backpressure_recovered_events") or 0
                ) + 1
                self._metrics["last_backpressure_recovered_ts_ms"] = now_ts_ms
                self._metrics["last_backpressure_recovered_reason"] = previous_reason
        if recovered:
            emit_counter(
                "async_price_writer_backpressure_recovered_events",
                1,
                component="engine.runtime.async_writer",
                extra_tags={"reason": previous_reason or "unknown"},
            )
            emit_gauge("async_price_writer_backpressure_active", 0, component="engine.runtime.async_writer")
            record_component_health(
                "async_price_writer",
                ok=True,
                status="ok",
                detail="backpressure_recovered",
                observed_ts_ms=now_ts_ms,
                extra={
                    "enabled": bool(self.enabled),
                    "queue_depth": int(depth),
                    "spool_pending_bytes": int(pending_bytes or 0),
                    "previous_reason": previous_reason,
                },
            )

    def _shutdown_drain_budget_s(self, *, timeout_s: float) -> float:
        queue_depth = int(self._spool_stats().get("pending_batches") or 0)
        batch_count = int(math.ceil(queue_depth / max(1, int(self._config.batch_size)))) if queue_depth > 0 else 0
        with self._state_lock:
            last_write_ms = int(self._metrics.get("last_db_write_duration_ms") or 0)
        per_batch_s = max(float(last_write_ms) / 1000.0, min(float(self._config.flush_interval_s), 0.25), 0.01)
        return min(max(0.0, float(self._config.shutdown_drain_max_s)), max(max(0.0, float(timeout_s)), batch_count * per_batch_s))

    def _drain_residual_until(self, deadline: float) -> None:
        while time.monotonic() < float(deadline):
            progressed = False
            if self._legacy_spool_pending():
                if self._drain_residual_shard_once(shard_id=-1):
                    progressed = True
                    continue
            for shard_id in range(self._worker_count()):
                if time.monotonic() >= float(deadline):
                    return
                if self._drain_residual_shard_once(shard_id=shard_id):
                    progressed = True
            if not progressed:
                return

    def _drain_residual_shard_once(self, *, shard_id: int) -> bool:
        batch, corrupt = self._spool.select_batch(
            limit=int(self._config.batch_size),
            shard_id=int(shard_id),
        )
        if corrupt:
            self._dead_letter_corrupt_spool_records(corrupt)
            self._delete_spool_rows(
                (record.id for record in corrupt),
                reason="corrupt_payload",
                shard_id=max(0, int(shard_id)),
                row_count=sum(int(record.total_rows) for record in corrupt),
            )
        if not batch:
            return bool(corrupt)
        rows = self._batch_row_count(batch)
        if self._flush(batch, shard_id=max(0, int(shard_id))):
            with self._state_lock:
                self._metrics["shutdown_drained_batches"] = int(self._metrics.get("shutdown_drained_batches") or 0) + len(batch)
                self._metrics["shutdown_drained_rows"] = int(self._metrics.get("shutdown_drained_rows") or 0) + rows
            return True
        return False

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
            "batches": [{"source": envelope.source, "created_ts_ms": int(envelope.created_ts_ms), "shard_id": int(envelope.shard_id), "prices": list(envelope.prices), "quotes": list(envelope.quotes), "raw": list(envelope.raw)} for envelope in batch],
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
        corrupt_payload_rows = int(sum(int(record.total_rows) for record in records))
        payload = {
            "ts_ms": int(time.time() * 1000),
            "reason": "spool_payload_corrupt",
            "spool_path": str(self._spool.path),
            "records": [{"id": int(record.id), "shard_id": int(record.shard_id), "created_ts_ms": int(record.created_ts_ms), "total_rows": int(record.total_rows), "payload_bytes": int(record.payload_bytes), "error": str(record.error), "payload_json": str(record.payload_json)} for record in records],
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
            self._metrics["spool_corrupt_payload_rows"] = int(self._metrics.get("spool_corrupt_payload_rows") or 0) + corrupt_payload_rows
            self._metrics["dropped_batches"] = int(self._metrics.get("dropped_batches") or 0) + len(records)
            self._metrics["dropped_rows"] = int(self._metrics.get("dropped_rows") or 0) + corrupt_payload_rows
            self._metrics["last_error"] = "spool_payload_corrupt"
            self._metrics["last_error_ts_ms"] = int(payload["ts_ms"])
        emit_counter("async_price_writer_spool_corrupt_rows", len(records), component="engine.runtime.async_writer")
        emit_counter(
            "async_price_writer_dropped_rows",
            corrupt_payload_rows,
            component="engine.runtime.async_writer",
            extra_tags={"reason": "spool_payload_corrupt"},
        )

    def get_snapshot(self) -> dict[str, Any]:
        spool_stats = self._spool_stats()
        shard_spool_stats = self._spool_shard_stats()
        legacy_spool_stats = self._spool_stats(shard_id=-1)
        queue_depth = int(spool_stats.get("pending_batches") or 0)
        queue_maxsize = max(1, int(self._config.queue_maxsize))
        high_watermark_depth = int(self._high_watermark_depth())
        now_ts_ms = int(time.time() * 1000)
        oldest_created_ts_ms = int(spool_stats.get("oldest_created_ts_ms") or 0)
        spool_oldest_age_ms = (
            max(0, int(now_ts_ms) - oldest_created_ts_ms)
            if oldest_created_ts_ms > 0
            else 0
        )
        with self._state_lock:
            metrics = dict(self._metrics)
            self._ensure_shard_metrics_locked()
            shard_metrics = [dict(item) for item in self._shard_metrics]
            thread_alive = any(thread.is_alive() for thread in self._threads)
            worker_alive_count = sum(1 for thread in self._threads if thread.is_alive())
        backpressure_active = bool(metrics.get("backpressure_active")) or self._queue_at_high_watermark(
            queue_depth,
            pending_bytes=int(spool_stats.get("pending_bytes") or 0),
        )
        shard_stats_by_id = {
            int(item.get("shard_id") or 0): dict(item)
            for item in shard_spool_stats
        }
        threads_by_shard = {
            shard_id: False
            for shard_id in range(self._worker_count())
        }
        for index, thread in enumerate(list(self._threads)):
            if index in threads_by_shard:
                threads_by_shard[index] = bool(thread.is_alive())
        shards: list[dict[str, Any]] = []
        for shard_id in range(self._worker_count()):
            spool = dict(shard_stats_by_id.get(shard_id) or {})
            metric = dict(shard_metrics[shard_id] if shard_id < len(shard_metrics) else {})
            shard_depth = int(spool.get("pending_batches") or 0)
            oldest_created_ts_ms = int(spool.get("oldest_created_ts_ms") or 0)
            newest_created_ts_ms = int(spool.get("newest_created_ts_ms") or 0)
            pending_lag_ms = (
                max(0, int(now_ts_ms) - oldest_created_ts_ms)
                if oldest_created_ts_ms > 0
                else 0
            )
            shards.append(
                {
                    "shard_id": int(shard_id),
                    "thread_alive": bool(threads_by_shard.get(shard_id)),
                    "batch_size": int(self._config.batch_size),
                    "queue_depth": shard_depth,
                    "queue_fill_ratio": float(shard_depth / queue_maxsize),
                    "spool_pending_batches": shard_depth,
                    "spool_pending_rows": int(spool.get("pending_rows") or 0),
                    "spool_pending_bytes": int(spool.get("pending_bytes") or 0),
                    "oldest_created_ts_ms": (oldest_created_ts_ms or None),
                    "newest_created_ts_ms": (newest_created_ts_ms or None),
                    "pending_lag_ms": int(pending_lag_ms),
                    "enqueued_batches": int(metric.get("enqueued_batches") or 0),
                    "enqueued_rows": int(metric.get("enqueued_rows") or 0),
                    "flushed_batches": int(metric.get("flushed_batches") or 0),
                    "flushed_rows": int(metric.get("flushed_rows") or 0),
                    "last_batch_envelopes": int(metric.get("last_batch_envelopes") or 0),
                    "last_batch_rows": int(metric.get("last_batch_rows") or 0),
                    "write_failures": int(metric.get("write_failures") or 0),
                    "retry_count": int(metric.get("retry_count") or 0),
                    "backpressure_events": int(metric.get("backpressure_events") or 0),
                    "last_backpressure_reason": str(metric.get("last_backpressure_reason") or ""),
                    "last_backpressure_ts_ms": (int(metric.get("last_backpressure_ts_ms") or 0) or None),
                    "dropped_batches": int(metric.get("dropped_batches") or 0),
                    "dropped_rows": int(metric.get("dropped_rows") or 0),
                    "inflight_batches": int(metric.get("inflight_batches") or 0),
                    "inflight_rows": int(metric.get("inflight_rows") or 0),
                    "last_enqueue_ts_ms": (int(metric.get("last_enqueue_ts_ms") or 0) or None),
                    "last_flush_ts_ms": (int(metric.get("last_flush_ts_ms") or 0) or None),
                    "last_flush_latency_ms": int(metric.get("last_flush_latency_ms") or 0),
                    "last_db_write_duration_ms": int(metric.get("last_db_write_duration_ms") or 0),
                    "last_error": str(metric.get("last_error") or ""),
                    "last_error_ts_ms": (int(metric.get("last_error_ts_ms") or 0) or None),
                }
            )
        return {
            "ok": (not self.enabled) or (thread_alive and bool(spool_stats.get("ok", True)) and not str(metrics.get("last_error") or "").strip() and not backpressure_active),
            "enabled": bool(self.enabled),
            "thread_alive": thread_alive,
            "worker_count": int(self._worker_count()),
            "worker_alive_count": int(worker_alive_count),
            "queue_depth": queue_depth,
            "queue_rows": int(spool_stats.get("pending_rows") or 0),
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
            "backpressure_recovered_events": int(metrics.get("backpressure_recovered_events") or 0),
            "last_backpressure_recovered_ts_ms": (int(metrics.get("last_backpressure_recovered_ts_ms") or 0) or None),
            "last_backpressure_recovered_reason": str(metrics.get("last_backpressure_recovered_reason") or ""),
            "dropped_batches": int(metrics.get("dropped_batches") or 0),
            "dropped_rows": int(metrics.get("dropped_rows") or 0),
            "rejected_batches": int(metrics.get("rejected_batches") or 0),
            "rejected_rows": int(metrics.get("rejected_rows") or 0),
            "spooled_batches": int(metrics.get("spooled_batches") or 0),
            "spooled_rows": int(metrics.get("spooled_rows") or 0),
            "residual_dropped_batches": int(metrics.get("residual_dropped_batches") or 0),
            "residual_dropped_rows": int(metrics.get("residual_dropped_rows") or 0),
            "residual_loss_rows": int(metrics.get("residual_dropped_rows") or 0),
            "residual_spooled_batches": int(metrics.get("residual_spooled_batches") or 0),
            "residual_spooled_rows": int(metrics.get("residual_spooled_rows") or 0),
            "replayed_batches": int(metrics.get("replayed_batches") or 0),
            "replayed_rows": int(metrics.get("replayed_rows") or 0),
            "shutdown_drained_batches": int(metrics.get("shutdown_drained_batches") or 0),
            "shutdown_drained_rows": int(metrics.get("shutdown_drained_rows") or 0),
            "startup_pending_batches": int(metrics.get("startup_pending_batches") or 0),
            "startup_pending_rows": int(metrics.get("startup_pending_rows") or 0),
            "startup_replay_high_watermark_id": int(metrics.get("startup_replay_high_watermark_id") or 0),
            "spool_path": str(spool_stats.get("path") or self._spool.path),
            "spool_synchronous": str(spool_stats.get("synchronous") or self._spool.synchronous),
            "spool_pending_batches": int(spool_stats.get("pending_batches") or 0),
            "spool_pending_rows": int(spool_stats.get("pending_rows") or 0),
            "spool_pending_bytes": int(spool_stats.get("pending_bytes") or 0),
            "legacy_unsharded_spool_pending_batches": int(legacy_spool_stats.get("pending_batches") or 0),
            "legacy_unsharded_spool_pending_rows": int(legacy_spool_stats.get("pending_rows") or 0),
            "spool_file_bytes": int(spool_stats.get("file_bytes") or 0),
            "spool_max_envelopes": int(spool_stats.get("max_envelopes") or self._config.queue_maxsize),
            "spool_max_bytes": int(spool_stats.get("max_bytes") or self._config.spool_max_bytes),
            "spool_bytes_fill_ratio": float(spool_stats.get("bytes_fill_ratio") or 0.0),
            "spool_oldest_created_ts_ms": spool_stats.get("oldest_created_ts_ms"),
            "spool_oldest_age_ms": int(spool_oldest_age_ms),
            "spool_newest_created_ts_ms": spool_stats.get("newest_created_ts_ms"),
            "spool_oldest_id": spool_stats.get("oldest_id"),
            "spool_newest_id": spool_stats.get("newest_id"),
            "spool_deleted_batches": int(metrics.get("spool_deleted_batches") or 0),
            "spool_deleted_rows": int(metrics.get("spool_deleted_rows") or 0),
            "spool_delete_failures": int(metrics.get("spool_delete_failures") or 0),
            "spool_enqueue_failures": int(metrics.get("spool_enqueue_failures") or 0),
            "spool_corrupt_rows": int(metrics.get("spool_corrupt_rows") or 0),
            "spool_corrupt_payload_rows": int(metrics.get("spool_corrupt_payload_rows") or 0),
            "spool_corruption_events": int(metrics.get("spool_corruption_events") or 0),
            "spool_last_quarantine_paths": list(spool_stats.get("last_quarantine_paths") or []),
            "spool_error": str(spool_stats.get("error") or ""),
            "last_flush_latency_ms": int(metrics.get("last_flush_latency_ms") or 0),
            "total_flush_latency_ms": int(metrics.get("total_flush_latency_ms") or 0),
            "last_db_write_duration_ms": int(metrics.get("last_db_write_duration_ms") or 0),
            "total_db_write_duration_ms": int(metrics.get("total_db_write_duration_ms") or 0),
            "write_failures": int(metrics.get("write_failures") or 0),
            "row_copy_avoided_rows": int(metrics.get("row_copy_avoided_rows") or 0),
            "row_copy_fallback_rows": int(metrics.get("row_copy_fallback_rows") or 0),
            "last_shutdown_drain_duration_ms": int(metrics.get("last_shutdown_drain_duration_ms") or 0),
            "last_shutdown_drain_deadline_ms": int(metrics.get("last_shutdown_drain_deadline_ms") or 0),
            "inflight_batches": int(metrics.get("inflight_batches") or 0),
            "inflight_rows": int(metrics.get("inflight_rows") or 0),
            "last_enqueue_ts_ms": (int(metrics.get("last_enqueue_ts_ms") or 0) or None),
            "last_flush_ts_ms": (int(metrics.get("last_flush_ts_ms") or 0) or None),
            "last_error": str(metrics.get("last_error") or ""),
            "last_error_ts_ms": (int(metrics.get("last_error_ts_ms") or 0) or None),
            "shards": shards,
            "dead_letter_path": str(self._config.dead_letter_path),
            "ts_ms": int(now_ts_ms),
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
    if writer is None:
        return {"ok": True, "enabled": False, "thread_alive": False, "queue_depth": 0, "detail": "async_writer_not_started", "ts_ms": int(time.time() * 1000)}
    snapshot = dict(writer.close(timeout_s=timeout_s) or {})
    snapshot["detail"] = "async_writer_stopped"
    if not bool(snapshot.get("thread_alive")):
        with _WRITER_LOCK:
            if _WRITER is writer:
                _WRITER = None
    else:
        snapshot["detail"] = "async_writer_shutdown_pending"
    return snapshot


def enqueue_price_persistence(
    *,
    prices: tuple[Mapping[str, Any], ...] = (),
    quotes: tuple[Mapping[str, Any], ...] = (),
    raw: tuple[Mapping[str, Any], ...] = (),
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
