"""Non-blocking prediction tracking for Timescale-backed model observability."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import queue
import threading
import time
from datetime import datetime, timezone
from typing import Any

from engine.runtime.db_guard import resolve_db_path
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import (
    get_timescale_client,
    init_db,
    record_prediction_explanation,
    run_write_txn,
)

LOG = get_logger("engine.prediction_logger")
_WARNED_NONFATAL_KEYS: set[str] = set()
_PREVIOUS_TRACKING_SINK = globals().get("_TRACKING_SINK")


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="prediction_tracking_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.prediction_logger",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _current_db_path() -> str:
    try:
        return str(resolve_db_path())
    except Exception:
        return ""


def _is_missing_tracking_table_error(error: BaseException, *, table: str) -> bool:
    text = str(error or "").strip().lower()
    return "no such table" in text and str(table or "").strip().lower() in text


def _run_local_tracking_write(write_fn, *, table: str, operation: str) -> None:
    try:
        run_write_txn(write_fn, table=table, operation=operation)
        return
    except Exception as exc:
        if not _is_missing_tracking_table_error(exc, table=table):
            raise
    init_db()
    run_write_txn(write_fn, table=table, operation=operation)


def _safe_text(value: Any, *, field: str, allow_empty: bool = False) -> str:
    text = str(value or "").strip()
    if not text and not allow_empty:
        raise ValueError(f"missing_required_field:{field}")
    return text


def _safe_float(value: Any, *, field: str) -> float:
    if value is None or value == "":
        raise ValueError(f"missing_required_field:{field}")
    return float(value)


def _normalize_metadata(metadata: Any) -> dict[str, Any]:
    if metadata is None:
        return {}
    if isinstance(metadata, dict):
        return dict(metadata)
    raise TypeError("metadata must be a dict when provided")


def _safe_optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception:
        return None


def _normalize_timestamp_value(value: Any, *, field: str) -> int:
    if value is None or value == "":
        return _now_ms()
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return int(value.replace(tzinfo=timezone.utc).timestamp() * 1000)
        return int(value.astimezone(timezone.utc).timestamp() * 1000)
    if isinstance(value, (int, float)):
        ts = float(value)
        if abs(ts) < 10_000_000_000:
            ts *= 1000.0
        return int(ts)
    text = str(value).strip()
    if not text:
        return _now_ms()
    if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
        return _normalize_timestamp_value(int(text), field=field)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception as exc:
        raise ValueError(f"invalid_timestamp:{field}:{text}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return int(parsed.timestamp() * 1000)


def _normalize_top_features(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    out: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, dict):
            out.append(dict(item))
    return out


class _TrackingSink:
    def __init__(self, *, db_path: str | None = None) -> None:
        self._db_path = str(db_path if db_path is not None else _current_db_path())
        self._queue: queue.Queue[tuple[str, dict[str, Any]]] = queue.Queue(
            maxsize=max(128, int(os.environ.get("PREDICTION_TRACKING_QUEUE_MAXSIZE", "4096")))
        )
        self._flush_interval_s = max(0.01, float(os.environ.get("PREDICTION_TRACKING_FLUSH_INTERVAL_S", "0.25")))
        self._max_batch_size = max(1, int(os.environ.get("PREDICTION_TRACKING_BATCH_SIZE", "128")))
        self._enqueue_timeout_s = max(0.05, float(os.environ.get("PREDICTION_TRACKING_ENQUEUE_TIMEOUT_S", "1.0")))
        self._retry_attempts = max(1, int(os.environ.get("PREDICTION_TRACKING_RETRY_ATTEMPTS", "3")))
        self._retry_base_s = max(0.01, float(os.environ.get("PREDICTION_TRACKING_RETRY_BASE_S", "0.1")))
        self._started = False
        self._start_lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._drop_pending = threading.Event()
        self._state_lock = threading.Lock()
        self._submitted_count = 0
        self._completed_count = 0
        self._inflight_count = 0
        self._flush_condition = threading.Condition(self._state_lock)

    @property
    def db_path(self) -> str:
        return str(self._db_path or "")

    def submit(self, kind: str, payload: dict[str, Any]) -> bool:
        self._ensure_started()
        try:
            self._queue.put_nowait((str(kind), dict(payload)))
        except queue.Full as exc:
            _warn_nonfatal(
                "PREDICTION_TRACKING_QUEUE_FULL",
                exc,
                once_key=None,
                queue_maxsize=int(self._queue.maxsize),
                kind=str(kind),
            )
            return False
        with self._flush_condition:
            self._submitted_count += 1
            self._flush_condition.notify_all()
        return True

    def flush(self, timeout_s: float | None = None) -> bool:
        deadline = time.monotonic() + float(timeout_s if timeout_s is not None else 5.0)
        with self._flush_condition:
            while True:
                if (
                    self._completed_count >= self._submitted_count
                    and self._inflight_count <= 0
                    and self._queue.empty()
                ):
                    return True
                remaining = deadline - time.monotonic()
                if remaining <= 0.0:
                    return False
                self._flush_condition.wait(timeout=remaining)

    def close(self, timeout_s: float | None = None) -> bool:
        self._stop_event.set()
        thread = self._thread
        if thread is None:
            return True
        thread.join(timeout=float(timeout_s if timeout_s is not None else 2.0))
        return not thread.is_alive()

    def abort(self, timeout_s: float | None = None) -> bool:
        self._drop_pending.set()
        discarded = self._discard_enqueued_rows()
        if discarded > 0:
            with self._flush_condition:
                self._completed_count += int(discarded)
                self._flush_condition.notify_all()
        self._stop_event.set()
        thread = self._thread
        if thread is None:
            return True
        thread.join(timeout=float(timeout_s if timeout_s is not None else 1.0))
        return not thread.is_alive()

    def _ensure_started(self) -> None:
        if self._started:
            return
        with self._start_lock:
            if self._started:
                return
            self._thread = threading.Thread(target=self._run, name="prediction-tracking", daemon=True)
            self._thread.start()
            self._started = True

    def _run(self) -> None:
        pending: dict[str, list[dict[str, Any]]] = {
            "model_registry": [],
            "predictions": [],
            "prediction_explanations": [],
        }
        pending_since: dict[str, float] = {}
        while True:
            if self._drop_pending.is_set():
                discarded_pending = 0
                for kind in tuple(pending.keys()):
                    discarded_pending += len(pending.get(kind) or [])
                    pending[kind] = []
                    pending_since.pop(kind, None)
                discarded_queue = self._discard_enqueued_rows()
                if discarded_pending > 0 or discarded_queue > 0:
                    with self._flush_condition:
                        self._completed_count += int(discarded_pending + discarded_queue)
                        self._flush_condition.notify_all()
                if self._queue.empty() and self._inflight_count <= 0:
                    break
                time.sleep(min(0.05, float(self._flush_interval_s)))
                continue
            if self._stop_event.is_set() and self._queue.empty() and not any(pending.values()):
                break
            item = self._get_next_item(pending, pending_since)
            if item is not None:
                kind, payload = item
                if kind not in pending:
                    with self._flush_condition:
                        self._completed_count += 1
                        self._flush_condition.notify_all()
                    continue
                pending[kind].append(payload)
                pending_since.setdefault(kind, time.monotonic())
            due_kinds = self._due_kinds(pending, pending_since, stopping=self._stop_event.is_set())
            for kind in due_kinds:
                if self._drop_pending.is_set():
                    break
                rows = list(pending.get(kind) or [])
                if not rows:
                    pending_since.pop(kind, None)
                    continue
                self._mark_inflight(len(rows))
                try:
                    self._flush_rows(kind, rows)
                except Exception as exc:
                    _warn_nonfatal(
                        "PREDICTION_TRACKING_FLUSH_FAILED",
                        exc,
                        once_key=None,
                        kind=str(kind),
                        rows=int(len(rows)),
                        db_path=str(self.db_path or ""),
                    )
                finally:
                    self._mark_completed(len(rows))
                pending[kind] = []
                pending_since.pop(kind, None)

    def _get_next_item(
        self,
        pending: dict[str, list[dict[str, Any]]],
        pending_since: dict[str, float],
    ) -> tuple[str, dict[str, Any]] | None:
        timeout_s = self._next_wait_timeout(pending, pending_since)
        try:
            return self._queue.get(timeout=timeout_s)
        except queue.Empty:
            return None

    def _next_wait_timeout(
        self,
        pending: dict[str, list[dict[str, Any]]],
        pending_since: dict[str, float],
    ) -> float:
        if not any(pending.values()):
            return float(self._flush_interval_s)
        now = time.monotonic()
        remaining = [
            max(0.0, float(self._flush_interval_s) - max(0.0, now - pending_since.get(kind, now)))
            for kind, rows in pending.items()
            if rows
        ]
        if not remaining:
            return 0.0
        return float(min(remaining))

    def _due_kinds(
        self,
        pending: dict[str, list[dict[str, Any]]],
        pending_since: dict[str, float],
        *,
        stopping: bool,
    ) -> list[str]:
        due: list[str] = []
        now = time.monotonic()
        for kind in ("model_registry", "predictions", "prediction_explanations"):
            rows = pending.get(kind) or []
            if not rows:
                continue
            age_s = max(0.0, now - pending_since.get(kind, now))
            if stopping or len(rows) >= int(self._max_batch_size) or age_s >= float(self._flush_interval_s):
                due.append(kind)
        return due

    def _flush_rows(self, kind: str, rows: list[dict[str, Any]]) -> None:
        self._flush_local_rows(kind, rows)
        if kind == "prediction_explanations":
            return
        client = get_timescale_client()
        if client is None or not bool(getattr(client, "enabled", False)):
            return
        enqueue = getattr(client, "enqueue_model_registry" if kind == "model_registry" else "enqueue_predictions", None)
        if not callable(enqueue):
            raise RuntimeError(f"tracking_enqueue_unsupported:{kind}")
        last_error: BaseException | None = None
        for attempt in range(1, int(self._retry_attempts) + 1):
            try:
                enqueue(tuple(rows), timeout_s=float(self._enqueue_timeout_s))
                return
            except Exception as exc:
                last_error = exc
                if attempt >= int(self._retry_attempts):
                    break
                time.sleep(min(1.0, float(self._retry_base_s) * (2 ** (attempt - 1))))
        if last_error is not None:
            _warn_nonfatal(
                "PREDICTION_TRACKING_FLUSH_FAILED",
                last_error,
                once_key=None,
                kind=str(kind),
                rows=int(len(rows)),
            )

    def _flush_local_rows(self, kind: str, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        init_db()
        if kind == "model_registry":
            def _write_model_registry(con) -> None:
                for row in rows:
                    created_ts_ms = _normalize_timestamp_value(row.get("created_at"), field="created_at")
                    metadata_json = json.dumps(
                        _normalize_metadata(row.get("metadata")),
                        separators=(",", ":"),
                        sort_keys=True,
                        default=str,
                    )
                    con.execute(
                        """
                        INSERT INTO tracked_model_registry(
                          model_name, version, created_ts_ms, updated_ts_ms, metadata_json
                        )
                        VALUES(?,?,?,?,?)
                        ON CONFLICT(model_name, version) DO UPDATE SET
                          created_ts_ms=CASE
                            WHEN tracked_model_registry.created_ts_ms < excluded.created_ts_ms
                            THEN tracked_model_registry.created_ts_ms
                            ELSE excluded.created_ts_ms
                          END,
                          updated_ts_ms=excluded.updated_ts_ms,
                          metadata_json=excluded.metadata_json
                        """,
                        (
                            str(row["model_name"]),
                            str(row["version"]),
                            int(created_ts_ms),
                            int(_now_ms()),
                            str(metadata_json),
                        ),
                    )

            _run_local_tracking_write(
                _write_model_registry,
                table="tracked_model_registry",
                operation="prediction_tracking_model_registry",
            )
            return

        if kind == "predictions":
            def _write_predictions(con) -> None:
                for row in rows:
                    metadata_json = json.dumps(
                        _normalize_metadata(row.get("metadata")),
                        separators=(",", ":"),
                        sort_keys=True,
                        default=str,
                    )
                    con.execute(
                        """
                        INSERT INTO tracked_predictions(
                          ts_ms, symbol, model_name, model_version, prediction, confidence, features_version,
                          event_id, horizon_s, prediction_id, source_alert_id, model_id, tracking_source, metadata_json
                        )
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            int(_normalize_timestamp_value(row.get("time"), field="time")),
                            str(row["symbol"]),
                            str(row["model_name"]),
                            str(row["model_version"]),
                            float(row["prediction"]),
                            float(row["confidence"]),
                            str(row["features_version"]),
                            _safe_optional_int(row.get("event_id")),
                            _safe_optional_int(row.get("horizon_s")),
                            _safe_optional_int(row.get("prediction_id")),
                            _safe_optional_int(row.get("source_alert_id")),
                            (str(row["model_id"]) if row.get("model_id") not in (None, "") else None),
                            (str(row["tracking_source"]) if row.get("tracking_source") not in (None, "") else None),
                            str(metadata_json),
                        ),
                    )

            _run_local_tracking_write(
                _write_predictions,
                table="tracked_predictions",
                operation="prediction_tracking_predictions",
            )
            return

        if kind == "prediction_explanations":
            def _write_prediction_explanations(con) -> None:
                for row in rows:
                    record_prediction_explanation(
                        symbol=str(row["symbol"]),
                        ts=int(_normalize_timestamp_value(row.get("time"), field="time")),
                        model_family=str(row["model_family"]),
                        model_name=(str(row["model_name"]) if row.get("model_name") not in (None, "") else None),
                        version=(str(row["version"]) if row.get("version") not in (None, "") else None),
                        explanation_type=str(row["explanation_type"]),
                        top_features=_normalize_top_features(row.get("top_features")),
                        base_value=(float(row["base_value"]) if row.get("base_value") not in (None, "") else None),
                        diagnostics=_normalize_metadata(row.get("diagnostics")),
                        created_ts=int(_now_ms()),
                        con=con,
                    )

            _run_local_tracking_write(
                _write_prediction_explanations,
                table="prediction_explanations",
                operation="prediction_tracking_explanations",
            )
            return

        raise ValueError(f"unsupported_tracking_kind:{kind}")

    def _discard_enqueued_rows(self) -> int:
        discarded = 0
        while True:
            try:
                self._queue.get_nowait()
            except queue.Empty:
                return int(discarded)
            discarded += 1

    def _mark_inflight(self, row_count: int) -> None:
        with self._flush_condition:
            self._inflight_count += int(max(0, row_count))
            self._flush_condition.notify_all()

    def _mark_completed(self, row_count: int) -> None:
        with self._flush_condition:
            completed = int(max(0, row_count))
            self._inflight_count = max(0, self._inflight_count - completed)
            self._completed_count += completed
            self._flush_condition.notify_all()

class _TrackingSinkManager:
    def __init__(self, previous: Any = None) -> None:
        self._lock = threading.RLock()
        self._sink: _TrackingSink | None = None
        if previous is not None:
            self._shutdown_sink(previous, drain=False, timeout_s=0.5)

    def _shutdown_sink(self, sink: Any, *, drain: bool, timeout_s: float | None) -> bool:
        if sink is None:
            return True
        if not drain:
            abort = getattr(sink, "abort", None)
            if callable(abort):
                try:
                    return bool(abort(timeout_s=timeout_s))
                except Exception:
                    return False
        close = getattr(sink, "close", None)
        if callable(close):
            try:
                return bool(close(timeout_s=timeout_s))
            except Exception:
                return False
        return True

    def _ensure_sink(self) -> _TrackingSink:
        current_db_path = _current_db_path()
        old_sink: _TrackingSink | None = None
        with self._lock:
            sink = self._sink
            if sink is None:
                sink = _TrackingSink(db_path=current_db_path)
                self._sink = sink
                return sink
            if str(sink.db_path or "") == str(current_db_path or ""):
                return sink
            old_sink = sink
            sink = _TrackingSink(db_path=current_db_path)
            self._sink = sink
        if old_sink is not None:
            self._shutdown_sink(old_sink, drain=False, timeout_s=0.5)
        return sink

    def submit(self, kind: str, payload: dict[str, Any]) -> bool:
        return bool(self._ensure_sink().submit(kind, payload))

    def flush(self, timeout_s: float | None = None) -> bool:
        return bool(self._ensure_sink().flush(timeout_s=timeout_s))

    def close(self, timeout_s: float | None = None) -> bool:
        with self._lock:
            sink = self._sink
            self._sink = None
        return self._shutdown_sink(sink, drain=True, timeout_s=timeout_s)

    def abort(self, timeout_s: float | None = None) -> bool:
        with self._lock:
            sink = self._sink
            self._sink = None
        return self._shutdown_sink(sink, drain=False, timeout_s=timeout_s)


_TRACKING_SINK = _TrackingSinkManager(previous=_PREVIOUS_TRACKING_SINK)


def submit_model_registry_record(*, model_name: str, version: str, metadata: dict[str, Any], created_at: Any) -> bool:
    """Queue a model-registry tracking record for asynchronous persistence."""
    payload = {
        "model_name": _safe_text(model_name, field="model_name"),
        "version": _safe_text(version, field="version"),
        "created_at": _normalize_timestamp_value(created_at, field="created_at"),
        "metadata": _normalize_metadata(metadata),
    }
    return bool(_TRACKING_SINK.submit("model_registry", payload))


class PredictionLogger:
    """Queue prediction and explanation rows without blocking the caller."""

    async def log_prediction(
        self,
        model_name: Any,
        model_version: Any,
        symbol: Any,
        timestamp: Any,
        prediction: Any,
        confidence: Any,
        features_version: Any,
        *,
        event_id: Any = None,
        horizon_s: Any = None,
        prediction_id: Any = None,
        source_alert_id: Any = None,
        model_id: Any = None,
        tracking_source: Any = None,
        metadata: Any = None,
    ) -> None:
        """Async wrapper that queues one prediction-tracking payload."""
        self.log_prediction_nowait(
            model_name=model_name,
            model_version=model_version,
            symbol=symbol,
            timestamp=timestamp,
            prediction=prediction,
            confidence=confidence,
            features_version=features_version,
            event_id=event_id,
            horizon_s=horizon_s,
            prediction_id=prediction_id,
            source_alert_id=source_alert_id,
            model_id=model_id,
            tracking_source=tracking_source,
            metadata=metadata,
        )

    async def log_prediction_explanation(
        self,
        *,
        symbol: Any,
        timestamp: Any,
        model_family: Any,
        explanation_type: Any,
        model_name: Any = None,
        version: Any = None,
        top_features: Any = None,
        base_value: Any = None,
        diagnostics: Any = None,
    ) -> None:
        """Async wrapper that queues one explanation payload."""
        self.log_prediction_explanation_nowait(
            symbol=symbol,
            timestamp=timestamp,
            model_family=model_family,
            explanation_type=explanation_type,
            model_name=model_name,
            version=version,
            top_features=top_features,
            base_value=base_value,
            diagnostics=diagnostics,
        )

    def log_prediction_nowait(
        self,
        *,
        model_name: Any,
        model_version: Any,
        symbol: Any,
        timestamp: Any,
        prediction: Any,
        confidence: Any,
        features_version: Any,
        event_id: Any = None,
        horizon_s: Any = None,
        prediction_id: Any = None,
        source_alert_id: Any = None,
        model_id: Any = None,
        tracking_source: Any = None,
        metadata: Any = None,
    ) -> bool:
        """Queue one prediction row for background persistence."""
        features_version_text = _safe_text(features_version, field="features_version", allow_empty=True) or "unknown"
        payload = {
            "time": _normalize_timestamp_value(timestamp, field="timestamp"),
            "symbol": _safe_text(symbol, field="symbol"),
            "model_name": _safe_text(model_name, field="model_name"),
            "model_version": _safe_text(model_version, field="model_version"),
            "prediction": _safe_float(prediction, field="prediction"),
            "confidence": _safe_float(confidence, field="confidence"),
            "features_version": str(features_version_text),
            "event_id": _safe_optional_int(event_id),
            "horizon_s": _safe_optional_int(horizon_s),
            "prediction_id": _safe_optional_int(prediction_id),
            "source_alert_id": _safe_optional_int(source_alert_id),
            "model_id": (_safe_text(model_id, field="model_id", allow_empty=True) or None),
            "tracking_source": (_safe_text(tracking_source, field="tracking_source", allow_empty=True) or None),
            "metadata": _normalize_metadata(metadata),
        }
        return bool(_TRACKING_SINK.submit("predictions", payload))

    def log_prediction_explanation_nowait(
        self,
        *,
        symbol: Any,
        timestamp: Any,
        model_family: Any,
        explanation_type: Any,
        model_name: Any = None,
        version: Any = None,
        top_features: Any = None,
        base_value: Any = None,
        diagnostics: Any = None,
    ) -> bool:
        """Queue one prediction-explanation payload for background persistence."""
        payload = {
            "time": _normalize_timestamp_value(timestamp, field="timestamp"),
            "symbol": _safe_text(symbol, field="symbol"),
            "model_family": _safe_text(model_family, field="model_family"),
            "model_name": (_safe_text(model_name, field="model_name", allow_empty=True) or None),
            "version": (_safe_text(version, field="version", allow_empty=True) or None),
            "explanation_type": _safe_text(explanation_type, field="explanation_type"),
            "top_features": _normalize_top_features(top_features),
            "base_value": (_safe_float(base_value, field="base_value") if base_value not in (None, "") else None),
            "diagnostics": _normalize_metadata(diagnostics),
        }
        return bool(_TRACKING_SINK.submit("prediction_explanations", payload))

    def flush(self, timeout_s: float | None = None) -> bool:
        """Block until the background queue is flushed or the timeout expires."""
        return bool(_TRACKING_SINK.flush(timeout_s=timeout_s))

    def close(self, timeout_s: float | None = None) -> bool:
        """Flush and close the shared background tracking sink."""
        return bool(_TRACKING_SINK.close(timeout_s=timeout_s))


DEFAULT_PREDICTION_LOGGER = PredictionLogger()


def flush_prediction_tracking(timeout_s: float | None = None) -> bool:
    """Flush the shared prediction-tracking sink."""
    return bool(DEFAULT_PREDICTION_LOGGER.flush(timeout_s=timeout_s))


def shutdown_prediction_tracking(timeout_s: float | None = None) -> bool:
    """Close the shared prediction-tracking sink."""
    return bool(DEFAULT_PREDICTION_LOGGER.close(timeout_s=timeout_s))


__all__ = [
    "PredictionLogger",
    "DEFAULT_PREDICTION_LOGGER",
    "flush_prediction_tracking",
    "shutdown_prediction_tracking",
    "submit_model_registry_record",
]
