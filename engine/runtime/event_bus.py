"""In-process pub/sub bus for decoupled runtime event fanout."""

import concurrent.futures
import os
import threading
import time
from collections import defaultdict, deque
from typing import Any, Callable, Dict, List

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

LOG = get_logger("engine.runtime.event_bus")


def _env_int(name: str, default: int) -> int:
    raw = str(os.environ.get(name, "")).strip()
    if raw == "":
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _env_csv(name: str, default: str) -> tuple[str, ...]:
    raw = str(os.environ.get(name, default) or default)
    values = [
        str(part or "").strip().lower()
        for part in raw.split(",")
        if str(part or "").strip()
    ]
    return tuple(values)


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="event_bus_nonfatal",
        code=code,
        message=code,
        error=error,
        level=30,
        component="engine.runtime.event_bus",
        extra=extra or None,
        persist=False,
    )


class EventBus:
    """Dispatch typed runtime events to subscribed handlers on a worker pool."""

    def __init__(self, max_queue_size: int = 10000, handler_workers: int = 8):
        self._subs: Dict[str, List[Callable[[Dict[str, Any]], None]]] = defaultdict(list)
        self._lock = threading.RLock()
        self._cv = threading.Condition(self._lock)
        self._critical_queue: deque[Dict[str, Any]] = deque()
        self._normal_queue: deque[Dict[str, Any]] = deque()
        self._max_queue_size = max(32, int(max_queue_size))
        self._critical_queue_max_size = max(
            8,
            min(
                int(self._max_queue_size),
                _env_int(
                    "EVENT_BUS_CRITICAL_QUEUE_MAX_SIZE",
                    max(16, int(self._max_queue_size // 4)),
                ),
            ),
        )
        self._handler_workers = max(1, int(handler_workers))
        self._drop_log_every = max(1, _env_int("EVENT_BUS_DROP_LOG_EVERY", 100))
        self._critical_prefixes = _env_csv(
            "EVENT_BUS_CRITICAL_PREFIXES",
            "execution,risk,position",
        )
        self._executor: concurrent.futures.ThreadPoolExecutor | None = None
        self._worker: threading.Thread | None = None
        self._stop = threading.Event()
        self._started = False
        self._metrics: Dict[str, Any] = {
            "published_count": 0,
            "dropped_count": 0,
            "normal_dropped_count": 0,
            "dispatched_count": 0,
            "handler_calls": 0,
            "handler_failures": 0,
            "critical_handler_failures": 0,
            "critical_inline_dispatch_count": 0,
            "critical_backpressure_count": 0,
            "critical_backpressure_active": 0,
            "queue_high_watermark": 0,
            "last_publish_ts_ms": 0,
            "last_dispatch_ts_ms": 0,
            "last_drop_ts_ms": 0,
            "last_normal_overflow_ts_ms": 0,
            "last_dropped_event_type": "",
            "last_failed_event_type": "",
            "last_critical_inline_dispatch_ts_ms": 0,
            "last_critical_backpressure_ts_ms": 0,
            "normal_overflow_active": 0,
            "avg_dispatch_lag_ms": 0.0,
            "max_dispatch_lag_ms": 0,
        }

    def _is_critical_event(self, event_type: str) -> bool:
        event_key = str(event_type or "").strip().lower()
        if not event_key:
            return False
        return any(event_key.startswith(prefix) for prefix in self._critical_prefixes)

    def _queue_size_locked(self) -> int:
        return int(len(self._critical_queue) + len(self._normal_queue))

    def _dispatch_event(self, event: Dict[str, Any], *, inline: bool = False) -> None:
        handlers: List[Callable[[Dict[str, Any]], None]] = []
        with self._cv:
            handlers = list(self._subs.get(str(event.get("type") or ""), []) or [])

        dispatch_ts_ms = int(time.time() * 1000)
        published_ts_ms = int(event.get("_published_ts_ms") or event.get("ts_ms") or dispatch_ts_ms)
        dispatch_lag_ms = max(0, int(dispatch_ts_ms) - int(published_ts_ms))
        with self._lock:
            prior_count = int(self._metrics.get("dispatched_count") or 0)
            self._metrics["dispatched_count"] = prior_count + 1
            self._metrics["last_dispatch_ts_ms"] = int(dispatch_ts_ms)
            prev_avg = float(self._metrics.get("avg_dispatch_lag_ms") or 0.0)
            self._metrics["avg_dispatch_lag_ms"] = (
                float(dispatch_lag_ms)
                if prior_count <= 0
                else ((prev_avg * float(prior_count)) + float(dispatch_lag_ms)) / float(prior_count + 1)
            )
            self._metrics["max_dispatch_lag_ms"] = max(
                int(self._metrics.get("max_dispatch_lag_ms") or 0),
                int(dispatch_lag_ms),
            )

        if not handlers:
            return

        if inline:
            for handler in handlers:
                self._invoke_handler(handler, dict(event))
            return

        executor = self._executor
        if executor is None:
            return
        for handler in handlers:
            try:
                executor.submit(self._invoke_handler, handler, dict(event))
            except Exception as exc:
                _warn_nonfatal("EVENT_BUS_HANDLER_SUBMIT_FAILED", exc, event_type=str(event.get("type") or ""))

    def subscribe(self, event_type: str, handler: Callable[[Dict[str, Any]], None]) -> None:
        """Register a handler for one event type."""
        if not callable(handler):
            raise RuntimeError(f"event_handler_not_callable:{event_type}:{handler}")
        with self._cv:
            self._subs[str(event_type)].append(handler)

    def unsubscribe(self, event_type: str, handler: Callable[[Dict[str, Any]], None]) -> None:
        """Remove a previously registered handler for one event type."""
        with self._cv:
            handlers = self._subs.get(str(event_type)) or []
            if handler in handlers:
                handlers.remove(handler)

    def publish(self, event: Dict[str, Any]) -> None:
        """Queue one event for asynchronous dispatch."""
        if not isinstance(event, dict):
            raise RuntimeError(f"event_must_be_dict:{type(event).__name__}")
        event_type = str(event.get("type") or "").strip()
        if not event_type:
            raise RuntimeError(f"event_missing_type:{event}")

        published_ts_ms = int(time.time() * 1000)
        envelope = dict(event)
        envelope.setdefault("ts_ms", published_ts_ms)
        envelope["_published_ts_ms"] = int(published_ts_ms)
        envelope["_critical"] = bool(self._is_critical_event(event_type))
        drop_meta = None
        inline_event = None

        with self._cv:
            self._metrics["published_count"] = int(self._metrics.get("published_count") or 0) + 1
            self._metrics["last_publish_ts_ms"] = int(published_ts_ms)
            if bool(envelope.get("_critical")):
                if len(self._critical_queue) >= self._critical_queue_max_size:
                    inline_event = dict(envelope)
                    self._metrics["critical_backpressure_active"] = 1
                    self._metrics["critical_inline_dispatch_count"] = int(
                        self._metrics.get("critical_inline_dispatch_count") or 0
                    ) + 1
                    self._metrics["critical_backpressure_count"] = int(
                        self._metrics.get("critical_backpressure_count") or 0
                    ) + 1
                    self._metrics["last_critical_inline_dispatch_ts_ms"] = int(published_ts_ms)
                    self._metrics["last_critical_backpressure_ts_ms"] = int(published_ts_ms)
                else:
                    self._critical_queue.append(envelope)
                    self._cv.notify()
            else:
                if len(self._normal_queue) >= self._max_queue_size:
                    dropped = self._normal_queue.popleft()
                    dropped_count = int(self._metrics.get("dropped_count") or 0) + 1
                    dropped_event_type = str((dropped or {}).get("type") or "")
                    self._metrics["dropped_count"] = int(dropped_count)
                    self._metrics["normal_dropped_count"] = int(
                        self._metrics.get("normal_dropped_count") or 0
                    ) + 1
                    self._metrics["normal_overflow_active"] = 1
                    self._metrics["last_drop_ts_ms"] = int(published_ts_ms)
                    self._metrics["last_normal_overflow_ts_ms"] = int(published_ts_ms)
                    self._metrics["last_dropped_event_type"] = str(dropped_event_type)
                    if dropped_count == 1 or (dropped_count % int(self._drop_log_every)) == 0:
                        drop_meta = {
                            "dropped_count": int(dropped_count),
                            "dropped_event_type": str(dropped_event_type),
                        }
                self._normal_queue.append(envelope)
                self._cv.notify()
            self._metrics["queue_high_watermark"] = max(
                int(self._metrics.get("queue_high_watermark") or 0),
                int(self._queue_size_locked()),
            )
        if drop_meta:
            _warn_nonfatal(
                "EVENT_BUS_QUEUE_OVERFLOW",
                RuntimeError("event_bus_queue_overflow"),
                queue_max_size=int(self._max_queue_size),
                incoming_event_type=str(event_type),
                **drop_meta,
            )
        if inline_event is not None:
            self._dispatch_event(dict(inline_event), inline=True)

    def start(self) -> None:
        with self._cv:
            if self._started:
                return
            self._started = True
            self._stop.clear()
            self._executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=int(self._handler_workers),
                thread_name_prefix="event_bus_handler",
            )
            self._worker = threading.Thread(target=self._run, name="event_bus_dispatcher", daemon=True)
            self._worker.start()

    def stop(self) -> None:
        worker = None
        executor = None
        with self._cv:
            self._stop.set()
            worker = self._worker
            executor = self._executor
            self._cv.notify_all()
        if worker is not None:
            try:
                worker.join(timeout=5.0)
            except Exception as exc:
                _warn_nonfatal("EVENT_BUS_WORKER_JOIN_FAILED", exc)
        if executor is not None:
            try:
                executor.shutdown(wait=False, cancel_futures=True)
            except Exception as exc:
                _warn_nonfatal("EVENT_BUS_EXECUTOR_SHUTDOWN_FAILED", exc)
        with self._cv:
            self._worker = None
            self._executor = None
            self._started = False

    def _run(self) -> None:
        while True:
            with self._cv:
                while not self._critical_queue and not self._normal_queue and not self._stop.is_set():
                    self._cv.wait(timeout=0.25)
                if self._stop.is_set() and not self._critical_queue and not self._normal_queue:
                    return
                if self._critical_queue:
                    event = self._critical_queue.popleft()
                else:
                    event = self._normal_queue.popleft()
                if len(self._critical_queue) < self._critical_queue_max_size:
                    self._metrics["critical_backpressure_active"] = 0
                if len(self._normal_queue) < self._max_queue_size:
                    self._metrics["normal_overflow_active"] = 0
            self._dispatch_event(dict(event), inline=False)

    def _invoke_handler(self, handler: Callable[[Dict[str, Any]], None], event: Dict[str, Any]) -> None:
        try:
            handler(event)
            with self._lock:
                self._metrics["handler_calls"] = int(self._metrics.get("handler_calls") or 0) + 1
        except Exception as exc:
            with self._lock:
                self._metrics["handler_failures"] = int(self._metrics.get("handler_failures") or 0) + 1
                if bool(event.get("_critical")):
                    self._metrics["critical_handler_failures"] = int(
                        self._metrics.get("critical_handler_failures") or 0
                    ) + 1
                self._metrics["last_failed_event_type"] = str(event.get("type") or "")
            _warn_nonfatal(
                "EVENT_BUS_HANDLER_FAILED",
                exc,
                event_type=str(event.get("type") or ""),
                handler=getattr(handler, "__name__", type(handler).__name__),
            )

    def get_stats(self) -> Dict[str, Any]:
        with self._cv:
            subscribers = {k: len(v) for k, v in self._subs.items()}
            critical_queue_size = int(len(self._critical_queue))
            normal_queue_size = int(len(self._normal_queue))
            queue_size = int(critical_queue_size + normal_queue_size)
            started = bool(self._started)
        with self._lock:
            metrics = dict(self._metrics)
        degraded_reasons: list[str] = []
        if bool(metrics.get("normal_overflow_active")):
            degraded_reasons.append("normal_queue_overflow")
        if bool(metrics.get("critical_backpressure_active")):
            degraded_reasons.append("critical_queue_backpressure")
        return {
            "degraded": bool(degraded_reasons),
            "degraded_reasons": degraded_reasons,
            "started": bool(started),
            "queue_size": int(queue_size),
            "queue_max_size": int(self._max_queue_size),
            "critical_queue_size": int(critical_queue_size),
            "critical_queue_max_size": int(self._critical_queue_max_size),
            "normal_queue_size": int(normal_queue_size),
            "handler_workers": int(self._handler_workers),
            "critical_prefixes": list(self._critical_prefixes),
            "subscribers": subscribers,
            "published_count": int(metrics.get("published_count") or 0),
            "dropped_count": int(metrics.get("dropped_count") or 0),
            "normal_dropped_count": int(metrics.get("normal_dropped_count") or 0),
            "dispatched_count": int(metrics.get("dispatched_count") or 0),
            "handler_calls": int(metrics.get("handler_calls") or 0),
            "handler_failures": int(metrics.get("handler_failures") or 0),
            "critical_handler_failures": int(metrics.get("critical_handler_failures") or 0),
            "critical_inline_dispatch_count": int(metrics.get("critical_inline_dispatch_count") or 0),
            "critical_backpressure_count": int(metrics.get("critical_backpressure_count") or 0),
            "critical_backpressure_active": bool(metrics.get("critical_backpressure_active") or 0),
            "queue_high_watermark": int(metrics.get("queue_high_watermark") or 0),
            "last_publish_ts_ms": (int(metrics.get("last_publish_ts_ms") or 0) or None),
            "last_dispatch_ts_ms": (int(metrics.get("last_dispatch_ts_ms") or 0) or None),
            "last_drop_ts_ms": (int(metrics.get("last_drop_ts_ms") or 0) or None),
            "last_normal_overflow_ts_ms": (int(metrics.get("last_normal_overflow_ts_ms") or 0) or None),
            "last_dropped_event_type": str(metrics.get("last_dropped_event_type") or ""),
            "last_failed_event_type": str(metrics.get("last_failed_event_type") or ""),
            "last_critical_inline_dispatch_ts_ms": (
                int(metrics.get("last_critical_inline_dispatch_ts_ms") or 0) or None
            ),
            "last_critical_backpressure_ts_ms": (
                int(metrics.get("last_critical_backpressure_ts_ms") or 0) or None
            ),
            "normal_overflow_active": bool(metrics.get("normal_overflow_active") or 0),
            "drop_log_every": int(self._drop_log_every),
            "avg_dispatch_lag_ms": float(metrics.get("avg_dispatch_lag_ms") or 0.0),
            "max_dispatch_lag_ms": int(metrics.get("max_dispatch_lag_ms") or 0),
        }


_GLOBAL_EVENT_BUS: EventBus | None = None
_GLOBAL_LOCK = threading.Lock()


def get_event_bus() -> EventBus:
    """Return the process-wide runtime event bus singleton."""
    global _GLOBAL_EVENT_BUS
    if _GLOBAL_EVENT_BUS is None:
        with _GLOBAL_LOCK:
            if _GLOBAL_EVENT_BUS is None:
                _GLOBAL_EVENT_BUS = EventBus(
                    max_queue_size=max(128, _env_int("EVENT_BUS_MAX_QUEUE_SIZE", 10000)),
                    handler_workers=max(1, _env_int("EVENT_BUS_HANDLER_WORKERS", 8)),
                )
                _GLOBAL_EVENT_BUS.start()
    return _GLOBAL_EVENT_BUS


def publish_event(event_type: str, payload: Dict[str, Any]) -> None:
    """Publish one typed event through the process-wide event bus."""
    bus = get_event_bus()
    evt = {
        "type": str(event_type),
        "payload": dict(payload or {}),
        "ts_ms": int(time.time() * 1000),
    }
    bus.publish(evt)


def subscribe_event(event_type: str, handler: Callable[[Dict[str, Any]], None]) -> None:
    """Register a handler on the process-wide event bus."""
    bus = get_event_bus()
    bus.subscribe(str(event_type), handler)


def unsubscribe_event(event_type: str, handler: Callable[[Dict[str, Any]], None]) -> None:
    """Remove a handler from the process-wide event bus."""
    bus = _GLOBAL_EVENT_BUS
    if bus is None:
        return
    bus.unsubscribe(str(event_type), handler)


def shutdown_event_bus() -> Dict[str, Any]:
    """Stop the process-wide event bus and return its final snapshot."""
    global _GLOBAL_EVENT_BUS
    with _GLOBAL_LOCK:
        bus = _GLOBAL_EVENT_BUS
        _GLOBAL_EVENT_BUS = None
    if bus is None:
        return {
            "ok": True,
            "started": False,
            "queue_size": 0,
            "detail": "event_bus_not_started",
        }
    before = dict(bus.get_stats() or {})
    bus.stop()
    after = dict(bus.get_stats() or {})
    return {
        "ok": True,
        "started": bool(after.get("started")),
        "detail": "event_bus_stopped",
        "before": before,
        "after": after,
    }
