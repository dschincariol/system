"""Small Redis circuit breaker used by the hot-path cache."""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable

LOG = logging.getLogger(__name__)


class CacheUnavailable(RuntimeError):
    """Raised when the Redis cache should be bypassed."""


@dataclass(frozen=True)
class CacheAlert:
    code: str
    message: str
    severity: str
    component: str
    ts_ms: int
    extra: dict[str, Any]


AlertHandler = Callable[[CacheAlert], None]

_alert_handler: AlertHandler | None = None


def _now_ms() -> int:
    return int(time.time() * 1000)


def set_alert_handler(handler: AlertHandler | None) -> None:
    """Install a process-local alert hook for tests or runtime integration."""

    global _alert_handler
    _alert_handler = handler


def emit_alert(
    code: str,
    message: str,
    *,
    severity: str = "warning",
    component: str = "engine.cache",
    extra: dict[str, Any] | None = None,
) -> CacheAlert:
    alert = CacheAlert(
        code=str(code),
        message=str(message),
        severity=str(severity),
        component=str(component),
        ts_ms=_now_ms(),
        extra=dict(extra or {}),
    )
    if _alert_handler is not None:
        _alert_handler(alert)
    else:
        level = logging.ERROR if severity.lower() in {"error", "critical"} else logging.WARNING
        LOG.log(level, "%s: %s extra=%s", alert.code, alert.message, alert.extra)
    return alert


class CircuitBreaker:
    """Consecutive-failure circuit breaker with cooldown probes."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"
    HALF_OPEN_PROBING = "half_open_probing"

    def __init__(
        self,
        *,
        failure_threshold: int = 3,
        cooldown_s: float = 3.0,
        name: str = "redis",
    ) -> None:
        self.failure_threshold = max(1, int(failure_threshold))
        self.cooldown_s = max(0.0, float(cooldown_s))
        self.name = str(name)
        self._state = self.CLOSED
        self._failures = 0
        self._opened_at = 0.0
        self._lock = threading.Lock()

    @property
    def state(self) -> str:
        with self._lock:
            return str(self._state)

    @property
    def failures(self) -> int:
        with self._lock:
            return int(self._failures)

    def reset(self) -> None:
        with self._lock:
            self._state = self.CLOSED
            self._failures = 0
            self._opened_at = 0.0

    def force_open(self, reason: str = "forced_open") -> None:
        with self._lock:
            already_open = self._state == self.OPEN
            self._state = self.OPEN
            self._failures = max(self.failure_threshold, self._failures)
            self._opened_at = time.monotonic()
        if not already_open:
            emit_alert(
                "CACHE_REDIS_UNAVAILABLE",
                "Redis cache circuit opened; reads are falling through to Postgres.",
                severity="warning",
                component="engine.cache.circuit",
                extra={"name": self.name, "reason": str(reason)},
            )

    def _before_call(self) -> None:
        with self._lock:
            if self._state == self.HALF_OPEN_PROBING:
                raise CacheUnavailable("redis_cache_circuit_half_open_probe_in_progress")
            if self._state != self.OPEN:
                return
            elapsed = time.monotonic() - float(self._opened_at)
            if elapsed >= self.cooldown_s:
                self._state = self.HALF_OPEN_PROBING
                return
        raise CacheUnavailable("redis_cache_circuit_open")

    def _record_success(self) -> None:
        emit_close = False
        with self._lock:
            if self._state in {self.OPEN, self.HALF_OPEN, self.HALF_OPEN_PROBING}:
                emit_close = True
            self._state = self.CLOSED
            self._failures = 0
            self._opened_at = 0.0
        if emit_close:
            emit_alert(
                "CACHE_REDIS_RECOVERED",
                "Redis cache circuit closed; reads are using Redis again.",
                severity="info",
                component="engine.cache.circuit",
                extra={"name": self.name},
            )

    def _record_failure(self, error: BaseException) -> None:
        emit_open = False
        with self._lock:
            self._failures += 1
            should_open = self._state in {
                self.HALF_OPEN,
                self.HALF_OPEN_PROBING,
            } or self._failures >= self.failure_threshold
            if should_open and self._state != self.OPEN:
                emit_open = True
                self._state = self.OPEN
                self._opened_at = time.monotonic()
        if emit_open:
            emit_alert(
                "CACHE_REDIS_UNAVAILABLE",
                "Redis cache circuit opened; reads are falling through to Postgres.",
                severity="warning",
                component="engine.cache.circuit",
                extra={"name": self.name, "error": f"{type(error).__name__}: {error}"},
            )

    def call(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        self._before_call()
        try:
            result = func(*args, **kwargs)
        except CacheUnavailable:
            raise
        except Exception as exc:
            self._record_failure(exc)
            raise CacheUnavailable(str(exc)) from exc
        self._record_success()
        return result


_GLOBAL_CIRCUIT: CircuitBreaker | None = None
_GLOBAL_LOCK = threading.Lock()


def cache_circuit() -> CircuitBreaker:
    global _GLOBAL_CIRCUIT
    with _GLOBAL_LOCK:
        if _GLOBAL_CIRCUIT is None:
            _GLOBAL_CIRCUIT = CircuitBreaker(
                failure_threshold=int(os.environ.get("TS_REDIS_CIRCUIT_FAILURES", "3")),
                cooldown_s=float(os.environ.get("TS_REDIS_CIRCUIT_COOLDOWN_S", "3")),
            )
        return _GLOBAL_CIRCUIT


def reset_global_circuit() -> None:
    global _GLOBAL_CIRCUIT
    with _GLOBAL_LOCK:
        _GLOBAL_CIRCUIT = None
