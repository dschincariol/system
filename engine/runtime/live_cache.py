"""Runtime live-cache boundary for price and feature snapshots.

The live market-data path still exposes price/feature cache helpers through
`engine.data.price_cache` and `engine.data.feature_store`, but ownership now
flows through this runtime interface so the backing store can move from local
memory to Redis without changing callers.
"""

from __future__ import annotations

import copy
import json
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Mapping

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

try:
    from engine.cache.redis_pool import (
        redis_dependency_available as _redis_dependency_available,
        redis_from_url as _redis_from_url,
        redis_watch_error_type as _redis_watch_error_type,
    )
except Exception as exc:  # pragma: no cover - optional dependency at runtime
    _redis_dependency_available = lambda: False  # type: ignore[assignment]
    _redis_from_url = None  # type: ignore[assignment]
    _redis_watch_error_type = lambda: None  # type: ignore[assignment]
    _REDIS_IMPORT_ERROR = f"{type(exc).__name__}:{exc}"
else:  # pragma: no cover - exercised only when redis is installed
    _REDIS_IMPORT_ERROR = "" if _redis_dependency_available() else "ModuleNotFoundError:redis"
_redis = object() if _redis_dependency_available() else None

LOG = get_logger("engine.runtime.live_cache")
_WARNED_NONFATAL_KEYS: set[str] = set()
_LIVE_CACHE_LOCK = threading.RLock()
_LIVE_CACHE: "_BaseLiveCache | None" = None
_LIVE_CACHE_CONFIG_KEY = ""


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.runtime.live_cache",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(str(once_key))


def _env_float(name: str, default: float) -> float:
    raw = str(os.environ.get(name, "")).strip()
    if raw == "":
        return float(default)
    try:
        return float(raw)
    except ValueError as exc:
        _warn_nonfatal(
            "LIVE_CACHE_ENV_FLOAT_PARSE_FAILED",
            exc,
            once_key=f"env_float:{name}:{raw}",
            env=name,
            value=raw,
            default=float(default),
        )
        return float(default)


def _normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


@dataclass(frozen=True)
class LiveCacheConfig:
    backend: str
    redis_url: str
    redis_key_prefix: str
    connect_timeout_s: float
    socket_timeout_s: float

    @classmethod
    def from_env(cls) -> "LiveCacheConfig":
        backend = str(os.environ.get("LIVE_CACHE_BACKEND", "auto") or "auto").strip().lower()
        if backend not in {"auto", "memory", "redis"}:
            backend = "auto"
        redis_url = str(
            os.environ.get("LIVE_CACHE_REDIS_URL")
            or os.environ.get("REDIS_URL")
            or os.environ.get("REDIS_CACHE_URL")
            or ""
        ).strip()
        return cls(
            backend=str(backend),
            redis_url=str(redis_url),
            redis_key_prefix=str(os.environ.get("LIVE_CACHE_REDIS_KEY_PREFIX", "trading-system:live-cache") or "trading-system:live-cache").strip()
            or "trading-system:live-cache",
            connect_timeout_s=max(0.1, _env_float("LIVE_CACHE_REDIS_CONNECT_TIMEOUT_S", 1.0)),
            socket_timeout_s=max(0.1, _env_float("LIVE_CACHE_REDIS_SOCKET_TIMEOUT_S", 1.0)),
        )


class _BaseLiveCache:
    requested_backend = "memory"
    resolved_backend = "memory"
    fallback_reason = ""

    # system-audit: ignore[stub] Base backend is an intentional safe no-op.
    def close(self) -> None:
        return None

    # system-audit: ignore[stub] Base backend is an intentional safe no-op.
    def clear_price(self, symbol: str | None = None) -> None:
        return None

    # system-audit: ignore[stub] Base backend exposes cache-miss semantics.
    def get_price_snapshot(self, symbol: str) -> dict[str, Any] | None:
        return None

    def set_price_snapshot(self, symbol: str, payload: Mapping[str, Any], *, ttl_s: float, snapshot_ts_ms: int) -> bool:
        return False

    # system-audit: ignore[stub] Base backend is an intentional safe no-op.
    def clear_feature(self, symbol: str | None = None) -> None:
        return None

    # system-audit: ignore[stub] Base backend exposes cache-miss semantics.
    def get_feature_snapshot(self, symbol: str) -> dict[str, Any] | None:
        return None

    def set_feature_snapshot(self, symbol: str, payload: Mapping[str, Any], *, ttl_s: float, snapshot_ts_ms: int) -> bool:
        return False

    def get_snapshot(self) -> dict[str, Any]:
        return {
            "ok": False,
            "backend": str(self.resolved_backend),
            "requested_backend": str(self.requested_backend),
            "resolved_backend": str(self.resolved_backend),
            "degraded": True,
            "fallback_reason": str(self.fallback_reason or "base_live_cache_no_backend"),
            "price_symbols": 0,
            "price_points": 0,
            "feature_symbols": 0,
            "price_write_count": 0,
            "feature_write_count": 0,
            "last_price_write_ts_ms": None,
            "last_feature_write_ts_ms": None,
            "last_price_snapshot_ts_ms": None,
            "last_feature_snapshot_ts_ms": None,
            "redis_configured": False,
            "redis_available": bool(_redis is not None and _redis_dependency_available()),
            "ts_ms": int(time.time() * 1000),
        }


class _MemoryCacheEntry:
    __slots__ = ("expires_at_monotonic", "snapshot_ts_ms", "payload")

    def __init__(self, *, expires_at_monotonic: float, snapshot_ts_ms: int, payload: Mapping[str, Any]) -> None:
        self.expires_at_monotonic = float(expires_at_monotonic)
        self.snapshot_ts_ms = int(snapshot_ts_ms)
        self.payload = copy.deepcopy(dict(payload or {}))


class MemoryLiveCache(_BaseLiveCache):
    resolved_backend = "memory"

    def __init__(self, *, requested_backend: str, fallback_reason: str = "") -> None:
        self.requested_backend = str(requested_backend or "memory")
        self.fallback_reason = str(fallback_reason or "")
        self._lock = threading.RLock()
        self._price: dict[str, _MemoryCacheEntry] = {}
        self._feature: dict[str, _MemoryCacheEntry] = {}
        self._metrics = {
            "price_write_count": 0,
            "feature_write_count": 0,
            "last_price_write_ts_ms": 0,
            "last_feature_write_ts_ms": 0,
            "last_price_snapshot_ts_ms": 0,
            "last_feature_snapshot_ts_ms": 0,
        }

    def _purge_expired_locked(self, bucket: dict[str, _MemoryCacheEntry]) -> None:
        now_monotonic = time.monotonic()
        expired = [key for key, entry in list(bucket.items()) if float(entry.expires_at_monotonic) <= float(now_monotonic)]
        for key in expired:
            bucket.pop(str(key), None)

    def _clear_locked(self, bucket: dict[str, _MemoryCacheEntry], symbol: str | None = None) -> None:
        self._purge_expired_locked(bucket)
        if symbol is None:
            bucket.clear()
            return
        bucket.pop(_normalize_symbol(symbol), None)

    def _get_locked(self, bucket: dict[str, _MemoryCacheEntry], symbol: str) -> dict[str, Any] | None:
        self._purge_expired_locked(bucket)
        entry = bucket.get(_normalize_symbol(symbol))
        if entry is None:
            return None
        return copy.deepcopy(dict(entry.payload or {}))

    def _set_locked(
        self,
        bucket: dict[str, _MemoryCacheEntry],
        symbol: str,
        payload: Mapping[str, Any],
        *,
        ttl_s: float,
        snapshot_ts_ms: int,
        metric_key: str,
        metric_ts_key: str,
    ) -> bool:
        self._purge_expired_locked(bucket)
        symbol_key = _normalize_symbol(symbol)
        existing = bucket.get(symbol_key)
        if existing is not None and int(existing.snapshot_ts_ms) > int(snapshot_ts_ms):
            return False
        bucket[symbol_key] = _MemoryCacheEntry(
            expires_at_monotonic=(time.monotonic() + max(0.05, float(ttl_s))),
            snapshot_ts_ms=int(snapshot_ts_ms),
            payload=payload,
        )
        self._metrics[metric_key] = int(self._metrics.get(metric_key) or 0) + 1
        self._metrics[metric_ts_key] = int(time.time() * 1000)
        if metric_key == "price_write_count":
            self._metrics["last_price_snapshot_ts_ms"] = int(snapshot_ts_ms)
        if metric_key == "feature_write_count":
            self._metrics["last_feature_snapshot_ts_ms"] = int(snapshot_ts_ms)
        return True

    def clear_price(self, symbol: str | None = None) -> None:
        with self._lock:
            self._clear_locked(self._price, symbol=symbol)

    def get_price_snapshot(self, symbol: str) -> dict[str, Any] | None:
        with self._lock:
            return self._get_locked(self._price, symbol)

    def set_price_snapshot(self, symbol: str, payload: Mapping[str, Any], *, ttl_s: float, snapshot_ts_ms: int) -> bool:
        with self._lock:
            return self._set_locked(
                self._price,
                symbol,
                payload,
                ttl_s=ttl_s,
                snapshot_ts_ms=int(snapshot_ts_ms),
                metric_key="price_write_count",
                metric_ts_key="last_price_write_ts_ms",
            )

    def clear_feature(self, symbol: str | None = None) -> None:
        with self._lock:
            self._clear_locked(self._feature, symbol=symbol)

    def get_feature_snapshot(self, symbol: str) -> dict[str, Any] | None:
        with self._lock:
            return self._get_locked(self._feature, symbol)

    def set_feature_snapshot(self, symbol: str, payload: Mapping[str, Any], *, ttl_s: float, snapshot_ts_ms: int) -> bool:
        with self._lock:
            return self._set_locked(
                self._feature,
                symbol,
                payload,
                ttl_s=ttl_s,
                snapshot_ts_ms=int(snapshot_ts_ms),
                metric_key="feature_write_count",
                metric_ts_key="last_feature_write_ts_ms",
            )

    def get_snapshot(self) -> dict[str, Any]:
        with self._lock:
            self._purge_expired_locked(self._price)
            self._purge_expired_locked(self._feature)
            price_symbols = int(len(self._price))
            feature_symbols = int(len(self._feature))
            price_points = int(
                sum(len(list((entry.payload or {}).get("points") or [])) for entry in list(self._price.values()))
            )
            metrics = dict(self._metrics)
        return {
            "ok": True,
            "backend": "memory",
            "requested_backend": str(self.requested_backend),
            "resolved_backend": "memory",
            "degraded": bool(self.fallback_reason),
            "fallback_reason": (str(self.fallback_reason) if self.fallback_reason else None),
            "price_symbols": int(price_symbols),
            "price_points": int(price_points),
            "feature_symbols": int(feature_symbols),
            "price_write_count": int(metrics.get("price_write_count") or 0),
            "feature_write_count": int(metrics.get("feature_write_count") or 0),
            "last_price_write_ts_ms": int(metrics.get("last_price_write_ts_ms") or 0) or None,
            "last_feature_write_ts_ms": int(metrics.get("last_feature_write_ts_ms") or 0) or None,
            "last_price_snapshot_ts_ms": int(metrics.get("last_price_snapshot_ts_ms") or 0) or None,
            "last_feature_snapshot_ts_ms": int(metrics.get("last_feature_snapshot_ts_ms") or 0) or None,
            "redis_configured": False,
            "redis_available": bool(_redis is not None and _redis_dependency_available()),
            "ts_ms": int(time.time() * 1000),
        }


class RedisLiveCache(_BaseLiveCache):
    resolved_backend = "redis"

    def __init__(self, config: LiveCacheConfig, *, requested_backend: str) -> None:
        if _redis is None or not _redis_dependency_available() or _redis_from_url is None:  # pragma: no cover - guarded by builder
            raise RuntimeError("redis_dependency_unavailable")
        self.requested_backend = str(requested_backend or "redis")
        self.fallback_reason = ""
        self._config = config
        self._last_error = ""
        self._last_error_ts_ms = 0
        self._client = _redis_from_url(
            str(config.redis_url),
            socket_connect_timeout=float(config.connect_timeout_s),
            socket_timeout=float(config.socket_timeout_s),
            decode_responses=True,
        )

    def close(self) -> None:
        try:
            close = getattr(self._client, "close", None)
            if callable(close):
                close()
        except Exception as exc:  # pragma: no cover - best effort shutdown
            self._record_error(exc, once_key="redis_live_cache_close_failed")

    def _record_error(self, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
        self._last_error = f"{type(error).__name__}:{error}"
        self._last_error_ts_ms = int(time.time() * 1000)
        _warn_nonfatal(
            "LIVE_CACHE_REDIS_OPERATION_FAILED",
            error,
            once_key=once_key,
            backend="redis",
            **extra,
        )

    def _key(self, kind: str, symbol: str) -> str:
        return f"{self._config.redis_key_prefix}:{str(kind)}:{_normalize_symbol(symbol)}"

    def _serialize(self, payload: Mapping[str, Any], snapshot_ts_ms: int) -> str:
        return json.dumps(
            {
                "snapshot_ts_ms": int(snapshot_ts_ms),
                "payload": dict(payload or {}),
            },
            separators=(",", ":"),
            sort_keys=True,
        )

    def _deserialize(self, raw: Any) -> tuple[int, dict[str, Any] | None]:
        if raw in (None, ""):
            return 0, None
        try:
            parsed = json.loads(str(raw))
        except Exception as exc:
            self._record_error(exc, once_key="redis_live_cache_deserialize_failed")
            return 0, None
        if not isinstance(parsed, dict):
            return 0, None
        payload = dict(parsed.get("payload") or {})
        snapshot_ts_ms = int(parsed.get("snapshot_ts_ms") or payload.get("ts_ms") or 0)
        return int(snapshot_ts_ms), payload

    def _set_if_newer(self, key: str, payload: Mapping[str, Any], *, ttl_s: float, snapshot_ts_ms: int) -> bool:
        try:
            pipe = self._client.pipeline()
            while True:
                try:
                    pipe.watch(key)
                    current_raw = pipe.get(key)
                    current_ts_ms, _current_payload = self._deserialize(current_raw)
                    if int(current_ts_ms) > int(snapshot_ts_ms):
                        pipe.unwatch()
                        return False
                    pipe.multi()
                    pipe.set(key, self._serialize(payload, int(snapshot_ts_ms)), ex=max(1, int(round(float(ttl_s)))))
                    pipe.execute()
                    return True
                except Exception as exc:  # pragma: no cover - requires redis watch errors
                    watch_error = _redis_watch_error_type()
                    if watch_error is not None and isinstance(exc, watch_error):
                        continue
                    raise
                finally:
                    try:
                        pipe.reset()
                    except Exception as exc:
                        _warn_nonfatal(
                            "LIVE_CACHE_REDIS_PIPELINE_RESET_FAILED",
                            exc,
                            once_key="live_cache_redis_pipeline_reset_failed",
                        )
        except Exception as exc:
            self._record_error(exc, once_key="redis_live_cache_set_failed", key=str(key))
            return False

    def _get(self, key: str) -> dict[str, Any] | None:
        try:
            _snapshot_ts_ms, payload = self._deserialize(self._client.get(key))
            return payload
        except Exception as exc:
            self._record_error(exc, once_key=f"redis_live_cache_get_failed:{key}", key=str(key))
            return None

    def _delete_kind(self, kind: str, symbol: str | None = None) -> None:
        try:
            if symbol is not None:
                self._client.delete(self._key(kind, symbol))
                return
            pattern = f"{self._config.redis_key_prefix}:{str(kind)}:*"
            for key in self._client.scan_iter(match=pattern, count=256):
                self._client.delete(key)
        except Exception as exc:
            self._record_error(exc, once_key=f"redis_live_cache_delete_failed:{kind}", kind=str(kind))

    def clear_price(self, symbol: str | None = None) -> None:
        self._delete_kind("price", symbol=symbol)

    def get_price_snapshot(self, symbol: str) -> dict[str, Any] | None:
        return self._get(self._key("price", symbol))

    def set_price_snapshot(self, symbol: str, payload: Mapping[str, Any], *, ttl_s: float, snapshot_ts_ms: int) -> bool:
        return self._set_if_newer(
            self._key("price", symbol),
            payload,
            ttl_s=ttl_s,
            snapshot_ts_ms=int(snapshot_ts_ms),
        )

    def clear_feature(self, symbol: str | None = None) -> None:
        self._delete_kind("feature", symbol=symbol)

    def get_feature_snapshot(self, symbol: str) -> dict[str, Any] | None:
        return self._get(self._key("feature", symbol))

    def set_feature_snapshot(self, symbol: str, payload: Mapping[str, Any], *, ttl_s: float, snapshot_ts_ms: int) -> bool:
        return self._set_if_newer(
            self._key("feature", symbol),
            payload,
            ttl_s=ttl_s,
            snapshot_ts_ms=int(snapshot_ts_ms),
        )

    def get_snapshot(self) -> dict[str, Any]:
        ok = False
        try:
            ok = bool(self._client.ping())
        except Exception as exc:
            self._record_error(exc, once_key="redis_live_cache_ping_failed")
        return {
            "ok": bool(ok),
            "backend": "redis",
            "requested_backend": str(self.requested_backend),
            "resolved_backend": "redis",
            "degraded": not bool(ok),
            "fallback_reason": None,
            "price_symbols": None,
            "price_points": None,
            "feature_symbols": None,
            "price_write_count": None,
            "feature_write_count": None,
            "last_price_write_ts_ms": None,
            "last_feature_write_ts_ms": None,
            "redis_configured": bool(self._config.redis_url),
            "redis_available": True,
            "redis_url_present": bool(self._config.redis_url),
            "last_error": (str(self._last_error) if self._last_error else None),
            "last_error_ts_ms": (int(self._last_error_ts_ms) if self._last_error_ts_ms > 0 else None),
            "ts_ms": int(time.time() * 1000),
        }


def _config_key(config: LiveCacheConfig) -> str:
    return "|".join(
        [
            str(config.backend),
            str(config.redis_url),
            str(config.redis_key_prefix),
            str(config.connect_timeout_s),
            str(config.socket_timeout_s),
        ]
    )


def _build_live_cache(config: LiveCacheConfig) -> _BaseLiveCache:
    if str(config.backend) == "memory":
        return MemoryLiveCache(requested_backend="memory")
    wants_redis = str(config.backend) == "redis"
    auto_mode = str(config.backend) == "auto"
    if _redis is None or not _redis_dependency_available():
        if wants_redis:
            return MemoryLiveCache(requested_backend="redis", fallback_reason="redis_dependency_unavailable")
        return MemoryLiveCache(requested_backend=("auto" if auto_mode else str(config.backend)))
    if not str(config.redis_url or "").strip():
        if wants_redis:
            return MemoryLiveCache(requested_backend="redis", fallback_reason="redis_url_missing")
        return MemoryLiveCache(requested_backend=("auto" if auto_mode else str(config.backend)))
    try:
        backend = RedisLiveCache(config, requested_backend=("auto" if auto_mode else str(config.backend)))
        snapshot = backend.get_snapshot()
        if bool(snapshot.get("ok")):
            return backend
        if wants_redis:
            return MemoryLiveCache(
                requested_backend="redis",
                fallback_reason=str(snapshot.get("last_error") or "redis_ping_failed"),
            )
    except Exception as exc:  # pragma: no cover - runtime defensive guard
        if wants_redis:
            return MemoryLiveCache(requested_backend="redis", fallback_reason=f"redis_init_failed:{type(exc).__name__}")
        _warn_nonfatal(
            "LIVE_CACHE_REDIS_INIT_FAILED",
            exc,
            once_key="live_cache_redis_init_failed",
        )
    return MemoryLiveCache(requested_backend=("auto" if auto_mode else str(config.backend)))


def get_live_cache() -> _BaseLiveCache:
    global _LIVE_CACHE, _LIVE_CACHE_CONFIG_KEY
    config = LiveCacheConfig.from_env()
    config_key = _config_key(config)
    with _LIVE_CACHE_LOCK:
        if _LIVE_CACHE is not None and str(_LIVE_CACHE_CONFIG_KEY) == str(config_key):
            return _LIVE_CACHE
        previous = _LIVE_CACHE
        _LIVE_CACHE = _build_live_cache(config)
        _LIVE_CACHE_CONFIG_KEY = str(config_key)
    if previous is not None:
        try:
            previous.close()
        except Exception as exc:
            _warn_nonfatal(
                "LIVE_CACHE_PREVIOUS_CLOSE_FAILED",
                exc,
                once_key="live_cache_previous_close_failed",
            )
    return _LIVE_CACHE


def get_live_cache_snapshot() -> dict[str, Any]:
    return dict(get_live_cache().get_snapshot() or {})


def close_live_cache() -> None:
    global _LIVE_CACHE, _LIVE_CACHE_CONFIG_KEY
    with _LIVE_CACHE_LOCK:
        backend = _LIVE_CACHE
        _LIVE_CACHE = None
        _LIVE_CACHE_CONFIG_KEY = ""
    if backend is not None:
        try:
            backend.close()
        except Exception as exc:
            _warn_nonfatal(
                "LIVE_CACHE_CLOSE_FAILED",
                exc,
                once_key="live_cache_close_failed",
            )


__all__ = [
    "LiveCacheConfig",
    "close_live_cache",
    "get_live_cache",
    "get_live_cache_snapshot",
]
