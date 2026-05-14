"""Process-wide Redis client factory for the hot-path cache."""

from __future__ import annotations

import os
import threading
from typing import Any

from engine.cache.circuit import cache_circuit
from engine.runtime.platform import default_redis_url

try:  # pragma: no cover - dependency availability is environment-specific.
    import redis
except Exception:  # pragma: no cover
    redis = None  # type: ignore[assignment]

_CLIENT: Any | None = None
_CLIENT_KEY: tuple[str, int] | None = None
_PINGED_KEY: tuple[str, int] | None = None
_LOCK = threading.Lock()


def redis_url() -> str:
    return str(os.environ.get("TS_REDIS_URL") or os.environ.get("REDIS_URL") or default_redis_url()).strip()


def redis_pool_size() -> int:
    return max(1, int(os.environ.get("TS_REDIS_POOL_SIZE", "16")))


def _redis_timeout_s(name: str, default: float) -> float:
    try:
        return max(0.05, float(os.environ.get(name, str(default))))
    except Exception:
        return float(default)


def reset_redis_pool() -> None:
    global _CLIENT, _CLIENT_KEY, _PINGED_KEY
    with _LOCK:
        old = _CLIENT
        _CLIENT = None
        _CLIENT_KEY = None
        _PINGED_KEY = None
    close = getattr(old, "close", None)
    if callable(close):
        close()


def redis_dependency_available() -> bool:
    return redis is not None


def redis_watch_error_type():
    if redis is None:
        return None
    return getattr(redis, "WatchError", None)


def redis_from_url(url: str, **kwargs: Any):
    if redis is None:
        cache_circuit().force_open("redis_dependency_unavailable")
        raise RuntimeError("redis_dependency_unavailable")
    return redis.Redis.from_url(str(url), **kwargs)


def redis_pool():
    """Return the singleton ``redis.Redis`` client.

    The URL scheme in ``TS_REDIS_URL`` selects Unix socket or TCP transport.
    Startup ping failures open the circuit but do not prevent the process from
    continuing in Postgres fall-through mode.
    """

    global _CLIENT, _CLIENT_KEY, _PINGED_KEY
    url = redis_url()
    pool_size = redis_pool_size()
    key = (url, int(pool_size))
    with _LOCK:
        if _CLIENT is None or _CLIENT_KEY != key:
            if redis is None:
                cache_circuit().force_open("redis_dependency_unavailable")
                raise RuntimeError("redis_dependency_unavailable")
            _CLIENT = redis_from_url(
                url,
                decode_responses=False,
                max_connections=int(pool_size),
                socket_connect_timeout=_redis_timeout_s("TS_REDIS_CONNECT_TIMEOUT_S", 0.25),
                socket_timeout=_redis_timeout_s("TS_REDIS_SOCKET_TIMEOUT_S", 0.25),
            )
            _CLIENT_KEY = key
            _PINGED_KEY = None
        client = _CLIENT
    if _PINGED_KEY != key:
        try:
            cache_circuit().call(client.ping)
            _PINGED_KEY = key
        except Exception:
            cache_circuit().force_open("redis_ping_failed")
    return client
