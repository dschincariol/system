"""Process-wide Redis client factory for the hot-path cache."""

from __future__ import annotations

import os
import threading
import time
from typing import Any
from urllib.parse import quote, urlparse, urlunparse

from engine.cache.circuit import cache_circuit
from engine.runtime.ingestion_tuning import tuned_float, tuned_int
from engine.runtime.platform import default_redis_url

try:  # pragma: no cover - dependency availability is environment-specific.
    import redis
except Exception:  # pragma: no cover
    redis = None  # type: ignore[assignment]

_CLIENT: Any | None = None
_CLIENT_KEY: tuple[str, int] | None = None
_LAST_HEALTH_CHECK_KEY: tuple[str, int] | None = None
_LAST_HEALTH_CHECK_MONOTONIC = 0.0
_LOCK = threading.Lock()


def _secret_text_from_env(*env_names: str) -> str:
    secret_name = ""
    for env_name in env_names:
        name = str(env_name or "").strip()
        file_env_names = [name] if name.endswith("_FILE") else []
        if name.endswith("_SECRET"):
            file_env_names.append(f"{name.removesuffix('_SECRET')}_FILE")
        for file_env_name in file_env_names:
            path = str(os.environ.get(file_env_name) or "").strip()
            if path:
                from engine.runtime.secret_sources import read_secret_text_file

                return read_secret_text_file(path)
        if name.endswith("_FILE"):
            continue
        secret_name = str(os.environ.get(name) or "").strip()
        if secret_name:
            break
    if not secret_name:
        return ""
    from services.secrets.loader import load_secret

    return load_secret(secret_name).decode("utf-8", "ignore").rstrip("\r\n")


def _url_with_password(url: str, password: str) -> str:
    text = str(url or "").strip()
    if not text or not password:
        return text
    parsed = urlparse(text)
    if parsed.password or not parsed.scheme or not parsed.hostname:
        return text
    user = quote(str(parsed.username or ""), safe="")
    host = str(parsed.hostname or "")
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if parsed.port:
        host = f"{host}:{parsed.port}"
    auth = f"{user}:{quote(password, safe='')}@" if user else f":{quote(password, safe='')}@"
    return urlunparse((parsed.scheme, auth + host, parsed.path, parsed.params, parsed.query, parsed.fragment))


def redis_url() -> str:
    raw = str(os.environ.get("TS_REDIS_URL") or os.environ.get("REDIS_URL") or default_redis_url()).strip()
    password = _secret_text_from_env("TS_REDIS_PASSWORD_SECRET", "REDIS_PASSWORD_SECRET")
    return _url_with_password(raw, password)


def redis_pool_size() -> int:
    return tuned_int("TS_REDIS_POOL_SIZE", 16, 1, 64)


def redis_pool_healthcheck_interval_s() -> float:
    return tuned_float("TS_REDIS_POOL_HEALTHCHECK_INTERVAL_S", 30.0, 0.1, 300.0)


def _redis_timeout_s(name: str, default: float) -> float:
    return tuned_float(name, float(default), 0.05, 5.0)


def reset_redis_pool() -> None:
    global _CLIENT, _CLIENT_KEY, _LAST_HEALTH_CHECK_KEY, _LAST_HEALTH_CHECK_MONOTONIC
    with _LOCK:
        old = _CLIENT
        _CLIENT = None
        _CLIENT_KEY = None
        _LAST_HEALTH_CHECK_KEY = None
        _LAST_HEALTH_CHECK_MONOTONIC = 0.0
    close = getattr(old, "close", None)
    if callable(close):
        close()


def redis_dependency_available() -> bool:
    return redis is not None


def redis_watch_error_type():
    if redis is None:
        return None
    return getattr(redis, "WatchError", None)


def redis_no_script_error_type():
    if redis is None:
        return None
    exceptions = getattr(redis, "exceptions", None)
    return getattr(exceptions, "NoScriptError", None)


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

    global _CLIENT, _CLIENT_KEY, _LAST_HEALTH_CHECK_KEY, _LAST_HEALTH_CHECK_MONOTONIC
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
            _LAST_HEALTH_CHECK_KEY = None
            _LAST_HEALTH_CHECK_MONOTONIC = 0.0
        client = _CLIENT
        now = time.monotonic()
        should_ping = (
            _LAST_HEALTH_CHECK_KEY != key
            or (float(now) - float(_LAST_HEALTH_CHECK_MONOTONIC)) >= redis_pool_healthcheck_interval_s()
        )
        if should_ping:
            _LAST_HEALTH_CHECK_KEY = key
            _LAST_HEALTH_CHECK_MONOTONIC = float(now)
    if should_ping:
        try:
            cache_circuit().call(client.ping)
        except Exception:
            cache_circuit().force_open("redis_ping_failed")
    return client
