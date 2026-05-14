"""Write-through Redis cache API."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Callable

from engine.cache import codec
from engine.cache.circuit import cache_circuit
from engine.cache.redis_pool import redis_pool
from engine.runtime import storage
from engine.runtime.metrics import emit_counter

LOG = logging.getLogger(__name__)

CacheBytes = bytes | bytearray | memoryview | str
CacheValue = CacheBytes | Callable[[], CacheBytes | None] | None
CacheEntries = Mapping[str, CacheValue] | Callable[[], Mapping[str, CacheValue]]


def _as_bytes(value: CacheBytes) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    return str(value).encode("utf-8")


def _redis_get(key: str):
    client = redis_pool()
    return client.get(str(key))


def _redis_set(key: str, value: bytes, ttl_s: int | None = None):
    client = redis_pool()
    if ttl_s is None:
        return client.set(str(key), value)
    return client.set(str(key), value, ex=max(1, int(ttl_s)))


def _redis_delete(key: str):
    client = redis_pool()
    return client.delete(str(key))


def _payload_version(value: object) -> int | None:
    raw = value
    if isinstance(raw, tuple) and raw:
        raw = raw[0]
    if raw is None:
        return None
    try:
        return codec.envelope_version(_as_bytes(raw))  # type: ignore[arg-type]
    except Exception:
        return None


def _redis_payload_version(key: str) -> int | None:
    try:
        return _payload_version(_redis_get(str(key)))
    except Exception:
        return None


def _version_tag(version: int | None) -> str:
    return "missing" if version is None else str(int(version))


def _record_write_through_lag(
    key: str,
    *,
    postgres_version: int | None,
    redis_pre_write_version: int | None,
    redis_post_write_version: int | None,
) -> None:
    if postgres_version is None and redis_pre_write_version is None and redis_post_write_version is None:
        return
    emit_counter(
        "cache_write_through_lag_observed",
        1,
        component="engine.cache.store",
        extra_tags={
            "key": str(key),
            "postgres_version": _version_tag(postgres_version),
            "redis_pre_write_version": _version_tag(redis_pre_write_version),
            "redis_post_write_version": _version_tag(redis_post_write_version),
        },
    )


def read(
    key: str,
    loader: Callable[[], CacheBytes | None] | None = None,
    *,
    ttl_s: int | None = 300,
) -> bytes | None:
    """Read bytes from Redis, loading and populating on misses."""

    raw = None
    try:
        raw = cache_circuit().call(_redis_get, str(key))
    except Exception:
        raw = None
    if raw is not None:
        return _as_bytes(raw)
    if loader is None:
        return None
    loaded = loader()
    if loaded is None:
        return None
    payload = _as_bytes(loaded)
    try:
        cache_circuit().call(_redis_set, str(key), payload, ttl_s)
    except Exception as exc:
        LOG.warning(
            "CACHE_REDIS_POPULATE_FAILED: serving Postgres-loaded value without Redis key=%s error=%s",
            key,
            exc,
        )
    return payload


def write_through(
    key: str,
    value: CacheValue,
    *,
    persist: Callable[[object], None],
    ttl_s: int | None = 300,
) -> None:
    """Persist to Postgres, then update Redis after the transaction commits."""

    with storage.transaction() as tx:
        persist(tx)

    try:
        cache_value = value() if callable(value) else value
    except Exception as exc:
        LOG.warning(
            "CACHE_REDIS_WRITE_THROUGH_BUILD_FAILED: committed Postgres write but skipped cache update key=%s error=%s",
            key,
            exc,
        )
        return
    if cache_value is None:
        invalidate(str(key))
        return

    payload = _as_bytes(cache_value)
    postgres_version = _payload_version(payload)
    redis_pre_write_version = _redis_payload_version(str(key))
    try:
        cache_circuit().call(_redis_set, str(key), payload, ttl_s)
        _record_write_through_lag(
            str(key),
            postgres_version=postgres_version,
            redis_pre_write_version=redis_pre_write_version,
            redis_post_write_version=_redis_payload_version(str(key)),
        )
    except Exception as exc:
        LOG.warning(
            "CACHE_REDIS_WRITE_THROUGH_FAILED: invalidating key after cache set failure key=%s error=%s",
            key,
            exc,
        )
        invalidate(str(key))


def write_through_many(
    entries: CacheEntries,
    *,
    persist: Callable[[object], None],
    ttl_s: int | None = 300,
) -> None:
    """Persist once, then update one or more Redis keys after commit."""

    with storage.transaction() as tx:
        persist(tx)

    try:
        resolved = entries() if callable(entries) else entries
    except Exception as exc:
        LOG.warning(
            "CACHE_REDIS_WRITE_THROUGH_BUILD_FAILED: committed Postgres write but skipped cache update error=%s",
            exc,
        )
        return
    for key, value in dict(resolved or {}).items():
        cache_value = value() if callable(value) else value
        if cache_value is None:
            invalidate(str(key))
            continue
        payload = _as_bytes(cache_value)
        postgres_version = _payload_version(payload)
        redis_pre_write_version = _redis_payload_version(str(key))
        try:
            cache_circuit().call(_redis_set, str(key), payload, ttl_s)
            _record_write_through_lag(
                str(key),
                postgres_version=postgres_version,
                redis_pre_write_version=redis_pre_write_version,
                redis_post_write_version=_redis_payload_version(str(key)),
            )
        except Exception as exc:
            LOG.warning(
                "CACHE_REDIS_WRITE_THROUGH_FAILED: invalidating key after cache set failure key=%s error=%s",
                key,
                exc,
            )
            invalidate(str(key))


def prime(key: str, value: CacheBytes, *, ttl_s: int | None = 300) -> None:
    """Update Redis for data already committed to Postgres."""

    payload = _as_bytes(value)
    try:
        cache_circuit().call(_redis_set, str(key), payload, ttl_s)
    except Exception as exc:
        LOG.warning(
            "CACHE_REDIS_PRIME_FAILED: invalidating key after cache prime failure key=%s error=%s",
            key,
            exc,
        )
        invalidate(str(key))


def invalidate(key: str) -> None:
    try:
        cache_circuit().call(_redis_delete, str(key))
    except Exception:
        try:
            _redis_delete(str(key))
        except Exception:
            LOG.debug("cache invalidate skipped while Redis is unavailable: %s", key)
