"""Write-through Redis cache API."""

from __future__ import annotations

import logging
import hashlib
import os
import threading
import time
from collections.abc import Iterable, Mapping
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
CacheBatchLoader = Callable[[list[str]], Mapping[str, CacheBytes | None] | None]

_TTL_JITTER_RATIO = 0.10
_SINGLEFLIGHT_LOCK_TIMEOUT_S = 5.0
_SINGLEFLIGHT_MAX_LOCKS = 4096
_LOAD_LOCKS_GUARD = threading.RLock()
_LOAD_LOCKS: dict[str, "_LoadLockEntry"] = {}


class _SingleFlightLockTimeout(RuntimeError):
    pass


class _LoadLockEntry:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.ref_count = 0
        self.generation = 0
        self.last_payload: bytes | None = None
        self.last_missing = False
        self.last_error: BaseException | None = None
        self.last_used_monotonic = time.monotonic()


def _as_bytes(value: CacheBytes) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    return str(value).encode("utf-8")


def _ttl_with_jitter(ttl_s: int | None, key: str) -> int | None:
    if ttl_s is None:
        return None
    base = max(1, int(ttl_s))
    if base <= 1 or _TTL_JITTER_RATIO <= 0.0:
        return base
    span = max(1, int(round(float(base) * float(_TTL_JITTER_RATIO))))
    digest = hashlib.blake2b(str(key).encode("utf-8"), digest_size=2).digest()
    offset = int.from_bytes(digest, "big") % ((span * 2) + 1)
    return max(1, int(base - span + offset))


def _singleflight_lock_timeout_s() -> float:
    raw = str(os.environ.get("TS_REDIS_SINGLEFLIGHT_LOCK_TIMEOUT_S", "") or "").strip()
    try:
        value = float(raw) if raw else float(_SINGLEFLIGHT_LOCK_TIMEOUT_S)
    except Exception:
        value = float(_SINGLEFLIGHT_LOCK_TIMEOUT_S)
    return max(0.001, float(value))


def _singleflight_max_locks() -> int:
    raw = str(os.environ.get("TS_REDIS_SINGLEFLIGHT_MAX_LOCKS", "") or "").strip()
    try:
        value = int(float(raw)) if raw else int(_SINGLEFLIGHT_MAX_LOCKS)
    except Exception:
        value = int(_SINGLEFLIGHT_MAX_LOCKS)
    return max(128, int(value))


def _emit_singleflight_counter(metric: str, value: int = 1, **tags: object) -> None:
    try:
        emit_counter(
            metric,
            int(value),
            component="engine.cache.store",
            extra_tags={str(key): tag_value for key, tag_value in dict(tags or {}).items()},
        )
    except Exception as exc:
        LOG.debug("cache single-flight metric emit failed metric=%s error=%s", metric, exc)


def _cleanup_load_locks_locked() -> None:
    max_locks = _singleflight_max_locks()
    overflow = len(_LOAD_LOCKS) - max_locks
    if overflow <= 0:
        return
    idle = [
        (entry.last_used_monotonic, key)
        for key, entry in _LOAD_LOCKS.items()
        if entry.ref_count <= 0 and not entry.lock.locked()
    ]
    for _last_used, key in sorted(idle)[:overflow]:
        _LOAD_LOCKS.pop(key, None)


def _get_load_lock(key: str) -> _LoadLockEntry:
    cache_key = str(key)
    with _LOAD_LOCKS_GUARD:
        _cleanup_load_locks_locked()
        entry = _LOAD_LOCKS.get(cache_key)
        if entry is None:
            entry = _LoadLockEntry()
            _LOAD_LOCKS[cache_key] = entry
        entry.ref_count += 1
        entry.last_used_monotonic = time.monotonic()
        return entry


def _release_load_lock(key: str, entry: _LoadLockEntry) -> None:
    cache_key = str(key)
    with _LOAD_LOCKS_GUARD:
        current = _LOAD_LOCKS.get(cache_key)
        if current is None or current is not entry:
            return
        current.ref_count = max(0, int(current.ref_count) - 1)
        current.last_used_monotonic = time.monotonic()
        if current.ref_count <= 0 and not current.lock.locked():
            _LOAD_LOCKS.pop(cache_key, None)
        else:
            _cleanup_load_locks_locked()


def _acquire_load_lock(key: str, entry: _LoadLockEntry, *, path: str) -> tuple[bool, bool]:
    if entry.lock.acquire(blocking=False):
        return True, False

    _emit_singleflight_counter("cache_singleflight_waits_total", 1, path=path)
    started = time.monotonic()
    acquired = entry.lock.acquire(timeout=_singleflight_lock_timeout_s())
    if acquired:
        return True, True

    wait_ms = int(max(0.0, time.monotonic() - started) * 1000.0)
    _emit_singleflight_counter(
        "cache_singleflight_failures_total",
        1,
        path=path,
        reason="lock_timeout",
    )
    LOG.warning(
        "CACHE_SINGLEFLIGHT_LOCK_TIMEOUT: falling back to direct loader key=%s path=%s wait_ms=%s",
        key,
        path,
        wait_ms,
    )
    return False, True


def _record_singleflight_result(
    entry: _LoadLockEntry,
    *,
    payload: bytes | None,
    missing: bool = False,
    error: BaseException | None = None,
) -> None:
    entry.generation += 1
    entry.last_payload = payload
    entry.last_missing = bool(missing)
    entry.last_error = error
    entry.last_used_monotonic = time.monotonic()


def _singleflight_reused_result(entry: _LoadLockEntry, start_generation: int) -> tuple[bool, bytes | None]:
    if int(entry.generation) <= int(start_generation):
        return False, None
    if entry.last_error is not None:
        raise entry.last_error
    if entry.last_missing:
        return True, None
    return True, entry.last_payload


def _unique_keys(keys: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for key in keys or []:
        cache_key = str(key)
        if not cache_key or cache_key in seen:
            continue
        seen.add(cache_key)
        out.append(cache_key)
    return out


def _redis_get(key: str):
    client = redis_pool()
    return client.get(str(key))


def _redis_set(key: str, value: bytes, ttl_s: int | None = None):
    client = redis_pool()
    if ttl_s is None:
        return client.set(str(key), value)
    return client.set(str(key), value, ex=max(1, int(ttl_s)))


def _redis_mget(keys: list[str]):
    client = redis_pool()
    return client.mget([str(key) for key in keys])


def _redis_pipeline_set_many(entries: Mapping[str, bytes], ttl_s: int | None = None, *, jitter: bool = True):
    client = redis_pool()
    entry_map = {str(key): value for key, value in dict(entries or {}).items()}
    if not entry_map:
        return "none"
    pipeline_fn = getattr(client, "pipeline", None)
    if not callable(pipeline_fn):
        for key, value in entry_map.items():
            effective_ttl_s = _ttl_with_jitter(ttl_s, str(key)) if jitter else ttl_s
            if effective_ttl_s is None:
                client.set(str(key), value)
            else:
                client.set(str(key), value, ex=max(1, int(effective_ttl_s)))
        return "sequential_set_many"
    pipe = pipeline_fn(transaction=False)
    for key, value in entry_map.items():
        cache_key = str(key)
        effective_ttl_s = _ttl_with_jitter(ttl_s, cache_key) if jitter else ttl_s
        if effective_ttl_s is None:
            pipe.set(cache_key, value)
        else:
            pipe.set(cache_key, value, ex=effective_ttl_s)
    pipe.execute()
    return "pipeline_set_many"


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


def _version_tag(version: int | None) -> str:
    return "missing" if version is None else str(int(version))


def _payload_versions_tag(values: Iterable[bytes]) -> str:
    versions = {_version_tag(_payload_version(value)) for value in list(values or [])}
    if not versions:
        return "none"
    if len(versions) == 1:
        return next(iter(versions))
    return "mixed"


def _record_write_through_path(
    mode: str,
    *,
    key_count: int,
    result: str,
    payload_version: str = "none",
) -> None:
    try:
        emit_counter(
            "cache_write_through_path_total",
            1,
            component="engine.cache.store",
            extra_tags={
                "mode": str(mode),
                "result": str(result),
                "key_count": str(max(0, int(key_count))),
                "payload_version": str(payload_version or "none"),
            },
        )
    except Exception as exc:
        LOG.debug("cache write-through path metric emit failed mode=%s error=%s", mode, exc)


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

    entry = _get_load_lock(str(key))
    start_generation = int(entry.generation)
    acquired = False
    try:
        acquired, waited = _acquire_load_lock(str(key), entry, path="read")
        if not acquired:
            try:
                raw = cache_circuit().call(_redis_get, str(key))
            except Exception:
                raw = None
            if raw is not None:
                return _as_bytes(raw)
            try:
                loaded = loader()
            except Exception:
                _emit_singleflight_counter(
                    "cache_singleflight_failures_total",
                    1,
                    path="read",
                    reason="loader_exception_after_lock_timeout",
                )
                raise
            if loaded is None:
                return None
            payload = _as_bytes(loaded)
            try:
                cache_circuit().call(_redis_set, str(key), payload, _ttl_with_jitter(ttl_s, str(key)))
            except Exception as exc:
                LOG.warning(
                    "CACHE_REDIS_POPULATE_FAILED: serving Postgres-loaded value without Redis key=%s error=%s",
                    key,
                    exc,
                )
            return payload

        try:
            raw = cache_circuit().call(_redis_get, str(key))
        except Exception:
            raw = None
        if raw is not None:
            return _as_bytes(raw)

        if waited:
            reused, payload = _singleflight_reused_result(entry, start_generation)
            if reused:
                return payload

        _emit_singleflight_counter("cache_singleflight_wins_total", 1, path="read")
        try:
            loaded = loader()
        except Exception as exc:
            _record_singleflight_result(entry, payload=None, error=exc)
            _emit_singleflight_counter(
                "cache_singleflight_failures_total",
                1,
                path="read",
                reason="loader_exception",
            )
            raise
        if loaded is None:
            _record_singleflight_result(entry, payload=None, missing=True)
            return None
        payload = _as_bytes(loaded)
        _record_singleflight_result(entry, payload=payload)
        try:
            cache_circuit().call(_redis_set, str(key), payload, _ttl_with_jitter(ttl_s, str(key)))
        except Exception as exc:
            LOG.warning(
                "CACHE_REDIS_POPULATE_FAILED: serving Postgres-loaded value without Redis key=%s error=%s",
                key,
                exc,
            )
        return payload
    finally:
        if acquired:
            try:
                entry.lock.release()
            except RuntimeError:
                pass
        _release_load_lock(str(key), entry)


def read_many(
    keys: Iterable[str],
    batch_loader: CacheBatchLoader,
    ttl_s: int | None = 300,
) -> dict[str, bytes | None]:
    """Read many byte values, loading misses once and pipelining backfill.

    The returned dict contains one entry per distinct input key in first-seen
    order. Duplicate input keys share the same cache lookup/load result and do
    not trigger duplicate loader or backfill work.
    """

    cache_keys = _unique_keys(keys)
    if not cache_keys:
        return {}

    out: dict[str, bytes | None] = {key: None for key in cache_keys}
    raw_values: list[object | None]
    try:
        raw_result = cache_circuit().call(_redis_mget, cache_keys)
        raw_values = list(raw_result or [])
    except Exception:
        raw_values = []
    if len(raw_values) != len(cache_keys):
        raw_values = [None] * len(cache_keys)

    misses: list[str] = []
    for key, raw in zip(cache_keys, raw_values):
        if raw is None:
            misses.append(key)
            continue
        out[key] = _as_bytes(raw)  # type: ignore[arg-type]

    if not misses:
        return out

    lock_entries = [(key, _get_load_lock(key)) for key in sorted(set(misses))]
    start_generations = {key: int(entry.generation) for key, entry in lock_entries}
    acquired_entries: list[tuple[str, _LoadLockEntry]] = []
    try:
        for key, entry in lock_entries:
            acquired, _waited = _acquire_load_lock(key, entry, path="read_many")
            if not acquired:
                raise _SingleFlightLockTimeout(f"cache_singleflight_lock_timeout:{key}")
            acquired_entries.append((key, entry))

        recheck_values: list[object | None]
        try:
            raw_recheck = cache_circuit().call(_redis_mget, misses)
            recheck_values = list(raw_recheck or [])
        except Exception:
            recheck_values = []
        if len(recheck_values) != len(misses):
            recheck_values = [None] * len(misses)

        still_missing: list[str] = []
        for key, raw in zip(misses, recheck_values):
            if raw is None:
                still_missing.append(key)
                continue
            out[key] = _as_bytes(raw)  # type: ignore[arg-type]

        if not still_missing:
            return out

        missing_after_singleflight: list[str] = []
        entry_by_key = {key: entry for key, entry in lock_entries}
        for key in still_missing:
            entry = entry_by_key[key]
            reused, payload = _singleflight_reused_result(entry, start_generations.get(key, 0))
            if reused:
                out[key] = payload
            else:
                missing_after_singleflight.append(key)

        if not missing_after_singleflight:
            return out

        _emit_singleflight_counter(
            "cache_singleflight_wins_total",
            len(missing_after_singleflight),
            path="read_many",
        )
        try:
            loaded = dict(batch_loader(list(missing_after_singleflight)) or {})
        except Exception as exc:
            for key in missing_after_singleflight:
                _record_singleflight_result(entry_by_key[key], payload=None, error=exc)
            _emit_singleflight_counter(
                "cache_singleflight_failures_total",
                len(missing_after_singleflight),
                path="read_many",
                reason="loader_exception",
            )
            raise
        backfill: dict[str, bytes] = {}
        for key in missing_after_singleflight:
            value = loaded.get(key)
            if value is None:
                out[key] = None
                _record_singleflight_result(entry_by_key[key], payload=None, missing=True)
                continue
            payload = _as_bytes(value)
            out[key] = payload
            backfill[key] = payload
            _record_singleflight_result(entry_by_key[key], payload=payload)

        if backfill:
            try:
                cache_circuit().call(_redis_pipeline_set_many, backfill, ttl_s)
            except Exception as exc:
                LOG.warning(
                    "CACHE_REDIS_BATCH_POPULATE_FAILED: serving Postgres-loaded values without Redis backfill count=%s error=%s",
                    len(backfill),
                    exc,
                )
        return out
    except _SingleFlightLockTimeout:
        for _key, entry in reversed(acquired_entries):
            try:
                entry.lock.release()
            except RuntimeError:
                pass
        acquired_entries.clear()
        try:
            raw_recheck = cache_circuit().call(_redis_mget, misses)
            recheck_values = list(raw_recheck or [])
        except Exception:
            recheck_values = []
        if len(recheck_values) != len(misses):
            recheck_values = [None] * len(misses)
        fallback_missing: list[str] = []
        for key, raw in zip(misses, recheck_values):
            if raw is None:
                fallback_missing.append(key)
            else:
                out[key] = _as_bytes(raw)  # type: ignore[arg-type]
        if not fallback_missing:
            return out
        try:
            loaded = dict(batch_loader(list(fallback_missing)) or {})
        except Exception:
            _emit_singleflight_counter(
                "cache_singleflight_failures_total",
                len(fallback_missing),
                path="read_many",
                reason="loader_exception_after_lock_timeout",
            )
            raise
        backfill = {}
        for key in fallback_missing:
            value = loaded.get(key)
            if value is None:
                out[key] = None
                continue
            payload = _as_bytes(value)
            out[key] = payload
            backfill[key] = payload
        if backfill:
            try:
                cache_circuit().call(_redis_pipeline_set_many, backfill, ttl_s)
            except Exception as exc:
                LOG.warning(
                    "CACHE_REDIS_BATCH_POPULATE_FAILED: serving Postgres-loaded values without Redis backfill count=%s error=%s",
                    len(backfill),
                    exc,
                )
        return out
    finally:
        for _key, entry in reversed(acquired_entries):
            try:
                entry.lock.release()
            except RuntimeError:
                pass
        for key, entry in lock_entries:
            _release_load_lock(key, entry)


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
        _record_write_through_path(
            "invalidate",
            key_count=1,
            result="success",
            payload_version="none",
        )
        return

    payload = _as_bytes(cache_value)
    payload_version = _version_tag(_payload_version(payload))
    try:
        cache_circuit().call(_redis_set, str(key), payload, ttl_s)
        _record_write_through_path(
            "single_set",
            key_count=1,
            result="success",
            payload_version=payload_version,
        )
    except Exception as exc:
        LOG.warning(
            "CACHE_REDIS_WRITE_THROUGH_FAILED: invalidating key after cache set failure key=%s error=%s",
            key,
            exc,
        )
        invalidate(str(key))
        _record_write_through_path(
            "single_set",
            key_count=1,
            result="failure",
            payload_version=payload_version,
        )


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
    pending_sets: dict[str, bytes] = {}
    invalidated = 0
    for key, value in dict(resolved or {}).items():
        cache_value = value() if callable(value) else value
        if cache_value is None:
            invalidate(str(key))
            invalidated += 1
            continue
        pending_sets[str(key)] = _as_bytes(cache_value)
    if invalidated:
        _record_write_through_path(
            "invalidate_many",
            key_count=int(invalidated),
            result="success",
            payload_version="none",
        )
    if not pending_sets:
        return

    payload_version = _payload_versions_tag(pending_sets.values())
    mode_hint = "single_set" if len(pending_sets) == 1 else "pipeline_set_many"
    try:
        mode = cache_circuit().call(_redis_pipeline_set_many, pending_sets, ttl_s, jitter=False)
        _record_write_through_path(
            str(mode or mode_hint),
            key_count=len(pending_sets),
            result="success",
            payload_version=payload_version,
        )
    except Exception as exc:
        LOG.warning(
            "CACHE_REDIS_WRITE_THROUGH_FAILED: invalidating keys after cache batch set failure count=%s error=%s",
            len(pending_sets),
            exc,
        )
        for key in pending_sets:
            invalidate(str(key))
        _record_write_through_path(
            mode_hint,
            key_count=len(pending_sets),
            result="failure",
            payload_version=payload_version,
        )


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
