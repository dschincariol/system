"""Shared helpers for typed cache wrappers."""

from __future__ import annotations

import copy
import json
import logging
import threading
import time
from typing import Any, Callable

from engine.cache import store
from engine.runtime.metrics import emit_counter

LOG = logging.getLogger(__name__)
_L1_LOCK = threading.RLock()
_L1_CACHE: dict[str, tuple[float, Any]] = {}
L1_HOT_WRAPPER_TTL_S = 1.0
L1_NEGATIVE_CACHE_TTL_S = 1.0
L1_HOT_WRAPPER_MAX_ENTRIES = 2048


class _L1Missing:
    def __deepcopy__(self, memo: dict[int, Any]) -> "_L1Missing":
        return self


L1_MISSING = _L1Missing()


def now_ms() -> int:
    return int(time.time() * 1000)


def parse_json(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return default


def dumps_json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def l1_get(key: str) -> Any:
    cache_key = str(key)
    now = time.monotonic()
    with _L1_LOCK:
        item = _L1_CACHE.get(cache_key)
        if item is None:
            return None
        expires_at, value = item
        if expires_at <= now:
            _L1_CACHE.pop(cache_key, None)
            return None
        return copy.deepcopy(value)


def l1_set(key: str, value: Any, *, ttl_s: float = L1_HOT_WRAPPER_TTL_S) -> None:
    try:
        ttl = float(ttl_s)
    except (TypeError, ValueError):
        ttl = float(L1_HOT_WRAPPER_TTL_S)
    if ttl <= 0.0:
        l1_invalidate(key)
        return
    with _L1_LOCK:
        _l1_prune_locked()
        _L1_CACHE[str(key)] = (time.monotonic() + ttl, copy.deepcopy(value))
        _l1_prune_locked()


def l1_set_missing(key: str, *, ttl_s: float = L1_NEGATIVE_CACHE_TTL_S) -> None:
    l1_set(key, L1_MISSING, ttl_s=ttl_s)


def l1_is_missing(value: Any) -> bool:
    return value is L1_MISSING


def l1_invalidate(key: str) -> None:
    with _L1_LOCK:
        _L1_CACHE.pop(str(key), None)


def l1_clear() -> None:
    with _L1_LOCK:
        _L1_CACHE.clear()


def l1_size() -> int:
    with _L1_LOCK:
        _l1_prune_locked()
        return len(_L1_CACHE)


def _l1_max_entries() -> int:
    return max(1, int(L1_HOT_WRAPPER_MAX_ENTRIES))


def _l1_prune_locked() -> None:
    now = time.monotonic()
    for cache_key, (expires_at, _value) in list(_L1_CACHE.items()):
        if expires_at <= now:
            _L1_CACHE.pop(cache_key, None)

    overflow = len(_L1_CACHE) - _l1_max_entries()
    if overflow <= 0:
        return
    oldest = sorted(
        (expires_at, cache_key)
        for cache_key, (expires_at, _value) in _L1_CACHE.items()
    )
    for _expires_at, cache_key in oldest[:overflow]:
        _L1_CACHE.pop(cache_key, None)


def row_dict(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    keys = getattr(row, "keys", None)
    if callable(keys):
        return {str(key): row[key] for key in keys()}
    return {}


def after_commit_or_now(con: Any, callback: Callable[[], None]) -> None:
    register = getattr(con, "register_after_commit", None)
    if callable(register) and bool(getattr(con, "in_transaction", False)):
        register(callback)
        return
    if not bool(getattr(con, "in_transaction", False)):
        callback()


def reload_after_codec_version_mismatch(
    key: str,
    loader: Callable[[], bytes | bytearray | memoryview | str | None],
    *,
    ttl_s: int | None,
    wrapper: str,
    expected_version: int,
    error: BaseException,
) -> bytes | None:
    LOG.warning(
        "CACHE_CODEC_VERSION_MISMATCH: invalidating stale cache key=%s wrapper=%s expected_version=%s error=%s",
        key,
        wrapper,
        expected_version,
        error,
    )
    emit_counter(
        "codec_version_mismatch_count",
        1,
        component="engine.cache",
        extra_tags={
            "wrapper": str(wrapper),
            "key": str(key),
            "expected_version": int(expected_version),
        },
    )

    store.invalidate(key)
    loaded = loader()
    if loaded is None:
        return None
    store.prime(key, loaded, ttl_s=ttl_s)
    if isinstance(loaded, bytes):
        return loaded
    if isinstance(loaded, (bytearray, memoryview)):
        return bytes(loaded)
    return str(loaded).encode("utf-8")
