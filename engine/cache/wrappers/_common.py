"""Shared helpers for typed cache wrappers."""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Callable

from engine.cache import store
from engine.runtime.metrics import emit_counter

LOG = logging.getLogger(__name__)


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
