"""Canonical cache serialization."""

from __future__ import annotations

import json
import time
from decimal import Decimal
from typing import Any

try:  # pragma: no cover - dependency availability is environment-specific.
    import msgpack as _msgpack
except Exception:  # pragma: no cover
    _msgpack = None


CURRENT_VERSION = 1


class CacheCodecError(ValueError):
    """Base class for cache payload decode errors."""


class UnsupportedCacheVersion(CacheCodecError):
    """Raised when a payload version is not understood by this code."""


def _now_ms() -> int:
    return int(time.time() * 1000)


def _canonical(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _canonical(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical(v) for v in value]
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, memoryview):
        return bytes(value)
    return value


def encode(data: Any, *, version: int | None = None, ts_ms: int | None = None) -> bytes:
    envelope = {
        "v": int(CURRENT_VERSION if version is None else version),
        "ts": int(ts_ms if ts_ms is not None else _now_ms()),
        "data": _canonical(data),
    }
    if _msgpack is not None:
        return bytes(_msgpack.packb(envelope, use_bin_type=True))
    return json.dumps(envelope, separators=(",", ":"), sort_keys=True).encode("utf-8")


def decode(payload: bytes | bytearray | memoryview, *, expected_version: int | None = None) -> Any:
    raw = bytes(payload)
    try:
        if _msgpack is not None:
            envelope = _msgpack.unpackb(raw, raw=False)
        else:
            envelope = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise CacheCodecError(f"cache_payload_decode_failed:{type(exc).__name__}") from exc
    if not isinstance(envelope, dict):
        raise CacheCodecError("cache_payload_envelope_not_dict")
    version = int(envelope.get("v") or 0)
    expected = int(CURRENT_VERSION if expected_version is None else expected_version)
    if version != expected:
        raise UnsupportedCacheVersion(f"unsupported_cache_payload_version:{version}")
    if "data" not in envelope:
        raise CacheCodecError("cache_payload_missing_data")
    return envelope["data"]


def envelope_version(payload: bytes | bytearray | memoryview) -> int | None:
    raw = bytes(payload)
    try:
        if _msgpack is not None:
            envelope = _msgpack.unpackb(raw, raw=False)
        else:
            envelope = json.loads(raw.decode("utf-8"))
    except Exception:
        return None
    if not isinstance(envelope, dict) or "v" not in envelope:
        return None
    try:
        return int(envelope.get("v") or 0)
    except Exception:
        return None
