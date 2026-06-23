"""Canonical cache serialization."""

from __future__ import annotations

import json
import os
import time
from decimal import Decimal
from typing import Any

try:  # pragma: no cover - dependency availability is environment-specific.
    import msgpack as _msgpack
except Exception:  # pragma: no cover
    _msgpack = None


CURRENT_VERSION = 1
_TRUE_VALUES = {"1", "true", "yes", "y", "on"}
_PRODUCTION_VALUES = {"prod", "production", "live"}
_LIVE_LIKE_MODES = {"live", "shadow", "paper"}


class CacheCodecError(ValueError):
    """Base class for cache payload decode errors."""


class UnsupportedCacheVersion(CacheCodecError):
    """Raised when a payload version is not understood by this code."""


def _now_ms() -> int:
    return int(time.time() * 1000)


def msgpack_available() -> bool:
    """Return whether the binary envelope codec is available."""
    return _msgpack is not None


def _env_truthy(name: str, default: bool = False) -> bool:
    raw = os.environ.get(str(name), "")
    if raw is None or str(raw).strip() == "":
        return bool(default)
    return str(raw).strip().lower() in _TRUE_VALUES


def _runtime_requires_msgpack() -> bool:
    if _env_truthy("CACHE_CODEC_REQUIRE_MSGPACK", False):
        return True
    try:
        from engine.runtime.config_schema import get_runtime_safety_context

        return bool((get_runtime_safety_context() or {}).get("strict_runtime"))
    except Exception:
        # no-op-guard: allow - early imports fall back to direct env inspection below.
        pass

    env_values = [
        os.environ.get("ENV"),
        os.environ.get("APP_ENV"),
        os.environ.get("TS_ENV"),
        os.environ.get("NODE_ENV"),
    ]
    if any(str(value or "").strip().lower() in _PRODUCTION_VALUES for value in env_values):
        return True
    if _env_truthy("ENGINE_SUPERVISED", False):
        return True

    explicit_non_prod = any(str(value or "").strip().lower() in {"dev", "development", "test"} for value in env_values)
    mode_values = [
        os.environ.get("ENGINE_MODE"),
        os.environ.get("EXECUTION_MODE"),
        os.environ.get("OPERATOR_MODE"),
    ]
    return bool((not explicit_non_prod) and any(str(value or "").strip().lower() in _LIVE_LIKE_MODES for value in mode_values))


def json_fallback_allowed() -> bool:
    """Return whether the explicit non-production JSON fallback is enabled."""
    return bool((not _runtime_requires_msgpack()) and _env_truthy("CACHE_CODEC_ALLOW_JSON_FALLBACK", False))


def codec_name() -> str:
    if msgpack_available():
        return "msgpack"
    if json_fallback_allowed():
        return "json"
    return "unavailable"


def require_msgpack() -> None:
    if not msgpack_available():
        raise CacheCodecError("cache_msgpack_dependency_unavailable")


def require_codec_ready() -> None:
    if msgpack_available() or json_fallback_allowed():
        return
    if _runtime_requires_msgpack():
        raise CacheCodecError("cache_msgpack_required_in_production")
    raise CacheCodecError("cache_msgpack_unavailable_json_fallback_disabled")


def readiness_snapshot() -> dict[str, Any]:
    """Return startup/preflight-visible codec readiness state."""
    msgpack_ok = bool(msgpack_available())
    required = bool(_runtime_requires_msgpack())
    fallback_allowed = bool(json_fallback_allowed())
    backend = str(os.environ.get("LIVE_CACHE_BACKEND", "auto") or "auto").strip().lower() or "auto"
    require_reasons: list[str] = []
    if required:
        require_reasons.append("strict_runtime")
    if backend == "redis":
        required = True
        require_reasons.append("live_cache_backend_redis")
    if _env_truthy("CACHE_CODEC_REQUIRE_MSGPACK", False):
        required = True
        require_reasons.append("CACHE_CODEC_REQUIRE_MSGPACK")

    blockers: list[str] = []
    warnings: list[str] = []
    if not msgpack_ok and required:
        blockers.append("cache_msgpack_dependency_unavailable")
    elif not msgpack_ok and fallback_allowed:
        warnings.append("cache_codec_json_fallback_enabled")
    elif not msgpack_ok:
        warnings.append("cache_codec_unavailable_json_fallback_disabled")

    return {
        "ok": bool(msgpack_ok or (not required and fallback_allowed)),
        "required": bool(required),
        "codec": codec_name(),
        "msgpack_available": msgpack_ok,
        "json_fallback_allowed": fallback_allowed,
        "backend": backend,
        "require_reasons": require_reasons,
        "blockers": blockers,
        "warnings": warnings,
    }


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
    require_codec_ready()
    return json.dumps(envelope, separators=(",", ":"), sort_keys=True).encode("utf-8")


def decode(payload: bytes | bytearray | memoryview, *, expected_version: int | None = None) -> Any:
    raw = bytes(payload)
    try:
        if _msgpack is not None:
            envelope = _msgpack.unpackb(raw, raw=False)
        else:
            require_codec_ready()
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
            require_codec_ready()
            envelope = json.loads(raw.decode("utf-8"))
    except Exception:
        return None
    if not isinstance(envelope, dict) or "v" not in envelope:
        return None
    try:
        return int(envelope.get("v") or 0)
    except Exception:
        return None
