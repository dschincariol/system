"""Runtime JSON codec with an explicit fast path and stdlib fallback."""

from __future__ import annotations

import json
from typing import Any, Callable

try:  # pragma: no cover - dependency availability is environment-specific.
    import orjson as _orjson
except Exception:  # pragma: no cover
    _orjson = None  # type: ignore[assignment]


JsonDefault = Callable[[Any], Any]


def orjson_available() -> bool:
    return _orjson is not None


def codec_name() -> str:
    return "orjson" if orjson_available() else "json"


def _normalize_json_input(payload: str | bytes | bytearray | memoryview) -> str | bytes | bytearray:
    if isinstance(payload, memoryview):
        return payload.tobytes()
    if isinstance(payload, (str, bytes, bytearray)):
        return payload
    raise TypeError(f"json payload must be str, bytes, bytearray, or memoryview; got {type(payload).__name__}")


def loads(payload: str | bytes | bytearray | memoryview) -> Any:
    """Decode a JSON payload from text or bytes-like input."""
    normalized = _normalize_json_input(payload)
    if _orjson is not None:
        return _orjson.loads(normalized)
    return json.loads(normalized)


def dumps_bytes(
    value: Any,
    *,
    sort_keys: bool = False,
    default: JsonDefault | None = None,
) -> bytes:
    """Encode JSON as UTF-8 bytes."""
    if _orjson is not None:
        option = _orjson.OPT_SORT_KEYS if sort_keys else 0
        return bytes(_orjson.dumps(value, option=option, default=default))
    text = json.dumps(
        value,
        separators=(",", ":"),
        sort_keys=bool(sort_keys),
        default=default,
        allow_nan=False,
    )
    return text.encode("utf-8")


def dumps_text(
    value: Any,
    *,
    sort_keys: bool = False,
    default: JsonDefault | None = None,
) -> str:
    """Encode JSON as a compact UTF-8 string."""
    return dumps_bytes(value, sort_keys=sort_keys, default=default).decode("utf-8")
