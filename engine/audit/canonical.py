"""Canonical serialization for audit hash-chain rows."""

from __future__ import annotations

import datetime as _dt
import json
import math
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

HASH_COLUMNS = frozenset({"prev_hash", "row_hash"})
_ADAPTER_UNSUPPORTED = object()


def canonical_row_bytes(row: Mapping[str, Any]) -> bytes:
    """Return deterministic JSON bytes for a row excluding hash columns."""

    payload = {
        str(key): _normalize_value(value)
        for key, value in dict(row or {}).items()
        if str(key) not in HASH_COLUMNS
    }
    return _emit(payload).encode("ascii")


def normalize_row_for_hash(row: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize a row into the value domain used by ``canonical_row_bytes``."""

    return {
        str(key): _normalize_value(value)
        for key, value in dict(row or {}).items()
        if str(key) not in HASH_COLUMNS
    }


def _normalize_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int, Decimal)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"non_finite_float:{value!r}")
        if value == 0:
            return 0
        return Decimal(str(value))
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    if isinstance(value, _dt.datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_dt.timezone.utc)
        dt = dt.astimezone(_dt.timezone.utc)
        return dt.isoformat(timespec="microseconds").replace("+00:00", "Z")
    if isinstance(value, _dt.date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(k): _normalize_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_value(v) for v in value]
    if isinstance(value, set):
        return sorted((_normalize_value(v) for v in value), key=_emit)
    if hasattr(value, "item"):
        try:
            item_value = value.item()
        except (AttributeError, TypeError, ValueError):
            # fallback: array-like values may expose item() while not being scalar.
            item_value = _ADAPTER_UNSUPPORTED
        if item_value is not _ADAPTER_UNSUPPORTED:
            return _normalize_value(item_value)
    if hasattr(value, "tolist"):
        try:
            list_value = value.tolist()
        except (AttributeError, TypeError, ValueError):
            # fallback: non-array adapters can advertise tolist() without supporting it.
            list_value = _ADAPTER_UNSUPPORTED
        if list_value is not _ADAPTER_UNSUPPORTED:
            return _normalize_value(list_value)
    return str(value)


def _emit(value: Any) -> str:
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, int) and not isinstance(value, bool):
        return str(int(value))
    if isinstance(value, Decimal):
        return _emit_decimal(value)
    if isinstance(value, float):
        return _emit(_normalize_value(value))
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=True, separators=(",", ":"))
    if isinstance(value, Mapping):
        parts = []
        for key in sorted(str(k) for k in value.keys()):
            parts.append(f"{_emit(str(key))}:{_emit(_normalize_value(value[key]))}")
        return "{" + ",".join(parts) + "}"
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_emit(_normalize_value(v)) for v in value) + "]"
    return _emit(_normalize_value(value))


def _emit_decimal(value: Decimal) -> str:
    try:
        dec = value.normalize()
    except InvalidOperation as exc:
        raise ValueError(f"invalid_decimal:{value!r}") from exc
    if not dec.is_finite():
        raise ValueError(f"non_finite_decimal:{value!r}")
    if dec == 0:
        return "0"
    text = format(dec, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text
