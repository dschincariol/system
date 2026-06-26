"""Shared helpers for graceful read-side API degradation."""

from __future__ import annotations

import re
from typing import Any


_MISSING_RELATION_MARKERS = (
    "no such table",
    "undefinedtable",
    "undefined table",
    "does not exist",
)


def _normalize_identifier(value: str) -> str:
    text = str(value or "").strip().strip('"').strip("'").lower()
    if "." in text:
        text = text.rsplit(".", 1)[-1]
    return re.sub(r"[^a-z0-9_]+", "", text)


def is_missing_table_error(error: BaseException, table_name: str | None = None) -> bool:
    """Return True when a DB exception represents a missing relation/table."""

    text = str(error or "").strip().lower()
    if not text or not any(marker in text for marker in _MISSING_RELATION_MARKERS):
        return False
    expected = _normalize_identifier(str(table_name or ""))
    if not expected:
        return True
    return expected in {_normalize_identifier(match) for match in re.findall(r"[a-zA-Z_][a-zA-Z0-9_.]*", text)}


def missing_table_reason(table_name: str) -> str:
    table = _normalize_identifier(table_name)
    return f"{table}_missing" if table else "table_missing"


def degraded_empty_read(
    reason: str,
    *,
    source: str,
    ready: bool = False,
    table_present: bool | None = None,
    count: int = 0,
    status: int = 200,
    **extra: Any,
) -> dict[str, Any]:
    """Build a reasoned empty read payload for optional dashboard data."""

    normalized_reason = str(reason or "no_data_yet").strip() or "no_data_yet"
    normalized_source = str(source or "unknown").strip() or "unknown"
    payload: dict[str, Any] = {
        "ok": True,
        "error": None,
        "ready": bool(ready),
        "reason": normalized_reason,
        "source": normalized_source,
        "meta": {
            "status": int(status),
            "ready": bool(ready),
            "reason": normalized_reason,
            "source": normalized_source,
            "count": int(count),
        },
    }
    if table_present is not None:
        payload["table_present"] = bool(table_present)
        payload["meta"]["table_present"] = bool(table_present)
    payload.update(extra)
    return payload


def degraded_missing_table_read(table_name: str, **extra: Any) -> dict[str, Any]:
    """Build the standard 200/empty-rows payload for optional missing tables."""

    reason = missing_table_reason(table_name)
    payload: dict[str, Any] = degraded_empty_read(
        reason,
        source=str(table_name),
        table_present=False,
        rows=[],
    )
    payload.update(extra)
    payload.setdefault("rows", [])
    return payload


__all__ = ["degraded_empty_read", "degraded_missing_table_read", "is_missing_table_error", "missing_table_reason"]
