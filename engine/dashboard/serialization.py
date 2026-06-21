"""Small serialization helpers used by dashboard handlers."""

from __future__ import annotations

import json
from typing import Any, Callable, cast


def json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return cast(dict[str, Any], value)
    return {}


def snapshot_json_default(value: Any, *, warn_nonfatal: Callable[..., None]) -> str:
    try:
        return str(value)
    except Exception as e:
        warn_nonfatal(
            "DASHBOARD_SERVER_SNAPSHOT_JSON_DEFAULT_FAILED",
            e,
            value_type=type(value).__name__,
        )
        return repr(value)


def normalize_explain_json(
    value: Any,
    *,
    warn_nonfatal: Callable[..., None],
    log_failure_fn: Callable[..., None],
    log: Any,
) -> str:
    if value is None:
        return "{}"
    try:
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8", errors="replace")
    except Exception as e:
        log_failure_fn(
            log,
            event="dashboard_server_snapshot_decode_failed",
            code="DASHBOARD_SERVER_SNAPSHOT_DECODE_FAILED",
            message=str(e),
            error=e,
            level=30,
            component="dashboard_server",
            include_health=False,
            persist=True,
        )

    s = str(value).strip()
    if not s:
        return "{}"

    try:
        json.loads(s)
        return s
    except Exception as e:
        warn_nonfatal("DASHBOARD_SERVER_EXPLAIN_JSON_PARSE_FAILED", e)
        return json.dumps({"raw": s})
