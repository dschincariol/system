"""Normalization, serialization, and nonfatal logging helpers for runtime health."""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Callable, Dict, List


WarnFn = Callable[..., None]


def warn_nonfatal(
    logger: Any,
    log_failure_fn: Callable[..., None],
    scope: str,
    err: Exception,
    **extra: Any,
) -> None:
    """Emit the runtime health nonfatal warning event."""
    log_failure_fn(
        logger,
        event="runtime_health_nonfatal",
        code=str(scope).replace(".", "_"),
        message=str(scope),
        error=err,
        level=logging.WARNING,
        component="engine.runtime.health",
        extra=dict(extra or {}) or None,
        persist=False,
    )


def trace_section(
    name: str,
    started: float,
    *,
    enabled: bool,
    logger: Any,
    warn: WarnFn,
    perf_counter: Callable[[], float] = time.perf_counter,
    **extra: Any,
) -> None:
    """Emit optional health section timing without changing failure tolerance."""
    if not enabled:
        return
    payload: Dict[str, Any] = {
        "section": str(name),
        "elapsed_ms": round((perf_counter() - float(started)) * 1000.0, 2),
    }
    if extra:
        payload.update(dict(extra))
    try:
        logger.info(
            "health_snapshot_section",
            extra={
                "event": "health_snapshot_section",
                "extra_json": payload,
            },
        )
    except Exception as e:
        warn("health.trace_section", e, section=str(name))


def int_or(value: Any, default: int = 0, *, warn: WarnFn | None = None) -> int:
    if value is None or str(value).strip() == "":
        return int(default)
    try:
        return int(value)
    except Exception as e:
        if warn is not None:
            warn("health.int_or", e, value_type=type(value).__name__)
        return int(default)


def float_or(value: Any, default: float = 0.0, *, warn: WarnFn | None = None) -> float:
    if value is None or str(value).strip() == "":
        return float(default)
    try:
        return float(value)
    except Exception as e:
        if warn is not None:
            warn("health.float_or", e, value_type=type(value).__name__)
        return float(default)


def dedupe_strs(values: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values or []:
        key = str(value or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def dict_or_empty(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def json_dict_or_empty(raw: Any, *, warn: WarnFn | None = None) -> Dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except Exception as e:
        if warn is not None:
            warn("health.json_dict_or_empty.decode", e, raw_preview=text[:120])
        return {}
    return payload if isinstance(payload, dict) else {}


def json_list_or_empty(raw: Any, *, warn: WarnFn | None = None) -> List[Any]:
    text = str(raw or "").strip()
    if not text:
        return []
    try:
        payload = json.loads(text)
    except Exception as e:
        if warn is not None:
            warn("health.json_list_or_empty.decode", e, raw_preview=text[:120])
        return []
    return list(payload) if isinstance(payload, list) else []


def json_meta_get(
    key: str,
    *,
    meta_get: Callable[[str, str], Any],
    warn: WarnFn,
) -> Dict[str, Any]:
    try:
        raw = str(meta_get(key, "") or "").strip()
        if not raw:
            return {}
        payload = json.loads(raw)
        return payload if isinstance(payload, dict) else {}
    except Exception as e:
        warn("health.meta_get.decode", e, key=key)
        return {}
