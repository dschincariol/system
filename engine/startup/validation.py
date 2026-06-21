"""Startup validation payload and logging helpers."""

import re
import time
from typing import Any, Callable, Dict, Optional

SENSITIVE_LOG_KEYS = (
    "api_key",
    "apikey",
    "authorization",
    "credential",
    "dsn",
    "password",
    "secret",
    "token",
)


def startup_validation_summary(snapshot: Optional[Dict[str, Any]], *, now_ms: Optional[int] = None) -> Dict[str, Any]:
    """Normalize a health snapshot's startup-validation section."""
    snap = dict(snapshot or {})
    gates = dict(snap.get("gates") or snap.get("checks") or {})
    blocking_gates = list(snap.get("blocking_gates") or snap.get("blocking_checks") or [])
    return {
        "ok": bool(snap.get("ok")),
        "mode": str(snap.get("mode") or ""),
        "blocking_checks": list(blocking_gates),
        "blocking_gates": list(blocking_gates),
        "critical_systems_missing": list(snap.get("critical_systems_missing") or []),
        "reasons": list(snap.get("reasons") or []),
        "health_reasons": list(snap.get("health_reasons") or []),
        "checks": gates,
        "gates": gates,
        "db_validation": dict(snap.get("db_validation") or {}),
        "ts_ms": int(snap.get("ts_ms") or (int(time.time() * 1000) if now_ms is None else int(now_ms))),
    }


def redact_log_string(value: str) -> str:
    """Redact connection strings and secret-shaped key/value material."""
    text = str(value)
    text = re.sub(r"(?i)(password\s*=\s*)[^\s,;}\"]+", r"\1<redacted>", text)
    text = re.sub(r"(?i)(://[^:/@\s]+:)[^@/\s]+@", r"\1<redacted>@", text)
    text = re.sub(r"(?i)((?:api[_-]?key|token|secret|password)\s*[=:]\s*)[^&\s,;}\"]+", r"\1<redacted>", text)
    return text


def redact_for_log(value: Any, *, key: str = "") -> Any:
    """Recursively redact sensitive startup validation payload values."""
    key_l = str(key or "").strip().lower()
    sensitive_key = any(marker in key_l for marker in SENSITIVE_LOG_KEYS)
    if isinstance(value, dict):
        return {str(k): redact_for_log(v, key=str(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [redact_for_log(v, key=key) for v in value]
    if isinstance(value, tuple):
        return [redact_for_log(v, key=key) for v in value]
    if sensitive_key and value not in (None, "", False, True) and not isinstance(value, (int, float)):
        return "<redacted>"
    if isinstance(value, str):
        return redact_log_string(value)
    return value


def persist_startup_validation(
    trace: Dict[str, Any],
    snapshot: Optional[Dict[str, Any]],
    *,
    stage: str,
    attempt: int,
    timeout_s: float,
    persist_startup_trace: Callable[[], None],
    meta_set_json: Callable[[str, Any], None],
    now_ms: Optional[int] = None,
) -> Dict[str, Any]:
    """Persist normalized startup-validation payload into trace and runtime meta."""
    payload = startup_validation_summary(snapshot, now_ms=now_ms)
    payload["stage"] = str(stage)
    payload["attempt"] = int(attempt)
    payload["timeout_s"] = float(timeout_s)
    trace["startup_health_validation"] = payload
    persist_startup_trace()
    meta_set_json("startup_health_validation", payload)
    return payload


def validation_gate_payload(checks: list[str], failures: list[Dict[str, Any]], *, now_ms: Optional[int] = None) -> Dict[str, Any]:
    """Build the production validation gate trace payload."""
    return {
        "ok": len(failures) == 0,
        "checks": checks,
        "failures": failures,
        "ts_ms": int(time.time() * 1000) if now_ms is None else int(now_ms),
    }
