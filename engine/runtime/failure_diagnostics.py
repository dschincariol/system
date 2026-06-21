from __future__ import annotations

import logging
import os
import re
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import Any, Dict, Optional

from engine.runtime.logging import flush_logging_handlers, get_logger


LOG = get_logger("runtime.failure_diagnostics")
_JSON_SAFE_MAX_DEPTH = 4
_JSON_SAFE_MAX_ITEMS = 20
_JSON_SAFE_MAX_STRING = 512
_JSON_SAFE_MAX_FALLBACK = 1024


def _now_ms() -> int:
    return int(time.time() * 1000)


def _env_truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _failure_event_persistence_enabled() -> bool:
    raw = os.environ.get("TRADING_FAILURE_DIAGNOSTICS_PERSIST")
    if raw is None:
        return True
    return _env_truthy(raw)


def _log_internal_nonfatal(event: str, error: BaseException | None = None, **extra: Any) -> None:
    payload = {
        "event": str(event),
        "component": "engine.runtime.failure_diagnostics",
        "error_type": (type(error).__name__ if error is not None else ""),
        "error_message": (_safe_text(error, limit=1024) if error is not None else ""),
        "extra": _json_safe(dict(extra or {})),
        "ts_ms": _now_ms(),
    }
    exc_info = None
    if error is not None:
        exc_info = (type(error), error, error.__traceback__)
    try:
        LOG.log(
            logging.WARNING,
            str(event),
            exc_info=exc_info,
            extra={
                "event": str(event),
                "component": "engine.runtime.failure_diagnostics",
                "extra_json": payload,
            },
        )
    except Exception as e:
        logging.log(
            logging.WARNING,
            "failure_diagnostics_internal_log_failed event=%s error=%s",
            str(event),
            f"{type(e).__name__}: {e}",
        )
        return


def normalize_root_cause_code(value: str, *, fallback: str = "RUNTIME_FAILURE") -> str:
    text = re.sub(r"[^A-Za-z0-9]+", "_", str(value or "").strip()).strip("_").upper()
    return text or str(fallback)


def _safe_text(value: Any, *, limit: int = 12000) -> str:
    try:
        text = str(value)
    except Exception:
        try:
            text = repr(value)
        except Exception:
            text = "<unprintable>"
    if len(text) > int(limit):
        marker = "...[truncated]"
        max_chars = max(0, int(limit) - len(marker))
        return f"{text[:max_chars]}{marker}"
    return text


def _json_safe(value: Any, *, depth: int = 0) -> Any:
    if depth >= _JSON_SAFE_MAX_DEPTH:
        return _safe_text(value, limit=_JSON_SAFE_MAX_STRING)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return _safe_text(value, limit=_JSON_SAFE_MAX_STRING)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, BaseException):
        return f"{type(value).__name__}: {_safe_text(value, limit=_JSON_SAFE_MAX_STRING)}"
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for key, item in list(value.items())[:_JSON_SAFE_MAX_ITEMS]:
            out[_safe_text(key, limit=128)] = _json_safe(item, depth=depth + 1)
        return out
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item, depth=depth + 1) for item in list(value)[:_JSON_SAFE_MAX_ITEMS]]
    return _safe_text(value, limit=_JSON_SAFE_MAX_FALLBACK)


def _redact_diagnostic_payload(value: Any) -> Any:
    try:
        from engine.api.redaction import redact_api_payload

        return redact_api_payload(value)
    except Exception as e:
        _log_internal_nonfatal("failure_diagnostics_redaction_failed", e)
        return value


def _ctx_summary(ctx: Any) -> Dict[str, Any]:
    if not isinstance(ctx, dict):
        return {}
    return {
        "has_jobs": bool(ctx.get("JOBS")),
        "has_supervisor": bool(ctx.get("SUPERVISOR")),
        "has_api_handlers": bool(ctx.get("API_HANDLERS")),
        "keys": sorted(str(key) for key in list(ctx.keys())[:20]),
    }


def _safe_process_int(fn: Any, *, default: int = 0, label: str = "") -> int:
    try:
        return int(fn() or default)
    except Exception as e:
        _log_internal_nonfatal(
            "failure_diagnostics_process_int_failed",
            e,
            label=str(label or ""),
        )
        return int(default)


def _safe_process_text(fn: Any, *, default: str = "", label: str = "", limit: int = 2048) -> str:
    try:
        return _safe_text(fn(), limit=limit)
    except Exception as e:
        _log_internal_nonfatal(
            "failure_diagnostics_process_text_failed",
            e,
            label=str(label or ""),
        )
        return str(default)


def _safe_process_argv() -> list[str]:
    try:
        return [_safe_text(arg, limit=512) for arg in list(sys.argv)[:20]]
    except Exception as e:
        _log_internal_nonfatal("failure_diagnostics_process_argv_failed", e)
        return []


def _health_summary() -> Dict[str, Any]:
    try:
        from engine.runtime.health import get_health_snapshot

        health = dict(get_health_snapshot() or {})
        return {
            "ok": bool(health.get("ok")),
            "reasons": _json_safe(list(health.get("reasons") or [])[:20]),
            "db": _json_safe(health.get("db") or {}),
            "prices": _json_safe(health.get("prices") or {}),
            "providers": _json_safe(health.get("providers") or {}),
            "job_summary": _json_safe(health.get("job_summary") or {}),
            "execution_barrier": _json_safe(health.get("execution_barrier") or {}),
            "broker_connection": _json_safe(health.get("broker_connection") or {}),
            "predictions": _json_safe(health.get("predictions") or {}),
        }
    except Exception as e:
        _log_internal_nonfatal("failure_diagnostics_health_summary_failed", e)
        return {
            "ok": False,
            "error": f"{type(e).__name__}: {_safe_text(e, limit=1024)}",
        }


def _storage_summary(*, include_quick_check: bool = True) -> Dict[str, Any]:
    summary: Dict[str, Any] = {}
    try:
        from engine.runtime.storage import DB_PATH, get_db_debug_snapshot

        db_path = Path(DB_PATH)
        summary["db_path"] = str(db_path)
        summary["db_exists"] = bool(db_path.exists())
        debug = dict(get_db_debug_snapshot(include_quick_check=include_quick_check) or {})
        startup_trace = dict(debug.get("startup_trace") or {})
        import_smoke = dict(debug.get("import_smoke") or {})
        summary.update(
            {
                "db_bytes": int(debug.get("db_bytes") or 0),
                "wal_bytes": int(debug.get("wal_bytes") or 0),
                "shm_bytes": int(debug.get("shm_bytes") or 0),
                "reader_count": int(debug.get("reader_count") or 0),
                "writer_count": int(debug.get("writer_count") or 0),
                "long_lived_reader_count": len(debug.get("long_lived_readers") or []),
                "db_validation": _json_safe(debug.get("db_validation") or {}),
                "failure_classification": _json_safe(debug.get("failure_classification") or {}),
                "ingestion_state": _json_safe(debug.get("ingestion_state") or {}),
                "supervisor_analysis": _json_safe(debug.get("supervisor_analysis") or {}),
                "startup_trace": {
                    "phase": str(startup_trace.get("phase") or ""),
                    "first_failure": _json_safe(startup_trace.get("first_failure") or {}),
                    "import_errors": _json_safe(list(startup_trace.get("import_errors") or [])[:20]),
                    "ts_ms": int(startup_trace.get("ts_ms") or 0),
                },
                "import_smoke": {
                    "ok": bool(import_smoke.get("ok", True)),
                    "failures": _json_safe(list(import_smoke.get("failures") or [])[:20]),
                    "ts_ms": int(import_smoke.get("ts_ms") or 0),
                },
            }
        )
    except Exception as e:
        summary["storage_error"] = f"{type(e).__name__}: {_safe_text(e, limit=1024)}"
    return summary


def capture_system_state_snapshot(
    *,
    ctx: Any = None,
    include_health: bool = False,
    include_quick_check: bool = True,
) -> Dict[str, Any]:
    snapshot: Dict[str, Any] = {
        "ts_ms": _now_ms(),
        "process": {
            "pid": _safe_process_int(os.getpid, label="pid"),
            "ppid": _safe_process_int(getattr(os, "getppid", lambda: 0), label="ppid"),
            "thread_name": _safe_process_text(lambda: threading.current_thread().name or "", label="thread_name", limit=256),
            "cwd": _safe_process_text(os.getcwd, label="cwd", limit=1024),
            "python": _safe_process_text(lambda: sys.executable or "", label="python", limit=1024),
            "argv": _safe_process_argv(),
        },
        "runtime": {
            "engine_mode": str(os.environ.get("ENGINE_MODE") or ""),
            "job_name": str(os.environ.get("ENGINE_JOB_NAME") or ""),
            "supervised": str(os.environ.get("ENGINE_SUPERVISED") or ""),
            "launched_by_supervisor": str(os.environ.get("ENGINE_LAUNCHED_BY_SUPERVISOR") or ""),
            "service_name": str(os.environ.get("ENGINE_SERVICE_NAME") or ""),
        },
        "paths": {
            "db_path": str(os.environ.get("DB_PATH") or ""),
            "trading_logs": str(os.environ.get("TRADING_LOGS") or os.environ.get("LOG_DIR") or ""),
            "trading_data": str(os.environ.get("TRADING_DATA") or os.environ.get("DATA_DIR") or ""),
        },
        "context": _ctx_summary(ctx),
        "storage": _storage_summary(include_quick_check=include_quick_check),
    }

    try:
        from engine.runtime.lifecycle import snapshot as lifecycle_snapshot

        snapshot["lifecycle"] = dict(lifecycle_snapshot() or {})
    except Exception as e:
        snapshot["lifecycle_error"] = f"{type(e).__name__}: {_safe_text(e, limit=1024)}"

    if include_health:
        snapshot["health"] = _health_summary()

    return snapshot


def _append_failure_event(payload: Dict[str, Any]) -> None:
    try:
        from engine.runtime.event_log import append_event

        append_event(
            event_type="runtime_failure",
            event_source=str(payload.get("failure_scope") or "runtime.failure"),
            entity_type="failure",
            entity_id=str(payload.get("root_cause_code") or ""),
            payload=_json_safe(payload),
            ts_ms=int(payload.get("ts_ms") or _now_ms()),
        )
    except Exception as e:
        _log_internal_nonfatal("failure_diagnostics_append_failure_event_failed", e)
        return


def build_failure_payload(
    *,
    code: str,
    message: str,
    error: Optional[BaseException] = None,
    scope: str,
    extra: Optional[Dict[str, Any]] = None,
    ctx: Any = None,
    include_health: bool = False,
    include_quick_check: bool = True,
) -> Dict[str, Any]:
    root_cause_code = normalize_root_cause_code(code or scope)
    payload: Dict[str, Any] = {
        "ts_ms": _now_ms(),
        "root_cause_code": root_cause_code,
        "failure_scope": str(scope or ""),
        "error_message": _safe_text(message or error or root_cause_code, limit=4000),
        "error_type": type(error).__name__ if error is not None else "",
        "extra": _json_safe(extra or {}),
        "system_state_snapshot": capture_system_state_snapshot(
            ctx=ctx,
            include_health=include_health,
            include_quick_check=include_quick_check,
        ),
    }
    if error is not None:
        payload["traceback"] = _safe_text(
            "".join(traceback.format_exception(type(error), error, error.__traceback__)),
            limit=16000,
        )
    return _redact_diagnostic_payload(payload)


def log_failure(
    logger: Optional[logging.Logger],
    *,
    event: str,
    code: str,
    message: str,
    error: Optional[BaseException] = None,
    level: int = logging.ERROR,
    component: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
    ctx: Any = None,
    include_health: bool = False,
    include_quick_check: bool = True,
    persist: bool = True,
    flush: bool = False,
) -> Dict[str, Any]:
    logger = logger or LOG
    try:
        payload = build_failure_payload(
            code=code,
            message=message,
            error=error,
            scope=event,
            extra=extra,
            ctx=ctx,
            include_health=include_health,
            include_quick_check=include_quick_check,
        )
    except Exception as build_error:
        _log_internal_nonfatal(
            "failure_diagnostics_build_failure_payload_failed",
            build_error,
            event=str(event),
            code=str(code),
        )
        payload = {
            "ts_ms": _now_ms(),
            "root_cause_code": normalize_root_cause_code(code or event),
            "failure_scope": str(event or ""),
            "error_message": _safe_text(message or error or code, limit=4000),
            "error_type": type(error).__name__ if error is not None else "",
            "extra": _json_safe(extra or {}),
            "system_state_snapshot": {
                "ts_ms": _now_ms(),
                "process": {
                    "pid": _safe_process_int(os.getpid, label="pid_fallback"),
                    "ppid": _safe_process_int(getattr(os, "getppid", lambda: 0), label="ppid_fallback"),
                },
                "diagnostics_error": f"{type(build_error).__name__}: {_safe_text(build_error, limit=1024)}",
            },
        }
        payload = _redact_diagnostic_payload(payload)
    exc_info = None
    if error is not None and _env_truthy(os.environ.get("TRADING_FAILURE_DIAGNOSTICS_RAW_EXC_INFO")):
        exc_info = (type(error), error, error.__traceback__)
    try:
        logger.log(
            int(level),
            str(event),
            exc_info=exc_info,
            extra={
                "event": str(event),
                "component": str(component or logger.name),
                "extra_json": _json_safe(payload),
            },
        )
    except Exception as log_error:
        _log_internal_nonfatal(
            "failure_diagnostics_logger_log_failed",
            log_error,
            event=str(event),
            code=str(code),
        )
    if persist and _failure_event_persistence_enabled():
        try:
            _append_failure_event(payload)
        except Exception as persist_error:
            _log_internal_nonfatal(
                "failure_diagnostics_append_failure_event_dispatch_failed",
                persist_error,
                event=str(event),
                code=str(code),
            )
    if flush:
        try:
            flush_logging_handlers()
        except Exception as e:
            _log_internal_nonfatal("failure_diagnostics_flush_logging_handlers_failed", e)
    return payload


def failure_response(
    logger: Optional[logging.Logger],
    *,
    event: str,
    code: str,
    message: str,
    error: Optional[BaseException] = None,
    component: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
    ctx: Any = None,
    include_health: bool = False,
    include_quick_check: bool = True,
    log_level: int = logging.ERROR,
    persist: bool = True,
) -> Dict[str, Any]:
    payload = log_failure(
        logger,
        event=event,
        code=code,
        message=message,
        error=error,
        level=log_level,
        component=component,
        extra=extra,
        ctx=ctx,
        include_health=include_health,
        include_quick_check=include_quick_check,
        persist=persist,
    )
    return {
        "ok": False,
        "error": str(payload.get("error_message") or message),
        "error_code": str(payload.get("root_cause_code") or normalize_root_cause_code(code or event)),
        "root_cause_code": str(payload.get("root_cause_code") or normalize_root_cause_code(code or event)),
        "failure_scope": str(payload.get("failure_scope") or event),
        "failure_type": str(payload.get("error_type") or ""),
        "system_state_snapshot": dict(payload.get("system_state_snapshot") or {}),
    }
