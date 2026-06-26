"""
FILE: api_operator_handlers.py

HTTP/API handlers for operator handlers endpoints.
"""

# engine/api/api_operator_handlers.py
# Operator console APIs extracted from dashboard_server.py

import json
import logging
import os
import threading
import time
from typing import Any, Callable, Dict, Optional, cast

from engine.api.http_parsing import qs as _qs
from engine.api.log_filters import coerce_int, ensure_lines, filter_lines, lines_to_text, normalize_level
from engine.runtime.failure_diagnostics import failure_response, log_failure
from engine.runtime.startup_orchestrator import StartupOrchestrator


LOG = logging.getLogger(__name__)
JsonDict = Dict[str, Any]
ApiHandler = Callable[..., Any]


def _ctx_dict(ctx: Any) -> JsonDict:
    return dict(ctx) if isinstance(ctx, dict) else {}


def _dict_or_empty(value: Any) -> JsonDict:
    return dict(value) if isinstance(value, dict) else {}


def _str_lines(value: Any) -> list[str]:
    return str(value or "").splitlines()


def _str_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    return []


def _truthy_confirmation_value(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "ack", "confirmed"}


def _api_handler(ctx: JsonDict, name: str) -> Optional[ApiHandler]:
    handlers = (_ctx_dict(ctx).get("API_HANDLERS") or {})
    fn = handlers.get(name)
    return cast(Optional[ApiHandler], fn if callable(fn) else None)


def _dedupe_text(values: Any) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    if isinstance(values, (list, tuple, set)):
        candidates = list(values)
    else:
        candidates = [values]
    for value in candidates:
        text = str(value or "").strip()
        if text and text not in seen:
            out.append(text)
            seen.add(text)
    return out


def _institutional_reason_from_item(item: Any) -> str:
    if isinstance(item, dict):
        for key in ("code", "reason_code", "reason", "error", "root_cause_code", "message", "detail"):
            text = str(item.get(key) or "").strip()
            if text:
                return text
        return ""
    return str(item or "").strip()


def _institutional_issue_is_failing(item: Any) -> bool:
    if not isinstance(item, dict):
        return True
    if item.get("ok") is True or item.get("pass") is True:
        return False
    level = str(item.get("level") or item.get("severity") or "").strip().lower()
    if level in {"info", "ok", "pass", "passed", "success"}:
        return False
    status = str(item.get("status") or item.get("state") or "").strip().lower()
    if status in {"ok", "pass", "passed", "healthy", "success"}:
        return False
    if level in {"warn", "warning", "error", "fail", "failed", "critical"}:
        return True
    if status in {"warn", "warning", "error", "fail", "failed", "blocked", "degraded", "unavailable"}:
        return True
    return not bool(item.get("passed") is True or item.get("healthy") is True)


def _institutional_reason_text_is_failing(value: str) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return False
    passing_tokens = {
        "config_valid",
        "startup_complete",
        "database_reachable",
        "schema_valid",
        "ingestion_active",
        "ingestion_not_stale",
    }
    if text in passing_tokens:
        return False
    if text.endswith(("_valid", "_complete", "_reachable", "_active", "_not_stale")):
        return False
    return True


def _institutional_payload_reasons(payload: JsonDict, *, fallback: str) -> list[str]:
    reasons: list[str] = []

    for key in ("reason_code", "reason", "root_cause_code", "error"):
        text = str(payload.get(key) or "").strip()
        if text:
            reasons.append(text)

    for key in ("reason_codes", "reasons", "errors", "critical_blockers", "current_degraded_reasons"):
        value = payload.get(key)
        if isinstance(value, (list, tuple, set)):
            reasons.extend(_institutional_reason_from_item(item) for item in value)
        elif value is not None:
            reasons.append(_institutional_reason_from_item(value))

    production_validation = _dict_or_empty(payload.get("production_validation"))
    for key in ("summary_reason", "status"):
        text = str(production_validation.get(key) or "").strip()
        if text and text.lower() not in {"healthy", "ok", "pass", "passed"}:
            reasons.append(text)
    for key in ("current_degraded_reasons", "critical_failures"):
        value = production_validation.get(key)
        if isinstance(value, (list, tuple, set)):
            reasons.extend(_institutional_reason_from_item(item) for item in value)

    readiness = _dict_or_empty(payload.get("readiness"))
    issues = readiness.get("issues") if readiness else payload.get("issues")
    if isinstance(issues, (list, tuple, set)):
        reasons.extend(_institutional_reason_from_item(item) for item in issues if _institutional_issue_is_failing(item))

    status = str(payload.get("status") or payload.get("state") or "").strip()
    if status and status.lower() not in {"healthy", "ok", "pass", "passed", "running", "live"}:
        reasons.append(status)

    deduped = [reason for reason in _dedupe_text(reasons) if _institutional_reason_text_is_failing(reason)]
    return deduped or [fallback]


def _institutional_check_status(payload: JsonDict, *, ok: bool) -> str:
    if ok:
        return "pass"

    status = str(payload.get("status") or payload.get("state") or "").strip().lower()
    if bool(payload.get("failed")) or status in {"failed", "fail", "error", "blocked", "critical"}:
        return "fail"
    if bool(payload.get("degraded")) or status in {"degraded", "starting", "warming_up", "warn", "warning"}:
        return "degraded"
    return "fail"


def _institutional_check(name: str, payload: JsonDict, *, ok: bool, fallback: str) -> JsonDict:
    status = _institutional_check_status(payload, ok=ok)
    reasons = [] if ok else _institutional_payload_reasons(payload, fallback=fallback)
    check: JsonDict = {
        "name": name,
        "ok": bool(ok),
        "status": status,
        "level": "info" if ok else ("warn" if status == "degraded" else "error"),
        "reasons": reasons,
        "reason": reasons[0] if reasons else None,
        "summary": "pass" if ok else f"{name}:{reasons[0] if reasons else fallback}",
        "details": {
            "status": payload.get("status"),
            "state": payload.get("state"),
            "ready": payload.get("ready"),
            "degraded": payload.get("degraded"),
            "failed": payload.get("failed"),
            "safe_to_operate": payload.get("safe_to_operate"),
        },
    }
    return check


def _institutional_internal_failure(sub_check: str, error: BaseException, ctx: JsonDict) -> JsonDict:
    message = f"Institutional check {sub_check} sub-check raised {type(error).__name__}."
    out = failure_response(
        LOG,
        event="api_operator_handlers_institutional_check_subcheck_failed",
        code="API_OPERATOR_HANDLERS_INSTITUTIONAL_CHECK_SUBCHECK_FAILED",
        message=message,
        error=error,
        component="engine.api.api_operator_handlers",
        ctx=ctx,
        extra={"sub_check": str(sub_check or "unknown")},
    )
    out["sub_check"] = str(sub_check or "unknown")
    out["message"] = message
    out["meta"] = {"status": 500}
    return out


def _parse_mode(body, default="safe"):
    try:
        if isinstance(body, dict):
            value = str(body.get("mode") or default).strip()
            return value or default
    except Exception as e:
        log_failure(
            LOG,
            event="api_operator_handlers_parse_mode_failed",
            code="API_OPERATOR_HANDLERS_PARSE_MODE_FAILED",
            message=str(e),
            error=e,
            level=logging.WARNING,
            component="engine.api.api_operator_handlers",
            include_health=False,
            persist=True,
        )
    return default


def _non_live_operator_mode(body, *, default: str = "safe") -> tuple[str, Optional[JsonDict]]:
    mode = str(_parse_mode(body, default=default) or default).strip().lower() or default
    if mode in {"live", "trading"}:
        return mode, {
            "ok": False,
            "mode": mode,
            "error": "live_mode_not_supported",
            "reason": "dashboard operator start/bootstrap routes are non-live only",
            "meta": {"status": 403},
        }
    return mode, None


def _operator_timeout_s(ctx: JsonDict, action: str, fallback: float) -> float:
    action_key = str(action or "").strip().upper()
    names = (
        f"OPERATOR_{action_key}_REQUEST_TIMEOUT_S",
        "OPERATOR_ORCHESTRATION_REQUEST_TIMEOUT_S",
    )
    raw = None
    for name in names:
        if isinstance(ctx, dict) and ctx.get(name) is not None:
            raw = ctx.get(name)
            break
        if os.environ.get(name) is not None:
            raw = os.environ.get(name)
            break
    try:
        return max(0.05, min(120.0, float(raw if raw is not None else fallback)))
    except Exception:
        return float(fallback)


def _run_bounded(action: str, timeout_s: float, fn: Callable[[], Any]) -> JsonDict:
    started = time.perf_counter()
    result: dict[str, Any] = {}
    error: dict[str, BaseException | None] = {"value": None}
    done = threading.Event()

    def _runner() -> None:
        try:
            result["value"] = fn()
        except BaseException as exc:  # noqa: BLE001 - surfaced as API failure below
            error["value"] = exc
        finally:
            done.set()

    thread = threading.Thread(target=_runner, name=f"operator_{action}_request", daemon=True)
    thread.start()
    finished = done.wait(timeout=max(0.05, float(timeout_s)))
    elapsed_ms = int(round((time.perf_counter() - started) * 1000.0))

    if not finished:
        return {
            "ok": False,
            "error": f"{action}_timeout",
            "reason": f"{action} orchestration did not finish within {float(timeout_s):.1f}s",
            "timed_out": True,
            "duration_ms": elapsed_ms,
            "meta": {"status": 504},
        }

    if error["value"] is not None:
        exc = error["value"]
        return {
            "ok": False,
            "error": f"{action}_failed:{type(exc).__name__}",
            "reason": str(exc),
            "duration_ms": elapsed_ms,
            "meta": {"status": 500},
        }

    value = result.get("value")
    if isinstance(value, dict):
        out = dict(value)
        out.setdefault("duration_ms", elapsed_ms)
        return out
    return {
        "ok": False,
        "error": f"{action}_invalid_response",
        "reason": type(value).__name__,
        "duration_ms": elapsed_ms,
        "meta": {"status": 502},
    }


def _stringify_operator_detail(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float, bool)):
        return str(value)
    try:
        return json.dumps(value, default=str, sort_keys=True)
    except Exception:
        return str(value)


def _first_operator_error(values: Any) -> str:
    if isinstance(values, dict):
        for key in ("error", "reason_code", "reason", "message", "details", "detail"):
            text = _stringify_operator_detail(values.get(key))
            if text:
                return text
        return ""
    if isinstance(values, (list, tuple, set)):
        for item in values:
            text = _stringify_operator_detail(item)
            if text:
                return text
    return _stringify_operator_detail(values)


def _operator_failure_reason(payload: JsonDict) -> str:
    result = payload.get("result")
    if isinstance(result, dict):
        text = _first_operator_error(result.get("errors"))
        if text:
            return text

    text = _first_operator_error(payload.get("errors"))
    if text:
        return text

    for step in payload.get("steps") or []:
        if not isinstance(step, dict) or step.get("ok", True) is not False:
            continue
        text = _first_operator_error(step)
        if text:
            return text

    for key in ("reason", "reason_code", "error"):
        text = _stringify_operator_detail(payload.get(key))
        if text and text != "request_failed":
            return text
    return ""


def _operator_failure_code(action: str, reason: str) -> str:
    text = str(reason or "").strip()
    if text.startswith("start_failed:"):
        parts = text.split(":")
        if len(parts) >= 2:
            return ":".join(parts[:2])
    if text.startswith("bootstrap_failed:"):
        parts = text.split(":")
        if len(parts) >= 2:
            return ":".join(parts[:2])
    if "preflight" in text.lower():
        return "preflight_failed"
    return f"{str(action or 'operator').strip() or 'operator'}_failed"


def _finalize_operator_orchestration(payload: JsonDict, *, action: str, failure_status: int = 503) -> JsonDict:
    out = dict(payload or {})
    if out.get("ok") is not False:
        return out

    reason = _operator_failure_reason(out) or f"{action}_failed"
    error = str(out.get("error") or "").strip()
    if not error or error == "request_failed" or error == reason:
        error = _operator_failure_code(action, reason)
    elif error.startswith("start_failed:"):
        error = _operator_failure_code(action, error)

    out["error"] = error
    out.setdefault("reason", reason)
    meta = dict(out.get("meta") or {})
    if "status" not in meta and "http_status" not in meta:
        meta["status"] = 422 if error == "preflight_failed" else int(failure_status)
    out["meta"] = meta
    return out


def _attach_execution_barrier(out: JsonDict, ctx: JsonDict) -> JsonDict:
    payload = dict(out or {})
    barrier: JsonDict = {}
    handler = _api_handler(ctx, "api_get_execution_barrier")
    if callable(handler):
        try:
            snap = _dict_or_empty(handler(None, ctx))
            raw_barrier = snap.get("execution_barrier") if isinstance(snap.get("execution_barrier"), dict) else snap
            barrier = _dict_or_empty(raw_barrier)
        except Exception as e:
            barrier = {
                "ok": False,
                "allowed": False,
                "real_trading_allowed": False,
                "reason": f"execution_barrier_unavailable:{type(e).__name__}",
            }
    payload.setdefault("execution_barrier", barrier)
    payload["real_trading_allowed"] = bool(barrier.get("real_trading_allowed")) if barrier else False
    return payload


# ------------------------------------------------------
# OPERATOR STATUS
# ------------------------------------------------------

def api_get_operator_status(_parsed=None, ctx=None):
    ctx = _ctx_dict(ctx)
    fn = ctx.get("_operator_status_payload")
    boot_fn = ctx.get("_boot_diagnostics")
    status_handler = _api_handler(ctx, "api_get_status")
    # Operator status is layered on top of the base system status so UI clients
    # get one merged payload instead of stitching it together themselves.
    base = _dict_or_empty(status_handler(None, ctx)) if callable(status_handler) else {"ok": False, "error": "status_unavailable"}
    try:
        boot = _dict_or_empty(boot_fn()) if callable(boot_fn) else {}
    except Exception:
        boot = {}
    storage = _dict_or_empty(boot.get("storage"))

    if callable(fn):
        operator_status = _dict_or_empty(fn())
        return {
            **base,
            "ok": bool(operator_status.get("ok")) and bool(base.get("ok")),
            "operator_status": operator_status,
            "storage": storage or _dict_or_empty(base.get("storage")),
            "boot_diagnostics": {"storage": storage} if storage else {},
        }
    return {
        **base,
        "ok": False,
        "operator_status": {"ok": False, "error": "operator_status_unavailable"},
        "storage": storage or _dict_or_empty(base.get("storage")),
        "boot_diagnostics": {"storage": storage} if storage else {},
        "error": "operator_status_unavailable",
    }


def api_get_operator_bootstrap_status(_parsed=None, ctx=None):
    ctx = _ctx_dict(ctx)
    preflight_fn = ctx.get("_operator_preflight_steps")
    status_fn = ctx.get("_operator_status_payload")
    boot_fn = ctx.get("_boot_diagnostics")
    status_handler = _api_handler(ctx, "api_get_status")
    base = _dict_or_empty(status_handler(None, ctx)) if callable(status_handler) else {"ok": False, "error": "status_unavailable"}

    try:
        pre = _dict_or_empty(preflight_fn()) if callable(preflight_fn) else {"ok": False}
    except Exception as e:
        pre = _dict_or_empty(failure_response(
            LOG,
            event="api_operator_handlers_bootstrap_preflight_failed",
            code="API_OPERATOR_HANDLERS_BOOTSTRAP_PREFLIGHT_FAILED",
            message=str(e),
            error=e,
            component="engine.api.api_operator_handlers",
            ctx=ctx,
        ))

    try:
        operator_status = _dict_or_empty(status_fn()) if callable(status_fn) else {}
    except Exception as e:
        operator_status = _dict_or_empty(failure_response(
            LOG,
            event="api_operator_handlers_bootstrap_status_failed",
            code="API_OPERATOR_HANDLERS_BOOTSTRAP_STATUS_FAILED",
            message=str(e),
            error=e,
            component="engine.api.api_operator_handlers",
            ctx=ctx,
        ))
    try:
        boot = _dict_or_empty(boot_fn()) if callable(boot_fn) else {}
    except Exception as e:
        boot = _dict_or_empty(failure_response(
            LOG,
            event="api_operator_handlers_bootstrap_diagnostics_failed",
            code="API_OPERATOR_HANDLERS_BOOTSTRAP_DIAGNOSTICS_FAILED",
            message=str(e),
            error=e,
            component="engine.api.api_operator_handlers",
            ctx=ctx,
        ))
    execution_mode = str(base.get("execution_mode") or base.get("mode") or "unknown")

    return {
        **base,
        "ok": bool(pre.get("ok")) and bool(base.get("ok")),
        "preflight": pre,
        "operator_status": operator_status,
        "boot_diagnostics": boot,
        "engine_mode": execution_mode,
        "ts_ms": int(time.time() * 1000),
    }


def api_get_operator_preflight(parsed, ctx=None):
    ctx = _ctx_dict(ctx)
    preflight_fn = ctx.get("_operator_preflight_steps")
    if callable(preflight_fn):
        try:
            return preflight_fn()
        except Exception as e:
            return failure_response(
                LOG,
                event="api_operator_handlers_preflight_failed",
                code="API_OPERATOR_HANDLERS_PREFLIGHT_FAILED",
                message=str(e),
                error=e,
                component="engine.api.api_operator_handlers",
                ctx=ctx,
            )
    return {"ok": False, "error": "preflight_unavailable"}


# ------------------------------------------------------
# OPERATOR START / STOP
# ------------------------------------------------------

def api_post_operator_start(parsed, body=None, ctx=None):
    ctx = _ctx_dict(ctx)
    mode, blocked = _non_live_operator_mode(body)
    if blocked:
        return blocked
    fn = ctx.get("_operator_start_impl")
    if callable(fn):
        result = _run_bounded(
            "start",
            _operator_timeout_s(ctx, "START", 15.0),
            lambda: fn(mode),
        )
        result = _finalize_operator_orchestration(result, action="start", failure_status=503)
        return _attach_execution_barrier(result, ctx)
    return {"ok": False, "error": "operator_start_unavailable"}


def api_post_operator_bootstrap(parsed, body=None, ctx=None):
    ctx = _ctx_dict(ctx)
    mode, blocked = _non_live_operator_mode(body)
    if blocked:
        return blocked
    jobs = ctx.get("JOBS")
    supervisor = ctx.get("SUPERVISOR")
    health_handler = _api_handler(ctx, "api_get_health")

    if jobs is None or supervisor is None:
        return {"ok": False, "error": "operator_bootstrap_unavailable"}
    if health_handler is None:
        return {"ok": False, "error": "health_handler_unavailable"}

    try:
        orchestrator = StartupOrchestrator(
            jobs=jobs,
            supervisor=supervisor,
            health_fn=lambda: _dict_or_empty(health_handler(None, ctx)),
        )
        result = _run_bounded(
            "bootstrap",
            _operator_timeout_s(ctx, "BOOTSTRAP", 25.0),
            lambda: orchestrator.run(mode),
        )
        result = _finalize_operator_orchestration(result, action="bootstrap", failure_status=503)
        return _attach_execution_barrier(result, ctx)
    except Exception as e:
        return failure_response(
            LOG,
            event="api_operator_handlers_bootstrap_failed",
            code="API_OPERATOR_HANDLERS_BOOTSTRAP_FAILED",
            message=str(e),
            error=e,
            component="engine.api.api_operator_handlers",
            ctx=ctx,
        )


def api_post_operator_stop(_parsed=None, _body=None, ctx=None):
    ctx = _ctx_dict(ctx)
    jobs = ctx.get("JOBS")
    if jobs is None:
        return {"ok": False, "error": "jobs_manager_unavailable", "stopped": [], "errors": []}

    stopped = []
    errors = []

    try:
        rows = jobs.list_jobs()
    except Exception as e:
        log_failure(
            LOG,
            event="api_operator_handlers_stop_list_jobs_failed",
            code="API_OPERATOR_HANDLERS_STOP_LIST_JOBS_FAILED",
            message=str(e),
            error=e,
            level=logging.WARNING,
            component="engine.api.api_operator_handlers",
            ctx=ctx,
            persist=True,
        )
        rows = []

    # Stop is best-effort across all known jobs so a partial failure still
    # returns a detailed picture of what was actually stopped.
    for row in rows:
        name = str(row.get("name") or "")
        if not name:
            continue
        try:
            jobs.stop(name)
            stopped.append(name)
        except Exception as e:
            errors.append({"job": name, "error": str(e)})

    return {
        "ok": len(errors) == 0,
        "stopped": stopped,
        "errors": errors,
    }


def api_post_operator_restart(parsed, body=None, ctx=None):
    stop = _dict_or_empty(api_post_operator_stop(parsed, body, ctx))
    start = _dict_or_empty(api_post_operator_start(parsed, body, ctx))

    return {
        "ok": bool(stop.get("ok")) and bool(start.get("ok")),
        "stop": stop,
        "start": start,
    }


# ------------------------------------------------------
# OPERATOR LOGS
# ------------------------------------------------------

def api_get_operator_logs(_parsed=None, ctx=None):
    ctx = _ctx_dict(ctx)
    fn = ctx.get("_tail_text_file")
    path = ctx.get("_OPERATOR_LOG_PATH")
    q = _qs(_parsed)
    level = normalize_level(q.get("level") or "")
    needle = str(q.get("q") or "").strip()
    limit = coerce_int(q.get("limit") or "0", 0, 0, 4000)

    if callable(fn) and path:
        try:
            text = str(fn(path) or "")
            raw_lines = ensure_lines(text)
            filtered_lines = filter_lines(raw_lines, level=level, query=needle, limit=limit)
            filtered_text = lines_to_text(filtered_lines)
            return {
                "ok": True,
                "source": "operator:runtime",
                "text": filtered_text,
                "log": filtered_text,
                "lines": filtered_lines,
                "raw_line_count": len(raw_lines),
                "filtered_line_count": len(filtered_lines),
                "applied_filters": {
                    "q": needle,
                    "level": level,
                    "limit": int(limit) if limit > 0 else None,
                },
            }
        except Exception as e:
            out = failure_response(
                LOG,
                event="api_operator_handlers_logs_failed",
                code="API_OPERATOR_HANDLERS_LOGS_FAILED",
                message=str(e),
                error=e,
                component="engine.api.api_operator_handlers",
                ctx=ctx,
                extra={"path": str(path)},
            )
            out.update({"text": "", "log": "", "lines": []})
            return out

    return {"ok": False, "error": "logs_unavailable", "text": "", "log": "", "lines": []}


def api_get_operator_stderr_tail(parsed, ctx=None):
    ctx = _ctx_dict(ctx)
    fn = ctx.get("_tail_text_file")
    path = ctx.get("_OPERATOR_STDERR_LOG_PATH")
    q = _qs(parsed)
    level = normalize_level(q.get("level") or "")
    needle = str(q.get("q") or "").strip()
    limit = coerce_int(q.get("limit") or "0", 0, 0, 4000)

    if callable(fn) and path:
        raw_lines = _str_lines(fn(path))
        filtered_lines = filter_lines(raw_lines, level=level, query=needle, limit=limit)
        filtered_text = lines_to_text(filtered_lines)
        return {
            "ok": True,
            "source": "operator:stderr",
            "lines": filtered_lines,
            "text": filtered_text,
            "log": filtered_text,
            "raw_line_count": len(raw_lines),
            "filtered_line_count": len(filtered_lines),
            "applied_filters": {
                "q": needle,
                "level": level,
                "limit": int(limit) if limit > 0 else None,
            },
        }

    return {"ok": False, "error": "stderr_unavailable", "text": "", "log": "", "lines": []}

# ------------------------------------------------------
# OPERATOR FEED CONTROL
# ------------------------------------------------------

def api_post_operator_restart_feeds(_parsed=None, _body=None, ctx=None):
    ctx = _ctx_dict(ctx)
    jobs = ctx.get("JOBS")
    supervisor = ctx.get("SUPERVISOR")
    if jobs is None or supervisor is None:
        return {
            "ok": False,
            "error": "feed_control_unavailable",
            "stopped": [],
            "started": [],
            "errors": [],
        }

    restart_guard_cleared = {"ok": False, "reason": "not_attempted"}
    try:
        from engine.runtime.ingestion_runtime import clear_child_restart_accounting

        restart_guard_cleared = clear_child_restart_accounting(reason="operator_restart_feeds")
    except Exception as e:
        restart_guard_cleared = {"ok": False, "error": str(e), "reason": "operator_restart_feeds"}
        log_failure(
            LOG,
            event="api_operator_handlers_restart_guard_clear_failed",
            code="API_OPERATOR_HANDLERS_RESTART_GUARD_CLEAR_FAILED",
            message=str(e),
            error=e,
            level=logging.WARNING,
            component="engine.api.api_operator_handlers",
            ctx=ctx,
            persist=True,
        )

    stopped = []
    errors = []

    # Feed restart explicitly targets ingestion and price-feed jobs without
    # tearing down the whole runtime.
    for name in ("ingestion_runtime",):
        try:
            jobs.stop(name)
            stopped.append(name)
        except Exception as e:
            errors.append({"job": name, "error": str(e)})

    try:
        rows = jobs.list_jobs()
    except Exception as e:
        log_failure(
            LOG,
            event="api_operator_handlers_restart_feeds_list_jobs_failed",
            code="API_OPERATOR_HANDLERS_RESTART_FEEDS_LIST_JOBS_FAILED",
            message=str(e),
            error=e,
            level=logging.WARNING,
            component="engine.api.api_operator_handlers",
            ctx=ctx,
            persist=True,
        )
        rows = []

    for row in rows:
        name = str(row.get("name") or "")
        group = str(row.get("group") or "")

        if group != "price_feed":
            continue

        try:
            jobs.stop(name)
            stopped.append(name)
        except Exception as e:
            errors.append({"job": name, "error": str(e)})

    started = []

    for name in ("ingestion_runtime", "provider_monitor"):
        try:
            result = supervisor.deterministic_start([name], include_deps=True, strict=False)
            if result.get("ok"):
                started.append(name)
        except Exception as e:
            errors.append({"job": name, "error": str(e)})

    return {
        "ok": "ingestion_runtime" in started,
        "stopped": stopped,
        "started": started,
        "errors": errors,
        "restart_guard_cleared": restart_guard_cleared,
    }


def api_post_operator_emergency_stop(_parsed=None, _body=None, ctx=None):
    ctx = _ctx_dict(ctx)
    stop = api_post_operator_stop(_parsed, _body, ctx)
    safety_errors = []
    try:
        from engine.execution.kill_switch import activate
        activate("global", "global", reason="operator_emergency_stop", actor="operator")
    except Exception as e:
        safety_errors.append(f"kill_switch_activate_failed:{type(e).__name__}:{e}")
    try:
        from engine.execution.execution_mode import set_execution_armed
        set_execution_armed(0, actor="operator", reason="operator_emergency_stop")
    except Exception as e:
        safety_errors.append(f"execution_disarm_failed:{type(e).__name__}:{e}")
    status_handler = _api_handler(ctx, "api_get_status")
    base = _dict_or_empty(status_handler(None, ctx)) if callable(status_handler) else {"ok": False, "error": "status_unavailable"}
    reasons = _str_list(base.get("reasons"))
    reasons.append("operator_emergency_stop")
    reasons.extend(safety_errors)
    return {
        **base,
        "ok": bool(stop.get("ok")) and not safety_errors,
        "status": "KILL_SWITCH",
        "execution_allowed": False,
        "reasons": reasons,
        "operator_stop": stop,
        "safety_errors": safety_errors,
    }


def api_post_operator_broker_risk(_parsed=None, body=None, ctx=None):
    """Run an explicit broker cancel/flatten command from an operator surface."""

    ctx = _ctx_dict(ctx)
    payload = body if isinstance(body, dict) else {}
    policy = str(payload.get("policy") or payload.get("action") or "").strip()
    if not policy:
        return {
            "ok": False,
            "error": "broker_risk_policy_required",
            "allowed_policies": [
                "observe_only",
                "cancel_only",
                "flatten_positions",
                "cancel_and_flatten",
            ],
        }
    actor = str(payload.get("actor") or payload.get("who") or "operator").strip() or "operator"
    reason = str(payload.get("reason") or payload.get("justification") or "operator_broker_risk").strip()
    command_id = str(payload.get("command_id") or payload.get("request_id") or "").strip() or None
    broker = str(payload.get("broker") or "").strip() or None
    engine_mode = str(payload.get("engine_mode") or payload.get("mode") or "").strip() or None
    try:
        timeout_s = float(payload.get("timeout_s") or payload.get("timeout") or 15.0)
    except Exception:
        timeout_s = 15.0

    try:
        from engine.execution.broker_shutdown_risk import handle_broker_shutdown_risk

        return handle_broker_shutdown_risk(
            policy=policy,
            broker=broker,
            engine_mode=engine_mode,
            timeout_s=timeout_s,
            command_id=command_id,
            actor=actor,
            reason=reason,
            source="engine.api.api_operator_handlers",
            require_explicit_live_policy=False,
        )
    except Exception as e:
        return failure_response(
            LOG,
            event="api_operator_handlers_broker_risk_failed",
            code="API_OPERATOR_HANDLERS_BROKER_RISK_FAILED",
            message=str(e),
            error=e,
            component="engine.api.api_operator_handlers",
            ctx=ctx,
            extra={"policy": policy, "actor": actor, "broker": broker or ""},
        )


def api_post_operator_execution_arm(_parsed=None, body=None, ctx=None):
    """Arm or disarm audited live execution through the DB execution-mode row."""

    ctx = _ctx_dict(ctx)
    payload = body if isinstance(body, dict) else {}
    requested = payload.get("armed", payload.get("arm", 1))
    arm = str(requested).strip().lower() not in {"0", "false", "no", "off", "disarm"}
    actor = str(payload.get("actor") or "operator").strip() or "operator"
    reason = str(payload.get("reason") or ("operator_live_arm" if arm else "operator_live_disarm")).strip()

    try:
        from engine.execution.execution_mode import get_execution_mode, set_execution_armed

        before = get_execution_mode()
        if arm and str(before.get("mode") or "").strip().lower() != "live":
            return {
                "ok": False,
                "error": "execution_mode_not_live",
                "execution_mode": before,
            }
        after = set_execution_armed(1 if arm else 0, actor=actor, reason=reason)
    except Exception as e:
        return failure_response(
            LOG,
            event="api_operator_handlers_execution_arm_failed",
            code="API_OPERATOR_HANDLERS_EXECUTION_ARM_FAILED",
            message=str(e),
            error=e,
            component="engine.api.api_operator_handlers",
            ctx=ctx,
            extra={"armed": int(bool(arm)), "actor": actor, "reason": reason},
        )

    barrier_handler = _api_handler(ctx, "api_get_execution_barrier")
    barrier = _dict_or_empty(barrier_handler(None, ctx)) if callable(barrier_handler) else {}
    target_armed = 1 if arm else 0
    return {
        "ok": bool(int((after or {}).get("armed") or 0) == target_armed and ((not arm) or (after or {}).get("mode") == "live")),
        "armed": int((after or {}).get("armed") or 0),
        "execution_mode": after,
        "previous_execution_mode": before,
        "execution_barrier": barrier,
    }


def api_post_operator_clear_manual_halt(_parsed=None, body=None, ctx=None):
    """Clear a non-rules kill-switch hold through an explicit operator workflow."""

    ctx = _ctx_dict(ctx)
    payload = body if isinstance(body, dict) else {}
    expected = "CLEAR_MANUAL_HALT"
    confirmation = str(payload.get("confirmation") or payload.get("confirm") or "").strip()
    actor = str(payload.get("actor") or payload.get("who") or "").strip()
    source = str(payload.get("source") or payload.get("source_surface") or "").strip()
    reason = str(payload.get("reason") or payload.get("note") or "").strip()
    missing = []
    if confirmation != expected:
        missing.append("confirmation")
    if not _truthy_confirmation_value(payload.get("consequence_ack")):
        missing.append("consequence_ack")
    if not actor:
        missing.append("actor")
    if not source:
        missing.append("source")
    if not reason:
        missing.append("reason")
    if missing:
        return {
            "ok": False,
            "error": "confirmation_required",
            "required_confirm": expected,
            "required_token": expected,
            "action_id": "operator.clear_manual_halt",
            "missing": missing,
            "meta": {"status": 422},
        }

    scope = str(payload.get("scope") or "global").strip() or "global"
    key = str(payload.get("key") or "global").strip() or "global"
    try:
        from engine.execution.kill_switch import clear_manual_halt

        result = clear_manual_halt(
            scope,
            key,
            reason=reason,
            actor=actor,
            meta={
                "source": source,
                "operator_endpoint": "api_post_operator_clear_manual_halt",
                "confirmation": expected,
            },
        )
    except Exception as e:
        return failure_response(
            LOG,
            event="api_operator_handlers_clear_manual_halt_failed",
            code="API_OPERATOR_HANDLERS_CLEAR_MANUAL_HALT_FAILED",
            message=str(e),
            error=e,
            component="engine.api.api_operator_handlers",
            ctx=ctx,
            extra={"scope": scope, "key": key, "actor": actor, "source": source},
        )

    if not bool(result.get("ok")):
        status = 403 if str(result.get("error") or "") == "manual_clear_refused_rules_owned_halt" else 409
        result = dict(result)
        result.setdefault("meta", {})
        if isinstance(result["meta"], dict):
            result["meta"].setdefault("status", status)
    return result


def api_post_operator_autofix(_parsed=None, _body=None, ctx=None):
    ctx = _ctx_dict(ctx)
    repair_handler = _api_handler(ctx, "api_post_repair_schema")
    if repair_handler is None:
        return {"ok": False, "error": "repair_handler_unavailable", "steps": []}

    repair = _dict_or_empty(
        repair_handler(
            None,
            {
                "confirmation": "REPAIR_SCHEMA",
                "confirm": "REPAIR_SCHEMA",
                "consequence_ack": True,
                "actor": "operator_autofix",
                "source": "api_post_operator_autofix",
            },
            ctx,
        )
    )
    feeds = _dict_or_empty(api_post_operator_restart_feeds(None, None, ctx))

    return {
        "ok": bool(repair.get("ok")) and bool(feeds.get("ok")),
        "steps": [
            {"step": "repair_schema", "result": repair},
            {"step": "restart_feeds", "result": feeds},
        ],
    }


def api_post_operator_clear_last_error(_parsed=None, _body=None, ctx=None):
    ctx = _ctx_dict(ctx)
    cleared = []
    errors = []

    jobs = ctx.get("JOBS")
    if jobs is not None:
        try:
            for row in (jobs.list_jobs() or []):
                name = str(row.get("name") or "")
                if not name:
                    continue
                job = jobs.get(name)
                if job is None:
                    continue
                try:
                    job.last_error = None
                    cleared.append(f"job:{name}")
                except Exception as e:
                    errors.append({"target": f"job:{name}", "error": str(e)})
        except Exception as e:
            errors.append({"target": "jobs", "error": str(e)})

    try:
        from engine.runtime.runtime_meta import meta_get, meta_set

        raw = str(meta_get("ingestion_state", "") or "").strip()
        if raw:
            state = json.loads(raw)
            if isinstance(state, dict):
                state["last_error"] = ""
                children = state.get("children")
                if isinstance(children, dict):
                    for info in children.values():
                        if isinstance(info, dict) and "last_error" in info:
                            info["last_error"] = ""
                meta_set("ingestion_state", json.dumps(state, separators=(",", ":"), sort_keys=True))
                cleared.append("runtime_meta:ingestion_state")
    except Exception as e:
        errors.append({"target": "runtime_meta:ingestion_state", "error": str(e)})

    return {
        "ok": len(errors) == 0,
        "cleared": cleared,
        "errors": errors,
    }

# ------------------------------------------------------
# OPERATOR DATA
# ------------------------------------------------------

def api_get_operator_market_data(parsed, ctx=None):
    ctx = _ctx_dict(ctx)
    symbol = "SPY"

    try:
        q = (ctx.get("qs") or (lambda _parsed: {}))(parsed)
        symbol = q.get("symbol", "SPY")
    except Exception as e:
        log_failure(
            LOG,
            event="api_operator_handlers_market_data_symbol_parse_failed",
            code="API_OPERATOR_HANDLERS_MARKET_DATA_SYMBOL_PARSE_FAILED",
            message=str(e),
            error=e,
            level=logging.WARNING,
            component="engine.api.api_operator_handlers",
            ctx=ctx,
            persist=True,
        )

    handler = _api_handler(ctx, "api_get_market_candles")

    if not handler:
        return {"ok": False, "error": "market_candles_unavailable"}

    try:
        return handler(parsed, ctx)
    except Exception as e:
        out = failure_response(
            LOG,
            event="api_operator_handlers_market_data_failed",
            code="API_OPERATOR_HANDLERS_MARKET_DATA_FAILED",
            message=str(e),
            error=e,
            component="engine.api.api_operator_handlers",
            ctx=ctx,
            extra={"symbol": str(symbol)},
        )
        out["symbol"] = symbol
        return out


def api_get_operator_strategy_decisions(_parsed=None, ctx=None):
    ctx = _ctx_dict(ctx)
    strategy_status = _api_handler(ctx, "api_get_strategy_status")
    strategy_metrics = _api_handler(ctx, "api_get_strategy_metrics")
    portfolio = _api_handler(ctx, "api_get_portfolio")
    broker = _api_handler(ctx, "api_get_broker")
    barrier = _api_handler(ctx, "api_get_execution_barrier")
    if (
        strategy_status is None
        or strategy_metrics is None
        or portfolio is None
        or broker is None
        or barrier is None
    ):
        return {"ok": False, "error": "strategy_decision_handlers_unavailable"}

    return {
        "ok": True,
        "strategy_status": strategy_status(None, ctx),
        "strategy_metrics": strategy_metrics(None, ctx),
        "portfolio": portfolio(None, ctx),
        "broker": broker(None, ctx),
        "execution_barrier": barrier(None, ctx),
    }


def api_get_operator_institutional_check(_parsed=None, ctx=None):
    ctx = _ctx_dict(ctx)
    readiness_handler = _api_handler(ctx, "api_get_readiness")
    health_handler = _api_handler(ctx, "api_get_health")
    if readiness_handler is None or health_handler is None:
        missing = []
        if readiness_handler is None:
            missing.append("api_get_readiness")
        if health_handler is None:
            missing.append("api_get_health")
        return {
            "ok": False,
            "error": "institutional_check_handlers_unavailable",
            "reason_code": "handler_resolution_failed",
            "message": "Institutional check requires dashboard readiness and health handlers.",
            "missing_handlers": missing,
            "meta": {"status": 500},
        }

    try:
        readiness = _dict_or_empty(readiness_handler(None, ctx))
    except Exception as e:
        return _institutional_internal_failure("readiness", e, ctx)

    try:
        health = _dict_or_empty(health_handler(None, ctx))
    except Exception as e:
        return _institutional_internal_failure("health", e, ctx)

    readiness_ok = bool(readiness.get("ok"))
    health_ok = bool(health.get("ok"))
    ok = readiness_ok and health_ok
    checks = [
        _institutional_check("readiness", readiness, ok=readiness_ok, fallback="readiness_not_ok"),
        _institutional_check("health", health, ok=health_ok, fallback="health_not_ok"),
    ]
    blockers = [
        {
            "name": check["name"],
            "status": check["status"],
            "reason": check["reason"],
            "reasons": list(check.get("reasons") or []),
        }
        for check in checks
        if not bool(check.get("ok"))
    ]
    reasons = [
        f"{blocker['name']}:{reason}"
        for blocker in blockers
        for reason in (blocker.get("reasons") or [blocker.get("reason")])
        if str(reason or "").strip()
    ]

    return {
        "ok": ok,
        "pass": ok,
        "configValid": readiness_ok,
        "healthOk": health_ok,
        "checks": checks,
        "blockers": blockers,
        "reasons": reasons,
        "errors": [f"{blocker['name']}: {blocker['reason']}" for blocker in blockers if blocker.get("reason")],
        "readiness": readiness,
        "health": health,
        "meta": {
            "status": 200,
            "completed": True,
            "ts_ms": int(time.time() * 1000),
        },
    }


def api_get_operator_summary(_parsed=None, ctx=None):
    ctx = _ctx_dict(ctx)
    readiness_handler = _api_handler(ctx, "api_get_readiness")
    health_handler = _api_handler(ctx, "api_get_health")
    if readiness_handler is None or health_handler is None:
        return {"ok": False, "error": "summary_handlers_unavailable", "state": "ERROR"}

    readiness = _dict_or_empty(readiness_handler(None, ctx))
    health = _dict_or_empty(health_handler(None, ctx))

    readiness_ok = bool(readiness.get("ok"))
    health_ok = bool(health.get("ok"))

    if readiness_ok and health_ok:
        state = "RUNNING"
    elif readiness_ok or health_ok:
        state = "DEGRADED"
    else:
        state = "ERROR"

    return {
        "ok": bool(readiness_ok and health_ok),
        "state": state,
        "health": health,
        "readiness": readiness,
    }
