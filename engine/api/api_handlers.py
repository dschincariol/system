from __future__ import annotations
# engine/api/api_handlers.py
"""
Legacy compatibility handlers.

dashboard_server.py imports these (best-effort):
  - api_get_kill_switches
  - api_get_job_log
  - api_get_job_history

Keep this file as a thin compatibility bridge only.
"""

"""
FILE: api_handlers.py

HTTP/API handlers for handlers endpoints.
"""

import logging
from typing import Any, Dict

from engine.api.http_parsing import qs as _qs
from engine.api.log_filters import coerce_int, ensure_lines, filter_lines, lines_to_text, normalize_level
from engine.runtime.failure_diagnostics import failure_response


LOG = logging.getLogger(__name__)


def api_get_kill_switches(parsed: Any, _ctx: Dict[str, Any] | None = None) -> Dict[str, Any]:
    try:
        # Keep this as a compatibility bridge so older imports can still get a
        # normalized kill-switch payload without routing back through status
        # handlers, which would recurse when status itself queries this bridge.
        from engine.execution.kill_switch import snapshot as _snapshot

        data = _snapshot()
        if not isinstance(data, dict):
            data = {"state": []}
        return {
            "ok": True,
            "status": "OK",
            "state": "UNKNOWN",
            "mode": "unknown",
            "execution_mode": "unknown",
            "execution_allowed": False,
            "reasons": [],
            "health": {},
            "ingestion": {},
            "services": {},
            "readiness": {},
            "timestamps": {},
            "data": data,
            "kill_switches": data,
        }
    except Exception as e:
        out = failure_response(
            LOG,
            event="api_handlers_kill_switches_failed",
            code="API_HANDLERS_KILL_SWITCHES_FAILED",
            message=str(e),
            error=e,
            component="engine.api.api_handlers",
            ctx=_ctx,
            include_health=True,
        )
        return {
            **out,
            "status": "DEGRADED",
            "state": "UNKNOWN",
            "mode": "unknown",
            "execution_mode": "unknown",
            "execution_allowed": False,
            "reasons": [f"kill_switch_exception:{e}", str(out.get("root_cause_code") or "")],
            "health": {},
            "ingestion": {},
            "services": {},
            "readiness": {},
            "timestamps": {},
        }


def api_get_job_log(parsed: Any, ctx: Dict[str, Any] | None = None) -> Dict[str, Any]:
    q = _qs(parsed)
    name = str(q.get("name") or q.get("job") or "").strip()
    tail = coerce_int(q.get("tail") or "400", 400, 1, 4000)
    query = str(q.get("q") or "").strip()
    level = normalize_level(q.get("level") or "")
    limit = coerce_int(q.get("limit") or "0", 0, 0, 4000)

    if not name:
        return {"ok": False, "error": "missing_job_name"}

    if not isinstance(ctx, dict) or "JOBS" not in ctx:
        return {"ok": False, "error": "missing_ctx_jobs"}

    try:
        jobs = ctx["JOBS"]
        out = jobs.get_job_log(name=name, tail=tail)
        if isinstance(out, dict):
            out = dict(out)
            raw_lines = ensure_lines(out)
            filtered_lines = filter_lines(raw_lines, level=level, query=query, limit=limit)
            text = lines_to_text(filtered_lines)
            out.setdefault("ok", True)
            out.setdefault("job", name)
            out["tail"] = int(tail)
            out["lines"] = filtered_lines
            out["text"] = text
            out["log"] = text
            out["source"] = f"job:{name}"
            out["raw_line_count"] = len(raw_lines)
            out["filtered_line_count"] = len(filtered_lines)
            out["applied_filters"] = {
                "q": query,
                "level": level,
                "limit": int(limit) if limit > 0 else None,
                "tail": int(tail),
            }
            return out
        raw_lines = ensure_lines(out)
        filtered_lines = filter_lines(raw_lines, level=level, query=query, limit=limit)
        text = lines_to_text(filtered_lines)
        return {
            "ok": True,
            "job": name,
            "tail": int(tail),
            "source": f"job:{name}",
            "data": out,
            "lines": filtered_lines,
            "text": text,
            "log": text,
            "raw_line_count": len(raw_lines),
            "filtered_line_count": len(filtered_lines),
            "applied_filters": {
                "q": query,
                "level": level,
                "limit": int(limit) if limit > 0 else None,
                "tail": int(tail),
            },
        }
    except Exception as e:
        out = failure_response(
            LOG,
            event="api_handlers_job_log_failed",
            code="API_HANDLERS_JOB_LOG_FAILED",
            message=str(e),
            error=e,
            component="engine.api.api_handlers",
            ctx=ctx,
            extra={"job": name, "tail": int(tail)},
        )
        out["job"] = name
        out["tail"] = int(tail)
        return out


def api_get_job_history(parsed: Any, _body: Any = None, ctx: Dict[str, Any] | None = None) -> Dict[str, Any]:
    q = _qs(parsed)
    name = str(q.get("name") or q.get("job") or "").strip()
    try:
        limit = int(q.get("limit") or "200")
    except Exception:
        limit = 200

    if not name:
        return {"ok": False, "error": "missing_job_name"}

    if not isinstance(ctx, dict) or "JOBS" not in ctx:
        return {"ok": False, "error": "missing_ctx_jobs"}

    try:
        jobs = ctx["JOBS"]
        out = jobs.get_job_history(name=name, limit=limit)
        if isinstance(out, dict):
            out.setdefault("ok", True)
            return out
        return {"ok": True, "job": name, "limit": limit, "data": out}
    except Exception as e:
        out = failure_response(
            LOG,
            event="api_handlers_job_history_failed",
            code="API_HANDLERS_JOB_HISTORY_FAILED",
            message=str(e),
            error=e,
            component="engine.api.api_handlers",
            ctx=ctx,
            extra={"job": name, "limit": int(limit)},
        )
        out["job"] = name
        out["limit"] = int(limit)
        return out
