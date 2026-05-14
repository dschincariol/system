"""
FILE: api_jobs.py

HTTP/API handlers for jobs endpoints.
"""

# engine/api/api_jobs.py
# Route specs + implementations for job control + pipeline endpoints.
# This file contains no imports from dashboard_server.py.

import os
import time
import copy
import logging
import threading
from engine.api.http_parsing import qs as _qs
from engine.runtime.failure_diagnostics import failure_response, log_failure, normalize_root_cause_code
from engine.runtime.job_registry import ALLOWED_JOBS, PIPELINE_ORDER, JOB_ORDER
from engine.runtime.storage import connect as _db_connect, _pid_is_running

log = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# Routes
# ----------------------------------------------------------------------

ROUTE_SPECS = [
    ("GET",  "/api/jobs/log",     "api_get_job_log"),
    ("GET",  "/api/jobs/history", "api_get_job_history"),
    ("GET",  "/api/jobs",         "api_get_jobs"),
    ("POST", "/api/jobs/start",   "api_post_job_start"),
    ("POST", "/api/jobs/stop",    "api_post_job_stop"),
    ("POST", "/api/pipeline/run", "api_post_pipeline_run"),
]

ROUTE_SPECS_JOBS = ROUTE_SPECS

_JOBS_CACHE_LOCK = threading.Lock()
_JOBS_CACHE = {
    "ts_ms": 0,
    "payload": None,
}
_JOBS_CACHE_TTL_MS = int(float(os.environ.get("API_JOBS_CACHE_TTL_S", "2.5")) * 1000.0)
_API_JOB_LIST_TIMEOUT_S = float(os.environ.get("API_JOB_LIST_TIMEOUT_S", "0.5"))


def _warn(scope: str, err: Exception, **extra) -> None:
    log_failure(
        log,
        event=f"api_jobs_{scope}",
        code=normalize_root_cause_code(f"api_jobs_{scope}"),
        message=str(err),
        error=err,
        level=logging.WARNING,
        component="engine.api.api_jobs",
        extra=extra or None,
        include_health=False,
        persist=True,
    )


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _job_name_from(parsed, body) -> str:
    q = _qs(parsed) or {}
    name = (q.get("name") or "").strip()
    if not name and isinstance(body, dict):
        name = str(body.get("name") or "").strip()
    return name


def _jobs_from(ctx):
    try:
        if isinstance(ctx, dict):
            return ctx.get("JOBS")
    except Exception as e:
        _warn("ctx_jobs_lookup", e)
    return None


def _supervisor_from(ctx):
    try:
        if isinstance(ctx, dict):
            return ctx.get("SUPERVISOR")
    except Exception as e:
        _warn("ctx_supervisor_lookup", e)
    return None


def _wait_for_job_completion(JOBS, name: str, timeout_s: float = 3600.0):
    # Used by synchronous job-control endpoints that need a bounded wait for a
    # oneshot job to finish before replying.
    deadline = time.time() + max(1.0, float(timeout_s))
    while time.time() < deadline:
        try:
            job = JOBS.get(name)
        except Exception as e:
            _warn("job_lookup_failed", e, job=name)
            return {"ok": False, "error": f"job_lookup_failed: {e}", "job": name}

        if job is None:
            return {"ok": False, "error": "job_not_found", "job": name}

        proc = getattr(job, "proc", None)
        if proc is None:
            exit_code = getattr(job, "exit_code", None)
            if exit_code in (None, 0):
                return {"ok": True, "job": name, "exit_code": exit_code}
            return {"ok": False, "error": "job_failed", "job": name, "exit_code": exit_code}

        try:
            rc = proc.poll()
        except Exception as e:
            _warn("poll_failed", e, job=name)
            return {"ok": False, "error": f"poll_failed: {e}", "job": name}

        if rc is not None:
            if int(rc) == 0:
                return {"ok": True, "job": name, "exit_code": int(rc)}
            return {"ok": False, "error": "job_failed", "job": name, "exit_code": int(rc)}

        time.sleep(0.25)

    return {"ok": False, "error": "job_timeout", "job": name, "timeout_s": float(timeout_s)}


def _pipeline_names_from(body, *, include_execution: bool):
    requested = None
    if isinstance(body, dict):
        raw = body.get("jobs")
        if isinstance(raw, (list, tuple)):
            requested = [str(x).strip() for x in raw if str(x).strip()]

    order = list(requested if requested is not None else (PIPELINE_ORDER or []))
    selected = []
    skipped = []

    for name in order:
        if name not in ALLOWED_JOBS:
            skipped.append({"job": name, "reason": "not_registered"})
            continue

        meta = (ALLOWED_JOBS.get(name) or ("", "", "", {}))[3] if len(ALLOWED_JOBS.get(name) or ()) >= 4 else {}
        if (meta or {}).get("execution") is True and not include_execution:
            skipped.append({"job": name, "reason": "execution_excluded"})
            continue

        selected.append(name)

    return selected, skipped


def _clear_jobs_cache() -> None:
    try:
        with _JOBS_CACHE_LOCK:
            _JOBS_CACHE["ts_ms"] = 0
            _JOBS_CACHE["payload"] = None
    except Exception as e:
        _warn("cache_clear", e)


# ----------------------------------------------------------------------
# Endpoints
# ----------------------------------------------------------------------

def api_get_jobs(parsed, _body=None, ctx=None):
    JOBS = _jobs_from(ctx)
    if not JOBS:
        return {"ok": False, "error": "jobs_manager_unavailable"}

    now_ms = int(time.time() * 1000)
    cache_ttl_ms = max(0, int(_JOBS_CACHE_TTL_MS))
    if cache_ttl_ms > 0:
        try:
            with _JOBS_CACHE_LOCK:
                cached_ts_ms = int(_JOBS_CACHE.get("ts_ms") or 0)
                cached_payload = _JOBS_CACHE.get("payload")
                if (
                    isinstance(cached_payload, dict)
                    and cached_ts_ms > 0
                    and (now_ms - cached_ts_ms) <= cache_ttl_ms
                ):
                    cached = copy.deepcopy(cached_payload)
                    cached["ts_ms"] = now_ms
                    return cached
        except Exception as e:
            _warn("cache_read", e)

    stale_after_s = int(float(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180")))

    try:
        try:
            running = JOBS.list_jobs(timeout_s=max(0.05, float(_API_JOB_LIST_TIMEOUT_S)), include_persisted=False) or []
        except TypeError:
            running = JOBS.list_jobs() or []
    except TimeoutError as e:
        _warn("jobs_list_timeout", e)
        running = []
    except Exception as e:
        _warn("jobs_list", e)
        running = []

    running_by_name = {}
    for j in running:
        try:
            n = str(j.get("name") or "").strip()
            if n:
                running_by_name[n] = j
        except Exception as e:
            _warn("jobs_running_row", e, row=repr(j)[:200])
            continue

    lock_rows = {}
    history_summary = {}
    con = None

    try:
        con = _db_connect(readonly=True)
        try:
            rows = con.execute(
                """
                SELECT job_name, owner, pid, acquired_ts_ms, heartbeat_ts_ms
                FROM job_locks
                """
            ).fetchall() or []

            for job_name, owner, pid, acquired_ts_ms, heartbeat_ts_ms in rows:
                lock_rows[str(job_name or "")] = {
                    "owner": str(owner or ""),
                    "pid": (int(pid) if pid is not None else None),
                    "acquired_ts_ms": (int(acquired_ts_ms) if acquired_ts_ms is not None else None),
                    "heartbeat_ts_ms": (int(heartbeat_ts_ms) if heartbeat_ts_ms is not None else None),
                }
        except Exception as e:
            _warn("job_locks_read", e)

        try:
            rows = con.execute(
                """
                SELECT
                  job_name,
                  SUM(CASE WHEN event IN ('autorestart_started', 'autorestart_scheduled', 'autorestart_stall_detected') THEN 1 ELSE 0 END) AS restart_count,
                  SUM(CASE WHEN event IN ('exit', 'start_failed', 'stop_hard_kill') THEN 1 ELSE 0 END) AS crash_like_count,
                  MAX(CASE WHEN event = 'exit' THEN exit_code END) AS last_exit_code,
                  MAX(ts_ms) AS last_event_ts_ms
                FROM job_history
                GROUP BY job_name
                """
            ).fetchall() or []

            for job_name, restart_count, crash_like_count, last_exit_code, last_event_ts_ms in rows:
                history_summary[str(job_name or "")] = {
                    "restart_count": int(restart_count or 0),
                    "crash_like_count": int(crash_like_count or 0),
                    "last_exit_code": (int(last_exit_code) if last_exit_code is not None else None),
                    "last_event_ts_ms": (int(last_event_ts_ms) if last_event_ts_ms is not None else None),
                }
        except Exception as e:
            _warn("job_history_read", e)
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn("db_close", e)

    # Merge live JobManager state with persisted lock/history state so the API
    # can describe jobs even when they are currently stopped.
    allowed_names = list(ALLOWED_JOBS.keys())

    order = list(JOB_ORDER or [])
    remaining = sorted([n for n in allowed_names if n not in set(order)])
    names = [n for n in order if n in set(allowed_names)] + remaining

    out = []
    for name in names:
        spec = (ALLOWED_JOBS.get(name) or ("", "", "", {}))
        if name in running_by_name:
            row = dict(running_by_name[name] or {})
        else:
            row = {
                "name": name,
                "script": spec[0],
                "mode": spec[1],
                "group": spec[2],
                "running": False,
                "started_at_ms": None,
                "exited_at_ms": None,
                "exit_code": None,
                "log_lines": 0,
                "stop_requested": False,
                "next_restart_ms": 0,
            }

        lock = dict(lock_rows.get(name) or {})
        hist = dict(history_summary.get(name) or {})

        hb_ts = lock.get("heartbeat_ts_ms") or row.get("heartbeat_ts_ms")
        heartbeat_age_s = (round((now_ms - int(hb_ts)) / 1000.0, 1) if hb_ts else None)
        pid = lock.get("pid")
        if pid is None:
            pid = row.get("pid")
        pid_running = bool(pid and _pid_is_running(pid))
        stale = bool(heartbeat_age_s is not None and heartbeat_age_s > float(stale_after_s))

        if (not bool(row.get("running"))) and pid_running and not stale:
            row["running"] = True
            row["started_at_ms"] = row.get("started_at_ms") or lock.get("acquired_ts_ms")
            row["exit_code"] = None
            row["exited_at_ms"] = None

        if bool(row.get("running")) and ((pid and (not pid_running)) or stale):
            row["running"] = False
            if row.get("exit_code") is None:
                row["exit_code"] = hist.get("last_exit_code")

        row["owner"] = lock.get("owner") or row.get("lock_owner")
        row["pid"] = pid
        row["pid_running"] = pid_running
        row["acquired_ts_ms"] = lock.get("acquired_ts_ms") or row.get("started_at_ms")
        row["heartbeat_ts_ms"] = hb_ts
        row["heartbeat_age_s"] = heartbeat_age_s
        row["stale"] = stale
        row["restart_count"] = int(hist.get("restart_count") or row.get("restart_count") or 0)
        row["crash_like_count"] = int(hist.get("crash_like_count") or 0)
        row["last_exit_code"] = hist.get("last_exit_code")
        row["last_event_ts_ms"] = hist.get("last_event_ts_ms")

        out.append(row)

    payload = {
        "ok": True,
        "ts_ms": now_ms,
        "jobs": out,
        "pipeline_order": list(PIPELINE_ORDER or []),
        "allowed": names,
    }
    if cache_ttl_ms > 0:
        try:
            with _JOBS_CACHE_LOCK:
                _JOBS_CACHE["ts_ms"] = int(time.time() * 1000)
                _JOBS_CACHE["payload"] = copy.deepcopy(payload)
        except Exception as e:
            _warn("cache_write", e)
    return payload


def api_post_job_start(parsed, body=None, ctx=None):
    JOBS = _jobs_from(ctx)
    if not JOBS:
        return {"ok": False, "error": "jobs_manager_unavailable"}

    name = _job_name_from(parsed, body)
    if not name:
        return {"ok": False, "error": "missing_name"}

    if name not in ALLOWED_JOBS:
        return {"ok": False, "error": "job_not_registered", "job": name}

    try:
        res = JOBS.start(name)
        _clear_jobs_cache()
        if isinstance(res, dict):
            return res
        return {"ok": True, "job": name, "status": "started"}
    except Exception as e:
        out = failure_response(
            log,
            event="api_jobs_start_failed",
            code="API_JOBS_START_FAILED",
            message=str(e),
            error=e,
            component="engine.api.api_jobs",
            ctx=ctx,
            extra={"job": name},
        )
        out["job"] = name
        return out


def api_post_job_stop(parsed, body=None, ctx=None):
    JOBS = _jobs_from(ctx)
    if not JOBS:
        return {"ok": False, "error": "jobs_manager_unavailable"}

    name = _job_name_from(parsed, body)
    if not name:
        return {"ok": False, "error": "missing_name"}

    if name not in ALLOWED_JOBS:
        return {"ok": False, "error": "job_not_registered", "job": name}

    try:
        res = JOBS.stop(name)
        _clear_jobs_cache()
        if isinstance(res, dict):
            return res
        return {"ok": True, "job": name, "status": "stopped"}
    except Exception as e:
        out = failure_response(
            log,
            event="api_jobs_stop_failed",
            code="API_JOBS_STOP_FAILED",
            message=str(e),
            error=e,
            component="engine.api.api_jobs",
            ctx=ctx,
            extra={"job": name},
        )
        out["job"] = name
        return out


def api_get_job_log(parsed, _body=None, ctx=None):
    JOBS = _jobs_from(ctx)
    if not JOBS:
        return {"ok": False, "error": "jobs_manager_unavailable"}

    q = _qs(parsed) or {}
    name = str((q.get("name") or "")).strip()
    tail_s = str((q.get("tail") or "200")).strip()

    if not name:
        return {"ok": False, "error": "missing_name"}

    try:
        tail = max(1, min(4000, int(tail_s)))
    except Exception:
        tail = 200

    # JobManager already exposes get_job_log
    try:
        out = JOBS.get_job_log(name, tail=tail)
        if isinstance(out, dict):
            out.setdefault("ok", True)
            out.setdefault("job", name)
            return out
        return {
            "ok": True,
            "job": name,
            "tail": tail,
            "data": out or [],
        }
    except Exception as e:
        out = failure_response(
            log,
            event="api_jobs_get_job_log_failed",
            code="API_JOBS_GET_JOB_LOG_FAILED",
            message="job_log_exception",
            error=e,
            component="engine.api.api_jobs",
            ctx=ctx,
            extra={"job": name, "tail": int(tail)},
        )
        out.update({"detail": str(e), "job": name, "data": []})
        return out


def api_get_job_history(parsed, _body=None, ctx=None):
    JOBS = _jobs_from(ctx)
    if not JOBS:
        return {"ok": False, "error": "jobs_manager_unavailable"}

    q = _qs(parsed) or {}
    name = str((q.get("name") or "")).strip()
    limit_s = str((q.get("limit") or "200")).strip()

    if not name:
        return {"ok": False, "error": "missing_name"}

    try:
        limit = max(1, min(5000, int(limit_s)))
    except Exception:
        limit = 200

    try:
        out = JOBS.get_job_history(name, limit=limit)

        if isinstance(out, dict):
            out.setdefault("ok", True)
            out.setdefault("job", name)
            return out

        return {
            "ok": True,
            "job": name,
            "limit": limit,
            "data": out or [],
        }

    except Exception as e:
        out = failure_response(
            log,
            event="api_jobs_get_job_history_failed",
            code="API_JOBS_GET_JOB_HISTORY_FAILED",
            message="job_history_exception",
            error=e,
            component="engine.api.api_jobs",
            ctx=ctx,
            extra={"job": name, "limit": int(limit)},
        )
        out.update({"detail": str(e), "job": name, "data": []})
        return out

def api_post_pipeline_run(parsed, body=None, ctx=None):
    # Runs the PIPELINE_ORDER sequentially and waits for each oneshot step
    # to complete. When available, use RuntimeSupervisor deterministic DAG start.
    JOBS = _jobs_from(ctx)
    if not JOBS:
        return {"ok": False, "error": "jobs_manager_unavailable"}

    q = _qs(parsed) or {}
    include_execution = str(q.get("include_execution") or "").strip().lower() in ("1", "true", "yes", "y", "on")
    if not include_execution and isinstance(body, dict):
        include_execution = str(body.get("include_execution") or "").strip().lower() in ("1", "true", "yes", "y", "on")
    pipeline_names, skipped = _pipeline_names_from(body, include_execution=include_execution)
    supervisor = _supervisor_from(ctx)

    if supervisor is not None and hasattr(supervisor, "deterministic_start") and not isinstance(body, dict):
        try:
            return supervisor.deterministic_start(
                list(PIPELINE_ORDER or []),
                include_deps=False,
                strict=False,
            )
        except Exception as e:
            out = failure_response(
                log,
                event="api_jobs_pipeline_supervisor_failed",
                code="API_JOBS_PIPELINE_SUPERVISOR_FAILED",
                message=f"pipeline_supervisor_failed: {e}",
                error=e,
                component="engine.api.api_jobs",
                ctx=ctx,
                extra={"pipeline_order": list(PIPELINE_ORDER or [])},
            )
            out["pipeline_order"] = list(PIPELINE_ORDER or [])
            return out

    started = []
    errors = []

    for name in pipeline_names:
        try:
            res = JOBS.start(name)
            if isinstance(res, dict) and not res.get("ok", True):
                errors.append({"job": name, "error": res})
                break

            wait = _wait_for_job_completion(JOBS, name)
            if not wait.get("ok"):
                errors.append({"job": name, "error": wait})
                break

            started.append({"job": name, "result": res, "wait": wait})
        except Exception as e:
            errors.append({"job": name, "error": str(e)})
            break

    return {
        "ok": len(errors) == 0,
        "started": started,
        "skipped": skipped,
        "errors": errors,
        "pipeline_order": pipeline_names,
    }

__all__ = [
    "ROUTE_SPECS",
    "ROUTE_SPECS_JOBS",
    "api_get_jobs",
    "api_post_job_start",
    "api_post_job_stop",
    "api_get_job_log",
    "api_get_job_history",
    "api_post_pipeline_run",
]
