"""Mutating self-repair API handlers and route metadata.

This module owns operator-triggered repair actions. Read-only health,
readiness, and system aggregation remain in ``engine.api.api_system`` and are
called only for final status summaries.
"""

import logging
import os
import time

from engine.runtime.failure_diagnostics import (
    failure_response,
    log_failure,
    normalize_root_cause_code,
)
from engine.runtime.health import run_preflight
from engine.runtime.storage import connect as _db_connect, run_write_txn


log = logging.getLogger(__name__)


ROUTE_SPECS_SELF_REPAIR = [
    ("POST", "/api/system/self_repair", "api_post_self_repair"),
    ("POST", "/api/operator/self_repair", "api_post_self_repair"),
    ("POST", "/api/system/repair_schema", "api_post_repair_schema"),
    ("POST", "/api/repair_schema", "api_post_repair_schema"),
]


def _warn(scope: str, err: Exception, **extra) -> None:
    log_failure(
        log,
        event=str(scope),
        code=normalize_root_cause_code(str(scope)),
        message=str(err),
        error=err,
        level=logging.WARNING,
        component="engine.api.api_self_repair",
        extra=extra or None,
        include_health=False,
        persist=True,
    )


def _failure_out(event: str, code: str, error: BaseException, **extra) -> dict:
    payload = failure_response(
        log,
        event=event,
        code=code,
        message=str(error),
        error=error,
        component="engine.api.api_self_repair",
        extra=extra or None,
    )
    payload.setdefault("error", str(error))
    payload.update(extra or {})
    return payload


def api_get_runtime_health(_parsed, ctx=None):
    from engine.api.api_system import api_get_runtime_health as _impl

    return _impl(_parsed, ctx)


def api_get_trading_readiness(_parsed, ctx=None):
    from engine.api.api_system import api_get_trading_readiness as _impl

    return _impl(_parsed, ctx)


def api_post_repair_schema(_parsed=None, body=None, ctx=None):
    del _parsed, body, ctx
    try:
        from engine.runtime.jobs.repair_schema import run as repair_schema

        result = repair_schema()
        if isinstance(result, dict):
            return result
        return {"ok": True}
    except Exception as e:
        return _failure_out(
            "api_self_repair_repair_schema_failed",
            "API_SELF_REPAIR_REPAIR_SCHEMA_FAILED",
            e,
        )


def api_post_self_repair(_parsed=None, body=None, ctx=None):
    try:
        steps = []
        started_ts_ms = int(time.time() * 1000)
        body_payload = body if isinstance(body, dict) else {}
        mode = str(
            body_payload.get("mode")
            or os.environ.get("ENGINE_MODE")
            or "paper"
        ).strip().lower() or "paper"
        log.info("api_self_repair_begin mode=%s", mode)

        def _add_step(step_id, ok, detail=None, **extra):
            item = {
                "step": str(step_id),
                "ok": bool(ok),
                "detail": detail,
            }
            item.update(extra or {})
            steps.append(item)
            return item

        try:
            initial = run_preflight()
            _add_step("preflight_before", bool(initial.get("ok")), initial)
        except Exception as e:
            initial = {"ok": False, "error": str(e)}
            _add_step("preflight_before", False, initial)

        try:
            from engine.runtime.jobs.repair_schema import run as repair_schema

            repair = repair_schema()
            _add_step("repair_schema", bool(repair.get("ok")), repair)
        except Exception as e:
            _add_step("repair_schema", False, {"error": str(e)})

        try:
            from engine.runtime.first_run import bootstrap_first_run

            boot = bootstrap_first_run(mode=mode)
            _add_step("bootstrap_first_run", bool(boot.get("ok")), boot)
        except Exception as e:
            _add_step("bootstrap_first_run", False, {"error": str(e)})

        try:
            stale_after_s = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
            threshold_ms = int(time.time() * 1000) - (stale_after_s * 1000)

            from engine.runtime.storage import _pid_is_running

            con = _db_connect(readonly=True)
            try:
                rows = con.execute(
                    """
                    SELECT job_name, owner, pid, heartbeat_ts_ms
                    FROM job_locks
                    WHERE (heartbeat_ts_ms IS NULL OR heartbeat_ts_ms < ?)
                      AND job_name NOT LIKE 'ingestion_restart_guard/v1::%'
                    ORDER BY job_name ASC
                    """,
                    (int(threshold_ms),),
                ).fetchall() or []
            finally:
                try:
                    con.close()
                except Exception as e:
                    _warn("api_self_repair.job_locks.close", e)

            cleared = []
            skipped = []

            for row in rows:
                job_name = str(row[0] or "")
                owner = str(row[1] or "")
                pid = int(row[2] or 0)
                if not job_name:
                    continue
                if _pid_is_running(pid):
                    skipped.append({"job_name": job_name, "owner": owner, "pid": pid})
                    continue

                def _write(con, _job_name=job_name):
                    con.execute("DELETE FROM job_locks WHERE job_name=?", (_job_name,))
                    con.execute("DELETE FROM job_heartbeats WHERE job_name=?", (_job_name,))

                run_write_txn(_write)
                cleared.append(job_name)

            _add_step(
                "clear_stale_job_locks",
                True,
                {"cleared": cleared, "count": len(cleared), "skipped_running": skipped},
            )
        except Exception as e:
            _add_step("clear_stale_job_locks", False, {"error": str(e)})

        try:
            sup = ctx.get("SUPERVISOR") if ctx else None
            started = []
            results = {}
            isolated_ingestion = str(os.environ.get("START_INGESTION_WITH_SERVER", "1")).strip().lower() in (
                "1",
                "true",
                "yes",
                "on",
            )

            def _env_enabled(key: str, default: bool = False) -> bool:
                raw = os.environ.get(key)
                if raw is None:
                    return bool(default)
                return str(raw).strip().lower() in ("1", "true", "yes", "on")

            def _repair_job_allowed(name: str) -> bool:
                if isolated_ingestion and name in {
                    "ingestion_runtime",
                    "stream_prices_polygon_ws",
                    "stream_prices_ibkr",
                    "poll_prices",
                }:
                    return False
                if name == "stream_prices_polygon_ws":
                    return _env_enabled("POLYGON_WS_ENABLED", False) and bool(
                        str(os.environ.get("POLYGON_API_KEY", "") or "").strip()
                    )
                if name == "stream_prices_ibkr":
                    return _env_enabled("IBKR_ENABLED", False)
                if name == "poll_prices":
                    return any(
                        _env_enabled(key, False)
                        for key in (
                            "YFINANCE_ENABLED",
                            "POLYGON_REST_ENABLED",
                            "CCXT_ENABLED",
                            "TRADIER_ENABLED",
                        )
                    )
                return True

            for name in [
                "ingestion_runtime",
                "stream_prices_polygon_ws",
                "poll_prices",
                "provider_monitor",
                "metrics_collector",
            ]:
                if not _repair_job_allowed(name):
                    results[name] = {"ok": True, "skipped": True, "reason": "disabled_or_isolated_ingestion"}
                    continue
                try:
                    if sup:
                        result = sup.deterministic_start([name], include_deps=True, strict=False)
                    else:
                        result = {"ok": False, "error": "supervisor_missing"}
                    results[name] = result
                    if bool(result.get("ok")):
                        started.append(name)
                except Exception as e:
                    results[name] = {"ok": False, "error": str(e)}

            _add_step("restart_runtime_daemons", len(started) > 0, {"started": started, "results": results})
        except Exception as e:
            _add_step("restart_runtime_daemons", False, {"error": str(e)})

        try:
            sup = ctx.get("SUPERVISOR") if ctx else None
            if sup:
                universe = sup.deterministic_start(["update_universe"], include_deps=True, strict=False)
            else:
                universe = {"ok": False, "error": "supervisor_missing"}
            _add_step("update_universe", bool(universe.get("ok")), universe)
        except Exception as e:
            _add_step("update_universe", False, {"error": str(e)})

        try:
            final_preflight = run_preflight()
            _add_step("preflight_after", bool(final_preflight.get("ok")), final_preflight)
        except Exception as e:
            final_preflight = {"ok": False, "error": str(e)}
            _add_step("preflight_after", False, final_preflight)

        runtime_health = api_get_runtime_health(_parsed, ctx)
        trading_readiness = api_get_trading_readiness(_parsed, ctx)

        ok = bool(final_preflight.get("ok")) or bool(trading_readiness.get("ready"))

        finished_ts_ms = int(time.time() * 1000)
        result = {
            "ok": bool(ok),
            "mode": mode,
            "steps": steps,
            "runtime_health": runtime_health,
            "trading_readiness": trading_readiness,
            "started_ts_ms": started_ts_ms,
            "finished_ts_ms": finished_ts_ms,
            "duration_ms": int(max(0, finished_ts_ms - started_ts_ms)),
        }
        log.info(
            "api_self_repair_done mode=%s ok=%s duration_ms=%s steps=%s",
            mode,
            bool(result.get("ok")),
            int(result.get("duration_ms") or 0),
            len(steps),
        )
        return result

    except Exception as e:
        return _failure_out(
            "api_self_repair_failed",
            "API_SELF_REPAIR_FAILED",
            e,
            steps=[],
        )
