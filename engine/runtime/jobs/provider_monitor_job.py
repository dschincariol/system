"""
FILE: provider_monitor_job.py

Job entrypoint or scheduled task for `provider_monitor_job`.
"""

import logging
import json
import os
import sys
import time

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.ingestion_status import pipeline_health_summary
from engine.runtime.storage import (
    acquire_job_lock,
    connect_ro,
    init_db,
    put_job_heartbeat,
    release_job_lock,
    touch_job_lock,
)

JOB_NAME = "provider_monitor"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()

HEARTBEAT_EVERY_S = float(os.environ.get("PROVIDER_MONITOR_HEARTBEAT_S", "10"))
CHECK_EVERY_S = float(os.environ.get("PROVIDER_MONITOR_CHECK_S", "15"))
STALE_AFTER_S = float(os.environ.get("PROVIDER_MONITOR_STALE_AFTER_S", "120"))
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
LOG = get_logger("engine.runtime.jobs.provider_monitor_job")


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="provider_monitor_job_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.runtime.jobs.provider_monitor_job",
        extra=extra or None,
        persist=False,
    )


def _provider_snapshot() -> dict:
    now_ms = int(time.time() * 1000)
    cutoff_ms = int(now_ms - (float(STALE_AFTER_S) * 1000.0))

    con = connect_ro()
    error_text = None
    try:
        rows = con.execute(
            """
            SELECT p.provider, p.ts_ms, p.ok, p.latency_ms, p.n_symbols, p.error
            FROM price_provider_health p
            INNER JOIN (
                SELECT provider, MAX(ts_ms) AS max_ts_ms
                FROM price_provider_health
                GROUP BY provider
            ) latest
              ON latest.provider = p.provider
             AND latest.max_ts_ms = p.ts_ms
            ORDER BY p.provider
            """
        ).fetchall() or []
    except Exception as e:
        rows = []
        error_text = str(e)
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("PROVIDER_MONITOR_CONNECTION_CLOSE_FAILED", e, operation="_provider_snapshot")

    providers = {}
    healthy = 0

    for provider, ts_ms, ok, latency_ms, n_symbols, error in rows:
        provider_s = str(provider or "").strip().lower()
        last_ts_ms = int(ts_ms or 0)
        age_ms = max(0, now_ms - last_ts_ms) if last_ts_ms > 0 else 10**12
        provider_ok = bool(int(ok or 0) == 1 and last_ts_ms >= cutoff_ms)
        providers[provider_s] = {
            "last_ts_ms": last_ts_ms,
            "age_ms": int(age_ms),
            "latency_ms": (None if latency_ms is None else int(latency_ms)),
            "n_symbols": int(n_symbols or 0),
            "ok": provider_ok,
            "error": (None if error is None else str(error)),
        }
        if provider_ok:
            healthy += 1

    pipeline_summary = pipeline_health_summary(stale_after_s=float(STALE_AFTER_S))

    return {
        "ok": bool(healthy > 0),
        "ts_ms": int(now_ms),
        "healthy_providers": int(healthy),
        "providers": providers,
        "ingestion_pipelines": pipeline_summary,
        "error": error_text,
    }


def main():
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("provider_monitor must be launched by supervisor")
        raise SystemExit(1)

    init_db()

    if not acquire_job_lock(JOB_NAME, OWNER, PID, stale_after_s=LOCK_STALE_AFTER_S):
        print("provider_monitor lock already held")
        raise SystemExit(2)

    last_check_at = 0.0
    loop_error_streak = 0

    try:
        while True:
            try:
                now = time.time()

                touch_job_lock(JOB_NAME, OWNER, PID)

                extra_json = None
                if (now - last_check_at) >= float(CHECK_EVERY_S):
                    last_check_at = now
                    extra_json = json.dumps(_provider_snapshot(), separators=(",", ":"), sort_keys=True)

                put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=str(extra_json or ""))

                loop_error_streak = 0
                time.sleep(max(1.0, float(HEARTBEAT_EVERY_S)))
            except KeyboardInterrupt:
                raise
            except SystemExit:
                raise
            except Exception as e:
                loop_error_streak += 1
                print(f"provider_monitor loop error #{loop_error_streak}: {type(e).__name__}: {e}", file=sys.stderr, flush=True)
                time.sleep(min(30.0, max(1.0, float(HEARTBEAT_EVERY_S) * max(1, loop_error_streak))))
    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as e:
            _warn_nonfatal("PROVIDER_MONITOR_LOCK_RELEASE_FAILED", e, job=JOB_NAME, owner=OWNER, pid=int(PID))


if __name__ == "__main__":
    main()
