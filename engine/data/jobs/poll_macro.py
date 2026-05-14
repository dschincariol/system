"""
FILE: poll_macro.py

Job entrypoint or scheduled task for `poll_macro`.
"""

import json
import logging
import os
import time

from engine.data.factor_ingestion import sync_macro_factors
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.ingestion_status import record_pipeline_status
from engine.runtime.logging import get_logger
from engine.runtime.storage import (
    acquire_job_lock,
    init_db,
    put_job_heartbeat,
    release_job_lock,
    touch_job_lock,
)
from services.data_source_manager import get_manager

JOB_NAME = "poll_macro"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "300"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))
POLL_SECONDS = float(os.environ.get("MACRO_POLL_SECONDS", "21600.0"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [poll_macro] %(message)s",
)
LOG = get_logger("engine.data.jobs.poll_macro")


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="poll_macro_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.data.jobs.poll_macro",
        extra=extra or None,
        persist=False,
    )


def _run_once() -> dict:
    started_ms = int(time.time() * 1000)
    summary = sync_macro_factors(now_ms=started_ms)
    ok = len(summary.get("errors") or []) == 0
    status = record_pipeline_status(
        JOB_NAME,
        ok=ok,
        raw_rows=int(summary.get("observation_rows") or 0),
        event_rows=int(summary.get("event_rows") or 0),
        last_ingested_ts_ms=int(time.time() * 1000),
        error=("; ".join(str(e) for e in (summary.get("errors") or [])[:3])) if not ok else None,
        latency_ms=int(time.time() * 1000) - started_ms,
        meta={
            "series": int(summary.get("series") or 0),
            "feature_rows": int(summary.get("feature_rows") or 0),
            "series_status": summary.get("series_status") or {},
        },
    )
    get_manager().record_job_status(
        JOB_NAME,
        ok=ok,
        message="macro cycle complete" if ok else "macro cycle failed",
        error=("; ".join(str(e) for e in (summary.get("errors") or [])[:3])) if not ok else "",
        meta={
            "series": int(summary.get("series") or 0),
            "feature_rows": int(summary.get("feature_rows") or 0),
            "event_rows": int(summary.get("event_rows") or 0),
            "observation_rows": int(summary.get("observation_rows") or 0),
        },
    )
    logging.info(
        "macro cycle ok=%s series=%s observations=%s features=%s events=%s errors=%s",
        ok,
        int(summary.get("series") or 0),
        int(summary.get("observation_rows") or 0),
        int(summary.get("feature_rows") or 0),
        int(summary.get("event_rows") or 0),
        len(summary.get("errors") or []),
    )
    put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
    return summary


def main() -> None:
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("poll_macro must be launched by supervisor")
        raise SystemExit(1)

    init_db()
    manager = get_manager()

    if not manager.is_job_enabled(JOB_NAME, default=True):
        manager.record_job_status(JOB_NAME, ok=True, message="poll_macro disabled by data source control plane")
        raise SystemExit(0)

    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        raise SystemExit(2)

    last_hb_s = 0.0
    try:
        while True:
            if not manager.is_job_enabled(JOB_NAME, default=True):
                manager.record_job_status(JOB_NAME, ok=True, message="poll_macro disabled by data source control plane")
                break
            now_s = time.time()
            if (now_s - last_hb_s) >= HEARTBEAT_EVERY_S:
                touch_job_lock(JOB_NAME, OWNER, PID)
                put_job_heartbeat(
                    JOB_NAME,
                    OWNER,
                    PID,
                    extra_json=json.dumps({"poll_seconds": float(POLL_SECONDS)}, separators=(",", ":"), sort_keys=True),
                )
                last_hb_s = now_s

            try:
                _run_once()
            except Exception as e:
                logging.exception("macro_cycle_failed")
                status = record_pipeline_status(
                    JOB_NAME,
                    ok=False,
                    raw_rows=0,
                    event_rows=0,
                    last_ingested_ts_ms=int(time.time() * 1000),
                    error=str(e),
                    meta={"poll_seconds": float(POLL_SECONDS)},
                )
                manager.record_job_status(
                    JOB_NAME,
                    ok=False,
                    message="macro cycle failed",
                    error=str(e),
                    meta={"poll_seconds": float(POLL_SECONDS)},
                )
                put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))

            time.sleep(max(300.0, float(POLL_SECONDS)))
    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as e:
            _warn_nonfatal("POLL_MACRO_LOCK_RELEASE_FAILED", e, job=JOB_NAME)


if __name__ == "__main__":
    main()
