"""
FILE: pipeline_runner.py

Human-readable purpose:
Owns auto-pipeline scheduling for strategy-oriented background workflows such as
the data/model pipeline, challenger updates, and size-policy refreshes.
"""

import logging
import os
import time

# -------------------------------------------------
# Last-run timestamps (process local, read-only)
# -------------------------------------------------

LAST_AUTO_PIPELINE_TS = None
LAST_AUTO_PIPELINE_HEARTBEAT_TS = None

LAST_AUTO_CHALLENGER_TS = None
LAST_AUTO_CHALLENGER_HEARTBEAT_TS = None

LAST_AUTO_SIZE_POLICY_TS = None
LAST_AUTO_SIZE_POLICY_HEARTBEAT_TS = None

LOG = logging.getLogger(__name__)

from engine.runtime.storage import connect as _db_connect
from engine.runtime.ipc import market_data_status

from engine.runtime.dashboard_config import (
    AUTO_PIPELINE_INCLUDE_EXECUTION,
    AUTO_PIPELINE_START_DELAY_S,
    AUTO_PIPELINE_INTERVAL_S,
    AUTO_PIPELINE_LOG,
    AUTO_CHALLENGER_START_DELAY_S,
    AUTO_CHALLENGER_INTERVAL_S,
    AUTO_CHALLENGER_LOG,
    AUTO_CHALLENGER_MIN_DRIFT,
    AUTO_SIZE_POLICY_START_DELAY_S,
    AUTO_SIZE_POLICY_INTERVAL_S,
    AUTO_SIZE_POLICY_LOG,
)

from engine.runtime.jobs_manager import _acquire_lock, _release_lock
from engine.runtime.job_registry import PIPELINE_ORDER, is_execution_job


# -------------------------------------------------
# PIPELINE RUNNER
# -------------------------------------------------

def run_pipeline(JOBS):
    if not _acquire_lock("pipeline", ttl_ms=20 * 60 * 1000):
        return {"ok": False, "error": "pipeline locked"}

    try:
        price_running = (
            JOBS.is_running("ingestion_runtime")
            or JOBS.is_running("poll_prices")
            or JOBS.is_running("stream_prices_polygon_ws")
        )

        if not price_running:
            try:
                snap = market_data_status(
                    max_age_ms=int(float(os.environ.get("HEALTH_PRICES_MAX_AGE_S", "120")) * 1000.0)
                )
                price_running = bool(snap.get("ok") and snap.get("running"))
            except Exception:
                price_running = False

        if not price_running:
            return {"ok": False, "error": "prices daemon must be running (isolated ingestion or poll_prices or stream_prices_polygon_ws)"}

        # Jobs are started in registry order so dependency assumptions remain
        # centralized in the runtime registry rather than scattered here.
        for name in PIPELINE_ORDER:
            if is_execution_job(name) and not AUTO_PIPELINE_INCLUDE_EXECUTION:
                continue

            job = JOBS.get(name)
            if not job or job.mode == "daemon":
                continue

            res = JOBS.start(name)
            if not res.get("ok"):
                return {"ok": False, "error": f"{name}: {res.get('error') or res.get('reason')}"}

            start_ts = time.time()
            while True:
                time.sleep(0.25)

                if not job.proc:
                    break

                if job.proc.poll() is not None:
                    if job.exit_code not in (0, None):
                        return {"ok": False, "error": f"{name} exited rc={job.exit_code}"}
                    break

                if time.time() - start_ts > 20 * 60:
                    return {"ok": False, "error": f"{name} timed out"}

        return {"ok": True}

    finally:
        _release_lock("pipeline")


# -------------------------------------------------
# AUTO PIPELINE LOOP
# -------------------------------------------------

def auto_pipeline_loop(JOBS):
    global LAST_AUTO_PIPELINE_TS
    global LAST_AUTO_PIPELINE_HEARTBEAT_TS

    time.sleep(max(0.0, float(AUTO_PIPELINE_START_DELAY_S)))

    while True:
        LAST_AUTO_PIPELINE_HEARTBEAT_TS = int(time.time())
        try:
            LAST_AUTO_PIPELINE_TS = int(time.time())

            res = run_pipeline(JOBS)

            if AUTO_PIPELINE_LOG:
                LOG.info("auto_pipeline result=%s", res)
        except Exception as e:
            if AUTO_PIPELINE_LOG:
                LOG.log(logging.WARNING, "auto_pipeline failed: %s", e, exc_info=True)

        time.sleep(max(5.0, float(AUTO_PIPELINE_INTERVAL_S)))


# -------------------------------------------------
# AUTO CHALLENGER LOOP
# -------------------------------------------------

def _max_drift_ratio():
    con = _db_connect()
    try:
        row = con.execute("SELECT MAX(drift_ratio) FROM model_drift").fetchone()
        return float(row[0] or 0.0) if row else 0.0
    finally:
        con.close()

def auto_challenger_loop(JOBS):
    global LAST_AUTO_CHALLENGER_TS
    global LAST_AUTO_CHALLENGER_HEARTBEAT_TS

    time.sleep(max(0.0, float(AUTO_CHALLENGER_START_DELAY_S)))

    while True:
        LAST_AUTO_CHALLENGER_HEARTBEAT_TS = int(time.time())
        try:
            md = _max_drift_ratio()
            if AUTO_CHALLENGER_MIN_DRIFT > 0.0 and md < AUTO_CHALLENGER_MIN_DRIFT:
                if AUTO_CHALLENGER_LOG:
                    LOG.info("auto_challenger skipped drift=%.3f", md)
            else:
                LAST_AUTO_CHALLENGER_TS = int(time.time())
                res = JOBS.start("pipeline_train_and_eval")

                if AUTO_CHALLENGER_LOG:
                    LOG.info("auto_challenger result=%s", res)
        except Exception as e:
            if AUTO_CHALLENGER_LOG:
                LOG.log(logging.WARNING, "auto_challenger failed: %s", e, exc_info=True)

        time.sleep(max(30.0, float(AUTO_CHALLENGER_INTERVAL_S)))


# -------------------------------------------------
# AUTO SIZE POLICY LOOP (RE-ENABLED)
# -------------------------------------------------

def auto_size_policy_loop(JOBS):
    global LAST_AUTO_SIZE_POLICY_TS

    time.sleep(max(0.0, float(AUTO_SIZE_POLICY_START_DELAY_S)))

    while True:
        try:
            LAST_AUTO_SIZE_POLICY_TS = int(time.time())
            res = JOBS.start("train_size_policy")

            if AUTO_SIZE_POLICY_LOG:
                LOG.info("auto_size_policy result=%s", res)
        except Exception as e:
            if AUTO_SIZE_POLICY_LOG:
                LOG.log(logging.WARNING, "auto_size_policy failed: %s", e, exc_info=True)

        time.sleep(max(60.0, float(AUTO_SIZE_POLICY_INTERVAL_S)))
