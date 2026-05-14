"""
FILE: compute_drift.py

Operational helper script for `compute_drift`.
"""

# compute_drift.py
import os
import time
import json
import logging

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import (
    init_db,
    connect,
    acquire_job_lock,
    release_job_lock,
    touch_job_lock,
    put_job_heartbeat,
)
from engine.strategy.drift import compute_and_store_drift
from engine.strategy.distribution_drift import compute_and_store_distribution_drift

JOB_NAME = "compute_drift"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))

# stale-data guardrails
MAX_PREDICTIONS_AGE_S = float(os.environ.get("DRIFT_MAX_PREDICTIONS_AGE_S", "900"))
MAX_LABELS_AGE_S = float(os.environ.get("DRIFT_MAX_LABELS_AGE_S", "900"))

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [compute_drift] %(message)s",
)


def _warn_nonfatal(event: str, error: BaseException, **extra) -> None:
    log_failure(
        logging.getLogger("compute_drift"),
        event=event,
        code=event,
        message=event,
        error=error,
        level=logging.WARNING,
        component="ops.compute_drift",
        extra=extra,
        persist=False,
    )


def _latest_age_s(con, table: str) -> float | None:
    try:
        row = con.execute(f"SELECT MAX(ts_ms) FROM {table}").fetchone()
    except Exception as e:
        logging.warning("compute_drift latest_age_failed table=%s err=%s", table, e)
        return None
    if not row or not row[0]:
        return None
    return (int(time.time() * 1000) - int(row[0])) / 1000.0


def main():
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("options_poll must be launched by supervisor")
        raise SystemExit(1)

    init_db()

    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        logging.error("another instance is holding the job lock; exiting")
        raise SystemExit(2)

    started_ms = int(time.time() * 1000)
    last_hb_s = 0.0

    try:
        con = connect()

        pred_age_s = _latest_age_s(con, "predictions")
        lbl_age_s = _latest_age_s(con, "labels")

        if pred_age_s is None or pred_age_s > MAX_PREDICTIONS_AGE_S:
            logging.warning(
                "skipping drift: predictions stale or missing age_s=%s limit=%s",
                pred_age_s,
                MAX_PREDICTIONS_AGE_S,
            )
            return

        if lbl_age_s is None or lbl_age_s > MAX_LABELS_AGE_S:
            logging.warning(
                "skipping drift: labels stale or missing age_s=%s limit=%s",
                lbl_age_s,
                MAX_LABELS_AGE_S,
            )
            return

        now_s = time.time()
        if (now_s - last_hb_s) >= HEARTBEAT_EVERY_S:
            touch_job_lock(JOB_NAME, OWNER, PID)
            put_job_heartbeat(JOB_NAME, OWNER, PID)
            put_job_heartbeat(
                JOB_NAME,
                OWNER,
                PID,
                extra_json=json.dumps(
                    {
                        "predictions_age_s": round(pred_age_s, 1),
                        "labels_age_s": round(lbl_age_s, 1),
                    }
                ),
            )
            last_hb_s = now_s

        compute_and_store_drift()
        compute_and_store_distribution_drift()

        dur_ms = int(time.time() * 1000) - started_ms
        logging.info("drift computation complete dur_ms=%s", int(dur_ms))

    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as e:
            _warn_nonfatal("compute_drift_release_lock_failed", e)


if __name__ == "__main__":
    main()
