"""
FILE: compute_drift.py

Data job entrypoint for `compute_drift`.
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
from engine.strategy.production_monitoring import compute_and_store_production_monitoring

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
        component="engine.data.jobs.compute_drift",
        extra=extra,
        persist=False,
    )


def _latest_age_s(con, table: str) -> float | None:
    try:
        row = con.execute(f"SELECT MAX(ts_ms) FROM {table}").fetchone()
    except Exception as e:
        _warn_nonfatal("compute_drift_latest_age_failed", e, table=str(table))
        return None
    if not row or not row[0]:
        return None
    return (int(time.time() * 1000) - int(row[0])) / 1000.0


def main():
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("compute_drift must be launched by supervisor")
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
            log_failure(
                logging.getLogger("compute_drift"),
                event="compute_drift_predictions_stale",
                code="COMPUTE_DRIFT_PREDICTIONS_STALE",
                message="skipping drift: predictions stale or missing",
                level=logging.WARNING,
                component="engine.data.jobs.compute_drift",
                extra={
                    "predictions_age_s": pred_age_s,
                    "max_predictions_age_s": MAX_PREDICTIONS_AGE_S,
                },
                persist=False,
            )
            return

        if lbl_age_s is None or lbl_age_s > MAX_LABELS_AGE_S:
            log_failure(
                logging.getLogger("compute_drift"),
                event="compute_drift_labels_stale",
                code="COMPUTE_DRIFT_LABELS_STALE",
                message="skipping drift: labels stale or missing",
                level=logging.WARNING,
                component="engine.data.jobs.compute_drift",
                extra={
                    "labels_age_s": lbl_age_s,
                    "max_labels_age_s": MAX_LABELS_AGE_S,
                },
                persist=False,
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
        production_monitoring = compute_and_store_production_monitoring(con=con, emit_signals=True)
        con.commit()

        dur_ms = int(time.time() * 1000) - started_ms
        logging.info(
            "drift computation complete dur_ms=%s production_monitoring_state=%s signals=%s",
            int(dur_ms),
            str(((production_monitoring.get("status") or {}).get("state"))),
            int(len(production_monitoring.get("signals") or [])),
        )

    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as e:
            _warn_nonfatal("compute_drift_release_lock_failed", e)


if __name__ == "__main__":
    main()
