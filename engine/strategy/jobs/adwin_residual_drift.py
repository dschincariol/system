"""One-shot champion residual ADWIN drift update job."""

from __future__ import annotations

import json
import os

from engine.runtime.storage import (
    acquire_job_lock,
    init_db,
    put_job_heartbeat,
    release_job_lock,
    touch_job_lock,
)
from engine.strategy.adwin_drift import run_adwin_residual_drift


JOB_NAME = "adwin_residual_drift"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))


def main() -> int:
    init_db()
    result = run_adwin_residual_drift(enqueue_retrain=True)
    print(json.dumps(result, separators=(",", ":"), sort_keys=True, default=str))
    return 0 if bool(result.get("ok")) else 1


if __name__ == "__main__":
    init_db()
    if not acquire_job_lock(JOB_NAME, OWNER, PID, stale_after_s=LOCK_STALE_AFTER_S):
        raise SystemExit(0)

    try:
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(
            JOB_NAME,
            OWNER,
            PID,
            extra_json=json.dumps({"phase": "start"}, separators=(",", ":"), sort_keys=True),
        )
        rc = int(main() or 0)
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(
            JOB_NAME,
            OWNER,
            PID,
            extra_json=json.dumps({"phase": "done", "rc": rc}, separators=(",", ":"), sort_keys=True),
        )
        raise SystemExit(rc)
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)
