"""One-shot job wrapper for model lifecycle planning, retraining, and retirement."""

import json
import os

from engine.runtime.storage import (
    acquire_job_lock,
    init_db,
    put_job_heartbeat,
    release_job_lock,
    touch_job_lock,
)
from engine.strategy.model_lifecycle import run_model_lifecycle_job

JOB_NAME = "model_lifecycle_manager"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))


def main() -> int:
    init_db()
    if not acquire_job_lock(JOB_NAME, OWNER, PID, stale_after_s=LOCK_STALE_AFTER_S):
        return 0
    try:
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(
            JOB_NAME,
            OWNER,
            PID,
            extra_json=json.dumps({"phase": "start"}, separators=(",", ":"), sort_keys=True),
        )
        result = run_model_lifecycle_job()
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(
            JOB_NAME,
            OWNER,
            PID,
            extra_json=json.dumps(
                {"phase": "done", "ok": bool(result.get("ok")), "retired": len(result.get("retired_versions") or [])},
                separators=(",", ":"),
                sort_keys=True,
            ),
        )
        print(json.dumps(result, separators=(",", ":"), sort_keys=True))
        return 0
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    raise SystemExit(main())
