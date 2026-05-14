"""One-shot job wrapper for CPCV/PBO backtesting."""

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
from engine.strategy.cpcv import run_backtest_cpcv_job


JOB_NAME = "backtest_cpcv"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))


def main() -> int:
    """Run one CPCV backtest job invocation and return a process exit code."""
    init_db()
    model_name = str(
        os.environ.get(
            "CPCV_MODEL_NAME",
            os.environ.get("MODEL_V2_NAME", "regime_stats_v2"),
        )
        or "regime_stats_v2"
    ).strip()
    candidate_version = str(
        os.environ.get(
            "CPCV_CANDIDATE_VERSION",
            os.environ.get("CANDIDATE_VERSION", os.environ.get("MODEL_VERSION", "")),
        )
        or ""
    ).strip()

    result = run_backtest_cpcv_job(
        model_name=model_name,
        candidate_version=candidate_version,
    )
    print(json.dumps(result, separators=(",", ":"), sort_keys=True))
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
