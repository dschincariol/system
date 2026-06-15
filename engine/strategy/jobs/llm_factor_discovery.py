"""Registered research job for LLM-proposed factor hypotheses.

The job is research-side only: it writes discovered features as experimental
shadow records and never imports or calls execution modules.
"""

from __future__ import annotations

import json
import os

from engine.strategy.discovery.llm_factor_generator import run_llm_factor_discovery

JOB_NAME = "llm_factor_discovery"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))


def main() -> int:
    from engine.runtime.storage import acquire_job_lock, init_db, put_job_heartbeat, release_job_lock, touch_job_lock

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
        summary = run_llm_factor_discovery()
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(
            JOB_NAME,
            OWNER,
            PID,
            extra_json=json.dumps({"phase": "done", **dict(summary)}, separators=(",", ":"), sort_keys=True),
        )
        print(json.dumps(summary, separators=(",", ":"), sort_keys=True))
        return 0
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    raise SystemExit(main())
