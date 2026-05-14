"""
FILE: snapshot_equity.py

Job entrypoint or scheduled task for `snapshot_equity`.
"""

import json
import os
import time

from engine.data.equity_snapshot import snapshot_equity
from engine.runtime.storage import (
    acquire_job_lock,
    init_db,
    put_job_heartbeat,
    release_job_lock,
    touch_job_lock,
)


JOB_NAME = "snapshot_equity"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()
INTERVAL_S = float(os.environ.get("SNAPSHOT_EQUITY_INTERVAL_S", "60"))
HEARTBEAT_EVERY_S = float(os.environ.get("SNAPSHOT_EQUITY_HEARTBEAT_S", "15"))
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))


def _run_once() -> int:
    ok = snapshot_equity()
    print(f"[equity_snapshot] ok={ok}")
    return 0 if ok else 1


def main():
    init_db()

    run_once = str(os.environ.get("SNAPSHOT_EQUITY_RUN_ONCE", "0") or "0").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    if os.environ.get("ENGINE_SUPERVISED") != "1" or run_once:
        return _run_once()

    if not acquire_job_lock(JOB_NAME, OWNER, PID, stale_after_s=LOCK_STALE_AFTER_S):
        print("snapshot_equity lock already held")
        return 2

    last_hb_s = 0.0
    try:
        while True:
            now_s = time.time()
            if (now_s - last_hb_s) >= HEARTBEAT_EVERY_S:
                last_hb_s = now_s
                touch_job_lock(JOB_NAME, OWNER, PID)
                put_job_heartbeat(
                    JOB_NAME,
                    OWNER,
                    PID,
                    extra_json=json.dumps(
                        {
                            "interval_s": float(INTERVAL_S),
                            "heartbeat_every_s": float(HEARTBEAT_EVERY_S),
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                )

            _run_once()
            time.sleep(max(1.0, float(INTERVAL_S)))
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    raise SystemExit(main())
