# FILE: universe_discovery_job.py
# NEW FILE (CREATE):

# universe_discovery_job.py
"""
Runs the Universe Discovery Engine once.

Recommended schedule:
  - every 1–5 minutes (live)
  - every 15 minutes (paper)

Writes summary JSON to stdout.
"""

import json
import os
import sys
import time

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect, init_db, acquire_job_lock, release_job_lock
from engine.data.universe_discovery import discover_universe_once

JOB_NAME = "universe_discovery"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "120"))
LOG = get_logger("engine.strategy.jobs.universe_discovery_job")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.strategy.jobs.universe_discovery_job",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _print(obj):
    sys.stdout.write(json.dumps(obj, sort_keys=True) + "\n")
    sys.stdout.flush()


def main() -> int:
    con = connect()
    try:
        init_db()
        if not acquire_job_lock(JOB_NAME, OWNER, PID, stale_after_s=LOCK_STALE_AFTER_S):
            _print({"ok": True, "status": "locked_out", "job": JOB_NAME})
            return 0

        started = int(time.time() * 1000)
        res = discover_universe_once(con=con, ts_ms=started)
        _print({"ok": True, "status": "done", "job": JOB_NAME, "result": res, "dur_ms": int(time.time() * 1000) - started})
        return 0
    except Exception as e:
        _print({"ok": False, "status": "error", "job": JOB_NAME, "error": str(e)})
        return 2
    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as e:
            _warn_nonfatal("UNIVERSE_DISCOVERY_RELEASE_LOCK_FAILED", e, once_key="release_lock", job_name=JOB_NAME)
        try:
            con.commit()
        except Exception as e:
            _warn_nonfatal("UNIVERSE_DISCOVERY_COMMIT_FAILED", e, once_key="commit", job_name=JOB_NAME)
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("UNIVERSE_DISCOVERY_CLOSE_FAILED", e, once_key="close", job_name=JOB_NAME)


if __name__ == "__main__":
    raise SystemExit(main())
