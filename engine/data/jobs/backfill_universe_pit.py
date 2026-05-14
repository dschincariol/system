"""
Offline backfill job for point-in-time universe lifecycle rows.
"""

from __future__ import annotations

import json
import logging
import os

from engine.data.universe_pit import backfill_universe_pit, pit_universe_backfill_enabled
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import (
    acquire_job_lock,
    connect,
    init_db,
    put_job_heartbeat,
    release_job_lock,
)

JOB_NAME = "backfill_universe_pit"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "300"))

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [backfill_universe_pit] %(message)s",
)
LOG = get_logger("engine.data.jobs.backfill_universe_pit")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(code),
        error=error,
        level=logging.WARNING,
        component="engine.data.jobs.backfill_universe_pit",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def main() -> int:
    """Run the PIT universe backfill job once and emit a JSON summary."""
    init_db()
    if not pit_universe_backfill_enabled():
        print("backfill_universe_pit: disabled (PIT_UNIVERSE_BACKFILL_ENABLED=0)")
        return 0

    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        logging.error("another instance is holding the job lock; exiting")
        return 2

    con = connect()
    try:
        summary = backfill_universe_pit(con)
        con.commit()
        try:
            put_job_heartbeat(
                JOB_NAME,
                OWNER,
                PID,
                extra_json=json.dumps(summary, separators=(",", ":"), sort_keys=True),
            )
        except Exception as e:
            _warn_nonfatal("BACKFILL_UNIVERSE_PIT_HEARTBEAT_FAILED", e, once_key="heartbeat")
        logging.info("PIT universe backfill complete summary=%s", json.dumps(summary, separators=(",", ":"), sort_keys=True))
        print(json.dumps(summary, separators=(",", ":"), sort_keys=True))
        return 0
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("BACKFILL_UNIVERSE_PIT_CLOSE_FAILED", e, once_key="close")
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as e:
            _warn_nonfatal("BACKFILL_UNIVERSE_PIT_RELEASE_LOCK_FAILED", e, once_key="release_lock")


if __name__ == "__main__":
    raise SystemExit(main())
