"""Disabled-by-default Quiver government-flow ingestion daemon.

README:
- Source: Quiver Quantitative Tier-1 API for congressional trading, lobbying
  spend, and government contract awards.
- Cadence: daily by default via ``QUIVER_GOV_POLL_SECONDS``. The provider feeds
  are disclosure-driven and idempotently upserted by source record id.
- Availability lag: congressional trades enter features only at disclosure
  date/time. Lobbying and contract awards enter at Quiver disclosure/publish
  time. Transaction/award dates are stored but never used as availability.
- Caveats: without ``QUIVER_API_KEY`` the job reports a blocked-but-healthy
  status and does no network work. The existing free congressional ingestion
  remains the fallback source and the gov feature resolver dedupes on
  member/ticker/transaction date.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from engine.data.quiver_gov import ingest_quiver_gov_batch
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.ingestion_status import record_pipeline_status
from engine.runtime.storage import (
    acquire_job_lock,
    init_db,
    put_job_heartbeat,
    release_job_lock,
    touch_job_lock,
)
from services.data_source_manager import get_manager

JOB_NAME = (os.environ.get("ENGINE_JOB_NAME") or "ingest_quiver_gov").strip() or "ingest_quiver_gov"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()

INGEST_ENABLED = os.environ.get("INGEST_QUIVER_GOV_ENABLED", "0").strip().lower() in {"1", "true", "yes", "on"}
POLL_SECONDS = float(os.environ.get("QUIVER_GOV_POLL_SECONDS", "86400"))
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "300"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))
MAX_BACKOFF_SECONDS = float(os.environ.get("QUIVER_GOV_MAX_BACKOFF_SECONDS", "3600"))

logging.basicConfig(
    level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format=f"%(asctime)s %(levelname)s [{JOB_NAME}] %(message)s",
)
LOGGER = logging.getLogger(__name__)
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOGGER,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component=__name__,
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _run_once() -> bool:
    manager = get_manager()
    summary = ingest_quiver_gov_batch()
    errors = list(summary.get("errors") or [])
    blocked = bool(summary.get("blocked"))
    ok = bool(summary.get("ok")) or blocked
    raw_rows = int(summary.get("congress_rows") or 0) + int(summary.get("lobbying_rows") or 0) + int(summary.get("contract_rows") or 0)
    status = record_pipeline_status(
        JOB_NAME,
        ok=bool(ok),
        raw_rows=int(raw_rows),
        event_rows=0,
        last_ingested_ts_ms=int(summary.get("last_ingested_ts_ms") or time.time() * 1000),
        error="; ".join(str(err) for err in errors[:5]) if errors and not blocked else None,
        meta={
            "blocked": bool(blocked),
            "blocker": str(summary.get("blocker") or ""),
            "congress_rows": int(summary.get("congress_rows") or 0),
            "lobbying_rows": int(summary.get("lobbying_rows") or 0),
            "contract_rows": int(summary.get("contract_rows") or 0),
            "written": int(summary.get("written") or 0),
            "errors": int(len(errors)),
            "poll_seconds": float(POLL_SECONDS),
        },
    )
    manager.record_job_status(
        JOB_NAME,
        ok=bool(ok),
        message=(
            "Quiver gov ingestion blocked: QUIVER_API_KEY missing"
            if blocked
            else ("Quiver gov ingestion cycle complete" if ok else "Quiver gov ingestion cycle degraded")
        ),
        error=None if blocked else ("; ".join(str(err) for err in errors[:5]) if errors else None),
        meta={
            "blocked": bool(blocked),
            "congress_rows": int(summary.get("congress_rows") or 0),
            "lobbying_rows": int(summary.get("lobbying_rows") or 0),
            "contract_rows": int(summary.get("contract_rows") or 0),
            "written": int(summary.get("written") or 0),
            "errors": int(len(errors)),
        },
    )
    put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
    return bool(ok)


def main() -> None:
    """Run the supervised Quiver government-flow ingestion loop."""
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("ingest_quiver_gov must be launched by supervisor")
        raise SystemExit(1)

    manager = get_manager()
    if not INGEST_ENABLED:
        manager.record_job_status(JOB_NAME, ok=True, message="Quiver gov ingestion disabled by env flag")
        return
    if not manager.is_job_enabled(JOB_NAME, default=False):
        manager.record_job_status(JOB_NAME, ok=True, message="Quiver gov ingestion disabled by data source control plane")
        return

    init_db()
    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        raise SystemExit(2)

    last_hb_s = 0.0
    backoff_s = 1.0
    try:
        while True:
            if not manager.is_job_enabled(JOB_NAME, default=False):
                manager.record_job_status(JOB_NAME, ok=True, message="Quiver gov ingestion disabled by data source control plane")
                break
            now_s = time.time()
            if (now_s - last_hb_s) >= HEARTBEAT_EVERY_S:
                touch_job_lock(JOB_NAME, OWNER, PID)
                put_job_heartbeat(
                    JOB_NAME,
                    OWNER,
                    PID,
                    extra_json=json.dumps({"poll_seconds": float(POLL_SECONDS)}, separators=(",", ":"), sort_keys=True),
                )
                last_hb_s = now_s
            try:
                ok = _run_once()
                backoff_s = 1.0 if ok else min(float(MAX_BACKOFF_SECONDS), max(2.0, backoff_s * 2.0))
            except Exception as exc:
                LOGGER.exception("quiver_gov_cycle_failed")
                _warn_nonfatal("INGEST_QUIVER_GOV_CYCLE_FAILED", exc, once_key="cycle_failed")
                status = record_pipeline_status(
                    JOB_NAME,
                    ok=False,
                    raw_rows=0,
                    event_rows=0,
                    last_ingested_ts_ms=int(time.time() * 1000),
                    error=str(exc),
                    meta={"poll_seconds": float(POLL_SECONDS)},
                )
                manager.record_job_status(JOB_NAME, ok=False, message="Quiver gov ingestion cycle failed", error=str(exc))
                put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
                backoff_s = min(float(MAX_BACKOFF_SECONDS), max(2.0, backoff_s * 2.0))
            time.sleep(max(1.0, min(float(POLL_SECONDS), float(backoff_s))))
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    main()
