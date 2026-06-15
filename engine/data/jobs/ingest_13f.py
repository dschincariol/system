"""Disabled-by-default SEC 13F institutional holdings ingestion daemon.

README:
- Source: SEC EDGAR 13F-HR / 13F-HR/A filings for configured manager CIKs.
  The job fetches EDGAR submissions, resolves the filing archive information
  table XML, parses CUSIP/value/share rows, and stores unmapped holdings for
  review.
- Cadence: the source is quarterly; the supervised job polls daily by default
  via ``INST_13F_POLL_SECONDS`` during filing windows so individual manager
  filings are captured as they trickle in.
- Availability lag: holdings become usable only at the EDGAR acceptance
  timestamp of each filing, not at quarter end/report date.
- Caveats: CUSIP mapping is best-effort through local mappings/security-master
  tables plus optional Polygon/FMP reference lookups. Low-turnover screening is
  computed from each manager's own filing history before features contribute.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from engine.data.inst_13f import ingest_13f_batch
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

JOB_NAME = (os.environ.get("ENGINE_JOB_NAME") or "ingest_13f").strip() or "ingest_13f"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()

INGEST_ENABLED = os.environ.get("INGEST_13F_ENABLED", "0").strip().lower() in {"1", "true", "yes", "on"}
POLL_SECONDS = float(os.environ.get("INST_13F_POLL_SECONDS", "86400"))
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "300"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))
MAX_BACKOFF_SECONDS = float(os.environ.get("INST_13F_MAX_BACKOFF_SECONDS", "7200"))

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
    summary = ingest_13f_batch()
    errors = list(summary.get("errors") or [])
    ok = not bool(errors)
    last_ts = int(summary.get("last_ingested_ts_ms") or time.time() * 1000)
    status = record_pipeline_status(
        JOB_NAME,
        ok=bool(ok),
        raw_rows=int(summary.get("holdings") or 0),
        event_rows=0,
        last_ingested_ts_ms=int(last_ts),
        error="; ".join(str(err) for err in errors[:5]) if errors else None,
        meta={
            "managers": int(summary.get("managers") or 0),
            "filings": int(summary.get("filings") or 0),
            "holdings": int(summary.get("holdings") or 0),
            "written_filings": int(summary.get("written_filings") or 0),
            "written_holdings": int(summary.get("written_holdings") or 0),
            "poll_seconds": float(POLL_SECONDS),
        },
    )
    manager.record_job_status(
        JOB_NAME,
        ok=bool(ok),
        message="13F ingestion cycle complete" if ok else "13F ingestion cycle degraded",
        error="; ".join(str(err) for err in errors[:5]) if errors else None,
        meta={
            "managers": int(summary.get("managers") or 0),
            "filings": int(summary.get("filings") or 0),
            "holdings": int(summary.get("holdings") or 0),
            "errors": int(len(errors)),
        },
    )
    put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
    return bool(ok)


def main() -> None:
    """Run the supervised SEC 13F ingestion loop."""
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("ingest_13f must be launched by supervisor")
        raise SystemExit(1)

    manager = get_manager()
    if not INGEST_ENABLED:
        manager.record_job_status(JOB_NAME, ok=True, message="13F ingestion disabled by env flag")
        return
    if not manager.is_job_enabled(JOB_NAME, default=False):
        manager.record_job_status(JOB_NAME, ok=True, message="13F ingestion disabled by data source control plane")
        return

    init_db()
    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        raise SystemExit(2)

    last_hb_s = 0.0
    backoff_s = 1.0
    try:
        while True:
            if not manager.is_job_enabled(JOB_NAME, default=False):
                manager.record_job_status(JOB_NAME, ok=True, message="13F ingestion disabled by data source control plane")
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
                LOGGER.exception("inst_13f_cycle_failed")
                _warn_nonfatal("INGEST_13F_CYCLE_FAILED", exc, once_key="cycle_failed")
                status = record_pipeline_status(
                    JOB_NAME,
                    ok=False,
                    raw_rows=0,
                    event_rows=0,
                    last_ingested_ts_ms=int(time.time() * 1000),
                    error=str(exc),
                    meta={"poll_seconds": float(POLL_SECONDS)},
                )
                manager.record_job_status(JOB_NAME, ok=False, message="13F ingestion cycle failed", error=str(exc))
                put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
                backoff_s = min(float(MAX_BACKOFF_SECONDS), max(2.0, backoff_s * 2.0))
            time.sleep(max(1.0, min(float(POLL_SECONDS), float(backoff_s))))
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    main()
