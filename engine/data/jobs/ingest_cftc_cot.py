"""Disabled-by-default CFTC Commitments of Traders ingestion daemon.

README:
- Source: CFTC Public Reporting/Socrata API at ``publicreporting.cftc.gov``,
  using Legacy Futures Only and Disaggregated Futures Only report datasets for
  a configured contract list.
- Cadence: daily by default via ``CFTC_COT_POLL_SECONDS``; the source updates
  weekly, so extra polls are idempotent.
- Availability lag: reports represent Tuesday positions but are treated as
  available only at the Friday 3:30 p.m. ET release timestamp. Downstream
  features join on that release timestamp, never on the Tuesday report date.
- Caveats: holiday schedules can delay CFTC releases. The row-level availability
  timestamp is the PIT guardrail; COT is regime context, not standalone alpha.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from engine.data.cftc_cot import ingest_cot_batch
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

JOB_NAME = (os.environ.get("ENGINE_JOB_NAME") or "ingest_cftc_cot").strip() or "ingest_cftc_cot"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()

INGEST_ENABLED = os.environ.get("INGEST_CFTC_COT_ENABLED", "0").strip().lower() in {"1", "true", "yes", "on"}
POLL_SECONDS = float(os.environ.get("CFTC_COT_POLL_SECONDS", "86400"))
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "300"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))
MAX_BACKOFF_SECONDS = float(os.environ.get("CFTC_COT_MAX_BACKOFF_SECONDS", "3600"))

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
    summary = ingest_cot_batch()
    errors = list(summary.get("errors") or [])
    ok = not bool(errors)
    last_ts = int(summary.get("last_ingested_ts_ms") or time.time() * 1000)
    status = record_pipeline_status(
        JOB_NAME,
        ok=bool(ok),
        raw_rows=int(summary.get("rows") or 0),
        event_rows=0,
        last_ingested_ts_ms=int(last_ts),
        error="; ".join(str(err) for err in errors[:5]) if errors else None,
        meta={
            "rows": int(summary.get("rows") or 0),
            "written": int(summary.get("written") or 0),
            "errors": int(len(errors)),
            "poll_seconds": float(POLL_SECONDS),
        },
    )
    manager.record_job_status(
        JOB_NAME,
        ok=bool(ok),
        message="CFTC COT ingestion cycle complete" if ok else "CFTC COT ingestion cycle degraded",
        error="; ".join(str(err) for err in errors[:5]) if errors else None,
        meta={"rows": int(summary.get("rows") or 0), "written": int(summary.get("written") or 0), "errors": int(len(errors))},
    )
    put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
    return bool(ok)


def main() -> None:
    """Run the supervised CFTC COT ingestion loop."""
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("ingest_cftc_cot must be launched by supervisor")
        raise SystemExit(1)

    manager = get_manager()
    if not INGEST_ENABLED:
        manager.record_job_status(JOB_NAME, ok=True, message="CFTC COT ingestion disabled by env flag")
        return
    if not manager.is_job_enabled(JOB_NAME, default=False):
        manager.record_job_status(JOB_NAME, ok=True, message="CFTC COT ingestion disabled by data source control plane")
        return

    init_db()
    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        raise SystemExit(2)

    last_hb_s = 0.0
    backoff_s = 1.0
    try:
        while True:
            if not manager.is_job_enabled(JOB_NAME, default=False):
                manager.record_job_status(JOB_NAME, ok=True, message="CFTC COT ingestion disabled by data source control plane")
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
                LOGGER.exception("cftc_cot_cycle_failed")
                _warn_nonfatal("INGEST_CFTC_COT_CYCLE_FAILED", exc, once_key="cycle_failed")
                status = record_pipeline_status(
                    JOB_NAME,
                    ok=False,
                    raw_rows=0,
                    event_rows=0,
                    last_ingested_ts_ms=int(time.time() * 1000),
                    error=str(exc),
                    meta={"poll_seconds": float(POLL_SECONDS)},
                )
                manager.record_job_status(JOB_NAME, ok=False, message="CFTC COT ingestion cycle failed", error=str(exc))
                put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
                backoff_s = min(float(MAX_BACKOFF_SECONDS), max(2.0, backoff_s * 2.0))
            time.sleep(max(1.0, min(float(POLL_SECONDS), float(backoff_s))))
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    main()
