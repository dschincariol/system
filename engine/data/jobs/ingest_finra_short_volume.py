"""Disabled-by-default FINRA daily short-sale volume ingestion daemon.

README:
- Source: FINRA consolidated daily short-sale volume files at
  ``https://cdn.finra.org/equity/regsho/daily/CNMSshvol{YYYYMMDD}.txt``.
- Cadence: job polls every ``FINRA_SHORT_VOLUME_POLL_SECONDS`` seconds
  (default six hours) because FINRA publishes trading-day files around
  6 p.m. ET.
- Availability lag: rows are timestamped at the file publication evening;
  features for day t intentionally use day t-1 and earlier files only.
- Caveats: daily files cover off-exchange TRF/ADF/ORF volume only. Use
  relative/z-scored forms, not absolute short-volume levels.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

from engine.data.finra_short import fetch_short_volume_file
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.ingestion_status import record_pipeline_status
from engine.runtime.storage import (
    acquire_job_lock,
    init_db,
    put_finra_short_sale_volume,
    put_job_heartbeat,
    release_job_lock,
    run_write_txn,
    touch_job_lock,
)
from services.data_source_manager import get_manager

JOB_NAME = (os.environ.get("ENGINE_JOB_NAME") or "ingest_finra_short_volume").strip() or "ingest_finra_short_volume"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()

INGEST_ENABLED = os.environ.get("INGEST_FINRA_SHORT_VOLUME_ENABLED", "0").strip().lower() in {"1", "true", "yes", "on"}
POLL_SECONDS = float(os.environ.get("FINRA_SHORT_VOLUME_POLL_SECONDS", "21600"))
BACKFILL_DAYS = max(1, int(os.environ.get("FINRA_SHORT_VOLUME_BACKFILL_DAYS", "10")))
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "300"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))
MAX_BACKOFF_SECONDS = float(os.environ.get("FINRA_SHORT_VOLUME_MAX_BACKOFF_SECONDS", "3600"))

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


def _candidate_dates(backfill_days: int) -> List[str]:
    today = datetime.now(tz=ZoneInfo("America/New_York")).date()
    return [(today - timedelta(days=offset)).isoformat() for offset in range(max(1, int(backfill_days)))]


def _fetch_rows() -> tuple[List[Dict[str, Any]], List[str]]:
    rows: List[Dict[str, Any]] = []
    errors: List[str] = []
    for day in _candidate_dates(BACKFILL_DAYS):
        try:
            rows.extend(fetch_short_volume_file(day))
        except Exception as exc:
            errors.append(f"{day}: {exc}")
            _warn_nonfatal(
                "INGEST_FINRA_SHORT_VOLUME_FETCH_FAILED",
                exc,
                once_key=f"short_volume_fetch:{day}",
                trade_date=str(day),
            )
            if bool(getattr(exc, "stop_cycle", False)):
                break
    return rows, errors


def _run_once() -> bool:
    manager = get_manager()
    rows, errors = _fetch_rows()

    def _write(conw) -> int:
        written = 0
        for row in rows:
            written += int(put_finra_short_sale_volume(row, con=conw) or 0)
        return int(written)

    written = 0
    if rows:
        written = int(
            run_write_txn(
                _write,
                table="finra_short_sale_volume",
                operation="ingest_finra_short_sale_volume",
                context={"job": JOB_NAME, "rows": int(len(rows))},
            )
            or 0
        )

    ok = bool(rows) and not bool(errors)
    last_ts = max([int(row.get("availability_ts_ms") or row.get("ingested_ts_ms") or 0) for row in rows] or [int(time.time() * 1000)])
    status = record_pipeline_status(
        JOB_NAME,
        ok=ok,
        raw_rows=int(len(rows)),
        event_rows=0,
        last_ingested_ts_ms=int(last_ts),
        error="; ".join(errors[:5]) if errors else None,
        meta={
            "rows": int(len(rows)),
            "written": int(written),
            "backfill_days": int(BACKFILL_DAYS),
            "poll_seconds": float(POLL_SECONDS),
            "degraded": bool(not ok),
        },
    )
    manager.record_job_status(
        JOB_NAME,
        ok=ok,
        message="FINRA short-sale volume cycle complete" if ok else "FINRA short-sale volume cycle degraded",
        error="; ".join(errors[:5]) if errors else None,
        meta={"rows": int(len(rows)), "written": int(written), "errors": int(len(errors))},
    )
    put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
    return bool(ok)


def main() -> None:
    """Run the supervised FINRA short-sale volume ingestion loop."""
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("ingest_finra_short_volume must be launched by supervisor")
        raise SystemExit(1)

    manager = get_manager()
    if not INGEST_ENABLED:
        manager.record_job_status(JOB_NAME, ok=True, message="FINRA short-sale volume disabled by env flag")
        return
    if not manager.is_job_enabled(JOB_NAME, default=False):
        manager.record_job_status(JOB_NAME, ok=True, message="FINRA short-sale volume disabled by data source control plane")
        return

    init_db()
    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        raise SystemExit(2)

    last_hb_s = 0.0
    backoff_s = 1.0
    try:
        while True:
            if not manager.is_job_enabled(JOB_NAME, default=False):
                manager.record_job_status(JOB_NAME, ok=True, message="FINRA short-sale volume disabled by data source control plane")
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
                LOGGER.exception("finra_short_volume_cycle_failed")
                _warn_nonfatal("INGEST_FINRA_SHORT_VOLUME_CYCLE_FAILED", exc, once_key="cycle_failed")
                status = record_pipeline_status(
                    JOB_NAME,
                    ok=False,
                    raw_rows=0,
                    event_rows=0,
                    last_ingested_ts_ms=int(time.time() * 1000),
                    error=str(exc),
                    meta={"poll_seconds": float(POLL_SECONDS)},
                )
                manager.record_job_status(JOB_NAME, ok=False, message="FINRA short-sale volume cycle failed", error=str(exc))
                put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
                backoff_s = min(float(MAX_BACKOFF_SECONDS), max(2.0, backoff_s * 2.0))
            time.sleep(max(1.0, min(float(POLL_SECONDS), float(backoff_s))))
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    main()
