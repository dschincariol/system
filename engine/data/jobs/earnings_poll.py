"""
FILE: earnings_poll.py

Job entrypoint or scheduled task for `earnings_poll`.
"""

import json
import os
import time
import logging
from datetime import date, timedelta

from engine.runtime.ingestion_status import record_pipeline_status
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import (
    init_db,
    acquire_job_lock,
    release_job_lock,
    touch_job_lock,
    put_job_heartbeat,
    put_normalized_event,
    run_write_txn,
)
from engine.data.event_normalization import normalize_earnings_event
from engine.data.calendar.fmp_earnings import fetch_earnings_calendar
from services.data_source_manager import get_manager

JOB_NAME = "poll_earnings"
OWNER = os.environ.get("JOB_OWNER", "system")
PID = os.getpid()

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "300"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))
POLL_SECONDS = float(os.environ.get("EARNINGS_POLL_SECONDS", "21600.0"))

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [earnings_poll] %(message)s",
)

LOOKAHEAD_DAYS = int(os.environ.get("EARNINGS_LOOKAHEAD_DAYS", "21"))
LOGGER = logging.getLogger(__name__)
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
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


def _run_once():
    manager = get_manager()
    ts_ms = int(time.time() * 1000)
    d0 = date.today()
    d1 = d0 + timedelta(days=LOOKAHEAD_DAYS)

    from_date = d0.isoformat()
    to_date = d1.isoformat()

    # Earnings data is advisory context for event scoring and risk adjustments.
    # Missing rows should degrade features, not stall the rest of the pipeline.
    items = []
    try:
        items = fetch_earnings_calendar(from_date, to_date)
    except Exception:
        items = []

    try:
        upserts = 0
        event_rows = 0
        errors = []
        def _write(conw):
            local_upserts = 0
            local_event_rows = 0
            local_errors = []
            for it in items or []:
                sym = ""
                dt = ""
                try:
                    sym = str(it.get("symbol") or "").upper().strip()
                    dt = str(it.get("date") or "").strip()
                    if not sym or not dt:
                        continue

                    tod = str(it.get("time") or "unknown").lower().strip()

                    conw.execute(
                        """
                        INSERT OR REPLACE INTO earnings_calendar(
                          symbol, earnings_date, time_of_day,
                          eps_est, eps_act, revenue_est, revenue_act,
                          source, updated_ts_ms
                        )
                        VALUES (?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            sym,
                            dt,
                            tod,
                            it.get("epsEstimated"),
                            it.get("eps"),
                            it.get("revenueEstimated"),
                            it.get("revenue"),
                            "fmp",
                            int(ts_ms),
                        ),
                    )
                    put_normalized_event(
                        normalize_earnings_event(
                            {
                                "ts_ms": int(ts_ms),
                                "source": "earnings_calendar",
                                "symbol": sym,
                                "earnings_date": dt,
                                "time_of_day": tod,
                                "eps_est": it.get("epsEstimated"),
                                "eps_act": it.get("eps"),
                                "revenue_est": it.get("revenueEstimated"),
                                "revenue_act": it.get("revenue"),
                                "title": f"{sym} earnings scheduled",
                                "body": f"{sym} earnings date {dt} ({tod})",
                                "event_key": f"earnings:{sym}:{dt}",
                                "source_id": f"{sym}:{dt}",
                            }
                        ),
                        con=conw,
                    )
                    local_upserts += 1
                    local_event_rows += 1
                except Exception as e:
                    local_errors.append(str(e))
                    _warn_nonfatal("EARNINGS_POLL_UPSERT_FAILED", e, once_key=f"upsert:{sym}:{dt}", symbol=sym, earnings_date=dt)
                    continue
            return local_upserts, local_event_rows, local_errors

        upserts, event_rows, errors = run_write_txn(
            _write,
            table="earnings_calendar",
            operation="ingest_earnings_batch",
            context={"job": JOB_NAME, "items": int(len(items or []))},
        )
        status = record_pipeline_status(
            JOB_NAME,
            ok=(len(errors) == 0),
            raw_rows=upserts,
            event_rows=event_rows,
            last_ingested_ts_ms=ts_ms,
            error=("; ".join(errors[:3])) if errors else None,
            meta={"window": f"{from_date}..{to_date}", "lookahead_days": int(LOOKAHEAD_DAYS)},
        )
        manager.record_job_status(
            JOB_NAME,
            ok=bool(len(errors) == 0),
            message="earnings cycle complete",
            error=("; ".join(errors[:3])) if errors else "",
            meta={"window": f"{from_date}..{to_date}", "upserts": int(upserts), "event_rows": int(event_rows)},
        )
        put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
        logging.info("earnings cycle ok=%s upserts=%s window=%s..%s", len(errors) == 0, upserts, from_date, to_date)
    finally:
        logging.debug("earnings_poll cycle_complete")


def main():
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("options_poll must be launched by supervisor")
        raise SystemExit(1)

    manager = get_manager()
    if not manager.is_job_enabled(JOB_NAME, default=True):
        manager.record_job_status(JOB_NAME, ok=True, message="earnings disabled by data source control plane")
        return

    init_db()

    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        raise SystemExit(2)

    last_hb_s = 0.0
    try:
        while True:
            if not manager.is_job_enabled(JOB_NAME, default=True):
                manager.record_job_status(JOB_NAME, ok=True, message="earnings disabled by data source control plane")
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
                _run_once()
            except Exception as e:
                logging.exception("earnings_cycle_failed")
                status = record_pipeline_status(
                    JOB_NAME,
                    ok=False,
                    raw_rows=0,
                    event_rows=0,
                    last_ingested_ts_ms=int(time.time() * 1000),
                    error=str(e),
                    meta={"poll_seconds": float(POLL_SECONDS)},
                )
                manager.record_job_status(JOB_NAME, ok=False, message="earnings cycle failed", error=str(e), meta={"poll_seconds": float(POLL_SECONDS)})
                put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
            time.sleep(max(60.0, float(POLL_SECONDS)))
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    main()
