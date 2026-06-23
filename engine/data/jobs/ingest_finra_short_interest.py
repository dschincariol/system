"""Disabled-by-default FINRA equity short-interest ingestion daemon.

README:
- Source: FINRA public Query API ``EquityShortInterest`` dataset.
- Cadence: job polls once per day by default via
  ``FINRA_SHORT_INTEREST_POLL_SECONDS`` because FINRA disseminates the data
  on a bi-monthly schedule.
- Availability lag: features join on FINRA dissemination time, not settlement
  date; settlement data is invisible until the dissemination timestamp.
- Caveats: symbols and field names in the public API may drift. The parser is
  alias-tolerant and stores the raw payload for audit/reparse.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, List

from engine.data.default_symbols import parse_symbol_limit
from engine.data.finra_short import fetch_short_interest_records
from engine.data.universe import get_active_symbols
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.ingestion_status import record_pipeline_status
from engine.runtime.storage import (
    acquire_job_lock,
    connect,
    init_db,
    put_finra_short_interest,
    put_job_heartbeat,
    release_job_lock,
    run_write_txn,
    touch_job_lock,
)
from services.data_source_manager import get_manager

JOB_NAME = (os.environ.get("ENGINE_JOB_NAME") or "ingest_finra_short_interest").strip() or "ingest_finra_short_interest"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()

INGEST_ENABLED = os.environ.get("INGEST_FINRA_SHORT_INTEREST_ENABLED", "0").strip().lower() in {"1", "true", "yes", "on"}
POLL_SECONDS = float(os.environ.get("FINRA_SHORT_INTEREST_POLL_SECONDS", "86400"))
QUERY_LIMIT = max(1, int(os.environ.get("FINRA_SHORT_INTEREST_QUERY_LIMIT", "5000")))
MAX_PAGES = max(1, int(os.environ.get("FINRA_SHORT_INTEREST_MAX_PAGES", "20")))
SYMBOL_LIMIT = parse_symbol_limit(os.environ.get("FINRA_SHORT_INTEREST_SYMBOL_LIMIT"), 600)
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "300"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))
MAX_BACKOFF_SECONDS = float(os.environ.get("FINRA_SHORT_INTEREST_MAX_BACKOFF_SECONDS", "3600"))

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


def _load_symbols() -> List[str]:
    con = None
    try:
        con = connect(readonly=True)
        return list(dict.fromkeys(get_active_symbols(con, limit=SYMBOL_LIMIT)))
    except Exception as exc:
        _warn_nonfatal("INGEST_FINRA_SHORT_INTEREST_SYMBOL_LOAD_FAILED", exc, once_key="symbol_load")
        return []
    finally:
        if con is not None:
            try:
                con.close()
            except Exception as exc:
                _warn_nonfatal("INGEST_FINRA_SHORT_INTEREST_SYMBOL_CLOSE_FAILED", exc, once_key="symbol_close")


def _run_once() -> bool:
    manager = get_manager()
    symbols = _load_symbols()
    rows = fetch_short_interest_records(symbols=symbols or None, limit=int(QUERY_LIMIT), max_pages=int(MAX_PAGES))

    def _write(conw) -> int:
        written = 0
        for row in rows:
            written += int(put_finra_short_interest(row, con=conw) or 0)
        return int(written)

    written = 0
    if rows:
        written = int(
            run_write_txn(
                _write,
                table="finra_short_interest",
                operation="ingest_finra_short_interest",
                context={"job": JOB_NAME, "rows": int(len(rows))},
            )
            or 0
        )

    last_ts = max([int(row.get("availability_ts_ms") or row.get("ingested_ts_ms") or 0) for row in rows] or [int(time.time() * 1000)])
    ok = bool(rows)
    status = record_pipeline_status(
        JOB_NAME,
        ok=ok,
        raw_rows=int(len(rows)),
        event_rows=0,
        last_ingested_ts_ms=int(last_ts),
        error=None if ok else "finra_short_interest_empty_payload",
        meta={
            "rows": int(len(rows)),
            "written": int(written),
            "symbols": int(len(symbols)),
            "poll_seconds": float(POLL_SECONDS),
            "degraded": bool(not ok),
        },
    )
    manager.record_job_status(
        JOB_NAME,
        ok=ok,
        message="FINRA short-interest cycle complete" if ok else "FINRA short-interest cycle degraded",
        error="" if ok else "finra_short_interest_empty_payload",
        meta={"rows": int(len(rows)), "written": int(written), "symbols": int(len(symbols)), "degraded": bool(not ok)},
    )
    put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
    return bool(ok)


def main() -> None:
    """Run the supervised FINRA short-interest ingestion loop."""
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("ingest_finra_short_interest must be launched by supervisor")
        raise SystemExit(1)

    manager = get_manager()
    if not INGEST_ENABLED:
        manager.record_job_status(JOB_NAME, ok=True, message="FINRA short-interest disabled by env flag")
        return
    if not manager.is_job_enabled(JOB_NAME, default=False):
        manager.record_job_status(JOB_NAME, ok=True, message="FINRA short-interest disabled by data source control plane")
        return

    init_db()
    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        raise SystemExit(2)

    last_hb_s = 0.0
    backoff_s = 1.0
    try:
        while True:
            if not manager.is_job_enabled(JOB_NAME, default=False):
                manager.record_job_status(JOB_NAME, ok=True, message="FINRA short-interest disabled by data source control plane")
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
                backoff_s = 1.0
            except Exception as exc:
                LOGGER.exception("finra_short_interest_cycle_failed")
                _warn_nonfatal("INGEST_FINRA_SHORT_INTEREST_CYCLE_FAILED", exc, once_key="cycle_failed")
                status = record_pipeline_status(
                    JOB_NAME,
                    ok=False,
                    raw_rows=0,
                    event_rows=0,
                    last_ingested_ts_ms=int(time.time() * 1000),
                    error=str(exc),
                    meta={"poll_seconds": float(POLL_SECONDS)},
                )
                manager.record_job_status(JOB_NAME, ok=False, message="FINRA short-interest cycle failed", error=str(exc))
                put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
                backoff_s = min(float(MAX_BACKOFF_SECONDS), max(2.0, backoff_s * 2.0))
            time.sleep(max(1.0, min(float(POLL_SECONDS), float(backoff_s))))
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    main()
