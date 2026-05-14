"""
FILE: sec_poll.py

Job entrypoint or scheduled task for `sec_poll`.
"""

import os
import time
import json
import logging

from engine.data.default_symbols import parse_symbol_limit
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.ingestion_status import record_pipeline_status
from engine.runtime.storage import (
    connect,
    init_db,
    acquire_job_lock,
    release_job_lock,
    touch_job_lock,
    put_job_heartbeat,
    put_normalized_event,
    run_write_txn,
)

from engine.data.event_normalization import normalize_filings_event
from engine.data.universe import get_active_symbols
from engine.data.sec.edgar_live import fetch_recent_filings
from services.data_source_manager import get_manager

JOB_NAME = "poll_sec_filings"
OWNER = os.environ.get("JOB_OWNER", "system")
PID = os.getpid()

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "300"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))
POLL_SECONDS = float(os.environ.get("SEC_POLL_SECONDS", "900.0"))

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [sec_poll] %(message)s",
)

FORMS_ALLOW = set(
    f.strip().upper()
    for f in os.environ.get("SEC_FORMS_ALLOW", "8-K,10-Q,10-K,6-K,S-1,424B2,424B3,13D,13G,4").split(",")
    if f.strip()
)

SYMBOL_LIMIT = parse_symbol_limit(os.environ.get("SEC_SYMBOL_LIMIT"), 600)
PER_SYMBOL_LIMIT = int(os.environ.get("SEC_PER_SYMBOL_LIMIT", "25"))
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

    con = connect()
    try:
        syms = get_active_symbols(con, limit=SYMBOL_LIMIT)
    finally:
        con.close()

    try:
        upserts = 0
        event_rows = 0
        last_ingested_ts_ms = ts_ms
        errors = []
        def _write(conw):
            local_upserts = 0
            local_event_rows = 0
            local_errors = []
            for sym in syms:
                try:
                    filings = fetch_recent_filings(sym, limit=PER_SYMBOL_LIMIT)
                except Exception as e:
                    local_errors.append(f"{sym}:{e}")
                    _warn_nonfatal("SEC_POLL_FETCH_FAILED", e, once_key=f"fetch:{sym}", symbol=sym)
                    continue

                if not filings:
                    continue

                rows = []
                for f in filings:
                    form = str(f.get("form") or "").upper()
                    accession = str(f.get("accession") or "").strip()
                    if FORMS_ALLOW and form and (form not in FORMS_ALLOW):
                        continue
                    if not accession:
                        continue

                    rows.append(
                        (
                            sym,
                            accession,
                            form,
                            f.get("filed_date"),
                            f.get("report_date"),
                            f.get("cik"),
                            f.get("company_name"),
                            f.get("primary_doc_url"),
                            None,
                            "sec",
                            ts_ms,
                        )
                    )
                    put_normalized_event(
                        normalize_filings_event(
                            {
                                "ts_ms": ts_ms,
                                "source": "sec",
                                "symbol": sym,
                                "accession": accession,
                                "form": form,
                                "filed_date": f.get("filed_date"),
                                "report_date": f.get("report_date"),
                                "cik": f.get("cik"),
                                "company_name": f.get("company_name"),
                                "primary_doc_url": f.get("primary_doc_url"),
                                "title": f"{sym} {form} filing",
                                "body": str(f.get("company_name") or ""),
                                "url": f.get("primary_doc_url"),
                                "event_key": f"sec:{accession}",
                                "source_id": accession,
                            }
                        ),
                        con=conw,
                    )
                    local_event_rows += 1

                if not rows:
                    continue

                conw.executemany(
                    """
                    INSERT OR REPLACE INTO sec_filings(
                      symbol, accession, form, filed_date, report_date,
                      cik, company_name, primary_doc_url,
                      items_json, source, ts_ms
                    )
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    rows,
                )
                local_upserts += len(rows)
            return local_upserts, local_event_rows, local_errors

        upserts, event_rows, errors = run_write_txn(
            _write,
            table="sec_filings",
            operation="ingest_sec_batch",
            context={"job": JOB_NAME, "symbols": int(len(syms))},
        )
        status = record_pipeline_status(
            JOB_NAME,
            ok=(len(errors) == 0),
            raw_rows=upserts,
            event_rows=event_rows,
            last_ingested_ts_ms=last_ingested_ts_ms,
            error=("; ".join(errors[:3])) if errors else None,
            meta={"symbols_n": len(syms), "forms_allow": sorted(FORMS_ALLOW)},
        )
        manager.record_job_status(
            JOB_NAME,
            ok=bool(len(errors) == 0),
            message="sec filings cycle complete",
            error=("; ".join(errors[:3])) if errors else "",
            meta={"symbols_n": len(syms), "upserts": int(upserts), "event_rows": int(event_rows)},
        )
        put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
        logging.info("sec filings cycle ok=%s upserts=%s event_rows=%s", len(errors) == 0, upserts, event_rows)
    finally:
        logging.debug("sec_poll cycle_complete")


def main():
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("options_poll must be launched by supervisor")
        raise SystemExit(1)

    manager = get_manager()
    if not manager.is_job_enabled(JOB_NAME, default=True):
        manager.record_job_status(JOB_NAME, ok=True, message="sec filings disabled by data source control plane")
        return

    init_db()

    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        raise SystemExit(2)

    last_hb_s = 0.0
    try:
        while True:
            if not manager.is_job_enabled(JOB_NAME, default=True):
                manager.record_job_status(JOB_NAME, ok=True, message="sec filings disabled by data source control plane")
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
                logging.exception("sec_cycle_failed")
                status = record_pipeline_status(
                    JOB_NAME,
                    ok=False,
                    raw_rows=0,
                    event_rows=0,
                    last_ingested_ts_ms=int(time.time() * 1000),
                    error=str(e),
                    meta={"poll_seconds": float(POLL_SECONDS)},
                )
                manager.record_job_status(JOB_NAME, ok=False, message="sec filings cycle failed", error=str(e), meta={"poll_seconds": float(POLL_SECONDS)})
                put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
            time.sleep(max(1.0, float(POLL_SECONDS)))
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    main()
