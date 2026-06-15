"""
Disabled-by-default SEC Form 4 ingestion daemon.

README - insider feature group
Source: SEC EDGAR Form 4 / 4/A ownership filings, normalized into
``insider_transactions`` by source transaction id.
Cadence: supervised daemon, default ``FORM4_POLL_SECONDS=1800`` seconds.
Availability lag: features may only use EDGAR acceptance/filing availability
(``availability_ts_ms`` / ``filing_ts_ms``), never the transaction date.
Caveats: Form 4s can arrive up to two business days after a trade, amendments
can revise rows, 10b5-1 plan detection is text-derived when present, and
routine/opportunistic labels require at least three prior calendar years of
available insider trade history.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List

from engine.data.default_symbols import parse_symbol_limit
from engine.data.event_normalization import normalize_insider_event
from engine.data.sec.form4_live import (
    fetch_form4_transactions,
    refresh_form4_transaction_resolution,
)
from engine.data.sec.form4_classifier import classify_insider_trade
from engine.data.universe import get_active_symbols
from engine.runtime.config import (
    FORM4_BACKFILL_DAYS,
    INGEST_FORM4_ENABLED as CONFIG_INGEST_FORM4_ENABLED,
)
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.ingestion_status import record_pipeline_status
from engine.runtime.storage import (
    acquire_job_lock,
    connect,
    init_db,
    put_insider_transaction,
    put_job_heartbeat,
    put_normalized_event,
    release_job_lock,
    run_write_txn,
    touch_job_lock,
)
from services.data_source_manager import get_manager

JOB_NAME = (os.environ.get("ENGINE_JOB_NAME") or "ingest_form4").strip() or "ingest_form4"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()

INGEST_FORM4_ENABLED = bool(CONFIG_INGEST_FORM4_ENABLED)
POLL_SECONDS = float(os.environ.get("FORM4_POLL_SECONDS", os.environ.get("SEC_POLL_SECONDS", "1800")))
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "300"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))
SYMBOL_LIMIT = parse_symbol_limit(os.environ.get("FORM4_SYMBOL_LIMIT", os.environ.get("SEC_SYMBOL_LIMIT")), 600)
FILING_LIMIT = int(os.environ.get("FORM4_FILING_LIMIT", "60"))
MAX_BACKOFF_SECONDS = float(os.environ.get("FORM4_MAX_BACKOFF_SECONDS", "7200"))

__all__ = ["classify_insider_trade", "main"]

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
    con = connect()
    try:
        return get_active_symbols(con, limit=SYMBOL_LIMIT)
    finally:
        try:
            con.close()
        except Exception as exc:
            _warn_nonfatal("INGEST_FORM4_SYMBOL_CONNECTION_CLOSE_FAILED", exc, once_key="ingest_form4_symbols_close")


def _row_dicts(cur) -> List[Dict[str, Any]]:
    rows = cur.fetchall() or []
    if not rows:
        return []
    if hasattr(rows[0], "keys"):
        return [{str(key): row[key] for key in row.keys()} for row in rows]
    columns = [str(col[0]) for col in (cur.description or [])]
    return [dict(zip(columns, row)) for row in rows]


def _resolution_changed(current: Dict[str, Any], updated: Dict[str, Any]) -> bool:
    keys = ("symbol", "entity_id", "resolution_status", "resolution_method", "diagnostics_json")
    return any((current.get(key) or None) != (updated.get(key) or None) for key in keys)


def _reconcile_unresolved_rows(conw, *, allowed_symbols: List[str]) -> List[Dict[str, Any]]:
    cutoff_ms = int(time.time() * 1000) - (int(FORM4_BACKFILL_DAYS) * 24 * 3600 * 1000)
    cur = conw.execute(
        """
        SELECT *
        FROM insider_transactions
        WHERE COALESCE(symbol, '') = ''
          AND COALESCE(transaction_ts_ms, filing_ts_ms, ingested_ts_ms, 0) >= ?
        ORDER BY COALESCE(transaction_ts_ms, filing_ts_ms, ingested_ts_ms) DESC
        LIMIT 500
        """,
        (int(cutoff_ms),),
    )
    refreshed: List[Dict[str, Any]] = []
    for current in _row_dicts(cur):
        updated = refresh_form4_transaction_resolution(current, allowed_symbols=allowed_symbols or None)
        if _resolution_changed(current, updated):
            refreshed.append(updated)
    return refreshed


def _run_once() -> bool:
    manager = get_manager()
    symbols = list(dict.fromkeys(_load_symbols()))

    rows = []
    errors: List[str] = []
    for symbol in symbols:
        try:
            rows.extend(
                fetch_form4_transactions(
                    symbol,
                    filing_limit=FILING_LIMIT,
                    backfill_days=FORM4_BACKFILL_DAYS,
                    allowed_symbols=symbols,
                )
            )
        except Exception as exc:
            errors.append(f"{symbol}:{exc}")
            _warn_nonfatal("INGEST_FORM4_FETCH_FAILED", exc, once_key=f"ingest_form4_fetch:{symbol}", symbol=symbol)

    def _write(conw) -> tuple[int, int, int]:
        written = 0
        event_rows = 0
        reconciled = 0
        for row in rows:
            normalized = refresh_form4_transaction_resolution(row, allowed_symbols=symbols or None)
            written += int(put_insider_transaction(normalized, con=conw) or 0)
            put_normalized_event(normalize_insider_event(normalized), con=conw)
            event_rows += 1
        for row in _reconcile_unresolved_rows(conw, allowed_symbols=symbols):
            written += int(put_insider_transaction(row, con=conw) or 0)
            put_normalized_event(normalize_insider_event(row), con=conw)
            event_rows += 1
            reconciled += 1
        return written, event_rows, reconciled

    written = 0
    event_rows = 0
    reconciled = 0
    written, event_rows, reconciled = run_write_txn(
        _write,
        table="insider_transactions",
        operation="ingest_form4_batch",
        context={"job": JOB_NAME, "symbols": int(len(symbols)), "rows": int(len(rows))},
    ) or (0, 0, 0)

    last_ingested_ts_ms = max(
        [
            int(row.get("transaction_ts_ms") or row.get("filing_ts_ms") or row.get("ingested_ts_ms") or 0)
            for row in rows
        ]
        or [int(time.time() * 1000)]
    )
    status = record_pipeline_status(
        JOB_NAME,
        ok=(len(errors) == 0),
        raw_rows=int(len(rows)),
        event_rows=int(event_rows),
        last_ingested_ts_ms=int(last_ingested_ts_ms),
        error=("; ".join(errors[:3])) if errors else None,
        meta={
            "symbols_n": int(len(symbols)),
            "written": int(written),
            "event_rows": int(event_rows),
            "reconciled_rows": int(reconciled),
            "backfill_days": int(FORM4_BACKFILL_DAYS),
        },
    )
    manager.record_job_status(
        JOB_NAME,
        ok=(len(errors) == 0),
        message=("form4 cycle complete" if symbols else "form4 cycle complete without active universe symbols"),
        error=("; ".join(errors[:3])) if errors else "",
        meta={
            "symbols_n": int(len(symbols)),
            "rows": int(len(rows)),
            "written": int(written),
            "event_rows": int(event_rows),
            "reconciled_rows": int(reconciled),
        },
    )
    put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
    return len(errors) == 0


def main() -> None:
    """Run the supervised SEC Form 4 ingestion loop until disabled."""
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("ingest_form4 must be launched by supervisor")
        raise SystemExit(1)

    manager = get_manager()
    if not INGEST_FORM4_ENABLED:
        manager.record_job_status(JOB_NAME, ok=True, message="form4 disabled by env flag")
        return
    if not manager.is_job_enabled(JOB_NAME, default=False):
        manager.record_job_status(JOB_NAME, ok=True, message="form4 disabled by data source control plane")
        return

    init_db()
    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        raise SystemExit(2)

    last_hb_s = 0.0
    consecutive_failures = 0
    try:
        while True:
            if not manager.is_job_enabled(JOB_NAME, default=False):
                manager.record_job_status(JOB_NAME, ok=True, message="form4 disabled by data source control plane")
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
                cycle_ok = bool(_run_once())
                consecutive_failures = 0 if cycle_ok else consecutive_failures + 1
            except Exception as exc:
                consecutive_failures += 1
                LOGGER.exception("form4_cycle_failed")
                status = record_pipeline_status(
                    JOB_NAME,
                    ok=False,
                    raw_rows=0,
                    event_rows=0,
                    last_ingested_ts_ms=int(time.time() * 1000),
                    error=str(exc),
                    meta={"poll_seconds": float(POLL_SECONDS)},
                )
                manager.record_job_status(JOB_NAME, ok=False, message="form4 cycle failed", error=str(exc))
                put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
            backoff_s = min(
                float(MAX_BACKOFF_SECONDS),
                float(POLL_SECONDS) * (2 ** min(6, int(consecutive_failures))),
            )
            time.sleep(max(1.0, float(backoff_s)))
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    main()
