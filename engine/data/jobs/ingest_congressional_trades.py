"""
Disabled-by-default congressional trade ingestion daemon.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List

from engine.data.congressional_trades import (
    fetch_congressional_trades,
    refresh_congressional_trade_resolution,
)
from engine.data.default_symbols import parse_symbol_limit
from engine.data.event_normalization import normalize_congressional_event
from engine.data.universe import get_active_symbols
from engine.runtime.config import (
    CONGRESSIONAL_BACKFILL_DAYS,
    INGEST_CONGRESSIONAL_ENABLED as CONFIG_INGEST_CONGRESSIONAL_ENABLED,
)
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.ingestion_status import record_pipeline_status
from engine.runtime.storage import (
    acquire_job_lock,
    connect,
    init_db,
    put_congressional_trade,
    put_job_heartbeat,
    put_normalized_event,
    release_job_lock,
    run_write_txn,
    touch_job_lock,
)
from services.data_source_manager import get_manager

JOB_NAME = (os.environ.get("ENGINE_JOB_NAME") or "ingest_congressional_trades").strip() or "ingest_congressional_trades"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()

INGEST_CONGRESSIONAL_ENABLED = bool(CONFIG_INGEST_CONGRESSIONAL_ENABLED)
POLL_SECONDS = float(os.environ.get("CONGRESSIONAL_POLL_SECONDS", "3600"))
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "300"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))
SYMBOL_LIMIT = parse_symbol_limit(os.environ.get("CONGRESSIONAL_SYMBOL_LIMIT", os.environ.get("SEC_SYMBOL_LIMIT")), 600)

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
            _warn_nonfatal(
                "INGEST_CONGRESSIONAL_SYMBOL_CONNECTION_CLOSE_FAILED",
                exc,
                once_key="ingest_congressional_symbols_close",
            )


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
    cutoff_ms = int(time.time() * 1000) - (int(CONGRESSIONAL_BACKFILL_DAYS) * 24 * 3600 * 1000)
    cur = conw.execute(
        """
        SELECT *
        FROM congressional_trades
        WHERE COALESCE(symbol, '') = ''
          AND COALESCE(transaction_ts_ms, disclosure_ts_ms, ingested_ts_ms, 0) >= ?
        ORDER BY COALESCE(transaction_ts_ms, disclosure_ts_ms, ingested_ts_ms) DESC
        LIMIT 500
        """,
        (int(cutoff_ms),),
    )
    refreshed: List[Dict[str, Any]] = []
    for current in _row_dicts(cur):
        updated = refresh_congressional_trade_resolution(current, allowed_symbols=allowed_symbols or None)
        if _resolution_changed(current, updated):
            refreshed.append(updated)
    return refreshed


def _run_once() -> None:
    manager = get_manager()
    symbols = list(dict.fromkeys(_load_symbols()))
    rows = fetch_congressional_trades(
        backfill_days=CONGRESSIONAL_BACKFILL_DAYS,
        allowed_symbols=symbols,
    )

    def _write(conw) -> tuple[int, int, int]:
        written = 0
        event_rows = 0
        reconciled = 0
        for row in rows:
            normalized = refresh_congressional_trade_resolution(row, allowed_symbols=symbols or None)
            written += int(put_congressional_trade(normalized, con=conw) or 0)
            put_normalized_event(normalize_congressional_event(normalized), con=conw)
            event_rows += 1
        for row in _reconcile_unresolved_rows(conw, allowed_symbols=symbols):
            written += int(put_congressional_trade(row, con=conw) or 0)
            put_normalized_event(normalize_congressional_event(row), con=conw)
            event_rows += 1
            reconciled += 1
        return written, event_rows, reconciled

    written, event_rows, reconciled = run_write_txn(
        _write,
        table="congressional_trades",
        operation="ingest_congressional_batch",
        context={"job": JOB_NAME, "rows": int(len(rows))},
    ) or (0, 0, 0)

    last_ingested_ts_ms = max(
        [
            int(row.get("transaction_ts_ms") or row.get("disclosure_ts_ms") or row.get("ingested_ts_ms") or 0)
            for row in rows
        ]
        or [int(time.time() * 1000)]
    )
    status = record_pipeline_status(
        JOB_NAME,
        ok=True,
        raw_rows=int(len(rows)),
        event_rows=int(event_rows),
        last_ingested_ts_ms=int(last_ingested_ts_ms),
        meta={
            "rows": int(len(rows)),
            "written": int(written),
            "event_rows": int(event_rows),
            "reconciled_rows": int(reconciled),
            "backfill_days": int(CONGRESSIONAL_BACKFILL_DAYS),
        },
    )
    manager.record_job_status(
        JOB_NAME,
        ok=True,
        message="congressional trades cycle complete",
        meta={
            "rows": int(len(rows)),
            "written": int(written),
            "event_rows": int(event_rows),
            "reconciled_rows": int(reconciled),
        },
    )
    put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))


def main() -> None:
    """Run the supervised congressional trade ingestion loop until disabled."""
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("ingest_congressional_trades must be launched by supervisor")
        raise SystemExit(1)

    manager = get_manager()
    if not INGEST_CONGRESSIONAL_ENABLED:
        manager.record_job_status(JOB_NAME, ok=True, message="congressional trades disabled by env flag")
        return
    if not manager.is_job_enabled(JOB_NAME, default=False):
        manager.record_job_status(JOB_NAME, ok=True, message="congressional trades disabled by data source control plane")
        return

    init_db()
    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        raise SystemExit(2)

    last_hb_s = 0.0
    try:
        while True:
            if not manager.is_job_enabled(JOB_NAME, default=False):
                manager.record_job_status(JOB_NAME, ok=True, message="congressional trades disabled by data source control plane")
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
            except Exception as exc:
                LOGGER.exception("congressional_cycle_failed")
                status = record_pipeline_status(
                    JOB_NAME,
                    ok=False,
                    raw_rows=0,
                    event_rows=0,
                    last_ingested_ts_ms=int(time.time() * 1000),
                    error=str(exc),
                    meta={"poll_seconds": float(POLL_SECONDS)},
                )
                manager.record_job_status(JOB_NAME, ok=False, message="congressional trades cycle failed", error=str(exc))
                put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
            time.sleep(max(1.0, float(POLL_SECONDS)))
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    main()
