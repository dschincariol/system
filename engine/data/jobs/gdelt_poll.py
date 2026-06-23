"""
FILE: gdelt_poll.py

Job entrypoint or scheduled task for `gdelt_poll`.
"""

import os
import time
import json
import logging

from engine.data.default_symbols import parse_symbol_limit
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.ingestion_status import record_pipeline_status
from engine.runtime.storage import (
    init_db,
    acquire_job_lock,
    release_job_lock,
    touch_job_lock,
    put_job_heartbeat,
    connect,
    put_normalized_event,
    run_write_txn,
)

from engine.data.event_normalization import normalize_news_event
from engine.data.universe import get_active_symbols
from engine.data.ingest.gdelt_ingest import gdelt_cooldown_remaining_s, ingest_gdelt_doc
from services.data_source_manager import get_manager

JOB_NAME = "poll_gdelt"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "10.0"))
POLL_SECONDS = float(os.environ.get("GDELT_POLL_SECONDS", "300.0"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# Controls
LOOKBACK_MINUTES = int(os.environ.get("GDELT_LOOKBACK_MINUTES", "45"))
MAXRECORDS = int(os.environ.get("GDELT_MAXRECORDS", "250"))
SYMBOL_LIMIT = parse_symbol_limit(os.environ.get("GDELT_SYMBOL_LIMIT"), 600)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [gdelt_poll] %(message)s",
)
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


def _emit_heartbeat(payload: dict) -> None:
    try:
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(
            JOB_NAME,
            OWNER,
            PID,
            extra_json=json.dumps(payload or {}, separators=(",", ":"), sort_keys=True),
        )
    except Exception as e:
        _warn_nonfatal("GDELT_POLL_HEARTBEAT_FAILED", e, once_key="heartbeat")


def _sleep_with_heartbeat(manager, status: dict, *, sleep_seconds: float | None = None) -> bool:
    requested_sleep = float(sleep_seconds) if sleep_seconds is not None else float(POLL_SECONDS)
    deadline = time.time() + max(float(HEARTBEAT_EVERY_S), requested_sleep)
    base_status = dict(status or {})
    while True:
        remaining = deadline - time.time()
        if remaining <= 0:
            return True
        if not manager.is_job_enabled(JOB_NAME, default=True):
            manager.record_job_status(JOB_NAME, ok=True, message="gdelt disabled by data source control plane")
            return False
        payload = dict(base_status)
        payload["phase"] = "sleep"
        payload["remaining_s"] = max(0.0, float(remaining))
        payload["poll_seconds"] = float(POLL_SECONDS)
        payload["sleep_seconds"] = float(requested_sleep)
        payload["heartbeat_every_s"] = float(HEARTBEAT_EVERY_S)
        _emit_heartbeat(payload)
        time.sleep(min(float(HEARTBEAT_EVERY_S), max(1.0, remaining)))


def _run_once() -> dict:
    manager = get_manager()
    started_ms = int(time.time() * 1000)
    con = connect()
    try:
        syms = get_active_symbols(con, limit=SYMBOL_LIMIT)
    except Exception:
        syms = []
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal(
                "GDELT_POLL_CLOSE_FAILED",
                e,
                once_key="gdelt_poll_close",
            )

    items, errors = ingest_gdelt_doc(
        symbols=syms,
        lookback_minutes=LOOKBACK_MINUTES,
        maxrecords=MAXRECORDS,
        language=os.environ.get("GDELT_LANGUAGE", "english"),
    )

    event_rows = 0
    last_ingested_ts_ms = 0
    if items:
        def _write(con):
            local_rows = 0
            local_last_ts_ms = 0
            for it in items:
                put_normalized_event(normalize_news_event(it), con=con)
                local_rows += 1
                local_last_ts_ms = max(local_last_ts_ms, int(it.get("ts_ms") or 0))
            return local_rows, local_last_ts_ms

        event_rows, last_ingested_ts_ms = run_write_txn(
            _write,
            table="events",
            operation="ingest_gdelt_batch",
            context={"job": JOB_NAME, "items": int(len(items))},
        )

    dur_ms = int(time.time() * 1000) - started_ms
    status = record_pipeline_status(
        JOB_NAME,
        ok=(len(errors) == 0),
        raw_rows=len(items),
        event_rows=event_rows,
        last_ingested_ts_ms=(last_ingested_ts_ms or started_ms),
        error=("; ".join(str(e) for e in errors[:3])) if errors else None,
        latency_ms=dur_ms,
        meta={
            "lookback_minutes": int(LOOKBACK_MINUTES),
            "maxrecords": int(MAXRECORDS),
            "symbol_limit": int(SYMBOL_LIMIT or 0),
            "symbols_n": len(syms),
            "cooldown_remaining_s": float(gdelt_cooldown_remaining_s()),
        },
    )
    logging.info(
        "gdelt_cycle ok=%s raw_rows=%s event_rows=%s errors=%s dur_ms=%s",
        len(errors) == 0,
        len(items),
        event_rows,
        len(errors),
        dur_ms,
    )
    if errors:
        for e in errors[:10]:
            _warn_nonfatal("GDELT_POLL_ERROR", RuntimeError(str(e)), error_text=str(e))
    manager.record_job_status(
        JOB_NAME,
        ok=bool(len(errors) == 0),
        message="gdelt cycle complete",
        error=("; ".join(str(e) for e in errors[:3])) if errors else "",
        meta={"raw_rows": len(items), "event_rows": int(event_rows), "symbols_n": len(syms)},
    )
    return status


def main() -> None:
    manager = get_manager()
    if not manager.is_job_enabled(JOB_NAME, default=True):
        manager.record_job_status(JOB_NAME, ok=True, message="gdelt disabled by data source control plane")
        return

    init_db()

    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        logging.error("another instance is holding the job lock; exiting")
        raise SystemExit(2)

    try:
        while True:
            if not manager.is_job_enabled(JOB_NAME, default=True):
                manager.record_job_status(JOB_NAME, ok=True, message="gdelt disabled by data source control plane")
                break
            _emit_heartbeat(
                {
                    "phase": "cycle_start",
                    "poll_seconds": float(POLL_SECONDS),
                    "heartbeat_every_s": float(HEARTBEAT_EVERY_S),
                }
            )
            try:
                status = _run_once()
            except Exception as e:
                logging.exception("gdelt_cycle_failed")
                status = record_pipeline_status(
                    JOB_NAME,
                    ok=False,
                    raw_rows=0,
                    event_rows=0,
                    last_ingested_ts_ms=int(time.time() * 1000),
                    error=str(e),
                    meta={"poll_seconds": float(POLL_SECONDS)},
                )
                manager.record_job_status(JOB_NAME, ok=False, message="gdelt cycle failed", error=str(e), meta={"poll_seconds": float(POLL_SECONDS)})
            _emit_heartbeat(status)
            sleep_seconds = max(float(POLL_SECONDS), float(gdelt_cooldown_remaining_s()))
            if not _sleep_with_heartbeat(manager, status, sleep_seconds=sleep_seconds):
                break
    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as e:
            _warn_nonfatal(
                "GDELT_POLL_RELEASE_LOCK_FAILED",
                e,
                once_key="gdelt_poll_release_lock",
            )


if __name__ == "__main__":
    main()
