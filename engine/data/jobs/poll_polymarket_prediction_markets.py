"""Read-only Polymarket event prediction-market ingestion job."""

from __future__ import annotations

import json
import logging
import os
import time

from engine.data.prediction_market_providers import fetch_polymarket_event_batch
from engine.data.prediction_market_storage import put_prediction_market_batch
from engine.runtime.ingestion_status import record_pipeline_status
from engine.runtime.storage import (
    acquire_job_lock,
    init_db,
    put_job_heartbeat,
    release_job_lock,
    run_write_txn,
    touch_job_lock,
)
from services.data_source_manager import get_manager


JOB_NAME = "poll_polymarket_prediction_markets"
SOURCE_KEY = "polymarket_event_signals"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "300"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))
POLL_SECONDS = float(os.environ.get("POLYMARKET_POLL_SECONDS", "900.0"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [poll_polymarket_prediction_markets] %(message)s",
)
LOG = logging.getLogger(__name__)


def _source_settings(manager) -> dict:
    source = manager.get_source(SOURCE_KEY, include_credentials=True) or {}
    return dict(source.get("settings") or {})


def _run_once() -> dict:
    started_ms = int(time.time() * 1000)
    manager = get_manager()
    settings = _source_settings(manager)
    batch = fetch_polymarket_event_batch(settings=settings, now_ms=started_ms)
    counts: dict[str, int] = {}

    def _txn(con) -> None:
        nonlocal counts
        counts = put_prediction_market_batch(con, now_ms=started_ms, **batch)

    run_write_txn(_txn)
    status = record_pipeline_status(
        JOB_NAME,
        ok=True,
        raw_rows=sum(int(v) for v in counts.values()),
        event_rows=int(counts.get("events") or 0),
        last_ingested_ts_ms=int(time.time() * 1000),
        latency_ms=int(time.time() * 1000) - int(started_ms),
        meta={
            "counts": counts,
            "source_key": SOURCE_KEY,
            "shadow_only": True,
            "direct_trading_authority": False,
            "data_only": True,
        },
    )
    manager.record_job_status(
        JOB_NAME,
        ok=True,
        message="polymarket event prediction-market cycle complete",
        meta={
            "counts": counts,
            "source_key": SOURCE_KEY,
            "shadow_only": True,
            "direct_trading_authority": False,
            "data_only": True,
        },
    )
    put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
    LOG.info("polymarket prediction-market cycle counts=%s", counts)
    return {"ok": True, "counts": counts}


def main() -> None:
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("poll_polymarket_prediction_markets must be launched by supervisor")
        raise SystemExit(1)

    init_db()
    manager = get_manager()
    if not manager.is_job_enabled(JOB_NAME, default=False):
        manager.record_job_status(JOB_NAME, ok=True, message="polymarket prediction-market source disabled")
        raise SystemExit(0)
    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        raise SystemExit(2)
    last_hb_s = 0.0
    try:
        while True:
            if not manager.is_job_enabled(JOB_NAME, default=False):
                manager.record_job_status(JOB_NAME, ok=True, message="polymarket prediction-market source disabled")
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
                LOG.exception("polymarket_prediction_market_cycle_failed")
                status = record_pipeline_status(
                    JOB_NAME,
                    ok=False,
                    raw_rows=0,
                    event_rows=0,
                    last_ingested_ts_ms=int(time.time() * 1000),
                    error=str(exc),
                    meta={"source_key": SOURCE_KEY, "data_only": True},
                )
                manager.record_job_status(
                    JOB_NAME,
                    ok=False,
                    message="polymarket prediction-market cycle failed",
                    error=str(exc),
                    meta={"source_key": SOURCE_KEY, "data_only": True},
                )
                put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
            time.sleep(max(60.0, float(POLL_SECONDS)))
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    main()
