"""Read-only ForecastEx regulated event-contract ingestion job."""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Mapping

from engine.data.forecastex_event_contracts import fetch_forecastex_csv_batch
from engine.data.ibkr_event_contracts import fetch_ibkr_event_contract_batch, ibkr_event_contracts_enabled
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


JOB_NAME = "poll_forecastex_event_contracts"
SOURCE_KEY = "forecastex_event_contracts"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "300"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))
POLL_SECONDS = float(os.environ.get("FORECASTEX_POLL_SECONDS", "600.0"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [poll_forecastex_event_contracts] %(message)s",
)
LOG = logging.getLogger(__name__)


def _source_settings(manager: Any) -> dict[str, Any]:
    source = manager.get_source(SOURCE_KEY, include_credentials=True) or {}
    return dict(source.get("settings") or {})


def _merge_batch(target: dict[str, list[dict[str, Any]]], source: Mapping[str, Any]) -> None:
    for key in ("events", "markets", "orderbooks", "trades"):
        target.setdefault(key, [])
        target[key].extend([dict(item or {}) for item in list((source or {}).get(key) or []) if isinstance(item, Mapping)])


def _run_once() -> dict[str, Any]:
    started_ms = int(time.time() * 1000)
    manager = get_manager()
    settings = _source_settings(manager)
    forecastex_batch = fetch_forecastex_csv_batch(settings=settings, now_ms=started_ms)
    ibkr_batch = fetch_ibkr_event_contract_batch(settings=settings, now_ms=started_ms) if ibkr_event_contracts_enabled(settings) else {
        "events": [],
        "markets": [],
        "orderbooks": [],
        "trades": [],
        "health": {
            "provider": "ibkr_event_contracts",
            "enabled": False,
            "status": "disabled",
            "read_only": True,
            "direct_trading_authority": False,
        },
    }

    batch: dict[str, list[dict[str, Any]]] = {"events": [], "markets": [], "orderbooks": [], "trades": []}
    _merge_batch(batch, forecastex_batch)
    _merge_batch(batch, ibkr_batch)
    counts: dict[str, int] = {}

    def _txn(con) -> None:
        nonlocal counts
        counts = put_prediction_market_batch(con, now_ms=started_ms, **batch)

    run_write_txn(_txn)
    health = {
        "source_key": SOURCE_KEY,
        "shadow_only": True,
        "direct_trading_authority": False,
        "forecastex": dict(forecastex_batch.get("health") or {}),
        "ibkr_event_contracts": dict(ibkr_batch.get("health") or {}),
        "counts": counts,
    }
    rows_parsed = int((health["forecastex"].get("rows_parsed") or 0)) + int((health["ibkr_event_contracts"].get("rows_parsed") or 0))
    rows_skipped = int((health["forecastex"].get("rows_skipped") or 0)) + int((health["ibkr_event_contracts"].get("rows_skipped") or 0))
    parse_errors = int((health["forecastex"].get("parse_error_count") or 0)) + int((health["ibkr_event_contracts"].get("parse_error_count") or 0))
    stale_count = int((health["forecastex"].get("stale_count") or 0))
    health.update(
        {
            "last_successful_csv_date": health["forecastex"].get("last_successful_csv_date"),
            "rows_parsed": int(rows_parsed),
            "rows_skipped": int(rows_skipped),
            "parse_error_count": int(parse_errors),
            "stale_count": int(stale_count),
            "contract_categories_enabled": health["forecastex"].get("contract_categories_enabled") or [],
            "ibkr_enabled": bool(health["ibkr_event_contracts"].get("enabled")),
        }
    )
    status = record_pipeline_status(
        JOB_NAME,
        ok=True,
        raw_rows=sum(int(v) for v in counts.values()),
        event_rows=int(counts.get("events") or 0),
        last_ingested_ts_ms=int(time.time() * 1000),
        latency_ms=int(time.time() * 1000) - int(started_ms),
        meta=health,
    )
    manager.record_job_status(
        JOB_NAME,
        ok=True,
        message="ForecastEx regulated event-contract cycle complete",
        meta=health,
    )
    put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
    LOG.info("forecastex event-contract cycle counts=%s health=%s", counts, {k: health.get(k) for k in ("rows_parsed", "rows_skipped", "stale_count", "parse_error_count")})
    return {"ok": True, "counts": counts, "health": health}


def main() -> None:
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("poll_forecastex_event_contracts must be launched by supervisor")
        raise SystemExit(1)

    init_db()
    manager = get_manager()
    if not manager.is_job_enabled(JOB_NAME, default=False):
        manager.record_job_status(JOB_NAME, ok=True, message="ForecastEx regulated event-contract source disabled")
        raise SystemExit(0)
    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        raise SystemExit(2)
    last_hb_s = 0.0
    try:
        while True:
            if not manager.is_job_enabled(JOB_NAME, default=False):
                manager.record_job_status(JOB_NAME, ok=True, message="ForecastEx regulated event-contract source disabled")
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
                LOG.exception("forecastex_event_contract_cycle_failed")
                status = record_pipeline_status(
                    JOB_NAME,
                    ok=False,
                    raw_rows=0,
                    event_rows=0,
                    last_ingested_ts_ms=int(time.time() * 1000),
                    error=str(exc),
                    meta={"source_key": SOURCE_KEY, "direct_trading_authority": False},
                )
                manager.record_job_status(
                    JOB_NAME,
                    ok=False,
                    message="ForecastEx regulated event-contract cycle failed",
                    error=str(exc),
                    meta={"source_key": SOURCE_KEY, "direct_trading_authority": False},
                )
                put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
            time.sleep(max(60.0, float(POLL_SECONDS)))
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    main()
