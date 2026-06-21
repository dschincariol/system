"""Read-only sportsbook and betting-exchange odds research ingestion job."""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from engine.data.sportsbook_odds import fetch_sportsbook_odds_batch, put_sportsbook_odds_batch
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


JOB_NAME = "poll_sportsbook_odds"
SOURCE_KEY = "sportsbook_odds_research"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "300"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))
DEFAULT_POLL_SECONDS = float(os.environ.get("SPORTSBOOK_ODDS_POLL_SECONDS", "1800.0"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [poll_sportsbook_odds] %(message)s",
)
LOG = logging.getLogger(__name__)


def _source(manager) -> dict[str, Any]:
    return dict(manager.get_source(SOURCE_KEY, include_credentials=True) or {})


def _source_settings(manager) -> dict[str, Any]:
    return dict(_source(manager).get("settings") or {})


def _source_credentials(manager) -> dict[str, Any]:
    return dict(_source(manager).get("credentials") or {})


def _poll_seconds(manager) -> float:
    settings = _source_settings(manager)
    try:
        return max(300.0, float(settings.get("poll_seconds") or DEFAULT_POLL_SECONDS))
    except Exception:
        return max(300.0, float(DEFAULT_POLL_SECONDS))


def _run_once() -> dict[str, Any]:
    started_ms = int(time.time() * 1000)
    manager = get_manager()
    settings = _source_settings(manager)
    credentials = _source_credentials(manager)
    batch = fetch_sportsbook_odds_batch(settings=settings, credentials=credentials, now_ms=started_ms)
    counts: dict[str, int] = {}

    def _txn(con) -> None:
        nonlocal counts
        counts = put_sportsbook_odds_batch(
            con,
            odds=list(batch.get("odds") or []),
            mappings=list(batch.get("mappings") or []),
            now_ms=started_ms,
        )

    run_write_txn(
        _txn,
        table="sportsbook_odds_snapshots",
        operation="poll_sportsbook_odds",
        context={"job": JOB_NAME, "source_key": SOURCE_KEY},
    )
    provider_state = dict(batch.get("provider_state") or {})
    status = record_pipeline_status(
        JOB_NAME,
        ok=True,
        raw_rows=int(counts.get("odds") or 0),
        event_rows=0,
        last_ingested_ts_ms=int(time.time() * 1000),
        latency_ms=int(time.time() * 1000) - int(started_ms),
        meta={
            "counts": counts,
            "source_key": SOURCE_KEY,
            "provider_state": provider_state,
            "shadow_only": True,
            "research_only": True,
            "data_only": True,
            "direct_trading_authority": False,
        },
    )
    manager.record_job_status(
        JOB_NAME,
        ok=True,
        message="sportsbook odds research cycle complete",
        meta={
            "counts": counts,
            "source_key": SOURCE_KEY,
            "provider_state": provider_state,
            "shadow_only": True,
            "research_only": True,
            "data_only": True,
            "direct_trading_authority": False,
        },
    )
    put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
    LOG.info("sportsbook odds research cycle counts=%s provider_state=%s", counts, provider_state)
    return {"ok": True, "counts": counts, "provider_state": provider_state}


def main() -> None:
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("poll_sportsbook_odds must be launched by supervisor")
        raise SystemExit(1)

    init_db()
    manager = get_manager()
    if not manager.is_job_enabled(JOB_NAME, default=False):
        manager.record_job_status(JOB_NAME, ok=True, message="sportsbook odds research source disabled")
        raise SystemExit(0)
    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        raise SystemExit(2)
    last_hb_s = 0.0
    try:
        while True:
            if not manager.is_job_enabled(JOB_NAME, default=False):
                manager.record_job_status(JOB_NAME, ok=True, message="sportsbook odds research source disabled")
                break
            now_s = time.time()
            if (now_s - last_hb_s) >= HEARTBEAT_EVERY_S:
                touch_job_lock(JOB_NAME, OWNER, PID)
                put_job_heartbeat(
                    JOB_NAME,
                    OWNER,
                    PID,
                    extra_json=json.dumps(
                        {
                            "poll_seconds": float(_poll_seconds(manager)),
                            "source_key": SOURCE_KEY,
                            "research_only": True,
                            "direct_trading_authority": False,
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                )
                last_hb_s = now_s
            try:
                _run_once()
            except Exception as exc:
                LOG.exception("sportsbook_odds_research_cycle_failed")
                status = record_pipeline_status(
                    JOB_NAME,
                    ok=False,
                    raw_rows=0,
                    event_rows=0,
                    last_ingested_ts_ms=int(time.time() * 1000),
                    error=str(exc),
                    meta={"source_key": SOURCE_KEY, "data_only": True, "direct_trading_authority": False},
                )
                manager.record_job_status(
                    JOB_NAME,
                    ok=False,
                    message="sportsbook odds research cycle failed",
                    error=str(exc),
                    meta={"source_key": SOURCE_KEY, "data_only": True, "direct_trading_authority": False},
                )
                put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
            time.sleep(max(300.0, float(_poll_seconds(manager))))
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    main()
