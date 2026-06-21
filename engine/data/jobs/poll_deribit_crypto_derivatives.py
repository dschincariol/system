"""Read-only Deribit public crypto derivatives ingestion job."""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from engine.data.deribit_crypto_derivatives import (
    deribit_snapshots_to_crypto_funding_rows,
    fetch_deribit_public_batch,
    load_deribit_settings,
    put_deribit_batch,
)
from engine.runtime.ingestion_status import record_pipeline_status
from engine.runtime.storage import (
    acquire_job_lock,
    init_db,
    put_crypto_funding_rate,
    put_job_heartbeat,
    release_job_lock,
    run_write_txn,
    touch_job_lock,
)
from services.data_source_manager import get_manager


JOB_NAME = "poll_deribit_crypto_derivatives"
SOURCE_KEY = "deribit_crypto_derivatives"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "300"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))
DEFAULT_POLL_SECONDS = float(os.environ.get("DERIBIT_POLL_SECONDS", "900.0"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [poll_deribit_crypto_derivatives] %(message)s",
)
LOG = logging.getLogger(__name__)


def _source_settings(manager) -> dict[str, Any]:
    source = manager.get_source(SOURCE_KEY, include_credentials=True) or {}
    return dict(source.get("settings") or {})


def _poll_seconds(manager) -> float:
    try:
        settings = load_deribit_settings(_source_settings(manager))
        return max(60.0, float(settings.poll_seconds))
    except Exception:
        return max(60.0, float(DEFAULT_POLL_SECONDS))


def _run_once() -> dict[str, Any]:
    started_ms = int(time.time() * 1000)
    manager = get_manager()
    settings = _source_settings(manager)
    batch = fetch_deribit_public_batch(settings=settings, now_ms=started_ms)
    counts: dict[str, int] = {}
    funding_written = 0
    funding_rows = deribit_snapshots_to_crypto_funding_rows(batch.get("snapshots") or [])

    def _txn(con) -> None:
        nonlocal counts, funding_written
        counts = put_deribit_batch(con, batch=batch, now_ms=started_ms)
        for row in funding_rows:
            funding_written += int(put_crypto_funding_rate(row, con=con) or 0)

    run_write_txn(
        _txn,
        table="deribit_market_snapshots",
        operation="poll_deribit_crypto_derivatives",
        context={"job": JOB_NAME, "source_key": SOURCE_KEY},
    )
    counts["crypto_funding_rows"] = int(funding_written)
    errors = list(batch.get("errors") or [])
    readiness = dict(batch.get("readiness") or {})
    ok = not bool(errors)
    status = record_pipeline_status(
        JOB_NAME,
        ok=ok,
        raw_rows=int(counts.get("snapshots") or 0),
        event_rows=0,
        last_ingested_ts_ms=int(time.time() * 1000),
        latency_ms=int(time.time() * 1000) - int(started_ms),
        error="; ".join(str(err) for err in errors[:8]) if errors else None,
        meta={
            "counts": counts,
            "source_key": SOURCE_KEY,
            "readiness": readiness,
            "shadow_only": True,
            "data_only": True,
            "direct_trading_authority": False,
        },
    )
    manager.record_job_status(
        JOB_NAME,
        ok=ok,
        message="deribit crypto derivatives cycle complete" if ok else "deribit crypto derivatives cycle degraded",
        error="; ".join(str(err) for err in errors[:8]) if errors else None,
        meta={
            "counts": counts,
            "source_key": SOURCE_KEY,
            "readiness": readiness,
            "shadow_only": True,
            "data_only": True,
            "direct_trading_authority": False,
        },
    )
    put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
    LOG.info("deribit crypto derivatives cycle counts=%s errors=%s", counts, len(errors))
    return {"ok": ok, "counts": counts, "readiness": readiness, "errors": errors}


def main() -> None:
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("poll_deribit_crypto_derivatives must be launched by supervisor")
        raise SystemExit(1)

    init_db()
    manager = get_manager()
    if not manager.is_job_enabled(JOB_NAME, default=False):
        manager.record_job_status(JOB_NAME, ok=True, message="deribit crypto derivatives source disabled")
        raise SystemExit(0)
    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        raise SystemExit(2)
    last_hb_s = 0.0
    try:
        while True:
            if not manager.is_job_enabled(JOB_NAME, default=False):
                manager.record_job_status(JOB_NAME, ok=True, message="deribit crypto derivatives source disabled")
                break
            now_s = time.time()
            if (now_s - last_hb_s) >= HEARTBEAT_EVERY_S:
                touch_job_lock(JOB_NAME, OWNER, PID)
                put_job_heartbeat(
                    JOB_NAME,
                    OWNER,
                    PID,
                    extra_json=json.dumps(
                        {"poll_seconds": float(_poll_seconds(manager)), "source_key": SOURCE_KEY},
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                )
                last_hb_s = now_s
            try:
                _run_once()
            except Exception as exc:
                LOG.exception("deribit_crypto_derivatives_cycle_failed")
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
                    message="deribit crypto derivatives cycle failed",
                    error=str(exc),
                    meta={"source_key": SOURCE_KEY, "data_only": True, "direct_trading_authority": False},
                )
                put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
            time.sleep(max(60.0, float(_poll_seconds(manager))))
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    main()
