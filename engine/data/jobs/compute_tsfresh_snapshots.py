"""
Supervisor-run daemon that materializes persisted tsfresh feature snapshots.
"""

from __future__ import annotations

import json
import logging
import os
import time

from engine.data.universe import get_active_symbols
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.ingestion_status import record_pipeline_status
from engine.runtime.storage import (
    _table_exists,
    acquire_job_lock,
    connect,
    init_db,
    put_job_heartbeat,
    release_job_lock,
    touch_job_lock,
)
from engine.runtime.workload_profiles import (
    assert_offline_work_allowed,
    tsfresh_snapshot_batch_size,
    tsfresh_snapshot_symbol_limit,
)
from engine.strategy.tsfresh_features import (
    TSFRESH_WINDOW_S,
    get_tsfresh_feature_ids,
    materialize_tsfresh_feature_snapshots,
)
from services.data_source_manager import get_manager

JOB_NAME = "compute_tsfresh_snapshots"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))
SNAPSHOT_SLEEP_S = float(
    os.environ.get(
        "TSFRESH_SNAPSHOT_SLEEP_S",
        os.environ.get("MODEL_FEATURE_SNAPSHOT_SLEEP_S", "300"),
    )
)
SNAPSHOT_BUCKET_SEC = max(
    60,
    int(
        os.environ.get(
            "TSFRESH_SNAPSHOT_BUCKET_SEC",
            os.environ.get("MODEL_FEATURE_SNAPSHOT_BUCKET_SEC", "300"),
        )
    ),
)
SNAPSHOT_SYMBOL_LIMIT = tsfresh_snapshot_symbol_limit()
SNAPSHOT_BATCH_SIZE = tsfresh_snapshot_batch_size()

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [compute_tsfresh_snapshots] %(message)s",
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
    except Exception as exc:
        _warn_nonfatal("COMPUTE_TSFRESH_SNAPSHOTS_HEARTBEAT_FAILED", exc, once_key="heartbeat")


def _sleep_with_heartbeat(manager, status: dict) -> bool:
    deadline = time.time() + max(1.0, float(SNAPSHOT_SLEEP_S))
    base_status = dict(status or {})
    while True:
        remaining = deadline - time.time()
        if remaining <= 0:
            return True
        if not manager.is_job_enabled(JOB_NAME, default=True):
            manager.record_job_status(JOB_NAME, ok=True, message="compute_tsfresh_snapshots disabled by data source control plane")
            return False
        payload = dict(base_status)
        payload["phase"] = "sleep"
        payload["remaining_s"] = max(0.0, float(remaining))
        payload["bucket_sec"] = int(SNAPSHOT_BUCKET_SEC)
        payload["sleep_s"] = float(SNAPSHOT_SLEEP_S)
        payload["heartbeat_every_s"] = float(HEARTBEAT_EVERY_S)
        payload["window_s"] = int(TSFRESH_WINDOW_S)
        payload["symbol_limit"] = int(SNAPSHOT_SYMBOL_LIMIT)
        payload["batch_size"] = int(SNAPSHOT_BATCH_SIZE)
        _emit_heartbeat(payload)
        time.sleep(min(float(HEARTBEAT_EVERY_S), max(1.0, remaining)))


def _bucket_start(ts_ms: int, bucket_sec: int) -> int:
    size_ms = max(1, int(bucket_sec)) * 1000
    return int(ts_ms // size_ms) * size_ms


def _load_symbols(con, *, limit: int | None) -> list[str]:
    syms = [str(s).upper().strip() for s in get_active_symbols(con, limit=limit) if str(s).strip()]
    if syms:
        return syms
    if not _table_exists(con, "price_quotes"):
        return []
    fallback_limit = 5000 if limit is None else int(limit)
    try:
        rows = con.execute(
            """
            SELECT DISTINCT symbol
            FROM price_quotes
            WHERE symbol IS NOT NULL
              AND symbol <> ''
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (int(fallback_limit),),
        ).fetchall()
    except Exception as exc:
        _warn_nonfatal(
            "COMPUTE_TSFRESH_SNAPSHOTS_LOAD_SYMBOLS_FALLBACK_FAILED",
            exc,
            once_key="compute_tsfresh_snapshots_load_symbols_fallback",
            table="price_quotes",
        )
        return []
    return [str(row[0]).upper().strip() for row in rows or [] if row and row[0]]


def main() -> None:
    """Run the supervised daemon that materializes persisted TSFresh snapshots."""
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("compute_tsfresh_snapshots must be launched by supervisor")
        raise SystemExit(1)

    init_db()
    manager = get_manager()

    if not manager.is_job_enabled(JOB_NAME, default=True):
        manager.record_job_status(JOB_NAME, ok=True, message="compute_tsfresh_snapshots disabled by data source control plane")
        raise SystemExit(0)

    try:
        assert_offline_work_allowed(job_name=JOB_NAME)
    except RuntimeError as exc:
        detail = str(exc)
        manager.record_job_status(
            JOB_NAME,
            ok=False,
            message="compute_tsfresh_snapshots blocked by workload profile",
            error=detail,
        )
        print(f"[workload_profile] {detail}")
        raise SystemExit(3)

    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        raise SystemExit(2)

    try:
        while True:
            if not manager.is_job_enabled(JOB_NAME, default=True):
                manager.record_job_status(JOB_NAME, ok=True, message="compute_tsfresh_snapshots disabled by data source control plane")
                break
            started_ms = int(time.time() * 1000)
            anchor_ts_ms = _bucket_start(int(started_ms), int(SNAPSHOT_BUCKET_SEC))
            snapshot_stats = {
                "snapshots": 0,
                "symbols": 0,
                "feature_dim": len(get_tsfresh_feature_ids()),
                "window_s": int(TSFRESH_WINDOW_S),
                "ts_ms": int(anchor_ts_ms),
            }
            _emit_heartbeat(
                {
                    "phase": "cycle_start",
                    "anchor_ts_ms": int(anchor_ts_ms),
                    "bucket_sec": int(SNAPSHOT_BUCKET_SEC),
                    "window_s": int(TSFRESH_WINDOW_S),
                    "heartbeat_every_s": float(HEARTBEAT_EVERY_S),
                }
            )
            try:
                con = None
                con = connect(readonly=False)
                try:
                    symbols = _load_symbols(
                        con,
                        limit=(int(SNAPSHOT_SYMBOL_LIMIT) if SNAPSHOT_SYMBOL_LIMIT is not None else None),
                    )
                    snapshot_stats = materialize_tsfresh_feature_snapshots(
                        symbols=symbols,
                        ts_ms=int(anchor_ts_ms),
                        window_s=int(TSFRESH_WINDOW_S),
                        symbol_limit=int(SNAPSHOT_SYMBOL_LIMIT),
                        batch_size=int(SNAPSHOT_BATCH_SIZE),
                        con=con,
                    )
                    con.commit()
                finally:
                    try:
                        if con is not None:
                            con.close()
                    except Exception as exc:
                        _warn_nonfatal(
                            "COMPUTE_TSFRESH_SNAPSHOTS_CONN_CLOSE_FAILED",
                            exc,
                            once_key="compute_tsfresh_snapshots_conn_close",
                        )

                status = record_pipeline_status(
                    JOB_NAME,
                    ok=True,
                    raw_rows=int(snapshot_stats.get("snapshots") or 0),
                    event_rows=0,
                    last_ingested_ts_ms=int(anchor_ts_ms),
                    latency_ms=int(time.time() * 1000) - int(started_ms),
                    meta={
                        "symbols": int(snapshot_stats.get("symbols") or 0),
                        "feature_dim": int(snapshot_stats.get("feature_dim") or len(get_tsfresh_feature_ids())),
                        "window_s": int(snapshot_stats.get("window_s") or TSFRESH_WINDOW_S),
                        "bucket_sec": int(SNAPSHOT_BUCKET_SEC),
                    },
                )
                manager.record_job_status(
                    JOB_NAME,
                    ok=True,
                    message="tsfresh snapshot cycle complete",
                    meta={
                        "symbols": int(snapshot_stats.get("symbols") or 0),
                        "snapshots": int(snapshot_stats.get("snapshots") or 0),
                        "feature_dim": int(snapshot_stats.get("feature_dim") or len(get_tsfresh_feature_ids())),
                        "window_s": int(snapshot_stats.get("window_s") or TSFRESH_WINDOW_S),
                        "bucket_sec": int(SNAPSHOT_BUCKET_SEC),
                        "symbol_limit": int(SNAPSHOT_SYMBOL_LIMIT),
                        "batch_size": int(SNAPSHOT_BATCH_SIZE),
                    },
                )
                logging.info(
                    "anchor_ts_ms=%s symbols=%s snapshots=%s feature_dim=%s window_s=%s symbol_limit=%s batch_size=%s",
                    int(anchor_ts_ms),
                    int(snapshot_stats.get("symbols") or 0),
                    int(snapshot_stats.get("snapshots") or 0),
                    int(snapshot_stats.get("feature_dim") or len(get_tsfresh_feature_ids())),
                    int(snapshot_stats.get("window_s") or TSFRESH_WINDOW_S),
                    int(SNAPSHOT_SYMBOL_LIMIT),
                    int(SNAPSHOT_BATCH_SIZE),
                )
            except Exception as exc:
                status = record_pipeline_status(
                    JOB_NAME,
                    ok=False,
                    raw_rows=int(snapshot_stats.get("snapshots") or 0),
                    event_rows=0,
                    last_ingested_ts_ms=int(anchor_ts_ms),
                    error=str(exc),
                    latency_ms=int(time.time() * 1000) - int(started_ms),
                    meta={
                        "symbols": int(snapshot_stats.get("symbols") or 0),
                        "feature_dim": int(snapshot_stats.get("feature_dim") or len(get_tsfresh_feature_ids())),
                        "window_s": int(snapshot_stats.get("window_s") or TSFRESH_WINDOW_S),
                        "bucket_sec": int(SNAPSHOT_BUCKET_SEC),
                    },
                )
                manager.record_job_status(
                    JOB_NAME,
                    ok=False,
                    message="tsfresh snapshot cycle failed",
                    error=str(exc),
                    meta={
                        "symbols": int(snapshot_stats.get("symbols") or 0),
                        "snapshots": int(snapshot_stats.get("snapshots") or 0),
                        "feature_dim": int(snapshot_stats.get("feature_dim") or len(get_tsfresh_feature_ids())),
                        "window_s": int(snapshot_stats.get("window_s") or TSFRESH_WINDOW_S),
                        "bucket_sec": int(SNAPSHOT_BUCKET_SEC),
                    },
                )
                logging.exception("tsfresh_snapshot_cycle_failed")

            _emit_heartbeat(status)
            if not _sleep_with_heartbeat(manager, status):
                break
    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as exc:
            _warn_nonfatal(
                "COMPUTE_TSFRESH_SNAPSHOTS_RELEASE_LOCK_FAILED",
                exc,
                once_key="compute_tsfresh_snapshots_release_lock",
            )


if __name__ == "__main__":
    main()
