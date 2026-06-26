"""Helpers for ingestion pipeline health snapshots and persistence.

This module records per-pipeline success, latency, row counts, and freshness so
runtime diagnostics, operator views, and provider monitoring can reason about
ingestion health without scraping logs.
"""

from __future__ import annotations

import json
import os
import threading
import time
from typing import Any, Dict, Optional

from engine.runtime.runtime_meta import meta_get, meta_set
from engine.runtime.feed_truth import classify_pipeline_liveness
from engine.runtime.storage import connect_ro
from engine.runtime.telemetry_append_buffer import append_ingestion_pipeline_health_row
from engine.runtime.timeseries_write_policy import get_timeseries_write_policy

STATUS_PREFIX = "ingestion_pipeline_status::"
_PIPELINE_HEALTH_MIN_INTERVAL_MS = max(
    0,
    int(float(os.environ.get("INGESTION_PIPELINE_HEALTH_MIN_INTERVAL_S", "5.0") or 5.0) * 1000.0),
)
_PIPELINE_HEALTH_WRITE_LOCK = threading.Lock()
_LAST_PIPELINE_HEALTH_WRITE: dict[str, dict[str, Any]] = {}

DEFAULT_INGESTION_PIPELINE_JOBS = [
    "stream_prices_polygon_ws",
    "stream_prices_ibkr",
    "poll_prices",
    "options_poll",
    "poll_macro",
    "ingest_now",
    "snapshot_model_features",
    "inference_health_probe",
    "poll_gdelt",
    "poll_sec_filings",
    "ingest_form4",
    "ingest_congressional_trades",
    "poll_earnings",
    "poll_social_reddit",
    "poll_social_stocktwits",
    "poll_weather_forecasts",
    "poll_weather_alerts",
]


def default_ingestion_pipeline_jobs() -> list[str]:
    return list(DEFAULT_INGESTION_PIPELINE_JOBS)


def _status_key(pipeline: str) -> str:
    return f"{STATUS_PREFIX}{str(pipeline or '').strip()}"


def _load_status(pipeline: str) -> Dict[str, Any]:
    try:
        raw = str(meta_get(_status_key(pipeline), "") or "").strip()
        if not raw:
            return {}
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception as e:
        from engine.runtime.failure_diagnostics import log_failure
        from engine.runtime.logging import get_logger
        log_failure(
            get_logger("engine.runtime.ingestion_status"),
            event="ingestion_status_json_parse_failed",
            code="INGESTION_STATUS_JSON_PARSE_FAILED",
            message=str(e),
            error=e,
            level=30,
            component="engine.runtime.ingestion_status",
            persist=False,
        )
        return {}


def _should_persist_pipeline_health_row(
    pipeline: str,
    *,
    ok: bool,
    error: str | None,
    best_effort: bool,
    now_ms: int,
) -> bool:
    if (not best_effort) or _PIPELINE_HEALTH_MIN_INTERVAL_MS <= 0:
        return True
    pipeline_name = str(pipeline or "").strip()
    error_s = str(error or "")
    with _PIPELINE_HEALTH_WRITE_LOCK:
        previous = dict(_LAST_PIPELINE_HEALTH_WRITE.get(pipeline_name) or {})
        last_ts_ms = int(previous.get("ts_ms") or 0)
        last_ok = bool(previous.get("ok"))
        last_error = str(previous.get("error") or "")
        if last_ts_ms > 0 and (now_ms - last_ts_ms) < _PIPELINE_HEALTH_MIN_INTERVAL_MS and last_ok == bool(ok) and last_error == error_s:
            return False
        _LAST_PIPELINE_HEALTH_WRITE[pipeline_name] = {
            "ts_ms": int(now_ms),
            "ok": bool(ok),
            "error": error_s,
        }
    return True


def _should_buffer_pipeline_health_row(*, best_effort: bool) -> bool:
    # Buffered appends are reserved for the high-frequency path. When writes are
    # already throttled to one row per interval, keeping the row DB-visible
    # immediately preserves the operator contract without adding hot-path churn.
    return get_timeseries_write_policy().should_buffer_pipeline_health(
        best_effort=bool(best_effort),
        min_interval_ms=int(_PIPELINE_HEALTH_MIN_INTERVAL_MS),
    )


def record_pipeline_status(
    pipeline: str,
    *,
    ok: bool,
    raw_rows: int = 0,
    event_rows: int = 0,
    last_ingested_ts_ms: Optional[int] = None,
    error: str | None = None,
    meta: Optional[Dict[str, Any]] = None,
    latency_ms: Optional[int] = None,
    best_effort: bool = False,
) -> Dict[str, Any]:
    pipeline_name = str(pipeline or "").strip()
    now_ms = int(time.time() * 1000)
    status = _load_status(pipeline_name)

    status["pipeline"] = pipeline_name
    status["ok"] = bool(ok)
    status["updated_ts_ms"] = int(now_ms)
    status["raw_rows_last"] = int(raw_rows or 0)
    status["event_rows_last"] = int(event_rows or 0)
    status["raw_rows_total"] = int(status.get("raw_rows_total") or 0) + max(0, int(raw_rows or 0))
    status["event_rows_total"] = int(status.get("event_rows_total") or 0) + max(0, int(event_rows or 0))
    status["success_count"] = int(status.get("success_count") or 0) + (1 if ok else 0)
    status["failure_count"] = int(status.get("failure_count") or 0) + (0 if ok else 1)
    status["consecutive_failures"] = 0 if ok else int(status.get("consecutive_failures") or 0) + 1
    status["last_error"] = "" if ok else str(error or "")
    status["latency_ms"] = None if latency_ms is None else int(latency_ms)
    status["meta"] = dict(meta or {})
    if ok:
        status["last_success_ts_ms"] = int(now_ms)
    else:
        status["last_failure_ts_ms"] = int(now_ms)
    if last_ingested_ts_ms is not None:
        status["last_ingested_ts_ms"] = int(last_ingested_ts_ms)

    meta_set(
        _status_key(pipeline_name),
        json.dumps(status, separators=(",", ":"), sort_keys=True),
        best_effort=bool(best_effort),
    )

    if not _should_persist_pipeline_health_row(
        pipeline_name,
        ok=bool(ok),
        error=(None if ok else str(error or "")),
        best_effort=bool(best_effort),
        now_ms=int(now_ms),
    ):
        return status

    meta_json = json.dumps(status.get("meta") or {}, separators=(",", ":"), sort_keys=True)
    row = (
        int(now_ms),
        str(pipeline_name),
        1 if ok else 0,
        None if latency_ms is None else int(latency_ms),
        int(raw_rows or 0),
        int(event_rows or 0),
        None if last_ingested_ts_ms is None else int(last_ingested_ts_ms),
        (None if ok else str(error or "")[:1000]),
        str(meta_json),
    )
    append_ingestion_pipeline_health_row(
        row,
        prefer_buffer=_should_buffer_pipeline_health_row(best_effort=bool(best_effort)),
        attempts=(1 if bool(best_effort) else None),
        timeout_s=(0.25 if bool(best_effort) else None),
        busy_timeout_ms=(250 if bool(best_effort) else None),
    )
    return status


def get_pipeline_status(pipeline: str) -> Dict[str, Any]:
    return _load_status(pipeline)


def get_all_pipeline_statuses() -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    con = None
    try:
        con = connect_ro()
        rows = con.execute(
            """
            SELECT key, value
            FROM runtime_meta
            WHERE key LIKE ?
            ORDER BY key
            """,
            (f"{STATUS_PREFIX}%",),
        ).fetchall() or []
    except Exception:
        rows = []
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            try:
                from engine.runtime.failure_diagnostics import log_failure
                from engine.runtime.logging import get_logger

                log_failure(
                    get_logger("engine.runtime.ingestion_status"),
                    event="ingestion_status_connection_close_failed",
                    code="INGESTION_STATUS_CONNECTION_CLOSE_FAILED",
                    message="ingestion_status_connection_close_failed",
                    error=e,
                    level=30,
                    component="engine.runtime.ingestion_status",
                    persist=False,
                )
            except Exception:
                raise

    for key, value in rows:
        try:
            pipeline = str(key or "")[len(STATUS_PREFIX):]
            parsed = json.loads(str(value or "{}"))
            if isinstance(parsed, dict) and pipeline:
                out[pipeline] = parsed
        except Exception as e:
            from engine.runtime.failure_diagnostics import log_failure
            from engine.runtime.logging import get_logger
            log_failure(
                get_logger("engine.runtime.ingestion_status"),
                event="ingestion_status_row_parse_failed",
                code="INGESTION_STATUS_ROW_PARSE_FAILED",
                message=str(e),
                error=e,
                level=30,
                component="engine.runtime.ingestion_status",
                extra={"pipeline": str(pipeline)},
                persist=False,
            )
            continue
    return out


def pipeline_health_summary(*, stale_after_s: float = 900.0) -> Dict[str, Any]:
    now_ms = int(time.time() * 1000)
    max_age_ms = max(1, int(float(stale_after_s) * 1000.0))
    pipelines = get_all_pipeline_statuses()
    healthy = 0
    stale = 0
    not_live = 0
    simulated = 0
    missing_credentials = 0
    for status in pipelines.values():
        updated_ts_ms = int(status.get("updated_ts_ms") or 0)
        age_ms = max(0, now_ms - updated_ts_ms) if updated_ts_ms > 0 else 10**12
        status["age_ms"] = int(age_ms)
        status["stale"] = bool(age_ms > max_age_ms)
        truth = classify_pipeline_liveness(status)
        if bool(truth.get("applies")):
            status.update(truth)
            if not bool(truth.get("live_market_data_ok")) and bool(status.get("ok")) and not bool(status.get("stale")):
                not_live += 1
                if bool(truth.get("simulated")):
                    simulated += 1
                if str(truth.get("live_feed_status") or "") == "missing_credentials":
                    missing_credentials += 1
        counts_as_healthy = bool(status.get("ok")) and not bool(status.get("stale")) and bool(
            truth.get("live_market_data_ok", True)
        )
        if counts_as_healthy:
            healthy += 1
        if bool(status.get("stale")):
            stale += 1
    return {
        "ok": bool(pipelines) and healthy == len(pipelines),
        "total": int(len(pipelines)),
        "healthy": int(healthy),
        "stale": int(stale),
        "not_live": int(not_live),
        "simulated": int(simulated),
        "missing_credentials": int(missing_credentials),
        "stale_after_s": float(stale_after_s),
        "pipelines": pipelines,
        "updated_ts_ms": int(now_ms),
    }
