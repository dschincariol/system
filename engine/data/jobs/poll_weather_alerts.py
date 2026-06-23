"""
Weather alerts poller (event stream).

Provider: NWS (api.weather.gov)
Stores alerts into weather_alerts and emits symbol-scoped weather alert events
for impacted symbols.
"""

import json
import logging
import os
import time
from typing import Any, Dict, List

import requests

from engine.data.weather_event_factory import build_symbol_alert_events
from engine.data.weather_mapping import load_weather_region_map
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.ingestion_status import record_pipeline_status
from engine.runtime.storage import (
    acquire_job_lock,
    init_db,
    put_job_heartbeat,
    put_normalized_event,
    release_job_lock,
    run_write_txn,
    touch_job_lock,
)
from engine.runtime.telemetry_append_buffer import append_weather_provider_health
from services.data_source_manager import get_manager

WEATHER_ALERTS_PROVIDER = os.environ.get("WEATHER_ALERTS_PROVIDER", "nws").strip().lower()
POLL_SECONDS = int(os.environ.get("WEATHER_ALERTS_POLL_SECONDS", "900"))
JOB_NAME = (
    os.environ.get("ENGINE_JOB_NAME")
    or os.environ.get("WEATHER_ALERTS_JOB_NAME")
    or "poll_weather_alerts"
).strip() or "poll_weather_alerts"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "300"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "30.0"))

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format=f"%(asctime)s %(levelname)s [{JOB_NAME}] %(message)s",
)
LOGGER = logging.getLogger(__name__)
_WARNED_NONFATAL_KEYS: set[str] = set()

UA = os.environ.get("WEATHER_HTTP_UA", "trading-system/1.0 (admin@example.com)")
_NWS_COOLDOWN_UNTIL_S = 0.0


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


def _warn_state(code: str, message: str, **extra: Any) -> None:
    log_failure(
        LOGGER,
        event=str(code).lower(),
        code=str(code),
        message=str(message),
        error=None,
        level=logging.WARNING,
        component=__name__,
        extra=extra or None,
        persist=False,
    )


def _log_lifecycle(manager, event_type: str, message: str, **detail: Any) -> None:
    try:
        manager.log_event(
            "weather_alerts",
            event_type=str(event_type),
            message=str(message),
            detail=dict(detail or {}),
        )
    except Exception as e:
        _warn_nonfatal(
            "POLL_WEATHER_ALERTS_LIFECYCLE_LOG_FAILED",
            e,
            once_key="poll_weather_alerts_lifecycle_log",
            event_type=str(event_type),
            message=str(message),
        )


def _load_region_map() -> Dict[str, Any]:
    return load_weather_region_map()


def _heartbeat_payload(phase: str, **extra: Any) -> str:
    payload = {
        "phase": str(phase),
        "poll_seconds": int(POLL_SECONDS),
        "heartbeat_every_s": float(HEARTBEAT_EVERY_S),
    }
    payload.update({str(k): v for k, v in extra.items()})
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _sleep_with_heartbeat(manager) -> bool:
    cooldown = max(0.0, float(_NWS_COOLDOWN_UNTIL_S) - time.time())
    deadline = time.time() + float(max(30, int(POLL_SECONDS), int(cooldown)))
    while True:
        remaining = deadline - time.time()
        if remaining <= 0:
            return True
        if not manager.is_job_enabled(JOB_NAME, default=True):
            manager.record_job_status(JOB_NAME, ok=True, message="weather alerts disabled by data source control plane")
            return False
        try:
            touch_job_lock(JOB_NAME, OWNER, PID)
            put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=_heartbeat_payload("sleep", remaining_s=max(0.0, remaining)))
        except Exception as e:
            _warn_nonfatal(
                "POLL_WEATHER_ALERTS_SLEEP_HEARTBEAT_FAILED",
                e,
                once_key="poll_weather_alerts_sleep_heartbeat",
                remaining_s=max(0.0, remaining),
            )
        time.sleep(min(float(HEARTBEAT_EVERY_S), max(1.0, remaining)))


def _fetch_nws_active(area: str) -> Dict[str, Any]:
    global _NWS_COOLDOWN_UNTIL_S
    cooldown = max(0.0, float(_NWS_COOLDOWN_UNTIL_S) - time.time())
    if cooldown > 0:
        raise RuntimeError(f"nws_rate_limited:cooldown_remaining_s={cooldown:.0f}")
    url = "https://api.weather.gov/alerts/active"
    params = {}
    if area:
        params["area"] = str(area).upper()
    headers = {"User-Agent": UA, "Accept": "application/geo+json"}
    response = requests.get(url, params=params, headers=headers, timeout=20)
    status_code = int(getattr(response, "status_code", 0) or 0)
    if status_code == 429:
        retry_after = 300.0
        try:
            retry_after = max(1.0, float((getattr(response, "headers", {}) or {}).get("Retry-After") or 300.0))
        except Exception:
            retry_after = 300.0
        _NWS_COOLDOWN_UNTIL_S = max(float(_NWS_COOLDOWN_UNTIL_S), time.time() + retry_after)
        raise RuntimeError(f"nws_rate_limited:retry_after_s={retry_after:.0f}")
    if status_code == 503:
        retry_after = 300.0
        try:
            retry_after = max(1.0, float((getattr(response, "headers", {}) or {}).get("Retry-After") or 300.0))
        except Exception:
            retry_after = 300.0
        _NWS_COOLDOWN_UNTIL_S = max(float(_NWS_COOLDOWN_UNTIL_S), time.time() + retry_after)
        raise RuntimeError(f"nws_temporarily_unavailable:retry_after_s={retry_after:.0f}")
    if status_code == 401:
        raise RuntimeError("nws_credentials_rejected:status_code=401")
    if status_code == 403:
        raise RuntimeError("nws_entitlement_missing:status_code=403")
    response.raise_for_status()
    payload = response.json() or {}
    if not isinstance(payload, dict) or str(payload.get("type") or "") != "FeatureCollection" or not isinstance(payload.get("features"), list):
        raise RuntimeError("nws_alerts_malformed_payload")
    return payload


def _ts_to_ms(value: Any) -> int:
    try:
        import datetime as dt

        raw = str(value or "").strip()
        if not raw:
            return 0
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        parsed = dt.datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return int(parsed.timestamp() * 1000)
    except Exception as e:
        _warn_nonfatal("POLL_WEATHER_ALERTS_PARSE_TS_FAILED", e, once_key="parse_ts", value=repr(value)[:120])
        return 0


def _upsert_alert(
    con,
    *,
    provider: str,
    alert_id: str,
    issued_ts: int,
    effective_ts: int,
    expires_ts: int,
    event: str,
    severity: str,
    urgency: str,
    certainty: str,
    area_desc: str,
    polygon_geojson: str,
    affected_regions_json: str,
    headline: str,
    description: str,
    source_uri: str,
) -> int:
    cur = con.execute(
        """
        INSERT OR IGNORE INTO weather_alerts(
          provider, alert_id, issued_ts, effective_ts, expires_ts,
          event, severity, urgency, certainty,
          area_desc, polygon_geojson, affected_regions,
          headline, description, source_uri
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            str(provider),
            str(alert_id),
            int(issued_ts),
            int(effective_ts) if effective_ts else None,
            int(expires_ts) if expires_ts else None,
            str(event) if event else None,
            str(severity) if severity else None,
            str(urgency) if urgency else None,
            str(certainty) if certainty else None,
            str(area_desc) if area_desc else None,
            polygon_geojson,
            affected_regions_json,
            str(headline) if headline else None,
            str(description) if description else None,
            str(source_uri) if source_uri else None,
        ),
    )
    return 1 if int(cur.rowcount or 0) > 0 else 0


def _run_once() -> None:
    manager = get_manager()
    cfg = _load_region_map()
    regions = (cfg or {}).get("regions") or {}
    if not isinstance(regions, dict) or not regions:
        logging.info("no regions configured in weather region map; nothing to do")
        return

    area_to_regions: Dict[str, List[str]] = {}
    for region_id, meta in regions.items():
        area = str((meta or {}).get("nws_area") or "").strip().upper()
        if not area:
            continue
        area_to_regions.setdefault(area, []).append(str(region_id))
    if not area_to_regions:
        logging.info("no regions have nws_area configured; nothing to do")
        return

    put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=_heartbeat_payload("fetch", provider=WEATHER_ALERTS_PROVIDER))
    if True:
        inserted_rows = 0
        event_rows = 0
        errors = []
        last_ingested_ts_ms = int(time.time() * 1000)
        for area, region_ids in area_to_regions.items():
            try:
                touch_job_lock(JOB_NAME, OWNER, PID)
                put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=_heartbeat_payload("area", area=str(area)))
            except Exception as e:
                _warn_nonfatal(
                    "POLL_WEATHER_ALERTS_AREA_HEARTBEAT_FAILED",
                    e,
                    once_key="poll_weather_alerts_area_heartbeat",
                    area=str(area),
                )
            try:
                if WEATHER_ALERTS_PROVIDER != "nws":
                    raise RuntimeError(f"unsupported WEATHER_ALERTS_PROVIDER={WEATHER_ALERTS_PROVIDER}")
                payload = _fetch_nws_active(area)
                features = payload.get("features") or []
                def _write_area(con):
                    local_inserted_rows = 0
                    local_event_rows = 0
                    local_last_ingested_ts_ms = 0
                    local_batch_count = 0
                    for feature in features:
                        props = (feature or {}).get("properties") or {}
                        alert_id = str(props.get("id") or props.get("@id") or props.get("api") or "").strip()
                        if not alert_id:
                            continue
                        if "://" in alert_id and "/" in alert_id:
                            alert_id = alert_id.rstrip("/").split("/")[-1]
                        issued_ms = _ts_to_ms(props.get("sent") or props.get("issued") or props.get("onset") or props.get("effective"))
                        effective_ms = _ts_to_ms(props.get("effective") or props.get("onset"))
                        expires_ms = _ts_to_ms(props.get("expires"))
                        local_inserted_rows += _upsert_alert(
                            con,
                            provider=WEATHER_ALERTS_PROVIDER,
                            alert_id=alert_id,
                            issued_ts=int(issued_ms) if issued_ms else int(time.time() * 1000),
                            effective_ts=int(effective_ms) if effective_ms else 0,
                            expires_ts=int(expires_ms) if expires_ms else 0,
                            event=str(props.get("event") or ""),
                            severity=str(props.get("severity") or ""),
                            urgency=str(props.get("urgency") or ""),
                            certainty=str(props.get("certainty") or ""),
                            area_desc=str(props.get("areaDesc") or ""),
                            polygon_geojson=json.dumps((feature or {}).get("geometry")) if (feature or {}).get("geometry") is not None else "",
                            affected_regions_json=json.dumps(region_ids),
                            headline=str(props.get("headline") or ""),
                            description=str(props.get("description") or ""),
                            source_uri=str(props.get("@id") or ""),
                        )
                        normalized_events = build_symbol_alert_events(
                            alert_id=alert_id,
                            provider=WEATHER_ALERTS_PROVIDER,
                            issued_ms=int(issued_ms) if issued_ms else int(time.time() * 1000),
                            expires_ms=int(expires_ms) if expires_ms else 0,
                            region_ids=list(region_ids),
                            event_name=str(props.get("event") or ""),
                            severity=str(props.get("severity") or ""),
                            urgency=str(props.get("urgency") or ""),
                            certainty=str(props.get("certainty") or ""),
                            headline=str(props.get("headline") or props.get("event") or "Weather alert"),
                            description=str(props.get("description") or props.get("areaDesc") or ""),
                            url=str(props.get("@id") or ""),
                            cfg=cfg,
                        )
                        for event in normalized_events:
                            put_normalized_event(event, con=con)
                        local_event_rows += len(normalized_events)
                        local_last_ingested_ts_ms = max(local_last_ingested_ts_ms, int(issued_ms or effective_ms or 0))
                        local_batch_count += 1
                    return local_inserted_rows, local_event_rows, local_last_ingested_ts_ms, local_batch_count

                batch_inserted_rows, batch_event_rows, batch_last_ingested_ts_ms, batch_count = run_write_txn(
                    _write_area,
                    table="weather_alerts",
                    operation="ingest_weather_alerts_area",
                    context={"job": JOB_NAME, "area": str(area), "features": int(len(features))},
                )
                inserted_rows += int(batch_inserted_rows or 0)
                event_rows += int(batch_event_rows or 0)
                last_ingested_ts_ms = max(last_ingested_ts_ms, int(batch_last_ingested_ts_ms or 0))
                logging.info("ingested nws alerts area=%s n=%s", area, batch_count)
            except Exception as exc:
                _warn_nonfatal("POLL_WEATHER_ALERTS_AREA_FETCH_FAILED", exc, once_key=f"area_fetch:{area}", area=str(area))
                errors.append(f"{area}:{exc}")

        health_ts_ms = int(time.time() * 1000)
        health_error = None if not errors else "; ".join(errors[:3])
        append_weather_provider_health(
            provider=str(WEATHER_ALERTS_PROVIDER),
            ok=bool(len(errors) == 0),
            latency_ms=None,
            error=health_error,
            ts_ms=int(health_ts_ms),
        )
        status = record_pipeline_status(
            JOB_NAME,
            ok=(len(errors) == 0),
            raw_rows=int(inserted_rows),
            event_rows=int(event_rows),
            last_ingested_ts_ms=int(last_ingested_ts_ms or time.time() * 1000),
            error=("; ".join(errors[:3])) if errors else None,
            meta={"provider": WEATHER_ALERTS_PROVIDER, "areas_n": len(area_to_regions)},
            best_effort=True,
        )
        manager.record_job_status(
            JOB_NAME,
            ok=bool(len(errors) == 0),
            message="weather alerts cycle complete",
            error=("; ".join(errors[:3])) if errors else "",
            meta={"provider": WEATHER_ALERTS_PROVIDER, "areas_n": len(area_to_regions), "raw_rows": int(inserted_rows), "event_rows": int(event_rows)},
        )
        put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))

def main() -> None:
    manager = get_manager()
    desired_jobs = []
    try:
        desired_jobs = list(manager.get_desired_ingestion_jobs() or [])
    except Exception:
        desired_jobs = []
    if not manager.is_job_enabled(JOB_NAME, default=True):
        _log_lifecycle(manager, "lifecycle", "weather alerts disabled before start", job_name=JOB_NAME, pid=PID)
        _warn_state("POLL_WEATHER_ALERTS_JOB_DISABLED", "Weather alerts job is disabled before start.", job_name=JOB_NAME, desired_jobs=list(desired_jobs))
        manager.record_job_status(JOB_NAME, ok=True, message="weather alerts disabled by data source control plane")
        return

    init_db()
    if not acquire_job_lock(JOB_NAME, OWNER, PID, stale_after_s=LOCK_STALE_AFTER_S):
        _log_lifecycle(manager, "lifecycle", "weather alerts lock not acquired", job_name=JOB_NAME, owner=OWNER, pid=PID)
        _warn_state("POLL_WEATHER_ALERTS_LOCK_NOT_ACQUIRED", "Weather alerts job lock was not acquired.", job_name=JOB_NAME, owner=OWNER, pid=PID, desired_jobs=list(desired_jobs))
        return
    try:
        _log_lifecycle(
            manager,
            "lifecycle",
            "weather alerts loop start",
            job_name=JOB_NAME,
            owner=OWNER,
            pid=PID,
            poll_seconds=POLL_SECONDS,
        )
        logging.info(
            "weather alerts loop starting job_name=%s provider=%s poll_seconds=%s owner=%s pid=%s",
            JOB_NAME,
            WEATHER_ALERTS_PROVIDER,
            POLL_SECONDS,
            OWNER,
            PID,
        )
        last_hb = 0.0
        while True:
            if not manager.is_job_enabled(JOB_NAME, default=True):
                _log_lifecycle(manager, "lifecycle", "weather alerts disabled in loop", job_name=JOB_NAME, pid=PID)
                manager.record_job_status(JOB_NAME, ok=True, message="weather alerts disabled by data source control plane")
                break
            now_s = time.time()
            if now_s - last_hb >= HEARTBEAT_EVERY_S:
                touch_job_lock(JOB_NAME, OWNER, PID)
                put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=_heartbeat_payload("loop"))
                last_hb = now_s
            try:
                _log_lifecycle(manager, "lifecycle", "weather alerts cycle begin", job_name=JOB_NAME, pid=PID)
                logging.info("weather alerts cycle begin job_name=%s pid=%s", JOB_NAME, PID)
                _run_once()
                _log_lifecycle(
                    manager,
                    "lifecycle",
                    "weather alerts cycle complete",
                    job_name=JOB_NAME,
                    pid=PID,
                    sleep_s=max(30, int(POLL_SECONDS)),
                )
                logging.info(
                    "weather alerts cycle complete job_name=%s pid=%s sleeping_s=%s",
                    JOB_NAME,
                    PID,
                    max(30, int(POLL_SECONDS)),
                )
            except Exception as exc:
                logging.exception("weather_alert_cycle_failed")
                status = record_pipeline_status(
                    JOB_NAME,
                    ok=False,
                    raw_rows=0,
                    event_rows=0,
                    last_ingested_ts_ms=int(time.time() * 1000),
                    error=str(exc),
                    meta={"provider": WEATHER_ALERTS_PROVIDER},
                )
                manager.record_job_status(JOB_NAME, ok=False, message="weather alerts cycle failed", error=str(exc), meta={"provider": WEATHER_ALERTS_PROVIDER})
                put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
            slept = _sleep_with_heartbeat(manager)
            _log_lifecycle(manager, "lifecycle", "weather alerts sleep return", job_name=JOB_NAME, pid=PID, slept=bool(slept))
            if not slept:
                break
    finally:
        _log_lifecycle(manager, "lifecycle", "weather alerts releasing lock", job_name=JOB_NAME, owner=OWNER, pid=PID)
        logging.info("weather alerts releasing lock job_name=%s owner=%s pid=%s", JOB_NAME, OWNER, PID)
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    main()
