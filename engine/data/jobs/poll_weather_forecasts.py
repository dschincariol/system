"""
Weather forecast poller (region-daily aggregates).

Default provider: Open-Meteo (no key).
Stores immutable as-issued rows in weather_forecast_region_daily and emits
symbol-scoped weather forecast events for impacted symbols.
"""

import json
import logging
import os
import time
from typing import Any, Dict, Tuple

import requests

from engine.data.weather_event_factory import build_symbol_forecast_events
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

WEATHER_PROVIDER = os.environ.get("WEATHER_PROVIDER", "open_meteo").strip().lower()
POLL_SECONDS = int(os.environ.get("WEATHER_POLL_SECONDS", "21600"))
JOB_NAME = (
    os.environ.get("ENGINE_JOB_NAME")
    or os.environ.get("WEATHER_FORECAST_JOB_NAME")
    or "poll_weather_forecasts"
).strip() or "poll_weather_forecasts"
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

DD_BASE_F = float(os.environ.get("WEATHER_DD_BASE_F", "65.0"))
DD_BASE_C = (DD_BASE_F - 32.0) * (5.0 / 9.0)


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
            "weather_forecasts",
            event_type=str(event_type),
            message=str(message),
            detail=dict(detail or {}),
        )
    except Exception as e:
        _warn_nonfatal(
            "POLL_WEATHER_FORECASTS_LIFECYCLE_LOG_FAILED",
            e,
            once_key="poll_weather_forecasts_lifecycle_log",
            event_type=str(event_type),
            message=str(message),
        )


def _load_region_map() -> Dict[str, Any]:
    return load_weather_region_map()


def _iso_date_to_day_ts_ms(raw: str) -> int:
    try:
        import datetime as dt

        year, month, day = str(raw).split("-")
        value = dt.datetime(int(year), int(month), int(day), 0, 0, 0, tzinfo=dt.timezone.utc)
        return int(value.timestamp() * 1000)
    except Exception as e:
        _warn_nonfatal("POLL_WEATHER_FORECASTS_DATE_PARSE_FAILED", e, once_key="date_to_ts", raw=str(raw))
        return 0


def _hdd_cdd_from_temp_c(temp_mean_c: float) -> Tuple[float, float]:
    hdd = max(0.0, DD_BASE_C - float(temp_mean_c))
    cdd = max(0.0, float(temp_mean_c) - DD_BASE_C)
    return float(hdd), float(cdd)


def _heartbeat_payload(phase: str, **extra: Any) -> str:
    payload = {
        "phase": str(phase),
        "poll_seconds": int(POLL_SECONDS),
        "heartbeat_every_s": float(HEARTBEAT_EVERY_S),
    }
    payload.update({str(k): v for k, v in extra.items()})
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _sleep_with_heartbeat(manager) -> bool:
    deadline = time.time() + float(max(10, int(POLL_SECONDS)))
    while True:
        remaining = deadline - time.time()
        if remaining <= 0:
            return True
        if not manager.is_job_enabled(JOB_NAME, default=True):
            manager.record_job_status(JOB_NAME, ok=True, message="weather forecasts disabled by data source control plane")
            return False
        try:
            touch_job_lock(JOB_NAME, OWNER, PID)
            put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=_heartbeat_payload("sleep", remaining_s=max(0.0, remaining)))
        except Exception as e:
            _warn_nonfatal(
                "POLL_WEATHER_FORECASTS_SLEEP_HEARTBEAT_FAILED",
                e,
                once_key="poll_weather_forecasts_sleep_heartbeat",
                remaining_s=max(0.0, remaining),
            )
        time.sleep(min(float(HEARTBEAT_EVERY_S), max(1.0, remaining)))


def _fetch_open_meteo_daily(lat: float, lon: float) -> Dict[str, Any]:
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": float(lat),
        "longitude": float(lon),
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max",
        "timezone": "UTC",
    }
    response = requests.get(url, params=params, timeout=20)
    response.raise_for_status()
    return response.json() or {}


def _upsert_region_day(
    con,
    *,
    provider: str,
    region_id: str,
    run_ts: int,
    day_ts: int,
    temp_mean_c: float,
    hdd: float,
    cdd: float,
    wind_mean_mps: float,
    precip_sum_mm: float,
    spread: float,
    source_uri: str,
) -> int:
    cur = con.execute(
        """
        INSERT OR IGNORE INTO weather_forecast_region_daily(
          provider, region_id, run_ts, day_ts,
          temp_mean_c, hdd65, cdd65, wind_mean_mps, precip_sum_mm, spread,
          source_uri
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            str(provider),
            str(region_id),
            int(run_ts),
            int(day_ts),
            float(temp_mean_c),
            float(hdd),
            float(cdd),
            float(wind_mean_mps),
            float(precip_sum_mm),
            float(spread),
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

    now_ms = int(time.time() * 1000)
    put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=_heartbeat_payload("fetch", provider=WEATHER_PROVIDER))

    if True:
        inserted_rows = 0
        event_rows = 0
        errors = []
        last_ingested_ts_ms = now_ms
        for region_id, meta in regions.items():
            try:
                touch_job_lock(JOB_NAME, OWNER, PID)
                put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=_heartbeat_payload("region", region=str(region_id)))
            except Exception as e:
                _warn_nonfatal(
                    "POLL_WEATHER_FORECASTS_REGION_HEARTBEAT_FAILED",
                    e,
                    once_key="poll_weather_forecasts_region_heartbeat",
                    region_id=str(region_id),
                )
            try:
                lat = float(meta.get("lat"))
                lon = float(meta.get("lon"))
            except Exception as e:
                _warn_nonfatal(
                    "POLL_WEATHER_FORECASTS_COORD_PARSE_FAILED",
                    e,
                    once_key=f"coords:{region_id}",
                    region_id=str(region_id),
                    lat=repr(meta.get("lat"))[:120],
                    lon=repr(meta.get("lon"))[:120],
                )
                continue
            try:
                if WEATHER_PROVIDER != "open_meteo":
                    message = f"unsupported_weather_provider:{WEATHER_PROVIDER}"
                    _warn_state(
                        "POLL_WEATHER_FORECASTS_UNSUPPORTED_PROVIDER",
                        message,
                        provider=str(WEATHER_PROVIDER),
                    )
                    errors.append(message)
                    break
                payload = _fetch_open_meteo_daily(lat, lon)
                daily = (payload or {}).get("daily") or {}
                times = daily.get("time") or []
                tmax = daily.get("temperature_2m_max") or []
                tmin = daily.get("temperature_2m_min") or []
                precip = daily.get("precipitation_sum") or []
                wind_max = daily.get("windspeed_10m_max") or []
                source_uri = (
                    "https://api.open-meteo.com/v1/forecast"
                    f"?latitude={lat}&longitude={lon}"
                    "&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max"
                    "&timezone=UTC"
                )
                run_ts = now_ms
                n_rows = min(len(times), len(tmax), len(tmin), len(precip), len(wind_max))
                def _write_region(con):
                    local_inserted_rows = 0
                    local_event_rows = 0
                    local_last_ingested_ts_ms = 0
                    for idx in range(n_rows):
                        day_ts = _iso_date_to_day_ts_ms(str(times[idx]))
                        if day_ts <= 0:
                            continue
                        temp_mean_c = (float(tmax[idx]) + float(tmin[idx])) / 2.0
                        hdd, cdd = _hdd_cdd_from_temp_c(temp_mean_c)
                        wind_mean_mps = float(wind_max[idx]) / 3.6
                        precip_sum_mm = float(precip[idx] or 0.0)
                        local_inserted_rows += _upsert_region_day(
                            con,
                            provider=WEATHER_PROVIDER,
                            region_id=str(region_id),
                            run_ts=int(run_ts),
                            day_ts=int(day_ts),
                            temp_mean_c=float(temp_mean_c),
                            hdd=float(hdd),
                            cdd=float(cdd),
                            wind_mean_mps=float(wind_mean_mps),
                            precip_sum_mm=float(precip_sum_mm),
                            spread=float(meta.get("spread", 0.0) or 0.0),
                            source_uri=str(source_uri),
                        )
                        normalized_events = build_symbol_forecast_events(
                            region_id=str(region_id),
                            provider=WEATHER_PROVIDER,
                            run_ts=int(run_ts),
                            day_ts=int(day_ts),
                            temp_mean_c=float(temp_mean_c),
                            hdd=float(hdd),
                            cdd=float(cdd),
                            wind_mean_mps=float(wind_mean_mps),
                            precip_sum_mm=float(precip_sum_mm),
                            source_uri=str(source_uri),
                            cfg=cfg,
                        )
                        for event in normalized_events:
                            put_normalized_event(event, con=con)
                        local_event_rows += len(normalized_events)
                        local_last_ingested_ts_ms = max(local_last_ingested_ts_ms, int(day_ts))
                    return local_inserted_rows, local_event_rows, local_last_ingested_ts_ms

                batch_inserted_rows, batch_event_rows, batch_last_ingested_ts_ms = run_write_txn(
                    _write_region,
                    table="weather_forecast_regions",
                    operation="ingest_weather_forecast_region",
                    context={"job": JOB_NAME, "region_id": str(region_id), "rows": int(n_rows)},
                )
                inserted_rows += int(batch_inserted_rows or 0)
                event_rows += int(batch_event_rows or 0)
                last_ingested_ts_ms = max(last_ingested_ts_ms, int(batch_last_ingested_ts_ms or 0))
                logging.info("ingested daily forecast region=%s n_days=%s", region_id, n_rows)
            except Exception as exc:
                _warn_nonfatal("POLL_WEATHER_FORECASTS_REGION_FETCH_FAILED", exc, once_key=f"region_fetch:{region_id}", region_id=str(region_id))
                errors.append(f"{region_id}:{exc}")

        if not errors and inserted_rows <= 0 and event_rows <= 0:
            errors.append("weather_forecast_empty_response")

        health_error = None if not errors else "; ".join(errors[:3])
        append_weather_provider_health(
            provider=str(WEATHER_PROVIDER),
            ok=bool(len(errors) == 0),
            latency_ms=None,
            error=health_error,
            ts_ms=int(now_ms),
        )
        status = record_pipeline_status(
            JOB_NAME,
            ok=(len(errors) == 0),
            raw_rows=int(inserted_rows),
            event_rows=int(event_rows),
            last_ingested_ts_ms=int(last_ingested_ts_ms),
            error=("; ".join(errors[:3])) if errors else None,
            meta={"provider": WEATHER_PROVIDER, "regions_n": len(regions)},
            best_effort=True,
        )
        manager.record_job_status(
            JOB_NAME,
            ok=bool(len(errors) == 0),
            message="weather forecast cycle complete",
            error=("; ".join(errors[:3])) if errors else "",
            meta={"provider": WEATHER_PROVIDER, "regions_n": len(regions), "raw_rows": int(inserted_rows), "event_rows": int(event_rows)},
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
        _log_lifecycle(manager, "lifecycle", "weather forecasts disabled before start", job_name=JOB_NAME, pid=PID)
        _warn_state("POLL_WEATHER_FORECASTS_JOB_DISABLED", "Weather forecasts job is disabled before start.", job_name=JOB_NAME, desired_jobs=list(desired_jobs))
        manager.record_job_status(JOB_NAME, ok=True, message="weather forecasts disabled by data source control plane")
        return

    init_db()
    if not acquire_job_lock(JOB_NAME, OWNER, PID, stale_after_s=LOCK_STALE_AFTER_S):
        _log_lifecycle(manager, "lifecycle", "weather forecasts lock not acquired", job_name=JOB_NAME, owner=OWNER, pid=PID)
        _warn_state("POLL_WEATHER_FORECASTS_LOCK_NOT_ACQUIRED", "Weather forecasts job lock was not acquired.", job_name=JOB_NAME, owner=OWNER, pid=PID, desired_jobs=list(desired_jobs))
        return
    try:
        _log_lifecycle(
            manager,
            "lifecycle",
            "weather forecasts loop start",
            job_name=JOB_NAME,
            owner=OWNER,
            pid=PID,
            poll_seconds=POLL_SECONDS,
        )
        logging.info(
            "weather forecasts loop starting job_name=%s provider=%s poll_seconds=%s owner=%s pid=%s",
            JOB_NAME,
            WEATHER_PROVIDER,
            POLL_SECONDS,
            OWNER,
            PID,
        )
        last_hb = 0.0
        while True:
            if not manager.is_job_enabled(JOB_NAME, default=True):
                _log_lifecycle(manager, "lifecycle", "weather forecasts disabled in loop", job_name=JOB_NAME, pid=PID)
                manager.record_job_status(JOB_NAME, ok=True, message="weather forecasts disabled by data source control plane")
                break
            now_s = time.time()
            if now_s - last_hb >= HEARTBEAT_EVERY_S:
                touch_job_lock(JOB_NAME, OWNER, PID)
                put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=_heartbeat_payload("loop"))
                last_hb = now_s
            try:
                _log_lifecycle(manager, "lifecycle", "weather forecasts cycle begin", job_name=JOB_NAME, pid=PID)
                logging.info("weather forecasts cycle begin job_name=%s pid=%s", JOB_NAME, PID)
                _run_once()
                _log_lifecycle(
                    manager,
                    "lifecycle",
                    "weather forecasts cycle complete",
                    job_name=JOB_NAME,
                    pid=PID,
                    sleep_s=max(10, int(POLL_SECONDS)),
                )
                logging.info(
                    "weather forecasts cycle complete job_name=%s pid=%s sleeping_s=%s",
                    JOB_NAME,
                    PID,
                    max(10, int(POLL_SECONDS)),
                )
            except Exception as exc:
                logging.exception("weather_forecast_cycle_failed")
                status = record_pipeline_status(
                    JOB_NAME,
                    ok=False,
                    raw_rows=0,
                    event_rows=0,
                    last_ingested_ts_ms=int(time.time() * 1000),
                    error=str(exc),
                    meta={"provider": WEATHER_PROVIDER},
                )
                manager.record_job_status(JOB_NAME, ok=False, message="weather forecast cycle failed", error=str(exc), meta={"provider": WEATHER_PROVIDER})
                put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
            slept = _sleep_with_heartbeat(manager)
            _log_lifecycle(manager, "lifecycle", "weather forecasts sleep return", job_name=JOB_NAME, pid=PID, slept=bool(slept))
            if not slept:
                break
    finally:
        _log_lifecycle(manager, "lifecycle", "weather forecasts releasing lock", job_name=JOB_NAME, owner=OWNER, pid=PID)
        logging.info("weather forecasts releasing lock job_name=%s owner=%s pid=%s", JOB_NAME, OWNER, PID)
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    main()
