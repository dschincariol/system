import json
import math
import os
import logging
from typing import Dict, List, Optional

from engine.data.weather_mapping import (
    alert_severity_score,
    load_weather_region_map,
    score_weather_conditions,
    symbol_regions,
)
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect, table_exists

WEATHER_PROVIDER = os.environ.get("WEATHER_PROVIDER", "open_meteo").strip().lower()
WEATHER_ALERTS_PROVIDER = os.environ.get("WEATHER_ALERTS_PROVIDER", "nws").strip().lower()

HORIZON_3D = int(os.environ.get("WEATHER_HORIZON_3D", "3"))
HORIZON_7D = int(os.environ.get("WEATHER_HORIZON_7D", "7"))
LOG = get_logger("engine.data.weather_features")

WEATHER_FEATURE_ZERO_VALUES: Dict[str, float] = {
    "hdd_3d": 0.0,
    "hdd_7d": 0.0,
    "cdd_3d": 0.0,
    "cdd_7d": 0.0,
    "precip_7d": 0.0,
    "wind_3d": 0.0,
    "spread_7d": 0.0,
    "storm_risk": 0.0,
    "anomaly_score": 0.0,
    "extreme_event_score": 0.0,
    "alert_severity": 0.0,
    "temp_anomaly_3d": 0.0,
    "wind_anomaly_3d": 0.0,
    "precip_anomaly_7d": 0.0,
}


def zero_weather_feature_snapshot() -> Dict[str, float]:
    return dict(WEATHER_FEATURE_ZERO_VALUES)


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="weather_features_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.data.weather_features",
        extra=extra or None,
        persist=False,
    )


def _day_start_utc_ms(ts_ms: int) -> int:
    return (int(ts_ms) // 86_400_000) * 86_400_000


def _latest_run_asof(con, provider: str, region_id: str, asof_ms: int) -> Optional[int]:
    row = con.execute(
        """
        SELECT MAX(run_ts)
        FROM weather_forecast_region_daily
        WHERE provider=? AND region_id=? AND run_ts <= ?
        """,
        (str(provider), str(region_id), int(asof_ms)),
    ).fetchone()
    if not row:
        return None
    try:
        value = int(row[0] or 0)
    except Exception:
        value = 0
    return value if value > 0 else None


def _fetch_days(
    con,
    provider: str,
    region_id: str,
    run_ts: int,
    day0_ms: int,
    dayN_ms: int,
) -> List[Dict[str, float]]:
    rows = con.execute(
        """
        SELECT day_ts, temp_mean_c, hdd65, cdd65, wind_mean_mps, precip_sum_mm, spread
        FROM weather_forecast_region_daily
        WHERE provider=? AND region_id=? AND run_ts=? AND day_ts BETWEEN ? AND ?
        ORDER BY day_ts ASC
        """,
        (str(provider), str(region_id), int(run_ts), int(day0_ms), int(dayN_ms)),
    ).fetchall()
    out = []
    for row in rows or []:
        try:
            out.append(
                {
                    "day_ts": int(row[0]),
                    "temp_mean_c": float(row[1] or 0.0),
                    "hdd": float(row[2] or 0.0),
                    "cdd": float(row[3] or 0.0),
                    "wind_mean_mps": float(row[4] or 0.0),
                    "precip_sum_mm": float(row[5] or 0.0),
                    "spread": float(row[6] or 0.0),
                }
            )
        except Exception as e:
            _warn_nonfatal("WEATHER_FEATURES_SNAPSHOT_ROW_PARSE_FAILED", e, row=repr(row)[:200])
            continue
    return out


def _sum(rows: List[Dict[str, float]], key: str) -> float:
    return float(sum(float(row.get(key, 0.0) or 0.0) for row in rows or []))


def _mean(rows: List[Dict[str, float]], key: str) -> float:
    if not rows:
        return 0.0
    return _sum(rows, key) / float(len(rows))


def _active_alert_state(con, region_ids: List[str], ts_ms: int) -> Dict[str, float]:
    if not region_ids:
        return {"alert_severity": 0.0, "alert_count": 0.0}
    min_issued = int(ts_ms) - 14 * 24 * 3600 * 1000
    rows = con.execute(
        """
        SELECT issued_ts, expires_ts, severity, urgency, certainty, affected_regions
        FROM weather_alerts
        WHERE provider=? AND issued_ts >= ?
        ORDER BY issued_ts DESC
        """,
        (str(WEATHER_ALERTS_PROVIDER), int(min_issued)),
    ).fetchall()
    region_set = {str(region_id) for region_id in region_ids}
    total_score = 0.0
    alert_count = 0
    for issued_ts, expires_ts, severity, urgency, certainty, affected_regions in rows or []:
        try:
            issued_value = int(issued_ts or 0)
            expires_value = int(expires_ts or 0) if expires_ts is not None else 0
            if issued_value > int(ts_ms):
                continue
            if expires_value and int(ts_ms) > expires_value:
                continue
            parsed_regions = json.loads(affected_regions) if affected_regions else []
            if not isinstance(parsed_regions, list):
                parsed_regions = []
            if not region_set.intersection({str(value) for value in parsed_regions}):
                continue
            total_score += alert_severity_score(
                severity=severity,
                urgency=urgency,
                certainty=certainty,
            )
            alert_count += 1
        except Exception as e:
            _warn_nonfatal(
                "WEATHER_FEATURES_ALERT_ROW_PARSE_FAILED",
                e,
                row=repr((issued_ts, expires_ts, severity, urgency, certainty, affected_regions))[:200],
            )
            continue
    severity = 0.0 if total_score <= 0 else float(1.0 - math.exp(-0.85 * total_score))
    return {"alert_severity": severity, "alert_count": float(alert_count)}


def get_weather_feature_snapshot(*, symbol: str, ts_ms: int) -> Dict[str, float]:
    cfg = load_weather_region_map()
    regions = symbol_regions(symbol, cfg)
    base = zero_weather_feature_snapshot()
    if not regions:
        return dict(base)

    day0 = _day_start_utc_ms(int(ts_ms))
    day3 = day0 + (max(1, int(HORIZON_3D)) * 86_400_000) - 86_400_000
    day7 = day0 + (max(1, int(HORIZON_7D)) * 86_400_000) - 86_400_000

    agg = dict(base)
    con = connect()
    try:
        if not table_exists(con, "weather_forecast_region_daily"):
            return dict(base)

        for region_id, weight, _channels in regions:
            run_ts = _latest_run_asof(con, WEATHER_PROVIDER, str(region_id), int(ts_ms))
            if not run_ts:
                continue
            rows3 = _fetch_days(con, WEATHER_PROVIDER, str(region_id), int(run_ts), int(day0), int(day3))
            rows7 = _fetch_days(con, WEATHER_PROVIDER, str(region_id), int(run_ts), int(day0), int(day7))
            if not rows3 and not rows7:
                continue

            mean_temp_3d = _mean(rows3, "temp_mean_c")
            mean_wind_3d = _mean(rows3, "wind_mean_mps")
            precip_7d = _sum(rows7, "precip_sum_mm")

            condition_scores = score_weather_conditions(
                region_id=str(region_id),
                temp_mean_c=mean_temp_3d,
                wind_mean_mps=mean_wind_3d,
                precip_sum_mm=precip_7d,
                precip_window_days=max(1, int(HORIZON_7D)),
                cfg=cfg,
            )

            agg["hdd_3d"] += float(weight) * _sum(rows3, "hdd")
            agg["hdd_7d"] += float(weight) * _sum(rows7, "hdd")
            agg["cdd_3d"] += float(weight) * _sum(rows3, "cdd")
            agg["cdd_7d"] += float(weight) * _sum(rows7, "cdd")
            agg["precip_7d"] += float(weight) * precip_7d
            agg["wind_3d"] += float(weight) * mean_wind_3d
            agg["spread_7d"] += float(weight) * _mean(rows7, "spread")
            agg["anomaly_score"] += float(weight) * float(condition_scores["anomaly_score"])
            agg["extreme_event_score"] += float(weight) * float(condition_scores["extreme_event_score"])
            agg["temp_anomaly_3d"] += float(weight) * float(condition_scores["temp_anomaly_c"])
            agg["wind_anomaly_3d"] += float(weight) * float(condition_scores["wind_anomaly_mps"])
            agg["precip_anomaly_7d"] += float(weight) * float(condition_scores["precip_anomaly_mm"])

        if table_exists(con, "weather_alerts"):
            alert_state = _active_alert_state(con, [region_id for region_id, _weight, _channels in regions], int(ts_ms))
            agg["alert_severity"] = float(alert_state.get("alert_severity", 0.0) or 0.0)
        agg["storm_risk"] = float(
            max(
                agg["alert_severity"],
                0.65 * float(agg["extreme_event_score"]) + 0.35 * float(agg["anomaly_score"]),
            )
        )
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("WEATHER_FEATURES_CLOSE_FAILED", e, symbol=str(symbol or ""))

    return {key: float(value or 0.0) for key, value in agg.items()}
