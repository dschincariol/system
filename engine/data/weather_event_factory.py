from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from engine.data.event_normalization import normalize_weather_event
from engine.data.weather_mapping import (
    alert_severity_score,
    load_weather_region_map,
    region_impacted_symbols,
    score_weather_conditions,
)


def build_symbol_forecast_events(
    *,
    region_id: str,
    provider: str,
    run_ts: int,
    day_ts: int,
    temp_mean_c: float,
    hdd: float,
    cdd: float,
    wind_mean_mps: float,
    precip_sum_mm: float,
    source_uri: str,
    cfg: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    config = cfg or load_weather_region_map()
    scores = score_weather_conditions(
        region_id=str(region_id),
        temp_mean_c=float(temp_mean_c),
        wind_mean_mps=float(wind_mean_mps),
        precip_sum_mm=float(precip_sum_mm),
        precip_window_days=1,
        cfg=config,
    )
    impacted = region_impacted_symbols(str(region_id), config)
    affected_symbols = [str(row.get("symbol")) for row in impacted if str(row.get("symbol") or "").strip()]
    common = {
        "weather_kind": "forecast",
        "ts_ms": int(run_ts),
        "source": "weather_forecast",
        "provider": str(provider),
        "region_id": str(region_id),
        "run_ts": int(run_ts),
        "day_ts": int(day_ts),
        "temp_mean_c": float(temp_mean_c),
        "hdd65": float(hdd),
        "cdd65": float(cdd),
        "wind_mean_mps": float(wind_mean_mps),
        "precip_sum_mm": float(precip_sum_mm),
        "anomaly_score": float(scores["anomaly_score"]),
        "extreme_event_score": float(scores["extreme_event_score"]),
        "alert_severity": 0.0,
        "affected_symbols": affected_symbols,
        "source_uri": str(source_uri),
        "url": str(source_uri),
    }

    if not impacted:
        return [
            normalize_weather_event(
                {
                    **common,
                    "title": f"{region_id} weather forecast",
                    "body": (
                        f"Forecast for {region_id} on {day_ts}: "
                        f"temp_mean_c={round(float(temp_mean_c), 2)} "
                        f"precip_mm={round(float(precip_sum_mm), 2)} "
                        f"wind_mps={round(float(wind_mean_mps), 2)}"
                    ),
                    "forecast_id": f"{provider}:{region_id}:{run_ts}:{day_ts}",
                    "event_key": f"weather_forecast:{provider}:{region_id}:{run_ts}:{day_ts}",
                }
            )
        ]

    out = []
    for mapping in impacted:
        symbol = str(mapping.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        out.append(
            normalize_weather_event(
                {
                    **common,
                    "symbol": symbol,
                    "impact_weight": float(mapping.get("weight") or 0.0),
                    "impact_channels": list(mapping.get("channels") or []),
                    "title": f"{symbol} weather forecast impact",
                    "body": (
                        f"{symbol} exposed to {region_id} weather on {day_ts}: "
                        f"anomaly={round(float(scores['anomaly_score']), 3)} "
                        f"extreme={round(float(scores['extreme_event_score']), 3)}"
                    ),
                    "forecast_id": f"{provider}:{region_id}:{run_ts}:{day_ts}",
                    "event_key": f"weather_forecast:{provider}:{region_id}:{run_ts}:{day_ts}:{symbol}",
                }
            )
        )
    return out


def build_symbol_alert_events(
    *,
    alert_id: str,
    provider: str,
    issued_ms: int,
    expires_ms: int,
    region_ids: List[str],
    event_name: str,
    severity: str,
    urgency: str,
    certainty: str,
    headline: str,
    description: str,
    url: str,
    cfg: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    config = cfg or load_weather_region_map()
    impacted: Dict[str, Dict[str, Any]] = {}
    for region_id in region_ids:
        for mapping in region_impacted_symbols(str(region_id), config):
            symbol = str(mapping.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            row = impacted.setdefault(symbol, {"weight": 0.0, "channels": set()})
            row["weight"] += float(mapping.get("weight") or 0.0)
            for channel in mapping.get("channels") or []:
                if channel:
                    row["channels"].add(str(channel).strip().lower())

    severity_value = alert_severity_score(severity=severity, urgency=urgency, certainty=certainty)
    affected_symbols = sorted(impacted.keys())
    common = {
        "weather_kind": "alert",
        "ts_ms": int(issued_ms or int(time.time() * 1000)),
        "source": "weather_alert",
        "provider": str(provider),
        "alert_id": str(alert_id),
        "event": str(event_name or ""),
        "severity": str(severity or ""),
        "urgency": str(urgency or ""),
        "certainty": str(certainty or ""),
        "affected_regions": list(region_ids or []),
        "affected_symbols": affected_symbols,
        "expires_ts": int(expires_ms or 0),
        "anomaly_score": float(severity_value),
        "extreme_event_score": float(severity_value),
        "alert_severity": float(severity_value),
        "title": str(headline or event_name or "Weather alert"),
        "body": str(description or ""),
        "description": str(description or ""),
        "url": str(url or ""),
    }

    if not impacted:
        return [
            normalize_weather_event(
                {
                    **common,
                    "event_key": f"weather_alert:{provider}:{alert_id}",
                }
            )
        ]

    total_weight = sum(abs(float(row.get("weight") or 0.0)) for row in impacted.values())
    out = []
    for symbol in sorted(impacted.keys()):
        row = impacted[symbol]
        impact_weight = float(row.get("weight") or 0.0)
        if total_weight > 1e-12:
            impact_weight = impact_weight / total_weight
        out.append(
            normalize_weather_event(
                {
                    **common,
                    "symbol": symbol,
                    "impact_weight": float(impact_weight),
                    "impact_channels": sorted(row.get("channels") or []),
                    "event_key": f"weather_alert:{provider}:{alert_id}:{symbol}",
                }
            )
        )
    return out
