# CREATE NEW FILE: dev_core/weather_api.py
"""
Minimal API helpers for weather endpoints.

This file is framework-agnostic:
- call functions, return dicts
- your dashboard server can expose them at routes:
    GET /api/weather/snapshot?symbol=SPY
    GET /api/weather/alerts
    GET /api/weather/effect
"""

from typing import Dict, Any, Optional

from engine.runtime.dashboard_weather_widgets import (
    get_weather_snapshot_for_symbol,
    get_weather_alert_summary,
    get_weather_effect_summary,
)


def api_weather_snapshot(symbol: str, ts_ms: Optional[int] = None) -> Dict[str, Any]:
    # Keep API modules decoupled from widget internals: callers import these
    # helpers and get stable dict payloads without knowing dashboard details.
    return get_weather_snapshot_for_symbol(symbol=str(symbol), ts_ms=ts_ms)


def api_weather_alerts(ts_ms: Optional[int] = None) -> Dict[str, Any]:
    return get_weather_alert_summary(ts_ms=ts_ms)


def api_weather_effect(ts_ms: Optional[int] = None) -> Dict[str, Any]:
    return get_weather_effect_summary(ts_ms=ts_ms)
