"""
Smoke test for symbol-aware weather mapping and event fanout.
"""

from __future__ import annotations

import pathlib
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.data.weather_event_factory import build_symbol_alert_events, build_symbol_forecast_events
from engine.data.weather_mapping import load_weather_region_map, symbol_regions


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> int:
    cfg = load_weather_region_map(force=True)

    xle_regions = symbol_regions("XLE", cfg)
    dba_regions = symbol_regions("DBA", cfg)
    iyt_regions = symbol_regions("IYT", cfg)

    _assert(any(region_id == "us_gulf" for region_id, _weight, _channels in xle_regions), "XLE must map to us_gulf")
    _assert(any(region_id == "corn_belt" for region_id, _weight, _channels in dba_regions), "DBA must map to corn_belt")
    _assert(any("transport" in channels for _region_id, _weight, channels in iyt_regions), "IYT must have transport exposure")

    now_ms = int(time.time() * 1000)
    forecast_events = build_symbol_forecast_events(
        region_id="us_gulf",
        provider="open_meteo",
        run_ts=now_ms,
        day_ts=now_ms,
        temp_mean_c=29.0,
        hdd=0.0,
        cdd=10.0,
        wind_mean_mps=16.0,
        precip_sum_mm=18.0,
        source_uri="https://example.test/forecast",
        cfg=cfg,
    )
    forecast_symbols = {str(event.get("symbol") or "") for event in forecast_events if str(event.get("symbol") or "")}
    _assert("XLE" in forecast_symbols, "forecast fanout must include XLE")
    _assert("IYT" in forecast_symbols, "forecast fanout must include IYT")

    alert_events = build_symbol_alert_events(
        alert_id="demo-alert",
        provider="nws",
        issued_ms=now_ms,
        expires_ms=now_ms + 3600000,
        region_ids=["corn_belt"],
        event_name="Flood Warning",
        severity="Severe",
        urgency="Immediate",
        certainty="Observed",
        headline="Demo alert",
        description="Heavy rain over crop and rail corridors",
        url="https://example.test/alert",
        cfg=cfg,
    )
    alert_symbols = {str(event.get("symbol") or "") for event in alert_events if str(event.get("symbol") or "")}
    _assert("DBA" in alert_symbols, "alert fanout must include DBA")
    _assert("IYT" in alert_symbols or "FDX" in alert_symbols, "alert fanout must include transport exposure")
    _assert(all("alert_severity" in dict(event.get("derived_features") or {}) for event in alert_events), "alert events must carry alert_severity")

    print("weather_symbol_smoke: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
