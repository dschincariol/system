"""
Validates the weather region map.

Exit code 0 OK, 2 invalid.
"""

import json
import sys
from typing import Any, List

from engine.data.weather_mapping import load_weather_region_map, resolve_weather_region_map_path


def _fail(message: str) -> None:
    print(f"[weather_region_map_validate] ERROR: {message}")
    raise SystemExit(2)


def _ensure_numeric_mapping(raw: Any, *, section: str, key: str) -> None:
    if raw is None:
        return
    if not isinstance(raw, dict):
        _fail(f"{section}.{key} must be an object")
    for field, value in raw.items():
        try:
            float(value)
        except Exception:
            _fail(f"{section}.{key}.{field} must be numeric")


def _validate_symbol_list(entries: Any, *, section: str) -> None:
    if not isinstance(entries, list):
        _fail(f"{section} must be a list")
    for entry in entries:
        if isinstance(entry, str):
            if not entry.strip():
                _fail(f"{section} contains an empty symbol")
            continue
        if not isinstance(entry, dict):
            _fail(f"{section} entries must be strings or objects")
        symbol = str(entry.get("symbol") or entry.get("ticker") or "").strip()
        if not symbol:
            _fail(f"{section} entries must include symbol")
        if "weight" in entry:
            try:
                float(entry.get("weight"))
            except Exception:
                _fail(f"{section} weight must be numeric")


def main() -> int:
    path = resolve_weather_region_map_path()
    cfg = load_weather_region_map(force=True)
    if not isinstance(cfg, dict) or not cfg:
        _fail(f"cannot load {path}")

    regions = cfg.get("regions")
    if not isinstance(regions, dict) or not regions:
        _fail("missing or empty `regions` dict")

    symbol_groups = cfg.get("symbol_groups") or {}
    if symbol_groups and not isinstance(symbol_groups, dict):
        _fail("`symbol_groups` must be a dict if present")
    for group_name, members in (symbol_groups or {}).items():
        if not str(group_name).strip():
            _fail("symbol_groups contains an empty key")
        _validate_symbol_list(members, section=f"symbol_groups.{group_name}")

    for region_id, region_cfg in regions.items():
        if not str(region_id).strip():
            _fail("empty region id")
        if not isinstance(region_cfg, dict):
            _fail(f"region {region_id} must be an object")
        if "lat" not in region_cfg or "lon" not in region_cfg:
            _fail(f"region {region_id} must include lat/lon")
        try:
            float(region_cfg["lat"])
            float(region_cfg["lon"])
        except Exception:
            _fail(f"region {region_id} lat/lon must be numeric")

        _ensure_numeric_mapping(region_cfg.get("normals") or region_cfg.get("normal"), section=f"regions.{region_id}", key="normals")
        _ensure_numeric_mapping(region_cfg.get("thresholds") or region_cfg.get("extreme_thresholds"), section=f"regions.{region_id}", key="thresholds")

        impacted = region_cfg.get("impacted_symbols") or region_cfg.get("symbols")
        if impacted is not None:
            _validate_symbol_list(impacted, section=f"regions.{region_id}.impacted_symbols")

        groups = region_cfg.get("impact_groups") or region_cfg.get("groups") or []
        if groups and not isinstance(groups, list):
            _fail(f"regions.{region_id}.impact_groups must be a list")
        for entry in groups:
            if isinstance(entry, str):
                if str(entry).strip().lower() not in {str(key).strip().lower() for key in symbol_groups.keys()}:
                    _fail(f"region {region_id} refers to unknown symbol group {entry}")
                continue
            if not isinstance(entry, dict):
                _fail(f"region {region_id} impact_groups entries must be strings or objects")
            group_name = str(entry.get("group") or entry.get("channel") or entry.get("category") or "").strip().lower()
            if not group_name:
                _fail(f"region {region_id} impact_groups entries must include group")
            if group_name not in {str(key).strip().lower() for key in symbol_groups.keys()}:
                _fail(f"region {region_id} refers to unknown symbol group {group_name}")
            if "weight" in entry:
                try:
                    float(entry.get("weight"))
                except Exception:
                    _fail(f"region {region_id} impact_groups weight must be numeric")

    symbols = cfg.get("symbols") or {}
    if symbols and not isinstance(symbols, dict):
        _fail("`symbols` must be a dict if present")
    for symbol, mapping in (symbols or {}).items():
        if not str(symbol).strip():
            _fail("empty symbol key in symbols map")
        refs: List[Any]
        if isinstance(mapping, str):
            refs = [mapping]
        elif isinstance(mapping, list):
            refs = list(mapping)
        else:
            _fail(f"symbol {symbol} mapping must be a string or list")
        for ref in refs:
            if isinstance(ref, str):
                region_id = str(ref).strip()
                if region_id not in regions:
                    _fail(f"symbol {symbol} refers to unknown region {region_id}")
                continue
            if not isinstance(ref, dict):
                _fail(f"symbol {symbol} mapping entries must be strings or objects")
            region_id = str(ref.get("region_id") or "").strip()
            if region_id not in regions:
                _fail(f"symbol {symbol} refers to unknown region {region_id}")
            if "weight" in ref:
                try:
                    float(ref.get("weight"))
                except Exception:
                    _fail(f"symbol {symbol} weight must be numeric")

    print(f"[weather_region_map_validate] OK {json.dumps({'path': path}, separators=(',', ':'))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
