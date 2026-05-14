from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

_REGION_MAP_CACHE: Dict[str, Any] | None = None
_REGION_MAP_CACHE_PATH: str | None = None
_REGION_MAP_CACHE_MTIME: float | None = None

_DEFAULT_NORMALS = {
    "temp_mean_c": 18.0,
    "wind_mean_mps": 5.0,
    "precip_sum_mm": 3.0,
    "precip_7d_mm": 21.0,
}

_DEFAULT_THRESHOLDS = {
    "anomaly_temp_c": 6.0,
    "anomaly_wind_mps": 4.0,
    "anomaly_precip_mm": 6.0,
    "anomaly_precip_7d_mm": 18.0,
    "extreme_temp_anomaly_c": 10.0,
    "extreme_wind_mps": 12.0,
    "extreme_precip_mm": 12.0,
    "extreme_precip_7d_mm": 35.0,
}
_WARNED_NONFATAL_KEYS: set[str] = set()
LOG = get_logger("data.weather_mapping")


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="data_weather_mapping_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.data.weather_mapping",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _clip01(value: Any) -> float:
    try:
        return float(max(0.0, min(1.0, float(value))))
    except Exception as e:
        _warn_nonfatal("WEATHER_MAPPING_CLIP01_FAILED", e, once_key="clip01", value=repr(value)[:120])
        return 0.0


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception as e:
        _warn_nonfatal("WEATHER_MAPPING_TO_FLOAT_FAILED", e, once_key="to_float", value=repr(value)[:120])
        return float(default)


def _to_symbol(value: Any) -> Optional[str]:
    text = str(value or "").strip().upper()
    return text or None


def _normalize_channels(raw: Any, default_channel: Optional[str] = None) -> List[str]:
    values: List[str] = []
    if isinstance(raw, str):
        values.append(raw)
    elif isinstance(raw, (list, tuple, set)):
        values.extend(str(v or "") for v in raw)
    if default_channel:
        values.append(str(default_channel))
    out: List[str] = []
    seen = set()
    for value in values:
        text = str(value or "").strip().lower()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def resolve_weather_region_map_path(path: Optional[str] = None) -> str:
    candidates: List[Path] = []
    env_path = str(path or os.environ.get("WEATHER_REGION_MAP") or "").strip()
    if env_path:
        candidates.append(Path(env_path))
    candidates.append(Path(__file__).resolve().with_name("weather_region_map.json"))
    candidates.append(Path("engine") / "data" / "weather_region_map.json")
    candidates.append(Path("data") / "weather_region_map.json")

    seen = set()
    fallback = None
    for candidate in candidates:
        candidate = candidate.expanduser()
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        if fallback is None:
            fallback = candidate
        if candidate.exists():
            return str(candidate)
    return str(fallback or (Path("data") / "weather_region_map.json"))


def load_weather_region_map(path: Optional[str] = None, *, force: bool = False) -> Dict[str, Any]:
    global _REGION_MAP_CACHE, _REGION_MAP_CACHE_MTIME, _REGION_MAP_CACHE_PATH
    resolved = resolve_weather_region_map_path(path)
    try:
        mtime = os.path.getmtime(resolved)
    except Exception:
        mtime = None
    if (
        (not force)
        and _REGION_MAP_CACHE is not None
        and _REGION_MAP_CACHE_PATH == resolved
        and _REGION_MAP_CACHE_MTIME == mtime
    ):
        return dict(_REGION_MAP_CACHE)
    try:
        with open(resolved, "r", encoding="utf-8") as f:
            cfg = json.load(f) or {}
    except Exception:
        cfg = {}
    _REGION_MAP_CACHE = dict(cfg)
    _REGION_MAP_CACHE_PATH = resolved
    _REGION_MAP_CACHE_MTIME = mtime
    return dict(cfg)


def region_meta(region_id: str, cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    config = cfg or load_weather_region_map()
    regions = (config or {}).get("regions") or {}
    value = regions.get(str(region_id)) if isinstance(regions, dict) else None
    return dict(value or {}) if isinstance(value, dict) else {}


def region_normal_baseline(region_id: str, cfg: Optional[Dict[str, Any]] = None) -> Dict[str, float]:
    meta = region_meta(region_id, cfg)
    raw = meta.get("normals") or meta.get("normal") or {}
    if not isinstance(raw, dict):
        raw = {}
    out = dict(_DEFAULT_NORMALS)
    for key, value in raw.items():
        out[str(key)] = _to_float(value, out.get(str(key), 0.0))
    if "precip_7d_mm" not in raw:
        out["precip_7d_mm"] = max(7.0, 7.0 * _to_float(out.get("precip_sum_mm"), 3.0))
    return out


def region_thresholds(region_id: str, cfg: Optional[Dict[str, Any]] = None) -> Dict[str, float]:
    meta = region_meta(region_id, cfg)
    raw = meta.get("thresholds") or meta.get("extreme_thresholds") or {}
    if not isinstance(raw, dict):
        raw = {}
    out = dict(_DEFAULT_THRESHOLDS)
    for key, value in raw.items():
        out[str(key)] = _to_float(value, out.get(str(key), 0.0))
    return out


def _normalize_symbol_entry(
    entry: Any,
    *,
    default_weight: float = 1.0,
    default_channel: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    if isinstance(entry, str):
        symbol = _to_symbol(entry)
        if not symbol:
            return None
        return {
            "symbol": symbol,
            "weight": float(default_weight),
            "channels": _normalize_channels(None, default_channel=default_channel),
        }
    if not isinstance(entry, dict):
        return None
    symbol = _to_symbol(entry.get("symbol") or entry.get("ticker"))
    if not symbol:
        return None
    weight = _to_float(entry.get("weight", default_weight), default_weight)
    channels = _normalize_channels(entry.get("channels") or entry.get("channel"), default_channel=default_channel)
    return {
        "symbol": symbol,
        "weight": float(weight),
        "channels": channels,
    }


def _merge_symbol_impacts(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    for entry in entries:
        symbol = _to_symbol(entry.get("symbol"))
        if not symbol:
            continue
        row = merged.setdefault(symbol, {"symbol": symbol, "weight": 0.0, "channels": []})
        row["weight"] += _to_float(entry.get("weight"), 0.0)
        for channel in _normalize_channels(entry.get("channels")):
            if channel not in row["channels"]:
                row["channels"].append(channel)
    total = sum(abs(_to_float(row.get("weight"), 0.0)) for row in merged.values())
    out = []
    for symbol in sorted(merged.keys()):
        row = merged[symbol]
        weight = _to_float(row.get("weight"), 0.0)
        if total > 1e-12:
            weight = weight / total
        out.append(
            {
                "symbol": symbol,
                "weight": float(weight),
                "channels": list(row.get("channels") or []),
            }
        )
    return out


def region_impacted_symbols(region_id: str, cfg: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    config = cfg or load_weather_region_map()
    meta = region_meta(region_id, config)
    symbol_groups = (config or {}).get("symbol_groups") or {}
    entries: List[Dict[str, Any]] = []

    for entry in meta.get("impacted_symbols") or meta.get("symbols") or []:
        normalized = _normalize_symbol_entry(entry)
        if normalized:
            entries.append(normalized)

    for group_entry in meta.get("impact_groups") or meta.get("groups") or []:
        if isinstance(group_entry, str):
            group_name = str(group_entry).strip().lower()
            group_weight = 1.0
        elif isinstance(group_entry, dict):
            group_name = str(
                group_entry.get("group")
                or group_entry.get("channel")
                or group_entry.get("category")
                or ""
            ).strip().lower()
            group_weight = _to_float(group_entry.get("weight", 1.0), 1.0)
        else:
            continue
        if not group_name:
            continue
        members = symbol_groups.get(group_name) or symbol_groups.get(group_name.upper()) or []
        if not isinstance(members, list):
            continue
        for member in members:
            normalized = _normalize_symbol_entry(
                member,
                default_weight=group_weight,
                default_channel=group_name,
            )
            if normalized:
                entries.append(normalized)

    top_level_symbols = (config or {}).get("symbols") or {}
    if isinstance(top_level_symbols, dict):
        for symbol, mapping in top_level_symbols.items():
            refs = [mapping] if isinstance(mapping, str) else list(mapping or [])
            for ref in refs:
                if isinstance(ref, str):
                    ref_region = str(ref).strip()
                    ref_weight = 1.0
                    ref_channels: List[str] = []
                elif isinstance(ref, dict):
                    ref_region = str(ref.get("region_id") or "").strip()
                    ref_weight = _to_float(ref.get("weight", 1.0), 1.0)
                    ref_channels = _normalize_channels(ref.get("channels") or ref.get("channel"))
                else:
                    continue
                if ref_region != str(region_id):
                    continue
                normalized = _normalize_symbol_entry(
                    {"symbol": symbol, "weight": ref_weight, "channels": ref_channels}
                )
                if normalized:
                    entries.append(normalized)

    return _merge_symbol_impacts(entries)


def symbol_regions(symbol: str, cfg: Optional[Dict[str, Any]] = None) -> List[Tuple[str, float, List[str]]]:
    config = cfg or load_weather_region_map()
    regions = (config or {}).get("regions") or {}
    sym_u = _to_symbol(symbol)
    if not sym_u or not isinstance(regions, dict):
        return []
    out: List[Tuple[str, float, List[str]]] = []
    for region_id in regions.keys():
        for mapping in region_impacted_symbols(str(region_id), config):
            if _to_symbol(mapping.get("symbol")) != sym_u:
                continue
            out.append(
                (
                    str(region_id),
                    float(_to_float(mapping.get("weight"), 0.0)),
                    list(mapping.get("channels") or []),
                )
            )
    total = sum(abs(weight) for _, weight, _ in out)
    if total <= 1e-12:
        return out
    return [(region_id, float(weight) / total, channels) for region_id, weight, channels in out]


def score_weather_conditions(
    *,
    region_id: str,
    temp_mean_c: float,
    wind_mean_mps: float,
    precip_sum_mm: float,
    precip_window_days: int = 7,
    cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, float]:
    normals = region_normal_baseline(region_id, cfg)
    thresholds = region_thresholds(region_id, cfg)

    normal_temp = _to_float(normals.get("temp_mean_c"), _DEFAULT_NORMALS["temp_mean_c"])
    normal_wind = _to_float(normals.get("wind_mean_mps"), _DEFAULT_NORMALS["wind_mean_mps"])
    precip_key = "precip_7d_mm" if int(precip_window_days) >= 7 else "precip_sum_mm"
    normal_precip = normals.get(precip_key)
    if normal_precip is None:
        normal_precip = _to_float(normals.get("precip_sum_mm"), _DEFAULT_NORMALS["precip_sum_mm"]) * max(1, int(precip_window_days))
    normal_precip = _to_float(normal_precip, _DEFAULT_NORMALS["precip_7d_mm"])

    temp_gap = abs(_to_float(temp_mean_c) - normal_temp)
    wind_gap = max(0.0, _to_float(wind_mean_mps) - normal_wind)
    precip_gap = abs(_to_float(precip_sum_mm) - normal_precip)

    anomaly = (
        0.45 * _clip01(temp_gap / max(1.0, _to_float(thresholds.get("anomaly_temp_c"), 6.0)))
        + 0.25 * _clip01(wind_gap / max(1.0, _to_float(thresholds.get("anomaly_wind_mps"), 4.0)))
        + 0.30
        * _clip01(
            precip_gap
            / max(
                1.0,
                _to_float(
                    thresholds.get("anomaly_precip_7d_mm" if int(precip_window_days) >= 7 else "anomaly_precip_mm"),
                    18.0 if int(precip_window_days) >= 7 else 6.0,
                ),
            )
        )
    )

    extreme = max(
        _clip01(temp_gap / max(1.0, _to_float(thresholds.get("extreme_temp_anomaly_c"), 10.0))),
        _clip01(_to_float(wind_mean_mps) / max(1.0, _to_float(thresholds.get("extreme_wind_mps"), 12.0))),
        _clip01(
            _to_float(precip_sum_mm)
            / max(
                1.0,
                _to_float(
                    thresholds.get("extreme_precip_7d_mm" if int(precip_window_days) >= 7 else "extreme_precip_mm"),
                    35.0 if int(precip_window_days) >= 7 else 12.0,
                ),
            )
        ),
    )

    return {
        "anomaly_score": float(_clip01(anomaly)),
        "extreme_event_score": float(_clip01(extreme)),
        "temp_anomaly_c": float(temp_gap),
        "wind_anomaly_mps": float(wind_gap),
        "precip_anomaly_mm": float(precip_gap),
        "normal_temp_mean_c": float(normal_temp),
        "normal_wind_mean_mps": float(normal_wind),
        "normal_precip_mm": float(normal_precip),
    }


def alert_severity_score(*, severity: Any, urgency: Any = None, certainty: Any = None) -> float:
    severity_score = {
        "extreme": 1.00,
        "severe": 0.82,
        "moderate": 0.58,
        "minor": 0.32,
        "unknown": 0.18,
    }.get(str(severity or "").strip().lower(), 0.12 if str(severity or "").strip() else 0.0)
    urgency_score = {
        "immediate": 1.00,
        "expected": 0.72,
        "future": 0.48,
        "past": 0.10,
        "unknown": 0.20,
    }.get(str(urgency or "").strip().lower(), 0.20 if str(urgency or "").strip() else 0.0)
    certainty_score = {
        "observed": 1.00,
        "likely": 0.76,
        "possible": 0.48,
        "unlikely": 0.18,
        "unknown": 0.20,
    }.get(str(certainty or "").strip().lower(), 0.20 if str(certainty or "").strip() else 0.0)
    return float(
        _clip01((0.60 * severity_score) + (0.25 * urgency_score) + (0.15 * certainty_score))
    )
