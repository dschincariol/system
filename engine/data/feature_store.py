"""
Hybrid feature store backed by in-memory snapshots and TimescaleDB persistence.
"""

from __future__ import annotations

import copy
import json
import math
import os
import time
from typing import Any, Dict, Iterable, Mapping, Sequence

from engine.data import price_cache as _default_price_cache
from engine.data.price_cache import PriceSnapshot
from engine.runtime.db_guard import resolve_db_path
from engine.runtime.data_quality import (
    FEATURE_VALIDATION_MAX_AGE_S,
    record_feature_validation,
)
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.live_cache import get_live_cache
from engine.runtime.logging import get_logger
from engine.runtime import price_cache as _runtime_price_cache
from engine.runtime.storage import connect, get_timescale_client, register_after_commit, run_write_txn

FEATURE_SCHEMA_VERSION = max(1, int(os.environ.get("FEATURE_STORE_SCHEMA_VERSION", "1")))
FEATURE_SET_TAG = f"price_feature_store_v{int(FEATURE_SCHEMA_VERSION)}"
FEATURE_CACHE_TTL_S = max(0.05, float(os.environ.get("FEATURE_STORE_TTL_S", "5.0")))
FEATURE_STORE_SQLITE_WRITE_ENABLED = str(
    os.environ.get("FEATURE_STORE_SQLITE_WRITE_ENABLED", "1")
).strip().lower() in {"1", "true", "yes", "on"}
FEATURE_RETURN_WINDOWS_MS = {
    "5m": 5 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "1d": 24 * 60 * 60 * 1000,
}
FEATURE_VOLUME_SHORT_WINDOW = max(2, int(os.environ.get("FEATURE_STORE_VOLUME_SHORT_WINDOW", "5")))
FEATURE_VOLUME_LONG_WINDOW = max(3, int(os.environ.get("FEATURE_STORE_VOLUME_LONG_WINDOW", "20")))
FEATURE_TREND_WINDOW = max(5, int(os.environ.get("FEATURE_STORE_TREND_WINDOW", "20")))
LOG = get_logger("engine.data.feature_store")
_WARNED_NONFATAL_KEYS: set[str] = set()

FEATURE_NAMES = (
    "rolling_return_5m",
    "rolling_return_1h",
    "rolling_return_1d",
    "pct_return_5m",
    "pct_return_1h",
    "pct_return_1d",
    "volatility_20",
    "volatility_60",
    "atr_pct_14",
    "momentum_5m",
    "momentum_1h",
    "momentum_1d",
    "trend_strength_20",
    "volume_last",
    "volume_sma_5",
    "volume_sma_20",
    "volume_rel_5",
    "volume_rel_20",
    "volume_zscore_20",
    "volume_momentum_5",
    "volume_nonzero_share_20",
    "dollar_volume_last",
    "dollar_volume_sma_20",
    "dollar_volume_rel_20",
)
FEATURE_REQUIRED_NAMES = tuple(
    str(name)
    for name in FEATURE_NAMES
)


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.data.feature_store",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(str(once_key))


def _normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if out != out or out in (float("inf"), float("-inf")):
        return float(default)
    return float(out)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if value in (None, "", b"", bytearray()):
        return {}
    raw = value.decode("utf-8", errors="replace") if isinstance(value, (bytes, bytearray)) else str(value)
    obj = json.loads(raw)
    return dict(obj) if isinstance(obj, Mapping) else {}


def _clip(value: float, lo: float, hi: float) -> float:
    return float(max(float(lo), min(float(hi), float(value))))


def _mean(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return float(sum(float(v) for v in values) / float(len(values)))


def _sample_std(values: Sequence[float]) -> float:
    if len(values) < 3:
        return 0.0
    mean = _mean(values)
    var = sum((float(value) - mean) ** 2 for value in values) / float(max(1, len(values) - 1))
    return float(math.sqrt(max(0.0, var)))


def _latest_price_at_or_before(points: Sequence[tuple[int, float]], target_ts_ms: int) -> float | None:
    for point_ts_ms, point_price in reversed(list(points or [])):
        if int(point_ts_ms) <= int(target_ts_ms):
            return float(point_price)
    return None


def _log_return(now_price: float | None, then_price: float | None) -> float:
    if now_price is None or then_price is None or float(now_price) <= 0.0 or float(then_price) <= 0.0:
        return 0.0
    try:
        return float(math.log(float(now_price) / float(then_price)))
    except Exception as exc:
        _warn_nonfatal(
            "FEATURE_STORE_LOG_RETURN_FAILED",
            exc,
            once_key=f"feature_store_log_return:{now_price}:{then_price}",
        )
        return 0.0


def _pct_return(now_price: float | None, then_price: float | None) -> float:
    if now_price is None or then_price is None or float(then_price) <= 0.0:
        return 0.0
    try:
        return float((float(now_price) / float(then_price)) - 1.0)
    except Exception as exc:
        _warn_nonfatal(
            "FEATURE_STORE_PCT_RETURN_FAILED",
            exc,
            once_key=f"feature_store_pct_return:{now_price}:{then_price}",
        )
        return 0.0


def _rolling_rv(points: Sequence[tuple[int, float]], n: int) -> float:
    returns = []
    last_price = None
    for _ts_ms, price in points or []:
        current_price = float(price)
        if last_price is not None and float(last_price) > 0.0 and current_price > 0.0:
            returns.append(float(math.log(current_price / float(last_price))))
        last_price = current_price
    if len(returns) < max(3, int(n)):
        return 0.0
    window = returns[-int(n) :]
    mean = _mean(window)
    var = sum((value - mean) ** 2 for value in window) / float(max(1, len(window) - 1))
    return float(math.sqrt(max(0.0, var)))


def _rolling_std(points: Sequence[tuple[int, float]], n: int) -> float:
    return _rolling_rv(points, n)


def _atr_pct(points: Sequence[tuple[int, float]], n: int) -> float:
    moves = []
    last_price = None
    for _ts_ms, price in points or []:
        current_price = float(price)
        if last_price is not None and float(last_price) > 0.0 and current_price > 0.0:
            moves.append(abs(float(math.log(current_price / float(last_price)))))
        last_price = current_price
    if len(moves) < max(3, int(n)):
        return 0.0
    window = moves[-int(n) :]
    return float(_mean(window))


def _trend_strength(points: Sequence[tuple[int, float]], n: int) -> float:
    point_series = list(points or [])
    if len(point_series) < max(5, int(n) + 1):
        return 0.0
    returns = []
    for idx in range(max(1, len(point_series) - int(n)), len(point_series)):
        returns.append(_log_return(point_series[idx][1], point_series[idx - 1][1]))
    if len(returns) < 3:
        return 0.0
    mean = _mean(returns)
    std = _sample_std(returns)
    if std <= 1e-12:
        return 0.0
    return float(_clip(abs(mean) / std, 0.0, 10.0))


def _resolve_price_snapshot(symbol: str, price_cache: Any) -> PriceSnapshot:
    if isinstance(price_cache, PriceSnapshot):
        return price_cache

    getter = getattr(price_cache, "get_symbol_snapshot", None)
    if callable(getter):
        return getter(symbol)

    getter = getattr(price_cache, "snapshot", None)
    if callable(getter):
        return getter(symbol)

    raise TypeError("price_cache must be a PriceSnapshot or expose get_symbol_snapshot(symbol)")


def _empty_feature_map() -> Dict[str, float]:
    return {str(name): 0.0 for name in FEATURE_NAMES}


def _zero_feature_snapshot(symbol: str) -> Dict[str, Any]:
    symbol_key = _normalize_symbol(symbol)
    return {
        "symbol": str(symbol_key),
        "ts_ms": 0,
        "schema_version": int(FEATURE_SCHEMA_VERSION),
        "feature_set_tag": str(FEATURE_SET_TAG),
        "feature_names": list(FEATURE_NAMES),
        "vector": [0.0 for _ in FEATURE_NAMES],
        "point_count": 0,
        "source_timestamps": {},
        "features": _empty_feature_map(),
    }


def validate_feature_snapshot(
    snapshot: Mapping[str, Any],
    *,
    now_ms: int | None = None,
    stale_after_s: float | None = None,
    required_feature_names: Sequence[str] | None = None,
) -> Dict[str, Any]:
    """Validate one canonical feature snapshot for live runtime use."""
    current_ts_ms = int(now_ms or time.time() * 1000)
    stale_after_ms = int(float(stale_after_s or FEATURE_VALIDATION_MAX_AGE_S) * 1000.0)
    feature_map = dict(snapshot.get("features") or {})
    vector = list(snapshot.get("vector") or [])
    feature_names = list(snapshot.get("feature_names") or FEATURE_NAMES)
    symbol = _normalize_symbol(snapshot.get("symbol"))
    feature_ts_ms = _safe_int(snapshot.get("ts_ms"), 0)
    point_count = _safe_int(snapshot.get("point_count"), 0)
    schema_version = _safe_int(snapshot.get("schema_version"), 0)
    feature_set_tag = str(snapshot.get("feature_set_tag") or "").strip()
    required_names = [
        str(name)
        for name in list(required_feature_names or FEATURE_REQUIRED_NAMES)
        if str(name or "").strip()
    ]

    missing_required = [
        str(name)
        for name in required_names
        if str(name) not in feature_map
    ]
    invalid_feature_ids = []
    for name, value in feature_map.items():
        try:
            numeric = float(value)
        except Exception:
            invalid_feature_ids.append(str(name))
            continue
        if not math.isfinite(numeric):
            invalid_feature_ids.append(str(name))

    vector_invalid = any(not math.isfinite(_safe_float(value, float("nan"))) for value in vector)
    stale = bool(feature_ts_ms <= 0 or (current_ts_ms - int(feature_ts_ms)) > stale_after_ms)
    reason_codes: list[str] = []
    if not symbol:
        reason_codes.append("feature_symbol_missing")
    if feature_ts_ms <= 0:
        reason_codes.append("feature_timestamp_missing")
    if point_count <= 0:
        reason_codes.append("feature_point_count_empty")
    if not feature_set_tag:
        reason_codes.append("feature_set_tag_missing")
    if missing_required:
        reason_codes.append("feature_required_fields_missing")
    if invalid_feature_ids:
        reason_codes.append("feature_values_invalid")
    if vector and len(vector) != len(FEATURE_NAMES):
        reason_codes.append("feature_vector_length_invalid")
    if vector_invalid:
        reason_codes.append("feature_vector_values_invalid")
    if stale:
        reason_codes.append("feature_snapshot_stale")

    ok = not bool(reason_codes)
    detail = "ok"
    if reason_codes:
        detail = str(reason_codes[0])

    return {
        "ok": bool(ok),
        "status": ("ok" if ok else ("stale" if stale else "invalid")),
        "detail": str(detail),
        "symbol": str(symbol),
        "validated_ts_ms": int(current_ts_ms),
        "feature_ts_ms": int(feature_ts_ms),
        "feature_set_tag": str(feature_set_tag),
        "schema_version": int(schema_version),
        "point_count": int(point_count),
        "feature_count": int(len(feature_map)),
        "vector_size": int(len(vector)),
        "missing_required_features": list(missing_required),
        "invalid_feature_ids": list(invalid_feature_ids),
        "stale": bool(stale),
        "age_ms": (max(0, int(current_ts_ms) - int(feature_ts_ms)) if feature_ts_ms > 0 else 0),
        "reason_codes": list(reason_codes),
        "feature_names": list(feature_names),
    }


def _coerce_feature_snapshot(symbol: str, features: Mapping[str, Any]) -> Dict[str, Any]:
    symbol_key = _normalize_symbol(symbol or features.get("symbol"))
    if not symbol_key:
        raise ValueError("missing_symbol")

    if isinstance(features.get("features"), Mapping):
        raw_feature_map = dict(features.get("features") or {})
        ts_ms = _safe_int(features.get("ts_ms"), 0)
        point_count = _safe_int(features.get("point_count"), 0)
        source_timestamps = dict(features.get("source_timestamps") or {})
    else:
        raw_feature_map = dict(features or {})
        ts_ms = _safe_int(raw_feature_map.pop("ts_ms", 0), 0)
        point_count = _safe_int(raw_feature_map.pop("point_count", 0), 0)
        source_timestamps = dict(raw_feature_map.pop("source_timestamps", {}) or {})

    feature_map = _empty_feature_map()
    for name in FEATURE_NAMES:
        feature_map[str(name)] = float(_safe_float(raw_feature_map.get(name), 0.0))

    feature_names = list(features.get("feature_names") or FEATURE_NAMES)
    vector = [float(feature_map.get(name, 0.0)) for name in FEATURE_NAMES]

    return {
        "symbol": str(symbol_key),
        "ts_ms": int(ts_ms),
        "schema_version": int(_safe_int(features.get("schema_version"), FEATURE_SCHEMA_VERSION)),
        "feature_set_tag": str(features.get("feature_set_tag") or FEATURE_SET_TAG),
        "feature_names": feature_names,
        "vector": vector,
        "point_count": int(point_count),
        "source_timestamps": dict(source_timestamps),
        "features": feature_map,
    }

_CACHE_DB_PATH = ""


def _sqlite_feature_snapshot_reads_enabled() -> bool:
    return bool(FEATURE_STORE_SQLITE_WRITE_ENABLED)


def _feature_store_write_mode(*, timescale_enabled: bool) -> str:
    if FEATURE_STORE_SQLITE_WRITE_ENABLED and bool(timescale_enabled):
        return "sqlite+timescale"
    if FEATURE_STORE_SQLITE_WRITE_ENABLED:
        return "sqlite"
    if bool(timescale_enabled):
        return "timescale"
    return "memory"


def _feature_store_read_mode() -> str:
    if _sqlite_feature_snapshot_reads_enabled():
        return "cache+sqlite_recovery+recompute"
    return "cache+recompute"


def _current_db_path() -> str:
    try:
        return str(resolve_db_path())
    except Exception:
        return ""


def _reset_cache_if_db_changed() -> None:
    global _CACHE_DB_PATH
    current_db_path = _current_db_path()
    if str(_CACHE_DB_PATH or "") == str(current_db_path or ""):
        return
    _CACHE_DB_PATH = str(current_db_path or "")
    try:
        get_live_cache().clear_feature()
    except Exception as exc:
        _warn_nonfatal(
            "FEATURE_STORE_CACHE_RESET_FAILED",
            exc,
            once_key="feature_store_cache_reset_failed",
        )


def clear_feature_cache(symbol: str | None = None) -> None:
    """Clear one symbol or the entire in-process feature snapshot cache."""
    _reset_cache_if_db_changed()
    try:
        get_live_cache().clear_feature(symbol)
    except Exception as exc:
        _warn_nonfatal(
            "FEATURE_STORE_CACHE_CLEAR_FAILED",
            exc,
            once_key="feature_store_cache_clear_failed",
            symbol=(str(symbol) if symbol is not None else None),
        )


def _get_cached_snapshot(symbol: str) -> Dict[str, Any] | None:
    _reset_cache_if_db_changed()
    symbol_key = _normalize_symbol(symbol)
    if not symbol_key:
        return None
    try:
        payload = get_live_cache().get_feature_snapshot(symbol_key)
    except Exception as exc:
        _warn_nonfatal(
            "FEATURE_STORE_CACHE_GET_FAILED",
            exc,
            once_key=f"feature_store_cache_get_failed:{symbol_key}",
            symbol=str(symbol_key),
        )
        return None
    if not isinstance(payload, Mapping):
        return None
    return _coerce_feature_snapshot(str(symbol_key), dict(payload))


def _set_cached_snapshot(snapshot: Mapping[str, Any]) -> Dict[str, Any]:
    _reset_cache_if_db_changed()
    snapshot_copy = copy.deepcopy(dict(snapshot or {}))
    symbol_key = _normalize_symbol(snapshot_copy.get("symbol"))
    if not symbol_key:
        return snapshot_copy

    snapshot_ts_ms = _safe_int(snapshot_copy.get("ts_ms"), 0)
    try:
        get_live_cache().set_feature_snapshot(
            str(symbol_key),
            snapshot_copy,
            ttl_s=float(FEATURE_CACHE_TTL_S),
            snapshot_ts_ms=int(snapshot_ts_ms),
        )
    except Exception as exc:
        _warn_nonfatal(
            "FEATURE_STORE_CACHE_SET_FAILED",
            exc,
            once_key=f"feature_store_cache_set_failed:{symbol_key}",
            symbol=str(symbol_key),
        )
        return snapshot_copy
    return _get_cached_snapshot(str(symbol_key)) or snapshot_copy


def _load_latest_snapshot_from_db(symbol: str) -> Dict[str, Any] | None:
    _reset_cache_if_db_changed()
    if not _sqlite_feature_snapshot_reads_enabled():
        return None
    con = connect(readonly=True)
    try:
        row = con.execute(
            """
            SELECT features_json
            FROM market_features
            WHERE symbol = ?
            ORDER BY ts_ms DESC, v DESC
            LIMIT 1
            """,
            (str(symbol),),
        ).fetchone()
    except Exception as exc:
        row = None
        _warn_nonfatal(
            "FEATURE_STORE_DB_RECOVERY_FAILED",
            exc,
            once_key=f"feature_store_db_recovery_failed:{symbol}",
            symbol=str(symbol),
        )
    finally:
        try:
            con.close()
        except Exception as exc:
            _warn_nonfatal(
                "FEATURE_STORE_DB_CLOSE_FAILED",
                exc,
                once_key="feature_store_db_close_failed",
            )

    if not row:
        return None
    try:
        snapshot = _json_dict(row[0])
    except Exception as exc:
        _warn_nonfatal(
            "FEATURE_STORE_DB_PARSE_FAILED",
            exc,
            once_key=f"feature_store_db_parse_failed:{symbol}",
            symbol=str(symbol),
        )
        return None
    if not isinstance(snapshot, dict):
        return None
    return _coerce_feature_snapshot(str(symbol), snapshot)


def _load_feature_snapshot_asof(symbol: str, ts_ms: int, *, con: Any | None = None) -> Dict[str, Any] | None:
    if not _sqlite_feature_snapshot_reads_enabled():
        return None
    owns = con is None
    if con is None:
        con = connect(readonly=True)
    try:
        row = con.execute(
            """
            SELECT features_json
            FROM market_features
            WHERE symbol = ?
              AND ts_ms <= ?
            ORDER BY ts_ms DESC, v DESC
            LIMIT 1
            """,
            (str(symbol), int(ts_ms)),
        ).fetchone()
    except Exception as exc:
        row = None
        _warn_nonfatal(
            "FEATURE_STORE_ASOF_DB_LOOKUP_FAILED",
            exc,
            once_key=f"feature_store_asof_db_lookup_failed:{symbol}",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
    finally:
        if owns:
            try:
                con.close()
            except Exception as exc:
                _warn_nonfatal(
                    "FEATURE_STORE_ASOF_DB_CLOSE_FAILED",
                    exc,
                    once_key="feature_store_asof_db_close_failed",
                )
    if not row:
        return None
    try:
        payload = _json_dict(row[0])
    except Exception as exc:
        _warn_nonfatal(
            "FEATURE_STORE_ASOF_DB_PARSE_FAILED",
            exc,
            once_key=f"feature_store_asof_db_parse_failed:{symbol}",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        return None
    if not isinstance(payload, dict):
        return None
    return _coerce_feature_snapshot(str(symbol), payload)


def compute_features(symbol: str, price_cache: Any) -> Dict[str, Any]:
    """Compute the canonical feature snapshot for one symbol from price history."""
    price_snapshot = _resolve_price_snapshot(symbol, price_cache)
    symbol_key = _normalize_symbol(price_snapshot.symbol or symbol)
    feature_map = _empty_feature_map()

    points = [
        (int(point.ts_ms), float(point.price))
        for point in list(price_snapshot.points or ())
        if int(point.ts_ms) > 0 and float(point.price) > 0.0
    ]
    volumes = [
        float(max(0.0, _safe_float(point.volume, 0.0)))
        for point in list(price_snapshot.points or ())
        if int(point.ts_ms) > 0 and float(point.price) > 0.0
    ]
    dollar_volumes = [
        float(max(0.0, _safe_float(point.volume, 0.0))) * float(point.price)
        for point in list(price_snapshot.points or ())
        if int(point.ts_ms) > 0 and float(point.price) > 0.0
    ]

    latest_ts_ms = int(points[-1][0]) if points else 0
    latest_price = float(points[-1][1]) if points else 0.0

    if points:
        for label, window_ms in FEATURE_RETURN_WINDOWS_MS.items():
            previous_price = _latest_price_at_or_before(points, int(latest_ts_ms) - int(window_ms))
            feature_map[f"rolling_return_{label}"] = float(_clip(_log_return(latest_price, previous_price), -2.0, 2.0))
            feature_map[f"pct_return_{label}"] = float(_clip(_pct_return(latest_price, previous_price), -1.0, 1.0))

        rv_20 = _rolling_rv(points, 20)
        rv_60 = _rolling_std(points, 60)
        baseline_vol = max(float(rv_60), 1e-6)
        feature_map["volatility_20"] = float(_clip(rv_20, 0.0, 1.0))
        feature_map["volatility_60"] = float(_clip(rv_60, 0.0, 1.0))
        feature_map["atr_pct_14"] = float(_clip(_atr_pct(points, 14), 0.0, 1.0))
        feature_map["momentum_5m"] = float(_clip(feature_map["rolling_return_5m"] / baseline_vol, -10.0, 10.0))
        feature_map["momentum_1h"] = float(_clip(feature_map["rolling_return_1h"] / baseline_vol, -10.0, 10.0))
        feature_map["momentum_1d"] = float(_clip(feature_map["rolling_return_1d"] / baseline_vol, -10.0, 10.0))
        feature_map["trend_strength_20"] = float(_clip(_trend_strength(points, FEATURE_TREND_WINDOW), 0.0, 10.0))

    if volumes:
        short_window = volumes[-int(FEATURE_VOLUME_SHORT_WINDOW) :]
        long_window = volumes[-int(FEATURE_VOLUME_LONG_WINDOW) :]
        last_volume = float(volumes[-1])
        sma_short = float(_mean(short_window))
        sma_long = float(_mean(long_window))
        prev_short = (
            float(_mean(volumes[-(int(FEATURE_VOLUME_SHORT_WINDOW) * 2) : -int(FEATURE_VOLUME_SHORT_WINDOW)]))
            if len(volumes) >= int(FEATURE_VOLUME_SHORT_WINDOW) * 2
            else 0.0
        )
        std_long = float(_sample_std(long_window))
        feature_map["volume_last"] = float(last_volume)
        feature_map["volume_sma_5"] = float(sma_short)
        feature_map["volume_sma_20"] = float(sma_long)
        feature_map["volume_rel_5"] = float(_clip(last_volume / sma_short, 0.0, 100.0)) if sma_short > 0.0 else 0.0
        feature_map["volume_rel_20"] = float(_clip(last_volume / sma_long, 0.0, 100.0)) if sma_long > 0.0 else 0.0
        feature_map["volume_zscore_20"] = (
            float(_clip((last_volume - sma_long) / std_long, -10.0, 10.0)) if std_long > 1e-12 else 0.0
        )
        feature_map["volume_momentum_5"] = (
            float(_clip(math.log((1.0 + sma_short) / (1.0 + prev_short)), -5.0, 5.0))
            if prev_short > 0.0
            else 0.0
        )
        feature_map["volume_nonzero_share_20"] = float(
            sum(1 for value in long_window if float(value) > 0.0) / float(max(1, len(long_window)))
        )

    if dollar_volumes:
        last_dollar_volume = float(dollar_volumes[-1])
        sma_dollar_long = float(_mean(dollar_volumes[-int(FEATURE_VOLUME_LONG_WINDOW) :]))
        feature_map["dollar_volume_last"] = float(last_dollar_volume)
        feature_map["dollar_volume_sma_20"] = float(sma_dollar_long)
        feature_map["dollar_volume_rel_20"] = (
            float(_clip(last_dollar_volume / sma_dollar_long, 0.0, 100.0))
            if sma_dollar_long > 0.0
            else 0.0
        )

    return {
        "symbol": str(symbol_key),
        "ts_ms": int(latest_ts_ms),
        "schema_version": int(FEATURE_SCHEMA_VERSION),
        "feature_set_tag": str(FEATURE_SET_TAG),
        "feature_names": list(FEATURE_NAMES),
        "vector": [
            float(_safe_float(feature_map.get(name), 0.0))
            for name in FEATURE_NAMES
        ],
        "point_count": int(len(points)),
        "source_timestamps": {
            "price_history_first_ts_ms": int(points[0][0]) if points else None,
            "price_history_last_ts_ms": int(latest_ts_ms) if latest_ts_ms > 0 else None,
        },
        "features": {
            str(name): float(_safe_float(feature_map.get(name), 0.0))
            for name in FEATURE_NAMES
        },
    }


def _prepare_feature_snapshots(snapshots: Sequence[Mapping[str, Any]]) -> list[Dict[str, Any]]:
    normalized = []
    for snapshot in snapshots or []:
        try:
            normalized.append(_coerce_feature_snapshot(str((snapshot or {}).get("symbol") or ""), dict(snapshot or {})))
        except Exception as exc:
            _warn_nonfatal(
                "FEATURE_STORE_SNAPSHOT_NORMALIZE_FAILED",
                exc,
                once_key=f"feature_store_snapshot_normalize_failed:{(snapshot or {}).get('symbol')}",
            )
            continue

    return [snapshot for snapshot in normalized if _safe_int(snapshot.get("ts_ms"), 0) > 0]


def _register_feature_snapshot_side_effects(
    con: Any | None,
    persistable: Sequence[Mapping[str, Any]],
) -> None:
    if not persistable:
        return

    def _warm_feature_cache() -> None:
        for snapshot in persistable:
            _set_cached_snapshot(snapshot)

    # Keep runtime reads hot before any optional external enqueue work.
    register_after_commit(con, _warm_feature_cache)

    client = get_timescale_client()
    if client is not None and bool(getattr(client, "enabled", False)):
        timescale_rows = [
            {
                "symbol": str(snapshot["symbol"]),
                "timestamp": int(snapshot["ts_ms"]),
                "feature_vector": dict(snapshot),
            }
            for snapshot in persistable
        ]

        def _enqueue_timescale() -> None:
            try:
                client.enqueue_feature_data(tuple(timescale_rows))
            except Exception as exc:
                _warn_nonfatal(
                    "FEATURE_STORE_TIMESCALE_ENQUEUE_FAILED",
                    exc,
                    once_key="feature_store_timescale_enqueue_failed",
                    rows=int(len(timescale_rows)),
                )

        register_after_commit(con, _enqueue_timescale)


def get_feature_cache_snapshot() -> Dict[str, Any]:
    """Return a compact diagnostic snapshot of the live feature cache."""
    _reset_cache_if_db_changed()
    backend_snapshot = dict(get_live_cache().get_snapshot() or {})
    cached_symbols = int(backend_snapshot.get("feature_symbols") or 0)
    latest_snapshot_ts_ms = int(
        backend_snapshot.get("last_feature_snapshot_ts_ms")
        or backend_snapshot.get("last_feature_write_ts_ms")
        or 0
    )
    return {
        "ok": bool(cached_symbols > 0),
        "cached_symbols": int(cached_symbols),
        "latest_snapshot_ts_ms": (
            int(latest_snapshot_ts_ms)
            if int(latest_snapshot_ts_ms) > 0
            else None
        ),
        "ttl_s": float(FEATURE_CACHE_TTL_S),
        "backend": str(backend_snapshot.get("resolved_backend") or backend_snapshot.get("backend") or "memory"),
        "backend_requested": str(backend_snapshot.get("requested_backend") or "memory"),
        "backend_degraded": bool(backend_snapshot.get("degraded")),
        "backend_fallback_reason": backend_snapshot.get("fallback_reason"),
        "sqlite_db_path": (_current_db_path() or None),
        "ts_ms": int(time.time() * 1000),
    }


def get_feature_store_snapshot(*, timescale_snapshot: Mapping[str, Any] | None = None) -> Dict[str, Any]:
    """Return an operator-facing snapshot of the market feature-store mode."""
    resolved_timescale_snapshot: Dict[str, Any] = dict(timescale_snapshot or {})
    if not resolved_timescale_snapshot:
        try:
            client = get_timescale_client()
            if client is not None:
                resolved_timescale_snapshot = dict(client.get_snapshot())
        except Exception as exc:
            _warn_nonfatal(
                "FEATURE_STORE_TIMESCALE_SNAPSHOT_FAILED",
                exc,
                once_key="feature_store_timescale_snapshot_failed",
            )
            resolved_timescale_snapshot = {}

    timescale_enabled = bool(resolved_timescale_snapshot.get("enabled"))
    timescale_degraded = bool(resolved_timescale_snapshot.get("degraded"))
    timescale_feature_stats = dict(
        (
            (
                (resolved_timescale_snapshot.get("metrics") or {})
                .get("table_stats")
                or {}
            ).get("feature_data")
            or {}
        )
    )
    write_mode = _feature_store_write_mode(timescale_enabled=timescale_enabled)
    degraded_reasons = []
    if write_mode == "timescale" and timescale_degraded:
        degraded_reasons.extend(list(resolved_timescale_snapshot.get("degraded_reasons") or []))

    return {
        "ok": not bool(degraded_reasons),
        "degraded": bool(degraded_reasons),
        "degraded_reasons": degraded_reasons,
        "write_mode": str(write_mode),
        "read_mode": str(_feature_store_read_mode()),
        "sqlite_write_enabled": bool(FEATURE_STORE_SQLITE_WRITE_ENABLED),
        "sqlite_read_fallback_enabled": bool(_sqlite_feature_snapshot_reads_enabled()),
        "sqlite_table": "market_features",
        "sqlite_db_path": (_current_db_path() or None),
        "timescale_enabled": bool(timescale_enabled),
        "timescale_started": bool(resolved_timescale_snapshot.get("started")),
        "timescale_queue_depth": int(resolved_timescale_snapshot.get("queue_depth") or 0),
        "timescale_feature_data": timescale_feature_stats,
        "cache": get_feature_cache_snapshot(),
        "ts_ms": int(time.time() * 1000),
    }


def _write_feature_snapshots(con: Any, snapshots: Sequence[Mapping[str, Any]]) -> int:
    persistable = _prepare_feature_snapshots(snapshots)
    if not persistable:
        return 0

    rows = [
        (
            int(snapshot["ts_ms"]),
            str(snapshot["symbol"]),
            int(snapshot.get("schema_version") or FEATURE_SCHEMA_VERSION),
            _json_dumps(snapshot),
        )
        for snapshot in persistable
    ]

    con.executemany(
        """
        INSERT OR REPLACE INTO market_features(ts_ms, symbol, v, features_json)
        VALUES (?, ?, ?, ?)
        """,
        rows,
    )

    _register_feature_snapshot_side_effects(con, persistable)
    return int(len(persistable))


def _persist_feature_snapshots(
    snapshots: Sequence[Mapping[str, Any]],
    *,
    con: Any | None = None,
    operation: str,
    context: Mapping[str, Any] | None = None,
) -> int:
    persistable = _prepare_feature_snapshots(snapshots)
    if not persistable:
        return 0

    if not FEATURE_STORE_SQLITE_WRITE_ENABLED:
        _register_feature_snapshot_side_effects(con, persistable)
        return int(len(persistable))

    if con is not None:
        return _write_feature_snapshots(con, persistable)

    run_write_txn(
        lambda db: _write_feature_snapshots(db, persistable),
        table="market_features",
        operation=str(operation or "persist_feature_snapshots"),
        context=dict(context or {}),
    )
    return int(len(persistable))


def store_features(symbol: str, features: Mapping[str, Any], *, con: Any | None = None) -> Dict[str, Any]:
    """Persist and cache one normalized feature snapshot."""
    _reset_cache_if_db_changed()
    snapshot = _coerce_feature_snapshot(symbol, features)
    if int(snapshot.get("ts_ms") or 0) <= 0:
        return snapshot

    _persist_feature_snapshots(
        [snapshot],
        con=con,
        operation="store_features",
        context={"symbol": str(snapshot.get("symbol") or ""), "ts_ms": int(snapshot.get("ts_ms") or 0)},
    )
    if con is not None and not FEATURE_STORE_SQLITE_WRITE_ENABLED:
        return copy.deepcopy(snapshot)
    return _get_cached_snapshot(str(snapshot.get("symbol") or symbol)) or copy.deepcopy(snapshot)


def get_features_asof(
    symbol: str,
    ts_ms: int,
    *,
    price_cache: Any = _default_price_cache,
    con: Any | None = None,
    persist: bool = False,
) -> Dict[str, Any]:
    """Resolve the latest feature snapshot at or before a target timestamp."""
    symbol_key = _normalize_symbol(symbol)
    target_ts_ms = int(ts_ms or 0)
    if not symbol_key or target_ts_ms <= 0:
        return {
            "symbol": str(symbol_key),
            "ts_ms": 0,
            "schema_version": int(FEATURE_SCHEMA_VERSION),
            "feature_set_tag": str(FEATURE_SET_TAG),
            "feature_names": list(FEATURE_NAMES),
            "vector": [0.0 for _ in FEATURE_NAMES],
            "point_count": 0,
            "source_timestamps": {},
            "features": _empty_feature_map(),
        }

    stored = _load_feature_snapshot_asof(symbol_key, int(target_ts_ms), con=con)
    price_snapshot = None
    latest_price_ts_ms = 0

    loader = getattr(price_cache, "load_symbol_snapshot", None)
    if callable(loader):
        price_snapshot = loader(symbol_key, asof_ts_ms=int(target_ts_ms), con=con)
    else:
        price_snapshot = _resolve_price_snapshot(symbol_key, price_cache)

    latest_price_ts_ms = int((price_snapshot.asof_ts_ms if price_snapshot is not None else 0) or 0)
    if stored is not None and int(stored.get("ts_ms") or 0) >= int(latest_price_ts_ms):
        return stored
    if latest_price_ts_ms <= 0:
        if stored is not None:
            return stored
        return {
            "symbol": str(symbol_key),
            "ts_ms": 0,
            "schema_version": int(FEATURE_SCHEMA_VERSION),
            "feature_set_tag": str(FEATURE_SET_TAG),
            "feature_names": list(FEATURE_NAMES),
            "vector": [0.0 for _ in FEATURE_NAMES],
            "point_count": 0,
            "source_timestamps": {},
            "features": _empty_feature_map(),
        }

    computed = compute_features(symbol_key, price_snapshot)
    if not persist:
        return computed
    if con is not None:
        _persist_feature_snapshots(
            [computed],
            con=con,
            operation="get_features_asof",
            context={"symbol": str(symbol_key), "ts_ms": int(computed.get("ts_ms") or 0)},
        )
        return computed
    return store_features(symbol_key, computed)


def refresh_symbols(symbols: Iterable[str], price_cache: Any = _default_price_cache) -> Dict[str, Dict[str, Any]]:
    """Recompute and persist feature snapshots for a batch of symbols."""
    symbol_keys = sorted({_normalize_symbol(symbol) for symbol in symbols or [] if _normalize_symbol(symbol)})
    if not symbol_keys:
        return {}

    snapshots = [compute_features(symbol, price_cache) for symbol in symbol_keys]
    if not snapshots:
        return {}

    _persist_feature_snapshots(
        snapshots,
        operation="refresh_symbols",
        context={"symbols": int(len(symbol_keys))},
    )
    return {
        str(snapshot.get("symbol") or ""): (_get_cached_snapshot(str(snapshot.get("symbol") or "")) or dict(snapshot))
        for snapshot in snapshots
    }


def get_live_features(
    symbol: str,
    *,
    price_cache: Any = _runtime_price_cache,
    persist: bool = False,
) -> Dict[str, Any]:
    """Return the freshest available feature snapshot for live use."""
    _reset_cache_if_db_changed()
    symbol_key = _normalize_symbol(symbol)
    if not symbol_key:
        return _zero_feature_snapshot("")

    cached = _get_cached_snapshot(symbol_key)
    db_snapshot = _load_latest_snapshot_from_db(symbol_key)
    cached_ts_ms = _safe_int((cached or {}).get("ts_ms"), 0)
    db_ts_ms = _safe_int((db_snapshot or {}).get("ts_ms"), 0)
    price_snapshot = _resolve_price_snapshot(symbol_key, price_cache)
    latest_price_ts_ms = int(price_snapshot.asof_ts_ms or 0)

    if cached is not None and int(cached_ts_ms) >= int(latest_price_ts_ms):
        validation = validate_feature_snapshot(cached)
        record_feature_validation(validation)
        return cached
    if db_snapshot is not None and int(db_ts_ms) >= int(latest_price_ts_ms):
        snapshot = _set_cached_snapshot(db_snapshot)
        validation = validate_feature_snapshot(snapshot)
        record_feature_validation(validation)
        return snapshot

    if latest_price_ts_ms <= 0:
        if cached is not None:
            snapshot = cached
        elif db_snapshot is not None:
            snapshot = _set_cached_snapshot(db_snapshot)
        else:
            snapshot = _zero_feature_snapshot(symbol_key)
        validation = validate_feature_snapshot(snapshot)
        record_feature_validation(validation)
        return snapshot

    computed = compute_features(symbol_key, price_snapshot)
    if persist:
        try:
            snapshot = store_features(symbol_key, computed)
        except Exception as exc:
            _warn_nonfatal(
                "FEATURE_STORE_LIVE_PERSIST_FAILED",
                exc,
                once_key=f"feature_store_live_persist_failed:{symbol_key}",
                symbol=str(symbol_key),
            )
            snapshot = _set_cached_snapshot(computed)
    else:
        snapshot = _set_cached_snapshot(computed)
    validation = validate_feature_snapshot(snapshot)
    record_feature_validation(validation)
    return snapshot


def get_features(symbol: str) -> Dict[str, Any]:
    """Compatibility wrapper that returns the latest live feature snapshot."""
    _reset_cache_if_db_changed()
    symbol_key = _normalize_symbol(symbol)
    if not symbol_key:
        return _zero_feature_snapshot("")

    cached = _get_cached_snapshot(symbol_key)
    if cached is not None:
        return cached

    db_snapshot = _load_latest_snapshot_from_db(symbol_key)
    price_snapshot = _default_price_cache.get_symbol_snapshot(symbol_key)
    latest_price_ts_ms = int(price_snapshot.asof_ts_ms or 0)
    latest_feature_ts_ms = _safe_int((db_snapshot or {}).get("ts_ms"), 0)

    if db_snapshot is not None and int(latest_feature_ts_ms) >= int(latest_price_ts_ms):
        return _set_cached_snapshot(db_snapshot)

    if latest_price_ts_ms > 0:
        computed = compute_features(symbol_key, _default_price_cache)
        return store_features(symbol_key, computed)

    if db_snapshot is not None:
        return _set_cached_snapshot(db_snapshot)

    return _zero_feature_snapshot(symbol_key)
