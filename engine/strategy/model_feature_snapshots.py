"""
Canonical per-symbol point-in-time feature snapshots for model training,
inference parity, and backtesting replay.
"""

from __future__ import annotations

import json
import math
import os
import logging
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.config import (
    CONGRESSIONAL_BACKFILL_DAYS as CONFIG_CONGRESSIONAL_BACKFILL_DAYS,
    FORM4_BACKFILL_DAYS as CONFIG_FORM4_BACKFILL_DAYS,
)
from engine.data.finbert_sentiment import (
    FINBERT_FEATURE_IDS as _FINBERT_FEATURE_IDS,
    USE_FINBERT_SENTIMENT,
    resolve_finbert_sentiment_snapshot,
)
from engine.data.weather_features import get_weather_feature_snapshot
from engine.data.weather_mapping import load_weather_region_map, symbol_regions
from engine.data.asset_map import asset_class_for_symbol
from engine.runtime.storage import connect, get_timescale_client, register_after_commit
from engine.strategy.feature_registry import (
    CONGRESSIONAL_FEATURE_IDS,
    EVENT_FEATURE_IDS,
    INSIDER_FEATURE_IDS,
    MACRO_FEATURE_IDS as _BASE_MACRO_FEATURE_IDS,
    OPTIONS_FEATURE_IDS,
    PRICE_FEATURE_IDS,
    UNIFIED_MACRO_FEATURE_IDS as MACRO_FEATURE_IDS,
    UNIFIED_SOCIAL_FEATURE_IDS as SOCIAL_FEATURE_IDS,
    UNIFIED_SYMBOL_FEATURE_IDS,
    WEATHER_FEATURE_IDS,
)
from engine.strategy.social_regime import classify_regime_from_features

FEATURE_SET_TAG = "unified_symbol_v1"
SNAPSHOT_VERSION = 1

DEFAULT_PRICE_LOOKBACK = max(80, int(os.environ.get("MODEL_FEATURE_PRICE_LOOKBACK", "512")))
DEFAULT_EVENT_LOOKBACK_HOURS = max(6, int(os.environ.get("MODEL_FEATURE_EVENT_LOOKBACK_HOURS", "24")))
DEFAULT_OPTIONS_BUCKET_SEC = max(60, int(os.environ.get("OPTIONS_INTRADAY_BUCKET_SEC", "900")))
DEFAULT_SOCIAL_BUCKET_SEC = max(60, int(os.environ.get("SOCIAL_DEFAULT_BUCKET_SEC", "300")))
DEFAULT_SNAPSHOT_BUCKET_SEC = max(60, int(os.environ.get("MODEL_FEATURE_SNAPSHOT_BUCKET_SEC", "300")))
DEFAULT_INSIDER_LOOKBACK_DAYS = max(30, int(CONFIG_FORM4_BACKFILL_DAYS))
DEFAULT_CONGRESSIONAL_LOOKBACK_DAYS = max(30, int(CONFIG_CONGRESSIONAL_BACKFILL_DAYS))
DEFAULT_WEATHER_PROVIDER = str(os.environ.get("WEATHER_PROVIDER", "open_meteo")).strip().lower()
DEFAULT_WEATHER_ALERTS_PROVIDER = str(os.environ.get("WEATHER_ALERTS_PROVIDER", "nws")).strip().lower()
FINBERT_FEATURE_IDS = list(_FINBERT_FEATURE_IDS) if USE_FINBERT_SENTIMENT else []
LOG = get_logger("engine.strategy.model_feature_snapshots")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(event: str, code: str, error: BaseException, *, warn_key: str | None = None, **extra: Any) -> None:
    if warn_key and warn_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=event,
        code=code,
        message=event,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.model_feature_snapshots",
        extra=extra or None,
        persist=False,
    )
    if warn_key:
        _WARNED_NONFATAL_KEYS.add(warn_key)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception as e:
        _warn_nonfatal(
            "model_feature_snapshots_safe_float_failed",
            "MODEL_FEATURE_SNAPSHOTS_SAFE_FLOAT_FAILED",
            e,
            warn_key=f"model_feature_snapshots_safe_float:{value}",
            raw_value=value,
        )
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception as e:
        _warn_nonfatal(
            "model_feature_snapshots_safe_int_failed",
            "MODEL_FEATURE_SNAPSHOTS_SAFE_INT_FAILED",
            e,
            warn_key=f"model_feature_snapshots_safe_int:{value}",
            raw_value=value,
        )
        return int(default)


def _bucket_start(ts_ms: int, bucket_sec: int) -> int:
    size_ms = max(1, int(bucket_sec)) * 1000
    return int(ts_ms // size_ms) * size_ms


def _json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _feature_set_tag(feature_ids: Sequence[str]) -> str:
    if list(feature_ids) == list(UNIFIED_SYMBOL_FEATURE_IDS):
        return FEATURE_SET_TAG
    return f"{FEATURE_SET_TAG}+custom"


def _register_timescale_feature_rows_after_commit(con, snapshots: Sequence[Dict[str, Any]]) -> None:
    client = get_timescale_client()
    if client is None or not bool(getattr(client, "enabled", False)):
        return

    rows = []
    for snap in snapshots or []:
        symbol = str(snap.get("symbol") or "").upper().strip()
        if not symbol:
            continue
        rows.append(
            {
                "symbol": symbol,
                "timestamp": int(snap.get("ts_ms") or 0),
                "feature_vector": {
                    "availability": dict(snap.get("availability") or {}),
                    "feature_ids": list(snap.get("feature_ids") or []),
                    "feature_set_tag": str(snap.get("feature_set_tag") or FEATURE_SET_TAG),
                    "features": dict(snap.get("features") or {}),
                    "snapshot_version": int(snap.get("snapshot_version") or SNAPSHOT_VERSION),
                    "source_timestamps": dict(snap.get("source_timestamps") or {}),
                    "vector": list(snap.get("vector") or []),
                },
            }
        )
    if not rows:
        return

    def _enqueue() -> None:
        try:
            client.enqueue_feature_data(tuple(rows))
        except Exception as e:
            _warn_nonfatal(
                "model_feature_snapshots_timescale_enqueue_failed",
                "MODEL_FEATURE_SNAPSHOTS_TIMESCALE_ENQUEUE_FAILED",
                e,
                warn_key="model_feature_snapshots_timescale_enqueue_failed",
                rows=int(len(rows)),
            )

    register_after_commit(con, _enqueue)


def _timestamp_or_none(value: Any) -> Optional[int]:
    out = _safe_int(value, 0)
    return int(out) if out > 0 else None


def _is_lookahead(ts_ms: Any, anchor_ts_ms: int) -> bool:
    value = _timestamp_or_none(ts_ms)
    if value is None:
        return False
    return int(value) > int(anchor_ts_ms)


def _record_validation_violation(
    violations: List[Dict[str, Any]],
    *,
    symbol: str,
    anchor_ts_ms: int,
    group: str,
    field: str,
    value: Any,
    max_examples: int,
) -> None:
    if len(violations) >= int(max_examples):
        return
    violations.append(
        {
            "symbol": str(symbol),
            "ts_ms": int(anchor_ts_ms),
            "group": str(group),
            "field": str(field),
            "value": _timestamp_or_none(value),
        }
    )


def summarize_model_feature_snapshots(
    snapshots: Sequence[Dict[str, Any]],
    *,
    max_examples: int = 8,
) -> Dict[str, Any]:
    groups = ("price", "events", "macro", "options", "social", "weather")
    availability_counts = {group: 0 for group in groups}
    violations: List[Dict[str, Any]] = []
    lookahead_violations = 0

    for snap in snapshots or []:
        symbol = str((snap or {}).get("symbol") or "").upper().strip()
        anchor_ts_ms = _safe_int((snap or {}).get("ts_ms"), 0)
        availability = dict((snap or {}).get("availability") or {})
        source_timestamps = dict((snap or {}).get("source_timestamps") or {})

        for group in groups:
            if bool(availability.get(group)):
                availability_counts[group] += 1

        checks = {
            "price.quote_ts_ms": ((source_timestamps.get("price") or {}).get("quote_ts_ms")),
            "price.history_last_ts_ms": ((source_timestamps.get("price") or {}).get("history_last_ts_ms")),
            "events.latest_event_ts_ms": ((source_timestamps.get("events") or {}).get("latest_event_ts_ms")),
            "macro.asof_ts_ms": ((source_timestamps.get("macro") or {}).get("asof_ts_ms")),
            "macro.effective_ts_ms": ((source_timestamps.get("macro") or {}).get("effective_ts_ms")),
            "options.bucket_ts_ms": ((source_timestamps.get("options") or {}).get("bucket_ts_ms")),
            "options.snapshot_ts_ms": ((source_timestamps.get("options") or {}).get("snapshot_ts_ms")),
            "social.bucket_ts_ms": ((source_timestamps.get("social") or {}).get("bucket_ts_ms")),
            "sentiment.ts_ms": ((source_timestamps.get("sentiment") or {}).get("ts_ms")),
            "weather.forecast_run_ts_ms": ((source_timestamps.get("weather") or {}).get("forecast_run_ts_ms")),
            "weather.alert_issued_ts_ms": ((source_timestamps.get("weather") or {}).get("alert_issued_ts_ms")),
        }
        for key, value in checks.items():
            if not _is_lookahead(value, int(anchor_ts_ms)):
                continue
            lookahead_violations += 1
            group, field = key.split(".", 1)
            _record_validation_violation(
                violations,
                symbol=symbol,
                anchor_ts_ms=int(anchor_ts_ms),
                group=group,
                field=field,
                value=value,
                max_examples=int(max_examples),
            )

    total = int(len(list(snapshots or [])))
    return {
        "snapshots": int(total),
        "availability_counts": {k: int(v) for k, v in availability_counts.items()},
        "availability_share": {
            k: (float(v) / float(total) if total > 0 else 0.0)
            for k, v in availability_counts.items()
        },
        "lookahead_violations": int(lookahead_violations),
        "ok": bool(lookahead_violations == 0),
        "violations": list(violations),
    }


def validate_model_feature_snapshots_or_raise(
    snapshots: Sequence[Dict[str, Any]],
    *,
    max_examples: int = 8,
    context: str = "model_feature_snapshots",
) -> Dict[str, Any]:
    validation = summarize_model_feature_snapshots(
        snapshots,
        max_examples=int(max_examples),
    )
    if bool(validation.get("ok", True)):
        return validation
    raise ValueError(
        f"{str(context)}_lookahead_detected:"
        f"{int(validation.get('lookahead_violations') or 0)}:"
        f"{_json_dumps(list(validation.get('violations') or []))}"
    )


def _latest_price_at_or_before(points: Sequence[Tuple[int, float]], target_ts_ms: int) -> Optional[float]:
    for point_ts_ms, point_price in reversed(list(points or [])):
        if int(point_ts_ms) <= int(target_ts_ms):
            return float(point_price)
    return None


def _log_return(now_price: Optional[float], then_price: Optional[float]) -> float:
    if now_price is None or then_price is None or now_price <= 0.0 or then_price <= 0.0:
        return 0.0
    try:
        return float(math.log(float(now_price) / float(then_price)))
    except Exception as e:
        _warn_nonfatal(
            "model_feature_snapshots_log_return_failed",
            "MODEL_FEATURE_SNAPSHOTS_LOG_RETURN_FAILED",
            e,
            warn_key=f"model_feature_snapshots_log_return:{now_price}:{then_price}",
            now_price=now_price,
            then_price=then_price,
        )
        return 0.0


def _pct_return(now_price: Optional[float], then_price: Optional[float]) -> float:
    if now_price is None or then_price is None or then_price <= 0.0:
        return 0.0
    try:
        return float((float(now_price) / float(then_price)) - 1.0)
    except Exception as e:
        _warn_nonfatal(
            "model_feature_snapshots_pct_return_failed",
            "MODEL_FEATURE_SNAPSHOTS_PCT_RETURN_FAILED",
            e,
            warn_key=f"model_feature_snapshots_pct_return:{now_price}:{then_price}",
            now_price=now_price,
            then_price=then_price,
        )
        return 0.0


def _rolling_rv(points: Sequence[Tuple[int, float]], n: int) -> float:
    rets: List[float] = []
    last = None
    for _ts_ms, price in points or []:
        cur = float(price)
        if last is not None and last > 0.0 and cur > 0.0:
            rets.append(float(math.log(cur / last)))
        last = cur
    if len(rets) < max(3, int(n)):
        return 0.0
    window = rets[-int(n):]
    mean = sum(window) / float(len(window))
    var = sum((x - mean) ** 2 for x in window) / float(max(1, len(window) - 1))
    return float(math.sqrt(max(0.0, var)))


def _rolling_std(points: Sequence[Tuple[int, float]], n: int) -> float:
    return _rolling_rv(points, n)


def _atr_pct(points: Sequence[Tuple[int, float]], n: int) -> float:
    moves: List[float] = []
    last = None
    for _ts_ms, price in points or []:
        cur = float(price)
        if last is not None and last > 0.0 and cur > 0.0:
            moves.append(abs(float(math.log(cur / last))))
        last = cur
    if len(moves) < max(3, int(n)):
        return 0.0
    window = moves[-int(n):]
    return float(sum(window) / float(len(window)))


def _clip(value: float, lo: float, hi: float) -> float:
    return float(max(float(lo), min(float(hi), float(value))))


def _benchmark_symbol_for(symbol: str) -> Optional[str]:
    symbol_key = str(symbol or "").upper().strip()
    if not symbol_key:
        return None
    primary = {
        "EQUITY": "SPY",
        "CRYPTO": "BTC",
        "COMMODITY": "OIL",
        "FX": "SPY",
        "RATES": "SPY",
    }
    fallback = {
        "SPY": "VIX",
        "BTC": "SPY",
        "OIL": "SPY",
    }
    bench = str(primary.get(asset_class_for_symbol(symbol_key), "") or "").upper().strip()
    if not bench or bench == symbol_key:
        bench = str(fallback.get(symbol_key, "") or "").upper().strip()
    return bench or None


def _aligned_log_return_pairs(
    points: Sequence[Tuple[int, float]],
    benchmark_points: Sequence[Tuple[int, float]],
) -> List[Tuple[float, float]]:
    if len(points or []) < 3 or len(benchmark_points or []) < 3:
        return []

    bench_idx = 0
    bench_now: Optional[float] = None
    aligned: List[Optional[float]] = []
    bench_series = list(benchmark_points or [])
    for point_ts_ms, _point_price in points or []:
        while bench_idx < len(bench_series) and int(bench_series[bench_idx][0]) <= int(point_ts_ms):
            bench_now = float(bench_series[bench_idx][1])
            bench_idx += 1
        aligned.append(bench_now)

    out: List[Tuple[float, float]] = []
    point_series = list(points or [])
    for idx in range(1, len(point_series)):
        prev_px = float(point_series[idx - 1][1])
        cur_px = float(point_series[idx][1])
        prev_bench = aligned[idx - 1]
        cur_bench = aligned[idx]
        if prev_bench is None or cur_bench is None:
            continue
        self_ret = _log_return(cur_px, prev_px)
        bench_ret = _log_return(cur_bench, prev_bench)
        out.append((float(self_ret), float(bench_ret)))
    return out


def _cross_asset_features(
    points: Sequence[Tuple[int, float]],
    benchmark_points: Sequence[Tuple[int, float]],
    *,
    latest_px: Optional[float],
    ts_ms: int,
) -> Dict[str, float]:
    out = {
        "price.cross_asset_rel_1h": 0.0,
        "price.cross_asset_rel_1d": 0.0,
        "price.cross_asset_corr_20": 0.0,
        "price.cross_asset_beta_20": 0.0,
    }
    if not points or not benchmark_points:
        return out

    bench_latest = _latest_price_at_or_before(benchmark_points, int(ts_ms))
    bench_1h = _latest_price_at_or_before(benchmark_points, int(ts_ms) - 60 * 60 * 1000)
    bench_1d = _latest_price_at_or_before(benchmark_points, int(ts_ms) - 24 * 60 * 60 * 1000)

    out["price.cross_asset_rel_1h"] = float(
        _clip(
            _log_return(latest_px, _latest_price_at_or_before(points, int(ts_ms) - 60 * 60 * 1000))
            - _log_return(bench_latest, bench_1h),
            -2.0,
            2.0,
        )
    )
    out["price.cross_asset_rel_1d"] = float(
        _clip(
            _log_return(latest_px, _latest_price_at_or_before(points, int(ts_ms) - 24 * 60 * 60 * 1000))
            - _log_return(bench_latest, bench_1d),
            -2.0,
            2.0,
        )
    )

    pairs = _aligned_log_return_pairs(points, benchmark_points)
    if len(pairs) < 20:
        return out

    recent = pairs[-20:]
    self_rets = [a for a, _b in recent]
    bench_rets = [b for _a, b in recent]
    mean_self = sum(self_rets) / float(len(self_rets))
    mean_bench = sum(bench_rets) / float(len(bench_rets))
    var_bench = sum((x - mean_bench) ** 2 for x in bench_rets) / float(max(1, len(bench_rets) - 1))
    covar = sum((a - mean_self) * (b - mean_bench) for a, b in recent) / float(max(1, len(recent) - 1))
    std_self = math.sqrt(max(0.0, sum((x - mean_self) ** 2 for x in self_rets) / float(max(1, len(self_rets) - 1))))
    std_bench = math.sqrt(max(0.0, var_bench))

    if std_self > 1e-12 and std_bench > 1e-12:
        out["price.cross_asset_corr_20"] = float(_clip(covar / (std_self * std_bench), -1.0, 1.0))
    if var_bench > 1e-12:
        out["price.cross_asset_beta_20"] = float(_clip(covar / var_bench, -5.0, 5.0))
    return out


def _trend_strength(points: Sequence[Tuple[int, float]], n: int) -> float:
    if len(points or []) < max(5, int(n) + 1):
        return 0.0
    rets: List[float] = []
    point_series = list(points or [])
    for idx in range(max(1, len(point_series) - int(n)), len(point_series)):
        rets.append(_log_return(point_series[idx][1], point_series[idx - 1][1]))
    if len(rets) < 3:
        return 0.0
    mean_ret = sum(rets) / float(len(rets))
    sd_ret = math.sqrt(
        max(
            0.0,
            sum((ret - mean_ret) ** 2 for ret in rets) / float(max(1, len(rets) - 1)),
        )
    )
    if sd_ret <= 1e-12:
        return 0.0
    return float(_clip(abs(mean_ret) / sd_ret, 0.0, 10.0))


def _volatility_regime(rv_fast: float, rv_slow: float) -> Tuple[str, float]:
    if rv_fast <= 0.0 or rv_slow <= 0.0:
        return "MID", 1.0
    ratio = float(rv_fast / max(rv_slow, 1e-9))
    if ratio <= 0.85:
        return "LOW", ratio
    if ratio >= 1.20:
        return "HIGH", ratio
    return "MID", ratio


def _load_price_group(con, *, symbol: str, ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    features = {fid: 0.0 for fid in PRICE_FEATURE_IDS}
    source_meta: Dict[str, Any] = {
        "quote_ts_ms": None,
        "history_last_ts_ms": None,
        "benchmark_symbol": None,
        "benchmark_last_ts_ms": None,
    }

    try:
        quote_row = con.execute(
            """
            SELECT ts_ms, last, bid, ask, spread, volume
            FROM price_quotes
            WHERE symbol = ?
              AND ts_ms <= ?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (str(symbol), int(ts_ms)),
        ).fetchone()
    except Exception:
        quote_row = None
    if quote_row:
        quote_ts_ms = _safe_int(quote_row[0], 0)
        last_px = _safe_float(quote_row[1], 0.0)
        bid_px = _safe_float(quote_row[2], 0.0)
        ask_px = _safe_float(quote_row[3], 0.0)
        spread = quote_row[4]
        if spread is None and bid_px > 0.0 and ask_px > 0.0:
            spread = float(ask_px - bid_px)
        spread_bps = 0.0
        if last_px > 0.0 and spread is not None:
            spread_bps = 10000.0 * _safe_float(spread, 0.0) / last_px
        features["price.last"] = float(last_px)
        features["price.spread_bps"] = float(spread_bps)
        features["price.volume"] = float(_safe_float(quote_row[5], 0.0))
        source_meta["quote_ts_ms"] = int(quote_ts_ms)

    try:
        rows = con.execute(
            """
            SELECT ts_ms, COALESCE(price, px) AS value
            FROM prices
            WHERE symbol = ?
              AND ts_ms <= ?
              AND COALESCE(price, px) IS NOT NULL
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (str(symbol), int(ts_ms), int(DEFAULT_PRICE_LOOKBACK)),
        ).fetchall()
    except Exception:
        rows = []
    points = [(int(row[0]), float(row[1])) for row in reversed(rows or []) if row and row[1] is not None]
    if points:
        latest_ts_ms = int(points[-1][0])
        latest_px = float(points[-1][1])
        source_meta["history_last_ts_ms"] = int(latest_ts_ms)
        if features["price.last"] <= 0.0:
            features["price.last"] = float(latest_px)
        px_5m = _latest_price_at_or_before(points, int(ts_ms) - 5 * 60 * 1000)
        px_1h = _latest_price_at_or_before(points, int(ts_ms) - 60 * 60 * 1000)
        px_1d = _latest_price_at_or_before(points, int(ts_ms) - 24 * 60 * 60 * 1000)

        log_ret_5m = _log_return(latest_px, px_5m)
        log_ret_1h = _log_return(latest_px, px_1h)
        log_ret_1d = _log_return(latest_px, px_1d)
        pct_ret_5m = _pct_return(latest_px, px_5m)
        pct_ret_1h = _pct_return(latest_px, px_1h)
        pct_ret_1d = _pct_return(latest_px, px_1d)
        rv_20 = _rolling_rv(points, 20)
        rv_60 = _rolling_std(points, 60)

        features["price.log_ret_5m"] = float(_clip(log_ret_5m, -2.0, 2.0))
        features["price.log_ret_1h"] = float(_clip(log_ret_1h, -2.0, 2.0))
        features["price.log_ret_1d"] = float(_clip(log_ret_1d, -2.0, 2.0))
        features["price.pct_ret_5m"] = float(_clip(pct_ret_5m, -1.0, 1.0))
        features["price.pct_ret_1h"] = float(_clip(pct_ret_1h, -1.0, 1.0))
        features["price.pct_ret_1d"] = float(_clip(pct_ret_1d, -1.0, 1.0))
        features["price.rv_20"] = float(_clip(rv_20, 0.0, 1.0))
        features["price.atr_pct_14"] = float(_clip(_atr_pct(points, 14), 0.0, 1.0))
        features["price.vol_std_20"] = float(_clip(rv_20, 0.0, 1.0))
        features["price.vol_std_60"] = float(_clip(rv_60, 0.0, 1.0))

        baseline_vol = max(float(rv_60), 1e-6)
        features["price.momentum_5m"] = float(_clip(log_ret_5m / baseline_vol, -10.0, 10.0))
        features["price.momentum_1h"] = float(_clip(log_ret_1h / baseline_vol, -10.0, 10.0))
        features["price.momentum_1d"] = float(_clip(log_ret_1d / baseline_vol, -10.0, 10.0))

        vol_regime, vol_ratio = _volatility_regime(float(rv_20), float(rv_60))
        features["price.vol_regime_low"] = 1.0 if vol_regime == "LOW" else 0.0
        features["price.vol_regime_mid"] = 1.0 if vol_regime == "MID" else 0.0
        features["price.vol_regime_high"] = 1.0 if vol_regime == "HIGH" else 0.0
        features["price.vol_regime_ratio"] = float(_clip(vol_ratio, 0.0, 5.0))

        trend_strength = _trend_strength(points, 20)
        is_trend = trend_strength >= 0.35
        features["price.trend_regime_trend"] = 1.0 if is_trend else 0.0
        features["price.trend_regime_mean_reversion"] = 0.0 if is_trend else 1.0
        features["price.trend_strength_20"] = float(_clip(trend_strength, 0.0, 10.0))

        benchmark_symbol = _benchmark_symbol_for(str(symbol))
        if benchmark_symbol:
            benchmark_rows = con.execute(
                """
                SELECT ts_ms, COALESCE(price, px) AS value
                FROM prices
                WHERE symbol = ?
                  AND ts_ms <= ?
                  AND COALESCE(price, px) IS NOT NULL
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (str(benchmark_symbol), int(ts_ms), int(DEFAULT_PRICE_LOOKBACK)),
            ).fetchall()
            benchmark_points = [
                (int(row[0]), float(row[1]))
                for row in reversed(benchmark_rows or [])
                if row and row[1] is not None
            ]
            if benchmark_points:
                source_meta["benchmark_symbol"] = str(benchmark_symbol)
                source_meta["benchmark_last_ts_ms"] = int(benchmark_points[-1][0])
                features.update(
                    _cross_asset_features(
                        points,
                        benchmark_points,
                        latest_px=latest_px,
                        ts_ms=int(ts_ms),
                    )
                )

    available = bool(source_meta.get("quote_ts_ms") or source_meta.get("history_last_ts_ms"))
    return features, source_meta, available


def _load_event_group(con, *, symbol: str, ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    features = {fid: 0.0 for fid in EVENT_FEATURE_IDS}
    cutoff_24h = int(ts_ms) - int(DEFAULT_EVENT_LOOKBACK_HOURS) * 3600 * 1000
    rows = con.execute(
        """
        SELECT
          e.ts_ms,
          e.importance_score,
          n.sentiment_score,
          n.novelty_score,
          n.is_duplicate
        FROM events e
        LEFT JOIN news_event_features n
          ON n.event_id = e.id
        WHERE e.symbol = ?
          AND e.ts_ms <= ?
          AND e.ts_ms >= ?
        ORDER BY e.ts_ms DESC
        """,
        (str(symbol), int(ts_ms), int(cutoff_24h)),
    ).fetchall()

    if not rows:
        return features, {"latest_event_ts_ms": None, "window_start_ts_ms": int(cutoff_24h)}, False

    count_1h = 0
    count_6h = 0
    sentiments_now: List[float] = []
    sentiments_prev: List[float] = []
    novelty_6h: List[float] = []
    dupes_6h = 0
    importance_24h: List[float] = []
    velocity_6h = 0.0
    latest_event_ts_ms = _safe_int(rows[0][0], 0)

    for event_ts_ms, importance_score, sentiment_score, novelty_score, is_duplicate in rows or []:
        event_ts_ms = int(event_ts_ms or 0)
        age_ms = max(0, int(ts_ms) - int(event_ts_ms))
        age_h = age_ms / 3_600_000.0
        importance_24h.append(_safe_float(importance_score, 0.0))
        if age_ms <= 3600_000:
            count_1h += 1
        if age_ms <= 6 * 3600_000:
            count_6h += 1
            velocity_6h += math.exp(-age_h)
            novelty_6h.append(_safe_float(novelty_score, 0.0))
            dupes_6h += 1 if _safe_int(is_duplicate, 0) else 0
            if age_ms <= 3 * 3600_000:
                sentiments_now.append(_safe_float(sentiment_score, 0.0))
            else:
                sentiments_prev.append(_safe_float(sentiment_score, 0.0))

    features["events.count_1h"] = float(count_1h)
    features["events.count_6h"] = float(count_6h)
    features["events.count_24h"] = float(len(rows))
    features["events.velocity_6h"] = float(velocity_6h)
    features["events.sentiment_trend_6h"] = float(
        (sum(sentiments_now) / len(sentiments_now) if sentiments_now else 0.0)
        - (sum(sentiments_prev) / len(sentiments_prev) if sentiments_prev else 0.0)
    )
    features["events.avg_novelty_6h"] = float(sum(novelty_6h) / len(novelty_6h)) if novelty_6h else 0.0
    features["events.duplicate_share_6h"] = float(dupes_6h / max(1, count_6h)) if count_6h > 0 else 0.0
    features["events.importance_mean_24h"] = (
        float(sum(importance_24h) / len(importance_24h)) if importance_24h else 0.0
    )
    features["events.hours_since_last"] = float(max(0, int(ts_ms) - int(latest_event_ts_ms)) / 3_600_000.0)
    return features, {"latest_event_ts_ms": int(latest_event_ts_ms), "window_start_ts_ms": int(cutoff_24h)}, True


def _load_insider_group(con, *, symbol: str, ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    features = {fid: 0.0 for fid in INSIDER_FEATURE_IDS}
    if not INSIDER_FEATURE_IDS:
        return features, {"latest_transaction_ts_ms": None, "window_start_ts_ms": None}, False

    window_90d_start = int(ts_ms) - int(90 * 24 * 3600 * 1000)
    window_30d_start = int(ts_ms) - int(30 * 24 * 3600 * 1000)
    query_start = min(int(window_90d_start), int(ts_ms) - int(DEFAULT_INSIDER_LOOKBACK_DAYS * 24 * 3600 * 1000))
    try:
        rows = con.execute(
            """
            SELECT
              COALESCE(transaction_ts_ms, filing_ts_ms, ingested_ts_ms) AS event_ts_ms,
              direction,
              value,
              shares,
              price,
              insider_cik,
              insider_name
            FROM insider_transactions
            WHERE symbol = ?
              AND COALESCE(transaction_ts_ms, filing_ts_ms, ingested_ts_ms) <= ?
              AND COALESCE(transaction_ts_ms, filing_ts_ms, ingested_ts_ms) >= ?
            ORDER BY COALESCE(transaction_ts_ms, filing_ts_ms, ingested_ts_ms) DESC, id DESC
            """,
            (str(symbol), int(ts_ms), int(query_start)),
        ).fetchall()
    except Exception:
        rows = []

    if not rows:
        return features, {"latest_transaction_ts_ms": None, "window_start_ts_ms": int(query_start)}, False

    buy_count_30d = 0
    sell_count_30d = 0
    net_value_30d = 0.0
    unique_insiders_90d: set[str] = set()
    latest_transaction_ts_ms = _safe_int(rows[0][0], 0)

    for event_ts_ms, direction, value, shares, price, insider_cik, insider_name in rows or []:
        event_ts_ms = _safe_int(event_ts_ms, 0)
        if event_ts_ms <= 0:
            continue
        direction_key = str(direction or "").strip().lower()
        value_num = _safe_float(value, 0.0)
        if value_num <= 0.0:
            shares_num = _safe_float(shares, 0.0)
            price_num = _safe_float(price, 0.0)
            if shares_num > 0.0 and price_num > 0.0:
                value_num = float(shares_num * price_num)

        insider_key = str(insider_cik or insider_name or "").strip().upper()
        if event_ts_ms >= int(window_90d_start) and insider_key:
            unique_insiders_90d.add(insider_key)
        if event_ts_ms < int(window_30d_start):
            continue
        if direction_key == "buy":
            buy_count_30d += 1
            net_value_30d += float(value_num)
        elif direction_key == "sell":
            sell_count_30d += 1
            net_value_30d -= float(value_num)

    features["insider.buy_count_30d"] = float(buy_count_30d)
    features["insider.sell_count_30d"] = float(sell_count_30d)
    features["insider.net_value_30d"] = float(net_value_30d)
    features["insider.unique_insiders_90d"] = float(len(unique_insiders_90d))
    return features, {"latest_transaction_ts_ms": int(latest_transaction_ts_ms), "window_start_ts_ms": int(query_start)}, True


def _load_congressional_group(con, *, symbol: str, ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    features = {fid: 0.0 for fid in CONGRESSIONAL_FEATURE_IDS}
    if not CONGRESSIONAL_FEATURE_IDS:
        return features, {"latest_trade_ts_ms": None, "window_start_ts_ms": None}, False

    window_30d_start = int(ts_ms) - int(30 * 24 * 3600 * 1000)
    query_start = int(ts_ms) - int(DEFAULT_CONGRESSIONAL_LOOKBACK_DAYS * 24 * 3600 * 1000)
    try:
        rows = con.execute(
            """
            SELECT
              COALESCE(transaction_ts_ms, disclosure_ts_ms, ingested_ts_ms) AS event_ts_ms,
              direction
            FROM congressional_trades
            WHERE symbol = ?
              AND COALESCE(transaction_ts_ms, disclosure_ts_ms, ingested_ts_ms) <= ?
              AND COALESCE(transaction_ts_ms, disclosure_ts_ms, ingested_ts_ms) >= ?
            ORDER BY COALESCE(transaction_ts_ms, disclosure_ts_ms, ingested_ts_ms) DESC, id DESC
            """,
            (str(symbol), int(ts_ms), int(query_start)),
        ).fetchall()
    except Exception:
        rows = []

    if not rows:
        return features, {"latest_trade_ts_ms": None, "window_start_ts_ms": int(query_start)}, False

    buy_count_30d = 0
    sell_count_30d = 0
    latest_trade_ts_ms = _safe_int(rows[0][0], 0)
    for event_ts_ms, direction in rows or []:
        event_ts_ms = _safe_int(event_ts_ms, 0)
        if event_ts_ms < int(window_30d_start):
            continue
        direction_key = str(direction or "").strip().lower()
        if direction_key == "buy":
            buy_count_30d += 1
        elif direction_key == "sell":
            sell_count_30d += 1

    features["congressional.buy_count_30d"] = float(buy_count_30d)
    features["congressional.sell_count_30d"] = float(sell_count_30d)
    features["congressional.net_signal_30d"] = float(buy_count_30d - sell_count_30d)
    return features, {"latest_trade_ts_ms": int(latest_trade_ts_ms), "window_start_ts_ms": int(query_start)}, True


def _load_finbert_group(con, *, symbol: str, ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    features = {fid: 0.0 for fid in FINBERT_FEATURE_IDS}
    if not FINBERT_FEATURE_IDS:
        return features, {"event_id": None, "label": None, "model_name": None, "model_version": None, "ts_ms": None}, False
    resolved, meta, available = resolve_finbert_sentiment_snapshot(
        symbol=str(symbol),
        ts_ms=int(ts_ms),
        con=con,
    )
    for fid in FINBERT_FEATURE_IDS:
        features[fid] = float(_safe_float((resolved or {}).get(fid), 0.0))
    return features, dict(meta or {}), bool(available)


def _factor_feature_row_asof(con, *, feature_id: str, ts_ms: int) -> Tuple[float, Optional[int], Optional[int]]:
    row = con.execute(
        """
        SELECT value, asof_ts, effective_ts
        FROM factor_features
        WHERE feature_id = ?
          AND asof_ts <= ?
          AND effective_ts <= ?
        ORDER BY asof_ts DESC, effective_ts DESC
        LIMIT 1
        """,
        (str(feature_id), int(ts_ms), int(ts_ms)),
    ).fetchone()
    if not row:
        return 0.0, None, None
    return _safe_float(row[0], 0.0), _safe_int(row[1], 0), _safe_int(row[2], 0)


def _load_macro_group(con, *, ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    features = {fid: 0.0 for fid in MACRO_FEATURE_IDS}
    latest_asof_ts_ms = None
    latest_effective_ts_ms = None
    available = False

    for fid in _BASE_MACRO_FEATURE_IDS:
        value, asof_ts_ms, effective_ts_ms = _factor_feature_row_asof(con, feature_id=fid, ts_ms=int(ts_ms))
        features[fid] = float(value)
        if asof_ts_ms:
            latest_asof_ts_ms = max(int(latest_asof_ts_ms or 0), int(asof_ts_ms))
            latest_effective_ts_ms = max(int(latest_effective_ts_ms or 0), int(effective_ts_ms or 0))
            available = True

    gdelt_row = con.execute(
        """
        SELECT bucket_ts_ms, doc_count, tone_mean, tone_std, conflict_share, econ_share
        FROM gdelt_macro_features
        WHERE bucket_ts_ms <= ?
        ORDER BY bucket_ts_ms DESC
        LIMIT 1
        """,
        (int(ts_ms),),
    ).fetchone()
    if gdelt_row:
        features["macro.gdelt_doc_count"] = float(_safe_float(gdelt_row[1], 0.0))
        features["macro.gdelt_tone_mean"] = float(_safe_float(gdelt_row[2], 0.0))
        features["macro.gdelt_tone_std"] = float(_safe_float(gdelt_row[3], 0.0))
        features["macro.gdelt_conflict_share"] = float(_safe_float(gdelt_row[4], 0.0))
        features["macro.gdelt_econ_share"] = float(_safe_float(gdelt_row[5], 0.0))
        latest_asof_ts_ms = max(int(latest_asof_ts_ms or 0), int(gdelt_row[0] or 0))
        latest_effective_ts_ms = max(int(latest_effective_ts_ms or 0), int(gdelt_row[0] or 0))
        available = True

    return features, {
        "asof_ts_ms": latest_asof_ts_ms,
        "effective_ts_ms": latest_effective_ts_ms,
    }, available


def _load_options_group(con, *, symbol: str, ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    features = {fid: 0.0 for fid in OPTIONS_FEATURE_IDS}
    symbol_key = str(symbol or "").upper().strip()
    if not symbol_key:
        return features, {"bucket_ts_ms": None, "snapshot_ts_ms": None}, False

    def _fetch(bucket_sec: int):
        return con.execute(
            """
            SELECT
              bucket_ts_ms,
              snapshot_ts_ms,
              iv_rank,
              iv_rank_short,
              skew_25d,
              term_structure_slope,
              unusual_volume_score,
              call_put_volume_ratio,
              call_put_oi_ratio,
              signal_score
            FROM options_symbol_features
            WHERE symbol = ?
              AND bucket_sec = ?
              AND bucket_ts_ms <= ?
            ORDER BY bucket_ts_ms DESC
            LIMIT 1
            """,
            (symbol_key, int(bucket_sec), int(_bucket_start(ts_ms, bucket_sec))),
        ).fetchone()

    row = _fetch(DEFAULT_OPTIONS_BUCKET_SEC)
    bucket_sec_used = DEFAULT_OPTIONS_BUCKET_SEC
    if not row and int(DEFAULT_OPTIONS_BUCKET_SEC) != 86400:
        row = _fetch(86400)
        bucket_sec_used = 86400
    if not row:
        return features, {"bucket_ts_ms": None, "snapshot_ts_ms": None, "bucket_sec": bucket_sec_used}, False

    features["options_symbol.iv_rank"] = float(_safe_float(row[2], 0.0))
    features["options_symbol.iv_rank_short"] = float(_safe_float(row[3], 0.0))
    features["options_symbol.skew_25d"] = float(_safe_float(row[4], 0.0))
    features["options_symbol.term_structure_slope"] = float(_safe_float(row[5], 0.0))
    features["options_symbol.unusual_volume_score"] = float(_safe_float(row[6], 0.0))
    features["options_symbol.call_put_volume_ratio"] = float(_safe_float(row[7], 1.0))
    features["options_symbol.call_put_oi_ratio"] = float(_safe_float(row[8], 1.0))
    features["options_symbol.signal_score"] = float(_safe_float(row[9], 0.0))
    return features, {
        "bucket_ts_ms": _safe_int(row[0], 0),
        "snapshot_ts_ms": _safe_int(row[1], 0),
        "bucket_sec": int(bucket_sec_used),
    }, True


def _load_social_group(con, *, symbol: str, ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    features = {fid: 0.0 for fid in SOCIAL_FEATURE_IDS}
    symbol_key = str(symbol or "").upper().strip()
    if not symbol_key:
        return features, {"bucket_ts_ms": None, "bucket_sec": DEFAULT_SOCIAL_BUCKET_SEC}, False

    row = con.execute(
        """
        SELECT
          bucket_ts_ms,
          mention_count,
          unique_authors,
          new_author_ratio,
          engagement_now,
          sentiment_mean,
          sentiment_dispersion,
          mention_rate_z,
          bot_likelihood_mean,
          promo_likelihood_mean,
          manip_risk,
          attention_shock,
          cross_platform_confirm
        FROM social_features
        WHERE symbol = ?
          AND bucket_sec = ?
          AND bucket_ts_ms <= ?
        ORDER BY bucket_ts_ms DESC
        LIMIT 1
        """,
        (symbol_key, int(DEFAULT_SOCIAL_BUCKET_SEC), int(_bucket_start(ts_ms, DEFAULT_SOCIAL_BUCKET_SEC))),
    ).fetchone()
    if not row:
        return features, {"bucket_ts_ms": None, "bucket_sec": DEFAULT_SOCIAL_BUCKET_SEC}, False

    social_base = {
        "mention_count": _safe_float(row[1], 0.0),
        "unique_authors": _safe_float(row[2], 0.0),
        "new_author_ratio": _safe_float(row[3], 0.0),
        "engagement_now": _safe_float(row[4], 0.0),
        "sentiment_mean": _safe_float(row[5], 0.0),
        "sentiment_dispersion": _safe_float(row[6], 0.0),
        "mention_rate_z": _safe_float(row[7], 0.0),
        "bot_likelihood_mean": _safe_float(row[8], 0.0),
        "promo_likelihood_mean": _safe_float(row[9], 0.0),
        "manip_risk": _safe_float(row[10], 0.0),
        "attention_shock": _safe_float(row[11], 0.0),
        "cross_platform_confirm": _safe_float(row[12], 0.0),
    }
    regime = classify_regime_from_features(social_base)

    features["social.mention_rate_z"] = float(social_base["mention_rate_z"])
    features["social.unique_authors"] = float(social_base["unique_authors"])
    features["social.new_author_ratio"] = float(social_base["new_author_ratio"])
    features["social.sentiment_mean"] = float(social_base["sentiment_mean"])
    features["social.sentiment_dispersion"] = float(social_base["sentiment_dispersion"])
    features["social.manip_risk"] = float(social_base["manip_risk"])
    features["social.attention_shock"] = float(social_base["attention_shock"])
    features["social.promo_likelihood_mean"] = float(social_base["promo_likelihood_mean"])
    features["social_regime.mania_score"] = float(_safe_float(regime.get("mania_score"), 0.0))
    features["social_regime.fear_score"] = float(_safe_float(regime.get("fear_score"), 0.0))
    features["social_regime.churn_score"] = float(_safe_float(regime.get("churn_score"), 0.0))
    features["social_regime.regime_quiet"] = 1.0 if str(regime.get("regime") or "").upper() == "QUIET" else 0.0
    features["social_regime.regime_churn"] = 1.0 if str(regime.get("regime") or "").upper() == "CHURN" else 0.0
    features["social_regime.regime_fear"] = 1.0 if str(regime.get("regime") or "").upper() == "FEAR" else 0.0
    features["social_regime.regime_mania"] = 1.0 if str(regime.get("regime") or "").upper() == "MANIA" else 0.0
    features["social_regime.regime_conf"] = float(_safe_float(regime.get("regime_conf"), 0.0))
    return features, {
        "bucket_ts_ms": _safe_int(row[0], 0),
        "bucket_sec": int(DEFAULT_SOCIAL_BUCKET_SEC),
    }, True


def _load_weather_source_timestamps(con, *, symbol: str, ts_ms: int) -> Dict[str, Any]:
    cfg = load_weather_region_map()
    regions = symbol_regions(str(symbol or ""), cfg)
    if not regions:
        return {"forecast_run_ts_ms": None, "alert_issued_ts_ms": None}

    forecast_run_ts_ms = 0
    for region_id, _weight, _channels in regions:
        row = con.execute(
            """
            SELECT MAX(run_ts)
            FROM weather_forecast_region_daily
            WHERE provider = ?
              AND region_id = ?
              AND run_ts <= ?
            """,
            (str(DEFAULT_WEATHER_PROVIDER), str(region_id), int(ts_ms)),
        ).fetchone()
        forecast_run_ts_ms = max(forecast_run_ts_ms, _safe_int((row or [0])[0], 0))

    alert_rows = con.execute(
        """
        SELECT issued_ts, expires_ts, affected_regions
        FROM weather_alerts
        WHERE provider = ?
          AND issued_ts <= ?
          AND issued_ts >= ?
        ORDER BY issued_ts DESC
        """,
        (str(DEFAULT_WEATHER_ALERTS_PROVIDER), int(ts_ms), int(ts_ms) - 14 * 24 * 3600 * 1000),
    ).fetchall()
    region_ids = {str(region_id) for region_id, _weight, _channels in regions}
    alert_issued_ts_ms = 0
    for issued_ts, expires_ts, affected_regions in alert_rows or []:
        issued_value = _safe_int(issued_ts, 0)
        expires_value = _safe_int(expires_ts, 0)
        if issued_value <= 0:
            continue
        if expires_value > 0 and int(ts_ms) > int(expires_value):
            continue
        try:
            parsed = json.loads(affected_regions) if affected_regions else []
        except Exception as exc:
            _warn_nonfatal(
                "model_feature_snapshots_weather_regions_parse_failed",
                "MODEL_FEATURE_SNAPSHOTS_WEATHER_REGIONS_PARSE_FAILED",
                exc,
                warn_key="model_feature_snapshots_weather_regions_parse_failed",
            )
            parsed = []
        if not isinstance(parsed, list):
            parsed = []
        if region_ids.intersection({str(value) for value in parsed}):
            alert_issued_ts_ms = max(alert_issued_ts_ms, int(issued_value))
            break

    return {
        "forecast_run_ts_ms": int(forecast_run_ts_ms) if forecast_run_ts_ms > 0 else None,
        "alert_issued_ts_ms": int(alert_issued_ts_ms) if alert_issued_ts_ms > 0 else None,
    }


def _load_weather_group(con, *, symbol: str, ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    features = {fid: 0.0 for fid in WEATHER_FEATURE_IDS}
    snapshot = get_weather_feature_snapshot(symbol=str(symbol), ts_ms=int(ts_ms)) or {}
    for fid in WEATHER_FEATURE_IDS:
        key = fid.split(".", 1)[1]
        features[fid] = float(_safe_float(snapshot.get(key), 0.0))
    source_meta = _load_weather_source_timestamps(con, symbol=str(symbol), ts_ms=int(ts_ms))
    available = any(value is not None for value in source_meta.values()) or any(
        abs(float(value or 0.0)) > 0.0 for value in snapshot.values()
    )
    return features, source_meta, available


def build_model_feature_snapshot(
    *,
    symbol: str,
    ts_ms: int,
    feature_ids: Optional[Sequence[str]] = None,
    con=None,
) -> Dict[str, Any]:
    symbol_key = str(symbol or "").upper().strip()
    anchor_ts_ms = int(ts_ms)
    ids = list(feature_ids or UNIFIED_SYMBOL_FEATURE_IDS)

    group_features: Dict[str, float] = {}
    source_timestamps: Dict[str, Any] = {"anchor_ts_ms": int(anchor_ts_ms)}

    owns = False
    if con is None:
        con = connect()
        owns = True
    try:
        try:
            price_features, price_meta, price_available = _load_price_group(con, symbol=symbol_key, ts_ms=anchor_ts_ms)
        except Exception as exc:
            _warn_nonfatal(
                "model_feature_snapshots_price_group_failed",
                "MODEL_FEATURE_SNAPSHOTS_PRICE_GROUP_FAILED",
                exc,
                warn_key="model_feature_snapshots_price_group_failed",
                symbol=symbol_key,
                ts_ms=int(anchor_ts_ms),
            )
            price_features, price_meta, price_available = ({fid: 0.0 for fid in PRICE_FEATURE_IDS}, {"quote_ts_ms": None}, False)
        try:
            event_features, event_meta, event_available = _load_event_group(con, symbol=symbol_key, ts_ms=anchor_ts_ms)
        except Exception as exc:
            _warn_nonfatal(
                "model_feature_snapshots_event_group_failed",
                "MODEL_FEATURE_SNAPSHOTS_EVENT_GROUP_FAILED",
                exc,
                warn_key="model_feature_snapshots_event_group_failed",
                symbol=symbol_key,
                ts_ms=int(anchor_ts_ms),
            )
            event_features, event_meta, event_available = ({fid: 0.0 for fid in EVENT_FEATURE_IDS}, {"latest_event_ts_ms": None}, False)
        try:
            macro_features, macro_meta, macro_available = _load_macro_group(con, ts_ms=anchor_ts_ms)
        except Exception as exc:
            _warn_nonfatal(
                "model_feature_snapshots_macro_group_failed",
                "MODEL_FEATURE_SNAPSHOTS_MACRO_GROUP_FAILED",
                exc,
                warn_key="model_feature_snapshots_macro_group_failed",
                ts_ms=int(anchor_ts_ms),
            )
            macro_features, macro_meta, macro_available = ({fid: 0.0 for fid in MACRO_FEATURE_IDS}, {"asof_ts_ms": None}, False)
        try:
            options_features, options_meta, options_available = _load_options_group(con, symbol=symbol_key, ts_ms=anchor_ts_ms)
        except Exception as exc:
            _warn_nonfatal(
                "model_feature_snapshots_options_group_failed",
                "MODEL_FEATURE_SNAPSHOTS_OPTIONS_GROUP_FAILED",
                exc,
                warn_key="model_feature_snapshots_options_group_failed",
                symbol=symbol_key,
                ts_ms=int(anchor_ts_ms),
            )
            options_features, options_meta, options_available = ({fid: 0.0 for fid in OPTIONS_FEATURE_IDS}, {"snapshot_ts_ms": None}, False)
        try:
            insider_features, insider_meta, _insider_available = _load_insider_group(con, symbol=symbol_key, ts_ms=anchor_ts_ms)
        except Exception as exc:
            _warn_nonfatal(
                "model_feature_snapshots_insider_group_failed",
                "MODEL_FEATURE_SNAPSHOTS_INSIDER_GROUP_FAILED",
                exc,
                warn_key="model_feature_snapshots_insider_group_failed",
                symbol=symbol_key,
                ts_ms=int(anchor_ts_ms),
            )
            insider_features, insider_meta = ({fid: 0.0 for fid in INSIDER_FEATURE_IDS}, {"latest_transaction_ts_ms": None})
        try:
            congressional_features, congressional_meta, _congressional_available = _load_congressional_group(
                con,
                symbol=symbol_key,
                ts_ms=anchor_ts_ms,
            )
        except Exception as exc:
            _warn_nonfatal(
                "model_feature_snapshots_congressional_group_failed",
                "MODEL_FEATURE_SNAPSHOTS_CONGRESSIONAL_GROUP_FAILED",
                exc,
                warn_key="model_feature_snapshots_congressional_group_failed",
                symbol=symbol_key,
                ts_ms=int(anchor_ts_ms),
            )
            congressional_features, congressional_meta = ({fid: 0.0 for fid in CONGRESSIONAL_FEATURE_IDS}, {"latest_trade_ts_ms": None})
        try:
            social_features, social_meta, social_available = _load_social_group(con, symbol=symbol_key, ts_ms=anchor_ts_ms)
        except Exception as exc:
            _warn_nonfatal(
                "model_feature_snapshots_social_group_failed",
                "MODEL_FEATURE_SNAPSHOTS_SOCIAL_GROUP_FAILED",
                exc,
                warn_key="model_feature_snapshots_social_group_failed",
                symbol=symbol_key,
                ts_ms=int(anchor_ts_ms),
            )
            social_features, social_meta, social_available = ({fid: 0.0 for fid in SOCIAL_FEATURE_IDS}, {"bucket_ts_ms": None}, False)
        try:
            finbert_features, finbert_meta, _finbert_available = _load_finbert_group(con, symbol=symbol_key, ts_ms=anchor_ts_ms)
        except Exception as exc:
            _warn_nonfatal(
                "model_feature_snapshots_finbert_group_failed",
                "MODEL_FEATURE_SNAPSHOTS_FINBERT_GROUP_FAILED",
                exc,
                warn_key="model_feature_snapshots_finbert_group_failed",
                symbol=symbol_key,
                ts_ms=int(anchor_ts_ms),
            )
            finbert_features, finbert_meta = ({fid: 0.0 for fid in FINBERT_FEATURE_IDS}, {"ts_ms": None})
        try:
            weather_features, weather_meta, weather_available = _load_weather_group(con, symbol=symbol_key, ts_ms=anchor_ts_ms)
        except Exception as exc:
            _warn_nonfatal(
                "model_feature_snapshots_weather_group_failed",
                "MODEL_FEATURE_SNAPSHOTS_WEATHER_GROUP_FAILED",
                exc,
                warn_key="model_feature_snapshots_weather_group_failed",
                symbol=symbol_key,
                ts_ms=int(anchor_ts_ms),
            )
            weather_features, weather_meta, weather_available = ({fid: 0.0 for fid in WEATHER_FEATURE_IDS}, {"forecast_run_ts_ms": None}, False)

        for mapping in (
            price_features,
            event_features,
            macro_features,
            options_features,
            insider_features,
            congressional_features,
            social_features,
            finbert_features,
            weather_features,
        ):
            group_features.update(mapping)

        availability = {
            "price": bool(price_available),
            "events": bool(event_available),
            "macro": bool(macro_available),
            "options": bool(options_available),
            "social": bool(social_available),
            "weather": bool(weather_available),
        }
        group_features["availability.price"] = 1.0 if availability["price"] else 0.0
        group_features["availability.events"] = 1.0 if availability["events"] else 0.0
        group_features["availability.macro"] = 1.0 if availability["macro"] else 0.0
        group_features["availability.options"] = 1.0 if availability["options"] else 0.0
        group_features["availability.social"] = 1.0 if availability["social"] else 0.0
        group_features["availability.weather"] = 1.0 if availability["weather"] else 0.0

        source_timestamps.update(
            {
                "price": price_meta,
                "events": event_meta,
                "macro": macro_meta,
                "options": options_meta,
                "insider": insider_meta,
                "congressional": congressional_meta,
                "social": social_meta,
                "sentiment": finbert_meta,
                "weather": weather_meta,
            }
        )

        features = {fid: float(_safe_float(group_features.get(fid), 0.0)) for fid in ids}
        vector = [float(features[fid]) for fid in ids]
        return {
            "symbol": str(symbol_key),
            "ts_ms": int(anchor_ts_ms),
            "feature_set_tag": str(_feature_set_tag(ids)),
            "snapshot_version": int(SNAPSHOT_VERSION),
            "feature_ids": list(ids),
            "vector": list(vector),
            "features": dict(features),
            "source_timestamps": dict(source_timestamps),
            "availability": dict(availability),
        }
    finally:
        if owns:
            try:
                con.close()
            except Exception as exc:
                _warn_nonfatal(
                    "model_feature_snapshots_close_failed",
                    "MODEL_FEATURE_SNAPSHOTS_CLOSE_FAILED",
                    exc,
                    warn_key="model_feature_snapshots_close_failed",
                )


def store_model_feature_snapshots(
    snapshots: Iterable[Dict[str, Any]],
    *,
    con=None,
) -> int:
    snapshot_list = list(snapshots or [])
    owns = False
    if con is None:
        con = connect(readonly=False)
        owns = True
    rows = []
    now_ms = int(time.time() * 1000)
    for snap in snapshot_list:
        symbol = str(snap.get("symbol") or "").upper().strip()
        if not symbol:
            continue
        rows.append(
            (
                symbol,
                int(snap.get("ts_ms") or 0),
                str(snap.get("feature_set_tag") or FEATURE_SET_TAG),
                int(snap.get("snapshot_version") or SNAPSHOT_VERSION),
                _json_dumps(list(snap.get("feature_ids") or [])),
                _json_dumps(list(snap.get("vector") or [])),
                _json_dumps(dict(snap.get("features") or {})),
                _json_dumps(dict(snap.get("source_timestamps") or {})),
                _json_dumps(dict(snap.get("availability") or {})),
                int(now_ms),
            )
        )
    if not rows:
        return 0

    try:
        con.executemany(
            """
            INSERT OR REPLACE INTO model_feature_snapshots(
              symbol, ts_ms, feature_set_tag, snapshot_version,
              feature_ids_json, vector_json, features_json,
              source_timestamps_json, availability_json, created_ts_ms
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        _register_timescale_feature_rows_after_commit(con, snapshot_list)
        try:
            from engine.cache.wrappers.feature_snapshots import prime_feature_snapshot

            def _prime_cache() -> None:
                for snap in snapshot_list:
                    try:
                        prime_feature_snapshot(dict(snap or {}))
                    except Exception:
                        continue

            register_after_commit(con, _prime_cache)
        except Exception:
            logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)
        if owns:
            con.commit()
        return int(len(rows))
    finally:
        if owns:
            try:
                con.close()
            except Exception as exc:
                _warn_nonfatal(
                    "model_feature_snapshots_validation_close_failed",
                    "MODEL_FEATURE_SNAPSHOTS_VALIDATION_CLOSE_FAILED",
                    exc,
                    warn_key="model_feature_snapshots_validation_close_failed",
                )


def materialize_model_feature_snapshots(
    *,
    symbols: Sequence[str],
    ts_ms: int,
    feature_ids: Optional[Sequence[str]] = None,
    strict_validation: bool = False,
    con=None,
) -> Dict[str, Any]:
    ids = list(feature_ids or UNIFIED_SYMBOL_FEATURE_IDS)
    owns = False
    if con is None:
        con = connect(readonly=False)
        owns = True
    try:
        snapshots = [
            build_model_feature_snapshot(
                symbol=str(symbol),
                ts_ms=int(ts_ms),
                feature_ids=ids,
                con=con,
            )
            for symbol in (symbols or [])
            if str(symbol or "").strip()
        ]
        validation = (
            validate_model_feature_snapshots_or_raise(
                snapshots,
                context="materialize_model_feature_snapshots",
            )
            if strict_validation
            else summarize_model_feature_snapshots(snapshots)
        )
        written = store_model_feature_snapshots(snapshots, con=con)
        if owns:
            con.commit()
        return {
            "snapshots": int(written),
            "symbols": int(len(snapshots)),
            "feature_dim": int(len(ids)),
            "feature_set_tag": str(_feature_set_tag(ids)),
            "ts_ms": int(ts_ms),
            "validation": dict(validation),
        }
    finally:
        if owns:
            try:
                con.close()
            except Exception as exc:
                _warn_nonfatal(
                    "model_feature_snapshots_latest_close_failed",
                    "MODEL_FEATURE_SNAPSHOTS_LATEST_CLOSE_FAILED",
                    exc,
                    warn_key="model_feature_snapshots_latest_close_failed",
                )


def backfill_model_feature_snapshots(
    *,
    symbols: Sequence[str],
    start_ts_ms: int,
    end_ts_ms: int,
    bucket_sec: int = DEFAULT_SNAPSHOT_BUCKET_SEC,
    feature_ids: Optional[Sequence[str]] = None,
    strict_validation: bool = False,
    con=None,
) -> Dict[str, Any]:
    bucket_ms = max(1, int(bucket_sec)) * 1000
    start_bucket = _bucket_start(int(start_ts_ms), int(bucket_sec))
    end_bucket = _bucket_start(int(end_ts_ms), int(bucket_sec))
    owns = False
    if con is None:
        con = connect(readonly=False)
        owns = True
    total_written = 0
    total_ticks = 0
    last_ts_ms = start_bucket
    try:
        ts_cursor = int(start_bucket)
        while ts_cursor <= int(end_bucket):
            stats = materialize_model_feature_snapshots(
                symbols=symbols,
                ts_ms=int(ts_cursor),
                feature_ids=feature_ids,
                strict_validation=bool(strict_validation),
                con=con,
            )
            total_written += int(stats.get("snapshots") or 0)
            total_ticks += 1
            last_ts_ms = int(ts_cursor)
            ts_cursor += int(bucket_ms)
        if owns:
            con.commit()
        return {
            "snapshots": int(total_written),
            "ticks": int(total_ticks),
            "symbols": int(len([s for s in symbols or [] if str(s or "").strip()])),
            "feature_dim": int(len(list(feature_ids or UNIFIED_SYMBOL_FEATURE_IDS))),
            "feature_set_tag": str(_feature_set_tag(list(feature_ids or UNIFIED_SYMBOL_FEATURE_IDS))),
            "bucket_sec": int(bucket_sec),
            "last_ts_ms": int(last_ts_ms),
        }
    finally:
        if owns:
            try:
                con.close()
            except Exception as exc:
                _warn_nonfatal(
                    "model_feature_snapshots_history_close_failed",
                    "MODEL_FEATURE_SNAPSHOTS_HISTORY_CLOSE_FAILED",
                    exc,
                    warn_key="model_feature_snapshots_history_close_failed",
                )


def load_model_feature_snapshot(
    *,
    symbol: str,
    ts_ms: int,
    feature_set_tag: str = FEATURE_SET_TAG,
    exact: bool = False,
    con=None,
) -> Optional[Dict[str, Any]]:
    if not bool(exact):
        try:
            current_ms = int(time.time() * 1000)
            if int(ts_ms) >= current_ms - int(DEFAULT_SNAPSHOT_BUCKET_SEC * 1000):
                from engine.cache.wrappers.feature_snapshots import latest

                cached = latest(str(symbol), str(feature_set_tag))
                if isinstance(cached, dict) and cached:
                    return cached
        except Exception:
            logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)
    owns = False
    if con is None:
        con = connect()
        owns = True
    try:
        symbol_key = str(symbol or "").upper().strip()
        if not symbol_key:
            return None
        comparator = "=" if bool(exact) else "<="
        row = con.execute(
            f"""
            SELECT
              symbol,
              ts_ms,
              feature_set_tag,
              snapshot_version,
              feature_ids_json,
              vector_json,
              features_json,
              source_timestamps_json,
              availability_json,
              created_ts_ms
            FROM model_feature_snapshots
            WHERE symbol = ?
              AND feature_set_tag = ?
              AND ts_ms {comparator} ?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (symbol_key, str(feature_set_tag), int(ts_ms)),
        ).fetchone()
        if not row:
            return None
        return {
            "symbol": str(row[0] or ""),
            "ts_ms": _safe_int(row[1], 0),
            "feature_set_tag": str(row[2] or ""),
            "snapshot_version": _safe_int(row[3], SNAPSHOT_VERSION),
            "feature_ids": json.loads(str(row[4] or "[]")),
            "vector": json.loads(str(row[5] or "[]")),
            "features": json.loads(str(row[6] or "{}")),
            "source_timestamps": json.loads(str(row[7] or "{}")),
            "availability": json.loads(str(row[8] or "{}")),
            "created_ts_ms": _safe_int(row[9], 0),
        }
    finally:
        if owns:
            try:
                con.close()
            except Exception as exc:
                _warn_nonfatal(
                    "model_feature_snapshots_replay_close_failed",
                    "MODEL_FEATURE_SNAPSHOTS_REPLAY_CLOSE_FAILED",
                    exc,
                    warn_key="model_feature_snapshots_replay_close_failed",
                )
