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
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

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
from engine.data.sec.form4_classifier import (
    OPPORTUNISTIC,
    availability_ts_ms as _insider_availability_ts_ms,
    classify_insider_trade,
    insider_key as _insider_key,
    role_buy_weight as _insider_role_buy_weight,
    transaction_value as _insider_transaction_value,
)
from engine.data.finra_short import (
    asof_date as _finra_asof_date,
    parse_date as _parse_finra_date,
    short_interest_surprise as _short_interest_surprise,
    short_volume_ratio_z as _short_volume_ratio_z,
)
from engine.data.crypto_positioning import compute_positioning_features as _compute_crypto_positioning_features
from engine.data.weather_features import get_weather_feature_snapshot
from engine.data.weather_mapping import load_weather_region_map, symbol_regions
from engine.data.asset_map import asset_class_for_symbol
from engine.data.prediction_market_features import (
    resolve_prediction_market_event_snapshot,
    resolve_prediction_market_macro_snapshot,
)
from engine.data.prediction_market_providers import (
    PREDICTION_MARKET_EVENT_FEATURE_GROUP,
    PREDICTION_MARKET_EVENT_FEATURE_IDS,
    PREDICTION_MARKET_MACRO_FEATURE_GROUP,
    PREDICTION_MARKET_MACRO_FEATURE_IDS,
)
from engine.data.deribit_crypto_derivatives import (
    DERIBIT_FEATURE_GROUP,
    DERIBIT_FEATURE_IDS,
    DERIBIT_FEATURE_PREFIX,
    resolve_deribit_crypto_derivatives_snapshot,
)
from engine.data.sportsbook_odds import (
    SPORTSBOOK_ODDS_FEATURE_GROUP,
    SPORTSBOOK_ODDS_FEATURE_IDS,
    SPORTSBOOK_ODDS_FEATURE_PREFIX,
    resolve_sportsbook_odds_snapshot,
)
from engine.runtime.storage import connect, get_timescale_client, register_after_commit
from engine.strategy.feature_registry import (
    BOCPD_FEATURE_IDS,
    CONGRESSIONAL_FEATURE_IDS,
    COT_FEATURE_IDS,
    CRYPTO_POSITIONING_FEATURE_IDS,
    EVENT_FEATURE_IDS,
    ETF_FLOW_FEATURE_IDS,
    FUNDAMENTALS_PIT_FEATURE_IDS,
    GOV_FEATURE_IDS,
    INST_13F_FEATURE_IDS,
    INSIDER_FEATURE_IDS,
    MACRO_FEATURE_IDS as _BASE_MACRO_FEATURE_IDS,
    NEWS_FLOW_FEATURE_IDS,
    OPTIONS_FEATURE_IDS,
    PRICE_FEATURE_IDS,
    SHORT_FEATURE_IDS,
    STRUCTURED_DOCUMENT_EVENT_FEATURE_IDS,
    TECH_FEATURE_IDS,
    TS_FOUNDATION_CHRONOS_FEATURE_IDS,
    UNIFIED_MACRO_FEATURE_IDS as MACRO_FEATURE_IDS,
    UNIFIED_SOCIAL_FEATURE_IDS as SOCIAL_FEATURE_IDS,
    UNIFIED_SYMBOL_FEATURE_IDS,
    WEATHER_FEATURE_IDS,
)
from engine.strategy.feature_pit import (
    enforce_feature_pit_controls,
    evaluate_group_policy,
    policy_metadata_for_groups,
)
from engine.strategy.graph_relational import (
    GRAPH_RELATIONAL_FEATURE_IDS,
    GRAPH_RELATIONAL_GROUP,
    GRAPH_RELATIONAL_PREFIX,
    build_graph_relational_snapshot,
    graph_metadata_from_snapshot,
)
from engine.strategy.social_regime import classify_regime_from_features
from engine.strategy.ts_foundation_encoder import (
    TS_FOUNDATION_CHRONOS_GROUP,
    resolve_chronos_foundation_features,
)

FEATURE_SET_TAG = "unified_symbol_v1"
SNAPSHOT_VERSION = 1

DEFAULT_PRICE_LOOKBACK = max(80, int(os.environ.get("MODEL_FEATURE_PRICE_LOOKBACK", "512")))
DEFAULT_EVENT_LOOKBACK_HOURS = max(6, int(os.environ.get("MODEL_FEATURE_EVENT_LOOKBACK_HOURS", "24")))
DEFAULT_OPTIONS_BUCKET_SEC = max(60, int(os.environ.get("OPTIONS_INTRADAY_BUCKET_SEC", "900")))
DEFAULT_SOCIAL_BUCKET_SEC = max(60, int(os.environ.get("SOCIAL_DEFAULT_BUCKET_SEC", "300")))
DEFAULT_SNAPSHOT_BUCKET_SEC = max(60, int(os.environ.get("MODEL_FEATURE_SNAPSHOT_BUCKET_SEC", "300")))
DEFAULT_INSIDER_LOOKBACK_DAYS = max(4 * 366, int(CONFIG_FORM4_BACKFILL_DAYS))
DEFAULT_CONGRESSIONAL_LOOKBACK_DAYS = max(30, int(CONFIG_CONGRESSIONAL_BACKFILL_DAYS))
DEFAULT_SHORT_VOLUME_LOOKBACK_DAYS = max(45, int(os.environ.get("FINRA_SHORT_VOLUME_FEATURE_LOOKBACK_DAYS", "45")))
DEFAULT_SHORT_INTEREST_LOOKBACK_READINGS = max(6, int(os.environ.get("FINRA_SHORT_INTEREST_FEATURE_LOOKBACK_READINGS", "24")))
DEFAULT_SHORT_SI_EWMA_ALPHA = float(os.environ.get("SHORT_SI_EWMA_ALPHA", "0.5"))
DEFAULT_CRYPTO_POSITIONING_LOOKBACK_DAYS = max(30, int(os.environ.get("CRYPTO_POSITIONING_FEATURE_LOOKBACK_DAYS", "30")))
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
    if any(str(fid or "").startswith(GRAPH_RELATIONAL_PREFIX) for fid in list(feature_ids or [])):
        return f"{FEATURE_SET_TAG}+graph_relational_v1_shadow"
    if any(str(fid or "").startswith("prediction_market_event_v1.") for fid in list(feature_ids or [])):
        return f"{FEATURE_SET_TAG}+prediction_market_event_v1_shadow"
    if any(str(fid or "").startswith(DERIBIT_FEATURE_PREFIX) for fid in list(feature_ids or [])):
        return f"{FEATURE_SET_TAG}+deribit_crypto_derivatives_v1_shadow"
    if any(str(fid or "").startswith(SPORTSBOOK_ODDS_FEATURE_PREFIX) for fid in list(feature_ids or [])):
        return f"{FEATURE_SET_TAG}+sports_odds_sector_v1_shadow"
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
                    "feature_metadata": dict(snap.get("feature_metadata") or {}),
                    "pit_controls": dict(snap.get("pit_controls") or {}),
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
    groups = (
        "price",
        "events",
        "macro",
        "options",
        "insider",
        "short",
        "crypto_positioning",
        "news_flow",
        "structured_doc_events",
        "etf_flow",
        "cot",
        "inst_13f",
        "gov",
        "fundamentals",
        "congressional",
        "social",
        "sentiment",
        "weather",
        "bocpd_regime",
        DERIBIT_FEATURE_GROUP,
        SPORTSBOOK_ODDS_FEATURE_GROUP,
        PREDICTION_MARKET_MACRO_FEATURE_GROUP,
        PREDICTION_MARKET_EVENT_FEATURE_GROUP,
        TS_FOUNDATION_CHRONOS_GROUP,
        GRAPH_RELATIONAL_GROUP,
    )
    availability_counts = {group: 0 for group in groups}
    violations: List[Dict[str, Any]] = []
    lookahead_violations = 0
    pit_policy_violations = 0

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
            "insider.latest_availability_ts_ms": ((source_timestamps.get("insider") or {}).get("latest_availability_ts_ms")),
            "short.latest_short_volume_availability_ts_ms": (
                (source_timestamps.get("short") or {}).get("latest_short_volume_availability_ts_ms")
            ),
            "short.latest_short_interest_availability_ts_ms": (
                (source_timestamps.get("short") or {}).get("latest_short_interest_availability_ts_ms")
            ),
            "crypto_positioning.latest_availability_ts_ms": (
                (source_timestamps.get("crypto_positioning") or {}).get("latest_availability_ts_ms")
            ),
            "news_flow.latest_availability_ts_ms": (
                (source_timestamps.get("news_flow") or {}).get("latest_availability_ts_ms")
            ),
            "structured_doc_events.latest_availability_ts_ms": (
                (source_timestamps.get("structured_doc_events") or {}).get("latest_availability_ts_ms")
            ),
            "structured_doc_events.latest_event_ts_ms": (
                (source_timestamps.get("structured_doc_events") or {}).get("latest_event_ts_ms")
            ),
            "etf_flow.latest_availability_ts_ms": (
                (source_timestamps.get("etf_flow") or {}).get("latest_availability_ts_ms")
            ),
            "cot.latest_availability_ts_ms": (
                (source_timestamps.get("cot") or {}).get("latest_availability_ts_ms")
            ),
            "inst_13f.latest_availability_ts_ms": (
                (source_timestamps.get("inst_13f") or {}).get("latest_availability_ts_ms")
            ),
            "gov.latest_availability_ts_ms": (
                (source_timestamps.get("gov") or {}).get("latest_availability_ts_ms")
            ),
            "fundamentals.latest_publish_ts_ms": (
                (source_timestamps.get("fundamentals") or {}).get("latest_publish_ts_ms")
            ),
            "congressional.latest_availability_ts_ms": (
                (source_timestamps.get("congressional") or {}).get("latest_availability_ts_ms")
            ),
            "congressional.latest_trade_ts_ms": (
                (source_timestamps.get("congressional") or {}).get("latest_trade_ts_ms")
            ),
            "congressional.latest_transaction_ts_ms": (
                (source_timestamps.get("congressional") or {}).get("latest_transaction_ts_ms")
            ),
            "social.bucket_ts_ms": ((source_timestamps.get("social") or {}).get("bucket_ts_ms")),
            "sentiment.ts_ms": ((source_timestamps.get("sentiment") or {}).get("ts_ms")),
            "weather.forecast_run_ts_ms": ((source_timestamps.get("weather") or {}).get("forecast_run_ts_ms")),
            "weather.alert_issued_ts_ms": ((source_timestamps.get("weather") or {}).get("alert_issued_ts_ms")),
            f"{DERIBIT_FEATURE_GROUP}.latest_source_ts_ms": (
                (source_timestamps.get(DERIBIT_FEATURE_GROUP) or {}).get("latest_source_ts_ms")
            ),
            f"{DERIBIT_FEATURE_GROUP}.latest_availability_ts_ms": (
                (source_timestamps.get(DERIBIT_FEATURE_GROUP) or {}).get("latest_availability_ts_ms")
            ),
            f"{SPORTSBOOK_ODDS_FEATURE_GROUP}.latest_source_ts_ms": (
                (source_timestamps.get(SPORTSBOOK_ODDS_FEATURE_GROUP) or {}).get("latest_source_ts_ms")
            ),
            f"{SPORTSBOOK_ODDS_FEATURE_GROUP}.latest_availability_ts_ms": (
                (source_timestamps.get(SPORTSBOOK_ODDS_FEATURE_GROUP) or {}).get("latest_availability_ts_ms")
            ),
            f"{PREDICTION_MARKET_MACRO_FEATURE_GROUP}.latest_source_ts_ms": (
                (source_timestamps.get(PREDICTION_MARKET_MACRO_FEATURE_GROUP) or {}).get("latest_source_ts_ms")
            ),
            f"{PREDICTION_MARKET_EVENT_FEATURE_GROUP}.latest_source_ts_ms": (
                (source_timestamps.get(PREDICTION_MARKET_EVENT_FEATURE_GROUP) or {}).get("latest_source_ts_ms")
            ),
            f"{PREDICTION_MARKET_MACRO_FEATURE_GROUP}.latest_availability_ts_ms": (
                (source_timestamps.get(PREDICTION_MARKET_MACRO_FEATURE_GROUP) or {}).get("latest_availability_ts_ms")
            ),
            f"{TS_FOUNDATION_CHRONOS_GROUP}.price_history_last_ts_ms": (
                (source_timestamps.get(TS_FOUNDATION_CHRONOS_GROUP) or {}).get("price_history_last_ts_ms")
            ),
            f"{TS_FOUNDATION_CHRONOS_GROUP}.encoder_artifact_created_ts_ms": (
                (source_timestamps.get(TS_FOUNDATION_CHRONOS_GROUP) or {}).get("encoder_artifact_created_ts_ms")
            ),
            f"{GRAPH_RELATIONAL_GROUP}.max_source_ts_ms": (
                (source_timestamps.get(GRAPH_RELATIONAL_GROUP) or {}).get("max_source_ts_ms")
            ),
            f"{GRAPH_RELATIONAL_GROUP}.max_availability_ts_ms": (
                (source_timestamps.get(GRAPH_RELATIONAL_GROUP) or {}).get("max_availability_ts_ms")
            ),
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

        for group in groups:
            detail = evaluate_group_policy(
                group=str(group),
                source_meta=dict(source_timestamps.get(str(group)) or {}),
                anchor_ts_ms=int(anchor_ts_ms),
                available=bool(availability.get(str(group))),
            )
            if bool(detail.get("ok", True)):
                continue
            pit_policy_violations += 1
            _record_validation_violation(
                violations,
                symbol=symbol,
                anchor_ts_ms=int(anchor_ts_ms),
                group=str(group),
                field="pit_policy:" + ",".join(str(code) for code in list(detail.get("reason_codes") or [])),
                value=detail.get("availability_ts_ms") or detail.get("source_ts_ms"),
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
        "pit_policy_violations": int(pit_policy_violations),
        "ok": bool(lookahead_violations == 0 and pit_policy_violations == 0),
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


def _load_tech_group(con, *, symbol: str, ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    features = {fid: 0.0 for fid in TECH_FEATURE_IDS}
    source_meta: Dict[str, Any] = {
        "har_forecast_ts_ms": None,
        "har_forecast_asof_ts_ms": None,
        "har_forecast_source": None,
    }
    try:
        from engine.strategy.har_rv import latest_har_forecast

        row = latest_har_forecast(con, str(symbol), ts_ms=int(ts_ms))
    except Exception as exc:
        _warn_nonfatal(
            "model_feature_snapshots_tech_group_failed",
            "MODEL_FEATURE_SNAPSHOTS_TECH_GROUP_FAILED",
            exc,
            warn_key="model_feature_snapshots_tech_group_failed",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        row = None
    if not row:
        return features, source_meta, False

    features["tech.har_rv_forecast_1d"] = float(_safe_float(row.get("forecast_vol_1d"), 0.0))
    features["tech.har_rv_forecast_ratio"] = float(_safe_float(row.get("forecast_ratio"), 0.0))
    source_meta["har_forecast_ts_ms"] = int(row.get("ts_ms") or 0)
    source_meta["har_forecast_asof_ts_ms"] = row.get("asof_ts_ms")
    source_meta["har_forecast_source"] = str(row.get("source") or "")
    return features, source_meta, True


def _load_event_group(con, *, symbol: str, ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    features = {fid: 0.0 for fid in EVENT_FEATURE_IDS}
    cutoff_24h = int(ts_ms) - int(DEFAULT_EVENT_LOOKBACK_HOURS) * 3600 * 1000
    rows = con.execute(
        """
        SELECT
          e.ts_ms,
          e.importance_score,
          n.sentiment_score,
          COALESCE(n.embedding_novelty_score, n.novelty_score),
          COALESCE(n.stale_flag, n.is_duplicate)
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
    sentiments_now: List[Tuple[float, float]] = []
    sentiments_prev: List[Tuple[float, float]] = []
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
            novelty_value = _safe_float(novelty_score, 0.0)
            novelty_6h.append(novelty_value)
            dupes_6h += 1 if _safe_int(is_duplicate, 0) else 0
            weight = max(0.05, min(1.0, novelty_value))
            if age_ms <= 3 * 3600_000:
                sentiments_now.append((_safe_float(sentiment_score, 0.0), weight))
            else:
                sentiments_prev.append((_safe_float(sentiment_score, 0.0), weight))

    features["events.count_1h"] = float(count_1h)
    features["events.count_6h"] = float(count_6h)
    features["events.count_24h"] = float(len(rows))
    features["events.velocity_6h"] = float(velocity_6h)
    features["events.sentiment_trend_6h"] = float(
        (
            sum(value * weight for value, weight in sentiments_now) / max(1e-9, sum(weight for _value, weight in sentiments_now))
            if sentiments_now
            else 0.0
        )
        - (
            sum(value * weight for value, weight in sentiments_prev) / max(1e-9, sum(weight for _value, weight in sentiments_prev))
            if sentiments_prev
            else 0.0
        )
    )
    features["events.avg_novelty_6h"] = float(sum(novelty_6h) / len(novelty_6h)) if novelty_6h else 0.0
    features["events.duplicate_share_6h"] = float(dupes_6h / max(1, count_6h)) if count_6h > 0 else 0.0
    features["events.importance_mean_24h"] = (
        float(sum(importance_24h) / len(importance_24h)) if importance_24h else 0.0
    )
    features["events.hours_since_last"] = float(max(0, int(ts_ms) - int(latest_event_ts_ms)) / 3_600_000.0)
    return features, {
        "latest_event_ts_ms": int(latest_event_ts_ms),
        "latest_event_availability_ts_ms": int(latest_event_ts_ms),
        "window_start_ts_ms": int(cutoff_24h),
    }, True


def _storage_row_dict(row: Any) -> Dict[str, Any]:
    if hasattr(row, "keys"):
        return {str(key): row[key] for key in row.keys()}
    return {}


def _insider_direction(row: Dict[str, Any]) -> str:
    code = str(row.get("transaction_code") or "").strip().upper()
    if code == "P":
        return "buy"
    if code == "S":
        return "sell"
    return str(row.get("direction") or "").strip().lower()


def _adv_dollar_volume(con, *, symbol: str, ts_ms: int) -> float:
    window_start = int(ts_ms) - int(30 * 24 * 3600 * 1000)
    try:
        rows = con.execute(
            """
            SELECT last, volume
            FROM price_quotes
            WHERE symbol = ?
              AND ts_ms <= ?
              AND ts_ms >= ?
              AND last IS NOT NULL
              AND volume IS NOT NULL
            ORDER BY ts_ms DESC
            LIMIT 390
            """,
            (str(symbol), int(ts_ms), int(window_start)),
        ).fetchall()
    except Exception:
        rows = []
    values = [
        float(_safe_float(row[0], 0.0) * _safe_float(row[1], 0.0))
        for row in rows or []
        if _safe_float(row[0], 0.0) > 0.0 and _safe_float(row[1], 0.0) > 0.0
    ]
    if not values:
        return 1.0
    return float(max(1.0, sum(values) / max(1, len(values))))


def _insider_opp_sell_z(classified_rows: Sequence[Tuple[Dict[str, Any], str]], *, ts_ms: int, window_start: int) -> float:
    history_start = int(ts_ms) - int(390 * 24 * 3600 * 1000)
    current_sell_value = 0.0
    daily_history: Dict[int, float] = {}
    for row, label in classified_rows:
        if label != OPPORTUNISTIC or _insider_direction(row) != "sell":
            continue
        avail = int(_insider_availability_ts_ms(row) or 0)
        if avail <= 0 or avail > int(ts_ms):
            continue
        value = float(_insider_transaction_value(row))
        if value <= 0.0:
            continue
        if avail >= int(window_start):
            current_sell_value += float(value)
        elif avail >= int(history_start):
            bucket = int(avail // 86_400_000)
            daily_history[bucket] = float(daily_history.get(bucket, 0.0) + value)
    values = list(daily_history.values())
    if len(values) < 2:
        return 0.0
    mean = float(sum(values) / len(values))
    variance = float(sum((value - mean) ** 2 for value in values) / max(1, len(values) - 1))
    std = math.sqrt(max(0.0, variance))
    if std <= 1e-9:
        return 0.0
    return float(max(-10.0, min(10.0, (current_sell_value - mean) / std)))


def _load_insider_group(con, *, symbol: str, ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    features = {fid: 0.0 for fid in INSIDER_FEATURE_IDS}
    if not INSIDER_FEATURE_IDS:
        return features, {"latest_availability_ts_ms": None, "window_start_ts_ms": None}, False

    window_30d_start = int(ts_ms) - int(30 * 24 * 3600 * 1000)
    window_5d_start = int(ts_ms) - int(5 * 24 * 3600 * 1000)
    query_start = int(ts_ms) - int(DEFAULT_INSIDER_LOOKBACK_DAYS * 24 * 3600 * 1000)
    try:
        rows = con.execute(
            """
            SELECT
              id,
              source_transaction_id,
              COALESCE(availability_ts_ms, filing_ts_ms, ingested_ts_ms, created_ts_ms) AS availability_ts_ms,
              transaction_ts_ms,
              transaction_date,
              direction,
              transaction_code,
              transaction_type,
              security_type,
              value,
              shares,
              price,
              insider_cik,
              insider_name,
              insider_role,
              insider_title,
              is_10b5_1_plan,
              payload_json,
              diagnostics_json
            FROM insider_transactions
            WHERE symbol = ?
              AND COALESCE(availability_ts_ms, filing_ts_ms, ingested_ts_ms, created_ts_ms, 0) <= ?
              AND COALESCE(availability_ts_ms, filing_ts_ms, ingested_ts_ms, created_ts_ms, 0) >= ?
            ORDER BY COALESCE(availability_ts_ms, filing_ts_ms, ingested_ts_ms, created_ts_ms, 0) ASC, id ASC
            """,
            (str(symbol), int(ts_ms), int(query_start)),
        ).fetchall()
    except Exception:
        rows = []

    if not rows:
        return features, {"latest_availability_ts_ms": None, "window_start_ts_ms": int(query_start)}, False

    row_dicts = [_storage_row_dict(row) for row in rows or []]
    row_dicts = [row for row in row_dicts if row]
    classified_rows: List[Tuple[Dict[str, Any], str]] = [
        (row, classify_insider_trade(row, row_dicts, asof_ts_ms=int(ts_ms)))
        for row in row_dicts
    ]

    net_value_30d = 0.0
    buy_count_30d = 0
    cluster_buyers_5d: set[str] = set()
    officer_buy_weight = 0.0
    latest_availability = max([int(_insider_availability_ts_ms(row) or 0) for row in row_dicts] or [0])
    latest_transaction_ts = max([_safe_int(row.get("transaction_ts_ms"), 0) for row in row_dicts] or [0])
    adv_dollars = _adv_dollar_volume(con, symbol=str(symbol), ts_ms=int(ts_ms))

    for row, label in classified_rows:
        if label != OPPORTUNISTIC:
            continue
        avail = int(_insider_availability_ts_ms(row) or 0)
        if avail <= 0 or avail > int(ts_ms) or avail < int(window_30d_start):
            continue
        direction_key = _insider_direction(row)
        value_num = float(_insider_transaction_value(row))
        if direction_key == "buy":
            buy_count_30d += 1
            net_value_30d += float(value_num)
            officer_buy_weight = max(float(officer_buy_weight), float(_insider_role_buy_weight(row)))
            if avail >= int(window_5d_start):
                key = _insider_key(row)
                if key:
                    cluster_buyers_5d.add(key)
        elif direction_key == "sell":
            net_value_30d -= float(value_num)

    features["insider_opp_net_buy_30d"] = float(net_value_30d / max(1.0, adv_dollars))
    features["insider_opp_buy_count_30d"] = float(buy_count_30d)
    features["insider_cluster_buy_5d"] = float(len(cluster_buyers_5d))
    features["insider_officer_buy_flag"] = float(officer_buy_weight)
    features["insider_opp_sell_z"] = float(
        _insider_opp_sell_z(classified_rows, ts_ms=int(ts_ms), window_start=int(window_30d_start))
    )
    return (
        features,
        {
            "latest_availability_ts_ms": int(latest_availability) if latest_availability > 0 else None,
            "latest_transaction_ts_ms": int(latest_transaction_ts) if latest_transaction_ts > 0 else None,
            "window_start_ts_ms": int(query_start),
            "normalizer": "adv_dollar_volume_30d",
            "normalizer_value": float(adv_dollars),
        },
        True,
    )


def _earnings_window_proximity(con, *, symbol: str, ts_ms: int) -> float:
    anchor_day = _finra_asof_date(int(ts_ms))
    try:
        rows = con.execute(
            """
            SELECT earnings_date, updated_ts_ms
            FROM earnings_calendar
            WHERE symbol = ?
              AND COALESCE(updated_ts_ms, 0) <= ?
            """,
            (str(symbol), int(ts_ms)),
        ).fetchall()
    except Exception:
        return 0.0
    nearest_days: int | None = None
    for row in rows or []:
        try:
            earnings_day = _parse_finra_date(row[0])
        except Exception:
            continue
        distance = abs((earnings_day - anchor_day).days)
        if nearest_days is None or distance < nearest_days:
            nearest_days = int(distance)
    if nearest_days is None or nearest_days > 5:
        return 0.0
    return float(max(0.0, min(1.0, 1.0 - (float(nearest_days) / 5.0))))


def _load_short_group(con, *, symbol: str, ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    features = {fid: 0.0 for fid in SHORT_FEATURE_IDS}
    if not SHORT_FEATURE_IDS:
        return (
            features,
            {
                "latest_short_volume_availability_ts_ms": None,
                "latest_short_interest_availability_ts_ms": None,
            },
            False,
        )

    anchor_day = _finra_asof_date(int(ts_ms)).isoformat()
    volume_start = int(ts_ms) - int(DEFAULT_SHORT_VOLUME_LOOKBACK_DAYS * 24 * 3600 * 1000)
    try:
        volume_rows = con.execute(
            """
            SELECT
              trade_date,
              trade_ts_ms,
              availability_ts_ms,
              short_volume,
              short_exempt_volume,
              total_volume,
              market
            FROM finra_short_sale_volume
            WHERE symbol = ?
              AND availability_ts_ms <= ?
              AND availability_ts_ms >= ?
              AND trade_date < ?
            ORDER BY trade_ts_ms DESC, availability_ts_ms DESC, id DESC
            LIMIT 64
            """,
            (str(symbol), int(ts_ms), int(volume_start), str(anchor_day)),
        ).fetchall()
    except Exception:
        volume_rows = []
    volume_dicts = [_storage_row_dict(row) for row in volume_rows or []]
    volume_dicts = [row for row in volume_dicts if row]
    features["short_vol_ratio_z20"] = float(_short_volume_ratio_z(volume_dicts, lookback=20))
    latest_volume_availability = max([_safe_int(row.get("availability_ts_ms"), 0) for row in volume_dicts] or [0])
    latest_volume_trade_ts = max([_safe_int(row.get("trade_ts_ms"), 0) for row in volume_dicts] or [0])
    latest_volume_trade_date = None
    if volume_dicts:
        latest_volume_trade_date = str(
            max(
                volume_dicts,
                key=lambda row: _safe_int(row.get("trade_ts_ms") or row.get("availability_ts_ms"), 0),
            ).get("trade_date")
            or ""
        )

    try:
        si_rows = con.execute(
            """
            SELECT
              settlement_date,
              settlement_ts_ms,
              dissemination_date,
              dissemination_ts_ms,
              availability_ts_ms,
              short_interest_shares,
              days_to_cover
            FROM finra_short_interest
            WHERE symbol = ?
              AND availability_ts_ms <= ?
            ORDER BY availability_ts_ms DESC, settlement_ts_ms DESC, id DESC
            LIMIT ?
            """,
            (str(symbol), int(ts_ms), int(DEFAULT_SHORT_INTEREST_LOOKBACK_READINGS)),
        ).fetchall()
    except Exception:
        si_rows = []
    si_dicts = [_storage_row_dict(row) for row in si_rows or []]
    si_dicts = [row for row in si_dicts if row]
    surprise, dtc_delta = _short_interest_surprise(
        si_dicts,
        alpha=float(DEFAULT_SHORT_SI_EWMA_ALPHA),
        shares_normalizer=None,
    )
    features["si_surprise"] = float(max(-10.0, min(10.0, surprise)))
    features["days_to_cover_delta"] = float(max(-10.0, min(10.0, dtc_delta)))
    earnings_window = _earnings_window_proximity(con, symbol=str(symbol), ts_ms=int(ts_ms))
    features["si_surprise_x_earnings_window"] = float(features["si_surprise"] * float(earnings_window))
    latest_si_availability = max([_safe_int(row.get("availability_ts_ms"), 0) for row in si_dicts] or [0])
    latest_si_settlement_ts = max([_safe_int(row.get("settlement_ts_ms"), 0) for row in si_dicts] or [0])
    latest_si_settlement_date = None
    if si_dicts:
        latest_si_settlement_date = str(
            max(
                si_dicts,
                key=lambda row: _safe_int(row.get("settlement_ts_ms") or row.get("availability_ts_ms"), 0),
            ).get("settlement_date")
            or ""
        )

    return (
        features,
        {
            "latest_short_volume_availability_ts_ms": int(latest_volume_availability) if latest_volume_availability > 0 else None,
            "latest_short_volume_trade_ts_ms": int(latest_volume_trade_ts) if latest_volume_trade_ts > 0 else None,
            "latest_short_volume_trade_date": latest_volume_trade_date or None,
            "latest_short_interest_availability_ts_ms": int(latest_si_availability) if latest_si_availability > 0 else None,
            "latest_short_interest_settlement_ts_ms": int(latest_si_settlement_ts) if latest_si_settlement_ts > 0 else None,
            "latest_short_interest_settlement_date": latest_si_settlement_date or None,
            "short_volume_window_start_ts_ms": int(volume_start),
            "short_interest_readings": int(len(si_dicts)),
            "earnings_window_proximity": float(earnings_window),
            "si_surprise_normalizer": "trailing_short_interest_std",
        },
        bool(volume_dicts or si_dicts),
    )


def _load_crypto_positioning_group(con, *, symbol: str, ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    features = {fid: 0.0 for fid in CRYPTO_POSITIONING_FEATURE_IDS}
    meta = {
        "latest_availability_ts_ms": None,
        "latest_funding_ts_ms": None,
        "window_start_ts_ms": None,
    }
    if not CRYPTO_POSITIONING_FEATURE_IDS:
        return features, meta, False
    if str(asset_class_for_symbol(symbol)).upper() != "CRYPTO":
        return features, meta, False

    window_start = int(ts_ms) - int(DEFAULT_CRYPTO_POSITIONING_LOOKBACK_DAYS * 24 * 3600 * 1000)
    try:
        rows = con.execute(
            """
            SELECT
              exchange,
              perp_market,
              spot_market,
              funding_ts_ms,
              availability_ts_ms,
              funding_rate,
              perp_basis_pct,
              mark_price,
              spot_price,
              is_live
            FROM crypto_funding_rates
            WHERE symbol = ?
              AND availability_ts_ms <= ?
              AND availability_ts_ms >= ?
            ORDER BY funding_ts_ms ASC, availability_ts_ms ASC, id ASC
            """,
            (str(symbol), int(ts_ms), int(window_start)),
        ).fetchall()
    except Exception:
        rows = []

    row_dicts = [_storage_row_dict(row) for row in rows or []]
    row_dicts = [row for row in row_dicts if row]
    if not row_dicts:
        meta["window_start_ts_ms"] = int(window_start)
        return features, meta, False

    computed = _compute_crypto_positioning_features(row_dicts, asof_ts_ms=int(ts_ms))
    for fid in CRYPTO_POSITIONING_FEATURE_IDS:
        features[fid] = float(_safe_float(computed.get(fid), 0.0))
    latest = max(row_dicts, key=lambda row: _safe_int(row.get("availability_ts_ms"), 0))
    return (
        features,
        {
            "latest_availability_ts_ms": _safe_int(latest.get("availability_ts_ms"), 0) or None,
            "latest_funding_ts_ms": _safe_int(latest.get("funding_ts_ms"), 0) or None,
            "window_start_ts_ms": int(window_start),
            "exchange": str(latest.get("exchange") or ""),
            "perp_market": str(latest.get("perp_market") or ""),
            "basis_available": 1 if any(_safe_float(row.get("perp_basis_pct")) is not None for row in row_dicts) else 0,
            "rows": int(len(row_dicts)),
        },
        True,
    )


def _load_deribit_crypto_derivatives_group(
    con,
    *,
    symbol: str,
    ts_ms: int,
    feature_ids: Optional[Sequence[str]] = None,
) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    requested = [
        str(fid)
        for fid in list(feature_ids or DERIBIT_FEATURE_IDS)
        if str(fid or "").startswith(DERIBIT_FEATURE_PREFIX)
    ]
    features = {fid: 0.0 for fid in list(requested or DERIBIT_FEATURE_IDS)}
    try:
        resolved, meta, available = resolve_deribit_crypto_derivatives_snapshot(
            con,
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
    except Exception as exc:
        _warn_nonfatal(
            "model_feature_snapshots_deribit_crypto_derivatives_group_failed",
            "MODEL_FEATURE_SNAPSHOTS_DERIBIT_CRYPTO_DERIVATIVES_GROUP_FAILED",
            exc,
            warn_key="model_feature_snapshots_deribit_crypto_derivatives_group_failed",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        return features, {"latest_source_ts_ms": None, "latest_availability_ts_ms": None, "status": "error"}, False
    for fid in features:
        features[fid] = float(_safe_float((resolved or {}).get(fid), 0.0))
    return features, dict(meta or {}), bool(available)


def _load_sportsbook_odds_group(
    con,
    *,
    symbol: str,
    ts_ms: int,
    feature_ids: Optional[Sequence[str]] = None,
) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    requested = [
        str(fid)
        for fid in list(feature_ids or SPORTSBOOK_ODDS_FEATURE_IDS)
        if str(fid or "").startswith(SPORTSBOOK_ODDS_FEATURE_PREFIX)
    ]
    features = {fid: 0.0 for fid in list(requested or SPORTSBOOK_ODDS_FEATURE_IDS)}
    try:
        resolved, meta, available = resolve_sportsbook_odds_snapshot(
            con,
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
    except Exception as exc:
        _warn_nonfatal(
            "model_feature_snapshots_sportsbook_odds_group_failed",
            "MODEL_FEATURE_SNAPSHOTS_SPORTSBOOK_ODDS_GROUP_FAILED",
            exc,
            warn_key="model_feature_snapshots_sportsbook_odds_group_failed",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        return features, {"latest_source_ts_ms": None, "latest_availability_ts_ms": None, "status": "error"}, False
    for fid in features:
        features[fid] = float(_safe_float((resolved or {}).get(fid), 0.0))
    return features, dict(meta or {}), bool(available)


def _load_news_flow_group(con, *, symbol: str, ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    features = {fid: 0.0 for fid in NEWS_FLOW_FEATURE_IDS}
    if not NEWS_FLOW_FEATURE_IDS:
        return features, {"latest_availability_ts_ms": None}, False
    try:
        from engine.data.news_flow import resolve_news_flow_features

        resolved, meta, available = resolve_news_flow_features(con, symbol=str(symbol), ts_ms=int(ts_ms))
    except Exception as exc:
        _warn_nonfatal(
            "model_feature_snapshots_news_flow_group_failed",
            "MODEL_FEATURE_SNAPSHOTS_NEWS_FLOW_GROUP_FAILED",
            exc,
            warn_key="model_feature_snapshots_news_flow_group_failed",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        return features, {"latest_availability_ts_ms": None}, False
    for fid in NEWS_FLOW_FEATURE_IDS:
        features[fid] = float(_safe_float((resolved or {}).get(fid), 0.0))
    return features, dict(meta or {}), bool(available)


def _load_structured_doc_events_group(con, *, symbol: str, ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    features = {fid: 0.0 for fid in STRUCTURED_DOCUMENT_EVENT_FEATURE_IDS}
    if not STRUCTURED_DOCUMENT_EVENT_FEATURE_IDS:
        return features, {"latest_availability_ts_ms": None, "latest_event_ts_ms": None}, False
    try:
        from engine.data.structured_document_events import resolve_structured_document_event_features

        resolved, meta, available = resolve_structured_document_event_features(
            con,
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
    except Exception as exc:
        _warn_nonfatal(
            "model_feature_snapshots_structured_doc_events_group_failed",
            "MODEL_FEATURE_SNAPSHOTS_STRUCTURED_DOC_EVENTS_GROUP_FAILED",
            exc,
            warn_key="model_feature_snapshots_structured_doc_events_group_failed",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        return features, {"latest_availability_ts_ms": None, "latest_event_ts_ms": None}, False
    for fid in STRUCTURED_DOCUMENT_EVENT_FEATURE_IDS:
        features[fid] = float(_safe_float((resolved or {}).get(fid), 0.0))
    return features, dict(meta or {}), bool(available)


def _load_etf_flow_group(con, *, symbol: str, ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    features = {fid: 0.0 for fid in ETF_FLOW_FEATURE_IDS}
    if not ETF_FLOW_FEATURE_IDS:
        return features, {"latest_availability_ts_ms": None, "latest_asof_date": None}, False
    try:
        from engine.data.etf_flows import resolve_etf_flow_features

        resolved, meta, available = resolve_etf_flow_features(con, symbol=str(symbol), ts_ms=int(ts_ms))
    except Exception as exc:
        _warn_nonfatal(
            "model_feature_snapshots_etf_flow_group_failed",
            "MODEL_FEATURE_SNAPSHOTS_ETF_FLOW_GROUP_FAILED",
            exc,
            warn_key="model_feature_snapshots_etf_flow_group_failed",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        return features, {"latest_availability_ts_ms": None, "latest_asof_date": None}, False
    for fid in ETF_FLOW_FEATURE_IDS:
        features[fid] = float(_safe_float((resolved or {}).get(fid), 0.0))
    return features, dict(meta or {}), bool(available)


def _load_cot_group(con, *, symbol: str, ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    features = {fid: 0.0 for fid in COT_FEATURE_IDS}
    if not COT_FEATURE_IDS:
        return features, {"latest_availability_ts_ms": None, "contracts": []}, False
    try:
        from engine.data.cftc_cot import resolve_cot_features

        resolved, meta, available = resolve_cot_features(con, symbol=str(symbol), ts_ms=int(ts_ms))
    except Exception as exc:
        _warn_nonfatal(
            "model_feature_snapshots_cot_group_failed",
            "MODEL_FEATURE_SNAPSHOTS_COT_GROUP_FAILED",
            exc,
            warn_key="model_feature_snapshots_cot_group_failed",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        return features, {"latest_availability_ts_ms": None, "contracts": []}, False
    for fid in COT_FEATURE_IDS:
        features[fid] = float(_safe_float((resolved or {}).get(fid), 0.0))
    return features, dict(meta or {}), bool(available)


def _load_inst_13f_group(con, *, symbol: str, ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    features = {fid: 0.0 for fid in INST_13F_FEATURE_IDS}
    if not INST_13F_FEATURE_IDS:
        return features, {"latest_availability_ts_ms": None, "holding_managers": []}, False
    try:
        from engine.data.inst_13f import resolve_13f_features

        resolved, meta, available = resolve_13f_features(con, symbol=str(symbol), ts_ms=int(ts_ms))
    except Exception as exc:
        _warn_nonfatal(
            "model_feature_snapshots_inst_13f_group_failed",
            "MODEL_FEATURE_SNAPSHOTS_INST_13F_GROUP_FAILED",
            exc,
            warn_key="model_feature_snapshots_inst_13f_group_failed",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        return features, {"latest_availability_ts_ms": None, "holding_managers": []}, False
    for fid in INST_13F_FEATURE_IDS:
        features[fid] = float(_safe_float((resolved or {}).get(fid), 0.0))
    return features, dict(meta or {}), bool(available)


def _load_gov_group(con, *, symbol: str, ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    features = {fid: 0.0 for fid in GOV_FEATURE_IDS}
    if not GOV_FEATURE_IDS:
        return features, {"latest_availability_ts_ms": None, "sector": ""}, False
    try:
        from engine.data.quiver_gov import resolve_gov_features

        resolved, meta, available = resolve_gov_features(con, symbol=str(symbol), ts_ms=int(ts_ms))
    except Exception as exc:
        _warn_nonfatal(
            "model_feature_snapshots_gov_group_failed",
            "MODEL_FEATURE_SNAPSHOTS_GOV_GROUP_FAILED",
            exc,
            warn_key="model_feature_snapshots_gov_group_failed",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        return features, {"latest_availability_ts_ms": None, "sector": ""}, False
    for fid in GOV_FEATURE_IDS:
        features[fid] = float(_safe_float((resolved or {}).get(fid), 0.0))
    return features, dict(meta or {}), bool(available)


def _load_fundamentals_group(con, *, symbol: str, ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    features = {fid: 0.0 for fid in FUNDAMENTALS_PIT_FEATURE_IDS}
    if not FUNDAMENTALS_PIT_FEATURE_IDS:
        return features, {"latest_publish_ts_ms": None, "mode": "disabled"}, False
    try:
        from engine.data.fundamentals_pit import resolve_fundamentals_features

        resolved, meta, available = resolve_fundamentals_features(con, symbol=str(symbol), ts_ms=int(ts_ms))
    except Exception as exc:
        _warn_nonfatal(
            "model_feature_snapshots_fundamentals_group_failed",
            "MODEL_FEATURE_SNAPSHOTS_FUNDAMENTALS_GROUP_FAILED",
            exc,
            warn_key="model_feature_snapshots_fundamentals_group_failed",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        return features, {"latest_publish_ts_ms": None, "mode": "pit"}, False
    for fid in FUNDAMENTALS_PIT_FEATURE_IDS:
        features[fid] = float(_safe_float((resolved or {}).get(fid), 0.0))
    return features, dict(meta or {}), bool(available)


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
              COALESCE(disclosure_ts_ms, ingested_ts_ms, created_ts_ms, transaction_ts_ms) AS availability_ts_ms,
              transaction_ts_ms,
              direction
            FROM congressional_trades
            WHERE symbol = ?
              AND COALESCE(disclosure_ts_ms, ingested_ts_ms, created_ts_ms, transaction_ts_ms) <= ?
              AND COALESCE(disclosure_ts_ms, ingested_ts_ms, created_ts_ms, transaction_ts_ms) >= ?
            ORDER BY COALESCE(disclosure_ts_ms, ingested_ts_ms, created_ts_ms, transaction_ts_ms) DESC, id DESC
            """,
            (str(symbol), int(ts_ms), int(query_start)),
        ).fetchall()
    except Exception:
        rows = []

    if not rows:
        return features, {"latest_trade_ts_ms": None, "window_start_ts_ms": int(query_start)}, False

    buy_count_30d = 0
    sell_count_30d = 0
    latest_availability_ts_ms = _safe_int(rows[0][0], 0)
    latest_transaction_ts_ms = max([_safe_int(row[1], 0) for row in rows or []] or [0])
    for availability_ts_ms, _transaction_ts_ms, direction in rows or []:
        availability_ts_ms = _safe_int(availability_ts_ms, 0)
        if availability_ts_ms < int(window_30d_start):
            continue
        direction_key = str(direction or "").strip().lower()
        if direction_key == "buy":
            buy_count_30d += 1
        elif direction_key == "sell":
            sell_count_30d += 1

    features["congressional.buy_count_30d"] = float(buy_count_30d)
    features["congressional.sell_count_30d"] = float(sell_count_30d)
    features["congressional.net_signal_30d"] = float(buy_count_30d - sell_count_30d)
    return features, {
        "latest_availability_ts_ms": int(latest_availability_ts_ms),
        "latest_trade_ts_ms": int(latest_availability_ts_ms),
        "latest_transaction_ts_ms": int(latest_transaction_ts_ms) if latest_transaction_ts_ms > 0 else None,
        "window_start_ts_ms": int(query_start),
    }, True


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
    from engine.data.factor_ingestion import macro_feature_row_asof

    return macro_feature_row_asof(con, feature_id=str(feature_id), ts_ms=int(ts_ms))


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


def _load_prediction_market_macro_group(con, *, symbol: str, ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    return resolve_prediction_market_macro_snapshot(con, symbol=str(symbol), ts_ms=int(ts_ms))


def _load_prediction_market_event_group(con, *, symbol: str, ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    return resolve_prediction_market_event_snapshot(con, symbol=str(symbol), ts_ms=int(ts_ms))


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
              signal_score,
              gex_norm_z,
              gex_sign,
              opt_flow_imbalance_z
            FROM options_symbol_features
            WHERE symbol = ?
              AND bucket_sec = ?
              AND bucket_ts_ms <= ?
              AND snapshot_ts_ms <= ?
            ORDER BY bucket_ts_ms DESC, snapshot_ts_ms DESC
            LIMIT 1
            """,
            (symbol_key, int(bucket_sec), int(_bucket_start(ts_ms, bucket_sec)), int(ts_ms)),
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
    features["options_symbol.gex_norm_z"] = float(_safe_float(row[10], 0.0))
    features["options_symbol.gex_sign"] = float(_safe_float(row[11], 0.0))
    features["options_symbol.opt_flow_imbalance_z"] = float(_safe_float(row[12], 0.0))
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


def _load_bocpd_group(con, *, symbol: str, ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    features = {fid: 0.0 for fid in BOCPD_FEATURE_IDS}
    try:
        from engine.strategy.bocpd import feature_map_from_summary, load_latest_summary

        summary = load_latest_summary(
            con,
            symbol=str(symbol),
            series_type="realized_vol",
            as_of_ts_ms=int(ts_ms),
        )
        if not summary:
            summary = load_latest_summary(
                con,
                symbol="*",
                series_type="portfolio_correlation",
                as_of_ts_ms=int(ts_ms),
            )
        if not summary:
            return features, {"summary_ts_ms": None, "series_key": None, "series_type": None}, False
        features.update(feature_map_from_summary(summary))
        return features, {
            "summary_ts_ms": _safe_int(summary.get("ts_ms"), 0) or None,
            "series_key": str(summary.get("series_key") or ""),
            "series_type": str(summary.get("series_type") or ""),
        }, True
    except Exception as exc:
        _warn_nonfatal(
            "model_feature_snapshots_bocpd_group_failed",
            "MODEL_FEATURE_SNAPSHOTS_BOCPD_GROUP_FAILED",
            exc,
            warn_key="model_feature_snapshots_bocpd_group_failed",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        return features, {"summary_ts_ms": None, "series_key": None, "series_type": None}, False


def _load_ts_foundation_chronos_group(
    con,
    *,
    symbol: str,
    ts_ms: int,
    feature_ids: Optional[Sequence[str]] = None,
) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    features = {fid: 0.0 for fid in TS_FOUNDATION_CHRONOS_FEATURE_IDS}
    requested = [
        str(fid)
        for fid in list(feature_ids or TS_FOUNDATION_CHRONOS_FEATURE_IDS)
        if str(fid or "").strip()
    ]
    if requested:
        features = {fid: 0.0 for fid in requested}
    try:
        resolved, meta, available = resolve_chronos_foundation_features(
            con,
            symbol=str(symbol),
            ts_ms=int(ts_ms),
            feature_ids=list(requested or TS_FOUNDATION_CHRONOS_FEATURE_IDS),
        )
    except Exception as exc:
        _warn_nonfatal(
            "model_feature_snapshots_ts_foundation_chronos_group_failed",
            "MODEL_FEATURE_SNAPSHOTS_TS_FOUNDATION_CHRONOS_GROUP_FAILED",
            exc,
            warn_key="model_feature_snapshots_ts_foundation_chronos_group_failed",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        return features, {"status": "error", "price_history_last_ts_ms": None}, False
    for fid in features:
        features[fid] = float(_safe_float((resolved or {}).get(fid), 0.0))
    return features, dict(meta or {}), bool(available)


def _load_graph_relational_group(
    con,
    *,
    symbol: str,
    ts_ms: int,
    feature_ids: Optional[Sequence[str]] = None,
) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    requested = [
        str(fid)
        for fid in list(feature_ids or GRAPH_RELATIONAL_FEATURE_IDS)
        if str(fid or "").startswith(GRAPH_RELATIONAL_PREFIX)
    ]
    features = {fid: 0.0 for fid in list(requested or GRAPH_RELATIONAL_FEATURE_IDS)}
    try:
        snapshot = build_graph_relational_snapshot(
            symbol=str(symbol),
            ts_ms=int(ts_ms),
            feature_ids=list(requested or GRAPH_RELATIONAL_FEATURE_IDS),
            con=con,
        )
    except Exception as exc:
        _warn_nonfatal(
            "model_feature_snapshots_graph_relational_group_failed",
            "MODEL_FEATURE_SNAPSHOTS_GRAPH_RELATIONAL_GROUP_FAILED",
            exc,
            warn_key="model_feature_snapshots_graph_relational_group_failed",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        return features, {"status": "error", "max_source_ts_ms": None, "max_availability_ts_ms": None}, False

    resolved = dict((snapshot or {}).get("features") or {})
    for fid in features:
        features[fid] = float(_safe_float(resolved.get(fid), 0.0))
    graph_meta = graph_metadata_from_snapshot(snapshot or {})
    source_meta = {
        **dict((snapshot or {}).get("source_timestamps") or {}),
        **dict(graph_meta or {}),
        "edge_counts": dict((snapshot or {}).get("edge_counts") or {}),
        "graph_metadata": dict(graph_meta or {}),
    }
    return features, source_meta, bool((snapshot or {}).get("availability", {}).get(GRAPH_RELATIONAL_GROUP))


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
    graph_feature_ids = [
        str(fid)
        for fid in ids
        if str(fid or "").startswith(GRAPH_RELATIONAL_PREFIX)
    ]
    deribit_feature_ids = [
        str(fid)
        for fid in ids
        if str(fid or "").startswith(DERIBIT_FEATURE_PREFIX)
    ]
    sportsbook_odds_feature_ids = [
        str(fid)
        for fid in ids
        if str(fid or "").startswith(SPORTSBOOK_ODDS_FEATURE_PREFIX)
    ]

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
            tech_features, tech_meta, tech_available = _load_tech_group(con, symbol=symbol_key, ts_ms=anchor_ts_ms)
        except Exception as exc:
            _warn_nonfatal(
                "model_feature_snapshots_tech_group_failed",
                "MODEL_FEATURE_SNAPSHOTS_TECH_GROUP_FAILED",
                exc,
                warn_key="model_feature_snapshots_tech_group_failed_outer",
                symbol=symbol_key,
                ts_ms=int(anchor_ts_ms),
            )
            tech_features, tech_meta, tech_available = (
                {fid: 0.0 for fid in TECH_FEATURE_IDS},
                {"har_forecast_ts_ms": None},
                False,
            )
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
            prediction_market_macro_features, prediction_market_macro_meta, prediction_market_macro_available = (
                _load_prediction_market_macro_group(con, symbol=symbol_key, ts_ms=anchor_ts_ms)
            )
        except Exception as exc:
            _warn_nonfatal(
                "model_feature_snapshots_prediction_market_macro_group_failed",
                "MODEL_FEATURE_SNAPSHOTS_PREDICTION_MARKET_MACRO_GROUP_FAILED",
                exc,
                warn_key="model_feature_snapshots_prediction_market_macro_group_failed",
                symbol=symbol_key,
                ts_ms=int(anchor_ts_ms),
            )
            prediction_market_macro_features, prediction_market_macro_meta, prediction_market_macro_available = (
                {fid: 0.0 for fid in PREDICTION_MARKET_MACRO_FEATURE_IDS},
                {"latest_availability_ts_ms": None, "latest_source_ts_ms": None},
                False,
            )
        try:
            prediction_market_event_features, prediction_market_event_meta, prediction_market_event_available = (
                _load_prediction_market_event_group(con, symbol=symbol_key, ts_ms=anchor_ts_ms)
            )
        except Exception as exc:
            _warn_nonfatal(
                "model_feature_snapshots_prediction_market_event_group_failed",
                "MODEL_FEATURE_SNAPSHOTS_PREDICTION_MARKET_EVENT_GROUP_FAILED",
                exc,
                warn_key="model_feature_snapshots_prediction_market_event_group_failed",
                symbol=symbol_key,
                ts_ms=int(anchor_ts_ms),
            )
            prediction_market_event_features, prediction_market_event_meta, prediction_market_event_available = (
                {fid: 0.0 for fid in PREDICTION_MARKET_EVENT_FEATURE_IDS},
                {"latest_availability_ts_ms": None, "latest_source_ts_ms": None},
                False,
            )
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
            insider_features, insider_meta, insider_available = _load_insider_group(con, symbol=symbol_key, ts_ms=anchor_ts_ms)
        except Exception as exc:
            _warn_nonfatal(
                "model_feature_snapshots_insider_group_failed",
                "MODEL_FEATURE_SNAPSHOTS_INSIDER_GROUP_FAILED",
                exc,
                warn_key="model_feature_snapshots_insider_group_failed",
                symbol=symbol_key,
                ts_ms=int(anchor_ts_ms),
            )
            insider_features, insider_meta, insider_available = (
                {fid: 0.0 for fid in INSIDER_FEATURE_IDS},
                {"latest_availability_ts_ms": None},
                False,
            )
        try:
            short_features, short_meta, short_available = _load_short_group(con, symbol=symbol_key, ts_ms=anchor_ts_ms)
        except Exception as exc:
            _warn_nonfatal(
                "model_feature_snapshots_short_group_failed",
                "MODEL_FEATURE_SNAPSHOTS_SHORT_GROUP_FAILED",
                exc,
                warn_key="model_feature_snapshots_short_group_failed",
                symbol=symbol_key,
                ts_ms=int(anchor_ts_ms),
            )
            short_features, short_meta, short_available = (
                {fid: 0.0 for fid in SHORT_FEATURE_IDS},
                {
                    "latest_short_volume_availability_ts_ms": None,
                    "latest_short_interest_availability_ts_ms": None,
                },
                False,
            )
        try:
            crypto_positioning_features, crypto_positioning_meta, crypto_positioning_available = _load_crypto_positioning_group(
                con,
                symbol=symbol_key,
                ts_ms=anchor_ts_ms,
            )
        except Exception as exc:
            _warn_nonfatal(
                "model_feature_snapshots_crypto_positioning_group_failed",
                "MODEL_FEATURE_SNAPSHOTS_CRYPTO_POSITIONING_GROUP_FAILED",
                exc,
                warn_key="model_feature_snapshots_crypto_positioning_group_failed",
                symbol=symbol_key,
                ts_ms=int(anchor_ts_ms),
            )
            crypto_positioning_features, crypto_positioning_meta, crypto_positioning_available = (
                {fid: 0.0 for fid in CRYPTO_POSITIONING_FEATURE_IDS},
                {"latest_availability_ts_ms": None, "latest_funding_ts_ms": None},
                False,
            )
        deribit_features: Dict[str, float] = {}
        deribit_meta: Dict[str, Any] = {"latest_availability_ts_ms": None, "latest_source_ts_ms": None, "status": "not_requested"}
        deribit_available = False
        if deribit_feature_ids:
            try:
                deribit_features, deribit_meta, deribit_available = _load_deribit_crypto_derivatives_group(
                    con,
                    symbol=symbol_key,
                    ts_ms=anchor_ts_ms,
                    feature_ids=list(deribit_feature_ids),
                )
            except Exception as exc:
                _warn_nonfatal(
                    "model_feature_snapshots_deribit_crypto_derivatives_group_failed",
                    "MODEL_FEATURE_SNAPSHOTS_DERIBIT_CRYPTO_DERIVATIVES_GROUP_FAILED",
                    exc,
                    warn_key="model_feature_snapshots_deribit_crypto_derivatives_group_failed_outer",
                    symbol=symbol_key,
                    ts_ms=int(anchor_ts_ms),
                )
                deribit_features, deribit_meta, deribit_available = (
                    {fid: 0.0 for fid in deribit_feature_ids},
                    {"latest_availability_ts_ms": None, "latest_source_ts_ms": None, "status": "error"},
                    False,
                )
        sportsbook_odds_features: Dict[str, float] = {}
        sportsbook_odds_meta: Dict[str, Any] = {
            "latest_availability_ts_ms": None,
            "latest_source_ts_ms": None,
            "status": "not_requested",
            "research_only": True,
            "direct_trading_authority": False,
            "broad_market_default_allowed": False,
        }
        sportsbook_odds_available = False
        if sportsbook_odds_feature_ids:
            try:
                sportsbook_odds_features, sportsbook_odds_meta, sportsbook_odds_available = _load_sportsbook_odds_group(
                    con,
                    symbol=symbol_key,
                    ts_ms=anchor_ts_ms,
                    feature_ids=list(sportsbook_odds_feature_ids),
                )
            except Exception as exc:
                _warn_nonfatal(
                    "model_feature_snapshots_sportsbook_odds_group_failed",
                    "MODEL_FEATURE_SNAPSHOTS_SPORTSBOOK_ODDS_GROUP_FAILED",
                    exc,
                    warn_key="model_feature_snapshots_sportsbook_odds_group_failed_outer",
                    symbol=symbol_key,
                    ts_ms=int(anchor_ts_ms),
                )
                sportsbook_odds_features, sportsbook_odds_meta, sportsbook_odds_available = (
                    {fid: 0.0 for fid in sportsbook_odds_feature_ids},
                    {"latest_availability_ts_ms": None, "latest_source_ts_ms": None, "status": "error"},
                    False,
                )
        try:
            news_flow_features, news_flow_meta, news_flow_available = _load_news_flow_group(
                con,
                symbol=symbol_key,
                ts_ms=anchor_ts_ms,
            )
        except Exception as exc:
            _warn_nonfatal(
                "model_feature_snapshots_news_flow_group_failed",
                "MODEL_FEATURE_SNAPSHOTS_NEWS_FLOW_GROUP_FAILED",
                exc,
                warn_key="model_feature_snapshots_news_flow_group_failed_outer",
                symbol=symbol_key,
                ts_ms=int(anchor_ts_ms),
            )
            news_flow_features, news_flow_meta, news_flow_available = (
                {fid: 0.0 for fid in NEWS_FLOW_FEATURE_IDS},
                {"latest_availability_ts_ms": None},
                False,
            )
        try:
            structured_doc_events_features, structured_doc_events_meta, structured_doc_events_available = (
                _load_structured_doc_events_group(
                    con,
                    symbol=symbol_key,
                    ts_ms=anchor_ts_ms,
                )
            )
        except Exception as exc:
            _warn_nonfatal(
                "model_feature_snapshots_structured_doc_events_group_failed",
                "MODEL_FEATURE_SNAPSHOTS_STRUCTURED_DOC_EVENTS_GROUP_FAILED",
                exc,
                warn_key="model_feature_snapshots_structured_doc_events_group_failed_outer",
                symbol=symbol_key,
                ts_ms=int(anchor_ts_ms),
            )
            structured_doc_events_features, structured_doc_events_meta, structured_doc_events_available = (
                {fid: 0.0 for fid in STRUCTURED_DOCUMENT_EVENT_FEATURE_IDS},
                {"latest_availability_ts_ms": None, "latest_event_ts_ms": None},
                False,
            )
        try:
            etf_flow_features, etf_flow_meta, etf_flow_available = _load_etf_flow_group(
                con,
                symbol=symbol_key,
                ts_ms=anchor_ts_ms,
            )
        except Exception as exc:
            _warn_nonfatal(
                "model_feature_snapshots_etf_flow_group_failed",
                "MODEL_FEATURE_SNAPSHOTS_ETF_FLOW_GROUP_FAILED",
                exc,
                warn_key="model_feature_snapshots_etf_flow_group_failed_outer",
                symbol=symbol_key,
                ts_ms=int(anchor_ts_ms),
            )
            etf_flow_features, etf_flow_meta, etf_flow_available = (
                {fid: 0.0 for fid in ETF_FLOW_FEATURE_IDS},
                {"latest_availability_ts_ms": None, "latest_asof_date": None},
                False,
            )
        try:
            cot_features, cot_meta, cot_available = _load_cot_group(
                con,
                symbol=symbol_key,
                ts_ms=anchor_ts_ms,
            )
        except Exception as exc:
            _warn_nonfatal(
                "model_feature_snapshots_cot_group_failed",
                "MODEL_FEATURE_SNAPSHOTS_COT_GROUP_FAILED",
                exc,
                warn_key="model_feature_snapshots_cot_group_failed_outer",
                symbol=symbol_key,
                ts_ms=int(anchor_ts_ms),
            )
            cot_features, cot_meta, cot_available = (
                {fid: 0.0 for fid in COT_FEATURE_IDS},
                {"latest_availability_ts_ms": None, "contracts": []},
                False,
            )
        try:
            inst_13f_features, inst_13f_meta, inst_13f_available = _load_inst_13f_group(
                con,
                symbol=symbol_key,
                ts_ms=anchor_ts_ms,
            )
        except Exception as exc:
            _warn_nonfatal(
                "model_feature_snapshots_inst_13f_group_failed",
                "MODEL_FEATURE_SNAPSHOTS_INST_13F_GROUP_FAILED",
                exc,
                warn_key="model_feature_snapshots_inst_13f_group_failed_outer",
                symbol=symbol_key,
                ts_ms=int(anchor_ts_ms),
            )
            inst_13f_features, inst_13f_meta, inst_13f_available = (
                {fid: 0.0 for fid in INST_13F_FEATURE_IDS},
                {"latest_availability_ts_ms": None, "holding_managers": []},
                False,
            )
        try:
            gov_features, gov_meta, gov_available = _load_gov_group(
                con,
                symbol=symbol_key,
                ts_ms=anchor_ts_ms,
            )
        except Exception as exc:
            _warn_nonfatal(
                "model_feature_snapshots_gov_group_failed",
                "MODEL_FEATURE_SNAPSHOTS_GOV_GROUP_FAILED",
                exc,
                warn_key="model_feature_snapshots_gov_group_failed_outer",
                symbol=symbol_key,
                ts_ms=int(anchor_ts_ms),
            )
            gov_features, gov_meta, gov_available = (
                {fid: 0.0 for fid in GOV_FEATURE_IDS},
                {"latest_availability_ts_ms": None, "sector": ""},
                False,
            )
        try:
            fundamentals_features, fundamentals_meta, fundamentals_available = _load_fundamentals_group(
                con,
                symbol=symbol_key,
                ts_ms=anchor_ts_ms,
            )
        except Exception as exc:
            _warn_nonfatal(
                "model_feature_snapshots_fundamentals_group_failed",
                "MODEL_FEATURE_SNAPSHOTS_FUNDAMENTALS_GROUP_FAILED",
                exc,
                warn_key="model_feature_snapshots_fundamentals_group_failed_outer",
                symbol=symbol_key,
                ts_ms=int(anchor_ts_ms),
            )
            fundamentals_features, fundamentals_meta, fundamentals_available = (
                {fid: 0.0 for fid in FUNDAMENTALS_PIT_FEATURE_IDS},
                {"latest_publish_ts_ms": None, "mode": "pit"},
                False,
            )
        try:
            congressional_features, congressional_meta, congressional_available = _load_congressional_group(
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
            congressional_features, congressional_meta, congressional_available = (
                {fid: 0.0 for fid in CONGRESSIONAL_FEATURE_IDS},
                {"latest_trade_ts_ms": None},
                False,
            )
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
            finbert_features, finbert_meta, finbert_available = _load_finbert_group(con, symbol=symbol_key, ts_ms=anchor_ts_ms)
        except Exception as exc:
            _warn_nonfatal(
                "model_feature_snapshots_finbert_group_failed",
                "MODEL_FEATURE_SNAPSHOTS_FINBERT_GROUP_FAILED",
                exc,
                warn_key="model_feature_snapshots_finbert_group_failed",
                symbol=symbol_key,
                ts_ms=int(anchor_ts_ms),
            )
            finbert_features, finbert_meta, finbert_available = ({fid: 0.0 for fid in FINBERT_FEATURE_IDS}, {"ts_ms": None}, False)
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
        try:
            bocpd_features, bocpd_meta, bocpd_available = _load_bocpd_group(con, symbol=symbol_key, ts_ms=anchor_ts_ms)
        except Exception as exc:
            _warn_nonfatal(
                "model_feature_snapshots_bocpd_group_failed",
                "MODEL_FEATURE_SNAPSHOTS_BOCPD_GROUP_FAILED",
                exc,
                warn_key="model_feature_snapshots_bocpd_group_failed_outer",
                symbol=symbol_key,
                ts_ms=int(anchor_ts_ms),
            )
            bocpd_features, bocpd_meta, bocpd_available = (
                {fid: 0.0 for fid in BOCPD_FEATURE_IDS},
                {"summary_ts_ms": None, "series_key": None, "series_type": None},
                False,
            )
        try:
            ts_foundation_feature_ids = [
                fid
                for fid in ids
                if str(fid or "").startswith("tsfm.chronos_v2.")
            ]
            ts_foundation_features, ts_foundation_meta, ts_foundation_available = _load_ts_foundation_chronos_group(
                con,
                symbol=symbol_key,
                ts_ms=anchor_ts_ms,
                feature_ids=list(ts_foundation_feature_ids or TS_FOUNDATION_CHRONOS_FEATURE_IDS),
            )
        except Exception as exc:
            _warn_nonfatal(
                "model_feature_snapshots_ts_foundation_chronos_group_failed",
                "MODEL_FEATURE_SNAPSHOTS_TS_FOUNDATION_CHRONOS_GROUP_FAILED",
                exc,
                warn_key="model_feature_snapshots_ts_foundation_chronos_group_failed_outer",
                symbol=symbol_key,
                ts_ms=int(anchor_ts_ms),
            )
            ts_foundation_features, ts_foundation_meta, ts_foundation_available = (
                {fid: 0.0 for fid in TS_FOUNDATION_CHRONOS_FEATURE_IDS},
                {"status": "error", "price_history_last_ts_ms": None},
                False,
            )
        graph_features: Dict[str, float] = {}
        graph_meta: Dict[str, Any] = {"status": "not_requested"}
        graph_available = False
        if graph_feature_ids:
            try:
                graph_features, graph_meta, graph_available = _load_graph_relational_group(
                    con,
                    symbol=symbol_key,
                    ts_ms=anchor_ts_ms,
                    feature_ids=list(graph_feature_ids),
                )
            except Exception as exc:
                _warn_nonfatal(
                    "model_feature_snapshots_graph_relational_group_failed",
                    "MODEL_FEATURE_SNAPSHOTS_GRAPH_RELATIONAL_GROUP_FAILED",
                    exc,
                    warn_key="model_feature_snapshots_graph_relational_group_failed_outer",
                    symbol=symbol_key,
                    ts_ms=int(anchor_ts_ms),
                )
                graph_features, graph_meta, graph_available = (
                    {fid: 0.0 for fid in graph_feature_ids},
                    {"status": "error", "max_source_ts_ms": None, "max_availability_ts_ms": None},
                    False,
                )

        for mapping in (
            price_features,
            tech_features,
            event_features,
            macro_features,
            prediction_market_macro_features,
            prediction_market_event_features,
            options_features,
            insider_features,
            short_features,
            crypto_positioning_features,
            deribit_features,
            sportsbook_odds_features,
            news_flow_features,
            structured_doc_events_features,
            etf_flow_features,
            cot_features,
            inst_13f_features,
            gov_features,
            fundamentals_features,
            congressional_features,
            social_features,
            finbert_features,
            weather_features,
            bocpd_features,
            ts_foundation_features,
            graph_features,
        ):
            group_features.update(mapping)

        availability = {
            "price": bool(price_available),
            "tech": bool(tech_available),
            "events": bool(event_available),
            "macro": bool(macro_available),
            PREDICTION_MARKET_MACRO_FEATURE_GROUP: bool(prediction_market_macro_available),
            PREDICTION_MARKET_EVENT_FEATURE_GROUP: bool(prediction_market_event_available),
            "options": bool(options_available),
            "insider": bool(insider_available),
            "short": bool(short_available),
            "crypto_positioning": bool(crypto_positioning_available),
            "news_flow": bool(news_flow_available),
            "structured_doc_events": bool(structured_doc_events_available),
            "etf_flow": bool(etf_flow_available),
            "cot": bool(cot_available),
            "inst_13f": bool(inst_13f_available),
            "gov": bool(gov_available),
            "fundamentals": bool(fundamentals_available),
            "congressional": bool(congressional_available),
            "social": bool(social_available),
            "sentiment": bool(finbert_available),
            "weather": bool(weather_available),
            "bocpd_regime": bool(bocpd_available),
            TS_FOUNDATION_CHRONOS_GROUP: bool(ts_foundation_available),
        }
        if deribit_feature_ids:
            availability[DERIBIT_FEATURE_GROUP] = bool(deribit_available)
        if sportsbook_odds_feature_ids:
            availability[SPORTSBOOK_ODDS_FEATURE_GROUP] = bool(sportsbook_odds_available)
        if graph_feature_ids:
            availability[GRAPH_RELATIONAL_GROUP] = bool(graph_available)
        source_timestamps.update(
            {
                "price": price_meta,
                "tech": tech_meta,
                "events": event_meta,
                "macro": macro_meta,
                PREDICTION_MARKET_MACRO_FEATURE_GROUP: prediction_market_macro_meta,
                PREDICTION_MARKET_EVENT_FEATURE_GROUP: prediction_market_event_meta,
                "options": options_meta,
                "insider": insider_meta,
                "short": short_meta,
                "crypto_positioning": crypto_positioning_meta,
                "news_flow": news_flow_meta,
                "structured_doc_events": structured_doc_events_meta,
                "etf_flow": etf_flow_meta,
                "cot": cot_meta,
                "inst_13f": inst_13f_meta,
                "gov": gov_meta,
                "fundamentals": fundamentals_meta,
                "congressional": congressional_meta,
                "social": social_meta,
                "sentiment": finbert_meta,
                "weather": weather_meta,
                "bocpd_regime": bocpd_meta,
                TS_FOUNDATION_CHRONOS_GROUP: ts_foundation_meta,
            }
        )
        if deribit_feature_ids:
            source_timestamps[DERIBIT_FEATURE_GROUP] = dict(deribit_meta or {})
        if sportsbook_odds_feature_ids:
            source_timestamps[SPORTSBOOK_ODDS_FEATURE_GROUP] = dict(sportsbook_odds_meta or {})
        if graph_feature_ids:
            source_timestamps[GRAPH_RELATIONAL_GROUP] = dict(graph_meta or {})

        group_features, availability, pit_controls = enforce_feature_pit_controls(
            features=group_features,
            availability=availability,
            source_timestamps=source_timestamps,
            anchor_ts_ms=int(anchor_ts_ms),
            feature_ids=list(ids),
        )
        feature_metadata = policy_metadata_for_groups(sorted(set(pit_controls.keys()) | set(availability.keys())))
        source_timestamps["_pit_controls"] = dict(pit_controls)
        source_timestamps["_feature_metadata"] = dict(feature_metadata)

        group_features["availability.price"] = 1.0 if availability.get("price") else 0.0
        group_features["availability.events"] = 1.0 if availability.get("events") else 0.0
        group_features["availability.macro"] = 1.0 if availability.get("macro") else 0.0
        group_features["availability.options"] = 1.0 if availability.get("options") else 0.0
        group_features["availability.social"] = 1.0 if availability.get("social") else 0.0
        group_features["availability.weather"] = 1.0 if availability.get("weather") else 0.0

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
            "feature_metadata": dict(feature_metadata),
            "pit_controls": dict(pit_controls),
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


def _attach_snapshot_pit_metadata(payload: Mapping[str, Any] | None) -> Dict[str, Any]:
    out = dict(payload or {})
    source_timestamps = dict(out.get("source_timestamps") or {})
    out.setdefault("feature_metadata", dict(source_timestamps.get("_feature_metadata") or {}))
    out.setdefault("pit_controls", dict(source_timestamps.get("_pit_controls") or {}))
    return out


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
                    return _attach_snapshot_pit_metadata(cached)
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
        payload = {
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
        return _attach_snapshot_pit_metadata(payload)
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
