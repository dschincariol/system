"""Learned alpha decay, capacity, and crowding policy.

The training job in this module turns realized net-after-cost labels into
cohort-level estimates that live policy paths can consume:

- learned half-life and maximum useful signal age
- normalized capacity estimate
- crowding penalty and resulting size multiplier

Rows are grouped by signal age, model family, symbol, regime, liquidity bucket,
spread bucket, volatility bucket, and factor group.  Consumers fail open when no
estimate exists, but fail closed for stale or low-capacity estimates once a
matching cohort has been learned.
"""

from __future__ import annotations

import json
import math
import os
import statistics
import time
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect, init_db, run_write_txn, table_exists
from engine.strategy.net_after_cost_labels import infer_model_family


LOG = get_logger("engine.strategy.learned_alpha_decay")
RUNS_TABLE = "learned_alpha_decay_runs"
ESTIMATES_TABLE = "learned_alpha_decay_estimates"
AGE_EDGES_TABLE = "learned_alpha_decay_age_edges"

LOOKBACK_DAYS = int(os.environ.get("LEARNED_ALPHA_LOOKBACK_DAYS", "90"))
MIN_SAMPLES = int(os.environ.get("LEARNED_ALPHA_MIN_SAMPLES", "50"))
AGE_BUCKET_MS = int(os.environ.get("LEARNED_ALPHA_AGE_BUCKET_MS", str(60 * 1000)))
REFERENCE_CAPACITY = float(os.environ.get("LEARNED_ALPHA_REFERENCE_CAPACITY", "0.10"))
MIN_CAPACITY = float(os.environ.get("LEARNED_ALPHA_MIN_CAPACITY", "0.005"))
MIN_SIZE_MULTIPLIER = float(os.environ.get("LEARNED_ALPHA_MIN_SIZE_MULTIPLIER", "0.05"))
MAX_CROWDING_PENALTY = float(os.environ.get("LEARNED_ALPHA_MAX_CROWDING_PENALTY", "0.95"))
MIN_POSITIVE_RATE = float(os.environ.get("LEARNED_ALPHA_MIN_POSITIVE_RATE", "0.45"))
MIN_MEAN_EDGE = float(os.environ.get("LEARNED_ALPHA_MIN_MEAN_EDGE", "0.0"))
MAX_LOOKUP_AGE_MS = int(os.environ.get("LEARNED_ALPHA_MAX_LOOKUP_AGE_MS", str(7 * 24 * 60 * 60 * 1000)))

_WARNED_NONFATAL_KEYS: set[str] = set()
_SCHEMA_READY = False


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.strategy.learned_alpha_decay",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(str(once_key))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return float(default)
        out = float(value)
        return float(out) if math.isfinite(out) else float(default)
    except Exception:
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return int(default)
        return int(value)
    except Exception:
        return int(default)


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(float(lo), min(float(hi), float(value)))


def _json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if value in (None, ""):
        return {}
    try:
        parsed = json.loads(str(value))
    except Exception:
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _table_exists(con, table_name: str) -> bool:
    try:
        return bool(table_exists(con, str(table_name)))
    except Exception as error:
        _warn_nonfatal(
            "LEARNED_ALPHA_TABLE_EXISTS_PROBE_FAILED",
            error,
            once_key=f"table_exists:{table_name}",
            table_name=str(table_name),
        )
        return False


def _columns(con, table_name: str) -> set[str]:
    try:
        return {
            str(row[1] or "").strip()
            for row in (con.execute(f"PRAGMA table_info({table_name})").fetchall() or [])
            if row and len(row) > 1
        }
    except Exception:
        return set()


def ensure_schema(con) -> None:
    con.executescript(
        f"""
        CREATE TABLE IF NOT EXISTS {RUNS_TABLE} (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          lookback_days INTEGER NOT NULL,
          min_samples INTEGER NOT NULL,
          age_bucket_ms INTEGER NOT NULL,
          params_json TEXT,
          metrics_json TEXT
        );

        CREATE TABLE IF NOT EXISTS {ESTIMATES_TABLE} (
          run_id INTEGER NOT NULL,
          ts_ms INTEGER NOT NULL,
          cohort_key TEXT NOT NULL,
          cohort_level TEXT NOT NULL,
          model_family TEXT NOT NULL,
          symbol TEXT NOT NULL,
          regime TEXT NOT NULL,
          liquidity_bucket TEXT NOT NULL,
          spread_bucket TEXT NOT NULL,
          volatility_bucket TEXT NOT NULL,
          factor_group TEXT NOT NULL,
          n_obs INTEGER NOT NULL,
          mean_realized_edge REAL NOT NULL,
          positive_rate REAL NOT NULL,
          half_life_ms INTEGER NOT NULL,
          max_useful_age_ms INTEGER NOT NULL,
          capacity_estimate REAL NOT NULL,
          crowding_penalty REAL NOT NULL,
          size_multiplier REAL NOT NULL,
          block_signal INTEGER NOT NULL DEFAULT 0,
          detail_json TEXT,
          PRIMARY KEY (run_id, cohort_key)
        );

        CREATE TABLE IF NOT EXISTS {AGE_EDGES_TABLE} (
          run_id INTEGER NOT NULL,
          cohort_key TEXT NOT NULL,
          age_bucket_ms INTEGER NOT NULL,
          n_obs INTEGER NOT NULL,
          mean_realized_edge REAL NOT NULL,
          positive_rate REAL NOT NULL,
          avg_total_cost_bps REAL NOT NULL,
          avg_capacity_unit REAL NOT NULL,
          detail_json TEXT,
          PRIMARY KEY (run_id, cohort_key, age_bucket_ms)
        );

        CREATE INDEX IF NOT EXISTS idx_learned_alpha_decay_runs_ts
          ON {RUNS_TABLE}(ts_ms);
        CREATE INDEX IF NOT EXISTS idx_learned_alpha_decay_estimates_lookup
          ON {ESTIMATES_TABLE}(cohort_key, ts_ms);
        CREATE INDEX IF NOT EXISTS idx_learned_alpha_decay_estimates_dims
          ON {ESTIMATES_TABLE}(model_family, symbol, regime, factor_group, ts_ms);
        CREATE INDEX IF NOT EXISTS idx_learned_alpha_decay_age_edges
          ON {AGE_EDGES_TABLE}(cohort_key, age_bucket_ms);
        """
    )


def init_schema() -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    run_write_txn(
        ensure_schema,
        table=ESTIMATES_TABLE,
        operation="init_learned_alpha_decay_schema",
        direct=True,
    )
    _SCHEMA_READY = True


def _bucket_spread(spread_bps: Any) -> str:
    value = _safe_float(spread_bps, -1.0)
    if value < 0.0:
        return "unknown"
    if value <= 2.0:
        return "tight"
    if value <= 6.0:
        return "normal"
    if value <= 15.0:
        return "wide"
    return "very_wide"


def _bucket_volatility(value: Any, *payloads: Mapping[str, Any]) -> str:
    for payload in payloads:
        for key in ("volatility_bucket", "volatility_regime", "vol_bucket", "vol_regime"):
            text = str((payload or {}).get(key) or "").strip().lower()
            if text:
                return text[:64]
    vol = _safe_float(value, -1.0)
    if vol < 0.0:
        return "unknown"
    if vol <= 0.01:
        return "low"
    if vol <= 0.03:
        return "medium"
    if vol <= 0.07:
        return "high"
    return "extreme"


def _bucket_liquidity(*payloads: Mapping[str, Any]) -> str:
    for payload in payloads:
        for key in ("liquidity_bucket", "liquidity_regime", "liquidity", "liquidity_tier"):
            text = str((payload or {}).get(key) or "").strip().lower()
            if text:
                return text[:64]
    return "unknown"


def _factor_group(*payloads: Mapping[str, Any], model_id: str = "", model_name: str = "") -> str:
    for payload in payloads:
        for key in ("factor_group", "factor_family", "feature_group", "alpha_group", "group"):
            text = str((payload or {}).get(key) or "").strip().lower()
            if text:
                return text[:96]
        for nested_key in ("signal", "model_intent", "competition", "features"):
            nested = (payload or {}).get(nested_key)
            if isinstance(nested, dict):
                out = _factor_group(nested, model_id=model_id, model_name=model_name)
                if out != "unknown":
                    return out
    identity = str(model_id or model_name or "").strip().lower()
    for sep in (":", ".", "_"):
        if sep in identity:
            prefix = identity.split(sep, 1)[0].strip()
            if prefix:
                return prefix[:96]
    return "unknown"


def _cohort_key(dims: Mapping[str, str]) -> str:
    return "|".join(
        [
            str(dims.get("model_family") or "*").strip().lower() or "*",
            str(dims.get("symbol") or "*").strip().upper() or "*",
            str(dims.get("regime") or "*").strip().lower() or "*",
            str(dims.get("liquidity_bucket") or "*").strip().lower() or "*",
            str(dims.get("spread_bucket") or "*").strip().lower() or "*",
            str(dims.get("volatility_bucket") or "*").strip().lower() or "*",
            str(dims.get("factor_group") or "*").strip().lower() or "*",
        ]
    )


def cohort_dimensions_from_payload(payload: Mapping[str, Any]) -> Dict[str, str]:
    obj = dict(payload or {})
    explain = obj.get("explain")
    explain_obj = dict(explain or {}) if isinstance(explain, dict) else {}
    signal_obj = dict(explain_obj.get("signal") or {}) if isinstance(explain_obj.get("signal"), dict) else {}
    model_family = str(obj.get("model_family") or "").strip().lower()
    if not model_family:
        model_family = infer_model_family(
            obj.get("model_name"),
            obj.get("model_id"),
            explain_obj.get("model_name"),
            explain_obj.get("model_id"),
        )
    symbol = str(obj.get("symbol") or signal_obj.get("symbol") or "").strip().upper() or "*"
    regime = str(
        obj.get("regime")
        or obj.get("market_regime")
        or explain_obj.get("regime")
        or explain_obj.get("market_regime")
        or signal_obj.get("regime")
        or "global"
    ).strip().lower() or "global"
    liquidity_bucket = _bucket_liquidity(obj, explain_obj, signal_obj)
    spread_bucket = str(obj.get("spread_bucket") or "").strip().lower() or _bucket_spread(
        obj.get("spread_bps") or obj.get("true_spread_bps") or obj.get("entry_spread_bps")
    )
    volatility_bucket = _bucket_volatility(
        obj.get("volatility") or obj.get("realized_vol") or obj.get("sigma"),
        obj,
        explain_obj,
        signal_obj,
    )
    factor_group = _factor_group(
        obj,
        explain_obj,
        signal_obj,
        model_id=str(obj.get("model_id") or explain_obj.get("model_id") or ""),
        model_name=str(obj.get("model_name") or explain_obj.get("model_name") or ""),
    )
    return {
        "model_family": str(model_family or "unknown").lower(),
        "symbol": str(symbol or "*").upper(),
        "regime": str(regime or "global").lower(),
        "liquidity_bucket": str(liquidity_bucket or "unknown").lower(),
        "spread_bucket": str(spread_bucket or "unknown").lower(),
        "volatility_bucket": str(volatility_bucket or "unknown").lower(),
        "factor_group": str(factor_group or "unknown").lower(),
    }


def _extract_capacity_unit(label_metadata: Mapping[str, Any], confidence_metadata: Mapping[str, Any], fill_count: int) -> float:
    candidates: List[Any] = []
    for payload in (label_metadata, confidence_metadata):
        candidates.extend(
            [
                payload.get("target_weight"),
                payload.get("to_weight"),
                payload.get("abs_weight"),
                payload.get("position_weight"),
                payload.get("position_size_fraction"),
                payload.get("notional_fraction"),
            ]
        )
        execution_trace = payload.get("execution_trace")
        if isinstance(execution_trace, dict):
            notional = _safe_float(execution_trace.get("notional"), 0.0)
            if notional > 0.0:
                candidates.append(min(1.0, notional / 1_000_000.0))
    for value in candidates:
        parsed = abs(_safe_float(value, 0.0))
        if parsed > 0.0:
            return float(_clamp(parsed, 0.0, 1.0))
    if fill_count > 0:
        return float(_clamp(0.01 * float(fill_count), 0.01, 1.0))
    return 0.01


def _load_net_after_cost_observations(con, *, min_ts_ms: int) -> List[Dict[str, Any]]:
    if not _table_exists(con, "net_after_cost_labels"):
        return []
    try:
        rows = con.execute(
            """
            SELECT model_family, model_name, model_id, symbol, regime,
                   label_ts_ms, entry_ts_ms, exit_ts_ms, computed_at_ts_ms,
                   net_return, total_cost_bps, spread_bps, confidence_metadata_json,
                   label_metadata_json, fill_count, order_count
            FROM net_after_cost_labels
            WHERE label_ts_ms >= ?
              AND realized=1
            """,
            (int(min_ts_ms),),
        ).fetchall() or []
    except Exception as e:
        _warn_nonfatal("LEARNED_ALPHA_NET_LABEL_QUERY_FAILED", e, once_key="net_label_query")
        return []

    observations: List[Dict[str, Any]] = []
    for row in rows:
        (
            model_family,
            model_name,
            model_id,
            symbol,
            regime,
            label_ts_ms,
            entry_ts_ms,
            exit_ts_ms,
            computed_at_ts_ms,
            net_return,
            total_cost_bps,
            spread_bps,
            confidence_metadata_json,
            label_metadata_json,
            fill_count,
            order_count,
        ) = row
        conf_meta = _json_dict(confidence_metadata_json)
        label_meta = _json_dict(label_metadata_json)
        signal_ts = _safe_int(label_ts_ms, 0)
        entry_ts = _safe_int(entry_ts_ms, 0)
        exit_ts = _safe_int(exit_ts_ms, 0)
        computed_ts = _safe_int(computed_at_ts_ms, 0)
        observed_ts = entry_ts or exit_ts or computed_ts or signal_ts
        age_ms = max(0, int(observed_ts) - int(signal_ts)) if signal_ts > 0 else 0
        volatility_value = (
            conf_meta.get("volatility")
            or label_meta.get("volatility")
            or conf_meta.get("realized_vol")
            or label_meta.get("realized_vol")
        )
        dims = cohort_dimensions_from_payload(
            {
                "model_family": model_family,
                "model_name": model_name,
                "model_id": model_id,
                "symbol": symbol,
                "regime": regime,
                "spread_bps": spread_bps,
                "volatility": volatility_value,
                "liquidity_bucket": _bucket_liquidity(conf_meta, label_meta),
                "volatility_bucket": _bucket_volatility(volatility_value, conf_meta, label_meta),
                "factor_group": _factor_group(conf_meta, label_meta, model_id=str(model_id or ""), model_name=str(model_name or "")),
            }
        )
        observations.append(
            {
                **dims,
                "signal_age_ms": int(age_ms),
                "realized_edge": _safe_float(net_return, 0.0),
                "total_cost_bps": max(0.0, _safe_float(total_cost_bps, 0.0)),
                "spread_bps": max(0.0, _safe_float(spread_bps, 0.0)),
                "capacity_unit": _extract_capacity_unit(label_meta, conf_meta, _safe_int(fill_count, 0)),
                "fill_count": _safe_int(fill_count, 0),
                "order_count": _safe_int(order_count, 0),
                "source": "net_after_cost_labels",
            }
        )
    return observations


def _load_labels_exec_observations(con, *, min_ts_ms: int) -> List[Dict[str, Any]]:
    if not _table_exists(con, "labels_exec"):
        return []
    cols = _columns(con, "labels_exec")
    if "net_ret" not in cols and "net_return" not in cols:
        return []
    select_cols = [
        "symbol" if "symbol" in cols else "'' AS symbol",
        "ts_ms" if "ts_ms" in cols else "0 AS ts_ms",
        "net_ret" if "net_ret" in cols else "net_return AS net_ret",
        "spread_bps" if "spread_bps" in cols else "0 AS spread_bps",
        "liquidity" if "liquidity" in cols else "'' AS liquidity",
        "volatility_regime" if "volatility_regime" in cols else "'' AS volatility_regime",
        "model_family" if "model_family" in cols else "'' AS model_family",
        "model_name" if "model_name" in cols else "'' AS model_name",
        "model_id" if "model_id" in cols else "'' AS model_id",
        "regime" if "regime" in cols else "'' AS regime",
    ]
    try:
        rows = con.execute(
            f"""
            SELECT {", ".join(select_cols)}
            FROM labels_exec
            WHERE ts_ms >= ?
            """,
            (int(min_ts_ms),),
        ).fetchall() or []
    except Exception as e:
        _warn_nonfatal("LEARNED_ALPHA_LABELS_EXEC_QUERY_FAILED", e, once_key="labels_exec_query")
        return []

    out: List[Dict[str, Any]] = []
    for symbol, ts_ms, net_ret, spread_bps, liquidity, volatility_regime, model_family, model_name, model_id, regime in rows:
        dims = cohort_dimensions_from_payload(
            {
                "model_family": model_family,
                "model_name": model_name,
                "model_id": model_id,
                "symbol": symbol,
                "regime": regime,
                "spread_bps": spread_bps,
                "liquidity_bucket": liquidity,
                "volatility_bucket": volatility_regime,
            }
        )
        out.append(
            {
                **dims,
                "signal_age_ms": 0,
                "realized_edge": _safe_float(net_ret, 0.0),
                "total_cost_bps": 0.0,
                "spread_bps": max(0.0, _safe_float(spread_bps, 0.0)),
                "capacity_unit": 0.01,
                "fill_count": 0,
                "order_count": 0,
                "source": "labels_exec",
            }
        )
    return out


def load_realized_edge_observations(con, *, lookback_days: int, now_ms: Optional[int] = None) -> List[Dict[str, Any]]:
    ts = int(now_ms if now_ms is not None else _now_ms())
    min_ts_ms = int(ts) - int(max(1, lookback_days)) * 86_400_000
    observations = _load_net_after_cost_observations(con, min_ts_ms=int(min_ts_ms))
    if observations:
        return observations
    return _load_labels_exec_observations(con, min_ts_ms=int(min_ts_ms))


def _quantile(values: Sequence[float], q: float) -> float:
    vals = sorted(float(v) for v in values if math.isfinite(float(v)))
    if not vals:
        return 0.0
    if len(vals) == 1:
        return float(vals[0])
    pos = _clamp(float(q), 0.0, 1.0) * float(len(vals) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(vals[lo])
    frac = pos - lo
    return float(vals[lo] * (1.0 - frac) + vals[hi] * frac)


def _mean(values: Iterable[float]) -> float:
    vals = [float(v) for v in values]
    return float(sum(vals) / float(len(vals))) if vals else 0.0


def _std(values: Iterable[float]) -> float:
    vals = [float(v) for v in values]
    if len(vals) < 2:
        return 0.0
    return float(statistics.stdev(vals))


def _estimate_from_observations(
    observations: Sequence[Mapping[str, Any]],
    *,
    dims: Mapping[str, str],
    cohort_level: str,
    age_bucket_ms: int,
    run_ts_ms: int,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    edges = [_safe_float(row.get("realized_edge"), 0.0) for row in observations]
    costs = [_safe_float(row.get("total_cost_bps"), 0.0) for row in observations]
    capacities = [_safe_float(row.get("capacity_unit"), 0.0) for row in observations]
    n_obs = len(edges)
    mean_edge = _mean(edges)
    std_edge = _std(edges)
    positive_rate = float(sum(1 for edge in edges if edge > 0.0) / float(max(1, n_obs)))
    avg_cost = _mean(costs)

    by_age: Dict[int, List[Mapping[str, Any]]] = {}
    for row in observations:
        age = max(0, _safe_int(row.get("signal_age_ms"), 0))
        bucket = int(age // max(1, int(age_bucket_ms))) * int(age_bucket_ms)
        by_age.setdefault(int(bucket), []).append(row)

    age_rows: List[Dict[str, Any]] = []
    for bucket, bucket_rows in sorted(by_age.items()):
        bucket_edges = [_safe_float(row.get("realized_edge"), 0.0) for row in bucket_rows]
        bucket_costs = [_safe_float(row.get("total_cost_bps"), 0.0) for row in bucket_rows]
        bucket_caps = [_safe_float(row.get("capacity_unit"), 0.0) for row in bucket_rows]
        age_rows.append(
            {
                "age_bucket_ms": int(bucket),
                "n_obs": int(len(bucket_rows)),
                "mean_realized_edge": _mean(bucket_edges),
                "positive_rate": float(sum(1 for edge in bucket_edges if edge > 0.0) / float(max(1, len(bucket_edges)))),
                "avg_total_cost_bps": _mean(bucket_costs),
                "avg_capacity_unit": _mean(bucket_caps),
            }
        )

    positive_age_rows = [row for row in age_rows if float(row["mean_realized_edge"]) > 0.0]
    peak_edge = max((float(row["mean_realized_edge"]) for row in age_rows), default=0.0)
    half_life_ms = int(max(1, age_bucket_ms))
    if peak_edge > 0.0:
        threshold = 0.5 * float(peak_edge)
        crossed = next((row for row in age_rows if float(row["mean_realized_edge"]) <= threshold), None)
        if crossed is not None:
            half_life_ms = int(max(age_bucket_ms, int(crossed["age_bucket_ms"]) + int(age_bucket_ms // 2)))
        elif age_rows:
            half_life_ms = int(max(age_bucket_ms, int(age_rows[-1]["age_bucket_ms"]) + int(age_bucket_ms)))

    zero_cross = next((row for row in age_rows if float(row["mean_realized_edge"]) <= 0.0), None)
    if zero_cross is not None:
        max_useful_age_ms = int(max(0, int(zero_cross["age_bucket_ms"])))
    elif positive_age_rows:
        last_positive = int(positive_age_rows[-1]["age_bucket_ms"])
        max_useful_age_ms = int(last_positive + int(age_bucket_ms))
    else:
        max_useful_age_ms = int(max(0, age_bucket_ms))

    positive_capacity = [
        _safe_float(row.get("capacity_unit"), 0.0)
        for row in observations
        if _safe_float(row.get("realized_edge"), 0.0) > 0.0
    ]
    if positive_capacity:
        capacity_estimate = _quantile(positive_capacity, 0.75)
    else:
        capacity_estimate = 0.0
    if capacity_estimate <= 0.0 and mean_edge > 0.0:
        capacity_estimate = max(float(MIN_CAPACITY), positive_rate * float(REFERENCE_CAPACITY))
    capacity_estimate = float(_clamp(capacity_estimate, 0.0, 1.0))

    edge_bps = abs(float(mean_edge)) * 10000.0
    cost_pressure = float(avg_cost) / max(1e-9, float(avg_cost) + float(edge_bps))
    loss_pressure = max(0.0, -float(mean_edge)) / max(1e-9, abs(float(mean_edge)) + float(std_edge))
    crowding_penalty = (0.50 * cost_pressure) + (0.50 * loss_pressure)
    if mean_edge > 0.0 and positive_rate >= 0.55:
        crowding_penalty *= 0.50
    crowding_penalty = float(_clamp(crowding_penalty, 0.0, MAX_CROWDING_PENALTY))

    capacity_multiplier = _clamp(capacity_estimate / max(1e-9, REFERENCE_CAPACITY), 0.0, 1.0)
    edge_multiplier = _clamp(positive_rate, 0.0, 1.0) if mean_edge > MIN_MEAN_EDGE else 0.0
    size_multiplier = float(_clamp((1.0 - crowding_penalty) * capacity_multiplier * edge_multiplier, 0.0, 1.0))
    block_signal = bool(
        n_obs > 0
        and (
            capacity_estimate < float(MIN_CAPACITY)
            or size_multiplier <= float(MIN_SIZE_MULTIPLIER)
            or positive_rate < float(MIN_POSITIVE_RATE)
            or mean_edge <= float(MIN_MEAN_EDGE)
        )
    )

    detail = {
        "cohort_level": str(cohort_level),
        "age_bucket_ms": int(age_bucket_ms),
        "std_realized_edge": float(std_edge),
        "avg_total_cost_bps": float(avg_cost),
        "avg_spread_bps": _mean(_safe_float(row.get("spread_bps"), 0.0) for row in observations),
        "avg_capacity_unit": _mean(capacities),
        "capacity_multiplier": float(capacity_multiplier),
        "edge_multiplier": float(edge_multiplier),
        "source_counts": {
            source: sum(1 for row in observations if str(row.get("source") or "") == source)
            for source in sorted({str(row.get("source") or "") for row in observations})
        },
    }

    estimate = {
        "ts_ms": int(run_ts_ms),
        "cohort_key": _cohort_key(dims),
        "cohort_level": str(cohort_level),
        **{key: str(dims.get(key) or "*") for key in ("model_family", "symbol", "regime", "liquidity_bucket", "spread_bucket", "volatility_bucket", "factor_group")},
        "n_obs": int(n_obs),
        "mean_realized_edge": float(mean_edge),
        "positive_rate": float(positive_rate),
        "half_life_ms": int(max(1, half_life_ms)),
        "max_useful_age_ms": int(max(0, max_useful_age_ms)),
        "capacity_estimate": float(capacity_estimate),
        "crowding_penalty": float(crowding_penalty),
        "size_multiplier": float(size_multiplier),
        "block_signal": int(1 if block_signal else 0),
        "detail": detail,
    }
    return estimate, age_rows


_AGGREGATION_LEVELS: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    ("exact", ("model_family", "symbol", "regime", "liquidity_bucket", "spread_bucket", "volatility_bucket", "factor_group")),
    ("family_symbol_regime", ("model_family", "symbol", "regime")),
    ("family_symbol", ("model_family", "symbol")),
    ("family_regime", ("model_family", "regime")),
    ("family", ("model_family",)),
    ("global", ()),
)


def _aggregate_estimates(
    observations: Sequence[Mapping[str, Any]],
    *,
    min_samples: int,
    age_bucket_ms: int,
    run_ts_ms: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
    estimates: Dict[str, Dict[str, Any]] = {}
    age_edges: Dict[str, List[Dict[str, Any]]] = {}
    dim_names = ("model_family", "symbol", "regime", "liquidity_bucket", "spread_bucket", "volatility_bucket", "factor_group")

    for level_name, retained_dims in _AGGREGATION_LEVELS:
        grouped: Dict[str, List[Mapping[str, Any]]] = {}
        dims_by_key: Dict[str, Dict[str, str]] = {}
        for row in observations:
            dims = {
                name: (str(row.get(name) or "*").upper() if name == "symbol" else str(row.get(name) or "*").lower())
                for name in dim_names
            }
            reduced = {name: (dims[name] if name in retained_dims else "*") for name in dim_names}
            key = _cohort_key(reduced)
            grouped.setdefault(key, []).append(row)
            dims_by_key[key] = reduced
        for key, rows in grouped.items():
            if len(rows) < int(min_samples):
                continue
            estimate, age_rows = _estimate_from_observations(
                rows,
                dims=dims_by_key[key],
                cohort_level=str(level_name),
                age_bucket_ms=int(age_bucket_ms),
                run_ts_ms=int(run_ts_ms),
            )
            estimates[key] = estimate
            age_edges[key] = age_rows
    return list(estimates.values()), age_edges


def _store_estimates(
    con,
    *,
    ts_ms: int,
    lookback_days: int,
    min_samples: int,
    age_bucket_ms: int,
    observations_n: int,
    estimates: Sequence[Mapping[str, Any]],
    age_edges: Mapping[str, Sequence[Mapping[str, Any]]],
) -> int:
    ensure_schema(con)
    params = {
        "lookback_days": int(lookback_days),
        "min_samples": int(min_samples),
        "age_bucket_ms": int(age_bucket_ms),
        "reference_capacity": float(REFERENCE_CAPACITY),
        "min_capacity": float(MIN_CAPACITY),
        "min_size_multiplier": float(MIN_SIZE_MULTIPLIER),
        "max_crowding_penalty": float(MAX_CROWDING_PENALTY),
    }
    metrics = {
        "observations": int(observations_n),
        "estimates": int(len(estimates or [])),
        "age_edge_rows": int(sum(len(rows or []) for rows in (age_edges or {}).values())),
    }
    con.execute(
        f"""
        INSERT INTO {RUNS_TABLE}(ts_ms, lookback_days, min_samples, age_bucket_ms, params_json, metrics_json)
        VALUES (?,?,?,?,?,?)
        """,
        (
            int(ts_ms),
            int(lookback_days),
            int(min_samples),
            int(age_bucket_ms),
            json.dumps(params, separators=(",", ":"), sort_keys=True),
            json.dumps(metrics, separators=(",", ":"), sort_keys=True),
        ),
    )
    run_id = _safe_int((con.execute("SELECT last_insert_rowid()").fetchone() or [0])[0], 0)

    for estimate in estimates or []:
        detail = dict(estimate.get("detail") or {})
        con.execute(
            f"""
            INSERT INTO {ESTIMATES_TABLE}(
              run_id, ts_ms, cohort_key, cohort_level, model_family, symbol, regime,
              liquidity_bucket, spread_bucket, volatility_bucket, factor_group,
              n_obs, mean_realized_edge, positive_rate, half_life_ms,
              max_useful_age_ms, capacity_estimate, crowding_penalty,
              size_multiplier, block_signal, detail_json
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(run_id),
                int(ts_ms),
                str(estimate.get("cohort_key") or ""),
                str(estimate.get("cohort_level") or ""),
                str(estimate.get("model_family") or "*"),
                str(estimate.get("symbol") or "*"),
                str(estimate.get("regime") or "*"),
                str(estimate.get("liquidity_bucket") or "*"),
                str(estimate.get("spread_bucket") or "*"),
                str(estimate.get("volatility_bucket") or "*"),
                str(estimate.get("factor_group") or "*"),
                _safe_int(estimate.get("n_obs"), 0),
                _safe_float(estimate.get("mean_realized_edge"), 0.0),
                _safe_float(estimate.get("positive_rate"), 0.0),
                max(1, _safe_int(estimate.get("half_life_ms"), 1)),
                max(0, _safe_int(estimate.get("max_useful_age_ms"), 0)),
                _safe_float(estimate.get("capacity_estimate"), 0.0),
                _safe_float(estimate.get("crowding_penalty"), 0.0),
                _safe_float(estimate.get("size_multiplier"), 0.0),
                1 if bool(estimate.get("block_signal")) else 0,
                json.dumps(detail, separators=(",", ":"), sort_keys=True),
            ),
        )
        for age_row in age_edges.get(str(estimate.get("cohort_key") or ""), []) or []:
            con.execute(
                f"""
                INSERT INTO {AGE_EDGES_TABLE}(
                  run_id, cohort_key, age_bucket_ms, n_obs, mean_realized_edge,
                  positive_rate, avg_total_cost_bps, avg_capacity_unit, detail_json
                )
                VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(run_id),
                    str(estimate.get("cohort_key") or ""),
                    _safe_int(age_row.get("age_bucket_ms"), 0),
                    _safe_int(age_row.get("n_obs"), 0),
                    _safe_float(age_row.get("mean_realized_edge"), 0.0),
                    _safe_float(age_row.get("positive_rate"), 0.0),
                    _safe_float(age_row.get("avg_total_cost_bps"), 0.0),
                    _safe_float(age_row.get("avg_capacity_unit"), 0.0),
                    json.dumps(dict(age_row), separators=(",", ":"), sort_keys=True),
                ),
            )
    return int(run_id)


def train_learned_alpha_decay(
    con=None,
    *,
    lookback_days: int = LOOKBACK_DAYS,
    min_samples: int = MIN_SAMPLES,
    age_bucket_ms: int = AGE_BUCKET_MS,
    now_ms: Optional[int] = None,
) -> Dict[str, Any]:
    owns = con is None
    if con is None:
        con = connect()
    try:
        ensure_schema(con)
        run_ts_ms = int(now_ms if now_ms is not None else _now_ms())
        observations = load_realized_edge_observations(con, lookback_days=int(lookback_days), now_ms=int(run_ts_ms))
        if len(observations) < int(min_samples):
            return {
                "ok": False,
                "status": "insufficient_observations",
                "observations": int(len(observations)),
                "min_samples": int(min_samples),
            }
        estimates, age_edges = _aggregate_estimates(
            observations,
            min_samples=int(min_samples),
            age_bucket_ms=max(1, int(age_bucket_ms)),
            run_ts_ms=int(run_ts_ms),
        )
        if not estimates:
            return {
                "ok": False,
                "status": "no_estimates",
                "observations": int(len(observations)),
                "min_samples": int(min_samples),
            }
        run_id = _store_estimates(
            con,
            ts_ms=int(run_ts_ms),
            lookback_days=int(lookback_days),
            min_samples=int(min_samples),
            age_bucket_ms=max(1, int(age_bucket_ms)),
            observations_n=int(len(observations)),
            estimates=estimates,
            age_edges=age_edges,
        )
        try:
            con.commit()
        except Exception as error:
            _warn_nonfatal(
                "LEARNED_ALPHA_COMMIT_FAILED",
                error,
                once_key=f"train_commit:{run_id}",
                run_id=int(run_id),
            )
        return {
            "ok": True,
            "status": "trained",
            "run_id": int(run_id),
            "observations": int(len(observations)),
            "estimates": int(len(estimates)),
            "age_edge_rows": int(sum(len(rows or []) for rows in age_edges.values())),
        }
    finally:
        if owns:
            con.close()


def _latest_run_id(con) -> Optional[int]:
    if not _table_exists(con, RUNS_TABLE):
        return None
    try:
        row = con.execute(
            f"""
            SELECT id, ts_ms
            FROM {RUNS_TABLE}
            ORDER BY ts_ms DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
    except Exception:
        return None
    if not row:
        return None
    run_id = _safe_int(row[0], 0)
    ts_ms = _safe_int(row[1], 0)
    if ts_ms > 0 and (_now_ms() - ts_ms) > int(MAX_LOOKUP_AGE_MS):
        return None
    return int(run_id) if run_id > 0 else None


def _lookup_keys(dims: Mapping[str, str]) -> List[str]:
    out: List[str] = []
    dim_names = ("model_family", "symbol", "regime", "liquidity_bucket", "spread_bucket", "volatility_bucket", "factor_group")
    normalized = {name: str(dims.get(name) or "*") for name in dim_names}
    for _, retained_dims in _AGGREGATION_LEVELS:
        reduced = {name: (normalized[name] if name in retained_dims else "*") for name in dim_names}
        key = _cohort_key(reduced)
        if key not in out:
            out.append(key)
    return out


def load_learned_alpha_estimate(con, payload: Mapping[str, Any]) -> Dict[str, Any]:
    if os.environ.get("USE_LEARNED_ALPHA_DECAY", "1") != "1":
        return {"available": False, "reason": "disabled"}
    if not _table_exists(con, ESTIMATES_TABLE):
        return {"available": False, "reason": "table_missing"}
    run_id = _latest_run_id(con)
    if not run_id:
        return {"available": False, "reason": "no_fresh_run"}
    dims = cohort_dimensions_from_payload(payload)
    for key in _lookup_keys(dims):
        try:
            row = con.execute(
                f"""
                SELECT run_id, ts_ms, cohort_key, cohort_level, model_family, symbol, regime,
                       liquidity_bucket, spread_bucket, volatility_bucket, factor_group,
                       n_obs, mean_realized_edge, positive_rate, half_life_ms,
                       max_useful_age_ms, capacity_estimate, crowding_penalty,
                       size_multiplier, block_signal, detail_json
                FROM {ESTIMATES_TABLE}
                WHERE run_id=? AND cohort_key=?
                LIMIT 1
                """,
                (int(run_id), str(key)),
            ).fetchone()
        except Exception as e:
            _warn_nonfatal("LEARNED_ALPHA_LOOKUP_FAILED", e, once_key="estimate_lookup")
            return {"available": False, "reason": "lookup_failed"}
        if not row:
            continue
        detail = _json_dict(row[20])
        return {
            "available": True,
            "run_id": _safe_int(row[0], 0),
            "ts_ms": _safe_int(row[1], 0),
            "cohort_key": str(row[2] or ""),
            "cohort_level": str(row[3] or ""),
            "model_family": str(row[4] or ""),
            "symbol": str(row[5] or ""),
            "regime": str(row[6] or ""),
            "liquidity_bucket": str(row[7] or ""),
            "spread_bucket": str(row[8] or ""),
            "volatility_bucket": str(row[9] or ""),
            "factor_group": str(row[10] or ""),
            "n_obs": _safe_int(row[11], 0),
            "mean_realized_edge": _safe_float(row[12], 0.0),
            "positive_rate": _safe_float(row[13], 0.0),
            "half_life_ms": max(1, _safe_int(row[14], 1)),
            "max_useful_age_ms": max(0, _safe_int(row[15], 0)),
            "capacity_estimate": _safe_float(row[16], 0.0),
            "crowding_penalty": _safe_float(row[17], 0.0),
            "size_multiplier": _safe_float(row[18], 0.0),
            "block_signal": bool(_safe_int(row[19], 0)),
            "detail": detail,
            "lookup_dims": dims,
        }
    return {"available": False, "reason": "no_matching_cohort", "lookup_dims": dims}


def learned_alpha_size_multiplier(estimate: Mapping[str, Any]) -> float:
    if not bool((estimate or {}).get("available")):
        return 1.0
    if bool((estimate or {}).get("block_signal")):
        return 0.0
    raw = (estimate or {}).get("size_multiplier")
    return float(_clamp(_safe_float(raw, 1.0 if raw is None else 0.0), 0.0, 1.0))


def execution_adjustment_for_order(
    con,
    order: Mapping[str, Any],
    *,
    age_ms: int,
    ttl_ms: int,
    half_life_ms: int,
    now_ms: Optional[int] = None,
) -> Dict[str, Any]:
    del now_ms
    estimate = dict(order.get("learned_alpha_decay") or {}) if isinstance(order.get("learned_alpha_decay"), dict) else {}
    if not estimate:
        estimate = load_learned_alpha_estimate(con, order)
    if not bool(estimate.get("available")):
        return {
            "available": False,
            "blocked": False,
            "reason": str(estimate.get("reason") or "unavailable"),
            "ttl_ms": int(ttl_ms),
            "half_life_ms": int(half_life_ms),
            "size_multiplier": 1.0,
            "estimate": estimate,
        }
    max_useful_age_ms = max(0, _safe_int(estimate.get("max_useful_age_ms"), 0))
    learned_half_life_ms = max(1, _safe_int(estimate.get("half_life_ms"), int(half_life_ms)))
    capacity_estimate = _safe_float(estimate.get("capacity_estimate"), 0.0)
    size_multiplier = learned_alpha_size_multiplier(estimate)
    if max_useful_age_ms > 0 and int(age_ms) >= int(max_useful_age_ms):
        return {
            "available": True,
            "blocked": True,
            "reason": "learned_alpha_stale",
            "ttl_ms": int(min(max(1, ttl_ms), max_useful_age_ms)),
            "half_life_ms": int(min(max(1, half_life_ms), learned_half_life_ms)),
            "size_multiplier": 0.0,
            "estimate": estimate,
        }
    if capacity_estimate < float(MIN_CAPACITY):
        return {
            "available": True,
            "blocked": True,
            "reason": "learned_alpha_low_capacity",
            "ttl_ms": int(ttl_ms),
            "half_life_ms": int(min(max(1, half_life_ms), learned_half_life_ms)),
            "size_multiplier": 0.0,
            "estimate": estimate,
        }
    if bool(estimate.get("block_signal")) or size_multiplier <= float(MIN_SIZE_MULTIPLIER):
        return {
            "available": True,
            "blocked": True,
            "reason": "learned_alpha_crowded_or_no_edge",
            "ttl_ms": int(ttl_ms),
            "half_life_ms": int(min(max(1, half_life_ms), learned_half_life_ms)),
            "size_multiplier": 0.0,
            "estimate": estimate,
        }
    effective_ttl = int(ttl_ms)
    if max_useful_age_ms > 0:
        effective_ttl = int(min(max(1, int(ttl_ms)), int(max_useful_age_ms)))
    effective_half_life = int(min(max(1, int(half_life_ms)), int(learned_half_life_ms)))
    return {
        "available": True,
        "blocked": False,
        "reason": "learned_alpha_applied",
        "ttl_ms": int(effective_ttl),
        "half_life_ms": int(effective_half_life),
        "size_multiplier": float(size_multiplier),
        "estimate": estimate,
    }


def portfolio_adjustment_for_intent(
    con,
    intent: Mapping[str, Any],
    *,
    signal_age_ms: int,
    now_ms: Optional[int] = None,
) -> Dict[str, Any]:
    ttl_ms = max(1, _safe_int(intent.get("alpha_ttl_ms"), int(5 * 60 * 1000)))
    half_life_ms = max(1, _safe_int(intent.get("alpha_half_life_ms"), int(90 * 1000)))
    return execution_adjustment_for_order(
        con,
        intent,
        age_ms=int(signal_age_ms),
        ttl_ms=int(ttl_ms),
        half_life_ms=int(half_life_ms),
        now_ms=now_ms,
    )


def champion_gate_for_candidate(con, candidate: Mapping[str, Any]) -> Dict[str, Any]:
    payload = dict(candidate or {})
    meta = dict(payload.get("meta") or {}) if isinstance(payload.get("meta"), dict) else {}
    payload = {
        **meta,
        **payload,
        "model_family": meta.get("model_family") or payload.get("model_family"),
        "model_name": payload.get("model_name") or meta.get("model_name"),
        "model_id": payload.get("model_id") or meta.get("model_id"),
        "symbol": payload.get("symbol") or meta.get("symbol") or "*",
        "regime": payload.get("regime") or meta.get("regime") or "global",
        "liquidity_bucket": meta.get("liquidity_bucket") or meta.get("liquidity_regime"),
        "volatility_bucket": meta.get("volatility_bucket") or meta.get("volatility_regime"),
        "factor_group": meta.get("factor_group") or meta.get("feature_group"),
    }
    estimate = load_learned_alpha_estimate(con, payload)
    if not bool(estimate.get("available")):
        return {"allowed": True, "available": False, "reason": str(estimate.get("reason") or "unavailable"), "estimate": estimate}
    size_multiplier = learned_alpha_size_multiplier(estimate)
    blocked = bool(
        bool(estimate.get("block_signal"))
        or _safe_float(estimate.get("capacity_estimate"), 0.0) < float(MIN_CAPACITY)
        or size_multiplier <= float(MIN_SIZE_MULTIPLIER)
    )
    return {
        "allowed": not blocked,
        "available": True,
        "reason": "learned_alpha_allowed" if not blocked else "learned_alpha_gate_blocked",
        "score_multiplier": float(size_multiplier),
        "estimate": estimate,
    }


def _preflight_smoke_enabled() -> bool:
    return str(os.environ.get("PREFLIGHT_SMOKE", "") or "").strip().lower() in {"1", "true", "yes", "on"}


def main() -> int:
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("train_learned_alpha_decay must be launched by supervisor")
        return 1
    init_db()
    init_schema()
    con = connect()
    try:
        result = train_learned_alpha_decay(con)
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("LEARNED_ALPHA_CLOSE_FAILED", e, once_key="main_close")
    if bool(result.get("ok")):
        print(
            "[learned_alpha_decay] trained "
            f"run_id={int(result.get('run_id') or 0)} "
            f"observations={int(result.get('observations') or 0)} "
            f"estimates={int(result.get('estimates') or 0)}"
        )
        return 0
    message = (
        f"[learned_alpha_decay] {str(result.get('status') or 'failed')}: "
        f"{int(result.get('observations') or 0)} < {int(result.get('min_samples') or MIN_SAMPLES)}"
    )
    print(message)
    if _preflight_smoke_enabled():
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
