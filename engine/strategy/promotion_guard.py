"""
FILE: promotion_guard.py

Human-readable purpose:
Blocks or allows model promotion based on runtime safety, drift, alerts, and
evaluation quality thresholds. This is the final gate before a candidate model
can be treated as promotion-eligible.
"""

import math
import os
import logging
import time
from statistics import NormalDist
from typing import Dict, Any, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import (
    connect,
    fetch_latest_backtest_cpcv_run,
    init_db,
    record_hypothesis_result,
)
from engine.strategy.cpcv import cpcv_config_from_env
from engine.strategy.statistical_gates import (
    passes_promotion_gate,
    promotion_gate_config_from_env,
)
from engine.strategy.promotion_audit import record_statistical_evidence
from engine.strategy.feature_neutralization import (
    extract_feature_rows,
    feature_neutral_ic,
    neutralize_feature_ids,
)
from engine.strategy.era_boost import (
    coerce_optional_labels as _shared_coerce_optional_labels,
    era_labels_for as _shared_era_labels_for,
    month_label as _shared_month_label,
)
from engine.strategy.statistics.factor_threshold import (
    FactorThresholdResult,
    harvey_liu_zhu_threshold_result,
)
from engine.strategy.statistics.multiple_testing import benjamini_hochberg
from engine.strategy.statistics.reality_check import white_reality_check

# ------            -- ------------------------------------------------------
# Global enable switch (env default ON)
# ------            -- ------------------------------------------------------

PROMOTION_ENABLED_ENV = os.environ.get("PROMOTION_ENABLED", "1") == "1"

# ------            -- ------------------------------------------------------
# Guard thresholds
# ------            -- ------------------------------------------------------

PROMOTION_CRIT_ALERT_LOOKBACK_S = int(os.environ.get("PROMOTION_CRIT_ALERT_LOOKBACK_S", "7200"))  # 2h
PROMOTION_MAX_CRIT_ALERTS = int(os.environ.get("PROMOTION_MAX_CRIT_ALERTS", "0"))  # 0 => any CRIT blocks

PROMOTION_MAX_DRIFT_RATIO = float(os.environ.get("PROMOTION_MAX_DRIFT_RATIO", "0.0"))  # 0 disables
PROMOTION_DRIFT_LOOKBACK_S = int(os.environ.get("PROMOTION_DRIFT_LOOKBACK_S", "86400"))  # 24h

PROMOTION_EQUITY_DRIFT_LOOKBACK_S = int(
    os.environ.get("PROMOTION_EQUITY_DRIFT_LOOKBACK_S", "7200")
)  # 2h
PROMOTION_BLOCK_IF_EQUITY_CRIT = os.environ.get(
    "PROMOTION_BLOCK_IF_EQUITY_CRIT", "1"
) == "1"

# ------            -- ------------------------------------------------------
# Logging
# ------            -- ------------------------------------------------------

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [promotion_guard] %(message)s",
)
LOG = logging.getLogger("promotion_guard")
_NORMAL = NormalDist()
_EPS = 1e-12

# ------            -- ------------------------------------------------------
# Coverage / sanity thresholds (metric-based promotion)
# ------            -- ------------------------------------------------------

MIN_EVAL_ROWS = int(os.environ.get("PROMOTE_MIN_EVAL_ROWS", "200"))
MAX_ABS_RMSE = float(os.environ.get("PROMOTE_MAX_ABS_RMSE", "10.0"))
MAX_ABS_BIAS = float(os.environ.get("PROMOTE_MAX_ABS_BIAS", "5.0"))

# ------            -- ------------------------------------------------------
# Time helper
# ------            -- ------------------------------------------------------

def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(event: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOG,
        event=event,
        code=event,
        message=event,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.promotion_guard",
        extra=extra,
        persist=False,
    )


def _table_exists(con, table_name: str) -> bool:
    try:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (str(table_name),),
        ).fetchone()
        return bool(row)
    except Exception as e:
        _warn_nonfatal("promotion_guard_table_exists_failed", e, table_name=str(table_name))
        return False


def _warn_state(event: str, message: str, **extra: Any) -> None:
    log_failure(
        LOG,
        event=event,
        code=event,
        message=message,
        error=None,
        level=logging.WARNING,
        component="engine.strategy.promotion_guard",
        extra=extra,
        persist=False,
    )


def _finite_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception as exc:
        _warn_nonfatal(
            "PROMOTION_GUARD_FLOAT_PARSE_FAILED",
            exc,
            once_key=f"finite_float:{repr(value)[:80]}",
            value_repr=repr(value),
        )
        return None
    return float(out) if math.isfinite(out) else None


def _clean_numeric_series(values: Any) -> list[float]:
    if values is None:
        return []
    out: list[float] = []
    if isinstance(values, (str, bytes)):
        raw_iter = [values]
    else:
        try:
            raw_iter = list(values)
        except Exception as exc:
            _warn_nonfatal(
                "PROMOTION_GUARD_SERIES_ITER_FAILED",
                exc,
                once_key=f"series_iter:{type(values).__name__}",
                value_type=type(values).__name__,
            )
            raw_iter = [values]
    for raw in raw_iter:
        try:
            value = float(raw)
        except Exception as exc:
            _warn_nonfatal(
                "PROMOTION_GUARD_SERIES_VALUE_PARSE_FAILED",
                exc,
                once_key=f"series_value:{repr(raw)[:80]}",
                value_repr=repr(raw),
            )
            continue
        if math.isfinite(value):
            out.append(float(value))
    return out


def _safe_float_env(name: str, default: float) -> float:
    raw = os.environ.get(name, "")
    if raw in (None, ""):
        return float(default)
    try:
        value = float(raw)
    except Exception as e:
        _warn_nonfatal(
            "PROMOTION_GUARD_FLOAT_ENV_PARSE_FAILED",
            e,
            once_key=f"float_env:{name}",
            env_name=str(name),
            raw_value=str(raw),
        )
        return float(default)
    return float(value) if math.isfinite(value) else float(default)


def _safe_bool_env(name: str, default: bool) -> bool:
    raw = str(os.environ.get(name, "1" if default else "0") or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "y", "on"}


def _safe_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name, "")
    if raw in (None, ""):
        return int(default)
    try:
        return int(raw)
    except Exception as e:
        _warn_nonfatal(
            "PROMOTION_GUARD_INT_ENV_PARSE_FAILED",
            e,
            once_key=f"int_env:{name}",
            env_name=str(name),
            raw_value=str(raw),
        )
        return int(default)


def _series_sharpe(values: Any) -> float:
    arr = _clean_numeric_series(values)
    n = int(len(arr))
    if n <= 1:
        return 0.0
    mean = sum(arr) / float(n)
    variance = sum((value - mean) ** 2 for value in arr) / float(max(1, n - 1))
    std = math.sqrt(max(0.0, variance))
    if std <= _EPS:
        if mean > 0.0:
            return 10.0
        if mean < 0.0:
            return -10.0
        return 0.0
    return float(mean / std)


def _align_series(left: list[float], right: list[float]) -> tuple[list[float], list[float]]:
    n = min(int(len(left)), int(len(right)))
    if n <= 0:
        return [], []
    return list(left[:n]), list(right[:n])


def _pearson_corr(left: list[float], right: list[float]) -> float | None:
    x, y = _align_series(left, right)
    n = int(len(x))
    if n < 2:
        return None
    mean_x = sum(x) / float(n)
    mean_y = sum(y) / float(n)
    dx = [value - mean_x for value in x]
    dy = [value - mean_y for value in y]
    var_x = sum(value * value for value in dx)
    var_y = sum(value * value for value in dy)
    denom = math.sqrt(max(0.0, var_x * var_y))
    if denom <= _EPS:
        return 1.0 if all(abs(a - b) <= _EPS for a, b in zip(x, y)) else 0.0
    return float(max(-1.0, min(1.0, sum(a * b for a, b in zip(dx, dy)) / denom)))


def _series_ic(predictions: Any, realized: Any) -> float | None:
    corr = _pearson_corr(_clean_numeric_series(predictions), _clean_numeric_series(realized))
    return None if corr is None else float(corr)


def _feature_neutral_ic_payload(
    *,
    challenger_predictions: Any,
    realized_returns: Any,
    neutralization_features: Any = None,
    candidate_features: Any = None,
) -> Dict[str, Any]:
    predictions = _clean_numeric_series(challenger_predictions)
    realized = _clean_numeric_series(realized_returns)
    feature_ids = neutralize_feature_ids()
    feature_rows = extract_feature_rows(neutralization_features, feature_ids)
    if not feature_rows:
        feature_rows = extract_feature_rows(candidate_features, feature_ids)
    n = min(int(len(predictions)), int(len(realized)), int(len(feature_rows)))
    threshold = _safe_float_env("PROMOTION_MAX_FACTOR_DEPENDENCE", 0.10)
    if n < 8:
        return {
            "applied": False,
            "passed": True,
            "flagged": False,
            "status": "insufficient_aligned_rows",
            "n": int(n),
            "min_n": 8,
            "feature_ids": list(feature_ids),
            "max_factor_dependence": float(threshold),
        }
    metric = feature_neutral_ic(
        predictions[:n],
        realized[:n],
        feature_rows[:n],
        feature_ids=feature_ids,
        min_symbols=8,
    )
    gap = float(metric.get("raw_minus_fnc") or 0.0)
    flagged = bool(gap > float(threshold))
    return {
        **dict(metric),
        "applied": bool(metric.get("applied")),
        "passed": True,
        "flagged": bool(flagged),
        "status": "flagged_factor_dependence" if flagged else "evaluated",
        "max_factor_dependence": float(threshold),
        "log_only": True,
    }


def _stddev(values: list[float]) -> float:
    arr = [float(value) for value in list(values or []) if math.isfinite(float(value))]
    n = int(len(arr))
    if n <= 1:
        return 0.0
    mean = sum(arr) / float(n)
    return float(math.sqrt(sum((value - mean) ** 2 for value in arr) / float(n - 1)))


def _bottom_quartile_mean(values: list[float]) -> float | None:
    arr = sorted(float(value) for value in list(values or []) if math.isfinite(float(value)))
    if not arr:
        return None
    count = max(1, int(math.ceil(float(len(arr)) * 0.25)))
    bucket = arr[:count]
    return float(sum(bucket) / float(len(bucket)))


def _month_label(ts_ms: Any) -> str:
    return _shared_month_label(ts_ms)


def _coerce_optional_labels(values: Any) -> list[str]:
    return _shared_coerce_optional_labels(values)


def _era_labels_for(
    *,
    n_obs: int,
    timestamps: Any = None,
    era_labels: Any = None,
    regime_labels: Any = None,
) -> tuple[list[str], Dict[str, Any]]:
    labels, diagnostics = _shared_era_labels_for(
        n_obs=int(n_obs),
        timestamps=timestamps,
        era_labels=era_labels,
        regime_labels=regime_labels,
    )
    return labels, dict(diagnostics)


def _era_table(
    *,
    returns: list[float],
    labels: list[str],
    predictions: Any = None,
    realized: Any = None,
    min_obs: int = 2,
) -> list[Dict[str, Any]]:
    n = min(int(len(returns)), int(len(labels)))
    pred = _clean_numeric_series(predictions)
    actual = _clean_numeric_series(realized)
    by_era: Dict[str, Dict[str, list[float]]] = {}
    for idx in range(n):
        label = str(labels[idx] or "unknown")
        bucket = by_era.setdefault(label, {"returns": [], "predictions": [], "realized": []})
        bucket["returns"].append(float(returns[idx]))
        if idx < len(pred) and idx < len(actual):
            bucket["predictions"].append(float(pred[idx]))
            bucket["realized"].append(float(actual[idx]))

    rows: list[Dict[str, Any]] = []
    for label in sorted(by_era.keys()):
        bucket = by_era[label]
        ret = list(bucket.get("returns") or [])
        if len(ret) < int(max(1, min_obs)):
            continue
        ic = _series_ic(bucket.get("predictions"), bucket.get("realized")) if bucket.get("predictions") else None
        rows.append(
            {
                "era": str(label),
                "n_obs": int(len(ret)),
                "cost_adjusted_sharpe": float(_series_sharpe(ret)),
                "ic": (None if ic is None else float(ic)),
                "pnl": float(sum(ret)),
                "mean_return": float(sum(ret) / float(len(ret))) if ret else 0.0,
            }
        )
    return rows


def _era_regime_robustness_gate(
    *,
    challenger_series: list[float],
    champion_series: list[float],
    evaluation_timestamps: Any = None,
    era_labels: Any = None,
    regime_labels: Any = None,
    challenger_predictions: Any = None,
    realized_returns: Any = None,
) -> tuple[Dict[str, Any], bool]:
    min_obs = max(2, _safe_int_env("PROMOTION_MIN_ERA_OBS", 2))
    labels, label_diag = _era_labels_for(
        n_obs=len(challenger_series),
        timestamps=evaluation_timestamps,
        era_labels=era_labels,
        regime_labels=regime_labels,
    )
    if not bool(label_diag.get("applied")):
        return dict(label_diag), True

    challenger_rows = _era_table(
        returns=list(challenger_series),
        labels=list(labels),
        predictions=challenger_predictions,
        realized=realized_returns,
        min_obs=int(min_obs),
    )
    champion_rows = _era_table(
        returns=list(champion_series[: len(labels)]),
        labels=list(labels[: len(champion_series)]),
        min_obs=int(min_obs),
    )
    challenger_sharpes = [float(row.get("cost_adjusted_sharpe") or 0.0) for row in challenger_rows]
    champion_sharpes = [float(row.get("cost_adjusted_sharpe") or 0.0) for row in champion_rows]
    challenger_worst = _bottom_quartile_mean(challenger_sharpes)
    champion_worst = _bottom_quartile_mean(champion_sharpes)

    env_threshold_raw = os.environ.get("PROMOTION_MIN_WORST_ERA_SHARPE")
    if env_threshold_raw not in (None, ""):
        min_worst = _safe_float_env("PROMOTION_MIN_WORST_ERA_SHARPE", 0.0)
        threshold_source = "env"
    elif champion_worst is not None:
        min_worst = float(champion_worst)
        threshold_source = "champion_worst_quartile"
    else:
        min_worst = float("-inf")
        threshold_source = "unbounded_no_champion_context"

    max_std = _safe_float_env("PROMOTION_MAX_ERA_STD", 2.0)
    std_log_only = _safe_bool_env("PROMOTION_ERA_STD_LOG_ONLY", True)
    era_std = _stddev(challenger_sharpes)
    enough_eras = len(challenger_rows) >= 1
    worst_passed = bool(challenger_worst is not None and float(challenger_worst) >= float(min_worst))
    std_within_threshold = bool(era_std <= float(max_std))
    std_passed = bool(std_within_threshold or std_log_only)
    passed = bool(enough_eras and worst_passed and std_passed)
    status = "evaluated"
    if not enough_eras:
        status = "insufficient_eras"
    elif not worst_passed:
        status = "worst_quartile_sharpe_below_threshold"
    elif not std_within_threshold and std_log_only:
        status = "era_std_above_threshold_log_only"
    elif not std_within_threshold:
        status = "era_std_above_threshold"

    payload: Dict[str, Any] = {
        "applied": True,
        "status": status,
        "passed": bool(passed),
        "bucket_mode": str(label_diag.get("bucket_mode") or ""),
        "min_era_obs": int(min_obs),
        "era_count": int(len(challenger_rows)),
        "champion_era_count": int(len(champion_rows)),
        "worst_quartile_sharpe": (None if challenger_worst is None else float(challenger_worst)),
        "champion_worst_quartile_sharpe": (None if champion_worst is None else float(champion_worst)),
        "min_worst_era_sharpe": (None if not math.isfinite(float(min_worst)) else float(min_worst)),
        "min_worst_era_sharpe_source": str(threshold_source),
        "era_score_std": float(era_std),
        "max_era_std": float(max_std),
        "era_std_log_only": bool(std_log_only),
        "era_std_within_threshold": bool(std_within_threshold),
        "eras": challenger_rows,
        "champion_eras": champion_rows,
    }
    return payload, bool(passed)


def _iter_return_series_map(values: Any) -> list[tuple[str, list[float]]]:
    if values is None:
        return []
    rows: list[tuple[str, Any]] = []
    if isinstance(values, dict):
        rows = [(str(label), raw) for label, raw in values.items()]
    elif isinstance(values, (list, tuple)):
        if values and all(not isinstance(item, (list, tuple, dict)) for item in values):
            rows = [("pool", values)]
        else:
            rows = [(f"pool_{idx}", raw) for idx, raw in enumerate(values, start=1)]
    else:
        rows = [("pool", values)]

    out: list[tuple[str, list[float]]] = []
    for label, raw in rows:
        series = _clean_numeric_series(raw)
        if len(series) >= 2:
            out.append((str(label or f"pool_{len(out) + 1}"), series))
    return out


def _equally_weighted_pool_series(
    values: Any,
    *,
    exclude_labels: set[str] | None = None,
) -> tuple[list[float], list[str]]:
    excluded = {str(label or "").strip() for label in set(exclude_labels or set()) if str(label or "").strip()}
    series_rows = [
        (label, series)
        for label, series in _iter_return_series_map(values)
        if str(label or "").strip() not in excluded
    ]
    if not series_rows:
        return [], []
    n = min(len(series) for _label, series in series_rows)
    if n < 2:
        return [], []
    labels = [str(label) for label, _series in series_rows]
    pool = [
        sum(float(series[idx]) for _label, series in series_rows) / float(len(series_rows))
        for idx in range(int(n))
    ]
    return pool, labels


def _portfolio_blend_metrics(
    *,
    challenger_series: list[float],
    baseline_series: list[float],
    sleeve_weight: float,
) -> Dict[str, Any]:
    base, challenger = _align_series(list(baseline_series), list(challenger_series))
    n = int(len(base))
    if n < 2:
        return {
            "applied": False,
            "status": "insufficient_aligned_returns",
            "passed": True,
            "n_obs": int(n),
        }
    weight = max(0.0, min(1.0, float(sleeve_weight)))
    with_challenger = [
        ((1.0 - weight) * float(base_value)) + (weight * float(challenger_value))
        for base_value, challenger_value in zip(base, challenger)
    ]
    baseline_sharpe = _series_sharpe(base)
    with_challenger_sharpe = _series_sharpe(with_challenger)
    baseline_pnl = float(sum(base))
    with_challenger_pnl = float(sum(with_challenger))
    return {
        "applied": True,
        "status": "evaluated",
        "n_obs": int(n),
        "sleeve_weight": float(weight),
        "baseline_sharpe": float(baseline_sharpe),
        "with_challenger_sharpe": float(with_challenger_sharpe),
        "marginal_sharpe": float(with_challenger_sharpe - baseline_sharpe),
        "baseline_pnl": float(baseline_pnl),
        "with_challenger_pnl": float(with_challenger_pnl),
        "marginal_pnl": float(with_challenger_pnl - baseline_pnl),
    }


def _pool_contribution_gates(
    *,
    challenger_series: list[float],
    champion_series: list[float],
    model_id: str,
    model_name: str,
    candidate_version: str,
    models_returns: Any = None,
    pool_returns: Any = None,
    max_pool_corr: float | None = None,
    corr_sharpe_uplift: float | None = None,
    min_mpc: float | None = None,
    mpc_weight: float | None = None,
) -> tuple[Dict[str, Any], Dict[str, Any], bool]:
    context = pool_returns if pool_returns is not None else models_returns
    threshold = float(
        max_pool_corr
        if max_pool_corr is not None
        else _safe_float_env("PROMOTION_MAX_POOL_CORR", 0.70)
    )
    uplift = float(
        corr_sharpe_uplift
        if corr_sharpe_uplift is not None
        else _safe_float_env("PROMOTION_CORR_SHARPE_UPLIFT", 0.10)
    )
    min_mpc_value = float(min_mpc if min_mpc is not None else _safe_float_env("PROMOTION_MIN_MPC", 0.0))
    sleeve = float(mpc_weight if mpc_weight is not None else _safe_float_env("PROMOTION_MPC_WEIGHT", 0.10))
    excluded = {
        str(model_id or "").strip(),
        str(model_name or "").strip(),
        str(candidate_version or "").strip(),
        "challenger",
    }
    pool_series, pool_labels = _equally_weighted_pool_series(context, exclude_labels=excluded)

    if context is None:
        skipped = {
            "applied": False,
            "status": "no_pool_context",
            "passed": True,
            "max_pool_corr": float(threshold),
        }
        mpc_skipped = {
            "applied": False,
            "status": "no_pool_context",
            "passed": True,
            "min_mpc": float(min_mpc_value),
            "sleeve_weight": float(max(0.0, min(1.0, sleeve))),
        }
        return skipped, mpc_skipped, True

    champion_corr = _pearson_corr(challenger_series, champion_series)
    pool_corr = _pearson_corr(challenger_series, pool_series)
    corr_values = [value for value in (champion_corr, pool_corr) if value is not None]
    challenger_sharpe = _series_sharpe(challenger_series)
    champion_sharpe = _series_sharpe(champion_series)
    pool_sharpe = _series_sharpe(pool_series)
    incumbent_sharpe = champion_sharpe if champion_series else pool_sharpe
    max_corr = max(corr_values) if corr_values else 0.0
    high_corr = bool(corr_values and float(max_corr) > float(threshold))
    required_uplift_sharpe = float(incumbent_sharpe) * (1.0 + max(0.0, float(uplift)))
    uplift_override = bool(high_corr and float(challenger_sharpe) >= float(required_uplift_sharpe))
    corr_passed = bool((not high_corr) or uplift_override)
    corr_payload = {
        "applied": bool(corr_values),
        "status": (
            "high_correlation_uplift_override"
            if uplift_override
            else ("high_correlation_without_uplift" if high_corr else ("evaluated" if corr_values else "insufficient_aligned_returns"))
        ),
        "passed": bool(corr_passed),
        "champion_corr": (None if champion_corr is None else float(champion_corr)),
        "pool_corr": (None if pool_corr is None else float(pool_corr)),
        "max_correlation": float(max_corr),
        "max_pool_corr": float(threshold),
        "corr_sharpe_uplift": float(uplift),
        "challenger_sharpe": float(challenger_sharpe),
        "incumbent_sharpe": float(incumbent_sharpe),
        "required_uplift_sharpe": float(required_uplift_sharpe),
        "uplift_override": bool(uplift_override),
        "pool_model_labels": list(pool_labels),
    }

    baseline = pool_series if pool_series else champion_series
    mpc_payload = _portfolio_blend_metrics(
        challenger_series=challenger_series,
        baseline_series=baseline,
        sleeve_weight=float(sleeve),
    )
    mpc_payload.update(
        {
            "min_mpc": float(min_mpc_value),
            "baseline_source": "pool" if pool_series else ("champion" if champion_series else "none"),
            "pool_model_labels": list(pool_labels),
        }
    )
    if bool(mpc_payload.get("applied")):
        mpc_passed = bool(
            float(mpc_payload.get("marginal_sharpe") or 0.0) >= float(min_mpc_value)
            and float(mpc_payload.get("marginal_pnl") or 0.0) >= -_EPS
        )
        mpc_payload["passed"] = bool(mpc_passed)
        if not mpc_passed:
            mpc_payload["status"] = "non_positive_marginal_contribution"
    else:
        mpc_payload["passed"] = True

    return corr_payload, mpc_payload, bool(corr_passed and bool(mpc_payload.get("passed")))


def _two_sided_normal_p_value(t_stat: float) -> float:
    if not math.isfinite(float(t_stat)):
        return 0.0 if float(t_stat) > 0.0 else 1.0
    tail = 1.0 - float(_NORMAL.cdf(abs(float(t_stat))))
    return float(max(0.0, min(1.0, 2.0 * tail)))


def _extract_feature_id(raw: Any) -> str:
    if isinstance(raw, dict):
        for key in ("feature_id", "id", "name", "feature"):
            text = str(raw.get(key) or "").strip()
            if text:
                return text
        return ""
    return str(raw or "").strip()


def _as_feature_records(
    *,
    candidate_features: Any = None,
    new_features: Any = None,
    current_feature_ids: Any = None,
    challenger_feature_ids: Any = None,
    feature_returns: Any = None,
    feature_p_values: Any = None,
    feature_t_stats: Any = None,
) -> list[dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    order: list[str] = []

    def ensure(fid: str) -> dict[str, Any]:
        key = str(fid or "").strip()
        if key not in records:
            records[key] = {"feature_id": key}
            order.append(key)
        return records[key]

    raw_new_features = list(new_features or [])
    if not raw_new_features and challenger_feature_ids is not None:
        current = {str(fid or "").strip() for fid in list(current_feature_ids or []) if str(fid or "").strip()}
        raw_new_features = [
            str(fid or "").strip()
            for fid in list(challenger_feature_ids or [])
            if str(fid or "").strip() and str(fid or "").strip() not in current
        ]

    for raw in raw_new_features:
        fid = _extract_feature_id(raw)
        if not fid:
            continue
        rec = ensure(fid)
        if isinstance(raw, dict):
            for key in ("p_value", "q_value", "t_stat", "n_obs"):
                if key in raw:
                    rec[key] = raw.get(key)
            for key in ("returns", "oos_returns", "return_series", "factor_returns"):
                if key in raw:
                    rec["returns"] = raw.get(key)
                    break

    for raw in list(candidate_features or []):
        fid = _extract_feature_id(raw)
        if not fid:
            continue
        rec = ensure(fid)
        if isinstance(raw, dict):
            for key in ("p_value", "q_value", "t_stat", "n_obs"):
                if key in raw:
                    rec[key] = raw.get(key)
            for key in ("returns", "oos_returns", "return_series", "factor_returns"):
                if key in raw:
                    rec["returns"] = raw.get(key)
                    break

    if isinstance(feature_returns, dict):
        for fid, values in feature_returns.items():
            ensure(str(fid)).setdefault("returns", values)
    if isinstance(feature_p_values, dict):
        for fid, value in feature_p_values.items():
            ensure(str(fid))["p_value"] = value
    elif feature_p_values is not None:
        for fid, value in zip(order, list(feature_p_values)):
            ensure(str(fid))["p_value"] = value
    if isinstance(feature_t_stats, dict):
        for fid, value in feature_t_stats.items():
            ensure(str(fid))["t_stat"] = value
    elif feature_t_stats is not None:
        for fid, value in zip(order, list(feature_t_stats)):
            ensure(str(fid))["t_stat"] = value

    return [records[fid] for fid in order if str(fid or "").strip()]


def assess_challenger(
    *,
    model_id: str | None = None,
    model_name: str | None = None,
    candidate_version: str | None = None,
    challenger_returns: Any = None,
    champion_returns: Any = None,
    models_returns: Any = None,
    pool_returns: Any = None,
    evaluation_timestamps: Any = None,
    era_labels: Any = None,
    regime_labels: Any = None,
    challenger_predictions: Any = None,
    realized_returns: Any = None,
    neutralization_features: Any = None,
    candidate_features: Any = None,
    new_features: Any = None,
    current_feature_ids: Any = None,
    challenger_feature_ids: Any = None,
    feature_returns: Any = None,
    feature_p_values: Any = None,
    feature_t_stats: Any = None,
    max_pool_corr: float | None = None,
    corr_sharpe_uplift: float | None = None,
    min_mpc: float | None = None,
    mpc_weight: float | None = None,
    alpha: float = 0.05,
    fdr_q: float = 0.10,
    random_state: int = 42,
    bootstrap_samples: int = 10_000,
    persist: bool = True,
    con=None,
) -> Tuple[bool, Dict[str, Any]]:
    """
    Non-bypassable statistical promotion assessment.

    The challenger must pass White's Reality Check against the incumbent.
    Newly introduced features must pass BH-FDR at q=0.10 and the
    Harvey-Liu-Zhu `|t| > 3.0` factor threshold.
    """

    model_key = str(model_id or "").strip()
    if not model_key:
        raise ValueError(
            "ambiguous_model_id: assess_challenger requires explicit model_id "
            f"model_name={str(model_name or '').strip() or '<missing>'} "
            f"candidate_version={str(candidate_version or '').strip() or '<missing>'}"
        )
    evidence_ts = _now_ms()
    diagnostics: Dict[str, Any] = {
        "enabled": True,
        "applied": True,
        "model_id": str(model_key),
        "model_name": str(model_name or ""),
        "candidate_version": str(candidate_version or ""),
        "alpha": float(alpha),
        "fdr_q": float(fdr_q),
        "random_state": int(random_state),
        "bootstrap_samples": int(bootstrap_samples),
        "tests": {},
        "evidence_ts": int(evidence_ts),
        "passed": False,
    }

    challenger_series = _clean_numeric_series(challenger_returns)
    champion_series = _clean_numeric_series(champion_returns)
    reality = white_reality_check(
        challenger_series,
        champion_series,
        alpha=float(alpha),
        bootstrap_samples=int(bootstrap_samples),
        random_state=int(random_state),
    )
    reality_payload = reality.to_dict(include_distribution=True)
    diagnostics["tests"]["white_reality_check"] = dict(reality_payload)
    reality_decision = "pass" if bool(reality.passed) else "fail"
    if persist:
        record_statistical_evidence(
            con=con,
            ts=int(evidence_ts),
            model_id=str(model_key),
            test_name="white_reality_check",
            t_stat=float(reality.observed_statistic),
            p_value=float(reality.p_value),
            bootstrap_samples=int(reality.bootstrap_samples),
            decision=str(reality_decision),
            payload=dict(reality_payload),
        )

    feature_records = _as_feature_records(
        candidate_features=candidate_features,
        new_features=new_features,
        current_feature_ids=current_feature_ids,
        challenger_feature_ids=challenger_feature_ids,
        feature_returns=feature_returns,
        feature_p_values=feature_p_values,
        feature_t_stats=feature_t_stats,
    )
    feature_gate_passed = True
    if feature_records:
        factor_results: list[FactorThresholdResult] = []
        p_values: list[float] = []
        labels: list[str] = []
        for rec in feature_records:
            fid = str(rec.get("feature_id") or "").strip()
            labels.append(fid)
            t_value = _finite_float_or_none(rec.get("t_stat"))
            returns = rec.get("returns")
            result: FactorThresholdResult | None = None
            try:
                if t_value is not None:
                    result = harvey_liu_zhu_threshold_result(
                        feature_id=fid,
                        t_stat=float(t_value),
                        n_obs=int(rec.get("n_obs") or 0),
                        threshold=3.0,
                    )
                else:
                    result = harvey_liu_zhu_threshold_result(
                        y=_clean_numeric_series(returns),
                        feature_id=fid,
                        threshold=3.0,
                    )
            except Exception as e:
                _warn_nonfatal(
                    "PROMOTION_GUARD_FACTOR_THRESHOLD_FAILED",
                    e,
                    model_id=str(model_key),
                    feature_id=str(fid),
                )
                result = FactorThresholdResult(
                    feature_id=fid,
                    t_stat=0.0,
                    p_value=1.0,
                    threshold=3.0,
                    passed=False,
                    n_obs=0,
                    lags=0,
                    beta=0.0,
                    standard_error=0.0,
                )
            factor_results.append(result)
            p_raw = _finite_float_or_none(rec.get("p_value"))
            p_values.append(float(result.p_value if p_raw is None else max(0.0, min(1.0, p_raw))))

        bh = benjamini_hochberg(p_values, q=float(fdr_q), labels=labels)
        bh_payload = bh.to_dict()
        q_by_feature = {
            str(label): float(q_value)
            for label, q_value in zip(labels, list(bh.q_values))
        }
        rejected_by_feature = {
            str(label): bool(float(q_by_feature.get(str(label), 1.0)) < float(fdr_q))
            for label in labels
        }
        factor_payloads = []
        for result in factor_results:
            payload = result.to_dict()
            payload["q_value"] = float(q_by_feature.get(str(result.feature_id), 1.0))
            payload["bh_rejected"] = bool(rejected_by_feature.get(str(result.feature_id), False))
            payload["decision_components"] = {
                "bh_fdr_pass": bool(payload["bh_rejected"]),
                "hlz_threshold_pass": bool(result.passed),
            }
            factor_payloads.append(payload)

        bh_passed = bool(feature_records) and all(bool(v) for v in rejected_by_feature.values())
        threshold_passed = bool(factor_results) and all(bool(result.passed) for result in factor_results)
        feature_gate_passed = bool(bh_passed and threshold_passed)
        diagnostics["tests"]["benjamini_hochberg_fdr"] = {
            **bh_payload,
            "passed": bool(bh_passed),
            "feature_q_values": q_by_feature,
            "feature_rejected": rejected_by_feature,
        }
        diagnostics["tests"]["harvey_liu_zhu_factor_threshold"] = {
            "passed": bool(threshold_passed),
            "features": factor_payloads,
        }

        if persist:
            record_statistical_evidence(
                con=con,
                ts=int(evidence_ts),
                model_id=str(model_key),
                test_name="benjamini_hochberg_fdr",
                p_value=float(max(p_values) if p_values else 1.0),
                q_value=float(max(q_by_feature.values()) if q_by_feature else 1.0),
                decision=("pass" if bool(bh_passed) else "fail"),
                payload=diagnostics["tests"]["benjamini_hochberg_fdr"],
            )
            for payload in factor_payloads:
                record_statistical_evidence(
                    con=con,
                    ts=int(evidence_ts),
                    model_id=str(model_key),
                    feature_id=str(payload.get("feature_id") or ""),
                    test_name="harvey_liu_zhu_factor_threshold",
                    t_stat=float(payload.get("t_stat") or 0.0),
                    p_value=float(payload.get("p_value") or 1.0),
                    q_value=float(payload.get("q_value") or 1.0),
                    decision=("pass" if bool(payload.get("decision_components", {}).get("bh_fdr_pass")) and bool(payload.get("passed")) else "fail"),
                    payload=payload,
                )
    else:
        diagnostics["tests"]["benjamini_hochberg_fdr"] = {"applied": False, "passed": True, "status": "no_new_features"}
        diagnostics["tests"]["harvey_liu_zhu_factor_threshold"] = {"applied": False, "passed": True, "status": "no_new_features"}

    pool_corr_payload, mpc_payload, pool_gates_passed = _pool_contribution_gates(
        challenger_series=challenger_series,
        champion_series=champion_series,
        model_id=str(model_key),
        model_name=str(model_name or ""),
        candidate_version=str(candidate_version or ""),
        models_returns=models_returns,
        pool_returns=pool_returns,
        max_pool_corr=max_pool_corr,
        corr_sharpe_uplift=corr_sharpe_uplift,
        min_mpc=min_mpc,
        mpc_weight=mpc_weight,
    )
    diagnostics["tests"]["pool_correlation"] = dict(pool_corr_payload)
    diagnostics["tests"]["marginal_portfolio_contribution"] = dict(mpc_payload)
    if persist and bool(pool_corr_payload.get("applied")):
        record_statistical_evidence(
            con=con,
            ts=int(evidence_ts),
            model_id=str(model_key),
            test_name="pool_correlation",
            decision=("pass" if bool(pool_corr_payload.get("passed")) else "fail"),
            payload=dict(pool_corr_payload),
        )
    if persist and bool(mpc_payload.get("applied")):
        record_statistical_evidence(
            con=con,
            ts=int(evidence_ts),
            model_id=str(model_key),
            test_name="marginal_portfolio_contribution",
            decision=("pass" if bool(mpc_payload.get("passed")) else "fail"),
            payload=dict(mpc_payload),
        )

    fnc_payload = _feature_neutral_ic_payload(
        challenger_predictions=challenger_predictions,
        realized_returns=realized_returns,
        neutralization_features=neutralization_features,
        candidate_features=candidate_features,
    )
    diagnostics["tests"]["feature_neutral_ic"] = dict(fnc_payload)
    diagnostics["feature_neutral_ic"] = dict(fnc_payload)
    if persist and bool(fnc_payload.get("applied")):
        record_statistical_evidence(
            con=con,
            ts=int(evidence_ts),
            model_id=str(model_key),
            test_name="feature_neutral_ic",
            t_stat=float(fnc_payload.get("raw_minus_fnc") or 0.0),
            decision=("flag" if bool(fnc_payload.get("flagged")) else "pass"),
            payload=dict(fnc_payload),
        )

    era_payload, era_gate_passed = _era_regime_robustness_gate(
        challenger_series=challenger_series,
        champion_series=champion_series,
        evaluation_timestamps=evaluation_timestamps,
        era_labels=era_labels,
        regime_labels=regime_labels,
        challenger_predictions=challenger_predictions,
        realized_returns=realized_returns,
    )
    diagnostics["tests"]["era_regime_robustness"] = dict(era_payload)
    if persist and bool(era_payload.get("applied")):
        record_statistical_evidence(
            con=con,
            ts=int(evidence_ts),
            model_id=str(model_key),
            test_name="era_regime_robustness",
            decision=("pass" if bool(era_payload.get("passed")) else "fail"),
            payload=dict(era_payload),
        )

    diagnostics["passed"] = bool(reality.passed and feature_gate_passed and pool_gates_passed and era_gate_passed)
    diagnostics["status"] = "pass" if bool(diagnostics["passed"]) else "fail"
    return bool(diagnostics["passed"]), diagnostics

# ------            -- ------------------------------------------------------
# Guard state (DB overrides env)
# ------            -- ------------------------------------------------------

def set_guard(key: str, value: str) -> None:
    init_db()
    con = connect()
    try:
        _ensure_guard_schema(con)

        con.execute(
            """
            INSERT OR REPLACE INTO model_promotion_guard(key, value, updated_ts_ms)
            VALUES (?,?,?)
            """,
            (str(key), str(value), _now_ms()),
        )
        con.commit()
    finally:
        con.close()


def _ensure_guard_schema(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS model_promotion_guard (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL,
          updated_ts_ms INTEGER NOT NULL
        )
        """
    )


def get_guard(key: str, default: str) -> str:
    init_db()
    con = connect()
    try:
        _ensure_guard_schema(con)
        r = con.execute(
            "SELECT value FROM model_promotion_guard WHERE key=?",
            (str(key),),
        ).fetchone()
        return str(r[0]) if r and r[0] is not None else str(default)
    finally:
        con.close()


def statistical_gate_config(config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    return promotion_gate_config_from_env(config)


def cpcv_gate_config(config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    base = cpcv_config_from_env()
    overrides = dict(config or {})
    nested = overrides.get("cpcv") if isinstance(overrides.get("cpcv"), dict) else {}
    merged = dict(base)
    for key in (
        "enabled",
        "n_splits",
        "n_test_splits",
        "embargo_pct",
        "label_horizon",
        "max_pbo",
        "min_path_sharpe",
        "costs_enabled",
        "retrain_cadence_replay",
        "retrain_cadence_ms",
        "gated_backtest",
    ):
        if key in overrides:
            merged[key] = overrides.get(key)
        if key in nested:
            merged[key] = nested.get(key)
    merged["enabled"] = str(merged.get("enabled") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
    merged["n_splits"] = int(max(2, int(merged.get("n_splits") or 6)))
    merged["n_test_splits"] = int(max(1, int(merged.get("n_test_splits") or 2)))
    merged["embargo_pct"] = float(max(0.0, float(merged.get("embargo_pct") or 0.0)))
    merged["label_horizon"] = int(max(0, int(merged.get("label_horizon") or 0)))
    merged["max_pbo"] = float(max(0.0, float(merged.get("max_pbo") or 0.0)))
    merged["min_path_sharpe"] = float(merged.get("min_path_sharpe") or 0.0)
    merged["costs_enabled"] = str(merged.get("costs_enabled", True)).strip().lower() in {"1", "true", "yes", "y", "on"}
    merged["retrain_cadence_replay"] = str(merged.get("retrain_cadence_replay", True)).strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }
    merged["retrain_cadence_ms"] = int(max(1, int(merged.get("retrain_cadence_ms") or 1)))
    merged["gated_backtest"] = str(merged.get("gated_backtest", True)).strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }
    return merged


def _cpcv_run_mismatch_fields(run: Dict[str, Any], gate_config: Dict[str, Any]) -> list[str]:
    mismatch = []
    if int(run.get("n_splits") or 0) != int(gate_config.get("n_splits") or 0):
        mismatch.append("n_splits")
    if int(run.get("n_test_splits") or 0) != int(gate_config.get("n_test_splits") or 0):
        mismatch.append("n_test_splits")
    if abs(float(run.get("embargo_pct") or 0.0) - float(gate_config.get("embargo_pct") or 0.0)) > 1e-9:
        mismatch.append("embargo_pct")
    diagnostics = dict(run.get("diagnostics") or {})
    cpcv_diagnostics = dict(diagnostics.get("cpcv") or {})
    has_modern_diagnostics = any(
        key in diagnostics
        for key in (
            "metric_basis",
            "cpcv",
            "gated_backtest",
            "retrain_cadence_replay",
        )
    )
    if not has_modern_diagnostics:
        return mismatch
    metric_basis = str(
        diagnostics.get("metric_basis")
        or cpcv_diagnostics.get("metric_basis")
        or ""
    ).strip()
    required_basis = "gated_cost_adjusted" if bool(gate_config.get("gated_backtest", True)) else "cost_adjusted"
    if bool(gate_config.get("costs_enabled", True)) and metric_basis != str(required_basis):
        mismatch.append("metric_basis")
    gated_diag = dict(diagnostics.get("gated_backtest") or cpcv_diagnostics.get("gated_backtest") or {})
    if bool(gate_config.get("gated_backtest", True)) and not bool(gated_diag.get("enabled")):
        mismatch.append("gated_backtest")
    replay_diag = dict(diagnostics.get("retrain_cadence_replay") or cpcv_diagnostics.get("retrain_cadence_replay") or {})
    if bool(gate_config.get("retrain_cadence_replay", True)) and not bool(replay_diag.get("enabled")):
        mismatch.append("retrain_cadence_replay")
    if bool(replay_diag.get("enabled")) and int(replay_diag.get("cadence_ms") or 0) != int(
        gate_config.get("retrain_cadence_ms") or 0
    ):
        mismatch.append("retrain_cadence_ms")
    return mismatch


def _materialize_cpcv_run(
    *,
    model_name: str,
    candidate_version: str,
    gate_config: Dict[str, Any],
) -> Dict[str, Any]:
    try:
        from engine.strategy.cpcv import run_backtest_cpcv_job
    except Exception as e:
        _warn_nonfatal(
            "PROMOTION_GUARD_CPCV_IMPORT_FAILED",
            e,
            model_name=str(model_name),
            candidate_version=str(candidate_version),
        )
        return {"ok": False, "status": "auto_run_import_failed", "error": f"{type(e).__name__}:{e}"}

    try:
        return dict(
            run_backtest_cpcv_job(
                model_name=str(model_name),
                candidate_version=str(candidate_version),
                n_splits=int(gate_config.get("n_splits") or 0),
                n_test_splits=int(gate_config.get("n_test_splits") or 0),
                embargo_pct=float(gate_config.get("embargo_pct") or 0.0),
                label_horizon=int(gate_config.get("label_horizon") or 0),
                replay_retrain_cadence=bool(gate_config.get("retrain_cadence_replay", True)),
                retrain_cadence_ms=int(gate_config.get("retrain_cadence_ms") or 0),
                cost_config={"enabled": bool(gate_config.get("costs_enabled", True))},
                gated_backtest=bool(gate_config.get("gated_backtest", True)),
            )
            or {}
        )
    except Exception as e:
        _warn_nonfatal(
            "PROMOTION_GUARD_CPCV_AUTORUN_FAILED",
            e,
            model_name=str(model_name),
            candidate_version=str(candidate_version),
        )
        return {"ok": False, "status": "auto_run_failed", "error": f"{type(e).__name__}:{e}"}


def evaluate_cpcv_promotion_gate(
    *,
    model_name: str,
    candidate_version: str,
    config: Dict[str, Any] | None = None,
) -> Tuple[bool, Dict[str, Any]]:
    gate_config = cpcv_gate_config(config)
    diagnostics: Dict[str, Any] = {
        "enabled": bool(gate_config.get("enabled")),
        "status": "disabled",
        "model_name": str(model_name or "").strip(),
        "candidate_version": str(candidate_version or "").strip(),
        "required_n_splits": int(gate_config.get("n_splits") or 0),
        "required_n_test_splits": int(gate_config.get("n_test_splits") or 0),
        "required_embargo_pct": float(gate_config.get("embargo_pct") or 0.0),
        "max_pbo": float(gate_config.get("max_pbo") or 0.0),
        "min_path_sharpe": float(gate_config.get("min_path_sharpe") or 0.0),
        "passed": True,
    }
    if not bool(gate_config.get("enabled")):
        return True, diagnostics

    run = fetch_latest_backtest_cpcv_run(
        model_name=str(model_name or "").strip(),
        candidate_version=str(candidate_version or "").strip(),
        include_paths=False,
    )
    mismatch = _cpcv_run_mismatch_fields(dict(run or {}), gate_config) if isinstance(run, dict) and run else []
    if (not isinstance(run, dict) or not run) or mismatch:
        diagnostics["auto_run"] = _materialize_cpcv_run(
            model_name=str(model_name or "").strip(),
            candidate_version=str(candidate_version or "").strip(),
            gate_config=gate_config,
        )
        run = fetch_latest_backtest_cpcv_run(
            model_name=str(model_name or "").strip(),
            candidate_version=str(candidate_version or "").strip(),
            include_paths=False,
        )
        mismatch = _cpcv_run_mismatch_fields(dict(run or {}), gate_config) if isinstance(run, dict) and run else []

    if not isinstance(run, dict) or not run:
        diagnostics["status"] = str(dict(diagnostics.get("auto_run") or {}).get("status") or "missing_run")
        diagnostics["passed"] = False
        return False, diagnostics

    diagnostics["latest_run"] = {
        "id": int(run.get("id") or 0),
        "created_ts": int(run.get("created_ts") or 0),
        "n_splits": int(run.get("n_splits") or 0),
        "n_test_splits": int(run.get("n_test_splits") or 0),
        "embargo_pct": float(run.get("embargo_pct") or 0.0),
        "n_paths": int(run.get("n_paths") or 0),
        "mean_sharpe": float(run.get("mean_sharpe") or 0.0),
        "median_sharpe": float(run.get("median_sharpe") or 0.0),
        "pbo": float(run.get("pbo") or 0.0),
    }
    diagnostics["run_diagnostics"] = dict(run.get("diagnostics") or {})

    if mismatch:
        diagnostics["status"] = "parameter_mismatch"
        diagnostics["mismatch_fields"] = mismatch
        diagnostics["passed"] = False
        return False, diagnostics

    if int(run.get("n_paths") or 0) <= 0:
        diagnostics["status"] = "no_valid_paths"
        diagnostics["passed"] = False
        return False, diagnostics

    if float(run.get("pbo") or 0.0) > float(gate_config.get("max_pbo") or 0.0):
        diagnostics["status"] = "pbo_above_threshold"
        diagnostics["passed"] = False
        return False, diagnostics

    if float(run.get("median_sharpe") or 0.0) < float(gate_config.get("min_path_sharpe") or 0.0):
        diagnostics["status"] = "median_sharpe_below_threshold"
        diagnostics["passed"] = False
        return False, diagnostics

    diagnostics["status"] = "evaluated"
    return True, diagnostics


def evaluate_statistical_promotion_gate(
    *,
    model_name: str,
    candidate_version: str,
    returns,
    n_competing_trials: int,
    models_returns: Dict[str, Any] | None = None,
    config: Dict[str, Any] | None = None,
    persist: bool = True,
) -> Tuple[bool, Dict[str, Any]]:
    gate_config = statistical_gate_config(config)
    stat_passed, statistical_diagnostics = passes_promotion_gate(
        returns,
        n_competing_trials,
        config=gate_config,
        models_returns=models_returns,
    )
    cpcv_passed, cpcv_diagnostics = evaluate_cpcv_promotion_gate(
        model_name=str(model_name or "").strip(),
        candidate_version=str(candidate_version or "").strip(),
        config=config,
    )
    diagnostics = dict(statistical_diagnostics or {})
    diagnostics["model_name"] = str(model_name or "").strip()
    diagnostics["candidate_version"] = str(candidate_version or "").strip()
    diagnostics["statistical_gate"] = dict(statistical_diagnostics or {})
    diagnostics["cpcv"] = dict(cpcv_diagnostics or {})
    diagnostics["validation_enabled"] = bool(
        bool((statistical_diagnostics or {}).get("enabled")) or bool((cpcv_diagnostics or {}).get("enabled"))
    )
    diagnostics["applied"] = bool(diagnostics.get("validation_enabled"))
    diagnostics["passed"] = bool(stat_passed and cpcv_passed)
    if not bool(diagnostics.get("validation_enabled")):
        diagnostics["status"] = "disabled"
    elif not bool(stat_passed):
        diagnostics["status"] = str((statistical_diagnostics or {}).get("status") or "statistical_gate_failed")
    elif not bool(cpcv_passed):
        diagnostics["status"] = str((cpcv_diagnostics or {}).get("status") or "cpcv_gate_failed")
    else:
        diagnostics["status"] = "evaluated"

    if bool(persist) and bool((statistical_diagnostics or {}).get("enabled")):
        try:
            record_hypothesis_result(
                model_name=str(model_name or "").strip(),
                candidate_version=str(candidate_version or model_name or "").strip(),
                n_observations=int(diagnostics.get("n_observations") or 0),
                t_statistic=float(diagnostics.get("t_statistic") or 0.0),
                deflated_sharpe=float(diagnostics.get("deflated_sharpe") or 0.0),
                threshold_t=float(diagnostics.get("threshold_t") or 0.0),
                n_competing_trials=int(diagnostics.get("n_competing_trials") or 0),
                passed=bool(diagnostics.get("passed")),
                diagnostics=diagnostics,
            )
        except Exception as e:
            _warn_nonfatal(
                "PROMOTION_GUARD_HYPOTHESIS_RECORD_FAILED",
                e,
                model_name=str(model_name or "").strip(),
                candidate_version=str(candidate_version or "").strip(),
            )

    return bool(stat_passed and cpcv_passed), diagnostics

# ------            -- ------------------------------------------------------
# A) Metric-based promotion decision (RESTORED, not lost)
# ------            -- ------------------------------------------------------

def promotion_allowed_by_metrics(
    challenger_metrics: Dict[str, Any],
    champion_metrics: Dict[str, Any],
    min_improvement: float,
    diracc_tol: float,
) -> bool:
    """
    Pure metric-based promotion gate.
    Returns True if challenger is statistically better.
    """
    try:
        # ---- coverage ----
        n_eval = int(challenger_metrics.get("n_eval", 0))
        if n_eval < MIN_EVAL_ROWS:
            _warn_state("PROMOTE_BLOCKED_INSUFFICIENT_EVAL_ROWS", "Promotion blocked due to insufficient evaluation rows.", n_eval=n_eval)
            return False

        # ---- sanity ----
        rmse = float(challenger_metrics.get("rmse", float("inf")))
        bias = abs(float(challenger_metrics.get("bias", 0.0)))

        if not math.isfinite(rmse) or rmse > MAX_ABS_RMSE:
            _warn_state("PROMOTE_BLOCKED_BAD_RMSE", "Promotion blocked due to invalid RMSE.", rmse=rmse)
            return False

        if not math.isfinite(bias) or bias > MAX_ABS_BIAS:
            _warn_state("PROMOTE_BLOCKED_BAD_BIAS", "Promotion blocked due to excessive bias.", bias=bias)
            return False

        # ---- improvement ----
        champ_rmse = float(champion_metrics.get("rmse", float("inf")))
        if rmse >= champ_rmse * (1.0 - min_improvement):
            logging.info(
                "PROMOTE_BLOCKED no_rmse_improvement rmse=%s champ=%s",
                rmse,
                champ_rmse,
            )
            return False

        # ---- directional accuracy ----
        ch_dir = float(challenger_metrics.get("dir_acc", 0.0))
        cp_dir = float(champion_metrics.get("dir_acc", 0.0))
        if ch_dir < cp_dir - diracc_tol:
            logging.info(
                "PROMOTE_BLOCKED dir_acc_worse ch=%s cp=%s",
                ch_dir,
                cp_dir,
            )
            return False

        logging.info(
            "PROMOTE_ALLOWED metrics rmse=%s dir_acc=%s n_eval=%s",
            rmse,
            ch_dir,
            n_eval,
        )
        return True

    except Exception as e:
        _warn_nonfatal(
            "PROMOTE_BLOCKED_METRICS_EXCEPTION",
            e,
            challenger_metrics=dict(challenger_metrics or {}),
            champion_metrics=dict(champion_metrics or {}),
        )
        return False

# ------            -- ------------------------------------------------------
# B) System-state promotion guard (public API)
# ------            -- ------------------------------------------------------

def promotion_allowed() -> Tuple[bool, Dict[str, Any]]:
    """
    System-wide promotion guard.
    Returns (allowed, reason_dict).
    """
    init_db()

    enabled_db = get_guard("promotion_enabled", "1")
    enabled = (enabled_db == "1") and PROMOTION_ENABLED_ENV

    reason: Dict[str, Any] = {
        "promotion_enabled_env": bool(PROMOTION_ENABLED_ENV),
        "promotion_enabled_db": enabled_db,
        "statistical_gate": statistical_gate_config(),
        "cpcv_gate": cpcv_gate_config(),
        "blockers": [],
    }

    if not enabled:
        reason["blockers"].append("disabled")
        return (False, reason)

    try:
        from engine.runtime.backup_evidence import backup_restore_evidence_snapshot

        backup_evidence = backup_restore_evidence_snapshot(engine_mode=os.environ.get("ENGINE_MODE", "safe"))
        reason["backup_restore_evidence"] = dict(backup_evidence or {})
        if bool(backup_evidence.get("required")) and not bool(backup_evidence.get("ok")):
            reason["blockers"].extend(str(item) for item in list(backup_evidence.get("blockers") or []))
    except Exception as e:
        _warn_nonfatal("promotion_guard_backup_evidence_failed", e)
        if str(os.environ.get("ENGINE_MODE", "")).strip().lower() == "live" or os.environ.get(
            "PREFLIGHT_REQUIRE_BACKUP_EVIDENCE", "0"
        ) == "1":
            reason["blockers"].append("backup_evidence_unavailable")

    con = connect()
    try:
        now = _now_ms()

        # ---- cooldown guard (global, fail-closed) ----
        cooldown_s = int(os.environ.get("PROMOTION_COOLDOWN_S", "21600"))  # 6h
        cooldown_ms = int(cooldown_s) * 1000
        try:
            last_promo = con.execute(
                """
                SELECT MAX(ts_ms) FROM model_promotion_audit
                WHERE action='promote'
                """
            ).fetchone()[0]
            last_promo = int(last_promo or 0)
        except Exception:
            last_promo = 0

        reason["last_promo_ts_ms"] = last_promo
        reason["cooldown_s"] = int(cooldown_s)

        if last_promo > 0 and (now - last_promo) < cooldown_ms:
            reason["blockers"].append("cooldown")

        # ---- CRIT alerts guard ----
        try:
            lookback_ms = PROMOTION_CRIT_ALERT_LOOKBACK_S * 1000
            n_crit = con.execute(
                """
                SELECT COUNT(1) FROM alerts
                WHERE severity='CRIT' AND ts_ms >= ?
                """,
                (now - lookback_ms,),
            ).fetchone()[0]
            n_crit = int(n_crit or 0)
        except Exception:
            n_crit = 0

        reason["crit_alerts"] = n_crit
        if n_crit > PROMOTION_MAX_CRIT_ALERTS:
            reason["blockers"].append("crit_alerts")

        # ---- equity drift CRIT ----
        if PROMOTION_BLOCK_IF_EQUITY_CRIT:
            reason["equity_drift_available"] = _table_exists(con, "equity_drift")
            if reason["equity_drift_available"]:
                try:
                    ed_ms = PROMOTION_EQUITY_DRIFT_LOOKBACK_S * 1000
                    ed = con.execute(
                        """
                        SELECT COUNT(1) FROM equity_drift
                        WHERE level='CRIT' AND ts_ms >= ?
                        """,
                        (now - ed_ms,),
                    ).fetchone()[0]
                    ed = int(ed or 0)
                except Exception:
                    ed = 0
            else:
                ed = 0
            reason["equity_drift_crit_points"] = ed
            if ed > 0:
                reason["blockers"].append("equity_drift_crit")

        # ---- model drift ratio ----
        if PROMOTION_MAX_DRIFT_RATIO > 0.0:
            try:
                md = con.execute(
                    "SELECT MAX(drift_ratio) FROM model_drift"
                ).fetchone()[0]
                md = float(md or 0.0)
            except Exception:
                md = 0.0

            reason["max_drift_ratio"] = md
            if md > PROMOTION_MAX_DRIFT_RATIO:
                reason["blockers"].append("drift_ratio")

        # ------------------------------------------------------------
        # Trade Attribution Guard (capital-based pruning)
        # ------------------------------------------------------------
        try:
            rows = con.execute(
                """
                SELECT
                  json_extract(model_json, '$.model_name') AS model_name,
                  SUM(
                    COALESCE(
                      json_extract(signal_json, '$.pnl_attribution.total_pnl'),
                      COALESCE(json_extract(signal_json, '$.pnl_attribution.realized_pnl'), 0.0)
                      + COALESCE(json_extract(signal_json, '$.pnl_attribution.unrealized_pnl'), 0.0)
                      - COALESCE(fees, 0.0)
                      - COALESCE(json_extract(signal_json, '$.pnl_attribution.extra.slippage_cost'), 0.0)
                    )
                  ) AS total_pnl
                FROM trade_attribution_ledger
                WHERE suppression_reason IS NULL
                  AND ts_ms >= ?
                GROUP BY model_name
                """,
                (now - (PROMOTION_DRIFT_LOOKBACK_S * 1000),),
            ).fetchall()

            model_pnl = {str(r[0]): float(r[1] or 0.0) for r in rows if r[0]}

            reason["model_pnl_snapshot"] = model_pnl

            # block promotion if any live model is negative capital impact
            negative_models = [m for m, p in model_pnl.items() if float(p) < 0.0]
            if negative_models:
                reason["blockers"].append("negative_real_pnl_models")
                reason["negative_models"] = negative_models

        except Exception as e:
            _warn_nonfatal("promotion_guard_model_pnl_snapshot_failed", e)
    finally:
        con.close()

    allowed = len(reason["blockers"]) == 0
    return (allowed, reason)
