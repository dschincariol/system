"""Residualized incremental signal validation for promotion gates.

The validator uses a DML-style residualization step: both the candidate
signal and realized outcome are residualized against observed confounders,
then the residual outcome is regressed on the residual signal.  The promotion
gate consumes the resulting incremental effect, p-value, residual IC, and
stability diagnostics.
"""

from __future__ import annotations

import math
import os
from collections.abc import Mapping, Sequence
from statistics import NormalDist
from typing import Any

import numpy as np

_NORMAL = NormalDist()
_EPS = 1.0e-12

REQUIRED_CONFOUNDER_GROUPS: tuple[str, ...] = (
    "beta",
    "sector",
    "size",
    "volatility",
    "liquidity",
    "regime",
    "existing_model_exposure",
)

_NUMERIC_ALIASES: dict[str, tuple[str, ...]] = {
    "beta": ("beta", "market_beta", "capm_beta", "spy_beta"),
    "size": ("size", "market_cap", "log_market_cap", "ln_market_cap", "mkt_cap"),
    "volatility": (
        "volatility",
        "vol",
        "realized_volatility",
        "realized_vol",
        "rv",
        "rv_20",
        "vol_std_20",
        "atr",
    ),
    "liquidity": (
        "liquidity",
        "adv",
        "adv20",
        "dollar_volume",
        "avg_dollar_volume",
        "volume",
        "turnover",
        "bid_ask_spread",
    ),
    "existing_model_exposure": (
        "existing_model_exposure",
        "model_exposure",
        "champion_exposure",
        "champion_signal",
        "champion_prediction",
        "pool_signal",
        "pool_exposure",
    ),
}

_CATEGORICAL_ALIASES: dict[str, tuple[str, ...]] = {
    "sector": ("sector", "gics_sector", "industry_sector", "sector_name"),
    "regime": ("regime", "market_regime", "hmm_regime", "risk_regime"),
}


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
    except Exception:
        return int(default)


def _safe_float_env(name: str, default: float) -> float:
    raw = os.environ.get(name, "")
    if raw in (None, ""):
        return float(default)
    try:
        value = float(raw)
    except Exception:
        return float(default)
    return float(value) if math.isfinite(value) else float(default)


def deconfounded_gate_config(config: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Return normalized deconfounded promotion-gate thresholds."""

    overrides = dict(config or {})
    return {
        "enabled": bool(overrides.get("enabled", _safe_bool_env("PROMOTION_DECONFOUNDED_ENABLED", True))),
        "min_obs": int(max(4, int(overrides.get("min_obs", _safe_int_env("PROMOTION_DECONFOUNDED_MIN_OBS", 8))))),
        "alpha": float(max(0.0, min(1.0, float(overrides.get("alpha", _safe_float_env("PROMOTION_DECONFOUNDED_ALPHA", 0.10)))))),
        "min_effect": float(overrides.get("min_effect", _safe_float_env("PROMOTION_DECONFOUNDED_MIN_EFFECT", 0.0))),
        "min_residual_ic": float(
            overrides.get("min_residual_ic", _safe_float_env("PROMOTION_DECONFOUNDED_MIN_RESIDUAL_IC", 0.01))
        ),
        "min_stability_share": float(
            max(
                0.0,
                min(
                    1.0,
                    float(
                        overrides.get(
                            "min_stability_share",
                            _safe_float_env("PROMOTION_DECONFOUNDED_MIN_STABILITY_SHARE", 0.60),
                        )
                    ),
                ),
            )
        ),
        "max_signal_confounder_r2": float(
            max(
                0.0,
                min(
                    1.0,
                    float(
                        overrides.get(
                            "max_signal_confounder_r2",
                            _safe_float_env("PROMOTION_DECONFOUNDED_MAX_SIGNAL_CONFOUNDER_R2", 0.95),
                        )
                    ),
                ),
            )
        ),
        "folds": int(max(2, int(overrides.get("folds", _safe_int_env("PROMOTION_DECONFOUNDED_FOLDS", 3))))),
        "ridge_lambda": float(max(0.0, float(overrides.get("ridge_lambda", _safe_float_env("PROMOTION_DECONFOUNDED_RIDGE_LAMBDA", 1.0e-6))))),
        "require_all_controls": bool(
            overrides.get(
                "require_all_controls",
                _safe_bool_env("PROMOTION_DECONFOUNDED_REQUIRE_ALL_CONTROLS", True),
            )
        ),
    }


def _is_sequence(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, np.ndarray):
        return value.reshape(-1).tolist()
    if _is_sequence(value):
        return list(value)
    return [value]


def _float_or_nan(value: Any) -> float:
    try:
        out = float(value)
    except Exception:
        return float("nan")
    return float(out) if math.isfinite(out) else float("nan")


def _finite_corr(left: np.ndarray, right: np.ndarray) -> float:
    x = np.asarray(left, dtype=np.float64).reshape(-1)
    y = np.asarray(right, dtype=np.float64).reshape(-1)
    mask = np.isfinite(x) & np.isfinite(y)
    if int(mask.sum()) < 2:
        return 0.0
    x = x[mask] - float(np.mean(x[mask]))
    y = y[mask] - float(np.mean(y[mask]))
    denom = float(np.linalg.norm(x) * np.linalg.norm(y))
    if denom <= _EPS:
        return 0.0
    return float(max(-1.0, min(1.0, float(np.dot(x, y) / denom))))


def _coerce_control_rows(controls: Any, n_hint: int) -> list[Mapping[str, Any]]:
    if controls is None:
        return []
    if isinstance(controls, Mapping):
        for key in ("controls", "confounders", "control_rows", "rows", "features", "feature_rows"):
            if key in controls:
                return _coerce_control_rows(controls.get(key), n_hint)
        column_like = {
            str(key): _as_list(value)
            for key, value in controls.items()
            if _is_sequence(value) or isinstance(value, np.ndarray)
        }
        if column_like:
            n = min([int(n_hint)] + [len(values) for values in column_like.values() if values])
            rows: list[dict[str, Any]] = []
            for idx in range(max(0, n)):
                rows.append({key: values[idx] for key, values in column_like.items() if idx < len(values)})
            return rows
        return [controls]
    if _is_sequence(controls) or isinstance(controls, np.ndarray):
        out: list[Mapping[str, Any]] = []
        for row in list(controls):
            if isinstance(row, Mapping):
                nested = row.get("features") if isinstance(row.get("features"), Mapping) else row
                out.append(dict(nested or {}))
        return out
    return []


def _extract_group_values(
    *,
    group: str,
    rows: list[Mapping[str, Any]],
    explicit: Any,
    n_hint: int,
) -> list[Any]:
    explicit_values = _as_list(explicit)
    if explicit_values:
        return explicit_values[:n_hint]
    aliases = _NUMERIC_ALIASES.get(group, ()) + _CATEGORICAL_ALIASES.get(group, ())
    out: list[Any] = []
    for idx in range(n_hint):
        row = rows[idx] if idx < len(rows) else {}
        value = None
        for key in aliases:
            if key in row:
                value = row.get(key)
                break
        out.append(value)
    return out


def _group_is_present(group: str, values: Sequence[Any]) -> bool:
    if group in _CATEGORICAL_ALIASES:
        return any(str(value or "").strip() for value in values)
    return any(math.isfinite(_float_or_nan(value)) for value in values)


def _encode_numeric(values: Sequence[Any]) -> np.ndarray | None:
    arr = np.asarray([_float_or_nan(value) for value in values], dtype=np.float64).reshape(-1)
    finite = np.isfinite(arr)
    if int(finite.sum()) <= 0:
        return None
    fill = float(np.mean(arr[finite]))
    arr = np.where(finite, arr, fill)
    std = float(np.std(arr))
    if std <= _EPS:
        return np.zeros(arr.shape[0], dtype=np.float64)
    return ((arr - float(np.mean(arr))) / std).astype(np.float64, copy=False)


def _encode_categorical(values: Sequence[Any], *, prefix: str) -> tuple[np.ndarray | None, list[str]]:
    labels = [str(value or "missing").strip() or "missing" for value in values]
    categories = sorted(set(labels))
    if len(categories) <= 1:
        return None, []
    cols = []
    names = []
    for category in categories[1:]:
        cols.append(np.asarray([1.0 if label == category else 0.0 for label in labels], dtype=np.float64))
        names.append(f"{prefix}:{category}")
    return np.column_stack(cols).astype(np.float64, copy=False), names


def _build_control_matrix(
    group_values: Mapping[str, Sequence[Any]],
    n_obs: int,
) -> tuple[np.ndarray, list[str]]:
    cols: list[np.ndarray] = []
    names: list[str] = []
    for group in ("beta", "size", "volatility", "liquidity", "existing_model_exposure"):
        encoded = _encode_numeric(list(group_values.get(group) or [])[:n_obs])
        if encoded is None:
            continue
        cols.append(encoded.reshape(-1, 1))
        names.append(str(group))
    for group in ("sector", "regime"):
        encoded_cat, cat_names = _encode_categorical(list(group_values.get(group) or [])[:n_obs], prefix=group)
        if encoded_cat is None:
            continue
        cols.append(encoded_cat)
        names.extend(cat_names)
    if not cols:
        return np.zeros((n_obs, 0), dtype=np.float64), []
    return np.column_stack(cols).astype(np.float64, copy=False), names


def _ridge_predict(train_x: np.ndarray, train_y: np.ndarray, test_x: np.ndarray, ridge_lambda: float) -> np.ndarray:
    x_train = np.column_stack([np.ones(train_x.shape[0], dtype=np.float64), train_x])
    x_test = np.column_stack([np.ones(test_x.shape[0], dtype=np.float64), test_x])
    penalty = np.eye(x_train.shape[1], dtype=np.float64) * float(max(0.0, ridge_lambda))
    penalty[0, 0] = 0.0
    lhs = x_train.T @ x_train + penalty
    rhs = x_train.T @ train_y
    try:
        coef = np.linalg.solve(lhs, rhs)
    except np.linalg.LinAlgError:
        coef = np.linalg.pinv(lhs) @ rhs
    return np.asarray(x_test @ coef, dtype=np.float64).reshape(-1)


def _crossfit_residuals(
    y: np.ndarray,
    controls: np.ndarray,
    *,
    folds: int,
    ridge_lambda: float,
) -> tuple[np.ndarray, np.ndarray, float]:
    values = np.asarray(y, dtype=np.float64).reshape(-1)
    n_obs = int(values.shape[0])
    if n_obs <= 1:
        return values - float(np.mean(values)) if n_obs else values, np.zeros_like(values), 0.0
    if controls.shape[1] <= 0:
        fitted = np.full(n_obs, float(np.mean(values)), dtype=np.float64)
    else:
        k = min(max(2, int(folds)), max(2, n_obs // 2))
        fitted = np.zeros(n_obs, dtype=np.float64)
        indices = np.arange(n_obs)
        for fold_idx in range(k):
            test_mask = (indices % k) == fold_idx
            train_mask = ~test_mask
            if int(train_mask.sum()) < 2 or int(test_mask.sum()) <= 0:
                fitted[test_mask] = float(np.mean(values[train_mask])) if int(train_mask.sum()) else float(np.mean(values))
                continue
            fitted[test_mask] = _ridge_predict(
                controls[train_mask],
                values[train_mask],
                controls[test_mask],
                ridge_lambda=float(ridge_lambda),
            )
    residuals = values - fitted
    ss_total = float(np.sum((values - float(np.mean(values))) ** 2))
    ss_resid = float(np.sum(residuals ** 2))
    r2 = 0.0 if ss_total <= _EPS else float(max(0.0, min(1.0, 1.0 - (ss_resid / ss_total))))
    return residuals.astype(np.float64, copy=False), fitted.astype(np.float64, copy=False), float(r2)


def _effect_stats(signal_resid: np.ndarray, outcome_resid: np.ndarray) -> dict[str, float]:
    d = np.asarray(signal_resid, dtype=np.float64).reshape(-1)
    y = np.asarray(outcome_resid, dtype=np.float64).reshape(-1)
    n_obs = int(min(d.shape[0], y.shape[0]))
    d = d[:n_obs]
    y = y[:n_obs]
    x_centered = d - float(np.mean(d))
    y_centered = y - float(np.mean(y))
    denom = float(np.dot(x_centered, x_centered))
    if n_obs < 3 or denom <= _EPS:
        return {
            "coefficient": 0.0,
            "intercept": float(np.mean(y)) if n_obs else 0.0,
            "standard_error": float("inf"),
            "t_stat": 0.0,
            "p_value": 1.0,
            "residual_ic": 0.0,
            "partial_r2": 0.0,
        }
    coefficient = float(np.dot(x_centered, y_centered) / denom)
    intercept = float(np.mean(y) - (coefficient * float(np.mean(d))))
    error = y - (intercept + coefficient * d)
    sigma2 = float(np.sum(error ** 2) / float(max(1, n_obs - 2)))
    se = math.sqrt(max(0.0, sigma2 / denom))
    if se <= _EPS:
        if coefficient > 0.0:
            t_stat = float("inf")
        elif coefficient < 0.0:
            t_stat = float("-inf")
        else:
            t_stat = 0.0
    else:
        t_stat = float(coefficient / se)
    p_value = _two_sided_normal_p_value(t_stat)
    residual_ic = _finite_corr(d, y)
    return {
        "coefficient": float(coefficient),
        "intercept": float(intercept),
        "standard_error": float(se),
        "t_stat": float(t_stat),
        "p_value": float(p_value),
        "residual_ic": float(residual_ic),
        "partial_r2": float(max(0.0, min(1.0, residual_ic * residual_ic))),
    }


def _two_sided_normal_p_value(t_stat: float) -> float:
    if not math.isfinite(float(t_stat)):
        return 0.0 if float(t_stat) > 0.0 else 1.0
    tail = 1.0 - float(_NORMAL.cdf(abs(float(t_stat))))
    return float(max(0.0, min(1.0, 2.0 * tail)))


def _default_stability_labels(n_obs: int, labels: Any = None) -> list[str]:
    raw = _as_list(labels)
    if raw:
        return [str(value or "unknown") for value in raw[:n_obs]]
    if n_obs <= 0:
        return []
    bucket_count = min(4, max(1, n_obs // 2))
    return [f"block_{min(bucket_count - 1, int(idx * bucket_count / n_obs)) + 1}" for idx in range(n_obs)]


def _stability_payload(
    signal_resid: np.ndarray,
    outcome_resid: np.ndarray,
    *,
    labels: Any = None,
    min_bucket_obs: int = 3,
) -> dict[str, Any]:
    n_obs = int(min(signal_resid.shape[0], outcome_resid.shape[0]))
    label_values = _default_stability_labels(n_obs, labels=labels)
    buckets: dict[str, list[int]] = {}
    for idx, label in enumerate(label_values[:n_obs]):
        buckets.setdefault(str(label or "unknown"), []).append(int(idx))
    rows: list[dict[str, Any]] = []
    for label in sorted(buckets):
        idxs = buckets[label]
        if len(idxs) < int(max(2, min_bucket_obs)):
            continue
        stats = _effect_stats(signal_resid[idxs], outcome_resid[idxs])
        rows.append(
            {
                "label": str(label),
                "n_obs": int(len(idxs)),
                "coefficient": float(stats.get("coefficient") or 0.0),
                "residual_ic": float(stats.get("residual_ic") or 0.0),
            }
        )
    if not rows:
        stats = _effect_stats(signal_resid, outcome_resid)
        rows = [
            {
                "label": "all",
                "n_obs": int(n_obs),
                "coefficient": float(stats.get("coefficient") or 0.0),
                "residual_ic": float(stats.get("residual_ic") or 0.0),
            }
        ]
    positive = [row for row in rows if float(row.get("coefficient") or 0.0) > 0.0]
    share = float(len(positive) / float(len(rows))) if rows else 0.0
    worst = min((float(row.get("coefficient") or 0.0) for row in rows), default=0.0)
    return {
        "bucket_count": int(len(rows)),
        "positive_effect_share": float(share),
        "worst_bucket_effect": float(worst),
        "buckets": rows,
    }


def validate_deconfounded_signal(
    *,
    candidate_signal: Any,
    outcome: Any,
    controls: Any = None,
    beta: Any = None,
    sector: Any = None,
    size: Any = None,
    volatility: Any = None,
    liquidity: Any = None,
    regime: Any = None,
    existing_model_exposure: Any = None,
    stability_labels: Any = None,
    config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Estimate incremental residual signal effect after confounder controls."""

    gate_config = deconfounded_gate_config(config)
    signal_raw = _as_list(candidate_signal)
    outcome_raw = _as_list(outcome)
    n_hint = min(len(signal_raw), len(outcome_raw))
    payload: dict[str, Any] = {
        "enabled": bool(gate_config.get("enabled")),
        "applied": bool(gate_config.get("enabled")),
        "passed": False,
        "status": "disabled" if not bool(gate_config.get("enabled")) else "not_evaluated",
        "method": "crossfit_residualized_incremental_effect",
        "required_controls": list(REQUIRED_CONFOUNDER_GROUPS),
        "config": dict(gate_config),
    }
    if not bool(gate_config.get("enabled")):
        payload["passed"] = True
        return payload
    if n_hint <= 0:
        payload.update({"status": "missing_signal_or_outcome", "n_obs": 0})
        return payload

    rows = _coerce_control_rows(controls, n_hint)
    explicit = {
        "beta": beta,
        "sector": sector,
        "size": size,
        "volatility": volatility,
        "liquidity": liquidity,
        "regime": regime,
        "existing_model_exposure": existing_model_exposure,
    }
    raw_group_values = {
        group: _extract_group_values(group=group, rows=rows, explicit=explicit.get(group), n_hint=n_hint)
        for group in REQUIRED_CONFOUNDER_GROUPS
    }

    kept_signal: list[float] = []
    kept_outcome: list[float] = []
    kept_group_values: dict[str, list[Any]] = {group: [] for group in REQUIRED_CONFOUNDER_GROUPS}
    kept_labels: list[Any] = []
    labels = _as_list(stability_labels)
    for idx in range(n_hint):
        signal_value = _float_or_nan(signal_raw[idx])
        outcome_value = _float_or_nan(outcome_raw[idx])
        if not math.isfinite(signal_value) or not math.isfinite(outcome_value):
            continue
        kept_signal.append(float(signal_value))
        kept_outcome.append(float(outcome_value))
        for group in REQUIRED_CONFOUNDER_GROUPS:
            values = raw_group_values.get(group) or []
            kept_group_values[group].append(values[idx] if idx < len(values) else None)
        kept_labels.append(labels[idx] if idx < len(labels) else None)

    n_obs = int(len(kept_signal))
    present = {group: _group_is_present(group, kept_group_values.get(group) or []) for group in REQUIRED_CONFOUNDER_GROUPS}
    missing = [group for group, ok in present.items() if not ok]
    payload.update(
        {
            "n_obs": int(n_obs),
            "min_obs": int(gate_config.get("min_obs") or 0),
            "controls_present": dict(present),
            "missing_controls": list(missing),
        }
    )
    if n_obs < int(gate_config.get("min_obs") or 0):
        payload["status"] = "insufficient_observations"
        return payload
    if bool(gate_config.get("require_all_controls")) and missing:
        payload["status"] = "missing_required_confounders"
        return payload

    signal = np.asarray(kept_signal, dtype=np.float64)
    outcome_arr = np.asarray(kept_outcome, dtype=np.float64)
    control_matrix, control_names = _build_control_matrix(kept_group_values, n_obs)
    payload["control_columns"] = list(control_names)
    payload["control_column_count"] = int(control_matrix.shape[1])
    if control_matrix.shape[1] <= 0 and bool(gate_config.get("require_all_controls")):
        payload["status"] = "no_usable_control_columns"
        return payload

    signal_resid, signal_fitted, signal_r2 = _crossfit_residuals(
        signal,
        control_matrix,
        folds=int(gate_config.get("folds") or 2),
        ridge_lambda=float(gate_config.get("ridge_lambda") or 0.0),
    )
    outcome_resid, _outcome_fitted, outcome_r2 = _crossfit_residuals(
        outcome_arr,
        control_matrix,
        folds=int(gate_config.get("folds") or 2),
        ridge_lambda=float(gate_config.get("ridge_lambda") or 0.0),
    )
    residual_signal_var = float(np.var(signal_resid))
    raw_signal_var = float(np.var(signal))
    confounder_r2 = 1.0 if raw_signal_var <= _EPS else float(max(0.0, min(1.0, 1.0 - (residual_signal_var / raw_signal_var))))
    stats = _effect_stats(signal_resid, outcome_resid)
    stability = _stability_payload(
        signal_resid,
        outcome_resid,
        labels=(kept_labels if any(value is not None for value in kept_labels) else None),
    )
    raw_ic = _finite_corr(signal, outcome_arr)
    coefficient = float(stats.get("coefficient", 0.0))
    intercept = float(stats.get("intercept", 0.0))
    standard_error = float(stats.get("standard_error", 0.0))
    t_stat = float(stats.get("t_stat", 0.0))
    p_value = float(stats.get("p_value", 1.0))
    residual_ic = float(stats.get("residual_ic", 0.0))
    partial_r2 = float(stats.get("partial_r2", 0.0))
    min_effect = float(gate_config.get("min_effect", 0.0))
    alpha = float(gate_config.get("alpha", 0.0))
    min_residual_ic = float(gate_config.get("min_residual_ic", 0.0))
    min_stability_share = float(gate_config.get("min_stability_share", 0.0))
    explained_by_confounders = bool(
        residual_signal_var <= _EPS
        or confounder_r2 >= float(gate_config.get("max_signal_confounder_r2", 1.0))
    )
    effect_passed = bool(
        coefficient > min_effect
        and p_value <= alpha
        and residual_ic >= min_residual_ic
    )
    stability_passed = bool(
        float(stability.get("positive_effect_share") or 0.0) >= min_stability_share
        and float(stability.get("worst_bucket_effect") or 0.0) > min_effect
    )
    passed = bool(effect_passed and stability_passed and not explained_by_confounders)
    if explained_by_confounders:
        status = "signal_explained_by_confounders"
    elif not effect_passed:
        status = "weak_incremental_effect"
    elif not stability_passed:
        status = "unstable_incremental_effect"
    else:
        status = "evaluated"

    payload.update(
        {
            "status": str(status),
            "passed": bool(passed),
            "raw_ic": float(raw_ic),
            "coefficient": float(coefficient),
            "intercept": float(intercept),
            "standard_error": float(standard_error),
            "t_stat": float(t_stat),
            "p_value": float(p_value),
            "residual_ic": float(residual_ic),
            "partial_r2": float(partial_r2),
            "signal_confounder_r2": float(confounder_r2),
            "outcome_confounder_r2": float(outcome_r2),
            "residual_signal_variance": float(residual_signal_var),
            "raw_signal_variance": float(raw_signal_var),
            "explained_by_confounders": bool(explained_by_confounders),
            "stability": dict(stability),
            "decision_components": {
                "effect_passed": bool(effect_passed),
                "stability_passed": bool(stability_passed),
                "confounder_explanation_passed": bool(not explained_by_confounders),
                "coefficient_gt_min_effect": bool(coefficient > min_effect),
                "p_value_lte_alpha": bool(p_value <= alpha),
                "residual_ic_gte_min": bool(residual_ic >= min_residual_ic),
            },
            "diagnostic_samples": {
                "signal_fitted_head": [float(value) for value in signal_fitted[:5]],
                "signal_residual_head": [float(value) for value in signal_resid[:5]],
            },
        }
    )
    return payload


__all__ = [
    "REQUIRED_CONFOUNDER_GROUPS",
    "deconfounded_gate_config",
    "validate_deconfounded_signal",
]
