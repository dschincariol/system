"""Conformal prediction intervals and action-level risk diagnostics.

Calibration is built from matured ``decision_log`` rows joined to ``labels``.
The grouping rule is deliberately simple and point-in-time safe:

1. use exact ``(model_family, horizon_s, symbol)`` residuals when there is
   enough history;
2. otherwise fall back to ``(model_family, horizon_s, asset_class)`` where the
   asset class comes from the static asset map;
3. otherwise use the model-family/horizon global pool.

No model receives order authority from this module.  It only emits interval
and risk-control diagnostics; portfolio sizing and execution suppression
consume those diagnostics through their existing runtime gates.
"""

from __future__ import annotations

import json
import math
import os
import time
from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np

from engine.data.asset_map import asset_class_for_symbol


EPS = 1.0e-12
DEFAULT_ALPHA = float(os.environ.get("CONFORMAL_ALPHA", "0.2"))
DEFAULT_WINDOW = int(os.environ.get("CONFORMAL_WINDOW", "250"))
DEFAULT_MIN_RESIDUALS = int(os.environ.get("CONFORMAL_MIN_RESIDUALS", "40"))
DEFAULT_ACI_GAMMA = float(os.environ.get("CONFORMAL_ACI_GAMMA", "0.005"))
DEFAULT_CONF_SCALE = float(os.environ.get("CONFORMAL_CONF_SCALE", "1.0"))
DEFAULT_SIZE_WIDTH_SCALE = float(os.environ.get("CONFORMAL_SIZE_WIDTH_SCALE", "1.0"))
DEFAULT_RISK_TARGET = float(os.environ.get("CONFORMAL_RISK_TARGET", "0.05") or "0.05")
DEFAULT_RISK_MIN_SAMPLES = int(os.environ.get("CONFORMAL_RISK_MIN_SAMPLES", "40") or "40")
_CACHE_TTL_S = float(os.environ.get("CONFORMAL_CACHE_TTL_S", "30"))
_CALIBRATION_CACHE: dict[tuple[Any, ...], dict[str, Any]] = {}
VALID_RISK_ACTIONS = {"log_only", "size_compress", "suppress", "hard_block"}
VALID_RISK_CONTROL_MODES = {"log_only", "size_compress", "suppress", "hard_block"}
VALID_RISK_LOSSES = {
    "accepted_trade_loss",
    "var_breach",
    "drawdown_contribution",
    "slippage_breach",
    "size_rule_shortfall",
}
VALID_ADAPTIVE_CONTROLLERS = {"aci", "dtaci", "pid"}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _clip01(value: Any) -> float:
    return max(0.0, min(1.0, _safe_float(value, 0.0)))


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(str(name))
    if raw is None:
        return bool(default)
    text = str(raw).strip().lower()
    if not text:
        return bool(default)
    if text in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "f", "no", "n", "off"}:
        return False
    return bool(default)


def conformal_mode() -> str:
    mode = str(os.environ.get("CONFORMAL_MODE", "log_only") or "log_only").strip().lower()
    return mode if mode in {"log_only", "gate", "gate_and_size"} else "log_only"


def target_alpha() -> float:
    return max(0.001, min(0.999, _safe_float(os.environ.get("CONFORMAL_ALPHA"), DEFAULT_ALPHA)))


def calibration_window() -> int:
    return max(10, _safe_int(os.environ.get("CONFORMAL_WINDOW"), DEFAULT_WINDOW))


def min_residuals() -> int:
    return max(5, _safe_int(os.environ.get("CONFORMAL_MIN_RESIDUALS"), DEFAULT_MIN_RESIDUALS))


def aci_gamma() -> float:
    return max(0.0, min(1.0, _safe_float(os.environ.get("CONFORMAL_ACI_GAMMA"), DEFAULT_ACI_GAMMA)))


def adaptive_controller() -> str:
    controller = str(os.environ.get("CONFORMAL_ADAPTIVE_CONTROLLER", "aci") or "aci").strip().lower()
    aliases = {"conformal-pid": "pid", "conformal_pid": "pid", "dt_aci": "dtaci", "dt-aci": "dtaci"}
    controller = aliases.get(controller, controller)
    return controller if controller in VALID_ADAPTIVE_CONTROLLERS else "aci"


def risk_control_enabled() -> bool:
    return _env_bool("CONFORMAL_RISK_CONTROL_ENABLED", False)


def risk_control_mode() -> str:
    mode = str(os.environ.get("CONFORMAL_RISK_CONTROL_MODE", "log_only") or "log_only").strip().lower()
    if mode in {"shrink", "compress"}:
        mode = "size_compress"
    if mode in {"block", "hard"}:
        mode = "hard_block"
    return mode if mode in VALID_RISK_CONTROL_MODES else "log_only"


def risk_loss_definition() -> str:
    value = str(os.environ.get("CONFORMAL_RISK_LOSS", "accepted_trade_loss") or "accepted_trade_loss").strip().lower()
    return value if value in VALID_RISK_LOSSES else "accepted_trade_loss"


def risk_target() -> float:
    return max(0.0001, min(0.999, _safe_float(os.environ.get("CONFORMAL_RISK_TARGET"), DEFAULT_RISK_TARGET)))


def risk_min_samples() -> int:
    return max(5, _safe_int(os.environ.get("CONFORMAL_RISK_MIN_SAMPLES"), DEFAULT_RISK_MIN_SAMPLES))


def symbol_group(symbol: str) -> str:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return "asset:GLOBAL"
    try:
        asset = str(asset_class_for_symbol(sym) or "UNKNOWN").upper().strip()
    except Exception:
        asset = "UNKNOWN"
    if asset in {"EQUITY", "EQUITIES", "US_EQUITY", "STOCK", "STOCKS"}:
        return "asset:EQUITY"
    if asset in {"CRYPTO", "CRYPTOCURRENCY"}:
        return "asset:CRYPTO"
    if asset in {"FX", "FOREX", "CURRENCY"}:
        return "asset:FX"
    if asset in {"COMMODITY", "COMMODITIES"}:
        return "asset:COMMODITY"
    return f"asset:{asset or 'UNKNOWN'}"


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except Exception:
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _family_from_name(name: str) -> str:
    value = str(name or "").strip()
    for family in ("lgbm_ranker", "lgbm_regressor", "xgb_regressor", "patchtst", "gbm_regressor", "embed_regressor"):
        if value == family or value.startswith(f"{family}.") or value.startswith(f"{family}:"):
            return family
    return value.split(".", 1)[0].split(":", 1)[0] if value else ""


def _model_family_from_payload(payload: Mapping[str, Any] | None) -> str:
    obj = dict(payload or {})
    for key in ("served_model_family", "model_family", "requested_model_family", "family"):
        raw = str(obj.get(key) or "").strip()
        if raw:
            return raw
    model = obj.get("model")
    if isinstance(model, Mapping):
        for key in ("served_model_family", "model_family", "family", "name", "model_name"):
            raw = str(model.get(key) or "").strip()
            if raw:
                return _family_from_name(raw)
    for key in ("model_name", "model_id", "model_kind", "model"):
        raw = str(obj.get(key) or "").strip()
        if raw:
            return _family_from_name(raw)
    return ""


def _first_scalar(value: Any) -> Any:
    if isinstance(value, np.ndarray):
        arr = value.reshape(-1)
        return arr[0].item() if arr.size else None
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray, Mapping)):
        return _first_scalar(value[0]) if value else None
    return value


def _finite_or_none(value: Any) -> float | None:
    out = _safe_float(_first_scalar(value), math.nan)
    return float(out) if math.isfinite(out) else None


def _quantile_candidates(payload: Mapping[str, Any] | None) -> list[tuple[str, dict[str, Any]]]:
    obj = dict(payload or {})
    candidates: list[tuple[str, dict[str, Any]]] = [("payload", obj)]
    for key in (
        "quantile_forecast",
        "quantile_forecasts",
        "prediction_quantiles",
        "forecast_quantiles",
        "model_quantiles",
        "cqr",
        "uncertainty_detail",
        "ensemble_output",
        "model",
    ):
        value = obj.get(key)
        if isinstance(value, Mapping):
            candidates.append((key, dict(value)))
    for key in ("explain", "reason", "model_intent", "alpha_intent", "signal"):
        value = obj.get(key)
        if isinstance(value, Mapping):
            candidates.extend(_quantile_candidates(value))
        elif key == "explain" and isinstance(value, str):
            parsed = _json_obj(value)
            if parsed:
                candidates.extend(_quantile_candidates(parsed))
    return candidates


def _extract_quantile_mapping(value: Any, *, prediction: float | None, source: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    obj = dict(value)
    if any(key in obj for key in ("interval_lower", "interval_upper", "interval_width", "interval_excludes_zero")):
        return {}
    method = str(obj.get("method") or obj.get("source") or "").lower()
    if "conformal" in method and source not in {"quantile_forecast", "quantile_forecasts", "prediction_quantiles"}:
        return {}

    lower = _finite_or_none(
        obj.get(
            "prediction_lower",
            obj.get("lower_prediction", obj.get("lower_quantile", obj.get("q_lower", obj.get("p10")))),
        )
    )
    median = _finite_or_none(
        obj.get(
            "prediction_median",
            obj.get("median_prediction", obj.get("median", obj.get("q_median", obj.get("p50")))),
        )
    )
    upper = _finite_or_none(
        obj.get(
            "prediction_upper",
            obj.get("upper_prediction", obj.get("upper_quantile", obj.get("q_upper", obj.get("p90")))),
        )
    )
    lower_q = _finite_or_none(obj.get("lower_q", obj.get("lower_quantile_level", obj.get("alpha_lower"))))
    upper_q = _finite_or_none(obj.get("upper_q", obj.get("upper_quantile_level", obj.get("alpha_upper"))))

    numeric_quantiles: list[tuple[float, float]] = []
    for key, raw_value in obj.items():
        try:
            q = float(str(key))
        except Exception:
            continue
        value_f = _finite_or_none(raw_value)
        if value_f is not None and 0.0 < q < 1.0:
            numeric_quantiles.append((float(q), float(value_f)))
    for key in ("quantiles", "values", "prediction_quantiles", "forecast_quantiles"):
        raw = obj.get(key)
        if isinstance(raw, Mapping):
            for q_raw, value_raw in raw.items():
                q = _finite_or_none(q_raw)
                value_f = _finite_or_none(value_raw)
                if q is not None and value_f is not None and 0.0 < q < 1.0:
                    numeric_quantiles.append((float(q), float(value_f)))
        elif isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
            for item in raw:
                if not isinstance(item, Mapping):
                    continue
                q = _finite_or_none(item.get("q", item.get("quantile", item.get("probability"))))
                value_f = _finite_or_none(item.get("value", item.get("prediction", item.get("forecast"))))
                if q is not None and value_f is not None and 0.0 < q < 1.0:
                    numeric_quantiles.append((float(q), float(value_f)))

    if numeric_quantiles and (lower is None or upper is None):
        numeric_quantiles = sorted(numeric_quantiles, key=lambda pair: pair[0])
        lower_pair = next((pair for pair in numeric_quantiles if pair[0] < 0.5), numeric_quantiles[0])
        upper_pair = next((pair for pair in reversed(numeric_quantiles) if pair[0] > 0.5), numeric_quantiles[-1])
        median_pair = min(numeric_quantiles, key=lambda pair: abs(pair[0] - 0.5))
        lower = float(lower_pair[1])
        upper = float(upper_pair[1])
        median = float(median if median is not None else median_pair[1])
        lower_q = float(lower_q if lower_q is not None else lower_pair[0])
        upper_q = float(upper_q if upper_q is not None else upper_pair[0])

    if lower is None and "lower" in obj and ("median" in obj or "prediction_median" in obj or "upper" in obj):
        lower = _finite_or_none(obj.get("lower"))
    if upper is None and "upper" in obj and ("median" in obj or "prediction_median" in obj or "lower" in obj):
        upper = _finite_or_none(obj.get("upper"))
    if median is None:
        median = _finite_or_none(obj.get("prediction", obj.get("predicted_z", obj.get("score"))))
    if median is None and prediction is not None and math.isfinite(float(prediction)):
        median = float(prediction)

    if lower is None or upper is None:
        return {}
    lo = float(min(lower, upper))
    hi = float(max(lower, upper))
    med = float(median if median is not None else (lo + hi) / 2.0)
    if not all(math.isfinite(v) for v in (lo, hi, med)) or hi <= lo:
        return {}
    return {
        "available": True,
        "lower": float(lo),
        "median": float(med),
        "upper": float(hi),
        "lower_quantile": float(lower_q) if lower_q is not None else None,
        "upper_quantile": float(upper_q) if upper_q is not None else None,
        "source": str(source),
    }


def extract_quantile_forecast(payload: Mapping[str, Any] | None, *, prediction: float | None = None) -> dict[str, Any]:
    """Return lower/median/upper forecast contract if the model exposed one."""

    for source, candidate in _quantile_candidates(payload):
        parsed = _extract_quantile_mapping(candidate, prediction=prediction, source=source)
        if parsed:
            return parsed
    return {}


def _action_rank(action: str) -> int:
    order = {"log_only": 0, "size_compress": 1, "suppress": 2, "hard_block": 3}
    return int(order.get(str(action or "log_only").strip().lower(), 0))


def _cap_action(action: str, mode: str) -> str:
    action_text = str(action or "log_only").strip().lower()
    mode_text = str(mode or "log_only").strip().lower()
    if action_text not in VALID_RISK_ACTIONS:
        action_text = "log_only"
    if mode_text not in VALID_RISK_CONTROL_MODES:
        mode_text = "log_only"
    if _action_rank(action_text) > _action_rank(mode_text):
        return mode_text
    return action_text


def monotone_trading_loss(
    loss_definition: str,
    *,
    prediction: Any,
    realized: Any | None = None,
    payload: Mapping[str, Any] | None = None,
) -> float | None:
    """Compute a non-negative action-level loss proxy from labeled rows."""

    definition = str(loss_definition or "accepted_trade_loss").strip().lower()
    obj = dict(payload or {})
    pred = _finite_or_none(prediction)
    y = _finite_or_none(realized)

    if definition == "slippage_breach":
        slippage = _finite_or_none(obj.get("slippage_bps", obj.get("realized_slippage_bps")))
        threshold = _finite_or_none(obj.get("slippage_threshold_bps", os.environ.get("CONFORMAL_RISK_SLIPPAGE_BPS")))
        if slippage is None:
            return None
        if threshold is None:
            return max(0.0, float(slippage))
        return 1.0 if float(slippage) > float(threshold) else 0.0

    if definition == "size_rule_shortfall":
        explicit = _finite_or_none(obj.get("size_rule_shortfall", obj.get("sizing_shortfall")))
        if explicit is not None:
            return max(0.0, float(explicit))
        requested = _finite_or_none(obj.get("requested_size", obj.get("target_weight")))
        accepted = _finite_or_none(obj.get("accepted_size", obj.get("final_weight")))
        if requested is None or accepted is None:
            return None
        return max(0.0, abs(float(requested)) - abs(float(accepted)))

    if definition == "drawdown_contribution":
        explicit = _finite_or_none(obj.get("drawdown_contribution", obj.get("expected_drawdown_contribution")))
        if explicit is not None:
            return max(0.0, float(explicit))
        if pred is None or y is None:
            return None
        direction = 1.0 if pred >= 0.0 else -1.0
        return max(0.0, -(direction * float(y)))

    if definition == "var_breach":
        if pred is None or y is None:
            return None
        threshold = _finite_or_none(obj.get("var_threshold", os.environ.get("CONFORMAL_RISK_VAR_BREACH_Z")))
        threshold = abs(float(threshold if threshold is not None else 0.0))
        direction = 1.0 if pred >= 0.0 else -1.0
        return 1.0 if (direction * float(y)) < -threshold else 0.0

    if pred is None or y is None:
        return None
    direction = 1.0 if pred >= 0.0 else -1.0
    return max(0.0, -(direction * float(y)))


def current_risk_loss_estimate(payload: Mapping[str, Any] | None, *, loss_definition: str) -> float | None:
    obj = dict(payload or {})
    keys = (
        "conformal_risk_score",
        "risk_loss_estimate",
        "loss_estimate",
        "expected_loss",
        f"{str(loss_definition).lower()}_estimate",
        f"{str(loss_definition).lower()}_forecast",
    )
    for key in keys:
        value = _finite_or_none(obj.get(key))
        if value is not None:
            return max(0.0, float(value))
    for key in ("model_intent", "signal", "risk_control", "conformal_risk_control"):
        nested = obj.get(key)
        if isinstance(nested, Mapping):
            value = current_risk_loss_estimate(nested, loss_definition=loss_definition)
            if value is not None:
                return value
    return None


def conformal_quantile(residuals: Sequence[Any], alpha: float) -> float:
    raw = [] if residuals is None else list(residuals)
    vals = np.asarray([_safe_float(v, math.nan) for v in raw], dtype=np.float64)
    vals = vals[np.isfinite(vals)]
    if vals.size <= 0:
        return 0.0
    vals.sort()
    a = max(0.001, min(0.999, _safe_float(alpha, DEFAULT_ALPHA)))
    n = int(vals.size)
    rank = int(math.ceil((n + 1) * (1.0 - a))) - 1
    rank = max(0, min(n - 1, rank))
    return float(vals[rank])


def _adaptive_step(
    *,
    alpha: float,
    target: float,
    miss: int,
    gamma: float,
    controller: str,
    state: dict[str, Any],
) -> tuple[float, dict[str, Any]]:
    error = float(target) - float(miss)
    controller_name = str(controller or "aci").strip().lower()
    if controller_name == "dtaci":
        miss_streak = int(state.get("miss_streak") or 0)
        miss_streak = miss_streak + 1 if int(miss) else 0
        max_mult = max(1.0, _safe_float(os.environ.get("CONFORMAL_DTACI_MAX_GAMMA_MULT"), 4.0))
        dynamic_mult = min(float(max_mult), 1.0 + float(miss_streak))
        dynamic_gamma = max(0.0, min(1.0, float(gamma) * float(dynamic_mult)))
        alpha_next = float(alpha) + (dynamic_gamma * error)
        state.update(
            {
                "miss_streak": int(miss_streak),
                "gamma_last": float(dynamic_gamma),
                "gamma_multiplier_last": float(dynamic_mult),
                "gamma_multiplier_max": float(max_mult),
            }
        )
    elif controller_name == "pid":
        kp = _safe_float(os.environ.get("CONFORMAL_PID_KP"), 1.0)
        ki = _safe_float(os.environ.get("CONFORMAL_PID_KI"), 0.05)
        kd = _safe_float(os.environ.get("CONFORMAL_PID_KD"), 0.0)
        integral = _safe_float(state.get("integral"), 0.0) + float(error)
        integral_limit = max(1.0, _safe_float(os.environ.get("CONFORMAL_PID_INTEGRAL_LIMIT"), 25.0))
        integral = max(-integral_limit, min(integral_limit, float(integral)))
        prev_error = _safe_float(state.get("prev_error"), 0.0)
        derivative = float(error) - float(prev_error)
        adjustment = float(gamma) * ((float(kp) * float(error)) + (float(ki) * integral) + (float(kd) * derivative))
        alpha_next = float(alpha) + float(adjustment)
        state.update(
            {
                "integral": float(integral),
                "prev_error": float(error),
                "derivative": float(derivative),
                "kp": float(kp),
                "ki": float(ki),
                "kd": float(kd),
                "gamma_last": float(gamma),
            }
        )
    else:
        alpha_next = float(alpha) + (float(gamma) * error)
        state.update({"gamma_last": float(gamma)})
    state["last_error"] = float(error)
    return max(0.001, min(0.999, float(alpha_next))), state


def adaptive_alpha_from_residuals(
    residuals: Sequence[Any],
    *,
    alpha_target: float | None = None,
    gamma: float | None = None,
    window: int | None = None,
    min_history: int | None = None,
    controller: str | None = None,
) -> dict[str, Any]:
    raw = [] if residuals is None else list(residuals)
    vals = [_safe_float(v, math.nan) for v in raw]
    vals = [float(v) for v in vals if math.isfinite(v) and v >= 0.0]
    target = max(0.001, min(0.999, _safe_float(alpha_target, target_alpha())))
    g = max(0.0, min(1.0, _safe_float(gamma, aci_gamma())))
    w = max(10, _safe_int(window, calibration_window()))
    mh = max(5, _safe_int(min_history, min_residuals()))
    controller_name = str(controller or adaptive_controller()).strip().lower()
    if controller_name not in VALID_ADAPTIVE_CONTROLLERS:
        controller_name = "aci"
    alpha = float(target)
    misses = 0
    evaluated = 0
    path: list[float] = [float(alpha)]
    controller_state: dict[str, Any] = {"controller": str(controller_name)}
    for idx, residual in enumerate(vals):
        history = vals[max(0, idx - w):idx]
        if len(history) >= mh:
            q = conformal_quantile(history, alpha)
            miss = 1 if float(residual) > float(q) else 0
            misses += int(miss)
            evaluated += 1
            alpha, controller_state = _adaptive_step(
                alpha=float(alpha),
                target=float(target),
                miss=int(miss),
                gamma=float(g),
                controller=str(controller_name),
                state=controller_state,
            )
            path.append(float(alpha))
    return {
        "alpha": float(alpha),
        "alpha_target": float(target),
        "gamma": float(g),
        "controller": str(controller_name),
        "controller_state": dict(controller_state),
        "evaluated": int(evaluated),
        "misses": int(misses),
        "empirical_miss_rate": float(misses / evaluated) if evaluated else 0.0,
        "path": path,
    }


def evaluate_adaptive_coverage(
    initial_residuals: Sequence[Any],
    realized_residuals: Sequence[Any],
    *,
    alpha_target: float | None = None,
    gamma: float | None = None,
    window: int | None = None,
    min_history: int | None = None,
    controller: str | None = None,
) -> dict[str, Any]:
    initial_raw = [] if initial_residuals is None else list(initial_residuals)
    realized_raw = [] if realized_residuals is None else list(realized_residuals)
    history = [
        float(v)
        for v in (_safe_float(item, math.nan) for item in initial_raw)
        if math.isfinite(v) and v >= 0.0
    ]
    realized = [
        float(v)
        for v in (_safe_float(item, math.nan) for item in realized_raw)
        if math.isfinite(v) and v >= 0.0
    ]
    target = max(0.001, min(0.999, _safe_float(alpha_target, target_alpha())))
    g = max(0.0, min(1.0, _safe_float(gamma, aci_gamma())))
    w = max(10, _safe_int(window, calibration_window()))
    mh = max(5, _safe_int(min_history, min_residuals()))
    controller_name = str(controller or adaptive_controller()).strip().lower()
    if controller_name not in VALID_ADAPTIVE_CONTROLLERS:
        controller_name = "aci"
    alpha = float(target)
    covered: list[bool] = []
    alpha_path = [float(alpha)]
    controller_state: dict[str, Any] = {"controller": str(controller_name)}
    for residual in realized:
        calibration = history[-w:]
        if len(calibration) < mh:
            q = conformal_quantile(calibration, alpha) if calibration else float("inf")
        else:
            q = conformal_quantile(calibration, alpha)
        is_covered = bool(float(residual) <= float(q))
        covered.append(is_covered)
        miss = 0 if is_covered else 1
        alpha, controller_state = _adaptive_step(
            alpha=float(alpha),
            target=float(target),
            miss=int(miss),
            gamma=float(g),
            controller=str(controller_name),
            state=controller_state,
        )
        alpha_path.append(float(alpha))
        history.append(float(residual))
    last_n = min(w, len(covered))
    last = covered[-last_n:] if last_n > 0 else []
    return {
        "coverage": float(sum(1 for item in covered if item) / len(covered)) if covered else 0.0,
        "last_window_coverage": float(sum(1 for item in last if item) / len(last)) if last else 0.0,
        "alpha_final": float(alpha),
        "alpha_path": alpha_path,
        "controller": str(controller_name),
        "controller_state": dict(controller_state),
        "n": int(len(covered)),
    }


def score_interval_from_residuals(
    prediction: float,
    residuals: Sequence[Any],
    *,
    alpha: float | None = None,
    adaptive: bool = True,
    group_key: str = "",
    source: str = "provided",
) -> dict[str, Any]:
    vals = [
        float(v)
        for v in (_safe_float(item, math.nan) for item in list(residuals or []))
        if math.isfinite(v) and v >= 0.0
    ]
    n = int(len(vals))
    min_n = min_residuals()
    mode = conformal_mode()
    if n < int(min_n):
        return {
            "enabled": True,
            "available": False,
            "mode": str(mode),
            "reason": "insufficient_calibration_residuals",
            "n": int(n),
            "min_n": int(min_n),
            "group_key": str(group_key),
            "source": str(source),
            "interval": {
                "available": False,
                "method": "split_conformal_aci",
                "reason": "insufficient_calibration_residuals",
            },
            "calibration": {
                "sample_size": int(n),
                "min_samples": int(min_n),
                "group_key": str(group_key),
                "source": str(source),
            },
        }
    target = max(0.001, min(0.999, _safe_float(alpha, target_alpha())))
    alpha_meta = adaptive_alpha_from_residuals(vals, alpha_target=target) if adaptive else {"alpha": target}
    alpha_eff = max(0.001, min(0.999, _safe_float(alpha_meta.get("alpha"), target)))
    window_vals = vals[-calibration_window():]
    half_width = conformal_quantile(window_vals, alpha_eff)
    yhat = _safe_float(prediction, 0.0)
    lower = float(yhat - half_width)
    upper = float(yhat + half_width)
    interval_width = float(upper - lower)
    excludes_zero = bool(lower > 0.0 or upper < 0.0)
    conf_scale = max(EPS, _safe_float(os.environ.get("CONFORMAL_CONF_SCALE"), DEFAULT_CONF_SCALE))
    size_scale = max(EPS, _safe_float(os.environ.get("CONFORMAL_SIZE_WIDTH_SCALE"), DEFAULT_SIZE_WIDTH_SCALE))
    confidence = max(0.0, min(1.0, 1.0 - ((interval_width / max(abs(yhat), EPS)) * conf_scale)))
    size_mult = 0.0
    if excludes_zero:
        size_mult = max(0.0, min(1.0, abs(yhat) / max(interval_width * size_scale, EPS)))
    interval_diag = {
        "available": True,
        "method": "split_conformal_aci",
        "prediction": float(yhat),
        "lower": float(lower),
        "upper": float(upper),
        "width": float(interval_width),
        "half_width": float(half_width),
        "excludes_zero": bool(excludes_zero),
        "confidence": float(confidence),
        "size_mult": float(size_mult),
        "alpha_target": float(target),
        "alpha_effective": float(alpha_eff),
    }
    calibration_diag = {
        "sample_size": int(n),
        "min_samples": int(min_n),
        "window": int(calibration_window()),
        "group_key": str(group_key),
        "source": str(source),
        "controller": str(alpha_meta.get("controller") or adaptive_controller()),
    }
    return {
        "enabled": True,
        "available": True,
        "method": "split_conformal_aci",
        "mode": str(mode),
        "prediction": float(yhat),
        "lower": float(lower),
        "upper": float(upper),
        "interval_lower": float(lower),
        "interval_upper": float(upper),
        "interval_half_width": float(half_width),
        "interval_width": float(interval_width),
        "interval_excludes_zero": bool(excludes_zero),
        "confidence": float(confidence),
        "size_mult": float(size_mult),
        "alpha_target": float(target),
        "alpha_effective": float(alpha_eff),
        "aci": {k: v for k, v in dict(alpha_meta).items() if k != "path"},
        "controller_state": dict(alpha_meta.get("controller_state") or {}),
        "interval": interval_diag,
        "calibration": calibration_diag,
        "recommended_action": "log_only" if excludes_zero else "suppress",
        "n": int(n),
        "window": int(calibration_window()),
        "group_key": str(group_key),
        "source": str(source),
    }


def score_interval_from_quantile_forecast(
    quantile_forecast: Mapping[str, Any],
    calibration_scores: Sequence[Any],
    *,
    alpha: float | None = None,
    adaptive: bool = True,
    group_key: str = "",
    source: str = "provided",
) -> dict[str, Any]:
    """Score asymmetric conformalized quantile-regression intervals."""

    qf = dict(quantile_forecast or {})
    lower_q = _finite_or_none(qf.get("lower"))
    median_q = _finite_or_none(qf.get("median", qf.get("prediction")))
    upper_q = _finite_or_none(qf.get("upper"))
    vals = [
        float(v)
        for v in (_safe_float(item, math.nan) for item in list(calibration_scores or []))
        if math.isfinite(v) and v >= 0.0
    ]
    n = int(len(vals))
    min_n = min_residuals()
    mode = conformal_mode()
    if lower_q is None or upper_q is None or not math.isfinite(float(lower_q)) or not math.isfinite(float(upper_q)):
        return {
            "enabled": True,
            "available": False,
            "mode": str(mode),
            "method": "conformalized_quantile_regression",
            "reason": "missing_quantile_forecast",
            "n": int(n),
            "min_n": int(min_n),
            "group_key": str(group_key),
            "source": str(source),
            "interval": {
                "available": False,
                "method": "conformalized_quantile_regression",
                "reason": "missing_quantile_forecast",
            },
            "calibration": {
                "sample_size": int(n),
                "min_samples": int(min_n),
                "group_key": str(group_key),
                "source": str(source),
            },
        }
    if n < int(min_n):
        return {
            "enabled": True,
            "available": False,
            "mode": str(mode),
            "method": "conformalized_quantile_regression",
            "reason": "insufficient_cqr_calibration_scores",
            "n": int(n),
            "min_n": int(min_n),
            "group_key": str(group_key),
            "source": str(source),
            "quantile_forecast": dict(qf),
            "interval": {
                "available": False,
                "method": "conformalized_quantile_regression",
                "reason": "insufficient_cqr_calibration_scores",
            },
            "calibration": {
                "sample_size": int(n),
                "min_samples": int(min_n),
                "group_key": str(group_key),
                "source": str(source),
            },
        }

    target = max(0.001, min(0.999, _safe_float(alpha, target_alpha())))
    alpha_meta = adaptive_alpha_from_residuals(vals, alpha_target=target) if adaptive else {"alpha": target}
    alpha_eff = max(0.001, min(0.999, _safe_float(alpha_meta.get("alpha"), target)))
    window_vals = vals[-calibration_window():]
    qhat = conformal_quantile(window_vals, alpha_eff)
    lo_base = float(min(float(lower_q), float(upper_q)))
    hi_base = float(max(float(lower_q), float(upper_q)))
    yhat = float(median_q if median_q is not None else (lo_base + hi_base) / 2.0)
    lower = float(lo_base - qhat)
    upper = float(hi_base + qhat)
    interval_width = float(upper - lower)
    excludes_zero = bool(lower > 0.0 or upper < 0.0)
    conf_scale = max(EPS, _safe_float(os.environ.get("CONFORMAL_CONF_SCALE"), DEFAULT_CONF_SCALE))
    size_scale = max(EPS, _safe_float(os.environ.get("CONFORMAL_SIZE_WIDTH_SCALE"), DEFAULT_SIZE_WIDTH_SCALE))
    confidence = max(0.0, min(1.0, 1.0 - ((interval_width / max(abs(yhat), EPS)) * conf_scale)))
    size_mult = 0.0
    if excludes_zero:
        size_mult = max(0.0, min(1.0, abs(yhat) / max(interval_width * size_scale, EPS)))
    interval_diag = {
        "available": True,
        "method": "conformalized_quantile_regression",
        "prediction": float(yhat),
        "base_lower": float(lo_base),
        "base_upper": float(hi_base),
        "lower": float(lower),
        "upper": float(upper),
        "width": float(interval_width),
        "conformal_score_quantile": float(qhat),
        "excludes_zero": bool(excludes_zero),
        "confidence": float(confidence),
        "size_mult": float(size_mult),
        "alpha_target": float(target),
        "alpha_effective": float(alpha_eff),
    }
    calibration_diag = {
        "sample_size": int(n),
        "min_samples": int(min_n),
        "window": int(calibration_window()),
        "group_key": str(group_key),
        "source": str(source),
        "controller": str(alpha_meta.get("controller") or adaptive_controller()),
        "cqr_sample_size": int(n),
    }
    return {
        "enabled": True,
        "available": True,
        "method": "conformalized_quantile_regression",
        "mode": str(mode),
        "prediction": float(yhat),
        "lower": float(lower),
        "upper": float(upper),
        "interval_lower": float(lower),
        "interval_upper": float(upper),
        "interval_half_width": None,
        "interval_width": float(interval_width),
        "interval_excludes_zero": bool(excludes_zero),
        "confidence": float(confidence),
        "size_mult": float(size_mult),
        "alpha_target": float(target),
        "alpha_effective": float(alpha_eff),
        "aci": {k: v for k, v in dict(alpha_meta).items() if k != "path"},
        "controller_state": dict(alpha_meta.get("controller_state") or {}),
        "quantile_forecast": dict(qf),
        "interval": interval_diag,
        "calibration": calibration_diag,
        "recommended_action": "log_only" if excludes_zero else "suppress",
        "n": int(n),
        "window": int(calibration_window()),
        "group_key": str(group_key),
        "source": str(source),
    }


def calibrate_conformal_risk_control(
    losses: Sequence[Any],
    *,
    target_risk: float | None = None,
    loss_definition: str | None = None,
    current_loss: float | None = None,
    mode: str | None = None,
    min_samples: int | None = None,
) -> dict[str, Any]:
    """Calibrate a monotone action-level loss threshold and advisory action."""

    enabled = risk_control_enabled()
    mode_text = str(mode or risk_control_mode()).strip().lower()
    if mode_text in {"shrink", "compress"}:
        mode_text = "size_compress"
    if mode_text not in VALID_RISK_CONTROL_MODES:
        mode_text = "log_only"
    definition = str(loss_definition or risk_loss_definition()).strip().lower()
    if definition not in VALID_RISK_LOSSES:
        definition = "accepted_trade_loss"
    vals = [
        float(v)
        for v in (_safe_float(item, math.nan) for item in list(losses or []))
        if math.isfinite(v) and v >= 0.0
    ]
    n = int(len(vals))
    min_n = max(5, _safe_int(min_samples, risk_min_samples()))
    target = max(0.0001, min(0.999, _safe_float(target_risk, risk_target())))
    base = {
        "enabled": bool(enabled),
        "available": False,
        "mode": str(mode_text),
        "loss_definition": str(definition),
        "target_risk": float(target),
        "sample_size": int(n),
        "min_samples": int(min_n),
        "recommended_action": "log_only",
        "raw_recommended_action": "log_only",
        "size_mult": 1.0,
    }
    if not enabled:
        return {**base, "reason": "disabled"}
    if n < int(min_n):
        return {**base, "reason": "insufficient_risk_calibration"}

    threshold = conformal_quantile(vals, target)
    exceedances = int(sum(1 for value in vals if float(value) > float(threshold)))
    empirical_risk = float(exceedances / n) if n else 0.0
    current = None if current_loss is None else max(0.0, _safe_float(current_loss, math.nan))
    ratio = None
    raw_action = "log_only"
    size_mult = 1.0
    if current is not None and math.isfinite(float(current)):
        denom = max(float(threshold), EPS)
        ratio = float(current / denom)
        if current > threshold:
            if ratio >= 2.0:
                raw_action = "hard_block"
                size_mult = 0.0
            elif ratio >= 1.25:
                raw_action = "suppress"
                size_mult = 0.0
            else:
                raw_action = "size_compress"
                size_mult = max(0.0, min(1.0, float(threshold) / max(float(current), EPS)))
    action = _cap_action(raw_action, mode_text)
    if action in {"suppress", "hard_block"}:
        size_mult = 0.0
    elif action == "log_only":
        size_mult = 1.0
    return {
        **base,
        "available": True,
        "reason": "",
        "calibrated_loss_threshold": float(threshold),
        "realized_empirical_risk": float(empirical_risk),
        "empirical_risk": float(empirical_risk),
        "exceedances": int(exceedances),
        "current_loss": current,
        "current_loss_ratio": ratio,
        "recommended_action": str(action),
        "raw_recommended_action": str(raw_action),
        "size_mult": float(max(0.0, min(1.0, size_mult))),
    }


def _fetch_labeled_decision_rows(con, *, horizon_s: int, as_of_ts_ms: int, limit: int) -> list[dict[str, Any]]:
    rows = con.execute(
        """
        SELECT d.ts_ms, d.symbol, d.horizon_s, d.predicted_z,
               d.model_name, COALESCE(d.model_kind, ''), COALESCE(d.explain_json, '{}'),
               COALESCE(l.impact_z, l.realized_ret), COALESCE(l.created_at_ms, d.ts_ms)
        FROM decision_log d
        JOIN labels l
          ON l.event_id=d.event_id
         AND l.symbol=d.symbol
         AND l.horizon_s=d.horizon_s
        WHERE d.horizon_s=?
          AND d.predicted_z IS NOT NULL
          AND (l.impact_z IS NOT NULL OR l.realized_ret IS NOT NULL)
          AND COALESCE(l.created_at_ms, d.ts_ms) <= ?
        ORDER BY COALESCE(l.created_at_ms, d.ts_ms) DESC
        LIMIT ?
        """,
        (int(horizon_s), int(as_of_ts_ms), int(limit)),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows or []:
        explain = _json_obj(row[6] if len(row) > 6 else "{}")
        model_family = _model_family_from_payload(explain) or _family_from_name(str(row[4] or row[5] or ""))
        symbol = str(row[1] or "").upper().strip()
        pred = _safe_float(row[3], math.nan)
        y = _safe_float(row[7], math.nan)
        if not symbol or not math.isfinite(pred) or not math.isfinite(y):
            continue
        quantile_forecast = extract_quantile_forecast(explain, prediction=pred)
        cqr_score = math.nan
        if quantile_forecast:
            q_lower = _safe_float(quantile_forecast.get("lower"), math.nan)
            q_upper = _safe_float(quantile_forecast.get("upper"), math.nan)
            if math.isfinite(q_lower) and math.isfinite(q_upper):
                lo = min(float(q_lower), float(q_upper))
                hi = max(float(q_lower), float(q_upper))
                cqr_score = max(float(lo) - float(y), float(y) - float(hi), 0.0)
        out.append(
            {
                "ts_ms": _safe_int(row[0], 0),
                "label_ts_ms": _safe_int(row[8], 0),
                "symbol": symbol,
                "symbol_group": symbol_group(symbol),
                "horizon_s": _safe_int(row[2], 0),
                "prediction": float(pred),
                "realized": float(y),
                "residual": abs(float(y) - float(pred)),
                "cqr_score": float(cqr_score) if math.isfinite(cqr_score) else None,
                "quantile_forecast": dict(quantile_forecast),
                "explain": dict(explain),
                "model_family": str(model_family),
            }
        )
    return out


def load_conformal_residuals(
    con,
    *,
    model_family: str,
    horizon_s: int,
    symbol: str,
    as_of_ts_ms: int | None = None,
) -> dict[str, Any]:
    family = str(model_family or "").strip()
    sym = str(symbol or "").upper().strip()
    h = int(horizon_s or 0)
    as_of = int(as_of_ts_ms or time.time() * 1000)
    loss_def = risk_loss_definition()
    key = (family, h, sym, str(loss_def), int(as_of // max(1, int(_CACHE_TTL_S * 1000))))
    cached = _CALIBRATION_CACHE.get(key)
    now_s = time.time()
    if cached and (now_s - float(cached.get("ts_s") or 0.0)) < float(_CACHE_TTL_S):
        return dict(cached.get("payload") or {})

    window = calibration_window()
    rows = _fetch_labeled_decision_rows(
        con,
        horizon_s=int(h),
        as_of_ts_ms=int(as_of),
        limit=max(window * 25, min_residuals() * 20),
    )
    if family:
        rows = [row for row in rows if str(row.get("model_family") or "") == family]
    rows = sorted(rows, key=lambda row: int(row.get("label_ts_ms") or row.get("ts_ms") or 0))
    exact = [row for row in rows if str(row.get("symbol") or "") == sym]
    group_key = symbol_group(sym)
    grouped = [row for row in rows if str(row.get("symbol_group") or "") == group_key]
    source = "family_horizon_global"
    chosen = rows
    chosen_key = f"{family or '*'}:{h}:global"
    if len(exact) >= min_residuals():
        chosen = exact
        source = "exact_symbol"
        chosen_key = f"{family or '*'}:{h}:{sym}"
    elif len(grouped) >= min_residuals():
        chosen = grouped
        source = "symbol_group"
        chosen_key = f"{family or '*'}:{h}:{group_key}"
    residuals = [float(row.get("residual") or 0.0) for row in chosen[-window:]]
    cqr_scores = [
        float(row.get("cqr_score"))
        for row in chosen[-window:]
        if _finite_or_none(row.get("cqr_score")) is not None and float(row.get("cqr_score")) >= 0.0
    ]
    risk_losses: list[float] = []
    for row in chosen[-window:]:
        loss = monotone_trading_loss(
            loss_def,
            prediction=row.get("prediction"),
            realized=row.get("realized"),
            payload=dict(row.get("explain") or {}),
        )
        if loss is not None and math.isfinite(float(loss)) and float(loss) >= 0.0:
            risk_losses.append(float(loss))
    payload = {
        "model_family": str(family),
        "horizon_s": int(h),
        "symbol": str(sym),
        "symbol_group": str(group_key),
        "group_key": str(chosen_key),
        "source": str(source),
        "residuals": residuals,
        "cqr_scores": cqr_scores,
        "risk_losses": risk_losses,
        "risk_loss_definition": str(loss_def),
        "n": int(len(residuals)),
        "n_cqr": int(len(cqr_scores)),
        "n_risk": int(len(risk_losses)),
    }
    _CALIBRATION_CACHE[key] = {"ts_s": float(now_s), "payload": dict(payload)}
    return payload


def score_live_conformal(
    con,
    *,
    symbol: str,
    horizon_s: int,
    prediction: float,
    model_family: str = "",
    explain: Mapping[str, Any] | None = None,
    as_of_ts_ms: int | None = None,
) -> dict[str, Any]:
    try:
        calib = load_conformal_residuals(
            con,
            model_family=str(model_family or ""),
            horizon_s=int(horizon_s),
            symbol=str(symbol),
            as_of_ts_ms=as_of_ts_ms,
        )
        explain_obj = dict(explain or {})
        quantile_forecast = extract_quantile_forecast(explain_obj, prediction=float(prediction))
        cqr_scores = list(calib.get("cqr_scores") or [])
        if quantile_forecast and len(cqr_scores) >= min_residuals():
            result = score_interval_from_quantile_forecast(
                quantile_forecast,
                cqr_scores,
                group_key=str(calib.get("group_key") or ""),
                source=str(calib.get("source") or "decision_log_labels"),
            )
        else:
            result = score_interval_from_residuals(
                float(prediction),
                list(calib.get("residuals") or []),
                group_key=str(calib.get("group_key") or ""),
                source=str(calib.get("source") or "decision_log_labels"),
            )
            result["cqr"] = {
                "available": False,
                "current_quantile_forecast_available": bool(quantile_forecast),
                "calibration_sample_size": int(len(cqr_scores)),
                "min_samples": int(min_residuals()),
                "fallback_reason": (
                    "missing_current_quantile_forecast"
                    if not quantile_forecast
                    else "insufficient_cqr_calibration_scores"
                ),
            }

        loss_def = str(calib.get("risk_loss_definition") or risk_loss_definition())
        current_loss = current_risk_loss_estimate(explain_obj, loss_definition=loss_def)
        risk_diag = calibrate_conformal_risk_control(
            list(calib.get("risk_losses") or []),
            target_risk=risk_target(),
            loss_definition=loss_def,
            current_loss=current_loss,
            mode=risk_control_mode(),
            min_samples=risk_min_samples(),
        )
        interval_action = str(result.get("recommended_action") or "log_only").strip().lower()
        risk_action = str(risk_diag.get("recommended_action") or "log_only").strip().lower()
        recommended = interval_action if _action_rank(interval_action) >= _action_rank(risk_action) else risk_action
        risk_mult = max(0.0, min(1.0, _safe_float(risk_diag.get("size_mult"), 1.0)))
        if risk_mult < 1.0:
            result["size_mult"] = min(max(0.0, min(1.0, _safe_float(result.get("size_mult"), 1.0))), float(risk_mult))
            if isinstance(result.get("interval"), Mapping):
                interval_obj = dict(result.get("interval") or {})
                interval_obj["size_mult"] = float(result["size_mult"])
                result["interval"] = interval_obj
        calibration_obj = dict(result.get("calibration") or {})
        calibration_obj.update(
            {
                "sample_size": int(result.get("n") or calibration_obj.get("sample_size") or 0),
                "residual_sample_size": int(calib.get("n") or 0),
                "cqr_sample_size": int(calib.get("n_cqr") or 0),
                "risk_sample_size": int(calib.get("n_risk") or 0),
                "risk_min_samples": int(risk_min_samples()),
                "risk_loss_definition": str(loss_def),
            }
        )
        result["calibration"] = calibration_obj
        result["risk_control"] = dict(risk_diag)
        result["recommended_action"] = str(recommended if recommended in VALID_RISK_ACTIONS else "log_only")
        result.update(
            {
                "model_family": str(model_family or ""),
                "symbol": str(symbol or "").upper().strip(),
                "horizon_s": int(horizon_s),
                "symbol_group": str(calib.get("symbol_group") or symbol_group(str(symbol))),
                "schema_version": "conformal_v2",
            }
        )
        return result
    except Exception as exc:
        return {
            "enabled": True,
            "available": False,
            "mode": conformal_mode(),
            "reason": f"score_failed:{type(exc).__name__}",
            "error": str(exc),
            "symbol": str(symbol or "").upper().strip(),
            "horizon_s": int(horizon_s or 0),
            "model_family": str(model_family or ""),
        }


def apply_conformal_to_explain(
    *,
    con,
    symbol: str,
    horizon_s: int,
    prediction: float,
    confidence: float,
    explain: Mapping[str, Any] | None,
    signal_ts_ms: int | None = None,
) -> tuple[float, dict[str, Any], dict[str, Any]]:
    out = dict(explain or {})
    family = _model_family_from_payload(out)
    result = score_live_conformal(
        con,
        symbol=str(symbol),
        horizon_s=int(horizon_s),
        prediction=float(prediction),
        model_family=str(family),
        explain=out,
        as_of_ts_ms=signal_ts_ms,
    )
    out["conformal"] = dict(result)
    if bool(result.get("available")):
        out["conformal_interval_lower"] = float(result.get("interval_lower") or 0.0)
        out["conformal_interval_upper"] = float(result.get("interval_upper") or 0.0)
        out["conformal_interval_width"] = float(result.get("interval_width") or 0.0)
        out["conformal_interval_excludes_zero"] = bool(result.get("interval_excludes_zero"))
        out["interval_excludes_zero"] = bool(result.get("interval_excludes_zero"))
        out["conformal_confidence"] = float(result.get("confidence") or 0.0)
        out["conformal_size_mult"] = float(result.get("size_mult") or 0.0)
    if isinstance(result.get("risk_control"), Mapping):
        out["conformal_risk_control"] = dict(result.get("risk_control") or {})
    if result.get("recommended_action") is not None:
        out["conformal_recommended_action"] = str(result.get("recommended_action") or "log_only")
    final_conf = _clip01(confidence)
    if bool(result.get("available")) and conformal_mode() in {"gate", "gate_and_size"}:
        final_conf = _clip01(result.get("confidence"))
        out["raw_confidence_before_conformal"] = _clip01(confidence)
        out["confidence"] = float(final_conf)
        out["probability"] = float(final_conf)
        out["uncertainty"] = float(1.0 - final_conf)
        engine_blob = dict(out.get("confidence_engine") or {})
        engine_blob["conformal"] = dict(result)
        engine_blob["confidence"] = float(final_conf)
        out["confidence_engine"] = engine_blob
    return float(final_conf), out, result


def _candidate_payloads(payload: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    obj = dict(payload or {})
    candidates: list[dict[str, Any]] = [obj]
    for key in ("explain", "reason", "model_intent", "alpha_intent", "signal"):
        value = obj.get(key)
        if isinstance(value, Mapping):
            candidates.append(dict(value))
        elif key == "explain" and isinstance(value, str):
            parsed = _json_obj(value)
            if parsed:
                candidates.append(parsed)
    explain_json = obj.get("explain_json")
    parsed_explain = _json_obj(explain_json)
    if parsed_explain:
        candidates.append(parsed_explain)
    for candidate in list(candidates):
        for key in ("conformal", "model_intent", "reason", "signal"):
            nested = candidate.get(key)
            if isinstance(nested, Mapping):
                candidates.append(dict(nested))
    return candidates


def extract_conformal_payload(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    for candidate in _candidate_payloads(payload):
        nested = candidate.get("conformal")
        if isinstance(nested, Mapping):
            nested_dict = dict(nested)
            if nested_dict.get("interval_excludes_zero") is not None or nested_dict.get("available") is not None:
                return nested_dict
        if candidate.get("conformal_interval_excludes_zero") is not None or candidate.get("interval_excludes_zero") is not None:
            return {
                "enabled": True,
                "available": True,
                "mode": str(candidate.get("conformal_mode") or conformal_mode()),
                "interval_excludes_zero": bool(
                    candidate.get("conformal_interval_excludes_zero", candidate.get("interval_excludes_zero"))
                ),
                "interval_lower": _safe_float(candidate.get("conformal_interval_lower", candidate.get("interval_lower")), 0.0),
                "interval_upper": _safe_float(candidate.get("conformal_interval_upper", candidate.get("interval_upper")), 0.0),
                "interval_width": _safe_float(candidate.get("conformal_interval_width", candidate.get("interval_width")), 0.0),
                "confidence": _safe_float(candidate.get("conformal_confidence", candidate.get("confidence")), 0.0),
                "size_mult": _safe_float(candidate.get("conformal_size_mult", candidate.get("size_mult")), 1.0),
                "recommended_action": str(candidate.get("conformal_recommended_action") or "log_only"),
                "risk_control": dict(candidate.get("conformal_risk_control") or {}),
            }
    return {}


def conformal_gate_from_payload(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    mode = conformal_mode()
    conf = extract_conformal_payload(payload)
    if not conf:
        return {"enabled": False, "applied": False, "mode": str(mode), "hard_block": False, "reason": "missing_conformal_interval"}
    available = bool(conf.get("available", True))
    excludes_zero = bool(conf.get("interval_excludes_zero"))
    risk = dict(conf.get("risk_control") or {})
    risk_action = str(risk.get("recommended_action") or conf.get("recommended_action") or "log_only").strip().lower()
    risk_mode = risk_control_mode()
    risk_size_mult = max(0.0, min(1.0, _safe_float(risk.get("size_mult"), 1.0)))
    interval_hard_block = bool(available and mode in {"gate", "gate_and_size"} and not excludes_zero)
    risk_hard_block = bool(
        available
        and bool(risk.get("enabled"))
        and risk_mode in {"suppress", "hard_block"}
        and risk_action in {"suppress", "hard_block"}
    )
    hard_block = bool(interval_hard_block or risk_hard_block)
    size_mult = max(0.0, min(1.0, _safe_float(conf.get("size_mult"), 1.0)))
    if bool(risk.get("enabled")) and risk_mode in {"size_compress", "suppress", "hard_block"}:
        size_mult = min(float(size_mult), float(risk_size_mult))
    if hard_block:
        action = "HARD_BLOCK"
    elif bool(risk.get("enabled")) and risk_action == "size_compress" and risk_mode in {"size_compress", "suppress", "hard_block"}:
        action = "SIZE_COMPRESSION"
    elif available and not excludes_zero:
        action = "LOG_ONLY"
    elif bool(risk.get("enabled")) and risk_action != "log_only":
        action = "LOG_ONLY"
    else:
        action = "NONE"
    reason = ""
    if interval_hard_block or (available and not excludes_zero):
        reason = "interval_straddles_zero"
    if risk_hard_block:
        reason = "risk_control_" + str(risk_action)
    elif action == "SIZE_COMPRESSION":
        reason = "risk_control_size_compress"
    return {
        "enabled": True,
        "available": bool(available),
        "applied": bool(hard_block or action == "SIZE_COMPRESSION"),
        "mode": str(mode),
        "source": "conformal_risk_control" if reason.startswith("risk_control") else "conformal_interval",
        "action": str(action),
        "recommended_action": str(risk_action if _action_rank(risk_action) > _action_rank(conf.get("recommended_action")) else conf.get("recommended_action") or "log_only"),
        "hard_block": bool(hard_block),
        "interval_excludes_zero": bool(excludes_zero),
        "interval_lower": _safe_float(conf.get("interval_lower", conf.get("lower")), 0.0),
        "interval_upper": _safe_float(conf.get("interval_upper", conf.get("upper")), 0.0),
        "interval_width": _safe_float(conf.get("interval_width"), 0.0),
        "confidence": _clip01(conf.get("confidence")),
        "size_mult": float(size_mult),
        "risk_control": dict(risk),
        "reason": str(reason),
    }
