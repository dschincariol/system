"""Split-conformal prediction intervals for model confidence and sizing.

Calibration is built from matured ``decision_log`` rows joined to ``labels``.
The grouping rule is deliberately simple and point-in-time safe:

1. use exact ``(model_family, horizon_s, symbol)`` residuals when there is
   enough history;
2. otherwise fall back to ``(model_family, horizon_s, asset_class)`` where the
   asset class comes from the static asset map;
3. otherwise use the model-family/horizon global pool.

No model receives order authority from this module.  It only emits interval
diagnostics; portfolio sizing and execution suppression consume those
diagnostics through their existing runtime gates.
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
_CACHE_TTL_S = float(os.environ.get("CONFORMAL_CACHE_TTL_S", "30"))
_CALIBRATION_CACHE: dict[tuple[Any, ...], dict[str, Any]] = {}


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


def adaptive_alpha_from_residuals(
    residuals: Sequence[Any],
    *,
    alpha_target: float | None = None,
    gamma: float | None = None,
    window: int | None = None,
    min_history: int | None = None,
) -> dict[str, Any]:
    raw = [] if residuals is None else list(residuals)
    vals = [_safe_float(v, math.nan) for v in raw]
    vals = [float(v) for v in vals if math.isfinite(v) and v >= 0.0]
    target = max(0.001, min(0.999, _safe_float(alpha_target, target_alpha())))
    g = max(0.0, min(1.0, _safe_float(gamma, aci_gamma())))
    w = max(10, _safe_int(window, calibration_window()))
    mh = max(5, _safe_int(min_history, min_residuals()))
    alpha = float(target)
    misses = 0
    evaluated = 0
    path: list[float] = [float(alpha)]
    for idx, residual in enumerate(vals):
        history = vals[max(0, idx - w):idx]
        if len(history) >= mh:
            q = conformal_quantile(history, alpha)
            miss = 1 if float(residual) > float(q) else 0
            misses += int(miss)
            evaluated += 1
            alpha = max(0.001, min(0.999, alpha + (g * (target - float(miss)))))
            path.append(float(alpha))
    return {
        "alpha": float(alpha),
        "alpha_target": float(target),
        "gamma": float(g),
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
    alpha = float(target)
    covered: list[bool] = []
    alpha_path = [float(alpha)]
    for residual in realized:
        calibration = history[-w:]
        if len(calibration) < mh:
            q = conformal_quantile(calibration, alpha) if calibration else float("inf")
        else:
            q = conformal_quantile(calibration, alpha)
        is_covered = bool(float(residual) <= float(q))
        covered.append(is_covered)
        miss = 0 if is_covered else 1
        alpha = max(0.001, min(0.999, alpha + (g * (target - float(miss)))))
        alpha_path.append(float(alpha))
        history.append(float(residual))
    last_n = min(w, len(covered))
    last = covered[-last_n:] if last_n > 0 else []
    return {
        "coverage": float(sum(1 for item in covered if item) / len(covered)) if covered else 0.0,
        "last_window_coverage": float(sum(1 for item in last if item) / len(last)) if last else 0.0,
        "alpha_final": float(alpha),
        "alpha_path": alpha_path,
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
        "n": int(n),
        "window": int(calibration_window()),
        "group_key": str(group_key),
        "source": str(source),
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
    key = (family, h, sym, int(as_of // max(1, int(_CACHE_TTL_S * 1000))))
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
    payload = {
        "model_family": str(family),
        "horizon_s": int(h),
        "symbol": str(sym),
        "symbol_group": str(group_key),
        "group_key": str(chosen_key),
        "source": str(source),
        "residuals": residuals,
        "n": int(len(residuals)),
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
        result = score_interval_from_residuals(
            float(prediction),
            list(calib.get("residuals") or []),
            group_key=str(calib.get("group_key") or ""),
            source=str(calib.get("source") or "decision_log_labels"),
        )
        result.update(
            {
                "model_family": str(model_family or ""),
                "symbol": str(symbol or "").upper().strip(),
                "horizon_s": int(horizon_s),
                "symbol_group": str(calib.get("symbol_group") or symbol_group(str(symbol))),
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
            }
    return {}


def conformal_gate_from_payload(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    mode = conformal_mode()
    conf = extract_conformal_payload(payload)
    if not conf:
        return {"enabled": False, "applied": False, "mode": str(mode), "hard_block": False, "reason": "missing_conformal_interval"}
    available = bool(conf.get("available", True))
    excludes_zero = bool(conf.get("interval_excludes_zero"))
    hard_block = bool(available and mode in {"gate", "gate_and_size"} and not excludes_zero)
    action = "HARD_BLOCK" if hard_block else ("LOG_ONLY" if available and not excludes_zero else "NONE")
    return {
        "enabled": True,
        "available": bool(available),
        "applied": bool(hard_block),
        "mode": str(mode),
        "source": "conformal_interval",
        "action": str(action),
        "hard_block": bool(hard_block),
        "interval_excludes_zero": bool(excludes_zero),
        "interval_lower": _safe_float(conf.get("interval_lower", conf.get("lower")), 0.0),
        "interval_upper": _safe_float(conf.get("interval_upper", conf.get("upper")), 0.0),
        "interval_width": _safe_float(conf.get("interval_width"), 0.0),
        "confidence": _clip01(conf.get("confidence")),
        "size_mult": max(0.0, min(1.0, _safe_float(conf.get("size_mult"), 1.0))),
        "reason": "interval_straddles_zero" if available and not excludes_zero else "",
    }
