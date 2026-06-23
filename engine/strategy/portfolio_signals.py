"""Signal normalization and candidate-ranking helpers for portfolio construction."""

from __future__ import annotations

import json
import math
from typing import Any, Callable, Dict, List, Optional, Tuple


WarnFn = Callable[..., None]


def novelty_from_explain(
    explain_json: str, *, warn_nonfatal: WarnFn | None = None
) -> float:
    try:
        parsed = json.loads(explain_json or "{}")
        meta = parsed.get("event_meta") if isinstance(parsed, dict) else None
        if not isinstance(meta, dict):
            return 0.0
        value = float(meta.get("novelty", 0.0))
        if value != value:
            return 0.0
        return max(0.0, min(1.0, value))
    except Exception as exc:
        if warn_nonfatal is not None:
            warn_nonfatal(
                "PORTFOLIO_EXPLAIN_NOVELTY_FAILED", exc, once_key="explain_novelty"
            )
        return 0.0


def safe_json_obj(raw: Any, *, warn_nonfatal: WarnFn | None = None) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if not isinstance(raw, str):
        return {}
    try:
        parsed = json.loads(raw or "{}")
    except Exception as exc:
        if warn_nonfatal is not None:
            warn_nonfatal(
                "PORTFOLIO_SAFE_JSON_OBJ_FAILED",
                exc,
                once_key="safe_json_obj",
                raw_type=type(raw).__name__,
            )
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def coerce_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in (
        "1",
        "true",
        "yes",
        "y",
        "on",
        "enter",
        "trade",
        "buy",
        "sell",
        "long",
        "short",
    ):
        return True
    if text in ("0", "false", "no", "n", "off", "hold", "flat", "skip", "none"):
        return False
    return None


def coerce_float(value: Any, *, warn_nonfatal: WarnFn | None = None) -> Optional[float]:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception as exc:
        if warn_nonfatal is not None:
            warn_nonfatal(
                "PORTFOLIO_COERCE_FLOAT_FAILED",
                exc,
                once_key="coerce_float_failed",
                value_type=type(value).__name__,
            )
        return None
    return out if math.isfinite(out) else None


def intent_container_candidates(
    explain: Dict[str, Any],
    *,
    is_canonical_model_intent_fn: Callable[[Any], bool],
    dict_str_any_fn: Callable[[Any], Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not isinstance(explain, dict):
        return []
    out: List[Dict[str, Any]] = []
    canonical = explain.get("model_intent")
    if is_canonical_model_intent_fn(canonical):
        return [dict_str_any_fn(canonical)]
    keys = (
        "model_intent",
        "model_output",
        "portfolio_decision",
        "trade_decision",
        "decision",
        "signal",
        "strategy_output",
        "portfolio",
        "execution",
    )
    for key in keys:
        value = explain.get(key)
        if isinstance(value, dict):
            out.append(dict(value))
    direct_keys = {
        "should_trade",
        "trade",
        "action",
        "target_weight",
        "portfolio_weight",
        "position_size",
        "size_mult",
        "selection_score",
        "trade_score",
        "include_in_universe",
        "universe_score",
        "selected_features",
        "features_used",
    }
    if any(key in explain for key in direct_keys):
        out.append(dict(explain))
    return out


def extract_model_intent_from_explain(
    explain_json: str,
    *,
    safe_json_obj_fn: Callable[[Any], Dict[str, Any]],
    intent_container_candidates_fn: Callable[[Dict[str, Any]], List[Dict[str, Any]]],
    is_canonical_model_intent_fn: Callable[[Any], bool],
    dict_str_any_fn: Callable[[Any], Dict[str, Any]],
    coerce_float_fn: Callable[[Any], Optional[float]],
    coerce_bool_fn: Callable[[Any], Optional[bool]],
) -> Dict[str, Any]:
    explain = safe_json_obj_fn(explain_json)
    canonical = explain.get("model_intent")
    if is_canonical_model_intent_fn(canonical):
        return dict_str_any_fn(canonical)
    intent: Dict[str, Any] = {}

    for container in intent_container_candidates_fn(explain):
        if not isinstance(container, dict):
            continue

        for key in (
            "selection_score",
            "trade_score",
            "score",
            "prediction_strength",
            "priority",
            "rank_score",
        ):
            value = coerce_float_fn(container.get(key))
            if value is not None:
                intent["score"] = float(value)
                break

        for key in (
            "target_weight",
            "portfolio_weight",
            "target_exposure",
            "notional_frac",
            "size",
            "position_size",
        ):
            value = coerce_float_fn(container.get(key))
            if value is not None:
                intent["target_weight"] = float(value)
                break

        for key in (
            "size_mult",
            "size_factor",
            "allocation_multiplier",
            "weight_multiplier",
        ):
            value = coerce_float_fn(container.get(key))
            if value is not None:
                intent["size_mult"] = float(value)
                break

        for key in (
            "confidence",
            "signal_confidence",
            "trade_confidence",
            "probability",
        ):
            value = coerce_float_fn(container.get(key))
            if value is not None:
                intent["confidence"] = float(value)
                break

        for key in ("prediction_strength", "signal_strength", "strength"):
            value = coerce_float_fn(container.get(key))
            if value is not None:
                intent["prediction_strength"] = float(value)
                intent.setdefault("score", float(value))
                break

        for key in ("expected_z", "predicted_z", "signal_z"):
            value = coerce_float_fn(container.get(key))
            if value is not None:
                intent["expected_z"] = float(value)
                break

        for key in ("side", "direction", "action"):
            raw = container.get(key)
            if raw is None:
                continue
            side = str(raw).strip().upper()
            if side in ("BUY", "LONG"):
                intent["side"] = "LONG"
                break
            if side in ("SELL", "SHORT"):
                intent["side"] = "SHORT"
                break
            if side in ("FLAT", "HOLD", "SKIP", "NONE"):
                intent["side"] = "FLAT"
                break

        for key in ("should_trade", "trade", "enter", "allow_trade"):
            value = coerce_bool_fn(container.get(key))
            if value is not None:
                intent["should_trade"] = bool(value)
                break

        for key in ("timing", "entry_timing", "trade_timing", "when"):
            raw = container.get(key)
            if raw is None:
                continue
            timing = str(raw).strip().lower()
            if timing:
                intent["timing"] = timing
                break

        for key in ("selected_features", "features_used", "feature_ids", "feature_set"):
            raw = container.get(key)
            if isinstance(raw, list):
                features = [
                    str(value).strip() for value in raw if str(value or "").strip()
                ]
                if features:
                    intent["selected_features"] = features
                    break

        for key in ("include_in_universe", "universe_include", "promote_symbol"):
            value = coerce_bool_fn(container.get(key))
            if value is not None:
                intent["include_in_universe"] = bool(value)
                break

        for key in ("universe_score", "universe_rank", "rank"):
            value = coerce_float_fn(container.get(key))
            if value is not None:
                intent["universe_score"] = float(value)
                break

    return intent


def has_explicit_model_trade_intent(intent: Optional[Dict[str, Any]]) -> bool:
    intent = intent or {}
    return any(
        key in intent
        for key in (
            "should_trade",
            "target_weight",
            "score",
            "side",
            "timing",
            "selected_features",
        )
    )


def has_canonical_model_trade_intent(
    intent: Optional[Dict[str, Any]],
    *,
    is_canonical_model_intent_fn: Callable[[Any], bool],
    has_explicit_model_trade_intent_fn: Callable[[Optional[Dict[str, Any]]], bool],
) -> bool:
    return is_canonical_model_intent_fn(intent) and has_explicit_model_trade_intent_fn(
        intent
    )


def model_intent_allows_symbol(
    intent: Optional[Dict[str, Any]],
    *,
    coerce_float_fn: Callable[[Any], Optional[float]],
) -> bool:
    intent = intent or {}
    if bool(intent.get("include_in_universe")):
        return True
    if coerce_float_fn(intent.get("universe_score")) is not None:
        return True
    if bool(intent.get("should_trade")):
        return True
    if coerce_float_fn(intent.get("target_weight")) is not None:
        return True
    return False


def alert_effective_signal(
    alert: Dict[str, Any],
    *,
    coerce_float_fn: Callable[[Any], Optional[float]],
) -> Tuple[float, float]:
    intent = (alert or {}).get("_model_intent")
    z_value = coerce_float_fn((intent or {}).get("expected_z"))
    conf_value = coerce_float_fn((intent or {}).get("confidence"))
    if z_value is None:
        z_value = coerce_float_fn((alert or {}).get("expected_z"))
    if conf_value is None:
        conf_value = coerce_float_fn((alert or {}).get("confidence"))
    return float(z_value or 0.0), float(conf_value or 0.0)


def model_intent_trade_allowed(
    alert: Dict[str, Any],
    *,
    coerce_bool_fn: Callable[[Any], Optional[bool]],
) -> bool:
    intent = (alert or {}).get("_model_intent") or {}
    should_trade = coerce_bool_fn(intent.get("should_trade"))
    if should_trade is False:
        return False
    timing = str(intent.get("timing") or "").strip().lower()
    if timing in ("hold", "skip", "wait", "defer", "flat"):
        return False
    side = str(intent.get("side") or "").strip().upper()
    if side == "FLAT":
        return False
    return True


def score_from_alert(
    z: float,
    conf: float,
    severity: str,
    explain_json: str,
    *,
    novelty_alpha: float,
    novelty_from_explain_fn: Callable[[str], float],
) -> float:
    score = abs(float(z)) * float(conf)
    severity_s = (severity or "").upper()
    if severity_s == "CRIT":
        score *= 1.15
    elif severity_s == "HIGH":
        score *= 1.08

    novelty = novelty_from_explain_fn(explain_json)
    score *= 1.0 + float(novelty_alpha) * float(novelty)
    return float(score)


def tradability_from_explain(
    explain_json: str, *, warn_nonfatal: WarnFn | None = None
) -> Dict[str, float]:
    try:
        parsed = json.loads(explain_json or "{}")
        tradability = parsed.get("tradability") or {}
        return {
            "expected_ret_net": float(tradability.get("expected_ret_net", 0.0)),
            "p_win": float(tradability.get("p_win", 0.5)),
            "expected_dd": float(tradability.get("expected_dd", 0.0)),
        }
    except Exception as exc:
        if warn_nonfatal is not None:
            warn_nonfatal(
                "PORTFOLIO_TRADABILITY_PARSE_FAILED",
                exc,
                once_key="tradability_from_explain",
            )
        return {
            "expected_ret_net": 0.0,
            "p_win": 0.5,
            "expected_dd": 0.0,
        }


def strategy_candidate_limit(
    alerts: List[Dict],
    default_limit: int,
    *,
    model_intent_max_positions: int,
    has_canonical_model_trade_intent_fn: Callable[[Optional[Dict[str, Any]]], bool],
    model_intent_trade_allowed_fn: Callable[[Dict[str, Any]], bool],
) -> int:
    default_n = max(1, int(default_limit or 1))
    canonical_count = 0
    for alert in alerts or []:
        intent = (alert or {}).get("_model_intent")
        if not has_canonical_model_trade_intent_fn(intent):
            continue
        if not model_intent_trade_allowed_fn(alert):
            continue
        canonical_count += 1

    if canonical_count <= 0:
        return int(default_n)

    explicit_cap = int(model_intent_max_positions)
    if explicit_cap > 0:
        return max(1, min(int(canonical_count), explicit_cap))
    return max(int(default_n), int(canonical_count))


def merge_model_intent_reason(
    reason: Dict[str, Any],
    alert: Dict[str, Any],
    *,
    safe_float_fn: Callable[[Any], float],
) -> Dict[str, Any]:
    out = dict(reason or {})
    intent = (alert or {}).get("_model_intent") or {}
    if not intent:
        return out
    out["model_intent"] = dict(intent)
    if intent.get("selected_features"):
        out["selected_features"] = list(intent.get("selected_features") or [])
    if intent.get("timing"):
        out["trade_timing"] = str(intent.get("timing"))
    if intent.get("target_weight") is not None:
        out["model_target_weight"] = safe_float_fn(intent.get("target_weight"))
    if intent.get("size_mult") is not None:
        out["model_size_mult"] = safe_float_fn(intent.get("size_mult"))
    if intent.get("score") is not None:
        out["model_score"] = safe_float_fn(intent.get("score"))
    if intent.get("prediction_strength") is not None:
        out["prediction_strength"] = safe_float_fn(intent.get("prediction_strength"))
    if intent.get("include_in_universe") is not None:
        out["model_universe_include"] = bool(intent.get("include_in_universe"))
    if intent.get("universe_score") is not None:
        out["model_universe_score"] = safe_float_fn(intent.get("universe_score"))
    return out


def pick_best_per_symbol(
    alerts: List[Dict],
    *,
    model_intent_trade_allowed_fn: Callable[[Dict[str, Any]], bool],
    alert_effective_signal_fn: Callable[[Dict[str, Any]], Tuple[float, float]],
    eff_min_conf_fn: Callable[[], float],
    eff_min_abs_z_fn: Callable[[], float],
    has_explicit_model_trade_intent_fn: Callable[[Optional[Dict[str, Any]]], bool],
    coerce_float_fn: Callable[[Any], Optional[float]],
    score_from_alert_fn: Callable[[float, float, str, str], float],
    tradability_from_explain_fn: Callable[[str], Dict[str, float]],
) -> Dict[str, Dict]:
    best: Dict[str, Dict] = {}
    for alert in alerts:
        if not model_intent_trade_allowed_fn(alert):
            continue
        symbol = alert["symbol"]
        z_value, conf_value = alert_effective_signal_fn(alert)
        model_intent = alert.get("_model_intent") or {}
        if conf_value < eff_min_conf_fn():
            continue
        explicit_trade_intent = has_explicit_model_trade_intent_fn(model_intent)
        if (not explicit_trade_intent) and abs(z_value) < eff_min_abs_z_fn():
            continue

        model_score = coerce_float_fn(model_intent.get("score"))
        if model_score is not None:
            base_score = float(model_score)
        else:
            base_score = score_from_alert_fn(
                z_value,
                conf_value,
                str(alert.get("severity") or ""),
                str(alert.get("explain_json") or "{}"),
            )

        tradability = tradability_from_explain_fn(alert.get("explain_json", "{}"))
        net = float(tradability.get("expected_ret_net", 0.0))
        pwin = float(tradability.get("p_win", 0.5))
        dd = float(tradability.get("expected_dd", 0.0))

        tradability_mult = 1.0
        if net < 0.0:
            tradability_mult *= 0.5
        else:
            tradability_mult *= 1.0 + min(0.5, net * 10.0)

        tradability_mult *= 0.75 + 0.5 * max(0.0, min(1.0, pwin))
        tradability_mult *= 1.0 / (1.0 + dd * 10.0)

        score = base_score * tradability_mult
        current = best.get(symbol)
        if current is None or score > float(current.get("_score", 0.0)):
            chosen = dict(alert)
            chosen["expected_z"] = float(z_value)
            chosen["confidence"] = float(conf_value)
            chosen["_score"] = float(score)
            best[symbol] = chosen
    return best
