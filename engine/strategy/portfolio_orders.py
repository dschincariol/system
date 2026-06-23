"""Order and rebalance-result helpers for the portfolio facade."""

from __future__ import annotations

import json
from typing import Any, Callable, Dict, List, Tuple


def selected_alert_ids_from_desired(
    desired: Dict[str, Dict[str, Any]],
    *,
    safe_float_fn: Callable[[Any, float], float],
    safe_int_fn: Callable[[Any, int], int],
) -> List[int]:
    out: set[int] = set()
    for target in (desired or {}).values():
        if not isinstance(target, dict):
            continue
        side = str(target.get("side") or "FLAT").upper().strip()
        weight = abs(safe_float_fn(target.get("weight", 0.0), 0.0))
        if side not in {"LONG", "SHORT"} or weight <= 0.0:
            continue
        alert_id = safe_int_fn(target.get("source_alert_id"), 0)
        if alert_id > 0:
            out.add(int(alert_id))
    return sorted(out)


def apply_flip_flop_penalty(
    con,
    desired: Dict[str, Dict],
    state: Dict[str, Dict],
    *,
    portfolio_flip_lambda_fn: Callable[[], float],
    normalize_model_id_fn: Callable[[Any], str],
    desired_symbol_fn: Callable[[Any, Dict[str, Any] | None], str],
    side_signed_weight_fn: Callable[[Any, Any], float],
    ensure_reason_dict_fn: Callable[[Dict[str, Any] | None], Dict[str, Any]],
    put_meta_fn: Callable[[Any, str, str], None],
    logger: Any,
) -> Tuple[Dict[str, Dict], Dict[str, Any]]:
    lambda_flip = portfolio_flip_lambda_fn()
    normalized_state: Dict[str, Dict] = {}
    for previous in (state or {}).values():
        if not isinstance(previous, dict):
            continue
        previous_key = (
            f"{normalize_model_id_fn(previous.get('model_id'))}:"
            f"{str(previous.get('symbol') or '').strip().upper()}"
        )
        normalized_state[previous_key] = previous

    flips: List[Dict[str, Any]] = []
    total_delta = 0.0
    for item_key, target in list((desired or {}).items()):
        if not isinstance(target, dict):
            continue
        symbol = desired_symbol_fn(item_key, target)
        if not symbol:
            continue
        model_id = normalize_model_id_fn(target.get("model_id"))
        previous = normalized_state.get(f"{model_id}:{str(symbol).strip().upper()}")
        if not previous:
            continue
        previous_signed = side_signed_weight_fn(
            previous.get("side"), previous.get("weight")
        )
        target_signed = side_signed_weight_fn(target.get("side"), target.get("weight"))
        if previous_signed * target_signed >= 0.0:
            continue
        delta_weight = abs(float(target_signed) - float(previous_signed))
        penalty = float(lambda_flip) * float(delta_weight)
        total_delta += float(delta_weight)
        detail = {
            "model_id": str(model_id),
            "symbol": str(symbol),
            "prev_side": str(previous.get("side") or ""),
            "target_side": str(target.get("side") or ""),
            "prev_weight": float(previous_signed),
            "target_weight": float(target_signed),
            "delta_weight": float(delta_weight),
            "lambda_flip": float(lambda_flip),
            "penalty": float(penalty),
        }
        flips.append(detail)
        reason = ensure_reason_dict_fn(target)
        reason["flip_flop_penalty"] = dict(detail)

    meta = {
        "enabled": True,
        "lambda_flip": float(lambda_flip),
        "flip_count": int(len(flips)),
        "turnover": float(total_delta),
        "penalty": float(lambda_flip) * float(total_delta),
        "flips": flips,
    }
    put_meta_fn(
        con,
        "last_flip_flop_penalty",
        json.dumps(meta, separators=(",", ":"), sort_keys=True),
    )
    if flips:
        logger.warning(
            "portfolio_flip_flop_penalty flip_count=%s penalty=%s lambda_flip=%s",
            len(flips),
            meta["penalty"],
            lambda_flip,
        )
    return desired, meta


def build_rebalance_result(
    ctx: Any,
    *,
    execution_blocked: bool,
    execution_blocked_codes: List[str],
) -> Dict[str, Any]:
    desired = ctx.desired
    portfolio_diag = ctx.portfolio_diag
    return {
        "ok": True,
        "strategy": "multi_strategy",
        "changed": [] if execution_blocked else list(ctx.changed),
        "orders_n": 0 if execution_blocked else int(ctx.orders_n),
        "selected": [
            str((value or {}).get("symbol") or key) for key, value in desired.items()
        ],
        "execution_blocked": bool(execution_blocked),
        "execution_blocked_codes": list(execution_blocked_codes),
        "portfolio_diagnostics": {
            "degraded": bool(ctx.degraded_reasons),
            "degraded_reasons": list(ctx.degraded_reasons),
            "execution_blocked": bool(execution_blocked),
            "execution_blocked_codes": list(execution_blocked_codes),
            "position_summary": dict(
                (portfolio_diag or {}).get("position_summary") or {}
            ),
            "model_summary": dict((portfolio_diag or {}).get("model_summary") or {}),
            "flip_flop_penalty": dict(ctx.flip_penalty or {}),
        },
    }
