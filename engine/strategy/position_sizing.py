"""
FILE: position_sizing.py

Turns model output into a position size.

The main decision path starts from `(expected_z, confidence)` and then applies
optional modifiers for alpha decay, learned execution-aware size policy,
capital-preservation mode, and regime compatibility.
"""

import logging
import os
import time
from typing import Optional, Tuple, Dict, Any

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect
from engine.runtime.risk_state import get_state
from engine.strategy.regime_compat import regime_compat_multiplier

MAX_POS = float(os.environ.get("MAX_POSITION_FRACTION", "0.20"))  # 20% notional
Z_REF = float(os.environ.get("POSITION_Z_REF", "2.0"))            # z=2 => full scale (before conf)

MIN_CONF = float(os.environ.get("POSITION_MIN_CONF", "0.55"))
MIN_ABS_Z = float(os.environ.get("POSITION_MIN_ABS_Z", "0.75"))

USE_SIZE_POLICY = os.environ.get("USE_SIZE_POLICY", "0") == "1"
SIZE_POLICY_CACHE_TTL_S = float(os.environ.get("SIZE_POLICY_CACHE_TTL_S", "30"))

# Capital Preservation Mode (CPM) sizing compression
CAPITAL_PRESERVE_SIZE_MULT = float(os.environ.get("CAPITAL_PRESERVE_SIZE_MULT", "0.40"))

# Alpha decay
USE_ALPHA_DECAY = os.environ.get("USE_ALPHA_DECAY", "1") == "1"

_size_policy_cache = {
    "ts": 0.0,
    "buckets": 0,
    "points": None,  # list of dicts with {conf_lo, conf_hi, factor, ...}
}
LOG = get_logger("engine.strategy.position_sizing")


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="position_sizing_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.position_sizing",
        extra=extra or None,
        persist=False,
    )


def _get_size_policy_factor(confidence: float) -> Optional[Tuple[float, dict]]:
    """
    Returns (factor, point_obj) for given confidence using latest size_policy_points.
    Returns None if table/policy is missing.

    NOTE: cached for SIZE_POLICY_CACHE_TTL_S seconds to avoid a DB read on
    every sizing decision.
    """
    c = max(0.0, min(1.0, float(confidence)))
    now = time.time()

    # In-process cache is enough here because policies change relatively slowly.
    # If the table is missing or empty we cache that miss briefly too.
    if (
        _size_policy_cache.get("points") is not None
        and (now - float(_size_policy_cache.get("ts") or 0.0)) < float(SIZE_POLICY_CACHE_TTL_S)
    ):
        pts = _size_policy_cache["points"] or []
        for p in pts:
            try:
                if c >= float(p.get("conf_lo")) and c < float(p.get("conf_hi")):
                    return float(p.get("factor") or 1.0), dict(p)
            except Exception as e:
                _warn_nonfatal("POSITION_SIZING_CACHE_BUCKET_PARSE_FAILED", e, confidence=float(c))
                continue
        return None

    con = connect()
    try:
        from engine.strategy.size_policy import load_latest_size_policy

        policy = load_latest_size_policy(con)
        if not policy:
            _size_policy_cache.update({"ts": now, "buckets": 0, "points": []})
            return None
        points = list(policy.get("points") or [])
        buckets = int(policy.get("buckets") or len(points))
        _size_policy_cache.update({"ts": now, "buckets": buckets, "points": points})

        for p in points:
            try:
                if c >= float(p.get("conf_lo")) and c < float(p.get("conf_hi")):
                    return float(p.get("factor") or 1.0), dict(p)
            except Exception as e:
                _warn_nonfatal(
                    "POSITION_SIZING_DB_BUCKET_PARSE_FAILED",
                    e,
                    confidence=float(c),
                    policy_id=int(policy.get("policy_id") or 0),
                )
                continue

        return None
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("POSITION_SIZING_CLOSE_FAILED", e, operation="_get_size_policy_factor")


def position_from_signal(
    expected_z: float,
    confidence: float,
    alpha_remaining: Optional[float] = None,
    allocation_multiplier: float = 1.0,
) -> Dict[str, Any]:
    """
    Returns sizing decision dict.

    Applies the full sizing stack:
      - baseline confidence and z-score scaling
      - optional alpha decay
      - optional learned size policy
      - capital-preserve compression
      - regime compatibility multiplier / suppression
      - allocation multiplier
    """
    z = float(expected_z)
    c = float(confidence)

    az = abs(z)
    if c < float(MIN_CONF) or az < float(MIN_ABS_Z):
        return {
            "direction": "FLAT",
            "size": 0.0,
            "notional_frac": 0.0,
            "reason": f"below_threshold conf<{MIN_CONF} or |z|<{MIN_ABS_Z}",
        }

    direction = "LONG" if z > 0 else "SHORT"

    # Baseline sizing is intentionally simple so downstream multipliers remain
    # interpretable when reviewing a trade decision.
    raw = (az / max(1e-9, float(Z_REF))) * c
    size = max(0.0, min(float(MAX_POS), float(raw) * float(MAX_POS)))

    # Alpha decay multiplier (if provided)
    if USE_ALPHA_DECAY and alpha_remaining is not None:
        try:
            decay_mult = max(0.0, min(1.0, float(alpha_remaining)))
            size = max(0.0, min(float(MAX_POS), float(size) * float(decay_mult)))
        except Exception as e:
            _warn_nonfatal("POSITION_SIZING_ALPHA_DECAY_FAILED", e, alpha_remaining=alpha_remaining)

    # Learned size policy reflects realized net returns, so it is the main knob
    # for shrinking exposure when execution quality degrades.
    size_policy_blob = None
    if USE_SIZE_POLICY:
        got = _get_size_policy_factor(c)
        if got is not None:
            factor, point = got
            factor = max(0.0, float(factor))
            size = max(0.0, min(float(MAX_POS), float(size) * float(factor)))
            size_policy_blob = {
                "factor": float(factor),
                "bucket": {
                    "conf_lo": point.get("conf_lo"),
                    "conf_hi": point.get("conf_hi"),
                    "n": point.get("n"),
                    "mean_net_ret": point.get("mean_net_ret"),
                    "std_net_ret": point.get("std_net_ret"),
                },
            }

    # Capital Preservation Mode: compress position size
    cap_mode = str(get_state("capital_mode", "normal") or "normal")
    cap_mult = 1.0
    if cap_mode == "preserve":
        try:
            cap_mult = max(0.0, min(1.0, float(CAPITAL_PRESERVE_SIZE_MULT)))
        except Exception:
            cap_mult = 1.0
        size = max(0.0, min(float(MAX_POS), float(size) * float(cap_mult)))

    # Regime compatibility (model × regime) sizing + suppression (fail-open)
    # Use model_registry if available; fallback to MODEL_NAME env.
    try:
        from engine.model_registry import get_active_model_name  # type: ignore
        model_name = str(get_active_model_name() or "").strip() or ""
    except Exception:
        model_name = ""

    if not model_name:
        model_name = os.environ.get("MODEL_NAME", "embed_regressor").strip() or "embed_regressor"

    compat = {}
    try:
        # Your earlier code used anchor="SPY" in one path; keep that behavior.
        compat = regime_compat_multiplier(model_name=str(model_name), anchor="SPY") or {}
    except Exception:
        compat = {}

    if bool(compat.get("suppressed")):
        return {
            "direction": "FLAT",
            "size": 0.0,
            "notional_frac": 0.0,
            "reason": "regime_compat_suppressed",
            "regime_compat": compat,
            "alpha_remaining": (float(alpha_remaining) if alpha_remaining is not None else None),
        }

    try:
        mult = float(compat.get("mult") or 1.0)
    except Exception:
        mult = 1.0
    if mult < 0.0:
        mult = 0.0

    size = max(0.0, min(float(MAX_POS), float(size) * float(mult)))

    try:
        alloc_mult = max(0.0, float(allocation_multiplier))
    except Exception:
        alloc_mult = 1.0
    size = max(0.0, min(float(MAX_POS), float(size) * float(alloc_mult)))

    # output
    out: Dict[str, Any] = {
        "direction": direction,
        "size": float(size),
        "notional_frac": float(size),
        "reason": "scaled_by_confidence_z_regime_compat_and_decay",
        "regime_compat": compat,
        "alpha_remaining": (float(alpha_remaining) if alpha_remaining is not None else None),
        "allocation_multiplier": float(alloc_mult),
    }

    if size_policy_blob is not None:
        out["size_policy"] = size_policy_blob

    if cap_mode == "preserve":
        out["capital_mode"] = "preserve"
        out["capital_preserve_mult"] = float(cap_mult)

    return out
