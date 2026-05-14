"""
FILE: conservative.py

Conservative portfolio strategy variant with stricter thresholds, fewer
positions, and a lower gross cap than the baseline strategy.
"""

import os
import logging
from typing import Dict, List

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.strategy import portfolio as P

NAME = "conservative"

MIN_CONF = float(os.environ.get("STRAT_CONSERVATIVE_MIN_CONF", "0.70"))
MIN_ABS_Z = float(os.environ.get("STRAT_CONSERVATIVE_MIN_ABS_Z", "1.60"))
MAX_POS = int(os.environ.get("STRAT_CONSERVATIVE_MAX_POSITIONS", "2"))
GROSS_CAP = float(os.environ.get("STRAT_CONSERVATIVE_GROSS_CAP", "0.70"))
SCORE_NORM = float(os.environ.get("STRAT_CONSERVATIVE_SCORE_NORM", str(P.PORTFOLIO_SCORE_NORM)))
LOG = get_logger("strategy.models.conservative")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="strategy_conservative_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.models.conservative",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)

def _clamp(x, lo, hi):
    return max(float(lo), min(float(hi), float(x)))

def build_desired(alerts: List[Dict], now_ms: int) -> Dict[str, Dict]:
    # This variant intentionally rejects mediocre alerts before scoring so the
    # rest of the portfolio logic only sees higher-conviction candidates.
    filtered = []
    for a in alerts or []:
        try:
            z, c = P._alert_effective_signal(a)
            if c < float(MIN_CONF):
                continue
            explicit_trade_intent = P._has_explicit_model_trade_intent(a.get("_model_intent"))
            if (not explicit_trade_intent) and abs(z) < float(MIN_ABS_Z):
                continue
            if not P._model_intent_trade_allowed(a):
                continue
            a = dict(a)
            a["expected_z"] = float(z)
            a["confidence"] = float(c)
            filtered.append(a)
        except Exception as e:
            _warn_nonfatal("CONSERVATIVE_ALERT_PARSE_FAILED", e, once_key="alert_parse")
            continue

    best = {}
    for a in filtered:
        sym = a["symbol"]
        z = float(a["expected_z"])
        conf = float(a["confidence"])
        score = P._coerce_float((a.get("_model_intent") or {}).get("score"))
        if score is None:
            score = P._score_from_alert(z, conf, a.get("severity"), a.get("explain_json", "{}"))
        cur = best.get(sym)
        if (cur is None) or (score > float(cur.get("_score", 0.0))):
            b = dict(a)
            b["_score"] = float(score)
            best[sym] = b

    candidates = sorted(best.values(), key=lambda a: float(a.get("_score", 0.0)), reverse=True)
    candidates = candidates[: P._strategy_candidate_limit(candidates, MAX_POS)]

    desired = {}
    for a in candidates:
        sym = a["symbol"]
        z = float(a["expected_z"])
        conf = float(a["confidence"])
        score = float(a["_score"])

        # weight formula like baseline but with local caps
        intent_weight = P._coerce_float((a.get("_model_intent") or {}).get("target_weight"))
        if intent_weight is not None:
            w = abs(float(intent_weight))
            size_mult = P._coerce_float((a.get("_model_intent") or {}).get("size_mult"))
            if size_mult is not None:
                w = float(w) * max(0.0, float(size_mult))
        else:
            w = (float(score) / float(SCORE_NORM)) * float(GROSS_CAP)
        w = _clamp(w, 0.0, P._symbol_cap(sym))
        intent_side = str(((a.get("_model_intent") or {}).get("side") or "")).upper()
        if intent_side in ("LONG", "SHORT", "FLAT"):
            side = intent_side
        else:
            side = "LONG" if z > 0 else "SHORT"

        desired[sym] = {
            "symbol": sym,
            "side": side,
            "weight": float(w),
            "source_alert_id": int(a["id"]),
            "reason": P._merge_model_intent_reason({
                "event_title": a.get("event_title", ""),
                "severity": a.get("severity", ""),
                "horizon_s": a.get("horizon_s", 0),
                "expected_z": float(z),
                "confidence": float(conf),
                "score": float(score),
                "min_conf": float(MIN_CONF),
                "min_abs_z": float(MIN_ABS_Z),
            }, a),
            "explain_json": a.get("explain_json") or "{}",
            "_strategy": NAME,
            "_now_ms": int(now_ms),
        }

    # gross normalize to MIN(GROSS_CAP, portfolio gross cap)
    gross_cap = min(float(GROSS_CAP), float(P.PORTFOLIO_GROSS_CAP))
    gross = sum(abs(float(v["weight"])) for v in desired.values())
    if gross > float(gross_cap) and gross > 1e-9:
        scale = float(gross_cap) / float(gross)
        for sym in list(desired.keys()):
            desired[sym]["weight"] = float(desired[sym]["weight"]) * float(scale)

    return desired
