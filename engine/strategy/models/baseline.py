"""
FILE: baseline.py

Default portfolio strategy implementation. It picks the best alert per symbol,
scores candidates, and converts them into desired weights using the shared
portfolio helpers.
"""

from typing import Dict, List

from engine.strategy import portfolio as P

NAME = "baseline"

def build_desired(alerts: List[Dict], now_ms: int) -> Dict[str, Dict]:
    # Reuse the central portfolio helpers so strategy variants differ mainly in
    # selection/gating, not in duplicated risk math.
    best = P._pick_best_per_symbol(alerts)

    candidates = sorted(best.values(), key=lambda a: float(a.get("_score", 0.0)), reverse=True)
    candidates = candidates[: P._strategy_candidate_limit(candidates, P.PORTFOLIO_MAX_POSITIONS)]

    desired = {}
    for a in candidates:
        sym = a["symbol"]
        z = float(a["expected_z"])
        conf = float(a["confidence"])
        score = float(a["_score"])
        w = P._resolve_desired_weight(a, score, sym)
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
            }, a),
            "explain_json": a.get("explain_json") or "{}",
            "_strategy": NAME,
            "_now_ms": int(now_ms),
        }

    # gross normalize to portfolio gross cap
    gross = sum(abs(float(v["weight"])) for v in desired.values())
    if gross > float(P.PORTFOLIO_GROSS_CAP) and gross > 1e-9:
        scale = float(P.PORTFOLIO_GROSS_CAP) / float(gross)
        for sym in list(desired.keys()):
            desired[sym]["weight"] = float(desired[sym]["weight"]) * float(scale)

    return desired
