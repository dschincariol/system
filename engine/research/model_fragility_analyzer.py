"""
Offline fragility checks for governance and execution advisory data.
"""

from __future__ import annotations

from typing import Any, Dict

from engine.execution.execution_ai_advisor import list_execution_advisories
from engine.strategy.model_governance_ext import build_governance_summary


def analyze_model_fragility() -> Dict[str, Any]:
    governance = build_governance_summary(limit_audit=10)
    advisories = list_execution_advisories(limit=25)

    high_urgency = [
        item for item in (advisories.get("items") or [])
        if str((item or {}).get("urgency") or "").lower() == "high"
    ]
    governance_alerts = list(governance.get("governance_alerts") or [])

    fragility_score = 0.0
    fragility_score += min(5.0, float(len(high_urgency)) * 0.5)
    fragility_score += min(5.0, float(len(governance_alerts)) * 1.0)

    return {
        "ok": True,
        "fragility_score": float(round(fragility_score, 3)),
        "high_urgency_advisories": int(len(high_urgency)),
        "governance_alerts": governance_alerts,
        "top_champion": ((governance.get("champions") or [{}])[0] if (governance.get("champions") or []) else None),
    }
