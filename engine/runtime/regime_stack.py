"""
FILE: regime_stack.py

Runtime subsystem module for `regime_stack`.
"""

# engine/runtime/regime_stack.py
import time
from typing import Dict, Any


def _now_ms() -> int:
    return int(time.time() * 1000)


def regime_stack_snapshot() -> Dict[str, Any]:
    """
    Unified regime stack (macro / asset / microstructure).
    Wire this into your existing regime signals later.
    """
    return {
        "ok": True,
        "ts_ms": _now_ms(),
        "macro": {"regime": "UNKNOWN", "conf": 0.0},
        "asset": {"regime": "UNKNOWN", "conf": 0.0},
        "micro": {"regime": "UNKNOWN", "conf": 0.0},
        "reasons": [],
    }
