"""
Offline adversarial scenario generation for research and stress testing.
"""

from __future__ import annotations

from typing import Any, Dict, List


def generate_execution_scenarios() -> List[Dict[str, Any]]:
    return [
        {
            "name": "latency_spike",
            "description": "Assume fill latency doubles while slippage rises moderately.",
            "assumptions": {"latency_mult": 2.0, "slippage_bps_add": 4.0},
        },
        {
            "name": "tail_slippage",
            "description": "Assume p95 slippage jumps sharply during stressed routing.",
            "assumptions": {"latency_mult": 1.25, "slippage_bps_add": 12.0},
        },
        {
            "name": "crowded_symbol",
            "description": "Assume the highest-weight symbols become crowded and capacity-constrained.",
            "assumptions": {"top_symbol_scale": 0.70, "slippage_bps_add": 8.0},
        },
    ]
