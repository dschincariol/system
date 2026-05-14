"""
Offline smoke harness for the integrated research / governance / execution sidecars.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.research.adversarial_scenario_generator import generate_execution_scenarios
from engine.research.model_fragility_analyzer import analyze_model_fragility


def main() -> int:
    scenarios = generate_execution_scenarios()
    fragility = analyze_model_fragility()
    print(
        json.dumps(
            {
                "ok": True,
                "scenario_count": int(len(scenarios)),
                "scenarios": scenarios,
                "fragility": fragility,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
