"""
Job entrypoint for periodic model competition re-evaluation.
"""

import json

from engine.strategy.champion_manager import run_model_competition_job


def main() -> int:
    result = run_model_competition_job()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
