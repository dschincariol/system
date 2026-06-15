"""
Job entrypoint for periodic model competition re-evaluation.
"""

import json
import logging

from engine.strategy.champion_manager import run_model_competition_job

LOG = logging.getLogger(__name__)


def main() -> int:
    result = run_model_competition_job()
    LOG.info("model_competition_result result=%s", json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
