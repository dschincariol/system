from __future__ import annotations

import numpy as np
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from engine.strategy.statistics.reality_check import stationary_bootstrap_indices, white_reality_check


def test_stationary_bootstrap_is_deterministic() -> None:
    first = stationary_bootstrap_indices(25, random_state=123, average_block_length=5.0)
    second = stationary_bootstrap_indices(25, random_state=123, average_block_length=5.0)

    assert first.tolist() == second.tolist()
    assert len(first) == 25
    assert all(0 <= int(idx) < 25 for idx in first)


def test_reality_check_detects_superior_challenger() -> None:
    champion = [0.0] * 50
    challenger = [0.20] * 50

    result = white_reality_check(
        challenger,
        champion,
        bootstrap_samples=199,
        random_state=7,
        alpha=0.05,
    )

    assert result.passed
    assert result.p_value < 0.05
    assert result.bootstrap_distribution.size == 199


def test_reality_check_null_false_positive_rate_is_controlled() -> None:
    false_positives = 0
    for seed in range(100):
        rng = np.random.default_rng(seed)
        champion = rng.normal(0.0, 1.0, 80)
        challenger = rng.normal(0.0, 1.0, 80)
        result = white_reality_check(
            challenger,
            champion,
            bootstrap_samples=399,
            random_state=seed,
            alpha=0.05,
        )
        false_positives += int(result.passed)

    assert false_positives <= 6
