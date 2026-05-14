from __future__ import annotations

import numpy as np
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from engine.strategy.statistics.multiple_testing import benjamini_hochberg, bonferroni, holm


def test_bh_q_values_known_vector() -> None:
    result = benjamini_hochberg([0.001, 0.04, 0.03, 0.20], q=0.10)

    np.testing.assert_allclose(
        result.q_values,
        np.asarray([0.004, 0.05333333333333334, 0.05333333333333334, 0.20]),
        rtol=0.0,
        atol=1e-12,
    )
    assert result.rejected.tolist() == [True, True, True, False]
    assert result.n_rejected == 3


def test_bh_edge_cases() -> None:
    assert benjamini_hochberg([1.0, 1.0], q=0.10).q_values.tolist() == [1.0, 1.0]
    assert benjamini_hochberg([1.0, 1.0], q=0.10).rejected.tolist() == [False, False]
    assert benjamini_hochberg([0.0, 0.0], q=0.10).q_values.tolist() == [0.0, 0.0]
    assert benjamini_hochberg([0.0, 0.0], q=0.10).rejected.tolist() == [True, True]

    single = benjamini_hochberg([0.03], q=0.10)
    assert single.q_values.tolist() == [0.03]
    assert single.rejected.tolist() == [True]


def test_bonferroni_and_holm() -> None:
    p_values = [0.01, 0.03, 0.20]

    bon = bonferroni(p_values, alpha=0.05)
    np.testing.assert_allclose(bon.q_values, np.asarray([0.03, 0.09, 0.60]), atol=1e-12, rtol=0.0)
    assert bon.rejected.tolist() == [True, False, False]

    step = holm(p_values, alpha=0.05)
    np.testing.assert_allclose(step.q_values, np.asarray([0.03, 0.06, 0.20]), atol=1e-12, rtol=0.0)
    assert step.rejected.tolist() == [True, False, False]
