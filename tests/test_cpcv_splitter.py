import math
import sys
from pathlib import Path

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from engine.backtest.cpcv import CombinatorialPurgedKFold, embargo_indices


def test_cpcv_yields_all_test_group_combinations():
    splitter = CombinatorialPurgedKFold(n_splits=6, n_test_splits=2, embargo=0.0)
    splits = list(splitter.split(np.arange(60)))

    assert len(splits) == math.comb(6, 2)
    for train_idx, test_idx in splits:
        assert len(test_idx) == 20
        assert len(train_idx) == 40
        assert set(train_idx).isdisjoint(set(test_idx))


def test_embargo_indices_excludes_fraction_after_each_test_block():
    test_idx = np.arange(10, 20)

    excluded = embargo_indices(test_idx, n_samples=30, embargo=0.10)

    assert excluded.tolist() == [20, 21, 22]


def test_splitter_applies_embargo_to_train_indices():
    splitter = CombinatorialPurgedKFold(n_splits=3, n_test_splits=1, embargo=0.10)
    splits = list(splitter.split(np.arange(30)))
    train_idx, test_idx = splits[1]

    assert test_idx.tolist() == list(range(10, 20))
    assert not ({20, 21, 22} & set(train_idx.tolist()))
