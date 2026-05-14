import sys
from pathlib import Path

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from engine.backtest.cpcv import CombinatorialPurgedKFold, purged_train_indices


def _has_overlap(i: int, j: int, starts: np.ndarray, ends: np.ndarray) -> bool:
    return bool(starts[i] <= ends[j] and ends[i] >= starts[j])


def test_purged_train_indices_removes_explicit_label_window_overlaps():
    starts = np.arange(10, dtype=float)
    ends = starts + 3.0
    train = [0, 1, 2, 3, 4, 7, 8, 9]
    test = [5, 6]

    purged = purged_train_indices(
        train,
        test,
        label_start_times=starts,
        label_end_times=ends,
        n_samples=10,
    )

    for train_i in purged:
        for test_i in test:
            assert not _has_overlap(int(train_i), int(test_i), starts, ends)


def test_splitter_purges_overlapping_labels_for_every_cpcv_split():
    n = 24
    starts = np.arange(n, dtype=float)
    ends = starts + 2.0
    splitter = CombinatorialPurgedKFold(
        n_splits=6,
        n_test_splits=2,
        label_start_times=starts,
        label_end_times=ends,
    )

    for train_idx, test_idx in splitter.split(np.arange(n)):
        for train_i in train_idx:
            for test_i in test_idx:
                assert not _has_overlap(int(train_i), int(test_i), starts, ends)
