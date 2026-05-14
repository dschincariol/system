from __future__ import annotations

import importlib
import os
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class TemporalTrainingIntegrityRegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "temporal_training_integrity.db")
        (self.storage, self.temporal_trainer) = _reload_modules(
            "engine.runtime.storage",
            "engine.strategy.train_temporal_predictor",
        )

    def tearDown(self) -> None:
        os.environ.pop("DB_PATH", None)
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception:
            pass
        self.tmp.cleanup()

    def test_temporal_holdout_indices_purge_overlap_before_eval_block(self) -> None:
        train_idx, eval_idx = self.temporal_trainer._temporal_holdout_indices(
            10,
            eval_fraction=0.30,
            label_horizon_rows=3,
            embargo_pct=0.0,
        )

        self.assertEqual(list(train_idx.tolist()), [0, 1, 2, 3])
        self.assertEqual(list(eval_idx.tolist()), [7, 8, 9])

    def test_temporal_holdout_indices_preserve_tail_eval_without_overlap(self) -> None:
        train_idx, eval_idx = self.temporal_trainer._temporal_holdout_indices(
            8,
            eval_fraction=0.25,
            label_horizon_rows=1,
            embargo_pct=0.0,
        )

        self.assertEqual(list(eval_idx.tolist()), [6, 7])
        self.assertEqual(list(train_idx.tolist()), [0, 1, 2, 3, 4])
        self.assertFalse(set(train_idx.tolist()) & set(eval_idx.tolist()))


if __name__ == "__main__":
    unittest.main()
