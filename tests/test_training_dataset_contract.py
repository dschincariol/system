from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class TrainingDatasetContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.tmp.name) / "training_dataset_contract.db"
        self.dataset_root = Path(self.tmp.name) / "training_datasets"
        self._env_backup = {
            key: os.environ.get(key)
            for key in (
                "DB_PATH",
                "TRAINING_DATASET_STORE_ROOT",
                "TRAINING_DATASET_URI_PREFIX",
                "HMM_TRAIN_LOOKBACK_ROWS",
                "HMM_TRAIN_MIN_ROWS",
            )
        }
        os.environ["DB_PATH"] = str(self.db_path)
        os.environ["TRAINING_DATASET_STORE_ROOT"] = str(self.dataset_root)
        os.environ["TRAINING_DATASET_URI_PREFIX"] = "s3://training-datasets"
        os.environ["HMM_TRAIN_LOOKBACK_ROWS"] = "5"
        os.environ["HMM_TRAIN_MIN_ROWS"] = "2"
        _, self.storage, self.learning_loop, self.lifecycle = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.strategy.learning_loop",
            "engine.strategy.model_lifecycle",
        )
        self.storage.init_db()

    def tearDown(self) -> None:
        for key, value in self._env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception:
            pass
        self.tmp.cleanup()

    def test_build_dataset_snapshot_materializes_parquet_bundle_with_schema_and_window(self) -> None:
        con = self.storage.connect()
        try:
            con.execute(
                """
                INSERT INTO labels(event_id, symbol, horizon_s, baseline_ret, realized_ret, impact_z, created_at_ms, vol_proxy, regime)
                VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (1, "AAPL", 300, None, 1.25, 1.25, 101, None, "global"),
            )
            con.execute(
                """
                INSERT INTO labels(event_id, symbol, horizon_s, baseline_ret, realized_ret, impact_z, created_at_ms, vol_proxy, regime)
                VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (2, "AAPL", 300, None, -0.75, -0.75, 202, None, "global"),
            )
            con.commit()
        finally:
            con.close()

        dataset = self.learning_loop.build_dataset_snapshot(
            model_name="gbm_regressor.unit",
            lookback_days=0,
            symbols=["AAPL"],
            horizons=[300],
            feature_ids=["base.source_credibility", "base.normalized_text_len"],
            feature_schema={
                "feature_ids": ["base.source_credibility", "base.normalized_text_len"],
                "feature_set_tag": "base.unit",
                "feature_count": 2,
            },
            training_window={
                "start_ts_ms": 1000,
                "end_ts_ms": 2000,
                "lookback_days": 30,
                "horizon_s": 300,
            },
            extra={"job_name": "unit_test_dataset_snapshot"},
        )

        self.assertEqual(str(dataset.get("dataset_format") or ""), "parquet")
        self.assertEqual(str(dataset.get("storage_backend") or ""), "object")
        self.assertTrue(str(dataset.get("dataset_uri") or "").startswith("s3://training-datasets/gbm_regressor_unit/"))
        self.assertTrue(Path(str(dataset.get("dataset_local_path") or "")).exists())
        self.assertTrue(Path(str(dataset.get("dataset_manifest_local_path") or "")).exists())

        manifest = json.loads(Path(str(dataset.get("dataset_manifest_local_path") or "")).read_text(encoding="utf-8"))
        self.assertEqual(str(manifest.get("dataset_format") or ""), "parquet")
        self.assertEqual(str(manifest.get("feature_schema", {}).get("feature_set_tag") or ""), "base.unit")
        self.assertEqual(int(manifest.get("training_window", {}).get("start_ts_ms") or 0), 1000)
        self.assertEqual(int(manifest.get("training_window", {}).get("end_ts_ms") or 0), 2000)

        frame = pd.read_parquet(Path(str(dataset.get("dataset_local_path") or "")))
        self.assertIn("source_name", list(frame.columns))
        self.assertIn("labels", set(str(value) for value in frame["source_name"].tolist()))
        labels_row = frame.loc[frame["source_name"] == "labels"].iloc[0].to_dict()
        self.assertEqual(int(labels_row.get("row_count") or 0), 2)
        self.assertEqual(int(labels_row.get("distinct_horizons") or 0), 1)

    def test_hmm_dataset_snapshot_materializes_parquet_bundle(self) -> None:
        con = self.storage.connect()
        try:
            for index in range(5):
                ts_ms = 1_710_000_000_000 + (index * 60_000)
                con.execute(
                    """
                    INSERT INTO prices(ts_ms, symbol, price, px, source)
                    VALUES (?,?,?,?,?)
                    """,
                    (int(ts_ms), "SPY", float(500.0 + index), float(500.0 + index), "unit_test"),
                )
            con.commit()
        finally:
            con.close()

        dataset = self.lifecycle._build_hmm_dataset_snapshot(symbol="SPY", lookback_rows=5)

        self.assertEqual(str(dataset.get("dataset_format") or ""), "parquet")
        self.assertEqual(str(dataset.get("feature_schema", {}).get("feature_set_tag") or ""), "hmm.regime.v1")
        self.assertEqual(int(dataset.get("training_window", {}).get("lookback_rows") or 0), 5)
        self.assertTrue(Path(str(dataset.get("dataset_local_path") or "")).exists())

        manifest = json.loads(Path(str(dataset.get("dataset_manifest_local_path") or "")).read_text(encoding="utf-8"))
        self.assertEqual(int(manifest.get("training_window", {}).get("lookback_rows") or 0), 5)
        self.assertEqual(str(manifest.get("storage_backend") or ""), "object")

        frame = pd.read_parquet(Path(str(dataset.get("dataset_local_path") or "")))
        self.assertEqual(set(str(value) for value in frame["source_name"].tolist()), {"prices", "regime_vectors"})


if __name__ == "__main__":
    unittest.main()
