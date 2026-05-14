from __future__ import annotations

import importlib
import os
import pickle
import sys
import tempfile
import time
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


class InferenceRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "inference_runtime.db")
        (
            self.storage,
            self.feature_store,
            self.registry,
            self.inference_runtime,
        ) = _reload_modules(
            "engine.runtime.storage",
            "engine.data.feature_store",
            "engine.model_registry",
            "engine.runtime.inference_runtime",
        )
        self.storage.init_db()

    def tearDown(self) -> None:
        try:
            self.storage.close_pooled_connections()
        except Exception:
            pass
        self.tmp.cleanup()

    def _store_feature_snapshot(self, symbol: str) -> dict[str, object]:
        now_ms = int(time.time() * 1000)
        feature_map = {
            str(name): float(idx + 1)
            for idx, name in enumerate(self.feature_store.FEATURE_NAMES)
        }
        snapshot = {
            "symbol": str(symbol).upper(),
            "ts_ms": int(now_ms),
            "feature_set_tag": str(self.feature_store.FEATURE_SET_TAG),
            "feature_names": list(self.feature_store.FEATURE_NAMES),
            "point_count": 32,
            "source_timestamps": {"price_history_last_ts_ms": int(now_ms)},
            "features": feature_map,
        }
        return self.feature_store.store_features(str(symbol), snapshot)

    def test_runtime_feature_reader_delegates_to_online_feature_store(self) -> None:
        stored = self._store_feature_snapshot("AAPL")

        contract = self.inference_runtime.get_online_feature_contract()
        snapshot = self.inference_runtime.read_online_feature_snapshot("AAPL")
        validation = self.inference_runtime.validate_online_feature_snapshot(snapshot)

        self.assertTrue(bool(contract.get("ok")))
        self.assertEqual(list(contract.get("feature_names") or []), list(self.feature_store.FEATURE_NAMES))
        self.assertEqual(str(contract.get("feature_set_tag") or ""), str(self.feature_store.FEATURE_SET_TAG))
        self.assertEqual(str(snapshot.get("symbol") or ""), "AAPL")
        self.assertEqual(int(snapshot.get("ts_ms") or 0), int(stored.get("ts_ms") or 0))
        self.assertTrue(bool(validation.get("ok")))

    def test_runtime_model_catalog_reads_delegate_to_registry_cache(self) -> None:
        artifact_path = Path(self.tmp.name) / "aapl_runtime_linear_v1.pkl"
        with artifact_path.open("wb") as handle:
            pickle.dump({"prediction": 0.25, "confidence": 0.8}, handle)
        self.registry.register_model(
            symbol="AAPL",
            model_name="runtime_linear",
            model_kind="constant",
            version="v1",
            artifact_uri=str(artifact_path),
            metadata={"model_id": "runtime_linear:AAPL:v1", "feature_ids": list(self.feature_store.FEATURE_NAMES)},
            performance_metrics={"quality_score": 0.8},
            is_active=True,
        )

        loaded = self.inference_runtime.load_online_model_record(
            "AAPL",
            model_name="runtime_linear",
            active_only=True,
        )
        listed = self.inference_runtime.list_online_model_records("AAPL", active_only=True, limit=10)
        best = self.inference_runtime.get_best_online_model_record("AAPL", model_name="runtime_linear")

        self.assertIsNotNone(loaded)
        self.assertEqual(str((loaded or {}).get("model_name") or ""), "runtime_linear")
        self.assertEqual(int(len(listed)), 1)
        self.assertEqual(str(listed[0].get("version") or ""), "v1")
        self.assertIsNotNone(best)
        self.assertEqual(str((best or {}).get("model_name") or ""), "runtime_linear")


if __name__ == "__main__":
    unittest.main()
