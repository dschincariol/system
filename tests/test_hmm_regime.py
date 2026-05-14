from __future__ import annotations

import importlib
import os
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class HmmRegimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "hmm_regime.db")
        os.environ["HMM_REGIME_ENABLED"] = "1"
        os.environ["HMM_NUM_STATES"] = "3"
        self.storage, self.hmm_regime, self.feature_registry = _reload_modules(
            "engine.runtime.storage",
            "engine.strategy.hmm_regime",
            "engine.strategy.feature_registry",
        )
        self.storage.init_db()

    def tearDown(self) -> None:
        for key in (
            "DB_PATH",
            "HMM_REGIME_ENABLED",
            "HMM_NUM_STATES",
            "HMM_REGIME_ENSEMBLE_WEIGHT_ENABLED",
        ):
            os.environ.pop(key, None)
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception:
            pass
        self.tmp.cleanup()

    def _feature_rows(self) -> list[dict[str, float]]:
        rng = np.random.default_rng(7)
        rows: list[dict[str, float]] = []
        regimes = [
            (0.10, 0.12, 0.08, 0.05, 0.10, 0.08),
            (0.45, 0.42, 0.38, 0.40, 0.35, 0.32),
            (0.88, 0.84, 0.90, 0.82, 0.78, 0.75),
        ]
        names = list(self.hmm_regime.DEFAULT_HMM_FEATURE_NAMES)
        for center in regimes:
            base = np.asarray(center, dtype=np.float64)
            for _ in range(48):
                sample = np.clip(base + rng.normal(0.0, 0.03, size=base.shape[0]), 0.0, 1.0)
                rows.append({name: float(sample[idx]) for idx, name in enumerate(names)})
        return rows

    def test_state_inference_stability(self) -> None:
        if getattr(self.hmm_regime, "_GaussianHMM", None) is None:
            self.skipTest("hmmlearn unavailable")
        rows = self._feature_rows()
        model = self.hmm_regime.train_hmm(rows)
        self.assertTrue(bool(model.get("available")))

        first = self.hmm_regime.infer_regime(model, rows[-1])
        second = self.hmm_regime.infer_regime(model, rows[-1])

        self.assertEqual(str(first.get("regime_label") or ""), str(second.get("regime_label") or ""))
        self.assertAlmostEqual(float(first.get("confidence") or 0.0), float(second.get("confidence") or 0.0), places=8)
        self.assertAlmostEqual(
            sum(float(value) for value in dict(first.get("state_probabilities") or {}).values()),
            1.0,
            places=6,
        )
        self.assertEqual(dict(first.get("state_probabilities") or {}), dict(second.get("state_probabilities") or {}))

    def test_persistence_and_load_round_trip(self) -> None:
        if getattr(self.hmm_regime, "_GaussianHMM", None) is None:
            self.skipTest("hmmlearn unavailable")
        rows = self._feature_rows()
        model = self.hmm_regime.train_hmm(rows)
        persist = self.hmm_regime.persist_hmm_model(model, symbol="SPY")
        self.assertTrue(bool(persist.get("ok")))

        loaded = self.hmm_regime.load_latest_hmm_model("AAPL")
        self.assertIsInstance(loaded, dict)
        self.assertEqual(str((loaded or {}).get("symbol") or ""), "SPY")

        expected = self.hmm_regime.infer_regime(model, rows[0])
        actual = self.hmm_regime.infer_regime(loaded, rows[0])
        self.assertEqual(str(expected.get("regime_label") or ""), str(actual.get("regime_label") or ""))
        self.assertEqual(
            dict(expected.get("state_probabilities") or {}),
            dict(actual.get("state_probabilities") or {}),
        )

    def test_disabled_feature_path_returns_zeroed_hmm_features(self) -> None:
        os.environ["HMM_REGIME_ENABLED"] = "0"
        _, _, feature_registry = _reload_modules(
            "engine.runtime.storage",
            "engine.strategy.hmm_regime",
            "engine.strategy.feature_registry",
        )
        snapshot = feature_registry.build_feature_snapshot(
            event={"ts_ms": 1_700_000_000_000},
            symbol="AAPL",
            feature_ids=[
                "hmm_regime.enabled",
                "hmm_regime.model_available",
                "hmm_regime.state_0_prob",
                "hmm_regime.label_risk_off_prob",
            ],
        )

        self.assertEqual(float(snapshot.get("hmm_regime.enabled") or 0.0), 0.0)
        self.assertEqual(float(snapshot.get("hmm_regime.model_available") or 0.0), 0.0)
        self.assertEqual(float(snapshot.get("hmm_regime.state_0_prob") or 0.0), 0.0)
        self.assertEqual(float(snapshot.get("hmm_regime.label_risk_off_prob") or 0.0), 0.0)


if __name__ == "__main__":
    unittest.main()
