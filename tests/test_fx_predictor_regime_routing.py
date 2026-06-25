from __future__ import annotations

import importlib
import os
import sys
import unittest
import uuid
from pathlib import Path
from unittest import mock

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class FxPredictorRegimeRoutingTests(unittest.TestCase):
    def setUp(self) -> None:
        os.environ.pop("USE_FX_REGIME", None)
        self.predictor = importlib.reload(importlib.import_module("engine.strategy.predictor"))

    def test_regime_anchor_symbol_is_asset_class_aware(self) -> None:
        self.assertEqual(self.predictor._regime_anchor_symbol("EURUSD"), "EURUSD")
        self.assertEqual(self.predictor._regime_anchor_symbol("eur/usd"), "EURUSD")
        self.assertEqual(self.predictor._regime_anchor_symbol("SPY"), "SPY")
        self.assertEqual(self.predictor._regime_anchor_symbol("AAPL"), "SPY")

    def test_prediction_regime_context_uses_fx_symbol_not_spy(self) -> None:
        seen: list[str] = []

        def fake_current_regime(symbol: str) -> str | None:
            seen.append(symbol)
            return None if symbol == "EURUSD" else "HIGH"

        with mock.patch.object(self.predictor, "get_current_regime", side_effect=fake_current_regime):
            regime, context = self.predictor._prediction_regime_context("EURUSD", {"ts_ms": 2_000})
            self.assertEqual(seen[0], "EURUSD")
            self.assertEqual(regime, "FX_MID")
            self.assertEqual(context["anchor_symbol"], "EURUSD")

            seen.clear()
            regime, context = self.predictor._prediction_regime_context("SPY", {"ts_ms": 2_000})
            self.assertEqual(seen[0], "SPY")
            self.assertEqual(regime, "HIGH")
            self.assertEqual(context, {})

    def test_resolved_model_explain_gets_fx_regime_context_without_feature_or_family_changes(self) -> None:
        canary = "CANARY-" + uuid.uuid4().hex
        os.environ["FX04_SECRET_SHAPED_VALUE"] = canary
        self.addCleanup(os.environ.pop, "FX04_SECRET_SHAPED_VALUE", None)
        active_model = {
            "model_name": "fx-test-model",
            "model_id": "fx-test-model-id",
            "family": "embed_regressor",
            "model_family": "embed_regressor",
            "feature_ids": ["base.source_credibility"],
        }
        calls: dict[str, object] = {}

        def fake_adapter_predict(family, query_vec, sym, h, **kwargs):
            calls["active_family"] = family
            calls["symbol"] = sym
            calls["feature_ids"] = list(kwargs.get("feature_ids") or [])
            return 0.25, 0.75, {
                "model_family": family,
                "feature_ids": list(kwargs.get("feature_ids") or []),
            }

        with (
            mock.patch.object(self.predictor, "_knn_raw", return_value=(0.0, 1.0, {})),
            mock.patch.object(self.predictor, "_adapter_predict", side_effect=fake_adapter_predict),
            mock.patch.object(self.predictor, "get_current_regime", return_value=None) as regime_lookup,
            mock.patch(
                "engine.strategy.regime_stack.compute_regime_vector",
                return_value={
                    "macro": {
                        "fx_usd_strength_z": 0.0,
                        "fx_usd_strength_dir": 0.0,
                        "fx_carry_pressure": 0.0,
                    },
                    "asset": {},
                    "micro": {},
                    "drift": {},
                },
            ),
        ):
            pred, conf, explain = self.predictor._predict_resolved_model(
                np.zeros(3),
                "EURUSD",
                300,
                top_k=1,
                active_model=active_model,
                event={"ts_ms": 2_000},
            )

        self.assertEqual(pred, 0.25)
        self.assertEqual(conf, 0.75)
        self.assertEqual(calls["active_family"], "embed_regressor")
        self.assertEqual(calls["symbol"], "EURUSD")
        self.assertEqual(calls["feature_ids"], ["base.source_credibility"])
        regime_lookup.assert_called_with("EURUSD")
        self.assertEqual(explain["regime_at_trade"], "FX_MID")
        self.assertEqual(explain["regime_anchor_symbol"], "EURUSD")
        self.assertEqual(explain["fx_regime_context"]["anchor_symbol"], "EURUSD")
        self.assertEqual(explain["feature_ids"], ["base.source_credibility"])
        self.assertNotIn(canary, repr(explain))


if __name__ == "__main__":
    unittest.main()
