from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

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


class PredictorEnsembleBlendingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.tmp.name) / "predictor_ensemble_blending.db"
        os.environ["DB_PATH"] = str(self.db_path)
        os.environ["ENSEMBLE_BLEND_ENABLED"] = "1"
        os.environ["ENSEMBLE_BLEND_MODE"] = "equal"
        os.environ["ENSEMBLE_MAX_WEIGHT"] = "0.75"

        self.storage, self.ensemble_blender, self.predictor = _reload_modules(
            "engine.runtime.storage",
            "engine.strategy.ensemble_blender",
            "engine.strategy.predictor",
        )
        self.storage.init_db()

    def tearDown(self) -> None:
        for key in (
            "DB_PATH",
            "ENSEMBLE_BLEND_ENABLED",
            "ENSEMBLE_BLEND_MODE",
            "ENSEMBLE_MAX_WEIGHT",
            "HMM_REGIME_ENSEMBLE_WEIGHT_ENABLED",
        ):
            os.environ.pop(key, None)
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception:
            pass
        self.tmp.cleanup()

    def _insert_ensemble_history(self, rows: list[dict]) -> None:
        con = self.storage.connect(readonly=False)
        try:
            for row in rows:
                con.execute(
                    """
                    INSERT INTO ensemble_predictions(
                      symbol, ts, blended_prediction, family_preds_json, weights_json, agreement
                    )
                    VALUES (?,?,?,?,?,?)
                    """,
                    (
                        str(row.get("symbol") or "AAPL"),
                        int(row.get("ts") or 0),
                        float(row.get("blended_prediction") or 0.0),
                        json.dumps(row.get("family_preds") or {}, separators=(",", ":"), sort_keys=True),
                        json.dumps(row.get("weights") or {}, separators=(",", ":"), sort_keys=True),
                        float(row.get("agreement") or 0.0),
                    ),
                )
            con.commit()
        finally:
            try:
                con.close()
            except Exception:
                pass

    def test_predict_event_prefetches_feature_snapshots_with_batch_cache(self) -> None:
        event = {"ts_ms": 123_000}
        latest_many_calls: list[tuple[list[str], str]] = []

        class FakeCon:
            def close(self) -> None:
                pass

        def fake_latest_many(symbols, feature_group):
            latest_many_calls.append((list(symbols), str(feature_group)))
            return {
                str(symbol).upper().strip(): {
                    "symbol": str(symbol).upper().strip(),
                    "ts_ms": 122_000,
                    "feature_set_tag": str(feature_group),
                    "features": {"cached": str(symbol).upper().strip()},
                }
                for symbol in symbols
            }

        def fake_features_from_cached_snapshot(_symbol, _group, snap, *, decision_ts_ms=None):
            if isinstance(snap, dict):
                return dict(snap.get("features") or {})
            return None

        def fake_predict_single_model(_query_vec, sym, h, *, top_k, event=None):
            feature_snapshot = self.predictor._cached_or_build_feature_snapshot(
                event=event,
                symbol=str(sym),
                feature_ids=["unit_feature"],
            )
            return (
                0.10,
                0.20,
                {
                    "model_name": "unit_model",
                    "feature_ids": ["unit_feature"],
                    "feature_snapshot": dict(feature_snapshot or {}),
                },
            )

        def fake_describe_signal_confidence(**kwargs):
            return {"confidence": float(kwargs.get("confidence") or 0.0)}

        with patch("engine.cache.wrappers.feature_snapshots.latest_many", side_effect=fake_latest_many), \
            patch.object(self.predictor, "_resolve_active_model", return_value={
                "model_name": "unit_model",
                "feature_ids": ["unit_feature"],
                "feature_set_tag": "fg",
            }), \
            patch.object(self.predictor, "_registry_feature_set_tag", return_value="fg"), \
            patch.object(self.predictor, "_features_from_cached_snapshot", side_effect=fake_features_from_cached_snapshot), \
            patch.object(self.predictor, "build_feature_snapshot", side_effect=AssertionError("batch cache not used")), \
            patch.object(self.predictor, "_predict_single_model", side_effect=fake_predict_single_model), \
            patch.object(self.predictor, "_maybe_apply_lgbm_ranker_batch", side_effect=lambda out, **_kwargs: out), \
            patch.object(self.predictor, "get_spillover_betas", return_value=[]), \
            patch.object(self.predictor, "neutralize_mode", return_value="off"), \
            patch.object(self.predictor, "connect", return_value=FakeCon()), \
            patch.object(self.predictor, "calibrate_confidence_score", side_effect=lambda **kwargs: (kwargs["confidence_raw"], {})), \
            patch.object(self.predictor, "describe_signal_confidence", side_effect=fake_describe_signal_confidence), \
            patch.object(self.predictor, "apply_confidence_payload", side_effect=lambda explain, _payload: explain), \
            patch.object(self.predictor, "apply_conformal_to_explain", side_effect=lambda **kwargs: (kwargs["confidence"], kwargs["explain"], None)), \
            patch.object(self.predictor, "_maybe_attach_prediction_explanation", side_effect=lambda **kwargs: kwargs["explain"]), \
            patch.object(self.predictor, "_apply_model_serving_diagnostics", side_effect=lambda explain, _active: explain), \
            patch.object(self.predictor, "_track_prediction_output", return_value=None), \
            patch.object(self.predictor, "resolve_feature_ids", return_value=["unit_feature"]):
            out = self.predictor.predict_event(
                np.array([0.0]),
                ["AAPL", "MSFT"],
                [60],
                event=event,
            )

        self.assertEqual(latest_many_calls, [(["AAPL", "MSFT"], "fg")])
        self.assertEqual(out[("AAPL", 60)][2]["feature_snapshot"], {"cached": "AAPL"})
        self.assertEqual(out[("MSFT", 60)][2]["feature_snapshot"], {"cached": "MSFT"})

    def test_equal_weighting(self) -> None:
        weights = self.ensemble_blender.compute_blend_weights(
            {
                "embed_regressor": {"prediction": 0.3},
                "temporal_predictor": {"prediction": 0.6},
            },
            "equal",
        )

        self.assertAlmostEqual(float(weights["embed_regressor"]), 0.5, places=6)
        self.assertAlmostEqual(float(weights["temporal_predictor"]), 0.5, places=6)

    def test_inverse_variance_weighting(self) -> None:
        rows = []
        for idx in range(20):
            rows.append(
                {
                    "symbol": "AAPL",
                    "ts": 1_700_000_000_000 + idx,
                    "blended_prediction": 0.0,
                    "family_preds": {
                        "embed_regressor": {"prediction": 0.10 + (0.001 * idx), "confidence": 0.6},
                        "temporal_predictor": {"prediction": (-1.0 if idx % 2 else 1.0), "confidence": 0.6},
                    },
                    "weights": {"mode": "equal"},
                    "agreement": 0.5,
                }
            )
        self._insert_ensemble_history(rows)

        weights = self.ensemble_blender.compute_blend_weights(
            {
                "embed_regressor": {"prediction": 0.2},
                "temporal_predictor": {"prediction": 0.4},
            },
            "inverse_variance",
        )

        self.assertGreater(float(weights["embed_regressor"]), float(weights["temporal_predictor"]))
        self.assertAlmostEqual(sum(float(value) for value in weights.values()), 1.0, places=6)

    def test_missing_family_degradation(self) -> None:
        prediction, diagnostics = self.ensemble_blender.blend_predictions(
            {
                "embed_regressor": {
                    "prediction": 0.42,
                    "confidence": 0.55,
                    "model_name": "embed_regressor.live",
                }
            },
            {
                "embed_regressor": 0.25,
                "temporal_predictor": 0.75,
            },
        )

        self.assertAlmostEqual(float(prediction), 0.42, places=6)
        self.assertAlmostEqual(float(diagnostics["effective_weights"]["embed_regressor"]), 1.0, places=6)
        self.assertIn("temporal_predictor", list(diagnostics.get("missing_families") or []))

    def test_max_weight_cap_enforcement(self) -> None:
        os.environ["ENSEMBLE_MAX_WEIGHT"] = "0.60"
        rows = []
        for idx in range(24):
            rows.append(
                {
                    "symbol": "AAPL",
                    "ts": 1_700_000_100_000 + idx,
                    "blended_prediction": 0.0,
                    "family_preds": {
                        "embed_regressor": {"prediction": 0.200001 + (idx * 1e-7), "confidence": 0.6},
                        "temporal_predictor": {"prediction": float(idx % 2), "confidence": 0.6},
                    },
                    "weights": {"mode": "inverse_variance"},
                    "agreement": 0.4,
                }
            )
        self._insert_ensemble_history(rows)

        weights = self.ensemble_blender.compute_blend_weights(
            {
                "embed_regressor": {"prediction": 0.2},
                "temporal_predictor": {"prediction": 0.4},
            },
            "inverse_variance",
        )

        self.assertLessEqual(float(weights["embed_regressor"]), 0.60 + 1e-9)
        self.assertAlmostEqual(sum(float(value) for value in weights.values()), 1.0, places=6)

    def test_payload_backward_compatibility(self) -> None:
        active_model = {
            "model_name": "embed_regressor.live",
            "model_id": "embed_regressor.live:AAPL:v1",
            "family": "embed_regressor",
            "model_family": "embed_regressor",
            "model_version": "v1",
            "model_kind": "ridge",
            "feature_ids": [],
            "feature_schema": {},
        }
        family_models = {
            "embed_regressor": dict(active_model),
            "temporal_predictor": {
                "model_name": "temporal_predictor.live",
                "model_id": "temporal_predictor.live:AAPL:v1",
                "family": "temporal_predictor",
                "model_family": "temporal_predictor",
                "model_version": "v1",
                "model_kind": "temporal",
                "feature_ids": [],
                "feature_schema": {},
            },
        }

        def fake_predict_resolved_model(query_vec, sym, h, *, top_k, active_model, event=None):
            family = str(active_model.get("family") or "")
            if family == "temporal_predictor":
                return (
                    0.20,
                    0.80,
                    {
                        "model_name": "temporal_predictor.live",
                        "model_id": "temporal_predictor.live:AAPL:v1",
                        "model_family": "temporal_predictor",
                        "model_version": "v1",
                        "model_kind": "temporal",
                    },
                )
            return (
                0.60,
                0.40,
                {
                    "model_name": "embed_regressor.live",
                    "model_id": "embed_regressor.live:AAPL:v1",
                    "model_family": "embed_regressor",
                    "model_version": "v1",
                    "model_kind": "ridge",
                },
            )

        with patch.object(self.predictor, "_resolve_active_model", return_value=dict(active_model)):
            with patch.object(
                self.predictor,
                "_resolve_active_model_for_family",
                side_effect=lambda symbol, horizon_s, family, primary_active_model=None: dict(family_models.get(family) or {}),
            ):
                with patch.object(self.predictor, "_predict_resolved_model", side_effect=fake_predict_resolved_model):
                    with patch.object(self.predictor, "active_model_names", return_value=["embed_regressor.live", "temporal_predictor.live"]):
                        with patch.object(self.predictor, "_track_prediction_output"):
                            prediction, confidence, explain = self.predictor._predict_single_model(
                                np.asarray([1.0], dtype=np.float32),
                                "AAPL",
                                300,
                                top_k=8,
                                event={"ts_ms": 1_700_000_000_000},
                            )

        self.assertIsInstance(prediction, float)
        self.assertIsInstance(confidence, float)
        self.assertIsInstance(explain, dict)
        self.assertEqual(str(explain["model_name"]), "embed_regressor.live")
        self.assertEqual(str(explain["model_id"]), "embed_regressor.live:AAPL:v1")
        self.assertIn("ensemble_blend", explain)
        self.assertIn("ensemble_output", explain)

    def test_family_fallback_is_excluded_from_blend(self) -> None:
        active_model = {
            "model_name": "embed_regressor.live",
            "model_id": "embed_regressor.live:AAPL:v1",
            "family": "embed_regressor",
            "model_family": "embed_regressor",
            "model_version": "v1",
            "model_kind": "ridge",
            "feature_ids": [],
            "feature_schema": {},
        }
        family_models = {
            "embed_regressor": dict(active_model),
            "temporal_predictor": {
                "model_name": "temporal_predictor.live",
                "model_id": "temporal_predictor.live:AAPL:v1",
                "family": "temporal_predictor",
                "model_family": "temporal_predictor",
                "model_version": "v1",
                "model_kind": "temporal",
                "feature_ids": [],
                "feature_schema": {},
            },
        }

        def fake_predict_resolved_model(query_vec, sym, h, *, top_k, active_model, event=None):
            family = str(active_model.get("family") or "")
            if family == "temporal_predictor":
                return (
                    0.25,
                    0.70,
                    {
                        "model_name": "temporal_predictor.live",
                        "model_id": "temporal_predictor.live:AAPL:v1",
                        "model_family": "temporal_predictor",
                        "model_version": "v1",
                        "model_kind": "temporal",
                        "serve_fallback": {
                            "requested_family": "temporal_predictor",
                            "served_family": "knn_prior",
                        },
                    },
                )
            return (
                0.55,
                0.45,
                {
                    "model_name": "embed_regressor.live",
                    "model_id": "embed_regressor.live:AAPL:v1",
                    "model_family": "embed_regressor",
                    "model_version": "v1",
                    "model_kind": "ridge",
                },
            )

        with patch.object(self.predictor, "_resolve_active_model", return_value=dict(active_model)):
            with patch.object(
                self.predictor,
                "_resolve_active_model_for_family",
                side_effect=lambda symbol, horizon_s, family, primary_active_model=None: dict(family_models.get(family) or {}),
            ):
                with patch.object(self.predictor, "_predict_resolved_model", side_effect=fake_predict_resolved_model):
                    with patch.object(self.predictor, "active_model_names", return_value=["embed_regressor.live", "temporal_predictor.live"]):
                        with patch.object(self.predictor, "_track_prediction_output"):
                            prediction, confidence, explain = self.predictor._predict_single_model(
                                np.asarray([1.0], dtype=np.float32),
                                "AAPL",
                                300,
                                top_k=8,
                                event={"ts_ms": 1_700_000_000_000},
                            )

        self.assertEqual((prediction, confidence), (0.55, 0.45))
        self.assertIn("ensemble_blend", explain)
        self.assertFalse(bool(explain["ensemble_blend"]["applied"]))
        self.assertIn("temporal_predictor", list(explain["ensemble_blend"]["missing_families"]))
        self.assertEqual(str(explain["ensemble_output"]["fallback_reason"]), "insufficient_family_predictions")

    def test_disabled_path_backward_compatibility(self) -> None:
        os.environ["ENSEMBLE_BLEND_ENABLED"] = "0"
        _, _, predictor = _reload_modules(
            "engine.runtime.storage",
            "engine.strategy.ensemble_blender",
            "engine.strategy.predictor",
        )
        active_model = {
            "model_name": "embed_regressor.live",
            "model_id": "embed_regressor.live:AAPL:v1",
            "family": "embed_regressor",
            "model_family": "embed_regressor",
            "model_version": "v1",
            "model_kind": "ridge",
            "feature_ids": [],
            "feature_schema": {},
        }
        base_result = (
            0.55,
            0.45,
            {
                "model_name": "embed_regressor.live",
                "model_id": "embed_regressor.live:AAPL:v1",
                "model_family": "embed_regressor",
                "model_version": "v1",
                "model_kind": "ridge",
            },
        )

        with patch.object(predictor, "_resolve_active_model", return_value=dict(active_model)):
            with patch.object(predictor, "_predict_resolved_model", return_value=base_result):
                with patch.object(predictor, "_track_prediction_output"):
                    prediction, confidence, explain = predictor._predict_single_model(
                        np.asarray([1.0], dtype=np.float32),
                        "AAPL",
                        300,
                        top_k=8,
                        event={"ts_ms": 1_700_000_000_000},
                    )

        self.assertEqual((prediction, confidence), (0.55, 0.45))
        self.assertEqual(str(explain["model_name"]), "embed_regressor.live")
        self.assertNotIn("ensemble_blend", explain)
        self.assertNotIn("ensemble_output", explain)

    def test_hmm_uncertainty_softens_ensemble_weights_when_enabled(self) -> None:
        os.environ["HMM_REGIME_ENSEMBLE_WEIGHT_ENABLED"] = "1"
        _, _, predictor = _reload_modules(
            "engine.runtime.storage",
            "engine.strategy.ensemble_blender",
            "engine.strategy.predictor",
        )
        active_model = {
            "model_name": "embed_regressor.live",
            "model_id": "embed_regressor.live:AAPL:v1",
            "family": "embed_regressor",
            "model_family": "embed_regressor",
            "model_version": "v1",
            "model_kind": "ridge",
            "feature_ids": [],
            "feature_schema": {},
        }
        family_models = {
            "embed_regressor": dict(active_model),
            "temporal_predictor": {
                "model_name": "temporal_predictor.live",
                "model_id": "temporal_predictor.live:AAPL:v1",
                "family": "temporal_predictor",
                "model_family": "temporal_predictor",
                "model_version": "v1",
                "model_kind": "temporal",
                "feature_ids": [],
                "feature_schema": {},
            },
        }

        def fake_predict_resolved_model(query_vec, sym, h, *, top_k, active_model, event=None):
            family = str(active_model.get("family") or "")
            if family == "temporal_predictor":
                return (
                    0.20,
                    0.80,
                    {
                        "model_name": "temporal_predictor.live",
                        "model_id": "temporal_predictor.live:AAPL:v1",
                        "model_family": "temporal_predictor",
                        "model_version": "v1",
                        "model_kind": "temporal",
                    },
                )
            return (
                0.60,
                0.40,
                {
                    "model_name": "embed_regressor.live",
                    "model_id": "embed_regressor.live:AAPL:v1",
                    "model_family": "embed_regressor",
                    "model_version": "v1",
                    "model_kind": "ridge",
                },
            )

        with patch.object(predictor, "_resolve_active_model", return_value=dict(active_model)):
            with patch.object(
                predictor,
                "_resolve_active_model_for_family",
                side_effect=lambda symbol, horizon_s, family, primary_active_model=None: dict(family_models.get(family) or {}),
            ):
                with patch.object(predictor, "_predict_resolved_model", side_effect=fake_predict_resolved_model):
                    with patch.object(predictor, "active_model_names", return_value=["embed_regressor.live", "temporal_predictor.live"]):
                        with patch.object(predictor, "compute_blend_weights", return_value={"embed_regressor": 0.90, "temporal_predictor": 0.10}):
                            with patch("engine.strategy.hmm_regime.resolve_hmm_regime_snapshot", return_value={"enabled": True, "model_available": True, "confidence": 0.0, "regime_label": "VOLATILE"}):
                                with patch.object(predictor, "_track_prediction_output"):
                                    prediction, confidence, explain = predictor._predict_single_model(
                                        np.asarray([1.0], dtype=np.float32),
                                        "AAPL",
                                        300,
                                        top_k=8,
                                        event={"ts_ms": 1_700_000_000_000},
                                    )

        self.assertIsInstance(prediction, float)
        self.assertIsInstance(confidence, float)
        self.assertIn("ensemble_blend", explain)
        self.assertIn("hmm_weight_adjustment", explain["ensemble_blend"])
        adjusted = dict(explain["ensemble_blend"]["hmm_weight_adjustment"]["adjusted_weights"])
        self.assertLess(float(adjusted["embed_regressor"]), 0.90)
        self.assertGreater(float(adjusted["temporal_predictor"]), 0.10)


if __name__ == "__main__":
    unittest.main()
