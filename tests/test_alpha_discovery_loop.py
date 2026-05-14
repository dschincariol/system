from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class AlphaDiscoveryLoopTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.tmp.name) / "alpha_discovery.db"
        self._env_backup = {
            key: os.environ.get(key)
            for key in (
                "DB_PATH",
                "MODEL_CONFIG_JSON",
                "ALPHA_DISCOVERY_ENABLED",
                "ALPHA_DISCOVERY_MAX_CANDIDATES",
                "ALPHA_DISCOVERY_ALLOWED_FAMILIES",
                "ALPHA_DISCOVERY_SHADOW_ONLY",
                "ALPHA_DISCOVERY_REQUIRE_CPCV",
                "ALPHA_DISCOVERY_REQUIRE_STAT_GATE",
                "MODEL_HORIZON_MEDIUM_S",
                "GBM_MIN_SAMPLES",
                "SYMBOLIC_ALPHA_ENABLED",
                "SYMBOLIC_ALPHA_MAX_EXPRESSIONS",
                "SYMBOLIC_ALPHA_MAX_COMPLEXITY",
                "SYMBOLIC_ALPHA_ALLOWED_OPERATORS",
                "SYMBOLIC_ALPHA_REQUIRE_SHADOW_ONLY",
            )
        }
        os.environ["DB_PATH"] = str(self.db_path)
        os.environ["MODEL_HORIZON_MEDIUM_S"] = "3600"
        os.environ["GBM_MIN_SAMPLES"] = "4"
        os.environ["SYMBOLIC_ALPHA_ENABLED"] = "0"
        os.environ["SYMBOLIC_ALPHA_MAX_EXPRESSIONS"] = "2"
        os.environ["SYMBOLIC_ALPHA_MAX_COMPLEXITY"] = "2"
        os.environ["SYMBOLIC_ALPHA_ALLOWED_OPERATORS"] = "add,sub,mul,div,abs,neg"
        os.environ["SYMBOLIC_ALPHA_REQUIRE_SHADOW_ONLY"] = "1"
        os.environ["MODEL_CONFIG_JSON"] = json.dumps(
            [
                {
                    "model_name": "gbm_regressor.alpha_seed",
                    "family": "gbm_regressor",
                    "enabled": False,
                    "prediction_enabled": False,
                    "experimental": True,
                    "feature_ids": [],
                    "symbol_universe": ["AAPL"],
                    "horizon_s": 3600,
                    "horizons_s": [3600],
                    "training_window_days": 30,
                    "risk_profile": "balanced",
                    "model_kind": "lightgbm",
                    "hyperparams": {},
                }
            ],
            separators=(",", ":"),
            sort_keys=True,
        )
        (
            self.storage,
            self.alpha,
            self.model_registry,
            self.model_lifecycle,
            self.champion_manager,
        ) = _reload_modules(
            "engine.runtime.storage",
            "engine.research.alpha_generator",
            "engine.model_registry",
            "engine.strategy.model_lifecycle",
            "engine.strategy.champion_manager",
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

    def _set_alpha_env(self, **overrides: object) -> None:
        defaults = {
            "ALPHA_DISCOVERY_ENABLED": "1",
            "ALPHA_DISCOVERY_MAX_CANDIDATES": "1",
            "ALPHA_DISCOVERY_ALLOWED_FAMILIES": "gbm_regressor",
            "ALPHA_DISCOVERY_SHADOW_ONLY": "1",
            "ALPHA_DISCOVERY_REQUIRE_CPCV": "1",
            "ALPHA_DISCOVERY_REQUIRE_STAT_GATE": "1",
        }
        defaults.update({key: str(value) for key, value in overrides.items()})
        for key, value in defaults.items():
            os.environ[str(key)] = str(value)

    def _feature_ids(self) -> list[str]:
        feature_ids = list(self.alpha.default_feature_ids() or [])
        if len(feature_ids) < 3:
            feature_ids = list(self.alpha.registered_feature_ids() or [])
        self.assertGreaterEqual(len(feature_ids), 3)
        return feature_ids[:3]

    def _base_training_config(self, feature_ids: list[str]) -> dict:
        return {
            "model_name": "gbm_regressor.alpha_seed",
            "family": "gbm_regressor",
            "feature_ids": list(feature_ids),
            "symbol_universe": ["AAPL"],
            "horizon_s": 3600,
            "horizons_s": [3600],
            "training_window_days": 30,
            "risk_profile": "balanced",
            "model_kind": "lightgbm",
            "hyperparams": {"num_leaves": 8},
            "enabled": False,
        }

    def _candidate_spec(
        self,
        feature_ids: list[str],
        *,
        candidate_name: str = "alpha_gbm_01_test",
        generation_method: str = "single_group_v1",
        group_names: list[str] | None = None,
        symbolic_candidate: dict | None = None,
    ) -> dict:
        payload = {
            "candidate_name": str(candidate_name),
            "model_family": "gbm_regressor",
            "generation_method": str(generation_method),
            "group_names": list(group_names or ["base", "momentum"]),
            "feature_ids": list(feature_ids),
            "feature_set_tag": str(self.alpha.feature_set_tag_from_ids(list(feature_ids))),
        }
        if symbolic_candidate is not None:
            payload["symbolic_candidate"] = dict(symbolic_candidate or {})
        return payload

    def _symbolic_candidate_record(self, *, feature_id: str = "symbolic.alpha.deadbeef") -> dict:
        return {
            "id": 7,
            "created_ts": 1700000000000,
            "expression_text": 'sub("price.last","price.rv_20")',
            "source_feature_ids": ["price.last", "price.rv_20"],
            "complexity": 1,
            "score": 0.42,
            "status": "accepted",
            "feature_id": str(feature_id),
        }

    def _eval_rows(self) -> list[dict]:
        return [
            {"event_id": 101, "symbol": "AAPL", "horizon_s": 3600},
            {"event_id": 102, "symbol": "AAPL", "horizon_s": 3600},
        ]

    def _raw_training_rows(self) -> list[dict]:
        return [
            {"event_id": 1, "symbol": "AAPL", "horizon_s": 3600, "label": 0.1},
            {"event_id": 2, "symbol": "AAPL", "horizon_s": 3600, "label": -0.2},
            {"event_id": 3, "symbol": "AAPL", "horizon_s": 3600, "label": 0.3},
            {"event_id": 4, "symbol": "AAPL", "horizon_s": 3600, "label": 0.2},
        ]

    def _train_result_side_effect(self, feature_ids: list[str]):
        def _side_effect(*, candidate_spec, train_cfg, loop_cfg, rows, created_ts=None, candidate_version=None):
            return {
                "ok": True,
                "status": "trained",
                "candidate_version": str(candidate_version or created_ts or 1700000000000),
                "created_ts": int(created_ts or 1700000000000),
                "model_kind": "lightgbm",
                "blob": b"alpha-model",
                "feature_schema": {
                    "feature_ids": list(feature_ids),
                    "feature_set_tag": str(candidate_spec.get("feature_set_tag") or ""),
                    "feature_count": int(len(feature_ids)),
                    "ts_ms": int(created_ts or 1700000000000),
                },
                "training_metrics": {
                    "model_name": str(candidate_spec.get("candidate_name") or ""),
                    "model_kind": "lightgbm",
                    "n_train": 4,
                    "n_eval": 2,
                    "rmse": 0.11,
                    "spearman": 0.5,
                    "directional_acc": 0.75,
                    "quality_score": 0.75,
                    "feature_ids": list(feature_ids),
                    "feature_set_tag": str(candidate_spec.get("feature_set_tag") or ""),
                    "feature_schema": {
                        "feature_ids": list(feature_ids),
                        "feature_set_tag": str(candidate_spec.get("feature_set_tag") or ""),
                        "feature_count": int(len(feature_ids)),
                        "ts_ms": int(created_ts or 1700000000000),
                    },
                    "model_version": str(candidate_version or created_ts or 1700000000000),
                    "model_family": str(candidate_spec.get("model_family") or "gbm_regressor"),
                    "signed_alpha": 1.2,
                },
                "evaluation": {
                    "rows": self._eval_rows(),
                    "predictions": [0.4, -0.1],
                    "returns": [0.6, 0.4],
                    "mean_confidence": 0.7,
                    "metrics": {
                        "rmse": 0.11,
                        "spearman": 0.5,
                        "directional_acc": 0.75,
                        "n_eval": 2,
                        "signed_alpha": 1.2,
                    },
                },
            }

        return _side_effect

    def _replay_snapshot(self, candidate_name: str, candidate_version: int, *, approved: bool) -> dict:
        return {
            "snapshot": {
                "models": {
                    "alpha-row": {
                        "model_name": str(candidate_name),
                        "model_ts_ms": int(candidate_version),
                        "symbol": "AAPL",
                        "horizon_s": 3600,
                        "regime": "global",
                        "approved": bool(approved),
                        "dir_acc": 0.65,
                        "signed_alpha": 1.2,
                        "n": 2,
                    }
                }
            }
        }

    def _fetch_marketplace_row(self, model_name: str) -> dict | None:
        con = self.storage.connect(readonly=True)
        try:
            row = con.execute(
                """
                SELECT model_id, model_name, symbol, horizon_s, regime, stage, score, trades, meta_json
                FROM model_marketplace_scores
                WHERE model_name=?
                LIMIT 1
                """,
                (str(model_name),),
            ).fetchone()
        finally:
            con.close()
        if not row:
            return None
        return {
            "model_id": str(row[0] or ""),
            "model_name": str(row[1] or ""),
            "symbol": str(row[2] or ""),
            "horizon_s": int(row[3] or 0),
            "regime": str(row[4] or "global"),
            "stage": str(row[5] or ""),
            "score": float(row[6] or 0.0),
            "trades": int(row[7] or 0),
            "meta": json.loads(row[8] or "{}"),
        }

    def _run_discovery(
        self,
        *,
        shadow_only: bool,
        validation_passed: bool = True,
        replay_approved: bool = True,
        candidate_spec: dict | None = None,
        train_cfg: dict | None = None,
    ) -> tuple[dict, dict]:
        self._set_alpha_env(ALPHA_DISCOVERY_SHADOW_ONLY="1" if shadow_only else "0")
        feature_ids = self._feature_ids()
        candidate_spec = dict(candidate_spec or self._candidate_spec(feature_ids))
        candidate_feature_ids = list(candidate_spec.get("feature_ids") or feature_ids)
        train_cfg = dict(train_cfg or self._base_training_config(feature_ids))
        fixed_now_ms = 1700000000000
        patches = [
            patch.object(self.alpha, "_now_ms", return_value=fixed_now_ms),
            patch.object(self.alpha, "_base_training_config", return_value=dict(train_cfg)),
            patch.object(self.alpha, "generate_candidate_specs", return_value=[dict(candidate_spec)]),
            patch.object(self.alpha, "build_dataset_snapshot", return_value={"dataset_id": "alpha-dataset-1"}),
            patch.object(self.alpha, "_load_labeled_feature_rows", return_value=self._raw_training_rows()),
            patch.object(self.alpha, "_train_candidate_bundle", side_effect=self._train_result_side_effect(candidate_feature_ids)),
            patch.object(self.alpha, "persist_gbm_model_record", side_effect=lambda *args, **kwargs: None),
            patch.object(
                self.alpha,
                "refresh_replay_validation_snapshot",
                return_value=self._replay_snapshot(
                    candidate_name=str(candidate_spec["candidate_name"]),
                    candidate_version=fixed_now_ms,
                    approved=bool(replay_approved),
                ),
            ),
            patch.object(
                self.alpha,
                "evaluate_statistical_promotion_gate",
                return_value=(
                    bool(validation_passed),
                    {
                        "status": "validated" if validation_passed else "validation_gate_failed",
                        "require_cpcv": True,
                        "require_stat_gate": True,
                    },
                ),
            ),
            patch.object(
                self.alpha,
                "build_model_registration_metadata",
                side_effect=lambda cfg: {
                    "model_id": str(cfg.get("model_name") or ""),
                    "model_family": str(cfg.get("family") or cfg.get("model_family") or "gbm_regressor"),
                    "instance_name": str(cfg.get("instance_name") or cfg.get("model_name") or ""),
                    "risk_profile": str(cfg.get("risk_profile") or "balanced"),
                },
            ),
        ]
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9]:
            summary = self.alpha.run_alpha_discovery()
        return summary, candidate_spec

    def test_candidate_generation_is_bounded_and_deterministic(self) -> None:
        feature_ids = self._feature_ids()
        base_cfg = self._base_training_config(feature_ids)

        first = self.alpha.generate_candidate_specs(base_config=dict(base_cfg), max_candidates=3)
        second = self.alpha.generate_candidate_specs(base_config=dict(base_cfg), max_candidates=3)

        self.assertLessEqual(len(first), 3)
        self.assertEqual(first, second)
        allowed = set(self.alpha.registered_feature_ids())
        for spec in first:
            self.assertTrue(set(spec["feature_ids"]).issubset(allowed))
            self.assertGreater(len(spec["feature_ids"]), 0)

    def test_symbolic_candidate_generation_is_bounded_and_train_safe(self) -> None:
        os.environ["SYMBOLIC_ALPHA_ENABLED"] = "1"
        os.environ["SYMBOLIC_ALPHA_REQUIRE_SHADOW_ONLY"] = "1"
        base_cfg = self._base_training_config(self._feature_ids())
        symbolic_record = self._symbolic_candidate_record()

        with patch(
            "engine.research.symbolic_alpha_generator.generate_symbolic_alpha_candidates",
            return_value=[dict(symbolic_record)],
        ):
            specs = self.alpha.generate_candidate_specs(base_config=dict(base_cfg), max_candidates=1)

        self.assertEqual(len(specs), 1)
        spec = specs[0]
        self.assertEqual(str(spec["generation_method"]), "symbolic_expression_v1")
        self.assertEqual(str((spec.get("symbolic_candidate") or {}).get("feature_id")), str(symbolic_record["feature_id"]))
        self.assertTrue(bool((spec.get("symbolic_candidate") or {}).get("shadow_only")))
        self.assertIn(str(symbolic_record["feature_id"]), list(spec.get("feature_ids") or []))
        self.assertEqual(str(spec["feature_set_tag"]), "base+symbolic")

    def test_noop_when_disabled(self) -> None:
        self._set_alpha_env(ALPHA_DISCOVERY_ENABLED="0")

        summary = self.alpha.run_alpha_discovery()

        self.assertTrue(summary["ok"])
        self.assertEqual(summary["status"], "disabled")
        self.assertEqual(self.storage.fetch_recent_alpha_candidates(limit=20), [])

    def test_provenance_persistence_records_single_candidate_lifecycle(self) -> None:
        summary, candidate_spec = self._run_discovery(shadow_only=True, validation_passed=True, replay_approved=True)

        self.assertEqual(summary["registered_shadow"], 1)
        candidates = self.storage.fetch_recent_alpha_candidates(limit=10, candidate_name=candidate_spec["candidate_name"])
        self.assertEqual(len(candidates), 1)
        candidate = candidates[0]
        self.assertEqual(candidate["status"], "registered_shadow")
        self.assertEqual(candidate["generation_method"], "single_group_v1")
        self.assertEqual(candidate["feature_ids"], candidate_spec["feature_ids"])
        self.assertIn("validation", dict(candidate["diagnostics"]))
        lifecycle = self.storage.fetch_alpha_lifecycle(candidate_id=int(candidate["id"]), limit=20)
        self.assertGreaterEqual(len(lifecycle), 5)
        self.assertTrue(all(int(row["candidate_id"]) == int(candidate["id"]) for row in lifecycle))
        stages = {str(row["stage"]) for row in lifecycle}
        self.assertTrue({"generation", "training", "shadow_validation", "registration"}.issubset(stages))

    def test_shadow_only_enforcement_registers_shadow_but_not_challenger(self) -> None:
        summary, candidate_spec = self._run_discovery(shadow_only=True, validation_passed=True, replay_approved=True)

        self.assertEqual(summary["registered_shadow"], 1)
        self.assertEqual(summary["registered_challenger"], 0)
        shadow = self.model_registry.get_stage_latest(candidate_spec["candidate_name"], "shadow")
        challenger = self.model_registry.get_stage_latest(candidate_spec["candidate_name"], "challenger")
        latest_version = self.model_lifecycle.get_latest_version(candidate_spec["candidate_name"])
        marketplace = self._fetch_marketplace_row(candidate_spec["candidate_name"])

        self.assertIsNotNone(shadow)
        self.assertIsNone(challenger)
        self.assertIsNotNone(latest_version)
        self.assertEqual(latest_version["stage"], "shadow")
        self.assertEqual(latest_version["status"], "validated")
        self.assertFalse(latest_version["live_ready"])
        self.assertIsNotNone(marketplace)
        self.assertEqual(marketplace["stage"], "shadow")
        self.assertEqual(marketplace["meta"]["score_source"], "shadow_predictions")

    def test_rejection_path_marks_candidate_rejected_without_marketplace_registration(self) -> None:
        summary, candidate_spec = self._run_discovery(shadow_only=True, validation_passed=False, replay_approved=True)

        self.assertEqual(summary["rejected"], 1)
        candidates = self.storage.fetch_recent_alpha_candidates(limit=10, candidate_name=candidate_spec["candidate_name"])
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["status"], "rejected")
        latest_version = self.model_lifecycle.get_latest_version(candidate_spec["candidate_name"])
        shadow = self.model_registry.get_stage_latest(candidate_spec["candidate_name"], "shadow")
        challenger = self.model_registry.get_stage_latest(candidate_spec["candidate_name"], "challenger")
        marketplace = self._fetch_marketplace_row(candidate_spec["candidate_name"])
        lifecycle = self.storage.fetch_alpha_lifecycle(candidate_id=int(candidates[0]["id"]), limit=20)

        self.assertIsNotNone(latest_version)
        self.assertEqual(latest_version["stage"], "retired")
        self.assertEqual(latest_version["status"], "rejected")
        self.assertIsNone(shadow)
        self.assertIsNone(challenger)
        self.assertIsNone(marketplace)
        self.assertIn("validation", {str(row["stage"]) for row in lifecycle})

    def test_validated_survivor_registers_in_existing_challenger_flow_but_stays_non_live(self) -> None:
        summary, candidate_spec = self._run_discovery(shadow_only=False, validation_passed=True, replay_approved=True)

        self.assertEqual(summary["registered_challenger"], 1)
        challenger = self.model_registry.get_stage_latest(candidate_spec["candidate_name"], "challenger")
        latest_version = self.model_lifecycle.get_latest_version(candidate_spec["candidate_name"])
        marketplace = self._fetch_marketplace_row(candidate_spec["candidate_name"])

        self.assertIsNotNone(challenger)
        self.assertIsNotNone(latest_version)
        self.assertEqual(latest_version["stage"], "challenger")
        self.assertEqual(latest_version["status"], "validated")
        self.assertFalse(latest_version["live_ready"])
        self.assertIsNotNone(marketplace)
        self.assertEqual(marketplace["stage"], "challenger")
        self.assertEqual(marketplace["meta"]["score_source"], "shadow_predictions")
        self.assertFalse(self.champion_manager._candidate_is_live_promotable(marketplace))

    def test_symbolic_candidate_registers_through_alpha_discovery_shadow_only(self) -> None:
        os.environ["SYMBOLIC_ALPHA_ENABLED"] = "1"
        os.environ["SYMBOLIC_ALPHA_REQUIRE_SHADOW_ONLY"] = "1"
        symbolic_record = self._symbolic_candidate_record()
        candidate_feature_ids = list(self.alpha.BASE_FEATURE_IDS[:2]) + [str(symbolic_record["feature_id"])]
        symbolic_spec = self._candidate_spec(
            candidate_feature_ids,
            candidate_name="alpha_gbm_symbolic_test",
            generation_method="symbolic_expression_v1",
            group_names=["base", "symbolic"],
            symbolic_candidate={
                "candidate_id": int(symbolic_record["id"]),
                "feature_id": str(symbolic_record["feature_id"]),
                "expression_text": str(symbolic_record["expression_text"]),
                "source_feature_ids": list(symbolic_record["source_feature_ids"]),
                "complexity": int(symbolic_record["complexity"]),
                "score": float(symbolic_record["score"]),
                "status": str(symbolic_record["status"]),
                "shadow_only": True,
            },
        )

        summary, candidate_spec = self._run_discovery(
            shadow_only=False,
            validation_passed=True,
            replay_approved=True,
            candidate_spec=symbolic_spec,
            train_cfg=self._base_training_config(list(self.alpha.BASE_FEATURE_IDS[:2])),
        )

        self.assertEqual(summary["registered_shadow"], 1)
        self.assertEqual(summary["registered_challenger"], 0)
        latest_version = self.model_lifecycle.get_latest_version(candidate_spec["candidate_name"])
        shadow = self.model_registry.get_stage_latest(candidate_spec["candidate_name"], "shadow")
        challenger = self.model_registry.get_stage_latest(candidate_spec["candidate_name"], "challenger")
        marketplace = self._fetch_marketplace_row(candidate_spec["candidate_name"])
        candidates = self.storage.fetch_recent_alpha_candidates(limit=10, candidate_name=candidate_spec["candidate_name"])

        self.assertIsNotNone(latest_version)
        self.assertEqual(str(latest_version["mutation_kind"]), "symbolic_alpha_discovery")
        self.assertEqual(str(latest_version["stage"]), "shadow")
        self.assertIsNotNone(shadow)
        self.assertIsNone(challenger)
        self.assertIsNotNone(marketplace)
        self.assertEqual(str(marketplace["stage"]), "shadow")
        self.assertEqual(len(candidates), 1)
        self.assertEqual(str(candidates[0]["generation_method"]), "symbolic_expression_v1")
        self.assertEqual(
            str((candidates[0]["diagnostics"] or {}).get("symbolic_candidate", {}).get("expression_text")),
            str(symbolic_record["expression_text"]),
        )


if __name__ == "__main__":
    unittest.main()
