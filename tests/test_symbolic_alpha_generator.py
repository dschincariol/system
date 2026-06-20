from __future__ import annotations

import importlib
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


class _FakeConnection:
    def close(self) -> None:
        return None


class SymbolicAlphaGeneratorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.tmp.name) / "symbolic_alpha.db"
        self._env_backup = {
            key: os.environ.get(key)
            for key in (
                "DB_PATH",
                "SYMBOLIC_ALPHA_ENABLED",
                "SYMBOLIC_ALPHA_MAX_EXPRESSIONS",
                "SYMBOLIC_ALPHA_MAX_COMPLEXITY",
                "SYMBOLIC_ALPHA_ALLOWED_OPERATORS",
                "SYMBOLIC_ALPHA_REQUIRE_SHADOW_ONLY",
                "SYMBOLIC_ALPHA_PIPELINE_COMPAT_ENABLED",
            )
        }
        os.environ["DB_PATH"] = str(self.db_path)
        os.environ["SYMBOLIC_ALPHA_ENABLED"] = "1"
        os.environ["SYMBOLIC_ALPHA_MAX_EXPRESSIONS"] = "2"
        os.environ["SYMBOLIC_ALPHA_MAX_COMPLEXITY"] = "2"
        os.environ["SYMBOLIC_ALPHA_ALLOWED_OPERATORS"] = "add,sub,mul,div,abs,neg"
        os.environ["SYMBOLIC_ALPHA_REQUIRE_SHADOW_ONLY"] = "1"
        os.environ["SYMBOLIC_ALPHA_PIPELINE_COMPAT_ENABLED"] = "1"
        _, self.storage, self.symbolic = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.research.symbolic_alpha_generator",
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

    def test_safe_expression_validation_and_eval(self) -> None:
        validated = self.symbolic.validate_symbolic_expression('div("price.last","price.rv_20")')
        self.assertEqual(validated["source_feature_ids"], ["price.last", "price.rv_20"])
        self.assertEqual(int(validated["complexity"]), 1)

        value = self.symbolic.evaluate_symbolic_expression(
            'div("price.last","price.rv_20")',
            {"price.last": 10.0, "price.rv_20": 2.0},
        )
        self.assertAlmostEqual(float(value), 5.0, places=6)

        with self.assertRaises(ValueError):
            self.symbolic.validate_symbolic_expression('__import__("os").system("whoami")')

    def test_persistence_and_definition_round_trip(self) -> None:
        record = self.symbolic.persist_symbolic_alpha_candidate(
            expression_text='sub("price.momentum_1h","price.rv_20")',
            score=0.42,
            diagnostics={"source": "unit_test"},
        )
        self.assertGreater(int(record["id"]), 0)
        self.assertEqual(str(record["status"]), "accepted")

        rows = self.symbolic.list_symbolic_alpha_candidates()
        self.assertEqual(len(rows), 1)
        self.assertEqual(str(rows[0]["feature_id"]), str(record["feature_id"]))

        loaded = self.symbolic.load_symbolic_feature_definition(str(record["feature_id"]))
        self.assertIsNotNone(loaded)
        self.assertEqual(str(loaded["expression_text"]), 'sub("price.momentum_1h","price.rv_20")')

        con = self.storage.connect(readonly=True)
        try:
            db_row = con.execute(
                "SELECT expression_text, status FROM symbolic_alpha_candidates LIMIT 1"
            ).fetchone()
        finally:
            con.close()
        self.assertEqual(str(db_row[0]), 'sub("price.momentum_1h","price.rv_20")')
        self.assertEqual(str(db_row[1]), "accepted")

    def test_generation_is_bounded(self) -> None:
        rows = [
            {
                "event_id": idx + 1,
                "symbol": "AAPL",
                "horizon_s": 300,
                "target_z": float(idx),
                "event": {"ts_ms": 1_700_000_000_000 + idx, "title": "", "body": "", "source": ""},
            }
            for idx in range(30)
        ]
        matrix = {
            "price.last": [float(idx) for idx in range(30)],
            "price.rv_20": [float(idx) * 0.5 for idx in range(30)],
            "price.volume": [float((idx % 3) - 1) for idx in range(30)],
        }
        with patch.object(self.symbolic, "_load_training_rows", return_value=rows), patch.object(
            self.symbolic, "_discover_source_feature_ids", return_value=list(matrix.keys())
        ), patch.object(self.symbolic, "_build_feature_matrix", return_value=matrix):
            records = self.symbolic.generate_symbolic_alpha_candidates(
                {"model_name": "embed_regressor", "feature_ids": list(matrix.keys())},
                max_expressions=2,
            )

        self.assertLessEqual(len(records), 2)
        self.assertTrue(records)
        self.assertTrue(all(str(record["feature_id"]).startswith("symbolic.alpha.") for record in records))

    def test_feature_registry_can_compute_persisted_symbolic_feature(self) -> None:
        record = self.symbolic.persist_symbolic_alpha_candidate(
            expression_text='sub("base.session_us","base.session_eu")',
            score=0.25,
        )
        (_, _, _, feature_registry) = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.research.symbolic_alpha_generator",
            "engine.strategy.feature_registry",
        )
        snap = feature_registry.build_feature_snapshot(
            event={"ts_ms": 14 * 60 * 60 * 1000, "title": "", "body": "", "source": ""},
            symbol="SPY",
            feature_ids=[str(record["feature_id"])],
        )
        self.assertIn(str(record["feature_id"]), snap)
        self.assertAlmostEqual(float(snap[str(record["feature_id"])]), 1.0, places=6)

    def test_disabled_path_is_compatible(self) -> None:
        os.environ["SYMBOLIC_ALPHA_ENABLED"] = "0"
        (_, _, symbolic) = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.research.symbolic_alpha_generator",
        )
        configs = symbolic.build_symbolic_candidate_model_configs(
            [{"model_name": "embed_regressor", "feature_ids": ["price.last"]}]
        )
        self.assertEqual(configs, [])

    def test_pipeline_handoff_keeps_symbolic_variants_shadow_only(self) -> None:
        (_, _, pipeline) = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.strategy.pipeline_train_and_eval",
        )

        model_cfg = {
            "model_name": "embed_regressor.symbolic_deadbeef",
            "family": "embed_regressor",
            "feature_ids": ["price.last", "symbolic.alpha.deadbeef"],
            "symbolic_candidate": {
                "feature_id": "symbolic.alpha.deadbeef",
                "expression_text": 'sub("price.last","price.rv_20")',
                "shadow_only": True,
            },
            "shadow_only": True,
        }
        registered_versions = []
        registered_models = []
        audits = []

        def _register_model_version(**kwargs):
            registered_versions.append(dict(kwargs))

        def _register_model(**kwargs):
            registered_models.append(dict(kwargs))

        def _audit(**kwargs):
            audits.append(dict(kwargs))

        with patch.dict(
            os.environ,
            {"RUNTIME_WORKLOAD_PROFILE": "offline", "ALLOW_TRAINING": "1"},
        ), patch.object(pipeline, "training_allowed", return_value=True), patch.object(
            pipeline, "_data_gates_or_exit", return_value=None
        ), patch.object(pipeline, "acquire_job_lock", return_value=True), patch.object(
            pipeline, "release_job_lock", return_value=None
        ), patch.object(pipeline, "touch_job_lock", return_value=None), patch.object(
            pipeline, "put_job_heartbeat", return_value=None
        ), patch.object(pipeline, "connect", side_effect=lambda: _FakeConnection()), patch.object(
            pipeline, "load_model_configs", return_value=[dict(model_cfg)]
        ), patch.object(
            pipeline, "_extend_with_symbolic_model_configs", side_effect=lambda configs: list(configs)
        ), patch.object(
            pipeline, "_latest_embed_eval_snapshot", side_effect=[0, 123]
        ), patch.object(
            pipeline, "_run_python", return_value=0
        ), patch.object(
            pipeline, "_load_embed_eval_rows", return_value=[("ridge", 20, 0.2, 0.6)]
        ), patch.object(
            pipeline, "load_feature_schema",
            return_value={"feature_ids": list(model_cfg["feature_ids"]), "feature_set_tag": "base+symbolic", "ts_ms": 123},
        ), patch.object(
            pipeline, "_net_eval_metrics", return_value=None
        ), patch.object(
            pipeline, "register_model_version", side_effect=_register_model_version
        ), patch.object(
            pipeline, "record_version_performance", return_value=None
        ), patch.object(
            pipeline, "register_model", side_effect=_register_model
        ), patch.object(
            pipeline, "audit", side_effect=_audit
        ), patch.object(
            pipeline, "promotion_allowed", side_effect=AssertionError("promotion should not be evaluated for shadow-only symbolic variants")
        ):
            rc = pipeline.main()

        self.assertEqual(int(rc), 0)
        self.assertEqual(len(registered_versions), 1)
        self.assertEqual(str(registered_versions[0]["stage"]), "shadow")
        self.assertEqual(str(registered_versions[0]["mutation_kind"]), "symbolic_alpha_discovery")
        self.assertEqual(len(registered_models), 1)
        self.assertEqual(str(registered_models[0]["stage"]), "shadow")
        self.assertEqual(audits[-1]["action"], "shadow_only")


if __name__ == "__main__":
    unittest.main()
