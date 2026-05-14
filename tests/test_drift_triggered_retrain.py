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


def _labels_dataset_snapshot(row_count: int = 128) -> dict:
    return {
        "model_name": "regime_stats_v2",
        "lookback_days": 180,
        "symbols": ["AAPL", "MSFT"],
        "horizons": [300],
        "feature_ids": [],
        "captured_ts_ms": 1,
        "sources": {
            "labels": {
                "table": "labels",
                "row_count": int(row_count),
                "latest_created_at_ms": 1,
                "latest_event_ts_ms": 1,
                "distinct_symbols": 2,
                "distinct_horizons": 1,
            }
        },
        "fingerprint": "dataset-fingerprint",
    }


def _learning_signals(*, drift_ratio: float, drift_detected: bool, performance_drop: bool, regime_shift: bool) -> dict:
    return {
        "model_name": "regime_stats_v2",
        "ts_ms": 1,
        "performance_drop": bool(performance_drop),
        "performance_reasons": (["runtime_negative_pnl"] if performance_drop else []),
        "drift_detected": bool(drift_detected),
        "regime_shift": bool(regime_shift),
        "drift_ratio": float(drift_ratio),
        "drift_ratio_trigger": 1.25,
        "distribution_state": ("CRITICAL" if regime_shift else "NORMAL"),
        "distribution_snapshot": {},
        "runtime_signal": {
            "detected": bool(performance_drop),
            "reasons": (["runtime_negative_pnl"] if performance_drop else []),
            "trade_count": 12,
            "rolling_total_pnl": -3.0 if performance_drop else 3.0,
            "recent_total_pnl": -2.0 if performance_drop else 2.0,
            "win_rate": 0.40 if performance_drop else 0.65,
            "champion_model_version": "v000001",
        },
        "shadow_signal": {
            "detected": False,
            "reasons": [],
            "points": 0,
            "avg_dir_acc": None,
            "avg_net_rmse": None,
        },
        "temporal_shadow_signal": {
            "detected": False,
            "reasons": [],
            "failed_rows": 0,
        },
    }


class DriftTriggeredRetrainTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.tmp.name) / "drift_retrain_test.db"
        self._env_backup = {
            key: os.environ.get(key)
            for key in (
                "DB_PATH",
                "DRIFT_RETRAIN_ENABLED",
                "DRIFT_RETRAIN_COOLDOWN_S",
                "DRIFT_RETRAIN_MIN_DEGRADATION",
                "DRIFT_RETRAIN_REQUIRE_CPCV",
                "DRIFT_RETRAIN_REQUIRE_STAT_GATE",
                "DRIFT_RETRAIN_MAX_PARALLEL_JOBS",
                "CHAMPION_PROMOTION_USE_STAT_GATE",
                "CPCV_ENABLED",
            )
        }
        os.environ["DB_PATH"] = str(self.db_path)
        os.environ["DRIFT_RETRAIN_ENABLED"] = "1"
        os.environ["DRIFT_RETRAIN_COOLDOWN_S"] = "3600"
        os.environ["DRIFT_RETRAIN_MIN_DEGRADATION"] = "0.25"
        os.environ["DRIFT_RETRAIN_REQUIRE_CPCV"] = "1"
        os.environ["DRIFT_RETRAIN_REQUIRE_STAT_GATE"] = "1"
        os.environ["DRIFT_RETRAIN_MAX_PARALLEL_JOBS"] = "1"
        modules = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.strategy.model_lifecycle",
            "engine.strategy.champion_manager",
            "engine.strategy.drift_retrain_controller",
        )
        self.storage = modules[1]
        self.lifecycle = modules[2]
        self.champion_manager = modules[3]
        self.controller = modules[4]
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

    def test_noop_when_disabled(self) -> None:
        os.environ["DRIFT_RETRAIN_ENABLED"] = "0"
        _, self.storage, self.lifecycle, self.champion_manager, self.controller = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.strategy.model_lifecycle",
            "engine.strategy.champion_manager",
            "engine.strategy.drift_retrain_controller",
        )
        self.storage.init_db()

        with patch.object(self.controller, "get_lifecycle_summary") as mock_summary:
            result = self.controller.run_drift_retrain_job()

        self.assertTrue(bool(result.get("ok")))
        self.assertEqual(str(result.get("status")), "disabled")
        self.assertFalse(bool(result.get("enabled")))
        mock_summary.assert_not_called()
        self.assertEqual(self.storage.fetch_recent_drift_retrain_events(limit=10), [])

    def test_noop_when_insufficient_evidence(self) -> None:
        summary = {
            "ok": True,
            "families": {
                "regime_stats_v2": {
                    "latest": {"model_version": "v000001", "stage": "champion", "status": "live", "live_ready": True},
                    "learning_signals": _learning_signals(
                        drift_ratio=1.05,
                        drift_detected=False,
                        performance_drop=False,
                        regime_shift=False,
                    ),
                }
            },
        }

        with patch.object(self.controller, "get_lifecycle_summary", return_value=summary), patch.object(
            self.controller, "create_training_plan"
        ) as mock_plan, patch.object(self.controller, "dispatch_training_plan") as mock_dispatch:
            result = self.controller.run_drift_retrain_job()

        self.assertTrue(bool(result.get("ok")))
        self.assertEqual(list(result.get("triggered_models") or []), [])
        mock_plan.assert_not_called()
        mock_dispatch.assert_not_called()
        events = self.storage.fetch_recent_drift_retrain_events(limit=5, model_name="regime_stats_v2")
        self.assertEqual(len(events), 1)
        self.assertEqual(str(events[0].get("outcome_status")), "insufficient_evidence")

    def test_retrain_trigger_when_drift_threshold_exceeded(self) -> None:
        dataset_used = _labels_dataset_snapshot()
        captured: dict[str, object] = {}
        summary = {
            "ok": True,
            "families": {
                "regime_stats_v2": {
                    "latest": {"model_version": "v000001", "stage": "champion", "status": "live", "live_ready": True},
                    "learning_signals": _learning_signals(
                        drift_ratio=1.60,
                        drift_detected=True,
                        performance_drop=False,
                        regime_shift=False,
                    ),
                }
            },
        }
        plan = {
            "model_name": "regime_stats_v2",
            "model_version": "v000002",
            "parent_version": "v000001",
            "job_name": "train_model_v2",
            "module_name": "engine.strategy.jobs.train_model_v2",
            "dataset_used": dataset_used,
            "train_scope": {"dataset_used": dataset_used},
        }

        def _dispatch(plan_payload, *, triggered_by):
            captured["plan"] = dict(plan_payload)
            captured["triggered_by"] = str(triggered_by)
            return {"ok": True, "run_id": 17, "model_version": "v000002"}

        with patch.object(self.controller, "get_lifecycle_summary", return_value=summary), patch.object(
            self.controller, "create_training_plan", return_value=plan
        ), patch.object(self.controller, "dispatch_training_plan", side_effect=_dispatch):
            result = self.controller.run_drift_retrain_job()

        self.assertEqual(list(result.get("triggered_models") or []), ["regime_stats_v2"])
        self.assertEqual(str(captured["triggered_by"]), "drift_triggered_retrain")
        dispatched_plan = dict(captured["plan"] or {})
        self.assertEqual(str(dispatched_plan.get("mutation_kind")), "drift_retrain")
        self.assertEqual(
            str((((dispatched_plan.get("train_scope") or {}).get("promotion_requirements") or {}).get("source"))),
            "drift_triggered_retrain",
        )
        self.assertTrue(
            bool((((dispatched_plan.get("train_scope") or {}).get("promotion_requirements") or {}).get("require_cpcv")))
        )
        events = self.storage.fetch_recent_drift_retrain_events(limit=5, model_name="regime_stats_v2")
        self.assertEqual(len(events), 1)
        self.assertEqual(str(events[0].get("action_taken")), "queue_training")
        self.assertEqual(str(events[0].get("outcome_status")), "dispatched")
        self.assertEqual(str(events[0].get("candidate_version")), "v000002")

    def test_cooldown_enforcement(self) -> None:
        dataset_used = _labels_dataset_snapshot()
        summary = {
            "ok": True,
            "families": {
                "regime_stats_v2": {
                    "latest": {"model_version": "v000001", "stage": "champion", "status": "live", "live_ready": True},
                    "learning_signals": _learning_signals(
                        drift_ratio=1.50,
                        drift_detected=True,
                        performance_drop=False,
                        regime_shift=False,
                    ),
                }
            },
        }
        plan = {
            "model_name": "regime_stats_v2",
            "model_version": "v000002",
            "parent_version": "v000001",
            "job_name": "train_model_v2",
            "module_name": "engine.strategy.jobs.train_model_v2",
            "dataset_used": dataset_used,
            "train_scope": {"dataset_used": dataset_used},
        }

        with patch.object(self.controller, "get_lifecycle_summary", return_value=summary), patch.object(
            self.controller, "create_training_plan", return_value=plan
        ), patch.object(
            self.controller,
            "dispatch_training_plan",
            return_value={"ok": True, "run_id": 21, "model_version": "v000002"},
        ) as mock_dispatch, patch.object(self.controller, "_count_open_retrain_runs", return_value=0):
            first = self.controller.run_drift_retrain_job()
            second = self.controller.run_drift_retrain_job()

        self.assertEqual(list(first.get("triggered_models") or []), ["regime_stats_v2"])
        self.assertEqual(list(second.get("triggered_models") or []), [])
        self.assertEqual(mock_dispatch.call_count, 1)
        events = self.storage.fetch_recent_drift_retrain_events(limit=5, model_name="regime_stats_v2")
        self.assertEqual(str(events[0].get("outcome_status")), "cooldown")
        self.assertTrue(bool(events[0].get("cooldown_applied")))

    def test_duplicate_trigger_suppression(self) -> None:
        os.environ["DRIFT_RETRAIN_COOLDOWN_S"] = "0"
        _, self.storage, self.lifecycle, self.champion_manager, self.controller = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.strategy.model_lifecycle",
            "engine.strategy.champion_manager",
            "engine.strategy.drift_retrain_controller",
        )
        self.storage.init_db()

        dataset_used = _labels_dataset_snapshot()
        summary = {
            "ok": True,
            "families": {
                "regime_stats_v2": {
                    "latest": {"model_version": "v000001", "stage": "champion", "status": "live", "live_ready": True},
                    "learning_signals": _learning_signals(
                        drift_ratio=1.45,
                        drift_detected=True,
                        performance_drop=False,
                        regime_shift=False,
                    ),
                }
            },
        }
        plan = {
            "model_name": "regime_stats_v2",
            "model_version": "v000002",
            "parent_version": "v000001",
            "job_name": "train_model_v2",
            "module_name": "engine.strategy.jobs.train_model_v2",
            "dataset_used": dataset_used,
            "train_scope": {"dataset_used": dataset_used},
        }

        with patch.object(self.controller, "get_lifecycle_summary", return_value=summary), patch.object(
            self.controller, "create_training_plan", return_value=plan
        ), patch.object(
            self.controller,
            "dispatch_training_plan",
            side_effect=[
                {"ok": True, "run_id": 5, "model_version": "v000002"},
                {"ok": True, "skipped": True, "reason": "training_already_pending", "model_version": "v000002"},
            ],
        ) as mock_dispatch, patch.object(self.controller, "_count_open_retrain_runs", return_value=0):
            first = self.controller.run_drift_retrain_job()
            second = self.controller.run_drift_retrain_job()

        self.assertEqual(list(first.get("triggered_models") or []), ["regime_stats_v2"])
        self.assertEqual(list(second.get("triggered_models") or []), [])
        self.assertEqual(mock_dispatch.call_count, 2)
        events = self.storage.fetch_recent_drift_retrain_events(limit=5, model_name="regime_stats_v2")
        self.assertEqual(str(events[0].get("action_taken")), "duplicate_suppressed")
        self.assertEqual(str(events[0].get("outcome_status")), "training_already_pending")

    def test_supported_hmm_family_is_blocked_explicitly_when_prerequisites_are_missing(self) -> None:
        summary = {
            "ok": True,
            "families": {
                "hmm_regime": {
                    "latest": {"model_version": "hmm-000001", "stage": "champion", "status": "live", "live_ready": True},
                    "learning_signals": _learning_signals(
                        drift_ratio=1.60,
                        drift_detected=True,
                        performance_drop=False,
                        regime_shift=False,
                    ),
                }
            },
        }
        dataset_used = {
            "model_name": "hmm_regime",
            "sources": {
                "prices": {"row_count": 64, "latest_ts_ms": 1, "symbol": "SPY"},
                "regime_vectors": {"usable_rows": 64, "required_min_rows": 96},
            },
        }
        plan = {
            "model_name": "hmm_regime",
            "model_version": "hmm-000002",
            "parent_version": "hmm-000001",
            "job_name": "train_hmm_regime",
            "module_name": "engine.strategy.jobs.train_hmm_regime",
            "dataset_used": dataset_used,
            "train_scope": {
                "symbols": ["SPY"],
                "min_rows": 96,
                "dataset_used": dataset_used,
            },
        }

        with patch.object(self.controller, "get_lifecycle_summary", return_value=summary), patch.object(
            self.controller, "create_training_plan", return_value=plan
        ), patch.object(self.controller, "dispatch_training_plan") as mock_dispatch:
            result = self.controller.run_drift_retrain_job()

        self.assertEqual(list(result.get("triggered_models") or []), [])
        self.assertEqual(list(result.get("skipped_models") or []), ["hmm_regime"])
        mock_dispatch.assert_not_called()
        events = self.storage.fetch_recent_drift_retrain_events(limit=5, model_name="hmm_regime")
        self.assertEqual(len(events), 1)
        self.assertEqual(str(events[0].get("action_taken")), "block_training")
        self.assertEqual(str(events[0].get("outcome_status")), "missing_training_prerequisites")

    def test_unsupported_family_records_explicit_outcome(self) -> None:
        summary = {
            "ok": True,
            "families": {
                "unknown_runtime_family": {
                    "latest": {"model_version": "mystery-000001", "stage": "champion", "status": "live", "live_ready": True},
                    "learning_signals": _learning_signals(
                        drift_ratio=1.60,
                        drift_detected=True,
                        performance_drop=False,
                        regime_shift=False,
                    ),
                }
            },
        }

        with patch.object(self.controller, "get_lifecycle_summary", return_value=summary), patch.object(
            self.controller, "create_training_plan", return_value={}
        ), patch.object(self.controller, "dispatch_training_plan") as mock_dispatch:
            result = self.controller.run_drift_retrain_job()

        self.assertEqual(list(result.get("triggered_models") or []), [])
        self.assertEqual(list(result.get("skipped_models") or []), ["unknown_runtime_family"])
        mock_dispatch.assert_not_called()
        events = self.storage.fetch_recent_drift_retrain_events(limit=5, model_name="unknown_runtime_family")
        self.assertEqual(len(events), 1)
        self.assertEqual(str(events[0].get("action_taken")), "unsupported_family")
        self.assertEqual(str(events[0].get("outcome_status")), "unsupported_model_family")

    def test_promotion_still_requires_existing_gates(self) -> None:
        os.environ["CHAMPION_PROMOTION_USE_STAT_GATE"] = "0"
        os.environ["CPCV_ENABLED"] = "0"
        _, self.storage, self.lifecycle, self.champion_manager, self.controller = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.strategy.model_lifecycle",
            "engine.strategy.champion_manager",
            "engine.strategy.drift_retrain_controller",
        )
        self.storage.init_db()

        promotion_requirements = {
            "source": "drift_triggered_retrain",
            "require_cpcv": True,
            "require_stat_gate": True,
            "config": {"enabled": True, "cpcv": {"enabled": True}},
        }
        self.lifecycle.register_model_version(
            model_name="regime_stats_v2",
            model_version="vdrift-000001",
            model_kind="regime_stats_v2",
            parent_version="v000001",
            mutation_kind="drift_retrain",
            stage="shadow",
            status="queued",
            live_ready=False,
            training_job_name="train_model_v2",
            train_scope={"promotion_requirements": promotion_requirements},
            meta={"trigger": {"promotion_requirements": promotion_requirements}},
        )

        version_row = self.lifecycle.get_model_version("regime_stats_v2", "vdrift-000001")
        self.assertIsNotNone(version_row)
        self.assertEqual(str(version_row.get("stage")), "shadow")
        self.assertFalse(bool(version_row.get("live_ready")))

        captured: dict[str, object] = {}

        def _fake_gate(**kwargs):
            captured.update(kwargs)
            return False, {"passed": False, "status": "blocked"}

        row = {
            "model_name": "regime_stats_v2",
            "meta": {
                "model_version": "vdrift-000001",
                "score_source": "pnl_attribution",
            },
        }

        with patch.object(self.champion_manager, "evaluate_statistical_promotion_gate", side_effect=_fake_gate):
            passed, diagnostics = self.champion_manager._evaluate_promotion_stat_gate(row, 1)

        self.assertFalse(bool(passed))
        self.assertFalse(bool(diagnostics.get("passed")))
        config = dict(captured.get("config") or {})
        self.assertTrue(bool(config.get("enabled")))
        self.assertTrue(bool(dict(config.get("cpcv") or {}).get("enabled")))


if __name__ == "__main__":
    unittest.main()
