from __future__ import annotations

import importlib
import json
import os
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


class CompetitionPreSubmitRevalidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.prev_db_path = os.environ.get("DB_PATH")
        self.prev_storage_backend = os.environ.get("TS_STORAGE_BACKEND")
        self.prev_capital_plan_age_ms = os.environ.get("COMPETITION_CAPITAL_PLAN_MAX_AGE_MS")
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "competition_pre_submit_revalidation.db")
        os.environ["TS_STORAGE_BACKEND"] = "sqlite"
        os.environ["COMPETITION_CAPITAL_PLAN_MAX_AGE_MS"] = "60000"
        self._reload_runtime_modules()

    def tearDown(self) -> None:
        if self.prev_db_path is None:
            os.environ.pop("DB_PATH", None)
        else:
            os.environ["DB_PATH"] = str(self.prev_db_path)
        if self.prev_storage_backend is None:
            os.environ.pop("TS_STORAGE_BACKEND", None)
        else:
            os.environ["TS_STORAGE_BACKEND"] = str(self.prev_storage_backend)
        if self.prev_capital_plan_age_ms is None:
            os.environ.pop("COMPETITION_CAPITAL_PLAN_MAX_AGE_MS", None)
        else:
            os.environ["COMPETITION_CAPITAL_PLAN_MAX_AGE_MS"] = str(self.prev_capital_plan_age_ms)

        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception:
            pass
        self.tmp.cleanup()

    def _reload_runtime_modules(self) -> None:
        _, self.storage, self.champion_manager, self.broker_apply_orders = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.strategy.champion_manager",
            "engine.execution.broker_apply_orders",
        )
        self.storage.init_db()

    def _set_competition_plan(
        self,
        *,
        champion_model_name: str,
        models: list[dict],
        group_budget_fraction: float = 0.40,
        risk_limit_multiplier: float = 1.0,
    ) -> None:
        now_ms = int(time.time() * 1000)
        self.champion_manager.meta_set(
            "competition_capital_plan",
            json.dumps(
                {
                    "updated_ts_ms": int(now_ms),
                    "allocation_strategy": "proportional",
                    "allocations": {
                        "AAPL|300|global": {
                            "symbol": "AAPL",
                            "horizon_s": 300,
                            "regime": "global",
                            "champion_model_name": str(champion_model_name),
                            "allocation_strategy": "proportional",
                            "group_budget_fraction": float(group_budget_fraction),
                            "risk_limit_multiplier": float(risk_limit_multiplier),
                            "models": list(models),
                        }
                    },
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
        )

    def _competition_policy(self, model_name: str | None) -> dict:
        return self.champion_manager.get_competition_policy_for_intent(
            symbol="AAPL",
            horizon_s=300,
            model_name=model_name,
            regime="global",
        )

    def _real_order(self, *, model_name: str | None, model_id: str, competition: dict | None = None) -> dict:
        order = {
            "symbol": "AAPL",
            "horizon_s": 300,
            "regime": "global",
            "model_id": str(model_id),
            "execution_target": "real",
        }
        if model_name is not None:
            order["model_name"] = str(model_name)
        if competition is not None:
            order["competition"] = dict(competition)
        return order

    def test_invalid_competition_routes_to_shadow(self) -> None:
        self._set_competition_plan(
            champion_model_name="champ_aapl_v1",
            models=[
                {
                    "model_name": "champ_aapl_v1",
                    "allocation_fraction": 1.0,
                    "effective_allocation_fraction": 0.40,
                    "model_risk_limit_multiplier": 1.0,
                }
            ],
        )
        previous_policy = self._competition_policy("champ_aapl_v1")

        self._set_competition_plan(
            champion_model_name="challenger_aapl_v2",
            models=[
                {
                    "model_name": "champ_aapl_v1",
                    "allocation_fraction": 0.25,
                    "effective_allocation_fraction": 0.10,
                    "model_risk_limit_multiplier": 1.0,
                },
                {
                    "model_name": "challenger_aapl_v2",
                    "allocation_fraction": 0.75,
                    "effective_allocation_fraction": 0.30,
                    "model_risk_limit_multiplier": 1.0,
                },
            ],
        )

        updated_orders, rerouted_orders = self.broker_apply_orders._pre_submit_revalidate_competition(
            [
                self._real_order(
                    model_name="champ_aapl_v1",
                    model_id="champ_aapl_v1",
                    competition=previous_policy,
                )
            ]
        )

        self.assertEqual(len(updated_orders), 1)
        self.assertEqual(len(rerouted_orders), 1)
        order = dict(updated_orders[0] or {})
        competition = dict(order.get("competition") or {})
        real_orders, shadow_groups = self.broker_apply_orders._split_execution_payload(updated_orders)

        self.assertEqual(str(order.get("execution_target") or ""), "shadow")
        self.assertEqual(str(competition.get("reason") or ""), "champion_mismatch")
        self.assertIn("champion_mismatch", list(order.get("competition_pre_submit_reasons") or []))
        self.assertEqual(str(competition.get("champion_model_name") or ""), "challenger_aapl_v2")
        self.assertEqual(len(real_orders), 0)
        self.assertEqual(len(shadow_groups.get("champ_aapl_v1") or []), 1)
        self.assertEqual(str((rerouted_orders[0] or {}).get("reason") or ""), "champion_mismatch")

    def test_valid_competition_allows_execution(self) -> None:
        self._set_competition_plan(
            champion_model_name="champ_aapl_v1",
            models=[
                {
                    "model_name": "champ_aapl_v1",
                    "allocation_fraction": 1.0,
                    "effective_allocation_fraction": 0.40,
                    "model_risk_limit_multiplier": 1.0,
                }
            ],
        )
        previous_policy = self._competition_policy("champ_aapl_v1")

        updated_orders, rerouted_orders = self.broker_apply_orders._pre_submit_revalidate_competition(
            [
                self._real_order(
                    model_name="champ_aapl_v1",
                    model_id="champ_aapl_v1",
                    competition=previous_policy,
                )
            ]
        )

        self.assertEqual(len(updated_orders), 1)
        self.assertEqual(rerouted_orders, [])
        order = dict(updated_orders[0] or {})
        competition = dict(order.get("competition") or {})
        real_orders, shadow_groups = self.broker_apply_orders._split_execution_payload(updated_orders)

        self.assertEqual(str(order.get("execution_target") or ""), "real")
        self.assertFalse(order.get("competition_pre_submit_reasons"))
        self.assertFalse(bool(competition.get("blocked")))
        self.assertEqual(str(competition.get("champion_model_name") or ""), "champ_aapl_v1")
        self.assertEqual(len(real_orders), 1)
        self.assertEqual(shadow_groups, {})

    def test_missing_model_blocks_execution(self) -> None:
        self._set_competition_plan(
            champion_model_name="champ_aapl_v1",
            models=[
                {
                    "model_name": "champ_aapl_v1",
                    "allocation_fraction": 1.0,
                    "effective_allocation_fraction": 0.40,
                    "model_risk_limit_multiplier": 1.0,
                }
            ],
        )

        updated_orders, rerouted_orders = self.broker_apply_orders._pre_submit_revalidate_competition(
            [
                self._real_order(
                    model_name=None,
                    model_id="baseline",
                    competition={},
                )
            ]
        )

        self.assertEqual(len(updated_orders), 1)
        self.assertEqual(len(rerouted_orders), 1)
        order = dict(updated_orders[0] or {})
        competition = dict(order.get("competition") or {})
        real_orders, shadow_groups = self.broker_apply_orders._split_execution_payload(updated_orders)

        self.assertEqual(str(order.get("execution_target") or ""), "shadow")
        self.assertEqual(str(competition.get("reason") or ""), "model_identity_missing")
        self.assertIn("model_identity_missing", list(order.get("competition_pre_submit_reasons") or []))
        self.assertEqual(len(real_orders), 0)
        self.assertEqual(len(shadow_groups.get("baseline") or []), 1)
        self.assertEqual(str((rerouted_orders[0] or {}).get("reason") or ""), "model_identity_missing")


if __name__ == "__main__":
    unittest.main()
