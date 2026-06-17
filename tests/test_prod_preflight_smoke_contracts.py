from __future__ import annotations

import importlib
import io
import json
import os
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_module():
    import engine.runtime.prod_preflight as prod_preflight

    return importlib.reload(prod_preflight)


class ProdPreflightSmokeContractTests(unittest.TestCase):
    def test_run_cmd_sets_preflight_smoke_env_for_child_processes(self) -> None:
        prod_preflight = _reload_module()
        captured_env = {}

        def _fake_run(argv, cwd, env, stdout, stderr, timeout, check, text):
            captured_env.update(dict(env))
            return subprocess.CompletedProcess(argv, 0, stdout="ok")

        with patch.object(prod_preflight.subprocess, "run", side_effect=_fake_run):
            rc, out = prod_preflight._run_cmd("engine.strategy.portfolio_rebalance", ["python", "-m", "x"], timeout_s=5)

        self.assertEqual(rc, 0)
        self.assertEqual(out, "ok")
        self.assertEqual(captured_env["ENGINE_SUPERVISED"], "1")
        self.assertEqual(captured_env["PREFLIGHT_SMOKE"], "1")

    def test_run_cmd_can_point_child_process_at_isolated_smoke_db(self) -> None:
        prod_preflight = _reload_module()
        captured_env = {}

        def _fake_run(argv, cwd, env, stdout, stderr, timeout, check, text):
            captured_env.update(dict(env))
            return subprocess.CompletedProcess(argv, 0, stdout="ok")

        with patch.object(prod_preflight.subprocess, "run", side_effect=_fake_run):
            rc, out = prod_preflight._run_cmd(
                "engine.strategy.portfolio_rebalance",
                ["python", "-m", "x"],
                timeout_s=5,
                smoke_db_path="C:/tmp/preflight-smoke.db",
            )

        self.assertEqual(rc, 0)
        self.assertEqual(out, "ok")
        self.assertEqual(captured_env["DB_PATH"], "C:/tmp/preflight-smoke.db")
        self.assertEqual(captured_env["PREFLIGHT_SMOKE_DB_PATH"], "C:/tmp/preflight-smoke.db")

    def test_warning_only_preflight_result_is_not_marked_production_ready(self) -> None:
        prod_preflight = _reload_module()
        stdout = io.StringIO()

        with patch.dict(os.environ, {"PREFLIGHT_ISOLATE_SMOKE_DB": "0"}):
            with patch.object(sys, "argv", ["prod_preflight.py", "--json"]):
                with patch.object(sys, "stdout", stdout):
                    with patch.object(prod_preflight, "_runtime_config_gate", return_value=(["runtime config ok"], [])):
                        with patch.object(prod_preflight, "_api_mutation_auth_gate", return_value=(["api mutation auth ok"], [])):
                            with patch.object(prod_preflight, "_compile_files", return_value=[]):
                                with patch.object(prod_preflight, "_ensure_schemas", return_value=["core db ok"]):
                                    with patch.object(
                                        prod_preflight,
                                        "_verify_sqlite_contract",
                                        return_value=(["sqlite contract ok"], [], {"ok": True}),
                                    ):
                                        with patch.object(
                                            prod_preflight,
                                            "_check_external_services",
                                            return_value=([], [], [], []),
                                        ):
                                            with patch.object(
                                                prod_preflight,
                                                "SMOKE_CMDS",
                                                [("engine.strategy.jobs.train_size_policy", ["python", "-m", "x"])],
                                            ):
                                                with patch.object(
                                                    prod_preflight,
                                                    "_run_cmd",
                                                    return_value=(1, "[size_policy] not enough samples: 0 < 200"),
                                                ):
                                                    with patch.object(
                                                        prod_preflight,
                                                        "_exec_cost_gate_sanity",
                                                        return_value=([], []),
                                                    ):
                                                        with patch.object(
                                                            prod_preflight,
                                                            "_capital_reconciliation_sanity",
                                                            return_value=([], [], []),
                                                        ):
                                                            rc = prod_preflight.main()

        payload = json.loads(stdout.getvalue())
        self.assertEqual(rc, 2)
        self.assertFalse(bool(payload.get("ok")))
        self.assertEqual(payload.get("status"), "warning")
        self.assertFalse(bool(payload.get("production_ready")))
        self.assertTrue(payload.get("warnings"))

    def test_zero_exit_smoke_with_sqlite_lock_traceback_is_hard_failure(self) -> None:
        prod_preflight = _reload_module()

        classification = prod_preflight._classify_smoke_result(
            "engine.execution.broker_sim",
            0,
            "Traceback...\nsqlite3.OperationalError: database is locked\n",
        )

        self.assertEqual(
            classification,
            (
                "error",
                "smoke failed: engine.execution.broker_sim output matched sqlite3.operationalerror: database is locked",
            ),
        )

    def test_zero_exit_smoke_with_deferred_lock_contention_is_hard_failure(self) -> None:
        prod_preflight = _reload_module()

        classification = prod_preflight._classify_smoke_result(
            "engine.execution.broker_sim",
            0,
            '{"storage_status":"best_effort_deferred_lock_contention"}',
        )

        self.assertEqual(
            classification,
            (
                "error",
                "smoke failed: engine.execution.broker_sim output matched best_effort_deferred_lock_contention",
            ),
        )

    def test_api_mutation_auth_gate_fails_closed_in_prod_without_token(self) -> None:
        prod_preflight = _reload_module()

        with patch.dict(
            os.environ,
            {"ENV": "prod", "ENGINE_MODE": "safe", "EXECUTION_MODE": "safe"},
            clear=True,
        ):
            notes, errors = prod_preflight._api_mutation_auth_gate()

        self.assertEqual(notes, [])
        self.assertTrue(errors)
        self.assertIn("DASHBOARD_API_TOKEN must be set", errors[0])

    def test_backup_restore_evidence_gate_fails_closed_when_required(self) -> None:
        prod_preflight = _reload_module()

        with patch.dict(os.environ, {"ENGINE_MODE": "live"}, clear=False):
            with patch(
                "engine.runtime.backup_evidence.backup_restore_evidence_snapshot",
                return_value={
                    "ok": False,
                    "fresh": False,
                    "required": True,
                    "reason": "backup_evidence_wal_archive_stale",
                    "blockers": ["backup_evidence_wal_archive_stale"],
                    "policy": {"wal_archive_max_age_s": 120},
                    "base_backup": {},
                    "wal_archive": {},
                    "restore_drill": {},
                },
            ):
                notes, warnings, errors, state = prod_preflight._backup_restore_evidence_gate()

        self.assertEqual(notes, [])
        self.assertEqual(warnings, [])
        self.assertEqual(state["reason"], "backup_evidence_wal_archive_stale")
        self.assertEqual(errors, ["backup restore evidence invalid: backup_evidence_wal_archive_stale"])


if __name__ == "__main__":
    unittest.main()
