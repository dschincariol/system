from __future__ import annotations

import importlib
import io
import json
import os
import subprocess
import sys
import tempfile
import time
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
                                with patch.object(
                                    prod_preflight,
                                    "_operator_sidecar_security_gate",
                                    return_value=(["operator sidecar ok"], [], [], {"ok": True}),
                                ):
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

    def test_operator_sidecar_security_gate_blocks_weak_token_in_prod(self) -> None:
        prod_preflight = _reload_module()

        with patch.dict(
            os.environ,
            {
                "ENV": "prod",
                "ENGINE_MODE": "safe",
                "EXECUTION_MODE": "safe",
                "OPERATOR_API_TOKEN": "short",
                "PREFLIGHT_CHECK_OPERATOR_SIDECAR_HTTP": "0",
            },
            clear=True,
        ):
            notes, warnings, errors, state = prod_preflight._operator_sidecar_security_gate()

        self.assertEqual(notes, ["operator sidecar compose exposure ok expose=1"])
        self.assertEqual(warnings, [])
        self.assertTrue(errors)
        self.assertIn("weak_operator_api_token", errors[0])
        self.assertEqual(state["operator_api_token_issue"], "weak_operator_api_token")

    def test_operator_sidecar_security_gate_blocks_public_bind_and_port(self) -> None:
        prod_preflight = _reload_module()

        with patch.dict(
            os.environ,
            {
                "ENV": "prod",
                "ENGINE_MODE": "safe",
                "EXECUTION_MODE": "safe",
                "OPERATOR_API_TOKEN": "operator-token-1234567890",
                "OPERATOR_BIND_HOST": "0.0.0.0",
                "OPERATOR_PUBLIC_PORT": "4001",
                "PREFLIGHT_CHECK_OPERATOR_SIDECAR_HTTP": "0",
            },
            clear=True,
        ):
            _notes, _warnings, errors, state = prod_preflight._operator_sidecar_security_gate()

        rendered = "\n".join(errors)
        self.assertIn("operator_bind_host_public_without_internal_only", rendered)
        self.assertIn("operator_sidecar_public_port_forbidden", rendered)
        self.assertFalse(state["ok"])

    def test_operator_sidecar_security_gate_detects_unauthenticated_get(self) -> None:
        prod_preflight = _reload_module()

        with patch.dict(
            os.environ,
            {
                "ENV": "prod",
                "ENGINE_MODE": "safe",
                "EXECUTION_MODE": "safe",
                "OPERATOR_API_TOKEN": "operator-token-1234567890",
                "PREFLIGHT_CHECK_OPERATOR_SIDECAR_HTTP": "1",
            },
            clear=True,
        ), patch.object(
            prod_preflight,
            "_operator_sensitive_get_denied",
            return_value=(False, 200, "sensitive_get_allowed_without_operator_token"),
        ):
            _notes, _warnings, errors, state = prod_preflight._operator_sidecar_security_gate()

        self.assertTrue(errors)
        self.assertIn("sensitive GET is not fail-closed", errors[-1])
        self.assertEqual(state["unauthenticated_sensitive_get"]["status"], 200)

    def test_runtime_config_gate_reports_promotion_observation_governance(self) -> None:
        prod_preflight = _reload_module()

        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch.dict(
                os.environ,
                {
                    "ENV": "dev",
                    "ENGINE_MODE": "safe",
                    "EXECUTION_MODE": "safe",
                    "DB_PATH": str(Path(tmp_dir) / "preflight.db"),
                    "CHAMPION_PROMOTION_MIN_OBSERVATIONS": "7",
                    "CHAMPION_PROMOTION_USE_STAT_GATE": "0",
                    "CPCV_ENABLED": "0",
                },
                clear=True,
            ), patch(
                "engine.runtime.live_trading_preflight.live_trading_preflight",
                return_value={"required": False, "ok": True, "blockers": []},
            ):
                notes, errors = prod_preflight._runtime_config_gate()

        self.assertEqual(errors, [])
        self.assertIn(
            "promotion observation governance ok min_observations=7 non_bypassable=1 legacy_stat_gate=0 cpcv=0",
            notes,
        )

    def test_disk_pressure_gate_warns_before_critical_pressure(self) -> None:
        prod_preflight = _reload_module()

        with patch(
            "engine.runtime.health.get_disk_pressure_snapshot",
            return_value={
                "ok": True,
                "status": "warning",
                "warnings": ["root:disk_warning:free_bytes=100:free_pct=7.5"],
                "critical": [],
                "paths": [{"label": "root", "free_bytes": 100, "free_pct": 7.5}],
            },
        ):
            notes, warnings, errors, state = prod_preflight._disk_pressure_gate()

        self.assertEqual(errors, [])
        self.assertEqual(warnings, ["disk pressure warning: root:disk_warning:free_bytes=100:free_pct=7.5"])
        self.assertEqual(state["status"], "warning")
        self.assertTrue(notes[0].startswith("disk pressure status=warning"))

    def test_disk_pressure_gate_fails_on_critical_pressure(self) -> None:
        prod_preflight = _reload_module()

        with patch(
            "engine.runtime.health.get_disk_pressure_snapshot",
            return_value={
                "ok": False,
                "status": "critical",
                "warnings": [],
                "critical": ["backup_root:disk_critical:free_bytes=10:free_pct=0.1"],
                "paths": [{"label": "root", "free_bytes": 10, "free_pct": 0.1}],
            },
        ):
            _notes, warnings, errors, state = prod_preflight._disk_pressure_gate()

        self.assertEqual(warnings, [])
        self.assertEqual(errors, ["disk pressure critical: backup_root:disk_critical:free_bytes=10:free_pct=0.1"])
        self.assertEqual(state["status"], "critical")

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

    def test_backup_restore_evidence_gate_blocks_unsigned_required_evidence(self) -> None:
        prod_preflight = _reload_module()

        with tempfile.TemporaryDirectory() as td:
            evidence_path = Path(td) / "latest_backup_restore_evidence.json"
            now = time.time()
            evidence_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "generated_at_ts": now,
                        "status": "pass",
                        "base_backup": {"status": "pass", "verified_at_ts": now},
                        "wal_archive": {"status": "pass", "verified_at_ts": now},
                        "restore_drill": {
                            "status": "pass",
                            "verified_at_ts": now,
                            "time_to_recover_s": 45,
                        },
                    }
                ),
                encoding="utf-8",
            )

            with patch.dict(
                os.environ,
                {
                    "ENGINE_MODE": "live",
                    "BACKUP_EVIDENCE_PATH": str(evidence_path),
                    "BACKUP_EVIDENCE_REQUIRE_SIGNATURE": "1",
                    "BACKUP_EVIDENCE_BASE_BACKUP_MAX_AGE_S": "3600",
                    "BACKUP_EVIDENCE_RPO_S": "3600",
                    "BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S": "3600",
                    "BACKUP_EVIDENCE_RTO_S": "300",
                },
                clear=False,
            ):
                for key in (
                    "BACKUP_EVIDENCE_HMAC_KEY",
                    "BACKUP_EVIDENCE_SIGNING_KEY",
                    "BACKUP_EVIDENCE_HMAC_KEY_FILE",
                    "BACKUP_EVIDENCE_SIGNING_KEY_FILE",
                ):
                    os.environ.pop(key, None)
                notes, warnings, errors, state = prod_preflight._backup_restore_evidence_gate()

        self.assertEqual(notes, [])
        self.assertEqual(warnings, [])
        self.assertIn("backup_evidence_unsigned", state["blockers"])
        self.assertEqual(errors, ["backup restore evidence invalid: backup_evidence_unsigned"])


if __name__ == "__main__":
    unittest.main()
