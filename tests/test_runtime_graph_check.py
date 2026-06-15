from __future__ import annotations

import io
import os
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from tools.runtime_graph_check import (
    _collect_entrypoint_imports,
    _embedded_startup_validation_error,
    _run_cold_boot_db_bootstrap_check,
    _run_timeseries_sidecar_startup_check,
    _validate_supervisor_graph,
    bootstrap_validation_env,
    run_canonical_validation,
)


class RuntimeGraphCheckTests(unittest.TestCase):
    def test_bootstrap_validation_env_forces_read_only_entrypoint_imports(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "DATA_SOURCE_MANAGER_READ_ONLY": "",
                "ENGINE_PRIMARY_BOOTSTRAP_DONE": "",
                "AUTO_BOOT_DAEMONS": "",
                "TRADING_VALIDATION_IMPORT_DASHBOARD": "",
                "TRADING_VALIDATION_IMPORT_HEAVY_ENTRYPOINTS": "",
            },
            clear=False,
        ):
            env = bootstrap_validation_env()

        self.assertEqual(str(env.get("DATA_SOURCE_MANAGER_READ_ONLY") or ""), "1")
        self.assertEqual(str(env.get("ENGINE_PRIMARY_BOOTSTRAP_DONE") or ""), "1")
        self.assertEqual(str(env.get("AUTO_BOOT_DAEMONS") or ""), "0")
        self.assertEqual(str(env.get("TRADING_VALIDATION_IMPORT_DASHBOARD") or ""), "0")
        self.assertEqual(str(env.get("TRADING_VALIDATION_IMPORT_HEAVY_ENTRYPOINTS") or ""), "0")

    def test_entrypoint_imports_skip_dashboard_by_default(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "TRADING_VALIDATION_IMPORT_DASHBOARD": "0",
                "TRADING_VALIDATION_IMPORT_HEAVY_ENTRYPOINTS": "0",
            },
            clear=False,
        ):
            modules = _collect_entrypoint_imports()
        self.assertNotIn("dashboard_server", modules)
        self.assertNotIn("start_ingestion", modules)
        self.assertNotIn("start_system", modules)
        self.assertNotIn("engine.app", modules)

    def test_startup_bootstrap_uses_sqlite_backend_by_default(self) -> None:
        with patch.dict(os.environ, {"TRADING_VALIDATION_MODE": "startup"}, clear=True):
            env = bootstrap_validation_env()

        self.assertEqual(str(env.get("TS_STORAGE_BACKEND") or ""), "sqlite")

    def test_startup_bootstrap_preserves_production_backend_requirement(self) -> None:
        with patch.dict(
            os.environ,
            {
                "TRADING_VALIDATION_MODE": "startup",
                "TS_STORAGE_BACKEND": "",
                "TS_ENV": "production",
            },
            clear=True,
        ):
            env = bootstrap_validation_env()

        self.assertEqual(str(env.get("TS_STORAGE_BACKEND") or ""), "")

    def test_embedded_startup_validation_detects_blocking_checks(self) -> None:
        message = _embedded_startup_validation_error(
            {
                "ok": True,
                "startup_validation": {
                    "ok": False,
                    "blocking_checks": ["model_cache_ready", "ingestion_active"],
                    "reasons": ["model cache cold"],
                },
            }
        )

        self.assertEqual(
            message,
            "startup_validation_failed:blocking_checks=model_cache_ready,ingestion_active",
        )

    def test_embedded_startup_validation_accepts_blocking_gates_alias(self) -> None:
        message = _embedded_startup_validation_error(
            {
                "ok": True,
                "startup_validation": {
                    "ok": False,
                    "blocking_gates": ["config_valid", "schema_valid"],
                },
            }
        )

        self.assertEqual(
            message,
            "startup_validation_failed:blocking_checks=config_valid,schema_valid",
        )

    def test_embedded_startup_validation_ignores_healthy_payload(self) -> None:
        self.assertIsNone(
            _embedded_startup_validation_error(
                {
                    "ok": True,
                    "startup_validation": {
                        "ok": True,
                        "blocking_checks": [],
                    },
                }
            )
        )

    def test_validate_supervisor_graph_uses_local_supervisor_without_delegate(self) -> None:
        seen = {}

        class _FakeSupervisor:
            def __init__(self, *args, **kwargs) -> None:
                seen["args"] = args
                seen["kwargs"] = kwargs

            def validate_graph(self, *, strict: bool = True):
                seen["strict"] = strict
                return {"ok": True, "errors": []}

        with patch("engine.runtime.supervisor.RuntimeSupervisor", _FakeSupervisor):
            result = _validate_supervisor_graph()

        self.assertEqual(seen.get("args"), ())
        self.assertEqual(seen.get("kwargs"), {})
        self.assertTrue(bool(seen.get("strict")))
        self.assertTrue(bool(result.get("ok")))

    def test_cold_boot_db_bootstrap_check_runs_repair_subprocess_with_temp_db(self) -> None:
        completed = subprocess.CompletedProcess(
            args=["python", "-c", "print('ok')"],
            returncode=0,
            stdout="{'ok': True}",
            stderr="",
        )
        with patch("tools.runtime_graph_check.subprocess.run", return_value=completed) as mock_run:
            result = _run_cold_boot_db_bootstrap_check(timeout_s=12.5)

        self.assertTrue(bool(result.get("ok")))
        kwargs = mock_run.call_args.kwargs
        self.assertEqual(kwargs.get("cwd"), str(REPO_ROOT))
        self.assertEqual(float(kwargs.get("timeout") or 0.0), 12.5)
        env = dict(kwargs.get("env") or {})
        self.assertEqual(str(env.get("ENGINE_SUPERVISED") or ""), "1")
        self.assertEqual(str(env.get("AUTO_BOOT_DAEMONS") or ""), "0")
        self.assertEqual(str(env.get("SQLITE_TRACE_REPORT_EVERY_S") or ""), "0")
        self.assertEqual(str(env.get("TS_PG_SCHEMA_PER_DB_PATH") or ""), "1")
        self.assertTrue(str(env.get("DB_PATH") or "").endswith("cold_boot_validation.db"))

    def test_cold_boot_db_bootstrap_check_surfaces_timeouts(self) -> None:
        with patch(
            "tools.runtime_graph_check.subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd=["python"], timeout=7),
        ):
            result = _run_cold_boot_db_bootstrap_check(timeout_s=7)

        self.assertFalse(bool(result.get("ok")))
        self.assertEqual(str(result.get("error") or ""), "timeout")
        self.assertEqual(float(result.get("timeout_s") or 0.0), 7.0)

    def test_timeseries_sidecar_startup_check_runs_storage_subprocess_with_temp_db(self) -> None:
        completed = subprocess.CompletedProcess(
            args=["python", "-c", "print('ok')"],
            returncode=0,
            stdout='{"ok": true, "enabled": false}',
            stderr="",
        )
        with patch("tools.runtime_graph_check.subprocess.run", return_value=completed) as mock_run:
            result = _run_timeseries_sidecar_startup_check(timeout_s=13.5)

        self.assertTrue(bool(result.get("ok")))
        kwargs = mock_run.call_args.kwargs
        self.assertEqual(kwargs.get("cwd"), str(REPO_ROOT))
        self.assertEqual(float(kwargs.get("timeout") or 0.0), 13.5)
        env = dict(kwargs.get("env") or {})
        self.assertEqual(str(env.get("ENGINE_SUPERVISED") or ""), "1")
        self.assertEqual(str(env.get("AUTO_BOOT_DAEMONS") or ""), "0")
        self.assertEqual(str(env.get("SQLITE_TRACE_REPORT_EVERY_S") or ""), "0")
        self.assertEqual(str(env.get("TS_PG_SCHEMA_PER_DB_PATH") or ""), "1")
        self.assertTrue(str(env.get("DB_PATH") or "").endswith("timeseries_validation.db"))
        cmd = list(mock_run.call_args.args[0] or [])
        self.assertIn("init_timeseries_storage", str(cmd[2]))
        self.assertIn("ensure_schema", str(cmd[2]))
        self.assertIn("get_timeseries_storage_snapshot", str(cmd[2]))
        self.assertIn("RUNTIME_GRAPH_TIMESERIES_SCHEMA_TIMEOUT_S", str(cmd[2]))

    def test_startup_runtime_graph_accepts_registered_job_entrypoints(self) -> None:
        with (
            patch("tools.runtime_graph_check._collect_entrypoint_imports", return_value=[]),
            patch("tools.runtime_graph_check._run_cold_boot_db_bootstrap_check", return_value={"ok": True}),
            patch("tools.runtime_graph_check._run_timeseries_sidecar_startup_check", return_value={"ok": True}),
            patch("sys.stdout", new_callable=io.StringIO),
        ):
            exit_code = run_canonical_validation(mode="startup")

        self.assertEqual(exit_code, 0)


if __name__ == "__main__":
    unittest.main()
