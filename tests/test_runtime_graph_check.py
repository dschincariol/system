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
    _VALIDATION_DATA_SOURCE_MASTER_KEY,
    _collect_entrypoint_imports,
    _embedded_startup_validation_error,
    _run_cold_boot_db_bootstrap_check,
    _run_timeseries_sidecar_startup_check,
    _validate_supervisor_graph,
    bootstrap_validation_env,
    run_canonical_validation,
)


_NO_GO_RAW_SECRET_ENV_KEYS = (
    "TS_PG_PASSWORD",
    "OBJECT_STORE_ACCESS_KEY",
    "OBJECT_STORE_SECRET_KEY",
    "TS_PG_DSN",
    "TIMESCALE_DSN",
    "TIMESCALE_PRICES_DSN",
    "OFFLINE_TS_PG_DSN",
)
_NO_GO_FILE_SECRET_ENV_KEYS = (
    "TIMESCALE_PASSWORD_FILE",
    "TS_PG_PASSWORD_FILE",
    "OBJECT_STORE_ACCESS_KEY_FILE",
    "OBJECT_STORE_SECRET_KEY_FILE",
)
_VALIDATION_SECRET_FILE_ENV_KEYS = (
    "DATA_SOURCE_MASTER_KEY_FILE",
    "DASHBOARD_API_TOKEN_FILE",
    "OPERATOR_API_TOKEN_FILE",
)


def _assert_local_validation_secret_sources(env: dict[str, str]) -> None:
    for key in _NO_GO_RAW_SECRET_ENV_KEYS + _NO_GO_FILE_SECRET_ENV_KEYS:
        assert str(env.get(key) or "") == ""
    assert str(env.get("TS_SECRETS_PROVIDER") or "") == ""
    assert str(env.get("CREDENTIALS_DIRECTORY") or "") == ""
    assert str(env.get("TS_DEV_SECRETS_DIR") or "") == ""

    policy_root = Path(str(env.get("TRADING_SECRET_POLICY_REPO_ROOT") or ""))
    assert policy_root.is_dir()
    for key in _VALIDATION_SECRET_FILE_ENV_KEYS:
        path = Path(str(env.get(key) or ""))
        assert path.is_file()
        assert path.stat().st_mode & 0o777 == 0o600


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

    def test_bootstrap_validation_env_uses_validation_master_key_for_local_startup(self) -> None:
        with patch.dict(
            os.environ,
            {
                "DATA_SOURCE_MASTER_KEY": "raw-dev-key",
                "DATA_SOURCE_MASTER_KEY_FILE": "/tmp/raw-dev-key",
                "TS_PG_PASSWORD": "raw-runtime-pg-password",
                "OBJECT_STORE_ACCESS_KEY": "raw-object-access-key",
                "OBJECT_STORE_SECRET_KEY": "raw-object-secret-key",
                "TS_PG_DSN": "host=127.0.0.1 password=raw-pg-password dbname=trading",
                "TIMESCALE_DSN": "postgresql://user:raw-timescale-password@timescale.local/trading",
                "TIMESCALE_PRICES_DSN": "postgresql://user:raw-prices-password@prices.local/trading",
                "OFFLINE_TS_PG_DSN": "postgresql://user:raw-offline-password@offline.local/trading",
                "TIMESCALE_PASSWORD_FILE": "/tmp/raw-pg-key",
                "TS_PG_PASSWORD_FILE": "/tmp/raw-runtime-pg-key",
                "OBJECT_STORE_ACCESS_KEY_FILE": "/tmp/raw-object-access",
                "OBJECT_STORE_SECRET_KEY_FILE": "/tmp/raw-object-secret",
                "TS_SECRETS_PROVIDER": "systemd",
                "ENV": "dev",
                "ENGINE_MODE": "safe",
            },
            clear=True,
        ):
            env = bootstrap_validation_env()

        self.assertEqual(str(env.get("DATA_SOURCE_MASTER_KEY") or ""), "")
        key_file = Path(env["DATA_SOURCE_MASTER_KEY_FILE"])
        self.assertEqual(key_file.read_text(encoding="utf-8"), _VALIDATION_DATA_SOURCE_MASTER_KEY)
        self.assertTrue(key_file.is_file())
        _assert_local_validation_secret_sources(env)

    def test_bootstrap_validation_env_preserves_prod_dependency_master_key(self) -> None:
        with patch.dict(
            os.environ,
            {
                "TRADING_VALIDATION_REQUIRE_PROD_DEPS": "1",
                "DATA_SOURCE_MASTER_KEY": "real-prod-key",
                "DATA_SOURCE_MASTER_KEY_FILE": "/run/secrets/data_source_master_key",
            },
            clear=True,
        ):
            env = bootstrap_validation_env()

        self.assertEqual(env["DATA_SOURCE_MASTER_KEY"], "real-prod-key")
        self.assertEqual(env["DATA_SOURCE_MASTER_KEY_FILE"], "/run/secrets/data_source_master_key")

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
        with patch.dict(
            os.environ,
            {
                "TRADING_VALIDATION_MODE": "startup",
                "TIMESCALE_ENABLED": "1",
                "TIMESCALE_DSN": "postgresql://timescale.local/trading",
                "TIMESCALE_PRICES_ENABLED": "1",
                "TIMESCALE_PRICES_DSN": "postgresql://prices.local/trading",
                "TELEMETRY_READ_BACKEND": "timescale",
                "PRICE_READ_BACKEND": "timescale",
                "TS_PG_DSN": "host=127.0.0.1 port=5432 dbname=trading",
                "LIVE_CACHE_BACKEND": "redis",
                "LIVE_CACHE_REDIS_URL": "redis://127.0.0.1:6379/0",
                "REDIS_URL": "redis://127.0.0.1:6379/0",
                "PREFLIGHT_REQUIRE_TIMESCALE": "1",
                "PREFLIGHT_REQUIRE_REDIS": "1",
                "PREFLIGHT_REQUIRE_OBJECT_STORAGE": "1",
            },
            clear=True,
        ):
            env = bootstrap_validation_env()

        self.assertEqual(str(env.get("TS_STORAGE_BACKEND") or ""), "sqlite")
        self.assertEqual(str(env.get("TIMESCALE_ENABLED") or ""), "0")
        self.assertEqual(str(env.get("TIMESCALE_PRICES_ENABLED") or ""), "0")
        self.assertEqual(str(env.get("TELEMETRY_READ_BACKEND") or ""), "sqlite")
        self.assertEqual(str(env.get("PRICE_READ_BACKEND") or ""), "sqlite")
        self.assertEqual(str(env.get("LIVE_CACHE_BACKEND") or ""), "memory")
        self.assertEqual(str(env.get("PREFLIGHT_REQUIRE_TIMESCALE") or ""), "0")
        self.assertEqual(str(env.get("PREFLIGHT_REQUIRE_REDIS") or ""), "0")
        self.assertEqual(str(env.get("PREFLIGHT_REQUIRE_OBJECT_STORAGE") or ""), "0")
        self.assertEqual(str(env.get("TIMESCALE_DSN") or ""), "")
        self.assertEqual(str(env.get("TIMESCALE_PRICES_DSN") or ""), "")
        self.assertEqual(str(env.get("TS_PG_DSN") or ""), "")
        self.assertEqual(str(env.get("LIVE_CACHE_REDIS_URL") or ""), "")
        self.assertEqual(str(env.get("REDIS_URL") or ""), "")

    def test_startup_bootstrap_preserves_production_backend_requirement(self) -> None:
        with patch.dict(
            os.environ,
            {
                "TRADING_VALIDATION_MODE": "startup",
                "TRADING_VALIDATION_REQUIRE_PROD_DEPS": "1",
                "TS_STORAGE_BACKEND": "",
                "TS_ENV": "production",
                "TIMESCALE_ENABLED": "1",
                "TIMESCALE_DSN": "postgresql://timescale.local/trading",
                "TS_PG_DSN": "host=timescaledb dbname=trading",
                "LIVE_CACHE_BACKEND": "redis",
                "REDIS_URL": "redis://redis.local:6379/0",
                "PREFLIGHT_REQUIRE_REDIS": "1",
            },
            clear=True,
        ):
            env = bootstrap_validation_env()

        self.assertEqual(str(env.get("TS_STORAGE_BACKEND") or ""), "")
        self.assertEqual(str(env.get("TIMESCALE_ENABLED") or ""), "1")
        self.assertEqual(str(env.get("TIMESCALE_DSN") or ""), "postgresql://timescale.local/trading")
        self.assertEqual(str(env.get("TS_PG_DSN") or ""), "host=timescaledb dbname=trading")
        self.assertEqual(str(env.get("LIVE_CACHE_BACKEND") or ""), "redis")
        self.assertEqual(str(env.get("REDIS_URL") or ""), "redis://redis.local:6379/0")
        self.assertEqual(str(env.get("PREFLIGHT_REQUIRE_REDIS") or ""), "1")

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

        def fake_run(*args, **kwargs):
            _assert_local_validation_secret_sources(dict(kwargs.get("env") or {}))
            return completed

        with (
            patch.dict(
                os.environ,
                {
                    "TIMESCALE_ENABLED": "1",
                    "TIMESCALE_DSN": "postgresql://timescale.local/trading",
                    "TS_PG_DSN": "host=127.0.0.1 port=5432 dbname=trading",
                    "TS_PG_PASSWORD": "raw-runtime-pg-password",
                    "OBJECT_STORE_ACCESS_KEY": "raw-object-access-key",
                    "OBJECT_STORE_SECRET_KEY": "raw-object-secret-key",
                    "OFFLINE_TS_PG_DSN": "postgresql://user:raw-offline-password@offline.local/trading",
                    "TIMESCALE_PASSWORD_FILE": "/tmp/raw-pg-key",
                    "TS_PG_PASSWORD_FILE": "/tmp/raw-runtime-pg-key",
                    "OBJECT_STORE_ACCESS_KEY_FILE": "/tmp/raw-object-access",
                    "OBJECT_STORE_SECRET_KEY_FILE": "/tmp/raw-object-secret",
                    "TS_SECRETS_PROVIDER": "systemd",
                    "LIVE_CACHE_BACKEND": "redis",
                    "REDIS_URL": "redis://127.0.0.1:6379/0",
                    "PREFLIGHT_REQUIRE_REDIS": "1",
                },
                clear=False,
            ),
            patch("tools.runtime_graph_check.subprocess.run", side_effect=fake_run) as mock_run,
        ):
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
        self.assertEqual(str(env.get("TS_STORAGE_BACKEND") or ""), "sqlite")
        self.assertEqual(str(env.get("TIMESCALE_ENABLED") or ""), "0")
        self.assertEqual(str(env.get("LIVE_CACHE_BACKEND") or ""), "memory")
        self.assertEqual(str(env.get("TIMESCALE_DSN") or ""), "")
        self.assertEqual(str(env.get("TS_PG_DSN") or ""), "")
        self.assertEqual(str(env.get("REDIS_URL") or ""), "")
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

        def fake_run(*args, **kwargs):
            _assert_local_validation_secret_sources(dict(kwargs.get("env") or {}))
            return completed

        with (
            patch.dict(
                os.environ,
                {
                    "TIMESCALE_ENABLED": "1",
                    "TIMESCALE_DSN": "postgresql://timescale.local/trading",
                    "TIMESCALE_PRICES_ENABLED": "1",
                    "TIMESCALE_PRICES_DSN": "postgresql://prices.local/trading",
                    "TELEMETRY_READ_BACKEND": "timescale",
                    "PRICE_READ_BACKEND": "timescale",
                    "TS_PG_DSN": "host=127.0.0.1 port=5432 dbname=trading",
                    "TS_PG_PASSWORD": "raw-runtime-pg-password",
                    "OBJECT_STORE_ACCESS_KEY": "raw-object-access-key",
                    "OBJECT_STORE_SECRET_KEY": "raw-object-secret-key",
                    "OFFLINE_TS_PG_DSN": "postgresql://user:raw-offline-password@offline.local/trading",
                    "TIMESCALE_PASSWORD_FILE": "/tmp/raw-pg-key",
                    "TS_PG_PASSWORD_FILE": "/tmp/raw-runtime-pg-key",
                    "OBJECT_STORE_ACCESS_KEY_FILE": "/tmp/raw-object-access",
                    "OBJECT_STORE_SECRET_KEY_FILE": "/tmp/raw-object-secret",
                    "TS_SECRETS_PROVIDER": "systemd",
                    "LIVE_CACHE_BACKEND": "redis",
                    "LIVE_CACHE_REDIS_URL": "redis://127.0.0.1:6379/0",
                    "REDIS_URL": "redis://127.0.0.1:6379/0",
                    "PREFLIGHT_REQUIRE_TIMESCALE": "1",
                    "PREFLIGHT_REQUIRE_REDIS": "1",
                },
                clear=False,
            ),
            patch("tools.runtime_graph_check.subprocess.run", side_effect=fake_run) as mock_run,
        ):
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
        self.assertEqual(str(env.get("TS_STORAGE_BACKEND") or ""), "sqlite")
        self.assertEqual(str(env.get("TIMESCALE_ENABLED") or ""), "0")
        self.assertEqual(str(env.get("TIMESCALE_PRICES_ENABLED") or ""), "0")
        self.assertEqual(str(env.get("TELEMETRY_READ_BACKEND") or ""), "sqlite")
        self.assertEqual(str(env.get("PRICE_READ_BACKEND") or ""), "sqlite")
        self.assertEqual(str(env.get("LIVE_CACHE_BACKEND") or ""), "memory")
        self.assertEqual(str(env.get("TIMESCALE_DSN") or ""), "")
        self.assertEqual(str(env.get("TIMESCALE_PRICES_DSN") or ""), "")
        self.assertEqual(str(env.get("TS_PG_DSN") or ""), "")
        self.assertEqual(str(env.get("LIVE_CACHE_REDIS_URL") or ""), "")
        self.assertEqual(str(env.get("REDIS_URL") or ""), "")
        self.assertTrue(str(env.get("DB_PATH") or "").endswith("timeseries_validation.db"))
        cmd = list(mock_run.call_args.args[0] or [])
        self.assertIn("init_timeseries_storage", str(cmd[2]))
        self.assertIn("ensure_schema", str(cmd[2]))
        self.assertIn("get_timeseries_storage_snapshot", str(cmd[2]))
        self.assertIn("RUNTIME_GRAPH_TIMESERIES_SCHEMA_TIMEOUT_S", str(cmd[2]))

    def test_timeseries_sidecar_startup_check_preserves_production_dependencies(self) -> None:
        completed = subprocess.CompletedProcess(
            args=["python", "-c", "print('ok')"],
            returncode=0,
            stdout='{"ok": true, "enabled": true}',
            stderr="",
        )
        with (
            patch.dict(
                os.environ,
                {
                    "TRADING_VALIDATION_REQUIRE_PROD_DEPS": "1",
                    "TIMESCALE_ENABLED": "1",
                    "TIMESCALE_DSN": "postgresql://timescale.local/trading",
                    "TS_PG_DSN": "host=timescaledb dbname=trading",
                    "TS_PG_PASSWORD": "real-prod-password",
                    "OBJECT_STORE_SECRET_KEY": "real-object-secret",
                    "DATA_SOURCE_MASTER_KEY_FILE": "/run/secrets/data_source_master_key",
                    "LIVE_CACHE_BACKEND": "redis",
                    "REDIS_URL": "redis://redis.local:6379/0",
                    "PREFLIGHT_REQUIRE_REDIS": "1",
                },
                clear=True,
            ),
            patch("tools.runtime_graph_check.subprocess.run", return_value=completed) as mock_run,
        ):
            result = _run_timeseries_sidecar_startup_check(timeout_s=13.5)

        self.assertTrue(bool(result.get("ok")))
        env = dict(mock_run.call_args.kwargs.get("env") or {})
        self.assertEqual(str(env.get("TRADING_VALIDATION_REQUIRE_PROD_DEPS") or ""), "1")
        self.assertEqual(str(env.get("TIMESCALE_ENABLED") or ""), "1")
        self.assertEqual(str(env.get("TIMESCALE_DSN") or ""), "postgresql://timescale.local/trading")
        self.assertEqual(str(env.get("TS_PG_DSN") or ""), "host=timescaledb dbname=trading")
        self.assertEqual(str(env.get("TS_PG_PASSWORD") or ""), "real-prod-password")
        self.assertEqual(str(env.get("OBJECT_STORE_SECRET_KEY") or ""), "real-object-secret")
        self.assertEqual(str(env.get("DATA_SOURCE_MASTER_KEY_FILE") or ""), "/run/secrets/data_source_master_key")
        self.assertEqual(str(env.get("LIVE_CACHE_BACKEND") or ""), "redis")
        self.assertEqual(str(env.get("REDIS_URL") or ""), "redis://redis.local:6379/0")
        self.assertEqual(str(env.get("PREFLIGHT_REQUIRE_REDIS") or ""), "1")
        self.assertNotEqual(str(env.get("TS_STORAGE_BACKEND") or ""), "sqlite")

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
