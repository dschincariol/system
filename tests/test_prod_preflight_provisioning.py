from __future__ import annotations

import importlib
import io
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


def _reload_module():
    import engine.runtime.prod_preflight as prod_preflight

    return importlib.reload(prod_preflight)


def _compose_postgres_tuning_env() -> dict[str, str]:
    return {
        "PREFLIGHT_REQUIRE_DOCKER_POSTGRES_TUNING": "1",
        "TRADING_RESOURCE_HOST_MEMORY": "123g",
        "TRADING_RESOURCE_MIN_HEADROOM_MEMORY": "24g",
        "TIMESCALE_CPUS": "8",
        "TIMESCALE_MEM_LIMIT": "32g",
        "TIMESCALE_MAX_CONNECTIONS": "100",
        "TIMESCALE_SHARED_BUFFERS": "8GB",
        "TIMESCALE_EFFECTIVE_CACHE_SIZE": "22GB",
        "TIMESCALE_WORK_MEM": "48MB",
        "TIMESCALE_WORK_MEM_ACTIVE_CONNECTIONS": "64",
        "TIMESCALE_WORK_MEM_NODE_FACTOR": "2",
        "TIMESCALE_MAINTENANCE_WORK_MEM": "2GB",
        "TIMESCALE_AUTOVACUUM_WORK_MEM": "512MB",
        "TIMESCALE_WAL_BUFFERS": "64MB",
        "TIMESCALE_MIN_WAL_SIZE": "4GB",
        "TIMESCALE_MAX_WAL_SIZE": "16GB",
        "TIMESCALE_WAL_KEEP_SIZE": "1GB",
        "TIMESCALE_MAX_SLOT_WAL_KEEP_SIZE": "8GB",
        "TIMESCALE_WAL_DISK_BUDGET": "40g",
        "TIMESCALE_ARCHIVE_MODE": "on",
        "TIMESCALE_ARCHIVE_COMMAND": '/opt/trading/ops/backup/wal_archive.sh "%p" "%f"',
        "TIMESCALE_ARCHIVE_TIMEOUT": "60s",
        "TIMESCALE_CHECKPOINT_TIMEOUT": "15min",
        "TIMESCALE_CHECKPOINT_COMPLETION_TARGET": "0.9",
        "TIMESCALE_MAX_WORKER_PROCESSES": "16",
        "TIMESCALE_MAX_PARALLEL_WORKERS": "8",
        "TIMESCALE_MAX_PARALLEL_WORKERS_PER_GATHER": "4",
        "TIMESCALE_MAX_PARALLEL_MAINTENANCE_WORKERS": "4",
        "TIMESCALE_TIMESCALEDB_MAX_BACKGROUND_WORKERS": "8",
        "TIMESCALE_AUTOVACUUM": "on",
        "TIMESCALE_AUTOVACUUM_MAX_WORKERS": "4",
        "TIMESCALE_AUTOVACUUM_NAPTIME": "10s",
        "TIMESCALE_AUTOVACUUM_VACUUM_COST_LIMIT": "4000",
        "TIMESCALE_AUTOVACUUM_VACUUM_COST_DELAY": "2ms",
        "TIMESCALE_RANDOM_PAGE_COST": "1.1",
        "TIMESCALE_EFFECTIVE_IO_CONCURRENCY": "200",
        "TIMESCALE_MAINTENANCE_IO_CONCURRENCY": "200",
    }


class ProdPreflightProvisioningTests(unittest.TestCase):
    def test_systemd_prod_preflight_unit_declares_production_contract(self) -> None:
        unit = (REPO_ROOT / "ops/server/systemd/trading-prod-preflight.service").read_text(encoding="utf-8")

        self.assertIn("Type=oneshot", unit)
        self.assertIn("User=trading", unit)
        self.assertIn("Group=trading", unit)
        self.assertIn("EnvironmentFile=-/etc/trading/trading.env", unit)
        self.assertNotIn("EnvironmentFile=-/etc/trading/provider.env", unit)
        self.assertIn("Environment=TS_SECRETS_PROVIDER=systemd-creds", unit)
        self.assertIn("Environment=DASHBOARD_API_TOKEN_SECRET=dashboard_api_token", unit)
        self.assertIn("Environment=OPERATOR_API_TOKEN_SECRET=operator_api_token", unit)
        self.assertIn("Environment=OBJECT_STORE_ACCESS_KEY_SECRET=object_store_access_key", unit)
        self.assertIn("Environment=OBJECT_STORE_SECRET_KEY_SECRET=object_store_secret_key", unit)
        self.assertIn("Environment=BACKUP_EVIDENCE_HMAC_KEY_SECRET=backup_evidence_hmac_key", unit)
        self.assertIn("Environment=PREFLIGHT_REQUIRE_CPU_POWER_POLICY=1", unit)
        self.assertIn("Environment=PREFLIGHT_REQUIRE_MEMORY_PRESSURE_POLICY=1", unit)
        self.assertIn(
            "LoadCredentialEncrypted=pg_password_app:/etc/credstore.encrypted/pg_password_app.cred",
            unit,
        )
        self.assertIn(
            "LoadCredentialEncrypted=redis_password:/etc/credstore.encrypted/redis_password.cred",
            unit,
        )
        self.assertIn(
            "LoadCredentialEncrypted=object_store_access_key:/etc/credstore.encrypted/object_store_access_key.cred",
            unit,
        )
        self.assertIn(
            "LoadCredentialEncrypted=object_store_secret_key:/etc/credstore.encrypted/object_store_secret_key.cred",
            unit,
        )
        self.assertIn(
            "LoadCredentialEncrypted=dashboard_api_token:/etc/credstore.encrypted/dashboard_api_token.cred",
            unit,
        )
        self.assertIn(
            "LoadCredentialEncrypted=operator_api_token:/etc/credstore.encrypted/operator_api_token.cred",
            unit,
        )
        self.assertIn(
            "LoadCredentialEncrypted=backup_evidence_hmac_key:/etc/credstore.encrypted/backup_evidence_hmac_key.cred",
            unit,
        )
        self.assertIn("UnsetEnvironment=OBJECT_STORE_ACCESS_KEY OBJECT_STORE_ACCESS_KEY_FILE", unit)
        self.assertIn("ExecStart=/opt/trading/venv/bin/python engine/runtime/prod_preflight.py --json", unit)
        self.assertIn("ReadWritePaths=/var/lib/trading /var/backups/trading", unit)
        self.assertIn("ProtectSystem=strict", unit)

    def test_server_provisioning_registers_prod_preflight_paths(self) -> None:
        bootstrap = (REPO_ROOT / "ops/server/bootstrap.sh").read_text(encoding="utf-8")
        verify = (REPO_ROOT / "ops/server/verify.sh").read_text(encoding="utf-8")
        runner = (REPO_ROOT / "ops/server/run_prod_preflight.sh").read_text(encoding="utf-8")

        self.assertIn("trading-prod-preflight.service", bootstrap)
        self.assertIn("trading-prod-preflight.service", verify)
        self.assertIn("PREFLIGHT_REQUIRE_MEMORY_PRESSURE_POLICY=1", bootstrap)
        self.assertIn("memory_pressure_hardening.sh", bootstrap)
        self.assertIn("memory_pressure_hardening.sh", verify)
        self.assertIn("check_memory_pressure_assets", verify)
        self.assertIn("check_prod_preflight_runner", verify)
        self.assertIn("object_store_access_key", verify)
        self.assertIn(
            "redis_password object_store_access_key object_store_secret_key dashboard_api_token operator_api_token backup_evidence_hmac_key",
            (REPO_ROOT / "ops/server/credstore/install.sh").read_text(encoding="utf-8"),
        )
        self.assertIn("--property=\"LoadCredentialEncrypted=pg_password_app:${CREDSTORE_DIR}/pg_password_app.cred\"", runner)
        self.assertIn("--property=\"LoadCredentialEncrypted=redis_password:${CREDSTORE_DIR}/redis_password.cred\"", runner)
        self.assertIn(
            "--property=\"LoadCredentialEncrypted=object_store_access_key:${CREDSTORE_DIR}/object_store_access_key.cred\"",
            runner,
        )
        self.assertIn(
            "--property=\"LoadCredentialEncrypted=object_store_secret_key:${CREDSTORE_DIR}/object_store_secret_key.cred\"",
            runner,
        )
        self.assertIn(
            "--property=\"LoadCredentialEncrypted=dashboard_api_token:${CREDSTORE_DIR}/dashboard_api_token.cred\"",
            runner,
        )
        self.assertIn(
            "--property=\"LoadCredentialEncrypted=operator_api_token:${CREDSTORE_DIR}/operator_api_token.cred\"",
            runner,
        )
        self.assertIn(
            "--property=\"LoadCredentialEncrypted=backup_evidence_hmac_key:${CREDSTORE_DIR}/backup_evidence_hmac_key.cred\"",
            runner,
        )
        self.assertIn("DASHBOARD_API_TOKEN_SECRET=dashboard_api_token", runner)
        self.assertIn("OPERATOR_API_TOKEN_SECRET=operator_api_token", runner)
        self.assertIn("OBJECT_STORE_ACCESS_KEY_SECRET=object_store_access_key", runner)
        self.assertIn("OBJECT_STORE_SECRET_KEY_SECRET=object_store_secret_key", runner)
        self.assertIn("unset OBJECT_STORE_ACCESS_KEY OBJECT_STORE_ACCESS_KEY_FILE", runner)
        self.assertIn("BACKUP_EVIDENCE_HMAC_KEY_SECRET=backup_evidence_hmac_key", runner)
        self.assertIn("TS_SECRETS_PROVIDER=systemd-creds", runner)
        self.assertNotIn('if [ -r "$PROVIDER_ENV" ]', runner)
        self.assertIn("engine/runtime/prod_preflight.py --json", runner)

    def test_relative_runtime_data_root_fails_closed(self) -> None:
        with patch.dict(
            os.environ,
            {
                "TS_PG_DSN": "host=timescaledb port=5432 user=trading dbname=trading password=inline-test",
                "DB_PATH": "data/runtime",
            },
            clear=True,
        ):
            prod_preflight = _reload_module()
            notes, errors, snapshot = prod_preflight._production_provisioning_gate()

        self.assertEqual(notes, [])
        self.assertTrue(any("runtime data root must be absolute: data/runtime" in error for error in errors), errors)
        self.assertEqual(snapshot["data_root"]["path"], "data/runtime")

    def test_missing_systemd_credential_directory_fails_actionably(self) -> None:
        data_root = Path.cwd()
        with patch.dict(
            os.environ,
            {
                "TS_SECRETS_PROVIDER": "systemd-creds",
                "DB_PATH": str(data_root),
                "TS_CREDENTIAL_AUDIT_ENABLED": "0",
            },
            clear=True,
        ):
            prod_preflight = _reload_module()
            notes, errors, snapshot = prod_preflight._production_provisioning_gate()

        self.assertEqual(notes, [])
        rendered = "\n".join(errors)
        self.assertIn("CREDENTIALS_DIRECTORY is unset", rendered)
        self.assertIn("LoadCredentialEncrypted=", rendered)
        self.assertEqual(snapshot["credentials"]["required_names"], ["pg_password_app"])

    def test_bad_runtime_data_root_permissions_fail_clearly(self) -> None:
        data_root = Path.cwd()
        cred_dir = data_root / "prod_preflight_test_creds"
        cred_dir.mkdir(exist_ok=True)
        try:
            (cred_dir / "pg_password_app").write_text("test-password", encoding="utf-8")
            with patch.dict(
                os.environ,
                {
                    "TS_SECRETS_PROVIDER": "systemd-creds",
                    "CREDENTIALS_DIRECTORY": str(cred_dir),
                    "DB_PATH": str(data_root),
                    "TS_CREDENTIAL_AUDIT_ENABLED": "0",
                },
                clear=True,
            ):
                prod_preflight = _reload_module()
                real_access = prod_preflight.os.access

                def fake_access(path, mode):
                    if Path(path) == data_root and int(mode) == prod_preflight.os.W_OK:
                        return False
                    return real_access(path, mode)

                with patch.object(prod_preflight.os, "access", side_effect=fake_access):
                    notes, errors, snapshot = prod_preflight._production_provisioning_gate()

            self.assertEqual(notes, [])
            self.assertTrue(any("runtime data root not writable" in error for error in errors), errors)
            self.assertFalse(snapshot["data_root"]["writable"])
        finally:
            try:
                (cred_dir / "pg_password_app").unlink()
                cred_dir.rmdir()
            except OSError:
                pass

    def test_valid_systemd_credential_and_data_root_passes_static_gate(self) -> None:
        data_root = Path.cwd()
        cred_dir = data_root / "prod_preflight_valid_creds"
        cred_dir.mkdir(exist_ok=True)
        try:
            (cred_dir / "pg_password_app").write_text("test-password", encoding="utf-8")
            with patch.dict(
                os.environ,
                {
                    "TS_SECRETS_PROVIDER": "systemd-creds",
                    "CREDENTIALS_DIRECTORY": str(cred_dir),
                    "DB_PATH": str(data_root),
                    "TS_CREDENTIAL_AUDIT_ENABLED": "0",
                },
                clear=True,
            ):
                prod_preflight = _reload_module()
                notes, errors, snapshot = prod_preflight._production_provisioning_gate()

            self.assertEqual(errors, [])
            self.assertIn("credential source ok provider=systemd-creds names=pg_password_app", notes)
            self.assertTrue(any(note.startswith("runtime data root ok") for note in notes))
            self.assertEqual(snapshot["credentials"]["provider"], "systemd-creds")
        finally:
            try:
                (cred_dir / "pg_password_app").unlink()
                cred_dir.rmdir()
            except OSError:
                pass

    def test_signed_backup_evidence_requires_systemd_hmac_credential(self) -> None:
        data_root = Path.cwd()
        cred_dir = data_root / "prod_preflight_backup_evidence_creds"
        cred_dir.mkdir(exist_ok=True)
        try:
            (cred_dir / "pg_password_app").write_text("test-password", encoding="utf-8")
            (cred_dir / "backup_evidence_hmac_key").write_text("test-hmac-key", encoding="utf-8")
            with patch.dict(
                os.environ,
                {
                    "TS_SECRETS_PROVIDER": "systemd-creds",
                    "CREDENTIALS_DIRECTORY": str(cred_dir),
                    "DB_PATH": str(data_root),
                    "BACKUP_EVIDENCE_REQUIRE_SIGNATURE": "1",
                    "BACKUP_EVIDENCE_HMAC_KEY_SECRET": "backup_evidence_hmac_key",
                    "TS_CREDENTIAL_AUDIT_ENABLED": "0",
                },
                clear=True,
            ):
                prod_preflight = _reload_module()
                notes, errors, snapshot = prod_preflight._production_provisioning_gate()

            self.assertEqual(errors, [])
            self.assertIn("credential source ok provider=systemd-creds names=pg_password_app,backup_evidence_hmac_key", notes)
            self.assertEqual(
                snapshot["credentials"]["required_names"],
                ["pg_password_app", "backup_evidence_hmac_key"],
            )
        finally:
            for name in ("pg_password_app", "backup_evidence_hmac_key"):
                try:
                    (cred_dir / name).unlink()
                except OSError:
                    pass
            try:
                cred_dir.rmdir()
            except OSError:
                pass

    def test_inline_dsn_password_is_rejected_in_strict_preflight(self) -> None:
        with patch.dict(
            os.environ,
            {
                "TS_PG_DSN": "host=timescaledb port=5432 user=trading dbname=trading password=inline-test",
                "DB_PATH": str(Path.cwd()),
                "PROD_LOCK": "1",
                "TRADING_ENFORCE_SECRET_SOURCE_POLICY": "1",
                "TRADING_SECRET_POLICY_REPO_ROOT": str(Path.cwd() / "missing-secret-policy-test-root"),
            },
            clear=True,
        ):
            prod_preflight = _reload_module()
            notes, errors, snapshot = prod_preflight._production_provisioning_gate()

        self.assertEqual(notes, [])
        rendered = "\n".join(errors)
        self.assertIn("inline_secret_env:TS_PG_DSN", rendered)
        self.assertEqual(snapshot["credentials"]["required_names"], ["pg_password_app"])

    def test_password_file_does_not_require_secret_provider(self) -> None:
        data_root = Path.cwd()
        with tempfile.TemporaryDirectory(prefix="prod_preflight_secret_policy_") as tmp:
            tmp_path = Path(tmp)
            password_file = tmp_path / "secrets" / "timescale_password"
            password_file.parent.mkdir(parents=True, exist_ok=True)
            password_file.write_text("file-backed-password", encoding="utf-8")
            password_file.chmod(0o600)
            repo_root = tmp_path / "repo"
            repo_root.mkdir()
            with patch.dict(
                os.environ,
                {
                    "TS_PG_DSN": "host=timescaledb port=5432 user=trading dbname=trading",
                    "TS_PG_PASSWORD_FILE": str(password_file),
                    "DB_PATH": str(data_root),
                    "PROD_LOCK": "1",
                    "TRADING_ENFORCE_SECRET_SOURCE_POLICY": "1",
                    "TRADING_SECRET_POLICY_REPO_ROOT": str(repo_root),
                    "TS_CREDENTIAL_AUDIT_ENABLED": "0",
                },
                clear=True,
            ):
                prod_preflight = _reload_module()
                notes, errors, snapshot = prod_preflight._production_provisioning_gate()

        self.assertEqual(errors, [])
        self.assertIn("credential source ok provider=file_or_secret_reference", notes)
        self.assertEqual(snapshot["credentials"]["required_names"], [])
        self.assertTrue(snapshot["secret_sources"]["ok"])

    def test_compose_postgres_tuning_fits_123g_host_profile(self) -> None:
        from engine.runtime.postgres_tuning import BYTES_IN_GIB, docker_postgres_tuning_snapshot

        snapshot = docker_postgres_tuning_snapshot(_compose_postgres_tuning_env(), required=True)

        self.assertTrue(snapshot["ok"], snapshot)
        self.assertEqual(snapshot["derivation"]["memory_source"], "TIMESCALE_MEM_LIMIT")
        self.assertLessEqual(
            snapshot["memory_budget"]["estimated_peak_bytes"],
            snapshot["memory_budget"]["allowed_peak_bytes"],
        )
        self.assertEqual(snapshot["wal_budget"]["configured_retained_wal_ceiling_bytes"], 25 * BYTES_IN_GIB)

    def test_memory_pressure_gate_fails_closed_when_required(self) -> None:
        with patch.dict(os.environ, {"PREFLIGHT_REQUIRE_MEMORY_PRESSURE_POLICY": "1"}, clear=True):
            prod_preflight = _reload_module()
            with patch(
                "engine.runtime.memory_pressure.host_memory_pressure_snapshot",
                return_value={
                    "required": True,
                    "ok": False,
                    "meets_policy": False,
                    "reason": "memory_pressure_total_swap_below_policy",
                    "errors": ["memory_pressure_total_swap_below_policy"],
                    "warnings": [],
                },
            ):
                notes, warnings, errors, snapshot = prod_preflight._memory_pressure_gate()

        self.assertEqual(notes, [])
        self.assertEqual(warnings, [])
        self.assertEqual(errors, ["memory_pressure_total_swap_below_policy"])
        self.assertFalse(snapshot["ok"])

    def test_compose_postgres_tuning_rejects_insufficient_host_headroom(self) -> None:
        from engine.runtime.postgres_tuning import docker_postgres_tuning_snapshot

        env = _compose_postgres_tuning_env()
        env["TRADING_RESOURCE_HOST_MEMORY"] = "40g"
        snapshot = docker_postgres_tuning_snapshot(env, required=True)

        self.assertFalse(snapshot["ok"])
        self.assertTrue(any("exceeds host headroom" in error for error in snapshot["errors"]), snapshot)

    def test_compose_postgres_tuning_rejects_wal_budget_overrun(self) -> None:
        from engine.runtime.postgres_tuning import docker_postgres_tuning_snapshot

        env = _compose_postgres_tuning_env()
        env["TIMESCALE_MAX_WAL_SIZE"] = "32GB"
        snapshot = docker_postgres_tuning_snapshot(env, required=True)

        self.assertFalse(snapshot["ok"])
        self.assertTrue(
            any("WAL retention budget exceeds configured disk budget" in error for error in snapshot["errors"]),
            snapshot,
        )

    def test_compose_postgres_tuning_rejects_inline_archive_command(self) -> None:
        from engine.runtime.postgres_tuning import docker_postgres_tuning_snapshot

        env = _compose_postgres_tuning_env()
        env["TIMESCALE_ARCHIVE_COMMAND"] = "mkdir -p /var/backups/trading/wal && cp %p /var/backups/trading/wal/%f"
        snapshot = docker_postgres_tuning_snapshot(env, required=True)

        self.assertFalse(snapshot["ok"])
        self.assertTrue(
            any("archive_command must invoke audited wal_archive.sh" in error for error in snapshot["errors"]),
            snapshot,
        )

    def test_compose_postgres_tuning_rejects_noop_archive_command(self) -> None:
        from engine.runtime.postgres_tuning import docker_postgres_tuning_snapshot

        env = _compose_postgres_tuning_env()
        env["TIMESCALE_ARCHIVE_COMMAND"] = "/bin/true"
        snapshot = docker_postgres_tuning_snapshot(env, required=True)

        self.assertFalse(snapshot["ok"])
        self.assertTrue(
            any("archive_command must invoke audited wal_archive.sh" in error for error in snapshot["errors"]),
            snapshot,
        )

    def test_compose_postgres_tuning_rejects_effective_pg_settings_drift(self) -> None:
        from engine.runtime.postgres_tuning import PG_SETTING_SPECS, docker_postgres_tuning_snapshot

        env = _compose_postgres_tuning_env()
        effective_settings = {
            spec.pg_name: {"setting": env[spec.env], "unit": ""}
            for spec in PG_SETTING_SPECS
        }
        effective_settings["shared_buffers"] = {"setting": "4GB", "unit": ""}

        snapshot = docker_postgres_tuning_snapshot(env, required=True, effective_settings=effective_settings)

        self.assertFalse(snapshot["ok"])
        self.assertEqual(snapshot["effective_mismatches"][0]["name"], "shared_buffers")
        self.assertTrue(
            any("postgres effective setting mismatch: shared_buffers" in error for error in snapshot["errors"])
        )

    def test_prod_preflight_postgres_tuning_gate_fails_before_runtime_config(self) -> None:
        env = _compose_postgres_tuning_env()
        env["TIMESCALE_EFFECTIVE_CACHE_SIZE"] = "40GB"
        env["PREFLIGHT_POSTGRES_TUNING_QUERY_EFFECTIVE"] = "0"
        with patch.dict(os.environ, env, clear=True):
            prod_preflight = _reload_module()
            notes, warnings, errors, snapshot = prod_preflight._postgres_tuning_gate()

        self.assertEqual(notes, [])
        self.assertEqual(warnings, [])
        self.assertTrue(any("effective_cache_size exceeds service memory limit" in error for error in errors), errors)
        self.assertFalse(snapshot["ok"])

    def test_required_postgres_tuning_gate_requires_effective_pg_settings_evidence(self) -> None:
        env = _compose_postgres_tuning_env()
        with patch.dict(os.environ, env, clear=True):
            prod_preflight = _reload_module()
            with patch.object(
                prod_preflight,
                "_postgres_tuning_effective_settings",
                side_effect=RuntimeError("pg unavailable"),
            ):
                notes, warnings, errors, snapshot = prod_preflight._postgres_tuning_gate()

        self.assertEqual(notes, [])
        self.assertTrue(any("effective settings unavailable" in error for error in errors), errors)
        self.assertEqual(warnings, [])
        self.assertFalse(snapshot["ok"])
        self.assertEqual(snapshot["effective_query"]["source"], "pg_settings")

    def test_effective_runtime_state_gate_fails_when_required_evidence_missing(self) -> None:
        with patch.dict(os.environ, {"PREFLIGHT_REQUIRE_DOCKER_RUNTIME_EVIDENCE": "1"}, clear=True):
            prod_preflight = _reload_module()
            with patch(
                "engine.runtime.effective_runtime_state.effective_runtime_state_snapshot",
                return_value={
                    "ok": False,
                    "required": True,
                    "errors": ["docker_runtime_evidence_missing"],
                    "warnings": [],
                    "operator_commands": [{"command": "sudo docker inspect ...", "proves": "docker state"}],
                },
            ):
                notes, warnings, errors, snapshot = prod_preflight._effective_runtime_state_gate()

        self.assertEqual(notes, [])
        self.assertEqual(warnings, [])
        self.assertEqual(errors, ["docker_runtime_evidence_missing"])
        self.assertFalse(snapshot["ok"])

    def test_compile_files_uses_temp_cache_for_read_only_source_tree(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
            root = Path(td) / "readonly"
            root.mkdir()
            source = root / "module.py"
            source.write_text("VALUE = 1\n", encoding="utf-8")
            root.chmod(0o555)
            try:
                prod_preflight = _reload_module()
                errors = prod_preflight._compile_files([str(source)])
            finally:
                root.chmod(0o755)

        self.assertEqual(errors, [])

    def test_main_stops_before_config_when_provisioning_fails(self) -> None:
        stdout = io.StringIO()
        with patch.dict(
            os.environ,
            {
                "TS_SECRETS_PROVIDER": "systemd-creds",
                "DB_PATH": str(Path.cwd()),
                "TS_CREDENTIAL_AUDIT_ENABLED": "0",
            },
            clear=True,
        ):
            prod_preflight = _reload_module()
            with patch.object(sys, "argv", ["prod_preflight.py", "--json"]):
                with patch.object(sys, "stdout", stdout):
                    with patch.object(
                        prod_preflight,
                        "_runtime_config_gate",
                        side_effect=AssertionError("config gate should not run"),
                    ):
                        rc = prod_preflight.main()

        payload = json.loads(stdout.getvalue())
        self.assertEqual(rc, 3)
        self.assertFalse(payload["ok"])
        self.assertTrue(any("CREDENTIALS_DIRECTORY is unset" in error for error in payload["errors"]))
        self.assertEqual(payload["steps"], [])


if __name__ == "__main__":
    unittest.main()
