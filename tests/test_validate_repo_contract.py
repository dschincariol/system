from __future__ import annotations

import io
import os
import subprocess
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from tools import validate_repo


class ValidateRepoContractTests(unittest.TestCase):
    def _run_main(
        self,
        *,
        failing_label: str | None = None,
        returncode: int = 23,
        env_overrides: dict[str, str] | None = None,
        argv: list[str] | None = None,
    ) -> tuple[int, list[tuple[str, list[str], dict[str, str]]], str, Path]:
        root = Path("C:/validate-repo-root")
        calls: list[tuple[str, list[str], dict[str, str]]] = []
        output = io.StringIO()

        def fake_run(label: str, args: list[str], env: dict[str, str]) -> None:
            calls.append((label, list(args), dict(env)))
            if label == failing_label:
                raise subprocess.CalledProcessError(returncode=returncode, cmd=args)

        with (
            patch.object(validate_repo, "ROOT", root),
            patch.object(validate_repo, "_project_python", return_value="python-bin"),
            patch.object(validate_repo, "_project_pytest", return_value=["pytest-bin"]),
            patch.object(validate_repo, "_run", side_effect=fake_run),
            patch.dict(os.environ, dict(env_overrides or {}), clear=True),
            redirect_stdout(output),
        ):
            exit_code = validate_repo.main(list(argv or []))

        return exit_code, calls, output.getvalue(), root

    def test_validate_repo_fails_on_test_failure(self) -> None:
        expected_labels = {
            "pytest-collection": [
                "repo-artifact-hygiene",
                "syntax",
                "pyright-money-path",
                "ruff-static-release-gate",
                "docs",
                "ui-asset-refs",
                "dependency-lock",
                "noop-guard",
                "storage-route-audit",
                "runtime-graph-startup",
                "pytest-collection",
            ],
            "pytest-tests": [
                "repo-artifact-hygiene",
                "syntax",
                "pyright-money-path",
                "ruff-static-release-gate",
                "docs",
                "ui-asset-refs",
                "dependency-lock",
                "noop-guard",
                "storage-route-audit",
                "runtime-graph-startup",
                "pytest-collection",
                "pytest-tests",
            ],
        }

        for failing_label, labels_before_exit in expected_labels.items():
            with self.subTest(failing_label=failing_label):
                exit_code, calls, output, _ = self._run_main(failing_label=failing_label)

                self.assertEqual(exit_code, 23)
                self.assertEqual([label for label, _, _ in calls], labels_before_exit)
                self.assertIn(f"Validation failed during {failing_label}.", output)
                self.assertNotIn("Validation complete.", output)

    def test_validate_repo_runs_pytest(self) -> None:
        exit_code, calls, _, root = self._run_main()

        self.assertEqual(exit_code, 0)
        pytest_call = next(call for call in calls if call[0] == "pytest-tests")
        self.assertEqual(pytest_call[1], ["pytest-bin", "tests/", "-v", "--tb=short"])
        self.assertEqual(pytest_call[2]["PYTHONPATH"], str(root))

    def test_validate_repo_runs_runtime_graph_check_in_startup_mode(self) -> None:
        exit_code, calls, _, root = self._run_main()

        self.assertEqual(exit_code, 0)
        runtime_graph_call = next(call for call in calls if call[0] == "runtime-graph-startup")
        self.assertEqual(
            runtime_graph_call[1],
            ["python-bin", "tools/runtime_graph_check.py", "--mode", "startup"],
        )
        self.assertEqual(runtime_graph_call[2]["PYTHONPATH"], str(root))
        self.assertEqual(runtime_graph_call[2]["TRADING_VALIDATE_REPO_LIVE"], "0")

    def test_validate_repo_runs_dependency_lock_in_strict_mode(self) -> None:
        exit_code, calls, _, _ = self._run_main()

        self.assertEqual(exit_code, 0)
        dependency_call = next(call for call in calls if call[0] == "dependency-lock")
        self.assertEqual(
            dependency_call[1],
            ["python-bin", "tools/validate_dependency_lock.py", "--strict"],
        )

    def test_validate_repo_runs_pyright_money_path_gate(self) -> None:
        exit_code, calls, _, root = self._run_main()

        self.assertEqual(exit_code, 0)
        pyright_call = next(call for call in calls if call[0] == "pyright-money-path")
        self.assertEqual(pyright_call[1], ["python-bin", "tools/pyright_money_path_gate.py"])
        self.assertEqual(pyright_call[2]["PYTHONPATH"], str(root))

    def test_runtime_graph_startup_env_is_hermetic_by_default(self) -> None:
        run_env = validate_repo._runtime_graph_startup_env(
            {
                "PYTHONPATH": str(Path("repo")),
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
            }
        )

        self.assertEqual(run_env["TRADING_VALIDATION_MODE"], "startup")
        self.assertEqual(run_env["TS_STORAGE_BACKEND"], "sqlite")
        self.assertEqual(run_env["TIMESCALE_ENABLED"], "0")
        self.assertEqual(run_env["TIMESCALE_PRICES_ENABLED"], "0")
        self.assertEqual(run_env["TELEMETRY_READ_BACKEND"], "sqlite")
        self.assertEqual(run_env["PRICE_READ_BACKEND"], "sqlite")
        self.assertEqual(run_env["LIVE_CACHE_BACKEND"], "memory")
        self.assertEqual(run_env["PREFLIGHT_REQUIRE_TIMESCALE"], "0")
        self.assertEqual(run_env["PREFLIGHT_REQUIRE_REDIS"], "0")
        self.assertEqual(run_env["PREFLIGHT_REQUIRE_OBJECT_STORAGE"], "0")
        self.assertNotIn("TIMESCALE_DSN", run_env)
        self.assertNotIn("TIMESCALE_PRICES_DSN", run_env)
        self.assertNotIn("TS_PG_DSN", run_env)
        self.assertNotIn("LIVE_CACHE_REDIS_URL", run_env)
        self.assertNotIn("REDIS_URL", run_env)

    def test_runtime_graph_startup_env_preserves_live_dependencies(self) -> None:
        run_env = validate_repo._runtime_graph_startup_env(
            {
                "TRADING_VALIDATE_REPO_LIVE": "1",
                "TRADING_VALIDATION_REQUIRE_PROD_DEPS": "1",
                "TIMESCALE_ENABLED": "1",
                "TIMESCALE_DSN": "postgresql://timescale.local/trading",
                "TS_PG_DSN": "host=timescaledb dbname=trading",
                "LIVE_CACHE_BACKEND": "redis",
                "REDIS_URL": "redis://redis.local:6379/0",
                "PREFLIGHT_REQUIRE_REDIS": "1",
            }
        )

        self.assertEqual(run_env["TRADING_VALIDATION_MODE"], "startup")
        self.assertEqual(run_env["TRADING_VALIDATE_REPO_LIVE"], "1")
        self.assertEqual(run_env["TRADING_VALIDATION_REQUIRE_PROD_DEPS"], "1")
        self.assertEqual(run_env["TIMESCALE_ENABLED"], "1")
        self.assertEqual(run_env["TIMESCALE_DSN"], "postgresql://timescale.local/trading")
        self.assertEqual(run_env["TS_PG_DSN"], "host=timescaledb dbname=trading")
        self.assertEqual(run_env["LIVE_CACHE_BACKEND"], "redis")
        self.assertEqual(run_env["REDIS_URL"], "redis://redis.local:6379/0")
        self.assertEqual(run_env["PREFLIGHT_REQUIRE_REDIS"], "1")

    def test_validate_repo_live_loads_compose_env_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            compose_dir = root / "deploy" / "compose"
            compose_dir.mkdir(parents=True)
            (compose_dir / ".env").write_text(
                "\n".join(
                    [
                        "DASHBOARD_PUBLIC_PORT=18000",
                        "DASHBOARD_API_TOKEN=dashboard-secret",
                        "OPERATOR_API_TOKEN=operator-secret",
                        "TIMESCALE_ENABLED=1",
                        "LIVE_CACHE_BACKEND=redis",
                        "PREFLIGHT_REQUIRE_REDIS=1",
                    ]
                ),
                encoding="utf-8",
            )
            calls: list[tuple[str, list[str], dict[str, str]]] = []

            def fake_run(label: str, args: list[str], env: dict[str, str]) -> None:
                calls.append((label, list(args), dict(env)))

            with (
                patch.object(validate_repo, "ROOT", root),
                patch.object(validate_repo, "_project_python", return_value="python-bin"),
                patch.object(validate_repo, "_project_pytest", return_value=["pytest-bin"]),
                patch.object(validate_repo, "_run", side_effect=fake_run),
                patch.dict(os.environ, {}, clear=True),
            ):
                exit_code = validate_repo.main(["--live"])

        self.assertEqual(exit_code, 0)
        labels = [label for label, _, _ in calls]
        self.assertNotIn("production-readiness-gate", labels)
        smoke_call = next(call for call in calls if call[0] == "pipeline-smoke")
        self.assertEqual(smoke_call[2]["DASHBOARD_API_TOKEN"], "dashboard-secret")
        self.assertEqual(smoke_call[2]["OPERATOR_API_TOKEN"], "operator-secret")
        self.assertEqual(smoke_call[2]["PIPELINE_SMOKE_OPERATOR_TOKEN"], "operator-secret")
        self.assertEqual(smoke_call[2]["PIPELINE_SMOKE_BASE"], "http://127.0.0.1:18000")
        self.assertEqual(smoke_call[2]["PIPELINE_SMOKE_OPERATOR_BASE"], "http://127.0.0.1:18000/operator")
        runtime_graph_call = next(call for call in calls if call[0] == "runtime-graph-startup")
        self.assertEqual(runtime_graph_call[2]["TRADING_VALIDATE_REPO_LIVE"], "1")
        self.assertEqual(runtime_graph_call[2]["TRADING_VALIDATION_REQUIRE_PROD_DEPS"], "1")
        self.assertEqual(runtime_graph_call[2]["TIMESCALE_ENABLED"], "1")
        self.assertEqual(runtime_graph_call[2]["LIVE_CACHE_BACKEND"], "redis")
        self.assertEqual(runtime_graph_call[2]["PREFLIGHT_REQUIRE_REDIS"], "1")

    def test_validate_repo_pre_deploy_runs_production_readiness_gate(self) -> None:
        exit_code, calls, _, _ = self._run_main(argv=["--pre-deploy"])

        self.assertEqual(exit_code, 0)
        labels = [label for label, _, _ in calls]
        self.assertIn("production-readiness-gate", labels)
        self.assertNotIn("pipeline-smoke", labels)
        gate_call = next(call for call in calls if call[0] == "production-readiness-gate")
        self.assertEqual(gate_call[1], ["python-bin", "tools/production_readiness_gate.py", "--compact"])

    def test_validate_repo_skips_telemetry_burnin_check_by_default(self) -> None:
        exit_code, calls, _, _ = self._run_main()

        self.assertEqual(exit_code, 0)
        self.assertNotIn("telemetry-dual-write-burnin", [label for label, _, _ in calls])

    def test_validate_repo_runs_telemetry_burnin_check_when_validation_enabled(self) -> None:
        exit_code, calls, _, root = self._run_main(
            env_overrides={
                "TIMESCALE_TELEMETRY_VALIDATION_ENABLED": "1",
                "TIMESCALE_ENABLED": "1",
                "TIMESCALE_DSN": "postgres://timescale",
                "TIMESCALE_TELEMETRY_VALIDATE_LOOKBACK_MINUTES": "15",
                "TIMESCALE_TELEMETRY_MAX_COUNT_DELTA": "2",
                "TIMESCALE_TELEMETRY_MAX_LAST_TS_LAG_MS": "2500",
            }
        )

        self.assertEqual(exit_code, 0)
        burnin_call = next(call for call in calls if call[0] == "telemetry-dual-write-burnin")
        self.assertEqual(
            burnin_call[1],
            [
                "python-bin",
                "tools/compare_timescale_telemetry_dual_write.py",
                "--strict",
                "--require-healthy-mirror",
                "--require-healthy-timescale",
                "--lookback-minutes",
                "15",
                "--max-count-delta",
                "2",
                "--max-last-ts-lag-ms",
                "2500",
                "--json",
            ],
        )
        self.assertEqual(burnin_call[2]["PYTHONPATH"], str(root))

    def test_validate_repo_runs_telemetry_burnin_check_for_timescale_read_cutover(self) -> None:
        exit_code, calls, _, _ = self._run_main(
            env_overrides={
                "TS_STORAGE_BACKEND": "sqlite",
                "TIMESCALE_ENABLED": "1",
                "TIMESCALE_DSN": "postgres://timescale",
                "TELEMETRY_READ_BACKEND": "auto",
            }
        )

        self.assertEqual(exit_code, 0)
        self.assertIn("telemetry-dual-write-burnin", [label for label, _, _ in calls])

    def test_validate_repo_skips_legacy_telemetry_burnin_for_postgres_primary(self) -> None:
        exit_code, calls, _, _ = self._run_main(
            env_overrides={
                "TIMESCALE_ENABLED": "1",
                "TIMESCALE_DSN": "postgres://timescale",
                "TELEMETRY_READ_BACKEND": "auto",
                "TS_STORAGE_BACKEND": "postgres",
            }
        )

        self.assertEqual(exit_code, 0)
        self.assertNotIn("telemetry-dual-write-burnin", [label for label, _, _ in calls])

    def test_validate_repo_runs_storage_route_audit(self) -> None:
        exit_code, calls, _, root = self._run_main()

        self.assertEqual(exit_code, 0)
        audit_call = next(call for call in calls if call[0] == "storage-route-audit")
        self.assertEqual(
            audit_call[1],
            ["python-bin", "tools/storage_route_audit.py"],
        )
        self.assertEqual(audit_call[2]["PYTHONPATH"], str(root))

    def test_storage_backend_scope_validator_allows_documented_compat_loader(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runtime_dir = root / "engine" / "runtime"
            runtime_dir.mkdir(parents=True)
            (runtime_dir / "storage_sqlite.py").write_text(
                "\n".join(
                    [
                        "_PG_COMPAT_HELPER_NAMES = ('put_event',)",
                        "def _load_pg_compat_module():",
                        "    from engine.runtime import storage_pg as _pg",
                        "    return _pg",
                    ]
                ),
                encoding="utf-8",
            )
            (runtime_dir / "README.md").write_text(
                "bounded first slice with _PG_COMPAT_HELPER_NAMES compatibility shim",
                encoding="utf-8",
            )

            self.assertEqual(validate_repo._storage_sqlite_pg_compat_violations(root), [])

    def test_storage_backend_scope_validator_blocks_direct_pg_import_and_clone_markers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runtime_dir = root / "engine" / "runtime"
            runtime_dir.mkdir(parents=True)
            (runtime_dir / "storage_sqlite.py").write_text(
                "\n".join(
                    [
                        "from engine.runtime import storage_pg",
                        "def _clone_pg_helpers():",
                        "    return storage_pg.put_event.__code__",
                    ]
                ),
                encoding="utf-8",
            )
            (runtime_dir / "README.md").write_text(
                "bounded first slice with _PG_COMPAT_HELPER_NAMES compatibility shim",
                encoding="utf-8",
            )

            violations = validate_repo._storage_sqlite_pg_compat_violations(root)

        self.assertTrue(any("FunctionType" not in item and ".__code__" in item for item in violations))
        self.assertTrue(any("_clone_pg_helpers" in item for item in violations))
        self.assertTrue(any("storage_pg only inside _load_pg_compat_module" in item for item in violations))

    def test_worktree_layout_validator_blocks_loose_duplicate_sibling(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            root = parent / "system"
            duplicate = parent / "system-disk-retention-hardening"
            root.mkdir()
            duplicate.mkdir()

            with self.assertRaisesRegex(RuntimeError, "not a registered git worktree"):
                validate_repo._validate_worktree_layout(root)

    def test_validate_repo_runs_pytest_collection_gate(self) -> None:
        exit_code, calls, _, root = self._run_main()

        self.assertEqual(exit_code, 0)
        collection_call = next(call for call in calls if call[0] == "pytest-collection")
        self.assertEqual(
            collection_call[1],
            ["pytest-bin", "tests/", "--collect-only", "-q"],
        )
        self.assertEqual(collection_call[2]["PYTHONPATH"], str(root))

    def test_validate_repo_does_not_run_unittest_discovery(self) -> None:
        exit_code, calls, _, _ = self._run_main()

        self.assertEqual(exit_code, 0)
        self.assertNotIn("unit-tests", [label for label, _, _ in calls])
        for _, command, _ in calls:
            self.assertNotIn("unittest", command)

    def test_unit_test_env_forces_safe_test_auth_context(self) -> None:
        run_env = validate_repo._unit_test_env(
            {
                "APP_ENV": "prod",
                "PROD_LOCK": "1",
                "DASHBOARD_API_TOKEN": "live-token-should-not-control-tests",
                "DASHBOARD_API_TOKEN_FILE": "data/secrets/dashboard_api_token",
                "TIMESCALE_PASSWORD_FILE": "data/secrets/timescale_password",
                "OBJECT_STORE_SECRET_KEY": "live-object-secret-should-not-control-tests",
                "TS_SECRETS_PROVIDER": "systemd",
                "DB_PATH": "var/runtime/live.sqlite",
                "SQLITE_LIVENESS_DB_PATH": "var/runtime/live.liveness.sqlite",
                "TS_STORAGE_BACKEND": "postgres",
            }
        )

        self.assertEqual(run_env["APP_ENV"], "test")
        self.assertEqual(run_env["PROD_LOCK"], "0")
        self.assertEqual(run_env["TS_TESTING"], "1")
        self.assertEqual(run_env["TS_STORAGE_BACKEND"], "sqlite")
        self.assertNotIn("ENV", run_env)
        self.assertNotIn("NODE_ENV", run_env)
        self.assertNotIn("TS_ENV", run_env)
        self.assertNotIn("DASHBOARD_API_TOKEN", run_env)
        self.assertNotIn("DASHBOARD_API_TOKEN_FILE", run_env)
        self.assertNotIn("TIMESCALE_PASSWORD_FILE", run_env)
        self.assertNotIn("OBJECT_STORE_SECRET_KEY", run_env)
        self.assertNotIn("TS_SECRETS_PROVIDER", run_env)
        self.assertIn("/var/tmp/trading-system-tests-", run_env["TMPDIR"].replace("\\", "/"))
        self.assertTrue(Path(run_env["TRADING_TEST_TMPDIR"]).name.startswith("validate-repo-"))
        self.assertEqual(run_env["TMPDIR"], run_env["PYTEST_DEBUG_TEMPROOT"])
        self.assertEqual(run_env["TMPDIR"], run_env["TRADING_TEST_TMPDIR"])
        self.assertEqual(
            Path(run_env["DB_PATH"]),
            Path(run_env["TRADING_TEST_TMPDIR"]) / "validate_repo_unit" / "runtime-test.sqlite",
        )
        self.assertEqual(
            Path(run_env["SQLITE_LIVENESS_DB_PATH"]),
            Path(run_env["TRADING_TEST_TMPDIR"]) / "validate_repo_unit" / "runtime-test.liveness.sqlite",
        )

    def test_unit_test_env_preserves_explicit_test_tmp_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_env = validate_repo._unit_test_env({"TRADING_TEST_TMPDIR": tmp})

            self.assertEqual(Path(run_env["TRADING_TEST_TMPDIR"]), Path(tmp))
            self.assertEqual(Path(run_env["TMPDIR"]), Path(tmp))
            self.assertEqual(
                Path(run_env["DB_PATH"]),
                Path(tmp) / "validate_repo_unit" / "runtime-test.sqlite",
            )


if __name__ == "__main__":
    unittest.main()
