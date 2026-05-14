from __future__ import annotations

import io
import os
import subprocess
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
            exit_code = validate_repo.main([])

        return exit_code, calls, output.getvalue(), root

    def test_validate_repo_fails_on_test_failure(self) -> None:
        expected_labels = {
            "unit-tests": [
                "syntax",
                "docs",
                "ui-asset-refs",
                "dependency-lock",
                "noop-guard",
                "storage-route-audit",
                "runtime-graph-startup",
                "unit-tests",
            ],
            "pytest-tests": [
                "syntax",
                "docs",
                "ui-asset-refs",
                "dependency-lock",
                "noop-guard",
                "storage-route-audit",
                "runtime-graph-startup",
                "unit-tests",
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
                "TIMESCALE_ENABLED": "1",
                "TIMESCALE_DSN": "postgres://timescale",
                "TELEMETRY_READ_BACKEND": "auto",
            }
        )

        self.assertEqual(exit_code, 0)
        self.assertIn("telemetry-dual-write-burnin", [label for label, _, _ in calls])

    def test_validate_repo_runs_storage_route_audit(self) -> None:
        exit_code, calls, _, root = self._run_main()

        self.assertEqual(exit_code, 0)
        audit_call = next(call for call in calls if call[0] == "storage-route-audit")
        self.assertEqual(
            audit_call[1],
            ["python-bin", "tools/storage_route_audit.py"],
        )
        self.assertEqual(audit_call[2]["PYTHONPATH"], str(root))

    def test_validate_repo_runs_unittest(self) -> None:
        exit_code, calls, _, root = self._run_main()

        self.assertEqual(exit_code, 0)
        unittest_call = next(call for call in calls if call[0] == "unit-tests")
        self.assertEqual(
            unittest_call[1],
            ["python-bin", "-m", "unittest", "discover", "-s", "tests", "-v"],
        )
        self.assertEqual(unittest_call[2]["PYTHONPATH"], str(root))


if __name__ == "__main__":
    unittest.main()
