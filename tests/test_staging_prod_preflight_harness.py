from __future__ import annotations

import importlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_module():
    import engine.runtime.staging_prod_preflight as harness

    return importlib.reload(harness)


class StagingProdPreflightHarnessTests(unittest.TestCase):
    def _write_env(self, directory: Path, extra: str = "") -> Path:
        env_path = directory / "staging.env"
        env_path.write_text(
            "\n".join(
                [
                    "STAGING_PREFLIGHT_TARGET_ENV=staging",
                    "STAGING_PREFLIGHT_TARGET_ID=stage-a",
                    "TS_STORAGE_BACKEND=postgres",
                    "TS_PG_DSN=host=staging-db.internal port=5432 user=ts_app dbname=trading_staging password=stage-secret",
                    "APP_ENV=staging",
                    "ENGINE_MODE=safe",
                    "EXECUTION_MODE=safe",
                    "DASHBOARD_API_TOKEN=stage-dashboard-token",
                    extra,
                ]
            ),
            encoding="utf-8",
        )
        return env_path

    def test_guardrails_require_explicit_postgres_dsn(self) -> None:
        harness = _reload_module()

        with self.assertRaisesRegex(harness.GuardrailError, "requires TS_PG_DSN"):
            harness.validate_guardrails(
                {
                    "TS_STORAGE_BACKEND": "postgres",
                    "APP_ENV": "staging",
                },
                target_env="staging",
            )

        with self.assertRaisesRegex(harness.GuardrailError, "TS_STORAGE_BACKEND=postgres"):
            harness.validate_guardrails(
                {
                    "TS_STORAGE_BACKEND": "sqlite",
                    "TS_PG_DSN": "host=staging-db dbname=trading_staging",
                },
                target_env="staging",
            )

    def test_guardrails_reject_production_signals_without_confirmation(self) -> None:
        harness = _reload_module()
        env = {
            "TS_STORAGE_BACKEND": "postgres",
            "TS_PG_DSN": "host=prod-db.internal port=5432 user=ts_app dbname=trading_prod",
            "APP_ENV": "staging",
        }

        with self.assertRaisesRegex(harness.GuardrailError, "production-like target signals"):
            harness.validate_guardrails(env, target_env="staging")

        findings = harness.validate_guardrails(
            env,
            target_env="staging",
            allow_production_target=True,
            production_confirmation=harness.CONFIRM_PRODUCTION_PHRASE,
        )
        self.assertTrue(findings)
        self.assertIn("TS_PG_DSN contains a production-like marker", findings)

    def test_redaction_removes_secrets_from_env_and_output(self) -> None:
        harness = _reload_module()
        env = {
            "TS_PG_DSN": "host=staging-db user=ts_app password=stage-secret dbname=trading_staging",
            "DASHBOARD_API_TOKEN": "stage-dashboard-token",
            "OBJECT_STORE_SECRET_KEY": "object-secret",
        }
        known = harness.sensitive_values(env)

        snapshot = harness.redacted_env_snapshot(env)
        rendered = json.dumps(snapshot, sort_keys=True)
        self.assertNotIn("stage-secret", rendered)
        self.assertNotIn("stage-dashboard-token", rendered)
        self.assertNotIn("object-secret", rendered)
        self.assertIn("password=<redacted:", snapshot["TS_PG_DSN"])

        redacted_output = harness.redact_string("failed with stage-secret and stage-dashboard-token", known)
        self.assertNotIn("stage-secret", redacted_output)
        self.assertNotIn("stage-dashboard-token", redacted_output)

    def test_default_child_env_does_not_leak_ambient_database_credentials(self) -> None:
        harness = _reload_module()
        with tempfile.TemporaryDirectory() as td:
            env_path = self._write_env(Path(td))
            child_env, loaded = harness.load_child_env(
                [env_path],
                base_env={
                    "PATH": "/usr/bin",
                    "TS_PG_DSN": "host=prod-db.internal password=prod-secret",
                    "PGPASSWORD": "prod-secret",
                },
                allow_ambient_env=False,
            )

        self.assertEqual(loaded, [str(env_path)])
        self.assertIn(str(REPO_ROOT), child_env["PYTHONPATH"])
        self.assertIn("staging-db.internal", child_env["TS_PG_DSN"])
        self.assertNotIn("prod-db.internal", json.dumps(child_env, sort_keys=True))
        self.assertNotIn("prod-secret", json.dumps(child_env, sort_keys=True))

    def test_run_invokes_prod_preflight_and_writes_redacted_evidence(self) -> None:
        harness = _reload_module()
        with tempfile.TemporaryDirectory() as td:
            temp_root = Path(td)
            env_path = self._write_env(temp_root)
            evidence_dir = temp_root / "evidence"
            captured = {}

            def fake_run(argv, cwd, env, text, stdout, stderr, timeout, check):
                captured.update({"argv": argv, "cwd": cwd, "env": dict(env), "timeout": timeout})
                return subprocess.CompletedProcess(
                    argv,
                    2,
                    stdout=json.dumps(
                        {
                            "ok": False,
                            "status": "warning",
                            "errors": [],
                            "smoke": [
                                {
                                    "name": "sample",
                                    "out": "connected with password=stage-secret and token stage-dashboard-token",
                                }
                            ],
                        }
                    ),
                    stderr="dsn password=stage-secret",
                )

            with patch.object(harness.subprocess, "run", side_effect=fake_run):
                with patch.object(sys, "stdout"):
                    rc = harness.run(
                        [
                            "--env-file",
                            str(env_path),
                            "--target-env",
                            "staging",
                            "--evidence-dir",
                            str(evidence_dir),
                            "--timeout-s",
                            "12",
                        ],
                        base_env={"PATH": "/usr/bin"},
                    )

            self.assertEqual(rc, 2)
            self.assertEqual(captured["argv"][1:], ["engine/runtime/prod_preflight.py", "--json", "--timeout_s", "12"])
            self.assertEqual(captured["env"]["TS_STORAGE_BACKEND"], "postgres")
            self.assertEqual(captured["env"]["STAGING_PREFLIGHT_TARGET_ENV"], "staging")

            evidence_files = list((evidence_dir / "staging").glob("prod_preflight_*.json"))
            self.assertEqual(len(evidence_files), 1)
            payload = json.loads(evidence_files[0].read_text(encoding="utf-8"))
            rendered = json.dumps(payload, sort_keys=True)
            self.assertEqual(payload["process"]["returncode"], 2)
            self.assertEqual(payload["prod_preflight"]["status"], "warning")
            self.assertNotIn("stage-secret", rendered)
            self.assertNotIn("stage-dashboard-token", rendered)
            self.assertIn("<redacted:", rendered)

    def test_static_assets_wire_the_harness(self) -> None:
        wrapper = (REPO_ROOT / "ops" / "server" / "run_staging_prod_preflight.sh").read_text(encoding="utf-8")
        env_example = (REPO_ROOT / "deploy" / "env" / "staging-prod-preflight.env.example").read_text(encoding="utf-8")
        docs = (REPO_ROOT / "docs" / "STAGING_PROD_PREFLIGHT_EVIDENCE.md").read_text(encoding="utf-8")

        self.assertIn("engine.runtime.staging_prod_preflight", wrapper)
        self.assertIn("STAGING_PREFLIGHT_TARGET_ENV=staging", env_example)
        self.assertIn("TS_STORAGE_BACKEND=postgres", env_example)
        self.assertIn("TS_PG_DSN=", env_example)
        self.assertNotIn("stage-secret", env_example)
        self.assertIn("I_UNDERSTAND_THIS_USES_PRODUCTION_CREDENTIALS", docs)
        self.assertIn("var/artifacts/preflight/staging", docs)


if __name__ == "__main__":
    unittest.main()
