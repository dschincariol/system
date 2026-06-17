from __future__ import annotations

import importlib
import io
import json
import os
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


class ProdPreflightProvisioningTests(unittest.TestCase):
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

    def test_inline_dsn_password_does_not_require_secret_provider(self) -> None:
        with patch.dict(
            os.environ,
            {
                "TS_PG_DSN": "host=timescaledb port=5432 user=trading dbname=trading password=inline-test",
                "DB_PATH": str(Path.cwd()),
            },
            clear=True,
        ):
            prod_preflight = _reload_module()
            notes, errors, snapshot = prod_preflight._production_provisioning_gate()

        self.assertEqual(errors, [])
        self.assertIn("credential source ok provider=inline_or_env_password", notes)
        self.assertEqual(snapshot["credentials"]["required_names"], [])

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
