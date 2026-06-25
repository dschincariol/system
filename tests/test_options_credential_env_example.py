from __future__ import annotations

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class OptionsCredentialEnvExampleTests(unittest.TestCase):
    def _env_example_lines(self) -> dict[str, str]:
        text = (REPO_ROOT / ".env.example").read_text(encoding="utf-8")
        out: dict[str, str] = {}
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            out[str(key).strip()] = str(value).strip()
        return out

    def test_options_credentials_and_file_forms_are_documented_without_secret_values(self) -> None:
        values = self._env_example_lines()

        for key in ("POLYGON_API_KEY_FILE", "TRADIER_API_TOKEN", "TRADIER_API_TOKEN_FILE"):
            with self.subTest(key=key):
                self.assertIn(key, values)
                self.assertEqual(values[key], "")

    def test_file_keys_match_credential_loader_convention(self) -> None:
        values = self._env_example_lines()

        for secret_name in ("POLYGON_API_KEY", "TRADIER_API_TOKEN"):
            with self.subTest(secret_name=secret_name):
                self.assertIn(secret_name, values)
                self.assertIn(f"{secret_name}_FILE", values)


if __name__ == "__main__":
    unittest.main()
