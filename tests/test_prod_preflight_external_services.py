from __future__ import annotations

import importlib
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _var_db_path(name: str) -> str:
    path = (REPO_ROOT / "var" / "db" / name).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


class ProdPreflightExternalServiceTests(unittest.TestCase):
    def _load_module(self):
        import engine.runtime.prod_preflight as prod_preflight

        return importlib.reload(prod_preflight)

    def test_main_stops_before_smoke_when_required_external_service_fails(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ENV": "dev",
                "DB_PATH": _var_db_path("prod_preflight_external.db"),
                "ALLOW_TRAINING": "0",
            },
            clear=True,
        ):
            prod_preflight = self._load_module()
            with patch.object(sys, "argv", ["prod_preflight.py"]):
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
                                        return_value=([], [], ["timescale_primary unreachable"], [{"name": "timescale_primary"}]),
                                    ):
                                        with patch.object(
                                            prod_preflight,
                                            "_run_cmd",
                                            side_effect=AssertionError("smoke jobs should not run"),
                                        ):
                                            rc = prod_preflight.main()

        self.assertEqual(rc, 3)


if __name__ == "__main__":
    unittest.main()
