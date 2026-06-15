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


class ProdPreflightStorageContractTests(unittest.TestCase):
    def _load_module(self):
        import engine.runtime.prod_preflight as prod_preflight

        return importlib.reload(prod_preflight)

    def test_verify_sqlite_contract_reports_missing_indexes(self) -> None:
        db_path = str((Path.cwd() / "prod_preflight_contracts.db").resolve())
        with patch.dict(
            os.environ,
            {
                "ENV": "dev",
                "DB_PATH": db_path,
                "ALLOW_TRAINING": "0",
            },
            clear=True,
        ):
            prod_preflight = self._load_module()
            with patch(
                "engine.runtime.storage.get_db_validation_snapshot",
                return_value={
                    "ok": False,
                    "missing_tables": [],
                    "missing_columns": {},
                    "missing_indexes": ["idx_prices_symbol_ts"],
                    "schema_version": 11,
                    "expected_schema_version": 11,
                    "schema_version_ok": True,
                    "schema_status": "applied",
                    "quick_check": "ok",
                },
            ):
                notes, errors, validation = prod_preflight._verify_sqlite_contract()

        self.assertEqual(notes, [])
        self.assertEqual(validation.get("missing_indexes"), ["idx_prices_symbol_ts"])
        self.assertTrue(any("missing indexes: idx_prices_symbol_ts" in error for error in errors), errors)

    def test_verify_sqlite_contract_reports_missing_live_ingestion_columns(self) -> None:
        db_path = str((Path.cwd() / "prod_preflight_contracts_columns.db").resolve())
        with patch.dict(
            os.environ,
            {
                "ENV": "dev",
                "DB_PATH": db_path,
                "ALLOW_TRAINING": "0",
            },
            clear=True,
        ):
            prod_preflight = self._load_module()
            with patch(
                "engine.runtime.storage.get_db_validation_snapshot",
                return_value={
                    "ok": False,
                    "missing_tables": [],
                    "missing_columns": {
                        "price_quotes_raw": ["event_key"],
                        "ingestion_pipeline_health": ["meta_json"],
                    },
                    "missing_indexes": [],
                    "schema_version": 11,
                    "expected_schema_version": 11,
                    "schema_version_ok": True,
                    "schema_status": "applied",
                    "quick_check": "ok",
                },
            ):
                notes, errors, validation = prod_preflight._verify_sqlite_contract()

        self.assertEqual(notes, [])
        self.assertEqual(
            validation.get("missing_columns"),
            {
                "price_quotes_raw": ["event_key"],
                "ingestion_pipeline_health": ["meta_json"],
            },
        )
        self.assertTrue(
            any(
                "missing columns: ingestion_pipeline_health(meta_json); price_quotes_raw(event_key)"
                in error
                for error in errors
            ),
            errors,
        )

    def test_verify_sqlite_contract_reports_missing_live_ingestion_tables(self) -> None:
        db_path = str((Path.cwd() / "prod_preflight_contracts_tables.db").resolve())
        with patch.dict(
            os.environ,
            {
                "ENV": "dev",
                "DB_PATH": db_path,
                "ALLOW_TRAINING": "0",
            },
            clear=True,
        ):
            prod_preflight = self._load_module()
            with patch(
                "engine.runtime.storage.get_db_validation_snapshot",
                return_value={
                    "ok": False,
                    "missing_tables": [
                        "price_feed_lock",
                        "options_symbol_ingestion_state",
                    ],
                    "missing_columns": {},
                    "missing_indexes": [],
                    "schema_version": 11,
                    "expected_schema_version": 11,
                    "schema_version_ok": True,
                    "schema_status": "applied",
                    "quick_check": "ok",
                },
            ):
                notes, errors, validation = prod_preflight._verify_sqlite_contract()

        self.assertEqual(notes, [])
        self.assertEqual(
            sorted(validation.get("missing_tables") or []),
            ["options_symbol_ingestion_state", "price_feed_lock"],
        )
        self.assertTrue(
            any(
                "missing tables: options_symbol_ingestion_state,price_feed_lock" in error
                for error in errors
            ),
            errors,
        )

    def test_main_stops_before_smoke_when_sqlite_contract_invalid(self) -> None:
        db_path = str((Path.cwd() / "prod_preflight_contracts_main.db").resolve())
        with patch.dict(
            os.environ,
            {
                "ENV": "dev",
                "DB_PATH": db_path,
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
                                    return_value=(
                                        [],
                                        ["sqlite contract invalid missing indexes: idx_prices_symbol_ts"],
                                        {
                                            "ok": False,
                                            "missing_indexes": ["idx_prices_symbol_ts"],
                                            "schema_version": 11,
                                            "expected_schema_version": 11,
                                            "schema_version_ok": True,
                                            "quick_check": "ok",
                                        },
                                    ),
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
