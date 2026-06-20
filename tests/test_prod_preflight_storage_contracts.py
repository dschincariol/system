from __future__ import annotations

import importlib
import json
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
        self.assertEqual(len(errors), 1)
        self.assertIn("postgres_schema_validation_failed", errors[0])
        self.assertIn("missing_indexes_count=1", errors[0])
        self.assertIn("missing_indexes=idx_prices_symbol_ts", errors[0])

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
        self.assertEqual(len(errors), 1)
        self.assertIn("missing_columns_count=2", errors[0])
        self.assertIn("ingestion_pipeline_health(meta_json)", errors[0])
        self.assertIn("price_quotes_raw(event_key)", errors[0])

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
            "missing_tables=options_symbol_ingestion_state,price_feed_lock" in errors[0],
            errors,
        )

    def test_verify_postgres_contract_condenses_large_schema_validation(self) -> None:
        db_path = str((Path.cwd() / "prod_preflight_pg_contracts.db").resolve())
        with patch.dict(
            os.environ,
            {
                "ENV": "prod",
                "DB_PATH": db_path,
                "ALLOW_TRAINING": "0",
            },
            clear=True,
        ):
            prod_preflight = self._load_module()
            missing_indexes = [f"idx_big_{idx}" for idx in range(20)]
            missing_tables = [f"table_{idx}" for idx in range(20)]
            with patch(
                "engine.runtime.storage.get_db_validation_snapshot",
                return_value={
                    "ok": False,
                    "missing_tables": missing_tables,
                    "missing_columns": {"prices": ["source", "provider", "venue", "feed", "extra"]},
                    "missing_indexes": missing_indexes,
                    "schema_version": 63,
                    "expected_schema_version": 20,
                    "schema_version_ok": False,
                    "schema_status": "unexpected_migrations",
                    "schema_migration_unexpected_ids": list(range(21, 64)),
                    "quick_check": "not_applicable",
                },
            ):
                notes, errors, validation = prod_preflight._verify_postgres_contract()

        self.assertEqual(notes, [])
        self.assertEqual(validation.get("schema_version"), 63)
        self.assertEqual(len(errors), 1)
        error = errors[0]
        self.assertLess(len(error), 1000)
        self.assertIn("postgres_schema_validation_failed", error)
        self.assertIn("migration_required=1", error)
        self.assertIn("actual_schema_version=63", error)
        self.assertIn("expected_schema_version=20", error)
        self.assertIn("missing_tables_count=20", error)
        self.assertIn("missing_indexes_count=20", error)
        self.assertIn("+14_more", error)
        self.assertNotIn("idx_big_19", error)

    def test_docker_log_cap_gate_flags_uncapped_running_container(self) -> None:
        with patch.dict(os.environ, {"ENV": "prod"}, clear=True):
            prod_preflight = self._load_module()
            ps_result = type(
                "Result",
                (),
                {"returncode": 0, "stdout": "abc123\n", "stderr": ""},
            )()
            inspect_result = type(
                "Result",
                (),
                {
                    "returncode": 0,
                    "stdout": json.dumps(
                        [
                            {
                                "Id": "abc123",
                                "Name": "/trading-runtime",
                                "HostConfig": {
                                    "LogConfig": {
                                        "Type": "json-file",
                                        "Config": {},
                                    }
                                },
                            }
                        ]
                    ),
                    "stderr": "",
                },
            )()
            with patch.object(prod_preflight.shutil, "which", return_value="/usr/bin/docker"):
                with patch.object(prod_preflight.subprocess, "run", side_effect=[ps_result, inspect_result]):
                    notes, warnings, errors, state = prod_preflight._docker_log_cap_gate()

        self.assertEqual(notes, [])
        self.assertEqual(warnings, [])
        self.assertEqual(len(errors), 1)
        self.assertIn("trading-runtime", errors[0])
        self.assertEqual(state["containers"][0]["driver"], "json-file")
        self.assertFalse(bool(state["containers"][0]["capped"]))

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
