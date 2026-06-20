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


class ProdPreflightStorageContractTests(unittest.TestCase):
    def _load_module(self):
        import engine.runtime.prod_preflight as prod_preflight

        return importlib.reload(prod_preflight)

    def test_verify_sqlite_contract_reports_missing_indexes(self) -> None:
        db_path = _var_db_path("prod_preflight_contracts.db")
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
        db_path = _var_db_path("prod_preflight_contracts_columns.db")
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
        db_path = _var_db_path("prod_preflight_contracts_tables.db")
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

    def test_verify_postgres_contract_reports_recent_migration_column_index_and_stale_id(self) -> None:
        db_path = _var_db_path("prod_preflight_pg_contracts.db")
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
            with patch(
                "engine.runtime.storage.get_db_validation_snapshot",
                return_value={
                    "ok": False,
                    "missing_tables": [],
                    "missing_columns": {"alert_acks": ["expires_ts_ms"]},
                    "missing_indexes": ["idx_alert_lifecycle_events_alert_ts"],
                    "schema_version": 54,
                    "expected_schema_version": 55,
                    "schema_version_ok": False,
                    "schema_status": "stale",
                    "schema_migration_missing_ids": [55],
                    "quick_check": "not_applicable",
                },
            ):
                notes, errors, validation = prod_preflight._verify_postgres_contract()

        self.assertEqual(notes, [])
        self.assertEqual(validation.get("missing_columns"), {"alert_acks": ["expires_ts_ms"]})
        rendered = "\n".join(errors)
        self.assertIn("missing columns: alert_acks(expires_ts_ms)", rendered)
        self.assertIn("missing indexes: idx_alert_lifecycle_events_alert_ts", rendered)
        self.assertIn("schema version: actual=54 expected=55 status=stale", rendered)
        self.assertIn("missing migrations: 55", rendered)

    def test_verify_postgres_contract_reports_owned_live_ingestion_drift(self) -> None:
        db_path = _var_db_path("prod_preflight_pg_owned_contracts.db")
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
            with patch(
                "engine.runtime.storage.get_db_validation_snapshot",
                return_value={
                    "ok": False,
                    "missing_tables": [],
                    "missing_columns": {},
                    "missing_indexes": [],
                    "schema_version": 55,
                    "expected_schema_version": 55,
                    "schema_version_ok": True,
                    "schema_status": "applied",
                    "owned_schema_ok": False,
                    "owned_missing_columns": {"price_quotes_raw": ["event_key"]},
                    "owned_unexpected_columns": {"prices": ["provider"]},
                    "owned_type_mismatches": {
                        "prices": {"price": {"expected": "REAL", "actual": "TEXT"}}
                    },
                    "owned_pk_mismatches": {"prices": {"ts_ms": {"expected": 2, "actual": 0}}},
                    "owned_missing_indexes": {"prices": ["idx_prices_symbol_ts"]},
                    "quick_check": "not_applicable",
                },
            ):
                notes, errors, validation = prod_preflight._verify_postgres_contract()

        self.assertEqual(notes, [])
        self.assertFalse(bool(validation.get("owned_schema_ok")))
        rendered = "\n".join(errors)
        self.assertIn("owned missing columns: price_quotes_raw(event_key)", rendered)
        self.assertIn("owned unexpected columns: prices(provider)", rendered)
        self.assertIn("owned type mismatches: prices(['price'])", rendered)
        self.assertIn("owned primary keys: prices(['ts_ms'])", rendered)
        self.assertIn("owned missing indexes: prices(idx_prices_symbol_ts)", rendered)

    def test_backup_restore_evidence_gate_reports_backup_accounting_mount_and_retention(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ENV": "prod",
                "ENGINE_MODE": "safe",
                "ALLOW_TRAINING": "0",
            },
            clear=True,
        ):
            prod_preflight = self._load_module()
            with patch(
                "engine.runtime.backup_evidence.backup_restore_evidence_snapshot",
                return_value={
                    "fresh": True,
                    "required": True,
                    "warnings": [],
                    "policy": {"restore_rto_s": 1800},
                    "base_backup": {"age_s": 60},
                    "wal_archive": {"age_s": 30},
                    "restore_drill": {"age_s": 120},
                },
            ):
                with patch(
                    "engine.runtime.backup_evidence.backup_accounting_snapshot",
                    return_value={
                        "ok": True,
                        "host_path": "/var/backups/trading",
                        "container_path": "/var/backups/trading",
                        "container_mount_source": "/host/backups/trading",
                        "root_size": {
                            "apparent_bytes": 1234,
                            "allocated_bytes": 4096,
                        },
                        "retention_status": "configured",
                        "retention": {
                            "status": "configured",
                            "keep_daily_days": 14,
                            "keep_weekly_days": 365,
                        },
                        "warnings": [],
                    },
                ):
                    notes, warnings, errors, state = prod_preflight._backup_restore_evidence_gate()

        self.assertEqual(errors, [])
        self.assertEqual(warnings, [])
        rendered = "\n".join(notes)
        self.assertIn("host_path=/var/backups/trading", rendered)
        self.assertIn("container_mount_source=/host/backups/trading", rendered)
        self.assertIn("retention_status=configured", rendered)
        self.assertEqual(state["accounting"]["retention_status"], "configured")

    def test_main_stops_before_smoke_when_sqlite_contract_invalid(self) -> None:
        db_path = _var_db_path("prod_preflight_contracts_main.db")
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
