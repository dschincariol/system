from __future__ import annotations

import importlib
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class DBRepairTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.tmp.name) / "db_repair.db"
        self._env_backup = {
            key: os.environ.get(key)
            for key in (
                "DB_PATH",
                "DB_REPAIR_AUTO_REINDEX_EVENT_LOG_INDEXES",
            )
        }
        os.environ["DB_PATH"] = str(self.db_path)
        os.environ.pop("DB_REPAIR_AUTO_REINDEX_EVENT_LOG_INDEXES", None)
        _, self.storage, self.db_repair = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.runtime.db_repair",
        )
        self.storage.init_db()

    def tearDown(self) -> None:
        for key, value in self._env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception:
            pass
        self.tmp.cleanup()

    def test_repair_startup_fast_path_bootstraps_fresh_database(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir, patch.dict(
            os.environ,
            {
                "DB_PATH": str(Path(tmpdir) / "db_repair_fresh.db"),
                "SQLITE_LIVENESS_DB_ENABLED": "0",
                "SQLITE_TRACE_REPORT_EVERY_S": "0",
            },
            clear=False,
        ):
            storage, db_repair = _reload_modules(
                "engine.runtime.storage",
                "engine.runtime.db_repair",
            )
            out = db_repair.repair(startup_fast_path=True)

            self.assertTrue(bool(out.get("ok")), out)
            self.assertTrue(bool(out.get("init_db")))
            self.assertTrue(bool((out.get("integrity_check") or {}).get("deferred")))

            validation = storage.get_db_validation_snapshot(include_quick_check=False)
            self.assertTrue(bool(validation.get("ok")), validation)
            self.assertGreaterEqual(int(validation.get("schema_version") or 0), int(storage.SCHEMA_VERSION))

            storage.close_pooled_connections()

    def test_db_guard_honors_explicit_sqlite_validation_backend(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir, patch.dict(
            os.environ,
            {
                "DB_PATH": str(Path(tmpdir) / "sqlite_validation.db"),
                "SQLITE_LIVENESS_DB_ENABLED": "0",
                "SQLITE_TRACE_REPORT_EVERY_S": "0",
                "TS_STORAGE_BACKEND": "sqlite",
                "TRADING_VALIDATION_MODE": "startup",
                "DATA_SOURCE_MANAGER_READ_ONLY": "1",
                "ENGINE_PRIMARY_BOOTSTRAP_DONE": "1",
            },
            clear=False,
        ):
            db_guard, storage = _reload_modules(
                "engine.runtime.db_guard",
                "engine.runtime.storage",
            )
            storage.init_db()
            out = db_guard.ensure_db_ok()
            storage.close_pooled_connections()

        self.assertTrue(bool(out.get("ok")), out)
        self.assertEqual(str(out.get("storage") or ""), "sqlite")
        self.assertNotIn("postgres_connect_failed", str(out))

    def test_repair_schema_delegates_postgres_backend_to_migrator(self) -> None:
        repair_schema, storage, migrator = _reload_modules(
            "engine.runtime.jobs.repair_schema",
            "engine.runtime.storage",
            "engine.runtime.schema.migrator",
        )
        validation = {
            "ok": True,
            "backend": "postgres",
            "schema_version": 56,
            "expected_schema_version": 56,
            "schema_version_ok": True,
        }

        with patch.object(repair_schema, "load_runtime_config", return_value=SimpleNamespace(db_path="/var/lib/trading")):
            with patch.object(storage, "_SQLITE_TEST_BACKEND", False, create=True):
                with patch.object(storage, "init_db") as init_db:
                    with patch.object(storage, "get_db_validation_snapshot", return_value=validation) as validate:
                        with patch.object(migrator, "apply_migrations", return_value=[56]) as apply:
                            with patch.object(migrator, "expected_schema_version", return_value=56):
                                result = repair_schema.run(include_quick_check=False)

        self.assertTrue(bool(result.get("ok")), result)
        self.assertEqual(str(result.get("backend") or ""), "postgres")
        self.assertEqual(result.get("applied_migrations"), [56])
        self.assertEqual(int(result.get("schema_version") or 0), 56)
        self.assertEqual(int(result.get("expected_schema_version") or 0), 56)
        self.assertEqual(init_db.call_count, 2)
        apply.assert_called_once_with()
        validate.assert_called_once_with(include_quick_check=False)

    def test_storage_validation_skips_missing_schema_table_warnings_on_empty_db(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir, patch.dict(
            os.environ,
            {
                "DB_PATH": str(Path(tmpdir) / "validation_empty.db"),
                "SQLITE_LIVENESS_DB_ENABLED": "0",
                "SQLITE_TRACE_REPORT_EVERY_S": "0",
            },
            clear=False,
        ):
            empty_db = Path(tmpdir) / "validation_empty.db"
            sqlite3.connect(str(empty_db)).close()
            (storage,) = _reload_modules("engine.runtime.storage")

            with patch.object(storage, "_warn_nonfatal") as mock_warn:
                validation = storage.get_db_validation_snapshot(include_quick_check=False)

            warned_codes = [str(call.args[0]) for call in mock_warn.call_args_list if call.args]
            self.assertNotIn("STORAGE_DB_VALIDATION_SCHEMA_READ_FAILED", warned_codes)
            self.assertNotIn("STORAGE_DB_VALIDATION_RUNTIME_META_READ_FAILED", warned_codes)
            if str(validation.get("storage") or "").lower() == "postgres":
                self.assertGreaterEqual(int(validation.get("schema_version") or 0), int(storage.SCHEMA_VERSION))
            else:
                self.assertIsNone(validation.get("schema_version"))

            storage.close_pooled_connections()

    def test_backfill_alert_prediction_ids_skips_when_predictions_parent_is_absent(self) -> None:
        con = sqlite3.connect(":memory:")
        try:
            con.execute("PRAGMA foreign_keys=OFF")
            con.execute(
                """
                CREATE TABLE alerts (
                  id INTEGER PRIMARY KEY,
                  prediction_id INTEGER REFERENCES predictions(id),
                  event_id INTEGER,
                  symbol TEXT,
                  horizon_s INTEGER,
                  ts_ms INTEGER
                )
                """
            )
            con.execute(
                """
                INSERT INTO alerts(id, prediction_id, event_id, symbol, horizon_s, ts_ms)
                VALUES (1, 1, 7, 'SPY', 60, 1000)
                """
            )
            con.commit()
            con.execute("PRAGMA foreign_keys=ON")

            self.storage._backfill_alert_prediction_ids(con)

            row = con.execute(
                "SELECT id, prediction_id FROM alerts WHERE id = 1"
            ).fetchone()
            self.assertEqual(row, (1, 1))
        finally:
            con.close()

    def test_repair_surfaces_event_log_reindex_maintenance_when_integrity_fails(self) -> None:
        findings = [
            "row 7 missing from index idx_event_log_corr",
            "wrong # of entries in index idx_event_log_type_ts",
        ]
        with patch.object(self.db_repair, "ensure_db_ok", return_value={"ok": True}), patch.object(
            self.db_repair, "repair_schema", return_value={"ok": True}
        ), patch.object(
            self.db_repair, "init_db", return_value=None
        ), patch.object(
            self.db_repair, "_integrity_check_rows", return_value=list(findings)
        ), patch.object(
            self.db_repair, "_auto_reindex_enabled", return_value=False
        ):
            out = self.db_repair.repair()

        self.assertFalse(bool(out.get("ok")))
        self.assertEqual(str(out.get("error")), "integrity_check_failed")
        self.assertEqual(
            list((out.get("integrity_check") or {}).get("repairable_indexes") or []),
            ["idx_event_log_corr", "idx_event_log_type_ts"],
        )
        maintenance = dict(out.get("maintenance_required") or {})
        self.assertEqual(str(maintenance.get("action")), "reindex_event_log_indexes")
        self.assertEqual(
            list(maintenance.get("recommended_sql") or []),
            ["REINDEX idx_event_log_corr;", "REINDEX idx_event_log_type_ts;"],
        )

    def test_repair_can_report_success_after_safe_event_log_reindex(self) -> None:
        with patch.object(self.db_repair, "ensure_db_ok", return_value={"ok": True}), patch.object(
            self.db_repair, "repair_schema", return_value={"ok": True}
        ), patch.object(
            self.db_repair, "init_db", return_value=None
        ), patch.object(
            self.db_repair,
            "_integrity_check_rows",
            side_effect=[
                [
                    "row 7 missing from index idx_event_log_corr",
                    "wrong # of entries in index idx_event_log_type_ts",
                ],
                ["ok"],
            ],
        ), patch.object(
            self.db_repair, "_auto_reindex_enabled", return_value=True
        ), patch.object(
            self.db_repair,
            "_reindex_event_log_indexes",
            return_value=["idx_event_log_corr", "idx_event_log_type_ts"],
        ) as mock_reindex:
            out = self.db_repair.repair()

        self.assertTrue(bool(out.get("ok")))
        self.assertEqual(
            list((out.get("integrity_check") or {}).get("auto_reindexed") or []),
            ["idx_event_log_corr", "idx_event_log_type_ts"],
        )
        self.assertEqual(
            dict(out.get("event_log_index_maintenance") or {}).get("status"),
            "reindexed",
        )
        mock_reindex.assert_called_once_with(["idx_event_log_corr", "idx_event_log_type_ts"])

    def test_repair_reapplies_storage_owned_live_ingestion_schema(self) -> None:
        with sqlite3.connect(str(self.db_path)) as con:
            con.execute("DROP TABLE IF EXISTS price_quotes")
            con.execute("DROP TABLE IF EXISTS price_provider_health")
            con.execute("DROP TABLE IF EXISTS ingestion_pipeline_health")
            con.execute("DROP TABLE IF EXISTS price_feed_lock")
            con.execute("DROP TABLE IF EXISTS options_symbol_ingestion_state")
            con.execute(
                """
                CREATE TABLE price_quotes (
                  ts_ms INTEGER NOT NULL,
                  symbol TEXT NOT NULL,
                  last REAL,
                  bid REAL,
                  ask REAL,
                  spread REAL,
                  volume REAL,
                  PRIMARY KEY(symbol, ts_ms)
                )
                """
            )
            con.execute(
                "CREATE INDEX idx_price_quotes_symbol_ts ON price_quotes(symbol, ts_ms)"
            )
            con.execute("CREATE INDEX idx_price_quotes_ts ON price_quotes(ts_ms)")
            con.execute(
                """
                CREATE TABLE price_provider_health (
                  ts_ms INTEGER NOT NULL,
                  provider TEXT NOT NULL,
                  ok INTEGER NOT NULL,
                  latency_ms INTEGER,
                  n_symbols INTEGER,
                  error TEXT,
                  PRIMARY KEY(provider, ts_ms)
                )
                """
            )
            con.execute(
                """
                CREATE TABLE ingestion_pipeline_health (
                  ts_ms INTEGER NOT NULL,
                  pipeline TEXT NOT NULL,
                  ok INTEGER NOT NULL,
                  PRIMARY KEY(pipeline, ts_ms)
                )
                """
            )
            con.execute(
                """
                CREATE TABLE options_symbol_ingestion_state (
                  symbol TEXT NOT NULL PRIMARY KEY,
                  disabled_until_ts_ms INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            con.execute(
                """
                CREATE INDEX idx_options_symbol_ingestion_disabled
                ON options_symbol_ingestion_state(disabled_until_ts_ms)
                """
            )
            con.commit()

        self.storage._INIT_DB_READY_PATH = ""

        with patch.object(self.db_repair, "ensure_db_ok", return_value={"ok": True}), patch.object(
            self.db_repair,
            "repair_schema",
            return_value={
                "ok": True,
                "schema_version": self.storage.SCHEMA_VERSION,
                "expected_schema_version": self.storage.SCHEMA_VERSION,
            },
        ), patch.object(
            self.db_repair, "_integrity_check_rows", return_value=["ok"]
        ):
            out = self.db_repair.repair()

        self.assertTrue(bool(out.get("ok")))
        self.assertTrue(bool(out.get("init_db")))

        validation = self.storage.get_db_validation_snapshot(include_quick_check=False)
        self.assertTrue(bool(validation.get("ok")), validation)
        self.assertEqual((validation.get("missing_columns") or {}).get("price_quotes"), None)
        self.assertEqual((validation.get("missing_columns") or {}).get("price_provider_health"), None)
        self.assertEqual((validation.get("missing_columns") or {}).get("ingestion_pipeline_health"), None)
        self.assertEqual((validation.get("missing_columns") or {}).get("options_symbol_ingestion_state"), None)
        self.assertEqual((validation.get("missing_tables") or []).count("price_feed_lock"), 0)

    def test_repair_normalizes_legacy_prices_schema_before_owned_contract_gate(self) -> None:
        with sqlite3.connect(str(self.db_path)) as con:
            con.execute("DROP TABLE IF EXISTS prices")
            con.execute(
                """
                CREATE TABLE prices (
                  ts_ms INTEGER NOT NULL,
                  symbol TEXT NOT NULL,
                  price REAL NOT NULL,
                  source TEXT,
                  provider TEXT,
                  ingest_ts_ms INTEGER,
                  PRIMARY KEY (ts_ms, symbol)
                )
                """
            )
            con.execute(
                "CREATE INDEX idx_prices_symbol_ts ON prices(symbol, ts_ms DESC)"
            )
            con.execute(
                """
                INSERT INTO prices(ts_ms, symbol, price, source, provider, ingest_ts_ms)
                VALUES (5001, 'DIA', 410.25, 'legacy_feed', 'legacy_provider', 5001)
                """
            )
            con.commit()

        self.storage._INIT_DB_READY_PATH = ""

        with patch.object(self.db_repair, "ensure_db_ok", return_value={"ok": True}), patch.object(
            self.db_repair,
            "repair_schema",
            return_value={
                "ok": True,
                "schema_version": self.storage.SCHEMA_VERSION,
                "expected_schema_version": self.storage.SCHEMA_VERSION,
            },
        ), patch.object(
            self.db_repair, "_integrity_check_rows", return_value=["ok"]
        ):
            out = self.db_repair.repair()

        self.assertTrue(bool(out.get("ok")), out)
        storage_validation = dict(out.get("storage_validation") or {})
        self.assertTrue(bool(storage_validation.get("owned_schema_ok")), storage_validation)
        self.assertEqual(dict(storage_validation.get("owned_unexpected_columns") or {}), {})
        if str(storage_validation.get("storage") or "").lower() == "postgres":
            return

        with sqlite3.connect(str(self.db_path)) as con:
            columns = {
                str(row[1]): int(row[5] or 0)
                for row in (con.execute("PRAGMA table_info(prices)").fetchall() or [])
            }
            row = con.execute(
                "SELECT ts_ms, symbol, price, px, source FROM prices"
            ).fetchone()

        self.assertEqual(set(columns), {"ts_ms", "symbol", "price", "px", "source"})
        self.assertEqual(columns["symbol"], 1)
        self.assertEqual(columns["ts_ms"], 2)
        self.assertEqual(row, (5001, "DIA", 410.25, 410.25, "legacy_feed"))


if __name__ == "__main__":
    unittest.main()
