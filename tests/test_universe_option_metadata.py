from __future__ import annotations

import importlib
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from engine.data.fx_instrument import parse_fx_symbol
from engine.data.options_instrument import parse_option_symbol
from engine.runtime.schema.migrator import expected_migration_ids


OPTION_METADATA_COLUMNS = (
    "opt_underlying",
    "opt_expiry",
    "opt_right",
    "opt_strike",
    "opt_multiplier",
    "opt_exercise_style",
    "opt_settlement",
    "opt_price_ccy",
)


def _reload_storage_sqlite(db_path: Path):
    env = {
        "DB_PATH": str(db_path),
        "TS_STORAGE_BACKEND": "sqlite",
        "TS_TESTING": "1",
        "TIMESCALE_ENABLED": "0",
        "SQLITE_LIVENESS_DB_ENABLED": "0",
        "SQLITE_LIVENESS_QUEUE_ENABLED": "0",
        "ASSET_CLASS_MAP_JSON": "",
    }
    with patch.dict(os.environ, env, clear=False):
        return importlib.reload(importlib.import_module("engine.runtime.storage_sqlite"))


class UniverseOptionMetadataTests(unittest.TestCase):
    def test_option_instrument_metadata_persists_with_sqlite_affinity(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            storage_sqlite = _reload_storage_sqlite(Path(tmpdir) / "options_instrument_metadata.db")
            storage_sqlite.init_db()

            import engine.data.universe as universe

            universe = importlib.reload(universe)
            con = storage_sqlite.connect_rw_direct()
            try:
                universe.upsert_symbol(con, "O:SPY240920C00450000", status="ACTIVE", score_delta=1.0)
                universe.upsert_symbol(con, "EURUSD", status="ACTIVE", score_delta=1.0)
                con.commit()

                row = con.execute(
                    """
                    SELECT symbol, asset_class, instrument_kind, opt_underlying, opt_expiry,
                           opt_right, opt_strike, opt_multiplier, opt_exercise_style,
                           opt_settlement, opt_price_ccy, session_calendar,
                           instrument_meta_source
                    FROM symbols
                    WHERE symbol='SPY240920C00450000'
                    """
                ).fetchone()

                self.assertIsNotNone(row)
                assert row is not None
                self.assertEqual(row[0], "SPY240920C00450000")
                self.assertEqual(row[1], "OPTION")
                self.assertEqual(row[2], "option")
                self.assertEqual(row[3], "SPY")
                self.assertEqual(row[4], "2024-09-20")
                self.assertEqual(row[5], "C")
                self.assertIsInstance(row[6], float)
                self.assertEqual(row[6], 450.0)
                self.assertIsInstance(row[7], float)
                self.assertGreater(float(row[7]), 0.0)
                self.assertIsInstance(row[8], str)
                self.assertIsInstance(row[9], str)
                self.assertIsInstance(row[10], str)
                self.assertIsInstance(row[11], str)
                self.assertEqual(row[12], "parser_default_unverified")

                table_info = {
                    item[1]: item[2].upper() for item in con.execute("PRAGMA table_info(symbols)").fetchall()
                }
                self.assertEqual(table_info["opt_underlying"], "TEXT")
                self.assertEqual(table_info["opt_expiry"], "TEXT")
                self.assertEqual(table_info["opt_strike"], "REAL")
                self.assertEqual(table_info["opt_multiplier"], "REAL")

                option_metadata = universe.get_instrument_metadata(con, "O:SPY240920C00450000")
                parsed_option = parse_option_symbol("SPY240920C00450000")
                self.assertIsNotNone(parsed_option)
                assert parsed_option is not None
                self.assertEqual(option_metadata, parsed_option.to_dict())
                self.assertEqual(option_metadata["contract_specs_verified"], False)
                self.assertEqual(option_metadata["multiplier_source"], "parser_default_unverified")

                fx_metadata = universe.get_instrument_metadata(con, "EURUSD")
                parsed_fx = parse_fx_symbol("EURUSD")
                self.assertIsNotNone(parsed_fx)
                assert parsed_fx is not None
                self.assertEqual(fx_metadata, parsed_fx.to_dict())
                self.assertEqual(list(fx_metadata or {}), list(parsed_fx.to_dict()))
            finally:
                con.close()

    def test_option_metadata_falls_back_to_parser_when_columns_are_missing(self) -> None:
        con = sqlite3.connect(":memory:")
        try:
            con.execute("CREATE TABLE symbols(symbol TEXT PRIMARY KEY)")
            metadata = importlib.import_module("engine.data.universe").get_instrument_metadata(
                con,
                "SPY240920C00450000",
            )
        finally:
            con.close()

        parsed = parse_option_symbol("SPY240920C00450000")
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(metadata, parsed.to_dict())

    def test_options_instrument_metadata_migration_contract(self) -> None:
        migration = importlib.import_module("engine.runtime.schema.migrations.0073_options_instrument_metadata")

        self.assertEqual(migration.id, 73)
        self.assertTrue(migration.description)
        self.assertTrue(callable(migration.up))
        ids = expected_migration_ids()
        self.assertIn(73, ids)
        self.assertEqual(ids, tuple(sorted(ids)))
        self.assertEqual(ids, tuple(range(1, max(ids) + 1)))

        text = Path("engine/runtime/schema/migrations/0073_options_instrument_metadata.py").read_text(
            encoding="utf-8",
        )
        for column in OPTION_METADATA_COLUMNS:
            with self.subTest(column=column):
                self.assertIn(column, text)
        self.assertIn("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS", text)


if __name__ == "__main__":
    unittest.main()
