from __future__ import annotations

import importlib
from pathlib import Path
import sys
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from engine.runtime.schema.migrator import expected_migration_ids


FUTURES_METADATA_COLUMNS = (
    "fut_root",
    "fut_exchange",
    "fut_multiplier",
    "fut_tick_size",
    "fut_tick_value",
    "fut_price_ccy",
    "fut_margin_ref",
    "fut_expiry_rule",
    "fut_roll_method",
    "fut_continuous_alias",
)


class FuturesInstrumentMigrationTests(unittest.TestCase):
    def test_futures_instrument_metadata_migration_contract(self) -> None:
        migration = importlib.import_module("engine.runtime.schema.migrations.0072_futures_contract_metadata")

        self.assertEqual(migration.id, 72)
        self.assertTrue(migration.description)
        self.assertTrue(callable(migration.up))

    def test_expected_migration_ids_include_futures_metadata_migration_contiguously(self) -> None:
        ids = expected_migration_ids()

        self.assertIn(72, ids)
        self.assertEqual(ids, tuple(sorted(ids)))
        self.assertEqual(ids, tuple(range(1, max(ids) + 1)))

    def test_futures_instrument_metadata_migration_source_mentions_all_columns(self) -> None:
        path = Path("engine/runtime/schema/migrations/0072_futures_contract_metadata.py")
        text = path.read_text(encoding="utf-8")

        for column in FUTURES_METADATA_COLUMNS:
            with self.subTest(column=column):
                self.assertIn(column, text)
        self.assertIn("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS", text)


if __name__ == "__main__":
    unittest.main()
