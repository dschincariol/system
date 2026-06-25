from __future__ import annotations

import importlib
from pathlib import Path

from engine.runtime.schema.migrator import expected_migration_ids


FX_METADATA_COLUMNS = (
    "instrument_kind",
    "base_ccy",
    "quote_ccy",
    "pip_size",
    "contract_size",
    "pnl_ccy",
    "leverage_cap",
    "session_calendar",
    "instrument_meta_source",
)


def test_fx_migration_slot_70_is_deliberately_occupied() -> None:
    migration = importlib.import_module("engine.runtime.schema.migrations.0070_data_source_populate_evidence")

    assert migration.id == 70
    assert "populate" in migration.description
    text = Path("engine/runtime/schema/migrations/0070_data_source_populate_evidence.py").read_text(
        encoding="utf-8",
    )
    assert "data_source_populate_evidence" in text


def test_fx_instrument_metadata_migration_contract() -> None:
    migration = importlib.import_module("engine.runtime.schema.migrations.0071_fx_instrument_metadata")

    assert migration.id == 71
    assert migration.description
    assert callable(migration.up)


def test_expected_migration_ids_include_fx_metadata_migration_contiguously() -> None:
    ids = expected_migration_ids()

    assert 70 in ids
    assert 71 in ids
    assert ids == tuple(sorted(ids))
    assert ids == tuple(range(1, max(ids) + 1))


def test_fx_instrument_metadata_migration_source_mentions_all_columns() -> None:
    path = Path("engine/runtime/schema/migrations/0071_fx_instrument_metadata.py")
    text = path.read_text(encoding="utf-8")

    for column in FX_METADATA_COLUMNS:
        assert column in text
    assert "ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS" in text
