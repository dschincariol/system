from __future__ import annotations

import importlib
from pathlib import Path

from engine.runtime.schema.migrator import expected_migration_ids


CORPORATE_ACTION_COLUMNS = (
    "corporate_actions",
    "symbol",
    "action_type",
    "ex_date",
    "ex_ts_ms",
    "pay_date",
    "pay_ts_ms",
    "record_date",
    "cash_amount",
    "split_from",
    "split_to",
    "currency",
    "availability_ts_ms",
    "source",
    "source_record_id",
    "ingested_ts_ms",
    "payload_json",
    "diagnostics_json",
)


def test_corporate_actions_migration_contract() -> None:
    migration = importlib.import_module("engine.runtime.schema.migrations.0076_corporate_actions")

    assert migration.id == 76
    assert migration.description
    assert callable(migration.up)


def test_expected_migration_ids_include_corporate_actions_contiguously() -> None:
    ids = expected_migration_ids()

    assert 76 in ids
    assert ids == tuple(sorted(ids))
    assert ids == tuple(range(1, max(ids) + 1))


def test_corporate_actions_migration_source_mentions_contract() -> None:
    text = Path("engine/runtime/schema/migrations/0076_corporate_actions.py").read_text(encoding="utf-8")

    for column in CORPORATE_ACTION_COLUMNS:
        assert column in text
    assert "uq_corporate_actions_source_record_id" in text
    assert "CREATE UNIQUE INDEX IF NOT EXISTS" in text
