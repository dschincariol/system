from __future__ import annotations

import importlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_live_ingestion_coordination_migration_declares_tables() -> None:
    migration = importlib.import_module("engine.runtime.schema.migrations.0030_live_ingestion_coordination")

    class FakeConn:
        def __init__(self) -> None:
            self.statements: list[str] = []

        def execute(self, sql: str, params=None):
            del params
            self.statements.append(str(sql))
            return self

    conn = FakeConn()
    migration.up(conn)
    sql = "\n".join(conn.statements)

    assert "CREATE TABLE IF NOT EXISTS price_feed_lock" in sql
    assert "CREATE TABLE IF NOT EXISTS options_symbol_ingestion_state" in sql
    assert "idx_options_symbol_ingestion_disabled" in sql
    assert "ADD COLUMN IF NOT EXISTS pid BIGINT NOT NULL DEFAULT 0" in sql
