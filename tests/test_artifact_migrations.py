from __future__ import annotations

import importlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.runtime.schema.table_classification import regular_tables


class RecordingConn:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, sql: str, params=None):
        del params
        self.statements.append(str(sql))
        return self


def test_artifact_migration_declares_tables_and_indexes() -> None:
    migration = importlib.import_module("engine.runtime.schema.migrations.0005_artifacts")
    conn = RecordingConn()

    migration.up(conn)
    sql = "\n".join(conn.statements)

    assert "CREATE TABLE IF NOT EXISTS artifacts" in sql
    assert "CREATE TABLE IF NOT EXISTS artifact_aliases" in sql
    assert "CREATE TABLE IF NOT EXISTS artifact_fsck_findings" in sql
    assert "artifacts_metadata_gin" in sql
    assert "artifact_aliases_current" in sql
    assert "ALTER TABLE IF EXISTS temporal_models ADD COLUMN IF NOT EXISTS artifact_sha256 TEXT" in sql
    assert "ALTER TABLE IF EXISTS ensemble_blend_weights ADD COLUMN IF NOT EXISTS meta_artifact_sha256 TEXT" in sql


def test_artifact_tables_are_regular() -> None:
    regular = regular_tables()
    assert "artifacts" in regular
    assert "artifact_aliases" in regular
    assert "artifact_fsck_findings" in regular
