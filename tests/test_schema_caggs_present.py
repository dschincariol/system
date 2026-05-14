from __future__ import annotations

import importlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
TESTS_DIR = ROOT / "tests"
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from test_schema_hypertable_creation import _prepare_db

cagg_migration = importlib.import_module("engine.runtime.schema.migrations.0004_continuous_aggregates")


def test_continuous_aggregates_exist() -> None:
    storage_pg = _prepare_db()
    with storage_pg.connect_ro_direct(timeout_s=1) as conn:
        rows = conn.execute(
            """
            SELECT view_name
            FROM timescaledb_information.continuous_aggregates
            WHERE view_schema = ANY (current_schemas(false))
            """
        ).fetchall()
        actual = {str(row[0]) for row in rows or []}
        missing = sorted(set(cagg_migration.CONTINUOUS_AGGREGATES) - actual)
        assert not missing, "Continuous aggregates missing: " + ", ".join(missing)


def test_continuous_aggregate_refresh_policies_are_armed() -> None:
    storage_pg = _prepare_db()
    with storage_pg.connect_ro_direct(timeout_s=1) as conn:
        rows = conn.execute(
            """
            SELECT hypertable_name, config::text
            FROM timescaledb_information.jobs
            WHERE proc_name = 'policy_refresh_continuous_aggregate'
            """
        ).fetchall()
        policy_text = "\n".join(f"{row[0]} {row[1]}" for row in rows or [])
        missing = [
            view_name
            for view_name in cagg_migration.CONTINUOUS_AGGREGATES
            if view_name not in policy_text
        ]
        assert not missing, "Refresh policies missing for: " + ", ".join(missing)
