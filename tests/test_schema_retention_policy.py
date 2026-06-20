from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
TESTS_DIR = ROOT / "tests"
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from engine.runtime.schema.table_classification import Hypertable, TABLE_CLASS
from test_schema_hypertable_creation import _existing_classified_hypertables, _prepare_db


def test_retention_policy_exists_for_each_retained_hypertable() -> None:
    storage_pg = _prepare_db()
    with storage_pg.connect_ro_direct(timeout_s=1) as conn:
        expected = {
            table_name
            for table_name, classification in _existing_classified_hypertables(conn).items()
            if classification.retain
        }
        rows = conn.execute(
            """
            SELECT hypertable_name
            FROM timescaledb_information.jobs
            WHERE proc_name = 'policy_retention'
              AND hypertable_schema = ANY (current_schemas(false))
            """
        ).fetchall()
        actual = {str(row[0]) for row in rows or []}
        missing = sorted(expected - actual)
        assert not missing, "Retention policy missing for: " + ", ".join(missing)


def test_compliance_ledger_has_no_retention_policy() -> None:
    storage_pg = _prepare_db()
    ledger = TABLE_CLASS["trade_attribution_ledger"]
    assert isinstance(ledger, Hypertable)
    with storage_pg.connect_ro_direct(timeout_s=1) as conn:
        rows = conn.execute(
            """
            SELECT 1
            FROM timescaledb_information.jobs
            WHERE proc_name = 'policy_retention'
              AND hypertable_schema = ANY (current_schemas(false))
              AND hypertable_name = 'trade_attribution_ledger'
            """
        ).fetchall()
        assert rows == []


def test_backup_retention_scripts_are_capacity_aware() -> None:
    prune = (ROOT / "ops" / "backup" / "prune.sh").read_text(encoding="utf-8")
    accounting = (ROOT / "ops" / "backup" / "accounting.sh").read_text(encoding="utf-8")
    runbook = (ROOT / "docs" / "DISK_RETENTION_RUNBOOK.md").read_text(encoding="utf-8")

    for text in (prune, accounting, runbook):
        assert "TS_BACKUP_MAX_BYTES" in text
        assert "observed_wal_bytes_per_day" in text

    assert "backup_over_budget" in prune
    assert "backup_capacity_preflight_failed" in prune
    assert "TS_BACKUP_ENFORCE_BUDGET" in prune
    assert "TS_RESTORE_DRILL_WORK_TTL_DAYS" in prune
    assert "projected_days_to_full" in accounting
