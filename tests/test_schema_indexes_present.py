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

from engine.runtime.schema.table_classification import Hypertable, TABLE_CLASS
from test_schema_hypertable_creation import (
    _column_exists,
    _existing_classified_hypertables,
    _prepare_db,
    _table_exists,
)

index_migration = importlib.import_module("engine.runtime.schema.migrations.0003_indexes")


def _indexes(conn) -> dict[str, str]:
    rows = conn.execute(
        """
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE schemaname = ANY (current_schemas(false))
        """
    ).fetchall()
    return {str(row[0]): str(row[1]) for row in rows or []}


def _jsonb_columns(conn, table_name: str) -> tuple[str, ...]:
    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = ANY (current_schemas(false))
          AND table_name = ?
          AND udt_name = 'jsonb'
        """,
        (str(table_name),),
    ).fetchall()
    return tuple(str(row[0]) for row in rows or [])


def test_brin_and_symbol_time_indexes_exist() -> None:
    storage_pg = _prepare_db()
    with storage_pg.connect_ro_direct(timeout_s=1) as conn:
        indexes = _indexes(conn)
        for table_name, classification in _existing_classified_hypertables(conn).items():
            brin_name = index_migration.brin_index_name(table_name, classification.time_column)
            assert brin_name in indexes, f"{table_name} missing BRIN index {brin_name}"
            assert "USING brin" in indexes[brin_name].lower()
            if _column_exists(conn, table_name, "symbol"):
                symbol_name = index_migration.symbol_time_index_name(table_name, classification.time_column)
                assert symbol_name in indexes, f"{table_name} missing symbol/time index {symbol_name}"


def test_jsonb_gin_indexes_exist_for_hypertables() -> None:
    storage_pg = _prepare_db()
    with storage_pg.connect_ro_direct(timeout_s=1) as conn:
        indexes = _indexes(conn)
        for table_name, classification in TABLE_CLASS.items():
            if not isinstance(classification, Hypertable) or not _table_exists(conn, table_name):
                continue
            for column_name in _jsonb_columns(conn, table_name):
                index_name = index_migration.jsonb_gin_index_name(table_name, column_name)
                assert index_name in indexes, f"{table_name}.{column_name} missing GIN index"
                assert "USING gin" in indexes[index_name].lower()


def test_targeted_performance_indexes_exist() -> None:
    storage_pg = _prepare_db()
    with storage_pg.connect_ro_direct(timeout_s=1) as conn:
        indexes = _indexes(conn)
        expected = {
            "idx_model_feature_snapshots_symbol_feature_set_ts_desc",
            "idx_decision_log_reason",
            "idx_decision_log_family",
            "idx_model_promotion_audit_model_ts_desc",
            "idx_runtime_metrics_metric_ts_desc",
            "idx_job_history_job_name_ts_desc",
        }
        if _table_exists(conn, "promotion_statistical_evidence"):
            expected.add("idx_promotion_statistical_evidence_model_ts_desc")
        if _column_exists(conn, "trade_attribution_ledger", "order_id"):
            expected.add("idx_trade_attribution_ledger_order_ts_desc")
        elif _column_exists(conn, "trade_attribution_ledger", "source_alert_id"):
            expected.add("idx_trade_attribution_ledger_source_alert_ts_desc")
        missing = sorted(expected - set(indexes))
        assert not missing, "Targeted indexes missing: " + ", ".join(missing)
