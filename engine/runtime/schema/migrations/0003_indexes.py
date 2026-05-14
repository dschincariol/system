"""Performance-critical production indexes for the Timescale schema."""

from __future__ import annotations

import hashlib
import re

from engine.runtime.schema.table_classification import Hypertable, TABLE_CLASS

id = 3
description = "production hypertable and JSONB indexes"

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _ident(value: str) -> str:
    text = str(value)
    if not _IDENT_RE.match(text):
        raise ValueError(f"unsafe SQL identifier: {text!r}")
    return '"' + text.replace('"', '""') + '"'


def _index_name(*parts: str) -> str:
    raw = "_".join(re.sub(r"[^a-zA-Z0-9_]+", "_", str(part)).strip("_") for part in parts if part)
    raw = re.sub(r"_+", "_", raw.lower()).strip("_")
    if len(raw) <= 60:
        return raw
    suffix = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:8]
    return f"{raw[:51]}_{suffix}"


def brin_index_name(table_name: str, time_column: str) -> str:
    return _index_name("idx", table_name, time_column, "brin")


def symbol_time_index_name(table_name: str, time_column: str) -> str:
    return _index_name("idx", table_name, "symbol", time_column, "desc")


def jsonb_gin_index_name(table_name: str, column_name: str) -> str:
    return _index_name("idx", table_name, column_name, "gin")


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = current_schema()
          AND table_name = ?
        """,
        (str(table_name),),
    ).fetchone()
    return bool(row)


def _column_exists(conn, table_name: str, column_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM pg_attribute a
        JOIN pg_class c
          ON c.oid = a.attrelid
        JOIN pg_namespace n
          ON n.oid = c.relnamespace
        WHERE n.nspname = current_schema()
          AND c.relname = ?
          AND a.attname = ?
          AND NOT a.attisdropped
        """,
        (str(table_name), str(column_name)),
    ).fetchone()
    return bool(row)


def _columns_exist(conn, table_name: str, *column_names: str) -> bool:
    return all(_column_exists(conn, table_name, column_name) for column_name in column_names)


def _jsonb_columns(conn, table_name: str) -> tuple[str, ...]:
    rows = conn.execute(
        """
        SELECT a.attname
        FROM pg_attribute a
        JOIN pg_class c
          ON c.oid = a.attrelid
        JOIN pg_namespace n
          ON n.oid = c.relnamespace
        JOIN pg_type t
          ON t.oid = a.atttypid
        WHERE n.nspname = current_schema()
          AND c.relname = ?
          AND t.typname = 'jsonb'
          AND NOT a.attisdropped
        ORDER BY a.attnum
        """,
        (str(table_name),),
    ).fetchall()
    return tuple(str(row[0]) for row in rows or [])


def _create_brin(conn, table_name: str, time_column: str) -> None:
    if not _column_exists(conn, table_name, time_column):
        return
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS {_ident(brin_index_name(table_name, time_column))} "
        f"ON {_ident(table_name)} USING BRIN ({_ident(time_column)})"
    )


def _create_time_btree(conn, table_name: str, key_column: str, time_column: str) -> None:
    if not (_column_exists(conn, table_name, key_column) and _column_exists(conn, table_name, time_column)):
        return
    index_name = (
        symbol_time_index_name(table_name, time_column)
        if key_column == "symbol"
        else _index_name("idx", table_name, key_column, time_column, "desc")
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS {_ident(index_name)} "
        f"ON {_ident(table_name)} ({_ident(key_column)}, {_ident(time_column)} DESC)"
    )


def _create_jsonb_gin(conn, table_name: str, column_name: str) -> None:
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS {_ident(jsonb_gin_index_name(table_name, column_name))} "
        f"ON {_ident(table_name)} USING GIN ({_ident(column_name)} jsonb_path_ops)"
    )


def _create_expression_index(conn, table_name: str, index_name: str, expression_sql: str) -> None:
    if not _table_exists(conn, table_name):
        return
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS {_ident(index_name)} "
        f"ON {_ident(table_name)} ({expression_sql})"
    )


def _create_regular_index(conn, table_name: str, index_name: str, columns_sql: str) -> None:
    if not _table_exists(conn, table_name):
        return
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS {_ident(index_name)} "
        f"ON {_ident(table_name)} ({columns_sql})"
    )


def _create_hypertable_indexes(conn, table_name: str, spec: Hypertable) -> None:
    if not _table_exists(conn, table_name):
        return
    _create_brin(conn, table_name, spec.time_column)
    _create_time_btree(conn, table_name, "symbol", spec.time_column)
    for segment_column in spec.segmentby:
        if segment_column != "symbol":
            _create_time_btree(conn, table_name, str(segment_column), spec.time_column)
    for column_name in _jsonb_columns(conn, table_name):
        _create_jsonb_gin(conn, table_name, column_name)


def _create_targeted_indexes(conn) -> None:
    if _columns_exist(conn, "model_feature_snapshots", "symbol", "feature_set_tag", "ts_ms"):
        _create_regular_index(
            conn,
            "model_feature_snapshots",
            "idx_model_feature_snapshots_symbol_feature_set_ts_desc",
            '"symbol", "feature_set_tag", "ts_ms" DESC',
        )

    if _columns_exist(conn, "decision_log", "extra_json", "explain_json"):
        _create_expression_index(
            conn,
            "decision_log",
            "idx_decision_log_reason",
            "(COALESCE(extra_json->>'reason', explain_json->>'reason'))",
        )
    if _columns_exist(conn, "decision_log", "extra_json", "explain_json", "model_name"):
        _create_expression_index(
            conn,
            "decision_log",
            "idx_decision_log_family",
            "(COALESCE(extra_json->>'family', explain_json->>'family', model_name))",
        )

    if _columns_exist(conn, "model_promotion_audit", "model_name", "ts_ms"):
        _create_regular_index(
            conn,
            "model_promotion_audit",
            "idx_model_promotion_audit_model_ts_desc",
            '"model_name", "ts_ms" DESC',
        )

    if _columns_exist(conn, "promotion_statistical_evidence", "model_id", "ts"):
        _create_regular_index(
            conn,
            "promotion_statistical_evidence",
            "idx_promotion_statistical_evidence_model_ts_desc",
            '"model_id", "ts" DESC',
        )

    if _table_exists(conn, "trade_attribution_ledger"):
        if _column_exists(conn, "trade_attribution_ledger", "order_id"):
            _create_regular_index(
                conn,
                "trade_attribution_ledger",
                "idx_trade_attribution_ledger_order_ts_desc",
                '"order_id", "ts_ms" DESC',
            )
        elif _column_exists(conn, "trade_attribution_ledger", "source_alert_id"):
            _create_regular_index(
                conn,
                "trade_attribution_ledger",
                "idx_trade_attribution_ledger_source_alert_ts_desc",
                '"source_alert_id", "ts_ms" DESC',
            )

    if _columns_exist(conn, "runtime_metrics", "metric", "ts_ms"):
        _create_regular_index(
            conn,
            "runtime_metrics",
            "idx_runtime_metrics_metric_ts_desc",
            '"metric", "ts_ms" DESC',
        )

    if _columns_exist(conn, "job_history", "job_name", "ts_ms"):
        _create_regular_index(
            conn,
            "job_history",
            "idx_job_history_job_name_ts_desc",
            '"job_name", "ts_ms" DESC',
        )


def up(conn) -> None:
    for table_name, spec in sorted(TABLE_CLASS.items()):
        if isinstance(spec, Hypertable):
            _create_hypertable_indexes(conn, table_name, spec)
    _create_targeted_indexes(conn)


__all__ = [
    "brin_index_name",
    "jsonb_gin_index_name",
    "symbol_time_index_name",
    "up",
]
