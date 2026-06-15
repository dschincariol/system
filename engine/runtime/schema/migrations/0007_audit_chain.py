"""Tamper-evident hash-chain columns for audit ledgers."""

from __future__ import annotations
import logging

from typing import Any

from engine.audit.chain import coerce_row_for_hash, order_by_clause, table_columns
from engine.audit.hashing import compute_row_hash
from engine.runtime.schema.table_classification import audit_tables

id = 7
description = "audit ledger hash chains"
LOG = logging.getLogger(__name__)


def up(conn) -> None:
    for table_name in audit_tables():
        try:
            if not _table_exists(conn, table_name):
                continue
            _add_column(conn, table_name, "prev_hash", "BYTEA")
            _add_column(conn, table_name, "row_hash", "BYTEA")
            _backfill_table(conn, table_name)
            _set_not_null(conn, table_name, "row_hash")
            if not _column_exists(conn, table_name, "row_hash"):
                raise RuntimeError(f"audit_chain_row_hash_column_missing:{table_name}")
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{_ident(table_name)}_row_hash ON {_ident(table_name)} (row_hash)"
            )
        except Exception as exc:
            raise RuntimeError(f"audit_chain_migration_failed:{table_name}:{type(exc).__name__}:{exc}") from exc


def _backfill_table(conn, table_name: str) -> None:
    columns = table_columns(conn, table_name)
    names = [col.name for col in columns]
    if "row_hash" not in names:
        return
    order_sql = order_by_clause(conn, table_name)
    rows = conn.execute(f"SELECT * FROM {_ident(table_name)} {order_sql}").fetchall() or []
    prev_hash: bytes | None = None
    for idx, raw in enumerate(rows):
        row = coerce_row_for_hash(_row_dict(raw, names), columns)
        row_hash = compute_row_hash(prev_hash, row)
        _update_hashes(conn, table_name, row, idx, prev_hash, row_hash)
        prev_hash = row_hash


def _update_hashes(
    conn,
    table_name: str,
    row: dict[str, Any],
    index: int,
    prev_hash: bytes | None,
    row_hash: bytes,
) -> None:
    if row.get("id") is not None:
        conn.execute(
            f"UPDATE {_ident(table_name)} SET prev_hash = ?, row_hash = ? WHERE id = ?",
            (prev_hash, row_hash, row["id"]),
        )
        return
    pk_cols = [key for key in row if key not in {"prev_hash", "row_hash"} and key.endswith("_id")]
    if pk_cols:
        where = " AND ".join(f"{_ident(key)} = ?" for key in pk_cols)
        conn.execute(
            f"UPDATE {_ident(table_name)} SET prev_hash = ?, row_hash = ? WHERE {where}",
            (prev_hash, row_hash, *(row[key] for key in pk_cols)),
        )
        return
    try:
        if _try_execute(
            conn,
            f"sp_audit_ctid_{_ident(table_name)}",
            f"""
                UPDATE {_ident(table_name)}
                   SET prev_hash = ?, row_hash = ?
                 WHERE ctid = (
                   SELECT ctid FROM {_ident(table_name)} {order_by_clause(conn, table_name)} OFFSET ? LIMIT 1
                 )
                """,
            (prev_hash, row_hash, int(index)),
        ):
            return
    except Exception:
        LOG.debug("audit_chain_ctid_backfill_failed table=%s", table_name, exc_info=True)
    conn.execute(
        f"UPDATE {_ident(table_name)} SET prev_hash = ?, row_hash = ? WHERE rowid = ?",
        (prev_hash, row_hash, int(index) + 1),
    )


def _try_execute(conn, savepoint_name: str, sql: str, params=None) -> bool:
    sp = _ident(savepoint_name)
    conn.execute(f"SAVEPOINT {sp}")
    try:
        conn.execute(sql, params)
        conn.execute(f"RELEASE SAVEPOINT {sp}")
        return True
    except Exception:
        LOG.debug("audit_chain_try_execute_failed savepoint=%s sql=%s", savepoint_name, sql, exc_info=True)
        try:
            conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
        finally:
            conn.execute(f"RELEASE SAVEPOINT {sp}")
        return False


def _try_fetchone(conn, savepoint_name: str, sql: str, params=None):
    sp = _ident(savepoint_name)
    conn.execute(f"SAVEPOINT {sp}")
    try:
        row = conn.execute(sql, params).fetchone()
        conn.execute(f"RELEASE SAVEPOINT {sp}")
        return True, row
    except Exception:
        LOG.debug("audit_chain_try_fetchone_failed savepoint=%s sql=%s", savepoint_name, sql, exc_info=True)
        try:
            conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
        finally:
            conn.execute(f"RELEASE SAVEPOINT {sp}")
        return False, None


def _add_column(conn, table_name: str, column_name: str, ddl: str) -> None:
    if _column_exists(conn, table_name, column_name):
        return
    if _try_execute(
        conn,
        f"sp_add_{_ident(table_name)}_{_ident(column_name)}",
        f"ALTER TABLE IF EXISTS {_ident(table_name)} ADD COLUMN IF NOT EXISTS {_ident(column_name)} {ddl}",
    ):
        return
    conn.execute(f"ALTER TABLE {_ident(table_name)} ADD COLUMN {_ident(column_name)} {ddl}")


def _set_not_null(conn, table_name: str, column_name: str) -> None:
    _try_execute(
        conn,
        f"sp_not_null_{_ident(table_name)}_{_ident(column_name)}",
        f"ALTER TABLE {_ident(table_name)} ALTER COLUMN {_ident(column_name)} SET NOT NULL",
    )


def _table_exists(conn, table_name: str) -> bool:
    ok, row = _try_fetchone(
        conn,
        f"sp_table_exists_{_ident(table_name)}",
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = current_schema()
          AND table_name = ?
        LIMIT 1
        """,
        (str(table_name),),
    )
    if ok:
        if row:
            return True
    else:
        LOG.debug("audit_chain_information_schema_table_lookup_failed table=%s", table_name)
    try:
        row = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (str(table_name),)).fetchone()
        return bool(row)
    except Exception:
        LOG.warning("audit_chain_sqlite_table_lookup_failed table=%s", table_name, exc_info=True)
        return False


def _column_exists(conn, table_name: str, column_name: str) -> bool:
    ok, row = _try_fetchone(
        conn,
        f"sp_col_exists_{_ident(table_name)}_{_ident(column_name)}",
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = ?
          AND column_name = ?
        LIMIT 1
        """,
        (str(table_name), str(column_name)),
    )
    if ok:
        if row:
            return True
    else:
        LOG.debug("audit_chain_information_schema_column_lookup_failed table=%s column=%s", table_name, column_name)
    try:
        rows = conn.execute(f"PRAGMA table_info({_ident(table_name)})").fetchall() or []
        return any(str(row[1]) == str(column_name) for row in rows)
    except Exception:
        LOG.warning(
            "audit_chain_sqlite_column_lookup_failed table=%s column=%s",
            table_name,
            column_name,
            exc_info=True,
        )
        return False


def _row_dict(row, columns: list[str]) -> dict[str, Any]:
    if hasattr(row, "keys"):
        try:
            return {str(key): row[key] for key in row.keys()}
        except Exception:
            LOG.debug("audit_chain_row_mapping_conversion_failed", exc_info=True)
    return {str(columns[idx]): row[idx] for idx in range(min(len(columns), len(row)))}


def _ident(name: str) -> str:
    text = str(name or "")
    if not text.replace("_", "").isalnum() or (not text[:1].isalpha() and text[:1] != "_"):
        raise ValueError(f"invalid_identifier:{text}")
    return text
