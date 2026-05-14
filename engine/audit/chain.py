"""Append API for tamper-evident audit hash chains."""

from __future__ import annotations

import hashlib
import json
import re
import threading
from dataclasses import dataclass
from json import JSONDecodeError
from typing import Any, Iterable, Mapping

from psycopg.errors import UndefinedFunction

from engine.audit.hashing import compute_row_hash
from engine.runtime.dbapi_compat import is_sqlite_connection, is_sqlite_error

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_LOCKS: dict[str, threading.RLock] = {}
_LOCKS_GUARD = threading.Lock()


@dataclass(frozen=True)
class ColumnInfo:
    name: str
    type_name: str = ""
    notnull: bool = False
    pk: int = 0


@dataclass(frozen=True)
class ChainResult:
    table: str
    row_id: int | None
    prev_hash: bytes | None
    row_hash: bytes

    @property
    def prev_hash_hex(self) -> str | None:
        return self.prev_hash.hex() if self.prev_hash else None

    @property
    def row_hash_hex(self) -> str:
        return self.row_hash.hex()


def append_chain_row(table: str, row: Mapping[str, Any], conn) -> ChainResult:
    """Append one audit row after taking the per-table chain lock."""

    table_name = _ident(table)
    payload = {str(k): v for k, v in dict(row or {}).items() if str(k) not in {"prev_hash", "row_hash"}}
    sqlite_owned_txn = _sqlite_owned_transaction(conn)
    lock = _table_thread_lock(table_name)
    prev_hash: bytes | None = None
    row_hash = b""
    with lock:
        try:
            if sqlite_owned_txn:
                conn.execute("BEGIN IMMEDIATE")
            _advisory_xact_lock(conn, table_name)
            columns = table_columns(conn, table_name)
            payload = coerce_row_for_hash(payload, columns)
            if "id" in {c.name for c in columns} and payload.get("id") is None:
                payload["id"] = _allocate_id(conn, table_name)
            payload = _fill_omitted_nullable_columns(payload, columns)
            prev_hash = latest_row_hash(conn, table_name)
            row_hash = compute_row_hash(prev_hash, payload)
            insert_payload = dict(payload)
            insert_payload["prev_hash"] = prev_hash
            insert_payload["row_hash"] = row_hash
            _insert_payload(conn, table_name, insert_payload, columns)
            if sqlite_owned_txn:
                conn.commit()
        except Exception:
            if sqlite_owned_txn:
                conn.rollback()
            raise
    return ChainResult(
        table=table_name,
        row_id=(int(payload["id"]) if payload.get("id") is not None else None),
        prev_hash=prev_hash,
        row_hash=row_hash,
    )


def table_columns(conn, table: str) -> list[ColumnInfo]:
    rows = conn.execute(f"PRAGMA table_info({_ident(table)})").fetchall() or []
    out: list[ColumnInfo] = []
    for row in rows:
        out.append(
            ColumnInfo(
                name=str(row[1]),
                type_name=str(row[2] or ""),
                notnull=bool(row[3]),
                pk=int(row[5] or 0),
            )
        )
    return out


def latest_row_hash(conn, table: str) -> bytes | None:
    order_by = order_by_clause(conn, table, descending=True)
    row = conn.execute(
        f"SELECT row_hash FROM {_ident(table)} WHERE row_hash IS NOT NULL {order_by} LIMIT 1"
    ).fetchone()
    return _bytes_or_none(row[0]) if row else None


def order_by_clause(conn, table: str, *, descending: bool = False) -> str:
    columns = table_columns(conn, table)
    names = {col.name for col in columns}
    direction = "DESC" if descending else "ASC"
    order: list[str] = []
    for candidate in ("ts_ms", "ts", "created_ts_ms", "timestamp"):
        if candidate in names:
            order.append(candidate)
            break
    if "id" in names:
        order.append("id")
    elif order:
        pk_cols = [col.name for col in sorted(columns, key=lambda c: c.pk) if col.pk > 0 and col.name not in order]
        order.extend(pk_cols)
    if not order:
        pk_cols = [col.name for col in sorted(columns, key=lambda c: c.pk) if col.pk > 0]
        order.extend(pk_cols)
    if not order:
        return ""
    return "ORDER BY " + ", ".join(f"{_ident(col)} {direction}" for col in order)


def row_identifier(row: Mapping[str, Any], index: int) -> int | None:
    if row.get("id") is not None:
        try:
            return int(row.get("id"))
        except (TypeError, ValueError):
            return None
    return int(index)


def _advisory_xact_lock(conn, table: str) -> None:
    try:
        conn.execute("SELECT pg_advisory_xact_lock(?)", (_lock_key(table),)).fetchone()
    except UndefinedFunction:
        # fallback: SQLite and non-Postgres test adapters have no advisory locks.
        return
    except Exception as exc:
        if is_sqlite_error(exc, "OperationalError"):
            # fallback: SQLite and non-Postgres test adapters have no advisory locks.
            return
        raise


def _lock_key(table: str) -> int:
    digest = hashlib.sha256(str(table).encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=False) & 0x7FFFFFFFFFFFFFFF


def _table_thread_lock(table: str) -> threading.RLock:
    with _LOCKS_GUARD:
        lock = _LOCKS.get(table)
        if lock is None:
            lock = threading.RLock()
            _LOCKS[table] = lock
        return lock


def _allocate_id(conn, table: str) -> int:
    try:
        row = conn.execute("SELECT nextval(pg_get_serial_sequence(?, 'id'))", (table,)).fetchone()
        if row and row[0] is not None:
            return int(row[0])
    except UndefinedFunction:
        # fallback: SQLite and non-serial audit tables allocate from MAX(id).
        return _allocate_id_from_existing_rows(conn, table)
    except Exception as exc:
        if is_sqlite_error(exc, "OperationalError"):
            # fallback: SQLite and non-serial audit tables allocate from MAX(id).
            return _allocate_id_from_existing_rows(conn, table)
        raise
    return _allocate_id_from_existing_rows(conn, table)


def _allocate_id_from_existing_rows(conn, table: str) -> int:
    row = conn.execute(f"SELECT COALESCE(MAX(id), 0) + 1 FROM {_ident(table)}").fetchone()
    return int(row[0] or 1)


def _insert_payload(conn, table: str, payload: Mapping[str, Any], column_infos: Iterable[ColumnInfo]) -> None:
    columns = [str(key) for key in payload.keys()]
    column_map = {col.name: col for col in column_infos}
    sql = (
        f"INSERT INTO {_ident(table)} ({', '.join(_ident(col) for col in columns)}) "
        f"VALUES ({', '.join(['?'] * len(columns))})"
    )
    conn.execute(sql, tuple(_storage_value(conn, col, payload[col], column_map.get(col)) for col in columns))


def coerce_row_for_hash(row: Mapping[str, Any], columns: Iterable[ColumnInfo]) -> dict[str, Any]:
    """Normalize storage-native JSON values into the canonical hash shape."""

    json_columns = {
        col.name
        for col in columns
        if "JSON" in col.type_name.upper() or col.name.endswith("_json") or col.name in {"payload", "payload_excerpt"}
    }
    out: dict[str, Any] = {}
    for key, value in dict(row or {}).items():
        if key in json_columns and isinstance(value, str):
            try:
                out[key] = json.loads(value)
                continue
            except JSONDecodeError:
                # fallback: legacy rows may store plain text in JSON-named columns.
                out[key] = value
                continue
        out[key] = value
    return out


def _fill_omitted_nullable_columns(row: Mapping[str, Any], columns: Iterable[ColumnInfo]) -> dict[str, Any]:
    out = dict(row)
    for col in columns:
        if col.name in {"prev_hash", "row_hash"}:
            continue
        if col.name in out:
            continue
        if col.pk:
            continue
        if col.notnull:
            continue
        out[col.name] = None
    return out


def _storage_value(conn, column_name: str, value: Any, column: ColumnInfo | None) -> Any:
    if isinstance(value, (dict, list)):
        type_name = str(column.type_name if column else "").upper()
        if not is_sqlite_connection(conn) and "JSON" in type_name:
            return value
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    return value


def _sqlite_owned_transaction(conn) -> bool:
    if not is_sqlite_connection(conn):
        return False
    return not bool(getattr(conn, "in_transaction", False))


def _bytes_or_none(value: Any) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return bytes(value)
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return bytes(value)
    if isinstance(value, str):
        try:
            return bytes.fromhex(value)
        except ValueError:
            return value.encode("utf-8")
    return bytes(value)


def _ident(name: str) -> str:
    text = str(name or "")
    if not _IDENT_RE.match(text):
        raise ValueError(f"invalid_identifier:{text}")
    return text
