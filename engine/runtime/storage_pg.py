"""Postgres implementation for the public runtime storage facade.

Connection DSNs are owned by ``storage_pool`` and platform defaults, including
Postgres role passwords loaded through ``services.secrets``.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from contextlib import contextmanager
from typing import Any, Callable, Iterable, Optional, Sequence

import psycopg
from psycopg import errors
from psycopg.pq import TransactionStatus
from psycopg.types.json import Jsonb
import sqlparse

from engine.runtime.platform import default_data_root
from engine.runtime.storage_dialect import to_pg_params
from engine.runtime.storage_pool import (
    acquire,
    close_pool,
    pool_snapshot,
    release,
    storage_readiness_snapshot,
)

LOGGER = logging.getLogger(__name__)

def _load_expected_schema_version() -> int:
    try:
        from engine.runtime.schema.migrator import expected_schema_version

        return int(expected_schema_version())
    except Exception:
        LOGGER.debug("postgres_expected_schema_version_load_failed", exc_info=True)
        return 1


SCHEMA_VERSION = _load_expected_schema_version()
DB_PATH = default_data_root()

PG_LIVENESS_DB_ENABLED = False
PG_LIVENESS_DB_PATH = DB_PATH / "liveness"

_PK_CACHE: dict[str, tuple[str, ...]] = {}
_AUTO_INIT_LOCKS_GUARD = threading.Lock()
_AUTO_INIT_LOCKS: dict[str, threading.RLock] = {}
_AUTO_INIT_SCHEMAS: set[str] = set()
_AUTO_INIT_ACTIVE_SCHEMAS: set[str] = set()


def _sqlite_compat_cell(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)
    return value


def _json_column_indexes(description: Any) -> set[int]:
    indexes: set[int] = set()
    for idx, col in enumerate(description or ()):
        type_code = getattr(col, "type_code", None)
        if type_code is None and isinstance(col, (tuple, list)) and len(col) > 1:
            type_code = col[1]
        if int(type_code or 0) in (114, 3802):
            indexes.add(int(idx))
    return indexes


def _sqlite_compat_row(values: Sequence[Any], json_indexes: set[int] | None = None) -> tuple[Any, ...]:
    indexes = json_indexes or set()
    return tuple(_sqlite_compat_cell(value) if idx in indexes else value for idx, value in enumerate(values))


class StorageRow(tuple):
    def __new__(cls, values: Sequence[Any], columns: Sequence[str] = ()):
        obj = super().__new__(cls, values)
        obj._columns = tuple(str(c) for c in columns)
        obj._index = {name: idx for idx, name in enumerate(obj._columns)}
        return obj

    def keys(self) -> tuple[str, ...]:
        return self._columns

    def __getitem__(self, key):  # type: ignore[override]
        if isinstance(key, str):
            return super().__getitem__(self._index[key])
        return super().__getitem__(key)

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except Exception:
            return default


class StorageCursor:
    def __init__(
        self,
        cursor,
        rows: Sequence[Sequence[Any]] | None = None,
        columns: Sequence[str] = (),
        *,
        lastrowid: int = 0,
        on_close: Callable[[], None] | None = None,
    ):
        self._cursor = cursor
        self._rows = [StorageRow(row, columns) for row in rows] if rows is not None else None
        self._offset = 0
        self._columns = tuple(columns)
        self.lastrowid = int(lastrowid or 0)
        self._on_close = on_close
        self._closed = False

    @property
    def rowcount(self) -> int:
        if self._rows is not None:
            return len(self._rows)
        return int(getattr(self._cursor, "rowcount", -1) or 0)

    @property
    def description(self):
        if self._rows is not None:
            return tuple((name, None, None, None, None, None, None) for name in self._columns)
        return getattr(self._cursor, "description", None)

    def _column_names(self) -> tuple[str, ...]:
        desc = getattr(self._cursor, "description", None) or ()
        return tuple(str(col.name if hasattr(col, "name") else col[0]) for col in desc)

    def _json_indexes(self) -> set[int]:
        return _json_column_indexes(getattr(self._cursor, "description", None) or ())

    def fetchone(self):
        if self._rows is not None:
            if self._offset >= len(self._rows):
                return None
            row = self._rows[self._offset]
            self._offset += 1
            return row
        row = self._cursor.fetchone()
        if row is None:
            return None
        return StorageRow(_sqlite_compat_row(row, self._json_indexes()), self._column_names())

    def fetchall(self):
        if self._rows is not None:
            rows = self._rows[self._offset :]
            self._offset = len(self._rows)
            return rows
        rows = self._cursor.fetchall()
        columns = self._column_names()
        json_indexes = self._json_indexes()
        return [StorageRow(_sqlite_compat_row(row, json_indexes), columns) for row in rows]

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            if self._cursor is not None:
                self._cursor.close()
        finally:
            if self._on_close is not None:
                self._on_close()

    def __iter__(self):
        while True:
            row = self.fetchone()
            if row is None:
                break
            yield row


class StorageNativeCursor:
    def __init__(self, con: "StorageConnection"):
        self._con = con
        self._cursor = con.raw.cursor()
        self._on_close = con._track_cursor(self._cursor)
        self._synthetic: StorageCursor | None = None
        self._lastrowid = 0
        self._closed = False

    @property
    def rowcount(self) -> int:
        if self._synthetic is not None:
            return self._synthetic.rowcount
        return int(getattr(self._cursor, "rowcount", -1) or 0)

    @property
    def lastrowid(self) -> int:
        return int(self._lastrowid or 0)

    @property
    def description(self):
        if self._synthetic is not None:
            return self._synthetic.description
        return getattr(self._cursor, "description", None)

    @property
    def connection(self):
        return self._con

    def execute(self, sql: str, params: Any = None):
        self._synthetic = _synthetic_cursor(self._con, sql, params)
        if self._synthetic is not None:
            self._lastrowid = int(self._synthetic.lastrowid or 0)
            return self
        normalized = _normalize_sql(sql, self._con.raw)
        self._cursor.execute(normalized, _normalize_params(params))
        self._lastrowid = self._con._record_lastrowid(normalized, self._cursor)
        return self

    def executemany(self, sql: str, seq_of_params: Iterable[Any]):
        self._synthetic = None
        self._cursor.executemany(
            _normalize_sql(sql, self._con.raw),
            [_normalize_params(params) for params in seq_of_params],
        )
        return self

    def _column_names(self) -> tuple[str, ...]:
        desc = getattr(self._cursor, "description", None) or ()
        return tuple(str(col.name if hasattr(col, "name") else col[0]) for col in desc)

    def _json_indexes(self) -> set[int]:
        return _json_column_indexes(getattr(self._cursor, "description", None) or ())

    def fetchone(self):
        if self._synthetic is not None:
            return self._synthetic.fetchone()
        row = self._cursor.fetchone()
        if row is None:
            return None
        return StorageRow(_sqlite_compat_row(row, self._json_indexes()), self._column_names())

    def fetchall(self):
        if self._synthetic is not None:
            return self._synthetic.fetchall()
        rows = self._cursor.fetchall()
        columns = self._column_names()
        json_indexes = self._json_indexes()
        return [StorageRow(_sqlite_compat_row(row, json_indexes), columns) for row in rows]

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._cursor.close()
        finally:
            self._on_close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


class StorageConnection:
    row_factory = StorageRow

    def __init__(self, raw, *, readonly: bool = False, timeout_s: float | None = None):
        self._raw = raw
        self.readonly = bool(readonly)
        self._closed = False
        self._after_commit: list[Callable[[], None]] = []
        self._timeout_s = timeout_s
        self._lastrowid = 0
        self._open_cursors: list[Any] = []

    @property
    def raw(self):
        return self._raw

    @property
    def in_transaction(self) -> bool:
        try:
            return self._raw.info.transaction_status != TransactionStatus.IDLE
        except Exception:
            return False

    def begin_managed_write(self) -> None:
        if self.readonly:
            raise psycopg.OperationalError("write_transaction_not_allowed_on_readonly_connection")
        if not self.in_transaction:
            with self._raw.cursor() as cur:
                cur.execute("BEGIN")

    def execute(self, sql: str, params: Any = None):
        synthetic = _synthetic_cursor(self, sql, params)
        if synthetic is not None:
            return synthetic
        cur = self._raw.cursor()
        on_close = self._track_cursor(cur)
        try:
            normalized = _normalize_sql(sql, self._raw)
            cur.execute(normalized, _normalize_params(params))
            lastrowid = self._record_lastrowid(normalized, cur)
            return StorageCursor(cur, lastrowid=lastrowid, on_close=on_close)
        except Exception:
            try:
                cur.close()
            finally:
                on_close()
            raise

    def executemany(self, sql: str, seq_of_params: Iterable[Any]):
        cur = self._raw.cursor()
        on_close = self._track_cursor(cur)
        try:
            cur.executemany(_normalize_sql(sql, self._raw), [_normalize_params(params) for params in seq_of_params])
            return StorageCursor(cur, on_close=on_close)
        except Exception:
            try:
                cur.close()
            finally:
                on_close()
            raise

    def executescript(self, sql_script: str):
        last = None
        for statement in sqlparse.split(str(sql_script or "")):
            text = statement.strip()
            if not text:
                continue
            last = self.execute(text)
        return last or StorageCursor(None, [])

    def cursor(self):
        return StorageNativeCursor(self)

    def _record_lastrowid(self, sql: str, cursor) -> int:
        lastrowid = _last_insert_id(self._raw, sql, cursor)
        self._lastrowid = int(lastrowid or 0)
        return self._lastrowid

    def register_after_commit(self, callback: Callable[[], None]) -> None:
        self._after_commit.append(callback)

    def _track_cursor(self, cursor) -> Callable[[], None]:
        self._open_cursors.append(cursor)

        def _untrack() -> None:
            try:
                self._open_cursors.remove(cursor)
            # system-audit: ignore[silent_except] cursor may already be untracked by close/reset.
            except ValueError:
                pass

        return _untrack

    def _close_open_cursors(self) -> None:
        cursors = list(self._open_cursors)
        self._open_cursors.clear()
        for cursor in cursors:
            try:
                cursor.close()
            except Exception:
                logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)

    def commit(self) -> None:
        self._raw.commit()
        callbacks = list(self._after_commit)
        self._after_commit.clear()
        for callback in callbacks:
            callback()

    def rollback(self) -> None:
        self._after_commit.clear()
        self._raw.rollback()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            try:
                self._close_open_cursors()
            finally:
                if self.in_transaction:
                    self._raw.rollback()
        finally:
            release(self._raw)

    def transaction(self):
        return self._raw.transaction()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                self.commit()
            else:
                self.rollback()
        finally:
            self.close()
        return False


def _normalize_sql(sql: str, raw=None) -> str:
    text = to_pg_params(str(sql or ""))
    text = _rewrite_insert_or_ignore(text)
    text = _rewrite_insert_or_replace(text, raw)
    text = _rewrite_json_extract(text)
    text = re.sub(
        r"CAST\(\s*strftime\(\s*(['\"])%s\1\s*,\s*(['\"])now\2\s*\)\s+AS\s+INTEGER\s*\)",
        "(EXTRACT(EPOCH FROM now())::BIGINT)",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"strftime\(\s*(['\"])%s\1\s*,\s*(['\"])now\2\s*\)",
        "(EXTRACT(EPOCH FROM now())::BIGINT)",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"\bINTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT\b",
        "BIGSERIAL PRIMARY KEY",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"\bAUTOINCREMENT\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bBLOB\b", "BYTEA", text, flags=re.IGNORECASE)
    text = re.sub(r"\bDATETIME\b", "TIMESTAMPTZ", text, flags=re.IGNORECASE)
    ddl_probe = re.sub(r"(?m)^\s*--.*(?:\r?\n|$)", "", text).lstrip()
    if re.match(r"\s*(CREATE\s+TABLE|ALTER\s+TABLE)\b", ddl_probe, re.IGNORECASE):
        text = re.sub(r"\bINTEGER\b", "BIGINT", text, flags=re.IGNORECASE)
    if (
        re.match(r"\s*SELECT\s+COUNT\(\*\)\s*,", text, re.IGNORECASE)
        and not re.search(r"\b(GROUP|ORDER|LIMIT)\s+BY\b|\bLIMIT\b", text, re.IGNORECASE)
    ):
        match = re.match(
            r"\s*SELECT\s+COUNT\(\*\)\s*,\s*(?P<cols>.+?)\s+FROM\s+(?P<table>[A-Za-z_][A-Za-z0-9_]*)\s+WHERE\s+(?P<where>.+?)\s*;?\s*$",
            text,
            re.IGNORECASE | re.DOTALL,
        )
        if match:
            columns = str(match.group("cols")).strip()
            if not re.search(r"\b(COUNT|SUM|AVG|MIN|MAX)\s*\(", columns, re.IGNORECASE):
                text = text.rstrip().rstrip(";") + f" GROUP BY {columns}"
    return text


def _primary_key_columns(raw, table: str) -> tuple[str, ...]:
    table_name = str(table or "")
    if table_name in _PK_CACHE:
        return _PK_CACHE[table_name]
    if raw is None:
        return ()
    try:
        rows = raw.execute(
            """
            SELECT k.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage k
              ON k.table_schema = tc.table_schema
             AND k.table_name = tc.table_name
             AND k.constraint_name = tc.constraint_name
            WHERE tc.table_schema = ANY (current_schemas(false))
              AND tc.table_name = %s
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY k.ordinal_position
            """,
            (table_name,),
        ).fetchall()
        columns = tuple(str(row[0]) for row in rows or [])
    except Exception:
        columns = ()
    _PK_CACHE[table_name] = columns
    return columns


def _identifier_csv(columns: Sequence[str]) -> str:
    return ", ".join(_ident(str(column)) for column in columns)


def _rewrite_insert_or_ignore(sql: str) -> str:
    if not re.search(r"\bINSERT\s+OR\s+IGNORE\s+INTO\b", str(sql or ""), re.IGNORECASE):
        return str(sql or "")
    text = re.sub(r"\bINSERT\s+OR\s+IGNORE\s+INTO\b", "INSERT INTO", str(sql or ""), flags=re.IGNORECASE)
    if "ON CONFLICT" in text.upper():
        return text
    return text.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"


def _rewrite_insert_or_replace(sql: str, raw=None) -> str:
    if not re.search(r"\bINSERT\s+OR\s+REPLACE\s+INTO\b", str(sql or ""), re.IGNORECASE):
        return str(sql or "")
    text = re.sub(r"\bINSERT\s+OR\s+REPLACE\s+INTO\b", "INSERT INTO", str(sql or ""), flags=re.IGNORECASE)
    if "ON CONFLICT" in text.upper():
        return text
    match = re.match(
        r"\s*INSERT\s+INTO\s+(?P<table>[A-Za-z_][A-Za-z0-9_]*)\s*\((?P<columns>.*?)\)\s*VALUES\b",
        text,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return text.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
    table = str(match.group("table"))
    insert_columns = tuple(
        str(part).strip().strip('"')
        for part in str(match.group("columns") or "").split(",")
        if str(part).strip()
    )
    pk_columns = _primary_key_columns(raw, table)
    if not pk_columns:
        return text.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
    update_columns = tuple(column for column in insert_columns if column not in set(pk_columns))
    conflict = _identifier_csv(pk_columns)
    if not update_columns:
        return text.rstrip().rstrip(";") + f" ON CONFLICT ({conflict}) DO NOTHING"
    updates = ", ".join(f"{_ident(column)}=excluded.{_ident(column)}" for column in update_columns)
    return text.rstrip().rstrip(";") + f" ON CONFLICT ({conflict}) DO UPDATE SET {updates}"


_JSON_EXTRACT_PATTERN = re.compile(
    r"json_extract\(\s*(?P<expr>[A-Za-z_][A-Za-z0-9_\.]*)\s*,\s*(?P<quote>['\"])(?P<path>\$\.[^'\"]+)(?P=quote)\s*\)",
    re.IGNORECASE,
)


def _json_path_text_expr(expr: str, path: str) -> str:
    parts = [part for part in str(path or "").removeprefix("$.").split(".") if part]
    if not parts:
        return "NULL"
    pg_path = ",".join(part.replace("\\", "\\\\").replace('"', '\\"').replace(",", "\\,") for part in parts)
    return f"(NULLIF(({expr})::text, '')::jsonb #>> '{{{pg_path}}}')"


def _rewrite_json_extract(sql: str) -> str:
    def _json_extract_match() -> str:
        return (
            r"json_extract\(\s*(?P<expr>[A-Za-z_][A-Za-z0-9_\.]*)\s*,\s*"
            r"(?P<quote>['\"])(?P<path>\$\.[^'\"]+)(?P=quote)\s*\)"
        )

    numeric_coalesce = re.compile(
        r"COALESCE\(\s*"
        + _json_extract_match()
        + r"\s*,\s*(?P<default>[+-]?\d+(?:\.\d+)?)\s*\)",
        re.IGNORECASE,
    )

    def repl_numeric_coalesce(match: re.Match[str]) -> str:
        json_text = _json_path_text_expr(match.group("expr"), match.group("path"))
        return f"COALESCE(({json_text})::DOUBLE PRECISION, {match.group('default')})"

    text = numeric_coalesce.sub(repl_numeric_coalesce, str(sql or ""))

    numeric_compare = re.compile(
        _json_extract_match() + r"\s*=\s*(?P<value>[+-]?\d+(?:\.\d+)?)",
        re.IGNORECASE,
    )

    def repl_numeric_compare(match: re.Match[str]) -> str:
        json_text = _json_path_text_expr(match.group("expr"), match.group("path"))
        return f"(({json_text})::DOUBLE PRECISION = {match.group('value')})"

    text = numeric_compare.sub(repl_numeric_compare, text)
    return _JSON_EXTRACT_PATTERN.sub(
        lambda match: _json_path_text_expr(match.group("expr"), match.group("path")),
        text,
    )


def _normalize_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return Jsonb(value)
    if isinstance(value, memoryview):
        return bytes(value)
    return value


def _normalize_params(params: Any) -> Any:
    if params is None:
        return None
    if isinstance(params, dict):
        return {str(key): _normalize_value(value) for key, value in params.items()}
    if isinstance(params, (tuple, list)):
        return tuple(_normalize_value(value) for value in params)
    return params


def _insert_table_name(sql: str) -> str | None:
    ident = r'(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_]*)'
    match = re.match(
        rf"\s*INSERT\s+(?:OR\s+\w+\s+)?INTO\s+(?:{ident}\.)?({ident})\b",
        str(sql or ""),
        re.IGNORECASE,
    )
    if not match:
        return None
    return str(match.group(1)).strip('"')


def _last_insert_id(raw, sql: str, cursor) -> int:
    table = _insert_table_name(sql)
    if not table or int(getattr(cursor, "rowcount", 0) or 0) <= 0:
        return 0
    try:
        with raw.transaction():
            row = raw.execute(
                """
                SELECT pg_get_serial_sequence(format('%%I.%%I', current_schema(), %s::text), 'id')
                WHERE EXISTS (
                      SELECT 1
                      FROM pg_class c
                      JOIN pg_namespace n
                        ON n.oid = c.relnamespace
                      JOIN pg_attribute a
                        ON a.attrelid = c.oid
                       AND a.attname = 'id'
                       AND NOT a.attisdropped
                      WHERE n.nspname = current_schema()
                        AND c.relname = %s
                        AND c.relkind IN ('r', 'p')
                  )
                  AND EXISTS (
                      SELECT 1
                      FROM pg_attrdef d
                      JOIN pg_class c
                        ON c.oid = d.adrelid
                      JOIN pg_namespace n
                        ON n.oid = c.relnamespace
                      JOIN pg_attribute a
                        ON a.attrelid = c.oid
                       AND a.attnum = d.adnum
                      WHERE n.nspname = current_schema()
                        AND c.relname = %s
                        AND a.attname = 'id'
                        AND pg_get_expr(d.adbin, d.adrelid) LIKE 'nextval%%'
                  )
                """,
                (str(table), str(table), str(table)),
            ).fetchone()
            sequence_name = str(row[0] or "") if row else ""
            if not sequence_name:
                return 0
            seq_row = raw.execute("SELECT currval(%s::regclass)", (sequence_name,)).fetchone()
            return int(seq_row[0] or 0) if seq_row else 0
    except Exception:
        return 0


def _synthetic_cursor(con: StorageConnection, sql: str, params: Any = None) -> StorageCursor | None:
    text = str(sql or "").strip()
    if re.match(r"SELECT\s+last_insert_rowid\(\)\s*;?$", text, re.IGNORECASE):
        return StorageCursor(None, [(int(con._lastrowid or 0),)], ("last_insert_rowid",), lastrowid=con._lastrowid)
    if re.match(r"PRAGMA\s+quick_check\s*;?$", text, re.IGNORECASE):
        return StorageCursor(None, [("ok",)], ("quick_check",))
    if re.match(r"PRAGMA\s+database_list\s*;?$", text, re.IGNORECASE):
        db_identity = str(os.environ.get("DB_PATH") or DB_PATH)
        return StorageCursor(None, [(0, "main", db_identity)], ("seq", "name", "file"))
    match = re.match(r"PRAGMA\s+index_list\((?P<table>[A-Za-z_][A-Za-z0-9_]*)\)\s*;?$", text, re.IGNORECASE)
    if match:
        table = match.group("table")
        rows = _pg_index_list(con, table)
        return StorageCursor(None, rows, ("seq", "name", "unique", "origin", "partial"))
    match = re.match(r"PRAGMA\s+table_info\((?P<table>[A-Za-z_][A-Za-z0-9_]*)\)\s*;?$", text, re.IGNORECASE)
    if match:
        table = match.group("table")
        rows = _pg_table_info(con, table)
        return StorageCursor(None, rows, ("cid", "name", "type", "notnull", "dflt_value", "pk"))
    if "sqlite_master" in text.lower():
        object_type = "BASE TABLE"
        if "type='index'" in text.lower() or 'type="index"' in text.lower():
            rows = _pg_index_lookup(con, params)
            return StorageCursor(None, rows, ("name",))
        rows = _pg_table_lookup(con, params, object_type=object_type)
        return StorageCursor(None, rows, ("name",))
    return None


def _pg_table_info(con: StorageConnection, table: str) -> list[tuple[Any, ...]]:
    rows = con.raw.execute(
        """
        SELECT
          a.attnum - 1 AS cid,
          a.attname AS column_name,
          UPPER(format_type(a.atttypid, a.atttypmod)) AS data_type,
          CASE WHEN a.attnotnull THEN 1 ELSE 0 END AS notnull,
          pg_get_expr(ad.adbin, ad.adrelid) AS column_default,
          COALESCE(pk.ordinality, 0) AS pk
        FROM pg_catalog.pg_class cls
        JOIN pg_catalog.pg_namespace ns
          ON ns.oid = cls.relnamespace
        JOIN pg_catalog.pg_attribute a
          ON a.attrelid = cls.oid
         AND a.attnum > 0
         AND NOT a.attisdropped
        LEFT JOIN pg_catalog.pg_attrdef ad
          ON ad.adrelid = cls.oid
         AND ad.adnum = a.attnum
        LEFT JOIN pg_catalog.pg_index idx
          ON idx.indrelid = cls.oid
         AND idx.indisprimary
        LEFT JOIN LATERAL unnest(idx.indkey) WITH ORDINALITY AS pk(attnum, ordinality)
          ON pk.attnum = a.attnum
        WHERE ns.nspname = current_schema()
          AND cls.relname = %s
          AND cls.relkind IN ('r', 'p', 'v', 'm', 'f')
        ORDER BY a.attnum
        """,
        (str(table),),
    ).fetchall()
    return [tuple(row) for row in rows]


def _pg_table_lookup(con: StorageConnection, params: Any, *, object_type: str) -> list[tuple[str]]:
    values = tuple(params or ())
    if not values:
        rows = con.raw.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = ANY (current_schemas(false))
              AND table_type = %s
            """,
            (object_type,),
        ).fetchall()
    else:
        rows = con.raw.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = ANY (current_schemas(false))
              AND table_type = %s
              AND table_name = ANY (%s)
            """,
            (object_type, list(str(v) for v in values)),
        ).fetchall()
    return [(str(row[0]),) for row in rows]


def _pg_index_lookup(con: StorageConnection, params: Any) -> list[tuple[str]]:
    values = tuple(params or ())
    if not values:
        rows = con.raw.execute(
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = ANY (current_schemas(false))
            ORDER BY indexname
            """
        ).fetchall()
    else:
        rows = con.raw.execute(
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = ANY (current_schemas(false))
              AND indexname = ANY (%s)
            ORDER BY indexname
            """,
            (list(str(v) for v in values),),
        ).fetchall()
    return [(str(row[0]),) for row in rows]


def _pg_index_list(con: StorageConnection, table: str) -> list[tuple[Any, ...]]:
    rows = con.raw.execute(
        """
        SELECT
          row_number() OVER (ORDER BY idx.relname) - 1 AS seq,
          idx.relname AS name,
          CASE WHEN i.indisunique THEN 1 ELSE 0 END AS is_unique,
          CASE WHEN i.indisprimary THEN 'pk' ELSE 'c' END AS origin,
          CASE WHEN i.indpred IS NULL THEN 0 ELSE 1 END AS partial
        FROM pg_index i
        JOIN pg_class tbl
          ON tbl.oid = i.indrelid
        JOIN pg_namespace n
          ON n.oid = tbl.relnamespace
        JOIN pg_class idx
          ON idx.oid = i.indexrelid
        WHERE tbl.oid = to_regclass(%s)
          AND n.nspname = ANY (current_schemas(false))
        ORDER BY idx.relname
        """,
        (str(table),),
    ).fetchall()
    return [(int(row[0]), str(row[1]), int(row[2]), str(row[3]), int(row[4])) for row in rows or []]


def _env_truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _autoinit_schema_key(schema: str | None = None) -> str:
    from engine.runtime.storage_pool import schema_name, validate_schema_name

    if schema is not None:
        return validate_schema_name(str(schema))
    return schema_name()


def _autoinit_lock(schema: str) -> threading.RLock:
    key = _autoinit_schema_key(schema)
    with _AUTO_INIT_LOCKS_GUARD:
        lock = _AUTO_INIT_LOCKS.get(key)
        if lock is None:
            lock = threading.RLock()
            _AUTO_INIT_LOCKS[key] = lock
        return lock


def _ensure_autoinit_schema() -> None:
    if not _env_truthy(os.environ.get("TRADING_PG_AUTOINIT_ON_CONNECT")):
        return

    schema = _autoinit_schema_key()
    if schema in _AUTO_INIT_SCHEMAS or schema in _AUTO_INIT_ACTIVE_SCHEMAS:
        return

    with _autoinit_lock(schema):
        if schema in _AUTO_INIT_SCHEMAS or schema in _AUTO_INIT_ACTIVE_SCHEMAS:
            return
        init_db(schema)


def connect(readonly: bool = False, **_: Any) -> StorageConnection:
    skip_autoinit = bool(_.pop("_skip_autoinit", False))
    if not skip_autoinit:
        _ensure_autoinit_schema()
    timeout_s = _.get("timeout_s")
    return StorageConnection(acquire(timeout_s=timeout_s), readonly=bool(readonly), timeout_s=timeout_s)


def connect_ro() -> StorageConnection:
    return connect(readonly=True)


def connect_ro_direct(**kwargs: Any) -> StorageConnection:
    return connect(readonly=True, **kwargs)


def connect_rw_direct(**kwargs: Any) -> StorageConnection:
    kwargs.setdefault("_skip_autoinit", True)
    return connect(readonly=False, **kwargs)


def connect_liveness_ro_direct(**kwargs: Any) -> StorageConnection:
    return connect_ro_direct(**kwargs)


def connect_liveness_rw_direct(**kwargs: Any) -> StorageConnection:
    return connect_rw_direct(**kwargs)


@contextmanager
def connection(readonly: bool = False):
    con = connect(readonly=readonly)
    try:
        yield con
    finally:
        con.close()


@contextmanager
def transaction(readonly: bool = False):
    con = connect(readonly=readonly)
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def execute(sql: str, params: Any = None):
    with transaction() as con:
        return con.execute(sql, params)


def executemany(sql: str, seq_of_params: Iterable[Any]):
    with transaction() as con:
        return con.executemany(sql, seq_of_params)


def fetch_one(sql: str, params: Any = None):
    with connection(readonly=True) as con:
        return con.execute(sql, params).fetchone()


def fetch_all(sql: str, params: Any = None):
    with connection(readonly=True) as con:
        return con.execute(sql, params).fetchall()


def _is_transient_pg_error(exc: BaseException) -> bool:
    return isinstance(
        exc,
        (
            psycopg.OperationalError,
            errors.SerializationFailure,
            errors.DeadlockDetected,
            errors.LockNotAvailable,
        ),
    )


def run_write_txn(
    fn: Callable[[StorageConnection], Any],
    *,
    table: str | None = None,
    operation: str | None = None,
    context: dict[str, Any] | None = None,
    attempts: int | None = None,
    direct: bool = False,
    maintenance: bool = True,
    timeout_s: float | None = None,
    busy_timeout_ms: int | None = None,
):
    del table, operation, context, direct, maintenance, busy_timeout_ms
    total_attempts = max(1, int(attempts or os.environ.get("TS_PG_WRITE_RETRY_ATTEMPTS", "3") or 3))
    last_error: BaseException | None = None
    for attempt in range(total_attempts):
        con = connect(readonly=False, timeout_s=timeout_s)
        try:
            result = fn(con)
            con.commit()
            return result
        except Exception as exc:
            last_error = exc
            try:
                con.rollback()
            except Exception:
                logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)
            if (not _is_transient_pg_error(exc)) or attempt >= total_attempts - 1:
                raise
            time.sleep(min(1.0, 0.05 * (2**attempt)))
        finally:
            con.close()
    if last_error is not None:
        raise last_error
    return None


def register_after_commit(con: StorageConnection | None, callback: Callable[[], None]) -> None:
    if con is None:
        callback()
        return
    register = getattr(con, "register_after_commit", None)
    if not callable(register):
        raise psycopg.ProgrammingError("after_commit_registration_not_supported_for_connection")
    register(callback)


def _safe_commit(con: StorageConnection, *, maintenance: bool = True) -> None:
    del maintenance
    con.commit()


def set_write_maintenance(con: StorageConnection, enabled: bool = True) -> None:
    del con, enabled


def note_write(con: StorageConnection, *, maintenance: bool = True) -> None:
    del con, maintenance


def checkpoint_if_due(
    con: StorageConnection | None = None,
    *,
    writes: int = 1,
    force: bool = False,
    reason: str = "manual",
) -> dict[str, Any]:
    del con, writes, force
    return {"ok": True, "reason": str(reason), "storage": "postgres", "ts_ms": int(time.time() * 1000)}


def _new_connection(**kwargs: Any) -> StorageConnection:
    return connect(**kwargs)


def _pid_is_running(pid: int) -> bool:
    try:
        pid_i = int(pid)
        if pid_i <= 0:
            return False
        try:
            import psutil

            return bool(psutil.pid_exists(pid_i))
        except Exception:
            if os.name == "nt":
                return True
            os.kill(pid_i, 0)
            return True
    except Exception:
        return False


def _raise_schema_error(code: str, error: BaseException, **extra: Any) -> None:
    raise RuntimeError(f"{code}:{type(error).__name__}:{error}:{extra or {}}")


def _table_exists(con: StorageConnection, table: str) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ANY (current_schemas(false))
          AND table_name = %s
        LIMIT 1
        """,
        (str(table),),
    ).fetchone()
    return bool(row)


def _has_column(con: StorageConnection, table: str, col: str) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = ANY (current_schemas(false))
          AND table_name = %s
          AND column_name = %s
        LIMIT 1
        """,
        (str(table), str(col)),
    ).fetchone()
    return bool(row)


def _liveness_db_path_key() -> str:
    return "postgres:liveness"


def apply_migrations() -> list[int]:
    from engine.runtime.schema.migrator import apply_migrations as _apply

    applied = _apply()
    _ensure_sqlite_compat_bigints()
    _ensure_walk_forward_registry_columns()
    _ensure_backend_compat_schema()
    _PK_CACHE.clear()
    return applied


def init_db(schema: str | None = None):
    schema_key = _autoinit_schema_key(schema)
    lock = _autoinit_lock(schema_key)
    with lock:
        if schema_key in _AUTO_INIT_ACTIVE_SCHEMAS:
            return []
        _AUTO_INIT_ACTIVE_SCHEMAS.add(schema_key)
        try:
            from engine.runtime.storage_pool import schema_name, schema_name_override

            with schema_name_override(schema_key):
                applied = apply_migrations()
                from engine.execution.execution_ledger import init_execution_ledger

                init_execution_ledger()
                _PK_CACHE.clear()
                _AUTO_INIT_SCHEMAS.add(schema_name())
                return applied
        finally:
            _AUTO_INIT_ACTIVE_SCHEMAS.discard(schema_key)


def close_pooled_connections() -> None:
    close_pool()


def get_connection_debug_snapshot() -> dict[str, Any]:
    return {"pool": pool_snapshot(), "storage": "postgres", "readiness": storage_readiness_snapshot()}


def _ensure_sqlite_compat_bigints() -> None:
    timestamp_columns = ("ts_ms", "timestamp", "created_ts_ms", "updated_ts_ms", "model_ts_ms")
    with connection(readonly=False) as con:
        rows = con.execute(
            """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND data_type = 'integer'
              AND (
                    column_name IN (%s, %s, %s, %s, %s)
                 OR RIGHT(column_name, 6) = %s
              )
            ORDER BY table_name, ordinal_position
            """,
            (*timestamp_columns, "_ts_ms"),
        ).fetchall()
        for table_name, column_name in rows or []:
            con.execute(
                f"ALTER TABLE {_ident(str(table_name))} ALTER COLUMN {_ident(str(column_name))} TYPE BIGINT"
            )
        con.commit()


def _ensure_walk_forward_registry_columns() -> None:
    with connection(readonly=False) as con:
        if _compat_table_exists(con, "walk_forward_runs"):
            con.execute("ALTER TABLE walk_forward_runs ADD COLUMN IF NOT EXISTS model_selection_json JSONB")
        if _compat_table_exists(con, "walk_forward_scores"):
            con.execute("ALTER TABLE walk_forward_scores ADD COLUMN IF NOT EXISTS model_name TEXT")
            con.execute("ALTER TABLE walk_forward_scores ADD COLUMN IF NOT EXISTS model_version TEXT")
            con.execute("ALTER TABLE walk_forward_scores ADD COLUMN IF NOT EXISTS model_kind TEXT")
        con.commit()


def _ensure_backend_compat_schema() -> None:
    with connection(readonly=False) as con:
        def _ensure_index(table: str, index: str, columns: Sequence[str]) -> None:
            if not _compat_table_exists(con, table):
                return
            available = _table_columns(con, table)
            if not set(columns).issubset(available):
                return
            con.execute(
                "CREATE INDEX IF NOT EXISTS "
                f"{_ident(index)} ON {_ident(table)} ({_identifier_csv(tuple(columns))})"
            )

        con.execute("ALTER TABLE labels ADD COLUMN IF NOT EXISTS impact_z DOUBLE PRECISION")
        con.execute("ALTER TABLE labels ADD COLUMN IF NOT EXISTS created_at_ms BIGINT")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS labels_price (
              ts_pred_ms BIGINT NOT NULL,
              ts_eval_ms BIGINT NOT NULL,
              symbol TEXT NOT NULL,
              horizon_s BIGINT NOT NULL,
              entry_price DOUBLE PRECISION NOT NULL,
              exit_price DOUBLE PRECISION NOT NULL,
              ret DOUBLE PRECISION NOT NULL,
              ret_z DOUBLE PRECISION,
              dir BIGINT,
              PRIMARY KEY(ts_pred_ms, symbol, horizon_s)
            )
            """
        )
        con.execute("CREATE INDEX IF NOT EXISTS idx_labels_price_eval ON labels_price(ts_eval_ms)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_labels_price_symbol_eval ON labels_price(symbol, ts_eval_ms)")

        if _compat_table_exists(con, "model_promotion_audit"):
            con.execute("ALTER TABLE model_promotion_audit ADD COLUMN IF NOT EXISTS details_json JSONB")
            con.execute("ALTER TABLE model_promotion_audit ADD COLUMN IF NOT EXISTS prev_hash BYTEA")
            con.execute("ALTER TABLE model_promotion_audit ADD COLUMN IF NOT EXISTS row_hash BYTEA")
            con.execute(
                "ALTER TABLE model_promotion_audit ALTER COLUMN ts_ms "
                "SET DEFAULT ((EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT)"
            )
            con.execute("ALTER TABLE model_promotion_audit ALTER COLUMN action SET DEFAULT 'audit_chain_append'")
            con.execute("ALTER TABLE model_promotion_audit ALTER COLUMN model_name SET DEFAULT ''")

        if _compat_table_exists(con, "model_hyperparameter_registry"):
            con.execute("ALTER TABLE model_hyperparameter_registry ADD COLUMN IF NOT EXISTS symbol TEXT")
            con.execute("ALTER TABLE model_hyperparameter_registry ADD COLUMN IF NOT EXISTS seed BIGINT")

        _ensure_index("execution_orders", "idx_execution_orders_submit_ts", ("submit_ts_ms",))
        _ensure_index("execution_orders", "idx_execution_orders_source_alert", ("source_alert_id",))
        _ensure_index("execution_fills", "idx_execution_fills_ts", ("fill_ts_ms",))
        _ensure_index("execution_fills", "idx_execution_fills_client", ("client_order_id",))
        con.commit()


def _ensure_alert_prediction_schema() -> None:
    with connection(readonly=False) as con:
        if _compat_table_exists(con, "alerts"):
            con.execute("ALTER TABLE alerts ADD COLUMN IF NOT EXISTS prediction_id BIGINT")
            con.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_alerts_prediction_id
                ON alerts(prediction_id, ts_ms DESC)
                """
            )
        con.commit()


_PG_REQUIRED_TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "schema_migrations": ("id", "description", "applied_at"),
    "alert_acks": ("alert_id", "acked_ts_ms", "acked_by", "source", "expires_ts_ms", "reason"),
    "alert_shelves": (
        "alert_id",
        "shelved_ts_ms",
        "expires_ts_ms",
        "shelved_by",
        "reason",
        "source",
        "severity",
        "detail_json",
    ),
    "alert_lifecycle_events": (
        "id",
        "alert_id",
        "ts_ms",
        "lifecycle_state",
        "actor",
        "reason",
        "source",
        "detail_json",
    ),
}

_PG_REQUIRED_INDEXES: tuple[str, ...] = ("idx_alert_lifecycle_events_alert_ts",)


def _current_expected_schema_version() -> int:
    try:
        from engine.runtime.schema.migrator import expected_schema_version

        return int(expected_schema_version())
    except Exception:
        LOGGER.debug("postgres_expected_schema_version_refresh_failed", exc_info=True)
        return int(SCHEMA_VERSION)


def _expected_migration_ids() -> tuple[int, ...]:
    try:
        from engine.runtime.schema.migrator import expected_migration_ids

        return tuple(int(item) for item in expected_migration_ids())
    except Exception:
        LOGGER.debug("postgres_expected_migration_ids_load_failed", exc_info=True)
        return (int(SCHEMA_VERSION),)


def _validation_contract() -> tuple[dict[str, tuple[str, ...]], tuple[str, ...]]:
    from engine.runtime.storage_sqlite import _REQUIRED_INDEXES, _REQUIRED_TABLE_COLUMNS

    required_columns = {str(table): tuple(str(col) for col in cols) for table, cols in _REQUIRED_TABLE_COLUMNS.items()}
    for table, cols in _PG_REQUIRED_TABLE_COLUMNS.items():
        existing = tuple(required_columns.get(str(table), ()))
        required_columns[str(table)] = tuple(dict.fromkeys((*existing, *(str(col) for col in cols))))
    required_indexes = tuple(dict.fromkeys((*_REQUIRED_INDEXES, *_PG_REQUIRED_INDEXES)))
    return required_columns, required_indexes


def _validation_table_columns(con: StorageConnection, table: str) -> dict[str, dict[str, Any]]:
    rows = con.execute(
        """
        SELECT
          c.column_name,
          UPPER(c.data_type) AS data_type,
          c.udt_name,
          CASE WHEN c.is_nullable = 'NO' THEN 1 ELSE 0 END AS notnull,
          c.column_default,
          COALESCE(pk.ordinal_position, 0) AS pk
        FROM information_schema.columns c
        LEFT JOIN (
          SELECT
            k.table_schema,
            k.table_name,
            k.column_name,
            k.ordinal_position
          FROM information_schema.table_constraints tc
          JOIN information_schema.key_column_usage k
            ON k.table_schema = tc.table_schema
           AND k.table_name = tc.table_name
           AND k.constraint_name = tc.constraint_name
          WHERE tc.constraint_type = 'PRIMARY KEY'
        ) pk
          ON pk.table_schema = c.table_schema
         AND pk.table_name = c.table_name
         AND pk.column_name = c.column_name
        WHERE c.table_schema = current_schema()
          AND c.table_name = ?
        ORDER BY c.ordinal_position
        """,
        (str(table),),
    ).fetchall() or []
    return {
        str(row[0]): {
            "type": str(row[1] or "").upper(),
            "udt_name": str(row[2] or "").lower(),
            "notnull": bool(row[3]),
            "default": None if row[4] is None else str(row[4]),
            "pk": int(row[5] or 0),
        }
        for row in rows
    }


def _validation_index_names(con: StorageConnection) -> set[str]:
    rows = con.execute(
        """
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = current_schema()
        UNION
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n
          ON n.oid = c.relnamespace
        WHERE n.nspname = current_schema()
          AND c.relkind IN ('i', 'I')
        """
    ).fetchall() or []
    return {str(row[0]) for row in rows}


def _expected_type_matches_pg(*, expected: str, actual: str, udt_name: str, column_name: str) -> bool:
    expected_type = str(expected or "").strip().upper()
    actual_type = str(actual or "").strip().upper()
    actual_udt = str(udt_name or "").strip().lower()
    column = str(column_name or "").strip().lower()
    if expected_type == "INTEGER":
        return actual_type in {"SMALLINT", "INTEGER", "BIGINT"} or actual_udt in {"int2", "int4", "int8"}
    if expected_type == "REAL":
        return actual_type in {"REAL", "DOUBLE PRECISION", "NUMERIC"} or actual_udt in {"float4", "float8", "numeric"}
    if expected_type == "TEXT":
        if actual_type in {"TEXT", "CHARACTER VARYING", "CHARACTER"} or actual_udt in {"text", "varchar", "bpchar"}:
            return True
        return column.endswith("_json") and actual_type in {"JSON", "JSONB"} and actual_udt in {"json", "jsonb"}
    if expected_type == "BLOB":
        return actual_type == "BYTEA" or actual_udt == "bytea"
    return expected_type == actual_type or expected_type == actual_udt.upper()


def get_db_validation_snapshot(*, include_quick_check: bool = True, strict: bool = False) -> dict[str, Any]:
    del include_quick_check
    have_tables: list[str] = []
    missing_tables: list[str] = []
    missing_columns: dict[str, list[str]] = {}
    missing_indexes: list[str] = []
    owned_missing_tables: list[str] = []
    owned_missing_columns: dict[str, list[str]] = {}
    owned_unexpected_columns: dict[str, list[str]] = {}
    owned_type_mismatches: dict[str, dict[str, dict[str, str]]] = {}
    owned_pk_mismatches: dict[str, dict[str, dict[str, int]]] = {}
    owned_missing_indexes: dict[str, list[str]] = {}
    schema_migration_ids: list[int] = []
    missing_migration_ids: list[int] = []
    unexpected_migration_ids: list[int] = []
    schema_version: int | None = None
    schema_status = "missing"
    expected_version = _current_expected_schema_version()
    expected_ids = _expected_migration_ids()
    try:
        with connection(readonly=True) as con:
            required_columns, required_indexes = _validation_contract()
            have_tables = [
                str(row[0])
                for row in con.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = current_schema()
                      AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                    """
                ).fetchall()
            ]
            have_set = set(have_tables)
            for table, columns in required_columns.items():
                if table not in have_set:
                    missing_tables.append(table)
                    continue
                actual = _validation_table_columns(con, table)
                missing = [col for col in columns if col not in actual]
                if missing:
                    missing_columns[table] = missing

            index_names = _validation_index_names(con)
            missing_indexes = sorted(name for name in required_indexes if name not in index_names)

            if "schema_migrations" in have_set:
                schema_migration_ids = [
                    int(row[0])
                    for row in con.execute("SELECT id FROM schema_migrations ORDER BY id").fetchall()
                    if row and row[0] is not None
                ]
                schema_version = max(schema_migration_ids) if schema_migration_ids else None
                actual_ids = set(schema_migration_ids)
                expected_id_set = set(expected_ids)
                missing_migration_ids = [item for item in expected_ids if item not in actual_ids]
                unexpected_migration_ids = sorted(item for item in actual_ids if item not in expected_id_set)
                if schema_migration_ids and not missing_migration_ids and not unexpected_migration_ids:
                    schema_status = "applied"
                elif missing_migration_ids:
                    schema_status = "stale"
                elif unexpected_migration_ids:
                    schema_status = "unexpected"
                else:
                    schema_status = "empty"
            else:
                missing_migration_ids = list(expected_ids)
                schema_status = "missing_schema_migrations"

            from engine.runtime.storage_live_ingestion_schema import (
                OWNED_LIVE_TABLE_COLUMN_SPECS,
                OWNED_LIVE_TABLE_REQUIRED_INDEXES,
            )

            for table, expected_specs in OWNED_LIVE_TABLE_COLUMN_SPECS.items():
                if table not in have_set:
                    owned_missing_tables.append(table)
                    continue
                actual = _validation_table_columns(con, table)
                expected_cols = set(expected_specs)
                actual_cols = set(actual)
                missing = sorted(expected_cols - actual_cols)
                unexpected = sorted(actual_cols - expected_cols)
                if missing:
                    owned_missing_columns[table] = missing
                if unexpected:
                    owned_unexpected_columns[table] = unexpected
                type_diff: dict[str, dict[str, str]] = {}
                pk_diff: dict[str, dict[str, int]] = {}
                for column_name, expected_spec in expected_specs.items():
                    if column_name not in actual:
                        continue
                    expected_type = str((expected_spec or {}).get("type") or "")
                    actual_spec = actual[column_name]
                    if not _expected_type_matches_pg(
                        expected=expected_type,
                        actual=str(actual_spec.get("type") or ""),
                        udt_name=str(actual_spec.get("udt_name") or ""),
                        column_name=str(column_name),
                    ):
                        type_diff[column_name] = {
                            "expected": expected_type,
                            "actual": str(actual_spec.get("type") or ""),
                        }
                    actual_pk = int(actual_spec.get("pk") or 0)
                    expected_pk = int((expected_spec or {}).get("pk") or 0)
                    if actual_pk != expected_pk:
                        pk_diff[column_name] = {"expected": expected_pk, "actual": actual_pk}
                if type_diff:
                    owned_type_mismatches[table] = type_diff
                if pk_diff:
                    owned_pk_mismatches[table] = pk_diff
                missing_owned_indexes = [
                    name
                    for name in OWNED_LIVE_TABLE_REQUIRED_INDEXES.get(table, ())
                    if name not in index_names
                ]
                if missing_owned_indexes:
                    owned_missing_indexes[table] = sorted(missing_owned_indexes)

            schema_version_ok = (
                schema_version is not None
                and int(schema_version) == int(expected_version)
                and not missing_migration_ids
                and not unexpected_migration_ids
                and schema_status == "applied"
            )
            owned_drift_tables = sorted(
                set(owned_missing_tables)
                | set(owned_missing_columns)
                | set(owned_unexpected_columns)
                | set(owned_type_mismatches)
                | set(owned_pk_mismatches)
                | set(owned_missing_indexes)
            )
            owned_schema_ok = not owned_drift_tables
            ok = (
                not missing_tables
                and not missing_columns
                and not missing_indexes
                and bool(schema_version_ok)
                and bool(owned_schema_ok)
            )
        return {
            "ok": bool(ok),
            "storage": "postgres",
            "backend": "postgres",
            "have_tables": list(have_tables),
            "required_tables": list(required_columns.keys()),
            "required_columns": {table: list(cols) for table, cols in required_columns.items()},
            "required_indexes": list(required_indexes),
            "missing_tables": list(missing_tables),
            "missing_columns": dict(missing_columns),
            "missing_cols": dict(missing_columns),
            "missing_indexes": list(missing_indexes),
            "schema_version": (int(schema_version) if schema_version is not None else None),
            "expected_schema_version": int(expected_version),
            "expected_migration_ids": list(expected_ids),
            "schema_migration_ids": list(schema_migration_ids),
            "schema_migration_missing_ids": list(missing_migration_ids),
            "schema_migration_unexpected_ids": list(unexpected_migration_ids),
            "schema_version_ok": bool(schema_version_ok),
            "schema_status": str(schema_status),
            "owned_tables": list(OWNED_LIVE_TABLE_COLUMN_SPECS.keys()),
            "owned_schema_ok": bool(owned_schema_ok),
            "owned_drift_tables": list(owned_drift_tables),
            "owned_missing_tables": list(owned_missing_tables),
            "owned_missing_columns": dict(owned_missing_columns),
            "owned_unexpected_columns": dict(owned_unexpected_columns),
            "owned_type_mismatches": dict(owned_type_mismatches),
            "owned_pk_mismatches": dict(owned_pk_mismatches),
            "owned_missing_indexes": dict(owned_missing_indexes),
            "quick_check": "not_applicable",
            "ts_ms": int(time.time() * 1000),
        }
    except Exception as exc:
        if strict:
            raise
        return {
            "ok": False,
            "storage": "postgres",
            "error": f"{type(exc).__name__}: {exc}",
            "schema_version": schema_version,
            "expected_schema_version": int(expected_version),
            "expected_migration_ids": list(expected_ids),
            "schema_migration_ids": list(schema_migration_ids),
            "schema_migration_missing_ids": list(missing_migration_ids),
            "schema_migration_unexpected_ids": list(unexpected_migration_ids),
            "schema_version_ok": False,
            "schema_status": str(schema_status),
            "missing_tables": list(missing_tables),
            "missing_columns": dict(missing_columns),
            "missing_cols": dict(missing_columns),
            "missing_indexes": list(missing_indexes),
            "owned_schema_ok": False,
            "owned_drift_tables": [],
            "owned_missing_tables": list(owned_missing_tables),
            "owned_missing_columns": dict(owned_missing_columns),
            "owned_unexpected_columns": dict(owned_unexpected_columns),
            "owned_type_mismatches": dict(owned_type_mismatches),
            "owned_pk_mismatches": dict(owned_pk_mismatches),
            "owned_missing_indexes": dict(owned_missing_indexes),
            "ts_ms": int(time.time() * 1000),
        }


def _compat_table_exists(con: Any, table_name: str) -> bool:
    try:
        row = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (str(table_name),),
        ).fetchone()
        return bool(row)
    except Exception:
        sqlite_catalog_lookup_failed = True
    else:
        sqlite_catalog_lookup_failed = False
    try:
        row = con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = current_schema()
              AND table_name = ?
            LIMIT 1
            """,
            (str(table_name),),
        ).fetchone()
        return bool(row)
    except Exception:
        if sqlite_catalog_lookup_failed:
            return False
        return False


def _backfill_alert_prediction_ids(con: Any | None = None) -> int:
    """Compatibility helper retained for legacy repair callers."""

    owns_connection = con is None
    db = con or connect()
    try:
        if not _compat_table_exists(db, "alerts") or not _compat_table_exists(db, "predictions"):
            return 0
        cursor = db.execute(
            """
            UPDATE alerts
            SET prediction_id = (
                SELECT p.id
                FROM predictions p
                WHERE p.event_id = alerts.event_id
                  AND (p.symbol = alerts.symbol OR p.symbol IS NULL OR alerts.symbol IS NULL)
                  AND (p.horizon_s = alerts.horizon_s OR p.horizon_s IS NULL OR alerts.horizon_s IS NULL)
                ORDER BY p.id DESC
                LIMIT 1
            )
            WHERE prediction_id IS NULL
              AND event_id IS NOT NULL
            """
        )
        if hasattr(db, "commit"):
            db.commit()
        return max(0, int(getattr(cursor, "rowcount", 0) or 0))
    finally:
        if owns_connection:
            db.close()


def get_db_debug_snapshot(*, include_quick_check: bool = True) -> dict[str, Any]:
    return {
        "storage": "postgres",
        "db_path": str(DB_PATH),
        "pool": pool_snapshot(),
        "readiness": storage_readiness_snapshot(),
        "db_validation": get_db_validation_snapshot(include_quick_check=include_quick_check),
        "ts_ms": int(time.time() * 1000),
    }


def get_timescale_client():
    from engine.runtime.timescale_client import get_timescale_client as _get_timescale_client

    return _get_timescale_client()


def init_timeseries_storage() -> dict[str, Any]:
    return get_timeseries_storage_snapshot()


def shutdown_timeseries_storage(timeout_s: float | None = None) -> dict[str, Any]:
    del timeout_s
    return get_timeseries_storage_snapshot()


def get_timeseries_storage_snapshot() -> dict[str, Any]:
    snapshot: dict[str, Any] = {"enabled": False, "ok": True, "detail": "postgres_runtime_storage"}
    try:
        from engine.data.feature_store import get_feature_store_snapshot

        snapshot["market_feature_store"] = get_feature_store_snapshot(timescale_snapshot=snapshot)
    except Exception as exc:
        snapshot["market_feature_store"] = {
            "ok": False,
            "degraded": True,
            "degraded_reasons": [str(exc)],
        }
    return snapshot


def _json_payload(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return value


def _ident(name: str) -> str:
    text = str(name or "")
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", text):
        raise ValueError(f"invalid_identifier:{text}")
    return text


def _insert_dict(table: str, row: dict[str, Any], *, returning_id: bool = False, con: StorageConnection | None = None):
    table_name = _ident(table)
    clean = {str(k): v for k, v in dict(row or {}).items() if v is not None}
    if not clean:
        return 0
    columns = [_ident(col) for col in clean]
    sql = (
        f"INSERT INTO {table_name} ({', '.join(columns)}) "
        f"VALUES ({', '.join(['?'] * len(columns))})"
    )
    if returning_id:
        sql += " RETURNING id"

    def _write(db: StorageConnection):
        cur = db.execute(sql, tuple(clean[col] for col in columns))
        if returning_id:
            fetched = cur.fetchone()
            return int(fetched[0] or 0) if fetched else 0
        return int(cur.rowcount or 0)

    if con is not None:
        return _write(con)
    return run_write_txn(_write)


def _table_columns(con: StorageConnection, table: str) -> set[str]:
    try:
        return {
            str(row[1] or "")
            for row in (con.execute(f"PRAGMA table_info({_ident(table)})").fetchall() or [])
            if row and len(row) > 1
        }
    except Exception:
        logging.getLogger(__name__).debug("postgres_table_columns_probe_failed", exc_info=True)
        return set()


def _upsert_dict(
    table: str,
    row: dict[str, Any],
    *,
    conflict_column: str,
    conflict_columns: Sequence[str] | None = None,
    returning_id: bool = False,
    con: StorageConnection | None = None,
) -> int:
    table_name = _ident(table)
    conflict_names = tuple(
        str(col)
        for col in (conflict_columns or (conflict_column,))
        if str(col or "").strip()
    ) or (str(conflict_column),)
    conflict_idents = tuple(_ident(col) for col in conflict_names)
    clean = {str(k): v for k, v in dict(row or {}).items() if v is not None}
    if not clean:
        return 0
    if not str(clean.get(conflict_column) or "").strip():
        return int(_insert_dict(table, clean, returning_id=returning_id, con=con) or 0)

    columns = [_ident(col) for col in clean]
    placeholders = ", ".join(["?"] * len(columns))
    update_columns = [col for col in columns if col not in set(conflict_idents)]
    conflict_sql_columns = ", ".join(conflict_idents)
    if update_columns:
        updates = ", ".join(
            f"{col}=COALESCE(excluded.{col}, {table_name}.{col})"
            for col in update_columns
        )
        conflict_sql = f"ON CONFLICT({conflict_sql_columns}) DO UPDATE SET {updates}"
    else:
        conflict_sql = f"ON CONFLICT({conflict_sql_columns}) DO NOTHING"
    sql = (
        f"INSERT INTO {table_name} ({', '.join(columns)}) "
        f"VALUES ({placeholders}) {conflict_sql}"
    )
    if returning_id:
        sql += " RETURNING id"

    def _write(db: StorageConnection) -> int:
        cur = db.execute(sql, tuple(clean[col] for col in columns))
        if returning_id:
            fetched = cur.fetchone()
            return int(fetched[0] or 0) if fetched else 0
        return int(cur.rowcount or 0)

    if con is not None:
        return _write(con)
    return int(run_write_txn(_write) or 0)


def put_event(ts_ms, source, title, body, url, event_key, meta_json=None):
    return _insert_dict(
        "events",
        {
            "ts_ms": int(ts_ms or 0),
            "timestamp": int(ts_ms or 0),
            "source": str(source or ""),
            "title": str(title or ""),
            "body": body,
            "url": url,
            "event_key": str(event_key or ""),
            "meta_json": _json_payload(meta_json),
        },
        returning_id=True,
    )


def put_normalized_event(event: dict[str, Any], con: StorageConnection | None = None):
    payload = dict(event or {})
    observed_ts_ms = int(time.time() * 1000)
    source_ts_ms = int(payload.get("timestamp") or payload.get("ts_ms") or observed_ts_ms)
    meta_payload = payload.get("meta_json") or payload
    if isinstance(meta_payload, str):
        try:
            meta_payload = json.loads(meta_payload)
        except json.JSONDecodeError:
            meta_payload = {"raw_meta_json": meta_payload}
    meta = dict(meta_payload or {}) if isinstance(meta_payload, dict) else {}
    pipeline_timing = dict(meta.get("pipeline_timing") or {})
    pipeline_timing.setdefault("source_event_ts_ms", int(source_ts_ms))
    pipeline_timing["db_observed_ts_ms"] = int(max(observed_ts_ms, source_ts_ms))
    pipeline_timing["ingestion_to_db_latency_ms"] = int(max(0, int(pipeline_timing["db_observed_ts_ms"]) - int(source_ts_ms)))
    meta["pipeline_timing"] = pipeline_timing
    now_ms = int(source_ts_ms)
    raw_payload = payload.get("raw_payload")
    if raw_payload is not None and not isinstance(raw_payload, str):
        raw_payload = json.dumps(raw_payload, separators=(",", ":"), sort_keys=True, default=str)
    derived_features = payload.get("derived_features")
    if derived_features is not None and not isinstance(derived_features, str):
        derived_features = json.dumps(derived_features, separators=(",", ":"), sort_keys=True, default=str)
    row = {
        "ts_ms": now_ms,
        "timestamp": now_ms,
        "event_type": payload.get("event_type") or payload.get("type") or "event",
        "symbol": payload.get("symbol"),
        "source": payload.get("source") or payload.get("event_source") or "runtime",
        "title": payload.get("title") or payload.get("event_title") or "",
        "body": payload.get("body"),
        "url": payload.get("url"),
        "importance_score": payload.get("importance_score"),
        "raw_payload": raw_payload,
        "derived_features": derived_features,
        "event_key": payload.get("event_key") or payload.get("source_id"),
        "source_id": payload.get("source_id"),
        "dedupe_hash": payload.get("dedupe_hash"),
        "meta_json": meta,
    }
    def _write(db: StorageConnection) -> int:
        if row.get("event_key"):
            event_id = int(
                _upsert_dict(
                    "events",
                    row,
                    conflict_column="event_key",
                    conflict_columns=("event_key", "ts_ms"),
                    returning_id=True,
                    con=db,
                )
                or 0
            )
        else:
            event_id = int(_insert_dict("events", row, returning_id=True, con=db) or 0)
        _put_structured_document_events_for_normalized_event(db, row=row, event_id=event_id)
        return int(event_id)

    if con is not None:
        return _write(con)
    return run_write_txn(_write)


def _put_structured_document_events_for_normalized_event(
    con: StorageConnection,
    *,
    row: dict[str, Any],
    event_id: int,
) -> None:
    event_type = str((row or {}).get("event_type") or "").strip().lower()
    if event_type not in {"news", "filing", "transcript"}:
        return
    try:
        from engine.data.structured_document_events import (
            extract_structured_document_events,
            put_structured_document_events,
        )

        payload = dict(row or {})
        payload["event_id"] = int(event_id or 0)
        records = extract_structured_document_events(payload)
        if records:
            put_structured_document_events(con, records)
    except Exception as exc:
        _warn_nonfatal(
            "STRUCTURED_DOCUMENT_EVENT_EXTRACTION_FAILED",
            exc,
            event_type=event_type,
            event_id=int(event_id or 0),
            source=str((row or {}).get("source") or ""),
            symbol=str((row or {}).get("symbol") or ""),
        )


def put_price(ts_ms, symbol, price, source: str = "runtime", con: StorageConnection | None = None):
    from engine.runtime.price_router import publish_price_event

    return publish_price_event(
        {
            "ts_ms": int(ts_ms),
            "symbol": str(symbol).upper(),
            "price": float(price),
            "source": str(source),
            "provider": str(source),
        },
        con=con,
        write_prices=True,
        write_quotes=False,
        write_raw=False,
        emit_telemetry=False,
        default_provider=str(source),
    )


def _payload_writer(table: str, row: dict[str, Any], con: StorageConnection | None = None) -> int:
    payload = dict(row or {})
    ts_ms = int(payload.get("ts_ms") or payload.get("ts") or time.time() * 1000)
    return int(
        _insert_dict(
            table,
            {
                "ts_ms": ts_ms,
                "symbol": payload.get("symbol"),
                "event_id": payload.get("event_id"),
                "payload_json": payload,
            },
            returning_id=False,
            con=con,
        )
        or 0
    )


_NEWS_EVENT_FEATURE_COLUMNS = (
    "event_id",
    "ts_ms",
    "symbol",
    "cluster_key",
    "headline_key",
    "sentiment_score",
    "novelty_score",
    "is_duplicate",
    "duplicate_count",
    "company_match_method",
    "company_match_conf",
    "source_count",
    "payload_json",
    "meta_json",
    "embedding_backend",
    "embedding_model_name",
    "embedding_novelty_score",
    "embedding_max_similarity",
    "stale_flag",
    "novelty_computed_ts_ms",
    "finbert_label",
    "finbert_score",
    "finbert_confidence",
    "finbert_pos",
    "finbert_neg",
    "finbert_neu",
)


def _news_event_feature_row(payload: dict[str, Any]) -> dict[str, Any]:
    source = dict(payload or {})
    row: dict[str, Any] = {
        "ts_ms": int(source.get("ts_ms") or source.get("ts") or time.time() * 1000),
        "payload_json": source,
    }
    for column in _NEWS_EVENT_FEATURE_COLUMNS:
        if column in {"ts_ms", "payload_json"}:
            continue
        if column == "symbol" and source.get(column) is not None:
            symbol = str(source.get(column) or "").strip().upper()
            if symbol:
                row[column] = symbol
            continue
        if column in source:
            row[column] = source.get(column)
    for column in ("is_duplicate", "stale_flag"):
        if column in row:
            row[column] = int(bool(row[column]))
    for column in ("event_id", "duplicate_count", "source_count", "novelty_computed_ts_ms"):
        if row.get(column) is not None:
            row[column] = int(row[column])
    for column in (
        "sentiment_score",
        "novelty_score",
        "company_match_conf",
        "embedding_novelty_score",
        "embedding_max_similarity",
        "finbert_score",
        "finbert_confidence",
        "finbert_pos",
        "finbert_neg",
        "finbert_neu",
    ):
        if row.get(column) is not None:
            row[column] = float(row[column])
    return row


_INSIDER_TRANSACTION_COLUMNS = (
    "ts_ms",
    "symbol",
    "event_id",
    "source_transaction_id",
    "created_ts_ms",
    "ingested_ts_ms",
    "source",
    "filing_accession",
    "filing_identifier",
    "filing_url",
    "filing_ts_ms",
    "availability_ts_ms",
    "filing_date",
    "filing_accepted_at",
    "transaction_ts_ms",
    "transaction_date",
    "issuer_name",
    "issuer_cik",
    "insider_name",
    "insider_cik",
    "insider_role",
    "insider_title",
    "transaction_code",
    "transaction_type",
    "direction",
    "security_type",
    "shares",
    "price",
    "value",
    "ownership_nature",
    "is_10b5_1_plan",
    "entity_id",
    "resolution_status",
    "resolution_method",
    "payload_json",
    "diagnostics_json",
)

_CONGRESSIONAL_TRADE_COLUMNS = (
    "ts_ms",
    "symbol",
    "event_id",
    "source_trade_id",
    "source_record_id",
    "source_url",
    "created_ts_ms",
    "ingested_ts_ms",
    "source",
    "chamber",
    "office",
    "politician_name",
    "owner_name",
    "issuer_name",
    "transaction_type_raw",
    "transaction_type",
    "direction",
    "amount_range",
    "amount_low",
    "amount_high",
    "amount_mid",
    "transaction_date",
    "transaction_ts_ms",
    "disclosure_date",
    "disclosure_ts_ms",
    "entity_id",
    "resolution_status",
    "resolution_method",
    "payload_json",
    "diagnostics_json",
)

_FINRA_SHORT_SALE_VOLUME_COLUMNS = (
    "ts_ms",
    "symbol",
    "trade_date",
    "trade_ts_ms",
    "availability_ts_ms",
    "source_record_id",
    "source_url",
    "ingested_ts_ms",
    "short_volume",
    "short_exempt_volume",
    "total_volume",
    "market",
    "payload_json",
    "diagnostics_json",
)

_FINRA_SHORT_INTEREST_COLUMNS = (
    "ts_ms",
    "symbol",
    "settlement_date",
    "settlement_ts_ms",
    "dissemination_date",
    "dissemination_ts_ms",
    "availability_ts_ms",
    "source_record_id",
    "ingested_ts_ms",
    "short_interest_shares",
    "days_to_cover",
    "payload_json",
    "diagnostics_json",
)

_CRYPTO_FUNDING_RATE_COLUMNS = (
    "ts_ms",
    "symbol",
    "exchange",
    "perp_market",
    "spot_market",
    "funding_ts_ms",
    "availability_ts_ms",
    "funding_rate",
    "mark_price",
    "index_price",
    "spot_price",
    "spot_ts_ms",
    "perp_ts_ms",
    "perp_basis_pct",
    "source_record_id",
    "ingested_ts_ms",
    "is_live",
    "payload_json",
    "diagnostics_json",
)


def _alt_data_row(payload: dict[str, Any], columns: tuple[str, ...]) -> dict[str, Any]:
    source = dict(payload or {})
    ts_ms = int(
        source.get("ts_ms")
        or source.get("availability_ts_ms")
        or source.get("transaction_ts_ms")
        or source.get("disclosure_ts_ms")
        or source.get("filing_ts_ms")
        or source.get("trade_ts_ms")
        or source.get("settlement_ts_ms")
        or source.get("funding_ts_ms")
        or source.get("ingested_ts_ms")
        or source.get("created_ts_ms")
        or time.time() * 1000
    )
    row: dict[str, Any] = {"ts_ms": ts_ms}
    for column in columns:
        if column == "ts_ms":
            continue
        if column == "symbol" and source.get(column) is not None:
            symbol = str(source.get(column) or "").strip().upper()
            if symbol:
                row[column] = symbol
            continue
        if column == "payload_json":
            row[column] = source.get("payload_json") if source.get("payload_json") is not None else source
            continue
        if column in source:
            row[column] = source.get(column)
    return row


def _alt_data_upsert(
    table: str,
    row: dict[str, Any],
    *,
    columns: tuple[str, ...],
    conflict_column: str,
    con: StorageConnection | None = None,
) -> int:
    return _upsert_dict(
        table,
        _alt_data_row(row, columns),
        conflict_column=conflict_column,
        returning_id=True,
        con=con,
    )


def put_news_event_feature(row: dict[str, Any], con: StorageConnection | None = None) -> None:
    payload = _news_event_feature_row(dict(row or {}))
    if payload.get("event_id") is None:
        _payload_writer("news_event_features", row, con=con)
        return
    _upsert_dict(
        "news_event_features",
        payload,
        conflict_column="event_id",
        conflict_columns=("event_id", "ts_ms"),
        returning_id=False,
        con=con,
    )


def _payload_dict(value: Any) -> dict[str, Any]:
    payload = _json_payload(value)
    return dict(payload) if isinstance(payload, dict) else {}


def put_finbert_sentiment_enrichment(row: dict[str, Any], con: StorageConnection | None = None) -> None:
    payload = dict(row or {})
    ts_ms = int(payload.get("ts_ms") or payload.get("ts") or time.time() * 1000)
    event_id = payload.get("event_id")
    symbol = payload.get("symbol")
    model_name = payload.get("model_name")

    def _write(db: StorageConnection) -> None:
        db.execute(
            """
            INSERT INTO finbert_sentiment_enrichments(
              ts_ms, symbol, event_id, source_identifier, model_name, payload_json
            ) VALUES (?,?,?,?,?,?)
            """,
            (
                ts_ms,
                str(symbol).upper() if symbol is not None and str(symbol).strip() else None,
                int(event_id) if event_id is not None else None,
                payload.get("source_identifier"),
                str(model_name) if model_name is not None else None,
                payload,
            ),
        )
        if event_id is not None:
            event_id_int = int(event_id)
            symbol_clean = str(symbol).upper() if symbol is not None and str(symbol).strip() else None
            existing = db.execute(
                "SELECT 1 FROM news_event_features WHERE event_id=? LIMIT 1",
                (event_id_int,),
            ).fetchone()
            params = (
                ts_ms,
                symbol_clean,
                payload.get("label"),
                payload.get("score"),
                payload.get("confidence"),
                payload.get("pos"),
                payload.get("neg"),
                payload.get("neu"),
                event_id_int,
            )
            if existing is not None:
                db.execute(
                    """
                    UPDATE news_event_features
                    SET ts_ms=?,
                        symbol=COALESCE(?, symbol),
                        finbert_label=?,
                        finbert_score=?,
                        finbert_confidence=?,
                        finbert_pos=?,
                        finbert_neg=?,
                        finbert_neu=?
                    WHERE event_id=?
                    """,
                    params,
                )
                return
            db.execute(
                """
                INSERT INTO news_event_features(
                  event_id, ts_ms, symbol,
                  finbert_label, finbert_score, finbert_confidence,
                  finbert_pos, finbert_neg, finbert_neu
                ) VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    event_id_int,
                    ts_ms,
                    symbol_clean,
                    payload.get("label"),
                    payload.get("score"),
                    payload.get("confidence"),
                    payload.get("pos"),
                    payload.get("neg"),
                    payload.get("neu"),
                ),
            )

    if con is not None:
        _write(con)
        return
    run_write_txn(_write)


def put_news_symbol_feature(row: dict[str, Any], con: StorageConnection | None = None) -> None:
    _payload_writer("news_symbol_features", row, con=con)


def put_options_event_feature(row: dict[str, Any], con: StorageConnection | None = None) -> None:
    _payload_writer("options_event_features", row, con=con)


def put_insider_transaction(row: dict[str, Any], con: StorageConnection | None = None) -> int:
    return _alt_data_upsert(
        "insider_transactions",
        row,
        columns=_INSIDER_TRANSACTION_COLUMNS,
        conflict_column="source_transaction_id",
        con=con,
    )


def put_congressional_trade(row: dict[str, Any], con: StorageConnection | None = None) -> int:
    return _alt_data_upsert(
        "congressional_trades",
        row,
        columns=_CONGRESSIONAL_TRADE_COLUMNS,
        conflict_column="source_trade_id",
        con=con,
    )


def put_finra_short_sale_volume(row: dict[str, Any], con: StorageConnection | None = None) -> int:
    return _alt_data_upsert(
        "finra_short_sale_volume",
        row,
        columns=_FINRA_SHORT_SALE_VOLUME_COLUMNS,
        conflict_column="source_record_id",
        con=con,
    )


def put_finra_short_interest(row: dict[str, Any], con: StorageConnection | None = None) -> int:
    return _alt_data_upsert(
        "finra_short_interest",
        row,
        columns=_FINRA_SHORT_INTEREST_COLUMNS,
        conflict_column="source_record_id",
        con=con,
    )


def put_crypto_funding_rate(row: dict[str, Any], con: StorageConnection | None = None) -> int:
    return _alt_data_upsert(
        "crypto_funding_rates",
        row,
        columns=_CRYPTO_FUNDING_RATE_COLUMNS,
        conflict_column="source_record_id",
        con=con,
    )


def load_finbert_sentiment_enrichment_for_event(
    event_id: int,
    *,
    model_name: str | None = None,
    con: StorageConnection | None = None,
    **_: Any,
):
    owns = con is None
    con = con or connect(readonly=True)
    try:
        where = "WHERE event_id=?"
        params: list[Any] = [int(event_id)]
        if str(model_name or "").strip():
            where += " AND model_name=?"
            params.append(str(model_name))
        row = con.execute(
            f"SELECT payload_json FROM finbert_sentiment_enrichments {where} ORDER BY ts_ms DESC LIMIT 1",
            tuple(params),
        ).fetchone()
        return _payload_dict(row[0]) if row else None
    finally:
        if owns:
            con.close()


def load_latest_finbert_sentiment_enrichment(
    symbol: str = "",
    *,
    ts_ms: int | None = None,
    model_name: str | None = None,
    con: StorageConnection | None = None,
    **_: Any,
):
    owns = con is None
    con = con or connect(readonly=True)
    try:
        filters: list[str] = []
        params: list[Any] = []
        if str(symbol or "").strip():
            filters.append("symbol=?")
            params.append(str(symbol).upper())
        if int(ts_ms or 0) > 0:
            filters.append("ts_ms<=?")
            params.append(int(ts_ms or 0))
        if str(model_name or "").strip():
            filters.append("model_name=?")
            params.append(str(model_name))
        where = f"WHERE {' AND '.join(filters)}" if filters else ""
        row = con.execute(
            f"SELECT payload_json FROM finbert_sentiment_enrichments {where} ORDER BY ts_ms DESC LIMIT 1",
            tuple(params),
        ).fetchone()
        return _payload_dict(row[0]) if row else None
    finally:
        if owns:
            con.close()


def record_prediction_explanation(**kwargs: Any) -> int:
    row = dict(kwargs or {})
    con = row.pop("con", None)
    row.setdefault("created_ts", int(time.time() * 1000))
    return int(_insert_dict("prediction_explanations", row, returning_id=True, con=con) or 0)


def fetch_prediction_explanations(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
    if args:
        kwargs.setdefault("symbol", args[0])
    con = kwargs.pop("con", None)
    limit = _bounded_limit(kwargs.pop("limit", None), default=25, maximum=500)
    filters: list[str] = []
    params: list[Any] = []
    for key in ("symbol", "model_family", "model_name", "version", "explanation_type"):
        value = kwargs.get(key)
        if value not in (None, ""):
            filters.append(f"{_ident(key)}=?")
            params.append(str(value))
    ts_value = kwargs.get("ts")
    if ts_value not in (None, ""):
        filters.append("ts<=?")
        params.append(int(ts_value))
    where_sql = ("WHERE " + " AND ".join(filters)) if filters else ""
    columns = (
        "id",
        "symbol",
        "ts",
        "model_family",
        "model_name",
        "version",
        "explanation_type",
        "top_features",
        "base_value",
        "diagnostics",
        "created_ts",
    )

    def _read(db: StorageConnection) -> list[dict[str, Any]]:
        rows = db.execute(
            f"""
            SELECT {', '.join(columns)}
            FROM prediction_explanations
            {where_sql}
            ORDER BY ts DESC, id DESC
            LIMIT ?
            """,
            tuple(params) + (limit,),
        ).fetchall()
        return [
            _format_plain_row(row, columns, json_columns=("top_features", "diagnostics"))
            for row in rows
        ]

    if con is not None:
        return _read(con)
    with connect(readonly=True) as db:
        return _read(db)


def fetch_latest_prediction_explanation(*args: Any, **kwargs: Any):
    rows = fetch_prediction_explanations(*args, **kwargs)
    return rows[0] if rows else None


def log_alert_interaction(**kwargs: Any) -> int:
    return int(_insert_dict("alert_interactions", kwargs, returning_id=True) or 0)


def log_decision_view(**kwargs: Any) -> int:
    return int(_insert_dict("decision_views", kwargs, returning_id=True) or 0)


def record_hypothesis_result(**kwargs: Any) -> int:
    return int(_insert_dict("hypothesis_registry", kwargs, returning_id=True) or 0)


def record_backtest_cpcv_run(con: StorageConnection | None = None, **kwargs: Any) -> int:
    row = dict(kwargs or {})
    if row.get("created_ts") is None:
        row["created_ts"] = int(row.get("ts") or time.time() * 1000)
    paths = row.get("path_sharpes") or []
    try:
        row.setdefault("n_paths", len(list(paths)))
    except Exception:
        logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)
    return int(_insert_dict("backtest_cpcv_runs", row, returning_id=True, con=con) or 0)


def record_backtest_cpcv_path_result(con: StorageConnection | None = None, **kwargs: Any) -> int:
    row = dict(kwargs or {})
    path_id = int(_insert_dict("backtest_cpcv_path_results", row, returning_id=True, con=con) or 0)
    compat = {
        "created_ts": int(row.get("ts") or time.time() * 1000),
        "ts": row.get("ts"),
        "model_id": row.get("model_id"),
        "path_index": row.get("path_index"),
        "sharpe": row.get("sharpe"),
        "deflated_sharpe": row.get("deflated_sharpe"),
        "n_trials": row.get("n_trials"),
        "total_return": row.get("total_return"),
        "max_drawdown": row.get("max_drawdown"),
        "cfg": row.get("cfg"),
        "payload": row.get("payload"),
    }
    _insert_dict("backtest_cpcv_runs", compat, returning_id=False, con=con)
    return int(path_id or 0)


def record_alpha_candidate(**kwargs: Any) -> int:
    return int(_insert_dict("alpha_candidates", kwargs, returning_id=True) or 0)


def update_alpha_candidate(candidate_id: int, **kwargs: Any) -> None:
    if not kwargs:
        return
    columns = [_ident(col) for col in kwargs]
    assignments = ", ".join(f"{col}=?" for col in columns)

    def _write(con: StorageConnection):
        con.execute(
            f"UPDATE alpha_candidates SET {assignments} WHERE id=?",
            tuple(kwargs[col] for col in columns) + (int(candidate_id),),
        )

    run_write_txn(_write)


def record_alpha_lifecycle(**kwargs: Any) -> int:
    return int(_insert_dict("alpha_lifecycle", kwargs, returning_id=True) or 0)


def record_drift_retrain_event(**kwargs: Any) -> int:
    row = dict(kwargs or {})
    row.setdefault("created_ts", int(time.time() * 1000))
    return int(_insert_dict("drift_retrain_events", row, returning_id=True) or 0)


def record_model_hyperparameter_registry(con: StorageConnection | None = None, **kwargs: Any) -> int:
    row = _normalise_model_hparam_row(dict(kwargs or {}))

    def _write(db: StorageConnection) -> int:
        available = _table_columns(db, "model_hyperparameter_registry")
        registry_row = {key: value for key, value in row.items() if key in available}
        registry_id = int(_insert_dict("model_hyperparameter_registry", registry_row, returning_id=True, con=db) or 0)
        params = _json_payload(row.get("params")) or _json_payload(row.get("params_json")) or {}
        if isinstance(params, dict) and row.get("model_family"):
            upsert_model_best_params(
                model_family=str(row.get("model_family") or ""),
                symbol=str(row.get("symbol") or "GLOBAL"),
                study_name=str(row.get("study_name") or ""),
                params_json=dict(params),
                value=float(row.get("metric_value") or 0.0),
                ts=int(row.get("ts") or time.time() * 1000),
                trial_number=(None if row.get("best_trial_number") is None else int(row.get("best_trial_number") or 0)),
                seed=(None if row.get("seed") is None else int(row.get("seed") or 0)),
                con=db,
            )
        return registry_id

    if con is not None:
        return _write(con)
    return int(run_write_txn(_write) or 0)


def upsert_model_best_params(
    *,
    model_family: str,
    symbol: str,
    study_name: str,
    params_json: Any,
    value: float,
    ts: int | None = None,
    trial_number: int | None = None,
    seed: int | None = None,
    con: StorageConnection | None = None,
) -> int:
    row = {
        "model_family": str(model_family or "").strip(),
        "symbol": str(symbol or "global").strip().upper() or "GLOBAL",
        "ts": int(ts if ts is not None else time.time() * 1000),
        "study_name": str(study_name or "").strip(),
        "params_json": params_json or {},
        "value": float(value),
        "trial_number": None if trial_number is None else int(trial_number),
        "seed": None if seed is None else int(seed),
    }

    def _write(db: StorageConnection):
        db.execute(
            """
            INSERT INTO model_best_params(
              model_family, symbol, ts, study_name, params_json, value, trial_number, seed
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(model_family, symbol) DO UPDATE SET
              ts=excluded.ts,
              study_name=excluded.study_name,
              params_json=excluded.params_json,
              value=excluded.value,
              trial_number=excluded.trial_number,
              seed=excluded.seed
            """,
            (
                row["model_family"],
                row["symbol"],
                row["ts"],
                row["study_name"],
                row["params_json"],
                row["value"],
                row["trial_number"],
                row["seed"],
            ),
        )
        return 1

    if con is not None:
        return int(_write(con) or 0)
    return int(run_write_txn(_write) or 0)


def _empty_recent(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
    del args, kwargs
    return []


def _format_plain_row(row: Any, columns: Sequence[str], *, json_columns: Sequence[str] = ()) -> dict[str, Any]:
    out = _row_to_dict(row, columns)
    json_names = {str(name) for name in json_columns}
    for key in list(out.keys()):
        if key in json_names:
            out[key] = _json_read_value(out[key])
        else:
            out[key] = _json_safe_value(out[key])
    return out


def _normalise_model_hparam_row(row: dict[str, Any]) -> dict[str, Any]:
    clean = dict(row or {})
    clean.setdefault("ts", int(time.time() * 1000))
    clean.setdefault("symbol", "GLOBAL")
    clean.setdefault("study_name", "")
    clean.setdefault("tuner", "")
    clean.setdefault("objective", "")
    clean.setdefault("trial_count", 0)
    clean.setdefault("best_trial_number", 0)
    params = clean.get("params")
    params_json = clean.get("params_json")
    if params_json in (None, "") and params not in (None, ""):
        clean["params_json"] = params
    if params in (None, "") and params_json not in (None, ""):
        clean["params"] = params_json
    return clean


def _bounded_limit(limit: int | None, *, default: int = 20, maximum: int = 500) -> int:
    try:
        value = int(limit if limit is not None else default)
    except Exception:
        value = int(default)
    return max(1, min(int(maximum), value))


def fetch_recent_hypothesis_registry(
    *,
    model_name: str | None = None,
    candidate_version: str | None = None,
    limit: int = 20,
    con: StorageConnection | None = None,
) -> list[dict[str, Any]]:
    owns = con is None
    con = con or connect(readonly=True)
    try:
        where: list[str] = []
        params: list[Any] = []
        if str(model_name or "").strip():
            where.append("model_name=?")
            params.append(str(model_name or "").strip())
        if str(candidate_version or "").strip():
            where.append("candidate_version=?")
            params.append(str(candidate_version or "").strip())
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        columns = [
            "id",
            "created_ts",
            "model_name",
            "candidate_version",
            "n_observations",
            "t_statistic",
            "deflated_sharpe",
            "threshold_t",
            "n_competing_trials",
            "passed",
            "diagnostics",
        ]
        rows = con.execute(
            f"""
            SELECT {', '.join(columns)}
            FROM hypothesis_registry
            {where_sql}
            ORDER BY COALESCE(created_ts, 0) DESC, id DESC
            LIMIT ?
            """,
            tuple(params) + (_bounded_limit(limit),),
        ).fetchall()
        return [_format_plain_row(row, columns, json_columns=("diagnostics",)) for row in rows]
    finally:
        if owns:
            con.close()


def fetch_recent_alpha_candidates(
    *,
    candidate_name: str | None = None,
    status: str | None = None,
    model_family: str | None = None,
    limit: int = 20,
    con: StorageConnection | None = None,
) -> list[dict[str, Any]]:
    owns = con is None
    con = con or connect(readonly=True)
    try:
        where: list[str] = []
        params: list[Any] = []
        if str(candidate_name or "").strip():
            where.append("candidate_name=?")
            params.append(str(candidate_name or "").strip())
        if str(status or "").strip():
            where.append("status=?")
            params.append(str(status or "").strip())
        if str(model_family or "").strip():
            where.append("model_family=?")
            params.append(str(model_family or "").strip())
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        columns = [
            "id",
            "candidate_name",
            "candidate_version",
            "model_family",
            "feature_ids",
            "generation_method",
            "hyperparams",
            "status",
            "diagnostics",
            "created_ts",
        ]
        rows = con.execute(
            f"""
            SELECT {', '.join(columns)}
            FROM alpha_candidates
            {where_sql}
            ORDER BY COALESCE(created_ts, 0) DESC, id DESC
            LIMIT ?
            """,
            tuple(params) + (_bounded_limit(limit),),
        ).fetchall()
        return [
            _format_plain_row(row, columns, json_columns=("feature_ids", "hyperparams", "diagnostics"))
            for row in rows
        ]
    finally:
        if owns:
            con.close()


def fetch_alpha_lifecycle(
    *,
    candidate_id: int | None = None,
    alert_id: int | None = None,
    stage: str | None = None,
    limit: int = 20,
    con: StorageConnection | None = None,
) -> list[dict[str, Any]]:
    owns = con is None
    con = con or connect(readonly=True)
    try:
        where: list[str] = []
        params: list[Any] = []
        if candidate_id is not None:
            where.append("candidate_id=?")
            params.append(int(candidate_id))
        if alert_id is not None:
            where.append("alert_id=?")
            params.append(int(alert_id))
        if str(stage or "").strip():
            where.append("stage=?")
            params.append(str(stage or "").strip())
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        columns = [
            "id",
            "candidate_id",
            "stage",
            "outcome",
            "metrics",
            "notes",
            "created_ts",
            "alert_id",
            "created_ts_ms",
            "expires_ts_ms",
            "half_life_ms",
            "volatility",
            "status",
            "last_touch_ts_ms",
            "meta_json",
        ]
        rows = con.execute(
            f"""
            SELECT {', '.join(columns)}
            FROM alpha_lifecycle
            {where_sql}
            ORDER BY COALESCE(created_ts, created_ts_ms, last_touch_ts_ms, 0) DESC, id DESC
            LIMIT ?
            """,
            tuple(params) + (_bounded_limit(limit),),
        ).fetchall()
        return [_format_plain_row(row, columns, json_columns=("metrics", "notes", "meta_json")) for row in rows]
    finally:
        if owns:
            con.close()


def fetch_recent_drift_retrain_events(
    *,
    model_name: str | None = None,
    family: str | None = None,
    outcome_status: str | None = None,
    limit: int = 20,
    con: StorageConnection | None = None,
) -> list[dict[str, Any]]:
    owns = con is None
    con = con or connect(readonly=True)
    try:
        where: list[str] = []
        params: list[Any] = []
        if str(model_name or "").strip():
            where.append("model_name=?")
            params.append(str(model_name or "").strip())
        if str(family or "").strip():
            where.append("family=?")
            params.append(str(family or "").strip())
        if str(outcome_status or "").strip():
            where.append("outcome_status=?")
            params.append(str(outcome_status or "").strip())
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        columns = [
            "id",
            "created_ts",
            "model_name",
            "family",
            "trigger_type",
            "trigger_metrics",
            "action_taken",
            "cooldown_applied",
            "candidate_version",
            "outcome_status",
            "diagnostics",
        ]
        rows = con.execute(
            f"""
            SELECT {', '.join(columns)}
            FROM drift_retrain_events
            {where_sql}
            ORDER BY COALESCE(created_ts, 0) DESC, id DESC
            LIMIT ?
            """,
            tuple(params) + (_bounded_limit(limit),),
        ).fetchall()
        return [
            _format_plain_row(row, columns, json_columns=("trigger_metrics", "diagnostics"))
            for row in rows
        ]
    finally:
        if owns:
            con.close()


def fetch_recent_backtest_cpcv_runs(
    *,
    model_name: str | None = None,
    candidate_version: str | None = None,
    include_paths: bool = False,
    limit: int = 20,
    con: StorageConnection | None = None,
) -> list[dict[str, Any]]:
    owns = con is None
    con = con or connect(readonly=True)
    try:
        where: list[str] = []
        params: list[Any] = []
        if str(model_name or "").strip():
            where.append("model_name=?")
            params.append(str(model_name or "").strip())
        if str(candidate_version or "").strip():
            where.append("candidate_version=?")
            params.append(str(candidate_version or "").strip())
        if not include_paths:
            where.append("path_index IS NULL")
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        columns = [
            "id",
            "created_ts",
            "ts",
            "model_name",
            "candidate_version",
            "model_id",
            "n_splits",
            "n_test_splits",
            "embargo_pct",
            "n_paths",
            "path_index",
            "path_returns",
            "path_sharpes",
            "mean_sharpe",
            "median_sharpe",
            "pbo",
            "sharpe",
            "deflated_sharpe",
            "n_trials",
            "total_return",
            "max_drawdown",
            "cfg",
            "payload",
            "diagnostics",
        ]
        rows = con.execute(
            f"""
            SELECT {', '.join(columns)}
            FROM backtest_cpcv_runs
            {where_sql}
            ORDER BY COALESCE(created_ts, ts, 0) DESC, id DESC
            LIMIT ?
            """,
            tuple(params) + (max(1, min(500, int(limit or 20))),),
        ).fetchall()
        return [
            _format_plain_row(
                row,
                columns,
                json_columns=("path_returns", "path_sharpes", "cfg", "payload", "diagnostics"),
            )
            for row in rows
        ]
    finally:
        if owns and con is not None:
            con.close()


def fetch_model_best_params(
    *,
    model_family: str,
    symbol: str = "GLOBAL",
    con: StorageConnection | None = None,
) -> dict[str, Any] | None:
    owns = con is None
    con = con or connect(readonly=True)
    try:
        family = str(model_family or "").strip()
        sym = str(symbol or "GLOBAL").strip().upper() or "GLOBAL"
        row = con.execute(
            """
            SELECT model_family, symbol, ts, study_name, params_json, value, trial_number, seed
            FROM model_best_params
            WHERE model_family=? AND symbol=?
            LIMIT 1
            """,
            (family, sym),
        ).fetchone()
        if row is None and sym != "GLOBAL":
            row = con.execute(
                """
                SELECT model_family, symbol, ts, study_name, params_json, value, trial_number, seed
                FROM model_best_params
                WHERE model_family=? AND symbol='GLOBAL'
                LIMIT 1
                """,
                (family,),
            ).fetchone()
        if not row:
            return None
        out = _format_plain_row(
            row,
            ("model_family", "symbol", "ts", "study_name", "params_json", "value", "trial_number", "seed"),
            json_columns=("params_json",),
        )
        out["params"] = dict(out.get("params_json") or {})
        return out
    finally:
        if owns and con is not None:
            con.close()


def fetch_recent_audit_records(
    table: str,
    *,
    limit: int = 100,
    from_id: int | None = None,
    to_id: int | None = None,
    con: StorageConnection | None = None,
) -> list[dict[str, Any]]:
    table_name = _audit_table_name(table)
    return _fetch_audit_records(
        table_name,
        limit=limit,
        from_id=from_id,
        to_id=to_id,
        con=con,
    )


def fetch_audit_record(
    table: str,
    row_id: int,
    *,
    con: StorageConnection | None = None,
) -> dict[str, Any] | None:
    table_name = _audit_table_name(table)

    def _read(db: StorageConnection) -> dict[str, Any] | None:
        if not _relation_exists_compat(db, table_name):
            return None
        columns = _table_column_metadata(db, table_name)
        if "id" not in {name for name, _type in columns}:
            return None
        row = db.execute(f"SELECT * FROM {table_name} WHERE id=? LIMIT 1", (int(row_id),)).fetchone()
        if not row:
            return None
        return _format_audit_record(row, columns)

    return _with_read_connection(con, _read)


def fetch_recent_promotion_statistical_evidence(
    limit: int = 50,
    *,
    model_id: str | None = None,
    con: StorageConnection | None = None,
) -> list[dict[str, Any]]:
    extra_where: list[str] = []
    extra_params: list[Any] = []
    if model_id:
        extra_where.append("model_id=?")
        extra_params.append(str(model_id))
    rows = _fetch_audit_records(
        "promotion_statistical_evidence",
        limit=limit,
        con=con,
        extra_where=extra_where,
        extra_params=extra_params,
    )
    for row in rows:
        if "payload" not in row and "payload_json" in row:
            row["payload"] = _json_read_value(row.get("payload_json"))
    return rows


def fetch_recent_decisions(
    limit: int = 100,
    *,
    symbol: str | None = None,
    con: StorageConnection | None = None,
) -> list[dict[str, Any]]:
    extra_where: list[str] = []
    extra_params: list[Any] = []
    if symbol:
        extra_where.append("symbol=?")
        extra_params.append(str(symbol).upper())
    return _fetch_audit_records(
        "decision_log",
        limit=limit,
        con=con,
        extra_where=extra_where,
        extra_params=extra_params,
    )


def fetch_latest_backtest_cpcv_run(*args: Any, **kwargs: Any):
    rows = fetch_recent_backtest_cpcv_runs(*args, **kwargs)
    return rows[0] if rows else None


def fetch_latest_drift_retrain_event(*args: Any, **kwargs: Any):
    rows = fetch_recent_drift_retrain_events(*args, **kwargs)
    return rows[0] if rows else None


def fetch_latest_model_hyperparameters(*args: Any, **kwargs: Any):
    if args:
        kwargs.setdefault("model_family", args[0])
    model_family = str(kwargs.get("model_family") or "").strip()
    model_name = str(kwargs.get("model_name") or "").strip()
    if not model_family:
        model_family = model_name.split(":", 1)[0] if model_name else ""
    if not model_family:
        return None

    owns = kwargs.get("con") is None
    db = kwargs.get("con") or connect(readonly=True)
    try:
        if _table_exists(db, "model_hyperparameter_registry"):
            available = _table_columns(db, "model_hyperparameter_registry")
            desired = (
                "id",
                "ts",
                "model_family",
                "model_name",
                "symbol",
                "tuner",
                "objective",
                "study_name",
                "params",
                "params_json",
                "metric_value",
                "trial_count",
                "best_trial_number",
                "seed",
                "cpcv_mean_sharpe",
                "cpcv_median_sharpe",
                "cpcv_pbo",
                "diagnostics",
            )
            columns = tuple(column for column in desired if column in available)
            if columns:
                filters = ["model_family=?"]
                params: list[Any] = [model_family]
                if model_name and "model_name" in available:
                    filters.append("model_name=?")
                    params.append(model_name)
                tuner = kwargs.get("tuner")
                if tuner not in (None, "") and "tuner" in available:
                    filters.append("tuner=?")
                    params.append(str(tuner))
                symbol_filter = kwargs.get("symbol")
                if symbol_filter not in (None, "") and "symbol" in available:
                    filters.append("symbol=?")
                    params.append(str(symbol_filter).strip().upper() or "GLOBAL")
                row = db.execute(
                    f"""
                    SELECT {', '.join(_ident(column) for column in columns)}
                    FROM model_hyperparameter_registry
                    WHERE {' AND '.join(filters)}
                    ORDER BY ts DESC, id DESC
                    LIMIT 1
                    """,
                    tuple(params),
                ).fetchone()
                if row:
                    out = _format_plain_row(row, columns, json_columns=("params", "params_json", "diagnostics"))
                    params_value = out.get("params")
                    if not isinstance(params_value, dict):
                        params_value = out.get("params_json")
                    out["params"] = dict(params_value or {})
                    if "params_json" in out and not isinstance(out.get("params_json"), dict):
                        out["params_json"] = dict(out.get("params") or {})
                    return out
        symbol = str(kwargs.get("symbol") or "GLOBAL")
        return fetch_model_best_params(model_family=model_family, symbol=symbol, con=db)
    finally:
        if owns:
            db.close()


def fetch_decision_detail(decision_id: int, *, con: StorageConnection | None = None):
    return fetch_audit_record("decision_log", int(decision_id), con=con)


def _fetch_audit_records(
    table: str,
    *,
    limit: int,
    from_id: int | None = None,
    to_id: int | None = None,
    con: StorageConnection | None = None,
    extra_where: Sequence[str] = (),
    extra_params: Sequence[Any] = (),
) -> list[dict[str, Any]]:
    table_name = _audit_table_name(table)
    capped_limit = max(1, min(10000, int(limit or 100)))

    def _read(db: StorageConnection) -> list[dict[str, Any]]:
        if not _relation_exists_compat(db, table_name):
            return []
        columns = _table_column_metadata(db, table_name)
        names = {name for name, _type in columns}
        where = [str(part) for part in extra_where if str(part).strip()]
        params = list(extra_params)
        if "id" in names and from_id is not None:
            where.append("id>=?")
            params.append(int(from_id))
        if "id" in names and to_id is not None:
            where.append("id<=?")
            params.append(int(to_id))
        where_sql = (" WHERE " + " AND ".join(where)) if where else ""

        from engine.audit.chain import order_by_clause

        order_sql = order_by_clause(db, table_name, descending=True)
        rows = db.execute(
            f"SELECT * FROM {table_name}{where_sql} {order_sql} LIMIT ?",
            tuple(params) + (capped_limit,),
        ).fetchall() or []
        return [_format_audit_record(row, columns) for row in rows]

    return _with_read_connection(con, _read)


def _with_read_connection(con: StorageConnection | None, fn: Callable[[StorageConnection], Any]) -> Any:
    if con is not None:
        return fn(con)
    with connect(readonly=True) as db:
        return fn(db)


def _audit_table_name(table: str) -> str:
    table_name = _ident(table)
    from engine.runtime.schema.table_classification import audit_tables

    if table_name not in set(audit_tables()):
        raise ValueError(f"not_audit_table:{table_name}")
    return table_name


def _relation_exists_compat(con: StorageConnection, table: str) -> bool:
    try:
        return _table_exists(con, table)
    except Exception:
        logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)
    try:
        row = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (str(table),)).fetchone()
        return bool(row)
    except Exception:
        return False


def _table_column_metadata(con: StorageConnection, table: str) -> list[tuple[str, str]]:
    rows = con.execute(f"PRAGMA table_info({_ident(table)})").fetchall() or []
    return [(str(row[1]), str(row[2] or "")) for row in rows]


def _format_audit_record(row: Any, columns: Sequence[tuple[str, str]]) -> dict[str, Any]:
    column_names = [name for name, _type in columns]
    raw = _row_to_dict(row, column_names)
    json_columns = {
        name
        for name, type_name in columns
        if "JSON" in str(type_name).upper()
        or name.endswith("_json")
        or name in {"payload", "payload_excerpt", "detail_json", "reason_json"}
    }
    out: dict[str, Any] = {}
    for key, value in raw.items():
        if key in {"prev_hash", "row_hash"}:
            out[key] = _hash_hex(value)
        elif key in json_columns:
            out[key] = _json_read_value(value)
        else:
            out[key] = _json_safe_value(value)
    return out


def _row_to_dict(row: Any, columns: Sequence[str]) -> dict[str, Any]:
    if hasattr(row, "keys"):
        try:
            return {str(key): row[key] for key in row.keys()}
        except Exception:
            logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)
    return {str(columns[idx]): row[idx] for idx in range(min(len(columns), len(row)))}


def _hash_hex(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, memoryview):
        return bytes(value).hex()
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    text = str(value)
    try:
        return bytes.fromhex(text).hex()
    except Exception:
        return text


def _json_read_value(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return value
    return _json_safe_value(value)


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, memoryview):
        return bytes(value).hex()
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    if isinstance(value, dict):
        return {str(k): _json_safe_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe_value(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe_value(v) for v in value]
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)
    return value


def fetch_human_alignment_report(*args: Any, **kwargs: Any) -> dict[str, Any]:
    del args, kwargs
    return {"ok": True, "rows": []}


def acquire_job_lock(job_name: str, owner: str, pid: int, ttl_s: int = 180, stale_after_s: int | None = None) -> bool:
    ttl = int(stale_after_s if stale_after_s is not None else ttl_s)
    now_ms = int(time.time() * 1000)
    stale_ms = max(1, ttl) * 1000

    def _write(con: StorageConnection) -> bool:
        row = con.execute(
            "SELECT owner, pid, heartbeat_ts_ms FROM job_locks WHERE job_name=?",
            (str(job_name),),
        ).fetchone()
        if row:
            current_owner = str(row[0] or "")
            current_pid = int(row[1] or 0)
            heartbeat_ts_ms = int(row[2] or 0)
            same_owner = current_owner == str(owner) and current_pid == int(pid)
            stale = heartbeat_ts_ms <= 0 or (now_ms - heartbeat_ts_ms) > stale_ms
            dead = current_pid > 0 and not _pid_is_running(current_pid)
            if not (same_owner or stale or dead):
                return False
        con.execute(
            """
            INSERT INTO job_locks(job_name, owner, pid, acquired_ts_ms, heartbeat_ts_ms, expires_ms)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(job_name) DO UPDATE SET
              owner=excluded.owner,
              pid=excluded.pid,
              heartbeat_ts_ms=excluded.heartbeat_ts_ms,
              expires_ms=excluded.expires_ms
            """,
            (str(job_name), str(owner), int(pid), now_ms, now_ms, now_ms + stale_ms),
        )
        return True

    return bool(run_write_txn(_write, attempts=1, timeout_s=0.5))


def release_job_lock(job_name: str, owner: str, pid: int) -> None:
    def _write(con: StorageConnection) -> None:
        con.execute(
            "DELETE FROM job_locks WHERE job_name=? AND owner=? AND pid=?",
            (str(job_name), str(owner), int(pid)),
        )
        con.execute(
            "DELETE FROM job_heartbeats WHERE job_name=? AND owner=? AND pid=?",
            (str(job_name), str(owner), int(pid)),
        )

    run_write_txn(_write, attempts=1, timeout_s=0.5)


def touch_job_lock(job_name: str, owner: str, pid: int, *, best_effort: bool = False) -> None:
    del best_effort
    now_ms = int(time.time() * 1000)

    def _write(con: StorageConnection) -> None:
        con.execute(
            "UPDATE job_locks SET heartbeat_ts_ms=? WHERE job_name=? AND owner=? AND pid=?",
            (now_ms, str(job_name), str(owner), int(pid)),
        )

    run_write_txn(_write, attempts=1, timeout_s=0.5)


def put_job_heartbeat(
    job_name: str,
    owner: str,
    pid: int,
    extra_json: Optional[str] = None,
    *,
    best_effort: bool = False,
) -> None:
    del best_effort
    now_ms = int(time.time() * 1000)

    def _write(con: StorageConnection) -> None:
        con.execute(
            """
            INSERT INTO job_heartbeats(job_name, owner, pid, ts_ms, extra_json)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(job_name) DO UPDATE SET
              owner=excluded.owner,
              pid=excluded.pid,
              ts_ms=excluded.ts_ms,
              extra_json=excluded.extra_json
            """,
            (str(job_name), str(owner), int(pid), now_ms, extra_json),
        )
        con.execute(
            "UPDATE job_locks SET heartbeat_ts_ms=? WHERE job_name=? AND owner=? AND pid=?",
            (now_ms, str(job_name), str(owner), int(pid)),
        )

    run_write_txn(_write, attempts=1, timeout_s=0.5)


def get_job_checkpoint(job_name: str) -> dict[str, int]:
    row = fetch_one(
        "SELECT last_event_id, last_event_ts_ms FROM job_checkpoints WHERE job_name=? LIMIT 1",
        (str(job_name),),
    )
    if not row:
        return {"last_event_id": 0, "last_event_ts_ms": 0}
    return {"last_event_id": int(row[0] or 0), "last_event_ts_ms": int(row[1] or 0)}


def put_job_checkpoint(job_name: str, last_event_id: int, last_event_ts_ms: int, *, con: StorageConnection | None = None) -> None:
    now_ms = int(time.time() * 1000)

    def _write(db: StorageConnection) -> None:
        db.execute(
            """
            INSERT INTO job_checkpoints(job_name, last_event_id, last_event_ts_ms, updated_ts_ms)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(job_name) DO UPDATE SET
              last_event_id=excluded.last_event_id,
              last_event_ts_ms=excluded.last_event_ts_ms,
              updated_ts_ms=excluded.updated_ts_ms
            """,
            (str(job_name), int(last_event_id), int(last_event_ts_ms), now_ms),
        )

    if con is not None:
        _write(con)
    else:
        run_write_txn(_write)


def flush_job_liveness_queue(*, max_batches: int = 8, force: bool = True) -> dict[str, Any]:
    del max_batches, force
    return {"ok": True, "enabled": False, "pending": 0}


def shutdown_job_liveness_queue(*, timeout_s: float = 2.0) -> dict[str, Any]:
    del timeout_s
    return flush_job_liveness_queue()


def _job_liveness_queue_snapshot() -> dict[str, Any]:
    return {"enabled": False, "pending": 0}


def _warn_nonfatal(code: str, error: Exception, **extra: Any) -> None:
    LOGGER.warning("%s: %s extra=%s", str(code), error, extra or {})


def _warn_nonfatal_once(code: str, error: Exception, *, once_key: str | None = None, **extra: Any) -> None:
    del once_key
    _warn_nonfatal(code, error, **extra)


def _ensure_price_quotes_schema(con: StorageConnection) -> None:
    from engine.runtime.storage_live_ingestion_schema import ensure_price_quotes_schema

    ensure_price_quotes_schema(con, warn_nonfatal=_warn_nonfatal)


def _ensure_price_quotes_raw_schema(con: StorageConnection) -> None:
    from engine.runtime.storage_live_ingestion_schema import ensure_price_quotes_raw_schema

    ensure_price_quotes_raw_schema(con, warn_nonfatal=_warn_nonfatal)


def __getattr__(name: str):
    if name.startswith("_ensure_") and name.endswith("_schema"):
        def _ensure(*args: Any, **kwargs: Any) -> None:
            del args, kwargs
            init_db()

        return _ensure
    raise AttributeError(name)


__all__ = [name for name in globals() if not name.startswith("__")]
