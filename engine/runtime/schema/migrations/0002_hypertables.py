"""Convert append-mostly runtime tables into Timescale hypertables."""

from __future__ import annotations

import hashlib
import os
import re
from typing import Iterable

from engine.runtime.storage_pool import quote_ident, schema_name
from engine.runtime.schema.table_classification import Hypertable, TABLE_CLASS

id = 2
description = "timescale hypertables and lifecycle policies"


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_INTERVAL_RE = re.compile(r"^\s*(?P<count>\d+)\s*(?P<unit>[A-Za-z]+)\s*$")
_MS_BY_UNIT = {
    "millisecond": 1,
    "milliseconds": 1,
    "ms": 1,
    "second": 1_000,
    "seconds": 1_000,
    "minute": 60_000,
    "minutes": 60_000,
    "hour": 3_600_000,
    "hours": 3_600_000,
    "day": 86_400_000,
    "days": 86_400_000,
    "week": 604_800_000,
    "weeks": 604_800_000,
    "year": 31_536_000_000,
    "years": 31_536_000_000,
}
_INTEGER_TIME_TYPES = {"int2", "int4", "int8", "smallint", "integer", "bigint"}


def _env_truthy(name: str) -> bool:
    return str(os.environ.get(name) or "").strip().lower() in {"1", "true", "yes", "on"}


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


def _interval_to_ms(interval: str) -> int:
    match = _INTERVAL_RE.match(str(interval or ""))
    if not match:
        raise ValueError(f"unsupported policy interval: {interval!r}")
    unit = match.group("unit").lower()
    if unit not in _MS_BY_UNIT:
        raise ValueError(f"unsupported policy interval unit: {interval!r}")
    return int(match.group("count")) * int(_MS_BY_UNIT[unit])


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute("SELECT to_regclass(?)", (str(table_name),)).fetchone()
    return bool(row and row[0] is not None)


def _column_info(conn, table_name: str, column_name: str):
    return conn.execute(
        """
        SELECT data_type, udt_name
        FROM information_schema.columns
        WHERE table_schema = ANY (current_schemas(false))
          AND table_name = ?
          AND column_name = ?
        """,
        (str(table_name), str(column_name)),
    ).fetchone()


def _column_exists(conn, table_name: str, column_name: str) -> bool:
    return _column_info(conn, table_name, column_name) is not None


def _existing_columns(conn, table_name: str, columns: Iterable[str]) -> tuple[str, ...]:
    return tuple(str(col) for col in columns if _column_exists(conn, table_name, str(col)))


def _is_integer_time(conn, table_name: str, column_name: str) -> bool:
    row = _column_info(conn, table_name, column_name)
    if row is None:
        return False
    return str(row["udt_name"] or row["data_type"] or "").lower() in _INTEGER_TIME_TYPES


def _is_hypertable(conn, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n
          ON n.oid = c.relnamespace
        JOIN timescaledb_information.hypertables h
          ON h.hypertable_schema = n.nspname
         AND h.hypertable_name = c.relname
        WHERE c.oid = to_regclass(?)
        """,
        (str(table_name),),
    ).fetchone()
    return bool(row)


def _create_integer_now_func(conn) -> None:
    schema = quote_ident(schema_name())
    conn.execute(
        f"""
        CREATE OR REPLACE FUNCTION {schema}.unix_ms_now()
        RETURNS BIGINT
        LANGUAGE SQL
        STABLE
        AS $$
          SELECT (EXTRACT(EPOCH FROM now()) * 1000)::BIGINT
        $$
        """
    )


def _unique_constraints(conn, table_name: str) -> list[tuple[str, str, tuple[str, ...]]]:
    rows = conn.execute(
        """
        SELECT
          c.conname,
          c.contype,
          array_agg(a.attname ORDER BY u.ord) AS columns
        FROM pg_constraint c
        JOIN unnest(c.conkey) WITH ORDINALITY AS u(attnum, ord) ON TRUE
        JOIN pg_attribute a
          ON a.attrelid = c.conrelid
         AND a.attnum = u.attnum
        WHERE c.conrelid = to_regclass(?)
          AND c.contype IN ('p', 'u')
        GROUP BY c.conname, c.contype
        ORDER BY CASE WHEN c.contype = 'p' THEN 0 ELSE 1 END, c.conname
        """,
        (str(table_name),),
    ).fetchall()
    out: list[tuple[str, str, tuple[str, ...]]] = []
    for row in rows or []:
        cols = tuple(str(col) for col in (row["columns"] or ()))
        out.append((str(row["conname"]), str(row["contype"]), cols))
    return out


def _unique_indexes(conn, table_name: str) -> list[tuple[str, tuple[str, ...]]]:
    rows = conn.execute(
        """
        SELECT
          idx.relname AS index_name,
          array_agg(att.attname ORDER BY keys.ord) AS columns
        FROM pg_index i
        JOIN pg_class tbl ON tbl.oid = i.indrelid
        JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
        JOIN pg_class idx ON idx.oid = i.indexrelid
        JOIN unnest(i.indkey) WITH ORDINALITY AS keys(attnum, ord) ON keys.attnum > 0
        JOIN pg_attribute att
          ON att.attrelid = tbl.oid
         AND att.attnum = keys.attnum
        WHERE i.indrelid = to_regclass(?)
          AND i.indisunique
          AND NOT i.indisprimary
          AND ns.nspname = ANY (current_schemas(false))
        GROUP BY idx.relname
        ORDER BY idx.relname
        """,
        (str(table_name),),
    ).fetchall()
    out: list[tuple[str, tuple[str, ...]]] = []
    for row in rows or []:
        out.append((str(row["index_name"]), tuple(str(col) for col in (row["columns"] or ()))))
    return out


def _create_lookup_index(conn, table_name: str, columns: tuple[str, ...]) -> None:
    if not columns:
        return
    index_name = _index_name("idx", table_name, *columns, "lookup")
    column_sql = ", ".join(_ident(col) for col in columns)
    conn.execute(f"CREATE INDEX IF NOT EXISTS {_ident(index_name)} ON {_ident(table_name)} ({column_sql})")


def _create_timescale_unique_index(
    conn,
    table_name: str,
    columns: tuple[str, ...],
    time_column: str,
) -> None:
    if not columns:
        return
    index_columns = tuple(dict.fromkeys((*columns, time_column)))
    index_name = _index_name("uq", table_name, *index_columns)
    column_sql = ", ".join(_ident(col) for col in index_columns)
    conn.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {_ident(index_name)} ON {_ident(table_name)} ({column_sql})")


def _normalize_constraints_for_hypertable(conn, table_name: str, time_column: str) -> None:
    primary_key: tuple[str, ...] | None = None
    dropped_primary = False
    for constraint_name, constraint_type, columns in _unique_constraints(conn, table_name):
        if time_column in columns:
            continue
        if constraint_type == "p":
            primary_key = columns
            dropped_primary = True
        conn.execute(
            f"ALTER TABLE {_ident(table_name)} DROP CONSTRAINT IF EXISTS {_ident(constraint_name)}"
        )
        if constraint_type == "u":
            _create_timescale_unique_index(conn, table_name, columns, time_column)
            _create_lookup_index(conn, table_name, columns)

    for index_name, columns in _unique_indexes(conn, table_name):
        if time_column in columns:
            continue
        conn.execute(f"DROP INDEX IF EXISTS {_ident(index_name)}")
        _create_timescale_unique_index(conn, table_name, columns, time_column)
        _create_lookup_index(conn, table_name, columns)

    if dropped_primary and primary_key:
        new_columns = tuple(dict.fromkeys((*primary_key, time_column)))
        pk_name = _index_name("pk", table_name, time_column)
        column_sql = ", ".join(_ident(col) for col in new_columns)
        conn.execute(
            f"ALTER TABLE {_ident(table_name)} "
            f"ADD CONSTRAINT {_ident(pk_name)} PRIMARY KEY ({column_sql})"
        )


def _create_hypertable(conn, table_name: str, spec: Hypertable) -> None:
    if not _table_exists(conn, table_name):
        return
    time_column = spec.time_column
    if not _column_exists(conn, table_name, time_column):
        return
    if _is_hypertable(conn, table_name):
        return

    integer_time = _is_integer_time(conn, table_name, time_column)
    _normalize_constraints_for_hypertable(conn, table_name, time_column)
    if integer_time:
        conn.execute(
            """
            SELECT create_hypertable(
              ?::regclass,
              ?,
              chunk_time_interval => ?::bigint,
              if_not_exists => TRUE,
              migrate_data => TRUE
            )
            """,
            (str(table_name), str(time_column), int(_interval_to_ms(spec.chunk))),
        )
        conn.execute(
            "SELECT set_integer_now_func(?::regclass, ?)",
            (str(table_name), f"{schema_name()}.unix_ms_now"),
        )
        return

    conn.execute(
        """
        SELECT create_hypertable(
          ?::regclass,
          ?,
          chunk_time_interval => ?::interval,
          if_not_exists => TRUE,
          migrate_data => TRUE
        )
        """,
        (str(table_name), str(time_column), str(spec.chunk)),
    )


def _enable_compression(conn, table_name: str, spec: Hypertable) -> None:
    if not spec.compress_after or not _table_exists(conn, table_name) or not _is_hypertable(conn, table_name):
        return
    time_column = spec.time_column
    segment_columns = _existing_columns(conn, table_name, spec.segmentby)
    options = [
        "timescaledb.compress",
        f"timescaledb.compress_orderby = '{_ident(time_column)} DESC'",
    ]
    if segment_columns:
        options.append(
            "timescaledb.compress_segmentby = '"
            + ", ".join(_ident(col) for col in segment_columns)
            + "'"
        )
    conn.execute(f"ALTER TABLE {_ident(table_name)} SET ({', '.join(options)})")
    if _is_integer_time(conn, table_name, time_column):
        conn.execute(
            "SELECT add_compression_policy(?::regclass, ?::bigint, if_not_exists => TRUE)",
            (str(table_name), int(_interval_to_ms(spec.compress_after))),
        )
    else:
        conn.execute(
            "SELECT add_compression_policy(?::regclass, ?::interval, if_not_exists => TRUE)",
            (str(table_name), str(spec.compress_after)),
        )


def _enable_retention(conn, table_name: str, spec: Hypertable) -> None:
    if not spec.retain or not _table_exists(conn, table_name) or not _is_hypertable(conn, table_name):
        return
    if _is_integer_time(conn, table_name, spec.time_column):
        conn.execute(
            "SELECT add_retention_policy(?::regclass, ?::bigint, if_not_exists => TRUE)",
            (str(table_name), int(_interval_to_ms(spec.retain))),
        )
    else:
        conn.execute(
            "SELECT add_retention_policy(?::regclass, ?::interval, if_not_exists => TRUE)",
            (str(table_name), str(spec.retain)),
        )


def up(conn) -> None:
    if _env_truthy("TRADING_UNIT_TEST_SCHEMA_FAST"):
        return
    conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")
    _create_integer_now_func(conn)
    for table_name, spec in sorted(TABLE_CLASS.items()):
        if not isinstance(spec, Hypertable):
            continue
        _create_hypertable(conn, table_name, spec)
    for table_name, spec in sorted(TABLE_CLASS.items()):
        if not isinstance(spec, Hypertable):
            continue
        _enable_compression(conn, table_name, spec)
        _enable_retention(conn, table_name, spec)
