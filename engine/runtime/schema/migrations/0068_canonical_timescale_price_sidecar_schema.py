"""Canonicalize Timescale price sidecar tables onto the shared schema."""

from __future__ import annotations

import importlib

from engine.runtime.price_timescale_schema import (
    PRICE_TIMESCALE_TABLE_COLUMN_SPECS,
    PRICE_TIMESCALE_PRIMARY_KEYS,
    PRICE_TIMESCALE_TABLES,
    price_timescale_create_table_sql,
    price_timescale_time_desc_index_sql,
    quote_ident,
)
from engine.runtime.schema.table_classification import TABLE_CLASS

id = 68
description = "canonical Timescale price sidecar schema"


def _row_value(row, key: str, index: int = 0):
    if row is None:
        return None
    try:
        return row[key]
    except Exception:
        try:
            return row[index]
        except Exception:
            return None


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute("SELECT to_regclass(?)", (str(table_name),)).fetchone()
    return bool(_row_value(row, "to_regclass", 0))


def _resolved_table_schema(conn, table_name: str) -> str:
    row = conn.execute(
        """
        SELECT n.nspname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.oid = to_regclass(?)
        """,
        (str(table_name),),
    ).fetchone()
    return str(_row_value(row, "nspname", 0) or "").strip()


def _column_exists(conn, table_name: str, column_name: str) -> bool:
    schema = _resolved_table_schema(conn, table_name)
    if schema:
        row = conn.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = ?
              AND table_name = ?
              AND column_name = ?
            """,
            (schema, str(table_name), str(column_name)),
        ).fetchone()
        return bool(row)
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = ANY (current_schemas(false))
          AND table_name = ?
          AND column_name = ?
        """,
        (str(table_name), str(column_name)),
    ).fetchone()
    return bool(row)


def _available_legacy_table_name(conn, table_name: str) -> str:
    base = f"{table_name}_legacy_ts_ms"
    if not _table_exists(conn, base):
        return base
    idx = 2
    while _table_exists(conn, f"{base}_{idx}"):
        idx += 1
    return f"{base}_{idx}"


def _rename_legacy_primary_key_if_needed(conn, table_name: str, legacy_table: str) -> None:
    pk_name, _columns = _primary_key(conn, legacy_table)
    if pk_name != f"{table_name}_pkey":
        return
    conn.execute(
        f"""
        ALTER TABLE {quote_ident(legacy_table)}
        RENAME CONSTRAINT {quote_ident(pk_name)} TO {quote_ident(f"{legacy_table}_pkey")}
        """
    )


def _add_missing_canonical_columns(conn, table_name: str) -> None:
    for column_name, sql_type in PRICE_TIMESCALE_TABLE_COLUMN_SPECS[str(table_name)]:
        if _column_exists(conn, table_name, column_name):
            continue
        ddl = str(sql_type)
        drop_default = False
        if "NOT NULL" in ddl.upper():
            if column_name == "time":
                ddl = "TIMESTAMPTZ NOT NULL DEFAULT to_timestamp(0)"
                drop_default = True
            elif "TEXT" in ddl.upper():
                ddl = "TEXT NOT NULL DEFAULT ''"
                drop_default = True
        conn.execute(f"ALTER TABLE {quote_ident(table_name)} ADD COLUMN IF NOT EXISTS {quote_ident(column_name)} {ddl}")
        if drop_default:
            conn.execute(
                f"ALTER TABLE {quote_ident(table_name)} ALTER COLUMN {quote_ident(column_name)} DROP DEFAULT"
            )


def _ensure_canonical_table(conn, table_name: str) -> None:
    relation_ref = quote_ident(table_name)
    conn.execute(price_timescale_create_table_sql(relation_ref, table_name))
    _add_missing_canonical_columns(conn, table_name)
    conn.execute(price_timescale_time_desc_index_sql(relation_ref, table_name))


def _rename_legacy_ts_ms_table_if_needed(conn, table_name: str) -> str:
    if not _table_exists(conn, table_name):
        return ""
    if _column_exists(conn, table_name, "time"):
        return ""
    if not _column_exists(conn, table_name, "ts_ms"):
        return ""
    legacy_table = _available_legacy_table_name(conn, table_name)
    conn.execute(f"ALTER TABLE {quote_ident(table_name)} RENAME TO {quote_ident(legacy_table)}")
    _rename_legacy_primary_key_if_needed(conn, table_name, legacy_table)
    return str(legacy_table)


def _legacy_col(conn, table_name: str, column_name: str, default_sql: str = "NULL") -> str:
    return quote_ident(column_name) if _column_exists(conn, table_name, column_name) else str(default_sql)


def _legacy_time_expr(conn, table_name: str) -> str:
    if _column_exists(conn, table_name, "time"):
        return quote_ident("time")
    return "to_timestamp((ts_ms)::double precision / 1000.0)"


def _copy_legacy_prices_to_price_ticks(conn) -> None:
    if not _table_exists(conn, "prices"):
        return
    if not (_column_exists(conn, "prices", "ts_ms") and _column_exists(conn, "prices", "symbol")):
        return
    price_expr = "price" if _column_exists(conn, "prices", "price") else "NULL"
    px_expr = "px" if _column_exists(conn, "prices", "px") else "NULL"
    source_expr = "source" if _column_exists(conn, "prices", "source") else "NULL"
    conn.execute(
        f"""
        INSERT INTO price_ticks("time", symbol, last, source, provider)
        SELECT DISTINCT ON (symbol, "time")
          "time", symbol, last, source, source AS provider
        FROM (
          SELECT
            to_timestamp((ts_ms)::double precision / 1000.0) AS "time",
            symbol,
            COALESCE({price_expr}, {px_expr}) AS last,
            {source_expr} AS source
          FROM prices
          WHERE ts_ms IS NOT NULL
            AND symbol IS NOT NULL
        ) migrated_prices
        WHERE last IS NOT NULL
        ORDER BY symbol, "time"
        ON CONFLICT(symbol, "time") DO UPDATE SET
          last=EXCLUDED.last,
          source=EXCLUDED.source,
          provider=EXCLUDED.provider
        """
    )


def _copy_legacy_price_quotes(conn, legacy_table: str) -> None:
    if not legacy_table:
        return
    conn.execute(
        f"""
        INSERT INTO price_quotes(
          "time", symbol, last, bid, ask, spread, volume, source,
          last_trade_ts_ms, last_quote_ts_ms, last_update_ts_ms
        )
        SELECT DISTINCT ON (symbol, "time")
          "time", symbol, last, bid, ask, spread, volume, source,
          last_trade_ts_ms, last_quote_ts_ms, last_update_ts_ms
        FROM (
          SELECT
            {_legacy_time_expr(conn, legacy_table)} AS "time",
            {_legacy_col(conn, legacy_table, "symbol", "''")} AS symbol,
            {_legacy_col(conn, legacy_table, "last")} AS last,
            {_legacy_col(conn, legacy_table, "bid")} AS bid,
            {_legacy_col(conn, legacy_table, "ask")} AS ask,
            {_legacy_col(conn, legacy_table, "spread")} AS spread,
            {_legacy_col(conn, legacy_table, "volume")} AS volume,
            {_legacy_col(conn, legacy_table, "source")} AS source,
            {_legacy_col(conn, legacy_table, "last_trade_ts_ms")} AS last_trade_ts_ms,
            {_legacy_col(conn, legacy_table, "last_quote_ts_ms")} AS last_quote_ts_ms,
            {_legacy_col(conn, legacy_table, "last_update_ts_ms")} AS last_update_ts_ms
          FROM {quote_ident(legacy_table)}
        ) migrated_quotes
        WHERE "time" IS NOT NULL
          AND symbol IS NOT NULL
        ORDER BY symbol, "time"
        ON CONFLICT(symbol, "time") DO UPDATE SET
          last=EXCLUDED.last,
          bid=EXCLUDED.bid,
          ask=EXCLUDED.ask,
          spread=EXCLUDED.spread,
          volume=EXCLUDED.volume,
          source=EXCLUDED.source,
          last_trade_ts_ms=EXCLUDED.last_trade_ts_ms,
          last_quote_ts_ms=EXCLUDED.last_quote_ts_ms,
          last_update_ts_ms=EXCLUDED.last_update_ts_ms
        """
    )


def _legacy_raw_event_key_expr(conn, legacy_table: str) -> str:
    ts_expr = _legacy_col(conn, legacy_table, "ts_ms", "0")
    symbol_expr = _legacy_col(conn, legacy_table, "symbol", "''")
    provider_expr = _legacy_col(conn, legacy_table, "provider", "''")
    event_type = _legacy_col(conn, legacy_table, "event_type", "'legacy'")
    event_ts = _legacy_col(conn, legacy_table, "event_ts_ms", ts_expr)
    trade_ts = _legacy_col(conn, legacy_table, "trade_ts_ms", ts_expr)
    quote_ts = _legacy_col(conn, legacy_table, "quote_ts_ms", ts_expr)
    generated = (
        f"'legacy:' || {symbol_expr} || ':' || "
        f"{provider_expr} || ':' || "
        f"COALESCE(CAST({event_type} AS TEXT), 'legacy') || ':' || "
        f"COALESCE(CAST({event_ts} AS TEXT), CAST({ts_expr} AS TEXT)) || ':' || "
        f"COALESCE(CAST({ts_expr} AS TEXT), '0') || ':' || "
        f"COALESCE(CAST({trade_ts} AS TEXT), CAST({ts_expr} AS TEXT)) || ':' || "
        f"COALESCE(CAST({quote_ts} AS TEXT), CAST({ts_expr} AS TEXT))"
    )
    if _column_exists(conn, legacy_table, "event_key"):
        return f"COALESCE(NULLIF(CAST(event_key AS TEXT), ''), {generated})"
    return generated


def _copy_legacy_price_quotes_raw(conn, legacy_table: str) -> None:
    if not legacy_table:
        return
    event_key_expr = _legacy_raw_event_key_expr(conn, legacy_table)
    conn.execute(
        f"""
        INSERT INTO price_quotes_raw(
          "time", symbol, provider, event_key, event_type, event_ts_ms,
          last, bid, ask, spread, volume,
          trade_ts_ms, quote_ts_ms, ingest_ts_ms, source
        )
        SELECT DISTINCT ON (symbol, provider, event_key, "time")
          "time", symbol, provider, event_key, event_type, event_ts_ms,
          last, bid, ask, spread, volume,
          trade_ts_ms, quote_ts_ms, ingest_ts_ms, source
        FROM (
          SELECT
            {_legacy_time_expr(conn, legacy_table)} AS "time",
            {_legacy_col(conn, legacy_table, "symbol", "''")} AS symbol,
            {_legacy_col(conn, legacy_table, "provider", "''")} AS provider,
            {event_key_expr} AS event_key,
            {_legacy_col(conn, legacy_table, "event_type", "'legacy'")} AS event_type,
            {_legacy_col(conn, legacy_table, "event_ts_ms", _legacy_col(conn, legacy_table, "ts_ms", "0"))} AS event_ts_ms,
            {_legacy_col(conn, legacy_table, "last")} AS last,
            {_legacy_col(conn, legacy_table, "bid")} AS bid,
            {_legacy_col(conn, legacy_table, "ask")} AS ask,
            {_legacy_col(conn, legacy_table, "spread")} AS spread,
            {_legacy_col(conn, legacy_table, "volume")} AS volume,
            {_legacy_col(conn, legacy_table, "trade_ts_ms", _legacy_col(conn, legacy_table, "ts_ms", "0"))} AS trade_ts_ms,
            {_legacy_col(conn, legacy_table, "quote_ts_ms", _legacy_col(conn, legacy_table, "ts_ms", "0"))} AS quote_ts_ms,
            {_legacy_col(conn, legacy_table, "ingest_ts_ms", _legacy_col(conn, legacy_table, "ts_ms", "0"))} AS ingest_ts_ms,
            {_legacy_col(conn, legacy_table, "source", _legacy_col(conn, legacy_table, "provider", "''"))} AS source
          FROM {quote_ident(legacy_table)}
        ) migrated_raw
        WHERE "time" IS NOT NULL
          AND symbol IS NOT NULL
          AND provider IS NOT NULL
          AND event_key IS NOT NULL
        ORDER BY symbol, provider, event_key, "time"
        ON CONFLICT(symbol, provider, event_key, "time") DO UPDATE SET
          event_type=EXCLUDED.event_type,
          event_ts_ms=EXCLUDED.event_ts_ms,
          last=EXCLUDED.last,
          bid=EXCLUDED.bid,
          ask=EXCLUDED.ask,
          spread=EXCLUDED.spread,
          volume=EXCLUDED.volume,
          trade_ts_ms=EXCLUDED.trade_ts_ms,
          quote_ts_ms=EXCLUDED.quote_ts_ms,
          ingest_ts_ms=EXCLUDED.ingest_ts_ms,
          source=EXCLUDED.source
        """
    )


def _primary_key(conn, table_name: str) -> tuple[str, tuple[str, ...]]:
    row = conn.execute(
        """
        SELECT c.conname, array_agg(a.attname ORDER BY keys.ord) AS columns
        FROM pg_constraint c
        JOIN unnest(c.conkey) WITH ORDINALITY AS keys(attnum, ord) ON TRUE
        JOIN pg_attribute a
          ON a.attrelid = c.conrelid
         AND a.attnum = keys.attnum
        WHERE c.conrelid = to_regclass(?)
          AND c.contype = 'p'
        GROUP BY c.conname
        LIMIT 1
        """,
        (str(table_name),),
    ).fetchone()
    if not row:
        return "", ()
    columns = _row_value(row, "columns", 1) or ()
    return str(_row_value(row, "conname", 0) or ""), tuple(str(column) for column in columns)


def _dedupe_for_primary_key(conn, table_name: str, columns: tuple[str, ...]) -> None:
    predicates = " AND ".join(
        f"older.{quote_ident(column)} = newer.{quote_ident(column)}" for column in columns
    )
    if not predicates:
        return
    conn.execute(
        f"""
        DELETE FROM {quote_ident(table_name)} older
        USING {quote_ident(table_name)} newer
        WHERE older.ctid < newer.ctid
          AND {predicates}
        """
    )


def _ensure_primary_key(conn, table_name: str) -> None:
    desired = PRICE_TIMESCALE_PRIMARY_KEYS[str(table_name)]
    desired_name = f"{table_name}_pkey"
    pk_name, columns = _primary_key(conn, table_name)
    if columns == desired and pk_name == desired_name:
        return
    _dedupe_for_primary_key(conn, table_name, desired)
    if pk_name:
        conn.execute(f"ALTER TABLE {quote_ident(table_name)} DROP CONSTRAINT IF EXISTS {quote_ident(pk_name)}")
    column_sql = ", ".join(quote_ident(column) for column in desired)
    conn.execute(
        f"""
        ALTER TABLE {quote_ident(table_name)}
        ADD CONSTRAINT {quote_ident(desired_name)}
        PRIMARY KEY({column_sql})
        """
    )


def _ensure_hypertables(conn) -> None:
    hypertables = importlib.import_module("engine.runtime.schema.migrations.0002_hypertables")
    conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")
    for table_name in PRICE_TIMESCALE_TABLES:
        spec = TABLE_CLASS.get(table_name)
        if spec is not None:
            hypertables._create_hypertable(conn, table_name, spec)


def up(conn) -> None:
    legacy_quotes = _rename_legacy_ts_ms_table_if_needed(conn, "price_quotes")
    legacy_raw = _rename_legacy_ts_ms_table_if_needed(conn, "price_quotes_raw")

    for table_name in PRICE_TIMESCALE_TABLES:
        _ensure_canonical_table(conn, table_name)

    _copy_legacy_prices_to_price_ticks(conn)
    _copy_legacy_price_quotes(conn, legacy_quotes)
    _copy_legacy_price_quotes_raw(conn, legacy_raw)
    for table_name in PRICE_TIMESCALE_TABLES:
        _ensure_primary_key(conn, table_name)
    _ensure_hypertables(conn)
