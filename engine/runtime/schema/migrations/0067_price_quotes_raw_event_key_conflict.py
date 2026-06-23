"""Align raw price-event conflict keys with deterministic producer event keys."""

from __future__ import annotations

id = 67
description = "price quotes raw event key conflict alignment"


def _quote_ident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute("SELECT to_regclass(?)", (str(table_name),)).fetchone()
    return bool(row and row[0] is not None)


def _table_has_columns(conn, table_name: str, *column_names: str) -> bool:
    for column_name in column_names:
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
        if not row:
            return False
    return True


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
    columns = row[1] if len(row) > 1 else ()
    return str(row[0] or ""), tuple(str(col) for col in (columns or ()))


def up(conn) -> None:
    table_name = "price_quotes_raw"
    desired = ("symbol", "provider", "event_key", "ts_ms")
    if not _table_exists(conn, table_name):
        return
    if not _table_has_columns(conn, table_name, *desired):
        return

    conn.execute(
        """
        UPDATE price_quotes_raw
        SET event_key =
          'legacy:' || symbol || ':' || provider || ':' ||
          COALESCE(CAST(event_type AS TEXT), 'legacy') || ':' ||
          COALESCE(CAST(event_ts_ms AS TEXT), CAST(ts_ms AS TEXT)) || ':' ||
          CAST(ts_ms AS TEXT) || ':' ||
          COALESCE(CAST(trade_ts_ms AS TEXT), CAST(ts_ms AS TEXT)) || ':' ||
          COALESCE(CAST(quote_ts_ms AS TEXT), CAST(ts_ms AS TEXT))
        WHERE event_key IS NULL OR btrim(event_key) = ''
        """
    )
    conn.execute(
        """
        DELETE FROM price_quotes_raw older
        USING price_quotes_raw newer
        WHERE older.ctid < newer.ctid
          AND older.symbol = newer.symbol
          AND older.provider = newer.provider
          AND older.event_key = newer.event_key
          AND older.ts_ms = newer.ts_ms
        """
    )

    pk_name, columns = _primary_key(conn, table_name)
    if columns == desired and pk_name == "price_quotes_raw_pkey":
        return
    if pk_name:
        conn.execute(f"ALTER TABLE price_quotes_raw DROP CONSTRAINT IF EXISTS {_quote_ident(pk_name)}")
    conn.execute(
        """
        ALTER TABLE price_quotes_raw
        ADD CONSTRAINT price_quotes_raw_pkey
        PRIMARY KEY(symbol, provider, event_key, ts_ms)
        """
    )
