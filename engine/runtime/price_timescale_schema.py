"""Canonical Timescale price-sidecar schema definitions."""

from __future__ import annotations

PRICE_TIMESCALE_TABLES: tuple[str, ...] = ("price_ticks", "price_quotes", "price_quotes_raw")

PRICE_TIMESCALE_TABLE_COLUMN_SPECS: dict[str, tuple[tuple[str, str], ...]] = {
    "price_ticks": (
        ("time", "TIMESTAMPTZ NOT NULL"),
        ("symbol", "TEXT NOT NULL"),
        ("last", "DOUBLE PRECISION"),
        ("source", "TEXT"),
        ("provider", "TEXT"),
        ("bid", "DOUBLE PRECISION"),
        ("ask", "DOUBLE PRECISION"),
        ("spread", "DOUBLE PRECISION"),
        ("volume", "DOUBLE PRECISION"),
        ("latency_ms", "INTEGER"),
        ("provider_score", "DOUBLE PRECISION"),
        ("last_update_ts_ms", "BIGINT"),
        ("ingest_ts_ms", "BIGINT"),
    ),
    "price_quotes": (
        ("time", "TIMESTAMPTZ NOT NULL"),
        ("symbol", "TEXT NOT NULL"),
        ("last", "DOUBLE PRECISION"),
        ("bid", "DOUBLE PRECISION"),
        ("ask", "DOUBLE PRECISION"),
        ("spread", "DOUBLE PRECISION"),
        ("volume", "DOUBLE PRECISION"),
        ("source", "TEXT"),
        ("last_trade_ts_ms", "BIGINT"),
        ("last_quote_ts_ms", "BIGINT"),
        ("last_update_ts_ms", "BIGINT"),
    ),
    "price_quotes_raw": (
        ("time", "TIMESTAMPTZ NOT NULL"),
        ("symbol", "TEXT NOT NULL"),
        ("provider", "TEXT NOT NULL"),
        ("event_key", "TEXT NOT NULL"),
        ("event_type", "TEXT"),
        ("event_ts_ms", "BIGINT"),
        ("last", "DOUBLE PRECISION"),
        ("bid", "DOUBLE PRECISION"),
        ("ask", "DOUBLE PRECISION"),
        ("spread", "DOUBLE PRECISION"),
        ("volume", "DOUBLE PRECISION"),
        ("trade_ts_ms", "BIGINT"),
        ("quote_ts_ms", "BIGINT"),
        ("ingest_ts_ms", "BIGINT"),
        ("source", "TEXT"),
    ),
}

PRICE_TIMESCALE_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "price_ticks": ("symbol", "time"),
    "price_quotes": ("symbol", "time"),
    "price_quotes_raw": ("symbol", "provider", "event_key", "time"),
}

PRICE_TIMESCALE_TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    table_name: tuple(column for column, _sql_type in specs)
    for table_name, specs in PRICE_TIMESCALE_TABLE_COLUMN_SPECS.items()
}

PRICE_TIMESCALE_STAGING_TABLE_NAMES: dict[str, str] = {
    "price_ticks": "price_ticks_write_staging",
    "price_quotes": "price_quotes_write_staging",
    "price_quotes_raw": "price_quotes_raw_write_staging",
}

PRICE_TIMESCALE_STAGING_TABLE_COLUMN_SPECS: dict[str, tuple[tuple[str, str], ...]] = {
    table_name: (
        ("staging_session", "TEXT NOT NULL"),
        ("staging_ordinal", "BIGINT NOT NULL"),
        *specs,
    )
    for table_name, specs in PRICE_TIMESCALE_TABLE_COLUMN_SPECS.items()
}

PRICE_TIMESCALE_STAGING_TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    PRICE_TIMESCALE_STAGING_TABLE_NAMES[table_name]: tuple(column for column, _sql_type in specs)
    for table_name, specs in PRICE_TIMESCALE_STAGING_TABLE_COLUMN_SPECS.items()
}

PRICE_TIMESCALE_COPY_TYPES: dict[str, tuple[str, ...]] = {
    "price_ticks": (
        "text",
        "int8",
        "timestamptz",
        "text",
        "float8",
        "text",
        "text",
        "float8",
        "float8",
        "float8",
        "float8",
        "int4",
        "float8",
        "int8",
        "int8",
    ),
    "price_quotes": (
        "text",
        "int8",
        "timestamptz",
        "text",
        "float8",
        "float8",
        "float8",
        "float8",
        "float8",
        "text",
        "int8",
        "int8",
        "int8",
    ),
    "price_quotes_raw": (
        "text",
        "int8",
        "timestamptz",
        "text",
        "text",
        "text",
        "text",
        "int8",
        "float8",
        "float8",
        "float8",
        "float8",
        "float8",
        "int8",
        "int8",
        "int8",
        "text",
    ),
}

PRICE_TIMESCALE_SCHEMA_INDEXES: tuple[str, ...] = (
    "price_ticks_pkey",
    "price_quotes_pkey",
    "price_quotes_raw_pkey",
    "idx_price_ticks_time_desc",
    "idx_price_quotes_time_desc",
    "idx_price_quotes_raw_time_desc",
    "idx_price_ticks_write_staging_session",
    "idx_price_quotes_write_staging_session",
    "idx_price_quotes_raw_write_staging_session",
)


def quote_ident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def price_timescale_table_body(table_name: str) -> str:
    table = str(table_name)
    specs = PRICE_TIMESCALE_TABLE_COLUMN_SPECS[table]
    pk_columns = PRICE_TIMESCALE_PRIMARY_KEYS[table]
    column_sql = ",\n          ".join(
        f"{quote_ident(column) if column == 'time' else column} {sql_type}"
        for column, sql_type in specs
    )
    pk_sql = ", ".join(quote_ident(column) if column == "time" else column for column in pk_columns)
    return f"{column_sql},\n          PRIMARY KEY({pk_sql})"


def price_timescale_create_table_sql(relation_ref: str, table_name: str) -> str:
    return f"CREATE TABLE IF NOT EXISTS {relation_ref} (\n          {price_timescale_table_body(table_name)}\n        )"


def price_timescale_staging_table_ddl(schema_ref: str, table_name: str) -> str:
    table = str(table_name)
    staging_table = PRICE_TIMESCALE_STAGING_TABLE_NAMES[table]
    column_defs = ",\n          ".join(
        f"{quote_ident(column)} {sql_type}"
        for column, sql_type in PRICE_TIMESCALE_STAGING_TABLE_COLUMN_SPECS[table]
    )
    return f"""
        CREATE UNLOGGED TABLE IF NOT EXISTS {schema_ref}.{quote_ident(staging_table)} (
          {column_defs}
        )
        """


def price_timescale_time_desc_index_name(table_name: str) -> str:
    return f"idx_{str(table_name)}_time_desc"


def price_timescale_time_ref(table_alias: str = "") -> str:
    alias = str(table_alias or "").strip()
    prefix = f"{quote_ident(alias)}." if alias else ""
    return f"{prefix}{quote_ident('time')}"


def price_timescale_ts_ms_expr(table_alias: str = "") -> str:
    """Return the API-compatible millisecond timestamp expression for Timescale rows."""
    return f"(EXTRACT(EPOCH FROM {price_timescale_time_ref(table_alias)}) * 1000)::BIGINT"


def price_timescale_time_after_ms_predicate(
    *,
    table_alias: str = "",
    placeholder: str = "%s",
) -> str:
    """Return an index-friendly predicate against the canonical Timescale time column."""
    return f"{price_timescale_time_ref(table_alias)} > TO_TIMESTAMP({str(placeholder)} / 1000.0)"


def price_timescale_time_desc_index_sql(relation_ref: str, table_name: str) -> str:
    return (
        f"CREATE INDEX IF NOT EXISTS {price_timescale_time_desc_index_name(table_name)} "
        f"ON {relation_ref} ({quote_ident('time')} DESC)"
    )


PRICE_TIMESCALE_BASELINE_TABLE_DEFS: tuple[tuple[str, str], ...] = tuple(
    (table_name, price_timescale_table_body(table_name))
    for table_name in PRICE_TIMESCALE_TABLES
)


__all__ = [
    "PRICE_TIMESCALE_BASELINE_TABLE_DEFS",
    "PRICE_TIMESCALE_COPY_TYPES",
    "PRICE_TIMESCALE_PRIMARY_KEYS",
    "PRICE_TIMESCALE_SCHEMA_INDEXES",
    "PRICE_TIMESCALE_STAGING_TABLE_COLUMNS",
    "PRICE_TIMESCALE_STAGING_TABLE_COLUMN_SPECS",
    "PRICE_TIMESCALE_STAGING_TABLE_NAMES",
    "PRICE_TIMESCALE_TABLES",
    "PRICE_TIMESCALE_TABLE_COLUMNS",
    "PRICE_TIMESCALE_TABLE_COLUMN_SPECS",
    "price_timescale_create_table_sql",
    "price_timescale_staging_table_ddl",
    "price_timescale_table_body",
    "price_timescale_time_after_ms_predicate",
    "price_timescale_time_desc_index_name",
    "price_timescale_time_desc_index_sql",
    "price_timescale_time_ref",
    "price_timescale_ts_ms_expr",
    "quote_ident",
]
