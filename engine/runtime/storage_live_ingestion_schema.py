from __future__ import annotations

from typing import Any, Callable, Dict

WarnNonfatal = Callable[..., None]


def _is_sqlite_connection(con: Any) -> bool:
    module = str(type(con).__module__ or "").lower()
    if "sqlite" in module:
        return True
    if "storage_pg" in module or "psycopg" in module:
        return False
    raw = getattr(con, "raw", None)
    raw_module = str(type(raw).__module__ or "").lower()
    if "storage_pg" in raw_module or "psycopg" in raw_module:
        return False
    try:
        con.execute("SELECT sqlite_version()").fetchone()
        return True
    except Exception:
        rollback = getattr(con, "rollback", None)
        if callable(rollback):
            try:
                rollback()
            # system-audit: ignore[silent_except] no-op-guard: allow sqlite probe rollback is best-effort cleanup.
            except Exception:
                pass
        return False

OWNED_LIVE_TABLE_COLUMN_SPECS: Dict[str, Dict[str, Dict[str, object]]] = {
    "prices": {
        "ts_ms": {"type": "INTEGER", "pk": 2},
        "symbol": {"type": "TEXT", "pk": 1},
        "price": {"type": "REAL", "pk": 0},
        "px": {"type": "REAL", "pk": 0},
        "source": {"type": "TEXT", "pk": 0},
    },
    "price_quotes": {
        "ts_ms": {"type": "INTEGER", "pk": 2},
        "symbol": {"type": "TEXT", "pk": 1},
        "last": {"type": "REAL", "pk": 0},
        "bid": {"type": "REAL", "pk": 0},
        "ask": {"type": "REAL", "pk": 0},
        "spread": {"type": "REAL", "pk": 0},
        "volume": {"type": "REAL", "pk": 0},
        "source": {"type": "TEXT", "pk": 0},
        "last_trade_ts_ms": {"type": "INTEGER", "pk": 0},
        "last_quote_ts_ms": {"type": "INTEGER", "pk": 0},
        "last_update_ts_ms": {"type": "INTEGER", "pk": 0},
    },
    "price_quotes_raw": {
        "ts_ms": {"type": "INTEGER", "pk": 0},
        "symbol": {"type": "TEXT", "pk": 1},
        "provider": {"type": "TEXT", "pk": 2},
        "event_key": {"type": "TEXT", "pk": 3},
        "event_type": {"type": "TEXT", "pk": 0},
        "event_ts_ms": {"type": "INTEGER", "pk": 0},
        "last": {"type": "REAL", "pk": 0},
        "bid": {"type": "REAL", "pk": 0},
        "ask": {"type": "REAL", "pk": 0},
        "spread": {"type": "REAL", "pk": 0},
        "volume": {"type": "REAL", "pk": 0},
        "trade_ts_ms": {"type": "INTEGER", "pk": 0},
        "quote_ts_ms": {"type": "INTEGER", "pk": 0},
        "ingest_ts_ms": {"type": "INTEGER", "pk": 0},
        "source": {"type": "TEXT", "pk": 0},
    },
    "price_provider_health": {
        "ts_ms": {"type": "INTEGER", "pk": 2},
        "provider": {"type": "TEXT", "pk": 1},
        "ok": {"type": "INTEGER", "pk": 0},
        "latency_ms": {"type": "INTEGER", "pk": 0},
        "n_symbols": {"type": "INTEGER", "pk": 0},
        "error": {"type": "TEXT", "pk": 0},
        "last_success_ts_ms": {"type": "INTEGER", "pk": 0},
        "error_count": {"type": "INTEGER", "pk": 0},
    },
    "ingestion_pipeline_health": {
        "ts_ms": {"type": "INTEGER", "pk": 2},
        "pipeline": {"type": "TEXT", "pk": 1},
        "ok": {"type": "INTEGER", "pk": 0},
        "latency_ms": {"type": "INTEGER", "pk": 0},
        "raw_rows": {"type": "INTEGER", "pk": 0},
        "event_rows": {"type": "INTEGER", "pk": 0},
        "last_ingested_ts_ms": {"type": "INTEGER", "pk": 0},
        "error": {"type": "TEXT", "pk": 0},
        "meta_json": {"type": "TEXT", "pk": 0},
    },
    "price_feed_lock": {
        "id": {"type": "INTEGER", "pk": 1},
        "owner": {"type": "TEXT", "pk": 0},
        "pid": {"type": "INTEGER", "pk": 0},
        "ts_ms": {"type": "INTEGER", "pk": 0},
    },
    "options_symbol_ingestion_state": {
        "symbol": {"type": "TEXT", "pk": 1},
        "provider": {"type": "TEXT", "pk": 0},
        "consecutive_failures": {"type": "INTEGER", "pk": 0},
        "total_failures": {"type": "INTEGER", "pk": 0},
        "last_failure_ts_ms": {"type": "INTEGER", "pk": 0},
        "last_failure_error": {"type": "TEXT", "pk": 0},
        "last_success_ts_ms": {"type": "INTEGER", "pk": 0},
        "last_fresh_snapshot_ts_ms": {"type": "INTEGER", "pk": 0},
        "last_cached_snapshot_ts_ms": {"type": "INTEGER", "pk": 0},
        "last_fallback_ts_ms": {"type": "INTEGER", "pk": 0},
        "last_row_count": {"type": "INTEGER", "pk": 0},
        "disabled_until_ts_ms": {"type": "INTEGER", "pk": 0},
        "updated_ts_ms": {"type": "INTEGER", "pk": 0},
    },
}

OWNED_LIVE_TABLE_REQUIRED_INDEXES: Dict[str, tuple[str, ...]] = {
    "prices": ("idx_prices_symbol_ts",),
    "price_quotes": (
        "idx_price_quotes_symbol_ts",
        "idx_price_quotes_ts",
    ),
    "price_quotes_raw": (
        "idx_price_quotes_raw_symbol_ts",
        "idx_price_quotes_raw_provider_ts",
        "idx_price_quotes_raw_ts",
        "idx_price_quotes_raw_provider_event_ts",
    ),
    "price_provider_health": (
        "idx_price_provider_health_ts",
        "idx_price_provider_health_provider",
    ),
    "ingestion_pipeline_health": (
        "idx_ingestion_pipeline_health_ts",
        "idx_ingestion_pipeline_health_pipeline",
    ),
    "price_feed_lock": (),
    "options_symbol_ingestion_state": ("idx_options_symbol_ingestion_disabled",),
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS prices (
  ts_ms INTEGER NOT NULL,
  symbol TEXT NOT NULL,
  price REAL,
  px REAL,
  source TEXT,
  PRIMARY KEY(symbol, ts_ms)
);
CREATE INDEX IF NOT EXISTS idx_prices_symbol_ts
  ON prices(symbol, ts_ms);

CREATE TABLE IF NOT EXISTS price_quotes (
  ts_ms INTEGER NOT NULL,
  symbol TEXT NOT NULL,
  last REAL,
  bid REAL,
  ask REAL,
  spread REAL,
  volume REAL,
  source TEXT,
  PRIMARY KEY(symbol, ts_ms)
);
CREATE INDEX IF NOT EXISTS idx_price_quotes_symbol_ts
  ON price_quotes(symbol, ts_ms);
CREATE INDEX IF NOT EXISTS idx_price_quotes_ts
  ON price_quotes(ts_ms);

CREATE TABLE IF NOT EXISTS price_quotes_raw (
  ts_ms INTEGER NOT NULL,
  symbol TEXT NOT NULL,
  provider TEXT NOT NULL,
  event_key TEXT NOT NULL,
  event_type TEXT,
  event_ts_ms INTEGER,
  last REAL,
  bid REAL,
  ask REAL,
  spread REAL,
  volume REAL,
  trade_ts_ms INTEGER,
  quote_ts_ms INTEGER,
  ingest_ts_ms INTEGER,
  source TEXT,
  PRIMARY KEY(symbol, provider, event_key)
);
CREATE INDEX IF NOT EXISTS idx_price_quotes_raw_symbol_ts
  ON price_quotes_raw(symbol, ts_ms);
CREATE INDEX IF NOT EXISTS idx_price_quotes_raw_provider_ts
  ON price_quotes_raw(provider, ts_ms);
CREATE INDEX IF NOT EXISTS idx_price_quotes_raw_ts
  ON price_quotes_raw(ts_ms);
CREATE INDEX IF NOT EXISTS idx_price_quotes_raw_provider_event_ts
  ON price_quotes_raw(provider, event_ts_ms);

CREATE TABLE IF NOT EXISTS price_provider_health (
  ts_ms INTEGER NOT NULL,
  provider TEXT NOT NULL,
  ok INTEGER NOT NULL,
  latency_ms INTEGER,
  n_symbols INTEGER,
  error TEXT,
  last_success_ts_ms INTEGER,
  error_count INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(provider, ts_ms)
);
CREATE INDEX IF NOT EXISTS idx_price_provider_health_ts
  ON price_provider_health(ts_ms);
CREATE INDEX IF NOT EXISTS idx_price_provider_health_provider
  ON price_provider_health(provider);

CREATE TABLE IF NOT EXISTS ingestion_pipeline_health (
  ts_ms INTEGER NOT NULL,
  pipeline TEXT NOT NULL,
  ok INTEGER NOT NULL,
  latency_ms INTEGER,
  raw_rows INTEGER NOT NULL DEFAULT 0,
  event_rows INTEGER NOT NULL DEFAULT 0,
  last_ingested_ts_ms INTEGER,
  error TEXT,
  meta_json TEXT,
  PRIMARY KEY (pipeline, ts_ms)
);
CREATE INDEX IF NOT EXISTS idx_ingestion_pipeline_health_ts
  ON ingestion_pipeline_health(ts_ms);
CREATE INDEX IF NOT EXISTS idx_ingestion_pipeline_health_pipeline
  ON ingestion_pipeline_health(pipeline);

CREATE TABLE IF NOT EXISTS price_feed_lock(
  id INTEGER PRIMARY KEY,
  owner TEXT NOT NULL,
  pid INTEGER NOT NULL,
  ts_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS options_symbol_ingestion_state (
  symbol TEXT NOT NULL PRIMARY KEY,
  provider TEXT NOT NULL DEFAULT '',
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  total_failures INTEGER NOT NULL DEFAULT 0,
  last_failure_ts_ms INTEGER,
  last_failure_error TEXT,
  last_success_ts_ms INTEGER,
  last_fresh_snapshot_ts_ms INTEGER,
  last_cached_snapshot_ts_ms INTEGER,
  last_fallback_ts_ms INTEGER,
  last_row_count INTEGER NOT NULL DEFAULT 0,
  disabled_until_ts_ms INTEGER NOT NULL DEFAULT 0,
  updated_ts_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_options_symbol_ingestion_disabled
  ON options_symbol_ingestion_state(disabled_until_ts_ms);
"""


def _table_exists(con, table: str) -> bool:
    if _is_sqlite_connection(con):
        cursor = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (str(table),),
        )
        fetchone = getattr(cursor, "fetchone", None)
        if not callable(fetchone):
            return False
        row = fetchone()
        return bool(row)
    cursor = con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = current_schema()
          AND table_name=?
        LIMIT 1
        """,
        (str(table),),
    )
    fetchone = getattr(cursor, "fetchone", None)
    if not callable(fetchone):
        return False
    row = fetchone()
    return bool(row)


def _table_column_specs(con, table: str) -> Dict[str, Dict[str, object]]:
    if _is_sqlite_connection(con):
        rows = con.execute(f"PRAGMA table_info({str(table)})").fetchall() or []
        return {
            str(row[1]).strip().lower(): {
                "type": str(row[2] or "").strip().upper(),
                "pk": int(row[5] or 0),
            }
            for row in rows
        }
    rows = con.execute(
        """
        SELECT
          c.ordinal_position - 1 AS cid,
          c.column_name,
          UPPER(c.data_type),
          CASE WHEN c.is_nullable = 'NO' THEN 1 ELSE 0 END AS notnull,
          c.column_default,
          COALESCE(k.ordinal_position, 0) AS pk
        FROM information_schema.columns c
        LEFT JOIN information_schema.table_constraints tc
          ON tc.table_schema = c.table_schema
         AND tc.table_name = c.table_name
         AND tc.constraint_type = 'PRIMARY KEY'
        LEFT JOIN information_schema.key_column_usage k
          ON k.table_schema = c.table_schema
         AND k.table_name = c.table_name
         AND k.constraint_name = tc.constraint_name
         AND k.column_name = c.column_name
        WHERE c.table_schema = current_schema()
          AND c.table_name=?
        ORDER BY c.ordinal_position
        """,
        (str(table),),
    ).fetchall() or []
    return {
        str(row[1]).strip().lower(): {
            "type": str(row[2] or "").strip().upper(),
            "pk": int(row[5] or 0),
        }
        for row in rows
    }


def _type_matches(con, *, expected: str, actual: str) -> bool:
    expected_type = str(expected or "").strip().upper()
    actual_type = str(actual or "").strip().upper()
    if _is_sqlite_connection(con):
        return expected_type == actual_type
    if expected_type == "INTEGER":
        return actual_type in {"SMALLINT", "INTEGER", "BIGINT"}
    if expected_type == "REAL":
        return actual_type in {"REAL", "DOUBLE PRECISION", "NUMERIC"}
    if expected_type == "TEXT":
        return actual_type in {"TEXT", "CHARACTER", "CHARACTER VARYING"}
    return expected_type == actual_type


def _table_matches_owned_contract(con, table: str) -> bool:
    expected_specs = OWNED_LIVE_TABLE_COLUMN_SPECS[str(table)]
    actual_specs = _table_column_specs(con, str(table))
    if set(actual_specs) != set(expected_specs):
        return False
    return all(
        _type_matches(
            con,
            expected=str((expected_spec or {}).get("type") or ""),
            actual=str((actual_specs.get(column_name) or {}).get("type") or ""),
        )
        and int((actual_specs.get(column_name) or {}).get("pk") or 0)
        == int((expected_spec or {}).get("pk") or 0)
        for column_name, expected_spec in expected_specs.items()
    )


def _next_legacy_table_name(con, base: str) -> str:
    if not _table_exists(con, base):
        return base
    idx = 2
    while _table_exists(con, f"{base}_{idx}"):
        idx += 1
    return f"{base}_{idx}"


def _has_column(con, table: str, col: str) -> bool:
    return str(col).strip().lower() in _table_column_specs(con, str(table))


def _execute_script(con, script: str) -> None:
    executescript = getattr(con, "executescript", None)
    if callable(executescript):
        executescript(script)
        return
    for statement in str(script or "").split(";"):
        sql = statement.strip()
        if sql:
            con.execute(sql)


def ensure_prices_schema(con, *, warn_nonfatal: WarnNonfatal) -> None:
    needs_rebuild = False
    legacy_table = ""
    if _table_exists(con, "prices"):
        needs_rebuild = not _table_matches_owned_contract(con, "prices")
    if needs_rebuild and _table_exists(con, "prices"):
        legacy_table = _next_legacy_table_name(con, "prices_legacy_exact_once")
        con.execute(f"ALTER TABLE prices RENAME TO {legacy_table}")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS prices (
          ts_ms INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          price REAL,
          px REAL,
          source TEXT,
          PRIMARY KEY(symbol, ts_ms)
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_prices_symbol_ts
          ON prices(symbol, ts_ms)
        """
    )
    if not legacy_table and _table_exists(con, "prices_legacy_exact_once"):
        legacy_table = "prices_legacy_exact_once"
    if legacy_table and _table_exists(con, legacy_table):
        try:
            legacy_specs = _table_column_specs(con, legacy_table)
            legacy_columns = set(legacy_specs)
            price_expr = "price" if "price" in legacy_columns else ("px" if "px" in legacy_columns else "NULL")
            if "px" in legacy_columns and "price" in legacy_columns:
                px_expr = "COALESCE(px, price)"
            elif "px" in legacy_columns:
                px_expr = "px"
            elif "price" in legacy_columns:
                px_expr = "price"
            else:
                px_expr = "NULL"
            source_expr = "source" if "source" in legacy_columns else ("provider" if "provider" in legacy_columns else "NULL")
            con.execute(
                f"""
                INSERT OR REPLACE INTO prices(ts_ms, symbol, price, px, source)
                SELECT ts_ms, symbol, {price_expr}, {px_expr}, {source_expr}
                FROM {legacy_table}
                """
            )
        except Exception as e:
            warn_nonfatal("STORAGE_PRICES_REBUILD_FAILED", e, table="prices")
    try:
        if not _has_column(con, "prices", "px"):
            con.execute("ALTER TABLE prices ADD COLUMN px REAL;")
    except Exception as e:
        warn_nonfatal("STORAGE_PRICES_MIGRATION_FAILED", e, table="prices", column="px")
    try:
        if not _has_column(con, "prices", "source"):
            con.execute("ALTER TABLE prices ADD COLUMN source TEXT;")
    except Exception as e:
        warn_nonfatal("STORAGE_PRICES_MIGRATION_FAILED", e, table="prices", column="source")


def ensure_price_quotes_schema(con, *, warn_nonfatal: WarnNonfatal) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS price_quotes (
          ts_ms INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          last REAL,
          bid REAL,
          ask REAL,
          spread REAL,
          volume REAL,
          source TEXT,
          PRIMARY KEY(symbol, ts_ms)
        );

        CREATE INDEX IF NOT EXISTS idx_price_quotes_symbol_ts
          ON price_quotes(symbol, ts_ms);

        CREATE INDEX IF NOT EXISTS idx_price_quotes_ts
          ON price_quotes(ts_ms);
        """
    )
    for column_name, ddl in (
        ("source", "TEXT"),
        ("last_trade_ts_ms", "INTEGER"),
        ("last_quote_ts_ms", "INTEGER"),
        ("last_update_ts_ms", "INTEGER"),
    ):
        try:
            if _table_exists(con, "price_quotes") and not _has_column(con, "price_quotes", column_name):
                con.execute(f"ALTER TABLE price_quotes ADD COLUMN {column_name} {ddl};")
        except Exception as e:
            warn_nonfatal(
                "STORAGE_PRICE_QUOTES_MIGRATION_FAILED",
                e,
                table="price_quotes",
                column=str(column_name),
            )


def ensure_price_quotes_raw_schema(con, *, warn_nonfatal: WarnNonfatal) -> None:
    needs_rebuild = False
    legacy_table = ""
    if _table_exists(con, "price_quotes_raw"):
        needs_rebuild = not _table_matches_owned_contract(con, "price_quotes_raw")

    if needs_rebuild and _table_exists(con, "price_quotes_raw"):
        legacy_table = _next_legacy_table_name(con, "price_quotes_raw_legacy_exact_once")
        con.execute(f"ALTER TABLE price_quotes_raw RENAME TO {legacy_table}")

    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS price_quotes_raw (
          ts_ms INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          provider TEXT NOT NULL,
          event_key TEXT NOT NULL,
          event_type TEXT,
          event_ts_ms INTEGER,
          last REAL,
          bid REAL,
          ask REAL,
          spread REAL,
          volume REAL,
          trade_ts_ms INTEGER,
          quote_ts_ms INTEGER,
          ingest_ts_ms INTEGER,
          source TEXT,
          PRIMARY KEY(symbol, provider, event_key)
        );

        CREATE INDEX IF NOT EXISTS idx_price_quotes_raw_symbol_ts
          ON price_quotes_raw(symbol, ts_ms);

        CREATE INDEX IF NOT EXISTS idx_price_quotes_raw_provider_ts
          ON price_quotes_raw(provider, ts_ms);

        CREATE INDEX IF NOT EXISTS idx_price_quotes_raw_ts
          ON price_quotes_raw(ts_ms);

        CREATE INDEX IF NOT EXISTS idx_price_quotes_raw_provider_event_ts
          ON price_quotes_raw(provider, event_ts_ms);
        """
    )
    if not legacy_table and _table_exists(con, "price_quotes_raw_legacy_exact_once"):
        legacy_table = "price_quotes_raw_legacy_exact_once"
    if legacy_table and _table_exists(con, legacy_table):
        legacy_specs = _table_column_specs(con, legacy_table)
        legacy_columns = set(legacy_specs)
        symbol_expr = "symbol" if "symbol" in legacy_columns else "''"
        provider_expr = "provider" if "provider" in legacy_columns else "''"
        ts_expr = "ts_ms" if "ts_ms" in legacy_columns else "0"
        event_key_expr = (
            "COALESCE(CAST(event_key AS TEXT), "
            f"'legacy:' || {symbol_expr} || ':' || {provider_expr} || ':' || {ts_expr} || ':' || "
            f"COALESCE(CAST({'last' if 'last' in legacy_columns else 'NULL'} AS TEXT), '') || ':' || "
            f"COALESCE(CAST({'bid' if 'bid' in legacy_columns else 'NULL'} AS TEXT), '') || ':' || "
            f"COALESCE(CAST({'ask' if 'ask' in legacy_columns else 'NULL'} AS TEXT), '') || ':' || "
            f"COALESCE(CAST({'volume' if 'volume' in legacy_columns else 'NULL'} AS TEXT), ''))"
            if "event_key" in legacy_columns
            else (
                f"'legacy:' || {symbol_expr} || ':' || {provider_expr} || ':' || {ts_expr} || ':' || "
                f"COALESCE(CAST({'last' if 'last' in legacy_columns else 'NULL'} AS TEXT), '') || ':' || "
                f"COALESCE(CAST({'bid' if 'bid' in legacy_columns else 'NULL'} AS TEXT), '') || ':' || "
                f"COALESCE(CAST({'ask' if 'ask' in legacy_columns else 'NULL'} AS TEXT), '') || ':' || "
                f"COALESCE(CAST({'volume' if 'volume' in legacy_columns else 'NULL'} AS TEXT), '')"
            )
        )

        def col(name: str, default: str) -> str:
            return name if name in legacy_columns else default

        con.execute(
            f"""
            INSERT OR IGNORE INTO price_quotes_raw(
              ts_ms, symbol, provider, event_key, event_type, event_ts_ms,
              last, bid, ask, spread, volume,
              trade_ts_ms, quote_ts_ms, ingest_ts_ms, source
            )
            SELECT
              {col('ts_ms', '0')},
              {col('symbol', "''")},
              {col('provider', "''")},
              {event_key_expr},
              {col('event_type', "'legacy'")},
              {col('event_ts_ms', col('ts_ms', '0'))},
              {col('last', 'NULL')},
              {col('bid', 'NULL')},
              {col('ask', 'NULL')},
              {col('spread', 'NULL')},
              {col('volume', 'NULL')},
              {col('trade_ts_ms', col('ts_ms', '0'))},
              {col('quote_ts_ms', col('ts_ms', '0'))},
              {col('ingest_ts_ms', col('ts_ms', '0'))},
              {col('source', col('provider', "''"))}
            FROM {legacy_table}
            """
        )


def ensure_price_provider_health_schema(con, *, warn_nonfatal: WarnNonfatal) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS price_provider_health (
          ts_ms INTEGER NOT NULL,
          provider TEXT NOT NULL,
          ok INTEGER NOT NULL,
          latency_ms INTEGER,
          n_symbols INTEGER,
          error TEXT,
          last_success_ts_ms INTEGER,
          error_count INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY(provider, ts_ms)
        );

        CREATE INDEX IF NOT EXISTS idx_price_provider_health_ts
          ON price_provider_health(ts_ms);

        CREATE INDEX IF NOT EXISTS idx_price_provider_health_provider
          ON price_provider_health(provider);
        """
    )
    try:
        if _table_exists(con, "price_provider_health") and not _has_column(con, "price_provider_health", "last_success_ts_ms"):
            con.execute("ALTER TABLE price_provider_health ADD COLUMN last_success_ts_ms INTEGER;")
    except Exception as e:
        warn_nonfatal(
            "STORAGE_PRICE_PROVIDER_HEALTH_MIGRATION_FAILED",
            e,
            table="price_provider_health",
            column="last_success_ts_ms",
        )
    try:
        if _table_exists(con, "price_provider_health") and not _has_column(con, "price_provider_health", "error_count"):
            con.execute("ALTER TABLE price_provider_health ADD COLUMN error_count INTEGER NOT NULL DEFAULT 0;")
    except Exception as e:
        warn_nonfatal(
            "STORAGE_PRICE_PROVIDER_HEALTH_MIGRATION_FAILED",
            e,
            table="price_provider_health",
            column="error_count",
        )


def ensure_ingestion_pipeline_health_schema(con, *, warn_nonfatal: WarnNonfatal) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS ingestion_pipeline_health (
          ts_ms INTEGER NOT NULL,
          pipeline TEXT NOT NULL,
          ok INTEGER NOT NULL,
          latency_ms INTEGER,
          raw_rows INTEGER NOT NULL DEFAULT 0,
          event_rows INTEGER NOT NULL DEFAULT 0,
          last_ingested_ts_ms INTEGER,
          error TEXT,
          meta_json TEXT,
          PRIMARY KEY (pipeline, ts_ms)
        );

        CREATE INDEX IF NOT EXISTS idx_ingestion_pipeline_health_ts
          ON ingestion_pipeline_health(ts_ms);

        CREATE INDEX IF NOT EXISTS idx_ingestion_pipeline_health_pipeline
          ON ingestion_pipeline_health(pipeline);
        """
    )
    for column_name, ddl in (
        ("latency_ms", "INTEGER"),
        ("raw_rows", "INTEGER NOT NULL DEFAULT 0"),
        ("event_rows", "INTEGER NOT NULL DEFAULT 0"),
        ("last_ingested_ts_ms", "INTEGER"),
        ("error", "TEXT"),
        ("meta_json", "TEXT"),
    ):
        try:
            if _table_exists(con, "ingestion_pipeline_health") and not _has_column(con, "ingestion_pipeline_health", column_name):
                con.execute(f"ALTER TABLE ingestion_pipeline_health ADD COLUMN {column_name} {ddl};")
        except Exception as e:
            warn_nonfatal(
                "STORAGE_INGESTION_PIPELINE_HEALTH_MIGRATION_FAILED",
                e,
                table="ingestion_pipeline_health",
                column=str(column_name),
            )


def ensure_price_feed_lock_schema(con, *, warn_nonfatal: WarnNonfatal) -> None:
    _execute_script(
        con,
        """
        CREATE TABLE IF NOT EXISTS price_feed_lock(
          id INTEGER PRIMARY KEY,
          owner TEXT NOT NULL,
          pid INTEGER NOT NULL,
          ts_ms INTEGER NOT NULL
        );
        """
    )
    if not _is_sqlite_connection(con):
        for column_name, ddl in (
            ("owner", "TEXT NOT NULL DEFAULT ''"),
            ("pid", "BIGINT NOT NULL DEFAULT 0"),
            ("ts_ms", "BIGINT NOT NULL DEFAULT 0"),
        ):
            try:
                con.execute(f"ALTER TABLE price_feed_lock ADD COLUMN IF NOT EXISTS {column_name} {ddl};")
            except Exception as e:
                warn_nonfatal(
                    "STORAGE_PRICE_FEED_LOCK_MIGRATION_FAILED",
                    e,
                    table="price_feed_lock",
                    column=str(column_name),
                )
    for column_name, ddl in (
        ("owner", "TEXT NOT NULL DEFAULT ''"),
        ("pid", "INTEGER NOT NULL DEFAULT 0"),
        ("ts_ms", "INTEGER NOT NULL DEFAULT 0"),
    ):
        try:
            if _table_exists(con, "price_feed_lock") and not _has_column(con, "price_feed_lock", column_name):
                con.execute(f"ALTER TABLE price_feed_lock ADD COLUMN {column_name} {ddl};")
        except Exception as e:
            warn_nonfatal(
                "STORAGE_PRICE_FEED_LOCK_MIGRATION_FAILED",
                e,
                table="price_feed_lock",
                column=str(column_name),
            )


def ensure_options_symbol_ingestion_state_schema(con, *, warn_nonfatal: WarnNonfatal) -> None:
    _execute_script(
        con,
        """
        CREATE TABLE IF NOT EXISTS options_symbol_ingestion_state (
          symbol TEXT NOT NULL PRIMARY KEY,
          provider TEXT NOT NULL DEFAULT '',
          consecutive_failures INTEGER NOT NULL DEFAULT 0,
          total_failures INTEGER NOT NULL DEFAULT 0,
          last_failure_ts_ms INTEGER,
          last_failure_error TEXT,
          last_success_ts_ms INTEGER,
          last_fresh_snapshot_ts_ms INTEGER,
          last_cached_snapshot_ts_ms INTEGER,
          last_fallback_ts_ms INTEGER,
          last_row_count INTEGER NOT NULL DEFAULT 0,
          disabled_until_ts_ms INTEGER NOT NULL DEFAULT 0,
          updated_ts_ms INTEGER NOT NULL
        );
        """
    )
    for column_name, ddl in (
        ("provider", "TEXT NOT NULL DEFAULT ''"),
        ("consecutive_failures", "INTEGER NOT NULL DEFAULT 0"),
        ("total_failures", "INTEGER NOT NULL DEFAULT 0"),
        ("last_failure_ts_ms", "INTEGER"),
        ("last_failure_error", "TEXT"),
        ("last_success_ts_ms", "INTEGER"),
        ("last_fresh_snapshot_ts_ms", "INTEGER"),
        ("last_cached_snapshot_ts_ms", "INTEGER"),
        ("last_fallback_ts_ms", "INTEGER"),
        ("last_row_count", "INTEGER NOT NULL DEFAULT 0"),
        ("disabled_until_ts_ms", "INTEGER NOT NULL DEFAULT 0"),
        ("updated_ts_ms", "INTEGER NOT NULL DEFAULT 0"),
    ):
        try:
            if _table_exists(con, "options_symbol_ingestion_state") and not _has_column(
                con,
                "options_symbol_ingestion_state",
                column_name,
            ):
                con.execute(
                    f"ALTER TABLE options_symbol_ingestion_state ADD COLUMN {column_name} {ddl};"
                )
        except Exception as e:
            warn_nonfatal(
                "STORAGE_OPTIONS_SYMBOL_INGESTION_STATE_MIGRATION_FAILED",
                e,
                table="options_symbol_ingestion_state",
                column=str(column_name),
            )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_options_symbol_ingestion_disabled
          ON options_symbol_ingestion_state(disabled_until_ts_ms);
        """
    )


def ensure_live_ingestion_schema(con, *, warn_nonfatal: WarnNonfatal) -> None:
    ensure_prices_schema(con, warn_nonfatal=warn_nonfatal)
    ensure_price_quotes_schema(con, warn_nonfatal=warn_nonfatal)
    ensure_price_quotes_raw_schema(con, warn_nonfatal=warn_nonfatal)
    ensure_price_provider_health_schema(con, warn_nonfatal=warn_nonfatal)
    ensure_ingestion_pipeline_health_schema(con, warn_nonfatal=warn_nonfatal)
    ensure_price_feed_lock_schema(con, warn_nonfatal=warn_nonfatal)
    ensure_options_symbol_ingestion_state_schema(con, warn_nonfatal=warn_nonfatal)


__all__ = [
    "OWNED_LIVE_TABLE_COLUMN_SPECS",
    "OWNED_LIVE_TABLE_REQUIRED_INDEXES",
    "SCHEMA",
    "ensure_prices_schema",
    "ensure_price_quotes_schema",
    "ensure_price_quotes_raw_schema",
    "ensure_price_provider_health_schema",
    "ensure_ingestion_pipeline_health_schema",
    "ensure_price_feed_lock_schema",
    "ensure_options_symbol_ingestion_state_schema",
    "ensure_live_ingestion_schema",
]
