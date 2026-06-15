"""Inference runtime persistence tables and regime metadata columns."""

from __future__ import annotations

id = 15
description = "inference runtime persistence tables"


def _table_exists(conn, table: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = current_schema()
          AND table_name = ?
        """,
        (str(table),),
    ).fetchone()
    return bool(row)


def _column_exists(conn, table: str, column: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = ?
          AND column_name = ?
        """,
        (str(table), str(column)),
    ).fetchone()
    return bool(row)


def _add_column(conn, table: str, column: str, definition: str) -> None:
    if not _table_exists(conn, table):
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {definition}")


def _ensure_regime_state_schema(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS regime_state (
          time BIGINT NOT NULL,
          symbol TEXT NOT NULL,
          volatility_regime TEXT NOT NULL,
          trend_regime TEXT NOT NULL,
          liquidity_regime TEXT NOT NULL,
          created_ts_ms BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT,
          PRIMARY KEY(symbol, time)
        )
        """
    )
    for column, definition in (
        ("time", "BIGINT"),
        ("symbol", "TEXT"),
        ("volatility_regime", "TEXT"),
        ("trend_regime", "TEXT"),
        ("liquidity_regime", "TEXT"),
        ("created_ts_ms", "BIGINT"),
    ):
        conn.execute(f"ALTER TABLE regime_state ADD COLUMN IF NOT EXISTS {column} {definition}")
    has_ts_ms = _column_exists(conn, "regime_state", "ts_ms")
    time_expr = "COALESCE(time, ts_ms, created_ts_ms, 0)" if has_ts_ms else "COALESCE(time, created_ts_ms, 0)"
    created_expr = "COALESCE(created_ts_ms, time, ts_ms, 0)" if has_ts_ms else "COALESCE(created_ts_ms, time, 0)"
    conn.execute(
        f"""
        UPDATE regime_state
        SET
          time = {time_expr},
          symbol = COALESCE(symbol, ''),
          volatility_regime = COALESCE(volatility_regime, 'unknown'),
          trend_regime = COALESCE(trend_regime, 'unknown'),
          liquidity_regime = COALESCE(liquidity_regime, 'unknown'),
          created_ts_ms = {created_expr}
        WHERE
          time IS NULL OR symbol IS NULL OR volatility_regime IS NULL
          OR trend_regime IS NULL OR liquidity_regime IS NULL
          OR created_ts_ms IS NULL
        """
    )
    conn.execute(
        """
        DELETE FROM regime_state a
        USING regime_state b
        WHERE a.ctid < b.ctid
          AND a.symbol = b.symbol
          AND a.time = b.time
        """
    )
    conn.execute("ALTER TABLE regime_state ALTER COLUMN time SET NOT NULL")
    conn.execute("ALTER TABLE regime_state ALTER COLUMN symbol SET NOT NULL")
    conn.execute("ALTER TABLE regime_state ALTER COLUMN volatility_regime SET NOT NULL")
    conn.execute("ALTER TABLE regime_state ALTER COLUMN trend_regime SET NOT NULL")
    conn.execute("ALTER TABLE regime_state ALTER COLUMN liquidity_regime SET NOT NULL")
    conn.execute("ALTER TABLE regime_state ALTER COLUMN created_ts_ms SET NOT NULL")
    conn.execute(
        """
        ALTER TABLE regime_state
          ALTER COLUMN created_ts_ms
          SET DEFAULT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
        """
    )
    conn.execute(
        """
        DO $$
        DECLARE
          pk_name TEXT;
          pk_cols TEXT[];
        BEGIN
          SELECT c.conname, array_agg(a.attname ORDER BY keys.ordinality)
            INTO pk_name, pk_cols
          FROM pg_constraint c
          JOIN unnest(c.conkey) WITH ORDINALITY AS keys(attnum, ordinality)
            ON TRUE
          JOIN pg_attribute a
            ON a.attrelid = c.conrelid
           AND a.attnum = keys.attnum
          WHERE c.conrelid = 'regime_state'::regclass
            AND c.contype = 'p'
          GROUP BY c.conname
          LIMIT 1;

          IF pk_cols = ARRAY['symbol', 'time']::TEXT[] THEN
            RETURN;
          END IF;

          IF pk_name IS NOT NULL THEN
            EXECUTE format('ALTER TABLE regime_state DROP CONSTRAINT %I', pk_name);
          END IF;

          ALTER TABLE regime_state
            ADD CONSTRAINT regime_state_pkey PRIMARY KEY(symbol, time);
        END $$;
        """
    )


def up(conn) -> None:
    for table in ("predictions", "prediction_history"):
        _add_column(conn, table, "regime_time_ms", "BIGINT")
        _add_column(conn, table, "volatility_regime", "TEXT")
        _add_column(conn, table, "trend_regime", "TEXT")
        _add_column(conn, table, "liquidity_regime", "TEXT")

    _ensure_regime_state_schema(conn)
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_regime_state_symbol_time_desc
          ON regime_state(symbol, time DESC)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tracked_model_registry (
          model_name TEXT NOT NULL,
          version TEXT NOT NULL,
          created_ts_ms BIGINT NOT NULL,
          updated_ts_ms BIGINT NOT NULL,
          metadata_json TEXT NOT NULL DEFAULT '{}',
          PRIMARY KEY(model_name, version)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_tracked_model_registry_updated
          ON tracked_model_registry(updated_ts_ms DESC)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tracked_predictions (
          id BIGSERIAL PRIMARY KEY,
          ts_ms BIGINT NOT NULL,
          symbol TEXT NOT NULL,
          model_name TEXT NOT NULL,
          model_version TEXT NOT NULL,
          prediction DOUBLE PRECISION NOT NULL,
          confidence DOUBLE PRECISION NOT NULL,
          features_version TEXT NOT NULL,
          event_id BIGINT,
          horizon_s BIGINT,
          prediction_id BIGINT,
          source_alert_id BIGINT,
          model_id TEXT,
          tracking_source TEXT,
          metadata_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_tracked_predictions_ts
          ON tracked_predictions(ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_tracked_predictions_symbol_ts
          ON tracked_predictions(symbol, ts_ms DESC)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS prediction_explanations (
          id BIGSERIAL PRIMARY KEY,
          symbol TEXT NOT NULL,
          ts BIGINT NOT NULL,
          model_family TEXT NOT NULL,
          model_name TEXT,
          version TEXT,
          explanation_type TEXT NOT NULL,
          top_features JSONB,
          base_value DOUBLE PRECISION,
          diagnostics JSONB,
          created_ts BIGINT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_prediction_explanations_symbol_ts
          ON prediction_explanations(symbol, ts DESC)
        """
    )
