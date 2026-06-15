"""Options dealer GEX and snapshot flow-imbalance feature columns."""

from __future__ import annotations

id = 37
description = "options dealer gex and flow imbalance features"


OPTIONS_GEX_FLOW_COLUMNS: tuple[tuple[str, str], ...] = (
    ("gex_raw", "DOUBLE PRECISION"),
    ("gex_norm", "DOUBLE PRECISION NOT NULL DEFAULT 0.0"),
    ("gex_norm_z", "DOUBLE PRECISION NOT NULL DEFAULT 0.0"),
    ("gex_sign", "DOUBLE PRECISION NOT NULL DEFAULT 0.0"),
    ("opt_flow_imbalance", "DOUBLE PRECISION NOT NULL DEFAULT 0.0"),
    ("opt_flow_imbalance_z", "DOUBLE PRECISION NOT NULL DEFAULT 0.0"),
    ("gex_zero_gamma_flip", "DOUBLE PRECISION"),
)


def _add_columns(conn, table_name: str) -> None:
    for column_name, column_type in OPTIONS_GEX_FLOW_COLUMNS:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} {column_type}")


def up(conn) -> None:
    _add_columns(conn, "options_symbol_features")
    _add_columns(conn, "options_event_features")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_options_symbol_features_snapshot_available
          ON options_symbol_features(symbol, bucket_sec, bucket_ts_ms DESC, snapshot_ts_ms DESC)
        """
    )
