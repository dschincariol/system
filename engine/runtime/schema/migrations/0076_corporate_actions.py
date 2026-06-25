"""Point-in-time split and cash-dividend corporate-action calendar."""

from __future__ import annotations

id = 76
description = "point-in-time split and cash-dividend corporate-action calendar"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS corporate_actions (
            id BIGSERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            action_type TEXT NOT NULL,
            ex_date TEXT,
            ex_ts_ms BIGINT,
            pay_date TEXT,
            pay_ts_ms BIGINT,
            record_date TEXT,
            cash_amount DOUBLE PRECISION,
            split_from DOUBLE PRECISION,
            split_to DOUBLE PRECISION,
            currency TEXT,
            availability_ts_ms BIGINT NOT NULL,
            source TEXT NOT NULL,
            source_record_id TEXT NOT NULL,
            ingested_ts_ms BIGINT,
            payload_json JSONB,
            diagnostics_json JSONB
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_corporate_actions_source_record_id
          ON corporate_actions(source_record_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_corporate_actions_symbol_type_ex
          ON corporate_actions(symbol, action_type, ex_ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_corporate_actions_symbol_availability
          ON corporate_actions(symbol, availability_ts_ms)
        """
    )
