"""Triple-barrier labels for meta-label classifier training."""

from __future__ import annotations

id = 44
description = "triple barrier meta-label training rows"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS triple_barrier_labels (
            id BIGSERIAL PRIMARY KEY,
            source_table TEXT NOT NULL,
            source_id BIGINT NOT NULL,
            event_id BIGINT,
            symbol TEXT NOT NULL,
            horizon_s BIGINT NOT NULL,
            ts_ms BIGINT NOT NULL,
            entry_ts_ms BIGINT NOT NULL,
            vertical_ts_ms BIGINT NOT NULL,
            exit_ts_ms BIGINT NOT NULL,
            side TEXT NOT NULL,
            side_sign BIGINT NOT NULL,
            model_name TEXT,
            model_id TEXT,
            model_family TEXT,
            primary_predicted_z DOUBLE PRECISION NOT NULL,
            primary_confidence DOUBLE PRECISION NOT NULL,
            sigma DOUBLE PRECISION NOT NULL,
            sigma_source TEXT NOT NULL,
            barrier_k DOUBLE PRECISION NOT NULL,
            profit_take_ret DOUBLE PRECISION NOT NULL,
            stop_loss_ret DOUBLE PRECISION NOT NULL,
            realized_ret DOUBLE PRECISION NOT NULL,
            outcome TEXT NOT NULL,
            label BIGINT NOT NULL,
            timeout_sign BIGINT NOT NULL DEFAULT 0,
            feature_ids_json JSONB,
            feature_schema_json JSONB,
            feature_values_json JSONB,
            meta_json JSONB,
            created_ts_ms BIGINT NOT NULL,
            UNIQUE(source_table, source_id)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_triple_barrier_labels_symbol_ts
          ON triple_barrier_labels(symbol, ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_triple_barrier_labels_family_ts
          ON triple_barrier_labels(model_family, ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_triple_barrier_labels_outcome
          ON triple_barrier_labels(outcome, label)
        """
    )
