"""Add data-source Populate Now evidence storage."""

from __future__ import annotations

id = 70
description = "data-source populate evidence"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS data_source_populate_evidence (
          source_key TEXT PRIMARY KEY,
          ts_ms BIGINT NOT NULL,
          status TEXT NOT NULL,
          contract_status TEXT NOT NULL,
          row_count BIGINT NOT NULL DEFAULT 0,
          storage_table TEXT NOT NULL,
          latest_ts_ms BIGINT,
          latency_ms BIGINT,
          missing_null_counts_json JSONB,
          duplicate_drops BIGINT NOT NULL DEFAULT 0,
          stale_gap_status TEXT,
          provider_evidence_json JSONB,
          contract_json JSONB,
          error TEXT,
          actor TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_data_source_populate_evidence_status
          ON data_source_populate_evidence(status, contract_status)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_data_source_populate_evidence_ts
          ON data_source_populate_evidence(ts_ms DESC)
        """
    )
