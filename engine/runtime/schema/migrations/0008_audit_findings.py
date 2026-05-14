"""Verifier findings for audit hash-chain divergence."""

from __future__ import annotations

id = 8
description = "audit chain verifier findings"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS audit_chain_findings (
            id BIGSERIAL PRIMARY KEY,
            ts TIMESTAMPTZ NOT NULL DEFAULT now(),
            table_name TEXT NOT NULL,
            row_id BIGINT,
            finding TEXT NOT NULL,
            expected_hash BYTEA,
            actual_hash BYTEA,
            payload_excerpt JSONB
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_audit_chain_findings_table_row
          ON audit_chain_findings(table_name, row_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_audit_chain_findings_ts
          ON audit_chain_findings(ts DESC)
        """
    )
