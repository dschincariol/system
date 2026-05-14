"""Strategy promotion audit hardening."""

from __future__ import annotations

id = 28
description = "strategy promotion audit hardening"


def up(conn) -> None:
    conn.execute(
        "ALTER TABLE IF EXISTS decision_log ADD COLUMN IF NOT EXISTS feature_set_tag TEXT NULL"
    )
    conn.execute(
        "ALTER TABLE IF EXISTS promotion_statistical_evidence ADD COLUMN IF NOT EXISTS evidence_kind TEXT NULL"
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_promotion_statistical_evidence_unique_kind_ts
          ON promotion_statistical_evidence(model_id, evidence_kind, ts)
          WHERE evidence_kind IS NOT NULL
        """
    )
