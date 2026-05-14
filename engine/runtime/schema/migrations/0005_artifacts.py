"""Artifact metadata and alias tables."""

from __future__ import annotations

id = 5
description = "content-addressed artifact metadata"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS artifacts (
            sha256 TEXT PRIMARY KEY,
            size_bytes BIGINT NOT NULL,
            content_type TEXT NOT NULL,
            kind TEXT NOT NULL,
            created_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
            metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
            ref_count INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS artifacts_kind_created ON artifacts (kind, created_ts DESC)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS artifacts_metadata_gin ON artifacts USING GIN (metadata jsonb_path_ops)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS artifact_aliases (
            alias TEXT NOT NULL,
            sha256 TEXT NOT NULL REFERENCES artifacts(sha256),
            set_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (alias, set_at)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS artifact_aliases_current
            ON artifact_aliases (alias, set_at DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS artifact_fsck_findings (
            id BIGSERIAL PRIMARY KEY,
            checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            severity TEXT NOT NULL,
            finding_type TEXT NOT NULL,
            sha256 TEXT,
            path TEXT,
            detail_json JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS artifact_fsck_findings_checked
            ON artifact_fsck_findings (checked_at DESC, finding_type)
        """
    )
    for table_name, columns in {
        "embed_models2": ("artifact_sha256 TEXT", "artifact_alias TEXT"),
        "temporal_models": ("artifact_sha256 TEXT", "artifact_alias TEXT"),
        "gbm_models": ("artifact_sha256 TEXT", "artifact_alias TEXT"),
        "hmm_regime_models": ("artifact_sha256 TEXT", "artifact_alias TEXT"),
        "rl_strategy_policy_models": ("artifact_sha256 TEXT", "artifact_alias TEXT"),
        "ensemble_blend_weights": ("meta_artifact_sha256 TEXT", "meta_artifact_alias TEXT"),
    }.items():
        for column in columns:
            conn.execute(f"ALTER TABLE IF EXISTS {table_name} ADD COLUMN IF NOT EXISTS {column}")
