"""Backend-aware news story embeddings and news-flow features."""

from __future__ import annotations

id = 36
description = "news flow embedding novelty storage"


NEWS_EVENT_FEATURE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("payload_json", "JSONB"),
    ("embedding_backend", "TEXT"),
    ("embedding_model_name", "TEXT"),
    ("embedding_novelty_score", "DOUBLE PRECISION"),
    ("embedding_max_similarity", "DOUBLE PRECISION"),
    ("stale_flag", "BIGINT"),
    ("novelty_computed_ts_ms", "BIGINT"),
)


def _add_columns(conn, table_name: str, columns: tuple[tuple[str, str], ...]) -> None:
    for column_name, column_type in columns:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} {column_type}")


def up(conn) -> None:
    _add_columns(conn, "news_event_features", NEWS_EVENT_FEATURE_COLUMNS)
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_news_event_features_event_id
          ON news_event_features(event_id)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS news_story_embeddings (
            id BIGSERIAL PRIMARY KEY,
            event_id BIGINT NOT NULL,
            symbol TEXT NOT NULL,
            publish_ts_ms BIGINT,
            availability_ts_ms BIGINT NOT NULL,
            source TEXT,
            embedding_backend TEXT NOT NULL,
            model_name TEXT NOT NULL,
            dim BIGINT NOT NULL,
            vector BYTEA NOT NULL,
            text_hash TEXT,
            novelty_score DOUBLE PRECISION NOT NULL DEFAULT 1.0,
            max_similarity DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            stale_flag BIGINT NOT NULL DEFAULT 0,
            matched_event_id BIGINT,
            ingested_ts_ms BIGINT,
            payload_json JSONB,
            diagnostics_json JSONB
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_news_story_embeddings_event_space
          ON news_story_embeddings(event_id, symbol, embedding_backend, model_name)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_news_story_embeddings_symbol_space_avail
          ON news_story_embeddings(symbol, embedding_backend, model_name, availability_ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS news_flow_features (
            symbol TEXT NOT NULL,
            asof_ts_ms BIGINT NOT NULL,
            bucket_ts_ms BIGINT NOT NULL,
            embedding_backend TEXT NOT NULL,
            model_name TEXT NOT NULL,
            news_novelty_max_24h DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            news_stale_share_24h DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            news_velocity_z DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            fresh_neg_news_flag DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            event_count_24h BIGINT NOT NULL DEFAULT 0,
            source_max_availability_ts_ms BIGINT,
            created_ts_ms BIGINT,
            meta_json JSONB,
            PRIMARY KEY(symbol, asof_ts_ms, embedding_backend, model_name)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_news_flow_features_symbol_asof
          ON news_flow_features(symbol, asof_ts_ms DESC)
        """
    )
