"""NLP text, embedding, and sentiment caches."""

from __future__ import annotations

id = 12
description = "nlp content-hash caches"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS nlp_text_blobs (
            hash TEXT PRIMARY KEY,
            source TEXT,
            ts BIGINT,
            symbol TEXT NULL,
            text TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_nlp_text_blobs_symbol_ts
          ON nlp_text_blobs(symbol, ts)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_nlp_text_blobs_source_ts
          ON nlp_text_blobs(source, ts)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS nlp_embeddings (
            hash TEXT NOT NULL,
            model_name TEXT NOT NULL,
            backend TEXT DEFAULT 'legacy',
            model_namespace TEXT,
            dim BIGINT NOT NULL,
            vector BYTEA NOT NULL,
            model_metadata_json JSONB,
            PRIMARY KEY(hash, model_name)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_nlp_embeddings_model_name
          ON nlp_embeddings(model_name)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS nlp_sentiments (
            hash TEXT NOT NULL,
            model_name TEXT NOT NULL,
            backend TEXT DEFAULT 'legacy',
            model_namespace TEXT,
            score DOUBLE PRECISION,
            label TEXT,
            model_metadata_json JSONB,
            PRIMARY KEY(hash, model_name)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_nlp_sentiments_model_name
          ON nlp_sentiments(model_name)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_nlp_embeddings_backend_namespace
          ON nlp_embeddings(backend, model_namespace)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_nlp_sentiments_backend_namespace
          ON nlp_sentiments(backend, model_namespace)
        """
    )
