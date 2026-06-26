"""Backend-aware NLP cache metadata columns."""

from __future__ import annotations


id = 83
description = "nlp backend/model namespace metadata"


def _add_columns(conn, table_name: str, columns: tuple[tuple[str, str], ...]) -> None:
    for column_name, column_type in columns:
        try:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} {column_type}")
            continue
        except Exception:
            pass  # no-op-guard: allow - fallback for engines without ADD COLUMN IF NOT EXISTS.
        try:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
        except Exception:
            continue


def up(conn) -> None:
    _add_columns(
        conn,
        "nlp_embeddings",
        (
            ("backend", "TEXT DEFAULT 'legacy'"),
            ("model_namespace", "TEXT"),
            ("model_metadata_json", "JSONB"),
        ),
    )
    _add_columns(
        conn,
        "nlp_sentiments",
        (
            ("backend", "TEXT DEFAULT 'legacy'"),
            ("model_namespace", "TEXT"),
            ("model_metadata_json", "JSONB"),
        ),
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
