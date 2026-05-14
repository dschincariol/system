"""FinBERT sentiment enrichment persistence tables."""

from __future__ import annotations

id = 19
description = "finbert sentiment enrichment persistence"


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = current_schema()
          AND table_name = ?
        """,
        (str(table_name),),
    ).fetchone()
    return bool(row)


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS finbert_sentiment_enrichments (
            id BIGSERIAL PRIMARY KEY,
            ts_ms BIGINT NOT NULL,
            symbol TEXT,
            event_id BIGINT,
            source_identifier TEXT,
            model_name TEXT,
            payload_json JSONB NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_finbert_sentiment_event_model_ts
          ON finbert_sentiment_enrichments(event_id, model_name, ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_finbert_sentiment_symbol_model_ts
          ON finbert_sentiment_enrichments(symbol, model_name, ts_ms DESC)
        """
    )
    if not _table_exists(conn, "news_event_features"):
        return
    conn.execute("ALTER TABLE news_event_features ADD COLUMN IF NOT EXISTS finbert_label TEXT")
    conn.execute("ALTER TABLE news_event_features ADD COLUMN IF NOT EXISTS finbert_score DOUBLE PRECISION")
    conn.execute("ALTER TABLE news_event_features ADD COLUMN IF NOT EXISTS finbert_confidence DOUBLE PRECISION")
    conn.execute("ALTER TABLE news_event_features ADD COLUMN IF NOT EXISTS finbert_pos DOUBLE PRECISION")
    conn.execute("ALTER TABLE news_event_features ADD COLUMN IF NOT EXISTS finbert_neg DOUBLE PRECISION")
    conn.execute("ALTER TABLE news_event_features ADD COLUMN IF NOT EXISTS finbert_neu DOUBLE PRECISION")
