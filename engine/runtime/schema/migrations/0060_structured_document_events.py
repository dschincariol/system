"""Structured document/event extraction storage."""

from __future__ import annotations

id = 60
description = "structured document event extraction storage"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS structured_document_events (
            id BIGSERIAL PRIMARY KEY,
            source_document_id TEXT NOT NULL,
            source_event_id BIGINT,
            symbol TEXT NOT NULL DEFAULT '',
            document_type TEXT NOT NULL,
            source TEXT,
            event_type TEXT NOT NULL,
            event_ts_ms BIGINT NOT NULL,
            availability_ts_ms BIGINT NOT NULL,
            extraction_confidence DOUBLE PRECISION NOT NULL,
            polarity DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            feature_id TEXT NOT NULL,
            evidence TEXT,
            extractor_name TEXT NOT NULL,
            extractor_version TEXT NOT NULL,
            created_ts_ms BIGINT NOT NULL,
            payload_json JSONB,
            pit_metadata_json JSONB
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_structured_document_events_doc_type_ts
          ON structured_document_events(source_document_id, symbol, event_type, event_ts_ms, extractor_version)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_structured_document_events_symbol_avail
          ON structured_document_events(symbol, availability_ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_structured_document_events_source_event
          ON structured_document_events(source_event_id)
        """
    )
