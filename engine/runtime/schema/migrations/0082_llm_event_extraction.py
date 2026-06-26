"""Structured LLM event extraction audit and lineage tables."""

from __future__ import annotations


id = 82
description = "structured LLM event extraction audit storage"


def up(conn) -> None:
    from engine.data.llm_event_extraction import ensure_llm_event_extraction_schema

    ensure_llm_event_extraction_schema(conn)
