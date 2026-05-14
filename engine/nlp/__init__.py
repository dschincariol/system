"""Offline NLP encoders, caches, and aggregations for financial text."""

from __future__ import annotations

from engine.nlp.aggregators import aggregate_symbol_day_documents
from engine.nlp.cache import NlpCache, normalize_text, text_hash
from engine.nlp.encoder import (
    Encoder,
    FinBertSentimentEncoder,
    SentenceTransformerEncoder,
)

__all__ = [
    "Encoder",
    "FinBertSentimentEncoder",
    "NlpCache",
    "SentenceTransformerEncoder",
    "aggregate_symbol_day_documents",
    "normalize_text",
    "text_hash",
]
