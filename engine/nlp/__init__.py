"""Offline NLP encoders, caches, and aggregations for financial text."""

from __future__ import annotations

from engine.nlp.aggregators import aggregate_symbol_day_documents
from engine.nlp.cache import NlpCache, normalize_text, text_hash
from engine.nlp.encoder import (
    EncodedTextBatch,
    Encoder,
    HashingEmbeddingEncoder,
    OpenAIEmbeddingEncoder,
    FinBertSentimentEncoder,
    SentenceTransformerEncoder,
    TextEmbeddingConfig,
    build_text_embedding_encoder,
    current_sentiment_config,
    resolve_text_embedding_config,
)

__all__ = [
    "EncodedTextBatch",
    "Encoder",
    "FinBertSentimentEncoder",
    "HashingEmbeddingEncoder",
    "NlpCache",
    "OpenAIEmbeddingEncoder",
    "SentenceTransformerEncoder",
    "TextEmbeddingConfig",
    "aggregate_symbol_day_documents",
    "build_text_embedding_encoder",
    "current_sentiment_config",
    "normalize_text",
    "resolve_text_embedding_config",
    "text_hash",
]
