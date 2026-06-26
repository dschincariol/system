from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class CountingEncoder:
    backend = "unit_backend"
    model_name = "unit-encoder"

    def __init__(self) -> None:
        self.calls = 0

    def encode(self, texts: list[str]) -> np.ndarray:
        self.calls += 1
        rows = []
        for idx, text in enumerate(texts):
            rows.append([float(len(text)), float(idx + 1)])
        return np.asarray(rows, dtype=np.float32)


def test_cache_hit_returns_identical_bytes_without_encoder_call() -> None:
    from engine.nlp.cache import NlpCache, text_hash
    from engine.nlp.encoder import encoder_namespace

    con = sqlite3.connect(":memory:")
    cache = NlpCache(con)
    cache.ensure_schema()
    encoder = CountingEncoder()
    namespace = encoder_namespace(encoder)
    texts = ["Apple revenue beat expectations."]

    first = cache.get_or_encode_embeddings(texts, encoder, source="news", ts=1, symbol="AAPL")
    expected_hash = text_hash(texts[0])
    first_bytes = con.execute(
        "SELECT vector FROM nlp_embeddings WHERE hash=? AND model_name=?",
        (expected_hash, namespace),
    ).fetchone()[0]
    second = cache.get_or_encode_embeddings(texts, encoder, source="news", ts=1, symbol="AAPL")
    second_bytes = con.execute(
        "SELECT vector FROM nlp_embeddings WHERE hash=? AND model_name=?",
        (expected_hash, namespace),
    ).fetchone()[0]

    assert encoder.calls == 1
    assert first.misses == 1
    assert second.misses == 0
    assert first_bytes == second_bytes
    np.testing.assert_allclose(first.values, second.values)
    meta = con.execute(
        "SELECT backend, model_namespace, model_metadata_json FROM nlp_embeddings WHERE hash=? AND model_name=?",
        (expected_hash, namespace),
    ).fetchone()
    assert meta[0] == "unit_backend"
    assert meta[1] == namespace
    assert "unit-encoder" in str(meta[2])


def test_embedding_cache_isolates_same_model_name_by_backend() -> None:
    from engine.nlp.cache import NlpCache
    from engine.nlp.encoder import embedding_namespace

    con = sqlite3.connect(":memory:")
    cache = NlpCache(con)
    cache.ensure_schema()
    text = ["same cached text"]

    first = CountingEncoder()
    second = CountingEncoder()
    second.backend = "other_backend"

    cache.get_or_encode_embeddings(text, first, source="news", ts=1, symbol="AAPL")
    cache.get_or_encode_embeddings(text, second, source="news", ts=1, symbol="AAPL")

    rows = con.execute("SELECT model_name, backend FROM nlp_embeddings ORDER BY model_name").fetchall()
    assert rows == [
        (embedding_namespace("other_backend", "unit-encoder"), "other_backend"),
        (embedding_namespace("unit_backend", "unit-encoder"), "unit_backend"),
    ]
    assert first.calls == 1
    assert second.calls == 1


def test_legacy_raw_model_name_cache_row_is_still_readable() -> None:
    from engine.nlp.cache import NlpCache, text_hash, vector_to_bytes
    from engine.nlp.encoder import embedding_namespace

    con = sqlite3.connect(":memory:")
    cache = NlpCache(con)
    cache.ensure_schema()
    vector = np.asarray([1.0, 2.0], dtype=np.float32)
    hash_value = text_hash("legacy text")
    con.execute(
        "INSERT INTO nlp_embeddings(hash, model_name, dim, vector) VALUES (?, ?, ?, ?)",
        (hash_value, "unit-encoder", 2, vector_to_bytes(vector)),
    )

    loaded = cache.get_embedding(
        hash_value,
        "unit-encoder",
        backend="unit_backend",
        namespace=embedding_namespace("unit_backend", "unit-encoder"),
    )

    np.testing.assert_allclose(loaded, vector)


def test_cached_text_hash_normalization_remains_stable() -> None:
    from engine.nlp.cache import normalize_text, text_hash

    messy = "  Cafe\u0301   revenue\nbeat\t expectations  "
    normalized = "Caf\u00e9 revenue beat expectations"

    assert normalize_text(messy) == normalized
    assert text_hash(messy) == "78c531b35c869d9553700cff4b96312e7f94dc93"
    assert text_hash(messy) == text_hash(normalized)


def test_sentiment_cache_writes_embedding_and_scalar_summary() -> None:
    from engine.nlp.cache import NlpCache
    from engine.nlp.encoder import FinBertSentimentEncoder

    con = sqlite3.connect(":memory:")
    cache = NlpCache(con)
    cache.ensure_schema()
    calls = {"count": 0}

    def fake_predict(texts: list[str]) -> np.ndarray:
        calls["count"] += 1
        return np.asarray([[0.7, 0.1, 0.2] for _ in texts], dtype=np.float32)

    encoder = FinBertSentimentEncoder(predict_fn=fake_predict)
    first = cache.get_or_encode_sentiments(["Strong profit growth"], encoder, source="news", ts=2, symbol="MSFT")
    second = cache.get_or_encode_sentiments(["Strong profit growth"], encoder, source="news", ts=2, symbol="MSFT")

    assert calls["count"] == 1
    assert first.summaries[0]["label"] == "positive"
    assert abs(float(first.summaries[0]["score"]) - 0.6) < 1e-6
    assert second.misses == 0
    assert con.execute("SELECT COUNT(*) FROM nlp_sentiments").fetchone()[0] == 1
    assert con.execute("SELECT COUNT(*) FROM nlp_embeddings").fetchone()[0] == 1
