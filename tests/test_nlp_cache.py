from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class CountingEncoder:
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

    con = sqlite3.connect(":memory:")
    cache = NlpCache(con)
    cache.ensure_schema()
    encoder = CountingEncoder()
    texts = ["Apple revenue beat expectations."]

    first = cache.get_or_encode_embeddings(texts, encoder, source="news", ts=1, symbol="AAPL")
    expected_hash = text_hash(texts[0])
    first_bytes = con.execute(
        "SELECT vector FROM nlp_embeddings WHERE hash=? AND model_name=?",
        (expected_hash, encoder.model_name),
    ).fetchone()[0]
    second = cache.get_or_encode_embeddings(texts, encoder, source="news", ts=1, symbol="AAPL")
    second_bytes = con.execute(
        "SELECT vector FROM nlp_embeddings WHERE hash=? AND model_name=?",
        (expected_hash, encoder.model_name),
    ).fetchone()[0]

    assert encoder.calls == 1
    assert first.misses == 1
    assert second.misses == 0
    assert first_bytes == second_bytes
    np.testing.assert_allclose(first.values, second.values)


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
