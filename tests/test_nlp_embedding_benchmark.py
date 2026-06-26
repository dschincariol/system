from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def test_embedding_benchmark_outputs_backend_decision_evidence() -> None:
    from engine.nlp.benchmark import run_embedding_benchmark
    from engine.nlp.encoder import TextEmbeddingConfig

    docs = [
        {"doc_id": "1", "source": "news", "symbol": "AAPL", "text": "Apple revenue beat guidance", "availability_ts_ms": 1, "label_value": 0.03},
        {"doc_id": "2", "source": "news", "symbol": "AAPL", "text": "Apple revenue beat guidance", "availability_ts_ms": 2, "label_value": 0.02},
        {"doc_id": "3", "source": "filing", "symbol": "AAPL", "text": "Apple operating cash flow improved", "availability_ts_ms": 3, "label_value": 0.01},
        {"doc_id": "4", "source": "news", "symbol": "MSFT", "text": "Microsoft cloud margin expanded", "availability_ts_ms": 4, "label_value": -0.01},
        {"doc_id": "5", "source": "transcript", "symbol": "MSFT", "text": "Microsoft cloud margin expanded", "availability_ts_ms": 5, "label_value": -0.02},
        {"doc_id": "6", "source": "news", "symbol": "MSFT", "text": "Microsoft capex warning pressured shares", "availability_ts_ms": 6, "label_value": -0.03},
        {"doc_id": "7", "source": "filing", "symbol": "NVDA", "text": "Nvidia demand remains supply constrained", "availability_ts_ms": 7, "label_value": 0.04},
        {"doc_id": "8", "source": "transcript", "symbol": "NVDA", "text": "Nvidia demand remains supply constrained", "availability_ts_ms": 8, "label_value": 0.05},
    ]
    cfg = TextEmbeddingConfig(backend="hashing", model_name="hashing-v1", dim=64)

    result = run_embedding_benchmark(docs=docs, config=cfg, asof_ts_ms=8, top_k=2, stale_threshold=0.80)

    assert result["backend"] == "hashing"
    assert result["namespace"] == "hashing:hashing-v1"
    assert result["sample_counts_by_source"]["news"] == 4
    assert result["metrics"]["retrieval_relevance"]["status"] == "ok"
    assert result["metrics"]["duplicate_staleness"]["status"] == "ok"
    assert result["metrics"]["entity_event_clustering"]["status"] == "ok"
    assert result["metrics"]["downstream_feature_ic_oos"]["status"] == "ok"
    assert result["decision_evidence"]["can_choose_backend"] is True
    assert set(result["decision_evidence"]["required_metrics"]) == {
        "retrieval_relevance",
        "duplicate_staleness",
        "entity_event_clustering",
        "downstream_feature_ic_oos",
    }


def test_benchmark_cached_text_loader_excludes_future_text() -> None:
    from engine.nlp.benchmark import load_cached_text_documents
    from engine.nlp.cache import text_hash

    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE nlp_text_blobs(hash TEXT PRIMARY KEY, source TEXT, ts INTEGER, symbol TEXT, text TEXT)")
    con.execute(
        "INSERT INTO nlp_text_blobs(hash, source, ts, symbol, text) VALUES (?, ?, ?, ?, ?)",
        (text_hash("available"), "news", 1_000, "AAPL", "available"),
    )
    con.execute(
        "INSERT INTO nlp_text_blobs(hash, source, ts, symbol, text) VALUES (?, ?, ?, ?, ?)",
        (text_hash("future"), "news", 2_000, "AAPL", "future"),
    )

    docs = load_cached_text_documents(con, asof_ts_ms=1_500)

    assert [doc.text for doc in docs] == ["available"]
