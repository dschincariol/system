from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def test_empty_input_produces_no_rows() -> None:
    from engine.nlp.aggregators import aggregate_symbol_day_documents

    assert aggregate_symbol_day_documents([]) == []


def test_recency_weighted_mean_and_vector_pools() -> None:
    from engine.nlp.aggregators import aggregate_symbol_day_documents, recency_weights

    docs = [
        {"symbol": "AAPL", "ts_ms": 1_700_000_000_000, "score": 0.2, "embedding": np.asarray([1.0, 2.0])},
        {"symbol": "AAPL", "ts_ms": 1_700_000_000_000 + 3_600_000, "score": 0.8, "embedding": np.asarray([3.0, 1.0])},
    ]
    rows = aggregate_symbol_day_documents(docs, half_life_hours=1.0)
    weights = recency_weights([doc["ts_ms"] for doc in docs], half_life_hours=1.0)

    assert len(rows) == 1
    row = rows[0]
    assert row["symbol"] == "AAPL"
    assert row["count"] == 2
    assert abs(float(row["score_weighted_mean"]) - float(np.dot([0.2, 0.8], weights))) < 1e-9
    np.testing.assert_allclose(row["embedding_mean"], np.asarray([2.0, 1.5], dtype=np.float32))
    np.testing.assert_allclose(row["embedding_max"], np.asarray([3.0, 2.0], dtype=np.float32))
