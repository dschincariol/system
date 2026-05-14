"""Symbol-day NLP aggregation helpers."""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Any, Iterable

import numpy as np


DAY_MS = 86_400_000


def day_start_ms(ts_ms: int) -> int:
    return int(ts_ms // DAY_MS) * DAY_MS


def recency_weights(ts_values: list[int], *, half_life_hours: float = 36.0) -> np.ndarray:
    if not ts_values:
        return np.zeros((0,), dtype=np.float64)
    half_life_ms = max(1.0, float(half_life_hours) * 3_600_000.0)
    anchor = max(int(ts) for ts in ts_values)
    weights = [0.5 ** max(0.0, (float(anchor) - float(ts)) / half_life_ms) for ts in ts_values]
    arr = np.asarray(weights, dtype=np.float64)
    total = float(arr.sum())
    if total <= 0.0 or not math.isfinite(total):
        return np.ones((len(ts_values),), dtype=np.float64) / max(1, len(ts_values))
    return arr / total


def aggregate_symbol_day_documents(
    documents: Iterable[dict[str, Any]],
    *,
    symbol_key: str = "symbol",
    ts_key: str = "ts_ms",
    value_key: str = "score",
    vector_key: str = "embedding",
    half_life_hours: float = 36.0,
) -> list[dict[str, Any]]:
    """Aggregate document-level NLP outputs into one row per symbol UTC day."""

    grouped: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for doc in list(documents or []):
        symbol = str((doc or {}).get(symbol_key) or "").upper().strip()
        if not symbol:
            continue
        try:
            ts_ms = int((doc or {}).get(ts_key) or 0)
        except Exception:
            ts_ms = 0
        if ts_ms <= 0:
            continue
        grouped[(symbol, day_start_ms(ts_ms))].append(dict(doc or {}))

    out: list[dict[str, Any]] = []
    for (symbol, bucket_ts_ms), rows in sorted(grouped.items()):
        ts_values = [int(row.get(ts_key) or 0) for row in rows]
        weights = recency_weights(ts_values, half_life_hours=half_life_hours)
        result: dict[str, Any] = {
            "symbol": symbol,
            "day_ts_ms": int(bucket_ts_ms),
            "count": int(len(rows)),
        }

        scalar_values: list[float] = []
        scalar_ts: list[int] = []
        for row in rows:
            if value_key not in row:
                continue
            try:
                value = float(row.get(value_key))
            except Exception:
                continue
            if not math.isfinite(value):
                continue
            scalar_values.append(value)
            scalar_ts.append(int(row.get(ts_key) or 0))
        if scalar_values:
            values = np.asarray(scalar_values, dtype=np.float64)
            value_weights = recency_weights(scalar_ts, half_life_hours=half_life_hours)
            result[f"{value_key}_mean"] = float(values.mean())
            result[f"{value_key}_weighted_mean"] = float(np.dot(values, value_weights))
            result[f"{value_key}_max"] = float(values.max())

        vectors: list[np.ndarray] = []
        vector_ts: list[int] = []
        for row in rows:
            raw = row.get(vector_key)
            if raw is None:
                continue
            arr = np.asarray(raw, dtype=np.float32).reshape(-1)
            if arr.size <= 0:
                continue
            vectors.append(arr)
            vector_ts.append(int(row.get(ts_key) or 0))
        if vectors:
            matrix = np.vstack(vectors).astype(np.float32)
            vector_weights = recency_weights(vector_ts, half_life_hours=half_life_hours).astype(np.float32)
            result[f"{vector_key}_mean"] = matrix.mean(axis=0)
            result[f"{vector_key}_weighted_mean"] = np.average(matrix, axis=0, weights=vector_weights)
            result[f"{vector_key}_max"] = matrix.max(axis=0)

        out.append(result)
    return out
