"""Local benchmark harness for financial text embedding backends."""

from __future__ import annotations

import argparse
import json
import math
import time
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np

from engine.nlp.cache import text_hash
from engine.nlp.encoder import (
    TextEmbeddingConfig,
    encode_texts_with_config,
    model_card_metadata,
    resolve_text_embedding_config,
)


@dataclass(frozen=True)
class BenchmarkDocument:
    doc_id: str
    source: str
    symbol: str
    text: str
    availability_ts_ms: int
    label_value: float | None = None
    event_type: str = ""
    hash: str = ""

    @property
    def text_hash(self) -> str:
        return str(self.hash or text_hash(self.text))


def _row_get(row: Any, key: str, idx: int, default: Any = None) -> Any:
    try:
        if hasattr(row, "keys"):
            return row[key]
    except Exception:
        pass  # no-op-guard: allow - row objects may not expose mapping keys.
    try:
        return row[idx]
    except Exception:
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def _cosine_matrix(vectors: np.ndarray) -> np.ndarray:
    arr = np.asarray(vectors, dtype=np.float32)
    if arr.ndim != 2 or arr.shape[0] == 0:
        return np.zeros((0, 0), dtype=np.float32)
    norms = np.linalg.norm(arr, axis=1, keepdims=True)
    norms[norms <= 1e-12] = 1.0
    unit = arr / norms
    return np.clip(unit @ unit.T, -1.0, 1.0).astype(np.float32)


def load_cached_text_documents(
    con: Any,
    *,
    asof_ts_ms: int,
    limit: int = 1000,
    sources: Sequence[str] = ("news", "filing", "transcript", "transcript_qa"),
) -> list[BenchmarkDocument]:
    placeholders = ",".join("?" for _ in list(sources or []))
    rows = con.execute(
        f"""
        SELECT hash, source, ts, symbol, text
        FROM nlp_text_blobs
        WHERE ts IS NOT NULL
          AND ts <= ?
          AND source IN ({placeholders})
          AND LENGTH(COALESCE(text, '')) > 0
        ORDER BY ts ASC, hash ASC
        LIMIT ?
        """,
        (int(asof_ts_ms), *[str(src) for src in sources], int(limit)),
    ).fetchall()
    out: list[BenchmarkDocument] = []
    for row in rows or []:
        out.append(
            BenchmarkDocument(
                doc_id=str(_row_get(row, "hash", 0, "")),
                source=str(_row_get(row, "source", 1, "") or ""),
                symbol=str(_row_get(row, "symbol", 3, "") or "").upper().strip(),
                text=str(_row_get(row, "text", 4, "") or ""),
                availability_ts_ms=int(_row_get(row, "ts", 2, 0) or 0),
                hash=str(_row_get(row, "hash", 0, "") or ""),
            )
        )
    return out


def _retrieval_relevance_metrics(docs: Sequence[BenchmarkDocument], vectors: np.ndarray, *, top_k: int = 5) -> dict[str, Any]:
    sims = _cosine_matrix(vectors)
    if sims.shape[0] < 2:
        return {"status": "insufficient", "sample_n": int(sims.shape[0])}
    labels = [str(doc.symbol or "").upper() for doc in docs]
    precisions: list[float] = []
    reciprocal_ranks: list[float] = []
    for idx, label in enumerate(labels):
        if not label or labels.count(label) < 2:
            continue
        order = [j for j in np.argsort(-sims[idx]).tolist() if j != idx]
        top = order[: max(1, int(top_k))]
        precisions.append(sum(1 for j in top if labels[j] == label) / float(max(1, len(top))))
        rr = 0.0
        for rank, j in enumerate(order, start=1):
            if labels[j] == label:
                rr = 1.0 / float(rank)
                break
        reciprocal_ranks.append(rr)
    if not precisions:
        return {"status": "insufficient", "sample_n": 0}
    return {
        "status": "ok",
        "sample_n": int(len(precisions)),
        "precision_at_k": float(np.mean(precisions)),
        "mrr": float(np.mean(reciprocal_ranks)),
        "top_k": int(top_k),
    }


def _duplicate_staleness_metrics(
    docs: Sequence[BenchmarkDocument],
    vectors: np.ndarray,
    *,
    threshold: float,
) -> dict[str, Any]:
    if len(docs) < 2:
        return {"status": "insufficient", "sample_n": int(len(docs))}
    sims = _cosine_matrix(vectors)
    order = sorted(range(len(docs)), key=lambda idx: (int(docs[idx].availability_ts_ms), str(docs[idx].doc_id)))
    tp = fp = tn = fn = 0
    samples = 0
    seen_hashes_by_symbol: dict[str, set[str]] = {}
    for pos, idx in enumerate(order):
        symbol = str(docs[idx].symbol or "")
        prior = order[:pos]
        if not prior:
            seen_hashes_by_symbol.setdefault(symbol, set()).add(docs[idx].text_hash)
            continue
        max_sim = max(float(sims[idx, j]) for j in prior)
        pred = bool(max_sim >= float(threshold))
        label = docs[idx].text_hash in seen_hashes_by_symbol.get(symbol, set())
        tp += 1 if pred and label else 0
        fp += 1 if pred and not label else 0
        tn += 1 if (not pred) and (not label) else 0
        fn += 1 if (not pred) and label else 0
        samples += 1
        seen_hashes_by_symbol.setdefault(symbol, set()).add(docs[idx].text_hash)
    precision = tp / float(max(1, tp + fp))
    recall = tp / float(max(1, tp + fn))
    f1 = 0.0 if precision + recall <= 0.0 else 2.0 * precision * recall / (precision + recall)
    return {
        "status": "ok" if samples else "insufficient",
        "sample_n": int(samples),
        "threshold": float(threshold),
        "precision": float(precision),
        "recall": float(recall),
        "f1": float(f1),
        "accuracy": float((tp + tn) / float(max(1, samples))),
        "confusion": {"tp": int(tp), "fp": int(fp), "tn": int(tn), "fn": int(fn)},
    }


def _entity_clustering_metrics(docs: Sequence[BenchmarkDocument], vectors: np.ndarray) -> dict[str, Any]:
    labels = [str(doc.symbol or "").upper() for doc in docs]
    usable_labels = sorted({label for label in labels if label and labels.count(label) >= 2})
    if len(usable_labels) < 2:
        return {"status": "insufficient", "sample_n": 0}
    arr = np.asarray(vectors, dtype=np.float32)
    sims = _cosine_matrix(arr)
    correct = 0
    samples = 0
    separation_values: list[float] = []
    for idx, label in enumerate(labels):
        if label not in usable_labels:
            continue
        centroid_scores: dict[str, float] = {}
        for candidate_label in usable_labels:
            member_idx = [j for j, other in enumerate(labels) if other == candidate_label and j != idx]
            if not member_idx:
                continue
            centroid_scores[candidate_label] = float(np.mean([sims[idx, j] for j in member_idx]))
        if not centroid_scores:
            continue
        predicted = max(centroid_scores.items(), key=lambda item: item[1])[0]
        correct += 1 if predicted == label else 0
        samples += 1
        own = centroid_scores.get(label)
        other_scores = [score for key, score in centroid_scores.items() if key != label]
        if own is not None and other_scores:
            separation_values.append(float(own - max(other_scores)))
    return {
        "status": "ok" if samples else "insufficient",
        "sample_n": int(samples),
        "purity": float(correct / float(max(1, samples))),
        "mean_similarity_margin": float(np.mean(separation_values)) if separation_values else 0.0,
    }


def _feature_score(vectors: np.ndarray) -> np.ndarray:
    arr = np.asarray(vectors, dtype=np.float64)
    if arr.ndim != 2 or arr.shape[0] == 0:
        return np.zeros((0,), dtype=np.float64)
    centered = arr - arr.mean(axis=0, keepdims=True)
    try:
        u, s, _vt = np.linalg.svd(centered, full_matrices=False)
        if s.size:
            return (u[:, 0] * s[0]).astype(np.float64)
    except Exception:
        pass  # no-op-guard: allow - SVD score falls back to vector norm.
    return np.linalg.norm(arr, axis=1).astype(np.float64)


def _downstream_ic_oos_metrics(docs: Sequence[BenchmarkDocument], vectors: np.ndarray) -> dict[str, Any]:
    pairs = [(idx, _safe_float(doc.label_value, float("nan"))) for idx, doc in enumerate(docs) if doc.label_value is not None]
    pairs = [(idx, value) for idx, value in pairs if math.isfinite(value)]
    if len(pairs) < 6:
        return {"status": "insufficient", "sample_n": int(len(pairs))}
    score = _feature_score(vectors)
    idxs = [idx for idx, _value in pairs]
    x = np.asarray([score[idx] for idx in idxs], dtype=np.float64)
    y = np.asarray([value for _idx, value in pairs], dtype=np.float64)
    if float(np.std(x)) <= 1e-12 or float(np.std(y)) <= 1e-12:
        return {"status": "insufficient", "sample_n": int(len(pairs)), "reason": "constant_series"}
    ic = float(np.corrcoef(x, y)[0, 1])
    split = max(3, min(len(x) - 2, int(len(x) * 0.7)))
    x_train = x[:split]
    y_train = y[:split]
    x_oos = x[split:]
    y_oos = y[split:]
    if len(y_oos) < 2:
        return {"status": "insufficient", "sample_n": int(len(pairs)), "ic": ic}
    beta = float(np.cov(x_train, y_train, ddof=1)[0, 1] / max(1e-12, np.var(x_train, ddof=1)))
    alpha = float(np.mean(y_train) - beta * np.mean(x_train))
    pred = alpha + beta * x_oos
    baseline = np.full_like(y_oos, float(np.mean(y_train)), dtype=np.float64)
    model_mse = float(np.mean((y_oos - pred) ** 2))
    baseline_mse = float(np.mean((y_oos - baseline) ** 2))
    contribution = 1.0 - model_mse / max(1e-12, baseline_mse)
    return {
        "status": "ok",
        "sample_n": int(len(pairs)),
        "ic": ic,
        "oos_r2_vs_mean": float(contribution),
        "train_n": int(len(y_train)),
        "oos_n": int(len(y_oos)),
    }


def _recommendation(metrics: Mapping[str, Mapping[str, Any]], *, sample_n: int) -> dict[str, Any]:
    required = ("retrieval_relevance", "duplicate_staleness", "entity_event_clustering", "downstream_feature_ic_oos")
    ready = [name for name in required if str((metrics.get(name) or {}).get("status")) == "ok"]
    reasons = [f"{name}:{(metrics.get(name) or {}).get('status', 'missing')}" for name in required if name not in ready]
    score_parts = [
        _safe_float((metrics.get("retrieval_relevance") or {}).get("precision_at_k")),
        _safe_float((metrics.get("duplicate_staleness") or {}).get("f1")),
        _safe_float((metrics.get("entity_event_clustering") or {}).get("purity")),
        max(0.0, min(1.0, 0.5 + _safe_float((metrics.get("downstream_feature_ic_oos") or {}).get("ic")) / 2.0)),
    ]
    enough = sample_n >= 6 and len(ready) == len(required)
    return {
        "can_choose_backend": bool(enough),
        "required_metrics": list(required),
        "ready_metrics": ready,
        "blocking_reasons": reasons,
        "score": float(np.mean(score_parts)) if score_parts else 0.0,
    }


def run_embedding_benchmark(
    *,
    docs: Sequence[BenchmarkDocument | Mapping[str, Any]] | None = None,
    con: Any | None = None,
    config: TextEmbeddingConfig | None = None,
    asof_ts_ms: int | None = None,
    limit: int = 1000,
    top_k: int = 5,
    stale_threshold: float = 0.85,
) -> dict[str, Any]:
    cfg = config or resolve_text_embedding_config(kind="news")
    if docs is None:
        if con is None:
            from engine.runtime.storage import connect

            con = connect(readonly=True)
        docs_list = load_cached_text_documents(con, asof_ts_ms=int(asof_ts_ms or time.time() * 1000), limit=int(limit))
    else:
        docs_list = [
            item
            if isinstance(item, BenchmarkDocument)
            else BenchmarkDocument(
                doc_id=str(item.get("doc_id") or item.get("hash") or len(str(item))),
                source=str(item.get("source") or "news"),
                symbol=str(item.get("symbol") or "").upper().strip(),
                text=str(item.get("text") or ""),
                availability_ts_ms=int(item.get("availability_ts_ms") or item.get("ts_ms") or 0),
                label_value=(None if item.get("label_value") is None else _safe_float(item.get("label_value"))),
                event_type=str(item.get("event_type") or ""),
                hash=str(item.get("hash") or ""),
            )
            for item in docs
        ]
    docs_list = [doc for doc in docs_list if doc.text and int(doc.availability_ts_ms or 0) <= int(asof_ts_ms or 2**63 - 1)]
    encoded = encode_texts_with_config([doc.text for doc in docs_list], cfg)
    counts: dict[str, int] = {}
    for doc in docs_list:
        counts[doc.source] = int(counts.get(doc.source, 0) + 1)
    base = {
        "backend": encoded.effective_config.backend,
        "model_name": encoded.effective_config.model_name,
        "namespace": encoded.effective_config.namespace,
        "requested_backend": encoded.requested_config.backend,
        "requested_model_name": encoded.requested_config.model_name,
        "sample_n": int(len(docs_list)),
        "sample_counts_by_source": counts,
        "degraded": bool(encoded.degraded),
        "errors": list(encoded.errors),
        "model_metadata": model_card_metadata(encoded.effective_config.backend, encoded.effective_config.model_name).to_dict(),
    }
    if encoded.values.shape[0] != len(docs_list) or encoded.values.shape[0] == 0:
        metrics = {
            "retrieval_relevance": {"status": "insufficient", "sample_n": 0},
            "duplicate_staleness": {"status": "insufficient", "sample_n": 0},
            "entity_event_clustering": {"status": "insufficient", "sample_n": 0},
            "downstream_feature_ic_oos": {"status": "insufficient", "sample_n": 0},
        }
        return {**base, "metrics": metrics, "decision_evidence": _recommendation(metrics, sample_n=len(docs_list))}
    metrics = {
        "retrieval_relevance": _retrieval_relevance_metrics(docs_list, encoded.values, top_k=int(top_k)),
        "duplicate_staleness": _duplicate_staleness_metrics(docs_list, encoded.values, threshold=float(stale_threshold)),
        "entity_event_clustering": _entity_clustering_metrics(docs_list, encoded.values),
        "downstream_feature_ic_oos": _downstream_ic_oos_metrics(docs_list, encoded.values),
    }
    return {**base, "metrics": metrics, "decision_evidence": _recommendation(metrics, sample_n=len(docs_list))}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", default=None)
    parser.add_argument("--model", default=None)
    parser.add_argument("--asof-ts-ms", type=int, default=int(time.time() * 1000))
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--stale-threshold", type=float, default=0.85)
    args = parser.parse_args(list(argv or []))
    cfg = resolve_text_embedding_config(kind="news", backend=args.backend, model_name=args.model)
    result = run_embedding_benchmark(
        config=cfg,
        asof_ts_ms=int(args.asof_ts_ms),
        limit=int(args.limit),
        top_k=int(args.top_k),
        stale_threshold=float(args.stale_threshold),
    )
    print(json.dumps(result, separators=(",", ":"), sort_keys=True))
    return 0 if result.get("decision_evidence", {}).get("can_choose_backend") else 2


if __name__ == "__main__":
    raise SystemExit(main())
