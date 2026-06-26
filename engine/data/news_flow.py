"""Backend-aware news novelty and flow features.

README:
- Source: normalized news ``events`` plus persisted ``news_event_features`` and
  story embeddings computed from article title/body.
- Cadence: ``process_news_flow`` is registered for periodic batches
  (default 900 seconds).
- Availability lag: every embedding row carries ``availability_ts_ms`` from the
  ingested event timestamp; feature joins only use rows whose availability is
  <= the requested ``ts_ms``.
- Caveats: novelty comparisons are only valid within the same
  ``NEWS_EMBED_BACKEND`` and model name. Changing backend/model intentionally
  starts a clean novelty history rather than mixing embedding spaces.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np

from engine.nlp.cache import bytes_to_vector, vector_to_bytes
from engine.nlp.encoder import (
    EncodedTextBatch,
    TextEmbeddingConfig,
    encode_texts_with_config,
    embedding_namespace,
    resolve_text_embedding_config,
)
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.metrics import emit_gauge
from engine.runtime.storage import connect, run_write_txn

LOG = get_logger("engine.data.news_flow")
_WARNED_NONFATAL_KEYS: set[str] = set()
NEWS_FLOW_METRIC_COMPONENT = "engine.data.news_flow"

NEWS_FLOW_FEATURE_IDS = [
    "news_novelty_max_24h",
    "news_stale_share_24h",
    "news_velocity_z",
    "fresh_neg_news_flag",
]

NEWS_NOVELTY_LOOKBACK_MS = int(float(os.environ.get("NEWS_NOVELTY_LOOKBACK_DAYS", "7")) * 24 * 3600 * 1000)
NEWS_NOVELTY_MAX_COMPARISONS = max(1, int(os.environ.get("NEWS_NOVELTY_MAX_COMPARISONS", "200")))
NEWS_STALE_SIM_THRESHOLD = float(os.environ.get("NEWS_STALE_SIM_THRESHOLD", "0.85"))
NEWS_FLOW_BASELINE_DAYS = max(2, int(os.environ.get("NEWS_FLOW_BASELINE_DAYS", "7")))
NEWS_FRESH_NOVELTY_MIN = float(os.environ.get("NEWS_FRESH_NOVELTY_MIN", "0.5"))
NEWS_NEGATIVE_SENTIMENT_THRESHOLD = float(os.environ.get("NEWS_NEGATIVE_SENTIMENT_THRESHOLD", "-0.1"))


@dataclass(frozen=True)
class NewsEmbeddingConfig:
    backend: str
    model_name: str
    fallback_policy: str = "skip"
    local_files_only: bool = False

    @property
    def namespace(self) -> str:
        return embedding_namespace(self.backend, self.model_name)


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.data.news_flow",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _json_dumps(data: Dict[str, Any]) -> str:
    return json.dumps(dict(data or {}), separators=(",", ":"), sort_keys=True)


def _row_get(row: Any, key: str, idx: int, default: Any = None) -> Any:
    try:
        if hasattr(row, "keys"):
            return row[key]
    # system-audit: ignore[silent_except] mapping access failure falls back to positional row access.
    except Exception:
        pass
    try:
        return row[idx]
    except Exception:
        return default


def _norm_symbol(value: Any) -> str:
    return str(value or "").upper().strip()


def normalize_news_text(title: Any, body: Any = "") -> str:
    return "\n".join(part for part in (str(title or "").strip(), str(body or "").strip()) if part).strip()


def text_hash(text: Any) -> str:
    normalized = " ".join(str(text or "").split()).strip()
    return hashlib.sha1(normalized.encode("utf-8", errors="ignore")).hexdigest()


def current_embedding_config() -> NewsEmbeddingConfig:
    cfg = resolve_text_embedding_config(kind="news")
    return NewsEmbeddingConfig(
        backend=str(cfg.backend),
        model_name=str(cfg.model_name),
        fallback_policy=str(cfg.fallback_policy),
        local_files_only=bool(cfg.local_files_only),
    )


def _normalize_matrix(values: np.ndarray) -> np.ndarray:
    arr = np.asarray(values, dtype=np.float32)
    if arr.ndim == 1:
        arr = arr.reshape(1, -1)
    if arr.size == 0:
        return arr.astype(np.float32)
    arr = np.nan_to_num(arr, nan=0.0, posinf=0.0, neginf=0.0).astype(np.float32)
    return arr


def _hashing_embeddings(texts: Sequence[str], dim: int = 128) -> np.ndarray:
    rows: List[np.ndarray] = []
    for text in texts:
        vec = np.zeros(int(dim), dtype=np.float32)
        for token in str(text or "").lower().split():
            digest = hashlib.blake2b(token.encode("utf-8", errors="ignore"), digest_size=8).digest()
            bucket = int.from_bytes(digest[:4], "little") % int(dim)
            sign = 1.0 if (digest[4] % 2) == 0 else -1.0
            vec[bucket] += float(sign)
        rows.append(vec)
    return np.vstack(rows).astype(np.float32) if rows else np.zeros((0, int(dim)), dtype=np.float32)


def _to_text_embedding_config(config: Optional[NewsEmbeddingConfig | TextEmbeddingConfig]) -> TextEmbeddingConfig:
    cfg = config or current_embedding_config()
    if isinstance(cfg, TextEmbeddingConfig):
        return cfg
    return TextEmbeddingConfig(
        backend=str(cfg.backend),
        model_name=str(cfg.model_name),
        batch_size=max(1, int(os.environ.get("NEWS_EMBED_BATCH_SIZE", "32" if str(cfg.backend) == "finbert" else "64"))),
        cache_dir=str(os.environ.get("NLP_MODEL_CACHE_DIR", "") or "") or None,
        device=str(os.environ.get("NLP_DEVICE", os.environ.get("EMBED_DEVICE", "")) or "").strip(),
        local_files_only=bool(getattr(cfg, "local_files_only", False)),
        fallback_policy=str(getattr(cfg, "fallback_policy", "skip") or "skip").strip().lower(),
        dim=max(1, int(os.environ.get("NEWS_EMBED_HASHING_DIM", os.environ.get("NLP_HASHING_EMBED_DIM", "128")) or 128)),
        api_key=str(os.environ.get("OPENAI_API_KEY", "") or "").strip(),
    )


def _news_config_from_text_config(config: TextEmbeddingConfig) -> NewsEmbeddingConfig:
    return NewsEmbeddingConfig(
        backend=str(config.backend),
        model_name=str(config.model_name),
        fallback_policy=str(config.fallback_policy),
        local_files_only=bool(config.local_files_only),
    )


def encode_news_texts_with_metadata(
    texts: Sequence[str],
    config: Optional[NewsEmbeddingConfig | TextEmbeddingConfig] = None,
) -> EncodedTextBatch:
    return encode_texts_with_config(texts, _to_text_embedding_config(config))


def encode_news_texts(texts: Sequence[str], config: Optional[NewsEmbeddingConfig | TextEmbeddingConfig] = None) -> np.ndarray:
    rows = [str(text or "") for text in list(texts or [])]
    if not rows:
        return np.zeros((0, 0), dtype=np.float32)
    encoded = encode_news_texts_with_metadata(rows, config)
    if encoded.values.shape[0] == 0 and encoded.degraded:
        raise RuntimeError(";".join(encoded.errors) or "news_embedding_backend_unavailable")
    return _normalize_matrix(encoded.values)


def cosine_max_similarity(vector: np.ndarray, candidates: Sequence[np.ndarray]) -> float:
    vec = np.asarray(vector, dtype=np.float32).reshape(-1)
    mats = [np.asarray(candidate, dtype=np.float32).reshape(-1) for candidate in list(candidates or [])]
    mats = [candidate for candidate in mats if candidate.size == vec.size and candidate.size > 0]
    if not mats or vec.size == 0:
        return 0.0
    mat = np.vstack(mats).astype(np.float32, copy=False)
    vec_norm = float(np.linalg.norm(vec))
    mat_norm = np.linalg.norm(mat, axis=1)
    good = mat_norm > 1e-12
    if vec_norm <= 1e-12 or not bool(np.any(good)):
        return 0.0
    sims = (mat[good] @ vec) / (mat_norm[good] * vec_norm)
    if sims.size == 0:
        return 0.0
    return float(np.max(sims))


def _assert_same_embedding_space(
    vector_space: str | None,
    candidate_spaces: Sequence[str | None] | None,
) -> None:
    if not vector_space or candidate_spaces is None:
        return
    expected = str(vector_space)
    for candidate_space in list(candidate_spaces or []):
        if not candidate_space:
            continue
        if str(candidate_space) != expected:
            raise ValueError(f"mixed_embedding_space_refused:{expected}!={candidate_space}")


def novelty_from_vector(
    vector: np.ndarray,
    candidates: Sequence[np.ndarray],
    *,
    vector_space: str | None = None,
    candidate_spaces: Sequence[str | None] | None = None,
) -> Tuple[float, float, bool]:
    _assert_same_embedding_space(vector_space, candidate_spaces)
    max_sim = cosine_max_similarity(vector, candidates)
    novelty = float(max(0.0, min(1.0, 1.0 - max(0.0, max_sim))))
    stale = bool(max_sim > float(NEWS_STALE_SIM_THRESHOLD))
    return novelty, float(max_sim), stale


def ensure_news_flow_tables(con) -> None:
    for column_name, column_type in (
        ("payload_json", "JSONB"),
        ("embedding_backend", "TEXT"),
        ("embedding_model_name", "TEXT"),
        ("embedding_novelty_score", "DOUBLE PRECISION"),
        ("embedding_max_similarity", "DOUBLE PRECISION"),
        ("stale_flag", "BIGINT"),
        ("novelty_computed_ts_ms", "BIGINT"),
    ):
        try:
            con.execute(f"ALTER TABLE news_event_features ADD COLUMN IF NOT EXISTS {column_name} {column_type}")
        except Exception:
            try:
                con.execute(f"ALTER TABLE news_event_features ADD COLUMN {column_name} {column_type}")
            except Exception:
                continue
    try:
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_news_event_features_event_id
              ON news_event_features(event_id)
            """
        )
    # system-audit: ignore[silent_except] optional index creation must not block core table creation.
    except Exception:
        pass
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS news_story_embeddings (
            id BIGSERIAL PRIMARY KEY,
            event_id BIGINT NOT NULL,
            symbol TEXT NOT NULL,
            publish_ts_ms BIGINT,
            availability_ts_ms BIGINT NOT NULL,
            source TEXT,
            embedding_backend TEXT NOT NULL,
            model_name TEXT NOT NULL,
            dim BIGINT NOT NULL,
            vector BYTEA NOT NULL,
            text_hash TEXT,
            novelty_score DOUBLE PRECISION NOT NULL DEFAULT 1.0,
            max_similarity DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            stale_flag BIGINT NOT NULL DEFAULT 0,
            matched_event_id BIGINT,
            ingested_ts_ms BIGINT,
            payload_json JSONB,
            diagnostics_json JSONB
        )
        """
    )
    con.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_news_story_embeddings_event_space
          ON news_story_embeddings(event_id, symbol, embedding_backend, model_name)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_news_story_embeddings_symbol_space_avail
          ON news_story_embeddings(symbol, embedding_backend, model_name, availability_ts_ms DESC)
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS news_flow_features (
            symbol TEXT NOT NULL,
            asof_ts_ms BIGINT NOT NULL,
            bucket_ts_ms BIGINT NOT NULL,
            embedding_backend TEXT NOT NULL,
            model_name TEXT NOT NULL,
            news_novelty_max_24h DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            news_stale_share_24h DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            news_velocity_z DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            fresh_neg_news_flag DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            event_count_24h BIGINT NOT NULL DEFAULT 0,
            source_max_availability_ts_ms BIGINT,
            created_ts_ms BIGINT,
            meta_json JSONB,
            PRIMARY KEY(symbol, asof_ts_ms, embedding_backend, model_name)
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_news_flow_features_symbol_asof
          ON news_flow_features(symbol, asof_ts_ms DESC)
        """
    )


def _fetch_pending_events(con, *, config: NewsEmbeddingConfig, limit: int) -> List[Dict[str, Any]]:
    rows = con.execute(
        """
        SELECT
          e.id,
          e.ts_ms,
          COALESCE(e.timestamp, e.ts_ms),
          e.source,
          e.title,
          e.body,
          e.symbol,
          e.source_id,
          e.event_key
        FROM events e
        LEFT JOIN news_story_embeddings nse
          ON nse.event_id = e.id
         AND nse.symbol = e.symbol
         AND nse.embedding_backend = ?
         AND nse.model_name = ?
        WHERE e.event_type = 'news'
          AND e.symbol IS NOT NULL
          AND e.symbol != ''
          AND LENGTH(COALESCE(e.title, '') || COALESCE(e.body, '')) > 0
          AND nse.event_id IS NULL
        ORDER BY COALESCE(e.timestamp, e.ts_ms) ASC, e.id ASC
        LIMIT ?
        """,
        (str(config.backend), str(config.model_name), int(limit)),
    ).fetchall()
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        symbol = _norm_symbol(_row_get(row, "symbol", 6))
        text = normalize_news_text(_row_get(row, "title", 4), _row_get(row, "body", 5))
        if not symbol or not text:
            continue
        publish_ts_ms = _safe_int(_row_get(row, "ts_ms", 1), 0)
        availability_ts_ms = _safe_int(_row_get(row, "availability_ts_ms", 2), publish_ts_ms)
        out.append(
            {
                "event_id": _safe_int(_row_get(row, "id", 0), 0),
                "publish_ts_ms": int(publish_ts_ms),
                "availability_ts_ms": int(availability_ts_ms or publish_ts_ms),
                "source": str(_row_get(row, "source", 3, "news") or "news"),
                "title": str(_row_get(row, "title", 4, "") or ""),
                "body": str(_row_get(row, "body", 5, "") or ""),
                "symbol": symbol,
                "source_id": _row_get(row, "source_id", 7),
                "event_key": _row_get(row, "event_key", 8),
                "text": text,
            }
        )
    return out


def _recent_embedding_rows(
    con,
    *,
    symbol: str,
    availability_ts_ms: int,
    config: NewsEmbeddingConfig,
    exclude_event_id: Optional[int] = None,
    max_rows: int = NEWS_NOVELTY_MAX_COMPARISONS,
) -> List[Dict[str, Any]]:
    cutoff = int(availability_ts_ms) - int(NEWS_NOVELTY_LOOKBACK_MS)
    rows = con.execute(
        """
        SELECT event_id, dim, vector, availability_ts_ms
        FROM news_story_embeddings
        WHERE symbol = ?
          AND embedding_backend = ?
          AND model_name = ?
          AND availability_ts_ms <= ?
          AND availability_ts_ms >= ?
          AND event_id != ?
        ORDER BY availability_ts_ms DESC, event_id DESC
        LIMIT ?
        """,
        (
            str(symbol),
            str(config.backend),
            str(config.model_name),
            int(availability_ts_ms),
            int(cutoff),
            int(exclude_event_id or -1),
            int(max_rows),
        ),
    ).fetchall()
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        dim = _safe_int(_row_get(row, "dim", 1), 0)
        if dim <= 0:
            continue
        out.append(
            {
                "event_id": _safe_int(_row_get(row, "event_id", 0), 0),
                "vector": bytes_to_vector(_row_get(row, "vector", 2, b""), dim),
                "availability_ts_ms": _safe_int(_row_get(row, "availability_ts_ms", 3), 0),
            }
        )
    return out


def _prefetch_recent_embedding_rows(
    con,
    *,
    events: Sequence[Dict[str, Any]],
    config: NewsEmbeddingConfig,
) -> Tuple[Dict[str, List[Dict[str, Any]]], int]:
    event_rows = [dict(event or {}) for event in list(events or [])]
    symbols = sorted({_norm_symbol(event.get("symbol")) for event in event_rows if _norm_symbol(event.get("symbol"))})
    if not event_rows or not symbols:
        return {}, 0
    min_cutoff = min(
        int(event.get("availability_ts_ms") or event.get("publish_ts_ms") or 0) - int(NEWS_NOVELTY_LOOKBACK_MS)
        for event in event_rows
    )
    max_availability = max(
        int(event.get("availability_ts_ms") or event.get("publish_ts_ms") or 0)
        for event in event_rows
    )
    placeholders = ",".join("?" for _ in symbols)
    rows = con.execute(
        f"""
        SELECT event_id, symbol, dim, vector, availability_ts_ms
        FROM news_story_embeddings
        WHERE symbol IN ({placeholders})
          AND embedding_backend = ?
          AND model_name = ?
          AND availability_ts_ms <= ?
          AND availability_ts_ms >= ?
        ORDER BY symbol ASC, availability_ts_ms DESC, event_id DESC
        """,
        (
            *symbols,
            str(config.backend),
            str(config.model_name),
            int(max_availability),
            int(min_cutoff),
        ),
    ).fetchall()
    by_symbol: Dict[str, List[Dict[str, Any]]] = {symbol: [] for symbol in symbols}
    count = 0
    for row in rows or []:
        dim = _safe_int(_row_get(row, "dim", 2), 0)
        if dim <= 0:
            continue
        symbol = _norm_symbol(_row_get(row, "symbol", 1))
        if not symbol:
            continue
        by_symbol.setdefault(symbol, []).append(
            {
                "event_id": _safe_int(_row_get(row, "event_id", 0), 0),
                "vector": bytes_to_vector(_row_get(row, "vector", 3, b""), dim),
                "availability_ts_ms": _safe_int(_row_get(row, "availability_ts_ms", 4), 0),
            }
        )
        count += 1
    return by_symbol, int(count)


def _select_recent_embedding_rows(
    prefetched_by_symbol: Dict[str, List[Dict[str, Any]]],
    in_cycle_by_symbol: Dict[str, List[Dict[str, Any]]],
    *,
    event: Dict[str, Any],
    max_rows: int = NEWS_NOVELTY_MAX_COMPARISONS,
) -> List[Dict[str, Any]]:
    symbol = _norm_symbol(event.get("symbol"))
    if not symbol:
        return []
    event_id = int(event.get("event_id") or -1)
    availability_ts_ms = int(event.get("availability_ts_ms") or event.get("publish_ts_ms") or 0)
    cutoff = int(availability_ts_ms) - int(NEWS_NOVELTY_LOOKBACK_MS)
    candidates: List[Dict[str, Any]] = []
    for row in list(prefetched_by_symbol.get(symbol) or []) + list(in_cycle_by_symbol.get(symbol) or []):
        row_event_id = int(row.get("event_id") or -1)
        row_availability = int(row.get("availability_ts_ms") or 0)
        if row_event_id == event_id:
            continue
        if row_availability > int(availability_ts_ms) or row_availability < int(cutoff):
            continue
        candidates.append(row)
    candidates.sort(key=lambda row: (int(row.get("availability_ts_ms") or 0), int(row.get("event_id") or 0)), reverse=True)
    return candidates[: max(1, int(max_rows))]


def _put_story_embeddings_batch(
    con,
    *,
    items: Sequence[Dict[str, Any]],
    config: NewsEmbeddingConfig,
    now_ms: Optional[int] = None,
) -> Dict[str, int]:
    rows = [dict(item or {}) for item in list(items or [])]
    if not rows:
        return {
            "story_embedding_write_batches": 0,
            "event_feature_write_batches": 0,
            "write_batches": 0,
            "story_embedding_write_rows": 0,
            "event_feature_write_rows": 0,
        }

    story_params: List[Tuple[Any, ...]] = []
    event_feature_params: List[Tuple[Any, ...]] = []
    diagnostics = _json_dumps(
        {
            "lookback_ms": int(NEWS_NOVELTY_LOOKBACK_MS),
            "max_comparisons": int(NEWS_NOVELTY_MAX_COMPARISONS),
            "stale_similarity_threshold": float(NEWS_STALE_SIM_THRESHOLD),
            "embedding_namespace": config.namespace,
        }
    )
    event_feature_meta = _json_dumps(
        {
            "news_flow": True,
            "embedding_backend": config.backend,
            "embedding_model_name": config.model_name,
            "embedding_namespace": config.namespace,
        }
    )
    ingested_ts_ms = int(now_ms or _now_ms())

    for item in rows:
        event = dict(item.get("event") or {})
        arr = np.asarray(item.get("vector"), dtype=np.float32).reshape(-1)
        novelty = float(item.get("novelty") or 0.0)
        max_similarity = float(item.get("max_similarity") or 0.0)
        stale = bool(item.get("stale"))
        matched_event_id = item.get("matched_event_id")
        payload = {
            "source_id": event.get("source_id"),
            "event_key": event.get("event_key"),
            "text_hash": text_hash(event.get("text")),
            "embedding_backend": config.backend,
            "embedding_model_name": config.model_name,
            "embedding_namespace": config.namespace,
        }
        availability_ts_ms = int(event.get("availability_ts_ms") or event.get("publish_ts_ms") or 0)
        publish_ts_ms = int(event.get("publish_ts_ms") or availability_ts_ms or 0)
        story_params.append(
            (
                int(event["event_id"]),
                str(event["symbol"]),
                int(publish_ts_ms),
                int(availability_ts_ms),
                str(event.get("source") or "news"),
                str(config.backend),
                str(config.model_name),
                int(arr.size),
                vector_to_bytes(arr),
                str(payload["text_hash"]),
                float(novelty),
                float(max_similarity),
                1 if stale else 0,
                int(matched_event_id) if matched_event_id else None,
                int(ingested_ts_ms),
                _json_dumps(payload),
                diagnostics,
            )
        )
        event_feature_params.append(
            (
                int(event["event_id"]),
                int(availability_ts_ms),
                str(event["symbol"]),
                float(novelty),
                str(config.backend),
                str(config.model_name),
                float(novelty),
                float(max_similarity),
                1 if stale else 0,
                int(ingested_ts_ms),
                event_feature_meta,
            )
        )

    con.executemany(
        """
        INSERT INTO news_story_embeddings(
          event_id, symbol, publish_ts_ms, availability_ts_ms, source,
          embedding_backend, model_name, dim, vector, text_hash,
          novelty_score, max_similarity, stale_flag, matched_event_id,
          ingested_ts_ms, payload_json, diagnostics_json
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(event_id, symbol, embedding_backend, model_name) DO UPDATE SET
          publish_ts_ms=excluded.publish_ts_ms,
          availability_ts_ms=excluded.availability_ts_ms,
          source=excluded.source,
          dim=excluded.dim,
          vector=excluded.vector,
          text_hash=excluded.text_hash,
          novelty_score=excluded.novelty_score,
          max_similarity=excluded.max_similarity,
          stale_flag=excluded.stale_flag,
          matched_event_id=excluded.matched_event_id,
          ingested_ts_ms=excluded.ingested_ts_ms,
          payload_json=excluded.payload_json,
          diagnostics_json=excluded.diagnostics_json
        """,
        story_params,
    )
    con.executemany(
        """
        INSERT INTO news_event_features(
          event_id, ts_ms, symbol, novelty_score,
          embedding_backend, embedding_model_name, embedding_novelty_score,
          embedding_max_similarity, stale_flag, novelty_computed_ts_ms, meta_json
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(event_id) DO UPDATE SET
          ts_ms=COALESCE(excluded.ts_ms, news_event_features.ts_ms),
          symbol=COALESCE(excluded.symbol, news_event_features.symbol),
          novelty_score=excluded.novelty_score,
          embedding_backend=excluded.embedding_backend,
          embedding_model_name=excluded.embedding_model_name,
          embedding_novelty_score=excluded.embedding_novelty_score,
          embedding_max_similarity=excluded.embedding_max_similarity,
          stale_flag=excluded.stale_flag,
          novelty_computed_ts_ms=excluded.novelty_computed_ts_ms,
          meta_json=COALESCE(news_event_features.meta_json, excluded.meta_json)
        """,
        event_feature_params,
    )
    return {
        "story_embedding_write_batches": 1,
        "event_feature_write_batches": 1,
        "write_batches": 2,
        "story_embedding_write_rows": int(len(story_params)),
        "event_feature_write_rows": int(len(event_feature_params)),
    }


def _emit_news_flow_batch_metrics(result: Dict[str, Any], *, config: NewsEmbeddingConfig) -> None:
    tags = {"backend": str(config.backend), "model_name": str(config.model_name)}
    metrics = {
        "news_flow_batch_size": result.get("batch_size", result.get("rows_seen", 0)),
        "news_flow_recent_embedding_prefetch_rows": result.get("recent_embedding_prefetch_rows", 0),
        "news_flow_recent_embedding_queries": result.get("recent_embedding_queries", 0),
        "news_flow_write_batches": result.get("write_batches", 0),
        "news_flow_embedding_db_round_trips": result.get("embedding_db_round_trips", 0),
    }
    for metric, value in metrics.items():
        emit_gauge(
            str(metric),
            value,
            component=NEWS_FLOW_METRIC_COMPONENT,
            job="process_news_flow",
            extra_tags=tags,
        )


def process_news_flow_batch(
    *,
    limit: int = 100,
    config: Optional[NewsEmbeddingConfig] = None,
    now_ms: Optional[int] = None,
) -> Dict[str, Any]:
    cfg = config or current_embedding_config()
    started = int(now_ms or _now_ms())
    con = connect(readonly=True)
    try:
        candidates = _fetch_pending_events(con, config=cfg, limit=int(limit))
    finally:
        try:
            con.close()
        except Exception as exc:
            _warn_nonfatal("NEWS_FLOW_FETCH_CLOSE_FAILED", exc, once_key="fetch_close")

    if not candidates:
        empty_result = {
            "rows_seen": 0,
            "batch_size": 0,
            "encoded": 0,
            "written": 0,
            "backend": cfg.backend,
            "model_name": cfg.model_name,
            "pending_event_queries": 1,
            "recent_embedding_queries": 0,
            "recent_embedding_prefetch_rows": 0,
            "write_batches": 0,
            "embedding_db_round_trips": 0,
            "core_db_round_trips": 1,
        }
        _emit_news_flow_batch_metrics(empty_result, config=cfg)
        return empty_result

    encoded_batch = encode_news_texts_with_metadata([str(row["text"]) for row in candidates], cfg)
    if encoded_batch.degraded and encoded_batch.values.shape[0] != len(candidates):
        degraded_result = {
            "rows_seen": int(len(candidates)),
            "batch_size": int(len(candidates)),
            "encoded": 0,
            "written": 0,
            "backend": cfg.backend,
            "model_name": cfg.model_name,
            "requested_backend": cfg.backend,
            "requested_model_name": cfg.model_name,
            "degraded": True,
            "errors": list(encoded_batch.errors),
            "pending_event_queries": 1,
            "recent_embedding_queries": 0,
            "recent_embedding_prefetch_rows": 0,
            "write_batches": 0,
            "embedding_db_round_trips": 0,
            "core_db_round_trips": 1,
        }
        _emit_news_flow_batch_metrics(degraded_result, config=cfg)
        return degraded_result
    effective_cfg = _news_config_from_text_config(encoded_batch.effective_config)
    embeddings = encoded_batch.values
    if embeddings.shape[0] != len(candidates):
        raise RuntimeError(f"news_embedding_count_mismatch:{embeddings.shape[0]}:{len(candidates)}")

    def _write(db) -> Dict[str, Any]:
        ensure_news_flow_tables(db)
        prefetched_by_symbol, prefetch_rows = _prefetch_recent_embedding_rows(db, events=candidates, config=effective_cfg)
        recent_embedding_queries = 1
        in_cycle_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
        write_items: List[Dict[str, Any]] = []
        touched_symbols: set[str] = set()
        written = 0
        stale_count = 0
        for event, vector in zip(candidates, embeddings):
            symbol = _norm_symbol(event.get("symbol"))
            recent = _select_recent_embedding_rows(
                prefetched_by_symbol,
                in_cycle_by_symbol,
                event=event,
                max_rows=int(NEWS_NOVELTY_MAX_COMPARISONS),
            )
            novelty, max_similarity, stale = novelty_from_vector(
                vector,
                [row["vector"] for row in recent],
                vector_space=effective_cfg.namespace,
                candidate_spaces=[effective_cfg.namespace for _row in recent],
            )
            matched_event_id = None
            if recent and max_similarity > 0.0:
                sims = [
                    (cosine_max_similarity(vector, [row["vector"]]), int(row["event_id"]))
                    for row in recent
                ]
                matched_event_id = max(sims, key=lambda item: item[0])[1]
            write_items.append(
                {
                    "event": event,
                    "vector": vector,
                    "novelty": float(novelty),
                    "max_similarity": float(max_similarity),
                    "stale": bool(stale),
                    "matched_event_id": matched_event_id,
                }
            )
            in_cycle_by_symbol.setdefault(symbol, []).append(
                {
                    "event_id": int(event["event_id"]),
                    "vector": np.asarray(vector, dtype=np.float32).reshape(-1),
                    "availability_ts_ms": int(event["availability_ts_ms"]),
                }
            )
            touched_symbols.add(symbol)
            written += 1
            stale_count += 1 if stale else 0
        write_stats = _put_story_embeddings_batch(
            db,
            items=write_items,
            config=effective_cfg,
            now_ms=int(started),
        )
        for symbol in sorted(touched_symbols):
            materialize_news_flow_features(db, symbol=symbol, ts_ms=int(started), config=effective_cfg)
        write_batches = int(write_stats.get("write_batches") or 0)
        return {
            "written": int(written),
            "stale": int(stale_count),
            "symbols": sorted(touched_symbols),
            "recent_embedding_queries": int(recent_embedding_queries),
            "recent_embedding_prefetch_rows": int(prefetch_rows),
            "embedding_db_round_trips": int(recent_embedding_queries + write_batches),
            **dict(write_stats),
        }

    result = run_write_txn(
        _write,
        table="news_story_embeddings",
        operation="process_news_flow_batch",
        context={"rows": int(len(candidates)), "backend": effective_cfg.backend, "model_name": effective_cfg.model_name},
    )
    final_result = {
        "rows_seen": int(len(candidates)),
        "batch_size": int(len(candidates)),
        "encoded": int(embeddings.shape[0]),
        "backend": str(effective_cfg.backend),
        "model_name": str(effective_cfg.model_name),
        "requested_backend": str(cfg.backend),
        "requested_model_name": str(cfg.model_name),
        "degraded": bool(encoded_batch.degraded),
        "errors": list(encoded_batch.errors),
        "pending_event_queries": 1,
        **dict(result or {}),
    }
    final_result["core_db_round_trips"] = int(
        final_result.get("pending_event_queries", 0)
        + final_result.get("recent_embedding_queries", 0)
        + final_result.get("write_batches", 0)
    )
    _emit_news_flow_batch_metrics(final_result, config=effective_cfg)
    return final_result


def _daily_counts(timestamps: Iterable[int], *, end_ts_ms: int, days: int) -> List[int]:
    counts = [0 for _ in range(max(1, int(days)))]
    day_ms = 24 * 3600 * 1000
    start = int(end_ts_ms) - int(days) * day_ms
    for ts in timestamps:
        value = int(ts or 0)
        if value < start or value >= int(end_ts_ms):
            continue
        idx = min(len(counts) - 1, max(0, int((value - start) // day_ms)))
        counts[idx] += 1
    return counts


def _zscore(value: float, history: Sequence[float]) -> float:
    vals = [float(v) for v in history if math.isfinite(float(v))]
    if len(vals) < 2:
        return 0.0
    mean = sum(vals) / float(len(vals))
    var = sum((x - mean) ** 2 for x in vals) / float(max(1, len(vals) - 1))
    sd = math.sqrt(max(0.0, var))
    if sd <= 1e-9:
        return 0.0
    return float(max(-10.0, min(10.0, (float(value) - mean) / sd)))


def resolve_news_flow_features(
    con,
    *,
    symbol: str,
    ts_ms: int,
    config: Optional[NewsEmbeddingConfig] = None,
) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    cfg = config or current_embedding_config()
    feature_map = {fid: 0.0 for fid in NEWS_FLOW_FEATURE_IDS}
    sym = _norm_symbol(symbol)
    if not sym:
        return feature_map, {"latest_availability_ts_ms": None, "embedding_backend": cfg.backend, "model_name": cfg.model_name}, False

    start_24h = int(ts_ms) - 24 * 3600 * 1000
    rows = con.execute(
        """
        SELECT
          nse.event_id,
          nse.availability_ts_ms,
          nse.novelty_score,
          nse.stale_flag,
          COALESCE(nef.finbert_score, nef.sentiment_score, 0.0) AS sentiment_score,
          nef.finbert_neg
        FROM news_story_embeddings nse
        LEFT JOIN (
          SELECT
            event_id,
            AVG(finbert_score) AS finbert_score,
            AVG(sentiment_score) AS sentiment_score,
            MAX(finbert_neg) AS finbert_neg
          FROM news_event_features
          WHERE event_id IS NOT NULL
          GROUP BY event_id
        ) nef
          ON nef.event_id = nse.event_id
        WHERE nse.symbol = ?
          AND nse.embedding_backend = ?
          AND nse.model_name = ?
          AND nse.availability_ts_ms <= ?
          AND nse.availability_ts_ms >= ?
        ORDER BY nse.availability_ts_ms DESC, nse.event_id DESC
        """,
        (sym, str(cfg.backend), str(cfg.model_name), int(ts_ms), int(start_24h)),
    ).fetchall()
    if not rows:
        return feature_map, {"latest_availability_ts_ms": None, "embedding_backend": cfg.backend, "model_name": cfg.model_name}, False

    novelty_values: List[float] = []
    stale_count = 0
    latest_availability = 0
    fresh_negative = 0.0
    for row in rows or []:
        availability = _safe_int(_row_get(row, "availability_ts_ms", 1), 0)
        novelty = _safe_float(_row_get(row, "novelty_score", 2), 0.0)
        stale = bool(_safe_int(_row_get(row, "stale_flag", 3), 0))
        sentiment = _safe_float(_row_get(row, "sentiment_score", 4), 0.0)
        finbert_neg = _safe_float(_row_get(row, "finbert_neg", 5), 0.0)
        latest_availability = max(int(latest_availability), int(availability))
        novelty_values.append(float(novelty))
        stale_count += 1 if stale else 0
        negative = bool(sentiment <= float(NEWS_NEGATIVE_SENTIMENT_THRESHOLD) or finbert_neg >= 0.5)
        if (not stale) and novelty >= float(NEWS_FRESH_NOVELTY_MIN) and negative:
            fresh_negative = 1.0

    baseline_start = int(ts_ms) - int(NEWS_FLOW_BASELINE_DAYS + 1) * 24 * 3600 * 1000
    baseline_rows = con.execute(
        """
        SELECT availability_ts_ms
        FROM news_story_embeddings
        WHERE symbol = ?
          AND embedding_backend = ?
          AND model_name = ?
          AND availability_ts_ms < ?
          AND availability_ts_ms >= ?
        ORDER BY availability_ts_ms DESC
        LIMIT ?
        """,
        (
            sym,
            str(cfg.backend),
            str(cfg.model_name),
            int(start_24h),
            int(baseline_start),
            int((NEWS_FLOW_BASELINE_DAYS + 1) * NEWS_NOVELTY_MAX_COMPARISONS),
        ),
    ).fetchall()
    baseline_counts = _daily_counts(
        [_safe_int(_row_get(row, "availability_ts_ms", 0), 0) for row in baseline_rows or []],
        end_ts_ms=int(start_24h),
        days=int(NEWS_FLOW_BASELINE_DAYS),
    )

    count_24h = int(len(rows))
    feature_map["news_novelty_max_24h"] = float(max(novelty_values) if novelty_values else 0.0)
    feature_map["news_stale_share_24h"] = float(stale_count / max(1, count_24h))
    feature_map["news_velocity_z"] = float(_zscore(float(count_24h), [float(v) for v in baseline_counts]))
    feature_map["fresh_neg_news_flag"] = float(fresh_negative)
    return (
        feature_map,
        {
            "latest_availability_ts_ms": int(latest_availability) if latest_availability > 0 else None,
            "window_start_ts_ms": int(start_24h),
            "event_count_24h": int(count_24h),
            "embedding_backend": str(cfg.backend),
            "model_name": str(cfg.model_name),
            "baseline_days": int(NEWS_FLOW_BASELINE_DAYS),
            "baseline_counts": list(baseline_counts),
        },
        True,
    )


def materialize_news_flow_features(
    con,
    *,
    symbol: str,
    ts_ms: int,
    config: Optional[NewsEmbeddingConfig] = None,
) -> Dict[str, Any]:
    cfg = config or current_embedding_config()
    ensure_news_flow_tables(con)
    features, meta, available = resolve_news_flow_features(con, symbol=str(symbol), ts_ms=int(ts_ms), config=cfg)
    bucket_ms = 3600 * 1000
    bucket_ts_ms = int(ts_ms // bucket_ms) * bucket_ms
    con.execute(
        """
        INSERT INTO news_flow_features(
          symbol, asof_ts_ms, bucket_ts_ms, embedding_backend, model_name,
          news_novelty_max_24h, news_stale_share_24h, news_velocity_z,
          fresh_neg_news_flag, event_count_24h, source_max_availability_ts_ms,
          created_ts_ms, meta_json
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol, asof_ts_ms, embedding_backend, model_name) DO UPDATE SET
          bucket_ts_ms=excluded.bucket_ts_ms,
          news_novelty_max_24h=excluded.news_novelty_max_24h,
          news_stale_share_24h=excluded.news_stale_share_24h,
          news_velocity_z=excluded.news_velocity_z,
          fresh_neg_news_flag=excluded.fresh_neg_news_flag,
          event_count_24h=excluded.event_count_24h,
          source_max_availability_ts_ms=excluded.source_max_availability_ts_ms,
          created_ts_ms=excluded.created_ts_ms,
          meta_json=excluded.meta_json
        """,
        (
            _norm_symbol(symbol),
            int(ts_ms),
            int(bucket_ts_ms),
            str(cfg.backend),
            str(cfg.model_name),
            float(features["news_novelty_max_24h"]),
            float(features["news_stale_share_24h"]),
            float(features["news_velocity_z"]),
            float(features["fresh_neg_news_flag"]),
            int(meta.get("event_count_24h") or 0),
            meta.get("latest_availability_ts_ms"),
            int(_now_ms()),
            _json_dumps(dict(meta, available=bool(available))),
        ),
    )
    return {"features": dict(features), "meta": dict(meta), "available": bool(available)}
