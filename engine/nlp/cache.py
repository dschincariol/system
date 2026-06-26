"""Content-hash keyed cache for offline NLP outputs."""

from __future__ import annotations

import hashlib
import json
import time
import unicodedata
from dataclasses import dataclass
from typing import Any, Sequence

import numpy as np

from engine.nlp.encoder import FinBertSentimentEncoder, embedding_namespace, encoder_namespace


@dataclass(frozen=True)
class CacheResult:
    values: np.ndarray
    hashes: list[str]
    hits: int
    misses: int
    encoded: int
    summaries: list[dict[str, Any]]


def normalize_text(text: Any) -> str:
    normalized = unicodedata.normalize("NFC", str(text or ""))
    return " ".join(normalized.split()).strip()


def text_hash(text: Any) -> str:
    return hashlib.sha1(normalize_text(text).encode("utf-8")).hexdigest()


def vector_to_bytes(vector: np.ndarray) -> bytes:
    return np.asarray(vector, dtype=np.float32).tobytes(order="C")


def bytes_to_vector(payload: bytes | memoryview, dim: int) -> np.ndarray:
    raw = payload.tobytes() if isinstance(payload, memoryview) else bytes(payload or b"")
    arr = np.frombuffer(raw, dtype=np.float32)
    if int(dim or 0) > 0:
        arr = arr[: int(dim)]
    return np.array(arr, dtype=np.float32, copy=True)


def _row_get(row: Any, key: str, idx: int, default: Any = None) -> Any:
    if row is None:
        return default
    try:
        return row[key]
    except Exception:
        try:
            return row[idx]
        except Exception:
            return default


class NlpCache:
    """Small DB-backed cache for text blobs, embeddings, and sentiments."""

    def __init__(self, con: Any | None = None) -> None:
        self.con = con

    def ensure_schema(self) -> None:
        con, owns = self._connection(readonly=False)
        try:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS nlp_text_blobs(
                  hash TEXT PRIMARY KEY,
                  source TEXT,
                  ts INTEGER,
                  symbol TEXT NULL,
                  text TEXT
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS nlp_embeddings(
                  hash TEXT,
                  model_name TEXT,
                  backend TEXT DEFAULT 'legacy',
                  model_namespace TEXT,
                  dim INTEGER,
                  vector BYTEA,
                  model_metadata_json JSONB,
                  PRIMARY KEY(hash, model_name)
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS nlp_sentiments(
                  hash TEXT,
                  model_name TEXT,
                  backend TEXT DEFAULT 'legacy',
                  model_namespace TEXT,
                  score REAL,
                  label TEXT,
                  model_metadata_json JSONB,
                  PRIMARY KEY(hash, model_name)
                )
                """
            )
            self._ensure_optional_columns(
                con,
                "nlp_embeddings",
                (
                    ("backend", "TEXT DEFAULT 'legacy'"),
                    ("model_namespace", "TEXT"),
                    ("model_metadata_json", "JSONB"),
                ),
            )
            self._ensure_optional_columns(
                con,
                "nlp_sentiments",
                (
                    ("backend", "TEXT DEFAULT 'legacy'"),
                    ("model_namespace", "TEXT"),
                    ("model_metadata_json", "JSONB"),
                ),
            )
            self._commit(con)
        finally:
            self._close(con, owns)

    def record_text_blob(
        self,
        hash_value: str,
        text: str,
        *,
        source: str = "",
        ts: int | None = None,
        symbol: str | None = None,
        con: Any | None = None,
    ) -> None:
        db, owns = (con, False) if con is not None else self._connection(readonly=False)
        try:
            db.execute(
                """
                INSERT INTO nlp_text_blobs(hash, source, ts, symbol, text)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(hash) DO UPDATE SET
                  source=COALESCE(excluded.source, nlp_text_blobs.source),
                  ts=COALESCE(excluded.ts, nlp_text_blobs.ts),
                  symbol=COALESCE(excluded.symbol, nlp_text_blobs.symbol),
                  text=excluded.text
                """,
                (
                    str(hash_value),
                    str(source or ""),
                    int(ts) if ts is not None else None,
                    str(symbol).upper().strip() if symbol else None,
                    str(text or ""),
                ),
            )
            if owns:
                self._commit(db)
        finally:
            self._close(db, owns)

    def get_embedding(
        self,
        hash_value: str,
        model_name: str,
        *,
        backend: str | None = None,
        namespace: str | None = None,
        con: Any | None = None,
    ) -> np.ndarray | None:
        db, owns = (con, False) if con is not None else self._connection(readonly=True)
        try:
            keys = self._cache_key_candidates(model_name, backend=backend, namespace=namespace)
            row = None
            for key in keys:
                row = db.execute(
                    "SELECT dim, vector FROM nlp_embeddings WHERE hash=? AND model_name=?",
                    (str(hash_value), str(key)),
                ).fetchone()
                if row is not None:
                    break
            if row is None:
                return None
            return bytes_to_vector(_row_get(row, "vector", 1, b""), int(_row_get(row, "dim", 0, 0) or 0))
        finally:
            self._close(db, owns)

    def put_embedding(
        self,
        hash_value: str,
        model_name: str,
        vector: np.ndarray,
        *,
        backend: str | None = None,
        namespace: str | None = None,
        model_metadata: dict[str, Any] | None = None,
        con: Any | None = None,
    ) -> None:
        arr = np.asarray(vector, dtype=np.float32).reshape(-1)
        cache_key = str(namespace or (embedding_namespace(backend, model_name) if backend else str(model_name)))
        backend_key = str(backend or "legacy")
        metadata_json = self._json_dumps(
            dict(model_metadata or {}, backend=backend_key, model_name=str(model_name), namespace=cache_key)
        )
        db, owns = (con, False) if con is not None else self._connection(readonly=False)
        try:
            db.execute(
                """
                INSERT INTO nlp_embeddings(hash, model_name, backend, model_namespace, dim, vector, model_metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(hash, model_name) DO UPDATE SET
                  backend=excluded.backend,
                  model_namespace=excluded.model_namespace,
                  dim=excluded.dim,
                  vector=excluded.vector,
                  model_metadata_json=excluded.model_metadata_json
                """,
                (
                    str(hash_value),
                    cache_key,
                    backend_key,
                    cache_key,
                    int(arr.size),
                    vector_to_bytes(arr),
                    metadata_json,
                ),
            )
            if owns:
                self._commit(db)
        finally:
            self._close(db, owns)

    def get_sentiment(
        self,
        hash_value: str,
        model_name: str,
        *,
        backend: str | None = None,
        namespace: str | None = None,
        con: Any | None = None,
    ) -> dict[str, Any] | None:
        db, owns = (con, False) if con is not None else self._connection(readonly=True)
        try:
            row = None
            for key in self._cache_key_candidates(model_name, backend=backend, namespace=namespace):
                row = db.execute(
                    "SELECT score, label FROM nlp_sentiments WHERE hash=? AND model_name=?",
                    (str(hash_value), str(key)),
                ).fetchone()
                if row is not None:
                    break
            if row is None:
                return None
            return {
                "score": float(_row_get(row, "score", 0, 0.0) or 0.0),
                "label": str(_row_get(row, "label", 1, "") or ""),
            }
        finally:
            self._close(db, owns)

    def put_sentiment(
        self,
        hash_value: str,
        model_name: str,
        *,
        score: float,
        label: str,
        backend: str | None = None,
        namespace: str | None = None,
        model_metadata: dict[str, Any] | None = None,
        con: Any | None = None,
    ) -> None:
        cache_key = str(namespace or (embedding_namespace(backend, model_name) if backend else str(model_name)))
        backend_key = str(backend or "legacy")
        metadata_json = self._json_dumps(
            dict(model_metadata or {}, backend=backend_key, model_name=str(model_name), namespace=cache_key)
        )
        db, owns = (con, False) if con is not None else self._connection(readonly=False)
        try:
            db.execute(
                """
                INSERT INTO nlp_sentiments(hash, model_name, backend, model_namespace, score, label, model_metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(hash, model_name) DO UPDATE SET
                  backend=excluded.backend,
                  model_namespace=excluded.model_namespace,
                  score=excluded.score,
                  label=excluded.label,
                  model_metadata_json=excluded.model_metadata_json
                """,
                (str(hash_value), cache_key, backend_key, cache_key, float(score), str(label), metadata_json),
            )
            if owns:
                self._commit(db)
        finally:
            self._close(db, owns)

    def get_or_encode_embeddings(
        self,
        texts: Sequence[str],
        encoder: Any,
        *,
        source: str = "",
        ts: int | Sequence[int] | None = None,
        symbol: str | Sequence[str | None] | None = None,
    ) -> CacheResult:
        rows = [normalize_text(text) for text in list(texts or [])]
        hashes = [text_hash(text) for text in rows]
        backend = str(getattr(encoder, "backend", "legacy") or "legacy")
        model_name = str(getattr(encoder, "model_name", "") or "")
        namespace = encoder_namespace(encoder)
        metadata = dict(getattr(encoder, "model_metadata", {}) or {})
        con, owns = self._connection(readonly=False)
        try:
            self._record_batch_blobs(con, hashes, rows, source=source, ts=ts, symbol=symbol)
            cached: dict[str, np.ndarray] = {}
            misses: list[tuple[str, str]] = []
            for hash_value, text in dict(zip(hashes, rows)).items():
                vector = self.get_embedding(hash_value, model_name, backend=backend, namespace=namespace, con=con)
                if vector is None:
                    misses.append((hash_value, text))
                else:
                    cached[hash_value] = vector
            if misses:
                encoded = np.asarray(encoder.encode([text for _hash, text in misses]), dtype=np.float32)
                if encoded.ndim == 1:
                    encoded = encoded.reshape(len(misses), -1)
                for (hash_value, _text), vector in zip(misses, encoded):
                    arr = np.asarray(vector, dtype=np.float32).reshape(-1)
                    self.put_embedding(
                        hash_value,
                        model_name,
                        arr,
                        backend=backend,
                        namespace=namespace,
                        model_metadata=metadata,
                        con=con,
                    )
                    cached[hash_value] = arr
            values = self._ordered_values(hashes, cached)
            self._commit(con)
            if self.con is None:
                self._emit_cache_metrics("embedding", namespace, len(hashes), len(misses))
            return CacheResult(
                values=values,
                hashes=hashes,
                hits=max(0, len(set(hashes)) - len(misses)),
                misses=len(misses),
                encoded=len(misses),
                summaries=[],
            )
        finally:
            self._close(con, owns)

    def get_or_encode_sentiments(
        self,
        texts: Sequence[str],
        encoder: FinBertSentimentEncoder,
        *,
        source: str = "",
        ts: int | Sequence[int] | None = None,
        symbol: str | Sequence[str | None] | None = None,
    ) -> CacheResult:
        rows = [normalize_text(text) for text in list(texts or [])]
        hashes = [text_hash(text) for text in rows]
        backend = str(getattr(encoder, "backend", "finbert") or "finbert")
        model_name = str(getattr(encoder, "model_name", "") or "")
        namespace = encoder_namespace(encoder)
        metadata = dict(getattr(encoder, "model_metadata", {}) or {})
        con, owns = self._connection(readonly=False)
        try:
            self._record_batch_blobs(con, hashes, rows, source=source, ts=ts, symbol=symbol)
            cached: dict[str, np.ndarray] = {}
            summaries_by_hash: dict[str, dict[str, Any]] = {}
            misses: list[tuple[str, str]] = []
            for hash_value, text in dict(zip(hashes, rows)).items():
                vector = self.get_embedding(hash_value, model_name, backend=backend, namespace=namespace, con=con)
                sentiment = self.get_sentiment(hash_value, model_name, backend=backend, namespace=namespace, con=con)
                if vector is None or sentiment is None:
                    misses.append((hash_value, text))
                    continue
                cached[hash_value] = vector
                summaries_by_hash[hash_value] = dict(sentiment)
            if misses:
                probs = np.asarray(encoder.encode([text for _hash, text in misses]), dtype=np.float32)
                scores = encoder.score_from_probabilities(probs)
                labels = encoder.labels_from_probabilities(probs)
                for idx, ((hash_value, _text), vector) in enumerate(zip(misses, probs)):
                    arr = np.asarray(vector, dtype=np.float32).reshape(-1)
                    score = float(scores[idx])
                    label = str(labels[idx])
                    self.put_embedding(
                        hash_value,
                        model_name,
                        arr,
                        backend=backend,
                        namespace=namespace,
                        model_metadata=metadata,
                        con=con,
                    )
                    self.put_sentiment(
                        hash_value,
                        model_name,
                        score=score,
                        label=label,
                        backend=backend,
                        namespace=namespace,
                        model_metadata=metadata,
                        con=con,
                    )
                    cached[hash_value] = arr
                    summaries_by_hash[hash_value] = {"score": score, "label": label}
            values = self._ordered_values(hashes, cached)
            summaries = [dict(summaries_by_hash.get(hash_value) or {}) for hash_value in hashes]
            self._commit(con)
            if self.con is None:
                self._emit_cache_metrics("sentiment", namespace, len(hashes), len(misses))
            return CacheResult(
                values=values,
                hashes=hashes,
                hits=max(0, len(set(hashes)) - len(misses)),
                misses=len(misses),
                encoded=len(misses),
                summaries=summaries,
            )
        finally:
            self._close(con, owns)

    def _record_batch_blobs(
        self,
        con: Any,
        hashes: list[str],
        texts: list[str],
        *,
        source: str,
        ts: int | Sequence[int] | None,
        symbol: str | Sequence[str | None] | None,
    ) -> None:
        seen: set[str] = set()
        for idx, (hash_value, text) in enumerate(zip(hashes, texts)):
            if hash_value in seen:
                continue
            seen.add(hash_value)
            row_ts = ts[idx] if isinstance(ts, (list, tuple)) else ts
            row_symbol = symbol[idx] if isinstance(symbol, (list, tuple)) else symbol
            self.record_text_blob(hash_value, text, source=source, ts=row_ts, symbol=row_symbol, con=con)

    @staticmethod
    def _ensure_optional_columns(con: Any, table_name: str, columns: Sequence[tuple[str, str]]) -> None:
        for column_name, column_type in list(columns or []):
            try:
                con.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} {column_type}")
                continue
            except Exception:
                pass  # no-op-guard: allow - fallback for engines without ADD COLUMN IF NOT EXISTS.
            try:
                con.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
            except Exception:
                continue

    @staticmethod
    def _cache_key_candidates(
        model_name: str,
        *,
        backend: str | None = None,
        namespace: str | None = None,
    ) -> list[str]:
        raw_model = str(model_name or "").strip()
        keys: list[str] = []
        if namespace:
            keys.append(str(namespace))
        elif backend:
            keys.append(embedding_namespace(str(backend), raw_model))
        if raw_model and raw_model not in keys:
            keys.append(raw_model)
        return keys or ["unknown"]

    @staticmethod
    def _json_dumps(data: dict[str, Any]) -> str:
        return json.dumps(dict(data or {}), separators=(",", ":"), sort_keys=True)

    @staticmethod
    def _ordered_values(hashes: list[str], cached: dict[str, np.ndarray]) -> np.ndarray:
        if not hashes:
            return np.zeros((0, 0), dtype=np.float32)
        values = [np.asarray(cached[hash_value], dtype=np.float32).reshape(-1) for hash_value in hashes]
        if not values:
            return np.zeros((0, 0), dtype=np.float32)
        return np.vstack(values).astype(np.float32)

    def _connection(self, *, readonly: bool) -> tuple[Any, bool]:
        if self.con is not None:
            return self.con, False
        from engine.runtime.storage import connect

        return connect(readonly=readonly), True

    @staticmethod
    def _commit(con: Any) -> None:
        commit = getattr(con, "commit", None)
        if callable(commit):
            commit()

    @staticmethod
    def _close(con: Any, owns: bool) -> None:
        if not owns:
            return
        close = getattr(con, "close", None)
        if callable(close):
            close()

    @staticmethod
    def _emit_cache_metrics(kind: str, model_name: str, total: int, misses: int) -> None:
        if total <= 0:
            return
        hits = max(0, int(total) - int(misses))
        try:
            from engine.runtime.metrics import emit_gauge

            emit_gauge(
                "nlp_cache_hit_rate",
                float(hits / max(1, int(total))),
                component="engine.nlp.cache",
                extra_tags={"kind": str(kind), "model_name": str(model_name)},
            )
        except Exception:
            return


def now_ms() -> int:
    return int(time.time() * 1000)
