# NLP Subsystem

The `engine/nlp/` package owns the offline text-feature primitives that turn financial documents (news, filings, transcripts) into sentiment scores and embeddings. It provides lazily loaded local transformer encoders, a content-hash cache that avoids recomputing on unchanged text, and recency-weighted symbol-day aggregation. It is consumed by the offline embedding jobs in `engine/strategy/jobs/` (`embed_news.py`, `embed_filings.py`, `embed_transcripts.py`) and by `engine/data/news_flow.py`, which feed the NLP/FinBERT/news, filings, and transcript feature groups in the feature registry.

## Files

- [encoder.py](encoder.py)
  Lazy backend-aware encoders. `FinBertSentimentEncoder` remains the conservative finance sentiment fallback and returns per-document probabilities in fixed `(positive, negative, neutral)` order. `SentenceTransformerEncoder` can serve the legacy `all-MiniLM-L6-v2` model or operator-selected finance-domain sentence-transformer models such as the Fin-E5 candidate. `OpenAIEmbeddingEncoder` and `HashingEmbeddingEncoder` preserve API and deterministic fallback behavior. Backends are selected through `NEWS_EMBED_BACKEND` / `NLP_EMBED_BACKEND` and sentiment through `NLP_SENTIMENT_MODEL_NAME`; all optional packages import lazily.
- [cache.py](cache.py)
  `NlpCache`, a content-hash keyed cache for text blobs, embeddings, and sentiments. Text is NFC-normalized and whitespace-collapsed, then SHA-1 hashed; `get_or_encode_embeddings` / `get_or_encode_sentiments` return cached rows and encode only the misses. New writes use a backend/model namespace in `model_name` and persist `backend`, `model_namespace`, and `model_metadata_json`; legacy raw model-name rows are still readable. Persistence goes through the shared `engine.runtime.storage` facade, so it is Postgres-backed in production (SQLite only under the test backend). Emits the `nlp_cache_hit_rate` gauge when it owns its connection.
- [aggregators.py](aggregators.py)
  `aggregate_symbol_day_documents` collapses per-document scores and embeddings into one row per symbol and UTC day. It applies exponential recency weighting anchored to the most recent timestamp in each bucket (default half-life 36 hours) and emits mean, recency-weighted-mean, and max for both the scalar score and the embedding vector.
- [benchmark.py](benchmark.py)
  Local benchmark harness for cached news, filings, and transcripts. It reports retrieval relevance, duplicate/staleness classification, entity/event clustering, and downstream IC/OOS contribution, plus a decision evidence block that says whether the sample is sufficient to choose a backend. Run it with `python tools/benchmark_financial_text_embeddings.py --backend hashing --limit 500` or substitute a reviewed local finance model.

## Key Tables / Outputs

`NlpCache.ensure_schema()` creates three tables, all keyed by the content hash plus the backend/model namespace:

- `nlp_text_blobs` — `hash` primary key, plus `source`, `ts`, `symbol`, and the normalized `text`.
- `nlp_embeddings` — `(hash, model_name)` primary key where `model_name` is the namespace (for example `sentence_transformer:all-MiniLM-L6-v2`), with `backend`, `model_namespace`, model-card/license metadata, `dim`, and the float32 `vector` stored as bytes.
- `nlp_sentiments` — `(hash, model_name)` primary key with the same namespace metadata plus `score` and `label`.

Because embeddings and sentiments are keyed by a backend/model namespace, switching backend or model naturally produces a fresh cache namespace rather than colliding with prior outputs. `engine/data/news_flow.py` also persists `embedding_backend`, `model_name`, and `availability_ts_ms`; novelty comparisons and feature resolution filter on all three fields and refuse explicit mixed-space comparisons.

## Model Review

Finance-domain candidates are not silently promoted. The shipped candidate metadata for `FinanceMTEB/FinE5` records the Hugging Face model-card URL, marks the license as requiring operator review, and requires `NLP_FINANCIAL_EMBED_LICENSE_REVIEW_ACK=1` before the finance sentence-transformer backend will load. Text embeddings remain feature inputs only; they do not place trades and remain behind the existing feature/model promotion gates.

## Contract

These encoders are offline batch primitives: model intent and live ordering are owned elsewhere. The package produces deterministic text features for the registry and does not place trades, gate execution, or hold order authority.
