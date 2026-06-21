# NLP Subsystem

The `engine/nlp/` package owns the offline text-feature primitives that turn financial documents (news, filings, transcripts) into sentiment scores and embeddings. It provides lazily loaded local transformer encoders, a content-hash cache that avoids recomputing on unchanged text, and recency-weighted symbol-day aggregation. It is consumed by the offline embedding jobs in `engine/strategy/jobs/` (`embed_news.py`, `embed_filings.py`, `embed_transcripts.py`) and by `engine/data/news_flow.py`, which feed the NLP/FinBERT/news, filings, and transcript feature groups in the feature registry.

## Files

- [encoder.py](encoder.py)
  Lazy local transformer encoders. `FinBertSentimentEncoder` wraps `ProsusAI/finbert` (via `transformers.AutoModelForSequenceClassification`) and returns per-document probabilities in fixed `(positive, negative, neutral)` order, with a scalar sentiment score of `positive - negative`. `SentenceTransformerEncoder` wraps a `sentence-transformers` model (default `all-MiniLM-L6-v2`) for embeddings. Both load weights only on first use, are context managers that release model and accelerator memory on exit, and resolve their torch device through `engine.runtime.hardware.resolve_torch_device`.
- [cache.py](cache.py)
  `NlpCache`, a content-hash keyed cache for text blobs, embeddings, and sentiments. Text is NFC-normalized and whitespace-collapsed, then SHA-1 hashed; `get_or_encode_embeddings` / `get_or_encode_sentiments` return cached rows and encode only the misses. Persistence goes through the shared `engine.runtime.storage` facade, so it is Postgres-backed in production (SQLite only under the test backend). Emits the `nlp_cache_hit_rate` gauge when it owns its connection.
- [aggregators.py](aggregators.py)
  `aggregate_symbol_day_documents` collapses per-document scores and embeddings into one row per symbol and UTC day. It applies exponential recency weighting anchored to the most recent timestamp in each bucket (default half-life 36 hours) and emits mean, recency-weighted-mean, and max for both the scalar score and the embedding vector.

## Key Tables / Outputs

`NlpCache.ensure_schema()` creates three tables, all keyed by the content hash:

- `nlp_text_blobs` — `hash` primary key, plus `source`, `ts`, `symbol`, and the normalized `text`.
- `nlp_embeddings` — `(hash, model_name)` primary key, with `dim` and the float32 `vector` stored as bytes.
- `nlp_sentiments` — `(hash, model_name)` primary key, with `score` and `label`.

Because embeddings and sentiments are keyed by `(hash, model_name)`, switching the encoder model id naturally produces a fresh cache namespace rather than colliding with prior outputs.

## Contract

These encoders are offline batch primitives: model intent and live ordering are owned elsewhere. The package produces deterministic text features for the registry and does not place trades, gate execution, or hold order authority.
