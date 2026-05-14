# Codex Prompt 08 — FinBERT-Grade NLP for News, Filings, and Transcripts

You are working in a Python systematic trading system that ingests
GDELT, RSS company news, SEC/EDGAR filings, and earnings-call
transcripts. The current NLP layer is **lexical**: keyword counts,
simple positive/negative dictionaries, and aggregate sentiment scores.
That throws away most of the signal in financial text. This prompt
upgrades the NLP layer to a **finance-domain transformer** (FinBERT),
adds **earnings-call-transcript embeddings** with a sentence-transformer
encoder, and emits well-typed features into `feature_registry`.

The new features must be **schema-versioned** so promotions and
retraining know whether a serving model was trained on the old
sentiment column or the new one.

## Goal

1. A `engine/nlp/` subpackage hosting reusable encoders and caches.
2. Three new feature groups in `feature_registry`:
   - `nlp_finbert_news_v1` — per-article and per-symbol-aggregated
     sentiment from FinBERT (`ProsusAI/finbert`).
   - `nlp_filings_v1` — paragraph-level embeddings of SEC filings
     reduced to a fixed-dim symbol-day vector (mean / max pool).
   - `nlp_transcripts_v1` — embeddings of earnings-call transcripts
     plus Q&A-section sentiment.
3. A backfill job that processes historical text once and caches
   results so re-runs are cheap.
4. Strict cost / latency budget: **NLP runs out of the prediction
   path, never inline**. Predictors consume cached features only.

## Files to read first (read-only)

- `engine/data/ingest/company_news_ingest.py`,
  `engine/data/ingest/rss_ingest.py`,
  `engine/data/ingest/gdelt_ingest.py` — text sources.
- Whatever module today computes lexical sentiment (grep for
  `sentiment`, `vader`, `loughran`, `dictionary`); replace its output
  contract but keep its column name during a deprecation window.
- `engine/strategy/feature_registry.py` — feature registration.
- `engine/runtime/storage.py` — schema patterns; you will add caches.
- `engine/runtime/job_registry.py` — register the new NLP jobs.
- The transcripts ingestion pipeline (grep `transcript`).
- `engine/data/ingest/sec_edgar_ingest.py` (or whichever module ingests
  SEC filings) — the source of paragraph text for filings.

## Files to create

- `engine/nlp/__init__.py`
- `engine/nlp/encoder.py` — `Encoder` base class with
  `encode(texts: list[str]) -> np.ndarray`. Implementations:
  `FinBertSentimentEncoder` (3-class softmax → scalar score),
  `SentenceTransformerEncoder` (default `all-MiniLM-L6-v2`, 384-dim)
  for filings and transcripts.
- `engine/nlp/cache.py` — content-hash-keyed cache of embeddings and
  sentiments so the same text is never re-encoded. Backed by a new
  table.
- `engine/nlp/aggregators.py` — symbol-day aggregations (mean,
  recency-weighted mean, max, count).
- `engine/strategy/jobs/embed_news.py`
- `engine/strategy/jobs/embed_filings.py`
- `engine/strategy/jobs/embed_transcripts.py`
- `tests/test_nlp_encoder.py`
- `tests/test_nlp_cache.py`
- `tests/test_nlp_aggregators.py`
- `tests/test_nlp_feature_registration.py`

## Files to modify

- `engine/strategy/feature_registry.py` — register the three new
  feature groups; mark the old lexical group as `deprecated_after =
  '<commit hash>'` but keep it serving for the deprecation window.
- `engine/runtime/job_registry.py` — register the three NLP jobs.
- `engine/runtime/storage.py` — add tables:
  - `nlp_text_blobs(hash TEXT PK, source TEXT, ts INTEGER,
    symbol TEXT NULL, text TEXT)`
  - `nlp_embeddings(hash TEXT, model_name TEXT, dim INTEGER,
    vector BLOB, PRIMARY KEY(hash, model_name))`
  - `nlp_sentiments(hash TEXT, model_name TEXT, score REAL,
    label TEXT, PRIMARY KEY(hash, model_name))`

## Implementation plan

1. **Encoder layer.** Wrap `transformers` and `sentence-transformers`
   behind one interface. Models load lazily; first call downloads
   weights to `models/nlp/`. Subsequent calls reuse.
2. **Caching.** Every text input is normalized (whitespace, NFC
   unicode), hashed (SHA-1), and looked up before encoding. Cache
   hit-rate is logged to the same metrics path other jobs use.
3. **Batching.** Encoder calls are batched (FinBERT batch_size 32,
   SentenceTransformer 64) to keep CPU usage reasonable. GPU is
   detected and used when present.
4. **Aggregation.** `aggregators.py` produces per-symbol-day rows
   from per-document outputs. Recency-weighted mean uses half-life
   36 hours (configurable).
5. **Backfill.** Each NLP job iterates the corresponding text table
   in date-batches; idempotent via cache.
6. **Feature registration.** New groups have explicit version suffixes
   (`_v1`). Old lexical group remains until end of deprecation window.
7. **Latency hygiene.** No NLP code runs inside `predictor.predict`;
   predictors read pre-computed feature rows. A test asserts no
   `transformers` import is reachable from `predictor.py`.
8. **Fail-open ingestion.** If FinBERT fails (download error, OOM),
   the news/filings ingestion job logs and proceeds; the NLP table
   simply lacks rows for that batch — it does not block the rest of
   the system.

## Acceptance criteria

- [ ] FinBERT encoder produces a 3-vector `(positive, negative,
      neutral)` summing to 1.0 ± 1e-5.
- [ ] Sentiment score is computed as `pos - neg`, range [-1, 1], and
      matches the model card's published examples in a regression test.
- [ ] Cache hit on second call returns identical bytes; encoder is not
      invoked.
- [ ] Aggregator produces no row when no documents exist for a
      `(symbol, day)` pair (no zero-stuffing into the registry).
- [ ] New feature groups appear in `feature_registry.list_groups()`;
      old lexical group still appears with `deprecated_after` set.
- [ ] No new code path inside `predictor.py` imports a transformer.
- [ ] Jobs are idempotent: a second run within 60 seconds inserts
      zero rows into `nlp_embeddings` / `nlp_sentiments`.
- [ ] Ingestion does not stop on encoder failure (fail-open verified
      with a forced-error fixture).

## Test plan

- `tests/test_nlp_encoder.py` — small text fixture; FinBERT yields
  the expected sign on canonical positive / negative sentences;
  SentenceTransformer yields stable-norm vectors.
- `tests/test_nlp_cache.py` — round-trip; hit / miss accounting.
- `tests/test_nlp_aggregators.py` — recency-weighted mean math;
  empty-input behavior.
- `tests/test_nlp_feature_registration.py` — registry contains the
  new groups and the deprecation marker on the old one.

Run: `pytest -q tests/test_nlp_encoder.py tests/test_nlp_cache.py
tests/test_nlp_aggregators.py tests/test_nlp_feature_registration.py`

## Out of scope

- Do not call any external paid API. FinBERT and sentence-transformers
  are local.
- Do not add a vector database (FAISS, Qdrant, pgvector). Cache is a
  simple keyed table; downstream similarity search is a future prompt.
- Do not change the existing lexical sentiment column's serving path.
  The deprecation window is intentional so models trained against it
  continue to work.
- Do not run NLP inline in any low-latency code path.
- Do not make live trading depend on an LLM response.
