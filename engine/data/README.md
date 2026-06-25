# Data Subsystem

The `engine/data/` tree owns external data acquisition and transformation into DB-ready facts.

## Directory Map

- `jobs/`
  One-shot data jobs for news, filings, earnings, ingestion, labeling prep, and specialized data tasks.
- `ingest/`
  Low-level ingest helpers such as RSS and GDELT ingestion.
- `providers/`
  Provider abstractions and concrete provider implementations.
- `provider_sessions/`
  Long-lived session management for providers like Polygon WebSocket and IBKR.
- `live_prices/`
  Live market data adapters. CCXT live-price polling keeps one cached exchange
  instance per exchange id/config/sandbox fingerprint so CCXT's session, market
  metadata, credentials, timeout/options, and rate limiter are reused across
  matching cycles; venues with native `fetchTickers` support are queried in
  batches before falling back to per-symbol `fetchTicker`. Runtime counters
  record batch attempts/successes/failures, batched markets/rows, unsupported
  batches, missing symbols, invalid batch rows, failed requested symbols,
  fallback fetches/rows/successes/failures, cache hits/misses/evictions, market
  cache hits/reloads/invalidations, batch/fallback latency, and the selected
  fetch path for each CCXT price cycle.
  Polygon REST live-price polling uses the full-market snapshot endpoint with a
  comma-separated `tickers` filter in bounded chunks, so one provider request can
  serve many symbols without probing last-trade/NBBO entitlements per symbol.
  Batch 401/403 entitlement failures, 429 rate limits, and transient HTTP
  failures are logged as batch-level classifications and do not fan out into
  per-symbol fallback. Only an unsupported batch endpoint response, such as
  404/405, enables the legacy per-symbol fallback path; that fallback now uses
  the single-ticker snapshot endpoint first and only then previous close data.
  Polygon WebSocket payloads, control frames, and REST snapshot/replay responses
  use `engine.runtime.json_codec`, which prefers `orjson` from the runtime
  dependency set and keeps an explicit stdlib fallback for development or
  partial installs. Hot-path tests assert bytes/string decode, invalid-payload
  logging, numeric normalization, and codec use at the WebSocket and REST
  call sites.
  OANDA FX polling lives in `live_prices/oanda_live.py` and is read-only:
  it calls OANDA v20 account pricing, returns the standard polling row shape,
  and degrades to an empty snapshot when `OANDA_ACCESS_TOKEN`/`OANDA_API_KEY`
  or `OANDA_ACCOUNT_ID` is missing. It does not import or expose order,
  cancel, replace, trade, account-mutation, or flatten endpoints.
- `prices/`
  Price utilities, returns, and volatility helpers.
- `options/`
  Options data integrations.
- `sec/`
  SEC ingestion helpers.
- `calendar/`
  Calendar-style sources such as earnings schedules.

## High-Value Files

- [poll_prices.py](poll_prices.py)
  Main polling market-data job and one of the most operationally sensitive data paths. It collects independent REST provider snapshots with bounded parallelism, keeps provider ordering deterministic for merge arbitration, consumes async price-persistence backpressure status from `price_router`, preloads recent price history for every merged cycle symbol with one batch query before outlier and split-like rejection, routes high-volume auxiliary non-price telemetry such as ingest slippage through the durable telemetry append buffer, records provider/raw enqueue pressure, and keeps a cycle backoff when the durable writers are saturated. Heartbeat, status, and transient slowdown paths do not close runtime pooled Postgres connections; pool cleanup is left to process shutdown/fatal lifecycle handling so polling cycles keep warm connections instead of rebuilding them.
- [options_poll.py](options_poll.py)
  Options-chain polling job. It bulk-loads per-symbol retry state once per cycle,
  fetches providers with bounded HTTP concurrency, buffers high-volume
  `options_chain`/`options_chain_v2` rows, snapshot `events`, and
  `options_symbol_ingestion_state` rows, and commits progress at 25-50 symbol
  batch boundaries while preserving provider failover and entitlement handling.
  Each flush is first written to the bounded SQLite WAL options durable buffer;
  a full or unavailable spool rejects the flush before the DB write starts, and
  old spool rows replay on the next cycle before newly fetched symbols are
  processed.
- [price_cache.py](price_cache.py)
  Price snapshot cache shared by feature generation, inference, and regime detection. Ingestion-cycle
  multi-symbol updates batch existing snapshot reads and freshness-protected writes through the runtime
  live-cache backend instead of issuing sequential per-symbol Redis GET/SET operations.
- [feature_store.py](feature_store.py)
  Point-in-time feature snapshot builder backed by price history, an in-process runtime cache, optional SQLite persistence, and after-commit Timescale enqueue hooks. The active write mode is exposed through `storage.get_timeseries_storage_snapshot()["market_feature_store"]`.
- [provider_router.py](provider_router.py)
  Selects or routes among providers.
- [provider_registry.py](provider_registry.py)
  Registry of providers and provider capabilities. OANDA is registered as a
  default-off polling `live_price_provider` for `asset_classes=["fx"]`; IBKR's
  streaming definition advertises both equities and FX because IBKR supports
  FX cash/IDEALPRO market data.
- [finbert_sentiment.py](finbert_sentiment.py)
  FinBERT-powered sentiment enrichment used to turn news and transcript text into train/serve-safe sentiment features.
- [congressional_trades.py](congressional_trades.py)
  Congressional disclosure normalization, symbol resolution, and fetch helpers.
- [sec/form4_live.py](sec/form4_live.py)
  Form 4 parsing and transaction normalization for insider-trading ingestion.
- [universe_pit.py](universe_pit.py)
  Point-in-time symbol-universe reconstruction used by retraining, validation, and replay-safe backfills.
- [jobs/gdelt_poll.py](jobs/gdelt_poll.py)
  Structured news polling job.
- [jobs/sec_poll.py](jobs/sec_poll.py)
  SEC filing polling job.
- [jobs/earnings_poll.py](jobs/earnings_poll.py)
  Earnings calendar polling job.
- [jobs/ingest_now.py](jobs/ingest_now.py)
  Consolidates ingested source data into the `events` plane.
- [news_flow.py](news_flow.py)
  Builds backend-aware news story embeddings and point-in-time novelty/staleness
  features. Each processing cycle fetches pending news once, prefetches all
  comparable recent embeddings for the batch in one query, carries same-cycle
  embeddings in memory so duplicate detection remains idempotent before commit,
  and writes `news_story_embeddings` plus `news_event_features` through batched
  upserts. Runtime gauges expose `news_flow_batch_size`,
  `news_flow_recent_embedding_queries`, `news_flow_write_batches`, and
  `news_flow_embedding_db_round_trips` for the core embedding read/write path.
- [jobs/process_events.py](jobs/process_events.py)
  Event processing stage before labels and predictions.
- [jobs/label_due_events.py](jobs/label_due_events.py)
  Converts due events into training labels once enough horizon data exists.

## Newer Feature Families

- Alternative disclosure ingestion:
  [jobs/ingest_form4.py](jobs/ingest_form4.py),
  [jobs/ingest_congressional_trades.py](jobs/ingest_congressional_trades.py),
  [sec/form4_live.py](sec/form4_live.py), and
  [congressional_trades.py](congressional_trades.py).
- Text and sentiment enrichment:
  [finbert_sentiment.py](finbert_sentiment.py),
  [jobs/process_finbert_sentiment.py](jobs/process_finbert_sentiment.py), and
  [ingest/news_enrichment.py](ingest/news_enrichment.py).
- Point-in-time training support:
  [feature_store.py](feature_store.py),
  [universe_pit.py](universe_pit.py), and
  [jobs/backfill_universe_pit.py](jobs/backfill_universe_pit.py).
- Time-series feature backfills:
  [jobs/compute_tsfresh_snapshots.py](jobs/compute_tsfresh_snapshots.py) and
  [jobs/snapshot_model_features.py](jobs/snapshot_model_features.py).

## Maintenance Guidance

- Keep provider capability metadata accurate.
  Ingestion supervision depends on correctly identifying polling versus streaming behavior.
- Distinguish raw acquisition from normalized tables.
  Polling code should not silently mix transport concerns with schema decisions.
- Treat price freshness and provider health as first-class outputs.
  The runtime uses them for gating and lifecycle transitions.
- Preserve point-in-time semantics for training helpers.
  `feature_store.py`, `universe_pit.py`, and the related backfill jobs are only useful if they can reproduce a historical view without leakage.
- Source-specific HTTP failures are not always fatal.
  Document which failures should degrade, retry, or merely log.
- Keep provider rate-limit state in production adapters, not only in tests.
  CCXT exchange objects are long-lived and are evicted only for stale or fatal
  exchange errors. Bad-symbol errors invalidate cached markets so the next cycle
  reloads metadata without discarding the exchange rate limiter; per-symbol
  fallback failures are telemetry-bounded, and `ccxt_live_*` runtime counters
  must continue to distinguish exchange-cache, market-cache, batch,
  missing-symbol, invalid-row, failed-symbol, unsupported, and fallback usage.
- Keep yfinance live prices on the batched path when the package is available.
  `yf.download` processes the selected universe in chunks with one module-level
  HTTP session whose adapter pool is sized at least to
  `YFINANCE_LIVE_MAX_WORKERS`, logs partial batch misses, and reserves v8 chart
  requests for bounded `1d` fallback only. Batch timeout wrappers and chart
  fallback work share one bounded module-level executor across cycles. If the
  adapter must run on the fallback path and `YFINANCE_LIVE_FALLBACK_SYMBOL_LIMIT`
  caps the selected symbols, the warning includes the exact skipped symbol list
  and runtime metrics mark the cycle degraded with
  `configured_fallback_symbol_limit`; tests and shutdown paths should use
  `shutdown_yfinance_resources()` or
  `reset_yfinance_resources_for_tests()` rather than discarding module globals.
- Keep hot non-price ingestion on durable bounded writers. `poll_prices.py`
  auxiliary provider telemetry uses `telemetry_append_buffer.py`, and
  `options_poll.py` uses the options durable buffer for option-chain,
  snapshot-event, and symbol-state rows. Lower-frequency batch jobs are
  documented in [INGESTION_NON_PRICE_BUFFER_AUDIT.md](../../docs/INGESTION_NON_PRICE_BUFFER_AUDIT.md)
  and should remain visibly failing/rerunnable if a DB outage occurs.
- Keep polling loops out of pool teardown. `poll_prices.py` treats lock waits,
  statement timeouts, and pool-acquisition timeouts as cycle backpressure, logs
  the relevant busy event, and lets the next polling sleep/backoff absorb the
  slowdown instead of calling runtime pooled-connection cleanup from the hot
  path.

## Extending Data Flows

When adding a new source:

1. Add the provider or job implementation under `providers/`, `provider_sessions/`, or `jobs/`.
2. Register the job in [engine/runtime/job_registry.py](../runtime/job_registry.py).
3. Decide whether the source belongs in startup orchestration.
4. Document the source in this README and any relevant runtime docs.

FX-01 adds a first-pass FX data substrate without creating a new instrument
schema. `default_symbols.py` can seed the seven major FX pairs behind
`FX_PAIRS_ENABLED` or `OANDA_FX_PAIRS`, storing canonical six-letter symbols
such as `EURUSD` while deriving OANDA instruments such as `EUR_USD` through the
shared helper. `factor_ingestion.py` adds raw FRED rows for real yields, broad
USD, ECB policy rates, UK SONIA, and Japan call-money/interbank rates; per-pair
rate differentials, carry, DXY transforms, cross-pair correlations, and trend
features belong to the later feature-loader workstream. `cftc_cot.py` owns the
major FX futures COT contract specs (`6E`, `6B`, `6J`, `6S`, `6C`, `6A`, `6N`)
and keeps the COT daemon default-off/control-plane gated.
