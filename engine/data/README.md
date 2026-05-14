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
  Live market data adapters.
- `prices/`
  Price utilities, returns, and volatility helpers.
- `options/`
  Options data integrations.
- `sec/`
  SEC ingestion helpers.
- `calendar/`
  Calendar-style sources such as earnings schedules.

## High-Value Files

- [poll_prices.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\data\poll_prices.py)
  Main polling market-data job and one of the most operationally sensitive data paths.
- [price_cache.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\data\price_cache.py)
  In-memory price snapshot cache shared by feature generation, inference, and regime detection.
- [feature_store.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\data\feature_store.py)
  Point-in-time feature snapshot builder backed by price history, an in-process runtime cache, optional SQLite persistence, and after-commit Timescale enqueue hooks. The active write mode is exposed through `storage.get_timeseries_storage_snapshot()["market_feature_store"]`.
- [provider_router.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\data\provider_router.py)
  Selects or routes among providers.
- [provider_registry.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\data\provider_registry.py)
  Registry of providers and provider capabilities.
- [finbert_sentiment.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\data\finbert_sentiment.py)
  FinBERT-powered sentiment enrichment used to turn news and transcript text into train/serve-safe sentiment features.
- [congressional_trades.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\data\congressional_trades.py)
  Congressional disclosure normalization, symbol resolution, and fetch helpers.
- [sec/form4_live.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\data\sec\form4_live.py)
  Form 4 parsing and transaction normalization for insider-trading ingestion.
- [universe_pit.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\data\universe_pit.py)
  Point-in-time symbol-universe reconstruction used by retraining, validation, and replay-safe backfills.
- [jobs/gdelt_poll.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\data\jobs\gdelt_poll.py)
  Structured news polling job.
- [jobs/sec_poll.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\data\jobs\sec_poll.py)
  SEC filing polling job.
- [jobs/earnings_poll.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\data\jobs\earnings_poll.py)
  Earnings calendar polling job.
- [jobs/ingest_now.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\data\jobs\ingest_now.py)
  Consolidates ingested source data into the `events` plane.
- [jobs/process_events.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\data\jobs\process_events.py)
  Event processing stage before labels and predictions.
- [jobs/label_due_events.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\data\jobs\label_due_events.py)
  Converts due events into training labels once enough horizon data exists.

## Newer Feature Families

- Alternative disclosure ingestion:
  [jobs/ingest_form4.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\data\jobs\ingest_form4.py),
  [jobs/ingest_congressional_trades.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\data\jobs\ingest_congressional_trades.py),
  [sec/form4_live.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\data\sec\form4_live.py), and
  [congressional_trades.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\data\congressional_trades.py).
- Text and sentiment enrichment:
  [finbert_sentiment.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\data\finbert_sentiment.py),
  [jobs/process_finbert_sentiment.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\data\jobs\process_finbert_sentiment.py), and
  [ingest/news_enrichment.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\data\ingest\news_enrichment.py).
- Point-in-time training support:
  [feature_store.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\data\feature_store.py),
  [universe_pit.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\data\universe_pit.py), and
  [jobs/backfill_universe_pit.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\data\jobs\backfill_universe_pit.py).
- Time-series feature backfills:
  [jobs/compute_tsfresh_snapshots.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\data\jobs\compute_tsfresh_snapshots.py) and
  [jobs/snapshot_model_features.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\data\jobs\snapshot_model_features.py).

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

## Extending Data Flows

When adding a new source:

1. Add the provider or job implementation under `providers/`, `provider_sessions/`, or `jobs/`.
2. Register the job in [engine/runtime/job_registry.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\job_registry.py).
3. Decide whether the source belongs in startup orchestration.
4. Document the source in this README and any relevant runtime docs.
