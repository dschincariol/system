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

- [poll_prices.py](poll_prices.py)
  Main polling market-data job and one of the most operationally sensitive data paths.
- [price_cache.py](price_cache.py)
  In-memory price snapshot cache shared by feature generation, inference, and regime detection.
- [feature_store.py](feature_store.py)
  Point-in-time feature snapshot builder backed by price history, an in-process runtime cache, optional SQLite persistence, and after-commit Timescale enqueue hooks. The active write mode is exposed through `storage.get_timeseries_storage_snapshot()["market_feature_store"]`.
- [provider_router.py](provider_router.py)
  Selects or routes among providers.
- [provider_registry.py](provider_registry.py)
  Registry of providers and provider capabilities.
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
- Prediction-market alternative data:
  [prediction_market_providers.py](prediction_market_providers.py),
  [prediction_market_storage.py](prediction_market_storage.py),
  [prediction_market_features.py](prediction_market_features.py),
  [forecastex_event_contracts.py](forecastex_event_contracts.py),
  [ibkr_event_contracts.py](ibkr_event_contracts.py),
  [jobs/poll_kalshi_prediction_markets.py](jobs/poll_kalshi_prediction_markets.py),
  [jobs/poll_cme_fedwatch.py](jobs/poll_cme_fedwatch.py),
  [jobs/poll_polymarket_prediction_markets.py](jobs/poll_polymarket_prediction_markets.py), and
  [jobs/poll_forecastex_event_contracts.py](jobs/poll_forecastex_event_contracts.py),
  [jobs/backfill_prediction_market_macro.py](jobs/backfill_prediction_market_macro.py).
  These jobs are read-only, disabled by default in the data-source control
  plane where they are alternative data, and feed shadow-only PIT features.
  Polymarket reads public Gamma/Data/CLOB market data and never handles wallet,
  bridge, position, or order-placement flows.
  ForecastEx reads regulated event-contract CSV files; optional IBKR
  event-contract access is read-only market data behind an explicit conid
  allowlist and does not touch execution or account state.
- Deribit crypto derivatives data:
  [deribit_crypto_derivatives.py](deribit_crypto_derivatives.py) and
  [jobs/poll_deribit_crypto_derivatives.py](jobs/poll_deribit_crypto_derivatives.py).
  This is a public BTC/ETH/SOL derivatives signal source, not a
  prediction-market feed. It ingests instruments, ticker snapshots, optional
  order-book snapshots, futures/perpetual basis, funding, option IV/skew, open
  interest, and volume as shadow-only crypto-volatility and positioning inputs
  over HTTP by default or public JSON-RPC-over-WebSocket when
  `DERIBIT_MODE=websocket`. The job is disabled by default in the data-source
  control plane, has no credential fields, and never calls authenticated Deribit
  trading endpoints.
- Sportsbook and betting-exchange odds research:
  [sportsbook_odds.py](sportsbook_odds.py),
  [jobs/poll_sportsbook_odds.py](jobs/poll_sportsbook_odds.py), and
  [jobs/backfill_sportsbook_odds_event_study.py](jobs/backfill_sportsbook_odds_event_study.py).
  This pipeline is low priority for broad stock/crypto trading and exists only
  for narrow asset mappings, event studies, and probability-calibration
  research. It ingests read-only odds feeds or historical files, removes vig
  before feature use, requires explicit mapping rows, starts disabled in the
  data-source control plane, and never models sportsbook execution, accounts,
  balances, wagers, or betting orders. Promotion research writes a separate
  `sportsbook_odds_promotion_evidence` record only for approved narrow mappings
  and records OOS, net-after-cost, PIT, deconfounding, provider-readiness, and
  production-readiness checks. Universe inventory is exact-symbol only: active
  or watch rows and explicit model-config symbols can qualify when they are in
  the narrow allowlist; wildcard model configs are diagnostic only and do not
  make the feature group promotable.

## Maintenance Guidance

- Keep provider capability metadata accurate.
  Ingestion supervision depends on correctly identifying polling versus streaming behavior.
- Distinguish raw acquisition from normalized tables.
  Polling code should not silently mix transport concerns with schema decisions.
- Treat price freshness and provider health as first-class outputs.
  The runtime uses them for gating and lifecycle transitions.
- Preserve point-in-time semantics for training helpers.
  `feature_store.py`, `universe_pit.py`, and the related backfill jobs are only useful if they can reproduce a historical view without leakage.
- Keep prediction-market providers read-only. Event-contract expectations must
  enter through normalized storage and PIT snapshots, not execution or order
  endpoints.
- Keep crypto derivatives providers read-only until promoted through the same
  out-of-sample, net-after-cost, PIT, deconfounded, and production-readiness
  evidence path as other challenger features.
- Keep sportsbook odds research-only unless a specific narrow asset mapping
  proves incremental out-of-sample, net-after-cost, PIT-safe, deconfounded, and
  production-ready edge through `evaluate_sportsbook_odds_go_gate`. Do not add
  general sports odds to broad-market default feature sets. Approved mappings
  must carry owner, rationale, mapping version, approver, approval timestamp,
  approval reason, and `approved_for_promotion=true` before promotion evidence
  can satisfy the gate.
- To create a future sportsbook GO candidate, operators must first configure
  `sportsbook_odds_research` with a read-only historical file or feed, exact
  approved mappings, and real price coverage. Then run
  `poll_sportsbook_odds` to persist no-vig odds rows and
  `backfill_sportsbook_odds_event_study` with explicit
  `SPORTSBOOK_ODDS_EVENT_STUDY_*` latency/cost/window settings. A missing file,
  missing approved mapping, wildcard-only model universe, or missing persisted
  `sportsbook_odds_promotion_evidence` remains NO-GO.
- Source-specific HTTP failures are not always fatal.
  Document which failures should degrade, retry, or merely log.

## Extending Data Flows

When adding a new source:

1. Add the provider or job implementation under `providers/`, `provider_sessions/`, or `jobs/`.
2. Register the job in [engine/runtime/job_registry.py](../runtime/job_registry.py).
3. Decide whether the source belongs in startup orchestration.
4. Document the source in this README and any relevant runtime docs.
