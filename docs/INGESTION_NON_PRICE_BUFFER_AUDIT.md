# Non-Price Ingestion Durable Buffer Audit

H14 reviewed non-price ingestion writers under `engine/data` and `engine/jobs`
for database-slowdown behavior. High-volume hot paths must use a bounded durable
buffer before returning success to producers; lower-frequency batch jobs may
remain synchronous when their source is replayable and failures are surfaced.

## Current Durable Coverage

| Path | Tables / surfaces | Classification | H14 action |
| --- | --- | --- | --- |
| `engine/data/poll_prices.py` auxiliary telemetry | `price_provider_health`, `weather_provider_health`, `ingestion_pipeline_health`, `ingest_slippage`, `price_quotes_raw` | Already durable-buffered hot path | Uses `engine/runtime/telemetry_append_buffer.py` with `SQLiteNonPriceIngestionSpool`; enqueue persists before return, bounded row/byte caps reject under pressure, selected rows are deleted only after the table-specific DB commit. |
| `engine/data/options_poll.py` | `options_chain`, `options_chain_v2`, `options_symbol_ingestion_state`, per-symbol `events` rows with `event_type='options_snapshot'` | Converted hot path | `OptionsWriteBuffer` now stages option-chain rows, symbol-state rows, and snapshot-event rows together, enqueues them to `options_poll_durable_buffer.sqlite` before the DB transaction, replays old spool rows on cycle start, and deletes the selected spool batch only after commit. Existing table schemas and idempotent `ON CONFLICT` keys are preserved. |
| `engine/jobs/stream_prices_polygon_ws.py` | Price events plus provider health surfaced through runtime writers | Not an H14 non-price table writer | Price persistence is owned by the async price writer. Provider-health style auxiliary rows are covered by the telemetry append buffer. |

## Remaining Synchronous Paths

| Path | Tables / outputs | Classification | Rationale |
| --- | --- | --- | --- |
| `engine/data/jobs/ingest_options.py` | Options backfill / one-shot options ingest | Batch-low-risk | Operator-triggered batch job, not the continuous hot poller. Failures surface to the job; provider data can be rerun. Continuous `options_poll.py` is now the durable hot path. |
| `engine/data/jobs/gdelt_poll.py`, `engine/data/ingest/gdelt_ingest.py`, `engine/data/gdelt_macro.py` | GDELT/news/macro event rows | Batch-low-risk | Poll/backfill cadence is low relative to price/options hot loops. Source queries are replayable and job failures are visible. |
| `engine/data/jobs/sec_poll.py`, `engine/data/sec/edgar_live.py` | SEC filings and derived document events | Batch-low-risk | Filing polling is bounded by provider cadence and can be replayed by accession/time window. |
| `engine/data/jobs/earnings_poll.py`, `engine/data/calendar/fmp_earnings.py` | Earnings calendar events | Batch-low-risk | Low-frequency calendar data with replayable provider windows. |
| `engine/data/jobs/process_events.py`, `process_events_live.py`, `process_events_shadow.py`, `process_events_enriched.py` | Event enrichment, embeddings, downstream event state | Batch-low-risk | CPU/batch processing over persisted inputs. DB failures fail the job rather than silently acknowledging upstream acquisition. |
| `engine/data/jobs/process_news_flow.py`, `engine/data/news_flow.py`, `engine/data/ingest/news_enrichment.py`, `engine/data/jobs/backfill_news_features.py` | News flow, sentiment, and feature enrichment | Batch-low-risk | Recomputable from stored news/events; not a hot external acquisition loop. |
| `engine/data/jobs/process_finbert_sentiment.py`, `engine/data/finbert_sentiment.py` | Sentiment enrichment | Batch-low-risk | Recomputable batch enrichment; failures are visible and do not consume unbounded provider streams. |
| `engine/data/jobs/poll_social_reddit.py`, `poll_social_stocktwits.py` | Social signals/events | Batch-low-risk | Poll cadence and input windows are bounded; job failures are visible and replayable by time window. |
| `engine/data/jobs/poll_macro.py`, `backfill_macro_vintages.py` | Macro/vintage rows | Batch-low-risk | Low-frequency source with explicit backfill job. |
| `engine/data/jobs/poll_weather_forecasts.py`, `poll_weather_alerts.py`, `compute_weather_ingest.py`, `compute_weather_alerts_ingest.py`, `compute_weather_promotion_guard.py`, `engine/data/weather_*` | Weather forecasts, alerts, derived weather features | Batch-low-risk | Forecast/alert cycles are bounded and replayable; provider-health telemetry is covered by the telemetry append buffer. |
| `engine/data/jobs/ingest_form4.py`, `engine/data/sec/form4_live.py` | Insider/Form 4 events | Batch-low-risk | Filing-based, replayable by accession/time window. |
| `engine/data/jobs/ingest_congressional_trades.py`, `engine/data/congressional_trades.py`, `engine/data/jobs/ingest_quiver_gov.py`, `engine/data/quiver_gov.py` | Congressional/Quiver disclosures | Batch-low-risk | Disclosure ingestion is bounded and replayable. |
| `engine/data/jobs/ingest_13f.py`, `engine/data/inst_13f.py` | 13F holdings | Batch-low-risk | Periodic filings with replayable windows. |
| `engine/data/jobs/ingest_fundamentals_pit.py`, `engine/data/fundamentals_pit.py` | Point-in-time fundamentals | Batch-low-risk | Batch ingestion with replayable source snapshots. |
| `engine/data/jobs/ingest_cftc_cot.py`, `engine/data/cftc_cot.py` | CFTC COT data | Batch-low-risk | Weekly/batch source; synchronous failure is visible and rerunnable. |
| `engine/data/jobs/ingest_etf_flows.py`, `engine/data/etf_flows.py` | ETF flow rows | Batch-low-risk | Batch source with bounded row counts and rerunnable windows. |
| `engine/data/jobs/ingest_finra_short_interest.py`, `ingest_finra_short_volume.py`, `engine/data/finra_short.py` | FINRA short interest/volume | Batch-low-risk | Batch source with rerunnable dates. |
| `engine/data/jobs/ingest_crypto_funding.py`, `engine/data/crypto_positioning.py` | Crypto funding/positioning | Batch-low-risk | Poll/backfill cadence is bounded; failures are surfaced to the job. |
| `engine/data/jobs/update_universe.py`, `backfill_universe_pit.py`, `engine/data/universe.py`, `engine/data/universe_pit.py`, `engine/data/universe_discovery.py` | Universe membership and point-in-time universe rows | Batch-low-risk | Recomputable and bounded by symbol universe size. |
| `engine/data/jobs/compute_tsfresh_snapshots.py`, `snapshot_model_features.py`, `backfill_features.py`, `engine/data/feature_store.py` | Derived model/market features | Batch-low-risk / existing bounded sidecar where eligible | Derived from persisted price/event inputs. `feature_store.py` exposes its active mode through storage diagnostics and can enqueue eligible time-series rows through the Timescale sidecar. |
| `engine/data/jobs/label_due_events.py`, `backfill_labels_price_from_prices.py` | Training labels | Batch-low-risk | Derived batch writes from persisted events/prices; failures are visible and rerunnable. |
| `engine/data/jobs/calibrate_price_confidence.py`, `compute_drift.py` | Calibration/drift artifacts | Batch-low-risk | Offline computation with explicit job status. |
| `engine/data/live_prices/*`, `engine/data/prices/*`, `engine/data/price_cache.py`, `engine/data/price_hygiene.py` | Live or historical price data | Not suitable for H14 non-price conversion | Price data is covered by async price writer, Timescale sidecar, or price-specific storage contracts rather than the non-price durable spool. |

## Production Enforcement

- `options_poll.py` constructs a `SQLiteNonPriceIngestionSpool` with
  `OPTIONS_POLL_DURABLE_BUFFER_MAX_ROWS` and
  `OPTIONS_POLL_DURABLE_BUFFER_MAX_BYTES`. A full or unavailable spool raises
  `NonPriceIngestionSpoolFullError`/`NonPriceIngestionSpoolUnavailableError`
  before the DB write is attempted, increments rejected-row and backpressure
  counters, and leaves in-process rows intact for a visible failed run.
- The options spool row contains tagged table rows for `options_chain_v2`,
  `options_chain`, `events`, and `options_symbol_ingestion_state`. Replay
  decodes by tag and writes through the same table-specific upsert helpers used
  by normal flushes.
- `OptionsWriteBuffer.replay_spooled()` runs at the start of each
  `options_poll` cycle. It selects spool rows without deleting them, writes and
  commits the target DB transaction, then deletes the selected spool ids only
  after the commit returns.
- Backpressure and loss visibility are production fields, not only tests:
  `durable_buffer_pending_rows`, `durable_buffer_pending_bytes`,
  `durable_buffer_oldest_age_ms`, fill ratios, spooled/replayed/deleted rows,
  rejected rows, dropped rows, enqueue/replay/delete failures, corruption
  counters, and backpressure active/events/recoveries are emitted in
  `options.poll.durable_buffer.*` runtime metrics and included in the
  `options_poll` write-buffer metadata recorded by ingestion pipeline health.
