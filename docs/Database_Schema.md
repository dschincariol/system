# Database Schema

Last verified against code: 2026-06-21

This document records the production Postgres 16 + TimescaleDB 2.x schema classification. `engine/runtime/schema/table_classification.py` is the importable source of truth; this document is the human review record. New tables must be added there and here before shipping.

This document is the authoritative, complete table register, kept in lockstep with `engine/runtime/schema/table_classification.py`. The curated, human-readable data-flow view is [README_DATABASE_MAP.md](README_DATABASE_MAP.md), which is a subset and does not replace this register.

The register here documents more than raw base tables: it also includes continuous-aggregate views (see [Continuous Aggregates](#continuous-aggregates)) and segment-level / column-level notes (segment keys, time columns, and index plans). Its entry count is therefore expected to be larger than a raw `pg_class` table count. It reconciles with `engine/runtime/schema/table_classification.py`, which classifies every entry as either a `Hypertable` (with its `segmentby` segment keys, time column, and chunk/compression/retention) or a `Regular` table; continuous aggregates/views are documented here and in migration `0004_continuous_aggregates.py` rather than in that dict.

## Migration Scope

- `0002_hypertables.py` enables TimescaleDB, converts append-mostly tables with a real time column into hypertables, configures integer-time `now()` support for epoch-ms tables, and installs compression and retention policies.
- `0003_indexes.py` creates BRIN indexes on hypertable time columns, `(symbol, time DESC)` indexes where a `symbol` column exists, segment/time indexes for non-symbol segment keys, JSONB GIN indexes, and targeted expression indexes for decision and audit predicates.
- `0004_continuous_aggregates.py` creates dashboard rollups: `cagg_prices_5m`, `cagg_prices_1h`, `cagg_decision_volume`, and `cagg_runtime_metrics_5m`, with refresh and retention policies.
- `0007_audit_chain.py` adds and backfills `prev_hash`/`row_hash` on audit-chain tables.
- `0008_audit_findings.py` creates `audit_chain_findings` for verifier divergence reports.
- `0063_model_scoring_indexes.py` adds the unresolved model-scoring indexes used to find the latest tracked prediction per prediction id and to anti-probe already scored rows in `model_performance`.
- `0001_baseline.py` was not changed in this prompt. The baseline already uses JSONB for structured payloads and the runtime schema stores time as epoch-ms `BIGINT` columns, so `0002` uses Timescale integer hypertables instead of forcing a TIMESTAMPTZ rewrite.

## Classification Rules

- Hypertable: primary access is by time range, writes are append-mostly, and cardinality grows with calendar time.
- Regular table: rows are mutated in place, looked up primarily by natural key/current state, or bounded by a registry/configuration domain.
- Compliance ledgers and audit streams have no retention unless explicitly safe to age out. `trade_attribution_ledger` has no retention and no compression.
- Future prompt tables are classified now but migrations skip them until their creating migrations add the physical table and configured time column.

## Defaults

| Category | Chunk | Compress after | Retention | Notes |
| --- | --- | --- | --- | --- |
| Tick / quote stream | 1 day | 7 days | 30 days raw | `prices`, quote streams, and tick-like inputs |
| Derived price bars | 1 day | 7 days | 1 year | Dashboard/model bar reads |
| Time-series features | 1 week | 30 days | 3 years | Feature replay and point-in-time model inputs |
| Health metrics | 1 day | 14 days | 180 days | Operational dashboards and recent diagnostics |
| Audit ledgers | 1 week | 90 days | none | Forensic history retained indefinitely |
| Execution/compliance ledgers | 1 week | 90 days unless compliance-exempt | none | Financial evidence and attribution |
| Job-history style tables | regular | n/a | app-managed 90 days | Latest-row and job-name lookup |

## Index Plan

- Every existing classified hypertable gets `idx_<table>_<time>_brin` using BRIN on its time column.
- Every existing classified hypertable with a `symbol` column gets `idx_<table>_symbol_<time>_desc` for single-symbol range scans.
- Hypertables with non-symbol segment keys, such as `options_chain_v2.underlying` and `runtime_metrics.metric`, get matching `(segment, time DESC)` indexes.
- JSONB columns on hypertables get `jsonb_path_ops` GIN indexes so `@>` and existence predicates stay bounded.
- Targeted expression/index coverage includes `decision_log` reason/family predicates, `model_feature_snapshots(symbol, feature_set_tag, ts_ms DESC)`, model-promotion audit lookups, runtime metric dashboard lookups, and job-history latest-row reads.

## Model Scoring Query Plan

`engine/model_scoring.py` loads unresolved predictions in two bounded branches. In Postgres, the predictions-table branch first materializes at most `MODEL_SCORING_BATCH_LIMIT` unresolved `predictions` in `(ts_ms, id)` order, anti-probing `model_performance` through the non-null partial index `idx_model_performance_prediction_id`. It then uses `LEFT JOIN LATERAL` to resolve the newest tracking row through the non-null partial index `idx_tracked_predictions_prediction_id_ts_id` on `(prediction_id, ts_ms DESC, id DESC)`. The expected Postgres plan is a bounded ordered prediction scan with nested index probes; the latest-tracking lateral subquery should be an index scan with `LIMIT 1` and no sort.

The tracked-only branch handles `tracked_predictions` rows that have no `prediction_id`. It anti-probes `model_performance` through `ux_model_performance_tracked_prediction_id`, which also backs the scorer's `ON CONFLICT(tracked_prediction_id)` retry path. Migration `0063` removes pre-existing duplicate non-null `tracked_prediction_id` performance rows before creating the unique index, so retries update the prior score instead of inserting duplicates.

## Continuous Aggregates

| View | Source | Grain | Refresh | Retention | Purpose |
| --- | --- | --- | --- | --- | --- |
| `cagg_prices_5m` | `prices` | 5 minutes | every 1 minute, lagging 5 minutes | 1 year | OHLC/count dashboard reads |
| `cagg_prices_1h` | `cagg_prices_5m`, with direct `prices` fallback | 1 hour | every 5 minutes | 1 year | Long-horizon OHLC/count dashboard reads |
| `cagg_decision_volume` | `decision_log` | 1 hour | every 5 minutes | 3 years | Decision volume by family |
| `cagg_runtime_metrics_5m` | `runtime_metrics` | 5 minutes | every 1 minute | 180 days | Runtime mean/p99 dashboard reads |

## Table Register

| Table | Class | Lifecycle | Write rate | Read patterns | Rationale |
| --- | --- | --- | --- | --- | --- |
| `active_feature_policy` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `artifact_aliases` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | registry/catalog table; primary access is by natural key, not by time range |
| `artifact_fsck_findings` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | registry/catalog table; primary access is by natural key, not by time range |
| `artifacts` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | registry/catalog table; primary access is by natural key, not by time range |
| `audit_chain_findings` | Regular | cleanup=n/a | low | time-range audit review and table/row investigation | tamper-evidence verifier findings; append-only diagnostics retained indefinitely |
| `alert_acks` | Hypertable | time=acked_ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=none | low | time-range audit review and actor/entity lookup | forensic audit ledger; append-only and retained indefinitely |
| `alert_interactions` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=none | low | time-range audit review and actor/entity lookup | forensic audit ledger; append-only and retained indefinitely |
| `alert_lifecycle_events` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=none | low | time-range audit review and actor/entity lookup | forensic audit ledger; append-only and retained indefinitely |
| `alert_resolutions` | Hypertable | time=resolved_ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=none | low | time-range audit review and actor/entity lookup | forensic audit ledger; append-only and retained indefinitely |
| `alert_shelves` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `alerts` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `alerts_archive` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `alpha_decay_metrics` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `alpha_decay_runtime_history` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `alpha_decay_strategy_metrics` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `alpha_lifecycle` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `learned_alpha_decay_runs` | Regular | cleanup=n/a | low | latest run lookup and training audit | training-run metadata for learned alpha decay, capacity, and crowding estimates |
| `learned_alpha_decay_estimates` | Regular | cleanup=n/a | low | latest cohort lookup from execution, portfolio, and champion paths | learned half-life, max useful age, capacity, crowding penalty, size multiplier, and block flag by cohort |
| `learned_alpha_decay_age_edges` | Regular | cleanup=n/a | low | run/cohort drill-down and estimator audit | realized net edge by signal-age bucket behind learned-alpha estimates |
| `alpha_preservation_kpis` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=symbol | medium | order, symbol, and time-range execution analysis | execution ledger; append-mostly financial evidence retained indefinitely |
| `backtest_scores` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `broker_account` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `broker_config_audit` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=none | low | time-range audit review and actor/entity lookup | forensic audit ledger; append-only and retained indefinitely |
| `broker_connection_health` | Hypertable | time=ts_ms; chunk=1 day; compress=14 days; retain=180 days; segmentby=broker | medium | recent broker health by broker/time | broker liveness samples; append-mostly and dashboarded by time/broker |
| `broker_fills` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=symbol | medium | order, symbol, and time-range execution analysis | execution ledger; append-mostly financial evidence retained indefinitely |
| `broker_meta` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `broker_order_state` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `broker_positions` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `broker_shadow_account` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `broker_shadow_meta` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `broker_shadow_order_state` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `broker_shadow_positions` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `capital_efficiency` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=symbol | medium | order, symbol, and time-range execution analysis | execution ledger; append-mostly financial evidence retained indefinitely |
| `capital_preservation_audit` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=none | low | time-range audit review and actor/entity lookup | forensic audit ledger; append-only and retained indefinitely |
| `causal_scores` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `challenger_shadow_orders` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `champion_assignments` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `competition_post_commit_actions` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `confidence_calibration` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `congressional_trades` | Regular | cleanup=n/a | low | source id upsert and symbol/time feature snapshot reads | low-rate alternative data table upserted by source trade id |
| `cftc_cot_positions` | Regular | cleanup=n/a | low | source id upsert and contract/availability-time feature snapshot reads | weekly CFTC COT rows upserted by source report/contract id |
| `cot_contract_symbol_map` | Regular | cleanup=n/a | low | symbol-to-contract mapping lookups | config table mapping CFTC futures contracts into model symbols and macro topics |
| `cot_symbol_features` | Hypertable | time=asof_ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | low | latest COT feature snapshot by symbol/time | materialized point-in-time COT positioning feature cache |
| `crypto_funding_rates` | Regular | cleanup=n/a | low | source id upsert and symbol/availability-time feature snapshot reads | hourly crypto perpetual funding and basis rows upserted by exchange funding event |
| `credential_access_log` | Hypertable | time=ts; chunk=1 week; compress=none; retain=1 year; segmentby=none | low | time-range credential access review | credential read audit trail; append-only and reviewed by time and credential name |
| `crash_recovery_audit` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=none | low | time-range audit review and actor/entity lookup | forensic audit ledger; append-only and retained indefinitely |
| `data_source_logs` | Hypertable | time=ts_ms; chunk=1 day; compress=14 days; retain=180 days; segmentby=none | medium | recent operational time windows | operational health metric stream; append-mostly and dashboarded by time |
| `data_sources` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | registry/catalog table; primary access is by natural key, not by time range |
| `decision_log` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | high | decision replay by (symbol, time) and JSON predicates | model decision/prediction stream; append-mostly and replayed by symbol/time |
| `decision_views` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | high | decision replay by (symbol, time) and JSON predicates | model decision/prediction stream; append-mostly and replayed by symbol/time |
| `domain_blacklist` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `domain_perf` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `drawdown_bootstrap_baseline` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=none | low | time-range audit review and actor/entity lookup | forensic audit ledger; append-only and retained indefinitely |
| `earnings_calendar` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `embed_conf_calib` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `embed_model_eval` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `embed_model_feature_schema` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `embed_models2` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | registry/catalog table; primary access is by natural key, not by time range |
| `ensemble_blend_weights` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `ensemble_family_performance` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `ensemble_predictions` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | high | decision replay by (symbol, time) and JSON predicates | model decision/prediction stream; append-mostly and replayed by symbol/time |
| `equity_history` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `event_embeddings` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `event_embeddings_seq` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `event_log` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `event_log_state` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `events` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `exec_conf_calib` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `exec_open_orders` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `exec_order_events` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `execution_ai_advisory` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `execution_ai_advisory_actions` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `execution_alerts` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `execution_analytics` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=symbol | medium | order, symbol, and time-range execution analysis | execution ledger; append-mostly financial evidence retained indefinitely |
| `execution_capital_efficiency` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=symbol | medium | order, symbol, and time-range execution analysis | execution ledger; append-mostly financial evidence retained indefinitely |
| `execution_divergence` | Hypertable | time=ts_ms; chunk=1 day; compress=14 days; retain=180 days; segmentby=none | medium | recent operational time windows | operational health metric stream; append-mostly and dashboarded by time |
| `execution_fill_quality` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=symbol | medium | order, symbol, and time-range execution analysis | execution ledger; append-mostly financial evidence retained indefinitely |
| `execution_fills` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=symbol | medium | order, symbol, and time-range execution analysis | execution ledger; append-mostly financial evidence retained indefinitely |
| `execution_health_state` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `execution_meta` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `execution_metrics` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=symbol | medium | order, symbol, and time-range execution analysis | execution ledger; append-mostly financial evidence retained indefinitely |
| `execution_mode` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `execution_mode_audit` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=none | low | time-range audit review and actor/entity lookup | forensic audit ledger; append-only and retained indefinitely |
| `execution_order_idempotency` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `execution_orders` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `execution_policy_audit` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=none | low | time-range audit review and actor/entity lookup | forensic audit ledger; append-only and retained indefinitely |
| `execution_policy_feedback` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=symbol | medium | order, symbol, and time-range execution analysis | execution ledger; append-mostly financial evidence retained indefinitely |
| `execution_slippage_feedback` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=symbol | medium | order, symbol, and time-range execution analysis | execution ledger; append-mostly financial evidence retained indefinitely |
| `execution_strategy_attribution` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=symbol | medium | order, symbol, and time-range execution analysis | execution ledger; append-mostly financial evidence retained indefinitely |
| `etf_flow_features` | Hypertable | time=asof_ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | medium | latest ETF flow feature snapshot by symbol/time | materialized point-in-time ETF unexpected-flow feature cache |
| `etf_shares_outstanding` | Regular | cleanup=n/a | low | source id upsert and symbol/availability-time feature snapshot reads | daily ETF shares-outstanding rows upserted by symbol/as-of source record id |
| `factor_features` | Hypertable | time=asof_ts; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `factor_group_scores` | Hypertable | time=ts; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `factor_groups` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `factor_observations` | Hypertable | time=asof_ts; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `factor_registry` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | registry/catalog table; primary access is by natural key, not by time range |
| `feature_data` | Hypertable | time=timestamp; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | high | feature-store replay by (symbol, timestamp) | Timescale sidecar feature vectors; append-mostly and read by symbol/time |
| `feature_distribution_drift` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `production_monitoring_metrics` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded latest-state production monitoring for feature drift, missing features, prediction/label drift, calibration, conformal coverage, shadow disagreement, and net-PnL degradation |
| `feature_store` | Hypertable | time=time; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | high | latest feature vector at or before time | versioned Timescale feature store; append/update-current bucket by symbol/time/version |
| `finra_short_interest` | Regular | cleanup=n/a | low | source id upsert and symbol/availability-time feature snapshot reads | bi-monthly FINRA short-interest rows upserted by source record id |
| `finra_short_sale_volume` | Regular | cleanup=n/a | low | source id upsert and symbol/availability-time feature snapshot reads | daily FINRA short-sale volume rows upserted by source record id |
| `fundamentals_pit` | Regular | cleanup=n/a | low | source id upsert and symbol/metric publish-time feature snapshot reads | immutable point-in-time fundamentals vendor metric publications keyed by source record id |
| `fundamentals_pit_backfill_state` | Regular | cleanup=n/a | low | vendor/state-key backfill cursor lookup | resumable PIT fundamentals bulk-load cursors by vendor |
| `fundamentals_pit_symbol_features` | Hypertable | time=asof_ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | low | latest fundamentals feature snapshot by symbol/time | materialized point-in-time fundamentals feature cache |
| `gbm_models` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | registry/catalog table; primary access is by natural key, not by time range |
| `gdelt_macro_features` | Hypertable | time=bucket_ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | macro replay windows by bucket time | macro feature buckets; append-mostly and read by historical time windows |
| `graph_relationship_edges` | Regular | cleanup=source-specific retention; preserve rows needed by retained graph snapshots and promotion evidence | medium | PIT graph snapshot construction by source or target symbol, relationship type, and availability time | point-in-time graph relationship edge catalog keyed by source/target symbol, relation type, and availability time |
| `graph_relational_snapshots` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | medium | latest graph snapshot by symbol/graph_id/time and historical replay windows | versioned point-in-time graph/relational feature snapshots for shadow-only train/serve parity and promotion evidence |
| `gov_committee_sector_map` | Regular | cleanup=n/a | low | committee sector lookup for gov feature conditioning | static committee-to-sector conditioning map |
| `gov_member_committee_map` | Regular | cleanup=n/a | low | member committee lookup for gov feature conditioning | static congressional member-to-committee conditioning map |
| `gov_member_leadership_map` | Regular | cleanup=n/a | low | member leadership lookup for gov feature conditioning | static congressional leadership member map |
| `gov_symbol_features` | Hypertable | time=asof_ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | low | latest gov feature snapshot by symbol/time | materialized point-in-time Quiver government-flow feature cache |
| `gov_symbol_sector_map` | Regular | cleanup=n/a | low | symbol sector lookup | symbol-to-sector map for government-flow feature conditioning |
| `har_rv_forecasts` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | medium | latest forecast by (symbol, ts_ms) and walk-forward validation windows | point-in-time HAR-RV volatility forecasts used by sizing and Monte Carlo risk inputs |
| `hmm_regime_models` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | registry/catalog table; primary access is by natural key, not by time range |
| `ingest_slippage` | Hypertable | time=ts_ms; chunk=1 day; compress=14 days; retain=180 days; segmentby=none | medium | recent operational time windows | operational health metric stream; append-mostly and dashboarded by time |
| `ingestion_pipeline_health` | Hypertable | time=ts_ms; chunk=1 day; compress=14 days; retain=180 days; segmentby=none | medium | recent operational time windows | operational health metric stream; append-mostly and dashboarded by time |
| `inst_13f_cusip_symbol_map` | Regular | cleanup=n/a | low | CUSIP mapping lookups | 13F CUSIP-to-symbol mapping cache and manual review table |
| `inst_13f_filings` | Regular | cleanup=n/a | low | manager/latest filing lookup by availability time | quarterly SEC 13F filing metadata keyed by manager/accession and EDGAR acceptance time |
| `inst_13f_holdings` | Regular | cleanup=n/a | low | symbol and manager/report feature snapshot reads | raw 13F information-table holdings keyed by manager/accession/CUSIP row |
| `inst_13f_manager_universe` | Regular | cleanup=n/a | low | manager configuration lookup | configured 13F manager universe with active flags and turnover thresholds |
| `inst_13f_symbol_features` | Hypertable | time=asof_ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | low | latest 13F overlay feature snapshot by symbol/time | materialized point-in-time 13F low-turnover manager overlay cache |
| `insider_transactions` | Regular | cleanup=n/a | low | source id upsert and symbol/time feature snapshot reads | low-rate alternative data table upserted by source transaction id |
| `ipc_channels` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `ipc_messages` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `job_checkpoints` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `job_heartbeats` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `job_history` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `job_locks` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `kill_switch_audit` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=none | low | time-range audit review and actor/entity lookup | forensic audit ledger; append-only and retained indefinitely |
| `kill_switch_state` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `labels` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `labels_exec` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `labels_price` | Hypertable | time=ts_eval_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | medium | calibration and validation scans by symbol, prediction time, evaluation time, and horizon | derived realized price labels keyed to prediction and evaluation time for confidence calibration and validation |
| `net_after_cost_labels` | Hypertable | time=label_ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | training, evaluation, and promotion scans by label time and model identity | timestamp-safe net-after-cost label artifacts keyed to prediction time and replayed by model/symbol/horizon |
| `macro_series_vintages` | Regular | cleanup=n/a | low | series vintage upserts and point-in-time macro feature materialization | ALFRED/FRED macro observations keyed by series, observation date, and vintage date |
| `macro_vintage_backfill_state` | Regular | cleanup=n/a | low | primary-key lookup by macro series id | resumable state for one-time macro vintage backfills |
| `market_features` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | medium | latest feature snapshot and historical replay by (symbol, time) | point-in-time feature series; append-mostly and replayed by symbol/time |
| `market_microstructure_signals` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | medium | latest feature snapshot and historical replay by (symbol, time) | point-in-time feature series; append-mostly and replayed by symbol/time |
| `model_competition_rankings` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `model_drift` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `model_feature_snapshots` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | high | latest snapshot by (symbol, feature_set_tag, ts_ms) and replay windows | canonical point-in-time feature snapshots; latest lookup is by symbol, feature_set_tag, and time |
| `model_governance_log` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=none | low | time-range audit review and actor/entity lookup | forensic audit ledger; append-only and retained indefinitely |
| `model_lifecycle_runs` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `model_marketplace_scores` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `model_metrics` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `model_oos_predictions` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | high | decision replay by (symbol, time) and JSON predicates | model decision/prediction stream; append-mostly and replayed by symbol/time |
| `model_position_state` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `model_post_promo_results` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `model_post_promo_watch` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `model_predictions` | Hypertable | time=timestamp; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | high | decision replay by (symbol, time) and JSON predicates | model decision/prediction stream; append-mostly and replayed by symbol/time |
| `model_promotion_audit` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=none | low | time-range audit review and actor/entity lookup | forensic audit ledger; append-only and retained indefinitely |
| `model_promotion_cooldown` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `model_promotion_guard` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `model_registry` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | registry/catalog table; primary access is by natural key, not by time range |
| `model_runs` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `model_stats` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `model_stats_regime` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `model_stats_regime_versions` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `model_stats_versions` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `model_version_performance` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `model_versions` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `model_weather_effect` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `models` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | registry/catalog table; primary access is by natural key, not by time range |
| `narrative_clusters` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `narrative_members` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `news_event_features` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | medium | latest feature snapshot and historical replay by (symbol, time) | point-in-time feature series; append-mostly and replayed by symbol/time |
| `news_flow_features` | Hypertable | time=asof_ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | medium | latest news-flow feature snapshot by symbol/time/backend | materialized point-in-time news novelty/staleness feature cache |
| `news_story_embeddings` | Regular | cleanup=n/a | medium | symbol/backend availability-window novelty comparisons and feature snapshots | backend-aware per-story news embeddings and novelty scores keyed by event/symbol/model |
| `news_symbol_features` | Hypertable | time=bucket_ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | medium | latest feature snapshot and historical replay by (symbol, time) | point-in-time feature series; append-mostly and replayed by symbol/time |
| `nlp_embeddings` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `nlp_sentiments` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `nlp_text_blobs` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `notification_channel_tests` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `options_chain` | Hypertable | time=ts_ms; chunk=1 day; compress=7 days; retain=3 years; segmentby=underlying | high | underlying/time option surface and chain scans | options market data stream; append-mostly and queried by underlying/time |
| `options_chain_v2` | Hypertable | time=ts_ms; chunk=1 day; compress=7 days; retain=3 years; segmentby=underlying | high | underlying/time option surface and chain scans | options market data stream; append-mostly and queried by underlying/time |
| `options_event_features` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | medium | latest feature snapshot and historical replay by (symbol, time) | point-in-time option event feature series, including IV/skew/unusual volume plus GEX/flow proxies |
| `options_surface` | Hypertable | time=ts_ms; chunk=1 day; compress=7 days; retain=3 years; segmentby=underlying | high | underlying/time option surface and chain scans | options market data stream; append-mostly and queried by underlying/time |
| `options_surface_agg` | Hypertable | time=ts_ms; chunk=1 day; compress=7 days; retain=3 years; segmentby=none | medium | dashboard scans by time | global options surface aggregate stream; append-mostly and read by time |
| `options_symbol_features` | Hypertable | time=bucket_ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | medium | latest feature snapshot and historical replay by (symbol, time) | point-in-time symbol option features; joins must require snapshot_ts_ms <= as-of time to avoid bucket lookahead |
| `options_symbol_ingestion_state` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `order_commands` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `order_events` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=symbol | medium | order, symbol, and time-range execution analysis | execution ledger; append-mostly financial evidence retained indefinitely |
| `pipeline_stage_audit` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=none | low | time-range audit review and actor/entity lookup | forensic audit ledger; append-only and retained indefinitely |
| `pnl_attribution` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=symbol | medium | order, symbol, and time-range execution analysis | execution ledger; append-mostly financial evidence retained indefinitely |
| `pnl_decomposition` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `portfolio_bt_points` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `portfolio_bt_runs` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `portfolio_equity_state` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `portfolio_kill_snapshots` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=none | low | time-range audit review and actor/entity lookup | forensic audit ledger; append-only and retained indefinitely |
| `portfolio_meta` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `portfolio_model_corr_snapshots` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `portfolio_orders` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `portfolio_position_corr_snapshots` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `portfolio_risk_snapshots` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=none | low | time-range audit review and actor/entity lookup | forensic audit ledger; append-only and retained indefinitely |
| `portfolio_state` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `position_reconcile_audit` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=none | low | time-range audit review and actor/entity lookup | forensic audit ledger; append-only and retained indefinitely |
| `position_reconcile_baseline` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `policy_ope_observations` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | high | decision replay by (symbol, time) and JSON predicates | logged off-policy decisions, propensities, outcomes, and model estimates used by OPE promotion gates |
| `policy_ope_evidence` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | append-only doubly robust OPE promotion evidence with uncertainty, support, and pass/fail decision |
| `prediction_history` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | high | decision replay by (symbol, time) and JSON predicates | model decision/prediction stream; append-mostly and replayed by symbol/time |
| `predictions` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | high | decision replay by (symbol, time) and JSON predicates | model decision/prediction stream; append-mostly and replayed by symbol/time |
| `price_anomalies` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | medium | latest feature snapshot and historical replay by (symbol, time) | point-in-time feature series; append-mostly and replayed by symbol/time |
| `price_bars` | Hypertable | time=ts_ms; chunk=1 day; compress=7 days; retain=1 year; segmentby=symbol | high | dashboard and model windows by (symbol, time) | derived bar/price series; append-mostly and read by symbol/time ranges |
| `price_data` | Hypertable | time=timestamp; chunk=1 day; compress=7 days; retain=1 year; segmentby=symbol | high | dashboard and model windows by (symbol, time) | derived bar/price series; append-mostly and read by symbol/time ranges |
| `price_feed_lock` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `price_provider_health` | Hypertable | time=ts_ms; chunk=1 day; compress=14 days; retain=180 days; segmentby=none | medium | recent operational time windows | operational health metric stream; append-mostly and dashboarded by time |
| `price_quotes` | Hypertable | time=ts_ms; chunk=1 day; compress=7 days; retain=30 days; segmentby=symbol | very high | latest and intraday ranges by (symbol, time) | high-rate market data stream; append-mostly and queried by symbol/time windows |
| `price_quotes_raw` | Hypertable | time=ts_ms; chunk=1 day; compress=7 days; retain=30 days; segmentby=symbol | very high | latest and intraday ranges by (symbol, time) | high-rate market data stream; append-mostly and queried by symbol/time windows |
| `price_ticks` | Hypertable | time=time; chunk=1 day; compress=7 days; retain=30 days; segmentby=symbol | very high | latest and intraday ranges by (symbol, time) | high-rate market data stream; append-mostly and queried by symbol/time windows |
| `prices` | Hypertable | time=ts_ms; chunk=1 day; compress=7 days; retain=30 days; segmentby=symbol | very high | latest and intraday ranges by (symbol, time) | high-rate market data stream; append-mostly and queried by symbol/time windows |
| `promotion_statistical_evidence` | Hypertable | time=ts; chunk=1 week; compress=90 days; retain=none; segmentby=none | low | time-range audit review and actor/entity lookup | forensic audit ledger; append-only and retained indefinitely |
| `strategy_promotion_candidates` | Regular | cleanup=n/a | low | pending candidate and operator approval lookup by strategy/status | governed shadow-strategy promotion candidates; live mutation requires approval, realized PnL, replay/OPE/statistical evidence, cooldown, and audit records |
| `quiver_congressional_trades` | Regular | cleanup=n/a | low | source id upsert, dedupe-key lookup, and symbol/disclosure-time feature snapshot reads | Quiver congressional trade disclosures keyed by source record id and disclosure availability time |
| `quiver_gov_contracts` | Regular | cleanup=n/a | low | source id upsert and symbol/sector availability-time feature snapshot reads | Quiver government contract award disclosures keyed by source record id |
| `quiver_lobbying_filings` | Regular | cleanup=n/a | low | source id upsert and symbol/sector availability-time feature snapshot reads | Quiver lobbying spend disclosures keyed by source record id |
| `regime_compat_scores` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `residual_distribution_drift` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `risk_events` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=none | low | time-range audit review and actor/entity lookup | forensic audit ledger; append-only and retained indefinitely |
| `risk_state` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `rl_policies` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | registry/catalog table; primary access is by natural key, not by time range |
| `rl_shadow_actions` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `rl_shadow_eval` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `rl_strategy_policy_decisions` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | high | decision replay by (symbol, time) and JSON predicates | model decision/prediction stream; append-mostly and replayed by symbol/time |
| `rl_strategy_policy_models` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | registry/catalog table; primary access is by natural key, not by time range |
| `rules_audit` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=none | low | time-range audit review and actor/entity lookup | forensic audit ledger; append-only and retained indefinitely |
| `runtime_meta` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `runtime_metrics` | Hypertable | time=ts_ms; chunk=1 day; compress=14 days; retain=180 days; segmentby=metric | high | metric/time dashboard windows | runtime metric stream; append-mostly and rolled up for dashboards |
| `runtime_metrics_state` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `schema_migrations` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `schema_version` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `sec_filings` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | medium | latest feature snapshot and historical replay by (symbol, time) | point-in-time feature series; append-mostly and replayed by symbol/time |
| `self_critic_alerts` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `shadow_capital_scores` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `shadow_metrics` | Hypertable | time=window_end_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `shadow_order_intents` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `shadow_predictions` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | high | decision replay by (symbol, time) and JSON predicates | model decision/prediction stream; append-mostly and replayed by symbol/time |
| `shadow_training_runs` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `size_policy` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `size_policy_points` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `sleeve_allocations` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `sleeve_metrics` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `sleeve_registry` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `social_features` | Hypertable | time=bucket_ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | medium | latest feature snapshot and historical replay by (symbol, time) | point-in-time feature series; append-mostly and replayed by symbol/time |
| `social_posts` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | medium | latest feature snapshot and historical replay by (symbol, time) | point-in-time feature series; append-mostly and replayed by symbol/time |
| `social_regimes` | Hypertable | time=bucket_ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | medium | latest feature snapshot and historical replay by (symbol, time) | point-in-time feature series; append-mostly and replayed by symbol/time |
| `spillover_beta` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `spillover_beta_versions` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `strategy_allocations` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `strategy_allocator_history` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `strategy_allocator_scores` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `strategy_cooldowns` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `strategy_metrics` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `strategy_promotion_log` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `strategy_registry` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `strategy_shadow_runs` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `structured_document_events` | Regular | cleanup=n/a | medium | symbol availability-window PIT feature snapshots and source-document audits | structured extracted events from filings, transcripts, and news keyed by source document and extractor version |
| `suppression_opportunity` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=symbol | medium | order, symbol, and time-range execution analysis | execution ledger; append-mostly financial evidence retained indefinitely |
| `symbol_blacklist` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `symbol_universe` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `symbolic_alpha_candidates` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `symbols` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `temporal_model_eval` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `temporal_model_feature_schema` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `temporal_models` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | registry/catalog table; primary access is by natural key, not by time range |
| `temporal_predictions` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | high | decision replay by (symbol, time) and JSON predicates | model decision/prediction stream; append-mostly and replayed by symbol/time |
| `temporal_shadow_eval` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `timescale_schema_version` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | registry/catalog table; primary access is by natural key, not by time range |
| `terminal_intent_rejections` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=symbol | medium | order, symbol, and time-range execution analysis | execution ledger; append-mostly financial evidence retained indefinitely |
| `trade_attribution_ledger` | Hypertable | time=ts_ms; chunk=1 week; compress=none; retain=none; segmentby=symbol | medium | order/source_alert/model/symbol lookup and time-range forensic review | compliance attribution ledger; append-only, never compressed, never deleted |
| `trade_decision_snapshot` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `trade_outcomes` | Hypertable | time=timestamp; chunk=1 week; compress=90 days; retain=none; segmentby=symbol | medium | order, symbol, and time-range execution analysis | execution ledger; append-mostly financial evidence retained indefinitely |
| `trade_suppression_audit` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=none | low | time-range audit review and actor/entity lookup | forensic audit ledger; append-only and retained indefinitely |
| `trade_suppression_state` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `trades` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=symbol | medium | order, symbol, and time-range execution analysis | execution ledger; append-mostly financial evidence retained indefinitely |
| `universe_audit` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=none | low | time-range audit review and actor/entity lookup | forensic audit ledger; append-only and retained indefinitely |
| `universe_pit` | Regular | cleanup=job_history and alerts use app-managed rotation where configured | low | primary-key or latest-state lookup | bounded or low-rate operational table; primary lookup is not a time-range scan |
| `validation_scores` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `walk_forward_runs` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `walk_forward_scores` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `weather_alerts` | Hypertable | time=issued_ts; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `weather_forecast_region_daily` | Hypertable | time=run_ts; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `weather_provider_health` | Hypertable | time=ts_ms; chunk=1 day; compress=14 days; retain=180 days; segmentby=none | medium | recent operational time windows | operational health metric stream; append-mostly and dashboarded by time |

## Additional Classified Tables

| Table | Class | Policy | Write rate | Read pattern | Rationale |
|---|---|---|---|---|---|
| `alpha_candidates` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `backtest_cpcv_path_results` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `backtest_cpcv_runs` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `bocpd_ensemble_triggers` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `bocpd_regime_state` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `causal_dags` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | registry/catalog table; primary access is by natural key, not by time range |
| `champion_residual_adwin_state` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `data_source_audit` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=none | low | time-range audit review and actor/entity lookup | forensic audit ledger; append-only and retained indefinitely |
| `drift_retrain_events` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `ensemble_weights` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `equity_drift` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `feature_candidates` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `feature_evaluation` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `feature_registry` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | registry/catalog table; primary access is by natural key, not by time range |
| `experiment_ledger` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | append-only generated-candidate ledger for lineage, trial budgets, false-discovery evidence, redundancy checks, and promotion decisions |
| `finbert_sentiment_enrichments` | Hypertable | time=asof_ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | medium | latest sentiment enrichment by symbol/time/source | point-in-time FinBERT sentiment enrichments keyed by symbol/source availability |
| `hypothesis_registry` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | registry/catalog table; primary access is by natural key, not by time range |
| `model_best_params` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `model_hyperparameter_registry` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `model_performance` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `position_reconcile_bootstrap_audit` | Hypertable | time=ts_ms; chunk=1 week; compress=90 days; retain=none; segmentby=none | low | time-range audit review and actor/entity lookup | forensic audit ledger; append-only and retained indefinitely |
| `position_reconcile_state` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `prediction_explanations` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | high | decision replay by (symbol, time) and JSON predicates | model decision/prediction stream; append-mostly and replayed by symbol/time |
| `realized_outcomes` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=none | medium | time-range replay and dashboard scans | append-mostly feature/evaluation series keyed primarily by time |
| `regime_state` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | mutable state table; current value is the contract and rows are updated in place |
| `rl_shadow_decisions` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | high | decision replay by (symbol, time) and JSON predicates | model decision/prediction stream; append-mostly and replayed by symbol/time |
| `rl_training_runs` | Regular | cleanup=n/a | low to medium | model/run keyed lookup and latest status reads | training/model artifact metadata; looked up by model/run identifiers |
| `tracked_model_registry` | Regular | cleanup=n/a | low | primary-key or latest-state lookup | registry/catalog table; primary access is by natural key, not by time range |
| `tracked_predictions` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | high | decision replay by (symbol, time) and JSON predicates | model decision/prediction stream; append-mostly and replayed by symbol/time |
| `triple_barrier_labels` | Hypertable | time=ts_ms; chunk=1 week; compress=30 days; retain=3 years; segmentby=symbol | high | decision replay by (symbol, time) and JSON predicates | model decision/prediction stream; append-mostly and replayed by symbol/time |
