# Codex DB Prompt 03 — Production Schema: Hypertables, Indexes, Retention, Compression

You are working in a Python systematic trading system whose ~210
tables are about to land in Postgres 16 + TimescaleDB 2.x for the
first time (prompts 01 + 02 set up the host and the storage layer).
This prompt **designs and writes the production schema** — the right
table types, the right partitioning, the right indexes, compression,
retention, and continuous aggregates so that the database is **never
the bottleneck** at production ingestion and decision rates on a
single Linux server.

This is the most consequential prompt in this series. Schema design
is hard to change later; do it right now.

## Goal

1. Every table classified into one of:
   - **Hypertable** (time-series; partitioned by `ts`)
   - **Regular table** (OLTP state, registries, configs)
2. For every hypertable: chunk interval, compression policy,
   retention policy, primary key, BRIN index on `ts`, B-tree on
   `(symbol, ts)` where applicable.
3. JSONB GIN indexes on the keys actually queried (audit
   `payload->>'reason'`, decision `payload->>'family'`, etc.).
4. Continuous aggregates rolling up the highest-cardinality
   hypertables to 5-minute and 1-hour bars for dashboard reads.
5. A schema-design document `docs/Database_Schema.md` that records
   every classification decision and its reasoning. New tables added
   later must be classified before they ship.

## Files to read first (read-only)

- `engine/runtime/storage.py` — every `CREATE TABLE IF NOT EXISTS`.
- A grep `CREATE TABLE IF NOT EXISTS` across all of `engine/` and
  `ops/` — there are tables created outside `storage.py` (NLP cache,
  causal scores, feature candidates, etc., from sibling prompts).
- `engine/strategy/feature_registry.py` — to understand what
  `model_feature_snapshots` holds and how it is read.
- `engine/strategy/decision_log.py` — read pattern: by `(symbol, ts)`
  range; payload search by `reason`.
- `engine/strategy/promotion_audit.py` — read pattern: by `model_id`,
  by `ts` range.
- `engine/execution/trade_attribution_ledger.py` — read pattern: by
  `order_id`, by `ts` range; **never deleted**.
- `engine/runtime/jobs_manager.py` — read pattern: most-recent-row
  by `job_name`.
- `engine/runtime/schema/migrations/0001_baseline.py` from prompt 02
  — the baseline this prompt's `0002_hypertables` migration extends.

## Files to create

- `engine/runtime/schema/migrations/0002_hypertables.py` — converts
  every classified time-series table into a Timescale hypertable;
  applies the chunk / compression / retention policies; creates the
  continuous aggregates.
- `engine/runtime/schema/migrations/0003_indexes.py` — every
  performance-critical index. Separated from the baseline so an index
  rewrite is its own auditable migration.
- `engine/runtime/schema/migrations/0004_continuous_aggregates.py` —
  the dashboard rollups; refresh policies.
- `engine/runtime/schema/table_classification.py` — single source of
  truth for every table's classification. Importable so other code
  (audit jobs, the verifier) can ask "is this table append-only?".
  Format:
  ```python
  TABLE_CLASS = {
      "price_quotes_raw": Hypertable(
          chunk="1 day", compress_after="7 days",
          retain="30 days", segmentby=("symbol",),
      ),
      "trade_attribution_ledger": Hypertable(
          chunk="1 week", compress_after="90 days",
          retain=None, segmentby=("symbol",),
      ),
      "model_registry": Regular(),
      ...
  }
  ```
- `docs/Database_Schema.md` — the design document: each table, its
  class, its rationale, its expected write rate, its read patterns.
- `tests/test_schema_classification.py` — every `CREATE TABLE` found
  in the codebase has an entry in `table_classification.py`. Build
  fails if a new table ships without classification.
- `tests/test_schema_hypertable_creation.py` — each hypertable is
  actually a hypertable post-migration: `SELECT FROM
  timescaledb_information.hypertables`.
- `tests/test_schema_compression_policy.py` — every hypertable with
  `compress_after` has a matching policy.
- `tests/test_schema_retention_policy.py` — every hypertable with
  `retain` has a matching policy.
- `tests/test_schema_indexes_present.py` — explicit list of
  performance-critical indexes; query `pg_indexes` to confirm.
- `tests/test_schema_caggs_present.py` — continuous aggregates
  exist and refresh policies are armed.

## Files to modify

- `engine/runtime/schema/migrations/0001_baseline.py` (from prompt
  02) — only to align column types where prompt 03's design
  requires JSONB / TIMESTAMPTZ that the baseline did not anticipate.
  Document any change.

## Classification rules

Apply these rules consistently. Every classification decision goes
into `docs/Database_Schema.md` with one-line reasoning.

**Hypertable** when **all** of:
- Primary access pattern is by time range.
- Append-mostly (no in-place updates of historic rows).
- Cardinality grows monotonically with calendar time.

**Regular table** when **any** of:
- Mutated in place (state machine; current value matters).
- Looked up by something other than `ts` (registry, config).
- Cardinality bounded by a small enumeration.

### Initial classification (apply unless your read of the code
overrides; document overrides):

**Hypertable**
- `price_quotes_raw`, `price_quotes`, `prices`,
  `market_microstructure_signals`, `price_anomalies`
- `options_chain_v2`, `options_chain`, `options_surface`,
  `options_surface_agg`
- `market_features`, `model_feature_snapshots`,
  `news_event_features`, `news_symbol_features`,
  `options_event_features`, `options_symbol_features`,
  `social_features`, `social_posts`, `gdelt_macro_features`
- `broker_fills`, `runtime_metrics`, `event_log`,
  `ingest_slippage`, `ingestion_pipeline_health`,
  `price_provider_health`
- `decision_log` (forthcoming),
  `model_oos_predictions` (from prompt 06),
  `nlp_embeddings`, `nlp_sentiments`, `nlp_text_blobs`
  (from prompt 08), `causal_scores` (from prompt 09)
- All `*_audit` tables (kill_switch_audit, execution_mode_audit,
  execution_policy_audit, position_reconcile_audit,
  alert_acks, alert_resolutions, trade_suppression_audit,
  promotion_statistical_evidence)
- `trade_attribution_ledger` — special: hypertable with **no
  retention and no compression** (compliance).

**Regular table**
- All `*_registry` (model, strategy, sleeve, data_sources)
- All `*_state` tables (kill_switch_state, execution_mode,
  execution_health_state, broker_order_state,
  position_reconcile_baseline, trade_suppression_state)
- `domain_blacklist`, `domain_perf`
- `schema_version`, `schema_migrations`, `runtime_meta`,
  `execution_meta`, `runtime_metrics_state`
- `job_history` (consider hypertable if it grows large; default
  regular with 90-day app-managed cleanup)
- `strategy_allocations`, `strategy_allocator_scores`,
  `strategy_cooldowns`, `sleeve_allocations`, `sleeve_metrics`
- `model_promotion_cooldown`, `model_post_promo_watch`,
  `model_post_promo_results`
- `alpha_decay_strategy_metrics`, `alpha_decay_runtime_history`
- `alerts` (current), with rotation to a hypertable
  `alerts_archive` after 90 days

## Compression and retention defaults

| Class | Chunk | Compress after | Compression order | Retention |
|---|---|---|---|---|
| Tick / quote stream | 1 day | 7 days | real price time column DESC | 30 days raw, 1 y bars |
| Time-series features | 1 week | 30 days | classified time column DESC | 3 years |
| Audit ledgers | 1 week | 90 days | classified time column DESC | **none** |
| `trade_attribution_ledger` | 1 week | **none** | n/a | **none** |
| Health metrics | 1 day | 14 days | classified/sidecar time column DESC | 180 days |
| Job-history-style | regular | n/a | n/a | app-managed 90 days |

Override per-table in `table_classification.py` with one-line reason.
Compression setup must preserve each table's `segmentby` policy and set
`compress_orderby` to the table's real time column. For sidecar-owned Timescale
tables, use the actual DDL column (`"time"` for operational telemetry and
`"timestamp"` for price/feature/model/trade sidecar tables), even when a legacy
classification entry uses a different storage-layer time name.

## Index plan

For every hypertable:
- `BRIN` on `ts` (cheap, large-range-friendly; default).
- `(symbol, ts DESC)` B-tree where queries filter on a single symbol
  (price tables, feature tables, decisions).

For audit tables with JSONB payloads:
- `GIN (payload jsonb_path_ops)` so existence and `@>` queries are
  fast.
- Targeted expression indexes on the most common predicates, e.g.
  `((payload->>'reason'))` on `decision_log`.

For state tables read by every decision:
- The PK suffices; ensure the PK is the read predicate.

For `model_feature_snapshots`:
- `(symbol, feature_group, ts DESC)` to satisfy "latest features for
  symbol".

## Continuous aggregates

Define under `engine/runtime/schema/migrations/0004_continuous_aggregates.py`:
- `cagg_prices_5m` — 5-minute OHLCV from `prices`. Refresh policy
  every 1 minute lagging by 5 minutes.
- `cagg_prices_1h` — 1-hour OHLCV from `cagg_prices_5m`. Refresh
  every 5 minutes.
- `cagg_decision_volume` — hourly count of decisions per family.
- `cagg_runtime_metrics_5m` — 5-minute mean / p99 of runtime
  metrics for the dashboard.

Each cagg has retention so that its underlying hypertable's
retention is consistent.

## Performance targets

- A "latest features for symbol" lookup against
  `model_feature_snapshots` returns in **< 1 ms p50** through the
  pool.
- A 24-hour range scan over `prices` for a single symbol returns in
  **< 50 ms** with the `(symbol, ts DESC)` index in use.
- A JSON-path predicate against `decision_log.payload`
  (`@> '{"reason":"size_compress"}'`) using the GIN index returns in
  **< 100 ms** for the trailing 30 days.
- Compression on `prices` reduces storage by **≥ 10×** on the first
  week's data after the policy fires.
- Retention enforcement runs to completion in under one chunk
  interval (so the system stays caught up without backlog).

## Acceptance criteria

- [ ] Every table created anywhere in `engine/` or `ops/` has an
      entry in `table_classification.py`.
      `tests/test_schema_classification.py` enforces this.
- [ ] Every hypertable is a hypertable post-migration:
      `SELECT count(*) FROM timescaledb_information.hypertables` is
      ≥ the count of hypertable classifications.
- [ ] Every classified compression / retention rule has a matching
      policy in `timescaledb_information.jobs`.
- [ ] Every continuous aggregate is created and has an active
      refresh policy.
- [ ] BRIN on `ts` exists on every hypertable.
- [ ] `(symbol, ts DESC)` exists on every hypertable that has a
      `symbol` column.
- [ ] `GIN` exists on every JSONB column queried by predicate.
- [ ] `0002`, `0003`, `0004` migrations apply cleanly to a fresh
      database (created via prompt 01 → migration 0001 → 0002 → 0003
      → 0004).
- [ ] Re-applying any migration is a no-op.
- [ ] `docs/Database_Schema.md` exists, lists every table, its
      classification, its rationale, expected write rate, and read
      patterns.

## Test plan

- `tests/test_schema_classification.py` — diff `CREATE TABLE` matches
  `TABLE_CLASS`; fails informatively when divergent.
- `tests/test_schema_hypertable_creation.py` — assert hypertable
  presence and chunk interval for each.
- `tests/test_schema_compression_policy.py` — assert compression
  policy presence, `compress_after`, and generated `compress_orderby` options.
- `tests/test_schema_retention_policy.py` — assert retention policy
  presence (or explicit absence for compliance tables).
- `tests/test_schema_indexes_present.py` — query `pg_indexes`,
  assert each performance-critical index by name.
- `tests/test_schema_caggs_present.py` — assert continuous
  aggregates and their refresh policies.

Run: `pytest -q tests/test_schema_classification.py
tests/test_schema_hypertable_creation.py
tests/test_schema_compression_policy.py
tests/test_schema_retention_policy.py
tests/test_schema_indexes_present.py tests/test_schema_caggs_present.py`

## Out of scope

- Application-level schema changes (new tables for prompts 06 / 07
  / 08 / 09 from the original 1–10 series). Those prompts include
  their own migrations, which inherit this prompt's classification
  framework.
- Materialized views beyond the four continuous aggregates above.
- Partitioning by anything other than time (no symbol-hash
  partitioning; unnecessary at single-server scale).
- Foreign keys between hypertables. Time-series joins use natural
  keys with index support; FKs across hypertables hurt write
  performance more than they help.
