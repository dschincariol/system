# Database Production Plan

**Audience:** owner / sole operator preparing the system for live capital.
**Scope:** persistence layer only. Application code, models, and risk
logic are out of scope here.

## 1. Current state

Status note, 2026-06: this document is now historical migration planning. The current production-like runtime uses the Postgres-backed storage facade in `engine/runtime/storage_pg.py`; `engine/runtime/storage_sqlite.py` is retained for isolated Python tests and compatibility coverage. Use `docs/README_DATABASE_MAP.md`, `docs/ARCHITECTURE.md`, `.env.example`, and the runtime code as the current contract.

At the time this plan was written, the persistence backbone was a **single SQLite database in WAL
mode** holding **~210 distinct tables** (verified by grep against
`engine/runtime/storage.py` and the per-module create statements). It
covers four very different workloads on one file:

1. **Streaming time-series writes** — `price_quotes`, `price_quotes_raw`,
   `market_microstructure_signals`, `options_chain_v2`,
   `options_surface`, `social_posts`, `gdelt_macro_features`,
   `broker_fills`, `news_event_features`. These are the high-rate
   writers; cardinality scales with universe × cadence.
2. **Hot-read serving state** — `kill_switch_state`,
   `execution_mode`, `execution_health_state`,
   `broker_order_state`, `position_reconcile_baseline`,
   `model_feature_snapshots`, `strategy_allocations`,
   `alpha_decay_runtime_history`. Read on every decision tick.
3. **Append-only audit** — `kill_switch_audit`,
   `execution_mode_audit`, `execution_policy_audit`,
   `trade_attribution_ledger`, `position_reconcile_audit`,
   `promotion_statistical_evidence` (forthcoming),
   `decision_log` (forthcoming). Compliance-grade; must never lose
   rows.
4. **Analytical reads** — training jobs scan months of `prices`,
   `market_features`, `model_feature_snapshots`, joining against
   `model_registry`, `model_post_promo_results`, etc.

SQLite has carried this admirably as a single-process developer
runtime. It does not survive contact with production live trading at
the workload above without changes.

## 2. Where SQLite breaks under live trading

| Failure mode | Mechanism | Triggered by |
|---|---|---|
| **Writer contention** | SQLite WAL allows N readers + 1 writer. A live Polygon WS streamer writing `price_quotes_raw` competes with `decision_log`, `broker_fills`, `kill_switch_audit`, the UI's `runtime_metrics`, and every job's heartbeat. | Multi-symbol live streaming + decision pipeline + audit writers running in the same minute. |
| **Lock-stall propagating into the decision path** | A long `VACUUM` or `ALTER TABLE` blocks readers; the predictor stalls; the decision pipeline times out. | Schema migrations, retention deletes, manual maintenance. |
| **No native time-series indexing** | B-tree on `(symbol, ts)` works but range scans across months pull a lot of pages; analytical reads compete with hot writes. | Backtests, training jobs run during market hours. |
| **Single-file blast radius** | Disk corruption, ransomware, or a bad shutdown takes the whole system down. WAL helps with crash recovery, not with media failure. | Hardware fault, ungraceful host reboot. |
| **No replication** | Recovery from backup loses every transaction since the last snapshot. | Anything that requires restoring. RPO ≈ snapshot interval. |
| **No row-level access control** | Every process has full write authority on every table. | Defense in depth, especially around credentials. |
| **JSON columns are TEXT** | `decision_log.payload`, audit payloads, etc. are stored as serialized JSON; no GIN equivalent in SQLite, so any predicate inside a JSON blob is full-scan. | Investigations ("why did model X promote on date Y"). |
| **Single-machine ceiling** | Distributed training workers cannot share a SQLite file safely. Network filesystems break SQLite locking. | Horizontal-scale training. |
| **Retention is a write storm** | `DELETE FROM price_quotes_raw WHERE ts < ?` against a multi-GB table is slow and locks. | Disk pressure. |
| **Hot backup contends with writers** | `.backup` API works but doubles write cost during the snapshot window. | Compliance backups. |
| **No connection pooling** | SQLite uses one file handle per connection; concurrent jobs each open the DB. Fine until file-descriptor pressure. | Many subprocesses. |

The system is correct under SQLite. It will not be **available** under
SQLite at production cadence. Every one of the items above is fixable;
the question is the order.

## 3. Target architecture

Not "switch to Postgres." A trading system has four persistence
workloads that pull in different directions, and the right answer is a
tiered layer where each tier is good at one job and the boundaries are
explicit.

```
                  ┌──────────────────────────────────────────┐
                  │           Application processes          │
                  │ ingestion · decision · execution · UI    │
                  └──────────────────────────────────────────┘
                       │            │            │
                       │            │            │
              ┌────────▼─┐  ┌───────▼──────┐  ┌──▼──────────────┐
              │  Redis   │  │  Postgres 16 │  │   Object store  │
              │ hot work │  │  + Timescale │  │  (S3 / MinIO)   │
              │   set    │  │   extension  │  │                 │
              ├──────────┤  ├──────────────┤  ├─────────────────┤
              │ kill sw. │  │ OLTP core    │  │ Model artifacts │
              │ positions│  │ + hypertables│  │ Parquet history │
              │ risk st. │  │ + JSONB+GIN  │  │ DB snapshots    │
              │ rate lim.│  │ + WAL archive│  │ Large text blobs│
              └──────────┘  └──────────────┘  └─────────────────┘
                                  │
                                  │ logical replication
                                  ▼
                          ┌──────────────────┐
                          │ Read replica     │
                          │ + DuckDB/Parquet │
                          │ analytical layer │
                          └──────────────────┘
```

### 3.1 Postgres 16 + TimescaleDB — the durable backbone

- **Why Postgres**: ACID, replication, role separation, JSONB + GIN,
  partial indexes, mature ops tooling, every cloud has it managed.
- **Why TimescaleDB extension**: hypertables turn `price_quotes`,
  `market_features`, `decision_log`, `news_event_features`, and the
  audit ledgers into time-partitioned tables with native compression
  (typical 10–20× on tick data) and retention policies as one-line
  declarations. Continuous aggregates pre-roll 1-min → 5-min → 1-hour
  bars so dashboards do not scan raw ticks.
- **Concurrency**: many writers + many readers via MVCC, no single-
  writer ceiling. PgBouncer in front for connection pooling.
- **Audit columns become real**: `JSONB` payloads with GIN indexes on
  expression keys make "find all promotions where reason contained
  X" a millisecond query, not a full scan.

### 3.2 Redis — the hot working set

- **What lives here, and only here for live reads**: `kill_switch_state`,
  `execution_mode`, current per-symbol position, current per-symbol
  predicted weight, the rate-limit / throttle counters, the latest
  Monte-Carlo risk overlay snapshot, the open-order map.
- **Source of truth still in Postgres**: every Redis write is mirrored
  asynchronously to a Postgres audit table. Redis is the cache;
  Postgres is the ledger. If Redis loses memory, the system rebuilds
  from Postgres and pauses trading for the warm-up.
- **Why**: the decision path must not block on disk I/O. Redis
  delivers sub-millisecond reads, and the kill-switch read is on the
  *innermost* hot loop.
- **Flavor**: Redis 7 with AOF persistence + Sentinel for failover.
  Redis Cluster is overkill at solo-operator scale.

### 3.3 Object storage — model artifacts, large blobs, snapshots

- **Model files** (joblib pickles, PatchTST `state_dict`, FinBERT
  weights) belong on object storage, not in the database. Postgres
  stores the metadata + the S3 URI. Today they likely live on local
  disk; promote them to S3/MinIO/B2 with content-hash keys and
  immutable retention.
- **Raw text** (full SEC filings, full transcripts) belongs on object
  storage. Postgres holds the metadata, the embedding, and the URI.
- **Database backups**: daily `pg_basebackup` + continuous WAL archive
  to object storage delivers point-in-time recovery to any second in
  the retention window.

### 3.4 Read replica + Parquet analytical layer

- **Read replica** of Postgres for the dashboard and any human-driven
  analytics. Live decision pipeline never queries it; it never
  contends with the primary's writes.
- **Parquet snapshots** of completed-day data exported nightly to
  object storage. Backtests and training read Parquet via
  DuckDB/Polars, not the OLTP DB. This is the single biggest
  protection against an exploratory query taking down live trading.

### 3.5 Optional, defer until needed

- **Streaming buffer (Redpanda / NATS JetStream)** between high-rate
  ingestors and the database. Decouples burst writes; replayable. Add
  only if Postgres ingest becomes the bottleneck — at a single-
  operator scale it likely will not.
- **pgvector** on the Postgres instance if and when the NLP layer
  (prompt 08) wants similarity search over embeddings. Cheap to add
  later; do not preempt.
- **HashiCorp Vault / cloud KMS** for credential encryption keys.
  See §6.

## 4. Per-table-class treatment

The 210 tables fall into eight classes. Treatment is per-class, not
per-table.

| Class | Examples | Target store | Treatment |
|---|---|---|---|
| **Tick / quote stream** | `price_quotes_raw`, `price_quotes`, `market_microstructure_signals`, `broker_fills` | Timescale hypertable | Daily chunks; compression after 7d; retention raw 30d, 1-min bars 1y, 1-day bars indefinite. Continuous aggregates auto-roll. BRIN index on `ts`. |
| **Time-series features** | `market_features`, `news_event_features`, `news_symbol_features`, `options_event_features`, `social_features`, `gdelt_macro_features`, `model_feature_snapshots` | Timescale hypertable | Weekly chunks; compression after 30d; retention 3y. JSONB payload with GIN where searched. |
| **Audit / append-only** | `trade_attribution_ledger`, `kill_switch_audit`, `execution_mode_audit`, `execution_policy_audit`, `position_reconcile_audit`, `alert_acks`, `alert_resolutions`, `promotion_statistical_evidence` (new), `decision_log` (new) | Postgres regular table OR Timescale without compression | **No retention** unless legally allowed. Hash-chain column (sha256 of prior row + this row's payload) for tamper evidence. WAL-archived. |
| **Live operational state** | `kill_switch_state`, `execution_mode`, `execution_health_state`, `broker_order_state`, `position_reconcile_baseline`, `strategy_allocations` | **Redis primary read**, Postgres write-through ledger | Redis for the hot read; every change emits an audit row to Postgres in the same transaction (outbox pattern). |
| **Registry / config** | `model_registry`, `strategy_registry`, `sleeve_registry`, `data_sources`, `domain_blacklist`, `domain_perf` | Postgres regular table | Replicated. Modest size, high read frequency. |
| **Job state** | `job_history`, `runtime_meta`, `runtime_metrics`, `event_log`, `event_log_state`, `ipc_messages` | Postgres regular table; runtime_metrics → Timescale | Job history retention 90d. Metrics → Timescale with 1d compression and 1y retention. |
| **Large blobs / artifacts** | model files, raw transcript text, raw SEC filing text, news article bodies | Object storage (S3/MinIO/B2) | Postgres holds metadata + URI + content-hash. Immutable bucket policy. Lifecycle rules for tiering to cold storage. |
| **Encrypted secrets** | `data_sources` (broker API keys, data-provider keys) | Postgres + KMS | Move encryption key out of code (`services/credential_encryption.py`) into a real KMS or Vault. Rotate quarterly. Audit every read. |

### 4.1 Hot-path tables that need explicit Redis caching

These are queried inside the decision pipeline. Every decision tick
that reads them from disk is a tax. Cache them.

- `kill_switch_state` — read on every order intent.
- `execution_mode` — read on every order intent.
- `execution_health_state` — read on every order intent.
- `position_reconcile_baseline` — read on every pre-trade gate.
- `strategy_allocations` — read on every portfolio rebuild.
- `broker_order_state` — read on every router decision.
- Per-symbol latest `model_feature_snapshots` row — read on every
  prediction.

Pattern: Redis is the read source; Postgres is the write source;
write-through with idempotent keys; cache invalidation on write; TTL
of 60 s as a defensive backstop against missed invalidations.

### 4.2 Tables that should be promoted to hypertables on day one

Pure time-series, high-write, range-queried by `ts`:

- `price_quotes_raw`, `price_quotes`, `market_microstructure_signals`,
  `price_anomalies`
- `options_chain_v2`, `options_chain`, `options_surface`,
  `options_surface_agg`
- `market_features`, `model_feature_snapshots`,
  `news_event_features`, `news_symbol_features`,
  `options_event_features`, `options_symbol_features`,
  `social_features`, `social_posts`, `gdelt_macro_features`
- `broker_fills`, `runtime_metrics`, `event_log`, `ingest_slippage`,
  `ingestion_pipeline_health`, `price_provider_health`

For each: create as a hypertable with weekly chunks; add a compression
policy after 30 days targeting 10×; add a retention policy keyed to
the table's role (see §4 table).

### 4.3 Tables to leave on Postgres regular (no Timescale)

OLTP-shaped state with small footprint and update-in-place semantics:

- All `*_registry` tables.
- All `*_state` tables (these are caches in front of audit ledgers).
- `data_sources`, `domain_blacklist`, `domain_perf`.
- `schema_version`, `runtime_meta`, `execution_meta`.

## 5. Migration roadmap (low-risk, phased)

The goal is to never break live trading. Each phase is reversible
until cutover. **No phase requires the next**; you can stop after any
of them.

### Phase 0 — Measurement (1 week)

- Instrument current writes per table per minute (a `pragma_busy_handler`
  tap or a wrapping `executemany` decorator). Output a CSV of
  rows/minute per table. **Decisions follow data; do not skip this.**
- Define explicit RTO and RPO. Suggested starting point for live
  capital: RTO ≤ 15 min, RPO ≤ 1 min.
- Snapshot DB size; project growth at 6 months and 24 months.

### Phase 1 — Storage abstraction (completed in current runtime)

- The public facade remains `engine/runtime/storage.py`.
- Production-like operation routes to `engine/runtime/storage_pg.py`.
- Isolated Python tests route to `engine/runtime/storage_sqlite.py` through `TS_STORAGE_BACKEND=sqlite` or `TS_TESTING=1`.
- `DB_PATH` is no longer a production database file path; it is a local data-root/legacy compatibility hint.

### Phase 2 — Stand up Postgres + Timescale (1 week)

- Provision one Postgres 16 + Timescale 2 instance. Managed (Aiven,
  Timescale Cloud, RDS) is fine for one operator. Self-host on a
  modest VM is also fine.
- Apply schema migrations for the regular tables.
- Apply hypertable conversions for the time-series tables (§4.2).
- Enable WAL archiving to object storage.
- Configure `pg_stat_statements`, set up basic monitoring.

### Phase 3 — Dual-write window (2–3 weeks)

- Each writer commits to SQLite as today and to Postgres in parallel.
- A reconciler job hourly diffs row counts per table; alerts on drift.
- All reads still come from SQLite. **No production behavior change.**
- Goal: prove Postgres can sustain the write load and that the schemas
  are equivalent.

### Phase 4 — Read cutover (1 week)

- Flip the storage abstraction to `postgres` for non-hot reads first
  (training jobs, dashboard, backfills).
- Keep SQLite serving hot reads for one more week.
- Watch error rates, p99 latencies, and the reconciler.

### Phase 5 — Full cutover + Redis (1 week)

- Stand up Redis with AOF + Sentinel.
- Wire write-through cache for the §4.1 hot-path tables.
- Flip hot reads to Redis. Audit writes still go to Postgres.
- SQLite continues to receive shadow writes for one more week as a
  rollback safety net.

### Phase 6 — Decommission shadow SQLite (1 day)

- Stop SQLite writes.
- Archive the final SQLite file to object storage.
- Remove the dual-write code path.

### Phase 7 — Object storage for artifacts (parallel to phases 4–6)

- Move model files out of local disk / DB blobs into object storage,
  keyed by content hash.
- Move raw text bodies (filings, transcripts, full news articles)
  to object storage; Postgres keeps metadata + URI + embedding.

### Phase 8 — Replica + analytical Parquet (week 8)

- Stand up a logical-replication read replica.
- Point the dashboard and any human analytics at the replica.
- Add a nightly Parquet export for completed-day data; backtests and
  training read Parquet via DuckDB.

### Phase 9 — Operational hardening (continuous)

- Quarterly restore drill: restore production from object storage to
  a clean instance, verify row counts and a smoke trade simulation.
- Quarterly key rotation through KMS / Vault.
- Annual DR exercise: stand up the full system in a different region
  / on a different host from cold backups.

**Estimated calendar time end-to-end**: 8–10 weeks at half-time, with
no live-trading downtime. Each phase has its own rollback path because
SQLite stays warm until Phase 6.

## 6. Operational additions

### 6.1 Backups and recovery

- Continuous WAL archive to object storage (point-in-time recovery).
- Nightly base backup retained 30 days, weekly retained 1 year,
  annual retained forever (or as policy requires).
- Quarterly restore drill into a clean instance — restoration is only
  proven when you have actually done it.
- In this repository, production-style backup and restore ownership lives under `ops/backup/` and `ops/server/systemd/`. The older `deploy/bin/backup_trading_db.sh` is a SQLite-file backup helper and is not sufficient for the current Postgres-backed runtime.

### 6.2 Secrets management

The current path keeps the encryption key for `data_sources` accessible
to the application process. For production:

- Move the master key into a real secret manager (HashiCorp Vault,
  AWS KMS, GCP KMS, 1Password Connect at minimum).
- Application processes obtain a short-lived data key via the secret
  manager's API and re-fetch on rotation.
- Audit log of every secret read goes to its own append-only table.
- Rotate provider API keys quarterly through the secret manager,
  not by editing code.

### 6.3 Role separation

Three Postgres roles, each with the minimum privileges they need:

- `ts_ingest` — INSERT on time-series tables, no DELETE.
- `ts_app` — full CRUD on operational state, INSERT-only on audit
  ledgers.
- `ts_reader` — SELECT only; used by dashboard, backtest, training.

Connection strings carry the right role; no service uses superuser.

### 6.4 Observability

- `pg_stat_statements` for top-N slow queries.
- Per-table write-rate dashboard (basic Grafana on top of
  `pg_stat_user_tables`).
- Long-running-transaction alert (anything > 60 s).
- Replication lag alert on the read replica.
- Object-storage upload-failure alert for WAL archive.

### 6.5 Schema migrations at scale

- Every migration goes through the file in
  `engine/runtime/schema/migrations.py` (created by prompt 04).
- Migrations are reviewed and applied in CI against a clone of
  production *before* they ever touch the live DB.
- For unavoidable big migrations on hypertables, use Timescale's
  `decompress_chunk → ALTER → recompress_chunk` pattern; never
  `ALTER TABLE` a multi-GB hot table directly.

### 6.6 Tamper-evident audit

For the audit-class tables, add a `prev_hash TEXT` column and a
`row_hash TEXT` column computed as
`sha256(prev_hash || canonical_json(row_minus_hashes))`. Application
writes both. A periodic verifier walks the chain and alerts on
divergence. Cheap, and the right hygiene for a system that may face
review.

## 7. What to leave alone

- **The 200+ env-var configuration model.** Out of scope here; prompt
  03 handles it.
- **The model-promotion logic, the risk gates, the execution slicing.**
  Persistence change must be invisible to them.
- **The `engine.runtime.storage` module's public API.** It is a fine
  facade. The work in prompt 04 keeps the API; this plan reuses it.
- **SQLite for isolated Python tests.** Local test runs use SQLite by default to avoid probing ambient PgBouncer/Postgres. Developer and production-like runtime behavior should be validated against the Postgres facade when storage availability matters.

## 8. Cost and sizing — order of magnitude

For a single-operator universe of ~500 symbols at 1-second bars and
the current ingestion mix:

- **Postgres + Timescale**: a single 4 vCPU / 16 GB / 500 GB managed
  instance is plenty for the first year. Approximate cost USD
  150–300/month managed, USD 30–60/month self-hosted.
- **Redis**: 1 GB managed instance, USD 15–30/month, or free
  self-hosted.
- **Object storage**: model artifacts + raw text + DB backups will
  comfortably fit under 100 GB year one. USD 2–5/month.
- **Read replica**: same shape as primary; double the Postgres bill
  if managed. Optional until you have a heavy dashboard or analytics
  user.

The all-in incremental run-rate, even managed end-to-end, is
< USD 500/month for a system that can take live capital. The bulk of
the work is the migration, not the ongoing cost.

## 9. Decision summary

Concrete recommendations, in priority order:

1. **Land Codex prompt 04** (storage abstraction). It is a hard
   prerequisite for everything below.
2. **Stand up Postgres 16 + TimescaleDB** and dual-write for two
   weeks before reading. Earn confidence.
3. **Cut hot-path reads to Redis** for the seven tables in §4.1.
   This is the single biggest decision-pipeline-latency win.
4. **Move model artifacts and raw text to object storage.** Pulls
   tens of GB of binary out of the OLTP store.
5. **Stand up a read replica + Parquet snapshot pipeline** so
   training and dashboards never contend with live writes.
6. **Move the credential encryption key to a real KMS / Vault.**
   Cheap, important, often deferred.
7. **Add tamper-evident hash chains to the audit-class tables.**
   Two columns, a verifier job, and your audit story is institutional.
8. **Skip the streaming buffer (Kafka/Redpanda).** At one-operator
   scale, Postgres handles the write rate. Add it only if
   `pg_stat_user_tables` shows ingest backpressure.
9. **Skip pgvector until prompt 08 ships.** It is one extension
   `CREATE EXTENSION` away when you need it.
10. **Make restore a quarterly drill.** Backups you have not restored
    are theatre.

---

The system is well-architected for what it is — a sole-operator
research runtime that has accreted production-grade discipline in the
last year. The persistence layer is the last big lift before live
capital. Done in the order above, it is 8–10 weeks of part-time work
with zero downtime and a clear rollback at every phase.
