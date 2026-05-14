# Storage Migration Backlog

This document is the execution backlog for moving the repository's hot, append-heavy data paths from the main SQLite database to the existing Postgres or Timescale sidecars, while keeping SQLite as the control-plane store.

This is a supplementary planning document. It does not override the current storage contracts documented in [README_DATABASE_MAP.md](README_DATABASE_MAP.md), [ARCHITECTURE.md](ARCHITECTURE.md), or the runtime code.

## 1. Goals

Primary goals:

- Remove lock-contention pressure from the main SQLite file.
- Keep cold boot and startup validation safe while the migration is in progress.
- Cut over writes first, then reads, then retire obsolete SQLite writes only after parity and health gates pass.
- Keep every phase reversible by environment flags until the final retirement step.

Non-goals:

- Do not remove SQLite from the repo wholesale.
- Do not migrate low-rate control-plane tables that are working acceptably in SQLite today.
- Do not change startup or storage contracts without adding matching validation coverage.

## 2. Storage Split

### Stays on SQLite

These tables are the control-plane store and should remain SQLite unless a separate program is approved:

- `runtime_meta`
- `schema_version`
- `job_checkpoints`
- `alerts`
- `decision_log`
- `portfolio_state`
- `portfolio_orders`
- `execution_orders`
- `execution_fills`
- `pnl_attribution`
- other low-rate operator, governance, and transactional state tables that are not top contention sources

### Stays on Separate SQLite Liveness DB

These should remain isolated from the main SQLite file:

- `job_locks`
- `job_heartbeats`

### Moves to Postgres or Timescale Primary

These are the hot append-heavy paths that should stop depending on the main SQLite file:

- `prices`
- `price_quotes`
- `price_quotes_raw`
- `runtime_metrics`
- `event_log`
- `ingestion_pipeline_health`
- `price_provider_health`
- `weather_provider_health`
- `data_source_logs`
- `feature_data`
- `model_predictions`
- `trade_outcomes`

### Existing Migration Building Blocks

The codebase already has the primitives needed for staged cutover:

- Async price writer: [engine/runtime/async_writer.py](../engine/runtime/async_writer.py)
- Postgres or Timescale price storage: [engine/runtime/storage_pg_prices.py](../engine/runtime/storage_pg_prices.py)
- Price parity validation: [engine/runtime/price_migration_validation.py](../engine/runtime/price_migration_validation.py)
- Price read router: [engine/runtime/price_read_router.py](../engine/runtime/price_read_router.py)
- Telemetry Timescale client: [engine/runtime/timescale_client.py](../engine/runtime/timescale_client.py)
- Telemetry parity validation: [engine/runtime/telemetry_migration_validation.py](../engine/runtime/telemetry_migration_validation.py)
- Telemetry read router: [engine/runtime/telemetry_read_router.py](../engine/runtime/telemetry_read_router.py)
- Hybrid feature-store write mode: [engine/data/feature_store.py](../engine/data/feature_store.py)

## 3. Phase Rules

Every migration phase must satisfy the same operating rules.

### Entry Gate

Before starting a new phase:

- Cold boot must pass on a fresh database.
- Startup validation must exercise the real repair and bootstrap path.
- No open high-severity storage bug may be ignored as "unrelated" if it touches the same tables or writer path.

Minimum required commands:

```powershell
python tools/runtime_graph_check.py --mode startup
python -m pytest tests/test_db_repair.py -q -k repair_startup_fast_path_bootstraps_fresh_database
python -m pytest tests/test_runtime_graph_check.py -q
```

### PR Bundle Rule

Every migration PR should include:

1. Code changes for exactly one bounded slice.
2. A validation update or test proving the new path.
3. A rollback path expressed as env flags or router fallback.
4. Evidence showing which old path and new path were exercised.

### Exit Gate

A phase is complete only when all of the following are true:

- Static completeness audit passes.
- Phase-specific tests are green.
- Startup validation is green.
- Runtime parity or health gates are green for the configured burn-in window.
- SQLite contention metrics for the migrated tables improve or disappear.

### Rollback Trigger

Roll back a phase immediately if any of the following appears during burn-in:

- cold boot or startup validation fails
- parity validators report sustained drift beyond the configured bounds
- read routers fall back unexpectedly for the cutover tables
- async queues or Timescale writers grow without draining
- SQLite remains a top contention source for the tables that were supposed to move

## 4. Cross-Phase Audits

These audits must exist before any read cutover.

### A. Static Write-Path Audit

Create `tools/storage_route_audit.py` to scan for direct writes against scoped hot tables.

It should flag:

- `run_write_txn(...)` calls targeting migrated tables outside approved wrappers
- direct `connect_rw_direct()` and `connect()` writes against migrated tables
- raw SQL `INSERT`, `UPDATE`, `DELETE`, `REPLACE`, `ALTER`, or `CREATE` on migrated tables outside owner modules

Initial approved owner modules:

- `engine/runtime/async_writer.py`
- `engine/runtime/data_source_log_store.py`
- `engine/runtime/event_log.py`
- `engine/runtime/metrics_store.py`
- `engine/runtime/storage_pg_prices.py`
- `engine/runtime/timescale_client.py`
- `engine/runtime/telemetry_append_buffer.py`
- `engine/runtime/price_router.py`
- `engine/runtime/price_migration_validation.py`
- `engine/runtime/telemetry_migration_validation.py`
- explicit storage-owned schema helpers in `engine/runtime/storage.py`

The audit tool should run in CI before any read-cutover phase is allowed to start.

### B. Runtime Contention Audit

Capture SQLite trace snapshots before and after each phase using the counters already exposed from [engine/runtime/storage.py](../engine/runtime/storage.py).

Track at least:

- `lock_error_count`
- `busy_retry_count`
- `max_lock_wait_ms`
- `top_contention_paths`
- `top_write_tables`

Required outcome:

- migrated tables stop showing up as persistent top contention sources

### C. Parity Audit

No read cutover is allowed without parity validation.

Current validators:

- Prices: [engine/runtime/price_migration_validation.py](../engine/runtime/price_migration_validation.py)
- Telemetry: [engine/runtime/telemetry_migration_validation.py](../engine/runtime/telemetry_migration_validation.py)

Every new migrated dataset family must either:

- extend one of the existing parity validators, or
- add a new validator and read router with equivalent health gates

## 5. Phase 0: Baseline And Inventory

Objective:

- produce the authoritative writer and reader inventory for all hot data paths

Work items:

- `SM-000` Build `tools/storage_route_audit.py`.
  Owner: `engine/runtime/`
  Scope: new audit tool plus a small contract test
- `SM-001` Produce a checked-in migration matrix for hot tables, current writer modules, target backend, read path, fallback mode, and validator owner.
  Owner: `docs/`
  Scope: this document plus a small table ledger update as the migration advances
- `SM-002` Capture a baseline SQLite contention snapshot under representative startup and ingest load.
  Owner: `engine/runtime/`
  Scope: documented procedure and baseline artifact location

Validation bundle:

```powershell
python tools/runtime_graph_check.py --mode startup
python -m pytest tests/test_db_repair.py -q
python -m pytest tests/test_runtime_graph_check.py -q
```

Exit criteria:

- every target table has a documented current writer, current reader, target writer, target reader, validator, and fallback path

## 6. Phase 1: Writer Normalization

Objective:

- eliminate unmanaged direct writes to the tables targeted for migration

Work items:

- `SM-100` Route remaining price writes through `price_router` and the async writer path where supported.
  Owner: `engine/runtime/`, `engine/data/`
- `SM-101` Route telemetry append workloads through `telemetry_append_buffer` or `timescale_client`, not direct hot-loop SQLite writes.
  Owner: `engine/runtime/`
- `SM-102` Convert non-critical runtime status writes in hot loops to best-effort or buffered writes.
  Owner: `engine/runtime/`, `engine/data/`, `engine/jobs/`
- `SM-103` Expand static regression coverage for hot-path best-effort behavior.
  Owner: `tests/`

Key files:

- [engine/runtime/price_router.py](../engine/runtime/price_router.py)
- [engine/runtime/telemetry_append_buffer.py](../engine/runtime/telemetry_append_buffer.py)
- [engine/runtime/runtime_meta.py](../engine/runtime/runtime_meta.py)
- [tests/test_sqlite_contention_relief.py](../tests/test_sqlite_contention_relief.py)

Validation bundle:

```powershell
python -m pytest tests/test_sqlite_contention_relief.py -q
python -m pytest tests/test_async_price_writer.py -q
python tools/runtime_graph_check.py --mode startup
```

Exit criteria:

- `tools/storage_route_audit.py` reports zero unmanaged direct write paths for scoped hot tables
- Current state: satisfied; the route-audit baseline is empty and Phase 1 validation is green

## 7. Phase 2: Telemetry Dual-Write

Objective:

- make Timescale the write sidecar for telemetry without changing telemetry reads yet

Scope:

- `runtime_metrics`
- `event_log`
- `ingestion_pipeline_health`
- `price_provider_health`
- `weather_provider_health`
- `data_source_logs`

Work items:

- `SM-200` Enable and harden Timescale telemetry writes in [engine/runtime/timescale_client.py](../engine/runtime/timescale_client.py).
  Owner: `engine/runtime/`
- `SM-201` Verify after-commit hooks and buffer flush behavior for telemetry families.
  Owner: `engine/runtime/`, `tests/`
- `SM-202` Add health and observability surfaces for queue depth, flush failures, and degraded reasons.
  Owner: `engine/runtime/`

Suggested burn-in configuration:

- `TIMESCALE_ENABLED=1`
- `TIMESCALE_DSN=...`
- `TIMESCALE_TELEMETRY_MIRROR_ENABLED=1`
- `TELEMETRY_READ_BACKEND=sqlite`
- `TELEMETRY_READ_REQUIRE_VALIDATION=1`

Validation bundle:

```powershell
python -m pytest tests/test_timescale_client_storage_gates.py -q
python -m pytest tests/test_telemetry_read_routing.py -q
python tools/runtime_graph_check.py --mode startup
python tools/compare_timescale_telemetry_dual_write.py --strict --require-healthy-mirror --require-healthy-timescale --lookback-minutes 15 --json
```

Exit criteria:

- telemetry writes succeed with SQLite still serving reads
- no regression in cold boot or startup validation
- strict telemetry parity gate stays green for the burn-in window when telemetry Timescale migration is enabled

## 8. Phase 3: Telemetry Read Cutover

Objective:

- switch telemetry reads to Timescale only after parity and health gates pass

Work items:

- `SM-300` Burn in telemetry parity validation using [engine/runtime/telemetry_migration_validation.py](../engine/runtime/telemetry_migration_validation.py).
  Owner: `engine/runtime/`
- `SM-301` Move telemetry consumers to router helpers where direct SQLite reads remain.
  Owner: `engine/runtime/`, `engine/api/`, `services/`
- `SM-302` Switch telemetry read mode to `auto`, leaving SQLite fallback enabled.
  Owner: `ops/`, `deploy/`

Validation bundle:

```powershell
python -m pytest tests/test_telemetry_read_routing.py -q
python tools/runtime_graph_check.py --mode startup
```

Exit criteria:

- `get_telemetry_read_backend()` resolves to `timescale` in burn-in environments
- router fallback events are zero or explicitly explained
- telemetry parity stays within configured count and lag bounds

## 9. Phase 4: Price Dual-Write

Objective:

- make the async writer plus Postgres or Timescale storage primary for price-side writes while SQLite remains the read source

Scope:

- `prices`
- `price_quotes`
- `price_quotes_raw`

Work items:

- `SM-400` Turn on [engine/runtime/async_writer.py](../engine/runtime/async_writer.py) in controlled environments.
  Owner: `engine/runtime/`
- `SM-401` Harden [engine/runtime/storage_pg_prices.py](../engine/runtime/storage_pg_prices.py) schema, retries, and observability.
  Owner: `engine/runtime/`
- `SM-402` Ensure all price ingress paths publish through [engine/runtime/price_router.py](../engine/runtime/price_router.py) and not ad hoc SQLite writes.
  Owner: `engine/runtime/`, `engine/data/`, `engine/jobs/`
- `SM-403` Keep SQLite price writes enabled during burn-in to preserve fallback and parity checks.
  Owner: `ops/`, `deploy/`

Suggested burn-in configuration:

- `TIMESCALE_PRICES_ENABLED=1`
- `TIMESCALE_PRICES_DSN=...`
- `ASYNC_PRICE_WRITER_ENABLED=1`
- `PRICE_ROUTER_REQUIRE_ASYNC_DURING_CUTOVER=1`
- `PRICE_ROUTER_SQLITE_WRITE_ENABLED=1`
- `PRICE_READ_BACKEND=sqlite`
- `PRICE_READ_REQUIRE_VALIDATION=1`

Validation bundle:

```powershell
python -m pytest tests/test_async_price_writer.py -q
python -m pytest tests/test_timescale_integration_hooks.py -q
python -m pytest tests/test_price_migration_validation.py -q
python tools/runtime_graph_check.py --mode startup
```

Exit criteria:

- async writer queue depth is stable
- PG price storage is healthy
- parity validator reports price, quote, and raw counts within bounds

## 10. Phase 5: Price Read Cutover

Objective:

- switch price reads to Timescale via the read router after parity succeeds

Work items:

- `SM-500` Finish migrating dashboard and API readers to [engine/runtime/price_read_router.py](../engine/runtime/price_read_router.py).
  Owner: `engine/api/`, `engine/runtime/`
- `SM-501` Burn in `PRICE_READ_BACKEND=auto` with fallback still enabled.
  Owner: `ops/`, `deploy/`
- `SM-502` Alert on any router fallback to SQLite during the burn-in window.
  Owner: `engine/runtime/`, `ops/`

Validation bundle:

```powershell
python -m pytest tests/test_price_migration_validation.py -q
python tools/runtime_graph_check.py --mode startup
```

Exit criteria:

- `get_price_read_backend()` resolves to `timescale` in the target environment
- no unexplained SQLite fallbacks occur during burn-in
- health snapshot exposes green `price_migration_validation`

## 11. Phase 6: Feature, Prediction, And Trade-Outcome Analytics

Objective:

- move analytical append-only datasets off SQLite where dual-write hooks already exist

Scope:

- `feature_data`
- `model_predictions`
- `trade_outcomes`

Work items:

- `SM-600` Promote Timescale writes for model predictions via existing after-commit hooks.
  Owner: `engine/strategy/`, `engine/runtime/`
- `SM-601` Promote Timescale writes for feature snapshots and market features.
  Owner: `engine/strategy/`, `engine/data/`
- `SM-602` Promote Timescale writes for trade outcomes.
  Owner: `engine/execution/`, `engine/runtime/`
- `SM-603` Decide which analytical reads should move first and which may continue to use SQLite as cache or fallback.
  Owner: `engine/strategy/`, `engine/execution/`, `engine/api/`

Validation bundle:

```powershell
python -m pytest tests/test_timescale_integration_hooks.py -q
python tools/runtime_graph_check.py --mode startup
```

Exit criteria:

- the analytical hooks are exercised only after commit
- readers for these datasets are explicit and documented

## 12. Phase 7: Retire Obsolete SQLite Hot Writes

Objective:

- stop writing migrated hot tables to the main SQLite file after the new primary backends have proven stable

Work items:

- `SM-700` Disable SQLite writes for telemetry families where Timescale is primary and validated.
  Owner: `engine/runtime/`, `ops/`
- `SM-701` Disable SQLite writes for price families after the price router and validators remain green.
  Owner: `engine/runtime/`, `ops/`
- `SM-702` Update canonical docs to describe SQLite as control-plane storage and Timescale as primary for migrated hot paths.
  Owner: `docs/`

Validation bundle:

```powershell
python tools/runtime_graph_check.py --mode startup
python -m pytest tests/test_price_migration_validation.py -q
python -m pytest tests/test_telemetry_read_routing.py -q
```

Exit criteria:

- migrated tables no longer appear as top SQLite contention sources
- SQLite remains the control-plane database only

## 13. Backlog Ledger

Use this table to track execution status as work starts.

| ID | Phase | Task | Status | Owner Area | Validation |
| --- | --- | --- | --- | --- | --- |
| `SM-000` | 0 | Build static storage route audit tool | Complete | `engine/runtime` | audit tool contract test |
| `SM-001` | 0 | Publish hot-table migration matrix | Planned | `docs` | doc review |
| `SM-002` | 0 | Capture SQLite contention baseline | Planned | `engine/runtime` | trace snapshot evidence |
| `SM-100` | 1 | Normalize price writers behind approved paths | Complete | `engine/runtime`, `engine/data` | `test_async_price_writer`, route audit |
| `SM-101` | 1 | Normalize telemetry writers behind approved paths | Complete | `engine/runtime` | `test_telemetry_read_routing`, route audit |
| `SM-102` | 1 | Convert hot non-critical status writes to best-effort/buffered | Complete | `engine/runtime`, `engine/data`, `engine/jobs` | `test_sqlite_contention_relief` |
| `SM-103` | 1 | Expand hot-path static regression coverage | Complete | `tests` | targeted pytest |
| `SM-200` | 2 | Turn on telemetry dual-write | Planned | `engine/runtime` | `test_timescale_client_storage_gates` |
| `SM-300` | 3 | Burn in telemetry parity and read cutover | Planned | `engine/runtime`, `ops` | telemetry router and parity tests |
| `SM-400` | 4 | Turn on price dual-write via async writer | Planned | `engine/runtime` | async writer and price parity tests |
| `SM-500` | 5 | Move price reads to router-controlled Timescale cutover | Planned | `engine/runtime`, `engine/api`, `ops` | price router and parity tests |
| `SM-600` | 6 | Promote analytical Timescale hooks | Planned | `engine/strategy`, `engine/execution` | `test_timescale_integration_hooks` |
| `SM-700` | 7 | Retire obsolete SQLite hot writes | Planned | `engine/runtime`, `ops`, `docs` | startup and parity suites |

## 14. Definition Of Done

The migration is complete only when all of the following are true:

- cold boot and startup validation remain green
- SQLite is no longer the write bottleneck for prices and telemetry
- read cutovers are controlled by validators and routers, not manual guesswork
- Timescale or Postgres is primary for the designated hot data families
- SQLite remains the control-plane store with an isolated liveness DB
- canonical docs are updated to describe the final split accurately
