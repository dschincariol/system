# Runtime Subsystem

The `engine/runtime/` package is the control plane of the trading system. It owns:

- boot validation
- lifecycle state
- DB access and coordination
- job registration and process management
- startup orchestration
- ingestion supervision
- health and diagnostics

If the system starts, stops, hangs, restarts, deadlocks, or corrupts state, the root cause is often here.

## Core Files

- [storage.py](storage.py)
  Public runtime storage facade. It selects exactly one backend, validates the `StorageBackend` contract before exposing backend symbols, and rejects [storage_sqlite.py](storage_sqlite.py) in real supervised/prod/live processes.
- [storage_pg.py](storage_pg.py)
  Postgres-backed runtime storage implementation, connection-pool integration, SQLite-shaped compatibility cursor helpers, schema migration entrypoints, validation snapshots, and degraded-storage reporting.
- [storage_sqlite.py](storage_sqlite.py)
  Test-only SQLite backend. Its schema bootstrap has a single reachable `_base_schema()` path, and compatibility helpers now call through a locked `storage_pg` helper wrapper instead of cloning runtime function code.
  This is a bounded first slice of the storage-backend rearchitecture: `_PG_COMPAT_HELPER_NAMES` is still a documented migration shim, not the final backend-neutral repository layer.
  `tools/validate_repo.py` enforces that this shim cannot reintroduce code-object cloning or import `storage_pg` outside the compatibility loader.
- [locks.py](locks.py)
  Cross-process job locks and `job_history` persistence.
- [runtime_meta.py](runtime_meta.py)
  Shared metadata store used for diagnostics and boot progress.
- [job_registry.py](job_registry.py)
  Canonical registry of runnable jobs and pipeline order.
- [job_catalog.py](job_catalog.py)
  Operator-facing job catalog serializer and backend job-action safety policy derived from the canonical registry.
- [jobs_manager.py](jobs_manager.py)
  Starts and stops jobs, owns their subprocesses, logs, heartbeats, and restart behavior.
- [supervisor.py](supervisor.py)
  Higher-level dependency-aware job starting and graph validation.
- [startup_orchestrator.py](startup_orchestrator.py)
  Post-bind startup flow that seeds sources and the early pipeline.
- [ingestion_runtime.py](ingestion_runtime.py)
  Supervises market-data/feed children and provider-state recovery.
- [lifecycle_state.py](lifecycle_state.py)
  Persistent runtime lifecycle state machine.
- [health.py](health.py)
  Registry-driven health snapshots and preflight checks used by UI and bootstrap. `get_health_snapshot()` owns cache/lock/connection handling and runs named `HealthSnapshotCheck` probes before the final readiness aggregation.
- [hardware.py](hardware.py)
  CPU-first device/profile resolver, bounded torch/BLAS thread defaults, runtime hardware snapshots, and NVIDIA telemetry gating.
- [live_trading_preflight.py](live_trading_preflight.py)
  Central fail-closed live deployment contract: execution mode, dashboard token, confirmation phrase, broker environment, startup broker preflight, initial kill-switch hold, backup evidence, pre-live reconciliation, and execution arming audit.
- [live_execution_control.py](live_execution_control.py)
  Shared emergency live-capital controls for `DISABLE_LIVE_EXECUTION` and pre-live reconciliation break-glass policy.
- [backup_evidence.py](backup_evidence.py)
  Backup, WAL archive, and restore-drill evidence freshness checks used by production preflight and live trading preflight.
- [ingestion_status.py](ingestion_status.py)
  Per-pipeline ingestion health snapshots, counters, freshness, and summaries consumed by diagnostics and provider monitoring.
- [alerts.py](alerts.py)
  Alert persistence, thresholding, dedupe, and alert-side event publishing.
- [price_router.py](price_router.py)
  Runtime-owned feed selection and fallback routing used by newer price-provider control surfaces.
- [position_store.py](position_store.py)
  Shared persistence helpers for broker positions and position-facing read models.
- [trade_lifecycle.py](trade_lifecycle.py)
  Cross-table trace builder that reconstructs the alert-to-order-to-fill lifecycle for audits and regressions.
- [allocator_status.py](allocator_status.py)
  Read-side snapshot builder for allocator freshness, capacity, and status diagnostics.
- [crash_recovery.py](crash_recovery.py)
  Broker/execution recovery helpers that reconstruct submit/fill state after a restart.
- [runtime_bootstrap.py](runtime_bootstrap.py)
  Idempotent bootstrap helpers for schema, startup state, and fail-fast storage initialization.
- [first_run.py](first_run.py)
  Initial schema/bootstrap path.
- [db_repair.py](db_repair.py)
  Defensive schema/data repair helpers.
- [event_log.py](event_log.py)
  Runtime event persistence.
- [event_bus.py](event_bus.py)
  Internal pub/sub and event fanout.
- [event_runtime.py](event_runtime.py)
  Event-driven bridge that reacts to price ticks, predictions, and strategy signals without coupling those domains directly.
- [ipc.py](ipc.py)
  Cross-process IPC used by ingestion and other supervised children.
- [observability.py](observability.py)
  Shared component-health and rolling-rate helpers used by async sidecars and long-lived services.
- [async_writer.py](async_writer.py)
  Durable SQLite WAL spool consumer that offloads append-heavy price persistence from the hot path.
- [async_writer_spool.py](async_writer_spool.py)
  Bounded embedded SQLite WAL spool for async price-write envelopes. It stores rows before enqueue returns and exposes replay/backpressure/corruption stats.
- [storage_pg_prices.py](storage_pg_prices.py)
  Postgres/Timescale-oriented price persistence sidecar for append-heavy market data.
- [timescale_client.py](timescale_client.py)
  Background TimescaleDB client for hypertable writes and schema management on newer time-series surfaces.
- [model_cache.py](model_cache.py)
  In-memory catalog cache used by serving and governance readers to avoid repeated registry scans.
- [price_cache.py](price_cache.py)
  Runtime-facing wrapper around the data price cache that exposes health-oriented accessors to other subsystems.
- [shutdown.py](shutdown.py)
  Graceful runtime stop, event logging, pooled-connection cleanup, and WAL checkpoint handling used by server shutdown paths.

## Newer Sidecars And Services

- Async persistence:
  [async_writer.py](async_writer.py),
  [async_writer_spool.py](async_writer_spool.py),
  [storage_pg_prices.py](storage_pg_prices.py), and
  [timescale_client.py](timescale_client.py).
- Event-driven orchestration:
  [event_bus.py](event_bus.py),
  [event_log.py](event_log.py), and
  [event_runtime.py](event_runtime.py).
- Observability and caches:
  [observability.py](observability.py),
  [model_cache.py](model_cache.py), and
  [price_cache.py](price_cache.py).

## Maintenance Guidance

- When changing startup behavior, review:
  [start_system.py](../../start_system.py), [dashboard_server.py](../../dashboard_server.py), [startup_orchestrator.py](startup_orchestrator.py), and [ingestion_runtime.py](ingestion_runtime.py) together.
- When changing job semantics, update:
  [job_registry.py](job_registry.py), [job_catalog.py](job_catalog.py), [jobs_manager.py](jobs_manager.py), and this README.
- Keep job safety authoritative in runtime/API code.
  Browser surfaces consume the catalog's `safety`, `prerequisites`, and `action_policy` fields; they do not decide whether an execution-sensitive, destructive/admin, or unavailable job may start.
- Keep blocking DB work out of hot control-plane paths.
  Startup and job start behavior are sensitive to blocking Postgres acquisition, schema validation, and migration work. Hot append-heavy paths should use the buffered/router surfaces instead of doing ad hoc synchronous writes.
- Keep lock naming consistent.
  One-shot jobs use `job:<name>` lock names; runtime lock mismatches cause startup failures and restart loops.
- Treat new sidecars as optional but observable.
  Async persistence, Timescale storage, and event-runtime helpers should fail open while still surfacing clear health snapshots to operators. The append-heavy market feature path now reports its explicit mode through `storage.get_timeseries_storage_snapshot()["market_feature_store"]`.
- Treat Postgres runtime storage as required for production-like operation.
  `engine.runtime.storage_pool` records readiness and degraded state; `db_guard.ensure_db_ok()`, `runtime_bootstrap.bootstrap_runtime()`, production preflight, and startup gates fail closed when Postgres cannot be acquired or schema validation fails.
- Keep runtime hardware CPU-first unless an accelerator profile is deliberately validated.
  Production defaults are `TRADING_DEPENDENCY_PROFILE=cpu`, `RUNTIME_HARDWARE_PROFILE=cpu`, `TORCH_DEVICE=cpu`, `EMBED_DEVICE=cpu`, `NLP_DEVICE=cpu`, `FINBERT_DEVICE=cpu`, and `TS_FOUNDATION_DEVICE=cpu`, with `TORCH_CPU_THREADS=8` and `TORCH_INTEROP_THREADS=4`. `auto` only selects CUDA when both the NVIDIA dependency profile and NVIDIA runtime profile are active and PyTorch verifies CUDA availability; otherwise health/preflight report the dependency profile, resolved device, disabled accelerator reason, and any profile mismatch. CUDA-specific telemetry, pinned prefetch, TF32, and cuDNN benchmark flags default off and must be enabled explicitly in a validated accelerator profile.
- Keep Postgres schema validation catalog-backed.
  `storage_pg.get_db_validation_snapshot(strict=True)` must fail closed on introspection errors, stale `schema_migrations`, missing required tables/columns/indexes, owned live-ingestion primary-key drift, and unexpected owned columns or type drift.
- Keep SQLite wording precise.
  SQLite remains in the repo for isolated Python tests, historical migration evidence, and compatibility shims such as `PRAGMA table_info`, `sqlite_master` lookups, and `last_insert_rowid()` translation inside `storage_pg.py`. It is not the production runtime fallback.
  The async price writer is the exception that intentionally uses an embedded SQLite WAL file as a bounded local spool; it is a durable write-ahead buffer for Postgres/Timescale price writes, not an alternate runtime storage backend.

## Storage Boot And Failure Behavior

- Cold boot uses `bootstrap_first_run()`, `repair_schema()`, `storage.init_db()`, and the migration files under `engine/runtime/schema/migrations/` to create or upgrade the Postgres schema.
- `DB_PATH` is retained as a local data-root/legacy compatibility hint for older callers and diagnostics. Connection targets come from `TS_PG_DSN` or platform defaults in `engine.runtime.platform`; `DB_PATH` is not a Postgres database location.
- Strict or supervised runtimes require `DB_PATH` to be explicitly set and absolute before normalization, but file-shaped legacy values are normalized by `db_guard.resolve_db_path()` to their parent data directory after that gate passes.
- When Postgres is unavailable, acquisition failures are surfaced as storage readiness `degraded` or `unavailable`, API storage payloads return retryable 503-style metadata where possible, and startup/preflight gates block readiness instead of silently falling back to SQLite.
- Runtime Postgres access uses one Python driver stack: psycopg 3.x. `storage_pool.py`, `storage_pg.py`, the Timescale/price sidecars, read routers, migration validation, and dependency readiness probes must use `psycopg`/`psycopg_pool`; `psycopg2` and `psycopg2-binary` are intentionally not part of the runtime dependency profile.
- Python tests default to `TS_TESTING=1`, `TS_STORAGE_BACKEND=sqlite`, and a temporary `DB_PATH` through `engine/runtime/test_isolation.py` and `tests/conftest.py`. Pytest also installs `engine/runtime/test_network_isolation.py`, which blocks DNS and non-local sockets by default while allowing loopback and Unix-domain sockets for hermetic local servers. Tests that need real Postgres should opt into the `requires_postgres` marker and a reachable local `TS_PG_DSN`; Redis tests should use `requires_redis` with a local `TS_REDIS_URL`. The CI production-backend gate sets `TS_PRODUCTION_BACKEND_TESTS=1` so test isolation preserves the explicit local Postgres/Redis targets instead of scrubbing them back to SQLite; local reproduction is documented in [../../docs/PRODUCTION_BACKEND_CI.md](../../docs/PRODUCTION_BACKEND_CI.md).
- Tests that intentionally call live broker, market-data, or public internet services must be marked `@pytest.mark.live_network` and run with `TRADING_TEST_ALLOW_LIVE_NETWORK=1`. Normal pytest and PR CI deselect `live_network` tests, and unmarked non-local network calls fail with `NetworkBlockedError` before DNS resolution or socket connection.
- Local `tools/validate_repo.py` runtime-graph startup validation is isolated from operator secrets. It sets startup validation mode, routes storage/cache reads to SQLite/memory defaults, clears inline secret and external-service env values, and provisions deterministic temporary `0600` secret files for the supervised imports that require production-shaped secret sources. `--live` or `TRADING_VALIDATION_REQUIRE_PROD_DEPS=1` preserves the real dependency and secret-source policy so production/live still rejects raw sensitive env values unless they come from `*_FILE`, `*_SECRET`, systemd credentials, Docker Compose secrets, or root-owned `0600` files.

## Common Extension Points

- Add a new job:
  register it in [job_registry.py](job_registry.py), ensure its script path is importable, and decide whether it belongs in startup orchestration.
- Add a new health signal:
  add a focused `HealthSnapshotCheck` in [health.py](health.py), register it in `_HEALTH_SNAPSHOT_CHECKS`, keep a fail-closed fallback payload for probe errors, and then wire any new read contract through `runtime_meta` and the dashboard handler layer.
- Add a new ingestion-health signal:
  update [ingestion_status.py](ingestion_status.py), the provider monitor job, and any API/UI readers together so freshness and counters stay coherent.
- Add a new runtime state transition:
  update [lifecycle_state.py](lifecycle_state.py) and any readers in API/UI code.
- Add a new operator diagnostic or support-snapshot field:
  update runtime ownership here first, then wire the read contract through [engine/api/api_system.py](../api/api_system.py) and the operator layer.
