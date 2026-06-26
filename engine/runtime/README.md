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
  It keeps SQLite-compat SQL normalization behind a bounded, thread-safe raw-SQL cache (`_SQL_NORMALIZATION_CACHE_MAXSIZE=1024`) so repeated statements do not re-run regex rewrites, DDL/read classification, or primary-key probing before each Postgres execute.
- [storage_pool.py](storage_pool.py)
  Process-local psycopg pool for runtime Postgres storage. Fresh connections set
  `search_path` once through the pool `configure` hook, and checkouts skip the
  setup SQL while a connection-local marker proves the current schema is already
  installed. Wrapper-executed `SET search_path`, `RESET`, or `DISCARD` SQL
  invalidates that marker so the next checkout repairs the session before use.
- [pg_connection_hygiene.py](pg_connection_hygiene.py)
  Shared psycopg transaction-state helpers used by runtime pools and direct
  read-router pools to detect non-idle connections, roll them back before reuse,
  and log rollback failures with transaction-status context.
- [storage_sqlite.py](storage_sqlite.py)
  Test-only SQLite backend. Its schema bootstrap has a single reachable `_base_schema()` path, and compatibility helpers now call through a locked `storage_pg` helper wrapper instead of cloning runtime function code.
  This is a bounded first slice of the storage-backend rearchitecture: `_PG_COMPAT_HELPER_NAMES` is still a documented migration shim, not the final backend-neutral repository layer.
  `tools/validate_repo.py` enforces that this shim cannot reintroduce code-object cloning or import `storage_pg` outside the compatibility loader.
- [storage_sqlite_normalization.py](storage_sqlite_normalization.py)
  Pure helpers extracted from `storage_sqlite.py` for environment truthiness,
  JSON adapter serialization, SQL read/write classification, SQL signature
  normalization, and SQLite parameter normalization. The `storage_sqlite.py`
  facade still exports the legacy helper names and delegates to this module;
  DB path resolution, connection lifecycle, write-locking, transaction
  boundaries, SQL repair, schema initialization, validation, and migrations stay
  in `storage_sqlite.py`.
- [locks.py](locks.py)
  Cross-process job locks and `job_history` persistence. In SQLite safe/test
  runs, `job_locks` and `job_heartbeats` may be routed through the separate
  liveness database, but `job_history` is regular runtime state: writes use the
  main runtime database and read-only history readers must use the same runtime
  read connection so dashboard history matches `/api/db/health`.
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
- Health helper modules:
  [health_normalization.py](health_normalization.py),
  [health_disk.py](health_disk.py),
  [health_storage_checks.py](health_storage_checks.py),
  [health_snapshot.py](health_snapshot.py),
  [health_subsystem_probes.py](health_subsystem_probes.py), and
  [health_readiness.py](health_readiness.py) hold the decomposed normalization,
  disk/schema probe, subsystem probe, snapshot scaffolding, and readiness serialization helpers
  used by the [health.py](health.py) compatibility facade. Keep new concrete
  subsystem probes registered through `health.py` until they have their own
  characterization coverage.
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
  Broker/execution recovery helpers that reconstruct submit/fill state after a restart. For live-capable brokers (`alpaca`, `ibkr`), broker open-order, fill, position, or pre-live reconcile failures are recorded as `crash_recovery_state` continuity gaps and block live order authority through `engine.runtime.gates`.
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
  Durable SQLite WAL spool consumer that offloads append-heavy price persistence from the hot path. Shard routing now trusts already-normalized row mappings and leaves the one defensive row copy at the durable spool serialization boundary, so callers remain protected from post-enqueue mutation without cloning every row before sharding. It reports queued, spooled, deleted, replayed, rejected, dropped, residual-loss, oldest-spooled-age, backpressure recovery, row-copy avoidance, and corruption counters in its snapshot.
- [async_writer_spool.py](async_writer_spool.py)
  Bounded embedded SQLite WAL spool for async price-write envelopes. It stores rows before enqueue returns, keeps failed flushes for retry until the downstream price write commits, and exposes replay/backpressure/corruption stats. The default WAL synchronous mode is `NORMAL` for fast re-fetchable market data; `FULL` and `EXTRA` are explicit stricter modes.
- [non_price_ingestion_spool.py](non_price_ingestion_spool.py)
  Shared bounded SQLite WAL spool for high-volume refetchable non-price ingestion rows. `telemetry_append_buffer.py` uses it for provider health, ingestion-pipeline health, ingest slippage, and raw quote telemetry. `engine/data/options_poll.py` uses the same spool semantics for tagged option-chain, options snapshot-event, and symbol-state batches. Accepted rows are durable before enqueue returns, failed DB flushes leave selected spool rows in place, rows are deleted only after the target DB commit succeeds, and row/byte caps surface backpressure instead of growing memory without bound. Telemetry shutdown drains use a separate bounded retry policy from normal flushing: steady-state writes remain fast (`attempts=1`, `0.25s`), while shutdown can spend the runtime drain deadline across more generous but finite attempts and reports residual spooled/loss counters.
- [storage_pg_prices.py](storage_pg_prices.py)
  Postgres/Timescale-oriented price persistence sidecar for append-heavy market data. Its hot
  write path binary-COPYs each flush into fixed unlogged staging tables, then runs one
  `INSERT ... SELECT DISTINCT ON (...) ... ON CONFLICT DO UPDATE` per target table. If the active
  adapter or proxy cannot provide psycopg binary COPY, the sidecar records the fallback and uses the
  bounded multi-row `INSERT ... VALUES (...), ... ON CONFLICT` path instead. Raw price events carry
  producer-computed `price_raw:v1` keys based on stable identifiers and timestamps, not prices or
  sizes; every raw writer upserts on `(symbol, provider, event_key, time/ts_ms)`. `write_batch()`
  normalizes trusted row mappings directly into SQL/COPY tuples without first materializing
  `dict(row)` clones, and exposes `normalization_input_rows`, `row_copy_avoided_rows`, and
  `row_copy_fallback_rows` so large-batch copy reduction is visible in production.
- [timescale_client.py](timescale_client.py)
  Background TimescaleDB client for hypertable writes and schema management on newer time-series surfaces. Eligible append-heavy tables use asyncpg `copy_records_to_table()` into a connection-local temp staging table prepared once per pooled connection, then one `INSERT ... SELECT DISTINCT ON (...) ... ON CONFLICT DO UPDATE` upsert. `model_registry` stays on the existing direct upsert path because its conflict handler preserves the earliest `created_at`. `TIMESCALE_COPY_STAGING_ENABLED` disables the COPY path, and `TIMESCALE_COPY_STAGING_FALLBACK_ENABLED` controls fallback to direct upsert when COPY support is unavailable.
- [model_cache.py](model_cache.py)
  In-memory catalog cache used by serving and governance readers to avoid repeated registry scans.
- [price_cache.py](price_cache.py)
  Runtime-facing wrapper around the data price cache that exposes health-oriented accessors to other subsystems.
- [live_cache.py](live_cache.py)
  Runtime live-cache boundary for price and feature snapshots. Memory remains the safe fallback. Redis mode requires the shared msgpack envelope codec, uses `decode_responses=False`, and writes snapshots through an atomic Lua `EVALSHA` script that rejects older `snapshot_ts_ms` values before setting the binary payload and TTL.
  Price cache callers use the backend's batch price APIs so multi-symbol ingestion cycles read with one
  Redis `MGET` and write with one freshness-protected Lua call instead of one read/modify/write loop per symbol.
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
- Keep Timescale COPY/staging bounded and observable.
  `TimescaleClient` defaults to a `TIMESCALE_BATCH_SIZE` of 2000 and `TIMESCALE_QUEUE_MAXSIZE` of 256 so the safe profile raises per-flush throughput without increasing the estimated buffered-row window. The `host_32t_123g` ingestion profile raises the Timescale batch to 4000 and lowers the queue to 128. `/api/health` exposes `timescale.copy_staging_enabled`, `timescale.copy_staging_tables`, `timescale.metrics.copy_batches`, `timescale.metrics.copy_fallback_count`, `timescale.metrics.executemany_batches`, `timescale.metrics.deduped_rows`, and per-table `last_write_path`.
- Keep append-heavy price writes on the COPY/staging path.
  `price_timescale_schema.py` owns the canonical Timescale price sidecar schema:
  `price_ticks`, `price_quotes`, and `price_quotes_raw` use `"time" TIMESTAMPTZ`;
  `prices(ts_ms, ...)` is retained as the legacy compatibility/readiness table.
  Fresh baseline migrations, compatibility migration `0068`, and the runtime
  sidecar must consume this module rather than carrying independent price DDL.
  `PostgresPriceStorage.write_batch()` must not create staging DDL during flushes; `ensure_schema()`
  owns the fixed unlogged staging tables and session indexes. Duplicate keys inside a flush are
  resolved deterministically by `staging_ordinal DESC` before the set-based upsert. Staging column
  metadata is keyed by the fixed staging table names such as `price_ticks_write_staging`, while
  target-table type and conflict metadata remain keyed by the base table names. The fallback
  `_execute_many_values()` path must stay bounded under PostgreSQL's 65,535 bind-parameter limit and
  must remain visible through `last_write_path`, `copy_fallbacks`, and `last_copy_unavailable`.
  `/api/health` includes `pg_price_storage.copy_batches`, `copy_rows`, `values_batches`,
  `values_rows`, `write_failures`, `retryable_failures`, `fatal_failures`, `pool_resets`,
  `write_circuit_open`, and `write_circuit_rejected_batches`; runtime metrics emit the matching
  `storage_pg_prices_copy_*`, `storage_pg_prices_values_*`, `storage_pg_prices_written_rows`, and
  `storage_pg_prices_failures` counters plus `storage_pg_prices_write_circuit_opened`,
  `storage_pg_prices_write_circuit_rejected_batches`, and the `storage_pg_prices_write_circuit_open`
  gauge. Retryable lock/deadlock/timeout failures are retried without resetting the whole pool unless
  the failure is classified as a broken connection or pool acquisition problem. After
  `TIMESCALE_PRICES_CIRCUIT_FAILURE_THRESHOLD` exhausted retryable `write_batch` failures, the
  sidecar opens a write circuit for `TIMESCALE_PRICES_CIRCUIT_OPEN_S` seconds so async price writer
  workers fail fast, retain selected rows in the durable SQLite WAL spool, and apply backpressure
  through normal queue/spool depth instead of stampeding the database.
- Keep Redis live-cache mode on the binary envelope fast path.
  `LIVE_CACHE_BACKEND=redis` must have `msgpack` installed; external-service preflight and startup gates report this as a hard error before live-cache Redis mode is considered ready. Production-like runtimes also require the cache codec to resolve to msgpack, while JSON fallback is a development-only opt-in through `CACHE_CODEC_ALLOW_JSON_FALLBACK=1`. New writes are a single `EVALSHA` round trip after startup script load, not a client-side `WATCH`/`MULTI` loop. `get_live_cache_snapshot()` must keep reporting `redis_write_path=evalsha_lua_msgpack`, accepted write counts, script-load count, evalsha attempts/results, NOSCRIPT reloads, stale-timestamp rejections, write failures, and fallback count/reason so production can verify the active path.
- Treat Postgres runtime storage as required for production-like operation.
  `engine.runtime.storage_pool` records readiness and degraded state; `db_guard.ensure_db_ok()`, `runtime_bootstrap.bootstrap_runtime()`, production preflight, and startup gates fail closed when Postgres cannot be acquired or schema validation fails.
- Keep runtime hardware CPU-first unless an accelerator profile is deliberately validated.
  Production defaults are `TRADING_DEPENDENCY_PROFILE=cpu`, `RUNTIME_HARDWARE_PROFILE=cpu`, `TORCH_DEVICE=cpu`, `EMBED_DEVICE=cpu`, `NLP_DEVICE=cpu`, `FINBERT_DEVICE=cpu`, and `TS_FOUNDATION_DEVICE=cpu`, with `TORCH_CPU_THREADS=8` and `TORCH_INTEROP_THREADS=4`. `auto` selects NVIDIA CUDA only when both the NVIDIA dependency profile and NVIDIA runtime profile are active and PyTorch verifies CUDA availability. `auto` selects AMD ROCm only when both the `amd-rocm` dependency profile and runtime hardware profile are active and PyTorch reports a HIP build, CUDA/HIP availability, and a nonzero device count; otherwise health/preflight report the dependency profile, resolved device, disabled accelerator reason, and any profile mismatch. CUDA-specific telemetry, pinned prefetch, TF32, and cuDNN benchmark flags default off and must be enabled explicitly in a validated accelerator profile.
- Keep Postgres schema validation catalog-backed.
  `storage_pg.get_db_validation_snapshot(strict=True)` must fail closed on introspection errors, stale `schema_migrations`, missing required tables/columns/indexes, owned live-ingestion primary-key drift, and unexpected owned columns or type drift.
- Keep small Postgres write coalescing explicit.
  `storage_pg.run_write_txn()` still owns one independent acquire/commit/close cycle per call unless a bounded helper path opts into coalescing. The CPCV compatibility helper `storage_pg.record_backtest_cpcv_path_result()` is the approved small-write coalesced path: when it owns the connection, it inserts the path-result row and legacy run row on one connection in one transaction, rolling both back together on failure. When a caller supplies a connection, that caller keeps the transaction boundary and the helper records a `caller_connection` bypass instead of committing. Job lock, heartbeat, and lock-touch writes are critical and are explicitly routed through the critical bypass branch so each public call still commits independently. Production metrics proving the active behavior are `storage_pg_small_write_coalesce_attempted`, `storage_pg_small_write_coalesce_committed`, `storage_pg_small_write_coalesce_failed`, `storage_pg_small_write_coalesced_calls`, `storage_pg_small_write_coalesced_rows`, `storage_pg_small_write_coalesce_bypassed`, and `storage_pg_small_write_coalesce_latency_ms`; bypass metrics carry reasons such as `critical`, `single_call`, and `caller_connection`.
- Keep SQLite wording precise.
  SQLite remains in the repo for isolated Python tests, historical migration evidence, and compatibility shims such as `PRAGMA table_info`, `sqlite_master` lookups, and `last_insert_rowid()` translation inside `storage_pg.py`. It is not the production runtime fallback.
  The async price writer, telemetry append buffer, and options poll durable buffer are the exceptions that intentionally use embedded SQLite WAL files as bounded local spools; they are durable write-ahead buffers for refetchable Postgres/Timescale price, telemetry, and options-ingestion writes, not alternate runtime storage backends. The telemetry append buffer keeps accepted rows in `telemetry_append_buffer_spool.sqlite` until the target DB transaction commits, then deletes the corresponding spool rows. At shutdown, durable residual telemetry rows remain in the spool for replay, while any legacy in-memory residual rows are counted as `residual_dropped_rows`/`residual_loss_rows` and emit residual-loss metrics. The options poller keeps tagged batches in `options_poll_durable_buffer.sqlite` with the same delete-after-commit rule.

## Storage Boot And Failure Behavior

- Cold boot uses `bootstrap_first_run()`, `repair_schema()`, `storage.init_db()`, and the migration files under `engine/runtime/schema/migrations/` to create or upgrade the Postgres schema.
- Postgres repair applies numbered migrations before runtime-side compatibility DDL. If strict validation is clean after migration replay, repair records `init_db=skipped_schema_valid` and does not run the compatibility `init_db()` pass; if validation still reports missing runtime-owned objects, repair falls back to `storage.init_db()` and validates again.
- Existing Postgres deployments rely on numbered migrations for baseline additions after first install. `0069_data_source_provider_accounts.py` carries the DF-04 shared provider-account registry and provider lookup index so standalone startup validation can repair databases that already applied `0001_baseline.py`.
- `DB_PATH` is retained as a local data-root/legacy compatibility hint for older callers and diagnostics. Connection targets come from `TS_PG_DSN` or platform defaults in `engine.runtime.platform`; `DB_PATH` is not a Postgres database location.
- Strict or supervised runtimes require `DB_PATH` to be explicitly set and absolute before normalization, but file-shaped legacy values are normalized by `db_guard.resolve_db_path()` to their parent data directory after that gate passes.
- When Postgres is unavailable, acquisition failures are surfaced as storage readiness `degraded` or `unavailable`, API storage payloads return retryable 503-style metadata where possible, and startup/preflight gates block readiness instead of silently falling back to SQLite.
- Runtime Postgres access uses one Python driver stack: psycopg 3.x. `storage_pool.py`, `storage_pg.py`, the Timescale/price sidecars, read routers, migration validation, and dependency readiness probes must use `psycopg`/`psycopg_pool`; `psycopg2` and `psycopg2-binary` are intentionally not part of the runtime dependency profile.
- Runtime psycopg pools must never return a connection to service while it is inside an open or aborted transaction. `storage_pool.py`, `price_read_router.py`, `telemetry_read_router.py`, and `storage_pg_prices.py` configure pool `check`/`reset` callbacks and perform explicit release-path rollbacks before `putconn()`. If rollback fails, the connection is closed/discarded instead of being reused. `storage_pg.py` logs the originating SQL failure with SQLSTATE, transaction status, and a parameter-free statement summary so later `InFailedSqlTransaction` symptoms can be traced to the first abort.
- Postgres-backed `storage_pg.py` still accepts bounded SQLite compatibility probes used by read adapters, including quoted `PRAGMA table_info("...")`, `sqlite_master` lookups, and `SELECT sqlite_version()`. Query adapters must avoid sending SQLite-only expressions such as `strftime()` through psycopg when the compatibility connection is backed by Postgres.
- Timescale telemetry and price read routers use module-level lazy `psycopg_pool.ConnectionPool` instances keyed by read role, DSN, schema, pool size, and timeout/application settings. They memoize parsed env config behind an env/password-source fingerprint and honor the existing `TIMESCALE_POOL_*` and `TIMESCALE_PRICES_POOL_*` config families instead of opening a fresh Postgres connection per dashboard/operator read. High-frequency telemetry, dashboard price, and market candle reads are coalesced through sub-second in-process `state_cache` entries; SSE market streams still read live on their polling cadence. Router close hooks empty all read pools during tests and process shutdown.
- Python tests default to `TS_TESTING=1`, `TS_STORAGE_BACKEND=sqlite`, and a temporary `DB_PATH` through `engine/runtime/test_isolation.py` and `tests/conftest.py`. Pytest also installs `engine/runtime/test_network_isolation.py`, which blocks DNS and non-local sockets by default while allowing loopback and Unix-domain sockets for hermetic local servers. Tests that need real Postgres should opt into the `requires_postgres` marker and a reachable local `TS_PG_DSN`; Redis tests should use `requires_redis` with a local `TS_REDIS_URL`. The CI production-backend gate sets `TS_PRODUCTION_BACKEND_TESTS=1` so test isolation preserves the explicit local Postgres/Redis targets instead of scrubbing them back to SQLite; local reproduction is documented in [../../docs/PRODUCTION_BACKEND_CI.md](../../docs/PRODUCTION_BACKEND_CI.md).
- Tests that intentionally call live broker, market-data, or public internet services must be marked `@pytest.mark.live_network` and run with `TRADING_TEST_ALLOW_LIVE_NETWORK=1`. Normal pytest and PR CI deselect `live_network` tests, and unmarked non-local network calls fail with `NetworkBlockedError` before DNS resolution or socket connection.
- Local `tools/validate_repo.py` runtime-graph startup validation is isolated from operator secrets. It sets startup validation mode, routes storage/cache reads to SQLite/memory defaults, clears inline secret and external-service env values, and provisions deterministic temporary `0600` secret files for the supervised imports that require production-shaped secret sources. `--live` or `TRADING_VALIDATION_REQUIRE_PROD_DEPS=1` preserves the real dependency and secret-source policy so production/live still rejects raw sensitive env values unless they come from `*_FILE`, `*_SECRET`, systemd credentials, Docker Compose secrets, or root-owned `0600` files.

## Local Sim Env And DSN Context

- Host-side Codex/sim-paper imports load the repo-local `.env`. Host DSNs must use host-resolvable targets such as `127.0.0.1` for Postgres/Timescale, Redis, and MinIO. Do not use Compose-only service names such as `timescaledb`, `redis`, `minio`, or role names such as `ts_app` in `.env` when the process runs on the host.
- Compose containers get their runtime env from `deploy/compose/.env` plus `docker-compose.stack.yml`. Inside containers, DSNs must use Compose service names such as `timescaledb`, `redis`, and `minio`, and secret files must be mounted at `/run/secrets/*`.
- Production-like local sim envs must keep credentials out of DSNs and URLs. Use passwordless connection strings plus file-backed secrets: `TS_PG_PASSWORD_FILE` for `TS_PG_DSN`/`TIMESCALE_DSN`/`TIMESCALE_PRICES_DSN`, `REDIS_PASSWORD_FILE` or `TS_REDIS_PASSWORD_FILE` for Redis URLs, `DASHBOARD_API_TOKEN_FILE`, `OPERATOR_API_TOKEN_FILE`, and `OBJECT_STORE_ACCESS_KEY_FILE`/`OBJECT_STORE_SECRET_KEY_FILE` for MinIO/object storage. For `default_pg_dsn()` and passwordless Postgres DSNs/URLs, systemd `LoadCredential` values, including explicit `*_PASSWORD_SECRET` names, remain preferred when `CREDENTIALS_DIRECTORY` is present. Non-systemd local/container starts skip unavailable systemd Postgres secret lookups, fall back to the configured `TS_PG_PASSWORD_FILE`/`TIMESCALE_PASSWORD_FILE` path without warning noise, and raise one actionable `SecretNotAvailable` only when no Postgres password source resolves.
- Startup preflight calls `engine.runtime.dsn_context.dsn_context_snapshot()` through `live_trading_preflight`. The snapshot reports only env keys, hostnames, ports, and reason codes. It blocks strict/prod/supervised starts when a configured DSN is for the wrong context or the hostname is not resolvable, and it never returns raw DSN values.
- `TRADING_DSN_CONTEXT=host|container` can force context detection for diagnostics/tests. Auto-detection uses `DASHBOARD_BIND_CONTEXT`, container marker envs, and container marker files.

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
