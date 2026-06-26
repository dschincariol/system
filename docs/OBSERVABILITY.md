# Observability

This document records the observability surfaces that are present in the repository today. It is grounded in `engine/runtime/observability.py`, `engine/runtime/failure_diagnostics.py`, `engine/runtime/health.py`, `engine/api/api_system.py`, `boot/operator_server.js`, and the current validation and probe tools under `tools/`.

## Primary Signals

| Surface | Owner | What it exposes |
| --- | --- | --- |
| Component health snapshots | `engine/runtime/observability.py` | In-memory `record_component_health(...)` status keyed by component name, with TTL-based stale detection from `get_component_health_snapshot(...)`. |
| Rolling rates | `engine/runtime/observability.py` | Rolling success-rate gauges and observation counters emitted through `record_rolling_rate(...)`. |
| Failure payloads | `engine/runtime/failure_diagnostics.py` | Structured failure logs plus `runtime_failure` event-log rows built by `log_failure(...)` and `failure_response(...)`. |
| Health snapshots | `engine/runtime/health.py` and `engine/api/api_system.py` | Repo-wide health, readiness, database, prices, providers, jobs, execution barrier, and broker-connection state surfaced by `/api/health` and `/api/readiness`. Runtime probes are registered as named `HealthSnapshotCheck` entries so probe failures are isolated and the final snapshot fails closed. |
| Runtime watchdogs | `engine/api/api_system.py` | Job heartbeat age, restart counters, ingestion freshness, and pipeline watchdog state from `/api/operator/runtime_watchdogs`. |
| Support snapshot | `engine/api/api_system.py` | Repair-oriented evidence bundle from `/api/operator/support_snapshot`. Includes preflight, DB debug, recent errors, watchdogs, job status, and synthesized diagnostics. |
| Provider telemetry | `engine/api/api_system.py` | Feed-level health and freshness from `/api/operator/provider_telemetry`. |
| Ingestion restart-storm guard | `engine/runtime/ingestion_runtime.py`, `engine/runtime/metrics.py`, `engine/runtime/event_log.py` | Child restart attempts from exits, feed-stall restarts, and spawn failures are persisted as namespaced expiring rows in `job_locks` (`ingestion_restart_guard/v1::*`) instead of process memory. `INGESTION_RUNTIME_CHILD_MAX_RESTARTS` and `INGESTION_RUNTIME_CHILD_RESTART_WINDOW_S` define the sliding window. Suppression emits `ingestion_child_restart_suppressed_total`, records `event_log.event_type='ingestion_child_restart_suppressed'`, publishes `market_data/child_restart_guard_triggered`, and includes each child's `restart_guard` snapshot in the ingestion heartbeat and `runtime_meta.ingestion_state`. |
| Cache single-flight metrics | `engine/cache/store.py`, `engine/runtime/metrics.py` | Redis miss stampede-control counters: `cache_singleflight_waits_total` for callers that had to wait on a per-key miss lock, `cache_singleflight_wins_total` for callers that ran the loader, and `cache_singleflight_failures_total` for lock timeouts or loader exceptions. Counters are tagged by `path` (`read` or `read_many`) and failure `reason` where applicable. |
| Cache write-through metrics | `engine/cache/store.py`, `engine/runtime/metrics.py` | Generic Postgres-backed cache writes emit `cache_write_through_path_total` after the Postgres commit and Redis update/invalidation attempt. Tags include `mode` (`single_set`, `pipeline_set_many`, `sequential_set_many`, `invalidate`, or `invalidate_many`), `result`, `key_count`, and payload codec version. These counters replace the previous before/after Redis version readback so write observability does not add extra `GET` round trips. |
| Redis live-cache write metrics | `engine/runtime/live_cache.py` | `get_live_cache_snapshot()` exposes the active backend, fallback reason/count, binary codec, `write_path`/`redis_write_path`, accepted price/feature write counts, script-load count, `EVALSHA` attempts/results, NOSCRIPT reloads, stale-timestamp rejections, write failures, `redis_health_check_interval_s`, and `redis_last_health_check_ts_ms`. Redis mode uses these fields, plus `live_cache_redis_write_path_total{mode=evalsha_lua_msgpack,result=accepted|rejected_older|failure}`, to prove the live price/feature snapshot path is on the Lua/msgpack fast path rather than a client-side read-modify-write loop; multi-symbol price-cache ingestion writes should increase `redis_evalsha_attempts` by one per batch while `redis_evalsha_results` reflects the per-symbol results returned by the script. |
| Live-price provider metrics | `engine/runtime/metrics.py`, `engine/data/live_prices/ccxt_live.py` | Provider-side runtime counters for fetch path selection. CCXT emits `ccxt_live_fetch_cycles`, `ccxt_live_fetch_tickers_attempts`, `ccxt_live_fetch_tickers_successes`, `ccxt_live_fetch_tickers_failures`, `ccxt_live_fetch_tickers_cycle_failures`, `ccxt_live_fetch_tickers_calls`, `ccxt_live_fetch_tickers_markets`, `ccxt_live_fetch_tickers_rows`, `ccxt_live_fetch_tickers_unsupported`, `ccxt_live_fetch_tickers_partial_misses`, `ccxt_live_fetch_tickers_missing_symbols`, `ccxt_live_failed_symbols`, `ccxt_live_fallback_fetches`, `ccxt_live_fallback_rows`, `ccxt_live_fallback_successes`, `ccxt_live_fallback_failures`, `ccxt_live_exchange_cache_hits`, `ccxt_live_exchange_cache_misses`, `ccxt_live_exchange_cache_evictions`, `ccxt_live_markets_loads`, `ccxt_live_markets_reuses`, `ccxt_live_market_cache_hits`, `ccxt_live_market_cache_reloads`, and `ccxt_live_market_cache_invalidations` tagged by exchange id, `supports_batch`, and path (`batch_only`, `batch_with_fallback`, `batch_failed`, `fallback_only`, or `batch_stopped`). Batch failure and fallback counters include `reason` / `batch_failure_reason` tags such as `unsupported`, `nonfatal`, `stale`, `missing_symbol`, or `invalid_row`. Timing metrics split market loads, batch ticker calls, and fallback ticker calls with `ccxt_live_markets_load_latency_ms`, `ccxt_live_fetch_tickers_latency_ms`, and `ccxt_live_fallback_fetch_latency_ms`. |
| Ingestion writer diagnostics | `engine/runtime/ingestion_runtime.py`, `engine/runtime/async_writer.py`, `engine/runtime/telemetry_append_buffer.py`, `engine/runtime/timescale_client.py`, `engine/runtime/storage_pg_prices.py`, `engine/data/options_poll.py`, `engine/data/options_data_quality.py` | Durable spool depth/bytes/oldest age, queued/spooled/deleted/replayed/rejected/dropped row counters, residual-loss rows, per-shard async price writer depth, pending lag, batch size, write latency, write failures, active/recovered backpressure counters, retry counts, corruption counters, COPY/fallback/dedupe counters, and DB write duration for ingestion writers. The Postgres price sidecar emits `storage_pg_prices_copy_batches`, `storage_pg_prices_copy_rows`, `storage_pg_prices_values_batches`, `storage_pg_prices_values_rows`, `storage_pg_prices_written_rows`, `storage_pg_prices_copy_fallbacks`, `storage_pg_prices_failures`, `storage_pg_prices_write_circuit_opened`, `storage_pg_prices_write_circuit_rejected_batches`, and the `storage_pg_prices_write_circuit_open` gauge. Its snapshot exposes retryable/fatal failure counts, pool reset count, and write-circuit/backpressure state so ordinary retryable lock/deadlock/timeout errors can be distinguished from connection failures. Options polling emits `options.poll.state_load_queries`, `options.poll.fetch_max_workers`, `options.poll.commit_batches`, `options.poll.max_symbols_per_commit`, `options.poll.rows_written`, `options.poll.event_rows_written`, `options.poll.state_rows_written`, `options.poll.cached_fallback_symbols`, `options.poll.copy_fallbacks`, `options.poll.bulk_write_failures`, `options.poll.event_write_failures`, `options.poll.state_write_failures`, `options.poll.durable_buffer.pending_rows`, `options.poll.durable_buffer.pending_bytes`, `options.poll.durable_buffer.oldest_age_ms`, `options.poll.durable_buffer.replayed_rows`, `options.poll.durable_buffer.deleted_rows`, `options.poll.durable_buffer.rejected_rows`, `options.poll.durable_buffer.dropped_rows`, `options.poll.durable_buffer.backpressure_active`, `options.poll.durable_buffer.backpressure_events`, and `write_buffer.write_paths` metadata showing `copy_staging` versus fallback writes. Options data-quality checks emit `options.dq.*` metrics for coverage, freshness, provider field completeness, and IV sanity, attach the same report to `/api/health.options_ingestion.data_quality`, and write a throttled normalized options event with `event_kind=options_data_quality_degraded` when configured thresholds fail. The ingestion runtime embeds these snapshots in its heartbeat and `runtime_meta.ingestion_state`; `/api/health` also exposes `async_price_persistence`, `pg_price_storage`, `timescale`, and `telemetry_append_buffer`. |
| Ingestion soak readiness | `engine/runtime/ingestion_soak.py`, `engine/runtime/health.py`, `engine/runtime/health_readiness.py`, and `engine/runtime/prod_preflight.py` | `/api/health.ingestion_soak` consolidates async price writer, telemetry append buffer, options durable buffer, Timescale client, Postgres price sidecar, Redis pool/circuit, and applied Timescale policy evidence. Required production checks fail closed when live writer evidence is missing, queues or spools exceed thresholds, backpressure is active, COPY falls back unexpectedly, loss/corruption/dead-letter counters are nonzero, Redis cache circuit is open, required scoring indexes are absent, or hypertable chunk/compression policy is not applied. |
| Execution barrier | `engine/runtime/gates.py` and `engine/api/api_system.py` | Fail-closed execution state from `/api/execution/barrier`. |
| Operator logs | `var/log/` and `boot/operator_server.js` | Runtime log tails, stderr tails, and operator-side snapshots exposed through the Node operator server. |
| Disk pressure | `engine/runtime/health.py` and `engine/runtime/prod_preflight.py` | Per-path filesystem headroom for `/`, runtime data, runtime logs, backups, and Docker roots when present. Critical disk-pressure evidence makes `/api/health.ok=false` in every engine mode. |
| Host memory pressure | `engine/runtime/memory_pressure.py`, `engine/runtime/health.py`, `engine/runtime/prod_preflight.py`, and `engine/strategy/jobs/observability_snapshot.py` | Read-only RAM/swap/zram/swappiness/ZFS ARC policy evidence. Required production/live checks fail closed when total swap, zram, managed swapfile, swappiness, ARC max, or container memory headroom do not meet policy. |
| Effective Docker/Postgres/Redis state | `engine/runtime/effective_runtime_state.py`, `engine/runtime/postgres_tuning.py`, `engine/runtime/health.py`, and `engine/runtime/prod_preflight.py` | `/api/health.effective_runtime_state` and `prod_preflight.py` compare intended compose/env limits with actual Docker inspect evidence, Redis `CONFIG GET`, and Postgres `pg_settings`. Required compose production checks fail closed when evidence is missing or drifted. |
| Storage/WAL guards | `engine/runtime/health.py` and `engine/strategy/jobs/observability_snapshot.py` | `/api/health.storage_wal_guards` exposes storage placement, `pg_stat_archiver`, `pg_wal` growth/backlog, and free-space evidence. The observability snapshot emits durability alerts for critical free space and confirmed WAL-archiver outage in every mode, while production-only placement/backlog warnings remain production-gated. |
| CPU power policy drift | `engine/strategy/jobs/observability_snapshot.py` and `engine/runtime/cpu_power_policy.py` | Read-only `cpu_power_policy.sh verify` snapshots recorded as component health under `cpu_power_policy`. Drift reports `status=drift` and `reason=cpu_power_policy_drift`; non-required environments with no visible host CPU controls report `status=skipped`. |
| Backup accounting | `engine/runtime/backup_evidence.py` and `ops/backup/accounting.sh` | Backup root apparent/allocated bytes, container mount source, subdirectory sizes, inventory counts, and retention status/settings. |

## Persistence And Retention Boundaries

- `engine/runtime/observability.py` keeps component health and rolling-rate windows in process memory.
- `engine/runtime/failure_diagnostics.py` persists structured failure events into the `event_log` table with `event_type='runtime_failure'`.
- `engine/runtime/metrics.py` and `engine/runtime/metrics_store.py` emit best-effort metrics into the repo's runtime metrics pipeline.
- `services/operator_ai/agent.js` writes diagnostics-only analyses to `var/log/ai_operator_log.jsonl` locally, or to the configured runtime log directory / explicit operator-AI log path.
- Runtime and operator log files use `var/log/` for local defaults; explicit deployment log directories still take precedence.
- Docker compose stdout/stderr logs are capped by `DOCKER_LOG_DRIVER=local`,
  `DOCKER_LOG_MAX_SIZE=50m`, and `DOCKER_LOG_MAX_FILE=5` unless the target host
  overrides them.
- File logs are rotated by `deploy/logrotate/trading-system`: app and compose
  mounted logs, boot stderr logs, ingestion stdout/stderr logs, and the
  diagnostics-only operator-AI JSONL log rotate daily or at
  `maxsize 50M`, keep 10 compressed rotations, delete rotations older than
  21 days, and use `copytruncate` so supervised processes do not need a restart.

## Canonical Endpoints

| Endpoint | Use |
| --- | --- |
| `GET /api/health` | Current health snapshot used by dashboard and operator UIs. |
| `GET /api/readiness` | Condensed readiness view derived from health, graph validation, and execution state. |
| `GET /api/execution/barrier` | Whether the execution pipeline and real trading are currently allowed, and why. |
| `GET /api/operator/service_status` | Aggregated engine and service status summary. |
| `GET /api/operator/provider_telemetry` | Current feed/provider freshness and runtime correlation details. |
| `GET /api/operator/runtime_watchdogs` | Heartbeat and freshness watchdog state for critical jobs; `ok` reflects response success and `watchdogs_ok` reflects operational watchdog health. |
| `GET /api/operator/support_snapshot` | Repair-oriented evidence bundle for humans and tooling. |
| `GET /api/telemetry` and `GET /api/telemetry/history` | Telemetry APIs exposed by `engine/api/api_system.py`. |

## Important Configuration

| Variable | Meaning | Default In Code |
| --- | --- | --- |
| `OBS_COMPONENT_HEALTH_TTL_S` | How long a component-health record stays fresh before it is marked stale. | `900` |
| `OBS_RATE_WINDOW` | Rolling observation window used by `record_rolling_rate(...)`. | `50` |
| `API_HEALTH_CACHE_TTL_S` | Cache TTL for `/api/health` snapshots. | `3.0` |
| `HEALTH_SNAPSHOT_TRACE` | Enables per-section `health_snapshot_section` debug logs for `/api/health`; trace-only `ok` fields default to false when a section omits its own `ok` sub-key so logs cannot show uncomputed sections as healthy. | off |
| `DISK_PRESSURE_WARN_FREE_PCT` / `DISK_PRESSURE_WARN_FREE_BYTES` | Warning threshold for filesystem headroom in health and preflight. | `15` / `21474836480` |
| `DISK_PRESSURE_CRITICAL_FREE_PCT` / `DISK_PRESSURE_CRITICAL_FREE_BYTES` | Critical threshold that fails startup validation and production preflight. | `5` / `5368709120` |
| `BACKUP_ACCOUNTING_DU_TIMEOUT_S` | Timeout for backup accounting size checks. | `8` |
| `INGESTION_TUNING_PROFILE` | Optional bounded ingestion profile. `safe` keeps conservative defaults; `host_32t_123g` increases writer batch and pool throughput for the 32-thread / 123 GiB host while reducing queue depths so the buffered-row risk window does not expand. | `safe` |
| `INGESTION_TUNING_MAX_TOTAL_DB_CONNECTIONS` | Hard preflight budget for the runtime PG pool plus enabled Timescale and price-storage pools. | `24` |
| `INGESTION_TUNING_MAX_BUFFERED_ROWS` | Hard preflight budget for estimated buffered rows across Timescale, async price writer, telemetry append, event-log, runtime-metrics, and runtime-meta buffers. | `1200000` |
| `INGESTION_SOAK_REQUIRE_EVIDENCE` / `PREFLIGHT_REQUIRE_INGESTION_SOAK_EVIDENCE` | Require the consolidated soak report in health/readiness and production preflight. Production-like runtimes require it automatically unless explicitly disabled for non-production diagnostics. | automatic in production-like modes |
| `INGESTION_SOAK_MAX_QUEUE_FILL_RATIO`, `INGESTION_SOAK_MAX_SPOOL_BYTES_FILL_RATIO`, `INGESTION_SOAK_MAX_SPOOL_ROWS_FILL_RATIO`, `INGESTION_SOAK_MAX_SPOOL_AGE_S` | Thresholds used by `ingestion_soak` to decide whether queue/spool growth proves ingestion is failing to keep up. | `0.75`, `0.80`, `0.80`, `300` |
| `INGESTION_SOAK_MAX_COPY_FALLBACKS`, `INGESTION_SOAK_MAX_WRITE_FAILURES`, `INGESTION_SOAK_MAX_FLUSH_FAILURES` | Maximum tolerated fallback/failure counters during production soak acceptance. | `0`, `0`, `0` |
| `TIMESCALE_BATCH_SIZE` / `TIMESCALE_QUEUE_MAXSIZE` | Timescale sidecar flush size and queue depth. Safe defaults are `2000` and `256`; `host_32t_123g` uses `4000` and `128` so batch throughput rises while the buffered-row risk window stays flat. | `2000` / `256` |
| `TIMESCALE_COPY_STAGING_ENABLED` / `TIMESCALE_COPY_STAGING_FALLBACK_ENABLED` | Enables asyncpg COPY-to-session-temp-staging for eligible append-heavy Timescale tables and permits direct-upsert fallback when `copy_records_to_table()` is unavailable, unsupported, or fails. | `1` / `1` |
| `INGESTION_RUNTIME_SNAPSHOT_CACHE_TTL_S` | Short in-process TTL for ingestion supervisor snapshots of latest prices, provider health, enabled price sources, data-source control-plane config, child config hashes, and child heartbeats. The runtime recomputes age fields from cached timestamps, reads the `data_sources_reload_ts_ms` marker directly on control-plane refresh to invalidate config snapshots across processes, and clamps the TTL to `0-5` seconds; `0` disables the cache for debugging. | `1.0` |
| `INGESTION_RUNTIME_CHILD_MAX_RESTARTS` / `INGESTION_RUNTIME_CHILD_RESTART_WINDOW_S` | Sliding-window child restart-storm guard. Each exit, feed-stall restart, or spawn failure writes an expiring accounting row into `job_locks`; counts survive ingestion supervisor restarts until the window expires. A stable child run, a newer data-source config reload marker, and `POST /api/operator/restart_feeds` clear the guard as recovery/manual override paths. | `10` / `300.0` |
| `INGESTION_SHARD_INDEX` / `INGESTION_SHARD_COUNT` | Optional ingestion supervisor shard coordinates. Defaults `0` / `1` keep one supervisor and the historical liveness rows; `count>1` writes shard-specific `job_locks`, `job_heartbeats`, and `ingestion_state::shard:*` rows while price/options pollers process only their stable symbol partition. `start_system.py` and `start_ingestion.py` use shard-suffixed ingestion pid/stdout/stderr files in multi-shard mode, and startup stale cleanup deletes only the current shard's liveness rows so sibling shards can keep running while a standby takes over one stale shard. | `0` / `1` |
| `ASYNC_PRICE_WRITER_WORKERS` | Number of async price writer shard workers. Rows are split into durable shard envelopes by stable symbol/event key before being spooled, so each symbol's writes stay on one worker and replay in spool order. Must be less than or equal to `TIMESCALE_PRICES_POOL_MAX_SIZE`; writer startup, storage config, and production preflight reject undersized price pools. | `4` (`8` under `host_32t_123g`) |
| `ASYNC_PRICE_WRITER_SPOOL_MAX_BYTES` | Logical payload byte cap for the SQLite WAL async price-writer spool. Enqueue rejects and dead-letters new envelopes when the cap or `ASYNC_PRICE_WRITER_QUEUE_MAXSIZE` envelope cap would be exceeded. | `268435456` |
| `ASYNC_PRICE_WRITER_SPOOL_PATH` | Optional durable spool file override. Empty defaults beside `DB_PATH` when file-shaped, under `DB_PATH` when directory-shaped, then under `TS_DATA_ROOT` or the platform data root. | empty |
| `ASYNC_PRICE_WRITER_SPOOL_BUSY_TIMEOUT_MS` / `ASYNC_PRICE_WRITER_SPOOL_SYNCHRONOUS` | SQLite writer lock wait and WAL durability mode for the async price-writer spool. Accepted values are `FULL`, `NORMAL`, and `EXTRA`; `NORMAL` remains the default and is safe against process crashes for re-fetchable market data, but a hard OS/power loss can lose the most recent spooled transaction. Set `FULL` on power-unreliable hosts to fsync every spool commit and eliminate that last-transaction loss at a measurable write-throughput cost. This affects market-data spool durability only, never order, ledger, risk, capital, or audit writes. | `50` / `NORMAL` |
| `TELEMETRY_APPEND_BUFFER_SPOOL_PATH` | Optional durable spool file override for high-volume refetchable non-price telemetry writes (`price_provider_health`, `weather_provider_health`, `ingestion_pipeline_health`, `ingest_slippage`, and `price_quotes_raw`). Empty defaults beside `DB_PATH` when file-shaped, under `DB_PATH` when directory-shaped, then under `TS_DATA_ROOT` or the platform data root. | empty |
| `TELEMETRY_APPEND_BUFFER_SPOOL_MAX_BYTES` | Logical payload byte cap for the non-price telemetry SQLite WAL spool. Enqueue returns backpressure and increments rejection/drop counters when either this cap or `TELEMETRY_APPEND_BUFFER_MAX_ROWS` would be exceeded. | `67108864` |
| `TELEMETRY_APPEND_BUFFER_SPOOL_BUSY_TIMEOUT_MS` / `TELEMETRY_APPEND_BUFFER_SPOOL_SYNCHRONOUS` | SQLite writer lock wait and WAL durability mode for the non-price telemetry spool. `NORMAL` matches the price spool default for refetchable telemetry; set `FULL` or `EXTRA` explicitly when local spool fsync strength matters more than enqueue latency. | `50` / `NORMAL` |
| `OPTIONS_POLL_DURABLE_BUFFER_PATH` | Optional durable spool file override for hot options non-price ingestion batches. Empty defaults beside `DB_PATH` when file-shaped, under `DB_PATH` when directory-shaped, then under `TS_DATA_ROOT` or the platform data root. | empty |
| `OPTIONS_POLL_DURABLE_BUFFER_MAX_ROWS` / `OPTIONS_POLL_DURABLE_BUFFER_MAX_BYTES` | Row and logical payload byte caps for the options polling durable spool. A full spool rejects the flush before the DB write starts, increments rejected-row/backpressure counters, and leaves rows available to fail visibly rather than being silently dropped. | `250000` / `134217728` |
| `OPTIONS_POLL_DURABLE_BUFFER_BUSY_TIMEOUT_MS` / `OPTIONS_POLL_DURABLE_BUFFER_SYNCHRONOUS` | SQLite writer lock wait and WAL durability mode for the options polling durable spool. | `5000` / `NORMAL` |
| `OPTIONS_POLL_DURABLE_REPLAY_MAX_ROWS` | Maximum tagged table rows replayed from the options durable spool at the beginning of one `options_poll` cycle. | `50000` |
| `INGESTION_CHILD_TS_PG_POOL_SIZE`, `INGESTION_CHILD_TIMESCALE_POOL_MAX_SIZE`, `INGESTION_CHILD_TIMESCALE_PRICES_POOL_MAX_SIZE` | Per-child ingestion pool caps used by spawned feed jobs so the host profile does not multiply parent-sized pools across processes. Child spawn also caps `ASYNC_PRICE_WRITER_WORKERS` to the child price pool max so every child worker has available price-storage pool capacity. | `2`, `2`, `2` |
| `FEATURE_REGISTRY_CACHE_TTL_S` | Short process TTL for feature registry id lists, stage maps, and frozenset allow-lists keyed by shadow/stage selection. Discovery registration invalidates the cache immediately; `0` disables the cache. | `15` |
| `PREFLIGHT_REQUIRE_CPU_POWER_POLICY` / `PREFLIGHT_CPU_POWER_POLICY_TIMEOUT_S` | Production preflight and observability CPU power verifier controls. Required production/live checks fail closed on drift; continuous observability is advisory and never reapplies the policy. | `0` unless production systemd env or live mode requires it / `3.0` |
| `PREFLIGHT_REQUIRE_MEMORY_PRESSURE_POLICY`, `TRADING_SWAPPINESS`, `TRADING_ZRAM_SIZE_GIB`, `TRADING_SWAPFILE_SIZE_GIB`, `TRADING_SWAPFILE_PATH`, `TRADING_ZFS_ARC_MAX_GIB` | Host memory-pressure verifier controls. The default bart policy requires `vm.swappiness=10`, 32 GiB zram, a 16 GiB `/swapfile-trading`, at least 48 GiB total swap, and `zfs_arc_max=48 GiB`. | required by production systemd preflight; `10`, `32`, `16`, `/swapfile-trading`, `48` |

`observability_snapshot` always records Postgres WAL metrics when the Postgres
snapshot can connect: `postgres.wal_archiver.failed_count`,
`postgres.wal_archiver.seconds_since_last_archive`,
`postgres.wal.directory_bytes`, `postgres.wal.file_count`, and
`postgres.wal.archive_ready_count`. It also records
`postgres.wal.alert_state` as `0=ok`, `1=warning`, `2=critical`, and
`-1=disabled` for any future disabled evaluator state. Critical free-space
evidence emits `STORAGE_FREE_SPACE_CRITICAL`, and confirmed `pg_stat_archiver`
outage blockers emit `WAL_ARCHIVER_OUTAGE`, regardless of `ENGINE_MODE`.
Production-like mode also turns placement, `pg_wal` backlog/free-space risk,
and warning-only transitions into runtime alerts with rule ids
`STORAGE_PLACEMENT_INVALID`, `PG_WAL_DISK_RISK`, or `STORAGE_WAL_WARNING`.
Newly inserted
storage/WAL runtime alerts are delivered through the configured email/webhook
notification channels by `engine.runtime.alerts_notify`. Stable failures are
fingerprinted in-process so the job does not call the alert emitter every 60s;
a recovery clears the fingerprint and a changed WAL segment, backlog, or free
space payload emits a new alert. A rising `pg_stat_archiver.failed_count` or
recent recovered `last_failed_wal` becomes a production-gated
`STORAGE_WAL_WARNING`; unrecovered archive failure, excessive production
`pg_wal` bytes/`.ready` backlog, or critical storage free space remains `CRIT`.

The dashboard alert queue preserves these backend alert rows but presents them
as lifecycle-aware incidents: WARN/HIGH/CRIT are actionable alarms, INFO is a
notification, acknowledgements remain unresolved until a resolution event is
recorded, shelving shows expiry/remaining time, and repeated similar rows are
collapsed into parent incidents. Persisted backend `incident_id` values are not
required yet; the frontend groups by lifecycle family, affected entity, horizon
bucket, and rule/message signature.

`/api/health.storage_wal_guards.storage_placement.targets[*]` is the operator
proof surface for placement. For GO, each high-volume target should show
`evidence_status=satisfied`, `evidence_level=verified_mount`,
`filesystem_type=zfs`, and the expected `host_source`,
`container_destination`, `mount_source`, `mount_point`, and `mount_options`.
`storage_wal_guards.wal_archiver_runtime` reports `archive_mode`,
`archive_command`, freshness, and newer-failure detection from
`pg_stat_archiver`; `storage_wal_guards.pg_wal_disk_risk` reports `wal_bytes`,
`wal_files`, `.ready` backlog, and local free-space evidence.

Dashboard/operator read paths now coalesce hot telemetry, dashboard price, and market-candle reads for 0.75s through `state_cache`. The Timescale telemetry and price read routers use lazy, role-keyed psycopg pools backed by `TIMESCALE_POOL_*` and `TIMESCALE_PRICES_POOL_*`, so repeated reads do not create a new Postgres connection each time. Router env parsing is memoized behind an env/password-source fingerprint and refreshes only when relevant settings change. Pool close hooks run on test cleanup and process shutdown. Cache-wrapper L1 hits are intentionally short at 0.5s; kill-switch and execution-mode wrappers only cache states that cannot make live execution more permissive.

## Ingestion Backpressure Signals

When raising `INGESTION_TUNING_PROFILE` or any writer batch/pool knob, confirm throughput is increasing instead of only building queues:

- `/api/health.ingestion_soak`: this is the first-pass acceptance report. In production-like modes, `ok=false` blocks `/api/readiness` and `prod_preflight.py` when live writer evidence is missing, async price/telemetry/options queues exceed threshold, durable spools grow past age or size thresholds, residual loss/drop/dead-letter/corruption counters are nonzero, COPY falls back unexpectedly, write/flush failures are nonzero, Redis cache circuit is open, required scoring indexes are missing, or Timescale hypertable chunk/compression policy evidence is absent. Use `INGESTION_SOAK_TIMESCALE_POLICY_JSON` or `PREFLIGHT_INGESTION_SOAK_POLICY_JSON` only for archived read-only evidence; otherwise the report queries Timescale directly when production evidence is required.
- `/api/health`: inspect `async_price_persistence.worker_count`, `async_price_persistence.worker_alive_count`, `async_price_persistence.queue_depth`, `async_price_persistence.queue_rows`, `async_price_persistence.queue_fill_ratio`, `async_price_persistence.shards[*].queue_depth`, `async_price_persistence.shards[*].pending_lag_ms`, `async_price_persistence.shards[*].batch_size`, `async_price_persistence.shards[*].last_batch_rows`, `async_price_persistence.shards[*].last_db_write_duration_ms`, `async_price_persistence.shards[*].write_failures`, `async_price_persistence.shards[*].backpressure_events`, `async_price_persistence.spool_pending_bytes`, `async_price_persistence.spool_bytes_fill_ratio`, `async_price_persistence.spool_oldest_age_ms`, `async_price_persistence.spool_synchronous`, `async_price_persistence.spooled_rows`, `async_price_persistence.spool_deleted_rows`, `async_price_persistence.replayed_rows`, `async_price_persistence.spool_corruption_events`, `async_price_persistence.backpressure_active`, `async_price_persistence.backpressure_recovered_events`, `async_price_persistence.rejected_rows`, `async_price_persistence.dropped_rows`, `async_price_persistence.residual_loss_rows`, `async_price_persistence.dead_letters`, `pg_price_storage.last_write_duration_ms`, `pg_price_storage.last_write_path`, `pg_price_storage.copy_batches`, `pg_price_storage.copy_rows`, `pg_price_storage.values_batches`, `pg_price_storage.values_rows`, `pg_price_storage.copy_fallbacks`, `pg_price_storage.write_failures`, `pg_price_storage.retryable_failures`, `pg_price_storage.fatal_failures`, `pg_price_storage.pool_resets`, `pg_price_storage.write_circuit_open`, `pg_price_storage.write_circuit_rejected_batches`, `timescale.queue_depth`, `timescale.metrics.backpressure_count`, `timescale.metrics.copy_batches`, `timescale.metrics.copy_fallback_count`, `timescale.metrics.executemany_batches`, `timescale.metrics.deduped_rows`, `timescale.metrics.last_write_path`, `timescale.metrics.last_db_write_duration_ms`, `telemetry_append_buffer.buffered_rows`, `telemetry_append_buffer.oldest_age_ms`, `telemetry_append_buffer.backpressure_events`, `telemetry_append_buffer.backpressure_recovered_events`, and `ingestion_pipeline_health.meta` for `job_name=options_poll` fields such as `state_load_queries`, `provider_fetch_max_workers`, `commit_batches`, `max_symbols_per_commit`, `rows_written`, `event_rows_written`, `state_rows_written`, `copy_staging_batches`, `executemany_batches`, `copy_fallbacks`, `cached_fallback_symbols`, `bulk_write_failures`, `event_write_failures`, `state_write_failures`, `durable_buffer_pending_rows`, `durable_buffer_pending_bytes`, `durable_buffer_oldest_age_ms`, `durable_buffer_replayed_rows`, `durable_buffer_deleted_rows`, `durable_buffer_rejected_rows`, `durable_buffer_dropped_rows`, `durable_buffer_backpressure_active`, and `durable_buffer_backpressure_events`.
- `runtime_metrics`: inspect CCXT `ccxt_live_fetch_cycles` `path` and `supports_batch` tags, `batch_failure_reason`, `ccxt_live_exchange_cache_hits`/`ccxt_live_exchange_cache_misses`/`ccxt_live_exchange_cache_evictions`, `ccxt_live_markets_loads`/`ccxt_live_markets_reuses`, `ccxt_live_market_cache_hits`/`ccxt_live_market_cache_reloads`/`ccxt_live_market_cache_invalidations`, `ccxt_live_fetch_tickers_attempts`/`ccxt_live_fetch_tickers_successes`/`ccxt_live_fetch_tickers_failures`/`ccxt_live_fetch_tickers_cycle_failures`, `ccxt_live_fetch_tickers_calls`/`ccxt_live_fetch_tickers_rows`, `ccxt_live_fetch_tickers_unsupported`, `ccxt_live_fetch_tickers_partial_misses` split by `reason`, `ccxt_live_fetch_tickers_missing_symbols`, `ccxt_live_failed_symbols`, `ccxt_live_fallback_fetches`/`ccxt_live_fallback_rows`/`ccxt_live_fallback_successes`/`ccxt_live_fallback_failures`, and `ccxt_live_markets_load_latency_ms`/`ccxt_live_fetch_tickers_latency_ms`/`ccxt_live_fallback_fetch_latency_ms` to confirm multi-symbol crypto cycles are reusing exchanges, using exchange-native batching when the venue supports it, and reporting missing or failed requested symbols.
- For the non-price telemetry spool, also inspect `telemetry_append_buffer.write_path`, `queue_depth`, `queue_fill_ratio`, `oldest_age_ms`, `spool_pending_rows`, `spool_pending_bytes`, `spool_file_bytes`, `spool_max_rows`, `spool_max_bytes`, `spool_oldest_age_ms`, `spool_synchronous`, `pending_by_table`, `accepted_rows`, `replayed_rows`, `committed_rows`, `deleted_rows`, `dropped_rows`, `retry_count`, `shutdown_drain_attempts`, `shutdown_drain_failures`, `steady_state_write_timeout_s`, `shutdown_drain_write_timeout_cap_s`, `backpressure_active`, `backpressure_events`, `backpressure_recovered_events`, `last_backpressure_reason`, `shutdown_drained_rows`, `residual_spooled_rows`, `residual_dropped_rows`, `residual_loss_rows`, `shutdown_deadline_exhausted`, `spool_unavailable_count`, `spool_corrupt_rows`, and `spool_corruption_events`. Rows are selected without deletion, written through the table-specific transaction, and deleted from the spool only after the DB commit returns. Normal background/manual flushes stay on the fast steady-state policy (`attempts=1`, `0.25s` write timeout). Shutdown uses the runtime shutdown deadline with up to four bounded retry attempts and a larger per-attempt cap, so SQLite/Postgres contention cannot block indefinitely. Shutdown emits `telemetry_append_buffer_residual_spooled_rows` for durable rows left for replay and both `telemetry_append_buffer_residual_dropped_rows` and `telemetry_append_buffer_residual_loss_rows` for rows left only in memory at the deadline.
- For the options poll durable spool, inspect `options.poll.durable_buffer.*` runtime metrics and the `options_poll` ingestion-pipeline-health metadata. Normal soak should show pending rows/bytes and oldest age returning to zero or the normal flush interval, deleted rows tracking spooled rows after successful commits, `rejected_rows=0`, `dropped_rows=0`, `backpressure_active=0`, and no replay/delete/corruption failures.
- `runtime_meta.ingestion_state.writer_diagnostics`: inspect `degraded_reasons`; async price writer high-watermark backpressure, durable spool byte pressure, drops/rejections, dead letters, spool corruption, Timescale backpressure, and price-storage errors mark ingestion degraded. Recovery emits `async_price_writer_backpressure_recovered_events` and a component-health `backpressure_recovered` state. Shutdown deadline rows stay in the SQLite spool as `residual_spooled_rows` and replay on startup; they are not counted as residual drops. Whole-runtime shutdown also writes a `runtime_shutdown_drain` event and `runtime_shutdown_drain` component-health snapshot summarizing async price writer and telemetry residual risk before SIGKILL escalation.
- `runtime_meta.ingestion_state.children[*].restart_guard`: inspect persisted restart-window `count`, `limit`, `window_s`, `suppressed`, and `suppressed_until_ts_ms` when a child is disabled by the storm guard. The same suppression is visible as `ingestion_child_restart_suppressed_total` metrics and `ingestion_child_restart_suppressed` event-log rows.
- Fast price producers consume the router's `async_persistence` status: `poll_prices` keeps a failure backoff when async persistence or provider auxiliary enqueue is backpressured, while Polygon WS pauses the stream loop and requeues live events if an enqueue is rejected.
- `poll_prices` keeps runtime Postgres pools warm across ordinary heartbeat and polling cycles. DB lock waits, statement timeouts, and pool-acquisition timeouts surface as `poll_prices_*_write_busy` events and cycle backoff; they should not show up as repeated pooled-connection teardown/rebuild churn.
- `poll_prices` symbol discovery is cycle-local. Provider subscriptions, provider snapshots, and stale-symbol maintenance should all reflect one active/watch universe read per polling cycle; intentional symbol changes should appear on the next cycle, not halfway through the current cycle.
- Polygon WS message handling keeps parse, validation, quote spread calculation, and legacy microstructure updates outside the shared `_last` lock. The lock only snapshots/commits current symbol state, per-stream ordering watermarks, duplicate keys, telemetry counters, and pending flush queue mutation, so `/api/health` and flush snapshots can continue reading while different-symbol message CPU work overlaps.
- `/api/execution/barrier`: confirm stale critical sources or `ingestion_stale` are blocking execution when prices lag. Live trading safety must not depend on ingestion catching up silently.

Read-only Timescale policy evidence commands:

```bash
python - <<'PY'
from engine.runtime.health import get_health_snapshot
import json
print(json.dumps(get_health_snapshot().get("ingestion_soak", {}), indent=2, sort_keys=True))
PY
psql "$TIMESCALE_DSN" -c "SELECT hypertable_schema, hypertable_name, num_dimensions, compression_enabled FROM timescaledb_information.hypertables ORDER BY hypertable_name;"
psql "$TIMESCALE_DSN" -c "SELECT hypertable_name, column_name, time_interval, integer_interval FROM timescaledb_information.dimensions ORDER BY hypertable_name;"
psql "$TIMESCALE_DSN" -c "SELECT hypertable_name, proc_name, schedule_interval FROM timescaledb_information.jobs WHERE proc_name IN ('policy_compression','policy_retention') ORDER BY hypertable_name, proc_name;"
psql "$TIMESCALE_DSN" -c "SELECT tablename, indexname FROM pg_indexes WHERE schemaname = current_schema() ORDER BY tablename, indexname;"
```

Run `python tools/ingestion_spine_benchmark.py --skip-postgres` for a local durable-spool smoke benchmark. Add `--postgres-dsn postgresql://127.0.0.1:5432/... --include-values-fallback` to measure price COPY/staging and COPY-disabled VALUES fallback against a local backend. The tool refuses non-loopback DSNs unless `--allow-production-target` is supplied and writes JSON to `var/artifacts/ingestion_spine_benchmark.json` by default.

## First Inspection Path

Use this order when the system looks unhealthy:

1. `GET /api/execution/barrier`
   Confirms whether the runtime is blocked from execution and surfaces the primary reason.
2. `GET /api/health`
   Shows the broad health snapshot used by the dashboard, including
   `disk_pressure` warnings and critical blockers before root, runtime data,
   runtime logs, or backup storage blocks writes. Inspect `memory_pressure.ok`,
   `memory_pressure.status`, `memory_pressure.errors`,
   `effective_runtime_state.ok`, `effective_runtime_state.errors`,
   `storage_wal_guards.ok`,
   `storage_wal_guards.blockers`, `storage_wal_guards.storage_placement`,
   `storage_wal_guards.wal_archiver_runtime`, and
   `storage_wal_guards.pg_wal_disk_risk` before treating storage/WAL as GO.
3. `GET /api/operator/runtime_watchdogs`
   Surfaces stale jobs, provider freshness, and watchdog counters.
4. `GET /api/operator/provider_telemetry`
   Confirms whether price and provider pipelines are actually alive.
5. `GET /api/operator/support_snapshot?mode=repair`
   Pulls the full repair bundle with DB debug state, recent failures, and synthesized diagnostics.
6. `sudo /opt/trading/ops/backup/accounting.sh`
   Confirms `/var/backups/trading` host/container accounting and retention
   before deleting any backup or Docker data.

## Repo Checks And Probes

The repository already includes lightweight observability-oriented checks:

- `python tools/validate_repo.py`
- `python tools/validate_docs.py`
- `python tools/runtime_graph_check.py`
- `python tools/runtime_stability_probe.py`
- `python tools/noop_guard.py`

`runtime_graph_check.py --mode startup` is the hermetic import/startup graph
used by `validate_repo.py`. Running `runtime_graph_check.py` without a mode is
the full environment check and may require the local Postgres/schema, credential
directory, and other configured runtime dependencies to be present. For local
full-mode checks against a passwordless `TS_PG_DSN`, provide a file-backed
Postgres password source such as `TS_PG_PASSWORD_FILE`; the startup-mode graph
still scrubs external services and raw secret values for hermetic validation.

`runtime_stability_probe.py` always waits for the dashboard health endpoint and
samples dashboard APIs with the dashboard token when one is configured. The
operator-status sample is skipped by default unless `--operator-url`,
`PIPELINE_SMOKE_OPERATOR_BASE`, or `--require-operator` is set; that keeps an
absent operator sidecar from failing a dashboard stability run. The probe loads
the repo `.env` and uses the same secret-file resolver as dashboard auth, so
local runs can use `DASHBOARD_API_TOKEN_FILE` without exporting the token value.
Set `PIPELINE_SMOKE_OPERATOR_TOKEN_FILE` or `OPERATOR_API_TOKEN_FILE` for the
direct `:4001` sidecar, and add `--require-operator` when the operator sample
must be a hard probe requirement.

`safe_sim_boot_smoke.py` is the reproducible local safe/sim acceptance probe.
It prepares a file-backed env from `.env.codex-sim-paper.bak`, starts the
dashboard and operator sidecar with live execution disabled and the global kill
switch held, and pins storage/read routers to local sqlite so a clean checkout
does not require an existing Docker/Postgres stack. It verifies the dashboard
entered `serve_forever`, then checks
`/api/system/kill_switches`, `/api/broker/config`,
`/api/execution/barrier`, and `/api/operator/readiness_evidence` through both
the dashboard token flow and the operator proxy token flow. The execution
barrier is considered healthy for this smoke when it is populated and
`allowed=false`, because safe/sim boot must prove trading is blocked. It prints
only statuses and token-source names.

Use `python tools/validate_repo.py --live` only against a running stack when a live smoke test is intended.
