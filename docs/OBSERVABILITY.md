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
| Ingestion writer diagnostics | `engine/runtime/ingestion_runtime.py`, `engine/runtime/async_writer.py`, `engine/runtime/telemetry_append_buffer.py`, `engine/runtime/timescale_client.py`, `engine/runtime/storage_pg_prices.py` | Durable spool depth/bytes, flush latency, dropped rows, retry counts, corruption counters, and DB write duration for ingestion writers. The ingestion runtime embeds these snapshots in its heartbeat and `runtime_meta.ingestion_state`; `/api/health` also exposes `async_price_persistence`, `pg_price_storage`, `timescale`, and `telemetry_append_buffer`. |
| Execution barrier | `engine/runtime/gates.py` and `engine/api/api_system.py` | Fail-closed execution state from `/api/execution/barrier`. |
| Operator logs | `var/log/` and `boot/operator_server.js` | Runtime log tails, stderr tails, and operator-side snapshots exposed through the Node operator server. |
| Disk pressure | `engine/runtime/health.py` and `engine/runtime/prod_preflight.py` | Per-path filesystem headroom for `/`, runtime data, runtime logs, backups, and Docker roots when present. |
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
| `GET /api/operator/runtime_watchdogs` | Heartbeat and freshness watchdog state for critical jobs. |
| `GET /api/operator/support_snapshot` | Repair-oriented evidence bundle for humans and tooling. |
| `GET /api/telemetry` and `GET /api/telemetry/history` | Telemetry APIs exposed by `engine/api/api_system.py`. |

## Important Configuration

| Variable | Meaning | Default In Code |
| --- | --- | --- |
| `OBS_COMPONENT_HEALTH_TTL_S` | How long a component-health record stays fresh before it is marked stale. | `900` |
| `OBS_RATE_WINDOW` | Rolling observation window used by `record_rolling_rate(...)`. | `50` |
| `API_HEALTH_CACHE_TTL_S` | Cache TTL for `/api/health` snapshots. | `3.0` |
| `DISK_PRESSURE_WARN_FREE_PCT` / `DISK_PRESSURE_WARN_FREE_BYTES` | Warning threshold for filesystem headroom in health and preflight. | `15` / `21474836480` |
| `DISK_PRESSURE_CRITICAL_FREE_PCT` / `DISK_PRESSURE_CRITICAL_FREE_BYTES` | Critical threshold that fails startup validation and production preflight. | `5` / `5368709120` |
| `BACKUP_ACCOUNTING_DU_TIMEOUT_S` | Timeout for backup accounting size checks. | `8` |
| `INGESTION_TUNING_PROFILE` | Optional bounded ingestion profile. `safe` keeps conservative defaults; `host_32t_123g` increases writer batch and pool throughput for the 32-thread / 123 GiB host while reducing queue depths so the buffered-row risk window does not expand. | `safe` |
| `INGESTION_TUNING_MAX_TOTAL_DB_CONNECTIONS` | Hard preflight budget for the runtime PG pool plus enabled Timescale and price-storage pools. | `24` |
| `INGESTION_TUNING_MAX_BUFFERED_ROWS` | Hard preflight budget for estimated buffered rows across Timescale, async price writer, telemetry append, event-log, runtime-metrics, and runtime-meta buffers. | `1200000` |
| `ASYNC_PRICE_WRITER_SPOOL_MAX_BYTES` | Logical payload byte cap for the SQLite WAL async price-writer spool. Enqueue rejects and dead-letters new envelopes when the cap or `ASYNC_PRICE_WRITER_QUEUE_MAXSIZE` envelope cap would be exceeded. | `268435456` |
| `ASYNC_PRICE_WRITER_SPOOL_PATH` | Optional durable spool file override. Empty defaults beside `DB_PATH` when file-shaped, under `DB_PATH` when directory-shaped, then under `TS_DATA_ROOT` or the platform data root. | empty |
| `INGESTION_CHILD_TS_PG_POOL_SIZE`, `INGESTION_CHILD_TIMESCALE_POOL_MAX_SIZE`, `INGESTION_CHILD_TIMESCALE_PRICES_POOL_MAX_SIZE` | Per-child ingestion pool caps used by spawned feed jobs so the host profile does not multiply parent-sized pools across processes. | `2`, `2`, `2` |

## Ingestion Backpressure Signals

When raising `INGESTION_TUNING_PROFILE` or any writer batch/pool knob, confirm throughput is increasing instead of only building queues:

- `/api/health`: inspect `async_price_persistence.queue_depth`, `async_price_persistence.queue_fill_ratio`, `async_price_persistence.spool_pending_bytes`, `async_price_persistence.spool_bytes_fill_ratio`, `async_price_persistence.spool_corruption_events`, `async_price_persistence.backpressure_active`, `async_price_persistence.dropped_rows`, `async_price_persistence.residual_dropped_rows`, `async_price_persistence.dead_letters`, `pg_price_storage.last_write_duration_ms`, `timescale.queue_depth`, `timescale.metrics.backpressure_count`, `timescale.metrics.last_db_write_duration_ms`, and `telemetry_append_buffer.buffered_rows`.
- `runtime_meta.ingestion_state.writer_diagnostics`: inspect `degraded_reasons`; async price writer high-watermark backpressure, durable spool byte pressure, drops, dead letters, spool corruption, Timescale backpressure, and price-storage errors mark ingestion degraded. Shutdown deadline rows stay in the SQLite spool as `residual_spooled_rows` and replay on startup; they are not counted as residual drops.
- `/api/execution/barrier`: confirm stale critical sources or `ingestion_stale` are blocking execution when prices lag. Live trading safety must not depend on ingestion catching up silently.

## First Inspection Path

Use this order when the system looks unhealthy:

1. `GET /api/execution/barrier`
   Confirms whether the runtime is blocked from execution and surfaces the primary reason.
2. `GET /api/health`
   Shows the broad health snapshot used by the dashboard, including
   `disk_pressure` warnings before root, runtime data, runtime logs, or backup
   storage blocks writes.
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

Use `python tools/validate_repo.py --live` only against a running stack when a live smoke test is intended.
