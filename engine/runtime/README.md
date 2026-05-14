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

- [storage.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\storage.py)
  Central SQLite access layer, connection management, pragmas, transaction helpers, schema helpers, and DB validation.
- [locks.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\locks.py)
  Cross-process job locks and `job_history` persistence.
- [runtime_meta.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\runtime_meta.py)
  Shared metadata store used for diagnostics and boot progress.
- [job_registry.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\job_registry.py)
  Canonical registry of runnable jobs and pipeline order.
- [jobs_manager.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\jobs_manager.py)
  Starts and stops jobs, owns their subprocesses, logs, heartbeats, and restart behavior.
- [supervisor.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\supervisor.py)
  Higher-level dependency-aware job starting and graph validation.
- [startup_orchestrator.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\startup_orchestrator.py)
  Post-bind startup flow that seeds sources and the early pipeline.
- [ingestion_runtime.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\ingestion_runtime.py)
  Supervises market-data/feed children and provider-state recovery.
- [lifecycle_state.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\lifecycle_state.py)
  Persistent runtime lifecycle state machine.
- [health.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\health.py)
  Health snapshots and preflight checks used by UI and bootstrap.
- [ingestion_status.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\ingestion_status.py)
  Per-pipeline ingestion health snapshots, counters, freshness, and summaries consumed by diagnostics and provider monitoring.
- [alerts.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\alerts.py)
  Alert persistence, thresholding, dedupe, and alert-side event publishing.
- [price_router.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\price_router.py)
  Runtime-owned feed selection and fallback routing used by newer price-provider control surfaces.
- [position_store.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\position_store.py)
  Shared persistence helpers for broker positions and position-facing read models.
- [trade_lifecycle.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\trade_lifecycle.py)
  Cross-table trace builder that reconstructs the alert-to-order-to-fill lifecycle for audits and regressions.
- [allocator_status.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\allocator_status.py)
  Read-side snapshot builder for allocator freshness, capacity, and status diagnostics.
- [crash_recovery.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\crash_recovery.py)
  Broker/execution recovery helpers that reconstruct submit/fill state after a restart.
- [runtime_bootstrap.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\runtime_bootstrap.py)
  Idempotent bootstrap helpers for schema and startup state.
- [first_run.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\first_run.py)
  Initial schema/bootstrap path.
- [db_repair.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\db_repair.py)
  Defensive schema/data repair helpers.
- [event_log.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\event_log.py)
  Runtime event persistence.
- [event_bus.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\event_bus.py)
  Internal pub/sub and event fanout.
- [event_runtime.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\event_runtime.py)
  Event-driven bridge that reacts to price ticks, predictions, and strategy signals without coupling those domains directly.
- [ipc.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\ipc.py)
  Cross-process IPC used by ingestion and other supervised children.
- [observability.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\observability.py)
  Shared component-health and rolling-rate helpers used by async sidecars and long-lived services.
- [async_writer.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\async_writer.py)
  Queue-backed batch writer that offloads append-heavy price persistence from the hot path.
- [storage_pg_prices.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\storage_pg_prices.py)
  Postgres/Timescale-oriented price persistence sidecar for append-heavy market data.
- [timescale_client.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\timescale_client.py)
  Background TimescaleDB client for hypertable writes and schema management on newer time-series surfaces.
- [model_cache.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\model_cache.py)
  In-memory catalog cache used by serving and governance readers to avoid repeated registry scans.
- [price_cache.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\price_cache.py)
  Runtime-facing wrapper around the data price cache that exposes health-oriented accessors to other subsystems.
- [shutdown.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\shutdown.py)
  Graceful runtime stop, event logging, pooled-connection cleanup, and WAL checkpoint handling used by server shutdown paths.

## Newer Sidecars And Services

- Async persistence:
  [async_writer.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\async_writer.py),
  [storage_pg_prices.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\storage_pg_prices.py), and
  [timescale_client.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\timescale_client.py).
- Event-driven orchestration:
  [event_bus.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\event_bus.py),
  [event_log.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\event_log.py), and
  [event_runtime.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\event_runtime.py).
- Observability and caches:
  [observability.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\observability.py),
  [model_cache.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\model_cache.py), and
  [price_cache.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\price_cache.py).

## Maintenance Guidance

- When changing startup behavior, review:
  [start_system.py](c:\Users\dschi\Documents\GitHub\Trading-System-\start_system.py), [dashboard_server.py](c:\Users\dschi\Documents\GitHub\Trading-System-\dashboard_server.py), [startup_orchestrator.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\startup_orchestrator.py), and [ingestion_runtime.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\ingestion_runtime.py) together.
- When changing job semantics, update:
  [job_registry.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\job_registry.py), [jobs_manager.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\jobs_manager.py), and this README.
- Keep blocking DB work out of hot control-plane paths.
  Startup and job start behavior are sensitive to synchronous SQLite writes and integrity checks.
- Keep lock naming consistent.
  One-shot jobs use `job:<name>` lock names; runtime lock mismatches cause startup failures and restart loops.
- Treat new sidecars as optional but observable.
  Async persistence, Timescale storage, and event-runtime helpers should fail open while still surfacing clear health snapshots to operators. The append-heavy market feature path now reports its explicit mode through `storage.get_timeseries_storage_snapshot()["market_feature_store"]`.

## Common Extension Points

- Add a new job:
  register it in [job_registry.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\job_registry.py), ensure its script path is importable, and decide whether it belongs in startup orchestration.
- Add a new health signal:
  wire it through [health.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\health.py), `runtime_meta`, and the dashboard handler layer.
- Add a new ingestion-health signal:
  update [ingestion_status.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\ingestion_status.py), the provider monitor job, and any API/UI readers together so freshness and counters stay coherent.
- Add a new runtime state transition:
  update [lifecycle_state.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\runtime\lifecycle_state.py) and any readers in API/UI code.
- Add a new operator diagnostic or support-snapshot field:
  update runtime ownership here first, then wire the read contract through [engine/api/api_system.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\api\api_system.py) and the operator layer.
