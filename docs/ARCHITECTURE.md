# System Architecture

This document describes the runtime that is actually assembled by `start_system.py`, `engine/api/server.py`, `dashboard_server.py`, `start_ingestion.py`, the supervision modules under `engine/runtime/`, the strategy and execution stack under `engine/strategy/` and `engine/execution/`, and the operator surfaces in `boot/`, `services/`, `routes/`, and `ui/`.

## System Purpose

This repository is a supervised trading runtime with five operational responsibilities:

1. Start and keep the local trading runtime coherent.
2. Ingest market, event, news, social, macro, and provider-health data.
3. Produce predictions, decision records, portfolio intents, and model-governance decisions.
4. Gate, route, and attribute execution.
5. Expose operator, dashboard, data-source, and browser-terminal control planes.

The authoritative startup entrypoint is `start_system.py`. The authoritative Python HTTP/control-plane startup boundary is `engine/api/server.py`, with `dashboard_server.py` retained as the compatibility host for the aggregated UI/API surface. The isolated ingestion entrypoint is `start_ingestion.py`.

## Major Layers

| Layer | Primary files | Runtime responsibility |
| --- | --- | --- |
| Bootstrap and lifecycle | `start_system.py`, `start_ingestion.py`, `engine/api/server.py`, `dashboard_server.py`, `engine/runtime/lifecycle_state.py` | Bootstraps environment, validates runtime architecture, sets lifecycle state, binds the HTTP server, supervises startup, and shuts down cleanly or degrades safely. |
| Runtime supervision | `engine/runtime/job_registry.py`, `engine/runtime/supervisor.py`, `engine/runtime/startup_orchestrator.py`, `engine/runtime/ingestion_runtime.py` | Defines allowed jobs, dependency order, auto-boot rules, ingestion child supervision, and the bounded startup pipeline. |
| Data-source control plane | `services/data_source_manager.py`, `routes/data_sources_routes.py` | Stores source configuration, projects credentials and settings into job environments, computes desired ingestion jobs, and exposes `/api/data_sources*`. |
| Strategy and model layer | `engine/strategy/model_intent.py`, `engine/strategy/validation.py`, `engine/strategy/decision_log.py`, `engine/strategy/portfolio.py`, `engine/strategy/champion_manager.py`, `engine/decision_engine.py` | Turns market and event data into predictions, canonical model intents, auditable decision records, portfolio orders, and champion/challenger assignments. |
| Execution and post-trade attribution | `engine/runtime/gates.py`, `engine/execution/kill_switch.py`, `engine/strategy/portfolio_execution_intents.py`, `engine/execution/broker_apply_orders.py`, `engine/execution/execution_poll_and_attrib.py`, `engine/execution/execution_ledger.py` | Applies runtime and risk gates, shapes execution payloads, routes orders, records fills, computes execution metrics, and materializes PnL attribution. |
| API and UI surfaces | `engine/api/server.py`, `dashboard_server.py`, `engine/api/*`, `engine/terminal/api/*`, `ui/*` | Serves the dashboard, data-source control plane, browser terminal, and the main JSON APIs used by operators and UI panels. |
| Operator sidecar | `boot/operator_server.js`, `services/operator_ai/agent.js` | Provides a separate Node.js operator server, runtime start/stop/emergency-stop controls, snapshot aggregation, and diagnostics-oriented AI support tooling. |

## Process And Runtime Topology

The runtime is not a single monolith. It is a supervised Python process plus optional child processes and a separate Node.js operator sidecar.

```text
start_system.py
  -> same-process engine/api/server.py
       -> compatibility host: dashboard_server.py
       -> HTTP server for /api/* and /ui/*
       -> background threads: lifecycle monitor, model scoring, auto rollback,
          startup orchestrator, optional auto pipeline / challenger / size policy loops
  -> child process: start_ingestion.py
       -> engine/runtime/ingestion_runtime.py
            -> child ingestion daemons selected from data-source config
  -> best-effort auxiliary services
       -> challenger runtime
       -> async writer
       -> pg price storage
       -> event runtime

boot/operator_server.js
  -> separate Node.js control plane
       -> operator snapshots
       -> runtime start/stop/emergency stop
       -> operator proxy routes
       -> AI patch preview/apply/rollback routes
```

Two topology details matter operationally:

- `dashboard_server.run_server()` binds HTTP before post-bind startup work. That means `/api/*` and `/ui/*` can come up while the runtime is still warming, validating, or repairing.
- `start_system.py` defers ingestion spawn until after the dashboard bind succeeds, then waits on startup health. If health never stabilizes, the runtime can fail open into `DEGRADED` when `TRADING_STARTUP_HEALTH_FAIL_OPEN` is enabled.

## Startup Sequence

The observed startup order is:

1. `start_system.py` bootstraps the environment.
   - Loads `.env`.
   - Clears the dead local proxy sentinel `http://127.0.0.1:9` if present.
   - Creates default `logs/` and `data/`.
   - Sets `TRADING_LOGS`, `TRADING_DATA`, and `DB_PATH`. `DB_PATH` is a local data-root/legacy compatibility hint; Postgres connection targets come from `TS_PG_DSN` or platform defaults.
2. `start_system.py` writes startup breadcrumbs into `runtime_meta`.
   - `startup_trace`
   - `import_smoke`
3. `start_system.py` validates the runtime graph with `engine.runtime.job_registry.validate_runtime_architecture()`.
4. `engine.runtime.lifecycle_state.set_lifecycle_state()` moves the runtime into `BOOTING`.
5. `engine.runtime.first_run.bootstrap_first_run(mode=mode)` runs first-run/bootstrap setup.
6. `services.data_source_manager.get_manager().initialize()` and `.apply_runtime_environment()` project enabled source settings into the process environment.
7. `start_system.py` starts best-effort side services.
   - cache warm
   - challenger runtime
   - async writer
   - pg price storage
   - event runtime
8. `start_system.py` imports and calls `engine.api.server.run_server()`.
9. `engine.api.server.run_server()` binds the HTTP server through the compatibility host and then performs post-bind boot work.
   - Marks lifecycle `WARMING_UP` if the dashboard binds before the first confirmed price tick.
   - Starts the lifecycle monitor.
   - Starts the model scoring service.
   - Starts `auto_rollback_loop`.
   - Runs bounded preflight and may call `api_post_self_repair()` if preflight fails.
   - Validates the dependency graph through the supervisor.
   - Auto-boots daemon jobs when configured.
   - Skips dashboard-side feed auto-boot when isolated ingestion owns feeds.
   - May start `StartupOrchestrator`.
10. After the dashboard bind, `start_system.py` spawns `start_ingestion.py` when ingestion is enabled.
11. `start_ingestion.py` sets supervised env flags, repairs the DB, writes an initial `runtime_meta["ingestion_state"]`, and then hands off to `engine.runtime.ingestion_runtime.main()`.
12. `start_system.py` starts the ingestion watchdog and waits for startup health.
   - Healthy startup ends in `LIVE`.
   - Configured fail-open startup can end in `DEGRADED`.
13. Shutdown and fatal paths call `runtime_shutdown()`, flush logs, terminate the ingestion child, and mark lifecycle as `SHUTTING_DOWN` or `DEGRADED` depending on whether shutdown was clean.

## Ingestion And Data Flow

### Source-of-truth for enabled feeds

`services/data_source_manager.py` is the control-plane authority for ingestible sources. It persists source records in `data_sources`, source logs in `data_source_logs`, audit rows in `data_source_audit`, and runtime dirtiness markers in `runtime_meta`.

Important control-plane functions:

- `list_sources()` returns materialized source records with status, masked credentials, field definitions, and editability flags.
- `build_job_environment(job_name)` projects enabled source credentials and settings into env vars for individual jobs.
- `get_desired_ingestion_jobs()` computes which ingestion daemons should exist right now.
- `manage_lifecycle()` returns the desired jobs and can start ingestion runtime if needed.

### Isolated ingestion runtime

`start_ingestion.py` is a thin wrapper around `engine/runtime/ingestion_runtime.py`. The ingestion runtime:

- owns isolated child supervision for ingestion jobs
- starts and stops children based on the data-source manager's desired job set
- restarts stale children with backoff
- disables restart on fatal provider-auth failures
- publishes heartbeat and channel state into `runtime_meta["ingestion_state"]`

The canonical ingestion children come from `engine/runtime/job_registry.py` and the source manager. In this repo that includes jobs such as:

- `stream_prices_polygon_ws`
- `stream_prices_ibkr`
- `poll_prices`
- `options_poll`
- `poll_macro`
- `poll_earnings`
- `poll_gdelt`
- `poll_sec_filings`
- `ingest_form4`
- `ingest_congressional_trades`
- the weather and social ingestion daemons listed in `ALLOWED_JOBS`

### Bootstrap pipeline

The bounded startup pipeline in `engine/runtime/startup_orchestrator.py` is a fail-closed sequence that persists progress to `runtime_meta["startup_orchestrator_progress"]`.

The concrete orchestrator order is:

1. `preflight_deferred`
2. `db_counts_initial`
3. `daemon_status_skipped`
4. `health_ready`
5. `update_universe` when needed
6. `symbols_ready`
7. `prices_ready`, or `start_poll_prices` followed by `prices_ready_retry`
8. `poll_gdelt`
9. `poll_sec_filings`
10. `poll_earnings`
11. `ingest_now`
12. `process_events`
13. `label_due_events`
14. `compute_drift`
15. `train_embed_models`
16. `train_model_v2`
17. `validate_now`
18. `process_events` again
19. `final_state`

The orchestrator only marks the lifecycle `LIVE` when prices are present.

## Strategy And Model Flow

The strategy stack uses auditable, table-backed transitions rather than passing an opaque execution blob from ingest directly to the broker.

### Prediction and intent path

1. Prediction writers in `engine/strategy/validation.py` persist:
   - an append-only row in `prediction_history`
   - the latest point-in-time row in `predictions`
2. `engine/strategy/model_intent.py` builds the canonical model-intent payload with `build_model_intent(...)`.
3. `engine/strategy/decision_log.py` writes `decision_log` rows so every prediction-to-decision step has an explainability record.
4. `engine/strategy/portfolio.py` maintains `portfolio_state` and writes `portfolio_orders`.
5. `engine/strategy/portfolio_execution_intents.py` converts the latest batch of `portfolio_orders` into execution-ready intents. It enriches those intents with:
   - alert lineage and timing
   - model identity
   - competition policy from `engine/strategy/champion_manager.py`
   - execution target (`real` or `shadow`)
   - budget and execution-regime context
6. `engine/decision_engine.py` can downgrade an otherwise real intent into `shadow` before the broker layer sees it.

### Model governance path

There are two related governance paths in this repo:

- `engine/strategy/champion_manager.py` manages symbol, horizon, and regime-specific champion assignments in `champion_assignments`.
- `engine/strategy/promotion_guard.py` and `engine/strategy/promotion_hardening.py` manage promotion eligibility, probation watches, and rollback.

The competition cycle is not hypothetical. `evaluate_competition_cycle()` makes assignment decisions with concrete branch reasons such as:

- `keep_current`
- `no_bootstrap`
- `bootstrap_best`
- `demotion_drawdown`
- `demotion_decay`
- `demotion_fallback`
- `challenger_outperformance`
- `best_blocked_self_critic`
- `replay_gate_blocked`
- `*_stat_gate_blocked`

## Execution Flow

The execution path is multi-stage and intentionally layered.

### Pre-submit path

1. `engine/strategy/portfolio_execution_intents.py` loads the latest execution intents.
2. `engine/execution/broker_apply_orders.py` is the main live order-application entrypoint.
3. `broker_apply_orders.py` evaluates the hard execution barrier from `engine/runtime/gates.py`.
4. It then applies additional live-only controls:
   - execution mode and arming checks
   - payload freshness checks
   - preflight
   - position reconciliation
   - execution risk governor
   - model and competition gating
   - broker connection watchdog
   - execution quality supervisor
5. Real orders route through `engine.execution.broker_router.apply_new_portfolio_orders_router`.
6. Shadow orders route through `engine.execution.broker_sim.apply_new_portfolio_orders`.

Two execution gates coexist by design:

- `execution_gate_snapshot()` is the broad runtime barrier. It blocks execution when lifecycle, execution mode, portfolio risk, or kill-switch state make the runtime unsafe.
- `engine.execution.kill_switch.execution_allowed()` is the more detailed kill-switch cascade used inside broker paths and other jobs.

### Post-submit path

The post-trade path is owned by `engine/execution/execution_poll_and_attrib.py`.

It:

1. polls broker fills from broker adapters
2. manages open orders best-effort
3. computes execution metrics
4. computes PnL attribution
5. computes capital-efficiency snapshots
6. builds execution analytics
7. refreshes the execution quality supervisor
8. repairs missing model identity on execution orders
9. upserts trade-attribution ledger rows
10. recomputes marketplace scores
11. recomputes shadow-capital scores
12. recomputes model rankings
13. computes PnL decomposition
14. enforces residual and orphan attribution invariants

`engine/runtime/trade_lifecycle.py` is the trace utility that stitches the path back together across:

- `alerts`
- `predictions`
- `prediction_history`
- `decision_log`
- `portfolio_orders`
- `execution_orders`
- `execution_fills`
- `model_position_state`
- `pnl_attribution`

## Operator And AI Sidecar Flow

### Node operator server

`boot/operator_server.js` is a separate Node.js control plane. It does not replace `dashboard_server.py`; it sits beside it.

Its operational responsibilities are:

- runtime start and stop
- emergency stop
- operator snapshots
- operator proxy routes
- runtime and stderr log access
- AI patch preview/apply/rollback helpers

Its persistent state lives under `data/operator/`:

- `data/operator/operator.secrets.json`
- `data/operator/operator.state.json`
- `data/operator/patches/`

It also uses repo-level log files:

- `logs/runtime.log`
- `boot/engine_stderr.log`

### Diagnostics-only operator AI module

`services/operator_ai/agent.js` is currently diagnostics-only. It fetches:

- `/api/operator/service_status`
- `/api/operator/health`
- `/api/operator/runtime_logs?lines=80`
- `/api/operator/support_snapshot?mode=quick`
- `/api/operator/snapshot?mode=quick`
- `/api/operator/provider_telemetry`
- `/api/operator/runtime_watchdogs`
- `/api/execution/barrier`

It calls those endpoints against `http://127.0.0.1:4001`, which is the Node operator server.

It builds a context object with:

- `status`
- `health`
- `logs`
- `snapshot`
- `telemetry`
- `watchdogs`
- `barrier`
- `allowed_actions`

In the inspected code, `ALLOWED_ACTIONS = []`, so this module returns diagnostics but does not execute actions. It writes JSONL records to `data/ai_operator_log.jsonl`.

One implementation detail matters for support work: `buildContext()` currently issues eight HTTP requests but destructures them into seven variables. As implemented:

- `snapshot` receives `/api/operator/support_snapshot?mode=quick`
- `telemetry` receives `/api/operator/snapshot?mode=quick`
- `watchdogs` receives `/api/operator/provider_telemetry`
- `barrier` receives `/api/operator/runtime_watchdogs`
- the final `/api/execution/barrier` response is fetched but not assigned into the returned context object

### Patch-preview and rollback path

`boot/operator_server.js` also contains a separate AI patch path. That path is not the same module as `services/operator_ai/agent.js`.

Important gates in the Node operator server:

- `POST /api/operator/ai/patch_preview`
- `POST /api/operator/ai/apply_patch`
- `POST /api/operator/ai/rollback_patch`

`apply_patch` is blocked unless all of the following are true:

- `confirm === "APPLY_PATCH"`
- `state.lastMode !== "live"`
- a file path is present
- a patch object is present
- `confidence >= 0.85`
- the `find` text matches exactly once in the target file

Applied patches are backed up to `data/operator/patches/*.bak` with adjacent JSON metadata. `rollback_patch` requires `confirm === "ROLLBACK_PATCH"`.

## UI And Control-Plane Flow

`dashboard_server.py` is the Python API aggregator and static UI host. It mounts:

- route specs from `engine/api/*`
- the data-source routes from `routes/data_sources_routes.py`
- the terminal routes from `engine.terminal.api.ROUTE_SPECS_TERMINAL_ALL`
- static files from `ui/`

### Dashboard surface

The main dashboard files are:

- `ui/dashboard.html`
- `ui/dashboard.js`

`ui/dashboard.js` actively polls and mutates concrete endpoints including:

- `/api/health`
- `/api/system/state`
- `/api/ingestion/status`
- `/api/supervisor/status`
- `/api/execution/barrier`
- `/api/broker`
- `/api/jobs`
- `/api/jobs/history`
- `/api/jobs/log`
- `/api/pnl`
- `/api/governance/summary`
- `/api/promotion/status`
- `/api/promotion/explain`
- `/api/execution/overlays`

### Data-source control plane

The data-source UI files are:

- `ui/data_sources.html`
- `ui/data_sources.js`

They drive:

- `GET /api/data_sources`
- `GET /api/data_sources/logs`
- `POST /api/data_sources/create`
- `POST /api/data_sources/update`
- `POST /api/data_sources/delete`
- `POST /api/data_sources/enable`
- `POST /api/data_sources/disable`
- `POST /api/data_sources/test`

### Browser terminal

The dedicated terminal files are:

- `ui/terminal/terminal.html`
- `ui/terminal/terminal.js`
- `engine/terminal/api/api_terminal.py`
- `engine/terminal/api/api_terminal_orders.py`

The terminal is intentionally risk-gated. It does not place broker orders directly. `POST /api/terminal/order` and `POST /api/terminal/flatten` only insert rows into `portfolio_orders` after `execution_gate_snapshot()` reports that real trading is currently allowed.

## Where Persistence Lives

| Location | Owner | What is stored there |
| --- | --- | --- |
| Postgres database selected by `TS_PG_DSN` or `engine.runtime.platform.default_pg_dsn()` | `engine/runtime/storage.py` -> `engine/runtime/storage_pg.py` | Primary runtime store. This is where `runtime_meta`, `predictions`, `prediction_history`, `decision_log`, `portfolio_orders`, `execution_orders`, `execution_fills`, `pnl_attribution`, `kill_switch_state`, `champion_assignments`, `data_sources`, schema migration state, and related tables live. |
| `DB_PATH` / `TRADING_DATA` local data root | `start_system.py`, `start_ingestion.py`, `engine/runtime/db_guard.py` | Local data directory and legacy identity path for older callers, diagnostics, artifacts, and test backends. It is not the production database location. File-shaped legacy defaults such as `${TRADING_DATA}/trading.db` are tolerated and normalized to their parent directory by `db_guard`. |
| temporary SQLite files under pytest temp roots | `engine/runtime/storage_sqlite.py`, `engine/runtime/test_isolation.py`, `tests/conftest.py` | Isolated unit-test storage when `TS_STORAGE_BACKEND=sqlite` or `TS_TESTING=1`. This path prevents tests from probing ambient Postgres/PgBouncer. |
| `logs/` | `start_system.py` and `boot/operator_server.js` | `runtime.pid`, `ingestion.pid`, `runtime.log`, and ingestion stdout/stderr log files. |
| `data/operator/` | `boot/operator_server.js` | Operator state, operator secrets, and AI patch backup metadata. |
| `data/ai_operator_log.jsonl` | `services/operator_ai/agent.js` | Diagnostics-only operator AI analysis log. |
| optional Timescale/price sidecars | `engine/runtime/storage_pg_prices.py`, `engine/runtime/timescale_client.py`, read routers | Append-heavy market-data and telemetry paths when enabled and validated. These sidecars complement the primary Postgres runtime storage and may be routed with fallback during migration windows. |

Storage boot is fail closed in production-like modes. `db_guard.ensure_db_ok()` must acquire Postgres, `bootstrap_first_run()` and `storage.init_db()` apply migrations and repairs, `get_db_validation_snapshot(strict=True)` validates the schema, and startup gates expose blocking `database_reachable` and `schema_valid` checks. If Postgres is unavailable, the runtime reports degraded/unavailable storage and should not be considered ready for trading.

## Repo Map By Responsibility

| Path | Responsibility |
| --- | --- |
| `start_system.py` | Main supervised runtime entrypoint. |
| `start_ingestion.py` | Isolated ingestion entrypoint and initial ingestion-state publisher. |
| `dashboard_server.py` | Main Python HTTP/UI server and post-bind runtime bootstrapper. |
| `engine/runtime/` | Lifecycle state, health, storage, supervision, orchestration, startup validation, job registry, and shutdown logic. |
| `engine/strategy/` | Prediction validation, decision logging, portfolio construction, capital guard, competition, promotion, and model-governance logic. |
| `engine/execution/` | Execution gates, broker routing, fill polling, ledgers, attribution, quality supervision, and kill switches. |
| `engine/api/` | JSON API handlers used by dashboard, operator, and system-control surfaces. |
| `engine/terminal/api/` | Read-mostly browser-terminal API plus terminal order-intent writes. |
| `services/` | Cross-cutting services, including the data-source manager and diagnostics-only operator AI module. |
| `routes/` | Standalone route groups mounted by `dashboard_server.py`; currently includes the data-source control plane. |
| `ui/` | Browser dashboard, browser terminal, data-source control plane, and supporting UI panels. |
| `boot/` | Node.js operator server and deployment/bootstrap helpers. |
| `docs/` | Repository documentation. |

## Practical Debugging Notes

- If the dashboard is up but the system is not trading, inspect lifecycle and barrier state first. The primary blockers are exposed through `engine/runtime/lifecycle_state.py`, `engine/runtime/gates.py`, and `/api/execution/barrier`.
- If the runtime is healthy but there are no fresh prices, inspect `start_ingestion.py`, `engine/runtime/ingestion_runtime.py`, `/api/operator/provider_telemetry`, `/api/operator/runtime_watchdogs`, and the data-source control plane.
- If orders exist in `portfolio_orders` but never become real broker orders, the failure is usually in the execution gate, kill-switch cascade, competition policy, preflight, or broker watchdog inside `engine/execution/broker_apply_orders.py`.
- If fills exist but attribution is missing, inspect `engine/execution/execution_poll_and_attrib.py`, `engine/execution/execution_ledger.py`, and `engine/runtime/trade_lifecycle.py`.
