# API Subsystem

The `engine/api/` package exposes the engine to the dashboard UI and operator tooling.

## File Roles

- [server.py](server.py)
  API server assembly.
- [http_transport.py](http_transport.py)
  Transport-layer request/response plumbing.
- [http_parsing.py](http_parsing.py)
  Parsing and normalization helpers.
- [api_system.py](api_system.py)
  System-level dashboard and runtime endpoints.
- [api_operator_handlers.py](api_operator_handlers.py)
  Operator-facing status and control handlers.
- [api_jobs.py](api_jobs.py)
  Job status, history, and job-control endpoints.
- [api_read.py](api_read.py)
  Core read APIs.
- [api_write.py](api_write.py)
  Mutating APIs.
- [api_dashboard_reads.py](api_dashboard_reads.py)
  Dashboard-specific read aggregation.
- [api_ops.py](api_ops.py)
  Route metadata for ops, diagnostics, execution analytics, and governance read surfaces.
- [api_ops_handlers.py](api_ops_handlers.py)
  Thin handler implementations for the ops route set.
- [api_governance.py](api_governance.py)
  Governance-specific handlers for promotion status, rollback, calibration, and governance summaries.
- [../terminal/api/api_terminal.py](../terminal/api/api_terminal.py)
  Read-mostly API surface used by the standalone browser terminal.
- [../terminal/api/api_terminal_orders.py](../terminal/api/api_terminal_orders.py)
  Risk-gated terminal order-entry handlers that emit normal portfolio-order intents.
- [api_system.py](api_system.py)
  System/operator diagnostics, support snapshots, service status, provider telemetry, watchdogs, and self-repair entrypoints.

## Terminal Order Contract

`POST /api/terminal/order` and `POST /api/terminal/flatten` do not submit directly to a broker. After the real-trading execution barrier allows the request, they persist a `portfolio_orders` intent for the normal execution pipeline.

Manual quantity orders keep portfolio-weight fields neutral: `from_weight = 0.0`, `to_weight = 0.0`, and `delta_weight = 0.0`. The requested quantity is stored in `explain_json.terminal_order` with `sizing = "quantity"`, positive `qty`, and signed `signed_qty`; `BUY` derives a positive signed quantity and `SELL` derives a negative signed quantity. The execution-intent loader turns that payload into `qty`, `order_sizing = "quantity"`, and `terminal_order = true`, while preserving neutral weights so weight-based consumers do not mistake share quantity for allocation.

## Operator Diagnostic Contract

The API layer now exposes a richer operator support package than basic health reads.

The main surfaces are:

- support snapshot payloads for guided repair and AI diagnosis
- runtime watchdog summaries for stale jobs and lifecycle failures
- provider telemetry for feed-level debugging
- service-status and trading-readiness summaries

These payloads are consumed by the dashboard, the local operator server, and the bounded operator AI sidecar.

## Companion Route Modules

Some HTTP surfaces are mounted by `dashboard_server.py` from outside `engine/api/` when they represent focused control planes rather than general handler domains.

- [routes/data_sources_routes.py](../../routes/data_sources_routes.py)
  Data-source inventory, CRUD, enable/disable, connection-test, and source-log endpoints backed by the data source manager.

## Maintenance Guidance

- Keep handler modules narrowly scoped by domain.
- Avoid embedding business logic directly in handlers when that logic belongs in runtime, strategy, data, or execution modules.
- When adding a new endpoint, update the relevant UI docs and subsystem README.
- Register mutating endpoints with non-GET methods, normally POST, so `http_transport.py` applies dashboard mutation auth, rate limiting, confirmation checks for confirmed control routes, and `api_mutation` audit events.
- Production/live mode must never depend on localhost-only fallback or placeholder dashboard tokens. Local no-token mutation fallback requires explicit safe dev/test mode plus `TS_API_ALLOW_LOCALHOST_MUTATIONS_WITHOUT_TOKEN=1`.
- Treat support-snapshot payload shape as a compatibility contract for the operator layer and `services/operator_ai/agent.js`.
