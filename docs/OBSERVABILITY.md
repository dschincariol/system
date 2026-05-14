# Observability

This document records the observability surfaces that are present in the repository today. It is grounded in `engine/runtime/observability.py`, `engine/runtime/failure_diagnostics.py`, `engine/runtime/health.py`, `engine/api/api_system.py`, `boot/operator_server.js`, and the current validation and probe tools under `tools/`.

## Primary Signals

| Surface | Owner | What it exposes |
| --- | --- | --- |
| Component health snapshots | `engine/runtime/observability.py` | In-memory `record_component_health(...)` status keyed by component name, with TTL-based stale detection from `get_component_health_snapshot(...)`. |
| Rolling rates | `engine/runtime/observability.py` | Rolling success-rate gauges and observation counters emitted through `record_rolling_rate(...)`. |
| Failure payloads | `engine/runtime/failure_diagnostics.py` | Structured failure logs plus `runtime_failure` event-log rows built by `log_failure(...)` and `failure_response(...)`. |
| Health snapshots | `engine/runtime/health.py` and `engine/api/api_system.py` | Repo-wide health, readiness, database, prices, providers, jobs, execution barrier, and broker-connection state surfaced by `/api/health` and `/api/readiness`. |
| Runtime watchdogs | `engine/api/api_system.py` | Job heartbeat age, restart counters, ingestion freshness, and pipeline watchdog state from `/api/operator/runtime_watchdogs`. |
| Support snapshot | `engine/api/api_system.py` | Repair-oriented evidence bundle from `/api/operator/support_snapshot`. Includes preflight, DB debug, recent errors, watchdogs, job status, and synthesized diagnostics. |
| Provider telemetry | `engine/api/api_system.py` | Feed-level health and freshness from `/api/operator/provider_telemetry`. |
| Execution barrier | `engine/runtime/gates.py` and `engine/api/api_system.py` | Fail-closed execution state from `/api/execution/barrier`. |
| Operator logs | `logs/` and `boot/operator_server.js` | Runtime log tails, stderr tails, and operator-side snapshots exposed through the Node operator server. |

## Persistence And Retention Boundaries

- `engine/runtime/observability.py` keeps component health and rolling-rate windows in process memory.
- `engine/runtime/failure_diagnostics.py` persists structured failure events into the `event_log` table with `event_type='runtime_failure'`.
- `engine/runtime/metrics.py` and `engine/runtime/metrics_store.py` emit best-effort metrics into the repo's runtime metrics pipeline.
- `services/operator_ai/agent.js` writes diagnostics-only analyses to `data/ai_operator_log.jsonl`.
- Runtime and operator log files live under `logs/` and the `boot/` layer's stderr path.

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

## First Inspection Path

Use this order when the system looks unhealthy:

1. `GET /api/execution/barrier`
   Confirms whether the runtime is blocked from execution and surfaces the primary reason.
2. `GET /api/health`
   Shows the broad health snapshot used by the dashboard.
3. `GET /api/operator/runtime_watchdogs`
   Surfaces stale jobs, provider freshness, and watchdog counters.
4. `GET /api/operator/provider_telemetry`
   Confirms whether price and provider pipelines are actually alive.
5. `GET /api/operator/support_snapshot?mode=repair`
   Pulls the full repair bundle with DB debug state, recent failures, and synthesized diagnostics.

## Repo Checks And Probes

The repository already includes lightweight observability-oriented checks:

- `python tools/validate_repo.py`
- `python tools/validate_docs.py`
- `python tools/runtime_graph_check.py`
- `python tools/runtime_stability_probe.py`
- `python tools/noop_guard.py`

Use `python tools/validate_repo.py --live` only against a running stack when a live smoke test is intended.
