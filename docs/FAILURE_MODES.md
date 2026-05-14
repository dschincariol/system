# Failure Modes

This document captures the recurrent failure classes that are explicit in the current code paths. It is grounded in `start_system.py`, `dashboard_server.py`, `engine/runtime/gates.py`, `engine/runtime/health.py`, `engine/runtime/ingestion_runtime.py`, `engine/runtime/failure_diagnostics.py`, `engine/execution/kill_switch.py`, `engine/execution/broker_apply_orders.py`, and the operator-facing APIs under `engine/api/api_system.py`.

## Fail-Closed Principle

The runtime is intentionally conservative in safety-critical paths:

- `engine/runtime/gates.py` blocks execution on unknown or critical runtime state.
- `engine/execution/kill_switch.py` adds an execution-specific kill-switch cascade.
- `engine/api/api_system.py` mirrors those states through `/api/execution/barrier`, `/api/readiness`, and `/api/operator/support_snapshot`.

## Common Failure Classes

| Failure class | Primary surfaces | First files to inspect |
| --- | --- | --- |
| Startup or preflight failure | `start_system.py`, `dashboard_server.py`, `/api/readiness`, `/api/operator/preflight_report` | `start_system.py`, `dashboard_server.py`, `engine/runtime/health.py`, `engine/runtime/job_registry.py` |
| Schema or storage failure | `runtime_failure` event-log rows, `/api/operator/support_snapshot`, DB validation output | `engine/runtime/storage.py`, `engine/runtime/db_repair.py`, `engine/runtime/jobs/repair_schema.py` |
| Ingestion runtime not running or stale | `/api/ingestion/status`, `/api/operator/runtime_watchdogs`, `/api/operator/provider_telemetry` | `start_ingestion.py`, `engine/runtime/ingestion_runtime.py`, `engine/runtime/ingestion_status.py` |
| Provider auth or source configuration failure | Data Sources Control Center, `data_source_logs`, `/api/data_sources/logs` | `services/data_source_manager.py`, `routes/data_sources_routes.py`, `services/credential_encryption.py` |
| Execution barrier block | `/api/execution/barrier`, `/api/readiness`, dashboard safety panels | `engine/runtime/gates.py`, `engine/execution/kill_switch.py`, `engine/execution/broker_apply_orders.py` |
| Portfolio risk block | `/api/risk/portfolio`, `/api/risk/monte_carlo`, `/api/execution/barrier` | `engine/risk/portfolio_risk_engine.py`, `engine/risk/monte_carlo_risk_engine.py`, `engine/runtime/risk_state.py` |
| Broker connection or execution-quality degradation | `/api/operator/runtime_watchdogs`, execution metrics APIs, execution barrier reasons | `engine/execution/execution_broker_watchdog.py`, `engine/execution/execution_quality_supervisor.py`, `engine/execution/broker_router.py` |
| Post-trade attribution or reconciliation failure | Attribution quality APIs, `pnl_attribution`, `trade_attribution_ledger`, recent errors | `engine/execution/execution_poll_and_attrib.py`, `engine/execution/execution_ledger.py`, `engine/runtime/trade_lifecycle.py` |

## What The Operator APIs Already Provide

The current operator-facing APIs give a bounded first-pass diagnosis without log scraping:

- `/api/execution/barrier`
  The fastest answer to "why is trading blocked?"
- `/api/operator/runtime_watchdogs`
  Job freshness, restart counters, and ingestion watchdog state.
- `/api/operator/provider_telemetry`
  Feed and provider freshness plus active child ownership.
- `/api/operator/support_snapshot`
  Preflight, DB debug state, recent failures, watchdogs, and synthesized diagnostics.

## Event And Failure Logging

`engine/runtime/failure_diagnostics.py` standardizes failure capture:

- `log_failure(...)` records structured failure payloads and can persist them.
- `failure_response(...)` returns API-safe envelopes that include `root_cause_code`, `failure_scope`, and a system-state snapshot.
- persisted failures land in `event_log` as `runtime_failure` events.

When a failure is hard to localize, start with the latest `runtime_failure` event or the latest support snapshot instead of isolated stderr text.
