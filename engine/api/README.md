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
  Job catalog, status, history, and job-control endpoints.
- [api_read.py](api_read.py)
  Core read APIs.
- [api_write.py](api_write.py)
  Mutating APIs, including alert acknowledgement, shelving, resolution, job-history writes, and promotion guard toggles.
- [api_dashboard_reads.py](api_dashboard_reads.py)
  Dashboard-specific read aggregation.
- [feature_visibility.py](feature_visibility.py)
  Read-only serializers for structured-document extraction and graph-relational feature visibility. The serializer backs `/api/data/feature_visibility` and decision-drilldown feature-lineage enrichment without changing model-serving or execution authority.
- [api_market.py](api_market.py)
  OHLCV candle aggregation and server-sent market stream endpoints.
- [api_replay.py](api_replay.py)
  Historical day replay aggregation endpoint.
- [api_broker_config.py](api_broker_config.py)
  Broker configuration read/write/test/audit control-plane endpoints with encrypted credential storage and masked reads.
- [api_ops.py](api_ops.py)
  Route metadata for ops, diagnostics, execution analytics, and governance read surfaces.
- [api_ops_handlers.py](api_ops_handlers.py)
  Thin handler implementations for the ops route set.
- [api_governance.py](api_governance.py)
  Governance-specific handlers for promotion status, rollback, calibration, governance summaries, and governance-evidence drilldowns.
- [governance_evidence.py](governance_evidence.py)
  Read-only serializer that aggregates persisted promotion, generated-candidate, model-risk, monitoring, and shadow-capital evidence into the `/api/governance/evidence*` contract. It does not mutate promotion state or allocation state.
- [readiness_evidence.py](readiness_evidence.py)
  Read-only serializer that normalizes live/paper readiness evidence from runtime health, execution barrier, kill switches, broker config/test state, live trading preflight, provider telemetry, data-source health, governance OPE/experiment-ledger evidence, production monitoring, and liveness/readiness probes.
- [../terminal/api/api_terminal.py](../terminal/api/api_terminal.py)
  Read-mostly API surface used by the standalone browser terminal.
- [../terminal/api/api_terminal_orders.py](../terminal/api/api_terminal_orders.py)
  Risk-gated terminal order-entry handlers that emit normal portfolio-order intents.
- [api_system.py](api_system.py)
  System/operator diagnostics, support snapshots, service status, provider telemetry, watchdogs, and self-repair entrypoints.

## Terminal Order Contract

`POST /api/terminal/order` and `POST /api/terminal/flatten` do not submit directly to a broker. After the real-trading execution barrier allows the request, they persist a `portfolio_orders` intent for the normal execution pipeline.

Manual quantity orders keep portfolio-weight fields neutral: `from_weight = 0.0`, `to_weight = 0.0`, and `delta_weight = 0.0`. The requested quantity is stored in `explain_json.terminal_order` with `sizing = "quantity"`, positive `qty`, and signed `signed_qty`; `BUY` derives a positive signed quantity and `SELL` derives a negative signed quantity. The execution-intent loader turns that payload into `qty`, `order_sizing = "quantity"`, and `terminal_order = true`, while preserving neutral weights so weight-based consumers do not mistake share quantity for allocation.

Before writing an intent, terminal mutations also enforce fresh price, max quantity, max notional, optional per-symbol caps, and duplicate-recent-order controls. Rejections are persisted to `terminal_intent_rejections` with stable reason codes.

## Broker Config Contract

`GET /api/broker/config`, `POST /api/broker/config`, `POST /api/broker/test_connection`, and `GET /api/broker/audit` are mounted from `api_broker_config.py`.

This surface stores broker config in `broker_meta`, encrypts supplied credentials with `services.credential_encryption`, masks credentials on reads, requires a fresh passing connection test before activating a non-`sim` broker, and records `broker_config_audit` rows for updates, tests, stale-test blocks, and blocked activations. Freshness is controlled by `BROKER_CONNECTION_TEST_MAX_AGE_S` and defaults to 24 hours.

## Operator Diagnostic Contract

The API layer now exposes a richer operator support package than basic health reads.

The main surfaces are:

- support snapshot payloads for guided repair and AI diagnosis
- runtime watchdog summaries for stale jobs and lifecycle failures
- provider telemetry for feed-level debugging
- service-status and trading-readiness summaries

These payloads are consumed by the dashboard, the local operator server, and the bounded operator AI sidecar.

## Execution Diagnostics Contract

`GET /api/execution/diagnostics` is a sensitive read-only execution diagnostics
route. It delegates aggregation to
`engine.execution.execution_diagnostics.build_execution_diagnostics(...)` and
returns:

- `inventory.routes` availability for existing execution analytics, terminal
  order/fill, advisory, suppression, LOB, DeepLOB, and learned-slicing sources
- `tca.by_symbol`, `tca.rolling`, and `tca.partial_fills`
- `order_flow.rejected_intents` and `order_flow.suppressed_intents` with stable
  reason codes and human-readable reasons
- `lob` freshness/readiness/calibration/shadow-only state
- `learned_slicing` policy/action/baseline/authority diagnostics
- `drilldowns` rows that trace intents to route, fill, rejection, or
  suppression outcomes

The route is explanatory only. It does not grant execution authority; live
execution remains governed by the broker config, execution barrier, risk gates,
execution policy engine, kill switches, and broker adapters.

## Readiness Evidence Contract

`GET /api/operator/readiness_evidence` is the consolidated operator view for live/paper readiness blockers. Aliases are `GET /api/readiness/evidence` and `GET /api/system/readiness_evidence`.

Each evidence item includes `id`, `title`, `status`, `severity`, `blocking`, `source_subsystem`, `source_route`, `source_config_key`, `freshness`, `detail`, and `remediation`. Status values are `passing`, `warning`, `blocked`, and `unavailable`; missing or stale critical evidence in live/paper context is never serialized as passing.

The route aggregates the existing authoritative producers instead of replacing them: `/api/readiness`, `/api/health`, `/api/liveness`, `/api/execution/barrier`, `/api/system/kill_switches`, `/api/broker/config`, `/api/operator/provider_telemetry`, `live_trading_preflight()`, `backup_restore_evidence_snapshot()`, `live_ai_safety_snapshot()`, `live_options_readiness_snapshot()`, and `/api/governance/evidence`.

The payload also includes `action_guards.broker_activation`, which the dashboard uses before posting broker activation. This is an advisory browser guard; backend broker activation still enforces the fresh connection-test requirement, and live/paper execution remains gated by the runtime/execution barriers.

## Governance Evidence Contract

`GET /api/governance/evidence` returns the dashboard Governance Evidence Center summary. It aggregates:

- promotion guard state
- latest OPE gate evidence from `policy_ope_evidence`
- generated-candidate provenance from `experiment_ledger`
- net-after-cost label coverage from `net_after_cost_labels`
- learned-alpha decay freshness from `learned_alpha_decay_*`
- alpha shrinkage metadata from `runtime_meta.last_alpha_shrinkage`
- production monitoring drift/calibration/shadow-live metrics from `production_monitoring_metrics`
- shadow-capital scores from `shadow_capital_scores`

Drilldowns are registered as `GET /api/governance/evidence/promotion_blockers`, `GET /api/governance/evidence/generated_candidates`, and `GET /api/governance/evidence/shadow_capital`. The first-class shadow-capital score route is `GET /api/governance/shadow_capital/scores`.

These routes are sensitive operational GET routes. They are explanatory only: promotion remains gated by the existing strategy/model governance code, and capital allocation remains gated by the existing runtime/execution controls.

## Feature Visibility Contract

`GET /api/data/feature_visibility` returns operator-facing visibility for optional structured-document and graph-relational feature groups. Query parameters are `symbol`, `limit`, and `low_confidence_threshold`.

The payload includes:

- `structured_documents`: extraction counts, latest extraction and availability timestamps, low-confidence counts, confidence buckets, source-document lineage, symbol coverage, event-type coverage, extraction-failure telemetry when persisted in `event_log`, shadow-only labels, and `structured_doc_events` PIT status.
- `graph_features`: graph snapshot counts, latest snapshot freshness, observed graph feature ids, relationship-type coverage, sampled snapshot lineage, `graph_relational_v1` PIT status, environment availability, and explicit shadow-only/direct-authority flags.
- `explanation_paths`: the decision/explanation sources that may carry structured-document or graph feature contributions.

The route is explanatory only. Live model serving still rejects shadow feature contracts through the feature registry, graph promotion remains blocked by existing governance gates, and execution authority remains in runtime, risk, broker, and execution-policy controls.

## Transport Security Contract

`http_transport.py` classifies registered routes before dispatch. Explicit health, liveness, and readiness GET routes remain public; other registered `/api/*` GET routes are sensitive by default, including `/api/system/config`, `/api/operator/logs`, `/api/operator/support_snapshot`, and `/api/terminal/positions`.

Sensitive GET routes require `X-API-Token` in production/live or remote-bind deployments, use the same rate limiter and append-only audit event path as protected mutations, and reject query-string `token` authentication in production/live. Responses from sensitive GET routes pass through the shared API redactor, which masks DSNs, tokens, passwords, API keys, Authorization header values, and broker/account identifiers.

Confirmed high-impact mutations are listed in the transport confirmation
registry. Emergency stop, runtime stop/restart, guarded job starts/stops,
pipeline runs, terminal orders, data-source destructive changes, broker
activation, repair/schema actions, guided bootstrap, and feed restarts require
server-side confirmation in production transport code before handlers run. The
confirmation payload includes the typed token, `action_id`, actor,
`source_surface`, `request_id`, `target`, `reason`, acknowledgement, and
confirmation method/hold metadata where supplied. `api_mutation` audit records
carry the same confirmation context, with consequence text hashed rather than
logged verbatim.

## Job Catalog Contract

`GET /api/jobs` remains backward-compatible for current dashboard callers, but each job row now includes the runtime catalog fields generated by `engine.runtime.job_catalog`: `id`, `group`, `script`, `module`, `mode`, `schedule`, `cadence_seconds`, `stage`, `owner_subsystem`, `dependencies`, `required_secrets`, `required_secret_any`, `required_providers`, `safety`, `execution_sensitivity`, `resource_class`, `purpose`, `action_policy`, `log_url`, `history_url`, and `last_output_url`.

`GET /api/jobs/catalog` returns the same first-class catalog and is available as a static registry read even when the jobs manager cannot provide live state. When live state is available, the catalog rows also include `latest_run` details from locks/history.

Job starts for execution-sensitive or destructive/admin jobs are guarded in `api_jobs.py` using the backend catalog policy. Browser checks are advisory only; direct handler calls must still provide the required confirmation payload for guarded starts, and unavailable jobs are rejected when required secrets are missing.

## Companion Route Modules

Some HTTP surfaces are mounted by `dashboard_server.py` from outside `engine/api/` when they represent focused control planes rather than general handler domains.

- [routes/data_sources_routes.py](../../routes/data_sources_routes.py)
  Data-source inventory, CRUD, enable/disable, connection-test, and source-log endpoints backed by the data source manager.

## Maintenance Guidance

- Keep handler modules narrowly scoped by domain.
- Avoid embedding business logic directly in handlers when that logic belongs in runtime, strategy, data, or execution modules.
- When adding a new endpoint, update the relevant UI docs and subsystem README.
- Register mutating endpoints with non-GET methods, normally POST, so `http_transport.py` applies dashboard mutation auth, rate limiting, confirmation checks for confirmed control routes, and `api_mutation` audit events.
- Register public GET routes only when they are intentionally safe health, liveness, or readiness probes. Operational GETs should remain sensitive so the transport applies token auth, rate limiting, audit logging, and response redaction in production/live or remote-bind deployments.
- Production/live mode must never depend on localhost-only fallback or placeholder dashboard tokens. Local no-token mutation fallback requires explicit safe dev/test mode plus `TS_API_ALLOW_LOCALHOST_MUTATIONS_WITHOUT_TOKEN=1`.
- Treat support-snapshot payload shape as a compatibility contract for the operator layer and `services/operator_ai/agent.js`.
