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
  Read-only system, health, readiness, telemetry, and runtime diagnostic endpoints.
- [system/](system/)
  Extracted helper package for `api_system.py` route metadata, shared response
  helpers, and readiness contract metadata.
- [api_self_repair.py](api_self_repair.py)
  Mutating self-repair and schema-repair endpoints.
- [api_operator_handlers.py](api_operator_handlers.py)
  Operator-facing status and control handlers.
- [api_jobs.py](api_jobs.py)
  Job catalog, status, history, and job-control endpoints.
- [api_read.py](api_read.py)
  Core read APIs.
- [api_read_advanced.py](api_read_advanced.py)
  Large advanced read-only data surface (model diagnostics, temporal eval, portfolio backtest/snapshot, rolling/by-symbol/by-confidence execution metrics, social features/regimes/blocks, validation rows, shadow-capital scoring, size policy, recent decisions, and decision-detail drilldowns). It is not a route module: it has no `ROUTE_SPECS` and no `api_*` handlers, instead exporting 18 public `get_*`/`run_*` accessors (plus internal `_` helpers) that are lazily imported by the thin route handlers in [api_ops_handlers.py](api_ops_handlers.py) and the dashboard aggregator [api_dashboard_reads.py](api_dashboard_reads.py).
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
- [api_handlers.py](api_handlers.py)
  Legacy compatibility bridge exporting the `api_get_kill_switches`, `api_get_job_log`, and `api_get_job_history` handlers that `dashboard_server.py` imports best-effort; it normalizes kill-switch snapshots and applies log level/query/limit filters without routing back through the status handlers.
- [api_ui_metrics.py](api_ui_metrics.py)
  Canonical UI-metrics adapter that registers `/api/ui/metrics` (`ROUTE_SPECS_UI_METRICS`) and provides `build_ui_metrics_snapshot`, which normalizes existing read-only PnL, PnL-summary, broker/account, positions, risk-summary, portfolio-risk, and terminal-positions payloads into one stable shape for top-level UI cards. It adds no new broker/account semantics; the `api_get_ui_metrics` route handler lives in `dashboard_server.py` and calls this normalizer.
- [api_relevance.py](api_relevance.py)
  Thin pass-through endpoint (`api_get_relevance_stats`) that returns input-relevance statistics from `engine.strategy.relevance`; business logic stays in the strategy layer.
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
  System/operator diagnostics, support snapshots, service status, provider telemetry, watchdogs, and read-only aggregation surfaces. `api_system.py` remains the compatibility facade; route specs and shared response helpers are delegated to [system/](system/).

## Terminal Order Contract

`POST /api/terminal/order` and `POST /api/terminal/flatten` do not submit directly to a broker. After the real-trading execution barrier allows the request, they persist a `portfolio_orders` intent for the normal execution pipeline.

Manual quantity orders keep portfolio-weight fields neutral: `from_weight = 0.0`, `to_weight = 0.0`, and `delta_weight = 0.0`. The requested quantity is stored in `explain_json.terminal_order` with `sizing = "quantity"`, positive `qty`, and signed `signed_qty`; `BUY` derives a positive signed quantity and `SELL` derives a negative signed quantity. The execution-intent loader turns that payload into `qty`, `order_sizing = "quantity"`, and `terminal_order = true`, while preserving neutral weights so weight-based consumers do not mistake share quantity for allocation.

Before writing an intent, terminal mutations also enforce fresh price, max quantity, max notional, optional per-symbol caps, and duplicate-recent-order controls. Rejections are persisted to `terminal_intent_rejections` with stable reason codes.

Expected terminal refusals are not server crashes. Safety-gate blocks return structured 403 payloads, and pre-trade business refusals such as stale price, duplicate recent intent, max quantity, or max notional return structured 409 payloads. All refusal payloads include `ok=false`, `error`, `reason_code`, a safe human `message`, and `meta.status`. The handlers still do not submit, cancel, replace, or flatten broker orders directly.

## Business Refusal Status Contract

The shared HTTP transport maps expected business refusals to 4xx responses instead of allowing them to fall through to 500:

- safety or execution blocks: 403
- pre-trade/order conflicts: 409
- missing data-source credentials or incomplete provider configuration: 422
- rejected provider credentials: 401
- missing provider entitlements: 403

Unexpected handler exceptions still return 500 with `error=internal_server_error`, `reason_code=handler_exception`, and a safe message that does not echo exception text to the client.

## Market Candle Contract

`GET /api/market/candles` is the canonical live chart history endpoint for dashboard and terminal pro charts. Query parameters are `symbol`, `tf`, `limit`, and `max_points`.

`limit` is normalized to `10..5000` and caps the number of newest candles kept after tick aggregation. The storage read may fetch more raw rows than the final candle count, but bounded SQLite and Timescale quote queries apply their row limit to the newest eligible rows first and then return rows in ascending timestamp order before candle building. This preserves dense-symbol recency without changing the frontend contract.

SQLite quote snapshots use `price_quotes.ts_ms`. Timescale price sidecars use canonical `price_quotes."time"` / `price_quotes_raw."time"` columns; the read router projects those values to API `ts_ms` with the shared helpers in `engine.runtime.price_timescale_schema` and keeps Timescale filters/order clauses on `"time"` for index use. Readers should not require a physical `ts_ms` column on Timescale `price_quotes`.

`max_points` is an optional presentation cap. When supplied, it is normalized to `50..20000`; when omitted, it defaults to the normalized `limit`. If the post-`limit` candle array still exceeds `max_points`, the API downsamples the ascending array and always includes the newest candle. Responses keep `candles` ascending by `ts_ms`; `meta.limit`, `meta.max_points`, `meta.fetch_limit`, and `meta.order` expose the effective bounds and ordering.

The dashboard and terminal pro-chart VWAP overlay is a client-side loaded-window VWAP, not a session VWAP. It accumulates `close * volume` and volume over only the candles currently loaded from this endpoint and subsequent stream updates; the accumulator does not reset at trading-session boundaries. Do not relabel it as session VWAP unless the API contract first carries reliable symbol asset-class, exchange timezone, and session-boundary metadata and the production indicator accumulator resets on those boundaries.

## Replay Day Contract

`GET /api/replay/day` returns a read-only historical day payload for the dashboard Historical Replay panel. Query parameters include `date`, `symbol`, `model_id`/`model`, `tf`, `max_points`, and `event_limit`.

The endpoint prefers persisted `price_bars` rows when that table has `ts_ms`, `symbol`, `o`, `h`, `l`, and `c`, preserving those values as `open`, `high`, `low`, and `close` in the response. If bars are unavailable, it aggregates supported snapshot tables into synthetic OHLCV candles with `_build_candles_from_rows`. Responses keep `candles` ascending by `ts_ms`, mirror them under `streams.candles`, and report the selected source under `meta.sources.price`.

Replay UI consumers must treat candles as OHLC, not close-only line points. Event streams use millisecond timestamps; fills carry their own execution price, while decisions and orders may not, so the browser renderer anchors price-less markers to the nearest replay candle close without mutating the API payload.

Browser consumers also enforce candle geometry defensively: malformed high/low
values are normalized so the displayed wick always contains open and close.
Pro-chart candles require an explicit finite positive `t`/`time`; missing,
zero, negative, or malformed timestamps are dropped instead of being rendered at
epoch zero.

## News Sentiment Contract

`GET /api/news/sentiment` preserves missing source sentiment as JSON `null`.
True numeric `0.0` remains a valid neutral sentiment value. The response `meta`
includes `count`, `valid_sentiment`, `missing_sentiment`, and `ready`; `ready`
means at least one numeric sentiment point is available for charting.

Dashboard consumers must treat `null` or malformed sentiment values as skipped
unavailable points, not neutral values. The browser chart is responsible for
display clamping to the expected `[-1, 1]` sentiment range and for exposing
clipped/skipped counts in its accessibility summary.

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

## Self-Repair Contract

Self-repair is mounted from `engine/api/api_self_repair.py`, not from
`api_system.py`. The canonical route specs are:

- `POST /api/system/self_repair`
- `POST /api/operator/self_repair`
- `POST /api/system/repair_schema`
- `POST /api/repair_schema`

`dashboard_server.py` imports `ROUTE_SPECS_SELF_REPAIR` and validates at import
time that these routes resolve to `engine.api.api_self_repair`. `api_system.py`
keeps compatibility exports for direct callers, but its `ROUTE_SPECS_SYSTEM`
remains read-only for health, state, readiness, telemetry, and diagnostics.

## Alpha Decay Chart Contract

`GET /api/alpha_decay` is owned by `engine/api/api_system.py::api_get_alpha_decay`.
`dashboard_server.py` only mounts the route from `ROUTE_SPECS_SYSTEM` and validates
at import time that the registered handler still resolves to the `api_system`
function. Do not add a dashboard-local handler or fallback route for this path.

The response is consumed by `ui/risk_charts.js` and includes `runtime`,
`runtime_history`, `strategies`, `strategy_history`, `ready`, and `unavailable`.
Keep those fields backward-compatible when changing the handler.

The `limit` query parameter caps `runtime_history` globally, but caps
`strategy_history` per strategy. The strategy history query must use a
per-strategy window so one strategy with many recent rows cannot starve other
strategies out of the chart selector. The response also reports
`strategy_history_limit_per_strategy` for operator/debug visibility.

Chart numeric fields preserve the difference between unavailable values and real
zeros. `runtime.min_throttle_mult`, `runtime_history[].min_throttle_mult`,
`strategies[].throttle_mult`, `strategy_history[].throttle_mult`, and chart
metrics such as `rolling_sharpe` are `null` when the source value is missing and
`0.0` when the runtime intentionally reported a zero multiplier or zero metric.

## Portfolio Risk UI Contract

`GET /api/risk/portfolio` is owned by
`engine/api/api_system.py::api_get_portfolio_risk`. The payload includes
`caps`, `summary`, `info`, and up to 200 `history` rows from
`portfolio_risk_snapshots`; the API returns those rows newest-first by `ts_ms`.

UI consumers that need the latest snapshot must select the row with the maximum
numeric `ts_ms` rather than assuming a positional row. This keeps risk headroom
stable if tests, replay tooling, or compatibility callers pass ascending
history. The browser bullet-bar thresholds live in
`ui/bullet_bars.js`: OK is `<0.85` of cap, Watch is `>=0.85` through exactly
`1.00`, and Over is strictly `>1.00`.

The risk-history chart draws a zero baseline with `yFor(0)` only when zero is
inside the visible y-domain. A plot midpoint is not a semantic zero reference
and must not be used for net-exposure crossing charts.

## Portfolio Backtest Chart Contract

`GET /api/portfolio/backtest/latest` and its alias
`GET /api/backtest/portfolio/latest` return the latest run and ordered
`run.points`. Point fields `ret`, `equity`, and `drawdown` are nullable chart
values: missing database values are emitted as JSON `null`, while true `0.0`
values remain numeric zeros. UI renderers must treat nulls as gaps or unavailable
states, not as zero-valued observations.

The same payload includes `run.benchmark` for the optional equity overlay. The
canonical benchmark is `SPY` from the production `prices` table, using
`COALESCE(price, px)` as the raw price. The API queries benchmark rows between
the first and last finite portfolio-equity points in the run and emits
`benchmark.points[].value` normalized so the first valid SPY price equals the
first finite portfolio equity value. `benchmark.available=false` is an honest
non-broken state; `unavailable_reason` identifies cases such as a missing
`prices` table, no SPY rows in the run window, or fewer than two usable prices.

## Model Performance Divergence Contract

`GET /api/model/performance_divergence` is assembled by
`model_performance_divergence.py` from existing backtest, shadow, live PnL,
execution, registry, and production-monitoring reads. Each `comparisons[]` row
must keep stable `key`, `label`, `unit`, `expected`, `shadow`, `realized`,
`delta`, `status`, and `explanation` fields so the dashboard can rank chartable
metrics without depending on backend row order.

The browser default chart ranks rows by status severity, displayed absolute
delta, source freshness, then product importance. Backend changes may add rows
or reorder `comparisons[]`, but they must not rely on row order to choose the
operator-facing chart metric.

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

The dashboard-owned `GET /api/operator/ping` bridge is intentionally public like health/liveness and proxies the operator sidecar ping with a bounded timeout. If the sidecar is unavailable, it returns a structured 503 with `reason_code=operator_sidecar_unreachable`.

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
