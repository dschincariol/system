# Trading System Function Map

This document maps the main Python files to the most important functions and classes inside them.

Last verified against code: 2026-06-17

It is designed for the moment when someone asks:

- "Which function actually does this?"
- "Where is the real entrypoint?"
- "What method should I read before editing this subsystem?"

This is not an exhaustive list of every helper. It focuses on the functions that matter most for understanding control flow and ownership.

## 1. How To Use This Document

Use it in this order:

1. find the subsystem you care about
2. identify the main file
3. read the listed top-level function or class first
4. only then go deeper into local helpers

That will keep you from getting lost in utility functions before you understand the main flow.

## 2. Top-Level Runtime Entrypoints

### `start_system.py`

This is the main Python runtime bootstrap.

| Function | What it does |
| --- | --- |
| `main()` | main entrypoint for starting the supervised runtime |
| `_bootstrap_start_system_env()` | loads environment, sets paths, and prepares logs/data directories |
| `_run_import_smoke()` | validates that critical modules import cleanly |
| `_run_production_validation_gate()` | runs startup validation before runtime proceeds |
| `_spawn_ingestion_if_enabled()` | starts the ingestion subprocess/runtime when enabled |
| `_terminate_ingestion()` | shuts down managed ingestion processes |
| `_bootstrap_runtime_side_effects()` | applies startup-time runtime initialization side effects |
| `_pick_mode_from_argv_or_env()` | determines runtime mode from CLI or env |
| `_record_phase()` | writes startup phase progress to runtime metadata |
| `_record_first_failure()` | records the first startup failure in structured form |

### `dashboard_server.py`

This is the HTTP/UI boundary and part of runtime orchestration.

| Function | What it does |
| --- | --- |
| `run_server()` | starts the dashboard HTTP server |
| `stop_server()` | stops the dashboard server |
| `_ensure_runtime_orchestration()` | coordinates dashboard-owned runtime bootstrap behavior |
| `_db_health_snapshot()` | builds database health summary |
| `api_get_db_health()` | exposes DB health to the API |
| `_operator_status_payload()` | builds the operator-facing runtime status payload |
| `_operator_preflight_steps()` | returns preflight steps/status for startup |
| `_operator_start_impl()` | runs operator startup action logic |
| `_update_startup_trace()` | publishes startup trace updates |
| `_record_startup_failure()` | publishes startup failure details |

### `boot/operator_server.js`

This is the local operator control plane and repair proxy.

| Function | What it does |
| --- | --- |
| `_llm(prompt)` | submits the bounded operator-AI prompt to the configured LLM backend |
| `operatorProxyGet(...)` | proxies operator reads through to dashboard/runtime endpoints |
| `buildSupportBundle(...)` | assembles operator evidence used by repair and diagnostics flows |
| `applyPatchFromAnalysis(...)` | writes a guarded patch file change from an approved analysis payload |
| `rollbackPatchById(...)` | rolls back a previously applied operator patch by patch id |
| `logAgentAction(...)` | persists operator-AI action and audit entries |

## 3. Runtime And Orchestration

### `engine/runtime/job_registry.py`

This file is the canonical job catalog.

| Function | What it does |
| --- | --- |
| `validate_runtime_architecture()` | validates the registry and architecture assumptions |
| `validate_job_registry_paths()` | checks that registered job paths are valid |
| `get_job_spec(job_name)` | returns the full job specification |
| `get_job_meta(job_name)` | returns the metadata portion of a job definition |
| `get_boot_jobs()` | returns the jobs expected to start at boot |
| `get_price_feed_jobs()` | returns jobs that are price-feed sources |
| `is_execution_job(job_name)` | identifies execution-sensitive jobs |
| `is_price_feed_job(job_name)` | identifies price-feed jobs |
| `is_market_data_job(job_name)` | identifies market-data jobs |

### `engine/runtime/jobs_manager.py`

This file owns job process state and lifecycle actions.

| Class or Function | What it does |
| --- | --- |
| `JobState` | per-job state container |
| `JobManager` | main job lifecycle manager |
| `_GlobalJobManager` | process-wide manager holder/state wrapper |
| `get_all_job_states()` | returns job states for APIs/UI |
| `get_job_log(job_name, tail)` | returns recent job log text |
| `get_job_history(job_name, limit)` | returns persisted job history |
| `_job_launch_trace_append()` | stores job launch trace details |

### `engine/runtime/startup_orchestrator.py`

This file owns startup sequencing.

| Class | What it does |
| --- | --- |
| `StartupOrchestrator` | coordinates startup order, dependencies, and post-bind flow |

## 4. Storage Layer

### `engine/runtime/storage.py`, `engine/runtime/storage_pg.py`, and `engine/runtime/storage_sqlite.py`

`engine/runtime/storage.py` is the public storage facade. In production-like operation it re-exports the Postgres implementation from `engine/runtime/storage_pg.py`; isolated Python tests can opt into `engine/runtime/storage_sqlite.py` through `TS_STORAGE_BACKEND=sqlite` or `TS_TESTING=1`.

| Function or Class | What it does |
| --- | --- |
| `storage.init_db()` | facade entrypoint that initializes the selected backend schema |
| `storage.init_timeseries_storage()` | initializes optional Timescale/feature/telemetry sidecars when enabled |
| `storage.get_timeseries_storage_snapshot()` | returns optional sidecar readiness and degraded-state details |
| `storage_pg.connect()` / `storage.connect()` | returns a routed DB connection through the active backend |
| `storage_pg.connect_ro()` / `storage.connect_ro()` | returns a read-oriented connection through the active backend |
| `storage_pg.run_write_txn(fn, ...)` / `storage.run_write_txn(fn, ...)` | runs a managed write transaction with backend safety behavior |
| `storage_pg.get_db_validation_snapshot()` | returns strict Postgres schema validation used by preflight/startup |
| `storage_pg.get_db_debug_snapshot()` | returns richer DB and connection diagnostics |
| `storage_pg.put_event(...)` | writes an event row |
| `storage_pg.put_price(...)` | writes a price row |
| `storage_pg.acquire_job_lock(...)` | obtains a runtime job lock |
| `storage_pg.release_job_lock(...)` | releases a runtime job lock |
| `storage_pg.touch_job_lock(...)` | heartbeats a runtime job lock |
| `storage_pg.put_job_heartbeat(...)` | writes a job heartbeat |
| `storage_pg.get_job_checkpoint(job_name)` | reads a job checkpoint |
| `storage_pg.put_job_checkpoint(...)` | writes a job checkpoint |

### Newer storage helpers added by the integration work

| Function | What it does |
| --- | --- |
| `storage_pg.log_alert_interaction(...)` | stores passive alert/decision interaction rows |
| `storage_pg.log_decision_view(...)` | stores decision-detail view events |
| `storage_pg.fetch_recent_decisions(limit)` | returns decision cards for the dashboard |
| `storage_pg.fetch_decision_detail(decision_id)` | builds a decision drilldown payload |
| `storage_pg.fetch_human_alignment_report(...)` | computes operator-interaction analytics |
| `engine.runtime.event_log.append_event(...)` | writes structured runtime lifecycle events used by diagnostics, execution mode changes, and support snapshots |

## 5. Strategy And Portfolio

### `engine/strategy/portfolio.py`

This is the main portfolio rebalance and portfolio-state logic file.

| Function | What it does |
| --- | --- |
| `compute_rebalance()` | main portfolio rebalance routine |
| `get_portfolio_snapshot(limit_orders)` | returns a snapshot for the dashboard/API |
| `init_portfolio_db()` | ensures portfolio-specific DB state exists |
| `_load_recent_alert_candidates(...)` | loads recent alerts as candidate trade inputs |
| `_pick_best_per_symbol(...)` | chooses the best candidate alert per symbol |
| `_apply_temporal_dampener(...)` | reduces aggressiveness based on time decay |
| `_apply_impact_aware_sizing(...)` | adjusts sizing using execution realism and impact |
| `_optimize_capital_allocation(...)` | applies allocation optimization logic |
| `_apply_capital_at_risk_gate(...)` | applies high-level portfolio gating |
| `_emit_order(...)` | records a portfolio order/change |

### Important strategy helpers in the same file

| Function | Why it matters |
| --- | --- |
| `_score_from_alert(...)` | converts alert context into a portfolio scoring signal |
| `_desired_weight(...)` | maps score to target weight |
| `_execution_realism_factor(...)` | adjusts sizing based on execution realism |
| `_load_shadow_performance(...)` | reads shadow performance inputs |
| `_score_shadow_targets(...)` | scores shadow strategies or targets |

## 6. Execution

### `engine/execution/execution_policy_engine.py`

This file shapes orders before they go further into the execution path.

| Function | What it does |
| --- | --- |
| `apply_execution_policy(...)` | main execution policy function |
| `_regime_compatibility(...)` | checks whether execution fits regime context |
| `_scale_order_fields(...)` | rescales quantities/weights after policy decisions |
| `_log_suppression_event(...)` | records policy-driven suppression events |
| `_decision_from_alpha(...)` | maps remaining alpha to policy stance |
| `_alpha_remaining(...)` | estimates remaining alpha over time |

### `engine/execution/broker_router.py`

This file routes orders to broker adapters and failover logic.

| Function | What it does |
| --- | --- |
| `apply_new_portfolio_orders_router(...)` | main routing entrypoint for new portfolio orders |
| `_adaptive_execute_orders(...)` | adapts routing/execution behavior using conditions and failover |
| `_execution_gate_or_block(...)` | blocks if execution gating conditions fail |
| `_real_trading_gate_or_block(...)` | applies stricter real-trading gating |
| `_call_adapter(...)` | invokes a broker adapter |
| `_apply_one(...)` | applies one order through routing logic |

### `engine/execution/broker_failover_policy.py`

This file owns live-broker failover validation and terminal broker-failure classification.

| Function | What it does |
| --- | --- |
| `canonical_broker_name(...)` | normalizes broker aliases such as `alpaca_rest` and IBKR variants |
| `configured_failover_chain(...)` | reads the configured broker failover chain from environment |
| `validate_live_failover_chain(...)` | blocks unsafe live chains, including sim/paper fallback in live mode |
| `live_broker_environment_contract(...)` | validates `BROKER`, `BROKER_NAME`, `LIVE_BROKER`, and failover consistency without touching broker APIs |
| `broker_startup_preflight(...)` | runs broker credential/reachability preflight for live chains |
| `terminal_broker_failure(...)` | builds a standard terminal broker-failure payload that stops unsafe retry/failover behavior |
| `is_non_retryable_broker_result(...)` | identifies failures that must stop failover, such as auth and configuration failures |
| `broker_exception_terminal_failure(...)` | maps broker exceptions into non-retryable terminal failure payloads |

### `engine/runtime/live_execution_control.py`

This file centralizes emergency live-capital controls shared by gates, terminal order entry, and broker routing.

| Function | What it does |
| --- | --- |
| `env_flag_truthy(...)` | treats unknown non-empty emergency flag values as true so safety controls fail closed |
| `live_execution_disabled()` | returns whether `DISABLE_LIVE_EXECUTION` blocks live capital |
| `disabled_live_execution_gate(...)` | builds the standard hard-block barrier payload |
| `prelive_reconcile_policy_snapshot(...)` | validates whether live pre-submit reconciliation is required, enabled, or break-glass overridden |
| `prelive_reconcile_policy_gate(...)` | returns a fatal block when pre-live reconciliation policy is not satisfied |
| `record_prelive_reconcile_break_glass_audit(...)` | persists accepted break-glass reconciliation overrides into the runtime event log |

### `engine/execution/broker_submission_recovery.py`

This file protects against broker-accepted orders that local bookkeeping failed to record.

| Function | What it does |
| --- | --- |
| `unrecorded_submission_gate(...)` | blocks broker routing when unreconciled accepted submissions exist for the broker |
| `record_submission_unrecorded(...)` | marks a missing durable submission, emits a critical execution alert, audits the broker action, and records failure telemetry |

### `engine/execution/broker_apply_orders.py`

This file is the application path between portfolio orders and broker routing.

| Function | What it does |
| --- | --- |
| `main()` | main job/script entrypoint for applying portfolio orders |
| `_execution_gate_snapshot()` | snapshots execution gate state |
| `_load_latest_payload()` | loads the latest portfolio-order payload to apply |
| `_apply_epe_compat(...)` | applies execution-policy compatibility shaping |
| `_log_shadow_intents(...)` | writes shadow intents when in shadow/non-live mode |
| `_write_execution_meta_last(...)` | writes last-execution metadata |

### `engine/execution/execution_ai_advisor.py`

This is the advisory-only sidecar added during integration.

| Function | What it does |
| --- | --- |
| `persist_execution_advisories(...)` | writes advisory records for shaped orders |
| `list_execution_advisories(limit)` | returns advisory list payloads for the UI |
| `record_execution_advisory_action(...)` | stores operator approval/rejection actions |
| `_historical_execution_snapshot(...)` | collects historical fill/analytics evidence |
| `_estimate_expected_slippage_bps(...)` | estimates expected slippage |
| `_advisory_for_order(...)` | builds the advisory payload for one order |

## 7. API Layer

### `engine/api/api_read_advanced.py`

This file contains higher-level read assembly functions for the dashboard and operator APIs.

| Function | What it does |
| --- | --- |
| `get_portfolio_snapshot(...)` | assembles portfolio snapshot payloads |
| `get_execution_metrics_rolling()` | returns rolling execution metrics |
| `get_execution_metrics_by_symbol(...)` | returns execution metrics by symbol |
| `get_execution_cost_by_confidence()` | links execution cost to confidence buckets |
| `get_model_diagnostics()` | returns model diagnostics payload |
| `get_temporal_models(limit)` | returns temporal model list/status |
| `get_latest_portfolio_backtest()` | returns latest portfolio backtest summary |
| `get_social_features(symbol, limit)` | returns social feature rows |
| `get_social_regimes(symbol, limit)` | returns social regime rows |
| `get_shadow_capital_scores(...)` | returns shadow capital scores |
| `run_shadow_capital_scores(...)` | executes the shadow capital scoring path |
| `get_size_policy()` | returns size policy data |
| `get_recent_decisions(limit)` | returns recent decisions for the UI |
| `get_decision_detail(decision_id)` | returns one decision drilldown payload |

### `engine/api/api_ops.py`

This file is route metadata, not handler logic.

| Symbol | What it does |
| --- | --- |
| `ROUTE_SPECS` | declares ops/diagnostic routes and their handler names |
| `ROUTE_SPECS_OPS` | alias for the same route list |

### `engine/api/api_governance.py`

This file owns governance-specific API handlers.

| Function | What it does |
| --- | --- |
| `api_post_rollback(...)` | performs explicit model rollback |
| `get_promotion_status()` | returns whether promotion is enabled and allowed |
| `get_promotion_explain()` | returns promotion state plus recent audit detail |
| `get_governance_summary()` | returns governance dashboard summary |
| `api_get_exec_conf_calib(...)` | returns execution confidence calibration |

### `engine/api/api_system.py`

This file owns the broad system/operator diagnostic surface.

| Function | What it does |
| --- | --- |
| `api_get_runtime_watchdogs(...)` | returns watchdog summaries for stale jobs, feeds, and lifecycle issues |
| `api_get_support_snapshot(...)` | returns the operator repair snapshot package |
| `api_get_provider_telemetry(...)` | returns provider/feed telemetry and runtime health correlation |
| `api_get_service_status(...)` | returns engine/operator/runtime status summary |

### `engine/api/api_broker_config.py`

This file owns the broker configuration control-plane API.

| Function | What it does |
| --- | --- |
| `api_get_broker_config(...)` | returns broker config with masked credentials and last test result |
| `api_post_broker_config(...)` | persists normalized broker config and encrypted credentials; blocks non-sim activation until a passing test exists |
| `api_post_broker_test_connection(...)` | runs a structured broker config test and stores the last result |
| `api_get_broker_audit(...)` | returns recent broker config audit rows with credentials stripped |

### `engine/api/api_write.py`

This file owns write-side API helpers used by dashboard/operator mutation routes.

| Function | What it does |
| --- | --- |
| `ack_alert(...)` | writes or refreshes an alert acknowledgement with expiry and lifecycle event |
| `shelve_alert(...)` | shelves an alert with a required reason and bounded expiry |
| `resolve_alert(...)` | records alert resolution and lifecycle event |
| `write_job_event(...)` | delegates job-history writes to the runtime lock/history subsystem |
| `set_promotion_enabled(...)` | updates the promotion guard flag |

### `engine/runtime/backup_evidence.py`

This file checks whether backup, WAL archive, and restore-drill evidence is fresh enough for live operation.

| Function | What it does |
| --- | --- |
| `backup_restore_evidence_snapshot(...)` | returns base-backup, WAL, and restore-drill freshness against the configured RPO/RTO policy |

## 8. Dashboard Endpoints In `dashboard_server.py`

### `dashboard_server.py`

Because `dashboard_server.py` is still a large integration boundary, it contains many API functions directly.

The most important newer ones are:

| Function | Purpose |
| --- | --- |
| `api_get_recent_decisions(...)` | returns dashboard decision list |
| `api_get_decision_detail(...)` | returns dashboard decision detail |
| `api_post_ui_interaction(...)` | stores alert/decision interaction events |
| `api_get_governance_summary(...)` | returns governance summary for the dashboard |
| `api_get_support_snapshot(...)` | returns operator repair evidence and diagnostics |
| `api_get_runtime_watchdogs(...)` | returns runtime watchdog signals for the operator layer |
| `api_get_provider_telemetry(...)` | returns provider/feed telemetry for operator diagnostics |

Other high-signal ones include:

| Function | Purpose |
| --- | --- |
| `api_get_portfolio(...)` | returns portfolio data |
| `api_get_prices(...)` | returns price data |
| `api_get_trades(...)` | returns trade/execution views |
| `api_get_strategy_status(...)` | returns strategy state |
| `api_get_risk_summary(...)` | returns risk summary |
| `api_get_models_status(...)` | returns model state |

## 9. Suggested Read Paths By Task

### "I need to understand startup"

Read:

1. `start_system.py::main`
2. `dashboard_server.py::run_server`
3. `engine/runtime/startup_orchestrator.py::StartupOrchestrator`

### "I need to understand job behavior"

Read:

1. `engine/runtime/job_registry.py::get_job_spec`
2. `engine/runtime/jobs_manager.py::JobManager`
3. `engine/runtime/jobs_manager.py::get_all_job_states`

### "I need to understand portfolio decisions"

Read:

1. `engine/strategy/portfolio.py::compute_rebalance`
2. `engine/strategy/portfolio.py::_load_recent_alert_candidates`
3. `engine/strategy/portfolio.py::_apply_impact_aware_sizing`
4. `engine/strategy/portfolio.py::_emit_order`

### "I need to understand execution"

Read:

1. `engine/execution/broker_apply_orders.py::main`
2. `engine/execution/execution_policy_engine.py::apply_execution_policy`
3. `engine/execution/broker_router.py::apply_new_portfolio_orders_router`

### "I need to understand the new advisory and oversight features"

Read:

1. `engine/runtime/storage.py::fetch_recent_decisions`
2. `engine/runtime/storage.py::fetch_decision_detail`
3. `engine/runtime/storage.py::fetch_human_alignment_report`
4. `engine/execution/execution_ai_advisor.py::persist_execution_advisories`
5. `engine/execution/execution_ai_advisor.py::list_execution_advisories`
6. `engine/api/api_governance.py::get_governance_summary`

## 10. Practical Notes

- Files like `dashboard_server.py`, `storage.py`, and `portfolio.py` contain many helpers. Do not start at the top and read linearly without first locating the main entrypoint function.
- Route metadata and route handlers are split in some places. `api_ops.py` is just route declarations, while handler logic lives elsewhere.
- Some newer functionality was integrated into existing files instead of isolated subsystems. That is why the function map often points to additions inside `storage.py`, `dashboard_server.py`, and `portfolio.py`.

## 11. Short Summary

If you need the shortest version:

> `start_system.py::main` boots the system, `dashboard_server.py::run_server` exposes it, `storage.py::init_db` and `connect` manage persistence, `portfolio.py::compute_rebalance` decides portfolio changes, `execution_policy_engine.py::apply_execution_policy` shapes execution, `broker_router.py::apply_new_portfolio_orders_router` routes orders, and the newer dashboard/advisory/governance features hang off `storage.py`, `execution_ai_advisor.py`, and `api_governance.py`.
