# Trading System Function Map

This document maps the main Python files to the most important functions and classes inside them.

Last verified against code: 2026-06-26

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

### `engine/dashboard/routing.py`

This owns dashboard route assembly. `filter_route_specs_for_handlers(...)` now
fails loudly when any advertised `ROUTE_SPECS` entry names a handler that is
missing or non-callable in `dashboard_server.API_HANDLERS`, preventing silent
route drops during boot and UI contract validation.

### `boot/operator_server.js`

This is the local operator control plane and repair proxy.

| Function | What it does |
| --- | --- |
| `_llm(prompt)` | submits the bounded operator-AI prompt to the configured LLM backend |
| `operatorProxyGet(...)` | proxies operator reads through to dashboard/runtime endpoints |
| `applyAiPatchWithBackup(...)` | writes a guarded patch file change from an approved analysis payload, backing up the original file first |
| `rollbackAiPatch(...)` | rolls back a previously applied operator patch by patch id |
| `logAgentAction(...)` | persists operator-AI action and audit entries |

## 3. Runtime And Orchestration

### `engine/runtime/job_registry.py`

This file is the canonical runnable-job registry. `engine/runtime/job_catalog.py`
serializes the operator-facing catalog and backend safety policy from it.

The runtime boot set includes `kill_switch_cache_refresh`, a non-execution
daemon that periodically reloads the DB-backed kill-switch snapshot into Redis.
It keeps `loaded_ts_ms`/`source`/`max_age_ms` diagnostics fresh and prevents
kill-switch cache freshness from depending only on activation or clear writes.

The PatchTST path includes `pretrain_patchtst_models`, a shadow-default
oneshot job that runs masked patch reconstruction before supervised
`train_patchtst_models` fine-tuning when PatchTST is enabled in the pipeline.
The iTransformer path includes `train_itransformer_models`, also a shadow-default
oneshot training job. It writes feature-contract artifacts and OOS prediction
rows for marketplace visibility, but those OOS rows are not live-promotion
evidence without the normal realized-PnL, replay, and promotion-gate path.
The TSFM benchmark path includes `tsfm_benchmark`, an opt-in shadow one-shot
job that evaluates Chronos/TimesFM/Moirai/Toto/fake adapters under PIT
walk-forward splits, writes OOS predictions and provenance, and exposes only
shadow marketplace evidence until normal champion/challenger promotion gates
are satisfied.
The graph challenger path includes `graph_challenger_benchmark`, an opt-in
shadow one-shot job enabled by `GRAPH_CHALLENGER_BENCHMARK_ENABLED=1`. It
builds PIT-safe temporal heterogeneous graph samples from existing feature
snapshots and graph relationship edges, trains the dependency-free relational
baseline, persists artifacts/OOS rows, and publishes only shadow marketplace
evidence with graph-vs-node-only ablation metrics.

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

### `engine/runtime/job_catalog.py`

This file owns the first-class operator job catalog contract consumed by
`/api/jobs`, `/api/jobs/catalog`, the dashboard Job Catalog, and command
palette job actions.

| Function | What it does |
| --- | --- |
| `build_job_catalog()` | serializes every registered job with purpose, prerequisites, safety, action policy, and log/history links |
| `enrich_job_runtime_row(...)` | merges live/persisted job state with the catalog row shape |
| `dangerous_job_start_confirmation_error(...)` | enforces backend confirmation for execution-sensitive or destructive/admin job starts and blocks unavailable jobs |

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

### `engine/runtime/health.py`

This file owns runtime health, readiness inputs, and preflight-facing snapshot evidence.

| Function or Class | What it does |
| --- | --- |
| `get_health_snapshot()` | small cache/lock/connection driver that runs the health-check registry and finalizes the canonical snapshot |
| `HealthSnapshotCheck` | named probe registration unit used by `_HEALTH_SNAPSHOT_CHECKS` |
| `HealthSnapshotContext` | mutable per-snapshot context shared by focused health probes |
| `_run_health_checks(...)` | isolates probe exceptions, records registry failures, and continues later checks |
| `_finalize_health_snapshot(...)` | computes aggregate `ok`, startup gates, reasons, root causes, critical blockers, system stage, and data-flow status |

## 4. Storage Layer

### `engine/runtime/storage.py`, `engine/runtime/storage_pg.py`, and `engine/runtime/storage_sqlite.py`

`engine/runtime/storage.py` is the public storage facade. It selects one concrete backend, validates the backend module against the `StorageBackend` contract, and then exposes the same facade symbols to callers. Production-like operation selects `engine/runtime/storage_pg.py`; isolated Python tests can opt into `engine/runtime/storage_sqlite.py` through `TS_STORAGE_BACKEND=sqlite` or `TS_TESTING=1`, and real supervised/prod/live processes reject that SQLite backend.

The SQLite/Postgres bridge work is intentionally a bounded first slice: runtime function-code cloning is removed, but `_PG_COMPAT_HELPER_NAMES` remains as a documented compatibility shim until those legacy helpers are moved to backend-neutral repositories. `tools/validate_repo.py` blocks any regression back to code-object cloning and confines the remaining `storage_pg` import to the compatibility loader.

Every `storage.*` symbol listed below — and the `storage.py::*` references in the read paths further down — is a **facade re-export**: the name is reachable on `engine/runtime/storage.py`, but the concrete implementation is physically in `engine/runtime/storage_pg.py` for production-like operation (or `engine/runtime/storage_sqlite.py` under the opt-in test backend). When you need the real body, open `storage_pg.py`.

| Function or Class | What it does |
| --- | --- |
| `storage.init_db()` | facade entrypoint that initializes the selected backend schema |
| `storage.init_timeseries_storage()` | initializes optional Timescale/feature/telemetry sidecars when enabled |
| `storage.get_timeseries_storage_snapshot()` | returns optional sidecar readiness and degraded-state details |
| `storage.get_active_backend_name()` | returns `postgres` or `sqlite` for the selected backend |
| `storage.get_active_backend()` | returns the concrete module after facade contract validation |
| `storage.connect()` | returns a routed DB connection through the active backend |
| `storage.connect_ro()` | returns a read-oriented connection through the active backend |
| `storage.run_write_txn(fn, ...)` | runs a managed write transaction with backend safety behavior |
| `storage.get_db_validation_snapshot()` | returns schema validation used by preflight/startup |
| `storage.get_db_debug_snapshot()` | returns richer DB and connection diagnostics |
| `storage.put_event(...)` | writes an event row |
| `storage.put_price(...)` | writes a price row |
| `storage.acquire_job_lock(...)` | obtains a runtime job lock |
| `storage.release_job_lock(...)` | releases a runtime job lock |
| `storage.touch_job_lock(...)` | heartbeats a runtime job lock |
| `storage.put_job_heartbeat(...)` | writes a job heartbeat |
| `storage.get_job_checkpoint(job_name)` | reads a job checkpoint |
| `storage.put_job_checkpoint(...)` | writes a job checkpoint |

### Newer storage helpers added by the integration work

| Function | What it does |
| --- | --- |
| `storage.log_alert_interaction(...)` | stores passive alert/decision interaction rows |
| `storage.log_decision_view(...)` | stores decision-detail view events |
| `storage.fetch_recent_decisions(limit)` | returns decision cards for the dashboard |
| `storage.fetch_decision_detail(decision_id)` | builds a decision drilldown payload |
| `storage.fetch_human_alignment_report(...)` | computes operator-interaction analytics |
| `engine.runtime.event_log.append_event(...)` | writes structured runtime lifecycle events used by diagnostics, execution mode changes, and support snapshots |

## 5. Strategy And Portfolio

### `engine/strategy/portfolio.py`

This is the main portfolio rebalance and portfolio-state logic file.

| Function | What it does |
| --- | --- |
| `compute_rebalance()` | main portfolio rebalance routine; runs the staged pipeline in production order |
| `get_portfolio_snapshot(limit_orders)` | returns a snapshot for the dashboard/API |
| `init_portfolio_db()` | ensures portfolio-specific DB state exists |
| `_load_recent_alert_candidates(...)` | loads recent alerts as candidate trade inputs |
| `_load_rebalance_inputs_stage(...)` | loads candidate alerts/current state and marks candidates seen |
| `_load_allocator_stage(...)` | loads allocator output, live/shadow strategy lists, competition plans, and allocation fallbacks |
| `_construct_rebalance_targets_stage(...)` | builds multi-strategy target weights before normalization |
| `_normalize_rebalance_targets_stage(...)` | canonicalizes target side/weight/model metadata and enforces gross caps |
| `_apply_rebalance_overlays_stage(...)` | applies blacklist, exploration, shrinkage, allocation, volatility, sizing, and execution overlays |
| `_apply_rebalance_risk_gates_stage(...)` | applies portfolio risk engine, hard risk gate, tail-risk, netting, total-risk, and flip-flop gates |
| `_apply_rebalance_execution_block_stage(...)` | converts critical degraded phases into an execution-blocked rebalance result |
| `_emit_rebalance_orders_stage(...)` | emits `portfolio_orders` and updates `portfolio_state` from final targets |
| `_persist_rebalance_stage(...)` | persists drawdown, strategy, timestamp, and runtime-health metadata |
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

### `engine/strategy/predictor.py`

Live prediction orchestrator and model-family routing — the runtime live-scoring path (`engine/strategy/predict.py` is a separate ~945-byte CLI relevance-stats shim and is not this module).

| Function | What it does |
| --- | --- |
| `predict_live_symbol(...)` | produces the live prediction for one symbol across the routed model families |
| `batch_predict_live_symbols(...)` | batched live-prediction entrypoint used by the runtime scoring path |
| `predict_event(...)` | scores a labeled event vector through the active model routing |
| `available_model_families()` | returns the model families currently eligible for routing |
| `realtime_inference_enabled()` | reports whether realtime inference is currently permitted |

### `engine/strategy/model_marketplace.py`

Champion/challenger marketplace: scoring, replay validation, self-critic, and capital planning for competing models.

| Function | What it does |
| --- | --- |
| `update_model_score(...)` | records or updates a model's marketplace score |
| `recompute_marketplace_scores()` | recomputes marketplace scores across candidates |
| `run_self_critic(...)` | runs the self-critic review used in the promotion path |
| `build_replay_validation_snapshot(...)` | builds the replay-validation evidence snapshot |
| `top_challengers(limit)` | returns the current top challenger candidates |
| `compute_capital_plan()` | computes the capital allocation plan across the marketplace |
| `publish_marketplace_snapshot(...)` | publishes the marketplace snapshot for the UI/operator |

### `engine/strategy/graph_challenger.py`

Shadow-only graph challenger framework for temporal heterogeneous graph
experiments. It never participates in order selection; it writes artifacts,
OOS predictions, graph metadata, feature/schema contracts, and marketplace
rows that remain non-promotable under `evaluate_graph_promotion_gate`.

| Function | What it does |
| --- | --- |
| `build_graph_challenger_dataset(...)` | materializes PIT-safe temporal graph samples from model-feature snapshots and graph relationship sources |
| `train_graph_challenger_models(...)` | trains node-only and relational-message ridge baselines on the same split |
| `run_graph_challenger_benchmark(...)` | persists the artifact, OOS rows, run metadata, and shadow marketplace rows |
| `load_graph_challenger_artifact(...)` | reloads the content-addressed graph challenger artifact |

### `engine/strategy/champion_manager.py`

Champion/challenger selection and the model-competition lifecycle.

| Function | What it does |
| --- | --- |
| `get_champion_assignment(...)` | returns the current champion assignment for a scope/symbol/horizon |
| `set_champion_assignment(...)` | records a champion assignment |
| `evaluate_competition_cycle()` | evaluates one competition cycle and stages promotion actions |
| `run_model_competition_job()` | the registered job that runs the competition end to end |
| `auto_promote_best()` | promotes the best eligible challenger subject to the governance gates |

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

### `engine/execution/contextual_bandit_slicer.py`

This file implements the execution-only learned slicing prototype.

| Function | What it does |
| --- | --- |
| `select_execution_adjustment(...)` | chooses bounded slice percentage, participation, interval, and entry-delay parameters |
| `enforce_execution_only_decision(...)` | rejects learned decisions that emit forbidden fields or values outside EPE-provided bounds |
| `validate_routed_learned_orders(...)` | broker-router guard that blocks direct learned-policy orders without EPE lock metadata |
| `evaluate_against_baselines(...)` | compares learned slicing against TWAP/VWAP/POV/adaptive baselines on shortfall, slippage, fill risk, and adverse selection |

### `engine/execution/lob_simulation.py`

This file owns the first reactive LOB simulation and shadow DeepLOB readiness slice.

| Function | What it does |
| --- | --- |
| `build_reactive_lob_simulation(...)` | computes queue position, spread crossing, queue-aware partial fills, adverse-selection bps, sweep bps, and L2-calibrated market impact for broker-sim fills |
| `lob_deeplob_readiness_snapshot(...)` | blocks the shadow model path unless L2 depth, latency assumptions, and simulator calibration evidence are sufficient |
| `shadow_deeplob_execution_signal(...)` | emits shadow-only execution-timing/adverse-selection diagnostics, never portfolio-selection or sizing directives |

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
| `validate_live_failover_chain(...)` | blocks unsafe live chains, including sim/paper fallback and mixed live brokers in live mode |
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

### `engine/execution/execution_ledger.py`

The execution attribution ledger (the largest execution module, ~218KB). It records order submissions and fills and computes execution analytics, P&L attribution, and capital efficiency.

| Function | What it does |
| --- | --- |
| `init_execution_ledger()` | initializes the execution-ledger schema |
| `log_submit(...)` | records an order submission into the ledger |
| `log_fill(...)` | records a fill against a previously submitted order |
| `audit_execution_integrity(...)` | audits ledger integrity and reports inconsistencies |
| `compute_metrics_snapshot(limit_orders)` | computes the execution-metrics snapshot for the dashboard |
| `compute_pnl_attribution_snapshot(lookback_orders)` | computes the P&L attribution snapshot |
| `compute_capital_efficiency_snapshot(limit_orders)` | computes the capital-efficiency snapshot |

## 7. Risk

### `engine/risk/portfolio_risk_engine.py`

The portfolio-risk engine (~138KB, one public entrypoint). It applies additive exposure, drawdown, volatility-target, and correlation-cluster checks and writes the current portfolio-risk state consumed by API reads and the execution barrier.

| Function | What it does |
| --- | --- |
| `apply_portfolio_risk_engine(...)` | the public entrypoint: evaluates gross/net caps, the drawdown throttle, volatility targeting, and correlation-cluster limits for a candidate portfolio and persists the resulting risk state and snapshots |

Knob families consumed here are documented in the configuration glossary: `PORTFOLIO_RISK_MAX_GROSS`/`PORTFOLIO_GROSS_CAP` and `PORTFOLIO_RISK_MAX_NET` (gross/net caps, defaults `1.00`/`0.60`), the drawdown throttle (`PORTFOLIO_CAR_MAX`), volatility targeting, and correlation-cluster limits (`PORTFOLIO_RISK_CLUSTER_MAX_GROSS`, default `0.45`).

### `engine/risk/monte_carlo_risk_engine.py`

The Monte Carlo risk refresher. It simulates forward portfolio paths and persists stressed VaR/CVaR and drawdown summaries plus compact visualization artifacts into `risk_state`.

| Function | What it does |
| --- | --- |
| `request_monte_carlo_refresh(desired)` | requests a background Monte Carlo refresh (`MC_SIMULATIONS` paths over `MC_HORIZON` steps) and persists the stressed risk summary served by `GET /api/risk/monte_carlo` |

### `engine/risk/var_backtesting.py`

VaR/CVaR model-validation helpers. This module persists forecast/backtest evidence, evaluates Kupiec POF and Christoffersen independence tests, computes rolling exception traffic-light status, and serves the read-only payload behind `GET /api/risk/var_backtest`.

| Function | What it does |
| --- | --- |
| `run_var_backtest(...)` | consumes matured `risk_var_forecasts`, aligns realized returns from `equity_history`, and upserts `risk_var_backtest_results` evidence rows |
| `build_var_backtest_payload(...)` | builds the dashboard/API payload for recent VaR/CVaR backtest rows and explicit empty/schema-missing states |

### `engine/risk/covariance.py`

Canonical covariance/correlation facade for money-at-risk paths (consumed by `portfolio_risk_engine.py`, `monte_carlo_risk_engine.py`, and `engine/strategy/risk.py`). It loads point-in-time price returns once for a symbol set, estimates with a Ledoit-Wolf shrinkage default, and falls back with explicit diagnostics when history is insufficient.

| Function | What it does |
| --- | --- |
| `estimate_covariance(con, symbols, ...)` | main entrypoint: loads aligned PIT returns and returns a `RiskCovarianceEstimate` (covariance, correlation, vols, diagnostics) with PSD/RMT handling and explicit fallbacks |
| `estimate_covariance_from_returns(...)` / `estimate_covariance_from_return_matrix(...)` | estimate directly from supplied return series/matrices |
| `correlation_matrix_dict(...)` / `covariance_matrix_dict(...)` | serialize an estimate into symbol-keyed dict matrices |
| `portfolio_volatility_from_estimate(...)` | computes portfolio volatility from an estimate and weights |

### `engine/strategy/garch_vol.py`

Conditional (GARCH-family/EWMA) volatility forecasts used as risk-sizing inputs only — they are not registered as alpha features by default. Forecasts persist into the `garch_vol_forecasts` table and are produced by the `garch_vol_forecast` job (`engine/strategy/jobs/garch_vol_forecast.py`).

| Function | What it does |
| --- | --- |
| `forecast_garch_for_symbol(con, symbol, ...)` | builds a one-symbol conditional-volatility forecast (optional `arch` GARCH fit, EWMA/realized-variance fallback) |
| `run_garch_vol_forecast_job(...)` | the registered job body: forecasts the candidate symbol set and upserts forecast rows |
| `latest_garch_forecast(...)` | reads the most recent persisted forecast for a symbol |
| `ensure_garch_vol_schema(con)` | ensures the forecast table/index exist |

## 8. API Layer

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

### `engine/strategy/production_monitoring.py`

This file owns production drift, calibration, and shadow-vs-live monitoring.

| Function | What it does |
| --- | --- |
| `compute_production_monitoring_metrics(...)` | computes latest feature drift, prediction drift, missing-feature, label, calibration, conformal, shadow/live, and net-PnL metrics |
| `compute_and_store_production_monitoring(...)` | persists latest metrics and emits retrain or shadow-review signal rows without promotion side effects |
| `get_latest_production_monitoring_snapshot(...)` | returns latest monitoring metrics for API/dashboard composition |

### `engine/api/api_system.py`

This file owns the broad system/operator diagnostic surface.

| Function | What it does |
| --- | --- |
| `api_get_runtime_watchdogs(...)` | returns watchdog summaries for stale jobs, feeds, and lifecycle issues |
| `api_get_support_snapshot(...)` | returns the operator repair snapshot package |
| `api_get_provider_telemetry(...)` | returns provider/feed telemetry and runtime health correlation |
| `api_get_service_status(...)` | returns engine/operator/runtime status summary |
| `api_get_alpha_decay(...)` | canonical `/api/alpha_decay` owner; returns latest alpha-decay runtime state plus per-strategy-limited historical strategy rows and runtime rows for `ui/risk_charts.js` |

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
| `ack_alert(...)` | writes or refreshes an alert acknowledgement with expiry and lifecycle event; unknown alert ids return `404 not_found` without audit writes |
| `shelve_alert(...)` | shelves a known alert with a required reason and bounded expiry; unknown alert ids return `404 not_found` without audit writes |
| `resolve_alert(...)` | records alert resolution and lifecycle event; unknown alert ids return `404 not_found` without audit writes |
| `write_job_event(...)` | delegates job-history writes to the runtime lock/history subsystem |
| `set_promotion_enabled(...)` | updates the promotion guard flag |

### `engine/runtime/backup_evidence.py`

This file checks whether backup, WAL archive, and restore-drill evidence is fresh enough for live operation.

| Function | What it does |
| --- | --- |
| `backup_restore_evidence_snapshot(...)` | returns base-backup, WAL, and restore-drill freshness against the configured RPO/RTO policy |

## 9. Dashboard Endpoints In `dashboard_server.py`

### `dashboard_server.py`

Because `dashboard_server.py` is still a large integration boundary, it contains many API functions directly.
`/api/alpha_decay` is not dashboard-local: `dashboard_server.py` mounts and
validates `engine/api/api_system.py::api_get_alpha_decay` as the sole production
handler for that route.

The most important newer ones are:

| Function | Purpose |
| --- | --- |
| `api_get_recent_decisions(...)` | returns dashboard decision list |
| `api_get_decision_detail(...)` | returns dashboard decision detail |
| `api_post_ui_interaction(...)` | stores alert/decision interaction events |
| `api_get_governance_summary(...)` | returns governance summary for the dashboard |

(`api_get_support_snapshot`, `api_get_runtime_watchdogs`, and `api_get_provider_telemetry` are
**not** dashboard-local — they are defined in `engine/api/api_system.py`; see that section above.)

Other high-signal ones include:

| Function | Purpose |
| --- | --- |
| `api_get_portfolio(...)` | returns portfolio data |
| `api_get_prices(...)` | returns price data |
| `api_get_trades(...)` | returns trade/execution views |
| `api_get_strategy_status(...)` | returns strategy state |
| `api_get_risk_summary(...)` | returns risk summary |
| `api_get_models_status(...)` | returns model state |

## 10. Suggested Read Paths By Task

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

## 11. Practical Notes

- Files like `dashboard_server.py`, `storage.py`, and `portfolio.py` contain many helpers. Do not start at the top and read linearly without first locating the main entrypoint function.
- Route metadata and route handlers are split in some places. `api_ops.py` is just route declarations, while handler logic lives elsewhere.
- Some newer functionality was integrated into existing files instead of isolated subsystems. That is why the function map often points to additions inside `storage.py`, `dashboard_server.py`, and `portfolio.py`.

## 12. Short Summary

If you need the shortest version:

> `start_system.py::main` boots the system, `dashboard_server.py::run_server` exposes it, `storage.py::init_db` and `connect` manage persistence, `portfolio.py::compute_rebalance` decides portfolio changes, `execution_policy_engine.py::apply_execution_policy` shapes execution, `broker_router.py::apply_new_portfolio_orders_router` routes orders, and the newer dashboard/advisory/governance features hang off `storage.py`, `execution_ai_advisor.py`, and `api_governance.py`.
