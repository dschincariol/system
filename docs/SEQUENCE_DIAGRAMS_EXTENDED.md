# Extended Sequence Diagrams

The diagrams below are grounded in the inspected startup entrypoints, runtime modules, route handlers, and UI callers. They are meant to be used during onboarding, incident response, and design review.

## 1. Startup

```mermaid
sequenceDiagram
    actor Service as Service or Operator
    participant Start as start_system.py
    participant DS as services.data_source_manager
    participant Life as engine.runtime.lifecycle_state
    participant Dash as dashboard_server.py
    participant Repair as engine.api.api_self_repair
    participant Sup as engine.runtime.supervisor
    participant Orch as engine.runtime.startup_orchestrator
    participant IngEntry as start_ingestion.py
    participant Ing as engine.runtime.ingestion_runtime

    Service->>Start: launch runtime
    Start->>Start: load .env, clear dead proxy sentinel
    Start->>Start: set TRADING_LOGS, TRADING_DATA, DB_PATH
    Start->>Start: write runtime_meta startup_trace/import_smoke
    Start->>Start: validate_runtime_architecture()
    Start->>Life: set BOOTING
    Start->>Start: bootstrap_first_run(mode)
    Start->>DS: initialize()
    Start->>DS: apply_runtime_environment()
    Start->>Start: start cache, challenger, async writer, pg price storage, event runtime (best effort)
    Start->>Dash: run_server()
    Dash->>Dash: bind HTTP and serve ui/*
    Dash->>Life: set WARMING_UP if first price not confirmed
    Dash->>Dash: start lifecycle monitor, model scoring, auto_rollback_loop
    Dash->>Dash: run bounded preflight
    alt preflight failed
        Dash->>Repair: api_post_self_repair()
    end
    Dash->>Sup: validate_graph()
    Dash->>Sup: auto-boot daemon jobs when configured
    opt AUTO_STARTUP_BOOTSTRAP enabled
        Dash->>Orch: run(mode)
        Orch->>Sup: start dependency-ordered jobs
        Orch->>Orch: health_ready -> universe -> prices -> pipeline jobs
        Orch->>Life: set LIVE when prices exist
    end
    Start->>IngEntry: spawn after dashboard bind
    IngEntry->>IngEntry: set supervised env flags and job name
    IngEntry->>IngEntry: db_repair.repair()
    IngEntry->>IngEntry: write runtime_meta["ingestion_state"] initial payload
    IngEntry->>Ing: main()
    Ing->>Ing: reconcile desired ingestion jobs
    Start->>Start: wait for startup health
    alt startup health ready
        Start->>Life: stay/live as healthy runtime
    else fail-open configured
        Start->>Life: set DEGRADED
    end
```

## 2. Ingest To Feature To Decision To Execution

```mermaid
sequenceDiagram
    participant CP as services.data_source_manager
    participant Ing as engine.runtime.ingestion_runtime
    participant Jobs as ingestion child jobs
    participant DB as trading.db
    participant Val as engine.strategy.validation
    participant MI as engine.strategy.model_intent
    participant DL as engine.strategy.decision_log
    participant Port as engine.strategy.portfolio
    participant Intents as engine.strategy.portfolio_execution_intents
    participant Dec as engine.decision_engine
    participant Exec as engine.execution.broker_apply_orders
    participant Gate as engine.runtime.gates
    participant KS as engine.execution.kill_switch
    participant Router as broker_router or broker_sim

    CP-->>Ing: desired ingestion jobs and env projection
    Ing->>Jobs: start/stop/restart children
    Jobs->>DB: write prices, events, news, options, macro, social, weather data
    Val->>DB: append prediction_history
    Val->>DB: upsert predictions
    MI->>MI: build_model_intent(symbol, horizon_s, expected_z, confidence, ...)
    DL->>DB: insert decision_log row
    Port->>DB: update portfolio_state
    Port->>DB: insert portfolio_orders
    Intents->>DB: load latest portfolio_orders batch
    Intents->>Intents: enrich with alert timing, model identity, budgets, exec regime
    Intents->>Dec: evaluate(prediction, confidence, risk)
    Dec-->>Intents: allow or downgrade to shadow
    Exec->>Intents: load_latest_execution_intents()
    Exec->>Gate: execution_gate_snapshot()
    Exec->>KS: execution_allowed()
    alt real execution target survives all gates
        Exec->>Router: apply_new_portfolio_orders_router(...)
    else shadow target or live blocked
        Exec->>Router: apply_new_portfolio_orders(...) via broker_sim
    end
```

What this diagram omits on purpose:

- the individual ingestion job implementations
- the full feature-engineering internals inside `process_events`
- the full execution-policy-engine shaping tree

Those subsystems exist, but the inspected code paths above are the stable cross-module boundaries.

## 3. Order To Fill To Attribution

```mermaid
sequenceDiagram
    participant PO as portfolio_orders
    participant Exec as engine.execution.broker_apply_orders
    participant Broker as broker_router and broker adapters
    participant EO as execution_orders
    participant Poll as engine.execution.execution_poll_and_attrib
    participant EF as execution_fills
    participant Ledger as engine.execution.execution_ledger
    participant TAL as trade_attribution_ledger
    participant MM as model marketplace and rankings
    participant TL as engine.runtime.trade_lifecycle

    PO-->>Exec: latest execution intents
    Exec->>EO: persist canonical execution order lineage
    Exec->>Broker: submit real orders when allowed
    Broker-->>Exec: broker order ids and status

    loop every execution poll interval
        Poll->>Broker: poll_and_log_fills(after_ts_ms)
        Broker->>EF: write fills
        Poll->>Ledger: compute_metrics_snapshot()
        Poll->>Ledger: compute_pnl_attribution_snapshot()
        Poll->>Ledger: compute_capital_efficiency_snapshot()
        Poll->>Ledger: build_execution_analytics()
        Poll->>TAL: upsert_from_latest_pnl_attribution_snapshot()
        Poll->>MM: recompute_marketplace_scores()
        Poll->>MM: recompute_model_rankings()
        Poll->>Poll: compute_pnl_decomposition_snapshot()
        Poll->>Poll: enforce residual and orphan invariants
    end

    TL->>PO: read source portfolio_orders
    TL->>EO: read execution_orders by source_alert_id or client_order_id
    TL->>EF: read fills by client_order_id
    TL->>Ledger: read pnl_attribution and model_position_state
    TL-->>TL: build end-to-end trade trace
```

Operationally important consequence:

- if `execution_orders` is present but `execution_fills` is not, the routing layer ran but post-trade polling did not complete
- if `execution_fills` exists but `pnl_attribution` does not, `execution_poll_and_attrib.py` has not finished or an invariant failed

## 4. Operator Diagnostics And Repair Path

```mermaid
sequenceDiagram
    actor Operator
    participant Agent as services.operator_ai.agent.js
    participant Op as boot/operator_server.js
    participant Dash as dashboard_server.py
    participant Ops as engine.api.api_operator_handlers
    participant Repair as engine.api.api_self_repair
    participant Jobs as JobsManager
    participant Safety as kill_switch and execution_mode

    Operator->>Agent: request diagnostic analysis
    Agent->>Op: GET /api/operator/service_status
    Agent->>Op: GET /api/operator/health
    Agent->>Op: GET /api/operator/runtime_logs?lines=80
    Agent->>Op: GET /api/operator/support_snapshot?mode=quick
    Agent->>Op: GET /api/operator/snapshot?mode=quick
    Agent->>Op: GET /api/operator/provider_telemetry
    Agent->>Op: GET /api/operator/runtime_watchdogs
    Agent->>Op: GET /api/execution/barrier
    Op->>Dash: proxy support and runtime diagnostics where applicable
    Agent->>Agent: normalize into diagnostics-only result
    Agent->>Agent: append var/log/ai_operator_log.jsonl
    Agent-->>Operator: {analysis, action:null, executed:null}

    Operator->>Dash: POST /api/operator/autofix
    Dash->>Ops: api_post_operator_autofix()
    Ops->>Repair: api_post_repair_schema(...)
    Ops->>Jobs: restart feed jobs
    Ops-->>Operator: {ok, steps:[repair_schema, restart_feeds]}

    opt emergency stop instead
        Operator->>Dash: POST /api/operator/emergency_stop
        Dash->>Ops: api_post_operator_emergency_stop()
        Ops->>Jobs: stop jobs
        Ops->>Safety: activate global kill switch
        Ops->>Safety: set_execution_armed(0)
        Ops-->>Operator: status KILL_SWITCH, execution_allowed false
    end
```

Important distinction:

- `services/operator_ai/agent.js` is diagnostics-only
- `boot/operator_server.js` separately exposes AI patch preview/apply/rollback routes
- `engine.api.api_operator_handlers.py` owns the Python autofix and emergency-stop paths
- `services/operator_ai/agent.js` currently fetches `/api/execution/barrier` but does not assign that final response into the returned context object because its promise destructuring is offset by one slot

## 5. Data-Source Credential, Update, And Test Flow

```mermaid
sequenceDiagram
    actor Operator
    participant UI as ui/data_sources.js
    participant Routes as routes/data_sources_routes.py
    participant Mgr as services.data_source_manager
    participant DB as data_sources tables
    participant Meta as runtime_meta
    participant Ing as engine.runtime.ingestion_runtime

    Operator->>UI: open data source control plane
    UI->>Routes: GET /api/data_sources
    Routes->>Mgr: list_sources()
    Routes->>Mgr: list_source_templates()
    Routes->>Mgr: get_runtime_snapshot()
    Routes->>Mgr: get_desired_ingestion_jobs()
    Mgr-->>UI: sources, templates, runtime, auth, desired_ingestion_jobs

    Operator->>UI: update source settings or credentials
    UI->>Routes: POST create/update/enable/disable
    Routes->>Mgr: create_source/update_source/set_enabled(...)
    Mgr->>DB: persist source row and audit/log entries
    Routes->>Mgr: manage_lifecycle(reason)
    Mgr->>Meta: mark_runtime_dirty()
    Mgr-->>Routes: lifecycle {ok, reason, desired_jobs, ingestion_runtime_started}
    Routes-->>UI: {ok, source or deleted, lifecycle}

    Ing->>Mgr: desired_ingestion_jobs(...)
    Mgr-->>Ing: desired jobs plus config hash
    Ing->>Ing: reconcile child processes to desired state

    Operator->>UI: run Test Connection
    UI->>Routes: POST /api/data_sources/test
    Routes->>Mgr: test_connection(source_key, actor, client_ip)
    Mgr->>DB: update status to tested or test_failed
    Mgr-->>UI: {ok, source_key, message or error}
```

Two debugging implications matter here:

- if the source row updates but `desired_ingestion_jobs` does not change, the problem is in template-to-job mapping, not ingestion supervision
- if `desired_ingestion_jobs` changes but the runtime stays stale, inspect `runtime_meta["data_sources_dirty"]`, `runtime_meta["data_sources_reload_ts_ms"]`, and the ingestion runtime reconciliation loop
