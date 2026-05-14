# Trading System Sequence Diagrams

This document shows the most important repo workflows as sequence diagrams.

It complements the architecture, database, and function maps by answering:

- what happens first
- what calls what
- what gets written to the database
- what the operator sees at the end

These are intentionally simplified. They show the main control path, not every helper or edge case.

## 1. Startup Flow

This is the main supervised startup path.

```mermaid
sequenceDiagram
    participant User as Operator or launcher
    participant SS as start_system.py
    participant DB as engine/runtime/storage.py
    participant DS as dashboard_server.py
    participant SO as StartupOrchestrator
    participant JR as job_registry.py
    participant JM as jobs_manager.py
    participant IR as ingestion runtime
    participant UI as Browser dashboard

    User->>SS: start runtime
    SS->>SS: bootstrap env and paths
    SS->>SS: run import smoke and validation gate
    SS->>DB: init_db()
    SS->>DS: boot dashboard server
    DS->>SO: create startup orchestration state
    SO->>JR: read boot jobs and runtime architecture
    SO->>JM: initialize managed jobs
    SS->>IR: spawn ingestion if enabled
    DS-->>UI: serve API and dashboard assets
```

### Human explanation

The runtime does not just "start a script." It validates the environment, ensures the DB is ready, brings up the HTTP/UI boundary, initializes orchestration, and then supervises ingestion and other jobs.

## 2. Decision-To-Order Flow

This is the main trading path from signal to execution.

```mermaid
sequenceDiagram
    participant Data as Ingestion tables
    participant Strat as Strategy and prediction jobs
    participant DL as decision_log
    participant Port as portfolio.py
    participant PO as portfolio_orders
    participant EPE as execution_policy_engine.py
    participant Apply as broker_apply_orders.py
    participant Router as broker_router.py
    participant Exec as execution_orders or execution_fills
    participant Adv as execution_ai_advisor.py

    Data->>Strat: raw data and features available
    Strat->>DL: write predictions and decision context
    DL->>Port: supply candidate decision inputs
    Port->>Port: score, size, and gate candidates
    Port->>PO: emit portfolio orders
    PO->>Apply: latest payload loaded
    Apply->>EPE: apply execution policy
    EPE-->>Apply: shaped or suppressed intents
    Apply->>Router: route executable intents
    Router->>Exec: write execution orders and later fills
    Apply->>Adv: persist advisory-only execution guidance
```

### Human explanation

The repo does not jump directly from model output to broker order. It passes through decision recording, portfolio shaping, policy checks, routing, and only then execution. The newer advisory layer observes that path and records guidance without taking authority over it.

## 3. Alert-To-Dashboard Flow

This is how alerts and decisions become operator-visible UI state.

```mermaid
sequenceDiagram
    participant Rules as Rules or runtime logic
    participant Alerts as alerts table
    participant API as dashboard_server plus engine/api
    participant UI as ui/dashboard.js
    participant User as Operator
    participant Int as alert_interactions and decision_views

    Rules->>Alerts: write alert row
    API->>Alerts: read alerts and decision summaries
    API-->>UI: return panel payloads
    UI-->>User: show alerts and decisions
    User->>UI: open or acknowledge item
    UI->>API: POST interaction event
    API->>Int: persist interaction row
```

### Human explanation

The dashboard is backed by persisted state, not just ephemeral in-memory events. When the operator opens a decision or interacts with an alert, that behavior can now be stored and later used by the human-alignment analytics layer.

## 4. Human-Alignment Analytics Flow

This is the passive oversight loop added during integration.

```mermaid
sequenceDiagram
    participant UI as Dashboard UI
    participant API as Operator API
    participant DB as alert_interactions
    participant HA as human-alignment read logic
    participant User as Operator

    User->>UI: open, ignore, or review alerts
    UI->>API: POST interaction events
    API->>DB: store interaction rows
    API->>HA: request human alignment summary
    HA->>DB: aggregate opens, acks, ignores, and noisy patterns
    HA-->>API: recommendations and summaries
    API-->>UI: human-alignment panel payload
    UI-->>User: show noisy rules and review suggestions
```

### Human explanation

This loop is deliberately passive. It studies operator behavior and recommends reviews, but it does not automatically retune alert thresholds.

## 5. Execution Advisory Flow

This is the advisory-only sidecar path.

```mermaid
sequenceDiagram
    participant Apply as broker_apply_orders.py
    participant EPE as execution_policy_engine.py
    participant Hist as execution analytics and fills
    participant Adv as execution_ai_advisor.py
    participant DB as execution_ai_advisory
    participant UI as Dashboard UI
    participant User as Operator
    participant Act as execution_ai_advisory_actions

    Apply->>EPE: shape intended orders
    EPE-->>Apply: shaped intents
    Apply->>Adv: persist advisories for shaped intents
    Adv->>Hist: inspect recent slippage and latency evidence
    Adv->>DB: write advisory rows
    UI->>DB: request advisory list and details via API
    DB-->>UI: advisory payloads
    UI-->>User: show recommendation and evidence
    User->>UI: approve or reject advisory
    UI->>Act: write action through API
```

### Human explanation

The advisory system is attached to execution but does not control execution. It watches what the system intends to do, checks recent realized execution history, and produces an auditable recommendation for the operator.

## 6. Governance And Promotion Flow

This is the main oversight path for model-governance state.

```mermaid
sequenceDiagram
    participant Jobs as Strategy or training jobs
    participant Reg as model_registry and related tables
    participant GovJob as strategy_governance_job.py
    participant GovExt as model_governance_ext.py
    participant GovLog as model_governance_log
    participant API as api_governance.py
    participant UI as Dashboard UI

    Jobs->>Reg: write model, shadow, and promotion-related state
    GovJob->>Reg: inspect promotion, replay, critic, and shadow signals
    GovJob->>GovExt: build governance summary inputs
    GovExt->>GovLog: write governance snapshot
    API->>GovExt: request governance summary
    GovExt-->>API: summary payload
    API-->>UI: governance panel payload
```

### Human explanation

Governance in this repo is not a single yes-no switch. It is a summary built from promotion status, replay freshness, critic signals, challenger/champion context, and shadow evidence. The dashboard now exposes that as a coherent view.

## 7. Champion-And-Challenger Flow

This is the live model-selection path.

```mermaid
sequenceDiagram
    participant Train as Training jobs
    participant Reg as model_registry and lifecycle tables
    participant CR as challenger_runtime.py
    participant MP as model_marketplace.py
    participant CM as champion_manager.py
    participant Assign as champion_assignments
    participant Pred as predictor.py

    Train->>Reg: register trained model version and feature contract
    CR->>MP: record shadow orders and shadow observations
    MP->>MP: score challengers, replay validation, self-critic checks
    CM->>MP: read ranked challengers and safety evidence
    CM->>Assign: keep, promote, or demote champion assignment
    CM->>Reg: sync champion state to durable registry metadata
    Pred->>Assign: resolve current champion for symbol/horizon
    Pred->>Reg: load feature schema and model metadata
    Pred-->>Pred: serve live inference with the selected champion
```

### Human explanation

The champion/challenger system is not just "pick the highest score." A challenger first proves itself in shadow mode, then replay and self-critic gates can still block it, and only then can the champion manager assign it as live.

## 8. Database-Centered View

This is the same system seen through write order instead of code modules.

```mermaid
sequenceDiagram
    participant Ingest as Ingestion jobs
    participant DB as trading.db
    participant Model as Strategy and prediction jobs
    participant Port as Portfolio logic
    participant Exec as Execution logic
    participant UI as Dashboard and operator actions

    Ingest->>DB: write prices, events, features, and buffered provider health telemetry
    Model->>DB: write predictions, labels, decisions, model state
    Port->>DB: write portfolio state and portfolio orders
    Exec->>DB: write execution orders, fills, policy audits, advisories
    UI->>DB: read current state through APIs
    UI->>DB: write acknowledgements and interaction logs through APIs
```

## 9. How To Use These Diagrams

### If you are debugging startup

Use:

- Startup Flow

### If you are debugging why a trade happened

Use:

- Decision-To-Order Flow
- Database-Centered View

### If you are debugging operator UI behavior

Use:

- Alert-To-Dashboard Flow
- Human-Alignment Analytics Flow

### If you are debugging model oversight or promotion issues

Use:

- Governance And Promotion Flow
- Champion-And-Challenger Flow

## 10. Short Summary

If you need the shortest explanation:

> The repo runs as a supervised pipeline: startup brings up orchestration and APIs, ingestion fills the database, strategy jobs create decisions, portfolio logic converts them into orders, execution logic shapes and routes those orders, and the dashboard exposes state plus newer oversight layers for decisions, human alignment, execution advisories, and governance.
