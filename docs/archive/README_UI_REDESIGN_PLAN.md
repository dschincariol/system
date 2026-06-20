# Trading System UI Redesign Plan

This document turns the repo audit into a concrete UI redesign plan.

It is intentionally tied to the current codebase:

- main dashboard: `ui/dashboard.html`, `ui/dashboard.js`
- operator console: `boot/operator_ui.html`, `boot/operator_server.js`
- trading terminal: `ui/terminal/terminal.html`, `ui/terminal/terminal.js`
- HTTP/API boundary: `dashboard_server.py`, `engine/api/`

The goal is not to invent a new product. The goal is to make the current product easier to operate.

## 1. Current Problem Statement

The repo already has strong runtime and API coverage, but the UX has drifted into three overlapping surfaces:

1. `ui/dashboard.html`
   broad monitoring, governance, jobs, portfolio, model, and alert views
2. `boot/operator_ui.html`
   startup, readiness, service control, repair, logs, and operator workflows
3. `ui/terminal/terminal.html`
   chart, watchlist, positions, fills, orders, and order entry

This creates three practical issues:

- operators need to page-hop for one workflow
- the main dashboard shows too many panels with weak prioritization
- some backend features are implemented but only partially surfaced in the UI

## 2. Product Direction

Adopt one primary surface for daily supervision:

- `ui/dashboard.html` becomes the main operations home

Keep two secondary surfaces:

- `ui/terminal/terminal.html` stays focused on execution and market interaction
- `boot/operator_ui.html` becomes an installation, recovery, and appliance-admin console

In short:

- dashboard = operate the system
- terminal = inspect and act on trading state
- operator console = recover or maintain the appliance

## 3. Target Information Architecture

The redesigned dashboard should be organized by operator workflow, not by subsystem ownership.

### A. Overview

Purpose:

- answer "is the system safe, live, and doing the right thing right now?"

Content:

- system state
- execution barrier / trading readiness
- alert summary
- current market stress
- current PnL
- top decisions
- operator headline / next actions

Primary files:

- `ui/dashboard.html`
- `ui/dashboard.js`
- `ui/operator_summary.js`
- `ui/system_state.js`
- `ui/telemetry_panel.js`

### B. Operate

Purpose:

- let operators intervene safely

Content:

- jobs and critical daemons
- feed restart / pipeline actions
- kill switches and read-only state
- execution advisories
- startup/readiness checklist

Primary files:

- `ui/dashboard.html`
- `ui/dashboard.js`
- `ui/kill_switch_ui.js`
- `ui/read_only_mode.js`
- `boot/operator_server.js`

### C. Explain

Purpose:

- explain why the system is acting or not acting

Content:

- recent decisions
- decision detail modal
- governance summary
- promotion blockers
- active strategy summary
- human-alignment signals

Primary files:

- `ui/dashboard.js`
- `ui/decision_bar.js`
- `ui/why_modal.js`
- `ui/promotion_safety.js`

### D. Analyze

Purpose:

- deeper investigation for advanced users

Content:

- model metrics
- temporal eval
- portfolio backtest
- calibration
- relevance stats
- job history
- crash analytics

Primary files:

- `ui/dashboard.html`
- `ui/dashboard.js`

## 4. Dashboard Layout Proposal

Replace the current long card wall with a layered layout.

### Header band

Keep:

- system mode
- execution/trading allowed state
- updated timestamp
- expert unlock state

Add:

- one clear "system safe / degraded / blocked" status
- one clear "what changed since last refresh" summary

### First row: operator-critical

Panels:

- Overview summary
- Alerts summary
- Trading readiness / execution barrier
- Live PnL
- Market stress

Rule:

- this row should fit without deep scrolling on a normal laptop

### Second row: current actions

Panels:

- Recent decisions
- Execution advisories
- Jobs / feeds
- Startup and readiness checklist

### Third row: explain and governance

Panels:

- Governance
- Promotions safety
- Active strategy
- Portfolio state

### Advanced section

Collapsed by default:

- calibration
- temporal shadow gates
- model metrics
- relevance stats
- job history
- crash analytics
- broker raw snapshots

## 5. Surface Ownership Changes

### Main dashboard

Should own:

- real-time supervision
- safe operational actions
- decision explainability
- execution risk awareness

Should not own:

- appliance configuration editing
- secrets management
- backup/update control
- raw repair workflow internals

Those belong in the operator console.

### Operator console

Should own:

- install/bootstrap
- config and secrets
- service management
- snapshot export
- AI repair, patch preview/apply/rollback
- backup/update/restart-operator workflows

Should not duplicate:

- portfolio monitoring
- decision review
- long-lived daily runtime supervision

### Trading terminal

Should own:

- chart-centric workflow
- watchlist
- positions
- orders
- fills
- order entry / flatten

Should gain:

- direct link back to a decision or advisory for the current symbol
- concise risk banner if execution barrier blocks live action

## 6. Specific Problems To Fix

### Problem 1: Too many first-class panels

Current effect:

- important states compete with research and diagnostics

Fix:

- move advanced panels into tabs or an "Advanced Analysis" section

### Problem 2: Inconsistent route naming

Current effect:

- legacy and newer route styles both exist
- browser code has higher coupling to server internals

Fix:

- standardize UI calls around grouped routes:
  - `/api/system/...`
  - `/api/execution/...`
  - `/api/operator/...`
  - `/api/portfolio/...`
  - `/api/models/...`

Compatibility can remain in `dashboard_server.py` during migration.

### Problem 3: Partial social/weather integration

Current effect:

- loader modules exist
- dashboard JS still calls them
- dashboard HTML does not expose matching sections

Fix:

Choose one:

1. fully surface social + weather in an "Alternative Signals" analysis section
2. remove those dashboard calls until the UI exists

Recommendation:

- keep them, but demote them to the advanced analysis area

### Problem 4: Split startup/readiness experience

Current effect:

- startup and readiness are clearer in `boot/operator_ui.html` than in the main dashboard

Fix:

- move a compact version of operator startup/readiness into the main dashboard
- keep full bootstrap admin workflow in operator console

### Problem 5: Weak cross-linking between surfaces

Current effect:

- dashboard, terminal, and operator panel feel separate

Fix:

- add persistent links between all three
- carry context when possible:
  - symbol
  - mode
  - selected decision
  - alert id

## 7. Recommended Component Refactor

The current `ui/dashboard.js` is doing too much orchestration and feature rendering.

Refactor toward these browser modules:

- `ui/views/overview_view.js`
- `ui/views/operate_view.js`
- `ui/views/explain_view.js`
- `ui/views/analyze_view.js`
- `ui/state/dashboard_store.js`
- `ui/api/client.js`

Recommended moves:

- centralize fetch/retry/error handling into one API client
- centralize shared dashboard state into one store
- convert panel loaders into view-level modules
- keep existing small renderer modules where useful

Do not rewrite everything at once.

## 8. API Cleanup Plan

### Phase target

UI code should call grouped endpoints, not ad hoc legacy names.

Examples:

- replace `/api/execution_metrics/by_confidence`
  with `/api/execution/metrics/by_confidence`
- replace `/api/temporal_models`
  with `/api/temporal/models`
- replace `/api/model_registry`
  with `/api/model/registry` or `/api/models/status`

### Rule

During migration:

- preserve old routes as compatibility aliases
- move new UI code to grouped routes only

## 9. Phased Implementation Plan

### Phase 1: Navigation and hierarchy cleanup

Scope:

- no major backend change
- mostly HTML/CSS/JS composition work

Changes:

- reorganize `ui/dashboard.html` into overview, operate, explain, analyze sections
- demote advanced cards below the fold
- add persistent links between dashboard, terminal, and operator console
- add compact readiness/startup panel into dashboard top area

Success criteria:

- operator can answer health, trading state, alerts, and decisions from the first screen

### Phase 2: Module split and API client cleanup

Scope:

- improve maintainability

Changes:

- extract fetch helpers from `ui/dashboard.js`
- extract view modules
- normalize shared state
- switch UI calls toward grouped API names

Success criteria:

- `ui/dashboard.js` becomes orchestration-only, not a monolith

### Phase 3: Cross-surface linking

Scope:

- better workflows across dashboard, operator console, and terminal

Changes:

- symbol-aware links to terminal
- alert-to-decision and decision-to-terminal jump actions
- advisory detail links to trading context

Success criteria:

- one issue can be traced from alert -> decision -> execution -> repair path without hunting

### Phase 4: Advanced analysis consolidation

Scope:

- polish lower-priority features already implemented

Changes:

- create one advanced analysis area
- properly surface social/weather if retained
- unify research and model diagnostics presentation

Success criteria:

- advanced features are discoverable without overwhelming daily operators

## 10. File-Level Change Map

### `ui/dashboard.html`

Change:

- restructure markup into workflow sections
- reduce visible-at-once card count
- add compact cross-links

### `ui/dashboard.js`

Change:

- remove direct ownership of every panel
- shift toward section orchestration

### `ui/dashboard_theme.css`

Change:

- strengthen hierarchy
- define section-level spacing, density, and panel priorities

### `boot/operator_ui.html`

Change:

- narrow purpose to recovery/admin workflows
- remove or de-emphasize duplicated daily monitoring content

### `ui/terminal/terminal.html`

Change:

- add contextual return path to dashboard decision/advisory views

### `dashboard_server.py`

Change:

- keep compatibility routes temporarily
- reduce long-term ownership of UI-specific route sprawl

### `engine/api/`

Change:

- continue consolidating canonical grouped route definitions

## 11. Highest-Priority Wins

If only a small amount of work is funded, do these first:

1. Reorganize the dashboard into Overview, Operate, Explain, Analyze.
2. Bring startup/readiness summary into the dashboard.
3. Push advanced diagnostics below the fold.
4. Add strong links between dashboard, terminal, and operator console.
5. Remove or finish dead social/weather integrations.

## 12. Non-Goals

This redesign does not require:

- changing trading logic
- changing DB schema
- removing existing operator safeguards
- merging every page into one file

The main objective is better operator comprehension and lower workflow friction.
