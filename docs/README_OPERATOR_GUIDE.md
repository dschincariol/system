# Trading System Operator Guide

This document explains the repo from the point of view of someone running or supervising the system.

It answers:

- what the operator is looking at
- what the main panels mean
- what the system does during normal runtime
- where the operator has oversight versus control

## 1. What An Operator Is Operating

The operator is not just watching a chart.

The operator is supervising a live runtime made of:

- ingestion jobs
- model and strategy jobs
- portfolio and execution logic
- health, alerts, and kill-switch surfaces
- dashboard and job-control APIs

In short, the operator is supervising an automated workflow, not manually trading from scratch.

## 2. Main Entrypoints

| Entrypoint | What it is for |
| --- | --- |
| `start_system.py` | Starts the main supervised runtime |
| `dashboard_server.py` | Serves the dashboard UI and API |
| `start_ingestion.py` | Starts the ingestion-focused runtime path |
| `boot/operator_server.js` | Local operator-side launcher/service |
| `ui/dashboard.html` | Main browser dashboard |
| `ui/data_sources.html` | Single-page Data Sources Control Center for provider setup, credential resets, tests, and enable/disable actions |
| `services/operator_ai/agent.js` | Bounded AI analysis layer for operator diagnostics and guarded repair flows |

## 3. What The Dashboard Is Meant To Show

The dashboard is the operator's control tower.

It is there to answer:

- Is the runtime healthy?
- Are data feeds alive?
- Are jobs running?
- Is the system producing alerts?
- What decisions is the system making?
- What execution issues or risks are emerging?
- Are governance or safety checks blocking something?

## 4. Operator Mental Model

Use this mental model:

1. Data enters the system.
2. Jobs process those data.
3. Models and policies produce decisions.
4. Portfolio logic converts them into position changes.
5. Execution logic determines how orders should be carried out.
6. The dashboard shows evidence, health, and exceptions.
7. The operator intervenes only when something needs review, approval, or containment.

## 5. Key Dashboard Areas

The exact UI may evolve, but the important panels now include the following.

| Panel | Meaning | What the operator should look for |
| --- | --- | --- |
| Health/System | Runtime, database, and service health | errors, failed checks, stale state |
| Readiness Evidence | Consolidated live/paper readiness evidence | `BLOCKED`, `WARN`, or `UNAVAILABLE` rows, owner subsystem, age, and remediation |
| Jobs | Background processes and job history | dead jobs, repeated restarts, missing feeds |
| Alerts | Operational and model-related warnings | noisy rules, persistent incidents, unresolved problems |
| Decisions | Recent system decisions and their explanations | unexpected actions, weak rationale, abnormal certainty |
| Human Alignment | Summary of how operators interact with alerts | alerts that are opened often but rarely acknowledged |
| Execution Advisories | Non-binding advice about execution quality or urgency | slippage risk, elevated latency, routing concerns |
| Governance | Model/promotion/safety state | stale replay, blocked promotion, critic failures |
| Portfolio/Execution panels | Current exposure and execution condition | concentration, exposure drift, execution degradation |

## 5.1 Where To Manage Data Sources

Use one page only for provider and feed setup:

- `http://127.0.0.1:8000/ui/data_sources.html`

That page is the single source of truth for:

- entering or replacing provider credentials
- resetting corrupted stored credentials
- enabling or disabling sources
- testing connections
- reading plain-language setup instructions and recommended next actions

Operators should not need to edit `.env` for provider credentials and should not need to bounce between the dashboard site and the operator site for this functional area.

## 6. Operator-Facing Features

### Decisions UI

The dashboard now shows recent decisions in a human-readable way.

That gives the operator:

- a list of recent decision records
- a detail drilldown
- a short explanation of why the system moved in that direction
- visibility into model/risk context when available

### Human Alignment Analytics

The system now tracks how operators interact with alerts and decisions.

This is passive analytics only. It is meant to answer:

- Which alerts are useful?
- Which alerts are ignored or repeatedly opened?
- Which rules are noisy?

It recommends reviews. It does not auto-change thresholds.

### Execution Advisory Panel

The system now records non-authoritative execution recommendations.

These advisories can include:

- expected slippage
- urgency suggestions
- recent historical execution evidence
- approval or rejection actions for audit purposes

These advisories do not place or block trades on their own.

### Readiness Evidence Panel

The dashboard includes a consolidated Readiness Evidence card backed by `GET /api/operator/readiness_evidence`.

Use it when a live, paper, or broker-facing action is blocked and the reason is not obvious from one panel. Each row shows a text status token (`PASS`, `WARN`, `BLOCKED`, or `UNAVAILABLE`), the owning subsystem, source route or config key, last update age, and remediation.

Live/paper critical evidence fails closed. Missing or stale critical evidence is shown as blocked or unavailable, not as passing. Broker activation reads this same evidence route before posting the activation request; the backend still independently enforces a fresh passing broker connection test and runtime/execution gates still decide whether trading can occur.

### Operator AI Diagnostic And Patch Flow

The operator surface now includes a bounded AI repair path.

What it does:

- reads service status, health, runtime logs, support snapshot, provider telemetry, watchdogs, and execution barrier data
- returns strict JSON analysis with summary, root cause, failing component, file hint, patch hint, and `action: null`
- logs analyses to `var/log/ai_operator_log.jsonl` locally, or to the configured runtime/operator-AI log path
- supports patch preview, apply, and rollback workflows from the operator server

What it does not do:

- invent arbitrary actions
- execute runtime-control actions from `services/operator_ai/agent.js`
- place trades
- bypass runtime execution gates
- apply patches in live mode

### Structured Confirmations

High-impact operator actions use structured confirmation instead of native
browser prompts. Live start, guided bootstrap, stop/restart, emergency stop,
factory reset, repair/admin actions, secret changes, feed restarts, and
operator-AI patch apply/rollback ask for a typed phrase, consequence
acknowledgement, and a reason or hold period where the server contract requires
one.

The modal is not the authority. Direct calls to the operator sidecar or
dashboard API must still include the structured confirmation fields
(`action_id`, confirmation token, acknowledgement, actor, source surface,
request id, target, reason, and hold metadata where required), and the server
rejects missing or invalid confirmation before running the mutation. Audit
records keep the action, actor, source, request id, target, confirmation method,
hold duration, and reason so incident review can reconstruct what was approved.

### Model-directed decisioning

The runtime now supports model-owned trading intent more directly.

Operators should expect recent decision records to increasingly include:

- canonical `model_intent`
- `feature_ids`
- `feature_set_tag`
- named `feature_snapshot` values

In plain English, this means the runtime can now show not just a score, but also:

- which symbols the model wanted to include
- which side it wanted
- whether it wanted to trade now
- what relative sizing it requested
- which feature schema was actually used at inference time

## 7. What The Operator Can Influence

The operator mainly has oversight and control-surface power, not model-authoring power during routine runtime.

Typical operator influence includes:

- reviewing alerts
- checking system health
- discovering jobs through the Job Catalog and starting or stopping jobs if the backend catalog marks the action available
- reading governance and decision state
- acknowledging or rejecting advisory items
- requesting AI diagnosis or guarded patch preview/apply flows when the runtime is degraded
- using kill-switch or read-only controls when needed
- confirming high-impact mutations through the structured confirmation flow, with
  a reason that is meaningful for later audit review

## 8. What The Operator Should Not Assume

- A dashboard panel is not necessarily the source of truth.
  The source of truth is usually runtime state plus Postgres-backed runtime storage. SQLite files are test or legacy-compatibility artifacts unless a specific test run opted into `TS_STORAGE_BACKEND=sqlite`.
- Job safety is not decided by browser text matching.
  The dashboard and command palette consume the backend job catalog's `safety`, prerequisite, and `action_policy` fields from `/api/jobs` or `/api/jobs/catalog`; guarded starts still require server-side confirmation.
- A confirmation modal is not an authorization bypass.
  Browser confirmation helps the operator provide the required evidence, but the
  sidecar/API route must validate the same payload before dangerous mutations
  run.
- A live runtime does not fall back to SQLite when Postgres is down.
  Treat storage readiness failures, `database_reachable` blockers, and schema-validation blockers as fail-closed conditions.
- An advisory is not an execution command.
  Advisory means informational unless explicitly wired otherwise.
- A quiet dashboard does not always mean a healthy system.
  Feed staleness and silent job failure still need health checks.

## 8.1 Offline Research And Training

Run research, TSFresh snapshot materialization, Optuna tuning, and model
training in the offline profile, not in the live runtime. The live profile is
for ingestion, scoring, execution safety, Timescale, Redis, and operator
control paths; it defaults to `ALLOW_TRAINING=0`, serial model workers,
`TSFRESH_N_JOBS=0`, small TSFresh symbol/batch caps, and low background
concurrency.

Use `deploy/compose/docker-compose.stack.yml` with `--profile offline` and an
isolated `OFFLINE_TS_PG_DSN` that points at a restored clone or other offline
datastore. The `offline-worker` service sets `RUNTIME_WORKLOAD_PROFILE=offline`
and uses the `OFFLINE_*` CPU, memory, model `n_jobs`, TSFresh, and tuning
trial settings.

Before and during offline jobs, verify live isolation:

```bash
python -m engine.runtime.prod_preflight --json
docker compose --env-file deploy/compose/.env -f deploy/compose/docker-compose.stack.yml ps
docker stats --no-stream runtime timescaledb redis offline-worker
```

Expected state: preflight reports `workload_profile=live` and
`allow_training=0` for the live runtime; `docker stats` shows runtime,
Timescale, and Redis inside their configured limits; offline jobs report
`RUNTIME_WORKLOAD_PROFILE=offline`. If live latency, feed freshness, or DB
headroom degrades, stop or downsize the offline job instead of borrowing from
live service budgets.

Enabling training or heavy research flags in a live profile requires the exact
`OFFLINE_TRAINING_LIVE_PROFILE_ACK=I_UNDERSTAND_OFFLINE_TRAINING_IN_LIVE_PROFILE`
phrase plus non-placeholder owner and reason. Production preflight and job
launch fail closed without that acknowledgement.

## 9. Common Operator Questions

### "Is the system running?"

Check:

- runtime health
- dashboard responsiveness
- job list
- database health
- recent event/error surfaces

### "Is the system making decisions?"

Check:

- recent decision records
- model/prediction outputs
- portfolio activity
- governance blocks

### "Why is the system not trading?"

Common reasons include:

- no signal
- policy/risk gate blocked the move
- governance blocked promotion or use
- execution mode is restrictive
- kill switch is active
- `DISABLE_LIVE_EXECUTION` is truthy in the process environment
- an operator/manual emergency hold is active and must be cleared through the explicit manual-halt workflow

Model-level execution kill switch is also active through the same execution gate.
It is additive to the global kill switch and blocks only the affected model's orders.
- feeds are stale or jobs are down
- `model_intent.should_trade` or timing suppressed entry

Even when the model does want to trade, the runtime can still block or compress the action through risk, execution, or kill-switch controls.

### Automatic vs manual halt ownership

Rules-engine halts are automatic DB rows owned by `actor=rules_engine` with `meta_json.trigger` set to `drawdown`, `drift`, `exec_winrate`, or `cost_spike`. If `RULES_AUTO_RESUME=1` is explicitly enabled, the rules engine may clear only those matching rows after the condition normalizes.

Operator, manual, emergency-stop, startup-gate, preflight, and break-glass holds are separate capital-safety holds. Automatic rules recovery must not clear them. Use `POST /api/operator/clear_manual_halt` with `CLEAR_MANUAL_HALT`, `consequence_ack`, `actor`, `source`, and `reason` to clear a manual DB hold after incident review.

### "Why is the system trading strangely?"

Check:

- decision explanations
- confidence levels
- allocation pressure
- governance summary
- execution advisory evidence
- whether the model switched feature schema or promoted different symbols into the universe

Recent decision logs and explain payloads are now the best place to verify that the served model contract matches what training registered.

## 10.1 Operator-visible model contract

The operator does not manage model internals during normal runtime, but the following concepts now matter operationally:

- `model_intent`
  the model's requested trade/universe decision
- `feature_ids`
  the exact features the served model expects
- `feature_set_tag`
  a readable identifier for the active feature layout
- `feature_snapshot`
  the named values actually computed for that decision

If these fields are missing unexpectedly for a promoted model, that usually indicates a stale legacy path, a registration problem, or a model that was promoted without a full schema contract.

## 10. Operator Workflow During An Incident

Use this sequence:

1. Confirm whether the problem is data, jobs, strategy, execution, or UI-only.
2. Check health and recent errors.
3. Check whether feeds and ingestion jobs are alive.
4. Check whether decisions are still being produced.
5. Check whether governance or execution safety is blocking action.
6. Use operator controls only after identifying the layer that is failing.

## 11. Operator Workflow During Normal Supervision

During normal operation, the operator should mostly watch for:

- feed freshness
- repeated alert noise
- unexpected decisions
- elevated execution slippage
- governance or replay drift
- concentration or execution degradation signals

## 12. Operator Boundaries

The current integrated design intentionally keeps some features non-automatic:

- human-alignment recommendations are not auto-applied
- execution AI is advisory only
- governance surfaces improve visibility before they change policy

That is by design. The system is optimized for supervised automation rather than blind automation.

The same boundary applies to AI-driven model behavior:

- the model can recommend symbol selection, timing, and sizing
- the system still enforces portfolio risk, execution policy, and safety controls

## 13. One-Paragraph Operator Summary

If you need a short plain-English description:

> The operator dashboard is the control tower for a supervised trading runtime. It shows whether data is flowing, jobs are healthy, decisions are being made, execution conditions are acceptable, and governance or safety systems are blocking anything. The operator mainly monitors, validates, and intervenes when the system behaves unexpectedly or enters a degraded state.
