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

The dashboard opens on a mission-control command surface. The sticky mission
bar is the first operational read: it answers whether the system can trade,
whether it should trade, what changed most recently, and what the operator
should do next. The supporting Overview, Operator Guidance, KPI, health-score,
and readiness-evidence panels remain read-only advisory surfaces; backend
readiness, execution-barrier, broker, and confirmation APIs remain the actual
authority for trading and high-impact actions.

The documented one-command launcher path is `./start_all.sh` to
`start_all.py` to `boot/operator_server.js`. When the operator starts the
engine, it passes runtime DSN/config values such as `TS_PG_DSN`, `TS_PG_PORT`,
`DB_PATH`, `TRADING_DATA`, and `TRADING_LOGS`, plus secret-file pointers such as
`TS_PG_PASSWORD_FILE`, `TIMESCALE_PASSWORD_FILE`,
`DATA_SOURCE_MASTER_KEY_FILE`, `REDIS_PASSWORD_FILE`, and
`OBJECT_STORE_*_FILE` to every engine bootstrap subprocess and the final
`start_system.py` child. Inline password/master-key variables such as
`TS_PG_PASSWORD` and `TIMESCALE_PASSWORD` are stripped from those child
environments; use `*_FILE` or provider-backed secret references instead.
The operator-side `.env` loader follows `python-dotenv` quote and inline-comment
semantics before spawning the engine: one matching pair of surrounding single or
double quotes is stripped, quoted `#` and `=` characters remain data, and
unquoted comments begin only at whitespace followed by `#`. Keep DSN-style
values unquoted in checked-in examples and local `.env` files unless quoting is
required to preserve leading/trailing whitespace, newlines, or literal
whitespace-`#` content.

### Local Safe/Sim Boot Smoke

Use the safe/sim smoke before any execution testing on a clean local checkout:

```bash
python tools/safe_sim_boot_smoke.py
```

The smoke derives a temporary env from `.env.codex-sim-paper.bak`, writes
secret-shaped values to local `*_FILE` references under `var/tmp/safe_sim_boot`,
forces `ENGINE_MODE=safe`, `EXECUTION_MODE=safe`,
`DISABLE_LIVE_EXECUTION=1`, `KILL_SWITCH_GLOBAL=1`,
`LIVE_TRADING_CONFIRM=`, `LIVE_TRADING_REQUIRE_CONFIRMATION=1`, and
`BROKER/BROKER_NAME=sim`. For reproducibility it also forces local dev storage
with `TS_STORAGE_BACKEND=sqlite`, `PRICE_READ_BACKEND=sqlite`, and
`TELEMETRY_READ_BACKEND=sqlite`, so the smoke does not depend on an already
running Docker/Postgres stack. It then starts `start_system.py` and the operator
sidecar. It checks `/api/health`, `/api/system/kill_switches`,
`/api/broker/config`, `/api/execution/barrier`, and
`/api/operator/readiness_evidence` through the dashboard with `X-API-Token`
read from `DASHBOARD_API_TOKEN_FILE`. It also checks the same safety gates
through direct `:4001` operator proxy routes with `X-Operator-Token` read from
`OPERATOR_API_TOKEN_FILE`. In this safe/sim profile the execution barrier is
expected to return HTTP 200 with a populated payload and `allowed=false`; that
is the proof that execution is blocked. The script reports only token source
metadata, never token values, and shuts the processes down before verifying that
`:8000` and `:4001` are closed.

To generate the sanitized env without starting processes:

```bash
python tools/safe_sim_boot_smoke.py --prepare-only
TRADING_ENV_FILE=var/tmp/safe_sim_boot/.env.safe-sim ./start_local.sh
```

### Local Paper/Sim Fill Profile

Use `.env.codex-sim-paper-fills.bak` only when you need to exercise the
end-to-end simulated-fill path:

```bash
set -a
. ./.env.codex-sim-paper-fills.bak
set +a
python - <<'PY'
from engine.runtime.storage import init_db
from engine.cache.wrappers.execution_mode import set_execution_mode

init_db()
set_execution_mode("paper", actor="operator", reason="paper_sim_fill_profile")
PY
python -c "from engine.execution.broker_router import effective_broker_chain; assert effective_broker_chain() == ['sim']"
TRADING_ENV_FILE=.env.codex-sim-paper-fills.bak ./start_local.sh
```

This profile is simulation-only. It sets `ENGINE_MODE=paper`,
`EXECUTION_MODE=paper`, `BROKER=sim`, `BROKER_NAME=sim`, and
`BROKER_FAILOVER=sim`. It also keeps `DISABLE_LIVE_EXECUTION=1`, so live
capital cannot be armed or routed. `KILL_SWITCH_GLOBAL=0` lets the simulator run;
setting it to `1` freezes every order path, including paper simulation.

To drive one simulated fill through the dashboard/API, include an explicit
terminal confirmation payload. The browser terminal collects the same `TRADE`
token, consequence acknowledgement, and short hold through the shared modal
before it sends any BUY/SELL request; it does not synthesize those fields.

```bash
curl -sS -X POST http://127.0.0.1:8000/api/terminal/order \
  -H "Content-Type: application/json" \
  -H "X-API-Token: $(cat data/secrets/dashboard_api_token)" \
  -d '{
    "symbol":"AAPL",
    "side":"BUY",
    "qty":1,
    "confirm":"TRADE",
    "confirmation":"TRADE",
    "confirmation_token":"TRADE",
    "confirmation_method":"typed_phrase",
    "confirmation_hold_ms":0,
    "consequence_ack":true,
    "actor":"terminal_operator",
    "source":"terminal",
    "source_surface":"terminal",
    "action_id":"terminal.order",
    "target":"BUY AAPL qty 1.0000"
  }'
python engine/execution/jobs/broker_apply_orders.py
python engine/execution/jobs/execution_poll_and_attrib.py
```

Terminal order intents are recorded as explicit quantity orders with portfolio
sides (`FLAT` to `LONG`/`SHORT`) and a `source_alert_id` lineage value; when the
caller does not provide one, the terminal route uses the intent timestamp so
execution fills can be attributed.

Then confirm `/api/execution/barrier` reports `mode=paper`,
`allow_simulation=true`, and `real_trading_allowed=false`, and inspect the
dashboard execution/attribution panels for the simulated fill. The broker router
enforces the same contract in production code: when mode is paper, any live
broker in `BROKER`, `BROKER_NAME`, `LIVE_BROKER`, `INTENDED_LIVE_BROKER`, or
`BROKER_FAILOVER` fails before a live adapter can be resolved.

`tests/test_paper_mode_sim_fill_boot.py` is the boot-level regression for this
profile. It launches `start_system.py paper`, posts a terminal order through the
dashboard API, runs `broker_apply_orders` and `execution_poll_and_attrib`, and
asserts the simulated fill plus attribution rows persist while the barrier stays
paper-only (`allow_simulation=true`, `real_trading_allowed=false`) and shutdown
finishes within the bounded startup/shutdown path.

### Live Options With Tradier

Live option order routing is opt-in and remains shadow-only by default. The only
registered live options order broker is `tradier_options`; equity Alpaca and
IBKR routes still reject option contracts. A live options run must set
`BROKER=tradier_options`, `BROKER_NAME=tradier_options`,
`LIVE_BROKER=tradier_options`, and `BROKER_FAILOVER=tradier_options`, with
`ENGINE_MODE=live`, `EXECUTION_MODE=live`, and `OPTIONS_INSTRUMENTS_MODE=live`.

Every options readiness env gate must be enabled, every numeric options control
must be configured in range, and the runtime checks behind those gates must pass:
options data quality, bid/ask quality, portfolio greeks and margin/position
limits, lifecycle readiness, broker adapter importability, and kill-switch
`execution_allowed`. Tradier submission also requires `TRADIER_API_TOKEN` and
`TRADIER_ACCOUNT_ID`. Missing env gates produce the existing `*_missing`
readiness blockers; enabled gates whose runtime check fails produce
`*_check_failed` blockers. Do not treat a configured Tradier token as live
permission by itself.

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
| Operational KPI Summary | Compact runtime, readiness, execution, data, and alert state at the top of the dashboard | symbol-coded `OK` / `!` / `X` / `LOCK` / `?` states, stale or partial coverage, blocked execution |
| Operator Guidance | Numbered next steps derived from runtime blockers, readiness, decisions, and advisories | stop/hold guidance during blocked or degraded states; guarded action guidance only when readiness and execution are clear |
| Health/System | Runtime, database, and service health | errors, failed checks, stale state |
| Readiness Evidence | Consolidated live/paper readiness evidence | `BLOCKED`, `WARN`, or `UNAVAILABLE` rows, owner subsystem, age, and remediation |
| Data Sources Action Center | Feed credential, runtime, and readiness priorities from the canonical data-source page | urgent missing credentials, failed providers, degraded feeds, source cards that need testing or recovery |
| Jobs | Background processes and job history | dead jobs, repeated restarts, missing feeds |
| Alerts | Grouped alarm and notification lifecycle surface | open WARN/HIGH/CRIT alarms first, INFO notifications second; acknowledged/shelved items that remain unresolved; stale, suppressed, and grouped flood counts |
| Decisions | Recent system decisions and their explanations | unexpected actions, weak rationale, abnormal certainty |
| Human Alignment | Summary of how operators interact with alerts | alerts that are opened often but rarely acknowledged |
| Execution Advisories | Non-binding advice about execution quality or urgency | slippage risk, elevated latency, routing concerns |
| Governance | Model/promotion/safety state | stale replay, blocked promotion, critic failures |
| Portfolio/Execution panels | Current exposure and execution condition | concentration, exposure drift, execution degradation |

> **Accessing the UI from another computer (LAN):** by default the dashboard
> (:8000) binds to loopback and the operator sidecar (:4001) remains
> loopback/internal. To open the UI from another machine on a trusted LAN (e.g.
> a Windows desktop at `http://192.168.0.165:8000`), follow
> [LAN_ACCESS.md](LAN_ACCESS.md) — set `TRADING_NETWORK_MODE=lan` plus a
> `DASHBOARD_API_TOKEN`, open only dashboard port `:8000`, and use
> `/operator/` through the dashboard for operator workflows. Do not use
> NoMachine for normal UI viewing.

## 5.1 Where To Manage Data Sources

Use one page only for provider and feed setup (when viewing over the LAN,
substitute the host with your server's LAN IP, e.g. `http://192.168.0.165:8000`):

- `http://127.0.0.1:8000/ui/data_sources.html`

That page is the single source of truth for:

- entering or replacing provider credentials
- resetting corrupted stored credentials
- enabling or disabling sources
- testing connections
- reading plain-language setup instructions and recommended next actions

The credential-entry workflow is backed by first-class dashboard routes: Save
uses `POST /api/data_sources/update`, Test Connection uses
`POST /api/data_sources/test`, Test & Save uses
`POST /api/data_sources/test_save`, Populate Now uses
`POST /api/data_sources/populate_now`, and shared provider-account credentials
use `POST /api/data_sources/accounts/update`. Dashboard boot and UI contract
validation fail if any advertised route is missing its registered handler.

Operators should not need to edit `.env` for provider credentials and should not need to bounce between the dashboard site and the operator site for this functional area.

### FX Operator Surfaces

FX surfacing is read-only in the browser. The UI displays data that upstream FX workstreams have already placed in existing read-model payloads; it does not create FX prices, positions, sleeve exposure, leverage, or order authority.

- Data Sources: FX or OANDA-style feeds are marked with an `FX feed` badge. Use the existing Test Connection action to check connectivity. The test-result panel displays only status, ok, latency, detail, and message fields, never credential values.
- Positions & Exposure: the dashboard shows FX position count, sleeve gross/net exposure, effective leverage, and lot/unit sizing when those fields are present in `/api/ui/metrics`, `/api/portfolio`, `/api/risk/portfolio`, `/api/broker`, or `/api/terminal/positions`. If FX-05 or FX-06 has not surfaced those fields yet, the tile says `FX data not yet available`.
- Browser Terminal: FX pairs use pip-aware price formatting, lot-aware quantities, and a 24/5 session label. The session label mirrors FX-04's FX clock defaults: a 17:00 America/New_York boundary (Sunday open, Friday close) unless configured otherwise, which renders as roughly 21:00 UTC during US daylight time and 22:00 UTC during US standard time.

The UI must not be used as proof of profitability. Profitability remains a backend governance and backtest-gate question, net of realistic costs.

## 6. Operator-Facing Features

### Alert Lifecycle

The dashboard treats alert state as a lifecycle, not a binary read/unread flag.
WARN, HIGH, and CRIT rows are actionable alarms; INFO rows are informational
notifications unless they repeat or escalate. The alert summary strip keeps the
current counts visible for open alarms, notifications, acknowledgements,
shelved alerts, suppressed notification paths, stale unresolved alerts, and
grouped incidents.

Acknowledgement means an operator has seen the alert and owns follow-up. It does
not mean the condition cleared. Shelving means notifications are suppressed only
until the displayed expiry; if the condition is still active after expiry it
returns to the escalation path. Repeated similar alerts are collapsed into a
parent incident in the queue so floods remain visible without hiding severity,
affected entity, current state, freshness, or recommended action.

Backend follow-up for higher maturity is to persist a canonical `incident_id`
and explicit affected-entity field at alert creation time. Until then, the
frontend groups by lifecycle family, entity, horizon bucket, and rule/message
signature while preserving the original alert rows and detail drilldown.

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

### Institutional Check

The operator Institutional Check diagnostic is backed by canonical
`GET /api/operator/institutional_check`; the browser sidecar also keeps
`GET /api/operator/institutionalCheck` as a compatibility alias.

This is a completed diagnostic read, not a mutation or execution gate. When readiness or health is not passing in safe/sim, warming-up, or missing-feed states, the route still returns HTTP 200 with `ok=false`, legacy summary booleans (`configValid`, `healthOk`), and structured `checks`, `blockers`, and `reasons` fields. Each failed or degraded sub-check is named, and each blocker carries a machine-readable reason such as `readiness:storage_unavailable` or `health:health_snapshot_invalid`.

HTTP 500 is reserved for genuine internal faults in the check itself, such as missing dashboard handler wiring or a readiness/health handler exception. Those responses include `error`, `reason_code` or `root_cause_code`, the failing `sub_check` when known, and `meta.status=500` so the operator sees an actionable platform fault instead of a generic `request_failed`.

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
browser prompts. Start, direct startup bootstrap, live start, guided bootstrap,
stop/restart, emergency stop, factory reset, repair/admin actions, secret
changes, feed restarts, and operator-AI patch apply/rollback ask for a typed phrase, consequence
acknowledgement, and a reason or hold period where the server contract requires
one.

In the operator console, Emergency Stop is isolated at the end of the primary
control rows and rendered as a larger octagonal incident control. Its visual
shape, icon, size, position, and accessible name distinguish it from routine
start/restart/stop controls without changing the guarded emergency-stop
confirmation or server execution path.

The modal is not the authority. Direct calls to the operator sidecar or
dashboard API must still include the structured confirmation fields
(`action_id`, confirmation token, acknowledgement, actor, source surface,
request id, target, reason, and hold metadata where required), and the server
rejects missing or invalid confirmation before running the mutation. Audit
records keep the action, actor, source, request id, target, confirmation method,
hold duration, and reason so incident review can reconstruct what was approved.

Direct `POST /api/operator/start` and `POST /api/operator/bootstrap` calls are
non-live start orchestration paths. They reject `{}` with
`confirmation_required` before work starts. Confirmed start uses
`action_id=operator.start` and `START_OPERATOR`; confirmed direct bootstrap uses
`action_id=operator.bootstrap` and `BOOTSTRAP_OPERATOR`. Both require
`consequence_ack`, actor/source surface, target/reason context, bounded request
execution, and top-level domain errors such as `preflight_failed`,
`start_failed:ingestion_runtime`, or `bootstrap_timeout` when orchestration
does not complete cleanly.

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

### Portfolio vol: soft-only by default

`PORTFOLIO_RISK_VOL_HARD_BLOCK` is soft-only by default: it defaults to `0.0`,
which disables the portfolio-vol hard block. With that default, realized
portfolio vol only scales exposure through vol targeting
(`PORTFOLIO_RISK_VOL_TARGET`, default `0.020`); it never blocks a batch by
itself.

To make portfolio vol a hard stop, set `PORTFOLIO_RISK_VOL_HARD_BLOCK` to a
positive annualized/realized-vol threshold. The block fires when the realized
portfolio-vol proxy `pv >= threshold`. At the default `0.0`, gross/net caps and
`PORTFOLIO_RISK_DD_HARD_BLOCK` remain the binding hard stops.

### Kill-switch effective state precedence

The operator API reports both persisted rows and the effective kill-switch state.
The effective state is fail-safe: it is armed when any environment kill-switch
flag is armed, or when any persisted kill-switch row is armed. In formula form:
`effective.armed = env_armed OR persisted_armed`.

This means a persisted row can show `enabled=0` while the system is still armed
because `KILL_SWITCH_GLOBAL=1`, `TRADING_KILL_SWITCH=1`, `KILL_SWITCH=1`, or a
scoped env list such as `KILL_SWITCH_SYMBOLS` is set. In that case
`/api/system/kill_switches` and the dashboard show `ARMED VIA ENV` and
`PERSISTED DISARMED` instead of collapsing both sources into one ambiguous flag.
Clearing an environment hold does not auto-clear a persisted armed row, and
clearing a persisted row does not override an armed environment hold.

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
