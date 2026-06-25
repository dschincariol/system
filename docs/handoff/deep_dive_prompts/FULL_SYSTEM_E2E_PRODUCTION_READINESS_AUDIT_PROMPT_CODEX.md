# Full-System E2E Production-Readiness Audit Prompt (Codex)

> **Goal:** perform a hands-on, evidence-backed audit of the whole trading-system repo by starting the
> runtime, driving every server/API/UI/operator surface one function at a time, pressing every reachable
> button, activating every option, and proving whether the system is fully functional, complete, correct,
> and production-ready.
>
> This is an **audit prompt, not a fix prompt**. Do not patch implementation code during the audit.

---

## 0. Operating Contract

- **Target repo:** `/home/david/gitsandbox/system/system`. This is the Git repo. Ignore the sibling
  `/home/david/gitsandbox/system/prediction-market-sportsbook-worktree` unless a finding explicitly
  needs historical comparison.
- **Audit the working tree as-is.** The repo is expected to be dirty. Do not revert, clean, stage, or
  overwrite user changes.
- **Allowed writes:** the final report at
  `docs/handoff/verification/FULL_SYSTEM_E2E_PRODUCTION_READINESS_AUDIT_REPORT.md` and scratch files under
  `var/tmp/full_system_e2e_audit/` or `/tmp`.
- **Forbidden writes:** no source/test/doc fixes, no dependency lockfile changes, no generated repo
  artifacts outside the scratch/report paths.
- **Evidence first:** every PASS/PARTIAL/FAIL must cite a command, browser action, endpoint, DOM selector,
  file:line, log line, database row, or screenshot path. No evidence means **UNVERIFIED**, which blocks a
  production-ready claim.
- **Do not assume prior reports are current.** Existing UI/function/asset-class audits are useful context,
  but this run must regenerate its own route, control, and behavior inventory from the current working tree.
- **Production-ready verdict standard:** GO requires zero P0/P1 defects, no fake-green tests, no broken
  primary UI/action/API surfaces, no unsafe live-capital route, clean startup/shutdown, and an explainable
  disposition for every remaining gap.

## 1. Repository Scope To Cover

Ground the audit in the live entrypoints and canonical docs:

- `README.md`
- `docs/DOCUMENTATION_INDEX.md`
- `docs/README_OPERATOR_GUIDE.md`
- `docs/PRODUCTION_CHECKLIST.md`
- `docs/REFERENCE_CONFIGURATION_GLOSSARY.md`
- `docs/REFERENCE_DATA_SOURCE_CONTROL_PLANE.md`
- `docs/RUNTIME_STATE.md`
- `start_system.py`
- `start_all.py`
- `start_local.sh`
- `dashboard_server.py`
- `boot/operator_server.js`
- `ui/README.md`
- subsystem READMEs under `engine/`

Expected system shape:

- Python runtime: ingestion, storage, jobs, feature generation, models, portfolio/risk, execution policy,
  broker simulation/live gates, audit/observability, and dashboard API handlers.
- Browser dashboard on `:8000`: `ui/dashboard.html`, tabs `overview`, `operate`, `explain`, `analyze`,
  `data`, `positions`, `execution`.
- Data Sources Control Center on `:8000`: `ui/data_sources.html`.
- Browser terminal on `:8000`: `ui/terminal/terminal.html`.
- Mobile ops surface on `:8000`: `ui/mobile/index.html`.
- Node operator sidecar on `:4001`: `boot/operator_server.js` serving `boot/operator_ui.html`.
- Dashboard API route table: import `dashboard_server.ROUTE_SPECS` and regenerate the current mounted
  route inventory. This repo has previously imported with 205 mounted route specs; do not rely on that
  number without rechecking.

## 2. Safety Contract Before Any Runtime Or UI Mutation

Real capital must never be reachable during this audit.

Before starting anything, record:

```bash
cd /home/david/gitsandbox/system/system
git status --short --untracked-files=all
git log --oneline -10
uname -a
python --version
node --version || true
npm --version || true
df -hT /
```

Run the conservative safe/sim smoke first:

```bash
python tools/safe_sim_boot_smoke.py
```

Then use the prepared safe env for read/UI/control-plane testing:

```bash
python tools/safe_sim_boot_smoke.py --prepare-only
TRADING_ENV_FILE=var/tmp/safe_sim_boot/.env.safe-sim ./start_all.sh
```

Safety gates to prove after boot and before any button/API mutation:

- `ENGINE_MODE=safe` or a documented simulation-only mode.
- `EXECUTION_MODE=safe` for read/control testing.
- `DISABLE_LIVE_EXECUTION=1`.
- `BROKER=sim` and `BROKER_NAME=sim`.
- `LIVE_TRADING_CONFIRM` empty and `LIVE_TRADING_REQUIRE_CONFIRMATION=1`.
- `/api/system/kill_switches` shows safe fail-closed state for live trading.
- `/api/broker/config` resolves to simulator, not Alpaca/IBKR/Tradier/live CCXT.
- `/api/execution/barrier` shows `real_trading_allowed=false`.
- `/api/operator/readiness_evidence` is readable and does not leak secrets.

Use `X-API-Token` and `X-Operator-Token` from `*_FILE` paths only. Never put tokens in URLs, logs,
screenshots, or report bodies.

Use `.env.codex-sim-paper-fills.bak` only for the end-to-end simulated-fill section. That profile is
simulation-only and intentionally sets `KILL_SWITCH_GLOBAL=0`; before using it, repeat all broker/barrier
proofs from `docs/README_OPERATOR_GUIDE.md` and confirm `real_trading_allowed=false`.

If a path attempts a live broker connection, real order route, production secret print, or unbounded destructive
operation, stop that path, leave safety gates armed, and report a P0.

## 3. Build The Audit Inventories First

Create scratch inventory files under `var/tmp/full_system_e2e_audit/`:

1. **Mounted API route inventory**
   - Import `dashboard_server.ROUTE_SPECS`.
   - Print `method path handler`.
   - Group routes by read vs mutation and by subsystem: system/readiness, operator, jobs, data sources,
     broker, terminal, execution, portfolio/risk, market data, governance/promotion, model/strategy,
     news/social/weather, replay, telemetry, alerts.

2. **Operator sidecar route inventory**
   - Static scan `boot/operator_server.js` for `app.get`, `app.post`, websocket paths, and proxy routes.
   - Mark high-impact mutations that require structured confirmation.

3. **UI control inventory**
   - Parse these pages after serving them from the running app, not only from disk:
     - `/ui/dashboard.html`
     - `/ui/data_sources.html`
     - `/ui/terminal/terminal.html`
     - `/ui/mobile/index.html`
     - `/operator/`
   - Enumerate every visible and hidden actionable control:
     - links (`a[href]`)
     - buttons
     - forms
     - inputs
     - selects and every option
     - checkboxes/toggles
     - tabs/screen buttons
     - delegated controls with `data-*` action attributes
     - modals/drawers/confirmation flows
   - For each control capture: page, selector, text/aria-label/title, enabled/disabled state, expected endpoint
     if discoverable, and whether it is read-only, safe mutation, guarded mutation, or dangerous mutation.

4. **Static endpoint reference inventory**
   - Run the existing static checkers:
     - `python tools/check_local_asset_refs.py`
     - `python tools/check_dashboard_ui_contract.py --node-executable "$(command -v node)"`
   - Record every unregistered endpoint reference, missing asset, dead button, or route contract warning.

## 4. Start And Observe The System

Bring up the runtime under safe/sim:

```bash
mkdir -p var/tmp/full_system_e2e_audit
TRADING_ENV_FILE=var/tmp/safe_sim_boot/.env.safe-sim ./start_all.sh \
  > var/tmp/full_system_e2e_audit/start_all.safe.log 2>&1 &
echo $! > var/tmp/full_system_e2e_audit/start_all.safe.pid
```

Wait for bind and readiness. Capture:

- listening sockets for `127.0.0.1:8000` and `127.0.0.1:4001`
- startup phase logs
- first exception/traceback, if any
- `/api/health`
- `/api/liveness`
- `/api/status`
- `/api/system/state`
- `/api/system/health`
- `/api/readiness`
- `/api/operator/ping`

If startup fails, do not stop at "failed to start"; identify the first failing phase and responsible file/function.

## 5. API Route Audit

Drive every mounted dashboard API route from the regenerated `ROUTE_SPECS`.

For each **GET** route:

- Send an authenticated request with `X-API-Token` unless the route is documented public.
- Verify HTTP status is appropriate.
- Verify JSON is valid.
- Verify shape matches the owning UI/README expectation.
- Verify unavailable/degraded states are explicit, not generic `request_failed` unless truly fatal.
- Verify secret-shaped values are masked.
- Mark stale, empty, placeholder-only, 404, 401, 429, 500, timeout, schema mismatch, or console-dependent failures.

For each **POST/mutation** route:

- Classify as safe testable, guarded, destructive, or live-risk.
- For safe testable routes, send the smallest valid and invalid payloads; verify state change, audit record, and
  refusal behavior.
- For guarded/destructive routes, first prove missing/invalid confirmation is rejected. Only execute the mutation
  if it is safe in the current sim profile and has a bounded rollback/cleanup.
- For live-risk routes, do not execute; prove server-side gates reject or require confirmation while live execution
  is disabled.

Required API groups:

- health/liveness/readiness/system state/config/mode
- execution barrier, kill switches, manual halt, emergency stop refusal paths
- jobs catalog/log/history/start/stop/pipeline run
- data source CRUD/test/test-save/populate/account update/logs
- broker config/test/audit
- terminal watchlist/snapshot/positions/orders/fills/equity/markers/decision overlays/order/flatten
- market candles/stream/session/prices/feeds
- portfolio, PnL, risk summary, risk portfolio, Monte Carlo
- execution metrics/stats/advisories/diagnostics/overlays
- model registry/metrics/diagnostics/lifecycle/performance divergence
- promotion/governance/shadow-capital/champion rollback gates
- alerts/timeline/by-id/ack/shelve/resolve
- replay day
- news/social/weather/market stress/feature visibility
- operator bridge routes and sidecar proxy routes
- self-repair/repair-schema/autofix paths, with confirmation/live-mode refusals

## 6. Browser UI Audit

Use a real browser automation path if available: Playwright, Puppeteer, Selenium, or Chrome CDP. Capture
screenshots, console errors, failed network requests, and DOM state after every page load and major action.
If no browser is available, state that UI coverage is partial and compensate by driving backing endpoints directly.

For every page:

- load from the real running server
- wait until network is idle or a bounded timeout is reached
- capture full-page screenshot
- capture console errors/warnings
- capture failed requests and status codes
- record visible stale/empty/loading/error placeholders
- verify responsive viewport at desktop and mobile widths where relevant

### Dashboard: `/ui/dashboard.html`

Exercise all top-level links and controls:

- Sources link
- Terminal link
- Operator link
- Copilot toggle and Ask flow
- all screen tabs: Overview, Operate, Explain, Analyze, Data Health, Positions, Execution
- refresh controls and any command palette controls
- every collapsible card header/action
- every table search field
- every table sort button
- every select/dropdown and all options
- every modal/drawer open/close path

Required dashboard panels/functions:

- Operator Overview and health score coverage
- readiness evidence
- runtime diagnostics/status/telemetry
- system state and kill-switch display/actions
- jobs catalog/history/trends/run controls
- alerts list/timeline, incident drawer, ack/shelve/resolve flows
- decisions, decision bar, drilldown modal, attribution, stepper, terminal/operator cross-links
- governance evidence, promotion gate/safety, rollback/enable refusals
- model registry/diagnostics/performance divergence/lifecycle
- portfolio, positions, exposure, risk headroom, risk charts, Monte Carlo
- portfolio backtest charts and accessibility tables
- execution snapshot, orders, fills, outcomes, trace, TCA, advisory actions, degradation/microstructure/slicing
- data health, feature visibility, provider telemetry, futures panel, FX/crypto/options/equity surfacing
- market data, market stress, news/social/weather panels
- replay controls and pro chart overlays

For each button/control: click or activate it. For high-impact controls, prove server-side confirmation/refusal rather
than bypassing safety.

### Data Sources: `/ui/data_sources.html`

Exercise:

- token/session save and clear
- refresh
- add source modal
- source template/provider selection
- all input validation states
- test connection
- test and save
- save source
- enable/disable
- delete/reset credentials
- account modal and save account
- logs panel
- provider badges and next-action guidance

Verify credentials are never exposed in DOM, requests, responses, logs, screenshots, or report output.

### Terminal: `/ui/terminal/terminal.html`

Exercise:

- watchlist/symbol selection
- quote/snapshot loading
- chart/pro-chart bootstrap, indicators, decision overlays, markers
- positions/orders/fills filters and status dropdowns
- BUY, SELL, FLATTEN buttons
- invalid order payloads
- gated order refusal under safe mode
- one valid simulated order only under the paper/sim-fill profile

Terminal order testing must prove:

- broker chain is `["sim"]`
- `real_trading_allowed=false`
- order intent persists
- sim broker fill persists
- attribution persists
- dashboard execution panels reflect the fill

### Operator: `/operator/` and direct `:4001`

Exercise:

- mode buttons REVIEW/DATA/TRADING, including disabled live/trading state
- Start System Setup
- Start Engine / Restart / Stop safe behavior
- Emergency Stop gate
- Open Source Control / Open Dashboard
- all refresh buttons
- preflight, repair, institutional check
- restart feeds
- clear error
- config validate/save/factory reset guarded states
- secrets refresh/save guarded states
- logs, copy logs, support snapshot/copy/download
- service status, system health, market data, strategy decisions, trading monitor, trade blotter
- operator AI run/explain/patch preview/apply/rollback gates
- backup/update/restart operator guarded or unsupported paths
- websocket telemetry path if present

Verify direct `:4001` behavior and dashboard-proxied `/operator/` behavior agree where intended.

### Mobile Ops: `/ui/mobile/index.html`

Exercise:

- refresh
- token entry
- heartbeat/readiness/health/provider/ingestion summary
- PnL, positions, alerts, broker/feeds panels
- emergency stop phrase and hold-to-confirm flow; prove missing/invalid token/phrase rejection and avoid unsafe
  destructive execution unless the safe profile explicitly requires and bounds it.

## 7. End-To-End System Flows

Run these as scenario tests with evidence IDs.

1. **Cold boot to readiness**
   - fresh safe/sim boot
   - DB/cache/storage initialized
   - dashboard/operator reachable
   - no startup traceback
   - readiness explains all blockers

2. **Data source lifecycle**
   - create a non-secret/safe source or use a fixture-compatible source
   - validation errors for missing credentials
   - test refusal with stable reason code
   - enable/disable/delete or cleanup
   - audit/log rows written

3. **Job lifecycle**
   - list catalog
   - choose a safe job
   - start/stop or prove guarded refusal
   - verify job history/log update
   - verify pipeline run refusal or bounded execution

4. **Decision/explanation path**
   - get recent decisions
   - open drilldown
   - verify stage/inputs/risk/attribution payloads match UI
   - verify terminal/operator cross-links

5. **Risk and governance path**
   - portfolio/risk/Monte Carlo/governance endpoints
   - dashboard cards render corresponding data
   - promotion/rollback gates refuse unsafe action without confirmation

6. **Simulated terminal order path**
   - switch to `.env.codex-sim-paper-fills.bak`
   - prove simulator and live-disabled gates
   - post one tiny order through terminal/API
   - run `python engine/execution/jobs/broker_apply_orders.py`
   - run `python engine/execution/jobs/execution_poll_and_attrib.py`
   - prove intent -> risk/barrier -> sim fill -> attribution -> API -> UI

7. **Shutdown**
   - clean shutdown through documented endpoint or signal
   - no orphaned `start_system.py`, `operator_server.js`, dashboard server, or child job processes
   - ports closed or intentionally reused
   - safety gates remain conservative on next boot

## 8. Validation Commands

Run and record exit code plus key output:

```bash
python tools/git_worktree_triage.py
python tools/check_repo_artifact_hygiene.py --report
python tools/syntax_check_workspace.py
python tools/pyright_money_path_gate.py
python -m pytest --version
npm run check:ui
npm run test:ui
python tools/validate_repo.py
python tools/validate_repo.py --live
python tools/production_readiness_gate.py || true
python -m pytest -q -m safety_critical
python -m pytest tests/test_paper_mode_sim_fill_boot.py -q
```

If an environment dependency blocks a command, classify it as `BLOCKED-ENVIRONMENT`, include the exact missing
dependency, and explain whether production readiness can still be claimed.

## 9. Severity And Verdict Rules

- **P0 blocker:** startup failure; live capital reachable; unsafe order route; data loss/corruption; secret leak;
  destructive action without confirmation; core server/UI unavailable.
- **P1 critical:** primary advertised feature broken; end-to-end sim flow broken; false readiness/health; route
  returns 500/404/401/429 in normal authenticated use; button appears actionable but does nothing.
- **P2 major:** degraded but bounded behavior; stale/empty panel without clear reason; partial route/UI coverage;
  flaky startup/shutdown; missing audit trail for a mutation.
- **P3 minor:** cosmetic, copy, responsiveness, slow panel, non-blocking accessibility or polish issue.

Verdict vocabulary:

- `PASS`: directly exercised and evidence proves correct behavior.
- `GATED-OK`: intentionally disabled or refused, and the gate/refusal is itself proven correct.
- `PARTIAL`: some behavior works, but coverage/data/rendering/persistence is incomplete.
- `FAKE-GREEN`: tests or UI imply success but do not drive the production path.
- `BROKEN`: production path exists but fails.
- `MISSING`: advertised function/control/route has no implementation.
- `UNVERIFIED`: not tested or no evidence; blocks GO for production readiness if primary.

## 10. Required Report

Write the report to:

`docs/handoff/verification/FULL_SYSTEM_E2E_PRODUCTION_READINESS_AUDIT_REPORT.md`

Use this exact structure:

```markdown
# Full-System E2E Production-Readiness Audit Report

- Audit date: <date from date -Iseconds>
- Repo: /home/david/gitsandbox/system/system
- Git HEAD: <sha and subject>
- Working tree: <dirty summary>
- Runtime profiles used: <safe/sim, paper/sim-fill>
- Final verdict: GO / NO-GO

## Executive Verdict
<short evidence-backed statement; no unsupported optimism>

## Safety Gate Evidence
| Gate | Evidence | Verdict |
| --- | --- | --- |

## Inventory Coverage
| Inventory | Count | Exercised | Not exercised | Notes |
| --- | ---: | ---: | ---: | --- |
| Dashboard API routes | | | | |
| Operator sidecar routes | | | | |
| UI controls/buttons/options | | | | |
| Browser pages | | | | |

## Surface Results
| Surface | Verdict | Evidence | Defects |
| --- | --- | --- | --- |
| Runtime boot/shutdown | | | |
| Dashboard | | | |
| Data Sources | | | |
| Terminal | | | |
| Operator | | | |
| Mobile Ops | | | |
| API route table | | | |
| Jobs/pipeline | | | |
| Data ingestion/source lifecycle | | | |
| Portfolio/risk/governance | | | |
| Execution/sim fills | | | |
| Observability/readiness | | | |

## End-To-End Flow Evidence
| Flow | Verdict | Evidence IDs / rows / endpoints |
| --- | --- | --- |

## Findings Ranked
1. [P0/P1/P2/P3] <title> - <surface/endpoint/file:line>
   - Observed:
   - Expected:
   - Evidence:
   - Recommended fix:

## Works Verified
- <function> - <evidence>

## Fake-Green / Coverage Gaps
- <test/UI/API claim that is not production-path verified>

## Validator Results
| Command | Exit | Key output | Attribution |
| --- | ---: | --- | --- |

## Production Readiness Blockers
1. <blocker>

## Follow-Up Fix Prompts
<If NO-GO, produce focused Codex-ready fix prompts for the top blockers. Each prompt must be scoped, cite evidence, name files/tests, and include safety guardrails.>
```

Cap the main finding list to the highest-impact issues, but keep the full raw inventories as scratch attachments
under `var/tmp/full_system_e2e_audit/`.

Before ending the audit:

- stop all processes you started
- verify ports are closed or intentionally reused
- verify no live execution state was armed
- verify no secret values were printed into the report
- leave scratch logs/screenshots in `var/tmp/full_system_e2e_audit/`
