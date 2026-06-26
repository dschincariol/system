# Full-System E2E Production-Readiness Audit Report

- Audit date: 2026-06-25T13:42:29-04:00 (start) → 2026-06-25T15:1x (report)
- Repo: /home/david/gitsandbox/system/system
- Git HEAD: `afc1595` — "Stabilize production readiness worktree"
- Working tree: dirty (expected; audited as-is, no source/test/doc fixes made). ~40 tracked files modified (boot/, dashboard_server.py, docs/, engine/api/*, engine/runtime/*, services/, start_system.py, tests/).
- Runtime profiles used: safe/sim (`var/tmp/safe_sim_boot/.env.safe-sim` via `start_system.py safe`), paper/sim-fill (`tests/test_paper_mode_sim_fill_boot.py` hermetic profile = `.env.codex-sim-paper-fills.bak` equivalent)
- Host: `bart`, Linux 7.0.0-22-generic, Python 3.11.15 (.venv), Node v20.19.4, Postgres/TimescaleDB running, 278G free on /
- **Final verdict: NO-GO**

## Executive Verdict

The system boots cleanly in safe/sim, shuts down cleanly, and its **safety posture is strong**: there is **no reachable live-capital path**, no secret-value leak, no destructive action without multi-factor confirmation, and the simulated order→fill→attribution chain works end-to-end. The conservative `safe_sim_boot_smoke` and `production_readiness_gate` both pass (exit 0).

However, an exhaustive drive of all **209 mounted dashboard API routes** plus the **operator sidecar** surfaced **multiple P1 defects where advertised, button-backing read endpoints return HTTP 500/404 in normal authenticated use** (champion competition view, operator institutional check, jobs history, dashboard copilot, the entire weather subsystem). Per the stated standard (GO requires **zero P0/P1**), these block a production-ready claim. There are **0 P0** issues. The verdict is therefore **NO-GO — fixable**: the blockers are degraded-state/missing-table handling and a read-connection visibility bug, not architectural or safety failures.

A second, independent blocker is config-level: the production validation gate **`validate_repo --live` fails (exit 1)** because the production secret-source policy refuses to boot with `BACKUP_EVIDENCE_HMAC_KEY` supplied via a non-approved source — the same gate that prevents the `.env.codex-sim-paper-fills.bak` profile from booting via `start_system.py paper`. This is the policy working correctly (fail-closed) against a working-tree config that does not satisfy it.

Severity tally (deduplicated, post-verification): **P0 = 0, P1 = 8, P2 = 10, P3 = 8.**

## Safety Gate Evidence

| Gate | Evidence | Verdict |
| --- | --- | --- |
| ENGINE_MODE / EXECUTION_MODE = safe | `safe_sim_boot_smoke` safety_env: `ENGINE_MODE=safe`, `EXECUTION_MODE=safe`, `OPERATOR_MODE=safe` | PASS |
| DISABLE_LIVE_EXECUTION=1 | safety_env `DISABLE_LIVE_EXECUTION=1` | PASS |
| BROKER=sim / BROKER_NAME=sim | `/api/broker/config` → `config.active_broker=sim`, `paper_live_mode=safe`; operator :4001 same | PASS |
| LIVE_TRADING_CONFIRM empty + REQUIRE_CONFIRMATION=1 | safety_env `LIVE_TRADING_CONFIRM=""`, `LIVE_TRADING_REQUIRE_CONFIRMATION=1` | PASS |
| /api/system/kill_switches fail-closed | `data.effective.armed=true`, `env_armed=true`, active=`[KILL_SWITCH_GLOBAL]` | PASS |
| /api/broker/config = simulator (not Alpaca/IBKR/Tradier/live CCXT) | `active_broker=sim`; all live providers disabled (`IBKR_ENABLED=0`, `CCXT_ENABLED=0`, `TRADIER_ENABLED=0`, `POLYGON_*_ENABLED=0`) | PASS |
| /api/execution/barrier real_trading blocked | `allowed=false`, `execution_allowed=false`, `execution_mode=safe`, `state=DEGRADED`, 17 blocking reasons | PASS |
| /api/operator/readiness_evidence readable, no secrets | 44 items, `target_broker=sim`, blocking=0; secret-shaped values masked | PASS |
| Mutation auth enforced | POST `/api/jobs/start` no-token → **401**; POST `/api/operator/set_mode` no-token → **403** | PASS |
| Operator destructive mutations confirmation-gated | 24/24 destructive ops (set_mode/start/stop/restart/restart_engine/restart_operator/emergency_stop/factoryReset/self_repair/autofix/repairSchema/promote_model/…) → **422 confirmation_required** with empty payload; none executed | PASS |
| Secret hygiene | 921KB config dump + 3.1MB support snapshot scanned: only env-var NAMES + `<redacted:hash>` paths + SHA-256 fingerprints; **zero secret values** | PASS |
| No live import in paper mode | `test_paper_mode_sim_fill_boot` live-adapter-import canary not triggered; `effective_broker_chain()==["sim"]` | PASS |

## Inventory Coverage

| Inventory | Count | Exercised | Not exercised | Notes |
| --- | ---: | ---: | ---: | --- |
| Dashboard API routes (`ROUTE_SPECS`) | 209 | 209 | 0 | 162 GET + 47 POST; 12-agent parallel drive |
| Operator sidecar routes (`boot/operator_server.js`) | 85 | 85 (key GETs + all 26 mutation refusal paths) | — | static scan + live probe |
| UI controls/buttons/options (static) | dashboard 84 btn / 17 sel / 20 inp / 9 data-action / 6 chk / 5 link; operator 46 btn; data_sources 11 btn / 2 forms; terminal 3 btn (JS-built) / 4 sel / 5 chk; mobile 2 btn | static-parsed; backing endpoints driven via API audit | live click-through of JS-built controls | No Playwright/Puppeteer/CDP driver available |
| Browser pages (rendered) | 5 | 3 screenshots (operator, mobile, data_sources) | dashboard, terminal (hung on persistent SSE/poll connections) | Chrome present at /usr/bin/google-chrome; headless screenshot only |
| 7 dashboard tabs | 7 | confirmed present (Overview/Operate/Explain/Analyze/Data Health/Positions/Execution via `data-screens=`) | tab-switch interaction | — |

## Surface Results

| Surface | Verdict | Evidence | Defects |
| --- | --- | --- | --- |
| Runtime boot/shutdown | PASS | safe/sim boot bound :8000+:4001; SIGTERM → ports closed ~1s, 0 orphans, exit 0 | — |
| Dashboard (API + structure) | PARTIAL | 134/209 PASS, 29 GATED-OK, 36 PARTIAL(empty-but-correct sim), 9 BROKEN, 1 MISSING | P1×several |
| Data Sources | PARTIAL | 38 sources; all 10 mutations validate/refuse; delete guarded (DELETE_SOURCE token) | P2 (builtin-delete 500; token_required self-declaration) |
| Terminal | GATED-OK | 13 GET ok; order/flatten confirmation-gated (TRADE/FLATTEN + ack, flatten 1500ms hold); SSE stream ok | P3 (session OPEN vs empty candles) |
| Operator (:4001 + /operator proxy) | PASS (safety) / PARTIAL (reads) | all key GETs 200; 24 mutations 422-gated; auth 403 no-token | P1 (competition, institutionalCheck 500); P2 (bootstrap ungated+500, start ungated) |
| Mobile Ops | PASS | page 200, renders (screenshot); 2 controls; backing endpoints via API audit | — |
| API route table | PARTIAL | 209 mounted, regenerated live (prior reports said 205) | see findings |
| Jobs/pipeline | PARTIAL | catalog/log/training_status ok; start/stop/pipeline/repair 422-gated | P1 (jobs/history 500); P2 (validation raw SQL leak) |
| Data ingestion/source lifecycle | GATED-OK | ingestion DEGRADED by design (safe mode, START_INGESTION_WITH_SERVER=0) | — |
| Portfolio/risk/governance | PASS | portfolio/risk/summary/governance/summary 200 coherent; rollback/promote 422-gated | P3 (empty-marker labeling) |
| Execution/sim fills | PASS | isolated `test_paper_mode_sim_fill_boot` → broker_fills≥1, execution_fills≥1, pnl_attribution≥1, trade_attribution≥1 | P2 (advisories 500; concurrency robustness) |
| Observability/readiness | PASS | health/liveness/status/system_state/readiness all valid; readiness explains all blockers | P3 (runtime_watchdogs spurious top-level error) |

## End-To-End Flow Evidence

| Flow | Verdict | Evidence |
| --- | --- | --- |
| 1. Cold boot to readiness | PASS | safe/sim boot bound; readiness `failed` but **explains every blocker** (startup_complete, ingestion_active, ingestion_not_stale) — intentional in safe mode (ingestion disabled) |
| 2. Data source lifecycle | PARTIAL | 38 sources listed; missing-cred test → 422 credentials_missing (fail-closed); bogus key → 404 source_not_found; builtin delete refused (but as 500) |
| 3. Job lifecycle | GATED-OK | catalog + log readable; start/stop refuse without TRADE/JOB_ACTION confirmation (422); history viewer BROKEN (P1) |
| 4. Decision/explanation path | PARTIAL | decision_overlays 200; decisions empty (no ingestion/models in safe sim) — correct degraded state |
| 5. Risk & governance path | PASS | portfolio/risk/montecarlo*/governance render; promotion/rollback refuse unconfirmed (422) |
| 6. Simulated terminal order path | PASS (isolated) | order(AAPL BUY 1, confirmation=TRADE)→200; broker_apply_orders(sim)→fills; execution_poll_and_attrib→attribution; DB asserts broker_fills/execution_fills/pnl_attribution/trade_attribution ≥1. **NOTE: fails under concurrent Postgres load** (see P2). |
| 7. Shutdown | PASS | SIGTERM → both launchers exit 0, :8000/:4001 closed, zero orphans, next boot still safe |

## Findings Ranked

> Verified inline against the live server where noted. Severities deduplicated and adjusted after adversarial verification (one workflow-reported P1 — "GET read auth not enforced" — was **downgraded to P2** after confirming GET-open is by-design mutation-only auth).

1. **[P1] Champion competition view returns 500 for both `/api/system/competition` and `/api/operator/competition`** — shared handler `api_get_competition_view` (engine/api/api_system.py:2786)
   - Observed: 3/3 retries HTTP 500 `{detail:OperationalError, reason_code:handler_exception}`.
   - Expected: 200 with snapshot or graceful empty/degraded body.
   - Evidence: backing tables (`model_competition_rankings`, `model_marketplace_scores`, `champion_assignments`) EXIST in sim DB (db_schema have_tables), so it is a SQL/handler bug, not missing-table. `current_competition_snapshot()` (champion_manager.py:1404) raises.
   - Fix: wrap `current_competition_snapshot()` in the standard degraded-state guard; return `ok:true, rows:[], reason:...` when query fails.

2. **[P1] `/api/jobs/history` returns 500 "no such table: job_history" though the table exists with 833 rows** — read-connection visibility bug
   - Observed (verified inline): `dget /api/jobs/history?name=metrics_collector` → 500 `{error:job_history_exception}`; `sqlite3 …/trading.db "SELECT count(*) FROM job_history"` → **833**.
   - Expected: 200 history rows; `/api/jobs/log` (same job) works fine.
   - Evidence: `read_job_history()` (engine/runtime/locks_pg.py:275) uses `connect(readonly=True)` which cannot see the table the runtime/health connection sees → dashboard job-history viewer is a dead button.
   - Fix: align the readonly reader's DB path/attach with the writer; or fall back to the runtime connection.

3. **[P1] `/api/copilot/ask` returns 500 for every input** (verified inline)
   - Observed: empty body and well-formed `{question:"why is the system degraded?"}` both → 500.
   - Evidence: handler (dashboard_server.py:2298) returns `ok:false` with no `error/reason/meta`; `http_transport._map_error_to_status('')` (line 341) defaults that to 500. Dashboard copilot widget 500s on every use.
   - Fix: return graceful 200 (degraded answer) or 4xx for empty question / unavailable-LLM branches; populate `error`/`reason`.

4. **[P1] Weather subsystem 500s on missing tables instead of degrading** — `/api/weather/alerts`, `/api/weather/effect`, `/api/weather/snapshot` (default symbol SPY)
   - Observed: alerts/effect → 500 OperationalError with/without params; snapshot SPY/QQQ → 500, but AAPL/MSFT/BTC-USD → 200 zero-filled.
   - Evidence: `dashboard_weather_widgets.py` issues SELECTs against missing `weather_alerts`/`model_weather_effect`/`weather_snapshot` with no missing-table guard. Peers (news/sentiment) degrade gracefully (200 `ready:false`). The dashboard default invocation (SPY) crashes.
   - Fix: missing-table guard → empty 200; ensure schema bootstrap creates these tables.

5. **[P1] `/api/operator/institutionalCheck` returns 500 request_failed** (deterministic, 3/3)
   - Observed: `{configValid:false, error:request_failed, healthOk:false, meta.status:500}`. The operator institutional-readiness panel cannot render.
   - Fix: surface the real failing sub-check; degrade rather than 500.

6. **[P1] `/api/operator/clear_last_error` (snake_case) returns 404 unrouted** — only camelCase `/clearLastError` is registered (engine/dashboard/routing.py:39)
   - Observed: snake_case → 404 unknown_endpoint; camelCase → 200 (working). The live UI uses camelCase, so the *feature* works, but any caller/doc using snake_case hits a dead route. (Borderline P1/P2 — listed P1 because it is an advertised-but-unrouted path; mitigated by the working alias.)
   - Fix: register both aliases or remove the snake_case advertisement.

7. **[P1] `POST /api/operator/bootstrap` runs a slow (>30s) state-affecting startup orchestration with NO confirmation gate and returns 500 in sim**
   - Observed: empty `{}` body executes `StartupOrchestrator`, blocks past 30s, returns 500. Unlike all other operator mutations (which 422 on empty body), bootstrap has no confirmation enforcement despite a "medium" registry entry.
   - Note: server survived, execution stayed safe/disarmed — no capital reach, so not P0. But a state-affecting "start" being ungated + slow + erroring is a governance/button gap. (Companion: `POST /api/operator/start` is also ungated and 500s on a real start attempt — P2 #4 below.)
   - Fix: enforce `requireOperatorConfirmation` on bootstrap; make it async/bounded; map start failures to a real reason instead of generic request_failed.

1b. **[P1] `validate_repo --live` production runtime-graph-startup gate fails (exit 1) on the production secret-source policy.**
   - Observed: `tools/runtime_graph_check.py --mode startup` → `FAIL engine.runtime.jobs_manager / runtime_bootstrap → ConfigError: production secret source policy invalid: secret_file_invalid:BACKUP_EVIDENCE_HMAC_KEY; use *_FILE, *_SECRET, systemd credentials, Docker Compose secrets, or root-owned 0600 files` (+1 redacted secret). cold_boot_db sub-check exit_code=1.
   - Expected: production `--live` validation passes (or the secret is sourced via an approved method).
   - Evidence: `var/tmp/full_system_e2e_audit/val_validate_repo_live.log`. The same `runtime_graph_check[exit=1]` blocked `start_system.py paper` with the raw `.bak` profile during this audit.
   - Interpretation: the secret-source policy is **correctly fail-closed**; the working-tree config does not provide `BACKUP_EVIDENCE_HMAC_KEY` (and ≥1 other secret) via an approved source. This is a production-config blocker, not a code bug — but `validate_repo --live` does not pass as-is. (`safe` mode passes because the prepared safe env moves secrets to approved `*_FILE` sources.)
   - Fix: provide `BACKUP_EVIDENCE_HMAC_KEY` (and any other flagged secret) via `*_FILE`/`*_SECRET`/systemd cred/root-owned 0600 file; re-run `validate_repo --live`.

### P2 (major)
- **[P2] GET read endpoints serve full bodies without a token while `/api/data_sources` self-declares `auth.token_required:true`.** Verified: GET open is **by design** (`engine/api/auth_config.py` = mutation-auth only; POST→401, GET→200). Real issues: (a) the self-declared `token_required:true` contradicts actual behavior (misleading); (b) confirm LAN mode (`TRADING_NETWORK_MODE=lan`, 0.0.0.0 bind) gates *reads*, else the 921KB config + provider templates are exposed. Mitigated here by 127.0.0.1-only bind. No secret values leak.
- **[P2] `POST /api/data_sources/delete` of a builtin source returns 500** (`builtin_source_delete_not_allowed`) — refusal is correct (source preserved, count stayed 38) but a handled policy refusal should be 403/409, not 500.
- **[P2] `/api/execution/advisories` returns 500** (`no such table: execution_ai_advisory`) while sibling `/api/execution/diagnostics` reports the same condition as a graceful `state=unavailable` 200 — inconsistent degradation contract (api_ops_handlers.py:339).
- **[P2] `POST /api/operator/start` has no confirmation-token guard and executes a real start attempt**, then 500s with generic `request_failed` (real cause `start_failed:ingestion_runtime:process exited rc=1` only nested). Asymmetric vs restart/stop/self_repair which require confirmation. Stayed safe-mode, no live escape.
- **[P2] alert ack/resolve accept non-existent ids and report success**, writing phantom lifecycle/ack/resolution rows (POST `/api/alerts/999999/ack` → 200 while `/api/alerts` total=0, by_id=404). No SELECT-exists guard (api_write.py:222-263,355+). shelve is correctly stricter (422). Sim DB, bounded.
- **[P2] `/api/audit/records?table=alerts` returns 500** (uncaught `ValueError not_audit_table`) for a valid-identifier non-audit table; the SQL-injection path correctly returns 400. Bad input should be 4xx (api_dashboard_reads.py:203-229).
- **[P2] `/api/validation` leaks raw SQL** `no such table: validation_scores` as the client-facing error (HTTP 400) instead of a clean `{ok:true,rows:[]}` (api_read_advanced.py:1497).
- **[P2] Promotion-audit endpoints return empty `{data:[]}` with no marker** to distinguish "no activity" from "missing table/broken trail" (`/api/promotion_audit`, `/api/promotion/audit`).
- **[P2] Sim-fill execution path is not robust to concurrent Postgres access.** Running `test_paper_mode_sim_fill_boot` *concurrently with the full suite* failed with `broker_sim_account_snapshot_invalid (cash_raw=None)` → `Cannot operate on a closed database` at broker_apply_orders.py:2050/2055 → 0 fills. **Passes cleanly in isolation.** Production runs a single broker_apply_orders, so not a capital risk, but the premature connection-close on the error path is a latent bug.
- **[P2] relevance_stats / market_stress_history** return empty without a `reason`/`ready` marker (harder to tell "empty by design" from "broken").

### P3 (minor)
- [P3] `POST /api/repair_schema` handler has no internal confirmation guard (api_self_repair.py:74-82); safe only because the transport registry gates it (422 proven). Defense-in-depth gap.
- [P3] `GET /api/operator/runtime_watchdogs` returns top-level `error:"request_failed"` despite HTTP 200 + fully populated body (spurious error field).
- [P3] `POST /api/operator/execution_arm` refusal returns HTTP 400 while body `meta.status=422` (status mismatch; guard intact).
- [P3] `POST /api/notifications/test` returns 500 for `channel_not_configured`/`unknown_channel` (should be 4xx; refusal is correct + side-effect-free).
- [P3] `/api/market/session` reports OPEN while `/api/market/candles`+`/api/prices` are `ready:false` empty (correct degraded, but a UI may show "open market, no chart").
- [P3] analytics reads (`strategy_metrics`, `causal/scores`, `size_policy`) return empty arrays/null without a "no data yet" marker (inconsistent with peers that label emptiness).
- [P3] governance `generated_candidates` evidence permanently `block` on `missing_experiment_ledger` (correct fail-closed, but non-functional until ledger populated).
- [P3] Rendered-screenshot coverage partial: dashboard + terminal pages hang headless Chrome (persistent SSE/poll); no Playwright/Puppeteer/CDP driver in environment.

## Works Verified

- Safe/sim cold boot → bind :8000 + :4001; clean SIGTERM shutdown, 0 orphans (exit 0).
- All 12 safety gates fail-closed (table above): broker=sim, barrier blocked, kill switch armed, live disabled, no secret values.
- 134/209 dashboard GET routes return valid, well-shaped JSON; degraded states truthfully labeled.
- Operator sidecar: all key GETs 200; auth 403 without token; **all 24 destructive mutations confirmation-gated (422)** + rate-limited (429 at 6/min).
- Model subsystem (17 routes) fully consistent; promotion mutation 422-gated (PROMOTION token never sent).
- Governance/promotion (13 routes): rollback/enable 422-gated (ROLLBACK_CHAMPION/PROMOTION + ack).
- Terminal order/flatten gated (TRADE/FLATTEN + ack + 1500ms hold for flatten).
- **End-to-end simulated order→sim-fill→attribution persisted** (isolated): broker_fills/execution_fills/pnl_attribution/trade_attribution ≥1; no live adapter import.
- Static checkers + UI unit tests + money-path typing + syntax all green (validator table below).

## Fake-Green / Coverage Gaps

- **`production_readiness_gate` passes (exit 0) but does NOT probe the broken read endpoints** found here — it validates broker-sim pinning, db_health, and safety posture, not per-endpoint functional health. A green gate does not imply the dashboard read surface is healthy.
- **`.env.codex-sim-paper-fills.bak` does not boot via `start_system.py paper` as-is** on this host: it fails the `production_validation_gate` (`runtime_graph_check[exit=1]`) because it lacks `TRADING_SKIP_RUNTIME_GRAPH_CHECK=1` and pre-seeded prices. The passing sim-fill proof comes from the hermetic `test_paper_mode_sim_fill_boot` which sets those. The raw profile alone is not a one-command bootable paper runtime.
- UI **interaction** coverage is partial (no browser-automation driver): controls were enumerated statically and their backing endpoints driven via the API audit, but live click-through of JS-built terminal/dashboard controls was not performed.
- Empty panels in safe/sim (decisions, metrics, weather data) are correct degraded states, **not** proof those panels render real data under a live feed.

## Validator Results

| Command | Exit | Key output |
| --- | ---: | --- |
| `safe_sim_boot_smoke.py` | 0 | boot+probe+shutdown clean; all safety endpoints safe |
| `check_local_asset_refs.py` | 0 | 111 tracked files clean |
| `check_dashboard_ui_contract.py --node-executable node` | 0 | Assets=82 endpoints=235 js_modules=70 |
| `git_worktree_triage.py` | 0 | report ready:false (dry-run default note; not an error) |
| `check_repo_artifact_hygiene.py --report` | 0 | tracked artifact violations: 0 |
| `syntax_check_workspace.py` | 0 | Syntax OK: 869 files |
| `pyright_money_path_gate.py` | 0 | passed: 29 baseline errors, 0 warnings, 30 target files |
| `pytest --version` | 0 | pytest 9.0.3 |
| `npm run check:ui` | 0 | UI validation passed |
| `npm run test:ui` | 0 | UI validation passed (0 skipped) |
| `production_readiness_gate.py` | 0 | status PASS (broker_sim pinned, db_health ok) |
| `pytest test_paper_mode_sim_fill_boot.py` (isolated) | 0 | sim fill + attribution persisted |
| `pytest -m safety_critical` | 0 | PASSED (includes the isolated paper sim-fill test) |
| `validate_repo.py --live` | **1** | **FAILED at `runtime_graph_check --mode startup`**: `ConfigError: production secret source policy invalid: secret_file_invalid:BACKUP_EVIDENCE_HMAC_KEY` (+1 redacted). cold_boot_db + jobs_manager + runtime_bootstrap imports fail. See P1 finding #1b. |
| `validate_repo.py` (static, full suite) | n/a | started; interrupted to free Postgres for the isolated sim-fill run — superseded by `--live` |

## Production Readiness Blockers

0. **P1 `validate_repo --live` fails** on the production secret-source policy (`BACKUP_EVIDENCE_HMAC_KEY` + 1 redacted) — the production validation gate does not pass and paper/live boot is blocked until secrets are sourced via an approved method. (Config fix, not code.)
1. **P1 broken read/button-backing endpoints** must return 200/graceful-degraded (not 500): champion competition (×2), jobs/history, copilot/ask, weather alerts/effect/snapshot, operator institutionalCheck.
2. **P1 `/api/operator/clear_last_error` 404** (register alias or remove advertisement).
3. **P1 `POST /api/operator/bootstrap`** ungated + slow + 500 — add confirmation gate, bound runtime, real error mapping.
4. **P2 degradation-contract inconsistencies** (missing-table 500s vs graceful 200) across execution/advisories, validation, audit/records, data_sources/delete — standardize on graceful degraded responses.
5. **P2 alert ack/resolve phantom-record** integrity gap — add existence check.
6. **P2 sim-fill concurrency robustness** — fix premature connection close on the broker_apply_orders error path.

## Follow-Up Fix Prompts

> Expanded into 14 standalone, focused Codex deep-dive prompts (one per root cause, covering every P1/P2 and a P3 polish bundle) in `docs/handoff/deep_dive_prompts/FULL_SYSTEM_E2E_REMEDIATION_DEEP_DIVE_PROMPTS.md` (E2E-R1…E2E-R14). The condensed FIX-1…FIX-6 below remain as a quick index.

**FIX-1 — Graceful degradation for read endpoints that 500 on missing tables / DB errors.**
Scope: `engine/api/api_system.py:api_get_competition_view` (+ `engine/strategy/champion_manager.py:current_competition_snapshot`), `engine/runtime/dashboard_weather_widgets.py` (get_weather_alert_summary / get_weather_effect_summary / weather snapshot), `engine/api/api_ops_handlers.py:339` (execution advisories), `engine/api/api_read_advanced.py:1497` (validation), `engine/api/api_dashboard_reads.py:203-229` (audit records). For each: wrap the query in the project's standard missing-table/Operational-error guard and return `{ok:true, rows:[], reason:"<table>_missing"}` (HTTP 200), mirroring the news/sentiment peer pattern. Add a regression test per endpoint asserting 200 + `reason` against a cold sim DB. Guardrail: do not change any mutation/auth behavior; keep safe-mode gates intact.

**FIX-2 — Repair `/api/jobs/history` read-connection visibility.**
Scope: `engine/runtime/locks_pg.py:275 read_job_history()`. The `connect(readonly=True)` reader cannot see `job_history` (833 rows present). Align the readonly reader's DB path / attach with the runtime writer (or fall back). Add a test that writes a job_history row then asserts `/api/jobs/history?name=…` returns it. Guardrail: read-only, no schema change beyond connection wiring.

**FIX-3 — Gate and bound `POST /api/operator/bootstrap` and `POST /api/operator/start`.**
Scope: `boot/operator_server.js` (bootstrap @ ~line of `/api/operator/bootstrap`, start @6064). Add `requireOperatorConfirmation(req,res,"operator.start",…)` (or a dedicated bootstrap action_id) before execution, matching the sibling restart/stop handlers; make the orchestration async/bounded; map start failures to the real reason instead of generic `request_failed`. Add a refusal test (empty body → 422) and a failure-mapping test. Guardrail: must remain non-bypassable; never enable live execution.

**FIX-4 — Alert ack/resolve existence guard.**
Scope: `engine/api/api_write.py` ack_alert (222-263) / resolve_alert (355+). SELECT-exists before INSERT; return 404 not_found for unknown ids (match shelve's stricter behavior). Test: ack/resolve of a non-existent id → 404, no lifecycle rows written.

**FIX-5 — broker_apply_orders connection-close robustness.**
Scope: `engine/execution/broker_apply_orders.py:2050-2055` + the `broker_sim_account_snapshot_invalid` path (engine/execution/broker_sim.py). Ensure the connection is not closed before `commit()`/`close()` on the account-snapshot-invalid branch; default null cash to start-cash. Test: run two broker_apply_orders against a shared Postgres concurrently and assert both complete (or one cleanly defers) without `Cannot operate on a closed database`.

**FIX-6 — Satisfy the production secret-source policy so `validate_repo --live` passes.**
Scope: deployment/runtime config (not source). `validate_repo --live` → `runtime_graph_check --mode startup` fails because `BACKUP_EVIDENCE_HMAC_KEY` (and ≥1 redacted secret) are not supplied via an approved source. Provide each via `<NAME>_FILE` pointing at a root-owned 0600 file (or `*_SECRET`/systemd cred/Docker secret) per `engine/runtime/config_schema.py:validate_production_secret_sources`. Re-run `python tools/validate_repo.py --live` and `python tools/runtime_graph_check.py --mode startup`; both must exit 0. Guardrail: do not weaken the policy or hardcode secrets; this is the gate working as intended.

---

### Audit hygiene
- Processes started by this audit (safe-sim dashboard/operator, paper-fills attempts, validators) were stopped; ports :8000/:4001/:8200 confirmed closed at shutdown; no live execution state armed.
- No secret values were written into this report.
- All raw inventories, per-route results, screenshots, and validator logs are under `var/tmp/full_system_e2e_audit/` (route_inventory.json, route_groups.json, operator_routes.txt, ui_controls_static.json, pages/*.png, resp_*.json, val_*.log, flow6_*.log, operator_sidecar_findings.md).
