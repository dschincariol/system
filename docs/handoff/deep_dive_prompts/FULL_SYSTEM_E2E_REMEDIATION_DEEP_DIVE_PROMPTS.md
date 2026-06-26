# Full-System E2E Remediation — Deep-Dive Implementation Prompts

> Source audit: `docs/handoff/verification/FULL_SYSTEM_E2E_PRODUCTION_READINESS_AUDIT_REPORT.md`
> (Git HEAD `afc1595`, 2026-06-25, safe/sim + paper/sim-fill). Verdict: **NO-GO** — 0 P0, 8 P1, 10 P2, 8 P3.
>
> Each section below is a **standalone, focused implementation prompt**. Hand one at a time to a fresh agent.
> Every prompt is scoped to one root cause, cites the audit evidence, and demands the optimal production fix
> (not a test/doc patch). Severity in the heading reflects the audit. Re-confirm the defect on the current
> working tree before fixing — the tree is dirty and line numbers may have shifted.
>
> Standing safety contract for ALL prompts: never enable a live-capital path, never weaken an auth/confirmation
> gate, never hardcode a secret, and keep `safe`/`paper` modes fail-closed. Re-prove broker=sim and
> `real_trading_allowed=false` after any change that touches execution, broker, or operator surfaces.

---

## E2E-R1 — `validate_repo --live` fails on the production secret-source policy (**P1 / production-config blocker**)

**Context.** The production validation gate `validate_repo --live` runs `tools/runtime_graph_check.py --mode startup`, which boots the runtime under production-like config. It is the canonical "is this deployable" gate and is also what `start_system.py paper` invokes (`_run_production_validation_gate`).

**Defect (confirmed).** `validate_repo --live` exits **1**. `runtime_graph_check` reports `FAIL engine.runtime.jobs_manager` / `runtime_bootstrap` → `ConfigError: production secret source policy invalid: secret_file_invalid:BACKUP_EVIDENCE_HMAC_KEY; use *_FILE, *_SECRET, systemd credentials, Docker Compose secrets, or root-owned 0600 files` (plus ≥1 further redacted secret). The `cold_boot_db` sub-check returns `exit_code:1`. The identical gate blocked booting the `.env.codex-sim-paper-fills.bak` profile via `start_system.py paper` during the audit. Evidence: `var/tmp/full_system_e2e_audit/val_validate_repo_live.log`.

**Root cause (confirm).** `engine/runtime/config_schema.py:validate_production_secret_sources` (≈ line 398), called from `load_runtime_config` (≈ line 787) via `engine/runtime/db_guard.py:resolve_db_path` (≈ line 53), rejects `BACKUP_EVIDENCE_HMAC_KEY` (and other flagged secrets) because they are not supplied through an approved source. The policy is **working as intended (fail-closed)**; the working-tree/deploy config does not satisfy it. `safe` mode passes only because the prepared safe env relocates secrets to approved `*_FILE` sources.

**Your task.** Determine the complete set of secrets the production gate requires and make the **deployment/runtime configuration** supply each via an approved source (`<NAME>_FILE` → root-owned `0600` file, `*_SECRET`, systemd credential, or Docker Compose secret) — wire this into the documented production/staging env templates (`.env.example`, `deploy/env/*.example`, `deploy/profiles/*`) and the operator secret-management path, not by weakening the policy. Confirm there is a first-class, documented way for an operator to provision `BACKUP_EVIDENCE_HMAC_KEY` and any peers. If the policy itself has a gap (e.g., it should accept an additional legitimate source, or its error should name *all* failing secrets at once instead of one), fix that in code with tests. Do not hardcode any secret value and do not relax the policy to make the gate pass.

**Falsify.** Provision the secrets via the approved mechanism, then run `python tools/runtime_graph_check.py --mode startup` and `python tools/validate_repo.py --live` and prove both exit 0. Prove that removing/mis-permissioning a required secret still fails the gate (policy still fail-closed). Prove no secret value is printed to logs or committed.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## E2E-R2 — Champion competition view returns HTTP 500 on both `/api/system/competition` and `/api/operator/competition` (**P1 / High**)

**Context.** The dashboard "model competition / marketplace" panel reads from a single handler exposed under two routes.

**Defect (confirmed).** Both `GET /api/system/competition` and `GET /api/operator/competition` return HTTP **500** on 3/3 retries: `{"detail":"OperationalError","error":"internal_server_error","reason_code":"handler_exception"}`. The competition view button is dead.

**Root cause (confirm).** Handler `api_get_competition_view` (`engine/api/api_system.py` ≈ line 2786) calls `current_competition_snapshot()` (`engine/strategy/champion_manager.py` ≈ line 1404), which raises a DB `OperationalError`. This is **not** a missing-table degraded state — the backing tables `model_competition_rankings`, `model_marketplace_scores`, and `champion_assignments` all EXIST in the sim DB (verified via operator `db_schema` have_tables). It is a SQL/handler bug (e.g., a bad join/column/query against an empty-but-present schema).

**Your task.** Find why `current_competition_snapshot()` raises against a present-but-empty schema and implement the optimal fix so the endpoint returns a correct 200 (populated snapshot when data exists; a clean, explicitly-labeled empty/degraded payload — e.g. `{ok:true, rankings:[], reason:"no_competition_data"}` — when it does not). The fix must live in the handler/snapshot production code, not in a try/except that hides the real query bug. Keep both route aliases behaving identically.

**Falsify.** Hit both routes authenticated against a cold sim DB and prove 200 with a coherent (possibly empty, explicitly-labeled) body. Seed a competition row and prove it appears. Prove no `OperationalError`/500 in either route.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## E2E-R3 — `/api/jobs/history` returns 500 "no such table: job_history" though the table exists with rows (**P1 / High**)

**Context.** The dashboard job-history viewer reads per-job history rows.

**Defect (confirmed).** `GET /api/jobs/history?name=<any>` returns HTTP **500** `{"detail":"no such table: job_history","error":"job_history_exception"}` for every job (verified inline; tested metrics_collector, ingestion_runtime, ingest_now, repair_schema, train_lgbm_models). Contradiction: `sqlite3 var/tmp/safe_sim_boot/db/trading.db "SELECT count(*) FROM job_history"` returns **833**, and `/api/db/health` row_counts also reports `job_history` populated. `/api/jobs/log` (same job) works — the failure is specific to history.

**Root cause (confirm).** `read_job_history()` (`engine/runtime/locks_pg.py` ≈ line 275) opens the DB with `connect(readonly=True)`, and that read-only connection resolves to a different DB path / attach than the runtime+health connection that can see `job_history`. The read-only reader cannot see the table the writer created.

**Your task.** Implement the optimal fix so the read-only job-history reader resolves the same database (path/attach/schema) as the writer and health connection. Prefer fixing the DB-path/connection resolution for read-only readers generally if the bug is shared (check sibling read-only readers), rather than a one-off in `read_job_history`. The job-history viewer must return real rows.

**Falsify.** Write a job_history row, then prove `/api/jobs/history?name=<that job>` returns it (HTTP 200). Prove `/api/jobs/log` still works. Confirm no other read-only reader regressed.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## E2E-R4 — `/api/copilot/ask` returns HTTP 500 for every input (**P1 / High**)

**Context.** The dashboard "Copilot / Ask" widget posts a question to `/api/copilot/ask`.

**Defect (confirmed).** Both an empty body and a well-formed `{"question":"why is the system degraded?"}` return HTTP **500** `{error:request_failed, meta.status:500}` deterministically (verified inline). The copilot widget surfaces a 500 to operators on every use, including the empty-box and no-LLM-configured cases that should be graceful.

**Root cause (confirm).** The handler at `dashboard_server.py` ≈ line 2298 returns `{ok:false, answer:"...", suggested_actions:[...]}` with no `error`/`reason`/`meta` for both the empty-question and unavailable-LLM branches. `engine/api/http_transport.py` `respond_json` → `_derive_response_status` → `_map_error_to_status('')` (≈ line 341/167) maps an `ok:false`-without-`error` payload to HTTP 500 and stamps `error=request_failed`.

**Your task.** Implement the optimal fix so: (a) an empty/blank question returns a graceful 4xx (e.g. 422 `missing_question`); (b) an unavailable/unconfigured LLM endpoint returns a graceful 200 (or 503) with a clear `reason` and a helpful `answer`/`suggested_actions`, not a 500; (c) a real internal failure still returns 500 with an actionable `error`. The fix should set an explicit `error`/`reason` on the handler payload so `http_transport` derives the right status — and consider whether `_map_error_to_status('')` defaulting `ok:false`-without-`error` to 500 is itself too aggressive for "graceful negative" responses.

**Falsify.** Prove empty body → 4xx with `reason`; well-formed question with no LLM configured → 200/503 with `reason` (not 500); and that a genuine handler exception still yields a 500 with a real `error`. Show the dashboard copilot widget no longer 500s on normal use.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## E2E-R5 — Weather subsystem 500s on missing tables instead of degrading (**P1 / High**)

**Context.** The dashboard weather panels read alert/effect/snapshot summaries.

**Defect (confirmed).** `GET /api/weather/alerts` and `GET /api/weather/effect` return HTTP **500** `{error:internal_server_error, detail:OperationalError, reason_code:handler_exception}` with or without params. `GET /api/weather/snapshot` returns **500** for index/ETF symbols **including the handler default SPY** (and QQQ), while ad-hoc equities/crypto (AAPL/MSFT/BTC-USD) return 200 zero-filled. The default/dashboard invocation crashes.

**Root cause (confirm).** `engine/runtime/dashboard_weather_widgets.py` `get_weather_alert_summary` / `get_weather_effect_summary` (and the snapshot path; default symbol `SPY` set in `engine/api/api_ops_handlers.py` ≈ line 523) issue `con.execute(SELECT ... FROM weather_alerts / model_weather_effect / weather_snapshot ...)` with no missing-table guard; those tables are MISSING in the sim DB. Peer subsystems handle the same condition gracefully (e.g. news/sentiment → 200 `ready:false reason:social_features_table_missing`).

**Your task.** Implement the optimal fix so all weather read endpoints degrade gracefully when their tables are absent (HTTP 200, `ready:false`, explicit `reason`, zero-filled/empty body), exactly mirroring the news/sentiment peer pattern — and ensure the default SPY path does not crash. Also confirm/repair schema bootstrap so these tables are created where they should exist. Choose the standard repo helper for missing-table detection rather than a bespoke try/except.

**Falsify.** Against a cold sim DB (tables absent) prove all three endpoints return 200 with `ready:false`+`reason`, including the no-symbol (SPY default) and QQQ cases. Create the tables and prove populated responses. Prove no `OperationalError`/500 on any weather route.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## E2E-R6 — `/api/operator/institutionalCheck` returns HTTP 500 request_failed (**P1 / High**)

**Context.** The operator panel's institutional-readiness check.

**Defect (confirmed).** `GET /api/operator/institutionalCheck` returns HTTP **500** `{configValid:false, error:request_failed, healthOk:false, meta.status:500, ok:false}` deterministically (3/3). The institutional-readiness panel cannot render.

**Root cause (confirm).** Identify the handler (dashboard `/api/operator/institutionalCheck` mounted route and its backing function) and the sub-check that raises; like the copilot case, an `ok:false`-without-`error` or an uncaught exception is being mapped to a generic `request_failed` 500.

**Your task.** Implement the optimal fix so the institutional check returns 200 with a structured, per-check pass/fail/degraded breakdown (surfacing *which* sub-check failed and why), reserving 500 strictly for genuine internal faults. The operator must be able to see the real blocker, not an opaque `request_failed`.

**Falsify.** Prove the route returns 200 with a populated per-check body in safe/sim, and that each failing sub-check is named with a reason. Prove a forced internal fault still yields an actionable 500.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## E2E-R7 — `/api/operator/clear_last_error` (snake_case) returns 404; only the camelCase alias is routed (**P1 / Medium-High**)

**Context.** The operator "Clear Error" control. The live UI calls the camelCase path, which works; the snake_case path is advertised but unrouted.

**Defect (confirmed).** `POST /api/operator/clear_last_error` → **404** `{error:unknown_endpoint}`. The working alias `POST /api/operator/clearLastError` → 200 (clears the in-memory job last_error). Any caller/integration/doc using the snake_case path hits a dead endpoint.

**Root cause (confirm).** Only `/api/operator/clearLastError` is registered (`engine/dashboard/routing.py` ≈ line 39; `engine/api/http_transport.py` ≈ line 466). The snake_case form has no route.

**Your task.** Decide the optimal canonical convention for operator routes and implement it consistently: either register the snake_case alias alongside camelCase (preferred if the rest of the operator API is snake_case) or remove the snake_case advertisement everywhere it appears so the contract is single-sourced. Audit the operator route table for **other** camelCase/snake_case alias mismatches and fix the class of bug, not just this instance.

**Falsify.** Prove both intended forms resolve (or that only one is advertised anywhere) and that the "Clear Error" control works. Grep the route registry + UI + docs and show no remaining advertised-but-unrouted operator paths.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## E2E-R8 — Operator `bootstrap`/`start` are ungated state-changing actions that 500 (**P1 / Medium-High**)

**Context.** Every other operator mutation (set_mode, stop, restart, restart_engine, restart_operator, emergency_stop, factoryReset, self_repair, autofix, repairSchema, promote_model) refuses an empty payload with **422 confirmation_required** via `requireOperatorConfirmation` (`boot/operator_server.js` ≈ line 1195). Two state-changing "start" actions break that contract.

**Defect (confirmed).** (1) `POST /api/operator/bootstrap` with an empty `{}` body executes a `StartupOrchestrator` run with **no confirmation gate**, blocks past the 30s default, and returns HTTP **500** `request_failed`. (2) `POST /api/operator/start` (operator_server.js ≈ line 6064) also has **no confirmation-token guard**, executes a real "Deterministic start" of `ingestion_runtime`, and returns HTTP **500** with top-level `error="request_failed"` — the real cause (`start_failed:ingestion_runtime:process exited rc=1`, preflight `prices too stale (SAFE blocked)`) is only nested in `result.errors/steps`. Both stayed in `mode=safe` with `real_trading_allowed=false` (no capital reach), so P1-not-P0, but they are asymmetric, ungated, slow, and error-opaque.

**Root cause (confirm).** Missing `requireOperatorConfirmation(...)` on the `bootstrap` and `start` handlers (and the backing dashboard `/api/operator/bootstrap` handler), plus generic top-level error mapping that hides nested failure reasons.

**Your task.** Implement the optimal fix: (a) add the appropriate confirmation gate (a dedicated `operator.start`/`operator.bootstrap` action_id with token + consequence_ack, consistent with siblings) so an unconfirmed call returns 422 and does nothing; (b) make these orchestrations async/bounded so they don't block the request past their timeout; (c) map start/bootstrap failures to a real top-level `error`/`reason` (e.g. surface `start_failed:ingestion_runtime` and the preflight reason) instead of generic `request_failed`. Keep them fail-closed and never live-capable.

**Falsify.** Prove empty `{}` → 422 (no execution) for both; prove a confirmed start in safe/sim either succeeds or returns a 4xx/5xx whose top-level `error` names the real cause; prove the request returns promptly (does not hang past the bounded timeout). Re-prove `real_trading_allowed=false` after a confirmed start.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## E2E-R9 — Inconsistent degradation contract: handled conditions return 500 instead of graceful 200/4xx (**P2 / Major**)

**Context.** The repo's convention is that a missing-table / known-bad-input read should degrade to a labeled 200 (or a 4xx for bad input), not a 500. Three endpoints violate this.

**Defect (confirmed).**
- `GET /api/execution/advisories` → HTTP **500** `{error:"no such table: execution_ai_advisory", failure_type:OperationalError}` while the sibling `/api/execution/diagnostics` reports the identical missing table as `state=unavailable / reason=execution_ai_advisory_missing` with HTTP 200. Handler `api_get_execution_advisories` (`engine/api/api_ops_handlers.py` ≈ line 339) catches the OperationalError but re-emits it as 500.
- `GET /api/validation` → HTTP **400** `{error:"no such table: validation_scores"}` — `get_validation_rows()` (`engine/api/api_read_advanced.py` ≈ line 1497) returns `str(e)` (raw SQL) directly.
- `GET /api/audit/records?table=alerts` → HTTP **500** uncaught `ValueError not_audit_table:alerts` for a syntactically-valid non-audit table; the SQL-injection path (`?table=foo;DROP`) correctly returns 400. In `api_get_audit_records` (`engine/api/api_dashboard_reads.py` ≈ line 203-229) only `require_allowed_table_name` is inside the try; `get_audit_records()` at ≈ line 229 raises `ValueError` from `engine/runtime/storage_pg.py:_audit_table_name` (≈ line 3716-3721) into the outer except → 500.

**Root cause (confirm).** Each handler maps a *handled, known* condition (missing table / disallowed-but-valid table name) to a 5xx or leaks a raw SQL string, instead of the standard degraded/validation contract.

**Your task.** Implement the optimal fix for all three so: missing-table reads degrade to HTTP 200 with `ok:true, rows:[], reason:"<table>_missing"` (matching `/api/execution/diagnostics`); a disallowed-but-valid table name returns 400 `not_audit_table` (not 500); and no raw SQL exception text is ever returned to clients. Factor a shared helper if the pattern repeats. Consider sweeping for the same anti-pattern across other read handlers and fixing the class.

**Falsify.** Against a cold sim DB prove advisories → 200 `reason:..._missing`, validation → 200 `{ok:true,rows:[]}` with no raw SQL, audit/records?table=alerts → 400 (and `?table=<injection>` still 400, and a real audit table still 200). Prove no 500 on any of these in normal authenticated use.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## E2E-R10 — Data-sources: builtin-delete refusal returns 500, and `auth.token_required:true` is self-declared but unenforced on reads (**P2 / Major**)

**Context.** `/api/data_sources` manages 38 sources; deletes are guarded by a typed `DELETE_SOURCE` confirmation. The endpoint's body advertises `auth:{token_required:true, actor_required:true}`.

**Defect (confirmed).** (1) `POST /api/data_sources/delete` of a builtin source with the correct confirmation returns HTTP **500** `{error:"builtin_source_delete_not_allowed:<key>"}`. The refusal is correct (source preserved, count stayed 38) but a known/handled policy refusal should be a 4xx (403/409), not a 500. (2) All `data` GET reads return full bodies with **no token**, even though `/api/data_sources` self-declares `token_required:true`. Verified: GET-open is **by design** — `engine/api/auth_config.py` is mutation-auth only (POST→401 enforced; GET→200 intentional on loopback). So the bug is the **misleading self-declaration** and an **open question about LAN mode**.

**Root cause (confirm).** (1) The builtin-delete guard raises/returns a 500 instead of a 4xx. (2) The `auth.token_required:true` field in the `/api/data_sources` payload does not reflect the actual mutation-only auth model; and it is unverified whether `TRADING_NETWORK_MODE=lan` (0.0.0.0 bind) gates *reads* or only mutations — if reads stay open under LAN, the 921KB config + provider templates are network-exposed.

**Your task.** (1) Make the builtin-delete (and any other handled data-source policy refusal) return the correct 4xx (403/409) with a clear reason, never 500. (2) Make `/api/data_sources` report an accurate auth posture (e.g. `mutation_token_required:true, read_open_on_loopback:true`) consistent with `auth_config.py`. (3) Determine LAN-mode read behavior; if reads are open under LAN bind, decide and implement the optimal posture (gate reads behind the token in LAN mode, or document explicitly that loopback-only reads are the supported deployment) — fail-closed for any network-exposed deployment.

**Falsify.** Prove builtin delete → 4xx (source preserved). Prove the advertised auth fields match real behavior (POST no-token→401, GET loopback→200). Prove the LAN-mode read decision with evidence (either reads require the token under `TRADING_NETWORK_MODE=lan`, or it is documented as loopback-only). No secret values exposed.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## E2E-R11 — Alert ack/resolve accept non-existent ids and write phantom audit records (**P2 / Major**)

**Context.** Alert lifecycle: ack / shelve / resolve. `shelve` is correctly strict; `ack`/`resolve` are not.

**Defect (confirmed).** `POST /api/alerts/999999/ack` → 200 `{ok:true, acked_ts_ms, expires_ts_ms}` and `POST /api/alerts/999999/resolve` → 200 `{ok:true, resolved_ts_ms}`, yet `/api/alerts` reports `total=0` and `/api/alerts/by_id?id=999999` → 404 `not_found`. Operators get a misleading "acknowledged/resolved" confirmation and orphan rows pollute the lifecycle audit.

**Root cause (confirm).** `ack_alert()` / `resolve_alert()` (`engine/api/api_write.py` ≈ lines 222-263 and 355+) blindly INSERT into `alert_acks` / `alert_resolutions` / `alert_lifecycle_events` with no SELECT-exists guard and return `ok:true`. `shelve_alert` already does the stricter check (422).

**Your task.** Implement the optimal fix so ack/resolve validate the alert exists before writing and return 404 `not_found` (matching `shelve`'s strictness) for unknown ids, writing no lifecycle/ack/resolution rows. Ensure idempotency for legitimate re-ack/re-resolve is preserved (don't break the valid path). Consider whether any phantom rows already written need a cleanup/migration.

**Falsify.** Prove ack/resolve of a non-existent id → 404 with zero rows written to `alert_acks`/`alert_resolutions`/`alert_lifecycle_events`; prove ack/resolve of a real alert still succeeds and is idempotent. Show row counts before/after.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## E2E-R12 — `broker_apply_orders` is not robust to concurrent DB access (premature connection close) (**P2 / Major**)

**Context.** The sim-fill path `engine/execution/jobs/broker_apply_orders.py` is proven correct in isolation (`tests/test_paper_mode_sim_fill_boot.py` passes: broker_fills/execution_fills/pnl_attribution/trade_attribution ≥1). Under concurrent Postgres load it fails.

**Defect (confirmed).** Running the paper sim-fill flow **concurrently with the full test suite** failed: `broker_sim_account_snapshot_invalid` (`cash_raw=None`, equity_raw=0.0) → `ProgrammingError: Cannot operate on a closed database` at `broker_apply_orders.py` ≈ line 2050 (`con.commit()`) and ≈ line 2055 (`con.close()`) → **0 fills written** (`status:"no_changes"`). The same run logged `execution_ai_advisor_safe_float_failed (NoneType)` and `no such table: execution_analytics`. In isolation the test passes. Production runs a single `broker_apply_orders`, so this is not a live-capital risk, but the premature-close-on-error-path is a latent bug.

**Root cause (confirm).** On the `broker_sim_account_snapshot_invalid` branch (`engine/execution/broker_sim.py` account snapshot), the connection is closed (or invalidated) before `broker_apply_orders.main()` reaches its `commit()`/`close()`, so those operations throw. Null `cash` in the account snapshot is also not defended (should default to start-cash).

**Your task.** Implement the optimal fix so the broker-apply job is resilient: (a) never operate on a closed connection — guard `commit()`/`close()` and the error path so a snapshot-invalid condition fails cleanly without the secondary `Cannot operate on a closed database` exceptions; (b) default/repair a null `cash` in the broker account snapshot to the configured start-cash (or fail with a clear, single error); (c) confirm the `execution_ai_advisor` NoneType float and missing `execution_analytics` table degrade gracefully rather than spamming warnings. The single-run happy path must remain correct.

**Falsify.** Run two `broker_apply_orders` against a shared Postgres concurrently and prove both complete (or one cleanly defers) with **no** `Cannot operate on a closed database` and no lost fills. Re-run `tests/test_paper_mode_sim_fill_boot.py` in isolation and prove it still passes (fills+attribution persisted). Prove a null-cash snapshot is defended.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## E2E-R13 — Empty read responses lack a "no data yet" / "table present" marker (**P2-P3 / Minor**)

**Context.** Several read endpoints return empty results in safe/sim, but unlike well-behaved peers (alpha_decay, regime/history, drift/explainer, news/sentiment) they provide no `reason`/`ready`/`table_present` field, so a client cannot distinguish "empty by design" from "broken/missing trail".

**Defect (confirmed).** `GET /api/promotion_audit` and `/api/promotion/audit` → `{data:[],ok:true}` (also governance/summary.audit, promotion/explain.audit) with no marker for table-present vs absent; `GET /api/relevance_stats` & `/api/relevance/stats` → `{ok:true,cached:true,stats:{}}`; `GET /api/market_stress_history` → `{ok:true,series:[]}`; `GET /api/strategy_metrics`, `/api/strategy/metrics`, `/api/causal/scores` → `data:[]` with `error:null`; `GET /api/size_policy` & `/api/strategy/size_policy` → `policy:null`. All correct for unseeded sim, but undiagnosable from the client.

**Root cause (confirm).** These handlers return empty payloads without the standard degraded-state diagnostic fields (`ready`, `reason`, and ideally a `table_present`/`source` flag).

**Your task.** Implement the optimal, consistent degraded-state marker across these endpoints (and any siblings sharing the gap), so an empty result always carries an explicit `ready:false`/`reason`/`source` (e.g. `no_promotions_yet`, `table_missing`, `size_policy_untrained`). Adopt the existing peer pattern (news/sentiment, drift/explainer) rather than inventing a new shape; the UI panels backed by these should be able to render an explanatory message instead of a blank.

**Falsify.** Prove each endpoint returns an explicit reason/ready marker when empty, and that the marker distinguishes "table missing" from "table present but empty". Confirm populated cases still return data unchanged.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## E2E-R14 — Response-envelope correctness & defense-in-depth polish (**P3 / Low**)

**Context.** A cluster of small, non-blocking correctness issues in response envelopes, status codes, and guard layering.

**Defect (confirmed).**
- `GET /api/operator/runtime_watchdogs` returns HTTP 200 with `meta.status=200` and a fully-populated body, yet carries a spurious top-level `error:"request_failed"`, `ok:false` — a consumer checking the top-level field wrongly treats success as failure.
- `POST /api/operator/execution_arm` refusal returns outer HTTP **400** while the body has `meta.status:422` and `error:confirmation_required` (status mismatch; guard intact).
- `POST /api/notifications/test` returns HTTP **500** for `channel_not_configured` / `unknown_channel` (user/config conditions that should be 4xx; refusal itself is correct and side-effect-free).
- `POST /api/repair_schema` handler (`engine/api/api_self_repair.py` ≈ lines 74-82) has **no internal confirmation guard** (`del _parsed, body, ctx` then runs `repair_schema()`); safe only because the transport registry (`engine/api/http_transport.py` ≈ line 627) gates it (422 proven). Defense-in-depth gap: a future caller bypassing the transport wrapper would run an unconfirmed destructive_admin schema write.
- `GET /api/market/session` reports `state:OPEN` while `/api/market/candles` + `/api/prices` are `ready:false`/empty (correct degraded, but a UI may show "open market, no chart").

**Your task.** Fix each: (a) `runtime_watchdogs` must not set a top-level `error` when the body is healthy (derive `ok`/`error` from real status); (b) align `execution_arm`'s outer HTTP status with its body `meta.status` (422); (c) `notifications/test` config/input refusals → 4xx, not 500; (d) add an internal confirmation/assert guard inside `api_post_repair_schema` so the destructive write is gated even if the transport wrapper is bypassed (defense-in-depth, not a replacement for the registry); (e) decide and implement the right `market/session` vs no-data contract (e.g. annotate `data_ready:false` on session when candles/prices are unavailable). Keep all existing guards intact.

**Falsify.** Prove each endpoint's status code and envelope are now self-consistent (top-level `ok`/`error` match `meta.status`); prove `repair_schema` refuses an unconfirmed call both via transport AND via a direct handler-level guard; prove the refusals are side-effect-free.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.
