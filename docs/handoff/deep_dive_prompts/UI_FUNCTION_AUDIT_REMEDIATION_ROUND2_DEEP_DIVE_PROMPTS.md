# UI & Function Exercise Audit — Remediation Round 2 Deep-Dive Prompts (UIA-R1 … UIA-R4)

Source: [`docs/handoff/UI_FUNCTION_EXERCISE_AUDIT_2026-06-25_REVERIFY.md`](../UI_FUNCTION_EXERCISE_AUDIT_2026-06-25_REVERIFY.md) — the re-verification after round 1 (UIA-1…UIA-11). Round 1 fully fixed F1, F6, F7, F9, F10, F11. These four prompts close the **remaining blockers**: F2 (not fixed), N1 (new — subsumes F4 + F5-audit), F3-residual (partial), F8-residual (partial). The current verdict is **NO-GO**; resolving UIA-R1 and UIA-R2 is required for GO.

**Global constraints (apply to every prompt):**
- Work from repo root `/home/david/gitsandbox/system/system`. Posture `ENGINE_MODE=EXECUTION_MODE=OPERATOR_MODE=safe`. Never place/modify/cancel a real order, hit a production broker, rotate secrets, or run destructive DB ops. Don't weaken any auth/confirmation guard.
- **A prior fix was already attempted for some of these and was insufficient.** First determine *why the previous attempt didn't take effect*, then implement the optimal solution. Don't re-apply the same idea.
- File:line anchors are from the re-verification and may have drifted — confirm by reading before editing.

---

## UIA-R1 — Safe-mode runtime cannot stay up: warmup/startup-health gate kills a feedless engine (F2, **P1 / High**)

**Context.** In safe/no-credential mode there are no live market-data feeds and the simulated provider does not emit a "first price tick" that the lifecycle treats as readiness. The runtime must remain up and serve the dashboard in this state (degraded-but-serving), not exit.

**Defect (confirmed, still failing after round 1).** Booting via the documented operator auto-start path: the dashboard binds in ~5 s, then the engine **self-terminates after ~2 min** and the operator restarts it — a **flap loop**. Evidence from one session: `warmup_timeout` ×33, `first_price_ts_ms:""` (a tick never arrives), 5 `startup_begin` cycles, operator `restartAttempts:1`/`consecutiveStartupFailures:1`. Round 1 (UIA-4) added owner-pid reaper scoping but did **not** make the runtime survive a feedless safe-mode warmup.

**Root cause (confirm, two candidate levers).** (1) Warmup watchdog: `engine/runtime/lifecycle.py:147` `warmup_timeout_ms` (default `WARMUP_TIMEOUT_S=120`); at `:201-209` `not first_tick && elapsed >= warmup_timeout_ms` transitions to `DEGRADED` (and the monitor can drive `SHUTTING_DOWN` at `:132`). (2) Startup health gate: `.env` sets `TRADING_STARTUP_HEALTH_TIMEOUT_S=45`, `TRADING_STARTUP_HEALTH_FAIL_OPEN=0`, `TRADING_STARTUP_HEALTH_ASYNC_BIND=1` — if post-bind health never reaches "ready" because no price tick arrives, the fail-closed startup health validation can fail the run and cause `start_system` to exit. Reproduce a single boot, capture the exact transition/log line that precedes process exit, and identify which gate fires (warmup watchdog vs startup-health validation vs supervisor).

**Your task.** Make a feedless **safe-mode** runtime **stay up and serve** indefinitely as `DEGRADED`/`WARMING_UP` without exiting, while preserving fail-closed behavior in live/production mode. Design the optimal solution: e.g. in safe/no-credential mode treat "no first price tick" as an expected degraded condition (serve, don't exit), make the startup-health gate fail-open or not require live price readiness in safe mode, and/or have the safe-mode simulated provider emit a real price tick so warmup completes legitimately. Do not just raise `WARMUP_TIMEOUT_S` (that only delays the flap). Keep the operator's supervision/restart semantics intact for genuine crashes.

**Falsify.** Boot via the documented operator auto-start path in safe mode and prove the engine **stays up ≥15 minutes with zero restart cycles** (`startup_begin` count stays at 1, operator `restartAttempts:0`) while the dashboard serves. Prove that in a live/production-mode configuration the fail-closed health behavior is unchanged (a genuinely unhealthy live boot still fails). State which gate was firing and why round 1 didn't fix it.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## UIA-R2 — `InFailedSqlTransaction` poisons pooled DB connections (N1 — subsumes F4 candles + F5 audit, **P1 / High**)

**Context.** Runtime DB access goes through a psycopg connection pool. When a query aborts a connection's transaction, that connection must be rolled back before reuse; otherwise the next request that checks it out fails with `current transaction is aborted, commands ignored until end of transaction block`.

**Defect (confirmed).** With round 1's candles column fix in place (`UndefinedColumn: ts_ms` is gone), `GET /api/market/candles?symbol=GLD` now **still 500s consistently (4/4)** with `InFailedSqlTransaction`, and `POST /api/broker/test_connection` returns `audit_error:"InFailedSqlTransaction"`, `audit_persisted:false`. The originating error is not surfaced near the abort — it is inherited from a poisoned pooled connection. So the candles **column** fix landed but the endpoint is still broken, and the broker audit write fails, both from the same transaction-hygiene defect.

**Root cause (confirm).** A `_rollback_if_in_transaction(conn)` helper already exists at `engine/runtime/storage_pool.py:401-406` (calls `conn.rollback()`), but it is evidently **not invoked on the error/release path**, so a connection left in an aborted transaction is returned to the pool and reused. Trace the acquire/release lifecycle in `engine/runtime/storage_pool.py` (and `engine/runtime/storage_pg.py`): determine where connections are returned to the pool, whether `rollback`/`reset` runs on every release (especially after an exception), and whether psycopg pool `reset`/`check` is configured. Also find the *first* query that aborts the transaction (enable surfacing of the originating error) so the upstream offender is fixed too, not just the symptom.

**Your task.** Implement the optimal connection-hygiene fix so a poisoned connection can never be reused: ensure every connection is rolled back / reset to a clean transaction state on release (and/or on acquire) — e.g. wire `_rollback_if_in_transaction` (or psycopg_pool's `reset=`) into the pool's release path, and ensure handler code uses a transaction scope that rolls back on exception. Separately, surface/log the originating SQL error that aborts the transaction and fix that query if it's a real bug. After the fix, `/api/market/candles` must return real candles and the broker `test_connection` audit must persist.

**Falsify.** Prove `GET /api/market/candles?symbol=GLD` returns **200 with real OHLCV candles** repeatedly (≥5/5), including right after deliberately triggering a failing query on the pool (so a reused connection would have been poisoned). Prove `POST /api/broker/test_connection` returns `audit_persisted:true` with no `InFailedSqlTransaction`. Add a regression test that runs a failing query then a valid query on the same pool and asserts the valid query succeeds. Do not "fix" by catching `InFailedSqlTransaction` and returning empty — the connection must actually be reset.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## UIA-R3 — Dashboard still has unauthenticated panels (401) and a rate-limit storm (429) in-browser (F3 residual, **P2 / Medium**)

**Context.** Round 1 (UIA-3) made `ui/api_client.js#fetchJSON` attach `X-API-Token` from `?token=`/localStorage, which fixed most panels (in-browser DOM "error" mentions dropped 782→74, 173 values populated). But not all calls go through that client.

**Defect (confirmed, residual).** Loading `ui/dashboard.html?token=<dtok>` in headless Chrome still shows **12× 401** (some calls remain unauthenticated) and **15× 429** (rate-limit storm). So some panels are still blank/error in a real browser.

**Root cause (confirmed).** Several UI modules call `fetch(...)` / `EventSource(...)` **directly and do not import the token-aware `api_client`**, so they send no token → 401. Confirmed offenders (modules that use `fetch`/`EventSource` without importing `./api_client`): `ui/copilot.js`, `ui/social_panels.js`, `ui/kill_switch_ui.js`, `ui/snapshot.js`, `ui/pro_chart_core.js`, `ui/pro_chart_engine.js`, `ui/weather_widgets.js`, `ui/voice.js` (`ui/data_sources.js` has its own token handling and is OK). `EventSource` (SSE, e.g. `/api/market/stream`) cannot set headers, so those must pass the token via `?token=`. Separately, the dashboard's on-load panel fan-out + polling still exceeds the **60/min per-token** budget (`engine/api/rate_limit.py`), producing the 429s.

**Your task.** Implement the optimal fix on both axes: (1) route every authenticated `/api/*` call (and `EventSource` stream) through the token-aware path so the `X-API-Token` header (or `?token=` for SSE) is always attached — either by migrating the direct-`fetch`/`EventSource` modules to the shared client, or by centralizing token attachment (a single `apiFetch`/`apiEventSource` helper) used everywhere. Identify the offending modules from the console log and the list above; don't miss any. (2) Eliminate the 429 storm — stagger/batch/coalesce the on-load panel fan-out and polling so an authenticated dashboard stays within the token budget, or raise/relax the same-origin rate limit appropriately (without weakening protection against real abuse).

**Falsify.** Re-load `ui/dashboard.html?token=<dtok>` in headless Chrome and prove **0× 401 and 0× 429** in the console (or document any intentional 401 such as a truly public probe), with panels populated. Prove an SSE stream (`/api/market/stream`) connects authenticated. Do not weaken server-side auth to make the UI pass.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## UIA-R4 — Noisy `SecretNotAvailable` PG-password warnings at boot (F8 residual, **P3 / Low**)

**Context.** Round 1 (UIA-8) added a file-backed PG-password fallback, and the DB now connects (the engine binds). But the boot is still noisy.

**Defect (confirmed, residual).** Startup logs **8×** `SecretNotAvailable: credentials_directory_missing` from `services/secrets/providers/systemd_creds.py` even though the file fallback then succeeds — the systemd-creds provider is tried first on a non-systemd host and its failure is logged at WARNING before the fallback resolves. This is harmless functionally but pollutes logs and obscures real secret problems.

**Root cause (confirm).** In `engine/runtime/platform.py` (`_load_pg_password:435`, `_load_pg_password_file:402`, `_load_pg_password_secret_ref:417`) the provider order / log level emits a WARNING-level failure for the systemd provider before falling back to the file source, when `CREDENTIALS_DIRECTORY` is absent.

**Your task.** Implement the optimal fix so a successful fallback is quiet: when `CREDENTIALS_DIRECTORY` is not set (non-systemd host), prefer the file-backed source first (or skip the systemd provider), or downgrade the "provider unavailable, falling back" message to DEBUG and only emit a WARNING/ERROR if **all** sources fail. Preserve a loud, actionable error when the password genuinely cannot be resolved from any source, and don't change behavior on systemd hosts where the provider is configured.

**Falsify.** Boot in safe mode on this (non-systemd) host and prove **0** `SecretNotAvailable`/`credentials_directory_missing` WARNING lines while the DB still connects. Prove that removing every password source still produces exactly one loud, actionable failure. Don't suppress the message globally (it must still fire when all sources fail).

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.
