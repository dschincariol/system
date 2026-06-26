# UI & Function Exercise Audit — Re-verification after UIA-1…UIA-11 remediation

**Date:** 2026-06-25
**Re-runs:** [UI_FUNCTION_EXERCISE_AUDIT_2026-06-24_v2.md](UI_FUNCTION_EXERCISE_AUDIT_2026-06-24_v2.md) (original master audit). Remediation prompts: `deep_dive_prompts/UI_FUNCTION_AUDIT_REMEDIATION_DEEP_DIVE_PROMPTS.md` (UIA-1…UIA-11).
**Method:** Verified each fix in production code, restarted the stack via the **documented operator auto-start path** (with `.env` re-quoted to exercise the F1 trigger), then re-exercised every previously-failing surface live. Posture: `ENGINE_MODE=EXECUTION_MODE=OPERATOR_MODE=safe`.

## Verdict: **NO-GO** — materially improved, two P1 blockers remain

The critical **startup blocker is resolved** (F1) — the documented operator path now boots the engine cleanly and the dashboard binds in ~5 s. **6 of 11 findings are fully fixed, 3 partial, 2 not fixed**, and one new systemic DB-transaction bug surfaced. The system still **cannot stay up** (F2) and a **core surface (price chart) is still broken** (F4), so it is not production-ready.

## Per-finding results

| # | Finding | Sev | Status | Evidence |
|---|---|---|---|---|
| **F1** | Operator `.env` quote crash | Critical | ✅ **FIXED** | New quote-aware parser `boot/operator_env_file.js`; `parseOperatorEnvText('TS_PG_DSN="host=…"')` → `host=…` (no leading quote). Documented auto-start booted; dashboard bound 200 in ~5 s; **no** `invalid connection option` crash. |
| **F2** | Runtime self-terminates | High | ❌ **NOT FIXED** | `warmup_timeout` ×33, `first_price_ts_ms:""` (no price tick ever arrives), 5 restart cycles. Engine dies ~every 2 min on the warmup watchdog; operator restarts it (`restartAttempts:1`) → **flap loop**, never stably up. |
| **F3** | Dashboard UI can't auth / self-throttles | High | 🟡 **PARTIAL** | Token now attached (DOM "error" mentions 782→**74**, **173** populated values, panels render). But in-browser still: **12× 401** (some calls remain unauthenticated) + **15× 429** (rate-limit storm). Auth + throttle axes only partly resolved. |
| **F4** | `/api/market/candles` 500 | High | ❌ **NOT RESOLVED** | `UndefinedColumn` is gone (column fix landed: `{ts_ms_expr} AS ts_ms`), but the endpoint **still 500s consistently (4/4)** with `InFailedSqlTransaction`. Price chart still broken. |
| **F5** | broker test_connection hang + live dial | Med-High | 🟡 **PARTIAL** | Fast, **no live socket** in safe mode (`state:safe_mode_live_probe_skipped`, `runtime_safe_mode_does_not_open_live_broker_socket`) ✅. But the audit write fails: `audit_error:InFailedSqlTransaction`, `audit_persisted:false`. |
| **F6** | Advertised-but-dead routes | Med | ✅ **FIXED** | `/api/allocator/status` & `/api/model/lifecycle` → **200** with real payloads; `data_sources/test_save` & `populate_now` → **401** (mounted + guarded, were 404). Fail-loud guard added (`engine/dashboard/routing.py:find_missing_route_handlers` raises). |
| **F7** | Operator proxy/health 502 | Med | ✅ **FIXED** | Now `operatorHealthProxyGet`; `/api/operator/proxy/health` → **200** with the real health payload (`healthy:false, degraded:true` — honest). |
| **F8** | PG password file fallback | Low-Med | 🟡 **PARTIAL** | DB connects (file fallback works — engine bound), but **8×** `SecretNotAvailable: credentials_directory_missing` warnings still logged at boot (systemd provider tried first, noisy). |
| **F9** | Options-DQ InvalidColumnReference | Low | ✅ **FIXED** | **0** `OPTIONS_DQ_DEGRADATION_EVENT_FAILED`/`InvalidColumnReference` this boot; column-existence guard added (`information_schema` check). |
| **F10** | "Fake-red" (honest `ok:false` shown as error) | Med | ✅ **FIXED (code)** | Client now has `businessDegradedReason`/`isBusinessDegradedPayload` and renders degraded states instead of throwing; observed honest `degraded:true` payloads. |
| **F11** | Operator telemetry WebSocket through proxy | Med | ✅ **FIXED (code)** | Operator UI now opens the WS against the `:4001` sidecar origin (`OPERATOR_WS_PORT=4001`, `/ws/operator`) with a bridge/same-origin fallback. (Live WS frame not re-tested.) |

## New finding (regression surfaced by the F4 column fix)

**N1 — `InFailedSqlTransaction` poisons pooled DB connections (P2, systemic).** With the candles column bug fixed, the underlying failure is exposed: a query aborts a pooled connection's transaction and is **not rolled back**, so later requests on that connection fail with `current transaction is aborted, commands ignored until end of transaction block`. This is the actual cause of the remaining F4 (candles 500) and the F5 audit-write failure. The originating error isn't surfaced near the abort — needs a connection-hygiene fix (rollback on error / per-request transaction reset in the pool). One fix likely clears both F4 and F5's audit failure.

## Production-readiness blockers (ranked)

1. **P1 — F2:** runtime can't stay up (warmup-timeout flap loop; no price tick in safe/no-feed mode). The warmup watchdog still hard-shuts-down a feedless safe-mode runtime (UIA-4 part-b ineffective).
2. **P1 — F4 + N1:** the price-chart endpoint still 500s; root cause is the un-rolled-back aborted transaction (N1), which also breaks the broker audit write (F5).
3. **P2 — F3 residual:** 12× 401 + 15× 429 in-browser — some panels still call unauthenticated and the on-load fan-out still trips the rate limiter.
4. **P3 — F8 residual:** noisy `SecretNotAvailable` boot warnings (functionally harmless; DB connects).

## Bottom line

Big step forward: the documented startup is unblocked (F1) and 6/11 findings are fully closed, with the operator/health/routes surfaces now honest and working. But **GO requires F2 (stay up) and F4/N1 (chart + transaction hygiene) resolved**, plus closing the F3 residual. Recommend re-issuing focused remediation for **F2**, **N1 (which subsumes F4 + F5-audit)**, and **F3-residual**, then re-running this verification.
