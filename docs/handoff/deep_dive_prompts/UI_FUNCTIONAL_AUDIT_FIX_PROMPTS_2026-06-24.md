# UI Functional-Audit Fix Prompts (2026-06-24)

These are scoped, one-at-a-time fix briefs derived from `docs/handoff/UI_FUNCTION_EXERCISE_AUDIT_2026-06-24.md`. Use exactly one prompt per implementation session so an agent can inspect, fix, test, and report without mixing unrelated UI risks. The repo path (`/home/david/gitsandbox/system/system`) and the sim/paper boot profile are repeated in each prompt on purpose — that redundancy is intentional so every prompt stands alone. Each prompt names an existing test to extend (failing before, passing after) and carries a safety-guardrail bullet that must not be relaxed.

## How to reproduce the audit environment

Boot the system under the sim/paper profile with `TRADING_ENV_FILE=.env.codex-sim-paper-db.bak` via `start_all.py` (this brings up the dashboard on `:8000` and the operator sidecar on `:4001`). Confirm the execution barrier reports `real_trading_allowed=false` before exercising anything — the profile sets `DISABLE_LIVE_EXECUTION=1`, `KILL_SWITCH_GLOBAL=1`, `LIVE_TRADING_REQUIRE_CONFIRMATION=1`, and `BROKER=sim`, so no mutation can reach a live broker. Read endpoints are open over loopback except the token-gated sensitive GETs (supply `X-API-Token` in the header, never the URL). The audit findings were reproduced with two harnesses under `var/tmp/`: a read-probe (`ui_audit_get_probe.py`) that classifies every read endpoint, and a headless-Chrome CDP harness (`ui_audit_browser.js`, `--remote-debugging-port=9222`) that collects console errors, uncaught exceptions, failed requests, and DOM/button state across `/ui/dashboard.html`, `/ui/data_sources.html`, and `/operator/`. Prompt 12 ports both into a permanent CI gate.

## Severity index

| Prompt | Title | Severity |
| --- | --- | --- |
| 1 | Attach API Token In The Dashboard/Operator Browser Client | HIGH |
| 2 | Fix Market-Data Quote Read 500 (Undefined ts_ms Column) | HIGH |
| 3 | Raise API Rate Limits So The Dashboard Does Not Throttle Itself | HIGH |
| 4 | Stop Surfacing Valid ok:false Payloads As request_failed Errors | MEDIUM |
| 5 | Return 200 With Pass/Fail For Institutional Check Instead Of 500 | MEDIUM |
| 6 | Bridge The Operator Telemetry WebSocket Through The :8000 Proxy | MEDIUM |
| 7 | Raise HTTP Server Backlog/Concurrency To Survive Multi-Panel Load | MEDIUM |
| 8 | Wire Or Remove Dead Dashboard Buttons | LOW |
| 9 | Gate The Operator Clear-Error Mutation Like Other High-Impact Actions | LOW |
| 10 | Reconcile Phantom API Routes That 404 At Runtime | LOW |
| 11 | Harden Broker Credential Transport In The Config Panel | LOW |
| 12 | Add An Automated UI Functional Smoke As A Permanent Regression Gate | CAPSTONE |

## Prompt 1 - Attach API Token In The Dashboard/Operator Browser Client

You are working in `/home/david/gitsandbox/system/system`. The repo is dirty; do not revert unrelated user changes.

Deep dive and fix the dashboard/operator browser client never sending an API token, which 401-storms every sensitive read. Current evidence: `ui/api_client.js` `fetchJSON()` (lines 18-58) builds `Headers` with only `Content-Type` (lines 22-25) and never adds `X-API-Token`; `ui/dashboard.js` imports `fetchJSON` (line 34) and calls it for `/api/system/state` (line 8112), `/api/execution/barrier` (line 8432), `/api/ingestion/status` (line 6924), `/api/promotion/status` (line 8855) with zero token references. By contrast `ui/data_sources.js` `request()` (lines 150-162) reads a localStorage session (`SESSION_STORAGE_KEY` line 11; `saveSession`/`loadSession` lines 109-131) and sets `X-API-Token` (line 156). Server-side, any GET under `/api/` not in `_PUBLIC_GET_ENDPOINT_PATHS` defaults to `sensitive` (`engine/api/http_transport.py` `_normalize_route_sensitivity` lines 653-673; allowlist lines 328-337 excludes all four endpoints), so with the production gate active the transport returns HTTP 401 `unauthorized` (line 1269) and rejects query-string tokens with `query_token_forbidden` (lines 1257-1264) — the token MUST travel in the `X-API-Token` header, never the URL. The server gate mechanism is proven by `tests/test_api_security_hardening.py::test_sensitive_gets_require_dashboard_token_in_production_and_redact` (401-without-token + 200-with-header on representative sensitive GETs) and the query-string rejection by `::test_sensitive_get_rejects_query_string_token_in_production`. Operator routes are proxied with `X-Operator-Token` injected server-side (`dashboard_server.py` line 1250), so the browser only needs `X-API-Token`.

Requirements:
- Add a token-provisioning path to `ui/api_client.js` `fetchJSON()` that attaches `X-API-Token` from a shared session store, mirroring the `data_sources.js` localStorage model. Extract a common session helper (e.g. a new `ui/session_store.mjs`) and have BOTH `api_client.js` and `data_sources.js` consume it rather than duplicating ad hoc logic.
- `ui/dashboard.html` currently has NO token-entry input (only `data_sources.html` exposes `tokenInput`/`actorInput`); add a token-entry affordance reachable from the dashboard UI (input/session, or a documented loopback exemption) so a fresh browser can authenticate; persist via localStorage, never via URL/query string.
- Token must never appear in logs, URLs, or thrown error messages; keep `cache: "no-store"` (line 27) and existing `allowBusinessFalse` semantics (lines 20-21, 38-55) intact.
- Regression test: add/extend an assertion in `tests/test_dashboard_ui_contract.py` (alongside `test_business_refusal_ui_helpers_surface_reason_codes`, line 191) proving `api_client.js` attaches `X-API-Token` from the shared session store and places NO token in URLs/query strings; it MUST fail before the fix and pass after.
- Safety guardrail: the change is read/auth-header-only client-side and must NOT weaken execution safety (kill switch, `DISABLE_LIVE_EXECUTION`, confirmation gates, fail-closed barrier) and must keep the server 401 gate authoritative — no client-side bypass, no localhost auto-token leak into logs/URLs. Verify under the sim/paper profile (`TRADING_ENV_FILE=.env.codex-sim-paper-db.bak`, `BROKER=sim`).

Suggested files to inspect:
- `ui/api_client.js`, `ui/dashboard.js`, `ui/data_sources.js`, `ui/dashboard.html`
- `engine/api/http_transport.py` (sensitive-GET 401 gate; `_normalize_route_sensitivity`, `_PUBLIC_GET_ENDPOINT_PATHS`, `_request_api_token_parts`), `engine/api/auth_config.py` (`dashboard_api_token_from_env`, line 60)
- Tests to extend: `tests/test_dashboard_ui_contract.py` (static JS-surface contract); server behavior already covered by `tests/test_api_security_hardening.py`

Done criteria:
- New/extended assertion in `tests/test_dashboard_ui_contract.py` proves `api_client.js` attaches `X-API-Token` from the shared session store and that no token is placed in URLs/query strings; it fails before the fix and passes after.
- A fresh browser load of `dashboard.html` (production token gate enabled) populates without a 401 storm; manual/headless check shows the four sensitive reads return 200 once the token is entered.
- Full targeted suite green: `pytest tests/test_dashboard_ui_contract.py tests/test_api_security_hardening.py -q`.

## Prompt 2 - Fix Market-Data Quote Read 500 (Undefined ts_ms Column)

You are working in `/home/david/gitsandbox/system/system`. The repo is dirty; do not revert unrelated user changes.

Deep dive and fix the market-data quote read returning HTTP 500 with PostgreSQL `column "ts_ms" does not exist`. Current evidence: `_fetch_timescale_quote_rows()` (`engine/runtime/price_read_router.py:491`) builds `SELECT ts_ms, last, volume FROM (SELECT {ts_ms_expr} AS ts_ms ... FROM <schema>.price_quotes ... ORDER BY {time_ref} DESC ...)`, where `ts_ms_expr = price_timescale_ts_ms_expr()` (`engine/runtime/price_timescale_schema.py:197`) projects `(EXTRACT(EPOCH FROM "time") * 1000)::BIGINT` and `time_ref = price_timescale_time_ref()` (line 191) orders by the `"time"` column. The canonical `price_quotes` spec (`price_timescale_schema.py:23-35`) has `time`/`last`/`volume` and NO `ts_ms` column, and `storage_pg_prices.py:1786-1790` inserts/conflicts on `("time", symbol, ...)` / `ON CONFLICT(symbol, "time")` (conflict key derived at `storage_pg_prices.py:1594-1599`). So the live 500 means the timescale branch executed against a relation still keyed on legacy `ts_ms` (the `price_quotes` -> `price_quotes_legacy_ts_ms` compat rename asserted in `tests/test_price_timescale_schema.py:294-296` never applied to that schema), so the inner `ORDER BY "time"` resolves against a table without a `"time"` column. NOTE: `fetch_quote_rows()` (`price_read_router.py:580`) already wraps the timescale fetch in try/except and falls back to `_fetch_sqlite_quote_rows()` (lines 583-594) — but ONLY when `_READ_FALLBACK_TO_SQLITE` (line 30) is truthy and without distinguishing the legacy-schema case, so it can still re-raise an `UndefinedColumn` 500. SECONDARY: the timescale branch ran even though the profile set `PRICE_READ_BACKEND=sqlite`, so confirm `get_price_read_backend()` (`price_read_router.py:183`) precedence.

Requirements:
- Confirm `get_price_read_backend()` returns `"sqlite"` whenever `PRICE_READ_BACKEND=sqlite` (`_read_backend_mode()` line 179 reading module global `_READ_BACKEND` line 29) BEFORE any timescale probing; the candles path (`api_market.py:94` `_fetch_quote_rows` -> `fetch_quote_rows`) must NOT enter the timescale branch under that profile.
- `_fetch_timescale_quote_rows()` must detect a legacy/`ts_ms`-keyed `price_quotes` relation (missing the `"time"` column) and fail closed by raising a typed error (not a bare `UndefinedColumn`) so that `fetch_quote_rows()` falls back to `_fetch_sqlite_quote_rows()` deterministically (honoring `_READ_FALLBACK_TO_SQLITE`) rather than surfacing a 500.
- The candles call chain (`api_market.py:94` -> `api_market.py:186` `api_get_market_candles` -> `engine/api/api_operator_handlers.py:739` `api_get_operator_market_data`, which dispatches `api_get_market_candles` at line 759) must return a 200 with candles (or an empty result), never `{detail: UndefinedColumn, reason_code: handler_exception}`.
- Regression test (fails before, passes after): one case stubs a `price_quotes` relation lacking `"time"` (legacy `ts_ms` PK) and asserts the candles chain returns 200/empty via sqlite fallback rather than raising `UndefinedColumn`; a second asserts `get_price_read_backend()` returns `"sqlite"` and `_fetch_timescale_quote_rows` is never invoked when `PRICE_READ_BACKEND=sqlite`.
- Safety guardrail: do not weaken execution safety (kill switch, `DISABLE_LIVE_EXECUTION`, confirmation gates, fail-closed barrier); the read-path fallback must fail closed (no silent wrong-schema reads — distinguish legacy-relation vs. genuine sqlite path) and stay verifiable under the sim/paper profile (`TRADING_ENV_FILE=.env.codex-sim-paper-db.bak`, `BROKER=sim`).

Suggested files to inspect:
- `engine/runtime/price_read_router.py` (`_fetch_timescale_quote_rows` 491, `fetch_quote_rows` 580, `get_price_read_backend` 183, `_read_backend_mode` 179, `_READ_BACKEND` 29, `_READ_FALLBACK_TO_SQLITE` 30, `_warn_nonfatal` 152)
- `engine/runtime/price_timescale_schema.py` (`price_timescale_ts_ms_expr` 197, `price_timescale_time_ref` 191, `price_quotes` spec 23-35)
- `engine/runtime/storage_pg_prices.py` (`price_quotes` insert/conflict 1786-1790, conflict-key derivation 1594-1599), `engine/api/api_market.py`, `engine/api/api_operator_handlers.py`
- Extend `tests/test_market_candles.py` (`test_timescale_quote_rows_query_bounds_newest_rows_and_returns_ascending` 59, `test_market_candles_timescale_time_schema_returns_populated_candles` 111, sqlite-backend pattern at line 297 `monkeypatch.setattr(..., "_READ_BACKEND", "sqlite")`); add a helper assertion mirroring `tests/test_price_timescale_schema.py:97`.

Done criteria:
- The two regression cases above fail before the fix and pass after.
- `python -m pytest tests/test_market_candles.py tests/test_price_timescale_schema.py -q` passes.
- No `UndefinedColumn` / `reason_code: handler_exception` from the candles chain (`/api/market/candles`, `/api/operator/market_data`) under the sim/paper profile.

## Prompt 3 - Raise API Rate Limits So The Dashboard Does Not Throttle Itself

You are working in `/home/david/gitsandbox/system/system`. The repo is dirty; do not revert unrelated user changes.

Deep dive and fix self-inflicted dashboard rate-limit throttling. Current evidence: `engine/api/rate_limit.py:13-15` sets `DEFAULT_TOKEN_LIMIT_PER_MIN=60`, `DEFAULT_IP_LIMIT_PER_MIN=10`, `DEFAULT_DESTRUCTIVE_LIMIT_PER_MIN=6`. `ApiRateLimiter.__init__` (lines 76-103) reads `TS_API_RATE_LIMIT_TOKEN_PER_MIN` and `TS_API_RATE_LIMIT_DESTRUCTIVE_PER_MIN` via `_env_int`, but `ip_limit_per_min` (line 80/92) is a plain constant with NO env override. `ApiRateLimiter.check()` (lines 122-154) buckets per `token:<value>` else per `ip:<value>`, so the browser, the operator sidecar refresh loop, and any tool sharing a token/IP share one bucket. `engine/api/http_transport.py:_rate_limit_protected_request` (line 1319) enforces on auth-denied GETs with `token_for_bucket=""` (line 1635, draining the IP bucket) and on protected GETs (line 1658), routing `_DESTRUCTIVE_ENDPOINT_PATHS` (line 351, incl. `/api/operator/emergency_stop`, `/api/terminal/order`) through the destructive limit. The dashboard fans out per refresh (`ui/dashboard.js` `Promise.allSettled` at lines 5190, 6549, 8670 over telemetry/risk/pnl/ui-metrics/decisions), and `boot/operator_ui.html` `CORE_REFRESH_MS = 5000` (line 1319) plus `boot/operator_server.js` proxy/scan loops (`OPERATOR_PROCESS_SCAN_CACHE_TTL_MS`, lines 1394-1396) run ~5s, so one authenticated load returns 429 `rate_limit_exceeded`.

Requirements:
- Raise `DEFAULT_TOKEN_LIMIT_PER_MIN` to at least 600 and `DEFAULT_IP_LIMIT_PER_MIN` enough to absorb one full unauthenticated load fan-out; keep `DEFAULT_DESTRUCTIVE_LIMIT_PER_MIN` strict (unchanged or stricter) and keep routing `_DESTRUCTIVE_ENDPOINT_PATHS` through the destructive limit.
- Give loopback/operator-internal proxy traffic its own budget or an explicit exemption (net-new: `check()`/`_client_ip` has none today) so the sidecar cannot drain the browser's token bucket; the exemption must be loopback-scoped via `_is_loopback_ip` (line 720) and MUST NOT relax limits for remote IPs.
- Keep env overrides working: `TS_API_RATE_LIMIT_TOKEN_PER_MIN` and `TS_API_RATE_LIMIT_DESTRUCTIVE_PER_MIN` must still take precedence via `_env_int`; if you make the IP limit configurable, do so consistently (don't silently drop the existing token/destructive knobs).
- Optionally coalesce/stagger polling cadence, but the raised limits alone must let one authenticated load (all panels) complete with zero 429.
- Safety guardrail: do not weaken execution safety — destructive/mutation endpoints (kill switch, `/api/operator/emergency_stop`, `/api/terminal/order`, etc.), `DISABLE_LIVE_EXECUTION`, confirmation gates, and the fail-closed barrier keep strict limiting; verify under the sim/paper profile (`TRADING_ENV_FILE=.env.codex-sim-paper-db.bak`, `BROKER=sim`).

Suggested files to inspect:
- `engine/api/rate_limit.py` (defaults 13-15, `__init__` 76-103, `check` 122-154)
- `engine/api/http_transport.py` (`_rate_limit_protected_request` 1319; enforcement 1635 and 1658; `_DESTRUCTIVE_ENDPOINT_PATHS` 351; `_client_ip` 1130 / `_is_loopback_ip` 720)
- `ui/dashboard.js` (fan-out 5190, 6549, 8670)
- `boot/operator_ui.html` (`CORE_REFRESH_MS = 5000`, line 1319)
- `boot/operator_server.js` (proxy endpoints ~570, scan TTL 1394-1396)
- `tests/test_api_security_hardening.py` (extend near `test_sensitive_gets_are_rate_limited_when_protected` line 591 and `test_destructive_http_rate_limit_returns_429_retry_after` line 709)

Done criteria:
- A new regression test simulating one dashboard fan-out (request count >= the panel total) passes at the raised `DEFAULT_TOKEN_LIMIT_PER_MIN` with no 429, and FAILS against the old `=60` (assert before fix, pass after); use the existing `ctx={"API_RATE_LIMITER": ...}` injection and an injected `clock`.
- A test proves destructive endpoints still 429 after `DEFAULT_DESTRUCTIVE_LIMIT_PER_MIN` requests, and a loopback-budget/exemption test proves loopback proxy traffic does not consume the authenticated token bucket while a remote IP stays capped at `DEFAULT_IP_LIMIT_PER_MIN`.
- `TS_API_RATE_LIMIT_TOKEN_PER_MIN` / `TS_API_RATE_LIMIT_DESTRUCTIVE_PER_MIN` overrides still apply; targeted tests fail before and pass after. Confirm under `TRADING_ENV_FILE=.env.codex-sim-paper-db.bak`, `BROKER=sim`.

## Prompt 4 - Stop Surfacing Valid ok:false Payloads As request_failed Errors

You are working in `/home/david/gitsandbox/system/system`. The repo is dirty; do not revert unrelated user changes.

Deep dive and fix endpoints that return a fully populated payload alongside `ok:false`, whose own domain reason then gets masked as a generic hard error ("fake-red"). Current evidence: in `engine/api/http_transport.py`, `respond_json` (def line 1028) unconditionally clobbers the envelope at line 1046 — when `obj.get("ok")` is falsy it sets `obj["error"] = str(obj.get("error") or "request_failed")`, so a degraded-but-populated payload (e.g. competition view) loses its `reason`/`reasons` and is reported as `request_failed` even though `_derive_response_status` already carved status to 200. `_derive_response_status` (def line 290) returns `_map_error_to_status("")` == 500 for an empty/unmapped error code (line 148-149) UNLESS `_looks_like_state_payload` (def line 128) matches via `_STATE_PAYLOAD_HINT_KEYS` (line 86) or >=3 canonical keys — a populated payload that carries `degraded:true`/`reason` but lacks those keys still maps to 500. Real producers: `api_get_competition_view` (`engine/api/api_system.py:2742`) returns `_snapshot_response(snapshot, ok=bool(competition_health.get("ok")), competition={...})` (full data + `ok:false`); `api_get_runtime_config` (`engine/api/api_system.py:2270`) on `ConfigError` returns `{**snapshot, "ok": False, "status": "DEGRADED", "reasons": [...], "config": None, "error": str(e)}`. Client (`ui/api_client.js:53-54`) already throws on `data.ok === false` unless `allowBusinessFalse`; dashboard read-panels already pass `allowBusinessFalse:true` (e.g. `ui/dashboard.js:8186,8196`) but then render generic "unavailable" text (line 684 `"Readiness: unavailable"`, line 891 `"Health snapshot is unavailable..."`) instead of the carried `reason`/`reasons`.

Requirements:
- In `respond_json`, do not overwrite an existing non-empty `error` and do not synthesize `"request_failed"` when the payload is degraded-but-populated (carries `degraded:true` or a non-empty `reason`/`reasons`/`reason_code` alongside real data); preserve the original domain reason and keep HTTP 200 for that case.
- Extend `_derive_response_status`/the state carve-out so a degraded-but-populated payload (`degraded:true` or non-empty `reason`/`reason_code`) maps to 200, not 500; genuine errors (empty payload, mapped/non-empty error codes like `missing_*`, `execution_blocked`, `pre_trade_rejected`) keep their existing 4xx/5xx behavior.
- Update the affected dashboard read-panel renderers in `ui/dashboard.js` (health/readiness; and the runtime-config/competition panels if they are surfaced) to display the carried `reason`/`reasons` instead of generic "unavailable"; do not add a throwing loader where `allowBusinessFalse:true` is already used.
- Add a regression test that fails before the fix and passes after.
- Safety guardrail: this is a display/status-derivation fix only — it must not weaken execution safety (kill switch, `DISABLE_LIVE_EXECUTION`, confirmation gates, fail-closed barrier), must not turn any real error or `execution_blocked`/`pre_trade_rejected` refusal into a silent success (those keep their non-empty error and 4xx status), and must be verifiable under the sim/paper profile (`TRADING_ENV_FILE=.env.codex-sim-paper-db.bak`, `BROKER=sim`).

Suggested files to inspect:
- `engine/api/http_transport.py` (`respond_json` 1028-1051 / clobber at 1046, `_derive_response_status` 290-315, `_looks_like_state_payload` 128-143, `_STATE_PAYLOAD_HINT_KEYS` 86-104, `_map_error_to_status` 146-209)
- `engine/api/api_system.py` (`api_get_runtime_config` 2270-2305, `api_get_competition_view` 2742-2779, `_snapshot_response` 389)
- `ui/api_client.js` (38-57), `ui/dashboard.js` (health/readiness panel renderers ~684, ~891; reason/reasons rendering)
- `tests/test_http_transport_status_contract.py` (extend this; existing degraded-state-200 case at line 28)

Done criteria:
- A degraded-but-populated payload (`{"ok": False, "degraded": True, "reason": "...", "config": {...}}` or `{"ok": False, "reasons": ["config_error:..."], "config": None, ...}`) yields `_derive_response_status(...) == 200`, and `respond_json` does NOT overwrite `error` with `"request_failed"` nor drop the payload's `reason`/`reasons`.
- Genuine error payloads (empty, or `error` in {`missing_name`, `jobs_manager_unavailable`, `request_timeout`, `execution_blocked`, `pre_trade_rejected`}) still map to their existing 4xx/5xx codes; existing tests in `tests/test_http_transport_status_contract.py` still pass.
- New/extended targeted test in `tests/test_http_transport_status_contract.py` fails before the fix and passes after.

## Prompt 5 - Return 200 With Pass/Fail For Institutional Check Instead Of 500

You are working in `/home/david/gitsandbox/system/system`. The repo is dirty; do not revert unrelated user changes.

Deep dive and fix the operator Institutional Check returning HTTP 500 in the normal warming-up / no-feed state. Current evidence: `engine/api/api_operator_handlers.py:817-821` `api_get_operator_institutional_check` returns `{"ok": bool(readiness.get("ok")) and bool(health.get("ok")), "configValid": bool(readiness.get("ok")), "healthOk": bool(health.get("ok"))}`; when feeds are absent both readiness and health are false, so `ok` is false. `engine/api/http_transport.py:307-315` `_derive_response_status` returns the default 200 only when `ok` is truthy (line 307) or `_looks_like_state_payload` is true (line 310); this payload has only `ok`/`configValid`/`healthOk` (none in `_STATE_PAYLOAD_HINT_KEYS` at line 86, and fewer than the 3 canonical keys required at lines 135-143), so it falls through to `_map_error_to_status("")` which returns 500 (line 149). AUDIT: `/api/operator/institutionalCheck` returns 500 `{configValid:false, healthOk:false, ok:false}` while warming up, so `runInstitutional` in `boot/operator_ui.html:1593` shows a hard error rather than a pass/fail readout.

Requirements:
- Change `api_get_operator_institutional_check` so a completed-but-failing check returns HTTP 200 with a structured pass/fail body: keep `ok`/`configValid`/`healthOk`, add a machine-readable `reasons` list (e.g. `config_invalid`, `health_unavailable`), and force 200 classification. The reliable mechanism is `_derive_response_status`'s explicit-status path (lines 296-305): set `meta.status` (or `status_code`) to 200 on the completed-check payload. Note that adding `reasons` alone does NOT satisfy `_looks_like_state_payload` (it is only 1 of the 3 required canonical keys), so do not rely on key-count classification.
- Reserve `ok:false` → 500 only for genuine exceptions: the existing `institutional_check_handlers_unavailable` branch at line 812 MUST stay an error (no `meta.status:200` there); do not convert real handler-resolution failures into silent 200s.
- Do not change the meaning of `ok`: it must remain `configValid AND healthOk` (i.e. `readiness.ok AND health.ok`) so the UI pass/fail logic (`boot/operator_ui.html:1608` `if(j.ok)`) is unaffected.
- Safety guardrail: this is a read-only diagnostics endpoint — the fix must not weaken execution safety (kill switch, `DISABLE_LIVE_EXECUTION`, confirmation gates, fail-closed barrier) and must not turn any error or blocked-execution payload into a false 200 (keep the `institutional_check_handlers_unavailable` branch a 500); verify under the sim/paper profile (`TRADING_ENV_FILE=.env.codex-sim-paper-db.bak`, `BROKER=sim`).

Suggested files to inspect:
- `engine/api/api_operator_handlers.py` (`api_get_operator_institutional_check`, lines 807-821)
- `engine/api/http_transport.py` (`_derive_response_status` 290-315, `_looks_like_state_payload` 128-143, `_STATE_PAYLOAD_HINT_KEYS` 86, `_map_error_to_status` 146-149)
- `boot/operator_ui.html` (`runInstitutional` 1593-1612; pass/fail branch at 1608)
- `tests/test_operator_monitoring_contracts.py` (already imports `_derive_response_status` at line 8; see the `ok:false` → 200 pattern at line 47 and the degraded-payload contract at line 190)

Done criteria:
- A new regression test in `tests/test_operator_monitoring_contracts.py` calls `api_get_operator_institutional_check` with stubbed `api_get_readiness`/`api_get_health` handlers in `ctx` returning `{"ok": False}`, asserts the result carries `ok:False`, `configValid:False`, `healthOk:False`, a non-empty `reasons` list, and that `_derive_response_status(result) == 200`. This test fails before the fix (currently derives 500) and passes after.
- A second assertion confirms the all-green case (stubbed handlers return `ok:True`) still yields `ok:True`, empty/absent `reasons`, and `_derive_response_status == 200`; and that the `institutional_check_handlers_unavailable` branch (missing handler in `ctx`) still derives 500.
- `python -m pytest tests/test_operator_monitoring_contracts.py` passes.

## Prompt 6 - Bridge The Operator Telemetry WebSocket Through The :8000 Proxy

You are working in `/home/david/gitsandbox/system/system`. The repo is dirty; do not revert unrelated user changes.

Deep dive and fix the operator telemetry WebSocket failing to connect via the dashboard-proxied `/operator/` path. Current evidence: in `dashboard_server.py`, `_handle_operator_console_compat` (defined at line 1291) intercepts `path == "/operator/ws/operator"` at lines 1304-1315 and returns a hardcoded HTTP `426` (`{"error": "websocket_proxy_deferred"}`, header `Upgrade: websocket`, `X-Operator-Console-Bridge: 1`) via `_operator_send_json` instead of upgrading; `_operator_proxy_http` (line 1181) forwards HTTP only via `urllib_request`, and the server is `_ReusableThreadingHTTPServer(ThreadingHTTPServer)` with a `Handler(SimpleHTTPRequestHandler)` (`engine/api/http_transport.py:28,879,2069-2074`) with no upgrade path. The Node sidecar serves the stream at `/ws/operator` (`boot/operator_server.js:8097-8135`, `WebSocket.Server({server, path:"/ws/operator", verifyClient})` where `verifyClient` calls `operatorMutationAuthorized` and rejects with `done(false,403,"operator_forbidden")`). `_operator_sidecar_status_payload` (line 812) advertises `websocket.proxy_enabled: False`. In `boot/operator_ui.html`, `operatorTelemetryWsUrl()` (lines 938-949) targets `${OPERATOR_BRIDGE_PREFIX}/ws/operator` and passes the token only as the `operator_token` query param (browsers cannot set `X-Operator-Token` on `new WebSocket`); `startTelemetryStream()` (line 992) opens it, so headless-Chrome logs `WebSocket connection to ws://127.0.0.1:8000/operator/ws/operator failed`. Result: operator Live-PnL/telemetry never streams via :8000.

Requirements:
- Make `/operator/ws/operator` on :8000 reach the sidecar `/ws/operator` telemetry stream end-to-end. Either (a) implement a real WS upgrade proxy in the dashboard handler that bridges the client socket to `_operator_sidecar_ws_url()` (line 809) and forwards the operator token (the inbound `operator_token` query param and/or `X-Operator-Token` header) so the sidecar's `operatorRequestToken`/`verifyClient` passes; or (b) point `operatorTelemetryWsUrl()` directly at :4001 only when LAN mode is active, with a code comment justifying the exposure and falling back to the bridge otherwise.
- Remove or repurpose the unconditional `426` deferral at lines 1304-1315 so the proxied path no longer hard-fails; keep token-gated read access consistent with `_operator_require_bridge_read_gate` (line 1084).
- Update `_operator_sidecar_status_payload` so `websocket.proxy_enabled`/`deferred_reason` no longer advertise the deferred contract once the route works.
- If proxying, preserve frame integrity (text + close), pass `Sec-WebSocket-*` handshake headers, and tear down both sockets cleanly on either side closing.
- Safety guardrail: the bridge must remain fail-closed and read-gated (`verifyClient`/operator token still enforced, no anonymous telemetry, no new mutation surface); the change must not weaken the kill switch, `DISABLE_LIVE_EXECUTION`, confirmation gates, or the LAN-token requirement, and must be verifiable under the sim/paper profile (`TRADING_ENV_FILE=.env.codex-sim-paper-db.bak`, `BROKER=sim`).

Suggested files to inspect:
- `dashboard_server.py` (`_handle_operator_console_compat` 1291, `_operator_proxy_http` 1181, `_operator_sidecar_ws_url` 808-809, `_operator_sidecar_status_payload` 812-872 incl. `websocket` block 825-832, `_operator_require_bridge_read_gate` 1084, wrapper `_wrap_operator_console_routes`)
- `boot/operator_server.js` (`startTelemetryWebSocket` 8097-8135, `operatorRequestToken` 1320-1339, `operatorMutationAuthorized` 1341-1347), `boot/operator_ui.html` (`operatorTelemetryWsUrl` 938-949, `startTelemetryStream` 992-1035)
- `engine/api/http_transport.py` (import line 28, `Handler` 879, `_ReusableThreadingHTTPServer`/`httpd` 2069-2074 — upgrade handling)
- `tests/test_operator_console_bridge.py` (rewrite `test_operator_websocket_bridge_returns_deferred_upgrade_response` at line 529 to assert the new connect/upgrade or documented direct-:4001 contract); cross-check `tests/test_dashboard_http_lan_behavior.py`, `tests/test_operator_ui_polling_safety.py`, and the `proxy_enabled is False` assertions at lines 205, 302, 525.

Done criteria:
- `/operator/ws/operator` via :8000 establishes the telemetry stream (or the UI connects the documented LAN-only direct path); no `426 websocket_proxy_deferred` on the happy path.
- A regression test in `tests/test_operator_console_bridge.py` fails before the fix and passes after: stand up a stub WS sidecar, drive the dashboard handler, and assert a successful upgrade/frame round-trip (or assert the UI/status contract now advertises the working route instead of `proxy_enabled: False`).
- `python -m pytest tests/test_operator_console_bridge.py` passes; no unrelated route contracts regress.

## Prompt 7 - Raise HTTP Server Backlog/Concurrency To Survive Multi-Panel Load

You are working in `/home/david/gitsandbox/system/system`. The repo is dirty; do not revert unrelated user changes.

Deep dive and fix the dashboard HTTP server refusing connections under modest concurrency. Current evidence: in `engine/api/http_transport.py`, `run_http_server(host, port, handler_cls)` (lines 2067-2092) defines `_ReusableThreadingHTTPServer(ThreadingHTTPServer)` (lines 2069-2071) that sets only `allow_reuse_address`/`daemon_threads` and never sets `request_queue_size`, so it inherits the stdlib `socketserver.TCPServer` default of `5`. `dashboard_server.py` binds via this helper at line 6042 (`_HTTPD = run_http_server(...)`) and serves at line 6213 (`_HTTPD.serve_forever()`). AUDIT: the :8000 listener showed socket backlog 5; the read-probe plus a 3-page browser reload run simultaneously produced `503 Service Unavailable` (`candles`, `data_sources`, `training_status`) and a block of connection resets (ERR_ABORTED / connect failures), while the same endpoints return 200 sequentially.

Requirements:
- Raise the accept backlog on `_ReusableThreadingHTTPServer` by setting `request_queue_size` to a sane, env-overridable value (default 128, via `DASHBOARD_HTTP_BACKLOG`), and confirm the server stays threaded (`ThreadingHTTPServer`, `daemon_threads = True`) so requests are handled concurrently, not serially.
- Bound thread growth if needed (cap concurrent handler threads or document why unbounded `ThreadingHTTPServer` is acceptable here) so a burst cannot exhaust resources.
- Add a regression/load test that calls the REAL `run_http_server` (not a hand-rolled `ThreadingHTTPServer` subclass) against a trivial handler and fires a realistic multi-panel burst (>= 16 simultaneous GETs) at it, asserting zero connection refusals/resets and all responses succeed. This test MUST fail before the fix and pass after.
- Re-check whether `training_status` (route `GET /api/training_status` in `engine/api/system/route_specs.py` -> `api_get_training_status`, payload assembled in `engine/api/api_system.py` near line 2570) still returns intermittent 500/503 once backlog is fixed; if a residual race remains, fix it or document it precisely in the test.
- Safety guardrail: this is a transport/backlog change only — it must NOT weaken execution safety (kill switch, `DISABLE_LIVE_EXECUTION`, confirmation gates, fail-closed barrier) or relax any auth/mutation gating, and must be verifiable under the sim/paper profile (`TRADING_ENV_FILE=.env.codex-sim-paper-db.bak`, `BROKER=sim`).

Suggested files to inspect:
- `engine/api/http_transport.py` (`run_http_server`, `_ReusableThreadingHTTPServer`, lines 2067-2092)
- `dashboard_server.py` (lines 6042 bind, 6213 serve path)
- `engine/api/system/route_specs.py` (line 47, `training_status` route), `engine/api/api_system.py` (line ~2570, `training_status` payload)
- `tests/test_dashboard_route_contracts.py` and `tests/test_dashboard_http_lan_behavior.py` (already drive the real `run_http_server`; extend one, or add `tests/test_http_transport_concurrency.py` if neither fits)
- `tests/test_http_transport_status_contract.py` (note: spins up its own `_TestHTTPServer`, NOT `run_http_server`)

Done criteria:
- The targeted concurrency test fails before the fix (refusals/resets under a 16+ simultaneous-request burst) and passes after it.
- `_ReusableThreadingHTTPServer.request_queue_size` is verifiably > 5 (env-overridable via `DASHBOARD_HTTP_BACKLOG`) and the server is confirmed threaded.
- No multi-panel refresh burst yields 503/ERR_ABORTED for read endpoints in the test; the safety guardrail above holds (no weakened gates, verifiable under `.env.codex-sim-paper-db.bak`, `BROKER=sim`).

## Prompt 8 - Wire Or Remove Dead Dashboard Buttons

You are working in `/home/david/gitsandbox/system/system`. The repo is dirty; do not revert unrelated user changes.

Deep dive and fix dead dashboard buttons that render but have no click behavior. Current evidence (verify; line numbers may shift): ten button ids exist in `ui/dashboard.html` — `btnRefresh` (L2757), `btnExpertUnlock` (L2835), `btnRunPipeline` (L3060), `btnRefreshJobs` (L3067), `btnClear` (L3072), `btnOperatorMode` (L3076), `btnRunChallenger` (L3793), `btnReloadCalib` (L3903), `btnShowJobHistory` (L4745), `btnShowTrends` (L4746). In `ui/dashboard.js`, `btnRunPipeline` is only `getElementById`'d for state/display (L6915) with no `addEventListener`, and `btnRefresh` is merely `.click()`-ed by the `btnOpRefresh` handler (L9247) yet itself has no bound handler; `btnOperatorMode`/`btnExpertUnlock` are referenced only as label-text setters in `ui/policy.js` (L87, L93); the remaining six (`btnRefreshJobs`, `btnClear`, `btnRunChallenger`, `btnReloadCalib`, `btnShowJobHistory`, `btnShowTrends`) have zero JS references, so clicking them is a no-op. Confirmed: `grep -rn '/api/pipeline' ui/ tools/` returns nothing — no such route exists; do NOT invent one for `btnRunPipeline`.

Requirements:
- For each of the ten ids: either bind a real click handler to an EXISTING behavior (`btnRefresh`→`refresh()`; `btnRefreshJobs`/`btnClear`/`btnRunChallenger`/`btnReloadCalib`/`btnShowJobHistory`/`btnShowTrends`→their intended already-implemented action or toggle; `btnRunPipeline`→an already-registered pipeline endpoint **if one exists**, otherwise remove the button) OR remove the button from `ui/dashboard.html`. Label-only setters (`btnOperatorMode`, `btnExpertUnlock`) must gain a real toggle handler or be removed.
- Any newly wired endpoint MUST resolve through an existing registered dashboard route (validated by `find_unregistered_endpoint_references` in `tools/check_dashboard_ui_contract.py`); no invented backend paths. If no registered endpoint backs a button, remove the button rather than fabricate a route.
- Add a static UI assertion that every `<button id="...">` in `ui/dashboard.html` has a registered handler: a `click` `addEventListener` in `ui/dashboard.js`/`ui/policy.js`, an `onclick` attribute, a `data-action` delegation match, or is explicitly allowlisted in-test with a documented reason.
- Safety guardrail (UI-only): do not weaken or bypass the kill switch, `DISABLE_LIVE_EXECUTION`, confirmation/expert-unlock gates, or the fail-closed execution barrier; any wired action (especially `btnRunPipeline`) must keep server-side mutation blocks intact and remain verifiable under the sim/paper profile (`TRADING_ENV_FILE=.env.codex-sim-paper-db.bak`, `BROKER=sim`).
- Add a regression test in `tests/test_dashboard_ui_contract.py` that enumerates the ten ids and asserts each has a bound handler or has been removed; it MUST fail before the fix and pass after.

Suggested files to inspect:
- `ui/dashboard.html`, `ui/dashboard.js`, `ui/policy.js`
- `tools/check_dashboard_ui_contract.py` (add a button-handler collector alongside `collect_dashboard_asset_graph` at L100; reuse the endpoint machinery `collect_dashboard_endpoint_references`/`find_unregistered_endpoint_references` for any wired route)
- `tests/test_dashboard_ui_contract.py` (extend; existing coverage incl. `test_dashboard_html_js_surface_static_smoke`, `test_dashboard_ui_api_paths_are_registered_or_documented`, and its in-test allowlist pattern)

Done criteria:
- The new regression test enumerates the ten ids, fails before the fix, passes after.
- No remaining `<button id>` in `ui/dashboard.html` lacks a handler/`onclick`/`data-action` match unless the in-test allowlist documents why.
- Full `tests/test_dashboard_ui_contract.py` passes; no new unregistered-endpoint contract violations.

## Prompt 9 - Gate The Operator Clear-Error Mutation Like Other High-Impact Actions

You are working in `/home/david/gitsandbox/system/system`. The repo is dirty; do not revert unrelated user changes.

Deep dive and fix the unguarded operator Clear-Error mutation so it follows the same confirmation/lock model as every other operator state mutation. Current evidence: in `boot/operator_ui.html`, `clearLastError()` (lines 2223-2226) fires `jpost("/api/operator/clearLastError")` immediately with no `operatorMutationConfirmation(...)` call (contrast `repairSystem` at line 2201 routing through `operator.self_repair`, and `factoryReset` at line 2228 through `operator.factory_reset` + `adminWritesEnabled()` guard) and no guard; the "Clear Error" button (line 622) has NO `id` attribute and is absent from the `lockedWrites` list in `applyMonitoringSafetyLocks` (lines 963-985). In `boot/operator_server.js`, the `app.post("/api/operator/clearLastError", ...)` handler (lines 6186-6189) calls `clearLastError()` (defined lines 1632-1635, nulls `state.lastError` then `saveState()`) and returns `jsonOk` with no `requireOperatorConfirmation(...)` call; the JS gate is the `OPERATOR_CONFIRMATION_REGISTRY` (line 941) consumed by `requireOperatorConfirmation` (line 1190). The dashboard-transport mirror `_CONFIRMATION_REGISTRY` in `engine/api/http_transport.py` (lines 377-507+) also has no `/api/operator/clearLastError` entry. It is the only latched-state operator mutation with no consequence-ack.

Requirements:
- Choose ONE consistent treatment and apply it end to end: either (a) add a low-severity structured confirmation: register `operator.clear_last_error` in `OPERATOR_CONFIRMATION_REGISTRY` (`boot/operator_server.js`) with `require_ack` and a clear `consequence`, mirror the `/api/operator/clearLastError` entry in `_CONFIRMATION_REGISTRY` (`engine/api/http_transport.py`) following the existing `action_id`/`severity`/`consequence`/`require_ack` shape, call `requireOperatorConfirmation(req, res, "operator.clear_last_error", ...)` in the server handler, and route the UI button through `operatorMutationConfirmation("operator.clear_last_error", ...)`; OR (b) explicitly classify it as a low-risk display-state reset: give the button (line 622) an `id`, add it to the `lockedWrites` list in `applyMonitoringSafetyLocks`, gate `clearLastError()` via `adminWritesEnabled()`, and document the exemption-from-structured-confirmation in code comments next to BOTH the UI function and the server handler.
- Keep the chosen behavior consistent with the existing confirmation model (reuse the existing helpers/registries; no new framework).
- Do not alter the unrelated mutations or their existing confirmation contracts.
- Add a regression test (below) that fails before the fix and passes after.
- Safety guardrail: the fix must not weaken execution safety (kill switch, `DISABLE_LIVE_EXECUTION`, confirmation gates, fail-closed barrier) and must remain verifiable under the sim/paper profile (`TRADING_ENV_FILE=.env.codex-sim-paper-db.bak`, `BROKER=sim`); strengthen, never bypass, the confirmation model.

Suggested files to inspect:
- `boot/operator_ui.html` (`clearLastError`, `applyMonitoringSafetyLocks`/`lockedWrites`, `operatorMutationConfirmation`, button at line 622)
- `boot/operator_server.js` (`/api/operator/clearLastError` handler, `requireOperatorConfirmation`, `OPERATOR_CONFIRMATION_REGISTRY`)
- `engine/api/http_transport.py` (`_CONFIRMATION_REGISTRY`)
- `tests/test_operator_server_admin_contract_static.py` (extend this static contract suite, matching its `_extract_block` + `assert ... in text` idiom)

Done criteria:
- A new targeted test in `tests/test_operator_server_admin_contract_static.py` asserts the chosen contract for `/api/operator/clearLastError` via static text assertions (e.g. for (a): the handler block contains `requireOperatorConfirmation(req, res, "operator.clear_last_error"` and the UI/JS registry contains the `operator.clear_last_error` action_id; for (b): the button has an `id` present in `lockedWrites` and the exemption comment exists); it fails before the fix and passes after.
- `python -m pytest tests/test_operator_server_admin_contract_static.py -q` passes (full file).
- The "Clear Error" UI action no longer mutates latched error state with zero confirmation/lock inconsistent with the rest of the console.

## Prompt 10 - Reconcile Phantom API Routes That 404 At Runtime

You are working in `/home/david/gitsandbox/system/system`. The repo is dirty; do not revert unrelated user changes.

Deep dive and fix two advertised API routes that 404 at runtime because their handlers are never registered. Current evidence: `GET /api/allocator/status` is declared in `engine/api/system/route_specs.py:46` (`ROUTE_SPECS_SYSTEM`, handler `api_get_allocator_status`) and `GET /api/model/lifecycle` is declared in `engine/api/api_ops.py:16` (`ROUTE_SPECS`, handler `api_get_model_lifecycle`). Both handler functions exist (`api_get_allocator_status` at `engine/api/api_system.py:4035`; `api_get_model_lifecycle` at `engine/api/api_ops_handlers.py:150`, exported at `api_ops_handlers.py:623`), but neither name appears in the `API_HANDLERS` dict in `dashboard_server.py:5633`. The raw catalog `_RAW_ROUTE_SPECS` (`dashboard_server.py:2720`, 258 entries) is normalized into `ROUTE_SPECS` (`dashboard_server.py:2763`), then filtered by `engine/dashboard/routing.py` (`filter_route_specs_for_handlers`, called at `dashboard_server.py:5833`) down to 205 — silently dropping both specs, so they resolve to no handler and return the default 404. (Note: `GET /api/operator/broker_risk` returning 404 is EXPECTED — it is POST-only; do not touch it.)

Requirements:
- Register `api_get_allocator_status` and `api_get_model_lifecycle` in `dashboard_server.py` `API_HANDLERS` (importing them the way existing handlers are imported), so both routes resolve to their real handlers; OR, if intentionally unsupported, remove the two specs from `route_specs.py` / `api_ops.py`. Prefer registration since both handlers exist and are already wired into route specs.
- Add a regression test that, for EVERY `(method, path)` in the assembled pre-filter raw catalog (`dashboard_server._RAW_ROUTE_SPECS` normalized via `engine.dashboard.routing.normalize_route_specs`), asserts the named handler is present and callable in `dashboard_server.API_HANDLERS` — i.e. no spec is silently dropped by `filter_route_specs_for_handlers`. This must explicitly cover `/api/allocator/status` and `/api/model/lifecycle`.
- Do not weaken the existing `test_route_specs_integrity` (post-filter) check at `tests/test_dashboard_route_contracts.py:873`.
- Safety guardrail: this is a read-only routing/registration fix; it must NOT alter or bypass the kill switch, `DISABLE_LIVE_EXECUTION`, confirmation gates, or the fail-closed execution barrier, and must be verifiable under the sim/paper profile (`TRADING_ENV_FILE=.env.codex-sim-paper-db.bak`, `BROKER=sim`). Do not register any POST/execution route whose handler does not already exist.

Suggested files to inspect:
- `engine/dashboard/routing.py` (`normalize_route_specs:164`, `build_raw_route_specs:197`, `filter_route_specs_for_handlers:210`)
- `dashboard_server.py` (`_RAW_ROUTE_SPECS:2720`, `ROUTE_SPECS` normalize `:2763`, post-filter `:5833`, `API_HANDLERS:5633`)
- `engine/api/system/route_specs.py:46`, `engine/api/api_ops.py:16`, `engine/api/api_system.py:4035`, `engine/api/api_ops_handlers.py:150`
- `tests/test_dashboard_route_contracts.py` (`test_route_specs_integrity:873`; existing pre-filter pattern already uses `dashboard_server._RAW_ROUTE_SPECS` at lines 1063/1136 — extend alongside)

Done criteria:
- The new test FAILS before the fix (phantom `(GET, /api/allocator/status)` and `(GET, /api/model/lifecycle)` flagged as unregistered handlers) and PASSES after.
- `dashboard_server.ROUTE_SPECS` (post-filter) contains both paths after the fix, and a live request via `http_transport.build_handler(...)` (as used at `tests/test_dashboard_route_contracts.py:609`/`652`) to each returns a non-404 status under the sim profile.
- `python -m pytest tests/test_dashboard_route_contracts.py` passes.

## Prompt 11 - Harden Broker Credential Transport In The Config Panel

You are working in `/home/david/gitsandbox/system/system`. The repo is dirty; do not revert unrelated user changes.

Deep dive and fix the cleartext broker-credential transport in the dashboard config panel. Current evidence: `engine/api/api_broker_config.py` accepts plaintext credential dicts in request bodies — `api_post_broker_config` reads `body.get("credentials")` (line 328) and persists via `encrypt_credentials` (line 335), and `api_post_broker_test_connection` reads `payload.get("credentials")` (line 366); reads are masked via `mask_credentials` (line 220) and the audit row persisted at line 350 stores only `credentials_supplied`, while audit GET scrubs `detail.pop("credentials", None)` (line 412). The UI `ui/dashboard.html` exposes `brokerConfigCredentials` (textarea, line 4153) with `brokerConfigTestBtn`/`brokerConfigActivateBtn` (lines 4154-4155), posted by `brokerConfigFormPayload` (`ui/dashboard.js` line 5897) through `brokerConfigTestConnection` (line 6068) and `brokerConfigActivate` (line 6078). A loopback gate exists in `engine/api/http_transport.py` but only as handler-class methods (`_is_loopback_ip` line 720, `_is_loopback_bind_host` line 1209) — they are NOT importable, and the `_ctx` passed to handlers is the server-level ctx (carries `DASHBOARD_HOST`, line 1192), NOT per-request client IP. The transport is plain HTTP with no TLS/scheme metadata, so `api_broker_config.py` cannot see the request scheme and never gates credential bodies; `docs/LAN_ACCESS.md` documents no TLS requirement for credentials (line 159 only covers CORS).

Requirements:
- In both credential-accepting handlers, reject plaintext credential bodies when the configured bind host (`_ctx["DASHBOARD_HOST"]`, falling back to env `DASHBOARD_HOST`) is non-loopback AND TLS is not asserted. Replicate the loopback-host check locally (e.g. `ipaddress.ip_address(host.strip("[]")).is_loopback` plus `localhost`/`::1`), since the http_transport helpers are class methods. Treat TLS as present only when an explicit opt-in is set (new env flag, e.g. `BROKER_CREDENTIALS_TLS_TERMINATED=1`, documented as "TLS terminated at a trusted reverse proxy"). On rejection return `{"ok": false, "error": "credentials_require_tls", "meta": {"status": 403}}` (matching the repo idiom `forbidden_localhost_only`) instead of storing the secret. Credential-free config/test writes must still succeed on a LAN bind.
- Guarantee credential values never appear in any audit `detail_json`, log line, or GET/test response: keep the persisted audit dict at line 350 free of raw `credentials` (it already emits `credentials_supplied`), and scrub `credentials` from any error/echo/`_audit` path (e.g. the `activation_blocked` detail). Only `credentials_supplied`/masked forms may leave the handler.
- Document the TLS-for-credentials requirement in `docs/LAN_ACCESS.md`: non-loopback broker-credential writes require TLS termination at a reverse proxy and the `BROKER_CREDENTIALS_TLS_TERMINATED` opt-in.
- Add a regression test in `tests/test_broker_config_api.py` asserting raw credential values never appear in GET responses, test-connection responses, or audit detail, and that a simulated non-loopback bind without the TLS opt-in (pass a `_ctx` with `DASHBOARD_HOST=0.0.0.0`) is rejected with `credentials_require_tls`.
- Safety guardrail: the fix must not weaken execution safety (kill switch, `DISABLE_LIVE_EXECUTION`, confirmation gates, broker-test-before-activation gate, fail-closed barrier) and must be verifiable under the sim/paper profile (`TRADING_ENV_FILE=.env.codex-sim-paper-db.bak`, `BROKER=sim`).

Suggested files to inspect:
- `engine/api/api_broker_config.py` (lines 220, 328, 335, 350, 366, 412)
- `engine/api/http_transport.py` (`_is_loopback_ip` line 720, `_is_loopback_bind_host` line 1209, `_ctx`/`DASHBOARD_HOST` line 1192 — confirm these are class methods, not importable)
- `ui/dashboard.html` (broker config panel, lines 4152-4155), `ui/dashboard.js` (`brokerConfigFormPayload` line 5897, `brokerConfigTestConnection` line 6068, `brokerConfigActivate` line 6078)
- `docs/LAN_ACCESS.md`
- `tests/test_broker_config_api.py` (extend; existing masking/activation tests live here)

Done criteria:
- Raw credential strings are absent from every read/test/audit response and from logs; the new guard rejects non-loopback-bind credential writes that lack the TLS opt-in, while credential-free writes and loopback/TLS credential writes still succeed.
- The targeted test in `tests/test_broker_config_api.py` fails before the fix and passes after.
- Encrypted-at-rest storage and masking behavior are unchanged for valid loopback/TLS requests.

## Prompt 12 - Add An Automated UI Functional Smoke As A Permanent Regression Gate

You are working in `/home/david/gitsandbox/system/system`. The repo is dirty; do not revert unrelated user changes.

Deep dive and fix the gap that the UI is not functionally gated in CI, by adding a headless UI functional smoke that runs as a permanent regression gate. Current evidence: the entire UI audit was run from ad-hoc, non-CI harnesses under `var/tmp/`. `var/tmp/ui_audit_get_probe.py` GETs every read endpoint (catalog `ENDPOINTS` lines 11-71) with `X-API-Token` and classifies each via `classify()` (lines 80-100): `AUTH` for 401/403, `RATE_LIMITED` for 429, `FAIL` for >=500, `MISSING`/`CLIENT_ERR` for other 4xx, `REQ_FAILED` when `"request_failed"` is in the body `error`, and `HONEST_DEGRADED` only for `ok:false` whose base path is in the `HONEST_READINESS` whitelist (set literal lines 73-79). `var/tmp/ui_audit_browser.js` drives headless Chrome over CDP (port 9222) collecting `consoleErrors`/`exceptions`/`failedReq`/`httpErrors` (declared line 44, populated by the handlers lines 45-61), a DOM control count + error-banner probe (lines 70-84), and a screenshot, across `/ui/dashboard.html`, `/ui/data_sources.html`, and `/operator/` (`PAGES` lines 7-11). Note line 68: the harness explicitly *cannot* detect dead buttons (it only counts controls), so a real listener/wiring assertion is new work. Findings F1-F11 (401 storm, 500s, 429 throttling, `request_failed` masking, dead buttons, WS failure) reproduced via these harnesses but none are gated: `package.json` `check:ui` (line 18) runs `tools/run_ui_checks.mjs`, which is static asset/contract + node `--test` only (`NODE_TESTS` lines 11-31, `PYTEST_UI_TESTS` lines 32-43; steps built lines 271-282) and never boots the server or a browser.

Requirements:
- Add a CI-runnable UI smoke (e.g. `tools/ui_functional_smoke.py` plus a Chrome/CDP driver, porting the `classify()` semantics and the CDP collectors from the two `var/tmp/` reference harnesses) that boots the sim/paper profile by reusing `tools/safe_sim_boot_smoke.py` machinery (`prepare_safe_sim_env`, `_spawn` of `start_system.py safe`, `_wait_for_endpoint`, `_terminate_process`; dashboard port 8000, operator port 4001), then asserts every read endpoint returns no 401/403/429/5xx and no `request_failed`, allowing only the `HONEST_DEGRADED` `ok:false` set.
- The smoke itself must launch Chrome with `--remote-debugging-port=9222` (no existing tool does this — `tools/operator_startup_audit.js` is a plain `http.request` operator-endpoint checker, not a CDP harness), drive it over CDP, assert zero uncaught exceptions and zero console errors, assert the dashboard WebSocket connects, and assert primary navigation buttons are present AND wired (real listener/handler check, since the reference harness cannot per line 68) across `/ui/dashboard.html`, `/ui/data_sources.html`, and `/operator/`.
- The smoke must fail closed: if Chrome, the server, or the dashboard token is unavailable it must error (non-zero exit), never silently skip.
- Wire it into a new `npm run smoke:ui` script and reference it from `check:ui`/CI so a regression flips the gate red; skip gracefully only when Chrome is genuinely absent in a way CI detects (e.g. a CI-set env flag asserting Chrome presence), never inside CI itself.
- Add a regression test (fails before the smoke/fix exists, passes after) and a safety guardrail (below).
- Safety guardrail: the smoke is read-only and must not weaken execution safety — it must not disable the kill switch, `DISABLE_LIVE_EXECUTION`, `LIVE_TRADING_REQUIRE_CONFIRMATION`, or the fail-closed barrier (all set by `SAFETY_OVERRIDES` in `safe_sim_boot_smoke.py`), must issue no mutating requests, and must be verifiable under the sim/paper profile (`.env.codex-sim-paper.bak`, the `safe_sim_boot_smoke` default; the Postgres audit variant is `.env.codex-sim-paper-db.bak`) with `BROKER=sim`.

Suggested files to inspect:
- `var/tmp/ui_audit_get_probe.py` (reference read-endpoint probe + `classify()`, `HONEST_READINESS`)
- `var/tmp/ui_audit_browser.js` (reference CDP browser harness)
- `tools/run_ui_checks.mjs` and `package.json` (`check:ui` line 18)
- `tools/safe_sim_boot_smoke.py` (boot/spawn/wait/teardown machinery to reuse) and `tools/operator_startup_audit.js` (HTTP endpoint-check pattern, NOT CDP)
- `tests/test_safe_sim_boot_smoke.py` (extend) and `tests/test_pipeline_smoke_contract.py`

Done criteria:
- A new targeted test (extend `tests/test_safe_sim_boot_smoke.py` or add `tests/test_ui_functional_smoke.py`) fails before the smoke exists/regressions are caught and passes after, using a fake server that injects a 401/500/429/`request_failed` and asserting the smoke reports failure, plus a healthy-server case asserting it passes.
- The smoke classifies an injected 401/500/429/`request_failed` as a failure and an `HONEST_DEGRADED` `ok:false` (a path in `HONEST_READINESS`) as pass, matching the reference `classify()` semantics exactly.
- `npm run smoke:ui` exits non-zero on a seeded regression and zero on a clean sim/paper boot; the gate is invoked from `check:ui` or a make target.