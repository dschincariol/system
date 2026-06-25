# UI & Function Exercise Audit — "click everything, prove it works"

**Date:** 2026-06-24
**Auditor:** Claude (automated functional audit)
**Repo:** `/home/david/gitsandbox/system/system`
**Profile started:** `.env.codex-sim-paper-db.bak` (sim/paper, Postgres-backed) via `start_all.py`
**Posture verified safe before any exercise:** execution barrier `real_trading_allowed=false`, `allow_simulation=false`, `mode=safe`; kill switch `armed=true` (`KILL_SWITCH_GLOBAL`); broker `active_broker=sim`, no live credentials; `DISABLE_LIVE_EXECUTION=1`. **No live or simulated order was placed; no destructive action was taken.**

> One deliberate deviation from default config: to obtain clean functional verdicts I restarted once with `TS_API_RATE_LIMIT_TOKEN_PER_MIN=100000` (see Finding **F3**). All other env was the unmodified sim/paper profile.

---

## 1. Executive Summary

Started the full stack (dashboard `:8000`, operator sidecar `:4001`; infra Timescale/Redis/MinIO already running). Inventoried **3 UI surfaces** (`dashboard.html`, `operator_ui.html`, `data_sources.html`), **133 interactive controls** on the dashboard alone, and a **complete route catalog of 209 dashboard routes (168 GET / 41 POST) + 87 operator routes**. Exercised every read endpoint at the API level, drove all three surfaces in **real headless Chrome** (with and without auth), and verified the guard behaviour on the highest-impact mutations.

**Verdict counts (read endpoints, clean sequential probe):**

| Verdict | Count | Meaning |
|---|---|---|
| PASS | ~87 | 200, valid payload |
| HONEST-DEGRADED | ~16 | 200 `ok:false` because runtime is `WARMING_UP` (no price feed) — correct behaviour |
| **FAIL (500)** | **3 consistent (+1 intermittent)** | real server errors |
| **MISLEADING (`request_failed` on valid data)** | **2** | data present but flagged as error |
| 400 needs-param (by design) | 3 | endpoint requires a query arg the UI always supplies |
| 404 phantom route | 2 | advertised in catalog, unregistered at runtime |

**Mutation guards: 5/5 fail-closed** (no-token → 401/403; no-confirmation → 422 `confirmation_required`). The structured-confirmation safety layer is solid.

### Top problems (most serious first)
1. **F1 — The browser UI never sends an API token.** With `LIVE_TRADING_REQUIRE_DASHBOARD_API_TOKEN=1` (the operator's real config), `api_client.js`/`dashboard.js` attach no `X-API-Token`, so every token-gated read returns **401**. Fresh-browser dashboard core panels (system state, execution barrier, market data, ingestion, promotion) and the **entire operator console** fail to load. *Proven:* injecting the token via the browser collapsed 40+ errors to ~0 and the pages fully populated.
2. **F2 — Market-data read path is broken (500 `UndefinedColumn: ts_ms`).** `/api/market/candles` (dashboard Pro Chart), `/api/market/stream`, and `/api/operator/market_data` all 500. The quote query selects a `ts_ms` column that does not exist in schema v73.
3. **F3 — API rate limits throttle the UI against itself.** Unauthenticated callers get **10 req/min per-IP**; authenticated **60/min per-token**, shared with the operator's own 5-second proxy loop. A single dashboard load fires far more than that → **429 storm**, panels blank.
4. **F4/F5 — "Fake-red": valid/honest responses surfaced as errors.** The transport injects `error:"request_failed"` (and sometimes HTTP 500) whenever a handler returns `ok:false`, even when the payload is fully populated; the client then *throws* on `ok:false`, so panels render "unavailable" despite valid data.
5. **F6 — Operator telemetry WebSocket is dead through the `:8000` proxy** (`/operator/ws/operator` fails to upgrade) → Live-PnL/telemetry stream never connects from the proxied console.

**The classic "fake-green" risk did not materialise** — the dashboard *honestly* shows "CAUTION / unavailable" when feeds are absent. The opposite failure mode appeared instead (F4/F5: honest data shown as errors).

---

## 2. Coverage

| Surface | Controls inventoried | Read endpoints exercised | Mutations | Browser-rendered | Notes / gaps |
|---|---|---|---|---|---|
| `ui/dashboard.html` (:8000) | 133 (83 buttons, 26 inputs, 17 selects, forms, tabs) | All canonical dashboard GETs (143 probed) | Guard-verified, **not triggered** | Yes (Chrome, w/ + w/o token) | 10 dead buttons; client-side tabs/sort/filters verified rendered |
| `boot/operator_ui.html` (:4001, proxied `/operator/`) | 46 buttons, 87 routes | Operator GETs (browser run, httpErrors 0 w/ token) | `stop`/`emergency_stop` guard-verified | Yes | Telemetry WS fails (F6) |
| `ui/data_sources.html` (:8000) | 11 endpoints, ~20 controls | `GET /api/data_sources` ✓ | **Not triggered** (would write provider config) | Yes | Delete/Reset confirmation verified statically |

**Explicitly NOT exercised, and why (all by design / safety):**
- **Live broker activation & live/sim order placement** — live-action guarded; instead verified the guards block (kill-switch + confirmation gate; sim order correctly returned `422 confirmation_required`).
- **Data-source create/update/delete/populate** — would mutate provider config; left untouched to avoid polluting state. Delete/Reset confirmation gating verified by code inspection.
- **Operator destructive actions** (factory reset, system update, backup, secret write, self-repair, AI patch apply/rollback) — destructive/admin; representative guards (`emergency_stop`, `stop`) verified to reject missing-confirmation/missing-token.
- **End-to-end simulated fill** — this profile arms `KILL_SWITCH_GLOBAL`, which freezes all order paths; the terminal-order route correctly blocked at the confirmation gate.
- **Per-alias coverage of all 209 routes** — canonical paths exercised; snake_case/slash aliases share the same handler and were treated as equivalent.

**Artifacts (evidence on disk):**
- Read-probe results: `var/tmp/ui_audit_get_results.tsv`, anomaly bodies: `var/tmp/ui_audit_anomaly_bodies.txt`
- Browser results: `var/tmp/ui_audit_browser_results.json`
- Screenshots: `var/tmp/ui_audit_shot_dashboard.png`, `…_operator.png`, `…_data_sources.png`
- Probes/harnesses: `var/tmp/ui_audit_get_probe.py`, `var/tmp/ui_audit_browser.js`, `…_tokened.js`

---

## 3. Findings

### F1 — Dashboard/operator client never attaches an API token → 401 on all gated reads · **HIGH** · CONFIRMED
- **Surface/endpoint:** every token-gated GET (e.g. `/api/system/state`, `/api/execution/barrier`, `/api/market/candles`, `/api/ingestion/status`, `/api/promotion/status`, all `/operator/api/*`).
- **Observed:** Fresh Chrome on `dashboard.html` → 401 storm (9+ distinct endpoints); `/operator/` → **all** operator calls 401, visible **error banner**, WS fails. `curl /api/system/state` = **401 without token, 200 with token**. Profile has `LIVE_TRADING_REQUIRE_DASHBOARD_API_TOKEN=1`.
- **Expected:** Authenticated reads, panels populate.
- **Proof it's the token:** re-running the browser with `X-API-Token`/`X-Operator-Token` injected via CDP dropped dashboard httpErrors 40+→2 and operator →0, pages fully populated (operator body 18 KB→532 KB).
- **Root cause:** `ui/api_client.js` `fetchJSON()` (lines ~18-31) builds headers with only `Content-Type` — no token. `ui/dashboard.js` contains **zero** token references. No token-entry affordance on the dashboard.
- **Recommended next step (confirmed fix path):** have `fetchJSON` attach the session token from a token store (mirror `data_sources.html`'s actor/token session), add a token-entry/login affordance to `dashboard.html`, **or** exempt loopback from the read-token requirement. *Diagnostic to confirm intended design:* check whether the deployment expects the operator "Open Dashboard" link to carry a token/cookie — if so, that handoff is not happening on loopback.

### F2 — Market-data reads 500 with `UndefinedColumn: ts_ms` · **HIGH** · CONFIRMED (reproduced 3×)
- **Endpoints:** `/api/market/candles` (dashboard Pro Chart), `/api/market/stream`, `/api/operator/market_data`.
- **Observed:** `500 {"detail":"UndefinedColumn","error":"internal_server_error","reason_code":"handler_exception"}`; raw PG error `column "ts_ms" does not exist … SELECT ts_ms, last, volume`.
- **Root cause:** `engine/runtime/price_read_router.py:491` `_fetch_timescale_quote_rows()` builds `SELECT {ts_ms_expr} AS ts_ms, last, volume FROM <schema>.price_quotes` where `price_timescale_ts_ms_expr()` assumes a `ts_ms` column absent from schema v73 (the prices tables use `last_trade_ts_ms`/`last_quote_ts_ms`/`last_update_ts_ms`, per `storage_pg_prices.py:1786`). Called via `api_market.py:94 fetch_quote_rows` → `api_get_market_candles` → `api_get_operator_market_data` (`api_operator_handlers.py:739`).
- **Secondary:** the **timescale** branch executed although the profile set `PRICE_READ_BACKEND=sqlite` — confirm read-backend routing precedence.
- **Recommended next step (confirmed fix path):** correct `price_timescale_ts_ms_expr()` / the quote SQL to reference the real time column(s) (e.g. `COALESCE(last_trade_ts_ms,last_quote_ts_ms,last_update_ts_ms)`), and add a contract test that runs the candles query against schema v73. Separately verify `get_price_read_backend()` honours `PRICE_READ_BACKEND`.

### F3 — API rate limits starve the UI's own fan-out · **HIGH** · CONFIRMED
- **Observed:** Browser dashboard load returns `429 rate_limit_exceeded` on telemetry/risk/pnl/ui-metrics/decisions; a single 143-endpoint probe at 52/min tripped 429 for ~70 endpoints because the operator's 5 s proxy loop shares the same token bucket.
- **Root cause:** `engine/api/rate_limit.py:13-15` — `DEFAULT_IP_LIMIT_PER_MIN=10` (unauth), `DEFAULT_TOKEN_LIMIT_PER_MIN=60` (auth). Buckets are per-IP / per-token; browser + operator-proxy + any tool share one bucket. A dashboard refresh fans out dozens of GETs, instantly exceeding 10/min unauth.
- **Recommended next step:** raise defaults substantially (token ≥ 600/min) and/or give operator-internal proxy traffic its own budget, exempt loopback, and **coalesce dashboard polling** (batch/stagger the per-panel GETs). Env knobs already exist: `TS_API_RATE_LIMIT_TOKEN_PER_MIN`, `TS_API_RATE_LIMIT_DESTRUCTIVE_PER_MIN`.

### F4 — Transport marks `ok:false` payloads as `request_failed`; client then throws → panels show "unavailable" despite valid data · **MEDIUM** · CONFIRMED
- **Endpoints:** `/api/system/config`, `/api/system/competition` (both return a **populated** payload but `ok:false` + injected `error:"request_failed"`); the same masking affects honest-degraded `health`/`readiness`/`operator_summary`/`barrier`.
- **Root cause:** `engine/api/http_transport.py:1043-1046` injects `error="request_failed"` whenever `ok` is falsy; `ui/api_client.js:54-56` `throw`s when `data.ok === false` (unless `allowBusinessFalse`). Net effect: a 200 with real data is treated as a hard error by the UI (visible as "Health unavailable / Readiness unavailable" on the dashboard screenshot).
- **Recommended next step:** don't overwrite `error` with `request_failed` when a payload is present; return 200 + explicit `degraded:true`/reason instead of a generic error; have read-panel loaders tolerate `ok:false` (render the degraded reason rather than throwing).

### F5 — `/api/operator/institutionalCheck` returns HTTP 500 whenever degraded · **MEDIUM** · CONFIRMED
- **Observed:** `500 {"configValid":false,"error":"request_failed","healthOk":false,"ok":false}`.
- **Root cause:** `api_operator_handlers.py:807` returns `ok = readiness.ok AND health.ok`; when feeds are absent both are false → handler returns `ok:false` → `_derive_response_status` maps to **500**. The operator "Institutional Check" button therefore shows a hard error in normal warming-up/no-feed states.
- **Recommended next step:** return **200** with `{pass:false, configValid, healthOk, reasons[]}`; reserve 500 for genuine exceptions.

### F6 — Operator telemetry WebSocket fails through the `:8000` proxy · **MEDIUM** · CONFIRMED
- **Observed:** both browser runs: `WebSocket connection to 'ws://127.0.0.1:8000/operator/ws/operator' failed`. Route catalog notes the proxied path returns `426`.
- **Root cause:** `dashboard_server.py` operator-console compat (`_handle_operator_console_compat`) does not bridge the WS upgrade to the Node sidecar; the operator UI's Live-PnL/telemetry stream never connects when the console is opened via the dashboard-proxied `/operator/` path.
- **Recommended next step:** proxy the WS upgrade through `:8000`, or have the operator UI connect telemetry directly to `:4001` (and document it). Confirm whether LAN mode is expected to expose `:4001` for this.

### F7 — Server refuses connections under modest concurrency (backlog 5) · **MEDIUM** · CONFIRMED (load-dependent)
- **Observed:** running the probe + a 3-page browser reload simultaneously produced `503 Service Unavailable` (candles, data_sources, training_status) and a block of connection resets (`ERR_ABORTED`); the same endpoints return 200 when probed sequentially.
- **Root cause:** listener backlog is 5 (`LISTEN … 5` on `:8000`); limited request concurrency in the `BaseHTTPRequestHandler`-based server.
- **Recommended next step:** raise the socket backlog and confirm a threaded/pooled server; load-test a realistic multi-panel refresh. (`training_status` 500 was only seen under this load — re-check whether it has its own intermittent failure once concurrency is fixed.)

### F8 — 10 dead/unwired dashboard buttons · **LOW** · CONFIRMED (static)
- `btnRefresh`, `btnRunPipeline`, `btnRefreshJobs`, `btnClear`, `btnOperatorMode`, `btnExpertUnlock`, `btnRunChallenger`, `btnReloadCalib`, `btnShowJobHistory`, `btnShowTrends` exist in `ui/dashboard.html` with **no click handler** in `dashboard.js`. `btnRefresh` and `btnRunPipeline` read like primary operator actions but do nothing.
- **Recommended next step:** wire each to its intended handler or remove from the markup.

### F9 — "Clear Error" is the only un-gated operator mutation · **LOW** · CONFIRMED (static)
- `operator_ui.html` `clearLastError()` → `POST /api/operator/clearLastError` fires immediately — no confirmation modal, not covered by the admin-write lock (unlike every other high-impact action).
- **Recommended next step:** gate behind a lightweight confirmation, or document it as intentionally low-risk.

### F10 — Phantom routes return 404 · **LOW** · CONFIRMED
- `/api/allocator/status` and `/api/model/lifecycle` are listed in the assembled route catalog but return the default 404 page at runtime (no registered handler). (`GET /api/operator/broker_risk` 404 is **expected** — it is POST-only; not a defect.)
- **Recommended next step:** register the handlers or remove the entries/aliases so the catalog matches reality.

### F11 — Broker credentials POSTed in cleartext · **LOW** · CONFIRMED (static)
- `dashboard.html` broker panel sends credentials in the body of `POST /api/broker/test_connection` and `/api/broker/config` (reads return them masked).
- **Recommended next step:** ensure TLS for any non-loopback deployment and confirm these bodies are never logged.

### F12 — All paid-feed credentials empty → perpetual WARMING_UP · **INFO / pre-existing** · CONFIRMED
- `data/secrets/{alpaca_key_id,alpaca_secret_key,polygon_api_key,openai_api_key,tradier_api_token}` are **0 bytes**. Runtime stays `WARMING_UP` / `awaiting_first_price_tick`; most data panels honestly show "unavailable." This is the root reason behind the ~16 honest-degraded endpoints — **not a UI bug** (matches the prior data-feed audit). It does, however, mean the no-feed state is the *only* state currently exercisable; panels that need live data could not be value-verified.

---

## 4. Prioritized Fix List (recommended work queue)

| # | Finding | Sev | Why first | Effort |
|---|---|---|---|---|
| 1 | **F1** client sends no token | HIGH | Makes the whole UI unusable in the token-required (real) profile | M |
| 2 | **F2** market-data `ts_ms` 500 | HIGH | Breaks Pro Chart + operator market data outright; clear root cause | S–M |
| 3 | **F3** rate limits too low | HIGH | UI 429s itself even when authenticated | S (raise) / M (coalesce) |
| 4 | **F4** `request_failed` on valid data + client throws | MED | Panels read "unavailable" despite working backend | M |
| 5 | **F5** institutionalCheck 500 | MED | Operator action errors in normal degraded state | S |
| 6 | **F6** operator WS telemetry | MED | Live-PnL/telemetry dead via proxy | M |
| 7 | **F7** server backlog/concurrency | MED | 503s under realistic multi-panel load | S–M |
| 8 | **F8** dead buttons | LOW | Misleading operator affordances | S |
| 9 | **F9** un-gated Clear Error | LOW | Consistency of the confirmation model | S |
| 10 | **F10** phantom routes | LOW | Catalog/runtime mismatch | S |
| 11 | **F11** cleartext broker creds | LOW | Hardening for non-loopback | S |

---

## 5. Definition of Done

Every inventoried surface had every read endpoint exercised or explicitly marked NOT-EXERCISED with a reason; the highest-impact mutations were guard-verified (all fail-closed) rather than triggered; three UI surfaces were rendered in a real browser with screenshots; every FAIL has a root-cause-anchored next step with confirmed-vs-hypothesis labelled. No control was assumed working without independent evidence (HTTP status + payload + browser console + screenshot).

**Start here:** **F1** (wire the API token into `api_client.js` / add a token affordance) — it is the single change that turns the dashboard and operator console from "401 everywhere" into usable, after which **F2** (the `ts_ms` market-data 500) is the next most visible breakage.
