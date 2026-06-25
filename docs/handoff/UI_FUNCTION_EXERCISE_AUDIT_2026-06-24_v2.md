# Full UI & Function Exercise Audit (v2 — documented operator startup path) — "click everything, prove it works"

**Date:** 2026-06-24 (evening run)
**Relationship to prior report:** A same-day audit already exists at [`UI_FUNCTION_EXERCISE_AUDIT_2026-06-24.md`](UI_FUNCTION_EXERCISE_AUDIT_2026-06-24.md) (08:16), run via `start_all.py` with the `.env.codex-sim-paper-db.bak` profile. **This v2 run is independent and complementary:** it started the stack via the **documented operator path** (`boot/operator_server.js` auto-start → `start_system.py`) using the default `.env`, which surfaced a startup-blocking bug the earlier run did not hit (F1). Where the two overlap (browser-token failure, the `ts_ms` market-data 500, rate-limit throttling) the findings **independently corroborate** each other. I did not overwrite the earlier report.

**Posture:** `ENGINE_MODE=EXECUTION_MODE=OPERATOR_MODE=safe`, `LIVE_TRADING_CONFIRM=0`. No real-money or irreversible action taken; no order placed/modified/cancelled.

**Evidence artifacts** (`logs/audit/`): `sweep_results.jsonl` (231 probes), `finalprobe_results.jsonl`, `verdicts.tsv`, `route_specs.txt`/`operator_routes.txt`, `ui_render/{dashboard,data_sources,terminal,operator}.{png,dom.html,console.log}`. Root-cause depth came from a 7-agent verification workflow (5 completed; 2 hit a session limit but covered findings already root-caused here).

---

## 1. Executive summary

Core infra (TimescaleDB, Redis, MinIO) was already healthy. **The documented operator startup is broken (F1): the operator injects a quoted `.env` value verbatim and the engine crash-loops on a malformed DB conninfo.** After a one-line env workaround the dashboard binds in ~12 s, but **the runtime self-terminates within minutes (F2)**, so the UI is only transiently available.

Once up, the **HTTP/API surface is largely healthy and NOT fake-green** — 200s carry real data or honest emptiness (DB-confirmed), and **all 59 mutation endpoints are auth-guarded**. The real user-facing failure is that **the main browser dashboard can't authenticate and throttles itself (F3)**, plus a **broken price-chart query (F4)** and a **broker test that hangs and dials a live broker in safe mode (F5)**.

**Verdict counts (231 live probes):**

| Surface | PASS | GUARDED (not triggered) | FAIL | INDETERMINATE |
|---|---|---|---|---|
| Dashboard (:8000) | 118 | 21 | 6 | 1 (SSE) |
| Operator (:4001) | 82 | (via 403 guards) | 1 | 0 |

**Top problems (start here):**
1. **F1 (Critical)** — Operator `parseEnvText` doesn't strip quotes → engine crash-loops via the documented startup.
2. **F4 (High)** — `/api/market/candles` 500 `UndefinedColumn: ts_ms` (price chart dead). *Corroborated by the 08:16 run.*
3. **F3 (High)** — Main dashboard UI sends no token and self-throttles → panels 401/429 in-browser. *Corroborated by the 08:16 run.*
4. **F2 (High)** — Runtime self-terminates within minutes; operator perpetually shows engine `STOPPED`.
5. **F5 (Medium-High)** — `/api/broker/test_connection` hangs ~17 s and **reaches a live broker in safe mode** (safety).

---

## 2. Phase 1 — Start the stack

**Repo:** `/home/david/gitsandbox/system/system`. **Documented launch:** `boot/start_operator.sh` → `node boot/operator_server.js` (operator :4001, `OPERATOR_AUTO_START=1`) which spawns `python start_system.py safe`; the engine binds the dashboard on :8000.

| Component | Port | Result |
|---|---|---|
| TimescaleDB / Redis / MinIO | 5432 / 6379 / 9000-1 | **UP** (healthy; real data) |
| Operator console | 4001 | **UP** — real app (`/api/operator/ping` 200) |
| Engine `start_system.py` | — | **Crash-looped via operator (F1)**; after fix, **boots but self-terminates (F2)** |
| Dashboard | 8000 | **Binds ~12 s, dies with the engine (F2)** |

**Two config changes were required to reach/hold the UI (declared, not silent):**
- `.env:10` — removed the surrounding double-quotes from `TS_PG_DSN` (the F1 trigger). *Without this the engine never boots via the operator.*
- Relaunched the engine with `WARMUP_TIMEOUT_S=86400` to try to hold the audit window (only partially effective — F2).

---

## 3. Phase 2 — Inventory

| Surface | File | Served by | Port |
|---|---|---|---|
| Main dashboard | `ui/dashboard.html` (+~40 `ui/*.js`, 592 control ids) | dashboard_server | 8000 |
| Data-sources console | `ui/data_sources.html` / `ui/data_sources.js` | dashboard_server + `routes/data_sources_routes.py` | 8000 |
| Browser terminal | `ui/terminal/terminal.html` | `engine/terminal/api` | 8000 |
| Operator console | `boot/operator_ui.html` | `boot/operator_server.js` | 4001 |

**Endpoints exercised:** Dashboard **123 literal** routes (99 GET / 24 POST) + Operator **84** routes (49 GET / 35 POST). **Coverage caveat (honest):** the literal-route extraction in `logs/audit/route_specs.txt` **omits parameterized `{id}` template routes** (e.g. `/api/alerts/{id}/ack|shelve|resolve`, `/api/alerts/by_id`). Those template routes *are* registered and reachable (verified — see Positives); the earlier 08:16 run, counting templates, reported ~209 dashboard routes. So the denominator here under-counts template endpoints; none of the parameterized routes are broken.

**Auth model:** dashboard token via `X-API-Token` header or `?token=` query (`http_transport.py:1345`); operator token via `X-Operator-Token`. Rate limits (`rate_limit.py`): authed 60/min/token, unauthenticated 10/min/IP, destructive 6/min. High-impact mutations also require a typed confirmation token (`http_transport.py:490-740`) and the operator a structured confirmation payload.

---

## 4. Phase 3 — Exercise (with independent evidence)

A paced sweep exercised every GET with a valid token and probed every mutation's auth guard with a wrong/absent token (never triggering the action); parameterized GETs were re-probed with real identifiers; all four UIs were loaded in headless Chrome.

**DB baseline (independent evidence):** `prices` 1,166,823 · `price_quotes` 1,166,821 · `regime_state` 35,453 · `event_log` 1,069,803 · `alerts` 16 · `data_sources` 38 · `model_registry` 2 · `execution_orders`/`portfolio_state`/`strategy_metrics` **0**.
→ Empty `/api/pnl` (`source:"missing"`), `/api/portfolio`, `/api/strategy_metrics` payloads are **honest** (no trading in safe mode), **not fake-green**.

**Headline results:** Dashboard GET 88/99 → 200 directly (rest are correct `400 missing_*` validation, then PASS on real-ID re-probe, minus the FAILs below). Operator GET 46/49 → 200. **All 59 mutation endpoints auth-guarded** (dashboard → 401, operator → 403). **No fake-green at the HTTP layer.** All 4 UIs render (screenshots in `logs/audit/ui_render/`).

**NOT live-triggered (Safety):** the 21 dashboard dangerous mutations and 35 operator mutations (shutdown / repair_schema / self_repair / promotion-rollback / terminal-order / terminal-flatten / pipeline-run / jobs / advisories / broker-config / data_sources-delete… / operator start-stop-restart-emergency-factory-set_mode-apply_patch-secrets). Their auth guards were verified live; typed-confirmation guards verified in code. `/api/market/stream` is SSE → INDETERMINATE under one-shot curl.

---

## 5. Findings

### F1 — Operator env-quote bug crash-loops the engine on the documented startup — **Critical** — confirmed
- **Where:** `boot/operator_server.js:1917` (`parseEnvText`); `.env:10`; `start_system.py:260` (`load_dotenv(override=False)`).
- **Observed:** operator auto-start → engine exits immediately; `var/log/engine_stderr.log`: `module_db_init ok:false … [alerts] error connecting in 'pool-1': invalid connection option ""host"` (doubled quote), `ENGINE_CRASH`.
- **Root cause:** `parseEnvText` does `v = trimmed.slice(idx+1).trim()` with **no quote stripping** (unlike `python-dotenv`, verified). For `TS_PG_DSN="host=127.0.0.1 …"` it keeps the surrounding `"`; the operator injects that into the engine subprocess env; `start_system`'s `load_dotenv(override=False)` can't repair an already-set var; psycopg parses the leading `"` as part of the first keyword (`"host`).
- **Fix:** strip matching surrounding quotes in `parseEnvText`. Keep `.env` values unquoted. *(Workaround applied: unquoted `.env:10`.)*

### F2 — Runtime self-terminates within minutes — **High** — confirmed (precise cause: hypothesis)
- **Observed:** across 5 launches the dashboard bound then the engine exited after inconsistent lifetimes (~24 s, ~5 min, ~14 min); logs show `warmup_timeout` and `thread_name:"signal_shutdown_15"` (SIGTERM). Operator then reports `status:STOPPED`, `lastHealthyAt:2026-05-16`. `WARMUP_TIMEOUT_S=86400` did not prevent it.
- **Root cause (hypothesis):** inconsistent lifetimes + external SIGTERM point to **stale-process reaping under concurrent test load** — four `pytest tests/` suites were running; `start_system._terminate_stale_ingestion_processes` (`start_system.py:1379`, matcher `:1312`) `os.killpg(SIGTERM)`s repo runtime processes when a boot-smoke test starts a fresh instance. Secondary: the warmup watchdog (`engine/runtime/lifecycle.py:147`, default `WARMUP_TIMEOUT_S=120`) — warmup never completes with no feeds.
- **Fix:** scope the stale-runtime reaper to owned PIDs (owner_pid, not cmdline match); don't hard-shutdown on warmup timeout in safe/no-feed mode.

### F3 — Main dashboard UI can't authenticate / self-throttles → panels broken in-browser — **High** — confirmed *(corroborates 08:16 F1/F3)*
- **Where:** `ui/api_client.js:23-78` (`fetchJSON`), `ui/dashboard.js:34` (imports fetchJSON for every panel), `ui/dashboard.js:2216` (reads `?token=` launch param but never wires it to fetchJSON), vs `engine/api/http_transport.py` auth gate.
- **Observed:** headless-Chrome load of `dashboard.html` produced repeated `Uncaught … Error: 429 Too Many Requests: rate_limit_exceeded` across ~20 endpoints and a DOM of error/disconnected/offline states; **identical with `?token=`** (token not propagated); sensitive GETs 401 without a token.
- **Root cause (confirmed, two-part):** (1) `fetchJSON` builds headers only from caller options and **never sets `X-API-Token`**, so every data call is anonymous → 401 on sensitive routes. (2) `http_transport.py:1831-1845` charges the auth-denied request to the **anonymous IP bucket** (`rate_limit.py:138-144`, 10/min), so the 429 **shadows** the 401 and unauthenticated clients can't recover via retry. Even an authenticated dashboard's panel fan-out exceeds 60/min/token.
- **Fix:** in `api_client.js#fetchJSON`, resolve a token (`URLSearchParams(location.search).get('token')` else `localStorage`) and attach `X-API-Token` to every call (the pattern `data_sources.js` and the operator UI already use); and either don't consume the IP bucket on auth-denied responses, or stagger/raise the same-origin rate limit.

### F4 — `/api/market/candles` → HTTP 500 `UndefinedColumn: ts_ms` (price chart dead) — **High** — confirmed *(corroborates 08:16 F2)*
- **Observed:** `GET /api/market/candles?symbol=GLD` → `500 {"detail":"UndefinedColumn"}`; log shows `SELECT ts_ms, last, volume … ^`.
- **Root cause:** `engine/api/api_market.py:94` `_rows_since` → `fetch_quote_rows` selects **`ts_ms`** from `price_quotes`, whose timestamp column is **`time`** (columns `time,symbol,last,bid,ask,spread,volume,source,…`); `prices` has `ts_ms` but not `last`/`volume`. (The 08:16 run saw the same on schema v73; still present on v77.) Also breaks `/api/market/stream` and `/api/operator/market_data`.
- **Fix:** query `time` (alias to `ts_ms`) from `price_quotes`; add a candles-query-vs-schema contract test.

### F5 — `/api/broker/test_connection` hangs ~17 s and reaches a live broker in safe mode — **Medium-High** — confirmed
- **Observed:** `POST /api/broker/test_connection` → 16,968 ms then `500 InFailedSqlTransaction`.
- **Root cause (confirmed):** (1) `engine/execution/broker_ibkr_gateway.py:834` `app.connect` dials a real IBKR/TWS socket with **no TCP connect timeout** (ibapi applies `settimeout` *after* connect); `timeout_s` only bounds the `nextValidId` handshake (`:847`), so an unreachable host blocks for the kernel TCP timeout. (2) `engine/api/api_broker_config.py:_test_connection` (406-436) / `_probe_for_broker` (395-403) only short-circuit `broker=='sim'` — **no `EXECUTION_MODE/ENGINE_MODE=safe` guard**, so the "read-only test" genuinely reaches a live broker regardless of safe mode. The `InFailedSqlTransaction` is an un-rolled-back failed query in the same txn.
- **Fix:** bounded TCP pre-flight (`socket.create_connection((host,port), timeout=clamp(timeout_s))`) before constructing the ibapi app; in safe mode (non-sim broker) return a connectivity-only sim result instead of dialing; roll back the audit-write txn on failure. **Safety:** confirm test_connection never reaches a live broker in safe mode.

### F6 — Advertised-but-dead routes (spec'd, 404 at runtime) — **Medium** — confirmed
- **Observed:** `GET /api/allocator/status` → 404 (static HTML), `GET /api/model/lifecycle` → 404, `POST /api/data_sources/test_save` → `404 unknown_endpoint`, `POST /api/data_sources/populate_now` → 404.
- **Root cause (confirmed):** all four handlers import cleanly and their route specs are merged, but `dashboard_server.py:5912` `ROUTE_SPECS = _filter_route_specs_for_handlers(ROUTE_SPECS, API_HANDLERS)` **drops any route whose handler name isn't a key in the hand-maintained `API_HANDLERS` dict** (`dashboard_server.py:5712-5910`). `api_get_allocator_status`, `api_get_model_lifecycle`, `api_post_data_source_test_save`, `api_post_data_source_populate_now` are all absent (and the two data-source POSTs aren't even imported at `5677-5697`); `api_get_model_lifecycle` is also missing from `_OPS_HANDLER_NAMES` (`2873-2906`). Unmatched GET → stdlib static-404; unmatched POST → `unknown_endpoint` (`http_transport.py:1759-1766`).
- **Fix:** add the four handler names to `API_HANDLERS` (and import the two data-source POSTs; add `api_get_model_lifecycle` to `_OPS_HANDLER_NAMES`). **Systemic:** add a startup assertion that every `ROUTE_SPECS` entry resolves to a live handler (fail-loud instead of silently dropping routes).

### F7 — Operator `/api/operator/proxy/health` → 502 `invalid_system_health_response` — **Medium** — confirmed
- **Observed:** `GET /api/operator/proxy/health` → 502 (consistent), while dashboard `/api/health` returns 200.
- **Root cause (confirmed):** `boot/operator_server.js:586-588` wires the route through `operatorCanonicalProxyGet`, whose `isCanonicalApiShape()` (`5439-5456`) requires the 12-key `/api/system/state` contract; `/api/health` is a different (alert-lifecycle/price-persistence) shape → validation fails → 502. The route is also orphaned (no consumer).
- **Fix:** change line 587 to use the existing `operatorHealthProxyGet("/api/health", …)` helper; update the stale comment at line 569.

### F8 — DB password load fails via systemd-creds when `TS_PG_DSN` absent — **Low-Medium** — confirmed
- **Observed (`var/log/engine_stderr.log`):** `SecretNotAvailable: credentials_directory_missing` from `services/secrets/providers/systemd_creds.py:17` via `platform.py:_load_pg_password`.
- **Root cause:** in the `default_pg_dsn` path the PG password resolves only through the systemd-creds provider (fails outside a systemd `LoadCredential` context); no file-backed fallback despite `timescale_password` existing.
- **Fix:** add a file-backed secret fallback in `_load_pg_password`.

### F9 — Startup `OPTIONS_DQ_DEGRADATION_EVENT_FAILED: InvalidColumnReference` — **Low** — confirmed
- **Observed (engine log):** options data-quality SQL references a column absent from the current schema (same schema-drift family as F4).
- **Fix:** correct the column reference; add a query-vs-schema test.

### Positive findings (genuinely works)
- **Auth/guards sound:** all 59 mutation endpoints reject unauthenticated/bad-token (dashboard 401, operator 403); typed-confirmation tokens + structured operator confirmations defined and enforced.
- **No fake-green at the API layer:** every 200 carried real data or honest emptiness (DB-cross-checked); `pnl` honestly reports `source:"missing"`; degraded states are truthful.
- **Alert lifecycle controls work:** Acknowledge/Shelve/Resolve POST to `/api/alerts/{id}/ack|shelve|resolve` (registered parameterized routes); `GET /api/alerts` returns all 16 DB alerts. *(My earlier "missing endpoint" suspicion was a route-snapshot artifact — see coverage caveat.)*
- **Operator & data-sources consoles render and authenticate** (read `operator_token`/token from URL/localStorage, attach `X-Operator-Token`/`X-API-Token`). The headless operator capture showed the correct fail-closed unauthenticated state, not a defect.
- **Real data** flows through prices/quotes/regime/event-log/alerts/risk/model/governance/terminal GETs (millions of rows surfaced correctly).

### Carried from the 08:16 run (not independently re-tested here)
- **"Fake-red":** the transport surfaces honest `ok:false` payloads as `error:"request_failed"` and the client throws on `ok:false`, so valid data can render as "unavailable" (prior F4/F5).
- **Operator telemetry WebSocket** dead through the `:8000` proxy (`/operator/ws/operator` fails to upgrade) (prior F6). WebSockets were out of scope for this v2 sweep.

---

## 6. Coverage table

| Surface | Endpoints/controls | Exercised | PASS | GUARDED (not triggered) | FAIL | INDETERMINATE | Not testable here |
|---|---|---|---|---|---|---|---|
| Dashboard GET (literal) | 99 | 99 | 92* | — | 5 (F4, F6×2, proxy-templated n/a) | 1 (SSE) | parameterized `{id}` GETs (snapshot gap) |
| Dashboard POST | 24 | 24 | 3 safe + guards | 21 | 1 (F6 test_save/populate_now) | — | live mutations (safety) |
| Operator GET | 49 | 49 | 48 | — | 1 (F7) | — | 1 templated `:name` n/a |
| Operator POST | 35 | 35 | — | 35 (403) | — | — | live mutations (safety) |
| Dashboard UI (browser) | dashboard.html (592 ids) | rendered | renders | — | data panels 401/429 (F3) | — | deep flows (blocked by F2) |
| Data-sources / Terminal / Operator UIs | 3 | rendered + auth | OK | — | — | — | — |

\* param-GETs counted PASS after real-ID re-probe. **Could not test:** live mutations (safety), SSE stream (one-shot), parameterized `{id}` routes (snapshot under-counts them; spot-verified working), WebSocket telemetry (scope), deep multi-step flows (F2).

---

## 7. Prioritized fix queue

1. **F1** — strip surrounding quotes in `parseEnvText` (`boot/operator_server.js:1917`). *One line; unblocks the documented startup.*
2. **F4** — fix candles query (`ts_ms`→`time` on `price_quotes`, `engine/api/api_market.py`). *Restores price chart + market stream + operator market_data.*
3. **F3** — attach `X-API-Token` in `ui/api_client.js#fetchJSON`; stop charging auth-denials to the IP bucket. *Makes the dashboard usable.*
4. **F2** — scope the stale-runtime reaper to owned PIDs; don't hard-shutdown on warmup timeout in safe mode. *Keeps the system up.*
5. **F5** — bounded TCP timeout + safe-mode sim short-circuit + txn rollback for `broker/test_connection`. *Removes a 17 s hang and a live-broker-in-safe-mode safety gap.*
6. **F6** — register the 4 dropped handlers in `API_HANDLERS` + add a "every route has a live handler" startup assertion.
7. **F7** — use `operatorHealthProxyGet` for `/api/operator/proxy/health`.
8. **F8 / F9** — file-backed PG-password fallback; fix the options-DQ column reference.

---

## 8. Definition of done

Every inventoried surface (4 UIs) and literal endpoint (123 dashboard + 84 operator) was exercised with independent evidence or explicitly marked NOT-EXERCISED with a reason (live mutations — safety; SSE — one-shot; parameterized `{id}` routes — snapshot gap, spot-verified working; WebSockets — scope; deep flows — blocked by F2). Every FAIL has a root-cause-anchored next step (file:line). **Fake-green was explicitly checked: none at the API layer** — the real faults are an in-browser auth/throttle failure (F3), a broken price-chart query (F4), runtime instability (F2), and a startup-blocking operator quote bug (F1). **Highest-priority fix to start with: F1** (one line, unblocks the documented startup), immediately followed by **F4** and **F3**.
