# UI & Function Exercise Audit — Remediation Deep-Dive Prompts (UIA-1 … UIA-11)

Source audit: [`docs/handoff/UI_FUNCTION_EXERCISE_AUDIT_2026-06-24_v2.md`](../UI_FUNCTION_EXERCISE_AUDIT_2026-06-24_v2.md).
Each section below is a **self-contained codex deep-dive prompt** — design and implement the optimal solution for exactly one finding. Hand a single section to a codex agent; do not assume cross-section context.

**Global constraints (apply to every prompt):**
- Work from repo root `/home/david/gitsandbox/system/system`. Posture is `ENGINE_MODE=EXECUTION_MODE=OPERATOR_MODE=safe`. Never place/modify/cancel a real order, hit a production broker, rotate secrets, or run destructive DB ops.
- Respect the model-vs-runtime contract and the existing governance/guard architecture; extend, don't replace. Do not weaken any auth/confirmation guard.
- File:line anchors are from the audit and may have drifted a few lines — confirm by reading before editing.

---

## UIA-1 — Operator `.env` quote-stripping bug crash-loops the engine (F1, **Critical**)

**Context.** The documented local startup is `boot/start_operator.sh` → `node boot/operator_server.js` (`OPERATOR_AUTO_START=1`), which parses `.env` itself and spawns the engine `python start_system.py safe` with that parsed env injected into the child process.

**Defect (confirmed).** When any `.env` value is wrapped in quotes (e.g. `TS_PG_DSN="host=127.0.0.1 port=5432 user=trading dbname=trading"`), the engine crash-loops at DB init with `[alerts] error connecting in 'pool-1': invalid connection option ""host"` (note the doubled quote) → `module_db_init ok:false` → `ENGINE_CRASH`. The documented operator auto-start path therefore cannot bring the system up. (`var/log/engine_stderr.log` shows the repeating error.)

**Root cause (confirmed).** `boot/operator_server.js:1917` `parseEnvText` sets `v = trimmed.slice(idx+1).trim()` with **no surrounding-quote stripping** — unlike `python-dotenv`, which strips matching quotes (verified: `dotenv_values` returns the unquoted value). The operator injects the still-quoted value into the engine subprocess env; `start_system.py:260` `load_dotenv(env_file, override=False)` cannot repair an already-set variable; psycopg then parses the leading `"` as part of the first conninfo keyword (`"host`). `serializeEnv` (`boot/operator_server.js:1931`) is the inverse and must round-trip correctly.

**Your task.** Design and implement the optimal fix so the operator parses `.env` with the same quoting semantics as `python-dotenv`: strip a single matching pair of surrounding single or double quotes from each value (and handle escaped quotes / `#` inline-comment rules consistently with dotenv, without corrupting values that legitimately contain quotes or `=`). Ensure `parseEnvText`↔`serializeEnv` round-trips. Decide whether any currently-quoted keys in committed `.env*.example` templates need normalizing. Keep `.env` itself unquoted as defense-in-depth (note: the auditor already unquoted live `.env:10` as a workaround — your fix must make the operator correct regardless).

**Falsify.** A fix that only unquotes the one `TS_PG_DSN` line is NOT acceptable — the parser must be correct for all keys. Prove with a value that contains an internal `=` and one wrapped in quotes that both survive correctly, and that an unquoted value with a trailing inline comment is handled the same way dotenv handles it.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## UIA-2 — `/api/market/candles` 500 `UndefinedColumn: ts_ms` (price chart dead) (F4, **High**)

**Context.** The dashboard Pro Chart, the SSE market stream, and the operator market-data panel all read the quote-row time series.

**Defect (confirmed).** `GET /api/market/candles?symbol=GLD` → `500 {"detail":"UndefinedColumn"}`; the failing SQL is `SELECT ts_ms, last, volume …`. The `price_quotes` table's timestamp column is **`time`**, not `ts_ms` (columns: `time,symbol,last,bid,ask,spread,volume,source,last_trade_ts_ms,last_quote_ts_ms,last_update_ts_ms`); the `prices` table has `ts_ms` but **no** `last`/`volume` (columns: `ts_ms,symbol,price,px,source`). The same 500 also breaks `/api/market/stream` and `/api/operator/market_data`. (Independently reproduced on schema v77; the 08:16 audit hit it on v73 — a long-standing schema/column mismatch.)

**Root cause (confirmed).** `engine/api/api_market.py:87-94` `_rows_since` → `engine/runtime/price_read_router.py:580` `fetch_quote_rows`, whose `SELECT ts_ms, last, volume` branches (`price_read_router.py:~499, 519, 546-564`) reference `ts_ms` against relations that expose it as `time` (or don't expose `last`/`volume`).

**Your task.** Design and implement the optimal fix so `fetch_quote_rows` returns `(ts_ms, last, volume)` correctly from whichever relation actually holds last/volume quotes — e.g. `SELECT time AS ts_ms, last, volume FROM price_quotes …` for the quotes branch, and a correct projection for any `prices`/`price_quotes_raw` branch (map the real columns; `prices` has `price`/`px`, not `last`/`volume`). Make the read router resilient to the canonical column names per relation rather than assuming `ts_ms` everywhere. Add a query-vs-live-schema contract test for the candles path so this can't regress silently.

**Falsify.** A fix that returns rows for `price_quotes` but leaves `/api/market/stream` or `/api/operator/market_data` still 500 is incomplete — verify all three. A fix that only catches the exception and returns empty `candles:[]` is NOT acceptable (that hides the bug and shows an empty chart on a symbol with 1.16M quote rows). Prove with `symbol=GLD` that real OHLCV candles are returned (the DB has `price_quotes` rows for it).

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## UIA-3 — Main dashboard UI cannot authenticate and self-throttles (F3, **High**)

**Context.** The server requires `X-API-Token` (or `?token=`) for sensitive GETs; the data-sources console and operator console both attach a token, but the main dashboard panels do not.

**Defect (confirmed).** Loading `ui/dashboard.html` in a real browser produces a `429 Too Many Requests: rate_limit_exceeded` storm across ~20 endpoints and a DOM full of error/disconnected/offline states; **identical with `?token=`** (the token is not propagated to API calls). Net effect: the primary dashboard is unusable for a real user.

**Root cause (confirmed, two parts).** (1) `ui/api_client.js:23-78` `fetchJSON` builds `Headers` only from caller options and **never sets `X-API-Token`**; `ui/dashboard.js:34` passes `fetchJSON` to every panel with no token wiring, and `ui/dashboard.js:~2216` `applyDashboardLaunchParams` reads a `?token=` launch param but never hands it to the fetch layer. (2) `engine/api/http_transport.py:1831-1845` runs the rate-limit check with an **empty** token-for-bucket on auth-denied requests → `engine/api/rate_limit.py:138-144` maps empty token to the 10/min **per-IP** bucket, so the 429 **shadows** the underlying 401 and clients can never recover via retry. The token-attach pattern to mirror is `ui/data_sources.js:~139-156` (its own `request()` sets `X-API-Token`).

**Your task.** Design and implement the optimal fix on both axes: (a) make `ui/api_client.js#fetchJSON` resolve a dashboard token once (`new URLSearchParams(location.search).get('token')` → else `localStorage.getItem(...)`; persist a URL-supplied token to `localStorage`; clear on demand) and attach `X-API-Token` to every same-origin `/api/*` call, without leaking the token cross-origin; (b) stop charging auth-denied responses to the anonymous IP bucket (return the real 401 without consuming it, or use a separate denial bucket), and/or reduce the dashboard's on-load fan-out / stagger polling so an authenticated dashboard stays under the 60/min token budget. The dashboard must populate its core panels in a real browser when a valid token is provided.

**Falsify.** A fix that raises the rate limit but still sends no token leaves sensitive GETs at 401 — both axes are required. Prove via headless Chrome (`google-chrome --headless=new --dump-dom http://127.0.0.1:8000/ui/dashboard.html?token=$(cat data/secrets/dashboard_api_token)`) that the 429/401 console errors drop to ~0 and panels populate. Do not disable or weaken auth on the server to "fix" the UI.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## UIA-4 — Runtime self-terminates within minutes (F2, **High**)

**Context.** Once the dashboard binds, the engine must stay up to serve the UI. In this environment it does not.

**Defect (confirmed).** Across five launches the dashboard bound, then the engine exited after **inconsistent** lifetimes (~24 s / ~5 min / ~14 min). Logs show both `warmup_timeout` and `thread_name:"signal_shutdown_15"` (an external `SIGTERM`). Setting `WARMUP_TIMEOUT_S=86400` did **not** prevent it. The operator then perpetually reports `status:STOPPED`, `degradedComponents:[engine_process]`, `lastHealthyAt:2026-05-16`.

**Root cause (hypothesis — confirm first).** The inconsistent lifetimes plus an external SIGTERM point to **cross-process reaping under concurrent test load**: when a boot-smoke test (one of several concurrent `pytest tests/` runs) starts a fresh runtime, `start_system._terminate_stale_ingestion_processes` (`start_system.py:1379`; matcher `_looks_like_repo_ingestion_process` `:1312`; `os.killpg(SIGTERM)` `~:1710`) can match and kill a *different* repo runtime by cmdline+repo-path rather than by ownership. The `warmup_timeout` watchdog (`engine/runtime/lifecycle.py:147`, `lifecycle_state.py:139`, default `WARMUP_TIMEOUT_S=120`) is a secondary contributor — warmup never completes because no data feeds are configured.

**Your task.** First **prove which mechanism fires** (correlate the SIGTERM timestamp with boot-smoke test starts; check whether the warmup watchdog or the stale-reaper issued the signal). Then implement the optimal fix: scope the stale-runtime terminator to processes it genuinely owns (`owner_pid`/recorded pid lineage, not a broad cmdline+repo match) so concurrent suites and side-by-side instances can't cross-kill; and ensure the warmup watchdog does not hard-shutdown a healthy-but-feedless runtime in safe mode (honor `WARMUP_TIMEOUT_S`, or transition to a degraded-but-serving state instead of exiting). The dashboard must stay bound indefinitely in safe mode with no feeds.

**Falsify.** Do not "fix" this by globally disabling the stale-process reaper (that protects against real orphan ingestion). Prove the engine survives ≥15 minutes in safe mode while other `pytest tests/` runs are active, and that a genuine orphaned ingestion child is still reaped. State explicitly which mechanism was the cause.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## UIA-5 — `broker/test_connection` hangs and reaches a live broker in safe mode (F5, **Medium-High, safety**)

**Context.** `POST /api/broker/test_connection` is presented as a read-only connectivity check from the broker panel.

**Defect (confirmed).** In safe mode with `BROKER_NAME=ibkr`/`IBKR_PORT=7497` and no gateway running, the call blocked **16,968 ms** then returned `500 InFailedSqlTransaction`. Two problems: it hangs, and — a **safety-posture violation** — a "read-only test" genuinely dials a live broker socket regardless of `EXECUTION_MODE=safe`.

**Root cause (confirmed).** (1) `engine/execution/broker_ibkr_gateway.py:~834` `app.connect` (in `_connect_ib`) opens a real IBKR/TWS socket with **no TCP connect timeout** (ibapi applies `settimeout` *after* connect); `timeout_s` only bounds the `nextValidId` handshake wait (`~:847`); `ping_broker_connection` (`~:2306`) inherits this. (2) `engine/api/api_broker_config.py:_test_connection` (`406-436`) / `_probe_for_broker` (`395-403`) only short-circuit `broker=='sim'` — there is **no `EXECUTION_MODE/ENGINE_MODE=safe` guard** before invoking the live probe. The `InFailedSqlTransaction` is an un-rolled-back failed query in the same DB transaction.

**Your task.** Design and implement the optimal fix: (a) add a bounded TCP pre-flight (`socket.create_connection((host, port), timeout=clamp(timeout_s))`, then close) before constructing the ibapi app, so an unreachable host fails fast with a clean `connect_timeout` result instead of blocking on the kernel TCP timeout; (b) in `_test_connection`, when the runtime execution mode is `safe` (and broker is non-sim), return a connectivity-only / sim result and **do not dial a live broker** — this is the safety requirement; (c) ensure the audit-write path rolls back its transaction on failure so it can't poison subsequent queries. The call must return within a few seconds in all cases.

**Falsify.** A fix that only adds a timeout but still dials a live broker in safe mode does NOT satisfy the safety requirement. Prove: in safe mode the handler returns quickly with no live socket opened (verify no connection attempt to `127.0.0.1:7497`), and the InFailedSqlTransaction no longer occurs. Do not place any order or send any executable broker request.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## UIA-6 — Advertised routes silently dropped from the live handler table (F6, **Medium**)

**Context.** Route specs are merged into `ROUTE_SPECS`, but handlers are bound through a separately hand-maintained `API_HANDLERS` dict, then a filter drops any spec whose handler isn't registered.

**Defect (confirmed).** Four advertised routes 404 at runtime despite valid specs and importable handlers: `GET /api/allocator/status` (static-HTML 404), `GET /api/model/lifecycle` (static-HTML 404), `POST /api/data_sources/test_save` (`{"error":"unknown_endpoint"}`), `POST /api/data_sources/populate_now` (404).

**Root cause (confirmed).** `dashboard_server.py:5912` `ROUTE_SPECS = _filter_route_specs_for_handlers(ROUTE_SPECS, API_HANDLERS)` (`engine/dashboard/routing.py:210-218`) **silently drops** every route whose handler name is absent from the literal `API_HANDLERS` dict (`dashboard_server.py:5712-5910`). The four handler names are absent (the two data-source POSTs aren't even imported at `dashboard_server.py:5677-5697`; `api_get_model_lifecycle` is also missing from `_OPS_HANDLER_NAMES` at `2873-2906`). Handlers themselves are real and import cleanly (`engine/api/api_system.py` `api_get_allocator_status`; `engine/api/api_ops_handlers.py:150` `api_get_model_lifecycle`; `routes/data_sources_routes.py:436/452` for the two POSTs). Unmatched GET → stdlib static-404; unmatched non-GET → `unknown_endpoint` (`engine/api/http_transport.py:1759-1766`).

**Your task.** Implement the optimal fix on two levels. (1) Register the four routes: add their handler names (and the missing imports / `_OPS_HANDLER_NAMES` entry) so they resolve to their real handlers. (2) **Systemic guard against silent drops:** add a startup assertion/diagnostic so that any `ROUTE_SPECS` entry whose handler is missing from `API_HANDLERS` fails loudly (or is logged as a hard error at boot) instead of being silently filtered — a dead advertised route should never ship unnoticed again. Decide the right home for this check so it runs on every boot and in `tools/validate_repo.py`/the UI contract checker.

**Falsify.** Verify all four endpoints return their real payloads (not 404), and that the new fail-loud check actually fires when a handler name is removed (add a temporary missing entry in a test to prove it trips). Note: `POST /api/data_sources/test_save` persists a source and triggers a provider call — exercise it only with clearly-fake test data and reverse it, or assert via the contract checker rather than a live mutation.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## UIA-7 — Operator health proxy validates against the wrong contract (F7, **Medium**)

**Context.** The operator sidecar exposes `GET /api/operator/proxy/health` as a same-origin bridge to the dashboard health.

**Defect (confirmed).** It returns `502 {"error":"invalid_system_health_response","reason_code":"invalid_system_health_…"}` even though the dashboard `/api/health` returns 200 with a healthy payload.

**Root cause (confirmed).** `boot/operator_server.js:586-588` wires the route through `operatorCanonicalProxyGet("/api/health", "invalid_system_health_response")`, whose `isCanonicalApiShape()` (`5439-5456`) requires the 12-key `/api/system/state` "canonical" contract (`ok,status,state,mode,execution_mode,execution_allowed,reasons,health,ingestion,services,readiness,timestamps`). `/api/health` is a different domain shape (`alert_lifecycle`, `async_price_persistence`, …) → validation fails → 502. The route is also orphaned (stale comment at `:569`; no current consumer).

**Your task.** Implement the optimal fix: route `/api/operator/proxy/health` through the correct helper (`operatorHealthProxyGet`, already used successfully elsewhere) so it validates `/api/health` against the health contract rather than the system-state contract; update the stale `:569` comment. Decide whether the route should remain (fixed) or be folded into the catch-all proxy / removed as dead — justify the choice. Ensure the operator support-snapshot / any internal caller still gets correct health data.

**Falsify.** Prove `GET /api/operator/proxy/health` returns 200 with the real health payload while the engine is up, and that the operator support snapshot still validates. Don't make the validator accept everything — it must validate the *health* shape, not be disabled.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## UIA-8 — PG password resolution has no file fallback outside systemd (F8, **Low-Medium**)

**Context.** Postgres password resolution feeds every runtime DB connection. On non-systemd hosts (local dev, containers without `LoadCredential`) it must still work via the file-backed secret.

**Defect (confirmed).** When no explicit DSN is set (the `default_pg_dsn` path), boot logs `SecretNotAvailable: credentials_directory_missing` from `services/secrets/providers/systemd_creds.py:17`, failing the DB password load even though `data/secrets/timescale_password` exists.

**Root cause (confirmed).** `engine/runtime/platform.py:435` `_load_pg_password` resolves the password through the secret loader (`load_secret`, `:449`) which routes to the systemd-creds provider and raises when the credentials directory is absent. A file loader already exists — `_load_pg_password_file` (`:402`) backed by `TS_PG_PASSWORD_FILE` / `*_PASSWORD_FILE` — but the default path does not fall back to it.

**Your task.** Implement the optimal fix so `_load_pg_password` resolves robustly across environments: prefer/`fall back to` the file-backed secret (`_load_pg_password_file`) when the configured provider can't supply it (or order the providers so the file source is tried before the systemd provider raises), without weakening secret hygiene in production (file source still gated on the `*_PASSWORD_FILE` config). Make the failure non-fatal-degrading only where appropriate; the happy path on a non-systemd host must succeed silently.

**Falsify.** Prove that with `TS_PG_DSN` unset and no systemd credentials directory, the engine loads the PG password from the file and connects (no `credentials_directory_missing`), and that on a systemd host the existing provider path still wins where configured. Do not hardcode or log the password.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## UIA-9 — Options data-quality query references a non-existent column (F9, **Low**)

**Context.** The options data-quality (DQ) degradation check runs during startup/ingestion against the options tables.

**Defect (confirmed).** Startup logs `OPTIONS_DQ_DEGRADATION_EVENT_FAILED` with `error_type:"InvalidColumnReference"` from `engine.data.options_data_quality` — a SQL query references a column that doesn't exist in the current schema (same schema-drift family as UIA-2/F4).

**Root cause (to confirm).** In `engine/data/options_data_quality.py` the DQ queries (`SELECT … FROM options_chain_v2` at `~:296-299`, `FROM options_symbol_ingestion_state` at `~:229-236`, and `SELECT MAX(ts_ms) FROM {table} WHERE {symbol_column}=?` at `~:282`) reference a column/`symbol_column` that the live schema (schema v77) does not have. Identify the exact offending column by reproducing the query against the live DB and reading the table's `information_schema.columns`.

**Your task.** Implement the optimal fix: correct the column reference(s) to the real schema (or, if the column is legitimately expected, add/verify the migration that creates it — choose based on what the DQ check is contractually supposed to read). Make the DQ check schema-accurate and add a query-vs-live-schema guard/test so options-DQ column drift fails loudly in CI rather than as a swallowed startup warning.

**Falsify.** Prove the options-DQ check runs clean at startup (no `InvalidColumnReference`) against the live schema, and that the new guard would catch a reintroduced bad column. Don't silence the error by broadening an `except` — fix the query/schema.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## UIA-10 — "Fake-red": honest `ok:false`/degraded responses are surfaced as errors (F10, **Medium**)

**Context.** In safe mode (and during warmup, or for any business-level refusal), many handlers correctly return `200 {"ok":false, ...}` with valid data and an explicit reason (e.g. `WARMING_UP`, "no price feed"). This is the *honest* degraded contract — the opposite of fake-green.

**Defect (confirmed).** Those honest degraded responses render in the browser as hard errors / "unavailable", because the server masks the real reason with a generic `request_failed` and the client throws on any `ok:false`. This was the dominant failure mode the earlier (08:16) audit named "fake-red": valid data shown as an error. *(This finding was carried from the 08:16 run and re-verified against current code.)*

**Root cause (confirmed, two-sided).** (1) Server — `engine/api/http_transport.py:1227` overwrites the reason with a blanket fallback: `obj["error"] = str(obj.get("error") or "request_failed")` whenever a handler returns `ok:false` without an explicit `error`, erasing the specific reason/reason_code. (2) Client — `ui/api_client.js:45-61` `fetchJSON` **throws on any `data.ok === false`** unless the caller passes `allowBusinessFalse` (`if (data.ok === false && !allowBusinessFalse) throw new Error(...)`), so every panel that didn't opt in renders the degraded payload as an error. Net: a 200 with valid data + an honest `ok:false` reason becomes a thrown "request_failed" in the UI.

**Your task.** Design and implement the optimal **honest-degraded contract** end-to-end. Server: preserve the handler's explicit `reason`/`reason_code` and an appropriate non-fatal status instead of stamping `request_failed` (reserve `request_failed`/5xx for genuine transport/handler faults; a business refusal is not a 500). Client: make read panels distinguish (a) **business-degraded** — `ok:false` with a reason → render a labeled degraded/empty state showing the reason, no throw — from (b) **transport failure** — network error / 5xx / invalid JSON → error state. Decide whether `allowBusinessFalse` becomes the default for reads or is replaced by a clearer degraded-aware path; ensure genuine errors still surface. Keep the change consistent across the shared fetch layer so individual panels don't each have to opt in.

**Falsify.** Pick an endpoint that returns `ok:false` with a real reason while the runtime is degraded/warming (e.g. a market/portfolio read in safe mode) and prove the panel renders a labeled degraded/"warming up" state with the reason — not a generic error or a thrown exception. Separately prove a true transport failure (kill the endpoint / force a 500) still renders as an error. A fix that makes the client silently swallow ALL `ok:false` (hiding real refusals like auth/guard denials) is NOT acceptable.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## UIA-11 — Operator telemetry WebSocket dead through the `:8000` proxy (F11, **Medium**)

**Context.** The operator console can be reached two ways: directly on the sidecar (`http://127.0.0.1:4001/`) and via the dashboard proxy (`http://127.0.0.1:8000/operator/`). It offers a live telemetry/PnL stream over a WebSocket.

**Defect (confirmed).** When the operator console is opened through the `:8000` proxy, the telemetry WebSocket fails to establish (the upgrade never completes), so Live-PnL/telemetry never streams in the proxied console. *(Carried from the 08:16 run and re-verified against current code.)*

**Root cause (confirmed).** The operator sidecar serves the telemetry WS at `boot/operator_server.js:8097-8111` `startTelemetryWebSocket` (`new WebSocket.Server({ path: "/ws/operator" })`) on `:4001`. The dashboard server on `:8000` (stdlib `ThreadingHTTPServer`, `dashboard_server.py`) proxies only **HTTP** `/api/operator/*` routes (see the operator path table in `engine/api/http_transport.py:~331-356`) and has **no `Upgrade: websocket` handshake handling** for a proxied `/operator/ws/operator` path — stdlib `BaseHTTPRequestHandler` cannot perform a WS upgrade. So the proxied path can never connect.

**Your task.** Design and implement the optimal fix so the operator telemetry stream works for the documented access pattern, with auth/origin kept safe. Evaluate and choose among: (a) have the operator UI open the telemetry WS **directly against the sidecar origin** (`ws://<host>:4001/ws/operator`) with the `X-Operator-Token`/handshake auth, instead of routing it through `:8000` (requires correct origin/CORS handling and that the token is available to the WS handshake without leaking it); (b) add real WebSocket-upgrade proxying for `/operator/ws/*` to the dashboard server (note: this likely requires an upgrade-capable server path, not the stdlib handler); (c) if proxied telemetry is not supported, make the operator UI detect the access origin and connect the WS to the correct origin, and document that telemetry requires the sidecar origin. Whatever the choice, the WS must enforce auth (token required, origin-checked) and must not become an open relay.

**Falsify.** Prove the telemetry WS connects and streams frames when the operator console is opened the documented way (capture the successful `101 Switching Protocols` and at least one telemetry frame). Prove the WS rejects an unauthenticated/cross-origin handshake. A fix that disables or hides the telemetry feature, or that opens the WS without auth, is NOT acceptable.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.
