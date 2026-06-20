# Go-Live Deep Dive Fix Prompts

Use these prompts one at a time. Each prompt is intentionally scoped so an implementation agent can inspect, fix, test, and report without mixing unrelated go-live risks.

## Prompt 1 - Fix Operator Console Proxy Auth Bypass

You are working in `/home/david/gitsandbox/system/system`. The repo is dirty; do not revert unrelated user changes.

Deep dive and fix the operator-console reverse-proxy auth bypass. Current evidence: `dashboard_server.py` wraps the built HTTP handler with `_wrap_operator_console_routes()`, and `OperatorConsoleCompatHandler.do_GET/do_POST()` handles `/operator/*` before the normal `engine.api.http_transport` `_dispatch()` path can enforce `DASHBOARD_API_TOKEN`, mutation rate limits, confirmations, and mutation audit. The bridge then proxies to the Node sidecar at loopback. In `boot/operator_server.js`, `operatorMutationAuthorized()` currently returns true for loopback requests, so proxied mutating requests bypass both dashboard and operator tokens.

Requirements:
- Make mutating `/operator/api/*` bridge requests require the same dashboard mutation auth, rate-limit, and audit semantics as normal dashboard mutations before proxying.
- Forward a real sidecar credential such as `X-Operator-Token: OPERATOR_API_TOKEN` from trusted server-side config when the bridge proxies to the sidecar.
- Remove or strictly narrow the sidecar's loopback auto-authorization. Loopback alone must not authorize dangerous mutations.
- Preserve legitimate same-origin operator UI workflows by updating the UI or bridge token flow as needed.
- Ensure unauthenticated remote clients cannot call `set_mode`, `restart_engine`, `promote_model`, `secrets`, `factoryReset`, or `ai/apply_patch` through `/operator/api/*`.
- Add tests proving unauthenticated bridge POSTs are rejected and never reach the sidecar, while valid dashboard-token bridge POSTs reach the sidecar with the operator token.
- Add direct sidecar tests proving loopback POSTs without `OPERATOR_API_TOKEN` are rejected.
- Update docs/checklists that claim remote dashboard access is safe when token-protected so they explicitly cover the operator bridge.

Suggested files to inspect:
- `dashboard_server.py`
- `engine/api/http_transport.py`
- `engine/api/auth_config.py`
- `boot/operator_server.js`
- `boot/operator_ui.html`
- `tests/test_operator_console_bridge.py`
- `tests/test_api_security_hardening.py`
- `tests/test_operator_server_admin_contract_static.py`

Done criteria:
- Targeted tests fail before the fix and pass after it.
- No mutating operator bridge path can bypass dashboard auth.
- No sidecar mutation is authorized solely because the caller is loopback.

## Prompt 2 - Lock Down Direct Operator Sidecar Exposure and GET Disclosure

You are working in `/home/david/gitsandbox/system/system`. The repo is dirty; do not revert unrelated user changes.

Deep dive and fix direct operator sidecar exposure. Current evidence: `deploy/compose/docker-compose.stack.yml` binds `OPERATOR_BIND_HOST=0.0.0.0` and publishes port 4001. `boot/operator_server.js` skips auth for `GET`, `HEAD`, and `OPTIONS`, and sensitive GET routes include `/api/operator/config`, `/api/operator/logs`, `/api/operator/support_snapshot`, `/api/operator/secrets`, and runtime/system state endpoints. `/api/operator/config` currently returns `readEnv()` directly.

Requirements:
- Do not publish the operator sidecar publicly by default. Prefer an internal Docker network or loopback-only binding for compose.
- Require `OPERATOR_API_TOKEN` for all sensitive sidecar routes, including GET/HEAD and WebSocket access. Only explicitly harmless liveness endpoints, such as `/api/operator/ping`, may remain unauthenticated if justified.
- Redact sensitive values from config/log/support-snapshot responses even after auth. Raw `.env` values, dashboard tokens, DB credentials, broker keys, provider keys, and master keys must not be returned.
- Add live/prod preflight validation for unsafe `OPERATOR_BIND_HOST`, published sidecar ports, missing/placeholder/weak `OPERATOR_API_TOKEN`, and unauthenticated sensitive GET behavior.
- Update compose docs and smoke commands so they match the new exposure model.
- Add tests for direct sidecar GET denial without token, authorized redacted GET with token, compose asset assertions, and preflight blockers for unsafe operator bind/publish.

Suggested files to inspect:
- `boot/operator_server.js`
- `deploy/compose/docker-compose.stack.yml`
- `deploy/compose/.env.example`
- `deploy/compose/README.md`
- `deploy/README.md`
- `docs/PRODUCTION_CHECKLIST.md`
- `engine/runtime/live_trading_preflight.py`
- `engine/runtime/prod_preflight.py`
- `tests/test_compose_deployment_assets.py`

Done criteria:
- Sensitive operator GETs are protected and redacted.
- Compose no longer publishes an unauthenticated operator control plane by default.
- Production preflight blocks unsafe direct sidecar exposure.

## Prompt 3 - Make Cancel-Then-Replace Broker Orders Fill-Safe

You are working in `/home/david/gitsandbox/system/system`. The repo is dirty; do not revert unrelated user changes.

Deep dive and fix stale limit-order cancel-replace behavior. Current evidence: `engine/execution/execution_open_order_manager.py` calls `cancel_order_fn()` and then immediately `_resubmit_order()` without verifying the original order reached a terminal canceled state. `engine/execution/execution_microstructure.py` contains a similar Alpaca-specific path. IBKR `cancel_order()` returns success after `cancelOrder()` plus a short sleep without confirming broker state. Alpaca DELETE is also not treated as terminal confirmation. This can double-fill if the cancel races a fill or is rejected/transiently delayed.

Requirements:
- Resubmit only after a verified terminal cancel or zero-remaining state from the broker.
- If cancel fails, is ambiguous, times out, or re-query still reports an open/fillable order, do not submit a replacement. Mark the open order `needs_reconcile` or an equivalent explicit blocked state, emit a critical execution alert, and preserve enough metadata for operator reconciliation.
- Handle partial fills correctly: if the original order partially fills and then is confirmed canceled, replacement quantity must be remaining quantity only.
- Prefer broker-native atomic replace APIs where available, but keep a safe fallback.
- Apply the same policy to `execution_open_order_manager.py` and `execution_microstructure.py`, or consolidate duplicate logic into one tested helper.
- Strengthen IBKR and Alpaca adapter contracts so `cancel_order` communicates whether terminal cancel was actually verified.
- Add regression tests for: cancel exception, cancel returns no verification, cancel accepted but broker still open, cancel confirmed canceled, cancel after partial fill, and max-attempt escalation.

Suggested files to inspect:
- `engine/execution/execution_open_order_manager.py`
- `engine/execution/execution_microstructure.py`
- `engine/execution/broker_alpaca_rest.py`
- `engine/execution/broker_ibkr_gateway.py`
- `engine/execution/execution_quality_supervisor.py`
- `tests/test_broker_order_idempotency_regressions.py`
- `tests/test_execution_event_freshness.py`

Done criteria:
- No code path submits a replacement order unless the original order is terminal-canceled or zero-remaining.
- Ambiguous cancel outcomes fail closed into reconciliation, not replacement.
- Tests prove the double-fill race is blocked.

## Prompt 4 - Fail Closed on Low-Observation Champion Promotion

You are working in `/home/david/gitsandbox/system/system`. The repo is dirty; do not revert unrelated user changes.

Deep dive and fix low-observation challenger promotion. Current evidence: `engine/strategy/champion_manager.py::_evaluate_promotion_stat_gate()` returns `passed=True` with `status="insufficient_observations_advisory"` when observations are below `CHAMPION_PROMOTION_MIN_OBSERVATIONS` and legacy stat/CPCV gates are not enabled. That allows promotion decisions with only the lightweight `_candidate_is_eligible()` floor.

Requirements:
- In live and paper promotion paths, insufficient observations must be non-promotable: return `passed=False` and block assignment.
- Decide whether safe/shadow modes should retain advisory behavior; if so, make that mode distinction explicit and tested.
- Call `assess_challenger()` whenever enough aligned evidence exists, regardless of legacy env flags.
- Update metadata so blocked promotions clearly report `insufficient_observations` and required/min/current observation counts.
- Enable or require statistical/CPCV governance in production env templates and prod preflight, or document why the new non-bypassable gate is sufficient.
- Update tests that currently expect disabled legacy gates to be ignored. Add tests for below-min observations with no incumbent, with incumbent, with live mode, and with paper mode.

Suggested files to inspect:
- `engine/strategy/champion_manager.py`
- `engine/strategy/promotion_guard.py`
- `engine/strategy/statistical_gates.py`
- `engine/strategy/cpcv.py`
- `deploy/compose/.env.example`
- `deploy/env/trading.env.example`
- `engine/runtime/prod_preflight.py`
- `tests/test_model_competition_real_pnl.py`
- `tests/test_drift_triggered_retrain.py`
- `tests/test_champion_promotion_identity.py`

Done criteria:
- A challenger with fewer than the required observations cannot become champion in live/paper.
- Promotion diagnostics are clear and auditable.
- Production config no longer depends on off-by-default statistical controls for this safety property.

## Prompt 5 - Add Production Backend CI and Staging Prod-Preflight Evidence

You are working in `/home/david/gitsandbox/system/system`. The repo is dirty; do not revert unrelated user changes.

Deep dive and build CI coverage for the production storage/cache path. Current evidence: tests default to SQLite in `tests/conftest.py`, and `requires_postgres` / `requires_redis` tests auto-skip when services are unreachable. The GitHub workflow runs contract tests and `tools/validate_repo.py`, but does not provision Postgres/Redis as a hard-gated production-backend job.

Requirements:
- Add a CI job that provisions reachable Postgres and Redis services and sets `TS_STORAGE_BACKEND=postgres`, `TS_PG_DSN`, and `TS_REDIS_URL`.
- Run all `requires_postgres` and `requires_redis` tests in that job, and fail the job if the marked tests are skipped unexpectedly.
- Run targeted production-path tests for schema migrations, idempotency uniqueness, audit chain behavior, arming persistence, promotion evidence, and Redis kill-switch/live-cache wrappers.
- Add a staging `prod_preflight.py --json` harness or workflow step that runs against a designated test Postgres without touching unknown local DBs.
- Persist or print redacted evidence from staging preflight so release signoff can prove the production backend path was exercised.
- Keep the existing Linux SQLite contract job, but make it explicit that it is not the production-backend gate.
- Add docs for local reproduction of the Postgres/Redis CI job.

Suggested files to inspect:
- `.github/workflows/validate.yml`
- `tests/conftest.py`
- `engine/runtime/staging_prod_preflight.py`
- `engine/runtime/prod_preflight.py`
- `tools/validate_repo.py`
- `tests/test_storage_pg_runtime_regressions.py`
- `tests/test_storage_locks_pg.py`
- `tests/test_promotion_guard_fdr.py`
- `tests/test_cpcv.py`
- `tests/test_model_competition_real_pnl.py`

Done criteria:
- CI has a hard-gated Postgres/Redis production-backend job.
- Marked production-backend tests no longer silently no-op in the go-live gate.
- Staging prod-preflight evidence is generated and redacted.

## Prompt 6 - Add Kill-Switch Cache TTL and Stale-Cache Fail-Closed Semantics

You are working in `/home/david/gitsandbox/system/system`. The repo is dirty; do not revert unrelated user changes.

Deep dive and fix kill-switch cache freshness. Current evidence: `engine/cache/wrappers/kill_switch.py` uses `KILL_SWITCH_TTL_S = None`, so a stale Redis snapshot can be reused indefinitely. A stale clear snapshot could mask a tripped DB kill switch until cache invalidation happens successfully.

Requirements:
- Add a bounded TTL for kill-switch cache snapshots, configurable by env with a conservative production default.
- Include `loaded_ts_ms` / `source` / `max_age_ms` in cached snapshots and enforce freshness on read.
- If cache is stale and DB reload fails, return a fail-closed provider-unavailable kill-switch snapshot, not an empty/clear state.
- Ensure kill-switch activation/clear paths invalidate or re-prime cache after DB commit.
- Add an optional periodic re-prime job or integrate with existing runtime jobs so cache freshness does not rely only on writes.
- Update broker-router/order-path tests so stale-but-valid cache data cannot permit orders when DB truth is tripped or DB cannot be checked.
- Add metrics/logging that expose cache age and fallback source.

Suggested files to inspect:
- `engine/cache/wrappers/kill_switch.py`
- `engine/cache/store.py`
- `engine/execution/kill_switch.py`
- `engine/runtime/gates.py`
- `engine/execution/broker_router.py`
- `engine/runtime/job_registry.py`
- `tests/test_cache_wrappers_integration.py`
- `tests/test_kill_switch_regressions.py`
- `tests/test_real_capital_safety_e2e.py`

Done criteria:
- Stale kill-switch cache cannot allow trading.
- Cache freshness behavior is tested at wrapper and execution-gate levels.
- Operators can see cache age/source in health or diagnostics.

## Prompt 7 - Require Signed Backup/Restore Evidence for Live

You are working in `/home/david/gitsandbox/system/system`. The repo is dirty; do not revert unrelated user changes.

Deep dive and harden backup/restore evidence integrity. Current evidence: signature verification exists in `engine/runtime/backup_evidence.py`, but live templates currently set or default `BACKUP_EVIDENCE_REQUIRE_SIGNATURE=0`. Unsigned self-reported restore evidence can pass if the report fields are fresh and successful.

Requirements:
- Require backup evidence signatures in live/prod preflight by default.
- Update compose and systemd/env templates to set `BACKUP_EVIDENCE_REQUIRE_SIGNATURE=1` and a required `BACKUP_EVIDENCE_HMAC_KEY_FILE` or equivalent secret reference.
- Update backup evidence generation scripts to sign the canonical evidence payload with HMAC-SHA256 and include key id, signed timestamp, payload hash, and signature.
- Ensure unsigned, malformed, wrong-key, stale-signature, and payload-tampered evidence fail preflight.
- Keep non-live developer workflows ergonomic, but make the live path fail closed.
- Add tests for signed pass and unsigned/tampered failure.
- Update install and operations docs with key creation, storage permissions, rotation, and verification commands.

Suggested files to inspect:
- `engine/runtime/backup_evidence.py`
- `ops/backup/backup_restore_evidence.sh`
- `ops/server/install_backup_evidence_gate.sh`
- `deploy/compose/.env.example`
- `deploy/env/trading.env.example`
- `deploy/compose/README.md`
- `docs/PRODUCTION_CHECKLIST.md`
- `tests/test_backup_restore_evidence_pipeline.py`
- `tests/test_real_capital_safety_e2e.py`

Done criteria:
- Live preflight rejects unsigned or unverifiable backup evidence.
- Evidence generation produces verifiable signatures.
- Tests prove tampering is detected.

## Prompt 8 - Validate Data Source Master Key Strength and Placeholder Values

You are working in `/home/david/gitsandbox/system/system`. The repo is dirty; do not revert unrelated user changes.

Deep dive and harden `DATA_SOURCE_MASTER_KEY` validation. Current evidence: `services/credential_encryption.py` accepts arbitrary raw text and hashes it. This allows weak or placeholder master keys to protect provider/broker credentials at rest.

Requirements:
- In prod/live, reject missing, placeholder, short, low-entropy, and known-default `DATA_SOURCE_MASTER_KEY` values.
- Prefer requiring base64-encoded 32-byte random key material for prod/live. If raw text is still allowed for dev, make that explicitly non-production.
- Validate key files as strictly as env keys, including permissions where practical.
- Add a preflight/config-schema check so weak master keys block go-live before the operator UI can store secrets.
- Update install scripts to generate valid key material and set file permissions.
- Add tests for valid base64 32-byte keys, valid key files, placeholder strings, short strings, malformed base64, empty files, and prod/live rejection.
- Update docs to show key generation and rotation commands.

Suggested files to inspect:
- `services/credential_encryption.py`
- `services/data_source_manager.py`
- `engine/runtime/config_schema.py`
- `engine/runtime/prod_preflight.py`
- `deploy/install_trading_system.sh`
- `deploy/compose/.env.example`
- `deploy/env/trading.env.example`
- `tests/test_credential_encryption_env_key.py`
- `tests/test_runtime_config_schema.py`
- `tests/test_secrets_provider_plaintext.py`

Done criteria:
- Weak credential-encryption master keys cannot pass live/prod preflight.
- Existing install paths generate acceptable key material.
- Tests cover env and file-based keys.

## Prompt 9 - Prevent Rules AUTO_RESUME from Clearing Manual/Operator Halts

You are working in `/home/david/gitsandbox/system/system`. The repo is dirty; do not revert unrelated user changes.

Deep dive and fix rules-engine auto-resume behavior. Current evidence: `engine/strategy/rules_engine.py` defaults `RULES_AUTO_RESUME=1` and calls `clear("global", "global", ...)` when rule conditions normalize. That can clear a DB-only operator emergency halt if the halt shares the global key.

Requirements:
- Default `RULES_AUTO_RESUME` to off in live/prod, or require an explicit audited opt-in.
- Make the rules engine clear only halts it created. Persist ownership metadata such as `actor=rules_engine`, `trigger=drawdown|drift|exec_winrate|cost_spike`, and only clear when the active DB row matches that ownership.
- Never clear manual/operator/emergency-stop/global hold rows created by the operator API, startup gates, preflight, or break-glass workflows.
- Add a separate explicit operator endpoint or workflow for clearing manual halts, with confirmation and audit.
- Add tests for: rules-created halt auto-clears when enabled, operator-created halt does not auto-clear, live default disables auto-resume, and mixed triggers do not overwrite each other.
- Update readiness docs so operators understand automatic vs manual halt ownership.

Suggested files to inspect:
- `engine/strategy/rules_engine.py`
- `engine/execution/kill_switch.py`
- `engine/api/api_operator_handlers.py`
- `engine/runtime/live_execution_control.py`
- `engine/runtime/gates.py`
- `tests/test_kill_switch_regressions.py`
- `tests/test_guard_contract_regressions.py`
- `tests/test_real_capital_safety_e2e.py`

Done criteria:
- Automatic rules recovery cannot clear an operator/manual/emergency halt.
- Live/prod auto-resume behavior is explicit, audited, and tested.
- Operator docs distinguish automatic rule halts from manual capital-safety holds.
