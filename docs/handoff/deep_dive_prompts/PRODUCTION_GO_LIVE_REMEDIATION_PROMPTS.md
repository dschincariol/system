# Production Go-Live — Required-for-GO Remediation Deep-Dive Prompts (GO-R1 … GO-R11)

> Source audit: `docs/handoff/verification/PRODUCTION_GO_LIVE_READINESS_AUDIT_REPORT.md` (2026-06-25, Git HEAD `8184174`, branch `codex/worktree-production-readiness`). Verdict **NO-GO** on 10 open P1s (0 P0). Each prompt below flips exactly one Required-for-GO item. They are independent — run in any order; GO-R5/R6 share a file and are best done together.
>
> **How to run (each):** `cd /home/david/gitsandbox/system/system`, start Codex with approvals **on**, paste one prompt. Make the change in production code (not just tests/docs), keep all existing safe-mode/fail-closed gates intact, then perform the self-audit the prompt ends with. Line numbers are from HEAD `8184174` — re-confirm before editing.
>
> Every prompt ends with this **mandatory self-audit footer** (already appended):
> *"After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work."*

---

## GO-R1 — Make risk sizing-overlays fail CLOSED (P1)

ROLE: Senior trading-risk engineer. TARGET: `/home/david/gitsandbox/system/system`. Read-then-change; preserve every existing fail-closed gate.

PROBLEM. The portfolio risk sizing **overlays** fail OPEN: in `engine/risk/portfolio_risk_engine.py` (`apply_portfolio_risk_engine`) each of `_apply_drawdown_throttle` (~3084-3087), `_apply_alpha_decay_throttle` (~3160-3163), `_apply_symbol_vol_caps` (~3165-3168), `_apply_corr_cluster_caps` (~3170-3173), and `_apply_portfolio_vol_target` (~3175-3185) is wrapped in a `try/except` that only calls `_warn_nonfatal` and leaves `out` UNCHANGED. `_post_constraint_checks` (~1577-1682) re-validates only the gross/net/symbol/asset/sector/strategy/options HARD caps — it does NOT re-derive the overlay reductions. Separately, in `engine/strategy/portfolio.py`, the rebalance stages `vol_target` (~5459), `capital_at_risk` (~5899-5901), `temporal_dampener` (~5882-5884), and `exposure_netting` (~5914-5917) only call `_record_degraded_phase`, and their codes (`PORTFOLIO_VOL_TARGET_FAILED`, `PORTFOLIO_CAPITAL_AT_RISK_FAILED`, etc.) are NOT in `_PORTFOLIO_EXECUTION_BLOCKED_DEGRADED_CODES` (~1116-1123). So a silent overlay failure under degraded inputs lets orders emit at pre-overlay (larger / more concentrated / more correlated) sizes with no `blocked` flag.

REQUIRED CHANGE. Make a failure of any *enabled* overlay fail closed in live. Implement BOTH layers:
1. In `engine/risk/portfolio_risk_engine.py`: when an enabled overlay's `try/except` fires AND the runtime is live/required, set a `blocked`/degraded marker in the returned `info` (e.g. `info["overlay_failed"]=<name>`) and have `_post_constraint_checks` (or the caller) treat a required-overlay failure as `blocked=True` → `portfolio_risk_block=1`. Optionally assert each enabled overlay left its expected applied-scale key in `info` and block if missing.
2. In `engine/strategy/portfolio.py`: add `PORTFOLIO_VOL_TARGET_FAILED`, `PORTFOLIO_CAPITAL_AT_RISK_FAILED`, `PORTFOLIO_TEMPORAL_DAMPENER_FAILED`, `PORTFOLIO_EXPOSURE_NETTING_FAILED` to `_PORTFOLIO_EXECUTION_BLOCKED_DEGRADED_CODES` so `_apply_rebalance_execution_block_stage` short-circuits.
Keep the gross/net hard caps and their post-check exactly as-is (they already bind). Do not change behavior when overlays succeed. Confirm the gate reads `portfolio_risk_block` (`engine/runtime/gates.py:1253-1272`) and denies on it.

VERIFY. Add a fault-injection test that forces each enabled overlay to raise in a live/required runtime and asserts the batch is blocked (`portfolio_risk_block=1` / execution short-circuited), and a control test that overlays-succeed still emits orders.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## GO-R2 — Alarm disk-fill & WAL-archiver outage at RUNTIME in every mode (P1)

ROLE: SRE / observability engineer. TARGET: `/home/david/gitsandbox/system/system`. Read-then-change; do not weaken any existing fail-closed gate.

PROBLEM. The documented root-disk-fill / WAL-archiver-failure incident class is un-alarmed in the modes actually run (safe/sim/paper/shadow). `engine/strategy/jobs/observability_snapshot.py:326-330` `_emit_storage_wal_alerts()` returns early unless `_production_storage_alerts_enabled()` (~144-152) is true (PROD_LOCK / ENGINE_SUPERVISED / ENGINE_MODE∈{prod,live} only). In `engine/runtime/health.py`, `storage_wal_guards` only contributes to `ok` when `required` (~4523-4524), which is itself live/flag-gated (~2777-2783); `disk_pressure` is computed (~4444) but is referenced in **no** `out["ok"]` branch. `engine/runtime/backup_evidence.py:1021-1035,1129-1131` skip/downgrade the WAL-archiver probe unless required. Net: a filling disk or broken archiver leaves `/api/health.ok=true`, no alert row, no component-health error.

REQUIRED CHANGE. Treat disk-pressure and WAL-archiver health as **durability/availability** signals that alarm regardless of trading mode (these are not trading signals):
1. `engine/runtime/health.py`: make a CRITICAL `disk_pressure` (free-space critical) a contributor to `out["ok"]` / `critical_blockers` in all modes; add a `disk_pressure_ok` derivation referenced in every `ok` branch.
2. `engine/strategy/jobs/observability_snapshot.py`: let `_emit_storage_wal_alerts` emit `STORAGE_FREE_SPACE_CRITICAL` and a confirmed `WAL_ARCHIVER_OUTAGE` even when `_production_storage_alerts_enabled()` is false (gate only the *trading-specific* suppression behind prod-lock, not the durability alarm). Keep fingerprint/dedupe so it does not page every cycle.
Do not make safe-mode fail on routine backup churn — alarm on *critical free space* and *confirmed* archiver outage only.

VERIFY. With `ENGINE_MODE=paper`, simulate a critical free-space row and a stalled archiver; assert `/api/health.ok=false`, a `STORAGE_FREE_SPACE_CRITICAL` row written, and a `WAL_ARCHIVER_OUTAGE` alert; assert no alarm on a healthy disk.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## GO-R3 — Alert delivery: fail go-live when no channel is configured; deliver equity-recon pages (P1)

ROLE: SRE / on-call engineer. TARGET: `/home/david/gitsandbox/system/system`. Read-then-change.

PROBLEM. No alert reaches a human off-box by default. `engine/runtime/alerts_notify.py` (`_channel_status` ~445-485; `send_runtime_alert_notification` ~863-899; `send_runtime_health_notification` ~1160-1172) enables a channel only when `EQ_CRIT_SMTP_HOST`+`EQ_CRIT_EMAIL_TO` or `EQ_CRIT_WEBHOOK_URL` are set — all default empty (`engine/runtime/dashboard_config.py:93-99`) and appear in **no** env template. `engine/runtime/alerts.py:608` `emit_runtime_alert` only persists to DB. Equity-reconciliation (`engine/runtime/equity_drift.py:421,463`) calls **only** `emit_runtime_alert`, so equity-mismatch CRITICAL has no delivery path even when channels ARE configured. The auto-kill monitor (`kill_health_monitor.py:46`) and WAL/storage alerts route through the no-op delivery.

REQUIRED CHANGE.
1. Add a go-live gate in `engine/runtime/prod_preflight.py` that calls `get_notification_channel_status()` and emits a BLOCKER (live) / hard-WARN when zero channels are enabled (today preflight checks only "alerts schema ok" ~1530-1531).
2. Route equity-reconciliation CRITICAL through `send_runtime_alert_notification` (in addition to `emit_runtime_alert`) so a configured channel actually pages.
3. Add `EQ_CRIT_SMTP_HOST`/`EQ_CRIT_EMAIL_TO`/`EQ_CRIT_WEBHOOK_URL` to `deploy/env/trading.env.example` with a comment marking ≥1 channel **required for live**.
4. Make `send_runtime_alert_notification`/`send_runtime_health_notification` log a WARN once per process when a CRITICAL delivers to zero channels.
Operator runbook step (document in `docs/FAILURE_MODES.md`): provision SMTP or a webhook before go-live.

VERIFY. Preflight blocks with no channel set; a simulated CRITICAL with a test webhook delivers (`delivered>0`); equity-recon CRITICAL reaches the channel.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## GO-R4 — Off-host backup leg + fail-closed offsite evidence (P1)

ROLE: DR / backups engineer. TARGET: `/home/david/gitsandbox/system/system`. Read-then-change.

PROBLEM. All recovery points are single-host. `ops/backup/base_backup.sh:228-232` runs the offsite copy only when `TS_BASE_BACKUP_OFFSITE_CMD` is set; `deploy/compose/.env:169-170` ships `TS_BASE_BACKUP_OFFSITE_CMD=` / `TS_OFFSITE_BACKUP_DEST=` empty; `ops/backup/offsite_base_backup_stub.sh` is dead code (never invoked). `ops/backup/backup_restore_evidence.sh` has no offsite-freshness assertion, so the signed evidence gate **passes with zero off-host copies**. base + WAL + drills all live on the single `zpool/trading-backups`.

REQUIRED CHANGE.
1. Add an **offsite-freshness check** to `ops/backup/backup_restore_evidence.sh`: assert a recent off-host base copy (and WAL) exists within a configurable max-age, surface `offsite={pass|fail|disabled}` in the signed evidence JSON, and make the gate **fail closed** when off-host is required (live) but stale/absent. Add an `OFFSITE_REQUIRED`/age env knob.
2. Wire WAL off-host via `TS_WAL_OFFSITE_CMD` (referenced ~`backup_restore_evidence.sh:401`).
3. Document operator config (`docs/DISK_RETENTION_RUNBOOK.md`): set `TS_BASE_BACKUP_OFFSITE_CMD`/`TS_OFFSITE_BACKUP_DEST` (S3 or a separate mounted volume) and the WAL offsite command.
Keep on-host base+WAL behavior unchanged; the change adds the off-host requirement and its evidence, it does not remove local backups.

VERIFY. Evidence JSON reports `offsite=pass` with a fresh off-host tarball when configured; the gate returns non-zero (`offsite=fail`) when the off-host copy is removed/stale and `OFFSITE_REQUIRED=1`.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## GO-R5 — Hashed lockfiles + `--require-hashes` enforced on the install path (P1)

ROLE: Supply-chain / build engineer. TARGET: `/home/david/gitsandbox/system/system`. Read-then-change. (Best done together with GO-R6 — same validator + manifests.)

PROBLEM. Installs are version-pinned but **not hash-verified**. `requirements.lock.txt` / `requirements-dev.lock.txt` were generated by `uv pip compile` without `--generate-hashes` (`grep -c -- --hash` = 0). `deploy/compose/Dockerfile.runtime:23-30` installs with plain `python -m pip install -r "$REQ_FILE"` (no `--require-hashes`); locks resolve across pypi + download.pytorch.org with `--index-strategy unsafe-best-match`. `tools/validate_dependency_lock.py` has no hash logic. A poisoned/republished transitive wheel (torch, transformers, ccxt, ibapi) would execute arbitrary code in the trading process; builds are not reproducible.

REQUIRED CHANGE.
1. Regenerate `requirements.lock.txt` and `requirements-dev.lock.txt` with `--generate-hashes` (every line carries `--hash=sha256:…`). Keep pins identical; only add hashes.
2. Change the install path (`deploy/compose/Dockerfile.runtime` and any `pip install -r requirements.txt` site) to `pip install --require-hashes -r requirements.txt`.
3. Add a rule to `tools/validate_dependency_lock.py` (run in `.github/workflows/validate.yml`) that FAILS when any line in a runtime lock lacks a `--hash`. Wire it into `--strict`.
Note the two-index resolution in the lock header; ensure the pytorch CPU index wheels are hashed too.

VERIFY. `grep -c -- --hash requirements.lock.txt` > 0; the Dockerfile install line contains `--require-hashes`; `python tools/validate_dependency_lock.py --strict` exits 0 and would fail if a hash were removed.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## GO-R6 — Per-profile transitive lock for the ROCm/CUDA dependency profiles (P1)

ROLE: Supply-chain / build engineer. TARGET: `/home/david/gitsandbox/system/system`. Read-then-change.

PROBLEM. The GPU profiles install an **unconstrained transitive closure** on the model-running hosts. `requirements.txt:5` applies `-c requirements.lock.txt` for CPU, but `requirements-amd-rocm-full.txt` has no `-c`/`-r`/lock line (hand-maintained re-pin) and is installed live (`deploy/compose/Dockerfile.runtime:25-27`, `docker-compose.amd-rocm.yml:7-16`); `requirements-nvidia-cuda.txt:1` is `-r requirements-base.txt` + torch cu121 with no lock. The CI validator (`tools/validate_dependency_lock.py`) only enforces the lock for `requirements.txt` (RUNTIME_INSTALL_MANIFESTS), so it explicitly exempts the GPU profiles. Pure-transitive deps (aiohttp, pydantic/pydantic-core, httpx/anyio, sqlalchemy/alembic, tokenizers/safetensors, …) drift every rebuild with no integrity or reproducibility.

REQUIRED CHANGE.
1. Generate hashed per-profile lockfiles: `requirements-amd-rocm.lock.txt` and `requirements-nvidia-cuda.lock.txt` (use `--generate-hashes`, honoring the ROCm/cu121 torch index).
2. Reference them via `-c <profile>.lock.txt` in `requirements-amd-rocm-full.txt` / `requirements-nvidia-cuda.txt`; install GPU profiles with `--require-hashes`.
3. Extend `tools/validate_dependency_lock.py` to assert every runtime profile manifest carries a `-c *.lock.txt` constraint (fail otherwise).
Keep the direct torch/triton ROCm/CUDA pins exactly as-is.

VERIFY. Each GPU manifest contains a `-c *.lock.txt` line; `validate_dependency_lock.py` fails when a profile lacks its lock and passes with it; a GPU build resolves the same transitive closure twice.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## GO-R7 — `FX_LIVE_TRADING_ENABLED` disable-by-default gate (P1)

ROLE: Execution-safety engineer. TARGET: `/home/david/gitsandbox/system/system`. Read-then-change; mirror the existing futures gate exactly.

PROBLEM. FX uniquely lacks a live-disable-by-default gate. `engine/execution/broker_router.py:897-913` `_fx_order_safety_block` returns `None` (allows the batch) whenever an FX-capable (IBKR) broker is in the chain, and only blocks with `fx_broker_unavailable` when none is present — it never checks any FX live flag. `broker_ibkr_gateway.py:956-966` `_mk_fx_contract` builds a live CASH/IDEALPRO contract. Futures hard-block by default (`broker_router.py:1062`, `not _env_bool("FUTURES_LIVE_TRADING_ENABLED", False)`), crypto via `portfolio_risk_gate.py:405-414`, options via `broker_router.py:1869`. A repo-wide grep for `FX_LIVE_TRADING_ENABLED` returns nothing. Because IBKR is also the intended live equity broker, enabling FX research (`FX_PAIRS_ENABLED=1`) while live routes real FX orders to IDEALPRO with no dedicated toggle.

REQUIRED CHANGE. In `engine/execution/broker_router.py:_fx_order_safety_block`: when the batch has FX, a LIVE broker is in the chain, and `not _env_bool("FX_LIVE_TRADING_ENABLED", False)`, return a `stop_failover` block with status/reason `fx_live_trading_disabled_by_default` — symmetric with `_futures_order_safety_block`. Mirror the same guard on the IBKR live FX submit path (`broker_ibkr_gateway.py`). Default the flag OFF. Do not change sim/paper behavior or the FX sleeve/leverage caps.

VERIFY. A test parallel to the futures/crypto block tests: live IBKR chain + FX order with `FX_LIVE_TRADING_ENABLED` unset → blocked (`fx_live_trading_disabled_by_default`); with it set → proceeds. Add FX to the multi-asset preflight snapshot (`engine/runtime/live_trading_preflight.py`).

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## GO-R8 — Operator-UI crash resilience: fallback Emergency Stop + error boundary (P1)

ROLE: Front-end / operational-safety engineer. TARGET: `/home/david/gitsandbox/system/system`. Read-then-change.

PROBLEM. The operator console's halt control dies silently on any asset fault. `boot/operator_ui.html:1177` is the only `<script>` (one `type=module`); its three top-level static imports (`:1198-1214`: `/ui/runtime_status_summary.js`, `/ui/runtime_diagnostics.js`, `/ui/state_presenter.js`; the first transitively imports `market_stress_thresholds.js`) must all evaluate before `window.emergencyStopHard = emergencyStopHard` binds at `:3532`. The Emergency Stop buttons are static HTML `onclick="emergencyStopHard()"` (`:837`, `:1130`). There is no `window.onerror`/`unhandledrejection` boundary and no `nomodule` fallback. A 404 / wrong-MIME / syntax fault on any of those assets (e.g. a partial upgrade via the `operator.system_update` workflow) leaves the buttons rendered but inert ("emergencyStopHard is not defined").

REQUIRED CHANGE. Implement BOTH:
1. A small SEPARATE inline **non-module** `<script>` that defines a minimal `emergencyStopHard()` fallback POSTing to `/api/operator/emergency_stop` with a basic typed-confirm prompt — so the halt path survives module-load failure. Have the module overwrite it with the full handler on successful eval.
2. A `window.onerror` + `window.addEventListener('unhandledrejection', …)` boundary that, on module-eval failure, renders a visible red "Operator UI failed to load — Emergency Stop is in degraded fallback mode" banner.
Keep the confirmation contract (the fallback still requires a typed token). Do not weaken any backend gate.

VERIFY. Serve a deliberately broken or 404 `/ui/state_presenter.js`; confirm (a) the degraded banner shows and (b) clicking Emergency Stop still reaches `/api/operator/emergency_stop`. `node --check` the touched JS.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## GO-R9 — Guard operator sidecar `/start` & `/restart`; add global rejection handlers (P1)

ROLE: Node/operational-safety engineer. TARGET: `/home/david/gitsandbox/system/system`. Read-then-change.

PROBLEM. The two most-pressed incident routes can crash the only operator control plane. `boot/operator_server.js:6154` `app.post('/api/operator/start', async …)` has no enclosing try/catch (only a trailing bootstrap block ~6383-6445 is wrapped); `:6459` `app.post('/api/operator/restart', async …)` is fully unguarded (`stopEngine()` 6465, `sleep` 6466, `startEngine(mode)` 6467). `startEngine` (`:2916`, fs ops `:2931-2936`) throws on EACCES/ENOSPC (documented disk-fill history). `grep -c 'unhandledRejection|uncaughtException'` = 0; Express 4 does not forward async-handler rejections to the error middleware (`:419`); launched as plain `node` on Node 20 (default `throw`), an unhandled rejection **terminates the process**.

REQUIRED CHANGE.
1. Wrap the `/start` and `/restart` handler bodies with the existing `wrapOperatorRoute` (`:252-264`, already used by ~30 routes and proven to catch async errors) or an explicit try/catch returning `jsonFail(…, 500)`.
2. Add `process.on('unhandledRejection', …)` and `process.on('uncaughtException', …)` that log via `logOperatorCatch` and keep the process alive (or fail closed deliberately) rather than relying on Node's default crash.
Keep the confirmation gating, bounded-timeout, and reason-mapping already present.

VERIFY. Simulate an fs fault (e.g. make the env file unwritable / fill the log dir) and press Start: the sidecar returns a 500 JSON and **stays up**; repeat for Restart. `node --check boot/operator_server.js`.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## GO-R10 — Feed truth: don't count yfinance as a "live" provider; require a paid rail in live (P1)

ROLE: Data-platform engineer. TARGET: `/home/david/gitsandbox/system/system`. Read-then-change. (Pairs with the operator step in GO-R11: provision paid creds.)

PROBLEM. A yfinance-only deployment can self-certify as having live data. `engine/runtime/feed_truth.py:8` `SIMULATED_PROVIDER_NAMES={'simulated'}` only, so `annotate_provider_map_liveness` (~151-193) classifies a healthy yfinance row as `live_healthy` and sets `live_market_data_ok=True`. The paid-equity degradation gate filters required providers to `_EQUITY_PRICE_PROVIDERS=('polygon_ws','polygon','ibkr')` (`engine/runtime/prod_preflight.py:84,2271`); if none is enabled, `required_equity` is empty and the gate passes with reason `no_required_equity_provider` (~2281-2284). The system is otherwise correctly fail-closed on missing creds (422), but free/ToS-restricted yfinance must not be treated as a production rail.

REQUIRED CHANGE.
1. Add a "free/fallback, not-live" classification (e.g. extend `feed_truth.py` so yfinance does NOT set `live_market_data_ok=True` in paper/live), OR
2. Make `_paid_equity_provider_degradation_gate` (`prod_preflight.py`) emit a BLOCKER in paper/live when `required_equity` is empty AND only free/simulated providers are enabled (i.e. require ≥1 paid equity provider enabled+live before arming).
Pick the approach that keeps research/dev (non-live) unaffected. Do not change the existing missing-credential → 422 fail-closed behavior.

VERIFY. A paper-mode preflight with only yfinance enabled returns a blocker (not pass); with a paid provider live, `live_market_data_ok=true` and the gate passes. Operator step (document): provision Polygon/IBKR (+ news/earnings/options) `*_FILE` secrets at `/etc/trading/secrets/*` (chmod 600) and confirm `POST /api/data_sources/test` → `status==pass`.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## GO-R11 — Go-live config hygiene + host hardening so `validate_repo --live` passes (operator + small dev assists)

ROLE: Release/operator engineer (with dev assists). TARGET: `/home/david/gitsandbox/system/system`. Mostly operator filesystem/systemd actions on the host; the dev portions are unit-file + doc edits in the repo.

PROBLEM (gate working as designed; config not yet satisfying it).
1. `tools/validate_repo.py --live` → `runtime_graph_check --mode startup` fails because operator-local **untracked** files carry inline plaintext secrets and an over-permissive key: `.env:138,140,238` set inline `TRADING_MASTER_KEY`/`APP_MASTER_KEY`/`DASHBOARD_API_TOKEN` (`secret_sources.py:262-275,538-548,599-608` → `inline_secret_env`/`repo_local_inline_secret`); `/etc/trading/backup_evidence.hmac.key` is `0640 root:trading` (`secret_sources.py:471` rejects group/other bits). The shipped templates are already correct (`*_FILE`/`*_SECRET`).
2. Host memory hardening is not applied: only a 512 MiB `/swapfile`, no active zram (runbook specifies 16 GiB swap + 32 GiB zram + 48 GiB ARC cap, `docs/MEMORY_PRESSURE_RUNBOOK.md`); `deploy/systemd/trading-engine.service` has no `WatchdogSec`/`MemoryMax`/`OOMScoreAdjust`.

REQUIRED ACTIONS.
- **Operator (host):** remove the inline `*_MASTER_KEY`/`*_API_TOKEN` values from `.env` and `deploy/compose/.env` (keep only the `*_FILE`/`*_SECRET` forms already shipped); `sudo chmod 0600 /etc/trading/backup_evidence.hmac.key` (per `deploy/compose/README.md:29`); apply the swap/zram hardening from `docs/MEMORY_PRESSURE_RUNBOOK.md`.
- **Dev (repo):** add `WatchdogSec=<interval>` + `sd_notify WATCHDOG=1` heartbeat to `deploy/systemd/trading-engine.service` (and operator unit), and `MemoryHigh`/`MemoryMax`/`OOMScoreAdjust=<positive>` sized from the runbook budget; point `service_ctl.sh` `logs` at the file sink the units actually write (or switch units to journald) for consistency.

VERIFY. `python tools/validate_repo.py --live` and `python tools/runtime_graph_check.py --mode startup` both exit 0; `swapon --show` shows ≥16 GiB total; `systemctl show trading-engine -p MemoryMax -p WatchdogSec` are set; no secret value is printed in any output.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.


---

# Part B — P2 Hardening & P3 Polish Remediation Prompts (HG-1 … HG-13, PL-1 … PL-13)
> Added 2026-06-26 to cover **every remaining audit recommendation** (Part A above = GO-R1…GO-R11, the P1 blockers + go-live config). Each prompt below is self-contained: re-confirm the cited file:line at HEAD `8184174` before editing, design+implement the optimal solution, then run the self-audit footer. HG = P2 (fix soon); PL = P3 (polish). Independent — run in any order.

## P2 — Hardening (HG)

### HG-1 — Fail-closed when portfolio risk engine/gate disabled in live (non-bypassable gross/net notional backstop) (P2)

ROLE: You are a senior risk-systems engineer hardening the portfolio exposure backstop so that disabling the risk layers cannot silently remove all gross/net notional caps in live.

TARGET: /home/david/gitsandbox/system/system

PROBLEM
The two portfolio-level exposure caps are each fully removable by a single env flag, and there is NO independent notional clamp behind them. Confirmed at HEAD 8184174 (branch codex/worktree-production-readiness):

- engine/risk/portfolio_risk_engine.py:112 — `USE = os.environ.get("PORTFOLIO_USE_RISK_ENGINE", "1") == "1"`. The gross/net caps it enforces (MAX_GROSS default 1.00 at line 117, MAX_NET default 0.60 at line 118; applied at lines 1766 and 1785) only run when USE is truthy.
- engine/risk/portfolio_risk_engine.py:3028-3038 — when `not USE`, `apply_portfolio_risk_engine` sets `portfolio_risk_block` -> "0", status "disabled", and `return desired, {"enabled": False}` (allocations pass through UNCLAMPED).
- engine/strategy/portfolio_risk_gate.py:45 — `USE = os.environ.get("PORTFOLIO_USE_RISK_GATE", "1") == "1"`. GROSS_CAP (default 1.00, line 53) / MAX_NET (default 0.60, line 47) live here too.
- engine/strategy/portfolio_risk_gate.py:1359-1360 — when `not USE`, `apply_portfolio_risk_gate` does `return desired, {"enabled": False}` (no clamp).
- The only remaining portfolio-level transform, `_apply_total_portfolio_risk_limit` (engine/strategy/portfolio.py:2220-2257, env PORTFOLIO_TOTAL_RISK_LIMIT default 0.030), is a REALIZED-VOL scale, NOT a gross/net notional cap, and is itself a no-op when the limit is <= 0.
- Pipeline orchestration in engine/strategy/portfolio.py `_apply_rebalance_risk_gates_stage` calls the engine at line 5841, the gate at line 5863, and the total-risk limit at line 5921 — so setting both PORTFOLIO_USE_RISK_ENGINE=0 and PORTFOLIO_USE_RISK_GATE=0 lets allocations of arbitrary gross/net reach execution with zero independent notional backstop.
- The downstream consumer engine/runtime/gates.py:1183 reads `portfolio_risk_block` from risk state and CRITICAL-blocks when it is "1" (gates.py:1253-1272), but when the engine is disabled it is forced to "0", so this safety read is defeated.

Net effect: in LIVE, two cooperating flags remove ALL gross/net caps with no fail-closed behavior.

DESIGN / REQUIRED CHANGE
Implement a non-bypassable final gross/net notional clamp that runs independently of both USE flags, AND fail-close to a hard block if the clamp itself cannot be computed in live. Do read-then-change.

1. New module helper. Add `engine/risk/notional_backstop.py` exporting:
   - constants read once at import:
     - `BACKSTOP_MAX_GROSS = float(os.environ.get("PORTFOLIO_BACKSTOP_MAX_GROSS", "1.00"))`
     - `BACKSTOP_MAX_NET = float(os.environ.get("PORTFOLIO_BACKSTOP_MAX_NET", "0.60"))`
     - `BACKSTOP_ENABLED = os.environ.get("PORTFOLIO_NOTIONAL_BACKSTOP", "1") == "1"` (default ON; may be turned off ONLY in non-live).
   - `def apply_notional_backstop(desired: Dict[str, Dict[str, Any]], *, is_live: bool) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]`:
     - Compute gross = sum(abs(weight)) and net = abs(sum(weight)) across `desired` entries (weight key matches existing convention used at portfolio_risk_engine.py:1766/1785 — reuse the same `float(d.get("weight", 0.0) or 0.0)` access).
     - If gross > BACKSTOP_MAX_GROSS, scale every weight by `BACKSTOP_MAX_GROSS/gross`; recompute net afterward. If net > BACKSTOP_MAX_NET, apply an additional scale `BACKSTOP_MAX_NET/net`. Annotate each touched entry's `reason["portfolio_notional_backstop"]` with pre/cap/scale, mirroring the existing `portfolio_gross_cap`/`portfolio_net_cap` reason shape.
     - Return `(clamped, meta)` where meta includes `enabled`, `gross_pre`, `net_pre`, `gross_post`, `net_post`, `scaled` (bool), and `is_live`.
     - This function MUST NOT read PORTFOLIO_USE_RISK_ENGINE / PORTFOLIO_USE_RISK_GATE.

2. Wire it as the FINAL portfolio transform. In engine/strategy/portfolio.py `_apply_rebalance_risk_gates_stage`, AFTER the `_apply_total_portfolio_risk_limit` call at line 5921 (and after exposure netting), add a stage that always runs:
   - Determine live status using the existing canonical helper rather than reinventing it: import and use `from engine.runtime.live_execution_control import live_execution_disabled`; treat `is_live = not live_execution_disabled()`. (live_execution_disabled() is defined at engine/runtime/live_execution_control.py:86 and defaults True i.e. live disabled.)
   - Call `desired, _backstop = apply_notional_backstop(desired, is_live=is_live)`; persist via `_put_meta(con, "last_notional_backstop", json.dumps(_backstop or {}, ...))` following the existing `_put_meta` pattern at lines 5847/5868/5927.
   - FAIL-CLOSED: if `is_live` and the backstop stage raises OR `BACKSTOP_ENABLED` is False, set the risk state `portfolio_risk_block` -> "1" with status "backstop_unavailable" via the same `set_state` mechanism the engine uses (engine/risk/portfolio_risk_engine.py:3032 / 3273), so engine/runtime/gates.py:1183 reads it and CRITICAL-blocks. Use the existing `_record_degraded_phase`/`_warn_nonfatal` helpers for logging; do NOT swallow the block. (When NOT live, a backstop failure may degrade-without-block, matching current non-live tolerance.)

3. Defeat the disable bypass at the source. In engine/risk/portfolio_risk_engine.py at the `if not USE:` branch (lines 3028-3038): keep returning `enabled: False`, but do NOT unconditionally force `portfolio_risk_block` to "0" when live. Instead: if `not live_execution_disabled()`, leave/raise the block by setting `portfolio_risk_block` -> "1" with status "risk_engine_disabled_live" UNLESS the notional backstop is enabled (`PORTFOLIO_NOTIONAL_BACKSTOP=1`). Concretely: import the backstop module's `BACKSTOP_ENABLED`; when live and `BACKSTOP_ENABLED` is False, set block "1"; when live and backstop enabled, it is acceptable to set "0" because the independent clamp in step 2 covers it. Mirror the same logic for the gate's `if not USE:` early return at engine/strategy/portfolio_risk_gate.py:1359-1360 (it does not write risk state today; do not add state writes there, the engine path owns `portfolio_risk_block`).

4. Env documentation. Add `PORTFOLIO_NOTIONAL_BACKSTOP`, `PORTFOLIO_BACKSTOP_MAX_GROSS`, `PORTFOLIO_BACKSTOP_MAX_NET` to the env reference doc alongside the existing PORTFOLIO_USE_RISK_* entries (grep for `PORTFOLIO_USE_RISK_ENGINE` under docs/ to find the file; if none, add to the canonical env table used for the other PORTFOLIO_* vars).

5. Tests (add under the existing portfolio/risk test dir; grep `apply_portfolio_risk_engine` in tests/ to locate it):
   - `test_notional_backstop_clamps_gross_and_net`: build a desired with gross 3.0 / net 1.5, call `apply_notional_backstop(..., is_live=True)`, assert post gross <= 1.00 and net <= 0.60 within 1e-9 and `scaled is True`.
   - `test_backstop_runs_when_both_flags_disabled`: monkeypatch env PORTFOLIO_USE_RISK_ENGINE=0 and PORTFOLIO_USE_RISK_GATE=0, run `_apply_rebalance_risk_gates_stage` (or the smallest callable wrapper) over an over-gross book in a live context (DISABLE_LIVE_EXECUTION=0), assert resulting gross <= 1.00.
   - `test_live_block_when_backstop_disabled`: live + PORTFOLIO_NOTIONAL_BACKSTOP=0 -> risk state `portfolio_risk_block` == "1" and gate output `allowed` False / reason `portfolio_risk_block`.
   - `test_nonlive_disable_does_not_block`: DISABLE_LIVE_EXECUTION=1 + risk engine disabled -> no hard block (preserves dev/sim ergonomics).

GUARDRAIL
Preserve all existing fail-closed/safe-mode gates; never enable live execution; read-then-change. Do not weaken any existing block path; the backstop and disabled-in-live block are ADDITIVE. The backstop must never widen caps above MAX_GROSS/MAX_NET and must never read the USE flags.

VERIFY
- New tests pass: `pytest -q tests -k "notional_backstop or backstop"` (use the repo's actual test invocation found near other portfolio tests).
- Bypass proof: with `DISABLE_LIVE_EXECUTION=0 PORTFOLIO_USE_RISK_ENGINE=0 PORTFOLIO_USE_RISK_GATE=0`, drive an over-gross desired book through the rebalance risk-gate stage and confirm final gross <= PORTFOLIO_BACKSTOP_MAX_GROSS (default 1.00) and net <= PORTFOLIO_BACKSTOP_MAX_NET (default 0.60).
- Fail-closed proof: with `DISABLE_LIVE_EXECUTION=0 PORTFOLIO_NOTIONAL_BACKSTOP=0`, confirm risk state `portfolio_risk_block` == "1" and that engine/runtime/gates.py returns `allowed: False`, `real_trading_allowed: False`, reason containing `portfolio_risk_block`, severity CRITICAL.
- Non-live regression: with `DISABLE_LIVE_EXECUTION=1` and the risk engine disabled, confirm no new hard block is introduced.
- `git grep -n "PORTFOLIO_NOTIONAL_BACKSTOP"` shows the new env in module, docs, and tests; confirm engine/risk/notional_backstop.py contains no reference to PORTFOLIO_USE_RISK_ENGINE or PORTFOLIO_USE_RISK_GATE.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

### HG-2 — Execution gate fails open when portfolio_risk_state read raises in live mode (P2)

ROLE: You are a senior trading-runtime engineer hardening the execution-gate fail-closed contract.

TARGET: /home/david/gitsandbox/system/system

PROBLEM
The live execution gate (engine/runtime/gates.py:execution_gate_snapshot, def at line 714) reads the portfolio risk state from runtime risk-state and is supposed to deny order flow whenever the portfolio risk block is active. It does NOT fail closed when the READ ITSELF errors.

Re-confirmed at HEAD 8184174 (branch codex/worktree-production-readiness):
- engine/runtime/gates.py:1179-1181 initialize `portfolio_risk_state = None` and resolve `get_risk_state = risk_state_getter if callable(risk_state_getter) else _get_risk_state`.
- engine/runtime/gates.py:1182-1223 read `portfolio_risk_block`/`portfolio_risk_info`/`portfolio_risk_summary`/`portfolio_risk_status`/`portfolio_risk_ts_ms` and build the `portfolio_risk_state` dict.
- engine/runtime/gates.py:1224-1230 — on ANY exception in that read, the `except` calls `_warn_nonfatal("RUNTIME_GATES_PORTFOLIO_RISK_STATE_LOAD_FAILED", ...)` and sets `portfolio_risk_state = None`, then falls through.
- engine/runtime/gates.py:1253 only blocks when `isinstance(portfolio_risk_state, dict) and portfolio_risk_state.get("blocked")`. Because the failure path set it to `None`, this guard is skipped and control proceeds to the normal mode-mapping/allow path (gates.py:1430-1493). In `mode == "live"` with arming/runtime/preflight otherwise satisfied (gates.py:1490-1493) this yields `real_trading_allowed = True` and `allow_execution_pipeline = True`. Net effect: if the portfolio-risk-state read errors, the gate can ALLOW live order flow despite having no confirmation that the portfolio risk block is clear — i.e. it fails OPEN.

The stricter sibling already fails closed: engine/runtime/execution_barrier.py:173-179 catches the same read error and returns `ExecBarrierDecision(False, "portfolio_risk_state_error", {})`.

The live submission path uses the LENIENT one: engine/execution/broker_apply_orders.py:62 imports `execution_gate_snapshot` from engine.runtime.gates; the wrapper `_execution_gate_snapshot()` at lines 84-88 calls it, and it gates real submission at line 1972 via `_execution_gate_blocks` (lines 111-112, which keys off `ok` and `allow_execution_pipeline`). So the gap is reachable on the live order path.

DESIGN / REQUIRED CHANGE
Make the live/required runtime path fail closed when the portfolio_risk_state read raises, matching execution_barrier.py, WITHOUT changing non-live behavior.

In engine/runtime/gates.py, inside `execution_gate_snapshot`:

1. At the read block (around lines 1179-1230) add a local sentinel to distinguish "read errored" from "read returned no block". Initialize before the try, e.g. `portfolio_risk_state_read_error = False`. In the `except` at lines 1224-1230, after `_warn_nonfatal(...)`, set `portfolio_risk_state = None` (unchanged) AND `portfolio_risk_state_read_error = True`. Do not raise.

2. Immediately AFTER the existing `if isinstance(portfolio_risk_state, dict) and portfolio_risk_state.get("blocked"):` block (gates.py:1253-1272) and BEFORE the kill-switch handling that begins at `ks = _default_kill_switch_state() ...` (around line 1274), insert a new fail-closed branch that fires only when the read errored AND the resolved runtime mode requires the risk-state guarantee. Compute the mode the same way the rest of the function does (the finalized `mode` string is not yet assigned in the allow path at this point, so use the already-resolved mode variable in scope — confirm whether `mode` is final here; if `mode` may still be the env default, gate instead on the live-intent condition described next).

   Define "live/required" precisely as: `mode == "live"` OR `mode == "shadow"` is NOT required (shadow does not place real orders) — only require it for `mode == "live"`. To avoid coupling to other unrelated live preconditions, the branch must block independently:

       if portfolio_risk_state_read_error and mode == "live":
           return {
               "ok": True,
               "ts_ms": ts,
               "mode": mode,
               "armed": armed,
               "allow_execution": False,
               "allow_execution_pipeline": False,
               "allow_simulation": False,
               "real_trading_allowed": False,
               "allowed": False,
               "reason": "portfolio_risk_state_read_error",
               "source": source,
               "runtime_state": runtime_state,
               "runtime_detail": runtime_detail,
               "runtime_source": runtime_source,
               "portfolio_risk": None,
               "severity": "CRITICAL",
               "severity_reasons": _dedupe_strs(severity_reasons + ["portfolio_risk_state_read_error"]),
           }

   Match the exact key set and ordering used by the adjacent block-return dicts at gates.py:1253-1272 (same keys: ok/ts_ms/mode/armed/allow_execution/allow_execution_pipeline/allow_simulation/real_trading_allowed/allowed/reason/source/runtime_state/runtime_detail/runtime_source/portfolio_risk/severity/severity_reasons). The reason string MUST be `portfolio_risk_state_read_error` (note: gates uses this; execution_barrier uses `portfolio_risk_state_error` — keep the new gates reason distinct so the two layers remain distinguishable in logs).

3. If, after re-reading, `mode` is NOT yet finalized to "live" at the insertion point (i.e. final mode mapping only happens at gates.py:1430+), then move the new fail-closed check to fire inside the `elif mode == "live":` branch (gates.py:1441) as the FIRST condition — before the `live_execution_disabled()`/armed/runtime/preflight checks — by reading the sentinel computed earlier. Implement whichever placement is correct given the actual control flow at HEAD; the invariant is: when `mode` resolves to "live" and the portfolio_risk_state read errored, the function returns a denial with `allow_execution_pipeline=False`, `allowed=False`, `reason="portfolio_risk_state_read_error"`. For `mode` in {safe, paper, shadow} the behavior MUST be byte-for-byte unchanged (no new block, no reason change).

4. Do NOT introduce a new env var; this is an unconditional fail-closed for live mode. Do NOT alter `_warn_nonfatal` call sites, the parse-failure sub-handlers (gates.py:1195-1201, 1209-1215 — those are recoverable parse fallbacks, not read errors), or execution_barrier.py.

GUARDRAIL: preserve all existing fail-closed/safe-mode gates; never enable live execution; read-then-change.

VERIFY
1. Static: `grep -n "portfolio_risk_state_read_error" engine/runtime/gates.py` shows the new reason in exactly one new return branch.
2. Unit: add a test in the existing gates test module (locate via `grep -rl "execution_gate_snapshot" --include=test_*.py` / tests/ for engine/runtime/gates) that passes a `risk_state_getter` which raises on `"portfolio_risk_block"`, with conditions otherwise sufficient for live allow (mode="live", armed from audited db, runtime LIVE, preflight ok). Assert the returned snapshot has `allow_execution_pipeline is False`, `allowed is False`, `real_trading_allowed is False`, and `reason == "portfolio_risk_state_read_error"`.
3. Regression: a parallel test with the same raising getter but `mode` in {"paper","shadow","safe"} asserts the snapshot is identical to the pre-change output (no new block, no reason change) — confirming non-live behavior is preserved.
4. Run the gates test module: `python -m pytest tests/ -k "gate" -q` (or the discovered path) passes.
5. Confirm the live submission path benefits: `_execution_gate_blocks` (engine/execution/broker_apply_orders.py:111-112) returns True for the new denial snapshot (because `allow_execution_pipeline` is False), proving the live order path at broker_apply_orders.py:1972 now blocks on a portfolio-risk read error.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

### HG-3 — Broaden plaintext secret-provider guard to all strict production triggers and block plaintext in the policy snapshot (P2)

ROLE: You are a senior platform-security engineer hardening the trading runtime's secret-provider gating.

TARGET: /home/david/gitsandbox/system/system

PROBLEM (re-confirmed at HEAD 8184174, branch codex/worktree-production-readiness)
The plaintext dev secret provider's production guard is too narrow, and the strict secret-source policy never inspects which provider is selected. A live deploy can therefore load raw on-disk secrets.

Evidence:
- services/secrets/providers/plaintext.py:23-24 — `_is_production_env()` returns True ONLY when `os.environ['TS_ENV']` lowercases to `production`. Nothing else (ENV, APP_ENV, NODE_ENV, ENGINE_MODE/EXECUTION_MODE==live, PROD_LOCK, ENGINE_SUPERVISED) is consulted.
- services/secrets/providers/plaintext.py:36-42 — the import-time guard (line 41-42 `raise`) and `_ensure_not_production()` (line 36-38, called from `load`/`delete` at lines 58/69 via `_plaintext_forbidden_cached`) both rely on that same narrow `_is_production_env()`.
  => Net effect: a deploy with `ENV=prod` + `ENGINE_MODE=live` but `TS_ENV` unset and `TS_SECRETS_PROVIDER=plaintext` passes the guard and reads raw secret files.
- engine/runtime/secret_sources.py:511-634 — `secret_source_policy_snapshot(...)` enumerates inline-env / repo-local / file-source violations and builds `blockers` (lines 610-621) but NEVER inspects `TS_SECRETS_PROVIDER` / the resolved provider. So even under strict policy, selecting the plaintext provider is not a blocker.
- The broad, correct trigger set already exists: engine/runtime/secret_sources.py:278-290 `strict_secret_source_policy_required(environ=None)` returns True for PROD_LOCK, ENGINE_SUPERVISED, {ENV,APP_ENV,TS_ENV,NODE_ENV} in {prod,production}, or {ENGINE_MODE,EXECUTION_MODE,OPERATOR_MODE}==live. A parallel narrower helper is services/credential_encryption.py:84-96 `_production_like_runtime()`.
- Provider resolution: services/secrets/loader.py:60-65 — default provider is `systemd-creds` (`_default_provider_name`), and `selected_provider_name()` returns `(TS_SECRETS_PROVIDER or default).strip().lower()`. Keep systemd-creds as the safe default.

DESIGN / REQUIRED CHANGE
Make the plaintext provider refuse to operate under ANY strict production trigger, and make the policy snapshot treat a strict-required + plaintext-resolved provider as a hard blocker. Read each file before editing (read-then-change).

1) Broaden the plaintext provider guard (services/secrets/providers/plaintext.py):
   - Replace the body of `_is_production_env()` (lines 23-24) so it delegates to the canonical broad trigger set instead of the lone TS_ENV check. Import lazily inside the function to avoid an import cycle and to keep this dev-only module importable in isolation:
       `from engine.runtime.secret_sources import strict_secret_source_policy_required`
       `return bool(strict_secret_source_policy_required())`
     If that import fails (ImportError), fall back to a self-contained inline check that mirrors strict_secret_source_policy_required (PROD_LOCK or ENGINE_SUPERVISED truthy; ENV/APP_ENV/TS_ENV/NODE_ENV in {prod,production}; ENGINE_MODE/EXECUTION_MODE/OPERATOR_MODE == live) so the guard never silently weakens.
   - Keep the existing TTL cache (`_plaintext_forbidden_cached`, lines 27-33), the `_ensure_not_production()` callers (lines 58, 69), the import-time raise (lines 41-42), and the existing RuntimeError message string `plaintext_secrets_provider_forbidden_in_production` unchanged. Do NOT change the raised error identifier (tests/other code may match it).
   - Do NOT widen the test-suppression surface: this guard must stay strict. (The dev workflow uses the unset/dev environment, which still returns False.)

2) Add a provider blocker in the policy snapshot (engine/runtime/secret_sources.py):
   - In `secret_source_policy_snapshot(...)` (lines 511-634), after `required = strict_secret_source_policy_required(env)` (line 519) and `suppressed = ...` (line 520), resolve the selected provider from the passed `env` mapping (do not read os.environ directly so the `environ=` override stays honored):
       `provider = (env.get("TS_SECRETS_PROVIDER") or "systemd-creds").strip().lower()`  (mirror loader._default_provider_name / selected_provider_name semantics).
   - When `required and not suppressed and provider == "plaintext"`, append a violation:
       `{"key": "TS_SECRETS_PROVIDER", "kind": "config", "reason": "plaintext_provider_forbidden", "provider": provider}`
   - Add `"plaintext_provider_forbidden"` to the blocker-reason set used to build `blockers` (lines 610-620) so it surfaces in `blockers`, sets `ok=False`, and becomes the `reason` when first. Preserve `dict.fromkeys` dedup (line 621) and the existing return shape.

3) Tests:
   - Extend tests/test_secrets_provider_plaintext.py: add cases proving the provider guard now fires for ENV=prod (TS_ENV unset), ENGINE_MODE=live, EXECUTION_MODE=live, PROD_LOCK truthy, ENGINE_SUPERVISED truthy — each must raise `RuntimeError("plaintext_secrets_provider_forbidden_in_production")` on `load`/`delete` (clear the module TTL cache or reimport between cases). Keep an existing case proving a clean dev env (all triggers unset) still permits load.
   - Extend tests/test_secret_source_policy.py: assert `secret_source_policy_snapshot(environ={"ENGINE_MODE": "live", "TS_SECRETS_PROVIDER": "plaintext"})` returns `ok=False` with a `plaintext_provider_forbidden:TS_SECRETS_PROVIDER` blocker; and that with `TS_SECRETS_PROVIDER` unset (defaulting to systemd-creds) under the same strict env there is NO `plaintext_provider_forbidden` blocker.

GUARDRAIL: preserve all existing fail-closed/safe-mode gates; never enable live execution; read-then-change. Do not weaken any existing blocker/violation, do not alter the systemd-creds default, do not print or hardcode secret values.

VERIFY
1) `cd /home/david/gitsandbox/system/system && python -c "import os; os.environ['ENGINE_MODE']='live'; from services.secrets.providers import plaintext; plaintext._production_check_at=0.0; plaintext._production_forbidden=None; \
import traceback;\
\nok=False\ntry:\n plaintext.load('x')\nexcept RuntimeError as e:\n ok=(str(e)=='plaintext_secrets_provider_forbidden_in_production')\nassert ok, 'guard did not fire for ENGINE_MODE=live'\nprint('GUARD_OK')"` prints GUARD_OK (guard fires with TS_ENV unset).
2) `python -c "from engine.runtime.secret_sources import secret_source_policy_snapshot as s; \
snap=s(environ={'ENGINE_MODE':'live','TS_SECRETS_PROVIDER':'plaintext'}); \
assert not snap['ok'] and any(b.startswith('plaintext_provider_forbidden') for b in snap['blockers']), snap['blockers']; \
snap2=s(environ={'ENGINE_MODE':'live'}); \
assert not any('plaintext_provider_forbidden' in b for b in snap2['blockers']); print('SNAPSHOT_OK')"` prints SNAPSHOT_OK.
3) `python -m pytest tests/test_secrets_provider_plaintext.py tests/test_secret_source_policy.py -q` passes.
4) `git -C /home/david/gitsandbox/system/system diff --stat` shows only services/secrets/providers/plaintext.py, engine/runtime/secret_sources.py, and the two test files changed.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

### HG-4 — Validator: enforce shared scientific pins match across dependency profiles (P2)

ROLE: You are a senior release-engineering agent hardening the dependency-manifest validator so accelerator profiles cannot silently diverge from the CI-validated scientific stack.

TARGET: /home/david/gitsandbox/system/system

PROBLEM
The repo ships a CPU/base scientific stack that CI installs and validates, plus GPU "profile" manifests that override the SAME packages to newer versions. Nothing asserts these stay compatible, so a sklearn pickle or LightGBM booster trained/validated against the base stack can fail to load (or silently mis-predict) on a ROCm host. Re-confirmed at HEAD 8184174:

- requirements-base.txt pins: lightgbm==4.5.0 (line 11), numpy==2.1.2 (line 13), pandas==3.0.3 (line 17), scikit-learn==1.5.2 (line 27), scipy==1.17.1 (line 28), xgboost-cpu==2.1.4 (line 36).
- requirements-amd-rocm.txt overrides: numpy==2.4.6 (line 19), scikit-learn==1.9.0 (line 20), lightgbm==4.6.0 (line 21), xgboost==2.1.4 (line 22) — a 2-minor sklearn jump and a numpy major jump versus base.
- requirements-amd-rocm-full.txt overrides the same: numpy==2.4.6 (line 32), scikit-learn==1.9.0 (line 33), lightgbm==4.6.0 (line 34), pandas==3.0.3 (line 50), scipy==1.17.1 (line 60), xgboost==2.1.4 (line 68); its header comment (lines 16-18) even documents the divergence as intentional image parity.
- tools/validate_dependency_lock.py reads requirements-amd-rocm.txt and requirements-amd-rocm-full.txt ONLY for dev-tool leakage in _dev_runtime_separation_report (runtime_paths list, lines 217-225). _profile_requirements_report (lines 273-289) only checks NVIDIA diagnostics presence and emits a soft warning when the AMD marker file is missing (line 288). NO function compares the shared scientific pins between base and any profile, so the divergence above passes `--strict` clean.

DESIGN / REQUIRED CHANGE
Add a new validator rule in tools/validate_dependency_lock.py that asserts a defined set of shared scientific packages is pinned IDENTICALLY across requirements-base.txt and every accelerator profile manifest, unless a divergence is explicitly allow-listed with a documented reason.

1. Module-level constants (near the existing NVIDIA_ONLY_REQUIREMENTS / DEV_TOOL_REQUIREMENTS block, ~lines 18-37). Use normalized (lowercased, hyphenated) names to match _normalize_req_name:
   - SHARED_SCIENTIFIC_PINS = {"numpy", "scipy", "pandas", "scikit-learn", "lightgbm"}. (Do NOT include xgboost: base pins the CPU variant `xgboost-cpu` while profiles pin `xgboost`, which are different distribution names — keep them out of the equality check to avoid false negatives; note this exclusion in a code comment.)
   - PROFILE_MANIFESTS = (ROOT / "requirements-amd-rocm.txt", ROOT / "requirements-amd-rocm-full.txt", ROOT / "requirements-nvidia-cuda.txt"). Skip any manifest that does not exist (mirror the existing `if not path.exists(): continue` pattern).
   - SHARED_PIN_ALLOWLIST: a dict keyed by (manifest_relpath, package_name) -> short reason string, seeded EMPTY ({}). This is the only sanctioned escape hatch; an allow-listed entry downgrades the error to a warning that echoes the reason.

2. New function _shared_scientific_pin_report() -> Tuple[List[str], List[str]] placed alongside _profile_requirements_report (after line 289). Implementation:
   - Build base_pins by parsing requirements-base.txt with the EXISTING _requirements_entries helper, restricting to names in SHARED_SCIENTIFIC_PINS, and extract the exact pinned version from each entry (the entry value is `relpath:lineno:line`; parse the `==X.Y.Z` token from the line using the existing PIN_RE / a simple split on `==`). If requirements-base.txt is missing, append error `shared_pin_base_missing` and return.
   - For each existing manifest in PROFILE_MANIFESTS, parse its entries the same way. For each package in SHARED_SCIENTIFIC_PINS present in BOTH base and the manifest, compare the exact version strings.
   - On mismatch: if (manifest_relpath, package) is in SHARED_PIN_ALLOWLIST, append a warning `cross_profile_pin_allowlisted:{manifest_relpath}:{package}:base={base_ver}:profile={profile_ver}:{reason}`; otherwise append an error `cross_profile_pin_mismatch:{manifest_relpath}:{package}:base={base_ver}:profile={profile_ver}`.
   - Only compare packages present in both files (a profile that omits a shared package is fine — it inherits base via the include chain). Do not flag version-specifier style differences other than the resolved `==` version.

3. Wire into main() exactly like the sibling reports: add `shared_pin_errors, shared_pin_warnings = _shared_scientific_pin_report()` next to the `_profile_requirements_report()` call (line 405), then `errors.extend(shared_pin_errors)` (near line 416) and `warnings.extend(shared_pin_warnings)` (near line 427). The rule must be active in BOTH default and `--strict` runs (it is not gated on `args.strict`).

4. Resolve the current real divergence (numpy/scikit-learn/lightgbm between base and both AMD profiles) so the validator passes. Do ONE of the following; prefer (a):
   (a) Align the AMD profile pins down to the base versions (numpy==2.1.2, scikit-learn==1.5.2, lightgbm==4.6.0->4.5.0) in requirements-amd-rocm.txt (lines 19-21) and requirements-amd-rocm-full.txt (lines 32-34, plus update the explanatory comment block at lines 16-18) IF those base versions are installable on the ROCm Python 3.12 image; OR
   (b) If the ROCm image genuinely cannot satisfy the base versions, add the minimal entries to SHARED_PIN_ALLOWLIST with a concise, accurate reason for each, and add a one-paragraph note to docs/ (e.g. docs/README_DEVELOPER_MAP.md dependency section or the nearest existing dependency doc) explaining that allow-listed scientific-pin divergences require model artifacts to be re-validated per profile. Do NOT invent installability claims — if unverified, choose (b) with reason "unverified base-version availability on ROCm image; pending confirmation".

5. If the per-profile lock work (GO-R6) is also landing in this branch, coordinate: the equality check should run against the resolved profile lock/manifest GO-R6 produces rather than a second-guessed copy. If GO-R6 has not landed, implement against the current profile manifests listed above and leave a `# coordinate with GO-R6 per-profile lock` comment at the function.

GUARDRAIL: preserve all existing fail-closed/safe-mode gates; never enable live execution; read-then-change. This is a build/CI manifest validator only — do not touch runtime, risk, execution, or broker code, and do not loosen any existing validator rule.

VERIFY
1. `python tools/validate_dependency_lock.py --json` (run from repo root) emits `"ok": true` with NO `cross_profile_pin_mismatch:*` entries in `errors`; any intentional divergence appears only as a `cross_profile_pin_allowlisted:*` warning whose reason matches SHARED_PIN_ALLOWLIST.
2. Negative test: temporarily edit requirements-amd-rocm.txt to set numpy to a version differing from requirements-base.txt, re-run the validator, and confirm it exits non-zero (return code 1) with `ERROR cross_profile_pin_mismatch:requirements-amd-rocm.txt:numpy:base=...:profile=...`; then revert the temporary edit.
3. `python -m pytest tests -k "dependency_lock or validate_dependency" -q` passes (add or extend a test asserting (a) identical pins across base+profiles produce no error and (b) a deliberate mismatch produces a `cross_profile_pin_mismatch` error and a non-zero exit; reuse the existing test module for validate_dependency_lock if present).
4. `python -m pyright tools/validate_dependency_lock.py` and `ruff check tools/validate_dependency_lock.py` report no new findings.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

### HG-5 — Add coverage floor + zero-coverage guard for engine/strategy money-path modules (P2)

ROLE: You are a senior engineer hardening the coverage gate so the engine/strategy money-path is defended by an explicit per-package floor and a zero-coverage guard.

TARGET: /home/david/gitsandbox/system/system

PROBLEM
The coverage gate in this repo defends `engine/risk`, `engine/execution`, and `engine/runtime` with both per-package floors and a zero-covered-module guard, but `engine/strategy` is defended by NEITHER. It is covered only transitively by the aggregate `engine` 52% floor, which lets any single strategy module (including a newly added one) sit at 0% coverage without failing the gate.

Confirmed evidence at HEAD 8184174 (branch codex/worktree-production-readiness):
- `pyproject.toml:85-104` — `[tool.trading_system.coverage_gate]`: `minimum_percent = 52.0`, `package_roots = ["engine", "services", "routes", "ops"]`, and `zero_covered_module_roots = ["engine/risk", "engine/execution", "engine/runtime"]` (line 89). `engine/strategy` is absent. The zero-covered allowlist (lines 90-103) only lists execution/runtime modules.
- `pyproject.toml:106-109` — `[tool.trading_system.coverage_gate.package_minimums]` defines floors ONLY for `engine/risk = 50.81`, `engine/execution = 58.92`, `engine/runtime = 58.49`. No `engine/strategy` floor.
- `tools/coverage_gate.py:81-104` — config parse: `package_minimums` (roots normalized via `_normalize_root`) and `zero_covered_module_roots` are read straight from the TOML keys above; adding rows to those tables is sufficient to wire new enforcement (no code change required for parsing).
- `tools/coverage_gate.py:424-443` — `print_package_summary` renders the "Required package floors" table; every key in `package_minimums` produces one PASS/FAIL row.
- `tools/coverage_gate.py:502-528` — the gate enforcement: each `package_minimums` root must meet its floor (502-512), and any non-allowlisted zero-covered module under a `zero_covered_module_roots` root fails the gate (514-528).
- CLAUDE.md "Key files" names the money-path strategy modules: `engine/strategy/{predictor,champion_manager,model_intent,portfolio}.py`, and the roadmap section names `engine/strategy/{statistical_gates,promotion_guard}.py`.

DESIGN / REQUIRED CHANGE
Defend `engine/strategy` exactly the way `engine/risk` etc. are defended — config-only changes in `pyproject.toml`, no logic change in `tools/coverage_gate.py` (its table renderer and enforcement already iterate every configured root).

1. Measure the current honest coverage of `engine/strategy` so the floor is DEFENDED (set at/just below measured, never above), matching how the existing floors were derived:
   - Run: `python tools/coverage_gate.py run` (regenerates the stamped `artifacts/coverage/coverage.json` and prints the per-package summary). If a full run is infeasible in the sandbox, run `python tools/coverage_gate.py check` against the existing stamped report and read the `engine/strategy` row from `print_package_summary`.
   - Record the measured `engine/strategy` total_percent (the branch+line combined percent the gate uses).

2. In `pyproject.toml` under `[tool.trading_system.coverage_gate.package_minimums]` (currently lines 106-109), add a row:
   `"engine/strategy" = <FLOOR>` where `<FLOOR>` = `floor(measured_total_percent * 100) / 100` minus a small ratchet margin of at most 0.50 (e.g. measured 41.37 -> floor 40.87..41.37). The floor MUST be <= the just-measured value so the gate PASSES at this HEAD; never set it above measured.

3. In `pyproject.toml`, append `"engine/strategy"` to `zero_covered_module_roots` (currently line 89) so any zero-covered module under `engine/strategy/` fails the gate.

4. Seed the zero-covered allowlist honestly: run the gate and read its "Zero-covered module burndown" output (`tools/coverage_gate.py:444-459`). For every currently-zero `engine/strategy/*.py` module reported as new, add it to `[tool.trading_system.coverage_gate.zero_covered_module_allowlist]` (lines 90-103) so this change does not retroactively fail on pre-existing untested modules — the guard's purpose is to block NEWLY-added zero-covered strategy modules, not to force a backfill in this task. Do NOT allowlist the named money-path files `engine/strategy/{predictor,champion_manager,model_intent,portfolio,statistical_gates,promotion_guard}.py`; if any of those is at 0% coverage, instead add a minimal smoke/import test under `tests/` that imports and exercises it enough to register non-zero coverage, so it is genuinely covered rather than allowlisted.
   - Keep the allowlist sorted/grouped consistently with the existing entries.

5. Do not change `minimum_percent`, `package_roots`, the renderer, or enforcement code. This is a config-and-tests change that leans on the existing generic loops.

GUARDRAIL
Preserve all existing fail-closed/safe-mode gates; never enable live execution; read-then-change.

VERIFY
- `python tools/coverage_gate.py check` (or `run`) prints a "Required package floors" table that includes an `engine/strategy` row with a numeric Floor and result `PASS` (Measured >= Floor) at this HEAD.
- The "Zero-covered module burndown" line reports `new=0` for `engine/strategy` (no non-allowlisted zero-covered strategy module), and the gate exits 0.
- Sanity-check the guard bites: temporarily add a throwaway empty module `engine/strategy/_zerocov_probe.py` (a single `pass`/unused function, not imported by any test), re-run `python tools/coverage_gate.py check`, and confirm the gate now FAILS with `new zero-covered modules under critical roots: engine/strategy/_zerocov_probe.py` and a non-zero exit code; then delete the probe and confirm the gate returns to PASS.
- `git diff` shows changes ONLY in `pyproject.toml` (and any new minimal test added in step 4); `tools/coverage_gate.py` is unchanged.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

### HG-6 — Property/invariant tests for risk invariants (tail-loss ordering + gross/net cap monotonicity) wired into the safety_critical CI gate (P2)

ROLE: You are a senior quant-infra engineer adding property-based invariant tests for the risk engines and wiring them into the money-path CI gate.

TARGET: /home/david/gitsandbox/system/system

PROBLEM (re-confirmed at HEAD 8184174, branch codex/worktree-production-readiness):
The risk-engine test suite has solid example-based and idempotency/hash-chain coverage, but ZERO property/invariant coverage of the two most safety-relevant numerical invariants: tail-loss ordering in the Monte Carlo engine and gross/net cap monotonicity in the portfolio risk engine. Evidence:
- `rg "from hypothesis|@given|import hypothesis" tests/` returns nothing; `hypothesis` does not appear in `requirements-dev.in`, `requirements-dev.txt`, or `requirements-dev.lock.txt`. The dependency is absent.
- `tests/test_monte_carlo_risk_engine_contract.py` exercises `_simulate`/`_distribution_buckets`/`_fan_rows` with fixed `_ZeroRandom` inputs only; it never asserts any ordering between VaR and CVaR.
- `engine/risk/monte_carlo_risk_engine.py`:
  - `_pct(xs, q)` (line ~41) returns the lower-tail quantile by sorting ascending and indexing `round((n-1)*q)`.
  - `_cvar(xs, q)` (line ~49) computes `cutoff = _pct(xs, q)` then averages the tail `[x for x in xs if x <= cutoff]`. Therefore for the PnL convention used at lines ~396-399 (`var_95=_pct(base_pnl, 0.05)`, `cvar_95=_cvar(base_pnl, 0.05)`), CVaR is the mean of the worst (lowest) 5% and is the EXPECTED-SHORTFALL-style quantity, so the correct invariant is `cvar_95 <= var_95` (more-negative tail mean), NOT `cvar >= var`. For the drawdown convention (lines ~402-403, `_cvar(base_dd, 0.95)`) the cutoff is the UPPER tail and `drawdown_cvar_95 >= drawdown_p95`. The test must encode each direction correctly — confirm the sign by reading `_pct`/`_cvar` before asserting.
- `engine/risk/portfolio_risk_engine.py`:
  - Portfolio caps: `MAX_GROSS` (line ~118, default 1.00), `MAX_NET` (line ~119, default 0.60).
  - `_apply_portfolio_caps(desired, info)` (line ~1740) scales abs weights by `MAX_GROSS/g` when `g > MAX_GROSS` (lines ~1757-1770), then scales signed weights by `MAX_NET/|n|` when `|n| > MAX_NET` (lines ~1776-1789), recording `caps_post_gross`/`caps_post_net` (lines ~1791-1792). `_gross`/`_net` are at lines ~709-714; `_signed_weight` at line ~684.
There is no test asserting that for ANY random weight vector the post-cap output satisfies `gross <= MAX_GROSS + eps` and `|net| <= MAX_NET + eps`, that the clamp is monotone (a larger input gross never yields a larger post-cap gross than the cap), and that an already-within-cap vector is left unchanged.

DESIGN / REQUIRED CHANGE (implement exactly; do not re-discover):
1. Add the dev dependency. Add `hypothesis==6.x` (pin to the latest patch available in the existing index; choose a single concrete pinned version, e.g. `hypothesis==6.135.26`) to `requirements-dev.in` under the existing test tools (next to `pytest`), then regenerate the lock the same way the repo already does (the header of `requirements-dev.in` says installs go through `requirements-dev.txt` which applies `requirements-dev.lock.txt`). Regenerate with the project's existing pip-compile invocation (inspect the Makefile / `tools/` for the lock-regen command; do NOT hand-edit the lock hashes). If no regen tool is wired, add `hypothesis` (pinned, with hashes) to `requirements-dev.lock.txt` using the same `pip-compile --generate-hashes` mechanism that produced the file. The CI safety-critical job installs via `python -m pip install -r requirements-dev.txt` (`.github/workflows/validate.yml` ~line 248), so hypothesis MUST be resolvable there.

2. New test file `tests/test_risk_invariants_property.py` with `import pytest`, `from hypothesis import given, settings, strategies as st`, and a module-level `pytestmark = pytest.mark.safety_critical`. Keep all property tests deterministic and socket-free (the `tests/conftest.py` socket guard must not be tripped — these are pure-function tests, no DB/network). Set a bounded `@settings(max_examples=200, deadline=None)` so CI runtime stays predictable. Include:
   a. Monte Carlo tail-loss ordering — import `engine.risk.monte_carlo_risk_engine as mc`. Generate random finite float lists `xs` (`st.lists(st.floats(min_value=-1e6, max_value=1e6, allow_nan=False, allow_infinity=False), min_size=1, max_size=200)`). Assert for each confidence `q in (0.05, 0.01)`: `mc._cvar(xs, q) <= mc._pct(xs, q) + 1e-9` (lower-tail expected-shortfall is no greater than VaR), AND `min(xs) - 1e-9 <= mc._cvar(xs, q) <= mc._pct(xs, q) + 1e-9`. Add a separate drawdown-direction property: for `q in (0.95, 0.99)`, `mc._cvar(xs, q) >= mc._pct(xs, q) - 1e-9` (upper-tail). Empty-list edge: assert both return `0.0`.
   b. Gross/net cap monotonicity & clamp soundness — import `engine.risk.portfolio_risk_engine as pre`. Build a random `desired` map `{f"S{i}": {"weight": w}}` from `st.lists(st.floats(min_value=-5.0, max_value=5.0, allow_nan=False, allow_infinity=False), min_size=1, max_size=12)`. Call `out = pre._apply_portfolio_caps(dict(desired), info={})` and assert: `pre._gross(out) <= float(pre.MAX_GROSS) + 1e-6` (when `MAX_GROSS > 0`); `abs(pre._net(out)) <= float(pre.MAX_NET) + 1e-6` (when `MAX_NET > 0`); every output symbol's sign matches its input sign (sign preservation under scaling, lines ~1762-1763/1782); and idempotence/no-op-within-cap: if the input already satisfies both caps, output weights equal input weights within 1e-12. Add a monotonicity property: for two input vectors that are scalar multiples (`v` and `k*v`, `k>=1`), `pre._gross(clamp(k*v)) <= pre._gross(clamp(v)) + 1e-6` once the cap binds (post-cap gross never increases when you feed in a larger pre-cap gross). Do NOT monkeypatch `MAX_GROSS`/`MAX_NET` to enable anything — read them as-is.
   c. Sizing/cap monotonicity guard — if a pure per-symbol vol-cap helper is reachable without DB (inspect around lines ~1840-1860 `cap/gross` scaling), add a property that scaling a within-cap weight up past the cap yields a clamped weight equal to the cap (within eps) and never exceeds it; otherwise document in a code comment that the per-symbol cap path requires a `con` and is covered by existing example tests, and skip it (do not fabricate a DB).

3. Wire into the safety-critical CI gate (`.github/workflows/validate.yml`, the "Run safety-critical money-path suites without skips" step, ~lines 252-272): add `--expected-source tests/test_risk_invariants_property.py` to the `--expected-source` list AND append `tests/test_risk_invariants_property.py` to the explicit file list passed after `-m "safety_critical" -rs` (the trailing list at ~lines 264-272). Raise `--min-selected` from `130` to the new count: first run the gate locally to get the exact post-addition selected count, then set `--min-selected` to that exact number (it must increase by the number of new property tests collected). Do not lower it.

GUARDRAIL: preserve all existing fail-closed/safe-mode gates; never enable live execution; read-then-change. These are pure-function property tests — no env flags that enable trading may be set, no `MAX_GROSS`/`MAX_NET` may be widened, no socket may be opened, and the existing `safety_critical` selection/min-selected gate must get STRICTER (higher min-selected), never weaker.

VERIFY (exact checks that prove done):
- `python -c "import hypothesis; print(hypothesis.__version__)"` succeeds, and `grep -i hypothesis requirements-dev.in requirements-dev.lock.txt` shows the pinned entry in both.
- Baseline first: `python -m pytest tests/ -q -m safety_critical 2>&1 | tail -20` BEFORE edits to capture pre-existing reds; do not mask or mis-attribute them.
- `python -m pytest tests/test_risk_invariants_property.py -q` passes (all property tests green, no skips except any documented DB-gated case).
- Reproduce the CI gate locally and confirm it passes with the raised minimum and the new expected-source:
  `python tools/run_required_backend_tests.py --label safety-critical-money-path --junitxml var/artifacts/sc.xml --min-selected <NEW_COUNT> --expected-source tests/test_risk_invariants_property.py -- -q -m "safety_critical" -rs tests/test_risk_invariants_property.py` returns exit 0 and prints `missing_sources=0`, and the full gate command from validate.yml (with the new file appended to both lists and the new `--min-selected`) also returns 0.
- Confirm directionality is correct by reading `_pct`/`_cvar`: the lower-tail (q=0.05/0.01) property asserts `cvar <= var`, the upper-tail (q=0.95/0.99) property asserts `cvar >= var`. A test that asserts the wrong direction must fail — sanity-check by temporarily flipping one assertion and observing a hypothesis counterexample, then revert.
- `git diff --stat` shows only: `requirements-dev.in`, `requirements-dev.lock.txt`, `tests/test_risk_invariants_property.py`, `.github/workflows/validate.yml`. No production engine file is modified.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

### HG-7 — Add a scheduled nightly soak/chaos CI lane that runs the soak harnesses and fails on injected faults (P2)

ROLE: You are a release-engineering / CI hardening engineer adding a scheduled soak+chaos lane to GitHub Actions.

TARGET: /home/david/gitsandbox/system/system

You are working in `/home/david/gitsandbox/system/system` (branch codex/worktree-production-readiness, HEAD 8184174). The repo is dirty; do not revert unrelated user changes. Read-then-change: re-open every cited file and confirm line numbers/behavior at the current HEAD before editing.

PROBLEM
- `.github/workflows/` contains exactly one workflow, `validate.yml` (confirmed: `ls .github/workflows/` returns only `validate.yml`).
- That workflow runs only on push/PR: `validate.yml:3-5` is `on:\n  push:\n  pull_request:`. `rg -n 'schedule:|cron:' .github/` returns nothing — there is NO time-triggered lane anywhere in CI.
- Two soak/chaos harnesses exist but are invoked by no workflow:
  - `tools/safe_mode_soak.py` — polls dashboard (`/api/health`, `/api/system/state`, `/api/jobs`) and operator (`/api/operator/status`) endpoints on an interval, appends NDJSON samples (`main()` at `safe_mode_soak.py:128`, writes one JSON record per sample at `:155-174`, scans `var/log/runtime.log` for `FAIL_PATTERNS` at `:20-28`), and currently `return 0` unconditionally at `:186` (it logs evidence but never asserts a pass/fail). CLI: `--duration-s` (default 14400), `--interval-s` (default 30), `--out`, `--base-url`, `--operator-url`.
  - `tools/market_session_soak.py` — full GO/NO-GO chaos harness. `main()` at `:967`; exits `EXIT_OK=0` / `EXIT_NO_GO=2` (`:45-46`, `:936`, `:1008`); accumulates `runner.failures` via `fail()`/`step()` (`:608-615`); injects a provider-disconnect chaos fault via `exercise_provider_disconnect()` (`:775`, called at `:994`); requires `SOAK_REPORT_SIGNING_KEY` or it appends a `soak_report_signing_key_missing` failure and returns NO-GO (`:931-936`); flags `--skip-provider-restart`, `--skip-rollback`, `--allow-after-hours`, `--no-wait-for-open`, `--allow-partial-session` (`:959-963`); env knobs `SOAK_DURATION_S`, `SOAK_INTERVAL_S`, `SOAK_SYMBOL`, `SOAK_PROVIDER_JOB` (`:948-958`).
- Net effect: nothing exercises the running system over a sustained interval, and no fault injection ever blocks. The Postgres+Redis service-stack pattern already exists in `validate.yml` (`production-backend` job, `services:` at `:286-309`, env block `:310-338` with `ENGINE_MODE/EXECUTION_MODE/OPERATOR_MODE=safe`, `PROD_LOCK=0`, `KILL_SWITCH_GLOBAL=0`, `AUTO_BOOT_DAEMONS=0`) and is the template to reuse.

DESIGN / REQUIRED CHANGE
Add a NEW, separate scheduled workflow — do NOT touch the merge-gate `validate` job's triggers, and do NOT make this lane block merges.

1. New file `.github/workflows/soak_chaos.yml`:
   - Triggers: `on: schedule: - cron: "0 7 * * *"` (nightly, UTC) AND `workflow_dispatch:` (manual run), with optional `inputs.duration_s` (default the bounded CI value below). NO `push:`/`pull_request:` triggers.
   - `concurrency:` group keyed on the workflow + ref with `cancel-in-progress: true` so overlapping nightly runs do not pile up.
   - One job `soak-chaos` (`runs-on: ubuntu-latest`, `timeout-minutes: 30`). Reuse the EXACT `services:` (timescaledb pg16 + redis:7 with the same health-check options/ports) and the safe-mode env block from `validate.yml:286-338` verbatim (same `TS_*`, `LIVE_CACHE_*`, `APP_ENV=test`, `ENGINE_MODE/EXECUTION_MODE/OPERATOR_MODE=safe`, `PROD_LOCK=0`, `KILL_SWITCH_GLOBAL=0`, `AUTO_BOOT_DAEMONS=0`, `AUTO_PIPELINE=0`). Add `PYTHONPATH: .`.
   - Steps: checkout@v4; setup-python@v5 (3.11); install `requirements-dev.txt`; reuse the "Verify provisioned Postgres and Redis" probe step from `validate.yml:353-365`.
   - Boot the system in SAFE mode in the background so the soak harnesses have HTTP endpoints to poll. Start the dashboard server (`python dashboard_server.py` or the repo's documented safe-mode boot — confirm the correct entrypoint/flag against `start_system.py` and `dashboard_server.py` before wiring) bound to `127.0.0.1:8000`, redirecting stdout/stderr to `var/log/runtime.log`, and POLL `/api/health` until ready (bounded wait, ~120s) before proceeding; fail the job if it never comes up.
   - Generate an ephemeral `SOAK_REPORT_SIGNING_KEY` for the job (e.g. `echo "SOAK_REPORT_SIGNING_KEY=$(openssl rand -hex 32)" >> "$GITHUB_ENV"`) so `market_session_soak.py` does not NO-GO on the missing-signing-key check. Never print the key value.
   - Soak step 1 (bounded): run `python tools/safe_mode_soak.py --duration-s 600 --interval-s 30 --out "$RUNNER_TEMP/safe_mode_soak.ndjson"` (env override `SAFE_MODE_SOAK_DURATION_S` style not present; pass the flag). 600s keeps the nightly lane bounded.
   - Soak step 2 (chaos): run `python tools/market_session_soak.py --duration-s 600 --interval-s 30 --allow-after-hours --no-wait-for-open --allow-partial-session --out-dir "$RUNNER_TEMP/soaks"`; rely on its native non-zero (`EXIT_NO_GO=2`) to fail the job, which it already returns on any accumulated failure (incl. the injected provider-disconnect path) — do NOT add `|| true`.

2. `safe_mode_soak.py` does NOT currently assert — make it gate. Add an assertion pass over the NDJSON it just wrote (or a sibling validator) so the lane fails on a real soak regression, not just on liveness:
   - Add CLI flags `--max-error-rate` (default 0.0 for the chaos-free safe-mode poll, i.e. any `ok=false` sample or any `log_matches` hit is a failure) and `--max-rss-growth-mb` (default e.g. 200) to `safe_mode_soak.py`'s parser (`:128-136`).
   - Capture per-sample process RSS of the booted server into each NDJSON record (extend the `record` dict at `:155-172`), and at the end of `main()` (currently `return 0` at `:186`) compute: (a) fraction of samples with any `ok[*]=false`, (b) total count of `log_matches` (FAIL_PATTERNS hits), (c) RSS growth = last_sample_rss - first_sample_rss. Return non-zero (e.g. `2`) and print a structured `{"status":"NO-GO","reasons":[...]}` line if error-rate exceeds `--max-error-rate`, any FAIL_PATTERN matched, or RSS growth exceeds `--max-rss-growth-mb`. Keep `return 0` only when all thresholds pass. Preserve the existing NDJSON record shape (only add fields; do not rename/remove keys other tooling reads).
   - If reading the booted server's RSS is impractical from the poller, instead scan the same liveness/error evidence the harness already collects and gate purely on error-rate + FAIL_PATTERN count; document which path you chose in the code comment and the workflow.

3. Inject and prove a fault. Add a workflow step (or reuse `market_session_soak.py`'s `exercise_provider_disconnect`) that demonstrates the lane exits non-zero under an injected fault, and a unit test `tests/test_safe_mode_soak_gate.py` that feeds a crafted NDJSON containing an `ok=false` / FAIL_PATTERN sample (and an RSS-growth sample) and asserts the new gate function returns the NO-GO exit code, plus a clean-evidence case that returns 0.

4. Upload `var/log/runtime.log`, the safe-mode NDJSON, and the `market_session_soak` report dir as `actions/upload-artifact@v4` artifacts (`if: always()`) for post-mortem, matching the artifact pattern used elsewhere in `validate.yml`.

5. Register the new bundle: add a one-line row for this lane to `docs/handoff/deep_dive_prompts/README.md` only if you also add a prose doc; otherwise add a short note in the workflow header comment describing cron cadence and that it is non-blocking for merges.

GUARDRAIL: preserve all existing fail-closed/safe-mode gates; never enable live execution; read-then-change. The new lane MUST boot with `ENGINE_MODE=EXECUTION_MODE=OPERATOR_MODE=safe`, `PROD_LOCK=0`, `KILL_SWITCH_GLOBAL=0`, and must NOT set any broker/live credentials or live-arming env. Do not weaken or remove `market_session_soak.py`'s `soak_report_signing_key_missing` or `unintended_live_order_evidence` failures; the signing key is generated ephemerally per-run and never printed. Do not alter the `validate.yml` merge-gate triggers.

VERIFY
- `rg -n 'schedule:|cron:' .github/workflows/soak_chaos.yml` shows the nightly cron; `rg -n 'on:|push:|pull_request:' .github/workflows/soak_chaos.yml` confirms NO push/PR trigger and that `validate.yml:3-5` is unchanged.
- `python tools/safe_mode_soak.py --help` lists the new `--max-error-rate` and `--max-rss-growth-mb` flags.
- `python -m pytest tests/test_safe_mode_soak_gate.py -q` passes: the crafted bad-evidence NDJSON yields the NO-GO exit code and the clean NDJSON yields 0.
- Demonstrate the gate locally: feed a hand-built NDJSON with one `ok=false` sample to the new gate path and confirm exit code is non-zero (prove fail-closed); feed clean evidence and confirm exit 0.
- Lint the workflow (`actionlint .github/workflows/soak_chaos.yml` if available, else `python -c "import yaml; yaml.safe_load(open('.github/workflows/soak_chaos.yml'))"`) parses clean.
- Confirm the chaos harness still returns non-zero when its provider-disconnect/rollback assertions fail (do not regress `market_session_soak.py` exit semantics): `rg -n 'EXIT_NO_GO|exit_code' tools/market_session_soak.py` unchanged.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

### HG-8 — Add crypto live-disable safety block at broker_router boundary (defense-in-depth) (P2)

ROLE: Senior execution-safety engineer hardening the broker routing boundary with defense-in-depth gating.

TARGET: /home/david/gitsandbox/system/system

PROBLEM
Crypto live-enablement is currently enforced at only ONE layer: the upstream risk governor. Confirmed at HEAD 8184174:
- engine/strategy/portfolio_risk_gate.py:377 `_apply_crypto_live_order_gate(...)`; the live-disabled block fires at lines 405-414 returning status `blocked_crypto_live_trading_disabled` / reason `crypto_live_trading_disabled_by_default`, gated on `_env_bool("CRYPTO_LIVE_TRADING_ENABLED", False)` (read at :387). This gate is reached via `apply_execution_risk_governor` (broker_apply_orders.py).
- engine/execution/broker_router.py has NO crypto live-disable gate at the routing boundary. It only does capability routing: `_crypto_capable_broker` (broker_router.py:956) and `_prefer_crypto_capable_broker` (broker_router.py:965, invoked at :1893). `_batch_has_crypto` (broker_router.py:947) and `_is_crypto_order_symbol` (:937) already exist.

By contrast, futures and options ARE double-gated at the router boundary itself, independent of the upstream governor:
- Futures: `_futures_order_safety_block` (broker_router.py:1051) returns `futures_live_disabled` when the chain contains a LIVE broker and `FUTURES_LIVE_TRADING_ENABLED` is false (broker_router.py:1062-1071). It is invoked per-broker at broker_router.py:1619-1627 (chain=[name]) AND batch-wide at broker_router.py:1879-1887 (chain=chain).
- Options: `live_options_order_block(...)` invoked at broker_router.py:1609 and :1869.
- FX: `_fx_order_safety_block` (broker_router.py:897) invoked at broker_router.py:1889.

Consequence: if the upstream `_apply_crypto_live_order_gate` is ever bypassed, misconfigured, regressed, or the router is called from a code path that does not run the risk governor, crypto orders could reach a LIVE broker without any router-boundary live-enable check — unlike futures/options/fx which fail closed at the boundary. This is a defense-in-depth gap, not a known live-exposure today.

DESIGN / REQUIRED CHANGE
Add a router-boundary crypto live-disable safety block that mirrors `_futures_order_safety_block`'s LIVE-broker check, fail-closed, and wire it into both call sites alongside the futures/fx blocks. No new env var — reuse the existing `CRYPTO_LIVE_TRADING_ENABLED` (default false) so the boundary and governor agree.

1) New function in engine/execution/broker_router.py (place immediately after `_fx_order_safety_block` / `_batch_has_crypto` helpers, before `_prefer_crypto_capable_broker`):

```python
def _crypto_order_safety_block(
    orders: Optional[List[dict]],
    *,
    dry_run: bool,
    chain: Optional[List[str]],
) -> Optional[Dict[str, Any]]:
    if bool(dry_run) or not _batch_has_crypto(orders):
        return None
    normalized_chain = [canonical_broker_name(name) for name in list(chain or [])]
    if any(name in LIVE_BROKERS for name in normalized_chain) and not _env_bool("CRYPTO_LIVE_TRADING_ENABLED", False):
        return {
            "ok": False,
            "status": "crypto_live_disabled",
            "reason": "crypto_live_trading_disabled_by_default",
            "broker": "failover_chain",
            "stop_failover": True,
            "retryable": False,
            "env": {"CRYPTO_LIVE_TRADING_ENABLED": str(os.environ.get("CRYPTO_LIVE_TRADING_ENABLED", "0") or "0")},
        }
    return None
```
Notes: `LIVE_BROKERS`, `canonical_broker_name`, and `_env_bool` are already imported/defined in broker_router.py (imports at :40/:42; `_env_bool` at :262). Do NOT print the env value as a secret — it is a boolean flag, mirror the futures block exactly. The block must be dry_run-safe (return None on dry_run) so paper/sim/backtest paths are never affected, matching `_futures_order_safety_block` semantics.

2) Wire into the PER-BROKER site (broker_router.py ~:1619-1627). Immediately AFTER the existing `futures_block` handling (after the `return futures_block` at :1627, still inside the per-broker block that begins at the `is_live`/`_apply_one` path), add:
```python
        crypto_block = _crypto_order_safety_block(
            override_orders,
            dry_run=bool(dry_run),
            chain=[name],
        )
        if crypto_block is not None:
            crypto_block["broker"] = name
            return crypto_block
```

3) Wire into the BATCH-WIDE site (broker_router.py ~:1879-1891). Immediately AFTER the existing `futures_block` handling (after `return futures_block` at :1887) and either just before or just after the `fx_block` invocation at :1889, add:
```python
    crypto_block = _crypto_order_safety_block(
        override_orders,
        dry_run=bool(dry_run),
        chain=chain,
    )
    if crypto_block is not None:
        crypto_block["failover_attempts"] = []
        return crypto_block
```
Use the same variable name `crypto_block` is fine since the two sites are in different scopes; confirm the batch-wide site uses the batch `chain` (the chain in scope at :1879) BEFORE `_prefer_crypto_capable_broker` rewrites it at :1893, so the block evaluates the originally-resolved chain.

Read-then-change: re-open broker_router.py around :1590-1640 and :1855-1900 to confirm the exact insertion points and that `override_orders`, `dry_run`, `name`, and `chain` are the in-scope variable names at each site before editing.

GUARDRAIL: preserve all existing fail-closed/safe-mode gates; never enable live execution; read-then-change. This change only ADDS a fail-closed block — it must never widen permissions, must default to blocking (env default false), and must not alter dry_run/paper/sim/backtest behavior.

VERIFY
1) New test file tests/test_broker_router_crypto_live_gate.py (or extend tests/test_broker_router_dry_run_gates.py) asserting:
   a) With a crypto order, chain containing a LIVE broker, dry_run=False, and `CRYPTO_LIVE_TRADING_ENABLED` unset/false → router returns `status == "crypto_live_disabled"`, `ok is False`, `stop_failover is True`, and `_apply_one`/live broker submit is NOT invoked (assert via monkeypatch/spy that no live order call occurs).
   b) Same inputs but `CRYPTO_LIVE_TRADING_ENABLED=1` → no crypto block (routing proceeds; assert status != "crypto_live_disabled").
   c) dry_run=True with crypto + LIVE broker and flag false → `_crypto_order_safety_block` returns None (dry runs never blocked).
   d) Non-crypto (e.g. equity) batch with LIVE broker and flag false → returns None (no crypto block).
2) Run: `cd /home/david/gitsandbox/system/system && python -m pytest tests/test_broker_router_crypto_live_gate.py tests/test_broker_router_dry_run_gates.py tests/test_crypto_broker_routing.py -q` → all pass.
3) Grep confirmation: `grep -n "_crypto_order_safety_block" engine/execution/broker_router.py` shows the definition plus exactly two invocation sites (per-broker ~:1619 region and batch-wide ~:1879 region). `grep -n "crypto_live_disabled" engine/execution/broker_router.py` shows the new status string.
4) Sanity: `python -c "import engine.execution.broker_router"` imports cleanly with no new top-level imports added (LIVE_BROKERS / canonical_broker_name / _env_bool already present).

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

### HG-9 — Per-asset-class live-enablement snapshot in live_trading_preflight (P2)

ROLE: Senior trading-runtime engineer. TARGET: /home/david/gitsandbox/system/system. Read-then-change; preserve every existing fail-closed/safe-mode gate; never enable live execution.

PROBLEM. The boot-time live-readiness surface assembled by `live_trading_preflight()` in `engine/runtime/live_trading_preflight.py` reports per-asset-class live posture for OPTIONS only, and nothing for FX / CRYPTO / FUTURES. Confirmed at HEAD 8184174:
- `engine/runtime/live_trading_preflight.py:15` imports `live_options_readiness_snapshot`; it is invoked at ~1149-1155 and surfaced as `"options_instruments"` in the returned dict at ~1189. The LOB shadow snapshot is surfaced as `"lob_deeplob_shadow"` at ~1188 (~1145-1147).
- The returned dict (~1160-1190) has NO key reporting the live-enable gate state for FX, crypto, or futures. A grep of this file for `FX_LIVE_TRADING_ENABLED` / `CRYPTO_LIVE_TRADING_ENABLED` / `FUTURES_LIVE_TRADING_ENABLED` returns nothing.
- The actual per-asset-class live gates live elsewhere and are NOT mirrored into the readiness surface: futures `FUTURES_LIVE_TRADING_ENABLED` (default False) at `engine/execution/broker_router.py:1062` and `engine/execution/broker_ibkr_gateway.py:1120,1234`; crypto `CRYPTO_LIVE_TRADING_ENABLED` (default False) at `engine/strategy/portfolio_risk_gate.py:387,405-414`; options mode `OPTIONS_INSTRUMENTS_MODE` (default "shadow") at `engine/execution/options_readiness.py:152-154` (`options_instruments_mode()`); FX has its disable-by-default gate `FX_LIVE_TRADING_ENABLED` being added under GO-R7 (see `docs/handoff/deep_dive_prompts/PRODUCTION_GO_LIVE_REMEDIATION_PROMPTS.md` GO-R7), which mandates "Add FX to the multi-asset preflight snapshot".
Net effect: an operator inspecting the preflight/readiness output cannot see the full multi-asset live posture (which asset classes are live-permitted vs shadow/disabled) from one surface; FX/crypto/futures enablement is invisible until an order is actually routed and blocked deep in the broker/risk path.

DESIGN / REQUIRED CHANGE. Add a single per-asset-class live-enablement snapshot helper to `engine/runtime/live_trading_preflight.py` and surface it from `live_trading_preflight()`. This is an OBSERVABILITY/REPORTING surface only — it must NOT add any new path that could permit live execution, and it must NOT relax existing blockers.

1. New helper in `engine/runtime/live_trading_preflight.py`:
   `def asset_class_live_enablement_snapshot(*, engine_mode: Optional[str] = None) -> Dict[str, Any]:`
   - `mode = _normalize_mode(engine_mode if engine_mode is not None else os.environ.get("ENGINE_MODE"), "safe")`.
   - Build one entry per asset class with the SAME shape, reading each class's gate flag with the existing `_env_bool` / env helpers in this module (do not import heavy modules at call time unless guarded by try/except returning a safe "unknown" entry):
     - `fx`: flag `FX_LIVE_TRADING_ENABLED`, `live_permitted = _env_bool("FX_LIVE_TRADING_ENABLED", False)` (default OFF — must match the GO-R7 gate default; coordinate with GO-R7).
     - `crypto`: flag `CRYPTO_LIVE_TRADING_ENABLED`, `live_permitted = _env_bool("CRYPTO_LIVE_TRADING_ENABLED", False)` (default OFF).
     - `futures`: flag `FUTURES_LIVE_TRADING_ENABLED`, `live_permitted = _env_bool("FUTURES_LIVE_TRADING_ENABLED", False)` (default OFF).
     - `options`: gate is a MODE not a bool — read via `from engine.execution.options_readiness import options_instruments_mode, live_options_requested` inside a try/except; `mode_value = options_instruments_mode()` (default "shadow"), `live_permitted = (mode_value == "live") or live_options_requested()`. On import failure record `live_permitted=False`, `flag_value="unavailable"`.
   - Each entry shape: `{ "asset_class": <name>, "flag": <ENV_NAME or "OPTIONS_INSTRUMENTS_MODE">, "flag_value": <raw env string or mode>, "live_permitted": <bool>, "default_posture": "shadow"|"disabled" }`. Defaults must reflect shadow/disabled (FX/crypto/futures → "disabled" by default; options → "shadow" by default). Do NOT print or include any secret values.
   - Return `{ "mode": mode, "any_live_permitted": <bool: any entry live_permitted>, "classes": { "fx": {...}, "crypto": {...}, "futures": {...}, "options": {...} } }`.
   - This snapshot is REPORTING ONLY: it must NOT append to `blockers` and must NOT itself permit anything. The authoritative blocks remain in broker_router / portfolio_risk_gate / options_readiness. (Existing options blocker logic at ~1154-1155 stays unchanged.)

2. In `live_trading_preflight()` (~1149-1190): after the existing `options_instruments` block, add
   `asset_class_live_enablement = asset_class_live_enablement_snapshot(engine_mode=mode)`
   and add the key `"asset_class_live_enablement": dict(asset_class_live_enablement or {})` to the returned dict alongside `"options_instruments"` (~1189). Do not modify the `ok`/`blockers` computation.

3. Export the new helper in `__all__` (~1214-1230), next to `lob_deeplob_shadow_readiness_snapshot`.

GUARDRAIL: preserve all existing fail-closed/safe-mode gates; never enable live execution; read-then-change. The new snapshot is read-only reporting — it must add zero new live-permitting code paths and must leave the preflight `ok`/`blockers` result byte-for-byte identical for every input that does not change env.

VERIFY. Add a unit test (e.g. `tests/test_live_trading_preflight_asset_class_snapshot.py`) that, with all four gate envs unset, asserts `asset_class_live_enablement_snapshot()` reports `live_permitted=False` for fx/crypto/futures and options mode "shadow" (`live_permitted=False`), and `any_live_permitted=False`; and that setting each flag (`FX_LIVE_TRADING_ENABLED=1`, `CRYPTO_LIVE_TRADING_ENABLED=1`, `FUTURES_LIVE_TRADING_ENABLED=1`, `OPTIONS_INSTRUMENTS_MODE=live`) flips its `live_permitted` to True and sets `any_live_permitted=True`. Add an assertion that `live_trading_preflight(engine_mode="live")` now contains the `"asset_class_live_enablement"` key with the four classes, AND that adding the key did not change `state["ok"]`/`state["blockers"]` versus the prior behavior for a fixed env (snapshot the blockers list and confirm it is unchanged by this finding's edits). Run the new test and the existing preflight test module; capture exit codes.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

### HG-10 — Require CPCV/PBO (embargo+purge) leakage gate as a non-bypassable live-trading promotion blocker (P2)

ROLE: You are a senior trading-infra engineer hardening the live-trading preflight to make the CPCV/PBO leakage defense non-bypassable in live mode.

TARGET: /home/david/gitsandbox/system/system

PROBLEM (re-confirmed at HEAD 8184174, branch codex/worktree-production-readiness)
The Combinatorial Purged Cross-Validation / Probability-of-Backtest-Overfitting (CPCV/PBO) gate — the system's embargo+purge leakage defense for model promotions — is fully opt-in and is NEVER required, including in `ENGINE_MODE=live`. White's Reality Check and replay are already non-bypassable, but the embargo/purge leakage gate is not. Evidence:
- `[REPO] engine/strategy/statistical_gates.py:488` — `promotion_gate_config_from_env(...)` reads `"enabled": _safe_bool(os.environ.get("CHAMPION_PROMOTION_USE_STAT_GATE", "0"), False)` — the statistical promotion gate defaults OFF.
- `[REPO] engine/strategy/champion_manager.py:2251` — `cpcv_gate_enabled = str(os.environ.get("CPCV_ENABLED", "0")).strip().lower() in {"1","true","yes","on"}` — CPCV defaults OFF, and feeds `legacy_gate_enabled` at line 2257.
- `[REPO] engine/strategy/promotion_guard.py:1448-1449` — the CPCV gate evaluator returns `True, diagnostics` (status `"disabled"`, `passed=True`) immediately `if not bool(gate_config.get("enabled"))`. So when CPCV is off the gate self-reports PASS.
- `[REPO] engine/runtime/prod_preflight.py:533-540` — preflight only emits `f"cpcv={int(_env_truthy('CPCV_ENABLED', default=False))}"` as part of a NOTE string in the "promotion observation governance ok" note; it is informational, never a blocker.
- `[REPO] engine/runtime/live_trading_preflight.py:1056-1190` — `live_trading_preflight(...)` aggregates many `mode == "live"` blockers (prelive_reconcile, position_reconcile, broker_shutdown, backup_restore, wal_archiver, clock_health, execution_arming_audit, live_ai_safety, lob_deeplob_shadow, options_instruments) but has NO CPCV/PBO requirement. Net effect: live trading can be armed with the embargo/purge leakage gate entirely disabled.

DESIGN / REQUIRED CHANGE (implement exactly; read-then-change)
Add a new non-bypassable preflight blocker that requires the CPCV/PBO gate to be enabled (and minimally configured) whenever `ENGINE_MODE=live`. Mirror the existing required-snapshot wiring pattern used for `live_ai_safety` / `options_instruments` so the contract shape, dedup, and aggregation stay identical.

1) New snapshot function in `engine/runtime/live_trading_preflight.py` (place it alongside the other `*_snapshot` helpers, e.g. just before `live_trading_preflight`):
   ```python
   def cpcv_leakage_gate_snapshot(*, engine_mode: str) -> Dict[str, Any]:
       """Require the CPCV/PBO embargo+purge leakage gate to be enabled and configured in live."""
       mode = _normalize_mode(engine_mode, "safe")
       required = bool(mode == "live")
       # Default OFF -> override only by explicit operator action; never auto-enable execution.
       cpcv_enabled = _env_bool("CPCV_ENABLED", False)
       stat_gate_enabled = _env_bool("CHAMPION_PROMOTION_USE_STAT_GATE", False)
       embargo_pct = _safe_float(os.environ.get("CPCV_EMBARGO_PCT", "0"), 0.0)  # see config source below
       max_pbo = _safe_float(os.environ.get("CPCV_MAX_PBO", "0"), 0.0)
       blockers: list[str] = []
       if required:
           if not cpcv_enabled:
               blockers.append("cpcv_leakage_gate_disabled_in_live")
           if not stat_gate_enabled:
               blockers.append("champion_promotion_stat_gate_disabled_in_live")
           if cpcv_enabled and embargo_pct <= 0.0:
               blockers.append("cpcv_embargo_pct_not_configured")
           if cpcv_enabled and max_pbo <= 0.0:
               blockers.append("cpcv_max_pbo_not_configured")
       blockers = list(dict.fromkeys(blockers))
       return {
           "ok": not blockers,
           "required": required,
           "mode": mode,
           "cpcv_enabled": cpcv_enabled,
           "stat_gate_enabled": stat_gate_enabled,
           "embargo_pct": embargo_pct,
           "max_pbo": max_pbo,
           "reason": "ok" if not blockers else blockers[0],
           "blockers": blockers,
       }
   ```
   - Use the existing module helpers (`_env_bool` at line ~53, `_normalize_mode` at line ~83). If `_safe_float` is not already imported in this module, either import it from `engine.strategy.statistical_gates` or inline a small float coercion — do NOT add a new dependency cycle; prefer reading the canonical CPCV config via the existing accessor (next bullet) rather than re-reading env keys if that accessor already exists.
   - CONFIG SOURCE: first inspect `engine/strategy/promotion_guard.py` `cpcv_gate_config(...)` (used at promotion_guard.py:1435) and `engine/strategy/statistical_gates.py` to find the authoritative env var names for embargo and max-PBO (the gate config exposes `embargo_pct`, `max_pbo`, `n_splits`, `n_test_splits`, `min_path_sharpe` — see promotion_guard.py:1441-1446). Use those exact env var names instead of the placeholder `CPCV_EMBARGO_PCT` / `CPCV_MAX_PBO` above if they differ. The intent is: in live, CPCV must be enabled AND embargo>0 AND max_pbo>0 (a real leakage threshold, not the disabled 0.0 default).

2) Wire it into `live_trading_preflight(...)` (engine/runtime/live_trading_preflight.py, in the blocker-aggregation block ~1133-1156, immediately after the `execution_arming_audit` / before or after `live_ai_safety`):
   ```python
   cpcv_leakage_gate = cpcv_leakage_gate_snapshot(engine_mode=mode)
   if mode == "live" and not bool(cpcv_leakage_gate.get("ok")):
       blockers.extend(str(item) for item in list(cpcv_leakage_gate.get("blockers") or []))
   ```
   And add `"cpcv_leakage_gate": dict(cpcv_leakage_gate or {}),` to the returned dict (the block at ~1160-1190, next to `"execution_arming_audit"` and `"live_ai_safety"`).

3) Promote the prod_preflight NOTE to evidence: in `engine/runtime/prod_preflight.py` (~533-545), keep the existing note but additionally surface the new `cpcv_leakage_gate` sub-snapshot from `live_preflight` so that when `live_preflight["required"]` is true a disabled CPCV gate shows up as an issue/blocker line (mirror how `prelive_reconcile`, `clock_health`, etc. are pulled from `live_preflight` at ~546-549). Do not duplicate the blocker logic — read it from the `live_trading_preflight` result.

4) Do NOT change the default values of `CPCV_ENABLED`, `CHAMPION_PROMOTION_USE_STAT_GATE`, or any gate threshold. The requirement is enforced ONLY when `ENGINE_MODE=live`; safe/paper/shadow/dev behavior is unchanged. Do NOT modify `promotion_guard.py:1448` early-return semantics (that path stays as-is for non-live).

GUARDRAIL: Preserve all existing fail-closed/safe-mode gates; never enable live execution; read-then-change. New logic must be additive (a blocker that fails closed in live), must not auto-enable CPCV or any execution path, and must not alter non-live behavior.

VERIFY (exact checks that prove it done)
- `grep -n "cpcv_leakage_gate" engine/runtime/live_trading_preflight.py` shows the new snapshot function, its call in the aggregation block, and the returned-dict key.
- New/updated unit test (add to the existing live_trading_preflight test module under `tests/`, e.g. `tests/test_live_trading_preflight*.py`):
  - With `ENGINE_MODE=live` and `CPCV_ENABLED` unset/`0`: `live_trading_preflight()["ok"] is False` and `"cpcv_leakage_gate_disabled_in_live"` (and the stat-gate blocker) appear in `["blockers"]`.
  - With `ENGINE_MODE=live`, `CPCV_ENABLED=1`, `CHAMPION_PROMOTION_USE_STAT_GATE=1`, and embargo/max-PBO env vars set to positive values: the `cpcv_leakage_gate` sub-snapshot `["ok"] is True` and contributes no blockers.
  - With `ENGINE_MODE=safe` (and `paper`/`shadow`): `cpcv_leakage_gate["required"] is False` and it contributes no blockers regardless of `CPCV_ENABLED`.
- Run the targeted suite: `cd /home/david/gitsandbox/system/system && python -m pytest tests/ -k "live_trading_preflight or cpcv" -q` passes.
- `python -c "import engine.runtime.live_trading_preflight as p; import os; os.environ['ENGINE_MODE']='live'; s=p.live_trading_preflight(); print(s['ok'], [b for b in s['blockers'] if 'cpcv' in b])"` prints `False` with the cpcv blocker present (no secret values printed).

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

### HG-11 — Route terminal directional BUY/SELL through the confirmation modal up-front (P2)

ROLE: You are a senior front-end/runtime-safety engineer hardening the browser trading terminal's directional-order confirmation flow.

TARGET: /home/david/gitsandbox/system/system

PROBLEM (re-confirmed against HEAD 8184174 on branch codex/worktree-production-readiness; re-grep before editing — line numbers drift):
A live, sub-threshold directional BUY or SELL can be sent to the backend on a single click (or single keypress when armed) with NO operator consequence acknowledgement and NO typed confirmation token. The confirmation modal is only invoked REACTIVELY, after the backend rejects an over-threshold order.

Evidence in `ui/terminal/terminal.js`:
- `:44` — `import { requestConfirmation } from "../confirmation_modal.mjs";` (already wired, currently used only reactively).
- `:812-830` — `orderConfirmationPayload(token, holdMs = 0)` synthesizes a confirmation payload with `consequence_ack: true` hard-coded (`:822`), `confirmation_token`/`confirm`/`confirmation` set to the literal string (`:817-819`), and `confirmation_method` chosen by `holdMs`. For directional orders the caller passes the literal token `"TRADE"`, so the payload claims a typed/acknowledged confirmation that the human never performed.
- `:1252-1276` — `async function submitTerminalOrder(side, qty, label)`: after `canSubmitDirectionalOrder(label || side)` (`:1253`) and a positive-qty check, it builds `body = { symbol, side, qty, ...orderConfirmationPayload("TRADE") }` (`:1257-1262`) and POSTs DIRECTLY to `/api/terminal/order` (`:1265`). Only on a thrown `confirmation_required`/`threshold_*` error does it call `requestTerminalThresholdPayload(...)` -> `requestConfirmation(...)` and retry (`:1266-1273`). There is no up-front modal for a sub-threshold order.
- `:1416-1422` (`btnBuy` click) and `:1424-1430` (`btnSell` click) call `submitTerminalOrder` on a single click.
- `:1375-1397` — keydown handler: `b` (`:1379-1387`) and `s` (`:1389-1397`) call `submitTerminalOrder` gated only by `canUseKeyboardTradingShortcut(...)` (`:1198-1204`), which merely checks the `_terminalArmed` toggle (`:669`, toggled at `:1364-1373`) plus `canSubmitRealTrade`. An armed operator sends a live order on a single keypress with no per-order acknowledgement.
- Contrast FLATTEN: `startFlattenHold` (`:1299-1315`) requires a `FLATTEN_HOLD_MS = 1500` (`:48`) hold gesture before `submitTerminalFlatten` (`:1278-1297`); keypress `f` is explicitly refused (`:1399-1406`). Directional orders have strictly weaker protection than flatten.
- The confirmation modal's real payload builder is `buildConfirmationPayload` in `ui/confirmation_modal.mjs` (`:96-121`), and `requestConfirmation` (`:146`) resolves `{ ok: true, confirmed: true, phrase, reason, requestId, payload: buildConfirmationPayload(...) }` (`:343-353`). That payload already supplies `confirmation_token`, `confirmation_method`, `confirmation_hold_ms`, `consequence_ack: true`, and `request_id` derived from REAL operator input — exactly the fields the synthetic `orderConfirmationPayload("TRADE")` currently fakes.
- Backend execution barrier `canSubmitDirectionalOrder` (`:859-864`) and the server-side threshold gate stay as-is; this finding is purely the missing CLIENT-SIDE up-front confirmation.

DESIGN / REQUIRED CHANGE (specify the optimal solution; implement without re-discovery):
Make every directional submit obtain an explicit operator confirmation (consequence acknowledgement + typed token) BEFORE the first POST, so a sub-threshold live BUY/SELL can never be sent on a single click or single keypress. Reuse the existing modal and its honest payload; stop synthesizing `consequence_ack`/token for directional orders.

1. Add a directional confirmation step in `submitTerminalOrder` (`ui/terminal/terminal.js`):
   - After the `canSubmitDirectionalOrder` and positive-qty checks and `renderOrderPreview(side)`, and BEFORE building `body`, call `requestConfirmation(...)` to obtain operator confirmation. Mirror the option shape already used by `requestTerminalThresholdPayload` (`:1225-1235`):
     - `title: "Confirm terminal order"`
     - `action: "Terminal directional order"`
     - `actionId: "terminal.order"`
     - `target:` `` `${String(side).toUpperCase()} ${STATE.symbol} qty ${fmtSymbolQty(STATE.symbol, cleanQty)}` ``
     - `consequence:` a string built from the live order preview context (side, symbol, qty, est notional, and the execution-barrier mode) so the operator sees the real consequence — reuse the same fields `renderOrderPreview` (`:832-851`) already computes; factor that computation into a small helper if needed to avoid divergence.
     - `confirmText: "TRADE"` (the required typed token — keep the existing literal so backend contract is unchanged), `submitLabel: "Send Order"`, `actor: "terminal_operator"`, `sourceSurface: "terminal"`.
     - For the fast keyboard path, require a short hold to defeat key-repeat/double-fire: pass `holdMs:` a new constant `DIRECTIONAL_HOLD_MS` (define near `FLATTEN_HOLD_MS` at `:48`; default 750ms) when the call originates from a keyboard shortcut, and `0` when it originates from a button click. Thread an explicit `origin` ("button" | "keyboard") argument through `submitTerminalOrder(side, qty, label, { origin })` rather than inferring it.
   - If `!confirmation || !confirmation.ok`, call `setTerminalBanner("warn", `${label || side} confirmation cancelled.`)` and `return` (do NOT POST).
   - Build the POST `body` from the modal result, NOT from `orderConfirmationPayload("TRADE")`: spread the modal's `confirmation.payload` (which already carries `confirmation_token`/`confirmation`/`confirm`, `confirmation_method`, `confirmation_hold_ms`, `consequence_ack`, `actor`, `source`/`source_surface`, `request_id`) alongside `{ symbol: STATE.symbol, side, qty: cleanQty }`. Ensure the resulting body still contains the keys the backend reads (`confirm`/`confirmation`/`confirmation_token` === "TRADE", `action_id: "terminal.order"`, `source_surface: "terminal"`); if `buildConfirmationPayload` does not emit `action_id`/`target`, add those two keys explicitly from the request context. Do NOT hard-code `consequence_ack: true` — take it from the modal payload.
   - Leave the existing reactive `requestTerminalThresholdPayload` retry path (`:1266-1273`) intact for the over-threshold case; it now layers on top of (not instead of) the up-front confirmation.

2. Stop using the synthetic directional payload: `orderConfirmationPayload` (`:812-830`) keeps being used by FLATTEN (`:1282`); for directional orders it must no longer be the source of `consequence_ack`. Do not change FLATTEN behavior.

3. Keyboard fast path: keep the `_terminalArmed` arm toggle as the prerequisite to even reach the shortcut, but the shortcut must STILL go through the modal (with the `DIRECTIONAL_HOLD_MS` hold). Update the arm-toggle banner copy (`:1369-1372`) and the keyboard handlers (`:1379-1397`) to pass `{ origin: "keyboard" }`. Button handlers (`:1416-1430`) pass `{ origin: "button" }`. As an alternative permitted by the design, a one-time per-arm-session acknowledgement is acceptable ONLY if it still presents the typed-token modal at least once per arm session AND still applies the hold on each keypress; the per-order modal is the preferred default — implement the per-order modal unless it materially regresses an existing test, in which case STOP and flag NO-GO-pending-review.

4. Extract a pure, unit-testable helper so this can be tested without a DOM: add an exported function (e.g. `export function buildDirectionalOrderBody({ symbol, side, qty, confirmationPayload })`) to a small importable module (either export it from `ui/terminal/terminal.js` if the entry module can export, or place it in a new `ui/terminal/terminal_order.mjs` and import it into `terminal.js`). It must (a) refuse to produce a body when `confirmationPayload` lacks a truthy `consequence_ack` or a `confirmation_token`/`confirm` !== "TRADE" (throw or return null), and (b) otherwise return the exact POST body. `submitTerminalOrder` must call this helper so the guard cannot be bypassed.

GUARDRAIL: preserve all existing fail-closed/safe-mode gates; never enable live execution; read-then-change. Do not weaken `canSubmitDirectionalOrder`/`canSubmitRealTrade`/the backend threshold gate; do not alter FLATTEN; do not touch broker/order server code; never print or commit secret values.

VERIFY (the exact checks that prove it done):
- New JS unit test (Node `node:test`, mirroring `tests/test_terminal_decision_overlays.mjs`): import the extracted helper and assert (a) a valid confirmation payload yields a body whose `confirm`/`confirmation`/`confirmation_token` === "TRADE", `consequence_ack` is truthy, and `symbol`/`side`/`qty` match the inputs; (b) a payload missing `consequence_ack` or with a wrong token produces no body (throws/null). Run it: `node --test tests/test_terminal_directional_confirmation.mjs` passes.
- Behavioral assertion via a DOM-stubbed test OR an explicit code-path check: a `requestConfirmation` mock that resolves `{ ok: false }` (cancel) results in ZERO calls to `postJson("/api/terminal/order", ...)`; a mock that resolves `{ ok: true, payload }` results in exactly one POST whose body carries the modal-derived (not synthesized) `request_id`/`confirmation_method`.
- Grep proof of the up-front wiring: `grep -n "requestConfirmation" ui/terminal/terminal.js` shows a call inside `submitTerminalOrder` ABOVE the first `postJson("/api/terminal/order"` line; and `submitTerminalOrder` no longer spreads `orderConfirmationPayload("TRADE")` into the directional body.
- Regression: existing `tests/test_terminal_order_contracts.py` and `tests/test_terminal_gate_mode_parity.py` still pass (the on-the-wire body must still satisfy the backend's `confirm`/`action_id`/`source_surface` contract), and `node --test tests/test_terminal_decision_overlays.mjs` still passes. If `DISABLE_LIVE_EXECUTION` default-true behavior or any threshold-confirmation test would change, STOP and flag NO-GO-pending-review.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

### HG-12 — Make sim order→fill + dedup idempotency marker commit atomically (P2)

ROLE: Senior execution-engine engineer hardening the sim broker apply path against non-atomic idempotency.

TARGET: /home/david/gitsandbox/system/system

PROBLEM (re-confirmed at HEAD 8184174, branch codex/worktree-production-readiness)
`apply_new_portfolio_orders` in engine/execution/broker_sim.py commits the sim apply pass in TWO separate transactions, leaving a crash window where fills/cash are durable but the idempotency marker is not — causing the next run to re-apply the same batch (duplicate fills + doubled cash debit).

Evidence:
- engine/execution/broker_sim.py:1807 `_write_fill(...)` does a plain `INSERT INTO broker_fills(...)` (the three branches at 1830/1855/1873) with NO `ON CONFLICT`, NO per-fill commit, and writes a (nullable) `source_order_id`.
- The `broker_fills` DDL at engine/execution/broker_sim.py:450-464 has `source_order_id INTEGER` (nullable) and NO unique constraint; the only index is `idx_broker_fills_ts` (466). So nothing in the schema prevents duplicate fills for the same `source_order_id`.
- `_broker_sim_phase_persist_account_positions` (engine/execution/broker_sim.py:2916-2950) persists account/positions and ends with `con.commit()` at line 2948 (FIRST commit). It is called from `apply_new_portfolio_orders` at line 3890.
- Immediately after, at engine/execution/broker_sim.py:3897-3900, the idempotency marker is written and committed in a SECOND transaction:
  `if order_id is not None: _set_meta(con, "last_portfolio_orders_id", str(order_id), book_key=book_key); con.commit()`.
- The ONLY re-apply guard is `int(last_applied) >= int(order_id)` in `_broker_sim_phase_validate_gate` at engine/execution/broker_sim.py:2565-2569.
- Fills are written earlier in the loop (calls at 2198/2234/2795) and account/positions are committed at 2948 BEFORE the marker commit at 3900.

Failure mode: a crash, exception, or failed second commit between line 2948 and line 3900 leaves broker_fills rows and the cash debit durable while `last_portfolio_orders_id` stays un-advanced. The next `apply_new_portfolio_orders` for the same `order_id` passes the 2565-2569 guard (last_applied < order_id) and re-applies the entire batch → duplicate broker_fills + doubled cash debit + corrupted positions. This is sim-ledger only (no real capital) but it corrupts the attribution/PnL ledger the promotion loop trusts.

DESIGN / REQUIRED CHANGE (make the apply atomic; defense-in-depth dedup)
Read the full `apply_new_portfolio_orders` body and `_broker_sim_phase_persist_account_positions` first, then:

1) Single-transaction commit (primary fix). Fold the marker write into the SAME transaction that commits fills/account/positions, so the marker advances iff fills+account+positions are durable:
   - In `_broker_sim_phase_persist_account_positions` (engine/execution/broker_sim.py:2916-2950), REMOVE the trailing `con.commit()` at line 2948 (do NOT commit inside this phase). Keep the `return _mark_to_market(...)` (mark-to-market reads only; leave it). If `_mark_to_market` performs writes, ensure those writes also occur before the single commit (read it to confirm; if it writes, call it before committing in the caller).
   - In `apply_new_portfolio_orders`, move the marker write so it sits in the same uncommitted unit of work as the persisted account/positions, then issue ONE `con.commit()` covering fills (already executed earlier in the loop) + account/positions + marker. Concretely, replace the 3897-3900 block so it reads `_set_meta(...)` (no commit) when `order_id is not None`, and add a single `con.commit()` AFTER both the persist-account-positions call (3890) and the `_set_meta` call, BEFORE the options-lifecycle step (currently 3902-3906). Because SQLite autocommit is off (the codebase relies on explicit `con.commit()`), all `con.execute` writes since the last commit are part of one transaction — confirm no intermediate `con.commit()` exists between the first fill write in the loop and this new single commit (grep `con.commit()` within `apply_new_portfolio_orders`; if any intermediate commit exists in the per-row loop, that is acceptable only if it does not advance the marker — but ensure the FINAL fills+account+positions+marker all land together; if intermediate commits are required for the chunking loop, instead wrap the final persist+marker in one commit and rely on the dedup in step 2 to neutralize partial-batch replay).
   - Keep `apply_option_lifecycle` (3903-3904) committing its own work as it does today (it runs after the marker is durable; a crash there is safe because the batch is already marked applied).
   - On any exception before the single commit, the existing `try/finally` (con closed) must NOT commit — verify the `except`/`finally` around `apply_new_portfolio_orders` does not call `con.commit()`; if it does, change it to `con.rollback()` so a mid-apply crash leaves NOTHING durable (neither fills nor marker).

2) Dedup constraint (defense-in-depth, survives partial-batch replay and any future commit drift). Add a uniqueness guard on `broker_fills` keyed by the apply identity:
   - Add a partial unique index in the DDL block near engine/execution/broker_sim.py:466 (and ensure `init_broker_db()` creates it for existing DBs):
     `CREATE UNIQUE INDEX IF NOT EXISTS uq_broker_fills_src ON broker_fills(source, book_key, symbol, ts_ms, source_order_id) WHERE source_order_id IS NOT NULL;`
     (Pick the column tuple that is unique per real fill — re-read the loop to confirm each fill for a given order_id has a distinct symbol/ts_ms; if multiple chunked fills share symbol+ts_ms within one order_id, ADD the existing per-fill chunk index/sequence to the key, or add a new `fill_seq INTEGER` column written by `_write_fill` and include it. Do NOT collapse legitimately-distinct chunk fills.)
   - In `_write_fill` (1830/1855/1873 branches), change `INSERT INTO broker_fills` to `INSERT ... ON CONFLICT DO NOTHING` (use `INSERT OR IGNORE` for sqlite) so a replay cannot insert a duplicate fill. Guard this so it only applies when `source_order_id IS NOT NULL` (sim/shadow apply path) and does not silently swallow genuine inserts — count `rowcount` and surface it.
   - Before applying the index to existing DBs, the migration in `init_broker_db()` must be idempotent and must NOT crash if pre-existing duplicate rows would violate the new unique index; if duplicates already exist, log via `_warn_nonfatal(...)` and skip creating the unique index (or create it only after a dedup pass) — do not hard-fail startup.

3) Do NOT change the 2565-2569 guard semantics; it remains the fast-path skip. The atomic commit makes that guard correct; the dedup index makes it safe even if the guard is ever bypassed.

GUARDRAIL: preserve all existing fail-closed/safe-mode gates; never enable live execution; read-then-change. This is the SIM broker only — do not touch broker_router live/Alpaca/IBKR paths. Do not weaken the `_broker_sim_phase_validate_gate` dry-run/no-orders/already-applied gates (2545-2592).

VERIFY (must all pass)
1) `grep -n "con.commit()" engine/execution/broker_sim.py` shows the account/positions commit removed from `_broker_sim_phase_persist_account_positions` (no commit at the old 2948), and exactly ONE commit in `apply_new_portfolio_orders` covering account/positions+marker (the old 3900 standalone marker commit is gone).
2) New test engine/execution/tests (or the existing broker_sim test module) `test_broker_sim_apply_atomic_idempotency`:
   - Monkeypatch/inject a failure (raise) AFTER `_broker_sim_phase_persist_account_positions` but BEFORE/at the marker step; run `apply_new_portfolio_orders` for order_id N; assert it raised; then assert broker_fills has ZERO rows for that batch AND `last_portfolio_orders_id` is unchanged (nothing durable) — i.e., rollback, not partial.
   - Happy path: apply order_id N once → record fills count + cash; call `apply_new_portfolio_orders` AGAIN for the SAME order_id with the marker artificially un-advanced (simulate the old crash window) → assert NO new broker_fills rows are added (ON CONFLICT/unique index neutralizes replay) and cash is NOT double-debited.
   - Assert `uq_broker_fills_src` exists: `PRAGMA index_list(broker_fills)` includes it after `init_broker_db()`.
3) Run the existing broker_sim test suite green: `python -m pytest engine/execution -k broker_sim -q` (or the repo's canonical invocation) — no regressions in the dry-run/no-orders/already-applied paths.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

### HG-13 — systemd WatchdogSec heartbeat + MemoryMax/OOMScoreAdjust on engine/operator units (P2)

ROLE: You are a senior reliability engineer hardening the production systemd units so a hung process is killed and an unbounded allocation does not OOM-kill co-located Postgres/Redis.

TARGET: /home/david/gitsandbox/system/system

PROBLEM (re-confirmed at HEAD 8184174 / branch codex/worktree-production-readiness):
- deploy/systemd/trading-engine.service: `Type=simple` (line 9), `Restart=on-failure` / `RestartSec=5` (lines 15-16). ExecStart runs `start_system.py` (line 14). There is NO `WatchdogSec`, NO sd_notify `Type=notify`, and NO `MemoryHigh`/`MemoryMax`/`OOMScoreAdjust`.
- deploy/systemd/trading-operator.service: identical shape — `Type=simple` (line 9), `Restart=on-failure` (line 15), ExecStart runs `node /opt/trading-system/repo/boot/operator_server.js` (line 14); also missing watchdog and memory caps.
- Consequence #1 (liveness): a Python deadlock/livelock in the engine, or an event-loop stall in the operator, leaves the unit `active (running)` forever. `Restart=on-failure` never fires because the process never exits non-zero. No external liveness contract exists.
- Consequence #2 (memory): `ops/server/memory_pressure_hardening.sh` caps ZFS ARC at 48 GiB on the 128 GiB host `bart` (docs/MEMORY_PRESSURE_RUNBOOK.md lines 9, 17, 19-22), leaving ~80 GiB for Postgres shared memory/work_mem, Redis, and the runtime. With no cgroup `MemoryMax` and default `OOMScoreAdjust=0`, an unbounded allocation in the engine makes the kernel OOM killer free to pick Postgres/Redis instead of the engine — losing the database/cache rather than just restarting the (already-supervised, restart-guarded) engine.
- The engine already runs supervisory loops where a heartbeat can be emitted: the `_watch_ingestion()` watchdog thread (start_system.py line ~1084, sleeps every `_INGESTION_WATCHDOG_SLEEP_S`, default 2.0s) and the long-lived `dashboard_server.run_server` in `main()`. The operator already runs a periodic `setInterval` watchdog (boot/operator_server.js line ~7310) and a WS heartbeat timer (line ~8427).

DESIGN / REQUIRED CHANGE (implement exactly; do not re-discover):

1) Engine unit — deploy/systemd/trading-engine.service:
   - Change `Type=simple` to `Type=notify`.
   - Add `WatchdogSec=60` (env-overridable contract; the code MUST ping at <= WatchdogSec/2 = 30s).
   - Add `NotifyAccess=main`.
   - Add `MemoryAccounting=true`, `MemoryHigh=24G`, `MemoryMax=32G` (soft-throttle then hard-cap; leaves headroom for 48G ARC + Postgres shared_buffers per docs/MEMORY_PRESSURE_RUNBOOK.md). Add `OOMScoreAdjust=600` so the kernel prefers killing the (restart-guarded) engine over Postgres/Redis.
   - Keep `Restart=on-failure` and `RestartSec=5` unchanged — systemd reports a watchdog kill as a failure, so `Restart=on-failure` already covers it. Do NOT change to `Restart=always` (preserve the StartLimit crash-loop guard at lines 5-6).

2) Operator unit — deploy/systemd/trading-operator.service:
   - Same edits: `Type=notify`, `WatchdogSec=60`, `NotifyAccess=main`, `MemoryAccounting=true`, `MemoryHigh=4G`, `MemoryMax=6G`, `OOMScoreAdjust=700` (operator is the least critical of the three co-located memory consumers). Keep Restart/StartLimit unchanged.

3) Engine sd_notify wiring — start_system.py:
   - Add a small, dependency-free helper module (NEW FILE) engine/runtime/sd_notify.py exposing `notify_ready() -> bool`, `notify_watchdog() -> bool`, and `notify(state: str) -> bool`. Implementation: read `NOTIFY_SOCKET` env; if unset/empty, return False (no-op — must be a safe no-op under pytest, dev, and non-systemd runs). If set, send the datagram over `socket.AF_UNIX, SOCK_DGRAM` (handle the leading `@` abstract-namespace form by replacing it with NUL). Swallow all socket errors and return False; NEVER raise.
   - In start_system.py: import the helper. Emit `notify_ready()` once, immediately after the dashboard server has bound and the production readiness gate has passed (right before/at the point the long-lived server starts — after the existing `_run_dashboard_server*` bind path in `main()`). Add `READY=1` only after startup validation succeeds (fail-closed: do not signal ready on a degraded boot).
   - Emit `notify_watchdog()` (`WATCHDOG=1`) on a cadence strictly faster than `WatchdogSec/2`. Add it inside the existing `_watch_ingestion()` loop at the bottom of each iteration (the loop already wakes every ~2s via `_INGESTION_WATCHDOG_STOP.wait(...)`), guarded so it only pings while lifecycle state is RUNNING/healthy. The ping interval is bounded by `_INGESTION_WATCHDOG_SLEEP_S` (default 2.0s) << 30s, so the cadence is safe. Do NOT add a second always-on thread if the ingestion watchdog already runs; if `START_INGESTION_WITH_SERVER` is disabled (so `_watch_ingestion` early-returns), fall back to a minimal dedicated daemon thread that loops `notify_watchdog()` every `WATCHDOG_PING_SECONDS` (env, default `min(WatchdogSec/2, 30)`=15s) until `_INGESTION_WATCHDOG_STOP` is set. The watchdog ping MUST be gated on liveness (only ping when the runtime is actually healthy, e.g. lifecycle_state.get_state() not in a FAILED/STUCK state) so a livelocked-but-degraded engine stops pinging and is killed by systemd.

4) Operator sd_notify wiring — boot/operator_server.js:
   - Add a tiny notify helper using `dgram` to send to the `process.env.NOTIFY_SOCKET` UNIX datagram socket (handle abstract `@` -> NUL prefix); no-op if unset; never throw.
   - Send `READY=1` inside the `_httpServer = app.listen(...)` callback (line ~8461) once the server is listening.
   - Send `WATCHDOG=1` from the existing periodic watchdog `setInterval` (line ~7310) — gate it on the watchdog body completing without an unrecoverable error so a wedged operator stops pinging. Interval is already << 30s.

5) Heartbeat contract documentation:
   - Add a "systemd watchdog heartbeat contract" subsection to docs/FAILURE_MODES.md: state that engine + operator are `Type=notify`, send `READY=1` after successful bind/validation, ping `WATCHDOG=1` at <=30s (WatchdogSec=60), that a missed ping => systemd SIGABRT => `Restart=on-failure` (subject to StartLimitBurst), that the ping is liveness-gated, and document `OOMScoreAdjust`/`MemoryMax` values plus the rationale (engine/operator preferred OOM victims over Postgres/Redis; caps sized against the 48G ARC + Postgres budget in docs/MEMORY_PRESSURE_RUNBOOK.md). Cross-link MEMORY_PRESSURE_RUNBOOK.md.

6) Make values env-overridable where the deploy script templates units: if deploy/systemd units are installed/rendered by an installer (check ops/server/* and the compose/systemd override installer referenced in tests/test_compose_deployment_assets.py and tests/test_backup_restore_evidence_pipeline.py), thread `TRADING_ENGINE_MEMORY_MAX`, `TRADING_ENGINE_OOM_SCORE_ADJ`, `TRADING_WATCHDOG_SEC` (and operator equivalents) so operators can retune without editing committed unit files. If units are installed verbatim, keep the literal values above and note the override path is editing the unit + `systemctl daemon-reload`.

7) Test/validator (NEW): add tests/test_systemd_watchdog_hardening.py asserting, by parsing both unit files:
   - `Type=notify`, presence of `WatchdogSec=`, `NotifyAccess=main`, `MemoryAccounting=true`, `MemoryHigh=`, `MemoryMax=`, and `OOMScoreAdjust=` > 0 on BOTH units.
   - `Restart=on-failure` and the StartLimit lines are still present (no regression of the crash-loop guard).
   - WatchdogSec parses to an integer and the chosen engine ping interval (`_INGESTION_WATCHDOG_SLEEP_S` default and any `WATCHDOG_PING_SECONDS` default) is <= WatchdogSec/2.
   - Add a unit test for engine/runtime/sd_notify.py: with `NOTIFY_SOCKET` unset all functions return False and raise nothing; with a bound temporary `AF_UNIX SOCK_DGRAM` socket, `notify_ready()`/`notify_watchdog()` deliver the expected `READY=1`/`WATCHDOG=1` bytes.
   - If a repo-contract validator enumerates required unit directives (see tests/test_validate_repo_contract.py / tests/test_compose_deployment_assets.py), extend it to require the new directives so future edits cannot silently drop them.

GUARDRAIL: preserve all existing fail-closed/safe-mode gates; never enable live execution; read-then-change. READY=1 must only be sent after the production readiness gate passes; the watchdog ping must be liveness-gated so a degraded/wedged runtime is killed rather than kept alive; sd_notify must be a pure no-op when NOTIFY_SOCKET is unset (dev/pytest/non-systemd) and must never raise.

VERIFY (all must pass):
- `python -m pytest tests/test_systemd_watchdog_hardening.py -q` passes, and the existing tests/test_compose_deployment_assets.py + tests/test_validate_repo_contract.py still pass.
- `systemd-analyze verify deploy/systemd/trading-engine.service deploy/systemd/trading-operator.service` reports no errors for the new directives (run if systemd is available; otherwise the unit-file parse test covers it).
- `grep -nE 'Type=notify|WatchdogSec=|NotifyAccess=main|MemoryMax=|MemoryHigh=|OOMScoreAdjust=' deploy/systemd/trading-engine.service deploy/systemd/trading-operator.service` shows all directives on both units; `grep -n 'Restart=on-failure' deploy/systemd/*.service` still matches both.
- `grep -n 'WATCHDOG=1\|notify_watchdog\|notify_ready' start_system.py boot/operator_server.js engine/runtime/sd_notify.py` shows ready + heartbeat wiring in all three.
- `python -c "import os; os.environ.pop('NOTIFY_SOCKET',None); from engine.runtime.sd_notify import notify_ready, notify_watchdog; assert notify_ready() is False and notify_watchdog() is False; print('noop-ok')"` prints `noop-ok`.
- docs/FAILURE_MODES.md contains the heartbeat-contract subsection documenting WatchdogSec, the <=30s ping, OOMScoreAdjust/MemoryMax values, and a cross-link to docs/MEMORY_PRESSURE_RUNBOOK.md.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## P3 — Polish (PL)

### PL-1 — Router broker-health degraded probe fails open when execution-health reader is missing or raises (P3)

STATUS UPDATE (2026-06-26): Implemented in `engine/execution/broker_router.py` with regression coverage in `tests/test_broker_router_degraded_probe_fail_closed.py`. The router now imports `engine.cache.wrappers.execution_health.read_execution_health`, returns WARNING-active snapshots for missing/raising readers, leaves `None`/empty health quiet for fresh startup, and preserves the existing recognized degraded-state mapping.

ROLE: Senior execution-safety engineer hardening the broker router's degraded-health probe so an unknown/unreadable execution-health signal fails closed instead of being treated as healthy.

TARGET: /home/david/gitsandbox/system/system

PROBLEM (re-confirmed at HEAD 8184174, branch codex/worktree-production-readiness):
`engine/execution/broker_router.py` builds the `execution_degraded` input to the execution gate via `_execution_degraded_from_cache()` (`broker_router.py:468-490`), and passes the result into `_execution_gate_snapshot(...)` at BOTH live-gate call sites: `broker_router.py:578` (`_execution_gate_or_block`) and `broker_router.py:629` (the per-broker real-trading gate).

The probe fails OPEN in two ways:
1. Reader-missing: `if _read_execution_health is None: return {"active": False, "detail": {}}` (`broker_router.py:469-470`). This is not a corner case — `_read_execution_health` is HARDCODED to `None` at `broker_router.py:100` (`_read_execution_health = None  # type: ignore`) and is NEVER rebound by any import in the module. So the probe ALWAYS takes this branch today and reports "not degraded," even though a real reader exists at `engine/cache/wrappers/execution_health.py:63` (`def read_execution_health() -> dict[str, Any] | None`). The degraded probe is effectively dead and silently fail-open.
2. Reader-raises: the `except Exception` handler at `broker_router.py:473-479` logs `BROKER_ROUTER_EXECUTION_HEALTH_CACHE_READ_FAILED` then `return {"active": False, "detail": {}}` — a read failure is reported as healthy.

Only when state is one of {critical/degraded/down/unhealthy} does it return an active snapshot (`broker_router.py:480-490`). Unknown/empty health and read failures are indistinguishable from "healthy."

Downstream, `engine/runtime/gates.execution_gate_snapshot(...)` accepts `execution_degraded: bool | dict` and routes it through `get_execution_degraded_snapshot()` → `_explicit_execution_degraded_snapshot()` (`gates.py:667-695`, `gates.py:470`). A `{"active": False}` mapping contributes nothing, so the router's "unknown health" collapses to "healthy" in the gate. This is a REDUNDANT control (the hard live gates — `live_execution_disabled()`, mode/arming, kill-switch, real_trading_allowed — are independent and still block live), hence P3; but a safety probe must never silently fail open, and the dead-reader binding means the throttle/degrade signal from execution health currently never fires from the router.

DESIGN / REQUIRED CHANGE (read-then-change; minimal, additive, fail-closed):
File: `engine/execution/broker_router.py`.

A. Bind the real reader (fix the dead hardcode). Replace the bare `_read_execution_health = None` at `broker_router.py:100` with a guarded import mirroring the existing wrapper-import pattern used a few lines above for `_get_execution_mode` / `_kill_switch_snapshot` (try-import + `_import_nonfatal("BROKER_ROUTER_EXECUTION_HEALTH_WRAPPER_IMPORT_FAILED", e)` + `_read_execution_health = None` fallback):
```
try:
    from engine.cache.wrappers.execution_health import read_execution_health as _read_execution_health  # type: ignore
except Exception as e:
    _import_nonfatal("BROKER_ROUTER_EXECUTION_HEALTH_WRAPPER_IMPORT_FAILED", e)
    _read_execution_health = None  # type: ignore
```
Use the exact `_import_nonfatal` helper already used in this import block (do not introduce a new logger). Do not add any module-level side effects or DB/network calls at import time — `read_execution_health()` is only invoked inside `_execution_degraded_from_cache()`.

B. Make `_execution_degraded_from_cache()` fail closed on the two "unknown" paths (`broker_router.py:468-490`). The returned mapping must be shape-compatible with what `gates._explicit_execution_degraded_snapshot` consumes (keys: `active`, `severity`, `reason`, `reason_codes`, `detail`). Change:
- Reader-missing (`_read_execution_health is None`, currently `:469-470`): return an ACTIVE WARNING snapshot, NOT a critical hard-block (a redundant probe must not start hard-blocking the live gate where it previously did nothing), e.g.:
  `return {"active": True, "severity": "WARNING", "reason": "execution_health_reader_unavailable", "reason_codes": ["execution_health_reader_unavailable"], "detail": {}}`
- Reader-raises (`except Exception` at `:473-479`): keep the existing `_warn_nonfatal("BROKER_ROUTER_EXECUTION_HEALTH_CACHE_READ_FAILED", ...)` call UNCHANGED, then return an ACTIVE WARNING snapshot:
  `return {"active": True, "severity": "WARNING", "reason": "execution_health_read_failed", "reason_codes": ["execution_health_read_failed"], "detail": {}}`
- Reader returns None / empty mapping (the `health = _read_execution_health() or {}` path at `:472`, then `state` empty at `:480-482`): when the reader returns a usable mapping with a recognized healthy/empty state, keep returning `{"active": False, "detail": dict(health or {})}` as today — a genuine "healthy/quiet" read is NOT degraded. Only the missing-reader and raised-exception paths flip to WARNING-active. (If the reader returns `None` specifically — distinct from `{}` — treat it the same as an empty healthy read, i.e. NOT degraded, since `read_execution_health()` returns `None` for "no row primed yet"; do not hard-block a freshly started system. Document this with an inline comment.)
- The recognized-degraded path (`:480-490`) is unchanged.

Rationale for WARNING (not CRITICAL): `engine/runtime/gates.execution_gate_snapshot` and `get_execution_degraded_snapshot` rank a WARNING-active source as a throttle/degrade signal without forcing a CRITICAL hard stop (`gates.py:282` `_normalize_severity`, `gates.py:273` `_severity_rank`), so "unknown health" becomes a visible degraded reason instead of being silently dropped, while the independent live gates remain the authoritative blockers. Do NOT change `gates.py`, `execution_gate_snapshot`, or any live-enable logic.

C. No behavior change to the recognized-state mapping at `broker_router.py:483-490` and no change to either call site at `:578` / `:629` (they already pass the dict through `cast(Any, ...)`).

GUARDRAIL: preserve all existing fail-closed/safe-mode gates; never enable live execution; read-then-change. The change must only make the degraded probe MORE conservative (unknown ⇒ WARNING-active), never relax any block, and must not touch `live_execution_disabled()`, kill-switch, mode/arming, `real_trading_allowed`, or any hard live gate.

VERIFY (from /home/david/gitsandbox/system/system):
1. Reader now bound: `python -c "import engine.execution.broker_router as r; assert r._read_execution_health is not None, 'reader still None'; print('reader bound ok')"` → exit 0.
2. Add `tests/test_broker_router_degraded_probe_fail_closed.py` (flat `tests/*.py`, `unittest.TestCase`, `sys.path.insert(0, REPO_ROOT)`):
   - Monkeypatch `engine.execution.broker_router._read_execution_health = None`, call `_execution_degraded_from_cache()`, assert `out["active"] is True and out["severity"] == "WARNING" and "execution_health_reader_unavailable" in out["reason_codes"]`.
   - Monkeypatch `_read_execution_health` to a callable that raises, assert `out["active"] is True and out["severity"] == "WARNING" and "execution_health_read_failed" in out["reason_codes"]` (and that it did not raise).
   - Monkeypatch `_read_execution_health` to return `None` and separately `{}`: assert `out["active"] is False` for both (genuine quiet read is not degraded).
   - Monkeypatch `_read_execution_health` to return `{"state": "critical"}`: assert `out["active"] is True and out["severity"] == "CRITICAL"` (existing behavior preserved).
   - Regression: assert the live-gate hard blocks are unaffected — with `LIVE_EXECUTION_DISABLED`/safe defaults, `_execution_gate_or_block(dry_run=False)` still returns a block dict (status in {`execution_blocked`, `execution_blocked_gate_unavailable`, `execution_blocked_gate_providers_missing`}), i.e. the WARNING-active degraded signal did not flip anything to allowed.
   Run: `python -m pytest tests/test_broker_router_degraded_probe_fail_closed.py -q` → exit 0.
3. `python -c "import engine.execution.broker_router, engine.cache.wrappers.execution_health; print('imports ok')"` → exit 0.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

### PL-2 — Document PORTFOLIO_VOL_HARD_BLOCK=0 soft-only default and harden None suppression fallback to conservative SOFT_THROTTLE in live (P3)

ROLE: You are a senior risk/execution engineer hardening portfolio-vol and trade-suppression defaults for clarity and fail-closed behavior.

TARGET: /home/david/gitsandbox/system/system

PROBLEM (re-confirmed at HEAD 8184174, branch codex/worktree-production-readiness):

Two interacting permissive defaults around portfolio-vol and trade suppression are undocumented and silently behave in the permissive direction:

1. Portfolio-vol hard block is disabled by default.
   - engine/risk/portfolio_risk_engine.py:130 — `PORTFOLIO_VOL_HARD_BLOCK = float(os.environ.get("PORTFOLIO_RISK_VOL_HARD_BLOCK", "0.0"))  # 0 disables`.
   - engine/risk/portfolio_risk_engine.py:2901 surfaces `info["portfolio_vol_hard_block"]`, and engine/risk/portfolio_risk_engine.py:2904 gates the block as `if float(PORTFOLIO_VOL_HARD_BLOCK) > 0.0 and float(pv) >= float(PORTFOLIO_VOL_HARD_BLOCK):` (threshold echoed at engine/risk/portfolio_risk_engine.py:3182).
   - Net effect: with the default 0.0, portfolio realized vol only *scales* exposure (vol targeting), it can never *block*. An operator must explicitly set `PORTFOLIO_RISK_VOL_HARD_BLOCK` for the hard block to bind. This is a deliberate soft-only default but it is undocumented, so an operator may wrongly assume a vol ceiling hard-stops the book.

2. A `None` trade-suppression result is mapped to the most permissive state.
   - engine/execution/execution_policy_engine.py:737-745 — `apply_execution_policy` calls `resolved_trade_suppression(...) or {"state": "NONE", "action": "NONE", "size_mult": 1.0, "throttle_mult": 1.0, "hard_block": False}`.
   - `evaluate_trade_suppression` (engine/execution/trade_suppression_engine.py:417) is annotated to return `Dict[str, Any]`, but if it (or an injected `trade_suppression_fn`) ever returns `None`/empty, execution proceeds as fully unsuppressed (NONE, no throttle, no block). In live this is the wrong direction: an absent/failed suppression evaluation should be treated conservatively.
   - The execution mode is available in this scope via `get_execution_mode` (imported at engine/execution/execution_policy_engine.py:39 as `read_execution_mode`, called at execution_policy_engine.py:754 and :1558) and via the `mode` parameter (apply_execution_policy signature, execution_policy_engine.py:640).

Low blast radius: gross/net hard caps (engine/risk/portfolio_risk_engine.py:118-119) and drawdown hard-block (engine/risk/portfolio_risk_engine.py:124) still bind regardless. This is a clarity + fail-closed-direction fix, not a new control.

DESIGN / REQUIRED CHANGE (read-then-change; implement exactly):

A. Document the soft-only portfolio-vol default.
   - In docs/README_OPERATOR_GUIDE.md, add a subsection under the risk/portfolio-vol material titled "Portfolio vol: soft-only by default". State precisely: `PORTFOLIO_RISK_VOL_HARD_BLOCK` defaults to `0.0`, which DISABLES the portfolio-vol hard block; with the default, realized portfolio vol only scales exposure via vol targeting (`PORTFOLIO_RISK_VOL_TARGET`, default 0.020) and never blocks a batch. Document that to make portfolio vol a hard stop the operator must set `PORTFOLIO_RISK_VOL_HARD_BLOCK` to a positive annualized/realized-vol threshold (cite engine/risk/portfolio_risk_engine.py:2904 semantics: block fires when `pv >= threshold`). Note the gross/net caps and `PORTFOLIO_RISK_DD_HARD_BLOCK` remain the binding hard stops at the 0.0 default.
   - Add an inline clarifying comment at engine/risk/portfolio_risk_engine.py:130 explaining "0.0 = intentional soft-only default; vol targeting still scales; set >0 to make portfolio vol a hard stop" (keep the existing `# 0 disables` intent, expand it).

B. Make the None-suppression fallback conservative in live.
   - In engine/execution/execution_policy_engine.py, in `apply_execution_policy`, replace the inline `or {...permissive...}` default (execution_policy_engine.py:745) with an explicit branch on the resolved suppression result:
     - Add a module-level helper `_default_suppression_for_mode(mode: str, execution_mode: str) -> Dict[str, Any]` (place near other private helpers).
     - When `resolved_trade_suppression(...)` returns a falsy/`None` value: resolve effective live-ness using BOTH the `mode` argument and the canonical `get_execution_mode()` call (an order is treated as live if either string equals "live", case-insensitive).
       - If live: return a conservative SOFT_THROTTLE default `{"state": "SOFT_THROTTLE", "action": "SOFT_THROTTLE", "size_mult": <SOFT_MULT>, "throttle_mult": <SOFT_MULT>, "hard_block": False, "reason": "suppression_eval_none_conservative_default"}` where `<SOFT_MULT>` is read from a new env var `EXECUTION_NONE_SUPPRESSION_SOFT_MULT` (float, default `0.5`, clamped to (0.0, 1.0]). Do NOT hard_block (avoid a silent full halt), but DO throttle and emit a non-fatal warning via the existing `_warn_nonfatal` pattern with code `EXECUTION_POLICY_ENGINE_SUPPRESSION_NONE_LIVE` so the condition is observable.
       - If not live (paper/shadow/unknown): preserve current permissive behavior — return the existing NONE default `{"state": "NONE", "action": "NONE", "size_mult": 1.0, "throttle_mult": 1.0, "hard_block": False}` (no behavior change off-live, no test breakage).
   - Keep the downstream `tse.get("hard_block")` and existing `tse`-consuming logic (execution_policy_engine.py:747+) unchanged; only the construction of `tse` when the evaluator returns falsy changes.
   - Do NOT alter `evaluate_trade_suppression` itself; the contract is that a well-formed call returns a dict — this only hardens the defensive fallback.

C. Test.
   - Add/extend a test (e.g. tests/execution/test_execution_policy_suppression.py or the existing execution-policy test module) with two cases using `trade_suppression_fn=lambda **kw: None`:
     1. `mode="live"` (or `get_execution_mode` patched to "live") -> asserts the resolved suppression state is SOFT_THROTTLE with `size_mult == EXECUTION_NONE_SUPPRESSION_SOFT_MULT` default 0.5 and `hard_block is False`; assert the warning code is emitted.
     2. `mode="paper"`/"shadow" with non-live execution mode -> asserts state stays NONE and `size_mult == 1.0` (unchanged permissive path).

GUARDRAIL: preserve all existing fail-closed/safe-mode gates; never enable live execution; read-then-change. Do not weaken gross/net caps, drawdown hard-block, kill-switch, or `hard_block` handling. The live fallback must only THROTTLE, never silently widen exposure.

VERIFY (exact checks that prove done):
1. `grep -n "intentional soft-only" engine/risk/portfolio_risk_engine.py` returns the expanded comment at the `PORTFOLIO_VOL_HARD_BLOCK` definition, and `grep -ni "soft-only" docs/README_OPERATOR_GUIDE.md` returns the new operator-guide subsection naming `PORTFOLIO_RISK_VOL_HARD_BLOCK`.
2. `grep -n "_default_suppression_for_mode\|EXECUTION_NONE_SUPPRESSION_SOFT_MULT\|EXECUTION_POLICY_ENGINE_SUPPRESSION_NONE_LIVE" engine/execution/execution_policy_engine.py` shows the helper, env var, and warning code; the bare `or {"state": "NONE", ...}` literal at the call site (formerly execution_policy_engine.py:745) is gone.
3. `python -c "import ast,sys; ast.parse(open('engine/execution/execution_policy_engine.py').read()); ast.parse(open('engine/risk/portfolio_risk_engine.py').read())"` exits 0.
4. The new tests pass: `pytest -q tests/execution/test_execution_policy_suppression.py` (or the module you extended) is green, including both the live SOFT_THROTTLE case and the non-live NONE case.
5. Existing execution-policy tests still pass (no off-live behavior change): run the relevant `pytest -q` execution suite green.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

### PL-3 — Tighten restore-drill cadence and offer FULL spool durability (P3)

ROLE: You are a senior reliability/backup engineer hardening restore-drill cadence and spool durability for the trading system.

TARGET: /home/david/gitsandbox/system/system

PROBLEM (re-confirmed at HEAD 8184174, branch codex/worktree-production-readiness):
Two related durability weaknesses make recovery evidence and last-write durability looser than the rest of the production posture.

1) Restore-drill reuse window is 90 days, and the installed drill timer fires only monthly — far looser than the WAL/RPO posture and looser than the cited ~9.5-day-old live evidence implies.
   - ops/backup/backup_restore_evidence.sh:37
     `restore_reuse_max_s="${TS_BACKUP_EVIDENCE_REUSE_RESTORE_MAX_AGE_S:-${BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S:-7776000}}"` — default 7776000 s = 90 days. A passing restore-drill report is reused as valid live evidence for up to 90 days (enforced at ops/backup/backup_restore_evidence.sh:1139 `fresh_enough "$latest_report" "$restore_reuse_max_s"`; on staleness it returns 1 and writes `restore_drill_max_age_s` / `restore_drill_required_timer=trading-restore-drill.timer` to restore_drill.out around lines 1140-1165).
   - The drill cadence is `OnCalendar=monthly` in ops/server/systemd/trading-restore-drill.timer:5 (~30 days). With reuse at 90 days, evidence can legitimately age ~3x the drill interval, and a monthly drill cannot keep evidence inside a 7-14 day window.
   - check_systemd() in ops/backup/backup_restore_evidence.sh:726 already asserts trading-restore-drill.service and trading-restore-drill.timer are present (units list lines 736-737), enabled, and active (timers list line 743; is-enabled check ~line 769, is-active check ~line 778); failure sets overall_rc=1 at line 1690. So the assertion exists — but the timer that is installed/enabled (ops/server/bootstrap.sh:1221-1222 install, 1238-1247 `systemctl enable --now`) runs only monthly, and deploy/systemd/ has NO restore-drill timer at all (only ops/server/systemd/ does), so the deploy/ path ships none.
   - deploy/env/trading.env.example:229 `BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S=` is blank, so operators inherit the 90-day default silently.

2) Spool last-write durability defaults to SQLite synchronous=NORMAL, which can lose the last spooled transaction on a hard OS/power crash (market-data only, but recovery evidence should let operators opt into FULL).
   - engine/runtime/async_writer.py:77 `spool_synchronous: str = "NORMAL"`; engine/runtime/async_writer.py:108 reads `ASYNC_PRICE_WRITER_SPOOL_SYNCHRONOUS` defaulting to NORMAL and uppercases it; passed to the spool at engine/runtime/async_writer.py:132.
   - engine/runtime/async_writer_spool.py:115 ctor param `synchronous: str = "NORMAL"`; lines 121-123 normalize and accept only {"FULL","NORMAL","EXTRA"} (anything else falls back to NORMAL). So FULL/EXTRA are already supported but undocumented and unoffered.
   - deploy/env/trading.env.example:159 `ASYNC_PRICE_WRITER_SPOOL_SYNCHRONOUS=` is blank with no guidance on the durability/throughput tradeoff.

DESIGN / REQUIRED CHANGE (optimal, implement without re-discovery):

A. Tighten the restore-drill reuse default to 14 days (1209600 s) and keep it env-overridable.
   - In ops/backup/backup_restore_evidence.sh:37 change the innermost default from `7776000` to `1209600` (14 days). Preserve the exact override precedence: `TS_BACKUP_EVIDENCE_REUSE_RESTORE_MAX_AGE_S` then `BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S` then the new default. Do not change any other reuse window (base_reuse_max_s line 36 stays).
   - Set the documented production value in deploy/env/trading.env.example:229 to `BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S=604800` (7 days) with a comment stating the accepted range is 7-14 days and that staleness past this window fails backup evidence.

B. Tighten the drill cadence so evidence cannot age past the reuse window, in BOTH the ops/server and deploy/ unit sources.
   - Edit ops/server/systemd/trading-restore-drill.timer:5 from `OnCalendar=monthly` to a weekly cadence: `OnCalendar=weekly` plus a randomized delay to avoid contending with other backup timers (add `RandomizedDelaySec=3600`). Keep `Persistent=true` and the `[Install] WantedBy=timers.target`. A weekly drill keeps reused evidence comfortably inside both the 7-day documented value and the 14-day hard default.
   - Create deploy/systemd/trading-restore-drill.service and deploy/systemd/trading-restore-drill.timer if and only if the deploy/ install path is meant to ship these units; model them on the existing ops/server/systemd equivalents (same Unit=, same SyslogIdentifier, same weekly OnCalendar). If deploy/systemd/ is intentionally a thinner unit set (it currently ships only trading-backup.{service,timer}, trading-engine, trading-operator, trading-upgrade), then instead document in deploy/README.md / deploy/PRODUCTION_FILE_MANIFEST.md that ops/server/bootstrap.sh is the authoritative installer of restore-drill units and that the deploy/systemd path must not be used alone for production backups. Pick exactly one of these two and state which in your write-up; do not leave the deploy/ path silently missing the timer that check_systemd asserts.

C. Offer FULL spool durability as a documented, supported live option (do not change the default; NORMAL stays the default to protect throughput).
   - Do NOT change the code default at engine/runtime/async_writer.py:77 or :108. FULL/EXTRA are already accepted at engine/runtime/async_writer_spool.py:121-123; no code change is required there.
   - In deploy/env/trading.env.example:159 keep the variable but add a comment block documenting: accepted values FULL | NORMAL (default) | EXTRA; NORMAL is the default and is safe against process crashes but can lose the most recent spooled txn on a hard OS/power loss; FULL fsyncs every spool commit and eliminates that last-txn loss at a measurable write-throughput cost; this affects market-data spool only (never order/ledger/risk/capital/audit writes). Recommend FULL for power-unreliable hosts.
   - Add the same FULL-vs-NORMAL tradeoff note to the durability/backup documentation (docs/OBSERVABILITY.md and/or docs/RUNTIME_STATE.md and docs/REFERENCE_CONFIGURATION_GLOSSARY.md, wherever ASYNC_PRICE_WRITER_SPOOL_SYNCHRONOUS / BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S are already referenced — confirm with grep) so the env var, accepted values, default, and tradeoff are discoverable in one place.

D. Add a focused test/validator that locks the new posture in:
   - A test asserting backup_restore_evidence.sh's effective default reuse window is 1209600 s when neither override env var is set (e.g. source-or-grep the literal, or run the gate with both env vars unset and assert the `restore_drill_max_age_s=1209600` line on a deliberately stale report).
   - A test asserting the restore-drill timer ships a sub-monthly cadence (grep `OnCalendar=weekly` in the shipped timer source(s)) and that the deploy path is consistent with decision (B) — either the deploy/ timer exists with weekly cadence, or the manifest/README documents bootstrap.sh as authoritative.
   - A test asserting deploy/env/trading.env.example sets BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S within 604800-1209600 and documents ASYNC_PRICE_WRITER_SPOOL_SYNCHRONOUS values FULL/NORMAL/EXTRA.

GUARDRAIL: preserve all existing fail-closed/safe-mode gates; never enable live execution; read-then-change. Do not weaken any existing backup-evidence assertion (check_systemd must still fail-closed when the restore-drill unit/timer is missing, disabled, or inactive), do not change base-backup or WAL reuse windows, and keep NORMAL the spool default (FULL is opt-in only).

VERIFY (exact checks that prove done):
1. `grep -n 'restore_reuse_max_s=' ops/backup/backup_restore_evidence.sh` shows the innermost default is `1209600` and the override chain `TS_BACKUP_EVIDENCE_REUSE_RESTORE_MAX_AGE_S` -> `BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S` -> default is intact.
2. `grep -n 'OnCalendar' ops/server/systemd/trading-restore-drill.timer` (and deploy/systemd/trading-restore-drill.timer if created) shows `weekly`, not `monthly`; `RandomizedDelaySec` present.
3. `grep -n 'BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S' deploy/env/trading.env.example` shows a value in [604800,1209600] with the 7-14 day comment; `grep -n 'ASYNC_PRICE_WRITER_SPOOL_SYNCHRONOUS' deploy/env/trading.env.example` shows the FULL|NORMAL|EXTRA tradeoff comment with NORMAL as default.
4. A run of backup_restore_evidence.sh against a stale-by-design report with no reuse env vars set writes `restore_drill_max_age_s=1209600` and returns the staleness failure, proving the new default is live; with `BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S` set the override still wins.
5. The new tests pass; `bash -n ops/backup/backup_restore_evidence.sh` and `systemd-analyze verify` (or a syntax check) on the changed timer pass.
6. Run the changed-behavior tests, then `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators; capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

### PL-4 — Trace logging must not default ok=True on a missing health sub-key (P3)

ROLE: You are a senior reliability engineer hardening the health-snapshot debug trace so it cannot report fake-green sections.

TARGET: /home/david/gitsandbox/system/system

PROBLEM
The optional HEALTH_SNAPSHOT_TRACE debug line (emitted by `engine/runtime/health_normalization.py:trace_section` when enabled, gated through `engine/runtime/health.py:_trace_section` at line 155-164) derives its per-section `ok=` flag by reading the section payload's `ok` sub-key with a default of `True` when the key is absent. If a section dict is ever produced without an explicit `ok` key (future refactor, partial/short-circuit construction, or an exception path that does not set it), the trace would log a GREEN section that was never actually computed as healthy — i.e. fake-green in operator/debug logs.

Re-confirmed at HEAD 8184174 (branch codex/worktree-production-readiness), the offending `.get("ok", True)` defaults appear ONLY in `_trace_section` calls in `engine/runtime/health.py`:
- line 2746: `_check_memory_pressure` → `ok=bool((out.get("memory_pressure") or {}).get("ok", True))`
- line 2769: `_check_effective_runtime_state` → `ok=bool((out.get("effective_runtime_state") or {}).get("ok", True))`
- line 2837: `_check_storage_wal_guards` → `ok=bool((out.get("storage_wal_guards") or {}).get("ok", True))`
- line 3819: `_check_model_serving` (model_serving) → `ok=bool((out.get("model_serving") or {}).get("ok", True))`
- line 3839: `_check_alert_lifecycle` (alert_lifecycle) → `ok=bool((out.get("alert_lifecycle") or {}).get("ok", True))`

SCOPE NOTE (do not over-reach): This is trace-only. The PERSISTED payloads already set an explicit `ok` everywhere (e.g. storage_wal_guards at health.py:2814 and its except path at 2827; effective_runtime_state at 2761; memory_pressure error path at 2739-2742). The aggregate `/api/health.ok` gate is computed from blockers/requireds elsewhere (e.g. health.py:4518-4534) and MUST NOT be touched. All other `_trace_section` calls already use a falsy default (`.get("ok")` with no second arg) and are correct — leave them alone. Do NOT change any `.get("ok", True)` reads OUTSIDE `_trace_section` argument expressions (e.g. lines 813, 1839, 2651, 4518-4534, 4714, 5549 are real gate logic and are out of scope).

DESIGN / REQUIRED CHANGE
Make the trace `ok=` flag falsy when the section's `ok` sub-key is absent, so the debug line can never show green for an uncomputed/missing section, without altering any persisted value or gate.

1. In `engine/runtime/health.py`, change ONLY the five `_trace_section` `ok=` argument expressions listed above from `.get("ok", True)` to `.get("ok", False)`. Concretely, for each of memory_pressure (2746), effective_runtime_state (2769), storage_wal_guards (2837), model_serving (3819), and alert_lifecycle (3839):
   - `ok=bool((out.get("<section>") or {}).get("ok", True))` → `ok=bool((out.get("<section>") or {}).get("ok", False))`
   Use exact, unique string replacements per line; do not touch surrounding code or the persisted `out[...]` dicts.
2. Do NOT change `engine/runtime/health_normalization.py:trace_section`; it is provider-agnostic plumbing and already correct. Do NOT add or rename any env var (HEALTH_SNAPSHOT_TRACE stays as-is).
3. Rationale to encode in a one-line code comment above each changed call is OPTIONAL and discouraged unless it aids reviewers; prefer a single short comment in `_check_memory_pressure` only, e.g. `# trace-only: default ok=False so missing sub-key never logs fake-green`.

GUARDRAIL
Preserve all existing fail-closed/safe-mode gates; never enable live execution; read-then-change. This change is strictly trace/debug output and MUST NOT alter any persisted health payload, any `required`/`blockers` logic, or the aggregate `/api/health.ok` result.

VERIFY
1. `cd /home/david/gitsandbox/system/system && grep -n '\.get("ok", True)' engine/runtime/health.py` returns NO matches that are inside a `_trace_section(...)` call — confirm none of lines ~2746/2769/2837/3819/3839 still carry `, True)`; the remaining real-gate matches (813, 1839, 2651, 4518-4534, 4714, 5549) are unchanged.
2. `grep -n '\.get("ok", False)' engine/runtime/health.py` shows exactly the five edited trace calls now defaulting to False.
3. Add/extend a unit test (e.g. `tests/test_health_trace.py` or the nearest existing health-trace test) that, with `HEALTH_SNAPSHOT_TRACE` enabled, calls `_trace_section("memory_pressure", time.perf_counter())` after seeding `out["memory_pressure"] = {}` (no `ok` key) and asserts the emitted trace payload has `ok is False`; and with `out["memory_pressure"] = {"ok": True}` asserts `ok is True`. Capture the trace via the `logger`/`extra_json` payload.
4. `python -m pytest tests/ -k "health and trace" -q` passes; `python -c "import ast,sys; ast.parse(open('engine/runtime/health.py').read())"` succeeds.
5. Confirm `/api/health` behavior is unchanged: a snapshot run with all sections healthy still yields the same aggregate `ok` as before (run the existing health-snapshot test that exercises `_health_snapshot`/aggregate ok, e.g. the suite under `tests/` covering health.ok, and confirm it stays green).

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

### PL-5 — Unify effective-runtime redaction through engine.api.redaction and reject query-string tokens on non-loopback binds (P3)

ROLE: You are a senior platform-security engineer hardening the trading runtime's secret-redaction and dashboard-auth surfaces.

TARGET: /home/david/gitsandbox/system/system

PROBLEM (re-confirmed at HEAD 81841749eee4149ef0791d751e61059e1d5ae8b0, branch codex/worktree-production-readiness):

Two independent issues, both about a defense surface that has drifted from its canonical implementation.

(a) Divergent / weaker redaction in effective_runtime_state.py.
- engine/runtime/effective_runtime_state.py:25-29 defines a LOCAL, narrower secret matcher: `_SENSITIVE_KEY_RE = re.compile(r"(password|passwd|secret|token|api[_-]?key|access[_-]?key)", re.IGNORECASE)` and `_SECRET_KV_RE` covering the same small set.
- engine/runtime/effective_runtime_state.py:148-163 `_redact_string(value)` does ad-hoc URL-userinfo masking plus `_SECRET_KV_RE.sub(...)`.
- engine/runtime/effective_runtime_state.py:166-180 `redact_evidence(value, *, key="")` recurses dicts/lists and at :177 keys only on the narrow `_SENSITIVE_KEY_RE`, else calls `_redact_string`.
- Callers: engine/runtime/effective_runtime_state.py:423 `state["actual"] = redact_evidence(actual)`, :706 the CLI prints `redact_evidence(snapshot)`, and :270 `_redact_string(...)` on subprocess stderr.
- The CANONICAL module engine/api/redaction.py is strictly broader: `is_sensitive_key` (:133-135) matches exact keys + suffixes including master_key/hmac_key/private_key/session_token/refresh_token/client_secret/key_id/authorization/credentials (_SENSITIVE_KEY_EXACT :13-37, _SENSITIVE_KEY_SUFFIXES :38-61); `redact_string` (:185-206) handles DSN userinfo, Authorization headers, JSON-string secrets, key=value secrets, and known-sensitive-value substitution; `redact_api_payload` (:209-248) also redacts DSN/identifier keys (account numbers, broker order ids). The local copy in effective_runtime_state.py misses all of master_key/hmac_key/private_key/session_token/refresh_token/client_secret/authorization/account-id/DSN-key handling, so runtime evidence snapshots can leak secrets the canonical API path would mask. The two surfaces can silently drift further.

(b) Query-string token accepted on non-loopback binds in shadow/paper.
- engine/api/http_transport.py:1411-1430 `_request_api_token_parts` prefers the `X-API-Token` header (:1413-1418) but falls back to a `?token=` query parameter (:1420-1429), returning source `"query"`.
- engine/api/http_transport.py:1481-1516 `_require_protected_route_auth` only rejects the query source when `strict_mutation_auth_reasons()` is non-empty: at :1504-1511 `if source == "query" and strict_reasons:` returns `query_token_forbidden`. In shadow/paper modes `strict_reasons` is empty, so a `?token=` is accepted even when the server is bound to a non-loopback interface (LAN mode). Query strings leak into proxy/access logs, browser history, and Referer headers.
- The server already knows it is bound non-loopback via engine/api/http_transport.py:1468-1473 `_remote_bind_reasons()` (uses `_is_loopback_bind_host` :1456-1466 over `_server_bind_host_candidates` :1436-1454).
- The browser client boot/operator_ui.html:1249-1257 reads `?token=`/`?dashboard_token=`/`?operator_token=` from the URL; boot/operator_ui.html:1348 already notes query-token auth is "server-side only for backward-compatible tools." Header-based auth (X-API-Token) is the preferred path.

GUARDRAIL: preserve all existing fail-closed/safe-mode gates; never enable live execution; read-then-change. Do not weaken any existing rejection (the strict-mode `query_token_forbidden` at :1505-1511 must remain at least as strict). Do not print or log secret values. Do not change the WebSocket subprotocol token path (operator_ui.html:1289-1296/1358) — that is a separate transport.

DESIGN / REQUIRED CHANGE:

1) Route effective_runtime_state redaction through the canonical module.
   - In engine/runtime/effective_runtime_state.py, import from engine.api.redaction: `redact_api_payload`, `redact_string`, `is_sensitive_key` (the module is import-safe — confirm no circular import; if engine.api.redaction is unexpectedly heavy, import lazily inside the functions).
   - Replace the body of `redact_evidence(value, *, key="")` (:166-180) so it delegates to `redact_api_payload(value, key=key)`. Keep the public name and signature `redact_evidence` and the `__all__` entry (:728) intact so callers at :423 and :706 are unchanged. Pass through `redact_api_payload`'s default `redact_identifiers=True` so account/broker-order identifiers in runtime evidence are also masked (this is strictly more redaction, never less).
   - Replace `_redact_string(value)` (:148-163) so it delegates to `redact_string(value)` (canonical). Keep the name so the subprocess-stderr caller at :270 is unchanged.
   - DELETE the now-unused local `_SENSITIVE_KEY_RE` (:25) and `_SECRET_KV_RE` (:26-29) and the local `urlsplit/urlunsplit` import on :20 if and only if nothing else in the file references them (grep first; :270 only calls `_redact_string`). Keep `re` if still used elsewhere.
   - The redaction-marker format will change from the literal `<redacted>` to the canonical `<redacted:...>` / `<redacted-id:...>` markers — that is intended; update/align any test asserting the old literal (see VERIFY).

2) Reject query-string tokens whenever the server binds non-loopback (any mode), preferring the header.
   - In engine/api/http_transport.py `_require_protected_route_auth` (:1481-1516), broaden the existing query rejection. After computing `supplied, source = self._request_api_token_parts()` (:1504), reject a query-sourced token when EITHER strict reasons are present OR the bind is non-loopback:
       `bind_reasons = self._remote_bind_reasons()`
       `if source == "query" and (strict_reasons or bind_reasons): return {"ok": False, "error": "query_token_forbidden", "reason": "query_string_token_authentication_rejected_on_non_loopback_bind", "meta": {"status": 401}}`
     Keep the strict-mode reason string behavior backward-compatible: when `strict_reasons` is the trigger, emit the existing `query_string_token_authentication_disabled_in_production_live` reason; when only `bind_reasons` triggers, emit the new non-loopback reason. (Implement as a single branch that picks the reason based on which condition fired.)
   - Loopback binds with no strict reasons (pure local dev) must STILL accept `?token=` so existing local tooling keeps working — do not change that path.
   - Do NOT modify boot/operator_ui.html token discovery for the header path; it already prefers header/explicit params. Optionally (only if trivial and non-breaking) add a one-line comment near operator_ui.html:1348 noting query-token auth is rejected on non-loopback binds. No behavioral JS change required.

3) New regression tests.
   - Redaction unification: add a test (e.g. tests/runtime/test_effective_runtime_state_redaction.py or extend the nearest existing effective_runtime_state test) asserting that `redact_evidence({"master_key": "abc123", "session_token": "xyz789", "pg_dsn": "postgresql://u:p@h:5432/db", "broker_account_number": "U1234567"})` masks every secret/identifier value (no plaintext `abc123`/`xyz789`/`p@h`/`U1234567` in the JSON-serialized output) — values the OLD narrow matcher would have leaked. Assert the canonical marker prefix `<redacted` appears.
   - Query-token rejection: add/extend a test under tests/ for http_transport auth asserting that with a configured dashboard token, `strict_mutation_auth_reasons()` empty, and a non-loopback bind (`_remote_bind_reasons()` returns a reason), a request carrying only `?token=` is rejected with `error == "query_token_forbidden"` and status 401; and that the SAME request via the `X-API-Token` header succeeds; and that a loopback bind with no strict reasons still accepts `?token=`. Reuse the existing http_transport test harness/fixtures rather than inventing a new server.

VERIFY (all must pass; quote results):
- `cd /home/david/gitsandbox/system/system && grep -n "_SENSITIVE_KEY_RE\|_SECRET_KV_RE" engine/runtime/effective_runtime_state.py` returns no matches (local matchers removed) and `grep -n "from engine.api.redaction import\|engine.api.redaction" engine/runtime/effective_runtime_state.py` shows the canonical import.
- `python -c "from engine.runtime.effective_runtime_state import redact_evidence; import json; print(json.dumps(redact_evidence({'master_key':'abc123','session_token':'xyz789','pg_dsn':'postgresql://u:p@h:5432/db','broker_account_number':'U1234567'})))"` prints output containing none of `abc123`, `xyz789`, `:p@h`, `U1234567` and contains `<redacted`.
- `grep -n "query_string_token_authentication_rejected_on_non_loopback_bind\|_remote_bind_reasons()" engine/api/http_transport.py` shows the new rejection wired in `_require_protected_route_auth`, and the existing strict-mode branch/reason at the former :1505-1511 is still present (not removed).
- New tests pass: `python -m pytest tests/ -k "effective_runtime_state_redaction or http_transport" -q` (use the actual node ids of the tests you add/extend) is green, and the full file's existing tests are unbroken.
- `git grep -n "<redacted>" tests engine/runtime/effective_runtime_state.py` shows no test still asserting the old literal `<redacted>` marker against effective_runtime_state output (update any that did).

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

### PL-6 — prod_preflight: detect live venv vs requirements.lock.txt dependency drift (P3)

ROLE: You are a senior runtime-reliability engineer hardening the production preflight gate.

TARGET: /home/david/gitsandbox/system/system

PROBLEM
The repo gates dependency *manifests* statically and gates *installs* by hash, but nothing verifies that the live interpreter actually has the locked versions installed. A venv can drift after install (manual `pip install`, a partial upgrade, a rebuilt image with a stale layer) and no preflight check would notice.

Evidence re-confirmed at HEAD 8184174 (branch codex/worktree-production-readiness):
- `tools/validate_dependency_lock.py` validates only static files (manifest pins, lock direct-pin coverage, dev/runtime separation). It never imports or inspects the running interpreter. See `RUNTIME_INSTALL_MANIFESTS` at `tools/validate_dependency_lock.py:20-22` (`requirements.txt` -> (`requirements.in`, `requirements.lock.txt`)) and the reusable parsers `_iter_requirement_lines` (line 52), `_requirements_entries` (line 88), `_normalize_req_name` (line 45), `PIN_RE` (line 15).
- `engine/runtime/prod_preflight.py` (2769 lines) runs many gates in `main()` (line 2341) but has NO installed-package check. Each gate returns a `(notes, warnings, errors, state_dict)` tuple, is appended into the `result` dict (initialized lines 2348-2378), and on `errors` prints `[gate]` lines and `return 3`; warnings flow to the final block at lines 2749-2765 where any warning sets `status="warning"` and the process exits 2 (0 = pass, 2 = warnings, 3 = hard fail).
- `grep -rE 'pip freeze|pipdeptree|importlib.metadata' tools/ deploy/ bin/` returns nothing; `prod_preflight.py` has no such check either.
- `requirements.lock.txt` is the uv-generated, fully-pinned (`name==version`) lock; `requirements.txt` pulls direct deps from `requirements.in` constrained by the lock (`requirements.txt:1-6`).
- Prod/live detection convention already in this file: `_runtime_mode_name()` (line 102) returning `"live"` is used to escalate warnings to hard requirements (e.g. line 211). `_env_truthy()` is at line 87.

This complements GO-R5 (hashed installs gate the install) by catching a venv that drifted AFTER install.

DESIGN / REQUIRED CHANGE
Add a new warn-by-default, block-in-live preflight gate that compares the live interpreter's installed versions of the DIRECT requirements against `requirements.lock.txt`.

1. In `engine/runtime/prod_preflight.py`, add a function:
   `def _dependency_drift_gate() -> Tuple[List[str], List[str], List[str], Dict[str, Any]]:`  (returns notes, warnings, errors, state — same shape as peer gates like `_cpu_power_policy_gate`).
   - Resolve repo root from this file's path (mirror how other gates locate repo files; do not hardcode `/home/david`).
   - Determine the set of DIRECT package names = keys of the parsed `requirements.in` (top-level only; do not expand transitive deps). Reuse the parsing logic from `tools/validate_dependency_lock.py` by importing its helpers (`_requirements_entries`, `_normalize_req_name`) — import lazily inside the function and `_warn_nonfatal(...)` + return empty result if the import or file read fails (fail-soft, never crash the preflight). Do NOT duplicate the parser.
   - Parse `requirements.lock.txt` into a `{normalized_name: locked_version}` map. Extract the version with the existing `PIN_RE`/`==` convention; skip lines without an exact `==` pin and skip comment/`-` directive lines.
   - For each DIRECT name, read the installed version via `importlib.metadata.version(dist_name)` (stdlib; handle `importlib.metadata.PackageNotFoundError`). Normalize names with `_normalize_req_name` on both sides before comparison. For extras like `psycopg[binary,pool]` compare on the base distribution name (`psycopg`).
   - Classify each direct dep:
     - installed version == locked version -> ok (add to a counted note, not per-package).
     - installed but != locked -> drift entry `name installed=X locked=Y`.
     - not installed at all -> missing entry `name locked=Y not-installed`.
     - in requirements.in but absent from the lock map -> note as `unlocked_direct:name` (informational; do not fail).
   - Build a state dict: `{"direct_count": int, "ok_count": int, "drift": [...], "missing": [...], "checked": True}`.
   - Decision: if there are drift or missing entries, emit a single human-readable summary string (e.g. `dependency_drift: 2 drifted, 1 missing vs requirements.lock.txt: numpy installed=1.26.0 locked=1.26.4; ...`). Route it to `errors` when `_runtime_mode_name() == "live"` OR `_env_truthy("PROD_PREFLIGHT_REQUIRE_DEPENDENCY_LOCK_MATCH")`; otherwise route it to `warnings`. Add an escape hatch `PROD_PREFLIGHT_SKIP_DEPENDENCY_DRIFT` (`_env_truthy`) that downgrades any decision to a single warning note and never blocks (so an intentionally-divergent host can pass with a visible warning).
   - Add a note line summarizing scope, e.g. `dependency drift check: direct=N ok=M lock=requirements.lock.txt`.

2. Wire it into `main()` alongside the other gates (place it near the config/source-integrity gates, before the smoke-job block). Mirror the exact existing pattern:
   - `dep_notes, dep_warnings, dep_errors, dep_state = _dependency_drift_gate()`
   - `result["steps"].extend(dep_notes)`, `result["warnings"].extend(dep_warnings)`, `result["dependency_drift"] = dict(dep_state or {})`
   - if `dep_errors`: `result["errors"].extend(dep_errors)`, print JSON (if `args.json`) else per-error `print("[dependency-drift]", error)`, `return 3`.
   - Add `"dependency_drift": {}` to the `result` dict initializer (lines 2348-2378) so the key always exists.

3. Keep it dependency-free: use only stdlib (`importlib.metadata`) plus the existing `tools/validate_dependency_lock.py` parsers. Do not shell out to `pip freeze`/`pipdeptree`. Do not add network calls. The gate must complete fast and never raise.

GUARDRAIL: preserve all existing fail-closed/safe-mode gates; never enable live execution; read-then-change.

VERIFY
- `cd /home/david/gitsandbox/system/system && python -c "import ast,sys; ast.parse(open('engine/runtime/prod_preflight.py').read())"` parses clean.
- `python -m engine.runtime.prod_preflight --json` (or the project's standard invocation) emits a top-level `"dependency_drift"` object with `checked`, `direct_count`, `ok_count`, `drift`, `missing` keys, and on a clean matching venv reports zero drift/missing with a `dependency drift check:` step note.
- Negative test: simulate drift (e.g. uninstall or downgrade one direct dep in a throwaway venv, or monkeypatch `importlib.metadata.version`) and confirm the gate adds the drift summary to `warnings` in safe mode (exit 2) and to `errors` (exit 3, `[dependency-drift]` line) when `ENGINE_MODE=live` or `PROD_PREFLIGHT_REQUIRE_DEPENDENCY_LOCK_MATCH=1`; confirm `PROD_PREFLIGHT_SKIP_DEPENDENCY_DRIFT=1` downgrades it to a warning only.
- Add a focused unit test under the existing tests tree (e.g. `tests/runtime/test_prod_preflight_dependency_drift.py`) that calls `_dependency_drift_gate()` with `importlib.metadata.version` and the lock/requirements parsers monkeypatched to cover: all-match (no warn/error), drifted version (warn in safe, error in live), missing package, and the skip env. Run it: `python -m pytest tests/runtime/test_prod_preflight_dependency_drift.py -q` passes.
- `git grep -nE 'importlib.metadata|dependency_drift' engine/runtime/prod_preflight.py` now returns the new gate.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

### PL-7 — Exercise the pgbouncer pooling path in CI (remove the allow-skip) (P3)

ROLE: You are a senior CI/release engineer hardening the production-backend gate so the transaction-pooled pgbouncer path is actually exercised, not silently skipped.

TARGET: /home/david/gitsandbox/system/system

PROBLEM (re-confirmed at HEAD 8184174, branch codex/worktree-production-readiness):
- Production runs the application behind pgbouncer in transaction-pooling mode (ops/server/config/pgbouncer.ini:16 `pool_mode = transaction`, listen_port 6432, per-role pool sizes for ts_ingest/ts_app/ts_reader at lines 44-46). The runtime defaults the app DSN to the pooler on Linux (tests/test_pgbouncer_routing.py:25 asserts `port=6432` in default_pg_dsn, `port=5432` in default_admin_pg_dsn).
- The CI production-backend job provisions only raw Postgres + Redis as services, no pgbouncer: .github/workflows/validate.yml:286-309 (postgres `timescale/timescaledb:latest-pg16` on 5432, redis on 6379). The job env sets TS_PG_DSN to the raw Postgres on 127.0.0.1:5432 (validate.yml:314) and never sets TS_PGBOUNCER_TEST_DSN or TS_PG_DIRECT_TEST_DSN.
- The pgbouncer behavioral tests are therefore skipped, not run. tests/test_pgbouncer_routing.py:63-72 (`test_prepared_statements_work_through_pgbouncer_when_available`) skips with reason "TS_PGBOUNCER_TEST_DSN is not set"; tests/test_pgbouncer_routing.py:75-95 (`test_hundred_clients_multiplex_under_pool_size_when_available`) skips with "TS_PGBOUNCER_TEST_DSN and TS_PG_DIRECT_TEST_DSN are required".
- The skip-detector is told to tolerate these skips: validate.yml:404 passes `--allow-skip-message-regex "TS_PGBOUNCER_TEST_DSN"` to the `full-postgres-redis-suite` invocation of tools/run_required_backend_tests.py. The detector (tools/run_required_backend_tests.py:76-84) renders each skipped testcase as `label: reason` and drops any whose reason matches an allow regex, so these prepared-statement / transaction-pooling regression tests pass the gate without ever executing. The other allow regexes on that call (PySR/Julia at :403, ROCm runtime image at :405, ROCm torch GPU at :406, node at :407-408, dependency-free fallback at :409) cover legitimately optional/host-gated skips and must remain.

Net effect: prepared-statement behavior under transaction pooling (a known psycopg + pgbouncer footgun) and pool-size multiplexing under 100 clients are never validated in the gate, despite production running every app query through pgbouncer.

DESIGN / REQUIRED CHANGE (implement precisely; do not re-discover):
1) Add a pgbouncer service to the `production-backend` job in .github/workflows/validate.yml. Because the existing pgbouncer.ini binds a unix socket and uses scram + an external userlist, do NOT reuse it verbatim for CI; instead run a containerized pgbouncer that listens on TCP and points at the job's `postgres` service. Use a maintained image (e.g. `edoburu/pgbouncer` or `bitnami/pgbouncer`) added under `services:` alongside postgres/redis, configured for:
   - `pool_mode = transaction` (mirror production; this is the property under test),
   - upstream DB host = the `postgres` service, db `trading_ci`, user `ts_app`, password matching validate.yml:292,
   - `ignore_startup_parameters = extra_float_digits,options` (psycopg sets these; without it pgbouncer rejects connections),
   - a `default_pool_size` small enough to make the 100-client multiplex test meaningful (set it to 50 to match the test's default assert ceiling at tests/test_pgbouncer_routing.py:95, or set a smaller pool and export TS_PGBOUNCER_ASSERT_POOL_SIZE accordingly),
   - TCP listen port 6432 published to the runner (`ports: 6432:6432`),
   - a health check that proves the pooler is accepting connections before tests run.
   If a `services:` image cannot accept the required pgbouncer config cleanly, instead add an explicit setup step in the job that `docker run`s pgbouncer with a generated config file (mount or env-templated) on `--network host`, started before the test steps and torn down in an `if: always()` step. Either way, the pooler must reach the `postgres` service and listen on a runner-reachable TCP endpoint.
2) In the `production-backend` job env block (validate.yml:310-338), add:
   - `TS_PGBOUNCER_TEST_DSN`: a TCP DSN to the pooler, e.g. `host=127.0.0.1 port=6432 user=ts_app dbname=trading_ci password=test-app-password` (pragma allowlist secret; this is the CI throwaway password already present at :292/:314/:315 — do not invent a new secret value).
   - `TS_PG_DIRECT_TEST_DSN`: a TCP DSN straight to Postgres on 5432 (reuse the existing `TS_PG_DSN` value or define it as the same `host=127.0.0.1 port=5432 ...` string) so `test_hundred_clients_multiplex_under_pool_size_when_available` can read `pg_stat_activity` directly while clients connect through the pooler.
   - If you chose a pgbouncer `default_pool_size` other than 50, also set `TS_PGBOUNCER_ASSERT_POOL_SIZE` to that value so the assertion at tests/test_pgbouncer_routing.py:95 stays correct.
3) Remove the line `--allow-skip-message-regex "TS_PGBOUNCER_TEST_DSN"` from the `full-postgres-redis-suite` step (currently validate.yml:404). Leave every other allow regex on that invocation intact (:403, :405, :406, :407, :408, :409). After removal, if either pgbouncer test skips for any reason, the skip-detector in tools/run_required_backend_tests.py:192-196 must fail the job.
4) Ensure the pgbouncer service/step is healthy before the test steps execute. Extend the existing "Verify provisioned Postgres and Redis" step (validate.yml:353-365) to also open a psycopg connection to `TS_PGBOUNCER_TEST_DSN` (with `prepare_threshold=1`) and print a `pgbouncer_select_1=` line, so a misconfigured pooler fails fast with a clear message rather than as an opaque test skip/error.
5) Do not change tools/run_required_backend_tests.py logic or the test file assertions; the tests are correct and should now run. Do not change the production pgbouncer.ini.

GUARDRAIL: preserve all existing fail-closed/safe-mode gates; never enable live execution; read-then-change. Keep APP_ENV=test, ENGINE_MODE/EXECUTION_MODE/OPERATOR_MODE=safe, PROD_LOCK=0, KILL_SWITCH_GLOBAL=0 and all other safety env in the job unchanged. Keep all non-pgbouncer allow-skip regexes (PySR/Julia, ROCm runtime image, ROCm torch GPU, node, dependency-free fallback). Use only the existing CI throwaway DB password; never print or hardcode any real secret.

VERIFY:
- `python tools/run_required_backend_tests.py --label full-postgres-redis-suite --junitxml "$RUNNER_TEMP/pytest-full-postgres-redis.xml" --min-selected 2400 <remaining allow regexes> -- -q tests/ -rs` runs in the job and the gate passes. Inspect the produced JUnit XML (`pytest-full-postgres-redis.xml`): the two testcases `tests/test_pgbouncer_routing.py::test_prepared_statements_work_through_pgbouncer_when_available` and `::test_hundred_clients_multiplex_under_pool_size_when_available` must appear with NO `<skipped>` child element (they ran and passed). Confirm there is no remaining `TS_PGBOUNCER_TEST_DSN` allow regex in .github/workflows/validate.yml (`grep -n "TS_PGBOUNCER_TEST_DSN" .github/workflows/validate.yml` shows only the env assignment, not an allow-skip flag).
- Negative check: temporarily unset TS_PGBOUNCER_TEST_DSN (locally or in a scratch run) and confirm tools/run_required_backend_tests.py now FAILS the full-suite step with "selected tests skipped unexpectedly" listing both pgbouncer tests — proving the skip is no longer tolerated.
- Run targeted: `TS_PGBOUNCER_TEST_DSN=... TS_PG_DIRECT_TEST_DSN=... python -m pytest -q tests/test_pgbouncer_routing.py -rs` against a real local pgbouncer and confirm 0 skipped, all passed. Then run `git status --short --untracked-files=all` and `python tools/git_worktree_triage.py`, capturing exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

### PL-8 — Flag env-default MODEL_NAME as a no-champion fallback in live serving (P3)

ROLE: You are a senior production-readiness engineer hardening live-serving governance/observability.

TARGET: /home/david/gitsandbox/system/system

PROBLEM
Live model resolution can silently serve the env-default `MODEL_NAME` when NO champion exists (no competition winner, no per-symbol assignment, no registry champion) without emitting any "no-champion fallback" signal. Confirmed at HEAD 8184174 (branch codex/worktree-production-readiness):

- engine/strategy/predictor.py:459 always appends `MODEL_NAME` as a candidate with source `"env_default"`: `_append_candidate(MODEL_NAME, "env_default")`. The competition/assignment/registry candidates are appended at lines 440 (`assignment`) and 451 (`registry`).
- predictor.py:461-462 picks `requested_model_name` as the first deduped candidate, and 463-472 resolves `resolved_model_name` via `resolve_active_model_name(...)`, defaulting back to `MODEL_NAME` when empty (472).
- predictor.py:474-480 computes `resolution_source`: it is `"env_default"` when `resolved_model_name == MODEL_NAME` and nothing else matched (480).
- predictor.py:482-493 sets `serve_fallback_active` ONLY when `requested_model_name != resolved_model_name` (483-487). When there is no champion of any kind, both collapse to `MODEL_NAME`, so `requested == resolved`, `serve_fallback_active` stays `False`, and `fallback_reason` stays `""`.
- engine/runtime/live_ai_safety.py:627-628 (`live_model_serving_snapshot`) blocks live serving only on `serve_fallback_active` (-> blocker `live_model_resolution_fallback`) or a missing model name (630-631) / missing artifact (637-638) / failed feature contract (642-643).

Net effect: with artifact SHA/path integrity already enforced, an env-default `MODEL_NAME` with NO governed champion behind it serves live and is indistinguishable in the snapshot from a properly promoted champion. This is an observability/governance gap (silent ungoverned fallback), not an integrity break.

DESIGN / REQUIRED CHANGE
Surface "env-default with no governed champion" as a distinct, observable fallback flag, and gate it for live serving behind an explicit opt-in env var (fail-closed by default).

1) engine/strategy/predictor.py `_live_model_resolution` (the function returning the dict at lines 495-503):
   - Compute a new boolean `env_default_fallback`: True when `resolution_source == "env_default"` AND no governed candidate (source in {"competition", "assignment", "registry"} — use whatever exact source strings `_append_candidate` records; re-confirm by reading the candidate-append sites) resolved to `resolved_model_name`. Concretely: `env_default_fallback = (resolution_source == "env_default")` after the existing `resolution_source` computation at 474-480, i.e. there was no governed candidate that matched the resolved name.
   - When `env_default_fallback` is True and `serve_fallback_active` is currently False, set `fallback_reason = "no_governed_champion_env_default"` (do NOT overwrite an existing competition-vs-resolved `fallback_reason`).
   - Add two new keys to the returned dict (495-503): `"env_default_fallback": bool(env_default_fallback)` and keep `"resolution_source"` as-is. Do NOT change `serve_fallback_active` semantics in predictor (keep its existing requested!=resolved meaning) — the new flag is separate so existing consumers are unaffected.

2) engine/runtime/live_ai_safety.py `live_model_serving_snapshot` (def at 572, loop body 608-646):
   - After the existing `serve_fallback_active` check at 627-628, add: read `bool(resolution.get("env_default_fallback"))`. Define a module-level/inline gate using the existing helper `_env_truthy` (defined at line 72): `allow_env_default = _env_truthy("LIVE_ALLOW_ENV_DEFAULT_MODEL", False)`.
   - If `env_default_fallback` is True:
       - Always set `probe["env_default_fallback"] = True`.
       - If NOT `allow_env_default`: append blocker `"live_model_no_governed_champion"` and set `probe["ok"] = False` (fail-closed: ungoverned env-default does not serve live unless explicitly allowed).
       - If `allow_env_default`: do NOT block, but still record the flag on the probe so it is visible in the snapshot.
   - Include the resolved gate value in the returned snapshot for auditability: add `"allow_env_default_model": bool(allow_env_default)` to the final return dict at 649-657.
   - Keep `blockers = _dedupe(blockers)` (648) and existing return shape otherwise intact.

3) Env var documentation: add `LIVE_ALLOW_ENV_DEFAULT_MODEL` (default unset/false => fail-closed) to the live-serving env reference wherever other `LIVE_*`/`RL_ALLOW_*` live-safety flags are documented (grep for `RL_ALLOW_FALLBACK_AGENT` to find the doc surface; mirror that documentation pattern). Note it controls whether an ungoverned env-default `MODEL_NAME` may serve live.

GUARDRAIL: preserve all existing fail-closed/safe-mode gates; never enable live execution; read-then-change. Default behavior MUST remain fail-closed (no new env var set => env-default-without-champion is BLOCKED for live). Do not weaken the existing `serve_fallback_active`, artifact, or feature-contract blockers. Do not print or hardcode secret values.

VERIFY
Add/extend a unit test (place beside existing predictor/live-safety tests; grep `live_model_serving_snapshot` and `_live_model_resolution` under tests/ to find the file) proving:
  a) When `_live_model_resolution` is forced to resolve to env-default `MODEL_NAME` with no governed candidate (monkeypatch the assignment/registry/competition lookups to yield nothing), the returned dict has `env_default_fallback is True` and `fallback_reason == "no_governed_champion_env_default"`, while `serve_fallback_active is False`.
  b) With live mode required and `LIVE_ALLOW_ENV_DEFAULT_MODEL` unset, `live_model_serving_snapshot(...)` returns `ok is False` with `"live_model_no_governed_champion"` in `blockers`.
  c) With `LIVE_ALLOW_ENV_DEFAULT_MODEL=1`, the same call does NOT include that blocker, the probe still carries `env_default_fallback True`, and the snapshot reports `allow_env_default_model True`.
  d) A governed champion (assignment/registry candidate resolving to a non-default name) yields `env_default_fallback False` and no new blocker (regression guard).
Run the new/updated test and paste the passing output. Confirm via grep that `serve_fallback_active` semantics in predictor.py are unchanged and that the default (env unset) path is fail-closed.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

### PL-9 — Give the Emergency Stop control dominant incident salience in operator_ui.html (P3)

ROLE: Senior front-end / human-factors engineer hardening the operator emergency-control surface.

TARGET: /home/david/gitsandbox/system/system

PROBLEM (re-confirmed at HEAD 8184174, branch codex/worktree-production-readiness):
The Emergency Stop control is the single most consequential action on the operator's highest-stress screen, yet it is rendered as just another button — same geometry, same row, differentiated only by color.

- boot/operator_ui.html:837 — `<button class="danger" onclick="emergencyStopHard()">Emergency Stop</button>` sits inside the top status-card action `.row` (opened :832 `<div class="row" style="margin-top:14px;">`), immediately after the benign `Start System Setup` (:833), `Start Engine` (:834, class `ghost`), `Restart` (:835, class `secondary`), and `Stop` (:836, class `secondary`) cluster.
- boot/operator_ui.html:1130 — a second `<button class="danger" onclick="emergencyStopHard()">Emergency Stop</button>` sits in the Operator Workflow card `.row` (:1123), after `Start System Setup` (:1124), three `ghost` view buttons (:1125-1127), and `Restart Feeds`/`Restart Engine` (:1128-1129).
- The `.danger` class (boot/operator_ui.html:166-172) only changes `color:var(--status-crit)`, a red/maroon gradient background, `border-color:var(--danger)`, `border-style:double`, and `font-weight:900`. It inherits the base `button` geometry (boot/operator_ui.html:119-130: `padding:10px 14px; border-radius:8px`). So Emergency Stop is identical in size and shape to the benign buttons beside it.
- The parent `.row` (boot/operator_ui.html:90-95: `display:flex; gap:10px; flex-wrap:wrap`) places Emergency Stop flush against the Start/Restart/Stop controls with only the standard 10px gap — no physical separation.
- Net effect: differentiation is color + border only. For a color-vision-deficient operator under stress, Emergency Stop is not larger, not shaped as a STOP/octagon, not isolated, and not unmistakably dominant. Misfire risk (hitting Restart instead of E-Stop, or vice versa) is elevated.
- This PAIRS WITH the GO-R8 fallback Emergency Stop work (docs/handoff/deep_dive_prompts/PRODUCTION_GO_LIVE_REMEDIATION_PROMPTS.md:133); keep the existing `emergencyStopHard()` wiring intact and reuse the same handler — do not fork behavior.

DESIGN / REQUIRED CHANGE (implement precisely; no re-discovery needed):
Make Emergency Stop the single most prominent, unmistakable, multi-channel-encoded (size + shape + color + icon + position) control in both status cards, while leaving handler behavior, env gates, and safe-mode logic untouched.

1) New CSS class in boot/operator_ui.html (add directly after the `button.danger:hover` block at :173-176, reusing existing tokens `--danger`, `--status-crit`, `--status-crit-bg`):
   - `.btn-estop` — a dominant variant. Required properties: larger hit target (`min-height:56px; min-width:200px; padding:16px 24px; font-size:18px; letter-spacing:.5px; font-weight:900;`), an octagonal STOP silhouette via `clip-path: polygon(30% 0%, 70% 0%, 100% 30%, 100% 70%, 70% 100%, 30% 100%, 0% 70%, 0% 30%)` (override `border-radius` to 0 so the octagon reads cleanly), a solid high-contrast fill using `--danger`/`--status-crit` (not a subtle gradient), a heavy `box-shadow:0 0 0 3px var(--status-crit-bg)` halo for non-color separation, and a `:hover`/`:focus-visible` state that brightens and keeps the existing `var(--focus)` outline contract from :137-145. Add `@media (prefers-reduced-motion: reduce)` safety (no animation required, but if any pulse is added it must be gated here).
   - Keep `.danger` as-is for other destructive buttons; `.btn-estop` is additive.

2) Inline STOP iconography: prepend an inline SVG octagon glyph (white stroke/fill, `aria-hidden="true"`, `width=20 height=20`, vertical-align middle) inside each Emergency Stop button label so the shape signal survives grayscale and reinforces the octagon. Label text becomes the SVG + `Emergency Stop`.

3) Physical separation + dominance in both rows:
   - Wrap each Emergency Stop button in its own container `<div class="estop-wrap">` placed at the END of the `.row`, after the benign cluster, with a CSS rule `.estop-wrap{ margin-left:auto; padding-left:16px; border-left:1px solid var(--danger); display:flex; }` so it is pushed to the far side and visually fenced off from Start/Restart/Stop. On narrow/wrapped layouts (`.row` wraps), add `@media (max-width:640px){ .estop-wrap{ margin-left:0; width:100%; border-left:0; border-top:1px solid var(--danger); padding-left:0; padding-top:12px; margin-top:4px; } .btn-estop{ width:100%; } }` so it remains the isolated, full-width, dominant control on mobile.
   - boot/operator_ui.html:837: change to use `class="btn-estop"` and move it into the `estop-wrap` placed after the `<a>` links OR at row end (row end preferred so it is the terminal, isolated control); do the same structurally at :1130.

4) Accessibility / name-role-value:
   - Add `aria-label="Emergency Stop — halt all trading immediately"` to both buttons so the screen-reader name conveys consequence (the SVG is `aria-hidden`).
   - Do NOT change `onclick="emergencyStopHard()"`; do NOT add a new confirm/disable path here (confirmation modal work is owned by the UI_WORLD_CLASS prompt — leave any existing confirm flow inside `emergencyStopHard()` untouched).

5) No behavior, no gating, no execution-path changes. This is presentation + DOM-position + a11y only. `emergencyStopHard()` and its server call (boot/operator_ui.html:2965-2967 wizard render) stay byte-identical.

GUARDRAIL: preserve all existing fail-closed/safe-mode gates; never enable live execution; read-then-change.

VERIFY (exact checks proving done):
1) `grep -n 'class="btn-estop"' boot/operator_ui.html` returns exactly 2 hits (lines formerly 837 and 1130) and `grep -c 'class="danger" onclick="emergencyStopHard()"' boot/operator_ui.html` returns 0 (both converted).
2) `grep -n '\.btn-estop' boot/operator_ui.html` shows the new class defined with `min-height:56px` and a `clip-path: polygon(` octagon; `grep -n 'estop-wrap' boot/operator_ui.html` shows the wrapper class defined and used twice with `margin-left:auto`.
3) `grep -n 'aria-label="Emergency Stop' boot/operator_ui.html` returns 2 hits; the inline `<svg ... aria-hidden="true"` octagon appears inside each Emergency Stop button.
4) `grep -n 'emergencyStopHard()' boot/operator_ui.html` still returns the original handler references (:837/:1130 wiring + :2965-2967) unchanged — no edit to the function body or its API call.
5) Open boot/operator_ui.html in a browser (or headless screenshot): in both the top status card and the Operator Workflow card, Emergency Stop renders visibly larger, octagon-shaped, fenced off at the row end from the Start/Restart/Stop cluster, and remains the dominant isolated control when the window is narrowed to <=640px. Confirm it is still distinguishable when the page is viewed in grayscale (shape + size + icon carry the signal without color).
6) No new JS errors in the browser console on load; clicking Emergency Stop still invokes `emergencyStopHard()` exactly as before.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

### PL-10 — service_ctl.sh logs reads journald but units write to file sink — operator triage shows no engine output (P3)

ROLE: You are a senior platform/SRE engineer hardening the operator control plane so the "logs" tool returns the real application logs during incident triage.

TARGET: /home/david/gitsandbox/system/system

PROBLEM (re-confirmed at HEAD 8184174, branch codex/worktree-production-readiness)
The operator "logs" / "logs_since" actions read a different sink than the systemd units actually write to, so triage returns empty or systemd-only noise instead of real engine/operator output.

- deploy/systemd/trading-engine.service:17-18 sends application stdout/stderr to a FILE:
  `StandardOutput=append:/opt/trading-system/logs/engine.log`
  `StandardError=append:/opt/trading-system/logs/engine.log`
- deploy/systemd/trading-operator.service:17-18 — same pattern, `/opt/trading-system/logs/operator.log`.
- deploy/systemd/trading-upgrade.service:18-19 — same pattern, `/opt/trading-system/logs/upgrade.log`.
- deploy/systemd/trading-backup.service — has NO StandardOutput/StandardError override, so it logs to journald only.
- deploy/bin/service_ctl.sh:41-58 implements `logs` and `logs_since` purely as journalctl:
  - line 45: `exec sudo -n journalctl -u "$UNIT" --since "$SINCE" -n "$LINES" --no-pager`
  - line 47: `exec sudo -n journalctl -u "$UNIT" -n "${SINCE:-400}" --no-pager`
  - line 57: `exec sudo -n journalctl -u "$UNIT" --since "$SINCE" -n "$LINES" --no-pager`
  Because the units redirect app output to append:FILE, journalctl -u <unit> shows only systemd lifecycle messages (Started/Stopped/exited), NOT the engine/operator application output the operator UI needs.
- boot/operator_server.js consumes this: line 5531 calls `runServiceCtl(["logs", service, String(n)], ...)` for /api/operator/logs, and line 2439 calls `runServiceCtl(["logs_since", "engine", since, ...])`. So the operator HTTP API and AI diagnostics inherit the wrong sink.
- The FILE sink is the canonical/maintained log destination: deploy/logrotate/trading-system rotates `/opt/trading-system/logs/*.log` (engine.log, operator.log, upgrade.log, runtime.log, ingestion.*.log, etc.) and also `/opt/trading/app/logs/*.log` and `/auxpool/trading/runtime/logs/*.log`; deploy/bin/rotate_local_logs.sh rotates the local `TRADING_LOGS`/`LOG_DIR` equivalent.

NET EFFECT: an operator running "logs engine" during an incident gets systemd chatter, not the runtime's real output — exactly when accurate logs matter most.

DESIGN / REQUIRED CHANGE (pick the file-sink path — it is the existing, rotated, canonical sink; do NOT change the units to journald, since that would orphan the rotation config and change disk/retention behavior)
Make `service_ctl.sh logs`/`logs_since` read the same files the units write, with a journald fallback only for units that have no file sink (e.g. backup).

Edit ONLY deploy/bin/service_ctl.sh:

1. Add a log-directory resolver near the top (after the SINCE/LINES vars, before map_unit). Resolve the base log dir from an env var with the production default:
   `LOG_DIR="${TRADING_LOGS:-${TRADING_LOG_DIR:-/opt/trading-system/logs}}"`
   (Match the precedence already used by deploy/bin/rotate_local_logs.sh, which honors TRADING_LOGS then LOG_DIR. Use TRADING_LOGS first so both tools agree on the directory; fall back to the systemd default `/opt/trading-system/logs`.)

2. Add a `map_logfile()` function mapping the friendly target to its append-target basename, mirroring the unit files:
   - engine   -> "$LOG_DIR/engine.log"
   - operator -> "$LOG_DIR/operator.log"
   - upgrade  -> "$LOG_DIR/upgrade.log"
   - backup   -> "" (no file sink; signal journald fallback)
   - all      -> "" (no single file; logs of "all" is not meaningful — keep current behavior of mapping to a unit, journald fallback)
   Anything else: reuse map_unit's invalid_target handling (exit 2).

3. Rewrite the `logs` block (currently service_ctl.sh:41-48) and the `logs_since` block (currently :50-58) so that:
   - They resolve BOTH the unit (via map_unit, still needed for the journald fallback) and the logfile (via map_logfile).
   - If the logfile path is non-empty AND the file exists and is readable: serve it from the file sink instead of journald.
     - `logs` with no SINCE: `exec sudo -n tail -n "${LINES}" -- "$LOGFILE"` (NOTE: preserve the existing quirk-free behavior — `logs` currently passes the 3rd positional as the line count via `${SINCE:-400}`; keep the line count coming from the same positional the operator/UI already passes. boot/operator_server.js calls `["logs", service, String(n)]` so the 3rd arg IS the line count — honor that: tail -n "$SINCE_OR_LINES" where SINCE_OR_LINES="${SINCE:-$LINES}". Use `sudo -n` to read files owned by the `trading` user, consistent with current require_sudo gating.)
     - `logs` WITH a since timestamp: there is no cheap time filter on a flat file; emit the last "$LINES" lines and prepend a single JSON-free notice line to stderr (e.g. `# note: file sink has no --since filter; showing last N lines` ) so callers still get content. Do NOT silently drop the request.
     - `logs_since`: same — `--since` is journald-only; when serving from the file sink, tail the last "$LINES" lines and emit the same stderr notice. Keep the existing `missing_since` validation (service_ctl.sh:53-56) BEFORE choosing the sink.
   - If the logfile path is empty OR the file does not exist/is unreadable: FALL BACK to the existing journalctl behavior unchanged (so `backup` and `all` keep working, and a fresh install with no log file yet still returns systemd lines).
   - Keep `require_sudo` (service_ctl.sh:34-39) called before any privileged read, exactly as today.

4. Do NOT touch the JSON status/start/stop paths. Do NOT change any .service file. Do NOT add new privileged actions.

GUARDRAIL: preserve all existing fail-closed/safe-mode gates; never enable live execution; read-then-change. This is a read/diagnostics tool only — it must remain read-only (tail/journalctl), retain the `require_sudo` non-interactive gate, and must NOT widen what sudo can run beyond reading existing log files (tail) and journalctl, both already implied. No secret values may be printed; the operator layer already redacts via redactOperatorSensitiveText in boot/operator_server.js, so do not add new redaction here but do not bypass it either.

VERIFY (all must pass)
1. Static: `bash -n deploy/bin/service_ctl.sh` parses clean.
2. Sink correctness, file present: create a temp dir, write known marker lines to `$TMP/engine.log`, then run
   `TRADING_LOGS="$TMP" bash deploy/bin/service_ctl.sh logs engine 5`
   and confirm the output is the last 5 lines of that file (the markers), NOT journald output. (Stub `require_sudo`/`sudo -n` if running unprivileged, e.g. by exporting a shim PATH where `sudo` just `exec "$@"`.)
3. Fallback correctness, no file: point TRADING_LOGS at an empty dir and run `... logs backup 10` and `... logs engine 10`; confirm both fall back to the journalctl path (command resolves to `journalctl -u <unit> ...`) rather than erroring.
4. logs_since validation preserved: `... logs_since engine` (no since arg) still returns `{"ok":false,"error":"missing_since"}` and exit 2.
5. Grep proof that the file sink is now read: `grep -n 'tail -n' deploy/bin/service_ctl.sh` shows the new file-sink branch, and `grep -n 'LOG_DIR' deploy/bin/service_ctl.sh` shows the resolver using TRADING_LOGS with the `/opt/trading-system/logs` default.
6. Confirm operator wiring unchanged: boot/operator_server.js still calls `runServiceCtl(["logs", ...])` / `["logs_since", ...]` (lines ~5531 and ~2439) — no JS changes required; the fix is sink-side only.
7. Confirm no .service file was modified: `git diff --name-only` lists only deploy/bin/service_ctl.sh.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

### PL-11 — Use safe-commit/safe-close helpers in broker_sim (P3)

ROLE: You are a senior execution-runtime engineer hardening connection teardown in the simulated broker so secondary close/commit failures can never mask the original error.

TARGET: /home/david/gitsandbox/system/system

IMPLEMENTATION NOTE (2026-06-26): Remediated in `engine/execution/broker_sim.py`.
The simulator now defines `_is_closed_connection_error`,
`_connection_marked_closed`, `_safe_commit_connection`, and
`_safe_close_connection` immediately after `_warn_nonfatal`. The helper marker
set matches `broker_apply_orders.py`; already-marked-closed connections are not
touched; closed-connection and other secondary commit/close failures are logged
through broker-sim nonfatal warnings and return `False` without raising. The
job-side option-lifecycle commits, fallback account commit, final
`apply_new_portfolio_orders` commit, and apply/equity/snapshot closes route
through the helpers. The old `_broker_sim_phase_persist_account_positions`
commit site is absent because the apply path now uses one final atomic commit for
fills/account/positions/mark-to-market plus `last_portfolio_orders_id`. Covered
by `tests/test_broker_sim_safe_teardown.py`.

PROBLEM
engine/execution/broker_sim.py issues raw `con.commit()` and `con.close()` calls with no closed-connection guard. On an already-invalidated pooled/closed connection, a bare commit or a `finally: con.close()` can raise a secondary exception that masks the original error or escapes the job, unlike the sibling module which already centralizes this. Re-confirmed at HEAD 8184174:
- engine/execution/broker_sim.py:3900 — `con.commit()` after `_set_meta(...)` inside `apply_new_portfolio_orders`.
- engine/execution/broker_sim.py:3922 — `finally: con.close()` for `apply_new_portfolio_orders`.
- engine/execution/broker_sim.py:4007 — `finally: con.close()` for `broker_equity_at`.
- engine/execution/broker_sim.py:4054 — `finally: con.close()` for `broker_snapshot`.
- engine/execution/broker_sim.py:2948 — `con.commit()` in `_broker_sim_phase_persist_account_positions`.
- engine/execution/broker_sim.py:2347 and :2349 — two `con.commit()` calls in the options-lifecycle apply path.
- engine/execution/broker_sim.py:2634 — `con.commit()` after the fallback `_write_account`.
(Note line numbers drifted from the original report's 3880/3901-3902/3986-3987/4033-4034 — the values above are the confirmed current sites. The init path at :977/:989/:1003/:1016 is intentionally left as-is unless trivially covered by the new helpers; see DESIGN.)

By contrast, the sibling engine/execution/broker_apply_orders.py already defines and uses guarded helpers in every commit/finally:
- `_is_closed_connection_error(error)` (broker_apply_orders.py:226) — substring match on closed-connection markers.
- `_connection_marked_closed(con)` (broker_apply_orders.py:241) — inspects `_closed`/`closed`/`raw.closed` attributes.
- `_safe_commit_connection(con, *, context, once_key)` (broker_apply_orders.py:288) — skips/guards commit, returns bool, never raises.
- `_safe_close_connection(con, *, context, once_key)` (broker_apply_orders.py:318) — skips/guards close, returns bool, never raises.
These swallow closed-connection errors and emit a once-keyed nonfatal warning, never raising a secondary exception. broker_sim has no equivalent.

DESIGN / REQUIRED CHANGE
Add the same guarded teardown pattern to broker_sim and route the job-side commit/close sites through it.

1. Add three private helpers to engine/execution/broker_sim.py, placed just after the existing `_warn_nonfatal` definition (engine/execution/broker_sim.py:232):
   - `_is_closed_connection_error(error: BaseException) -> bool` — identical marker set to the sibling (broker_apply_orders.py:226): match (case-insensitive, stripped) any of "cannot operate on a closed database", "closed database", "connection already closed", "connection is closed", "connection closed", "closed connection".
   - `_connection_marked_closed(con: Any) -> bool` — port the sibling logic (broker_apply_orders.py:241): check `_closed`/`closed` then `raw.closed`/`raw._closed`; on attribute access failure, swallow and continue (do NOT raise). Use broker_sim's local `_warn_nonfatal` signature `(event, code, error, *, warn_key=..., **extra)` for any nonfatal logging.
   - `_safe_commit_connection(con: Any, *, context: str, once_key: str) -> bool` and `_safe_close_connection(con: Any, *, context: str, once_key: str) -> bool` — port broker_apply_orders.py:288 and :318 behavior: if connection already marked closed, log once and return False without touching it; otherwise try the operation; on a closed-connection error log once and return False; on any other error log nonfatal and return False; on success return True. NEVER raise. Map all logging through broker_sim's `_warn_nonfatal`, e.g. `_warn_nonfatal("broker_sim_commit_skipped_closed_connection", "BROKER_SIM_COMMIT_SKIPPED_CLOSED_CONNECTION", RuntimeError("connection already closed before commit"), warn_key=f"{once_key}:closed", context=str(context))`. Use distinct codes for commit-failed / close-skipped / close-failed analogous to the sibling.

2. Replace the job-side raw calls with the helpers (preserve surrounding logic exactly; only swap the commit/close call):
   - 3900 → `_safe_commit_connection(con, context="apply_new_portfolio_orders", once_key="apply_orders_commit")`.
   - 3922 → `_safe_close_connection(con, context="apply_new_portfolio_orders", once_key="apply_orders_close")`.
   - 4007 → `_safe_close_connection(con, context="broker_equity_at", once_key="broker_equity_at_close")`.
   - 4054 → `_safe_close_connection(con, context="broker_snapshot", once_key="broker_snapshot_close")`.
   - 2948 → `_safe_commit_connection(con, context="persist_account_positions", once_key="persist_account_positions_commit")`.
   - 2347 → `_safe_commit_connection(con, context="options_lifecycle", once_key="options_lifecycle_commit_account")`.
   - 2349 → `_safe_commit_connection(con, context="options_lifecycle", once_key="options_lifecycle_commit_mtm")`.
   - 2634 → `_safe_commit_connection(con, context="fallback_write_account", once_key="fallback_write_account_commit")`.
   Do NOT change the per-call rollback `try/except` blocks already present (e.g. the options-lifecycle `except` at ~2351 and init rollback at ~1005); they remain.

3. Leave the `init_broker_db` internal close/commit sites (engine/execution/broker_sim.py:977/:989/:1003/:1016) UNCHANGED unless the swap is a pure no-behavior-change substitution — these are short-lived local read/write-txn connections inside guarded blocks and out of the job-side blast radius; do not refactor their control flow.

4. Do not alter function signatures, return shapes, idempotency markers (`last_portfolio_orders_id`), or any caller. The helpers must be drop-in: where a raw `con.commit()` returned None and the surrounding code ignored the result, ignore the bool too.

GUARDRAIL: preserve all existing fail-closed/safe-mode gates; never enable live execution; read-then-change. This is broker_sim (simulated) only — do not touch live broker modules, the broker_router failover/reconciliation gate, or any kill-switch/suppression logic.

VERIFY
- Static: `cd /home/david/gitsandbox/system/system && python -c "import ast,sys; ast.parse(open('engine/execution/broker_sim.py').read())"` parses cleanly; `python -c "import engine.execution.broker_sim"` imports without error.
- Grep proves no remaining unguarded job-side teardown in broker_sim: `grep -nE '(^|[^_])con\.commit\(\)|(^|[^_])con\.close\(\)' engine/execution/broker_sim.py` shows ONLY the intentionally-retained `init_broker_db` sites (977/989/1003/1016); every site listed in DESIGN step 2 now routes through `_safe_commit_connection`/`_safe_close_connection`.
- Helper presence: `grep -n '_safe_commit_connection\|_safe_close_connection\|_is_closed_connection_error\|_connection_marked_closed' engine/execution/broker_sim.py` shows all four definitions plus the eight call sites.
- Behavioral test (add to the existing broker_sim test module, or create tests/execution/test_broker_sim_safe_teardown.py): pass a stub connection whose `.commit()`/`.close()` raise `sqlite3.ProgrammingError("Cannot operate on a closed database.")` (or set `closed=True`) into `_safe_commit_connection`/`_safe_close_connection` and assert each returns False and raises NOTHING; pass a healthy stub and assert it returns True and the underlying method was called exactly once. Run: `cd /home/david/gitsandbox/system/system && python -m pytest tests/execution/test_broker_sim_safe_teardown.py -q` is green.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

### PL-12 — Map execution_mode_not_live arm refusal to 409 instead of HTTP 500 (P3)

ROLE: Senior backend engineer hardening the operator API HTTP status-mapping layer.

TARGET: /home/david/gitsandbox/system/system

IMPLEMENTATION NOTE (2026-06-26): Remediated in `engine/api/http_transport.py` by mapping `execution_mode_not_live`, `mode_not_live`, and the broader `*_not_live` refusal family to HTTP 409 in the central status mapper. The arm handler refusal payload remains unchanged (`ok:false`, `error:"execution_mode_not_live"`, `execution_mode`), and `tests/test_http_transport_status_contract.py` covers the mapper, regression cases, and derived-payload status.

PROBLEM
The operator "arm live execution" endpoint returns a business refusal that is mis-mapped to HTTP 500, making a legitimate, expected refusal look like a server fault to clients/dashboards.

Evidence (re-confirmed at HEAD 8184174):
- engine/api/api_operator_handlers.py:928-933 — in `api_post_operator_execution_arm`, when `arm` is requested but the current mode is not "live", the handler returns:
  `{"ok": False, "error": "execution_mode_not_live", "execution_mode": before}`
  with NO `meta.status` (and no `status_code`/`http_status`).
- engine/api/http_transport.py:319-351 — `_derive_response_status(payload, default_status)` first honors an explicit `meta.status` / `meta.http_status` / `status_code` / `http_status` (lines 326-334); if none is present and `ok` is False with a non-empty `error`, it falls through to `_map_error_to_status(...)` (lines 349-351).
- engine/api/http_transport.py:167-238 — `_map_error_to_status(error_code)` has NO rule that matches `execution_mode_not_live` (it is not in the 403/409/422/400 sets, does not match any prefix/suffix branch). It therefore reaches the terminal `return 500` (line 238).

Net effect: a correct, intended refusal ("you cannot ARM because the system is not in live mode") is served as HTTP 500. This is cosmetic correctness only — the underlying arm guard is intact and still refuses; no live execution is enabled by this bug or its fix.

Sibling `*_not_live` reasons exist elsewhere and would also fall to 500 if surfaced as an `error` over HTTP (re-confirmed): `engine/execution/execution_mode.py:595` (`"mode_not_live"`), `engine/execution/mode_safety.py:307` (`"mode_not_live"`), `engine/api/api_system.py:1489` (`"simulated_market_data_not_live"`), `engine/api/api_system.py:1858`/`1886` (`f"mode_{mode}_not_live"`). The fix should generalize to these without special-casing each string.

DESIGN / REQUIRED CHANGE
Map `*_not_live` business refusals to HTTP 409 (Conflict) — the request is well-formed but conflicts with current system state (not in live mode). Do this centrally in the status mapper so all current and future `_not_live` refusals are covered; do NOT scatter `meta.status` literals into handlers.

1) engine/api/http_transport.py — `_map_error_to_status` (around lines 196-204, the existing 409 block):
   Extend the 409 mapping to also catch the not-live family. Concretely, add a branch (place it adjacent to the existing 409 `pre_trade_rejected`/`duplicate_recent_order` set, after the 403 block at ~195) such that any code that equals `execution_mode_not_live` or `mode_not_live`, OR ends with `_not_live`, returns 409. Example shape (match surrounding style; `code` is already lowercased/stripped at line 168):
       if code in {"execution_mode_not_live", "mode_not_live"} or code.endswith("_not_live"):
           return 409
   Ensure this branch is reached BEFORE the generic `missing_`/`invalid_`/`*_required` 400 block (lines 225-235) and before the terminal `return 500` — the `_not_live` codes do not match those earlier branches, so simply inserting the new branch in the 403/409 region is sufficient; verify ordering by reading the function top-to-bottom.
   Do not weaken or reorder any existing branch (timeout/413/401/403/404/410/429/422/400/503 must keep their current precedence and codes).

2) Leave engine/api/api_operator_handlers.py:928-933 behavior intact (still returns `ok:false` + `error:"execution_mode_not_live"`). No live-arm logic changes. The handler still refuses; only the derived HTTP status changes. (Optional, only if you also want belt-and-suspenders: you MAY add `"meta": {"status": 409}` to that refusal payload — but it is redundant once the mapper rule exists, and `_derive_response_status` already prefers `meta.status`. Prefer the central mapper rule alone to avoid drift.)

3) Add/extend a unit test for `_map_error_to_status` (find the existing test module for http_transport under tests/ — search `grep -rn "_map_error_to_status\|_derive_response_status" tests/`; if none exists, create tests/api/test_http_transport_status_map.py). Assert:
   - `_map_error_to_status("execution_mode_not_live") == 409`
   - `_map_error_to_status("mode_not_live") == 409`
   - `_map_error_to_status("simulated_market_data_not_live") == 409`
   - `_map_error_to_status("mode_paper_not_live") == 409`
   - Regression guards that existing mappings are unchanged: `"execution_blocked" == 403`, `"pre_trade_rejected" == 409`, `"unknown_endpoint" == 404`, `"missing_credentials" == 422`, and an arbitrary unknown code (e.g. `"some_other_error") == 500`.
   - End-to-end at the payload level: `_derive_response_status({"ok": False, "error": "execution_mode_not_live", "execution_mode": {}}, 200) == 409`.

GUARDRAIL: preserve all existing fail-closed/safe-mode gates; never enable live execution; read-then-change. This is status-code cosmetics only — the arm refusal guard, execution-mode gating, and every other `_map_error_to_status` branch must remain byte-for-byte equivalent in behavior except for the new 409 family.

VERIFY
- `grep -n "_not_live" engine/api/http_transport.py` shows the new 409 branch inside `_map_error_to_status`.
- Run the test module (e.g. `python -m pytest tests/api/test_http_transport_status_map.py -q` or the repo's test runner) — all new assertions pass and no prior http_transport tests regress.
- Sanity REPL: `python -c "from engine.api.http_transport import _map_error_to_status, _derive_response_status; assert _map_error_to_status('execution_mode_not_live')==409; assert _map_error_to_status('some_other_error')==500; assert _derive_response_status({'ok':False,'error':'execution_mode_not_live'},200)==409; print('ok')"` prints `ok`.
- Confirm no behavioral change to `api_post_operator_execution_arm` other than the HTTP status: the refusal body still contains `ok:false`, `error:"execution_mode_not_live"`, and `execution_mode`.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

### PL-13 — Wrap observability snapshot handlers (competition_view/replay_freshness/attribution_quality) in a degraded-snapshot guard (P3)

ROLE: Senior backend engineer hardening observability API endpoints for graceful degradation.

TARGET: /home/david/gitsandbox/system/system

PROBLEM (confirmed against HEAD 8184174, branch codex/worktree-production-readiness):
Three observability snapshot handlers in `engine/api/api_system.py` have NO try/except and can leak a bare transport-layer HTTP 500 instead of the system's standard degraded-snapshot payload:
- `api_get_competition_view` (`engine/api/api_system.py:2786`) — builds the snapshot and reads competition runtime.
- `api_get_replay_freshness` (`engine/api/api_system.py:2835`) — calls `_meta_json("competition_replay_validation")` / `_meta_json("competition_replay_validation_status")`.
- `api_get_attribution_quality` (`engine/api/api_system.py:2890`) — calls `_meta_json("attribution_completeness")`, `_meta_json("execution_order_model_identity_repair")`, `_meta_json("trade_attribution_historical_repair")`, `_meta_json("execution_poll_and_attrib_last")`.

`_meta_json` is at `engine/api/api_system.py:434` and delegates to `meta_get`. `meta_get` in `engine/runtime/runtime_meta.py:651-657` does `con = _db_connect(readonly=True)` then `con.execute("SELECT value FROM runtime_meta WHERE key=?", (key_s,))` with NO table-exists guard. If `runtime_meta` is missing, the DB is mid-failure, or the pool connection is poisoned, this raises — and because these three handlers (unlike their siblings) have no try/except, the exception propagates to the transport catch-all and the client gets a bare HTTP 500 with no structured reasons.

The correct pattern already exists in the SAME file: `api_get_runtime_health` (`engine/api/api_system.py:2929-2962`) wraps its body in `try/except`, on failure calls `failure_response(...)` and returns `_snapshot_response(snapshot, ok=False, status="DEGRADED" if snapshot.get("status")=="RUNNING" else snapshot.get("status"), reasons=_dedupe_reasons(snapshot.get("reasons"), [f"runtime_health_error:{e}"]), ..., error=str(e), root_cause_code=..., failure_scope=..., failure_type=..., system_state_snapshot=...)`.

Available helpers (already imported in this file): `failure_response` (from `engine.runtime.failure_diagnostics`, used at `:103`, `:2940`, `:3320`, `:3759`), `log` (module logger), `_dedupe_reasons` (`:394`), `_snapshot_response` (`:402`), `_build_system_snapshot` (`:1894`).

This is defense-in-depth: a missing `runtime_meta` table means the system is already non-functional, but standardizing graceful degradation prevents bare 500s and gives the dashboard/operator structured `ok:false` + reasons.

DESIGN / REQUIRED CHANGE:
Refactor each of the three handlers so the snapshot is always built first (outside the guard, exactly like `api_get_runtime_health` builds `snapshot` before the `try`), then wrap the body that touches `_meta_json`/runtime reads in `try/except Exception as e`, mirroring the `api_get_runtime_health` degraded path. Do NOT change the success-path response shape or keys — only add the failure path.

For each handler:
1. Keep `snapshot = _build_system_snapshot(_parsed, ctx)` as the first statement, OUTSIDE the try (so a snapshot is always available for the degraded response). The `_normalized_health_from_snapshot(...)` call and everything after it (the `_meta_json` reads, summary construction, and the final `_snapshot_response(...)` success return) move INSIDE the `try`.
2. In `except Exception as e:` build `failure = failure_response(log, event=<event>, code=<code>, message=str(e), error=e, component="engine.api.api_system", ctx=ctx, extra={"status": str(snapshot.get("status") or "")})` then `return _snapshot_response(snapshot, ok=False, status="DEGRADED" if snapshot.get("status")=="RUNNING" else snapshot.get("status"), reasons=_dedupe_reasons(snapshot.get("reasons"), [f"<reason_prefix>:{e}"]), <payload_key>={"ok": False, "error": str(e)}, error=str(e), root_cause_code=failure.get("root_cause_code"), failure_scope=failure.get("failure_scope"), failure_type=failure.get("failure_type"), system_state_snapshot=failure.get("system_state_snapshot"))`.

Use these per-handler identifiers (match existing snake/SCREAMING style):
- `api_get_competition_view`: event=`api_system_competition_view_failed`, code=`API_SYSTEM_COMPETITION_VIEW_FAILED`, reason prefix `competition_view_error`, payload key `competition={"ok": False, "error": str(e)}`.
- `api_get_replay_freshness`: event=`api_system_replay_freshness_failed`, code=`API_SYSTEM_REPLAY_FRESHNESS_FAILED`, reason prefix `replay_freshness_error`, payload key `replay_freshness={"ok": False, "error": str(e)}`.
- `api_get_attribution_quality`: event=`api_system_attribution_quality_failed`, code=`API_SYSTEM_ATTRIBUTION_QUALITY_FAILED`, reason prefix `attribution_quality_error`, payload key `attribution_quality={"ok": False, "error": str(e)}`.

Do not introduce new env vars, new modules, or new helpers — reuse the existing `failure_response`/`_dedupe_reasons`/`_snapshot_response` pattern verbatim so behavior is identical to `api_get_runtime_health`. Do not add a table-exists guard to `meta_get` as part of THIS change (that is a separate storage-layer concern); the fix is purely the handler-level degraded guard. The HTTP status mapping is whatever `_snapshot_response` already derives from `ok`/`status` (do not hardcode a numeric 200/503 in the handler — the existing response/transport layer maps degraded snapshots, as it does for `api_get_runtime_health`).

Add a regression test (extend the existing api_system test module under `tests/` — locate it with `grep -rl "api_get_runtime_health\|api_get_attribution_quality" tests/`; if none, create `tests/test_api_system_degraded_snapshots.py`) that monkeypatches `engine.api.api_system.meta_get` (or `_meta_json`) to raise, calls each of the three handlers, and asserts the return is a dict with `ok is False`, contains a `reasons` entry matching the handler's reason prefix, and that NO exception propagates (i.e. the handler returns rather than raises). Also assert the success path still returns `ok` truthy when `meta_get` returns normal data, to prove the success-path shape is unchanged.

GUARDRAIL: preserve all existing fail-closed/safe-mode gates; never enable live execution; read-then-change.

VERIFY:
1. `grep -n "try:" engine/api/api_system.py` shows each of `api_get_competition_view`, `api_get_replay_freshness`, `api_get_attribution_quality` now contains a try/except mirroring `api_get_runtime_health`.
2. Run the new/updated test: it passes — each handler returns `ok:False` with the correct reason prefix when `meta_get` raises, and never propagates the exception; success path still returns the unchanged payload shape.
3. `python -c "import ast,sys; ast.parse(open('engine/api/api_system.py').read())"` parses clean; targeted test run is green.
4. Run `git status --short --untracked-files=all` and `python tools/git_worktree_triage.py`; capture exit codes. Confirm the success-path response keys/values for all three endpoints are byte-for-byte unchanged when `meta_get` succeeds (diff the success branch against current behavior).

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---
