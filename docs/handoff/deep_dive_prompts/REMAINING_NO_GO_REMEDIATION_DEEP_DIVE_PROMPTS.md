# Remaining NO-GO Remediation — Focused Codex Deep-Dive Prompts

Authored 2026-06-26 after the post-run verification pass on `/home/david/gitsandbox/system/system` branch `codex/worktree-production-readiness` at HEAD `8184174`. These prompts convert the remaining NO-GO findings into independently runnable implementation workstreams. Run one prompt at a time in a fresh Codex session. Re-confirm every file path, line number, test name, and validator result on the current tree before editing because this worktree is actively changing.

Standing guardrail for all prompts: preserve all existing fail-closed and safe-mode gates, never enable live execution, never print or hardcode secret values, and make the production/runtime enforcement happen in code or CI gates rather than only tests/docs.

## REM-GO-1 — Implement off-host backup copy and fail-closed offsite evidence

ROLE: You are a disaster-recovery engineer closing the remaining single-host recovery-point gap.

TARGET: `/home/david/gitsandbox/system/system`

CURRENT AUDIT EVIDENCE: `ops/backup/base_backup.sh` still copies offsite only when `TS_BASE_BACKUP_OFFSITE_CMD` is set; `ops/backup/offsite_base_backup_stub.sh` exists but is not wired as the default offsite leg; `ops/backup/backup_restore_evidence.sh` does not enforce offsite freshness. The verification pass found no `ops/backup` worktree changes for GO-R4.

TASK: Design and implement the optimal off-host backup evidence path. Wire the offsite base-backup stub into the base-backup flow when `TS_OFFSITE_BACKUP_DEST` is configured, keep custom `TS_BASE_BACKUP_OFFSITE_CMD` support for operator-specific transports, and add a signed evidence assertion that fails when offsite backup evidence is required but missing, stale, same-host-only, or unreadable. Add explicit config for `OFFSITE_REQUIRED`, freshness windows, destination type, and expected evidence paths. The evidence JSON must distinguish local base backup, WAL archive, restore drill, and offsite copy status.

INTEGRATION REQUIREMENTS: Update `ops/backup/base_backup.sh`, `ops/backup/offsite_base_backup_stub.sh` only if needed, `ops/backup/backup_restore_evidence.sh`, deploy env templates, and production backup docs. Keep local backup/WAL/restore evidence behavior intact. Do not weaken existing HMAC/signature checks. Make `python tools/validate_repo.py --live` and any backup evidence preflight consume the new offsite status when required.

VERIFY: Add shell or Python tests covering fresh offsite evidence passes, stale/missing offsite evidence fails when required, same-host-only destinations are reported clearly, and offsite-not-required remains compatible. Run the backup evidence scripts against a temp directory and show exact pass/fail exit codes.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

## REM-GO-2 — Make validate_repo --live pass by closing live secret and host-readiness blockers

ROLE: You are a production release engineer finishing the go-live validation gate.

TARGET: `/home/david/gitsandbox/system/system`

CURRENT AUDIT EVIDENCE: `python tools/validate_repo.py --live` exited `1` during verification. Key blocker: `secret_file_invalid:BACKUP_EVIDENCE_HMAC_KEY` during `runtime-graph-startup`. Host checks also showed swap capacity present, but installed systemd units reported `WatchdogUSec=infinity` despite repo unit files containing `WatchdogSec=60`.

TASK: Design and implement the optimal closure for the live validation path. First, reproduce `python tools/validate_repo.py --live` and capture current blockers. Fix repo-side logic only where validation is incorrectly reading, redacting, or classifying evidence; fix operator/host configuration where the validation is working as designed. Ensure `BACKUP_EVIDENCE_HMAC_KEY` is sourced only through approved file/credential paths with strict mode and never as inline plaintext. Ensure installed units are reloaded/applied so `systemctl show trading-engine` and `systemctl show trading-operator` reflect the repo unit watchdog and memory directives.

INTEGRATION REQUIREMENTS: Do not suppress the strict secret-source policy. Do not change the systemd-creds default. If docs or install scripts need to enforce correct file modes and daemon-reload steps, update them. If `validate_repo.py --live` needs a clearer diagnostic for host-only operator actions, add one without bypassing the gate.

VERIFY: `python tools/validate_repo.py --live` exits `0`; `python tools/runtime_graph_check.py --mode startup` exits `0`; `swapon --show` shows at least 16 GiB total; `systemctl show trading-engine -p Type -p WatchdogUSec -p MemoryMax -p OOMScoreAdjust` and the operator equivalent show applied watchdog and memory limits. No secret values may appear in output.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

## REM-HG-4 — Enforce shared scientific pins across CPU, ROCm, and CUDA profiles

ROLE: You are a supply-chain engineer finishing dependency-profile reproducibility.

TARGET: `/home/david/gitsandbox/system/system`

CURRENT AUDIT EVIDENCE: GPU manifests now reference profile locks, and `python tools/validate_dependency_lock.py --strict` passes, but `tools/validate_dependency_lock.py` has no cross-profile scientific-pin consistency rule. Static search found no `cross_profile_pin_mismatch`, `SHARED_PIN_ALLOWLIST`, or equivalent.

TASK: Design and implement a validator rule that compares a defined set of shared scientific/runtime package pins across `requirements-base.txt`, CPU runtime locks/manifests, `requirements-amd-rocm-full.txt` plus `requirements-amd-rocm.lock.txt`, and `requirements-nvidia-cuda.txt` plus `requirements-nvidia-cuda.lock.txt`. Packages that must normally match include NumPy, pandas, SciPy, scikit-learn, statsmodels, numba, pyarrow, pydantic, SQLAlchemy, transformers-adjacent shared libraries where applicable, and any other package whose ABI/API mismatch can change model/risk behavior. Allow intentional divergence only through a small in-code allowlist with package, profile, expected version, and reason.

INTEGRATION REQUIREMENTS: Extend `tools/validate_dependency_lock.py --strict` and JSON output. Add contract tests that construct temporary manifests/locks and prove matching pins pass, mismatched pins fail with `cross_profile_pin_mismatch`, and allowlisted divergences warn with a documented reason. Update `docs/DEPENDENCY_PROFILES.md` to explain the rule and regeneration workflow.

VERIFY: `python tools/validate_dependency_lock.py --json` reports `ok: true` with no mismatch errors on the checked-in manifests; a deliberate temporary mismatch produces a non-zero strict validation; `python -m pytest tests -k "dependency_lock or validate_dependency" -q` passes; `ruff check tools/validate_dependency_lock.py tests/test_dependency_lock_contract.py` passes if ruff is available.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

## REM-HG-5 — Add engine/strategy coverage floor and zero-covered-module guard

ROLE: You are a test-infrastructure engineer hardening money-path coverage enforcement.

TARGET: `/home/david/gitsandbox/system/system`

CURRENT AUDIT EVIDENCE: `pyproject.toml` still has `zero_covered_module_roots = ["engine/risk", "engine/execution", "engine/runtime"]` and package floors only for `engine/risk`, `engine/execution`, and `engine/runtime`. `engine/strategy` is not guarded by a package floor or zero-coverage root.

TASK: Design and implement the minimum correct coverage-gate change. Measure current honest `engine/strategy` coverage using the existing coverage report or by running `python tools/coverage_gate.py run` if feasible. Add `engine/strategy` to `zero_covered_module_roots` and add a defended `engine/strategy` floor at or just below measured coverage. Seed the zero-covered allowlist only for pre-existing non-money-path modules that are currently zero-covered. Do not allowlist key money-path files such as `predictor.py`, `portfolio.py`, `portfolio_risk_gate.py`, promotion/governance paths, or live serving/risk surfaces; add focused smoke tests instead if any of those are zero.

INTEGRATION REQUIREMENTS: Prefer config-only changes in `pyproject.toml` unless a real bug in `tools/coverage_gate.py` is found. Update `docs/COVERAGE_GATE.md` to mention the strategy floor. Keep existing aggregate and runtime/risk/execution floors unchanged or stricter.

VERIFY: `python tools/coverage_gate.py check` or `run` prints an `engine/strategy` required-floor row and passes; a temporary unimported `engine/strategy/_zerocov_probe.py` causes the gate to fail as a new zero-covered module, then deleting it returns the gate to pass. Run `python -m pytest tests/test_coverage_gate.py -q` if applicable.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

## REM-HG-6 — Add Hypothesis property tests for risk invariants and wire safety-critical CI

ROLE: You are a quant-infra engineer adding property-based coverage for risk invariants.

TARGET: `/home/david/gitsandbox/system/system`

CURRENT AUDIT EVIDENCE: `tests/test_risk_invariants_property.py` is absent and `hypothesis` is not present in `requirements-dev.in`, `requirements-dev.txt`, or `requirements-dev.lock.txt`.

TASK: Implement the HG-6 prompt fully. Add a pinned `hypothesis==6.x` dev dependency using the repo's hashed lock workflow. Create `tests/test_risk_invariants_property.py` with module-level `pytestmark = pytest.mark.safety_critical`. Add bounded deterministic Hypothesis tests for `engine.risk.monte_carlo_risk_engine._pct/_cvar` lower-tail PnL direction (`cvar <= var` for `q=0.05/0.01`), upper-tail drawdown direction (`cvar >= pct` for `q=0.95/0.99`), empty-list behavior, portfolio gross/net cap soundness, sign preservation, no-op within cap, idempotence, and scalar monotonicity after caps bind.

INTEGRATION REQUIREMENTS: Wire the new file into `.github/workflows/validate.yml` safety-critical money-path step with `--expected-source tests/test_risk_invariants_property.py`, append the file to the explicit safety-critical test list, and raise `--min-selected` based on the actual post-addition selected count. Do not monkeypatch or widen `MAX_GROSS` / `MAX_NET`. Do not open sockets or touch DB-backed helpers unless a pure path exists.

VERIFY: `python -c "import hypothesis; print(hypothesis.__version__)"` succeeds; `grep -i hypothesis requirements-dev.in requirements-dev.lock.txt` shows the pinned entry; `python -m pytest tests/test_risk_invariants_property.py -q` passes; the local `tools/run_required_backend_tests.py` safety-critical command passes with `missing_sources=0` and the raised minimum; temporarily flipping one tail-direction assertion yields a Hypothesis counterexample, then revert.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

## REM-HG-7 — Add scheduled soak/chaos CI lane and make safe-mode soak assert failures

ROLE: You are a release-engineering engineer adding a bounded nightly soak/chaos gate.

TARGET: `/home/david/gitsandbox/system/system`

CURRENT AUDIT EVIDENCE: `.github/workflows/soak_chaos.yml` is missing. `python tools/safe_mode_soak.py --help` lacks `--max-error-rate` and `--max-rss-growth-mb`; the safe-mode soak tool still reports observations without a hard pass/fail gate.

TASK: Design and implement the soak/chaos lane. Add `.github/workflows/soak_chaos.yml` with `schedule` and `workflow_dispatch` triggers only, a bounded `soak-chaos` job, safe-mode environment (`ENGINE_MODE=EXECUTION_MODE=OPERATOR_MODE=safe`, `PROD_LOCK=0`, `KILL_SWITCH_GLOBAL=0`), Postgres/Redis services matching the backend CI lane, a safe dashboard boot, an ephemeral unprinted `SOAK_REPORT_SIGNING_KEY`, safe-mode soak execution, market-session chaos soak execution, and artifact upload on failure/success.

INTEGRATION REQUIREMENTS: Extend `tools/safe_mode_soak.py` with a pure evidence validator and CLI flags `--max-error-rate` and `--max-rss-growth-mb`. Return non-zero when NDJSON samples contain `ok=false`, fail-pattern log matches, no samples, excessive error rate, or excessive RSS growth. Do not weaken `tools/market_session_soak.py` missing-signing-key or unintended-live-order checks. Update docs for the new nightly lane.

VERIFY: `.github/workflows/soak_chaos.yml` parses as YAML and contains `schedule` but no `push` or `pull_request`; `python tools/safe_mode_soak.py --help` shows the new flags; `python -m pytest tests/test_safe_mode_soak_gate.py -q` passes for clean and bad NDJSON evidence; `rg -n "EXIT_NO_GO|soak_report_signing_key_missing|unintended_live_order_evidence" tools/market_session_soak.py` confirms those fail-closed paths remain.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

## REM-HG-8 — Add crypto live-disable safety block at broker-router boundary

ROLE: You are an execution-safety engineer adding defense-in-depth at the live broker boundary.

TARGET: `/home/david/gitsandbox/system/system`

CURRENT AUDIT EVIDENCE: Crypto live gating exists in `engine/strategy/portfolio_risk_gate.py`, but `engine/execution/broker_router.py` has no crypto live-disable block before broker selection/routing. Verification found `_prefer_crypto_capable_broker` and `_batch_has_crypto`, but no `_crypto_order_safety_block` equivalent to FX/futures.

TASK: Design and implement a broker-router crypto safety block. Before broker failover attempts, if a batch contains crypto, a live broker is in the chain, the route is not dry-run/paper/sim, and `CRYPTO_LIVE_TRADING_ENABLED` is unset/false, return a non-retryable `stop_failover` block with status/reason `crypto_live_trading_disabled_by_default`. Preserve existing `portfolio_risk_gate` checks and do not move or weaken them. Add a direct IBKR/PAXOS submit-path guard if there is a separate crypto live submit helper.

INTEGRATION REQUIREMENTS: Mirror the style of `_fx_order_safety_block` and `_futures_order_safety_block`. Keep sim/paper/dry-run behavior unchanged. Add env docs and preflight snapshot references if not already present. The new block must never widen permissions; default must be deny for live crypto orders.

VERIFY: Add or extend tests proving live IBKR chain plus crypto order with `CRYPTO_LIVE_TRADING_ENABLED` unset blocks at broker-router boundary, setting the flag permits routing to the next existing gates, dry-run/paper/sim are unchanged, non-crypto orders are unaffected, and the existing portfolio crypto gate tests still pass.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

## REM-PL-1 — Make broker-router execution-health probe fail closed on missing or raising reader

STATUS UPDATE (2026-06-26): Implemented in `engine/execution/broker_router.py` with regression coverage in `tests/test_broker_router_degraded_probe_fail_closed.py`. Missing or raising execution-health readers now produce WARNING-active degraded snapshots, while unprimed/empty health remains quiet and critical/down/unhealthy mappings are unchanged.

ROLE: You are an execution-safety engineer tightening degraded-health handling.

TARGET: `/home/david/gitsandbox/system/system`

CURRENT AUDIT EVIDENCE: `engine/execution/broker_router.py::_execution_degraded_from_cache()` returns `{"active": False}` when `_read_execution_health` is `None` or when it raises. Direct verification showed both missing-reader and raising-reader cases return inactive, which fails open.

TASK: Design and implement a conservative degraded-health response. When the execution-health reader is missing, unavailable, returns malformed data, or raises, return an active degraded result with severity at least `WARNING`, a stable reason code such as `execution_health_unavailable` or `execution_health_read_failed`, and diagnostic detail that does not leak secrets. Keep known critical/down/unhealthy states as `CRITICAL`. Ensure live broker routing consumes this active degraded state through the existing execution gate and blocks or warns according to existing policy.

INTEGRATION REQUIREMENTS: Do not touch kill-switch, mode/arming, real-trading allowance, or hard live gates except to make this probe more conservative. Add tests around `_execution_degraded_from_cache()` and the broker routing gate path so a missing/raising reader cannot silently allow live execution.

VERIFY: `python -m pytest tests/test_broker_router_degraded_probe_fail_closed.py -q` passes, or the equivalent test file you add; direct checks for missing and raising reader return `active=True`; existing broker-router dry-run/live gate tests still pass.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

## REM-PL-6 — Add live venv versus requirements lock dependency-drift preflight

ROLE: You are a runtime-reliability engineer adding fast dependency drift evidence.

TARGET: `/home/david/gitsandbox/system/system`

CURRENT AUDIT EVIDENCE: Static audit found no `_dependency_drift_gate`, `TRADING_SKIP_DEPENDENCY_DRIFT`, or equivalent in `engine/runtime/prod_preflight.py`. The validator checks manifests and locks, but live preflight does not compare the running interpreter's installed direct dependencies against the checked-in lock.

TASK: Design and implement a dependency-drift preflight gate. Parse direct runtime requirements from `requirements.in` using the existing parser helpers in `tools/validate_dependency_lock.py`; parse pinned expected versions from `requirements.lock.txt`; compare those direct packages with `importlib.metadata.version()` in the running interpreter. In safe/non-live mode, report warnings for missing/drifted packages. In live/production-like mode, fail closed unless an explicit documented skip env is set for diagnostics.

INTEGRATION REQUIREMENTS: Keep the gate fast, stdlib-only, and fail-soft on parser import/read errors outside strict live mode. Do not shell out to `pip freeze`. Redact paths and never print secrets. Add docs and config glossary entries for the skip env and live behavior.

VERIFY: Add `tests/runtime/test_prod_preflight_dependency_drift.py` or equivalent covering all-match, drifted version, missing package, parser failure, safe warning, live error, and skip env. Run the focused test and `python tools/validate_dependency_lock.py --strict`.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

## REM-PL-7 — Require PgBouncer tests in CI instead of allowing DSN skip

ROLE: You are a CI/backend engineer making the Postgres+Redis lane exercise PgBouncer.

TARGET: `/home/david/gitsandbox/system/system`

CURRENT AUDIT EVIDENCE: `.github/workflows/validate.yml` still contains `--allow-skip-message-regex "TS_PGBOUNCER_TEST_DSN"` in the full Postgres/Redis suite. `tests/test_pgbouncer_routing.py` skips when `TS_PGBOUNCER_TEST_DSN` and direct DSNs are absent.

TASK: Design and implement a CI path that starts or configures PgBouncer in the backend integration lane, exports `TS_PGBOUNCER_TEST_DSN` and `TS_PG_DIRECT_TEST_DSN`, and removes the allow-skip regex for PgBouncer. Reuse existing service containers where possible; add a minimal PgBouncer service/container/config only if needed. Keep all safe-mode env values and secret handling unchanged.

INTEGRATION REQUIREMENTS: The full backend suite must fail if PgBouncer tests are skipped. Do not remove unrelated allow-skip regexes. Do not hardcode real passwords; use the CI throwaway database password already present in the workflow. Update `docs/PRODUCTION_BACKEND_CI.md` with the new PgBouncer contract.

VERIFY: `grep -n "TS_PGBOUNCER_TEST_DSN" .github/workflows/validate.yml` shows env assignment but no allow-skip regex; the JUnit XML from `tools/run_required_backend_tests.py` includes the PgBouncer testcases with no skipped child; a targeted local PgBouncer run passes when DSNs are provided.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

## REM-PL-11 — Add safe commit/close helpers to broker_sim teardown paths

ROLE: You are an execution-ledger engineer hardening simulated broker teardown.

TARGET: `/home/david/gitsandbox/system/system`

CURRENT AUDIT EVIDENCE: Safe commit/close helpers exist in `engine/execution/broker_apply_orders.py`, but no broker-sim-specific safe teardown test file exists. Static search did not find `_safe_commit_connection` / `_safe_close_connection` in `broker_sim`.

TASK: Read `engine/execution/broker_sim.py` and identify every connection commit/close path in the simulated order-to-fill and idempotency marker application flow. Add small local safe-commit/safe-close helpers or reuse shared helpers if doing so does not create an import cycle. Ensure commit/close errors from already-closed SQLite connections are logged as recoverable teardown failures and do not mask the primary operation result, while real apply/transaction errors still fail.

INTEGRATION REQUIREMENTS: Do not touch live broker modules or live broker-router failover. Keep broker_sim dry-run/no-orders/already-applied gates unchanged. Add tests with stub connections whose `commit()` and `close()` raise `sqlite3.ProgrammingError("Cannot operate on a closed database.")` and healthy stubs that prove normal calls still happen exactly once.

VERIFY: `python -m pytest tests/execution/test_broker_sim_safe_teardown.py -q` passes, or the equivalent test module you add; existing broker_sim tests still pass.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

## REM-PL-12 — Map execution_mode_not_live arm refusals to HTTP 409

ROLE: You are an API engineer fixing status-code classification for expected operator refusals.

TARGET: `/home/david/gitsandbox/system/system`

CURRENT AUDIT EVIDENCE: Direct verification showed `engine.api.http_transport._map_error_to_status("execution_mode_not_live") == 500` and `operator_execution_mode_not_live == 500`. The existing business-refusal map already returns `409` for pre-trade conflicts.

IMPLEMENTATION NOTE (2026-06-26): Remediated in `engine/api/http_transport.py` by mapping `execution_mode_not_live`, `mode_not_live`, and the broader `*_not_live` refusal family to HTTP 409. The arm handler refusal payload and execution-mode safety gate remain unchanged; `tests/test_http_transport_status_contract.py` enforces the mapper and derived-payload contract.

TASK: Update `_map_error_to_status` so `execution_mode_not_live` and closely related non-live arm refusal variants return `409 Conflict`, not `500`. Preserve every existing mapping for auth, forbidden, rate limit, not found, deprecated, and true unexpected errors. Add or extend tests in the existing HTTP transport status contract module rather than inventing a parallel status system.

INTEGRATION REQUIREMENTS: Do not weaken the underlying execution-mode arm refusal. This is status-code classification only; payload shape, reason codes, and safety gates must remain unchanged.

VERIFY: `python -m pytest tests/test_http_transport_status_contract.py -q` or the focused status-map test passes, and a direct Python check prints `execution_mode_not_live 409`. Existing operator terminal/order contract tests still pass if relevant.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

## REM-ML-5A — Implement benchmarked financial text embedding and sentiment upgrade

ROLE: You are an ML/NLP engineer modernizing text-derived features without granting direct trading authority.

TARGET: `/home/david/gitsandbox/system/system`

CURRENT AUDIT EVIDENCE: `engine/nlp/encoder.py` still defaults to `ProsusAI/finbert` for sentiment and `all-MiniLM-L6-v2` for sentence embeddings. No new NLP encoder/news-flow implementation, benchmark harness, or financial embedding tests were found in the worktree.

TASK: Design and implement the ML-AI-5A recommendation. Add a pluggable financial text embedding backend layer that preserves current FinBERT, sentence-transformer, OpenAI, and hashing behavior while supporting finance-domain embedding models such as Fin-E5 or equivalent locally available finance-specialized encoders. Add sentiment encoder configuration that can select a modern finance-tuned classifier while keeping FinBERT as the conservative fallback. Build a local benchmark harness over cached news, filings, and transcripts that measures retrieval relevance, duplicate/staleness classification, entity/event clustering, and downstream feature IC/OOS contribution.

INTEGRATION REQUIREMENTS: Keep all text features PIT-safe with `availability_ts_ms`; include backend and model name in cache keys and persisted metadata so embedding spaces never mix. Optional model dependencies must import lazily and degrade cleanly when absent. Add license/model-card review metadata for any suggested default. Update feature registry metadata, env templates, docs, and operator configuration surfaces. Do not let text embeddings directly place trades; they remain features governed by existing promotion gates.

VERIFY: Add tests proving embedding namespaces are isolated by backend/model, novelty comparisons refuse mixed spaces, optional missing models degrade without crashing, cached text hashes remain stable, PIT filters exclude future text, benchmark output has enough evidence to choose a backend, and the old FinBERT/MiniLM path still works with new dependencies absent. Run the focused NLP/news-flow tests and relevant feature-registry tests.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.
