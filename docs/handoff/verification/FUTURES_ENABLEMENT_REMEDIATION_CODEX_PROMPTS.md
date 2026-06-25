# Futures Enablement — Remediation Codex Deep-Dive Prompts (FUT-FIX-01 … FUT-FIX-06)

> Source: blocking list in `docs/handoff/verification/FUTURES_ENABLEMENT_VERIFICATION_REPORT.md` (NO-GO, 2026-06-24).
> One focused prompt per recommendation. Each is self-contained: feed it to a coding agent verbatim.
> All prompts are **implementation** tasks operating on `/home/david/gitsandbox/system/system` (activate `.venv`).
>
> **Repo constraints that bind every prompt** (from `CLAUDE.md`): do **not** change the runtime storage facade
> schema (`engine/runtime/storage.py`) — futures tables are created in-module via `CREATE TABLE IF NOT EXISTS`;
> preserve **train/serve parity** (`engine/strategy/feature_registry.py`); **extend** existing governance/gate
> modules rather than forking parallel frameworks; keep all futures **fail-closed / default-off** invariants
> (`FUTURES_ENABLED`, `INGEST_FUTURES_ROLLS_ENABLED`, `USE_FUTURES_FEATURES`, live execution arming preflight)
> intact; leave equity/FX/crypto/options outputs **byte-for-byte unchanged**; keep the contract multiplier/tick
> single-sourced from FUT-01 metadata (`engine/data/futures_instrument.py` → `universe.get_instrument_metadata`).
> Audit the working tree **as-is** — a partial remediation may already be present (see the report's remediation
> addendum); **verify it against the spec, do not assume it is correct or complete.**

---

## FUT-FIX-01 — CRITICAL — Wire production futures labeling to the ratio-adjusted continuous series

You are remediating the program's #1 correctness risk for futures enablement. Operate on
`/home/david/gitsandbox/system/system` (activate `.venv`).

**Defect (evidence from the verification audit):** the *registered production* label job
`engine/data/jobs/label_due_events.py` (`engine/runtime/job_registry.py:560`, pipeline stage `label`) is
roll-blind. `compute_return` (`label_due_events.py:125-139`) calls `price_at_or_after` (`:87-122`), which reads
**raw rows by literal `symbol`** from the `prices` / `price_quotes` tables and returns `(p1-p0)/p0`. There is no
continuous-symbol (`<ROOT>.c.0`) resolution, no ratio adjustment, and no roll/closed-gap forward-window skip. The
roll-aware logic that *does* exist — `engine/strategy/labeling.py::label_event` futures branch (`:90-98`) and
`_futures_window_spans_roll` (`:43-63`) — has **no production caller** (only `run_dev.py:130` and tests).
`engine/strategy/retraining_pipeline.py:182` trains from the `labels` table that `label_due_events` writes, so a
roll's artificial price gap is recorded as a real `realized_ret`, silently poisoning training targets. The
ratio-adjusted continuous series lives in `futures_continuous_bars` (produced by the `ingest_futures_rolls` job,
`engine/data/futures_roll.py:398`) and is currently consumed **only** by feature serving
(`engine/strategy/feature_registry.py:2221-2261`) — so labels and features reference different series.

**Required outcome:** production futures labels MUST be computed from the ratio-adjusted continuous series and
MUST skip label windows that cross a roll boundary or a market-closed gap. When the continuous series (or roll
calendar) is unavailable for a futures symbol, the path MUST **fail closed** — return no label — and MUST NOT
fall back to raw front-month `prices`.

**Design guidance (choose the cleanest; justify your choice):**
- In `label_due_events.py`, branch on asset class via `engine/data/asset_map.py::asset_class_for_symbol` /
  `is_futures_symbol`. For futures, resolve the canonical continuous alias and read closes from
  `futures_continuous_bars` using the canonical read helpers in `engine/data/futures_roll.py` (continuous-alias /
  continuous-close / roll-window / roll-boundary). Do not duplicate ratio-adjust math — reuse the producers.
- Reuse the existing roll/closed-gap skip logic rather than reimplementing it: prefer routing futures through
  `labeling.py::label_event`'s branch, or factor the shared guard
  (`futures_sessions.futures_window_spans_closed_gap` + roll-calendar window) into one place so there is exactly
  **one** roll-aware label implementation, not two that can drift.
- Preserve point-in-time semantics (first observable sample at-or-after the target ts).
- The equity / FX / crypto label path must be **provably unchanged** (same code, same outputs).
- Do not modify `engine/runtime/storage.py` schema or `engine/data/prices/returns.py`.

**Tests to add (must drive the PRODUCTION job, not `label_event` in isolation):**
1. A roll-correctness regression that runs `label_due_events` for a futures symbol whose raw series crosses a roll,
   and asserts the recorded `realized_ret` **equals** the continuous return and **differs** from the raw
   front-month return across that boundary.
2. A fail-closed test: continuous bars / roll calendar absent → no label is written and there is **no** raw-price
   fallback.
3. A no-regression test proving an equity symbol's label is identical to the pre-change `compute_return` output.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## FUT-FIX-02 — HIGH — Feed roll boundaries into production CPCV so roll-straddle leakage is purged

You are remediating silent roll-boundary leakage in cross-validation for futures. Operate on
`/home/david/gitsandbox/system/system` (activate `.venv`).

**Defect (evidence from the verification audit):** the CPCV roll-straddle purge is correct in isolation but is
**dead code in production**. In `engine/backtest/cpcv.py` the purge (`:186-192`) only runs when `roll_times` /
`roll_boundaries` is supplied (param defaults `None` at `:223`), and **no production
`CombinatorialPurgedKFold` call site passes it**: `engine/strategy/jobs/train_model_v2.py:116`,
`engine/strategy/jobs/backtest_cpcv.py:359`, `engine/strategy/models/lgbm_ranker.py:386`, and
`engine/strategy/meta_labeling.py:802` all construct the splitter without roll dates. So CV does not purge train
samples whose label window straddles a roll boundary for futures datasets.

**IMPORTANT module-reconciliation note:** there appear to be two CPCV modules — `engine/backtest/cpcv.py` (the one
the audit traced) and `engine/strategy/cpcv.py` (listed in `CLAUDE.md` key files; referenced by the report's
remediation addendum). Before writing code, **determine which class/module the production callers above actually
import and use**, and wire the roll-boundary feed into that one. Do not wire a module the live path never imports.

**Required outcome:** for futures datasets, every production CPCV call site MUST load roll boundaries from
`futures_roll_calendar` (via the canonical helper in `engine/data/futures_roll.py`) and pass them into the
splitter so roll-straddling train samples are purged. For non-futures datasets, behavior MUST be **identical to
baseline** (no roll dates supplied → identical splits — assert this).

**Design guidance:**
- Source roll boundaries from `futures_roll_calendar` through one canonical read helper; reuse, don't duplicate.
- Detect futures via `asset_class_for_symbol` / `is_futures_symbol`; only futures datasets get roll dates.
- Surface diagnostics when roll-aware purging is active (e.g. `futures_roll_boundary_count` /
  `futures_roll_boundaries`) so the governance/backtest record shows the embargo engaged.
- Extend the existing CV class; do **not** fork a parallel CV framework. Keep purge/embargo semantics for equity
  intact.

**Tests to add (drive the production call path, not just the splitter):**
1. For a futures dataset with known roll boundaries, assert no resulting train/test split straddles a roll (purge
   demonstrably engaged; the kept-index count drops vs no-roll).
2. Assert that with no roll dates (non-futures) the splits are byte-identical to the pre-change baseline.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## FUT-FIX-03 — LOW — Prove (and wire if missing) the futures deflated-Sharpe / promotion route on cost-adjusted returns

You are closing a fake-green test gap in the futures backtest governance gate. Operate on
`/home/david/gitsandbox/system/system` (activate `.venv`).

**Defect (evidence from the verification audit):** FUT-08's output is the live/no-live gate, but the
"futures deflated-Sharpe" test (`tests/test_futures_backtest_costs.py:109-119`) calls the generic
`engine/backtest/deflated_sharpe.py::deflated_sharpe_ratio` directly with synthetic numbers — a smoke test, not a
routing proof. `deflated_sharpe.py` is unmodified and cost-agnostic by design. Nothing proves that a futures
challenger is actually evaluated by the deflated-Sharpe / promotion gate on **cost-adjusted futures returns**
(point-value P&L incl. per-tick slippage and the two-leg roll cost that FUT-08 computes in
`engine/strategy/portfolio_backtest.py`).

**Required outcome:** demonstrate end-to-end that a futures challenger's **cost-adjusted** return stream is what
reaches the deflated-Sharpe / promotion gate, and wire it if the route does not already do so. The DSR utility
itself stays generic; the proof is about *what series is fed into it* on the futures path.

**Design guidance:**
- Extend the existing governance/promotion path (`engine/strategy/statistical_gates.py` /
  `engine/strategy/gated_backtest.py` / `engine/backtest/deflated_sharpe.py`) — do not build a parallel gate.
- Do **not** assert or imply any profitability/edge — the result is gate-conditional only.
- Keep the multiplier/tick single-sourced from FUT-01 metadata.

**Tests to add:**
- A test that runs a futures challenger through the backtest → cost-adjustment → deflated-Sharpe / promotion path
  and asserts (a) the gate is actually invoked on the futures path, and (b) the returns consumed are the futures
  **cost-adjusted** series (e.g. demonstrably differ from a zero-cost or equity-bps series).

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## FUT-FIX-04 — LOW — Add the FUT-09 roll-calendar execution-block test (runtime confirmed, test missing)

You are closing a governance test-coverage gap on the futures execution adapter. Operate on
`/home/david/gitsandbox/system/system` (activate `.venv`). **This is read-only / no-live — do not enable live
trading; drive the block function directly.**

**Defect (evidence from the verification audit):** the IBKR futures order block's roll-calendar path —
`engine/execution/broker_ibkr_gateway.py::_query_futures_roll_window_block` (`:1116-1151`, a `SELECT` from
`futures_roll_calendar`) called from `futures_order_block` (`:1254`) — has **no named test**. The existing
`tests/test_futures_roll_window_block.py` only exercises the maintenance-break and delivery-window paths. The
roll-calendar block was confirmed to fire in the audit by direct drive, but governance requires a test.

**Required outcome:** a test that seeds a `futures_roll_calendar` row and asserts `futures_order_block` returns
the roll-window block (status `futures_roll_window_blocked` / reason `futures_order_inside_roll_window`), plus a
negative case (timestamp outside any roll window → not blocked on the roll-calendar dimension).

**Design guidance:**
- Mirror the structure of the existing `tests/test_futures_roll_window_block.py`; use an in-memory sqlite with a
  `futures_roll_calendar` table seeded via the same in-module helper the runtime uses.
- Keep live execution disabled (no order placement); call the gateway block function directly.
- Do not weaken or modify any execution-safety gate to make the test pass.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## FUT-FIX-05 — MEDIUM (non-futures, blocks the validator gate) — Document `ALERT_EXEC_COST_FILTER_ASSET_CLASSES`

You are unblocking `tools/validate_repo.py`. Operate on `/home/david/gitsandbox/system/system` (activate `.venv`).

**Defect (evidence from the verification audit):** `python tools/validate_repo.py` exits 1 with
"Documentation validation failed: - 1 env var(s) read in code but undocumented ...:
`ALERT_EXEC_COST_FILTER_ASSET_CLASSES`". The variable is read in `engine/strategy/edge_filter.py` and is absent
from `.env.example`, `docs/REFERENCE_CONFIGURATION_GLOSSARY.md`, and `docs/config_env_allowlist.txt`. It is not
futures-attributable, but it blocks the validator set the acceptance gate requires.

**Required outcome:** `tools/validate_repo.py`'s documentation check passes (no undocumented env var). Do this by
documenting the variable accurately — first read `engine/strategy/edge_filter.py` to determine its real semantics
(purpose, default, allowed values / asset-class list, effect on the execution-cost filter), then add a correct
entry to whichever source(s) the validator requires (confirm by reading `tools/validate_repo.py`'s docs check —
likely all of `.env.example` + glossary + allowlist).

**Design guidance:**
- Documentation-only; do **not** change runtime behavior. The doc text must match actual code behavior, not a
  guess.
- If the variable turns out to be genuinely orphaned/dead, surface that as a finding and propose removing the read
  instead — but default to documenting it.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## FUT-FIX-06 — Pre-existing / non-futures — Triage the coverage-gate floors and the Redis-dependent safety_critical test

You are making the acceptance validator set deterministically green (or explicitly, defensibly baselined). Operate
on `/home/david/gitsandbox/system/system` (activate `.venv`). These two reds are global/pre-existing and **not**
futures-attributable, but they block a wider-repo GO under the requested validator set. Treat them as two
sub-tasks.

**Sub-task A — coverage gate.** `python tools/coverage_gate.py check` exits 1: package floors fail (engine/risk
7.27% vs 50.81%, engine/execution 13.28% vs 58.92%, engine/runtime 8.08% vs 58.49%; "remaining=198 ... new=185"
zero-covered modules). This reads a **stale/partial** coverage report rather than a fresh full run.
- Required outcome: either regenerate the coverage report from a fresh run and re-evaluate the floors, or fix the
  gate to read fresh data; if the floors genuinely fail on fresh data, document the remediation path and the
  explicit baseline. **Do not fake-green** by lowering real coverage requirements without justification.

**Sub-task B — Redis-dependent test.**
`tests/test_paper_mode_sim_fill_boot.py::test_paper_mode_boot_terminal_order_sim_fill_and_attribution` fails with a
Redis socket `TimeoutError` (`NOAUTH Authentication required` / `CACHE_REDIS_UNAVAILABLE` circuit opened) when
Redis is unavailable in the environment.
- Required outcome: make the `safety_critical` suite deterministic w.r.t. Redis. Prefer a proper test fixture /
  isolation (e.g. an in-memory / fake cache, or skip **gated on Redis-availability detection** with a clear
  marker) over an unconditional skip. Alternatively, document the required Redis service for the gate. Do not mask
  a genuine code failure — confirm the failure is purely infra before isolating it.

**Design guidance:** keep changes scoped to coverage tooling/config and test isolation; do not alter trading
runtime behavior. If you choose to baseline rather than fix, make the baseline explicit and justified in docs.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.
