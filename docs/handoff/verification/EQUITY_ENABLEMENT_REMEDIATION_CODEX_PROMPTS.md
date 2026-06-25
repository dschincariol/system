# Equity Enablement — Remediation Deep-Dive Codex Prompts

> Derived from `EQUITY_ENABLEMENT_VERIFICATION_REPORT.md` (audit 2026-06-24). One focused prompt per
> recommendation. Prompts 1–2 are the **major** items gating an unqualified GO (each folds in the minor
> "register env vars in the allowlist" recommendation for its own workstream). Prompts 3–5 are the
> non-blocking hardening / binds-in-production recommendations (info-level in the audit).
>
> **Shared operating contract for every prompt below:** target repo `/home/david/gitsandbox/system/system`;
> activate `.venv` if present; paths relative to root. Anchor line numbers are **approximate** — locate by
> symbol name. Respect the model-vs-runtime contract and existing governance/gates. Do **not** weaken any
> execution gate, kill switch, reconcile guard, or reach a live broker `placeOrder` / `_req` POST path.
> Capture the relevant pre-change baselines (`pytest -m safety_critical`, `tools/pyright_money_path_gate.py`,
> and the named cost/realism baselines) before making changes so you can attribute any red.

---

## Prompt 1 — EQ-07: make broker_sim share-rounding match the live adapters for non-flat positions *(MAJOR — blocks unqualified GO)*

**Problem (verified by audit, file:line confirmed by direct read):** the stated whole point of EQ-07 is
sim/live execution parity, but the sim and the live adapters round **different quantities**:

- `engine/execution/broker_sim.py:~3088` rounds the **absolute target position** (`round_equity_qty(target_qty, …)`),
  then computes `delta = target_qty - cur_qty` (`~3094`).
- `engine/execution/broker_ibkr_gateway.py:~1609` and `engine/execution/broker_alpaca_rest.py:~1607` round the
  **order delta** (`round_equity_qty(delta, …)`), which is what a real broker receives.

For a fractional **non-flat** starting position this diverges: with `cur_qty=5.3`, raw target `12.4`, integer
increment → **sim final position = 12.0 but live final position = 12.3**. The parity test
`tests/test_broker_sim_share_rounding_matches_live.py` only exercises a **flat** start (`cur_qty=0` → target==delta),
which masks the bug. `EXEC_USE_SHARE_ROUNDING` defaults OFF, so this only manifests once an operator enables rounding —
but when enabled, paper ≠ live.

**Design + implement the optimal fix:**
1. Make the sim round the **same quantity the live adapters round** — i.e. round the **order delta** in
   `broker_sim.py`, not the absolute target — so the sim's resulting final position (`cur_qty + rounded_delta`)
   equals what the live IBKR/Alpaca adapters would produce for *any* starting position (flat or fractional non-flat).
   Reconcile this with the min-notional drop semantics (the live adapters apply the drop to the *order*, i.e. the
   delta) and the `EXEC_SIM_ROUNDING_BROKER` canonicalization (sim → ibkr) so the rounded increment matches the
   adapter being mirrored.
2. Preserve every other behavior: FX passthrough unchanged, the sub-min/zero-qty continue-guard, flag-OFF
   byte-for-byte legacy fractional behavior, reconcile/flatten `qty` sites left unrounded, and the unowned
   FX-lots seam left untouched.
3. Register the EQ-07 env vars in `docs/config_env_allowlist.txt` (and the glossary if applicable):
   `EXEC_USE_SHARE_ROUNDING`, `EXEC_IBKR_SHARE_INCREMENT`, `EXEC_ALPACA_SHARE_INCREMENT`,
   `EXEC_EQUITY_MIN_NOTIONAL_USD`, `EXEC_SIM_ROUNDING_BROKER`, `EXEC_SHARE_ROUNDING_DROP_SUB_MIN_NOTIONAL`,
   `EXEC_SHARE_ROUNDING_UNKNOWN_AS_EQUITY`.

**Required test (this is the acceptance proof, not optional):** extend
`tests/test_broker_sim_share_rounding_matches_live.py` with a **non-flat fractional** case (e.g. `cur_qty=5.3`)
that drives the real sim apply path and asserts the sim's final position **equals** the position the live IBKR
adapter (integer increment) and the live Alpaca adapter (fractional increment) would reach — and keep a flat-start
regression. The parity must be computed from the real rounding paths, not a re-implementation.

**Anti-fake-green traps to avoid:** do not assert parity only for the flat start; do not re-implement
`round_equity_qty` inside the test; do not round FX symbols; do not round a reconcile residual and orphan a
fractional position; do not weaken a gate or reach a live `placeOrder`/`_req` POST. `engine/execution` is
pyright-gated — capture the `tools/pyright_money_path_gate.py` baseline first and keep it green.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## Prompt 2 — EQ-02: make short-equity borrow cost bind in the default config *(MAJOR — blocks unqualified GO)*

**Problem (verified by audit):** the borrow rail is implemented correctly and *binds when enabled*
(`engine/strategy/net_after_cost_labels.py:~898` `net_value = net_value - borrow_return`;
`engine/backtest/cpcv.py:~446` `adjusted[idx] -= borrow_return`), but it is **inert in every shipped config**:
`engine/strategy/borrow_cost_model.py:~79` `borrow_cost_enabled()` returns `_env_bool("EQUITY_BORROW_COST_ENABLED", False)`
— default **False** — and the flag is set in **no** config (`.env`, `.env.example`, `deploy/` all clean), while the
EQ-02 spec mandates default `"1"`. `cpcv_borrow_cost_enabled()` (`~82`) inherits this default. Net effect: in a
default deployment, short-equity net-after-cost labels and CPCV short intervals are **optimistic by the borrow cost**.

**Design + implement the optimal fix:**
1. Make the borrow rail **default-on** to match the spec — flip the `EQUITY_BORROW_COST_ENABLED` default (and the
   inherited `CPCV_BORROW_COST_ENABLED` behavior) to enabled — while keeping the operator override intact
   (no hardcoded thresholds; the bps schedule/buckets stay operator-tunable).
2. **Carefully handle the byte-for-byte baselines.** `tests/test_cpcv_cost_realism.py` and any flag-off-dependent
   golden are sensitive: turning borrow on by default will legitimately change short-equity `net_return` / CPCV
   short intervals (that is the point), but it may flip currently-green tests that implicitly assumed borrow-off.
   Find every affected test, and for each decide and document whether it should now reflect borrow-charged values
   (update the expected numbers with evidence) or whether it must explicitly pin `EQUITY_BORROW_COST_ENABLED=0`
   to preserve a deliberately borrow-free realism baseline. Do not mask a real regression.
3. Preserve correctness guarantees: longs / non-equity / UNKNOWN unchanged; no double-counting when upstream
   supplies real borrow; Almgren-Chriss long/short symmetry unchanged; the live `broker_sim` SHORT-carry seam
   remains deferred (out of scope here unless trivially wired — if you wire it, prove sim/live parity).
4. Register the EQ-02 env vars in `docs/config_env_allowlist.txt` (and glossary):
   `EQUITY_BORROW_COST_ENABLED`, `CPCV_BORROW_COST_ENABLED`, `EQUITY_BORROW_BPS_PER_YEAR_JSON`,
   `EQUITY_BORROW_DEFAULT_BUCKET`, `EQUITY_BORROW_DTC_THRESHOLDS_JSON`, and any other `EQUITY_BORROW_*` / `CPCV_BORROW_*`.

**Required test (acceptance proof):** a test that sets **no** borrow env var (default config) and proves a
short-equity label's `net_return` is actually reduced by the borrow cost — i.e. the rail binds by default — plus
the existing flag-off and non-equity/long invariants still hold.

**Anti-fake-green traps to avoid:** do not "enable" it only in test setup; the default-config code path must bind.
Do not hardcode a numeric borrow value. Do not silently update a realism baseline without documenting why the new
number is correct. Capture the `pytest -m safety_critical`, `tests/test_cpcv_cost_realism.py`, and
`tools/pyright_money_path_gate.py` baselines before changing anything.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## Prompt 3 — EQ-09: operationalize the gov sector map so per-sector budgets actually bind on live stocks *(binds-in-production)*

**Problem (verified by audit):** `_apply_sector_budgets` (`engine/risk/portfolio_risk_engine.py:~1886`) is correct
and binds the money path **when a sector resolves** (`scale=cap/gross`, weight mutation at `~1917`, post-check
escalation to `blocked=True`). But it sources sectors from `engine/data/quiver_gov.py::sector_for_symbol`, which
returns `""` on an unseeded `gov_symbol_sector_map`. On a fresh/production DB without that map populated,
`sector_for_symbol("AAPL")` → `""`, so the rail is **inert for real stocks** (`USE_SECTOR_BUDGETS` default `"1"`,
`SECTOR_MAX_GROSS` default 0.30, but never engages). EQ-09 is intentionally decoupled from EQ-01 (it keys on
*resolved sector*, not EQUITY classification) — which means it under-covers live stocks until sectors are seeded.

**Design + implement the optimal fix:** make the sector map deterministically populated for the live equity
universe so per-sector budgets bind on real stocks in production. Design the most robust source:
1. Choose an authoritative, PIT-safe sector source for the equity universe (e.g. a checked-in reference/seed
   analogous to the SEC ticker registry used by EQ-01, and/or a registered ingestion job that populates
   `gov_symbol_sector_map`), failing **closed** (unresolved → `""`, no clamp, never a fabricated sector).
2. Wire it so EQUITY-classified symbols in the active universe get a sector available to `_sector_for` through the
   real production lookup. Keep `ensure_gov_tables` idempotent CREATE-IF-NOT-EXISTS. No look-ahead.
3. Report coverage: how many active-universe symbols now resolve to a sector vs remain unresolved.

**Required test (acceptance proof):** drive the **production** lookup (not a monkeypatched `_sector_for`) to show a
representative real stock (e.g. AAPL) resolves to a sector, and an over-concentrated sector built from real stocks
gets clamped end-to-end through `apply_portfolio_risk_engine` (gross compressed to `SECTOR_MAX_GROSS` and/or
post-check `blocked=True`).

**Anti-fake-green traps to avoid:** do not fabricate/guess sectors; unresolved must stay `""` (fail-open to no-clamp,
never fail to a wrong bucket); do not prove the bind only via a monkeypatched sector helper; no PIT look-ahead.
Register any new env vars in `docs/config_env_allowlist.txt` or `tools/validate_docs.py` will fail.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## Prompt 4 — EQ-06: fail-closed hardening of `equity_deployable_base` in reg_t without buying_power *(defense-in-depth)*

**Problem (verified by audit):** the live leverage path is correct — the engine stage
(`engine/risk/portfolio_risk_engine.py:~2073-2084`) hard-blocks reg_t when `buying_power is None` *before* sizing,
and the hard-block propagates to `blocked=True` at `~3122-3127`. **However**, the standalone helper
`engine/strategy/equity_sizing.py:~103-104` `equity_deployable_base`, called in reg_t mode with **no** buying_power,
returns `equity * max_leverage` (2.0x) using only equity — i.e. the helper itself does **not** fail closed. There is
no live-path exposure today (the engine guards it upstream), but it is a latent footgun: any future caller that uses
the helper without the engine's upstream guard would get an over-leveraged base.

**Design + implement the optimal fix:** make `equity_deployable_base` itself fail closed in reg_t when buying_power
is absent — return a conservative base (and/or an explicit "unavailable" signal in its reason) so the helper is safe
regardless of caller — without changing the already-correct live engine path (cash mode unchanged; the engine stage
must still hard-block and clamp identically).

**Required tests (acceptance proof):** a unit test asserting the helper fails closed in
reg_t-without-buying_power; and a regression proving the engine stage still hard-blocks and clamps exactly as before
(no double-relaxation, no behavior change to the passing live path).

**Anti-fake-green traps to avoid:** do not relax the engine-level hard-block to "compensate"; the existing
`tests/test_equity_leverage_hard_block.py` and `tests/test_equity_leverage_noop_superset.py` (both safety_critical)
must still pass; keep cash-mode and the PRAGMA-probed buying_power source behavior unchanged. Capture the
`tools/pyright_money_path_gate.py` baseline.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## Prompt 5 — EQ-10: remove the import-time binding footgun for the edge filter *(hardening)*

**Problem (verified by audit):** `engine/strategy/edge_filter.py:~20,~27` read `USE` and `MIN_NET_ABS_Z` from
`os.environ` at **module import** (top-level assignment). The EQ-10 tests avoid the trap by using
`monkeypatch.setattr(edge_filter, "USE", True)` on the module attribute, so they are honest today — but any future
test or runtime reconfiguration that sets these env vars **after** importing `edge_filter` would silently no-op.
This is asymmetric with `engine/runtime/config_schema.py`, which reads env at call time. The live gate
(`engine/runtime/alerts.py:~903/~917`) and the EQUITY-scope branch depend on these values.

**Design + implement the optimal fix:** read `USE` / `MIN_NET_ABS_Z` (and the
`EXEC_COST_FILTER_ASSET_CLASSES` scope) at **call time** inside `adjust_expected_z_for_costs` (or via a small
accessor it calls), so operator reconfig and setenv-after-import take effect, eliminating the silent-no-op. Behavior
must be identical when the environment is unchanged, and **defaults must stay OFF** (`ALERT_USE_EXEC_COST_FILTER`
default `"0"`, `ALERT_MIN_NET_ABS_Z` default `0.0`, scope empty ⇒ apply to all). The real env name is
`ALERT_MIN_NET_ABS_Z` (not bare `MIN_NET_ABS_Z`).

**Required test (acceptance proof):** a test that sets the env var **after** import (via `setenv`/`monkeypatch.setenv`,
**not** `setattr` on a module global) and proves the filter now activates and gates — the exact scenario that
silently no-ops today.

**Anti-fake-green traps to avoid:** the new test must not patch the module attribute (it must prove the call-time
read via env only); do not change any default to on; do not touch the Monte-Carlo block code (regression-locked,
out of scope); keep the calibration tool and the `validate_live_risk_thresholds` live-arming gate behavior intact.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.
