# Futures Enablement — Verification & Acceptance Report (FUT-01 … FUT-10)

> **Re-audit (post-remediation), 2026-06-25.** This supersedes the prior NO-GO audit (2026-06-24) and the interim
> remediation addenda. The two blocking defects (FUT-06 roll-blind production labels, FUT-08 dead CPCV
> roll-embargo) have been **independently re-verified as fixed in production code** — not merely claimed.
> Method: fresh orchestrated audit (baseline barrier → 10 parallel per-ID agents → cross-cutting checks), with the
> two former blockers re-verified by the orchestrator directly against primary source and targeted tests re-run.
> Read-only except this report.

---

## Final verdict

## ✅ GO — for futures enablement

"Futures enablement is correctly and completely implemented and production-wired across FUT-01 … FUT-10." All ten
workstreams PASS, the program's #1 risk (roll/continuous-series correctness) is **proven fixed end-to-end in the
production call path** (not merely in green unit tests), fail-closed/default-off invariants hold, the contract
multiplier is single-sourced, and equity/FX/crypto paths are unregressed.

**Non-blocking caveats (NOT futures):** the repo-wide `tools/validate_repo.py` docs gate is still red on an
unrelated undocumented runtime env var (`ENGINE_RUNTIME_OWNER_PID`), and the global coverage gate remains a
pre-existing item. Neither is futures-attributable; both should be triaged separately for a clean **full-repo**
validator GO. There is also one **latent dead-code footgun** (`labeling.py::label_event`) worth cleaning up.

| Gate criterion for GO | Status |
| --- | --- |
| Zero FAKE-GREEN / BROKEN / MISSING | ✅ all 10 per-ID PASS |
| Roll/continuous correctness **proven** in production (not just tests) | ✅ verified (label path + CPCV) |
| Every PARTIAL justified as legitimately-gated | ✅ no PARTIALs; FUT-09 GATED-OK (live-disabled by design) |
| Fail-closed / default-off invariants intact | ✅ all 5 PASS |
| Validators green vs baseline | ⚠️ futures-clean; non-futures `validate_repo` docs + coverage reds remain |

---

## What changed since the NO-GO (independently verified)

| Former blocker | Prior | Now | Evidence (re-verified vs primary source) |
| --- | --- | --- | --- |
| **FUT-06** production labels roll-blind | 🔴 BROKEN | ✅ **PASS** | `engine/data/jobs/label_due_events.py:179-181` — `compute_return` routes futures via `_is_futures_label_symbol` to `_compute_futures_return` (`:142-176`), which reads **only** `read_ratio_adjusted_continuous_close_at_or_after` (`engine/data/futures_roll.py:459-486`, `SELECT close FROM futures_continuous_bars WHERE adj_method='ratio'`), skips roll/closed-gap windows (`futures_label_window_block_reason`), and **fails closed (returns None) with no raw front-month fallback**. Equity path (`:182-195`) unchanged. |
| **FUT-08** CPCV roll-embargo dead code | 🟠 PARTIAL | ✅ **PASS** | All 3 real production callers wire it: `backtest_cpcv.py:370,382`, `train_model_v2.py:211,252`, `meta_labeling.py:807,827` call `load_futures_roll_boundaries(...)` and pass `roll_times=` into `engine.backtest.cpcv.CombinatorialPurgedKFold` (straddle-purge `cpcv.py:186-192`). `lgbm_ranker` omits it but **excludes FUTURES** from its universe (`:102-113`) → gated, no leak. |
| **FUT-FIX-05** undocumented env var | blocked validate_repo | ✅ fixed (the targeted var) | `ALERT_EXEC_COST_FILTER_ASSET_CLASSES` now documented in `.env.example` + `docs/REFERENCE_CONFIGURATION_GLOSSARY.md`. (A *different* non-futures var now trips the same gate — see caveats.) |
| **FUT-FIX-06** Redis safety_critical test | hard fail | improved → flaky | `safety_critical` = **298 passed, exit 0** across 3 full runs + 1 isolated; the paper-mode boot test now passes in isolation and on reruns (flaky, not a stable red). |

---

## Roll-up table (re-audit)

| ID | Verdict | Production-wiring evidence (file:line) | Tests |
| --- | --- | --- | --- |
| FUT-01 | ✅ PASS | `futures_instrument.py` parser/specs; `asset_map.py:197` FUTURES-before-FX; `universe.py` fut_* dispatch; migration id==72; `storage_sqlite.py` REAL affinity | 14 pass |
| FUT-02 | ✅ PASS (gated-OK) | `futures_live.py` read-only Databento + open_interest; `poll_prices.py` production persistence; default-off/fail-closed | 13 pass |
| FUT-03 | ✅ PASS | `futures_roll.py` OI+vol roll detection, ratio back-adjust, real roll-yield; daemon supervisor/control-plane gated | 14 pass |
| FUT-04 | ✅ PASS | `futures_sessions.py` real America/Chicago DST; `price_hygiene.py` FUTURES skip; equity thresholds unchanged | 12 pass |
| FUT-05 | ✅ PASS (gated-OK) | `feature_registry.py` fut.* behind `USE_FUTURES_FEATURES` default-off; continuous/roll-yield/COT loaders; COT on real roots | pass |
| FUT-06 | ✅ PASS | **production** `label_due_events.py` → continuous series, fail-closed; multiplier in `fill_notional`; two-leg roll cost | 18 pass (drives production job) |
| FUT-07 | ✅ PASS | `portfolio_risk_engine.py`/`portfolio_risk_gate.py` multiplier in exposure+sleeve; `futures_margin.py` floored contracts, min-cap, FX convert | 7 pass |
| FUT-08 | ✅ PASS | production CPCV callers feed roll boundaries; point-value P&L + per-tick + two-leg roll cost; futures DSR route via `gated_backtest`/`statistical_gates` | 6 pass |
| FUT-09 | ✅ GATED-OK | `broker_ibkr_gateway.py` FUT/CONTFUT build + multi-layer pre-submit block (incl. roll-calendar); router enforces arming/DISABLE_LIVE_EXECUTION; **live ships disabled** | 6 + 85 regr pass |
| FUT-10 | ✅ PASS | one read-only `GET /api/data/futures/rolls`; `ui/futures_panel.js`; no order controls; token-absence asserted | 3 pass |

Cross-cutting: **#1 roll-correctness end-to-end PASS** · #2 multiplier provenance PASS · #3 classifier ordering PASS · #4 fail-closed invariants PASS · #5 whole-suite+validators (orchestrator-run — below).

---

## Orchestrator's independent re-verification (the decisive checks)

- **Roll-correctness end-to-end (the #1 risk) — PASS.** FUT-03 continuous math correct (numeric probe: continuous
  return preserves the front contract's own return and removes the artificial cross-contract jump). FUT-06: the
  **registered production label job** is guaranteed to consume the ratio-adjusted continuous series and drop the
  sample (fail-closed) when continuous bars / roll calendar are missing — confirmed by reading
  `label_due_events.py:142-181` and `futures_roll.py:459-486` directly. FUT-08: roll boundaries are loaded from
  `futures_roll_calendar` and passed into the splitter the callers actually import (`engine.backtest.cpcv`).
- **Targeted tests (orchestrator-run):** former-blocker set (`test_futures_labeling` + `test_futures_cpcv_roll_embargo`
  + `test_futures_net_after_cost` + `test_futures_backtest_costs`) → **18 passed**; full futures suite
  (`test_futures_*` + `test_ingest_futures_rolls_gating`) → **83 passed**; FX structural-twin → **13 passed**.
- **Baseline:** `safety_critical` → **298 passed, exit 0**; pyright money-path gate → exit 0; no-touch guard files
  (`returns.py`, `fx_instrument.py`, `0071` migration, `oanda_live.py`, `engine/runtime/storage.py`) all clean.
- **Validator caveat:** `tools/validate_repo.py` → exit 1, failing only `validate_docs.py` on the **non-futures**
  undocumented var `ENGINE_RUNTIME_OWNER_PID` (read in `engine/runtime/ingestion_runtime.py` + `start_system.py`).

---

## GAP ledger

| Item | Classification | Notes |
| --- | --- | --- |
| `validate_repo.py` docs gate red: `ENGINE_RUNTIME_OWNER_PID` undocumented | real-defect (**non-futures**, MEDIUM) | Document it in `.env.example` / glossary / allowlist. Not futures-attributable; blocks the full-repo validator set, not futures acceptance. |
| `coverage_gate.py check` floors | baseline blocker (**non-futures**, pre-existing) | Global; not re-run this pass; pre-existing in the NO-GO audit. |
| `safety_critical` paper-mode boot test | flaky (env/Redis) | Now passes in isolation + reruns (298 green); treat as flake, not a stable red. |
| `labeling.py::label_event` still uses caller-supplied raw series | **latent footgun** (non-production, LOW) | Dead/dev-only (sole caller `run_dev.py`, not registered). A future caller wiring it into production would silently regress to raw front-month while unit tests stay green. **Recommend deleting it or making it self-load the continuous series** like `label_due_events`. |
| `lgbm_ranker` CPCV not roll-wired | legitimate-gated | FUTURES excluded from ranker universe → no roll leakage reachable. |
| Databento vendor probe / licensing; Postgres 0072 apply; live roll daemon on prod data | legitimate-gated | Sandbox/synthetic-only; vendor entitlement remains a NO-GO-to-prod operational gate, not a code defect. |

---

## Bottom line

**GO** for futures enablement: the implementation is correct, complete, production-wired, and the roll-correctness
keystone is proven fixed end-to-end. Before declaring a clean **whole-repo** validator GO, close the two
non-futures items (document `ENGINE_RUNTIME_OWNER_PID`; resolve/baseline the coverage gate) and optionally remove
the `label_event` dead-code footgun.
