# Crypto Enablement — Verification & Acceptance Deep-Dive Prompt (CRYPTO-01 … CRYPTO-06)

> **Goal:** independently verify that the Crypto enablement workstreams **CRYPTO-01 … CRYPTO-06** (from
> `docs/handoff/deep_dive_prompts/CRYPTO_ENABLEMENT_DEEP_DIVE_PROMPTS.md`) were implemented **correctly,
> completely, and are wired and functioning in the wider repo** — with no missing, broken, faked-green,
> or regressed requirements. The deliverable is an **evidence-backed audit, not new feature work**.
>
> Crypto **mirrors the merged FX twin** (`fx_instrument.py`, `fx_clock.py`, `fx_session.py`, `fx_costs.py`,
> `fx_sizing.py`, `fx_profitability_report.py` — all real code). **There is no `crypto_instrument.py`
> keystone**: each prompt uses a local `normalize_crypto_symbol` fallback and must flag the missing canonical
> owner — confirm that flag exists and that the fallback never disagrees with `asset_map.py`.

---

## How to run this audit in Claude Code

**You are an independent acceptance auditor.** Your job is to produce an evidence-backed verdict —
**not** to make the code pass. Everything here is read-only (see the operating contract below).

**Recommended orchestration (subagent fan-out):**
1. **Plan with `TodoWrite`.** One todo per requirement ID (CRYPTO-01 … CRYPTO-06), plus `baselines`, `cross-cutting`, and
   `write report`. Keep exactly one `in_progress`; mark `completed` as you finish each.
2. **Capture baselines first** (one `Bash` batch, before any verdict): `git status --short --untracked-files=all`,
   `git log --oneline -15`, the `safety_critical` pytest baseline, and the pyright money-path baseline. These gate
   every later attribution of red → change vs pre-existing.
3. **Fan out per requirement.** For each ID, spawn a verification subagent with the **`Agent`/Task tool**
   (`general-purpose`). Launch independent IDs **in parallel — multiple Agent calls in one message**. Give each
   subagent: (a) that ID's *Inspect / Tests / Run / Gating / Falsify* block **verbatim** from Section 3, (b) the
   five-lens method + verdict vocabulary (Sections 1–2), (c) the read-only constraint, (d) the **return contract**
   below. Each subagent runs **only its own** targeted tests + per-ID one-liners — **not** the whole suite or the
   shared validators (those run **once, by you**, in the cross-cutting phase, to avoid N redundant or
   sqlite-conflicting full runs).
   - Fewer IDs (e.g. CRYPTO/FX) can be verified **inline** instead — still track with `TodoWrite` and use parallel
     `Read`/`Grep`/`Bash` calls for independent work.
4. **Synthesize.** Collect the subagent verdicts, run the cross-cutting checks + whole-suite/validators once, then
   write the report and present the in-session summary (Section 5).

**Subagent return contract** — each subagent returns exactly:
```
ID: <e.g. OPT-03>
VERDICT: PASS | PARTIAL | FAKE-GREEN | MISSING | BROKEN | GATED-OK
RUNTIME_EVIDENCE: <file:line citations proving enforcement is in the LIVE call path, not tests/docs>
TESTS: <named test files — exist? assert the specified behavior? pass? paste key assertion + exit code>
VALIDATION: <each command -> literal exit code + key output line>
FALSIFY_RESULTS: <for each listed trap: refuted / CONFIRMED-DEFECT, with evidence>
DEFECTS: <concrete, file:line, severity>
GAPS: <TODO/xfail/unowned-seam/not-run-in-sandbox -> legitimate-gated vs real-defect>
```

**Claude Code tooling notes:**
- Inspect code with **`Read` / `Grep` / `Glob`** (not `cat`/`sed`/`grep` via Bash). Use **`Bash`** only for `pytest`,
  the `tools/*.py` validators, `python -c` one-liners, and `git`.
- Make **parallel tool calls** for independent reads/commands in a single message.
- **Anchor line numbers are approximate** — locate by symbol name with `Grep`.
- **Honesty is the whole point.** Report failures plainly; never fabricate green or soften a NO-GO. A passing test
  that does not drive the runtime is a **FAIL**, not a PASS. If you cannot verify something, mark it UNVERIFIED — do
  not guess.

---

## 0. Operating contract (read first — binds every section)

- **Target repo:** `/home/david/gitsandbox/system/system`. Activate `.venv` if present. Paths relative to root.
- **READ-ONLY AUDIT.** Do **NOT** modify runtime/tests/docs to make anything pass. Only writes allowed: the
  report at `docs/handoff/verification/CRYPTO_ENABLEMENT_VERIFICATION_REPORT.md` and `/tmp` scratch. Found a
  defect → **record it, do not fix it.**
- **Audit the working tree AS-IS.** Record `git status --short --untracked-files=all` + `git log --oneline -15`
  first; mark each finding committed vs working-tree-only.
- **Anchor line numbers are approximate** — re-locate by symbol name.
- **Baseline before verdicts:** `python -m pytest -q -m safety_critical 2>&1 | tail -30` and
  `python tools/pyright_money_path_gate.py > /tmp/crypto_pyright_baseline.txt 2>&1`. Attribute reds to (i) crypto
  change or (ii) pre-existing baseline. Never mask a red.
- **Evidence-first:** cite `file:line`, exact test assertions, literal command + exit code + key output. No
  evidence = **UNVERIFIED (= fail)**.
- **Respect intentional gating (shared global constraints):**
  - **No live broker mutation, anywhere, including tests.** Crypto order paths reachable only via `dry_run=True`
    and/or sim/paper/shadow, behind the **same** execution-mode / kill-switch / pre-live-reconcile / failover gates
    that protect equities/FX — **never a new bypass**. Tests use mocks/stubs only; no real sockets (socket guard
    in `tests/conftest.py`).
  - Data enablement (CRYPTO-01) ships via a **sim/paper profile** (`deploy/profiles/crypto_sim.env.example`) and
    must NOT enable any order path; `.env.example` ships `CCXT_ENABLED=0`, `INGEST_CRYPTO_FUNDING_ENABLED=0`,
    `USE_FUNDING_FEATURES=False`.
  - `CRYPTO_LIVE_TRADING_ENABLED` (CRYPTO-06) defaults **OFF** + a conservative `CRYPTO_NOTIONAL_CAP_USD`,
    independent of and **on top of** the global gates.
  - **`engine/runtime/storage.py` schema is frozen** — no crypto prompt needs a schema change (CRYPTO-01's table
    is already in migration `0034`). Any `storage.py` DDL diff is a defect.
  - Equity **and FX** behavior must be **byte-for-byte unchanged** for non-crypto symbols (prove with golden
    comparisons, not self-consistency).
  - **The sim weight→qty seam `broker_sim.py:2435` is NO-GO-pending-owner** — CRYPTO-02/04/06 must leave it
    untouched and flag it. That untouched state is **correct, not missing work**.
  - On-chain / OI / liquidation / social ingestion is **explicitly out of scope** — recorded as future work, NOT
    stubbed as fake success.
- **No fake-green:** a passing test that over-mocks runtime, asserts trivially, or never drives production is a **FAIL.**

## 1. Method — five lenses per CRYPTO-0X

1. **RUNTIME ENFORCEMENT** — open each anchor at its symbol; confirm real logic in the live call path. Cite `file:line`.
2. **TESTS PRESENT & HONEST** — named test files exist, assert the specified behavior, drive the runtime, **pass**.
3. **VALIDATION COMMANDS** — run the ID's exact commands; record exit codes + key lines.
4. **ANTI-FAKE-GREEN PROBES** — actively falsify using the listed traps.
5. **WIRING & NO-REGRESSION** — feature reachable end-to-end for a crypto symbol; equity/FX outputs
   **byte-for-byte unchanged**; gates still consulted (no bypass).

## 2. Verdict vocabulary

`PASS` · `PARTIAL` · `FAKE-GREEN` (highest priority) · `MISSING` · `BROKEN` · `GATED-OK` (confirm the gate, not the absence).

---

## 3. Per-requirement verification

### CRYPTO-01 — Crypto data enablement, validation & parity guard *(sim/paper only)*
- **Inspect:** `provider_registry.py` (ccxt provider, `_build_ccxt`); `services/data_source_manager.py` (`crypto_funding` SourceDefinition, `_test_crypto_funding_connection`, enabled-job list, storage-table contract); `job_registry.py` (`ingest_crypto_funding` daemon); `engine/data/jobs/ingest_crypto_funding.py`; `engine/data/crypto_positioning.py::compute_positioning_features`; migration `0034_crypto_funding_positioning` (canonical, frozen); `feature_registry.py` (`USE_FUNDING_FEATURES`, `_ALL_CRYPTO_POSITIONING_FEATURE_IDS`, gated build sites); `health.py`. New: `tools/validate_crypto_data.py`, `deploy/profiles/crypto_sim.env.example`, `docs/CRYPTO_DATA_ENABLEMENT.md`.
- **Tests:** `tests/test_crypto_data_enablement_flags.py`, `tests/test_crypto_funding_pipeline_smoke.py`, `tests/test_crypto_feature_parity_guard.py`, `tests/test_crypto_funding_features.py`.
- **Run:** `python tools/validate_crypto_data.py`; the 4 pytest files; `tools/syntax_check_workspace.py`; `tools/coverage_gate.py`.
- **Gating (expected):** data-only via the **sim/paper profile** (do NOT edit live `.env`); the profile must NOT set any live-trading flag; default-off contract (`CCXT_ENABLED=0`, `INGEST_CRYPTO_FUNDING_ENABLED=0`, `USE_FUNDING_FEATURES=False`); schema frozen (`0034`); on-chain/OI/liquidation/social out of scope.
- **Falsify:** (1) `validate_crypto_data.py` **SKIP-when-offline masquerading as PASS**, or exiting 0 on a real integrity failure (must distinguish PASS/SKIP/FAIL); (2) the funding-poller test using a real exchange/socket instead of a mocked exchange (socket guard), or writing 0 rows yet reporting success; (3) positioning features **not PIT-safe** — `availability_ts_ms <= ts_ms` lookahead not enforced; an equity symbol must return zeros (no leak); (4) `USE_FUNDING_FEATURES` read at **import** → train(off)/serve(on) parity mismatch not failing closed; canary leaking into a returned feature value or log.

### CRYPTO-02 — Crypto execution path: IBKR crypto contract + router preference *(sim/paper-gated)*
- **Inspect:** `broker_ibkr_gateway.py` (`_mk_contract_for_symbol` dispatcher gains crypto `secType="CRYPTO"`/`PAXOS`; the 4 call sites NOT touched); `broker_router.py` (`_batch_has_*`/`_*_capable_broker`/`_prefer_*` crypto preference that still routes **through** `_execution_gate_or_block`/`_real_trading_gate_or_block`/`validate_live_failover_chain`); `asset_map.py::asset_class_for_symbol` crypto floor classifier.
- **Tests:** `tests/test_crypto_ibkr_contract_construction.py`, `tests/test_crypto_broker_routing.py` (+ regression `tests/test_broker_router_dry_run_gates.py`, `tests/test_broker_apply_orders_modes.py`, `tests/test_fx_ibkr_contract_construction.py`, `tests/test_fx_broker_routing.py`).
- **Run:** the pyright + router baselines; the 2 crypto pytest files; the FX/router regression set; `tools/pyright_money_path_gate.py` (no new diagnostics vs baseline); `tools/syntax_check_workspace.py`.
- **Gating (expected):** strictly **dry_run/sim/paper-gated** — same gates as equities/FX, **no new bypass**, never alters `placeOrder`/cancel/flatten mechanics; crypto execution is IBKR-`PAXOS` (or chosen broker), **NOT** a new exchange adapter (`broker_coinbase.py`/`broker_binance.py` out of scope); the `broker_sim.py:2435` seam is unowned (flag, do NOT implement).
- **Falsify:** (1) crypto routing **skips/short-circuits a gate** instead of just reordering the validated chain — a test must assert gates are still consulted; (2) `_mk_contract_for_symbol` dispatch ordering **collides crypto/FX/STK** (a crypto pair misclassified as FX `CASH`); `_is_crypto_symbol` must be True only when `asset_class_for_symbol=="CRYPTO"` AND base/quote resolves; (3) a test **not truly `dry_run=True`** / a live `placeOrder` actually invoked (must assert un-called); (4) `normalize_crypto_symbol` fallback silently disagreeing with `asset_map.py`; missing `crypto_instrument.py` owner not flagged.

### CRYPTO-03 — 24/7 crypto session/clock + asset-class-aware session handling
- **Inspect:** `engine/execution/crypto_session.py` (pure, never raises; mirrors `fx_session.py` + `fx_clock.py`); integration in `execution_policy_engine.py` (`_attach_*_session_metadata`, `apply_execution_policy`, the `EPE_CRYPTO_SESSION_ENFORCE` block); `feature_registry.py::_session_flags` asset-class branch so crypto isn't marked out-of-session on equity Asia/EU/US UTC windows.
- **Tests:** `tests/test_crypto_session.py`, `tests/test_crypto_session_policy_integration.py` (+ FX regression `tests/test_fx_session.py`, `tests/test_fx_session_policy_integration.py`).
- **Run:** the 2 crypto pytest files; the 2 FX regression files; `tools/syntax_check_workspace.py`; `tools/pyright_money_path_gate.py`.
- **Gating (expected):** crypto default = **24/7 always-open, no suppression**; maintenance-window suppression is **opt-in / default-empty / disabled** via `CRYPTO_MAINTENANCE_*`; policy integration guarded by `EPE_CRYPTO_SESSION_ENFORCE` (default on); equity/FX session logic byte-for-byte unchanged; pure module, no DB/network/schema.
- **Falsify:** (1) **24/7 not actually enforced** — crypto still suppressed on a weekend by the inherited FX/equity market-hours path (a test must pin a weekend timestamp that FX marks closed but crypto marks open); (2) feature session flags still hardcoded equity Asia/EU/US windows for crypto (crypto labeled "out of session" — no asset-class branch wired at `_session_flags`); (3) equity/FX session output **not byte-identical** (no golden capture); (4) the new crypto session feature id added but not registered for train/serve parity, or `crypto_timing_adjustment` not pure / can raise.

### CRYPTO-04 — Crypto cost realism in offline sim + CPCV gates
- **Inspect:** `engine/execution/crypto_costs.py` (mirrors `fx_costs.py`; maker/taker + spread + perp funding-carry; CALIBRATION-TODO tables + env overrides); `broker_sim.py::_offline_ac_cost_components` crypto terms (do NOT touch `_exec_px` or `:2435`); `engine/strategy/cpcv.py` (`_default_commission_bps` crypto branch, `cpcv_cost_config_from_env`); `cost_models/almgren_chriss.py::_DEFAULT_OVERRIDES` (add a CRYPTO entry); `engine/strategy/crypto_profitability_report.py`; the **unchanged** governance path (`run_gated_backtest`, `statistical_gates`, `assess_challenger`, `champion_manager`) fed cost-adjusted returns.
- **Tests:** `tests/test_crypto_costs_unit.py`, `tests/test_broker_sim_crypto_cost_realism.py`, `tests/test_cpcv_crypto_cost_config.py`, `tests/test_crypto_gated_backtest_net_costs.py`, `tests/test_crypto_profitability_report.py`, `tests/test_crypto_no_promotion_bypass.py` (+ regression `test_cpcv_cost_realism test_broker_sim_contract test_gated_backtest test_promotion_guard_fdr test_champion_promotion_identity test_fx_costs_unit`).
- **Run:** the pyright baseline; the 6 crypto pytest files; the regression set; `pytest -m safety_critical`; `tools/pyright_money_path_gate.py`; `tools/validate_repo.py` (no `--live`).
- **Gating (expected):** **simulated/offline only**; "profitable" = net of crypto costs proven through the **existing** gates (never hand-asserted); `crypto_profitability_report` reports pass/fail only and must **NOT** promote (promotion stays with `champion_manager`→`assess_challenger`); cost tables are CALIBRATION-TODO placeholders; equity/FX cost output byte-for-byte unchanged; no schema; `broker_sim.py:2435` untouched.
- **Falsify:** (1) the funding-carry term computed but **never folded into `total_bps`** (cost realism not applied — net return not `< gross`; a marginal signal fails to flip net-negative); (2) `crypto_profitability_report` quietly **promoting**, or asserting profitability by hand instead of calling real `passes_promotion_gate`/`compute_pbo`, or importing a live path; (3) `almgren_chriss._DEFAULT_OVERRIDES` still silently using equity `eta=0.142, gamma=0.314` for crypto (no CRYPTO entry); (4) `funding_carry_bps` not sign-aware (longs pay / shorts receive) or not scaling by `nights`; non-crypto `cost_config` output not byte-identical.

### CRYPTO-05 — First-class crypto model + crypto-aware regime routing
- **Inspect:** `engine/strategy/models/lgbm_ranker.py` (`_is_equity_symbol`, the training filter `if not _is_equity_symbol(sym)` — the exclusion that must be replaced/scoped); `predictor.py` (`_prediction_asset_class`, `_regime_anchor_symbol` extended for crypto/BTC anchor, `default_regime = "CRYPTO_MID"` for crypto, ranker-scope gate); `conformal.py` (reuse existing `asset:CRYPTO` pool — do NOT duplicate); `bocpd_regime_update._symbols()` includes `BTCUSD`/`ETHUSD`; `feature_registry.py` crypto positioning + bocpd features.
- **Tests:** `tests/test_crypto_model_routing.py`, `tests/test_crypto_regime_routing.py`, `tests/test_crypto_ranker_equity_parity.py`, `tests/test_crypto_promotion_governed.py` (+ governance regression `test_promotion_guard_fdr test_champion_promotion_identity`).
- **Run:** the 4 crypto pytest files; `pytest -m safety_critical`; the governance regression; `tools/pyright_money_path_gate.py`; `tools/syntax_check_workspace.py`.
- **Gating (expected):** all training/serving offline/sim; **no profitability claim**; any crypto challenger must clear `assess_challenger`/`champion_manager` exactly as equities/FX (**no new promotion entrypoint**); models propose intent only (no order authority); reuse the existing conformal `asset:CRYPTO` pool; smallest change preferred (crypto-scoped model OR cross-asset ranker group); gate math/conformal grouping/promotion mechanics/ingestion/cost/schema out of scope.
- **Falsify:** (1) crypto **still silently dropped** — the exclusion at `lgbm_ranker.py` not actually replaced; a "crypto model" registered but routing never reaches it; (2) regime routing **not actually selecting a crypto model/anchor** — `_regime_anchor_symbol`/default still returns `MID` (no BTC anchor / `CRYPTO_MID`); (3) **equity ranker output NOT byte-for-byte identical** after enabling crypto (equities regressed — parity golden missing/weak); (4) a crypto promotion path bypassing `assess_challenger`; a failing crypto challenger not returning `passed=False`; watch for NotImplementedError/TODO/xfail in the new model path.

### CRYPTO-06 — Crypto risk/sizing: fractional units, vol/leverage profile, live-enable + notional cap
- **Inspect:** `engine/strategy/crypto_sizing.py` (mirrors `fx_sizing.py`: `crypto_weight_to_notional`, `clamp_crypto_weight_to_leverage`); `portfolio_risk_engine.py` (`_DEFAULT_ASSET_CLASS_BUDGETS` keeps `"CRYPTO": 0.35`, fallback `asset_class_for_symbol`); `portfolio_risk_gate.py` (sleeve max-gross, `_sleeve`); the **live-enable gate** `CRYPTO_LIVE_TRADING_ENABLED` + `CRYPTO_NOTIONAL_CAP_USD`. `broker_sim.py:2435` weight→qty **out of scope** (flag).
- **Tests:** `tests/test_crypto_sizing_unit.py`, `tests/test_crypto_risk_integration.py`, `tests/test_crypto_live_enable_gate.py`.
- **Run:** the 3 crypto pytest files; `pytest -m safety_critical`; `pytest -q -k "fx_sizing or portfolio_risk"`; `tools/pyright_money_path_gate.py`; `tools/syntax_check_workspace.py`.
- **Gating (expected):** **`CRYPTO_LIVE_TRADING_ENABLED` defaults OFF** + conservative `CRYPTO_NOTIONAL_CAP_USD` — crypto live orders blocked unless explicitly enabled, **independent of and on top of** the global execution-mode/kill-switch; `USE_CRYPTO_LEVERAGE_CAPS` default on; sizing is **consume-only / attaches metadata** (must NOT re-size downstream orders or write into sim qty math); keep `CRYPTO: 0.35` budget; equity/FX sizing byte-for-byte unchanged; no schema; `broker_sim.py:2435` untouched.
- **Falsify:** (1) **fractional units not handled** — crypto qty rounded to whole shares/int, or `min_increment`/fractional flag absent from diagnostics; (2) `CRYPTO_LIVE_TRADING_ENABLED` off but a crypto live order **still passes** (live-enable gate not truly enforced); over-cap notional not blocked; (3) the sizing module **writing back** into the sim weight→qty math (`:2435`) instead of attaching read-only metadata; (4) equity/FX sizing not byte-identical (golden missing); `CRYPTO: 0.35` budget silently weakened; `normalize_crypto_symbol` disagreeing with `asset_map.py` / missing-keystone not flagged.

---

## 4. Cross-cutting verification (after all CRYPTO-0X)

1. **No-bypass invariant (the program's safety crux).** For CRYPTO-02 (routing) and CRYPTO-06 (live-enable), prove
   a crypto order still passes through **every** gate equities/FX use — execution-mode, kill-switch, pre-live
   reconcile, `validate_live_failover_chain`, `_real_trading_gate_or_block` — and that `CRYPTO_LIVE_TRADING_ENABLED`
   is an **additional** block on top, not a replacement. Confirm no test ever reaches a real `placeOrder`/socket.
2. **Missing-keystone consistency.** There is no `crypto_instrument.py`; every prompt's local
   `normalize_crypto_symbol` must agree with `asset_map.py::asset_class_for_symbol` on crypto membership, and each
   must flag the missing canonical owner in its self-audit. Grep for divergent crypto-symbol normalizers.
3. **24/7 actually flows.** Trace one crypto symbol on a weekend timestamp: CRYPTO-03 marks it open while FX is
   closed → feature `_session_flags` not marked out-of-session → execution policy does not suppress. A silent
   weekend suppression is a correctness defect even with green unit tests.
4. **Cost realism actually binds.** Confirm CRYPTO-04's funding-carry + maker/taker + spread land in the offline
   sim `total_bps` and in CPCV, so a marginal crypto signal flips **net-negative** through the **real** gates —
   not via a hand-written profitability assertion.
5. **The `:2435` seam is correctly unowned.** Re-confirm `broker_sim.py:2435` (weight→qty) is **untouched** across
   CRYPTO-02/04/06 and flagged — that is the *expected* state, not missing work. If it was "implemented", scrutinize hard.
6. **No-regression + schema-frozen invariants.** Equity/FX cost/label/sizing/ranker output byte-for-byte unchanged;
   `CRYPTO: 0.35` budget intact; **no `storage.py` DDL diff** (schema frozen, `0034` canonical); default-off flags
   (`CCXT_ENABLED`, `INGEST_CRYPTO_FUNDING_ENABLED`, `USE_FUNDING_FEATURES`, `CRYPTO_LIVE_TRADING_ENABLED`) confirmed off.
7. **Whole-suite + validators.** `pytest -q -m safety_critical`, `tools/validate_repo.py`,
   `tools/pyright_money_path_gate.py`, `tools/syntax_check_workspace.py`, `tools/coverage_gate.py`,
   `tools/git_worktree_triage.py`. Attribute reds to baseline vs change.

## 5. Report (write to `docs/handoff/verification/CRYPTO_ENABLEMENT_VERIFICATION_REPORT.md`)

**Deliver two things:** (1) **write** the full report to the path in the heading; and (2) **present a condensed
in-session summary** to the user in your final message — the roll-up verdict table, the GO / NO-GO line, and the
top blocking defects — so they get the verdict without opening the file.


- **Roll-up table:** `ID | verdict | runtime evidence (file:line) | gates still consulted? | tests pass? | validation exit codes | defects`.
- **Per-ID detail** with cited evidence for all five lenses.
- **Cross-cutting section** (no-bypass, missing-keystone consistency, 24/7 flow, cost realism binds, `:2435`
  correctly-unowned, no-regression/schema-frozen, suite/validators).
- **GAP ledger:** mock-only data probes, on-chain/OI/liquidation/social out-of-scope, missing
  `crypto_instrument.py` owner, `broker_sim.py:2435` unowned seam, CALIBRATION-TODO cost tables — each classified
  **(legitimate-gated)** vs **(real-defect)**.
- **Final GO / NO-GO** for "Crypto enablement is correctly and completely implemented and functioning in the
  wider repo," with the blocking list if NO-GO. GO requires: zero `FAKE-GREEN`/`BROKEN`/`MISSING`, the
  **no-bypass invariant proven**, 24/7 + cost realism proven to actually flow, equity/FX no-regression proven,
  schema frozen, the `:2435` seam confirmed correctly-unowned, validators green vs baseline.
