# Equity Enablement — Verification & Acceptance Deep-Dive Prompt (EQ-01 … EQ-10)

> **Goal:** independently verify that the Equity enablement workstreams **EQ-01 … EQ-10** (from
> `docs/handoff/deep_dive_prompts/EQUITY_ENABLEMENT_CODEX_PROMPTS.md`) were implemented **correctly,
> completely, and are wired and functioning in the wider repo** — with no missing, broken, faked-green,
> or regressed requirements. The deliverable is an **evidence-backed audit, not new feature work**.
>
> The recurring failure mode for this program is **"enforcement in tests/docs only"** — a rail that
> *reports* a value but never *binds* the money path (budget still 1.00, borrow never subtracted from
> net return, edge filter never gates live arming). Hunt that specifically.

---

## 0. Operating contract (read first — binds every section)

- **Target repo:** `/home/david/gitsandbox/system/system`. Activate `.venv` if present. Paths relative to root.
- **READ-ONLY AUDIT.** Do **NOT** modify runtime/tests/docs to make anything pass. Only writes allowed: the
  report at `docs/handoff/verification/EQUITY_ENABLEMENT_VERIFICATION_REPORT.md` and `/tmp` scratch. Found a
  defect → **record it, do not fix it.**
- **Audit the working tree AS-IS.** Record `git status --short --untracked-files=all` + `git log --oneline -15`
  first; mark each finding committed vs working-tree-only.
- **Anchor line numbers are approximate** — re-locate by symbol name.
- **Baseline before verdicts:** capture the pre-existing `safety_critical` red set
  (`python -m pytest -q -m safety_critical 2>&1 | tail -30`), the pyright money-path baseline, and (for EQ-02)
  the CPCV cost baseline (`python -m pytest tests/test_cpcv_cost_realism.py -q 2>&1 | tail -20`). Attribute reds
  to (i) equity change or (ii) pre-existing baseline. Never mask a red.
- **Evidence-first:** cite `file:line`, exact test assertions, literal command + exit code + key output. No
  evidence = **UNVERIFIED (= fail)**.
- **Respect intentional gating:** flags default on/off per the table below; flag-off must be byte-for-byte
  unchanged. `UNIVERSE_LIFECYCLE_ENABLED` (EQ-05) and the EQ-10 live-arming requirement default **OFF** by design.
- **Known structural caveat (verify its impact):** **most real stocks classify `UNKNOWN`** today. EQ-01's
  registry is what fixes that; EQ-03/EQ-09/EQ-10 scope (corp-actions, sector budgets, edge filter) **under-covers
  real stocks unless EQ-01's registry actually binds.** Treat EQ-01 as the linchpin and verify the others' real
  coverage given EQ-01's state.
- **No fake-green:** a passing test that over-mocks runtime, asserts trivially, or never drives production is a **FAIL.**

## 1. Method — five lenses per EQ-0X

1. **RUNTIME ENFORCEMENT** — open each anchor at its symbol; confirm the rail **binds the money path** (the
   number actually changes a budget/return/order/arming decision), not just populates a report field. Cite `file:line`.
2. **TESTS PRESENT & HONEST** — named test files exist, assert the specified behavior, drive the runtime, **pass**.
3. **VALIDATION COMMANDS** — run the ID's exact commands; record exit codes + key lines.
4. **ANTI-FAKE-GREEN PROBES** — actively falsify using the listed traps.
5. **WIRING & NO-REGRESSION** — feature reachable end-to-end for a real stock; FX/crypto/futures + flag-off
   behavior **byte-for-byte unchanged** (run the no-touch `git status`/`git diff` guards).

## 2. Verdict vocabulary

`PASS` · `PARTIAL` · `FAKE-GREEN` (highest priority) · `MISSING` · `BROKEN` · `GATED-OK` (confirm the gate, not the absence).

---

## 3. Per-requirement verification

### EQ-01 — Bind asset-class classification to real stocks *(P0 linchpin)*
- **Inspect:** `asset_map.py::asset_class_for_symbol` (registry branch inserted **LAST**, before `return "UNKNOWN"`; `_load_equity_registry()` loaded once at import, behind `ASSET_MAP_USE_EQUITY_REGISTRY` default `"1"`); reuse of `default_symbols._sec_ticker_map_path`; `portfolio_risk_engine.py` (`_DEFAULT_ASSET_CLASS_BUDGETS` EQUITY value, `_apply_asset_class_budgets`, `_asset_class_for`). Seed `data/sec_company_tickers_exchange.json`.
- **Tests:** `tests/test_asset_map_equity_registry.py`, `tests/test_universe_equity_classification.py`, `tests/test_equity_budget_binds.py` *(safety_critical)*.
- **Run:** the `safety_critical` baseline; the 3 equity pytest files; the no-regression set (`test_fx_asset_class_derivation test_fx_portfolio_risk_sleeve test_portfolio_risk_engine_live_thresholds test_universe_pit`); full `safety_critical`; the import line; `tools/validate_repo.py`, `tools/coverage_gate.py`, `tools/git_worktree_triage.py`; `git status --short --untracked-files=all` (only the intended files changed).
- **Gating (expected):** `ASSET_MAP_USE_EQUITY_REGISTRY` default on; budget binding behind `PORTFOLIO_RISK_BIND_EQUITY_BUDGET` default on (binding value e.g. 0.80 `< MAX_GROSS` 1.00); flag-off restores legacy 1.00; **no schema/DDL change, no backfill migration** (re-classification only on next `upsert_symbol`); `ASSET_CLASS_MAP_JSON` override keeps priority.
- **Falsify (linchpin — be ruthless):** (1) EQUITY classified but the **budget still 1.00 == MAX_GROSS so the sleeve never binds** (the core non-binding-sleeve bug — the safety_critical test must prove a real stock is scaled down); (2) the registry branch inserted **before** FX/crypto/commodity/rates → reclassifies non-equity (must be LAST); (3) OTC/null-exchange tickers wrongly admitted to EQUITY (must stay UNKNOWN); (4) registry asserted only in tests/docs, not actually loaded into the runtime classifier.

### EQ-02 — Charge stock-borrow / financing cost on short equity
- **Inspect:** `engine/strategy/borrow_cost_model.py` (pure schedule + gates); `cpcv.py` (`_apply_transaction_costs_to_returns` per-step loop, `_cost_components_for_turnover`, `cpcv_cost_config_from_env`); `net_after_cost_labels.py` (`build_net_after_cost_label`, the **`net_return` subtraction**, `extract_borrow_financing_costs`); reads `finra_short.py` read-only; classifies via `asset_map`.
- **Tests:** `tests/test_borrow_cost_model.py`, `tests/test_cpcv_borrow_cost_realism.py` *(safety_critical)*, `tests/test_net_after_cost_borrow_short_equity.py` *(safety_critical)*.
- **Run:** the `safety_critical` + CPCV baselines; the 3 equity pytest files; `tests/test_cpcv_cost_realism.py`; full `safety_critical`; the import line; `tools/coverage_gate.py`, `tools/validate_repo.py`, `tools/pyright_money_path_gate.py`, `git_worktree_triage`; `git status --short`.
- **Gating (expected):** `EQUITY_BORROW_COST_ENABLED` default `"1"`; CPCV respects `CPCV_BORROW_COST_ENABLED`; borrow applies **SHORT + EQUITY only** (longs/non-equity/flag-off byte-identical); borrow columns already exist in the self-managed `net_after_cost_labels` table (**no `storage.py` DDL**); Almgren-Chriss long/short symmetry deliberately unchanged; live `broker_sim` SHORT-carry seam (~2389) deferred by design.
- **Falsify:** (1) **the documented CRITICAL GAP** — borrow populated into reported fields but `net_return` **NOT actually reduced** (the money-path fix is the subtraction, not the reporting); (2) borrow returning **0** because nothing upstream emits the keys / default bucket silently returns zero; (3) double-counting when upstream supplies real borrow; (4) CPCV cost-realism baseline numbers drifting when flag off (must be byte-for-byte).

### EQ-03 — Dividend + split corporate-action adjustment *(prices, labels, ETF flows)*
- **Inspect:** `engine/data/corporate_actions.py` (`ensure_corporate_actions_tables`, `corporate_action_total_return_factor`, `corporate_action_ex_dates`, fetchers); `engine/data/jobs/ingest_corporate_actions.py`; the new `0072_corporate_actions` migration (`id==72`, contiguous); the ret/label seam in `backfill_labels_price_from_prices.py` (+ `meta_json` write); `price_hygiene.py` (`is_split_like_price_jump`, suppression); `etf_flows.py::compute_flow_features` (optional `con`).
- **Tests:** `tests/test_corporate_actions_migration.py`, `tests/test_corporate_actions_pit.py`, `tests/test_corp_action_label_adjustment.py` *(safety_critical)*, `tests/test_price_hygiene_corp_action_suppression.py`, `tests/test_etf_flow_ex_dividend_suppression.py`.
- **Run:** the `safety_critical` baseline; the 5 equity pytest files; full `safety_critical`; the migration regression (`test_fundamentals_pit test_fx_instrument_migration test_storage_migrator`); the import line; `tools/validate_repo.py`, `tools/coverage_gate.py`, `git_worktree_triage`; `git status --short`.
- **Gating (expected):** this is the **one** workstream that legitimately **requires a schema addition** (`corporate_actions` via migration `0072`, id MUST be 72, contiguous); flags default `"1"` (`LABELS_USE_CORP_ACTION_ADJUSTMENT`, `PRICE_HYGIENE_USE_CORP_ACTION_CALENDAR`, `ETF_FLOW_SUPPRESS_EX_DIVIDEND`); ingest job default-OFF; `compute_flow_features(con=None)` unchanged; Polygon endpoints implemented to documented schema + fixture (no live call) is legitimate. **Coverage depends on EQ-01** (UNKNOWN stocks also key on presence of an authoritative corp-action row).
- **Falsify:** (1) corp-action adjustment **NOT applied** to the realized `ret`/labels (an ex-div drop still scores as negative return); (2) **PIT look-ahead** — using rows with `availability_ts_ms > start_ts_ms`; (3) a malformed split row **silently dropping** the discontinuity instead of failing closed (`corp_action_unparseable`); (4) ETF feature ids/numeric meaning changing when no ex-date applies (golden compare byte-for-byte); (5) migration id not contiguous/not 72.

### EQ-04 — US equity market-session / trading-hours model
- **Inspect:** `engine/execution/equity_session.py` (`equity_session_state`, `equity_timing_adjustment`, holiday/half-day table, mirroring `fx_clock.py`); integration in `execution_policy_engine.py::apply_execution_policy` (per-order loop, `_log_suppression_event`, reuses `execution_policy_audit`); classifies via `asset_map`.
- **Tests:** `tests/test_equity_session_state.py`, `tests/test_equity_session_dst.py`, `tests/test_equity_session_policy_integration.py` *(safety_critical)*, `tests/test_equity_session_non_equity_unchanged.py` *(safety_critical)*.
- **Run:** the pyright + `safety_critical` baselines; the 4 equity pytest files; full `safety_critical`; the import line; `tools/pyright_money_path_gate.py`, `tools/coverage_gate.py`, `tools/validate_repo.py`, `git_worktree_triage`; `git status --short`.
- **Gating (expected):** `EPE_EQUITY_SESSION_ENFORCE` default `"1"`, **EQUITY-only** (UNKNOWN/FX/CRYPTO unchanged); `EQUITY_SESSION_UNKNOWN_YEAR_POLICY` default `"open_rth"` (policy engine owns the fail-closed default for uncovered years); **no schema change** (reuses `execution_policy_audit`); `engine/execution` is pyright-gated (capture baseline); after-hours recalibration is a deferred residual (not implemented — legitimate).
- **Falsify:** (1) a closed-session order **not actually suppressed** / still present in the execution-ready list (enforcement observed only by calling the helper, not in engine output); (2) `"UNKNOWN"` mistakenly treated as equity; (3) **DST handled with a fixed offset** instead of zoneinfo (drift across spring-forward/fall-back); (4) holiday table fabricated/uncited or silently assuming "no holiday" for uncovered years; (5) timing bias **mutating the input decision dict** instead of returning a copy.

### EQ-05 — Detect and retire delisted / merged / renamed symbols *(survivorship + lifecycle)*
- **Inspect:** `engine/data/universe_lifecycle.py` (`staleness_retire_candidate`, `reference_retire_candidate`, `broker_tradability_retire_candidate`, `evaluate_symbol_retirement`, `retire_symbol`, `run_lifecycle_once`); `engine/data/jobs/retire_delisted_symbols.py`; `job_registry.py::ALLOWED_JOBS`; reuse of `universe.py`/`universe_pit.py`/`universe_audit`.
- **Tests:** `tests/test_universe_lifecycle_retire.py` *(safety_critical)*, `tests/test_universe_lifecycle_pit_exclusion.py` *(safety_critical)*, `tests/test_retire_delisted_symbols_job.py`.
- **Run:** the `safety_critical` baseline; the 3 equity pytest files; `tests/test_universe_pit.py`; full `safety_critical`; the import line; `tools/validate_repo.py`, `tools/coverage_gate.py`, `git_worktree_triage`; `git status --short`.
- **Gating (expected):** whole run gated `UNIVERSE_LIFECYCLE_ENABLED` default `"0"` (**OFF** — flag-off byte-identical); reference layer separately gated default `"0"`; **no schema change**; **two BLOCKING GAPS are legitimate** — no Polygon `/v3/reference/tickers` and no broker `/v2/assets` allowlist exist today, so a documented default-off no-op / fail-closed-when-creds-absent is the *correct* state; DISABLED is already sticky; PIT exclusion already works via existing inference (confirm, don't re-implement).
- **Falsify:** (1) retiring a symbol on **ABSENCE of evidence** (zero market history must NEVER retire); (2) the reference/broker layer **faking a signal** instead of failing closed when the credential/feed is absent; (3) FX/CRYPTO/COMMODITY rows wrongly retired (must skip non-EQUITY); (4) `retire_symbol` not idempotent (missing `WHERE status != 'DISABLED'`); (5) claiming PIT exclusion works without an end-to-end test against the real inference.

### EQ-06 — Pre-sizing equity leverage / buying-power guard *(Reg-T aware)*
- **Inspect:** `engine/risk/equity_leverage_caps.py` (`equity_leverage_mode`, `max_equity_leverage`); `engine/strategy/equity_sizing.py` (`equity_deployable_base`, `clamp_equity_gross_to_leverage`); `portfolio_risk_engine.py` (`_apply_equity_leverage_caps` stage + `_buying_power_reference` probe, wired after `_apply_fx_leverage_caps` before `_apply_strategy_budgets`); consumes `deployable_capital.compute_deployable_equity` read-only.
- **Tests:** `tests/test_equity_leverage_caps.py`, `tests/test_equity_sizing.py`, `tests/test_equity_buying_power_reference.py`, `tests/test_equity_leverage_hard_block.py` *(safety_critical)*, `tests/test_equity_leverage_noop_superset.py` *(safety_critical)*.
- **Run:** the `safety_critical` baseline; the 5 equity pytest files; full `safety_critical`; the FX-leverage regression (`test_fx_leverage_hard_block test_portfolio_risk_engine_live_thresholds`); the import line; `tools/validate_repo.py`, `tools/coverage_gate.py`, `git_worktree_triage`; `git status --short`.
- **Gating (expected):** `PORTFOLIO_RISK_USE_EQUITY_LEVERAGE_CAPS` default `"1"` but a strict **no-op in cash mode at gross ≤ 1.0** / no EQUITY symbol / flag off; `EQUITY_LEVERAGE_MODE` default `"cash"` (1.0), `reg_t`=2.0; **no schema change**; two `broker_account` schemas exist (broker_sim has no `buying_power`) — MUST probe `PRAGMA table_info`; `broker_sim.py:2388`, `deployable_capital.py`, `position_sizing.py` untouched.
- **Falsify:** (1) blindly reusing the **FX per-weight clamp** instead of the required **AGGREGATE `gross_notional/buying_power` clamp** (a single 0.20 weight is NOT 0.20x leverage); (2) assuming `buying_power` column exists → crash/wrong base on the broker_sim schema; (3) fail-closed branches (account_equity≤0; reg_t with no buying_power) not actually setting `blocked=True`/`block_reason` in engine output; (4) leverage diagnostics asserted only in tests, clamp not enforced in the runtime stage; (5) `_equity_reference` mistaken for a buying-power source (it only returns equity).

### EQ-07 — Per-broker share rounding, lot, and minimum-notional conventions
- **Inspect:** `engine/execution/share_rounding.py` (`equity_share_policy`, `round_equity_qty`); wiring in `broker_ibkr_gateway.py` (before order builders; reconcile sites left unrounded), `broker_alpaca_rest.py` (before submit), `broker_sim.py` (the weight→qty seam **2388/2395** for sim/live parity); classifies via `asset_map` (FX passthrough).
- **Tests:** `tests/test_share_rounding_helper.py`, `tests/test_ibkr_share_rounding_integer.py` *(safety_critical)*, `tests/test_alpaca_share_rounding.py` *(safety_critical)*, `tests/test_broker_sim_share_rounding_matches_live.py` *(safety_critical)*.
- **Run:** the pyright + `safety_critical` + broker baselines; the import; the 4 equity pytest files; the broker regression (`test_broker_sim_contract test_broker_router_dry_run_gates test_broker_apply_orders_modes`); full `safety_critical`; `tools/pyright_money_path_gate.py`, `tools/coverage_gate.py`, `git_worktree_triage`; `git status --short`.
- **Gating (expected):** gate `EXEC_USE_SHARE_ROUNDING` (implementer states default `"1"` only if proven superset, else `"0"`); `engine/execution` + `broker_router.py` pyright-gated (capture baseline); **no schema change**; **FX symbols passed through UNCHANGED** (the cross-prompt FX-lots seam at 2388 remains unowned — flag, don't fill); reconcile/flatten `qty` sites deliberately left unrounded.
- **Falsify:** (1) **sim NOT mirroring the live adapter's rounding** so paper≠live (the parity assertion is the whole point); (2) rounding FX symbols (must passthrough); (3) rounding a reconcile residual `qty` and orphaning fractional positions; (4) a dropped/0-qty order still submitted instead of hitting the existing continue-guard; (5) weakening a gate (`_execution_gate_or_block`, reconcile, kill switch) or reaching a live `placeOrder`/`_req` POST path in a test.

### EQ-08 — Fail loud on missing paid equity feeds; pin default feature-set count
- **Inspect:** `prod_preflight.py` (`_paid_equity_provider_degradation_gate()` + `main()` wiring + return-3 aggregation); `provider_registry.py::get_enabled_market_data_job_names` (free-fallback warn branch — telemetry only, **list unchanged**); consumes `health.py::provider_readiness_snapshot`/`_provider_credential_available`.
- **Tests:** `tests/test_paid_equity_provider_gate.py` *(safety_critical)*, `tests/test_provider_registry_paid_equity_downgrade_warn.py`, `tests/test_feature_default_count_parity.py`.
- **Run:** the `safety_critical` baseline; the 3 equity pytest files; the regression set (`test_provider_readiness_gates test_prod_preflight_external_services test_provider_registry_safe_jobs test_feature_registry_determinism`); full `safety_critical`; the import line; `tools/validate_repo.py`, `git_worktree_triage`, `tools/coverage_gate.py`; `git status --short`.
- **Gating (expected):** `PREFLIGHT_ENFORCE_PAID_EQUITY_PROVIDERS` default True but enforced **only in paper/live** (safe/dev = note, no error); `_EQUITY_PRICE_PROVIDERS=("polygon_ws","polygon","ibkr")` overridable; **no schema change**; **GROUND TRUTH: 111 IS the correct default feature count** (8 base + 103 unified) — docs DISAMBIGUATE, do NOT inflate; provider-registry warn is telemetry only (returned job list MUST stay byte-for-byte); feature SET unchanged (pin test only).
- **Falsify:** (1) "fixing" the doc by **inflating 111** to a larger number (the premise is wrong — 111 is right); (2) the registry warn **altering the returned job list** (must be unchanged); (3) the gate passing silently on import error instead of surfacing via `except _warn_nonfatal` (fail-closed-on-import); (4) leaking the secret VALUE instead of only the NAME `POLYGON_API_KEY`; (5) requiring a paid provider the operator never configured (false alarm — must be a no-op when not in the required set).

### EQ-09 — Enforced per-sector / factor concentration budgets
- **Inspect:** `portfolio_risk_engine.py` (`_apply_sector_budgets` wired after `_apply_fx_leverage_caps` before `_apply_strategy_budgets`; `_sector_for` adapter; `by_sector` bucket in `_exposure_snapshot`; **real** `sector_within_cap` + `sector_violations` in `_post_constraint_checks`); reads `engine/data/quiver_gov.py::sector_for_symbol` read-only.
- **Tests:** `tests/test_sector_budget_unit.py`, `tests/test_sector_budget_enforcement.py` *(safety_critical)*.
- **Run:** the `safety_critical` baseline; the 2 sector pytest files; full `safety_critical`; the regression set (`test_portfolio_risk_engine_live_thresholds test_fx_portfolio_risk_sleeve test_fx_currency_cluster_caps`); the import line; `tools/validate_docs.py`, `tools/validate_repo.py`, `git_worktree_triage`, `tools/coverage_gate.py`; `git status --short`.
- **Gating (expected):** `PORTFOLIO_RISK_USE_SECTOR_BUDGETS` default `"1"` but inert without resolvable sectors; `SECTOR_MAX_GROSS` default 0.30; optional `SECTOR_HARD_BLOCK` default `"0"` (soft clamp sufficient); new env flags MUST be registered in `docs/config_env_allowlist.txt` or `validate_docs.py` fails; gate on **resolved-sector**, not EQUITY classification (decoupled from EQ-01); `ensure_gov_tables` idempotent CREATE-IF-NOT-EXISTS allowed.
- **Falsify (brief contained errors — confirm they were corrected):** (1) flipping/asserting a `sector_within_cap` boolean that is **never enforced** (the brief's named `sector_correlation_within_cap` flag does **not** exist — must be a real check the block logic acts on); (2) sourcing sector from the wrong `equity_snapshot.py` (that's account NAV, **no sector** — must use `quiver_gov.sector_for_symbol`); (3) `_sector_for` **raising** on `con=None` instead of `_warn_nonfatal`-guarding to `""`; (4) sector scale-down not preserving signs / not asserting exact `scale=cap/gross`; (5) `by_sector` bucketing unclassified symbols (must skip empty sector).

### EQ-10 — Activate/calibrate/EQUITY-scope the cost-aware edge filter; close the live-arming gap
- **Inspect:** `engine/strategy/edge_filter.py` (`USE`, `MIN_NET_ABS_Z`, `adjust_expected_z_for_costs`, new `EXEC_COST_FILTER_ASSET_CLASSES` scope branch); `tools/calibrate_edge_filter_min_net_abs_z.py` (read-only); `config_schema.py` (`_LIVE_RISK_REQUIRED_*` lists, `live_risk_threshold_validation_snapshot`, `validate_live_risk_thresholds`). MC block path is **regression-locked, not modified**.
- **Tests:** `tests/test_edge_filter_equity_scope_and_reject.py`, `tests/test_calibrate_edge_filter_tool.py`, `tests/test_live_required_edge_filter_arming.py` *(safety_critical)*, `tests/test_mc_var_cvar_block_regression_lock.py` *(safety_critical)*.
- **Run:** the `safety_critical` + config + pyright baselines; the 4 pytest files; the regression set (`test_portfolio_risk_engine_live_thresholds test_runtime_config_schema test_monte_carlo_risk_engine_contract`); full `safety_critical`; `python tools/calibrate_edge_filter_min_net_abs_z.py --help`; `tools/validate_repo.py`, `tools/pyright_money_path_gate.py`, `tools/coverage_gate.py`, `git_worktree_triage`; `git status --short`.
- **Gating (expected):** defaults **stay off** — `ALERT_USE_EXEC_COST_FILTER` default `"0"`, `ALERT_MIN_NET_ABS_Z` default 0.0, MC thresholds default 0.0 (do NOT flip); `ALERT_EXEC_COST_FILTER_ASSET_CLASSES` default empty ⇒ apply to all; the new live requirement gated `EQUITY_EXEC_COST_FILTER_REQUIRED_IN_LIVE` default `"0"` (existing live deployments NOT suddenly blocked) AND only when `_live_risk_required`; real env name is `ALERT_MIN_NET_ABS_Z` (not bare `MIN_NET_ABS_Z`); **no schema change**; EQUITY-scope under-covers real stocks unless `ASSET_CLASS_MAP_JSON`/EQ-01 populated.
- **Falsify:** (1) the edge filter **not actually gating live arming** — asserted in test but `validate_live_risk_thresholds` doesn't raise `ConfigError` (the live-arming gap is the whole point); (2) the calibration tool **FABRICATING a threshold** instead of returning `status:"insufficient_data"`/null when the ledger is thin (sandbox likely has near-zero EQUITY fills); (3) hardcoding a numeric `ALERT_MIN_NET_ABS_Z` in code (must be operator-supplied, validator-enforced non-zero); (4) lowering/default-enabling MC thresholds or editing MC block code (out of scope — regression-lock only); (5) `USE`/`MIN_NET_ABS_Z` read at **import** so setenv-after-import tests silently no-op.

---

## 4. Cross-cutting verification (after all EQ-0X)

1. **The linchpin: does EQ-01 actually bind, and does its coverage propagate?** Prove a representative real stock
   (e.g. `AAPL`) classifies `EQUITY` **and** is scaled by a binding budget `< MAX_GROSS`. Then confirm that
   EQ-03 (corp-actions), EQ-09 (sector), EQ-10 (edge filter) actually cover that stock given EQ-01's state — or
   document precisely where UNKNOWN-classification still under-covers (that is a real coverage finding, not cosmetic).
2. **Money-path-binds, not reports.** For EQ-01 (budget), EQ-02 (`net_return`), EQ-06 (gross clamp), EQ-09
   (sector clamp), EQ-10 (live arming) — in each case prove the number **changes the decision/return/arming**, not
   just a reported field. This program's signature defect is report-without-bind.
3. **Sim/live execution parity.** EQ-04 (session suppression) and EQ-07 (share rounding) must produce **identical**
   decisions in `broker_sim` and the live adapters; confirm the parity assertions exist and pass.
4. **Classifier ordering & no cross-class bleed.** `asset_map.py` EQUITY registry branch is **LAST** and never
   reclassifies FX/CRYPTO/FUTURES/OPTION/COMMODITY/RATES; OTC/null-exchange stays UNKNOWN.
5. **Flag-off / no-regression invariants.** Every EQ flag off → byte-for-byte unchanged; non-equity untouched; the
   only legitimate schema change is EQ-03's `0072_corporate_actions` (id 72, contiguous) — confirm no other migration.
6. **Whole-suite + validators.** `pytest -q -m safety_critical`, `tools/validate_repo.py`,
   `tools/pyright_money_path_gate.py`, `tools/coverage_gate.py`, `tools/git_worktree_triage.py`,
   `tools/validate_docs.py`. Attribute reds to baseline vs change.

## 5. Report (write to `docs/handoff/verification/EQUITY_ENABLEMENT_VERIFICATION_REPORT.md`)

- **Roll-up table:** `ID | verdict | runtime evidence (file:line) | binds money path? | tests pass? | validation exit codes | defects`.
- **Per-ID detail** with cited evidence for all five lenses; flag every "reports-but-doesn't-bind" case explicitly.
- **Cross-cutting section** (EQ-01 linchpin + coverage propagation, money-path-binds, sim/live parity, classifier
  ordering, flag-off parity, suite/validators).
- **GAP ledger:** EQ-05's two blocking data-feed gaps, EQ-03 unverified Polygon endpoints, EQ-04 after-hours
  residual, UNKNOWN-classification under-coverage for EQ-03/09/10, `broker_sim.py:2388` unowned FX-lots seam —
  each classified **(legitimate-gated)** vs **(real-defect)**.
- **Final GO / NO-GO** for "Equity enablement is correctly and completely implemented and functioning in the
  wider repo," with the blocking list if NO-GO. GO requires: zero `FAKE-GREEN`/`BROKEN`/`MISSING`, **EQ-01 budget
  proven binding**, every money-path rail proven to bind (not just report), sim/live parity proven, flag-off
  parity proven, validators green vs baseline.
