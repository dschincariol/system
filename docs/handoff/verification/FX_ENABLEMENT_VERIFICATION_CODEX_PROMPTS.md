# Currency / FX Enablement — Verification & Acceptance Deep-Dive Prompt (FX-00 … FX-08)

> **Goal:** independently verify that the FX enablement workstreams **FX-00 … FX-08** (from
> `docs/handoff/deep_dive_prompts/FX_ENABLEMENT_CODEX_PROMPTS.md`) were implemented **correctly,
> completely, and are wired and functioning in the wider repo** — with no missing, broken, faked-green,
> or regressed requirements. The deliverable is an **evidence-backed audit, not new feature work**.
>
> **FX is the structural twin** that the futures and crypto enablements mirror, so FX correctness has the
> widest blast radius. Two seams are **deliberately unowned** and must NOT be flagged as missing (see below).

---

## 0. Operating contract (read first — binds every section)

- **Target repo:** `/home/david/gitsandbox/system/system`. Activate `.venv` if present. Paths relative to root.
- **READ-ONLY AUDIT.** Do **NOT** modify runtime/tests/docs to make anything pass. Only writes allowed: the
  report at `docs/handoff/verification/FX_ENABLEMENT_VERIFICATION_REPORT.md` and `/tmp` scratch. Found a defect →
  **record it, do not fix it.**
- **Audit the working tree AS-IS.** Record `git status --short --untracked-files=all` + `git log --oneline -15`
  first; mark each finding committed vs working-tree-only.
- **Anchor line numbers are approximate** — re-locate by symbol name.
- **Baseline before verdicts:** `python -m pytest -q -m safety_critical 2>&1 | tail -30`,
  `python tools/pyright_money_path_gate.py`, and `python -m pytest -q 2>&1 | tail -40` as a full-suite baseline.
  Attribute reds to (i) FX change or (ii) pre-existing baseline. Never mask a red.
- **Evidence-first:** cite `file:line`, exact test assertions, literal command + exit code + key output. No
  evidence = **UNVERIFIED (= fail)**.
- **Respect intentional gating:** OANDA feed (`OANDA_ENABLED`/`FX_PAIRS_ENABLED`), FX features
  (`USE_FX_FEATURES`), FX regime (`USE_FX_REGIME`) all ship **default-off**; FX execution is **IBKR CASH/IDEALPRO,
  sim/shadow only** with **no OANDA execution adapter** (deliberately deferred). A correctly-disabled path is
  **PASS, not "missing."**
- **TWO DELIBERATELY-UNOWNED SEAMS — do NOT report as missing work:**
  - `broker_sim.py:2388` (weight→qty/lots conversion) is **NO-GO-pending-owner**; FX-05/06/07 explicitly do not
    touch it. Confirm it is **untouched** and flagged in self-audits — that is the *correct* state.
  - **Account-currency conversion** (P&L back to account ccy) is out of scope; FX-05 attaches metadata only.
- **No fake-green:** a passing test that over-mocks runtime, asserts trivially, or never drives production is a **FAIL.**

## 1. Method — five lenses per FX-0X

1. **RUNTIME ENFORCEMENT** — open each anchor at its symbol; confirm real logic in the live call path. Cite `file:line`.
2. **TESTS PRESENT & HONEST** — named test files exist, assert the specified behavior, drive the runtime, **pass**.
3. **VALIDATION COMMANDS** — run the ID's exact commands; record exit codes + key lines.
4. **ANTI-FAKE-GREEN PROBES** — actively falsify using the listed traps.
5. **WIRING & NO-REGRESSION** — feature reachable end-to-end for an FX pair; equity/crypto/futures outputs
   **byte-for-byte unchanged** (run the no-touch `git diff --stat` guards).

## 2. Verdict vocabulary

`PASS` · `PARTIAL` · `FAKE-GREEN` (highest priority) · `MISSING` · `BROKEN` · `GATED-OK` (confirm the gate, not the absence).

---

## 3. Per-requirement verification

### FX-00 — Research dossier *(docs-only)*
- **Inspect:** `docs/handoff/research/FX_ENABLEMENT_RESEARCH.md` exists; the machine-checked network-mode marker
  (`> NETWORK MODE: ONLINE` or `> NETWORK UNAVAILABLE:`) present; **no `.py` edited** under FX-00.
- **Run:** `python tools/precommit_text_guards.py ...`, `python tools/validate_docs.py`, `python tools/validate_repo.py`;
  the secret-scan grep returns **exit 1 = PASS** (no match); `test -f docs/handoff/research/FX_ENABLEMENT_RESEARCH.md`.
- **Gating (expected):** changes no runtime behavior; canonical symbol form owned by FX-02 (dossier only records it);
  offline citations marked `ASSUMPTION — confirm:` are legitimate.
- **Falsify:** (1) fabricated URLs/citations not marked as assumptions; (2) any `.py` edit smuggled in (= NO-GO);
  (3) a real secret value present (env-var **names** only).

### FX-02 — First-class FX instrument model *(keystone — verify before FX-03/05/06/07)*
- **Inspect:** `engine/data/fx_instrument.py` (`parse_fx_symbol`, `is_fx_symbol`, frozen `InstrumentMetadata`, curated `KNOWN_CCY`); `asset_map.py` (hardcoded pair tuple replaced by `is_fx_symbol`); `universe.py::get_instrument_metadata` accessor + `upsert_symbol` INSERT/UPDATE column writes; the accepted superseded migration contract in `docs/handoff/verification/FX_MIGRATION_ID_DECISION.md` (`0070_data_source_populate_evidence.id==70` and `0071_fx_instrument_metadata.id==71`); `_create_table`/`_column_type` in `storage_sqlite.py`. Must NOT touch `storage.py` or `table_classification.py`.
- **Tests:** `tests/test_fx_instrument_parser.py`, `tests/test_fx_asset_class_derivation.py`, `tests/test_fx_instrument_metadata_storage.py`, `tests/test_fx_instrument_migration.py`.
- **Run:** `parse_fx_symbol('eur_usd')` → `symbol=='EURUSD'`, `base_ccy=='EUR'`, `quote_ccy=='USD'`, `pip_size==0.0001`, `pnl_ccy=='USD'`; `parse_fx_symbol('USDJPY').pip_size==0.01` and `.pnl_ccy=='JPY'`; `parse_fx_symbol('GOOGLE') is None` and `('') is None`; `is_fx_symbol('DXY')` True, `('SPY')` False; migration imports prove `0070_data_source_populate_evidence.id==70` and `0071_fx_instrument_metadata.id==71`; `expected_migration_ids()` contiguous incl. 70 and 71; the 4 pytest files + `tests/test_schema_classification.py tests/test_storage_migrator.py tests/test_storage_sqlite_decomposition_contract.py`.
- **Gating (expected):** **FX-02 owns the canonical stored symbol form** (6-letter uppercase concatenated `EURUSD`, `DXY` as-is); `leverage_cap` is reference metadata only (enforcement is FX-05); accessor lives in `universe`, not `storage.py`; Postgres apply not runnable in sandbox. `0071_fx_instrument_metadata` is accepted because committed `0070` is data-source populate evidence and renumbering applied migration IDs is unsafe; see `FX_MIGRATION_ID_DECISION.md`.
- **Falsify:** (1) **`_column_type` trap** — `pip_size`/`contract_size`/`leverage_cap` defaulting to TEXT (must force REAL); `pnl_ccy` matching the "pnl" substring → wrongly REAL (must force TEXT); the test must pin float-vs-text round-trip; (2) parser accepting arbitrary 6-letter tickers (`GOOGLE`) instead of the curated `KNOWN_CCY` set; (3) breaking `asset_class_for_symbol` for non-FX symbols or its `ASSET_CLASS_MAP_JSON` precedence; (4) editing, deleting, or renumbering a prior migration; (5) claiming a Postgres apply that never ran.

### FX-01 — FX data source + ingestion
- **Inspect:** `engine/data/live_prices/oanda_live.py` (`OANDAPriceProvider.fetch_last_prices`); `provider_registry.py` (register `oanda`, IBKR `supports` includes `"fx"`); `default_symbols.py` (`FX_MAJOR_SEED_SYMBOLS`, `fx_pair_to_oanda_instrument`/reverse); `poll_prices.py` (`oanda_map`); `factor_ingestion.py` (`MACRO_SERIES_SPECS`); `cftc_cot.py` (`DEFAULT_COT_CONTRACT_SPECS`); `services/data_source_manager.py` (`oanda_fx` source, `oanda` account, `_test_oanda_connection`).
- **Tests:** `tests/test_oanda_live.py`, `tests/test_fx_provider_registry.py`, `tests/test_fx_universe_seed.py`, `tests/test_fx_macro_specs.py`, `tests/test_fx_cot_contracts.py`, `tests/test_fx_data_source_catalog.py`.
- **Run:** the FX-majors regex one-liner; the 6 FX pytest files; the regression set (`test_provider_registry_safe_jobs test_data_source_catalog_metadata test_data_source_provider_accounts test_ccxt_live test_cftc_cot_features`); the combined import line; `get_provider_definition('oanda')` with `OANDA_ENABLED=1` → `supports['asset_classes']==['fx']`; ruff; syntax check; `git_worktree_triage`.
- **Gating (expected):** OANDA feed **default-OFF**, read-only pricing only (**no execution adapter**); `ingest_cftc_cot` default-off; raw rows only (per-pair transforms are FX-03). Canonical env is `OANDA_ACCESS_TOKEN`. Live OANDA probe is mock-only (declared GAP). FRED 4th non-US short-rate id may be unverifiable offline → `xfail`+TODO (legitimate).
- **Falsify:** (1) `_test_oanda_connection` live branch never exercised (mock-only — that's the GAP, not a pass-claim); (2) the 4th short-rate FRED id presented as verified instead of TODO+xfail; (3) **canary token leaking** into rows/logs/evidence/`meta_json`; (4) claiming FX support not actually implemented (e.g. `"fx"` added to CCXT without an FX-capable adapter).

### FX-03 — FX feature groups + train/serve parity
- **Inspect:** `feature_registry.py` (FX `*_FEATURE_IDS` constants, `USE_FX_FEATURES` flag, `FEATURE_GROUPS`/metadata, `"fx."` in `_SNAPSHOT_PREFIXES`, `feature_set_tag_from_ids`, guarded loaders + `compute_feature_snapshot`, `asset_class` kwarg threaded through `resolve_feature_ids`/`expected_columns` and the snapshot builders); `cftc_cot.py` consume-only.
- **Tests:** `tests/test_fx_feature_groups_parity.py` (+ `tests/test_feature_registry_determinism.py`, `tests/test_cftc_cot_features.py` regression).
- **Run:** the parity one-liner (`expected_columns() == expected_columns(asset_class=None)` and `len>=100`); the FX-gate one-liner (`resolve_feature_ids(..., asset_class='FX')` drops `options_symbol.`/`social.`/`social_regime.` and keeps an `fx.`); the EQUITY-gate one-liner (drops `fx.`, keeps `social.mention_rate_z`); the 3 pytest files; `tools/system_audit.py`, `tools/validate_repo.py`, `git_worktree_triage`.
- **Gating (expected):** `fx.event_*` group is a **PERMANENT STUB** (no calendar-feed owner — always `0.0`; documented, **not** a defect); `asset_class=None` byte-identical to pre-change; `USE_FX_FEATURES` default off; `BASE_FEATURE_IDS` order unchanged; COT consume-only. **Tests must use default-registered equity-only ids** (`options_symbol.iv_rank`, `social.mention_rate_z`, `social_regime.mania_score`) — `insider_*`/`13f_*` are empty by default and prove nothing.
- **Falsify:** (1) rate/carry/DXY/cross/momentum loaders **stubbed** as "if FX-01 resolver exists" instead of computing the transform — **FX-03 owns the math**; (2) gating only asserted in tests, not enforced in `resolve_feature_ids`/snapshot builders at runtime; (3) the gate tested with **unregistered** equity-only ids (trivially passes); (4) live FX feature values never verified end-to-end (sandbox structural-zero path only — mark as GAP); (5) calling live `fetch_cot_records` instead of `resolve_cot_features`.

### FX-04 — Prediction routing, FX regime, FX-correct labels *(owns the canonical 24/5 clock)*
- **Inspect:** `engine/data/prices/fx_clock.py` (`fx_market_closed`, `fx_forward_eval_ms`, `fx_window_spans_closed_gap` — **canonical** session clock, America/New_York 17:00-ET); FX branch in `labeling.py::label_event`; FX branch in `backfill_labels_price_from_prices.py` (+ `meta_json.fx_clock_corrected` flag); `regime_stack.py` (`_load_fx_regime` merged into the **`macro`** dict, in `compute_regime_vector`); `predictor.py` (`_predict_resolved_model` hardcoded `"SPY"` fixed, `_regime_anchor_symbol`). Must NOT touch `engine/strategy/models/`, `ridge_meta.py`, `storage_pg.py`, `storage_sqlite.py`, `hmm_regime.py` training schema.
- **Tests:** `tests/test_fx_clock.py`, `tests/test_fx_labeling_clock.py`, `tests/test_fx_backfill_labels_clock.py`, `tests/test_fx_regime_layer.py`, `tests/test_fx_predictor_regime_routing.py`.
- **Run:** the 5 FX pytest files; the regression set (`test_hmm_regime test_regime_detector test_conformal_prediction test_predictor_ensemble_blending test_pit_backtest_predictor_regressions`); `tools/validate_repo.py`; `git_worktree_triage`; **empty** diff over `models/`/`ridge_meta.py`/`storage_pg.py`/`storage_sqlite.py` and `hmm_regime.py` (or additive `build_hmm_feature_map` only), except for exact owner-exempt non-FX diffs documented in `docs/handoff/verification/FX_PROTECTED_FILE_DIFF_CLASSIFICATION.md`.
- **Gating (expected):** `fx_clock.py` is the **single source of truth** that FX-06/FX-08 derive from; `USE_FX_REGIME` default off; **no schema change** (the `meta_json.fx_clock_corrected` flag rides existing sqlite JSON; PG `labels_price` must **not** get a JSON column); HMM 6-key training schema untouched; routing change is regime-context only (not model selection); FX-01 data absent in sandbox → regime degrades to bounded zeros (open dependency, not a defect).
- **Falsify:** (1) **fixed-offset clock** instead of real `zoneinfo` DST (a test must pin a known DST-transition date); (2) FX regime keys added as a new top-level `fx` layer — **invisible** to `regime_compatibility` unless merged into `macro` (the flatten only iterates macro/asset/micro/drift); (3) a silent `ALTER TABLE` on PG or a new `meta_json` column; (4) non-FX label output changed (must be byte-identical); (5) editing `predictor.py` model adapters/feature contract instead of only regime context.

### FX-05 — Currency-aware portfolio sizing + risk
- **Inspect:** `engine/strategy/fx_sizing.py` (`_fx_instrument` adapter, `fx_weight_to_notional`, `clamp_fx_weight_to_leverage`); `engine/risk/fx_leverage_caps.py` (`regulatory_leverage_cap`, `effective_leverage_cap`); `portfolio_risk_engine.py` (`_asset_class_for` wired into `_exposure_snapshot` bucketing; new `_apply_fx_leverage_caps` stage; structural shared-ccy edges in `_corr_graph_components`). Must NOT touch broker/execution/api/routes/ui/feature_registry or `storage.py` DDL.
- **Tests:** `tests/test_fx_sizing_notional.py`, `tests/test_fx_leverage_caps.py`, `tests/test_fx_portfolio_risk_sleeve.py` *(safety_critical)*, `tests/test_fx_leverage_hard_block.py` *(safety_critical)*, `tests/test_fx_currency_cluster_caps.py`.
- **Run:** the captured `safety_critical` baseline; the 5 FX pytest files; the full `safety_critical` suite; `tests/test_portfolio_risk_engine_live_thresholds.py`; the import line; `tools/validate_repo.py`; `git_worktree_triage` `{"ok": true}`.
- **Gating (expected):** FX-02 accessor field-name divergence reconciled **only** via the `_fx_instrument` adapter (accept `base_ccy`/`base_currency`, `pip_size`/`pip`, `leverage_cap`/`max_leverage`); `fx_leverage_caps.py` is the de-facto source (FX-00 caps are docs-only — the "grep for persisted artifact" branch is intentionally dead); the `broker_sim.py:2388` weight→lots seam is **FX-06's, not FX-05's** (must remain untouched); no schema change.
- **Falsify:** (1) FX weight still resolving to share count `weight*equity/price` instead of base/quote **notional + lots**; (2) a missing pair-rate treated as **pass** instead of fail-closed `fx_leverage_hard_block` (sandbox has no FX price rows — live-rate path mocked/seeded only); (3) the leverage test covering only the fail-closed branch, not the **positive correct-math** clamp arithmetic; (4) the `"FX": 0.50` sleeve not actually binding because `_asset_class_for` isn't wired into bucketing; (5) account-ccy conversion silently faked (out of scope — metadata only).

### FX-06 — FX execution + broker routing + 24/5 session
- **Inspect:** `broker_ibkr_gateway.py` (`_mk_fx_contract`, `_is_fx_symbol`, `_mk_contract_for_symbol` replacing `_mk_stock_contract` at the **4 call sites**); `broker_router.py` (`_fx_capable_broker`, `_batch_has_fx`, FX preference **without** bypassing gates); `engine/execution/fx_session.py` (`fx_session_state`, `fx_timing_adjustment` deriving boundaries from FX-04 `fx_clock.py`); integration in `execution_policy_engine.py::apply_execution_policy`. Must NOT touch `broker_sim.py:2388`, create `broker_oanda_rest.py`, or change `storage.py`.
- **Tests:** `tests/test_fx_ibkr_contract_construction.py`, `tests/test_fx_ibkr_call_sites_use_dispatcher.py`, `tests/test_fx_broker_routing.py`, `tests/test_fx_session.py`, `tests/test_fx_session_policy_integration.py`.
- **Run:** the pyright + router baselines; the 5 FX pytest files; `tests/test_broker_router_dry_run_gates.py tests/test_broker_apply_orders_modes.py`; `tools/pyright_money_path_gate.py` (no new diagnostics vs baseline); `tools/coverage_gate.py`; `git_worktree_triage`.
- **Gating (expected):** **IBKR CASH/IDEALPRO ONLY — OANDA execution adapter deliberately NOT built; do NOT create `broker_oanda_rest.py`.** No live order path reachable (all dry_run/sim/shadow; no real socket); `fx_session.py` must **derive** boundaries from FX-04's clock (not duplicate one); **the untouched `broker_sim.py:2388` weight→lots seam is REQUIRED and flagged NO-GO-pending-owner — do NOT mistake it for missing work.**
- **Falsify:** (1) an **independent UTC clock** that disagrees with FX-04's ET clock (must derive, not duplicate); (2) routing preference **bypassing** `validate_live_failover_chain`/`_execution_gate_or_block`/`_real_trading_gate_or_block`; (3) the dispatcher source-test not exercising the 4 sites' live non-dry_run branches (accepted GAP — contract change is mode-invariant); (4) the equity STK path altered (must be byte-identical, golden-compared).

### FX-07 — FX backtest realism, governance, profitability evidence
- **Inspect:** `engine/execution/fx_costs.py` (`FX_PIP_SPREAD`, `FX_SWAP_PIPS_LONG/SHORT`, `FX_PIP_SIZE` fallback, `pip_spread_bps`, `swap_bps`, `weekend_gap_bps`, `is_fx_asset_class`); `broker_sim.py::_offline_ac_cost_components` FX terms; `engine/strategy/cpcv.py` FX cost branch; `engine/strategy/fx_profitability_report.py::evaluate_fx_challengers`. Must NOT touch `broker_sim.py:2388`, gate math, UI, `storage.py`.
- **Tests:** `tests/test_fx_costs_unit.py`, `tests/test_broker_sim_fx_cost_realism.py` *(safety_critical)*, `tests/test_cpcv_fx_cost_config.py`, `tests/test_fx_gated_backtest_net_costs.py`, `tests/test_fx_profitability_report.py`, `tests/test_fx_no_promotion_bypass.py` *(safety_critical)*.
- **Run:** the pyright baseline; the 6 FX pytest files; the regression set (`test_cpcv_cost_realism test_broker_sim_contract test_gated_backtest test_promotion_guard_fdr test_champion_promotion_identity`); full `safety_critical`; `--collect-only`; `tools/pyright_money_path_gate.py`; `tools/validate_repo.py`.
- **Gating (expected):** all cost numbers are conservative **CALIBRATION-TODO placeholders** (not broker-calibrated); `asset_class` test is tag-agnostic `startswith("FX")`; pip size read from FX-02 first; **no new promotion entrypoint** (must flow through real `run_gated_backtest`/`cpcv_backtest`/`passes_promotion_gate`/`assess_challenger`); `broker_sim.py:2388` and `_exec_px` untouched; no schema change.
- **Falsify:** (1) the profitability report **reimplementing gates** instead of calling real `passes_promotion_gate`/`compute_pbo`/`assess_challenger` (bypasses governance); (2) pip→bps arithmetic not pinned to a hand-computed value (must assert exact bps for one pair, not just "cost > 0"); (3) non-FX cost output not byte-identical (regression guard required); (4) **promoting/asserting profitability** instead of a net-of-cost gate pass; (5) editing the weight→qty conversion or gate statistical math.

### FX-08 — FX surfacing in operator UI *(display-only)*
- **Inspect:** `ui/fx_format.js` (`isFxSymbol`, `pipDecimals`, `formatFxPrice`, `pipValueDisplay`, `formatLotQty`); `ui/fx_session.js` (`fxSessionStatus` mirroring FX-04 clock); FX badge in `ui/data_sources.js`; FX tiles in `ui/dashboard.js::loadPositionsExposureScreen`; terminal formatting + session indicator; `tools/run_ui_checks.mjs` allowlists (`NODE_TESTS`, `PYTEST_UI_TESTS`). Must NOT edit any backend (`engine/**`, `services/**`, `routes/**`, `storage.py`) or add read-model fields.
- **Tests:** `tests/test_fx_format.mjs`, `tests/test_fx_session.mjs`, `tests/test_fx_ui_contract.py`, `tests/test_fx_ui_no_secret_leak.py`.
- **Run:** `node --test tests/test_fx_format.mjs tests/test_fx_session.mjs`; `tests/test_fx_ui_contract.py tests/test_fx_ui_no_secret_leak.py`; `tools/check_local_asset_refs.py`; `tools/check_dashboard_ui_contract.py --node-executable "$(command -v node)"`; `npm run check:ui` + `npm run test:ui` (only if Node 20/npm 10; else BLOCKED-ENVIRONMENT — verify `node --version`/`npm --version` per session).
- **Gating (expected):** **expected sandbox outcome = every FX dashboard/terminal value hits the "data not yet available" placeholder** (upstream FX unmerged + OANDA off) — placeholders satisfy DoD **only as placeholders, not live data**; FX-05 owns `info_json`→read-model serialization, so **FX-08 is forbidden from adding read-model payload fields**; `ui/fx_session.js` is a presentation mirror of FX-04 (must not diverge); new tests must be wired into the allowlists or they silently never run.
- **Falsify:** (1) **fabricating** FX prices/positions/sleeve numbers to fill DoD checkboxes instead of the placeholder path; (2) FX logic (pip decimals, lot math, 24/5 boundaries) only in tests, not exercised by `dashboard.js`/`terminal.js` call sites; (3) `ui/fx_session.js` boundaries diverging from FX-04 (must be test-pinned boundary-equivalent); (4) a credential/secret value reaching a UI payload (canary test); (5) editing a backend read API to add missing FX-05 fields, or new tests not wired into the gate allowlists.

---

## 4. Cross-cutting verification (after all FX-0X)

1. **Single canonical clock.** Confirm `fx_clock.py` (FX-04) is the **only** FX session-boundary authority and that
   `fx_session.py` (FX-06) and `ui/fx_session.js` (FX-08) **derive** from it (boundary-equivalent), with no
   second independently-coded UTC clock. A divergence here is a silent correctness defect.
2. **Single canonical symbol/metadata source.** `fx_instrument.py` (FX-02) is the only base/quote/pip authority;
   FX-03/05/06/07 must re-key through the `universe.get_instrument_metadata` accessor (FX-05 via its `_fx_instrument`
   adapter), never an inline parser. Grep for stray inline FX parsing.
3. **The two unowned seams are correctly unowned.** Re-confirm `broker_sim.py:2388` (weight→lots) is **untouched**
   and that account-ccy conversion is **not** silently faked anywhere. Both are *expected* gaps, flagged in
   self-audits — not defects. (If either was "implemented", scrutinize it hard for incorrectness.)
4. **Classifier ordering & no cross-class bleed.** `asset_map.py` FX branch (now `is_fx_symbol`) must not
   reclassify EQUITY/CRYPTO/FUTURES/OPTION; `DXY` still FX; `SPY` still not.
5. **Default-off / no-regression invariants.** `OANDA_ENABLED` off, `USE_FX_FEATURES` off, `USE_FX_REGIME` off;
   `asset_class=None` feature parity byte-identical; non-FX cost/label/sizing output byte-identical; no schema change.
6. **Whole-suite + validators.** `pytest -q -m safety_critical`, `tools/validate_repo.py`,
   `tools/pyright_money_path_gate.py`, `tools/coverage_gate.py`, `tools/git_worktree_triage.py`,
   plus full `pytest -q` vs the captured baseline.

## 5. Report (write to `docs/handoff/verification/FX_ENABLEMENT_VERIFICATION_REPORT.md`)

- **Roll-up table:** `ID | verdict | runtime evidence (file:line) | tests pass? | validation exit codes | defects`.
- **Per-ID detail** with cited evidence for all five lenses.
- **Cross-cutting section** (single clock, single metadata source, the two unowned seams, classifier ordering,
  default-off parity, suite/validators).
- **GAP ledger:** mock-only OANDA probe, FRED short-rate id, `fx.event_*` permanent stub, `broker_sim.py:2388`
  unowned seam, account-ccy conversion, UI placeholder paths — each classified **(legitimate-gated)** vs **(real-defect)**.
- **Final GO / NO-GO** for "FX enablement is correctly and completely implemented and functioning in the wider
  repo," with the blocking list if NO-GO. GO requires: zero `FAKE-GREEN`/`BROKEN`/`MISSING`, the two unowned
  seams confirmed correctly-unowned (not faked), single-clock/single-metadata invariants intact, default-off
  parity proven, validators green vs baseline.
