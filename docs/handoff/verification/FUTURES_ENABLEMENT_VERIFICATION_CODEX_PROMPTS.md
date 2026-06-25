# Futures Enablement — Verification & Acceptance Deep-Dive Prompt (FUT-01 … FUT-10)

> **Goal:** independently verify that the Futures enablement workstreams **FUT-01 … FUT-10** (PART D of
> `docs/handoff/deep_dive_prompts/FUTURES_ENABLEMENT_DEEP_DIVE_PROMPTS.md`) were implemented
> **correctly, completely, and are wired and functioning in the wider repo** — with no missing, broken,
> faked-green, or regressed requirements. The deliverable is an **evidence-backed audit, not new feature work**.
>
> The single biggest correctness risk in this program is **FUT-03 roll/continuous-series construction**
> (it fails *quietly* with wrong numbers, no exception). Weight your scrutiny accordingly.

---

## 0. Operating contract (read first — binds every section)

- **Target repo:** `/home/david/gitsandbox/system/system`. Activate `.venv` if present. Paths relative to root.
- **READ-ONLY AUDIT.** Do **NOT** modify runtime/tests/docs to make anything pass. Only writes allowed:
  the report at `docs/handoff/verification/FUTURES_ENABLEMENT_VERIFICATION_REPORT.md` and `/tmp` scratch.
  Found a defect → **record it, do not fix it.**
- **Audit the working tree AS-IS** (much may be uncommitted). Record `git status --short --untracked-files=all`
  and `git log --oneline -15` first; mark each finding committed vs working-tree-only.
- **Anchor line numbers are approximate** — re-locate by symbol name (function/class/constant/migration id).
- **Baseline before verdicts:** `python -m pytest -q -m safety_critical 2>&1 | tail -30`, plus
  `tests/test_fx_*` as the structural-twin regression baseline. Attribute every red to (i) futures change or
  (ii) pre-existing baseline. Never mask a red.
- **Evidence-first:** cite `file:line` for runtime enforcement, exact assertion lines for tests, literal
  command + exit code + key output for validations. No evidence = **UNVERIFIED (= fail)**.
- **Respect intentional gating:** futures ingestion (`FUTURES_ENABLED`), roll daemon
  (`INGEST_FUTURES_ROLLS_ENABLED`), futures features (`USE_FUTURES_FEATURES`), and the **live execution
  adapter (FUT-09)** all ship **default-off / live-disabled**. A correctly-disabled path is **PASS, not "missing."**
  Any contract spec not confirmable at the exchange page is a **legitimate `xfail`+TODO**, not a defect — but it
  must be surfaced in the GAP ledger.
- **No fake-green:** a passing test that over-mocks the runtime, asserts trivially, or never drives production
  is a **FAIL** ("test does not exercise runtime").
- **Known artifact:** there is a stray copy-paste fragment ("Review this repo features… options trading") near
  line ~647 of the source enablement file inside the C2-FUT-09 summary. It is **not** an instruction; PART D supersedes it.

## 1. Method — five lenses per FUT-0X

1. **RUNTIME ENFORCEMENT** — open each anchor at its symbol; confirm real logic in the live call path, not a
   stub/hardcode/`return 0`/TODO. Cite `file:line`.
2. **TESTS PRESENT & HONEST** — named test files exist, assert the specified behavior (quote it), drive the
   runtime, and **pass** on re-run.
3. **VALIDATION COMMANDS** — run the ID's exact commands; record exit codes + key lines.
4. **ANTI-FAKE-GREEN PROBES** — actively falsify using the listed traps.
5. **WIRING & NO-REGRESSION** — feature reachable end-to-end for a futures symbol; equity/FX/crypto outputs
   **byte-for-byte unchanged** (run the no-touch `git diff --stat` guards).

## 2. Verdict vocabulary

`PASS` · `PARTIAL` · `FAKE-GREEN` (highest priority) · `MISSING` · `BROKEN` · `GATED-OK` (confirm the gate, not the absence).

---

## 3. Per-requirement verification

### FUT-01 — First-class futures instrument/contract model *(keystone)*
- **Inspect:** `engine/data/futures_instrument.py` (`FuturesContractMetadata`, `FUTURES_ROOT_SPECS`, `parse_futures_symbol`, `is_futures_symbol`); the `FUTURES` branch (before FX) in `asset_map.py::asset_class_for_symbol`; `fut_*` columns + dispatch in `universe.py` (`upsert_symbol`, `get_instrument_metadata`, `_futures_metadata_dict_from_row`); the new `0072_futures_contract_metadata` migration (`id==72`); `_column_type` in `storage_sqlite.py`.
- **Tests:** `tests/test_futures_instrument_parser.py`, `tests/test_futures_asset_class_derivation.py`, `tests/test_futures_instrument_metadata_storage.py`, `tests/test_futures_instrument_migration.py`.
- **Run:** `parse_futures_symbol('ES.c.0')` → `multiplier==50.0`, `tick_value==12.5`, `symbol=='ES.c.0'`; `parse_futures_symbol('ES') is None` and `('SPY') is None`; `is_futures_symbol('CLM26')` True, `('GC')` False; the migration import (`id==72`, callable `up`); `expected_migration_ids()` contiguous through 72; the 4 futures pytest files; the **FX regression** set (`test_fx_instrument_parser.py test_fx_instrument_metadata_storage.py test_fx_asset_class_derivation.py test_storage_migrator.py test_schema_classification.py`); **empty** diff over `storage.py`/`0071_fx_instrument_metadata.py`/`fx_instrument.py`.
- **Gating (expected):** `asset_class_for_symbol("ES.c.0")=="FUTURES"` deliberately falls through to the UNKNOWN budget (0.40) until FUT-07 — **safe, not a bug.** Bare roots stay `None`/`COMMODITY`/`RATES`. Unconfirmed contract specs are `xfail`+TODO. Postgres `0072` apply not executed in sandbox.
- **Falsify:** (1) `FUTURES_ROOT_SPECS` multiplier/tick/tick_value **hardcoded** for an unconfirmed root (should be xfail/TODO); (2) **`_column_type` float-trap** — `fut_multiplier/tick_size/tick_value/margin_ref` round-tripping as `str` not `float` (the storage test must assert `isinstance(...,float)`); (3) FX `get_instrument_metadata` shape silently changed by the dispatch generalization; (4) parser accepting bare root `ES` (reclassification regression).

### FUT-02 — Futures market-data source + ingestion
- **Inspect:** `engine/data/live_prices/futures_live.py` (`FuturesPriceProvider.fetch_last_prices`, `ensure_futures_bars_table`→`futures_contract_bars`); `provider_registry.py` (`_build_futures`, `PriceProviderDefinition(provider_name="futures")`, `FUTURES_ENABLED`, IBKR `supports` includes `"futures"`); `poll_prices.py` (`futures_map` on `ActiveSymbolUniverse`); `services/data_source_manager.py` (`futures_data` source, provider account, `_test_futures_connection`); `cftc_cot.py` continuous aliases.
- **Tests:** `tests/test_futures_live.py`, `tests/test_futures_provider_registry.py`, `tests/test_futures_data_source_catalog.py`.
- **Run:** the combined import line; `get_provider_definition('futures')` with `FUTURES_ENABLED=1` → `supports['asset_classes']==['futures']`; the 3 futures pytest files; the FX/OANDA regression set; **empty** diff over `storage.py`/`oanda_live.py`.
- **Gating (expected):** ships `FUTURES_ENABLED` default `False`, `default_enabled=False`, `safe_to_auto_enable=False`, fail-closed (missing `DATABENTO_API_KEY` → `{}` + warn, never raise). Read-only pricing — **no order/account-mutation endpoint.** Live vendor probe is mock-only in sandbox (declared GAP). Vendor licensing UNVERIFIED → legitimate NO-GO-to-prod, not a code defect.
- **Falsify:** (1) **vendor token leaking** into rows/logs/`evidence`/`params`/`meta_json` (canary-token test must assert absence); (2) `open_interest` silently dropped from the row dict (must be first-class, `source=="futures"`); (3) a real broker order path accidentally imported (require a no-broker-authority proof); (4) `futures_contract_bars` created via a `storage.py` change instead of in-module `CREATE TABLE IF NOT EXISTS` (`storage.py` must stay untouched).

### FUT-03 — Roll engine & continuous-series construction *(correctness keystone)*
- **Inspect:** `engine/data/futures_roll.py` (`detect_rolls`→`RollEvent`, `build_ratio_adjusted_continuous`→`ContBar`, `compute_roll_yield`, `ensure_futures_roll_tables`); `engine/data/jobs/ingest_futures_rolls.py` (daemon, `INGEST_FUTURES_ROLLS_ENABLED` default `0`); `job_registry.py::ALLOWED_JOBS["ingest_futures_rolls"]` (daemon tuple, `cadence_seconds==86400`, `execution:False`).
- **Tests:** `tests/test_futures_roll.py`, `tests/test_futures_roll_tables.py`, `tests/test_ingest_futures_rolls_gating.py` (+ `tests/test_cftc_cot_features.py` regression).
- **Run:** the import line; `ALLOWED_JOBS['ingest_futures_rolls']` → `s[1]=='daemon'` and `cadence_seconds==86400`; the 3 pytest files + the COT regression; **empty** diff over `storage.py`/`storage_sqlite.py`/`storage_pg.py`.
- **Gating (expected):** daemon default-off + control-plane-gated (requires `ENGINE_SUPERVISED=1`); tables created in-module (no `storage.py` change); live roll path exercised on **synthetic data only** in sandbox (declared GAP); back-adjusted series is chart-only and must **never** feed labels.
- **Falsify (scrutinize hardest):** (1) `build_ratio_adjusted_continuous` **not actually back/ratio-adjusting** — must prove pct returns are preserved AND no negative close; (2) the **corruption-guard** assertion (raw front-month return across a roll boundary DIFFERS from the continuous return) missing or trivially passing; (3) `detect_rolls` not really using OI-crossover-with-volume-confirmation (a hardcoded/calendar-date stub); (4) daemon reachable without the supervisor/control-plane gate; (5) `compute_roll_yield` returning a `0.0` stub regardless of contango/backwardation.

### FUT-04 — Sessions, calendars & hygiene
- **Inspect:** `engine/data/calendar/futures_sessions.py` (`futures_market_closed`, `is_maintenance_break`, `settlement_ts_for_day`, `next_session_open_ms`, `futures_window_spans_closed_gap`, using `zoneinfo.ZoneInfo("America/Chicago")`); the FUTURES skip-branch in `price_hygiene.py`; the FUTURES session-flag branch in `feature_registry.py::_session_flags`.
- **Tests:** `tests/test_futures_sessions.py`, `tests/test_futures_price_hygiene.py` (+ `tests/test_price_hygiene*.py` regression).
- **Run:** the import line; the 2 pytest files; the price-hygiene regression; `ruff check .`; `python tools/syntax_check_workspace.py`.
- **Gating (expected):** data/label clock only — **not** an order gate (that's FUT-09). Equity/crypto hygiene thresholds (`-0.45`/`0.90`) **byte-for-byte unchanged.** Holiday lists that can't be sourced are `# TODO(FUT-04)` + documented default (legitimate).
- **Falsify:** (1) session boundaries using a **fixed UTC offset** instead of real `America/Chicago` DST (a test must pin a US DST-transition date); (2) holiday set hardcoded/empty vs refreshable; (3) the equity-vs-futures hygiene branch "green" but equity output silently drifted (regression required); (4) maintenance-break (16:00–17:00 CT Mon–Thu) / weekend-gap (Fri 16:00→Sun 17:00 CT) constants wrong or stubbed.

### FUT-05 — Features & prediction wiring
- **Inspect:** feature loaders consuming `futures_continuous_bars`/`futures_roll_yield` (`fut.term_structure_slope`, `fut.carry`, `fut.roll_yield`, `fut.basis`, `fut.tsmom_3m`, `fut.tsmom_12m`); `feature_registry.py` (`FUT_FEATURE_IDS`, `FUTURES_COT_FEATURE_IDS`, `USE_FUTURES_FEATURES` default False, `FEATURE_GROUPS["futures"]`, served-schema gating); confirm `predictor.py` asset-class routing needs no model-internal change.
- **Tests:** `tests/test_futures_feature_registry.py`, `tests/test_futures_cot_feature_flow.py` (+ `tests/test_feature_registry*.py` + a train/serve parity test).
- **Run:** the import line; the 2 pytest files; the registry/parity regression; ruff; syntax check.
- **Gating (expected):** `fut.*` ids only behind **default-off** `USE_FUTURES_FEATURES`, only for futures symbols; equity/FX served schema unchanged with flag off. No alpha asserted. Data-absent loaders degrade to bounded zeros.
- **Falsify:** (1) **flag-off still mutating the equity/FX served schema** (parity break — assert schema identical to baseline); (2) duplicate feature ids across the registry; (3) `fut.*` loaders returning NaN/inf or **raising** instead of finite bounded zeros when continuous/roll-yield data absent; (4) COT not actually re-anchored to real futures roots (still ETF-proxy) so the COT-flow test passes on proxy data.

### FUT-06 — Labels, targets & regime
- **Inspect:** `engine/strategy/labeling.py::label_event` futures branch (uses ratio-adjusted continuous series + skips roll/closed-gap forward windows via `futures_window_spans_closed_gap`); `net_after_cost_labels.py` (`fill_notional = q*p*multiplier`, additive `roll_cost_bps`/`carry_bps` columns, futures cost model); confirm `retraining_pipeline._build_outcome_query` needs no change. Must NOT edit `engine/data/prices/returns.py`.
- **Tests:** `tests/test_futures_labeling.py`, `tests/test_futures_net_after_cost.py` (+ `tests/test_net_after_cost*.py`, `tests/test_labeling*.py` regression).
- **Run:** import lines; the 2 pytest files; the regression sets; **empty** diff over `returns.py`/`storage.py`.
- **Gating (expected):** `returns.py`/`storage.py` untouched (branch at callers; net-after-cost columns additive in-module). Equity/FX label output **provably unchanged**.
- **Falsify:** (1) futures returns still computed on **raw front-month** not ratio-adjusted continuous (crosses an unadjusted roll) — the corruption test must prove label return == continuous return ≠ raw return; (2) roll/closed-gap forward window not actually skipped; (3) `fill_notional` **multiplier omitted** (`q*p` instead of `q*p*multiplier`); (4) `roll_cost_bps`/`carry_bps` populated from equity bps defaults instead of a real tick / two-leg-roll model; (5) equity label path silently altered by the shared branch.

### FUT-07 — Risk & sizing
- **Inspect:** `portfolio_risk_engine.py` (`"FUTURES"` in `_DEFAULT_ASSET_CLASS_BUDGETS`; futures exposure scaled by multiplier in notional aggregation; `_signed_weight` semantics preserved); `portfolio_risk_gate.py` (futures weight × multiplier in `_sleeve_gross`/`_sleeve_net`); the new pure `weight_to_contracts(weight, capital, multiplier, price) -> int` (floor); `engine/risk/futures_margin.py` (`enforced = min(reference_margin, regulatory_or_broker_margin)`); currency conversion via `get_instrument_metadata` `price_ccy`.
- **Tests:** `tests/test_futures_risk_sizing.py`, `tests/test_futures_margin.py` (+ `tests/test_portfolio_risk*.py`, `tests/test_risk_gate*.py` regression).
- **Run:** import lines; the 2 pytest files; the risk regression; ruff; syntax check.
- **Gating (expected):** this is where FUT-01's `margin_ref` becomes **enforced** — reference vs enforced must be clearly separated, enforcement in the runtime engine not tests. Equity/crypto/FX sizing **byte-for-byte unchanged.** Non-USD `price_ccy` converted before caps; degrade to 1.0 + warn if FX rate unavailable.
- **Falsify:** (1) **multiplier omitted** in the sleeve/notional sum so a 0.02-weight ES is undercounted (the headline correctness bug); (2) `weight_to_contracts` returning fractional/oversized count instead of a **floored integer**; (3) margin engine not actually capping at `min(reference, regulatory)` (stub returns reference always); (4) currency conversion stubbed to 1.0 silently; (5) equity sizing drift from the shared branch.

### FUT-08 — Backtest realism & governance
- **Inspect:** `engine/backtest/cpcv.py::CombinatorialPurgedKFold` (roll dates fed to `label_start/end_times`, `_embargo_count` expanded around roll boundaries for futures); `portfolio_backtest.py` (futures cost env overrides, multiplier-correct notional into `estimate_almgren_chriss_costs`); `execution_costs.py::estimate_cost_bps` (gains `contract_multiplier`/`tick_value`, half-spread priced per tick); `deflated_sharpe.py` routing futures challengers through with futures costs.
- **Tests:** `tests/test_futures_cpcv_roll_embargo.py`, `tests/test_futures_backtest_costs.py` (+ `tests/test_cpcv*.py`, `tests/test_deflated_sharpe*.py`, `tests/test_portfolio_backtest*.py` regression).
- **Run:** import lines; the 2 pytest files; the CV/backtest regression; ruff; syntax check.
- **Gating (expected):** this workstream's output **is** the live/no-live gate — futures net-of-cost edge must be proven here before any capital. Must **extend** existing gate modules, not build a parallel framework. Profitability is gate-conditional and **not** asserted. Equity CV/backtest identical to baseline when no roll dates supplied.
- **Falsify:** (1) **roll-leakage embargo not actually expanded** — train/test split still straddles a roll (silent leakage); (2) backtest P&L still using equity bps not point-value (`contracts*multiplier*Δprice`) + tick-value slippage + two-leg roll cost; (3) `estimate_cost_bps` ignoring the new multiplier/tick params and returning the equity-bps result; (4) deflated-Sharpe gate not actually run on the futures path; (5) an "edge" implied/asserted instead of stated gate-conditional.

### FUT-09 — Execution adapter *(governance-gated; live ships DISABLED)*
- **Inspect:** `broker_ibkr_gateway.py` (`Future()`/`ContFuture()` construction keyed off FUT-01 metadata, reusing `sanitize/validate_ibkr_order_ref`, `_place_order_with_order_ref`, `_set_order_total_quantity`; qty = integer contracts from FUT-07); roll-aware gating (block/convert near first-notice/expiry, block during maintenance break); `broker_router.py` futures route inheriting failover + `live_broker_mode_boundary_block`; `execution_mode.py` (futures live reachable **only** via `assert_live_execution_arming_preflight`, `DISABLE_LIVE_EXECUTION` unset, `armed=1`). `execution_policy_engine.py` qty scaling needs no change.
- **Tests:** `tests/test_futures_broker_order_build.py`, `tests/test_futures_roll_window_block.py`, `tests/test_futures_live_disabled.py` (+ `tests/test_execution_mode*.py`, `tests/test_broker_router*.py` regression).
- **Run:** import lines; the 3 pytest files; the execution-mode/router regression; ruff; syntax check.
- **Gating (expected):** **LIVE FUTURES ORDER PATHS SHIP DISABLED BY DEFAULT.** Default config is shadow/paper only; live reachable only after FUT-08 gates green + governance sign-off via the existing arming preflight. Must reuse/not weaken existing execution-safety machinery.
- **Falsify:** (1) a live order/cancel/replace/flatten path **reachable without arming preflight** — `test_futures_live_disabled` must prove unreachable with `DISABLE_LIVE_EXECUTION` truthy or `armed=0`; (2) roll-window/maintenance-break block not actually firing (trades into first-notice/expiry/delivery); (3) order qty not integer contracts; (4) canary creds appearing in logs/payloads; (5) an existing safety gate silently weakened to make the route work.

### FUT-10 — UI surfacing *(reuse-first, read-only)*
- **Inspect:** `dashboard_server.py` (one new read-only `api_get_*` handler, e.g. `GET /api/data/futures/rolls`, + `ROUTE_SPECS` registration); `ui/futures_panel.js` (fetch→normalize→render mirroring `ui/data_health.js`); `ui/dashboard.js` (`loadFuturesPanel()` task under the `data` screen); optional `ui/view_router.js` persona allowlist; `ui/data_sources.*` auto-surfaces `futures_data` with zero edits.
- **Tests:** `tests/test_futures_dashboard_api.py` (+ a JS panel render smoke test if present).
- **Run:** `python -c "import dashboard_server"`; the API test; confirm the `data_sources` panel needed no edit; ruff; syntax check; UI lint if configured.
- **Gating (expected):** **read-only — no order-entry/trade-control UI.** Reuse-first: exactly one endpoint + one panel; no new framework/screens/personas unless a test requires.
- **Falsify:** (1) **vendor token leaking** into the `/api/data/futures/rolls` payload (test must assert absence); (2) empty-data case **erroring** instead of returning a bounded payload; (3) an order/trade control sneaking into the panel; (4) existing screens broken by the new task/allowlist entry; (5) over-building (new screens/framework) vs the mandated one-endpoint-one-panel.

---

## 4. Cross-cutting verification (after all FUT-0X)

1. **Roll-correctness end-to-end (the program's #1 risk).** Trace one continuous symbol: FUT-03 builds the
   ratio-adjusted series → FUT-06 labels consume the **adjusted** series (never raw front-month) → FUT-08 CPCV
   embargo expands around the roll dates. Prove with a single fixture that a raw-vs-continuous return difference
   exists across a roll boundary and that labels/CV use the continuous side. If any stage silently uses raw
   front-month, that is a **BROKEN** verdict regardless of green tests.
2. **Multiplier provenance is consistent.** The `multiplier`/`tick_value` parsed in **FUT-01** must be the single
   source consumed by FUT-06 (`fill_notional`), FUT-07 (sizing/sleeve), and FUT-08 (cost/P&L). Grep for any
   second hardcoded multiplier in those paths.
3. **Classifier ordering & no cross-class bleed.** `asset_map.py::asset_class_for_symbol` FUTURES branch must not
   reclassify EQUITY/FX/CRYPTO/OPTION/COMMODITY/RATES; bare roots unaffected. Shared with the other enablements.
4. **Fail-closed/default-off invariants.** Re-assert: `FUTURES_ENABLED` False, `INGEST_FUTURES_ROLLS_ENABLED` 0,
   `USE_FUTURES_FEATURES` False, live futures path unreachable without arming preflight. No `storage.py` schema change.
5. **Whole-suite + validators.** `pytest tests/ -q -m safety_critical`, `tools/validate_repo.py`,
   `tools/syntax_check_workspace.py`, `tools/git_worktree_triage.py`, `tools/coverage_gate.py`,
   `tools/pyright_money_path_gate.py`. Attribute reds to baseline vs change.

## 5. Report (write to `docs/handoff/verification/FUTURES_ENABLEMENT_VERIFICATION_REPORT.md`)

- **Roll-up table:** `ID | verdict | runtime evidence (file:line) | tests pass? | validation exit codes | defects`.
- **Per-ID detail** with cited evidence for all five lenses; give FUT-03/06/08 roll-correctness its own deep subsection.
- **Cross-cutting section** (roll trace, multiplier provenance, classifier ordering, fail-closed invariants, suite/validators).
- **GAP ledger:** every unconfirmed contract spec / `xfail`/TODO / "not executed in sandbox" / vendor-licensing
  block / synthetic-only path, classified **(legitimate-gated)** vs **(real-defect)**.
- **Final GO / NO-GO** for "Futures enablement is correctly and completely implemented and functioning in the
  wider repo," with the blocking list if NO-GO. GO requires: zero `FAKE-GREEN`/`BROKEN`/`MISSING`, **roll/continuous
  correctness proven** (not just green tests), every `PARTIAL` justified as legitimately-gated, fail-closed
  invariants intact, validators green vs baseline.
