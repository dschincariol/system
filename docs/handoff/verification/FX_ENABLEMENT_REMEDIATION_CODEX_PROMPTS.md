# Currency / FX Enablement - Focused Remediation Prompts

These prompts are scoped from `docs/handoff/verification/FX_ENABLEMENT_VERIFICATION_REPORT.md`.
Use one prompt per Codex run unless the repository owner explicitly asks to combine them.

## FX-GO-01 - Reconcile FX Migration ID Contract

**Goal:** make FX instrument metadata migration evidence satisfy the acceptance contract: `0070_fx_instrument_metadata` with `id == 70`, or produce an explicit owner-approved contract update proving why the current `0071` placement is correct.

**Operating constraints:** preserve migration ordering, do not drop or rewrite applied production migrations without a compatibility plan, and do not mask migration contiguity failures by weakening tests.

**Deep dive:** inspect `engine/runtime/schema/migrations/0070_data_source_populate_evidence.py`, `0071_fx_instrument_metadata.py`, `engine/runtime/schema/migrations/__init__.py`, `tests/test_fx_instrument_migration.py`, `tests/test_storage_migrator.py`, and any expected-migration-ID helpers. Determine whether `0070_data_source_populate_evidence` predates the FX acceptance prompt or was introduced after it. If renumbering is safe, implement the migration-number correction and update all exact tests. If renumbering is unsafe, add explicit documentation and verifier evidence that the FX acceptance contract has been superseded, then update the FX verification prompt/report criteria with that owner-approved rationale.

**Acceptance:** `python -m pytest -q tests/test_fx_instrument_migration.py tests/test_storage_migrator.py tests/test_storage_sqlite_decomposition_contract.py tests/test_schema_classification.py` passes; a one-liner importing the FX migration proves the accepted `id`; `expected_migration_ids()` remains contiguous; no prior migration is silently edited without explicit compatibility evidence.

## FX-GO-02 - Fail Closed For FX Broker Routing Without IBKR

**Goal:** prevent FX orders from routing to non-FX execution brokers. FX execution must be IBKR CASH/IDEALPRO only, with no Alpaca or OANDA execution fallback.

**Operating constraints:** do not create `broker_oanda_rest.py`, do not bypass `validate_live_failover_chain`, `_execution_gate_or_block`, or `_real_trading_gate_or_block`, and preserve equity/crypto/futures routing behavior.

**Deep dive:** inspect `engine/execution/broker_router.py`, `tests/test_fx_broker_routing.py`, `tests/test_broker_router_dry_run_gates.py`, and `tests/test_broker_apply_orders_modes.py`. Replace the current fail-open behavior where `BROKER_FAILOVER=alpaca` can accept an FX batch. Add runtime blocking when a batch contains FX and no FX-capable broker is available after normal gates. Update the existing fake-green test that asserts `broker == "alpaca"` for FX.

**Acceptance:** targeted FX routing tests prove IBKR is preferred when available and fail closed when absent; non-FX router golden paths remain unchanged; `python -m pytest -q tests/test_fx_broker_routing.py tests/test_broker_router_dry_run_gates.py tests/test_broker_apply_orders_modes.py` passes.

## FX-GO-03 - Enforce A Single Canonical FX Clock Across Backend And UI

**Goal:** remove divergent fixed-UTC FX session clocks and prove all FX session boundaries match the canonical `engine/data/prices/fx_clock.py` America/New_York 17:00 clock, including DST.

**Operating constraints:** `fx_clock.py` remains the canonical backend source; UI can mirror the rules but must be boundary-equivalent; do not introduce a third clock or hardcode 22:00 UTC defaults.

**Deep dive:** inspect `engine/data/prices/fx_clock.py`, `engine/execution/fx_session.py`, `ui/fx_session.js`, `tests/test_fx_clock.py`, `tests/test_fx_session.py`, and `tests/test_fx_session.mjs`. Remove or constrain independent fixed-UTC fallback logic in `fx_session.py`. Replace UI defaults that assume `22:00 UTC` with logic equivalent to New York 17:00, or pass canonical boundary metadata from a safe existing source if available. Add tests for January standard time and June daylight time.

**Acceptance:** backend and UI tests pin Sunday open and Friday close in both EST and EDT; `node --test tests/test_fx_session.mjs` and `python -m pytest -q tests/test_fx_clock.py tests/test_fx_session.py tests/test_fx_session_policy_integration.py` pass; greps show no independent `22:00 UTC` FX boundary authority remains.

## FX-GO-04 - Restore The Unowned Broker-Sim Weight-To-Qty Seam

**Goal:** return the `broker_sim.py` weight-to-qty/lots conversion seam to the explicitly unowned NO-GO-pending-owner state, or isolate any unavoidable edits with owner-approved evidence.

**Operating constraints:** do not implement FX weight-to-lots conversion in `broker_sim.py`; do not silently change `_exec_px` or statistical gate math; preserve non-FX broker simulation behavior byte-for-byte where possible.

**Deep dive:** inspect `engine/execution/broker_sim.py` around the target-weight-to-quantity conversion, `tests/test_broker_sim_contract.py`, `tests/test_broker_sim_fx_cost_realism.py`, and the git diff for that file. Separate FX cost realism changes from unrelated conversion edits. Revert or surgically remove changes to the unowned conversion seam while keeping accepted FX-07 cost components intact. Add a self-audit assertion/comment/test that marks the seam as deliberately unowned rather than completed.

**Acceptance:** `git diff` for the conversion block shows only the deliberate NO-GO marker if needed; FX cost tests still pass; `python -m pytest -q tests/test_broker_sim_contract.py tests/test_broker_sim_fx_cost_realism.py tests/test_fx_gated_backtest_net_costs.py` passes.

## FX-GO-05 - Clean Protected-File No-Touch Guard Violations

**Goal:** make the FX acceptance no-touch guards clean for protected model/storage/backend files, or document non-FX ownership so the FX GO decision is not contaminated by unrelated working-tree edits.

**Operating constraints:** do not revert user work blindly; do not remove valid unrelated work. If edits are unrelated to FX, isolate them in a separate branch/commit or produce an explicit exclusion rationale before re-running the FX verifier.

**Deep dive:** inspect diffs for `engine/strategy/models/itransformer.py`, `engine/strategy/models/lgbm_regressor.py`, `engine/strategy/models/patchtst.py`, `engine/runtime/storage_pg.py`, `engine/runtime/storage_sqlite.py`, and `engine/runtime/schema/table_classification.py`. Classify each change as FX-required, unrelated, or accidental. Move unrelated work out of the FX remediation path or update the verification evidence to prove it is pre-existing and not attributable to FX.

**Acceptance:** FX verification guard commands over protected paths return empty or explicitly owner-exempt diffs; relevant tests for storage/model contracts still pass; no user changes are destroyed.

## FX-GO-06 - Resolve FX Macro/FRED Short-Rate Evidence Gap

**Goal:** satisfy the FX-01 requirement for short-rate macro series evidence: either verify the intended fourth non-US FRED short-rate ID or mark it as an explicit TODO/xfail rather than fake-verified runtime coverage.

**Operating constraints:** keep OANDA and FX macro ingestion default-off where required; do not use live network-only tests as mandatory CI gates; do not leak credentials or tokens.

**Deep dive:** inspect `engine/data/factor_ingestion.py`, `tests/test_fx_macro_specs.py`, FX research docs, and any data-source catalog metadata. Identify the intended rate set from the dossier. If the fourth non-US short-rate ID is verified, add it to `MACRO_SERIES_SPECS` with tests. If not verifiable offline, add an explicit TODO and xfail that names the unresolved ID and explains the network/data dependency.

**Acceptance:** `python -m pytest -q tests/test_fx_macro_specs.py tests/test_fx_provider_registry.py tests/test_fx_data_source_catalog.py` passes; the test suite can no longer pass while silently omitting the unresolved short-rate caveat.

## FX-GO-07 - Route FX Profitability Evidence Through Full Governance

**Goal:** ensure `evaluate_fx_challengers` cannot report a promotion-like pass without flowing through the full governance surface, including `assess_challenger`.

**Operating constraints:** do not create a new promotion entrypoint; do not reimplement gate math; keep all FX profitability output advisory/reporting unless the existing governance path approves it.

**Deep dive:** inspect `engine/strategy/fx_profitability_report.py`, `tests/test_fx_profitability_report.py`, `tests/test_fx_no_promotion_bypass.py`, `engine/strategy/gated_backtest.py`, and the module that defines `assess_challenger`. Refactor the report to call the real governance function or explicitly consume its verdict. Add tests that monkeypatch or exercise `assess_challenger` so the report fails if that call is removed.

**Acceptance:** `python -m pytest -q tests/test_fx_profitability_report.py tests/test_fx_no_promotion_bypass.py tests/test_gated_backtest.py tests/test_promotion_guard_fdr.py tests/test_champion_promotion_identity.py` passes; source inspection shows no duplicated statistical gate implementation.

## FX-GO-08 - Restore Repo-Wide Validators Needed For FX GO

**Goal:** make the repo-wide gates used by the FX verifier green or produce a clean baseline attribution that separates unrelated failures from FX acceptance.

**Operating constraints:** do not lower validation thresholds, do not weaken `tools/coverage_gate.py`, and do not remove UI asset checks. Fix root causes or explicitly isolate unrelated work from the FX branch.

**Deep dive:** inspect `ui/dashboard.js`, `ui/futures_panel.js`, `tools/check_local_asset_refs.py`, `tools/run_ui_checks.mjs`, and coverage artifacts under `artifacts/coverage/`. Fix the untracked/asset-reference issue without breaking the futures UI. For coverage, determine whether the current `coverage_gate.py run` failure is caused by changed coverage configuration, missing test selection, or unrelated branch state; restore the expected invocation or fix coverage attribution without weakening floors.

**Acceptance:** `python tools/validate_repo.py`, `python tools/coverage_gate.py run`, `npm run check:ui`, and `npm run test:ui` all pass, or a new FX verification report includes exact baseline evidence proving any remaining red is pre-existing and unrelated to FX.

