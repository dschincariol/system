# Acceptance Validator Baseline - 2026-06-24

Scope: coverage-gate stale-report handling and Redis determinism for the
`safety_critical` suite.

## Coverage Gate

`python tools/coverage_gate.py run` now removes stale coverage artifacts before
running pytest, requires a newly generated JSON report, stamps the report with
the pytest args, pytest exit code, report hash, and gate config, and strict
`check` rejects focused, stale, mutated, or failed-pytest reports.

Fresh full-run coverage data was regenerated on 2026-06-24. The full pytest run
completed and wrote fresh XML/JSON, but pytest exited nonzero because of six
pre-existing full-suite failures outside this remediation scope:

- `tests/test_dashboard_route_contracts.py::test_execution_barrier_uses_lightweight_snapshot_path`
- `tests/test_futures_data_source_catalog.py::test_futures_data_catalog_and_registry_entries`
- `tests/test_fx_data_source_catalog.py::test_oanda_fx_catalog_and_registry_entries`
- `tests/test_learned_alpha_decay.py::test_execution_policy_blocks_risk_increasing_order_beyond_learned_max_age`
- `tests/test_options_feature_ablation_feature_sets.py::OptionsFeatureAblationFeatureSetTests::test_tool_uses_registry_private_split_not_literals`
- `tests/test_options_predictor_vrp.py::OptionsPredictorVrpTest::test_import_does_not_change_feature_registry_or_options_feature_gate`

Because the report was produced from a failed pytest run, strict
`python tools/coverage_gate.py check` is intentionally NO-GO until those tests
are fixed and the full gate is rerun.

Forensic threshold evaluation of the fresh JSON with
`python tools/coverage_gate.py check --allow-unstamped` passed:

- total: `57.82%` versus `52.00%`
- `engine/risk`: `65.70%` versus `50.81%`
- `engine/execution`: `61.00%` versus `58.92%`
- `engine/runtime`: `61.84%` versus `58.49%`
- zero-covered critical-root burndown: `remaining=13 allowlisted=13 new=0`

Remediation path: fix the six full-suite failures above, rerun
`python tools/coverage_gate.py run`, then require strict
`python tools/coverage_gate.py check` to pass without `--allow-unstamped`.

## Redis-Dependent Safety Test

`tests/test_paper_mode_sim_fill_boot.py` now pins the boot subprocess and job
subprocesses to the SQLite/memory safety-critical contract:

- `LIVE_CACHE_BACKEND=memory`
- `TS_REDIS_URL=redis://127.0.0.1:1/15`
- Redis connect/socket timeouts are `0.05s`
- Redis circuit opens after one failure with a long cooldown

The test is not marked `requires_redis` because it verifies paper-mode boot,
terminal order creation, simulated fill, and attribution without requiring a
real Redis service. Tests that need real Redis remain responsible for using
`@pytest.mark.requires_redis` and the production-backend gate.
