# Options Enablement Verification Report

Audit date: 2026-06-24

Repo: `/home/david/gitsandbox/system/system`
Branch/head: `codex/worktree-production-readiness` / `39fc99f Stabilize repo for next additions`

Scope: read-only audit of OPT-01 through OPT-10 from
`docs/handoff/deep_dive_prompts/OPTIONS_ENABLEMENT_DEEP_DIVE_PROMPTS.md`.
The only repo write made by this audit is this report.

## Final Verdict

**NO-GO.**

Most runtime paths are present and the targeted OPT-01 through OPT-10 pytest suites pass, but the implementation is
not acceptable as complete because there are two blocking option-specific defects plus repo-wide validator reds:

1. **OPT-01 PARTIAL:** the canonical OCC parser hardcodes multiplier, exercise style, settlement, price currency, and
   session calendar as facts for every OCC-shaped contract (`engine/data/options_instrument.py:20-25`, `:96-109`).
   Tests assert those values (`tests/test_options_instrument_parser.py:29-31`,
   `tests/test_universe_option_metadata.py:147-153`) instead of marking unconfirmed specs as TODO/xfail. This violates
   the prompt's anti-fake-green trap for hardcoded contract specs.
2. **OPT-03 FAKE-GREEN:** the health fallback for a data-quality exception returns
   `{"available": False, "ok": True}` (`engine/runtime/health.py:1051-1058`, `:1075-1079`) and the test asserts that
   green value (`tests/test_options_ingestion_health_data_quality.py:53-67`). The prompt explicitly lists this as a
   fake-green trap.
3. Whole-repo validators are not green: `validate_repo.py` fails on an untracked futures UI asset reference, and the
   coverage gate fails/has a changed CLI contract.

## Worktree Baseline

Captured before verdicts:

- `git status --short --untracked-files=all`: dirty working tree with more than 100 modified files and more than 100
  untracked files. Options enablement files are working-tree-only, including
  `engine/data/options_instrument.py`, `engine/data/options_data_quality.py`,
  `engine/execution/broker_tradier_options.py`, `engine/execution/options_lifecycle.py`,
  `engine/strategy/options_predictor.py`, `tools/options_feature_ablation.py`, and the new `tests/test_options_*`
  files.
- `git log --oneline -15`: latest commit is `39fc99f Stabilize repo for next additions`, followed by
  `947c665 Wire ops shell tests into validation`, `f3bbe1c Enforce pool-wide ZFS compression checks`, and 12 older
  production-readiness commits.
- Attribution: every options finding below is working-tree-only unless explicitly noted otherwise.

Baseline commands:

- `python -m pytest -q -m safety_critical 2>&1 | tail -30`: exit `0`; `[100%]`, 256 safety-critical tests passed.
- `python tools/pyright_money_path_gate.py > /tmp/options_pyright_baseline.txt 2>&1`: exit `0`;
  `Pyright money-path gate passed: 33 baseline errors, 0 baseline warnings, 30 target files.`

## Roll-Up

| ID | Verdict | Runtime evidence | Tests pass? | Validation exit codes | Defects |
|---|---|---|---|---|---|
| OPT-01 | PARTIAL | `options_instrument.py:42`, `:84`; `asset_map.py:179-200`; `universe.py:148-156`, `:398-445`; `0073_options_instrument_metadata.py:5-19` | yes: 20 tests in OPT-01 group pass | one-liners `0`; pytest `0`; protected FX diff empty | hardcoded unconfirmed specs in parser |
| OPT-02 | PASS | `health.py:1002-1009`, `:1082-1214`; `prod_preflight.py:584-592`; `.env.example:392-394` | yes: 13 passed | imports `0`; grep `0`; pytest `0` | isolated diff guard is noisy due co-mingled worktree |
| OPT-03 | FAKE-GREEN | `options_data_quality.py:520-643`, `:646-654`, `:730-818`; `health.py:1061-1079` | pytest `0`, but wrong assertion at `test_options_ingestion_health_data_quality.py:53-67` | imports `0`; pytest `0` | DQ exception path masks failure as `ok=True` |
| OPT-04 | PASS | `tools/options_feature_ablation.py:36-49`, `:90-112`, `:477-479`, `:843`; protocol doc present | yes: 8 passed | imports `0`; registry one-liner `0`; synthetic run `0` | no option-specific defect found |
| OPT-05 | PASS | `broker_sim.py:1068-1124`, `:1180-1190`, `:3009-3049`, `:3429-3508`, `:3696-3747`; `0074_broker_sim_option_fields.py:5-12` | yes: 5 passed | imports `0`; pytest `0` | no option-specific defect found |
| OPT-06 | PASS | `portfolio_risk_engine.py:151-156`, `:916-974`, `:1013-1106`, `:1515-1550`, `:2852-2874`; `options_readiness.py:89-99` | yes: 8 passed | signature `0`; controls `0`; pytest `0` | no option-specific defect found |
| OPT-07 | PASS | `options_lifecycle.py:1-15`, `:281-429`, `:432-446`; `broker_sim.py:1882-1936`, `:2097-2183` | yes: 20 passed | import `0`; pytest `0` | inherits OPT-01 metadata-spec risk when parser defaults are used |
| OPT-08 | PASS | `portfolio_risk_engine.py:171-180`; `hierarchical_allocator.py:248-315`, `:318-330` | yes: 7 passed | budget/import `0`; pytest `0` | no option-specific defect found |
| OPT-09 | PASS | `options_predictor.py:34`, `:212-261`, `:432-491`, `:494-531`, `:662-690`; `0075_options_predictor_shadow.py:5-6` | yes: 7 passed | imports `0`; default feature count `0` | no option-specific defect found |
| OPT-10 | GATED-OK | `options_readiness.py:43`, `:648-701`, `:789-890`; `broker_router.py:1558-1624`; `broker_tradier_options.py:87-106`, `:302-333`, `:407-470` | yes: 16 passed | imports `0`; adapter/default-shadow checks `0`; pytest `0` | live path remains gated; no network reached in tests |

## Per-ID Detail

### OPT-01

Runtime is present: parser is pure/no-raise (`engine/data/options_instrument.py:84-117`), OPTION classification is wired
after overrides/bare roots (`engine/data/asset_map.py:179-200`), `universe.py` persists and reads `opt_*` metadata
(`engine/data/universe.py:148-156`, `:221-230`, `:398-445`), and migration `0073` is additive.

Tests are present and pass:

- `python -m pytest tests/test_options_instrument_parser.py tests/test_asset_map_option_branch.py tests/test_universe_option_metadata.py tests/test_options_readiness_consumes_parser.py -q`: exit `0`, `20 passed`.
- One-liners:
  - `parse_option_symbol('O:SPY240920C00450000')`: exit `0`, `ok`.
  - `asset_class_for_symbol(...)` for option/equity/commodity/rates/FX: exit `0`, `ok`.
  - imports: exit `0`, `imports ok`.
  - `git diff --stat -- engine/data/fx_instrument.py engine/runtime/schema/migrations/0071_fx_instrument_metadata.py`: exit `0`, empty.

Defect: parser metadata is overclaimed. `OPTION_CONTRACT_MULTIPLIER = 100.0`,
`OPTION_EXERCISE_STYLE = "american"`, `OPTION_SETTLEMENT = "physical"`, `OPTION_PRICE_CCY = "USD"`, and
`OPTION_SESSION_CALENDAR = "US_EQUITY_OPTION"` are assigned in `engine/data/options_instrument.py:20-25` and emitted
for every valid OCC symbol at `:96-109`. The tests assert these values as facts (`tests/test_options_instrument_parser.py:30`,
`tests/test_universe_option_metadata.py:147-153`). OCC compact symbols alone do not prove settlement/exercise style for
all products. This is a blocking DoD miss.

### OPT-02

Runtime is present: `_options_credentials_configured` resolves configured option credentials at
`engine/runtime/health.py:1002-1009`; `_options_ingestion_snapshot` distinguishes no-creds from configured-stale at
`:1082-1214`; preflight adds `options_chain_stale_despite_credentials` at `engine/runtime/prod_preflight.py:584-592`;
`.env.example` contains `POLYGON_API_KEY_FILE`, `TRADIER_API_TOKEN`, and `TRADIER_API_TOKEN_FILE` at lines `392-394`.

Tests are honest enough for this slice:

- No credentials benign-green: `tests/test_options_credential_health_visibility.py:35-47`.
- Configured stale degrades: `:49-72`.
- Configured fresh OK: `:74-96`.
- Preflight blocker: `tests/test_options_preflight_credential_assertion.py:62-88`.
- `python -m pytest -q tests/test_options_credential_health_visibility.py tests/test_options_credential_env_example.py tests/test_options_preflight_credential_assertion.py`: exit `0`, `13 passed`.

Protected-file diff guard is non-empty because the current worktree includes other futures/options changes to
`provider_registry.py`, `_credentials.py`, and `options_readiness.py`. I did not treat this as an OPT-02 runtime defect,
but it means isolated slice ownership cannot be proven from `git diff`.

### OPT-03

Runtime is present: `compute_options_data_quality` computes freshness, coverage, provider completeness, and IV sanity at
`engine/data/options_data_quality.py:520-643`; `options_data_quality_ok` returns false when unavailable at `:646-654`;
metrics/event emission is best-effort at `:657-818`; health calls the DQ path at `engine/runtime/health.py:1061-1079`.

Tests cover useful paths:

- Healthy Polygon snapshot: `tests/test_options_data_quality.py:150-163`.
- Legacy chain greeks/bid-ask completeness = 0: `:165-176`.
- IV sanity: `:178-199`.
- Empty tables unavailable/not green in direct DQ computation: `:201-209`.
- Event path via normalized events and no chain/state writes: `:232-268`.
- Targeted pytest: exit `0`, `9 passed`.

Blocking fake-green: the health error fallback contradicts the direct DQ semantics. `_options_data_quality_unavailable`
returns `ok=True` at `engine/runtime/health.py:1051-1058`, `_options_data_quality_snapshot` uses that fallback on compute
exceptions at `:1075-1079`, and `tests/test_options_ingestion_health_data_quality.py:53-67` asserts the green value. This
matches the prompt's fake-green trap: a DQ compute error is masked as green advisory metadata.

### OPT-04

Runtime is present and read-only: the tool imports the production registry split at
`tools/options_feature_ablation.py:36-42`, resolves feature sets from those imported values at `:90-99`, maps reports via
`evaluate_enablement` at `:112`, trains through production `train_gbm_model` at `:477-479`, and uses CPCV at `:646`.

Tests and validations:

- Verdict thresholds and ABSTAIN dominance: `tests/test_options_feature_ablation_verdict.py:48-89`.
- Registry import identity, not literal duplication: `tests/test_options_feature_ablation_feature_sets.py:31-37`.
- Default `USE_OPTIONS_FEATURES` stays off: `tests/test_options_feature_registry_unchanged.py:17-25`.
- Smoke writes structured report: `tests/test_options_feature_ablation_smoke.py:24-62`.
- `python -m pytest ...options_feature... -q`: exit `0`, `8 passed`.
- `python tools/options_feature_ablation.py --synthetic --min-rows 50 --min-gex-coverage 0.1`: exit `0`;
  verdict `ENABLE_SUPPORTED` on synthetic data, which is an allowed machine verdict.

### OPT-05

Runtime is present and in the simulator path:

- Option metadata lookup fails closed at `engine/execution/broker_sim.py:1068-1121`.
- Chain quote lookup reads `options_chain_v2` bid/ask directly and rejects missing/stale/bad quotes at `:1124-1170`.
- Sizing uses option mid times metadata multiplier and whole contracts at `:3009-3049`.
- Fill path applies option spread, multiplier notional, affordability, and reference short-margin debit at `:3228-3508`.
- MTM uses option chain mid times multiplier and fails closed to `missing_prices` at `:3696-3747`.

Tests and validations:

- Chain ask fill, x100 notional, MTM: `tests/test_broker_sim_option_fills.py:245-277`.
- Quote-less OCC order skips even with a `prices` row: `:279-304`.
- Non-100 metadata multiplier is honored: `:306-323`.
- Short margin debit is recorded: `:325-345`.
- Equity/FX path remains plain accounting with option columns NULL: `:347-387`.
- `python -m pytest tests/test_broker_sim_option_fills.py -q`: exit `0`, `5 passed`.

### OPT-06

Runtime is present and tied into the risk engine:

- Greek cap envs and fail-open sentinels: `engine/risk/portfolio_risk_engine.py:151-156`.
- Option greeks read chain data and require OPTION classification plus metadata multiplier: `:916-974`.
- Portfolio snapshot aggregates signed contracts times multiplier: `:1013-1106`.
- Post-check adds `options_greeks_within_cap` and violations: `:1515-1550`.
- Block reason joins the existing block decision as `options_greek_limit_breached`: `:2867-2874`.
- Readiness exposes gamma/vega numeric controls at `engine/execution/options_readiness.py:89-99`.

Tests and validations:

- 2-leg arithmetic: `tests/test_portfolio_risk_options_greeks.py:173-184`.
- Delta downsize before hard block: `:186-201`.
- Gamma/margin blocks: `:203-212`, `:248-266`.
- Empty caps fail open and non-option parity: `:214-246`.
- `python -m pytest tests/test_portfolio_risk_options_greeks.py tests/test_options_readiness_greek_controls.py -q`: exit `0`, `8 passed`.

### OPT-07

Runtime is present and default-off:

- Planner documents reference-grade assumptions and no random pin-risk assignment at
  `engine/execution/options_lifecycle.py:1-15`.
- Pure planner catches broad exceptions and returns planned events/no events at `:281-429`.
- Readiness evidence says shadow-only/no live authority at `:432-446`.
- Broker sim gate returns a no-op when disabled at `engine/execution/broker_sim.py:1882-1889`, `:2097-2101`.
- Applier writes through `_write_position`, `_write_fill`, and `_write_account` at `:2017-2048`, `:2173-2183`.

Tests and validations:

- Exercise/cash settlement/expire worthless/DTE roll/pin risk planner tests at
  `tests/test_options_lifecycle_planner.py:44-108`.
- Conservation and disabled no-op: `tests/test_options_lifecycle_apply_conservation.py:150-207`.
- Readiness unchanged and non-option skip: `tests/test_options_lifecycle_readiness_unchanged.py:70-91`.
- `python -m pytest tests/test_options_lifecycle_planner.py tests/test_options_lifecycle_apply_conservation.py tests/test_options_lifecycle_readiness_unchanged.py tests/test_options_instrument_readiness.py -q`: exit `0`, `20 passed`.

### OPT-08

Runtime is present:

- `OPTION: 0.20` is in `_DEFAULT_ASSET_CLASS_BUDGETS` at `engine/risk/portfolio_risk_engine.py:171-180`.
- Operator override remains via `PORTFOLIO_RISK_ASSET_CLASS_BUDGETS_JSON` at `:183-190`.
- Allocator fallback maps OPTION to `options` at `engine/runtime/hierarchical_allocator.py:248-255`, after explicit/env
  mapping checks in `_strategy_to_sleeve` at `:318-330`.

Tests and validations:

- Conservative OPTION budget and scaling: `tests/test_option_asset_class_budget.py:42-67`.
- Non-option budgets unchanged and override wins: `:69-93`.
- Sleeve fallback, explicit override, registry override, disabled binding: `tests/test_allocator_option_sleeve_binding.py:42-90`.
- `python -m pytest tests/test_option_asset_class_budget.py tests/test_allocator_option_sleeve_binding.py -q`: exit `0`, `7 passed`.

### OPT-09

Runtime is present and shadow-only:

- `USE_OPTIONS_PREDICTOR` defaults off at `engine/strategy/options_predictor.py:34`.
- VRP returns `None` on gaps at `:212-261`.
- Structure selection validates/built contracts through `parse_option_symbol` at `:289-309` and selects defined-risk
  structures at `:432-491`.
- Intent builder stamps `execution_target="shadow"` and calls `force_options_shadow_intent` at `:494-531`.
- Runtime entrypoint no-ops when disabled and requires OPT-04 evidence before forecasting at `:662-690`.

Tests and validations:

- Import does not change feature registry/default predictor flag: `tests/test_options_predictor_vrp.py:100-108`.
- Disabled/missing evidence no-op and shadow intent emission: `tests/test_options_predictor_shadow_gate.py:120-159`.
- `python -m pytest tests/test_options_predictor_vrp.py tests/test_options_predictor_selection.py tests/test_options_predictor_shadow_gate.py -q`: exit `0`, `7 passed`.
- `python -c "from engine.strategy.feature_registry import default_feature_ids; print(len(default_feature_ids()))"`:
  exit `0`, printed `111`.

### OPT-10

Runtime is present and still gated:

- Capstone registers only `tradier_options` at `engine/execution/options_readiness.py:35-43`, with 9 control groups at
  `:45-87`.
- `_GATE_PREDICATES` maps controls to real predicates at `:648-658`; `_control_flag_snapshot` requires both env flag and
  real predicate success at `:661-701`.
- Default readiness returns `required=False` and `shadow_only=True` when not required at `:789-822`.
- `live_options_order_block` blocks non-dry-run option orders unless readiness is OK at `:860-890`.
- Router applies live broker gates, mode boundary, options readiness, futures safety, pre-live reconcile, then Tradier
  adapter at `engine/execution/broker_router.py:1558-1624`.
- Tradier adapter credential block is terminal at `engine/execution/broker_tradier_options.py:87-106`; dry-run returns
  before HTTP at `:407-437`; non-dry-run checks credentials, readiness, kill switch, and pre-live reconcile before submit
  at `:439-470`.

Tests and validations:

- Positive readiness with patched real predicates: `tests/test_options_live_adapter_gates.py:85-99`.
- Env flag is insufficient when greeks/kill-switch checks fail: `:102-131`.
- Missing adapter blocks: `:134-148`.
- Missing credentials do not call HTTP: `:151-164`.
- Default remains shadow and force-shadow intent still applies: `:167-179`.
- Alpaca remains blocked exactly: `:182-195`.
- Tradier single-leg fields are consistent with current official Tradier docs for `class`, `symbol`, `option_symbol`,
  `side`, `quantity`, `type`, and `duration`: `:198-208`; official docs list these option order fields and side values
  at <https://docs.tradier.com/docs/trading> and <https://docs.tradier.com/docs/orders>.
- `python -m pytest tests/test_options_live_adapter_gates.py tests/test_options_instrument_readiness.py -q`: exit `0`,
  `16 passed`.

## Cross-Cutting Verification

Classifier ordering and no cross-class bleed:

- `asset_class_for_symbol` respects overrides/defaults first, then futures/options/FX/rates/equity registry
  (`engine/data/asset_map.py:179-208`).
- One-liner exit `0`: option -> `OPTION`, `SPY` -> `EQUITY`, `GC` -> `COMMODITY`, `ZN` -> `RATES`, `EURUSD` -> `FX`.

Multiplier provenance:

- Broker sim consumes `OptionContractMetadata.multiplier` and fails closed if absent (`broker_sim.py:1085-1099`,
  `:1114-1121`); tests monkeypatch multiplier `50.0` and assert it is used
  (`tests/test_broker_sim_option_fills.py:306-323`).
- Portfolio risk consumes metadata multiplier through `_option_contract_multiplier` (`portfolio_risk_engine.py:906-913`).
- Defect remains: parser itself hardcodes multiplier and related contract facts for every OCC contract, and
  `options_readiness._option_multiplier` falls back to `100.0` if parsing fails (`options_readiness.py:336-342`).

End-to-end shadow trace:

1. OCC symbol classifies as OPTION (`asset_map.py:199-200`).
2. Sim fill uses chain quote and multiplier (`broker_sim.py:3009-3049`, `:3228-3508`).
3. Portfolio risk aggregates greeks and can cap/block (`portfolio_risk_engine.py:1013-1106`, `:1515-1550`).
4. Lifecycle is a no-op unless `OPTIONS_LIFECYCLE_ENABLED` is true (`broker_sim.py:2097-2101`).
5. OPTION sleeve budget binds to `0.20` (`portfolio_risk_engine.py:171-180`) and allocator maps OPTION -> `options`
   (`hierarchical_allocator.py:248-255`).
6. Predictor emits only shadow intents (`options_predictor.py:494-531`).
7. Live path remains blocked unless mode, env flags, real predicates, credentials, kill-switch, and pre-live reconcile pass
   (`options_readiness.py:661-701`, `:860-890`; `broker_router.py:1558-1624`).

Fail-closed invariants:

- `.env.example:516`: `OPTIONS_INSTRUMENTS_MODE=shadow`.
- `USE_OPTIONS_FEATURES` remains import-default off (`engine/strategy/feature_registry.py:64`;
  test assertion `tests/test_options_feature_registry_unchanged.py:17-25`).
- `USE_OPTIONS_PREDICTOR` remains default off (`engine/strategy/options_predictor.py:34`).
- Capstone intentionally registers `LIVE_OPTIONS_BROKER_ADAPTERS=frozenset({"tradier_options"})`
  (`options_readiness.py:43`), but default readiness is not required and shadow-only (`tests/test_options_live_adapter_gates.py:167-179`).
- `len(CONTROL_FLAG_GROUPS)==9` by inspection at `options_readiness.py:45-87`.
- No test reaches real HTTP for missing credentials (`tests/test_options_live_adapter_gates.py:151-164`).

Whole-suite and validators:

- `python -m pytest -q -m safety_critical 2>&1 | tail -30`: exit `0`, `[100%]`, 256 passed.
- `python tools/pyright_money_path_gate.py`: exit `0`, 33 baseline errors / 0 target drift.
- `ruff check .`: exit `0`, `All checks passed!`.
- `python tools/syntax_check_workspace.py`: exit `0`, `Syntax OK: 858 file(s) checked`.
- `python tools/git_worktree_triage.py`: exit `0`, `ok: true`; dirty tree summary shows 107 modified and 134
  untracked paths.
- `python tools/validate_repo.py`: exit `1`; `Local asset reference validation failed` because
  `ui/dashboard.js:121` imports untracked `./futures_panel.js`. This appears futures/UI-related, not options-specific,
  but it blocks whole-repo green.
- `python tools/coverage_gate.py`: exit `2`; usage error because the tool requires `{run,check}`.
- `python tools/coverage_gate.py check`: exit `1`; existing coverage artifact total `3.01%` below required `52.00%`,
  with many zero-covered modules. This is not isolated to Options, but it blocks the GO criteria.

## GAP Ledger

| Gap | Classification | Evidence |
|---|---|---|
| Parser hardcodes multiplier/exercise style/settlement/session for every OCC symbol | real defect | `options_instrument.py:20-25`, `:96-109`; tests assert at `test_options_instrument_parser.py:30`, `test_universe_option_metadata.py:147-153` |
| DQ exception fallback returns `ok=True` | real defect / fake-green | `health.py:1051-1058`, `:1075-1079`; test asserts at `test_options_ingestion_health_data_quality.py:65-66` |
| `options_readiness._option_multiplier` fallback to `100.0` | real defect risk | `options_readiness.py:336-342`; downstream readiness can use guessed multiplier if parser fails |
| Live options adapter registered but default still shadow-only | legitimate-gated | `options_readiness.py:43`; default readiness test `test_options_live_adapter_gates.py:167-179` |
| Options feature gate not flipped | legitimate-gated | `feature_registry.py:64`; OPT-04 protocol documents operator enablement |
| Lifecycle American early exercise and assignment are reference-grade only | legitimate-gated | `options_lifecycle.py:1-15`, `lifecycle_readiness_evidence` at `:432-446` |
| Isolated per-slice diff guards are noisy | audit limitation | Current worktree co-mingles Options, Futures, Crypto, Equity, and UI changes; behavior was checked by tests/one-liners |
| `validate_repo.py` fails on untracked futures UI file | unrelated validator blocker | `ui/dashboard.js:121: untracked:js-import: ./futures_panel.js -> ui/futures_panel.js` |
| Coverage gate not green | validator blocker | exact command exits `2`; `check` exits `1`, total `3.01% < 52.00%` |

## Blocking List

1. Replace parser-level hardcoded contract facts with clearly marked parser defaults or unknown/TODO metadata, and ensure
   tests no longer assert unconfirmed multiplier/exercise-style/settlement for all OCC symbols.
2. Make the options data-quality health fallback fail visible: unavailable/exceptional DQ must not report `ok=True`.
   Update tests to catch that instead of asserting green.
3. Remove or address `options_readiness._option_multiplier`'s `100.0` fallback if parser metadata is unavailable.
4. Resolve repo-wide validator reds (`validate_repo.py`, coverage gate CLI/report).

GO requires all four blockers cleared and validators green relative to a freshly captured baseline.
