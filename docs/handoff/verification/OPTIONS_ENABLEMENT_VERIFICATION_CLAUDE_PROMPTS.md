# Options Enablement — Verification & Acceptance Deep-Dive Prompt (OPT-01 … OPT-10)

> **Goal:** independently verify that the Options enablement workstreams **OPT-01 … OPT-10** (from
> `docs/handoff/deep_dive_prompts/OPTIONS_ENABLEMENT_DEEP_DIVE_PROMPTS.md`) were implemented
> **correctly, completely, and are wired and functioning in the wider repo** — with no missing,
> broken, faked-green, or regressed requirements. The deliverable is an **evidence-backed audit, not
> new feature work**.
>
> Pair the original enablement file open beside this one — for each ID, re-read its *Definition of Done*,
> *Tests to add*, and *Validation commands*; this prompt tells you how to **prove** each was met. The
> **How to run this audit in Claude Code** section below gives the recommended subagent fan-out.

---

## How to run this audit in Claude Code

**You are an independent acceptance auditor.** Your job is to produce an evidence-backed verdict —
**not** to make the code pass. Everything here is read-only (see the operating contract below).

**Recommended orchestration (subagent fan-out):**
1. **Plan with `TodoWrite`.** One todo per requirement ID (OPT-01 … OPT-10), plus `baselines`, `cross-cutting`, and
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

- **Target repo:** `/home/david/gitsandbox/system/system`. Activate `.venv` if present. All paths relative to repo root.
- **THIS IS A READ-ONLY AUDIT.** Do **NOT** modify runtime code, tests, or docs to "make it pass."
  The only writes permitted are (a) the report at
  `docs/handoff/verification/OPTIONS_ENABLEMENT_VERIFICATION_REPORT.md`, and (b) throwaway files under `/tmp`.
  If you find a defect, **record it — do not fix it** (a separate remediation pass owns fixes).
- **Audit the working tree AS-IS.** Much of this work may be uncommitted. First run
  `git status --short --untracked-files=all` and `git log --oneline -15`; record what is committed vs
  working-tree-only so every finding is attributable.
- **Anchor line numbers are approximate.** The anchors below were captured at authoring time and may
  have drifted. **Re-locate by symbol name** (function/class/constant), not by line number.
- **Baseline before verdicts.** Capture the pre-existing red set:
  `python -m pytest -q -m safety_critical 2>&1 | tail -30` and `python tools/pyright_money_path_gate.py`.
  Attribute every red strictly to (i) the options change or (ii) a pre-existing unrelated baseline. **Never mask a red.**
- **Evidence-first.** Every verdict MUST cite: `file:line` for runtime enforcement, the **exact assertion lines**
  for tests, and the **literal command + exit code + key output** for validations. A claim without cited
  evidence is **UNVERIFIED (= fail)**.
- **Respect intentional gating.** Options ship **shadow-only / fail-closed**: `LIVE_OPTIONS_BROKER_ADAPTERS`
  defaults to `frozenset()`, `OPTIONS_INSTRUMENTS_MODE` defaults to `"shadow"`, `USE_OPTIONS_FEATURES`
  defaults to `0`. A correctly-disabled live path is **PASS, not "missing."** Verify the default state, and
  that *enabling the flag actually changes behavior*.
- **No fake-green tolerance.** A passing test that over-mocks the runtime, asserts trivially, or never
  drives the production path is a **FAIL** ("test does not exercise runtime"). Catching enforcement-in-tests-only
  is the whole point of this audit.

## 1. Method — apply all five lenses to every OPT-0X

For each requirement, derive the verdict from five independent checks:

1. **RUNTIME ENFORCEMENT** — open each named anchor at its symbol; confirm the logic is real and in the
   live call path (trace from an entry point), not a stub / hardcode / pass-through / `return 0` / `return None` / `TODO`. Cite `file:line`.
2. **TESTS PRESENT & HONEST** — each named test file exists, asserts the specified behavior (quote it),
   drives the runtime (not a re-implementation), and **passes** on re-run.
3. **VALIDATION COMMANDS** — run the ID's exact validation commands; record literal exit codes + key lines.
4. **ANTI-FAKE-GREEN PROBES** — actively try to falsify the implementation using the listed traps.
5. **WIRING & NO-REGRESSION** — confirm the feature is reachable end-to-end for an OCC option symbol, and
   that equity/FX/crypto/futures fills, sizing, and `get_instrument_metadata` are **byte-for-byte unchanged**
   (run the no-touch `git diff --stat` guards the prompt specified).

## 2. Verdict vocabulary (per ID)

`PASS` · `PARTIAL` (runtime present but a DoD item unmet / weakly tested / not wired) · `FAKE-GREEN`
(tests pass, runtime absent/stubbed/test-only — highest priority) · `MISSING` · `BROKEN` (errors / regresses
another class / fails own validation) · `GATED-OK` (intentionally disabled/shadow/xfail per prompt — confirm
the gate, not the absence).

---

## 3. Per-requirement verification

### OPT-01 — First-class OCC option instrument/contract model + OPTION asset class *(keystone)*
- **Inspect (runtime):** `engine/data/options_instrument.py` (`parse_option_symbol`, `is_option_symbol`, `OptionContractMetadata`); the `OPTION` branch in `engine/data/asset_map.py::asset_class_for_symbol`; `opt_*` columns + dispatch in `engine/data/universe.py` (`upsert_symbol`, `get_instrument_metadata`); `_column_type` in `engine/runtime/storage_sqlite.py`; the new `00NN_options_instrument_metadata` migration; consumption in `engine/execution/options_readiness.py`.
- **Tests:** `tests/test_options_instrument_parser.py`, `tests/test_asset_map_option_branch.py`, `tests/test_universe_option_metadata.py`, `tests/test_options_readiness_consumes_parser.py`.
- **Run:** the OPT-01 one-liners — `parse_option_symbol('O:SPY240920C00450000')` → `occ_symbol=='SPY240920C00450000'`, `asset_class=='OPTION'`; `asset_class_for_symbol` returns OPTION for the OCC symbol but **unchanged** EQUITY/COMMODITY/RATES/FX for `SPY`/`GC`/`ZN`/`EURUSD`; the import line; the 4 pytest files; `ruff check .`; `git diff --stat` of `fx_instrument.py` + `0071_fx_instrument_metadata.py` must be **empty**.
- **Gating (expected):** pure infra, no behavior change. Confirm `LIVE_OPTIONS_BROKER_ADAPTERS==frozenset()` and `OPTIONS_INSTRUMENTS_MODE=="shadow"` are **untouched**; FX `get_instrument_metadata` output byte-identical. Migration is import-only in sandbox.
- **Falsify:** (1) multiplier / exercise-style / settlement **hardcoded as fact** where spec is unconfirmed — should be `xfail`+TODO, not a seeded number; (2) the OCC regex drifted from `options_readiness._OCC_COMPACT_RE.pattern` (must be equal); (3) `parse_option_symbol` **raises** on bad input instead of returning `None`; (4) OPTION branch mis-ordered so bare `SPY`/`GC`/`ZN`/`TLT` reclassify; (5) migration id guessed, not next-free.

### OPT-02 — Options data-credential provisioning + fail-visible chain verification
- **Inspect:** `.env.example` (`POLYGON_API_KEY_FILE`, `TRADIER_API_TOKEN[_FILE]`); `engine/runtime/health.py::_options_ingestion_snapshot` + new `_options_credentials_configured` + the `credentials_configured` key on **all three** return paths; the `options_chain_stale_despite_credentials` blocker in `engine/runtime/prod_preflight.py`; degraded-vs-no-creds rendering in `dashboard_server.py`.
- **Tests:** `tests/test_options_credential_health_visibility.py`, `tests/test_options_credential_env_example.py`, `tests/test_options_preflight_credential_assertion.py`.
- **Run:** import lines for `health`/`prod_preflight`/`_credentials`/`dashboard_server`; the 3 pytest files; `grep -nE "TRADIER_API_TOKEN|POLYGON_API_KEY_FILE|TRADIER_API_TOKEN_FILE" .env.example` non-empty; `git diff --stat` of `_credentials.py`/`options_poll.py`/`provider_registry.py`/`options_readiness.py` **empty**.
- **Gating (expected):** "no creds configured" is the **expected shadow state** and must stay benign-green (`ok=True`, informational). Only configured-but-stale → `degraded`/blocker. `read_secret_text_file` **raises** on missing-but-configured file — that fail-closed must be preserved.
- **Falsify:** (1) the legacy `if not status:` early-return still returns `ok=True` regardless of creds (configured-but-no-status must become `degraded`); (2) preflight turned the expected no-creds case into a hard failure (wrong direction); (3) `credentials_configured` missing from one of the three returns; (4) the raise swallowed into a silent empty string; (5) the test re-implements preflight classification instead of driving the production path.

### OPT-03 — Options data-quality monitors (freshness, coverage, greeks/bid-ask completeness, IV sanity)
- **Inspect:** `engine/data/options_data_quality.py` (`compute_options_data_quality`, `options_data_quality_ok`, metric + degradation-event emitters); additive `data_quality` block on all three `_options_ingestion_snapshot` returns in `engine/runtime/health.py`. Confirm reads are **read-only** over chain/state tables.
- **Tests:** `tests/test_options_data_quality.py`, `tests/test_options_ingestion_health_data_quality.py`.
- **Run:** the two import lines; the 2 pytest files; `ruff check .`; the **EMPTY** `git diff --stat` over `options_poll.py options_features.py storage_sqlite.py storage_live_ingestion_schema.py feature_registry.py options_context.py ingestion_soak.py metrics_store.py alerts.py`.
- **Gating (expected):** prefer no new table (persist via metrics + health); `data_quality` is **advisory only** and must NOT change the existing `ok`/`degraded`/`critical` verdict; `options_data_quality_ok` helper present but not yet wired into the live consumer (that's OPT-04). Thresholds are env-tunable defaults, not facts.
- **Falsify:** (1) legacy `options_chain` greeks/bid-ask completeness silently treated as "complete" instead of `0.0`; (2) bid/ask completeness computed from a SELECT that omits `bid/ask/theta/vega` (must SELECT them directly); (3) DQ returning `{"available": False, "ok": True}` masking a compute error as green; (4) degradation event not actually emitted via `put_normalized_event(event_kind="options_data_quality_degraded")`; (5) empty-chains path returning true-green instead of "unavailable".

### OPT-04 — Options-feature validation harness + evidence-gated `USE_OPTIONS_FEATURES`
- **Inspect:** `tools/options_feature_ablation.py` (`main`, pure `evaluate_enablement(report)->dict`, feature-set resolver **importing** `_BASE_OPTIONS_FEATURE_IDS`/`_OPTIONS_GEX_FLOW_FEATURE_IDS` from `feature_registry`, CPCV via `engine/backtest/cpcv.py`, fit via `gbm_regressor.train_gbm_model`); the new `OPTIONS_FEATURE_ENABLEMENT_PROTOCOL.md`. Registry/env must be **unchanged**.
- **Tests:** `tests/test_options_feature_ablation_verdict.py`, `tests/test_options_feature_ablation_feature_sets.py`, `tests/test_options_feature_registry_unchanged.py`, `tests/test_options_feature_ablation_smoke.py`.
- **Run:** `hasattr(main)` + `hasattr(evaluate_enablement)`; the registry-unchanged one-liner (`all(f not in OPTIONS_FEATURE_IDS for f in _OPTIONS_GEX_FLOW_FEATURE_IDS)`); the 4 pytest files; `python tools/options_feature_ablation.py --synthetic --min-rows 50 --min-gex-coverage 0.1` and confirm the JSON `verdict` ∈ `{ENABLE_SUPPORTED, ENABLE_NOT_SUPPORTED, ABSTAIN_INSUFFICIENT_DATA}`; the **empty** diff guard over registry/options-features/gbm/cpcv/learning files.
- **Gating (expected):** measurement apparatus only; `USE_OPTIONS_FEATURES` stays `0`. Below-floor data or missing LightGBM → ABSTAIN/skip, never silent pass.
- **Falsify:** (1) feature lists **duplicated as literals** instead of imported (delta becomes meaningless); (2) ABSTAIN not dominating below the row floor (large delta on tiny data faking ENABLE_SUPPORTED); (3) smoke test fabricating a pass on empty `options_chain_v2`/`labels`; (4) threshold presented as "validated" rather than configurable default; (5) CPCV not passed real `label_end_times`/`embargo` (leakage inflating OOS).

### OPT-05 — `broker_sim` options fills: ×100 multiplier, chain bid/ask fills, margin, MTM
- **Inspect:** `engine/execution/broker_sim.py` (`_is_option_symbol`, `_get_option_quote_at_or_before`, `_option_short_margin`; option sizing, fill call sites, affordability/margin, MTM in `broker_equity_at`, the `_broker_fills_columns`-gated writer); the new `0073_broker_sim_option_fields` migration (`contract_multiplier`, `option_quote_source`, `option_margin_debit`). Confirm it **consumes** OPT-01's `OptionContractMetadata.multiplier`.
- **Tests:** `tests/test_broker_sim_option_fills.py`.
- **Run:** `hasattr(b,'_is_option_symbol')` + `hasattr(b,'_get_option_quote_at_or_before')`; `options_readiness.LIVE_OPTIONS_BROKER_ADAPTERS==frozenset()` and `len(CONTROL_FLAG_GROUPS)==9`; the pytest file; **empty** diff over `options_readiness.py`/`asset_map.py`/`options_instrument.py`; diff over `broker_sim.py` + migrations shows **only** broker_sim + new 0073.
- **Gating (expected):** shadow-only; equity/crypto/FX fills **byte-identical** (new option columns NULL on those fills); option path engages only for OCC-classified symbols; no fresh `options_chain_v2` quote → order **SKIPPED**, never filled at underlying; margin model is reference-grade (no Reg-T claim).
- **Falsify:** (1) multiplier **hardcoded `100`** as a silent fallback instead of sourced from metadata (the "multiplier source" test seeds a non-100 value — confirm it's honored or the order fails-closed); (2) quote-less OCC order silently filled at underlying `prices` value instead of skipped (the test seeds a `prices` row to prove it's **ignored**); (3) MTM marking `qty*mid` not `qty*mid*100`; (4) `_exec_px` bypassed instead of layering chain spread via `spread_bps_override`; (5) short margin not debited / not labeled `option_sim_margin_reference`.

### OPT-06 — Greeks-based portfolio risk engine (delta/gamma/vega/theta aggregation + limits)
- **Inspect:** `engine/risk/portfolio_risk_engine.py` (`OPTIONS_MAX_PORTFOLIO_GAMMA_ABS`/`..._VEGA_ABS`/`USE_OPTIONS_GREEK_LIMITS`; `_option_greeks`, `_options_greek_snapshot`; the `options_greeks_within_cap` check in `_post_constraint_checks`; optional delta downsize in `_apply_portfolio_caps`; `info["options_greeks_post"]`; the block decision joining `block_reason["type"]=="options_greek_limit_breached"`); the 2 new gamma/vega entries appended to `options_readiness.NUMERIC_CONTROLS` only.
- **Tests:** `tests/test_portfolio_risk_options_greeks.py`, `tests/test_options_readiness_greek_controls.py`.
- **Run:** `apply_portfolio_risk_engine.__code__.co_varnames[:4]` == `('con','desired','state','now_ms')` (signature unchanged); `LIVE_OPTIONS_BROKER_ADAPTERS==frozenset()`; `NUMERIC_CONTROLS` includes both new names; the 2 pytest files; **empty** diff over `asset_map.py`/`options_instrument.py`/`broker_sim.py`/`hierarchical_allocator.py`.
- **Gating (expected):** greek caps **default disabled** (`<=0.0` sentinel = no limit) — an unset env must **never** block a non-options book; signature, existing gross/net/asset-class/MC enforcement, and `_DEFAULT_ASSET_CLASS_BUDGETS` unchanged (no OPTION entry here — that's OPT-08).
- **Falsify:** (1) greeks aggregation returning **zeros** for option rows (asset-class never returns OPTION, or `_option_greeks` always `None`) — net delta silently 0, no block; the exact 2-leg arithmetic test catches this; (2) ×100 multiplier hardcoded vs sourced from OPT-01; (3) the new check not actually joining the **block decision** (cosmetic key, no enforcement); (4) a non-trivial book blocked when caps are unset (fail-open violated); (5) margin-aggregation formula guessed instead of xfail/TODO.

### OPT-07 — Option position lifecycle (DTE roll/auto-close, expiry settlement, assignment/exercise, pin risk)
- **Inspect:** `engine/execution/options_lifecycle.py` (state machine, pure `plan_option_lifecycle_events`, `lifecycle_readiness_evidence`); `broker_sim.py::apply_option_lifecycle` writing **value-conserving** transitions via existing `_write_fill`/`_write_position`/`_read_account`/`_write_account`. Env: `OPTIONS_LIFECYCLE_ENABLED` (default `0`), `OPTIONS_LIFECYCLE_MODE` (default `shadow`), pin/roll knobs.
- **Tests:** `tests/test_options_lifecycle_planner.py`, `tests/test_options_lifecycle_apply_conservation.py`, `tests/test_options_lifecycle_readiness_unchanged.py` (+ regression `tests/test_options_instrument_readiness.py`).
- **Run:** import line; the 3 pytest files; the readiness regression; **empty** diff over `options_readiness.py`/`options_instrument.py`/`asset_map.py`/`storage.py`.
- **Gating (expected):** flag **OFF** → `apply_option_lifecycle` is a pure no-op (`{"ok":True,"processed":0,"skipped_disabled":True}`); does NOT flip `assignment_exercise`/`expiration_risk` gates or touch `CONTROL_FLAG_GROUPS`/`force_options_shadow_intent`/`frozenset()`; American early-exercise **not** modeled (reference-grade, at/after expiry only).
- **Falsify:** (1) **value-conservation broken** — settlement/assignment writes a fill without the matching cash debit/credit so book equity moves by an unbooked delta (the conservation test asserts equity Δ == realized intrinsic only); (2) planner mutating inputs or raising on garbage instead of `[]`; (3) guessed strike/expiry/right when metadata is `None` instead of no-op; (4) lifecycle running when the flag is unset (tables must be byte-identical before/after); (5) readiness gates accidentally flipped (the unchanged-snapshot test guards this).

### OPT-08 — Bind options allocation sleeve to `asset_class=OPTION` with budget enforcement
- **Inspect:** `engine/risk/portfolio_risk_engine.py::_DEFAULT_ASSET_CLASS_BUDGETS` (`"OPTION": 0.20`) enforced via the existing override loop + `_apply_asset_class_budgets`; `engine/runtime/hierarchical_allocator.py::_strategy_to_sleeve` new last-resort `_asset_class_to_sleeve` fallback (`OPTION->options`) behind `HIER_ALLOC_BIND_ASSET_CLASS_SLEEVE` (default `1`).
- **Tests:** `tests/test_option_asset_class_budget.py`, `tests/test_allocator_option_sleeve_binding.py`.
- **Run:** `ASSET_CLASS_BUDGETS['OPTION']<=...['EQUITY']` and `<...['UNKNOWN']`; allocator import; the 2 pytest files; **empty** diff over `asset_map.py`/`fx_instrument.py`/`options_readiness.py`/migrations; whole `git diff --stat` shows only the 2 runtime files + 2 tests.
- **Gating (expected):** governance math only; `0.20` strictly `< UNKNOWN 0.40` (can only tighten). Inert until OPT-01 lands → OPTION-symbol-path tests are `xfail` (legitimate, not missing). Operator override `PORTFOLIO_RISK_ASSET_CLASS_BUDGETS_JSON` must still win.
- **Falsify:** (1) a **non-OPTION budget constant silently changed** (assert EQUITY/CRYPTO/FX/RATES/COMMODITY/UNKNOWN byte-identical); (2) the sleeve fallback overriding an explicit `STRATEGY_SLEEVE_MAP_JSON`/`meta_map["sleeve"]` instead of being last-resort; (3) OPTION budget set looser than UNKNOWN; (4) binding asserted via a **guessed** classification rather than xfail when OPT-01 absent.

### OPT-09 — Options-native predictor: IV/realized-vol forecast + contract-selection layer
- **Inspect:** `engine/strategy/options_predictor.py` (`forecast_vrp`, `select_option_structure`, `build_options_shadow_intent` routing through `force_options_shadow_intent`, `run_options_predictor`; `USE_OPTIONS_PREDICTOR` default `0`); the new `0072_options_predictor_shadow` migration. Must NOT touch `predictor.py`/`model_intent.py`/`portfolio.py`/`champion_manager.py`.
- **Tests:** `tests/test_options_predictor_vrp.py`, `tests/test_options_predictor_selection.py`, `tests/test_options_predictor_shadow_gate.py`.
- **Run:** `hasattr(forecast_vrp)`, `hasattr(select_option_structure)`, `USE_OPTIONS_PREDICTOR is False`; `len(default_feature_ids())==111` (unchanged); the 3 pytest files; **empty** diff over `predictor.py`/`model_intent.py`/`portfolio.py`/`champion_manager.py`/`options_readiness.py`/`feature_registry.py`.
- **Gating (expected):** shadow-only, default-OFF, gated on an OPT-04 evidence record; `run_options_predictor` is a no-op when the flag is off or evidence absent; emitted intents must carry `execution_target="shadow"`, pass `force_options_shadow_intent`, and never reach live.
- **Falsify:** (1) `forecast_vrp` returning a **guessed default** (e.g. `0.0`) instead of `None` on missing surface IV/price history; (2) a fake/stubbed OCC builder for selected legs instead of OPT-01's parser (legs must round-trip); (3) emitted intent not actually shadow-stamped (`execution_target=="shadow"`, `competition["blocked"] is True`, `is_options_order(intent)` True via the **real** `force_options_shadow_intent` call); (4) import flipping `USE_OPTIONS_FEATURES` or changing `default_feature_ids()`; (5) fabricating an OPT-04 evidence record rather than xfail.

### OPT-10 — One concrete LIVE options broker adapter + the nine readiness gates *(capstone)*
- **Inspect:** `engine/execution/broker_tradier_options.py` (credential gating via `get_data_credential("TRADIER_API_TOKEN")`, `_real_trading_gate()` consulting `kill_switch.execution_allowed`, `apply_latest_portfolio_orders_live(dry_run=...)`); `broker_router.py` routing branch + `is_live` set membership; `options_readiness.py` — `"tradier_options"` added to `LIVE_OPTIONS_BROKER_ADAPTERS` **and** `LIVE_BROKERS`, the **9 rubber-stamp predicates replaced** with real `_GATE_PREDICATES` delegating to OPT-03/05/06/07 + `execution_allowed`, and per-gate `detail`.
- **Tests:** `tests/test_options_live_adapter_gates.py` (+ regression `tests/test_options_instrument_readiness.py`).
- **Run:** the import line; `'tradier_options' in LIVE_OPTIONS_BROKER_ADAPTERS`; `live_options_readiness_snapshot()` → `required is False` **and** `shadow_only is True`; the 2 pytest files; **empty** diff over `broker_alpaca_rest.py`/`broker_ibkr_gateway.py`/`kill_switch.py`/`options/tradier_live.py`.
- **Gating (expected):** **ships live adapter DISABLED.** With no env flags, `required` stays `False` and `shadow_only` stays `True`. Registering the adapter must NOT by itself allow any order — all 9 gates + numeric controls + per-order blockers must still pass. `dry_run=True` short-circuits before any HTTP. Tradier wire fields may be `xfail`+TODO if unconfirmed.
- **Falsify (this is the highest-risk ID):** (1) gates left as **rubber-stamp `_env_bool` reads** — flipping the env asserts "ready" with nothing wired; the negative test must prove a `*_check_failed` blocker when env is set but a real check is patched to fail (env **necessary but not sufficient**); (2) a missing prereq **lazy-imported and degrading to "pass"** instead of fail-closed blocked; (3) `dry_run`/missing-token path making a real network call or raising instead of returning a terminal block dict (test patches HTTP and asserts un-called); (4) default mode/`shadow_only`/`required` accidentally flipped; (5) existing blocker strings (`options_live_*_gate_missing`) renamed, breaking dashboards; (6) adapter registration alone letting an order through.

---

## 4. Cross-cutting verification (after all OPT-0X)

1. **Classifier ordering & no cross-class bleed.** Confirm in `asset_map.py::asset_class_for_symbol` that the OPTION branch never reclassifies EQUITY/FX/CRYPTO/FUTURES/COMMODITY/RATES, and bare roots (`SPY`,`GC`,`ZN`,`TLT`) are unaffected. This is shared with the equity/futures/FX/crypto enablements — a regression here breaks them too.
2. **Multiplier provenance is consistent.** The ×100 (or per-contract) multiplier parsed in **OPT-01** must be the single source consumed by **OPT-05** (fills/MTM) and **OPT-06** (greeks). Grep for any second hardcoded `100`/`* 100` in those paths and confirm it traces to `OptionContractMetadata.multiplier`.
3. **End-to-end shadow trace.** Pick one OCC symbol and trace: classify → (OPT-05) sim fill uses chain quote + multiplier → (OPT-06) greeks aggregate + cap check → (OPT-07) lifecycle no-op when disabled → (OPT-08) OPTION sleeve budget → (OPT-09) shadow intent stays `execution_target="shadow"` → (OPT-10) live path blocked. Record where the chain breaks (and whether the break is a legitimate gate or a real defect).
4. **Fail-closed invariants intact.** Re-assert at the end: `LIVE_OPTIONS_BROKER_ADAPTERS==frozenset()` only if OPT-10 not enabled; `OPTIONS_INSTRUMENTS_MODE=="shadow"`; `USE_OPTIONS_FEATURES` default `0`; `len(CONTROL_FLAG_GROUPS)==9`.
5. **Whole-suite + validators.** `python -m pytest tests/ -q -m safety_critical`, `python tools/validate_repo.py`, `python tools/syntax_check_workspace.py`, `python tools/git_worktree_triage.py`, `python tools/coverage_gate.py`, `python tools/pyright_money_path_gate.py`. Attribute any red to baseline vs change.

## 5. Report (write to `docs/handoff/verification/OPTIONS_ENABLEMENT_VERIFICATION_REPORT.md`)

**Deliver two things:** (1) **write** the full report to the path in the heading; and (2) **present a condensed
in-session summary** to the user in your final message — the roll-up verdict table, the GO / NO-GO line, and the
top blocking defects — so they get the verdict without opening the file.


- **Roll-up table:** `ID | verdict | runtime evidence (file:line) | tests pass? | validation exit codes | defects`.
- **Per-ID detail** with cited evidence for all five lenses.
- **Cross-cutting section** (classifier ordering, multiplier provenance, end-to-end trace, fail-closed invariants, suite/validators).
- **GAP ledger:** every TODO / xfail / "not executed in sandbox" / unconfirmed contract spec, classified **(legitimate-gated)** vs **(real-defect)**.
- **Final GO / NO-GO** for "Options enablement is correctly and completely implemented and functioning in the wider repo," with the explicit blocking list if NO-GO. GO requires: zero `FAKE-GREEN`/`BROKEN`/`MISSING`, every `PARTIAL` justified as legitimately-gated, all fail-closed invariants intact, and validators green vs baseline.
