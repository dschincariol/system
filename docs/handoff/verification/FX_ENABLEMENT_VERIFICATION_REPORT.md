# FX Enablement — Verification & Acceptance Report (FX-00 … FX-08)

> **Audit type:** independent, read-only acceptance audit (evidence-backed verdict, not feature work).
> **Date:** 2026-06-24 · **HEAD:** `39fc99f` "Stabilize repo for next additions" · **Tree state:** dirty working tree (the squashed snapshot bundles FX + equity + options + futures + crypto workstreams; findings below are attributed per-workstream).
> **Target repo:** `/home/david/gitsandbox/system/system` (`.venv`, Python 3.11.15, Node v20.19.4 / npm 10.8.2).
> **Method:** `TodoWrite` plan → baselines → 9 per-requirement auditors + 5 cross-cutting invariant auditors fanned out in parallel (each ran only its own targeted tests + per-ID one-liners + falsify traps) → orchestrator ran the shared validators + full suite once and independently spot-checked every high-blast-radius claim.
>
> **Supersedes** the earlier same-day report that concluded **NO-GO**. Its two blockers no longer hold on the **current** working tree (which evolved after that report): (1) "downstream FX metadata users bypass the canonical accessor" — independently disproven below (all consumers re-key through `fx_instrument`/`universe.get_instrument_metadata`; the only inline 3-letter slices are a same-currency guard, a documented FX-02-preferring fallback, and leverage tier-classification, none a divergent parser); (2) "validate_repo fails on local asset references" — `tools/check_local_asset_refs.py` now returns **EXIT 0**; the remaining `validate_repo` red is a different, **non-FX** cause (an undocumented EQUITY env var, §4.6).

---

## 0. Baselines (captured before any verdict)

| Baseline | Result |
|---|---|
| `git status --short` | dirty working tree (~172 files across all asset-class workstreams); no FX-owned file conflicts |
| `git log --oneline -15` | HEAD `39fc99f`; all enablement work squashed into the snapshot (no per-ID commits) |
| `pytest -q -m safety_critical` | **EXIT 0** — full pass to 100% (money-path safety net clean) |
| `tools/pyright_money_path_gate.py` | **EXIT 0** — "29 baseline errors, 0 baseline warnings, 30 target files" |
| `tools/git_worktree_triage.py` | **EXIT 0** — `"ok": true`, `"layout_violations": []` |
| `pytest -q` (full suite) | **EXIT 0** on baseline AND post-audit re-run (console truncates ~87% with no summary on *both* runs — a reproducible capture/flush artifact, **not** an early-exit: no `os._exit`/`pytest.exit`/`sys.exit(0)` in the test infra; `PIPESTATUS[0]=0` is authoritative) |

---

## 1. Roll-up verdict table

| ID | Verdict | Runtime evidence (file:line) | Tests pass? | Validation exit codes | Defects (FX-attributable) |
|----|---------|------------------------------|-------------|-----------------------|---------------------------|
| **FX-00** | **PARTIAL** (deliverable effectively PASS) | dossier `docs/handoff/research/FX_ENABLEMENT_RESEARCH.md:3` (`> NETWORK MODE: ONLINE`); repo anchors `services/data_source_manager.py:98,676` verified | n/a (docs-only, no `.py`) | `test -f`→0; `precommit_text_guards`→0; secret-scan grep→1 (PASS, no leak); `validate_docs`→1; `validate_repo`→1 | **NONE.** The two red validators fail only on an **exogenous EQUITY var** (§4.6), not FX-00 |
| **FX-01** | **PASS** | `oanda_live.py:161`; `provider_registry.py:159-169` (`OANDA_ENABLED` default-off, `supports.asset_classes==['fx']`); `default_symbols.py:46-54`; `factor_ingestion.py:220-309`; `cftc_cot.py:74-80` | 6 files, 20 tests → **0** | majors-regex→0; combined-import→0; `OANDA_ENABLED=1 supports==['fx']`→0; ruff→0; py_compile→0; triage→0; regression 52→0 | NONE |
| **FX-02** | **PASS** | `fx_instrument.py:91-149` (curated `KNOWN_CCY`, JPY pip, DXY); `asset_map.py:201`; `universe.py:587-633` accessor + `upsert_symbol`; `storage_sqlite.py:264-362` `_column_type`; migrations id 70/71 | 4+3 files, 17+8 → **0** (3 PG-apply skips gated) | parser→0; migration-id→0; asset-class→0; pytest→0 | NONE; `storage.py`/`table_classification.py` FX-neutral |
| **FX-03** | **PASS** | `feature_registry.py:431-494` (32 FX ids), `:94` `USE_FX_FEATURES` default-off, `:1407-1475` runtime gating via `compute_feature_snapshot`→`asset_class_for_symbol`; loaders `:1826-2398` compute real math | 3 files, 18 → **0** | parity→0; FX-gate→0; EQUITY-gate→0; `system_audit`→0 | NONE (`fx.event_*` documented permanent stub) |
| **FX-04** | **PASS** | `fx_clock.py:20,52` real `ZoneInfo` DST (EST 22:00Z vs EDT 21:00Z); `labeling.py:82-89`; `backfill:120-141`; `regime_stack.py:486,773-775` (merged into **macro**); `predictor.py:2419,2443,2510` | 5 files, 13 → **0**; regression 22/5-skip → **0** | pytest→0; DST probe→0; protected-file diffs empty/owner-exempt | NONE (`hmm_regime.py`/`ridge_meta.py` untouched) |
| **FX-05** | **PASS** | `fx_sizing.py:111-114` notional+lots (not shares), `:175-185` sign-preserving clamp; `fx_leverage_caps.py:96,114-124`; `portfolio_risk_engine.py:190` `"FX":0.50`, `:1959` `_asset_class_for=="FX"`, `:1950` hard-block, `:2587-2621` shared-ccy edges | 5 files, 11 + live_thresholds → **0**; 2 safety_critical → **0** | pytest→0; safety_critical→0; import→0 | NONE; seam UNTOUCHED; `storage.py` zero diff |
| **FX-06** | **PASS** | `broker_ibkr_gateway.py:1262-1269` dispatcher at 4 sites (1733/2256/2583/2646), FX→CASH/IDEALPRO, STK byte-identical; `fx_session.py:19-23` derives from `fx_clock`; `execution_policy_engine.py:1131-1156` default-on; `broker_router.py` gates BEFORE FX reorder | 5 files, 18 + router → **0** | pytest→0; `broker_oanda_rest.py` absent (correct); contract one-liner→0 | NONE (OANDA exec adapter correctly absent) |
| **FX-07** | **PASS** | `fx_costs.py:124-308` (pip/swap/weekend, FX-02 pip first); `broker_sim.py:1506-1590` `is_fx`-gated; `cpcv.py:329-368`; `fx_profitability_report.py:15-228` calls REAL governance fns | 6 files, 19 → **0**; regression 30 → **0**; 2 safety_critical → **0** | pytest→0; collect-only→0; hand bps match (EUR/USD 0.72727, USD/JPY 0.68966) | NONE (gate-math/`_exec_px`/seam untouched); minor test-quality note (§3) |
| **FX-08** | **PASS** | `fx_format.js:35-81`, `fx_session.js:13-179` (America/New_York DST), called at `terminal.js:502-1077`, `dashboard.js:5273-5365`; placeholders `:5327,5534+`; secret whitelist `data_sources.js:782-795` | 4 files (node 9/9, pytest 7/7) → **0** | node→0; pytest→0; `check_local_asset_refs`→0; `check_dashboard_ui_contract`→0; `npm check:ui`/`test:ui`→0 (122/122) | NONE (no read-model field; cosmetic canary note) |

**Cross-cutting invariants:** single-clock **PASS** · single-metadata **PASS** · unowned-seams **PASS** · classifier-ordering **PASS** · default-off-parity **PASS** (details §4).

---

## 2. Per-requirement detail (five lenses)

### FX-00 — Research dossier *(docs-only)* — PARTIAL → deliverable effectively PASS
- **Runtime:** intentionally no runtime. Dossier is a pure `.md`; cited repo anchors (`services/data_source_manager.py:98` credential keys incl. `OANDA_ACCESS_TOKEN`, `:676` wiring) independently confirmed. Network-mode marker present/exact at line 3.
- **Validation:** `test -f`→0; `precommit_text_guards.py`→0; secret-scan grep→**1 (PASS** — only `OANDA_ACCESS_TOKEN=<redacted-do-not-commit>` placeholder + env-var names); `validate_docs.py`→**1**; `validate_repo.py`→**1**.
- **Falsify:** (1) fabricated citations → **refuted** (OANDA primitives `[S4]` WebFetch-confirmed; `[S8]` IBKR HTTP 403 bot-block = real domain). (2) `.py` smuggled → **refuted**. (3) real secret → **refuted**.
- **Why PARTIAL, why effectively PASS:** the dossier is honest and complete; the only non-green signals are `validate_docs`/`validate_repo`, red **solely** on the exogenous EQUITY var `ALERT_EXEC_COST_FILTER_ASSET_CLASSES` (§4.6) which is **absent** from the dossier and belongs to another workstream's `.py`. No FX-00 defect.

### FX-01 — FX data source + ingestion — PASS
Read-only OANDA v20 polling adapter (`oanda_live.py:161`, token only in `Authorization` header `:187`); default-OFF fx-only polling provider; IBKR `supports` includes `"fx"`; FX-major seeds + bidirectional OANDA converters → `poll_prices.oanda_map`; 5 FRED FX macro specs + 7 FX COT specs as raw rows; `ingest_cftc_cot` default-off. 20 tests pass; **canary token asserted absent** from rows/logs/route payloads. Regression 52 passed. All four traps refuted (live probe = declared mock-only GAP; Japan FRED id spec-guarded; no token leak; CCXT stays crypto-only).

### FX-02 — First-class FX instrument model *(keystone)* — PASS
`parse_fx_symbol` is curated-`KNOWN_CCY`-gated (rejects `GOOGLE`/`BTCUSD`/`""`), JPY pip 0.01, DXY index; the **single accessor** `universe.get_instrument_metadata` (imported by FX-03/05/07) lives in `universe.py` not `storage.py`; `upsert_symbol` canonicalizes to `EURUSD` and writes 9 columns; `_column_type` forces REAL for pip/contract/leverage and TEXT for `pnl_ccy` **before** the `"pnl"` substring heuristic. 17+8 tests pass (3 PG-apply skips correctly gated). All five traps refuted incl. independent `_column_type` probe and migration immutability. **Orchestrator confirmed** ids 70/71 contiguous, additive-only, `storage.py` zero diff.

### FX-03 — FX feature groups + train/serve parity — PASS
32 FX ids as real constants; gating **enforced in the live path** (`resolve_feature_ids`→`_apply_asset_class_feature_gating`, reached by `compute_feature_snapshot` via `asset_class_for_symbol`); loaders compute real transforms (not FX-01 stubs); `fx.event_*` = one documented permanent 0.0 stub; COT consume-only. 18 tests pass with **registered** equity-only ids. **Orchestrator confirmed** `expected_columns()`==`expected_columns(asset_class=None)` (both 111, byte-identical). GAP (legitimate): only sandbox structural-zero path exercised.

### FX-04 — Routing, FX regime, FX-correct labels *(canonical clock)* — PASS
`fx_clock.py` real `ZoneInfo` DST (runtime: EST 22:00Z vs EDT 21:00Z); FX label/backfill (`fx_clock_corrected` rides existing sqlite `meta_json`, **no PG JSON column**); FX regime merged into **macro** (visible to flatten/compatibility); predictor regime-context fixes hardcoded `"SPY"`. 13 FX + 22/5-skip regression pass, with canary guards. All five traps refuted (real DST; macro-merge; no `ALTER TABLE`; non-FX output byte-identical; adapters/`hmm_regime`/`ridge_meta` untouched).

### FX-05 — Currency-aware sizing + risk — PASS
`fx_weight_to_notional` → base/quote notional + lots (never shares); sign-preserving `min(instrument, regulatory)` clamp; `_asset_class_for` keys the FX bucket so `"FX":0.50` sleeve binds; `_apply_fx_leverage_caps` **fail-closes** missing rate → `fx_leverage_hard_block` → `info["blocked"]`; structural shared-ccy correlation edges. 11 items + live_thresholds + 2 safety_critical pass (zero skips); leverage test covers positive correct-math clamp (35→30). All five traps refuted; account-ccy grep EXIT 1 (metadata-only). **Orchestrator confirmed** seam untouched, `storage.py` zero diff.

### FX-06 — FX execution + broker routing + 24/5 session — PASS
`_mk_contract_for_symbol` dispatcher live at all 4 order sites; FX→CASH/IDEALPRO; **equity STK byte-identical** (runtime-verified AAPL STK/SMART/USD); `fx_session.py` **derives** from FX-04's clock (imports `fx_market_closed`/`fx_forward_eval_ms`); session adjustment in live `apply_execution_policy` (default-on `EPE_FX_SESSION_ENFORCE`); router runs all three gates **before** the FX reorder (spy-test asserts each called). 18 FX + router baselines pass. Traps 1/2/4 refuted; trap 3 (source-string vs live-submit) is the **explicitly-accepted mode-invariant GAP**. `broker_oanda_rest.py` correctly absent.

### FX-07 — Backtest realism, governance, profitability — PASS
`is_fx`-gated cost terms in the **live** offline cost path of both `broker_sim._offline_ac_cost_components` and `cpcv._cost_components_for_turnover`; non-FX output **byte-identical**; profitability report **calls** real `run_gated_backtest`/`cpcv_backtest`/`passes_promotion_gate`/`assess_challenger` (`persist=False`) and never promotes. 19 FX + 30 regression + 2 safety_critical pass. All five traps refuted; gate-math files + `_exec_px` zero diff; hand-computed bps match runtime. Minor (non-blocking): unit test recomputes expected bps from module constants — values hand-verified correct (§3).

### FX-08 — Operator UI surfacing *(display-only)* — PASS
Pip/lot/price/session helpers real and **called** at live render sites in `terminal.js`/`dashboard.js`; session mirror boundary-equivalent to FX-04 (proven identical at every DST instant; the diff **fixed** a prior Sunday-open divergence); every FX value degrades to "data not yet available" placeholders (expected gated state); secret whitelist blocks leakage; **no read-model field added**. node 9/9 + pytest 7/7 pass; all 4 tests **wired into** `run_ui_checks.mjs` allowlists (orchestrator-confirmed lines 29-30, 41-42); `npm check:ui`/`test:ui` 122/122. Minor (non-blocking): one canary test asserts absence of a fresh random UUID (§3).

---

## 3. Two minor (non-blocking) test-quality notes
- **FX-07:** pip→bps unit test derives `expected` from the same module constants/formula. Mitigated: bps independently hand-verified (EUR/USD 0.72727, USD/JPY 0.68966 bps).
- **FX-08:** one canary test asserts absence of a fresh random UUID (no detection power). Mitigated: the real leak guard (positive whitelist `data_sources.js:782-795`) is verified by sibling tests.

Neither is a runtime defect; both are recommendations.

---

## 4. Cross-cutting verification

### 4.1 Single canonical clock — PASS
`fx_clock.py` is the **only** FX weekend-boundary authority (Fri 17:00 ET → Sun 17:00 ET, `ZoneInfo America/New_York`). `fx_session.py` (FX-06) imports and delegates (`_market_closed`/`_next_open_ms` → `fx_market_closed`/`fx_forward_eval_ms`); its local zoneinfo is an **intraday rollover annotation only**. `ui/fx_session.js` (FX-08) mirrors the same default via DST-aware `Intl.DateTimeFormat`. Repo-wide grep for a second weekend clock → **EXIT 1 (none)**. Auditor ran both Python and JS clocks over 8 EST/EDT instants — JS `.open` is the exact inverse of Python `fx_market_closed` at every instant. A guard test forbids a second UTC clock. **Orchestrator independently confirmed** the import/delegation + `ZoneInfo` usage.

### 4.2 Single canonical symbol/metadata source — PASS
`fx_instrument.parse_fx_symbol` is the sole base/quote/pip authority. Consumers re-key through it / `get_instrument_metadata`: FX-05 `fx_sizing.py:72,82` (accessor + parser fallback) and `portfolio_risk_engine.py:324,903`; FX-03 `feature_registry.py:1766,1786`; FX-07 `fx_costs.py:132`. **Orchestrator independently hunted** for a divergent inline parser: the only inline 3-letter slices are (a) `fx_session.py:66` a *same-currency guard* (after `asset_class_for_symbol=="FX"`), (b) `fx_costs.py:162-163` a documented FX-02-preferring *fallback* in `normalize_fx_symbol` (reached only when `parse_fx_symbol` returns `None`), and (c) `fx_leverage_caps.py:68` *tier classification* (G10 major/minor/exotic) — none derives metadata in place of the authority. **No divergent second parser** → the prior report's "downstream bypass" blocker does not hold on the current tree.

### 4.3 The two deliberately-unowned seams are correctly unowned — PASS
- **(a) `broker_sim` weight→qty/lots** (`broker_sim.py:3074-3076`, relocated from the stale ~2388 anchor): still the generic `target_qty = (to_w*equity)/float(px_mid)`, flagged `NO-GO-pending-owner`, imports **no** FX sizing/instrument helpers (grep → EXIT 1). An honest self-audit test reads the real source and asserts the generic formula present + FX helpers absent (pass). **Orchestrator independently confirmed.**
- **(b) Account-currency P&L conversion** is **not** silently faked in the FX path (grep `currency_conversion_rate|account_price|account_ccy` over `engine/execution` + `fx_sizing` + `fx_instrument` → EXIT 1). FX-05 attaches metadata only (`pnl_ccy`). The repo's only account-ccy conversion is the **futures-margin** path (`futures_margin.py:36`), correctly scoped.

### 4.4 Classifier ordering & no cross-class bleed — PASS
`asset_map.py:179-208` orders the FX branch (line 201) **after** `ASSET_CLASS_MAP_JSON` override, `_DEFAULT`, equity/crypto/commodity heuristics, futures (197), option (199). **Orchestrator independently ran** the classifier: `DXY→FX`, `SPY→EQUITY`, `EURUSD→FX`, `BTCUSDT→UNKNOWN` (not stolen), `AAPL→EQUITY`; `is_fx_symbol` strict. `ASSET_CLASS_MAP_JSON` precedence preserved.

### 4.5 Default-off / no-regression invariants — PASS
`OANDA_ENABLED`/`USE_FX_FEATURES`/`USE_FX_REGIME` all default-off (source + runtime). `expected_columns(asset_class=None)` byte-identical to no-arg (111 cols; even with both flags forced on, the default serving surface stays 111 cols with zero `fx.` columns). Migration `0071` is **additive-only** (`ADD COLUMN IF NOT EXISTS`, id 71, does not renumber/edit `0070`). Non-FX cost/label/sizing output byte-identical.

### 4.6 Whole-suite + validators (run once by orchestrator)
| Validator | Exit | FX-attributable? |
|---|---|---|
| `pytest -q -m safety_critical` | **0** (full pass) | — green |
| `tools/pyright_money_path_gate.py` | **0** (29 baseline, unchanged) | — green |
| `tools/git_worktree_triage.py` | **0** (`ok:true`) | — green |
| `pytest -q` (full suite) | **0** (console truncates ~87% = reproducible capture artifact; no hard-exit; safety_critical completes clean) | — green |
| `tools/check_local_asset_refs.py` | **0** | — green (resolves the prior report's local-asset-ref blocker) |
| `tools/validate_repo.py` | **1** | **NO** — fails only at the `docs` stage on a **single** undocumented var `ALERT_EXEC_COST_FILTER_ASSET_CLASSES` read at `engine/strategy/edge_filter.py:30` (an **EQUITY** workstream var, EQ-10; `edge_filter.py` has zero FX references). **All FX env vars are documented** (none flagged). |
| `tools/coverage_gate.py run` | **1** | **NO** — measured **5.76% whole-repo** (engine/risk 7.27%, engine/execution 13.28%, engine/runtime 8.08%); the "zero-covered" list contains the **entire codebase** incl. `broker_apply_orders.py` (which the passing `safety_critical` suite provably exercises) and every migration `0001-0077`. A **subprocess-coverage measurement artifact** of a bare sandbox invocation (this multiprocess/supervisor architecture runs logic in subprocesses uncaptured without `COVERAGE_PROCESS_START`), **identical regardless of FX** — `fx_session.py` is zero-covered beside long-stable modules. Not an FX coverage regression. |

---

## 5. GAP ledger

| Gap | Classification |
|---|---|
| Live OANDA connection probe is mock-only | **legitimate-gated** — declared GAP; no OANDA execution adapter by design |
| 4th (Japan) FRED short-rate id not live-fetch-verified offline | **legitimate-gated** — spec-registered + TODO/xfail-style guard |
| `fx.event_*` feature group always 0.0 | **legitimate-gated** — documented **permanent stub** |
| FX-03 live/non-zero feature values unverified end-to-end | **legitimate-gated** — no FX-01 data in sandbox; loader math verified by Read |
| `broker_sim.py:3074-3076` weight→lots seam | **legitimate-gated** — the **required** NO-GO-pending-owner state; untouched, not faked |
| Account-currency P&L conversion | **legitimate-gated** — out of scope; metadata-only; not faked (grep EXIT 1) |
| FX-06 dispatcher live non-dry_run submit branch not executed | **legitimate-gated** — explicitly-accepted mode-invariant source-test GAP |
| FX-08 every FX dashboard/terminal value = placeholder | **legitimate-gated** — the **expected** sandbox state; placeholders, not fabricated data |
| `validate_repo.py` / `validate_docs.py` EXIT 1 | **real-defect, NON-FX** — owned by the EQUITY workstream (undocumented `ALERT_EXEC_COST_FILTER_ASSET_CLASSES`); flag for repo owner, **not an FX blocker** |
| `coverage_gate.py run` EXIT 1 (5.76%) | **environment/measurement, NON-FX** — subprocess coverage uncaptured in bare sandbox; whole-repo, not FX-specific; FX modules have dedicated passing tests |
| FX-07 / FX-08 minor test-quality notes (§3) | **non-blocking** — mitigated; recommendations only |

---

## 6. Final GO / NO-GO

### ✅ GO — the FX enablement workstream (FX-00 … FX-08) is correctly and completely implemented, wired, and functioning.

**GO criteria, all satisfied:**
- **Zero `FAKE-GREEN` / `BROKEN` / `MISSING`** across all 9 requirements and 5 cross-cutting invariants. FX-01…FX-08 are PASS; FX-00's only blemish is two shared-tree validators reddened by an **exogenous EQUITY var**, so its own deliverable is effectively PASS.
- **Both deliberately-unowned seams confirmed correctly-unowned (not faked)** — independently re-verified.
- **Single-clock and single-metadata invariants intact** — one `fx_clock.py` (FX-06/FX-08 derive, boundary-equivalent across 8 DST instants); one `fx_instrument.py` (all FX-0X re-key through it; no divergent parser).
- **Default-off parity proven** — all three FX flags default-off; `expected_columns(asset_class=None)` byte-identical; migration `0071` additive-only; non-FX output byte-identical.
- **FX-relevant validators green vs baseline** — `safety_critical` (0), pyright money-path (0, unchanged), full suite (0), `git_worktree_triage` (0), `check_local_asset_refs` (0). Independent orchestrator spot-checks corroborated every high-blast-radius claim.

**Two NON-blocking, NON-FX repo-level items (flagged for the owner; they do not gate FX and account for the only red validators):**
1. `validate_repo.py`/`validate_docs.py` red — the **EQUITY** workstream must document `ALERT_EXEC_COST_FILTER_ASSET_CLASSES` in `.env.example` / glossary / allowlist.
2. `coverage_gate.py` not meaningfully measurable via a bare sandbox `run` (subprocess coverage uncaptured → 5.76% whole-repo); rerun under CI's `COVERAGE_PROCESS_START` harness for a real number. FX's own modules are covered by dedicated, passing tests.

> **Reconciliation with the prior NO-GO:** both prior blockers are resolved on the current tree — the metadata-accessor "bypass" is disproven (§4.2, no divergent parser; all consumers re-key through the authority) and `check_local_asset_refs` is now green. The remaining red validators are demonstrably **non-FX** (EQUITY docs var; whole-repo coverage measurement artifact). Therefore the FX workstream is **GO**; the repo as a whole still needs the two non-FX items cleared before a fully-green validator state, but neither impugns FX correctness or completeness.

**Recommendations (optional, non-blocking):** add a hardcoded-literal bps assertion to `test_fx_costs_unit.py` (FX-07); replace the random-UUID canary in `test_fx_ui_no_secret_leak.py` with a fixed planted-secret fixture (FX-08).

---
*Report generated by independent read-only acceptance audit. Verdicts derive from runtime source reads, targeted test runs with literal exit codes, executed falsify traps, and orchestrator-level independent spot-checks — not from any prior verification report or self-audit doc.*
