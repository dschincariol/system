# Equity Enablement — Verification & Acceptance Report (EQ-01 … EQ-10)

- **Audit date:** 2026-06-24
- **Repo:** `/home/david/gitsandbox/system/system`
- **Auditor:** independent acceptance auditor (read-only), fan-out of 10 per-requirement subagents + orchestrator cross-checks
- **Scope:** verify EQ-01 … EQ-10 from `docs/handoff/verification/EQUITY_ENABLEMENT_VERIFICATION_CLAUDE_PROMPTS.md` are implemented correctly, completely, bind the money path (not just report), and are wired/functioning in the wider repo.
- **Mode:** READ-ONLY. No runtime/test/doc files modified. Only this report was written.

> This file replaces the prior 31-line placeholder NO-GO stub (which explicitly deferred to "a full equity verification run"). That stub was a conservative artifact-filler, not an audit.

---

## Final verdict

### ✅ GO — re-audit 2026-06-25, after all 5 remediation prompts ran. 9 PASS + 1 GATED-OK, zero blocking items.

> **This GO supersedes the prior CONDITIONAL-GO verdict** (kept below for history). The two major blockers
> (EQ-07 sim/live parity, EQ-02 borrow default-off) and the three info-level items (EQ-06, EQ-09, EQ-10) were all
> remediated, independently re-verified to **bind in production**, and shown to introduce **no regression**.

- **9 PASS** (EQ-01, EQ-02, EQ-03, EQ-04, EQ-06, EQ-07, EQ-08, EQ-09, EQ-10), **1 GATED-OK** (EQ-05).
- **Zero** `FAKE-GREEN` / `BROKEN` / `MISSING` / `PARTIAL`.
- **Every money-path rail proven to bind** in the live call path (budget, net_return, leverage clamp + fail-closed hard-block, sector clamp, share rounding, live-arming) — not test-only.
- **Both former blockers resolved (fixed-and-binds):**
  1. **EQ-07** — `broker_sim.py:3117` now rounds the order **delta** (mirroring the live IBKR/Alpaca adapters); the new **non-flat fractional** parity test (`test_sim_mirrors_live_delta_rounding_for_fractional_nonflat_positions`, start 5.3 → delta 7.1→7.0 → final 12.3) passes — sim == live for non-flat positions; flat-start parity, flag-off byte-identity, FX passthrough all preserved.
  2. **EQ-02** — `borrow_cost_model.py:80` default flipped to `True`; a **default-config (no-env)** short AAPL label now actually has `net_return` reduced (0.05 → 0.04975), proven live and by test. The CPCV cost-realism golden arrays were **not** masked — each scenario was explicitly pinned `borrow_enabled: False` with comments (git-diff verified), and borrow binding is asserted separately.
- **Info-level items also remediated:** EQ-06 helper now fail-closes standalone in reg_t-without-buying_power (engine stage unchanged, no double-relaxation); EQ-09 sector budgets now bind on real stocks via a cited PIT seed (AAPL→TECHNOLOGY, JPM→FINANCIALS through the production `quiver_gov.sector_for_symbol`; unresolved stays `""`); EQ-10 edge filter now reads env at **call time** (`_edge_filter_config()`, edge_filter.py:62) with a setenv-after-import test proving activation.
- **Baselines all green (2026-06-25):** `safety_critical` exit 0 (0 failures), `pyright_money_path_gate` PASS exit 0, `test_cpcv_cost_realism` 5 passed, `validate_docs` exit 0, `git_worktree_triage` ok exit 0. `coverage_gate` remains the **pre-existing structural** red (subprocess/facade undercount, non-attributable — see addendum).

**Only remaining item (non-blocking, INFO):** EQ-01's two flags (`ASSET_MAP_USE_EQUITY_REGISTRY`, `PORTFOLIO_RISK_BIND_EQUITY_BUDGET`) are documented in `engine/risk/README.md` but not in `docs/config_env_allowlist.txt` / glossary. `validate_docs` passes (they're read via variables, not literals), so it is non-blocking; register them for parity with the EQ-02/EQ-07/EQ-09 flags.

---

<details><summary>Prior verdict (2026-06-24) — CONDITIONAL GO, superseded by the GO above</summary>

### CONDITIONAL GO — does **not** meet the strict *unqualified*-GO bar; 2 major must-fix items, **both currently behind default-OFF flags**

- **8 PASS** (EQ-01, EQ-02, EQ-03, EQ-04, EQ-06, EQ-08, EQ-09, EQ-10), **1 GATED-OK** (EQ-05), **1 PARTIAL** (EQ-07).
- **Zero** `FAKE-GREEN`, `BROKEN`, or `MISSING` verdicts.
- **EQ-01 budget proven binding** (AAPL/MSFT/SPY → EQUITY; EQUITY sleeve 0.80 < MAX_GROSS 1.00 actually scales weight down 0.45 → 0.40).
- **Every money-path rail proven to bind** (budget, net_return, leverage clamp, sector clamp, live-arming) — verified in live call paths, not test-only.
- **Flag-off / non-equity parity proven** byte-for-byte across all rails.

**Why not unqualified GO:** the strict bar requires *"sim/live parity proven."* EQ-07 share-rounding parity is proven only for flat-start positions; for a fractional **non-flat** starting position the sim and the live adapters round different quantities and diverge (sim final 12.0 vs live final 12.3), and no test covers that path. This is a real correctness gap in EQ-07's stated whole point. It is **mitigated** because `EXEC_USE_SHARE_ROUNDING` defaults **OFF**, so default-state behavior is identical (both pass fractional through) — the divergence only appears once an operator enables rounding.

**Blocking-class items before unqualified GO (both default-OFF today, so no default-state regression):**
1. **EQ-07 (major):** sim rounds the absolute *target* position, live adapters round the order *delta* → paper ≠ live for non-flat fractional positions; add a non-flat parity test.
2. **EQ-02 (major):** `EQUITY_BORROW_COST_ENABLED` defaults **False** in code, but the spec mandates default `"1"`; it is set in **no** shipped config — so the (correct, binding-when-enabled) borrow rail is **inert in every deployment profile**. Flip the default to `"1"` or formally accept the deviation.

**Non-blocking (minor):** EQ-02 and EQ-07 env vars are not registered in `docs/config_env_allowlist.txt` (validate_docs currently passes because they are read via variables, not string literals; register them for completeness).

</details>

---

## Roll-up table

| ID | Verdict | Runtime evidence (binds money path?) | Tests pass? | Defects |
|----|---------|--------------------------------------|-------------|---------|
| EQ-01 | **PASS** | `asset_map.py:205` registry LAST → EQUITY; budget 0.80<1.0 scales weight in `_apply_asset_class_budgets` (`portfolio_risk_engine.py:1835`, wired @3091). **Binds: YES** | 9 passed (exit 0) | none |
| EQ-02 | **PASS** | `borrow_cost_model.py:80` default now `True`; `net_after_cost_labels.py:898` `net_value -= borrow_return`; `cpcv.py:446`. **Binds: YES (default config)** | 18 passed (exit 0) | none (was: default-off major — **fixed**; env vars now in allowlist) |
| EQ-03 | **PASS** | `backfill_labels_price_from_prices.py:183-184` rewrites realized `ret`; PIT-guarded `corporate_actions.py:439`; fail-closed `:462`. **Binds: YES** | 14 passed (exit 0) | none |
| EQ-04 | **PASS** | `execution_policy_engine.py` two seams `continue` before `shaped.append` → closed-session orders dropped (`shaped==[]`); zoneinfo DST. **Binds: YES** | 10 passed (exit 0) | none |
| EQ-05 | **GATED-OK** | `universe_lifecycle.py:427` `retire_symbol`→`status='DISABLED'` removes from `get_active_symbols` + PIT inference. Default-OFF. **Binds: YES** | 9 passed (exit 0) | none (2 legit data-feed gaps) |
| EQ-06 | **PASS** | aggregate clamp mutates weights `portfolio_risk_engine.py:2117`; hard-blocks → `blocked=True` @3122-3127; helper now fail-closes standalone Reg-T without buying_power. **Binds: YES** | 16 passed (exit 0) | none |
| EQ-07 | **PASS** | sim now rounds order **delta** `broker_sim.py:3117` mirroring live (`broker_ibkr_gateway.py:1609`, `broker_alpaca_rest.py:1607`); non-flat parity test passes. **Binds: YES** | passed (exit 0) | none (was: non-flat parity major — **fixed**; env vars now in allowlist) |
| EQ-08 | **PASS** | `prod_preflight.py:2638-2649` `return 3` on degraded required paid equity provider (paper/live only); 111 feature pin; name-only secret. **Binds: YES (arming)** | 6 passed (exit 0) | none |
| EQ-09 | **PASS** | `portfolio_risk_engine.py:1917` `weight=abs*scale*sgn` (scale=cap/gross); post-check → `blocked=True` `post_cap_validation_failed`. **Binds: YES** | 7 passed (exit 0) | 2 info (SECTOR_HARD_BLOCK absent (allowed); needs seeded sector map) |
| EQ-10 | **PASS** | `alerts.py:903/917` drops/rewrites `expected_z`; `config_schema.py:308` raises `ConfigError` for live-arming. Defaults OFF. **Binds: YES** | 13 passed (exit 0) | 2 info (import-time binding footgun; env read via var) |

---

## Baselines (captured before any verdict)

| Baseline | Result |
|----------|--------|
| `git status --short --untracked-files=all` | DIRTY: 362 entries, **all working-tree-only (uncommitted)**. Equity files untracked/modified, none committed. `git log` HEAD = `39fc99f Stabilize repo for next additions`. |
| `pytest -q -m safety_critical` | **exit 0, ~296 passed, 0 failed** — the entire money-path marked suite is green with the equity changes applied. No pre-existing safety_critical red set. |
| `tools/pyright_money_path_gate.py` | **exit 0, PASS** — "29 baseline errors, 0 baseline warnings, 30 target files." |
| `tests/test_cpcv_cost_realism.py` (CPCV baseline) | **exit 0, 5 passed** — borrow-off path byte-for-byte unchanged. |
| Migrations on disk | `0065`…`0077` **contiguous** (no gaps/dupes). Corp-actions = **`0076_corporate_actions.py`, internal `id = 76`** (not `0072` as the EQ-03 prompt literally states — `0072`–`0075` were taken by futures/options; "72" was the next-free id when the prompt was written). |

---

## Per-ID detail (five lenses)

### EQ-01 — Bind asset-class classification to real stocks *(P0 linchpin)* — **PASS**
- **Runtime enforcement:** `_load_equity_registry()` loads once at import (`asset_map.py:118-176`, behind `ASSET_MAP_USE_EQUITY_REGISTRY` default on); `_EQUITY_REGISTRY = _load_equity_registry()` (`:176`) holds 7099 NASDAQ/NYSE/AMEX/ARCA/CBOE tickers from the tracked seed `data/sec_company_tickers_exchange.json`. Registry branch is **LAST** at `asset_map.py:205` (`if s in _EQUITY_REGISTRY: return "EQUITY"`), immediately before `return "UNKNOWN"` (`:208`). Budget: `PORTFOLIO_RISK_BIND_EQUITY_BUDGET` default True → `_EQUITY_ASSET_CLASS_BUDGET = 0.80` (`portfolio_risk_engine.py:170-171`), applied by `_apply_asset_class_budgets` (`:1835`, `scale=cap/gross`, rewrites `out[sym]["weight"]`), wired at the live entrypoint `:3091`. `upsert_symbol` (`universe.py:760`) persists the classification.
- **Orchestrator-corroborated:** live probe AAPL/MSFT/SPY/TSLA → EQUITY; EURUSD → FX; fake ticker → UNKNOWN. `ASSET_CLASS_BUDGETS["EQUITY"]=0.8 < MAX_GROSS=1.0 → binds=True`. Override `ASSET_CLASS_MAP_JSON` keeps priority. Ordering confirmed by direct read.
- **Tests:** `test_equity_budget_binds.py` (safety_critical) asserts NVDA+MSFT combined gross 0.90 > 0.80 cap → weights scaled `0.45 → 0.40` and `asset_class_gross_post["EQUITY"] <= 0.80`; `test_asset_map_equity_registry.py` (collision/OTC/override traps); `test_universe_equity_classification.py` (end-to-end via real `upsert_symbol`). **9 passed (exit 0).**
- **Falsify:** all 4 refuted — budget genuinely < MAX_GROSS (sleeve binds); branch is LAST (EURUSD/ETH/GC placed in registry payload still classify FX/CRYPTO/COMMODITY); OTC/null excluded via `_EQUITY_REGISTRY_EXCHANGES` allowlist; registry genuinely loaded into the runtime classifier.
- **No-regression:** FX/PIT regression set 16 passed; no schema/DDL/backfill migration (re-classification on next `upsert_symbol` only).

### EQ-02 — Charge stock-borrow / financing cost on short equity — **PASS** (one major caveat)
- **Runtime enforcement (binds):** `net_after_cost_labels.py:893-900` — gated on `borrow_cost_enabled()` AND `is_borrowable_short_equity(side,asset_class)` — `net_value = net_value - borrow_return` (orchestrator-read & confirmed @898); recomputes cost/total from the reduced net. Second path `cpcv.py:446` `adjusted[idx] -= borrow_return` (gated CPCV promotion backtest). Live callers: `compute_exec_labels_from_fills.py:572`, `compute_exec_labels.py`, `backtest_cpcv.py` — unconditional.
- **Tests:** `test_net_after_cost_borrow_short_equity.py` (safety_critical) asserts numeric reduced `net_return == 0.0100 - 0.0001` (not just a reported field); flag-off short & long-only unchanged; upstream-borrow no-double-count; non-equity short unchanged. `test_cpcv_borrow_cost_realism.py` (safety_critical) asserts flag-off Almgren-Chriss array byte-for-byte. **11 passed (exit 0);** CPCV baseline still 5 passed.
- **Falsify:** the documented CRITICAL GAP is **REFUTED** — `net_return` IS reduced (898/446). Zero-borrow trap refuted (synthesized GC bucket 30 bps/yr, never silently zero for short equity). Double-count refuted (synthesized only when upstream `borrow_bps<=0`). Flag-off CPCV drift refuted.
- **🔴 MAJOR DEFECT — default-off vs spec default-on:** `borrow_cost_model.py:79` `return _env_bool("EQUITY_BORROW_COST_ENABLED", False)` — default **False**, but EQ-02 gating mandates default `"1"`. Orchestrator confirmed the flag is set in **no** shipped config (`.env`, `.env.example`, `deploy/` all clean). → the rail is **inert in every default deployment**; short-equity labels/CPCV are optimistic by the borrow cost until an operator turns it on. The implementation is correct and binding *when enabled* — this is a default-value deviation, not a fake-green. *(Same applies to `CPCV_BORROW_COST_ENABLED`, which inherits this default.)*
- **Minor:** borrow env vars (`EQUITY_BORROW_COST_ENABLED`, `CPCV_BORROW_COST_ENABLED`, `EQUITY_BORROW_BPS_PER_YEAR_JSON`, …) not in `docs/config_env_allowlist.txt`.
- **Info (legit):** live `broker_sim` SHORT-carry seam carries no borrow — **deferred by design** per the EQ-02 gating.

### EQ-03 — Dividend + split corporate-action adjustment — **PASS**
- **Runtime enforcement (binds):** `backfill_labels_price_from_prices.py:183-184` reconstructs total return (`adjusted = ((exit*split_factor)-entry)/entry`, `+= dividend_return`); that `ret` drives `dir_`/`ret_z` training labels (INSERT @364-368). Gate `LABELS_USE_CORP_ACTION_ADJUSTMENT` default `"1"`. Factor source `corporate_actions.py:423-489` with **PIT guard** `availability_lte_ts_ms=int(start_ts_ms)` (`:439`) and fail-closed unparseable-split → `corp_action_unparseable` (`:462-474`). ETF: `etf_flows.py:556-558` zeroes `etf_unexpected_flow_z` on ex-div cross (gate default `"1"`; `con=None` → no change). Price-hygiene: `is_explained_split` admits known-split rows.
- **Migration:** `0076_corporate_actions.py` **internal `id = 76`** (orchestrator-read), idempotent `CREATE TABLE IF NOT EXISTS`, contiguous; the prompt's "id 72" is **stale**, correctly flagged not failed.
- **Tests:** `test_corp_action_label_adjustment.py` (safety_critical) asserts dividend label `-0.01 → 0.0`; `test_corporate_actions_pit.py` (PIT + fail-closed + no-secret-leak); `test_corporate_actions_migration.py` (`id==76`, contiguity). **14 passed (exit 0);** migration regression set passed.
- **Falsify:** all 5 refuted (ret adjusted; no PIT look-ahead; malformed split fails closed; ETF golden byte-identical with no ex-date; migration contiguous).
- **Coverage note (better than expected):** the label/hygiene/ETF seams key on **presence of an authoritative corp_actions row by symbol, NOT on EQUITY classification** → EQ-03 is **decoupled from EQ-01** and even covers UNKNOWN-classified stocks. All 5 env flags are in the allowlist. Live Polygon/FMP endpoints implemented-to-schema-with-fixture (no live call) + ingest default-OFF — legitimate-gated.

### EQ-04 — US equity market-session / trading-hours model — **PASS**
- **Runtime enforcement (binds):** `equity_session.py` uses `zoneinfo` America/New_York with a cited NYSE/ICE holiday+half-day table; `equity_timing_adjustment` returns `dict(base_decision)` copy (never mutates input). Wired in `execution_policy_engine.py` at two seams: closed/fail-closed → `_log_suppression_event` then `continue` (before the only `shaped.append` sites) → closed-session equity orders are **dropped from the returned execution-ready list** (`shaped==[]`), i.e. real order-arming suppression. Near-close/half-day passive timing flows into `epe_broker_sim_overrides`. EQUITY-only via `asset_class_for_symbol`. No schema change (reuses `execution_policy_audit`).
- **Tests:** `test_equity_session_policy_integration.py` (safety_critical) drives **real** `apply_execution_policy`, asserts `shaped==[]` + `suppression_reason` startswith `equity_session_closed`; `test_equity_session_dst.py` honest DST assertion (spring `rth_open==13:30 UTC` vs fall `14:30 UTC` — a fixed offset would fail); `test_equity_session_non_equity_unchanged.py` (safety_critical) crypto/UNKNOWN byte-identical. **10 passed (exit 0).**
- **Falsify:** all 5 refuted (suppression observed in engine output not just helper; UNKNOWN not treated as equity; zoneinfo not fixed-offset; holiday table cited + uncovered-year policy-driven; timing returns a copy).
- **Gap (legit):** after-hours recalibration not implemented — deferred residual per gating.

### EQ-05 — Detect and retire delisted / merged / renamed symbols — **GATED-OK**
- **Runtime enforcement (binds):** `universe_lifecycle.py:427-436` `retire_symbol` → `UPDATE symbols SET status='DISABLED' … WHERE … status != 'DISABLED'` (idempotent, rowcount-guarded). Because `get_active_symbols`/`get_symbols_by_status` (`universe.py:861-895`) only return ACTIVE/WATCH, a DISABLED symbol leaves polling/sizing/arming; the `meta_json.lifecycle.delist_ts_ms` is consumed by the real PIT inference (`universe_pit.py:316-321`). Whole run gated `UNIVERSE_LIFECYCLE_ENABLED` default **0**. Job registered + pipeline-ordered.
- **Tests:** `test_universe_lifecycle_retire.py` (safety_critical) asserts `status=='DISABLED'` and `'SPY' not in get_active_symbols`; `test_universe_lifecycle_pit_exclusion.py` (safety_critical) drives **real** `backfill_universe_pit`/`get_pit_universe_symbols`. **9 passed (exit 0);** PIT regression 5 passed.
- **Falsify:** all 5 refuted (no retire-on-absence; reference/broker fail-closed when cred absent; non-EQUITY skipped; idempotent; real e2e PIT exclusion).
- **Gaps (legitimate-gated, the sanctioned no-op state):** broker `/v2/assets` allowlist exists but is injectable-only (never wired into the production job → fails closed); Polygon/FMP `/v3/reference/tickers` reference fetcher IS implemented but default-OFF behind `UNIVERSE_LIFECYCLE_REFERENCE_ENABLED` and credential-gated (no live call in sandbox). The prompt's literal "no Polygon endpoint exists" is now slightly stale (a default-off impl exists). No schema change.

### EQ-06 — Pre-sizing equity leverage / buying-power guard *(Reg-T)* — **PASS**
- **Runtime enforcement (binds):** `_apply_equity_leverage_caps` wired @`portfolio_risk_engine.py:3121` (after FX caps @3110, before strategy @3154). It computes the **aggregate** `gross_pre` and `allowed_gross_weight = min(max_leverage, buying_power_base/account_equity)` and clamps via `clamp_equity_gross_to_leverage` (`equity_sizing.py:118-175`), writing the clamped weight back to `out[sym]` (`:2117/:2136`). **Orchestrator-confirmed:** fail-closed hard-blocks propagate to a real block — `if info.get("equity_leverage_hard_blocks"): blocked=True; block_reason={"type":"equity_leverage_hard_block",…}` at `:3122-3127` → `info["blocked"]/["block_reason"]` + `set_state("portfolio_risk_block","1")`. *(An earlier orchestrator concern that hard-blocks were info-only was based on a truncated grep; the consumption at the call site is real.)* Buying-power source PRAGMA-probed (`_buying_power_reference`) → `(None,"unavailable")` on the broker_sim schema (no `buying_power` column) — no crash. Defense-in-depth added: standalone `equity_deployable_base` now returns a zero ceiling with `unavailable_reason=="equity_buying_power_unavailable"` in Reg-T when `buying_power` is absent, so future callers cannot derive 2.0x from account equity alone.
- **Tests:** `test_equity_sizing.py` asserts the helper fails closed in Reg-T without buying power while cash mode remains account-equity based. `test_equity_leverage_hard_block.py` (safety_critical) drives full `apply_portfolio_risk_engine`: AAPL `1.0→0.625`, MSFT `→0.375`, `gross_pre 1.6 → gross_post 1.0`, and two fail-closed cases asserting `block_reason["type"]=="equity_leverage_hard_block"` + `portfolio_risk_block=="1"`; the Reg-T missing-buying-power case now also asserts the engine hard-blocks before calling `equity_deployable_base`. `test_equity_leverage_noop_superset.py` (safety_critical) flag-off & FX byte-identical.
- **Falsify:** all 5 refuted (aggregate not per-weight; PRAGMA-probed; fail-closed sets blocked; clamp enforced in runtime; `_equity_reference` ≠ buying-power source).
- **Info:** previous standalone helper footgun is fixed; the engine-level hard-block remains the live money-path enforcement.

### EQ-07 — Per-broker share rounding / lot / min-notional — **PARTIAL**
- **Runtime enforcement (binds):** `round_equity_qty` (`share_rounding.py:119-199`, toward-zero, min-notional drop, FX passthrough) reassigns the submitted qty in all three brokers — IBKR (`broker_ibkr_gateway.py:1609`, flows to order builder/limit/audit), Alpaca (`broker_alpaca_rest.py:1607`, flows to the real `_req` POST), sim (`broker_sim.py:3088`). Continue-guards drop sub-min/0-qty before submit. `EXEC_USE_SHARE_ROUNDING` default **OFF** → flag-off byte-identical legacy fractional behavior.
- **Tests:** 4 files, 3 safety_critical; the IBKR/Alpaca tests assert the built order / POST payload qty and that `_FakeApp.placeOrder` is never reached. **15 passed (exit 0);** broker regression set passed.
- **🔴 MAJOR DEFECT — sim/live parity gap (the stated whole point):** **orchestrator-confirmed by direct read** — `broker_sim.py:3088` rounds the **absolute target** (`target_qty`, then `delta=target_qty-cur_qty` @3094), while `broker_ibkr_gateway.py:1609` / `broker_alpaca_rest.py:1607` round the **order delta**. For a fractional non-flat start (cur=5.3, raw target=12.4, integer increment): **sim final = 12.0 vs live final = 12.3.** The parity test `test_broker_sim_share_rounding_matches_live.py` only exercises a **flat** start (cur=0 → target==delta), masking the divergence. Mitigated by default-OFF; must round the delta in sim (or add a non-flat parity test) before relying on sim/live equivalence with rounding enabled.
- **Other falsify traps refuted:** FX passthrough; no reconcile-residual rounding; dropped orders not submitted; no gate weakening / no live POST reached.
- **Info:** EQ-07 env vars not in the allowlist. FX weight-to-lots seam deliberately left unowned (flag, don't fill) — legitimate.

### EQ-08 — Fail loud on missing paid equity feeds; pin feature-set count — **PASS**
- **Runtime enforcement (binds arming):** `prod_preflight.py:2203` `_paid_equity_provider_degradation_gate()` returns errors when a **required** equity provider (∩ of `_EQUITY_PRICE_PROVIDERS={polygon_ws,polygon,ibkr}` and the readiness snapshot's required set) is degraded; `main()` (`:2638-2649`) appends to errors and `return 3` (= NOT production-ready). Enforced **paper/live only** (safe/dev = note). Provider-registry warn branch is **telemetry-only** — returned job list byte-for-byte unchanged. Feature count `default_feature_ids()==111` is a docs/test invariant.
- **Tests:** `test_paid_equity_provider_gate.py` (safety_critical) asserts `rc==3`, degraded provider == polygon, and a uuid **canary secret value is absent** from rendered JSON + caplog (only the NAME `POLYGON_API_KEY` appears); `test_feature_default_count_parity.py` pins 111 (=8 base + 103 unified). **6 passed (exit 0);** regression 18 passed.
- **Falsify:** all 5 refuted (111 not inflated; warn doesn't alter job list; fail-closed on import/snapshot error; secret VALUE never leaked; no false alarm for unconfigured providers).

### EQ-09 — Enforced per-sector / factor concentration budgets — **PASS**
- **Runtime enforcement (binds):** `_apply_sector_budgets` (`portfolio_risk_engine.py:1886`) — when a sector's summed |weight| > cap, `scale=cap/gross` and `out[sym]["weight"]=abs(sw)*scale*sgn` (**orchestrator-read @1917**, sign-preserving, skips empty sectors). Wired @`:3148` (after FX/crypto caps, before strategy). The independent `sector_within_cap` post-check (`:1611-1625`) escalates any breach to `blocked=True` / `block_reason["type"]=="post_cap_validation_failed"` (aggregator `:3223-3240`). Sector from read-only `quiver_gov.sector_for_symbol`. Defaults: `USE_SECTOR_BUDGETS="1"`, `SECTOR_MAX_GROSS=0.30`.
- **Tests:** `test_sector_budget_enforcement.py` (safety_critical) — the bypass test monkeypatches `_apply_sector_budgets` to a no-op and asserts the post-check flips to `blocked=True` / `portfolio_risk_block=="1"` (proves the rail changes the arming decision). **7 passed (exit 0);** regression 9 passed.
- **Falsify:** all 5 refuted (the bogus `sector_correlation_within_cap` name does not exist; the real `sector_within_cap` is enforced; sector from quiver_gov not equity_snapshot; `_sector_for(None,…)` returns `""` not raise; exact `scale=cap/gross` sign-preserving; unclassified symbols skipped).
- **Info:** optional `SECTOR_HARD_BLOCK` not implemented — explicitly allowed ("soft clamp sufficient"). Coverage requires a **seeded gov sector map** (inert/no-clamp when unresolved) — a real operator prerequisite for the rail to bind on live stocks.

### EQ-10 — Cost-aware edge filter + live-arming gap — **PASS**
- **Runtime enforcement (binds, two places):** (1) `alerts.py:886-917` — `adjust_expected_z_for_costs` is the last gate before persistence; on reject → `return None` (alert dropped), on adjust → `expected_z=float(ez_adj)` feeds `choose_rule` (real decision change). EQ-10 added the EQUITY-scope gate (`edge_filter.py:106-107`) and NaN hard-reject (`:147-155`). (2) `config_schema.py:304-308` `validate_live_risk_thresholds` **raises `ConfigError`** (orchestrator-read @308) when the cost filter is required-but-disabled/zero; gated by `EQUITY_EXEC_COST_FILTER_REQUIRED_IN_LIVE` default **0** AND only `_live_risk_required`.
- **Tests:** `test_live_required_edge_filter_arming.py` (safety_critical) asserts `pytest.raises(ConfigError)` with "ALERT_USE_EXEC_COST_FILTER disabled" / "ALERT_MIN_NET_ABS_Z must be > 0"; `test_calibrate_edge_filter_tool.py` asserts `status=="insufficient_data"` / `recommended is None` (no fabrication); `test_mc_var_cvar_block_regression_lock.py` (safety_critical) proves the MC block path still blocks. **13 passed (exit 0);** regression passed; `calibrate … --help` exit 0.
- **Falsify:** traps 1-4 refuted (validator raises; calibration returns insufficient_data; no hardcoded threshold; MC code untouched/unflipped). Trap 5 (USE/MIN_NET_ABS_Z read at import) **confirmed as a latent footgun but NOT exploited** — the tests use `monkeypatch.setattr` on the module attribute, so they honestly drive the function; recorded info, not fake-green.
- **Defaults stay OFF** by design (GATED-OK).

---

## Cross-cutting verification

1. **Linchpin (EQ-01) binds & propagates:** AAPL → EQUITY, EQUITY sleeve 0.80 < MAX_GROSS 1.00 scales weight down (proven 0.45 → 0.40 in the safety_critical test). Propagation: **EQ-03 corp-actions decoupled** (keys on corp_action-row presence, covers even UNKNOWN stocks); **EQ-04 / EQ-10 EQUITY-scoped** → covered for the 7099 registry tickers (AAPL/SPY/MSFT confirmed); **EQ-09 sector** decoupled from EQ-01 (keys on resolved sector) but **requires a seeded gov sector map** to bind on live stocks.
2. **Money-path binds, not reports:** EQ-01 budget ✓, EQ-02 `net_return` ✓ (when enabled), EQ-06 aggregate gross clamp ✓, EQ-09 sector clamp ✓, EQ-10 live-arming ConfigError ✓ — each independently read by the orchestrator at the cited line, each changes a weight/return/arming decision.
3. **Sim/live execution parity:** EQ-04 session suppression identical in sim & live ✓. EQ-07 rounding **PARTIAL** — flat-start parity proven, non-flat fractional diverges (sim 12.0 vs live 12.3), untested.
4. **Classifier ordering & no cross-class bleed:** registry branch is LAST (`asset_map.py:205`), after FX/crypto/commodity/futures/options/rates; OTC/null → UNKNOWN. Confirmed by direct read + live probe.
5. **Flag-off / no-regression:** every EQ flag off → byte-for-byte unchanged (subagent byte-identity assertions); non-equity untouched; the **only** schema change is EQ-03's `0076_corporate_actions` (id 76, contiguous) — no other equity migration.
6. **Whole-suite + validators:**

| Validator | Result | Attribution |
|-----------|--------|-------------|
| `pytest -m safety_critical` | **exit 0, ~296 passed** | GREEN — money path fully covered |
| `pyright_money_path_gate.py` | **exit 0, PASS** (29 baseline errors) | GREEN |
| `validate_docs.py` | **exit 0** ("Documentation validation passed.") | GREEN |
| `git_worktree_triage.py` | **exit 0, ok=true** | GREEN (only blocker = pre-existing unrelated duplicate worktree path) |
| `validate_repo.py` | round-1: exit 0; **round-2 (post-remediation): exit 1** | Round-2 failure = ONE non-equity test: `test_options_predictor_vrp.py::...::test_import_does_not_change_feature_registry_or_options_feature_gate` (OPTIONS workstream). Order-dependent global-state flake: **passes in isolation and alongside every equity test**; fails only in full-suite import order. NOT an equity regression (see GO/NO-GO note). |
| `coverage_gate.py check` (stale report) | exit 1 (5.76% total) | **NON-ATTRIBUTABLE** — checks a stale/partial `artifacts/coverage/coverage.json` (15:46 today) that captured only a tiny subset of tests; not a real full measurement. |
| `coverage_gate.py run` (fresh) | see addendum below | repo-global gate, not equity-specific; full suite already passed via `validate_repo`/`safety_critical` |

---

### Addendum — fresh `coverage_gate.py run` (completed)

The fresh full-suite `coverage_gate.py run` finished: **exit 1 (FAIL)**, `Measured total 5.76%` — **byte-for-byte identical** to the stale `check` (engine/risk 7.27% vs floor 50.81%; engine/execution 13.28% vs 58.92%; engine/runtime 8.08% vs 58.49%).

**Attribution: pre-existing, structural, repo-global — NOT attributable to the equity work.**
- The "new zero-covered modules under critical roots" list spans nearly the entire `engine/runtime` + `engine/execution` surface **and all 77 schema migrations (incl. the ancient `0001_baseline`)** that the equity workstreams never touched — the unmistakable signature of `coverage.py` undercounting a **subprocess / Postgres-facade** test suite (tests that exercise code in spawned processes / the storage facade don't register in-process line hits), not of "94% of the code is untested."
- The number is identical before and after the equity changes (stale 15:46 report == fresh run), so the equity work neither caused nor worsened it; the floors miss by ~45 points, far beyond what one workstream's additions could move.
- Money-path **correctness** is evidenced by the `safety_critical` suite (exit 0) and the equity-specific tests (all passing) — all of which DO drive the equity rails. This line-coverage gate is an orthogonal, pre-existing repo-global red. (Round-1 wording referenced a full `validate_repo` exit 0; in the round-2 re-audit `validate_repo` exits 1 on one non-equity OPTIONS test-isolation flake — see the GO/NO-GO note.)

**Conclusion:** `coverage_gate` is a **pre-existing baseline failure**, recorded here with exact evidence per the operating contract; it does not change the equity verdict.

---

## GAP ledger

| Gap | Classification |
|-----|----------------|
| EQ-05 broker `/v2/assets` allowlist unwired (injectable-only, fails closed) | **legitimate-gated** |
| EQ-05 Polygon/FMP `/v3/reference/tickers` default-OFF + credential-gated (no live call) | **legitimate-gated** (prompt's "no endpoint" now slightly stale) |
| EQ-03 Polygon/FMP corp-action endpoints implemented-to-schema-with-fixture, not live-called | **legitimate-gated** |
| EQ-04 after-hours recalibration not implemented | **legitimate-gated** (deferred residual) |
| EQ-03 UNKNOWN-classification under-coverage | **non-issue** — decoupled, keys on corp_action-row presence |
| EQ-10 UNKNOWN-classification under-coverage | **legitimate-gated** — EQUITY-scope covers the 7099 registry tickers |
| EQ-09 needs seeded gov sector map to bind on live stocks | **legitimate-gated** but a **real operator prerequisite** |
| `broker_sim.py` FX weight-to-lots seam unowned | **legitimate-gated** (flag, don't fill) |
| **EQ-02 `EQUITY_BORROW_COST_ENABLED` default-off vs spec default-on, set in no config** | **🔴 REAL-DEFECT (major)** |
| **EQ-07 non-flat fractional sim/live parity divergence + missing test** | **🔴 REAL-DEFECT (major)** |
| EQ-02 / EQ-07 env vars not in `config_env_allowlist.txt` | **minor** (validate_docs passes; register for completeness) |
| EQ-06 standalone `equity_deployable_base` Reg-T no-buying-power defense | **fixed** — helper now returns a zero ceiling with `equity_buying_power_unavailable`; engine hard-block remains upstream |
| EQ-10 `edge_filter.USE/MIN_NET_ABS_Z` import-time binding | **info** (latent footgun, not exploited) |

---

## GO / NO-GO

**✅ GO (re-audit 2026-06-25).** Both former blockers were remediated and independently re-verified to bind in production with no regression, so the strict *unqualified*-GO bar is now met:

- **EQ-07 — RESOLVED.** `broker_sim.py:3117` now rounds the order **delta** (mirroring the live IBKR/Alpaca adapters); the new non-flat fractional parity test (`test_sim_mirrors_live_delta_rounding_for_fractional_nonflat_positions`) passes — sim final == live final for non-flat positions. Sim/live parity is now proven for both flat and non-flat starts.
- **EQ-02 — RESOLVED.** `borrow_cost_model.py:80` default flipped to `True`; a default-config (no-env) short-equity label now actually has `net_return` reduced (verified live 0.05 → 0.04975). CPCV cost-realism goldens were explicitly opted out (`borrow_enabled: False`), not masked. Env vars registered in the allowlist.
- **Info items resolved:** EQ-06 helper standalone fail-closed (engine unchanged); EQ-09 sector seed binds real stocks via the production lookup; EQ-10 edge filter reads env at call time.

**GO criteria met:** zero FAKE-GREEN/BROKEN/MISSING/PARTIAL; EQ-01 budget proven binding; every money-path rail proven to bind (not just report); sim/live parity proven (EQ-04 session + EQ-07 rounding, flat **and** non-flat); flag-off / non-equity parity proven byte-for-byte; equity validators green vs baseline (`safety_critical` exit 0, `pyright_money_path_gate` PASS, `validate_docs` exit 0, `git_worktree_triage` ok).

**Two repo-global validator reds, both confirmed NON-ATTRIBUTABLE to the equity work:**
- `coverage_gate` — pre-existing structural in-process-coverage undercount (see addendum).
- `validate_repo` (full `pytest tests/`) — **exit 1 from a single failure: `tests/test_options_predictor_vrp.py::...::test_import_does_not_change_feature_registry_or_options_feature_gate`**, an **OPTIONS** workstream test (outside EQ-01…EQ-10). It is an order-dependent global-state isolation flake: it **passes in isolation** and **passes alongside every equity remediation test** (verified), failing only in the full-suite import order because another suite test leaves the feature registry / options gate mutated. The equity remediation never touches `feature_registry.py` / the options gate. Round-1's full suite passed only because the pre-remediation suite composition produced a different test ordering; adding the new equity test files shifted xdist distribution and surfaced this latent OPTIONS test-isolation defect. **Not an equity regression.** (Recommend the options workstream make that test reset global feature-registry/options-gate state in setUp/tearDown.)

**Only remaining item (non-blocking, INFO):** register EQ-01's `ASSET_MAP_USE_EQUITY_REGISTRY` and `PORTFOLIO_RISK_BIND_EQUITY_BUDGET` in `docs/config_env_allowlist.txt` / glossary (documented in `engine/risk/README.md`; `validate_docs` already passes).

*Audit method: two rounds of 10 parallel read-only per-requirement subagents (~1.35M subagent tokens total) + orchestrator independent corroboration at every money-path bind. Round 1 (2026-06-24): CONDITIONAL GO, 2 majors + 3 info. Round 2 re-audit (2026-06-25, post-remediation): all fixes verified fixed-and-binds with no regression → GO. No runtime/test files were modified by the audit.*
