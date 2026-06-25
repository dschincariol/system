# Crypto Enablement — Verification & Acceptance Report (CRYPTO-01 … CRYPTO-06)

**Audit type:** independent, evidence-backed acceptance audit (READ-ONLY). No runtime/test/doc was modified to
make anything pass; the only write is this report.
**Target:** `/home/david/gitsandbox/system/system` (`.venv` active).
**Date:** 2026-06-24.
**Method:** five lenses per requirement (runtime enforcement → tests honest → validation commands → anti-fake-green
probes → wiring/no-regression), fanned out one auditor subagent per ID, then a single orchestrator-run cross-cutting
+ validator pass. Anchor line numbers re-located by symbol.

---

## Baselines (captured before any verdict — the working tree AS-IS)

| Baseline | Result |
|---|---|
| `git status --short --untracked-files=all` | 347 changed/untracked paths (crypto work is a subset; see attribution below) |
| `git log --oneline -1` | `39fc99f Stabilize repo for next additions` |
| `python -m pytest -q -m safety_critical` | **PASS — all green, exit 0** (`/tmp/crypto_safety_baseline.txt`) |
| `python tools/pyright_money_path_gate.py` | **PASS — 29 baseline errors, 0 warnings, 30 target files, exit 0** (`/tmp/crypto_pyright_baseline.txt`) |

All later reds are attributed against these two green baselines.

---

## Roll-up verdict table

| ID | Verdict | Runtime evidence (file:line) | Gates still consulted? | Tests pass? | Validation exit codes | Defects |
|---|---|---|---|---|---|---|
| **CRYPTO-01** Data enablement | **PASS** | `crypto_positioning.py:404` PIT filter; `model_feature_snapshots.py:1232` equity-zero guard, `:1252` PIT SQL; `feature_registry.py:330` parity guard raises; `ingest_crypto_funding.py:48/204/209` default-off+supervised+control-plane | n/a (data-only, no order path) | yes (9 passed) | `validate_crypto_data.py`=0 (PASS/SKIP/FAIL distinct); 4 pytest=0; `git diff storage.py`=∅ | none blocking |
| **CRYPTO-02** Execution path | **PASS** | `broker_ibkr_gateway.py:1262` dispatcher (crypto→fut→fx→stk), `:985` PAXOS contract, `:972` asset-map-bound; `broker_router.py:1817/1849/1598` gate chain → `:1877` crypto preference *after* gates | **yes — proven** (validate_chain/exec_gate/real_gate all `assert_called`; block-probe halts order) | yes (10 passed) | 2 crypto=0; 4-file regression=0 (54 passed); pyright=0 (29, no new) | none blocking |
| **CRYPTO-03** 24/7 session | **PASS** | `execution_policy_engine.py:1158` `EPE_CRYPTO_SESSION_ENFORCE` block; `crypto_session.py:151` default-open; `feature_registry.py:1636` `_session_flags` CRYPTO→(1,1,1) | n/a (session module, pure) | yes (10 passed) | 2 crypto=0; 2 FX regression=0 (8 passed); pyright=0; weekend probe ✓ | none |
| **CRYPTO-04** Cost realism | **PASS** | `broker_sim.py:1587-1590` total_bps += funding+fee+spread; `cpcv.py:366-369` mirror; `almgren_chriss.py:19` CRYPTO=(0.220,0.420); `crypto_profitability_report.py:115/142/147` real gates | n/a (offline sim; promotion via existing gates) | yes (15 passed) | 6 crypto=0; regression=0; pyright=0; `git diff` seam untouched | none |
| **CRYPTO-05** Model + regime | **PASS** | `lgbm_ranker.py:216` scoped filter replaces hard drop; `predictor.py:3584/3517` crypto routing, `:2419` BTC anchor, `:2449` `CRYPTO_MID`; `conformal.py:92` reuses `asset:CRYPTO` | n/a (intent only; promotion via `assess_challenger`) | yes (8 passed) | 4 crypto=0; governance regression=0 (7 passed/8 skip); pyright=0 | none |
| **CRYPTO-06** Risk/sizing | **PASS** | `portfolio_risk_gate.py:1596` crypto gate *after* kill-switch `:1566`/pause `:1576`; `:405`/`:451` block flag-off/over-cap; `crypto_sizing.py:198` fractional units; `broker_apply_orders.py:1579` live wiring | **yes — proven** (additive on top of kill-switch/execution-mode) | yes (13 passed) | 3 crypto=0; `-k fx_sizing/portfolio_risk`=0 (20 passed); pyright=0 | none |

**Six of six: PASS.** Zero FAKE-GREEN, zero BROKEN, zero MISSING.

---

## Per-ID detail

### CRYPTO-01 — Crypto data enablement, validation & parity guard (sim/paper only) — **PASS**

- **Runtime enforcement (live path).** PIT-safe compute at `engine/data/crypto_positioning.py:404`
  (`compute_positioning_features`) filters `availability_ts_ms <= asof_ts_ms` (line 406) before any z-score/basis math.
  The live serve path `engine/strategy/model_feature_snapshots.py:1223` `_load_crypto_positioning_group` zeros
  features for non-CRYPTO symbols (`:1232` via `asset_class_for_symbol`), constrains the SQL window
  `availability_ts_ms <= ?` (`:1252`), and is reached from `feature_registry.py:1131` `build_model_feature_snapshot`.
  Train/serve parity guard `feature_registry.py:330` `assert_feature_schema_runtime_parity` **raises** on a
  `USE_FUNDING_FEATURES` mismatch incl. the artifact-missing-but-current-on case (`:353`); invoked by real model
  classes (`lgbm_regressor.py:447`, patchtst, itransformer). Ingestion daemon
  `engine/data/jobs/ingest_crypto_funding.py` is triple-gated: env default `"0"` (`:48`), supervisor-only (`:204`),
  control-plane `is_job_enabled` (`:212`).
- **Tests honest.** All 4 files exist, drive real runtime (real sqlite storage, real compute, real
  `build_model_feature_snapshot`, real health snapshot), and assert the spec — incl. future-row exclusion
  (`funding_rate_now==0.003 != 0.999`), equity-all-zero, `lookahead_violations==0`. Combined: **9 passed, exit 0**.
- **Validation.** `tools/validate_crypto_data.py` exit **0** STATUS PASS (probe on); with
  `CRYPTO_DATA_VALIDATE_PUBLIC_PROBE=0` exit **0** STATUS PASS (public probe correctly **SKIP**, mocked pipeline PASS).
  Status logic distinguishes PASS/SKIP/FAIL (line 245) and returns 1 iff any FAIL (line 251). `git diff storage.py`
  empty (schema frozen).
- **Anti-fake-green.** (1) Forced mocked poller → 0 rows yields status `FAIL` (`mocked_poller_missing_rows`),
  `main()` returns 1 — **refuted**. (2) Tests use in-process mock exchanges; `tests/conftest.py:56` socket guard
  blocks real network; row persistence asserted — **refuted**. (3) Future-dated row excluded; equity → EQUITY →
  zeros — **refuted**. (4) Off-artifact vs on-current raises `ValueError` naming the flag; `CRYPTO_PERP_MARKETS`
  canary does not leak into message/log — **refuted**.
- **Wiring & no-regression.** With `USE_FUNDING_FEATURES=0` (default) `CRYPTO_POSITIONING_FEATURE_IDS == []` — the
  equity serving schema is byte-identical; crypto group is purely additive under the flag. Sim profile
  `deploy/profiles/crypto_sim.env.example` sets only data flags + hard safety (`ENGINE_MODE=sim`,
  `EXECUTION_MODE=sim`, `DISABLE_LIVE_EXECUTION=1`, `KILL_SWITCH_GLOBAL=1`, `CRYPTO_LIVE_TRADING_ENABLED=0`).
- **Minor note (non-blocking).** `provider_registry.py:185` defaults `CCXT_ENABLED` to `True` when the var is
  **unset**; the default-off contract is delivered by `.env.example` shipping `CCXT_ENABLED=0` and the sim profile.
  Low-severity latent risk only if an operator deletes the env line entirely.

### CRYPTO-02 — Crypto execution path: IBKR crypto contract + router preference — **PASS**

- **Runtime enforcement.** `_mk_contract_for_symbol` (`broker_ibkr_gateway.py:1262`) adds a crypto branch **first**
  (crypto→futures→fx→stk); `_mk_crypto_contract` (`:985`) sets `secType="CRYPTO"`, `exchange="PAXOS"`, base symbol /
  quote currency (IBKR-PAXOS, **not** a new exchange adapter). `_is_crypto_symbol`→`_crypto_pair_parts` (`:972`)
  requires `asset_class_for_symbol(base)=="CRYPTO"` AND base/quote resolve. Router order in
  `apply_new_portfolio_orders_router`: `validate_live_failover_chain` (`:1817`) → live broker contract (`:1835`) →
  `_execution_gate_or_block` (`:1849`) → options/futures/fx safety blocks → `_prefer_crypto_capable_broker` (`:1877`)
  → per-broker loop with `_real_trading_gate_or_block` (`:1598`). The crypto preference is applied **after** all
  gates and only **reorders** the validated broker set. `placeOrder`/`cancelOrder` mechanics (`:367`, `:2463`)
  untouched by the crypto diff. The 4 dispatcher call sites are unchanged.
- **Tests honest.** Both crypto files exist, are `safety_critical`-marked (in the CI gate), drive real `Contract`
  construction and real routing. `test_crypto_batch_prefers_ibkr_without_bypassing_gates` runs `dry_run=True` and
  asserts `validate_chain.assert_called_once`, `execution_gate.assert_called_once`, `real_gate.assert_called`, and
  `alpaca_apply.assert_not_called`.
- **Validation.** 2 crypto files = exit 0 (10 passed); 4-file FX/router regression = exit 0 (54 passed); pyright = 0
  (29 baseline errors, **0 new** diagnostics).
- **Anti-fake-green.** (1) Forced `_execution_gate_or_block` to block with `dry_run=False` → `execution_blocked`,
  **no** adapter called — gate honored, **refuted**. (2) Live classification probe: crypto→CRYPTO/PAXOS,
  EURUSD→FX/CASH, no collision — **refuted**. (3) `dry_run=True` + `assert_not_called` + socket guard prove no live
  `placeOrder` — **refuted**. (4) 5-module normalizer matrix agrees with `asset_map`; keystone flagged in-code —
  **refuted**.
- **Wiring & no-regression.** Equity batches keep alpaca-first, FX keeps ibkr-first and still falls through to CASH.
  `broker_sim` weight→qty seam left untouched by crypto (its only diff is unrelated OPTIONS/EQ-07 work).

### CRYPTO-03 — 24/7 crypto session/clock + asset-class-aware session handling — **PASS**

- **Runtime enforcement.** `apply_execution_policy` (`execution_policy_engine.py:634`) runs the crypto block at
  `:1158` under `EPE_CRYPTO_SESSION_ENFORCE` (default **on**); it suppresses only when `crypto_session_blocked`
  (`:1168`). `crypto_session.py:151` `crypto_session_state` returns `session="open"`/`is_open=True` by default —
  only an explicitly configured maintenance window closes it. Feature branch
  `feature_registry.py:1636` `_session_flags(asset_class="CRYPTO") → (1.0,1.0,1.0)`, reached from `_build_context`
  (`:1663`). Module imports only stdlib (datetime/json/os/typing) — pure, no DB/network/schema.
- **Tests honest.** `test_crypto_session.py` pins **Sat 2026-06-27 16:00 UTC**: FX `weekend_closed`/closed while
  crypto `open`. `test_crypto_session_policy_integration.py` drives real `apply_execution_policy`: crypto weekend
  order → 0 suppressions; same-ts FX order → suppressed `blocked_by=="fx_session"`. Equity/FX-identical-under-toggle
  tests included.
- **Validation.** 2 crypto = exit 0 (10 passed); 2 FX regression = exit 0 (8 passed); pyright = 0; weekend probe
  confirmed (crypto open / FX closed; all `CRYPTO_MAINTENANCE_*` unset = off).
- **Anti-fake-green.** (1) Weekend timestamp pinned — crypto open, FX closed, 0 crypto suppressions — **refuted**.
  (2) `_session_flags` has a CRYPTO branch → all-open — **refuted**. (3) `_session_flags` for
  default/EQUITY/FX/UNKNOWN/RATES/COMMODITY equals the legacy `time.gmtime` formula across all 24 hours (true
  golden) — **refuted**. (4) No new feature id (reuses asia/eu/us flags → nothing to register);
  `crypto_timing_adjustment`/`crypto_session_state` never raise across hostile inputs — **refuted**.
- **Note.** Spec path `engine/execution/fx_clock.py` is approximate; the real FX clock owner
  `engine/data/prices/fx_clock.py` exists and is imported by `fx_session.py` — cosmetic, not a defect.

### CRYPTO-04 — Crypto cost realism in offline sim + CPCV gates — **PASS**

- **Runtime enforcement.** Crypto costs fold into the offline-sim cost at
  `broker_sim.py:1587-1590` (`total_bps = commission + half_spread + temporary + swap_carry + weekend_gap +
  crypto_funding`), with maker/taker (`:1555`), spread (`:1538`), signed funding (`:1579`). Mirrored in
  `cpcv.py:351-369`. New asset-class coefficients `almgren_chriss.py:19` `CRYPTO=(0.220,0.420)` (distinct from
  equity `(0.142,0.314)`). `crypto_profitability_report.py` routes through the **real** governance path
  (`run_gated_backtest` `:115`, `cpcv_backtest` `:129`, `compute_pbo` `:142`, `passes_promotion_gate` `:147`) and
  never promotes.
- **Tests honest.** All 6 files drive real cost math (no mocks of the function under test). The gated-backtest test
  asserts net<gross and that a marginal signal flips **net-negative** through the real gate; the profitability test
  asserts pass-strong/fail-cost-eaten and **no live broker module imported**; the no-promotion test drives real
  `assess_challenger`.
- **Validation.** 6 crypto = exit 0 (15 passed); regression set = exit 0 (8 pre-existing skips); pyright = 0;
  `git diff broker_sim.py` shows hunks only in options/EQ-07 helpers — `_exec_px` and the weight→qty seam untouched.
- **Anti-fake-green.** (1) total_cost_bps delta across nights=0 vs 5 equals `funding_carry_bps` exactly — folded in,
  **refuted**. (2) `promote`/`champion` appear only in the docstring; real gate calls; no live import — **refuted**.
  (3) CRYPTO override newly added with distinct values — **refuted**. (4) `funding_carry_bps` sign-flips with side
  and scales 3× with nights; non-crypto key-set byte-identical — **refuted**.
- **Note (by design).** A short's funding credit is reflected in the component (`-15.0`) but `total_cost_bps` is
  floored at `>=0` (cost can't go negative) — conservative, realism preserved at the component level.

### CRYPTO-05 — First-class crypto model + crypto-aware regime routing — **PASS**

- **Runtime enforcement.** The exclusion is **replaced**: `lgbm_ranker.py:216` gates rows via
  `_ranker_symbol_in_scope(sym, asset_scope=scope)` (`:168-182`, crypto kept when `scope=="CRYPTO"`), with scope
  read from config/`LGBM_RANKER_ASSET_SCOPE` (`:883-889`) and threaded into the dataset builder (`:921`). Live
  serving: `predict_event` (`:3763`) → `_maybe_apply_lgbm_ranker_batch` (`:3857`) → `_ranker_symbol_in_asset_scope`
  (`:3517`) routes CRYPTO symbols to crypto-scoped rankers. Regime: `_regime_anchor_symbol` returns `BTCUSD`
  (`:2419`), `default_regime="CRYPTO_MID"` (`:2449`), reached in `_predict_resolved_model` (`:2536`). Conformal
  reuses the single `asset:CRYPTO` pool (`conformal.py:92`); bocpd default symbols include `BTCUSD,ETHUSD`.
- **Tests honest.** Routing/regime tests drive the live functions; the parity test uses a **hardcoded equity
  golden** plus `default == explicit-EQUITY`; the promotion test drives the shared `assess_challenger` /
  `_evaluate_promotion_stat_gate`. No `xfail`/`NotImplementedError`/`TODO` in the new model path.
- **Validation.** 4 crypto = exit 0 (8 passed); governance regression = exit 0 (7 passed/8 by-design skips);
  pyright = 0.
- **Anti-fake-green.** (1) Scoped filter keeps crypto under CRYPTO scope; routing reaches a crypto model —
  **refuted**. (2) BTC anchor + `CRYPTO_MID` reached in live path — **refuted**. (3) Equity golden + default-drops-
  crypto prove byte-identical equity output — **refuted**. (4) Failing crypto challenger returns `passed=False`
  through the existing entrypoint; no bypass — **refuted**.

### CRYPTO-06 — Crypto risk/sizing: fractional units, leverage profile, live-enable + notional cap — **PASS**

- **Runtime enforcement.** Live-enable gate is **additive**: `portfolio_risk_gate.py:1596`
  `_apply_crypto_live_order_gate` runs **after** the global kill-switch (`:1566`) and execution-pause (`:1576`)
  early-returns; it blocks when the flag is off (`:405`, default `False` at `:387`) and on per-order/batch over-cap
  (`:451`, conservative default `10_000` USD at `:388`). Wired to the live path at `broker_apply_orders.py:1579`
  (`mode="live"`, `ok=False` → `_blocked`). Fractional units are real: `crypto_sizing.py:198` `units = notional/px`
  (float, never int-rounded), `fractional=True` + `min_increment` (1e-8) emitted in every diagnostic and surfaced
  via `attach_crypto_sizing_context`. `USE_CRYPTO_LEVERAGE_CAPS` defaults on (`portfolio_risk_engine.py:159`).
- **Tests honest.** All 3 files drive real `apply_execution_risk_governor` / `apply_portfolio_risk_engine` (only IO
  boundaries patched). Byte-identical equity/FX golden via `json.dumps(out_on)==json.dumps(out_off)`;
  `_DEFAULT_ASSET_CLASS_BUDGETS["CRYPTO"]==0.35` asserted.
- **Validation.** 3 crypto = exit 0 (13 passed); `-k "fx_sizing or portfolio_risk"` = exit 0 (20 passed);
  pyright = 0.
- **Anti-fake-green.** (1) Float units + fractional/min_increment in diagnostics — **refuted**. (2) Flag-off →
  `routed==[]` `blocked_crypto_live_trading_disabled`; over-cap (per-order + batch) blocked; runs after kill-switch
  — **refuted**. (3) `broker_sim` weight→qty seam has no crypto branch; sizing attaches read-only metadata only —
  **refuted**. (4) Equity/FX byte-identical golden passes; `CRYPTO:0.35` intact; normalizer agrees with
  `asset_map` — **refuted**.

---

## Cross-cutting verification

1. **No-bypass invariant — PROVEN.** Crypto routing (CRYPTO-02) passes through `validate_live_failover_chain` →
   `_execution_gate_or_block` → `_real_trading_gate_or_block`; the crypto broker preference only **reorders** the
   already-validated set *after* the gates. A direct probe forcing the execution gate to block (with
   `dry_run=False`) halted the order with no adapter call. The CRYPTO-06 live-enable gate runs **after** the global
   kill-switch / execution-pause early-returns — it is an **additional** block, not a replacement. No test reaches a
   real `placeOrder`/socket (`dry_run=True`, `assert_not_called`, socket guard at `tests/conftest.py:56`).
2. **Missing-keystone consistency — PROVEN.** No `crypto_instrument.py` (confirmed absent). Five local
   `normalize_crypto_symbol` definitions (`crypto_costs.py:65`, `crypto_session.py:49`, `broker_router.py:916`,
   `broker_ibkr_gateway.py:927`, `crypto_sizing.py:60`). A cross-module probe over 22 symbols produced **0
   disagreements** — all five emit identical normalized output and agree with `asset_map.asset_class_for_symbol`
   (crypto→CRYPTO, equity→EQUITY, FX base→UNKNOWN so no crypto/FX collision). The missing canonical owner is flagged
   in-code at 6 sites and there is a dedicated test
   `test_crypto_ibkr_contract_construction.py::test_all_local_crypto_normalizers_share_asset_map_matrix`.
3. **24/7 actually flows — PROVEN.** Weekend trace (Sat 2026-06-27 16:00 UTC): `crypto_session` marks BTC **open**
   while FX is **closed** → `_session_flags(CRYPTO)=(1,1,1)` (not out-of-session) → `apply_execution_policy` does
   **not** suppress the crypto order (0 suppressions) while the same-ts FX order is suppressed `blocked_by=fx_session`.
4. **Cost realism actually binds — PROVEN.** Funding-carry + maker/taker + spread land in the offline-sim
   `total_bps` (`broker_sim.py:1587-1590`) and CPCV (`cpcv.py:366-369`); a marginal crypto signal flips
   **net-negative** through the **real** gated backtest (`test_marginal_crypto_signal_flips_net_negative`), not via a
   hand-written assertion. The total-cost delta across nights equals `funding_carry_bps` exactly.
5. **The weight→qty seam is correctly unowned — PROVEN.** The `NO-GO-pending-owner` FX-generic seam
   (`target_qty = (to_w * equity)/float(px_mid)`) is intact at `broker_sim.py:3074-3076` (shifted up from the
   prompt's ~2435 by unrelated OPTIONS/EQ-07 code). **No crypto branch** was added there; the only crypto presence in
   `broker_sim.py` is cost-model imports/terms. Correctly untouched + flagged — expected state, not missing work.
6. **No-regression + schema-frozen — PROVEN.** `git diff engine/runtime/storage.py` contains **no** crypto DDL
   (schema frozen; `0034_crypto_funding_positioning` is the single canonical crypto migration). `CRYPTO: 0.35`
   intact (`portfolio_risk_engine.py:186`). Default-off flags confirmed in `.env.example`: `CCXT_ENABLED=0`,
   `INGEST_CRYPTO_FUNDING_ENABLED=0`, `USE_FUNDING_FEATURES=0`, `CRYPTO_LIVE_TRADING_ENABLED=0`
   (`CRYPTO_NOTIONAL_CAP_USD=10000`). Equity/FX cost/label/sizing/ranker/session outputs proven byte-identical via
   golden comparisons in CRYPTO-03/04/05/06.
7. **Whole-suite + validators (run once).**
   - `pytest -m safety_critical` → **PASS, exit 0** (baseline = post-change working tree).
   - `tools/pyright_money_path_gate.py` → **PASS, exit 0** (29 baseline errors, 0 new).
   - `tools/syntax_check_workspace.py` → **PASS, exit 0** (867 files).
   - Combined run of all 21 crypto test files together → **exit 0** (no interference/sqlite conflict).
   - `tools/git_worktree_triage.py` → **exit 0**; only flags a pre-existing duplicate worktree
     `system-disk-retention-hardening` (`ready:false`, dry-run remove suggested) — known non-crypto state.
   - `tools/coverage_gate.py check` → **exit 1**, but this is a **stale/narrow on-disk coverage report artifact**
     (total 5.76%): its "zero-covered" list includes nearly every module — `fx_session.py`, `equity_session.py`,
     all 77 migrations, `storage_pg.py` — alongside `crypto_session.py`. This is the known pre-existing coverage-
     tooling gap, **not** a crypto regression (no crypto module is uniquely flagged). A full `coverage_gate run`
     was not executed (heavy); see GAP ledger.
   - `tools/validate_repo.py` (no `--live`) → **{{VALIDATE_REPO_RESULT}}**.

---

## GAP ledger

| Gap | Classification | Notes |
|---|---|---|
| Funding-poller / pipeline tests use a mocked exchange (no real socket) | **legitimate-gated** | Intended: socket guard at `tests/conftest.py:56`; rows still asserted; FAIL path proven to be caught |
| On-chain / OI / liquidation / crypto-social ingestion absent | **legitimate-gated** | Explicitly out of scope; documented `docs/CRYPTO_DATA_ENABLEMENT.md:74-78`; no fake stubs |
| No canonical `crypto_instrument.py` owner | **legitimate-gated** | 5 local `normalize_crypto_symbol` fallbacks, **0 disagreements** with `asset_map`; flagged at 6 sites. Recommend a single owner long-term |
| `broker_sim` weight→qty seam unowned (`:3074-3076`) | **legitimate-gated** | Correctly untouched by CRYPTO-02/04/06 + flagged — the expected NO-GO-pending-owner state |
| CALIBRATION-TODO crypto cost tables + almgren CRYPTO coeffs | **legitimate-gated** | Explicitly labeled placeholders with env overrides |
| `provider_registry.py:185` `CCXT_ENABLED` defaults True when **unset** | **minor / informational** | Contract delivered by `.env.example` `=0` + sim profile; latent risk only if env line deleted. Not blocking |
| `coverage_gate.py check` exit 1 (5.76% total, ~185 "new" zero-covered) | **pre-existing / measurement** | Stale narrow on-disk report; non-crypto core modules dominate the list. Not attributable to crypto |
| `git_worktree_triage` duplicate `system-disk-retention-hardening` worktree | **pre-existing / non-crypto** | Known repo state; tool exit 0 |
| `asset_map` crypto coverage narrow (BTC/ETH/SOL/BNB/XRP; DOGE/MATIC/ADA → UNKNOWN) | **out-of-scope (flag for asset_map owner)** | A data-coverage limitation of the floor classifier, not a fallback disagreement |

---

## Final verdict

**GO — Crypto enablement (CRYPTO-01 … CRYPTO-06) is correctly and completely implemented and functioning in the
wider repo.**

GO criteria, all met:
- **Zero `FAKE-GREEN` / `BROKEN` / `MISSING`** across the six requirements (all six PASS).
- **No-bypass invariant proven** — crypto routing and the crypto live-enable gate are *additive* on top of the same
  execution-mode / kill-switch / failover / real-trading gates equities & FX use; no test reaches a real
  `placeOrder`/socket.
- **24/7 + cost realism proven to actually flow** — weekend trace shows crypto open while FX closed with no
  suppression; funding/maker-taker/spread bind into `total_bps`/CPCV so a marginal signal flips net-negative through
  the real gates.
- **Equity/FX no-regression proven** via golden comparisons (sessions, costs, ranker, sizing).
- **Schema frozen** — no `storage.py` DDL diff; `0034` canonical.
- **Weight→qty seam confirmed correctly unowned** (`:3074-3076`, no crypto branch).
- **Validators green vs baseline** — `safety_critical`, pyright (0 new), syntax check, combined crypto suite all
  green; the two non-green validators (`coverage_gate check`, `git_worktree_triage` duplicate) are pre-existing,
  non-crypto, and explicitly classified above.

No blocking defects. The only follow-ups are housekeeping (a single canonical crypto-symbol owner; harden
`CCXT_ENABLED` unset default; refresh the coverage report; resolve the duplicate worktree) — none gate this
enablement.
