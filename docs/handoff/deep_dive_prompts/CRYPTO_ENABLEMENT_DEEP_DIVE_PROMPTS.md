# Crypto Enablement — Deep Dive Implementation Prompts

These six prompts each implement **one** of the crypto-readiness recommendations from the 2026-06-23
crypto support review. Each is self-contained and ends with a mandatory self-audit/NO-GO clause.

**Verified state (re-grepped 2026-06-23, `/home/david/gitsandbox/system/system`).** The FX enablement
series is **fully merged** — `engine/data/fx_instrument.py`, `engine/data/prices/fx_clock.py`,
`engine/execution/fx_session.py`, `engine/execution/fx_costs.py`, `engine/strategy/fx_sizing.py`, and
`engine/strategy/fx_profitability_report.py` all exist on disk. **This is the single most important fact
for these prompts: every crypto change below has an exact, already-merged FX twin to mirror.** Where a
prompt says "mirror the FX twin," that twin is real code you can read and copy structurally.

**Crypto today (the gap these prompts close).** Crypto data is real but switched off; crypto is excluded
from the primary alpha model; there is no crypto execution contract, no 24/7 session model, no crypto cost
calibration, and no crypto-specific sizing. Full evidence is cited inline per prompt.

**No canonical crypto instrument keystone exists.** Unlike FX (`fx_instrument.py`) and futures
(`futures_instrument.py`), there is **no `engine/data/crypto_instrument.py`**. The canonical stored crypto
symbol form is the bare root ticker used by `asset_map.py` and the `crypto_funding_rates` table (e.g.
`BTC`, `ETH`). Because no keystone owns symbol normalization, **each prompt below defines a small local
`normalize_crypto_symbol` fallback** (exactly as FX-07's `fx_costs.normalize_fx_symbol` does) and flags the
missing canonical owner in its self-audit. If you later author a `crypto_instrument.py` keystone (the FX-02
twin), collapse these fallbacks into it. Do **not** silently invent a second authority that disagrees with
`asset_map.py`.

### Shared global constraints (bind every CRYPTO-0x prompt)
- **Target repo:** `/home/david/gitsandbox/system/system`. All paths are relative to it. One repo only.
- **No live broker mutation, anywhere, including tests.** No live order/cancel/replace/flatten for any
  broker. All crypto order paths must be reachable only via `dry_run=True` and/or the sim/paper/shadow
  broker, behind the **same** execution-mode / kill-switch / pre-live-reconcile gates that protect
  equities and FX today — never a new bypass. Tests use mocks/stubs only; no test may open a real socket
  (the repo installs a socket guard in `tests/conftest.py`).
- **Never** print, log, echo, commit, or test with real secret values. Read every credential from
  env/secret-file indirection only. Use generated canary strings in any test that touches config and
  assert they never appear in outputs/logs.
- **Build on existing architecture; never replace it.** Respect the model-vs-runtime contract (models
  propose intent; runtime owns safety gates) and train/serve feature parity via
  `engine/strategy/feature_registry.py`. Do not give models order authority. Do not bypass the
  champion/challenger promotion governance path.
- **`engine/runtime/storage.py` schema is high blast radius.** No prompt below should require a schema
  change. (CRYPTO-01's data already has its table from migration `0034`.) If you believe one is
  unavoidable, stop and redesign; if genuinely unavoidable, add an additive `ADD COLUMN IF NOT EXISTS`
  migration + migration test and flag **NO-GO-pending-review**.
- Equity **and FX** behavior must be byte-for-byte unchanged when the symbol/asset-class is not crypto.
  Prove non-regression with golden comparisons, not just self-consistency.

### Recommended merge order
1. **CRYPTO-01** (data enablement) — foundational; everything else assumes funding/price data flows.
2. **CRYPTO-03** (24/7 session) and **CRYPTO-04** (cost realism) and **CRYPTO-06** (risk/sizing) — largely
   independent; can land in parallel after CRYPTO-01.
3. **CRYPTO-02** (execution path) — consumes CRYPTO-03's session timing; land after it.
4. **CRYPTO-05** (model + regime routing) — consumes CRYPTO-01's features; land last (most research-y).

---

## CRYPTO-01 — Crypto data enablement, validation & parity guard

**Mission.** The crypto data spine already exists but ships **disabled**. Turn on CCXT spot/OHLCV polling
and perpetual-funding ingestion **in a sim/paper profile only**, prove the pipeline end-to-end (funding
rows land, the six positioning features compute point-in-time-safe), enable `USE_FUNDING_FEATURES` without
breaking train/serve parity, and add a repeatable validation harness + readiness check so the capability
cannot silently regress. **No alpha claim, no live trading.**

**Prerequisites.** None (foundational). CRYPTO-02..06 consume this.

**Global constraints.** Shared constraints above bind. Additionally: enabling data must not enable any
order path; the sim/paper profile must not set any live-trading flag.

**Verified anchors (re-grepped 2026-06-23).**
- `engine/data/provider_registry.py:167-175` — `provider_name="ccxt"`, `enabled=_env_enabled("CCXT_ENABLED", True)` (`:170`), `supports={"asset_classes":["crypto"],"transport":"rest"}` (`:174`), `build_price_provider=_build_ccxt` (`:67`).
- `.env.example` — `CCXT_ENABLED=0`, `INGEST_CRYPTO_FUNDING_ENABLED=0`, `CCXT_EXCHANGE_ID=kraken`, `CCXT_FUNDING_EXCHANGE_ID=binance`, `CRYPTO_FUNDING_POLL_SECONDS=3600`, `CRYPTO_PERP_MARKETS=` (all crypto data OFF by default).
- `services/data_source_manager.py:1401-1426` — `crypto_funding` `SourceDefinition` (`job_name="ingest_crypto_funding"`); `:600` test handler `_test_crypto_funding_connection`; `:1958` in the enabled-job list; `:2102-2103` / `:2360-2361` storage-table contract (`crypto_funding_rates`).
- `engine/runtime/job_registry.py:1076-1081` — `ingest_crypto_funding` daemon, settlement-aligned 00/08/16 UTC, cadence 28800s.
- `engine/data/jobs/ingest_crypto_funding.py` + `engine/data/crypto_positioning.py` — the funding poller and `compute_positioning_features`.
- `engine/runtime/schema/migrations/0034_crypto_funding_positioning.py` — `crypto_funding_rates` table (already in tree; no schema work here).
- `engine/strategy/feature_registry.py:86` — `USE_FUNDING_FEATURES = _env_bool("USE_FUNDING_FEATURES", False)`; `:225` `_ALL_CRYPTO_POSITIONING_FEATURE_IDS`; `:287` gated `CRYPTO_POSITIONING_FEATURE_IDS`; `:561-562` group registration; `:1095-1096`, `:1449-1450` inclusion sites.
- `tests/test_crypto_funding_features.py` — existing pattern for feature/PIT tests to mirror.

**In scope.**
1. **A documented sim/paper crypto data profile** (do not edit the live `.env`). Add a committed example
   env fragment `deploy/profiles/crypto_sim.env.example` (or extend `.env.example` with a clearly-labeled
   crypto block) setting `CCXT_ENABLED=1`, `INGEST_CRYPTO_FUNDING_ENABLED=1`,
   `USE_FUNDING_FEATURES=1`, `CRYPTO_PERP_MARKETS=` (auto-discover from the symbols table),
   `ENGINE_MODE`/execution-mode left at sim — with an inline comment that this profile enables **data
   only**, never live execution. Confirm `provider_registry` and `data_source_manager` honor these flags
   (no code change expected; cite the lines that read them).
2. **A validation harness** `tools/validate_crypto_data.py` (new; read-only; safe to run headless) that:
   (a) imports `ccxt` and probes the configured public funding/spot endpoints **defensively** (skips with
   a clear message, never raises, if offline); (b) runs the funding poller for one cycle against a
   **mocked/stubbed** exchange and asserts rows persist to `crypto_funding_rates`; (c) calls
   `compute_positioning_features` and asserts the six features (`funding_rate_now`, `funding_z_30d`,
   `funding_extreme_flag`, `funding_cum_3d`, `perp_basis_pct`, `basis_z_30d`) materialize and respect
   `availability_ts_ms <= ts_ms` (no lookahead). Print a structured PASS/SKIP/FAIL report; exit 0 on
   PASS/SKIP, nonzero only on a real integrity failure.
3. **A readiness/health surface** — extend the existing crypto-funding health/test path
   (`data_source_manager.py:600` `_test_crypto_funding_connection`, and the source readiness in
   `engine/runtime/health.py` if it already reports per-source) so "crypto data" reports
   wired/enabled/last-row-age instead of being invisible. Reuse the existing health pattern; add no new
   table.
4. **Parity guard for `USE_FUNDING_FEATURES`.** This flag is read **at import** (`feature_registry.py:86`)
   and changes the global feature set. A model trained with the flag off must not be served with it on.
   Add an explicit train/serve parity assertion (or strengthen the existing feature-set fingerprint
   check) so a mismatch fails closed with a clear error, and document the toggle's parity implication.
5. **Docs:** `docs/CRYPTO_DATA_ENABLEMENT.md` — what flows (spot/OHLCV via CCXT, funding/basis), the env
   knobs, the "data-only, no live trading" guarantee, the parity caveat, and how to run
   `validate_crypto_data.py`. Note explicitly what is still **missing** (on-chain, open interest,
   liquidations, crypto social) so the gap is recorded, not hidden.

**Out of scope (do not touch).** Any order/execution path (CRYPTO-02). Any model change (CRYPTO-05). The
`crypto_funding_rates` schema (`0034` is canonical). On-chain / OI / liquidation / social ingestion (record
as future work; do not stub fake success). The live `.env` file.

**Tests to add** (flat `tests/`, mocked, no sockets).
- `tests/test_crypto_data_enablement_flags.py` — with the crypto profile env applied, `provider_registry`
  yields an enabled `ccxt` provider and `data_source_manager` lists `ingest_crypto_funding` as enabled;
  with flags off, both are disabled (assert the default-off contract).
- `tests/test_crypto_funding_pipeline_smoke.py` — mocked exchange → poller writes ≥1 row to
  `crypto_funding_rates` → `compute_positioning_features` returns all six features, PIT-safe; an equity
  symbol returns zeros (no lookahead). Mirror `tests/test_crypto_funding_features.py`.
- `tests/test_crypto_feature_parity_guard.py` — flipping `USE_FUNDING_FEATURES` changes the feature
  fingerprint and a train(off)/serve(on) mismatch fails closed; a canary in an override env never appears
  in any returned feature value or log.

**Validation commands** (from repo root; expected exit in parentheses).
- `python tools/validate_crypto_data.py` → 0 (PASS or SKIP-when-offline; never a silent fake-green)
- `python -m pytest tests/test_crypto_data_enablement_flags.py tests/test_crypto_funding_pipeline_smoke.py tests/test_crypto_feature_parity_guard.py tests/test_crypto_funding_features.py -q` → 0
- `python tools/syntax_check_workspace.py` → 0
- `python tools/coverage_gate.py` → 0 (new `tools/validate_crypto_data.py` logic covered)

*After implementation, audit your own work. Show exact files changed, why each change is required, and how
the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes
made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`,
`python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output
lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.*

---

## CRYPTO-02 — Crypto execution path: IBKR crypto contract + router preference (sim/paper-gated)

**Mission.** Teach the execution layer to construct and route **crypto** orders — add IBKR crypto contract
construction (`secType="CRYPTO"`, exchange `PAXOS`, e.g. `symbol="BTC"`, `currency="USD"`) to the existing
contract dispatcher, and make the router prefer a crypto-capable broker for crypto batches — **strictly
inside dry_run/sim/paper, with every live order/cancel/replace/flatten path left untouched.** This is the
**exact structural twin of FX-06**, which already shipped the same shape for FX (`CASH`/`IDEALPRO`).

**Prerequisites.** CRYPTO-01 (soft — provides the data/symbols). CRYPTO-03 (soft — provides 24/7 timing;
if absent, crypto inherits the default timing path, which is acceptable for this task).

**Global constraints.** Shared constraints bind. **No live broker mutation, including tests.** New crypto
code paths must remain behind the same `dry_run`/execution-mode/kill-switch/reconcile gates; they may not
introduce a new bypass. This task changes **which `Contract` is constructed** and **which broker is
preferred** — never the `placeOrder`/cancel/flatten mechanics.

**Verified anchors (re-grepped 2026-06-23).**
- `engine/execution/broker_ibkr_gateway.py:820` — `def _mk_stock_contract(symbol)` (`secType="STK"`); `:856` `def _mk_fx_contract(symbol)` (`secType="CASH"`, `exchange="IDEALPRO"`, base/quote split — the FX-06 twin); `:871` `def _mk_contract_for_symbol(symbol)` dispatcher (FX vs STK today). Call sites: **1256, 1776, 2096, 2159**.
- `engine/execution/broker_router.py:741` `def _batch_has_fx(orders)`, `:750` `def _fx_capable_broker(chain)` (returns `"ibkr"`), `:759` `def _prefer_fx_capable_broker(chain, orders)` — the FX-06 router twin to mirror. Gates: `_execution_gate_or_block` (`:452`), `_real_trading_gate_or_block` (`:485`). Adapter resolution: `_resolve_alpaca_apply` (`:170`), `_resolve_ibkr_apply` (`:184`).
- `engine/data/asset_map.py:76-99` — `asset_class_for_symbol`; crypto via `_OVERRIDE`/`_DEFAULT` + heuristic `if s in ("ETH","SOL","BNB","XRP"): return "CRYPTO"` (`:90-91`). **Floor classifier** (no `crypto_instrument.py` exists; use this, plus a local pair check).
- `tests/test_broker_router_dry_run_gates.py`, `tests/test_broker_apply_orders_modes.py`, `tests/test_fx_ibkr_contract_construction.py`, `tests/test_fx_broker_routing.py` — existing safety/contract tests to mirror.

**In scope.**
1. **Capture baselines first** (so "no regression" is objective): `python tools/pyright_money_path_gate.py
   > /tmp/crypto02_pyright_baseline.txt 2>&1 || true` and `python -m pytest
   tests/test_broker_router_dry_run_gates.py tests/test_broker_apply_orders_modes.py -q >
   /tmp/crypto02_router_baseline.txt 2>&1 || true`. Cite both in the self-audit.
2. **IBKR crypto contract** in `broker_ibkr_gateway.py`:
   - Add `def _is_crypto_symbol(symbol) -> bool` (True only when `asset_class_for_symbol(symbol) == "CRYPTO"`
     **and** the symbol resolves to a base/quote, e.g. `BTC`→base `BTC`/quote `USD`). Import lazily.
   - Add `def _crypto_pair_parts(symbol) -> tuple[str,str] | None` and `def _mk_crypto_contract(symbol)`
     building `Contract()` with `secType="CRYPTO"`, `exchange="PAXOS"`, `symbol=base`, `currency=quote`
     (IBKR crypto convention). Mirror `_mk_fx_contract`'s small-pure-helper shape.
   - Add `def normalize_crypto_symbol(symbol) -> str` local fallback (canonical bare-root form) and
     **flag the missing `crypto_instrument.py` canonical owner** in the self-audit.
   - Extend `_mk_contract_for_symbol` (`:871`) to dispatch **crypto → FX → STK** (crypto checked first
     among the non-equity branches; order chosen so the disjoint crypto/FX/STK sets cannot collide). The
     four call sites (1256/1776/2096/2159) already call the dispatcher — **do not touch them**; the
     dispatch change is enough.
3. **Crypto-aware routing** in `broker_router.py`:
   - Add `def _batch_has_crypto(orders) -> bool` (via `asset_class_for_symbol`) and
     `def _crypto_capable_broker(chain) -> Optional[str]` (return the crypto-capable broker alias present
     in the validated chain — `ibkr` for the PAXOS path, or `alpaca` if you also wire Alpaca crypto;
     pick one and document it). Mirror `_batch_has_fx`/`_fx_capable_broker`.
   - In the public router entry, when `_batch_has_crypto(...)` and a crypto-capable broker exists, prefer
     it **at the front of the existing validated chain** — routing through the *same*
     `validate_live_failover_chain` / `live_broker_environment_contract` / `_execution_gate_or_block` /
     `_real_trading_gate_or_block` / pre-live-reconcile / options-block logic. Never skip a gate. No
     crypto-capable broker → existing fallback (do not route crypto to an incapable broker outside
     failover semantics). Keep the file pyright-clean (money-path gated).
4. **Docs:** update `engine/execution/README.md` — the IBKR `CRYPTO`/`PAXOS` contract path, the
   crypto routing preference, and that **no live order path is altered**. State explicitly that crypto
   execution is IBKR-PAXOS (or the chosen broker), not a new exchange adapter, in this task.

**Out of scope (do not touch).** The live `app.placeOrder`/cancel/flatten mechanics. The sim weight→qty
conversion at `engine/execution/broker_sim.py:2435` (`target_qty = (to_w * equity) / px_mid`) — that
share-vs-unit seam is **unowned across the crypto series; flag it NO-GO-pending-owner in the self-audit**,
do not implement. `storage.py` schema. Any new standalone exchange adapter (`broker_coinbase.py`,
`broker_binance.py`) — out of scope here; pick the IBKR path.

**Tests to add** (`tests/`, `unittest.TestCase`, `pytestmark = pytest.mark.safety_critical`, fully mocked,
no sockets, `dry_run=True`).
- `tests/test_crypto_ibkr_contract_construction.py` — `_mk_crypto_contract("BTC")` → `secType=="CRYPTO"`,
  `exchange=="PAXOS"`, `symbol=="BTC"`, `currency=="USD"`; `_mk_contract_for_symbol` returns CRYPTO for
  `BTC`, CASH for an FX pair, STK for `AAPL`; `_is_crypto_symbol` True for crypto, False otherwise.
- `tests/test_crypto_broker_routing.py` — a crypto batch prefers the crypto-capable broker at the chain
  front; an equity/FX batch is unchanged; routing never bypasses gates (assert
  `validate_live_failover_chain` / `_execution_gate_or_block` / `_real_trading_gate_or_block` still
  consulted via mocks); no crypto-capable broker → safe fallback. All `dry_run=True`; assert no live
  `placeOrder` is invoked.

**Validation commands** (from repo root; expected exit in parentheses).
- `python -m pytest tests/test_crypto_ibkr_contract_construction.py tests/test_crypto_broker_routing.py -q` → 0
- `python -m pytest tests/test_broker_router_dry_run_gates.py tests/test_broker_apply_orders_modes.py tests/test_fx_ibkr_contract_construction.py tests/test_fx_broker_routing.py -q` → 0 (FX/equity regression guard vs `/tmp/crypto02_router_baseline.txt`)
- `python tools/pyright_money_path_gate.py` → 0 (no new diagnostics over `/tmp/crypto02_pyright_baseline.txt`)
- `python tools/syntax_check_workspace.py` → 0

*After implementation, audit your own work. Show exact files changed, why each change is required, and how
the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes
made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`,
`python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output
lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.*

---

## CRYPTO-03 — 24/7 crypto session/clock + asset-class-aware session handling

**Mission.** Crypto trades 24/7, but the system has no crypto clock: the execution session gate is
FX-week-aware (`fx_session.py`) and the feature session flags are hardcoded equity-style Asia/EU/US UTC
windows. Add a **pure, side-effect-free crypto session module** that models crypto as always-open (never
weekend-closed), and make the execution policy + feature session flags **asset-class-aware** so crypto is
never suppressed by equity/FX market-hours assumptions. **Equity and FX behavior must be byte-for-byte
unchanged.** This mirrors FX-04 (`fx_clock.py`) + FX-06 (`fx_session.py`).

**Prerequisites.** None hard. Pairs with CRYPTO-02 (execution consumes the timing).

**Global constraints.** Shared constraints bind. The session module must be pure (no DB/network/global
side effects). No schema change. Integration must reuse the existing execution-policy metadata/audit path.

**Verified anchors (re-grepped 2026-06-23).**
- `engine/execution/fx_session.py:135` `def fx_session_state(symbol, now_ms) -> Dict`; `:172`
  `def fx_timing_adjustment(symbol, now_ms, base_decision) -> Dict` — the twin to mirror (returns
  `is_fx`, `session`, `is_open`, `in_rollover_window`, `next_open_ms`; weekend → block, rollover → delay).
- `engine/data/prices/fx_clock.py:95` `def fx_market_closed(ts_ms)`, `:27-30` ET week-boundary constants —
  the canonical FX session-boundary owner; the crypto clock is its always-open counterpart.
- `engine/execution/execution_policy_engine.py:50` `from engine.execution.fx_session import
  fx_timing_adjustment`; `:595` `_attach_fx_session_metadata(...)`; `:607` `def apply_execution_policy(...)`;
  `:1026` `if _env_bool("EPE_FX_SESSION_ENFORCE", True):` — the integration site to mirror for crypto.
- `engine/strategy/feature_registry.py:101-103` session-flag feature ids (`base.session_asia/eu/us`);
  `:1502` `def _session_flags(ts_ms)` (fixed UTC windows, **no asset-class awareness**); `:2558-2562`
  the build sites.

**In scope.**
1. **`engine/execution/crypto_session.py`** (new; pure; never raises; returns deterministically):
   - `def normalize_crypto_symbol(symbol) -> str` local fallback (flag missing `crypto_instrument.py`).
   - `def crypto_session_state(symbol, now_ms) -> dict` returning
     `{"is_crypto": bool, "session": "open"|"maintenance", "is_open": bool, "in_maintenance_window": bool,
     "next_open_ms": int|None}`. Crypto is **always open** by default (`is_open=True`, `next_open_ms=None`),
     with an **optional, env-configurable per-venue maintenance window** (default empty/disabled) via
     `CRYPTO_MAINTENANCE_*` knobs — because some venues (e.g. brokered crypto) have brief daily downtime.
     Default behavior = 24/7 with no suppression.
   - `def crypto_timing_adjustment(symbol, now_ms, base_decision) -> dict` that, for crypto only:
     (a) never sets a weekend/closed block (unlike FX) unless an explicit maintenance window is configured
     and active, in which case it annotates `crypto_session_blocked=True` + reason; (b) otherwise passes
     `base_decision` through unchanged. Non-crypto passes through untouched. `base_decision` is the
     `decide_execution_strategy(...)` dict shape.
2. **Make feature session flags asset-class-aware** in `feature_registry.py`: for crypto symbols, the
   Asia/EU/US session flags must not impose an equity-day bias. Add a crypto branch (or a 24/7 session
   feature) so a crypto row is not labeled as "out of session." Keep equity/FX outputs **byte-identical**
   (golden test). If you add a feature id, register it for train/serve parity.
3. **Integrate into `apply_execution_policy`** (mirror the FX block at `:1026`): for crypto orders call
   `crypto_timing_adjustment`, attach its annotations via the existing metadata path (mirror
   `_attach_fx_session_metadata`), and only suppress when an explicit maintenance window blocks. Guard with
   `EPE_CRYPTO_SESSION_ENFORCE` (default on) so tests can disable it. Non-crypto orders take an identical
   code path to today (assert via golden test). Reuse the existing suppression/audit path — **no new
   table/column**.
4. **Docs:** `docs/CRYPTO_SESSION.md` + a note in `engine/execution/README.md`: crypto is 24/7, the
   maintenance-window knobs and defaults, and that equity/FX session logic is unchanged.

**Out of scope (do not touch).** FX session/clock files. Cost math (CRYPTO-04). Sizing (CRYPTO-06). Schema.
The sim weight→qty seam (`broker_sim.py:2435`).

**Tests to add** (`tests/`).
- `tests/test_crypto_session.py` — `crypto_session_state` returns `is_open=True` for arbitrary weekday and
  weekend timestamps (24/7); a configured maintenance window flips `in_maintenance_window=True`/
  `is_open=False`; env knobs respected; function is pure (same input → same output). Pin a weekend
  timestamp that FX would mark closed but crypto marks open (the key behavioral difference).
- `tests/test_crypto_session_policy_integration.py` (`safety_critical`) — `apply_execution_policy` does
  **not** suppress a crypto order on a weekend (vs FX which would), suppresses only inside a configured
  maintenance window (recorded via the existing audit path), and leaves an equity order **byte-for-byte
  identical** to the pre-change path (golden capture). FX behavior unchanged.

**Validation commands** (from repo root).
- `python -m pytest tests/test_crypto_session.py tests/test_crypto_session_policy_integration.py -q` → 0
- `python -m pytest tests/test_fx_session.py tests/test_fx_session_policy_integration.py -q` → 0 (FX regression guard)
- `python tools/syntax_check_workspace.py` → 0; `python tools/pyright_money_path_gate.py` → 0

*After implementation, audit your own work. Show exact files changed, why each change is required, and how
the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes
made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`,
`python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output
lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.*

---

## CRYPTO-04 — Crypto cost realism in offline sim + CPCV gates

**Mission.** Crypto backtests currently pass on **equity-calibrated fiction**: the offline cost path has no
crypto branch, the CPCV crypto branch resolves only a flat commission, and `AlmgrenChrissCost` has no
crypto impact coefficients. Add **crypto-realistic transaction costs** — maker/taker fee + spread +
**perpetual funding carry** for overnight/held positions — to the offline broker-sim and CPCV cost path,
and route crypto challengers through the existing CPCV/PBO/deflated-Sharpe/Reality-Check/promotion gates
**net of those costs**. **Promote nothing by hand; never bypass champion/challenger governance.** This is
the exact twin of FX-07 (`fx_costs.py` + the `_offline_ac_cost_components` FX branch).

**Prerequisites.** CRYPTO-01 (soft — supplies the funding-rate data the carry term can reference). FX-07 is
merged and is your structural template.

**Global constraints.** Shared constraints bind. Simulated/offline only — no network, no live broker. No
schema change. "Profitable" = net of realistic crypto costs proven through the existing gates, never
asserted by hand. Equity/FX cost behavior must be byte-for-byte unchanged when asset_class is not crypto.

**Verified anchors (re-grepped 2026-06-23).**
- `engine/execution/fx_costs.py` — the twin module: `:124` `is_fx_asset_class`, `:139` `normalize_fx_symbol`,
  `:276` `pip_spread_bps`, `:286` `swap_bps`, `:299` `weekend_gap_bps`, plus CALIBRATION-TODO data tables
  (`FX_PIP_SPREAD:20`, `FX_SWAP_PIPS_LONG/SHORT:36/49`, `FX_PIP_SIZE:64`, `FX_REF_PRICE:80`) and safe env
  overrides (`_json_payload:192`, `_float_override_map:203`).
- `engine/execution/broker_sim.py:39` `from engine.execution.fx_costs import is_fx_asset_class,
  pip_spread_bps, swap_bps, weekend_gap_bps`; `:1314` `def _offline_ac_cost_components(...)`; `:1335`
  `is_fx = is_fx_asset_class(asset_class)`; `:1356-1367` FX swap/weekend terms folded into `total_bps`;
  `:1379-1381` named keys (`fx_pip_spread_bps`, `swap_carry_bps`, `weekend_gap_bps`). **The crypto branch
  mirrors this exactly.** Do **not** touch `_exec_px` (live fill) or the weight→qty seam (`:2435`).
- `engine/strategy/cpcv.py:15` imports the fx_costs helpers; `:166` `def _default_commission_bps(asset_class)`
  — already has `if is_fx_asset_class(asset)` (`:171`) and `if "CRYPTO" in asset` (`:173-176`, env
  `CPCV_CRYPTO_COMMISSION_BPS`/`CPCV_CRYPTO_TAKER_BPS`/`CPCV_CRYPTO_MAKER_BPS`); `:182`
  `def cpcv_cost_config_from_env(...)` (FX branch at `:188`). Extend the crypto branch to resolve
  spread + carry, not just commission.
- `engine/execution/cost_models/almgren_chriss.py:14-16` `_DEFAULT_OVERRIDES` has only `US_EQUITY`/`EQUITY`
  — **no CRYPTO, no FX entry** (crypto silently uses equity `eta=0.142, gamma=0.314`).
- `engine/strategy/gated_backtest.py:run_gated_backtest` forwards `cost_config` into
  `simulate_weight_order_batch`; `engine/strategy/statistical_gates.py` (`passes_promotion_gate`,
  `deflated_sharpe_ratio`, `white_reality_check`); `engine/strategy/promotion_guard.py:assess_challenger`;
  `engine/strategy/champion_manager.py` — the unchanged governance path (feed it cost-adjusted returns).

**In scope.**
1. **`engine/execution/crypto_costs.py`** (new; mirror `fx_costs.py`; no imports from broker_sim/cpcv):
   - `is_crypto_asset_class(asset_class) -> bool` (`str(...).upper().strip()=="CRYPTO"` or `startswith
     ("CRYPTO")`), `is_crypto_symbol(symbol)`, `normalize_crypto_symbol(symbol)` (canonical root form;
     flag missing `crypto_instrument.py`).
   - CALIBRATION-TODO data tables keyed by canonical symbol: `CRYPTO_TAKER_BPS`, `CRYPTO_MAKER_BPS`,
     `CRYPTO_SPREAD_BPS`, and a **funding-carry** reference (`CRYPTO_FUNDING_BPS_PER_DAY` or read the live
     funding rate defensively from the `crypto_funding_rates` data when available, else fall back to the
     table). Conservative placeholder values; module docstring states these are CALIBRATION-TODO, not
     broker-calibrated.
   - `spread_bps(symbol, *, half=True)`, `fee_bps(symbol, *, taker=True)`,
     `funding_carry_bps(symbol, side_sign, nights)` — funding is sign-aware (longs pay positive funding,
     shorts receive) and scales by held nights. Env overrides (`CRYPTO_*_OVERRIDE_JSON`,
     `CRYPTO_FUNDING_*`) with safe parsing; malformed input → defaults, never raises; never read secrets.
2. **Extend `_offline_ac_cost_components`** (`broker_sim.py:1314`, mirroring the FX block at `:1335-1381`):
   when `crypto_costs.is_crypto_asset_class(cost_config.get("asset_class"))`, compute the spread/fee
   component from `crypto_costs` and ADD a `funding_carry_bps` term into `total_bps`; return explicit named
   keys (`crypto_spread_bps`, `crypto_fee_bps`, `funding_carry_bps`). Non-crypto/non-FX output stays
   byte-identical (regression test). Type-annotate (directory-covered by the pyright money-path gate).
3. **Extend the CPCV crypto branch** (`cpcv.py`): in `_default_commission_bps` keep the existing crypto
   commission resolution; in `cpcv_cost_config_from_env`, when crypto, set spread from
   `crypto_costs.spread_bps` and carry `symbol`/`nights` through so they reach
   `_offline_ac_cost_components`. Preserve all non-crypto keys.
4. **Register crypto impact coefficients** in `almgren_chriss._DEFAULT_OVERRIDES` (conservative crypto
   `(eta, gamma)`, clearly labeled CALIBRATION-TODO) so the temporary-impact term is not equity-calibrated;
   add a matching test; keep equity/FX entries unchanged.
5. **Reporting helper** `engine/strategy/crypto_profitability_report.py` (mirror
   `fx_profitability_report.py`): `evaluate_crypto_challengers(challengers, *, n_competing_trials,
   cost_config_base=None) -> dict` that normalizes each symbol, builds a crypto `cost_config`, runs
   `run_gated_backtest`/`cpcv_backtest` net of crypto costs, then calls the **real** gates
   (`passes_promotion_gate`, `compute_pbo`). It reports per-pair/per-factor pass/fail only — it must
   **not** promote; promotion stays with `champion_manager`→`assess_challenger`.
6. **Docs:** `docs/CRYPTO_COST_REALISM.md` — the cost components (fee/spread/funding-carry), env knobs,
   placeholder-calibration caveat, the canonical normalization rule, how to read the report, and the
   no-bypass guarantee.

**Out of scope (do not touch).** UI (future). Live broker paths. The weight→qty seam (`broker_sim.py:2435`,
owned by CRYPTO-02) — flag NO-GO-pending-owner. The gate math (`deflated_sharpe_ratio`,
`white_reality_check`, `compute_pbo`, BH-FDR). Equity/FX cost behavior. Schema.

**Tests to add** (`tests/`).
- `tests/test_crypto_costs_unit.py` — fee/spread bps + a **pinned, asserted-to-tolerance** funding-carry
  value for one pair; `normalize_crypto_symbol` maps `BTCUSD`/`BTC/USD`/`BTC` to one key; `funding_carry_bps`
  sign-aware long vs short and scales by `nights`; safe env-override JSON parsing; canary never leaks.
- `tests/test_broker_sim_crypto_cost_realism.py` (`safety_critical`) — FX-free crypto `cost_config` returns
  the named crypto keys and a higher `total_cost_bps` than `nights=0`; a NON-crypto `cost_config` yields
  byte-identical output to current behavior (regression guard).
- `tests/test_cpcv_crypto_cost_config.py` — `cpcv_cost_config_from_env({"asset_class":"CRYPTO",
  "symbol":"BTC"})` resolves crypto spread and carries `symbol`/`nights`; non-crypto unchanged.
- `tests/test_crypto_gated_backtest_net_costs.py` — `run_gated_backtest` with a crypto cost config yields
  `net_return < gross_return` and lower net Sharpe; a marginal gross-profitable signal flips net-negative.
- `tests/test_crypto_profitability_report.py` — `evaluate_crypto_challengers` passes a strong synthetic
  signal and fails a cost-eaten one net of crypto costs; asserts it imports only sim/gate modules (no live
  path) and never promotes.
- `tests/test_crypto_no_promotion_bypass.py` (`safety_critical`) — a failing crypto challenger fed through
  `assess_challenger` (cost-adjusted returns) returns `passed=False` — governance not bypassed.

**Validation commands** (from repo root).
- `python tools/pyright_money_path_gate.py > /tmp/pyright_crypto04_baseline.txt 2>&1; echo "baseline: $?"` (BEFORE edits)
- `python -m pytest tests/test_crypto_costs_unit.py tests/test_broker_sim_crypto_cost_realism.py tests/test_cpcv_crypto_cost_config.py tests/test_crypto_gated_backtest_net_costs.py tests/test_crypto_profitability_report.py tests/test_crypto_no_promotion_bypass.py -q` → 0
- `python -m pytest tests/test_cpcv_cost_realism.py tests/test_broker_sim_contract.py tests/test_gated_backtest.py tests/test_promotion_guard_fdr.py tests/test_champion_promotion_identity.py tests/test_fx_costs_unit.py -q` → 0 (no regression)
- `python -m pytest tests/ -m safety_critical -q` → 0
- `python tools/pyright_money_path_gate.py` → 0 (diff vs baseline; no new diagnostics in changed files)
- `python tools/validate_repo.py` → 0 (no `--live`)

*After implementation, audit your own work. Show exact files changed, why each change is required, and how
the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes
made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`,
`python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output
lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.*

---

## CRYPTO-05 — Give crypto a first-class model + crypto-aware regime routing

**Mission.** Crypto is currently **excluded from the primary alpha model**: the LightGBM cross-sectional
ranker hard-filters crypto out, prediction routing treats crypto as a generic `MID` regime (no anchor like
FX's `FX_MID`), and there is no crypto regime semantics. Give crypto a first-class model path — either a
crypto-scoped model/sleeve or inclusion in a cross-asset ranker group — and make prediction routing
crypto-aware (a BTC-based regime anchor). **All training/serving is offline/sim; any promotion goes through
the unchanged champion/challenger governance path. No profitability claim in this task.**

**Prerequisites.** CRYPTO-01 (hard — crypto features must flow; `USE_FUNDING_FEATURES` enabled).
CRYPTO-04 (soft — so any backtest is net of crypto costs; if absent, costs fall back to current behavior).

**Global constraints.** Shared constraints bind. Models propose intent only; runtime owns gates. Preserve
train/serve parity. Do **not** add a path that bypasses `assess_challenger`/`champion_manager`. This is the
most research-y prompt — prefer the **smallest** change that lets crypto reach a model without degrading
the equities path, and prove non-regression of equity ranking.

**Verified anchors (re-grepped 2026-06-23).**
- `engine/strategy/models/lgbm_ranker.py:100` `def _is_equity_symbol(symbol)`; `:105` returns True for
  equity classes; `:107` `if asset_class in {"CRYPTO","COMMODITY","FX","RATES","OPTION","OPTIONS"}: return
  False`; `:141` training filter `if not _is_equity_symbol(sym)` — **the exclusion to address**.
- `engine/strategy/predictor.py:2327` `def _prediction_asset_class(symbol)`; `:2341`
  `def _regime_anchor_symbol(symbol)` (FX-only today, `:2343`); `:2366` `default_regime = "FX_MID" if
  asset_class == "FX" else "MID"` — add a crypto anchor/default here; `:3362`
  `def _ranker_equity_scope_symbol(symbol)`; `:3417` ranker scope gate.
- `engine/strategy/conformal.py:82` `def symbol_group(symbol)`; `:92-93` crypto already maps to
  `"asset:CRYPTO"` (crypto already gets its own conformal residual pool — **reuse, don't duplicate**).
- `engine/strategy/jobs/bocpd_regime_update.py:57` `def _symbols()`, default `BOCPD_SYMBOLS=
  "SPY,QQQ,IWM,BTCUSD,ETHUSD"` (crypto already tracked for changepoints — build crypto regime on top).
- `engine/strategy/feature_registry.py:225/287` crypto positioning features; `:324-327` bocpd regime
  features — the inputs a crypto model/regime consumes.

**In scope.**
1. **Decide and document the model strategy** in the self-audit: **(a) crypto-scoped model** (a separate
   model/sleeve trained on crypto symbols only, registered alongside equities) **or (b) cross-asset
   ranker group** (let the ranker train a crypto group disjoint from the equity group). Pick the one that
   is **least disruptive to the equities ranker** and prove equity ranking output is unchanged.
2. **Stop silently dropping crypto with no model.** If (a): add the crypto model and its registration so a
   crypto symbol is routed to it. If (b): in `lgbm_ranker.py`, replace the blanket crypto exclusion with
   an **asset-class-grouped** training/predict path (crypto forms its own cross-sectional group; equities
   stay a separate group, byte-identical). Either way, `_ranker_equity_scope_symbol`/`_is_equity_symbol`
   semantics for equities must not change.
3. **Crypto-aware regime routing** in `predictor.py`: add a crypto regime anchor (BTC) analogous to the FX
   anchor at `:2343`, and a crypto default regime (e.g. `CRYPTO_MID`) at `:2366`, fed by the existing
   bocpd changepoint features for `BTCUSD`/`ETHUSD`. Reuse the conformal `asset:CRYPTO` pool (`:92-93`).
   Optionally add crypto-context regime states (e.g. funding-extreme) **only** from existing features —
   no new data source.
4. **Governance unchanged.** Any crypto challenger must clear `assess_challenger`/`champion_manager` exactly
   as equities/FX do. Add no new promotion entrypoint. If you touch `champion_manager.py`, keep edits
   fully type-annotated (explicit pyright money-path list).
5. **Docs:** `docs/CRYPTO_MODELING.md` — the chosen strategy, the regime anchor, the parity guarantee for
   equities, and the explicit statement that nothing is promoted outside governance and no profitability
   is claimed here.

**Out of scope (do not touch).** The gate math. The conformal grouping (reuse the existing crypto pool).
The promotion governance mechanics. Data ingestion (CRYPTO-01). Cost math (CRYPTO-04). Schema. Do not give
the model order authority.

**Tests to add** (`tests/`).
- `tests/test_crypto_model_routing.py` — a crypto symbol is routed to a model (not silently dropped);
  equity routing/scope is **unchanged** (golden compare of `_ranker_equity_scope_symbol`/`_is_equity_symbol`
  for a fixed equity set); FX routing unchanged.
- `tests/test_crypto_regime_routing.py` — `_regime_anchor_symbol` / default-regime resolution returns a
  crypto anchor/`CRYPTO_MID` for crypto and is unchanged for equities (`MID`) and FX (`FX_MID`); conformal
  `symbol_group("BTC")=="asset:CRYPTO"` still holds.
- `tests/test_crypto_ranker_equity_parity.py` (`safety_critical`) — with crypto enabled, the ranker's
  equity-group output for a fixed equity panel is byte-for-byte identical to the pre-change path (proves
  the equities model did not regress).
- `tests/test_crypto_promotion_governed.py` (`safety_critical`) — a crypto challenger is only promotable
  via `assess_challenger`; a failing one returns `passed=False`.

**Validation commands** (from repo root).
- `python -m pytest tests/test_crypto_model_routing.py tests/test_crypto_regime_routing.py tests/test_crypto_ranker_equity_parity.py tests/test_crypto_promotion_governed.py -q` → 0
- `python -m pytest tests/ -m safety_critical -q` → 0
- `python -m pytest tests/test_promotion_guard_fdr.py tests/test_champion_promotion_identity.py -q` → 0 (governance regression guard)
- `python tools/pyright_money_path_gate.py` → 0; `python tools/syntax_check_workspace.py` → 0

*After implementation, audit your own work. Show exact files changed, why each change is required, and how
the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes
made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`,
`python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output
lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.*

---

## CRYPTO-06 — Crypto risk/sizing: fractional units, vol/leverage profile, live-enable + notional cap

**Mission.** Crypto is currently sized like a generic equity: there is a 35% sleeve budget but **no
fractional-unit awareness, no crypto-specific volatility/leverage cap, and no dedicated crypto live-enable
or notional cap** — FX got `fx_sizing.py`; crypto got nothing. Add a **crypto sizing module** (fractional
units + a crypto leverage/vol cap), consumed read-only into the existing risk path, plus a dedicated
crypto live-trading enable flag and notional cap gate. This mirrors FX-05 (`fx_sizing.py`). **Consume-only;
do not re-size downstream orders or alter the sim weight→qty math.**

**Prerequisites.** CRYPTO-01 (soft). Pairs with CRYPTO-02 (execution consumes the sized intent).

**Global constraints.** Shared constraints bind. Sizing attaches metadata; runtime gates still own safety.
No schema change. Equity/FX sizing must be byte-for-byte unchanged.

**Verified anchors (re-grepped 2026-06-23).**
- `engine/strategy/fx_sizing.py:90` `def fx_weight_to_notional(...)`, `:137`
  `def clamp_fx_weight_to_leverage(symbol, weight, equity, instrument)`, `:188` `__all__` — the twin to
  mirror (consume instrument metadata, clamp to a leverage cap, return `(weight, diag)`).
- `engine/risk/portfolio_risk_engine.py:125` `USE_FX_LEVERAGE_CAPS = ...=="1"`; `:138`
  `_DEFAULT_ASSET_CLASS_BUDGETS`; `:140` `"CRYPTO": 0.35`; `:142` `"FX": 0.50`; `:144` `"UNKNOWN": 0.40`;
  `:208` fallback asset-class lookup via `asset_class_for_symbol` — the FX-leverage-cap pattern to mirror
  for crypto.
- `engine/strategy/portfolio_risk_gate.py:14` imports `asset_class_for_symbol`; `:58-59`
  `PORTFOLIO_SLEEVE_MAX_GROSS_JSON` (per-asset sleeve caps already configurable, incl. CRYPTO);
  `:102` `def _sleeve(sym)`; `:156` per-sleeve max-gross application.
- `engine/execution/broker_sim.py:2435` `target_qty = (to_w * equity) / float(px_mid)` — the
  weight→qty/units conversion. **Out of scope here** (owned by CRYPTO-02); flag, do not touch.

**In scope.**
1. **`engine/strategy/crypto_sizing.py`** (new; mirror `fx_sizing.py`; pure-ish helper, no order authority):
   - `normalize_crypto_symbol(symbol)` local fallback (flag missing `crypto_instrument.py`).
   - `crypto_weight_to_notional(...)` and `clamp_crypto_weight_to_leverage(symbol, weight, equity,
     instrument) -> tuple[float, dict]` — clamp to a crypto leverage cap (`CRYPTO_MAX_LEVERAGE`, conservative
     default) and optionally a vol-target adjustment using existing data; return the clamped weight + a
     diagnostics sub-dict. **Fractional-unit aware** (crypto trades in fractional units, not whole shares) —
     expose a `min_increment`/fractional flag in the diagnostics so downstream knows crypto is fractional.
   - The module **attaches** sizing context to the order `reason`/`crypto` sub-dict; it must **not** write
     back into the sim qty math.
2. **Wire crypto into the risk engine** mirroring the FX leverage-cap path (`portfolio_risk_engine.py:125`):
   add `USE_CRYPTO_LEVERAGE_CAPS` (default on) and apply `clamp_crypto_weight_to_leverage` for crypto
   symbols only; equity/FX paths unchanged. Keep the existing `CRYPTO: 0.35` budget (do not weaken it).
3. **Dedicated crypto live-enable + notional cap gate.** Add `CRYPTO_LIVE_TRADING_ENABLED` (default **off**)
   and `CRYPTO_NOTIONAL_CAP_USD` (default conservative) enforced in the risk/gate path so crypto can be
   isolated from equities — crypto live orders are blocked unless explicitly enabled, independent of the
   global execution-mode/kill-switch (which still apply on top). Reuse the existing sleeve/gate enforcement
   in `portfolio_risk_gate.py`; **no new table**.
4. **Docs:** `docs/CRYPTO_SIZING_RISK.md` — the leverage/vol cap, fractional-unit handling, the
   live-enable + notional-cap knobs and their safe defaults, and the equity/FX non-regression guarantee.

**Out of scope (do not touch).** The sim weight→qty conversion (`broker_sim.py:2435`) — flag
NO-GO-pending-owner. FX sizing. Cost math (CRYPTO-04). The model (CRYPTO-05). Schema. Do not grant order
authority to sizing.

**Tests to add** (`tests/`).
- `tests/test_crypto_sizing_unit.py` — `clamp_crypto_weight_to_leverage` clamps an over-levered crypto
  weight to the cap and passes a within-cap weight through; fractional-unit diagnostics present; pure
  given fixed inputs; `normalize_crypto_symbol` canonicalizes variants.
- `tests/test_crypto_risk_integration.py` (`safety_critical`) — the risk engine applies the crypto
  leverage cap for crypto only and leaves equity/FX sizing **byte-for-byte identical** (golden); the
  `CRYPTO: 0.35` budget still holds.
- `tests/test_crypto_live_enable_gate.py` (`safety_critical`) — with `CRYPTO_LIVE_TRADING_ENABLED` off, a
  crypto live order is blocked; over-cap notional is blocked; equity/FX orders are unaffected; all
  exercised paths are `dry_run`/sim with mocks — no live socket.

**Validation commands** (from repo root).
- `python -m pytest tests/test_crypto_sizing_unit.py tests/test_crypto_risk_integration.py tests/test_crypto_live_enable_gate.py -q` → 0
- `python -m pytest tests/ -m safety_critical -q` → 0
- `python -m pytest -q -k "fx_sizing or portfolio_risk"` → 0 (FX/equity sizing regression guard)
- `python tools/pyright_money_path_gate.py` → 0; `python tools/syntax_check_workspace.py` → 0

*After implementation, audit your own work. Show exact files changed, why each change is required, and how
the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes
made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`,
`python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output
lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.*
