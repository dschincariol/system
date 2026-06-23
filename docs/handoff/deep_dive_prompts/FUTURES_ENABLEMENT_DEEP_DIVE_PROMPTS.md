# FUTURES ENABLEMENT — Deep-Dive Audit, Research & Workstream Decomposition

> **Document type:** Research-backed audit + end-to-end blueprint + prompt-ready workstream
> decomposition (`FUT-01 … FUT-10`). **Produced by the FUT-00 deep dive.** No runtime code was
> changed by this deep dive.
>
> **Target repo:** `/home/david/gitsandbox/system/system` (the only repo; all paths relative).
> **Audit date / research access date:** 2026-06-23.
> **Sibling template:** `docs/handoff/deep_dive_prompts/FX_ENABLEMENT_CODEX_PROMPTS.md` (FX-01..FX-08).
>
> **Provenance tags used throughout:** `[REPO]` verified by reading the tree (file:line);
> `[WEB]` researched with citation; `[ASSUMPTION]` engineering judgment (minimized, risk-flagged).
> Line numbers are exact at audit time — **re-grep before relying on them.**
>
> **Profitability stance (binding):** This system never *asserts* profitability; it *proves* it
> net-of-cost through existing gates (net-after-cost labels → CPCV / deflated Sharpe →
> champion/challenger → shadow → paper) before any capital. Every alpha idea below is a
> **hypothesis to be proven through those gates**, not a claim.

---

## 0. Material correction to the FUT-00 starting facts (read first)

The FUT-00 prompt's "Starting facts" predated the FX merge. Re-grep confirms **FX-01's data layer
is already landed**, which changes both the precedent and the migration numbering:

- `[REPO]` `engine/data/live_prices/oanda_live.py` **exists** (read-only OANDA pricing provider).
- `[REPO]` `engine/data/provider_registry.py:140-149` — an `oanda` polling provider is registered,
  `supports={"asset_classes": ["fx"], "transport": "rest"}`.
- `[REPO]` `engine/data/provider_registry.py:105` — **IBKR already advertises
  `supports={"asset_classes": ["equities", "fx"], "transport": "gateway"}`** (FX-01 updated it). The
  earlier "IBKR is equities-only" claim is now stale.
- `[REPO]` `engine/data/cftc_cot.py:69-82` — COT specs are landed for FX (`6E…6N`, `topic="fx"`) AND
  the index/rates/energy/metals roots already carry their futures code in the symbol tuple, e.g.
  `CotContractSpec("ES", "legacy", "E-MINI S&P 500", ("SPY","VOO","IVV","VTI","ES"), "equity_index")`,
  `("ZN", …, ("TLT","IEF","ZN"), "rates")`, `("CL", "disaggregated", …, ("USO","XLE","OIL","XOM","CVX","CL"), "oil")`,
  `("GC", "disaggregated", …, ("GLD","GDX","GC"), "gold")`. **COT is still ETF-proxy-anchored** and
  `USE_COT_FEATURES` is still `False` by default (`engine/strategy/feature_registry.py:89`).
- `[REPO]` **FX-02 is landed as an *uncommitted working-tree* change (HEAD `947c665`).** `git status`
  shows `?? engine/data/fx_instrument.py`, `?? engine/runtime/schema/migrations/0071_fx_instrument_metadata.py`,
  `M engine/data/asset_map.py`, `M engine/data/universe.py`, and `?? tests/test_fx_*`. So
  `asset_map.py:94` already reads `if is_fx_symbol(s): return "FX"` (the old hardcoded tuple is gone),
  `universe.py` already carries the 9 FX instrument columns + `get_instrument_metadata` accessor, and
  the `_column_type` float-trap is already fixed (`explicit_real_columns`/`explicit_text_columns`,
  `storage_sqlite.py:266-278`). **This is real code to mirror anchor-for-anchor (see Part D).**
- `[REPO]` **FX-02 already holds migration id 71** (`0071_fx_instrument_metadata.py`). Committed HEAD
  tops out at `0070`; with FX-02 in the tree the next free slot is **`0072` — the first futures
  migration.** (The C2 summaries below were drafted saying `0071`; **Part D supersedes them with `0072`.**)

**Consequence:** Futures is the *structural twin of FX*, and the FX **data-plane** pattern is now
real, working code to copy — not a design sketch. The FX **instrument-model** pattern (FX-02) is
still only a written spec, so FUT-01 cannot literally import it; it must implement the same idea for
a dated/rolling/margined contract. Where this document says "FX precedent," it means
**FX-01 = landed code to mirror; FX-02 = spec to mirror.**

---

## PART A — Internet research brief (cited; access date 2026-06-23)

### A1. Market-data sources & ingestion options

| Vendor | Futures coverage | Continuous + OI? | Python | Fit / licensing notes |
|---|---|---|---|---|
| **Databento** `[WEB]` | CME Globex MDP 3.0 (CME/CBOT/NYMEX/COMEX) + ICE | **Yes** — live & historical *continuous-contract symbology* with **open-interest (`n`)** and **volume (`v`)** roll rules; **open interest + settlement** on statistics/definition schemas | Yes (Python/Rust/C++) | **Recommended PRIMARY** for history + reference + OI/roll. Clean roll-rule semantics map directly to FUT-03. CME exchange licensing still applies. |
| **Interactive Brokers (`ib_insync` `ContFuture`)** `[WEB]` | CME group + global via Gateway | Front-month **continuous** via `ContFuture(root, exchange)`; `reqHistoricalData` (incl. `keepUpToDate`) | Yes | **Recommended FALLBACK / live quotes** — the repo's IBKR gateway already exists and already advertises non-equity asset classes, so this is the lowest-blast-radius live path. Continuous is front-month only (no multi-method adjustment). |
| **Polygon.io → massive.com** `[WEB]` | Full CME group, 10+ yr trades/quotes, flat files | Per-contract; continuous less first-class than Databento | Yes | Viable secondary; **futures require CME exchange licensing per feed** (not covered by their data-residency program). |
| Barchart / Norgate / CSI / dxFeed / Tradovate / Rithmic `[WEB]` | Varies (EOD to real-time) | Varies | Varies | Niche fits (e.g. Norgate/CSI for clean EOD back-adjusted history; Tradovate/Rithmic for low-latency execution data). Not primary. |

**CME licensing caveat `[WEB]`:** Non-display use (algorithmic consumption that never shows a human
a quote) requires the CME **Information License Agreement (ILA) + Schedule 1/1a** and **non-display
category fees** charged per Designated Contract Market; **"Derived Works" must be licensed
separately.** A back-adjusted continuous series the system stores and trades from is arguably a
derived work — **confirm licensing scope with the chosen vendor/CME before storing or
redistributing.** *(Exact fee tiers and vendor pricing are **UNVERIFIED** here — procurement-time
confirmation required; **NO-GO** to hardcoding any number.)*

**Recommendation:** **Databento (primary, history+OI+roll reference) + IBKR `ContFuture` (fallback +
live), both behind the existing control-plane, default-off, fail-closed** — exactly the FX-01 OANDA
pattern.

Sources: [Databento futures](https://databento.com/futures), [Databento continuous-contract symbology](https://databento.com/blog/live-continuous-contract-symbology), [Databento OI & settlement](https://databento.com/docs/examples/futures/retrieving-oi-and-settlement-prices), [ib_insync](https://github.com/erdewit/ib_insync), [IBKR historical bars](https://interactivebrokers.github.io/tws-api/historical_bars.html), [Polygon/Massive futures](https://polygon.io/futures), [CME data licensing policy (PDF)](https://www.cmegroup.com/market-data/distributor/files/cme-group-data-licensing-policy-guidelines-and-non-display-licensing-faq.pdf), [CME market-data policy center](https://www.cmegroup.com/market-data/license-data/market-data-policy-education-center.html).

### A2. Continuous-contract construction & roll (the correctness keystone)

`[WEB]` Three adjustment families:

- **Back-adjusted ("Panama"):** add/subtract the roll gap to all prior bars. **Preserves absolute
  point moves** (good for $-P&L and chart patterns) but **injects a cumulative trend bias and can
  drive deep-history prices negative**, which makes **percentage returns wrong** — toxic for ML
  labels computed as returns.
- **Ratio-adjusted (proportional):** multiply prior bars by the price ratio at each roll.
  **Preserves percentage returns, never goes negative,** loses absolute price level.
- **Unadjusted:** raw front-month with visible roll jumps.

**Roll trigger:** calendar (fixed days before expiry) vs **volume/open-interest** (roll when the
deferred contract's liquidity overtakes the front). OI-based rolling tracks where real positioning
is and is the cleaner default for liquid contracts.

**Adopted approach (blueprint):** **Store all three layers** — (1) raw per-contract OHLCV+OI bars
(source of truth, execution reference), (2) an explicit **roll calendar** (roll date + from/to
contract + gap) derived from **open interest with volume confirmation**, and (3) a **ratio-adjusted
continuous series for return/label math** (FUT-06 labels consume this), with a back-adjusted view
available for charting only. **Never compute ML returns across an unadjusted roll boundary.**

Sources: [QuantStart continuous contracts](https://www.quantstart.com/articles/Continuous-Futures-Contracts-for-Backtesting-Purposes/), [QuantPedia methodology](https://quantpedia.com/continuous-futures-contracts-methodology-for-backtesting/).

### A3. Contract specifications (cite-and-confirm)

`[WEB]` Reference values for the flagship liquid contracts (confirm each at the listing exchange's
contractSpecs page at implementation time — **specs occasionally change; micros exist for capital-
efficient testing**):

| Root | Product / Exchange | Multiplier / point value | Tick | Tick value | Micro |
|---|---|---|---|---|---|
| **ES** | E-mini S&P 500 / CME | $50 × index | 0.25 | $12.50 | MES ($5×) |
| **NQ** | E-mini Nasdaq-100 / CME | $20 × index | 0.25 | $5.00 | MNQ |
| **CL** | WTI Crude / NYMEX | 1,000 bbl | $0.01 | $10.00 | MCL (100 bbl) |
| **GC** | Gold / COMEX | 100 oz | $0.10 | $10.00 | MGC (10 oz) |
| **ZN** | 10-yr T-Note / CBOT | $100,000 face | ½ of 1/32 | $15.625 | — |

These map onto the **FUT-01 registry fields** (multiplier, tick size, tick value, currency, expiry
rule, settlement type). Treat every value as **`[WEB]`-sourced and verify-at-build**; mark any spec
you cannot confirm against the exchange page **UNVERIFIED / NO-GO** rather than seeding it.

Sources: [CME ES Micro specs](https://www.cmegroup.com/markets/equities/sp/micro-e-mini-sandp-500.html), [CME Crude Oil specs](https://www.cmegroup.com/markets/energy/crude-oil/light-sweet-crude.contractSpecs.html), [CME Gold specs](https://www.cmegroup.com/markets/metals/precious/gold.contractSpecs.html), [CME 10-yr Note specs](https://www.cmegroup.com/markets/interest-rates/us-treasury/10-year-us-treasury-note.contractSpecs.html).

### A4. Margin, sessions, calendars

- **Margin (SPAN/SPAN2) `[WEB]`:** CME Clearing sets **initial** and **maintenance** margin; **initial
  = 100% of maintenance (non-HRP) or 110% (HRP)**; broker/exchange-set, **time-varying**, with
  separate **intraday day-trade vs overnight** rates. This is **refreshable reference data, never
  hardcoded** — FUT-01 stores a conservative *reference* value; FUT-07 owns enforcement.
- **Sessions `[WEB]`:** Globex opens **Sun 17:00 CT**, runs to **16:00 CT** next day, with a daily
  **maintenance break 16:00–17:00 CT (Mon–Thu)**; equity-index **settlement 15:15 CT** (electronic
  close 16:15 CT). This is the ~23×5 reality that breaks the repo's RTH session assumption.
- **Calendars `[WEB]`:** per-exchange holiday calendars come from CME's trading-hours/holiday
  schedule; model as a **session-calendar id** (`CME_EQUITY`, `CME_GLOBEX_24x5`, etc.) refreshed
  from the exchange, not baked in.

Sources: [CME holiday & trading hours](https://www.cmegroup.com/trading-hours.html), [CME equity trading-hours PDF](https://www.cmegroup.com/education/files/eq-trading-hours.pdf), [CME SPAN methodology](https://www.cmegroup.com/solutions/risk-management/performance-bonds-margins/span-methodology-overview.html), [CME SPAN 2 framework (PDF)](https://www.cmegroup.com/clearing/files/cme-span-2-margin-framework.pdf).

### A5. Systematic futures alpha (robust vs overfit) — hypotheses for the gates

- **Time-series momentum / trend (TSMOM) `[WEB]`** — Moskowitz, Ooi & Pedersen (2012), *JFE*
  104:228-250: a security's own past **12-month** excess return positively predicts the next month
  across 58 futures (equity indices, FX, commodities, bonds); persists ~1yr then partially reverses.
  The canonical CTA backbone. Overfit trap: lookback/holding-period mining; mitigate via the repo's
  **deflated-Sharpe trials gate**.
- **Carry `[WEB]`** — Koijen, Moskowitz, Pedersen & Vrugt (2018), *JFE* 127:197-225: expected return
  assuming price is unchanged; predicts returns in time-series and cross-section across asset
  classes. For futures, carry ≈ **roll yield** (term-structure slope).
- **Commodity term structure / roll yield `[WEB]`** — backwardation ⇒ positive roll yield, contango
  ⇒ negative; the core commodity-curve signal (CME, "Deconstructing Futures Returns").
- **COT positioning `[WEB]`/`[REPO]`** — commercial vs non-commercial extremes; **already ingested**
  (`cot_*` features), just proxy-anchored and off by default.
- **Sizing — vol-targeting / risk-parity `[WEB]`** — the standard CTA construction; scale each
  contract to a constant risk budget. The repo already has vol-targeting scaffolding
  (`engine/strategy/regime_size.py`, `portfolio_risk_engine.py` vol target).

These map cleanly to the **feature-registry + champion/challenger + net-after-cost-label** discipline
(FUT-05/06). None is asserted as profitable here.

Sources: [Time Series Momentum (SSRN)](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2089463), [AQR TSM](https://www.aqr.com/Insights/Research/Journal-Article/Time-Series-Momentum), [Carry (NBER w19325 PDF)](https://www.nber.org/system/files/working_papers/w19325/w19325.pdf), [CME roll-yield primer (PDF)](https://www.cmegroup.com/education/files/deconstructing-futures-returns-the-role-of-roll-yield.pdf).

### A6. Cost & execution realism

`[WEB]` All-in per-contract cost = **broker commission + exchange/clearing fee + NFA fee**. NFA
assessment is **$0.02/side**; IBKR passes through a CME regulatory fee (~$0.02) plus product-specific
exchange fees; **CME transaction-fee schedule changes took effect 2026-04-01**. **Slippage is
per-tick** (not bps), and **roll cost is a two-leg calendar spread** paid each roll. Futures have **no
borrow**, but margin carries an **opportunity cost**. These feed FUT-06 net-after-cost labels and
FUT-08 backtest costs. *(Exact per-product fees are **UNVERIFIED** here — pull from the broker/CME fee
finder at build time.)*

Sources: [IBKR futures commissions](https://www.interactivebrokers.com/en/pricing/commissions-futures.php), [CME clearing fees](https://www.cmegroup.com/company/clearing-fees.html), [CME Jan-2026 market-data fee list (PDF)](https://www.cmegroup.com/market-data/files/january-2026-market-data-fee-list.pdf).

### A7. Operational / regulatory (only what changes the design)

`[WEB]` PDT rules do **not** apply to futures, but **margin calls** and **CFTC position limits /
reportable levels** do; **24×5 trading** means the system must not assume an overnight close (orders,
risk, and roll logic must be session-calendar-aware). Physical-settled contracts (e.g. CL, GC) must
be **rolled or closed before delivery/first-notice**; cash-settled (ES, NQ) expire to settlement.

---

## PART B — Repo audit: exact integration points (end-to-end)

Legend: **Reuse** = use as-is · **Branch** = exists but equity/proxy-coupled, must branch by asset
class · **New** = net-new artifact.

### B1. Instrument / symbol model → **FUT-01**

| Anchor | Role | Disposition |
|---|---|---|
| `engine/data/asset_map.py:63` `asset_class_for_symbol` | classification; precedence `_OVERRIDE`(61-68) → `_DEFAULT`(24) → heuristics(75-84) → `UNKNOWN`(86) | **Branch**: add a futures branch (mirror FX-02's `is_fx_symbol` swap of lines 81-82); keep precedence/signature |
| `engine/data/universe.py:121` `upsert_symbol`; INSERT 161-180; UPDATE 211-231; `get_universe_snapshot` 290-328 | symbol-table writes/reads; both sites write `meta_json` | **Branch**: attach instrument columns on both sites; add `get_instrument_metadata` accessor |
| `engine/runtime/schema/migrations/0001_baseline.py:169` symbols DDL; indexes 372-373 | base schema | **Reuse** (additive only) |
| `engine/runtime/schema/migrations/0070_…` (highest = id 70) | migration chain | **New**: futures cols via `0071_*` (`ADD COLUMN IF NOT EXISTS`) |
| `engine/runtime/storage_sqlite.py:264` `_column_type`; `_create_table` symbols ~2713; `SCHEMA_VERSION=45`→`1` | sqlite test affinity; **float keywords include `value`/`margin`/`rate` but NOT `multiplier`/`tick`/`pip`** | **Branch**: add `multiplier`/`tick_size`/`tick_value` to the REAL set (the exact `_column_type` trap FX-02 calls out for `pip_size`) |
| `engine/runtime/storage.py:56` `_REQUIRED_BACKEND_SYMBOLS` | facade contract | **Do not touch** |

### B2. Data source + ingestion → **FUT-02** (+ control plane)

| Anchor | Role | Disposition |
|---|---|---|
| `engine/data/provider_registry.py:55` `_builtin_provider_definitions`; OANDA 140-149; IBKR 97-106 (`["equities","fx"]`) | provider catalog | **Branch/New**: add a futures provider def, `supports={"asset_classes":["futures"]}`; add `"futures"` to IBKR supports |
| `engine/data/poll_prices.py:717-727` `ActiveSymbolUniverse`; map build 801-855 (incl. landed `oanda` branch) | per-provider symbol routing | **Branch**: add `futures_map` + branch, exactly like the landed `oanda_map` |
| `services/data_source_manager.py` `SourceDefinition`~431; `_default_catalog` (polygon ~636-657); `_provider_account_catalog`~1610; `_PROVIDER_TEST_REGISTRY`~568; `_SOURCE_CATALOG_OPERATIONAL_METADATA`~1938 (cftc_cot ~2035); `inject_into_provider_registry`~5851; `get_desired_ingestion_jobs`~5862; `_http_json_probe` | control plane | **New** catalog entries mirroring OANDA/polygon; **Reuse** generic plumbing |
| `routes/data_sources_routes.py:19-31` generic dispatch; `api_get_data_sources:65` | API surface | **Reuse** (zero edits — new catalog entry auto-surfaces) |
| `engine/runtime/job_registry.py:341` `poll_prices`; 355 `poll_macro`; 362 `backfill_macro_vintages`; 1077 `ingest_cftc_cot` | job specs | **Reuse** `poll_prices` for live; **New** daemon for the roll/continuous derived job (FUT-03) |
| `engine/data/cftc_cot.py:69-82` COT specs | positioning | **Branch**: re-anchor index/rates/energy/metals roots to real futures symbols; keep ETF tuples for proxy joins |

### B3. Macro / curve → **FUT-02/FUT-05**

`engine/data/factor_ingestion.py:112-219` `MACRO_SERIES_SPECS` — FRED spot series (WTI/NatGas/rates).
**Reuse** for macro context; **the futures term structure / roll yield does NOT come from FRED** — it
is computed downstream (FUT-05) from FUT-02's per-contract bars + FUT-03's roll calendar.

### B4. Features / prediction → **FUT-05**

| Anchor | Role | Disposition |
|---|---|---|
| `engine/strategy/feature_registry.py:89` `USE_COT_FEATURES=False`; 289 gating; ~1357-1367 session flags; `asset_class_match` | train/serve feature contract | **Branch**: add futures feature group gated by asset class; turn COT on for futures; relax RTH-session assumption |
| `engine/strategy/predictor.py` asset-class routing / equity-only ranker; `base_model.py`/`ensemble_model.py`/`gbm_model.py` | prediction flow | **Branch**: route futures through generic models with futures features; keep equity ranker scoped |
| `engine/strategy/regime_stack.py`, `hmm_regime.py`, `regime_detector.py` | regime | **Reuse/Branch**: COT already wired to regime context |

### B5. Labels / regime → **FUT-06**

| Anchor | Role | Disposition |
|---|---|---|
| `engine/strategy/labeling.py:21-70` (`label_event`; return@36; z@42) | label math | **Branch**: returns must come from the **ratio-adjusted continuous** series, not raw |
| `engine/strategy/net_after_cost_labels.py:126-209` schema; `load_execution_trace` 414-591 (`fill_notional=q*p`@522); `build_net_after_cost_label` 594-709 | net-of-cost labels | **Branch**: `fill_notional = q*p*multiplier`; add `roll_cost_bps`/`carry_bps` |
| `engine/data/price_hygiene.py:14-15` (`-0.45`/`0.90`), 26-32, 105-140 | split filter | **Branch**: exempt futures / make asset-class-aware (roll & overnight gaps are legitimate) |
| `engine/strategy/meta_labeling.py:68-79`; `retraining_pipeline.py` label query | consumption | **Reuse** (roll-awareness enters via the continuous series + cpcv embargo) |

### B6. Risk / sizing → **FUT-07**

| Anchor | Role | Disposition |
|---|---|---|
| `engine/risk/portfolio_risk_engine.py:135-142` `_DEFAULT_ASSET_CLASS_BUDGETS` (EQUITY/CRYPTO/COMMODITY/FX/RATES/UNKNOWN); caps @1066/1203 | **already asset-class-branched** | **Branch**: add a futures budget; notional must include multiplier |
| `engine/strategy/portfolio_risk_gate.py:104` `asset_class_for_symbol`; `_sleeve_gross/_net` 115-143 (sums **weights**@122) | sleeve exposure | **Branch**: multiply weights by `multiplier` so a 0.05-weight contract isn't undercounted |
| `engine/strategy/portfolio.py` weight→intent; `regime_size.py:34-56` vol/regime multipliers | sizing | **Branch**: weight→**contracts** conversion (÷ multiplier÷price, integer round); **Reuse** vol-target scalars |
| `engine/risk/monte_carlo_risk_engine.py` vol/corr shock | tail risk | **Reuse** (vol shock approximates; roll-gap tail optional) |
| **margin** | initial/maintenance | **New**: a margin engine (none today) |

### B7. Backtest / governance → **FUT-08**

| Anchor | Role | Disposition |
|---|---|---|
| `engine/backtest/cpcv.py:178-245` `CombinatorialPurgedKFold`; purge 135-175 | purged CV | **Branch**: expand embargo around roll dates (pre/post-roll leakage) |
| `engine/backtest/deflated_sharpe.py:46-119` | trials gate | **Reuse** |
| `engine/strategy/portfolio_backtest.py:42-45` cost env; `execution_costs.py:16-18`/70-75 (bps spread); `execution_liquidity_model.py:100-141` (ADV/notional) | cost model | **Branch**: tick-value slippage, roll cost, point-value P&L, multiplier in notional |

### B8. Execution → **FUT-09** (design only)

`engine/execution/broker_ibkr_gateway.py`, `broker_router.py`, `execution_policy_engine.py:274-351`
(qty scaling is multiplier-agnostic — fine), `execution_microstructure.py`. **New** futures route
(recommend extend IBKR `Future()`); strictly **read-only → shadow → paper → governed live**;
roll-aware (never trade into expiry/first-notice). No live order authority until gates pass.

### B9. UI surfacing → **FUT-10**

`ui/view_router.js:16-115` screens/persona allowlists; `ui/dashboard.js:1444-1537`
`buildScreenRefreshTasks`; `ui/data_health.js` (fetch→render pattern); `ui/data_sources.{js,html}`
(control-plane UI, auto-surfaces new sources); `ui/job_catalog.js`; `ui/execution_metrics.js`;
`ui/symbol_context.mjs` (selected-symbol context). `dashboard_server.py:~2642-2688` route assembly.
**Reuse-first**: a new read-only panel = one `loadFuturesPanel()` (data_health.js pattern) + one task
entry + one API endpoint. Mirror UI conventions in `UI_SURFACING_DEEP_DIVE_PROMPTS.md` /
`UI_WORLD_CLASS_DEEP_DIVE_PROMPTS.md` (freshness/confidence/lineage/shadow-only labels).

### B10. Cross-cutting

`engine/runtime/storage.py` — **schema untouched; additive migrations only** (FX-02 `0071` pattern).
`engine/data/time_utils.py` + `engine/data/calendar/` — futures session calendars.
`engine/data/README.md`, `docs/Database_Schema.md` — doc touchpoints.

---

## PART C1 — Executive blueprint

**Today vs target (plain terms).** Today the system trades the **ETF shadows** of futures (SPY for
ES, USO for CL, TLT for ZN, GLD for GC) and borrows one futures dataset — CFTC COT positioning,
anchored to those ETFs and switched off by default — as a side-signal. There is no contract, no
expiry, no roll, no margin, and no 23-hour session anywhere in the code; sizing is `shares × price`
and the backtest charges equity-style bps. **Target:** real listed-futures contracts as first-class
instruments — priced from a real feed, rolled correctly into a continuous series, fed through
futures-native features (term structure, carry, TSMOM, COT), sized by contract multiplier and
margin, costed in ticks, proven net-of-cost through the existing gates, and routed to a futures
broker only after shadow/paper pass — all surfaced in the operator UI.

**Dependency-ordered roadmap (keystones first):**

1. **FUT-01 — Futures instrument/contract model** *(keystone; twin of FX-02)*. Registry of
   root/exchange/multiplier/tick/currency/margin-ref/session-id/expiry/roll-method + continuous alias;
   canonical symbol form; additive `symbols` columns + accessor + migration `0071`.
2. **FUT-02 — Futures market-data source + ingestion** *(twin of FX-01)*. Databento primary + IBKR
   `ContFuture` fallback; per-contract OHLCV **+ open interest**; control-plane registration;
   fail-closed; default-off.
3. **FUT-03 — Roll engine & continuous-series construction** *(correctness keystone)*. OI/volume roll
   calendar, ratio-adjusted continuous + raw + roll dates, roll-yield series; new derived-data daemon.
4. **FUT-04 — Sessions, calendars & hygiene**. Globex 23×5 calendar, settlement/maintenance, holidays;
   exempt futures from the split/dividend filter.
5. **FUT-05 — Features & prediction wiring** *(twin of FX-03)*. Term structure, carry/roll-yield,
   basis, TSMOM, COT (on, re-anchored), seasonality — train/serve parity, asset-class-gated.
6. **FUT-06 — Labels, targets & regime** *(twin of FX-04)*. Ratio-adjusted-return labels,
   net-after-cost labels with futures costs, futures-correct horizons/regimes.
7. **FUT-07 — Risk & sizing** *(twin of FX-05)*. Multiplier notional, margin engine, vol-targeting,
   integer-contract rounding, currency-aware, portfolio margin.
8. **FUT-08 — Backtest realism & governance** *(twin of FX-07)*. Roll/financing/tick costs, roll-aware
   CPCV purging, deflated Sharpe; **gates that prove net-of-cost edge before capital**.
9. **FUT-09 — Execution adapter** *(twin of FX-06)*. Futures route; read-only→shadow→paper→governed
   live; roll-aware orders; no live authority until gates pass.
10. **FUT-10 — UI surfacing** *(twin of FX-08; reuse-first)*. Feed health, roll calendar, term-structure
    curve, COT panel, margin/exposure-by-contract, decision attribution.

**Highest-leverage first step:** **FUT-01 → FUT-02 → FUT-03 as one keystone block.** Every downstream
correctness property — return labels, vol-targeting, backtest P&L, margin sizing — is blocked on
having a real contract registry, a real price feed, and a correctly rolled continuous series. Build
these three first; everything else is additive on top.

**Profitability stance (gate chain, not a claim):** hypothesized edges (TSMOM, carry/roll-yield, COT)
→ **net-after-cost labels** (FUT-06, with real futures costs) → **CPCV + deflated Sharpe** (FUT-08,
roll-aware) → **champion/challenger** promotion → **shadow** → **paper** → governed live (FUT-09).
No live capital until that chain is green.

**Rough effort / critical path `[ASSUMPTION]`:** FUT-01 (S) · FUT-02 (M) · FUT-03 (M-L, the hard one)
· FUT-04 (S-M) · FUT-05 (M) · FUT-06 (M) · FUT-07 (M-L) · FUT-08 (M) · FUT-09 (L, governance-gated)
· FUT-10 (S-M). Critical path runs FUT-01→02→03→06→08→09; FUT-04/05/07/10 parallelize off the keystone.

---

## PART C2 — `FUT-0x` workstream prompts

> Each section is implementable standalone. **Shared global constraints (apply to all FUT-0x):**
> target repo `/home/david/gitsandbox/system/system`; build on existing architecture, never fork;
> `engine/runtime/storage.py` schema untouched (additive numbered migrations only, FX-02 `0071`
> pattern; `ADD COLUMN IF NOT EXISTS`); fail-closed credentials (missing creds disable the feed and
> fall back, like OANDA); **no live broker order/cancel/replace/flatten** except as explicitly gated
> in FUT-09; never log/commit secrets (canary-token tests); **never assert profitability**;
> enforcement lives in runtime code, not just tests/docs. Re-grep every anchor before editing.

---

### FUT-01 — First-class futures instrument/contract model  *(keystone)*

**Mission.** Add a real dated/rolling/margined contract registry as the single source of truth for
futures symbol *semantics* (root, exchange, multiplier, tick size/value, currency, P&L currency,
margin reference, session-calendar id, expiry/settlement rule, roll method, continuous alias, canonical
symbol form), persisted additively on `symbols` and exposed through one accessor — the futures twin of
FX-02.

**Prerequisites.** None (keystone). FUT-02..FUT-10 consume this.

**In scope.** (1) Pure-Python `engine/data/futures_instrument.py`: frozen `FuturesContractMetadata`
dataclass + `parse_futures_symbol(sym) -> FuturesContractMetadata | None` + `is_futures_symbol(sym)`,
recognizing a curated root set (ES/NQ/RTY/YM/CL/NG/GC/SI/HG/ZB/ZN/ZF/ZT/ZC/ZS/ZW/6E/6J/6B + micros)
and the **canonical stored symbol form** (continuous alias `ES.c.0`; dated `ESZ6` = root+month-code+
year). Pure, never raises, returns `None` on non-futures. (2) Reference spec table seeded from A3
(multiplier/tick/currency/expiry/settlement) — values **verify-at-build, NO-GO on any unconfirmed
spec**. Margin is a **conservative reference constant only** (FUT-07 enforces). (3) Rewire
`asset_map.py:81-84` to derive `FUTURES` from `is_futures_symbol` (mirror FX-02's `is_fx_symbol`
swap), preserving `_OVERRIDE`/`_DEFAULT` precedence, signature, and all non-futures classifications.
(4) Persist nine+ nullable columns on `symbols` via `upsert_symbol` (both INSERT 161-180 and UPDATE
211-231) + `get_instrument_metadata(con, symbol)` accessor in `engine.data.universe`. (5) Migration
`0071_futures_contract_metadata.py`. (6) SQLite `_column_type` fix: add `multiplier`/`tick_size`/
`tick_value`/`margin_ref` to REAL handling.

**Out of scope.** Price feed (FUT-02), roll engine (FUT-03), features/labels/risk/exec/UI. No margin
*enforcement*. No second symbol parser. `storage.py` untouched.

**Verified anchors (re-grep).** `asset_map.py:63,81-84,86`; `universe.py:121,161-180,211-231,290-328`;
`0001_baseline.py:169,372-373`; highest migration **id 70** → new is **0071**; `storage_sqlite.py:264`
`_column_type` (REAL keywords lack `multiplier`/`tick`), symbols `_create_table`~2713, `SCHEMA_VERSION=1`@45;
`storage.py:56` `_REQUIRED_BACKEND_SYMBOLS` (untouched). **FX precedent:** FX-02 spec in
`FX_ENABLEMENT_CODEX_PROMPTS.md` (lines ~307-413) — same shape; note FX-02's `0070` is taken so use `0071`.

**Tests.** `tests/test_futures_instrument_parser.py` (parse ES/CL/GC/ZN + micros + dated + continuous;
non-futures→None); `tests/test_futures_asset_class_derivation.py` (`FUTURES` for roots, unchanged for
SPY/BTC/EURUSD/UNKNOWN, override still wins); `tests/test_futures_instrument_metadata_storage.py`
(sqlite reload pattern; **float columns round-trip as float not str** — the `_column_type` regression
guard; accessor returns canonical metadata; non-futures→None); `tests/test_futures_instrument_migration.py`
(`0071` discovered, contiguous, references all new columns).

**Validation.** `python -c "import engine.data.futures_instrument, engine.data.asset_map, engine.data.universe"`;
the four test files; `tests/test_schema_classification.py tests/test_storage_migrator.py`;
`python tools/syntax_check_workspace.py`; `ruff check .`.

**Self-audit & NO-GO.** Confirm: enforcement in runtime (asset_map/universe/migration/sqlite), not just
tests; `asset_class_for_symbol` unchanged for non-futures; `_column_type` floats→REAL; `storage.py`
untouched; **NO-GO any contract spec not confirmed at the exchange page** (stub + `# TODO(FUT-01)` +
xfail). Postgres apply marked "not executed in sandbox — covered by module import + migrator test."

---

### FUT-02 — Futures market-data source + ingestion

**Mission.** Stand up a real read-only futures price provider (per-contract OHLCV + **open interest** +
front-month) registered through the control plane, default-off, fail-closed — the futures twin of FX-01
(OANDA), reusing `poll_prices`, the provider registry, and `data_source_manager`.

**Prerequisites.** FUT-01 (symbol semantics).

**In scope.** (1) `engine/data/live_prices/futures_live.py` (mirror `oanda_live.py`): a provider class
implementing the duck-typed `fetch_last_prices(ticker_map) -> {symbol: {ts_ms,price,bid,ask,spread,
volume,open_interest,source}}`. **Recommended: Databento primary, IBKR `ContFuture` fallback**;
import-guarded deps; creds via `engine.data._credentials`; never raise into the poll loop (`{}` +
warn). (2) Register a `futures` polling provider in `provider_registry.py`
(`supports={"asset_classes":["futures"],"transport":"rest"}`, default-off via `FUTURES_ENABLED`); add
`"futures"` to IBKR `supports` (line 105). (3) `poll_prices.py`: add `futures_map` to
`ActiveSymbolUniverse` (717-727) + a `provider=="futures"` branch in the map builder (801-855) reading
`meta.get("futures_contract")` — exactly like the landed `oanda` branch. (4) Control plane
(`data_source_manager.py`): `futures_data` `SourceDefinition` (`source_type="price_provider"`),
provider-account entry, `_test_futures_connection` in `_PROVIDER_TEST_REGISTRY`, operational metadata;
flows through enable/disable, `inject_into_provider_registry`, `get_desired_ingestion_jobs`. (5)
Re-anchor COT (`cftc_cot.py:69-82`) index/rates/energy/metals roots to real futures symbols (keep ETF
tuples for proxy joins).

**Out of scope.** Roll/continuous construction (FUT-03 — this ships raw per-contract bars + OI only);
features/labels; execution; UI beyond control-plane status. No order endpoints.

**Verified anchors (re-grep).** `provider_registry.py:55,97-106,140-149`; `poll_prices.py:717-727,801-855`;
`data_source_manager.py:~431,~636-657,~1610,~568,~1938,~5851,~5862`, `_http_json_probe`;
`routes/data_sources_routes.py:19-31,65` (auto-surfaces); `job_registry.py:341` (`poll_prices`);
`oanda_live.py` (template). **FX precedent: landed code** — copy OANDA end-to-end.

**Tests.** `tests/test_futures_live.py` (mocked payload → row dict incl. open_interest, source; missing
creds → `{}` no raise; **canary token absent** from rows/logs); `tests/test_futures_provider_registry.py`
(`FUTURES_ENABLED=1` ⇒ `"futures"` in polling names, `supports.asset_classes==["futures"]`; IBKR now
includes `"futures"`); `tests/test_futures_data_source_catalog.py` (catalog entry, test handler,
inject + desired-jobs, `/api/data_sources` lists it without the token).

**Validation.** import line; the three test files; `tests/test_provider_registry_safe_jobs.py
tests/test_data_source_catalog_metadata.py`; ruff; syntax check.

**Self-audit & NO-GO.** Quote the `FUTURES_ENABLED` gate, default-off source, and missing-creds path;
secret-surface grep; confirm read-only (no order/trade endpoints). **Live vendor probe is mock-only
in sandbox — declare that GAP; vendor licensing UNVERIFIED → flag before enabling in prod.**

---

### FUT-03 — Roll engine & continuous-series construction  *(correctness keystone)*

**Mission.** Turn FUT-02's raw per-contract bars + OI into (a) an explicit **roll calendar**
(OI/volume-based, roll date + from/to + gap), (b) a **ratio-adjusted continuous series** for return/
label math, and (c) a **roll-yield** series — stored via idempotent `CREATE TABLE IF NOT EXISTS`,
driven by a new scheduled derived-data daemon.

**Prerequisites.** FUT-01, FUT-02.

**In scope.** (1) `engine/data/futures_roll.py`: pure roll logic — given per-contract bars+OI, pick the
roll date by **open interest crossover with volume confirmation** (A2), emit `(roll_ts, from_contract,
to_contract, gap)`. (2) Continuous construction: **ratio-adjusted** primary (returns-correct, never
negative), plus an unadjusted front-month view; store raw + roll calendar + continuous in new tables
via `CREATE TABLE IF NOT EXISTS` (no `storage.py` change). (3) Roll-yield = annualized
front/next log-price slope. (4) Register `ingest_futures_rolls` daemon in `job_registry.py`
(daemon, `cadence_seconds=86400`, `execution:False`) — **do not** add it to default-on jobs;
control-plane gated. (5) Wire it as a consumer/derived job of the `futures_data` source.

**Out of scope.** Feature ids (FUT-05 consumes the roll-yield series), labels (FUT-06), sizing.
No back-adjusted series used for ML returns (chart-only).

**Verified anchors (re-grep).** `job_registry.py:355,362,1077` (daemon/oneshot patterns);
`cftc_cot.py` `ensure_*`/`CREATE TABLE IF NOT EXISTS` idiom (table-creation template);
`factor_ingestion.py` materialize pattern. **FX precedent:** none direct (FX has no roll) — closest is
the macro materialize/backfill pattern.

**Tests.** `tests/test_futures_roll.py` (synthetic OI crossover → correct roll date; ratio-adjusted
series preserves pct returns and is positive across the roll; **a raw-vs-continuous return at the roll
boundary differs and only the continuous one is roll-safe**); table round-trip on sqlite.

**Validation.** import line; test file; `tools/syntax_check_workspace.py`; confirm daemon registered
but default-off.

**Self-audit & NO-GO.** State the single biggest risk lives here (roll correctness). Prove no negative
prices in the continuous series; prove the daemon is control-plane-gated and default-off.

---

### FUT-04 — Sessions, calendars & hygiene

**Mission.** Model futures sessions (Globex 23×5, settlement, maintenance break, holidays) as a
refreshable session-calendar keyed by FUT-01's `session_calendar` id, and stop equity hygiene from
discarding legitimate futures gaps.

**Prerequisites.** FUT-01.

**In scope.** (1) `engine/data/calendar/` + `time_utils.py`: a futures session calendar (open/close,
maintenance window, settlement time, holiday set) per `session_calendar` id (`CME_EQUITY`,
`CME_GLOBEX_24x5`, …), sourced/refreshable from the exchange schedule (A4), not hardcoded. (2)
`price_hygiene.py:14-32,105-140`: make `is_split_like_price_jump` / the filter **asset-class-aware** —
for futures, exempt roll-boundary and overnight gaps (use FUT-03's roll calendar + relaxed thresholds)
so they are not flagged as splits. (3) `feature_registry.py:~1357-1367` session flags: branch so
futures use the Globex calendar rather than RTH.

**Out of scope.** Order session-gating (FUT-09). Feature math (FUT-05).

**Verified anchors (re-grep).** `price_hygiene.py:14,15,26-32,105-140`; `feature_registry.py:~1357-1367`;
`engine/data/calendar/`, `engine/data/time_utils.py`.

**Tests.** `tests/test_futures_hygiene.py` (a +50% overnight/roll gap on a futures symbol is NOT
flagged; the same on an equity IS flagged — asset-class branch proven); `tests/test_futures_calendar.py`
(maintenance break/settlement/holiday correct for `CME_EQUITY`).

**Validation.** import + test files; ruff; syntax check.

**Self-audit & NO-GO.** Confirm equity behavior is byte-for-byte unchanged; calendars refreshable, not
baked; **NO-GO on any holiday list that can't be sourced — mark `# TODO(FUT-04)`.**

---

### FUT-05 — Features & prediction wiring

**Mission.** Register futures-native features (term-structure slope, carry/roll-yield, basis, TSMOM,
COT, seasonality) with train/serve parity, gated by asset class, consuming FUT-03's raw rows and
roll-yield — the futures twin of FX-03. **No alpha asserted.**

**Prerequisites.** FUT-01, FUT-03 (and FUT-02 data).

**In scope.** Feature loaders + `feature_registry.py` registration for `fut.term_structure_slope`,
`fut.carry`, `fut.roll_yield`, `fut.tsmom_*`, `fut.basis`, plus enabling COT (`USE_COT_FEATURES` for
futures, re-anchored to real roots). All gated by `asset_class_for_symbol(...)=="FUTURES"`; equity/FX
parity untouched.

**Out of scope.** Labels/targets (FUT-06), sizing, execution. Do not change equity feature sets.

**Verified anchors (re-grep).** `feature_registry.py:89,289,443,493-494,1026-1027,1297`; `predictor.py`
asset-class routing; `regime_stack.py` (COT regime wiring). **FX precedent:** FX-03 spec.

**Tests.** registry includes the `fut.*` ids only when asset class is futures; no duplicate feature ids;
train/serve parity holds; COT flows into a futures feature snapshot.

**Validation.** import; feature-registry tests; parity test; ruff; syntax check.

**Self-audit & NO-GO.** Confirm **no per-pair/per-contract alpha is asserted** and equity/FX feature
sets are unchanged; features are hypotheses for the gates.

---

### FUT-06 — Labels, targets & regime

**Mission.** Roll-adjusted, net-after-cost labels and futures-correct horizons/regimes — the futures
twin of FX-04.

**Prerequisites.** FUT-01, FUT-03, FUT-05.

**In scope.** (1) `labeling.py:21-70`: compute returns from the **ratio-adjusted continuous** series,
not raw. (2) `net_after_cost_labels.py:522,594-709`: `fill_notional = q*p*multiplier`; add
`roll_cost_bps`/`carry_bps` fields fed by a futures cost model (A6). (3) futures-appropriate horizons
(no RTH assumption). (4) regime tags for commodity/rates.

**Out of scope.** Sizing/risk (FUT-07), backtest (FUT-08), feature ids (FUT-05).

**Verified anchors (re-grep).** `labeling.py:21-70,36,42`; `net_after_cost_labels.py:126-209,414-591,522,594-709`;
`meta_labeling.py:68-79`; `retraining_pipeline.py` label query. **FX precedent:** FX-04 spec.

**Tests.** label returns across a roll equal continuous-series returns (not raw); net label subtracts
multiplier-correct futures cost; horizon config futures-correct.

**Validation.** import; label tests; ruff; syntax check.

**Self-audit & NO-GO.** Prove returns never cross an unadjusted roll; confirm cost model is futures
(tick/roll), not equity bps; no profitability asserted.

---

### FUT-07 — Risk & sizing

**Mission.** Replace `shares×price` with `contracts×multiplier×price`, add an initial/maintenance
**margin engine**, integer-contract rounding, vol-targeting, and currency-aware notional — the futures
twin of FX-05.

**Prerequisites.** FUT-01 (multiplier/margin ref).

**In scope.** (1) `portfolio_risk_engine.py:135-142`: add a `FUTURES` budget; notional/exposure include
multiplier. (2) `portfolio_risk_gate.py:115-143`: multiply weights by multiplier in `_sleeve_gross/_net`.
(3) `portfolio.py` + `regime_size.py:34-56`: weight→**contracts** conversion (integer round, reuse
vol-target scalars). (4) a **margin engine** consuming FUT-01's margin reference reconciled against a
regulatory/broker cap via `min(...)`. Currency-aware for non-USD-denominated contracts.

**Out of scope.** Execution/order routing (FUT-09); backtest costs (FUT-08).

**Verified anchors (re-grep).** `portfolio_risk_engine.py:135-142,1066,1203`; `portfolio_risk_gate.py:104,115-143`;
`regime_size.py:34-56`; `portfolio.py` sizing. **FX precedent:** FX-05 spec (leverage-cap enforcement).

**Tests.** a futures position's notional/sleeve uses multiplier; margin engine caps contracts at the
broker/reg `min`; integer rounding; equity sizing unchanged.

**Validation.** import; risk/sizing tests; ruff; syntax check.

**Self-audit & NO-GO.** Confirm equity/crypto/FX sizing unchanged; margin enforcement in runtime;
reference vs enforced clearly separated.

---

### FUT-08 — Backtest realism & governance

**Mission.** Roll/financing/tick-value costs, roll-aware CPCV purging, deflated-Sharpe trials — the
gate where futures net-of-cost edge is proven before capital. Futures twin of FX-07.

**Prerequisites.** FUT-03, FUT-06, FUT-07.

**In scope.** (1) `cpcv.py:135-175`: expand embargo around roll dates (no pre/post-roll leakage).
(2) `portfolio_backtest.py:42-45` + `execution_costs.py:16-18,70-75` + `execution_liquidity_model.py:100-141`:
tick-value slippage, two-leg roll cost, point-value P&L, multiplier in notional. (3) wire the futures
path through `deflated_sharpe.py` and the champion/challenger promotion gate.

**Out of scope.** Live execution (FUT-09); feature/label authoring.

**Verified anchors (re-grep).** `cpcv.py:178-245,135-175`; `deflated_sharpe.py:46-119`;
`portfolio_backtest.py:42-45`; `execution_costs.py:16-18,70-75`; `execution_liquidity_model.py:100-141`.
**FX precedent:** FX-07 spec.

**Tests.** purge embargo covers a roll date; backtest P&L uses point value + tick slippage + roll cost;
deflated-Sharpe gate runs on a futures strategy.

**Validation.** import; backtest tests; ruff; syntax check.

**Self-audit & NO-GO.** Prove roll-leakage is purged; costs are tick/roll not bps; **state that any
"edge" is gate-conditional, never asserted.**

---

### FUT-09 — Execution adapter  *(governance-gated; design-first)*

**Mission.** A futures-capable broker route, strictly **read-only → shadow → paper → governed live**,
roll-aware (never trade into expiry/first-notice), with **no live order authority until FUT-08 gates
pass.** Futures twin of FX-06.

**Prerequisites.** FUT-01..FUT-08 green in shadow/paper.

**In scope.** (1) Recommend **extend the existing IBKR gateway with `Future()` order support** (vs
net-new Tradovate/Rithmic) — lowest blast radius; route via `broker_router.py`. (2) Roll-aware order
logic (block/roll near first-notice/expiry using FUT-03 calendar). (3) Execution policy
(`execution_policy_engine.py`) honors futures sessions (FUT-04) and tick slippage. **Live order paths
ship disabled and governance-gated**; shadow/paper first.

**Out of scope.** Anything that grants a model order authority outside the gates. No live trading in
this workstream's default config.

**Verified anchors (re-grep).** `broker_ibkr_gateway.py`; `broker_router.py`;
`execution_policy_engine.py:274-351` (qty scaling multiplier-agnostic — OK); `execution_microstructure.py`.
**FX precedent:** FX-06 spec (read-only→graduated).

**Tests.** order builder produces a valid `Future()` order in **sim/paper only**; roll-window block
fires; no live path reachable without the governance flag; canary creds never logged.

**Validation.** import; sim/paper tests; confirm live disabled by default; ruff; syntax check.Review this repo features and functions, consider how it supports options trading, what data sources it has for options, how options are supported in its prediction engines, explain in short human terms how it works now and recommend how to improve it for options trading.

**Self-audit & NO-GO.** Prove no live order/cancel/replace/flatten is reachable without passing gates;
**NO-GO to enabling live until shadow+paper evidence exists.**

---

### FUT-10 — UI surfacing  *(reuse-first)*

**Mission.** Surface futures read-only: feed health, roll calendar, term-structure curve, COT
positioning, margin/exposure-by-contract, decision attribution — minimal new code, mirroring FX-08 and
existing UI conventions.

**Prerequisites.** FUT-02/03 data; FUT-07 margin.

**In scope.** (1) `data_sources.{js,html}` already auto-surfaces the futures source (zero edits).
(2) A `ui/futures_panel.js` (data_health.js fetch→render pattern) + one `loadFuturesPanel()` task entry
in `dashboard.js:1444-1537` + one read-only API endpoint (roll calendar / term structure / COT / margin).
(3) Reuse `symbol_context.mjs` for per-contract selection. Apply freshness/confidence/lineage/shadow-only
labels per `UI_WORLD_CLASS_DEEP_DIVE_PROMPTS.md`.

**Out of scope.** Order entry UI; anything beyond read-only status/analytics.

**Verified anchors (re-grep).** `ui/view_router.js:16-115`; `ui/dashboard.js:1444-1537`; `ui/data_health.js`;
`ui/data_sources.{js,html}`; `ui/symbol_context.mjs`; `dashboard_server.py:~2642-2688`.
**FX precedent:** FX-08 spec.

**Tests.** panel renders from a mocked endpoint; no token in any payload; existing screens unaffected.

**Validation.** UI smoke/render test; confirm no new route needed for the control-plane panel.

**Self-audit & NO-GO.** Confirm read-only; reuse-first (one panel + one endpoint); no secret in payloads.

---

## PART D — Full executable prompts (FUT-01 … FUT-10)

> **All ten workstreams** are written to FX-01/FX-02/FX-04 depth (verified anchors at exact lines, schema
> reality, test bodies, validation, self-audit) so they can be executed immediately. **Part D supersedes the
> shorter C2 summaries.** All anchors re-confirmed against HEAD `947c665` with FX-01 committed and FX-02 +
> FX-03 (partial) present in the working tree — **mirror that landed code, do not revert it.** **Migration
> number: FX-02 holds `0071`, so futures uses `0072`.** Execute in dependency order
> **FUT-01 → 02 → 03 → 04 → 05 → 06 → 07 → 08 → 09**, with **FUT-10** parallelizable once FUT-02/03 land.
> Hard gates: a human roll-correctness check after FUT-03, and the deflated-Sharpe/CPCV net-of-cost gate in
> FUT-08 before any live capital (FUT-09 ships live disabled).

---

### FUT-01 (full) — First-class futures instrument/contract model  *(keystone)*

**Mission.** Add a real futures **contract registry** as the single source of truth for futures symbol
*semantics* — root, exchange, point-value multiplier, tick size, tick value, price/settlement currency,
a conservative **margin reference**, session-calendar id, expiry/settlement rule, roll method, and the
**canonical stored symbol form** — persisted *additively* on the `symbols` table and exposed through the
existing `engine.data.universe.get_instrument_metadata` accessor. This is the **exact structural twin of
FX-02** (`engine/data/fx_instrument.py` + the `0071` migration + the `universe.py` instrument plumbing),
implemented for a dated/rolling/margined instrument. **No alpha, no profitability claim.**

**Prerequisites.** None (keystone). FUT-02..FUT-10 consume this. FX-02 must remain in the tree (you
extend its plumbing additively; do not revert or rewrite it).

**Global constraints.** The shared FUT-0x constraints (top of Part C2) bind. Additionally: **`storage.py`
untouched** (no entry added to `_REQUIRED_BACKEND_SYMBOLS` at `storage.py:56`); additive migration only
(`ADD COLUMN IF NOT EXISTS`, never drop/rename); **do not edit FX-02's columns, parser, `0071`
migration, or its tests**; preserve `asset_class_for_symbol`'s signature and every non-futures result.

**Canonical futures symbol form (normative — FUT-01 owns this).**

- **Continuous alias:** `<ROOT>.c.<N>` where `N=0` is front-month, `N=1` is next (`ES.c.0`, `CL.c.0`).
- **Dated contract:** `<ROOT><MONTHCODE><YY>` using CME month codes (F G H J K M N Q U V X Z), e.g.
  `ESZ26`, `CLM26`.
- **The parser recognizes ONLY these explicit futures forms — never a bare root.** Bare `ES`/`GC`/`CL`/
  `ZN` already classify as `COMMODITY`/`RATES`/equity-proxy via `asset_map.py:92,96` and as COT lookup
  keys; reclassifying them would break existing behavior. So `parse_futures_symbol("ES")` returns
  `None`, while `parse_futures_symbol("ES.c.0")` and `parse_futures_symbol("ESZ26")` return metadata.
  State this in the module docstring and the accessor docstring.

**In scope.**

1. **`engine/data/futures_instrument.py`** (new; pure — no DB/network/IO; never raises, returns `None`
   on unparseable input). Mirror `fx_instrument.py` structure exactly:
   - Frozen dataclass `FuturesContractMetadata` with fields: `symbol` (canonical), `asset_class="FUTURES"`,
     `instrument_kind` (`"fut_continuous"` | `"fut_dated"`), `root`, `exchange`, `multiplier: float`,
     `tick_size: float`, `tick_value: float`, `price_ccy: str`, `margin_ref: float` (reference only),
     `expiry_rule: str`, `roll_method: str` (default `"oi_volume"`), `continuous_alias: str | None`,
     `session_calendar: str`, `source="parser"`; plus `to_dict()` returning sorted-key JSON-safe dict
     (copy `fx_instrument.py:67-81`).
   - A curated `FUTURES_ROOT_SPECS` dict keyed by root → `{exchange, multiplier, tick_size, tick_value,
     price_ccy, settlement_type, expiry_rule, margin_ref, session_calendar, micro}` seeded from the
     audit's A3 table (`ES/NQ/RTY/YM/CL/NG/GC/SI/HG/ZB/ZN/ZF/ZT/ZC/ZS/ZW/6E/6J/6B` + micros).
     **Every value is verify-at-build: any spec you cannot confirm against the listing exchange's
     contractSpecs page → leave a `# TODO(FUT-01): verify <root> spec` and mark that root NO-GO/xfail;
     do not seed a guessed number.**
   - `parse_futures_symbol(symbol) -> FuturesContractMetadata | None`: normalize `str(sym or "").upper().strip()`;
     match `<ROOT>.c.<N>` or `<ROOT><MONTHCODE><YY>` against `FUTURES_ROOT_SPECS`; return metadata or
     `None`. `is_futures_symbol(symbol) -> bool` = `parse_futures_symbol(...) is not None`.
   - Use the `get_logger` + local `_warn_nonfatal` pattern from `fx_instrument.py:39-50`.
2. **`engine/data/asset_map.py`** — add a guarded import of `is_futures_symbol` (copy the FX guard at
   `:46-55`) and a branch `if is_futures_symbol(s): return "FUTURES"` placed **immediately before** the
   `if is_fx_symbol(s):` branch at `:94`. (The futures canonical forms are disjoint from every existing
   heuristic set, so this cannot reclassify any current symbol.) Do not change `_OVERRIDE`/`_DEFAULT`
   precedence, the signature, or any other branch.
3. **`engine/data/universe.py`** — additively thread futures columns through the *existing* FX plumbing:
   - Add a guarded `from engine.data.futures_instrument import parse_futures_symbol` (mirror the FX guard
     at `:74-84`).
   - Add `_FUTURES_INSTRUMENT_COLUMNS = ("fut_root","fut_exchange","fut_multiplier","fut_tick_size",
     "fut_tick_value","fut_price_ccy","fut_margin_ref","fut_expiry_rule","fut_roll_method",
     "fut_continuous_alias")` next to `_INSTRUMENT_METADATA_COLUMNS` (`:99-109`). Reuse the *shared*
     generic columns `instrument_kind`, `session_calendar`, `instrument_meta_source` (introduced by
     FX-02) for futures rows too — do not duplicate them.
   - Extend `_insert_symbol_row` (`:187-252`) and `_update_symbol_row` (`:255-328`): broaden the SQL
     column lists + value tuples to also write the 10 `fut_*` columns, populated from a new
     `_futures_column_values(meta)` helper (mirror `_instrument_column_values` at `:112-125`; all-`None`
     when not futures). Keep the existing `_missing_instrument_column_error` fallback (`:128-142`,
     extended to recognize the `fut_*` names) so an older DB degrades to the legacy write.
   - In `upsert_symbol` (`:405-417`): after `parse_fx_symbol`, also compute
     `futures_metadata = parse_futures_symbol(raw_sym)`; when FX is `None` and futures is not, canonicalize
     `sym` from `futures_metadata.symbol`. Pass both metadata objects to the row helpers.
   - Generalize `get_instrument_metadata` (`:331-373`) to **dispatch**: try `parse_fx_symbol` first
     (existing FX path, byte-for-byte unchanged); if `None`, try `parse_futures_symbol` and SELECT/return
     the futures columns via a new `_futures_metadata_dict_from_row` (mirror `_metadata_dict_from_row` at
     `:149-184`); else `None`. The FX return shape must not change.
4. **Migration `engine/runtime/schema/migrations/0072_futures_contract_metadata.py`** (new; `id = 72`,
   `description = "futures contract metadata columns"`, `def up(conn)`). Copy the **exact** additive shape
   of `0071_fx_instrument_metadata.py`: one idempotent
   `conn.execute("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS <col> <type>")` per `fut_*`
   column (`TEXT` for root/exchange/price_ccy/expiry_rule/roll_method/continuous_alias; `DOUBLE PRECISION`
   for multiplier/tick_size/tick_value/margin_ref). No backfill (lazy on-write, like `0071`). Do not edit
   any prior migration.
5. **`engine/runtime/storage_sqlite.py`** — (a) extend `_column_type` (`:264-278`): add
   `fut_root,fut_exchange,fut_price_ccy,fut_expiry_rule,fut_roll_method,fut_continuous_alias` to
   `explicit_text_columns` and `fut_multiplier,fut_tick_size,fut_tick_value,fut_margin_ref` to
   `explicit_real_columns`. (b) Append the 10 `fut_*` columns to the `_create_table(con,"symbols",(...))`
   tuple in `_ensure_universe_audit_schema` (`:2726-2750`). Leave `SCHEMA_VERSION=1` (`:45`).

**Out of scope.** Price feed (FUT-02); roll/continuous (FUT-03); sessions (FUT-04); features/labels/risk/
exec/UI. No margin *enforcement* (FUT-07 — `margin_ref` is reference only). No second parser. No edit to
`storage.py`, `table_classification.py`, FX-02 files, or any prior migration. Adding `"FUTURES"` to
`portfolio_risk_engine._DEFAULT_ASSET_CLASS_BUDGETS` is **FUT-07** — note in the self-audit that until then
`asset_class_for_symbol("ES.c.0")=="FUTURES"` falls through to the `UNKNOWN` budget (0.40), which is safe.

**Verified anchors (re-grep).** `fx_instrument.py` (whole file — the template); `0071_fx_instrument_metadata.py`
(id 71 — futures is 72); `asset_map.py:46-55` (FX import guard), `:94` (`is_fx_symbol` branch), `:92,96`
(COMMODITY/RATES bare-root); `universe.py:74-84,99-109,112-125,128-142,149-184,187-252,255-328,331-373,405-417`;
`storage_sqlite.py:45,264-278,2726-2750`; `0001_baseline.py:169,372-373`; `storage.py:56`. Working-tree:
FX-02 uncommitted (`git status` — do not revert it).

**Tests to add** (flat `tests/*.py`, `unittest.TestCase`, `REPO_ROOT` sys.path insert per
`tests/test_fx_instrument_parser.py`; sqlite reload pattern per
`tests/test_fx_instrument_metadata_storage.py` — `DB_PATH`/`TS_STORAGE_BACKEND=sqlite`/`TS_TESTING=1`/
`TIMESCALE_ENABLED=0` + two liveness flags, then `importlib.reload`):

- `tests/test_futures_instrument_parser.py` — `parse_futures_symbol("ES.c.0")` → `instrument_kind=="fut_continuous"`,
  `multiplier==50.0`, `tick_size==0.25`, `tick_value==12.50`, `price_ccy=="USD"`, canonical `symbol=="ES.c.0"`;
  `parse_futures_symbol("ESZ26")` → `"fut_dated"`; `parse_futures_symbol("CL.c.0")` multiplier `1000.0`,
  tick_value `10.0`; **bare `parse_futures_symbol("ES") is None`** and `("GC") is None` (no reclassify);
  non-futures (`SPY`,`EURUSD`,`""`,`None`) → `None`; `is_futures_symbol` agrees on every case.
- `tests/test_futures_asset_class_derivation.py` — `asset_class_for_symbol` returns `"FUTURES"` for
  `ES.c.0`/`ESZ26`/`CL.c.0`; **unchanged** for `SPY`(EQUITY)/`BTC`(CRYPTO)/`GC`(COMMODITY)/`ZN`(RATES)/
  `EURUSD`(FX)/`ZZZ`(UNKNOWN); an `ASSET_CLASS_MAP_JSON` override still wins; signature returns `str`.
- `tests/test_futures_instrument_metadata_storage.py` — after `upsert_symbol(con,"ES.c.0")`: row's `fut_*`
  columns populated; **`isinstance(multiplier,float)` and `multiplier==50.0`** and `fut_root=="ES"` is a
  `str` (the `_column_type` round-trip guard); `get_instrument_metadata(con,"ES.c.0")` returns the futures
  dict; `get_instrument_metadata(con,"EURUSD")` still returns the **FX** dict (FX path intact);
  `get_instrument_metadata(con,"SPY")` → `None`; `upsert_symbol(con,"SPY")` leaves `fut_*` NULL;
  `get_universe_snapshot(con)` still round-trips.
- `tests/test_futures_instrument_migration.py` — `importlib.import_module(".0072_futures_contract_metadata")`:
  `id==72`, callable `up`, non-empty `description`; `expected_migration_ids()` includes 72, strictly
  increasing, contiguous through 72; module source references all 10 `fut_*` column names.

**Validation commands** (from repo root; sqlite for storage):

- `python -c "import engine.data.futures_instrument, engine.data.asset_map, engine.data.universe"` → 0
- `python -c "from engine.data.futures_instrument import parse_futures_symbol as p, is_futures_symbol as f; m=p('ES.c.0'); assert m and m.multiplier==50.0 and m.tick_value==12.5 and m.symbol=='ES.c.0'; assert p('ES') is None and p('SPY') is None; assert f('CLM26') and not f('GC')"` → 0
- `python -c "import importlib; m=importlib.import_module('engine.runtime.schema.migrations.0072_futures_contract_metadata'); assert m.id==72 and callable(m.up)"` → 0
- `python -c "from engine.runtime.schema.migrator import expected_migration_ids as e; ids=e(); assert 72 in ids and list(ids)==sorted(ids)"` → 0
- `python -m pytest tests/test_futures_instrument_parser.py tests/test_futures_asset_class_derivation.py tests/test_futures_instrument_metadata_storage.py tests/test_futures_instrument_migration.py -q` → 0
- `python -m pytest tests/test_fx_instrument_parser.py tests/test_fx_instrument_metadata_storage.py tests/test_fx_asset_class_derivation.py tests/test_storage_migrator.py tests/test_schema_classification.py -q` → 0 (FX-02 regression guard)
- `python tools/syntax_check_workspace.py` → 0; `ruff check .` → 0
- `git diff --stat -- engine/runtime/storage.py engine/runtime/schema/migrations/0071_fx_instrument_metadata.py engine/data/fx_instrument.py` → empty (FX-02 + facade untouched)

**Self-audit & NO-GO.** (1) List changed files (new: `futures_instrument.py`, `0072_*`, 4 tests; modified:
`asset_map.py`, `universe.py`, `storage_sqlite.py`); confirm `storage.py`, `table_classification.py`, FX-02
files, and prior migrations are untouched. (2) Paste the before/after of the `asset_map.py` futures branch
and the `universe.py` dispatch in `get_instrument_metadata`; confirm enforcement is in runtime, not tests.
(3) Confirm FX `get_instrument_metadata` output is byte-identical and every non-futures `asset_class_for_symbol`
result is unchanged. (4) Confirm `_column_type` floats→REAL with the round-trip test cited. (5) **NO-GO any
contract spec not confirmed at the exchange page** (xfail + TODO). (6) Postgres `0072` apply: "not executed
in sandbox — covered by module-import + migrator-contract test." (7) Note the `UNKNOWN`-budget fall-through
until FUT-07. State **GO** or **NO-GO** with evidence.

---

### FUT-02 (full) — Futures market-data source + ingestion

**Mission.** Stand up a real **read-only** futures price provider that returns per-contract OHLCV **plus
open interest** through the existing polling loop, registered through the data-source control plane,
**default-off and fail-closed** — the exact structural twin of FX-01's landed OANDA feed
(`engine/data/live_prices/oanda_live.py` + the `oanda`/`oanda_fx` registrations). FUT-02 lands **raw
per-contract bars + OI only**; the roll calendar and continuous series are FUT-03. **No order/trade/
account-mutation endpoint anywhere. No alpha, no profitability claim.**

**Prerequisites.** FUT-01 (symbol semantics: the canonical futures symbol + `meta_json.futures_contract`
mapping, exchange, currency). FX-01 is committed and is the line-exact template — **mirror it, do not
fork the polling loop or the control plane.**

**Global constraints.** Shared FUT-0x constraints bind. Additionally: **read-only market data only** — do
not import or wire any broker order/cancel/replace/flatten path; **never log/echo the vendor token** (use
canary-token tests asserting absence in rows, logs, evidence, status payloads, `meta_json`); **fail closed**
— missing credentials disable the feed and fall back to existing providers exactly like OANDA's
`safe_no_credential` behavior (return `{}`, warn once, never raise into the poll loop); `storage.py` schema
untouched (the raw-bars table is created via in-module `CREATE TABLE IF NOT EXISTS`, FUT-03's pattern).

**Vendor decision (from audit A1).** Primary **Databento** (continuous-contract symbology with OI `n` /
volume `v` roll rules + settlement + OI on the statistics schema), fallback **IBKR `ContFuture`** (already
connected; reuses the existing gateway). Select via `FUTURES_PROVIDER` env (default the vendor); credential
via `get_data_credential` (e.g. `DATABENTO_API_KEY`). **Vendor licensing/pricing is UNVERIFIED — confirm CME
non-display / derived-works scope before enabling in prod; flag in self-audit.**

**In scope.**

1. **`engine/data/live_prices/futures_live.py`** (new) — mirror `oanda_live.py` exactly: import-guarded
   `requests` (or vendor SDK) with an `_unavailable` sentinel raised only if used without deps
   (`oanda_live.py:16-22,103-104`); `get_logger` + local `_warn_nonfatal` with `once_key`
   (`:31-46`); a `FuturesPriceProvider` class with `fetch_last_prices(ticker_map: Dict[str,str]) ->
   Dict[str,dict]` returning the standard row **plus open interest**:
   `{"ts_ms","price","bid","ask","spread","volume","open_interest","source":"futures"}` (the OANDA row
   shape at `:151-159` + `open_interest`). Resolve creds via `get_data_credential("DATABENTO_API_KEY")`
   (fallback per vendor); on missing creds / HTTP failure → `{}` + warn (`:165-200`), **never raise**.
   Read-only pricing/candles/statistics endpoint only.
2. **Raw-bars persistence** — add `ensure_futures_bars_table(con)` (in `futures_live.py` or a small
   `engine/data/futures_bars.py`) creating `futures_contract_bars(contract TEXT, ts_ms BIGINT, open REAL,
   high REAL, low REAL, close REAL, volume REAL, open_interest REAL, source TEXT, PRIMARY KEY(contract,
   ts_ms))` via idempotent `CREATE TABLE IF NOT EXISTS` (copy `cftc_cot.py:196` shape). This is the table
   **FUT-03 consumes**; it keeps OI first-class without any `storage.py` change.
3. **`engine/data/provider_registry.py`** — mirror the OANDA registration:
   - Add `_build_futures()` importing `FuturesPriceProvider` (copy `_build_oanda` at `:78-79`).
   - Add a `PriceProviderDefinition(provider_name="futures", mode="polling", implementation_kind=
     "live_price_provider", enabled=_env_enabled("FUTURES_ENABLED", False), daemon_job_name="poll_prices",
     supports={"asset_classes":["futures"],"transport":"rest"}, build_price_provider=_build_futures)` next
     to the OANDA def (`:141-149`).
   - In `_operational_market_data_job_names` (`:340`), add `futures_enabled = _env_enabled("FUTURES_ENABLED",
     False)` and `futures_key = bool(get_data_credential("DATABENTO_API_KEY"))` (copy the `oanda_enabled`/
     `oanda_key` lines at `:348-349`) and extend the `poll_prices` keep-condition at `:376` with
     `or (futures_key and futures_enabled and ((not chain) or ("futures" in chain)))`.
   - Add `"futures"` to the **IBKR** def's `supports["asset_classes"]` (currently `["equities","fx"]` at
     `:105`) — IBKR serves CME futures.
4. **`engine/data/poll_prices.py`** — mirror the landed `oanda_map` wiring:
   - Add `futures_map: Dict[str,str] = field(default_factory=dict)` to `ActiveSymbolUniverse` (`:718,724`)
     and include it in `assigned_symbol_count` (`:727-728`).
   - In the map-building loop add `if provider == "futures" or str(meta.get("futures_contract") or "").strip():`
     populating `futures_map[sym] = meta.get("futures_contract")` (copy the oanda branch `:819-821`).
   - Apply `filter_symbol_mapping_for_shard(futures_map, INGESTION_SHARD)` (`:867`), add `futures_map=futures_map`
     to the returned universe (`:874`), and add a `futures` branch to `_provider_symbol_map_for_cycle`
     (`:883-888`). The generic `PollingProviderSession` (`:584`, built via `build_price_provider("futures")`
     at `:1857/:1877`) fetches it — no per-provider fetch code needed.
5. **`services/data_source_manager.py`** — mirror the landed OANDA control-plane entries:
   - Add the vendor token to the base credential env list (near `OANDA_ACCESS_TOKEN` at `:118-119`) and
     `"FUTURES_ENABLED": "0"` to the settings defaults (near `:137`).
   - Add `"futures_data": {"handler": "_test_futures_connection", "label": "Futures pricing"}` to
     `_PROVIDER_TEST_REGISTRY` (`:570`).
   - Add a `"futures_data": SourceDefinition(source_type="price_provider", display_name="Futures Data",
     provider_name="futures", job_name="poll_prices", default_enabled=False, credential_env=
     {"api_key":"DATABENTO_API_KEY"}, setting_env={"provider":"FUTURES_PROVIDER","roots":"FUTURES_ROOTS"},
     safe_to_auto_enable=False, guide=_source_guide(... explicit read-only / no-order-authority note ...))`
     to `_default_catalog()` (mirror `oanda_fx` at `:658-726`).
   - Add a `"futures"` `ProviderAccountDefinition` (mirror `oanda` at `:1627-1666`); add
     `"futures_data": {...}` to `_SOURCE_CATALOG_OPERATIONAL_METADATA` with
     `storage_tables=("prices","price_quotes","price_quotes_raw","price_provider_health","futures_contract_bars")`,
     `consumers=("price_router","futures_roll_engine","model_feature_snapshots","dashboard_data_health")`,
     `safe_to_auto_enable=False` (mirror `:1949-1953`); add `"futures_data": _PRICE_CONTRACT` (`:2212`) and
     `"futures_data": "_populate_generic_price_marker"` (`:2407`).
   - Implement `_test_futures_connection(self, source)` mirroring `_test_oanda_connection` (`:7892-7914`):
     resolve token via `self._connection_effective_env_value(source, "DATABENTO_API_KEY")`; if absent →
     `self._missing_credentials_result("futures_data","DATABENTO_API_KEY",source=source)`; else probe a
     **read-only** vendor metadata/pricing endpoint via `self._http_json_probe(source, provider=
     "futures_data", url=..., headers={"Authorization": f"Bearer {token}"}, expected_paths=(...),
     success_message=..., empty_message=...)`. **Never put the token in `evidence`/`params`.**
6. **COT re-anchor (additive)** — in `engine/data/cftc_cot.py:69-82`, **append** each index/rates/energy/
   metals root's canonical futures continuous alias (e.g. `"ES.c.0"`) to its `CotContractSpec` symbol tuple,
   keeping the existing ETF + bare-root entries. This lets FUT-05 join COT to the real futures symbol; it does
   not change the topic or any existing mapping. (FUT-01 is still the sole author of any *new* COT FX/futures
   contract specs.)
7. **Routes** — confirm `futures_data` surfaces through the generic dispatch in `routes/data_sources_routes.py`
   (it will, with zero edits, like `oanda_fx`). Add nothing unless a test proves a gap.

**Out of scope.** Roll calendar / continuous / roll-yield (FUT-03 — FUT-02 lands raw bars + OI only);
features/labels/risk/exec; any broker order path; any OANDA/IBKR order endpoint. No `storage.py` schema
change. No new daemon (reuse `poll_prices`).

**Verified anchors (re-grep).** `oanda_live.py` (whole file — the template; row shape `:151-159`, creds
`:110-111`, fail-closed `:165-200`); `provider_registry.py:43,78-79,141-149,302,340,348-349,376,383`, IBKR
supports `:105`; `poll_prices.py:38,584,718,724,727-728,819-821,867,874,883-888,1857`;
`data_source_manager.py:118-119,137,570,658-726,1627-1666,1949-1953,2212,2407`, `_test_oanda_connection`
`:7892-7914`, helpers `_missing_credentials_result:7383`, `_connection_effective_env_value:7464`,
`_http_json_probe:7656`; `cftc_cot.py:69-82,196`; `routes/data_sources_routes.py` generic dispatch.

**Tests to add** (mirror `tests/test_oanda_live.py`, `tests/test_fx_provider_registry.py`,
`tests/test_fx_data_source_catalog.py`):

- `tests/test_futures_live.py` — `FuturesPriceProvider.fetch_last_prices` parses a **mocked** vendor payload
  into the standard row incl. `open_interest` and `source=="futures"` (monkeypatch `requests`); missing
  `DATABENTO_API_KEY` → `{}` and does not raise; a generated canary token never appears in returned rows or
  captured logs.
- `tests/test_futures_provider_registry.py` — `FUTURES_ENABLED=1` ⇒ `get_polling_provider_names()` includes
  `"futures"` with `supports["asset_classes"]==["futures"]`; with it unset, `"futures"` is absent and
  `poll_prices` remains the fallback; enabling futures keeps `poll_prices` in
  `get_enabled_market_data_job_names()`; the IBKR def's `asset_classes` now contains `"futures"`.
- `tests/test_futures_data_source_catalog.py` — `_default_catalog()` has `futures_data`
  (`source_type=="price_provider"`, `provider_name=="futures"`, `default_enabled is False`,
  `safe_to_auto_enable is False`); `_PROVIDER_TEST_REGISTRY["futures_data"]["handler"]=="_test_futures_connection"`;
  `_test_futures_connection` with no token → non-pass; with a mocked successful `_http_json_probe` → `status=="pass"`
  and the canary token is absent from the result/evidence; after enabling, `inject_into_provider_registry()`
  reports `futures` enabled and `get_desired_ingestion_jobs()` includes `poll_prices`; `/api/data_sources`
  lists `futures_data` and never contains the canary token.

**Validation commands.**

- `python -c "import engine.data.live_prices.futures_live, engine.data.provider_registry, services.data_source_manager, routes.data_sources_routes"` → 0
- `python -c "from engine.data.provider_registry import get_provider_definition; import os; os.environ['FUTURES_ENABLED']='1'; d=get_provider_definition('futures'); assert d and d.supports['asset_classes']==['futures']"` → 0
- `python -m pytest tests/test_futures_live.py tests/test_futures_provider_registry.py tests/test_futures_data_source_catalog.py -q` → 0
- `python -m pytest tests/test_oanda_live.py tests/test_fx_provider_registry.py tests/test_fx_data_source_catalog.py tests/test_provider_registry_safe_jobs.py tests/test_data_source_catalog_metadata.py -q` → 0 (FX-01 regression guard)
- `python tools/syntax_check_workspace.py` → 0; `ruff check .` → 0
- `git diff --stat -- engine/runtime/storage.py engine/data/live_prices/oanda_live.py` → empty (facade + FX-01 adapter untouched)

**Self-audit & NO-GO.** (1) Quote the runtime gates: the `FUTURES_ENABLED` default-off line, the default-off
`futures_data` source, and the missing-credentials fail-closed path — confirm enforcement is in
registry/manager/adapter, not tests. (2) Secret-surface grep over the diff for the vendor token /
`Authorization`; confirm none reach logs/evidence/`meta_json`; cite the canary test. (3) **No-broker-authority
proof:** confirm no order/trade/account-mutation endpoint and no execution adapter were added — read-only
pricing only. (4) Confirm `storage.py` untouched and the raw-bars table uses in-module `CREATE TABLE IF NOT
EXISTS`. (5) **Disclose the live vendor probe is mock-only in sandbox** and that **vendor licensing
(CME non-display/derived-works) is UNVERIFIED → NO-GO to enabling in prod until confirmed.** State **GO** or
**NO-GO** with evidence.

---

### FUT-03 (full) — Roll engine & continuous-series construction  *(correctness keystone)*

**Mission.** Turn FUT-02's raw per-contract bars + open interest into (a) an explicit, OI/volume-based
**roll calendar**, (b) a **ratio-adjusted continuous series** that is returns-correct and never negative,
and (c) a **roll-yield** series — persisted through idempotent `CREATE TABLE IF NOT EXISTS` helpers and
produced by a new **default-off, control-plane-gated** derived-data daemon. This is where futures
correctness lives: **ML returns must never be computed across an unadjusted roll boundary.**

**Prerequisites.** FUT-01 (symbol semantics + roll_method/exchange), FUT-02 (raw per-contract bars + OI in
storage). In the sandbox FUT-02 data is absent — the roll/continuous logic must be unit-testable on
**synthetic in-memory bars** and degrade to empty (never raise) when no data exists.

**Global constraints.** Shared FUT-0x constraints bind. Additionally: **`storage.py` schema untouched** —
new tables are created via `CREATE TABLE IF NOT EXISTS` inside this module's `ensure_*` helper, exactly
like `engine/data/cftc_cot.py::ensure_cot_tables` (`:196`) and
`engine/data/factor_ingestion.py::ensure_macro_vintage_tables` (`:654`). The daemon stays **default-off**
and **control-plane-gated** (mirror `ingest_cftc_cot.py` exactly); **do not flip any global default to on**.

**Adopted methodology (from audit A2 — normative here).** Store **three layers**: raw per-contract bars
(source of truth, untouched), the **roll calendar** (roll chosen by **open-interest crossover with volume
confirmation**), and a **ratio-adjusted** continuous series for return/label math. A back-adjusted view may
be derived for charting **only** and must never feed labels. `roll_yield` = annualized log-slope between
front and next settlement.

**In scope.**

1. **`engine/data/futures_roll.py`** (new; pure functions, no daemon logic):
   - `detect_rolls(bars_by_contract) -> list[RollEvent]` — given `{contract: [bars(ts,oi,volume,close)]}`,
     emit `RollEvent(root, roll_ts_ms, from_contract, to_contract, gap_ratio)` when the deferred
     contract's open interest overtakes the front (volume-confirmed). Deterministic, side-effect free.
   - `build_ratio_adjusted_continuous(bars_by_contract, rolls) -> list[ContBar]` — stitch front-month bars,
     multiplying pre-roll history by the price ratio at each roll so **percentage returns are preserved and
     values stay positive**; tag each bar with `roll_flag`.
   - `compute_roll_yield(front_settle, next_settle, days_between) -> float` — annualized log slope.
   - Never raise on empty/degenerate input → return `[]`/`0.0`.
2. **`ensure_futures_roll_tables(con)`** (in `futures_roll.py` or a small `engine/data/futures_continuous.py`)
   — idempotent `CREATE TABLE IF NOT EXISTS` + `CREATE INDEX IF NOT EXISTS` (copy the shape of
   `cftc_cot.py:196-294`): `futures_roll_calendar(root, roll_ts_ms, from_contract, to_contract, gap_ratio,
   method, ingested_ts_ms, PRIMARY KEY(root, roll_ts_ms))`; `futures_continuous_bars(continuous_symbol,
   ts_ms, adj_method, open, high, low, close, volume, roll_flag, PRIMARY KEY(continuous_symbol, ts_ms,
   adj_method))`; `futures_roll_yield(root, ts_ms, roll_yield, PRIMARY KEY(root, ts_ms))`. **No
   `storage.py` change.**
3. **`engine/data/jobs/ingest_futures_rolls.py`** (new daemon) — copy `ingest_cftc_cot.py` structure
   verbatim and adapt: `JOB_NAME="ingest_futures_rolls"`; `INGEST_ENABLED` from
   `INGEST_FUTURES_ROLLS_ENABLED` default `"0"` (`ingest_cftc_cot.py:40`); `main()` requires
   `ENGINE_SUPERVISED=1` (`:105`), returns early if `not INGEST_ENABLED` (`:110`), then
   `if not manager.is_job_enabled(JOB_NAME, default=False): return` (`:113`); a `_run_once()` that calls
   `ensure_futures_roll_tables`, reads raw bars (degrade to no-op if none), computes rolls/continuous/roll-yield,
   upserts them, and reports via `record_pipeline_status` + `manager.record_job_status` + `put_job_heartbeat`
   under `acquire_job_lock`.
4. **Register the job** in `engine/runtime/job_registry.py::ALLOWED_JOBS` (`:311`) — add, next to
   `ingest_cftc_cot` (`:1077`), a daemon tuple:
   `"ingest_futures_rolls": ("engine/data/jobs/ingest_futures_rolls.py","daemon",None,{"execution":False,
   "schedule":"every 86400s","cadence_seconds":86400})`. **Default-off**; it only runs when an operator
   enables it through the control plane.
5. **Control-plane wiring** — register `ingest_futures_rolls` as a consumer/derived job of the `futures_data`
   source from FUT-02 (operational metadata `storage_tables=("futures_roll_calendar","futures_continuous_bars",
   "futures_roll_yield")`, `safe_to_auto_enable=False`), so enabling the futures source offers the roll job
   without flipping a global default.

**Out of scope.** Feature ids (FUT-05 consumes `futures_roll_yield`/continuous), labels (FUT-06), sizing,
execution. No back-adjusted series in any ML/label path. No `storage.py` schema change.

**Verified anchors (re-grep).** `cftc_cot.py:196` `ensure_cot_tables` (CREATE TABLE/INDEX IF NOT EXISTS
template), `:295` `seed_default_cot_mappings`; `factor_ingestion.py:654` `ensure_macro_vintage_tables`,
`:1683` `sync_macro_factors`; `ingest_cftc_cot.py:36,40,72-100,103-119` (JOB_NAME / INGEST_ENABLED /
_run_once / gated main); `job_registry.py:311` `ALLOWED_JOBS`, `:355` `poll_macro`, `:1077` `ingest_cftc_cot`
(tuple shape `(script, "daemon", category, {config})`).

**Tests to add.**

- `tests/test_futures_roll.py` — synthetic two-contract bars where the deferred OI overtakes the front on a
  known day ⇒ `detect_rolls` returns one `RollEvent` at that ts with correct from/to; `build_ratio_adjusted_continuous`
  yields a series whose **percentage returns equal the per-contract returns** and whose values are **all
  positive** (assert no negative close); a **raw front-month return across the roll boundary differs from the
  continuous-series return** at the same ts (the corruption guard); `compute_roll_yield` sign matches
  contango/backwardation; empty input → `[]`/`0.0`, never raises.
- `tests/test_futures_roll_tables.py` — `ensure_futures_roll_tables(con)` on an in-memory sqlite DB creates
  the three tables and is idempotent on a second call; an insert+select round-trips `roll_yield` as `float`.
- `tests/test_ingest_futures_rolls_gating.py` — with `ENGINE_SUPERVISED` unset, `main()` exits non-zero
  (supervisor required); with `INGEST_FUTURES_ROLLS_ENABLED=0`, `main()` records "disabled by env flag" and
  does not write; assert no live broker path is import-reachable. (Test the helper path directly; only set
  `ENGINE_SUPERVISED=1` if invoking `main`.)

**Validation commands.**

- `python -c "import engine.data.futures_roll, engine.data.jobs.ingest_futures_rolls"` → 0
- `python -c "from engine.runtime.job_registry import ALLOWED_JOBS as J; s=J['ingest_futures_rolls']; assert s[1]=='daemon' and s[3].get('cadence_seconds')==86400"` → 0
- `python -m pytest tests/test_futures_roll.py tests/test_futures_roll_tables.py tests/test_ingest_futures_rolls_gating.py -q` → 0
- `python -m pytest tests/test_cftc_cot_features.py -q` → 0 (adjacent ensure-pattern regression)
- `python tools/syntax_check_workspace.py` → 0; `ruff check .` → 0
- `git diff --stat -- engine/runtime/storage.py engine/runtime/storage_sqlite.py engine/runtime/storage_pg.py` → empty (no schema-facade change; tables are module-created)

**Self-audit & NO-GO.** (1) Confirm the biggest project risk lives here and is contained: prove the
continuous series has **no negative prices** and that **no ML/label path reads an unadjusted roll boundary**
(cite the corruption-guard test). (2) Confirm the daemon is **default-off and control-plane-gated** (quote the
`INGEST_FUTURES_ROLLS_ENABLED` default `"0"` and the `is_job_enabled(..., default=False)` line) and that **no
global default was flipped**. (3) Confirm tables are created via `CREATE TABLE IF NOT EXISTS` in-module and
`storage.py`/backends are untouched (`git diff --stat` empty). (4) Disclose that the **live roll path is
exercised on synthetic data only** (no FUT-02 feed in sandbox) — mark it a GAP, not a pass. State **GO** or
**NO-GO** with evidence.

---

### FUT-04 (full) — Sessions, calendars & hygiene

**Mission.** Model the futures **23×5 Globex clock** as the single source of truth for futures
market-open/closed/settlement/maintenance boundaries, and stop the equity split/dividend hygiene filter
from discarding legitimate futures overnight and roll gaps. This is the futures analog of FX-04's
canonical `fx_clock.py`.

**Prerequisites.** FUT-01 (`session_calendar` id on each contract). Soft: FUT-03 (roll calendar refines
roll-boundary handling; degrade to "all futures gaps allowed" if absent).

**Global constraints.** Shared FUT-0x constraints. The clock module is **stdlib-only** (`datetime` +
`zoneinfo.ZoneInfo("America/Chicago")` — CME settles in CT; `zoneinfo` is confirmed available), no
storage/model/network imports, env-overridable boundaries, and **declares itself the canonical futures
session-boundary module** (FUT-09 execution session derives from it) with the CT↔UTC equivalence stated in
the docstring — exactly as FX-04's `fx_clock.py` does for FX.

**In scope.**

1. **`engine/data/calendar/futures_sessions.py`** (new) — mirror FX-04's `engine/data/prices/fx_clock.py`:
   `futures_market_closed(ts_ms, session_calendar="CME_GLOBEX_24x5") -> bool` (closed during the daily
   maintenance break **16:00–17:00 CT Mon–Thu** and the weekend gap **Fri 16:00 CT → Sun 17:00 CT**);
   `is_maintenance_break(ts_ms)`; `settlement_ts_for_day(ts_ms, session_calendar)` (e.g. equity-index
   **15:15 CT**); `next_session_open_ms(ts_ms)`; `futures_window_spans_closed_gap(start_ms, end_ms)`.
   Boundaries as module constants with env overrides (`FUT_WEEK_CLOSE_HOUR_CT`, `FUT_WEEK_OPEN_HOUR_CT`,
   `FUT_MAINT_START_CT`, `FUT_MAINT_END_CT`); a refreshable holiday set (do not hardcode beyond a documented
   default). Build on real `zoneinfo` DST; fixed-offset only as a logged fallback.
2. **`engine/data/price_hygiene.py`** — make `filter_split_like_price_rows` (`:105-140`) asset-class-aware:
   add `from engine.data.asset_map import asset_class_for_symbol` and, at the early-accept guard (`:121`),
   accept (skip the split filter) when `asset_class_for_symbol(symbol) == "FUTURES"` — futures roll and
   overnight gaps are legitimate, not corporate actions. Equity/crypto behavior at `is_split_like_price_jump`
   (`:26-32`, thresholds `-0.45`/`0.90` at `:14-15`) stays **byte-for-byte unchanged**. (Optional refinement
   once FUT-03 lands: instead of blanket-exempting, suppress only within ±N hours of a roll date.)
3. **`engine/strategy/feature_registry.py`** — the base session flags `base.session_asia/eu/us` (`:101-103`)
   are derived for an equity-ish clock; branch the `_session_flags` computation so `FUTURES` symbols derive
   sessions from `futures_sessions` (Globex), leaving equity/FX/crypto flags unchanged. Additive only; do not
   alter the served schema for non-futures symbols.

**Out of scope.** Order session-gating / live trading-session control (FUT-09 — this is a *data/label* clock,
not an order gate); the **label** roll-boundary skip (FUT-06 consumes this clock); feature math (FUT-05).

**Verified anchors (re-grep).** `price_hygiene.py:14-15,26-32,84,105-140` (accept guard `:121`);
`feature_registry.py:72,101-104`; `engine/data/calendar/` (dir exists; `fmp_earnings.py` only);
`engine/data/time_utils.py:8,17`. **FX precedent:** FX-04's `fx_clock.py` (the canonical-clock template) +
its `tests/test_fx_clock.py`.

**Tests to add.**

- `tests/test_futures_sessions.py` — `futures_market_closed` true at Fri-16:00-CT, in the Sat gap, and in the
  16:00–17:00-CT maintenance break; false intraday; `futures_window_spans_closed_gap` true across the weekend;
  **at least one assertion pinned to a US DST-transition date** proving real `America/Chicago` DST (not a fixed
  offset); env-override of the hour constants.
- `tests/test_futures_price_hygiene.py` — a **+50%** overnight/roll move on a `FUTURES` symbol is **accepted**
  (not flagged) while the same move on an equity symbol is **flagged** (assert via `ASSET_CLASS_MAP_JSON` or a
  futures canonical symbol); equity output is unchanged vs baseline.

**Validation.** `python -c "import engine.data.calendar.futures_sessions, engine.data.price_hygiene"` → 0;
the two test files; `tests/test_price_hygiene*.py` (regression) → 0; `tools/syntax_check_workspace.py`; ruff.

**Self-audit & NO-GO.** Confirm equity hygiene is byte-identical (cite the regression test); the clock is the
declared canonical futures boundary with CT↔UTC equivalence; calendars are refreshable not baked; **NO-GO any
holiday list that cannot be sourced** (TODO + documented default). GO/NO-GO with evidence.

---

### FUT-05 (full) — Features & prediction wiring

**Mission.** Register futures-native features — term-structure slope, carry/roll-yield, basis, time-series
momentum — plus futures-anchored COT, **gated by asset class with train/serve parity**, consuming FUT-03's
continuous + roll-yield rows. The futures analog of FX-03 (which is partially landed: `FX_COT_FEATURE_IDS`,
the `"fx_cot"` group). **No alpha asserted; these are hypotheses for the gates.**

**Prerequisites.** FUT-01 (asset class), FUT-03 (roll-yield + continuous series). Soft: FUT-02 data (absent in
sandbox ⇒ loaders degrade to bounded zeros, never raise).

**Global constraints.** Shared FUT-0x. Preserve `feature_registry.py` train/serve parity: futures ids are
added **only** behind a default-off flag and **only** apply to futures symbols; the equity/FX served schema
must be unchanged. Respect the model-vs-runtime contract — add features, not model order authority.

**In scope.**

1. **Feature loaders** (consume FUT-03's `futures_continuous_bars` / `futures_roll_yield`, never ingest):
   `fut.term_structure_slope` (front/next log-slope), `fut.carry` / `fut.roll_yield`, `fut.basis`
   (spot-vs-future where available), `fut.tsmom_3m` / `fut.tsmom_12m` (sign/scaled past return on the
   continuous series). All bounded/clamped and finite even with no data.
2. **`engine/strategy/feature_registry.py`** — mirror the FX-03 pattern exactly: add `FUT_FEATURE_IDS` and
   `FUTURES_COT_FEATURE_IDS`; add `USE_FUTURES_FEATURES = _env_bool("USE_FUTURES_FEATURES", False)` (copy
   `USE_COT_FEATURES` at `:72,89`); register a `FEATURE_GROUPS["futures"]` block gated on that flag (copy the
   `"fx_cot"`/`"cot"` group wiring at `:522,529,567-568`); include the ids in the served schema assembly
   (`:509`, `:1101-1102`) **only when the flag is on**. Enable COT for futures by mapping the re-anchored
   futures symbols (FUT-02) into the COT feature path.
3. **`engine/strategy/predictor.py`** — confirm futures symbols resolve the futures feature ids via the
   existing `resolve_feature_ids` path and route through the asset-agnostic model families; do **not** change
   model internals or feature contracts (mirror FX-03/FX-04: features + regime context only).

**Out of scope.** Labels/targets (FUT-06); sizing/risk; execution. Do not change equity/FX feature sets or
author roll/continuous (FUT-03). No new model classes.

**Verified anchors (re-grep).** `feature_registry.py:72,89,101-104,247,290,388,419,509,522,529,567-568,1101-1102`
(FX-03 landed: `FX_COT_FEATURE_IDS:388`, `"fx_cot":529`); `predictor.py` asset-class routing /
`resolve_feature_ids`. **FX precedent:** FX-03 (landed) — copy the FX feature-group registration shape.

**Tests to add.**

- `tests/test_futures_feature_registry.py` — with `USE_FUTURES_FEATURES=1` the `fut.*` ids appear in
  `FEATURE_GROUPS["futures"]` and the served schema; with the flag off they are absent and the equity/FX served
  schema is **identical to baseline**; no duplicate feature ids across the registry; all `fut.*` loaders return
  **finite, bounded** values when the underlying data is absent (the sandbox case).
- `tests/test_futures_cot_feature_flow.py` — a re-anchored futures symbol surfaces COT features in a feature
  snapshot when enabled.

**Validation.** import line; the two test files; `tests/test_feature_registry*.py` + a train/serve parity test
→ 0; ruff; syntax check.

**Self-audit & NO-GO.** Confirm **no profitability/alpha is asserted**; equity/FX feature sets and served schema
are unchanged with the flag off; futures features are gated and bounded; data-absent fallback is zeros not
exceptions. GO/NO-GO.

---

### FUT-06 (full) — Labels, targets & regime

**Mission.** Make futures labels correct: forward returns computed on the **ratio-adjusted continuous series**
(never across an unadjusted roll), net-after-cost labels that use **contract-multiplier notional + tick/roll
costs**, and futures-appropriate horizons. The futures analog of FX-04's label-clock correctness.

**Prerequisites.** FUT-01 (multiplier/cost metadata), FUT-03 (continuous series + roll calendar), FUT-04
(roll/session clock). Soft: FUT-05.

**Global constraints.** Shared FUT-0x. Do **not** edit `engine/data/prices/returns.py` (branch at callers, per
FX-04). Net-after-cost label table changes are additive via the existing in-module `CREATE TABLE IF NOT EXISTS`
(no `storage.py` change). Equity/FX label output must be provably unchanged.

**In scope.**

1. **`engine/strategy/labeling.py`** (`label_event`, `:21-65`) — for futures symbols, (a) the caller supplies
   the **ratio-adjusted continuous** price series (FUT-03) rather than raw front-month, and (b) **skip** any
   forward window where `futures_window_spans_closed_gap` / a roll boundary (FUT-03 calendar + FUT-04 clock)
   would measure return across a gap. Non-futures: `compute_return(series, event_ts, h_s*1000)` (`:36`) and the
   `impact_z = ret/vol` math (`:42`) are unchanged. Branch on `asset_class_for_symbol(sym)`.
2. **`engine/strategy/net_after_cost_labels.py`** — at `fill_notional = q * p` (`:522`), use
   `fill_notional = q * p * multiplier` for futures (multiplier from `get_instrument_metadata`); add
   `roll_cost_bps` and `carry_bps` columns to the `CREATE TABLE IF NOT EXISTS {TABLE_NAME}` schema (`:129-155`)
   and populate them from a **futures** cost model (tick-value slippage + two-leg roll cost, audit A6), not the
   equity bps defaults. Non-futures cost decomposition unchanged.
3. **Horizons** — ensure futures use session-correct forward horizons (no RTH assumption); reuse `HORIZONS_S`
   (`:16-19`) but evaluate via the FUT-04 clock for futures.
4. **`engine/strategy/retraining_pipeline.py`** — `_build_outcome_query` (`:120`, `FROM labels … horizon_s IN`)
   needs **no change**: labels remain keyed by `(symbol, horizon_s)`; roll-awareness is already baked into the
   continuous series + the skip. Confirm and document.

**Out of scope.** Sizing/risk (FUT-07); backtest costs/purging (FUT-08); feature ids (FUT-05). No
`engine/data/prices/returns.py` edit; no `storage.py` schema change.

**Verified anchors (re-grep).** `labeling.py:16-19,21,36,42,44-65`; `net_after_cost_labels.py:129,153-155,414,
519-547`; `retraining_pipeline.py:120,136,162`; FUT-03 roll calendar + FUT-04 `futures_sessions`. **FX
precedent:** FX-04 label-clock branch.

**Tests to add.**

- `tests/test_futures_labeling.py` — a futures label's forward return across a roll equals the **continuous-series**
  return (not the raw front-month return); a window spanning a roll/closed gap is **skipped**; an equity symbol
  with identical inputs is **byte-for-byte unchanged**.
- `tests/test_futures_net_after_cost.py` — `fill_notional` for a futures fill uses the multiplier; the net label
  subtracts a multiplier-correct tick/roll cost and carries `roll_cost_bps`; non-futures labels are unchanged;
  the additive columns round-trip on sqlite.

**Validation.** import lines; the two test files; `tests/test_net_after_cost*.py` + `tests/test_labeling*.py`
(regression) → 0; ruff; syntax check; `git diff --stat -- engine/data/prices/returns.py engine/runtime/storage.py`
→ empty.

**Self-audit & NO-GO.** Prove futures returns **never cross an unadjusted roll** (cite the corruption test);
the cost model is tick/roll not equity bps; equity/FX labels unchanged; `returns.py`/`storage.py` untouched; no
profitability asserted. GO/NO-GO.

---

### FUT-07 (full) — Risk & sizing

**Mission.** Replace equity `shares × price` exposure with **`contracts × multiplier × price`** notional,
add an initial/maintenance **margin engine**, integer-contract rounding, and currency-aware notional — so
a 1-lot ES position is risked as ~$300k notional, not its fractional weight. The futures twin of FX-05.
**No profitability claim; this is a safety layer.**

**Prerequisites.** FUT-01 (multiplier, `margin_ref`, `price_ccy`). Soft: FUT-04 (sessions).

**Global constraints.** Shared FUT-0x. The risk engine is **weight-based** (a "weight" = fraction of
capital); the futures change is to make a futures weight reflect **true contract notional** wherever
exposure is summed, and to convert weight→**integer contracts** at the sizing boundary. `margin_ref` from
FUT-01 is a *reference*; FUT-07 owns **enforcement** by reconciling it against a regulatory/broker cap via
`min(...)`. Equity/crypto/FX sizing must be provably unchanged.

**In scope.**

1. **`engine/risk/portfolio_risk_engine.py`** — add `"FUTURES"` to `_DEFAULT_ASSET_CLASS_BUDGETS` (`:135-142`;
   honors `PORTFOLIO_RISK_USE_ASSET_CLASS_BUDGETS` at `:125` and the JSON override at `:126`). Wherever
   exposure/notional is aggregated from weights, scale futures contributions by a per-symbol multiplier
   factor (from `get_instrument_metadata`) so gross/net caps see real notional. Keep `_signed_weight`
   (`:508-511`) semantics; the rescale stage (`:454-468`) stays weight-based.
2. **`engine/strategy/portfolio_risk_gate.py`** — in `_sleeve_gross`/`_sleeve_net` (`:115-143`, which sum
   `abs(weight)` at `:122`) multiply each futures symbol's weight by its multiplier factor so a small-weight,
   high-multiplier contract is not undercounted in sleeve exposure. `asset_class_for_symbol` is already
   called at `:104`.
3. **Weight→contracts + margin engine** — add a pure `weight_to_contracts(weight, capital, multiplier,
   price) -> int` (floor to whole contracts) and an `engine/risk/futures_margin.py` computing required
   initial/maintenance margin per position and capping total contracts so aggregate margin ≤ the asset-class
   budget, using `enforced = min(reference_margin, regulatory_or_broker_margin)`. Reuse the vol-target /
   regime scalars in `engine/strategy/regime_size.py` (`M_LOW`/`M_HIGH` at `:38-40`, `regime_multiplier`
   `:201`, `regime_capital_scale` `:221`) as-is — they act pre-notional.
4. **Currency-aware notional** — convert non-USD `price_ccy` contracts (FUT-01) to the account base currency
   before applying caps (FX rate via existing factor/price reads; degrade to 1.0 + warn if unavailable).

**Out of scope.** Order routing/execution (FUT-09); backtest cost model (FUT-08); features/labels. No model
order authority.

**Verified anchors (re-grep).** `portfolio_risk_engine.py:125-152,454-468,508-511,1066,1203`;
`portfolio_risk_gate.py:104,115-143`; `regime_size.py:38-40,201,221`; `portfolio.py` `to_weight` (`:391,827`);
`get_instrument_metadata` (FUT-01). **FX precedent:** FX-05.

**Tests to add.**

- `tests/test_futures_risk_sizing.py` — `"FUTURES"` present in the budget map; a futures position's
  sleeve/notional reflects the multiplier (a 0.02-weight ES contributes far more notional than a 0.02-weight
  equity); `weight_to_contracts` floors correctly and never returns a fractional/oversized count; equity
  sizing is **byte-for-byte unchanged**.
- `tests/test_futures_margin.py` — the margin engine caps contracts at `min(reference, regulatory)`; total
  margin never exceeds the budget; a non-USD contract is converted before capping.

**Validation.** import lines; the two test files; `tests/test_portfolio_risk*.py` + `tests/test_risk_gate*.py`
(regression) → 0; ruff; syntax check.

**Self-audit & NO-GO.** Confirm equity/crypto/FX sizing unchanged (cite regression); margin **enforcement** is
in runtime (engine), reference vs enforced clearly separated; integer-contract rounding can never oversize.
GO/NO-GO.

---

### FUT-08 (full) — Backtest realism & governance

**Mission.** Make the backtest tell the truth about futures — **roll-aware CV purging**, **tick-value
slippage + two-leg roll cost + point-value P&L** — and route futures strategies through the **existing**
deflated-Sharpe / CPCV / champion-challenger gates so net-of-cost edge is *proven before capital*. The
futures twin of FX-07. **This workstream's output is the live/no-live gate.**

**Prerequisites.** FUT-03 (roll calendar), FUT-06 (net-after-cost labels), FUT-07 (sizing).

**Global constraints.** Shared FUT-0x. Extend the existing gate modules; do **not** build a parallel gate
framework (CLAUDE.md: governance is integrated). Profitability is **proven through these gates, never
asserted** anywhere.

**In scope.**

1. **`engine/backtest/cpcv.py`** — feed roll dates into purging so no train/test split straddles a roll: the
   `CombinatorialPurgedKFold` path already accepts `label_start_times`/`label_end_times` (`:52-71`) and an
   embargo (`_embargo_count` `:105`); expand the embargo window to cover each roll boundary for futures
   symbols (pre/post-roll price scales differ ⇒ leakage). Equity/FX CV behavior unchanged when no roll dates
   are supplied.
2. **`engine/strategy/portfolio_backtest.py`** + **`engine/execution/execution_costs.py`** — futures cost =
   **tick-value slippage + two-leg roll cost + point-value P&L**, not equity bps. The backtest cost env
   (`portfolio_backtest.py:43-45`) gets futures overrides; `estimate_cost_bps` (`execution_costs.py:37-55`,
   defaults `:16-17`) gains a `contract_multiplier`/`tick_value` parameter so the half-spread (`:50`) is
   priced per tick and scaled to bps on true notional; `estimate_almgren_chriss_costs`
   (`portfolio_backtest.py:21,345`) receives multiplier-correct notional.
3. **Gate wiring** — run futures challenger strategies through `deflated_sharpe_ratio`
   (`deflated_sharpe.py:63`, `expected_max_sharpe:46`) and the champion/challenger promotion path with the
   futures cost model active, so promotion requires net-of-cost evidence.

**Out of scope.** Live execution (FUT-09); feature/label authoring (FUT-05/06).

**Verified anchors (re-grep).** `cpcv.py:52-71,105` (+ `CombinatorialPurgedKFold` body); `deflated_sharpe.py:46,63`;
`portfolio_backtest.py:21-22,43-45,339,345`; `execution_costs.py:16-17,37-55`. **FX precedent:** FX-07.

**Tests to add.**

- `tests/test_futures_cpcv_roll_embargo.py` — with a roll date inside a fold, the train set excludes samples
  whose label window straddles the roll (no leakage); without roll dates, splits are identical to baseline.
- `tests/test_futures_backtest_costs.py` — backtest P&L uses point value (`contracts*multiplier*Δprice`) and
  charges tick-value slippage + a roll cost on roll days; `estimate_cost_bps` with a multiplier/tick differs
  from the equity-bps result; `deflated_sharpe_ratio` runs end-to-end on a synthetic futures strategy.

**Validation.** import lines; the two test files; `tests/test_cpcv*.py` + `tests/test_deflated_sharpe*.py` +
`tests/test_portfolio_backtest*.py` (regression) → 0; ruff; syntax check.

**Self-audit & NO-GO.** Prove roll-leakage is purged (cite test); costs are tick/roll/point-value not bps;
equity CV/backtest unchanged when no roll dates supplied; **state explicitly that any "edge" is
gate-conditional and is NOT asserted here.** GO/NO-GO.

---

### FUT-09 (full) — Execution adapter  *(governance-gated; live ships disabled)*

**Mission.** A futures-capable broker route that is **read-only → shadow → paper → governed live**,
**roll-aware** (never trades into first-notice/expiry, never during the maintenance break), and has **no
live order authority until the existing arming preflight + gates pass.** The futures twin of FX-06. **Live
futures order paths ship disabled by default.**

**Prerequisites.** FUT-01..FUT-08 green in shadow/paper. This is the **last** workstream and must not be
enabled live until FUT-08's gates are green and governance has signed off.

**Global constraints.** Shared FUT-0x. **Reuse the existing execution-safety machinery — do not weaken it.**
`engine/execution/execution_mode.py` already enforces modes paper/shadow/live (`:11-13`), requires
`mode=='live' AND armed=1` (`:16`), and treats truthy `DISABLE_LIVE_EXECUTION` as an absolute block
(`:17,29-31`) via `live_execution_disabled` + `assert_live_execution_arming_preflight` /
`live_trading_preflight` (`:37`). `broker_router.py` adds `live_broker_mode_boundary_block` (`:36`) and the
failover/reconciliation gate. Futures must flow **through** these, not around them.

**In scope.**

1. **`engine/execution/broker_ibkr_gateway.py`** — add `Future()`/`ContFuture()` contract construction
   (alongside the existing equity path) keyed off FUT-01 metadata (root, exchange, expiry), reusing the
   existing order-ref validation (`sanitize/validate_ibkr_order_ref` `:286-316`,
   `_place_order_with_order_ref` `:356`) and `_set_order_total_quantity` (`:253`). Order quantity = **integer
   contracts** from FUT-07. **Recommend extending IBKR** (gateway already connects) over a net-new
   Tradovate/Rithmic adapter.
2. **Roll-aware order gating** — block (or convert to a roll) any order within the first-notice/expiry window
   (FUT-01 `expiry_rule` + FUT-03 roll calendar) and any order during the maintenance break / closed session
   (FUT-04 `futures_sessions`). Never trade into delivery on physically-settled contracts.
3. **Mode plumbing** — register the futures route in `broker_router.py` so it inherits failover +
   `live_broker_mode_boundary_block`; futures **live** is reachable **only** via the existing
   `assert_live_execution_arming_preflight` path with `DISABLE_LIVE_EXECUTION` unset and `armed=1`. Default
   config: shadow/paper only. `execution_policy_engine.py` qty scaling (`:274-351`) is multiplier-agnostic
   (qty already in contracts) — **no change needed**.

**Out of scope.** Granting models order authority outside the gates; flipping any live default on; sizing
(FUT-07) or cost (FUT-08) math. No new execution framework.

**Verified anchors (re-grep).** `execution_mode.py:11-17,29-31,37,53`; `broker_ibkr_gateway.py:253,286-356`;
`broker_router.py:36-42`; `execution_policy_engine.py:274-351`; `engine/runtime/live_execution_control.py`,
`live_trading_preflight.py`. **FX precedent:** FX-06.

**Tests to add.**

- `tests/test_futures_broker_order_build.py` — builds a valid `Future()` order in **sim/paper only** with
  integer contracts and a valid order ref; canary credentials never appear in logs/payloads.
- `tests/test_futures_roll_window_block.py` — an order inside the first-notice/expiry window or during the
  maintenance break is blocked; outside it is allowed.
- `tests/test_futures_live_disabled.py` — with `DISABLE_LIVE_EXECUTION` truthy or `armed=0`, **no live order
  path is reachable**; the futures route respects `assert_live_execution_arming_preflight` exactly like
  equities.

**Validation.** import lines; the three test files; `tests/test_execution_mode*.py` + `tests/test_broker_router*.py`
(regression) → 0; ruff; syntax check.

**Self-audit & NO-GO.** Prove **no live order/cancel/replace/flatten is reachable** without the existing
arming preflight (cite `test_futures_live_disabled`); roll/session blocking fires; no existing safety gate was
weakened. **NO-GO to enabling live until FUT-08 gates are green and governance signs off** — this prompt
delivers the *capability* gated off, not live trading. GO/NO-GO on the gated capability.

---

### FUT-10 (full) — UI surfacing  *(reuse-first, read-only)*

**Mission.** Surface futures read-only in the operator UI — feed health, roll calendar, term-structure
curve, COT positioning, margin/exposure-by-contract — with minimal new code, mirroring FX-08 and the
existing dashboard conventions. **No order-entry UI.**

**Prerequisites.** FUT-02/03 data; FUT-07 margin. Can start in parallel once data exists.

**Global constraints.** Shared FUT-0x. Reuse the existing screen/panel/loader machinery; do not build a new
UI framework. Read-only; never render or accept the vendor token; apply freshness/confidence/lineage/
shadow-only labels per `UI_WORLD_CLASS_DEEP_DIVE_PROMPTS.md`.

**In scope.**

1. **Control-plane panel** — the `futures_data` source (FUT-02) already auto-surfaces in
   `ui/data_sources.{js,html}` via the generic dispatch (**zero edits**).
2. **Read-only API** — add a handler in `dashboard_server.py` (mirror an existing `api_get_*` handler and the
   `ROUTE_SPECS` registration near `:2297`) e.g. `GET /api/data/futures/rolls` returning the roll calendar,
   term-structure curve, COT positioning, and margin/exposure-by-contract from FUT-03/07 tables.
3. **Panel** — `ui/futures_panel.js` following the `ui/data_health.js` fetch→normalize→render pattern; add a
   `loadFuturesPanel()` task to `buildScreenRefreshTasks` under the `data` screen (`ui/dashboard.js:1444-1446,
   1523-1524`, where `loadDataHealthScreen()` is registered) and, if a card allowlist is needed, a
   `futuresPanel` entry in `ui/view_router.js` `PERSONA_PANEL_ALLOWLISTS` (`:22-24`; `data` screen is already
   allowed for operations/expert at `:16-19`). Reuse `ui/symbol_context.mjs` for per-contract selection.

**Out of scope.** Order entry / trade controls; anything beyond read-only status/analytics; new screens or
personas unless a test requires it.

**Verified anchors (re-grep).** `ui/dashboard.js:120,1444-1446,1523-1524`; `ui/view_router.js:16-19,22-24`;
`ui/data_health.js` (template); `ui/symbol_context.mjs`; `dashboard_server.py:2297-2298` (+ `api_get_*` pattern,
`ROUTE_SPECS` imports `:2608`). **FX precedent:** FX-08 + `UI_SURFACING_DEEP_DIVE_PROMPTS.md` /
`UI_WORLD_CLASS_DEEP_DIVE_PROMPTS.md`.

**Tests to add.**

- `tests/test_futures_dashboard_api.py` — the new endpoint returns roll/curve/COT/margin shape and **never
  contains the vendor token**; an empty-data case returns a bounded payload, not an error.
- (JS) a panel render smoke test from a mocked endpoint; existing screens unaffected.

**Validation.** `python -c "import dashboard_server"` → 0; the API test; confirm `data_sources` panel needs no
edit; ruff; syntax check; UI lint if configured.

**Self-audit & NO-GO.** Confirm read-only (no order path), reuse-first (one endpoint + one panel), no token in
any payload, existing screens unchanged. GO/NO-GO.

---

## Definition of Done (this deep dive)

- [x] Part A delivered with primary-source citations + access date 2026-06-23; vendor pricing, exact
      fees, and unconfirmed specs explicitly marked **UNVERIFIED / NO-GO**.
- [x] Part B delivered with `file:line` anchors for all 10 stages, each tagged Reuse/Branch/New with the
      lowest-blast-radius path and its FX precedent.
- [x] C1 blueprint + C2 `FUT-01…FUT-10` sections delivered, dependency-ordered, in the FX prompt's
      structure, implementable.
- [x] Claims tagged `[REPO]`/`[WEB]`/`[ASSUMPTION]`; assumptions minimized and risk-flagged.
- [x] Profitability is designed-and-gated, never asserted; the gate chain is explicit.
- [x] Zero runtime/code changes; no live order paths touched; no secrets emitted.
- [x] Written to `docs/handoff/deep_dive_prompts/FUTURES_ENABLEMENT_DEEP_DIVE_PROMPTS.md`.

## Self-audit & NO-GO

1. **Files read / key anchors:** `provider_registry.py`, `oanda_live.py` (existence), `cftc_cot.py`,
   `asset_map.py`, `universe.py`, `0001_baseline.py` + migrations dir, `storage_sqlite.py`, `storage.py`,
   `feature_registry.py`, `price_hygiene.py`, `labeling.py`, `net_after_cost_labels.py`,
   `portfolio_risk_engine.py`, `portfolio_risk_gate.py`, `regime_size.py`, `cpcv.py`,
   `deflated_sharpe.py`, `portfolio_backtest.py`, `execution_costs.py`, `execution_liquidity_model.py`,
   `data_source_manager.py`, `job_registry.py`, `routes/data_sources_routes.py`, `ui/*`. Anchors cited inline.
2. **Primary-sourced vs UNVERIFIED:** Continuous-contract methods, session hours, SPAN initial-vs-
   maintenance relationship, TSMOM/Carry references, and contract *structure* are primary-sourced.
   **UNVERIFIED → NO-GO at build time:** exact vendor pricing/licensing tiers (CME non-display/derived-
   works scope), exact per-product exchange/clearing fees (CME changed schedules 2026-04-01), and any
   contract spec not reconfirmed at the exchange's contractSpecs page. Seed none of these blind.
3. **Biggest technical risk — DEFENDED:** **continuous-contract / roll correctness (FUT-03).** A wrong
   roll or a back-adjusted series used for return labels silently corrupts every downstream label,
   vol-target, backtest P&L, and gate decision — and it fails *quietly* (no exception, just wrong
   numbers). Mitigation: store raw + roll calendar + ratio-adjusted continuous separately; never compute
   ML returns across an unadjusted boundary; roll-aware CPCV embargo (FUT-08). This is why FUT-01→02→03
   is the mandatory keystone block.
4. **Sandbox-only / not executed:** no Postgres (the `0071` migration *apply* is unproven here — covered
   by module-import + migrator-contract tests); no live vendor creds (futures price + connection probes
   are mock-only); no internet-dependent tests; FRED/CME endpoints not hit live. All flagged in-place.
5. **Verdict: GO** — the blueprint and `FUT-01…FUT-10` decomposition are implementation-ready, with the
   keystone (FUT-01→02→03) and the roll-correctness risk explicitly called out, and every external
   dependency that could not be confirmed in-sandbox marked UNVERIFIED/NO-GO rather than assumed.
