# FX Enablement Research Dossier

> NETWORK MODE: ONLINE — citations fetched 2026-06-23 (see §9).

This dossier is the research-only substrate for FX-01 through FX-08. It writes no runtime code, persists no machine-readable artifact, and changes no enforcement behavior. All FX trading must graduate through the existing governance path in shadow/paper first; it may not bypass the model-vs-runtime contract where models propose intent and runtime owns safety gates, the train/serve feature parity catalog in `engine/strategy/feature_registry.py`, or the champion/challenger promotion path.

## §1 Execution venue / broker API comparison

### OANDA v20

OANDA is the recommended primary read-only/practice-first FX integration target. Its v20 API has REST and streaming hosts split by environment: practice REST `https://api-fxpractice.oanda.com`, live REST `https://api-fxtrade.oanda.com`, practice streaming `https://stream-fxpractice.oanda.com/`, and live streaming `https://stream-fxtrade.oanda.com/` [S2]. The API exposes market data and history, including real-time rates for tradeable pairs and historical pricing dating back to 2005 [S1]. Pricing supports both REST account pricing and a pricing stream, with the stream served from the streaming URLs and throttled to at most one price per 250 ms window per requested instrument [S3].

OANDA instrument names are base and quote currency separated by underscore, e.g. `EUR_USD`; the primitive schema explicitly defines `InstrumentName` that way [S4]. Instrument metadata includes `pipLocation`, display precision, trade units precision, minimum trade size, maximum order units, `marginRate`, commission, and financing fields with long/short rates and financing days [S4]. OANDA order schemas use `units` for position size; positive units create a long order and negative units create a short order [S5]. That fact is included only so later paper validation can model sign semantics correctly. FX-00 does not authorize any live order/cancel/replace/flatten path and does not define broker order payloads.

Recommended OANDA read-only/practice configuration shape:

```text
environment: practice
instrument: EUR_USD
market_data: GET /v3/accounts/{accountID}/pricing?instruments=EUR_USD
instrument_metadata: GET /v3/accounts/{accountID}/instruments
credentials:
  OANDA_ACCESS_TOKEN=<redacted-do-not-commit>
```

`OANDA_ACCESS_TOKEN` is the canonical token env-var name already registered in `services/data_source_manager.py` `_BASE_CREDENTIAL_RUNTIME_ENV_KEYS` around line 118, with `OANDA_API_KEY` also present as a legacy/alternate name around line 119. FX-06 must not introduce a divergent `OANDA_API_TOKEN`.

### IBKR Forex

IBKR is the recommended fallback broker path where the deployment already operates TWS or IB Gateway. IBKR's contract documentation specifies FX pairs as `secType="CASH"`, `exchange="IDEALPRO"` for true forex trading, `symbol` as the target currency, and `currency` as the base currency; its example uses `symbol="EUR"`, `secType="CASH"`, `exchange="IDEALPRO"`, `currency="GBP"` [S8]. IBKR's order documentation states that Forex orders can be placed in the denomination of the second currency in the pair using the `cashQty` field, and it explicitly warns that paper accounts are simulated and not indicative of real-world trading conditions [S9]. Use those facts only to design later paper simulation and sizing tests under FX-05/FX-06 governance.

Recommended IBKR read-only metadata shape:

```text
market_data_contract:
  secType: CASH
  exchange: IDEALPRO
  symbol: EUR
  currency: USD
account_mode: paper or read-only live metadata
credentials:
  IBKR_HOST, IBKR_PORT, IBKR_USERNAME, IBKR_PASSWORD, IBKR_CLIENT_ID
```

`services/data_source_manager.py` already registers `IBKR_HOST`, `IBKR_PORT`, `IBKR_USERNAME`, `IBKR_PASSWORD`, and `IBKR_CLIENT_ID` in `_BASE_CREDENTIAL_RUNTIME_ENV_KEYS` around lines 111-115. There is no `IBKR_API_KEY`; downstream work must not add one without a reviewed credential-design change.

### Alternatives and exclusion

FXCM/ForexConnect is a viable alternative adapter candidate, not a first choice. The ForexConnect guide describes an API for trading Forex and CFD instruments and covers common API usage [S27]. It is useful as a vendor comparison and potential future fallback, but the repo already carries OANDA/IBKR credential names and broker-adjacent control-plane concepts, so FXCM should not displace OANDA/IBKR in the first pass.

Alpaca has no retail FX. Alpaca's own docs say its trading account supports listed U.S. stocks and select cryptocurrencies, and its assets endpoint asset classes are `us_equity` and `crypto`; its local-currency API explicitly warns that local currency trading is not forex trading and data should not be repurposed for that use [S10]. Alpaca should remain out of FX execution scope.

## §2 Price / history data (live + historical bid/ask)

OANDA is the recommended live price source for the initial implementation because it combines account-specific pricing, bid/ask candles, practice/live environment separation, and broker-aligned instrument metadata. OANDA candle schemas include bid, ask, and midpoint candle components when requested [S4]. The OANDA pricing stream is useful for later streaming support, but FX-01 should start with read-only polling or paper/practice streaming, because runtime execution authority remains out of scope until governance is implemented.

Polygon/Massive is a strong secondary/data-only source. Its forex aggregate endpoint uses `/v2/aggs/ticker/{forexTicker}/range/...`; docs show a `C:EURUSD` ticker and state forex aggregates are generated from quoted bid/ask prices rather than executed trades [S11]. Its forex quotes endpoint returns historical best bid/offer records with bid/ask prices, exchange identifiers, and timestamps [S12]. This is suitable for cross-vendor validation and historical bid/ask backfills where licensing permits it.

CCXT is not a clean spot-FX source. CCXT describes itself as a unified cryptocurrency trading library over crypto exchanges [S13]. The repo can keep CCXT for crypto, but FX prompts should not treat CCXT as the primary spot-FX source.

Historical bid/ask candidates:

| Source | Coverage | Bid/ask fidelity | Licensing note | Recommendation |
| --- | --- | --- | --- | --- |
| OANDA candles/prices | Broker-aligned FX history; OANDA states historical pricing back to 2005 [S1] | Bid, ask, and midpoint candles are supported through pricing components [S4] | Broker account/API terms apply | Primary broker-aligned history |
| Polygon/Massive quotes | Historical forex BBO endpoint [S12] | Explicit bid/ask fields | Paid plan needed for real-time/all-history quote access [S12] | Secondary validation/backfill |
| TrueFX | Major-pair top-of-book tick data with fractional-pip spreads in millisecond detail [S14] | Tick-by-tick top of book | Registration/licensing required; current marketing also includes paid professional data [S14] | Good historical tick archive if license approved |
| HistData | M1 bars plus one-second bid/ask tick formats; FAQ says Generic ASCII tick data includes ask and bid for spread [S15] | Bid bars and ask in Generic ASCII tick format | Free-data terms must be checked before production use | Useful research archive, not primary |
| Dukascopy | Historical data export advertises tick-to-monthly timeframes [S16] | Commonly used for bid/ask tick research; validate exact file fields before use | Broker/site terms apply | Optional cross-check archive |

FX runs on a 24/5 clock. OANDA's U.S. hours page states FX is available Sunday 17:05 through Friday 16:59 New York time with a daily six-minute break from 16:59 to 17:05 [S6]. For system design, use one canonical session clock based on the 17:00 ET FX day boundary. ET to UTC equivalence changes with daylight saving time: 17:00 ET is 22:00 UTC during Eastern Standard Time and 21:00 UTC during Eastern Daylight Time. FX-02 should own the canonical session-boundary accessor, and labels, execution session gates, weekend-gap handling, and UI should derive from that source of truth.

Weekend gap policy: bars should not forward-fill across the Friday close to Sunday open as if continuous liquidity existed. Build explicit session gap markers, close the Friday session at the last valid quote before the daily/weekly break, reopen from the first Sunday quote, and exclude the closed interval from intraday volatility, spread, and slippage estimates unless the strategy explicitly trades weekend-gap risk in simulation.

## §3 Macro / alpha data

FX-01 should ingest raw macro series through the existing PIT path in `engine/data/factor_ingestion.py`, not through an ad-hoc FX fetcher. That module documents and implements leakage-safe `(asof_ts, effective_ts, version)` storage near the top of the file and carries FRED/ALFRED plumbing through `_FRED_OBSERVATIONS_URL`, `_ALFRED_DOWNLOAD_URL`, and `_ALFRED_VINTAGE_RE` around lines 48-53. FX-01 ingests raw series and raw bars only. FX-03's loaders compute per-pair rate-differential, carry, DXY, cross-pair correlation, and TSMOM/trend transforms from those raw rows; do not describe this as a resolver that may or may not exist.

Recommended FRED inputs:

| Use | FRED series | Fact verified online | Downstream transform owner |
| --- | --- | --- | --- |
| U.S. overnight/policy proxy | `DFF` | Effective Federal Funds Rate, daily [S17] | FX-03 computes pair differentials vs foreign short rates |
| U.S. target-range upper limit | `DFEDTARU` | Federal Funds Target Range - Upper Limit, daily 7-day effective data [S17] | FX-03 carry/rate state |
| Euro policy/cash proxy | `ECBDFR` | ECB Deposit Facility Rate for Euro Area [S17] | FX-03 `EUR_USD` differential |
| U.K. overnight proxy | `IUDSOIA` | Daily Sterling Overnight Index Average (SONIA) [S17] | FX-03 `GBP_USD` differential |
| Japan overnight proxy | `IRSTCI01JPM156N` | Japan call money/interbank rate, monthly [S17] | FX-03 `USD_JPY` differential; use cadence-aware lagging |
| U.S. real yield | `DFII10` | 10-year Treasury inflation-indexed constant maturity yield [S17] | FX-03 dollar real-yield feature |
| Broad dollar | `DTWEXBGS` | Nominal Broad U.S. Dollar Index, daily [S17] | FX-03 DXY/broad-dollar transform |

The initial major-pair differential matrix should cover `EUR_USD`, `USD_JPY`, `GBP_USD`, `AUD_USD`, `USD_CAD`, `USD_CHF`, and `NZD_USD`. FX-01 can start with verified U.S., EUR, GBP, and Japan series plus raw FRED placeholders for Canada, Switzerland, Australia, and New Zealand only after each series resolves through the FRED graph endpoint. FX-03 must be cadence-aware: daily U.S./EUR/GBP series and monthly Japan data cannot be aligned without point-in-time lagging and stale-value flags.

CFTC COT FX contract set for FX-01 to add later using the existing `CotContractSpec` shape in `engine/data/cftc_cot.py` around line 55:

| Contract | CFTC `market_name_contains` | Suggested symbols | Ownership |
| --- | --- | --- | --- |
| `6E` | `EURO FX` | `("EURUSD", "6E")` | Already partially present as `("FXE", "6E")`; FX-01 may normalize after FX-02 symbols |
| `6J` | `JAPANESE YEN` | `("USDJPY", "6J")` | FX-01 only |
| `6B` | `BRITISH POUND` | `("GBPUSD", "6B")` | FX-01 only |
| `6A` | `AUSTRALIAN DOLLAR` | `("AUDUSD", "6A")` | FX-01 only |
| `6C` | `CANADIAN DOLLAR` | `("USDCAD", "6C")` | FX-01 only |
| `6S` | `SWISS FRANC` | `("USDCHF", "6S")` | FX-01 only |
| `6N` | `NEW ZEALAND DOLLAR` | `("NZDUSD", "6N")` | FX-01 only; verify current CFTC availability before code because the 2026 current CFTC financial report fetched for this dossier did not contain a New Zealand dollar row |

The current CFTC financial futures report verifies the CME strings for Canadian dollar, Swiss franc, British pound, Japanese yen, Euro FX, and Australian dollar [S19]. The CFTC COT overview documents that COT reports are weekly position reports [S20]. FX-00 must not edit `DEFAULT_COT_CONTRACT_SPECS`; FX-01 alone owns COT contract-spec authoring. FX-03 only surfaces `fx.cot_*` feature ids and must not also author COT specs.

Economic-calendar source: Trading Economics Calendar API is the recommended first vendor to evaluate. Its docs describe a nearly real-time economic calendar updated 24 hours a day, with actual values from official sources, previous/revised values, survey consensus, and fields such as `Date`, `Country`, `Category`, `Event`, `Importance`, `Forecast`, `TEForecast`, `Source`, and `SourceURL` [S18]. FX-01 must ingest and persist FOMC, ECB, BoJ, NFP, and CPI events into a decision-time-safe calendar/event store; otherwise FX-03 `fx.event_*` window features are structurally dead `0.0`.

## §4 FX alpha factors with consensus citations

| Factor | Consensus and net-of-cost caveat | Computable feature | Feature-registry extension |
| --- | --- | --- | --- |
| Time-series momentum / trend | Currency momentum has documented cross-sectional excess returns, but Menkhoff, Sarno, Schmeling, and Schrimpf find transaction costs partially explain returns and limits to arbitrage prevent easy exploitation [S21]. Treat TSMOM as plausible but not assumed profitable. Short lookbacks and high-turnover rules fail OOS net of spread. | `fx.tsmom_63d_z`: pair log-return over 63 trading days, volatility-scaled, using bid/ask aware close from FX-01 bars. | FX-03 computes in loader, then registers a shadow-stage FX group in `FEATURE_GROUPS` around line 456. |
| Carry / rate differential | Lustig, Roussanov, and Verdelhan show carry trade returns persist after transaction costs but are compensation for dollar and carry risk factors, not free alpha [S22]. Profitable means net of spread and swap/carry. | `fx.rate_diff_1d`: domestic short-rate proxy minus foreign short-rate proxy, aligned PIT; `fx.carry_after_swap_est`: rate differential less broker swap estimate. | FX-03 computes from raw FRED/OANDA financing rows; FX-05/FX-07 consume costs. |
| Value / PPP | AQR's value/momentum work finds value and momentum premia across asset classes including currencies, but PPP/value is slow-moving and vulnerable to long drawdowns [S23]. High-turnover PPP variants are likely to fail net of costs. | `fx.ppp_deviation_z`: real exchange-rate or CPI-relative deviation from rolling 5-year mean, z-scored. | FX-03 computes after FX-01 adds raw CPI/PPP inputs; start shadow-only. |
| Positioning (COT) | CFTC positioning is a useful crowding/sentiment input, not standalone alpha. Current repo already has `COT_FEATURE_IDS` around line 34 and only the `6E` FX spec around line 74. | `fx.cot_noncomm_net_z`: non-commercial net positioning z-score by FX future, mapped to the canonical FX pair. | FX-01 adds missing COT specs; FX-03 maps existing COT outputs into FX feature ids. |
| Dollar / risk on/off beta | Lustig et al. identify dollar and carry risk factors in currency returns [S22]. Broad-dollar and risk beta features are risk controls as much as alpha; raw risk on/off beta often fails OOS if not regime-gated and cost-aware. | `fx.usd_broad_beta_126d`: rolling beta of pair returns to `DTWEXBGS`; optional VIX/risk beta after PIT inputs exist. | FX-03 computes from FRED broad-dollar and price rows; promotion gates decide usefulness. |

FX factors must enter through existing feature contracts. `engine/strategy/feature_registry.py` has `PRICE_FEATURE_IDS` around line 328, `FEATURE_GROUPS` around line 456, `FEATURE_STAGE_SHADOW = "shadow"` around line 504, and `FEATURE_STAGE_LIVE = "live"` around line 505. New FX groups should start shadow-stage. `engine/strategy/conformal.py` already maps `FX`, `FOREX`, and `CURRENCY` to `asset:FX` around line 95, so FX is recognized as an asset bucket rather than net-new runtime ontology.

## §5 FX mechanics math

Quote convention: an FX pair is base/quote. `EUR_USD` means one euro priced in U.S. dollars. Direct/indirect depends on account home currency; for a USD account, `EUR_USD`, `GBP_USD`, `AUD_USD`, and `NZD_USD` are direct USD quotes, while `USD_JPY`, `USD_CAD`, and `USD_CHF` are USD-base pairs.

Pip convention: one pip is `0.0001` for most major pairs and `0.01` for JPY-quoted spot pairs. OANDA's API expresses the same concept generically as `pipLocation`, where a pip decimal position is `10 ^ pipLocation`, e.g. `-4` maps to `0.0001` [S4]. FX-02 should store pip size from instrument metadata and not let downstream prompts re-derive it from string suffixes.

Pip value formula:

```text
pip_value_in_quote = units * pip_size
pip_value_in_account = pip_value_in_quote * quote_to_account_fx
```

For a standard `100,000` unit `EUR_USD` lot and pip size `0.0001`, pip value is `100,000 * 0.0001 = 10 USD` before account-currency conversion. OANDA's hours table lists one standard lot equivalent for FX as `100,000` units [S6]. OANDA's micro-lots help page lists standard, mini, and micro lots as `100,000`, `10,000`, and `1,000` units respectively, and notes that OANDA unit sizing does not require rounding to lot, mini-lot, or micro-lot boundaries [S28]. The implementation should not hard-code lot sizes as broker limits because OANDA instrument metadata also exposes minimum trade size and trade-unit precision [S4].

Notional and margin:

```text
notional_quote = units * spot_price
required_margin = notional_quote / leverage
effective_leverage = notional_quote / account_equity
```

Runtime sizing must use the stricter of broker instrument `marginRate`, account/regulatory cap, and internal risk cap. Models must not choose leverage directly.

Swap/rollover: OANDA states that positions open at 5 p.m. ET are held overnight and subject to a financing charge or credit that reflects the interest differential and admin fee [S7]. OANDA also states FX trades are typically settled T+2, and a position held Wednesday at 5 p.m. typically receives three days of funding because settlement rolls from Friday to Monday [S7]. CLS separately describes T+2 as the convention for most CLSSettlement FX instructions [S26]. FX backtests therefore need both spread and swap/carry costs.

Backtest cost model:

```text
entry_cost = half_spread_pips * pip_value_in_account + slippage_pips * pip_value_in_account
exit_cost = half_spread_pips * pip_value_in_account + slippage_pips * pip_value_in_account
daily_roll_cost = broker_long_or_short_swap_rate * position_value * days_charged / 365
net_pnl = gross_pnl - entry_cost - exit_cost - daily_roll_cost - commissions
```

Cost-table lookups must key off the canonical FX-02 stored symbol form flowing through symbols table -> features -> risk -> costs -> UI. FX-05, FX-06, and FX-07 must normalize via FX-02's accessor rather than re-deriving base/quote, because `EUR_USD` versus `EURUSD` mismatches will silently miss cost and cap rows. "Profitable" means net of spread in pips plus swap/carry, proven through the existing statistical/backtest gates and champion/challenger promotion path; FX-00 does not invent a parallel proof system.

## §6 Regulatory / leverage caps as hard sizing limits

U.S. retail FX caps: NFA's forex guide specifies security deposits of 2% for major currencies and 5% for other currencies, with the higher percentage required when a pair combines currencies with different deposit requirements [S24]. This corresponds to maximum leverage of 50:1 on major pairs and 20:1 on non-major/minor pairs. The same guide states FDMs may not carry offsetting positions in a customer account and must offset positions first-in, first-out [S24]. Runtime must treat FIFO and no-hedging as hard execution/risk constraints, not model preferences.

ESMA CFD caps: ESMA's adopted CFD measures restrict retail-client leverage on opening positions to 30:1 for major currency pairs and 20:1 for non-major currency pairs, gold, and major indices [S25]. If the deployment can serve EU/UK-style retail CFD accounts, FX-05 must apply the applicable jurisdictional cap at runtime.

These numbers are seed values for FX-05's runtime cap table. FX-00 persists no machine-readable table. FX-05 must hand-copy the values from this section into code with a comment/test referencing `docs/handoff/research/FX_ENABLEMENT_RESEARCH.md` §6.

| Jurisdiction | Major pair cap | Non-major/minor cap | Other hard rules | Runtime owner |
| --- | ---: | ---: | --- | --- |
| U.S. NFA/CFTC retail FX | 50:1 | 20:1 | FIFO, no simultaneous offsetting positions | FX-05 sizing + FX-06 execution gates |
| ESMA retail CFDs | 30:1 | 20:1 | Margin close-out and negative-balance protections are product-level requirements | FX-05 sizing with jurisdiction profile |

## §7 Decision matrix

| Candidate | Type | Coverage | Bid/ask fidelity | Cost transparency | API ergonomics | Paper/practice support | Licensing | Decision |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| OANDA v20 | Broker + data | Major/minor spot FX, account-specific instruments | High: pricing/candles expose bid/ask and instrument metadata [S3][S4] | High: financing fields and public financing docs [S4][S7] | High: REST + stream, explicit practice/live hosts [S2] | Strong practice environment [S2] | Broker/API terms | Chosen primary |
| IBKR Forex | Broker + data | Broad FX via TWS/Gateway | Medium/high: strong broker data, but gateway/session complexity | Medium: margin/financing depends on account/product | Medium: robust but operationally heavier | Paper accounts, with simulation caveat [S9] | IBKR account/API terms | Fallback broker |
| FXCM ForexConnect | Broker/API alternative | Forex + CFDs [S27] | Medium, broker-specific | Medium | Medium, additional SDK dependency | Demo/paper availability to verify | Vendor terms | Future alternative only |
| Polygon/Massive | Data only | Forex aggregates, snapshots, quotes [S11][S12] | High for BBO quotes; aggregates are quote-derived | High plan transparency in docs | High REST ergonomics | Not a broker | Paid plan for real-time/all-history quotes | Secondary data/backfill |
| TrueFX | Historical/data | Major-pair historical top-of-book ticks [S14] | High top-of-book tick detail | Medium: registration/plan dependent | Medium | Not a broker | Must approve license | Historical tick archive |
| HistData | Historical/data | FX M1 and one-second bid/ask tick formats [S15] | Medium/high if Generic ASCII bid/ask used | Medium | Low/medium manual files | Not a broker | Must approve license | Research archive |
| CCXT | Crypto exchange API | Crypto exchanges, not spot FX [S13] | Not applicable for spot FX | Not applicable | High for crypto | Exchange-dependent | Exchange/API terms | Do not use for spot FX |
| Alpaca | Broker/API | U.S. stocks/select crypto; no retail FX [S10] | Not applicable | Not applicable | High for equities/crypto | Paper for supported products | Alpaca terms | Exclude from FX |

## §8 One-page final recommendation

Primary broker/API: OANDA v20, read-only market data and instrument metadata first, then practice/paper-only simulated order construction after FX-05/FX-06 governance exists. Fallback broker: IBKR Forex through TWS/IB Gateway for deployments that already maintain IBKR infrastructure. Do not route retail FX through Alpaca.

Live price source: OANDA account pricing/candles. Secondary validation/backfill source: Polygon/Massive forex aggregates and BBO quotes. Historical bid/ask archive: TrueFX first if licensing is approved, with HistData/Dukascopy as research cross-checks.

Macro/calendar sources: FRED/ALFRED through `engine/data/factor_ingestion.py` for `DFF`, `DFEDTARU`, `ECBDFR`, `IUDSOIA`, `IRSTCI01JPM156N`, `DFII10`, and `DTWEXBGS`. Trading Economics is the first economic-calendar API to evaluate for FOMC, ECB, BoJ, NFP, and CPI timestamps. FX-01 owns raw series/bar/event ingestion and persistence; FX-03 owns per-pair rate-differential, carry, DXY/broad-dollar, cross-pair correlation, TSMOM/trend, and event-window transforms.

Initial universe: `EUR_USD`, `USD_JPY`, `GBP_USD`, `AUD_USD`, `USD_CAD`, `USD_CHF`, and `NZD_USD`. These are the liquid G10 majors needed to cover USD, EUR, JPY, GBP, AUD, CAD, CHF, and NZD while keeping the first implementation small enough for cost, leverage, session, and symbol semantics to be tested thoroughly.

Initial factor set: carry/rate differential, broker swap/carry estimate, TSMOM/trend, broad-dollar beta, cross-pair correlation, COT positioning, and slow value/PPP deviation. Every factor starts shadow-stage in `FEATURE_GROUPS` and must prove incremental net edge through existing statistical/backtest and champion/challenger gates before promotion.

Cost model: spread in pips plus slippage plus swap/carry/financing, with Wednesday triple-swap and T+2 settlement behavior represented. Profitability must be net of those costs and proven, never asserted.

Ownership map: FX-01 ingests prices, raw macro, COT raw rows/specs, and event calendar; FX-02 owns canonical FX symbol/instrument/session semantics; FX-03 owns feature transforms and train/serve registration; FX-05 owns leverage caps and sizing; FX-06 owns broker routing under read-only/paper-first governance; FX-07 owns backtest realism and cost evidence; FX-08 surfaces status and one canonical 17:00 ET session clock. All of this extends the existing model-vs-runtime, feature-registry, and champion/challenger architecture; it does not create a parallel FX framework.

## §9 Sources

1. [S1] OANDA v20 Introduction, https://developer.oanda.com/rest-live-v20/introduction/, retrieved 2026-06-23. Supports v20 API scope, real-time rates, and historical pricing back to 2005.
2. [S2] OANDA v20 Development Guide, https://developer.oanda.com/rest-live-v20/development-guide/, retrieved 2026-06-23. Supports practice/live REST and streaming hosts, rate limits, and Prices endpoint guidance.
3. [S3] OANDA v20 Pricing endpoint, https://developer.oanda.com/rest-live-v20/pricing-ep/, retrieved 2026-06-23. Supports account pricing stream endpoint and 250 ms stream-window behavior.
4. [S4] OANDA v20 Primitives definitions, https://developer.oanda.com/rest-live-v20/primitives-df/, retrieved 2026-06-23. Supports `InstrumentName`, `pipLocation`, `marginRate`, financing, and pricing component definitions.
5. [S5] OANDA v20 Order definitions, https://developer.oanda.com/rest-live-v20/order-df/, retrieved 2026-06-23. Supports `units` sign convention for long versus short orders.
6. [S6] OANDA hours of operation, https://www.oanda.com/us-en/trading/hours-of-operation/ and https://www.oanda.com/bvi-en/cfds/hours-of-operation/, retrieved 2026-06-23. Supports 17:05/16:59 New York FX hours, daily break, and 100,000-unit standard lot equivalent.
7. [S7] OANDA financing fees, https://www.oanda.com/us-en/trading/financing-fees/, retrieved 2026-06-23. Supports 5 p.m. ET rollover, financing formula concepts, T+2 settlement, and Wednesday three-day funding.
8. [S8] IBKR API contracts, https://www.interactivebrokers.com/campus/ibkr-api-page/contracts/, retrieved 2026-06-23. Supports FX `CASH` contracts, `IDEALPRO`, and base/target currency contract fields.
9. [S9] IBKR API order types, https://www.interactivebrokers.com/campus/ibkr-api-page/order-types/, retrieved 2026-06-23. Supports forex `cashQty` order sizing and paper-account simulation caveat.
10. [S10] Alpaca docs and blog, https://docs.alpaca.markets/us/docs/account-plans, https://docs.alpaca.markets/us/reference/getassets, and https://alpaca.markets/blog/offer-us-stocks-in-your-native-currency-with-alpacas-local-currency-trading-api/, retrieved 2026-06-23. Supports no retail spot-FX scope and local-currency API not being forex trading.
11. [S11] Polygon/Massive forex aggregates, https://polygon.io/docs/rest/forex/aggregates/custom-bars, retrieved 2026-06-23. Supports `C:EURUSD` style forex aggregates and quote-derived bars.
12. [S12] Polygon/Massive forex quotes, https://massive.com/docs/rest/forex/quotes/quotes, retrieved 2026-06-23. Supports historical BBO quotes with bid/ask prices and timestamps.
13. [S13] CCXT docs, https://docs.ccxt.com/ and https://github.com/ccxt/ccxt, retrieved 2026-06-23. Supports CCXT being a cryptocurrency exchange library.
14. [S14] TrueFX historical downloads, https://www.truefx.com/truefx-historical-downloads/ and https://www.truefx.com/, retrieved 2026-06-23. Supports top-of-book tick-by-tick market data, fractional-pip spreads, and millisecond detail.
15. [S15] HistData download and FAQ, https://www.histdata.com/download-free-forex-data/ and https://www.histdata.com/f-a-q/, retrieved 2026-06-23. Supports M1/tick downloads and bid/ask availability in Generic ASCII tick data.
16. [S16] Dukascopy historical data export, https://www.dukascopy.com/swiss/english/marketwatch/historical/, retrieved 2026-06-23. Supports tick-to-monthly historical data export.
17. [S17] FRED/St. Louis Fed series pages: `DFF`, `DFEDTARU`, `ECBDFR`, `IUDSOIA`, `IRSTCI01JPM156N`, `DFII10`, and `DTWEXBGS`, https://fred.stlouisfed.org/, retrieved 2026-06-23. Supports named macro series IDs and frequencies.
18. [S18] Trading Economics Calendar API docs, https://docs.tradingeconomics.com/economic_calendar/snapshot/ and https://tradingeconomics.com/api/calendar.aspx, retrieved 2026-06-23. Supports nearly real-time calendar, official-source actuals, forecasts, revisions, and response fields.
19. [S19] CFTC financial futures COT current report, https://www.cftc.gov/dea/futures/financial_lf.htm, retrieved 2026-06-23. Supports CME FX market names in the current financial futures report.
20. [S20] CFTC Commitments of Traders overview, https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm, retrieved 2026-06-23. Supports weekly COT report context.
21. [S21] Menkhoff, Sarno, Schmeling, and Schrimpf, "Currency momentum strategies," Journal of Financial Economics / BIS Working Paper 366, https://ideas.repec.org/a/eee/jfinec/v106y2012i3p660-684.html and https://www.bis.org/publ/work366.pdf, retrieved 2026-06-23. Supports currency momentum, transaction-cost caveats, and limits to arbitrage.
22. [S22] Lustig, Roussanov, and Verdelhan, "Common Risk Factors in Currency Markets," NBER Working Paper 14082 / Review of Financial Studies, https://www.nber.org/papers/w14082, retrieved 2026-06-23. Supports dollar and carry risk factors and net-of-cost carry evidence.
23. [S23] Asness, Moskowitz, and Pedersen, "Value and Momentum Everywhere," AQR / Journal of Finance, https://www.aqr.com/Insights/Research/Journal-Article/Value-and-Momentum-Everywhere, retrieved 2026-06-23. Supports cross-asset value and momentum premia, including currencies.
24. [S24] NFA Forex Transactions Regulatory Guide, https://www.nfa.futures.org/members/member-resources/files/forex-regulatory-guide.html, retrieved 2026-06-23. Supports 2%/5% security deposits, higher percentage rule, FIFO, and no offsetting positions.
25. [S25] ESMA CFD product intervention measures, https://www.esma.europa.eu/press-news/esma-news/esma-adopts-final-product-intervention-measures-cfds-and-binary-options, retrieved 2026-06-23. Supports 30:1 major and 20:1 non-major retail CFD leverage limits.
26. [S26] CLS report on FX settlement cycles, https://www.cls-group.com/insights/innovation/report-reimagining-same-day-fx-exploring-the-case-for-additional-settlement-cycles-shapingfx-series/, retrieved 2026-06-23. Supports T+2 as the convention for most CLSSettlement FX instructions.
27. [S27] FXCM ForexConnect API guide, https://www.fxcorporate.com/help/Java, retrieved 2026-06-23. Supports ForexConnect as a Forex/CFD API alternative.
28. [S28] OANDA micro lots help, https://help.oanda.com/us/en/faqs/micro-lots.htm, retrieved 2026-06-23. Supports standard/mini/micro lot unit relationships and OANDA unit-based sizing.
