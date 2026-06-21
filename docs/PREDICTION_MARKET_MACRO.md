# Prediction-Market Alternative Data

This document describes read-only prediction-market signal paths:

- Kalshi event-contract market data plus CME FedWatch rate-probability data for
  macro expectations.
- Polymarket public event-market data for crypto regulation, policy, election,
  geopolitical, AI/tech, and narrative event probabilities.
- ForecastEx regulated event-contract CSV data, plus optional read-only IBKR
  event-contract market data where an explicit conid allowlist is configured.

## Scope And Authority

The signal is read-only alternative data. It has no order, account, portfolio,
or execution authority.

The feature groups are `prediction_market_macro_v1` and
`prediction_market_event_v1`. Every feature id in both groups is registered as
`stage=shadow` with `direct_trading_authority=false`.
Live model serving rejects these ids through
`engine.strategy.feature_registry.assert_no_shadow_features(...)`.

Before any future promotion, the signal must pass the existing model-governance
path with out-of-sample, net-after-cost, PIT, deconfounded, replay, shadow/live
monitoring, production-readiness, and operator-audit evidence. This change does
not promote the group.

Polymarket data is alternative data, not trading advice. The integration is
data-only and does not include authenticated trading, wallet, bridge,
position-management, order-placement, or account endpoints. Operators are
responsible for honoring geographic/legal restrictions and provider terms before
enabling any public-data polling.

ForecastEx and optional IBKR event-contract data is also alternative data. The
IBKR adapter is limited to read-only market-data calls for allowlisted event
contracts and is intentionally separate from broker execution, account state,
portfolio, and order-placement code paths.

## Existing Signal Inventory

Polymarket is intentionally complementary to current feature groups:

- Crypto positioning: `funding_rate_now`, funding z-score/extreme/cumulative
  funding, perp basis, and basis z-score measure exchange positioning and
  derivatives crowding.
- Social: mention-rate, unique-author, new-author, sentiment-dispersion,
  manipulation-risk, attention-shock, promotional-likelihood, and social-regime
  mania/fear/churn features measure public chatter.
- News and document flow: event counts, velocity, novelty, staleness, importance,
  FinBERT sentiment, structured-document events, and news-flow embeddings measure
  realized text arrival and document extraction.
- Government flow: congressional trade, lobbying-spend, and government-contract
  features measure disclosed government-adjacent activity after publication.
- Macro: FRED-style macro vintages, GDELT macro news shares, Kalshi macro
  contracts, CME FedWatch, and ForecastEx regulated event-contract products
  measure rates, inflation, labor, commodities, weather/climate, energy, and
  broad macro expectations.

Polymarket adds market-implied probabilities for explicitly mapped future event
semantics. It should not duplicate news/social attention or government-flow
counts; it contributes only when markets are mapped to affected assets and pass
liquidity, status, freshness, and PIT controls.

ForecastEx adds regulated event-contract expectations from official daily and
intraday CSV files. These are not macro indicator vintages: they are market
probabilities and trading-activity metadata, and unavailable/stale contracts
remain unavailable rather than becoming zero-conviction feature values.

## Providers

Kalshi:

- Source key: `kalshi_prediction_market_macro`
- Job: `poll_kalshi_prediction_markets`
- Default enabled: `false`
- Endpoint family: unauthenticated public market data under
  `https://external-api.kalshi.com/trade-api/v2`
- Reads: series, events, markets, and order books
- Excludes: authenticated trading, portfolio, order, position, and websocket
  account endpoints

CME FedWatch:

- Source key: `cme_fedwatch`
- Job: `poll_cme_fedwatch`
- Default enabled: `false`
- Preferred mode: `official_api`, using the entitled CME FedWatch REST API with
  `CME_FEDWATCH_OAUTH_TOKEN`
- Public-page parsing mode: disabled unless
  `CME_FEDWATCH_ALLOW_PUBLIC_PAGE_PARSE=1` and
  `CME_FEDWATCH_MODE=public_page`

CME public-page parsing is operationally fragile because it depends on website
HTML/script structure, not a stable licensed API contract. Use it only for
research diagnostics when licensed API access is unavailable.

Polymarket:

- Source key: `polymarket_event_signals`
- Job: `poll_polymarket_prediction_markets`
- Default enabled: `false`
- Endpoint families:
  - public Gamma discovery under `https://gamma-api.polymarket.com`
  - public CLOB market-data endpoints under `https://clob.polymarket.com`
  - optional public Data API trade reads under `https://data-api.polymarket.com`
- Reads: events, markets, condition/token ids, outcome prices, order books,
  midpoint, spread, last trade, optional price history, volume, liquidity, open
  interest, status, and update timestamps
- Excludes: authenticated order placement, wallet, bridge, API-key, position,
  portfolio, signing, funding, and account-management flows

ForecastEx:

- Source key: `forecastex_event_contracts`
- Job: `poll_forecastex_event_contracts`
- Default enabled: `false`
- Endpoint family: public CSV downloads under `https://forecastex.com`
- Reads: daily/intraday pairs, daily prices, and product summary CSV files
- Preserves: file date, file kind, provider timestamp, refresh cadence, product
  ids, contract ids, product category, official resolution source metadata,
  market status, prices, volume, open interest, and total pairs
- Excludes: order placement, account state, and direct trading authority

IBKR event-contract market data:

- Optional child path of `forecastex_event_contracts`
- Default enabled: `false`
- Requires `FORECASTEX_IBKR_ENABLED=1` and
  `FORECASTEX_IBKR_CONTRACT_ALLOWLIST`
- Uses only allowlisted conids and safe market-data request/cancel calls
- Excludes: broker execution, account, portfolio, position, and order mutation

Primary provider references:

- Kalshi market data quick start:
  `https://docs.kalshi.com/getting_started/quick_start_market_data`
- Kalshi order book endpoint:
  `https://docs.kalshi.com/api-reference/market/get-market-orderbook`
- CME FedWatch tool:
  `https://www.cmegroup.com/markets/interest-rates/cme-fedwatch-tool.html`
- CME FedWatch API:
  `https://www.cmegroup.com/market-data/market-data-api/fedwatch-api.html`
- Polymarket CLOB API:
  `https://docs.polymarket.com/developers/CLOB/introduction`
- Polymarket Gamma markets:
  `https://docs.polymarket.com/developers/gamma-markets-api/get-markets`
- ForecastEx data downloads:
  `https://forecastex.com/data`
- IBKR event-contract API:
  `https://www.interactivebrokers.com/campus/ibkr-api-page/event-contracts/`

## Source Settings

Kalshi settings are projected from the data-source control plane:

- `base_url`
- `series_allowlist`
- `category_filters`
- `status`
- `poll_seconds`
- `limit`
- `max_pages`
- `max_orderbooks`
- `include_orderbooks`
- `asset_map_json`

CME FedWatch settings:

- `mode`
- `base_url`
- `public_page_url`
- `allow_public_page_parse`
- `poll_seconds`
- `asset_map_json`

Polymarket settings:

- `gamma_base_url`
- `data_base_url`
- `clob_base_url`
- `tags`
- `slugs`
- `keyword_allowlist`
- `category_filters`
- `event_type_filters`
- `status`
- `min_liquidity`
- `min_volume`
- `min_open_interest`
- `poll_seconds`
- `limit`
- `max_pages`
- `max_orderbooks`
- `include_orderbooks`
- `include_history`
- `include_data_trades`
- `asset_basket_map_json`
- `semantic_event_map_json`

ForecastEx settings:

- `base_url`
- `file_date_lookback`
- `file_dates`
- `file_kinds`
- `intraday_refresh_window`
- `product_allowlist`
- `product_category_allowlist`
- `asset_map_json`
- `resolution_source_map_json`
- `poll_seconds`
- `timeout_s`
- `ibkr_enabled`
- `ibkr_contract_allowlist`

Credentials:

- CME official API uses `oauth_token`, projected to `CME_FEDWATCH_OAUTH_TOKEN`
  when the source is enabled.
- Kalshi public market-data polling does not require credentials.
- Polymarket public data polling does not require credentials. The data-source
  template has no credential fields. Wallet, bridge, private-key, API-key,
  signing, funding, and trading settings are rejected by allowed-field
  validation or by the provider's data-only settings guard.
- ForecastEx CSV polling does not require credentials.
- IBKR event-contract market-data polling reuses existing IBKR socket settings
  only when explicitly enabled and contract allowlisted; it does not add order,
  account, portfolio, or execution settings to the ForecastEx source.

## Storage Contract

Migration `0065_prediction_market_macro.py` creates:

- `prediction_market_events`
- `prediction_market_markets`
- `prediction_market_orderbook_snapshots`
- `prediction_market_price_history`
- `prediction_market_backfill_state`

Rows include provider identity, provider category, source timestamp,
availability timestamp, event/resolution timestamps, liquidity, volume,
spread, affected assets, provider-specific raw JSON, and a deterministic raw
payload hash.

`prediction_market_events` stores event metadata.
`prediction_market_markets` stores market metadata and current implied
probabilities.
`prediction_market_orderbook_snapshots` is append-only by provider market,
availability timestamp, and raw payload hash.
`prediction_market_price_history` is reserved for provider trade/price history
when available.

Migration `0066_prediction_market_event_signals.py` adds event-market metadata
to the shared tables:

- `semantic_event_id`
- `resolution_semantics`
- `condition_id`
- `token_id`
- `outcome_name`

Cross-provider dispersion may use these columns only when `semantic_event_id`
and `resolution_semantics` are explicitly mapped. It never compares markets by
fuzzy title matching alone.

Migration `0067_prediction_market_regulated_event_contracts.py` adds regulated
event-contract metadata to the same shared tables:

- `provider_contract_id`
- `product_id`
- `official_resolution_source`
- `source_file_date`
- `source_file_kind`
- `refresh_cadence`
- `provider_timestamp_ms`

These fields make ForecastEx CSV provenance and optional IBKR conid lineage
auditable without introducing a provider-specific storage table.

Resolution outcome values are not feature inputs. The feature resolver excludes
markets whose `resolution_ts_ms <= decision_ts_ms`, so post-resolution rows do
not enter pre-resolution training features.

## Feature IDs

All ids are under `prediction_market_macro_v1.*`:

- `probability_level`
- `probability_delta`
- `event_urgency`
- `liquidity_adjusted_probability_move`
- `orderbook_imbalance`
- `spread_quality`
- `cme_vs_kalshi_disagreement`
- `kalshi_available`
- `cme_available`
- `available`

Event-market ids are under `prediction_market_event_v1.*`:

- `crypto_regulation_probability`
- `regulated_macro_probability`
- `regulated_energy_probability`
- `regulated_climate_weather_probability`
- `regulated_fx_rates_probability`
- `regulated_equity_index_probability`
- `regulated_commodity_probability`
- `probability_momentum`
- `liquidity_adjusted_event_shock`
- `orderbook_imbalance`
- `spread_quality`
- `event_urgency`
- `market_attention`
- `cross_provider_dispersion`
- `polymarket_available`
- `forecastex_available`
- `ibkr_event_contract_available`
- `available`

The PIT policy for both groups uses:

- source timestamp: `latest_source_ts_ms`
- availability timestamp: `latest_availability_ts_ms`
- freshness TTL: 36 hours
- stale behavior: zero features and mark the group unavailable

## Asset Mapping

The default conservative macro basket includes broad equity indexes, rates,
gold, USD-sensitive proxies, banks, homebuilders, crypto, and listed crypto
brokers/platforms:

`SPY`, `QQQ`, `IWM`, `TLT`, `IEF`, `SHY`, `GLD`, `UUP`, `XLF`, `KRE`, `IAT`,
`ITB`, `XHB`, `BTC`, `ETH`, `COIN`, `HOOD`.

Operators can override mapping with `asset_map_json` in the data-source
settings. The same setting projects to
`PREDICTION_MARKET_MACRO_ASSET_MAP_JSON` for legacy job compatibility.

Polymarket defaults map crypto regulation, BTC, ETH, SOL, miners, risk-on,
risk-off, policy, election, geopolitical, AI/tech, and narrative baskets to
symbols such as `BTC`, `ETH`, `SOL`, `COIN`, `HOOD`, `MSTR`, listed miners,
major risk ETFs, and configurable sector ETFs. Operators should override
`asset_basket_map_json` when a market has narrower exposure.

ForecastEx defaults map regulated macro, energy, climate/weather, FX/rates,
equity-index, and commodity event types to conservative baskets such as broad
index ETFs, rates ETFs, USD proxies, metals, energy ETFs, agriculture ETFs, and
sector proxies. Operators should override `asset_map_json` and
`resolution_source_map_json` for production research so product ids and
resolution-source metadata are explicit.

Use `semantic_event_map_json` to declare comparable event identities and
resolution semantics. Example:

```json
{
  "bitcoin-etf-approved": {
    "semantic_event_id": "spot_btc_etf_approval_2026",
    "resolution_semantics": "yes_if_us_spot_btc_etf_approved_by_2026_12_31",
    "event_type": "crypto_regulation",
    "affected_assets": ["BTC", "COIN", "MSTR", "IBIT"]
  }
}
```

Comparable Kalshi or other-provider rows must carry the same semantic id and
resolution semantics before `prediction_market_event_v1.cross_provider_dispersion`
can move away from zero.

## Operator Evaluation

Event markets can be noisy, thin, reflexive, or stale around headline bursts.
Choose categories narrowly:

- Prefer liquid markets with clear resolution criteria and stable token ids.
- Avoid broad novelty markets unless they map cleanly to an asset basket.
- Keep `min_liquidity`, `min_volume`, and `min_open_interest` above research
  noise levels for the target asset.
- Treat wide spreads as low-quality probability estimates.
- Treat ForecastEx sparse products, inactive products, old CSV dates, and
  unmapped product ids as explicit unavailable states.
- Keep optional IBKR conid allowlists narrow and read-only; do not use the IBKR
  event-contract adapter to infer execution eligibility.

Evaluate whether Polymarket, ForecastEx, or IBKR event-contract data leads
traded assets only after latency, slippage, borrow/funding, fees, spread, and
market-hours effects. Promotion evidence must be out-of-sample,
net-after-cost, PIT-safe, deconfounded from existing news/social/macro/
government features, and production-ready before any future non-shadow use is
considered.

## Backfill And Replay

`backfill_prediction_market_macro` replays stored
`prediction_market_markets.availability_ts_ms` values through
`build_model_feature_snapshot(...)` and writes canonical
`model_feature_snapshots` rows for explicit
`prediction_market_macro_v1.*` feature contracts.

The replay job does not fetch settlement outcomes and does not use event
resolution values as feature values. It only reconstructs what was available at
each historical availability timestamp.

`poll_forecastex_event_contracts` can backfill specific CSV dates through
`file_dates` or rolling windows through `file_date_lookback`. Reprocessing the
same file date is idempotent because normalized rows carry provider ids,
availability timestamps, and deterministic raw payload hashes. Official
resolution datasets remain metadata for feature generation before resolution;
final outcome values are never used as pre-resolution features.
