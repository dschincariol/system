# Prediction Market Deep Dive Implementation Prompts

Use these prompts one at a time. Each prompt is scoped to one data-source recommendation from the prediction-market review: Kalshi/CME, Polymarket, ForecastEx/IBKR, Deribit, and narrowly mapped sportsbook or betting-exchange odds.

## Common Preamble

You are working in `/home/david/gitsandbox/system/system`. The repo may be dirty; do not revert unrelated user changes. First read `README.md`, `docs/DOCUMENTATION_INDEX.md`, `docs/DATA_CONTRACTS.md`, `docs/REFERENCE_DATA_SOURCE_CONTROL_PLANE.md`, `engine/data/README.md`, `engine/strategy/README.md`, and `services/README.md`. Preserve the existing architecture: new alternative data must flow through the data-source control plane, registered runtime jobs, point-in-time feature snapshots, explicit feature ids, shadow/live staging, and the existing promotion/governance path. Do not give prediction-market data direct order authority. Start every new source as shadow or research-only until it passes out-of-sample, net-after-cost, PIT, deconfounded, and production-readiness checks.

## Prompt 1 - Kalshi and CME FedWatch Macro Event Expectations

Deep dive and implement the first prediction-market signal: regulated macro event expectations from Kalshi plus CME FedWatch/rate-probability data. Current evidence: the repo already has macro ingestion, macro feature ids, PIT freshness controls, model-feature snapshots, data-source management, and promotion gates, but it does not yet ingest event-contract order books or market-implied probabilities.

Requirements:
- Inventory existing macro ingestion and feature flow from `poll_macro` through `engine/strategy/feature_registry.py`, `engine/strategy/feature_pit.py`, and `engine/strategy/model_feature_snapshots.py`.
- Design normalized prediction-market storage that can be reused by later providers: event metadata, market metadata, order-book snapshots, trade/price history when available, provider category, resolution timestamp, source timestamp, availability timestamp, liquidity, volume, spread, and provider-specific raw payload hash.
- Implement a Kalshi read-only provider using public market-data endpoints for series, events, markets, and order books. Keep authenticated trading endpoints out of scope.
- Implement a CME FedWatch or Fed Funds futures-derived rate-probability provider. Prefer official or licensed market data where available; if scraping or public-page parsing is used, isolate it behind explicit provider settings and document operational fragility.
- Add data-source catalog entries and managed jobs for the new providers, including source settings for category filters, series allowlists, poll cadence, symbol/asset mapping, and provider enablement.
- Register canonical job entries in `engine/runtime/job_registry.py`; make startup and lifecycle behavior consistent with existing ingestion jobs.
- Add point-in-time feature ids under a new group such as `prediction_market_macro_v1`, starting as shadow-only. Include probability level, probability delta, event urgency, liquidity-adjusted probability move, order-book imbalance, spread quality, CME-vs-Kalshi disagreement, and availability flags.
- Add a PIT policy for prediction-market features using the provider availability timestamp, not resolution date or event date.
- Map macro events to affected assets conservatively: SPY, QQQ, IWM, TLT, GLD, USD-sensitive assets, banks, homebuilders, BTC, ETH, COIN, HOOD, and other configured baskets.
- Build backfill and replay support so the feature can be evaluated without lookahead. Resolution outcomes must never leak into pre-resolution training features.
- Add tests for provider parsing, idempotent writes, PIT enforcement, feature registration, feature snapshot inclusion/exclusion, stale-source behavior, shadow-only live-serving rejection, and data-source lifecycle projection.
- Add documentation for provider setup, source limits, data contract, feature ids, and how the signal must pass promotion gates before any live authority.

Suggested files to inspect:
- `services/data_source_manager.py`
- `engine/runtime/job_registry.py`
- `engine/data/`
- `engine/runtime/storage_pg.py`
- `engine/runtime/schema/migrations/`
- `engine/strategy/feature_registry.py`
- `engine/strategy/feature_pit.py`
- `engine/strategy/model_feature_snapshots.py`
- `engine/strategy/promotion_guard.py`
- `docs/DATA_CONTRACTS.md`
- `docs/REFERENCE_DATA_SOURCE_CONTROL_PLANE.md`
- `.env.example`

Acceptance:
- Kalshi and CME/FedWatch-derived macro expectations are ingested read-only through registered, operator-manageable providers.
- Prediction-market macro features are PIT-safe, shadow-only, and unavailable/stale when source freshness or availability timestamps fail.
- Backtests and challenger evaluation can include these features without leaking post-resolution outcomes.
- Live serving cannot use the new feature group unless a future audited change explicitly promotes it through the existing feature-stage and model-governance controls.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

## Prompt 2 - Polymarket Crypto, Policy, and Geopolitical Event Signals

Deep dive and implement a Polymarket read-only signal path for crypto regulation, policy, election, geopolitical, AI/tech, and narrative event probabilities. Current evidence: Polymarket exposes public discovery, data, CLOB order-book, pricing, midpoint, spread, and history endpoints, while this repo already has crypto positioning, news/social, macro, and government-flow features that can be augmented by event-market expectations.

Requirements:
- Inventory current crypto, social, news, government, and macro feature groups so Polymarket features complement rather than duplicate existing signals.
- Implement a read-only Polymarket provider for public Gamma/Data/CLOB endpoints. Do not add authenticated order placement, wallet, bridge, or position-management flows.
- Build configurable event discovery: tags, slugs, keyword allowlists, category filters, market status, liquidity thresholds, minimum volume/open interest, and asset-basket mappings.
- Normalize Polymarket markets into the shared prediction-market storage from Prompt 1 or create that shared storage if it does not yet exist.
- Capture order-book snapshots, midpoint, spread, last trade, price history, volume, open interest, market status, condition/token ids, and update timestamps.
- Add semantic event mapping for crypto and policy assets such as BTC, ETH, SOL, COIN, HOOD, MSTR, miners, risk-on/risk-off baskets, and configurable sector ETFs.
- Compute features such as crypto-regulation probability, probability momentum, liquidity-adjusted event shock, order-book imbalance, spread quality, event urgency, market attention, and cross-provider dispersion where Kalshi or other venues list comparable events.
- Add explicit safeguards for geographic/legal restrictions and provider terms: data-only integration, disabled trading APIs, no wallet credentials in the data-source manager, and documentation that the feed is alternative data rather than trading advice.
- Start the Polymarket feature group as shadow-only with `direct_trading_authority=false` metadata and live-serving rejection.
- Add tests for market discovery filters, token/market parsing, order-book normalization, stale and halted market handling, asset mapping, PIT feature snapshots, cross-provider disagreement calculation, and shadow-only enforcement.
- Add operator docs explaining how to choose event categories, how noisy event markets can be, and how to evaluate whether Polymarket leads traded assets after latency and costs.

Suggested files to inspect:
- `engine/data/crypto_positioning.py`
- `engine/data/quiver_gov.py`
- `engine/data/finbert_sentiment.py`
- `engine/strategy/feature_registry.py`
- `engine/strategy/model_feature_snapshots.py`
- `engine/strategy/feature_pit.py`
- `engine/strategy/deconfounded_promotion.py`
- `services/data_source_manager.py`
- `routes/data_sources_routes.py`
- `tests/test_feature_pit_controls.py`
- `tests/test_provider_readiness_gates.py`

Acceptance:
- Polymarket public data is ingested through a read-only, configurable, registered provider path.
- Only mapped, liquid, non-stale markets contribute to shadow features.
- Cross-provider disagreement is computed only when event identity and resolution semantics are explicitly mapped, not by fuzzy title matching alone.
- The integration cannot introduce wallet, bridge, or prediction-market trading authority.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

## Prompt 3 - ForecastEx and IBKR Regulated Event Contract Data

Deep dive and implement regulated event-contract data ingestion from ForecastEx CSVs and, where available, IBKR event-contract market-data access. Current evidence: ForecastEx publishes daily and intraday event-contract CSV files, and this repo already has IBKR broker/session infrastructure plus data-source and job supervision surfaces.

Requirements:
- Inventory existing IBKR integration, broker session handling, provider settings, and market-data jobs before adding event-contract access.
- Implement a ForecastEx CSV provider for daily pairs, intraday pairs, prices, and summary files. The parser must be idempotent, version tolerant, and explicit about file date, refresh cadence, and provider timestamp.
- Add optional IBKR event-contract market-data support only if it can reuse existing safe market-data paths without affecting broker execution or account state.
- Normalize ForecastEx and IBKR event-contract rows into the shared prediction-market storage, preserving provider contract ids, product ids, official resolution source, market status, price, volume, open interest or total pairs, and source/availability timestamps.
- Add provider-specific data-source settings: base URL, file date lookback, intraday refresh window, product/category allowlist, asset mapping, and optional IBKR contract allowlist.
- Add feature ids for regulated event-contract expectations. Include macro, energy, climate/weather, FX/rates, equity-index, and commodity event probabilities where mapped.
- Ensure official resolution datasets are metadata only for feature generation before resolution; do not leak final resolution values into pre-resolution features.
- Add stale handling for sparse contracts and inactive products so stale low-liquidity event contracts cannot silently look like fresh conviction.
- Integrate provider health and readiness: last successful CSV date, rows parsed, rows skipped, contract categories enabled, stale count, and parse-error count.
- Add tests for CSV parsing, idempotency, malformed rows, duplicate files, sparse/inactive contracts, PIT snapshots, optional IBKR-disabled behavior, provider health payloads, and data-source lifecycle projection.
- Document ForecastEx setup, regulated-data boundaries, IBKR optionality, and how these features differ from existing macro indicators.

Suggested files to inspect:
- `engine/execution/broker_ibkr_gateway.py`
- `engine/jobs/stream_prices_polygon_ws.py`
- `engine/runtime/ingestion_runtime.py`
- `engine/runtime/job_registry.py`
- `services/data_source_manager.py`
- `engine/runtime/storage_pg.py`
- `engine/runtime/schema/migrations/`
- `engine/strategy/feature_registry.py`
- `engine/strategy/model_feature_snapshots.py`
- `docs/REFERENCE_CONFIGURATION_GLOSSARY.md`
- `docs/REFERENCE_DATA_SOURCE_CONTROL_PLANE.md`

Acceptance:
- ForecastEx event-contract CSV data can be backfilled and refreshed idempotently.
- Optional IBKR event-contract access is read-only and cannot alter broker execution behavior.
- Sparse, stale, inactive, or unmapped contracts are explicit unavailable states, not zero-valued conviction.
- Regulated event-contract features are PIT-safe and shadow-only until promoted through existing governance.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

## Prompt 4 - Deribit Crypto Derivatives Volatility and Positioning

Deep dive and implement Deribit public crypto derivatives data as a crypto-volatility and positioning signal. This is not a prediction-market feed, but it was recommended because BTC/ETH options, futures, perpetuals, implied volatility, skew, term structure, and basis can be more directly useful for crypto trading than event-market probabilities.

Requirements:
- Inventory existing crypto positioning, options-symbol, price, and execution-readiness features so Deribit data extends the right group instead of creating duplicate concepts.
- Implement a Deribit public market-data provider using HTTP or WebSocket endpoints for instruments, ticker/order-book snapshots, futures/perpetuals, and options where available. Keep authenticated trading endpoints out of scope.
- Normalize instruments and snapshots with instrument name, base asset, expiry, strike, option type, mark/index price, bid/ask, implied volatility fields if provided, open interest, volume, funding or basis fields where available, source timestamp, and availability timestamp.
- Add data-source settings for enabled assets, instrument types, expiries, minimum liquidity, poll cadence, WebSocket/HTTP mode, and stale thresholds.
- Compute crypto derivatives features such as IV rank, short-dated IV, 25-delta skew or best available proxy, term-structure slope, put/call open-interest ratio, futures basis, perp basis, funding pressure, volume shock, and volatility-regime flags.
- Reuse or extend existing `crypto_positioning` and `options_symbol` groups where appropriate; if adding `deribit_crypto_derivatives_v1`, mark it shadow-only until out-of-sample evidence is available.
- Ensure Deribit data affects crypto assets only unless a deliberate mapping says otherwise.
- Add provider readiness and diagnostics: active instruments, stale instruments, missing IV fields, order-book spread quality, WebSocket reconnect state, and latest snapshot age.
- Add tests for instrument parsing, snapshot normalization, feature calculations, stale handling, feature snapshot PIT controls, crypto-only mapping, provider readiness, and no authenticated Deribit trading path.
- Update docs to explain why Deribit is a derivatives signal rather than a prediction market and how it should be evaluated for BTC/ETH/SOL/crypto-equity strategies.

Suggested files to inspect:
- `engine/data/crypto_positioning.py`
- `engine/strategy/feature_registry.py`
- `engine/strategy/model_feature_snapshots.py`
- `engine/strategy/feature_pit.py`
- `engine/execution/options_readiness.py`
- `engine/runtime/job_registry.py`
- `services/data_source_manager.py`
- `tests/test_options_instrument_readiness.py`
- `tests/test_provider_readiness_gates.py`
- `docs/LIVE_READINESS_CHECKLIST.md`

Acceptance:
- Deribit public derivatives data is ingested read-only and mapped to crypto-relevant feature groups.
- The feature path is PIT-safe, stale-aware, and unable to add live options or Deribit order authority.
- Operators can inspect provider health and understand whether missing IV/skew fields make the signal unavailable.
- Crypto derivatives features are evaluated through existing challenger, net-after-cost, and promotion evidence paths before any live use.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

## Prompt 5 - Sportsbook and Betfair Odds as Narrow Research-Only Signals

Deep dive and implement a research-only sportsbook or betting-exchange odds pipeline, but only for narrow asset mappings and probability-calibration research. Current recommendation: general sports odds are low priority for broad stock/crypto trading and should not be added as a broad-market signal. They may be useful for sportsbook equities, sports-media names, data providers, advertising-sensitive event studies, or model-calibration experiments.

Requirements:
- Inventory whether the current tradable universe includes sports betting, media, gaming, data-provider, apparel/sponsor, or ad-sensitive names. If no defensible mapping exists, implement only a documented research/backfill scaffold and say NO-GO for production features.
- Design a generic odds-provider interface for read-only feeds such as Betfair historical data, The Odds API, OpticOdds, OddsJam, or similar providers. Keep provider-specific credentials in the data-source manager and keep all betting execution out of scope.
- Normalize event odds with provider, sport/league, event id, market type, outcome, odds format, raw implied probability, normalized no-vig probability, line/spread/total when applicable, timestamp, availability timestamp, volume/liquidity if available, and settlement status only after resolution.
- Implement vig removal and multi-outcome probability normalization before any odds-derived feature can be used.
- Add strict mapping tables from sports/event categories to tradable assets or research labels. No fuzzy asset inference from team names, news headlines, or event titles should be used in production feature snapshots.
- Add feature groups as research-only or shadow-only, for example `sports_odds_sector_v1`, with explicit `direct_trading_authority=false`.
- Add controls that prevent sportsbook odds from entering broad-market default feature sets. They should be opt-in per asset basket, research experiment, or shadow challenger.
- Add event-study tooling or a backfill job to measure whether odds movements lead target assets after realistic latency, fees, and slippage.
- Add tests for vig removal, multi-outcome normalization, mapping allowlists, stale odds, settlement lookahead prevention, research-only feature staging, and rejection of unmapped events.
- Update docs to state that sportsbook odds are low priority and should be promoted only if a specific asset mapping and incremental out-of-sample edge are proven.

Suggested files to inspect:
- `engine/research/`
- `engine/strategy/feature_registry.py`
- `engine/strategy/model_feature_snapshots.py`
- `engine/strategy/experiment_ledger.py`
- `engine/strategy/promotion_guard.py`
- `services/data_source_manager.py`
- `engine/runtime/job_registry.py`
- `docs/DATA_CONTRACTS.md`
- `docs/handoff/QUICK_WINS.md`

Acceptance:
- Sportsbook or Betfair-style odds can be ingested or backfilled only as read-only data.
- Odds-derived probabilities are no-vig normalized before feature use.
- Unmapped or broad-market use is blocked by production code.
- If no defensible tradable mapping exists, the result is an explicit research-only scaffold with NO-GO for production signal use.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run git status --short --untracked-files=all, python tools/git_worktree_triage.py, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.
