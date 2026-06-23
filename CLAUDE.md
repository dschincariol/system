# Trading System — Claude Code Instructions

This file is loaded automatically into every Claude Code session in this repo. It is the persistent instruction surface for AI assistants working on the codebase.

Last verified against code: 2026-06-21

## North Star

Build an autonomous alpha discovery system that searches a universe of inputs, discovers which are useful, builds models linking inputs to predictions, and trades any financial product for profit. The goal is "best in the world" — comparable to Renaissance Technologies, Two Sigma, WorldQuant.

The current system is a substantial supervised trading runtime (~950+ Python files). **Improvements build on existing architecture — never replace it.**

## System at a glance

Full-stack supervised trading system, Postgres-backed runtime storage facade, ~100+ registered jobs.

- **Data sources:** Prices (CCXT/IBKR/Polygon/yfinance), News/RSS, SEC/EDGAR, Social (Reddit/StockTwits), GDELT, Weather (NOAA), Options (Polygon), Earnings (FMP), Macro, Transcripts, Form 4 insider, congressional trades, and PIT universe snapshots.
- **Features:** Schema-driven train/serve parity through `engine/strategy/feature_registry.py`. The default serving schema is ~111 feature ids; the registry contains ~1,700 ids when opt-in/shadow groups are included. Current groups include base, price, events, macro, FX, HMM regime, tech, stress, social, weather, options-symbol, availability, tsfresh, NLP/FinBERT/news, filings, and transcripts.
- **Models:** Current active families center on LightGBM, XGBoost, sklearn GBM, PatchTST, iTransformer, and a Ridge meta-ensemble (`engine/strategy/models/`, `engine/strategy/ensemble/ridge_meta.py`). Legacy/fallback paths still exist for embed regressors, temporal predictors, and regime/statistical baselines. Shadow-only RL remains advisory/shadow and has no live order authority.
- **Strategy:** Multi-model predictor routing, canonical model intent (`engine/strategy/model_intent.py`), portfolio construction (max 3 positions, anti-flip-flop), 3-layer regime stack (macro/asset/micro), alpha lifecycle with TTL/half-life decay.
- **Risk:** Portfolio risk engine (gross 1.0 / net 0.6, vol targeting, correlation clusters), Monte Carlo (1500 sims, VaR/CVaR), drawdown guard (6% throttle), circuit breaker, kill switch, trade suppression (HARD_BLOCK/SOFT_THROTTLE/SIZE_COMPRESSION).
- **Execution:** Policy engine (TTL, alpha decay, aggressiveness tiers, regime sizing), TWAP/VWAP/POV/adaptive slicing, broker router (Alpaca/IBKR/Sim with failover + reconciliation gate), AI advisor (advisory only), attribution ledger.
- **Oversight:** Dashboard server + browser UI, operator AI (bounded LLM diagnostics), governance (promotion/replay/critic/audit), browser terminal, CPCV/gated backtest evidence, BH-FDR/Harvey-Liu-Zhu/Reality-Check gates, pool-correlation/MPC gates, era robustness, Optuna/surface robustness, and drift-triggered retraining.

See `docs/README_ARCHITECTURE.md` for the full architecture document.

## Optimization roadmap (agreed priorities)

5-phase roadmap persisted in `.claude/projects/.../memory/optimization_roadmap.md`. Summary:

- **Phase 1 (P0, Months 1–3) — Foundation:** DONE in current code for statistical promotion gates, CPCV/PBO/deflated Sharpe, gated cost-adjusted promotion backtests, tsfresh/PySR feature discovery hooks, and Optuna HPO/surface robustness. Continue hardening survivorship-bias and data-quality coverage.
- **Phase 2 (P1, Months 3–6) — Intelligence:** PARTIAL/DONE in current code for LightGBM, XGBoost, sklearn GBM, PatchTST, iTransformer, FinBERT/NLP feature groups, causal scoring, Ridge meta-ensemble blending, and shadow RL. Graph models, TabNet, and broader LLM transcript workflows remain future work.
- **Phase 3 (P1, Months 6–9) — Scale:** PARTIAL. Runtime storage now goes through a Postgres-backed facade, but Kafka streaming, Feast feature store, Redis hot caching, broader universe expansion, and more alt data remain roadmap work.
- **Phase 4 (P1–P2, Months 9–12) — Autonomy:** Closed-loop alpha discovery (generate → test → backtest → shadow → promote → monitor → retire); meta-learning; deep RL portfolio manager (PPO/SAC via FinRL); self-monitoring/self-repair.
- **Phase 5 (P2–P3, 12+ months) — Edge:** L2 microstructure modeling, event-driven alpha (earnings/M&A/macro), options as instruments (vol surface arb, tail hedging).

**Immediate quick wins:** DONE in current code. Pointers: statistical gates in `engine/strategy/statistical_gates.py` and `engine/strategy/promotion_guard.py`; CPCV/gated backtests in `engine/strategy/cpcv.py` and `engine/strategy/gated_backtest.py`; tsfresh in `engine/strategy/tsfresh_features.py`; LightGBM/XGBoost/GBM/PatchTST/iTransformer in `engine/strategy/models/`; Ridge ensemble in `engine/strategy/ensemble/ridge_meta.py`. See `docs/handoff/QUICK_WINS.md` for the historical prompt archive.

## How to work in this repo

- **Consult the roadmap before proposing changes.** P0 items should land before P1, etc. If a request doesn't fit the roadmap, ask whether it should be prioritized.
- **Favor automated discovery, self-evaluation, closed-loop learning** over manual configuration. Replacing env vars with learned/optimized parameters is usually an improvement.
- **Respect the model-vs-runtime contract.** Models propose intent (symbols, side, size, timing); the runtime owns final safety gates (risk caps, kill switches, execution realism). Don't give models direct order authority.
- **Preserve train/serve parity.** Features flow through `engine/strategy/feature_registry.py` with explicit `feature_ids`. Any new feature must be registered there and round-trip through the persisted feature schema.
- **Champion/challenger is the promotion path.** New models enter as challengers, compete in the marketplace, pass replay + self-critic + cooldowns, then replace the champion. Don't add models that bypass this loop.
- **Runtime storage is high blast radius.** Nearly every subsystem reads/writes through `engine/runtime/storage.py` and the current Postgres-backed implementation behind it. Schema and write behavior changes require migrations and tests — proceed carefully.
- **Governance is integrated, not parallel.** New safety/promotion logic extends existing governance jobs rather than creating new frameworks alongside them.

## Key files

- `start_system.py` — main entrypoint
- `dashboard_server.py` — HTTP/UI boundary
- `engine/runtime/storage.py` — runtime storage facade (sensitive)
- `engine/runtime/job_registry.py` — canonical job catalog
- `engine/strategy/feature_registry.py` — feature catalog (schema-driven)
- `engine/strategy/predictor.py` — live prediction orchestrator / model routing
- `engine/strategy/champion_manager.py` — champion/challenger selection
- `engine/strategy/model_intent.py` — canonical model intent payload
- `engine/strategy/portfolio.py` — portfolio construction
- `engine/execution/broker_router.py` — broker failover + reconciliation gate
- `engine/execution/execution_policy_engine.py` — TTL, alpha decay, suppression
- `engine/risk/portfolio_risk_engine.py` — gross/net caps, vol targeting
- `engine/risk/monte_carlo_risk_engine.py` — VaR/CVaR

## Reference docs

- `docs/README_ARCHITECTURE.md` — full architecture
- `docs/README_SEQUENCE_DIAGRAMS.md` — runtime flows
- `docs/README_DEVELOPER_MAP.md` — file-level map
- `docs/README_DATABASE_MAP.md` — runtime database/storage map
- `docs/README_OPERATOR_GUIDE.md` — operator workflows
- `docs/handoff/TRADING_SYSTEM_HANDOFF.md` — consolidated handoff for Claude app Projects
- `docs/handoff/QUICK_WINS.md` — 5 self-contained implementation prompts
