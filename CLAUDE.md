# Trading System — Claude Code Instructions

This file is loaded automatically into every Claude Code session in this repo. It is the persistent instruction surface for AI assistants working on the codebase.

## North Star

Build an autonomous alpha discovery system that searches a universe of inputs, discovers which are useful, builds models linking inputs to predictions, and trades any financial product for profit. The goal is "best in the world" — comparable to Renaissance Technologies, Two Sigma, WorldQuant.

The current system is a substantial supervised trading runtime (~400+ Python files). **Improvements build on existing architecture — never replace it.**

## System at a glance

Full-stack supervised trading system, SQLite (WAL mode), ~100+ registered jobs.

- **Data (10 sources):** Prices (CCXT/IBKR/Polygon/yfinance), News/RSS, SEC/EDGAR, Social (Reddit/StockTwits), GDELT, Weather (NOAA), Options (Polygon), Earnings (FMP), Macro, Transcripts
- **Features:** 100+ named features in `engine/strategy/feature_registry.py` across 10 groups (base, price, events, macro, tech, stress, social, weather, options, availability). Schema-driven train/serve parity.
- **Models (3 families):** `regime_stats_v2` (Bayesian priors + spillover betas), `embed_regressor` (Ridge + Torch MLP), `temporal_predictor` (sequence model). Plus shadow-only RL linear policy. Champion/challenger competition with model marketplace, replay validation, self-critic, promotion cooldowns, drift detection.
- **Strategy:** Multi-model predictor routing, canonical model intent (`engine/strategy/model_intent.py`), portfolio construction (max 3 positions, anti-flip-flop), 3-layer regime stack (macro/asset/micro), alpha lifecycle with TTL/half-life decay.
- **Risk:** Portfolio risk engine (gross 1.0 / net 0.6, vol targeting, correlation clusters), Monte Carlo (1500 sims, VaR/CVaR), drawdown guard (6% throttle), circuit breaker, kill switch, trade suppression (HARD_BLOCK/SOFT_THROTTLE/SIZE_COMPRESSION).
- **Execution:** Policy engine (TTL, alpha decay, aggressiveness tiers, regime sizing), TWAP/VWAP/POV/adaptive slicing, broker router (Alpaca/IBKR/Sim with failover + reconciliation gate), AI advisor (advisory only), attribution ledger.
- **Oversight:** Dashboard server + browser UI, operator AI (bounded LLM diagnostics), governance (promotion/replay/critic/audit), browser terminal.

See `docs/README_ARCHITECTURE.md` for the full architecture document.

## Optimization roadmap (agreed priorities)

5-phase roadmap persisted in `.claude/projects/.../memory/optimization_roadmap.md`. Summary:

- **Phase 1 (P0, Months 1–3) — Foundation:** Multiple-hypothesis testing (Benjamini-Hochberg FDR, Harvey/Liu/Zhu t>3.0, White's Reality Check); automated feature discovery (tsfresh, PySR/gplearn); rigorous backtesting (de Prado's Combinatorial Purged CV, Almgren-Chriss, survivorship-bias correction); Optuna Bayesian HPO replacing 200+ hardcoded env vars.
- **Phase 2 (P1, Months 3–6) — Intelligence:** Deep learning model families (PatchTST/iTransformer, Graph Attention Networks, TabNet, LightGBM/XGBoost); enhanced NLP (FinBERT, LLM earnings calls, SEC filing diffs); causal discovery (Granger, DoWhy/CausalML, DAG inference); stacked-ensemble blending instead of single champion selection.
- **Phase 3 (P1, Months 6–9) — Scale:** SQLite → TimescaleDB; Kafka streaming; Feast feature store; Redis caching; universe expansion (all US equities + global ETFs + crypto + futures + FX); more alt data (satellite, credit card proxies, Form 4 insider, congressional STOCK Act).
- **Phase 4 (P1–P2, Months 9–12) — Autonomy:** Closed-loop alpha discovery (generate → test → backtest → shadow → promote → monitor → retire); meta-learning; deep RL portfolio manager (PPO/SAC via FinRL); self-monitoring/self-repair.
- **Phase 5 (P2–P3, 12+ months) — Edge:** L2 microstructure modeling, event-driven alpha (earnings/M&A/macro), options as instruments (vol surface arb, tail hedging).

**Immediate quick wins (1–2 weeks each):** (1) LightGBM/XGBoost model family; (2) tsfresh features in registry; (3) stacked-ensemble blending; (4) t>3.0 threshold for promotion; (5) purged walk-forward CV. Recommended order for execution: 1 (t-stat gate) → 2 (CPCV) → 4 (LightGBM) → 3 (tsfresh) → 5 (ensemble blending). See `docs/handoff/QUICK_WINS.md` for self-contained implementation prompts.

## How to work in this repo

- **Consult the roadmap before proposing changes.** P0 items should land before P1, etc. If a request doesn't fit the roadmap, ask whether it should be prioritized.
- **Favor automated discovery, self-evaluation, closed-loop learning** over manual configuration. Replacing env vars with learned/optimized parameters is usually an improvement.
- **Respect the model-vs-runtime contract.** Models propose intent (symbols, side, size, timing); the runtime owns final safety gates (risk caps, kill switches, execution realism). Don't give models direct order authority.
- **Preserve train/serve parity.** Features flow through `engine/strategy/feature_registry.py` with explicit `feature_ids`. Any new feature must be registered there and round-trip through the persisted feature schema.
- **Champion/challenger is the promotion path.** New models enter as challengers, compete in the marketplace, pass replay + self-critic + cooldowns, then replace the champion. Don't add models that bypass this loop.
- **SQLite is the center of gravity.** Nearly every subsystem reads/writes `engine/runtime/storage.py`. Schema and write behavior changes are high-blast-radius — proceed carefully.
- **Governance is integrated, not parallel.** New safety/promotion logic extends existing governance jobs rather than creating new frameworks alongside them.

## Key files

- `start_system.py` — main entrypoint
- `dashboard_server.py` — HTTP/UI boundary
- `engine/runtime/storage.py` — SQLite center of gravity (sensitive)
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
- `docs/README_DATABASE_MAP.md` — SQLite schema
- `docs/README_OPERATOR_GUIDE.md` — operator workflows
- `docs/handoff/TRADING_SYSTEM_HANDOFF.md` — consolidated handoff for Claude app Projects
- `docs/handoff/QUICK_WINS.md` — 5 self-contained implementation prompts
