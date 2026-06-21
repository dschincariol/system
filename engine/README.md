# Engine Overview

The `engine/` tree contains the Python application code. Most new work lands in one of these subsystems:

- [runtime/README.md](runtime/README.md)
  Boot, lifecycle, storage, jobs, orchestration, and supervision.
- [data/README.md](data/README.md)
  External data adapters, provider routing, ingestion, and source jobs.
- [strategy/README.md](strategy/README.md)
  Features, labels, models, predictions, and portfolio logic.
- [execution/README.md](execution/README.md)
  Broker integrations, routing, execution safety, and attribution.
- [research/README.md](research/README.md)
  Offline stress, fragility, and analysis helpers that consume existing runtime outputs.
- [api/README.md](api/README.md)
  HTTP handlers used by the dashboard and operator.
- [risk/README.md](risk/README.md)
  Risk engines and portfolio risk calculations.
- [terminal/README.md](terminal/README.md)
  Terminal-focused API handlers that back the standalone browser terminal and gated order-entry flow.
- [jobs/README.md](jobs/README.md)
  Live price-ingestion job path. Houses the registered `stream_prices_polygon_ws` daemon — the
  primary live Polygon websocket price feed that populates `price_quotes_raw` / `price_quotes` / `prices`.
- [audit/README.md](audit/README.md)
  Tamper-evident SHA-256 hash chain over append-only ledger tables: canonical row serialization,
  the per-table append/chaining API, and a verifier plus `python -m engine.audit` CLI that write
  divergences to `audit_chain_findings`.
- [cache/README.md](cache/README.md)
  Redis hot-path cache for the allowlisted runtime tables. Write-through behind the Postgres storage
  facade with a circuit breaker that degrades to Postgres loaders; Redis is an accelerator, never the
  source of truth.
- [backtest/README.md](backtest/README.md)
  Reusable promotion-gate primitives: the CombinatorialPurgedKFold splitter (purge + embargo) and
  Bailey–de Prado deflated Sharpe (DSR) diagnostics consumed by training jobs and the strategy
  CPCV/gated backtest.
- [nlp/README.md](nlp/README.md)
  Offline text-feature primitives: FinBERT (`ProsusAI/finbert`) sentiment and sentence-transformer
  embedding encoders, a content-hash cache, and recency-weighted symbol-day aggregation feeding the
  NLP/news/filings/transcript feature groups.
- [causal/README.md](causal/README.md)
  Causal-plausibility diagnostics: HAC-robust Granger causality, optional DoWhy backdoor estimation,
  curated DAGs, and a monotone [0,1] composite score persisted to runtime storage. Advisory only.
- [artifacts/README.md](artifacts/README.md)
  Content-addressed (SHA256) blob store for model checkpoints and serialized payloads: 2-level
  sharded layout, alias/ref-count metadata, hash-verified reads, and an fsck verifier/GC. All blob
  (de)serialization routes through this package (AST-lint enforced).
- [rl/README.md](rl/README.md)
  Shadow-only portfolio RL research stack — Gym `PortfolioEnv`, PPO/SAC agents, env wrappers, and a
  kill-switch-gated shadow evaluator that logs advisory deltas to `rl_shadow_decisions`. No live
  order authority.

## High-Value Top-Level Files

- [app.py](app.py)
  General app entry/wiring module.
- [model_registry.py](model_registry.py)
  Registry and lookup logic for stored models.
- [training_guard.py](training_guard.py)
  Training safety and gating logic.

## Working Model

Think of `engine/` as a layered system:

1. `runtime` keeps the process alive and consistent.
2. `data` produces facts.
3. `strategy` converts facts into decisions.
4. `execution` turns allowed decisions into broker actions.
5. `api` exposes all of the above to the UI and operator tooling.

If you change a lower layer, assume upper layers will feel it.
