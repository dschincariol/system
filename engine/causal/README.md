# Causal Subsystem

The `engine/causal/` package scores the causal plausibility of a feature for a forward-return target. It owns the pure diagnostics — multivariate Granger causality, optional DoWhy backdoor estimation, a curated DAG type, and a monotone `[0, 1]` composite score — plus the runtime-storage persistence helpers for those scores. The package is orchestrated by the `engine/strategy/jobs/causal_scoring.py` job; its persisted scores are consumed by `engine/strategy/promotion_audit.py` (to annotate promotion reasons), `engine/strategy/jobs/train_ensemble.py`, and the `/api/causal/scores` dashboard endpoint.

## Files

- [granger.py](granger.py)
  Multivariate Granger causality test in a VAR. Lag length is selected by BIC over `1..max_lag`; the reported statistic is a HAC (Newey-West) robust Wald statistic divided by the number of restrictions, evaluated against an F distribution. Returns a `GrangerResult` (p-value, selected lag, F-stat, HAC lag, n_obs, BIC).
- [dowhy_runner.py](dowhy_runner.py)
  Optional DoWhy integration. `run_dowhy` performs backdoor identification and `backdoor.linear_regression` effect estimation against a curated DAG, with a bootstrap refuter. `dowhy` and `pandas` are imported lazily; missing dependencies or columns produce a `skipped`/`failed` `DoWhyResult` decision rather than raising.
- [scores.py](scores.py)
  Composite scoring and persistence. `causal_score` combines the Granger p-value and a DoWhy t-statistic into `[0, 1]` via `0.5*sigmoid(-log10(p) - 1.5) + 0.5*sigmoid(t - 2.0)`. Defines the `causal_scores` and `causal_dags` table schema and the upsert/latest/load helpers over the runtime storage connection.
- [dag.py](dag.py)
  The `CausalDAG` dataclass: a JSON-serializable, cycle-checked curated DAG (nodes, edges, treatment, outcome, confounders) with `to_dot` Graphviz export used by the DoWhy runner.

## Key Tables / Outputs

- `causal_scores` — one row per `(feature, target, "window", ts)` carrying `granger_p`, `granger_lag`, optional `dowhy_effect`/`dowhy_p`, the composite `score`, and a `decision` string (e.g. `granger_only`, `estimated`, `insufficient_data`, `failed_granger`). Created here and by migration `engine/runtime/schema/migrations/0013_causal.py`.
- `causal_dags` — curated DAGs persisted by `name` as serialized JSON.

`latest_causal_scores` returns the most recent score per feature (preserving `None` for features without a row) and is the read path used to augment promotion-audit reasons.

## Scope Note

This package provides the diagnostics and persistence primitives only. Series extraction, DAG selection, the per-feature loop, and the `dowhy_t` derivation (from effect / standard error) live in the `causal_scoring` job, not here. The scores are advisory diagnostics for governance and ensemble inputs; they do not by themselves gate promotion or grant order authority.
