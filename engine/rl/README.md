# RL Subsystem

The `engine/rl/` package owns the shadow-only portfolio reinforcement-learning research stack: a Gym-compatible portfolio environment, PPO/SAC agent wrappers around stable-baselines3, env preprocessing wrappers, and a shadow evaluator that logs advisory target-weight deltas. It is consumed by the `run_rl_shadow` job (`engine/strategy/jobs/run_rl_shadow.py`) and the `train_rl_portfolio` training job. Nothing in this package has live order authority — its sole runtime output is the advisory `rl_shadow_decisions` table.

## Safety Boundary

This subsystem is SHADOW-ONLY and has NO live order authority. This is a hard boundary, verified against the code:

- `shadow_runner.py` deliberately never imports the broker router (its module docstring states this explicitly). `run_once` loads a policy, computes RL target weights, diffs them against the live portfolio weights, and writes rows to `rl_shadow_decisions`. It places no orders.
- Before logging, `run_once` consults the production kill switch via `engine.execution.kill_switch.execution_allowed(... model_id="rl_portfolio_shadow")`; if blocked or on any kill-switch error it returns `status="paused_kill_switch"` (or fails closed) and writes nothing.
- `PortfolioEnv` (in `portfolio_env.py`) is a daily-step target-weight environment whose docstring is "...environment for shadow-only portfolio RL." Every target and order it emits is tagged `shadow_only: True` (in `_weights_to_targets` the `reason`/`explain_json`, and in `_orders_from_weights` the per-order `explain`). The env routes proposed weights through the real `portfolio_risk_engine`, `portfolio_risk_gate`, Monte Carlo refresh, and `broker_sim.apply_new_portfolio_orders` — but `broker_sim` is the paper-execution simulator, not a live broker, so the env never touches a real venue. Book keys are prefixed `shadow_rl_`.
- The contract is the standard model-vs-runtime split: RL proposes intent only; the runtime owns all safety gates and execution. No path in this package reaches a live broker.

## Files

- [portfolio_env.py](portfolio_env.py)
  Gym-compatible daily-step, target-weight `PortfolioEnv` (config `PortfolioEnvConfig`); builds observations from feature snapshots and portfolio state, applies the real risk engine/gate and paper simulator, and computes a PnL-minus-cost-minus-risk-penalty reward. Falls back to `SimpleBox` and synthetic features when gymnasium/feature registry are unavailable.
- [agents.py](agents.py)
  PPO/SAC agent wrappers (`PortfolioAgent`, `PPOPortfolioAgent`, `SACPortfolioAgent`) over stable-baselines3, plus checkpoint save/load, `latest_checkpoint`, `train_agent`, and `policy_hash32`. A deterministic `_FallbackModel` is used only when stable-baselines3 is absent and `RL_ALLOW_FALLBACK_AGENT=1`.
- [shadow_runner.py](shadow_runner.py)
  Shadow evaluator (`RLShadowRunner`, `run_shadow_once`). Owns the `rl_shadow_decisions` schema and writes advisory per-symbol live-vs-RL weight deltas plus optional off-policy-evaluation fields. Kill-switch gated; never imports the broker router.
- [wrappers.py](wrappers.py)
  Gym env wrappers — `ObservationNormalizer`, `ActionRiskClipper`, `RewardShaper` — and the shared `clip_and_normalize_action` helper that clips per-asset weights to `max_w` and scales gross exposure to `leverage_cap`.

`__init__.py` re-exports `PortfolioEnv` and `PortfolioEnvConfig`.

## Key Tables / Outputs

- `rl_shadow_decisions` (classified as a decision series in `engine/runtime/schema/table_classification.py`): primary key `(ts, symbol)`; columns include `model_name` (default `rl_portfolio_shadow`), `candidate_type` (default `rl`), `live_weight`, `rl_weight`, `delta`, `obs_hash`, the OPE fields (`behavior_propensity`, `target_propensity`, `outcome`, `logged_model_estimate`, `target_model_estimate`), and `meta_json`. The schema is created/migrated idempotently by `ensure_shadow_schema`. This is advisory data only — it is not an order feed.

## Defaults

- `max_w = 0.35` per-asset weight cap; `leverage_cap = 1.0` gross exposure cap; `seed = 7`; default algo `ppo`.
- Default model root is `default_local_models_dir()/rl`; checkpoints are written per-algo under a millisecond-stamped directory and selected by lexicographic name via `latest_checkpoint`.
- `PortfolioEnvConfig` defaults: `episode_length = 252`, `lookback = 20`, `drawdown_threshold = 0.08`, reward penalties `lambda_vol = 0.10` / `lambda_dd = 1.0` / `risk_clip_penalty = 0.01`, `adv_notional = 1_000_000.0`, `model_id = "rl_portfolio_shadow"`, `strict_live_risk = True`.

## Configuration Families

- `RL_PORTFOLIO_*` — operator-facing knobs for the shadow job (`RL_PORTFOLIO_ALGO`, `RL_PORTFOLIO_SYMBOLS`, `RL_PORTFOLIO_MODEL_ROOT`, `RL_PORTFOLIO_CHECKPOINT`, `RL_PORTFOLIO_MAX_W`, `RL_PORTFOLIO_LEVERAGE_CAP`, `RL_PORTFOLIO_SEED`).
- `RL_ALLOW_FALLBACK_AGENT`, `RL_ALLOW_SIMPLE_GYM_FALLBACK`, `RL_PORTFOLIO_VALIDATE_FEATURE_IDS` — guarded escape hatches that default off; the first two gate use of the dependency-free fallbacks.
