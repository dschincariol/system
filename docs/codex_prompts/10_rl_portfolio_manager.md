# Codex Prompt 10 — RL Portfolio Manager (PPO/SAC) Trained Against the Simulator

You are working in a Python systematic trading system that already has
a non-trivial reinforcement-learning surface: `engine/strategy/rl_strategy_policy.py`,
`engine/execution/train_rl_strategy_policy.py`, and an explicitly
shadow-only `train_size_policy.py` / `train_drawdown_policy.py`. Today
the RL components are scoped to local sub-decisions (sizing,
suppression intensity, drawdown response) and their training data is
collected mostly opportunistically. There is no closed-loop **portfolio-
level** RL agent trained end-to-end against `broker_sim.py` with full
risk gating in the loop.

This prompt builds that agent as a research component: a **PPO and SAC
portfolio manager** trained in a Gym-compatible environment that wraps
the existing simulator, the existing 6-layer risk overlay, and the
existing Almgren-Chriss cost model from prompt 02. The agent ships in
**shadow only**; live execution remains under the supervised path.

## Goal

1. A Gym-compatible environment `PortfolioEnv` whose `step` calls the
   real `broker_sim` and the real risk engines so the agent learns
   *under production risk constraints*.
2. Two trained agents: PPO (default) and SAC (continuous-action
   alternative), via `stable-baselines3`.
3. A shadow-evaluation loop that runs the trained policy in lockstep
   with the live decision pipeline and logs the deltas.
4. Clear, non-bypassable guardrails so RL outputs never reach the live
   broker router unless an explicit human promotion path is taken.

## Files to read first (read-only)

- `engine/strategy/portfolio.py` — current portfolio constructor; the
  RL agent's action interpretation must be consistent with it.
- `engine/strategy/portfolio_risk_gate.py` — pre-trade gating.
- `engine/risk/portfolio_risk_engine.py` — 6-layer overlay.
- `engine/risk/monte_carlo_risk_engine.py` — MC stress.
- `engine/strategy/position_sizing.py` — current sizing surface.
- `engine/strategy/rl_strategy_policy.py` — existing RL surface; this
  prompt extends, it does not replace.
- `engine/execution/train_rl_strategy_policy.py`,
  `engine/execution/train_size_policy.py`,
  `engine/execution/train_drawdown_policy.py` — existing training
  patterns.
- `engine/execution/broker_sim.py` — simulator that becomes the
  training environment.
- `engine/execution/cost_models/almgren_chriss.py` (after prompt 02) —
  cost model used inside the env.
- `engine/execution/broker_router.py` — to verify the RL agent has no
  path into it (regression test).
- `engine/execution/kill_switch.py` — additional guardrail.

## Files to create

- `engine/rl/__init__.py`
- `engine/rl/portfolio_env.py` — Gym `Env`. Observation: feature vector
  from `feature_registry` + current positions + cash + recent realized
  vol. Action: target-weight vector across the universe (continuous,
  bounded by `[-max_w, max_w]`, summed-to-leverage cap). Reward:
  PnL minus a transaction-cost term minus a risk-overlay penalty.
- `engine/rl/wrappers.py` — observation normalization, action
  clipping to risk bounds, reward shaping.
- `engine/rl/agents.py` — thin wrappers over `stable-baselines3` PPO
  and SAC with project-friendly checkpoint paths and seeding.
- `engine/rl/shadow_runner.py` — runs the trained policy in parallel
  with the live decision pipeline; logs `(live_action, rl_action,
  delta)` to a new table.
- `engine/strategy/jobs/train_rl_portfolio.py` — training job.
- `engine/strategy/jobs/run_rl_shadow.py` — shadow-evaluation job.
- `tests/test_rl_portfolio_env.py`
- `tests/test_rl_agents.py`
- `tests/test_rl_shadow_runner.py`
- `tests/test_rl_no_live_path.py` — guardrail test asserting no
  `engine.rl.*` symbol is reachable from `broker_router.submit_order`.

## Files to modify

- `engine/runtime/storage.py` — add tables:
  - `rl_training_runs(id INTEGER PK, ts INTEGER, algo TEXT,
    config_json TEXT, total_steps INTEGER, eval_reward REAL,
    artifact_path TEXT)`
  - `rl_shadow_decisions(ts INTEGER, symbol TEXT,
    live_weight REAL, rl_weight REAL, delta REAL,
    obs_hash TEXT, PRIMARY KEY(ts, symbol))`
- `engine/runtime/job_registry.py` — register the two new jobs.

## Implementation plan

1. **Environment.** `PortfolioEnv` is a daily-step env by default.
   `reset()` rolls back to a random start date inside the configured
   training window. `step(action)`:
   - normalize action to weights summing to ≤ leverage_cap;
   - feed proposed weights through `portfolio_risk_gate` and the
     6-layer overlay — **the agent learns under the live gate**;
   - simulate fills via `broker_sim` with Almgren-Chriss costs;
   - compute reward: realized PnL − cost − `lambda_vol * portfolio_vol`
     − `lambda_dd * max(0, drawdown - threshold)`.
2. **Training.** PPO with default hyperparameters, 1M steps for
   smoke; SAC for continuous-action ablation. Seeds fixed. Best
   checkpoint persisted under `models/rl/<algo>/<ts>/`.
3. **Evaluation.** A held-out window not seen in training; report
   Sharpe, Sortino, max drawdown, turnover, average leverage.
4. **Shadow runner.** Periodically loads the latest checkpoint,
   observes the same state the live decision pipeline does,
   computes `rl_action`, records delta vs `live_action`. **Never
   submits orders.**
5. **Guardrails.**
   - `engine.rl` module never imports `engine.execution.broker_router`.
     A unit test asserts this with `import ast`.
   - `broker_router.submit_order` does not accept any input that
     traces back to an `engine.rl` symbol; this is enforced by
     stamping a `source` field on every order and rejecting
     `source.startswith('rl.')` in the router.
   - `kill_switch` integration: shadow loop respects the kill switch
     even though it doesn't trade; if the switch is tripped, shadow
     pauses to avoid baseline drift.

## Acceptance criteria

- [ ] `PortfolioEnv` conforms to Gymnasium API: `reset()`, `step()`,
      `observation_space`, `action_space`, `close()`.
- [ ] On a synthetic flat market, PPO converges to (near) zero
      turnover within 100k steps; on a synthetic momentum market, it
      learns a positive-Sharpe policy. Both asserted with seeds.
- [ ] Risk overlay is consulted on every step; an action that violates
      a hard limit is clipped before the simulator sees it (test
      asserts the clipping with a deliberate over-leverage action).
- [ ] No code path from `engine.rl` reaches `broker_router.submit_order`
      (asserted by `tests/test_rl_no_live_path.py`).
- [ ] Shadow runner writes one row per `(ts, symbol)` to
      `rl_shadow_decisions` and never logs an outbound order.
- [ ] Training is reproducible: the same seed produces
      bit-identical policy weights to a 32-bit hash.

## Test plan

- `tests/test_rl_portfolio_env.py` — Gym contract; risk-clip behavior;
  reward calculation on a hand-computed two-step trajectory.
- `tests/test_rl_agents.py` — PPO and SAC instantiate; one micro-
  training run completes; checkpoint round-trip yields identical
  `predict()`.
- `tests/test_rl_shadow_runner.py` — given a fake live-decision
  stream, shadow runner records the expected deltas; throws if it
  ever attempts to submit.
- `tests/test_rl_no_live_path.py` — AST scan of `engine/rl/`; assert
  no import of `broker_router`. Symmetric assertion in router.

Run: `pytest -q tests/test_rl_portfolio_env.py tests/test_rl_agents.py
tests/test_rl_shadow_runner.py tests/test_rl_no_live_path.py`

## Out of scope

- Do not promote the RL agent to live trading. Promotion is a
  separate, explicit process with paper-trading soak time.
- Do not add multi-asset multi-currency hedging dimensions. Single
  base currency, equity universe only.
- Do not introduce model-based RL (Dreamer, MuZero). PPO and SAC are
  sufficient for the first cut.
- Do not bypass any existing risk gate "for the agent's benefit." The
  agent must learn under the same constraints production faces — that
  is the entire point.

## Hard prerequisite

Prompt 02 (Almgren-Chriss costs) **must be merged first**. Without
realistic transaction costs in the env, the agent learns a high-
turnover policy that will not generalize.
