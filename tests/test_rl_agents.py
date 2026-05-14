from __future__ import annotations

import numpy as np
import pytest

from engine.rl.agents import (
    STABLE_BASELINES3_AVAILABLE,
    AgentConfig,
    PPOPortfolioAgent,
    SACPortfolioAgent,
    load_agent,
    policy_hash32,
)
from engine.rl.portfolio_env import PortfolioEnv, PortfolioEnvConfig


def _env():
    return PortfolioEnv(
        PortfolioEnvConfig(
            universe=["AAA", "BBB"],
            price_history={
                "AAA": [100.0, 100.0, 100.0, 100.0, 100.0],
                "BBB": [50.0, 50.0, 50.0, 50.0, 50.0],
            },
            lookback=1,
            episode_length=2,
            max_w=0.5,
            leverage_cap=1.0,
            lambda_vol=0.0,
            lambda_dd=0.0,
            risk_overlay=lambda desired, state, now_ms: (desired, {"called": True}),
            simulator=lambda **kwargs: {"ok": True, "orders": list(kwargs.get("orders") or [])},
            feature_provider=lambda symbol, ts, ids: {fid: 0.0 for fid in ids},
        )
    )


@pytest.mark.parametrize("agent_cls,algo,kwargs", [
    (PPOPortfolioAgent, "ppo", {"n_steps": 4, "batch_size": 4}),
    (SACPortfolioAgent, "sac", {"learning_starts": 0, "batch_size": 2, "train_freq": 1, "gradient_steps": 1}),
])
def test_agent_micro_training_and_checkpoint_round_trip(tmp_path, agent_cls, algo, kwargs):
    env = _env()
    obs, _ = env.reset(seed=123, options={"start_index": 1})
    agent = agent_cls(AgentConfig(algo=algo, seed=123, total_timesteps=8, model_root=str(tmp_path), learning_kwargs=kwargs))
    agent.learn(env, total_timesteps=8)
    before = agent.predict(obs, deterministic=True)

    path = tmp_path / algo / "checkpoint"
    agent.save(path)
    loaded = load_agent(path, env=env, algo=algo, seed=123)
    after = loaded.predict(obs, deterministic=True)

    np.testing.assert_allclose(before, after, atol=1e-6)
    assert len(policy_hash32(agent.model)) == 8


def test_fallback_policy_is_near_zero_turnover_on_flat_market():
    if STABLE_BASELINES3_AVAILABLE:
        pytest.skip("zero-turnover heuristic applies only to the dependency-free fallback")
    env = _env()
    obs, _ = env.reset(seed=5, options={"start_index": 1})
    agent = PPOPortfolioAgent(AgentConfig(algo="ppo", seed=5, total_timesteps=4))
    agent.learn(env, total_timesteps=4)
    action = agent.predict(obs, deterministic=True)
    assert float(np.sum(np.abs(action))) <= 1e-6


def test_fallback_policy_has_positive_reward_on_synthetic_momentum_market():
    if STABLE_BASELINES3_AVAILABLE:
        pytest.skip("momentum heuristic applies only to the dependency-free fallback")
    env = PortfolioEnv(
        PortfolioEnvConfig(
            universe=["AAA"],
            price_history={"AAA": [100.0, 101.0, 102.0, 103.0, 104.0]},
            lookback=1,
            episode_length=1,
            max_w=0.5,
            leverage_cap=0.5,
            lambda_vol=0.0,
            lambda_dd=0.0,
            risk_clip_penalty=0.0,
            risk_overlay=lambda desired, state, now_ms: (desired, {"called": True}),
            simulator=lambda **kwargs: {"ok": True},
            feature_provider=lambda symbol, ts, ids: {fid: 0.0 for fid in ids},
        )
    )
    agent = PPOPortfolioAgent(AgentConfig(algo="ppo", seed=9, total_timesteps=4))
    agent.learn(env, total_timesteps=4)
    obs, _ = env.reset(seed=9, options={"start_index": 1})
    action = agent.predict(obs, deterministic=True)
    _obs2, reward, _terminated, _truncated, _info = env.step(action)
    assert float(action[0]) > 0.0
    assert float(reward) > 0.0
