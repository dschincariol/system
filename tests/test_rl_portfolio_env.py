from __future__ import annotations

import math

import numpy as np

from engine.rl.portfolio_env import PortfolioEnv, PortfolioEnvConfig


class ZeroCost:
    def cost_bps(self, **kwargs):
        return 0.0


def _noop_simulator(**kwargs):
    return {"ok": True, "status": "simulated", "orders": list(kwargs.get("orders") or [])}


def test_portfolio_env_gym_contract_and_risk_clip():
    calls = {"risk": 0, "sim": 0}

    def risk_overlay(*, desired, state, now_ms):
        calls["risk"] += 1
        gross = sum(abs(float(row["weight"])) for row in desired.values())
        if gross > 0.50:
            scale = 0.50 / gross
            for row in desired.values():
                row["weight"] = float(row["weight"]) * scale
        return desired, {"gross_cap": 0.50, "called": True}

    def simulator(**kwargs):
        calls["sim"] += 1
        return _noop_simulator(**kwargs)

    env = PortfolioEnv(
        PortfolioEnvConfig(
            universe=["AAA", "BBB"],
            price_history={"AAA": [100, 101, 102, 103], "BBB": [100, 99, 98, 97]},
            lookback=1,
            episode_length=2,
            max_w=1.0,
            leverage_cap=1.0,
            risk_overlay=risk_overlay,
            simulator=simulator,
            feature_provider=lambda symbol, ts, ids: {fid: 0.0 for fid in ids},
        )
    )

    obs, info = env.reset(seed=11, options={"start_index": 1})
    assert env.observation_space.contains(obs)
    assert "obs_hash" in info

    obs2, reward, terminated, truncated, step_info = env.step(np.asarray([1.0, 1.0], dtype=np.float32))
    assert env.observation_space.contains(obs2)
    assert isinstance(reward, float)
    assert terminated is False
    assert truncated in {False, True}
    assert calls == {"risk": 1, "sim": 1}
    assert step_info["live_gate_consulted"] is True
    assert sum(abs(v) for v in step_info["risked_weights"].values()) <= 0.500001
    assert step_info["risk_clip"] > 0.0


def test_reward_calculation_on_hand_computed_trajectory():
    env = PortfolioEnv(
        PortfolioEnvConfig(
            universe=["AAA"],
            price_history={"AAA": [100.0, 110.0, 121.0]},
            lookback=1,
            episode_length=1,
            max_w=1.0,
            leverage_cap=1.0,
            lambda_vol=0.0,
            lambda_dd=0.0,
            risk_clip_penalty=0.0,
            risk_overlay=lambda desired, state, now_ms: (desired, {"called": True}),
            simulator=_noop_simulator,
            feature_provider=lambda symbol, ts, ids: {fid: 0.0 for fid in ids},
        )
    )
    env.cost_model = ZeroCost()
    obs, _ = env.reset(seed=3, options={"start_index": 1})
    _obs2, reward, _terminated, _truncated, info = env.step(np.asarray([1.0], dtype=np.float32))

    assert math.isclose(float(info["pnl"]), 0.10, rel_tol=1e-6, abs_tol=1e-6)
    assert math.isclose(float(info["cost"]), 0.0, abs_tol=1e-12)
    assert math.isclose(float(reward), 0.10, rel_tol=1e-6, abs_tol=1e-6)
