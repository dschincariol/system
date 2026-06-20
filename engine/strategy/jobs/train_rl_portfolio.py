"""Train a shadow-only portfolio RL policy against the simulator."""

from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any, Dict, Sequence

import numpy as np

from engine.rl.agents import policy_hash32, train_agent
from engine.rl.portfolio_env import PortfolioEnv, PortfolioEnvConfig
from engine.runtime import storage
from engine.runtime.platform import default_local_models_dir


def _csv(value: str, default: Sequence[str]) -> list[str]:
    parts = [p.strip().upper() for p in str(value or "").split(",") if p.strip()]
    return parts or [str(x).upper() for x in default]


def _evaluate(env: PortfolioEnv, agent, *, steps: int, seed: int) -> Dict[str, float]:
    obs, _ = env.reset(seed=int(seed))
    rewards: list[float] = []
    leverages: list[float] = []
    turnovers: list[float] = []
    equities: list[float] = [1.0]
    for _ in range(int(max(1, steps))):
        action = agent.predict(obs, deterministic=True)
        obs, reward, _terminated, truncated, info = env.step(action)
        rewards.append(float(reward))
        turnovers.append(float(info.get("turnover", 0.0) or 0.0))
        leverages.append(float(sum(abs(v) for v in (info.get("risked_weights") or {}).values())))
        equities.append(float(info.get("equity", equities[-1]) or equities[-1]))
        if truncated:
            break

    arr = np.asarray(rewards, dtype=np.float64)
    downside = arr[arr < 0.0]
    sharpe = float(np.mean(arr) / (np.std(arr) + 1e-12) * np.sqrt(252.0)) if arr.size else 0.0
    sortino = float(np.mean(arr) / (np.std(downside) + 1e-12) * np.sqrt(252.0)) if downside.size else sharpe
    peak = -1e30
    max_dd = 0.0
    for eq in equities:
        peak = max(peak, float(eq))
        if peak > 0.0:
            max_dd = max(max_dd, 1.0 - float(eq) / peak)
    return {
        "eval_reward": float(np.sum(arr)) if arr.size else 0.0,
        "sharpe": float(sharpe),
        "sortino": float(sortino),
        "max_drawdown": float(max_dd),
        "turnover": float(np.mean(turnovers)) if turnovers else 0.0,
        "average_leverage": float(np.mean(leverages)) if leverages else 0.0,
    }


def _record_training_run(*, algo: str, config: Dict[str, Any], total_steps: int, eval_reward: float, artifact_path: str) -> None:
    storage.init_rl_portfolio_tables()
    con = storage.connect()
    try:
        con.execute(
            """
            INSERT INTO rl_training_runs(ts, algo, config_json, total_steps, eval_reward, artifact_path)
            VALUES (?,?,?,?,?,?)
            """,
            (
                int(time.time() * 1000),
                str(algo).lower(),
                json.dumps(config, separators=(",", ":"), sort_keys=True),
                int(total_steps),
                float(eval_reward),
                str(artifact_path),
            ),
        )
        con.commit()
    finally:
        con.close()


def main(argv: Sequence[str] | None = None) -> Dict[str, Any]:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--algo", default=os.environ.get("RL_PORTFOLIO_ALGO", "ppo"), choices=["ppo", "sac"])
    parser.add_argument("--steps", type=int, default=int(os.environ.get("RL_PORTFOLIO_TOTAL_STEPS", "1000000")))
    parser.add_argument("--seed", type=int, default=int(os.environ.get("RL_PORTFOLIO_SEED", "7")))
    parser.add_argument("--symbols", default=os.environ.get("RL_PORTFOLIO_SYMBOLS", "SPY,AAPL,MSFT"))
    parser.add_argument(
        "--model-root",
        default=os.environ.get("RL_PORTFOLIO_MODEL_ROOT", str((default_local_models_dir() / "rl").resolve())),
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    universe = _csv(args.symbols, ["SPY", "AAPL", "MSFT"])
    feature_ids = _csv(os.environ.get("RL_PORTFOLIO_FEATURE_IDS", "price.pct_ret_1d,price.momentum_1d,price.rv_20"), [])
    env_config = PortfolioEnvConfig(
        universe=universe,
        feature_ids=feature_ids,
        episode_length=int(os.environ.get("RL_PORTFOLIO_EPISODE_LENGTH", "252")),
        lookback=int(os.environ.get("RL_PORTFOLIO_LOOKBACK", "20")),
        max_w=float(os.environ.get("RL_PORTFOLIO_MAX_W", "0.35")),
        leverage_cap=float(os.environ.get("RL_PORTFOLIO_LEVERAGE_CAP", "1.0")),
        seed=int(args.seed),
    )
    env = PortfolioEnv(env_config)
    agent, artifact_dir = train_agent(
        env,
        algo=str(args.algo),
        total_timesteps=int(args.steps),
        seed=int(args.seed),
        model_root=str(args.model_root),
    )
    metrics = _evaluate(
        env,
        agent,
        steps=int(os.environ.get("RL_PORTFOLIO_EVAL_STEPS", "252")),
        seed=int(args.seed) + 1,
    )
    model_hash = policy_hash32(agent.model)
    config_payload = {
        "algo": str(args.algo),
        "seed": int(args.seed),
        "universe": universe,
        "feature_ids": feature_ids,
        "env": env_config.__dict__,
        "policy_hash32": str(model_hash),
    }
    _record_training_run(
        algo=str(args.algo),
        config=config_payload,
        total_steps=int(args.steps),
        eval_reward=float(metrics.get("eval_reward", 0.0)),
        artifact_path=str(artifact_dir),
    )
    result = {
        "ok": True,
        "algo": str(args.algo),
        "artifact_path": str(artifact_dir),
        "policy_hash32": str(model_hash),
        "metrics": metrics,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return result


if __name__ == "__main__":
    main()
