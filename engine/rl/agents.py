"""Project-friendly wrappers around PPO/SAC portfolio agents."""

from __future__ import annotations
import logging

import hashlib
import json
import os
import random
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np

from engine.rl.wrappers import clip_and_normalize_action

try:  # pragma: no cover - optional dependency path
    from stable_baselines3 import PPO, SAC

    STABLE_BASELINES3_AVAILABLE = True
except Exception:  # pragma: no cover - exercised in this checkout
    PPO = None  # type: ignore
    SAC = None  # type: ignore
    STABLE_BASELINES3_AVAILABLE = False


@dataclass
class AgentConfig:
    algo: str = "ppo"
    seed: int = 7
    total_timesteps: int = 1_000_000
    model_root: str = "models/rl"
    policy: str = "MlpPolicy"
    device: str = "cpu"
    learning_kwargs: Dict[str, Any] = field(default_factory=dict)


def set_global_seeds(seed: int) -> None:
    seed_i = int(seed)
    random.seed(seed_i)
    np.random.seed(seed_i)
    try:  # pragma: no cover - torch is optional at import time
        import torch

        torch.manual_seed(seed_i)
        torch.use_deterministic_algorithms(True, warn_only=True)
    except Exception:
        logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)


def checkpoint_dir(config: AgentConfig, *, ts_ms: Optional[int] = None) -> Path:
    stamp = int(ts_ms if ts_ms is not None else time.time() * 1000)
    return Path(config.model_root) / str(config.algo).lower() / str(stamp)


class _FallbackModel:
    """Deterministic fallback used when stable-baselines3 is unavailable."""

    def __init__(
        self,
        *,
        algo: str,
        action_dim: int,
        max_w: float,
        leverage_cap: float,
        seed: int,
        feature_dim_per_symbol: int = 1,
        style: str = "zero",
        bias: Optional[np.ndarray] = None,
    ):
        self.algo = str(algo).lower()
        self.action_dim = int(action_dim)
        self.max_w = float(max_w)
        self.leverage_cap = float(leverage_cap)
        self.seed = int(seed)
        self.feature_dim_per_symbol = int(max(1, feature_dim_per_symbol))
        self.style = str(style)
        self.bias = np.asarray(bias if bias is not None else np.zeros(self.action_dim), dtype=np.float32)

    def learn(self, total_timesteps: int, progress_bar: bool = False, **_: Any):
        del total_timesteps, progress_bar
        return self

    def predict(self, observation: Any, deterministic: bool = True):
        del deterministic
        obs = np.asarray(observation, dtype=np.float32).reshape(-1)
        if self.style == "momentum":
            signals = []
            for idx in range(self.action_dim):
                pos = idx * self.feature_dim_per_symbol
                value = float(obs[pos]) if pos < obs.shape[0] else float(self.bias[idx])
                if abs(value) <= 1e-12:
                    value = float(self.bias[idx])
                signals.append(value)
            raw = np.sign(np.asarray(signals, dtype=np.float32)) * np.float32(abs(self.max_w) * 0.5)
        elif self.style == "mean_reversion":
            signals = []
            for idx in range(self.action_dim):
                pos = idx * self.feature_dim_per_symbol
                value = float(obs[pos]) if pos < obs.shape[0] else float(self.bias[idx])
                signals.append(value)
            raw = -np.sign(np.asarray(signals, dtype=np.float32)) * np.float32(abs(self.max_w) * 0.5)
        else:
            raw = np.zeros(self.action_dim, dtype=np.float32)
        action = clip_and_normalize_action(raw, max_w=self.max_w, leverage_cap=self.leverage_cap)
        return action, None

    def save(self, path: str | Path) -> None:
        p = Path(path)
        if p.suffix:
            p.parent.mkdir(parents=True, exist_ok=True)
            target = p
        else:
            p.mkdir(parents=True, exist_ok=True)
            target = p / "model.json"
        payload = {
            "fallback": True,
            "algo": self.algo,
            "action_dim": int(self.action_dim),
            "max_w": float(self.max_w),
            "leverage_cap": float(self.leverage_cap),
            "seed": int(self.seed),
            "feature_dim_per_symbol": int(self.feature_dim_per_symbol),
            "style": str(self.style),
            "bias": [float(x) for x in self.bias.reshape(-1)],
        }
        target.write_text(json.dumps(payload, separators=(",", ":"), sort_keys=True), encoding="utf-8")

    @classmethod
    def load(cls, path: str | Path):
        p = Path(path)
        target = p if p.is_file() else p / "model.json"
        payload = json.loads(target.read_text(encoding="utf-8"))
        return cls(
            algo=str(payload.get("algo") or "ppo"),
            action_dim=int(payload.get("action_dim") or 1),
            max_w=float(payload.get("max_w") or 0.0),
            leverage_cap=float(payload.get("leverage_cap") or 0.0),
            seed=int(payload.get("seed") or 0),
            feature_dim_per_symbol=int(payload.get("feature_dim_per_symbol") or 1),
            style=str(payload.get("style") or "zero"),
            bias=np.asarray(payload.get("bias") or [], dtype=np.float32),
        )

    def parameter_bytes(self) -> bytes:
        payload = {
            "algo": self.algo,
            "action_dim": self.action_dim,
            "max_w": self.max_w,
            "leverage_cap": self.leverage_cap,
            "seed": self.seed,
            "feature_dim_per_symbol": self.feature_dim_per_symbol,
            "style": self.style,
            "bias": [float(x) for x in self.bias.reshape(-1)],
        }
        return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _fallback_from_env(algo: str, env: Any, seed: int) -> _FallbackModel:
    action_dim = int(getattr(getattr(env, "action_space", None), "shape", (1,))[0])
    max_w = float(getattr(env, "max_w", 0.35))
    leverage_cap = float(getattr(env, "leverage_cap", 1.0))
    feature_dim = int(max(1, len(getattr(env, "feature_ids", []) or [0])))
    style = "zero"
    bias = np.zeros(action_dim, dtype=np.float32)

    try:
        rets = np.asarray(getattr(env, "returns_matrix"), dtype=np.float64)
        if rets.ndim == 2 and rets.shape[0] > 3:
            lag_scores = []
            for col in range(min(action_dim, rets.shape[1])):
                x = rets[1:, col]
                y = rets[:-1, col]
                if np.std(x) > 1e-12 and np.std(y) > 1e-12:
                    lag_scores.append(float(np.corrcoef(x, y)[0, 1]))
                bias[col] = float(np.mean(x))
            avg_lag = float(np.mean(lag_scores)) if lag_scores else 0.0
            if avg_lag > 0.05 or float(np.mean(bias)) > 1e-7:
                style = "momentum"
            elif avg_lag < -0.05:
                style = "mean_reversion"
    except Exception:
        style = "zero"

    return _FallbackModel(
        algo=algo,
        action_dim=action_dim,
        max_w=max_w,
        leverage_cap=leverage_cap,
        seed=int(seed),
        feature_dim_per_symbol=feature_dim,
        style=style,
        bias=bias,
    )


class PortfolioAgent:
    def __init__(self, config: AgentConfig):
        self.config = config
        self.model: Any = None

    @property
    def algo(self) -> str:
        return str(self.config.algo).lower()

    def build_model(self, env: Any):
        set_global_seeds(int(self.config.seed))
        algo = self.algo
        if STABLE_BASELINES3_AVAILABLE:
            cls = PPO if algo == "ppo" else SAC
            if cls is None:
                raise RuntimeError(f"stable-baselines3 class unavailable for {algo}")
            self.model = cls(
                self.config.policy,
                env,
                seed=int(self.config.seed),
                device=str(self.config.device),
                verbose=0,
                **dict(self.config.learning_kwargs or {}),
            )
        else:
            if os.environ.get("RL_ALLOW_FALLBACK_AGENT", "0") != "1":
                raise RuntimeError("stable-baselines3 is required for PPO/SAC portfolio agents")
            self.model = _fallback_from_env(algo, env, int(self.config.seed))
        return self.model

    def learn(self, env: Any, total_timesteps: Optional[int] = None):
        if self.model is None:
            self.build_model(env)
        steps = int(total_timesteps if total_timesteps is not None else self.config.total_timesteps)
        self.model.learn(total_timesteps=steps, progress_bar=False)
        return self

    def predict(self, observation: Any, deterministic: bool = True) -> np.ndarray:
        if self.model is None:
            raise RuntimeError("agent model is not loaded")
        action, _ = self.model.predict(observation, deterministic=bool(deterministic))
        return np.asarray(action, dtype=np.float32)

    def save(self, path: str | Path) -> Path:
        if self.model is None:
            raise RuntimeError("agent model is not loaded")
        p = Path(path)
        p.mkdir(parents=True, exist_ok=True)
        if isinstance(self.model, _FallbackModel):
            self.model.save(p)
        else:
            self.model.save(str(p / "model.zip"))
        meta = {
            "algo": self.algo,
            "seed": int(self.config.seed),
            "stable_baselines3": bool(STABLE_BASELINES3_AVAILABLE and not isinstance(self.model, _FallbackModel)),
            "ts_ms": int(time.time() * 1000),
            "policy_hash32": policy_hash32(self.model),
        }
        (p / "metadata.json").write_text(json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8")
        return p

    @classmethod
    def load(cls, path: str | Path, *, env: Any = None, algo: str = "ppo", seed: int = 7):
        config = AgentConfig(algo=str(algo), seed=int(seed))
        agent = cls(config)
        p = Path(path)
        meta_path = p / "metadata.json"
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                config.algo = str(meta.get("algo") or config.algo)
                config.seed = int(meta.get("seed") or config.seed)
            except Exception:
                logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)
        if (p / "model.json").exists():
            if os.environ.get("RL_ALLOW_FALLBACK_AGENT", "0") != "1":
                raise RuntimeError("fallback RL checkpoints are disabled; train a stable-baselines3 checkpoint")
            agent.model = _FallbackModel.load(p)
        elif STABLE_BASELINES3_AVAILABLE:
            cls_model = PPO if str(config.algo).lower() == "ppo" else SAC
            if cls_model is None:
                raise RuntimeError(f"stable-baselines3 class unavailable for {config.algo}")
            agent.model = cls_model.load(str(p / "model.zip"), env=env, device=str(config.device))
        else:
            raise RuntimeError("stable-baselines3 is unavailable and no fallback checkpoint exists")
        return agent


class PPOPortfolioAgent(PortfolioAgent):
    def __init__(self, config: Optional[AgentConfig] = None):
        cfg = config or AgentConfig(algo="ppo")
        cfg.algo = "ppo"
        super().__init__(cfg)


class SACPortfolioAgent(PortfolioAgent):
    def __init__(self, config: Optional[AgentConfig] = None):
        cfg = config or AgentConfig(algo="sac")
        cfg.algo = "sac"
        super().__init__(cfg)


def make_agent(algo: str = "ppo", **kwargs: Any) -> PortfolioAgent:
    config = AgentConfig(algo=str(algo).lower(), **kwargs)
    if config.algo == "sac":
        return SACPortfolioAgent(config)
    if config.algo != "ppo":
        raise ValueError(f"unsupported RL portfolio algo: {algo}")
    return PPOPortfolioAgent(config)


def train_agent(env: Any, *, algo: str = "ppo", total_timesteps: int = 1_000_000, seed: int = 7, model_root: str = "models/rl"):
    agent = make_agent(algo=algo, total_timesteps=int(total_timesteps), seed=int(seed), model_root=str(model_root))
    agent.learn(env, total_timesteps=int(total_timesteps))
    out_dir = checkpoint_dir(agent.config)
    agent.save(out_dir)
    return agent, out_dir


def load_agent(path: str | Path, *, env: Any = None, algo: str = "ppo", seed: int = 7) -> PortfolioAgent:
    return PortfolioAgent.load(path, env=env, algo=algo, seed=seed)


def latest_checkpoint(*, algo: str = "ppo", model_root: str = "models/rl") -> Optional[Path]:
    root = Path(model_root) / str(algo).lower()
    if not root.exists():
        return None
    candidates = [p for p in root.iterdir() if p.is_dir() and ((p / "model.json").exists() or (p / "model.zip").exists())]
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.name)
    return candidates[-1]


def policy_hash32(model: Any) -> str:
    if isinstance(model, _FallbackModel):
        payload = model.parameter_bytes()
    else:
        parts: list[bytes] = []
        try:
            params = model.get_parameters()
            for name in sorted(params.keys()):
                value = params[name]
                if isinstance(value, dict):
                    for sub in sorted(value.keys()):
                        arr = _to_numpy(value[sub])
                        parts.append(str(name).encode("utf-8") + b":" + str(sub).encode("utf-8") + b"=" + arr.tobytes())
                else:
                    arr = _to_numpy(value)
                    parts.append(str(name).encode("utf-8") + b"=" + arr.tobytes())
        except Exception:
            parts.append(repr(model).encode("utf-8"))
        payload = b"|".join(parts)
    return hashlib.sha256(payload).hexdigest()[:8]


def _to_numpy(value: Any) -> np.ndarray:
    try:
        import torch

        if isinstance(value, torch.Tensor):
            return value.detach().cpu().numpy().astype(np.float32, copy=False)
    except Exception:
        logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)
    return np.asarray(value, dtype=np.float32)
