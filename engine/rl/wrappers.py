"""Small Gym-compatible wrappers for portfolio RL research environments."""

from __future__ import annotations

from collections import deque
from typing import Any, Optional

import numpy as np

try:  # pragma: no cover - exercised when optional dependency is installed
    import gymnasium as gym
    from gymnasium import spaces
except Exception:  # pragma: no cover - local compatibility fallback
    gym = None  # type: ignore
    spaces = None  # type: ignore


def clip_and_normalize_action(action: Any, *, max_w: float, leverage_cap: float) -> np.ndarray:
    """Clip per-asset weights and scale gross exposure to the leverage cap."""
    arr = np.asarray(action, dtype=np.float32).reshape(-1)
    max_w_f = abs(float(max_w))
    if max_w_f > 0.0:
        arr = np.clip(arr, -max_w_f, max_w_f)
    else:
        arr = np.zeros_like(arr, dtype=np.float32)

    cap = max(0.0, float(leverage_cap))
    gross = float(np.sum(np.abs(arr)))
    if cap >= 0.0 and gross > cap and gross > 1e-12:
        arr = arr * np.float32(cap / gross)
    return arr.astype(np.float32, copy=False)


class _WrapperBase:
    def __init__(self, env: Any):
        self.env = env
        self.action_space = getattr(env, "action_space", None)
        self.observation_space = getattr(env, "observation_space", None)

    def reset(self, *args: Any, **kwargs: Any):
        return self.env.reset(*args, **kwargs)

    def step(self, action: Any):
        return self.env.step(action)

    def close(self) -> None:
        if hasattr(self.env, "close"):
            self.env.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self.env, name)


class ObservationNormalizer(gym.ObservationWrapper if gym is not None else _WrapperBase):  # type: ignore[misc]
    """Online observation normalization with bounded output."""

    def __init__(self, env: Any, *, epsilon: float = 1e-8, clip: float = 10.0):
        super().__init__(env)
        self.epsilon = float(epsilon)
        self.clip = float(clip)
        shape = tuple(getattr(getattr(env, "observation_space", None), "shape", ()) or ())
        self.count = 0
        self.mean = np.zeros(shape, dtype=np.float64)
        self.m2 = np.zeros(shape, dtype=np.float64)

    def observation(self, observation: Any):  # gymnasium hook
        return self._normalize(observation)

    def reset(self, *args: Any, **kwargs: Any):
        obs, info = self.env.reset(*args, **kwargs)
        return self._normalize(obs), info

    def step(self, action: Any):
        obs, reward, terminated, truncated, info = self.env.step(action)
        return self._normalize(obs), reward, terminated, truncated, info

    def _normalize(self, observation: Any) -> np.ndarray:
        obs = np.asarray(observation, dtype=np.float32)
        self.count += 1
        delta = obs.astype(np.float64) - self.mean
        self.mean += delta / float(self.count)
        delta2 = obs.astype(np.float64) - self.mean
        self.m2 += delta * delta2
        if self.count < 2:
            return obs
        var = self.m2 / float(max(1, self.count - 1))
        normed = (obs - self.mean.astype(np.float32)) / np.sqrt(var.astype(np.float32) + self.epsilon)
        return np.clip(normed, -self.clip, self.clip).astype(np.float32)


class ActionRiskClipper(gym.ActionWrapper if gym is not None else _WrapperBase):  # type: ignore[misc]
    """Clips actions to per-symbol and gross-leverage limits before env.step."""

    def __init__(self, env: Any, *, max_w: Optional[float] = None, leverage_cap: Optional[float] = None):
        super().__init__(env)
        self.max_w = float(max_w if max_w is not None else getattr(env, "max_w", 0.0))
        self.leverage_cap = float(
            leverage_cap if leverage_cap is not None else getattr(env, "leverage_cap", self.max_w)
        )

    def action(self, action: Any):  # gymnasium hook
        return clip_and_normalize_action(action, max_w=self.max_w, leverage_cap=self.leverage_cap)

    def step(self, action: Any):
        return self.env.step(self.action(action))


class RewardShaper(gym.RewardWrapper if gym is not None else _WrapperBase):  # type: ignore[misc]
    """Applies optional reward scaling and rolling baseline subtraction."""

    def __init__(self, env: Any, *, scale: float = 1.0, baseline_window: int = 0):
        super().__init__(env)
        self.scale = float(scale)
        self.baseline_window = int(max(0, baseline_window))
        self._recent = deque(maxlen=self.baseline_window or 1)

    def reward(self, reward: float):  # gymnasium hook
        shaped = float(reward)
        if self.baseline_window > 0 and self._recent:
            shaped -= float(sum(self._recent) / len(self._recent))
        self._recent.append(float(reward))
        return float(shaped * self.scale)

    def step(self, action: Any):
        obs, reward, terminated, truncated, info = self.env.step(action)
        return obs, self.reward(float(reward)), terminated, truncated, info
