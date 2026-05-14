"""Shadow evaluator for portfolio RL policies.

This module deliberately does not import the broker router. It only loads a
policy, computes target-weight deltas, and writes them to storage.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Sequence

import numpy as np

from engine.rl.agents import PortfolioAgent, latest_checkpoint, load_agent
from engine.rl.portfolio_env import PortfolioEnv, PortfolioEnvConfig, observation_hash
from engine.rl.wrappers import clip_and_normalize_action
from engine.runtime import storage


LiveDecisionFn = Callable[[], Any]
ObservationFn = Callable[[Sequence[str], Mapping[str, float], int], Any]
KillSwitchFn = Callable[[Optional[Any]], tuple[bool, str, Dict[str, Any]]]


@dataclass
class ShadowRunnerConfig:
    universe: Sequence[str]
    algo: str = "ppo"
    model_root: str = "models/rl"
    checkpoint_path: Optional[str] = None
    max_w: float = 0.35
    leverage_cap: float = 1.0
    seed: int = 7
    live_decision_fn: Optional[LiveDecisionFn] = None
    observation_fn: Optional[ObservationFn] = None
    kill_switch_fn: Optional[KillSwitchFn] = None


RL_SHADOW_SCHEMA = """
CREATE TABLE IF NOT EXISTS rl_shadow_decisions(
  ts INTEGER NOT NULL,
  symbol TEXT NOT NULL,
  live_weight REAL NOT NULL,
  rl_weight REAL NOT NULL,
  delta REAL NOT NULL,
  obs_hash TEXT NOT NULL,
  PRIMARY KEY(ts, symbol)
);
"""


def ensure_shadow_schema(con: Any) -> None:
    con.executescript(RL_SHADOW_SCHEMA)


def normalize_live_decisions(decisions: Any) -> Dict[str, float]:
    if decisions is None:
        return {}
    if isinstance(decisions, Mapping):
        return {str(k).upper().strip(): float(v or 0.0) for k, v in decisions.items() if str(k).strip()}
    out: Dict[str, float] = {}
    for item in list(decisions or []):
        if not isinstance(item, Mapping):
            continue
        sym = str(item.get("symbol") or "").upper().strip()
        if not sym:
            continue
        if item.get("weight") is not None:
            weight = float(item.get("weight") or 0.0)
        elif item.get("to_weight") is not None:
            side = str(item.get("to_side") or item.get("side") or "LONG").upper()
            weight = float(item.get("to_weight") or 0.0)
            if side == "SHORT":
                weight = -abs(weight)
            elif side == "FLAT":
                weight = 0.0
        else:
            weight = 0.0
        out[sym] = float(weight)
    return out


class RLShadowRunner:
    def __init__(self, config: ShadowRunnerConfig, *, agent: Optional[PortfolioAgent] = None):
        self.config = config
        self.agent = agent

    def run_once(self, *, live_decisions: Any = None, ts_ms: Optional[int] = None) -> Dict[str, Any]:
        ts = int(ts_ms if ts_ms is not None else time.time() * 1000)
        con = storage.connect()
        try:
            self._ensure_schema(con)
            allowed, reason, kill_meta = self._kill_switch_allowed(con)
            if not allowed:
                return {
                    "ok": True,
                    "status": "paused_kill_switch",
                    "reason": str(reason),
                    "meta": dict(kill_meta or {}),
                    "rows": 0,
                }

            live_raw = live_decisions
            if live_raw is None and self.config.live_decision_fn is not None:
                live_raw = self.config.live_decision_fn()
            live_weights = normalize_live_decisions(live_raw)
            universe = self._resolved_universe(live_weights)

            agent = self._load_agent()
            if agent is None:
                return {"ok": False, "status": "no_checkpoint", "rows": 0}

            obs = self._build_observation(universe, live_weights, ts)
            rl_action = agent.predict(obs, deterministic=True)
            rl_action = clip_and_normalize_action(
                rl_action,
                max_w=float(self.config.max_w),
                leverage_cap=float(self.config.leverage_cap),
            )
            obs_h = observation_hash(obs)

            rows = []
            for idx, sym in enumerate(universe):
                live_w = float(live_weights.get(sym, 0.0))
                rl_w = float(rl_action[idx]) if idx < len(rl_action) else 0.0
                rows.append((int(ts), str(sym), float(live_w), float(rl_w), float(rl_w - live_w), str(obs_h)))

            for row in rows:
                con.execute(
                    """
                    INSERT OR REPLACE INTO rl_shadow_decisions(
                      ts, symbol, live_weight, rl_weight, delta, obs_hash
                    )
                    VALUES (?,?,?,?,?,?)
                    """,
                    row,
                )
            con.commit()
            return {
                "ok": True,
                "status": "logged",
                "rows": int(len(rows)),
                "obs_hash": str(obs_h),
                "symbols": list(universe),
            }
        finally:
            con.close()

    def _ensure_schema(self, con: Any) -> None:
        if hasattr(storage, "init_rl_portfolio_tables"):
            storage.init_rl_portfolio_tables(con=con)
        else:
            ensure_shadow_schema(con)

    def _kill_switch_allowed(self, con: Any) -> tuple[bool, str, Dict[str, Any]]:
        if self.config.kill_switch_fn is not None:
            return self.config.kill_switch_fn(con)
        try:
            from engine.execution.kill_switch import execution_allowed

            allowed, reason, meta = execution_allowed(con=con, symbol="*", regime=None, model_id="rl_portfolio_shadow")
            return bool(allowed), str(reason or ""), dict(meta or {})
        except Exception as exc:
            return False, "kill_switch_error", {"error": f"{type(exc).__name__}: {exc}"}

    def _resolved_universe(self, live_weights: Mapping[str, float]) -> list[str]:
        out = [str(s).upper().strip() for s in self.config.universe if str(s).strip()]
        for sym in sorted(live_weights.keys()):
            if sym and sym not in out:
                out.append(sym)
        return out

    def _load_agent(self) -> Optional[PortfolioAgent]:
        if self.agent is not None:
            return self.agent
        path: Optional[Path]
        if self.config.checkpoint_path:
            path = Path(self.config.checkpoint_path)
        else:
            path = latest_checkpoint(algo=str(self.config.algo), model_root=str(self.config.model_root))
        if path is None:
            return None
        self.agent = load_agent(path, algo=str(self.config.algo), seed=int(self.config.seed))
        return self.agent

    def _build_observation(self, universe: Sequence[str], live_weights: Mapping[str, float], ts_ms: int) -> np.ndarray:
        if self.config.observation_fn is not None:
            return np.asarray(self.config.observation_fn(universe, live_weights, int(ts_ms)), dtype=np.float32)

        prices = {sym: [100.0, 100.0, 100.0] for sym in universe}
        env = PortfolioEnv(
            PortfolioEnvConfig(
                universe=list(universe),
                price_history=prices,
                episode_length=1,
                lookback=1,
                max_w=float(self.config.max_w),
                leverage_cap=float(self.config.leverage_cap),
                seed=int(self.config.seed),
                strict_live_risk=False,
                request_monte_carlo=False,
                risk_overlay=lambda desired, state, now_ms: (desired, {"test_or_shadow": True}),
                simulator=lambda **_: {"ok": True, "status": "shadow_observation_only"},
                feature_provider=lambda symbol, ts, ids: {str(fid): 0.0 for fid in ids},
            )
        )
        obs, _ = env.reset(seed=int(self.config.seed), options={"start_index": 1})
        weights = np.asarray([float(live_weights.get(str(sym).upper().strip(), 0.0)) for sym in universe], dtype=np.float32)
        env._weights = clip_and_normalize_action(  # noqa: SLF001 - internal observation bootstrap
            weights,
            max_w=float(self.config.max_w),
            leverage_cap=float(self.config.leverage_cap),
        )
        obs = env._observation()  # noqa: SLF001 - internal observation bootstrap
        env.close()
        return obs


def run_shadow_once(config: ShadowRunnerConfig, *, live_decisions: Any = None, ts_ms: Optional[int] = None) -> Dict[str, Any]:
    return RLShadowRunner(config).run_once(live_decisions=live_decisions, ts_ms=ts_ms)


def rows_for_snapshot(con: Any, ts_ms: int) -> list[dict[str, Any]]:
    rows = con.execute(
        """
        SELECT ts, symbol, live_weight, rl_weight, delta, obs_hash
        FROM rl_shadow_decisions
        WHERE ts=?
        ORDER BY symbol
        """,
        (int(ts_ms),),
    ).fetchall()
    return [
        {
            "ts": int(r[0]),
            "symbol": str(r[1]),
            "live_weight": float(r[2]),
            "rl_weight": float(r[3]),
            "delta": float(r[4]),
            "obs_hash": str(r[5]),
        }
        for r in rows or []
    ]
