"""Shadow evaluator for portfolio RL policies.

This module deliberately does not import the broker router. It only loads a
policy, computes target-weight deltas, and writes them to storage.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional, Sequence

import numpy as np

from engine.rl.agents import PortfolioAgent, latest_checkpoint, load_agent
from engine.rl.portfolio_env import PortfolioEnv, PortfolioEnvConfig, observation_hash
from engine.rl.wrappers import clip_and_normalize_action
from engine.runtime.platform import default_local_models_dir
from engine.runtime import storage


LiveDecisionFn = Callable[[], Any]
ObservationFn = Callable[[Sequence[str], Mapping[str, float], int], Any]
KillSwitchFn = Callable[[Optional[Any]], tuple[bool, str, Dict[str, Any]]]


def _default_rl_model_root() -> str:
    return str((default_local_models_dir() / "rl").resolve())


@dataclass
class ShadowRunnerConfig:
    universe: Sequence[str]
    algo: str = "ppo"
    model_root: str = field(default_factory=_default_rl_model_root)
    model_name: str = "rl_portfolio_shadow"
    candidate_type: str = "rl"
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
  model_name TEXT NOT NULL DEFAULT 'rl_portfolio_shadow',
  candidate_type TEXT NOT NULL DEFAULT 'rl',
  symbol TEXT NOT NULL,
  live_weight REAL NOT NULL,
  rl_weight REAL NOT NULL,
  delta REAL NOT NULL,
  obs_hash TEXT NOT NULL,
  behavior_propensity REAL,
  target_propensity REAL,
  outcome REAL,
  logged_model_estimate REAL,
  target_model_estimate REAL,
  meta_json TEXT NOT NULL DEFAULT '{}',
  PRIMARY KEY(ts, symbol)
);
"""


def ensure_shadow_schema(con: Any) -> None:
    con.executescript(RL_SHADOW_SCHEMA)
    cols = _table_columns(con, "rl_shadow_decisions")
    for column_name, definition in (
        ("model_name", "TEXT NOT NULL DEFAULT 'rl_portfolio_shadow'"),
        ("candidate_type", "TEXT NOT NULL DEFAULT 'rl'"),
        ("behavior_propensity", "REAL"),
        ("target_propensity", "REAL"),
        ("outcome", "REAL"),
        ("logged_model_estimate", "REAL"),
        ("target_model_estimate", "REAL"),
        ("meta_json", "TEXT NOT NULL DEFAULT '{}'"),
    ):
        if column_name not in cols:
            con.execute(f"ALTER TABLE rl_shadow_decisions ADD COLUMN {column_name} {definition}")


def _table_columns(con: Any, table_name: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({table_name})").fetchall() or []
    except Exception:
        return set()
    return {str(row[1] or "").strip() for row in rows if row and len(row) > 1 and str(row[1] or "").strip()}


def _safe_json_dumps(value: Mapping[str, Any]) -> str:
    import json

    return json.dumps(dict(value or {}), separators=(",", ":"), sort_keys=True, default=str)


def _float_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        out = float(value)
    except Exception:
        return None
    if not np.isfinite(out):
        return None
    return float(out)


def normalize_live_decisions(decisions: Any) -> Dict[str, float]:
    if decisions is None:
        return {}
    if isinstance(decisions, Mapping):
        out: Dict[str, float] = {}
        for key, value in decisions.items():
            sym = str(key).upper().strip()
            if not sym:
                continue
            if isinstance(value, Mapping):
                if value.get("weight") is not None:
                    weight = float(value.get("weight") or 0.0)
                elif value.get("to_weight") is not None:
                    weight = float(value.get("to_weight") or 0.0)
                else:
                    weight = 0.0
            else:
                weight = float(value or 0.0)
            out[sym] = float(weight)
        return out
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


def normalize_live_decision_metadata(decisions: Any) -> Dict[str, Dict[str, Any]]:
    if decisions is None:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    if isinstance(decisions, Mapping):
        for key, value in decisions.items():
            sym = str(key).upper().strip()
            if sym and isinstance(value, Mapping):
                out[sym] = dict(value)
        return out
    for item in list(decisions or []):
        if not isinstance(item, Mapping):
            continue
        sym = str(item.get("symbol") or "").upper().strip()
        if sym:
            out[sym] = dict(item)
    return out


def _first_detail_value(details: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in details:
            return details.get(key)
    return None


def _insert_shadow_decision(con: Any, row: Mapping[str, Any]) -> None:
    cols = _table_columns(con, "rl_shadow_decisions")
    ordered = [
        "ts",
        "model_name",
        "candidate_type",
        "symbol",
        "live_weight",
        "rl_weight",
        "delta",
        "obs_hash",
        "behavior_propensity",
        "target_propensity",
        "outcome",
        "logged_model_estimate",
        "target_model_estimate",
        "meta_json",
    ]
    insert_cols = [col for col in ordered if col in cols]
    placeholders = ",".join(["?"] * len(insert_cols))
    con.execute(
        f"""
        INSERT OR REPLACE INTO rl_shadow_decisions(
          {", ".join(insert_cols)}
        )
        VALUES ({placeholders})
        """,
        tuple(row.get(col) for col in insert_cols),
    )


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
            live_details = normalize_live_decision_metadata(live_raw)
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
                details = dict(live_details.get(sym) or {})
                behavior_propensity = _float_or_none(
                    _first_detail_value(
                        details,
                        "behavior_propensity",
                        "logging_propensity",
                        "logged_propensity",
                        "decision_propensity",
                        "propensity",
                    )
                )
                target_propensity = _float_or_none(
                    _first_detail_value(
                        details,
                        "target_propensity",
                        "candidate_propensity",
                        "evaluation_propensity",
                        "policy_propensity",
                    )
                )
                logged_model_estimate = _float_or_none(
                    _first_detail_value(
                        details,
                        "logged_model_estimate",
                        "behavior_model_estimate",
                        "q_logged",
                        "model_estimate",
                    )
                )
                target_model_estimate = _float_or_none(
                    _first_detail_value(
                        details,
                        "target_model_estimate",
                        "candidate_model_estimate",
                        "q_target",
                        "policy_model_estimate",
                    )
                )
                outcome = _float_or_none(
                    _first_detail_value(details, "outcome", "reward", "net_return", "net_ret", "realized_return")
                )
                rows.append(
                    {
                        "ts": int(ts),
                        "model_name": str(self.config.model_name or "rl_portfolio_shadow"),
                        "candidate_type": str(self.config.candidate_type or "rl"),
                        "symbol": str(sym),
                        "live_weight": float(live_w),
                        "rl_weight": float(rl_w),
                        "delta": float(rl_w - live_w),
                        "obs_hash": str(obs_h),
                        "behavior_propensity": behavior_propensity,
                        "target_propensity": target_propensity,
                        "outcome": outcome,
                        "logged_model_estimate": logged_model_estimate,
                        "target_model_estimate": target_model_estimate,
                        "meta_json": _safe_json_dumps(
                            {
                                "live_weight": float(live_w),
                                "rl_weight": float(rl_w),
                                "logged_action": f"weight:{live_w:.8f}",
                                "target_action": f"weight:{rl_w:.8f}",
                                "ope": {
                                    "behavior_propensity": behavior_propensity,
                                    "target_propensity": target_propensity,
                                    "outcome": outcome,
                                    "logged_model_estimate": logged_model_estimate,
                                    "target_model_estimate": target_model_estimate,
                                },
                            }
                        ),
                    }
                )

            for row in rows:
                _insert_shadow_decision(con, row)
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
