"""Gym-compatible portfolio-level RL environment backed by production gates."""

from __future__ import annotations
import logging

import hashlib
import json
import math
import os
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Mapping, Optional, Sequence

import numpy as np

from engine.execution.cost_models.almgren_chriss import AlmgrenChrissCost
from engine.rl.wrappers import clip_and_normalize_action

try:  # pragma: no cover - optional runtime dependency
    import gymnasium as gym
    from gymnasium import spaces
except Exception:  # pragma: no cover - exercised in this checkout
    gym = None  # type: ignore
    spaces = None  # type: ignore


class SimpleBox:
    """Minimal replacement for gymnasium.spaces.Box used in tests/imports."""

    def __init__(self, low: Any, high: Any, shape: Sequence[int], dtype: Any = np.float32):
        self.shape = tuple(int(x) for x in shape)
        self.dtype = dtype
        self.low = np.broadcast_to(np.asarray(low, dtype=dtype), self.shape).astype(dtype)
        self.high = np.broadcast_to(np.asarray(high, dtype=dtype), self.shape).astype(dtype)
        self._rng = np.random.default_rng()

    def seed(self, seed: Optional[int] = None) -> list[int]:
        self._rng = np.random.default_rng(seed)
        return [int(seed or 0)]

    def sample(self) -> np.ndarray:
        return self._rng.uniform(self.low, self.high).astype(self.dtype)

    def contains(self, x: Any) -> bool:
        arr = np.asarray(x, dtype=self.dtype)
        return arr.shape == self.shape and bool(np.all(arr >= self.low) and np.all(arr <= self.high))


Box = spaces.Box if spaces is not None else SimpleBox


DEFAULT_FEATURE_IDS = ("price.pct_ret_1d", "price.momentum_1d", "price.rv_20")


@dataclass
class PortfolioEnvConfig:
    universe: Sequence[str]
    feature_ids: Sequence[str] = field(default_factory=lambda: list(DEFAULT_FEATURE_IDS))
    price_history: Optional[Mapping[str, Sequence[float]]] = None
    start_ts_ms: Optional[int] = None
    end_ts_ms: Optional[int] = None
    episode_length: int = 252
    lookback: int = 20
    start_cash: float = 1.0
    max_w: float = 0.35
    leverage_cap: float = 1.0
    lambda_vol: float = 0.10
    lambda_dd: float = 1.0
    drawdown_threshold: float = 0.08
    risk_clip_penalty: float = 0.01
    adv_notional: float = 1_000_000.0
    seed: int = 7
    model_id: str = "rl_portfolio_shadow"
    book_key: Optional[str] = None
    strict_live_risk: bool = True
    request_monte_carlo: bool = True
    risk_overlay: Optional[Callable[..., tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]]] = None
    simulator: Optional[Callable[..., Dict[str, Any]]] = None
    feature_provider: Optional[Callable[[str, int, Sequence[str]], Mapping[str, float]]] = None


class PortfolioEnv(gym.Env if gym is not None else object):  # type: ignore[misc]
    """Daily-step target-weight environment for shadow-only portfolio RL.

    The default path calls ``portfolio_risk_engine``, ``portfolio_risk_gate``,
    and ``broker_sim.apply_new_portfolio_orders`` directly. Tests can inject
    risk and simulator callables, but the production default is the real stack.
    """

    metadata = {"render_modes": []}

    def __init__(self, config: PortfolioEnvConfig | Mapping[str, Any]):
        if gym is None and os.environ.get("RL_ALLOW_SIMPLE_GYM_FALLBACK", "0") != "1":
            raise RuntimeError("gymnasium is required for PortfolioEnv")
        cfg = config if isinstance(config, PortfolioEnvConfig) else PortfolioEnvConfig(**dict(config))
        if not cfg.universe:
            raise ValueError("PortfolioEnv requires a non-empty universe")

        self.config = cfg
        self.universe = [str(s).upper().strip() for s in cfg.universe if str(s).strip()]
        if not self.universe:
            raise ValueError("PortfolioEnv universe normalized to empty")

        self.feature_ids = self._resolve_feature_ids(cfg.feature_ids)
        self.max_w = float(cfg.max_w)
        self.leverage_cap = float(cfg.leverage_cap)
        self.lookback = int(max(1, cfg.lookback))
        self.episode_length = int(max(1, cfg.episode_length))
        self.rng = np.random.default_rng(int(cfg.seed))
        self.cost_model = AlmgrenChrissCost()

        self._prices = self._load_price_matrix(cfg)
        self._returns = self._price_returns(self._prices)
        self._feature_dim = len(self.universe) * len(self.feature_ids)
        self._obs_dim = self._feature_dim + len(self.universe) + 4

        self.action_space = Box(
            low=-abs(float(self.max_w)),
            high=abs(float(self.max_w)),
            shape=(len(self.universe),),
            dtype=np.float32,
        )
        self.observation_space = Box(
            low=-np.inf,
            high=np.inf,
            shape=(self._obs_dim,),
            dtype=np.float32,
        )

        self._episode_id = 0
        self._step_n = 0
        self._idx = self.lookback
        self._weights = np.zeros(len(self.universe), dtype=np.float32)
        self._cash = float(cfg.start_cash)
        self._equity = float(cfg.start_cash)
        self._peak_equity = float(cfg.start_cash)
        self._recent_portfolio_returns: deque[float] = deque(maxlen=max(2, self.lookback))
        self._last_info: Dict[str, Any] = {}
        self.book_key = self._new_book_key()

    @property
    def returns_matrix(self) -> np.ndarray:
        return self._returns.copy()

    @property
    def current_weights(self) -> np.ndarray:
        return self._weights.copy()

    def reset(self, *, seed: Optional[int] = None, options: Optional[Dict[str, Any]] = None):
        if seed is not None:
            self.rng = np.random.default_rng(int(seed))
            try:
                self.action_space.seed(int(seed))
            except Exception:
                logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)

        self._episode_id += 1
        self._step_n = 0
        self._weights = np.zeros(len(self.universe), dtype=np.float32)
        self._cash = float(self.config.start_cash)
        self._equity = float(self.config.start_cash)
        self._peak_equity = float(self.config.start_cash)
        self._recent_portfolio_returns.clear()
        self.book_key = self._new_book_key()

        max_start = max(self.lookback, self._prices.shape[0] - self.episode_length - 2)
        min_start = self.lookback
        if options and "start_index" in options:
            self._idx = int(max(min_start, min(max_start, int(options["start_index"]))))
        elif max_start > min_start:
            self._idx = int(self.rng.integers(min_start, max_start + 1))
        else:
            self._idx = int(min_start)

        obs = self._observation()
        info = {
            "ts_ms": self._ts_ms(),
            "book_key": self.book_key,
            "episode_id": int(self._episode_id),
            "obs_hash": observation_hash(obs),
        }
        self._last_info = dict(info)
        return obs, info

    def step(self, action: Any):
        proposed = clip_and_normalize_action(action, max_w=self.max_w, leverage_cap=self.leverage_cap)
        desired = self._weights_to_targets(proposed)
        state = self._weights_to_targets(self._weights)
        ts_ms = self._ts_ms()

        risked_targets, risk_info = self._apply_risk_overlay(desired, state, int(ts_ms))
        risked = self._targets_to_weights(risked_targets)
        risked = clip_and_normalize_action(risked, max_w=self.max_w, leverage_cap=self.leverage_cap)

        orders = self._orders_from_weights(self._weights, risked, int(ts_ms))
        simulator_result = self._simulate_orders(orders, int(ts_ms))

        next_idx = min(self._idx + 1, self._returns.shape[0] - 1)
        asset_ret = self._returns[next_idx]
        gross_pnl = float(np.dot(risked.astype(np.float64), asset_ret.astype(np.float64)))
        turnover = float(np.sum(np.abs(risked - self._weights)))
        cost_term = self._transaction_cost_term(self._weights, risked)
        self._weights = risked.astype(np.float32, copy=False)

        net_ret = float(gross_pnl - cost_term)
        self._equity = max(1e-9, float(self._equity) * (1.0 + net_ret))
        self._peak_equity = max(float(self._peak_equity), float(self._equity))
        drawdown = max(0.0, 1.0 - (float(self._equity) / max(1e-12, float(self._peak_equity))))
        self._recent_portfolio_returns.append(float(net_ret))
        realized_vol = self._recent_vol()
        self._cash = max(0.0, float(self._equity) * max(0.0, 1.0 - float(np.sum(np.abs(self._weights)))))

        risk_clip = float(np.sum(np.abs(proposed - risked)))
        risk_penalty = (
            float(self.config.lambda_vol) * float(realized_vol)
            + float(self.config.lambda_dd) * max(0.0, float(drawdown) - float(self.config.drawdown_threshold))
            + float(self.config.risk_clip_penalty) * float(risk_clip)
        )
        reward = float(gross_pnl - cost_term - risk_penalty)

        self._idx = int(next_idx)
        self._step_n += 1
        truncated = bool(
            self._step_n >= self.episode_length
            or self._idx >= self._returns.shape[0] - 2
        )
        terminated = False
        obs = self._observation()

        info = {
            "ts_ms": int(ts_ms),
            "book_key": self.book_key,
            "proposed_weights": self._weights_dict(proposed),
            "risked_weights": self._weights_dict(risked),
            "live_gate_consulted": True,
            "risk_info": risk_info,
            "risk_clip": float(risk_clip),
            "turnover": float(turnover),
            "pnl": float(gross_pnl),
            "cost": float(cost_term),
            "risk_penalty": float(risk_penalty),
            "equity": float(self._equity),
            "drawdown": float(drawdown),
            "realized_vol": float(realized_vol),
            "simulator_result": simulator_result,
            "obs_hash": observation_hash(obs),
        }
        self._last_info = dict(info)
        return obs, reward, terminated, truncated, info

    def close(self) -> None:
        return None

    def _new_book_key(self) -> str:
        if self.config.book_key:
            return str(self.config.book_key)
        return f"shadow_rl_{int(time.time() * 1000)}_{uuid.uuid4().hex[:10]}_{self._episode_id}"

    def _resolve_feature_ids(self, configured: Sequence[str]) -> list[str]:
        ids = [str(x).strip() for x in list(configured or []) if str(x).strip()]
        if not ids:
            ids = list(DEFAULT_FEATURE_IDS)
        if os.environ.get("RL_PORTFOLIO_VALIDATE_FEATURE_IDS", "0") != "1":
            return list(ids)
        try:
            from engine.strategy.feature_registry import resolve_feature_ids

            resolved = resolve_feature_ids(list(ids), fallback_to_default=False)
            return list(resolved or ids)
        except Exception:
            return list(ids)

    def _load_price_matrix(self, cfg: PortfolioEnvConfig) -> np.ndarray:
        if cfg.price_history:
            rows = []
            min_len = min(len(list(cfg.price_history.get(sym, []) or [])) for sym in self.universe)
            if min_len < 2:
                raise ValueError("price_history must contain at least two prices per symbol")
            for sym in self.universe:
                series = np.asarray(list(cfg.price_history.get(sym, []) or [])[:min_len], dtype=np.float64)
                series = np.where(np.isfinite(series) & (series > 0.0), series, np.nan)
                if np.isnan(series).any():
                    good = series[np.isfinite(series)]
                    fill = float(good[0]) if good.size else 100.0
                    series = np.nan_to_num(series, nan=fill, posinf=fill, neginf=fill)
                rows.append(series)
            return np.vstack(rows).T.astype(np.float64)

        loaded = self._load_prices_from_storage(cfg)
        if loaded is not None and loaded.shape[0] >= 2:
            return loaded
        return np.full((max(self.episode_length + self.lookback + 2, 32), len(self.universe)), 100.0, dtype=np.float64)

    def _load_prices_from_storage(self, cfg: PortfolioEnvConfig) -> Optional[np.ndarray]:
        try:
            from engine.runtime.storage import connect
        except Exception:
            return None

        con = None
        try:
            con = connect(readonly=True)
            per_symbol: list[list[float]] = []
            limit = int(max(self.episode_length + self.lookback + 2, 64))
            for sym in self.universe:
                params: tuple[Any, ...]
                where = "WHERE symbol=?"
                params_list: list[Any] = [str(sym)]
                if cfg.start_ts_ms is not None:
                    where += " AND ts_ms>=?"
                    params_list.append(int(cfg.start_ts_ms))
                if cfg.end_ts_ms is not None:
                    where += " AND ts_ms<=?"
                    params_list.append(int(cfg.end_ts_ms))
                params_list.append(limit)
                params = tuple(params_list)
                try:
                    rows = con.execute(
                        f"SELECT px FROM prices {where} ORDER BY ts_ms ASC LIMIT ?",
                        params,
                    ).fetchall()
                except Exception:
                    rows = con.execute(
                        f"SELECT price FROM prices {where} ORDER BY ts_ms ASC LIMIT ?",
                        params,
                    ).fetchall()
                values = [float(r[0]) for r in rows or [] if r and r[0] is not None and float(r[0]) > 0.0]
                per_symbol.append(values)
            min_len = min((len(x) for x in per_symbol), default=0)
            if min_len < 2:
                return None
            return np.vstack([np.asarray(x[:min_len], dtype=np.float64) for x in per_symbol]).T
        except Exception:
            return None
        finally:
            if con is not None:
                try:
                    con.close()
                except Exception:
                    logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)

    @staticmethod
    def _price_returns(prices: np.ndarray) -> np.ndarray:
        arr = np.asarray(prices, dtype=np.float64)
        prev = np.roll(arr, 1, axis=0)
        ret = np.zeros_like(arr, dtype=np.float64)
        ret[1:] = (arr[1:] / np.maximum(prev[1:], 1e-12)) - 1.0
        ret[~np.isfinite(ret)] = 0.0
        return ret.astype(np.float32)

    def _ts_ms(self) -> int:
        if self.config.start_ts_ms is not None:
            return int(self.config.start_ts_ms) + int(self._idx) * 86_400_000
        return int(time.time() * 1000) + int(self._idx) * 86_400_000

    def _observation(self) -> np.ndarray:
        ts_ms = self._ts_ms()
        features: list[float] = []
        for sidx, sym in enumerate(self.universe):
            provided = self._features_for_symbol(sym, ts_ms)
            for fid in self.feature_ids:
                if fid in provided:
                    value = float(provided.get(fid, 0.0) or 0.0)
                else:
                    value = self._synthetic_feature_value(sidx, fid)
                if not math.isfinite(value):
                    value = 0.0
                features.append(float(value))

        gross = float(np.sum(np.abs(self._weights)))
        drawdown = max(0.0, 1.0 - (float(self._equity) / max(1e-12, float(self._peak_equity))))
        tail = [
            float(self._cash / max(1e-12, self._equity)),
            float(self._recent_vol()),
            float(gross),
            float(drawdown),
        ]
        obs = np.asarray(features + [float(x) for x in self._weights] + tail, dtype=np.float32)
        obs[~np.isfinite(obs)] = 0.0
        return obs

    def _features_for_symbol(self, symbol: str, ts_ms: int) -> Mapping[str, float]:
        if self.config.feature_provider is not None:
            try:
                return dict(self.config.feature_provider(str(symbol), int(ts_ms), list(self.feature_ids)) or {})
            except Exception:
                return {}
        try:
            from engine.strategy.feature_registry import build_feature_snapshot

            return build_feature_snapshot(
                event={"ts_ms": int(ts_ms), "source": "rl_portfolio_env", "title": "", "body": ""},
                symbol=str(symbol),
                feature_ids=list(self.feature_ids),
            )
        except Exception:
            return {}

    def _synthetic_feature_value(self, symbol_idx: int, fid: str) -> float:
        idx = int(max(0, min(self._idx, self._returns.shape[0] - 1)))
        series = self._returns[max(0, idx - self.lookback + 1) : idx + 1, symbol_idx]
        text = str(fid)
        if text.endswith("pct_ret_1d") or text.endswith("log_ret_1d") or text.endswith("momentum_1d"):
            return float(self._returns[idx, symbol_idx])
        if text.endswith("rv_20") or text.endswith("vol_std_20"):
            return float(np.std(series)) if series.size else 0.0
        if text.endswith("last"):
            return float(self._prices[idx, symbol_idx])
        return 0.0

    def _recent_vol(self) -> float:
        if len(self._recent_portfolio_returns) < 2:
            if self._idx <= 1:
                return 0.0
            window = self._returns[max(0, self._idx - self.lookback + 1) : self._idx + 1]
            if window.size == 0:
                return 0.0
            proxy = np.dot(window, self._weights.astype(np.float32))
            return float(np.std(proxy)) if proxy.size else 0.0
        return float(np.std(np.asarray(self._recent_portfolio_returns, dtype=np.float64)))

    def _weights_to_targets(self, weights: np.ndarray) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for sym, weight in zip(self.universe, np.asarray(weights, dtype=np.float64)):
            w = float(weight)
            side = "FLAT"
            if w > 1e-12:
                side = "LONG"
            elif w < -1e-12:
                side = "SHORT"
            out[str(sym)] = {
                "model_id": str(self.config.model_id),
                "symbol": str(sym),
                "side": side,
                "weight": float(abs(w)),
                "source": "rl.portfolio_env",
                "reason": {"source": "rl.portfolio_env", "shadow_only": True},
                "explain_json": json.dumps(
                    {"source": "rl.portfolio_env", "shadow_only": True},
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            }
        return out

    def _targets_to_weights(self, targets: Mapping[str, Mapping[str, Any]]) -> np.ndarray:
        out = np.zeros(len(self.universe), dtype=np.float32)
        for idx, sym in enumerate(self.universe):
            row = dict((targets or {}).get(sym) or {})
            side = str(row.get("side") or "FLAT").upper()
            try:
                weight = float(row.get("weight", 0.0) or 0.0)
            except Exception:
                weight = 0.0
            if weight < 0.0:
                out[idx] = float(weight)
            elif side == "SHORT":
                out[idx] = -abs(float(weight))
            elif side == "LONG":
                out[idx] = abs(float(weight))
            else:
                out[idx] = 0.0
        return out

    def _apply_risk_overlay(
        self,
        desired: Dict[str, Dict[str, Any]],
        state: Dict[str, Dict[str, Any]],
        ts_ms: int,
    ) -> tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
        if self.config.risk_overlay is not None:
            try:
                return self.config.risk_overlay(desired=desired, state=state, now_ms=int(ts_ms))
            except TypeError:
                return self.config.risk_overlay(desired, state, int(ts_ms))

        con = None
        try:
            from engine.runtime.storage import connect
            from engine.risk.portfolio_risk_engine import apply_portfolio_risk_engine
            from engine.risk.monte_carlo_risk_engine import request_monte_carlo_refresh
            from engine.strategy.portfolio_risk_gate import apply_portfolio_risk_gate

            con = connect()
            out, risk_engine_info = apply_portfolio_risk_engine(con, desired, state, now_ms=int(ts_ms))
            if self.config.request_monte_carlo:
                try:
                    request_monte_carlo_refresh(out)
                except Exception:
                    logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)
            out, gate_info = apply_portfolio_risk_gate(con, out, state, now_ms=int(ts_ms))
            info = {
                "portfolio_risk_engine": dict(risk_engine_info or {}),
                "portfolio_risk_gate": dict(gate_info or {}),
                "blocked": bool((risk_engine_info or {}).get("blocked", False)),
            }
            if bool(info["blocked"]):
                return self._weights_to_targets(np.zeros(len(self.universe), dtype=np.float32)), info
            return out, info
        except Exception as exc:
            info = {"blocked": True, "risk_error": f"{type(exc).__name__}: {exc}"}
            if self.config.strict_live_risk:
                return self._weights_to_targets(np.zeros(len(self.universe), dtype=np.float32)), info
            return desired, info
        finally:
            if con is not None:
                try:
                    con.close()
                except Exception:
                    logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)

    def _orders_from_weights(self, current: np.ndarray, target: np.ndarray, ts_ms: int) -> list[dict[str, Any]]:
        orders: list[dict[str, Any]] = []
        for idx, sym in enumerate(self.universe):
            cur = float(current[idx])
            tgt = float(target[idx])
            if abs(tgt - cur) < 1e-9:
                continue
            side = "FLAT"
            if tgt > 1e-12:
                side = "LONG"
            elif tgt < -1e-12:
                side = "SHORT"
            orders.append(
                {
                    "source": "rl.portfolio_env",
                    "source_order_id": int(self._episode_id * 1_000_000 + self._step_n * 10_000 + idx + 1),
                    "ts_ms": int(ts_ms),
                    "model_id": str(self.config.model_id),
                    "symbol": str(sym),
                    "action": "REBALANCE",
                    "from_weight": abs(float(cur)),
                    "to_weight": abs(float(tgt)),
                    "delta_weight": float(tgt - cur),
                    "from_side": "SHORT" if cur < -1e-12 else ("LONG" if cur > 1e-12 else "FLAT"),
                    "to_side": side,
                    "explain": {"source": "rl.portfolio_env", "shadow_only": True},
                }
            )
        return orders

    def _simulate_orders(self, orders: list[dict[str, Any]], ts_ms: int) -> Dict[str, Any]:
        if self.config.simulator is not None:
            return dict(
                self.config.simulator(
                    orders=list(orders),
                    ts_ms=int(ts_ms),
                    book_key=str(self.book_key),
                    cost_model=self.cost_model,
                )
                or {}
            )
        try:
            from engine.execution.broker_sim import apply_new_portfolio_orders

            return dict(
                apply_new_portfolio_orders(
                    dry_run=False,
                    override_orders=list(orders or []),
                    override_order_id=int(self._episode_id * 1_000_000 + self._step_n + 1),
                    override_ts_ms=int(ts_ms),
                    book_key=str(self.book_key),
                    cost_model=self.cost_model,
                )
                or {}
            )
        except Exception as exc:
            return {"ok": False, "status": "simulator_error", "error": f"{type(exc).__name__}: {exc}"}

    def _transaction_cost_term(self, current: np.ndarray, target: np.ndarray) -> float:
        total = 0.0
        delta = np.asarray(target, dtype=np.float64) - np.asarray(current, dtype=np.float64)
        sigma_bps = max(1.0, float(self._recent_vol()) * 10000.0)
        equity = max(1e-12, float(self._equity))
        for dw in delta:
            notional = abs(float(dw)) * equity
            if notional <= 0.0:
                continue
            try:
                bps = self.cost_model.cost_bps(
                    notional=float(notional),
                    adv=float(self.config.adv_notional),
                    sigma_daily=float(sigma_bps),
                    participation=min(1.0, max(1e-6, float(notional) / max(1e-12, float(self.config.adv_notional)))),
                    half_spread_bps=0.5,
                    asset_class="US_EQUITY",
                )
            except Exception:
                bps = 0.0
            total += float(notional / equity) * (float(bps) / 10000.0)
        return float(total)

    def _weights_dict(self, weights: np.ndarray) -> Dict[str, float]:
        return {str(sym): float(weights[idx]) for idx, sym in enumerate(self.universe)}


def observation_hash(obs: Any) -> str:
    arr = np.asarray(obs, dtype=np.float32).reshape(-1)
    return hashlib.sha256(arr.tobytes()).hexdigest()[:16]
