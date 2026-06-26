"""Offline trajectory builder and risk-sensitive rewards for portfolio RL.

The module is deliberately research/shadow-only. It reads historical runtime
tables and emits trajectories plus OPE inputs; it does not import broker
routing or order-application modules.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from engine.rl.portfolio_env import DEFAULT_FEATURE_IDS, observation_hash
from engine.rl.wrappers import clip_and_normalize_action
from engine.runtime.platform import default_local_models_dir


DAY_MS = 86_400_000
LOG = logging.getLogger(__name__)


def _default_artifact_root() -> str:
    return str((default_local_models_dir() / "rl" / "offline").resolve())


@dataclass(frozen=True)
class RiskSensitiveRewardConfig:
    """Reward terms used for offline portfolio RL research."""

    fallback_cost_bps: float = 1.0
    drawdown_penalty: float = 1.0
    drawdown_threshold: float = 0.08
    turnover_penalty: float = 0.01
    slippage_penalty: float = 1.0
    concentration_penalty: float = 0.01
    cvar_penalty: float = 1.0
    cvar_alpha: float = 0.10
    cvar_window: int = 50

    def to_dict(self) -> dict[str, Any]:
        return {
            "fallback_cost_bps": float(self.fallback_cost_bps),
            "drawdown_penalty": float(self.drawdown_penalty),
            "drawdown_threshold": float(self.drawdown_threshold),
            "turnover_penalty": float(self.turnover_penalty),
            "slippage_penalty": float(self.slippage_penalty),
            "concentration_penalty": float(self.concentration_penalty),
            "cvar_penalty": float(self.cvar_penalty),
            "cvar_alpha": float(self.cvar_alpha),
            "cvar_window": int(self.cvar_window),
        }


@dataclass(frozen=True)
class OfflineDatasetConfig:
    """Historical trajectory-builder configuration."""

    universe: Sequence[str] = field(default_factory=list)
    feature_ids: Sequence[str] = field(default_factory=lambda: list(DEFAULT_FEATURE_IDS))
    feature_set_tag: str = "default"
    start_ts_ms: int | None = None
    end_ts_ms: int | None = None
    horizon_ms: int = DAY_MS
    max_w: float = 0.35
    leverage_cap: float = 1.0
    min_rows: int = 1
    require_outcomes: bool = True
    reward: RiskSensitiveRewardConfig = field(default_factory=RiskSensitiveRewardConfig)
    artifact_root: str = field(default_factory=_default_artifact_root)
    model_name: str = "offline_rl_behavior_cloning"
    candidate_version: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "universe": [str(x).upper().strip() for x in self.universe],
            "feature_ids": [str(x) for x in self.feature_ids],
            "feature_set_tag": str(self.feature_set_tag),
            "start_ts_ms": self.start_ts_ms,
            "end_ts_ms": self.end_ts_ms,
            "horizon_ms": int(self.horizon_ms),
            "max_w": float(self.max_w),
            "leverage_cap": float(self.leverage_cap),
            "min_rows": int(self.min_rows),
            "require_outcomes": bool(self.require_outcomes),
            "reward": self.reward.to_dict(),
            "artifact_root": str(self.artifact_root),
            "model_name": str(self.model_name),
            "candidate_version": str(self.candidate_version),
        }


@dataclass(frozen=True)
class OfflineTransition:
    ts_ms: int
    universe: tuple[str, ...]
    observation: tuple[float, ...]
    action: tuple[float, ...]
    logged_action: str
    target_action: str
    reward: float
    outcome: float
    logged_model_estimate: float
    target_model_estimate: float
    behavior_propensity: float
    target_propensity: float
    obs_hash: str
    source_ids: tuple[str, ...]
    meta: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "ts_ms": int(self.ts_ms),
            "universe": list(self.universe),
            "observation": [float(x) for x in self.observation],
            "action": [float(x) for x in self.action],
            "logged_action": str(self.logged_action),
            "target_action": str(self.target_action),
            "reward": float(self.reward),
            "outcome": float(self.outcome),
            "logged_model_estimate": float(self.logged_model_estimate),
            "target_model_estimate": float(self.target_model_estimate),
            "behavior_propensity": float(self.behavior_propensity),
            "target_propensity": float(self.target_propensity),
            "obs_hash": str(self.obs_hash),
            "source_ids": list(self.source_ids),
            "meta": dict(self.meta),
        }


@dataclass(frozen=True)
class OfflineRLDataset:
    config: OfflineDatasetConfig
    transitions: tuple[OfflineTransition, ...]
    dataset_hash: str
    behavior_policy: dict[str, Any]
    diagnostics: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "config": self.config.to_dict(),
            "dataset_hash": str(self.dataset_hash),
            "behavior_policy": dict(self.behavior_policy),
            "diagnostics": dict(self.diagnostics),
            "transitions": [row.to_dict() for row in self.transitions],
        }


def _json_loads(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if value in (None, "", b"", bytearray()):
        return {}
    raw = value.decode("utf-8", errors="replace") if isinstance(value, (bytes, bytearray)) else str(value)
    try:
        return json.loads(raw)
    except Exception:
        return {}


def _json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)


def _table_exists(con: Any, table_name: str) -> bool:
    name = str(table_name)
    try:
        row = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (name,),
        ).fetchone()
        if row:
            return True
    except Exception:
        LOG.debug("sqlite_table_exists_probe_failed", exc_info=True)
    try:
        row = con.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name=%s LIMIT 1",
            (name,),
        ).fetchone()
        return bool(row)
    except Exception:
        return False


def _table_columns(con: Any, table_name: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({table_name})").fetchall() or []
        cols = {str(row[1]) for row in rows if len(row) > 1}
        if cols:
            return cols
    except Exception:
        LOG.debug("sqlite_table_columns_probe_failed", exc_info=True)
    try:
        rows = con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name=%s",
            (str(table_name),),
        ).fetchall() or []
        return {str(row[0]) for row in rows if row}
    except Exception:
        return set()


def _signed_weight(side: Any, weight: Any) -> float:
    try:
        value = abs(float(weight or 0.0))
    except Exception:
        value = 0.0
    side_s = str(side or "FLAT").strip().upper()
    if side_s == "SHORT":
        return -value
    if side_s == "LONG":
        return value
    return 0.0


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def _normalize_universe(raw: Sequence[str]) -> list[str]:
    out: list[str] = []
    for value in raw or []:
        sym = str(value or "").upper().strip()
        if sym and sym not in out:
            out.append(sym)
    return out


def _derive_universe(con: Any, config: OfflineDatasetConfig) -> list[str]:
    configured = _normalize_universe(config.universe)
    if configured:
        return configured
    if not _table_exists(con, "portfolio_orders"):
        return []
    where, params = _time_where(config)
    rows = con.execute(
        f"SELECT DISTINCT UPPER(symbol) FROM portfolio_orders {where} ORDER BY UPPER(symbol)",
        tuple(params),
    ).fetchall() or []
    return _normalize_universe([str(row[0]) for row in rows if row and row[0]])


def _time_where(config: OfflineDatasetConfig) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if config.start_ts_ms is not None:
        clauses.append("ts_ms>=?")
        params.append(int(config.start_ts_ms))
    if config.end_ts_ms is not None:
        clauses.append("ts_ms<=?")
        params.append(int(config.end_ts_ms))
    if clauses:
        return "WHERE " + " AND ".join(clauses), params
    return "", params


def _load_orders(con: Any, config: OfflineDatasetConfig, universe: Sequence[str]) -> list[dict[str, Any]]:
    if not _table_exists(con, "portfolio_orders"):
        return []
    where, params = _time_where(config)
    universe_set = set(_normalize_universe(universe))
    rows = con.execute(
        f"""
        SELECT id, ts_ms, model_id, symbol, action, from_side, to_side,
               from_weight, to_weight, delta_weight, source_alert_id,
               prediction_id, explain_json
        FROM portfolio_orders
        {where}
        ORDER BY ts_ms ASC, id ASC
        """,
        tuple(params),
    ).fetchall() or []
    out: list[dict[str, Any]] = []
    for row in rows:
        sym = str(row[3] or "").upper().strip()
        if universe_set and sym not in universe_set:
            continue
        out.append(
            {
                "id": int(row[0]),
                "ts_ms": int(row[1]),
                "model_id": str(row[2] or ""),
                "symbol": sym,
                "action": str(row[4] or ""),
                "from_side": str(row[5] or "FLAT"),
                "to_side": str(row[6] or "FLAT"),
                "from_weight": _safe_float(row[7]),
                "to_weight": _safe_float(row[8]),
                "delta_weight": _safe_float(row[9]),
                "source_alert_id": row[10],
                "prediction_id": row[11],
                "explain": _json_loads(row[12]),
            }
        )
    return out


def _feature_snapshot(con: Any, *, symbol: str, ts_ms: int, config: OfflineDatasetConfig) -> tuple[dict[str, float], dict[str, Any], str]:
    if not _table_exists(con, "model_feature_snapshots"):
        return {}, {"missing_table": "model_feature_snapshots"}, ""
    cols = _table_columns(con, "model_feature_snapshots")
    tag_clause = ""
    params: list[Any] = [str(symbol).upper().strip(), int(ts_ms)]
    if "feature_set_tag" in cols and str(config.feature_set_tag).strip():
        tag_clause = "AND feature_set_tag=?"
        params.append(str(config.feature_set_tag))
    rows = con.execute(
        f"""
        SELECT ts_ms, feature_ids_json, vector_json, features_json,
               source_timestamps_json, feature_set_tag
        FROM model_feature_snapshots
        WHERE UPPER(symbol)=UPPER(?) AND ts_ms<=? {tag_clause}
        ORDER BY ts_ms DESC
        LIMIT 1
        """,
        tuple(params),
    ).fetchall() or []
    if not rows:
        return {}, {"missing_snapshot": True}, ""
    row = rows[0]
    snap_ts = int(row[0])
    source_ts = _json_loads(row[4])
    future_sources: dict[str, Any] = {}
    if isinstance(source_ts, Mapping):
        for key, value in source_ts.items():
            if _safe_float(value, -1.0) > float(ts_ms):
                future_sources[str(key)] = value
    if future_sources:
        return {}, {"future_source_timestamps": future_sources, "snapshot_ts_ms": snap_ts}, ""

    features_json = _json_loads(row[3])
    vector_json = _json_loads(row[2])
    feature_ids_json = _json_loads(row[1])
    values: dict[str, float] = {}
    if isinstance(features_json, Mapping):
        for key, value in features_json.items():
            values[str(key)] = _safe_float(value)
    if isinstance(vector_json, Sequence) and not isinstance(vector_json, (str, bytes, bytearray)):
        ids = list(feature_ids_json) if isinstance(feature_ids_json, Sequence) and not isinstance(feature_ids_json, (str, bytes, bytearray)) else []
        for idx, value in enumerate(vector_json):
            key = str(ids[idx]) if idx < len(ids) else f"feature_{idx}"
            values.setdefault(key, _safe_float(value))
    meta = {
        "snapshot_ts_ms": int(snap_ts),
        "feature_set_tag": str(row[5] or ""),
        "source_timestamps": source_ts if isinstance(source_ts, Mapping) else {},
    }
    return values, meta, f"model_feature_snapshots:{symbol}:{snap_ts}"


def _price_at_or_after(con: Any, *, symbol: str, ts_ms: int) -> float | None:
    if not _table_exists(con, "prices"):
        return None
    cols = _table_columns(con, "prices")
    price_expr = "COALESCE(price, px)" if {"price", "px"}.issubset(cols) else ("price" if "price" in cols else "px")
    try:
        rows = con.execute(
            f"""
            SELECT {price_expr}
            FROM prices
            WHERE UPPER(symbol)=UPPER(?) AND ts_ms>=?
              AND {price_expr} IS NOT NULL
            ORDER BY ts_ms ASC
            LIMIT 1
            """,
            (str(symbol).upper().strip(), int(ts_ms)),
        ).fetchall() or []
    except Exception:
        return None
    if not rows:
        return None
    value = _safe_float(rows[0][0], math.nan)
    return float(value) if math.isfinite(value) and value > 0.0 else None


def _forward_returns(con: Any, *, universe: Sequence[str], ts_ms: int, horizon_ms: int) -> tuple[np.ndarray, dict[str, Any]]:
    returns: list[float] = []
    missing: list[str] = []
    end_ts = int(ts_ms) + int(max(1, horizon_ms))
    for sym in universe:
        start_px = _price_at_or_after(con, symbol=str(sym), ts_ms=int(ts_ms))
        end_px = _price_at_or_after(con, symbol=str(sym), ts_ms=int(end_ts))
        if start_px is None or end_px is None:
            missing.append(str(sym))
            returns.append(0.0)
            continue
        returns.append(float((float(end_px) / max(1e-12, float(start_px))) - 1.0))
    return np.asarray(returns, dtype=np.float64), {"missing_price_symbols": missing, "horizon_ms": int(horizon_ms)}


def _sum_fill_costs(con: Any, order_ids: Sequence[int]) -> tuple[float, float]:
    order_ids_i = [int(x) for x in order_ids if x is not None]
    if not order_ids_i:
        return 0.0, 0.0
    costs = 0.0
    slippage_bps_values: list[float] = []
    for table, id_col, ts_col in (
        ("execution_fills", "portfolio_orders_id", "fill_ts_ms"),
        ("broker_fills", "source_order_id", "ts_ms"),
    ):
        if not _table_exists(con, table):
            continue
        cols = _table_columns(con, table)
        if id_col not in cols:
            continue
        placeholders = ",".join(["?"] * len(order_ids_i))
        numeric_terms: list[str] = []
        for col in ("fees", "commission", "cost", "costs", "option_margin_debit"):
            if col in cols:
                numeric_terms.append(f"COALESCE({col},0)")
        if numeric_terms:
            rows = con.execute(
                f"SELECT SUM({' + '.join(numeric_terms)}) FROM {table} WHERE {id_col} IN ({placeholders})",
                tuple(order_ids_i),
            ).fetchall() or []
            costs += abs(_safe_float(rows[0][0] if rows else 0.0))
        if "slippage_bps" in cols:
            rows = con.execute(
                f"SELECT slippage_bps FROM {table} WHERE {id_col} IN ({placeholders}) AND slippage_bps IS NOT NULL ORDER BY {ts_col}",
                tuple(order_ids_i),
            ).fetchall() or []
            slippage_bps_values.extend(abs(_safe_float(row[0])) for row in rows)
    slippage_bps = float(np.mean(slippage_bps_values)) if slippage_bps_values else 0.0
    return float(costs), float(slippage_bps)


def _observation(
    *,
    con: Any,
    universe: Sequence[str],
    weights: np.ndarray,
    ts_ms: int,
    equity: float,
    peak_equity: float,
    recent_returns: Sequence[float],
    config: OfflineDatasetConfig,
) -> tuple[np.ndarray | None, dict[str, Any], tuple[str, ...]]:
    feature_values: list[float] = []
    feature_meta: dict[str, Any] = {}
    source_ids: list[str] = []
    feature_ids = [str(x) for x in (config.feature_ids or DEFAULT_FEATURE_IDS)]
    rejected: dict[str, Any] = {}
    for sym in universe:
        values, meta, source_id = _feature_snapshot(con, symbol=str(sym), ts_ms=int(ts_ms), config=config)
        if meta.get("future_source_timestamps"):
            rejected[str(sym)] = dict(meta)
            continue
        feature_meta[str(sym)] = dict(meta)
        if source_id:
            source_ids.append(str(source_id))
        for fid in feature_ids:
            feature_values.append(_safe_float(values.get(str(fid), 0.0)))
    if rejected:
        return None, {"pit_rejected": rejected}, tuple(source_ids)

    gross = float(np.sum(np.abs(weights)))
    drawdown = max(0.0, 1.0 - (float(equity) / max(1e-12, float(peak_equity))))
    realized_vol = float(np.std(np.asarray(recent_returns, dtype=np.float64))) if len(recent_returns) >= 2 else 0.0
    tail = [
        float(max(0.0, 1.0 - gross)),
        float(realized_vol),
        float(gross),
        float(drawdown),
    ]
    obs = np.asarray(feature_values + [float(x) for x in weights] + tail, dtype=np.float32)
    obs[~np.isfinite(obs)] = 0.0
    return obs, {"features": feature_meta}, tuple(source_ids)


def _tail_cvar(values: Sequence[float], alpha: float) -> float:
    arr = np.asarray(list(values or []), dtype=np.float64)
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return 0.0
    count = max(1, int(math.ceil(float(arr.size) * max(0.0, min(1.0, float(alpha))))))
    worst = np.sort(arr)[:count]
    return float(np.mean(worst)) if worst.size else 0.0


def _reward_terms(
    *,
    gross_return: float,
    explicit_cost_fraction: float,
    turnover: float,
    slippage_bps: float,
    target_weights: np.ndarray,
    equity: float,
    peak_equity: float,
    recent_net_returns: Sequence[float],
    config: RiskSensitiveRewardConfig,
) -> tuple[float, dict[str, Any]]:
    fallback_cost = float(turnover) * (float(config.fallback_cost_bps) / 10000.0)
    slippage_cost = float(turnover) * (float(slippage_bps) / 10000.0)
    cost = max(0.0, float(explicit_cost_fraction)) + max(0.0, fallback_cost) + max(0.0, slippage_cost)
    net_return = float(gross_return) - float(cost)
    next_equity = max(1e-12, float(equity) * (1.0 + float(net_return)))
    next_peak = max(float(peak_equity), float(next_equity))
    drawdown = max(0.0, 1.0 - (float(next_equity) / max(1e-12, float(next_peak))))
    concentration = float(np.sum(np.square(np.asarray(target_weights, dtype=np.float64))))
    tail_window = list(recent_net_returns)[-max(1, int(config.cvar_window)) :] + [float(net_return)]
    cvar = _tail_cvar(tail_window, float(config.cvar_alpha))
    tail_loss = max(0.0, -float(cvar))
    drawdown_excess = max(0.0, float(drawdown) - float(config.drawdown_threshold))
    reward = (
        float(net_return)
        - float(config.drawdown_penalty) * float(drawdown_excess)
        - float(config.turnover_penalty) * float(turnover)
        - float(config.slippage_penalty) * float(slippage_cost)
        - float(config.concentration_penalty) * float(concentration)
        - float(config.cvar_penalty) * float(tail_loss)
    )
    return float(reward), {
        "gross_return": float(gross_return),
        "net_pnl_after_costs": float(net_return),
        "cost": float(cost),
        "fallback_cost": float(fallback_cost),
        "explicit_cost_fraction": float(explicit_cost_fraction),
        "slippage_bps": float(slippage_bps),
        "slippage_cost": float(slippage_cost),
        "turnover": float(turnover),
        "drawdown": float(drawdown),
        "drawdown_excess": float(drawdown_excess),
        "concentration": float(concentration),
        "cvar": float(cvar),
        "tail_loss": float(tail_loss),
        "reward": float(reward),
        "next_equity": float(next_equity),
        "next_peak_equity": float(next_peak),
    }


def _dataset_hash(config: OfflineDatasetConfig, transitions: Sequence[OfflineTransition]) -> str:
    payload = {
        "config": config.to_dict(),
        "transitions": [row.to_dict() for row in transitions],
    }
    return hashlib.sha256(_json_dumps(payload).encode("utf-8")).hexdigest()


def build_offline_rl_dataset(con: Any, config: OfflineDatasetConfig | Mapping[str, Any]) -> OfflineRLDataset:
    """Build a point-in-time offline RL dataset from historical runtime tables."""

    cfg = config if isinstance(config, OfflineDatasetConfig) else OfflineDatasetConfig(**dict(config))
    universe = _derive_universe(con, cfg)
    diagnostics: dict[str, Any] = {
        "source_tables": ["portfolio_orders", "model_feature_snapshots", "prices", "execution_fills", "broker_fills"],
        "pit_rejected": 0,
        "missing_outcomes": 0,
        "missing_features": 0,
        "input_orders": 0,
    }
    if not universe:
        return OfflineRLDataset(cfg, tuple(), "", {}, {**diagnostics, "status": "empty_universe"})

    orders = _load_orders(con, cfg, universe)
    diagnostics["input_orders"] = int(len(orders))
    grouped: dict[int, list[dict[str, Any]]] = {}
    for order in orders:
        grouped.setdefault(int(order["ts_ms"]), []).append(order)

    current = np.zeros(len(universe), dtype=np.float64)
    equity = 1.0
    peak_equity = 1.0
    recent_net_returns: list[float] = []
    transitions: list[OfflineTransition] = []
    source_models: dict[str, int] = {}

    for ts_ms in sorted(grouped):
        rows = grouped[ts_ms]
        for row in rows:
            source_models[str(row.get("model_id") or "")] = source_models.get(str(row.get("model_id") or ""), 0) + 1
        obs, obs_meta, source_ids = _observation(
            con=con,
            universe=universe,
            weights=current,
            ts_ms=int(ts_ms),
            equity=float(equity),
            peak_equity=float(peak_equity),
            recent_returns=recent_net_returns,
            config=cfg,
        )
        if obs is None:
            diagnostics["pit_rejected"] = int(diagnostics["pit_rejected"]) + 1
            continue
        if any((not dict(obs_meta.get("features") or {}).get(sym)) for sym in universe):
            diagnostics["missing_features"] = int(diagnostics["missing_features"]) + 1

        target = current.copy()
        order_ids: list[int] = []
        for row in rows:
            sym = str(row.get("symbol") or "").upper().strip()
            if sym not in universe:
                continue
            idx = universe.index(sym)
            target[idx] = _signed_weight(row.get("to_side"), row.get("to_weight"))
            order_ids.append(int(row["id"]))
        target = clip_and_normalize_action(target, max_w=float(cfg.max_w), leverage_cap=float(cfg.leverage_cap)).astype(np.float64)
        turnover = float(np.sum(np.abs(target - current)))
        asset_returns, outcome_meta = _forward_returns(con, universe=universe, ts_ms=int(ts_ms), horizon_ms=int(cfg.horizon_ms))
        if outcome_meta["missing_price_symbols"] and bool(cfg.require_outcomes):
            diagnostics["missing_outcomes"] = int(diagnostics["missing_outcomes"]) + 1
            continue

        gross_return = float(np.dot(target, asset_returns))
        fill_cost_abs, slippage_bps = _sum_fill_costs(con, order_ids)
        explicit_cost_fraction = float(fill_cost_abs / max(1e-12, equity))
        reward, reward_meta = _reward_terms(
            gross_return=float(gross_return),
            explicit_cost_fraction=float(explicit_cost_fraction),
            turnover=float(turnover),
            slippage_bps=float(slippage_bps),
            target_weights=target,
            equity=float(equity),
            peak_equity=float(peak_equity),
            recent_net_returns=recent_net_returns,
            config=cfg.reward,
        )
        action = tuple(float(x) for x in target)
        logged_action = "weights:" + ",".join(f"{sym}={action[idx]:.8f}" for idx, sym in enumerate(universe))
        meta = {
            "offline_rl": True,
            "shadow_only": True,
            "source": "engine.rl.offline_dataset",
            "orders": rows,
            "observation": obs_meta,
            "outcome": outcome_meta,
            "reward": reward_meta,
        }
        transition = OfflineTransition(
            ts_ms=int(ts_ms),
            universe=tuple(universe),
            observation=tuple(float(x) for x in obs.reshape(-1)),
            action=action,
            logged_action=logged_action,
            target_action=logged_action,
            reward=float(reward),
            outcome=float(reward),
            logged_model_estimate=float(reward),
            target_model_estimate=float(reward),
            behavior_propensity=1.0,
            target_propensity=1.0,
            obs_hash=observation_hash(obs),
            source_ids=tuple(list(source_ids) + [f"portfolio_orders:{oid}" for oid in order_ids]),
            meta=meta,
        )
        transitions.append(transition)
        current = target
        equity = float(reward_meta["next_equity"])
        peak_equity = float(reward_meta["next_peak_equity"])
        recent_net_returns.append(float(reward_meta["net_pnl_after_costs"]))

    dataset_h = _dataset_hash(cfg, transitions)
    behavior_policy = {
        "source": "portfolio_orders",
        "policy_family": "logged_behavior",
        "source_model_counts": dict(source_models),
        "deterministic_propensity": 1.0,
    }
    diagnostics.update(
        {
            "status": "ok" if len(transitions) >= int(cfg.min_rows) else "insufficient_rows",
            "rows": int(len(transitions)),
            "universe": list(universe),
            "observation_dim": int(len(transitions[0].observation)) if transitions else 0,
            "action_dim": int(len(transitions[0].action)) if transitions else len(universe),
            "dataset_hash": str(dataset_h),
        }
    )
    return OfflineRLDataset(cfg, tuple(transitions), dataset_h, behavior_policy, diagnostics)


def save_offline_dataset(dataset: OfflineRLDataset, artifact_dir: str | Path | None = None) -> Path:
    root = Path(artifact_dir or Path(dataset.config.artifact_root) / "datasets" / f"{int(time.time() * 1000)}_{dataset.dataset_hash[:8]}")
    root.mkdir(parents=True, exist_ok=True)
    metadata = {
        "dataset_hash": dataset.dataset_hash,
        "config": dataset.config.to_dict(),
        "behavior_policy": dataset.behavior_policy,
        "diagnostics": dataset.diagnostics,
    }
    (root / "metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")
    with (root / "transitions.jsonl").open("w", encoding="utf-8") as fh:
        for transition in dataset.transitions:
            fh.write(_json_dumps(transition.to_dict()) + "\n")
    return root


def load_offline_dataset(path: str | Path) -> OfflineRLDataset:
    root = Path(path)
    metadata = json.loads((root / "metadata.json").read_text(encoding="utf-8"))
    cfg_payload = dict(metadata.get("config") or {})
    reward_payload = cfg_payload.pop("reward", None)
    if isinstance(reward_payload, Mapping):
        cfg_payload["reward"] = RiskSensitiveRewardConfig(**dict(reward_payload))
    config = OfflineDatasetConfig(**cfg_payload)
    transitions: list[OfflineTransition] = []
    for line in (root / "transitions.jsonl").read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        transitions.append(
            OfflineTransition(
                ts_ms=int(row["ts_ms"]),
                universe=tuple(str(x) for x in row.get("universe") or []),
                observation=tuple(float(x) for x in row.get("observation") or []),
                action=tuple(float(x) for x in row.get("action") or []),
                logged_action=str(row.get("logged_action") or ""),
                target_action=str(row.get("target_action") or ""),
                reward=float(row.get("reward") or 0.0),
                outcome=float(row.get("outcome") or 0.0),
                logged_model_estimate=float(row.get("logged_model_estimate") or 0.0),
                target_model_estimate=float(row.get("target_model_estimate") or 0.0),
                behavior_propensity=float(row.get("behavior_propensity") or 0.0),
                target_propensity=float(row.get("target_propensity") or 0.0),
                obs_hash=str(row.get("obs_hash") or ""),
                source_ids=tuple(str(x) for x in row.get("source_ids") or []),
                meta=dict(row.get("meta") or {}),
            )
        )
    return OfflineRLDataset(
        config=config,
        transitions=tuple(transitions),
        dataset_hash=str(metadata.get("dataset_hash") or ""),
        behavior_policy=dict(metadata.get("behavior_policy") or {}),
        diagnostics=dict(metadata.get("diagnostics") or {}),
    )
