"""Shadow logging for offline portfolio RL policies."""

from __future__ import annotations

import time
from typing import Any, Callable, Mapping, Optional, Sequence

import numpy as np

from engine.rl.offline_policy import BehaviorCloningPolicy
from engine.rl.portfolio_env import observation_hash
from engine.rl.shadow_runner import _insert_shadow_decision, ensure_shadow_schema
from engine.rl.wrappers import clip_and_normalize_action


KillSwitchFn = Callable[[Optional[Any]], tuple[bool, str, dict[str, Any]]]


def _default_kill_switch(con: Any) -> tuple[bool, str, dict[str, Any]]:
    try:
        from engine.execution.kill_switch import execution_allowed

        allowed, reason, meta = execution_allowed(con=con, symbol="*", regime=None, model_id="offline_rl_portfolio_shadow")
        return bool(allowed), str(reason or ""), dict(meta or {})
    except Exception as exc:
        return False, "kill_switch_error", {"error": f"{type(exc).__name__}: {exc}"}


def _json_meta(value: Mapping[str, Any]) -> str:
    import json

    return json.dumps(dict(value or {}), separators=(",", ":"), sort_keys=True, default=str)


def log_offline_shadow_decisions(
    *,
    con: Any,
    policy: BehaviorCloningPolicy,
    universe: Sequence[str],
    observation: Any,
    live_weights: Mapping[str, float] | None = None,
    ts_ms: int | None = None,
    kill_switch_fn: KillSwitchFn | None = None,
    evidence: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Persist advisory offline-RL target-weight deltas.

    The function writes only to ``rl_shadow_decisions``. It never places orders
    and returns ``paused_kill_switch`` without writes when the kill switch blocks.
    """

    ts = int(ts_ms if ts_ms is not None else time.time() * 1000)
    symbols = [str(sym).upper().strip() for sym in universe if str(sym).strip()]
    ensure_shadow_schema(con)
    allowed, reason, kill_meta = (kill_switch_fn or _default_kill_switch)(con)
    if not bool(allowed):
        return {
            "ok": True,
            "status": "paused_kill_switch",
            "reason": str(reason),
            "meta": dict(kill_meta or {}),
            "rows": 0,
        }

    obs = np.asarray(observation, dtype=np.float32).reshape(-1)
    action = policy.predict(obs, deterministic=True)
    action = clip_and_normalize_action(action, max_w=float(policy.config.max_w), leverage_cap=float(policy.config.leverage_cap))
    live = {str(k).upper().strip(): float(v or 0.0) for k, v in dict(live_weights or {}).items()}
    obs_h = observation_hash(obs)
    evidence_payload = dict(evidence or {})
    rows = 0
    for idx, sym in enumerate(symbols):
        live_w = float(live.get(sym, 0.0))
        rl_w = float(action[idx]) if idx < len(action) else 0.0
        row = {
            "ts": int(ts),
            "model_name": str(policy.config.model_name),
            "candidate_type": "rl",
            "symbol": str(sym),
            "live_weight": float(live_w),
            "rl_weight": float(rl_w),
            "delta": float(rl_w - live_w),
            "obs_hash": str(obs_h),
            "behavior_propensity": None,
            "target_propensity": None,
            "outcome": None,
            "logged_model_estimate": None,
            "target_model_estimate": None,
            "meta_json": _json_meta(
                {
                    "offline_rl": True,
                    "shadow_only": True,
                    "dataset_hash": str(policy.dataset_hash),
                    "policy_hash32": policy.policy_hash32(),
                    "candidate_version": str(policy.config.candidate_version),
                    "live_weight": float(live_w),
                    "rl_weight": float(rl_w),
                    "evidence": evidence_payload,
                }
            ),
        }
        _insert_shadow_decision(con, row)
        rows += 1
    con.commit()
    return {
        "ok": True,
        "status": "logged",
        "rows": int(rows),
        "obs_hash": str(obs_h),
        "symbols": list(symbols),
        "model_name": str(policy.config.model_name),
        "dataset_hash": str(policy.dataset_hash),
    }
