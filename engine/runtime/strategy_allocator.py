"""
FILE: strategy_allocator.py

Runtime subsystem module for `strategy_allocator`.
"""

# engine/runtime/strategy_allocator.py
"""
Strategy Allocator (Meta Capital Engine)

Computes rolling, drawdown-aware, correlation-adjusted capital weights per strategy.

Inputs:
- execution_capital_efficiency (preferred)  [engine/execution/execution_ledger.py]
- strategy_shadow_runs (optional shadow proxy input)

Persists:
- strategy_metrics (window_days=0)   metrics_json includes allocator fields
- strategy_allocations (window_days=0) allocations_json includes normalized weights

Fail-open:
- If required tables are missing, returns empty allocations and does not raise.

Env (optional):
  STRATEGY_ALLOC_WINDOW_S=86400
  STRATEGY_ALLOC_BUCKET_S=900
  STRATEGY_ALLOC_CORR_GAMMA=1.5
  STRATEGY_ALLOC_CORR_CAP_THRESHOLD=0.85
  STRATEGY_ALLOC_CORR_CAP_MAX_SHARE=0.35
  STRATEGY_ALLOC_CORR_CAP_MIN_MULT=0.25
  STRATEGY_ALLOC_CORR_CAP_MIN_PEERS=1
  STRATEGY_ALLOC_DD_TH=0.10
  STRATEGY_ALLOC_DD_FLOOR=0.10
  STRATEGY_ALLOC_MIN_SHARE=0.0
  STRATEGY_ALLOC_MAX_SHARE=1.0
  STRATEGY_ALLOC_RISK_BUDGETS_JSON='{"baseline":0.60,"conservative":0.40}'
  STRATEGY_ALLOC_SCORE_FLOOR=0.0
  STRATEGY_ALLOC_SHADOW_PROXY_ALPHA=0.35

NOTE:
- This module does NOT change trade generation.
- It only produces weights for capital allocation across strategies.
"""

import json
import logging
import math
import os
import time
from typing import Any, Dict, List, Optional, Tuple

from engine.runtime.allocator_status import _safe_float, _table_exists
from engine.runtime.event_log import record_allocator_decision
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger


DEFAULT_WINDOW_S = int(os.environ.get("STRATEGY_ALLOC_WINDOW_S", "86400"))
DEFAULT_BUCKET_S = int(os.environ.get("STRATEGY_ALLOC_BUCKET_S", "900"))

CORR_GAMMA = float(os.environ.get("STRATEGY_ALLOC_CORR_GAMMA", "1.5"))
CORR_CAP_THRESHOLD = float(os.environ.get("STRATEGY_ALLOC_CORR_CAP_THRESHOLD", "0.85"))
CORR_CAP_MAX_SHARE = float(os.environ.get("STRATEGY_ALLOC_CORR_CAP_MAX_SHARE", "0.35"))
CORR_CAP_MIN_MULT = float(os.environ.get("STRATEGY_ALLOC_CORR_CAP_MIN_MULT", "0.25"))
CORR_CAP_MIN_PEERS = int(os.environ.get("STRATEGY_ALLOC_CORR_CAP_MIN_PEERS", "1"))
CORR_CAP_MIN_OBS = int(os.environ.get("STRATEGY_ALLOC_CORR_CAP_MIN_OBS", "3"))
CORR_CAP_POSITIVE_ONLY = str(os.environ.get("STRATEGY_ALLOC_CORR_CAP_POSITIVE_ONLY", "1")).strip() == "1"

DD_TH = float(os.environ.get("STRATEGY_ALLOC_DD_TH", "0.10"))
DD_FLOOR = float(os.environ.get("STRATEGY_ALLOC_DD_FLOOR", "0.10"))

MIN_SHARE = float(os.environ.get("STRATEGY_ALLOC_MIN_SHARE", "0.0"))
MAX_SHARE = float(os.environ.get("STRATEGY_ALLOC_MAX_SHARE", "1.0"))
EXPLORATION_MIN_ACTIVE_FLOOR = float(os.environ.get("STRATEGY_ALLOC_EXPLORATION_MIN_ACTIVE_FLOOR", "0.02"))
EXPLORATION_SOFTMAX_TEMP = float(os.environ.get("STRATEGY_ALLOC_EXPLORATION_SOFTMAX_TEMP", "0.35"))
EXPLORATION_SOFTMAX_MIX = float(os.environ.get("STRATEGY_ALLOC_EXPLORATION_SOFTMAX_MIX", "0.15"))
FRESHNESS_HALFLIFE_S = int(os.environ.get("STRATEGY_ALLOC_FRESHNESS_HALFLIFE_S", "21600"))
FRESHNESS_FLOOR = float(os.environ.get("STRATEGY_ALLOC_FRESHNESS_FLOOR", "0.30"))
MIN_SAMPLE_ROWS = int(os.environ.get("STRATEGY_ALLOC_MIN_SAMPLE_ROWS", "8"))
MIN_SHADOW_RUNS = int(os.environ.get("STRATEGY_ALLOC_MIN_SHADOW_RUNS", "4"))
CONCENTRATION_PENALTY_GAMMA = float(os.environ.get("STRATEGY_ALLOC_CONCENTRATION_PENALTY_GAMMA", "0.60"))
CONFIDENCE_QUALITY_FLOOR = float(os.environ.get("STRATEGY_ALLOC_CONFIDENCE_QUALITY_FLOOR", "0.55"))
ABS_Z_QUALITY_SCALE = float(os.environ.get("STRATEGY_ALLOC_ABS_Z_QUALITY_SCALE", "1.50"))

SCORE_FLOOR = float(os.environ.get("STRATEGY_ALLOC_SCORE_FLOOR", "0.0"))
SHADOW_PROXY_ALPHA = float(os.environ.get("STRATEGY_ALLOC_SHADOW_PROXY_ALPHA", "0.35"))

COOLDOWN_S = int(os.environ.get("STRATEGY_ALLOC_COOLDOWN_S", "21600"))
LOSS_STREAK_TRIGGER = int(os.environ.get("STRATEGY_ALLOC_LOSS_STREAK_TRIGGER", "3"))
LOSS_STREAK_STEP = float(os.environ.get("STRATEGY_ALLOC_LOSS_STREAK_STEP", "0.15"))
LOSS_STREAK_FLOOR = float(os.environ.get("STRATEGY_ALLOC_LOSS_STREAK_FLOOR", "0.35"))
DD_COOLDOWN_TH = float(os.environ.get("STRATEGY_ALLOC_DD_COOLDOWN_TH", "0.15"))
DD_COOLDOWN_FLOOR = float(os.environ.get("STRATEGY_ALLOC_DD_COOLDOWN_FLOOR", "0.50"))

_RISK_BUDGETS_RAW = os.environ.get("STRATEGY_ALLOC_RISK_BUDGETS_JSON", "").strip()
_ALLOCATOR_CONFIG_FILE = os.environ.get("STRATEGY_ALLOC_CONFIG_FILE", "").strip()
LOG = get_logger("engine.runtime.strategy_allocator")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(event: str, code: str, error: BaseException, *, warn_key: str | None = None, **extra: Any) -> None:
    if warn_key and warn_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=event,
        code=code,
        message=event,
        error=error,
        level=logging.WARNING,
        component="engine.runtime.strategy_allocator",
        extra=extra or None,
        persist=False,
    )
    if warn_key:
        _WARNED_NONFATAL_KEYS.add(warn_key)

try:
    from engine.runtime.alpha_decay_monitor import (
        compute_alpha_decay_snapshot,
        apply_alpha_decay_runtime_state,
        persist_alpha_decay_state,
    )
except Exception:
    compute_alpha_decay_snapshot = None
    apply_alpha_decay_runtime_state = None
    persist_alpha_decay_state = None


def _now_ms() -> int:
    return int(time.time() * 1000)


def _clamp(value: Any, lo: float, hi: float) -> float:
    try:
        v = float(value)
    except Exception:
        v = float(lo)
    if not math.isfinite(v):
        v = float(lo)
    return float(max(float(lo), min(float(hi), float(v))))


def _normalize_nonnegative_weights(weights: Dict[str, Any]) -> Dict[str, float]:
    cleaned: Dict[str, float] = {}
    total = 0.0
    for key, value in (weights or {}).items():
        vv = _safe_float(value, 0.0)
        if not math.isfinite(vv) or vv <= 0.0:
            cleaned[str(key)] = 0.0
            continue
        cleaned[str(key)] = float(vv)
        total += float(vv)

    if total <= 1e-12:
        return {str(key): 0.0 for key in cleaned.keys()}
    return {str(key): float(val) / float(total) for key, val in cleaned.items()}

def _parse_budget_map(obj: Any) -> Dict[str, float]:
    if not isinstance(obj, dict):
        return {}
    out: Dict[str, float] = {}
    for k, v in obj.items():
        try:
            kk = str(k).strip()
            if not kk:
                continue
            out[kk] = max(0.0, float(v))
        except Exception as e:
            _warn_nonfatal(
                "strategy_allocator_parse_budget_map_failed",
                "STRATEGY_ALLOCATOR_PARSE_BUDGET_MAP_FAILED",
                e,
                warn_key=f"parse_budget_map:{k!r}",
                budget_key=str(k),
            )
            continue
    return out


def _load_allocator_config() -> Dict[str, Any]:
    # Runtime config can come from a JSON file or env vars; file-based config is
    # checked first so operators can override without editing code.
    candidates: List[str] = []
    if _ALLOCATOR_CONFIG_FILE:
        candidates.append(str(_ALLOCATOR_CONFIG_FILE))
    candidates.append(os.path.join("data", "strategy_allocator_config.json"))

    for path in candidates:
        try:
            if not path or not os.path.exists(path):
                continue
            with open(path, "r", encoding="utf-8") as f:
                obj = json.load(f)
            if isinstance(obj, dict):
                return dict(obj)
        except Exception as e:
            _warn_nonfatal(
                "strategy_allocator_load_config_failed",
                "STRATEGY_ALLOCATOR_LOAD_CONFIG_FAILED",
                e,
                warn_key=f"allocator_config:{path}",
                config_path=str(path),
            )
            continue
    return {}


def _parse_risk_budgets() -> Dict[str, float]:
    out: Dict[str, float] = {}

    try:
        cfg = _load_allocator_config()
        cfg_rb = (
            cfg.get("risk_budgets")
            or cfg.get("strategy_risk_budgets")
            or ((cfg.get("allocator") or {}).get("risk_budgets") if isinstance(cfg.get("allocator"), dict) else {})
        )
        out.update(_parse_budget_map(cfg_rb))
    except Exception as e:
        _warn_nonfatal(
            "strategy_allocator_config_risk_budgets_load_failed",
            "STRATEGY_ALLOCATOR_CONFIG_RISK_BUDGETS_LOAD_FAILED",
            e,
            warn_key="strategy_allocator_config_risk_budgets_load_failed",
            config_file=str(_ALLOCATOR_CONFIG_FILE),
        )

    if _RISK_BUDGETS_RAW:
        try:
            out.update(_parse_budget_map(json.loads(_RISK_BUDGETS_RAW)))
        except Exception as e:
            _warn_nonfatal(
                "strategy_allocator_env_risk_budgets_parse_failed",
                "STRATEGY_ALLOCATOR_ENV_RISK_BUDGETS_PARSE_FAILED",
                e,
                warn_key="strategy_allocator_env_risk_budgets_parse_failed",
            )

    return out


def _load_strategy_registry_meta(con) -> Dict[str, Dict[str, Any]]:
    # Registry metadata is optional; allocator logic degrades gracefully if the
    # strategy registry table does not exist yet.
    if not _table_exists(con, "strategy_registry"):
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    try:
        rows = con.execute(
            """
            SELECT strategy_name, enabled, stage, meta_json
            FROM strategy_registry
            """
        ).fetchall()
    except Exception as e:
        _warn_nonfatal(
            "strategy_allocator_load_strategy_registry_meta_failed",
            "STRATEGY_ALLOCATOR_LOAD_STRATEGY_REGISTRY_META_FAILED",
            e,
            warn_key="load_strategy_registry_meta",
        )
        registry_meta: Dict[str, Dict[str, Any]] = {}
        return registry_meta

    for r in rows or []:
        try:
            sname = str(r[0] or "").strip()
            if not sname:
                continue

            meta_obj: Dict[str, Any] = {}
            try:
                raw = json.loads(r[3] or "{}")
                if isinstance(raw, dict):
                    meta_obj = dict(raw)
            except Exception:
                meta_obj = {}

            meta_obj["enabled"] = int(r[1] or 0)
            meta_obj["stage"] = str(r[2] or "").strip().lower()
            out[sname] = meta_obj
        except Exception as e:
            _warn_nonfatal(
                "strategy_allocator_registry_meta_row_failed",
                "STRATEGY_ALLOCATOR_REGISTRY_META_ROW_FAILED",
                e,
                strategy_name=str(r[0] or "") if len(r) > 0 else "",
            )
            continue

    return out


def _bucket_ts(ts_ms: int, bucket_s: int) -> int:
    b = int(max(1, int(bucket_s)))
    return int((int(ts_ms) // int(b * 1000)) * int(b * 1000))


def _load_active_strategy_cooldowns(con, now_ms: int) -> Dict[str, Dict[str, Any]]:
    # Cooldowns are a runtime overlay that can temporarily compress strategy
    # weights without changing the underlying metrics history.
    if int(now_ms) <= 0:
        return {}

    try:
        rows = con.execute(
            """
            SELECT strategy_name, trigger_ts_ms, cooldown_until_ts_ms, cooldown_scale, reason_json
            FROM strategy_cooldowns
            WHERE cooldown_until_ts_ms > ?
            ORDER BY cooldown_until_ts_ms DESC
            """,
            (int(now_ms),),
        ).fetchall()
    except Exception as e:
        _warn_nonfatal(
            "strategy_allocator_load_strategy_cooldowns_failed",
            "STRATEGY_ALLOCATOR_LOAD_STRATEGY_COOLDOWNS_FAILED",
            e,
            warn_key="load_active_strategy_cooldowns",
            now_ms=int(now_ms),
        )
        cooldowns: Dict[str, Dict[str, Any]] = {}
        return cooldowns

    out: Dict[str, Dict[str, Any]] = {}
    for r in rows or []:
        try:
            name = str(r[0] or "").strip()
            if not name:
                continue
            reason = json.loads(r[4] or "{}")
            if not isinstance(reason, dict):
                reason = {}
            out[name] = {
                "trigger_ts_ms": int(r[1] or 0),
                "cooldown_until_ts_ms": int(r[2] or 0),
                "cooldown_scale": float(max(0.0, min(1.0, _safe_float(r[3], 1.0)))),
                "reason": reason,
            }
        except Exception as e:
            _warn_nonfatal(
                "strategy_allocator_cooldown_row_failed",
                "STRATEGY_ALLOCATOR_COOLDOWN_ROW_FAILED",
                e,
                strategy_name=str(r[0] or "") if len(r) > 0 else "",
            )
            continue
    return out


def _upsert_strategy_cooldown(
    con,
    strategy_name: str,
    *,
    trigger_ts_ms: int,
    cooldown_until_ts_ms: int,
    cooldown_scale: float,
    reason: Dict[str, Any],
) -> None:
    # Cooldowns are persisted so allocator decisions survive process restarts
    # and remain visible to operator tooling.
    if not str(strategy_name or "").strip():
        return
    con.execute(
        """
        INSERT INTO strategy_cooldowns(
          strategy_name,
          trigger_ts_ms,
          cooldown_until_ts_ms,
          cooldown_scale,
          reason_json
        )
        VALUES (?,?,?,?,?)
        ON CONFLICT(strategy_name) DO UPDATE SET
          trigger_ts_ms=excluded.trigger_ts_ms,
          cooldown_until_ts_ms=excluded.cooldown_until_ts_ms,
          cooldown_scale=excluded.cooldown_scale,
          reason_json=excluded.reason_json
        """,
        (
            str(strategy_name).strip(),
            int(trigger_ts_ms),
            int(cooldown_until_ts_ms),
            float(max(0.0, min(1.0, _safe_float(cooldown_scale, 1.0)))),
            json.dumps(dict(reason or {}), separators=(",", ":"), sort_keys=True),
        ),
    )


def _loss_streak_info(rows: List[Tuple[int, float]]) -> Dict[str, Any]:
    # Loss streak logic walks backward from the newest rows and stops at the
    # first non-loss so brief recoveries clear the cooldown pressure.
    streak = 0
    streak_pnl = 0.0
    last_loss_ts_ms = 0

    for ts_ms, pnl_net in reversed(rows or []):
        pnl = _safe_float(pnl_net, 0.0)
        if pnl < 0.0:
            streak += 1
            streak_pnl += float(pnl)
            if last_loss_ts_ms <= 0:
                last_loss_ts_ms = int(ts_ms or 0)
            continue
        break

    return {
        "loss_streak": int(streak),
        "loss_streak_pnl": float(streak_pnl),
        "last_loss_ts_ms": int(last_loss_ts_ms),
    }


def _cooldown_scale_from_loss_streak(loss_streak: int) -> float:
    if int(loss_streak) < int(max(1, LOSS_STREAK_TRIGGER)):
        return 1.0
    extra_losses = int(loss_streak) - int(max(1, LOSS_STREAK_TRIGGER)) + 1
    return float(max(float(LOSS_STREAK_FLOOR), 1.0 - (float(extra_losses) * float(LOSS_STREAK_STEP))))


def _cooldown_scale_from_drawdown(dd: float) -> float:
    dd_val = max(0.0, _safe_float(dd, 0.0))
    dd_th = max(1e-9, float(DD_COOLDOWN_TH))
    if dd_val <= dd_th:
        return 1.0
    excess_ratio = min(1.0, max(0.0, (float(dd_val) - float(dd_th)) / float(dd_th)))
    return float(max(float(DD_COOLDOWN_FLOOR), 1.0 - excess_ratio))


def _merge_reason_dicts(*parts: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for part in parts:
        if isinstance(part, dict):
            out.update(part)
    return out


def _stddev(vals: List[float]) -> float:
    if not vals:
        return 0.0
    if len(vals) == 1:
        return 0.0
    m = sum(vals) / float(len(vals))
    var = sum((x - m) ** 2 for x in vals) / float(len(vals) - 1)
    return math.sqrt(max(0.0, var))


def _max_drawdown_from_pnl_series(pnl_series: List[Tuple[int, float]]) -> float:
    """pnl_series: list of (bucket_ts_ms, pnl) sorted by ts.
    Returns positive fraction-like magnitude computed on cumulative pnl.
    """
    if not pnl_series:
        return 0.0

    eq = 0.0
    peak = 0.0
    max_dd = 0.0

    for _, pnl in pnl_series:
        eq += float(pnl)
        if eq > peak:
            peak = eq
        dd = peak - eq
        if dd > max_dd:
            max_dd = dd

    denom = max(1e-9, abs(float(peak)))
    return float(max_dd) / float(denom)


def _corr(a: List[float], b: List[float], *, min_obs: int = 3) -> Tuple[float, int]:
    if not a or not b or len(a) != len(b):
        return 0.0, 0

    aa: List[float] = []
    bb: List[float] = []
    for x, y in zip(a, b):
        xf = _safe_float(x, 0.0)
        yf = _safe_float(y, 0.0)
        if abs(xf) <= 1e-12 or abs(yf) <= 1e-12:
            continue
        aa.append(float(xf))
        bb.append(float(yf))

    n_obs = int(len(aa))
    if n_obs < int(max(3, int(min_obs))):
        return 0.0, int(n_obs)

    ma = sum(aa) / len(aa)
    mb = sum(bb) / len(bb)
    va = sum((x - ma) ** 2 for x in aa)
    vb = sum((y - mb) ** 2 for y in bb)
    if va <= 1e-12 or vb <= 1e-12:
        return 0.0, int(n_obs)
    cov = sum((aa[i] - ma) * (bb[i] - mb) for i in range(len(aa)))
    return float(cov / math.sqrt(va * vb)), int(n_obs)


def _build_corr_matrix(
    strategies: List[str],
    series: Dict[str, List[float]],
    *,
    min_obs: int,
) -> Tuple[Dict[str, Dict[str, float]], Dict[str, Dict[str, int]]]:
    out: Dict[str, Dict[str, float]] = {}
    obs: Dict[str, Dict[str, int]] = {}
    for s in strategies:
        out[s] = {}
        obs[s] = {}
    for i, si in enumerate(strategies):
        out.setdefault(si, {})[si] = 1.0
        obs.setdefault(si, {})[si] = int(len(series.get(si) or []))
        for j in range(i + 1, len(strategies)):
            sj = strategies[j]
            c, n_obs = _corr(series.get(si) or [], series.get(sj) or [], min_obs=int(min_obs))
            cc = max(-1.0, min(1.0, float(c)))
            out.setdefault(si, {})[sj] = float(cc)
            out.setdefault(sj, {})[si] = float(cc)
            obs.setdefault(si, {})[sj] = int(n_obs)
            obs.setdefault(sj, {})[si] = int(n_obs)
        for sj in strategies:
            out.setdefault(si, {}).setdefault(sj, 0.0 if sj != si else 1.0)
            obs.setdefault(si, {}).setdefault(sj, 0 if sj != si else int(len(series.get(si) or [])))
    return out, obs


def _apply_corr_threshold_caps(
    alloc: Dict[str, float],
    corr_matrix: Dict[str, Dict[str, float]],
    corr_obs: Dict[str, Dict[str, int]],
    strategies: List[str],
    details: Dict[str, Dict[str, Any]],
) -> Dict[str, float]:
    out = {str(k): float(v) for k, v in (alloc or {}).items()}

    th = max(0.0, min(0.999999, float(CORR_CAP_THRESHOLD)))
    base_cap = max(0.0, min(1.0, float(CORR_CAP_MAX_SHARE)))
    min_mult = max(0.0, min(1.0, float(CORR_CAP_MIN_MULT)))
    min_peers = max(1, int(CORR_CAP_MIN_PEERS))
    min_obs = max(1, int(CORR_CAP_MIN_OBS))
    positive_only = bool(CORR_CAP_POSITIVE_ONLY)

    if not out or not strategies or base_cap <= 0.0:
        return out

    for s in strategies:
        peers = []
        row = corr_matrix.get(s) or {}
        row_obs = corr_obs.get(s) or {}
        for other in strategies:
            if other == s:
                continue
            c = _safe_float(row.get(other), 0.0)
            n_obs = int(row_obs.get(other) or 0)
            if n_obs < int(min_obs):
                continue
            if positive_only:
                if float(c) >= float(th):
                    peers.append((str(other), float(c), int(n_obs)))
            else:
                if abs(float(c)) >= float(th):
                    peers.append((str(other), float(c), int(n_obs)))

        peers_sorted = sorted(peers, key=lambda item: abs(float(item[1])), reverse=True)
        details.setdefault(s, {})
        details[s]["corr_cap_peer_count"] = int(len(peers_sorted))
        details[s]["corr_cap_peers"] = [
            {"strategy": str(name), "corr": float(c), "n_obs": int(n_obs)}
            for name, c, n_obs in peers_sorted
        ]

        if len(peers_sorted) < min_peers:
            details[s]["corr_cap_multiplier"] = 1.0
            details[s]["corr_cap_share"] = float(base_cap)
            continue

        max_corr = max(float(c) for _, c, _ in peers_sorted) if positive_only else max(abs(float(c)) for _, c, _ in peers_sorted)
        excess = max(0.0, min(1.0, (float(max_corr) - th) / max(1e-9, 1.0 - th)))
        peer_scale = min(1.0, float(len(peers_sorted)) / float(max(1, len(strategies) - 1)))
        mult = 1.0 - (excess * peer_scale)
        mult = max(float(min_mult), min(1.0, float(mult)))

        cap_share = min(float(MAX_SHARE), float(base_cap) * float(mult))
        pre = float(out.get(s, 0.0))
        post = min(pre, float(cap_share))
        out[s] = float(post)

        details[s]["corr_cap_multiplier"] = float(mult)
        details[s]["corr_cap_share"] = float(cap_share)
        if post + 1e-12 < pre:
            details[s]["corr_cap_applied"] = True
            details[s]["corr_cap_pre"] = float(pre)
            details[s]["corr_cap_post"] = float(post)
            details[s]["corr_cap_max_abs_corr"] = float(abs(max_corr))
            details[s]["corr_cap_threshold"] = float(th)
            details[s]["corr_cap_min_obs"] = int(min_obs)
            details[s]["corr_cap_positive_only"] = bool(positive_only)

    gross = sum(float(out.get(s, 0.0)) for s in strategies)
    if gross > 1e-12:
        for s in strategies:
            out[s] = float(out.get(s, 0.0)) / float(gross)

    return out


def _read_exec_cap_eff(con, since_ms: int, now_ms: int) -> List[Tuple[int, str, float, float, float]]:
    """Returns rows: (ts_ms, strategy_name, pnl_net, return_per_risk, drawdown_contrib)

    Uses execution_capital_efficiency if present.
    """
    if not _table_exists(con, "execution_capital_efficiency"):
        return []

    try:
        rows = con.execute(
            """
            SELECT ts_ms, strategy_name, pnl_net, return_per_risk, drawdown_contrib
            FROM execution_capital_efficiency
            WHERE ts_ms BETWEEN ? AND ?
            ORDER BY ts_ms ASC
            """,
            (int(since_ms), int(now_ms)),
        ).fetchall()
    except Exception as e:
        _warn_nonfatal(
            "strategy_allocator_read_exec_cap_eff_failed",
            "STRATEGY_ALLOCATOR_READ_EXEC_CAP_EFF_FAILED",
            e,
            warn_key="read_exec_cap_eff",
            since_ms=int(since_ms),
            now_ms=int(now_ms),
        )
        exec_cap_eff_rows: List[Tuple[int, str, float, float, float]] = []
        return exec_cap_eff_rows

    out: List[Tuple[int, str, float, float, float]] = []
    for r in rows or []:
        try:
            ts_ms = int(r[0] or 0)
            name = str(r[1] or "").strip()
            if not name:
                continue
            pnl_net = _safe_float(r[2], 0.0)
            rpr = _safe_float(r[3], 0.0)
            dd_c = _safe_float(r[4], 0.0)
            out.append((ts_ms, name, pnl_net, rpr, dd_c))
        except Exception as e:
            _warn_nonfatal(
                "strategy_allocator_exec_cap_eff_row_failed",
                "STRATEGY_ALLOCATOR_EXEC_CAP_EFF_ROW_FAILED",
                e,
                strategy_name=str(r[1] or "") if len(r) > 1 else "",
            )
            continue

    return out


def _read_shadow_strategy_proxy(con, since_ms: int, now_ms: int) -> Dict[str, Dict[str, float]]:
    """
    Returns per strategy:
      {
        strategy_name: {
          n_runs,
          proxy_score,
          avg_conf,
          avg_abs_z,
          gross_target,
          concentration,
        }
      }

    Uses strategy_shadow_runs populated by portfolio shadow-mode evaluation.
    """
    if not _table_exists(con, "strategy_shadow_runs"):
        return {}

    try:
        rows = con.execute(
            """
            SELECT ts_ms, strategy_name, metrics_json
            FROM strategy_shadow_runs
            WHERE ts_ms BETWEEN ? AND ?
            ORDER BY ts_ms ASC
            """,
            (int(since_ms), int(now_ms)),
        ).fetchall()
    except Exception as e:
        _warn_nonfatal(
            "strategy_allocator_read_shadow_strategy_proxy_failed",
            "STRATEGY_ALLOCATOR_READ_SHADOW_STRATEGY_PROXY_FAILED",
            e,
            warn_key="read_shadow_strategy_proxy",
            since_ms=int(since_ms),
            now_ms=int(now_ms),
        )
        shadow_proxy: Dict[str, Dict[str, float]] = {}
        return shadow_proxy

    agg: Dict[str, Dict[str, float]] = {}

    for r in rows or []:
        try:
            name = str(r[1] or "").strip()
            if not name:
                continue

            mj = json.loads(r[2] or "{}")
            if not isinstance(mj, dict):
                mj = {}

            cur = agg.get(name) or {
                "n_runs": 0.0,
                "proxy_score_sum": 0.0,
                "avg_conf_sum": 0.0,
                "avg_abs_z_sum": 0.0,
                "gross_target_sum": 0.0,
                "concentration_sum": 0.0,
                "last_ts_ms": 0.0,
            }

            cur["n_runs"] += 1.0
            cur["proxy_score_sum"] += _safe_float(mj.get("proxy_score"), 0.0)
            cur["avg_conf_sum"] += _safe_float(mj.get("avg_conf"), 0.0)
            cur["avg_abs_z_sum"] += _safe_float(mj.get("avg_abs_z"), 0.0)
            cur["gross_target_sum"] += _safe_float(mj.get("gross_target"), 0.0)
            cur["concentration_sum"] += _safe_float(mj.get("concentration"), 1.0)
            cur["last_ts_ms"] = float(max(cur.get("last_ts_ms", 0.0), _safe_float(r[0], 0.0)))

            agg[name] = cur
        except Exception as e:
            _warn_nonfatal(
                "strategy_allocator_shadow_strategy_proxy_row_failed",
                "STRATEGY_ALLOCATOR_SHADOW_STRATEGY_PROXY_ROW_FAILED",
                e,
                strategy_name=str(r[1] or "") if len(r) > 1 else "",
            )
            continue

    out: Dict[str, Dict[str, float]] = {}
    for name, cur in agg.items():
        n_runs = max(1.0, float(cur.get("n_runs", 0.0)))
        out[name] = {
            "n_runs": float(n_runs),
            "proxy_score": float(cur.get("proxy_score_sum", 0.0)) / float(n_runs),
            "avg_conf": float(cur.get("avg_conf_sum", 0.0)) / float(n_runs),
            "avg_abs_z": float(cur.get("avg_abs_z_sum", 0.0)) / float(n_runs),
            "gross_target": float(cur.get("gross_target_sum", 0.0)) / float(n_runs),
            "concentration": float(cur.get("concentration_sum", 0.0)) / float(n_runs),
            "last_ts_ms": int(cur.get("last_ts_ms", 0.0) or 0.0),
        }

    return out


def compute_and_persist_strategy_allocations(con, *, now_ms: Optional[int] = None) -> Dict[str, Any]:
    """Computes allocations and persists into strategy_metrics + strategy_allocations.

    Returns:
      {
        "ok": bool,
        "ts_ms": int,
        "window_s": int,
        "bucket_s": int,
        "allocations": {strategy_name: weight},
        "details": {strategy_name: {...metrics...}},
      }
    """
    ts_ms = int(now_ms) if now_ms is not None else _now_ms()
    window_s = int(max(60, int(DEFAULT_WINDOW_S)))
    bucket_s = int(max(60, int(DEFAULT_BUCKET_S)))
    cadence_s = int(max(30, int(os.environ.get("STRATEGY_ALLOC_CADENCE_S", "45"))))
    score_disable_th = _safe_float(os.environ.get("STRATEGY_ALLOC_DISABLE_SCORE_TH", "-0.25"), -0.25)
    dd_disable_th = _safe_float(os.environ.get("STRATEGY_ALLOC_DISABLE_DD_TH", "0.35"), 0.35)
    portfolio_target_gross = max(
        0.0,
        _safe_float(
            (_load_allocator_config().get("portfolio_target_gross") if isinstance(_load_allocator_config(), dict) else None),
            _safe_float(os.environ.get("PORTFOLIO_TARGET_GROSS", "1.0"), 1.0),
        ),
    )

    try:
        row = None
        if _table_exists(con, "strategy_allocations"):
            try:
                from engine.cache.wrappers.strategy_allocations import read_strategy_allocations

                cached_alloc = read_strategy_allocations(window_days=0)
                if cached_alloc:
                    row = (
                        int(cached_alloc.get("ts_ms") or 0),
                        json.dumps(dict(cached_alloc.get("allocations") or {}), separators=(",", ":"), sort_keys=True),
                        json.dumps(dict(cached_alloc.get("reason") or {}), separators=(",", ":"), sort_keys=True),
                    )
            except Exception:
                row = None
            if row is None:
                row = con.execute(
                    """
                    SELECT ts_ms, allocations_json, reason_json
                    FROM strategy_allocations
                    WHERE window_days=0
                    ORDER BY ts_ms DESC
                    LIMIT 1
                    """
                ).fetchone()

        if row:
            last_ts_ms = int(row[0] or 0)
            if last_ts_ms > 0 and (int(ts_ms) - int(last_ts_ms)) < int(cadence_s * 1000):
                alloc = json.loads(row[1] or "{}")
                if not isinstance(alloc, dict):
                    alloc = {}

                details: Dict[str, Dict[str, Any]] = {}
                if _table_exists(con, "strategy_metrics"):
                    drows = con.execute(
                        """
                        SELECT strategy_name, metrics_json
                        FROM strategy_metrics
                        WHERE window_days=0
                        """
                    ).fetchall()
                    for sname, mj in drows or []:
                        try:
                            m = json.loads(mj or "{}")
                            if isinstance(m, dict):
                                details[str(sname)] = m
                        except Exception as e:
                            _warn_nonfatal(
                                "strategy_allocator_cached_strategy_metrics_row_failed",
                                "STRATEGY_ALLOCATOR_CACHED_STRATEGY_METRICS_ROW_FAILED",
                                e,
                                strategy_name=str(sname),
                            )
                            continue

                reason_payload = {}
                try:
                    reason_payload = json.loads(row[2] or "{}")
                    if not isinstance(reason_payload, dict):
                        reason_payload = {}
                except Exception:
                    reason_payload = {}

                reason_payload["cached"] = True
                reason_payload["cadence_s"] = int(cadence_s)

                alpha_runtime = dict(reason_payload.get("alpha_decay_runtime") or {})
                if callable(apply_alpha_decay_runtime_state):
                    try:
                        alpha_runtime = apply_alpha_decay_runtime_state(details=dict(details), ts_ms=int(ts_ms)) or alpha_runtime
                    except Exception:
                        alpha_runtime = dict(reason_payload.get("alpha_decay_runtime") or {})

                alpha_persist = {}
                if callable(persist_alpha_decay_state):
                    try:
                        alpha_persist = persist_alpha_decay_state(
                            con,
                            details=dict(details),
                            runtime_summary=dict(alpha_runtime),
                            ts_ms=int(ts_ms),
                            window_days=0,
                            bucket_s=int(bucket_s),
                        ) or {}
                    except Exception:
                        alpha_persist = {}

                reason_payload["alpha_decay_runtime"] = dict(alpha_runtime)
                reason_payload["alpha_decay_persist"] = dict(alpha_persist)

                return {
                    "ok": True,
                    "ts_ms": int(last_ts_ms),
                    "window_s": int(window_s),
                    "bucket_s": int(bucket_s),
                    "allocations": dict(alloc),
                    "details": dict(details),
                    "reason": dict(reason_payload),
                    "regime": dict(reason_payload.get("regime") or {}),
                    "regime_confidence": float(reason_payload.get("regime_confidence", 0.0) or 0.0),
                    "portfolio_target_gross": float(reason_payload.get("portfolio_target_gross", portfolio_target_gross) or portfolio_target_gross),
                    "alpha_decay_runtime": dict(alpha_runtime),
                    "alpha_decay_persist": dict(reason_payload.get("alpha_decay_persist") or {}),
                }
    except Exception as e:
        _warn_nonfatal(
            "strategy_allocator_cached_state_load_failed",
            "STRATEGY_ALLOCATOR_CACHED_STATE_LOAD_FAILED",
            e,
            warn_key="strategy_allocator_cached_state_load_failed",
            ts_ms=int(ts_ms),
        )

    since_ms = int(ts_ms) - int(window_s * 1000)

    rows = _read_exec_cap_eff(con, since_ms=since_ms, now_ms=ts_ms)
    shadow_proxy = _read_shadow_strategy_proxy(con, since_ms=since_ms, now_ms=ts_ms)
    active_cooldowns = _load_active_strategy_cooldowns(con, now_ms=ts_ms)
    registry_meta = _load_strategy_registry_meta(con)

    live_enabled = {
        str(sname)
        for sname, meta in (registry_meta or {}).items()
        if int(meta.get("enabled", 0) or 0) == 1 and str(meta.get("stage", "") or "").lower() == "live"
    }

    if live_enabled:
        rows = [r for r in rows if str(r[1] or "").strip() in live_enabled]
        shadow_proxy = {k: v for k, v in (shadow_proxy or {}).items() if str(k) in live_enabled}

    if not rows and not shadow_proxy:
        return {
            "ok": False,
            "ts_ms": int(ts_ms),
            "window_s": int(window_s),
            "bucket_s": int(bucket_s),
            "allocations": {},
            "details": {},
            "reason": "no_execution_or_shadow_rows",
        }

    by_strat_bucket: Dict[str, Dict[int, float]] = {}
    by_strat_rpr: Dict[str, List[float]] = {}
    by_strat_ddc: Dict[str, List[float]] = {}
    by_strat_trade_rows: Dict[str, List[Tuple[int, float]]] = {}

    for r_ts_ms, name, pnl_net, rpr, dd_c in rows:
        sname = str(name or "").strip()
        if not sname:
            continue
        bts = _bucket_ts(int(r_ts_ms), bucket_s=bucket_s)
        by_strat_bucket.setdefault(sname, {})
        by_strat_bucket[sname][bts] = float(by_strat_bucket[sname].get(bts, 0.0)) + float(pnl_net)
        by_strat_rpr.setdefault(sname, []).append(float(rpr))
        by_strat_ddc.setdefault(sname, []).append(float(dd_c))
        by_strat_trade_rows.setdefault(sname, []).append((int(r_ts_ms), float(pnl_net)))

    strategies = sorted(set(by_strat_bucket.keys()) | set(shadow_proxy.keys()))
    all_buckets = sorted({b for m in by_strat_bucket.values() for b in m.keys()})

    series: Dict[str, List[float]] = {}
    pnl_series: Dict[str, List[Tuple[int, float]]] = {}

    for s in strategies:
        m = by_strat_bucket.get(s) or {}
        series[s] = [float(m.get(b, 0.0)) for b in all_buckets] if all_buckets else []
        pnl_series[s] = [(int(b), float(m.get(b, 0.0))) for b in all_buckets] if all_buckets else []

    corr_matrix, corr_obs = _build_corr_matrix(
        strategies,
        series,
        min_obs=int(max(1, CORR_CAP_MIN_OBS)),
    )
    corr_penalty: Dict[str, float] = {}
    avg_abs_corr: Dict[str, float] = {}

    for i, si in enumerate(strategies):
        abs_corrs = []
        for j, sj in enumerate(strategies):
            if i == j:
                continue
            n_obs = int(((corr_obs.get(si) or {}).get(sj)) or 0)
            if n_obs < int(max(1, CORR_CAP_MIN_OBS)):
                continue
            c = _safe_float((corr_matrix.get(si) or {}).get(sj), 0.0)
            abs_corrs.append(abs(float(c)))
        ac = sum(abs_corrs) / float(len(abs_corrs)) if abs_corrs else 0.0
        avg_abs_corr[si] = float(ac)
        pen = 1.0 / (1.0 + max(0.0, float(CORR_GAMMA)) * float(ac))
        corr_penalty[si] = float(max(0.0, min(1.0, pen)))

    budgets = _parse_risk_budgets()
    alloc_cfg = _load_allocator_config()
    try:
        cfg_rb = alloc_cfg.get("risk_budgets") or alloc_cfg.get("strategy_risk_budgets") or {}
        if isinstance(cfg_rb, dict):
            budgets.update(_parse_budget_map(cfg_rb))
    except Exception as e:
        _warn_nonfatal(
            "strategy_allocator_runtime_budget_override_failed",
            "STRATEGY_ALLOCATOR_RUNTIME_BUDGET_OVERRIDE_FAILED",
            e,
            warn_key="strategy_allocator_runtime_budget_override_failed",
        )

    for s in strategies:
        meta = dict((registry_meta or {}).get(str(s)) or {})
        rb = meta.get("risk_budget")
        if rb is None:
            rb = meta.get("allocator_risk_budget")
        if rb is None:
            try:
                alloc_obj = meta.get("allocator") or {}
                if isinstance(alloc_obj, dict):
                    rb = alloc_obj.get("risk_budget")
            except Exception:
                rb = None
        if rb is None:
            continue
        try:
            budgets[str(s)] = max(0.0, float(rb))
        except Exception as e:
            _warn_nonfatal(
                "strategy_allocator_meta_risk_budget_failed",
                "STRATEGY_ALLOCATOR_META_RISK_BUDGET_FAILED",
                e,
                strategy_name=str(s),
            )
            continue

    regime_vector: Dict[str, Any] = {}
    regime_conf = 1.0
    regime_stress = 0.0
    regime_budget_scale = 1.0

    try:
        from engine.strategy.regime_stack import compute_regime_vector

        regime_vector = compute_regime_vector(symbol="SPY", ts_ms=int(ts_ms), con=con) or {}
        regime_conf = _safe_float(((regime_vector.get("confidence") or {}).get("overall", 1.0)), 1.0)

        macro_vec = dict(regime_vector.get("macro") or {})
        micro_vec = dict(regime_vector.get("micro") or {})

        regime_stress = max(
            _safe_float(macro_vec.get("vol_expansion", 0.0), 0.0),
            _safe_float(macro_vec.get("credit_stress", 0.0), 0.0),
            _safe_float(macro_vec.get("drawdown_shift", 0.0), 0.0),
            _safe_float(micro_vec.get("vol_clustered", 0.0), 0.0),
            _safe_float(micro_vec.get("liquidity_thin", 0.0), 0.0),
        )
        regime_stress = max(0.0, min(1.0, float(regime_stress)))
        regime_budget_scale = max(0.35, 1.0 - (0.45 * float(regime_stress) * (0.75 + 0.25 * float(regime_conf))))
    except Exception:
        regime_vector = {}
        regime_conf = 1.0
        regime_stress = 0.0
        regime_budget_scale = 1.0

    details: Dict[str, Dict[str, Any]] = {}
    raw_scores: Dict[str, float] = {}
    weight_inputs: Dict[str, float] = {}

    for s in strategies:
        meta = dict((registry_meta or {}).get(str(s)) or {})
        registry_stage = str(meta.get("stage", "") or "").strip().lower()
        registry_enabled = int(meta.get("enabled", 0) or 0)

        rets = series.get(s) or []
        mu = sum(rets) / float(len(rets)) if rets else 0.0
        sd = _stddev(rets)
        sharpe = float(mu / sd) if sd > 1e-12 else 0.0

        dd = _max_drawdown_from_pnl_series(pnl_series.get(s) or [])

        dd_th = max(1e-6, float(DD_TH))
        if dd <= dd_th:
            dd_scale = 1.0
        else:
            dd_scale = max(float(DD_FLOOR), 1.0 - ((float(dd) - dd_th) / dd_th))

        streak_info = _loss_streak_info(by_strat_trade_rows.get(s) or [])
        loss_streak = int(streak_info.get("loss_streak", 0))
        loss_streak_pnl = float(streak_info.get("loss_streak_pnl", 0.0))
        last_loss_ts_ms = int(streak_info.get("last_loss_ts_ms", 0))

        loss_streak_scale = _cooldown_scale_from_loss_streak(loss_streak)
        drawdown_cooldown_scale = _cooldown_scale_from_drawdown(dd)

        cooldown_triggered = bool(
            float(loss_streak_scale) < 1.0 or float(drawdown_cooldown_scale) < 1.0
        )
        trigger_scale = float(min(loss_streak_scale, drawdown_cooldown_scale))

        active_cd = dict(active_cooldowns.get(s) or {})
        active_cd_until_ts_ms = int(active_cd.get("cooldown_until_ts_ms", 0))
        active_cd_scale = float(max(0.0, min(1.0, _safe_float(active_cd.get("cooldown_scale", 1.0), 1.0))))
        active_cd_reason = dict(active_cd.get("reason") or {})

        cooldown_reason: Dict[str, Any] = {}
        if float(drawdown_cooldown_scale) < 1.0:
            cooldown_reason["drawdown"] = {
                "max_drawdown_proxy": float(dd),
                "threshold": float(DD_COOLDOWN_TH),
                "cooldown_scale": float(drawdown_cooldown_scale),
            }
        if float(loss_streak_scale) < 1.0:
            cooldown_reason["losing_streak"] = {
                "loss_streak": int(loss_streak),
                "trigger": int(LOSS_STREAK_TRIGGER),
                "loss_streak_pnl": float(loss_streak_pnl),
                "last_loss_ts_ms": int(last_loss_ts_ms),
                "cooldown_scale": float(loss_streak_scale),
            }

        cooldown_active = False
        cooldown_scale = 1.0
        cooldown_until_ts_ms = 0

        if cooldown_triggered:
            cooldown_until_ts_ms = int(ts_ms) + (int(max(0, COOLDOWN_S)) * 1000)
            cooldown_scale = float(min(trigger_scale, active_cd_scale))
            cooldown_reason = _merge_reason_dicts(
                active_cd_reason,
                cooldown_reason,
                {
                    "cooldown_s": int(COOLDOWN_S),
                    "trigger_ts_ms": int(ts_ms),
                },
            )
            _upsert_strategy_cooldown(
                con,
                str(s),
                trigger_ts_ms=int(ts_ms),
                cooldown_until_ts_ms=int(cooldown_until_ts_ms),
                cooldown_scale=float(cooldown_scale),
                reason=cooldown_reason,
            )
            active_cooldowns[s] = {
                "trigger_ts_ms": int(ts_ms),
                "cooldown_until_ts_ms": int(cooldown_until_ts_ms),
                "cooldown_scale": float(cooldown_scale),
                "reason": dict(cooldown_reason),
            }
            cooldown_active = bool(float(cooldown_scale) < 1.0 and int(cooldown_until_ts_ms) > int(ts_ms))
        elif int(active_cd_until_ts_ms) > int(ts_ms) and float(active_cd_scale) < 1.0:
            cooldown_active = True
            cooldown_scale = float(active_cd_scale)
            cooldown_until_ts_ms = int(active_cd_until_ts_ms)
            cooldown_reason = dict(active_cd_reason)

        rpr_vals = by_strat_rpr.get(s) or []
        rpr_mean = sum(rpr_vals) / float(len(rpr_vals)) if rpr_vals else 0.0

        ddc_vals = by_strat_ddc.get(s) or []
        ddc_mean = sum(ddc_vals) / float(len(ddc_vals)) if ddc_vals else 0.0
        ddc_mag = max(0.0, min(1.0, abs(float(ddc_mean))))

        shadow = shadow_proxy.get(s) or {}
        shadow_runs = int(_safe_float(shadow.get("n_runs"), 0.0))
        shadow_score = _safe_float(shadow.get("proxy_score"), 0.0)
        shadow_avg_conf = _safe_float(shadow.get("avg_conf"), 0.0)
        shadow_avg_abs_z = _safe_float(shadow.get("avg_abs_z"), 0.0)
        shadow_gross = _safe_float(shadow.get("gross_target"), 0.0)
        shadow_conc = _safe_float(shadow.get("concentration"), 1.0)
        shadow_last_ts_ms = int(_safe_float(shadow.get("last_ts_ms"), 0.0))

        live_last_ts_ms = max((int(ts) for ts, _pnl in (by_strat_trade_rows.get(s) or [])), default=0)
        evidence_last_ts_ms = max(int(live_last_ts_ms), int(shadow_last_ts_ms))
        freshness_age_ms = max(0, int(ts_ms) - int(evidence_last_ts_ms)) if evidence_last_ts_ms > 0 else int(window_s * 1000)
        freshness_halflife_ms = max(60_000, int(FRESHNESS_HALFLIFE_S) * 1000)
        freshness_mult = 0.5 ** (float(freshness_age_ms) / float(freshness_halflife_ms))
        freshness_mult = max(float(FRESHNESS_FLOOR), min(1.0, float(freshness_mult)))

        live_sample_rows = int(len(by_strat_trade_rows.get(s) or []))
        sample_quality = math.sqrt(
            min(1.0, float(live_sample_rows) / float(max(1, int(MIN_SAMPLE_ROWS))))
        ) if live_sample_rows > 0 else 0.0
        shadow_sample_quality = math.sqrt(
            min(1.0, float(shadow_runs) / float(max(1, int(MIN_SHADOW_RUNS))))
        ) if shadow_runs > 0 else 0.0
        evidence_quality = max(sample_quality, 0.75 * shadow_sample_quality)
        evidence_quality = _clamp(evidence_quality, 0.15, 1.0)

        shadow_conf_quality = _clamp(
            float(CONFIDENCE_QUALITY_FLOOR) + ((1.0 - float(CONFIDENCE_QUALITY_FLOOR)) * max(0.0, float(shadow_avg_conf))),
            float(CONFIDENCE_QUALITY_FLOOR),
            1.0,
        ) if shadow_runs > 0 else 1.0
        shadow_z_quality = _clamp(
            0.65 + (0.35 * math.tanh(max(0.0, float(shadow_avg_abs_z)) / max(1e-6, float(ABS_Z_QUALITY_SCALE)))),
            0.65,
            1.0,
        ) if shadow_runs > 0 else 1.0
        concentration_penalty = _clamp(
            1.0 - (float(CONCENTRATION_PENALTY_GAMMA) * max(0.0, float(shadow_conc))),
            0.35,
            1.0,
        ) if shadow_runs > 0 else 1.0

        perf_consistency = 0.0
        if sd > 1e-12:
            perf_consistency = float(mu) / float(sd)
        perf_consistency = max(-2.0, min(2.0, float(perf_consistency)))

        base = float(sharpe) + float(rpr_mean) + (0.25 * float(perf_consistency))
        if shadow_runs > 0:
            base += float(shadow_score) * float(SHADOW_PROXY_ALPHA)

        alpha_decay = {
            "rolling_sharpe": float(sharpe),
            "half_life_buckets": None,
            "half_life_seconds": None,
            "structural_break_z": 0.0,
            "severity": "ok",
            "severity_score": 0.0,
            "throttle_mult": 1.0,
            "reasons": [],
            "n_obs": int(len(rets)),
        }
        if callable(compute_alpha_decay_snapshot):
            try:
                alpha_decay = compute_alpha_decay_snapshot(
                    strategy_name=str(s),
                    bucket_returns=list(rets),
                    bucket_s=int(bucket_s),
                    ts_ms=int(ts_ms),
                ) or alpha_decay
            except Exception as e:
                _warn_nonfatal(
                    "strategy_allocator_alpha_decay_snapshot_failed",
                    "STRATEGY_ALLOCATOR_ALPHA_DECAY_SNAPSHOT_FAILED",
                    e,
                    warn_key=f"strategy_allocator_alpha_decay_snapshot_failed:{s}",
                    strategy=str(s),
                    ts_ms=int(ts_ms),
                )
        alpha_decay_mult = float(max(0.0, min(1.0, _safe_float(alpha_decay.get("throttle_mult"), 1.0))))
        alpha_decay_severity = str(alpha_decay.get("severity") or "ok").strip().lower()

        if alpha_decay_severity in ("warn", "severe") and float(alpha_decay_mult) < 1.0:
            alpha_decay_reason = {
                "alpha_decay": {
                    "severity": str(alpha_decay.get("severity") or "ok"),
                    "severity_score": float(_safe_float(alpha_decay.get("severity_score"), 0.0)),
                    "throttle_mult": float(alpha_decay_mult),
                    "rolling_sharpe": float(_safe_float(alpha_decay.get("rolling_sharpe"), sharpe)),
                    "half_life_buckets": alpha_decay.get("half_life_buckets"),
                    "half_life_seconds": alpha_decay.get("half_life_seconds"),
                    "structural_break_z": float(_safe_float(alpha_decay.get("structural_break_z"), 0.0)),
                    "reasons": list(alpha_decay.get("reasons") or []),
                    "n_obs": int(alpha_decay.get("n_obs") or 0),
                }
            }
            merged_alpha_reason = _merge_reason_dicts(active_cd_reason, cooldown_reason, alpha_decay_reason)
            existing_until = int(active_cd_until_ts_ms or 0)
            alpha_cd_until_ts_ms = max(existing_until, int(ts_ms) + (int(max(0, COOLDOWN_S)) * 1000))
            alpha_cd_scale = float(min(active_cd_scale, alpha_decay_mult))

            _upsert_strategy_cooldown(
                con,
                str(s),
                trigger_ts_ms=int(ts_ms),
                cooldown_until_ts_ms=int(alpha_cd_until_ts_ms),
                cooldown_scale=float(alpha_cd_scale),
                reason=merged_alpha_reason,
            )

            active_cooldowns[s] = {
                "trigger_ts_ms": int(ts_ms),
                "cooldown_until_ts_ms": int(alpha_cd_until_ts_ms),
                "cooldown_scale": float(alpha_cd_scale),
                "reason": dict(merged_alpha_reason),
            }

            cooldown_active = True
            cooldown_scale = float(min(cooldown_scale, alpha_cd_scale))
            cooldown_until_ts_ms = int(alpha_cd_until_ts_ms)
            cooldown_reason = dict(merged_alpha_reason)

        regime_drawdown_penalty = max(0.25, 1.0 - (float(regime_stress) * float(ddc_mag)))
        regime_weight = float(regime_budget_scale) * float(regime_drawdown_penalty)

        pre_budget_score = (
            float(base)
            * float(dd_scale)
            * float(corr_penalty.get(s, 1.0))
            * float(cooldown_scale)
            * float(alpha_decay_mult)
            * float(regime_weight)
            * float(freshness_mult)
            * float(evidence_quality)
            * float(shadow_conf_quality)
            * float(shadow_z_quality)
            * float(concentration_penalty)
        )
        if float(pre_budget_score) < float(SCORE_FLOOR):
            pre_budget_score = float(SCORE_FLOOR)

        budget = float(budgets.get(s, 1.0))
        if budget < 0.0:
            budget = 0.0

        kill_by_registry = bool(registry_enabled == 0 or registry_stage == "shadow")
        kill_by_score = bool(float(pre_budget_score) < float(score_disable_th))
        kill_by_drawdown = bool(float(dd) >= float(dd_disable_th))
        is_active = not (kill_by_registry or kill_by_score or kill_by_drawdown or float(budget) <= 0.0)

        raw_scores[s] = float(max(0.0, pre_budget_score))
        weight_input = float(raw_scores[s]) * float(budget) if is_active else 0.0
        weight_inputs[s] = float(max(0.0, weight_input))

        details[s] = {
            "window_s": int(window_s),
            "bucket_s": int(bucket_s),
            "n_rows": int(sum(1 for _ in (by_strat_rpr.get(s) or []))),
            "mean_bucket_pnl": float(mu),
            "std_bucket_pnl": float(sd),
            "sharpe_bucket": float(sharpe),
            "rolling_performance_score": float(base),
            "performance_consistency": float(perf_consistency),
            "max_drawdown_proxy": float(dd),
            "dd_scale": float(dd_scale),
            "avg_abs_corr": float(avg_abs_corr.get(s, 0.0)),
            "corr_penalty": float(corr_penalty.get(s, 1.0)),
            "corr_row": dict(corr_matrix.get(s) or {}),
            "corr_obs_row": dict(corr_obs.get(s) or {}),
            "corr_min_obs": int(max(1, CORR_CAP_MIN_OBS)),
            "corr_positive_only": bool(CORR_CAP_POSITIVE_ONLY),
            "return_per_risk_unit": float(rpr_mean),
            "drawdown_contribution": float(ddc_mean),
            "loss_streak": int(loss_streak),
            "loss_streak_pnl": float(loss_streak_pnl),
            "last_loss_ts_ms": int(last_loss_ts_ms),
            "cooldown_active": bool(cooldown_active),
            "cooldown_scale": float(cooldown_scale),
            "cooldown_until_ts_ms": int(cooldown_until_ts_ms),
            "cooldown_reason": dict(cooldown_reason),
            "shadow_proxy_runs": int(shadow_runs),
            "shadow_proxy_score": float(shadow_score),
            "shadow_proxy_alpha": float(SHADOW_PROXY_ALPHA),
            "shadow_avg_conf": float(shadow_avg_conf),
            "shadow_avg_abs_z": float(shadow_avg_abs_z),
            "shadow_gross_target": float(shadow_gross),
            "shadow_concentration": float(shadow_conc),
            "shadow_concentration_penalty": float(concentration_penalty),
            "shadow_last_ts_ms": int(shadow_last_ts_ms),
            "shadow_confidence_quality": float(shadow_conf_quality),
            "shadow_abs_z_quality": float(shadow_z_quality),
            "live_sample_rows": int(live_sample_rows),
            "shadow_sample_runs": int(shadow_runs),
            "sample_quality": float(sample_quality),
            "shadow_sample_quality": float(shadow_sample_quality),
            "evidence_quality": float(evidence_quality),
            "last_evidence_ts_ms": int(evidence_last_ts_ms),
            "freshness_age_ms": int(freshness_age_ms),
            "freshness_halflife_s": int(FRESHNESS_HALFLIFE_S),
            "freshness_multiplier": float(freshness_mult),
            "risk_budget": float(budget),
            "regime_confidence": float(regime_conf),
            "regime_stress": float(regime_stress),
            "regime_budget_scale": float(regime_budget_scale),
            "regime_drawdown_penalty": float(regime_drawdown_penalty),
            "regime_labels": dict(regime_vector.get("regimes") or {}),
            "registry_stage": str(registry_stage),
            "registry_enabled": int(registry_enabled),
            "alpha_decay_rolling_sharpe": float(_safe_float(alpha_decay.get("rolling_sharpe"), sharpe)),
            "alpha_decay_half_life_buckets": alpha_decay.get("half_life_buckets"),
            "alpha_decay_half_life_seconds": alpha_decay.get("half_life_seconds"),
            "alpha_decay_structural_break_z": float(_safe_float(alpha_decay.get("structural_break_z"), 0.0)),
            "alpha_decay_structural_break": dict(alpha_decay.get("structural_break") or {}),
            "alpha_decay_severity": str(alpha_decay.get("severity") or "ok"),
            "alpha_decay_severity_score": float(_safe_float(alpha_decay.get("severity_score"), 0.0)),
            "alpha_decay_reasons": list(alpha_decay.get("reasons") or []),
            "alpha_decay_n_obs": int(alpha_decay.get("n_obs") or 0),
            "alpha_decay_throttle_mult": float(alpha_decay_mult),
            "raw_score": float(raw_scores[s]),
            "allocator_score": float(raw_scores[s]),
            "weight_input": float(weight_inputs[s]),
            "is_active": bool(is_active),
            "disabled_by_score": bool(kill_by_score),
            "disabled_by_drawdown": bool(kill_by_drawdown),
            "disabled_by_registry": bool(kill_by_registry),
            "score_disable_threshold": float(score_disable_th),
            "drawdown_disable_threshold": float(dd_disable_th),
            "portfolio_target_gross": float(portfolio_target_gross),
        }

    total = sum(float(v) for v in weight_inputs.values())
    alloc: Dict[str, float] = {}

    if total <= 1e-12:
        eligible = [s for s in strategies if bool((details.get(s) or {}).get("is_active"))]
        if not eligible:
            eligible = list(strategies)
        total = float(len(eligible) or 1)
        for s in strategies:
            alloc[s] = (1.0 / float(total)) if s in eligible else 0.0
            details.setdefault(s, {})
            details[s]["weight_input"] = float(alloc[s])
    else:
        alloc = {s: float(weight_inputs.get(s, 0.0)) / float(total) for s in strategies}

    alloc = _apply_corr_threshold_caps(
        alloc,
        corr_matrix,
        corr_obs,
        strategies,
        details,
    )

    total_budget = sum(
        float((details.get(s) or {}).get("risk_budget", 0.0))
        for s in strategies
        if bool((details.get(s) or {}).get("is_active"))
    )
    if total_budget > 1e-12:
        for s in strategies:
            det = details.get(s) or {}
            rb = float(det.get("risk_budget", 0.0) or 0.0)
            hard_cap = min(float(MAX_SHARE), max(0.0, rb / float(total_budget)))
            det["risk_budget_cap_share"] = float(hard_cap)
            pre_cap = float(alloc.get(s, 0.0))
            post_cap = min(pre_cap, float(hard_cap))
            alloc[s] = float(post_cap)
            if post_cap + 1e-12 < pre_cap:
                det["risk_budget_cap_applied"] = True
                det["risk_budget_cap_pre"] = float(pre_cap)
                det["risk_budget_cap_post"] = float(post_cap)
            details[s] = det

    alloc = _normalize_nonnegative_weights(alloc)

    active_strategies = [s for s in strategies if bool((details.get(s) or {}).get("is_active"))]
    active_count = int(len(active_strategies))
    if active_count > 0:
        active_floor = max(0.0, min(1.0 / float(active_count), float(EXPLORATION_MIN_ACTIVE_FLOOR)))
        softmax_temp = max(1e-6, float(EXPLORATION_SOFTMAX_TEMP))
        softmax_mix = max(0.0, min(1.0, float(EXPLORATION_SOFTMAX_MIX)))
        logits = [float(weight_inputs.get(s, alloc.get(s, 0.0))) for s in active_strategies]
        max_logit = max(logits) if logits else 0.0
        exp_scores = [float(math.exp((float(v) - float(max_logit)) / float(softmax_temp))) for v in logits]
        exp_total = sum(max(0.0, float(v)) for v in exp_scores)
        if exp_total > 1e-12:
            softmax_alloc = {
                str(s): float(max(0.0, float(v)) / float(exp_total))
                for s, v in zip(active_strategies, exp_scores)
            }
        else:
            softmax_alloc = {
                str(s): float(max(0.0, float(alloc.get(s, 0.0))))
                for s in active_strategies
            }
        active_pre_total = sum(float(max(0.0, float(alloc.get(s, 0.0)))) for s in active_strategies)
        if active_pre_total <= 1e-12:
            active_base_alloc = {str(s): (1.0 / float(active_count)) for s in active_strategies}
        else:
            active_base_alloc = {
                str(s): float(max(0.0, float(alloc.get(s, 0.0))) / float(active_pre_total))
                for s in active_strategies
            }
        floor_total = min(1.0, float(active_floor) * float(active_count))
        residual_scale = max(0.0, 1.0 - float(floor_total))
        for s in strategies:
            alloc[s] = 0.0
        for s in active_strategies:
            blended = ((1.0 - float(softmax_mix)) * float(active_base_alloc.get(s, 0.0))) + (
                float(softmax_mix) * float(softmax_alloc.get(s, 0.0))
            )
            alloc[s] = float(active_floor + (float(residual_scale) * float(blended)))
            det = dict(details.get(s) or {})
            det["exploration_active_floor"] = float(active_floor)
            det["exploration_softmax_temp"] = float(softmax_temp)
            det["exploration_softmax_mix"] = float(softmax_mix)
            det["exploration_softmax_weight"] = float(softmax_alloc.get(s, 0.0))
            det["exploration_base_weight"] = float(active_base_alloc.get(s, 0.0))
            details[s] = det

    for s in strategies:
        w = float(alloc.get(s, 0.0))
        w = max(float(MIN_SHARE), min(float(MAX_SHARE), float(w)))
        alloc[s] = float(w)

    alloc = _normalize_nonnegative_weights(alloc)

    try:
        for s in strategies:
            mj = dict(details.get(s) or {})
            mj["efficiency_score"] = float(mj.get("allocator_score", 0.0))
            mj["allocator_weight"] = float(alloc.get(s, 0.0))
            mj["allocator_kind"] = "strategy"
            mj["allocator_ts_ms"] = int(ts_ms)

            try:
                row = con.execute(
                    """
                    SELECT metrics_json
                    FROM strategy_metrics
                    WHERE strategy_name=? AND window_days=0
                    """,
                    (str(s),),
                ).fetchone()
                if row and row[0]:
                    base_mj = json.loads(row[0] or "{}")
                    if isinstance(base_mj, dict):
                        base_mj.update(mj)
                        mj = base_mj
            except Exception as e:
                _warn_nonfatal(
                    "strategy_allocator_existing_metrics_merge_failed",
                    "STRATEGY_ALLOCATOR_EXISTING_METRICS_MERGE_FAILED",
                    e,
                    warn_key=f"strategy_allocator_existing_metrics_merge_failed:{s}",
                    strategy=str(s),
                )

            con.execute(
                """
                INSERT INTO strategy_metrics(strategy_name, window_days, ts_ms, metrics_json, is_active)
                VALUES (?,?,?,?,?)
                ON CONFLICT(strategy_name, window_days) DO UPDATE SET
                  ts_ms=excluded.ts_ms,
                  metrics_json=excluded.metrics_json,
                  is_active=excluded.is_active
                """,
                (
                    str(s),
                    0,
                    int(ts_ms),
                    json.dumps(mj, separators=(",", ":"), sort_keys=True),
                    1 if bool((details.get(s) or {}).get("is_active")) else 0,
                ),
            )

            if _table_exists(con, "strategy_allocator_scores"):
                con.execute(
                    """
                    INSERT INTO strategy_allocator_scores(
                      strategy_name, ts_ms, window_days, score, raw_score,
                      allocation_weight, weight_input, risk_budget,
                      drawdown, corr_penalty, is_active, detail_json
                    )
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(strategy_name, ts_ms, window_days) DO UPDATE SET
                      score=excluded.score,
                      raw_score=excluded.raw_score,
                      allocation_weight=excluded.allocation_weight,
                      weight_input=excluded.weight_input,
                      risk_budget=excluded.risk_budget,
                      drawdown=excluded.drawdown,
                      corr_penalty=excluded.corr_penalty,
                      is_active=excluded.is_active,
                      detail_json=excluded.detail_json
                    """,
                    (
                        str(s),
                        int(ts_ms),
                        0,
                        float((details.get(s) or {}).get("allocator_score", 0.0) or 0.0),
                        float((details.get(s) or {}).get("raw_score", 0.0) or 0.0),
                        float(alloc.get(s, 0.0) or 0.0),
                        float((details.get(s) or {}).get("weight_input", 0.0) or 0.0),
                        float((details.get(s) or {}).get("risk_budget", 1.0) or 1.0),
                        float((details.get(s) or {}).get("max_drawdown_proxy", 0.0) or 0.0),
                        float((details.get(s) or {}).get("corr_penalty", 1.0) or 1.0),
                        1 if bool((details.get(s) or {}).get("is_active")) else 0,
                        json.dumps(details.get(s) or {}, separators=(",", ":"), sort_keys=True),
                    ),
                )
    except Exception as e:
        _warn_nonfatal(
            "strategy_allocator_metrics_persist_failed",
            "STRATEGY_ALLOCATOR_METRICS_PERSIST_FAILED",
            e,
            warn_key="strategy_allocator_metrics_persist_failed",
            ts_ms=int(ts_ms),
        )

    alpha_runtime = {}
    if callable(apply_alpha_decay_runtime_state):
        try:
            alpha_runtime = apply_alpha_decay_runtime_state(details=dict(details), ts_ms=int(ts_ms)) or {}
        except Exception:
            alpha_runtime = {}

    alpha_persist = {}
    if callable(persist_alpha_decay_state):
        try:
            alpha_persist = persist_alpha_decay_state(
                con,
                details=dict(details),
                runtime_summary=dict(alpha_runtime),
                ts_ms=int(ts_ms),
                window_days=0,
                bucket_s=int(bucket_s),
            ) or {}
        except Exception:
            alpha_persist = {}

    reason_payload = {
        "window_s": int(window_s),
        "bucket_s": int(bucket_s),
        "cadence_s": int(cadence_s),
        "strategies": strategies,
        "risk_budgets": budgets,
        "portfolio_target_gross": float(portfolio_target_gross),
        "corr_cap_threshold": float(CORR_CAP_THRESHOLD),
        "corr_cap_max_share": float(CORR_CAP_MAX_SHARE),
        "corr_cap_min_mult": float(CORR_CAP_MIN_MULT),
        "corr_cap_min_peers": int(CORR_CAP_MIN_PEERS),
        "corr_cap_min_obs": int(CORR_CAP_MIN_OBS),
        "corr_cap_positive_only": bool(CORR_CAP_POSITIVE_ONLY),
        "exploration_min_active_floor": float(EXPLORATION_MIN_ACTIVE_FLOOR),
        "exploration_softmax_temp": float(EXPLORATION_SOFTMAX_TEMP),
        "exploration_softmax_mix": float(EXPLORATION_SOFTMAX_MIX),
        "cooldown_s": int(COOLDOWN_S),
        "loss_streak_trigger": int(LOSS_STREAK_TRIGGER),
        "loss_streak_step": float(LOSS_STREAK_STEP),
        "loss_streak_floor": float(LOSS_STREAK_FLOOR),
        "dd_cooldown_threshold": float(DD_COOLDOWN_TH),
        "dd_cooldown_floor": float(DD_COOLDOWN_FLOOR),
        "score_disable_threshold": float(score_disable_th),
        "drawdown_disable_threshold": float(dd_disable_th),
        "regime": dict(regime_vector),
        "regime_confidence": float(regime_conf),
        "alpha_decay_runtime": dict(alpha_runtime),
        "alpha_decay_persist": dict(alpha_persist),
        "active_cooldowns": {
            str(name): {
                "trigger_ts_ms": int((active_cooldowns.get(name) or {}).get("trigger_ts_ms", 0)),
                "cooldown_until_ts_ms": int((active_cooldowns.get(name) or {}).get("cooldown_until_ts_ms", 0)),
                "cooldown_scale": float(_safe_float((active_cooldowns.get(name) or {}).get("cooldown_scale", 1.0), 1.0)),
                "reason": dict((active_cooldowns.get(name) or {}).get("reason") or {}),
            }
            for name in strategies
            if int((active_cooldowns.get(name) or {}).get("cooldown_until_ts_ms", 0)) > int(ts_ms)
        },
        "corr_matrix": corr_matrix,
        "corr_obs_matrix": corr_obs,
        "weight_input_total": float(sum(float(max(0.0, _safe_float(weight_inputs.get(s), 0.0))) for s in strategies)),
        "allocation_sum": float(sum(float(max(0.0, _safe_float(alloc.get(s), 0.0))) for s in strategies)),
        "cached": False,
    }

    try:
        try:
            from engine.cache.wrappers.strategy_allocations import set_strategy_allocations

            set_strategy_allocations(
                dict(alloc),
                reason=dict(reason_payload),
                ts_ms=int(ts_ms),
                window_days=0,
                con=con,
            )
        except Exception:
            con.execute(
                """
                INSERT INTO strategy_allocations(ts_ms, window_days, allocations_json, reason_json)
                VALUES (?,?,?,?)
                ON CONFLICT(ts_ms, window_days) DO UPDATE SET
                  allocations_json=excluded.allocations_json,
                  reason_json=excluded.reason_json
                """,
                (
                    int(ts_ms),
                    0,
                    json.dumps(alloc, separators=(",", ":"), sort_keys=True),
                    json.dumps(reason_payload, separators=(",", ":"), sort_keys=True),
                ),
            )

        if _table_exists(con, "strategy_allocator_history"):
            con.execute(
                """
                INSERT INTO strategy_allocator_history(
                  ts_ms, window_days, allocations_json, scores_json, details_json, reason_json
                )
                VALUES (?,?,?,?,?,?)
                ON CONFLICT(ts_ms, window_days) DO UPDATE SET
                  allocations_json=excluded.allocations_json,
                  scores_json=excluded.scores_json,
                  details_json=excluded.details_json,
                  reason_json=excluded.reason_json
                """,
                (
                    int(ts_ms),
                    0,
                    json.dumps(alloc, separators=(",", ":"), sort_keys=True),
                    json.dumps(raw_scores, separators=(",", ":"), sort_keys=True),
                    json.dumps(details, separators=(",", ":"), sort_keys=True),
                    json.dumps(reason_payload, separators=(",", ":"), sort_keys=True),
                ),
            )

        record_allocator_decision(
            ts_ms=int(ts_ms),
            allocations=dict(alloc),
            details=dict(details),
            reason=dict(reason_payload),
            con=con,
        )
    except Exception as e:
        _warn_nonfatal(
            "strategy_allocator_allocation_persist_failed",
            "STRATEGY_ALLOCATOR_ALLOCATION_PERSIST_FAILED",
            e,
            warn_key="strategy_allocator_allocation_persist_failed",
            ts_ms=int(ts_ms),
        )

    return {
        "ok": True,
        "ts_ms": int(ts_ms),
        "window_s": int(window_s),
        "bucket_s": int(bucket_s),
        "allocations": dict(alloc),
        "details": dict(details),
        "reason": dict(reason_payload),
        "regime": dict(regime_vector),
        "regime_confidence": float(regime_conf),
        "portfolio_target_gross": float(portfolio_target_gross),
        "alpha_decay_runtime": dict(alpha_runtime),
        "alpha_decay_persist": dict(alpha_persist),
    }
