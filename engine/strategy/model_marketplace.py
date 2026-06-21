"""Marketplace scoring and shadow-evidence utilities for model competition.

This module records challenger shadow orders, converts their outcomes into
comparable scores, validates candidates against replay data and self-critic
checks, and publishes the ranking and capital-allocation snapshots consumed by
governance and operator surfaces.
"""

import json
import logging
import math
import os
import time
from typing import Any, Dict, List, Optional, Tuple

from engine.strategy.capital_allocator import CapitalAllocator
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.event_replay import replay_state
from engine.runtime.runtime_meta import meta_get, meta_set
from engine.runtime.storage import connect, init_db, run_write_txn
from engine.strategy.model_competition import CompetitionRepository

LOGGER = logging.getLogger(__name__)
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: Exception, *, once_key: str | None = None, **extra: Any) -> None:
    key = str(once_key or "")
    if key:
        if key in _WARNED_NONFATAL_KEYS:
            return
        _WARNED_NONFATAL_KEYS.add(key)
    log_failure(
        LOGGER,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.strategy.model_marketplace",
        extra=extra or None,
        include_health=False,
        persist=False,
    )

REPLAY_LOOKBACK_EVENTS = int(os.environ.get("MODEL_REPLAY_LOOKBACK_EVENTS", "5000"))
REPLAY_RAW_MAX_CANDIDATES = int(os.environ.get("MODEL_REPLAY_RAW_MAX_CANDIDATES", "25"))
REPLAY_RAW_MAX_LABELS_PER_CANDIDATE = int(os.environ.get("MODEL_REPLAY_RAW_MAX_LABELS_PER_CANDIDATE", "200"))
REPLAY_RAW_MAX_SECONDS = float(os.environ.get("MODEL_REPLAY_RAW_MAX_SECONDS", "15.0"))
REPLAY_FRESH_MAX_AGE_MS = int(os.environ.get("MODEL_REPLAY_FRESH_MAX_AGE_MS", str(15 * 60 * 1000)))
REPLAY_MIN_N = int(os.environ.get("MODEL_REPLAY_MIN_N", "25"))
REPLAY_MIN_DIR_ACC = float(os.environ.get("MODEL_REPLAY_MIN_DIR_ACC", "0.50"))
REPLAY_MAX_NET_RMSE = float(os.environ.get("MODEL_REPLAY_MAX_NET_RMSE", "1.50"))
REPLAY_MAX_BASELINE_NET_RMSE_DEGRADATION = float(
    os.environ.get("MODEL_REPLAY_MAX_BASELINE_NET_RMSE_DEGRADATION", "0.0")
)
REPLAY_MIN_BASELINE_DIR_ACC_DELTA = float(
    os.environ.get("MODEL_REPLAY_MIN_BASELINE_DIR_ACC_DELTA", "0.0")
)
SELF_CRITIC_MIN_TRADES = int(os.environ.get("SELF_CRITIC_MIN_TRADES", "5"))
SELF_CRITIC_MAX_SLIPPAGE_BPS = float(
    os.environ.get("SELF_CRITIC_MAX_SLIPPAGE_BPS", "25.0")
)
SELF_CRITIC_MAX_LOSS = float(os.environ.get("SELF_CRITIC_MAX_LOSS", "-250.0"))
SELF_CRITIC_MAX_UNREALIZED_DRAWDOWN = float(
    os.environ.get("SELF_CRITIC_MAX_UNREALIZED_DRAWDOWN", "-150.0")
)
SELF_CRITIC_MIN_REPLAY_N = int(os.environ.get("SELF_CRITIC_MIN_REPLAY_N", "20"))
SELF_CRITIC_MAX_REPLAY_NET_RMSE_DELTA = float(
    os.environ.get("SELF_CRITIC_MAX_REPLAY_NET_RMSE_DELTA", "-0.05")
)
SELF_CRITIC_MAX_REPLAY_DIRACC_DELTA = float(
    os.environ.get("SELF_CRITIC_MAX_REPLAY_DIRACC_DELTA", "-0.02")
)
SELF_CRITIC_MAX_DRIFT_RATIO = float(
    os.environ.get("SELF_CRITIC_MAX_DRIFT_RATIO", "1.50")
)
SELF_CRITIC_MIN_CAPITAL_SCORE = float(
    os.environ.get("SELF_CRITIC_MIN_CAPITAL_SCORE", "-0.50")
)
SELF_CRITIC_RECENT_LOOKBACK_MS = int(
    os.environ.get("SELF_CRITIC_RECENT_LOOKBACK_MS", str(7 * 24 * 60 * 60 * 1000))
)
SELF_CRITIC_MIN_RECENT_WINDOW_N = int(
    os.environ.get("SELF_CRITIC_MIN_RECENT_WINDOW_N", "8")
)
SELF_CRITIC_MAX_RECENT_DIRACC_DROP = float(
    os.environ.get("SELF_CRITIC_MAX_RECENT_DIRACC_DROP", "-0.15")
)
SELF_CRITIC_MAX_RECENT_SIGNED_ALPHA_DROP = float(
    os.environ.get("SELF_CRITIC_MAX_RECENT_SIGNED_ALPHA_DROP", "-1.0")
)
SELF_CRITIC_MIN_SYMBOL_BREADTH = int(
    os.environ.get("SELF_CRITIC_MIN_SYMBOL_BREADTH", "3")
)
SELF_CRITIC_MAX_NEGATIVE_SYMBOL_FRACTION = float(
    os.environ.get("SELF_CRITIC_MAX_NEGATIVE_SYMBOL_FRACTION", "0.60")
)
CAPITAL_RECENCY_HALFLIFE_MS = int(
    os.environ.get("COMPETITION_CAPITAL_RECENCY_HALFLIFE_MS", str(7 * 24 * 60 * 60 * 1000))
)
CAPITAL_NET_PNL_SCALE = float(
    os.environ.get("COMPETITION_CAPITAL_NET_PNL_SCALE", "250.0")
)
CAPITAL_MAX_MODEL_GLOBAL_SHARE = float(
    os.environ.get("COMPETITION_CAPITAL_MAX_MODEL_GLOBAL_SHARE", "0.35")
)
CAPITAL_MAX_GROUP_CHAMPION_SHARE = float(
    os.environ.get("COMPETITION_CAPITAL_MAX_GROUP_CHAMPION_SHARE", "0.70")
)
CAPITAL_MIN_GROUP_BUDGET = float(
    os.environ.get("COMPETITION_CAPITAL_MIN_GROUP_BUDGET", "0.20")
)
CAPITAL_DRAWDOWN_SCALE = float(
    os.environ.get("COMPETITION_CAPITAL_DRAWDOWN_SCALE", str(max(1.0, CAPITAL_NET_PNL_SCALE)))
)
CAPITAL_RECENT_PNL_SCALE = float(
    os.environ.get("COMPETITION_CAPITAL_RECENT_PNL_SCALE", str(max(1.0, CAPITAL_NET_PNL_SCALE * 0.75)))
)
CAPITAL_SLIPPAGE_SCALE_BPS = float(
    os.environ.get("COMPETITION_CAPITAL_SLIPPAGE_SCALE_BPS", str(max(1.0, SELF_CRITIC_MAX_SLIPPAGE_BPS)))
)
CAPITAL_MODEL_RISK_MULT_MIN = float(
    os.environ.get("COMPETITION_CAPITAL_MODEL_RISK_MULT_MIN", "0.35")
)
CAPITAL_MODEL_RISK_MULT_MAX = float(
    os.environ.get("COMPETITION_CAPITAL_MODEL_RISK_MULT_MAX", "1.25")
)
COMPETITION_TOTAL_CAPITAL_FRACTION = float(
    os.environ.get("COMPETITION_TOTAL_CAPITAL_FRACTION", "1.0")
)
COMPETITION_ALLOCATION_STRATEGY = str(
    os.environ.get("COMPETITION_ALLOCATION_STRATEGY", "proportional")
).strip().lower() or "proportional"
COMPETITION_ALLOCATION_WINNER_POWER = float(
    os.environ.get("COMPETITION_ALLOCATION_WINNER_POWER", "1.35")
)
SHADOW_DEFAULT_COST_BPS = float(
    os.environ.get("MODEL_MARKETPLACE_SHADOW_COST_BPS", "6.0")
)
MODEL_COMPETITION_WINDOW_S = int(
    os.environ.get("MODEL_COMPETITION_WINDOW_S", os.environ.get("CHAMPION_COMPETITION_WINDOW_S", "86400"))
)
COMPETITION_SCORE_PNL_SCALE = float(
    os.environ.get("COMPETITION_SCORE_PNL_SCALE", str(max(1.0, CAPITAL_NET_PNL_SCALE)))
)
COMPETITION_SCORE_DRAWDOWN_SCALE = float(
    os.environ.get("COMPETITION_SCORE_DRAWDOWN_SCALE", str(max(1.0, CAPITAL_DRAWDOWN_SCALE)))
)
COMPETITION_SCORE_MIN_TRADES = int(
    os.environ.get("COMPETITION_SCORE_MIN_TRADES", str(max(3, SELF_CRITIC_MIN_TRADES)))
)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(v: Any, default: float = 0.0) -> float:
    if v is None:
        return float(default)
    if isinstance(v, str) and not v.strip():
        return float(default)
    try:
        return float(v)
    except Exception as e:
        _warn_nonfatal(
            "MODEL_MARKETPLACE_SAFE_FLOAT_FAILED",
            e,
            once_key=f"safe_float:{v}",
            raw_value=v,
        )
        return float(default)


def _safe_int(v: Any, default: int = 0) -> int:
    if v is None:
        return int(default)
    if isinstance(v, str) and not v.strip():
        return int(default)
    try:
        return int(v)
    except Exception as e:
        _warn_nonfatal(
            "MODEL_MARKETPLACE_SAFE_INT_FAILED",
            e,
            once_key=f"safe_int:{v}",
            raw_value=v,
        )
        return int(default)


def _safe_json_dict(v: Any) -> Dict[str, Any]:
    if isinstance(v, dict):
        return dict(v)
    if isinstance(v, str) and v.strip():
        try:
            obj = json.loads(v)
            return dict(obj) if isinstance(obj, dict) else {}
        except Exception as e:
            _warn_nonfatal(
                "MODEL_MARKETPLACE_SAFE_JSON_DICT_FAILED",
                e,
                once_key=f"safe_json_dict:{str(v)[:80]}",
                raw_preview=str(v)[:200],
            )
            return {}
    return {}


def _normalize_model_id(model_id: Any) -> str:
    mid = str(model_id or "").strip()
    return mid or "baseline"


def _allocation_strategy() -> str:
    strategy = str(COMPETITION_ALLOCATION_STRATEGY or "proportional").strip().lower()
    if strategy in {"equal_weight", "winner_take_most", "proportional"}:
        return strategy
    return "proportional"


def _normalize_fractions(models: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = [dict(row or {}) for row in (models or [])]
    total = sum(max(0.0, _safe_float(row.get("allocation_fraction"), 0.0)) for row in rows)
    if total <= 0.0:
        if not rows:
            return []
        eq = 1.0 / float(len(rows))
        for row in rows:
            row["allocation_fraction"] = float(eq)
        return rows
    for row in rows:
        row["allocation_fraction"] = float(
            max(0.0, _safe_float(row.get("allocation_fraction"), 0.0)) / float(total)
        )
    return rows


def _apply_group_champion_cap(models: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = _normalize_fractions(models)
    if len(rows) <= 1:
        return rows
    champion_share = max(0.0, _safe_float(rows[0].get("allocation_fraction"), 0.0))
    cap = max(0.0, min(1.0, float(CAPITAL_MAX_GROUP_CHAMPION_SHARE)))
    if champion_share <= cap:
        return rows

    excess = float(champion_share - cap)
    rows[0]["allocation_fraction"] = float(cap)
    tail_total = sum(max(0.0, _safe_float(row.get("allocation_fraction"), 0.0)) for row in rows[1:])
    if tail_total <= 0.0:
        return rows
    for row in rows[1:]:
        share = max(0.0, _safe_float(row.get("allocation_fraction"), 0.0))
        row["allocation_fraction"] = float(share + (excess * (share / tail_total)))
    return _normalize_fractions(rows)


def _clamp(value: Any, lo: float, hi: float) -> float:
    try:
        v = float(value)
    except Exception:
        v = float(lo)
    return float(max(float(lo), min(float(hi), float(v))))


def _bounded_tanh_01(value: Any, scale: float) -> float:
    sc = max(1e-9, float(scale))
    try:
        v = float(value)
    except Exception:
        v = 0.0
    return float(0.5 + (0.5 * math.tanh(float(v) / sc)))


def _bounded_tanh(value: Any, scale: float) -> float:
    return float((2.0 * _bounded_tanh_01(value, scale)) - 1.0)


def _recent_momentum(recent_total_pnl: float, prior_total_pnl: float) -> float:
    delta = float(recent_total_pnl) - float(prior_total_pnl)
    return _bounded_tanh_01(delta, max(1.0, float(CAPITAL_RECENT_PNL_SCALE)))


def _sample_stability(trades: int) -> float:
    min_trades = max(1, int(SELF_CRITIC_MIN_TRADES))
    return float(_clamp(math.sqrt(max(0.0, min(1.0, float(trades) / float(min_trades)))), 0.10, 1.0))


def _win_rate_stability(wins: int, trades: int) -> float:
    if int(trades) <= 0:
        return 0.50
    win_rate = float(max(0.0, min(1.0, float(wins) / float(max(1, trades)))))
    return float(_clamp(_bounded_tanh_01(win_rate - 0.50, 0.15), 0.15, 1.0))


def _drawdown_stability(max_drawdown: float) -> float:
    scale = max(1.0, float(CAPITAL_DRAWDOWN_SCALE))
    return float(_clamp(1.0 / (1.0 + (max(0.0, float(max_drawdown)) / scale)), 0.05, 1.0))


def _slippage_stability(avg_slippage_bps: float) -> float:
    scale = max(1.0, float(CAPITAL_SLIPPAGE_SCALE_BPS))
    return float(_clamp(1.0 / (1.0 + (max(0.0, float(avg_slippage_bps)) / scale)), 0.10, 1.0))


def _recent_regression_stability(recent_row: Dict[str, Any]) -> float:
    if not isinstance(recent_row, dict) or not recent_row:
        return 0.65
    dir_acc_drop = _safe_float(recent_row.get("dir_acc_drop"), 0.0)
    alpha_drop = _safe_float(recent_row.get("signed_alpha_drop"), 0.0)
    dir_component = _bounded_tanh_01(dir_acc_drop, 0.10)
    alpha_component = _bounded_tanh_01(alpha_drop, max(1.0, float(CAPITAL_RECENT_PNL_SCALE)))
    return float(_clamp((0.60 * dir_component) + (0.40 * alpha_component), 0.10, 1.0))


def _replay_reliability(replay_row: Dict[str, Any]) -> float:
    if not isinstance(replay_row, dict) or not replay_row:
        return 0.55
    approved = bool(replay_row.get("approved"))
    n_obs = max(0, _safe_int(replay_row.get("n"), 0))
    sample_component = _clamp(math.sqrt(min(1.0, float(n_obs) / float(max(1, REPLAY_MIN_N)))), 0.10, 1.0)
    dir_component = _bounded_tanh_01(_safe_float(replay_row.get("dir_acc_delta"), 0.0), 0.05)
    rmse_component = _bounded_tanh_01(_safe_float(replay_row.get("net_rmse_delta"), 0.0), 0.10)
    approved_bonus = 0.20 if approved else -0.10
    return float(
        _clamp(
            (0.40 * sample_component) + (0.25 * dir_component) + (0.20 * rmse_component) + 0.15 + approved_bonus,
            0.10,
            1.0,
        )
    )


def _compute_candidate_capital_metrics(
    row: Dict[str, Any],
    *,
    replay_row: Dict[str, Any],
    recent_row: Dict[str, Any],
    blocked: bool,
    now: int,
) -> Dict[str, Any]:
    meta = dict(row.get("meta") or {})
    trades = max(0, _safe_int(row.get("trades"), 0))
    wins = max(0, _safe_int(row.get("wins"), 0))
    score = _safe_float(row.get("score"), 0.0)
    net_pnl = _safe_float(row.get("net_pnl"), 0.0)
    capital_score = _safe_float(row.get("capital_score"), 0.0)
    max_drawdown = max(
        0.0,
        _safe_float(meta.get("max_drawdown"), 0.0),
    )
    avg_slippage_bps = max(0.0, _safe_float(meta.get("avg_slippage_bps"), 0.0))
    recent_total_pnl = _safe_float(meta.get("recent_total_pnl"), 0.0)
    prior_total_pnl = _safe_float(meta.get("prior_total_pnl"), 0.0)

    last_signal_ts_ms = _safe_int(meta.get("last_signal_ts_ms") or row.get("last_signal_ts_ms"), 0)
    age_ms = max(0, int(now - last_signal_ts_ms)) if last_signal_ts_ms > 0 else int(CAPITAL_RECENCY_HALFLIFE_MS)
    recency_mult = 0.5 ** (float(age_ms) / float(max(1, CAPITAL_RECENCY_HALFLIFE_MS)))

    performance_score = (
        (0.30 * _bounded_tanh_01(score, max(1.0, float(CAPITAL_NET_PNL_SCALE))))
        + (0.30 * _bounded_tanh_01(net_pnl, max(1.0, float(CAPITAL_NET_PNL_SCALE))))
        + (0.15 * _recent_momentum(recent_total_pnl, prior_total_pnl))
        + (0.15 * _bounded_tanh_01(capital_score, 0.75))
        + (0.10 * _replay_reliability(replay_row))
    )
    performance_score = float(_clamp(performance_score, 0.01, 1.0))

    stability_score = (
        (0.24 * _drawdown_stability(max_drawdown))
        + (0.18 * _sample_stability(trades))
        + (0.18 * _win_rate_stability(wins, trades))
        + (0.16 * _slippage_stability(avg_slippage_bps))
        + (0.14 * _recent_regression_stability(recent_row))
        + (0.10 * _replay_reliability(replay_row))
    )
    stability_score = float(_clamp(stability_score, 0.05, 1.0))

    governance_mult = 1.0
    if replay_row and not bool(replay_row.get("approved")):
        governance_mult *= 0.70
    if blocked:
        governance_mult *= 0.20
    if net_pnl < 0.0:
        governance_mult *= 0.85
    if avg_slippage_bps > float(SELF_CRITIC_MAX_SLIPPAGE_BPS):
        governance_mult *= 0.75
    elif avg_slippage_bps > float(SELF_CRITIC_MAX_SLIPPAGE_BPS) * 0.75:
        governance_mult *= 0.85

    effective_stability = float(_clamp(stability_score * governance_mult, 0.02, 1.0))
    raw_weight = float(max(0.001, performance_score * effective_stability * max(0.20, float(recency_mult))))
    model_risk_limit_multiplier = float(
        _clamp(
            float(CAPITAL_MODEL_RISK_MULT_MIN)
            + (
                (float(CAPITAL_MODEL_RISK_MULT_MAX) - float(CAPITAL_MODEL_RISK_MULT_MIN))
                * effective_stability
                * (0.50 + (0.50 * performance_score))
            ),
            float(CAPITAL_MODEL_RISK_MULT_MIN),
            float(CAPITAL_MODEL_RISK_MULT_MAX),
        )
    )
    budget_hint = float(
        _clamp(
            float(CAPITAL_MIN_GROUP_BUDGET)
            + (
                (1.0 - float(CAPITAL_MIN_GROUP_BUDGET))
                * ((0.55 * performance_score) + (0.45 * effective_stability))
            ),
            float(CAPITAL_MIN_GROUP_BUDGET),
            1.0,
        )
    )

    return {
        "raw_weight": float(raw_weight),
        "performance_score": float(performance_score),
        "stability_score": float(stability_score),
        "effective_stability_score": float(effective_stability),
        "recency_multiplier": float(recency_mult),
        "governance_multiplier": float(governance_mult),
        "drawdown_stability": float(_drawdown_stability(max_drawdown)),
        "slippage_stability": float(_slippage_stability(avg_slippage_bps)),
        "sample_stability": float(_sample_stability(trades)),
        "win_rate_stability": float(_win_rate_stability(wins, trades)),
        "recent_regression_stability": float(_recent_regression_stability(recent_row)),
        "replay_reliability": float(_replay_reliability(replay_row)),
        "group_budget_hint": float(budget_hint),
        "model_risk_limit_multiplier": float(model_risk_limit_multiplier),
    }


def _shape_ranked_model_allocations(
    weighted: List[Tuple[Dict[str, Any], float]],
    *,
    strategy: str,
    model_confidence: Optional[Dict[str, Any]] = None,
    risk_metrics: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    pairs = [(dict(row or {}), max(0.001, _safe_float(weight, 0.001))) for row, weight in (weighted or [])]
    if not pairs:
        return []

    allocator = CapitalAllocator(
        softmax_temp=max(
            1e-6,
            _safe_float(os.environ.get("COMPETITION_ALLOCATION_SOFTMAX_TEMP"), 0.35),
        ),
        softmax_mix=max(
            0.0,
            min(1.0, _safe_float(os.environ.get("COMPETITION_ALLOCATION_SOFTMAX_MIX"), 0.15)),
        ),
        min_model_floor=max(
            0.0,
            min(
                1.0 / float(len(pairs) or 1),
                _safe_float(os.environ.get("COMPETITION_ALLOCATION_MIN_MODEL_FLOOR"), 0.02),
            ),
        ),
        max_model_allocation=float(CAPITAL_MAX_GROUP_CHAMPION_SHARE),
    )
    alloc_res = allocator.allocate(
        [row for row, _raw_weight in pairs],
        model_confidence or {},
        {
            "strategy": str(strategy),
            "max_model_allocation": float(
                _safe_float(
                    (risk_metrics or {}).get("max_model_allocation"),
                    CAPITAL_MAX_GROUP_CHAMPION_SHARE,
                )
            ),
            "models": dict((risk_metrics or {}).get("models") or {}),
        },
    )
    allocation_map = {
        str(name): max(0.0, _safe_float(value, 0.0))
        for name, value in dict(alloc_res.get("allocations") or {}).items()
    }
    detail_map = {
        str(name): dict(value)
        for name, value in dict(alloc_res.get("details") or {}).items()
        if isinstance(value, dict)
    }
    ensemble_confidence = _safe_float(alloc_res.get("ensemble_confidence"), 0.0)
    ranked_models: List[Dict[str, Any]] = []
    for row, raw_weight in pairs:
        model_name = str(row.get("model_name") or "")
        detail = detail_map.get(model_name, {})
        ranked_models.append(
            {
                "model_name": model_name,
                "score": _safe_float(row.get("score"), 0.0),
                "capital_score": _safe_float(row.get("capital_score"), 0.0),
                "avg_confidence": _safe_float(row.get("avg_confidence"), 0.0),
                "ensemble_confidence": float(ensemble_confidence),
                "allocation_fraction": float(allocation_map.get(model_name, 0.0)),
                "raw_weight": float(raw_weight),
                "allocation_raw_weight": _safe_float(
                    detail.get("intelligent_score"),
                    row.get("raw_weight"),
                ),
                "allocation_floor_applied": float(allocator.min_model_floor),
                "allocation_softmax_temperature": float(allocator.softmax_temp),
                "allocation_softmax_mix": float(allocator.softmax_mix),
                "blended_confidence": _safe_float(detail.get("blended_confidence"), row.get("avg_confidence")),
                "signal_component": _safe_float(detail.get("signal_component"), 0.0),
                "underperformance_penalty": _safe_float(detail.get("underperformance_penalty"), 1.0),
                "drawdown_health": _safe_float(detail.get("drawdown_health"), 1.0),
                "risk_component": _safe_float(detail.get("risk_component"), 1.0),
                "allocation_cap": _safe_float(detail.get("allocation_cap"), 1.0),
                "prior_weight": _safe_float(detail.get("prior_weight"), 0.0),
                "intelligent_weight": _safe_float(detail.get("intelligent_weight"), 0.0),
                "blended_target_weight": _safe_float(detail.get("blended_target_weight"), 0.0),
                "trades": _safe_int(row.get("trades"), 0),
                "performance_score": _safe_float(row.get("performance_score"), 0.0),
                "stability_score": _safe_float(row.get("stability_score"), 0.0),
                "effective_stability_score": _safe_float(row.get("effective_stability_score"), 0.0),
                "recency_multiplier": _safe_float(row.get("recency_multiplier"), 0.0),
                "governance_multiplier": _safe_float(row.get("governance_multiplier"), 0.0),
                "drawdown_stability": _safe_float(row.get("drawdown_stability"), 0.0),
                "slippage_stability": _safe_float(row.get("slippage_stability"), 0.0),
                "sample_stability": _safe_float(row.get("sample_stability"), 0.0),
                "win_rate_stability": _safe_float(row.get("win_rate_stability"), 0.0),
                "recent_regression_stability": _safe_float(row.get("recent_regression_stability"), 0.0),
                "replay_reliability": _safe_float(row.get("replay_reliability"), 0.0),
                "model_risk_limit_multiplier": _safe_float(row.get("model_risk_limit_multiplier"), 1.0),
            }
        )
    ranked_models.sort(
        key=lambda item: (
            -_safe_float(item.get("allocation_fraction"), 0.0),
            -_safe_float(item.get("score"), 0.0),
            str(item.get("model_name") or ""),
        )
    )
    return _apply_group_champion_cap(ranked_models)


def _vwap(rows: List[Tuple[Any, Any]]) -> Optional[float]:
    qty_sum = 0.0
    notional = 0.0
    for px, qty in rows or []:
        q = abs(_safe_float(qty, 0.0))
        if q <= 0.0:
            continue
        qty_sum += q
        notional += q * _safe_float(px, 0.0)
    if qty_sum <= 0.0:
        return None
    return float(notional) / float(qty_sum)


def _load_latest_prices(con) -> Dict[str, float]:
    latest_px: Dict[str, float] = {}

    for sql in (
        """
        SELECT symbol, price
        FROM prices
        WHERE ts_ms IN (
          SELECT MAX(ts_ms)
          FROM prices
          GROUP BY symbol
        )
        """,
        """
        SELECT symbol, px
        FROM prices
        WHERE ts_ms IN (
          SELECT MAX(ts_ms)
          FROM prices
          GROUP BY symbol
        )
        """,
    ):
        try:
            rows = con.execute(sql).fetchall()
        except Exception:
            rows = []

        for sym, px in rows or []:
            symbol = str(sym or "").upper().strip()
            if not symbol:
                continue
            latest_px[symbol] = _safe_float(px, 0.0)

        if latest_px:
            break

    return latest_px


def _get_price_at_or_before(con, symbol: str, ts_ms: int) -> Optional[float]:
    for sql in (
        """
        SELECT price
        FROM prices
        WHERE symbol=? AND ts_ms<=?
        ORDER BY ts_ms DESC
        LIMIT 1
        """,
        """
        SELECT px
        FROM prices
        WHERE symbol=? AND ts_ms<=?
        ORDER BY ts_ms DESC
        LIMIT 1
        """,
    ):
        try:
            row = con.execute(
                sql,
                (str(symbol or "").upper().strip(), int(ts_ms)),
            ).fetchone()
        except Exception:
            row = None
        if row and row[0] is not None:
            return _safe_float(row[0], 0.0)
    return None


def _default_marketplace_row(
    *,
    model_id: str,
    model_name: str,
    symbol: str,
    horizon_s: int,
    regime: str,
    stage: str,
) -> Dict[str, Any]:
    return {
        "model_id": _normalize_model_id(model_id),
        "model_name": str(model_name or "").strip(),
        "symbol": str(symbol or "").upper().strip(),
        "horizon_s": int(horizon_s),
        "regime": str(regime or "global"),
        "stage": str(stage or "challenger"),
        "score": 0.0,
        "trades": 0,
        "wins": 0,
        "losses": 0,
        "gross_pnl": 0.0,
        "net_pnl": 0.0,
        "avg_confidence": 0.0,
        "last_signal_ts_ms": 0,
        "updated_ts_ms": _now_ms(),
        "order_ids": set(),
        "source_alert_ids": set(),
        "event_pnls": [],
        "meta": {
            "model_id": _normalize_model_id(model_id),
            "realized_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "transaction_cost": 0.0,
            "fee_cost": 0.0,
            "slippage_cost": 0.0,
            "entry_price": None,
            "avg_price": None,
            "exit_price": None,
            "last_price": None,
            "last_fill_price": None,
            "filled_qty": 0.0,
            "open_qty": 0.0,
            "position_size": 0.0,
            "slippage_weighted_sum": 0.0,
            "slippage_weight": 0.0,
        },
    }


def _load_latest_trade_attribution_rows(con) -> Dict[Tuple[int, str, str], Dict[str, Any]]:
    out: Dict[Tuple[int, str, str], Dict[str, Any]] = {}
    try:
        rows = con.execute(
            """
            SELECT
              source_alert_id,
              model_id,
              symbol,
              signal_json,
              model_json,
              regime_vector_json,
              decision_json
            FROM trade_attribution_ledger
            WHERE ts_ms = (SELECT MAX(ts_ms) FROM trade_attribution_ledger)
            """
        ).fetchall()
    except Exception:
        rows = []
    for source_alert_id, model_id, symbol, signal_json, model_json, regime_vector_json, decision_json in rows or []:
        if source_alert_id is None or symbol in (None, ""):
            continue
        out[
            (
                int(source_alert_id),
                _normalize_model_id(model_id),
                str(symbol).upper().strip(),
            )
        ] = {
            "signal_json": _safe_json_dict(signal_json),
            "model_json": _safe_json_dict(model_json),
            "regime_vector_json": _safe_json_dict(regime_vector_json),
            "decision_json": _safe_json_dict(decision_json),
        }
    return out


def _net_cost_evidence_for_signal(
    con,
    *,
    model_id: str,
    model_name: str,
    symbol: str,
    horizon_s: int,
    source_alert_id: Optional[int],
) -> Dict[str, Any]:
    try:
        from engine.runtime.storage import table_exists

        if not table_exists(con, "net_after_cost_labels"):
            return {"available": False, "n": 0}
    except Exception:
        return {"available": False, "n": 0}
    try:
        row = con.execute(
            """
            SELECT COUNT(1), AVG(net_return), AVG(gross_return), AVG(execution_cost_return),
                   AVG(total_cost_bps), MAX(computed_at_ts_ms)
            FROM net_after_cost_labels
            WHERE UPPER(TRIM(symbol))=UPPER(TRIM(?))
              AND horizon_s=?
              AND realized=1
              AND (
                (? IS NOT NULL AND source_alert_id=?)
                OR COALESCE(NULLIF(TRIM(model_id), ''), 'baseline') = COALESCE(NULLIF(TRIM(?), ''), 'baseline')
                OR model_name=?
              )
            """,
            (
                str(symbol or "").upper().strip(),
                int(horizon_s or 0),
                source_alert_id,
                source_alert_id,
                _normalize_model_id(model_id),
                str(model_name or "").strip(),
            ),
        ).fetchone()
    except Exception:
        return {"available": False, "n": 0}
    n = _safe_int((row or [0])[0], 0)
    return {
        "available": bool(n > 0),
        "n": int(n),
        "avg_net_return": (None if not row or row[1] is None else _safe_float(row[1], 0.0)),
        "avg_gross_return": (None if not row or row[2] is None else _safe_float(row[2], 0.0)),
        "avg_execution_cost_return": (None if not row or row[3] is None else _safe_float(row[3], 0.0)),
        "avg_total_cost_bps": (None if not row or row[4] is None else _safe_float(row[4], 0.0)),
        "latest_computed_at_ts_ms": _safe_int((row or [0, 0, 0, 0, 0, 0])[5], 0),
    }


def _shadow_qty(confidence: float) -> float:
    return max(1.0, round(_safe_float(confidence, 0.0) * 10.0, 4))


def _shadow_side_sign(predicted_z: float) -> int:
    return 1 if _safe_float(predicted_z, 0.0) >= 0.0 else -1


def _avg_confidence(prev_avg: float, prev_count: int, confidence: float) -> float:
    return (
        (float(prev_avg) * max(0, int(prev_count)))
        + float(_safe_float(confidence, 0.0))
    ) / max(1, int(prev_count) + 1)


def _meta_total_pnl(meta: Dict[str, Any]) -> float:
    if meta.get("total_pnl") is not None:
        return float(_safe_float(meta.get("total_pnl"), 0.0))
    return float(
        _safe_float(meta.get("realized_pnl"), 0.0)
        + _safe_float(meta.get("unrealized_pnl"), 0.0)
        - _safe_float(meta.get("transaction_cost"), 0.0)
    )


def _stddev(values: List[float]) -> float:
    cleaned = [float(_safe_float(v, 0.0)) for v in (values or [])]
    n = len(cleaned)
    if n <= 1:
        return 0.0
    mean = sum(cleaned) / float(n)
    variance = sum((float(v) - mean) ** 2 for v in cleaned) / float(max(1, n - 1))
    return float(math.sqrt(max(0.0, variance)))


def _downside_stddev(values: List[float]) -> float:
    cleaned = [min(0.0, float(_safe_float(v, 0.0))) for v in (values or [])]
    n = len(cleaned)
    if n <= 0:
        return 0.0
    variance = sum(float(v) ** 2 for v in cleaned) / float(max(1, n))
    return float(math.sqrt(max(0.0, variance)))


def _profit_factor(gross_profit: float, gross_loss_abs: float) -> Optional[float]:
    gp = max(0.0, float(gross_profit))
    gl = max(0.0, float(gross_loss_abs))
    if gl > 1e-9:
        return float(gp / gl)
    if gp > 1e-9:
        return None
    return 0.0


def _profit_factor_component(profit_factor: Optional[float], gross_profit: float, gross_loss_abs: float) -> float:
    if profit_factor is None:
        return 1.0 if float(gross_profit) > 0.0 else 0.0
    if float(gross_profit) <= 0.0 and float(gross_loss_abs) > 0.0:
        return -1.0
    return float(_bounded_tanh(float(profit_factor) - 1.0, 1.5))


def _risk_adjusted_score_components(
    *,
    total_pnl: float,
    max_drawdown: float,
    recent_total_pnl: float,
    prior_total_pnl: float,
    win_rate: float,
    trades: int,
    event_pnls: List[float],
) -> Dict[str, Any]:
    samples = [float(_safe_float(v, 0.0)) for v in (event_pnls or [])]
    if not samples and abs(float(total_pnl)) > 1e-12:
        samples = [float(total_pnl)]

    sample_count = int(len(samples))
    avg_trade_pnl = (sum(samples) / float(sample_count)) if sample_count > 0 else 0.0
    pnl_volatility = _stddev(samples)
    downside_volatility = _downside_stddev(samples)
    gross_profit = sum(max(0.0, float(v)) for v in samples)
    gross_loss_abs = abs(sum(min(0.0, float(v)) for v in samples))
    profit_factor = _profit_factor(gross_profit, gross_loss_abs)

    sharpe_like = 0.0
    if pnl_volatility > 1e-9:
        sharpe_like = (avg_trade_pnl / pnl_volatility) * math.sqrt(float(max(1, sample_count)))
    elif avg_trade_pnl > 0.0:
        sharpe_like = math.sqrt(float(max(1, sample_count)))
    elif avg_trade_pnl < 0.0:
        sharpe_like = -math.sqrt(float(max(1, sample_count)))

    sortino_like = 0.0
    if downside_volatility > 1e-9:
        sortino_like = (avg_trade_pnl / downside_volatility) * math.sqrt(float(max(1, sample_count)))
    elif avg_trade_pnl > 0.0:
        sortino_like = math.sqrt(float(max(1, sample_count)))
    elif avg_trade_pnl < 0.0:
        sortino_like = -math.sqrt(float(max(1, sample_count)))

    sample_component = _clamp(
        math.sqrt(float(max(0, sample_count)) / float(max(1, COMPETITION_SCORE_MIN_TRADES))),
        0.0,
        1.0,
    )
    momentum = float(recent_total_pnl) - float(prior_total_pnl)
    drawdown_penalty = _bounded_tanh(max_drawdown, max(1.0, float(COMPETITION_SCORE_DRAWDOWN_SCALE)))
    score = (
        (0.30 * _bounded_tanh(total_pnl, max(1.0, float(COMPETITION_SCORE_PNL_SCALE))))
        + (0.20 * _bounded_tanh(sortino_like, 2.0))
        + (0.15 * _bounded_tanh(sharpe_like, 2.0))
        + (0.10 * _profit_factor_component(profit_factor, gross_profit, gross_loss_abs))
        + (0.10 * _clamp((float(win_rate) - 0.50) / 0.50, -1.0, 1.0))
        + (0.10 * _bounded_tanh(momentum, max(1.0, float(COMPETITION_SCORE_PNL_SCALE))))
        + (0.05 * ((2.0 * float(sample_component)) - 1.0))
        - (0.25 * float(drawdown_penalty))
    )

    return {
        "risk_adjusted_score": float(score),
        "rolling_trade_count": int(sample_count),
        "rolling_avg_trade_pnl": float(avg_trade_pnl),
        "rolling_pnl_volatility": float(pnl_volatility),
        "rolling_downside_volatility": float(downside_volatility),
        "rolling_sharpe_like": float(sharpe_like),
        "rolling_sortino_like": float(sortino_like),
        "rolling_profit_factor": (
            None if profit_factor is None else float(profit_factor)
        ),
        "rolling_gross_profit": float(gross_profit),
        "rolling_gross_loss_abs": float(gross_loss_abs),
        "rolling_score_sample_component": float(sample_component),
        "rolling_score_momentum": float(momentum),
        "rolling_score_drawdown_penalty": float(drawdown_penalty),
        "win_rate": float(win_rate),
        "trade_count": int(max(int(trades), sample_count)),
    }


def _apply_marketplace_score(cur: Dict[str, Any]) -> Dict[str, Any]:
    meta = cur.setdefault("meta", {})
    event_pnls = [float(_safe_float(v, 0.0)) for v in list(cur.get("event_pnls") or [])]
    cumulative = 0.0
    peak = 0.0
    path_drawdown = 0.0
    for pnl_value in event_pnls:
        cumulative += float(pnl_value)
        peak = max(float(peak), float(cumulative))
        path_drawdown = max(float(path_drawdown), float(peak - cumulative))

    total_pnl = _safe_float(meta.get("total_pnl"), _safe_float(cur.get("net_pnl"), 0.0))
    max_drawdown = max(
        _safe_float(meta.get("max_drawdown"), 0.0),
        float(path_drawdown),
    )
    trades = max(_safe_int(cur.get("trades"), 0), len(event_pnls))
    wins = max(_safe_int(cur.get("wins"), 0), sum(1 for pnl_value in event_pnls if pnl_value > 0.0))
    win_rate = (float(wins) / float(trades)) if trades > 0 else 0.0

    score_metrics = _risk_adjusted_score_components(
        total_pnl=float(total_pnl),
        max_drawdown=float(max_drawdown),
        recent_total_pnl=_safe_float(meta.get("recent_total_pnl"), 0.0),
        prior_total_pnl=_safe_float(meta.get("prior_total_pnl"), 0.0),
        win_rate=float(win_rate),
        trades=int(trades),
        event_pnls=event_pnls,
    )
    meta.update(score_metrics)
    meta["max_drawdown"] = float(max_drawdown)
    cur["score"] = float(score_metrics.get("risk_adjusted_score") or 0.0)
    return cur


def _score_source_is_realized_pnl(meta: Optional[Dict[str, Any]]) -> bool:
    source = str((meta or {}).get("score_source") or "").strip().lower()
    return source in {"pnl_attribution", "execution_fills", "broker_fills"}


def _score_source_is_competition_candidate(meta: Optional[Dict[str, Any]]) -> bool:
    source = str((meta or {}).get("score_source") or "").strip().lower()
    return bool(_score_source_is_realized_pnl(meta) or source == "shadow_predictions")


def _update_row_tracking(
    cur: Dict[str, Any],
    *,
    ts_ms: int,
    pnl_delta: Optional[float] = None,
    window_mid_ms: Optional[int] = None,
) -> None:
    meta = cur.setdefault("meta", {})
    ts_i = _safe_int(ts_ms, 0)
    if ts_i > 0:
        first_ts = _safe_int(meta.get("first_signal_ts_ms"), 0)
        if first_ts <= 0 or ts_i < first_ts:
            meta["first_signal_ts_ms"] = int(ts_i)
        meta["last_signal_ts_ms"] = max(_safe_int(meta.get("last_signal_ts_ms"), 0), int(ts_i))
    total_now = _meta_total_pnl(meta)
    peak_total = max(_safe_float(meta.get("peak_total_pnl"), total_now), float(total_now))
    meta["peak_total_pnl"] = float(peak_total)
    meta["max_drawdown"] = max(
        _safe_float(meta.get("max_drawdown"), 0.0),
        float(max(0.0, peak_total - total_now)),
    )
    if pnl_delta is not None and window_mid_ms is not None and ts_i > 0:
        bucket = "recent_total_pnl" if int(ts_i) >= int(window_mid_ms) else "prior_total_pnl"
        meta[bucket] = _safe_float(meta.get(bucket), 0.0) + float(pnl_delta or 0.0)


def _dir_acc(preds: List[float], realized: List[float]) -> float:
    n = min(len(preds or []), len(realized or []))
    if n <= 0:
        return 0.0
    wins = 0
    for i in range(n):
        p = _safe_float(preds[i], 0.0)
        r = _safe_float(realized[i], 0.0)
        if (p >= 0.0 and r >= 0.0) or (p < 0.0 and r < 0.0):
            wins += 1
    return float(wins) / float(max(1, n))


def _rmse(preds: List[float], realized: List[float]) -> float:
    n = min(len(preds or []), len(realized or []))
    if n <= 0:
        return 9999.0
    se = 0.0
    for i in range(n):
        diff = _safe_float(preds[i], 0.0) - _safe_float(realized[i], 0.0)
        se += diff * diff
    return (_safe_float(se, 0.0) / float(max(1, n))) ** 0.5


def _raw_replay_artifact_identity(
    *,
    existing: Dict[str, Any],
    meta: Dict[str, Any],
    cached: Optional[Dict[str, Any]] = None,
) -> Tuple[str, int]:
    current_kind = str(existing.get("model_kind") or meta.get("model_kind") or "").strip()
    current_ts = _safe_int(existing.get("model_ts_ms") or meta.get("model_ts_ms"), 0)
    if current_kind or current_ts > 0:
        return current_kind, current_ts
    cached_obj = cached if isinstance(cached, dict) else {}
    return (
        str(cached_obj.get("model_kind") or "").strip(),
        _safe_int(cached_obj.get("model_ts_ms"), 0),
    )


def _raw_replay_cache_matches_artifact(
    cached: Dict[str, Any],
    *,
    current_kind: str,
    current_ts: int,
) -> bool:
    cached_kind = str(cached.get("model_kind") or "").strip()
    cached_ts = _safe_int(cached.get("model_ts_ms"), 0)
    if current_kind and cached_kind and current_kind != cached_kind:
        return False
    if current_ts > 0 and cached_ts > 0 and current_ts != cached_ts:
        return False
    return True


def _merge_raw_replay_validation(
    con,
    *,
    now: int,
    lookback_ms: int,
    models: Dict[str, Dict[str, Any]],
) -> None:
    try:
        from engine.strategy.predictor import predict_forced_model
    except Exception as e:
        _warn_nonfatal(
            "MODEL_MARKETPLACE_REPLAY_VALIDATION_IMPORT_FAILED",
            e,
            once_key="replay_validation_import",
        )
        return

    started_at = time.time()
    try:
        candidate_rows = con.execute(
            """
            SELECT model_name, symbol, horizon_s, regime, meta_json
            FROM model_marketplace_scores
            WHERE stage IN ('challenger', 'champion')
            ORDER BY updated_ts_ms DESC
            LIMIT ?
            """,
            (int(REPLAY_RAW_MAX_CANDIDATES),),
        ).fetchall()
    except Exception:
        candidate_rows = []

    for model_name, symbol, horizon_s, regime, meta_json in candidate_rows or []:
        if (time.time() - started_at) >= float(REPLAY_RAW_MAX_SECONDS):
            break
        name = str(model_name or "").strip()
        sym = str(symbol or "").upper().strip()
        hi = _safe_int(horizon_s, 0)
        reg = str(regime or "global")
        if not name or not sym or hi <= 0:
            continue
        meta = _safe_json_dict(meta_json)
        key = _competition_key(_normalize_model_id(meta.get("model_id")), name, sym, hi, reg)
        cache_key = f"raw_replay_cache:{name}:{sym}:{hi}:{reg}"
        existing = dict(models.get(key) or {})

        try:
            latest_row = con.execute(
                """
                SELECT MAX(e.id), MAX(e.ts_ms)
                FROM events e
                JOIN labels l
                  ON l.event_id = e.id
                 AND l.symbol = ?
                 AND l.horizon_s = ?
                LEFT JOIN labels_exec le
                  ON le.event_id = e.id
                 AND le.symbol = l.symbol
                 AND le.horizon_s = l.horizon_s
                WHERE e.ts_ms >= ?
                  AND COALESCE(le.net_z, l.impact_z) IS NOT NULL
                """,
                (
                    sym,
                    int(hi),
                    int(now - int(lookback_ms)),
                ),
            ).fetchone()
        except Exception:
            latest_row = None
        latest_event_id = _safe_int((latest_row or [0, 0])[0], 0)
        latest_window_end_ms = _safe_int((latest_row or [0, 0])[1], 0)
        if latest_event_id <= 0:
            continue

        cached = _safe_json_dict(meta_get(cache_key, "") or "")
        current_model_kind, current_model_ts_ms = _raw_replay_artifact_identity(
            existing=existing,
            meta=meta,
            cached=cached,
        )
        if cached and not _raw_replay_cache_matches_artifact(
            cached,
            current_kind=current_model_kind,
            current_ts=current_model_ts_ms,
        ):
            cached = {}
        cached_last_event_id = _safe_int(cached.get("last_event_id"), 0)
        cached_window_end_ms = _safe_int(cached.get("window_end_ms"), 0)
        if cached_last_event_id > 0 and cached_last_event_id >= latest_event_id and cached.get("n") is not None:
            baseline_net_rmse = (
                _safe_float(cached.get("baseline_net_rmse"), 0.0)
                if cached.get("baseline_n") is not None
                else None
            )
            baseline_dir_acc = (
                _safe_float(cached.get("baseline_dir_acc"), 0.0)
                if cached.get("baseline_n") is not None
                else None
            )
            net_rmse = _safe_float(cached.get("net_rmse"), 9999.0)
            dir_acc = _safe_float(cached.get("dir_acc"), 0.0)
            net_rmse_delta = (
                float(baseline_net_rmse) - float(net_rmse)
                if baseline_net_rmse is not None
                else None
            )
            dir_acc_delta = (
                float(dir_acc) - float(baseline_dir_acc)
                if baseline_dir_acc is not None
                else None
            )
            approved = bool(
                _safe_int(cached.get("n"), 0) >= int(REPLAY_MIN_N)
                and dir_acc >= float(REPLAY_MIN_DIR_ACC)
                and net_rmse <= float(REPLAY_MAX_NET_RMSE)
                and (
                    baseline_net_rmse is None
                    or net_rmse <= float(baseline_net_rmse) + float(REPLAY_MAX_BASELINE_NET_RMSE_DEGRADATION)
                )
                and (
                    dir_acc_delta is None
                    or dir_acc_delta >= float(REPLAY_MIN_BASELINE_DIR_ACC_DELTA)
                )
            )
            models[key] = {
                **existing,
                "model_name": name,
                "symbol": sym,
                "regime": reg,
                "horizon_s": hi,
                "model_kind": str(current_model_kind or cached.get("model_kind") or ""),
                "model_ts_ms": _safe_int(current_model_ts_ms or cached.get("model_ts_ms"), 0),
                "n": _safe_int(cached.get("n"), 0),
                "baseline_n": _safe_int(cached.get("baseline_n"), 0),
                "dir_acc": float(dir_acc),
                "baseline_dir_acc": baseline_dir_acc,
                "dir_acc_delta": (float(dir_acc_delta) if dir_acc_delta is not None else None),
                "gross_rmse": float(net_rmse),
                "net_rmse": float(net_rmse),
                "baseline_net_rmse": baseline_net_rmse,
                "net_rmse_delta": (float(net_rmse_delta) if net_rmse_delta is not None else None),
                "signed_alpha": _safe_float(cached.get("signed_alpha"), 0.0),
                "window_end_ms": int(max(cached_window_end_ms, latest_window_end_ms)),
                "last_event_id": int(max(cached_last_event_id, latest_event_id)),
                "approved": bool(approved),
                "source": "raw_event_replay_cache",
            }
            continue

        try:
            labeled_rows = con.execute(
                """
                SELECT
                  e.id,
                  e.ts_ms,
                  e.source,
                  e.title,
                  e.body,
                  e.url,
                  emb.vec,
                  p.predicted_z AS baseline_pred,
                  COALESCE(le.net_z, l.impact_z) AS realized_z
                FROM events e
                JOIN event_embeddings emb
                  ON emb.event_id = e.id
                JOIN labels l
                  ON l.event_id = e.id
                 AND l.symbol = ?
                 AND l.horizon_s = ?
                LEFT JOIN labels_exec le
                  ON le.event_id = e.id
                 AND le.symbol = l.symbol
                 AND le.horizon_s = l.horizon_s
                LEFT JOIN predictions p
                  ON p.event_id = e.id
                 AND p.symbol = l.symbol
                 AND p.horizon_s = l.horizon_s
                WHERE e.ts_ms >= ?
                  AND e.id > ?
                  AND COALESCE(le.net_z, l.impact_z) IS NOT NULL
                ORDER BY e.ts_ms DESC, e.id DESC
                LIMIT ?
                """,
                (
                    sym,
                    int(hi),
                    int(now - int(lookback_ms)),
                    int(max(0, cached_last_event_id)),
                    int(REPLAY_RAW_MAX_LABELS_PER_CANDIDATE),
                ),
            ).fetchall()
        except Exception:
            labeled_rows = []

        stats = {
            "n": _safe_int(cached.get("n"), 0),
            "correct": _safe_int(cached.get("correct"), 0),
            "sse": _safe_float(cached.get("sse"), 0.0),
            "baseline_n": _safe_int(cached.get("baseline_n"), 0),
            "baseline_correct": _safe_int(cached.get("baseline_correct"), 0),
            "baseline_sse": _safe_float(cached.get("baseline_sse"), 0.0),
            "signed_alpha": _safe_float(cached.get("signed_alpha"), 0.0),
            "last_event_id": int(max(cached_last_event_id, 0)),
            "window_end_ms": int(max(cached_window_end_ms, 0)),
        }

        for row in labeled_rows or []:
            if (time.time() - started_at) >= float(REPLAY_RAW_MAX_SECONDS):
                break
            event_id = _safe_int(row[0], 0)
            ts_ms = _safe_int(row[1], 0)
            vec_blob = row[6]
            if vec_blob is None:
                continue
            try:
                import numpy as np

                query_vec = np.frombuffer(vec_blob, dtype=np.float32)
            except Exception as e:
                _warn_nonfatal(
                    "MODEL_MARKETPLACE_REPLAY_VECTOR_PARSE_FAILED",
                    e,
                    once_key=f"replay_vector_parse:{event_id}",
                    event_id=int(event_id),
                )
                continue

            event_obj = {
                "id": int(event_id),
                "ts_ms": int(ts_ms),
                "source": str(row[2] or ""),
                "title": str(row[3] or ""),
                "body": str(row[4] or ""),
                "url": str(row[5] or ""),
            }
            try:
                pred_z, _conf, _explain = predict_forced_model(
                    query_vec,
                    symbol=sym,
                    horizon_s=int(hi),
                    model_name=name,
                    top_k=8,
                    event=event_obj,
                )
            except Exception as e:
                _warn_nonfatal(
                    "MODEL_MARKETPLACE_REPLAY_PREDICT_FAILED",
                    e,
                    once_key=f"replay_predict:{name}:{event_id}:{hi}",
                    model_name=str(name),
                    event_id=int(event_id),
                    horizon_s=int(hi),
                )
                continue

            realized_z = _safe_float(row[8], 0.0)
            pred_z = _safe_float(pred_z, 0.0)
            stats["n"] += 1
            stats["correct"] += 1 if ((pred_z >= 0.0 and realized_z >= 0.0) or (pred_z < 0.0 and realized_z < 0.0)) else 0
            diff = pred_z - realized_z
            stats["sse"] += diff * diff
            stats["signed_alpha"] += (1.0 if pred_z >= 0.0 else -1.0) * realized_z
            if row[7] is not None:
                baseline_pred = _safe_float(row[7], 0.0)
                stats["baseline_n"] += 1
                stats["baseline_correct"] += (
                    1 if ((baseline_pred >= 0.0 and realized_z >= 0.0) or (baseline_pred < 0.0 and realized_z < 0.0)) else 0
                )
                baseline_diff = baseline_pred - realized_z
                stats["baseline_sse"] += baseline_diff * baseline_diff
            stats["last_event_id"] = max(_safe_int(stats.get("last_event_id"), 0), event_id)
            stats["window_end_ms"] = max(_safe_int(stats.get("window_end_ms"), 0), ts_ms)

        n = _safe_int(stats.get("n"), 0)
        if n <= 0:
            continue
        baseline_n = _safe_int(stats.get("baseline_n"), 0)
        net_rmse = (_safe_float(stats.get("sse"), 0.0) / float(max(1, n))) ** 0.5
        dir_acc = float(_safe_int(stats.get("correct"), 0)) / float(max(1, n))
        baseline_net_rmse = (
            (_safe_float(stats.get("baseline_sse"), 0.0) / float(max(1, baseline_n))) ** 0.5
            if baseline_n > 0
            else None
        )
        baseline_dir_acc = (
            float(_safe_int(stats.get("baseline_correct"), 0)) / float(max(1, baseline_n))
            if baseline_n > 0
            else None
        )
        net_rmse_delta = (
            float(baseline_net_rmse) - float(net_rmse)
            if baseline_net_rmse is not None
            else None
        )
        dir_acc_delta = (
            float(dir_acc) - float(baseline_dir_acc)
            if baseline_dir_acc is not None
            else None
        )
        approved = bool(
            n >= int(REPLAY_MIN_N)
            and dir_acc >= float(REPLAY_MIN_DIR_ACC)
            and net_rmse <= float(REPLAY_MAX_NET_RMSE)
            and (
                baseline_net_rmse is None
                or net_rmse <= float(baseline_net_rmse) + float(REPLAY_MAX_BASELINE_NET_RMSE_DEGRADATION)
            )
            and (
                dir_acc_delta is None
                or dir_acc_delta >= float(REPLAY_MIN_BASELINE_DIR_ACC_DELTA)
            )
        )

        cache_payload = {
            "model_name": name,
            "symbol": sym,
            "regime": reg,
            "horizon_s": hi,
            "model_kind": str(current_model_kind or ""),
            "model_ts_ms": _safe_int(current_model_ts_ms, 0),
            "n": int(n),
            "correct": _safe_int(stats.get("correct"), 0),
            "sse": _safe_float(stats.get("sse"), 0.0),
            "baseline_n": int(baseline_n),
            "baseline_correct": _safe_int(stats.get("baseline_correct"), 0),
            "baseline_sse": _safe_float(stats.get("baseline_sse"), 0.0),
            "dir_acc": float(dir_acc),
            "baseline_dir_acc": (float(baseline_dir_acc) if baseline_dir_acc is not None else None),
            "net_rmse": float(net_rmse),
            "baseline_net_rmse": (float(baseline_net_rmse) if baseline_net_rmse is not None else None),
            "signed_alpha": _safe_float(stats.get("signed_alpha"), 0.0),
            "window_end_ms": _safe_int(stats.get("window_end_ms"), 0),
            "last_event_id": _safe_int(stats.get("last_event_id"), 0),
        }
        try:
            meta_set(cache_key, json.dumps(cache_payload, separators=(",", ":"), sort_keys=True))
        except Exception as e:
            _warn_nonfatal(
                "MODEL_MARKETPLACE_REPLAY_CACHE_PERSIST_FAILED",
                e,
                once_key="replay_cache_persist",
                cache_key=cache_key,
            )

        models[key] = {
            **existing,
            "model_name": name,
            "symbol": sym,
            "regime": reg,
            "horizon_s": hi,
            "model_kind": str(current_model_kind or ""),
            "model_ts_ms": _safe_int(current_model_ts_ms, 0),
            "n": int(n),
            "baseline_n": int(baseline_n),
            "dir_acc": float(dir_acc),
            "baseline_dir_acc": (float(baseline_dir_acc) if baseline_dir_acc is not None else None),
            "dir_acc_delta": (float(dir_acc_delta) if dir_acc_delta is not None else None),
            "gross_rmse": float(net_rmse),
            "net_rmse": float(net_rmse),
            "baseline_net_rmse": (float(baseline_net_rmse) if baseline_net_rmse is not None else None),
            "net_rmse_delta": (float(net_rmse_delta) if net_rmse_delta is not None else None),
            "signed_alpha": _safe_float(stats.get("signed_alpha"), 0.0),
            "window_end_ms": _safe_int(stats.get("window_end_ms"), 0),
            "last_event_id": _safe_int(stats.get("last_event_id"), 0),
            "approved": bool(approved),
            "source": "raw_event_replay",
        }


def _accumulate_shadow_prediction_scores(
    con,
    agg: Dict[Tuple[str, str, str, int, str], Dict[str, Any]],
    champion_keys: set[Tuple[str, str, str, int, str]],
    *,
    since_ms: int,
    window_mid_ms: int,
) -> int:
    try:
        rows = con.execute(
            """
            SELECT
              sp.ts_ms,
              sp.event_id,
              sp.symbol,
              sp.regime,
              sp.horizon_s,
              sp.model_name,
              sp.predicted_z,
              sp.confidence,
              sp.cost_est,
              sp.extra_json,
              le.mid_in,
              le.mid_out,
              le.total_cost_bps
            FROM shadow_predictions sp
            LEFT JOIN labels_exec le
              ON le.event_id = sp.event_id
             AND le.symbol = sp.symbol
             AND le.horizon_s = sp.horizon_s
            WHERE sp.ts_ms >= ?
            ORDER BY sp.ts_ms ASC, sp.id ASC
            """
            ,
            (int(since_ms),),
        ).fetchall()
    except Exception:
        rows = []

    written = 0
    for (
        pred_ts_ms,
        event_id,
        symbol,
        regime,
        horizon_s,
        model_name,
        predicted_z,
        confidence,
        cost_est,
        extra_json,
        mid_in,
        mid_out,
        total_cost_bps,
    ) in rows or []:
        sym = str(symbol or "").upper().strip()
        name = str(model_name or "").strip()
        reg = str(regime or "global").strip() or "global"
        hs = _safe_int(horizon_s, 0)
        if not sym or not name or hs <= 0:
            continue

        entry_px = _safe_float(mid_in, 0.0)
        if entry_px <= 0.0:
            entry_px = _safe_float(
                _get_price_at_or_before(con, sym, _safe_int(pred_ts_ms, 0)),
                0.0,
            )
        if entry_px <= 0.0:
            continue

        exit_px = _safe_float(mid_out, 0.0)
        if exit_px <= 0.0:
            exit_px = _safe_float(
                _get_price_at_or_before(
                    con,
                    sym,
                    _safe_int(pred_ts_ms, 0) + (hs * 1000),
                ),
                0.0,
            )

        extra = _safe_json_dict(extra_json)
        meta_obj = _safe_json_dict(extra.get("meta"))
        model_id = _normalize_model_id(meta_obj.get("model_id") or extra.get("model_id"))
        key = (model_id, name, sym, int(hs), reg)
        cur = agg.get(key)
        if cur is None:
            cur = _default_marketplace_row(
                model_id=model_id,
                model_name=name,
                symbol=sym,
                horizon_s=hs,
                regime=reg,
                stage=("champion" if key in champion_keys else "challenger"),
            )
            cur["meta"]["score_source"] = "shadow_predictions"
            agg[key] = cur

        qty = _shadow_qty(_safe_float(confidence, 0.0))
        side_sign = _shadow_side_sign(_safe_float(predicted_z, 0.0))
        order_id = f"shadow_pred:{_safe_int(event_id, 0)}:{name}:{sym}:{hs}"
        source_alert_id = None
        if meta_obj.get("source_alert_id") is not None:
            source_alert_id = _safe_int(meta_obj.get("source_alert_id"), 0)
            if source_alert_id <= 0:
                source_alert_id = None

        cur["order_ids"].add(order_id)
        if source_alert_id is not None:
            cur["source_alert_ids"].add(int(source_alert_id))
        cur["last_signal_ts_ms"] = max(
            _safe_int(cur.get("last_signal_ts_ms"), 0),
            _safe_int(pred_ts_ms, 0),
        )
        prev_trades = _safe_int(cur.get("trades"), 0)
        cur["avg_confidence"] = _avg_confidence(
            _safe_float(cur.get("avg_confidence"), 0.0),
            prev_trades,
            _safe_float(confidence, 0.0),
        )
        cur["trades"] = prev_trades + 1

        cost_bps = _safe_float(total_cost_bps, 0.0)
        if cost_bps <= 0.0:
            cost_bps = max(0.0, _safe_float(cost_est, 0.0))
        if cost_bps <= 0.0:
            cost_bps = float(SHADOW_DEFAULT_COST_BPS)

        gross_ret = 0.0
        if exit_px > 0.0 and entry_px > 0.0:
            gross_ret = ((float(exit_px) / float(entry_px)) - 1.0) * float(side_sign)
        gross_pnl = float(qty) * float(entry_px) * float(gross_ret)
        transaction_cost = float(qty) * float(entry_px) * (float(cost_bps) / 10000.0)

        cur["meta"]["realized_pnl"] = _safe_float(cur["meta"].get("realized_pnl"), 0.0) + float(gross_pnl)
        cur["meta"]["transaction_cost"] = _safe_float(cur["meta"].get("transaction_cost"), 0.0) + float(transaction_cost)
        cur["meta"]["filled_qty"] = _safe_float(cur["meta"].get("filled_qty"), 0.0) + float(abs(qty))
        cur["meta"]["entry_price"] = float(entry_px)
        cur["meta"]["exit_price"] = float(exit_px) if exit_px > 0.0 else None
        cur["meta"]["last_fill_price"] = float(exit_px) if exit_px > 0.0 else None
        cur["meta"]["open_qty"] = 0.0 if exit_px > 0.0 else float(side_sign * qty)
        cur["meta"]["score_source"] = "shadow_predictions"
        cur["meta"]["shadow_cost_bps"] = float(cost_bps)
        cur["meta"]["shadow_predictions_scored"] = _safe_int(
            cur["meta"].get("shadow_predictions_scored"),
            0,
        ) + 1
        cur["meta"]["last_shadow_event_id"] = _safe_int(event_id, 0)
        cur["meta"]["last_price"] = float(exit_px) if exit_px > 0.0 else cur["meta"].get("last_price")
        _update_row_tracking(
            cur,
            ts_ms=_safe_int(pred_ts_ms, 0),
            pnl_delta=float(gross_pnl - transaction_cost),
            window_mid_ms=int(window_mid_ms),
        )

        if gross_pnl > transaction_cost:
            cur["wins"] = _safe_int(cur.get("wins"), 0) + 1
        elif gross_pnl < transaction_cost:
            cur["losses"] = _safe_int(cur.get("losses"), 0) + 1

        written += 1

    return written


def _signed_fill_qty(fill_qty: Any, order_qty: Any) -> float:
    q = _safe_float(fill_qty, 0.0)
    if abs(q) > 1e-12:
        return float(q)
    oq = _safe_float(order_qty, 0.0)
    return float(oq)


def _apply_fill_to_marketplace_state(
    cur: Dict[str, Any],
    *,
    client_order_id: str,
    source_alert_id: Optional[int],
    qty_signed: float,
    fill_px: float,
    fees: float,
    slippage_bps: Optional[float],
    mid_px: Optional[float],
    expected_px: Optional[float],
    confidence: float,
    ts_ms: int,
) -> None:
    if not client_order_id:
        return

    order_ids = cur.setdefault("order_ids", set())
    source_alert_ids = cur.setdefault("source_alert_ids", set())

    if client_order_id not in order_ids:
        order_ids.add(client_order_id)
        cur["trades"] += 1
        trade_count = _safe_int(cur.get("trades"), 0)
        prev_conf = _safe_float(cur.get("avg_confidence"), 0.0)
        cur["avg_confidence"] = (
            (prev_conf * max(0, trade_count - 1)) + float(confidence or 0.0)
        ) / max(1, trade_count)

    if source_alert_id is not None:
        source_alert_ids.add(int(source_alert_id))

    cur["last_signal_ts_ms"] = max(
        _safe_int(cur.get("last_signal_ts_ms"), 0), _safe_int(ts_ms, 0)
    )

    meta = cur.setdefault("meta", {})
    fee_cost = _safe_float(fees, 0.0)
    slippage_cost = 0.0
    if slippage_bps is not None:
        slippage_cost = abs(float(qty_signed or 0.0)) * abs(float(fill_px or 0.0)) * (
            abs(_safe_float(slippage_bps, 0.0)) / 10000.0
        )

    meta["fee_cost"] = _safe_float(meta.get("fee_cost"), 0.0) + float(fee_cost)
    meta["slippage_cost"] = _safe_float(meta.get("slippage_cost"), 0.0) + float(
        slippage_cost
    )
    meta["transaction_cost"] = (
        _safe_float(meta.get("fee_cost"), 0.0)
        + _safe_float(meta.get("slippage_cost"), 0.0)
    )
    meta["score_source"] = "execution_fills"
    meta["filled_qty"] = _safe_float(meta.get("filled_qty"), 0.0) + abs(
        float(qty_signed or 0.0)
    )
    meta["last_fill_price"] = float(fill_px)

    if slippage_bps is not None:
        meta["slippage_weighted_sum"] = _safe_float(
            meta.get("slippage_weighted_sum"), 0.0
        ) + (abs(float(qty_signed or 0.0)) * _safe_float(slippage_bps, 0.0))
        meta["slippage_weight"] = _safe_float(meta.get("slippage_weight"), 0.0) + abs(
            float(qty_signed or 0.0)
        )

    open_qty = _safe_float(meta.get("open_qty"), 0.0)
    entry_price = meta.get("entry_price")
    entry_px = None if entry_price is None else _safe_float(entry_price, 0.0)

    if abs(open_qty) <= 1e-12:
        meta["open_qty"] = float(qty_signed)
        meta["position_size"] = float(qty_signed)
        meta["entry_price"] = float(fill_px)
        meta["avg_price"] = float(fill_px)
        return

    if open_qty * float(qty_signed) > 0:
        new_qty = float(open_qty) + float(qty_signed)
        if entry_px is None or abs(new_qty) <= 1e-12:
            meta["entry_price"] = float(fill_px)
        else:
            meta["entry_price"] = (
                (abs(float(open_qty)) * float(entry_px))
                + (abs(float(qty_signed)) * float(fill_px))
            ) / max(1e-12, abs(float(new_qty)))
        meta["open_qty"] = float(new_qty)
        meta["position_size"] = float(new_qty)
        meta["avg_price"] = meta.get("entry_price")
        return

    base_entry_px = float(entry_px) if entry_px is not None else float(fill_px)
    close_qty = min(abs(float(open_qty)), abs(float(qty_signed)))
    realized_delta = (
        (float(fill_px) - float(base_entry_px))
        * float(close_qty)
        * (1.0 if float(open_qty) > 0.0 else -1.0)
    )

    meta["realized_pnl"] = _safe_float(meta.get("realized_pnl"), 0.0) + float(
        realized_delta
    )
    meta["exit_price"] = float(fill_px)

    if realized_delta > 0:
        cur["wins"] += 1
    elif realized_delta < 0:
        cur["losses"] += 1

    remaining_qty = float(open_qty) + float(qty_signed)
    if abs(remaining_qty) <= 1e-12:
        meta["open_qty"] = 0.0
        meta["position_size"] = 0.0
        meta["entry_price"] = None
        meta["avg_price"] = None
        return

    if float(open_qty) * float(remaining_qty) < 0:
        meta["open_qty"] = float(remaining_qty)
        meta["position_size"] = float(remaining_qty)
        meta["entry_price"] = float(fill_px)
        meta["avg_price"] = float(fill_px)
        return

    meta["open_qty"] = float(remaining_qty)
    meta["position_size"] = float(remaining_qty)
    meta["entry_price"] = float(base_entry_px)
    meta["avg_price"] = float(base_entry_px)


def _extract_model_name(
    model_json: Dict[str, Any],
    signal_json: Dict[str, Any],
    decision_json: Dict[str, Any],
) -> str:
    for obj in (model_json, signal_json, decision_json):
        for key in ("model_name", "strategy_name", "strategy", "model"):
            val = obj.get(key)
            if isinstance(val, str) and val.strip():
                return str(val).strip()

    explain = signal_json.get("alert_explain")
    if isinstance(explain, dict):
        for key in ("model_name", "strategy_name", "strategy", "model"):
            val = explain.get(key)
            if isinstance(val, str) and val.strip():
                return str(val).strip()

        model_obj = explain.get("model")
        if isinstance(model_obj, dict):
            for key in ("model_name", "name", "id"):
                val = model_obj.get(key)
                if isinstance(val, str) and val.strip():
                    return str(val).strip()

        strategy_obj = explain.get("strategy")
        if isinstance(strategy_obj, dict):
            val = strategy_obj.get("name")
            if isinstance(val, str) and val.strip():
                return str(val).strip()

    dotted_name = model_json.get("model.name")
    if isinstance(dotted_name, str) and dotted_name.strip():
        return str(dotted_name).strip()

    return "default_challenger"


def _extract_model_id(model_json: Dict[str, Any], signal_json: Dict[str, Any]) -> str:
    for obj in (model_json, signal_json):
        for key in ("model_id", "agent_id"):
            val = obj.get(key)
            if isinstance(val, str) and val.strip():
                return _normalize_model_id(val)
    explain = signal_json.get("alert_explain")
    if isinstance(explain, dict):
        for key in ("model_id", "agent_id"):
            val = explain.get(key)
            if isinstance(val, str) and val.strip():
                return _normalize_model_id(val)
        model_obj = explain.get("model")
        if isinstance(model_obj, dict):
            for key in ("model_id", "id", "agent_id"):
                val = model_obj.get(key)
                if isinstance(val, str) and val.strip():
                    return _normalize_model_id(val)
    return "baseline"


def _extract_horizon_s(model_json: Dict[str, Any], signal_json: Dict[str, Any]) -> int:
    for obj in (signal_json, model_json):
        for key in ("horizon_s", "model.horizon_s"):
            val = obj.get(key)
            if val is not None:
                return _safe_int(val, 0)
    return 0


def _extract_model_kind(model_json: Dict[str, Any], signal_json: Dict[str, Any]) -> str:
    for obj in (model_json, signal_json):
        for key in ("model_kind", "kind", "type"):
            val = obj.get(key)
            if isinstance(val, str) and val.strip():
                return str(val).strip()
    model_obj = model_json.get("model")
    if isinstance(model_obj, dict):
        for key in ("model_kind", "kind", "type"):
            val = model_obj.get(key)
            if isinstance(val, str) and val.strip():
                return str(val).strip()
    return ""


def _extract_model_ts_ms(model_json: Dict[str, Any], signal_json: Dict[str, Any]) -> Optional[int]:
    for obj in (model_json, signal_json):
        for key in ("model_ts_ms", "ts_ms", "trained_ts_ms"):
            val = obj.get(key)
            if val is not None:
                try:
                    return int(val)
                except Exception as e:
                    _warn_nonfatal(
                        "MODEL_MARKETPLACE_MODEL_TS_PARSE_FAILED",
                        e,
                        once_key=f"model_ts_parse:{key}",
                        key=str(key),
                        value=val,
                    )
    model_obj = model_json.get("model")
    if isinstance(model_obj, dict):
        for key in ("model_ts_ms", "ts_ms", "trained_ts_ms"):
            val = model_obj.get(key)
            if val is not None:
                try:
                    return int(val)
                except Exception as e:
                    _warn_nonfatal(
                        "MODEL_MARKETPLACE_MODEL_TS_PARSE_FAILED",
                        e,
                        once_key=f"model_ts_parse:model:{key}",
                        key=str(key),
                        value=val,
                    )
    return None


def _extract_regime(regime_json: Dict[str, Any], signal_json: Dict[str, Any]) -> str:
    for obj in (regime_json, signal_json):
        for key in ("regime", "current_regime", "regime_label"):
            val = obj.get(key)
            if isinstance(val, str) and val.strip():
                return str(val).strip()
    return "global"


def _competition_key(
    model_id: str,
    model_name: str,
    symbol: str,
    horizon_s: int,
    regime: str,
) -> str:
    return "|".join(
        [
            str(model_name or "").strip(),
            _normalize_model_id(model_id),
            str(symbol or "").upper().strip(),
            str(int(horizon_s or 0)),
            str(regime or "global").strip() or "global",
        ]
    )


def _sqlite_table_columns_or_none(con, table_name: str) -> Optional[set[str]]:
    try:
        rows = con.execute(f"PRAGMA table_info({table_name})").fetchall()
    except Exception:
        return None
    return {
        str(row[1] or "").strip()
        for row in (rows or [])
        if row and len(row) > 1
    }


def _horizon_bucket(horizon_s: int) -> str:
    hs = _safe_int(horizon_s, 0)
    if hs <= 0:
        return "unknown"
    if hs <= 30 * 60:
        return "short"
    if hs <= 6 * 60 * 60:
        return "medium"
    return "long"


def _extract_stage_for_key(
    con,
    *,
    model_name: str,
    symbol: str,
    horizon_s: int,
    regime: str,
) -> str:
    champion_cols = _sqlite_table_columns_or_none(con, "champion_assignments")
    if champion_cols is not None and not {"model_name", "symbol", "horizon_s", "regime", "state"}.issubset(champion_cols):
        return "challenger"
    try:
        row = con.execute(
            """
            SELECT model_name
            FROM champion_assignments
            WHERE scope='global' AND symbol=? AND horizon_s=? AND regime=? AND state='champion'
            LIMIT 1
            """,
            (str(symbol).upper().strip(), int(horizon_s), str(regime or "global")),
        ).fetchone()
        if row and str(row[0] or "").strip() == str(model_name or "").strip():
            return "champion"
    except Exception as e:
        _warn_nonfatal(
            "MODEL_MARKETPLACE_CURRENT_STAGE_LOOKUP_FAILED",
            e,
            symbol=str(symbol).upper().strip(),
            horizon_s=int(horizon_s),
            regime=str(regime or "global"),
            model_name=str(model_name or ""),
        )
    return "challenger"


def _load_marketplace_snapshot_row(
    con,
    *,
    model_id: str,
    model_name: str,
    symbol: str,
    horizon_s: int,
    regime: str,
    stage: str,
) -> Dict[str, Any]:
    score_cols = _sqlite_table_columns_or_none(con, "model_marketplace_scores")
    if score_cols is not None and not {
        "model_id",
        "model_name",
        "symbol",
        "horizon_s",
        "regime",
        "score",
        "trades",
        "wins",
        "losses",
        "gross_pnl",
        "net_pnl",
        "avg_confidence",
        "last_signal_ts_ms",
        "meta_json",
    }.issubset(score_cols):
        return {
            "model_name": str(model_name or "").strip(),
            "model_id": _normalize_model_id(model_id),
            "symbol": str(symbol or "").upper().strip(),
            "horizon_s": int(horizon_s),
            "regime": str(regime or "global"),
            "stage": str(stage or "challenger"),
            "score": 0.0,
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "gross_pnl": 0.0,
            "net_pnl": 0.0,
            "avg_confidence": 0.0,
            "last_signal_ts_ms": 0,
            "updated_ts_ms": _now_ms(),
            "order_ids": set(),
            "source_alert_ids": set(),
            "meta": {
                "realized_pnl": 0.0,
                "model_id": _normalize_model_id(model_id),
                "unrealized_pnl": 0.0,
                "transaction_cost": 0.0,
                "entry_price": None,
                "exit_price": None,
                "last_price": None,
                "last_fill_price": None,
                "filled_qty": 0.0,
                "open_qty": 0.0,
                "slippage_weighted_sum": 0.0,
                "slippage_weight": 0.0,
            },
        }
    row = con.execute(
        """
        SELECT score, trades, wins, losses, gross_pnl, net_pnl, avg_confidence, last_signal_ts_ms, meta_json
        FROM model_marketplace_scores
        WHERE model_id=? AND model_name=? AND symbol=? AND horizon_s=? AND regime=?
        """,
        (
            _normalize_model_id(model_id),
            str(model_name or "").strip(),
            str(symbol or "").upper().strip(),
            int(horizon_s),
            str(regime or "global"),
        ),
    ).fetchone()

    if not row:
        return {
            "model_name": str(model_name or "").strip(),
            "model_id": _normalize_model_id(model_id),
            "symbol": str(symbol or "").upper().strip(),
            "horizon_s": int(horizon_s),
            "regime": str(regime or "global"),
            "stage": str(stage or "challenger"),
            "score": 0.0,
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "gross_pnl": 0.0,
            "net_pnl": 0.0,
            "avg_confidence": 0.0,
            "last_signal_ts_ms": 0,
            "updated_ts_ms": _now_ms(),
            "order_ids": set(),
            "source_alert_ids": set(),
            "meta": {
                "realized_pnl": 0.0,
                "model_id": _normalize_model_id(model_id),
                "unrealized_pnl": 0.0,
                "transaction_cost": 0.0,
                "entry_price": None,
                "exit_price": None,
                "last_price": None,
                "last_fill_price": None,
                "filled_qty": 0.0,
                "open_qty": 0.0,
                "slippage_weighted_sum": 0.0,
                "slippage_weight": 0.0,
            },
        }

    meta = _safe_json_dict(row[8])
    return {
        "model_name": str(model_name or "").strip(),
        "model_id": _normalize_model_id(model_id),
        "symbol": str(symbol or "").upper().strip(),
        "horizon_s": int(horizon_s),
        "regime": str(regime or "global"),
        "stage": str(stage or "challenger"),
        "score": _safe_float(row[0], 0.0),
        "trades": _safe_int(row[1], 0),
        "wins": _safe_int(row[2], 0),
        "losses": _safe_int(row[3], 0),
        "gross_pnl": _safe_float(row[4], 0.0),
        "net_pnl": _safe_float(row[5], 0.0),
        "avg_confidence": _safe_float(row[6], 0.0),
        "last_signal_ts_ms": _safe_int(row[7], 0),
        "updated_ts_ms": _now_ms(),
        "order_ids": set(str(x) for x in (meta.get("client_order_ids") or []) if x),
        "source_alert_ids": set(
            int(x) for x in (meta.get("source_alert_ids") or []) if x is not None
        ),
        "meta": {
            "realized_pnl": _safe_float(meta.get("realized_pnl"), 0.0),
            "model_id": _normalize_model_id(meta.get("model_id") or model_id),
            "unrealized_pnl": _safe_float(meta.get("unrealized_pnl"), 0.0),
            "transaction_cost": _safe_float(meta.get("transaction_cost"), 0.0),
            "fee_cost": _safe_float(meta.get("fee_cost"), 0.0),
            "slippage_cost": _safe_float(meta.get("slippage_cost"), 0.0),
            "entry_price": (
                meta.get("entry_price")
                if meta.get("entry_price") is not None
                else meta.get("avg_price")
            ),
            "avg_price": (
                meta.get("avg_price")
                if meta.get("avg_price") is not None
                else meta.get("entry_price")
            ),
            "exit_price": meta.get("exit_price"),
            "last_price": meta.get("last_price"),
            "last_fill_price": meta.get("last_fill_price"),
            "filled_qty": _safe_float(meta.get("filled_qty"), 0.0),
            "open_qty": _safe_float(
                meta.get("open_qty")
                if meta.get("open_qty") is not None
                else meta.get("position_size"),
                0.0,
            ),
            "position_size": _safe_float(
                meta.get("position_size")
                if meta.get("position_size") is not None
                else meta.get("open_qty"),
                0.0,
            ),
            "slippage_weighted_sum": (
                _safe_float(meta.get("avg_slippage_bps"), 0.0)
                * max(1.0, _safe_float(meta.get("filled_qty"), 0.0))
            ),
            "slippage_weight": max(0.0, _safe_float(meta.get("filled_qty"), 0.0)),
        },
    }


def _write_marketplace_row(con, cur: Dict[str, Any], now: Optional[int] = None) -> Dict[str, Any]:
    ts_now = int(now or _now_ms())
    meta = dict(cur.get("meta") or {})
    sym = str(cur.get("symbol") or "").upper().strip()
    latest_px = _load_latest_prices(con)
    last_px = latest_px.get(sym)
    if last_px is not None:
        meta["last_price"] = float(last_px)

    open_qty = _safe_float(meta.get("open_qty"), 0.0)
    entry_price = meta.get("entry_price")
    score_source = str(meta.get("score_source") or "").strip().lower()
    if (
        score_source == "execution_fills"
        and last_px is not None
        and entry_price is not None
        and abs(open_qty) > 1e-12
    ):
        meta["unrealized_pnl"] = (
            float(last_px) - _safe_float(entry_price, 0.0)
        ) * float(open_qty)
    elif score_source == "execution_fills":
        meta["unrealized_pnl"] = 0.0

    slippage_weight = _safe_float(meta.get("slippage_weight"), 0.0)
    meta["avg_slippage_bps"] = (
        _safe_float(meta.get("slippage_weighted_sum"), 0.0) / max(1e-12, slippage_weight)
        if slippage_weight > 0.0
        else 0.0
    )
    meta["position_size"] = float(open_qty)
    meta["avg_price"] = entry_price
    if score_source == "shadow_predictions":
        meta["transaction_cost"] = _safe_float(meta.get("transaction_cost"), 0.0)
    else:
        meta["transaction_cost"] = (
            _safe_float(meta.get("fee_cost"), 0.0)
            + _safe_float(meta.get("slippage_cost"), 0.0)
        )

    order_ids = cur.get("order_ids") or set()
    source_alert_ids = cur.get("source_alert_ids") or set()
    meta["client_order_ids"] = sorted(str(x) for x in order_ids if x)
    meta["model_id"] = _normalize_model_id(cur.get("model_id"))
    meta["source_alert_ids"] = sorted(
        int(x) for x in source_alert_ids if x is not None
    )
    meta["horizon_bucket"] = _horizon_bucket(_safe_int(cur.get("horizon_s"), 0))
    for key in ("evaluation_timestamps", "regime_labels", "challenger_predictions", "realized_returns"):
        values = cur.get(key)
        if isinstance(values, list) and values:
            meta[key] = list(values)

    meta.pop("slippage_weighted_sum", None)
    meta.pop("slippage_weight", None)
    meta.pop("execution_mid_cost", None)

    realized_pnl = _safe_float(meta.get("realized_pnl"), 0.0)
    unrealized_pnl = _safe_float(meta.get("unrealized_pnl"), 0.0)
    transaction_cost = _safe_float(meta.get("transaction_cost"), 0.0)
    meta["total_pnl"] = float(realized_pnl + unrealized_pnl - transaction_cost)
    meta["rolling_realized_pnl"] = float(realized_pnl)
    meta["rolling_unrealized_pnl"] = float(unrealized_pnl)
    meta["rolling_total_pnl"] = float(meta["total_pnl"])
    meta["rolling_window_ms"] = int(max(1, MODEL_COMPETITION_WINDOW_S) * 1000)
    meta["rolling_window_s"] = int(max(1, MODEL_COMPETITION_WINDOW_S))
    meta["observation_duration_ms"] = max(
        0,
        int(_safe_int(meta.get("last_signal_ts_ms"), ts_now) - _safe_int(meta.get("first_signal_ts_ms"), ts_now)),
    )
    cur["meta"] = meta
    _update_row_tracking(cur, ts_ms=_safe_int(meta.get("last_signal_ts_ms"), ts_now))

    cur["gross_pnl"] = float(realized_pnl + unrealized_pnl)
    cur["net_pnl"] = float(meta["total_pnl"])
    _apply_marketplace_score(cur)
    meta = dict(cur.get("meta") or meta)
    cur["updated_ts_ms"] = int(ts_now)

    CompetitionRepository(con).upsert_marketplace_score(
        cur,
        meta=meta,
        updated_ts_ms=int(ts_now),
    )
    return cur


def record_shadow_order(
    *,
    model_name: str,
    symbol: str,
    side: str,
    qty: float,
    ref_price: Optional[float],
    confidence: Optional[float],
    horizon_s: int = 0,
    regime: str = "global",
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    # Shadow orders are the raw challenger observations. They never place real
    # orders; they only capture "what this model would have done" so later
    # scoring and champion selection can compare candidates on a common ledger.
    con = connect()
    try:
        con.execute(
            """
            INSERT INTO challenger_shadow_orders(
              ts_ms, model_name, symbol, horizon_s, side, qty, ref_price, confidence, regime, status, meta_json
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                _now_ms(),
                str(model_name),
                str(symbol).upper().strip(),
                int(horizon_s),
                str(side).lower().strip(),
                float(qty or 0.0),
                None if ref_price is None else float(ref_price),
                None if confidence is None else float(confidence),
                str(regime or "global"),
                "shadow",
                json.dumps(meta or {}, separators=(",", ":"), sort_keys=True),
            ),
        )
        con.commit()
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("MODEL_MARKETPLACE_RECORD_SHADOW_ORDER_CLOSE_FAILED", e)


def update_model_score(
    *,
    model_name: str,
    symbol: str,
    horizon_s: int = 0,
    regime: str = "global",
    stage: str = "challenger",
    pnl_delta: float = 0.0,
    confidence: float = 0.0,
    won: Optional[bool] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    now = _now_ms()
    model_id = _normalize_model_id((meta or {}).get("model_id") if isinstance(meta, dict) else None)
    sym = str(symbol or "").upper().strip()
    name = str(model_name or "").strip()
    reg = str(regime or "global").strip() or "global"
    stg = str(stage or "challenger").strip() or "challenger"

    con = connect()
    try:
        row = con.execute(
            """
            SELECT score, trades, wins, losses, gross_pnl, net_pnl, avg_confidence, meta_json
            FROM model_marketplace_scores
            WHERE model_id=? AND model_name=? AND symbol=? AND horizon_s=? AND regime=?
            """,
            (model_id, name, sym, int(horizon_s), reg),
        ).fetchone()

        if row:
            score = _safe_float(row[0], 0.0)
            trades = _safe_int(row[1], 0)
            wins = _safe_int(row[2], 0)
            losses = _safe_int(row[3], 0)
            gross_pnl = _safe_float(row[4], 0.0)
            net_pnl = _safe_float(row[5], 0.0)
            avg_conf = _safe_float(row[6], 0.0)
            old_meta = _safe_json_dict(row[7])
        else:
            score = 0.0
            trades = 0
            wins = 0
            losses = 0
            gross_pnl = 0.0
            net_pnl = 0.0
            avg_conf = 0.0
            old_meta = {}

        trades += 1
        if won is True:
            wins += 1
        elif won is False:
            losses += 1

        gross_pnl += float(pnl_delta or 0.0)
        net_pnl += float(pnl_delta or 0.0)
        avg_conf = ((avg_conf * max(0, trades - 1)) + float(confidence or 0.0)) / max(
            1, trades
        )

        if isinstance(meta, dict):
            old_meta.update(meta)

        old_meta.setdefault("score_source", "manual_update")
        old_meta["rolling_realized_pnl"] = float(net_pnl)
        old_meta["rolling_unrealized_pnl"] = 0.0
        old_meta["rolling_total_pnl"] = float(net_pnl)
        old_meta["rolling_window_ms"] = int(max(1, MODEL_COMPETITION_WINDOW_S) * 1000)
        old_meta["rolling_window_s"] = int(max(1, MODEL_COMPETITION_WINDOW_S))
        tmp_cur = {
            "trades": int(trades),
            "wins": int(wins),
            "losses": int(losses),
            "net_pnl": float(net_pnl),
            "event_pnls": [float(pnl_delta or 0.0)] if abs(float(pnl_delta or 0.0)) > 1e-12 else [float(net_pnl)],
            "meta": dict(old_meta),
        }
        tmp_cur["meta"]["total_pnl"] = float(net_pnl)
        tmp_cur["meta"]["recent_total_pnl"] = float(net_pnl)
        tmp_cur["meta"]["prior_total_pnl"] = 0.0
        _apply_marketplace_score(tmp_cur)
        score = _safe_float(tmp_cur.get("score"), 0.0)
        old_meta = dict(tmp_cur.get("meta") or old_meta)

        CompetitionRepository(con).upsert_marketplace_score(
            {
                "model_id": model_id,
                "model_name": name,
                "symbol": sym,
                "horizon_s": int(horizon_s),
                "regime": reg,
                "stage": stg,
                "score": float(score),
                "trades": int(trades),
                "wins": int(wins),
                "losses": int(losses),
                "gross_pnl": float(gross_pnl),
                "net_pnl": float(net_pnl),
                "avg_confidence": float(avg_conf),
                "last_signal_ts_ms": int(now),
            },
            meta=old_meta,
            updated_ts_ms=int(now),
        )
        con.commit()

        snap = {
            "model_id": model_id,
            "model_name": name,
            "symbol": sym,
            "horizon_s": int(horizon_s),
            "regime": reg,
            "stage": stg,
            "score": float(score),
            "trades": int(trades),
            "wins": int(wins),
            "losses": int(losses),
            "gross_pnl": float(gross_pnl),
            "net_pnl": float(net_pnl),
            "avg_confidence": float(avg_conf),
            "updated_ts_ms": int(now),
        }
        return snap
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("MODEL_MARKETPLACE_SHADOW_ORDERS_CLOSE_FAILED", e)


def upsert_marketplace_candidate(
    *,
    model_name: str,
    symbol: str,
    horizon_s: int,
    regime: str = "global",
    stage: str = "challenger",
    score: float = 0.0,
    trades: int = 0,
    wins: int = 0,
    losses: int = 0,
    gross_pnl: float = 0.0,
    net_pnl: float = 0.0,
    avg_confidence: float = 0.0,
    last_signal_ts_ms: int = 0,
    meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    cur = _default_marketplace_row(
        model_id=_normalize_model_id((meta or {}).get("model_id") if isinstance(meta, dict) else None),
        model_name=str(model_name or "").strip(),
        symbol=str(symbol or "").upper().strip(),
        horizon_s=int(horizon_s or 0),
        regime=str(regime or "global"),
        stage=str(stage or "challenger"),
    )
    cur["score"] = float(score or 0.0)
    cur["trades"] = int(trades or 0)
    cur["wins"] = int(wins or 0)
    cur["losses"] = int(losses or 0)
    cur["gross_pnl"] = float(gross_pnl or 0.0)
    cur["net_pnl"] = float(net_pnl or 0.0)
    cur["avg_confidence"] = float(avg_confidence or 0.0)
    cur["last_signal_ts_ms"] = int(last_signal_ts_ms or 0)
    if isinstance(meta, dict):
        cur["meta"].update(dict(meta))

    con = connect()
    try:
        _write_marketplace_row(con, cur)
        con.commit()
        return cur
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("MODEL_MARKETPLACE_SCORE_SNAPSHOT_CLOSE_FAILED", e)


def record_live_fill_attribution(
    *,
    client_order_id: str,
    fill_qty: float,
    fill_px: float,
    fill_ts_ms: int,
    fees: float = 0.0,
    slippage_bps: Optional[float] = None,
    con=None,
    commit: bool = True,
) -> Dict[str, Any]:
    owns_con = con is None
    if owns_con:
        con = connect()
    try:
        row = con.execute(
            """
            SELECT
              o.source_alert_id,
              o.symbol,
              o.qty,
              o.submit_ts_ms,
              o.extra_json,
              a.horizon_s,
              a.confidence,
              a.explain_json,
              o.mid_px,
              o.expected_px
            FROM execution_orders o
            LEFT JOIN alerts a
              ON a.id = o.source_alert_id
            WHERE o.client_order_id=?
            LIMIT 1
            """,
            (str(client_order_id),),
        ).fetchone()
        if not row:
            return {"ok": False, "status": "missing_execution_order"}

        source_alert_id, symbol, order_qty, submit_ts_ms, order_extra_json_raw, alert_horizon_s, alert_confidence, alert_explain_raw, order_mid_px, order_expected_px = row
        sym = str(symbol or "").upper().strip()
        if not sym:
            return {"ok": False, "status": "missing_symbol"}

        order_extra_json = _safe_json_dict(order_extra_json_raw)
        alert_explain = _safe_json_dict(alert_explain_raw)
        signal_json: Dict[str, Any] = {
            "source_alert_id": (int(source_alert_id) if source_alert_id is not None else None),
            "horizon_s": _safe_int(
                alert_horizon_s if alert_horizon_s is not None else order_extra_json.get("horizon_s"),
                0,
            ),
            "confidence": _safe_float(
                alert_confidence if alert_confidence is not None else order_extra_json.get("confidence"),
                0.0,
            ),
        }
        if alert_explain:
            signal_json["alert_explain"] = alert_explain

        model_json = dict(order_extra_json)
        decision_json = alert_explain

        model_id = _extract_model_id(model_json, signal_json)
        model_name = _extract_model_name(model_json, signal_json, decision_json)
        model_kind = _extract_model_kind(model_json, signal_json)
        model_ts_ms = _extract_model_ts_ms(model_json, signal_json)
        horizon_s = _extract_horizon_s(model_json, signal_json)
        regime = _extract_regime(alert_explain, signal_json)
        stage = _extract_stage_for_key(
            con,
            model_name=model_name,
            symbol=sym,
            horizon_s=horizon_s,
            regime=regime,
        )

        cur = _load_marketplace_snapshot_row(
            con,
            model_id=model_id,
            model_name=model_name,
            symbol=sym,
            horizon_s=horizon_s,
            regime=regime,
            stage=stage,
        )

        _apply_fill_to_marketplace_state(
            cur,
            client_order_id=str(client_order_id or ""),
            source_alert_id=(int(source_alert_id) if source_alert_id is not None else None),
            qty_signed=float(_signed_fill_qty(fill_qty, order_qty)),
            fill_px=_safe_float(fill_px, 0.0),
            fees=_safe_float(fees, 0.0),
            slippage_bps=(_safe_float(slippage_bps, 0.0) if slippage_bps is not None else None),
            mid_px=(_safe_float(order_mid_px, 0.0) if order_mid_px is not None else None),
            expected_px=(
                _safe_float(order_expected_px, 0.0)
                if order_expected_px is not None
                else None
            ),
            confidence=_safe_float(signal_json.get("confidence"), 0.0),
            ts_ms=_safe_int(submit_ts_ms if submit_ts_ms is not None else fill_ts_ms, 0),
        )

        if model_kind:
            cur["meta"]["model_kind"] = str(model_kind)
        if model_ts_ms is not None:
            cur["meta"]["model_ts_ms"] = int(model_ts_ms)
        cur["model_id"] = _normalize_model_id(model_id)

        written = _write_marketplace_row(con, cur, now=_safe_int(fill_ts_ms, _now_ms()))
        if bool(commit):
            con.commit()
        return {
            "ok": True,
            "model_id": _normalize_model_id(model_id),
            "model_name": str(model_name),
            "symbol": sym,
            "horizon_s": int(horizon_s),
            "regime": str(regime),
            "score": float(written.get("score") or 0.0),
        }
    finally:
        if owns_con:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal("MODEL_MARKETPLACE_ASSIGNMENT_MUTATION_CLOSE_FAILED", e)


def build_replay_validation_snapshot(
    *,
    lookback_events: int = REPLAY_LOOKBACK_EVENTS,
    lookback_ms: int = 24 * 60 * 60 * 1000,
) -> Dict[str, Any]:
    now = _now_ms()
    try:
        from engine.runtime.event_log import flush_event_log_buffer

        flush_event_log_buffer(max_batches=64)
    except Exception as e:
        _warn_nonfatal(
            "MODEL_MARKETPLACE_REPLAY_VALIDATION_FLUSH_FAILED",
            e,
            once_key="model_marketplace_replay_validation_flush_failed",
        )
    con = connect()
    try:
        max_event_id = _safe_int(
            (con.execute("SELECT MAX(id) FROM event_log").fetchone() or [0])[0], 0
        )
        after_event_id = max(0, int(max_event_id) - int(max(1, lookback_events)))
        replay = replay_state(after_event_id=after_event_id, limit=int(lookback_events))
        models: Dict[str, Dict[str, Any]] = {}
        metric_rows = []
        try:
            metric_rows = con.execute(
                """
                SELECT
                  sp.ts_ms,
                  sp.event_id,
                  sp.symbol,
                  sp.regime,
                  sp.horizon_s,
                  sp.model_name,
                  sp.model_kind,
                  sp.model_ts_ms,
                  sp.predicted_z,
                  COALESCE(sp.net_pred_z, sp.predicted_z) AS net_pred_z,
                  p.predicted_z AS baseline_pred,
                  COALESCE(le.net_z, l.impact_z) AS realized_z
                FROM shadow_predictions sp
                LEFT JOIN labels_exec le
                  ON le.event_id = sp.event_id
                 AND le.symbol = sp.symbol
                 AND le.horizon_s = sp.horizon_s
                LEFT JOIN labels l
                  ON l.event_id = sp.event_id
                 AND l.symbol = sp.symbol
                 AND l.horizon_s = sp.horizon_s
                LEFT JOIN predictions p
                  ON p.event_id = sp.event_id
                 AND p.symbol = sp.symbol
                 AND p.horizon_s = sp.horizon_s
                WHERE sp.ts_ms >= ?
                  AND COALESCE(le.net_z, l.impact_z) IS NOT NULL
                ORDER BY sp.ts_ms DESC, sp.id DESC
                """,
                (int(now - int(lookback_ms)),),
            ).fetchall()
        except Exception:
            metric_rows = []

        grouped: Dict[Tuple[str, str, int, str], Dict[str, Any]] = {}
        grouped_all: Dict[Tuple[str, int, str], Dict[str, Any]] = {}

        for row in metric_rows or []:
            ts_ms = _safe_int(row[0], 0)
            event_id = _safe_int(row[1], 0)
            symbol = str(row[2] or "").upper().strip()
            regime = str(row[3] or "global")
            horizon_s = _safe_int(row[4], 0)
            model_name = str(row[5] or "")
            model_kind = str(row[6] or "")
            model_ts_ms = _safe_int(row[7], 0)
            predicted_z = _safe_float(row[8], 0.0)
            net_pred_z = _safe_float(row[9], predicted_z)
            realized_z = _safe_float(row[11], 0.0)
            if not symbol or not model_name or horizon_s <= 0:
                continue

            key = (model_name, symbol, horizon_s, regime)
            all_key = (model_name, horizon_s, regime)
            bucket = grouped.get(key)
            if bucket is None:
                bucket = {
                    "model_name": model_name,
                    "symbol": symbol,
                    "regime": regime,
                    "horizon_s": horizon_s,
                    "model_kind": model_kind,
                    "model_ts_ms": model_ts_ms,
                    "last_event_id": event_id,
                    "window_end_ms": ts_ms,
                    "preds": [],
                    "net_preds": [],
                    "realized": [],
                    "baseline": [],
                    "event_ts_ms": [],
                }
                grouped[key] = bucket

            bucket["preds"].append(predicted_z)
            bucket["net_preds"].append(net_pred_z)
            bucket["realized"].append(realized_z)
            bucket["event_ts_ms"].append(ts_ms)
            if row[10] is not None:
                bucket["baseline"].append(_safe_float(row[10], 0.0))
            if event_id > _safe_int(bucket.get("last_event_id"), 0):
                bucket["last_event_id"] = event_id
            if ts_ms > _safe_int(bucket.get("window_end_ms"), 0):
                bucket["window_end_ms"] = ts_ms
            if model_ts_ms > _safe_int(bucket.get("model_ts_ms"), 0):
                bucket["model_ts_ms"] = model_ts_ms
            if model_kind and not str(bucket.get("model_kind") or "").strip():
                bucket["model_kind"] = model_kind

            bucket_all = grouped_all.get(all_key)
            if bucket_all is None:
                bucket_all = {
                    "model_name": model_name,
                    "symbol": "*",
                    "regime": regime,
                    "horizon_s": horizon_s,
                    "model_kind": model_kind,
                    "model_ts_ms": model_ts_ms,
                    "last_event_id": event_id,
                    "window_end_ms": ts_ms,
                    "preds": [],
                    "net_preds": [],
                    "realized": [],
                    "baseline": [],
                    "event_ts_ms": [],
                }
                grouped_all[all_key] = bucket_all

            bucket_all["preds"].append(predicted_z)
            bucket_all["net_preds"].append(net_pred_z)
            bucket_all["realized"].append(realized_z)
            bucket_all["event_ts_ms"].append(ts_ms)
            if row[10] is not None:
                bucket_all["baseline"].append(_safe_float(row[10], 0.0))
            if event_id > _safe_int(bucket_all.get("last_event_id"), 0):
                bucket_all["last_event_id"] = event_id
            if ts_ms > _safe_int(bucket_all.get("window_end_ms"), 0):
                bucket_all["window_end_ms"] = ts_ms
            if model_ts_ms > _safe_int(bucket_all.get("model_ts_ms"), 0):
                bucket_all["model_ts_ms"] = model_ts_ms
            if model_kind and not str(bucket_all.get("model_kind") or "").strip():
                bucket_all["model_kind"] = model_kind

        for bucket in list(grouped.values()) + list(grouped_all.values()):
            realized = list(bucket.pop("realized", []) or [])
            preds = list(bucket.pop("preds", []) or [])
            net_preds = list(bucket.pop("net_preds", []) or [])
            baseline = list(bucket.pop("baseline", []) or [])
            event_ts_ms = list(bucket.pop("event_ts_ms", []) or [])
            n = min(len(realized), len(net_preds))
            baseline_n = min(len(realized), len(baseline))
            net_rmse = _rmse(net_preds[:n], realized[:n])
            dir_acc = _dir_acc(net_preds[:n], realized[:n])
            gross_rmse = _rmse(preds[:n], realized[:n])
            signed_alpha = sum(
                (1.0 if _safe_float(p, 0.0) >= 0.0 else -1.0) * _safe_float(r, 0.0)
                for p, r in zip(net_preds[:n], realized[:n])
            )

            baseline_net_rmse = _rmse(baseline[:baseline_n], realized[:baseline_n]) if baseline_n > 0 else None
            baseline_dir_acc = _dir_acc(baseline[:baseline_n], realized[:baseline_n]) if baseline_n > 0 else None
            net_rmse_delta = (
                float(baseline_net_rmse) - float(net_rmse)
                if baseline_net_rmse is not None
                else None
            )
            dir_acc_delta = (
                float(dir_acc) - float(baseline_dir_acc)
                if baseline_dir_acc is not None
                else None
            )

            approved = bool(
                n >= int(REPLAY_MIN_N)
                and dir_acc >= float(REPLAY_MIN_DIR_ACC)
                and net_rmse <= float(REPLAY_MAX_NET_RMSE)
                and (
                    baseline_net_rmse is None
                    or net_rmse <= float(baseline_net_rmse) + float(REPLAY_MAX_BASELINE_NET_RMSE_DEGRADATION)
                )
                and (
                    dir_acc_delta is None
                    or dir_acc_delta >= float(REPLAY_MIN_BASELINE_DIR_ACC_DELTA)
                )
            )

            key = _competition_key(
                _normalize_model_id(bucket.get("model_id")),
                str(bucket.get("model_name") or ""),
                str(bucket.get("symbol") or "*"),
                _safe_int(bucket.get("horizon_s"), 0),
                str(bucket.get("regime") or "global"),
            )
            models[key] = {
                "model_name": str(bucket.get("model_name") or ""),
                "model_id": _normalize_model_id(bucket.get("model_id")),
                "symbol": str(bucket.get("symbol") or "*"),
                "regime": str(bucket.get("regime") or "global"),
                "horizon_s": _safe_int(bucket.get("horizon_s"), 0),
                "model_kind": str(bucket.get("model_kind") or ""),
                "model_ts_ms": _safe_int(bucket.get("model_ts_ms"), 0),
                "n": int(n),
                "baseline_n": int(baseline_n),
                "dir_acc": float(dir_acc),
                "baseline_dir_acc": (float(baseline_dir_acc) if baseline_dir_acc is not None else None),
                "dir_acc_delta": (float(dir_acc_delta) if dir_acc_delta is not None else None),
                "gross_rmse": float(gross_rmse),
                "net_rmse": float(net_rmse),
                "baseline_net_rmse": (float(baseline_net_rmse) if baseline_net_rmse is not None else None),
                "net_rmse_delta": (float(net_rmse_delta) if net_rmse_delta is not None else None),
                "signed_alpha": float(signed_alpha),
                "window_end_ms": _safe_int(bucket.get("window_end_ms"), 0),
                "last_event_id": _safe_int(bucket.get("last_event_id"), 0),
                "evaluation_timestamps": [_safe_int(value, 0) for value in event_ts_ms[:n]],
                "regime_labels": [str(bucket.get("regime") or "global") for _idx in range(int(n))],
                "challenger_predictions": [float(_safe_float(value, 0.0)) for value in net_preds[:n]],
                "realized_returns": [float(_safe_float(value, 0.0)) for value in realized[:n]],
                "approved": bool(approved),
            }

        try:
            temporal_rows = con.execute(
                """
                SELECT
                  symbol,
                  horizon_s,
                  ts_ms,
                  n,
                  rmse,
                  baseline_rmse,
                  directional_acc,
                  baseline_directional_acc,
                  rmse_improvement,
                  diracc_delta,
                  pass_all,
                  detail_json
                FROM temporal_shadow_eval
                WHERE ts_ms >= ?
                ORDER BY ts_ms DESC
                """,
                (int(now - int(lookback_ms)),),
            ).fetchall()
        except Exception:
            temporal_rows = []

        for row in temporal_rows or []:
            symbol = str(row[0] or "").upper().strip()
            horizon_s = _safe_int(row[1], 0)
            ts_ms = _safe_int(row[2], 0)
            detail = _safe_json_dict(row[11])
            if not symbol or horizon_s <= 0:
                continue
            regime = str(detail.get("regime") or "global")
            key = _competition_key("baseline", "temporal_predictor", symbol, horizon_s, regime)
            models[key] = {
                "model_name": "temporal_predictor",
                "symbol": symbol,
                "regime": regime,
                "horizon_s": horizon_s,
                "model_kind": str(detail.get("latest_model_kind") or "temporal_mlp"),
                "model_ts_ms": _safe_int(detail.get("latest_model_ts_ms"), 0),
                "n": _safe_int(row[3], 0),
                "baseline_n": _safe_int(row[3], 0),
                "dir_acc": _safe_float(row[6], 0.0),
                "baseline_dir_acc": _safe_float(row[7], 0.0),
                "dir_acc_delta": _safe_float(row[9], 0.0),
                "gross_rmse": _safe_float(row[4], 9999.0),
                "net_rmse": _safe_float(row[4], 9999.0),
                "baseline_net_rmse": _safe_float(row[5], 9999.0),
                "net_rmse_delta": (
                    _safe_float(row[5], 9999.0) - _safe_float(row[4], 9999.0)
                    if row[5] is not None
                    else None
                ),
                "signed_alpha": _safe_float(detail.get("signed_alpha"), 0.0),
                "window_end_ms": ts_ms,
                "last_event_id": 0,
                "approved": bool(_safe_int(row[10], 0)),
            }

        _merge_raw_replay_validation(
            con,
            now=int(now),
            lookback_ms=int(lookback_ms),
            models=models,
        )

        out = {
            "ok": True,
            "after_event_id": int(after_event_id),
            "last_event_id": int(replay.get("last_event_id") or 0),
            "summary": {
                "decisions": int(len(replay.get("decisions") or [])),
                "orders": int(len(replay.get("orders") or {})),
                "fills": int(len(replay.get("fills") or {})),
                "risk_blocks": int(len(replay.get("risk_blocks") or {})),
                "errors": int(len(replay.get("errors") or [])),
            },
            "models": models,
            "updated_ts_ms": int(now),
        }
        meta_set(
            "competition_replay_validation",
            json.dumps(out, separators=(",", ":"), sort_keys=True),
        )
        meta_set(
            "competition_replay_validation_status",
            json.dumps(
                {
                    "ok": True,
                    "status": "ready",
                    "updated_ts_ms": int(now),
                    "lookback_events": int(lookback_events),
                    "lookback_ms": int(lookback_ms),
                    "model_count": int(len(models)),
                    "last_event_id": int(replay.get("last_event_id") or 0),
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
        )
        return out
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("MODEL_MARKETPLACE_LIVE_ASSIGNMENTS_CLOSE_FAILED", e)


def refresh_replay_validation_snapshot(
    *,
    lookback_events: int = REPLAY_LOOKBACK_EVENTS,
    lookback_ms: int = 24 * 60 * 60 * 1000,
) -> Dict[str, Any]:
    # Replay validation is the offline safety gate for challengers: before a
    # candidate can displace a champion, its historical predictions need a
    # fresh replay snapshot showing acceptable quality.
    started_at = _now_ms()
    try:
        snap = build_replay_validation_snapshot(
            lookback_events=int(lookback_events),
            lookback_ms=int(lookback_ms),
        )
        status = _safe_json_dict(meta_get("competition_replay_validation_status", "") or "{}")
        return {
            "ok": True,
            "status": "ready",
            "updated_ts_ms": _safe_int(status.get("updated_ts_ms") or snap.get("updated_ts_ms"), 0),
            "age_ms": 0,
            "model_count": len((snap or {}).get("models") or {}),
            "duration_ms": max(0, _now_ms() - int(started_at)),
            "snapshot": snap,
        }
    except Exception as e:
        failed = {
            "ok": False,
            "status": "error",
            "error": str(e),
            "updated_ts_ms": int(started_at),
        }
        try:
            meta_set(
                "competition_replay_validation_status",
                json.dumps(failed, separators=(",", ":"), sort_keys=True),
            )
        except Exception as e:
            _warn_nonfatal(
                "MODEL_MARKETPLACE_REPLAY_FAILURE_STATUS_PERSIST_FAILED",
                e,
                once_key="replay_failure_status_persist",
            )
        return failed


def get_cached_replay_validation_snapshot(
    *,
    max_age_ms: int = REPLAY_FRESH_MAX_AGE_MS,
) -> Dict[str, Any]:
    snap = _safe_json_dict(meta_get("competition_replay_validation", "") or "{}")
    status = _safe_json_dict(meta_get("competition_replay_validation_status", "") or "{}")
    updated_ts_ms = _safe_int(
        status.get("updated_ts_ms") or snap.get("updated_ts_ms"),
        0,
    )
    now = _now_ms()
    age_ms = max(0, int(now - updated_ts_ms)) if updated_ts_ms > 0 else 10 ** 12
    fresh = bool(updated_ts_ms > 0 and age_ms <= int(max(0, max_age_ms)))
    return {
        "ok": bool(snap),
        "fresh": bool(fresh),
        "stale": not bool(fresh),
        "age_ms": int(age_ms),
        "max_age_ms": int(max(0, max_age_ms)),
        "updated_ts_ms": int(updated_ts_ms),
        "status": str(status.get("status") or ("ready" if snap else "missing")),
        "error": status.get("error"),
        "snapshot": snap if isinstance(snap, dict) else {},
    }


def _recent_shadow_stability_rows(
    con,
    *,
    now: int,
    lookback_ms: int,
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    try:
        rows = con.execute(
            """
            SELECT
              sp.model_name,
              sp.symbol,
              sp.horizon_s,
              sp.regime,
              sp.predicted_z,
              COALESCE(le.net_z, l.impact_z) AS realized_z,
              sp.ts_ms
            FROM shadow_predictions sp
            LEFT JOIN labels_exec le
              ON le.event_id = sp.event_id
             AND le.symbol = sp.symbol
             AND le.horizon_s = sp.horizon_s
            LEFT JOIN labels l
              ON l.event_id = sp.event_id
             AND l.symbol = sp.symbol
             AND l.horizon_s = sp.horizon_s
            WHERE sp.ts_ms >= ?
              AND COALESCE(le.net_z, l.impact_z) IS NOT NULL
            ORDER BY sp.ts_ms DESC, sp.id DESC
            """,
            (int(now - int(lookback_ms)),),
        ).fetchall()
    except Exception:
        rows = []

    grouped: Dict[str, List[Tuple[float, float]]] = {}
    for model_name, symbol, horizon_s, regime, predicted_z, realized_z, _ts_ms in rows or []:
        key = _competition_key(
            "baseline",
            str(model_name or ""),
            str(symbol or "").upper().strip(),
            _safe_int(horizon_s, 0),
            str(regime or "global"),
        )
        grouped.setdefault(key, []).append(
            (_safe_float(predicted_z, 0.0), _safe_float(realized_z, 0.0))
        )

    for key, series in grouped.items():
        n = len(series or [])
        half = n // 2
        if n < max(2, int(SELF_CRITIC_MIN_RECENT_WINDOW_N) * 2) or half <= 0:
            continue
        recent = series[:half]
        prior = series[half : half * 2]
        if len(recent) < int(SELF_CRITIC_MIN_RECENT_WINDOW_N) or len(prior) < int(SELF_CRITIC_MIN_RECENT_WINDOW_N):
            continue

        recent_preds = [p for p, _ in recent]
        recent_realized = [r for _, r in recent]
        prior_preds = [p for p, _ in prior]
        prior_realized = [r for _, r in prior]
        recent_dir_acc = _dir_acc(recent_preds, recent_realized)
        prior_dir_acc = _dir_acc(prior_preds, prior_realized)
        recent_signed_alpha = sum((1.0 if _safe_float(p, 0.0) >= 0.0 else -1.0) * _safe_float(r, 0.0) for p, r in recent)
        prior_signed_alpha = sum((1.0 if _safe_float(p, 0.0) >= 0.0 else -1.0) * _safe_float(r, 0.0) for p, r in prior)

        out[key] = {
            "n": int(n),
            "window_n": int(min(len(recent), len(prior))),
            "recent_dir_acc": float(recent_dir_acc),
            "prior_dir_acc": float(prior_dir_acc),
            "dir_acc_drop": float(recent_dir_acc - prior_dir_acc),
            "recent_signed_alpha": float(recent_signed_alpha),
            "prior_signed_alpha": float(prior_signed_alpha),
            "signed_alpha_drop": float(recent_signed_alpha - prior_signed_alpha),
        }
    return out


def run_self_critic(
    *,
    replay_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    replay = replay_snapshot if isinstance(replay_snapshot, dict) else build_replay_validation_snapshot()
    replay_models = dict(replay.get("models") or {})
    now = _now_ms()
    con = connect()
    try:
        capital_scores: Dict[Tuple[str, str], Dict[str, Any]] = {}
        symbol_fragility: Dict[Tuple[str, str], Dict[str, Any]] = {}
        try:
            from engine.runtime.shadow_capital_allocator import get_shadow_capital_scores

            cap_rows = (get_shadow_capital_scores(limit=500, regime="global") or {}).get("rows") or []
            for row in cap_rows:
                capital_scores[(str(row.get("model_name") or ""), str(row.get("regime") or "global"))] = dict(row)
        except Exception:
            capital_scores = {}

        rows = con.execute(
            """
            SELECT model_name, symbol, horizon_s, regime, stage, score, trades, net_pnl, meta_json
            FROM model_marketplace_scores
            ORDER BY updated_ts_ms DESC
            """
        ).fetchall()
        recent_stability = _recent_shadow_stability_rows(
            con,
            now=int(now),
            lookback_ms=int(SELF_CRITIC_RECENT_LOOKBACK_MS),
        )
        grouped_symbols: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
        for row in rows or []:
            grouped_symbols.setdefault(
                (str(row[0] or ""), str(row[3] or "global")),
                [],
            ).append(
                {
                    "symbol": str(row[1] or "").upper().strip(),
                    "horizon_s": _safe_int(row[2], 0),
                    "score": _safe_float(row[5], 0.0),
                    "trades": _safe_int(row[6], 0),
                    "net_pnl": _safe_float(row[7], 0.0),
                }
            )
        for group_key, group_rows in grouped_symbols.items():
            symbols = [r for r in group_rows if str(r.get("symbol") or "").strip()]
            unique_symbols = sorted({str(r.get("symbol") or "") for r in symbols})
            if len(unique_symbols) < int(SELF_CRITIC_MIN_SYMBOL_BREADTH):
                continue
            negative_symbols = sorted(
                {
                    str(r.get("symbol") or "")
                    for r in symbols
                    if _safe_float(r.get("net_pnl"), 0.0) < 0.0
                }
            )
            score_values = [_safe_float(r.get("score"), 0.0) for r in symbols]
            symbol_fragility[group_key] = {
                "symbol_breadth": int(len(unique_symbols)),
                "negative_symbols": negative_symbols,
                "negative_symbol_fraction": float(len(negative_symbols)) / float(max(1, len(unique_symbols))),
                "score_span": (
                    float(max(score_values) - min(score_values))
                    if score_values
                    else 0.0
                ),
            }
        alerts: List[Dict[str, Any]] = []
        blocked: List[str] = []

        for model_name, symbol, horizon_s, regime, stage, score, trades, net_pnl, meta_json in rows or []:
            meta = _safe_json_dict(meta_json)
            model_id = _normalize_model_id(meta.get("model_id"))
            key = _competition_key(model_id, str(model_name or ""), str(symbol or ""), _safe_int(horizon_s, 0), str(regime or "global"))
            replay_key = _competition_key(model_id, str(model_name or ""), str(symbol or ""), _safe_int(horizon_s, 0), str(regime or "global"))
            replay_row = dict(replay_models.get(replay_key) or {})
            if not replay_row:
                replay_key = _competition_key(model_id, str(model_name or ""), "*", _safe_int(horizon_s, 0), str(regime or "global"))
                replay_row = dict(replay_models.get(replay_key) or {})
            avg_slippage = _safe_float(meta.get("avg_slippage_bps"), 0.0)
            unrealized_pnl = _safe_float(meta.get("unrealized_pnl"), 0.0)
            realized_pnl = _safe_float(meta.get("realized_pnl"), 0.0)
            replay_n = _safe_int(replay_row.get("n"), 0)
            replay_rmse_delta = replay_row.get("net_rmse_delta")
            replay_diracc_delta = replay_row.get("dir_acc_delta")
            capital_row = dict(capital_scores.get((str(model_name or ""), str(regime or "global")), {}) or {})
            capital_score = _safe_float(capital_row.get("score"), 0.0)
            capital_n = _safe_int(capital_row.get("n"), 0)
            fragility_row = dict(symbol_fragility.get((str(model_name or ""), str(regime or "global")), {}) or {})
            recent_row = dict(recent_stability.get(key) or {})
            drift_ratio = 0.0
            try:
                drow = con.execute(
                    """
                    SELECT MAX(drift_ratio)
                    FROM model_drift
                    WHERE symbol=? AND horizon_s=?
                    """,
                    (str(symbol or "").upper().strip(), int(horizon_s or 0)),
                ).fetchone()
                drift_ratio = _safe_float((drow or [0.0])[0], 0.0)
            except Exception:
                drift_ratio = 0.0

            issues: List[Tuple[str, str, bool, Dict[str, Any]]] = []
            if _safe_int(trades, 0) >= int(SELF_CRITIC_MIN_TRADES) and _safe_float(net_pnl, 0.0) <= float(SELF_CRITIC_MAX_LOSS):
                issues.append(
                    (
                        "underperformance",
                        "net_pnl below allowed loss threshold",
                        True,
                        {"net_pnl": _safe_float(net_pnl, 0.0), "threshold": float(SELF_CRITIC_MAX_LOSS)},
                    )
                )
            if avg_slippage >= float(SELF_CRITIC_MAX_SLIPPAGE_BPS):
                issues.append(
                    (
                        "anomaly_slippage",
                        "average slippage exceeds anomaly threshold",
                        True,
                        {"avg_slippage_bps": avg_slippage, "threshold": float(SELF_CRITIC_MAX_SLIPPAGE_BPS)},
                    )
                )
            if unrealized_pnl <= float(SELF_CRITIC_MAX_UNREALIZED_DRAWDOWN):
                issues.append(
                    (
                        "instability_drawdown",
                        "unrealized drawdown exceeds instability threshold",
                        True,
                        {"unrealized_pnl": unrealized_pnl, "threshold": float(SELF_CRITIC_MAX_UNREALIZED_DRAWDOWN)},
                    )
                )
            if replay_row and replay_n < int(SELF_CRITIC_MIN_REPLAY_N):
                issues.append(
                    (
                        "instability_low_sample",
                        "replay sample size too small for stable promotion",
                        True,
                        {"replay_n": int(replay_n), "threshold": int(SELF_CRITIC_MIN_REPLAY_N)},
                    )
                )
            if replay_row and replay_rmse_delta is not None and _safe_float(replay_rmse_delta, 0.0) <= float(SELF_CRITIC_MAX_REPLAY_NET_RMSE_DELTA):
                issues.append(
                    (
                        "overfitting_baseline_regression",
                        "replay net RMSE regressed versus baseline",
                        True,
                        {
                            "net_rmse_delta": _safe_float(replay_rmse_delta, 0.0),
                            "threshold": float(SELF_CRITIC_MAX_REPLAY_NET_RMSE_DELTA),
                            "baseline_net_rmse": replay_row.get("baseline_net_rmse"),
                            "net_rmse": replay_row.get("net_rmse"),
                        },
                    )
                )
            if replay_row and replay_diracc_delta is not None and _safe_float(replay_diracc_delta, 0.0) <= float(SELF_CRITIC_MAX_REPLAY_DIRACC_DELTA):
                issues.append(
                    (
                        "instability_directional_regression",
                        "replay directional accuracy regressed versus baseline",
                        True,
                        {
                            "dir_acc_delta": _safe_float(replay_diracc_delta, 0.0),
                            "threshold": float(SELF_CRITIC_MAX_REPLAY_DIRACC_DELTA),
                            "baseline_dir_acc": replay_row.get("baseline_dir_acc"),
                            "dir_acc": replay_row.get("dir_acc"),
                        },
                    )
                )
            if drift_ratio >= float(SELF_CRITIC_MAX_DRIFT_RATIO):
                issues.append(
                    (
                        "anomaly_drift",
                        "model drift ratio exceeds anomaly threshold",
                        True,
                        {"drift_ratio": float(drift_ratio), "threshold": float(SELF_CRITIC_MAX_DRIFT_RATIO)},
                    )
                )
            if capital_n > 0 and capital_score <= float(SELF_CRITIC_MIN_CAPITAL_SCORE):
                issues.append(
                    (
                        "instability_capital_efficiency",
                        "shadow capital score below stability threshold",
                        True,
                        {
                            "capital_score": float(capital_score),
                            "threshold": float(SELF_CRITIC_MIN_CAPITAL_SCORE),
                            "capital_n": int(capital_n),
                        },
                    )
                )
            if recent_row and _safe_float(recent_row.get("dir_acc_drop"), 0.0) <= float(SELF_CRITIC_MAX_RECENT_DIRACC_DROP):
                issues.append(
                    (
                        "instability_recent_regression",
                        "recent directional accuracy degraded versus prior window",
                        True,
                        {
                            "window_n": _safe_int(recent_row.get("window_n"), 0),
                            "recent_dir_acc": _safe_float(recent_row.get("recent_dir_acc"), 0.0),
                            "prior_dir_acc": _safe_float(recent_row.get("prior_dir_acc"), 0.0),
                            "dir_acc_drop": _safe_float(recent_row.get("dir_acc_drop"), 0.0),
                            "threshold": float(SELF_CRITIC_MAX_RECENT_DIRACC_DROP),
                        },
                    )
                )
            if recent_row and _safe_float(recent_row.get("signed_alpha_drop"), 0.0) <= float(SELF_CRITIC_MAX_RECENT_SIGNED_ALPHA_DROP):
                issues.append(
                    (
                        "instability_recent_alpha_decay",
                        "recent signed alpha degraded versus prior window",
                        True,
                        {
                            "window_n": _safe_int(recent_row.get("window_n"), 0),
                            "recent_signed_alpha": _safe_float(recent_row.get("recent_signed_alpha"), 0.0),
                            "prior_signed_alpha": _safe_float(recent_row.get("prior_signed_alpha"), 0.0),
                            "signed_alpha_drop": _safe_float(recent_row.get("signed_alpha_drop"), 0.0),
                            "threshold": float(SELF_CRITIC_MAX_RECENT_SIGNED_ALPHA_DROP),
                        },
                    )
                )
            if fragility_row and _safe_int(fragility_row.get("symbol_breadth"), 0) >= int(SELF_CRITIC_MIN_SYMBOL_BREADTH) and _safe_float(
                fragility_row.get("negative_symbol_fraction"), 0.0
            ) >= float(SELF_CRITIC_MAX_NEGATIVE_SYMBOL_FRACTION):
                issues.append(
                    (
                        "instability_cross_symbol_fragility",
                        "model underperforms on too many symbols in current marketplace view",
                        True,
                        {
                            "symbol_breadth": _safe_int(fragility_row.get("symbol_breadth"), 0),
                            "negative_symbols": list(fragility_row.get("negative_symbols") or []),
                            "negative_symbol_fraction": _safe_float(fragility_row.get("negative_symbol_fraction"), 0.0),
                            "threshold": float(SELF_CRITIC_MAX_NEGATIVE_SYMBOL_FRACTION),
                            "score_span": _safe_float(fragility_row.get("score_span"), 0.0),
                        },
                    )
                )
            if replay_row and not bool(replay_row.get("approved")):
                issues.append(
                    (
                        "overfitting_replay",
                        "replay validation failed",
                        True,
                        replay_row,
                    )
                )
            elif not replay_row and str(stage or "") == "champion":
                issues.append(
                    (
                        "replay_missing",
                        "missing replay validation for champion",
                        False,
                        {"model_name": str(model_name or ""), "horizon_s": _safe_int(horizon_s, 0)},
                    )
                )

            for code, message, hard_block, extra in issues:
                alert = {
                    "ts_ms": int(now),
                    "level": ("critical" if hard_block else "warn"),
                    "model_name": str(model_name or ""),
                    "symbol": str(symbol or ""),
                    "horizon_s": _safe_int(horizon_s, 0),
                    "code": str(code),
                    "message": str(message),
                    "meta": {
                        "score": _safe_float(score, 0.0),
                        "trades": _safe_int(trades, 0),
                        "net_pnl": _safe_float(net_pnl, 0.0),
                        "realized_pnl": realized_pnl,
                        "unrealized_pnl": unrealized_pnl,
                        "drift_ratio": float(drift_ratio),
                        "capital_score": float(capital_score),
                        "capital_n": int(capital_n),
                        **dict(extra or {}),
                    },
                }
                alerts.append(alert)
                if hard_block and key not in blocked:
                    blocked.append(key)
                try:
                    con.execute(
                        """
                        INSERT INTO self_critic_alerts(
                          ts_ms, level, model_name, symbol, horizon_s, code, message, meta_json
                        )
                        VALUES (?,?,?,?,?,?,?,?)
                        """,
                        (
                            int(alert["ts_ms"]),
                            str(alert["level"]),
                            str(alert["model_name"]),
                            str(alert["symbol"]),
                            int(alert["horizon_s"]),
                            str(alert["code"]),
                            str(alert["message"]),
                            json.dumps(alert["meta"], separators=(",", ":"), sort_keys=True),
                        ),
                    )
                except Exception as e:
                    _warn_nonfatal(
                        "MODEL_MARKETPLACE_SELF_CRITIC_ALERT_INSERT_FAILED",
                        e,
                        once_key=f"self_critic_alert:{str(alert.get('code') or '')}",
                        alert_code=str(alert.get("code") or ""),
                    )

        con.commit()
        out = {
            "ok": True,
            "alerts_written": int(len(alerts)),
            "blocked_keys": list(blocked),
            "updated_ts_ms": int(now),
        }
        meta_set("competition_self_critic", json.dumps(out, separators=(",", ":"), sort_keys=True))
        return out
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("MODEL_MARKETPLACE_SELF_CRITIC_CLOSE_FAILED", e)


def compute_capital_plan() -> Dict[str, Any]:
    con = connect()
    try:
        rows = con.execute(
            """
            SELECT model_name, symbol, horizon_s, regime, score, trades, net_pnl, avg_confidence, meta_json
            FROM model_marketplace_scores
            ORDER BY symbol ASC, horizon_s ASC, regime ASC, score DESC, updated_ts_ms DESC
            """
        ).fetchall()

        try:
            from engine.runtime.shadow_capital_allocator import get_shadow_capital_scores
        except Exception:
            get_shadow_capital_scores = None  # type: ignore

        replay_models = dict((_safe_json_dict(meta_get("competition_replay_validation", "") or "{}")).get("models") or {})
        self_critic = _safe_json_dict(meta_get("competition_self_critic", "") or "{}")
        blocked_keys = set(str(x) for x in (self_critic.get("blocked_keys") or []))
        now = _now_ms()

        capital_index: Dict[Tuple[str, str], float] = {}
        regimes = sorted(
            {
                str(r[3] or "global").strip() or "global"
                for r in (rows or [])
            }
        )
        if callable(get_shadow_capital_scores):
            for regime in regimes or ["global"]:
                try:
                    cap_rows = (get_shadow_capital_scores(limit=500, regime=str(regime)) or {}).get("rows") or []
                except Exception:
                    cap_rows = []
                for row in cap_rows:
                    capital_index[(str(row.get("model_name") or ""), str(regime))] = _safe_float(row.get("score"), 0.0)

        recent_stability = _recent_shadow_stability_rows(
            con,
            now=int(now),
            lookback_ms=int(SELF_CRITIC_RECENT_LOOKBACK_MS),
        )

        allocations: Dict[str, Any] = {}
        allocation_strategy = _allocation_strategy()
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for model_name, symbol, horizon_s, regime, score, trades, net_pnl, avg_confidence, meta_json in rows or []:
            meta = _safe_json_dict(meta_json)
            if not _score_source_is_realized_pnl(meta):
                continue
            gk = "|".join(
                [
                    str(symbol or "").upper().strip(),
                    str(int(horizon_s or 0)),
                    str(regime or "global").strip() or "global",
                ]
            )
            grouped.setdefault(gk, []).append(
                {
                    "model_name": str(model_name or ""),
                    "symbol": str(symbol or "").upper().strip(),
                    "horizon_s": _safe_int(horizon_s, 0),
                    "regime": str(regime or "global"),
                    "score": _safe_float(score, 0.0),
                    "trades": _safe_int(trades, 0),
                    "net_pnl": _safe_float(net_pnl, 0.0),
                    "avg_confidence": _safe_float(avg_confidence, 0.0),
                    "meta": meta,
                    "competition_key": gk,
                    "capital_score": _safe_float(
                        capital_index.get((str(model_name or ""), str(regime or "global"))),
                        0.0,
                    ),
                }
            )

        for group_key, candidates in grouped.items():
            if not candidates:
                continue

            weighted = []
            budget_weight_sum = 0.0
            budget_weighted_total = 0.0
            model_risk_weight_sum = 0.0
            model_risk_weighted_total = 0.0
            for row in candidates:
                meta = dict(row.get("meta") or {})
                replay_key = _competition_key(
                    _normalize_model_id(meta.get("model_id")),
                    str(row.get("model_name") or ""),
                    str(row.get("symbol") or ""),
                    _safe_int(row.get("horizon_s"), 0),
                    str(row.get("regime") or "global"),
                )
                replay_row = dict(replay_models.get(replay_key) or {})
                if not replay_row:
                    replay_row = dict(
                        replay_models.get(
                            _competition_key(
                                _normalize_model_id(meta.get("model_id")),
                                str(row.get("model_name") or ""),
                                "*",
                                _safe_int(row.get("horizon_s"), 0),
                                str(row.get("regime") or "global"),
                            )
                        )
                        or {}
                    )
                recent_key = replay_key
                recent_row = dict(recent_stability.get(recent_key) or {})
                metrics = _compute_candidate_capital_metrics(
                    row,
                    replay_row=replay_row,
                    recent_row=recent_row,
                    blocked=(replay_key in blocked_keys),
                    now=int(now),
                )
                enriched_row = {**dict(row), **dict(metrics)}
                raw_weight = float(metrics.get("raw_weight") or 0.001)
                weighted.append((enriched_row, raw_weight))
                budget_weight_sum += float(raw_weight)
                budget_weighted_total += float(metrics.get("group_budget_hint") or 0.0) * float(raw_weight)
                model_risk_weight_sum += float(raw_weight)
                model_risk_weighted_total += float(metrics.get("model_risk_limit_multiplier") or 1.0) * float(raw_weight)

            confidence_inputs = {
                "ensemble_confidence": (
                    sum(_safe_float((row or {}).get("avg_confidence"), 0.0) * float(weight) for row, weight in weighted)
                    / max(1e-9, sum(float(weight) for _row, weight in weighted))
                ),
                "models": {
                    str((row or {}).get("model_name") or ""): _safe_float((row or {}).get("avg_confidence"), 0.0)
                    for row, _weight in weighted
                },
            }
            risk_inputs = {
                "strategy": str(allocation_strategy),
                "max_model_allocation": float(CAPITAL_MAX_GROUP_CHAMPION_SHARE),
                "models": {
                    str((row or {}).get("model_name") or ""): {
                        "max_drawdown": _safe_float(
                            (dict((row or {}).get("meta") or {})).get("max_drawdown"),
                            0.0,
                        ),
                        "effective_stability_score": _safe_float(
                            (row or {}).get("effective_stability_score"),
                            (row or {}).get("stability_score"),
                        ),
                        "recent_regression_stability": _safe_float(
                            (row or {}).get("recent_regression_stability"),
                            0.65,
                        ),
                        "slippage_stability": _safe_float((row or {}).get("slippage_stability"), 0.75),
                        "governance_multiplier": _safe_float((row or {}).get("governance_multiplier"), 1.0),
                        "model_risk_limit_multiplier": _safe_float(
                            (row or {}).get("model_risk_limit_multiplier"),
                            1.0,
                        ),
                    }
                    for row, _weight in weighted
                },
            }
            ranked_models = _shape_ranked_model_allocations(
                weighted,
                strategy=allocation_strategy,
                model_confidence=confidence_inputs,
                risk_metrics=risk_inputs,
            )
            ranked_models.sort(
                key=lambda x: (
                    -_safe_float(x.get("allocation_fraction"), 0.0),
                    -_safe_float(x.get("score"), 0.0),
                    str(x.get("model_name") or ""),
                )
            )
            ranked_models = _normalize_fractions(ranked_models)
            group_budget_fraction = float(
                _clamp(
                    (
                        float(budget_weighted_total) / float(max(1e-9, budget_weight_sum))
                        if budget_weight_sum > 0.0
                        else float(CAPITAL_MIN_GROUP_BUDGET)
                    ),
                    float(CAPITAL_MIN_GROUP_BUDGET),
                    1.0,
                )
            )
            group_risk_limit_multiplier = float(
                _clamp(
                    (
                        float(model_risk_weighted_total) / float(max(1e-9, model_risk_weight_sum))
                        if model_risk_weight_sum > 0.0
                        else 1.0
                    ),
                    float(CAPITAL_MODEL_RISK_MULT_MIN),
                    float(CAPITAL_MODEL_RISK_MULT_MAX),
                )
            )
            for model_row in ranked_models:
                rel_alloc = _safe_float(model_row.get("allocation_fraction"), 0.0)
                model_row["effective_allocation_fraction"] = float(rel_alloc * group_budget_fraction)
                model_row["group_budget_fraction"] = float(group_budget_fraction)
                model_row["group_risk_limit_multiplier"] = float(group_risk_limit_multiplier)

            champion = ranked_models[0]
            champion_share = _safe_float(champion.get("allocation_fraction"), 0.0)
            champion_effective_share = _safe_float(champion.get("effective_allocation_fraction"), 0.0)
            allocations[group_key] = {
                "symbol": str(candidates[0].get("symbol") or ""),
                "horizon_s": _safe_int(candidates[0].get("horizon_s"), 0),
                "regime": str(candidates[0].get("regime") or "global"),
                "horizon_bucket": _horizon_bucket(_safe_int(candidates[0].get("horizon_s"), 0)),
                "champion_model_name": str(champion.get("model_name") or ""),
                "allocation_strategy": str(allocation_strategy),
                "capital_multiplier": float(champion_effective_share),
                "risk_limit_multiplier": float(group_risk_limit_multiplier),
                "group_risk_limit_multiplier": float(group_risk_limit_multiplier),
                "group_budget_fraction": float(group_budget_fraction),
                "group_budget_fraction_unscaled": float(group_budget_fraction),
                "group_weight_total": float(sum(float(max(0.0, _safe_float(weight, 0.0))) for _row, weight in weighted)),
                "models": ranked_models,
                "champion_share": float(champion_share),
            }

        total_capital_fraction = float(_clamp(float(COMPETITION_TOTAL_CAPITAL_FRACTION), 0.0, 1.0))
        total_group_budget_fraction_pre = sum(
            _safe_float((alloc or {}).get("group_budget_fraction"), 0.0)
            for alloc in allocations.values()
            if isinstance(alloc, dict)
        )
        global_budget_scale = 1.0
        if total_capital_fraction <= 0.0:
            global_budget_scale = 0.0
        elif total_group_budget_fraction_pre > total_capital_fraction and total_group_budget_fraction_pre > 1e-12:
            global_budget_scale = float(total_capital_fraction) / float(total_group_budget_fraction_pre)

        for alloc in allocations.values():
            if not isinstance(alloc, dict):
                continue
            models = list(alloc.get("models") or [])
            raw_group_budget_fraction = _safe_float(
                alloc.get("group_budget_fraction_unscaled"),
                _safe_float(alloc.get("group_budget_fraction"), 0.0),
            )
            scaled_group_budget_fraction = float(raw_group_budget_fraction) * float(global_budget_scale)
            alloc["group_budget_fraction_unscaled"] = float(raw_group_budget_fraction)
            alloc["group_budget_fraction"] = float(scaled_group_budget_fraction)
            alloc["competition_total_capital_fraction"] = float(total_capital_fraction)
            alloc["global_budget_scale"] = float(global_budget_scale)
            for model_row in models:
                rel_alloc = _safe_float((model_row or {}).get("allocation_fraction"), 0.0)
                model_row["group_budget_fraction_unscaled"] = float(raw_group_budget_fraction)
                model_row["group_budget_fraction"] = float(scaled_group_budget_fraction)
                model_row["competition_total_capital_fraction"] = float(total_capital_fraction)
                model_row["global_budget_scale"] = float(global_budget_scale)
                model_row["effective_allocation_fraction"] = float(rel_alloc) * float(scaled_group_budget_fraction)
            alloc["models"] = models

        model_totals: Dict[str, float] = {}
        for alloc in allocations.values():
            for model_row in list((alloc or {}).get("models") or []):
                model_name = str((model_row or {}).get("model_name") or "").strip()
                if not model_name:
                    continue
                model_totals[model_name] = model_totals.get(model_name, 0.0) + _safe_float(
                    (model_row or {}).get("effective_allocation_fraction"),
                    0.0,
                )

        for alloc in allocations.values():
            models = list(alloc.get("models") or [])
            if not models:
                continue
            scaled = False
            for model_row in models:
                model_name = str((model_row or {}).get("model_name") or "").strip()
                total_share = _safe_float(model_totals.get(model_name), 0.0)
                if total_share <= float(CAPITAL_MAX_MODEL_GLOBAL_SHARE) or total_share <= 0.0:
                    continue
                scale = float(CAPITAL_MAX_MODEL_GLOBAL_SHARE) / float(total_share)
                model_row["effective_allocation_fraction"] = _safe_float(
                    model_row.get("effective_allocation_fraction"),
                    0.0,
                ) * float(scale)
                model_row["global_model_share"] = float(total_share)
                model_row["global_model_share_cap"] = float(CAPITAL_MAX_MODEL_GLOBAL_SHARE)
                scaled = True
            models.sort(
                key=lambda x: (
                    -_safe_float(x.get("effective_allocation_fraction"), _safe_float(x.get("allocation_fraction"), 0.0)),
                    -_safe_float(x.get("score"), 0.0),
                    str(x.get("model_name") or ""),
                )
            )
            alloc["models"] = models
            alloc["champion_model_name"] = str((models[0] or {}).get("model_name") or "")
            champion_effective_share = _safe_float(
                (models[0] or {}).get("effective_allocation_fraction"),
                0.0,
            )
            alloc["capital_multiplier"] = float(champion_effective_share)
            alloc["risk_limit_multiplier"] = float(
                _clamp(
                    min(
                        _safe_float(alloc.get("group_risk_limit_multiplier"), 1.0),
                        _safe_float((models[0] or {}).get("model_risk_limit_multiplier"), 1.0),
                    ),
                    float(CAPITAL_MODEL_RISK_MULT_MIN),
                    float(CAPITAL_MODEL_RISK_MULT_MAX),
                )
            )
            alloc["group_budget_fraction"] = float(
                min(1.0, max(0.0, sum(_safe_float(m.get("effective_allocation_fraction"), 0.0) for m in models)))
            )
            if scaled:
                alloc["concentration_scaled"] = True

        out = {
            "ok": True,
            "allocation_strategy": str(allocation_strategy),
            "allocations": allocations,
            "competition_total_capital_fraction": float(total_capital_fraction),
            "total_group_budget_fraction_pre": float(total_group_budget_fraction_pre),
            "total_group_budget_fraction_post": float(
                sum(
                    _safe_float((alloc or {}).get("group_budget_fraction"), 0.0)
                    for alloc in allocations.values()
                    if isinstance(alloc, dict)
                )
            ),
            "global_budget_scale": float(global_budget_scale),
            "updated_ts_ms": _now_ms(),
        }
        meta_set("competition_capital_plan", json.dumps(out, separators=(",", ":"), sort_keys=True))
        return out
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("MODEL_MARKETPLACE_CAPITAL_PLAN_CLOSE_FAILED", e)


def recompute_marketplace_scores() -> Dict[str, Any]:
    init_db()
    now = _now_ms()
    window_ms = int(max(1, MODEL_COMPETITION_WINDOW_S) * 1000)
    since_ms = int(now - window_ms)
    window_mid_ms = int(since_ms + (window_ms // 2))
    try:
        from engine.execution.execution_ledger import compute_pnl_attribution_snapshot
    except Exception:
        compute_pnl_attribution_snapshot = None  # type: ignore
    try:
        from engine.execution.trade_attribution_ledger import (
            upsert_from_latest_pnl_attribution_snapshot,
        )
    except Exception:
        upsert_from_latest_pnl_attribution_snapshot = None  # type: ignore

    if callable(compute_pnl_attribution_snapshot):
        try:
            compute_pnl_attribution_snapshot(lookback_orders=5000)
        except Exception as e:
            _warn_nonfatal(
                "MODEL_MARKETPLACE_PNL_ATTRIBUTION_SNAPSHOT_FAILED",
                e,
                once_key="pnl_attribution_snapshot",
            )
    if callable(upsert_from_latest_pnl_attribution_snapshot):
        try:
            upsert_from_latest_pnl_attribution_snapshot()
        except Exception as e:
            _warn_nonfatal(
                "MODEL_MARKETPLACE_TRADE_ATTRIBUTION_LEDGER_REFRESH_FAILED",
                e,
                once_key="trade_attribution_ledger_refresh",
            )

    con = connect(readonly=True)
    try:

        champion_keys = set()
        try:
            rows = con.execute(
                """
                SELECT symbol, horizon_s, regime, model_name, meta_json
                FROM champion_assignments
                WHERE state='champion'
                """
            ).fetchall()
            for sym, horizon_s, regime, model_name, meta_json in rows or []:
                meta = _safe_json_dict(meta_json)
                champion_model_id = _normalize_model_id(meta.get("model_id"))
                champion_keys.add(
                    (
                        champion_model_id,
                        str(model_name or "").strip(),
                        str(sym or "").upper().strip(),
                        _safe_int(horizon_s, 0),
                        str(regime or "global").strip() or "global",
                    )
                )
        except Exception:
            champion_keys = set()

        try:
            rows = con.execute(
                """
                SELECT
                  p.ts_ms,
                  p.source_alert_id,
                  p.model_id,
                  p.model_version,
                  p.symbol,
                  p.pnl,
                  p.realized_pnl,
                  p.unrealized_pnl,
                  p.fees,
                  p.position_size,
                  p.avg_price,
                  p.extra_json,
                  (
                    SELECT MIN(eo.submit_ts_ms)
                    FROM execution_orders eo
                    WHERE eo.source_alert_id = p.source_alert_id
                      AND COALESCE(NULLIF(TRIM(eo.model_id), ''), 'baseline') = COALESCE(NULLIF(TRIM(p.model_id), ''), 'baseline')
                      AND UPPER(TRIM(eo.symbol)) = UPPER(TRIM(p.symbol))
                  ) AS submit_ts_ms,
                  (
                    SELECT eo.extra_json
                    FROM execution_orders eo
                    WHERE eo.source_alert_id = p.source_alert_id
                      AND COALESCE(NULLIF(TRIM(eo.model_id), ''), 'baseline') = COALESCE(NULLIF(TRIM(p.model_id), ''), 'baseline')
                      AND UPPER(TRIM(eo.symbol)) = UPPER(TRIM(p.symbol))
                    ORDER BY eo.submit_ts_ms DESC, eo.client_order_id DESC
                    LIMIT 1
                  ) AS order_extra_json,
                  (
                    SELECT a.horizon_s
                    FROM alerts a
                    WHERE a.id = p.source_alert_id
                    LIMIT 1
                  ) AS alert_horizon_s,
                  (
                    SELECT a.confidence
                    FROM alerts a
                    WHERE a.id = p.source_alert_id
                    LIMIT 1
                  ) AS alert_confidence,
                  (
                    SELECT a.explain_json
                    FROM alerts a
                    WHERE a.id = p.source_alert_id
                    LIMIT 1
                  ) AS alert_explain_json
                FROM pnl_attribution p
                WHERE p.ts_ms = (SELECT MAX(ts_ms) FROM pnl_attribution)
                ORDER BY COALESCE(
                  (
                    SELECT MIN(eo.submit_ts_ms)
                    FROM execution_orders eo
                    WHERE eo.source_alert_id = p.source_alert_id
                      AND COALESCE(NULLIF(TRIM(eo.model_id), ''), 'baseline') = COALESCE(NULLIF(TRIM(p.model_id), ''), 'baseline')
                      AND UPPER(TRIM(eo.symbol)) = UPPER(TRIM(p.symbol))
                  ),
                  p.ts_ms
                ) ASC
                """
            ).fetchall()
        except Exception:
            rows = []

        trade_attribution_rows = _load_latest_trade_attribution_rows(con)
        agg: Dict[Tuple[str, str, str, int, str], Dict[str, Any]] = {}
        for (
            attribution_ts_ms,
            source_alert_id,
            attribution_model_id,
            attribution_model_version,
            symbol,
            pnl,
            realized_pnl,
            unrealized_pnl,
            fees,
            position_size,
            avg_price,
            attribution_extra_json_raw,
            submit_ts_ms,
            order_extra_json_raw,
            alert_horizon_s,
            alert_confidence,
            alert_explain_raw,
        ) in rows or []:
            sym = str(symbol or "").upper().strip()
            if not sym:
                continue
            attribution_key = (
                int(source_alert_id) if source_alert_id is not None else -1,
                _normalize_model_id(attribution_model_id),
                sym,
            )
            trade_attr = dict(trade_attribution_rows.get(attribution_key) or {})
            signal_ts_ms = _safe_int(submit_ts_ms, 0)
            if signal_ts_ms <= 0:
                signal_ts_ms = _safe_int(attribution_ts_ms, 0)

            order_extra_json = _safe_json_dict(order_extra_json_raw)
            attribution_extra = _safe_json_dict(attribution_extra_json_raw)
            alert_explain = _safe_json_dict(alert_explain_raw)
            trade_signal_json = _safe_json_dict(trade_attr.get("signal_json"))
            trade_model_json = _safe_json_dict(trade_attr.get("model_json"))
            trade_regime_json = _safe_json_dict(trade_attr.get("regime_vector_json"))
            trade_decision_json = _safe_json_dict(trade_attr.get("decision_json"))

            signal_json: Dict[str, Any] = {
                "source_alert_id": (
                    int(source_alert_id) if source_alert_id is not None else None
                ),
                "horizon_s": _safe_int(
                    alert_horizon_s
                    if alert_horizon_s is not None
                    else order_extra_json.get("horizon_s"),
                    0,
                ),
                "confidence": _safe_float(
                    alert_confidence
                    if alert_confidence is not None
                    else order_extra_json.get("confidence"),
                    0.0,
                ),
            }
            if trade_signal_json:
                signal_json.update(trade_signal_json)
            if alert_explain:
                signal_json["alert_explain"] = alert_explain

            model_json = dict(order_extra_json)
            if trade_model_json:
                model_json.update(trade_model_json)
            if attribution_model_id not in (None, ""):
                model_json.setdefault("model_id", _normalize_model_id(attribution_model_id))
                signal_json.setdefault("model_id", _normalize_model_id(attribution_model_id))
            decision_json = dict(alert_explain or {})
            if trade_decision_json:
                decision_json.update(trade_decision_json)

            name = _extract_model_name(model_json, signal_json, decision_json)
            model_id = _normalize_model_id(
                attribution_model_id
                if attribution_model_id not in (None, "")
                else (
                    trade_model_json.get("model_id")
                    or _extract_model_id(model_json, signal_json)
                )
            )
            horizon_s = _extract_horizon_s(model_json, signal_json)
            regime = _extract_regime(
                trade_regime_json if trade_regime_json else alert_explain,
                signal_json,
            )
            confidence = _safe_float(signal_json.get("confidence"), 0.0)

            key = (_normalize_model_id(model_id), name, sym, int(horizon_s), regime)
            cur = agg.get(key)
            if cur is None:
                cur = _default_marketplace_row(
                    model_id=model_id,
                    model_name=name,
                    symbol=sym,
                    horizon_s=int(horizon_s),
                    regime=regime,
                    stage=("champion" if key in champion_keys else "challenger"),
                )
                cur["meta"]["score_source"] = "pnl_attribution"
                agg[key] = cur

            total_pnl = _safe_float(
                pnl,
                _safe_float(
                    attribution_extra.get("total_pnl"),
                    _safe_float(realized_pnl, 0.0)
                    + _safe_float(unrealized_pnl, 0.0)
                    - _safe_float(fees, 0.0)
                    - _safe_float(attribution_extra.get("slippage_cost"), 0.0),
                ),
            )
            fee_cost = _safe_float(fees, 0.0)
            total_cost = _safe_float(
                attribution_extra.get("total_cost"),
                fee_cost + _safe_float(attribution_extra.get("slippage_cost"), 0.0),
            )
            realized_trade_pnls = [
                _safe_float(v, 0.0)
                for v in list(attribution_extra.get("realized_trade_pnls") or [])
            ]
            position_size_f = _safe_float(position_size, 0.0)
            unrealized_pnl_f = _safe_float(unrealized_pnl, 0.0)
            open_position_pnl = float(total_pnl) - float(sum(realized_trade_pnls))
            row_event_pnls = list(realized_trade_pnls)
            if abs(float(position_size_f)) > 1e-12 or abs(float(unrealized_pnl_f)) > 1e-12:
                row_event_pnls.append(float(open_position_pnl))
            elif not row_event_pnls:
                row_event_pnls.append(float(total_pnl))
            if signal_ts_ms < int(since_ms) and not (
                abs(float(position_size_f)) > 1e-12 or abs(float(unrealized_pnl_f)) > 1e-12
            ):
                continue

            if source_alert_id is not None:
                cur["source_alert_ids"].add(int(source_alert_id))
            cur["last_signal_ts_ms"] = max(
                _safe_int(cur.get("last_signal_ts_ms"), 0),
                int(signal_ts_ms),
            )
            prev_trades = _safe_int(cur.get("trades"), 0)
            cur["avg_confidence"] = _avg_confidence(
                _safe_float(cur.get("avg_confidence"), 0.0),
                prev_trades,
                float(confidence),
            )
            cur["trades"] = prev_trades + int(len(row_event_pnls))
            cur["wins"] = _safe_int(cur.get("wins"), 0) + int(
                sum(1 for pnl_value in row_event_pnls if float(pnl_value) > 0.0)
            )
            cur["losses"] = _safe_int(cur.get("losses"), 0) + int(
                sum(1 for pnl_value in row_event_pnls if float(pnl_value) < 0.0)
            )

            meta = cur.setdefault("meta", {})
            meta["realized_pnl"] = _safe_float(meta.get("realized_pnl"), 0.0) + _safe_float(realized_pnl, 0.0)
            meta["unrealized_pnl"] = _safe_float(meta.get("unrealized_pnl"), 0.0) + _safe_float(unrealized_pnl, 0.0)
            meta["fee_cost"] = _safe_float(meta.get("fee_cost"), 0.0) + float(fee_cost)
            meta["transaction_cost"] = _safe_float(meta.get("transaction_cost"), 0.0) + float(total_cost)
            meta["position_size"] = _safe_float(meta.get("position_size"), 0.0) + _safe_float(position_size, 0.0)
            meta["open_qty"] = _safe_float(meta.get("open_qty"), 0.0) + _safe_float(position_size, 0.0)
            meta["avg_price"] = (
                _safe_float(avg_price, 0.0)
                if _safe_float(avg_price, 0.0) > 0.0
                else meta.get("avg_price")
            )
            meta["entry_price"] = (
                _safe_float(avg_price, 0.0)
                if _safe_float(avg_price, 0.0) > 0.0 and meta.get("entry_price") in (None, 0, 0.0)
                else meta.get("entry_price")
            )
            meta["last_fill_price"] = (
                _safe_float((attribution_extra.get("execution_quality") or {}).get("fill_price"), 0.0)
                if isinstance(attribution_extra.get("execution_quality"), dict)
                and _safe_float((attribution_extra.get("execution_quality") or {}).get("fill_price"), 0.0) > 0.0
                else meta.get("last_fill_price")
            )
            meta["last_price"] = (
                _safe_float(attribution_extra.get("last_px"), 0.0)
                if _safe_float(attribution_extra.get("last_px"), 0.0) > 0.0
                else meta.get("last_price")
            )
            meta["notional_traded"] = _safe_float(meta.get("notional_traded"), 0.0) + _safe_float(
                attribution_extra.get("notional_traded"), 0.0
            )
            meta["score_source"] = "pnl_attribution"
            net_cost_evidence = _net_cost_evidence_for_signal(
                con,
                model_id=str(model_id),
                model_name=str(name),
                symbol=str(sym),
                horizon_s=int(horizon_s),
                source_alert_id=(int(source_alert_id) if source_alert_id is not None else None),
            )
            meta["net_cost_evidence"] = dict(net_cost_evidence)
            meta["net_cost_label_count"] = _safe_int(net_cost_evidence.get("n"), 0)
            if attribution_model_version not in (None, ""):
                meta["model_version"] = str(attribution_model_version)
            model_kind = _extract_model_kind(model_json, signal_json)
            if model_kind:
                meta["model_kind"] = str(model_kind)
            model_ts_ms = _extract_model_ts_ms(model_json, signal_json)
            if model_ts_ms is not None:
                meta["model_ts_ms"] = int(model_ts_ms)
            meta["realized_trade_count"] = _safe_int(meta.get("realized_trade_count"), 0) + int(
                len(realized_trade_pnls)
            )
            meta["realized_trade_pnls"] = list(meta.get("realized_trade_pnls") or []) + list(
                realized_trade_pnls
            )
            cur.setdefault("event_pnls", []).extend(float(v) for v in row_event_pnls)

            _update_row_tracking(
                cur,
                ts_ms=int(signal_ts_ms),
                pnl_delta=float(sum(float(v) for v in row_event_pnls)),
                window_mid_ms=int(window_mid_ms),
            )

        shadow_rows = int(
            _accumulate_shadow_prediction_scores(
                con,
                agg,
                champion_keys,
                since_ms=int(since_ms),
                window_mid_ms=int(window_mid_ms),
            )
        )

        rows_to_write = list(agg.values())
        for cur in rows_to_write:
            cur.setdefault("meta", {})["window_start_ts_ms"] = int(since_ms)
            cur.setdefault("meta", {})["window_end_ts_ms"] = int(now)

        def _rewrite_scores(db) -> int:
            CompetitionRepository(db).delete_all_marketplace_scores()
            written_local = 0
            for cur in rows_to_write:
                _write_marketplace_row(db, cur, now=now)
                written_local += 1
            return int(written_local)

        written = int(
            run_write_txn(
                _rewrite_scores,
                table="model_marketplace_scores",
                operation="recompute_marketplace_scores",
                context={"row_count": len(rows_to_write)},
            )
            or 0
        )
        try:
            publish_marketplace_snapshot()
        except Exception as e:
            _warn_nonfatal(
                "MODEL_MARKETPLACE_PUBLISH_SNAPSHOT_FAILED",
                e,
                once_key="publish_marketplace_snapshot",
            )
        return {
            "ok": True,
            "rows_written": int(written),
            "shadow_predictions_scored": int(shadow_rows),
            "updated_ts_ms": int(now),
        }
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("MODEL_MARKETPLACE_RECOMPUTE_SCORES_CLOSE_FAILED", e)


def top_challengers(limit: int = 10) -> List[Dict[str, Any]]:
    con = connect()
    try:
        rows = con.execute(
            """
            SELECT model_name, symbol, horizon_s, regime, stage, score, trades, wins, losses, net_pnl, avg_confidence, updated_ts_ms, meta_json
                 , model_id
            FROM model_marketplace_scores
            WHERE stage IN ('challenger', 'champion')
            ORDER BY score DESC, updated_ts_ms DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()

        out: List[Dict[str, Any]] = []
        for r in rows or []:
            meta = _safe_json_dict(r[12])
            if not _score_source_is_competition_candidate(meta):
                continue
            out.append(
                {
                    "model_name": str(r[0] or ""),
                    "model_id": _normalize_model_id(r[13]),
                    "symbol": str(r[1] or ""),
                    "horizon_s": int(r[2] or 0),
                    "regime": str(r[3] or "global"),
                    "stage": str(r[4] or "challenger"),
                    "score": _safe_float(r[5], 0.0),
                    "trades": _safe_int(r[6], 0),
                    "wins": _safe_int(r[7], 0),
                    "losses": _safe_int(r[8], 0),
                    "net_pnl": _safe_float(r[9], 0.0),
                    "avg_confidence": _safe_float(r[10], 0.0),
                    "updated_ts_ms": _safe_int(r[11], 0),
                    "meta": meta,
                }
            )
        return out
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("MODEL_MARKETPLACE_PUBLISH_SNAPSHOT_CLOSE_FAILED", e)


def publish_marketplace_snapshot(
    active_symbols: Optional[List[str]] = None,
) -> Dict[str, Any]:
    # This snapshot is intentionally compact. It gives operators the current
    # headline competition view without exposing every intermediate table.
    snap = {
        "ok": True,
        "champion": {},
        "challengers": top_challengers(limit=10),
        "capital_plan": compute_capital_plan(),
        "active_symbols": list(active_symbols or []),
        "updated_ts_ms": _now_ms(),
    }
    meta_set(
        "competition_runtime", json.dumps(snap, separators=(",", ":"), sort_keys=True)
    )
    return snap
