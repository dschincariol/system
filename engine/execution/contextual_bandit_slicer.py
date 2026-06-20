"""Execution-only contextual bandit for bounded order slicing.

The policy in this module is intentionally narrow. It never emits symbols,
sides, target weights, broker choices, capital sizes, or order quantities. It
only chooses bounded execution parameters that an upstream execution policy has
already allowed.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import math
import os
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple


POLICY_NAME = "contextual_bandit_execution_slicer_v1"
POLICY_SCOPE = "execution_only"

ALLOWED_PARAMETER_FIELDS = frozenset(
    {
        "slice_pct",
        "target_participation",
        "slice_interval_ms",
        "entry_delay_ms",
    }
)

FORBIDDEN_POLICY_FIELDS = frozenset(
    {
        "asset",
        "broker",
        "capital",
        "capital_limit",
        "direction",
        "gross_exposure",
        "max_notional",
        "net_exposure",
        "notional",
        "order_notional",
        "portfolio_size",
        "qty",
        "quantity",
        "side",
        "symbol",
        "target_weight",
        "to_side",
        "to_weight",
        "weight",
    }
)

_TRUTHY = {"1", "true", "t", "yes", "y", "on"}
_FALSEY = {"0", "false", "f", "no", "n", "off", ""}


class LearnedExecutionPolicyViolation(ValueError):
    """Raised when a learned policy attempts to leave execution-only bounds."""


@dataclass(frozen=True)
class BanditAction:
    action_id: str
    slice_pct_mult: float
    participation_mult: float
    interval_mult: float
    delay_ms_delta: int
    stress_tilt: float
    adverse_tilt: float
    fill_risk_penalty: float
    prior_reward: float = 0.0


@dataclass(frozen=True)
class ExecutionSlicingConstraints:
    symbol: str
    side: str
    parent_qty: float
    parent_id: str
    base_slice_pct: float
    min_slice_pct: float
    max_slice_pct: float
    base_participation: float
    min_participation: float
    max_participation: float
    base_slice_interval_ms: int
    min_slice_interval_ms: int
    max_slice_interval_ms: int
    base_entry_delay_ms: int
    min_entry_delay_ms: int
    max_entry_delay_ms: int
    max_slices: int

    def as_guard(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "side": self.side,
            "parent_qty": float(self.parent_qty),
            "parent_id": self.parent_id,
            "base_slice_pct": float(self.base_slice_pct),
            "min_slice_pct": float(self.min_slice_pct),
            "max_slice_pct": float(self.max_slice_pct),
            "base_participation": float(self.base_participation),
            "min_participation": float(self.min_participation),
            "max_participation": float(self.max_participation),
            "base_slice_interval_ms": int(self.base_slice_interval_ms),
            "min_slice_interval_ms": int(self.min_slice_interval_ms),
            "max_slice_interval_ms": int(self.max_slice_interval_ms),
            "base_entry_delay_ms": int(self.base_entry_delay_ms),
            "min_entry_delay_ms": int(self.min_entry_delay_ms),
            "max_entry_delay_ms": int(self.max_entry_delay_ms),
            "max_slices": int(self.max_slices),
        }


@dataclass(frozen=True)
class LearnedExecutionDecision:
    policy_name: str
    policy_scope: str
    action_id: str
    parameters: Dict[str, float | int]
    scores: Dict[str, float]
    context: Dict[str, float]

    def as_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["allowed_parameter_fields"] = sorted(ALLOWED_PARAMETER_FIELDS)
        return out


DEFAULT_ACTIONS: Tuple[BanditAction, ...] = (
    BanditAction(
        action_id="baseline",
        slice_pct_mult=1.0,
        participation_mult=1.0,
        interval_mult=1.0,
        delay_ms_delta=0,
        stress_tilt=0.0,
        adverse_tilt=0.0,
        fill_risk_penalty=0.0,
        prior_reward=0.0,
    ),
    BanditAction(
        action_id="steady",
        slice_pct_mult=0.85,
        participation_mult=0.9,
        interval_mult=1.15,
        delay_ms_delta=75,
        stress_tilt=0.10,
        adverse_tilt=0.05,
        fill_risk_penalty=0.05,
        prior_reward=-0.005,
    ),
    BanditAction(
        action_id="patient",
        slice_pct_mult=0.65,
        participation_mult=0.75,
        interval_mult=1.45,
        delay_ms_delta=175,
        stress_tilt=0.22,
        adverse_tilt=0.16,
        fill_risk_penalty=0.12,
        prior_reward=-0.015,
    ),
    BanditAction(
        action_id="defensive",
        slice_pct_mult=0.45,
        participation_mult=0.55,
        interval_mult=1.85,
        delay_ms_delta=325,
        stress_tilt=0.35,
        adverse_tilt=0.28,
        fill_risk_penalty=0.28,
        prior_reward=-0.030,
    ),
)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        if not math.isfinite(out):
            return float(default)
        return float(out)
    except Exception:
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _clamp(value: float, lower: float, upper: float) -> float:
    lo = min(float(lower), float(upper))
    hi = max(float(lower), float(upper))
    return max(lo, min(hi, float(value)))


def _truthy(value: Any) -> Optional[bool]:
    if value is None:
        return None
    raw = str(value).strip().lower()
    if raw in _TRUTHY:
        return True
    if raw in _FALSEY:
        return False
    return None


def learned_execution_enabled(order: Mapping[str, Any] | None = None) -> bool:
    """Return whether the learned slicer should run for this order."""
    order_obj = dict(order or {})
    for key in (
        "learned_execution_slicing",
        "contextual_bandit_execution",
        "learned_execution_policy_enabled",
    ):
        parsed = _truthy(order_obj.get(key))
        if parsed is not None:
            return bool(parsed)

    parsed_env = _truthy(os.environ.get("LEARNED_EXECUTION_SLICING_ENABLED"))
    return bool(parsed_env) if parsed_env is not None else False


def normalize_side(value: Any) -> str:
    side = str(value or "").strip().upper()
    if side in {"BUY", "LONG"}:
        return "LONG"
    if side in {"SELL", "SHORT"}:
        return "SHORT"
    return side


def build_constraints(
    *,
    order: Mapping[str, Any],
    symbol: str,
    side: str,
    parent_qty: float,
    parent_id: str,
    base_slice_pct: float,
    base_participation: float,
    base_slice_interval_ms: int,
    base_entry_delay_ms: int,
    max_slices: int,
) -> ExecutionSlicingConstraints:
    """Create execution-only bounds from an already approved policy output."""
    base_slice_pct_f = _clamp(_safe_float(base_slice_pct, 0.20), 0.001, 1.0)
    base_participation_f = _clamp(_safe_float(base_participation, 0.03), 0.0001, 1.0)
    base_interval_i = max(0, _safe_int(base_slice_interval_ms, 250))
    base_delay_i = max(0, _safe_int(base_entry_delay_ms, 0))

    min_slice_floor = _clamp(
        _safe_float(os.environ.get("LEARNED_EXEC_MIN_SLICE_PCT", "0.02"), 0.02),
        0.001,
        base_slice_pct_f,
    )
    min_participation_floor = _clamp(
        _safe_float(os.environ.get("LEARNED_EXEC_MIN_PARTICIPATION", "0.005"), 0.005),
        0.0001,
        base_participation_f,
    )
    max_delay_add_ms = max(
        0,
        _safe_int(order.get("learned_execution_max_delay_add_ms"), _safe_int(os.environ.get("LEARNED_EXEC_MAX_DELAY_ADD_MS", "500"), 500)),
    )
    interval_mult_cap = max(
        1.0,
        _safe_float(
            order.get("learned_execution_max_interval_mult"),
            _safe_float(os.environ.get("LEARNED_EXEC_MAX_INTERVAL_MULT", "4.0"), 4.0),
        ),
    )

    return ExecutionSlicingConstraints(
        symbol=str(symbol or "").upper().strip(),
        side=normalize_side(side),
        parent_qty=float(parent_qty),
        parent_id=str(parent_id or ""),
        base_slice_pct=float(base_slice_pct_f),
        min_slice_pct=float(min_slice_floor),
        max_slice_pct=float(base_slice_pct_f),
        base_participation=float(base_participation_f),
        min_participation=float(min_participation_floor),
        max_participation=float(base_participation_f),
        base_slice_interval_ms=int(base_interval_i),
        min_slice_interval_ms=int(base_interval_i),
        max_slice_interval_ms=int(round(float(base_interval_i) * float(interval_mult_cap))),
        base_entry_delay_ms=int(base_delay_i),
        min_entry_delay_ms=int(base_delay_i),
        max_entry_delay_ms=int(base_delay_i + max_delay_add_ms),
        max_slices=max(1, _safe_int(max_slices, 1)),
    )


def build_context(
    *,
    order: Mapping[str, Any],
    feedback: Mapping[str, Any] | None = None,
    execution_decision: Mapping[str, Any] | None = None,
    extra: Mapping[str, Any] | None = None,
) -> Dict[str, float]:
    """Normalize execution-only context features for bandit scoring."""
    feedback_obj = dict(feedback or {})
    decision_obj = dict(execution_decision or {})
    extra_obj = dict(extra or {})

    spread_bps = max(
        _safe_float(order.get("true_spread_bps"), 0.0),
        _safe_float(order.get("spread_bps"), 0.0),
        _safe_float(feedback_obj.get("spread_bps"), 0.0),
    )
    volatility_bps = max(
        _safe_float(order.get("intraday_vol_bps"), 0.0),
        abs(_safe_float(order.get("volatility"), 0.0)) * 10000.0,
        _safe_float(feedback_obj.get("volatility_bps"), 0.0),
    )
    slippage_bps = max(
        0.0,
        _safe_float(order.get("slippage_bps"), 0.0),
        _safe_float(feedback_obj.get("slippage_bps"), 0.0),
        _safe_float(feedback_obj.get("slippage_bps_weighted"), 0.0),
        _safe_float(decision_obj.get("expected_slippage_bps"), 0.0),
    )
    adverse_selection_bps = max(
        0.0,
        _safe_float(order.get("adverse_selection_bps"), 0.0),
        _safe_float(feedback_obj.get("adverse_selection_bps"), 0.0),
        _safe_float(extra_obj.get("adverse_selection_bps"), 0.0),
    )
    fill_risk = _clamp(
        max(
            _safe_float(order.get("fill_risk"), 0.0),
            _safe_float(feedback_obj.get("fill_risk"), 0.0),
            _safe_float(feedback_obj.get("reject_rate"), 0.0),
            _safe_float(extra_obj.get("fill_risk"), 0.0),
        ),
        0.0,
        1.0,
    )
    alpha_remaining = _clamp(
        _safe_float(order.get("epe_alpha_remaining"), _safe_float(extra_obj.get("alpha_remaining"), 1.0)),
        0.0,
        1.0,
    )

    return {
        "spread_bps": float(spread_bps),
        "volatility_bps": float(volatility_bps),
        "slippage_bps": float(slippage_bps),
        "adverse_selection_bps": float(adverse_selection_bps),
        "fill_risk": float(fill_risk),
        "alpha_remaining": float(alpha_remaining),
        "stress": _execution_stress(
            spread_bps=spread_bps,
            volatility_bps=volatility_bps,
            slippage_bps=slippage_bps,
            adverse_selection_bps=adverse_selection_bps,
            fill_risk=fill_risk,
        ),
    }


def _execution_stress(
    *,
    spread_bps: float,
    volatility_bps: float,
    slippage_bps: float,
    adverse_selection_bps: float,
    fill_risk: float,
) -> float:
    parts = [
        _clamp(float(spread_bps) / 25.0, 0.0, 1.0),
        _clamp(float(volatility_bps) / 75.0, 0.0, 1.0),
        _clamp(float(slippage_bps) / 20.0, 0.0, 1.0),
        _clamp(float(adverse_selection_bps) / 20.0, 0.0, 1.0),
        _clamp(float(fill_risk), 0.0, 1.0),
    ]
    return float(sum(parts) / float(len(parts)))


class ContextualBanditExecutionPolicy:
    """Small deterministic contextual bandit over execution-only actions."""

    def __init__(
        self,
        *,
        actions: Iterable[BanditAction] = DEFAULT_ACTIONS,
        action_values: Mapping[str, float] | None = None,
    ) -> None:
        self.actions = tuple(actions)
        self.action_values = {str(k): float(v) for k, v in dict(action_values or {}).items()}
        if not self.actions:
            raise ValueError("at least one bandit action is required")

    def select_action(self, context: Mapping[str, float]) -> Tuple[BanditAction, Dict[str, float]]:
        stress = _clamp(_safe_float(context.get("stress"), 0.0), 0.0, 1.0)
        adverse = _clamp(_safe_float(context.get("adverse_selection_bps"), 0.0) / 20.0, 0.0, 1.0)
        fill_risk = _clamp(_safe_float(context.get("fill_risk"), 0.0), 0.0, 1.0)
        alpha_remaining = _clamp(_safe_float(context.get("alpha_remaining"), 1.0), 0.0, 1.0)

        scores: Dict[str, float] = {}
        for action in self.actions:
            urgency_penalty = (1.0 - alpha_remaining) * max(0.0, 1.0 - float(action.slice_pct_mult))
            score = (
                float(action.prior_reward)
                + float(self.action_values.get(action.action_id, 0.0))
                + (float(action.stress_tilt) * stress)
                + (float(action.adverse_tilt) * adverse)
                - (float(action.fill_risk_penalty) * fill_risk)
                - float(urgency_penalty)
            )
            scores[action.action_id] = float(score)

        best = max(self.actions, key=lambda action: (scores.get(action.action_id, float("-inf")), action.action_id))
        return best, scores


def select_execution_adjustment(
    *,
    context: Mapping[str, float],
    constraints: ExecutionSlicingConstraints,
    policy: ContextualBanditExecutionPolicy | None = None,
) -> LearnedExecutionDecision:
    active_policy = policy or ContextualBanditExecutionPolicy()
    action, scores = active_policy.select_action(context)
    params = {
        "slice_pct": _clamp(
            float(constraints.base_slice_pct) * float(action.slice_pct_mult),
            constraints.min_slice_pct,
            constraints.max_slice_pct,
        ),
        "target_participation": _clamp(
            float(constraints.base_participation) * float(action.participation_mult),
            constraints.min_participation,
            constraints.max_participation,
        ),
        "slice_interval_ms": int(
            round(
                _clamp(
                    float(constraints.base_slice_interval_ms) * float(action.interval_mult),
                    constraints.min_slice_interval_ms,
                    constraints.max_slice_interval_ms,
                )
            )
        ),
        "entry_delay_ms": int(
            round(
                _clamp(
                    float(constraints.base_entry_delay_ms) + float(action.delay_ms_delta),
                    constraints.min_entry_delay_ms,
                    constraints.max_entry_delay_ms,
                )
            )
        ),
    }
    decision = LearnedExecutionDecision(
        policy_name=POLICY_NAME,
        policy_scope=POLICY_SCOPE,
        action_id=str(action.action_id),
        parameters=params,
        scores={str(k): float(v) for k, v in scores.items()},
        context={str(k): float(v) for k, v in dict(context or {}).items()},
    )
    verdict = enforce_execution_only_decision(decision=decision, constraints=constraints)
    if not bool(verdict.get("ok")):
        raise LearnedExecutionPolicyViolation(str(verdict.get("reason") or "learned_execution_policy_violation"))
    return decision


def enforce_execution_only_decision(
    *,
    decision: LearnedExecutionDecision | Mapping[str, Any],
    constraints: ExecutionSlicingConstraints | Mapping[str, Any],
) -> Dict[str, Any]:
    decision_obj = decision.as_dict() if isinstance(decision, LearnedExecutionDecision) else dict(decision or {})
    constraints_obj = constraints.as_guard() if isinstance(constraints, ExecutionSlicingConstraints) else dict(constraints or {})
    parameters = dict(decision_obj.get("parameters") or {})

    if str(decision_obj.get("policy_scope") or "") != POLICY_SCOPE:
        return {"ok": False, "reason": "policy_scope_not_execution_only"}

    forbidden = sorted(set(parameters) & FORBIDDEN_POLICY_FIELDS)
    if forbidden:
        return {"ok": False, "reason": "forbidden_policy_fields", "fields": forbidden}

    unknown = sorted(set(parameters) - ALLOWED_PARAMETER_FIELDS)
    if unknown:
        return {"ok": False, "reason": "unknown_policy_fields", "fields": unknown}

    checks = (
        ("slice_pct", "min_slice_pct", "max_slice_pct"),
        ("target_participation", "min_participation", "max_participation"),
        ("slice_interval_ms", "min_slice_interval_ms", "max_slice_interval_ms"),
        ("entry_delay_ms", "min_entry_delay_ms", "max_entry_delay_ms"),
    )
    for param_key, min_key, max_key in checks:
        value = _safe_float(parameters.get(param_key), float("nan"))
        lower = _safe_float(constraints_obj.get(min_key), float("nan"))
        upper = _safe_float(constraints_obj.get(max_key), float("nan"))
        if not all(math.isfinite(v) for v in (value, lower, upper)):
            return {"ok": False, "reason": f"{param_key}_not_finite"}
        if value < min(lower, upper) - 1e-12 or value > max(lower, upper) + 1e-12:
            return {
                "ok": False,
                "reason": f"{param_key}_out_of_bounds",
                "value": value,
                "min": min(lower, upper),
                "max": max(lower, upper),
            }

    return {"ok": True, "reason": "ok"}


def metadata_for_order(
    *,
    decision: LearnedExecutionDecision,
    constraints: ExecutionSlicingConstraints,
    slice_index: int,
    slice_count: int,
) -> Dict[str, Any]:
    guard = constraints.as_guard()
    return {
        "learned_execution_policy": str(decision.policy_name),
        "learned_execution_policy_scope": str(decision.policy_scope),
        "learned_execution_locked": 1,
        "learned_execution_action_id": str(decision.action_id),
        "learned_execution_allowed_fields": sorted(ALLOWED_PARAMETER_FIELDS),
        "learned_execution_constraints": guard,
        "learned_execution_decision": decision.as_dict(),
        "learned_execution_guard": {
            "policy_name": str(decision.policy_name),
            "policy_scope": str(decision.policy_scope),
            "symbol": str(guard.get("symbol") or ""),
            "side": str(guard.get("side") or ""),
            "parent_qty": float(guard.get("parent_qty") or 0.0),
            "parent_id": str(guard.get("parent_id") or ""),
            "slice_index": int(slice_index),
            "slice_count": int(slice_count),
            "allowed_parameter_fields": sorted(ALLOWED_PARAMETER_FIELDS),
            "forbidden_policy_fields": sorted(FORBIDDEN_POLICY_FIELDS),
        },
        "learned_execution_parent_id": str(guard.get("parent_id") or ""),
        "learned_execution_slice_pct": float(decision.parameters["slice_pct"]),
        "learned_execution_target_participation": float(decision.parameters["target_participation"]),
    }


def _has_learned_marker(order: Mapping[str, Any]) -> bool:
    if order.get("learned_execution_policy") not in (None, ""):
        return True
    source = str(order.get("source") or order.get("order_source") or "").strip().lower()
    return source.startswith(("learned_execution", "contextual_bandit_execution"))


def validate_routed_learned_orders(orders: Iterable[Mapping[str, Any]] | None) -> Optional[Dict[str, Any]]:
    """Return a router block response if learned-order metadata is invalid."""
    blocked: List[Dict[str, Any]] = []
    grouped_abs_qty: Dict[str, float] = {}
    grouped_parent_qty: Dict[str, float] = {}

    for idx, order in enumerate(list(orders or [])):
        order_obj = dict(order or {})
        if not _has_learned_marker(order_obj):
            continue

        guard = dict(order_obj.get("learned_execution_guard") or {})
        constraints = dict(order_obj.get("learned_execution_constraints") or {})
        decision = dict(order_obj.get("learned_execution_decision") or {})

        def _block(reason: str, **extra: Any) -> None:
            blocked.append(
                {
                    "index": int(idx),
                    "symbol": str(order_obj.get("symbol") or "").upper().strip(),
                    "reason": str(reason),
                    **extra,
                }
            )

        if int(_safe_int(order_obj.get("execution_policy_locked"), 0)) != 1:
            _block("execution_policy_not_locked")
            continue
        if int(_safe_int(order_obj.get("learned_execution_locked"), 0)) != 1:
            _block("learned_execution_not_locked")
            continue
        if str(order_obj.get("learned_execution_policy_scope") or "") != POLICY_SCOPE:
            _block("policy_scope_not_execution_only")
            continue
        if not guard or not constraints or not decision:
            _block("learned_execution_guard_missing")
            continue

        symbol = str(order_obj.get("symbol") or "").upper().strip()
        side = normalize_side(order_obj.get("to_side") or order_obj.get("side"))
        if symbol != str(guard.get("symbol") or "").upper().strip():
            _block("symbol_changed", guard_symbol=str(guard.get("symbol") or ""))
            continue
        if side and side != normalize_side(guard.get("side")):
            _block("side_changed", guard_side=str(guard.get("side") or ""))
            continue

        verdict = enforce_execution_only_decision(decision=decision, constraints=constraints)
        if not bool(verdict.get("ok")):
            _block(str(verdict.get("reason") or "learned_execution_policy_violation"), verdict=dict(verdict))
            continue

        qty_abs = abs(_safe_float(order_obj.get("qty"), 0.0))
        parent_qty = abs(_safe_float(guard.get("parent_qty"), 0.0))
        if parent_qty <= 0.0:
            _block("parent_qty_missing")
            continue
        if qty_abs > parent_qty + 1e-9:
            _block("slice_qty_exceeds_parent_qty", qty=qty_abs, parent_qty=parent_qty)
            continue

        parent_id = str(guard.get("parent_id") or f"row:{idx}")
        grouped_abs_qty[parent_id] = grouped_abs_qty.get(parent_id, 0.0) + qty_abs
        grouped_parent_qty[parent_id] = max(grouped_parent_qty.get(parent_id, 0.0), parent_qty)

    for parent_id, qty_sum in grouped_abs_qty.items():
        parent_qty = grouped_parent_qty.get(parent_id, 0.0)
        if parent_qty > 0.0 and qty_sum > parent_qty + 1e-6:
            blocked.append(
                {
                    "index": -1,
                    "symbol": "",
                    "reason": "learned_slices_exceed_parent_qty",
                    "parent_id": str(parent_id),
                    "qty_sum": float(qty_sum),
                    "parent_qty": float(parent_qty),
                }
            )

    if not blocked:
        return None
    return {
        "ok": False,
        "status": "learned_execution_policy_forbidden",
        "reason": "learned execution orders must be execution-policy locked and execution-only bounded",
        "blocked_orders": blocked,
        "stop_failover": True,
        "retryable": False,
    }


def _estimated_metrics_for_policy(
    *,
    policy_name: str,
    context: Mapping[str, float],
    slice_pct_ratio: float,
    participation_ratio: float,
    interval_ratio: float,
) -> Dict[str, float]:
    spread = _safe_float(context.get("spread_bps"), 0.0)
    vol = _safe_float(context.get("volatility_bps"), 0.0)
    slip = max(_safe_float(context.get("slippage_bps"), 0.0), spread * 0.35)
    adverse = _safe_float(context.get("adverse_selection_bps"), 0.0)
    fill_risk = _clamp(_safe_float(context.get("fill_risk"), 0.0), 0.0, 1.0)

    style_mult = {
        "twap": (1.00, 1.00, 1.00),
        "vwap": (0.95, 0.95, 0.95),
        "pov": (0.90, 0.85, 1.10),
        "adaptive": (0.88, 0.90, 0.90),
        "learned": (
            0.82 + (0.12 * slice_pct_ratio),
            0.78 + (0.12 * participation_ratio),
            0.80 + (0.10 / max(1.0, interval_ratio)),
        ),
    }.get(str(policy_name), (1.0, 1.0, 1.0))

    slippage_bps = max(0.0, (slip + spread * 0.15 + vol * 0.02) * style_mult[0])
    adverse_bps = max(0.0, (adverse + spread * 0.10 + vol * 0.015) * style_mult[1])
    fill_risk_out = _clamp(
        fill_risk + max(0.0, 1.0 - participation_ratio) * 0.10 + max(0.0, interval_ratio - 1.0) * 0.04,
        0.0,
        1.0,
    )
    implementation_shortfall_bps = slippage_bps + adverse_bps + (fill_risk_out * 2.0)
    return {
        "implementation_shortfall_bps": float(implementation_shortfall_bps),
        "slippage_bps": float(slippage_bps),
        "fill_risk": float(fill_risk_out * style_mult[2]),
        "adverse_selection_bps": float(adverse_bps),
    }


def evaluate_against_baselines(
    samples: Iterable[Mapping[str, Any]],
    *,
    policy: ContextualBanditExecutionPolicy | None = None,
) -> Dict[str, Any]:
    """Evaluate learned slicing against TWAP/VWAP/POV/adaptive baselines.

    This prototype evaluator accepts historical or synthetic execution-context
    rows. It returns aggregate implementation shortfall, slippage, fill risk,
    and adverse-selection metrics for the learned policy and the baselines.
    """
    aggregate: Dict[str, Dict[str, float]] = {
        name: {
            "implementation_shortfall_bps": 0.0,
            "slippage_bps": 0.0,
            "fill_risk": 0.0,
            "adverse_selection_bps": 0.0,
            "n": 0.0,
        }
        for name in ("learned", "twap", "vwap", "pov", "adaptive")
    }
    decisions: List[Dict[str, Any]] = []

    for idx, sample in enumerate(list(samples or [])):
        order = dict(sample.get("order") or sample)
        constraints = sample.get("constraints")
        if isinstance(constraints, ExecutionSlicingConstraints):
            constraint_obj = constraints
        else:
            qty = _safe_float(order.get("qty"), _safe_float(sample.get("parent_qty"), 1.0))
            constraint_obj = build_constraints(
                order=order,
                symbol=str(order.get("symbol") or sample.get("symbol") or "UNKNOWN"),
                side=str(order.get("side") or order.get("to_side") or sample.get("side") or "LONG"),
                parent_qty=qty,
                parent_id=str(order.get("client_order_id") or sample.get("parent_id") or f"sample:{idx}"),
                base_slice_pct=_safe_float(sample.get("base_slice_pct"), 0.20),
                base_participation=_safe_float(sample.get("base_participation"), 0.03),
                base_slice_interval_ms=_safe_int(sample.get("base_slice_interval_ms"), 250),
                base_entry_delay_ms=_safe_int(sample.get("base_entry_delay_ms"), 0),
                max_slices=_safe_int(sample.get("max_slices"), 25),
            )
        context = build_context(
            order=order,
            feedback=dict(sample.get("feedback") or {}),
            execution_decision=dict(sample.get("execution_decision") or {}),
            extra=dict(sample.get("context") or {}),
        )
        decision = select_execution_adjustment(
            context=context,
            constraints=constraint_obj,
            policy=policy,
        )
        params = decision.parameters
        slice_pct_ratio = _safe_float(params.get("slice_pct"), constraint_obj.base_slice_pct) / max(
            1e-12,
            constraint_obj.base_slice_pct,
        )
        participation_ratio = _safe_float(params.get("target_participation"), constraint_obj.base_participation) / max(
            1e-12,
            constraint_obj.base_participation,
        )
        interval_ratio = _safe_float(params.get("slice_interval_ms"), constraint_obj.base_slice_interval_ms) / max(
            1.0,
            float(constraint_obj.base_slice_interval_ms),
        )
        decisions.append(
            {
                "sample_index": int(idx),
                "action_id": str(decision.action_id),
                "parameters": dict(params),
                "context": dict(context),
            }
        )

        metrics_by_policy = {
            "learned": _estimated_metrics_for_policy(
                policy_name="learned",
                context=context,
                slice_pct_ratio=slice_pct_ratio,
                participation_ratio=participation_ratio,
                interval_ratio=interval_ratio,
            ),
            "twap": _estimated_metrics_for_policy(
                policy_name="twap",
                context=context,
                slice_pct_ratio=1.0,
                participation_ratio=1.0,
                interval_ratio=1.0,
            ),
            "vwap": _estimated_metrics_for_policy(
                policy_name="vwap",
                context=context,
                slice_pct_ratio=1.0,
                participation_ratio=1.0,
                interval_ratio=1.0,
            ),
            "pov": _estimated_metrics_for_policy(
                policy_name="pov",
                context=context,
                slice_pct_ratio=0.8,
                participation_ratio=0.8,
                interval_ratio=1.2,
            ),
            "adaptive": _estimated_metrics_for_policy(
                policy_name="adaptive",
                context=context,
                slice_pct_ratio=0.9,
                participation_ratio=0.9,
                interval_ratio=1.0,
            ),
        }
        for policy_name, metrics in metrics_by_policy.items():
            for metric_name, value in metrics.items():
                aggregate[policy_name][metric_name] += float(value)
            aggregate[policy_name]["n"] += 1.0

    summary: Dict[str, Dict[str, float]] = {}
    for policy_name, totals in aggregate.items():
        n = max(1.0, float(totals.get("n") or 0.0))
        summary[policy_name] = {
            metric_name: (float(value) / n if metric_name != "n" else float(value))
            for metric_name, value in totals.items()
        }
    return {"ok": True, "summary": summary, "decisions": decisions}
