"""Live-order policy shaping and suppression engine.

The engine converts raw portfolio intents into execution-ready orders while
enforcing TTL decay, regime compatibility, capital-preservation compression,
and hard fail-closed barriers from kill switches and the trade-suppression
engine.
"""

"""
Execution Policy Engine (Unified + Regime Compatible + Trade Suppression Engine)

Preserves:
- TTL hard stop
- Half-life alpha decay
- Aggressiveness tiers
- Volatility slicing
- Broker-sim overrides
- Kill switch enforcement
- Structured audit trail
- Strict signal timestamp enforcement

Adds:
- Regime compatibility sizing
- Trade Suppression Engine (HARD_BLOCK / SOFT_THROTTLE / SIZE_COMPRESSION)
- Unified compatibility for both qty-orders and to_weight intents
"""

import logging
import math
import os
import time
from typing import Any, Dict, List, Optional, Tuple

from engine.audit.chain import append_chain_row
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import connect, init_db
from engine.runtime.risk_state import get_state
from engine.execution.kill_switch import execution_allowed
from engine.cache.wrappers.execution_mode import read_execution_mode as get_execution_mode
from engine.strategy.capital_guard import update_capital_preservation_mode
from engine.execution.trade_suppression_engine import (
    evaluate_trade_suppression,
)
from engine.execution.trade_attribution_ledger import log_suppression
from engine.execution.execution_decision_engine import (
    build_alpha_handoff,
    decide_execution_strategy,
    load_execution_feedback_snapshot,
)
from engine.execution.lob_simulation import (
    deeplob_shadow_enabled,
    shadow_deeplob_execution_signal,
)
from engine.strategy.regime_stack import (
    compute_regime_vector,
    regime_compatibility,
    regime_model_version,
)
from engine.strategy.meta_labeling import score_order_meta_label
from engine.strategy.conformal import conformal_gate_from_payload
from engine.strategy.ood import ood_gate_from_payload
from engine.strategy.uncertainty_sizing import uncertainty_gate_from_payload
from engine.runtime.live_ai_safety import live_ai_order_guard
from engine.strategy.learned_alpha_decay import execution_adjustment_for_order
from engine.strategy.ope_gate import evaluate_policy_ope_gate
from engine.execution.contextual_bandit_slicer import (
    LearnedExecutionPolicyViolation,
    build_constraints as build_learned_execution_constraints,
    build_context as build_learned_execution_context,
    learned_execution_enabled,
    metadata_for_order as learned_execution_metadata_for_order,
    select_execution_adjustment as select_learned_execution_adjustment,
)


LOG = logging.getLogger("engine.execution.execution_policy_engine")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: Exception, *, once_key: str | None = None, **extra: Any) -> None:
    key = str(once_key or "")
    if key:
        if key in _WARNED_NONFATAL_KEYS:
            return
        _WARNED_NONFATAL_KEYS.add(key)
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.execution.execution_policy_engine",
        extra=extra or {},
        include_health=False,
        persist=False,
    )


# ============================================================
# Defaults / knobs
# ============================================================

DEFAULT_TTL_MS = int(os.environ.get("EPE_DEFAULT_TTL_MS", str(5 * 60 * 1000)))
DEFAULT_HALF_LIFE_MS = int(os.environ.get("EPE_DEFAULT_HALF_LIFE_MS", str(90 * 1000)))

STRICT_SIGNAL_TS = os.environ.get("EPE_STRICT_SIGNAL_TS", "1") == "1"

PASSIVE_MIN_ALPHA = float(os.environ.get("EPE_PASSIVE_MIN_ALPHA", "0.70"))
NEUTRAL_MIN_ALPHA = float(os.environ.get("EPE_NEUTRAL_MIN_ALPHA", "0.40"))

SIM_LAT_MS_PASSIVE = int(os.environ.get("EPE_SIM_LAT_MS_PASSIVE", "220"))
SIM_LAT_MS_NEUTRAL = int(os.environ.get("EPE_SIM_LAT_MS_NEUTRAL", "140"))
SIM_LAT_MS_AGGRESSIVE = int(os.environ.get("EPE_SIM_LAT_MS_AGGRESSIVE", "80"))

SIM_CHUNK_PCT_PASSIVE = float(os.environ.get("EPE_SIM_CHUNK_PCT_PASSIVE", "0.22"))
SIM_CHUNK_PCT_NEUTRAL = float(os.environ.get("EPE_SIM_CHUNK_PCT_NEUTRAL", "0.33"))
SIM_CHUNK_PCT_AGGRESSIVE = float(os.environ.get("EPE_SIM_CHUNK_PCT_AGGRESSIVE", "0.45"))

SIM_EXTRA_SLIP_BPS_PASSIVE = float(os.environ.get("EPE_SIM_EXTRA_SLIP_BPS_PASSIVE", "0.0"))
SIM_EXTRA_SLIP_BPS_NEUTRAL = float(os.environ.get("EPE_SIM_EXTRA_SLIP_BPS_NEUTRAL", "0.5"))
SIM_EXTRA_SLIP_BPS_AGGRESSIVE = float(os.environ.get("EPE_SIM_EXTRA_SLIP_BPS_AGGRESSIVE", "1.5"))

# Capital Preservation Mode (CPM)
EPE_CAPITAL_PRESERVE_KEEP_PCT = float(os.environ.get("EPE_CAPITAL_PRESERVE_KEEP_PCT", "0.50"))
EPE_CAPITAL_PRESERVE_MIN_ORDERS = int(os.environ.get("EPE_CAPITAL_PRESERVE_MIN_ORDERS", "1"))
EPE_CAPITAL_PRESERVE_QTY_MULT = float(os.environ.get("EPE_CAPITAL_PRESERVE_QTY_MULT", "0.50"))
EPE_CAPITAL_PRESERVE_SIM_LAT_MS = int(os.environ.get("EPE_CAPITAL_PRESERVE_SIM_LAT_MS", "260"))
EPE_CAPITAL_PRESERVE_CHUNK_PCT = float(os.environ.get("EPE_CAPITAL_PRESERVE_CHUNK_PCT", "0.18"))
EPE_CAPITAL_PRESERVE_EXTRA_SLIP_BPS = float(os.environ.get("EPE_CAPITAL_PRESERVE_EXTRA_SLIP_BPS", "0.0"))

TSE_SOFT_MIN_ALPHA = float(os.environ.get("TSE_SOFT_MIN_ALPHA", "0.30"))
TSE_MIN_ABS_QTY = float(os.environ.get("TSE_MIN_ABS_QTY", "1e-9"))
TSE_MAX_SLICES = int(os.environ.get("TSE_MAX_SLICES", "25"))
EPE_MAX_FUTURE_SIGNAL_TS_MS = int(os.environ.get("EPE_MAX_FUTURE_SIGNAL_TS_MS", "30000"))
EPE_MIN_REGIME_COMPAT_SCALE = float(os.environ.get("EPE_MIN_REGIME_COMPAT_SCALE", "0.35"))


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if not math.isfinite(v):
            return float(default)
        return float(v)
    except Exception as e:
        _warn_nonfatal("EXECUTION_POLICY_ENGINE_SAFE_FLOAT_FAILED", e, once_key="safe_float", value=repr(x)[:120])
        return float(default)


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception as e:
        _warn_nonfatal("EXECUTION_POLICY_ENGINE_SAFE_INT_FAILED", e, once_key="safe_int", value=repr(x)[:120])
        return int(default)


def _ensure_tables(con) -> None:
    # The audit table is additive and compatibility-tolerant because policy
    # decisions are long-lived operational evidence across schema revisions.
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS execution_policy_audit (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          signal_id TEXT,
          model_id TEXT NOT NULL DEFAULT 'baseline',
          symbol TEXT,
          side TEXT,
          qty REAL,
          age_ms INTEGER,
          ttl_ms INTEGER,
          volatility REAL,
          regime_compat REAL,
          source_order_id INTEGER,
          policy_json TEXT NOT NULL,
          prev_hash BLOB,
          row_hash BLOB NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_epe_audit_ts ON execution_policy_audit(ts_ms);
        CREATE INDEX IF NOT EXISTS idx_epe_audit_sym ON execution_policy_audit(symbol);
        """
    )

    cols = {
        str(r[1]).strip().lower()
        for r in (con.execute("PRAGMA table_info(execution_policy_audit)").fetchall() or [])
    }

    if "source_alert_id" not in cols:
        try:
            con.execute("ALTER TABLE execution_policy_audit ADD COLUMN source_alert_id INTEGER;")
        except Exception as e:
            _warn_nonfatal("EXECUTION_POLICY_ENGINE_AUDIT_MIGRATION_FAILED", e, once_key="audit_column:source_alert_id", column="source_alert_id")
    if "model_id" not in cols:
        try:
            con.execute("ALTER TABLE execution_policy_audit ADD COLUMN model_id TEXT NOT NULL DEFAULT 'baseline';")
        except Exception as e:
            _warn_nonfatal("EXECUTION_POLICY_ENGINE_AUDIT_MIGRATION_FAILED", e, once_key="audit_column:model_id", column="model_id")
    if "decision_json" not in cols:
        try:
            con.execute("ALTER TABLE execution_policy_audit ADD COLUMN decision_json TEXT;")
        except Exception as e:
            _warn_nonfatal("EXECUTION_POLICY_ENGINE_AUDIT_MIGRATION_FAILED", e, once_key="audit_column:decision_json", column="decision_json")
    if "prev_hash" not in cols:
        try:
            con.execute("ALTER TABLE execution_policy_audit ADD COLUMN prev_hash BLOB;")
        except Exception as e:
            _warn_nonfatal("EXECUTION_POLICY_ENGINE_AUDIT_MIGRATION_FAILED", e, once_key="audit_column:prev_hash", column="prev_hash")
    if "row_hash" not in cols:
        try:
            con.execute("ALTER TABLE execution_policy_audit ADD COLUMN row_hash BLOB;")
        except Exception as e:
            _warn_nonfatal("EXECUTION_POLICY_ENGINE_AUDIT_MIGRATION_FAILED", e, once_key="audit_column:row_hash", column="row_hash")
    if "actor" not in cols:
        try:
            con.execute("ALTER TABLE execution_policy_audit ADD COLUMN actor TEXT;")
        except Exception as e:
            _warn_nonfatal("EXECUTION_POLICY_ENGINE_AUDIT_MIGRATION_FAILED", e, once_key="audit_column:actor", column="actor")
    if "mode" not in cols:
        try:
            con.execute("ALTER TABLE execution_policy_audit ADD COLUMN mode TEXT;")
        except Exception as e:
            _warn_nonfatal("EXECUTION_POLICY_ENGINE_AUDIT_MIGRATION_FAILED", e, once_key="audit_column:mode", column="mode")
    if "broker" not in cols:
        try:
            con.execute("ALTER TABLE execution_policy_audit ADD COLUMN broker TEXT;")
        except Exception as e:
            _warn_nonfatal("EXECUTION_POLICY_ENGINE_AUDIT_MIGRATION_FAILED", e, once_key="audit_column:broker", column="broker")
    if "portfolio_orders_batch_id" not in cols:
        try:
            con.execute("ALTER TABLE execution_policy_audit ADD COLUMN portfolio_orders_batch_id INTEGER;")
        except Exception as e:
            _warn_nonfatal("EXECUTION_POLICY_ENGINE_AUDIT_MIGRATION_FAILED", e, once_key="audit_column:portfolio_orders_batch_id", column="portfolio_orders_batch_id")
    if "suppression_state" not in cols:
        try:
            con.execute("ALTER TABLE execution_policy_audit ADD COLUMN suppression_state TEXT;")
        except Exception as e:
            _warn_nonfatal("EXECUTION_POLICY_ENGINE_AUDIT_MIGRATION_FAILED", e, once_key="audit_column:suppression_state", column="suppression_state")


def _alpha_remaining(age_ms: int, half_life_ms: int, ttl_ms: int) -> float:
    # Alpha decay is hard-clamped by TTL: once expired, downstream shaping
    # should treat the signal as having no remaining execution value.
    if ttl_ms <= 0 or age_ms >= ttl_ms:
        return 0.0
    hl = max(1, half_life_ms)
    rem = math.pow(0.5, float(age_ms) / float(hl))
    return max(0.0, min(1.0, rem))


def _decision_from_alpha(alpha_rem: float):
    # Map continuous alpha decay into coarse execution tiers so broker logic
    # receives stable, interpretable instructions rather than noisy floats.
    if alpha_rem >= PASSIVE_MIN_ALPHA:
        return "LIMIT", "PASSIVE", SIM_LAT_MS_PASSIVE, SIM_CHUNK_PCT_PASSIVE, SIM_EXTRA_SLIP_BPS_PASSIVE
    if alpha_rem >= NEUTRAL_MIN_ALPHA:
        return "LIMIT", "NEUTRAL", SIM_LAT_MS_NEUTRAL, SIM_CHUNK_PCT_NEUTRAL, SIM_EXTRA_SLIP_BPS_NEUTRAL
    return "MARKET", "AGGRESSIVE", SIM_LAT_MS_AGGRESSIVE, SIM_CHUNK_PCT_AGGRESSIVE, SIM_EXTRA_SLIP_BPS_AGGRESSIVE


def _normalize_side(o: Dict[str, Any]) -> str:
    side = str(o.get("side") or o.get("to_side") or "").upper().strip()
    if side in ("BUY", "LONG"):
        return "LONG"
    if side in ("SELL", "SHORT"):
        return "SHORT"

    qty = _safe_float(o.get("qty"), 0.0)
    if qty > 0.0:
        return "LONG"
    if qty < 0.0:
        return "SHORT"

    to_weight = _safe_float(o.get("to_weight"), 0.0)
    if to_weight > 0.0:
        return "LONG"
    if to_weight < 0.0:
        return "SHORT"

    return ""


def _normalize_signal_ts(o: Dict[str, Any], default_signal_ts_ms: Optional[int]) -> int:
    signal_ts = _safe_int(o.get("signal_ts_ms"), 0)
    if signal_ts > 0:
        return int(signal_ts)

    ts_ms = _safe_int(o.get("ts_ms"), 0)
    if ts_ms > 0:
        return int(ts_ms)

    source_ts_ms = _safe_int(o.get("source_ts_ms"), 0)
    if source_ts_ms > 0:
        return int(source_ts_ms)

    if default_signal_ts_ms is not None:
        return int(default_signal_ts_ms)
    return 0


def _regime_compatibility(con, symbol: str, signal_ts: int, o: Dict[str, Any]) -> Tuple[float, Optional[Dict[str, Any]]]:
    try:
        regime_vec = compute_regime_vector(symbol=symbol, ts_ms=int(signal_ts), con=con)
    except Exception as e:
        _warn_nonfatal(
            "EXECUTION_POLICY_ENGINE_REGIME_VECTOR_FAILED",
            e,
            once_key=f"regime_vector:{symbol}",
            symbol=str(symbol),
            signal_ts=int(signal_ts),
        )
        regime_vec = None

    try:
        prof = o.get("regime_profile")
        if isinstance(prof, dict) and regime_vec:
            regime_comp = float(regime_compatibility(prof, regime_vec))
        else:
            regime_comp = 1.0
    except Exception as e:
        _warn_nonfatal(
            "EXECUTION_POLICY_ENGINE_REGIME_COMPAT_FAILED",
            e,
            once_key=f"regime_compat:{symbol}",
            symbol=str(symbol),
            signal_ts=int(signal_ts),
        )
        regime_comp = 1.0

    if not (regime_comp == regime_comp):
        regime_comp = 1.0

    # Compatibility is a sizing modifier, not a standalone veto. Hard blocks
    # come from suppression and kill-switch layers.
    regime_comp = max(0.0, min(1.0, float(regime_comp)))
    return float(regime_comp), regime_vec


def _scale_order_fields(o: Dict[str, Any], scale: float) -> Dict[str, Any]:
    out = dict(o)
    sc = max(0.0, float(scale))

    if "qty" in out and out.get("qty") is not None:
        try:
            out["qty"] = float(out.get("qty") or 0.0) * float(sc)
        except Exception as e:
            _warn_nonfatal("EXECUTION_POLICY_ENGINE_QTY_SCALE_FAILED", e, once_key="qty_scale")

    if "to_weight" in out and out.get("to_weight") is not None:
        try:
            out["to_weight"] = float(out.get("to_weight") or 0.0) * float(sc)
        except Exception as e:
            _warn_nonfatal("EXECUTION_POLICY_ENGINE_WEIGHT_SCALE_FAILED", e, once_key="to_weight_scale")

    if "delta_weight" in out and out.get("delta_weight") is not None:
        try:
            out["delta_weight"] = float(out.get("delta_weight") or 0.0) * float(sc)
        except Exception as e:
            _warn_nonfatal("EXECUTION_POLICY_ENGINE_WEIGHT_SCALE_FAILED", e, once_key="delta_weight_scale")

    if "from_weight" in out and out.get("from_weight") is not None and "to_weight" in out:
        try:
            out["delta_weight"] = float(out.get("to_weight") or 0.0) - float(out.get("from_weight") or 0.0)
        except Exception as e:
            _warn_nonfatal("EXECUTION_POLICY_ENGINE_DELTA_WEIGHT_COMPUTE_FAILED", e, once_key="delta_weight_compute")

    return out


def _effective_abs_qty(o: Dict[str, Any]) -> float:
    qty = _safe_float(o.get("qty"), 0.0)
    if abs(qty) > 0.0:
        return abs(float(qty))
    tw = _safe_float(o.get("to_weight"), 0.0)
    if abs(tw) > 0.0:
        return abs(float(tw))
    return 0.0


def _signed_qty_or_weight(o: Dict[str, Any], side: str) -> float:
    qty = _safe_float(o.get("qty"), 0.0)
    if abs(qty) > 0.0:
        return float(qty)

    tw = _safe_float(o.get("to_weight"), 0.0)
    if abs(tw) > 0.0:
        if side == "SHORT":
            return -abs(float(tw))
        return abs(float(tw))

    return 0.0


def _first_present(o: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in o:
            return o.get(key)
    return None


def _ope_payload_from_order(
    original_order: Dict[str, Any],
    shaped_order: Dict[str, Any],
    *,
    side: str,
    signed_qty: float,
) -> Dict[str, Any]:
    payload = {
        "policy_type": _first_present(original_order, "policy_type", "candidate_type"),
        "candidate_key": _first_present(original_order, "candidate_key", "policy_id"),
        "candidate_version": _first_present(original_order, "candidate_version", "policy_version", "model_version"),
        "logged_action": _first_present(original_order, "logged_action", "behavior_action", "side", "to_side"),
        "target_action": _first_present(original_order, "target_action", "candidate_action", "policy_action"),
        "behavior_propensity": _first_present(
            original_order,
            "behavior_propensity",
            "logging_propensity",
            "logged_propensity",
            "decision_propensity",
            "propensity",
        ),
        "target_propensity": _first_present(
            original_order,
            "target_propensity",
            "candidate_propensity",
            "evaluation_propensity",
            "policy_propensity",
        ),
        "outcome": _first_present(
            original_order,
            "outcome",
            "reward",
            "net_return",
            "net_ret",
            "realized_return",
            "realized_ret",
        ),
        "logged_model_estimate": _first_present(
            original_order,
            "logged_model_estimate",
            "behavior_model_estimate",
            "q_logged",
            "model_estimate",
        ),
        "target_model_estimate": _first_present(
            original_order,
            "target_model_estimate",
            "candidate_model_estimate",
            "q_target",
            "policy_model_estimate",
        ),
        "target_qty_or_weight": float(signed_qty),
        "target_side": str(side),
        "target_to_weight": shaped_order.get("to_weight"),
        "target_qty": shaped_order.get("qty"),
    }
    if not payload.get("logged_action"):
        payload["logged_action"] = str(side)
    if not payload.get("target_action"):
        payload["target_action"] = str(side)
    return {str(key): value for key, value in payload.items() if value is not None and value != ""}


def _learned_execution_ope_gate(con, order: Dict[str, Any], *, symbol: str) -> tuple[bool, Dict[str, Any]]:
    policy_name = str(
        order.get("learned_execution_policy")
        or order.get("execution_policy_model_name")
        or order.get("policy_name")
        or "contextual_bandit_execution_slicer_v1"
    )
    policy_id = str(
        order.get("learned_execution_policy_id")
        or order.get("execution_policy_model_id")
        or order.get("policy_id")
        or policy_name
    )
    policy_version = str(
        order.get("learned_execution_policy_version")
        or order.get("execution_policy_model_version")
        or order.get("policy_version")
        or ""
    )
    candidate_type = str(
        order.get("learned_execution_candidate_type")
        or order.get("policy_type")
        or order.get("candidate_type")
        or "bandit"
    )
    return evaluate_policy_ope_gate(
        model_id=policy_id,
        model_name=policy_name,
        candidate_type=candidate_type,
        model_kind="contextual_bandit",
        candidate_version=policy_version,
        symbol=str(symbol or "").upper().strip(),
        regime=str(order.get("regime") or "global"),
        metadata={
            "candidate_key": str(order.get("learned_execution_candidate_key") or policy_id),
            "policy_id": policy_id,
            "policy_type": candidate_type,
            "source": "learned_execution.contextual_bandit",
        },
        con=con,
    )


def _is_risk_increasing_order(o: Dict[str, Any], side: str) -> bool:
    action = str(o.get("action") or "").strip().upper()
    if bool(o.get("reduce_only")) or action in {"CLOSE", "EXIT", "REDUCE", "FLATTEN", "LIQUIDATE"}:
        return False

    if "to_weight" in o and o.get("to_weight") is not None:
        from_weight = _safe_float(o.get("from_weight"), 0.0)
        to_weight = _safe_float(o.get("to_weight"), 0.0)
        if abs(float(to_weight)) <= abs(float(from_weight)) + float(TSE_MIN_ABS_QTY):
            return False
        return True

    qty = _safe_float(o.get("qty"), 0.0)
    if abs(float(qty)) <= float(TSE_MIN_ABS_QTY):
        return False

    if side == "SHORT" and qty > 0.0 and action in {"COVER", "BUY_TO_COVER"}:
        return False
    return True


def _build_slice_qtys(total_qty: float, slice_pct: float, max_slices: int) -> List[float]:
    signed_total = float(total_qty)
    abs_total = abs(float(signed_total))
    if abs_total <= 0.0:
        return []

    target_slice_abs = abs_total * max(0.0, float(slice_pct))
    if target_slice_abs <= 0.0:
        return [float(signed_total)]

    slice_count = int(math.ceil(abs_total / max(target_slice_abs, 1e-12)))
    slice_count = max(1, min(max(1, int(max_slices)), slice_count))
    base_slice_abs = abs_total / float(slice_count)
    sign = 1.0 if signed_total >= 0.0 else -1.0

    emitted_abs = 0.0
    slices: List[float] = []
    for idx in range(slice_count):
        if idx == (slice_count - 1):
            cur_abs = max(0.0, abs_total - emitted_abs)
        else:
            cur_abs = float(base_slice_abs)
        emitted_abs += float(cur_abs)
        slices.append(sign * float(cur_abs))
    return slices


def _log_suppression_event(
    *,
    o: Dict[str, Any],
    reason: str,
    decision_json: Dict[str, Any],
    execution_policy_json: Dict[str, Any],
) -> None:
    try:
        log_suppression(
            source_alert_id=(_safe_int(o.get("source_alert_id"), 0) if o.get("source_alert_id") is not None else None),
            symbol=str(o.get("symbol") or "").strip().upper(),
            suppression_reason=str(reason or "").strip(),
            signal_json={
                "signal_id": o.get("signal_id"),
                "signal_ts_ms": o.get("signal_ts_ms"),
                "side": o.get("side") or o.get("to_side"),
                "qty": o.get("qty"),
                "to_weight": o.get("to_weight"),
            },
            execution_policy_json=execution_policy_json,
            decision_json=decision_json,
        )
    except Exception as e:
        _warn_nonfatal("EXECUTION_POLICY_ENGINE_SUPPRESSION_LOG_FAILED", e, once_key="log_suppression")


def apply_execution_policy(
    orders: Optional[List[Dict[str, Any]]] = None,
    *,
    con=None,
    intents: Optional[List[Dict[str, Any]]] = None,
    actor: str = "system",
    mode: str = "unknown",
    broker: str = "unknown",
    portfolio_orders_batch_id: Optional[int] = None,
    portfolio_orders_id: Optional[int] = None,
    default_signal_ts_ms: Optional[int] = None,
    now_ms: Optional[int] = None,
    initialize_storage: bool = True,
    execution_allowed_fn: Any = None,
    trade_suppression_fn: Any = None,
    capital_preservation_fn: Any = None,
    execution_mode_fn: Any = None,
    risk_state_getter_fn: Any = None,
    regime_compatibility_fn: Any = None,
    execution_feedback_fn: Any = None,
) -> List[Dict[str, Any]]:
    """Filter and transform candidate orders into execution-ready instructions.

    Parameters
    ----------
    orders : list of dict, optional
        Legacy alias for ``intents``. Each item may include fields such as
        ``symbol``, side/quantity or target-weight data, ``confidence``,
        ``expected_z``/``zscore``, ``signal_ts_ms``, ``alpha_ttl_ms``, and
        ``alpha_half_life_ms``.
    con : storage connection, optional
        Existing database connection used for suppression lookups and audit
        writes. A temporary connection is opened when omitted.
    intents : list of dict, optional
        Preferred input list. When provided, it takes precedence over
        ``orders``.
    actor : str, default="system"
        Actor label forwarded into suppression and decision records.
    mode : str, default="unknown"
        Execution mode label recorded with suppression state.
    broker : str, default="unknown"
        Broker label recorded with suppression state.
    portfolio_orders_batch_id : int, optional
        Batch lineage identifier carried into shaped order metadata.
    portfolio_orders_id : int, optional
        Row lineage identifier carried into shaped order metadata.
    default_signal_ts_ms : int, optional
        Epoch milliseconds used when an order omits a signal timestamp.

    Returns
    -------
    list of dict
        Execution-ready order payloads. An empty list means the batch was fully
        suppressed or a hard execution barrier blocked the request.

    Raises
    ------
    Exception
        Propagates database or policy-evaluation failures.

    Notes
    -----
    Evaluation is fail-closed. Global kill-switch blocks and trade-suppression
    hard blocks both return an empty list after recording suppression events.
    Signal-age checks operate in milliseconds and reject timestamps that are
    too far in the future, older than TTL, or fully decayed by alpha
    half-life/TTL rules.

    Side Effects
    ------------
    Initializes execution tables, reads runtime risk state, and writes
    suppression or decision-audit records for blocked or transformed orders.
    """
    if bool(initialize_storage):
        init_db()

    payload = list(intents if intents is not None else (orders or []))
    shaped: List[Dict[str, Any]] = []
    feedback_cache: Dict[Tuple[str, str], Dict[str, Any]] = {}

    owns = False
    if con is None:
        con = connect()
        owns = True

    try:
        _ensure_tables(con)

        resolved_execution_allowed = execution_allowed_fn or execution_allowed
        allow, ks_reason, ks_meta = resolved_execution_allowed(con=con, symbol=None, regime=None)
        if not allow:
            for o in payload:
                if isinstance(o, dict):
                    _log_suppression_event(
                        o=o,
                        reason=str(ks_reason or "kill_switch_block"),
                        decision_json={"blocked_by": "kill_switch", "meta": ks_meta or {}},
                        execution_policy_json={"kill_switch_reason": ks_reason, "kill_switch_meta": ks_meta or {}},
                    )
            return []

        now_ms = int(now_ms if now_ms is not None else _now_ms())
        resolved_trade_suppression = trade_suppression_fn or evaluate_trade_suppression
        tse = resolved_trade_suppression(
            con=con,
            actor=str(actor or "system"),
            mode=str(mode or "unknown"),
            broker=str(broker or "unknown"),
            initialize_storage=bool(initialize_storage),
            now_ms=int(now_ms),
            persist_runtime_state=bool(initialize_storage),
        ) or {"state": "NONE", "action": "NONE", "size_mult": 1.0, "throttle_mult": 1.0, "hard_block": False}

        if bool(tse.get("hard_block")):
            for o in payload:
                if isinstance(o, dict):
                    _log_suppression_event(
                        o=o,
                        reason=f"tse_hard_block:{str(tse.get('reason') or '')}",
                        decision_json={"blocked_by": "tse", "tse": tse},
                        execution_policy_json={"tse": tse, "execution_mode": (execution_mode_fn or get_execution_mode)()},
                    )
            return []

        try:
            resolved_capital_preservation = capital_preservation_fn or update_capital_preservation_mode
            cpm_snapshot = resolved_capital_preservation(con=con) or {}
        except Exception as e:
            _warn_nonfatal(
                "EXECUTION_POLICY_ENGINE_CPM_UPDATE_FAILED",
                e,
                once_key="capital_preservation_update",
            )
            cpm_snapshot = {}

        resolved_risk_state_getter = risk_state_getter_fn or get_state
        capital_mode = str(resolved_risk_state_getter("capital_mode", "normal") or "normal").lower()
        work_orders = list(payload)

        if capital_mode == "preserve" and work_orders:
            ranked = []
            for idx, o in enumerate(work_orders):
                try:
                    qty_score = abs(_safe_float(o.get("qty"), 0.0))
                except Exception as e:
                    _warn_nonfatal(
                        "EXECUTION_POLICY_ENGINE_PRESERVE_QTY_SCORE_FAILED",
                        e,
                        once_key="capital_preservation_qty_score",
                    )
                    qty_score = 0.0

                try:
                    conf_score = max(0.01, float(o.get("confidence") or 1.0))
                except Exception as e:
                    _warn_nonfatal(
                        "EXECUTION_POLICY_ENGINE_PRESERVE_CONF_SCORE_FAILED",
                        e,
                        once_key="capital_preservation_conf_score",
                    )
                    conf_score = 1.0

                try:
                    z_score = max(1.0, abs(float(o.get("expected_z") or o.get("zscore") or 0.0)))
                except Exception as e:
                    _warn_nonfatal(
                        "EXECUTION_POLICY_ENGINE_PRESERVE_Z_SCORE_FAILED",
                        e,
                        once_key="capital_preservation_z_score",
                    )
                    z_score = 1.0

                ranked.append((-(qty_score * conf_score * z_score), idx))

            ranked.sort()

            keep_n = max(
                int(EPE_CAPITAL_PRESERVE_MIN_ORDERS),
                int(math.ceil(len(ranked) * max(0.0, min(1.0, EPE_CAPITAL_PRESERVE_KEEP_PCT)))),
            )

            keep_idx = {idx for _, idx in ranked[:keep_n]}
            work_orders = [o for idx, o in enumerate(work_orders) if idx in keep_idx]

        for o in work_orders:
            if not isinstance(o, dict):
                continue

            symbol = str(o.get("symbol") or "").strip().upper()
            if not symbol:
                _log_suppression_event(
                    o=o,
                    reason="missing_symbol",
                    decision_json={"blocked_by": "epe", "reason": "missing_symbol"},
                    execution_policy_json={"tse": tse},
                )
                continue

            side = _normalize_side(o)
            if not side:
                _log_suppression_event(
                    o=o,
                    reason="missing_side",
                    decision_json={"blocked_by": "epe", "reason": "missing_side"},
                    execution_policy_json={"tse": tse},
                )
                continue

            risk_increasing_order = bool(_is_risk_increasing_order(o, side))
            live_ai_gate = live_ai_order_guard(
                o,
                execution_mode=str(mode or ""),
                broker=str(broker or ""),
                risk_increasing=bool(risk_increasing_order),
                now_ms=int(now_ms),
            )
            if not bool(live_ai_gate.get("ok")):
                reason_code = str(live_ai_gate.get("reason") or "failed").strip() or "failed"
                _log_suppression_event(
                    o=o,
                    reason=f"live_ai_safety_{reason_code}",
                    decision_json={
                        "blocked_by": "live_ai_safety",
                        "live_ai_safety": dict(live_ai_gate),
                    },
                    execution_policy_json={
                        "live_ai_safety": dict(live_ai_gate),
                    },
                )
                continue

            signal_ts = _normalize_signal_ts(o, default_signal_ts_ms)
            ttl_ms = max(1, _safe_int(o.get("alpha_ttl_ms"), DEFAULT_TTL_MS))
            half_life_ms = max(1, _safe_int(o.get("alpha_half_life_ms"), DEFAULT_HALF_LIFE_MS))

            if signal_ts <= 0 and STRICT_SIGNAL_TS:
                signal_ts = int(now_ms)

            if signal_ts > 0 and signal_ts > (now_ms + int(EPE_MAX_FUTURE_SIGNAL_TS_MS)):
                _log_suppression_event(
                    o=o,
                    reason="future_signal_ts_ms",
                    decision_json={
                        "blocked_by": "epe",
                        "reason": "future_signal_ts_ms",
                        "signal_ts_ms": int(signal_ts),
                        "now_ms": int(now_ms),
                        "max_future_ms": int(EPE_MAX_FUTURE_SIGNAL_TS_MS),
                    },
                    execution_policy_json={"tse": tse},
                )
                continue

            age_ms = max(0, now_ms - signal_ts) if signal_ts > 0 else 0
            try:
                learned_alpha_gate = execution_adjustment_for_order(
                    con,
                    o,
                    age_ms=int(age_ms),
                    ttl_ms=int(ttl_ms),
                    half_life_ms=int(half_life_ms),
                    now_ms=int(now_ms),
                )
            except Exception as e:
                _warn_nonfatal(
                    "EXECUTION_POLICY_ENGINE_LEARNED_ALPHA_FAILED",
                    e,
                    once_key=f"learned_alpha:{symbol}",
                    symbol=str(symbol),
                )
                learned_alpha_gate = {
                    "available": False,
                    "blocked": False,
                    "reason": f"lookup_failed:{type(e).__name__}",
                    "ttl_ms": int(ttl_ms),
                    "half_life_ms": int(half_life_ms),
                    "size_multiplier": 1.0,
                    "estimate": {},
                }

            if bool(learned_alpha_gate.get("blocked")) and bool(risk_increasing_order):
                _log_suppression_event(
                    o=o,
                    reason=str(learned_alpha_gate.get("reason") or "learned_alpha_blocked"),
                    decision_json={
                        "blocked_by": "learned_alpha_decay",
                        "age_ms": int(age_ms),
                        "learned_alpha": dict(learned_alpha_gate),
                    },
                    execution_policy_json={
                        "learned_alpha": dict(learned_alpha_gate),
                    },
                )
                continue
            if bool(learned_alpha_gate.get("blocked")) and not bool(risk_increasing_order):
                learned_alpha_gate = {
                    **dict(learned_alpha_gate),
                    "blocked": False,
                    "risk_reduction_bypass": True,
                    "size_multiplier": 1.0,
                }

            ttl_ms = max(1, _safe_int(learned_alpha_gate.get("ttl_ms"), ttl_ms))
            half_life_ms = max(1, _safe_int(learned_alpha_gate.get("half_life_ms"), half_life_ms))

            if ttl_ms > 0 and age_ms > ttl_ms:
                _log_suppression_event(
                    o=o,
                    reason="ttl_expired",
                    decision_json={"blocked_by": "epe", "reason": "ttl_expired", "age_ms": age_ms, "ttl_ms": ttl_ms},
                    execution_policy_json={"tse": tse},
                )
                continue

            alpha_rem = _alpha_remaining(age_ms, half_life_ms, ttl_ms)

            if alpha_rem <= 0.0:
                _log_suppression_event(
                    o=o,
                    reason="alpha_decay_expired",
                    decision_json={
                        "blocked_by": "alpha_decay",
                        "alpha_remaining": alpha_rem,
                        "age_ms": age_ms,
                        "half_life_ms": half_life_ms,
                    },
                    execution_policy_json={
                        "alpha_remaining": alpha_rem,
                        "alpha_half_life_ms": half_life_ms,
                    },
                )
                continue

            alpha_rem = max(0.10, float(alpha_rem))
            order_type, aggressiveness, sim_lat_ms, sim_chunk_pct, sim_extra_slip = _decision_from_alpha(alpha_rem)

            if capital_mode == "preserve":
                order_type = "LIMIT"
                aggressiveness = "PASSIVE"
                sim_lat_ms = max(sim_lat_ms, int(EPE_CAPITAL_PRESERVE_SIM_LAT_MS))
                sim_chunk_pct = min(sim_chunk_pct, float(EPE_CAPITAL_PRESERVE_CHUNK_PCT))
                sim_extra_slip = min(sim_extra_slip, float(EPE_CAPITAL_PRESERVE_EXTRA_SLIP_BPS))

            sim_lat_ms = int(float(sim_lat_ms) * (1.0 + (1.0 - alpha_rem)))
            sim_chunk_pct = float(sim_chunk_pct) * max(0.15, alpha_rem)

            volatility = _safe_float(o.get("volatility"), 0.0)
            resolved_regime_compatibility = regime_compatibility_fn or _regime_compatibility
            regime_comp, regime_vec = resolved_regime_compatibility(con, symbol, signal_ts, o)

            size_scale = max(float(EPE_MIN_REGIME_COMPAT_SCALE), float(regime_comp))
            size_scale = max(0.0, min(1.0, float(size_scale)))

            learned_alpha_size_mult = max(0.0, min(1.0, _safe_float(learned_alpha_gate.get("size_multiplier"), 1.0)))
            size_scale *= float(learned_alpha_size_mult)
            size_scale = max(0.0, min(1.0, float(size_scale)))

            if capital_mode == "preserve":
                size_scale *= float(EPE_CAPITAL_PRESERVE_QTY_MULT)

            tse_action = str(tse.get("action") or "NONE").upper().strip()
            if tse_action == "SIZE_COMPRESSION":
                if float(alpha_rem) < float(TSE_SOFT_MIN_ALPHA):
                    _log_suppression_event(
                        o=o,
                        reason="tse_size_compression_alpha_gate",
                        decision_json={"blocked_by": "tse_size_compression", "alpha_remaining": alpha_rem, "tse": tse},
                        execution_policy_json={"tse": tse, "alpha_remaining": alpha_rem},
                    )
                    continue
                size_scale *= max(0.0, _safe_float(tse.get("size_mult"), 1.0))

            if tse_action == "SOFT_THROTTLE":
                if float(alpha_rem) < float(TSE_SOFT_MIN_ALPHA):
                    _log_suppression_event(
                        o=o,
                        reason=f"tse_soft_throttle_alpha_gate:{str(tse.get('reason') or '')}",
                        decision_json={"blocked_by": "tse_soft_throttle", "alpha_remaining": alpha_rem, "tse": tse},
                        execution_policy_json={"tse": tse, "alpha_remaining": alpha_rem},
                    )
                    continue
                size_scale *= max(0.0, _safe_float(tse.get("size_mult"), 1.0))
                order_type = "LIMIT"
                aggressiveness = "PASSIVE"
                sim_lat_ms = max(sim_lat_ms, SIM_LAT_MS_PASSIVE)
                sim_chunk_pct = min(sim_chunk_pct, SIM_CHUNK_PCT_PASSIVE)
                sim_extra_slip = min(sim_extra_slip, SIM_EXTRA_SLIP_BPS_PASSIVE)

            cache_key = (str(symbol), str(broker or "unknown").strip().lower())
            feedback_snapshot = feedback_cache.get(cache_key)
            if feedback_snapshot is None:
                resolved_execution_feedback = execution_feedback_fn or load_execution_feedback_snapshot
                feedback_snapshot = resolved_execution_feedback(
                    con,
                    symbol=str(symbol),
                    broker=str(broker or "unknown"),
                )
                feedback_cache[cache_key] = dict(feedback_snapshot or {})

            alpha_intent = build_alpha_handoff(
                o,
                side=str(side),
                signal_ts_ms=int(signal_ts),
                alpha_remaining=float(alpha_rem),
                ttl_ms=int(ttl_ms),
                half_life_ms=int(half_life_ms),
            )
            execution_decision = decide_execution_strategy(
                alpha_intent=alpha_intent,
                order=o,
                broker=str(broker or "unknown"),
                base_order_type=str(order_type),
                base_aggressiveness=str(aggressiveness),
                default_latency_ms=int(sim_lat_ms),
                default_chunk_pct=float(sim_chunk_pct),
                default_extra_slippage_bps=float(sim_extra_slip),
                feedback=feedback_snapshot,
            )

            order_type = str(execution_decision.get("order_type") or order_type)
            aggressiveness = str(execution_decision.get("aggressiveness") or aggressiveness)
            sim_lat_ms = int(
                max(
                    25,
                    round(
                        float(sim_lat_ms)
                        * float(execution_decision.get("latency_mult") or 1.0)
                    ),
                )
            )
            sim_chunk_pct = float(
                max(
                    0.05,
                    min(
                        1.0,
                        float(execution_decision.get("chunk_pct") or sim_chunk_pct),
                    ),
                )
            )
            sim_extra_slip = float(
                max(
                    float(sim_extra_slip),
                    float(execution_decision.get("sim_extra_slippage_bps") or 0.0),
                )
            )
            size_scale *= max(
                0.0,
                _safe_float(execution_decision.get("size_mult"), 1.0),
            )
            size_scale = max(0.0, min(1.0, float(size_scale)))

            lob_deeplob_shadow = {
                "enabled": False,
                "shadow_only": True,
                "reason": "disabled",
            }
            if deeplob_shadow_enabled():
                try:
                    lob_deeplob_shadow = shadow_deeplob_execution_signal(
                        con,
                        symbol=str(symbol),
                        side=("BUY" if side == "LONG" else "SELL"),
                        ts_ms=int(signal_ts or now_ms),
                        latency_ms=int(execution_decision.get("expected_fill_latency_ms") or sim_lat_ms),
                    )
                except Exception as e:
                    _warn_nonfatal(
                        "EXECUTION_POLICY_ENGINE_DEEPLOB_SHADOW_FAILED",
                        e,
                        once_key=f"deeplob_shadow:{symbol}",
                        symbol=str(symbol),
                    )
                    lob_deeplob_shadow = {
                        "ok": False,
                        "blocked": True,
                        "shadow_only": True,
                        "reason": f"shadow_signal_failed:{type(e).__name__}",
                    }

            try:
                meta_label_gate = score_order_meta_label(con, o, regime_vec=regime_vec)
            except Exception as e:
                _warn_nonfatal(
                    "EXECUTION_POLICY_ENGINE_META_LABEL_SCORE_FAILED",
                    e,
                    once_key=f"meta_label_score:{symbol}",
                    symbol=str(symbol),
                )
                meta_label_gate = {
                    "enabled": True,
                    "applied": False,
                    "probability": None,
                    "multiplier": 1.0,
                    "reason": f"score_failed:{type(e).__name__}",
                }
            meta_label_mult = max(0.0, min(1.0, _safe_float(meta_label_gate.get("multiplier"), 1.0)))
            size_scale *= float(meta_label_mult)
            size_scale = max(0.0, min(1.0, float(size_scale)))

            try:
                conformal_gate = conformal_gate_from_payload(o)
            except Exception as e:
                _warn_nonfatal(
                    "EXECUTION_POLICY_ENGINE_CONFORMAL_GATE_FAILED",
                    e,
                    once_key=f"conformal_gate:{symbol}",
                    symbol=str(symbol),
                )
                conformal_gate = {
                    "enabled": True,
                    "applied": False,
                    "mode": str(os.environ.get("CONFORMAL_MODE", "log_only") or "log_only"),
                    "hard_block": False,
                    "reason": f"score_failed:{type(e).__name__}",
                }
            if bool(conformal_gate.get("hard_block")):
                _log_suppression_event(
                    o=o,
                    reason="conformal_interval_straddles_zero",
                    decision_json={
                        "blocked_by": "conformal_interval",
                        "scale": 0.0,
                        "conformal_gate": dict(conformal_gate),
                        "meta_label": dict(meta_label_gate),
                        "tse": tse,
                    },
                    execution_policy_json={
                        "conformal_gate": dict(conformal_gate),
                        "meta_label": dict(meta_label_gate),
                        "tse": tse,
                        "scale": 0.0,
                    },
                )
                continue

            try:
                ood_gate = ood_gate_from_payload(o)
            except Exception as e:
                _warn_nonfatal(
                    "EXECUTION_POLICY_ENGINE_OOD_GATE_FAILED",
                    e,
                    once_key=f"ood_gate:{symbol}",
                    symbol=str(symbol),
                )
                ood_gate = {
                    "enabled": True,
                    "applied": False,
                    "mode": str(os.environ.get("OOD_MODE", "log_only") or "log_only"),
                    "multiplier": 1.0,
                    "hard_block": False,
                    "reason": f"score_failed:{type(e).__name__}",
                }
            try:
                uncertainty_gate = uncertainty_gate_from_payload(
                    o,
                    conformal_gate=conformal_gate,
                    ood_gate=ood_gate,
                    execution_mode=str(mode or ""),
                    broker=str(broker or ""),
                    risk_increasing=bool(risk_increasing_order),
                    now_ms=int(now_ms),
                )
            except Exception as e:
                if bool(live_ai_gate.get("required")):
                    _log_suppression_event(
                        o=o,
                        reason=f"live_ai_safety_uncertainty_gate_failed:{type(e).__name__}",
                        decision_json={
                            "blocked_by": "live_ai_safety",
                            "reason": "uncertainty_gate_failed",
                            "error": f"{type(e).__name__}: {e}",
                            "conformal_gate": dict(conformal_gate),
                            "ood_gate": dict(ood_gate),
                        },
                        execution_policy_json={
                            "live_ai_safety": dict(live_ai_gate),
                            "conformal_gate": dict(conformal_gate),
                            "ood_gate": dict(ood_gate),
                        },
                    )
                    continue
                _warn_nonfatal(
                    "EXECUTION_POLICY_ENGINE_UNCERTAINTY_GATE_FAILED",
                    e,
                    once_key=f"uncertainty_gate:{symbol}",
                    symbol=str(symbol),
                )
                uncertainty_gate = {
                    "enabled": True,
                    "applied": False,
                    "mode": str(os.environ.get("UNCERTAINTY_SIZING_MODE", "log_only") or "log_only"),
                    "multiplier": 1.0,
                    "hard_block": False,
                    "reason": f"score_failed:{type(e).__name__}",
                }
            uncertainty_mult = max(0.0, min(1.0, _safe_float(uncertainty_gate.get("multiplier"), 1.0)))
            if bool(uncertainty_gate.get("hard_block")):
                reason_code = str(uncertainty_gate.get("reason") or "hard_block").strip() or "hard_block"
                _log_suppression_event(
                    o=o,
                    reason=f"uncertainty_{reason_code}",
                    decision_json={
                        "blocked_by": "uncertainty",
                        "scale": 0.0,
                        "uncertainty_gate": dict(uncertainty_gate),
                        "conformal_gate": dict(conformal_gate),
                        "ood_gate": dict(ood_gate),
                        "meta_label": dict(meta_label_gate),
                        "tse": tse,
                    },
                    execution_policy_json={
                        "uncertainty_gate": dict(uncertainty_gate),
                        "conformal_gate": dict(conformal_gate),
                        "ood_gate": dict(ood_gate),
                        "meta_label": dict(meta_label_gate),
                        "tse": tse,
                        "scale": 0.0,
                    },
                )
                continue
            ood_mult = max(0.0, min(1.0, _safe_float(ood_gate.get("multiplier"), 1.0)))
            if bool(ood_gate.get("hard_block")):
                _log_suppression_event(
                    o=o,
                    reason="ood_hard_block",
                    decision_json={
                        "blocked_by": "ood_hard_block",
                        "scale": 0.0,
                        "ood_gate": dict(ood_gate),
                        "meta_label": dict(meta_label_gate),
                        "tse": tse,
                    },
                    execution_policy_json={
                        "ood_gate": dict(ood_gate),
                        "meta_label": dict(meta_label_gate),
                        "tse": tse,
                        "scale": 0.0,
                    },
                )
                continue
            size_scale *= float(uncertainty_mult)
            size_scale = max(0.0, min(1.0, float(size_scale)))
            size_scale *= float(ood_mult)
            size_scale = max(0.0, min(1.0, float(size_scale)))

            shaped_order = _scale_order_fields(o, size_scale)
            effective_abs_qty = _effective_abs_qty(shaped_order)
            if effective_abs_qty <= float(TSE_MIN_ABS_QTY):
                if bool(meta_label_gate.get("applied")) and float(meta_label_mult) <= 0.0:
                    original_qty = (
                        _safe_float(o.get("qty"), 0.0)
                        if "qty" in o and o.get("qty") is not None
                        else _safe_float(o.get("to_weight"), 0.0)
                    )
                    compressed_qty = (
                        _safe_float(shaped_order.get("qty"), 0.0)
                        if "qty" in shaped_order and shaped_order.get("qty") is not None
                        else _safe_float(shaped_order.get("to_weight"), 0.0)
                    )
                    meta = {
                        "original_qty": float(original_qty),
                        "compressed_qty": float(compressed_qty),
                    }
                    _log_suppression_event(
                        o=o,
                        reason="meta_label_size_compression_scaled_to_zero",
                        decision_json={
                            "blocked_by": "meta_label_size_compression",
                            "scale": size_scale,
                            "meta_label": dict(meta_label_gate),
                            "meta": meta,
                        },
                        execution_policy_json={"meta_label": dict(meta_label_gate), "scale": size_scale, "meta": meta},
                    )
                    continue
                if bool(ood_gate.get("applied")) and float(ood_mult) <= 0.0:
                    original_qty = (
                        _safe_float(o.get("qty"), 0.0)
                        if "qty" in o and o.get("qty") is not None
                        else _safe_float(o.get("to_weight"), 0.0)
                    )
                    compressed_qty = (
                        _safe_float(shaped_order.get("qty"), 0.0)
                        if "qty" in shaped_order and shaped_order.get("qty") is not None
                        else _safe_float(shaped_order.get("to_weight"), 0.0)
                    )
                    meta = {
                        "original_qty": float(original_qty),
                        "compressed_qty": float(compressed_qty),
                    }
                    _log_suppression_event(
                        o=o,
                        reason="ood_size_compression_scaled_to_zero",
                        decision_json={
                            "blocked_by": "ood_size_compression",
                            "scale": size_scale,
                            "ood_gate": dict(ood_gate),
                            "meta": meta,
                        },
                        execution_policy_json={"ood_gate": dict(ood_gate), "scale": size_scale, "meta": meta},
                    )
                    continue
                if bool(uncertainty_gate.get("applied")) and float(uncertainty_mult) <= 0.0:
                    original_qty = (
                        _safe_float(o.get("qty"), 0.0)
                        if "qty" in o and o.get("qty") is not None
                        else _safe_float(o.get("to_weight"), 0.0)
                    )
                    compressed_qty = (
                        _safe_float(shaped_order.get("qty"), 0.0)
                        if "qty" in shaped_order and shaped_order.get("qty") is not None
                        else _safe_float(shaped_order.get("to_weight"), 0.0)
                    )
                    meta = {
                        "original_qty": float(original_qty),
                        "compressed_qty": float(compressed_qty),
                    }
                    _log_suppression_event(
                        o=o,
                        reason="uncertainty_size_compression_scaled_to_zero",
                        decision_json={
                            "blocked_by": "uncertainty_size_compression",
                            "scale": size_scale,
                            "uncertainty_gate": dict(uncertainty_gate),
                            "meta": meta,
                        },
                        execution_policy_json={
                            "uncertainty_gate": dict(uncertainty_gate),
                            "scale": size_scale,
                            "meta": meta,
                        },
                    )
                    continue
                if tse_action == "SIZE_COMPRESSION":
                    original_qty = (
                        _safe_float(o.get("qty"), 0.0)
                        if "qty" in o and o.get("qty") is not None
                        else _safe_float(o.get("to_weight"), 0.0)
                    )
                    compressed_qty = (
                        _safe_float(shaped_order.get("qty"), 0.0)
                        if "qty" in shaped_order and shaped_order.get("qty") is not None
                        else _safe_float(shaped_order.get("to_weight"), 0.0)
                    )
                    meta = {
                        "original_qty": float(original_qty),
                        "compressed_qty": float(compressed_qty),
                    }
                    _log_suppression_event(
                        o=o,
                        reason="tse_size_compression_scaled_to_zero",
                        decision_json={
                            "blocked_by": "tse_size_compression",
                            "scale": size_scale,
                            "tse": tse,
                            "meta": meta,
                        },
                        execution_policy_json={"tse": tse, "scale": size_scale, "meta": meta},
                    )
                    continue
                _log_suppression_event(
                    o=o,
                    reason=f"tse_scaled_to_zero:{str(tse.get('reason') or '')}",
                    decision_json={"blocked_by": "tse_scale", "scale": size_scale, "tse": tse},
                    execution_policy_json={"tse": tse, "scale": size_scale},
                )
                continue

            signed_qty = _signed_qty_or_weight(shaped_order, side)
            if abs(signed_qty) <= float(TSE_MIN_ABS_QTY):
                _log_suppression_event(
                    o=o,
                    reason="no_effective_qty_after_scaling",
                    decision_json={"blocked_by": "epe", "scale": size_scale},
                    execution_policy_json={"tse": tse, "scale": size_scale},
                )
                continue

            compat = float(regime_comp)
            decision_blob = {
                "model_id": str(o.get("model_id") or "baseline"),
                "alpha_remaining": float(alpha_rem),
                "order_type": str(order_type),
                "aggressiveness": str(aggressiveness),
                "execution_mode": (execution_mode_fn or get_execution_mode)(),
                "regime_model_version": str(regime_model_version()),
                "regime_compat": float(regime_comp),
                "tse": tse,
                "size_scale": float(size_scale),
                "actor": str(actor or "system"),
                "mode": str(mode or "unknown"),
                "broker": str(broker or "unknown"),
                "portfolio_orders_batch_id": portfolio_orders_batch_id,
                "portfolio_orders_id": portfolio_orders_id,
                "learned_alpha": dict(learned_alpha_gate),
                "alpha_intent": dict(alpha_intent),
                "execution_decision": dict(execution_decision),
                "execution_feedback": dict(feedback_snapshot or {}),
                "lob_deeplob_shadow": dict(lob_deeplob_shadow or {}),
                "meta_label": dict(meta_label_gate),
                "conformal_gate": dict(conformal_gate),
                "ood_gate": dict(ood_gate),
                "uncertainty_gate": dict(uncertainty_gate),
            }
            ope_payload = _ope_payload_from_order(
                o,
                shaped_order,
                side=str(side),
                signed_qty=float(signed_qty),
            )
            if ope_payload:
                decision_blob["ope"] = dict(ope_payload)

            if "qty" in shaped_order and shaped_order.get("qty") is not None:
                slice_pct = 0.15 if volatility > 0.03 else 0.25
                if capital_mode == "preserve":
                    slice_pct = min(slice_pct, float(EPE_CAPITAL_PRESERVE_CHUNK_PCT))

                learned_execution = {
                    "enabled": False,
                    "applied": False,
                    "reason": "disabled",
                }
                learned_decision = None
                learned_constraints = None
                parent_id = str(
                    shaped_order.get("client_order_id")
                    or shaped_order.get("portfolio_orders_id")
                    or shaped_order.get("source_order_id")
                    or f"{symbol}:{portfolio_orders_batch_id or portfolio_orders_id or 'policy'}:{now_ms}"
                )
                base_entry_delay_ms = int(execution_decision.get("entry_delay_ms") or 0)
                base_slice_interval_ms = _safe_int(
                    o.get("slice_interval_ms") or o.get("interval_ms") or os.environ.get("EXEC_SLICE_INTERVAL_MS", "250"),
                    250,
                )
                base_participation = _safe_float(
                    o.get("target_participation")
                    or o.get("pov_participation")
                    or o.get("participation_rate")
                    or os.environ.get("EXEC_POV_PARTICIPATION", "0.03"),
                    0.03,
                )
                learned_slice_pct = float(slice_pct)
                learned_entry_delay_ms = int(base_entry_delay_ms)
                learned_slice_interval_ms = int(max(0, base_slice_interval_ms))
                learned_target_participation = float(base_participation)
                learned_requested = bool(learned_execution_enabled(o))
                learned_ope_gate: Dict[str, Any] = {}
                if learned_requested:
                    try:
                        learned_ope_ok, learned_ope_gate = _learned_execution_ope_gate(
                            con,
                            o,
                            symbol=str(symbol),
                        )
                    except Exception as e:
                        _warn_nonfatal(
                            "EXECUTION_POLICY_ENGINE_LEARNED_EXECUTION_OPE_FAILED",
                            e,
                            once_key=f"learned_execution_ope:{symbol}",
                            symbol=str(symbol),
                        )
                        learned_ope_ok = False
                        learned_ope_gate = {
                            "applied": True,
                            "passed": False,
                            "status": f"ope_gate_error:{type(e).__name__}",
                        }
                    if not bool(learned_ope_ok):
                        learned_execution = {
                            "enabled": True,
                            "applied": False,
                            "reason": "ope_gate_blocked",
                            "ope_gate": dict(learned_ope_gate or {}),
                        }

                if learned_requested and bool(learned_ope_gate.get("passed")):
                    try:
                        learned_constraints = build_learned_execution_constraints(
                            order=o,
                            symbol=str(symbol),
                            side=str(side),
                            parent_qty=float(shaped_order.get("qty") or 0.0),
                            parent_id=str(parent_id),
                            base_slice_pct=float(slice_pct),
                            base_participation=float(base_participation),
                            base_slice_interval_ms=int(base_slice_interval_ms),
                            base_entry_delay_ms=int(base_entry_delay_ms),
                            max_slices=int(TSE_MAX_SLICES),
                        )
                        learned_context = build_learned_execution_context(
                            order={
                                **dict(o),
                                "epe_alpha_remaining": float(alpha_rem),
                                "true_spread_bps": o.get("true_spread_bps", shaped_order.get("true_spread_bps")),
                                "intraday_vol_bps": o.get("intraday_vol_bps", shaped_order.get("intraday_vol_bps")),
                            },
                            feedback=feedback_snapshot,
                            execution_decision=execution_decision,
                            extra={
                                "alpha_remaining": float(alpha_rem),
                                "capital_mode": 1.0 if capital_mode == "preserve" else 0.0,
                            },
                        )
                        learned_decision = select_learned_execution_adjustment(
                            context=learned_context,
                            constraints=learned_constraints,
                        )
                        learned_slice_pct = float(learned_decision.parameters["slice_pct"])
                        learned_entry_delay_ms = int(learned_decision.parameters["entry_delay_ms"])
                        learned_slice_interval_ms = int(learned_decision.parameters["slice_interval_ms"])
                        learned_target_participation = float(learned_decision.parameters["target_participation"])
                        learned_execution = {
                            "enabled": True,
                            "applied": True,
                            "policy": str(learned_decision.policy_name),
                            "action_id": str(learned_decision.action_id),
                            "parameters": dict(learned_decision.parameters),
                            "constraints": learned_constraints.as_guard(),
                            "context": dict(learned_context),
                            "ope_gate": dict(learned_ope_gate or {}),
                        }
                    except LearnedExecutionPolicyViolation as e:
                        _log_suppression_event(
                            o=o,
                            reason="learned_execution_policy_violation",
                            decision_json={
                                "blocked_by": "learned_execution_policy",
                                "reason": str(e),
                            },
                            execution_policy_json={"learned_execution": {"enabled": True, "error": str(e)}},
                        )
                        continue
                    except Exception as e:
                        _warn_nonfatal(
                            "EXECUTION_POLICY_ENGINE_LEARNED_EXECUTION_FAILED",
                            e,
                            once_key=f"learned_execution:{symbol}",
                            symbol=str(symbol),
                        )
                        learned_execution = {
                            "enabled": True,
                            "applied": False,
                            "reason": f"fallback_baseline:{type(e).__name__}",
                        }

                slice_qtys = _build_slice_qtys(
                    float(shaped_order.get("qty") or 0.0),
                    float(learned_slice_pct),
                    int(TSE_MAX_SLICES),
                )
                if not slice_qtys:
                    slice_qtys = [float(shaped_order.get("qty") or 0.0)]
                slices = len(slice_qtys)
                slice_qty = abs(float(slice_qtys[0] or 0.0))

                for slice_index, slice_signed_qty in enumerate(slice_qtys):
                    row = dict(shaped_order)
                    row["qty"] = float(slice_signed_qty)
                    row["order_type"] = str(order_type)
                    row["aggressiveness"] = str(aggressiveness)
                    row["cancel_replace"] = True
                    row["max_reprice_attempts"] = 3
                    row["epe_alpha_remaining"] = float(alpha_rem)
                    row["regime_compatibility"] = float(regime_comp)
                    row["tse_state"] = str(tse.get("state") or "NONE")
                    row["tse_action"] = str(tse.get("action") or "NONE")
                    row["tse_size_mult"] = _safe_float(tse.get("size_mult"), 1.0)
                    row["tse_reason"] = str(tse.get("reason") or "")
                    row["capital_mode"] = str(capital_mode)
                    row["capital_preservation_snapshot"] = dict(cpm_snapshot or {})
                    row["learned_alpha_decay"] = dict(learned_alpha_gate)
                    row["learned_alpha_size_mult"] = float(learned_alpha_size_mult)
                    row["alpha_intent"] = dict(alpha_intent)
                    row["execution_policy"] = str(execution_decision.get("execution_policy") or "balanced")
                    row["execution_policy_locked"] = 1
                    row["entry_strategy"] = str(execution_decision.get("entry_strategy") or "working_limit")
                    row["entry_delay_ms"] = int(learned_entry_delay_ms)
                    row["slice_interval_ms"] = int(learned_slice_interval_ms)
                    row["target_participation"] = float(learned_target_participation)
                    row["expected_slippage_bps"] = float(execution_decision.get("expected_slippage_bps") or 0.0)
                    row["expected_fill_latency_ms"] = int(execution_decision.get("expected_fill_latency_ms") or sim_lat_ms)
                    row["slippage_size_mult"] = _safe_float(execution_decision.get("size_mult"), 1.0)
                    row["meta_label_probability"] = meta_label_gate.get("probability")
                    row["meta_label_size_mult"] = float(meta_label_mult)
                    row["meta_label_gate"] = dict(meta_label_gate)
                    row["conformal_interval_excludes_zero"] = conformal_gate.get("interval_excludes_zero")
                    row["conformal_size_mult"] = _safe_float(conformal_gate.get("size_mult"), 1.0)
                    row["conformal_gate"] = dict(conformal_gate)
                    row["ood_score"] = ood_gate.get("ood_score")
                    row["ood_size_mult"] = float(ood_mult)
                    row["ood_gate"] = dict(ood_gate)
                    row["uncertainty_size_mult"] = float(uncertainty_mult)
                    row["uncertainty_action"] = str(uncertainty_gate.get("action") or "NONE")
                    row["uncertainty_gate"] = dict(uncertainty_gate)
                    row["execution_feedback_snapshot"] = dict(feedback_snapshot or {})
                    row["lob_deeplob_shadow"] = dict(lob_deeplob_shadow or {})
                    row["entry_limit_offset_bps"] = float(execution_decision.get("limit_offset_bps") or 0.0)
                    row["epe_broker_sim_overrides"] = {
                        "latency_ms": int(sim_lat_ms),
                        "chunk_pct": float(sim_chunk_pct),
                        "extra_slippage_bps": float(sim_extra_slip),
                    }
                    row["learned_execution"] = dict(learned_execution)
                    if learned_decision is not None and learned_constraints is not None:
                        row.update(
                            learned_execution_metadata_for_order(
                                decision=learned_decision,
                                constraints=learned_constraints,
                                slice_index=int(slice_index),
                                slice_count=int(slices),
                            )
                        )
                    shaped.append(row)

                policy_json = dict(decision_blob)
                policy_json.update(
                    {
                        "slice_pct": float(learned_slice_pct),
                        "slice_qty": float(slice_qty),
                        "slices": int(slices),
                        "regime_vector": regime_vec,
                        "learned_execution": dict(learned_execution),
                    }
                )

                append_chain_row(
                    "execution_policy_audit",
                    {
                        "ts_ms": int(now_ms),
                        "signal_id": str(o.get("signal_id") or ""),
                        "model_id": str(o.get("model_id") or "baseline"),
                        "symbol": str(symbol),
                        "side": str(side),
                        "qty": float(shaped_order.get("qty") or 0.0),
                        "age_ms": int(age_ms),
                        "ttl_ms": int(ttl_ms),
                        "volatility": float(volatility),
                        "regime_compat": float(compat),
                        "source_order_id": int(o.get("source_order_id") or 0),
                        "policy_json": policy_json,
                        "source_alert_id": (_safe_int(o.get("source_alert_id"), 0) if o.get("source_alert_id") is not None else None),
                        "decision_json": decision_blob,
                        "actor": str(actor or "system"),
                        "mode": str(mode or "unknown"),
                        "broker": str(broker or "unknown"),
                        "portfolio_orders_batch_id": (int(portfolio_orders_batch_id) if portfolio_orders_batch_id is not None else (int(portfolio_orders_id) if portfolio_orders_id is not None else None)),
                        "suppression_state": str(tse.get("state") or "NONE"),
                    },
                    con,
                )
                continue

            row = dict(shaped_order)
            row["to_side"] = str(side)
            row["order_type"] = str(order_type)
            row["aggressiveness"] = str(aggressiveness)
            row["cancel_replace"] = True
            row["max_reprice_attempts"] = 3
            row["epe_alpha_remaining"] = float(alpha_rem)
            row["regime_compatibility"] = float(regime_comp)
            row["tse_state"] = str(tse.get("state") or "NONE")
            row["tse_action"] = str(tse.get("action") or "NONE")
            row["tse_size_mult"] = _safe_float(tse.get("size_mult"), 1.0)
            row["tse_reason"] = str(tse.get("reason") or "")
            row["capital_mode"] = str(capital_mode)
            row["capital_preservation_snapshot"] = dict(cpm_snapshot or {})
            row["learned_alpha_decay"] = dict(learned_alpha_gate)
            row["learned_alpha_size_mult"] = float(learned_alpha_size_mult)
            row["alpha_intent"] = dict(alpha_intent)
            row["execution_policy"] = str(execution_decision.get("execution_policy") or "balanced")
            row["execution_policy_locked"] = 1
            row["entry_strategy"] = str(execution_decision.get("entry_strategy") or "working_limit")
            row["entry_delay_ms"] = int(execution_decision.get("entry_delay_ms") or 0)
            row["expected_slippage_bps"] = float(execution_decision.get("expected_slippage_bps") or 0.0)
            row["expected_fill_latency_ms"] = int(execution_decision.get("expected_fill_latency_ms") or sim_lat_ms)
            row["slippage_size_mult"] = _safe_float(execution_decision.get("size_mult"), 1.0)
            row["meta_label_probability"] = meta_label_gate.get("probability")
            row["meta_label_size_mult"] = float(meta_label_mult)
            row["meta_label_gate"] = dict(meta_label_gate)
            row["conformal_interval_excludes_zero"] = conformal_gate.get("interval_excludes_zero")
            row["conformal_size_mult"] = _safe_float(conformal_gate.get("size_mult"), 1.0)
            row["conformal_gate"] = dict(conformal_gate)
            row["ood_score"] = ood_gate.get("ood_score")
            row["ood_size_mult"] = float(ood_mult)
            row["ood_gate"] = dict(ood_gate)
            row["uncertainty_size_mult"] = float(uncertainty_mult)
            row["uncertainty_action"] = str(uncertainty_gate.get("action") or "NONE")
            row["uncertainty_gate"] = dict(uncertainty_gate)
            row["execution_feedback_snapshot"] = dict(feedback_snapshot or {})
            row["lob_deeplob_shadow"] = dict(lob_deeplob_shadow or {})
            row["entry_limit_offset_bps"] = float(execution_decision.get("limit_offset_bps") or 0.0)
            row["epe_broker_sim_overrides"] = {
                "latency_ms": int(sim_lat_ms),
                "chunk_pct": float(sim_chunk_pct),
                "extra_slippage_bps": float(sim_extra_slip),
            }
            shaped.append(row)

            policy_json = dict(decision_blob)
            policy_json.update(
                {
                    "slice_pct": 1.0,
                    "slice_qty": float(_effective_abs_qty(row)),
                    "slices": 1,
                    "regime_vector": regime_vec,
                }
            )

            append_chain_row(
                "execution_policy_audit",
                {
                    "ts_ms": int(now_ms),
                    "signal_id": str(o.get("signal_id") or ""),
                    "model_id": str(o.get("model_id") or "baseline"),
                    "symbol": str(symbol),
                    "side": str(side),
                    "qty": float(signed_qty),
                    "age_ms": int(age_ms),
                    "ttl_ms": int(ttl_ms),
                    "volatility": float(volatility),
                    "regime_compat": float(compat),
                    "source_order_id": int(o.get("source_order_id") or 0),
                    "policy_json": policy_json,
                    "source_alert_id": (_safe_int(o.get("source_alert_id"), 0) if o.get("source_alert_id") is not None else None),
                    "decision_json": decision_blob,
                    "actor": str(actor or "system"),
                    "mode": str(mode or "unknown"),
                    "broker": str(broker or "unknown"),
                    "portfolio_orders_batch_id": (int(portfolio_orders_batch_id) if portfolio_orders_batch_id is not None else (int(portfolio_orders_id) if portfolio_orders_id is not None else None)),
                    "suppression_state": str(tse.get("state") or "NONE"),
                },
                con,
            )

        con.commit()
        return shaped

    finally:
        if owns:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal("EXECUTION_POLICY_ENGINE_CONNECTION_CLOSE_FAILED", e, once_key="apply_execution_policy_close", scope="apply_execution_policy")
