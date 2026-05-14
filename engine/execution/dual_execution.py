# FILE: dev_core/dual_execution.py
# NEW FILE (CREATE):

# dev_core/dual_execution.py
"""
Dual execution orchestration:

- Always runs paper sim (broker_sim) first (authoritative expected positions)
- Optionally runs live broker execution (IBKR) second
- Pulls live positions snapshot
- Computes divergence vs sim positions and enforces auto-disable if above threshold

This is designed to be called from broker_apply_orders.py when:
  - execution_mode == live AND armed=1
  - EXECUTION_DUAL_ENABLE=1

Tables (created idempotently):
  execution_divergence(ts_ms, broker, divergence, details_json)
"""

import json
import logging
import os
import time
from typing import Any, Dict, Optional, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import connect
from engine.execution import broker_sim
from engine.execution import broker_ibkr_gateway
from engine.execution.execution_mode import set_execution_mode, set_execution_armed


def _now_ms() -> int:
    return int(time.time() * 1000)


LOGGER = logging.getLogger(__name__)
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(event: str, code: str, error: BaseException, *, warn_key: str | None = None, **extra: Any) -> None:
    if warn_key and warn_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOGGER,
        event=event,
        code=code,
        message=event,
        error=error,
        level=logging.WARNING,
        component=__name__,
        extra=extra or None,
        persist=False,
    )
    if warn_key:
        _WARNED_NONFATAL_KEYS.add(warn_key)


def _ensure_schema(con) -> None:
    # Divergence history is persisted so operators can review when and why
    # dual execution auto-demoted live mode.
    con.executescript(
        """
CREATE TABLE IF NOT EXISTS execution_divergence (
  ts_ms INTEGER NOT NULL,
  broker TEXT NOT NULL,
  divergence REAL NOT NULL,
  details_json TEXT,
  PRIMARY KEY (ts_ms, broker)
);

CREATE INDEX IF NOT EXISTS idx_execution_divergence_ts
  ON execution_divergence(ts_ms);
"""
    )


def _read_sim_positions(con) -> Dict[str, float]:
    out: Dict[str, float] = {}
    try:
        rows = con.execute("SELECT symbol, qty FROM broker_positions").fetchall() or []
        for sym, qty in rows:
            try:
                out[str(sym)] = float(qty or 0.0)
            except Exception as e:
                _warn_nonfatal(
                    "dual_execution_sim_position_parse_failed",
                    "DUAL_EXECUTION_SIM_POSITION_PARSE_FAILED",
                    e,
                    warn_key="dual_execution_sim_position_parse_failed",
                    symbol=str(sym),
                    qty=qty,
                )
    except Exception as e:
        _warn_nonfatal(
            "dual_execution_sim_positions_read_failed",
            "DUAL_EXECUTION_SIM_POSITIONS_READ_FAILED",
            e,
            warn_key="dual_execution_sim_positions_read_failed",
        )
    return out


def _divergence(sim_pos: Dict[str, float], live_pos: Dict[str, float]) -> Tuple[float, Dict[str, Any]]:
    # Divergence is normalized by simulated exposure so small books do not look
    # healthy merely because absolute qty differences are small.
    syms = set(sim_pos.keys()) | set(live_pos.keys())
    num = 0.0
    den = 0.0
    top = []
    for s in syms:
        a = float(sim_pos.get(s, 0.0) or 0.0)
        b = float(live_pos.get(s, 0.0) or 0.0)
        d = abs(b - a)
        num += d
        den += abs(a)
        if d > 0:
            top.append((d, s, a, b))
    top.sort(reverse=True)
    det = {
        "num_abs_qty_diff": num,
        "den_abs_sim_qty": den,
        "top": [{"symbol": s, "sim_qty": a, "live_qty": b, "abs_diff": d} for (d, s, a, b) in top[:25]],
        "n_syms": len(syms),
    }
    div = float(num / (den + 1e-9))
    return div, det


def check_dual_divergence(con, ts_ms: int, exec_result: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    _ensure_schema(con)

    sim_pos = _read_sim_positions(con)
    live_pos = broker_ibkr_gateway.get_positions_snapshot(
        timeout_s=float(os.environ.get("IBKR_POS_SNAPSHOT_TIMEOUT_S", "10"))
    )
    div, det = _divergence(sim_pos, live_pos)

    con.execute(
        "INSERT OR REPLACE INTO execution_divergence(ts_ms, broker, divergence, details_json) VALUES (?,?,?,?)",
        (
            int(ts_ms or _now_ms()),
            "ibkr",
            float(div),
            json.dumps(
                {
                    "detail": det,
                    "exec_result": exec_result or {},
                }
            ),
        ),
    )

    actions = []
    alert_th = float(os.environ.get("EXECUTION_DIVERGENCE_ALERT_TH", "0.10"))
    disable_th = float(os.environ.get("EXECUTION_DIVERGENCE_DISABLE_TH", "0.25"))

    if div >= disable_th:
        try:
            set_execution_armed(0, actor="dual_execution", reason=f"divergence_disable:{div:.4f}", con=con)
        except Exception as e:
            _warn_nonfatal(
                "dual_execution_disable_arm_failed",
                "DUAL_EXECUTION_DISABLE_ARM_FAILED",
                e,
                warn_key="dual_execution_disable_arm_failed",
                divergence=float(div),
            )
        try:
            set_execution_mode("paper", actor="dual_execution", reason=f"divergence_disable:{div:.4f}", con=con)
        except Exception as e:
            _warn_nonfatal(
                "dual_execution_disable_mode_switch_failed",
                "DUAL_EXECUTION_DISABLE_MODE_SWITCH_FAILED",
                e,
                warn_key="dual_execution_disable_mode_switch_failed",
                divergence=float(div),
            )
        actions.append({"action": "live_disabled", "reason": "divergence", "divergence": float(div)})
    elif div >= alert_th:
        actions.append({"action": "divergence_alert", "divergence": float(div)})

    return {
        "ok": True,
        "broker": "ibkr",
        "ts_ms": int(ts_ms or _now_ms()),
        "divergence": float(div),
        "detail": det,
        "actions": actions,
    }


def apply_latest_portfolio_orders_dual_ibkr(dry_run_live: bool = False) -> Dict[str, Any]:
    """
    Runs:
      1) paper sim apply_new_portfolio_orders (authoritative expected)
      2) optional live IBKR order apply (unless dry_run_live)
      3) positions snapshot + divergence detect
      4) optional auto-disable live on divergence threshold

    Returns dict with keys:
      ok, sim, live, divergence, actions
    """
    return apply_portfolio_orders_dual_ibkr(
        dry_run_live=dry_run_live,
        override_orders=None,
        override_order_id=None,
        override_ts_ms=None,
    )


def apply_portfolio_orders_dual_ibkr(
    *,
    dry_run_live: bool = False,
    override_orders: Optional[list] = None,
    override_order_id: Optional[int] = None,
    override_ts_ms: Optional[int] = None,
) -> Dict[str, Any]:
    con = connect()
    try:
        _ensure_schema(con)

        # Sim runs first so live divergence is measured against a deterministic
        # expected portfolio state generated from the same intent batch.
        # 1) paper sim (always)
        sim_res = broker_sim.apply_new_portfolio_orders(
            dry_run=False,
            override_orders=(list(override_orders or []) if override_orders is not None else None),
            override_order_id=override_order_id,
            override_ts_ms=override_ts_ms,
        )

        # 2) live IBKR
        live_res: Dict[str, Any] = {"ok": True, "status": "skipped"}
        if not dry_run_live:
            live_res = broker_ibkr_gateway.apply_latest_portfolio_orders_live(
                dry_run=False,
                override_orders=(list(override_orders or []) if override_orders is not None else None),
                override_order_id=override_order_id,
                override_ts_ms=override_ts_ms,
            )

        # 3) divergence
        sim_pos = _read_sim_positions(con)
        live_pos = broker_ibkr_gateway.get_positions_snapshot(timeout_s=float(os.environ.get("IBKR_POS_SNAPSHOT_TIMEOUT_S", "10")))
        div, det = _divergence(sim_pos, live_pos)

        con.execute(
            "INSERT OR REPLACE INTO execution_divergence(ts_ms, broker, divergence, details_json) VALUES (?,?,?,?)",
            (_now_ms(), "ibkr", float(div), json.dumps({"detail": det, "live_ok": bool((live_res or {}).get("ok", True))})),
        )

        actions = []
        alert_th = float(os.environ.get("EXECUTION_DIVERGENCE_ALERT_TH", "0.10"))
        disable_th = float(os.environ.get("EXECUTION_DIVERGENCE_DISABLE_TH", "0.25"))

        if div >= disable_th:
            # Hard safety: disarm + mode paper
            try:
                set_execution_armed(0, actor="dual_execution", reason=f"divergence_disable:{div:.4f}", con=con)
            except Exception as e:
                _warn_nonfatal(
                    "dual_execution_apply_disable_arm_failed",
                    "DUAL_EXECUTION_APPLY_DISABLE_ARM_FAILED",
                    e,
                    warn_key="dual_execution_apply_disable_arm_failed",
                    divergence=float(div),
                )
            try:
                set_execution_mode("paper", actor="dual_execution", reason=f"divergence_disable:{div:.4f}", con=con)
            except Exception as e:
                _warn_nonfatal(
                    "dual_execution_apply_disable_mode_switch_failed",
                    "DUAL_EXECUTION_APPLY_DISABLE_MODE_SWITCH_FAILED",
                    e,
                    warn_key="dual_execution_apply_disable_mode_switch_failed",
                    divergence=float(div),
                )
            actions.append({"action": "live_disabled", "reason": "divergence", "divergence": float(div)})

        elif div >= alert_th:
            actions.append({"action": "divergence_alert", "divergence": float(div)})

        return {
            "ok": True,
            "mode": "dual_ibkr",
            "sim": sim_res,
            "live": live_res,
            "divergence": float(div),
            "actions": actions,
        }
    finally:
        try:
            con.commit()
        except Exception as e:
            _warn_nonfatal(
                "dual_execution_commit_failed",
                "DUAL_EXECUTION_COMMIT_FAILED",
                e,
                warn_key="dual_execution_commit_failed",
            )
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal(
                "dual_execution_close_failed",
                "DUAL_EXECUTION_CLOSE_FAILED",
                e,
                warn_key="dual_execution_close_failed",
            )
