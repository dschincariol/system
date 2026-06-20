"""
FILE: rules_engine.py

Applies hard trading rules and kill-switch logic from realized drawdown, drift,
and execution quality. This is the fail-closed guardrail layer above normal
portfolio/risk logic.
"""

import logging
import os
import time
from typing import Any, Dict

from engine.execution.kill_switch import activate_owned, clear_owned
from engine.strategy.drawdown_state import evaluate_current_drawdown
from engine.execution.exec_stats import get_exec_winrate_global, get_exec_stats_by_symbol
from engine.strategy.drift_utils import get_max_drift_ratio, get_symbol_max_drift_ratio
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import _table_exists, connect

LOG = logging.getLogger("rules_engine")

USE = os.environ.get("RULES_ENGINE_ENABLED", "1") == "1"

MAX_DD = float(os.environ.get("RULES_MAX_DRAWDOWN", "0.10"))
MAX_DRIFT = float(os.environ.get("RULES_MAX_DRIFT_RATIO", "2.5"))
MIN_GLOBAL_WINRATE = float(os.environ.get("RULES_MIN_EXEC_WINRATE", "0.45"))
EXEC_LOOKBACK_DAYS = int(os.environ.get("RULES_EXEC_LOOKBACK_DAYS", "30"))

# ------            -- ------------------------------------------------------
# Realized execution cost spike (from fills)
# ------            -- ------------------------------------------------------
EXEC_COST_SPIKE_BPS = float(os.environ.get("RULES_EXEC_COST_SPIKE_BPS", "35.0"))
EXEC_COST_SPIKE_WINDOW_S = int(os.environ.get("RULES_EXEC_COST_SPIKE_WINDOW_S", "180"))
EXEC_COST_SPIKE_MIN_N = int(os.environ.get("RULES_EXEC_COST_SPIKE_MIN_N", "10"))
EXEC_COST_SPIKE_PCTL = float(os.environ.get("RULES_EXEC_COST_SPIKE_PCTL", "80"))
EXEC_COST_SPIKE_COOLDOWN_S = int(os.environ.get("RULES_EXEC_COST_SPIKE_COOLDOWN_S", "300"))

# ------            -- ------------------------------------------------------
# Execution cost spike (spread proxy)
# ------            -- ------------------------------------------------------
COST_SPIKE_BPS = float(os.environ.get("RULES_COST_SPIKE_BPS", "45.0"))
COST_SPIKE_WINDOW_S = int(os.environ.get("RULES_COST_SPIKE_WINDOW_S", "120"))
COST_SPIKE_MIN_N = int(os.environ.get("RULES_COST_SPIKE_MIN_N", "20"))
COST_SPIKE_SYMBOL_LIMIT = int(os.environ.get("RULES_COST_SPIKE_SYMBOL_LIMIT", "50"))
COST_SPIKE_COOLDOWN_S = int(os.environ.get("RULES_COST_SPIKE_COOLDOWN_S", "300"))

SYM_MIN_N = int(os.environ.get("RULES_SYMBOL_MIN_N", "40"))
SYM_MIN_WINRATE = float(os.environ.get("RULES_SYMBOL_MIN_WINRATE", "0.40"))
SYM_MIN_AVG_NET_Z = float(os.environ.get("RULES_SYMBOL_MIN_AVG_NET_Z", "-0.05"))
SYM_MAX_DRIFT = float(os.environ.get("RULES_SYMBOL_MAX_DRIFT_RATIO", "3.0"))

RULES_ACTOR = "rules_engine"
RULES_GLOBAL_KEYS = {
    "cost_spike": "rules:cost_spike",
    "drawdown": "rules:drawdown",
    "drift": "rules:drift",
    "exec_winrate": "rules:exec_winrate",
}
RULES_SYMBOL_TRIGGER = "symbol"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(event: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOG,
        event=event,
        code=event,
        message=event,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.rules_engine",
        extra=extra,
        persist=False,
    )


def _live_mode_requested(con=None) -> bool | None:
    for name in ("EXECUTION_MODE", "ENGINE_MODE", "OPERATOR_MODE", "MODE"):
        if str(os.environ.get(name, "") or "").strip().lower() == "live":
            return True
    if con is None:
        return False
    try:
        if not _table_exists(con, "execution_mode"):
            return False
        mode_row = con.execute("SELECT mode FROM execution_mode WHERE id=1").fetchone()
        return bool(mode_row and str(mode_row[0] or "").strip().lower() == "live")
    except Exception as e:
        _warn_nonfatal("rules_engine_live_mode_check_failed", e)
        return None


def _env_truthy(name: str) -> bool:
    return str(os.environ.get(name, "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _production_like_requested() -> bool:
    env = str(os.environ.get("ENV") or os.environ.get("NODE_ENV") or os.environ.get("APP_ENV") or "").strip().lower()
    return bool(env in {"prod", "production"} or _env_truthy("PROD_LOCK") or _env_truthy("ENGINE_SUPERVISED"))


def _rules_auto_resume_enabled(con=None) -> bool:
    return _env_truthy("RULES_AUTO_RESUME")


def _rules_meta(trigger: str, payload: Dict[str, Any] | None = None, *, con=None) -> Dict[str, Any]:
    meta = dict(payload or {})
    meta["actor"] = RULES_ACTOR
    meta["trigger"] = str(trigger)
    return meta


def _rules_clear_meta(trigger: str, payload: Dict[str, Any] | None = None, *, con=None) -> Dict[str, Any]:
    meta = _rules_meta(trigger, payload, con=con)
    meta["rules_auto_resume_opt_in"] = True
    meta["rules_auto_resume_env"] = "RULES_AUTO_RESUME=1"
    meta["live_mode_requested"] = bool(_live_mode_requested(con))
    meta["production_like_requested"] = bool(_production_like_requested())
    return meta


def _activate_rule_halt(
    con,
    out: Dict[str, Any],
    *,
    scope: str,
    key: str,
    trigger: str,
    reason: str,
    meta: Dict[str, Any] | None = None,
    action_reason: Any = None,
) -> None:
    activated = activate_owned(
        scope,
        key,
        reason=reason,
        owner_actor=RULES_ACTOR,
        trigger=trigger,
        meta=_rules_meta(trigger, meta, con=con),
        action="AUTO",
        con=con,
    )
    if activated:
        out["actions"].append(
            {
                "scope": scope,
                "key": key,
                "enabled": 1,
                "reason": trigger if action_reason is None else action_reason,
            }
        )


def _clear_rule_halt(
    con,
    out: Dict[str, Any],
    *,
    scope: str,
    key: str,
    trigger: str,
    reason: str,
    meta: Dict[str, Any] | None = None,
    auto_resume: bool,
) -> None:
    if not auto_resume:
        return
    cleared = clear_owned(
        scope,
        key,
        owner_actor=RULES_ACTOR,
        trigger=trigger,
        reason=reason,
        meta=_rules_clear_meta(trigger, meta, con=con),
        con=con,
    )
    if cleared:
        out["actions"].append(
            {
                "scope": scope,
                "key": key,
                "enabled": 0,
                "reason": reason,
            }
        )

def _detect_realized_exec_cost_spike(con) -> Dict[str, Any]:
    """
    Uses realized execution costs from labels_exec and triggers on a high
    percentile rather than a mean so a cluster of bad fills is still visible.
    """
    now_ms = _now_ms()
    cutoff = now_ms - int(EXEC_COST_SPIKE_WINDOW_S) * 1000

    rows = con.execute(
        """
        SELECT total_cost_bps, slippage_bps
        FROM labels_exec
        WHERE realized=1
          AND ts_ms >= ?
        """,
        (int(cutoff),),
    ).fetchall()

    costs = []
    for total_bps, slip_bps in rows or []:
        try:
            if total_bps is not None:
                costs.append(float(total_bps))
            elif slip_bps is not None:
                costs.append(float(slip_bps))
        except Exception as e:
            _warn_nonfatal("RULES_ENGINE_REALIZED_COST_PARSE_FAILED", e)
            continue

    n = int(len(costs))
    if n < int(EXEC_COST_SPIKE_MIN_N):
        return {
            "spike": False,
            "n": n,
            "window_s": EXEC_COST_SPIKE_WINDOW_S,
            "threshold_bps": EXEC_COST_SPIKE_BPS,
        }

    costs_sorted = sorted(costs)
    k = int(round((EXEC_COST_SPIKE_PCTL / 100.0) * (n - 1)))
    if k < 0:
        k = 0
    if k >= n:
        k = n - 1

    pctl_bps = float(costs_sorted[k])
    avg_bps = float(sum(costs_sorted) / n)

    return {
        "spike": bool(pctl_bps >= EXEC_COST_SPIKE_BPS),
        "n": n,
        "avg_cost_bps": avg_bps,
        "pctl_cost_bps": pctl_bps,
        "pctl": EXEC_COST_SPIKE_PCTL,
        "window_s": EXEC_COST_SPIKE_WINDOW_S,
        "threshold_bps": EXEC_COST_SPIKE_BPS,
    }

def _detect_cost_spike(con) -> Dict[str, Any]:
    now_ms = _now_ms()
    cutoff = now_ms - int(COST_SPIKE_WINDOW_S) * 1000

    rows = con.execute(
        """
        SELECT last, bid, ask, spread
        FROM price_quotes
        WHERE ts_ms >= ?
        """,
        (int(cutoff),),
    ).fetchall()

    spreads = []
    for last, bid, ask, spr in rows or []:
        try:
            if last is None or last <= 1e-9:
                continue
            if spr is None and bid is not None and ask is not None:
                spr = float(ask) - float(bid)
            if spr is None:
                continue
            spreads.append(10000.0 * float(spr) / float(last))
        except Exception as e:
            _warn_nonfatal("RULES_ENGINE_COST_SPIKE_PARSE_FAILED", e)
            continue

    n = len(spreads)
    if n < COST_SPIKE_MIN_N:
        return {"spike": False, "n": n}

    avg_bps = float(sum(spreads) / n)
    return {
        "spike": avg_bps >= COST_SPIKE_BPS,
        "avg_spread_bps": avg_bps,
        "n": n,
        "window_s": COST_SPIKE_WINDOW_S,
        "threshold_bps": COST_SPIKE_BPS,
    }

def evaluate_rules() -> Dict[str, Any]:
    if not USE:
        return {"enabled": False}

    con = connect()
    try:
        now_ms = _now_ms()
        out: Dict[str, Any] = {"enabled": True, "ts_ms": now_ms, "actions": []}
        auto_resume = _rules_auto_resume_enabled(con)
        out["rules_auto_resume_enabled"] = bool(auto_resume)
        out["rules_auto_resume_requested"] = _env_truthy("RULES_AUTO_RESUME")
        out["rules_auto_resume_live_mode"] = bool(_live_mode_requested(con))
        out["rules_auto_resume_production_like"] = bool(_production_like_requested())

        # ---            -- ------------------------------------------------------
        # Execution cost spike → GLOBAL kill switch
        # ---            -- ------------------------------------------------------
        try:
            spike = _detect_cost_spike(con)
            out["exec_cost_spike"] = spike

            if spike.get("spike"):
                until_ms = now_ms + int(COST_SPIKE_COOLDOWN_S) * 1000
                meta = dict(spike)
                meta["until_ts_ms"] = int(until_ms)

                _activate_rule_halt(
                    con,
                    out,
                    scope="global",
                    key=RULES_GLOBAL_KEYS["cost_spike"],
                    trigger="cost_spike",
                    reason=f"rules_exec_cost_spike avg_spread_bps={float(spike.get('avg_spread_bps', 0.0)):.2f}",
                    meta=meta,
                )
            else:
                _clear_rule_halt(
                    con,
                    out,
                    scope="global",
                    key=RULES_GLOBAL_KEYS["cost_spike"],
                    trigger="cost_spike",
                    reason="rules_exec_cost_spike_clear",
                    meta=spike,
                    auto_resume=auto_resume,
                )
        except Exception as e:
            _warn_nonfatal("rules_engine_exec_cost_spike_evaluation_failed", e)

        # Global drawdown
        drawdown_state = evaluate_current_drawdown(con)
        out["drawdown_state"] = drawdown_state.to_dict()
        if not drawdown_state.ok:
            out["drawdown"] = None
            if _live_mode_requested(con) is not False:
                meta = {
                    "trigger": "drawdown_state_unavailable",
                    "reason_code": str(drawdown_state.reason_code),
                    "drawdown_state": drawdown_state.to_dict(),
                }
                _activate_rule_halt(
                    con,
                    out,
                    scope="global",
                    key=RULES_GLOBAL_KEYS["drawdown"],
                    trigger="drawdown",
                    reason=f"rules_drawdown_state_unavailable reason={drawdown_state.reason_code}",
                    meta=meta,
                    action_reason="drawdown_state_unavailable",
                )
        else:
            dd = float(drawdown_state.drawdown or 0.0)
            out["drawdown"] = float(dd)

        if drawdown_state.ok and dd >= MAX_DD:
            _activate_rule_halt(
                con,
                out,
                scope="global",
                key=RULES_GLOBAL_KEYS["drawdown"],
                trigger="drawdown",
                reason=f"rules_drawdown dd={dd:.3f}",
                meta={"dd": dd},
            )
        elif drawdown_state.ok:
            _clear_rule_halt(
                con,
                out,
                scope="global",
                key=RULES_GLOBAL_KEYS["drawdown"],
                trigger="drawdown",
                reason="rules_drawdown_clear",
                meta={"dd": dd},
                auto_resume=auto_resume,
            )

        # Global drift
        drift = 0.0
        try:
            drift = float(get_max_drift_ratio(con))
        except Exception:
            drift = 0.0
        out["max_drift_ratio"] = float(drift)

        if MAX_DRIFT > 0.0 and drift >= MAX_DRIFT:
            _activate_rule_halt(
                con,
                out,
                scope="global",
                key=RULES_GLOBAL_KEYS["drift"],
                trigger="drift",
                reason=f"rules_drift drift={drift:.2f}",
                meta={"drift": drift},
            )
        else:
            _clear_rule_halt(
                con,
                out,
                scope="global",
                key=RULES_GLOBAL_KEYS["drift"],
                trigger="drift",
                reason="rules_drift_clear",
                meta={"drift": drift},
                auto_resume=auto_resume,
            )

        # Global execution winrate
        gw = get_exec_winrate_global(con=con, lookback_days=EXEC_LOOKBACK_DAYS)
        out["exec_winrate_global"] = gw

        if gw is not None and gw < MIN_GLOBAL_WINRATE:
            _activate_rule_halt(
                con,
                out,
                scope="global",
                key=RULES_GLOBAL_KEYS["exec_winrate"],
                trigger="exec_winrate",
                reason=f"rules_exec_winrate winrate={gw:.2f}",
                meta={"winrate": gw},
                action_reason="global_winrate",
            )
        else:
            _clear_rule_halt(
                con,
                out,
                scope="global",
                key=RULES_GLOBAL_KEYS["exec_winrate"],
                trigger="exec_winrate",
                reason="rules_exec_winrate_clear",
                meta={"winrate": gw},
                auto_resume=auto_resume,
            )

        # Per-symbol halts
        sym_stats = get_exec_stats_by_symbol(con=con, lookback_days=EXEC_LOOKBACK_DAYS) or {}
        out["symbols_checked"] = int(len(sym_stats))

        for sym, st in sym_stats.items():
            n = float(st.get("n", 0.0))
            wr = float(st.get("winrate", 0.0))
            avg_z = float(st.get("avg_net_z", 0.0))

            sdr = 0.0
            try:
                sdr = float(get_symbol_max_drift_ratio(con, sym))
            except Exception:
                sdr = 0.0

            bad = False
            why: Dict[str, Any] = {}

            if n >= float(SYM_MIN_N) and wr < float(SYM_MIN_WINRATE):
                bad = True
                why["winrate"] = wr
                why["n"] = n

            if n >= float(SYM_MIN_N) and avg_z < float(SYM_MIN_AVG_NET_Z):
                bad = True
                why["avg_net_z"] = avg_z
                why["n"] = n

            if float(SYM_MAX_DRIFT) > 0.0 and sdr >= float(SYM_MAX_DRIFT):
                bad = True
                why["drift_ratio"] = sdr

            if bad:
                _activate_rule_halt(
                    con,
                    out,
                    scope="symbol",
                    key=sym,
                    trigger=RULES_SYMBOL_TRIGGER,
                    reason=f"rules_symbol_halt {sym}",
                    meta=why,
                    action_reason=why,
                )
            else:
                _clear_rule_halt(
                    con,
                    out,
                    scope="symbol",
                    key=sym,
                    trigger=RULES_SYMBOL_TRIGGER,
                    reason="rules_symbol_clear",
                    meta={"n": n, "winrate": wr, "avg_net_z": avg_z, "drift_ratio": sdr},
                    auto_resume=auto_resume,
                )

        return out
    finally:
        con.close()
