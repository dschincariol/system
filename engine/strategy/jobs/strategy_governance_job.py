# FILE: strategy_governance_job.py

"""
Unified Strategy Governance Job (FULLY UNIFIED + SAFE)

Preserves:
- strategy_registry stage promotion/demotion (original logic)
- ROI validation thresholds (_roi_pass)
- Metric freshness enforcement per strategy
- Promotion cooldown (PROMOTE_COOLDOWN_S)
- Promotion idempotency guard
- Champion / Challenger tracking
- portfolio_meta persistence
- Optional promotion audit hook
- dev_core.storage job lock (acquire / touch / release)
- Transaction-level locking (BEGIN IMMEDIATE)
- Validation metadata persistence

Adds:
- Capital efficiency gates
- return_per_risk_unit gate
- drawdown_contribution gate
- window_days selection for strategy_metrics
"""

import json
import logging
import os
import time
from typing import Dict, Optional, Tuple, List
from engine.runtime.failure_diagnostics import log_failure

from engine.runtime.storage import (
    connect,
    init_db,
    acquire_job_lock,
    release_job_lock,
    touch_job_lock,
)
from engine.strategy.ope_gate import evaluate_policy_ope_gate

try:
    from engine.strategy.promotion_audit import audit
except Exception:
    audit = None

try:
    from engine.strategy.model_governance_ext import record_governance_snapshot
except Exception:
    record_governance_snapshot = None

try:
    from engine.strategy.portfolio import init_portfolio_db
except Exception:
    init_portfolio_db = None

try:
    from engine.strategy.strategy_promotion_governance import (
        ensure_strategy_promotion_governance_schema,
        evaluate_strategy_promotion_governance,
        mark_strategy_promotion_promoted,
    )
except Exception:
    ensure_strategy_promotion_governance_schema = None
    evaluate_strategy_promotion_governance = None
    mark_strategy_promotion_promoted = None


# ----------------------------------------------------------------------
# ENV CONTROLS
# ----------------------------------------------------------------------

PROMOTE_STREAK = int(os.environ.get("STRAT_PROMOTE_STREAK", "3"))
PROMOTE_MIN_EDGE = float(os.environ.get("STRAT_PROMOTE_MIN_EDGE", "0.05"))

MIN_SHARPE = float(os.environ.get("STRAT_MIN_SHARPE", "0.5"))
MAX_DD = float(os.environ.get("STRAT_MAX_DD", "0.25"))

MIN_EFFICIENCY = float(os.environ.get("STRAT_MIN_EFFICIENCY", "0.0"))
MIN_RETURN_PER_RISK = float(os.environ.get("STRAT_MIN_RETURN_PER_RISK", "0.0"))
MAX_DD_CONTRIB = float(os.environ.get("STRAT_MAX_DD_CONTRIB", "-1.0"))

GOV_MIN_NET_CALMAR = float(os.environ.get("GOV_MIN_NET_CALMAR", "0.15"))
GOV_MIN_SHARPE = float(os.environ.get("GOV_MIN_SHARPE", "0.10"))
GOV_MAX_DRAWDOWN = float(os.environ.get("GOV_MAX_DRAWDOWN", "0.35"))
GOV_MIN_TOTAL_RETURN = float(os.environ.get("GOV_MIN_TOTAL_RETURN", "0.00"))

PROMOTE_COOLDOWN_S = int(os.environ.get("PROMOTE_COOLDOWN_S", "3600"))

METRICS_FRESH_S = int(os.environ.get("GOV_METRICS_FRESH_S", "21600"))
METRICS_WINDOW_DAYS = int(os.environ.get("GOV_METRICS_WINDOW_DAYS", "0"))

LOCK_NAME = os.environ.get("GOV_LOCK_NAME", "strategy_governance_job")
LOCK_STALE_S = int(os.environ.get("GOV_LOCK_STALE_S", "600"))
LOCK_WAIT_MS = int(os.environ.get("GOV_LOCK_WAIT_MS", "2500"))

OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()
LOGGER = logging.getLogger(__name__)
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: Exception, *, once_key: str | None = None, **extra) -> None:
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
        component="engine.strategy.jobs.strategy_governance_job",
        extra=extra or None,
        include_health=False,
        persist=False,
    )


# ----------------------------------------------------------------------

def _set_busy_timeout(con, ms: int) -> None:
    try:
        con.execute("PRAGMA busy_timeout = ?", (int(max(0, ms)),))
    except Exception:
        try:
            con.execute(f"PRAGMA busy_timeout = {int(max(0, ms))}")
        except Exception as e:
            _warn_nonfatal("STRATEGY_GOVERNANCE_BUSY_TIMEOUT_SET_FAILED", e, once_key="busy_timeout_set")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_json(s: str) -> Dict:
    try:
        x = json.loads(s or "{}")
        return x if isinstance(x, dict) else {}
    except Exception as e:
        _warn_nonfatal("STRATEGY_GOVERNANCE_SAFE_JSON_FAILED", e, once_key="safe_json", value=str(s)[:200])
        return {}


# ----------------------------------------------------------------------
# ROI VALIDATION (ORIGINAL + EXTENDED)
# ----------------------------------------------------------------------

def _roi_pass(metrics: Dict) -> Tuple[bool, Dict]:
    net_calmar = float(metrics.get("net_calmar", 0.0))
    sharpe = float(metrics.get("sharpe_simple", 0.0))
    max_dd = float(metrics.get("max_drawdown", 0.0))
    total_ret = float(metrics.get("total_return", 0.0))
    efficiency = float(metrics.get("efficiency_score", 0.0))
    ret_per_risk = float(metrics.get("return_per_risk_unit", 0.0))
    dd_contrib = float(metrics.get("drawdown_contribution", 0.0))

    ok = True
    reasons = {
        "net_calmar": net_calmar,
        "sharpe_simple": sharpe,
        "max_drawdown": max_dd,
        "total_return": total_ret,
        "efficiency_score": efficiency,
        "return_per_risk_unit": ret_per_risk,
        "drawdown_contribution": dd_contrib,
    }

    if net_calmar < GOV_MIN_NET_CALMAR:
        ok = False
        reasons["fail_net_calmar"] = True
    if sharpe < GOV_MIN_SHARPE:
        ok = False
        reasons["fail_sharpe"] = True
    if max_dd > GOV_MAX_DRAWDOWN:
        ok = False
        reasons["fail_max_drawdown"] = True
    if total_ret < GOV_MIN_TOTAL_RETURN:
        ok = False
        reasons["fail_total_return"] = True
    if efficiency < MIN_EFFICIENCY:
        ok = False
        reasons["fail_efficiency"] = True
    if ret_per_risk < MIN_RETURN_PER_RISK:
        ok = False
        reasons["fail_return_per_risk"] = True
    if dd_contrib < MAX_DD_CONTRIB:
        ok = False
        reasons["fail_drawdown_contribution"] = True

    return bool(ok), reasons


# ----------------------------------------------------------------------
# portfolio_meta helpers
# ----------------------------------------------------------------------

def _get_meta(con, key: str) -> Optional[str]:
    from engine.runtime.state_cache import cache_get, cache_set

    try:
        key_s = str(key)
        cached = cache_get("portfolio_meta", key_s)
        if cached is not None:
            return str(cached) if cached is not None else None

        row = con.execute("SELECT value FROM portfolio_meta WHERE key=?", (key_s,)).fetchone()
        value = str(row[0]) if row and row[0] is not None else None
        cache_set("portfolio_meta", key_s, value, ttl_s=3600.0)
        return value
    except Exception as e:
        _warn_nonfatal("STRATEGY_GOVERNANCE_META_GET_FAILED", e, once_key=f"meta_get:{key}", key=str(key))
        return None


def _set_meta(con, key: str, value: str) -> None:
    from engine.runtime.state_cache import cache_invalidate_namespace, cache_set

    key_s = str(key)
    value_s = str(value)

    con.execute(
        """
        INSERT INTO portfolio_meta(key,value)
        VALUES(?,?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,
        (key_s, value_s),
    )
    cache_set("portfolio_meta", key_s, value_s, ttl_s=3600.0)
    cache_invalidate_namespace("portfolio_snapshot")


# ----------------------------------------------------------------------
# Metric freshness enforcement
# ----------------------------------------------------------------------

def _latest_metrics_per_strategy(con, cutoff_ts_ms: int) -> Dict[str, Dict]:
    rows = con.execute(
        """
        SELECT m.strategy_name, m.ts_ms, m.metrics_json
        FROM strategy_metrics m
        JOIN (
          SELECT strategy_name, MAX(ts_ms) AS ts_ms
          FROM strategy_metrics
          WHERE ts_ms>=? AND window_days=?
          GROUP BY strategy_name
        ) t
        ON t.strategy_name=m.strategy_name AND t.ts_ms=m.ts_ms
        """,
        (int(cutoff_ts_ms), int(METRICS_WINDOW_DAYS)),
    ).fetchall()

    out: Dict[str, Dict] = {}
    for r in rows or []:
        out[str(r[0])] = {
            "ts_ms": int(r[1] or 0),
            "metrics": _safe_json(r[2] or "{}"),
        }
    return out


def _strategy_stage_map(con) -> Dict[str, str]:
    out: Dict[str, str] = {}
    try:
        rows = con.execute(
            """
            SELECT strategy_name, stage
            FROM strategy_registry
            WHERE enabled=1
            """
        ).fetchall()
        for r in rows or []:
            try:
                out[str(r[0])] = str(r[1] or "paper")
            except Exception as e:
                _warn_nonfatal("STRATEGY_GOVERNANCE_STAGE_ROW_PARSE_FAILED", e, once_key="stage_row_parse", row=repr(r)[:200])
                continue
    except Exception as e:
        _warn_nonfatal("STRATEGY_GOVERNANCE_LOAD_STAGES_FAILED", e, once_key="load_stages")
    return out


# ----------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------

def main():
    con = connect()
    try:
        init_db()
        _set_busy_timeout(con, int(LOCK_WAIT_MS))

        if init_portfolio_db:
            try:
                init_portfolio_db()
            except Exception as e:
                _warn_nonfatal("STRATEGY_GOVERNANCE_INIT_PORTFOLIO_DB_FAILED", e, once_key="init_portfolio_db")

        if callable(ensure_strategy_promotion_governance_schema):
            try:
                ensure_strategy_promotion_governance_schema(con)
            except Exception as e:
                _warn_nonfatal(
                    "STRATEGY_GOVERNANCE_PROMOTION_SCHEMA_FAILED",
                    e,
                    once_key="strategy_promotion_governance_schema",
                )

        if not acquire_job_lock(str(LOCK_NAME), str(OWNER), int(PID), ttl_s=int(LOCK_STALE_S)):
            print(json.dumps({"ok": True, "skipped": True, "reason": "lock held"}))
            return 0

        touch_job_lock(str(LOCK_NAME), str(OWNER), int(PID))

        now_ms = _now_ms()

        # Cooldown
        last_prom = _get_meta(con, "last_strategy_promotion_ts_ms")
        if last_prom and (now_ms - int(last_prom)) < int(PROMOTE_COOLDOWN_S) * 1000:
            release_job_lock(str(LOCK_NAME), str(OWNER), int(PID))
            print(json.dumps({"ok": True, "skipped": True, "reason": "promotion cooldown"}))
            return 0

        cutoff = now_ms - (METRICS_FRESH_S * 1000)
        latest = _latest_metrics_per_strategy(con, cutoff)

        if not latest:
            release_job_lock(str(LOCK_NAME), str(OWNER), int(PID))
            print(json.dumps({"ok": False, "error": "no fresh strategy_metrics"}))
            return 2

        promotion_system_guard: Dict[str, object] = {"passed": False, "reason": {"blockers": ["promotion_guard_unavailable"]}}
        try:
            from engine.strategy.promotion_guard import promotion_allowed

            promotion_system_allowed, promotion_system_reason = promotion_allowed()
            promotion_system_guard = {
                "passed": bool(promotion_system_allowed),
                "reason": dict(promotion_system_reason or {}),
            }
        except Exception as e:
            _warn_nonfatal("STRATEGY_GOVERNANCE_PROMOTION_GUARD_FAILED", e, once_key="promotion_guard")

        passing: List[Tuple[str, float]] = []
        validation: Dict[str, Dict] = {}

        stage_map = _strategy_stage_map(con)
        reg = bool(stage_map)

        try:
            if not bool(getattr(con, "in_transaction", False)):
                con.begin_managed_write()
        except Exception as e:
            _warn_nonfatal("STRATEGY_GOVERNANCE_BEGIN_IMMEDIATE_FAILED", e, once_key="begin_immediate")
            raise

        for name, rec in latest.items():
            m = rec["metrics"]
            ts_ms = rec["ts_ms"]

            sharpe_simple = float(m.get("sharpe_simple", 0.0))
            max_dd = float(m.get("max_drawdown", 1.0))

            current_stage = str(stage_map.get(str(name), "paper"))

            if reg:
                if current_stage == "shadow":
                    pass
                elif current_stage == "live" and sharpe_simple >= MIN_SHARPE and max_dd <= MAX_DD:
                    pass
                elif current_stage != "live":
                    con.execute(
                        "UPDATE strategy_registry SET stage='paper', updated_ts_ms=? WHERE strategy_name=?",
                        (int(now_ms), str(name)),
                    )
                else:
                    con.execute(
                        "UPDATE strategy_registry SET stage='paper', updated_ts_ms=? WHERE strategy_name=?",
                        (int(now_ms), str(name)),
                    )

            ok, reasons = _roi_pass(m)

            validation[name] = {
                "ok": ok,
                "ts_ms": ts_ms,
                "reasons": reasons,
            }

            try:
                con.execute(
                    "UPDATE strategy_metrics SET is_active=? WHERE strategy_name=? AND ts_ms=?",
                    (1 if ok else 0, str(name), int(ts_ms)),
                )
            except Exception as e:
                _warn_nonfatal(
                    "STRATEGY_GOVERNANCE_UPDATE_ACTIVE_FLAG_FAILED",
                    e,
                    once_key=f"update_active_flag:{name}",
                    strategy_name=str(name),
                    ts_ms=int(ts_ms),
                )

            if not ok:
                continue

            net_calmar = float(m.get("net_calmar", 0.0))
            efficiency = float(m.get("efficiency_score", 0.0))
            shadow_proxy = float(m.get("shadow_proxy_score", 0.0))

            score = net_calmar + (sharpe_simple * 0.25) + (efficiency * 0.25) - (max_dd * 0.25)

            # shadow strategies receive additional evaluation weight
            if current_stage == "shadow":
                score += float(shadow_proxy) * 0.50

            passing.append((name, score))

        if not passing:
            _set_meta(con, "last_strategy_validation", json.dumps(validation))
            con.commit()
            release_job_lock(str(LOCK_NAME), str(OWNER), int(PID))
            print(json.dumps({"ok": True, "promoted": False}))
            return 0

        passing.sort(key=lambda x: x[1], reverse=True)

        champion = passing[0][0]
        challenger = passing[1][0] if len(passing) > 1 else None

        current_champ = _get_meta(con, "strategy_champion")

        current_live = None
        for sname, sstage in stage_map.items():
            if str(sstage) == "live":
                current_live = str(sname)
                break

        champion_score = None
        live_score = None

        for sname, sscore in passing:
            if sname == champion and champion_score is None:
                champion_score = float(sscore)
            if current_live and sname == current_live and live_score is None:
                live_score = float(sscore)

        promote_candidate = False

        if champion:

            # prevent noise promotions when scores are extremely close
            if champion_score is not None and live_score is not None:
                diff = abs(float(champion_score) - float(live_score))
                if diff < float(PROMOTE_MIN_EDGE) * 0.25:
                    promote_candidate = False
            champion_stage = str(stage_map.get(str(champion), "paper"))

            if champion_stage == "shadow":
                if current_live:
                    if live_score is None:
                        promote_candidate = True
                    elif champion_score is not None and champion_score >= live_score + float(PROMOTE_MIN_EDGE):
                        promote_candidate = True
                else:
                    promote_candidate = True
            elif current_champ != champion:
                promote_candidate = True

        streak_subject = champion if promote_candidate else (current_live or champion)

        streak_key = f"strategy_streak::{streak_subject}"
        streak = int(_get_meta(con, streak_key) or 0) + 1
        _set_meta(con, streak_key, str(streak))

        # reset streaks for competing strategies
        for sname in stage_map.keys():
            if str(sname) != str(streak_subject):
                try:
                    _set_meta(con, f"strategy_streak::{sname}", "0")
                except Exception as e:
                    _warn_nonfatal(
                        "STRATEGY_GOVERNANCE_RESET_STREAK_FAILED",
                        e,
                        once_key=f"reset_streak:{sname}",
                        strategy_name=str(sname),
                    )

        promoted = False

        # Promotion hysteresis guard
        last_prom_ts = _get_meta(con, "last_strategy_promotion_ts_ms")

        cooldown_ok = True
        if last_prom_ts:
            try:
                elapsed = now_ms - int(last_prom_ts)
                cooldown_ok = elapsed >= int(PROMOTE_COOLDOWN_S) * 1000
            except Exception:
                cooldown_ok = True

        if streak >= PROMOTE_STREAK and cooldown_ok:
            if current_champ != champion:
                promotion_ope_ok = True
                promotion_ope_gate = {}
                try:
                    promotion_ope_ok, promotion_ope_gate = evaluate_policy_ope_gate(
                        model_id=f"strategy:{champion}",
                        model_name=str(champion),
                        candidate_version=str(now_ms),
                        regime="global",
                        metadata={
                            "candidate_key": f"strategy:{champion}",
                            "strategy_name": str(champion),
                            "source": "strategy_governance_job",
                        },
                        con=con,
                    )
                except Exception as e:
                    promotion_ope_ok = False
                    promotion_ope_gate = {
                        "applied": True,
                        "passed": False,
                        "status": f"ope_gate_error:{type(e).__name__}",
                    }
                    _warn_nonfatal(
                        "STRATEGY_GOVERNANCE_OPE_GATE_FAILED",
                        e,
                        once_key=f"ope_gate:{champion}",
                        champion=str(champion),
                    )
                validation["promotion_ope_gate"] = dict(promotion_ope_gate or {})
                if not bool(promotion_ope_ok):
                    _set_meta(con, "last_strategy_validation", json.dumps(validation))
                    _set_meta(con, "last_strategy_governance_ts_ms", str(now_ms))
                    con.commit()
                    release_job_lock(str(LOCK_NAME), str(OWNER), int(PID))
                    print(json.dumps({"ok": True, "promoted": False, "blocked_by": "ope_gate"}))
                    return 0

                champion_metrics = dict((latest.get(str(champion)) or {}).get("metrics") or {})
                champion_stage = str(stage_map.get(str(champion), "paper"))
                strategy_promotion_governance: Dict[str, object] = {
                    "passed": True,
                    "not_required": champion_stage == "live",
                }
                if champion_stage != "live":
                    if not callable(evaluate_strategy_promotion_governance):
                        strategy_promotion_governance = {
                            "passed": False,
                            "blockers": ["strategy_promotion_governance_unavailable"],
                        }
                    else:
                        promotion_governance_ok, strategy_promotion_governance = evaluate_strategy_promotion_governance(
                            con,
                            strategy_name=str(champion),
                            metrics=champion_metrics,
                            audit_block=True,
                            system_guard=dict(promotion_system_guard or {}),
                        )
                        strategy_promotion_governance = dict(strategy_promotion_governance or {})
                        strategy_promotion_governance["passed"] = bool(promotion_governance_ok)
                    validation["strategy_promotion_governance"] = dict(strategy_promotion_governance or {})
                    if not bool(strategy_promotion_governance.get("passed")):
                        _set_meta(con, "last_strategy_validation", json.dumps(validation))
                        _set_meta(con, "last_strategy_governance_ts_ms", str(now_ms))
                        con.commit()
                        release_job_lock(str(LOCK_NAME), str(OWNER), int(PID))
                        blockers = list(dict(strategy_promotion_governance or {}).get("blockers") or [])
                        print(
                            json.dumps(
                                {
                                    "ok": True,
                                    "promoted": False,
                                    "blocked_by": "strategy_promotion_governance",
                                    "blockers": blockers,
                                }
                            )
                        )
                        return 0

                try:
                    if current_champ and str(current_champ) != str(champion):
                        con.execute(
                            "UPDATE strategy_registry SET stage='shadow', updated_ts_ms=? WHERE strategy_name=?",
                            (int(now_ms), str(current_champ)),
                        )

                    if challenger and str(challenger) != str(champion):
                        con.execute(
                            "UPDATE strategy_registry SET stage='shadow', updated_ts_ms=? WHERE strategy_name=?",
                            (int(now_ms), str(challenger)),
                        )

                    con.execute(
                        "UPDATE strategy_registry SET stage='live', updated_ts_ms=? WHERE strategy_name=?",
                        (int(now_ms), str(champion)),
                    )
                    if champion_stage != "live" and callable(mark_strategy_promotion_promoted):
                        mark_strategy_promotion_promoted(
                            con,
                            strategy_name=str(champion),
                            governance=dict(strategy_promotion_governance or {}),
                        )
                except Exception as e:
                    _warn_nonfatal(
                        "STRATEGY_GOVERNANCE_PROMOTION_STAGE_UPDATE_FAILED",
                        e,
                        once_key=f"promotion_stage_update:{champion}",
                        champion=str(champion),
                        challenger=str(challenger or ""),
                        current_champion=str(current_champ or ""),
                    )
                    raise

                _set_meta(con, "strategy_champion", champion)
                if challenger:
                    _set_meta(con, "strategy_challenger", challenger)
                _set_meta(con, "last_strategy_promotion_ts_ms", str(now_ms))
                promoted = True

        _set_meta(con, "last_strategy_validation", json.dumps(validation))
        _set_meta(con, "last_strategy_governance_ts_ms", str(now_ms))

        if callable(record_governance_snapshot):
            try:
                record_governance_snapshot(
                    source="strategy_governance_job",
                    regime="global",
                    champion_name=str(champion or ""),
                    challenger_name=str(challenger or ""),
                    status=("promoted" if promoted else "evaluated"),
                    summary={
                        "promoted": bool(promoted),
                        "streak": int(streak),
                        "champion": str(champion or ""),
                        "challenger": str(challenger or ""),
                        "validation_count": int(len(validation)),
                        "passing_count": int(len(passing)),
                    },
                    con=con,
                )
            except Exception as e:
                _warn_nonfatal(
                    "STRATEGY_GOVERNANCE_SNAPSHOT_RECORD_FAILED",
                    e,
                    once_key="governance_snapshot",
                    champion=str(champion or ""),
                    challenger=str(challenger or ""),
                )

        con.commit()
        release_job_lock(str(LOCK_NAME), str(OWNER), int(PID))

        if promoted and audit:
            try:
                audit(
                    actor="strategy_governance",
                    action="PROMOTE_STRATEGY",
                    model_name=str(champion),
                    reason={"validation": validation},
                )
            except Exception as e:
                _warn_nonfatal(
                    "STRATEGY_GOVERNANCE_AUDIT_FAILED",
                    e,
                    once_key="promotion_audit",
                    champion=str(champion),
                )

        print(json.dumps({
            "ok": True,
            "champion": champion,
            "challenger": challenger,
            "promoted": promoted,
            "streak": streak,
        }))
        return 0

    except Exception as e:
        try:
            con.rollback()
        except Exception as rollback_error:
            _warn_nonfatal("STRATEGY_GOVERNANCE_ROLLBACK_FAILED", rollback_error, once_key="rollback")
        try:
            release_job_lock(str(LOCK_NAME), str(OWNER), int(PID))
        except Exception as release_error:
            _warn_nonfatal("STRATEGY_GOVERNANCE_RELEASE_JOB_LOCK_FAILED", release_error, once_key="release_job_lock")
        print(json.dumps({"ok": False, "error": str(e)}))
        return 2

    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("STRATEGY_GOVERNANCE_CLOSE_FAILED", e, once_key="close")


if __name__ == "__main__":
    raise SystemExit(main())
