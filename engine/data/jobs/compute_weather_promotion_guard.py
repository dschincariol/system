# CREATE NEW FILE: compute_weather_promotion_guard.py
"""
Promotion & gating for weather features by horizon + regime.

Reads:  model_weather_effect
Writes: active_feature_policy (group_id='weather', weight/state)
Optional: model_promotion_audit + model_promotion_cooldown (if you want full audit trail)

Fail-closed:
- If insufficient data or unstable, keeps weather OFF.
"""

import os
import time
import json
import logging
from typing import Dict, Any, List, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import (
    connect,
    init_db,
    acquire_job_lock,
    release_job_lock,
    touch_job_lock,
    put_job_heartbeat,
)


LOG = logging.getLogger("compute_weather_promotion_guard")
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
STRUCTURED_LOG = get_logger("engine.data.jobs.compute_weather_promotion_guard")

JOB_NAME = "compute_weather_promotion_guard"
OWNER = os.environ.get("JOB_OWNER", "system")
PID = os.getpid()

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "300"))
INTERVAL_S = int(os.environ.get("WX_PROMO_INTERVAL_S", "600"))

# Gates
MIN_N = int(os.environ.get("WX_PROMO_MIN_N", "200"))
MIN_IMPROVE = float(os.environ.get("WX_PROMO_MIN_RMSE_IMPROVE", "0.02"))  # absolute rmse improvement
MAX_SPEARMAN_DEGRADE = float(os.environ.get("WX_PROMO_MAX_SPEARMAN_DEGRADE", "0.05"))
CONSEC_OK = int(os.environ.get("WX_PROMO_CONSEC_OK", "3"))

# Demotion
DEMOTE_CONSEC_BAD = int(os.environ.get("WX_DEMOTE_CONSEC_BAD", "3"))
DEMOTE_RMSE = float(os.environ.get("WX_DEMOTE_RMSE", "0.02"))

# Scopes/horizons are stored as strings in active_feature_policy
# e.g. scope='global' horizon='3600'
SCOPE = os.environ.get("WX_PROMO_SCOPE", "global").strip()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        STRUCTURED_LOG,
        event="compute_weather_promotion_guard_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.data.jobs.compute_weather_promotion_guard",
        extra=extra or None,
        persist=False,
    )


def _ensure_group(con) -> None:
    # keep factor_groups populated for explainability (optional but useful)
    con.execute(
        """
        INSERT OR IGNORE INTO factor_groups(group_id, description, members_json, enabled)
        VALUES (?,?,?,1)
        """,
        ("weather", "Weather forecast + alerts feature group", json.dumps(["wx"], separators=(",", ":"))),
    )


def _read_recent_effect(con, horizon_s: int, limit_n: int = 20) -> List[Dict[str, Any]]:
    rows = con.execute(
        """
        SELECT ts_ms, base_rmse, wx_rmse, rmse_delta,
               base_spearman, wx_spearman, spearman_delta,
               n_eval
        FROM model_weather_effect
        WHERE key_type='global' AND key='global'
          AND horizon_s=?
        ORDER BY ts_ms DESC
        LIMIT ?
        """,
        (int(horizon_s), int(limit_n)),
    ).fetchall() or []

    out = []
    for r in rows:
        out.append({
            "ts_ms": int(r[0]),
            "base_rmse": float(r[1] or 0.0),
            "wx_rmse": float(r[2] or 0.0),
            "rmse_delta": float(r[3] or 0.0),
            "base_spearman": float(r[4] or 0.0),
            "wx_spearman": float(r[5] or 0.0),
            "spearman_delta": float(r[6] or 0.0),
            "n_eval": int(r[7] or 0),
        })
    return out


def _gate_decision(series: List[Dict[str, Any]]) -> Tuple[str, float, Dict[str, Any]]:
    """
    Returns: (state, weight, explain)
    state: 'on' | 'off'
    weight: 0..1
    """
    # Promotion is intentionally fail-closed: weather only turns on after repeated
    # evidence that it helps, and a weak/ambiguous series leaves it disabled.
    if not series:
        return "off", 0.0, {"reason": "no_series"}

    # require last CONSEC_OK evaluations pass
    ok_streak = 0
    bad_streak = 0

    for s in series[:max(CONSEC_OK, DEMOTE_CONSEC_BAD)]:
        n_eval = int(s.get("n_eval", 0))
        rmse_delta = float(s.get("rmse_delta", 0.0))
        sp_delta = float(s.get("spearman_delta", 0.0))

        if n_eval < MIN_N:
            return "off", 0.0, {"reason": "insufficient_n", "n_eval": n_eval, "min_n": MIN_N}

        is_ok = (rmse_delta <= -abs(MIN_IMPROVE)) and (sp_delta >= -abs(MAX_SPEARMAN_DEGRADE))
        is_bad = (rmse_delta >= abs(DEMOTE_RMSE))

        if is_ok:
            ok_streak += 1
            bad_streak = 0
        elif is_bad:
            bad_streak += 1
            ok_streak = 0
        else:
            # neither strong ok nor strong bad breaks streaks
            ok_streak = 0
            bad_streak = 0

        if bad_streak >= DEMOTE_CONSEC_BAD:
            return "off", 0.0, {"reason": "demote_bad_streak", "bad_streak": bad_streak, "rmse_delta": rmse_delta}

        if ok_streak >= CONSEC_OK:
            # weight can be proportional to how strong improvement is
            strength = min(1.0, max(0.0, (-rmse_delta) / max(1e-9, abs(MIN_IMPROVE) * 3.0)))
            return "on", float(strength), {"reason": "promote_ok_streak", "ok_streak": ok_streak, "rmse_delta": rmse_delta, "spearman_delta": sp_delta}

    return "off", 0.0, {"reason": "no_stable_signal"}


def _write_policy(con, horizon_s: int, state: str, weight: float, explain: Dict[str, Any]) -> None:
    ts = _now_ms()
    # `active_feature_policy` is the authoritative switch consumed elsewhere.
    # The score table below is dashboard/audit context, not the control plane.
    con.execute(
        """
        INSERT OR REPLACE INTO active_feature_policy(scope, horizon, group_id, weight, state, since_ts)
        VALUES (?,?,?,?,?,?)
        """,
        (str(SCOPE), str(int(horizon_s)), "weather", float(weight), str(state), int(ts)),
    )

    # Optional: record in factor_group_scores for dashboard comparisons
    con.execute(
        """
        INSERT OR REPLACE INTO factor_group_scores(
          ts, scope, horizon, group_id, model_id,
          metric_ic, metric_calibration, metric_drawdown, metric_turnover, metric_cost, metric_stability,
          delta_vs_base, decision
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            int(ts),
            str(SCOPE),
            str(int(horizon_s)),
            "weather",
            "global",
            None, None, None, None, None, None,
            float(explain.get("rmse_delta", 0.0)) if "rmse_delta" in explain else None,
            json.dumps({"state": state, "weight": weight, "explain": explain}, separators=(",", ":"), sort_keys=True),
        ),
    )


def run_once():
    con = connect()
    try:
        _ensure_group(con)

        # The guard only operates on horizons that actually have measured effect
        # rows; it does not invent policy entries for unseen weather horizons.
        horizons = con.execute(
            "SELECT DISTINCT horizon_s FROM model_weather_effect WHERE key_type='global' AND key='global' ORDER BY horizon_s"
        ).fetchall() or []

        for (h,) in horizons:
            try:
                horizon_s = int(h)
            except Exception as e:
                _warn_nonfatal(
                    "COMPUTE_WEATHER_PROMOTION_GUARD_HORIZON_PARSE_FAILED",
                    e,
                    value=repr(h)[:120],
                )
                continue

            series = _read_recent_effect(con, horizon_s=horizon_s, limit_n=30)
            state, weight, explain = _gate_decision(series)

            # attach latest deltas for transparency
            if series:
                explain["latest"] = series[0]

            _write_policy(con, horizon_s, state, weight, explain)

        con.commit()
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("COMPUTE_WEATHER_PROMOTION_GUARD_CLOSE_FAILED", e)


def main():
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("options_poll must be launched by supervisor")
        raise SystemExit(1)

    init_db()
    if not acquire_job_lock(JOB_NAME, OWNER, PID, stale_after_s=LOCK_STALE_AFTER_S):
        raise SystemExit(2)

    last_hb = 0.0
    try:
        while True:
            now = time.time()
            if now - last_hb > 30:
                touch_job_lock(JOB_NAME, OWNER, PID)
                put_job_heartbeat(
                    JOB_NAME,
                    OWNER,
                    PID,
                    extra_json=json.dumps(
                        {"scope": SCOPE, "min_n": MIN_N, "min_improve": MIN_IMPROVE, "consec_ok": CONSEC_OK},
                        separators=(",", ":"),
                    ),
                )
                last_hb = now

            run_once()
            time.sleep(float(INTERVAL_S))
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    main()
