# CREATE NEW FILE: compute_factor_group_scores.py
"""
Compute group-level external-factor usefulness + update active_feature_policy.

This is a generic scorer/pruner:
- Reads your existing evaluation table: shadow_metrics
- Computes deltas vs a configured baseline model per horizon/regime
- Writes factor_group_scores
- Updates active_feature_policy (on/off/weight) per (scope, horizon, group)

IMPORTANT:
- This file does NOT assume a specific model naming scheme.
- You must provide GROUP_MODEL_MAP_JSON mapping group_id -> model_name via env.

Example env (bash):
  export GROUP_BASELINE_MODEL_NAME="base_model"
  export GROUP_MODEL_MAP_JSON='{"weather":"wx_model","factor_universe":"factors_model","social":"social_model"}'
  export GROUP_SCORE_HORIZONS_JSON='[300, 900, 3600, 14400]'
  export GROUP_SCORE_REGIMES_JSON='["global","risk_on","risk_off","neutral"]'
"""

import os
import time
import json
import math
import logging
from typing import Dict, Any, List, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import (
    connect,
    init_db,
    acquire_job_lock,
    release_job_lock,
    touch_job_lock,
    put_job_heartbeat,
)

LOG = logging.getLogger("compute_factor_group_scores")
_WARNED_NONFATAL_KEYS: set[str] = set()
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))

JOB_NAME = "compute_factor_group_scores"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "300"))
INTERVAL_S = int(os.environ.get("GROUP_SCORE_INTERVAL_S", "600"))

# -----------------------------
# Scoring & gating parameters
# -----------------------------
MIN_N = int(os.environ.get("GROUP_SCORE_MIN_N", "200"))
PROMO_CONSEC_OK = int(os.environ.get("GROUP_PROMO_CONSEC_OK", "3"))

# Promote if group beats baseline by at least this (absolute RMSE)
MIN_RMSE_IMPROVE = float(os.environ.get("GROUP_MIN_RMSE_IMPROVE", "0.02"))

# Don't allow "improve RMSE but break ordering"
MAX_DIRACC_DEGRADE = float(os.environ.get("GROUP_MAX_DIRACC_DEGRADE", "0.05"))

# Demote if it loses by this much for DEMOTE_CONSEC_BAD streak
DEMOTE_CONSEC_BAD = int(os.environ.get("GROUP_DEMOTE_CONSEC_BAD", "3"))
DEMOTE_RMSE = float(os.environ.get("GROUP_DEMOTE_RMSE", "0.02"))

# Prefer using net_rmse if present (execution-aware); else rmse
USE_NET = os.environ.get("GROUP_SCORE_USE_NET", "1") == "1"

# Scope string stored in active_feature_policy
SCOPE = os.environ.get("GROUP_SCORE_SCOPE", "global").strip()

# -----------------------------
# Model mapping inputs
# -----------------------------
BASELINE_MODEL_NAME = os.environ.get("GROUP_BASELINE_MODEL_NAME", "").strip()

GROUP_MODEL_MAP_JSON = os.environ.get("GROUP_MODEL_MAP_JSON", "").strip()
# Optional: group descriptions + membership list
GROUP_META_JSON = os.environ.get("GROUP_META_JSON", "").strip()

# Optional restrict horizons/regimes to score
GROUP_SCORE_HORIZONS_JSON = os.environ.get("GROUP_SCORE_HORIZONS_JSON", "").strip()
GROUP_SCORE_REGIMES_JSON = os.environ.get("GROUP_SCORE_REGIMES_JSON", "").strip()


def _warn_nonfatal(event: str, error: BaseException, *, once_key: str | None = None, **extra) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=event,
        code=event,
        message=event,
        error=error,
        level=logging.WARNING,
        component="ops.compute_factor_group_scores",
        extra=extra,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(x) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else 0.0
    except Exception as e:
        _warn_nonfatal("compute_factor_group_scores_safe_float_failed", e, once_key="safe_float", value=repr(x)[:120])
        return 0.0


def _safe_int(x) -> int:
    try:
        return int(x)
    except Exception as e:
        _warn_nonfatal("compute_factor_group_scores_safe_int_failed", e, once_key="safe_int", value=repr(x)[:120])
        return 0


def _load_json(s: str, default):
    if not s:
        return default
    try:
        v = json.loads(s)
        return v if v is not None else default
    except Exception as e:
        _warn_nonfatal("compute_factor_group_scores_json_parse_failed", e, once_key="safe_json", value=str(s)[:200])
        return default


def _ensure_groups(con, group_ids: List[str]) -> None:
    meta = _load_json(GROUP_META_JSON, {})
    for gid in group_ids:
        desc = None
        members = None
        try:
            g = (meta or {}).get(str(gid)) or {}
            desc = g.get("description")
            members = g.get("members")
        except Exception:
            desc = None
            members = None

        if desc is None:
            desc = f"Feature group: {gid}"
        if members is None:
            members = [str(gid)]

        # Factor groups are metadata for policy/audit and dashboard explainability.
        # The actual enable/disable decision is written separately below.
        con.execute(
            """
            INSERT OR REPLACE INTO factor_groups(group_id, description, members_json, enabled)
            VALUES (?,?,?,1)
            """,
            (str(gid), str(desc), json.dumps(list(members), separators=(",", ":"), sort_keys=True)),
        )


def _latest_shadow_metric(
    con,
    *,
    model_name: str,
    regime: str,
    horizon_s: int,
    limit_windows: int = 12,
) -> List[Dict[str, Any]]:
    """
    Returns list of recent windows newest-first for gating/streak logic.
    """
    rows = con.execute(
        """
        SELECT window_end_ms, n, rmse, mae, dir_acc, avg_cost, net_rmse, extra_json
        FROM shadow_metrics
        WHERE model_name=?
          AND COALESCE(regime,'global')=?
          AND horizon_s=?
        ORDER BY window_end_ms DESC
        LIMIT ?
        """,
        (str(model_name), str(regime), int(horizon_s), int(limit_windows)),
    ).fetchall() or []

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "window_end_ms": _safe_int(r[0]),
                "n": _safe_int(r[1]),
                "rmse": _safe_float(r[2]),
                "mae": _safe_float(r[3]),
                "dir_acc": _safe_float(r[4]),
                "avg_cost": _safe_float(r[5]),
                "net_rmse": _safe_float(r[6]),
                "extra_json": (r[7] or ""),
            }
        )
    return out


def _choose_rmse(m: Dict[str, Any]) -> float:
    if USE_NET and _safe_float(m.get("net_rmse", 0.0)) > 0.0:
        return _safe_float(m.get("net_rmse"))
    return _safe_float(m.get("rmse"))


def _compute_deltas(base: Dict[str, Any], grp: Dict[str, Any]) -> Dict[str, Any]:
    base_rmse = _choose_rmse(base)
    grp_rmse = _choose_rmse(grp)
    rmse_delta = grp_rmse - base_rmse  # negative is better

    base_dir = _safe_float(base.get("dir_acc", 0.0))
    grp_dir = _safe_float(grp.get("dir_acc", 0.0))
    dir_delta = grp_dir - base_dir  # positive is better

    base_cost = _safe_float(base.get("avg_cost", 0.0))
    grp_cost = _safe_float(grp.get("avg_cost", 0.0))
    cost_delta = grp_cost - base_cost

    return {
        "base_rmse": float(base_rmse),
        "grp_rmse": float(grp_rmse),
        "rmse_delta": float(rmse_delta),
        "base_dir_acc": float(base_dir),
        "grp_dir_acc": float(grp_dir),
        "dir_delta": float(dir_delta),
        "base_cost": float(base_cost),
        "grp_cost": float(grp_cost),
        "cost_delta": float(cost_delta),
        "n_eval": int(min(_safe_int(base.get("n", 0)), _safe_int(grp.get("n", 0)))),
    }


def _gate_series(series: List[Dict[str, Any]]) -> Tuple[str, float, Dict[str, Any]]:
    """
    series: newest-first list of delta dicts produced by _compute_deltas
    Returns: (state, weight, explain)
    """
    # Group promotion is deliberately fail-closed. No history or weak evidence
    # means the feature group stays off instead of being partially trusted.
    if not series:
        return "off", 0.0, {"reason": "no_series"}

    ok_streak = 0
    bad_streak = 0

    for s in series[: max(PROMO_CONSEC_OK, DEMOTE_CONSEC_BAD)]:
        n_eval = int(s.get("n_eval", 0))
        rmse_delta = float(s.get("rmse_delta", 0.0))
        dir_delta = float(s.get("dir_delta", 0.0))

        if n_eval < MIN_N:
            return "off", 0.0, {"reason": "insufficient_n", "n_eval": n_eval, "min_n": MIN_N}

        is_ok = (rmse_delta <= -abs(MIN_RMSE_IMPROVE)) and (dir_delta >= -abs(MAX_DIRACC_DEGRADE))
        is_bad = (rmse_delta >= abs(DEMOTE_RMSE))

        if is_ok:
            ok_streak += 1
            bad_streak = 0
        elif is_bad:
            bad_streak += 1
            ok_streak = 0
        else:
            ok_streak = 0
            bad_streak = 0

        if bad_streak >= DEMOTE_CONSEC_BAD:
            return "off", 0.0, {"reason": "demote_bad_streak", "bad_streak": bad_streak, "latest": s}

        if ok_streak >= PROMO_CONSEC_OK:
            # weight proportional to improvement strength
            strength = min(1.0, max(0.0, (-rmse_delta) / max(1e-9, abs(MIN_RMSE_IMPROVE) * 3.0)))
            return "on", float(strength), {"reason": "promote_ok_streak", "ok_streak": ok_streak, "latest": s}

    return "off", 0.0, {"reason": "no_stable_signal", "latest": series[0]}


def _write_scores_and_policy(
    con,
    *,
    group_id: str,
    regime: str,
    horizon_s: int,
    model_id: str,
    deltas: Dict[str, Any],
    decision_state: str,
    decision_weight: float,
    explain: Dict[str, Any],
) -> None:
    ts = _now_ms()

    # factor_group_scores (dashboard-friendly)
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
            str(regime),
            str(int(horizon_s)),
            str(group_id),
            str(model_id),
            None,
            None,
            None,
            None,
            float(deltas.get("cost_delta", 0.0)),
            None,
            float(deltas.get("rmse_delta", 0.0)),
            json.dumps(
                {
                    "state": decision_state,
                    "weight": decision_weight,
                    "deltas": deltas,
                    "explain": explain,
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
        ),
    )

    # active_feature_policy (execution gate)
    con.execute(
        """
        INSERT OR REPLACE INTO active_feature_policy(scope, horizon, group_id, weight, state, since_ts)
        VALUES (?,?,?,?,?,?)
        """,
        (
            str(regime),
            str(int(horizon_s)),
            str(group_id),
            float(decision_weight),
            str(decision_state),
            int(ts),
        ),
    )


def run_once() -> None:
    group_map = _load_json(GROUP_MODEL_MAP_JSON, {})
    if not isinstance(group_map, dict) or not group_map:
        LOG.warning("GROUP_MODEL_MAP_JSON missing/empty; nothing to score.")
        return

    if not BASELINE_MODEL_NAME:
        LOG.warning("GROUP_BASELINE_MODEL_NAME missing; nothing to score.")
        return

    horizons = _load_json(GROUP_SCORE_HORIZONS_JSON, [])
    regimes = _load_json(GROUP_SCORE_REGIMES_JSON, [])

    con = connect()
    try:
        _ensure_groups(con, list(group_map.keys()))

        # If user didn't specify horizons/regimes, infer from shadow_metrics
        if not horizons:
            rows = con.execute("SELECT DISTINCT horizon_s FROM shadow_metrics ORDER BY horizon_s").fetchall() or []
            horizons = [int(r[0]) for r in rows if r and r[0] is not None]

        if not regimes:
            rows = con.execute("SELECT DISTINCT COALESCE(regime,'global') FROM shadow_metrics").fetchall() or []
            regimes = [str(r[0] or "global") for r in rows if r]

        for regime in regimes:
            for horizon_s in horizons:
                # baseline series newest-first
                base_series = _latest_shadow_metric(
                    con,
                    model_name=str(BASELINE_MODEL_NAME),
                    regime=str(regime),
                    horizon_s=int(horizon_s),
                    limit_windows=30,
                )
                if not base_series:
                    continue

                for group_id, group_model in group_map.items():
                    if not group_model:
                        continue

                    grp_series = _latest_shadow_metric(
                        con,
                        model_name=str(group_model),
                        regime=str(regime),
                        horizon_s=int(horizon_s),
                        limit_windows=30,
                    )
                    if not grp_series:
                        continue

                    # align windows by index (newest-first); conservative
                    aligned_n = min(len(base_series), len(grp_series), 30)
                    delta_series: List[Dict[str, Any]] = []
                    for i in range(aligned_n):
                        b = base_series[i]
                        g = grp_series[i]
                        d = _compute_deltas(b, g)
                        d["window_end_ms"] = int(min(_safe_int(b.get("window_end_ms")), _safe_int(g.get("window_end_ms"))))
                        delta_series.append(d)

                    state, weight, explain = _gate_series(delta_series)
                    latest = delta_series[0] if delta_series else {}
                    explain["latest"] = latest
                    explain["baseline_model"] = str(BASELINE_MODEL_NAME)
                    explain["group_model"] = str(group_model)

                    _write_scores_and_policy(
                        con,
                        group_id=str(group_id),
                        regime=str(regime),
                        horizon_s=int(horizon_s),
                        model_id="shadow_metrics",
                        deltas=latest,
                        decision_state=state,
                        decision_weight=weight,
                        explain=explain,
                    )

        con.commit()
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("compute_factor_group_scores_db_close_failed", e)


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
            if now - last_hb >= 30.0:
                touch_job_lock(JOB_NAME, OWNER, PID)
                put_job_heartbeat(
                    JOB_NAME,
                    OWNER,
                    PID,
                    extra_json=json.dumps(
                        {
                            "baseline": BASELINE_MODEL_NAME,
                            "min_n": MIN_N,
                            "min_rmse_improve": MIN_RMSE_IMPROVE,
                            "use_net": USE_NET,
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                )
                last_hb = now

            run_once()
            time.sleep(float(INTERVAL_S))
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    main()
