"""
FILE: post_promotion_monitor.py

Job entrypoint or scheduled task for `post_promotion_monitor`.
"""


# post_promotion_monitor.py
import json
import math
import os
import time
import logging
from typing import Any, Dict, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import connect, init_db
from engine.strategy.validation import init_validation_db
from engine.strategy.promotion_hardening import auto_rollback, close_watch
from engine.execution.kill_switch import activate

LOG = logging.getLogger("post_promotion_monitor")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(event: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=event,
        code=event,
        message=event,
        error=error,
        level=logging.WARNING,
        component="engine.runtime.jobs.post_promotion_monitor",
        extra=extra,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)

def _now_ms() -> int:
    return int(time.time() * 1000)

def _eval_window(con, *, regime: str, horizon_s: int, since_ms: int) -> Optional[Dict[str, Any]]:
    # Prefer execution-aware net_z if present, fallback to labels.impact_z
    rows = con.execute(
        """
        SELECT p.predicted_z, le.net_z, l.impact_z
        FROM predictions p
        JOIN labels l
          ON l.event_id=p.event_id
         AND l.symbol=p.symbol
         AND l.horizon_s=p.horizon_s
        LEFT JOIN labels_exec le
          ON le.event_id=p.event_id
         AND le.symbol=p.symbol
         AND le.horizon_s=p.horizon_s
        WHERE p.ts_ms >= ?
          AND p.horizon_s = ?
          AND COALESCE(l.regime,'global') = ?
        LIMIT 50000
        """,
        (int(since_ms), int(horizon_s), str(regime)),
    ).fetchall()

    if not rows or len(rows) < int(os.environ.get("POST_PROMO_MIN_EVAL_N", "50")):
        return None

    n = 0
    se = 0.0
    hit = 0

    nn = 0
    nse = 0.0
    nhit = 0

    for pred, netz, realz in rows:
        try:
            pr = float(pred)
        except Exception as e:
            _warn_nonfatal(
                "post_promotion_monitor_pred_parse_failed",
                e,
                once_key="pred_parse",
                predicted_z=repr(pred)[:120],
            )
            continue

        # gross target
        try:
            rz = float(realz)
        except Exception as e:
            _warn_nonfatal(
                "post_promotion_monitor_realized_parse_failed",
                e,
                once_key="realized_parse",
                realized_z=repr(realz)[:120],
            )
            continue

        n += 1
        e = pr - rz
        se += e * e
        if (pr >= 0 and rz >= 0) or (pr < 0 and rz < 0):
            hit += 1

        # net target (if available)
        if netz is not None:
            try:
                nz = float(netz)
            except Exception:
                nz = None
            if nz is not None:
                nn += 1
                ne = pr - nz
                nse += ne * ne
                if (pr >= 0 and nz >= 0) or (pr < 0 and nz < 0):
                    nhit += 1

    if n <= 0:
        return None

    out = {
        "n": int(n),
        "rmse": float(math.sqrt(se / n)),
        "dir_acc": float(hit) / float(n),
    }
    if nn >= 10:
        out["net_n"] = int(nn)
        out["net_rmse"] = float(math.sqrt(nse / nn))
        out["net_dir_acc"] = float(nhit) / float(nn)

    return out

def main() -> int:
    init_db()
    init_validation_db()
    con = connect()
    try:
        now = _now_ms()

        watches = con.execute(
            """
            SELECT id, model_name, regime, to_model_kind, to_model_ts_ms, watch_until_ts_ms, baseline_metrics_json
            FROM model_post_promo_watch
            WHERE status='active'
            ORDER BY ts_ms ASC
            """
        ).fetchall()

        if not watches:
            return 0

        horizon_s = int(os.environ.get("POST_PROMO_HORIZON_S", "300"))
        degrade_rmse_pct = float(os.environ.get("POST_PROMO_MAX_RMSE_DEGRADE_PCT", "0.15"))  # +15%
        degrade_dir_drop = float(os.environ.get("POST_PROMO_MAX_DIR_DROP", "0.03"))          # -3pp
        eval_lookback_s = int(os.environ.get("POST_PROMO_EVAL_LOOKBACK_S", "7200"))         # 2h
        # Policy: immediate rollback on first breach (no debounce)

        for wid, model_name, regime, to_kind, to_ts, until_ms, baseline_json in watches:
            wid = int(wid)
            until_ms = int(until_ms or 0)

            # Expired watch window => close as OK
            if until_ms > 0 and now > until_ms:
                close_watch(wid, "expired_ok", note="watch window expired")
                continue

            baseline = {}
            try:
                baseline = json.loads(baseline_json or "{}") if baseline_json else {}
            except Exception:
                baseline = {}

            since_ms = now - int(eval_lookback_s) * 1000
            cur = _eval_window(con, regime=str(regime), horizon_s=int(horizon_s), since_ms=int(since_ms))
            if not cur:
                continue

            con.execute(
                """
                INSERT INTO model_post_promo_results(
                  watch_id, ts_ms, n, rmse, dir_acc, net_rmse, net_dir_acc, extra_json
                )
                VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    wid,
                    now,
                    int(cur.get("n", 0)),
                    cur.get("rmse"),
                    cur.get("dir_acc"),
                    cur.get("net_rmse"),
                    cur.get("net_dir_acc"),
                    json.dumps({"baseline": baseline}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.commit()

            # Compare to baseline (prefer net metrics if available)
            b_rmse = baseline.get("rmse_net", baseline.get("rmse"))
            b_dir = baseline.get("directional_acc_net", baseline.get("directional_acc"))
            c_rmse = cur.get("net_rmse", cur.get("rmse"))
            c_dir = cur.get("net_dir_acc", cur.get("dir_acc"))

            if b_rmse is None or b_dir is None or c_rmse is None or c_dir is None:
                # baseline missing metrics; just keep watching
                continue

            try:
                b_rmse = float(b_rmse)
                b_dir = float(b_dir)
                c_rmse = float(c_rmse)
                c_dir = float(c_dir)
            except Exception as e:
                _warn_nonfatal(
                    "post_promotion_monitor_candidate_metrics_parse_failed",
                    e,
                    once_key="candidate_metrics_parse",
                    baseline_rmse=repr(b_rmse)[:120],
                    candidate_rmse=repr(c_rmse)[:120],
                )
                continue

            rmse_bad = c_rmse > (b_rmse * (1.0 + float(degrade_rmse_pct)))
            dir_bad = c_dir < (b_dir - float(degrade_dir_drop))

            if rmse_bad or dir_bad:
                # Immediate rollback on first confirmed breach (policy)
                rb = auto_rollback(
                    actor="system",
                    model_name=str(model_name),
                    regime=str(regime),
                    watch_id=int(wid),
                    reason={
                        "current": cur,
                        "baseline": baseline,
                        "thresholds": {
                            "rmse_pct": float(degrade_rmse_pct),
                            "dir_drop": float(degrade_dir_drop),
                        },
                        "policy": "immediate_first_breach",
                    },
                )

                # Activate regime kill-switch on rollback
                if rb:
                    try:
                        activate(
                            "regime",
                            str(regime),
                            reason="auto_post_promo_degradation",
                            actor="system",
                            meta={
                                "watch_id": int(wid),
                                "model_name": str(model_name),
                                "policy": "immediate_first_breach",
                            },
                            action="AUTO",
                            con=con,
                        )
                    except Exception as e:
                        _warn_nonfatal(
                            "post_promotion_monitor_kill_switch_activation_failed",
                            e,
                            watch_id=int(wid),
                            regime=str(regime),
                            model_name=str(model_name),
                        )
    finally:
        con.close()

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
