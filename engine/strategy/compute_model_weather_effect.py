# CREATE NEW FILE: compute_model_weather_effect.py
"""
Evaluates weather contribution: base model vs wx model.

Writes to `model_weather_effect`:
- base_rmse vs wx_rmse
- base_spearman vs wx_spearman
- deltas (wx - base)

Assumptions:
- Predictions live in temporal_predictions(expected_z) keyed by (event_id,symbol,horizon_s,model_kind)
- Realized target is labels.impact_z (or labels.realized_ret fallback if impact_z is NULL)
- You control which model_kind is "base" and which is "wx" via env:
    BASE_MODEL_KIND=mlp
    WX_MODEL_KIND=mlp_wx
"""

import os
import time
import json
import math
import logging
from typing import Any, List, Tuple

import numpy as np
from engine.runtime.failure_diagnostics import log_failure

from engine.runtime.storage import (
    connect,
    init_db,
    acquire_job_lock,
    release_job_lock,
    touch_job_lock,
    put_job_heartbeat,
)

LOG = logging.getLogger("compute_model_weather_effect")
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))

JOB_NAME = "compute_model_weather_effect"
OWNER = os.environ.get("JOB_OWNER", "system")
PID = os.getpid()

BASE_KIND = os.environ.get("BASE_MODEL_KIND", "mlp").strip()
WX_KIND = os.environ.get("WX_MODEL_KIND", "mlp_wx").strip()

EVAL_DAYS = int(os.environ.get("WX_EFFECT_EVAL_DAYS", "14"))
MIN_N = int(os.environ.get("WX_EFFECT_MIN_N", "50"))

POLL_INTERVAL_S = int(os.environ.get("WX_EFFECT_INTERVAL_S", "1800"))  # 30 min
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "300"))
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
        component="engine.strategy.compute_model_weather_effect",
        extra=extra,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _utc_ms() -> int:
    return int(time.time() * 1000)


def _spearman(a: np.ndarray, b: np.ndarray) -> float:
    if a.size < 3 or b.size < 3:
        return 0.0
    try:
        ra = a.argsort().argsort().astype(float)
        rb = b.argsort().argsort().astype(float)
        ra -= np.mean(ra)
        rb -= np.mean(rb)
        denom = float(np.sqrt(np.sum(ra * ra) * np.sum(rb * rb)))
        if denom <= 1e-12:
            return 0.0
        return float(np.sum(ra * rb) / denom)
    except Exception as e:
        _warn_nonfatal("compute_model_weather_effect_corr_failed", e, once_key="corr")
        return 0.0


def _rmse(y: np.ndarray, yhat: np.ndarray) -> float:
    if y.size < 1:
        return 0.0
    e = yhat - y
    v = float(np.sqrt(np.mean(e * e)))
    return v if math.isfinite(v) else 0.0


def _load_joined(con, horizon_s: int, ts_min: int) -> List[Tuple[float, float, float]]:
    """
    Returns rows of (y, base_pred, wx_pred)
    """
    rows = con.execute(
        """
        WITH y AS (
          SELECT event_id, symbol, horizon_s,
                 COALESCE(impact_z, realized_ret) AS y
          FROM labels
          WHERE horizon_s=?
            AND created_at_ms >= ?
        ),
        p_base AS (
          SELECT event_id, symbol, horizon_s, expected_z AS p
          FROM temporal_predictions
          WHERE horizon_s=? AND model_kind=?
        ),
        p_wx AS (
          SELECT event_id, symbol, horizon_s, expected_z AS p
          FROM temporal_predictions
          WHERE horizon_s=? AND model_kind=?
        )
        SELECT y.y, p_base.p, p_wx.p
        FROM y
        JOIN p_base
          ON p_base.event_id=y.event_id AND p_base.symbol=y.symbol AND p_base.horizon_s=y.horizon_s
        JOIN p_wx
          ON p_wx.event_id=y.event_id AND p_wx.symbol=y.symbol AND p_wx.horizon_s=y.horizon_s
        WHERE y.y IS NOT NULL
        """,
        (int(horizon_s), int(ts_min), int(horizon_s), str(BASE_KIND), int(horizon_s), str(WX_KIND)),
    ).fetchall() or []

    out: List[Tuple[float, float, float]] = []
    for y, b, w in rows:
        try:
            out.append((float(y), float(b), float(w)))
        except Exception as e:
            _warn_nonfatal(
                "compute_model_weather_effect_join_row_parse_failed",
                e,
                once_key="joined_row_parse",
                row=repr((y, b, w))[:200],
            )
            continue
    return out


def run_once() -> None:
    con = connect()
    try:
        now = _utc_ms()
        ts_min = now - int(EVAL_DAYS) * 24 * 3600 * 1000

        horizons = con.execute(
            "SELECT DISTINCT horizon_s FROM labels ORDER BY horizon_s ASC"
        ).fetchall() or []

        for (h,) in horizons:
            try:
                horizon_s = int(h)
            except Exception as e:
                _warn_nonfatal(
                    "compute_model_weather_effect_horizon_parse_failed",
                    e,
                    once_key="horizon_parse",
                    horizon=repr(h)[:120],
                )
                continue

            rows = _load_joined(con, horizon_s=horizon_s, ts_min=ts_min)
            if len(rows) < int(MIN_N):
                continue

            y = np.asarray([r[0] for r in rows], dtype=float)
            pb = np.asarray([r[1] for r in rows], dtype=float)
            pw = np.asarray([r[2] for r in rows], dtype=float)

            base_rmse = _rmse(y, pb)
            wx_rmse = _rmse(y, pw)
            base_sp = _spearman(y, pb)
            wx_sp = _spearman(y, pw)

            con.execute(
                """
                INSERT OR REPLACE INTO model_weather_effect(
                  key_type, key, horizon_s, ts_ms,
                  base_rmse, wx_rmse, rmse_delta,
                  base_spearman, wx_spearman, spearman_delta,
                  n_eval
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "global",
                    "global",
                    int(horizon_s),
                    int(now),
                    float(base_rmse),
                    float(wx_rmse),
                    float(wx_rmse - base_rmse),
                    float(base_sp),
                    float(wx_sp),
                    float(wx_sp - base_sp),
                    int(len(rows)),
                ),
            )

        con.commit()
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("compute_model_weather_effect_db_close_failed", e)


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
                        {"base_kind": BASE_KIND, "wx_kind": WX_KIND, "eval_days": EVAL_DAYS},
                        separators=(",", ":"),
                    ),
                )
                last_hb = now

            run_once()
            time.sleep(float(POLL_INTERVAL_S))
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    main()
