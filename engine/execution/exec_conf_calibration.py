"""
FILE: exec_conf_calibration.py

Execution subsystem module for `exec_conf_calibration`.
"""

# dev_core/exec_conf_calibration.py
"""
Execution-aware confidence calibration.

Goal:
- Join predictions(confidence) with labels_exec(net_ret) and learn how
  confidence maps to realized net outcomes (after execution costs).

Stores latest curve in SQLite table exec_conf_calib.

This is additive: no changes to predictor logic. It is used for dashboard
visibility and ops review; you may later use it to adjust sizing gates.
"""

import json
import time
import logging
from typing import Dict, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect

LOG = get_logger("engine.execution.exec_conf_calibration")


def _ensure_exec_conf_calib(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS exec_conf_calib (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          lookback_days INTEGER NOT NULL,
          buckets INTEGER NOT NULL,
          n_total INTEGER NOT NULL,
          x_json TEXT NOT NULL,
          winrate_json TEXT NOT NULL,
          mean_net_ret_json TEXT NOT NULL
        )
        """
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_exec_conf_calib_ts ON exec_conf_calib(ts_ms)")


def learn_exec_conf_calibration(
    lookback_days: int = 14,
    buckets: int = 10,
    now_ts_ms: Optional[int] = None,
) -> Dict:
    now_ms = int(now_ts_ms or int(time.time() * 1000))
    lookback_ms = int(lookback_days) * 86400 * 1000
    cutoff_ms = now_ms - lookback_ms

    buckets = max(2, min(50, int(buckets)))
    lookback_days = max(1, min(365, int(lookback_days)))

    con = connect()
    try:
        _ensure_exec_conf_calib(con)
        con.commit()

        try:
            rows = con.execute(
                """
                SELECT p.confidence, e.net_ret
                FROM predictions p
                JOIN labels_exec e
                  ON e.event_id = p.event_id
                 AND e.symbol = p.symbol
                 AND e.horizon_s = p.horizon_s
                WHERE p.ts_ms >= ?
                  AND e.realized = 1
                """
                ,
                (int(cutoff_ms),),
            ).fetchall()
        except Exception:
            rows = []

        vals = []
        for c, nr in rows or []:
            try:
                cc = float(c)
                rr = float(nr)
            except Exception as e:
                log_failure(
                    LOG,
                    event="exec_conf_calibration_row_parse_failed",
                    code="EXEC_CONF_CALIBRATION_ROW_PARSE_FAILED",
                    message="Execution confidence calibration row parse failed.",
                    error=e,
                    level=logging.WARNING,
                    component="engine.execution.exec_conf_calibration",
                    persist=False,
                )
                continue
            if cc != cc or rr != rr:
                continue
            vals.append((max(0.0, min(1.0, cc)), rr))

        # This learns an observational calibration curve from realized net
        # outcomes. It is governance/monitoring data, not an online predictor.
        # bucket edges in [0..1]
        xs = [i / buckets for i in range(buckets + 1)]

        win = [0] * buckets
        tot = [0] * buckets
        sum_ret = [0.0] * buckets

        for cc, rr in vals:
            idx = int(min(buckets - 1, max(0, int(cc * buckets))))
            tot[idx] += 1
            sum_ret[idx] += float(rr)
            if rr > 0:
                win[idx] += 1

        winrate = []
        mean_net_ret = []
        for i in range(buckets):
            n = tot[i]
            winrate.append((win[i] / n) if n else None)
            mean_net_ret.append((sum_ret[i] / n) if n else None)

        payload = {
            "ok": True,
            "ts_ms": now_ms,
            "lookback_days": lookback_days,
            "buckets": buckets,
            "n_total": int(len(vals)),
            "curve": [
                {
                    "conf_lo": xs[i],
                    "conf_hi": xs[i + 1],
                    "n": int(tot[i]),
                    "winrate": (None if winrate[i] is None else float(winrate[i])),
                    "mean_net_ret": (None if mean_net_ret[i] is None else float(mean_net_ret[i])),
                }
                for i in range(buckets)
            ],
        }

        

        con.execute(
            """
            INSERT INTO exec_conf_calib
              (ts_ms, lookback_days, buckets, n_total, x_json, winrate_json, mean_net_ret_json)
            VALUES (?,?,?,?,?,?,?)
            """
            ,
            (
                int(now_ms),
                int(lookback_days),
                int(buckets),
                int(len(vals)),
                json.dumps(xs),
                json.dumps(winrate),
                json.dumps(mean_net_ret),
            ),
        )
        con.commit()

        return payload
    finally:
        con.close()


def get_latest_exec_conf_calib() -> Dict:
    con = connect()
    try:
        _ensure_exec_conf_calib(con)
        con.commit()

        try:
            row = con.execute(
                """
                SELECT ts_ms, lookback_days, buckets, n_total, x_json, winrate_json, mean_net_ret_json
                FROM exec_conf_calib
                ORDER BY ts_ms DESC
                LIMIT 1
                """
            ).fetchone()
        except Exception:
            row = None

        if not row:
            return {"ok": True, "curve": None}

        ts_ms, lookback_days, buckets, n_total, xj, wj, mj = row

        try:
            xs = [float(x) for x in json.loads(xj or "[]")]
        except Exception:
            xs = []

        try:
            winrate = json.loads(wj or "[]")
        except Exception:
            winrate = []

        try:
            mean_net_ret = json.loads(mj or "[]")
        except Exception:
            mean_net_ret = []

        curve = []
        for i in range(int(buckets or 0)):
            clo = xs[i] if i < len(xs) else (i / max(1, int(buckets or 1)))
            chi = xs[i + 1] if (i + 1) < len(xs) else ((i + 1) / max(1, int(buckets or 1)))
            curve.append({
                "conf_lo": float(clo),
                "conf_hi": float(chi),
                "winrate": (None if i >= len(winrate) else winrate[i]),
                "mean_net_ret": (None if i >= len(mean_net_ret) else mean_net_ret[i]),
            })

        return {
            "ok": True,
            "ts_ms": int(ts_ms or 0),
            "lookback_days": int(lookback_days or 0),
            "buckets": int(buckets or 0),
            "n_total": int(n_total or 0),
            "curve": curve,
        }
    finally:
        con.close()
