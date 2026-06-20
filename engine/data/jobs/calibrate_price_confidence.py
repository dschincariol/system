"""
FILE: calibrate_price_confidence.py

Data job entrypoint for `calibrate_price_confidence`.
"""

# calibrate_price_confidence.py

import os
import time
import json
import math
import logging
from typing import Dict, Any, List, Tuple, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import (
    acquire_job_lock,
    connect,
    init_db,
    release_job_lock,
)

JOB_NAME = "calibrate_price_confidence"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [calibrate_price_confidence] %(message)s",
)
LOG = get_logger("engine.data.jobs.calibrate_price_confidence")
_WARNED_NONFATAL_KEYS: set[str] = set()

# Controls
MAX_ROWS = int(os.environ.get("PRICE_CALIB_MAX_ROWS", "4000"))
BIN_COUNT = int(os.environ.get("PRICE_CALIB_BINS", "10"))
MIN_SAMPLES = int(os.environ.get("PRICE_CALIB_MIN_SAMPLES", "120"))
LOOKUP_WINDOW_MS = int(os.environ.get("PRICE_CALIB_PRICE_LOOKUP_MS", str(10 * 60 * 1000)))  # 10m fallback window

RET_Z_FLOOR = float(os.environ.get("PRICE_CALIB_RET_Z_FLOOR", "1e-6"))


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="calibrate_price_confidence_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.data.jobs.calibrate_price_confidence",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _nearest_price_at_or_after(con, symbol: str, ts_ms: int) -> Optional[Tuple[int, float]]:
    """
    Return (ts_ms, price) at or after ts_ms, within LOOKUP_WINDOW_MS, else None.
    """
    row = con.execute(
        """
        SELECT ts_ms, price
        FROM prices
        WHERE symbol=?
          AND ts_ms >= ?
          AND ts_ms <= ?
        ORDER BY ts_ms ASC
        LIMIT 1
        """,
        (symbol, int(ts_ms), int(ts_ms + LOOKUP_WINDOW_MS)),
    ).fetchone()
    if not row:
        return None
    try:
        return int(row[0]), float(row[1])
    except Exception as e:
        _warn_nonfatal(
            "CALIBRATE_PRICE_CONFIDENCE_PRICE_PARSE_FAILED",
            e,
            once_key="nearest_price_parse",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        return None


def _rolling_vol_proxy(con, symbol: str, ts_ms: int, lookback_ms: int = 30 * 60 * 1000) -> float:
    """
    Simple proxy: stddev of 1-step returns in lookback window.
    """
    rows = con.execute(
        """
        SELECT price
        FROM prices
        WHERE symbol=?
          AND ts_ms >= ?
          AND ts_ms <= ?
        ORDER BY ts_ms ASC
        LIMIT 500
        """,
        (symbol, int(ts_ms - lookback_ms), int(ts_ms)),
    ).fetchall()

    prices: List[float] = []
    for r in rows or []:
        try:
            if r and r[0] is not None:
                prices.append(float(r[0]))
        except Exception as e:
            _warn_nonfatal(
                "CALIBRATE_PRICE_CONFIDENCE_VOL_PRICE_PARSE_FAILED",
                e,
                once_key="rolling_vol_price_parse",
                symbol=str(symbol),
                ts_ms=int(ts_ms),
                price_row=str(r)[:200],
            )
            continue

    if len(prices) < 6:
        return 0.0

    rets = []
    for i in range(1, len(prices)):
        p0 = prices[i - 1]
        p1 = prices[i]
        if p0 == 0:
            continue
        rets.append((p1 / p0) - 1.0)

    if len(rets) < 5:
        return 0.0

    mu = sum(rets) / len(rets)
    var = sum((x - mu) ** 2 for x in rets) / max(1, (len(rets) - 1))
    return float(math.sqrt(max(0.0, var)))


def _binning_curve(samples: List[Tuple[float, int]], bins: int) -> Dict[str, Any]:
    """
    samples: [(confidence, dir_correct_01)]
    Returns payload with bins in ascending conf.
    """
    samples = [(float(c), int(y)) for (c, y) in samples if c == c]  # NaN guard
    samples.sort(key=lambda t: t[0])

    n = len(samples)
    if n <= 0:
        return {"method": "binning_v1", "n": 0, "bins": []}

    bins = max(2, min(50, int(bins)))
    out_bins = []
    for b in range(bins):
        lo_i = int((b * n) / bins)
        hi_i = int(((b + 1) * n) / bins)
        if hi_i <= lo_i:
            continue
        chunk = samples[lo_i:hi_i]
        cs = [c for (c, _) in chunk]
        ys = [y for (_, y) in chunk]
        n_b = len(chunk)
        win = float(sum(ys)) / max(1, n_b)
        out_bins.append(
            {
                "conf_lo": float(min(cs)),
                "conf_hi": float(max(cs)),
                "conf_mid": float(sum(cs) / max(1, n_b)),
                "winrate": float(win),
                "n": int(n_b),
            }
        )

    return {
        "method": "binning_v1",
        "n": int(n),
        "bins": out_bins,
    }


def build_labels_and_calibration():
    init_db()
    con = connect()
    try:
        # Calibration is built from recent live predictions so the confidence
        # curve reflects the model the system is actually using operationally.
        # Pull recent predictions (most useful for operational calibration)
        rows = con.execute(
            """
            SELECT ts_ms, symbol, horizon_s, predicted_z, confidence
            FROM predictions
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (int(MAX_ROWS),),
        ).fetchall()

        if not rows:
            logging.info("no predictions found")
            return

        now_ms = _now_ms()

        # Build labels_price
        inserted_labels = 0
        inserted_prr = 0

        for ts_pred_ms, symbol, horizon_s, pred_z, conf in rows:
            try:
                ts_pred_ms = int(ts_pred_ms)
                symbol = str(symbol or "").upper().strip()
                horizon_s = int(horizon_s)
                if not symbol or horizon_s <= 0:
                    continue
            except Exception as e:
                _warn_nonfatal(
                    "CALIBRATE_PRICE_CONFIDENCE_LABEL_ROW_PARSE_FAILED",
                    e,
                    once_key="label_row_parse",
                    ts_pred_ms=ts_pred_ms,
                    symbol=str(symbol),
                    horizon_s=horizon_s,
                )
                continue

            ts_eval_ms = ts_pred_ms + horizon_s * 1000

            ep = _nearest_price_at_or_after(con, symbol, ts_pred_ms)
            xp = _nearest_price_at_or_after(con, symbol, ts_eval_ms)
            if not ep or not xp:
                continue

            entry_ts, entry_price = ep
            exit_ts, exit_price = xp
            if entry_price <= 0:
                continue

            ret = (exit_price / entry_price) - 1.0
            vol = _rolling_vol_proxy(con, symbol, ts_eval_ms)
            ret_z = float(ret / max(RET_Z_FLOOR, (vol if vol > 0 else 0.0) or RET_Z_FLOOR))

            direction = 1 if ret > 0 else (-1 if ret < 0 else 0)

            meta = {
                "entry_ts_ms": int(entry_ts),
                "exit_ts_ms": int(exit_ts),
                "pred_z": float(pred_z) if pred_z is not None else None,
                "confidence": float(conf) if conf is not None else None,
                "vol_proxy": float(vol),
            }

            con.execute(
                """
                INSERT OR REPLACE INTO labels_price(
                  ts_pred_ms, ts_eval_ms, symbol, horizon_s,
                  entry_price, exit_price, ret, ret_z, dir, meta_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(ts_pred_ms),
                    int(ts_eval_ms),
                    symbol,
                    int(horizon_s),
                    float(entry_price),
                    float(exit_price),
                    float(ret),
                    float(ret_z),
                    int(direction),
                    json.dumps(meta, separators=(",", ":"), sort_keys=True),
                ),
            )
            inserted_labels += 1

            # price_realized_returns keyed on eval ts (so it lines up with outcome time)
            con.execute(
                """
                INSERT OR REPLACE INTO price_realized_returns(ts_ms, symbol, ret)
                VALUES (?,?,?)
                """,
                (int(ts_eval_ms), symbol, float(ret)),
            )
            inserted_prr += 1

        con.commit()
        logging.info("labels_price upserted=%s; price_realized_returns upserted=%s", inserted_labels, inserted_prr)

        # Labels and calibration are built in the same pass so the confidence
        # table stays aligned with the exact realized-return sample set used.
        # Build calibration curves by joining predictions -> labels_price
        join_rows = con.execute(
            """
            SELECT p.symbol, p.horizon_s, p.confidence, lp.ret
            FROM predictions p
            JOIN labels_price lp
              ON lp.symbol = p.symbol
             AND lp.horizon_s = p.horizon_s
             AND lp.ts_pred_ms = p.ts_ms
            ORDER BY p.ts_ms DESC
            LIMIT ?
            """,
            (int(MAX_ROWS),),
        ).fetchall()

        if not join_rows:
            logging.info("no joined (prediction,label) samples yet")
            return

        buckets: Dict[Tuple[str, int], List[Tuple[float, int]]] = {}
        for sym, h, conf, ret in join_rows:
            try:
                sym = str(sym).upper().strip()
                h = int(h)
                conf = float(conf)
                ret = float(ret)
            except Exception as e:
                _warn_nonfatal(
                    "CALIBRATE_PRICE_CONFIDENCE_CALIBRATION_ROW_PARSE_FAILED",
                    e,
                    once_key="calibration_row_parse",
                    symbol=str(sym),
                    horizon_s=h,
                )
                continue
            # "correct direction" proxy: predicted_z sign isn't available here; we use ret sign only for winrate,
            # and confidence is treated as “probability of a win”.
            # This is intentionally simple: it measures how confidence correlates with positive outcomes.
            y = 1 if ret > 0 else 0
            buckets.setdefault((sym, h), []).append((conf, y))

        updated = 0
        for (sym, h), samples in buckets.items():
            if len(samples) < int(MIN_SAMPLES):
                continue
            payload = _binning_curve(samples, bins=int(BIN_COUNT))
            payload["symbol"] = sym
            payload["horizon_s"] = int(h)
            payload["updated_ts_ms"] = int(now_ms)

            con.execute(
                """
                INSERT OR REPLACE INTO confidence_calibration(
                  symbol, horizon_s, method, updated_ts_ms, payload_json
                ) VALUES (?,?,?,?,?)
                """,
                (sym, int(h), "binning_v1", int(now_ms), json.dumps(payload, separators=(",", ":"), sort_keys=True)),
            )
            updated += 1

        con.commit()
        logging.info("confidence_calibration updated=%s", updated)

    finally:
        con.close()


def main():
    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        logging.error("another instance is holding the job lock; exiting")
        raise SystemExit(2)

    try:
        build_labels_and_calibration()
    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as e:
            _warn_nonfatal("CALIBRATE_PRICE_CONFIDENCE_LOCK_RELEASE_FAILED", e, job=JOB_NAME)


if __name__ == "__main__":
    main()
