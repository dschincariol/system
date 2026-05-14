"""
FILE: calibrate_confidence_from_prices.py

Learns confidence calibration curves from price-derived outcomes. This job
builds simple, stable bin-based mappings from raw model confidence to empirical
win rate.
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

from engine.data.universe import get_active_symbols

JOB_NAME = "calibrate_confidence_from_prices"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [calib_prices] %(message)s",
)
LOG = get_logger("engine.strategy.jobs.calibrate_confidence_from_prices")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="calibrate_confidence_from_prices_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.jobs.calibrate_confidence_from_prices",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))

# training controls
MIN_SAMPLES = int(os.environ.get("CALIB_MIN_SAMPLES", "200"))
BINS = int(os.environ.get("CALIB_BINS", "12"))
METHOD = "binning_v1"

# Lookback window for training
LOOKBACK_DAYS = int(os.environ.get("CALIB_LOOKBACK_DAYS", "30"))


def _clip01(x: float) -> float:
    try:
        x = float(x)
    except Exception as e:
        _warn_nonfatal("CALIBRATE_CONFIDENCE_FROM_PRICES_SAFE_FLOAT_FAILED", e, once_key="safe_float", value=repr(x)[:120])
        return 0.0
    if x != x:
        return 0.0
    return max(0.0, min(1.0, x))


def _bin_edges(n_bins: int) -> List[float]:
    # uniform edges in [0,1]
    n = max(2, int(n_bins))
    return [i / n for i in range(n + 1)]


def _fit_binning(samples: List[Tuple[float, int]], n_bins: int) -> Dict[str, Any]:
    """
    samples: [(conf in [0,1], win in {0,1})]
    returns payload dict with edges + per-bin stats + monotone-smoothed win rates
    """
    edges = _bin_edges(n_bins)
    bins = [{"n": 0, "wins": 0, "win_rate": 0.5} for _ in range(len(edges) - 1)]

    for conf, win in samples:
        c = _clip01(conf)
        w = 1 if int(win) != 0 else 0
        # find bin
        idx = min(len(bins) - 1, max(0, int(c * len(bins))))
        bins[idx]["n"] += 1
        bins[idx]["wins"] += w

    # raw rates with Laplace smoothing
    for b in bins:
        n = b["n"]
        wins = b["wins"]
        # alpha=1 beta=1 smoothing -> (wins+1)/(n+2)
        b["win_rate_raw"] = (wins + 1.0) / (n + 2.0)
        b["win_rate"] = b["win_rate_raw"]

    # monotone-ish smoothing: enforce nondecreasing win_rate with increasing confidence
    # (simple pool-adjacent-violators style, but cheap)
    rates = [b["win_rate"] for b in bins]
    ns = [max(1, b["n"]) for b in bins]

    # forward pass
    for i in range(1, len(rates)):
        if rates[i] < rates[i - 1]:
            # blend toward previous weighted by counts
            tot = ns[i] + ns[i - 1]
            blended = (rates[i] * ns[i] + rates[i - 1] * ns[i - 1]) / tot
            rates[i] = blended
            rates[i - 1] = blended

    # backward pass
    for i in range(len(rates) - 2, -1, -1):
        if rates[i] > rates[i + 1]:
            tot = ns[i] + ns[i + 1]
            blended = (rates[i] * ns[i] + rates[i + 1] * ns[i + 1]) / tot
            rates[i] = blended
            rates[i + 1] = blended

    for i, r in enumerate(rates):
        bins[i]["win_rate"] = float(_clip01(r))

    return {
        "method": METHOD,
        "bins": len(bins),
        "edges": edges,
        "bin_stats": bins,
    }


def _calibrate_for_symbol(con, symbol: str, horizon_s: int, since_ms: int) -> bool:
    """
    Join predictions to labels_price to build (confidence, win) samples.
    """
    rows = con.execute(
        """
        SELECT p.confidence, p.predicted_z, lp.dir
        FROM predictions p
        JOIN labels_price lp
          ON lp.symbol = p.symbol
         AND lp.horizon_s = p.horizon_s
         AND lp.ts_pred_ms = p.ts_ms
        WHERE p.symbol = ?
          AND p.horizon_s = ?
          AND p.ts_ms >= ?
          AND p.confidence IS NOT NULL
          AND lp.dir IS NOT NULL
        ORDER BY p.ts_ms DESC
        """,
        (symbol, int(horizon_s), int(since_ms)),
    ).fetchall()

    if not rows or len(rows) < MIN_SAMPLES:
        return False

    samples: List[Tuple[float, int]] = []
    for conf, predicted_z, lp_dir in rows:
        try:
            c = _clip01(float(conf))
        except Exception as e:
            _warn_nonfatal(
                "CALIBRATE_CONFIDENCE_FROM_PRICES_ROW_PARSE_FAILED",
                e,
                once_key="row_parse",
                confidence=repr(conf)[:120],
                predicted_z=repr(predicted_z)[:120],
                lp_dir=repr(lp_dir)[:120],
            )
            continue
        try:
            z = float(predicted_z)
        except Exception:
            z = 0.0
        try:
            realized_dir = int(lp_dir)
        except Exception as e:
            _warn_nonfatal(
                "CALIBRATE_CONFIDENCE_FROM_PRICES_DIRECTION_PARSE_FAILED",
                e,
                once_key="direction_parse",
                lp_dir=repr(lp_dir)[:120],
            )
            continue

        pred_dir = 1 if z >= 0 else -1
        win = 1 if (realized_dir == pred_dir) else 0
        samples.append((c, win))

    if len(samples) < MIN_SAMPLES:
        return False

    payload = _fit_binning(samples, BINS)

    con.execute(
        """
        INSERT OR REPLACE INTO confidence_calibration(symbol, horizon_s, method, updated_ts_ms, payload_json)
        VALUES (?,?,?,?,?)
        """,
        (
            symbol,
            int(horizon_s),
            METHOD,
            int(time.time() * 1000),
            json.dumps(payload, separators=(",", ":"), sort_keys=True),
        ),
    )
    return True


def main():
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("options_poll must be launched by supervisor")
        raise SystemExit(1)

    init_db()

    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        logging.error("another instance holds lock; exiting")
        raise SystemExit(2)

    last_hb_s = 0.0

    try:
        con = connect()
        try:
            # Use active symbols universe
            symbols = get_active_symbols(con, limit=int(os.environ.get("CALIB_SYMBOL_LIMIT", "2000")))
            if not symbols:
                _warn_nonfatal(
                    "CALIBRATE_CONFIDENCE_FROM_PRICES_NO_ACTIVE_SYMBOLS",
                    RuntimeError("no active symbols found"),
                    job=JOB_NAME,
                )
                return

            since_ms = int((time.time() - LOOKBACK_DAYS * 86400) * 1000)

            updated = 0
            scanned = 0

            for sym in symbols:
                now_s = time.time()
                if now_s - last_hb_s >= HEARTBEAT_EVERY_S:
                    try:
                        touch_job_lock(JOB_NAME, OWNER, PID)
                        put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps({"symbol": sym}))
                    except Exception as e:
                        _warn_nonfatal("CALIBRATE_CONFIDENCE_FROM_PRICES_HEARTBEAT_FAILED", e, symbol=str(sym))
                    last_hb_s = now_s

                for h in (300, 3600):
                    scanned += 1
                    try:
                        if _calibrate_for_symbol(con, sym, h, since_ms):
                            updated += 1
                    except Exception as e:
                        _warn_nonfatal(
                            "CALIBRATE_CONFIDENCE_FROM_PRICES_SYMBOL_FAILED",
                            e,
                            symbol=str(sym),
                            horizon_s=int(h),
                        )

            con.commit()
            logging.info("done scanned=%s updated=%s", scanned, updated)

        finally:
            con.close()

    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as e:
            _warn_nonfatal("CALIBRATE_CONFIDENCE_FROM_PRICES_LOCK_RELEASE_FAILED", e, job=JOB_NAME)


if __name__ == "__main__":
    main()
"""
FILE: calibrate_confidence_from_prices.py

Job entrypoint wrapper for confidence calibration from realized prices.
"""
