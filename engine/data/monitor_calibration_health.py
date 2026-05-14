"""
FILE: monitor_calibration_health.py

Data subsystem module for `monitor_calibration_health`.
"""

# monitor_calibration_health.py

import os
import time
import json
import logging
from typing import Dict, Any, Optional, Tuple, List

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import connect, init_db, acquire_job_lock, release_job_lock
from engine.model_registry import rollback_champion
from engine.strategy.model_config import primary_active_model_name

JOB_NAME = "monitor_calibration_health"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [monitor_calibration_health] %(message)s",
)
LOGGER = logging.getLogger(__name__)
_WARNED_NONFATAL_KEYS: set[str] = set()

MODEL_NAME = os.environ.get("CALIB_HEALTH_MODEL_NAME", primary_active_model_name() or "embed_regressor")

# Which curves to check
SYMBOLS = [s.strip().upper() for s in os.environ.get("CALIB_HEALTH_SYMBOLS", "SPY,BTC,OIL").split(",") if s.strip()]
HORIZONS = [int(x) for x in os.environ.get("CALIB_HEALTH_HORIZONS", "3600").split(",") if x.strip()]

# Collapse heuristic
MIN_N = int(os.environ.get("CALIB_COLLAPSE_MIN_N", "200"))
MARGIN = float(os.environ.get("CALIB_COLLAPSE_MARGIN", "0.03"))  # top bin must exceed bottom by >= 3%
MAX_BAD_COUNT = int(os.environ.get("CALIB_COLLAPSE_BAD_COUNT", "3"))

RISK_KEY_PREFIX = "calib_health_bad_count"


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOGGER,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component=__name__,
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _read_curve(con, symbol: str, horizon_s: int) -> Optional[Dict[str, Any]]:
    row = con.execute(
        """
        SELECT payload_json
        FROM confidence_calibration
        WHERE symbol=? AND horizon_s=? AND method='binning_v1'
        """,
        (symbol, int(horizon_s)),
    ).fetchone()
    if not row:
        return None
    try:
        return json.loads(row[0] or "{}")
    except Exception as e:
        _warn_nonfatal(
            "MONITOR_CALIBRATION_HEALTH_CURVE_PARSE_FAILED",
            e,
            once_key="read_curve_parse",
        )
        return None


def _curve_collapsed(payload: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """
    Conservative: require enough samples, and require top winrate > bottom winrate + margin.
    """
    bins = payload.get("bins") or []
    try:
        n = int(payload.get("n") or 0)
    except Exception:
        n = 0

    if not bins or n < MIN_N or len(bins) < 3:
        return False, {"reason": "insufficient_data", "n": n, "bins": len(bins)}

    # Use first and last bins
    try:
        bottom = bins[0]
        top = bins[-1]
        wb = float(bottom.get("winrate") or 0.0)
        wt = float(top.get("winrate") or 0.0)
        nb = int(bottom.get("n") or 0)
        nt = int(top.get("n") or 0)
    except Exception as e:
        _warn_nonfatal(
            "MONITOR_CALIBRATION_HEALTH_CURVE_COLLAPSE_PARSE_FAILED",
            e,
            once_key="curve_collapsed_parse",
            curve_keys=list(payload.keys())[:10],
        )
        return False, {"reason": "parse_error"}

    collapsed = (wt <= (wb + float(MARGIN)))
    return bool(collapsed), {
        "n": n,
        "win_bottom": wb,
        "win_top": wt,
        "n_bottom": nb,
        "n_top": nt,
        "margin": float(MARGIN),
    }


def _risk_get(con, key: str) -> int:
    row = con.execute("SELECT value FROM risk_state WHERE key=?", (key,)).fetchone()
    if not row:
        return 0
    try:
        return int(row[0] or 0)
    except Exception as e:
        _warn_nonfatal(
            "MONITOR_CALIBRATION_HEALTH_RISK_GET_FAILED",
            e,
            once_key=f"risk_get:{key}",
            key=str(key),
        )
        return 0


def _risk_set(con, key: str, value: int) -> None:
    con.execute(
        "INSERT OR REPLACE INTO risk_state(key, value, updated_ts_ms) VALUES (?,?,?)",
        (key, int(value), _now_ms()),
    )


def main() -> None:
    init_db()

    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        logging.error("another instance is holding the job lock; exiting")
        raise SystemExit(2)

    con = connect()
    try:
        any_bad = False
        details: List[Dict[str, Any]] = []

        for sym in SYMBOLS:
            for h in HORIZONS:
                payload = _read_curve(con, sym, int(h))
                if not payload:
                    details.append({"symbol": sym, "horizon_s": int(h), "status": "missing"})
                    continue

                collapsed, info = _curve_collapsed(payload)
                details.append({"symbol": sym, "horizon_s": int(h), "collapsed": collapsed, **info})
                if collapsed:
                    any_bad = True

        key = f"{RISK_KEY_PREFIX}:{MODEL_NAME}"
        bad_count = _risk_get(con, key)

        if any_bad:
            bad_count += 1
            _risk_set(con, key, bad_count)
        else:
            bad_count = 0
            _risk_set(con, key, bad_count)

        con.commit()

        logging.info("calibration_health any_bad=%s bad_count=%s details=%s", any_bad, bad_count, details)

        if any_bad and bad_count >= MAX_BAD_COUNT:
            logging.error("CALIBRATION COLLAPSE -> rolling back champion for model=%s", MODEL_NAME)
            try:
                # This is intentionally one-way and operational: once calibration
                # collapses repeatedly, revert to the prior champion instead of
                # trying to auto-heal the current model in place.
                rollback_champion(MODEL_NAME)
                _risk_set(con, key, 0)
                con.commit()
            except Exception as e:
                logging.exception("rollback failed: %r", e)

    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal(
                "MONITOR_CALIBRATION_HEALTH_CLOSE_FAILED",
                e,
                once_key="monitor_calibration_health_close",
            )
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as e:
            _warn_nonfatal(
                "MONITOR_CALIBRATION_HEALTH_RELEASE_LOCK_FAILED",
                e,
                once_key="monitor_calibration_health_release_lock",
            )


if __name__ == "__main__":
    main()
