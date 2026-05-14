"""
FILE: compute_social_regime.py

Human-readable purpose:
Supervisor-run oneshot job that reads recent `social_features` buckets,
classifies the social regime for each symbol/time bucket, and writes the result
into `social_regimes` for downstream modeling and analysis.
"""

import os
import time
import json
import logging

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import (
    init_db,
    connect,
    acquire_job_lock,
    release_job_lock,
    touch_job_lock,
    put_job_heartbeat,
)
from engine.strategy.social_regime import classify_regime_from_features

JOB_NAME = "compute_social_regime"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))

SOCIAL_REGIME_LOOKBACK_S = int(os.environ.get("SOCIAL_REGIME_LOOKBACK_S", "21600"))  # 6h
SOCIAL_REGIME_BUCKET_SEC = int(os.environ.get("SOCIAL_REGIME_BUCKET_SEC", "300"))    # 5m

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [compute_social_regime] %(message)s",
)
LOG = get_logger("engine.strategy.compute_social_regime")


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="compute_social_regime_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.compute_social_regime",
        extra=extra or None,
        persist=False,
    )


def main():
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("options_poll must be launched by supervisor")
        raise SystemExit(1)

    init_db()

    if not acquire_job_lock(JOB_NAME, OWNER, PID, stale_after_s=LOCK_STALE_AFTER_S):
        logging.error("another instance is holding the job lock; exiting")
        raise SystemExit(2)

    last_hb_s = 0.0
    try:
        con = connect()

        now_ms = int(time.time() * 1000)
        cutoff_ms = now_ms - int(SOCIAL_REGIME_LOOKBACK_S) * 1000

        rows = con.execute(
            """
            SELECT
              symbol,
              bucket_ts_ms,
              bucket_sec,
              mention_rate_z,
              sentiment_mean,
              sentiment_dispersion,
              new_author_ratio,
              cross_platform_confirm
            FROM social_features
            WHERE bucket_sec = ?
              AND bucket_ts_ms >= ?
            ORDER BY bucket_ts_ms ASC
            """,
            (int(SOCIAL_REGIME_BUCKET_SEC), int(cutoff_ms)),
        ).fetchall()

        for (sym, bts, bsec, z, s, d, n, x) in rows or []:
            now_s = time.time()
            if (now_s - last_hb_s) >= HEARTBEAT_EVERY_S:
                # Long-running oneshots must refresh lock and heartbeat so the
                # supervisor does not treat them as dead or stale.
                touch_job_lock(JOB_NAME, OWNER, PID)
                put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps({"bucket_ts_ms": int(bts)}))
                last_hb_s = now_s

            sf = {
                "mention_rate_z": float(z or 0.0),
                "sentiment_mean": float(s or 0.0),
                "sentiment_dispersion": float(d or 0.0),
                "new_author_ratio": float(n or 0.0),
                "cross_platform_confirm": float(x or 0.0),
            }
            rg = classify_regime_from_features(sf)

            # Buckets are replaceable: re-running this job should converge to one
            # current regime row per `(symbol, bucket_ts_ms, bucket_sec)`.
            con.execute(
                """
                INSERT OR REPLACE INTO social_regimes(
                  symbol, bucket_ts_ms, bucket_sec,
                  regime, regime_conf,
                  mania_score, fear_score, churn_score
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(sym),
                    int(bts),
                    int(bsec),
                    str(rg.get("regime", "QUIET")),
                    float(rg.get("regime_conf", 0.0)),
                    float(rg.get("mania_score", 0.0)),
                    float(rg.get("fear_score", 0.0)),
                    float(rg.get("churn_score", 0.0)),
                ),
            )

        try:
            con.commit()
        except Exception as e:
            _warn_nonfatal("COMPUTE_SOCIAL_REGIME_COMMIT_FAILED", e, job=JOB_NAME)

    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("COMPUTE_SOCIAL_REGIME_CLOSE_FAILED", e, job=JOB_NAME)
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    main()
