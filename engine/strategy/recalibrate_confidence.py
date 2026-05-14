"""
FILE: recalibrate_confidence.py

One-shot maintenance job that refreshes confidence calibration curves and
relevance statistics. It is intended to be safe and repeatable.
"""

import os
import time

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import init_db, acquire_job_lock, release_job_lock
from engine.training_guard import training_allowed
from engine.strategy.embed_regressor import train_embed_models
from engine.strategy.learning import learn_relevance_stats

LOG = get_logger("engine.strategy.recalibrate_confidence")
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="recalibrate_confidence_nonfatal",
        code=code,
        message=code,
        error=error,
        level=30,
        component="engine.strategy.recalibrate_confidence",
        extra=extra or None,
        persist=False,
    )


def main():
    if not training_allowed():
        print("[training_guard] training disabled")
        raise SystemExit(0)

    init_db()

    if not acquire_job_lock("recalibrate_confidence", OWNER, PID, ttl_s=30 * 60):
        print("[recalibrate_confidence] locked (already running?)")
        raise SystemExit(0)

    try:
        # The training code branches on this env flag, so the job sets it
        # explicitly instead of depending on operator shell state.
        os.environ["EMBED_CONF_CALIB"] = "1"

        # Mirror train-time defaults while still allowing operators to override
        # symbols and horizons without editing code.
        symbols = os.environ.get("EMBED_MODEL_SYMBOLS", "SPY,QQQ,IWM").split(",")
        symbols = [s.strip().upper() for s in symbols if s.strip()]

        horizons = os.environ.get("EMBED_MODEL_HORIZONS_S", "60,300,900").split(",")
        horizons = [int(x.strip()) for x in horizons if x.strip()]

        min_samples = int(os.environ.get("EMBED_MODEL_MIN_SAMPLES", "200"))
        alpha = float(os.environ.get("EMBED_MODEL_RIDGE_ALPHA", "1.0"))
        lookback_days = int(os.environ.get("EMBED_MODEL_LOOKBACK_DAYS", "30"))
        kind = str(os.environ.get("EMBED_MODEL_KIND", "ridge")).strip().lower()
        if kind not in ("ridge", "mlp"):
            kind = "ridge"

        t0 = time.time()
        res = train_embed_models(
            symbols=symbols,
            horizons=horizons,
            min_samples=min_samples,
            alpha=alpha,
            lookback_days=lookback_days,
            kind=kind,
        )
        dt = time.time() - t0
        print("[recalibrate_confidence] embed_models:", res, f"dt_s={dt:.2f}")

        # Relevance stats are ancillary; treat failure here as visible but non-fatal.
        try:
            rs = learn_relevance_stats()
            print("[recalibrate_confidence] relevance_stats: ok", ("keys=" + str(len(rs)) if isinstance(rs, dict) else ""))
        except Exception as e:
            print("[recalibrate_confidence] relevance_stats error:", str(e))

    finally:
        try:
            release_job_lock("recalibrate_confidence", OWNER, PID)
        except Exception as e:
            _warn_nonfatal("RECALIBRATE_CONFIDENCE_LOCK_RELEASE_FAILED", e)


if __name__ == "__main__":
    main()
