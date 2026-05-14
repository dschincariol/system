"""
FILE: compute_gdelt_macro.py

Operational helper script for `compute_gdelt_macro`.
"""

import os
import time
import json
import signal
import logging
import statistics
import threading
from typing import Any, Dict, List, Tuple

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

JOB_NAME = "compute_gdelt_macro"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "10.0"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

BUCKET_SEC = int(os.environ.get("GDELT_MACRO_BUCKET_SEC", "900"))          # 15m
LOOKBACK_S = int(os.environ.get("GDELT_MACRO_LOOKBACK_S", "21600"))        # 6h
SLEEP_S = float(os.environ.get("GDELT_MACRO_SLEEP_S", "60.0"))


_STOP_EVENT = threading.Event()
MAX_RUNTIME_S = float(os.environ.get("GDELT_MACRO_MAX_RUNTIME_S", "0"))
LOG = get_logger("ops.compute_gdelt_macro")


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="compute_gdelt_macro_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="ops.compute_gdelt_macro",
        extra=extra or None,
        persist=False,
    )


def _handle_stop_signal(signum, _frame) -> None:
    logging.warning("stop signal received signum=%s", signum)
    _STOP_EVENT.set()


for _sig in (getattr(signal, "SIGINT", None), getattr(signal, "SIGTERM", None)):
    if _sig is None:
        continue
    try:
        signal.signal(_sig, _handle_stop_signal)
    except Exception:
        logging.exception("failed to install signal handler signum=%s", _sig)


def _bucket_start(ts_ms: int, bucket_sec: int) -> int:
    b = int(bucket_sec) * 1000
    if b <= 0:
        b = 900 * 1000
    return int(ts_ms // b) * b


def _is_conflict(meta: Dict[str, Any]) -> bool:
    gd = (meta or {}).get("gdelt") or {}
    themes = gd.get("themes") or []
    theme = str(gd.get("theme") or "").lower()
    # allow list: war, conflict, attacks, terrorism, sanctions, etc.
    keys = ("conflict", "war", "attack", "terror", "missile", "sanction", "military", "strike")
    if any(k in theme for k in keys):
        return True
    try:
        for t in themes:
            if any(k in str(t).lower() for k in keys):
                return True
    except Exception as e:
        _warn_nonfatal("COMPUTE_GDELT_MACRO_CONFLICT_THEME_PARSE_FAILED", e, theme_count=int(len(themes or [])))
    return False


def _is_econ(meta: Dict[str, Any]) -> bool:
    gd = (meta or {}).get("gdelt") or {}
    themes = gd.get("themes") or []
    theme = str(gd.get("theme") or "").lower()
    keys = ("econ", "econom", "inflation", "rates", "cpi", "gdp", "recession", "earnings", "guidance", "jobs")
    if any(k in theme for k in keys):
        return True
    try:
        for t in themes:
            if any(k in str(t).lower() for k in keys):
                return True
    except Exception as e:
        _warn_nonfatal("COMPUTE_GDELT_MACRO_ECON_THEME_PARSE_FAILED", e, theme_count=int(len(themes or [])))
    return False


def _compute_bucket(con, bucket_ts_ms: int, bucket_sec: int) -> Tuple[int, float, float, float, float]:
    rows = con.execute(
        """
        SELECT meta_json
        FROM events
        WHERE source='gdelt'
          AND ts_ms >= ?
          AND ts_ms < ?
        """,
        (int(bucket_ts_ms), int(bucket_ts_ms + bucket_sec * 1000)),
    ).fetchall()

    if not rows:
        return 0, 0.0, 0.0, 0.0, 0.0

    tones: List[float] = []
    conflict_n = 0
    econ_n = 0
    doc_n = 0

    # Bucket features are intentionally coarse summaries so later models can join
    # them cheaply without reprocessing every raw GDELT event.
    for (mj,) in rows:
        doc_n += 1
        try:
            meta = json.loads(mj) if mj else {}
        except Exception:
            meta = {}
        gd = (meta or {}).get("gdelt") or {}
        try:
            t = float(gd.get("tone", 0.0) or 0.0)
        except Exception:
            t = 0.0
        tones.append(float(t))
        if _is_conflict(meta):
            conflict_n += 1
        if _is_econ(meta):
            econ_n += 1

    tone_mean = float(statistics.mean(tones)) if tones else 0.0
    tone_std = float(statistics.pstdev(tones)) if len(tones) >= 2 else 0.0
    conflict_share = float(conflict_n) / float(max(1, doc_n))
    econ_share = float(econ_n) / float(max(1, doc_n))
    return int(doc_n), tone_mean, tone_std, conflict_share, econ_share


def main() -> None:
    init_db()

    if not acquire_job_lock(JOB_NAME, OWNER, PID, stale_after_s=LOCK_STALE_AFTER_S):
        logging.error("another instance is holding the job lock; exiting")
        raise SystemExit(2)

    started_s = time.time()
    last_hb_s = 0.0
    try:
        while not _STOP_EVENT.is_set():
            if MAX_RUNTIME_S > 0.0 and (time.time() - started_s) >= MAX_RUNTIME_S:
                logging.warning("max runtime reached; stopping max_runtime_s=%s", MAX_RUNTIME_S)
                break

            con = connect()
            try:
                now_ms = int(time.time() * 1000)
                cutoff_ms = now_ms - int(LOOKBACK_S) * 1000

                b = _bucket_start(int(cutoff_ms), int(BUCKET_SEC))
                end = _bucket_start(int(now_ms), int(BUCKET_SEC))

                wrote = 0
                # Recompute over a bounded recent window each pass. The write path
                # is idempotent, so reruns refresh buckets without accumulating drift.
                while b <= end:
                    if _STOP_EVENT.is_set():
                        break

                    now_s = time.time()
                    if (now_s - last_hb_s) >= HEARTBEAT_EVERY_S:
                        touch_job_lock(JOB_NAME, OWNER, PID)
                        put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps({"bucket_ts_ms": int(b)}))
                        last_hb_s = now_s

                    doc_n, tone_mean, tone_std, conflict_share, econ_share = _compute_bucket(con, int(b), int(BUCKET_SEC))
                    con.execute(
                        """
                        INSERT OR REPLACE INTO gdelt_macro_features(
                          bucket_ts_ms, bucket_sec,
                          doc_count, tone_mean, tone_std,
                          conflict_share, econ_share
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            int(b),
                            int(BUCKET_SEC),
                            int(doc_n),
                            float(tone_mean),
                            float(tone_std),
                            float(conflict_share),
                            float(econ_share),
                        ),
                    )
                    wrote += 1
                    b += int(BUCKET_SEC) * 1000

                try:
                    con.commit()
                except Exception:
                    try:
                        con.rollback()
                    except Exception:
                        logging.exception("rollback failed after commit failure")
                    logging.exception("gdelt macro commit failed")
                    raise

                logging.info("buckets_upserted=%s", wrote)
            finally:
                try:
                    con.close()
                except Exception:
                    logging.exception("failed to close gdelt macro db connection")

            _STOP_EVENT.wait(float(SLEEP_S))
    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception:
            logging.exception("failed to release job lock")


if __name__ == "__main__":
    main()
