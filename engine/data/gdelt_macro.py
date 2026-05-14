"""
FILE: gdelt_macro.py

Data subsystem module for `gdelt_macro`.
"""

import os
import statistics
import logging
from typing import Any, Dict, Optional, List

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect

DEFAULT_BUCKET_SEC = int(os.environ.get("GDELT_MACRO_BUCKET_SEC", "900"))  # 15m
ZWIN = int(os.environ.get("GDELT_MACRO_ZWIN", "64"))  # buckets
LOG = get_logger("engine.data.gdelt_macro")


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="gdelt_macro_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.data.gdelt_macro",
        extra=extra or None,
        persist=False,
    )


def _bucket_start(ts_ms: int, bucket_sec: int) -> int:
    b = int(bucket_sec) * 1000
    if b <= 0:
        b = int(DEFAULT_BUCKET_SEC) * 1000
    return int(ts_ms // b) * b


def _z(vals: List[float], x: float) -> float:
    if not vals:
        return 0.0
    mu = float(statistics.mean(vals))
    sd = float(statistics.pstdev(vals))
    if sd <= 1e-9:
        return 0.0
    return float((x - mu) / sd)


def get_gdelt_macro_snapshot(*, ts_ms: int, bucket_sec: Optional[int] = None) -> Dict[str, Any]:
    bsec = int(bucket_sec or DEFAULT_BUCKET_SEC)
    if bsec <= 0:
        bsec = int(DEFAULT_BUCKET_SEC)

    bts = _bucket_start(int(ts_ms), int(bsec))

    con = connect()
    try:
        row = con.execute(
            """
            SELECT doc_count, tone_mean, tone_std, conflict_share, econ_share
            FROM gdelt_macro_features
            WHERE bucket_sec = ?
              AND bucket_ts_ms <= ?
            ORDER BY bucket_ts_ms DESC
            LIMIT 1
            """,
            (int(bsec), int(bts)),
        ).fetchone()

        if not row:
            return {}

        # History is only used to contextualize the current bucket with z-scores;
        # the raw values remain the canonical macro features.
        # history for zscores
        hist = con.execute(
            """
            SELECT doc_count, tone_mean, conflict_share
            FROM gdelt_macro_features
            WHERE bucket_sec = ?
              AND bucket_ts_ms < ?
            ORDER BY bucket_ts_ms DESC
            LIMIT ?
            """,
            (int(bsec), int(bts), int(ZWIN)),
        ).fetchall()

        doc_hist = [float(r[0] or 0.0) for r in hist or []]
        tone_hist = [float(r[1] or 0.0) for r in hist or []]
        conf_hist = [float(r[2] or 0.0) for r in hist or []]

        doc = float(row[0] or 0.0)
        tone = float(row[1] or 0.0)
        tone_std = float(row[2] or 0.0)
        conf_share = float(row[3] or 0.0)
        econ_share = float(row[4] or 0.0)

        return {
            "doc_count": int(doc),
            "tone_mean": float(tone),
            "tone_std": float(tone_std),
            "conflict_share": float(conf_share),
            "econ_share": float(econ_share),
            "z_doc_count": float(_z(doc_hist, doc)),
            "z_tone_mean": float(_z(tone_hist, tone)),
            "z_conflict_share": float(_z(conf_hist, conf_share)),
        }
    except Exception as e:
        _warn_nonfatal("GDELT_MACRO_SNAPSHOT_FAILED", e)
        return {}
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("GDELT_MACRO_CLOSE_FAILED", e, ts_ms=int(ts_ms), bucket_sec=int(bsec))
