"""
FILE: training_window.py

Loads labeled training samples from a configurable historical window and adds
time-decay weights. This is the utility layer used by training jobs that want
recent data to count more heavily than older observations.
"""

import math
import time
import logging
from typing import List, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect

DAY_MS = 86400 * 1000
LOG = get_logger("strategy.training_window")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="strategy_training_window_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.training_window",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _now_ms() -> int:
    return int(time.time() * 1000)


def load_training_labels(
    *,
    min_days: int,
    max_days: int,
    halflife_days: int,
) -> List[Tuple]:
    """
    Returns rows from labels table with exponential time decay weights.
    Adds 'sample_weight' column at the end.
    """
    now = _now_ms()
    min_ts = now - (max_days * DAY_MS)
    max_ts = now - (min_days * DAY_MS)

    con = connect()
    try:
        # Early bootstrap may not have produced labels yet, so return an empty
        # dataset rather than raising into the caller.
        try:
            con.execute("SELECT 1 FROM labels LIMIT 1").fetchone()
        except Exception as e:
            _warn_nonfatal("TRAINING_WINDOW_LABELS_PROBE_FAILED", e, once_key="labels_probe")
            return []

        rows = con.execute(
                """
                SELECT
                  l.event_id,
                  l.symbol,
                  l.horizon_s,
                  CASE
                WHEN le.realized=1 THEN le.net_z
                ELSE COALESCE(le.net_z, l.realized_z)
                END AS target_z,
                  l.ts_ms
                FROM labels l
                LEFT JOIN labels_exec le
                  ON le.event_id=l.event_id AND le.symbol=l.symbol AND le.horizon_s=l.horizon_s
                WHERE l.ts_ms BETWEEN ? AND ?
                AND COALESCE(le.net_z, l.realized_z) IS NOT NULL
                """,
                (int(min_ts), int(max_ts)),
            ).fetchall()

    finally:
        con.close()

    out = []
    for eid, sym, h, z, ts in rows:
        age_days = max(0.0, (now - int(ts)) / DAY_MS)
        # Exponential decay keeps the weighting smooth while still making the
        # half-life parameter intuitive for operators.
        w = math.exp(-math.log(2.0) * age_days / max(1.0, float(halflife_days)))
        out.append((eid, sym, h, z, ts, float(w)))

    return out
