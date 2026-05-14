"""
FILE: drift.py

Computes simple model-drift metrics from realized label error and stores them
per symbol/horizon. The output is used for confidence/risk adjustments, not for
changing prediction direction.
"""

import time
import logging
import numpy as np

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect

RECENT_N = int(50)        # recent samples
BASELINE_N = int(200)     # long-term baseline
MIN_N = int(30)           # minimum to compute drift
LOG = get_logger("strategy.drift")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="strategy_drift_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.drift",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def compute_and_store_drift():
    now_ms = int(time.time() * 1000)

    con = connect()
    try:
        # Fail-soft if labels table isn't created yet
        try:
            con.execute("SELECT 1 FROM labels LIMIT 1").fetchone()
        except Exception as e:
            _warn_nonfatal("DRIFT_LABELS_TABLE_PROBE_FAILED", e, once_key="labels_table_probe")
            return {}

        rows = con.execute(
            """
            SELECT l.symbol, l.horizon_s, l.impact_z
            FROM labels l
            WHERE l.impact_z IS NOT NULL
            ORDER BY l.created_at_ms DESC
            """
        ).fetchall()

        if not rows:
            return {}

        buckets = {}
        for sym, h, z in rows:
            try:
                buckets.setdefault((str(sym), int(h)), []).append(abs(float(z)))
            except Exception as e:
                _warn_nonfatal(
                    "DRIFT_BUCKET_PARSE_FAILED",
                    e,
                    once_key="bucket_parse",
                    symbol=repr(sym)[:120],
                    horizon=repr(h)[:120],
                )
                continue

        out = {}

        for (sym, h), errs in buckets.items():
            if len(errs) < MIN_N:
                continue

            # Recent and baseline windows are taken from the same ordered error
            # series so the ratio stays easy to explain.
            recent = np.array(errs[:RECENT_N], dtype=float)
            base = np.array(errs[:BASELINE_N], dtype=float)

            if len(recent) < MIN_N or len(base) < MIN_N:
                continue

            mae_recent = float(np.mean(recent))
            mae_base = float(np.mean(base))

            if mae_base <= 0:
                continue

            drift_ratio = float(mae_recent / mae_base)

            con.execute(
                """
                INSERT INTO model_drift(symbol, horizon_s, ts_ms, n, mae, baseline_mae, drift_ratio)
                VALUES (?,?,?,?,?,?,?)
                ON CONFLICT(symbol, horizon_s) DO UPDATE SET
                  ts_ms=excluded.ts_ms,
                  n=excluded.n,
                  mae=excluded.mae,
                  baseline_mae=excluded.baseline_mae,
                  drift_ratio=excluded.drift_ratio
                """,
                (
                    sym,
                    int(h),
                    now_ms,
                    int(len(recent)),
                    mae_recent,
                    mae_base,
                    drift_ratio,
                ),
            )

            out[(sym, h)] = {
                "mae": mae_recent,
                "baseline_mae": mae_base,
                "drift_ratio": drift_ratio,
                "n": len(recent),
            }

        con.commit()
        return out

    finally:
        con.close()
