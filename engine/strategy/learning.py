"""
FILE: learning.py

Small learning/statistics helpers used by the strategy stack. Today this module
mainly exposes global priors and empirical relevance estimates from labels.
"""

import math
import logging
from typing import Tuple, Dict

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect
_WARNED_NONFATAL_KEYS: set[str] = set()
LOG = get_logger("strategy.learning")


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="strategy_learning_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.learning",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


# ------------            -- ------------------------------------------------------
# Confidence helpers (UNCHANGED)
# ------------            -- ------------------------------------------------------

def confidence_from_n(n: int) -> float:
    n = max(0, int(n))
    return float(1.0 - math.exp(-n / 25.0))


def confidence_from_weight(w: float) -> float:
    w = max(0.0, float(w))
    return float(1.0 - math.exp(-w / 3.0))


# ------------            -- ------------------------------------------------------
# Global priors (UNCHANGED)
# ------------            -- ------------------------------------------------------

def _ensure_model_stats():
    con = connect()
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS model_stats (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts_ms INTEGER NOT NULL,
              symbol TEXT NOT NULL,
              horizon_s INTEGER NOT NULL,
              n INTEGER NOT NULL,
              mean_impact_z REAL NOT NULL,
              UNIQUE(symbol, horizon_s)
            )
            """
        )
        con.commit()
    finally:
        con.close()


def get_global_prior(symbol: str, horizon_s: int) -> Tuple[float, int]:
    """
    Returns (mean_z, n). If missing, returns (0.0, 0).
    """
    _ensure_model_stats()
    con = connect()
    try:
        try:
            row = con.execute(
                """
                SELECT mean_impact_z, n
                FROM model_stats
                WHERE symbol=? AND horizon_s=?
                """,
                (str(symbol), int(horizon_s)),
            ).fetchone()
        except Exception as e:
            _warn_nonfatal(
                "LEARNING_GLOBAL_PRIOR_LOOKUP_FAILED",
                e,
                once_key=f"global_prior:{symbol}:{horizon_s}",
                symbol=str(symbol),
                horizon_s=repr(horizon_s)[:120],
            )
            return 0.0, 0

        if not row:
            return 0.0, 0

        return float(row[0]), int(row[1])
    finally:
        con.close()


# ------------            -- ------------------------------------------------------
# OPTION 5 — Learned relevance from real outcomes (NEW, SAFE)
# ------------            -- ------------------------------------------------------

def learn_relevance_stats(
    abs_z_threshold: float = 0.5,
) -> Dict[str, Dict[int, Dict[str, float]]]:
    """
    Learns empirical event→asset relevance from labels table.

    Definition (simple + robust):
      relevance = P(|impact_z| >= abs_z_threshold)

    Returns:
      {
        symbol: {
          horizon_s: {
            "relevance": float in [0,1],
            "n": int
          }
        }
      }

    Notes:
    - Uses ONLY existing tables (labels)
    - No schema changes
    - No side effects (read-only)
    - Stable, explainable, auditable
    """

    con = connect()
    try:
        # Fail-soft if labels table isn't created yet
        try:
            con.execute("SELECT 1 FROM labels LIMIT 1").fetchone()
        except Exception as e:
            _warn_nonfatal("LEARNING_LABELS_TABLE_LOOKUP_FAILED", e, once_key="labels_table_lookup")
            return {}

        rows = con.execute(
            """
            SELECT symbol, horizon_s, impact_z
            FROM labels
            WHERE impact_z IS NOT NULL
            """
        ).fetchall()
    finally:
        con.close()

    if not rows:
        return {}

    from collections import defaultdict

    total = defaultdict(int)
    hits = defaultdict(int)

    for sym, h, z in rows:
        try:
            z = float(z)
        except Exception as e:
            _warn_nonfatal(
                "LEARNING_RELEVANCE_ROW_PARSE_FAILED",
                e,
                once_key="relevance_row_parse",
                symbol=str(sym),
                horizon_s=repr(h)[:120],
            )
            continue

        key = (str(sym), int(h))
        total[key] += 1
        if abs(z) >= float(abs_z_threshold):
            hits[key] += 1

    out: Dict[str, Dict[int, Dict[str, float]]] = {}

    for (sym, h), n in total.items():
        if n <= 0:
            continue
        rel = float(hits[(sym, h)] / n)
        out.setdefault(sym, {})[h] = {
            "relevance": float(rel),
            "n": int(n),
        }

    return out


def get_model_stats():
    _ensure_model_stats()
    con = connect()
    try:
        rows = con.execute(
            """
            SELECT symbol, horizon_s, n, mean_impact_z, ts_ms
            FROM model_stats
            ORDER BY symbol ASC, horizon_s ASC
            """
        ).fetchall()
        return rows or []
    finally:
        con.close()
