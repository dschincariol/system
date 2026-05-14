"""
FILE: drift_utils.py

Small read helpers around the `model_drift` table. These are convenience
functions used by rules and risk gates that only need coarse drift summaries.
"""

import logging

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect

LOG = get_logger("strategy.drift_utils")


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="strategy_drift_utils_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.drift_utils",
        extra=dict(extra or {}) or None,
        persist=False,
    )


def get_max_drift_ratio(con=None) -> float:
    owns = False
    if con is None:
        con = connect()
        owns = True
    try:
        row = con.execute("SELECT MAX(drift_ratio) FROM model_drift").fetchone()
        return float(row[0]) if row and row[0] is not None else 0.0
    except Exception as e:
        _warn_nonfatal("DRIFT_UTILS_GET_MAX_DRIFT_RATIO_FAILED", e)
        return 0.0
    finally:
        if owns:
            con.close()


def get_symbol_max_drift_ratio(con, symbol: str) -> float:
    try:
        row = con.execute(
            "SELECT MAX(drift_ratio) FROM model_drift WHERE symbol=?",
            (str(symbol),),
        ).fetchone()
        return float(row[0]) if row and row[0] is not None else 0.0
    except Exception as e:
        _warn_nonfatal(
            "DRIFT_UTILS_GET_SYMBOL_MAX_DRIFT_RATIO_FAILED",
            e,
            symbol=str(symbol),
        )
        return 0.0
