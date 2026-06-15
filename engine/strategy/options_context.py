"""
Read-only as-of accessors for symbol-scoped options features.
"""

from __future__ import annotations

import os
import logging
from typing import Any, Dict, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import connect

DEFAULT_BUCKET_SEC = max(60, int(os.environ.get("OPTIONS_INTRADAY_BUCKET_SEC", "900")))
FALLBACK_BUCKET_SEC = 86400
LOG = logging.getLogger("options_context")


def _warn_nonfatal(event: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOG,
        event=event,
        code=event,
        message=event,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.options_context",
        extra=extra,
        persist=False,
    )


def _bucket_start(ts_ms: int, bucket_sec: int) -> int:
    size_ms = max(1, int(bucket_sec)) * 1000
    return int(ts_ms // size_ms) * size_ms


def _fetch_bucket(con, symbol: str, ts_ms: int, bucket_sec: int) -> Optional[Dict[str, Any]]:
    row = con.execute(
        """
        SELECT
          iv_rank,
          iv_rank_short,
          skew_25d,
          term_structure_slope,
          unusual_volume_score,
          call_put_volume_ratio,
          call_put_oi_ratio,
          signal_score,
          gex_norm,
          gex_norm_z,
          gex_sign,
          opt_flow_imbalance,
          opt_flow_imbalance_z
        FROM options_symbol_features
        WHERE symbol = ?
          AND bucket_sec = ?
          AND bucket_ts_ms <= ?
          AND snapshot_ts_ms <= ?
        ORDER BY bucket_ts_ms DESC, snapshot_ts_ms DESC
        LIMIT 1
        """,
        (str(symbol), int(bucket_sec), int(_bucket_start(ts_ms, bucket_sec)), int(ts_ms)),
    ).fetchone()
    if not row:
        return None
    return {
        "iv_rank": float(row[0] or 0.0),
        "iv_rank_short": float(row[1] or 0.0),
        "skew_25d": float(row[2] or 0.0),
        "term_structure_slope": float(row[3] or 0.0),
        "unusual_volume_score": float(row[4] or 0.0),
        "call_put_volume_ratio": float(row[5] or 1.0),
        "call_put_oi_ratio": float(row[6] or 1.0),
        "signal_score": float(row[7] or 0.0),
        "gex_norm": float(row[8] or 0.0),
        "gex_norm_z": float(row[9] or 0.0),
        "gex_sign": float(row[10] or 0.0),
        "opt_flow_imbalance": float(row[11] or 0.0),
        "opt_flow_imbalance_z": float(row[12] or 0.0),
    }


def get_options_feature_vector(*, symbol: str, ts_ms: int, bucket_sec: Optional[int] = None) -> Dict[str, Any]:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return {}

    requested_bucket = max(60, int(bucket_sec or DEFAULT_BUCKET_SEC))
    con = connect()
    try:
        row = _fetch_bucket(con, sym, int(ts_ms), int(requested_bucket))
        if row:
            return row
        if int(requested_bucket) != int(FALLBACK_BUCKET_SEC):
            row = _fetch_bucket(con, sym, int(ts_ms), int(FALLBACK_BUCKET_SEC))
            if row:
                return row
        return {}
    except Exception as e:
        _warn_nonfatal(
            "options_context_feature_lookup_failed",
            e,
            symbol=str(sym),
            ts_ms=int(ts_ms),
            bucket_sec=int(requested_bucket),
        )
        return {}
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("options_context_db_close_failed", e, symbol=str(sym))
