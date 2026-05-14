"""
FILE: factor_universe.py

Runtime subsystem module for `factor_universe`.
"""

import logging
import json
import math
import time
from typing import Dict, List, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect

LOG = get_logger("engine.runtime.factor_universe")

# ----------------------------------------------------------------------
# Canonical Tier-1 feature order (FIXED DIMENSION)
# Train and predict MUST stay consistent.
# ----------------------------------------------------------------------
FACTOR_FEATURE_ORDER: List[str] = [
    # Direct macro release-aware factors
    "macro.cpi_yoy",
    "macro.cpi_yoy_z",
    "macro.cpi_yoy_d1",
    "macro.policy_rate_upper",
    "macro.policy_rate_upper_z",
    "macro.policy_rate_upper_d5",
    "macro.unemployment_rate",
    "macro.unemployment_rate_z",
    "macro.unemployment_rate_d1",
    "macro.gdp_real_qoq_ann",
    "macro.gdp_real_qoq_ann_z",
    "macro.gdp_real_qoq_ann_d1",
    "macro.oil_wti_spot",
    "macro.oil_wti_spot_z",
    "macro.oil_wti_spot_d5",
    "macro.natgas_spot",
    "macro.natgas_spot_z",
    "macro.natgas_spot_d5",

    # Rates / liquidity (market-proxy macro)
    "macro.us_10y_yield_z",
    "macro.us_10y_yield_d5",
    "macro.us_5y_yield_z",
    "macro.us_5y_yield_d5",
    "macro.us_curve_10y_5y_z",

    # Vol / options structure (proxied)
    "vol.vix_z",
    "vol.vix_d5",
    "vol.rv20_z",

    # Credit stress (proxied)
    "credit.hyg_lqd_spread_z",
    "credit.hyg_lqd_spread_d5",

    # Flows / positioning (proxied)
    "flows.spy_agg_ratio_z",
    "flows.spy_agg_ratio_d5",

    # NEW: Direct execution alpha factors
    "options.skew_25d_z",
    "options.skew_25d_d5",
    "options.surface_skew_z",
    "options.term_structure_slope_z",
    "options.vol_of_vol_z",
    "flows.index_constituent_imbalance_z",
    "flows.index_constituent_imbalance_d5",
    "earnings.proximity_decay",
]

FACTOR_FEATURE_DIM = len(FACTOR_FEATURE_ORDER)


def _safe_float(x) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else 0.0
    except Exception as e:
        _warn_nonfatal("FACTOR_UNIVERSE_SAFE_FLOAT_FAILED", e, value=repr(x))
        return 0.0


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="factor_universe_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.runtime.factor_universe",
        extra=extra or None,
        persist=False,
    )


def put_factor_feature(
    con,
    *,
    feature_id: str,
    asof_ts: int,
    effective_ts: int,
    value: float,
    meta: Optional[Dict] = None,
) -> None:
    owns = False
    if con is None:
        con = connect()
        owns = True
    try:
        # Feature rows are keyed by feature_id plus as-of/effective timestamps,
        # which preserves point-in-time semantics for both training and replay.
        con.execute(
            """
            INSERT OR REPLACE INTO factor_features
              (feature_id, asof_ts, effective_ts, value, meta_json)
            VALUES (?,?,?,?,?)
            """,
            (
                str(feature_id),
                int(asof_ts),
                int(effective_ts),
                _safe_float(value),
                json.dumps(meta or {}, separators=(",", ":"), sort_keys=True),
            ),
        )
        if owns:
            con.commit()
    finally:
        if owns:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal("FACTOR_UNIVERSE_CLOSE_FAILED", e, operation="put_factor_feature", feature_id=str(feature_id))


def _get_feature_asof(con, feature_id: str, ts_ms: int) -> float:
    row = con.execute(
        """
        SELECT value
        FROM factor_features
        WHERE feature_id=?
          AND asof_ts <= ?
          AND effective_ts <= ?
        ORDER BY asof_ts DESC, effective_ts DESC
        LIMIT 1
        """,
        (str(feature_id), int(ts_ms), int(ts_ms)),
    ).fetchone()
    if not row:
        return 0.0
    return _safe_float(row[0])


def get_factor_universe_vector(con=None, ts_ms: Optional[int] = None) -> List[float]:
    """
    Read-only: returns FIXED-DIM vector in FACTOR_FEATURE_ORDER.

    Uses as-of join semantics:
    - Only values with asof_ts <= ts_ms are eligible.
    - effective_ts must also be <= ts_ms.
    """
    owns = False
    if con is None:
        con = connect()
        owns = True
    try:
        if ts_ms is None:
            ts_ms = int(time.time() * 1000)

        out: List[float] = []
        for fid in FACTOR_FEATURE_ORDER:
            out.append(_get_feature_asof(con, fid, int(ts_ms)))

        # Dimension stability is a hard contract with model training and
        # inference code; changing it silently would corrupt feature alignment.
        # Hard safety: never allow dimension drift
        if len(out) != FACTOR_FEATURE_DIM:
            raise RuntimeError(
                f"Factor universe dimension mismatch: got={len(out)} expected={FACTOR_FEATURE_DIM}"
            )

        return out
    finally:
        if owns:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal("FACTOR_UNIVERSE_CLOSE_FAILED", e, operation="get_factor_universe_vector")


def load_factor_universe_snapshot(con=None, ts_ms: Optional[int] = None) -> Dict[str, float]:
    """
    Convenience for explain_json / dashboard: dict feature_id -> value (as-of).
    """
    vec = get_factor_universe_vector(con=con, ts_ms=ts_ms)
    return {fid: _safe_float(v) for fid, v in zip(FACTOR_FEATURE_ORDER, vec)}
