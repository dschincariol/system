"""
FILE: compute_options_skew.py

Computes options-surface features for execution and sizing context. These
factors are conditioning inputs, not directional alpha signals.
"""

import os
import logging

from engine.runtime.storage import connect, init_db
from engine.runtime.factor_universe import put_factor_feature

LOG = logging.getLogger("compute_options_skew")
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))

_ZWIN = 240
_D5_LAG = 5


def _zscore(xs, win):
    xs = [float(x) for x in (xs or []) if x is not None]
    if len(xs) < max(30, win):
        return 0.0
    w = xs[-win:]
    mu = sum(w) / float(len(w))
    var = sum((x - mu) ** 2 for x in w) / float(len(w))
    if var <= 1e-18:
        return 0.0
    return float((xs[-1] - mu) / (var ** 0.5))


def _dlag(xs, lag):
    xs = [float(x) for x in (xs or []) if x is not None]
    if len(xs) <= int(lag):
        return 0.0
    return float(xs[-1] - xs[-1 - int(lag)])


def _load_series(con, col):
    # Keep this generic so one job can publish several derived surface features
    # from the same source table.
    rows = con.execute(
        f"""
        SELECT {col}
        FROM options_surface_agg
        WHERE {col} IS NOT NULL
        ORDER BY ts_ms ASC
        """
    ).fetchall()
    return [float(r[0]) for r in (rows or []) if r and r[0] is not None]


def main():
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("options_poll must be launched by supervisor")
        raise SystemExit(1)

    init_db()
    con = connect()

    rows = con.execute("""
        SELECT ts_ms
        FROM options_surface_agg
        ORDER BY ts_ms DESC
        LIMIT 1
    """).fetchall()

    if not rows:
        return

    now = int(rows[0][0])
    skew = _load_series(con, "skew_25d")
    slope = _load_series(con, "term_structure_slope")
    vov = _load_series(con, "vol_of_vol_1d")

    put_factor_feature(
        con,
        feature_id="options.skew_25d_z",
        asof_ts=now,
        effective_ts=now,
        value=_zscore(skew, _ZWIN),
        meta={"source": "options_surface_agg"}
    )

    put_factor_feature(
        con,
        feature_id="options.skew_25d_d5",
        asof_ts=now,
        effective_ts=now,
        value=_dlag(skew, _D5_LAG),
        meta={"source": "options_surface_agg"}
    )

    put_factor_feature(
        con,
        feature_id="options.surface_skew_z",
        asof_ts=now,
        effective_ts=now,
        value=_zscore(skew, _ZWIN),
        meta={"source": "options_surface_agg"}
    )

    put_factor_feature(
        con,
        feature_id="options.term_structure_slope_z",
        asof_ts=now,
        effective_ts=now,
        value=_zscore(slope, _ZWIN),
        meta={"source": "options_surface_agg"}
    )

    put_factor_feature(
        con,
        feature_id="options.vol_of_vol_z",
        asof_ts=now,
        effective_ts=now,
        value=_zscore(vov, _ZWIN),
        meta={"source": "options_surface_agg"}
    )

    con.commit()


if __name__ == "__main__":
    main()
