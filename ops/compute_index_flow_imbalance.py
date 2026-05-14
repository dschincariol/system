"""
FILE: compute_index_flow_imbalance.py

Operational helper script for `compute_index_flow_imbalance`.
"""

# compute_index_flow_imbalance.py
"""
Index constituent flow imbalance feature.
Improves execution sizing during institutional rotation.
"""

import os
import numpy as np
import logging

from engine.runtime.storage import connect, init_db
from engine.runtime.factor_universe import put_factor_feature

LOG = logging.getLogger("compute_index_flow_imbalance")
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))

_ZWIN = 240


def _zscore(xs, win):
    xs = np.asarray(xs, dtype=float)
    if xs.size < max(30, win):
        return 0.0
    w = xs[-win:]
    mu = float(np.mean(w))
    sd = float(np.std(w))
    if sd <= 1e-9:
        return 0.0
    return float((xs[-1] - mu) / sd)


def main():
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("options_poll must be launched by supervisor")
        raise SystemExit(1)

    init_db()
    con = connect()

    rows = con.execute("""
        SELECT ts_ms, buy_notional, sell_notional
        FROM index_constituent_flows
        ORDER BY ts_ms ASC
    """).fetchall()

    if not rows:
        return

    ts = np.array([int(r[0]) for r in rows])
    imbalance = np.array([
        float(r[1]) - float(r[2])
        for r in rows
    ])

    z = _zscore(imbalance, _ZWIN)

    d5 = 0.0
    if imbalance.size > 5:
        d5 = float(imbalance[-1] - imbalance[-6])

    now = int(ts[-1])

    put_factor_feature(
        con,
        feature_id="flows.index_constituent_imbalance_z",
        asof_ts=now,
        effective_ts=now,
        value=z,
        meta={}
    )

    put_factor_feature(
        con,
        feature_id="flows.index_constituent_imbalance_d5",
        asof_ts=now,
        effective_ts=now,
        value=d5,
        meta={}
    )

    con.commit()


if __name__ == "__main__":
    main()
