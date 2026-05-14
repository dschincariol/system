"""
FILE: labeling.py

Creates realized outcome labels from event timestamps and price series. This is
the basic supervised-learning bridge between raw events and downstream model
training.
"""

import time
from typing import Dict, List
from engine.data.prices.returns import compute_return
from engine.data.prices.volatility import compute_volatility
from engine.runtime.storage import connect
from engine.strategy.model_v2 import classify_regime

HORIZONS_S = {
    "5m": 300,
    "1h": 3600,
}

def label_event(
    event_id: int,
    event_ts: int,
    price_series: Dict[str, List[dict]],
):
    con = connect()
    try:
        cur = con.cursor()
        now_ms = int(time.time() * 1000)

        for sym, series in price_series.items():
            vol = float(compute_volatility(series) or 1e-6)
            regime = classify_regime(vol)

            for _, h_s in HORIZONS_S.items():
                ret = compute_return(series, event_ts, h_s * 1000)
                if ret is None:
                    continue

                # Labels are normalized into z-space so models can compare
                # outcomes across symbols with different volatility levels.
                impact_z = float(ret) / float(vol)

                cur.execute(
                    """
                    INSERT OR IGNORE INTO labels(
                      event_id, horizon_s, symbol,
                      baseline_ret, realized_ret,
                      impact_z, created_at_ms,
                      vol_proxy, regime
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(event_id),
                        int(h_s),
                        str(sym),
                        0.0,
                        float(ret),
                        float(impact_z),
                        int(now_ms),
                        float(vol),
                        str(regime),
                    ),
                )

        con.commit()
    finally:
        con.close()
