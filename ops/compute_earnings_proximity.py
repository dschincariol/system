"""
FILE: compute_earnings_proximity.py

Operational helper script for `compute_earnings_proximity`.
"""

# compute_earnings_proximity.py
"""
Earnings proximity decay modifier.
Scales execution risk near earnings.
"""

import time
import math
import os
import logging

from engine.runtime.storage import connect, init_db
from engine.runtime.factor_universe import put_factor_feature

LOG = logging.getLogger("compute_earnings_proximity")
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))

HALF_LIFE_DAYS = 5.0


def _decay(days):
    # The feature is symmetric around the earnings date: risk rises as you get
    # closer on either side, then decays with distance.
    return math.exp(-abs(days) / HALF_LIFE_DAYS)


def main():
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("options_poll must be launched by supervisor")
        raise SystemExit(1)

    init_db()
    con = connect()

    # This writes a generic factor-universe feature. Strategy/execution code can
    # consume it without depending directly on the earnings calendar schema.
    rows = con.execute("""
        SELECT symbol, earnings_ts_ms
        FROM earnings_calendar
    """).fetchall()

    now = int(time.time() * 1000)

    for sym, earn_ts in rows:
        if earn_ts is None:
            continue

        days = (int(earn_ts) - now) / 86400000.0
        val = _decay(days)

        put_factor_feature(
            con,
            feature_id="earnings.proximity_decay",
            asof_ts=now,
            effective_ts=now,
            value=val,
            meta={"symbol": sym}
        )

    con.commit()


if __name__ == "__main__":
    main()
