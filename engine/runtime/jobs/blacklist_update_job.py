"""
FILE: blacklist_update_job.py

Job entrypoint or scheduled task for `blacklist_update_job`.
"""

"""
blacklist_update_job.py

Auto-blacklist symbols that degrade PnL using pnl_attribution from execution_ledger.

Run every 1h (or daily) in production.

Env:
  BLACKLIST_MIN_TRADES=3
  BLACKLIST_PNL_THRESH=-50.0            (USD)
  BLACKLIST_SLIPPAGE_BPS_THRESH=25.0
  BLACKLIST_TTL_S=86400                 (24h)
  BLACKLIST_LOOKBACK_HOURS=72
"""

import os
import time
import json
import logging

from engine.runtime.storage import connect
from engine.strategy.symbol_blacklist import upsert_blacklist, init_blacklist

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [blacklist_update_job] %(message)s",
)

MIN_TRADES = int(os.environ.get("BLACKLIST_MIN_TRADES", "3"))
PNL_THRESH = float(os.environ.get("BLACKLIST_PNL_THRESH", "-50.0"))
SLIP_BPS_THRESH = float(os.environ.get("BLACKLIST_SLIPPAGE_BPS_THRESH", "25.0"))
TTL_S = int(os.environ.get("BLACKLIST_TTL_S", "86400"))
LOOKBACK_HOURS = int(os.environ.get("BLACKLIST_LOOKBACK_HOURS", "72"))


def main() -> int:
    init_blacklist()
    con = connect()
    try:
        now_ms = int(time.time() * 1000)
        cutoff = now_ms - int(LOOKBACK_HOURS) * 3600 * 1000

        # pnl_attribution rows are snapshots; aggregate last N hours
        rows = con.execute(
            """
            SELECT symbol,
                   COUNT(1) as n,
                   SUM(
                     COALESCE(realized_pnl, 0.0)
                     + COALESCE(unrealized_pnl, 0.0)
                     - COALESCE(fees, 0.0)
                     - COALESCE(json_extract(extra_json, '$.slippage_cost'), 0.0)
                   ) as pnl_sum,
                   AVG(COALESCE(slippage_bps,0)) as slip_avg
            FROM pnl_attribution
            WHERE ts_ms >= ?
            GROUP BY symbol
            """,
            (int(cutoff),),
        ).fetchall()

        n_blacklisted = 0
        for sym, n, pnl_sum, slip_avg in rows or []:
            sym = str(sym).upper().strip()
            n = int(n or 0)
            pnl_sum = float(pnl_sum or 0.0)
            slip_avg = float(slip_avg or 0.0)

            if n < int(MIN_TRADES):
                continue

            # criteria: negative pnl and/or excessive slippage
            bad_pnl = pnl_sum <= float(PNL_THRESH)
            bad_slip = abs(slip_avg) >= float(SLIP_BPS_THRESH)

            if bad_pnl or bad_slip:
                score = 0.0
                if bad_pnl:
                    score += min(10.0, abs(pnl_sum) / 50.0)
                if bad_slip:
                    score += min(10.0, abs(slip_avg) / 10.0)

                upsert_blacklist(
                    con,
                    symbol=sym,
                    reason="auto_pnl_slippage",
                    score=float(score),
                    ttl_s=int(TTL_S),
                    meta={
                        "lookback_hours": int(LOOKBACK_HOURS),
                        "n": int(n),
                        "pnl_sum": float(pnl_sum),
                        "slip_avg_bps": float(slip_avg),
                        "pnl_thresh": float(PNL_THRESH),
                        "slip_thresh_bps": float(SLIP_BPS_THRESH),
                    },
                )
                n_blacklisted += 1

        con.commit()
        out = {"ok": True, "blacklisted": int(n_blacklisted), "ts_ms": int(now_ms)}
        print(json.dumps(out, indent=2, sort_keys=True))
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
