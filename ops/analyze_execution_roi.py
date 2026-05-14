"""
FILE: analyze_execution_roi.py

Operational helper script for `analyze_execution_roi`.
"""

# analyze_execution_roi.py
"""
Validate that execution conditioning improves realized PnL per risk unit.
"""

import numpy as np
from engine.runtime.storage import connect, init_db


def main() -> int:
    init_db()
    con = connect()
    try:
        rows = con.execute("""
            SELECT pnl_bps, slippage_bps, json_extract(meta,'$.exec_stress.stress_size_mult')
            FROM execution_labels
            WHERE pnl_bps IS NOT NULL
        """).fetchall()
    finally:
        con.close()

    if not rows:
        print("No execution_labels found.")
        return 0

    # This is an offline diagnostic, not a governance gate. It reads labels and
    # prints simple summary statistics for human inspection.
    pnl = np.array([r[0] for r in rows], dtype=float)
    slip = np.array([r[1] for r in rows], dtype=float)
    size_mult = np.array([r[2] if r[2] is not None else 1.0 for r in rows], dtype=float)

    print("Mean pnl_bps:", pnl.mean())
    print("Mean slippage_bps:", slip.mean())
    print("Corr(size_mult, pnl):", np.corrcoef(size_mult, pnl)[0,1])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
