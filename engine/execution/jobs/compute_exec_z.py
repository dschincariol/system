"""
FILE: compute_exec_z.py

Execution job entrypoint for `compute_exec_z`.
"""

# compute_exec_z.py
"""
Compute net_z and gross_z for labels_exec by symbol+horizon using rolling stats.
"""

import math
import os
from collections import defaultdict

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect, init_db

LOG = get_logger("engine.execution.jobs.compute_exec_z")


def main():
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("compute_exec_z must be launched by supervisor")
        raise SystemExit(1)

    init_db()
    con = connect()
    try:

        rows = con.execute(
            """
            SELECT event_id, symbol, horizon_s, net_ret, gross_ret
            FROM labels_exec
            WHERE net_ret IS NOT NULL AND gross_ret IS NOT NULL
            """
        ).fetchall()

        # Z-scoring is done within each (symbol, horizon) bucket so execution
        # outcomes are normalized against comparable trades only.
        by_key = defaultdict(list)
        for eid, sym, h, net_r, gross_r in rows:
            try:
                by_key[(str(sym), int(h))].append((int(eid), float(net_r), float(gross_r)))
            except Exception as e:
                log_failure(
                    LOG,
                    event="compute_exec_z_row_parse_failed",
                    code="COMPUTE_EXEC_Z_ROW_PARSE_FAILED",
                    message=str(e),
                    error=e,
                    level=30,
                    component="engine.execution.jobs.compute_exec_z",
                    extra={"event_id": eid, "symbol": sym, "horizon_s": h},
                    persist=False,
                )
                continue

        updates = []
        for (sym, h), items in by_key.items():
            # This is a simple rolling cross-section normalization helper, not a
            # full calibration model. Gross and net are kept separate on purpose.
            # robust std on net_ret
            net_vals = [x[1] for x in items]
            gross_vals = [x[2] for x in items]
            if len(net_vals) < 30:
                continue

            def std(v):
                m = sum(v) / len(v)
                var = sum((x - m) ** 2 for x in v) / max(1, (len(v) - 1))
                return math.sqrt(var)

            net_std = std(net_vals)
            gross_std = std(gross_vals)
            if net_std <= 1e-12:
                continue

            for eid, net_r, gross_r in items:
                net_z = net_r / net_std
                gross_z = (gross_r / gross_std) if gross_std > 1e-12 else None
                updates.append((gross_z, net_z, int(eid), sym, int(h)))

        for gross_z, net_z, eid, sym, h in updates:
            pass

        con.execute(
                """
                UPDATE labels_exec
                SET gross_z=?, net_z=?
                WHERE event_id=? AND symbol=? AND horizon_s=?
                """,
                (gross_z, float(net_z), int(eid), str(sym), int(h)),
            )

        con.commit()
        print(f"[labels_exec] updated z for {len(updates)} rows")
    finally:
        con.close()


if __name__ == "__main__":
    main()
