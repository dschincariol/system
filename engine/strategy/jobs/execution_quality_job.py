# FILE: execution_quality_job.py
# NEW FILE (CREATE)

"""
Computes execution quality metrics:
  - avg slippage
  - latency
  - partial fill rate
  - commission total
"""

import json

from engine.runtime.storage import connect, init_db

def main():
    con = connect()
    try:
        init_db()

        rows = con.execute(
            "SELECT submit_ts_ms, fill_ts_ms, expected_px, fill_px, commission FROM execution_fills"
        ).fetchall() or []

        slippages = []
        latencies = []
        comm_total = 0.0

        for s, f, e, px, c in rows:
            if s and f:
                latencies.append((f - s) / 1000.0)
            if e and px:
                slippages.append(abs(px - e) / e)
            comm_total += float(c or 0.0)

        result = {
            "avg_slippage": sum(slippages)/len(slippages) if slippages else 0.0,
            "avg_latency_s": sum(latencies)/len(latencies) if latencies else 0.0,
            "commission_total": comm_total
        }

        print(json.dumps({"ok": True, "metrics": result}))
        return 0

    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e)}))
        return 2
    finally:
        con.close()

if __name__ == "__main__":
    raise SystemExit(main())
