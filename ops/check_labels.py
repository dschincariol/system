"""
FILE: check_labels.py

Operational helper script for `check_labels`.
"""

# check_labels.py
import os
import sqlite3
from pathlib import Path


def main() -> int:
    db_path = os.environ.get("DB_PATH", str(Path("./data/trading.db").resolve()))
    con = sqlite3.connect(db_path)
    try:
        # Simple cardinality sanity check for manual debugging, not a monitored
        # health signal with thresholds or automatic remediation.
        total = con.execute("SELECT COUNT(*) FROM labels").fetchone()[0]
        print("labels_total =", total)

        rows = con.execute(
                "SELECT symbol, horizon_s, COUNT(*) FROM labels GROUP BY symbol, horizon_s ORDER BY symbol, horizon_s"
            ).fetchall()

        for sym, h, c in rows:
                print(f"{sym} horizon_s={h} count={c}")
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
