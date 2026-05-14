"""
FILE: check_predictions.py

Operational helper script for `check_predictions`.
"""

# check_predictions.py
import os
import sqlite3
from pathlib import Path


def main() -> int:
    db_path = os.environ.get("DB_PATH", str(Path("./data/trading.db").resolve()))
    con = sqlite3.connect(db_path)
    try:
        # This script is intentionally read-only and presentation-oriented: it
        # answers "are predictions landing?" without any runtime side effects.
        total = con.execute("SELECT COUNT(*) FROM predictions").fetchone()[0]
        print("predictions_total =", total)

        rows = con.execute(
                """
                SELECT p.symbol, p.horizon_s, p.predicted_z, p.confidence, e.title
                FROM predictions p
                JOIN events e ON e.id = p.event_id
                ORDER BY p.ts_ms DESC
                LIMIT 20
                """
            ).fetchall()

        for sym, h, z, conf, title in rows:
                print(f"{sym} h={h} z={z:+.3f} conf={conf:.2f} :: {title}")
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
