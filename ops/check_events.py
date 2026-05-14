"""
FILE: check_events.py

Operational helper script for `check_events`.
"""

# check_events.py
import os
import sqlite3
from pathlib import Path


def main() -> int:
    db_path = os.environ.get("DB_PATH", str(Path("./data/trading.db").resolve()))
    con = sqlite3.connect(db_path)
    try:
        # This check intentionally bypasses runtime helpers so it can inspect a
        # DB file directly, even when the rest of the app environment is broken.
        total = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        print("events_total =", total)

        rows = con.execute(
                """
                SELECT ts_ms, source, title, url
                FROM events
                ORDER BY ts_ms DESC
                LIMIT 15
                """
            ).fetchall()

        for ts_ms, source, title, url in rows:
                print(source, "::", title)
                if url:
                    print("  ", url)
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
