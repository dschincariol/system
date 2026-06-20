"""
FILE: check_events.py

Runtime job entrypoint for `check_events`.
"""

# check_events.py
from engine.runtime.storage import connect, init_db


def main() -> int:
    init_db()
    con = connect(readonly=True)
    try:
        # Manual read-only terminal diagnostic for checking whether events are
        # landing in the active runtime storage backend.
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
