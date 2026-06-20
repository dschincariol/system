"""
FILE: check_labels.py

Runtime job entrypoint for `check_labels`.
"""

# check_labels.py
from engine.runtime.storage import connect, init_db


def main() -> int:
    init_db()
    con = connect(readonly=True)
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
