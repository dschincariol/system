"""
FILE: check_alerts.py

Runtime job entrypoint for `check_alerts`.
"""

# check_alerts.py
from engine.runtime.storage import connect, init_db


def main() -> int:
    init_db()
    con = connect(readonly=True)
    try:
        # `check_*` scripts are lightweight manual diagnostics. They are meant
        # for humans at a terminal, not for structured runtime integration.
        total = con.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
        print("alerts_total =", total)

        rows = con.execute(
                """
                SELECT severity, rule_id, symbol, horizon_s, expected_z, confidence, event_title, explain_json
                FROM alerts
                ORDER BY ts_ms DESC
                LIMIT 25
                """
            ).fetchall()

        for sev, rule_id, sym, h, z, conf, title, explain_json in rows:
                explain = ""
                if explain_json:
                    explain = " | explain_json=1"
                print(f"{sev} {rule_id} {sym} h={h} z={z:+.3f} conf={conf:.2f} :: {title}{explain}")
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
