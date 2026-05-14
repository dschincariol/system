"""
FILE: sleep_guard.py

Operational helper script for `sleep_guard`.
"""

# ops/sleep_guard.py
import os
import time
from typing import List

from engine.runtime.storage import connect, init_db
from engine.execution.kill_switch import snapshot as kill_snapshot, activate
from engine.runtime.health import get_health_snapshot
from ops.email_notifier import send_email


def _now_ms() -> int:
    return int(time.time() * 1000)


def check_health(con) -> List[str]:
    alerts = []
    health = get_health_snapshot()
    if not health.get("ok", False):
        alerts.append("DATA_HEALTH_FAILED")
        # `sleep_guard` is allowed to mutate kill-switch state because it is an
        # operational safety script, not just a passive reporting check.
        activate(
            "global",
            "global",
            reason="sleep_guard_data_health",
            actor="system",
            meta={"health": health},
            action="AUTO",
            con=con,
        )
    return alerts


def check_kill_switches() -> List[str]:
    ks = kill_snapshot()
    active = [k for k, v in ks.items() if v.get("enabled")]
    if active:
        return [f"KILL_SWITCH_ACTIVE: {active}"]
    return []


def check_stuck_watches(con) -> List[str]:
    alerts = []
    now = _now_ms()
    rows = con.execute(
        """
        SELECT id, model_name, regime, watch_until_ts_ms
        FROM model_post_promo_watch
        WHERE status='active'
        """
    ).fetchall()

    for wid, model, regime, until_ms in rows:
        if until_ms and now > (int(until_ms) + 5 * 60 * 1000):
            alerts.append(f"STUCK_WATCH model={model} regime={regime} watch_id={wid}")
    return alerts


def check_predictions_stalled(con) -> List[str]:
    row = con.execute("SELECT MAX(ts_ms) FROM predictions").fetchone()
    last = int(row[0] or 0)
    age_s = (time.time() * 1000 - last) / 1000.0 if last else 1e9
    if age_s > 300:
        activate(
            "global",
            "global",
            reason="sleep_guard_predictions_stalled",
            actor="system",
            meta={"age_s": age_s},
            action="AUTO",
            con=con,
        )
        return [f"PREDICTIONS_STALLED age_s={round(age_s,1)}"]
    return []


def main():
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("sleep_guard must be launched by supervisor")
        raise SystemExit(1)

    init_db()
    con = connect()
    try:
        alerts: List[str] = []
        alerts += check_health(con)
        alerts += check_kill_switches()
        alerts += check_stuck_watches(con)
        alerts += check_predictions_stalled(con)

        if alerts:
            # Email is only the notification side effect. The important control
            # action, when needed, has already happened via kill-switch activation.
            body = "\n".join(alerts)
            send_email(
                subject="[ALERT] Trading system requires attention",
                body=body,
            )
            print("[sleep_guard] alerts sent")
        else:
            print("[sleep_guard] all clear")

    finally:
        con.close()


if __name__ == "__main__":
    main()
