"""
FILE: kill_health_monitor.py

Auto-kill monitor for data-health failures.
"""

import time
import json
import logging
from typing import Any, Dict

from engine.execution.kill_switch import activate
from engine.runtime.alerts_notify import send_runtime_health_notification
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.health import get_health_snapshot
from engine.runtime.storage import connect, init_db

LOG = logging.getLogger("engine.strategy.kill_health_monitor")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(event: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOG,
        event=event,
        code=event,
        message=event,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.kill_health_monitor",
        extra=extra,
        persist=False,
    )


def main() -> int:
    init_db()
    con = connect()
    try:
        health = get_health_snapshot()
        notifications: Dict[str, Any] | None = None
        try:
            notifications = send_runtime_health_notification(
                health,
                actor="system",
                source="kill_health_monitor",
            )
        except Exception as e:
            _warn_nonfatal("kill_health_monitor_notify_failed", e)

        if not health.get("ok", False):
            try:
                activate(
                    "global",
                    "global",
                    reason="auto_data_health_failure",
                    actor="system",
                    meta={"health": health},
                    action="AUTO",
                    con=con,
                )
            except Exception as e:
                _warn_nonfatal("kill_health_monitor_activate_failed", e)

        out: Dict[str, Any] = {
            "ok": True,
            "health_ok": bool(health.get("ok", False)),
            "health": health,
            "notifications": notifications,
            "ts_ms": _now_ms(),
        }
        print(json.dumps(out, indent=2))
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
