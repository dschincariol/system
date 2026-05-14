"""
FILE: execution_broker_watchdog.py

Execution subsystem module for `execution_broker_watchdog`.
"""

import json
import os
import time
from typing import Any, Dict

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect

LOG = get_logger("engine.execution.execution_broker_watchdog")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.execution.execution_broker_watchdog",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)

def _now_ms() -> int:
    return int(time.time() * 1000)


def _default_broker() -> str:
    return str(os.environ.get("BROKER_NAME", os.environ.get("BROKER", "sim")) or "sim").lower().strip()


def _ensure_tables(con) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS broker_connection_health (
          ts_ms INTEGER NOT NULL,
          broker TEXT NOT NULL,
          ok INTEGER NOT NULL,
          state TEXT NOT NULL,
          latency_ms REAL,
          error TEXT,
          details_json TEXT,
          PRIMARY KEY (ts_ms, broker)
        );

        CREATE INDEX IF NOT EXISTS idx_broker_connection_health_broker_ts
          ON broker_connection_health(broker, ts_ms);
        """
    )


def refresh_broker_connection_health(broker: str | None = None) -> Dict[str, Any]:
    broker_name = str(broker or _default_broker()).lower().strip()
    started_ms = _now_ms()

    # Non-live brokers report healthy immediately because there is no external
    # connection to probe and execution quality logic still expects a payload.
    if broker_name in ("sim", "paper", "sandbox", ""):
        payload = {
            "ok": True,
            "broker": broker_name or "sim",
            "state": "connected",
            "latency_ms": 0,
            "ts_ms": int(started_ms),
            "detail": "non_live_broker",
        }
    elif broker_name in ("ibkr", "interactivebrokers", "interactive_brokers", "ib_gateway", "ibgateway", "tws"):
        try:
            from engine.execution.broker_ibkr_gateway import ping_broker_connection
            payload = dict(
                ping_broker_connection(
                    timeout_s=float(os.environ.get("IBKR_CONNECT_TIMEOUT_S", "8")),
                    retries=int(os.environ.get("IBKR_CONNECT_RETRIES", "2")),
                ) or {}
            )
            payload["ts_ms"] = int(_now_ms())
        except Exception as e:
            payload = {
                "ok": False,
                "broker": "ibkr",
                "state": "connect_failed",
                "latency_ms": int(_now_ms() - started_ms),
                "ts_ms": int(_now_ms()),
                "error": str(e),
            }
    else:
        payload = {
            "ok": False,
            "broker": broker_name,
            "state": "unsupported_broker",
            "latency_ms": int(_now_ms() - started_ms),
            "ts_ms": int(_now_ms()),
            "error": "watchdog_not_configured_for_broker",
        }

    # Persist every probe so quality supervision can reason about recent broker
    # health without actively pinging on every read path.
    con = connect()
    try:
        _ensure_tables(con)
        con.execute(
            """
            INSERT INTO broker_connection_health(
              ts_ms, broker, ok, state, latency_ms, error, details_json
            )
            VALUES(?,?,?,?,?,?,?)
            """,
            (
                int(payload.get("ts_ms") or _now_ms()),
                str(payload.get("broker") or broker_name),
                1 if bool(payload.get("ok")) else 0,
                str(payload.get("state") or "unknown"),
                float(payload.get("latency_ms") or 0.0),
                str(payload.get("error")) if payload.get("error") is not None else None,
                json.dumps(payload, separators=(",", ":"), sort_keys=True),
            ),
        )
        con.commit()
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal(
                "EXECUTION_BROKER_WATCHDOG_CLOSE_FAILED",
                e,
                once_key="execution_broker_watchdog_refresh_close",
            )

    return payload


def _readonly_unavailable_payload(broker_name: str, detail: str = "broker_connection_snapshot_unavailable") -> Dict[str, Any]:
    return {
        "ok": False,
        "broker": str(broker_name or _default_broker() or "sim"),
        "state": "unknown",
        "latency_ms": 0.0,
        "detail": str(detail),
    }


def get_broker_connection_health(broker: str | None = None, *, readonly: bool = False) -> Dict[str, Any]:
    broker_name = str(broker or _default_broker()).lower().strip()
    con = connect(readonly=bool(readonly))
    try:
        if not bool(readonly):
            _ensure_tables(con)
        row = con.execute(
            """
            SELECT ts_ms, broker, ok, state, latency_ms, error, details_json
            FROM broker_connection_health
            WHERE broker = ?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (str(broker_name),),
        ).fetchone()

        if not row:
            if bool(readonly):
                return _readonly_unavailable_payload(broker_name, "broker_connection_snapshot_missing")
            return refresh_broker_connection_health(broker=broker_name)

        payload = {}
        try:
            payload = json.loads(row[6] or "{}")
            if not isinstance(payload, dict):
                payload = {}
        except Exception:
            payload = {}

        if not payload:
            payload = {
                "ts_ms": int(row[0]) if row[0] is not None else None,
                "broker": str(row[1] or broker_name),
                "ok": bool(row[2]),
                "state": str(row[3] or "unknown"),
                "latency_ms": float(row[4] or 0.0),
                "error": row[5],
            }

        # Refresh stale snapshots on demand so readers get bounded-freshness
        # status without needing their own watchdog scheduler.
        max_age_ms = int(os.environ.get("BROKER_HEALTH_MAX_AGE_MS", "30000"))
        ts_ms = int(payload.get("ts_ms") or 0)
        if ts_ms <= 0 or (_now_ms() - ts_ms) > max_age_ms:
            if bool(readonly):
                payload = dict(payload or {})
                payload.setdefault("broker", broker_name)
                payload["ok"] = False
                payload["detail"] = "broker_connection_snapshot_stale"
                return payload
            return refresh_broker_connection_health(broker=broker_name)

        return payload
    except Exception as e:
        if bool(readonly):
            _warn_nonfatal(
                "EXECUTION_BROKER_WATCHDOG_READONLY_SNAPSHOT_FAILED",
                e,
                once_key="execution_broker_watchdog_readonly_snapshot_failed",
                broker=str(broker_name),
            )
            return _readonly_unavailable_payload(broker_name, f"broker_connection_snapshot_failed:{type(e).__name__}")
        raise
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal(
                "EXECUTION_BROKER_WATCHDOG_READ_CLOSE_FAILED",
                e,
                once_key="execution_broker_watchdog_read_close",
            )
