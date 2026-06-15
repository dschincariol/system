"""
FILE: ipc.py

Runtime subsystem module for `ipc`.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional

from engine.runtime import dbapi_compat as dbapi
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.startup_write_gate import should_defer_noncritical_startup_write
from engine.runtime.storage import connect_ro, run_write_txn
from engine.runtime.metrics import emit_counter, emit_gauge
from engine.runtime.tracing import trace_event


log = get_logger("runtime.ipc")
_IPC_CHANNEL_STATE_BEST_EFFORT_MIN_INTERVAL_MS = max(
    0,
    int(float(os.environ.get("IPC_CHANNEL_STATE_BEST_EFFORT_MIN_INTERVAL_S", "2.0") or 2.0) * 1000.0),
)
_IPC_CHANNEL_STATE_LOCK = threading.Lock()
_IPC_CHANNEL_STATE_RECENT: Dict[str, Dict[str, Any]] = {}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload or {}, separators=(",", ":"), sort_keys=True)


def _warn(scope: str, err: Exception, **extra) -> None:
    log_failure(
        log,
        event="runtime_ipc_nonfatal",
        code=str(scope).replace(".", "_"),
        message=str(scope),
        error=err,
        level=logging.WARNING,
        component="engine.runtime.ipc",
        extra=dict(extra or {}) or None,
        persist=False,
    )


def _is_busy_error(err: Exception) -> bool:
    if isinstance(err, dbapi.OperationalError) and dbapi.is_transient_write_error(err):
        return True
    return dbapi.is_sqlite_error(err, "OperationalError") and (
        "locked" in str(err).lower() or "busy" in str(err).lower()
    )


def _dropped_ipc_result(
    kind: str,
    channel: str,
    *,
    now_ms: int,
    owner: str = "",
    msg_type: str = "",
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "ok": False,
        "dropped": True,
        "detail": "sqlite_busy_best_effort_drop",
        "channel": str(channel or "").strip(),
        "updated_ts_ms": int(now_ms),
    }
    if owner:
        out["owner"] = str(owner)
    if msg_type:
        out["msg_type"] = str(msg_type)
    if kind == "message":
        out["seq"] = 0
        out["created_ts_ms"] = int(now_ms)
    else:
        out["last_seq"] = 0
    return out


def _debounced_ipc_result(channel: str, *, owner: str, now_ms: int, last_seq: int) -> Dict[str, Any]:
    return {
        "ok": True,
        "skipped": True,
        "detail": "best_effort_debounced",
        "channel": str(channel or "").strip(),
        "owner": str(owner or "").strip(),
        "last_seq": int(last_seq),
        "updated_ts_ms": int(now_ms),
    }


def _should_debounce_channel_state(*, channel: str, owner: str, state_json: str, now_ms: int, best_effort: bool) -> Dict[str, Any] | None:
    if (not best_effort) or _IPC_CHANNEL_STATE_BEST_EFFORT_MIN_INTERVAL_MS <= 0:
        return None
    key = str(channel or "").strip()
    with _IPC_CHANNEL_STATE_LOCK:
        previous = dict(_IPC_CHANNEL_STATE_RECENT.get(key) or {})
        last_ts_ms = int(previous.get("updated_ts_ms") or 0)
        if (
            last_ts_ms > 0
            and (now_ms - last_ts_ms) < _IPC_CHANNEL_STATE_BEST_EFFORT_MIN_INTERVAL_MS
            and str(previous.get("owner") or "") == str(owner or "")
            and str(previous.get("state_json") or "") == str(state_json or "")
        ):
            return _debounced_ipc_result(
                key,
                owner=str(owner or ""),
                now_ms=int(now_ms),
                last_seq=int(previous.get("last_seq") or 0),
            )
    return None


def _note_channel_state_write(*, channel: str, owner: str, state_json: str, updated_ts_ms: int, last_seq: int) -> None:
    with _IPC_CHANNEL_STATE_LOCK:
        _IPC_CHANNEL_STATE_RECENT[str(channel or "").strip()] = {
            "owner": str(owner or "").strip(),
            "state_json": str(state_json or ""),
            "updated_ts_ms": int(updated_ts_ms),
            "last_seq": int(last_seq),
        }

def _ensure_ipc_tables(con):
    # IPC is SQLite-backed on purpose so multiple local processes can exchange
    # state without adding another daemon or message broker dependency.
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ipc_channels(
          channel TEXT PRIMARY KEY,
          owner TEXT,
          state_json TEXT,
          last_seq INTEGER,
          updated_ts_ms INTEGER
        )
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ipc_messages(
          seq INTEGER PRIMARY KEY AUTOINCREMENT,
          channel TEXT,
          msg_type TEXT,
          payload_json TEXT,
          sender TEXT,
          created_ts_ms INTEGER
        )
        """
    )


def _begin_owned_write(con) -> bool:
    if bool(getattr(con, "in_transaction", False)):
        return False
    con.begin_managed_write()
    return True

def publish_channel_state(
    channel: str,
    state: Dict[str, Any],
    owner: Optional[str] = None,
    con=None,
    best_effort: bool = False,
) -> Dict[str, Any]:
    # Channel state stores the latest snapshot for a topic-like channel; it is
    # overwrite-oriented and meant for current status, not event history.
    owns = con is None
    now_ms = _now_ms()
    channel_s = str(channel or "").strip()
    owner_s = str(owner or "system").strip() or "system"
    state_json = _json_dumps(state)
    result: Dict[str, Any] = {}
    if best_effort and owns and should_defer_noncritical_startup_write():
        return _dropped_ipc_result(
            "channel_state",
            channel_s,
            now_ms=int(now_ms),
            owner=owner_s,
        )
    debounced = _should_debounce_channel_state(
        channel=channel_s,
        owner=owner_s,
        state_json=state_json,
        now_ms=int(now_ms),
        best_effort=bool(best_effort and owns),
    )
    if debounced is not None:
        return debounced

    def _write(db) -> Dict[str, Any]:
        _ensure_ipc_tables(db)
        cur = db.execute(
            "SELECT COALESCE(last_seq, 0) FROM ipc_channels WHERE channel=?",
            (channel_s,),
        ).fetchone()
        last_seq = int(cur[0] or 0) if cur else 0

        db.execute(
            """
            INSERT INTO ipc_channels(channel, owner, state_json, last_seq, updated_ts_ms)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(channel) DO UPDATE SET
              owner=excluded.owner,
              state_json=excluded.state_json,
              last_seq=excluded.last_seq,
              updated_ts_ms=excluded.updated_ts_ms
            """,
            (channel_s, owner_s, state_json, int(last_seq), int(now_ms)),
        )
        return {
            "ok": True,
            "channel": channel_s,
            "owner": owner_s,
            "last_seq": int(last_seq),
            "updated_ts_ms": int(now_ms),
        }

    if owns:
        try:
            result = dict(
                run_write_txn(
                    _write,
                    attempts=1,
                    table="ipc_channels",
                    operation="publish_channel_state",
                    context={"channel": channel_s},
                    direct=True,
                    maintenance=False,
                )
                or {}
            )
        except Exception as e:
            if best_effort and _is_busy_error(e):
                _warn("ipc.publish_channel_state.busy_drop", e, channel=channel_s)
                result = _dropped_ipc_result(
                    "channel_state",
                    channel_s,
                    now_ms=int(now_ms),
                    owner=owner_s,
                )
            else:
                raise
        owns_txn = True
    else:
        owns_txn = False
        try:
            owns_txn = _begin_owned_write(con)
            result = dict(_write(con) or {})
            if owns_txn:
                con.commit()
        except Exception as e:
            if owns_txn and bool(getattr(con, "in_transaction", False)):
                try:
                    con.rollback()
                except Exception as e:
                    _warn("ipc.publish_channel_state.rollback", e, channel=channel_s)
            if best_effort and _is_busy_error(e):
                _warn("ipc.publish_channel_state.busy_drop", e, channel=channel_s)
                result = _dropped_ipc_result(
                    "channel_state",
                    channel_s,
                    now_ms=int(now_ms),
                    owner=owner_s,
                )
            else:
                raise

    should_emit_telemetry = bool((owns or owns_txn) and not bool(result.get("dropped")))
    if bool(result.get("ok")) and not bool(result.get("dropped")):
        _note_channel_state_write(
            channel=channel_s,
            owner=owner_s,
            state_json=state_json,
            updated_ts_ms=int(result.get("updated_ts_ms") or now_ms),
            last_seq=int(result.get("last_seq") or 0),
        )
    try:
        if should_emit_telemetry:
            try:
                emit_counter(
                    "job_heartbeat",
                    1,
                    component="engine.runtime.ipc",
                    job=channel_s,
                    extra_tags={"ipc_type": "channel_state"},
                )
                emit_gauge(
                    "queue_depth",
                    int(result.get("last_seq") or 0),
                    component="engine.runtime.ipc",
                    job=channel_s,
                    extra_tags={"queue_name": f"ipc:{channel_s}"},
                )
                trace_event(
                    "ipc_channel_state",
                    component="engine.runtime.ipc",
                    entity_type="ipc_channel",
                    entity_id=str(channel_s),
                    payload={"owner": str(owner_s), "last_seq": int(result.get("last_seq") or 0)},
                    job=channel_s,
                )
            except Exception as e:
                _warn("ipc.publish_channel_state.telemetry", e, channel=channel_s)
    finally:
        if owns and con is not None:
            try:
                con.close()
            except Exception as e:
                _warn("ipc.publish_channel_state.close", e, channel=channel_s)
    return result


def publish_message(
    channel: str,
    msg_type: str,
    payload: Dict[str, Any],
    sender: Optional[str] = None,
    con=None,
    best_effort: bool = False,
) -> Dict[str, Any]:
    # Messages are append-only and advance the channel sequence so readers can
    # distinguish event streams from the latest channel snapshot.
    owns = con is None
    now_ms = _now_ms()
    channel_s = str(channel or "").strip()
    type_s = str(msg_type or "state").strip() or "state"
    sender_s = str(sender or "system").strip() or "system"
    payload_json = _json_dumps(payload)
    result: Dict[str, Any] = {}
    if best_effort and owns and should_defer_noncritical_startup_write():
        return _dropped_ipc_result(
            "message",
            channel_s,
            now_ms=int(now_ms),
            owner=sender_s,
            msg_type=type_s,
        )

    def _write(db) -> Dict[str, Any]:
        _ensure_ipc_tables(db)
        cur = db.execute(
            """
            INSERT INTO ipc_messages(channel, msg_type, payload_json, sender, created_ts_ms)
            VALUES (?, ?, ?, ?, ?)
            """,
            (channel_s, type_s, payload_json, sender_s, int(now_ms)),
        )
        seq = int(cur.lastrowid or 0)
        db.execute(
            """
            INSERT INTO ipc_channels(channel, owner, state_json, last_seq, updated_ts_ms)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(channel) DO UPDATE SET
              owner=excluded.owner,
              last_seq=excluded.last_seq,
              updated_ts_ms=excluded.updated_ts_ms
            """,
            (channel_s, sender_s, _json_dumps({}), int(seq), int(now_ms)),
        )
        return {"ok": True, "channel": channel_s, "seq": int(seq), "created_ts_ms": int(now_ms)}

    if owns:
        try:
            result = dict(
                run_write_txn(
                    _write,
                    attempts=1,
                    table="ipc_messages",
                    operation="publish_message",
                    context={"channel": channel_s, "msg_type": type_s},
                    direct=True,
                    maintenance=False,
                )
                or {}
            )
        except Exception as e:
            if best_effort and _is_busy_error(e):
                _warn("ipc.publish_message.busy_drop", e, channel=channel_s, msg_type=type_s)
                result = _dropped_ipc_result(
                    "message",
                    channel_s,
                    now_ms=int(now_ms),
                    owner=sender_s,
                    msg_type=type_s,
                )
            else:
                raise
        owns_txn = True
    else:
        owns_txn = False
        try:
            owns_txn = _begin_owned_write(con)
            result = dict(_write(con) or {})
            if owns_txn:
                con.commit()
        except Exception as e:
            if owns_txn and bool(getattr(con, "in_transaction", False)):
                try:
                    con.rollback()
                except Exception as e:
                    _warn("ipc.publish_message.rollback", e, channel=channel_s, msg_type=type_s)
            if best_effort and _is_busy_error(e):
                _warn("ipc.publish_message.busy_drop", e, channel=channel_s, msg_type=type_s)
                result = _dropped_ipc_result(
                    "message",
                    channel_s,
                    now_ms=int(now_ms),
                    owner=sender_s,
                    msg_type=type_s,
                )
            else:
                raise

    should_emit_telemetry = bool((owns or owns_txn) and not bool(result.get("dropped")))
    try:
        if should_emit_telemetry:
            try:
                emit_counter(
                    "job_heartbeat",
                    1,
                    component="engine.runtime.ipc",
                    job=channel_s,
                    extra_tags={"ipc_type": "message"},
                )
                emit_gauge(
                    "queue_depth",
                    int(result.get("seq") or 0),
                    component="engine.runtime.ipc",
                    job=channel_s,
                    extra_tags={"queue_name": f"ipc:{channel_s}"},
                )
                trace_event(
                    "ipc_message",
                    component="engine.runtime.ipc",
                    entity_type="ipc_channel",
                    entity_id=str(channel_s),
                    payload={"msg_type": str(type_s), "sender": str(sender_s), "seq": int(result.get("seq") or 0)},
                    job=channel_s,
                )
            except Exception as e:
                _warn("ipc.publish_message.telemetry", e, channel=channel_s, msg_type=type_s)
    finally:
        if owns and con is not None:
            try:
                con.close()
            except Exception as e:
                _warn("ipc.publish_message.close", e, channel=channel_s)
    return result


def read_channel_state(channel: str, max_age_ms: Optional[int] = None, con=None) -> Dict[str, Any]:
    # A stale channel is surfaced as not-ok so callers can distinguish "missing"
    # from "present but out of date".
    owns = con is None
    if con is None:
        con = connect_ro()

    try:
        row = con.execute(
            "SELECT channel, owner, state_json, last_seq, updated_ts_ms FROM ipc_channels WHERE channel=?",
            (str(channel or "").strip(),),
        ).fetchone()
        if not row:
            return {
                "ok": True,
                "channel": str(channel or "").strip(),
                "owner": "",
                "state": {},
                "last_seq": 0,
                "updated_ts_ms": 0,
                "age_ms": 10**9,
            }

        updated_ts_ms = int(row[4] or 0)
        age_ms = max(0, _now_ms() - updated_ts_ms)
        if max_age_ms is not None and updated_ts_ms > 0 and age_ms > int(max_age_ms):
            return {
                "ok": False,
                "error": "channel_stale",
                "channel": str(row[0]),
                "owner": str(row[1] or ""),
                "last_seq": int(row[3] or 0),
                "updated_ts_ms": updated_ts_ms,
                "age_ms": int(age_ms),
            }

        try:
            state = json.loads(row[2] or "{}")
        except Exception as e:
            _warn("ipc.read_channel_state.decode", e, channel=str(row[0] or ""))
            state = {}

        return {
            "ok": True,
            "channel": str(row[0]),
            "owner": str(row[1] or ""),
            "state": state,
            "last_seq": int(row[3] or 0),
            "updated_ts_ms": updated_ts_ms,
            "age_ms": int(age_ms),
        }
    finally:
        if owns:
            try:
                con.close()
            except Exception as e:
                _warn("ipc.read_channel_state.close", e, channel=str(channel or "").strip())


def market_data_status(max_age_ms: Optional[int] = None, con=None, *, emit_telemetry: bool = False) -> Dict[str, Any]:
    snap = read_channel_state("market_data", max_age_ms=max_age_ms, con=con)
    if not snap.get("ok"):
        return snap

    state = snap.get("state") or {}

    if not isinstance(state, dict):
        state = {}

    state.setdefault("providers", {})
    state.setdefault("fresh_rows", 0)
    state.setdefault("fresh_symbols", 0)
    state.setdefault("last_price_ts_ms", 0)
    state.setdefault("price_age_ms", 10**9)
    state.setdefault("healthy_providers", 0)

    out = {
        "ok": True,
        "channel": "market_data",
        "running": bool(state.get("running")),
        "active_child": str(state.get("active_child") or ""),
        "child_pid": int(state.get("child_pid") or 0),
        "fresh_rows": int(state.get("fresh_rows") or 0),
        "fresh_symbols": int(state.get("fresh_symbols") or 0),
        "last_price_ts_ms": int(state.get("last_price_ts_ms") or 0),
        "price_age_ms": int(state.get("price_age_ms") or 0),
        "healthy_providers": int(state.get("healthy_providers") or 0),
        "providers": state.get("providers") or {},
        "updated_ts_ms": int(snap.get("updated_ts_ms") or 0),
        "age_ms": int(snap.get("age_ms") or 0),
        "owner": str(snap.get("owner") or ""),
        "last_seq": int(snap.get("last_seq") or 0),
    }

    if emit_telemetry:
        try:
            emit_gauge(
                "queue_depth",
                int(out.get("fresh_rows") or 0),
                component="engine.runtime.ipc",
                job="market_data",
                extra_tags={"queue_name": "market_data_rows"},
            )
            emit_gauge(
                "provider_uptime",
                1.0 if bool(out.get("running")) else 0.0,
                component="engine.runtime.ipc",
                job="market_data",
                extra_tags={"metric_scope": "market_data_status"},
            )
            trace_event(
                "market_data_event",
                component="engine.runtime.ipc",
                entity_type="ipc_channel",
                entity_id="market_data",
                payload=out,
                job="market_data",
            )
        except Exception as e:
            _warn("ipc.market_data_status.telemetry", e)

    return out


def read_messages(
    channel: str,
    after_seq: int = 0,
    limit: int = 100,
    con=None,
) -> Dict[str, Any]:
    init_db()
    owns = con is None
    if con is None:
        con = connect_ro()

    try:
        rows = con.execute(
            """
            SELECT seq, channel, msg_type, payload_json, sender, created_ts_ms
            FROM ipc_messages
            WHERE channel=? AND seq>?
            ORDER BY seq ASC
            LIMIT ?
            """,
            (str(channel or "").strip(), int(after_seq or 0), int(limit or 0)),
        ).fetchall() or []

        out: List[Dict[str, Any]] = []
        for row in rows:
            try:
                payload = json.loads(row[3] or "{}")
            except Exception as e:
                _warn("ipc.read_messages.decode", e, channel=str(channel or "").strip(), seq=int(row[0] or 0))
                payload = {}
            out.append(
                {
                    "seq": int(row[0] or 0),
                    "channel": str(row[1] or ""),
                    "msg_type": str(row[2] or ""),
                    "payload": payload,
                    "sender": str(row[4] or ""),
                    "created_ts_ms": int(row[5] or 0),
                }
            )

        return {"ok": True, "channel": str(channel or "").strip(), "messages": out}
    finally:
        if owns:
            try:
                con.close()
            except Exception as e:
                _warn("ipc.read_messages.close", e, channel=str(channel or "").strip())
