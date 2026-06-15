"""Runtime lock facade."""

from __future__ import annotations

import contextlib
import os
import threading
import time
from typing import Any, Dict, List, Optional

from engine.runtime import storage as _storage


if not bool(getattr(_storage, "_SQLITE_TEST_BACKEND", False)):
    from engine.runtime.locks_pg import *  # noqa: F401,F403
else:
    def _db_connect_ro_direct():
        return _storage.connect_liveness_ro_direct()

    def _db_connect_rw_direct():
        return _storage.connect_liveness_rw_direct()

    def _get_conn(*, readonly: bool = False):
        return _db_connect_ro_direct() if bool(readonly) else _db_connect_rw_direct()

    @contextlib.contextmanager
    def advisory_lock(name: str):
        del name
        con = _get_conn(readonly=False)
        try:
            yield con
        finally:
            con.close()

    @contextlib.contextmanager
    def advisory_xact_lock(name: str, con=None):
        del name
        owns = con is None
        conn = con or _get_conn(readonly=False)
        try:
            if owns:
                conn.begin_managed_write()
            yield conn
            if owns:
                conn.commit()
        except Exception:
            if owns:
                conn.rollback()
            raise
        finally:
            if owns:
                conn.close()

    def ensure_job_locks() -> None:
        _storage.init_db()

    def _owner() -> tuple[str, int]:
        return f"{os.getpid()}:{threading.get_ident()}", int(os.getpid())

    def acquire_lock(name: str, ttl_ms: int = 10_000) -> bool:
        ensure_job_locks()
        now = int(time.time() * 1000)
        exp = int(now + int(ttl_ms))
        owner, pid = _owner()
        con = _get_conn(readonly=False)
        try:
            con.begin_managed_write()
            row = con.execute(
                "SELECT owner, pid, expires_ms FROM job_locks WHERE job_name=?",
                (str(name),),
            ).fetchone()
            if row:
                current_owner = str(row["owner"] if hasattr(row, "keys") else row[0] or "")
                current_pid = int(row["pid"] if hasattr(row, "keys") else row[1] or 0)
                expires_ms = int(row["expires_ms"] if hasattr(row, "keys") else row[2] or 0)
                same_owner = current_owner == owner and current_pid == pid
                expired = expires_ms <= 0 or expires_ms <= now
                dead = current_pid > 0 and not _storage._pid_is_running(current_pid)
                if not (same_owner or expired or dead):
                    con.rollback()
                    return False
            con.execute(
                """
                INSERT INTO job_locks(job_name, owner, pid, acquired_ts_ms, heartbeat_ts_ms, expires_ms)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_name) DO UPDATE SET
                  owner=excluded.owner,
                  pid=excluded.pid,
                  acquired_ts_ms=excluded.acquired_ts_ms,
                  heartbeat_ts_ms=excluded.heartbeat_ts_ms,
                  expires_ms=excluded.expires_ms
                """,
                (str(name), owner, pid, now, now, exp),
            )
            con.commit()
            return True
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

    def touch_lock(name: str, ttl_ms: int = 10_000) -> None:
        ensure_job_locks()
        exp = int(time.time() * 1000) + int(ttl_ms)
        con = _get_conn(readonly=False)
        try:
            con.begin_managed_write()
            con.execute("UPDATE job_locks SET expires_ms=? WHERE job_name=?", (exp, str(name)))
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

    def heartbeat_lock(name: str, ttl_ms: int = 60_000) -> None:
        ensure_job_locks()
        now = int(time.time() * 1000)
        exp = now + int(ttl_ms)
        owner, pid = _owner()
        con = _get_conn(readonly=False)
        try:
            con.begin_managed_write()
            con.execute(
                """
                UPDATE job_locks
                SET heartbeat_ts_ms=?, owner=?, pid=?, expires_ms=?
                WHERE job_name=?
                """,
                (now, owner, pid, exp, str(name)),
            )
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

    def read_lock(name: str) -> Optional[Dict[str, Any]]:
        ensure_job_locks()
        con = _get_conn(readonly=True)
        try:
            row = con.execute(
                "SELECT job_name, owner, pid, expires_ms, heartbeat_ts_ms FROM job_locks WHERE job_name=?",
                (str(name),),
            ).fetchone()
            if not row:
                return None
            return {
                "job_name": str(row["job_name"]),
                "owner": str(row["owner"]),
                "pid": int(row["pid"] or 0),
                "expires_ms": int(row["expires_ms"] or 0),
                "heartbeat_ts_ms": int(row["heartbeat_ts_ms"] or 0),
            }
        finally:
            con.close()

    def release_lock(name: str) -> None:
        ensure_job_locks()
        con = _get_conn(readonly=False)
        try:
            con.begin_managed_write()
            con.execute("DELETE FROM job_locks WHERE job_name=?", (str(name),))
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

    def ensure_job_history() -> None:
        def _write(con) -> None:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS job_history (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  ts_ms INTEGER NOT NULL,
                  job_name TEXT NOT NULL,
                  event TEXT NOT NULL,
                  detail TEXT,
                  exit_code INTEGER
                )
                """
            )
            con.execute("CREATE INDEX IF NOT EXISTS idx_job_history_job_ts ON job_history(job_name, ts_ms DESC)")

        _storage.run_write_txn(_write, table="job_history", operation="ensure_job_history")

    def write_job_history(job_name: str, event: str, detail: str = "", exit_code: Optional[int] = None, ts_ms: Optional[int] = None) -> None:
        ensure_job_history()
        now = int(ts_ms or time.time() * 1000)

        def _write(con) -> None:
            con.execute(
                """
                INSERT INTO job_history(ts_ms, job_name, event, detail, exit_code)
                VALUES (?, ?, ?, ?, ?)
                """,
                (now, str(job_name or ""), str(event or ""), str(detail or ""), int(exit_code) if exit_code is not None else None),
            )

        _storage.run_write_txn(_write, attempts=1, table="job_history", operation="write_job_history")

    def read_job_history(job_name: str, limit: int = 200) -> List[Dict[str, Any]]:
        ensure_job_history()
        con = _get_conn(readonly=True)
        try:
            rows = con.execute(
                """
                SELECT ts_ms, event, detail, exit_code
                FROM job_history
                WHERE job_name=?
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (str(job_name or ""), int(limit)),
            ).fetchall()
            return [
                {
                    "ts_ms": int(row["ts_ms"] or 0),
                    "event": str(row["event"] or ""),
                    "detail": str(row["detail"] or ""),
                    "exit_code": int(row["exit_code"]) if row["exit_code"] is not None else None,
                }
                for row in rows or []
            ]
        finally:
            con.close()

    _ensure_job_locks = ensure_job_locks
    _ensure_job_history = ensure_job_history

    __all__ = [
        "advisory_lock",
        "advisory_xact_lock",
        "ensure_job_locks",
        "acquire_lock",
        "touch_lock",
        "heartbeat_lock",
        "read_lock",
        "release_lock",
        "ensure_job_history",
        "write_job_history",
        "read_job_history",
        "_ensure_job_locks",
        "_ensure_job_history",
        "_db_connect_ro_direct",
        "_db_connect_rw_direct",
        "_get_conn",
    ]
