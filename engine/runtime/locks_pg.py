"""Postgres-backed cross-process locks and job history."""

from __future__ import annotations

import contextlib
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional

try:
    from psycopg.pq import TransactionStatus
except ModuleNotFoundError:
    class TransactionStatus:  # type: ignore[no-redef]
        IDLE = object()
        INERROR = object()

from engine.runtime.storage import connect, run_write_txn, _pid_is_running

LOG = logging.getLogger(__name__)

_CRC64_POLY = 0x42F0E1EBA9EA3693
_CRC64_TABLE: tuple[int, ...] | None = None


def _crc64_table() -> tuple[int, ...]:
    global _CRC64_TABLE
    if _CRC64_TABLE is not None:
        return _CRC64_TABLE
    table: list[int] = []
    for byte in range(256):
        crc = int(byte) << 56
        for _ in range(8):
            if crc & (1 << 63):
                crc = ((crc << 1) ^ _CRC64_POLY) & 0xFFFFFFFFFFFFFFFF
            else:
                crc = (crc << 1) & 0xFFFFFFFFFFFFFFFF
        table.append(crc)
    _CRC64_TABLE = tuple(table)
    return _CRC64_TABLE


def _lock_key(name: str) -> int:
    crc = 0
    table = _crc64_table()
    for byte in str(name or "").encode("utf-8"):
        crc = table[((crc >> 56) ^ int(byte)) & 0xFF] ^ ((crc << 8) & 0xFFFFFFFFFFFFFFFF)
    value = int(crc)
    if value >= 2**63:
        value -= 2**64
    return int(value)


def _transaction_failed(conn) -> bool:
    try:
        return conn.raw.info.transaction_status == TransactionStatus.INERROR
    except Exception:
        LOG.warning("postgres_transaction_status_check_failed", exc_info=True)
        return False


@contextlib.contextmanager
def advisory_lock(name: str):
    conn = connect(readonly=False)
    key = _lock_key(str(name))
    try:
        conn.execute("SELECT pg_advisory_lock(?)", (int(key),))
        yield conn
    finally:
        try:
            if _transaction_failed(conn):
                conn.rollback()
            conn.execute("SELECT pg_advisory_unlock(?)", (int(key),))
            conn.commit()
        finally:
            conn.close()


@contextlib.contextmanager
def advisory_xact_lock(name: str, con=None):
    owns = con is None
    conn = con or connect(readonly=False)
    try:
        conn.execute("SELECT pg_advisory_xact_lock(?)", (int(_lock_key(str(name))),))
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
    def _write(con) -> None:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS job_locks (
              job_name TEXT PRIMARY KEY,
              owner TEXT NOT NULL,
              pid BIGINT NOT NULL,
              acquired_ts_ms BIGINT NOT NULL,
              heartbeat_ts_ms BIGINT NOT NULL,
              expires_ms BIGINT
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS job_heartbeats (
              job_name TEXT PRIMARY KEY,
              owner TEXT NOT NULL,
              pid BIGINT NOT NULL,
              ts_ms BIGINT NOT NULL,
              extra_json JSONB
            )
            """
        )

    run_write_txn(_write, table="job_locks", operation="ensure_job_locks")


def acquire_lock(name: str, ttl_ms: int = 10_000) -> bool:
    ensure_job_locks()
    now = int(time.time() * 1000)
    exp = int(now + int(ttl_ms))
    owner = f"{os.getpid()}:{threading.get_ident()}"
    pid = int(os.getpid())

    def _write(con) -> bool:
        row = con.execute(
            "SELECT owner, pid, expires_ms FROM job_locks WHERE job_name=?",
            (str(name),),
        ).fetchone()
        if row:
            current_owner = str(row[0] or "")
            current_pid = int(row[1] or 0)
            expires_ms = int(row[2] or 0)
            same_owner = current_owner == owner and current_pid == pid
            expired = expires_ms <= 0 or expires_ms <= now
            dead = current_pid > 0 and not _pid_is_running(current_pid)
            if not (same_owner or expired or dead):
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
        return True

    return bool(run_write_txn(_write, attempts=1, timeout_s=0.5))


def touch_lock(name: str, ttl_ms: int = 10_000) -> None:
    ensure_job_locks()
    exp = int(time.time() * 1000) + int(ttl_ms)

    def _write(con) -> None:
        con.execute("UPDATE job_locks SET expires_ms=? WHERE job_name=?", (exp, str(name)))

    run_write_txn(_write, attempts=1, timeout_s=0.5)


def heartbeat_lock(name: str, ttl_ms: int = 60_000) -> None:
    ensure_job_locks()
    now = int(time.time() * 1000)
    exp = now + int(ttl_ms)
    owner = f"{os.getpid()}:{threading.get_ident()}"
    pid = int(os.getpid())

    def _write(con) -> None:
        con.execute(
            """
            UPDATE job_locks
            SET heartbeat_ts_ms=?, owner=?, pid=?, expires_ms=?
            WHERE job_name=?
            """,
            (now, owner, pid, exp, str(name)),
        )

    run_write_txn(_write, attempts=1, timeout_s=0.5)


def read_lock(name: str) -> Optional[Dict[str, Any]]:
    ensure_job_locks()
    con = connect(readonly=True)
    try:
        row = con.execute(
            "SELECT job_name, owner, pid, expires_ms, heartbeat_ts_ms FROM job_locks WHERE job_name=?",
            (str(name),),
        ).fetchone()
        if not row:
            return None
        return {
            "job_name": str(row[0] or ""),
            "owner": str(row[1] or ""),
            "pid": int(row[2] or 0),
            "expires_ms": int(row[3] or 0),
            "heartbeat_ts_ms": int(row[4] or 0),
        }
    finally:
        con.close()


def release_lock(name: str) -> None:
    ensure_job_locks()

    def _write(con) -> None:
        con.execute("DELETE FROM job_locks WHERE job_name=?", (str(name),))

    run_write_txn(_write, attempts=1, timeout_s=0.5)


def ensure_job_history() -> None:
    def _write(con) -> None:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS job_history (
              id BIGSERIAL PRIMARY KEY,
              ts_ms BIGINT NOT NULL,
              job_name TEXT NOT NULL,
              event TEXT NOT NULL,
              detail TEXT,
              exit_code INTEGER
            )
            """
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_job_history_job_ts ON job_history(job_name, ts_ms DESC)"
        )

    run_write_txn(_write, table="job_history", operation="ensure_job_history")


def write_job_history(
    job_name: str,
    event: str,
    detail: str = "",
    exit_code: Optional[int] = None,
    ts_ms: Optional[int] = None,
) -> None:
    ensure_job_history()
    now = int(ts_ms or time.time() * 1000)

    def _write(con) -> None:
        con.execute(
            """
            INSERT INTO job_history(ts_ms, job_name, event, detail, exit_code)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                now,
                str(job_name or ""),
                str(event or ""),
                str(detail or ""),
                int(exit_code) if exit_code is not None else None,
            ),
        )

    run_write_txn(_write, attempts=1, timeout_s=0.5)


def read_job_history(job_name: str, limit: int = 200) -> List[Dict[str, Any]]:
    ensure_job_history()
    con = connect(readonly=True)
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
                "ts_ms": int(row[0] or 0),
                "event": str(row[1] or ""),
                "detail": str(row[2] or ""),
                "exit_code": int(row[3]) if row[3] is not None else None,
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
]
