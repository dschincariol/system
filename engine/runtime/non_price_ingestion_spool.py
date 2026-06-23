"""Durable SQLite WAL spool for refetchable non-price ingestion rows."""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

from engine.runtime.dbapi_compat import sqlite3
from engine.runtime.platform import default_local_db_dir

DEFAULT_SPOOL_FILENAME = "telemetry_append_buffer_spool.sqlite"


class NonPriceIngestionSpoolFullError(RuntimeError):
    """Raised when the durable non-price spool has no remaining row or byte room."""


class NonPriceIngestionSpoolUnavailableError(RuntimeError):
    """Raised when the durable non-price spool cannot be opened or mutated."""


def default_spool_path() -> Path:
    """Return the default spool path for non-price ingestion telemetry."""

    configured = str(os.environ.get("TELEMETRY_APPEND_BUFFER_SPOOL_PATH") or "").strip()
    if configured:
        return Path(configured).expanduser()
    db_path = str(os.environ.get("DB_PATH") or "").strip()
    if db_path and "://" not in db_path:
        db = Path(db_path).expanduser()
        root = db if db.suffix == "" else db.parent
    else:
        root = Path(os.environ.get("TS_DATA_ROOT") or default_local_db_dir()).expanduser()
    return root / DEFAULT_SPOOL_FILENAME


def _json_default(value: Any) -> Any:
    try:
        return str(value)
    except Exception:
        return None


def _is_corruption_error(error: BaseException) -> bool:
    text = str(error).lower()
    return any(
        marker in text
        for marker in (
            "database disk image is malformed",
            "file is not a database",
            "database schema is corrupt",
            "malformed database schema",
        )
    )


@dataclass(frozen=True)
class NonPriceIngestionSpoolRecord:
    """One durable non-price spool batch selected for replay."""

    id: int
    table: str
    created_ts_ms: int
    rows: tuple[tuple[Any, ...], ...]
    total_rows: int
    payload_bytes: int


@dataclass(frozen=True)
class CorruptNonPriceIngestionSpoolRecord:
    """One durable spool row whose JSON payload cannot be decoded."""

    id: int
    table: str
    created_ts_ms: int
    total_rows: int
    payload_json: str
    error: str
    payload_bytes: int


class SQLiteNonPriceIngestionSpool:
    """Small SQLite database that durably buffers high-volume non-price rows."""

    def __init__(
        self,
        *,
        path: str | os.PathLike[str] | None = None,
        max_rows: int,
        max_bytes: int,
        busy_timeout_ms: int = 5000,
        synchronous: str = "NORMAL",
    ) -> None:
        self.path = Path(path).expanduser() if path else default_spool_path()
        self.max_rows = max(1, int(max_rows))
        self.max_bytes = max(1, int(max_bytes))
        self.busy_timeout_ms = max(1, int(busy_timeout_ms))
        self.synchronous = str(synchronous or "NORMAL").strip().upper()
        if self.synchronous not in {"FULL", "NORMAL", "EXTRA"}:
            self.synchronous = "NORMAL"
        self._lock = threading.RLock()
        self._con: sqlite3.Connection | None = None
        self._corruption_events = 0
        self._last_quarantine_paths: list[str] = []

    def open(self) -> None:
        """Open the spool and create its schema."""

        with self._lock:
            if self._con is not None:
                return
            try:
                self._con = self._connect()
                self._ensure_schema(self._con)
                self._verify_integrity(self._con)
            except sqlite3.DatabaseError as exc:
                self._close_locked()
                if not _is_corruption_error(exc):
                    raise NonPriceIngestionSpoolUnavailableError(
                        f"sqlite_non_price_spool_open_failed:{exc}"
                    ) from exc
                self._recover_from_corruption_locked()
            except OSError as exc:
                self._close_locked()
                raise NonPriceIngestionSpoolUnavailableError(
                    f"sqlite_non_price_spool_open_failed:{exc}"
                ) from exc

    def close(self) -> None:
        """Close the open SQLite connection."""

        with self._lock:
            self._close_locked()

    def enqueue(
        self,
        *,
        table: str,
        rows: Sequence[Sequence[Any]],
        created_ts_ms: int,
    ) -> dict[str, Any]:
        """Append one table-specific batch and return updated spool stats."""

        clean_rows = [tuple(row or ()) for row in list(rows or [])]
        total_rows = int(len(clean_rows))
        if total_rows <= 0:
            return self.stats()
        payload = {
            "table": str(table),
            "created_ts_ms": int(created_ts_ms),
            "rows": [list(row) for row in clean_rows],
        }
        payload_json = json.dumps(payload, separators=(",", ":"), default=_json_default)
        payload_bytes = len(payload_json.encode("utf-8"))

        with self._lock:
            self.open()
            try:
                return self._insert_locked(
                    table=str(table),
                    created_ts_ms=int(created_ts_ms),
                    total_rows=int(total_rows),
                    payload_bytes=int(payload_bytes),
                    payload_json=payload_json,
                )
            except sqlite3.DatabaseError as exc:
                if not _is_corruption_error(exc):
                    raise NonPriceIngestionSpoolUnavailableError(
                        f"sqlite_non_price_spool_enqueue_failed:{exc}"
                    ) from exc
                self._recover_from_corruption_locked()
                return self._insert_locked(
                    table=str(table),
                    created_ts_ms=int(created_ts_ms),
                    total_rows=int(total_rows),
                    payload_bytes=int(payload_bytes),
                    payload_json=payload_json,
                )

    def select_batch(
        self,
        *,
        limit_rows: int,
        tables: Sequence[str] | None = None,
    ) -> tuple[list[NonPriceIngestionSpoolRecord], list[CorruptNonPriceIngestionSpoolRecord]]:
        """Return oldest selected rows without deleting them."""

        table_filter = tuple(str(table) for table in list(tables or []) if str(table).strip())
        with self._lock:
            self.open()
            try:
                records: list[Any] = []
                selected_table = ""
                for table in table_filter or self._table_names_locked():
                    rows = self._con.execute(  # type: ignore[union-attr]
                        """
                        SELECT id, table_name, created_ts_ms, payload_json, total_rows, payload_bytes
                        FROM non_price_ingestion_spool
                        WHERE table_name = ?
                        ORDER BY created_ts_ms ASC, id ASC
                        LIMIT ?
                        """,
                        (str(table), max(1, int(limit_rows))),
                    ).fetchall()
                    if rows:
                        records = list(rows)
                        selected_table = str(table)
                        break
                if not records and not table_filter:
                    records = self._con.execute(  # type: ignore[union-attr]
                        """
                        SELECT id, table_name, created_ts_ms, payload_json, total_rows, payload_bytes
                        FROM non_price_ingestion_spool
                        ORDER BY created_ts_ms ASC, id ASC
                        LIMIT ?
                        """,
                        (max(1, int(limit_rows)),),
                    ).fetchall()
                    selected_table = str(records[0][1] or "") if records else ""
            except sqlite3.DatabaseError as exc:
                raise NonPriceIngestionSpoolUnavailableError(
                    f"sqlite_non_price_spool_select_failed:{exc}"
                ) from exc

        decoded: list[NonPriceIngestionSpoolRecord] = []
        corrupt: list[CorruptNonPriceIngestionSpoolRecord] = []
        selected_rows = 0
        for row in records:
            table_name = str(row[1] or selected_table)
            if selected_table and table_name != selected_table:
                continue
            row_count = int(row[4] or 0)
            if decoded and selected_rows + row_count > max(1, int(limit_rows)):
                break
            payload_json = str(row[3] or "")
            payload_bytes = int(row[5] or len(payload_json.encode("utf-8")))
            try:
                payload = json.loads(payload_json)
                payload_rows = tuple(tuple(item or ()) for item in list(payload.get("rows") or []))
            except Exception as exc:
                with self._lock:
                    self._corruption_events += 1
                corrupt.append(
                    CorruptNonPriceIngestionSpoolRecord(
                        id=int(row[0]),
                        table=table_name,
                        created_ts_ms=int(row[2] or 0),
                        total_rows=row_count,
                        payload_json=payload_json,
                        error=f"{type(exc).__name__}:{exc}",
                        payload_bytes=payload_bytes,
                    )
                )
                continue
            selected_rows += int(len(payload_rows))
            decoded.append(
                NonPriceIngestionSpoolRecord(
                    id=int(row[0]),
                    table=table_name,
                    created_ts_ms=int(row[2] or 0),
                    rows=payload_rows,
                    total_rows=int(row_count or len(payload_rows)),
                    payload_bytes=payload_bytes,
                )
            )
        return decoded, corrupt

    def delete(self, ids: Iterable[int]) -> int:
        """Delete selected spool rows by id after the target DB commit succeeds."""

        clean_ids = sorted({int(value) for value in list(ids or []) if int(value) > 0})
        if not clean_ids:
            return 0
        placeholders = ",".join("?" for _ in clean_ids)
        with self._lock:
            self.open()
            try:
                cur = self._con.execute(  # type: ignore[union-attr]
                    f"DELETE FROM non_price_ingestion_spool WHERE id IN ({placeholders})",
                    tuple(clean_ids),
                )
                self._con.commit()  # type: ignore[union-attr]
            except sqlite3.DatabaseError as exc:
                raise NonPriceIngestionSpoolUnavailableError(
                    f"sqlite_non_price_spool_delete_failed:{exc}"
                ) from exc
            return int(cur.rowcount if cur.rowcount is not None else 0)

    def stats(self, *, table: str | None = None) -> dict[str, Any]:
        """Return current spool depth and byte usage."""

        with self._lock:
            self.open()
            return self._stats_locked(table=table)

    def stats_by_table(self) -> dict[str, dict[str, Any]]:
        """Return current spool depth grouped by table."""

        with self._lock:
            self.open()
            try:
                rows = self._con.execute(  # type: ignore[union-attr]
                    """
                    SELECT table_name, COUNT(*), COALESCE(SUM(total_rows), 0), COALESCE(SUM(payload_bytes), 0)
                    FROM non_price_ingestion_spool
                    GROUP BY table_name
                    """
                ).fetchall()
            except sqlite3.DatabaseError as exc:
                raise NonPriceIngestionSpoolUnavailableError(
                    f"sqlite_non_price_spool_stats_by_table_failed:{exc}"
                ) from exc
        return {
            str(row[0] or ""): {
                "pending_batches": int(row[1] or 0),
                "pending_rows": int(row[2] or 0),
                "pending_bytes": int(row[3] or 0),
            }
            for row in rows
        }

    def _insert_locked(
        self,
        *,
        table: str,
        created_ts_ms: int,
        total_rows: int,
        payload_bytes: int,
        payload_json: str,
    ) -> dict[str, Any]:
        stats = self._stats_locked()
        pending_rows = int(stats.get("pending_rows") or 0)
        pending_bytes = int(stats.get("pending_bytes") or 0)
        if pending_rows + int(total_rows) > int(self.max_rows):
            raise NonPriceIngestionSpoolFullError(f"spool_row_limit:{self.max_rows}")
        if pending_bytes + int(payload_bytes) > int(self.max_bytes):
            raise NonPriceIngestionSpoolFullError(f"spool_byte_limit:{self.max_bytes}")
        cur = self._con.execute(  # type: ignore[union-attr]
            """
            INSERT INTO non_price_ingestion_spool(
                table_name,
                created_ts_ms,
                total_rows,
                payload_bytes,
                payload_json
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                str(table),
                int(created_ts_ms),
                int(total_rows),
                int(payload_bytes),
                payload_json,
            ),
        )
        inserted_id = int(cur.lastrowid or 0)
        self._con.commit()  # type: ignore[union-attr]
        stats = self._stats_locked()
        stats["inserted_id"] = inserted_id
        return stats

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(
            str(self.path),
            timeout=float(self.busy_timeout_ms) / 1000.0,
            check_same_thread=False,
        )
        con.execute(f"PRAGMA busy_timeout={int(self.busy_timeout_ms)}")
        con.execute("PRAGMA journal_mode=WAL")
        con.execute(f"PRAGMA synchronous={self.synchronous}")
        con.execute("PRAGMA foreign_keys=ON")
        return con

    def _ensure_schema(self, con: sqlite3.Connection) -> None:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS non_price_ingestion_spool (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT NOT NULL,
                created_ts_ms INTEGER NOT NULL,
                total_rows INTEGER NOT NULL,
                payload_bytes INTEGER NOT NULL,
                payload_json TEXT NOT NULL
            )
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_non_price_ingestion_spool_table_created
            ON non_price_ingestion_spool(table_name, created_ts_ms, id)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_non_price_ingestion_spool_created
            ON non_price_ingestion_spool(created_ts_ms, id)
            """
        )
        con.commit()

    def _verify_integrity(self, con: sqlite3.Connection) -> None:
        row = con.execute("PRAGMA quick_check").fetchone()
        result = str(row[0] if row else "").lower()
        if result and result != "ok":
            raise sqlite3.DatabaseError(f"sqlite_quick_check_failed:{result}")

    def _recover_from_corruption_locked(self) -> None:
        self._quarantine_files_locked()
        self._con = self._connect()
        self._ensure_schema(self._con)

    def _quarantine_files_locked(self) -> None:
        self._corruption_events += 1
        self._last_quarantine_paths = []
        stamp = int(time.time() * 1000)
        for suffix in ("", "-wal", "-shm"):
            src = Path(f"{self.path}{suffix}")
            if not src.exists():
                continue
            dst = src.with_name(f"{src.name}.corrupt.{stamp}")
            try:
                src.replace(dst)
                self._last_quarantine_paths.append(str(dst))
            except OSError:
                continue

    def _table_names_locked(self) -> tuple[str, ...]:
        rows = self._con.execute(  # type: ignore[union-attr]
            """
            SELECT table_name
            FROM non_price_ingestion_spool
            GROUP BY table_name
            ORDER BY MIN(created_ts_ms), MIN(id)
            """
        ).fetchall()
        return tuple(str(row[0] or "") for row in rows if str(row[0] or ""))

    def _stats_locked(self, *, table: str | None = None) -> dict[str, Any]:
        try:
            if table:
                row = self._con.execute(  # type: ignore[union-attr]
                    """
                    SELECT
                        COUNT(*),
                        COALESCE(SUM(total_rows), 0),
                        COALESCE(SUM(payload_bytes), 0),
                        MIN(created_ts_ms),
                        MAX(created_ts_ms),
                        MIN(id),
                        MAX(id)
                    FROM non_price_ingestion_spool
                    WHERE table_name = ?
                    """,
                    (str(table),),
                ).fetchone()
            else:
                row = self._con.execute(  # type: ignore[union-attr]
                    """
                    SELECT
                        COUNT(*),
                        COALESCE(SUM(total_rows), 0),
                        COALESCE(SUM(payload_bytes), 0),
                        MIN(created_ts_ms),
                        MAX(created_ts_ms),
                        MIN(id),
                        MAX(id)
                    FROM non_price_ingestion_spool
                    """
                ).fetchone()
        except sqlite3.DatabaseError as exc:
            raise NonPriceIngestionSpoolUnavailableError(
                f"sqlite_non_price_spool_stats_failed:{exc}"
            ) from exc
        pending_batches = int(row[0] or 0)
        pending_rows = int(row[1] or 0)
        pending_bytes = int(row[2] or 0)
        oldest_created_ts_ms = int(row[3] or 0) if pending_rows > 0 else 0
        now_ms = int(time.time() * 1000)
        oldest_age_ms = max(0, int(now_ms) - int(oldest_created_ts_ms)) if oldest_created_ts_ms > 0 else 0
        file_bytes = 0
        for suffix in ("", "-wal", "-shm"):
            try:
                file_bytes += int(Path(f"{self.path}{suffix}").stat().st_size)
            except OSError:
                pass
        stats = {
            "ok": True,
            "path": str(self.path),
            "pending_batches": pending_batches,
            "pending_rows": pending_rows,
            "pending_bytes": pending_bytes,
            "file_bytes": int(file_bytes),
            "max_rows": int(self.max_rows),
            "max_bytes": int(self.max_bytes),
            "rows_fill_ratio": float(pending_rows / float(max(1, int(self.max_rows)))),
            "bytes_fill_ratio": float(pending_bytes / float(max(1, int(self.max_bytes)))),
            "oldest_created_ts_ms": row[3],
            "newest_created_ts_ms": row[4],
            "oldest_age_ms": int(oldest_age_ms),
            "oldest_id": row[5],
            "newest_id": row[6],
            "corruption_events": int(self._corruption_events),
            "last_quarantine_paths": list(self._last_quarantine_paths),
            "synchronous": str(self.synchronous),
        }
        if table:
            stats["table"] = str(table)
        return stats

    def _close_locked(self) -> None:
        if self._con is None:
            return
        try:
            self._con.close()
        finally:
            self._con = None


__all__ = [
    "CorruptNonPriceIngestionSpoolRecord",
    "NonPriceIngestionSpoolFullError",
    "NonPriceIngestionSpoolRecord",
    "NonPriceIngestionSpoolUnavailableError",
    "SQLiteNonPriceIngestionSpool",
    "default_spool_path",
]
