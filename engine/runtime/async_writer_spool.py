"""Durable SQLite WAL spool for async price persistence."""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from engine.runtime.dbapi_compat import sqlite3
from engine.runtime.platform import default_local_db_dir

DEFAULT_SPOOL_FILENAME = "async_price_writer_spool.sqlite"


class PriceWriterSpoolFullError(RuntimeError):
    """Raised when the durable spool is at its configured envelope or byte cap."""


class PriceWriterSpoolUnavailableError(RuntimeError):
    """Raised when the durable spool cannot be opened or written."""


def default_spool_path() -> Path:
    """Return the default path for the async price-writer spool."""

    configured = str(os.environ.get("ASYNC_PRICE_WRITER_SPOOL_PATH") or "").strip()
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
class PriceWriterSpoolRecord:
    """One valid durable spool row selected for replay."""

    id: int
    source: str
    created_ts_ms: int
    prices: tuple[dict[str, Any], ...]
    quotes: tuple[dict[str, Any], ...]
    raw: tuple[dict[str, Any], ...]
    total_rows: int
    payload_bytes: int


@dataclass(frozen=True)
class CorruptPriceWriterSpoolRecord:
    """One durable spool row whose payload cannot be decoded."""

    id: int
    created_ts_ms: int
    payload_json: str
    error: str
    payload_bytes: int


class SQLitePriceWriterSpool:
    """Small SQLite database that durably buffers async price-write envelopes."""

    def __init__(
        self,
        *,
        path: str | os.PathLike[str] | None = None,
        max_envelopes: int,
        max_bytes: int,
        busy_timeout_ms: int = 5000,
        synchronous: str = "FULL",
    ) -> None:
        self.path = Path(path).expanduser() if path else default_spool_path()
        self.max_envelopes = max(1, int(max_envelopes))
        self.max_bytes = max(1, int(max_bytes))
        self.busy_timeout_ms = max(1, int(busy_timeout_ms))
        self.synchronous = str(synchronous or "FULL").strip().upper()
        if self.synchronous not in {"FULL", "NORMAL", "EXTRA"}:
            self.synchronous = "FULL"
        self._lock = threading.RLock()
        self._con: sqlite3.Connection | None = None
        self._corruption_events = 0
        self._last_quarantine_paths: list[str] = []

    def open(self) -> None:
        """Open the spool database and create its schema."""

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
                    raise PriceWriterSpoolUnavailableError(f"sqlite_spool_open_failed:{exc}") from exc
                self._recover_from_corruption_locked()
            except OSError as exc:
                self._close_locked()
                raise PriceWriterSpoolUnavailableError(f"sqlite_spool_open_failed:{exc}") from exc

    def close(self) -> None:
        """Close the open spool connection."""

        with self._lock:
            self._close_locked()

    def enqueue(
        self,
        *,
        source: str,
        created_ts_ms: int,
        prices: Iterable[dict[str, Any]] = (),
        quotes: Iterable[dict[str, Any]] = (),
        raw: Iterable[dict[str, Any]] = (),
    ) -> dict[str, Any]:
        """Append one envelope and return updated spool stats."""

        payload = {
            "source": str(source or "runtime"),
            "created_ts_ms": int(created_ts_ms),
            "prices": [dict(row or {}) for row in (prices or ())],
            "quotes": [dict(row or {}) for row in (quotes or ())],
            "raw": [dict(row or {}) for row in (raw or ())],
        }
        payload_json = json.dumps(payload, separators=(",", ":"), sort_keys=True, default=_json_default)
        payload_bytes = len(payload_json.encode("utf-8"))
        total_rows = int(len(payload["prices"]) + len(payload["quotes"]) + len(payload["raw"]))
        if total_rows <= 0:
            return self.stats()

        with self._lock:
            self.open()
            try:
                return self._insert_locked(
                    created_ts_ms=int(created_ts_ms),
                    source=str(source or "runtime"),
                    price_rows=len(payload["prices"]),
                    quote_rows=len(payload["quotes"]),
                    raw_rows=len(payload["raw"]),
                    total_rows=int(total_rows),
                    payload_bytes=int(payload_bytes),
                    payload_json=payload_json,
                )
            except sqlite3.DatabaseError as exc:
                if not _is_corruption_error(exc):
                    raise PriceWriterSpoolUnavailableError(f"sqlite_spool_enqueue_failed:{exc}") from exc
                self._recover_from_corruption_locked()
                return self._insert_locked(
                    created_ts_ms=int(created_ts_ms),
                    source=str(source or "runtime"),
                    price_rows=len(payload["prices"]),
                    quote_rows=len(payload["quotes"]),
                    raw_rows=len(payload["raw"]),
                    total_rows=int(total_rows),
                    payload_bytes=int(payload_bytes),
                    payload_json=payload_json,
                )

    def select_batch(
        self,
        *,
        limit: int,
    ) -> tuple[list[PriceWriterSpoolRecord], list[CorruptPriceWriterSpoolRecord]]:
        """Return the oldest valid and corrupt rows without deleting them."""

        with self._lock:
            self.open()
            try:
                rows = self._con.execute(  # type: ignore[union-attr]
                    """
                    SELECT id, source, created_ts_ms, payload_json, total_rows, payload_bytes
                    FROM async_price_writer_spool
                    ORDER BY created_ts_ms ASC, id ASC
                    LIMIT ?
                    """,
                    (max(1, int(limit)),),
                ).fetchall()
            except sqlite3.DatabaseError as exc:
                raise PriceWriterSpoolUnavailableError(f"sqlite_spool_select_failed:{exc}") from exc

        records: list[PriceWriterSpoolRecord] = []
        corrupt: list[CorruptPriceWriterSpoolRecord] = []
        for row in rows:
            row_id = int(row[0])
            created_ts_ms = int(row[2])
            payload_json = str(row[3] or "")
            payload_bytes = int(row[5] or len(payload_json.encode("utf-8")))
            try:
                payload = json.loads(payload_json)
                prices = tuple(dict(item or {}) for item in list(payload.get("prices") or []))
                quotes = tuple(dict(item or {}) for item in list(payload.get("quotes") or []))
                raw = tuple(dict(item or {}) for item in list(payload.get("raw") or []))
            except Exception as exc:
                with self._lock:
                    self._corruption_events += 1
                corrupt.append(
                    CorruptPriceWriterSpoolRecord(
                        id=row_id,
                        created_ts_ms=created_ts_ms,
                        payload_json=payload_json,
                        error=f"{type(exc).__name__}:{exc}",
                        payload_bytes=payload_bytes,
                    )
                )
                continue
            records.append(
                PriceWriterSpoolRecord(
                    id=row_id,
                    source=str(row[1] or "runtime"),
                    created_ts_ms=created_ts_ms,
                    prices=prices,
                    quotes=quotes,
                    raw=raw,
                    total_rows=int(row[4] or (len(prices) + len(quotes) + len(raw))),
                    payload_bytes=payload_bytes,
                )
            )
        return records, corrupt

    def delete(self, ids: Iterable[int]) -> int:
        """Delete spool rows by id and return the number removed."""

        clean_ids = sorted({int(value) for value in list(ids or []) if int(value) > 0})
        if not clean_ids:
            return 0
        placeholders = ",".join("?" for _ in clean_ids)
        with self._lock:
            self.open()
            try:
                cur = self._con.execute(  # type: ignore[union-attr]
                    f"DELETE FROM async_price_writer_spool WHERE id IN ({placeholders})",
                    tuple(clean_ids),
                )
                self._con.commit()  # type: ignore[union-attr]
            except sqlite3.DatabaseError as exc:
                raise PriceWriterSpoolUnavailableError(f"sqlite_spool_delete_failed:{exc}") from exc
            return int(cur.rowcount if cur.rowcount is not None else 0)

    def stats(self) -> dict[str, Any]:
        """Return current spool depth and byte usage."""

        with self._lock:
            self.open()
            return self._stats_locked()

    def _insert_locked(
        self,
        *,
        created_ts_ms: int,
        source: str,
        price_rows: int,
        quote_rows: int,
        raw_rows: int,
        total_rows: int,
        payload_bytes: int,
        payload_json: str,
    ) -> dict[str, Any]:
        stats = self._stats_locked()
        pending_batches = int(stats.get("pending_batches") or 0)
        pending_bytes = int(stats.get("pending_bytes") or 0)
        if pending_batches >= int(self.max_envelopes):
            raise PriceWriterSpoolFullError(f"spool_envelope_limit:{self.max_envelopes}")
        if pending_bytes + int(payload_bytes) > int(self.max_bytes):
            raise PriceWriterSpoolFullError(f"spool_byte_limit:{self.max_bytes}")
        self._con.execute(  # type: ignore[union-attr]
            """
            INSERT INTO async_price_writer_spool(
                source,
                created_ts_ms,
                price_rows,
                quote_rows,
                raw_rows,
                total_rows,
                payload_bytes,
                payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(source or "runtime"),
                int(created_ts_ms),
                int(price_rows),
                int(quote_rows),
                int(raw_rows),
                int(total_rows),
                int(payload_bytes),
                payload_json,
            ),
        )
        self._con.commit()  # type: ignore[union-attr]
        return self._stats_locked()

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
            CREATE TABLE IF NOT EXISTS async_price_writer_spool (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                created_ts_ms INTEGER NOT NULL,
                price_rows INTEGER NOT NULL DEFAULT 0,
                quote_rows INTEGER NOT NULL DEFAULT 0,
                raw_rows INTEGER NOT NULL DEFAULT 0,
                total_rows INTEGER NOT NULL,
                payload_bytes INTEGER NOT NULL,
                payload_json TEXT NOT NULL
            )
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_async_price_writer_spool_created
            ON async_price_writer_spool(created_ts_ms, id)
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

    def _stats_locked(self) -> dict[str, Any]:
        try:
            row = self._con.execute(  # type: ignore[union-attr]
                """
                SELECT
                    COUNT(*),
                    COALESCE(SUM(total_rows), 0),
                    COALESCE(SUM(payload_bytes), 0),
                    MIN(created_ts_ms),
                    MAX(created_ts_ms)
                FROM async_price_writer_spool
                """
            ).fetchone()
        except sqlite3.DatabaseError as exc:
            raise PriceWriterSpoolUnavailableError(f"sqlite_spool_stats_failed:{exc}") from exc
        pending_batches = int(row[0] or 0)
        pending_rows = int(row[1] or 0)
        pending_bytes = int(row[2] or 0)
        file_bytes = 0
        for suffix in ("", "-wal", "-shm"):
            try:
                file_bytes += int(Path(f"{self.path}{suffix}").stat().st_size)
            except OSError:
                pass
        return {
            "ok": True,
            "path": str(self.path),
            "pending_batches": pending_batches,
            "pending_rows": pending_rows,
            "pending_bytes": pending_bytes,
            "file_bytes": file_bytes,
            "max_envelopes": int(self.max_envelopes),
            "max_bytes": int(self.max_bytes),
            "bytes_fill_ratio": float(pending_bytes / float(max(1, int(self.max_bytes)))),
            "oldest_created_ts_ms": row[3],
            "newest_created_ts_ms": row[4],
            "corruption_events": int(self._corruption_events),
            "last_quarantine_paths": list(self._last_quarantine_paths),
        }

    def _close_locked(self) -> None:
        if self._con is None:
            return
        try:
            self._con.close()
        finally:
            self._con = None


__all__ = [
    "CorruptPriceWriterSpoolRecord",
    "PriceWriterSpoolFullError",
    "PriceWriterSpoolRecord",
    "PriceWriterSpoolUnavailableError",
    "SQLitePriceWriterSpool",
    "default_spool_path",
]
