from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


def connect(path: Path | str = ":memory:") -> sqlite3.Connection:
    con = sqlite3.connect(str(path), timeout=30.0, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL")
    return con


def create_audit_table(con: sqlite3.Connection, table: str = "audit_test") -> None:
    con.execute(
        f"""
        CREATE TABLE {table} (
            id INTEGER PRIMARY KEY,
            ts_ms INTEGER NOT NULL,
            actor TEXT,
            amount REAL,
            payload_json TEXT,
            prev_hash BLOB,
            row_hash BLOB NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE audit_chain_findings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            table_name TEXT,
            row_id INTEGER,
            finding TEXT,
            expected_hash BLOB,
            actual_hash BLOB,
            payload_excerpt TEXT
        )
        """
    )
    con.commit()


def row_dict(row: Any) -> dict[str, Any]:
    if hasattr(row, "keys"):
        return {str(key): row[key] for key in row.keys()}
    keys = ["id", "ts_ms", "actor", "amount", "payload_json", "prev_hash", "row_hash"]
    return {keys[idx]: row[idx] for idx in range(len(row))}
