"""Audit hash-chain micro-benchmark.

Synthetic, in-memory benchmark for the audit chain module. Uses an
in-memory SQLite database for self-contained timing — does not touch
the production Postgres backend or any real audit table.

Lives under tools/ (not engine/) so it stays out of the runtime path.
The runtime guard test ``tests/test_no_sqlite_in_runtime.py`` blocks
``import sqlite3`` from anywhere under ``engine/``; this benchmark
deliberately uses sqlite3 because in-memory sqlite is the cheapest way
to get a self-contained transactional store for timing the chain
algorithm in isolation.

Run::

    python tools/audit_benchmark.py --rows 10000 --batch-size 10000
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time

from engine.audit.chain import append_chain_row
from engine.audit.verifier import verify_table


def _benchmark(rows: int, batch_size: int) -> int:
    rows = max(1, int(rows or 1))
    table = "audit_benchmark"
    con = sqlite3.connect(":memory:")
    try:
        con.execute(
            f"""
            CREATE TABLE {table} (
                id INTEGER PRIMARY KEY,
                ts_ms INTEGER NOT NULL,
                actor TEXT,
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

        start = time.perf_counter()
        for idx in range(rows):
            append_chain_row(
                table,
                {"ts_ms": idx, "actor": "benchmark", "payload_json": {"idx": idx}},
                con,
            )
        append_s = time.perf_counter() - start

        start = time.perf_counter()
        result = verify_table(table, con, batch_size=batch_size, emit_findings=False)
        verify_s = time.perf_counter() - start
    finally:
        con.close()

    append_ms = (append_s / rows) * 1000.0
    verify_rows_s = rows / verify_s if verify_s > 0 else float("inf")
    print(f"rows={rows}")
    print(f"append_ms_per_row={append_ms:.4f}")
    print(f"verify_rows_per_second={verify_rows_s:.2f}")
    print(f"findings={len(result.findings)}")
    return 0 if result.ok and result.rows_verified == rows else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python tools/audit_benchmark.py")
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--batch-size", type=int, default=10000)
    args = parser.parse_args(argv)
    return _benchmark(args.rows, args.batch_size)


if __name__ == "__main__":
    sys.exit(main())
