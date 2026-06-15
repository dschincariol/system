"""Command-line tools for audit hash-chain verification."""

from __future__ import annotations

import argparse
import logging
import re
import sys

from engine.audit.chain import coerce_row_for_hash, table_columns
from engine.audit.hashing import compute_row_hash
from engine.audit.verifier import verify_all
from engine.runtime import storage

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m engine.audit")
    sub = parser.add_subparsers(dest="command", required=True)

    verify = sub.add_parser("verify")
    verify.add_argument("--table", default=None)
    verify.add_argument("--from-id", type=int, default=None)
    verify.add_argument("--to-id", type=int, default=None)
    verify.add_argument("--batch-size", type=int, default=10000)

    row_hash = sub.add_parser("hash-row")
    row_hash.add_argument("--table", required=True)
    row_hash.add_argument("--id", type=int, required=True)

    args = parser.parse_args(argv)
    if args.command == "verify":
        return _verify(args)
    if args.command == "hash-row":
        return _hash_row(args)
    return 2


def _verify(args) -> int:
    con = None
    try:
        storage.init_db()
        con = storage.connect()
        results = verify_all(
            con,
            table=args.table,
            from_id=args.from_id,
            to_id=args.to_id,
            batch_size=args.batch_size,
            emit_findings=True,
        )
        con.commit()
    except Exception as exc:
        if con is not None:
            try:
                con.rollback()
            # system-audit: ignore[silent_except] rollback is best-effort and
            # must not shadow the primary audit verification failure.
            except Exception:
                # fallback: rollback inside the error handler is best-effort.
                # The original `exc` is the failure operators care about; we
                # log the secondary rollback failure at debug level so it is
                # observable in journald without shadowing the primary error.
                logging.getLogger(__name__).debug(
                    "audit verify rollback also failed", exc_info=True
                )
        print(f"audit verify error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    finally:
        if con is not None:
            con.close()

    total_findings = 0
    for result in results:
        total_findings += len(result.findings)
        status = "PASS" if result.ok else "FAIL"
        print(f"{result.table_name}: {status} rows={result.rows_verified} findings={len(result.findings)}")
        for finding in result.findings[:20]:
            expected = finding.expected_hash.hex() if finding.expected_hash else ""
            actual = finding.actual_hash.hex() if finding.actual_hash else ""
            print(f"  row_id={finding.row_id} finding={finding.finding} expected={expected} actual={actual}")
    return 1 if total_findings else 0


def _hash_row(args) -> int:
    table = _ident(args.table)
    storage.init_db()
    con = storage.connect(readonly=True)
    try:
        columns = table_columns(con, table)
        row = con.execute(f"SELECT * FROM {table} WHERE id=?", (int(args.id),)).fetchone()
        if not row:
            print(f"{table}: row id={int(args.id)} not found", file=sys.stderr)
            return 1
        payload = coerce_row_for_hash(_row_dict(row, [col.name for col in columns]), columns)
        prev_hash = _bytes_or_none(payload.get("prev_hash"))
        computed = compute_row_hash(prev_hash, payload)
        actual = _bytes_or_none(payload.get("row_hash"))
    finally:
        con.close()

    print(f"table={table}")
    print(f"id={int(args.id)}")
    print(f"prev_hash={(prev_hash.hex() if prev_hash else '')}")
    print(f"computed_hash={computed.hex()}")
    print(f"stored_hash={(actual.hex() if actual else '')}")
    return 0 if actual == computed else 1


def _row_dict(row, columns: list[str]) -> dict[str, object]:
    if hasattr(row, "keys"):
        try:
            return {str(key): row[key] for key in row.keys()}
        except (AttributeError, TypeError, KeyError, IndexError):
            # fallback: some DBAPI rows expose incomplete mapping access but still support positional reads.
            return _row_sequence_dict(row, columns)
    return _row_sequence_dict(row, columns)


def _row_sequence_dict(row, columns: list[str]) -> dict[str, object]:
    return {str(columns[idx]): row[idx] for idx in range(min(len(columns), len(row)))}


def _bytes_or_none(value) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return bytes(value)
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return bytes(value)
    if isinstance(value, str):
        try:
            return bytes.fromhex(value)
        except ValueError:
            return value.encode("utf-8")
    return bytes(value)


def _ident(name: str) -> str:
    text = str(name or "")
    if not _IDENT_RE.match(text):
        raise ValueError(f"invalid_identifier:{text}")
    return text


if __name__ == "__main__":
    raise SystemExit(main())
