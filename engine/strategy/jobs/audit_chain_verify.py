"""Daily verifier for tamper-evident audit hash chains."""

from __future__ import annotations
import logging

import argparse
from dataclasses import dataclass
from typing import Sequence

from engine.audit.verifier import verify_all
from engine.runtime import storage
from engine.runtime.metrics_store import write_runtime_metric


@dataclass(frozen=True)
class AuditChainVerifySummary:
    rows_verified: int
    findings: int
    max_chain_length: int


def run(*, table: str | None = None, batch_size: int = 10000, emit_findings: bool = True) -> AuditChainVerifySummary:
    storage.init_db()
    with storage.connect() as conn:
        results = verify_all(conn, table=table, batch_size=batch_size, emit_findings=emit_findings)
        conn.commit()

    rows_verified = sum(result.rows_verified for result in results)
    findings = sum(len(result.findings) for result in results)
    max_chain_length = max((result.rows_verified for result in results), default=0)

    tags = {"job": "audit_chain_verify", "table": table or "*"}
    write_runtime_metric("audit_chain.rows_verified", value_num=rows_verified, tags=tags)
    write_runtime_metric("audit_chain.findings", value_num=findings, tags=tags)
    write_runtime_metric("audit_chain.max_chain_length", value_num=max_chain_length, tags=tags)

    return AuditChainVerifySummary(
        rows_verified=int(rows_verified),
        findings=int(findings),
        max_chain_length=int(max_chain_length),
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the daily audit hash-chain verifier")
    parser.add_argument("--table", default=None, help="verify one audit-chain table")
    parser.add_argument("--batch-size", type=int, default=10000, help="cursor batch size when supported")
    args = parser.parse_args(list(argv) if argv is not None else None)
    summary = run(table=args.table, batch_size=args.batch_size)
    print(
        "audit_chain_verify "
        f"rows_verified={summary.rows_verified} "
        f"findings={summary.findings} "
        f"max_chain_length={summary.max_chain_length}"
    )
    return 1 if summary.findings else 0


if __name__ == "__main__":
    raise SystemExit(main())
