from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from tests.audit_chain_test_utils import connect, create_audit_table

from engine.audit.chain import append_chain_row
from engine.audit.verifier import verify_table


def test_concurrent_writers_produce_single_linear_chain(tmp_path) -> None:
    db_path = tmp_path / "audit-chain.sqlite"
    setup = connect(db_path)
    create_audit_table(setup)
    setup.close()

    def write_rows(worker: int) -> None:
        con = connect(db_path)
        try:
            for idx in range(100):
                append_chain_row(
                    "audit_test",
                    {
                        "ts_ms": 1,
                        "actor": f"worker-{worker}",
                        "amount": float(idx),
                        "payload_json": {"worker": worker, "idx": idx},
                    },
                    con,
                )
        finally:
            con.close()

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(write_rows, range(8)))

    con = connect(db_path)
    try:
        ids = [row[0] for row in con.execute("SELECT id FROM audit_test ORDER BY id").fetchall()]
        assert ids == list(range(1, 801))

        result = verify_table("audit_test", con, emit_findings=False)
        assert result.rows_verified == 800
        assert result.findings == ()

        duplicate_prev = con.execute(
            """
            SELECT COUNT(*)
            FROM (
              SELECT prev_hash
              FROM audit_test
              WHERE prev_hash IS NOT NULL
              GROUP BY prev_hash
              HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
        assert duplicate_prev == 0
    finally:
        con.close()
