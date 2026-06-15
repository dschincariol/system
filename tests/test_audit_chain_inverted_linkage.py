from __future__ import annotations

import json

from tests.audit_chain_test_utils import connect, create_audit_table, row_dict

from engine.audit.chain import append_chain_row, coerce_row_for_hash, table_columns
from engine.audit.hashing import compute_row_hash
from engine.audit.verifier import verify_table


def test_verifier_rejects_forged_branch_before_row_hash_check() -> None:
    con = connect()
    create_audit_table(con)
    for idx in range(5):
        append_chain_row("audit_test", {"ts_ms": idx, "actor": "system", "payload_json": {"idx": idx}}, con)

    columns = table_columns(con, "audit_test")
    row_1 = _row(con, columns, 1)
    forged_prev = row_1["row_hash"]

    con.execute(
        "UPDATE audit_test SET payload_json=?, prev_hash=? WHERE id=?",
        (json.dumps({"idx": 2, "tampered": True}, separators=(",", ":"), sort_keys=True), forged_prev, 3),
    )
    row_3 = _row(con, columns, 3)
    forged_row_hash = compute_row_hash(forged_prev, row_3)
    con.execute("UPDATE audit_test SET row_hash=? WHERE id=?", (forged_row_hash, 3))

    prev_hash = forged_row_hash
    for row_id in (4, 5):
        con.execute("UPDATE audit_test SET prev_hash=? WHERE id=?", (prev_hash, row_id))
        row = _row(con, columns, row_id)
        row_hash = compute_row_hash(prev_hash, row)
        con.execute("UPDATE audit_test SET row_hash=? WHERE id=?", (row_hash, row_id))
        prev_hash = row_hash
    con.commit()

    result = verify_table("audit_test", con, emit_findings=False)

    assert len(result.findings) == 1
    finding = result.findings[0]
    assert finding.row_id == 3
    assert finding.finding == "prev_hash_mismatch"


def _row(con, columns, row_id: int) -> dict[str, object]:
    raw = con.execute("SELECT * FROM audit_test WHERE id=?", (row_id,)).fetchone()
    assert raw is not None
    return coerce_row_for_hash(row_dict(raw), columns)
