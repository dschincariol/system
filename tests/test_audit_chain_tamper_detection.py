from __future__ import annotations

import json

from audit_chain_test_utils import connect, create_audit_table

from engine.audit.chain import append_chain_row
from engine.audit.verifier import verify_table


def test_tampered_row_reports_one_finding_at_that_row() -> None:
    con = connect()
    create_audit_table(con)
    for idx in range(4):
        append_chain_row("audit_test", {"ts_ms": idx, "actor": "system", "payload_json": {"idx": idx}}, con)

    con.execute(
        "UPDATE audit_test SET payload_json=? WHERE id=?",
        (json.dumps({"idx": 2, "tampered": True}, sort_keys=True), 3),
    )
    con.commit()

    result = verify_table("audit_test", con)

    assert len(result.findings) == 1
    finding = result.findings[0]
    assert finding.row_id == 3
    assert finding.finding == "row_hash_mismatch"

    stored = con.execute("SELECT table_name, row_id, finding FROM audit_chain_findings").fetchall()
    assert stored == [("audit_test", 3, "row_hash_mismatch")]
