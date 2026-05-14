"""Layer 5 negative test: chain-bypass breach detection at runtime.

Even with the AST-level lint (test_layer5_no_audit_inserts_outside_helper.py),
a runtime safety net is essential: if anything ever writes to a
chained audit table without going through `append_chain_row()`, the
chain breaks. The verifier MUST detect that.

This test deliberately bypasses the helper, then runs the verifier
and asserts a finding is reported. Requires a live Postgres (skipped
in dev when TS_PG_DSN is unreachable).
"""

from __future__ import annotations

import json
import time

import pytest

pytestmark = pytest.mark.requires_postgres


def test_direct_insert_breaks_chain_and_verifier_detects() -> None:
    from engine.audit.chain import append_chain_row, table_columns
    from engine.audit.verifier import verify_table
    from engine.runtime import storage

    storage.init_db()
    table = "model_promotion_audit"

    conn = storage.connect()
    try:
        # Append two valid rows so the chain has a non-trivial state
        # to start from.
        row_a = {"actor": "layer5_test", "details_json": {"step": 1}}
        row_b = {"actor": "layer5_test", "details_json": {"step": 2}}
        append_chain_row(table, row_a, conn)
        append_chain_row(table, row_b, conn)
        conn.commit()

        # Snapshot the column metadata and the latest row id so we
        # can reverse the breach at the end.
        cols = list(table_columns(conn, table))
        col_names = {c.name for c in cols}
        assert "prev_hash" in col_names and "row_hash" in col_names, (
            "model_promotion_audit lacks the hash-chain columns; "
            "either the schema migration regressed or the table name "
            "changed."
        )
        before = conn.execute(
            f"SELECT MAX(id) FROM {table}"
        ).fetchone()
        max_id_before = int(before[0] or 0)

        # Bypass: raw INSERT with NULL prev_hash and a synthetic
        # (incorrect) row_hash. This is exactly what a buggy or
        # malicious code path would do.
        ts_col = "ts_ms" if "ts_ms" in col_names else None
        cols_to_insert = ["actor", "details_json", "prev_hash", "row_hash"]
        if ts_col:
            cols_to_insert.append(ts_col)
        values = [
            "layer5_test_attacker",
            json.dumps({"forged": True}),
            None,                   # NULL prev_hash → linkage-broken
            b"\x00" * 32,           # synthetic, not a real SHA-256
        ]
        if ts_col:
            values.append(int(time.time() * 1000))
        cols_sql = ", ".join(f'"{c}"' for c in cols_to_insert)
        placeholders = ", ".join(["?"] * len(values))
        conn.execute(
            f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders})",
            tuple(values),
        )
        conn.commit()

        # Run the verifier; the breach must produce a finding.
        result = verify_table(conn, table)
        kinds = {f.finding for f in result.findings}
        assert result.findings, (
            "verify_table found NO findings after a deliberate raw INSERT "
            "into the chain — tamper detection is broken."
        )
        assert ("prev_hash_mismatch" in kinds
                or "row_hash_mismatch" in kinds), (
            f"verifier did not flag the breach as linkage- or hash-mismatch; "
            f"saw findings of kinds: {sorted(kinds)}"
        )
    finally:
        # Best-effort cleanup: delete any rows we inserted as part of
        # this test so production audit history is not polluted with
        # the synthetic forged row. We only clean up rows newer than
        # max_id_before so we never touch unrelated history.
        try:
            conn.execute(
                f"DELETE FROM {table} WHERE id > ? AND actor LIKE 'layer5_%'",
                (max_id_before,),
            )
            conn.commit()
        except Exception:
            conn.rollback()
        conn.close()
