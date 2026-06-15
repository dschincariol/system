from __future__ import annotations

import ast
import importlib.util
import re
from pathlib import Path

import pytest

from tests.audit_chain_test_utils import connect, create_audit_table, row_dict

from engine.audit.chain import append_chain_row, coerce_row_for_hash, table_columns
from engine.audit.hashing import compute_row_hash
from engine.audit.verifier import verify_table
from engine.runtime import storage_pg


ROOT = Path(__file__).resolve().parents[1]


def test_append_three_rows_builds_linear_chain() -> None:
    con = connect()
    create_audit_table(con)

    for idx in range(3):
        append_chain_row(
            "audit_test",
            {
                "ts_ms": 1000 + idx,
                "actor": f"actor-{idx}",
                "amount": idx + 0.5,
                "payload_json": {"idx": idx, "status": "ok"},
            },
            con,
        )

    columns = table_columns(con, "audit_test")
    rows = con.execute("SELECT * FROM audit_test ORDER BY ts_ms, id").fetchall()

    prev_hash = None
    assert len(rows) == 3
    for raw in rows:
        row = coerce_row_for_hash(row_dict(raw), columns)
        expected = compute_row_hash(prev_hash, row)
        assert row["prev_hash"] == prev_hash
        assert row["row_hash"] == expected
        prev_hash = expected


def test_audit_chain_tables_have_no_direct_inserts_in_writers() -> None:
    writer_paths = [
        "engine/strategy/promotion_audit.py",
        "engine/execution/trade_attribution_ledger.py",
        "engine/execution/kill_switch.py",
        "engine/execution/execution_policy_engine.py",
        "engine/execution/position_reconcile.py",
        "engine/execution/execution_mode.py",
        "engine/cache/wrappers/execution_mode.py",
        "engine/cache/wrappers/kill_switch.py",
        "engine/strategy/decision_log.py",
    ]
    tables = (
        "trade_attribution_ledger",
        "kill_switch_audit",
        "execution_mode_audit",
        "execution_policy_audit",
        "position_reconcile_audit",
        "promotion_statistical_evidence",
        "model_promotion_audit",
        "decision_log",
    )
    pattern = re.compile(r"INSERT\s+(?:OR\s+(?:IGNORE|REPLACE)\s+)?INTO\s+(" + "|".join(tables) + r")\b", re.I)

    offenders = []
    for rel_path in writer_paths:
        text = "\n".join(_python_string_literals(ROOT / rel_path))
        if pattern.search(text):
            offenders.append(rel_path)

    assert offenders == []


def test_audit_record_read_helpers_surface_hash_hex() -> None:
    con = connect()
    con.execute(
        """
        CREATE TABLE decision_log (
            id INTEGER PRIMARY KEY,
            ts_ms INTEGER NOT NULL,
            event_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            horizon_s INTEGER NOT NULL,
            predicted_z REAL NOT NULL,
            confidence REAL NOT NULL,
            model_name TEXT NOT NULL,
            features_json JSONB,
            prev_hash BLOB,
            row_hash BLOB NOT NULL
        )
        """
    )
    con.commit()

    result = append_chain_row(
        "decision_log",
        {
            "ts_ms": 123,
            "event_id": 7,
            "symbol": "SPY",
            "horizon_s": 300,
            "predicted_z": 1.25,
            "confidence": 0.8,
            "model_name": "audit-test",
            "features_json": {"momentum": 0.4},
        },
        con,
    )

    detail = storage_pg.fetch_decision_detail(1, con=con)
    recent = storage_pg.fetch_recent_decisions(limit=5, con=con)

    assert detail is not None
    assert detail["prev_hash"] is None
    assert detail["row_hash"] == result.row_hash_hex
    assert detail["features_json"] == {"momentum": 0.4}
    assert recent == [detail]


def test_audit_chain_migration_backfills_existing_rows() -> None:
    con = connect()
    con.execute(
        """
        CREATE TABLE decision_log (
            id INTEGER PRIMARY KEY,
            ts_ms INTEGER NOT NULL,
            event_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            horizon_s INTEGER NOT NULL,
            predicted_z REAL NOT NULL,
            confidence REAL NOT NULL,
            model_name TEXT NOT NULL
        )
        """
    )
    con.executemany(
        """
        INSERT INTO decision_log(
          ts_ms, event_id, symbol, horizon_s, predicted_z, confidence, model_name
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [(idx, idx + 10, "SPY", 300, float(idx), 0.5, "historical") for idx in range(3)],
    )
    con.commit()

    migration = _load_migration("0007_audit_chain.py")
    migration.up(con)
    con.commit()

    columns = {row[1] for row in con.execute("PRAGMA table_info(decision_log)").fetchall()}
    null_hashes = con.execute("SELECT COUNT(*) FROM decision_log WHERE row_hash IS NULL").fetchone()[0]
    result = verify_table("decision_log", con, emit_findings=False)

    assert {"prev_hash", "row_hash"} <= columns
    assert null_hashes == 0
    assert result.ok
    assert result.rows_verified == 3


def test_append_propagates_broken_item_adapter() -> None:
    con = connect()
    create_audit_table(con)

    class BrokenItem:
        def item(self):
            raise RuntimeError("item adapter failed")

    with pytest.raises(RuntimeError, match="item adapter failed"):
        append_chain_row("audit_test", {"ts_ms": 1, "actor": BrokenItem()}, con)

    assert con.execute("SELECT COUNT(*) FROM audit_test").fetchone()[0] == 0


def test_append_propagates_broken_tolist_adapter() -> None:
    con = connect()
    create_audit_table(con)

    class BrokenList:
        def tolist(self):
            raise RuntimeError("tolist adapter failed")

    with pytest.raises(RuntimeError, match="tolist adapter failed"):
        append_chain_row("audit_test", {"ts_ms": 1, "actor": BrokenList()}, con)

    assert con.execute("SELECT COUNT(*) FROM audit_test").fetchone()[0] == 0


def test_append_propagates_unexpected_sequence_probe_failure() -> None:
    con = connect()
    create_audit_table(con)

    class SequenceProbeFailureConnection:
        def __init__(self, inner):
            self._inner = inner

        def execute(self, sql, params=()):
            if "nextval(pg_get_serial_sequence" in str(sql):
                raise RuntimeError("sequence probe failed")
            return self._inner.execute(sql, params)

        def __getattr__(self, name):
            return getattr(self._inner, name)

    with pytest.raises(RuntimeError, match="sequence probe failed"):
        append_chain_row("audit_test", {"ts_ms": 1, "actor": "system"}, SequenceProbeFailureConnection(con))

    assert con.execute("SELECT COUNT(*) FROM audit_test").fetchone()[0] == 0


def _python_string_literals(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    out: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            out.append(node.value)
        elif isinstance(node, ast.JoinedStr):
            parts: list[str] = []
            for value in node.values:
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    parts.append(value.value)
                else:
                    parts.append("{}")
            out.append("".join(parts))
    return out


def _load_migration(filename: str):
    path = ROOT / "engine/runtime/schema/migrations" / filename
    spec = importlib.util.spec_from_file_location(filename.replace(".py", ""), path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
