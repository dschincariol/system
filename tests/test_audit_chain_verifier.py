from __future__ import annotations

import sqlite3

import pytest

from audit_chain_test_utils import connect, create_audit_table

from engine.audit import cli, verifier
from engine.audit.chain import append_chain_row
from engine.audit.verifier import verify_table


def test_clean_chain_verifies_without_findings() -> None:
    con = connect()
    create_audit_table(con)
    for idx in range(5):
        append_chain_row("audit_test", {"ts_ms": idx, "actor": "system", "payload_json": {"idx": idx}}, con)

    result = verify_table("audit_test", con)

    assert result.ok
    assert result.rows_verified == 5
    assert result.findings == ()


def test_cli_returns_zero_for_clean_chain(monkeypatch, capsys) -> None:
    con = connect()
    create_audit_table(con)
    append_chain_row("audit_test", {"ts_ms": 1, "actor": "system", "payload_json": {"ok": True}}, con)

    monkeypatch.setattr(cli.storage, "init_db", lambda: None)
    monkeypatch.setattr(cli.storage, "connect", lambda: con)

    exit_code = cli.main(["verify", "--table", "audit_test"])

    assert exit_code == 0
    assert "audit_test: PASS rows=1 findings=0" in capsys.readouterr().out


def test_windowed_verifier_seeds_previous_hash() -> None:
    con = connect()
    create_audit_table(con)
    for idx in range(5):
        append_chain_row("audit_test", {"ts_ms": idx, "actor": "system", "payload_json": {"idx": idx}}, con)

    result = verify_table("audit_test", con, from_id=3, to_id=4, batch_size=1)

    assert result.ok
    assert result.rows_verified == 3


def test_windowed_verifier_rehashes_boundary_row() -> None:
    con = connect()
    create_audit_table(con)
    for idx in range(5):
        append_chain_row("audit_test", {"ts_ms": idx, "actor": "system", "payload_json": {"idx": idx}}, con)

    con.execute("UPDATE audit_test SET actor='tampered-boundary' WHERE id=2")
    con.commit()

    result = verify_table("audit_test", con, from_id=3, to_id=4, batch_size=1, emit_findings=False)

    assert result.rows_verified == 3
    assert any(finding.row_id == 2 for finding in result.findings)


def test_windowed_verifier_reports_missing_boundary_row() -> None:
    con = connect()
    create_audit_table(con)
    for idx in range(5):
        append_chain_row("audit_test", {"ts_ms": idx, "actor": "system", "payload_json": {"idx": idx}}, con)

    con.execute("DELETE FROM audit_test WHERE id=2")
    con.commit()

    result = verify_table("audit_test", con, from_id=3, to_id=4, batch_size=1, emit_findings=False)

    assert any(
        finding.row_id == 2 and finding.finding == "window_boundary_missing"
        for finding in result.findings
    )


def test_cli_returns_nonzero_for_broken_chain(monkeypatch) -> None:
    con = connect()
    create_audit_table(con)
    append_chain_row("audit_test", {"ts_ms": 1, "actor": "system", "payload_json": {"ok": True}}, con)
    con.execute("UPDATE audit_test SET actor='tampered' WHERE id=1")
    con.commit()

    monkeypatch.setattr(cli.storage, "init_db", lambda: None)
    monkeypatch.setattr(cli.storage, "connect", lambda: con)

    assert cli.main(["verify", "--table", "audit_test"]) == 1


def test_cli_returns_two_for_uncaught_verify_exception(monkeypatch, capsys) -> None:
    class FakeConnection:
        def __init__(self) -> None:
            self.closed = False
            self.rolled_back = False

        def rollback(self):
            self.rolled_back = True

        def close(self):
            self.closed = True

    con = FakeConnection()

    def raise_verify(*args, **kwargs):
        del args, kwargs
        raise ValueError("canonicalization failed")

    monkeypatch.setattr(cli.storage, "init_db", lambda: None)
    monkeypatch.setattr(cli.storage, "connect", lambda: con)
    monkeypatch.setattr(cli, "verify_all", raise_verify)

    assert cli.main(["verify", "--table", "audit_test"]) == 2
    assert con.rolled_back
    assert con.closed
    assert "audit verify error: ValueError: canonicalization failed" in capsys.readouterr().err


def test_cli_hash_row_reproduces_stored_hash(monkeypatch, capsys) -> None:
    con = connect()
    create_audit_table(con)
    result = append_chain_row("audit_test", {"ts_ms": 1, "actor": "system", "payload_json": {"ok": True}}, con)

    monkeypatch.setattr(cli.storage, "init_db", lambda: None)
    monkeypatch.setattr(cli.storage, "connect", lambda readonly=False: con)

    assert cli.main(["hash-row", "--table", "audit_test", "--id", "1"]) == 0
    output = capsys.readouterr().out
    assert f"computed_hash={result.row_hash_hex}" in output
    assert f"stored_hash={result.row_hash_hex}" in output


def test_verifier_seed_lookup_failure_is_observable() -> None:
    con = connect()
    create_audit_table(con)
    for idx in range(2):
        append_chain_row("audit_test", {"ts_ms": idx, "actor": "system"}, con)

    class SeedFailureConnection:
        def __init__(self, inner):
            self._inner = inner

        def execute(self, sql, params=()):
            if "WHERE id < ?" in str(sql):
                raise RuntimeError("seed lookup failed")
            return self._inner.execute(sql, params)

        def __getattr__(self, name):
            return getattr(self._inner, name)

    with pytest.raises(RuntimeError, match="seed lookup failed"):
        verify_table("audit_test", SeedFailureConnection(con), from_id=2)


def test_verifier_finding_persistence_failure_is_observable(monkeypatch) -> None:
    con = connect()
    create_audit_table(con)
    append_chain_row("audit_test", {"ts_ms": 1, "actor": "system", "payload_json": {"ok": True}}, con)
    con.execute("UPDATE audit_test SET actor='tampered' WHERE id=1")
    con.execute("DROP TABLE audit_chain_findings")
    con.commit()
    captured: list[dict[str, object]] = []

    def fake_log_failure(logger, **kwargs):
        del logger
        captured.append(dict(kwargs))
        return {}

    monkeypatch.setattr(verifier, "log_failure", fake_log_failure)

    with pytest.raises(sqlite3.OperationalError):
        verifier.verify_table("audit_test", con)

    assert captured
    assert captured[0]["event"] == "audit_chain_finding_emit_failed"
    assert captured[0]["code"] == "AUDIT_CHAIN_FINDING_EMIT_FAILED"


def test_verifier_cursor_close_failure_records_degraded_health(monkeypatch) -> None:
    con = connect()
    create_audit_table(con)
    append_chain_row("audit_test", {"ts_ms": 1, "actor": "system"}, con)
    observed: list[tuple[str, BaseException, dict[str, object]]] = []

    class CloseFailureCursor:
        def __init__(self, inner):
            self._inner = inner

        def fetchmany(self, size):
            return self._inner.fetchmany(size)

        def close(self):
            raise sqlite3.ProgrammingError("cursor close failed")

    class CloseFailureConnection:
        def __init__(self, inner):
            self._inner = inner

        def execute(self, sql, params=()):
            cursor = self._inner.execute(sql, params)
            if str(sql).startswith("SELECT * FROM audit_test"):
                return CloseFailureCursor(cursor)
            return cursor

        def __getattr__(self, name):
            return getattr(self._inner, name)

    def fake_record_degraded(event, error, **extra):
        observed.append((str(event), error, dict(extra)))

    monkeypatch.setattr(verifier, "_record_verifier_degraded", fake_record_degraded)

    result = verifier.verify_table("audit_test", CloseFailureConnection(con), emit_findings=False)

    assert result.ok
    assert observed
    assert observed[0][0] == "audit_chain_cursor_close_failed"
    assert observed[0][2]["table_name"] == "audit_test"


def test_catalog_probe_unexpected_failure_is_observable() -> None:
    class CatalogFailureConnection:
        def execute(self, sql, params=()):
            del params
            if "information_schema.tables" in str(sql):
                raise RuntimeError("catalog probe failed")
            raise AssertionError("unexpected fallback after catalog failure")

    with pytest.raises(RuntimeError, match="catalog probe failed"):
        verifier._existing_tables(CatalogFailureConnection())


def test_cli_row_mapping_unexpected_failure_is_observable() -> None:
    class BrokenMappingRow:
        def keys(self):
            raise RuntimeError("cli row keys failed")

    with pytest.raises(RuntimeError, match="cli row keys failed"):
        cli._row_dict(BrokenMappingRow(), ["id"])


def test_verifier_row_mapping_unexpected_failure_is_observable() -> None:
    class BrokenMappingRow:
        def keys(self):
            raise RuntimeError("verifier row keys failed")

    with pytest.raises(RuntimeError, match="verifier row keys failed"):
        verifier._row_dict(BrokenMappingRow(), ["id"])
