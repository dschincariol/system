"""Characterization tests for the storage_sqlite decomposition facade."""

from __future__ import annotations

import importlib
import inspect
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_storage_sqlite(monkeypatch, tmp_path):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "storage_sqlite_contract.db"))
    monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("TS_TESTING", "1")
    monkeypatch.setenv("TIMESCALE_ENABLED", "0")
    monkeypatch.setenv("SQLITE_LIVENESS_DB_ENABLED", "0")
    monkeypatch.setenv("SQLITE_LIVENESS_QUEUE_ENABLED", "0")
    return importlib.reload(importlib.import_module("engine.runtime.storage_sqlite"))


def test_storage_sqlite_facade_public_and_helper_signatures(monkeypatch, tmp_path):
    storage_sqlite = _reload_storage_sqlite(monkeypatch, tmp_path)

    expected_signatures = {
        "connect": "(readonly: 'bool' = False, **kwargs: 'Any') -> 'StorageConnection'",
        "connect_rw_direct": "(**kwargs: 'Any') -> 'StorageConnection'",
        "init_db": "(schema: 'str | None' = None)",
        "run_write_txn": "(fn: 'Callable[[StorageConnection], Any]', *, attempts: 'int' = 3, table: 'str | None' = None, operation: 'str | None' = None, direct: 'bool' = False, maintenance: 'bool' = True, timeout_s: 'float | None' = None, busy_timeout_ms: 'int | None' = None, **kwargs: 'Any') -> 'Any'",
        "close_pooled_connections": "() -> 'None'",
        "get_db_validation_snapshot": "(*, include_quick_check: 'bool' = True, strict: 'bool' = False) -> 'dict[str, Any]'",
        "_env_truthy": "(value: 'Any') -> 'bool'",
        "_adapt_json": "(value: 'Any') -> 'str'",
        "_is_read_statement": "(sql: 'str') -> 'bool'",
        "_is_auto_write_statement": "(sql: 'str') -> 'bool'",
        "_normalized_sql_signature": "(sql: 'str') -> 'str'",
        "_normalize_param": "(value: 'Any') -> 'Any'",
        "_normalize_params": "(params: 'Any') -> 'Any'",
    }

    for name, expected in expected_signatures.items():
        assert hasattr(storage_sqlite, name), name
        assert str(inspect.signature(getattr(storage_sqlite, name))) == expected


def test_storage_sqlite_pure_normalization_helpers_lock_current_behavior(
    monkeypatch,
    tmp_path,
):
    storage_sqlite = _reload_storage_sqlite(monkeypatch, tmp_path)

    truthy_cases = ["1", "true", "TRUE", " yes ", "on", 1]
    falsey_cases = ["", "0", "false", "off", "no", None, 0]
    assert [storage_sqlite._env_truthy(value) for value in truthy_cases] == [
        True,
        True,
        True,
        True,
        True,
        True,
    ]
    assert [storage_sqlite._env_truthy(value) for value in falsey_cases] == [
        False,
        False,
        False,
        False,
        False,
        False,
        False,
    ]

    assert storage_sqlite._adapt_json({"b": 2, "a": 1}) == '{"a":1,"b":2}'
    assert storage_sqlite._adapt_json(("x", 1)) == '["x",1]'
    assert storage_sqlite._adapt_json({"path": Path("a/b")}) == '{"path":"a/b"}'

    assert storage_sqlite._is_read_statement(" SELECT 1") is True
    assert (
        storage_sqlite._is_read_statement("\nWITH cte AS (SELECT 1) SELECT * FROM cte")
        is True
    )
    assert storage_sqlite._is_read_statement("PRAGMA table_info(prices)") is True
    assert storage_sqlite._is_read_statement("EXPLAIN SELECT 1") is False
    assert (
        storage_sqlite._is_read_statement("INSERT INTO prices(symbol) VALUES (?)")
        is False
    )

    assert (
        storage_sqlite._is_auto_write_statement(
            " INSERT INTO prices(symbol) VALUES (?)"
        )
        is True
    )
    assert storage_sqlite._is_auto_write_statement("update prices set price=?") is True
    assert storage_sqlite._is_auto_write_statement("DELETE FROM prices") is True
    assert (
        storage_sqlite._is_auto_write_statement(
            "replace into prices(symbol) values (?)"
        )
        is True
    )
    assert (
        storage_sqlite._is_auto_write_statement("CREATE TABLE x(id INTEGER)") is False
    )
    assert storage_sqlite._is_auto_write_statement("BEGIN IMMEDIATE") is False

    assert (
        storage_sqlite._normalized_sql_signature(
            " select  *\nfrom prices where symbol = ? "
        )
        == "SELECT*FROMPRICESWHERESYMBOL=?"
    )

    assert storage_sqlite._normalize_param({"b": 2, "a": 1}) == '{"a":1,"b":2}'
    assert storage_sqlite._normalize_param(["x", 1]) == '["x",1]'
    assert storage_sqlite._normalize_param(("x", 1)) == '["x",1]'
    assert storage_sqlite._normalize_param(memoryview(b"abc")) == b"abc"
    assert storage_sqlite._normalize_param("unchanged") == "unchanged"

    assert storage_sqlite._normalize_params(None) is None
    assert storage_sqlite._normalize_params(
        {"k": {"b": 2, "a": 1}, 9: memoryview(b"x")}
    ) == {
        "k": '{"a":1,"b":2}',
        "9": b"x",
    }
    assert storage_sqlite._normalize_params([{"b": 2, "a": 1}, memoryview(b"x")]) == (
        '{"a":1,"b":2}',
        b"x",
    )
    assert storage_sqlite._normalize_params("scalar") == "scalar"


def test_storage_sqlite_facade_helpers_delegate_to_extracted_module(
    monkeypatch,
    tmp_path,
):
    storage_sqlite = _reload_storage_sqlite(monkeypatch, tmp_path)
    normalization = storage_sqlite._sqlite_normalization

    calls = []

    def _record(name, value):
        calls.append(name)
        return value

    monkeypatch.setattr(
        normalization, "env_truthy", lambda value: _record("env_truthy", "truthy")
    )
    monkeypatch.setattr(
        normalization, "adapt_json", lambda value: _record("adapt_json", "json")
    )
    monkeypatch.setattr(
        normalization,
        "is_read_statement",
        lambda sql: _record("is_read_statement", "read"),
    )
    monkeypatch.setattr(
        normalization,
        "is_auto_write_statement",
        lambda sql: _record("is_auto_write_statement", "write"),
    )
    monkeypatch.setattr(
        normalization,
        "normalized_sql_signature",
        lambda sql: _record("normalized_sql_signature", "sig"),
    )
    monkeypatch.setattr(
        normalization,
        "normalize_param",
        lambda value: _record("normalize_param", "param"),
    )
    monkeypatch.setattr(
        normalization,
        "normalize_params",
        lambda params: _record("normalize_params", "params"),
    )

    assert storage_sqlite._env_truthy("1") == "truthy"
    assert storage_sqlite._adapt_json({"a": 1}) == "json"
    assert storage_sqlite._is_read_statement("SELECT 1") == "read"
    assert (
        storage_sqlite._is_auto_write_statement("INSERT INTO x VALUES (1)") == "write"
    )
    assert storage_sqlite._normalized_sql_signature("SELECT 1") == "sig"
    assert storage_sqlite._normalize_param({"a": 1}) == "param"
    assert storage_sqlite._normalize_params([{"a": 1}]) == "params"
    assert calls == [
        "env_truthy",
        "adapt_json",
        "is_read_statement",
        "is_auto_write_statement",
        "normalized_sql_signature",
        "normalize_param",
        "normalize_params",
    ]
