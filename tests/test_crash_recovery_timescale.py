import sqlite3
import importlib
import json

from engine.runtime import crash_recovery
from engine.runtime import storage_pg
from engine.runtime.storage_pg import _normalize_sql, _pg_index_lookup


class _Rows:
    def __init__(self, rows=()):
        self._rows = list(rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _HypertableConnection:
    raw = object()

    def __init__(self):
        self.statements = []
        self.commits = 0

    def executescript(self, script):
        self.statements.append(str(script))
        return _Rows()

    def execute(self, sql, params=None):
        del params
        self.statements.append(str(sql))
        if "timescaledb_information.hypertables" in str(sql):
            return _Rows([(1,)])
        return _Rows()

    def commit(self):
        self.commits += 1


def test_crash_recovery_audit_boot_uses_nonunique_replay_index_on_hypertable():
    con = _HypertableConnection()

    crash_recovery._ensure_tables(con)

    combined = "\n".join(con.statements)
    assert "uq_crash_recovery_audit_replay_key" not in combined
    assert "idx_crash_recovery_audit_replay_key" in combined
    assert con.commits == 1


def test_crash_recovery_audit_event_dedupes_replay_key(monkeypatch):
    monkeypatch.setattr(crash_recovery, "append_event", lambda **_: None)
    con = sqlite3.connect(":memory:")
    crash_recovery._ensure_tables(con)

    inserted = crash_recovery._audit_event(
        con,
        event_type="restore_open_order",
        replay_key="restore:test",
        detail={"ok": True},
    )
    duplicate = crash_recovery._audit_event(
        con,
        event_type="restore_open_order",
        replay_key="restore:test",
        detail={"ok": True},
    )

    count = con.execute("SELECT COUNT(*) FROM crash_recovery_audit").fetchone()[0]
    assert inserted is True
    assert duplicate is False
    assert count == 1


def test_update_universe_imports_without_removed_storage_schema_helpers():
    module = importlib.import_module("engine.data.jobs.update_universe")

    assert module.JOB_NAME == "update_universe"


def test_update_universe_decay_scores_uses_portable_floor_expression(monkeypatch):
    module = importlib.import_module("engine.data.jobs.update_universe")
    monkeypatch.setattr(module, "UNIVERSE_SCORE_DECAY_PER_RUN", 0.25)
    monkeypatch.setattr(module, "UNIVERSE_MIN_SCORE_FLOOR", 0.2)

    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE symbols(symbol TEXT PRIMARY KEY, score REAL, updated_ts_ms INTEGER)")
    con.execute("INSERT INTO symbols(symbol, score, updated_ts_ms) VALUES ('HIGH', 0.8, 0)")
    con.execute("INSERT INTO symbols(symbol, score, updated_ts_ms) VALUES ('LOW', 0.1, 0)")

    module._decay_scores(con)

    rows = dict(con.execute("SELECT symbol, score FROM symbols").fetchall())
    assert round(rows["HIGH"], 6) == 0.6
    assert rows["LOW"] == 0.2


def test_postgres_storage_rewrites_insert_or_ignore_to_do_nothing():
    sql = _normalize_sql("INSERT OR IGNORE INTO symbols(symbol, status) VALUES (?, ?)")

    assert sql.startswith("INSERT INTO symbols")
    assert "VALUES (%s, %s)" in sql
    assert sql.endswith("ON CONFLICT DO NOTHING")


class _RawIndexLookup:
    def __init__(self):
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((str(sql), params))
        return _Rows([("idx_alerts_ts",), ("idx_prices_symbol_ts",)])


class _IndexLookupConnection:
    def __init__(self):
        self.raw = _RawIndexLookup()


def test_postgres_sqlite_master_index_lookup_without_filter_lists_indexes():
    con = _IndexLookupConnection()

    rows = _pg_index_lookup(con, params=None)

    assert rows == [("idx_alerts_ts",), ("idx_prices_symbol_ts",)]
    sql, params = con.raw.calls[0]
    assert "pg_indexes" in sql
    assert "indexname = ANY" not in sql
    assert params is None


class _RecordingConnection:
    def __init__(self):
        self.statements = []
        self.commits = 0

    def execute(self, sql, params=None):
        del params
        self.statements.append(str(sql))
        return _Rows()

    def commit(self):
        self.commits += 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_postgres_init_ensures_alert_prediction_index(monkeypatch):
    con = _RecordingConnection()
    monkeypatch.setattr(storage_pg, "connection", lambda readonly=False: con)
    monkeypatch.setattr(storage_pg, "_compat_table_exists", lambda _con, table: table == "alerts")

    storage_pg._ensure_alert_prediction_schema()

    combined = "\n".join(con.statements)
    assert "ADD COLUMN IF NOT EXISTS prediction_id" in combined
    assert "idx_alerts_prediction_id" in combined
    assert "ON alerts(prediction_id, ts_ms DESC)" in combined
    assert con.commits == 1


def test_health_portfolio_runtime_snapshot_uses_supplied_readonly_connection(monkeypatch):
    from engine.runtime import health

    sentinel_con = object()
    calls = []

    def _fake_risk_state_value(con, key, default=""):
        calls.append((con, key, default))
        return json.dumps(
            {
                "updated_ts_ms": 123456,
                "degraded": False,
                "degraded_reasons": [],
            }
        )

    monkeypatch.setattr(health, "_risk_state_value_readonly", _fake_risk_state_value)

    snapshot = health._portfolio_runtime_snapshot(con=sentinel_con)

    assert calls == [(sentinel_con, "portfolio_runtime_health", "")]
    assert snapshot["ok"] is True
    assert snapshot["available"] is True


def test_execution_degraded_snapshot_accepts_readonly_risk_state_getter(monkeypatch):
    from engine.runtime import gates

    def _unexpected_global_get_state(*_args, **_kwargs):
        raise AssertionError("global risk_state.get_state should not be used")

    monkeypatch.setattr(gates, "_get_risk_state", _unexpected_global_get_state)

    snapshot = gates.get_execution_degraded_snapshot(
        risk_state_getter=lambda key, default="": json.dumps(
            {
                "degraded": True,
                "degraded_reasons": [
                    {
                        "code": "PORTFOLIO_RISK_GATE_FAILED",
                        "detail": "unit_test",
                    }
                ],
            }
        )
        if key == "portfolio_runtime_health"
        else default,
    )

    assert snapshot["active"] is True
    assert snapshot["severity"] == "CRITICAL"
    assert snapshot["reason"] == "portfolio_runtime_critical_degraded"
    assert "PORTFOLIO_RISK_GATE_FAILED" in snapshot["reason_codes"]
