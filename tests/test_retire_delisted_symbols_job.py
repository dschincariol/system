from __future__ import annotations

import importlib
import json
import sqlite3

import pytest

DAY_MS = 24 * 60 * 60 * 1000


class _ConnectionProxy:
    def __init__(self, con: sqlite3.Connection) -> None:
        self._con = con
        self.closed = False

    def __getattr__(self, name: str):
        return getattr(self._con, name)

    def close(self) -> None:
        self.closed = True


def _con() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.executescript(
        """
        CREATE TABLE symbols(
          symbol TEXT PRIMARY KEY,
          asset_class TEXT NOT NULL DEFAULT 'UNKNOWN',
          status TEXT NOT NULL DEFAULT 'WATCH',
          score REAL NOT NULL DEFAULT 0.0,
          last_seen_event_ts_ms INTEGER,
          last_traded_ts_ms INTEGER,
          meta_json TEXT,
          created_ts_ms INTEGER NOT NULL,
          updated_ts_ms INTEGER NOT NULL
        );
        CREATE TABLE prices(
          ts_ms INTEGER,
          symbol TEXT,
          price REAL,
          px REAL,
          source TEXT
        );
        CREATE TABLE price_quotes(
          ts_ms INTEGER,
          symbol TEXT,
          bid REAL,
          ask REAL,
          source TEXT
        );
        CREATE TABLE universe_audit(
          ts_ms INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          status_before TEXT,
          status_after TEXT,
          include INTEGER NOT NULL,
          score REAL,
          reasons_json TEXT,
          features_json TEXT,
          PRIMARY KEY(ts_ms, symbol)
        );
        """
    )
    return con


def _seed_stale_spy(con: sqlite3.Connection) -> tuple[int, int]:
    now_ms = 100 * DAY_MS
    stale_ts_ms = now_ms - (60 * DAY_MS)
    con.execute(
        """
        INSERT INTO symbols(
          symbol, asset_class, status, score, last_seen_event_ts_ms,
          last_traded_ts_ms, meta_json, created_ts_ms, updated_ts_ms
        )
        VALUES (?,?,?,?,?,?,?,?,?)
        """,
        ("SPY", "EQUITY", "ACTIVE", 1.0, None, None, "{}", 1000, 1000),
    )
    con.execute(
        "INSERT INTO prices(ts_ms, symbol, price, px, source) VALUES (?,?,?,?,?)",
        (stale_ts_ms, "SPY", 10.0, 10.0, "test"),
    )
    return now_ms, stale_ts_ms


def test_job_registered_and_pipeline_ordered_after_universe_before_pit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PIT_UNIVERSE_BACKFILL_ENABLED", "1")
    registry = importlib.reload(importlib.import_module("engine.runtime.job_registry"))
    job = importlib.reload(importlib.import_module("engine.data.jobs.retire_delisted_symbols"))

    assert job.JOB_NAME == "retire_delisted_symbols"
    assert registry.ALLOWED_JOBS["retire_delisted_symbols"][0] == "engine/data/jobs/retire_delisted_symbols.py"
    assert registry.ALLOWED_JOBS["retire_delisted_symbols"][1] == "oneshot"
    assert registry.ALLOWED_JOBS["retire_delisted_symbols"][3]["pipeline_stage"] == "universe_lifecycle"
    assert registry.ALLOWED_JOBS["retire_delisted_symbols"][3]["requires_secret_any"] == [
        "POLYGON_API_KEY",
        "FMP_API_KEY",
    ]
    assert registry.PIPELINE_ORDER.index("update_universe") < registry.PIPELINE_ORDER.index("retire_delisted_symbols")
    assert registry.PIPELINE_ORDER.index("retire_delisted_symbols") < registry.PIPELINE_ORDER.index("backfill_universe_pit")


def test_job_default_off_exits_without_lock_or_connection(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setenv("UNIVERSE_LIFECYCLE_ENABLED", "0")
    job = importlib.reload(importlib.import_module("engine.data.jobs.retire_delisted_symbols"))
    monkeypatch.setattr(job, "init_db", lambda: None)
    monkeypatch.setattr(job, "acquire_job_lock", lambda *_args, **_kwargs: pytest.fail("lock should not be acquired"))
    monkeypatch.setattr(job, "connect", lambda: pytest.fail("database should not be opened"))

    assert job.main() == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary == {"enabled": False, "job": "retire_delisted_symbols", "ok": True}


def test_job_runs_lifecycle_pass_and_heartbeat_summary(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setenv("UNIVERSE_LIFECYCLE_ENABLED", "1")
    monkeypatch.setenv("UNIVERSE_LIFECYCLE_REFERENCE_ENABLED", "0")
    con = _con()
    proxy = _ConnectionProxy(con)
    now_ms, stale_ts_ms = _seed_stale_spy(con)
    heartbeats: list[str] = []
    job = importlib.reload(importlib.import_module("engine.data.jobs.retire_delisted_symbols"))
    lifecycle = importlib.reload(importlib.import_module("engine.data.universe_lifecycle"))

    def _run_with_fixed_time(connection, fetch_reference=None):
        return lifecycle.run_lifecycle_once(connection, now_ms=now_ms, fetch_reference=fetch_reference)

    monkeypatch.setattr(job, "init_db", lambda: None)
    monkeypatch.setattr(job, "acquire_job_lock", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(job, "release_job_lock", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(job, "connect", lambda: proxy)
    monkeypatch.setattr(job, "put_job_heartbeat", lambda *_args, extra_json=None, **_kwargs: heartbeats.append(extra_json or ""))
    monkeypatch.setattr(job, "run_lifecycle_once", _run_with_fixed_time)

    assert job.main() == 0
    summary = json.loads(capsys.readouterr().out)

    assert summary["job"] == "retire_delisted_symbols"
    assert summary["retired"] == 1
    assert summary["reason_counts"] == {"stale_inactive": 1}
    assert summary["reference_enabled"] is False
    assert heartbeats and json.loads(heartbeats[-1])["retired"] == 1
    assert proxy.closed is True
    assert con.execute("SELECT status FROM symbols WHERE symbol='SPY'").fetchone()[0] == "DISABLED"
    assert con.execute("SELECT last_traded_ts_ms FROM symbols WHERE symbol='SPY'").fetchone()[0] == stale_ts_ms


def test_reference_enabled_missing_credentials_blocks_reference_layer_without_secret_leak(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("UNIVERSE_LIFECYCLE_ENABLED", "1")
    monkeypatch.setenv("UNIVERSE_LIFECYCLE_REFERENCE_ENABLED", "1")
    canary = "canary-secret-not-for-output"
    con = _con()
    proxy = _ConnectionProxy(con)
    now_ms, _stale_ts_ms = _seed_stale_spy(con)
    job = importlib.reload(importlib.import_module("engine.data.jobs.retire_delisted_symbols"))
    lifecycle = importlib.reload(importlib.import_module("engine.data.universe_lifecycle"))

    def _run_with_fixed_time(connection, fetch_reference=None):
        return lifecycle.run_lifecycle_once(connection, now_ms=now_ms, fetch_reference=fetch_reference)

    monkeypatch.setattr(job, "init_db", lambda: None)
    monkeypatch.setattr(job, "acquire_job_lock", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(job, "release_job_lock", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(job, "connect", lambda: proxy)
    monkeypatch.setattr(job, "put_job_heartbeat", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(job, "run_lifecycle_once", _run_with_fixed_time)
    monkeypatch.setattr(job, "get_data_credential", lambda _name: "")
    monkeypatch.setenv("POLYGON_API_KEY", canary)
    monkeypatch.setenv("FMP_API_KEY", canary)

    assert job.main() == 0
    summary = json.loads(capsys.readouterr().out)
    serialized = json.dumps(
        {
            "summary": summary,
            "audit": con.execute("SELECT reasons_json, features_json FROM universe_audit").fetchall(),
            "meta": con.execute("SELECT meta_json FROM symbols").fetchall(),
        },
        sort_keys=True,
        default=str,
    )

    assert summary["reference_enabled"] is True
    assert summary["reference_fetcher_configured"] is False
    assert summary["reference_blocked"] is True
    assert summary["reference_blocker"] == "missing_polygon_or_fmp_api_key"
    assert canary not in serialized
