import importlib
import json
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


AUDITED_SAFE_ROUTE_PATHS = [
    "/api/system/health",
    "/api/health",
    "/api/status",
    "/api/readiness",
    "/api/operator/snapshot",
    "/api/system/config",
    "/api/server/status",
    "/api/jobs",
    "/api/jobs/log",
    "/api/jobs/history",
    "/api/db/health",
    "/api/operator/db_schema",
    "/api/data_sources",
    "/api/data_sources/logs",
    "/api/market/candles",
]

EXPECTED_RESPONSE_KEYS = {
    "/api/system/health": {"ok", "ts_ms", "db"},
    "/api/health": {"ok", "ts_ms", "db"},
    "/api/status": {"ok", "status", "health", "ingestion", "services", "readiness"},
    "/api/readiness": {"ok", "readiness", "production_validation", "health_ok", "graph_valid", "system_state"},
    "/api/operator/snapshot": {"ok", "snapshot_schema", "production_validation", "diagnostics", "system_health"},
    "/api/system/config": {"ok", "status", "health", "config"},
    "/api/server/status": {"ok", "ts_ms", "uptime_s", "host", "port"},
    "/api/jobs": {"ok", "ts_ms", "jobs", "pipeline_order", "allowed"},
    "/api/jobs/log": {"ok", "job"},
    "/api/jobs/history": {"ok", "job"},
    "/api/db/health": {"ok", "ts_ms", "runtime_health", "system_snapshot"},
    "/api/operator/db_schema": {"ok", "ts_ms", "missing_tables", "missing_cols"},
    "/api/data_sources": {"ok", "ts_ms", "sources", "templates", "runtime", "auth", "desired_ingestion_jobs"},
    "/api/data_sources/logs": {"ok", "source_key", "logs"},
    "/api/market/candles": {"ok", "symbol", "tf", "candles", "meta"},
}

SAFE_LOCAL_HANDLER_NAMES = {
    "api_get_system_state",
    "api_get_health",
    "api_get_status",
    "api_get_readiness",
    "api_get_trading_readiness",
    "api_get_support_snapshot",
    "api_get_runtime_config",
    "api_get_supervisor_status",
    "api_get_ingestion_status",
    "api_get_server_status",
    "api_get_jobs",
    "api_get_job_log",
    "api_get_job_history",
    "api_get_db_health",
    "api_get_schema_audit",
    "api_get_data_sources",
    "api_get_data_source_logs",
    "api_get_market_candles",
}


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class _FakeJobs:
    def __init__(self, default_job_name: str) -> None:
        self.default_job_name = str(default_job_name)

    def list_jobs(self):
        return []

    def get_job_log(self, name: str, tail: int = 200):
        return {"ok": True, "job": str(name), "tail": int(tail), "data": []}

    def get_job_history(self, name: str, limit: int = 200):
        return {"ok": True, "job": str(name), "limit": int(limit), "data": []}


class _FakeSupervisor:
    def status(self):
        return {"ok": True, "state": "idle", "jobs": []}


def _connect_sqlite(db_path: Path, *, readonly: bool = False):
    del readonly
    con = sqlite3.connect(str(db_path), timeout=1.0, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def _ensure_route_contract_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with _connect_sqlite(db_path) as con:
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS runtime_meta (
              key TEXT PRIMARY KEY,
              value TEXT,
              updated_ts_ms INTEGER
            );

            CREATE TABLE IF NOT EXISTS runtime_metrics (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts_ms INTEGER NOT NULL,
              metric TEXT NOT NULL,
              value_num REAL,
              value_text TEXT,
              tags_json TEXT
            );

            CREATE TABLE IF NOT EXISTS event_log (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts_ms INTEGER NOT NULL,
              event_type TEXT NOT NULL,
              event_source TEXT,
              event_version INTEGER NOT NULL DEFAULT 1,
              entity_type TEXT,
              entity_id TEXT,
              correlation_id TEXT,
              payload_json TEXT
            );

            CREATE TABLE IF NOT EXISTS job_locks (
              job_name TEXT PRIMARY KEY,
              owner TEXT,
              pid INTEGER,
              acquired_ts_ms INTEGER,
              heartbeat_ts_ms INTEGER
            );

            CREATE TABLE IF NOT EXISTS job_history (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts_ms INTEGER NOT NULL,
              job_name TEXT NOT NULL,
              event TEXT NOT NULL,
              exit_code INTEGER
            );
            """
        )
        con.commit()


def _install_isolated_sqlite_storage(monkeypatch: pytest.MonkeyPatch, db_path: Path):
    storage = importlib.import_module("engine.runtime.storage")
    storage_pg = importlib.import_module("engine.runtime.storage_pg")
    storage_pool = importlib.import_module("engine.runtime.storage_pool")
    db_guard = importlib.import_module("engine.runtime.db_guard")
    postgres_attempts: list[dict[str, str]] = []

    def connect(readonly: bool = False, **_kwargs):
        return _connect_sqlite(db_path, readonly=bool(readonly))

    def connection(readonly: bool = False):
        return connect(readonly=readonly)

    def init_db(schema: str | None = None):
        del schema
        _ensure_route_contract_db(db_path)
        return []

    def run_write_txn(fn, attempts: int = 1, **_kwargs):
        last_exc = None
        for _ in range(max(1, int(attempts or 1))):
            con = connect(readonly=False)
            try:
                out = fn(con)
                con.commit()
                return out
            except sqlite3.OperationalError as exc:
                last_exc = exc
                con.rollback()
            except Exception:
                con.rollback()
                raise
            finally:
                con.close()
        raise last_exc or RuntimeError("sqlite_test_write_failed")

    def get_db_validation_snapshot(*, include_quick_check: bool = True, strict: bool = False):
        del include_quick_check, strict
        return {
            "ok": True,
            "initialized": True,
            "exists": db_path.exists(),
            "db_path": str(db_path),
            "storage": "sqlite-test",
            "missing_tables": [],
            "missing_cols": {},
            "owned_schema_ok": True,
        }

    def get_db_debug_snapshot(*, include_quick_check: bool = True):
        return {
            "ok": True,
            "db_path": str(db_path),
            "db_bytes": int(db_path.stat().st_size) if db_path.exists() else 0,
            "db_validation": get_db_validation_snapshot(include_quick_check=include_quick_check),
            "failure_classification": {"primary_cause": ""},
        }

    def _table_exists(con, table: str) -> bool:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (str(table),),
        ).fetchone()
        return bool(row)

    def _pid_is_running(_pid: int) -> bool:
        return False

    def storage_readiness_snapshot() -> dict:
        return {
            "checked": True,
            "ok": True,
            "status": "ready",
            "storage": "sqlite-test",
            "backend": "sqlite-test",
            "degraded": False,
            "required": False,
            "detail": "isolated_route_contract_storage",
            "error": "",
            "error_type": "",
            "timeout_s": None,
            "ts_ms": int(time.time() * 1000),
        }

    def probe_storage_readiness(*_args, **_kwargs) -> dict:
        return storage_readiness_snapshot()

    def assert_storage_ready(*_args, **_kwargs) -> dict:
        return storage_readiness_snapshot()

    def storage_unavailable_payload(*, endpoint: str = "", error: BaseException | None = None, readiness: dict | None = None) -> dict:
        snapshot = dict(readiness or storage_readiness_snapshot())
        return {
            "ok": False,
            "error": "storage_unavailable",
            "detail": str(error or snapshot.get("detail") or "runtime_storage_unavailable"),
            "endpoint": str(endpoint or ""),
            "storage": snapshot,
            "meta": {"status": 503, "retryable": True, "ts_ms": int(time.time() * 1000)},
        }

    def _blocked_postgres_acquire(*_args, **_kwargs):
        postgres_attempts.append({"path": str(db_path)})
        raise AssertionError("route-contract tests must use isolated sqlite storage, not postgres")

    monkeypatch.setattr(storage, "DB_PATH", db_path, raising=False)
    monkeypatch.setattr(storage, "PG_LIVENESS_DB_PATH", db_path, raising=False)
    monkeypatch.setattr(storage, "_route_contract_postgres_attempts", postgres_attempts, raising=False)
    monkeypatch.setattr(storage, "_pg_init_db", init_db, raising=False)
    monkeypatch.setattr(storage, "connect", connect, raising=False)
    monkeypatch.setattr(storage, "connect_ro", lambda: connect(readonly=True), raising=False)
    monkeypatch.setattr(storage, "connect_ro_direct", lambda **kwargs: connect(readonly=True, **kwargs), raising=False)
    monkeypatch.setattr(storage, "connect_rw_direct", lambda **kwargs: connect(readonly=False, **kwargs), raising=False)
    monkeypatch.setattr(storage, "connection", connection, raising=False)
    monkeypatch.setattr(storage, "init_db", init_db, raising=False)
    monkeypatch.setattr(storage, "init_rl_portfolio_tables", lambda con=None: None, raising=False)
    monkeypatch.setattr(storage, "run_write_txn", run_write_txn, raising=False)
    monkeypatch.setattr(storage, "close_pooled_connections", lambda: None, raising=False)
    monkeypatch.setattr(storage, "shutdown_timeseries_storage", lambda timeout_s=None: {"ok": True}, raising=False)
    monkeypatch.setattr(storage, "get_db_validation_snapshot", get_db_validation_snapshot, raising=False)
    monkeypatch.setattr(storage, "get_db_debug_snapshot", get_db_debug_snapshot, raising=False)
    monkeypatch.setattr(storage, "_table_exists", _table_exists, raising=False)
    monkeypatch.setattr(storage, "_pid_is_running", _pid_is_running, raising=False)

    monkeypatch.setattr(storage_pool, "storage_readiness_snapshot", storage_readiness_snapshot, raising=False)
    monkeypatch.setattr(storage_pool, "probe_storage_readiness", probe_storage_readiness, raising=False)
    monkeypatch.setattr(storage_pool, "assert_storage_ready", assert_storage_ready, raising=False)
    monkeypatch.setattr(storage_pool, "storage_unavailable_payload", storage_unavailable_payload, raising=False)
    monkeypatch.setattr(storage_pool, "acquire", _blocked_postgres_acquire, raising=False)

    monkeypatch.setattr(storage_pg, "DB_PATH", db_path, raising=False)
    monkeypatch.setattr(storage_pg, "PG_LIVENESS_DB_PATH", db_path, raising=False)
    monkeypatch.setattr(storage_pg, "connect", _blocked_postgres_acquire, raising=False)
    monkeypatch.setattr(storage_pg, "connect_ro", _blocked_postgres_acquire, raising=False)
    monkeypatch.setattr(storage_pg, "connect_ro_direct", _blocked_postgres_acquire, raising=False)
    monkeypatch.setattr(storage_pg, "connect_rw_direct", _blocked_postgres_acquire, raising=False)
    monkeypatch.setattr(storage_pg, "connection", _blocked_postgres_acquire, raising=False)
    monkeypatch.setattr(storage_pg, "init_db", init_db, raising=False)
    monkeypatch.setattr(storage_pg, "init_rl_portfolio_tables", lambda con=None: None, raising=False)
    monkeypatch.setattr(storage_pg, "run_write_txn", run_write_txn, raising=False)
    monkeypatch.setattr(storage_pg, "close_pooled_connections", lambda: None, raising=False)
    monkeypatch.setattr(storage_pg, "shutdown_timeseries_storage", lambda timeout_s=None: {"ok": True}, raising=False)
    monkeypatch.setattr(storage_pg, "get_db_validation_snapshot", get_db_validation_snapshot, raising=False)
    monkeypatch.setattr(storage_pg, "get_db_debug_snapshot", get_db_debug_snapshot, raising=False)
    monkeypatch.setattr(storage_pg, "storage_readiness_snapshot", storage_readiness_snapshot, raising=False)
    monkeypatch.setattr(storage_pg, "probe_storage_readiness", probe_storage_readiness, raising=False)
    monkeypatch.setattr(storage_pg, "assert_storage_ready", assert_storage_ready, raising=False)
    monkeypatch.setattr(storage_pg, "storage_unavailable_payload", storage_unavailable_payload, raising=False)
    monkeypatch.setattr(storage_pg, "acquire", _blocked_postgres_acquire, raising=False)

    monkeypatch.setattr(db_guard, "resolve_db_path", lambda: db_path, raising=False)
    monkeypatch.setattr(db_guard, "_resolve_db_path", lambda: db_path, raising=False)
    monkeypatch.setattr(
        db_guard,
        "ensure_db_ok",
        lambda include_quick_check=True: {
            "ok": True,
            "db_path": str(db_path),
            "action": "none",
            "error": None,
            "storage": "sqlite-test",
        },
        raising=False,
    )

    for module_name in (
        "engine.runtime.metrics_store",
        "engine.runtime.health",
        "engine.runtime.runtime_meta",
        "engine.runtime.telemetry_read_router",
        "engine.runtime.price_read_router",
        "engine.runtime.data_source_log_store",
        "services.data_source_manager",
        "engine.api.api_jobs",
        "engine.api.api_system",
        "engine.api.api_market",
        "dashboard_server",
        "engine.model_registry",
    ):
        module = sys.modules.get(module_name)
        if module is None:
            continue
        monkeypatch.setattr(module, "DB_PATH", db_path, raising=False)
        monkeypatch.setattr(module, "PG_LIVENESS_DB_PATH", db_path, raising=False)
        monkeypatch.setattr(module, "_db_connect", connect, raising=False)
        monkeypatch.setattr(module, "_connect", connect, raising=False)
        monkeypatch.setattr(module, "_connect_ro", lambda **kwargs: connect(readonly=True, **kwargs), raising=False)
        monkeypatch.setattr(module, "connect_ro", lambda **kwargs: connect(readonly=True, **kwargs), raising=False)
        monkeypatch.setattr(module, "connect_ro_direct", lambda **kwargs: connect(readonly=True, **kwargs), raising=False)
        monkeypatch.setattr(module, "run_write_txn", run_write_txn, raising=False)
        monkeypatch.setattr(module, "_init_db", init_db, raising=False)
        monkeypatch.setattr(module, "get_db_validation_snapshot", get_db_validation_snapshot, raising=False)
        monkeypatch.setattr(module, "get_db_debug_snapshot", get_db_debug_snapshot, raising=False)
    return storage


def _ensure_market_quote_fixture(db_path: Path) -> None:
    now_ms = int(time.time() * 1000)
    with sqlite3.connect(str(db_path)) as con:
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS price_quotes (
              ts_ms INTEGER NOT NULL,
              symbol TEXT NOT NULL,
              last REAL,
              bid REAL,
              ask REAL,
              spread REAL,
              volume REAL,
              source TEXT,
              PRIMARY KEY(symbol, ts_ms)
            );
            CREATE INDEX IF NOT EXISTS idx_price_quotes_symbol_ts
              ON price_quotes(symbol, ts_ms);
            """
        )
        con.execute(
            """
            INSERT OR REPLACE INTO price_quotes(ts_ms, symbol, last, volume, source)
            VALUES (?, ?, ?, ?, ?)
            """,
            (now_ms - 60_000, "SPY", 100.0, 10.0, "test"),
        )
        con.execute(
            """
            INSERT OR REPLACE INTO price_quotes(ts_ms, symbol, last, volume, source)
            VALUES (?, ?, ?, ?, ?)
            """,
            (now_ms, "SPY", 101.0, 20.0, "test"),
        )
        con.commit()


@pytest.fixture()
def route_runtime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "dashboard_route_contracts.db"

    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("TIMESCALE_ENABLED", "0")
    monkeypatch.delenv("TIMESCALE_DSN", raising=False)
    monkeypatch.delenv("TIMESCALE_URL", raising=False)
    monkeypatch.delenv("TIMESCALE_DATABASE_URL", raising=False)
    monkeypatch.setenv("FEATURE_STORE_ENABLED", "0")
    monkeypatch.setenv("FEATURE_STORE_INIT_ON_STARTUP", "0")
    monkeypatch.setenv("PRICE_READ_BACKEND", "sqlite")
    monkeypatch.setenv("PRICE_READ_REQUIRE_VALIDATION", "0")
    monkeypatch.setenv("TELEMETRY_READ_BACKEND", "sqlite")
    monkeypatch.setenv("RUNTIME_METRICS_BUFFER_ENABLED", "0")
    monkeypatch.setenv("RUNTIME_META_BEST_EFFORT_BUFFER_ENABLED", "0")
    monkeypatch.setenv("EVENT_LOG_BUFFER_ENABLED", "0")
    monkeypatch.setenv("TRADING_FAILURE_DIAGNOSTICS_PERSIST", "0")
    monkeypatch.delenv("DASHBOARD_API_TOKEN", raising=False)

    storage = _install_isolated_sqlite_storage(monkeypatch, db_path)
    storage.init_db()
    _ensure_market_quote_fixture(Path(storage.DB_PATH))

    (
        _telemetry_read_router,
        _price_read_router,
        _metrics_store,
        _metrics,
        _observability,
        _model_registry,
        _model_cache,
        _health,
        _runtime_meta,
        _data_source_log_store,
        data_source_manager,
        _data_source_routes,
        api_jobs,
        _internal_access,
        _api_system,
        _api_market,
        dashboard_server,
    ) = _reload_modules(
        "engine.runtime.telemetry_read_router",
        "engine.runtime.price_read_router",
        "engine.runtime.metrics_store",
        "engine.runtime.metrics",
        "engine.runtime.observability",
        "engine.model_registry",
        "engine.runtime.model_cache",
        "engine.runtime.health",
        "engine.runtime.runtime_meta",
        "engine.runtime.data_source_log_store",
        "services.data_source_manager",
        "routes.data_sources_routes",
        "engine.api.api_jobs",
        "engine.api.internal_access",
        "engine.api.api_system",
        "engine.api.api_market",
        "dashboard_server",
    )

    default_job_name = next(iter(api_jobs.ALLOWED_JOBS.keys()), "process_events")
    fake_jobs = _FakeJobs(default_job_name=default_job_name)
    fake_supervisor = _FakeSupervisor()

    # The packaged kill-switch delegate can block in test mode; use the
    # dashboard's built-in unavailable fallback for deterministic route probes.
    dashboard_server._api_get_kill_switches_impl = None
    dashboard_server.JOBS = fake_jobs
    dashboard_server.SUPERVISOR = fake_supervisor
    dashboard_server.ORCHESTRATOR = None

    ctx = {
        "JOBS": fake_jobs,
        "SUPERVISOR": fake_supervisor,
        "ORCHESTRATOR": None,
        "API_HANDLERS": dashboard_server.API_HANDLERS,
    }

    sources = data_source_manager.get_manager().list_sources()
    default_source_key = str((sources[0] if sources else {}).get("source_key") or "polygon_ws")

    try:
        yield {
            "dashboard_server": dashboard_server,
            "storage": storage,
            "db_path": db_path,
            "ctx": ctx,
            "default_job_name": default_job_name,
            "default_source_key": default_source_key,
        }
    finally:
        pg_attempts = list(getattr(storage, "_route_contract_postgres_attempts", []) or [])
        assert pg_attempts == [], f"route-contract tests attempted postgres storage: {pg_attempts}"
        try:
            storage.close_pooled_connections()
        except Exception:
            pass
        try:
            storage.shutdown_timeseries_storage(timeout_s=0.1)
        except Exception:
            pass


def _route_index(dashboard_server):
    return {
        (str(route.get("method") or ""), str(route.get("path") or "")): dict(route)
        for route in dashboard_server.ROUTE_SPECS
    }


def test_route_runtime_uses_isolated_sqlite_storage(route_runtime):
    db_path = route_runtime["db_path"]
    storage = route_runtime["storage"]

    assert Path(storage.DB_PATH) == db_path
    assert db_path.exists()
    with sqlite3.connect(str(db_path)) as con:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='runtime_meta' LIMIT 1"
        ).fetchone()
    assert row == (1,)


def test_dashboard_kill_switch_snapshot_uses_readonly_helper(route_runtime, monkeypatch: pytest.MonkeyPatch):
    dashboard_server = route_runtime["dashboard_server"]

    monkeypatch.setattr(
        dashboard_server,
        "get_kill_switch_snapshot_readonly",
        lambda: {"state": [{"scope": "global", "key": "*", "enabled": 0}]},
    )
    monkeypatch.setattr(
        dashboard_server,
        "_kill_switch_snapshot_impl",
        lambda: (_ for _ in ()).throw(AssertionError("legacy kill switch snapshot should not run")),
    )

    snapshot = dashboard_server._get_kill_switches_snapshot()

    assert snapshot == {"state": [{"scope": "global", "key": "*", "enabled": 0}]}


def test_dashboard_run_server_delegates_to_api_server(route_runtime, monkeypatch: pytest.MonkeyPatch):
    from engine.api import server as api_server

    dashboard_server = route_runtime["dashboard_server"]
    seen = {}

    def _fake_run_server(*, dashboard_module=None):
        seen["dashboard_module"] = dashboard_module
        return {"ok": True, "delegated": True}

    monkeypatch.setattr(api_server, "run_server", _fake_run_server)

    result = dashboard_server.run_server()

    assert result == {"ok": True, "delegated": True}
    assert seen["dashboard_module"] is dashboard_server


def test_dashboard_route_storage_timeout_returns_structured_503(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    from engine.api import http_transport
    from engine.runtime.storage_pool import StoragePoolTimeout

    monkeypatch.setattr(http_transport, "emit_counter", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(http_transport, "emit_timing", lambda *_args, **_kwargs: None)

    def _storage_blocked(_parsed=None, _ctx=None):
        raise StoragePoolTimeout("couldn't get a connection after 0.05 sec")

    handler_cls = http_transport.build_handler(
        [{"method": "GET", "path": "/api/ui/metrics", "handler": "api_get_ui_metrics"}],
        {"api_get_ui_metrics": _storage_blocked},
        "",
        ctx={"STORAGE_REQUIRED_PATHS": [], "STORAGE_REQUEST_TIMEOUT_S": 0.05},
        static_dir=str(tmp_path),
    )
    httpd = http_transport.run_http_server("127.0.0.1", 0, handler_cls)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{httpd.server_address[1]}/api/ui/metrics"
        with pytest.raises(urllib.error.HTTPError) as raised:
            urllib.request.urlopen(url, timeout=2.0)
        body = raised.value.read().decode("utf-8")
        payload = json.loads(body)
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=2.0)

    assert raised.value.code == 503
    assert payload["ok"] is False
    assert payload["error"] == "storage_unavailable"
    assert payload["meta"]["status"] == 503


def test_dashboard_static_route_does_not_probe_storage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    from engine.api import http_transport
    from engine.runtime import storage_pool

    static_ui = tmp_path / "ui"
    static_ui.mkdir(parents=True)
    (static_ui / "dashboard.html").write_text("<!doctype html><title>Dashboard</title>", encoding="utf-8")

    monkeypatch.setattr(http_transport, "emit_counter", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(http_transport, "emit_timing", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        storage_pool,
        "probe_storage_readiness",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("static route probed storage")),
    )

    handler_cls = http_transport.build_handler(
        [{"method": "GET", "path": "/api/db-backed", "handler": "db_backed"}],
        {"db_backed": lambda *_args, **_kwargs: {"ok": True}},
        "",
        ctx={"STORAGE_REQUIRED_PATHS": ["/api/db-backed"], "STORAGE_REQUEST_TIMEOUT_S": 0.05},
        static_dir=str(tmp_path),
    )
    httpd = http_transport.run_http_server("127.0.0.1", 0, handler_cls)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{httpd.server_address[1]}/ui/dashboard.html"
        with urllib.request.urlopen(url, timeout=2.0) as response:
            body = response.read().decode("utf-8")
            status = response.status
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=2.0)

    assert status == 200
    assert "Dashboard" in body


def test_dashboard_storage_required_route_short_circuits_503(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    from engine.api import http_transport
    from engine.runtime import storage_pool

    monkeypatch.setattr(http_transport, "emit_counter", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(http_transport, "emit_timing", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        storage_pool,
        "probe_storage_readiness",
        lambda *_args, **_kwargs: {
            "checked": True,
            "ok": False,
            "status": "unavailable",
            "storage": "postgres",
            "backend": "postgres",
            "degraded": True,
            "detail": "postgres_readiness_probe_failed",
            "error": "connection refused",
            "required": True,
            "ts_ms": int(time.time() * 1000),
        },
    )

    def _must_not_run(*_args, **_kwargs):
        raise AssertionError("DB-backed handler ran despite unavailable storage")

    handler_cls = http_transport.build_handler(
        [{"method": "GET", "path": "/api/db-backed", "handler": "db_backed"}],
        {"db_backed": _must_not_run},
        "",
        ctx={"STORAGE_REQUIRED_PATHS": ["/api/db-backed"], "STORAGE_REQUEST_TIMEOUT_S": 0.05},
        static_dir=str(tmp_path),
    )
    httpd = http_transport.run_http_server("127.0.0.1", 0, handler_cls)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{httpd.server_address[1]}/api/db-backed"
        with pytest.raises(urllib.error.HTTPError) as raised:
            urllib.request.urlopen(url, timeout=2.0)
        payload = json.loads(raised.value.read().decode("utf-8"))
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=2.0)

    assert raised.value.code == 503
    assert payload["ok"] is False
    assert payload["error"] == "storage_unavailable"
    assert payload["storage"]["status"] == "unavailable"


def test_readiness_reports_storage_unavailable_without_storage_route_precheck(route_runtime, monkeypatch):
    dashboard_server = route_runtime["dashboard_server"]
    (api_system,) = _reload_modules("engine.api.api_system")

    assert "/api/readiness" not in dashboard_server._STORAGE_REQUIRED_ROUTE_PATHS
    assert "/api/operator/readiness" not in dashboard_server._STORAGE_REQUIRED_ROUTE_PATHS

    storage_snapshot = {
        "checked": True,
        "ok": False,
        "status": "unavailable",
        "storage": "postgres",
        "backend": "postgres",
        "degraded": True,
        "detail": "postgres_readiness_probe_failed",
        "error": "connection refused",
        "required": True,
        "ts_ms": int(time.time() * 1000),
    }
    monkeypatch.setattr(
        api_system,
        "_build_readiness_snapshot",
        lambda *_args, **_kwargs: {
            "ts_ms": int(time.time() * 1000),
            "state": "DEGRADED",
            "mode": "safe",
            "execution_mode": "safe",
            "execution_allowed": False,
            "reasons": [],
            "readiness": {"ok": False, "ready": False, "issues": []},
            "production_validation": {
                "status": "failed",
                "safe_to_operate": False,
                "unsafe_to_operate": True,
                "current_degraded_reasons": [],
            },
            "health": {"ok": False},
            "graph": {"ok": True},
        },
    )

    response = api_system.api_get_readiness(
        {},
        {"_boot_diagnostics": lambda: {"storage": dict(storage_snapshot)}},
    )

    assert response["ok"] is False
    assert response["storage_degraded"] is True
    assert response["storage"]["status"] == "unavailable"
    assert "storage_unavailable" in response["reasons"]


def test_liveness_remains_alive_when_storage_unavailable(monkeypatch):
    (api_system,) = _reload_modules("engine.api.api_system")

    monkeypatch.setattr(
        api_system,
        "_cached_health_snapshot",
        lambda *args, **kwargs: {
            "ok": False,
            "db": {"ok": False, "error": "connection refused"},
            "lifecycle": {"state": "DEGRADED", "detail": "runtime_storage_unavailable"},
        },
    )

    response = api_system.api_get_liveness({}, {})

    assert response["ok"] is True
    assert response["alive"] is True
    assert response["status"] == "ALIVE"


def test_dashboard_auto_boot_skips_price_targets_when_isolated_ingestion_enabled(
    route_runtime,
    monkeypatch: pytest.MonkeyPatch,
):
    dashboard_server = route_runtime["dashboard_server"]

    monkeypatch.setenv("AUTO_BOOT_PRICE_TARGET", "poll_prices")
    monkeypatch.setattr(
        dashboard_server,
        "_OPERATOR_PRICE_JOB_CANDIDATES",
        ("stream_prices_polygon_ws", "poll_prices"),
    )
    monkeypatch.setattr(
        dashboard_server,
        "ALLOWED_JOBS",
        {
            "ingestion_runtime": ("engine/runtime/ingestion_runtime.py", "daemon", "runtime"),
            "poll_prices": ("engine/data/poll_prices.py", "daemon", "price_feed"),
            "provider_monitor": ("engine/runtime/provider_monitor.py", "daemon", "support"),
        },
    )
    monkeypatch.setattr(dashboard_server.os.path, "exists", lambda path: True)

    static_targets = dashboard_server._dashboard_auto_boot_static_targets(
        ["ingestion_runtime", "poll_prices", "provider_monitor"],
        ingestion_enabled=True,
    )
    price_candidates = dashboard_server._dashboard_auto_boot_price_candidates(
        ingestion_enabled=True,
    )

    assert static_targets == ["provider_monitor"]
    assert price_candidates == []


def _parsed_for_path(path: str, *, default_job_name: str, default_source_key: str):
    if path == "/api/jobs/log":
        return {"name": default_job_name, "tail": "10"}
    if path == "/api/jobs/history":
        return {"name": default_job_name, "limit": "10"}
    if path == "/api/data_sources/logs":
        return {"source_key": default_source_key, "limit": "10"}
    if path == "/api/market/candles":
        return {"symbol": "SPY", "tf": "1m", "limit": "10"}
    if path == "/api/operator/snapshot":
        return {"mode": "repair"}
    return None


def _invoke_route(runtime, *, method: str = "GET", path: str):
    dashboard_server = runtime["dashboard_server"]
    route = _route_index(dashboard_server)[(method, path)]
    handler_name = str(route.get("handler") or "")
    handler = dashboard_server.API_HANDLERS[handler_name]
    parsed = _parsed_for_path(
        path,
        default_job_name=runtime["default_job_name"],
        default_source_key=runtime["default_source_key"],
    )
    return dashboard_server._call_with_typeerror_fallbacks(
        handler_name,
        handler,
        (parsed, None, runtime["ctx"]),
        (parsed, runtime["ctx"]),
        (parsed,),
        (),
    )


def _is_path_with_params(path: str) -> bool:
    return any(token in str(path or "") for token in ("{", "}", "<", ">", ":"))


def test_route_specs_integrity(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("TIMESCALE_ENABLED", "0")
    monkeypatch.delenv("TIMESCALE_DSN", raising=False)
    monkeypatch.delenv("TIMESCALE_URL", raising=False)
    monkeypatch.delenv("TIMESCALE_DATABASE_URL", raising=False)
    monkeypatch.setenv("FEATURE_STORE_ENABLED", "0")
    monkeypatch.setenv("FEATURE_STORE_INIT_ON_STARTUP", "0")
    (dashboard_server,) = _reload_modules("dashboard_server")
    route_specs = list(dashboard_server.ROUTE_SPECS)
    api_handlers = dict(dashboard_server.API_HANDLERS)

    route_keys = [(str(route["method"]), str(route["path"])) for route in route_specs]
    duplicates = [key for key, count in Counter(route_keys).items() if count > 1]
    assert duplicates == [], f"duplicate registered routes: {duplicates}"

    missing_handlers = [
        {
            "method": str(route["method"]),
            "path": str(route["path"]),
            "handler": str(route["handler"]),
        }
        for route in route_specs
        if str(route["handler"]) not in api_handlers or not callable(api_handlers.get(str(route["handler"])))
    ]
    assert missing_handlers == [], f"route handlers missing from API_HANDLERS: {missing_handlers}"


def test_mutation_controls_are_post_only_and_confirmed(route_runtime):
    dashboard_server = route_runtime["dashboard_server"]
    route_index = _route_index(dashboard_server)

    mutation_paths = {
        "/api/promotion/enable",
        "/api/system/fix",
        "/api/size_policy/train",
        "/api/strategy/size_policy/train",
        "/api/champion/rollback",
    }
    for path in mutation_paths:
        assert ("GET", path) not in route_index
        assert ("POST", path) in route_index

    assert dashboard_server.api_get_promotion_enable(None)["http_status"] == 405
    assert dashboard_server.api_get_system_fix(None)["http_status"] == 405
    assert dashboard_server.api_get_champion_rollback(None)["http_status"] == 405

    assert dashboard_server.api_post_promotion_enable(None, {}, route_runtime["ctx"]) == {
        "ok": False,
        "error": "confirmation_required",
        "required_confirm": "PROMOTION",
        "http_status": 422,
    }
    assert dashboard_server.api_post_system_fix(None, {}, route_runtime["ctx"]) == {
        "ok": False,
        "error": "confirmation_required",
        "required_confirm": "SYSTEM_FIX",
        "http_status": 422,
    }
    assert dashboard_server.api_post_size_policy_train(None, {}, route_runtime["ctx"]) == {
        "ok": False,
        "error": "confirmation_required",
        "required_confirm": "TRAIN_SIZE_POLICY",
        "http_status": 422,
    }
    from engine.api.api_ops_handlers import api_post_rollback

    assert api_post_rollback(None, {}, route_runtime["ctx"]) == {
        "ok": False,
        "error": "confirmation_required",
        "required_confirm": "ROLLBACK_CHAMPION",
        "http_status": 422,
    }
    assert api_post_rollback(None, {"confirm": "ROLLBACK_CHAMPION"}, route_runtime["ctx"]) == {
        "ok": False,
        "error": "justification_required",
        "min_length": 12,
        "http_status": 422,
    }


def test_safe_routes_respond(route_runtime):
    dashboard_server = route_runtime["dashboard_server"]
    route_index = _route_index(dashboard_server)

    failures = []
    for path in AUDITED_SAFE_ROUTE_PATHS:
        route_key = ("GET", path)
        if route_key not in route_index:
            failures.append(f"{path}: route not registered")
            continue

        try:
            response = _invoke_route(route_runtime, method="GET", path=path)
        except Exception as exc:
            failures.append(f"{path}: raised {type(exc).__name__}: {exc}")
            continue

        if not isinstance(response, dict):
            failures.append(f"{path}: expected dict response, got {type(response).__name__}")
            continue

        expected_keys = EXPECTED_RESPONSE_KEYS[path]
        missing_keys = sorted(expected_keys - set(response))
        if missing_keys:
            failures.append(f"{path}: missing keys {missing_keys}")
            continue

        if path in ("/api/system/health", "/api/health") and not isinstance(response.get("db"), dict):
            failures.append(f"{path}: expected db payload to be a dict")
        if path == "/api/status" and not isinstance(response.get("health"), dict):
            failures.append(f"{path}: expected health payload to be a dict")
        if path == "/api/readiness" and not isinstance(response.get("readiness"), dict):
            failures.append(f"{path}: expected readiness payload to be a dict")
        if path == "/api/readiness" and not isinstance(response.get("production_validation"), dict):
            failures.append(f"{path}: expected production_validation payload to be a dict")
        if path == "/api/operator/snapshot":
            if not isinstance(response.get("production_validation"), dict):
                failures.append(f"{path}: expected production_validation payload to be a dict")
            if not isinstance(response.get("diagnostics"), dict):
                failures.append(f"{path}: expected diagnostics payload to be a dict")
        if path == "/api/jobs" and not isinstance(response.get("jobs"), list):
            failures.append(f"{path}: expected jobs payload to be a list")
        if path in ("/api/jobs/log", "/api/jobs/history") and str(response.get("job") or "").strip() == "":
            failures.append(f"{path}: expected resolved job name in response")
        if path == "/api/operator/db_schema" and not isinstance(response.get("missing_tables"), list):
            failures.append(f"{path}: expected missing_tables to be a list")
        if path == "/api/data_sources" and not isinstance(response.get("sources"), list):
            failures.append(f"{path}: expected sources payload to be a list")
        if path == "/api/data_sources/logs" and not isinstance(response.get("logs"), list):
            failures.append(f"{path}: expected logs payload to be a list")
        if path == "/api/market/candles":
            if not isinstance(response.get("candles"), list):
                failures.append(f"{path}: expected candles payload to be a list")
            if not isinstance(response.get("meta"), dict):
                failures.append(f"{path}: expected meta payload to be a dict")

    assert failures == [], "safe route contract failures:\n- " + "\n- ".join(failures)


def test_startup_timeout_only_preflight_is_non_repairable_warning(route_runtime):
    dashboard_server = route_runtime["dashboard_server"]

    assert dashboard_server._is_timeout_only_preflight(
        {
            "ok": False,
            "timed_out": True,
            "tables_ok": True,
            "notes": ["preflight_timeout_after_5.0s"],
        }
    )

    assert not dashboard_server._is_timeout_only_preflight(
        {
            "ok": False,
            "timed_out": False,
            "tables_ok": True,
            "notes": ["db_missing"],
        }
    )


def test_api_get_readiness_uses_lightweight_snapshot_path(route_runtime, monkeypatch):
    (api_system,) = _reload_modules("engine.api.api_system")

    light_snapshot = {
        "ok": False,
        "status": "DEGRADED",
        "state": "WARMING_UP",
        "mode": "safe",
        "execution_mode": "safe",
        "execution_allowed": False,
        "reasons": ["prices_not_ok"],
        "timestamps": {"ts_ms": 123456, "snapshot_ts_ms": 123456},
        "ts_ms": 123456,
        "health": {
            "ok": False,
            "startup_validation": {
                "ok": True,
                "ts_ms": 111,
                "gates": {
                    "config_valid": {"ok": True, "detail": "ok", "component": "runtime_config", "ts_ms": 111},
                    "database_reachable": {"ok": True, "detail": "ok", "component": "database", "ts_ms": 111},
                    "schema_valid": {"ok": True, "detail": "ok", "component": "database", "ts_ms": 111},
                    "core_services_initialized": {"ok": True, "detail": "ok", "component": "runtime", "ts_ms": 111},
                    "required_api_dependencies_available": {"ok": True, "detail": "ok", "component": "api", "ts_ms": 111},
                    "ui_static_assets_present": {"ok": True, "detail": "ok", "component": "ui", "ts_ms": 111},
                    "no_port_binding_conflict": {"ok": True, "detail": "dashboard_listener_bound", "component": "listener", "ts_ms": 111},
                },
                "db_validation": {
                    "last_write_timestamps": {
                        "prices_last_ts_ms": 180,
                        "event_log_last_ts_ms": 181,
                        "job_heartbeats_last_ts_ms": 182,
                    }
                },
            },
            "data_pipeline_gates": {
                "updated_ts_ms": 222,
                "gates": {
                    "ingestion_active": {"ok": True, "detail": "ok", "freshest_activity_ts_ms": 190},
                    "ingestion_not_stale": {"ok": False, "detail": "critical_source_stale:options"},
                    "critical_features_valid": {"ok": False, "detail": "feature_validation_missing"},
                    "model_inputs_valid": {"ok": False, "detail": "model_input_validation_missing"},
                    "scoring_pipeline_operational": {"ok": False, "detail": "scoring_pipeline_unreported"},
                },
            },
            "execution_supervisor": {
                "ok": True,
                "ts_ms": 333,
                "state": "ok",
                "alerts": [],
                "gates": {
                    "execution_engine_initialized": {"ok": True, "detail": "ok"},
                    "order_state_consistent": {"ok": True, "detail": "ok"},
                    "position_state_consistent": {"ok": True, "detail": "ok"},
                    "pnl_calculation_valid": {"ok": True, "detail": "ok"},
                },
            },
            "ingestion_runtime": {"running": True, "last_publish_ts_ms": 190},
            "providers": {"ok": False},
            "prices": {"ok": False, "last_ts_ms": 190},
            "predictions": {"ok": False, "last_ts_ms": None, "detail": "unreported"},
            "scoring_runtime": {"ok": False, "last_success_ts_ms": None, "detail": "unreported"},
            "execution": {"ok": True, "last_fill_ts_ms": None, "fills_table": "fills"},
            "lifecycle": {"state": "WARMING_UP", "detail": "awaiting_first_price_tick", "ts_ms": 333},
            "job_summary": {"ok": True, "total": 1, "stale": 0, "stale_jobs": []},
            "graph": {"ok": True},
        },
        "ingestion": {"ok": False, "last_price_ts_ms": 190},
        "services": {"ok": True, "engine": {"running": True}},
        "readiness": {"ok": False},
        "jobs": [],
        "kill_switches": {},
        "execution_barrier": {"ok": True, "allowed": False, "mode": "safe", "reason": "health_fast_path"},
        "system_state_detail": {"ok": True, "state": "WARMING_UP", "detail": "awaiting_first_price_tick"},
    }

    monkeypatch.setattr(api_system, "_get_cached_system_snapshot", lambda: None)
    monkeypatch.setattr(api_system, "_cached_health_snapshot", lambda *, allow_sync_on_miss=True: dict(light_snapshot["health"]))
    monkeypatch.setattr(api_system, "_build_system_snapshot", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("full snapshot path should not run")))
    monkeypatch.setattr(api_system, "_build_system_state_snapshot", lambda *_args, **_kwargs: dict(light_snapshot))
    monkeypatch.setattr(api_system, "_get_supervisor_graph", lambda *_args, **_kwargs: {"ok": True})
    monkeypatch.setattr(api_system, "api_get_runtime_watchdogs", lambda *_args, **_kwargs: {"ok": True, "pipeline_watchdog_state": {}})

    response = api_system.api_get_readiness({}, route_runtime["ctx"])

    assert isinstance(response, dict)
    assert isinstance(response.get("readiness"), dict)
    assert isinstance(response.get("production_validation"), dict)
    assert response["status"] == "FAILED"
    assert response["graph_valid"] is True


def test_system_state_uses_lightweight_snapshot_path(route_runtime, monkeypatch):
    (api_system,) = _reload_modules("engine.api.api_system")

    light_snapshot = {
        "ok": False,
        "status": "DEGRADED",
        "state": "LIVE",
        "mode": "safe",
        "execution_mode": "safe",
        "execution_allowed": False,
        "reasons": ["mode_safe"],
        "timestamps": {"ts_ms": 123456, "snapshot_ts_ms": 123456},
        "ts_ms": 123456,
        "health": {"ok": False},
        "ingestion": {"ok": True},
        "services": {"ok": True},
        "readiness": {"ok": False},
        "execution_barrier": {"ok": True, "allowed": False, "mode": "safe", "reason": "mode_safe"},
        "system_state_detail": {"ok": True, "state": "LIVE"},
    }

    monkeypatch.setattr(api_system, "_build_system_snapshot", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("full snapshot path should not run")))
    monkeypatch.setattr(api_system, "_build_system_state_snapshot", lambda *_args, **_kwargs: dict(light_snapshot))

    response = api_system.api_get_system_state({}, route_runtime["ctx"])

    assert response["state"] == "LIVE"
    assert response["mode"] == "safe"
    assert response["execution_allowed"] is False


def test_execution_barrier_uses_lightweight_snapshot_path(route_runtime, monkeypatch):
    (api_system,) = _reload_modules("engine.api.api_system")

    light_snapshot = {
        "ok": False,
        "status": "DEGRADED",
        "state": "LIVE",
        "mode": "safe",
        "execution_mode": "safe",
        "execution_allowed": False,
        "reasons": ["mode_safe"],
        "timestamps": {"ts_ms": 123456, "snapshot_ts_ms": 123456},
        "ts_ms": 123456,
        "health": {"ok": False},
        "ingestion": {"ok": True},
        "services": {"ok": True},
        "readiness": {"ok": False},
        "execution_barrier": {"ok": True, "allowed": False, "mode": "safe", "reason": "mode_safe"},
        "system_state_detail": {"ok": True, "state": "LIVE"},
    }

    monkeypatch.setattr(api_system, "_build_system_snapshot", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("full snapshot path should not run")))
    monkeypatch.setattr(api_system, "_build_system_state_snapshot", lambda *_args, **_kwargs: dict(light_snapshot))

    response = api_system.api_get_execution_barrier({}, route_runtime["ctx"])

    assert response["allowed"] is False
    assert response["reason"] == "mode_safe"
    assert response["execution_barrier"]["mode"] == "safe"


def test_lightweight_system_snapshot_avoids_heavy_state_reads(route_runtime, monkeypatch):
    (api_system,) = _reload_modules("engine.api.api_system")

    monkeypatch.setattr(
        api_system,
        "_cached_health_snapshot",
        lambda *, allow_sync_on_miss=True: {
            "ok": False,
            "reasons": ["health_fast_path"],
            "lifecycle": {"state": "WARMING_UP", "detail": "awaiting_first_price_tick"},
            "ingestion_runtime": {"running": True},
            "prices": {"ok": True, "last_ts_ms": 123},
            "providers": {"ok": True},
            "job_summary": {"ok": True, "total": 1, "running_count": 1, "stale": 0},
        },
    )
    monkeypatch.setattr(api_system, "_get_jobs_payload", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("jobs manager path should not run")))
    monkeypatch.setattr(api_system, "market_data_status", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("market data IPC path should not run")))
    monkeypatch.setattr(api_system, "_get_kill_switch_data", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("kill switch DB path should not run")))
    monkeypatch.setattr(api_system, "compute_system_state", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("heavy system state should not run")))

    snapshot = api_system._build_system_state_snapshot({}, route_runtime["ctx"])

    assert snapshot["state"] == "WARMING_UP"
    assert snapshot["mode"] == "safe"
    assert snapshot["execution_allowed"] is False
    assert snapshot["execution_barrier"]["fast_path"] is True
    assert snapshot["services"]["source"] == "cached_health"
    assert snapshot["ingestion"]["source"] == "cached_health"


def test_lightweight_system_snapshot_uses_safe_runtime_signal_when_lifecycle_missing(
    route_runtime,
    monkeypatch,
):
    (api_system,) = _reload_modules("engine.api.api_system")

    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setattr(
        api_system,
        "_cached_health_snapshot",
        lambda *, allow_sync_on_miss=True: {
            "ok": False,
            "startup": {"mode": "safe"},
            "db": {"ok": True},
            "ingestion_runtime": {"running": True},
            "prices": {"ok": True, "last_ts_ms": 123},
            "providers": {"ok": True},
            "job_summary": {"ok": True, "total": 1, "running_count": 1, "stale": 0},
            "execution_barrier": {"ok": True, "allowed": False, "mode": "safe", "reason": "health_fast_path"},
        },
    )
    monkeypatch.setattr(api_system, "_get_jobs_payload", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("jobs manager path should not run")))
    monkeypatch.setattr(api_system, "market_data_status", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("market data IPC path should not run")))

    snapshot = api_system._build_system_state_snapshot({}, route_runtime["ctx"])

    assert snapshot["state"] == "WARMING_UP"
    assert snapshot["status"] == "STARTING"
    assert snapshot["execution_allowed"] is False
    assert snapshot["execution_barrier"]["real_trading_allowed"] is False


def test_api_get_readiness_uses_cached_storage_readiness(route_runtime, monkeypatch):
    (api_system,) = _reload_modules("engine.api.api_system")
    import engine.runtime.storage_pool as storage_pool

    monkeypatch.setattr(
        storage_pool,
        "storage_readiness_snapshot",
        lambda: (_ for _ in ()).throw(AssertionError("live storage readiness should not run")),
    )
    monkeypatch.setattr(
        api_system,
        "_cached_health_snapshot",
        lambda *, allow_sync_on_miss=True: {
            "ok": False,
            "startup_validation": {
                "checks": {
                    "core_services_initialized": {
                        "boot_diagnostics": {
                            "storage": {"status": "ready", "degraded": False}
                        }
                    }
                }
            },
        },
    )
    monkeypatch.setattr(
        api_system,
        "_build_readiness_snapshot",
        lambda *_args, **_kwargs: {
            "ts_ms": 123456,
            "state": "WARMING_UP",
            "mode": "safe",
            "execution_mode": "safe",
            "execution_allowed": False,
            "reasons": ["health_fast_path"],
            "readiness": {"ok": False, "ready": False, "issues": []},
            "production_validation": {
                "status": "failed",
                "safe_to_operate": False,
                "unsafe_to_operate": True,
                "current_degraded_reasons": ["health_fast_path"],
            },
            "health": {"ok": False},
            "graph": {"ok": False, "source": "cached_health"},
        },
    )

    response = api_system.api_get_readiness({}, route_runtime["ctx"])

    assert response["ok"] is False
    assert response["execution_allowed"] is False
    assert response["storage"]["status"] == "ready"
    assert response["storage"]["checked"] is True


def test_runtime_watchdogs_health_cache_miss_does_not_sync_refresh(route_runtime, monkeypatch):
    (api_system,) = _reload_modules("engine.api.api_system")
    calls = []

    def fake_cached_health_snapshot(*, allow_sync_on_miss=True):
        calls.append(bool(allow_sync_on_miss))
        return {
            "ok": False,
            "prices": {"ok": False},
            "ingestion_freshness": {},
            "events": {},
            "labels": {},
            "model": {},
            "job_summary": {},
            "jobs": {
                "ingestion_runtime": {
                    "running": True,
                    "heartbeat_ts_ms": 123456,
                    "heartbeat_age_s": 1.0,
                    "restart_count": 0,
                    "stale": False,
                }
            },
        }

    monkeypatch.setattr(api_system, "_cached_health_snapshot", fake_cached_health_snapshot)
    monkeypatch.setattr(api_system, "_get_jobs_payload", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("jobs manager path should not run")))

    response = api_system.api_get_runtime_watchdogs({}, route_runtime["ctx"])

    assert calls == [False]
    assert response["ok"] is False
    assert response["pipeline_watchdog_state"]["ingestion_runtime"]["running"] is True


def test_api_get_health_degraded_cache_keeps_http_ok(route_runtime, monkeypatch):
    (api_system,) = _reload_modules("engine.api.api_system")

    monkeypatch.setattr(
        api_system,
        "_cached_health_snapshot",
        lambda *, allow_sync_on_miss=True: {
            "ok": False,
            "error": "storage_pool_timeout",
            "reasons": ["health_snapshot_error:storage_pool_timeout"],
            "meta": {"status": 500},
        },
    )

    response = api_system.api_get_health({}, route_runtime["ctx"])

    assert response["ok"] is False
    assert response["error"] == "storage_pool_timeout"
    assert response["meta"]["status"] == 200


def test_api_get_health_cache_miss_returns_placeholder_without_sync_refresh(route_runtime, monkeypatch):
    (api_system,) = _reload_modules("engine.api.api_system")

    api_system._HEALTH_CACHE["ts_ms"] = 0
    api_system._HEALTH_CACHE["payload"] = None

    scheduled = []
    monkeypatch.setattr(api_system, "_schedule_health_cache_refresh", lambda: scheduled.append(True))
    monkeypatch.setattr(
        api_system,
        "get_health_snapshot",
        lambda: (_ for _ in ()).throw(AssertionError("health snapshot should not run inline on cache miss")),
    )

    response = api_system.api_get_health(None, None)

    assert isinstance(response, dict)
    assert response["ok"] is False
    assert response["status"] == "BOOTING"
    assert response["error"] == "health_snapshot_pending"
    assert response["warming_up"] is True
    assert isinstance(response.get("db"), dict)
    assert response["db"]["detail"] == "health_snapshot_pending"
    assert isinstance(response.get("cache"), dict)
    assert response["cache"]["source"] == "api_system_cache"
    assert response["cache"]["stale"] is True
    assert scheduled == [True]


def test_cached_health_snapshot_returns_stale_payload_without_sync_refresh(monkeypatch):
    (api_system,) = _reload_modules("engine.api.api_system")

    refresh_calls = []
    now_ms = api_system._ts_ms()
    stale_payload = {"ok": True, "ts_ms": 111, "db": {"ok": True}}

    monkeypatch.setattr(api_system, "_schedule_health_cache_refresh", lambda: refresh_calls.append("scheduled"))
    monkeypatch.setattr(
        api_system,
        "get_health_snapshot",
        lambda: (_ for _ in ()).throw(AssertionError("synchronous health refresh should not run")),
    )

    with api_system._HEALTH_CACHE_LOCK:
        api_system._HEALTH_CACHE["ts_ms"] = int(now_ms - api_system._HEALTH_CACHE_TTL_MS - 500)
        api_system._HEALTH_CACHE["payload"] = dict(stale_payload)

    snapshot = api_system._cached_health_snapshot()

    assert snapshot["ok"] is True
    assert snapshot["cache"]["stale"] is True
    assert int(snapshot["cache"]["age_ms"]) > int(api_system._HEALTH_CACHE_TTL_MS)
    assert refresh_calls == ["scheduled"]


def test_get_kill_switch_data_prefers_direct_snapshot_over_handler(route_runtime, monkeypatch):
    (api_system,) = _reload_modules("engine.api.api_system")
    kill_switch = importlib.import_module("engine.execution.kill_switch")

    monkeypatch.setattr(kill_switch, "snapshot", lambda: {"state": [{"scope": "global", "key": "global", "enabled": 0}]})

    data, errors = api_system._get_kill_switch_data(
        {},
        {
            "API_HANDLERS": {
                "api_get_kill_switches": lambda *_args, **_kwargs: (_ for _ in ()).throw(
                    AssertionError("fallback handler should not run")
                )
            }
        },
    )

    assert errors == []
    assert isinstance(data, dict)
    assert isinstance(data.get("state"), list)


def test_no_500_from_registered_safe_routes(route_runtime):
    dashboard_server = route_runtime["dashboard_server"]

    covered = []
    failures = []

    for route in dashboard_server.ROUTE_SPECS:
        method = str(route.get("method") or "")
        path = str(route.get("path") or "")
        handler_name = str(route.get("handler") or "")

        if method != "GET":
            continue
        if _is_path_with_params(path):
            continue
        if handler_name not in SAFE_LOCAL_HANDLER_NAMES:
            continue

        covered.append(path)
        try:
            response = _invoke_route(route_runtime, method=method, path=path)
        except Exception as exc:
            failures.append(f"{path} ({handler_name}) raised {type(exc).__name__}: {exc}")
            continue

        if not isinstance(response, dict):
            failures.append(f"{path} ({handler_name}) returned {type(response).__name__}, expected dict")

    assert covered, "no registered safe GET routes were covered"
    assert failures == [], "registered safe route failures:\n- " + "\n- ".join(sorted(failures))


def test_route_deduplication_behavior(route_runtime):
    dashboard_server = route_runtime["dashboard_server"]

    normalized = dashboard_server._normalize_route_specs(
        [
            ("GET", "/api/test/first", "first_handler"),
            ("GET", "/api/test/first", "second_handler"),
            {"method": "GET", "path": "/api/test/second", "handler": "third_handler"},
            {"method": "GET", "path": "/api/test/second", "handler": "fourth_handler"},
        ]
    )

    assert normalized == [
        {"method": "GET", "path": "/api/test/first", "handler": "first_handler"},
        {"method": "GET", "path": "/api/test/second", "handler": "third_handler"},
    ]

    fallback_by_key = {
        (str(route.get("method") or "").upper(), str(route.get("path") or "")): str(route.get("handler") or "")
        for route in dashboard_server._FALLBACK_ROUTE_SPECS
    }
    pre_fallback_routes = list(dashboard_server._RAW_ROUTE_SPECS[:-len(dashboard_server._FALLBACK_ROUTE_SPECS)])
    canonical_by_key = {}
    for route in pre_fallback_routes:
        if isinstance(route, dict):
            key = (str(route.get("method") or "").upper(), str(route.get("path") or ""))
            handler = str(route.get("handler") or "")
        else:
            key = (str(route[0] or "").upper(), str(route[1] or ""))
            handler = str(route[2] or "")
        canonical_by_key.setdefault(key, handler)

    overlapping_keys = sorted(key for key in fallback_by_key if key in canonical_by_key)
    assert overlapping_keys, "expected fallback route overlap with canonical route specs"

    normalized_by_key = {
        (str(route.get("method") or "").upper(), str(route.get("path") or "")): str(route.get("handler") or "")
        for route in dashboard_server._normalize_route_specs(dashboard_server._RAW_ROUTE_SPECS)
    }
    for key in overlapping_keys:
        assert normalized_by_key[key] == canonical_by_key[key], (
            f"fallback route overrode canonical route for {key}: "
            f"canonical={canonical_by_key[key]} fallback={fallback_by_key[key]} normalized={normalized_by_key[key]}"
        )


def test_status_snapshot_passes_health_execution_degraded_into_gate(route_runtime, monkeypatch: pytest.MonkeyPatch):
    (api_system,) = _reload_modules("engine.api.api_system")

    execution_degraded = {
        "active": True,
        "severity": "CRITICAL",
        "reason": "event_bus_critical_backpressure",
        "reason_codes": ["event_bus_critical_backpressure"],
    }
    gate_calls = []

    monkeypatch.setattr(
        api_system,
        "get_health_snapshot",
        lambda: {
            "ok": False,
            "reasons": ["execution_degraded:event_bus_critical_backpressure"],
            "db": {"ok": True, "initialized": True, "exists": True},
            "prices": {"ok": True},
            "events": {"ok": True},
            "providers": {"ok": True},
            "job_summary": {"ok": True, "total": 1},
            "timeseries_storage": {"ok": True, "enabled": False},
            "feature_store": {"ok": True, "enabled": False},
            "portfolio_runtime": {"degraded": False},
            "execution_degraded": dict(execution_degraded),
            "execution_barrier": {"ok": False, "allowed": False, "reason": "stale_health_payload"},
        },
    )
    monkeypatch.setattr(
        api_system,
        "get_schema_audit",
        lambda: {"ok": True, "have_tables": [], "missing_tables": [], "missing_cols": {}},
    )
    monkeypatch.setattr(api_system, "run_preflight", lambda: {"ok": True, "notes": []})
    monkeypatch.setattr(api_system, "_get_supervisor_graph", lambda _ctx: {"ok": True})
    monkeypatch.setattr(api_system, "_get_jobs_payload", lambda _ctx: ([{"name": "process_events", "running": True, "stale": False}], []))
    monkeypatch.setattr(api_system, "_build_services_snapshot", lambda _jobs: {"ok": True, "engine": {"running": True}})
    monkeypatch.setattr(api_system, "_build_ingestion_snapshot", lambda jobs, health: {"ok": True, "running": True, "reasons": []})
    monkeypatch.setattr(
        api_system,
        "get_readiness_snapshot",
        lambda **kwargs: {"ok": True, "ready": True, "issues": [], "steps": []},
    )
    monkeypatch.setattr(
        api_system,
        "compute_system_state",
        lambda **kwargs: {"ok": True, "state": "LIVE", "mode": "live", "reasons": []},
    )

    def _capture_gate(**kwargs):
        gate_calls.append(dict(kwargs))
        return {
            "ok": True,
            "allowed": False,
            "mode": "live",
            "reason": "event_bus_critical_backpressure",
        }

    monkeypatch.setattr(api_system, "execution_gate_snapshot", _capture_gate)

    snapshot = api_system._build_system_snapshot(None, route_runtime["ctx"])

    assert gate_calls
    assert gate_calls[0]["execution_degraded"] == execution_degraded
    assert snapshot["execution_barrier"]["reason"] == "event_bus_critical_backpressure"


def test_production_validation_fails_closed_on_execution_integrity_break(route_runtime):
    (api_system,) = _reload_modules("engine.api.api_system")

    snapshot = {
        "ts_ms": 123456,
        "status": "RUNNING",
        "state": "LIVE",
        "system_state_detail": {"state": "LIVE", "detail": "ok"},
        "health": {
            "health": {
                "startup_validation": {
                    "ok": True,
                    "ts_ms": 111,
                    "gates": {
                        "config_valid": {"ok": True, "detail": "ok", "component": "runtime_config", "ts_ms": 111},
                        "database_reachable": {"ok": True, "detail": "ok", "component": "database", "ts_ms": 111},
                        "schema_valid": {"ok": True, "detail": "ok", "component": "database", "ts_ms": 111},
                        "core_services_initialized": {"ok": True, "detail": "ok", "component": "runtime", "ts_ms": 111},
                        "required_api_dependencies_available": {"ok": True, "detail": "ok", "component": "api", "ts_ms": 111},
                        "ui_static_assets_present": {"ok": True, "detail": "ok", "component": "ui", "ts_ms": 111},
                        "no_port_binding_conflict": {"ok": True, "detail": "dashboard_listener_bound", "component": "listener", "ts_ms": 111},
                    },
                },
                "data_pipeline_gates": {
                    "updated_ts_ms": 222,
                    "gates": {
                        "ingestion_active": {"ok": True, "detail": "ok"},
                        "ingestion_not_stale": {"ok": True, "detail": "ok"},
                        "critical_features_valid": {"ok": True, "detail": "ok"},
                        "model_inputs_valid": {"ok": True, "detail": "ok"},
                        "scoring_pipeline_operational": {"ok": True, "detail": "ok", "last_success_ts_ms": 200},
                    },
                },
                "execution_supervisor": {
                    "ok": True,
                    "ts_ms": 333,
                    "state": "critical",
                    "alerts": [
                        {"alert_type": "duplicate_order_risk_detected"},
                        {"alert_type": "pricing_unavailable_for_unrealized_pnl"},
                    ],
                    "gates": {
                        "execution_engine_initialized": {"ok": True, "detail": "ok"},
                        "order_state_consistent": {"ok": False, "detail": "duplicate_order_count=1"},
                        "position_state_consistent": {"ok": True, "detail": "ok"},
                        "pnl_calculation_valid": {"ok": False, "detail": "pricing_unavailable_count=1"},
                    },
                },
                "ingestion_runtime": {"running": True, "last_publish_ts_ms": 190},
                "providers": {"ok": True},
                "prices": {"ok": True, "last_ts_ms": 190},
                "predictions": {"ok": True, "last_ts_ms": 210, "detail": "ok"},
                "scoring_runtime": {"ok": True, "last_success_ts_ms": 210, "detail": "ok"},
                "execution": {"ok": True, "last_fill_ts_ms": 205, "fills_table": "fills"},
                "lifecycle": {"state": "LIVE", "detail": "ok", "ts_ms": 333},
            }
        },
        "db_validation": {
            "last_write_timestamps": {
                "prices_last_ts_ms": 180,
                "event_log_last_ts_ms": 181,
                "job_heartbeats_last_ts_ms": 182,
            }
        },
        "database_debug": {"failure_classification": {"primary_cause": ""}},
        "services": {"engine": {"running": True}},
        "ingestion": {"ok": True, "last_price_ts_ms": 190},
        "graph": {"ok": True},
        "jobs": [],
        "job_launch_trace": [],
        "supervisor_analysis": {},
        "readiness": {"ok": True},
    }

    production = api_system._build_production_validation(
        snapshot,
        ctx=route_runtime["ctx"],
        runtime_watchdogs={"ok": True, "pipeline_watchdog_state": {}},
    )

    assert production["status"] == "failed"
    assert production["safe_to_operate"] is False
    assert production["gates"]["order_state_consistent"]["status"] == "failed"
    assert production["gates"]["pnl_calculation_valid"]["status"] == "failed"


def test_production_validation_marks_partial_ui_dependencies_as_degraded(route_runtime):
    (api_system,) = _reload_modules("engine.api.api_system")

    snapshot = {
        "ts_ms": 123456,
        "status": "RUNNING",
        "state": "LIVE",
        "system_state_detail": {"state": "LIVE", "detail": "ok"},
        "health": {
            "health": {
                "startup_validation": {
                    "ok": True,
                    "ts_ms": 111,
                    "gates": {
                        "config_valid": {"ok": True, "detail": "ok", "component": "runtime_config", "ts_ms": 111},
                        "database_reachable": {"ok": True, "detail": "ok", "component": "database", "ts_ms": 111},
                        "schema_valid": {"ok": True, "detail": "ok", "component": "database", "ts_ms": 111},
                        "core_services_initialized": {"ok": True, "detail": "ok", "component": "runtime", "ts_ms": 111},
                        "required_api_dependencies_available": {"ok": True, "detail": "ok", "component": "api", "ts_ms": 111},
                        "ui_static_assets_present": {"ok": True, "detail": "ok", "component": "ui", "ts_ms": 111},
                        "no_port_binding_conflict": {"ok": True, "detail": "dashboard_listener_bound", "component": "listener", "ts_ms": 111},
                    },
                },
                "data_pipeline_gates": {
                    "updated_ts_ms": 222,
                    "gates": {
                        "ingestion_active": {"ok": True, "detail": "ok", "freshest_activity_ts_ms": 190},
                        "ingestion_not_stale": {"ok": True, "detail": "ok"},
                        "critical_features_valid": {"ok": True, "detail": "ok"},
                        "model_inputs_valid": {"ok": True, "detail": "ok"},
                        "scoring_pipeline_operational": {"ok": True, "detail": "ok", "last_success_ts_ms": 210},
                    },
                },
                "execution_supervisor": {
                    "ok": True,
                    "ts_ms": 333,
                    "state": "ok",
                    "alerts": [],
                    "gates": {
                        "execution_engine_initialized": {"ok": True, "detail": "ok"},
                        "order_state_consistent": {"ok": True, "detail": "ok"},
                        "position_state_consistent": {"ok": True, "detail": "ok"},
                        "pnl_calculation_valid": {"ok": True, "detail": "ok"},
                    },
                },
                "ingestion_runtime": {"running": True, "last_publish_ts_ms": 190},
                "providers": {"ok": True},
                "prices": {"ok": True, "last_ts_ms": 190},
                "predictions": {"ok": True, "last_ts_ms": 210, "detail": "ok"},
                "scoring_runtime": {"ok": True, "last_success_ts_ms": 210, "detail": "ok"},
                "execution": {"ok": True, "last_fill_ts_ms": 205, "fills_table": "fills"},
                "lifecycle": {"state": "LIVE", "detail": "ok", "ts_ms": 333},
            }
        },
        "db_validation": {
            "last_write_timestamps": {
                "prices_last_ts_ms": 180,
                "event_log_last_ts_ms": 181,
                "job_heartbeats_last_ts_ms": 182,
            }
        },
        "database_debug": {"failure_classification": {"primary_cause": ""}},
        "services": {"engine": {"running": True}},
        "ingestion": {"ok": True, "last_price_ts_ms": 190},
        "graph": {"ok": True},
        "jobs": [],
        "job_launch_trace": [],
        "supervisor_analysis": {},
        "readiness": {"ok": True},
    }

    production = api_system._build_production_validation(
        snapshot,
        ctx=route_runtime["ctx"],
        runtime_watchdogs={"ok": True},
    )

    assert production["status"] == "degraded"
    assert production["failed"] is False
    assert production["safe_to_operate"] is False
    assert production["gates"]["critical_ui_dependencies_available"]["status"] == "degraded"
