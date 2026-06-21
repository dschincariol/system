from __future__ import annotations

import importlib
import inspect
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_dashboard(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    token_file = tmp_path / "dashboard_api_token"
    token_file.write_text("dashboard-decomposition-token", encoding="utf-8")
    token_file.chmod(0o600)

    monkeypatch.setenv("DB_PATH", str(tmp_path / "dashboard-decomposition.sqlite"))
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    monkeypatch.setenv("OPERATOR_MODE", "safe")
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("PROD_LOCK", "0")
    monkeypatch.setenv("DASHBOARD_ROUTE_CONTRACT_INTROSPECTION", "1")
    monkeypatch.setenv("DASHBOARD_API_TOKEN_FILE", str(token_file))
    monkeypatch.setenv("TRADING_SECRET_POLICY_REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("TIMESCALE_ENABLED", "0")
    monkeypatch.setenv("FEATURE_STORE_ENABLED", "0")
    monkeypatch.setenv("FEATURE_STORE_INIT_ON_STARTUP", "0")
    monkeypatch.delenv("TIMESCALE_DSN", raising=False)
    monkeypatch.delenv("TIMESCALE_URL", raising=False)
    monkeypatch.delenv("TIMESCALE_DATABASE_URL", raising=False)

    module = importlib.import_module("dashboard_server")
    return importlib.reload(module)


def test_dashboard_public_decomposition_surface_is_characterized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    dashboard_server = _reload_dashboard(monkeypatch, tmp_path)

    public_helpers = [
        "_env_int",
        "_env_float",
        "_env_bool",
        "_json_dict",
        "_normalize_explain_json",
        "_normalize_route_specs",
        "_db_health_snapshot",
        "api_get_db_health",
        "api_get_schema_audit",
        "run_server",
        "stop_server",
    ]
    for name in public_helpers:
        assert callable(getattr(dashboard_server, name, None)), name

    signatures = {
        name: str(inspect.signature(getattr(dashboard_server, name)))
        for name in public_helpers
    }
    assert signatures == {
        "_env_int": "(key: 'str', default: 'int', *, minimum: 'int | None' = None, maximum: 'int | None' = None) -> 'int'",
        "_env_float": "(key: 'str', default: 'float', *, minimum: 'float | None' = None, maximum: 'float | None' = None) -> 'float'",
        "_env_bool": "(key: 'str', default: 'bool' = False) -> 'bool'",
        "_json_dict": "(value: 'Any') -> 'dict[str, Any]'",
        "_normalize_explain_json": "(val) -> str",
        "_normalize_route_specs": "(route_specs: 'list[Any] | tuple[Any, ...]') -> 'list[dict[str, str]]'",
        "_db_health_snapshot": "()",
        "api_get_db_health": "(_parsed, _ctx=None)",
        "api_get_schema_audit": "(_parsed)",
        "run_server": "()",
        "stop_server": "()",
    }

    for legacy_import in (
        "bootstrap_runtime",
        "StartupOrchestrator",
        "get_boot_jobs",
        "get_health_snapshot",
        "write_job_history",
        "auto_rollback_loop",
        "start_lifecycle_monitor",
        "BOOTING",
        "AUTO_SIZE_POLICY",
        "AUTO_PIPELINE",
        "AUTO_CHALLENGER",
    ):
        assert hasattr(dashboard_server, legacy_import), legacy_import

    assert dashboard_server.API_HANDLERS["api_get_db_health"] is dashboard_server.api_get_db_health
    assert dashboard_server.API_HANDLERS["api_get_schema_audit"] is dashboard_server.api_get_schema_audit
    assert dashboard_server.API_HANDLERS["api_get_server_status"] is dashboard_server.api_get_server_status

    fallback_specs = list(dashboard_server._FALLBACK_ROUTE_SPECS)
    assert fallback_specs
    assert dashboard_server._RAW_ROUTE_SPECS[-len(fallback_specs):] == fallback_specs
    assert fallback_specs[0] == {
        "method": "GET",
        "path": "/api/db/health",
        "handler": "api_get_db_health",
    }
    assert {
        "method": "GET",
        "path": "/api/operator/db_schema",
        "handler": "api_get_schema_audit",
    } in fallback_specs


def test_dashboard_route_normalization_keeps_first_valid_route(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    dashboard_server = _reload_dashboard(monkeypatch, tmp_path)

    normalized = dashboard_server._normalize_route_specs(
        [
            {"method": " get ", "path": " /api/example ", "handler": " first_handler "},
            ("GET", "/api/example", "second_handler"),
            ("post", "/api/other", "other_handler", "ignored_extra"),
            {"method": "", "path": "/api/skipped", "handler": "skipped_handler"},
            ("GET", "/api/malformed"),
            object(),
        ]
    )

    assert normalized == [
        {"method": "GET", "path": "/api/example", "handler": "first_handler"},
        {"method": "POST", "path": "/api/other", "handler": "other_handler"},
    ]


def test_dashboard_compat_helpers_keep_current_parsing_behavior(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    dashboard_server = _reload_dashboard(monkeypatch, tmp_path)

    monkeypatch.setenv("DASHBOARD_DECOMPOSITION_INT", "abc")
    monkeypatch.setenv("DASHBOARD_DECOMPOSITION_FLOAT", "9.9")
    monkeypatch.setenv("DASHBOARD_DECOMPOSITION_BOOL", "yes")

    assert dashboard_server._env_int("DASHBOARD_DECOMPOSITION_INT", 7, minimum=2, maximum=10) == 7
    assert dashboard_server._env_int("DASHBOARD_DECOMPOSITION_MISSING_INT", 99, maximum=10) == 10
    assert dashboard_server._env_float("DASHBOARD_DECOMPOSITION_FLOAT", 1.0, maximum=3.5) == 3.5
    assert dashboard_server._env_bool("DASHBOARD_DECOMPOSITION_BOOL") is True
    assert dashboard_server._env_bool("DASHBOARD_DECOMPOSITION_MISSING_BOOL", True) is True
    assert dashboard_server._json_dict({"ok": True}) == {"ok": True}
    assert dashboard_server._json_dict([("ok", True)]) == {}
    assert dashboard_server._normalize_explain_json(None) == "{}"
    assert dashboard_server._normalize_explain_json(b'{"ok": true}') == '{"ok": true}'
    assert dashboard_server._normalize_explain_json("not-json") == '{"raw": "not-json"}'


def test_dashboard_db_health_handler_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    dashboard_server = _reload_dashboard(monkeypatch, tmp_path)
    api_system = importlib.import_module("engine.api.api_system")
    runtime_health = importlib.import_module("engine.runtime.health")
    storage = importlib.import_module("engine.runtime.storage")

    class _Cursor:
        def __init__(self, rows):
            self._rows = list(rows)

        def fetchone(self):
            return self._rows[0] if self._rows else None

        def fetchall(self):
            return list(self._rows)

    class _Connection:
        closed = False

        def execute(self, sql: str):
            if "sqlite_master" in sql:
                return _Cursor([("runtime_meta",), ("event_log",)])
            if "COUNT(*) FROM runtime_meta" in sql:
                return _Cursor([(3,)])
            if "COUNT(*) FROM event_log" in sql:
                return _Cursor([(5,)])
            raise AssertionError(f"unexpected sql: {sql}")

        def close(self):
            self.closed = True

    connection = _Connection()
    monkeypatch.setattr(dashboard_server, "_db_health_snapshot", lambda: {"ok": True, "tables": []})
    monkeypatch.setattr(dashboard_server, "_dashboard_db_connect", lambda: connection)
    monkeypatch.setattr(
        api_system,
        "api_get_system_state",
        lambda _parsed, ctx: {"ok": True, "handler_count": len(ctx.get("API_HANDLERS") or {})},
    )
    monkeypatch.setattr(runtime_health, "get_health_snapshot", lambda: {"ok": True, "source": "health"})
    monkeypatch.setattr(api_system, "_recent_runtime_errors", lambda limit=10: [{"limit": limit}])
    monkeypatch.setattr(storage, "get_db_debug_snapshot", lambda: {"ok": True, "source": "storage"})

    payload = dashboard_server.api_get_db_health(None, {"API_HANDLERS": dashboard_server.API_HANDLERS})

    assert payload["ok"] is True
    assert payload["ts"] == payload["ts_ms"]
    assert payload["system_snapshot"]["ok"] is True
    assert payload["runtime_health"] == {"ok": True, "source": "health"}
    assert payload["recent_errors"] == [{"limit": 10}]
    assert payload["tables"] == ["runtime_meta", "event_log"]
    assert payload["row_counts"] == {"runtime_meta": 3, "event_log": 5}
    assert payload["storage_debug"] == {"ok": True, "source": "storage"}
    assert connection.closed is True
