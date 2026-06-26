from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen


@contextmanager
def _http_server(handler_cls):
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _build_handler(*, routes, handlers, token: str, static_dir: Path):
    import engine.api.http_transport as http_transport

    http_transport.emit_counter = lambda *args, **kwargs: None
    http_transport.emit_timing = lambda *args, **kwargs: None
    http_transport.deny_if_shutdown = lambda: None

    return http_transport.build_handler(
        ROUTE_SPECS=routes,
        API_HANDLERS=handlers,
        dashboard_api_token=token,
        ctx={},
        static_dir=str(static_dir),
    )


def _get_json(url: str, *, token: str) -> tuple[int, dict, dict]:
    req = Request(url, headers={"X-API-Token": token}, method="GET")
    with urlopen(req, timeout=5) as response:
        return response.status, dict(response.headers), json.loads(response.read().decode("utf-8"))


def _get_json_error(url: str, *, token: str) -> tuple[int, dict, dict]:
    req = Request(url, headers={"X-API-Token": token}, method="GET")
    try:
        with urlopen(req, timeout=5) as response:
            return response.status, dict(response.headers), json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        return exc.code, dict(exc.headers), json.loads(exc.read().decode("utf-8"))


def _prepare_cold_sim_db_without_optional_reads(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("DB_PATH", str(tmp_path / "e2e_r9_cold.sqlite"))

    from engine.runtime import storage

    storage.init_db()
    con = storage.connect(readonly=False)
    try:
        for table_name in ("execution_ai_advisory_actions", "execution_ai_advisory", "validation_scores"):
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.commit()
    finally:
        con.close()


def test_missing_table_reads_degrade_and_audit_table_errors_are_4xx(tmp_path: Path, monkeypatch) -> None:
    _prepare_cold_sim_db_without_optional_reads(tmp_path, monkeypatch)

    from engine.api.api_dashboard_reads import api_get_audit_records, api_get_validation
    from engine.api.api_ops_handlers import api_get_execution_advisories

    token = "production-token-1234567890"
    handler_cls = _build_handler(
        routes=[
            ("GET", "/api/execution/advisories", "api_get_execution_advisories"),
            ("GET", "/api/validation", "api_get_validation"),
            ("GET", "/api/audit/records", "api_get_audit_records"),
        ],
        handlers={
            "api_get_execution_advisories": api_get_execution_advisories,
            "api_get_validation": api_get_validation,
            "api_get_audit_records": api_get_audit_records,
        },
        token=token,
        static_dir=tmp_path,
    )

    with _http_server(handler_cls) as base_url:
        status, _headers, advisories = _get_json(f"{base_url}/api/execution/advisories", token=token)
        assert status == 200
        assert advisories["ok"] is True
        assert advisories["rows"] == []
        assert advisories["items"] == []
        assert advisories["reason"] == "execution_ai_advisory_missing"
        assert "no such table" not in json.dumps(advisories).lower()

        status, _headers, validation = _get_json(f"{base_url}/api/validation", token=token)
        assert status == 200
        assert validation["ok"] is True
        assert validation["rows"] == []
        assert validation["reason"] == "validation_scores_missing"
        assert "no such table" not in json.dumps(validation).lower()

        status, _headers, bad_audit_table = _get_json_error(
            f"{base_url}/api/audit/records?table=alerts",
            token=token,
        )
        assert status == 400
        assert bad_audit_table["ok"] is False
        assert bad_audit_table["error"] == "not_audit_table"
        assert bad_audit_table["reason"] == "not_audit_table"

        injected = quote("portfolio_state;DROP TABLE x;", safe="")
        status, _headers, injected_table = _get_json_error(
            f"{base_url}/api/audit/records?table={injected}",
            token=token,
        )
        assert status == 400
        assert injected_table["ok"] is False
        assert injected_table["error"] == "unauthorized_table"
        assert injected_table["reason"] == "unauthorized_table"

        status, _headers, audit_records = _get_json(
            f"{base_url}/api/audit/records?table=trade_attribution_ledger",
            token=token,
        )
        assert status == 200
        assert audit_records["ok"] is True
        assert audit_records["table"] == "trade_attribution_ledger"
        assert isinstance(audit_records["records"], list)
