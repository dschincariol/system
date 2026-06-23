from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from tools import safe_sim_boot_smoke


class _TokenHandler(BaseHTTPRequestHandler):
    expected_token = "dashboard-token-canary-123"

    def log_message(self, *_args):
        return

    def do_GET(self):
        if self.headers.get("X-API-Token") != self.expected_token:
            body = b'{"ok":false,"error":"unauthorized"}'
            self.send_response(401)
        else:
            body = b'{"ok":true,"gate":"safe"}'
            self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def test_prepare_safe_sim_env_moves_inline_secrets_to_files(tmp_path: Path) -> None:
    canary_dashboard = "dashboard-canary-secret-000000000000"
    canary_operator = "operator-canary-secret-000000000000"
    canary_master = "master-canary-secret-000000000000"
    canary_pg = "pg-canary-secret-000000000000"
    canary_redis = "redis-canary-secret-000000000000"
    base_env = tmp_path / ".env.codex-sim-paper.bak"
    base_env.write_text(
        "\n".join(
            [
                "ENGINE_MODE=live",
                "EXECUTION_MODE=live",
                "BROKER=alpaca",
                f"DASHBOARD_API_TOKEN={canary_dashboard}",
                f"OPERATOR_API_TOKEN={canary_operator}",
                f"DATA_SOURCE_MASTER_KEY={canary_master}",
                f"TS_PG_DSN=postgresql://app:{canary_pg}@127.0.0.1:5432/trading",
                f"REDIS_URL=redis://:{canary_redis}@127.0.0.1:6379/0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    prepared = safe_sim_boot_smoke.prepare_safe_sim_env(
        base_env_path=base_env,
        runtime_dir=tmp_path / "runtime",
        dashboard_port=18000,
        operator_port=14001,
    )

    env = prepared.env
    assert env["ENGINE_MODE"] == "safe"
    assert env["EXECUTION_MODE"] == "safe"
    assert env["DISABLE_LIVE_EXECUTION"] == "1"
    assert env["KILL_SWITCH_GLOBAL"] == "1"
    assert env["LIVE_TRADING_CONFIRM"] == ""
    assert env["LIVE_TRADING_REQUIRE_CONFIRMATION"] == "1"
    assert env["BROKER"] == "sim"
    assert env["BROKER_NAME"] == "sim"
    assert env["ENV"] == "dev"
    assert env["PROD_LOCK"] == "0"
    assert env["TS_STORAGE_BACKEND"] == "sqlite"
    assert env["PRICE_READ_BACKEND"] == "sqlite"
    assert env["TELEMETRY_READ_BACKEND"] == "sqlite"
    assert "DASHBOARD_API_TOKEN" not in env
    assert "OPERATOR_API_TOKEN" not in env
    assert Path(env["DASHBOARD_API_TOKEN_FILE"]).read_text(encoding="utf-8").strip() == canary_dashboard
    assert Path(env["OPERATOR_API_TOKEN_FILE"]).read_text(encoding="utf-8").strip() == canary_operator
    assert Path(env["DATA_SOURCE_MASTER_KEY_FILE"]).read_text(encoding="utf-8").strip() == canary_master
    assert Path(env["TS_PG_PASSWORD_FILE"]).read_text(encoding="utf-8").strip() == canary_pg
    assert Path(env["REDIS_PASSWORD_FILE"]).read_text(encoding="utf-8").strip() == canary_redis
    assert canary_pg not in env["TS_PG_DSN"]
    assert canary_redis not in env["REDIS_URL"]

    rendered_env = prepared.env_file.read_text(encoding="utf-8")
    rendered_metadata = json.dumps(prepared.metadata, sort_keys=True)
    for canary in (canary_dashboard, canary_operator, canary_master, canary_pg, canary_redis):
        assert canary not in rendered_env
        assert canary not in rendered_metadata


def test_http_json_uses_masked_header_token_flow() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _TokenHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{server.server_port}/api/system/kill_switches"
        denied = safe_sim_boot_smoke._http_json(url, token="", timeout_s=5)
        allowed = safe_sim_boot_smoke._http_json(url, token=_TokenHandler.expected_token, timeout_s=5)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert denied["status"] == 401
    assert allowed["status"] == 200
    assert allowed["body"]["ok"] is True
    assert _TokenHandler.expected_token not in json.dumps(allowed, sort_keys=True)


def test_gate_summary_requires_populated_safe_sim_shapes() -> None:
    broker = safe_sim_boot_smoke._summarize_gate_response(
        "/api/broker/config",
        {
            "status": 200,
            "body": {
                "ok": True,
                "config": {
                    "active_broker": "sim",
                    "paper_live_mode": "safe",
                    "secrets_masked": True,
                },
            },
        },
    )
    barrier = safe_sim_boot_smoke._summarize_gate_response(
        "/api/execution/barrier",
        {
            "status": 200,
            "body": {
                "ok": False,
                "allowed": False,
                "reason": "mode_safe",
                "reasons": ["mode_safe"],
                "execution_barrier": {"allowed": False, "reason": "mode_safe"},
            },
        },
    )
    readiness = safe_sim_boot_smoke._summarize_gate_response(
        "/api/operator/readiness_evidence",
        {
            "status": 200,
            "body": {
                "ok": True,
                "status": "warning",
                "target_broker": "sim",
                "items": [{"id": "execution.barrier"}],
                "summary": {"blocking": 0},
            },
        },
    )
    live_broker = safe_sim_boot_smoke._summarize_gate_response(
        "/api/broker/config",
        {
            "status": 200,
            "body": {
                "ok": True,
                "config": {"active_broker": "alpaca", "paper_live_mode": "live"},
            },
        },
    )

    assert broker["http_ok"] is True
    assert broker["populated"] is True
    assert broker["safe"] is True
    assert barrier["http_ok"] is True
    assert barrier["populated"] is True
    assert barrier["safe"] is True
    assert barrier["allowed"] is False
    assert readiness["populated"] is True
    assert readiness["safe"] is True
    assert live_broker["populated"] is True
    assert live_broker["safe"] is False
