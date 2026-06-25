from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import pytest

from tools import production_readiness_gate as gate_mod


def _response(status: int, body: dict[str, Any]) -> dict[str, Any]:
    return {"status": int(status), "body": dict(body)}


class FakeHttpClient:
    def __init__(self, routes: dict[tuple[str, str], Any]) -> None:
        self.routes = dict(routes)
        self.requests: list[tuple[str, str, dict[str, Any]]] = []

    def get(self, url: str, **_: Any) -> dict[str, Any]:
        return self._request("GET", url, {})

    def post(self, url: str, *, body: dict[str, Any] | None = None, **_: Any) -> dict[str, Any]:
        return self._request("POST", url, dict(body or {}))

    def _request(self, method: str, url: str, body: dict[str, Any]) -> dict[str, Any]:
        path = urlsplit(url).path
        self.requests.append((method, path, dict(body)))
        route = self.routes.get((method, path))
        if route is None:
            return _response(404, {"ok": False, "error": "missing_fake_route", "path": path})
        if callable(route):
            route = route(method, path, body)
        return copy.deepcopy(route)


@dataclass
class FakeProcess:
    log: str = ""
    shutdown_result: dict[str, Any] | None = None
    listener_result: dict[str, bool] | None = None
    start_result: dict[str, Any] | None = None
    boot_result: dict[str, Any] | None = None
    dashboard_token: str = "dashboard-token"
    operator_token: str = "operator-token"

    def start(self) -> dict[str, Any]:
        return dict(self.start_result or {"ok": True, "pid": 111, "runtime_dir": "/tmp/gate"})

    def wait_for_boot(self, timeout_s: float) -> dict[str, Any]:
        return dict(self.boot_result or {"ok": True, "runtime_pid": 222, "timeout_s": timeout_s})

    def log_text(self) -> str:
        return self.log

    def shutdown_runtime_sigterm(self, timeout_s: float) -> dict[str, Any]:
        return dict(
            self.shutdown_result
            or {"ok": True, "pid": 222, "signal": "SIGTERM", "killed": False, "elapsed_s": min(0.25, timeout_s)}
        )

    def stop_launcher(self, timeout_s: float = 15.0) -> dict[str, Any]:
        return {"ok": True, "name": "start_all", "timeout_s": timeout_s}

    def listeners_open(self) -> dict[str, bool]:
        return dict(self.listener_result or {"dashboard_port_open": False, "operator_port_open": False})


def _healthy_routes() -> dict[tuple[str, str], Any]:
    heartbeat_ts_ms = gate_mod._now_ms() + 60_000
    return {
        ("GET", "/api/operator/ping"): _response(200, {"ok": True}),
        ("GET", "/api/health"): _response(200, {"ok": True}),
        ("GET", "/api/system/kill_switches"): _response(
            200,
            {"ok": True, "kill_switches": {"global": {"enabled": True, "source": "env"}}},
        ),
        ("GET", "/api/broker/config"): _response(
            200,
            {"ok": True, "config": {"active_broker": "sim", "paper_live_mode": "safe"}},
        ),
        ("GET", "/api/execution/barrier"): _response(
            200,
            {
                "ok": True,
                "execution_barrier": {
                    "mode": "safe",
                    "allowed": False,
                    "real_trading_allowed": False,
                    "reason": "mode_safe",
                },
            },
        ),
        ("GET", "/api/operator/readiness_evidence"): _response(
            200,
            {
                "ok": True,
                "mode": "safe",
                "execution_mode": "safe",
                "target_broker": "sim",
                "summary": {"blocked": 0},
            },
        ),
        ("GET", "/api/db/health"): _response(
            200,
            {"ok": True, "liveness": "ok", "row_counts": {"runtime_meta": 1, "job_heartbeats": 1}},
        ),
        ("POST", "/api/jobs/start"): _response(
            200,
            {"ok": True, "job": "provider_monitor", "status": "started"},
        ),
        ("GET", "/api/jobs/catalog"): _response(
            200,
            {
                "ok": True,
                "jobs": [
                    {
                        "name": "provider_monitor",
                        "running": True,
                        "heartbeat_ts_ms": heartbeat_ts_ms,
                        "heartbeat_age_s": 0.1,
                        "heartbeat_missing": True,
                        "heartbeat_source": "job_heartbeats",
                        "status": "RUNNING",
                    }
                ],
            },
        ),
        ("GET", "/api/jobs/log"): _response(200, {"ok": True, "lines": ["provider_monitor heartbeat ok"]}),
        ("POST", "/api/terminal/order"): _response(
            403,
            {
                "ok": False,
                "error": "execution_blocked",
                "reason_code": gate_mod.DISABLE_LIVE_EXECUTION_REASON,
                "gate": {"mode": "safe", "real_trading_allowed": False, "allowed": False},
            },
        ),
    }


def _run_gate(
    routes: dict[tuple[str, str], Any],
    *,
    process: FakeProcess | None = None,
    request_timeout_s: float = 0.01,
) -> dict[str, Any]:
    gate = gate_mod.ProductionReadinessGate(
        dashboard_base="http://127.0.0.1:8000",
        operator_base="http://127.0.0.1:4001",
        dashboard_token="",
        operator_token="",
        job_name=gate_mod.DEFAULT_JOB_NAME,
        http_client=FakeHttpClient(routes),
        process=process or FakeProcess(),
        request_timeout_s=request_timeout_s,
        boot_timeout_s=1.0,
        shutdown_timeout_s=1.0,
    )
    return gate.run()


def test_safe_boot_env_scrub_removes_external_service_dependencies() -> None:
    env = {
        "TS_PG_DSN": "host=127.0.0.1 port=5432 dbname=trading",
        "TIMESCALE_DSN": "postgresql://127.0.0.1/trading",
        "LIVE_CACHE_REDIS_URL": "redis://127.0.0.1:6379/0",
        "OBJECT_STORE_ENDPOINT": "http://127.0.0.1:9000",
        "TS_STORAGE_BACKEND": "postgres",
        "PREFLIGHT_REQUIRE_TIMESCALE": "1",
    }

    gate_mod._scrub_safe_boot_external_service_env(env)

    assert "TS_PG_DSN" not in env
    assert "TIMESCALE_DSN" not in env
    assert "LIVE_CACHE_REDIS_URL" not in env
    assert "OBJECT_STORE_ENDPOINT" not in env
    assert env["TS_STORAGE_BACKEND"] == "sqlite"
    assert env["LIVE_CACHE_BACKEND"] == "memory"
    assert env["PREFLIGHT_REQUIRE_TIMESCALE"] == "0"


def test_production_readiness_gate_passes_healthy_safe_boot() -> None:
    report = _run_gate(_healthy_routes())

    assert report["ok"] is True
    assert gate_mod.exit_code(report) == 0
    assert report["failed_invariants"] == []
    assert {check["status"] for check in report["checks"]} == {"PASS"}
    assert {check["id"] for check in report["checks"]} == {
        "start_all_ports",
        "credential_crash_free",
        "kill_switches_available",
        "broker_sim",
        "execution_barrier_safe",
        "readiness_target_safe_sim",
        "db_health_ok",
        "write_job_started",
        "write_job_heartbeat",
        "write_job_no_set_local_parameter_errors",
        "terminal_order_blocked",
        "persistence_readback",
        "bounded_sigterm_shutdown",
        "listeners_closed",
    }


@pytest.mark.parametrize(
    ("fault_name", "expected_invariant"),
    [
        ("broker_not_sim", "broker_sim"),
        ("barrier_allows_execution", "execution_barrier_safe"),
        ("db_health_down", "db_health_ok"),
        ("write_job_wont_start", "write_job_started"),
        ("terminal_order_allowed", "terminal_order_blocked"),
        ("set_local_parameter_error", "write_job_no_set_local_parameter_errors"),
    ],
)
def test_production_readiness_gate_fails_closed_on_faults(fault_name: str, expected_invariant: str) -> None:
    routes = _healthy_routes()
    if fault_name == "broker_not_sim":
        routes[("GET", "/api/broker/config")] = _response(
            200,
            {"ok": True, "config": {"active_broker": "ibkr", "paper_live_mode": "safe"}},
        )
    elif fault_name == "barrier_allows_execution":
        routes[("GET", "/api/execution/barrier")] = _response(
            200,
            {
                "ok": True,
                "execution_barrier": {"mode": "safe", "allowed": True, "real_trading_allowed": False},
            },
        )
    elif fault_name == "db_health_down":
        routes[("GET", "/api/db/health")] = _response(
            503,
            {"ok": False, "liveness": "down", "error": "db_unreachable", "row_counts": {}},
        )
    elif fault_name == "write_job_wont_start":
        routes[("POST", "/api/jobs/start")] = _response(
            500,
            {"ok": False, "error": "job_start_failed"},
        )
        routes[("GET", "/api/jobs/catalog")] = _response(200, {"ok": True, "jobs": []})
    elif fault_name == "terminal_order_allowed":
        routes[("POST", "/api/terminal/order")] = _response(
            200,
            {"ok": True, "gate": {"mode": "safe", "real_trading_allowed": False}},
        )
    elif fault_name == "set_local_parameter_error":
        routes[("GET", "/api/jobs/log")] = _response(
            200,
            {"ok": True, "lines": ["psycopg error: SET LOCAL $1"]},
        )

    report = _run_gate(routes)

    assert report["ok"] is False
    assert gate_mod.exit_code(report) == 1
    assert expected_invariant in report["failed_invariants"]


def test_production_readiness_gate_fails_closed_when_shutdown_is_unbounded() -> None:
    process = FakeProcess(
        shutdown_result={
            "ok": False,
            "pid": 222,
            "signal": "SIGTERM",
            "killed": True,
            "elapsed_s": 30.0,
            "error": "runtime_shutdown_timeout",
        }
    )

    report = _run_gate(_healthy_routes(), process=process)

    assert report["ok"] is False
    assert gate_mod.exit_code(report) == 1
    assert "bounded_sigterm_shutdown" in report["failed_invariants"]
