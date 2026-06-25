from __future__ import annotations

"""Fail-closed safe/shadow production go-live readiness gate."""

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.safe_sim_boot_smoke import (  # noqa: E402
    _http_request_json,
    _port_open,
    _read_token_file,
    _terminate_process,
    _write_env_file,
    prepare_safe_sim_env,
)


DEFAULT_JOB_NAME = "provider_monitor"
DISABLE_LIVE_EXECUTION_REASON = "disable_live_execution_env"
SET_LOCAL_PARAMETER_ERROR = "SET LOCAL $1"
SAFE_BOOT_EXTERNAL_SERVICE_ENV_KEYS = (
    "TS_PG_DSN",
    "TS_PG_DSN_FILE",
    "PG_DSN",
    "PGPASSWORD",
    "PGPASSWORD_FILE",
    "TIMESCALE_DSN",
    "TIMESCALE_URL",
    "TIMESCALE_DATABASE_URL",
    "TIMESCALE_PASSWORD_FILE",
    "TIMESCALE_PRICES_DSN",
    "TIMESCALE_PRICES_URL",
    "TIMESCALE_PRICES_DATABASE_URL",
    "TIMESCALE_PRICES_PASSWORD_FILE",
    "DATABASE_URL",
    "POSTGRES_DSN",
    "POSTGRES_URL",
    "LIVE_CACHE_REDIS_URL",
    "REDIS_URL",
    "REDIS_CACHE_URL",
    "TS_REDIS_URL",
    "REDIS_PASSWORD_FILE",
    "TS_REDIS_PASSWORD_FILE",
    "LIVE_CACHE_REDIS_PASSWORD_FILE",
    "OBJECT_STORE_ENDPOINT",
    "OBJECT_STORE_BUCKET",
    "OBJECT_STORE_ACCESS_KEY",
    "OBJECT_STORE_ACCESS_KEY_FILE",
    "OBJECT_STORE_SECRET_KEY",
    "OBJECT_STORE_SECRET_KEY_FILE",
    "OBJECT_STORE_SESSION_TOKEN",
    "MINIO_ENDPOINT",
    "MINIO_BUCKET",
    "MINIO_ACCESS_KEY",
    "MINIO_ACCESS_KEY_FILE",
    "MINIO_SECRET_KEY",
    "MINIO_SECRET_KEY_FILE",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
)
SAFE_BOOT_LOCAL_SERVICE_DEFAULTS = {
    "TS_STORAGE_BACKEND": "sqlite",
    "TIMESCALE_ENABLED": "0",
    "TIMESCALE_PRICES_ENABLED": "0",
    "TELEMETRY_READ_BACKEND": "sqlite",
    "PRICE_READ_BACKEND": "sqlite",
    "LIVE_CACHE_BACKEND": "memory",
    "PREFLIGHT_REQUIRE_TIMESCALE": "0",
    "PREFLIGHT_REQUIRE_REDIS": "0",
    "PREFLIGHT_REQUIRE_OBJECT_STORAGE": "0",
}


def _scrub_safe_boot_external_service_env(env: dict[str, str]) -> None:
    for key in SAFE_BOOT_EXTERNAL_SERVICE_ENV_KEYS:
        env.pop(key, None)
    env.update(SAFE_BOOT_LOCAL_SERVICE_DEFAULTS)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _body(response: dict[str, Any]) -> dict[str, Any]:
    value = response.get("body")
    return dict(value) if isinstance(value, dict) else {}


def _status(response: dict[str, Any]) -> int:
    try:
        return int(response.get("status") or 0)
    except Exception:
        return 0


def _json_safe(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except Exception:
        return str(value)


def _job_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("jobs")
    if not isinstance(rows, list):
        rows = payload.get("catalog")
    return [dict(row) for row in rows or [] if isinstance(row, dict)]


def _find_job(payload: dict[str, Any], job_name: str) -> dict[str, Any]:
    target = str(job_name or "").strip()
    for row in _job_rows(payload):
        if str(row.get("name") or row.get("id") or "").strip() == target:
            return row
    return {}


def _pid_running(pid: int) -> bool:
    if int(pid or 0) <= 0:
        return False
    try:
        os.kill(int(pid), 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False


def _credential_crash_lines(text: str) -> list[str]:
    lines: list[str] = []
    explicit = {
        "wrong_credentials",
        "credentials_rejected",
        "credentials_directory_missing",
        "credential_decryption_failed",
        "credential_load_failed",
    }
    for raw in str(text or "").splitlines():
        line = raw.strip()
        lowered = line.lower()
        if any(item in lowered for item in explicit):
            lines.append(line[:500])
            continue
        if "credential" not in lowered and "credentials" not in lowered:
            continue
        if any(term in lowered for term in ("startup_crash", "fatal", "traceback", "exception", "crash")):
            lines.append(line[:500])
    return lines


@dataclass
class Check:
    id: str
    ok: bool
    summary: str
    observed: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "status": "PASS" if self.ok else "FAIL",
            "ok": bool(self.ok),
            "summary": self.summary,
            "observed": _json_safe(self.observed),
        }


class JsonHttpClient:
    def get(self, url: str, *, token: str = "", operator_token: str = "", timeout_s: float = 5.0) -> dict[str, Any]:
        return _http_request_json(
            url,
            method="GET",
            token=token,
            operator_token=operator_token,
            timeout_s=timeout_s,
        )

    def post(
        self,
        url: str,
        *,
        token: str = "",
        operator_token: str = "",
        timeout_s: float = 5.0,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return _http_request_json(
            url,
            method="POST",
            token=token,
            operator_token=operator_token,
            timeout_s=timeout_s,
            body=dict(body or {}),
        )


class StartAllSafeBoot:
    def __init__(self, args: argparse.Namespace, http_client: JsonHttpClient) -> None:
        self.args = args
        self.http = http_client
        self.runtime_dir = Path(args.runtime_dir).resolve()
        self.proc: subprocess.Popen | None = None
        self.prepared_env: dict[str, str] = {}
        self.dashboard_token = ""
        self.operator_token = ""
        self.dashboard_base = f"http://127.0.0.1:{int(args.dashboard_port)}"
        self.operator_base = f"http://127.0.0.1:{int(args.operator_port)}"
        self.start_all_stdout = self.runtime_dir / "log" / "production_readiness_start_all.stdout.log"
        self.start_all_stderr = self.runtime_dir / "log" / "production_readiness_start_all.stderr.log"
        self.runtime_pid_path = self.runtime_dir / "log" / "runtime.pid"

    def start(self) -> dict[str, Any]:
        prepared = prepare_safe_sim_env(
            base_env_path=Path(self.args.base_env).resolve(),
            runtime_dir=self.runtime_dir,
            dashboard_port=int(self.args.dashboard_port),
            operator_port=int(self.args.operator_port),
        )
        prepared.env.update(
            {
                "TRADING_ENV_FILE": str(prepared.env_file),
                "OPERATOR_ENV_PATH": str(prepared.env_file),
                "OPERATOR_AUTO_START": "1",
                "OPERATOR_AUTO_START_DELAY_MS": "0",
                "OPERATOR_DISABLE_INTERNAL_ENGINE_START": "0",
                "OPERATOR_AUTORESTART": "0",
                "OPERATOR_EXTERNAL_RUNTIME_CACHE_TTL_MS": "0",
                "OPEN_DASHBOARD_BROWSER_ON_START": "0",
                "API_JOBS_CACHE_TTL_S": "0",
                "TRADING_VALIDATION_MODE": "startup",
                "DATA_SOURCE_MANAGER_READ_ONLY": "1",
                "PROVIDER_MONITOR_HEARTBEAT_S": "1",
                "PROVIDER_MONITOR_CHECK_S": "1",
                "JOB_LOCK_STALE_AFTER_S": "30",
                "RUNTIME_SHUTDOWN_HARD_DEADLINE_S": str(float(self.args.runtime_shutdown_deadline_s)),
            }
        )
        _scrub_safe_boot_external_service_env(prepared.env)
        _write_env_file(prepared.env_file, prepared.env)
        self.prepared_env = dict(prepared.env)
        self.dashboard_token = _read_token_file(self.prepared_env.get("DASHBOARD_API_TOKEN_FILE", ""))
        self.operator_token = _read_token_file(self.prepared_env.get("OPERATOR_API_TOKEN_FILE", ""))

        dashboard_busy = _port_open("127.0.0.1", int(self.args.dashboard_port))
        operator_busy = _port_open("127.0.0.1", int(self.args.operator_port))
        if dashboard_busy or operator_busy:
            return {
                "ok": False,
                "error": "port_already_in_use",
                "dashboard_port_open": dashboard_busy,
                "operator_port_open": operator_busy,
            }

        for path in (self.start_all_stdout, self.start_all_stderr):
            try:
                path.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                pass
            path.parent.mkdir(parents=True, exist_ok=True)

        env = {key: value for key, value in os.environ.items() if key not in SAFE_BOOT_EXTERNAL_SERVICE_ENV_KEYS}
        env.update(self.prepared_env)
        env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")

        stdout_fh = self.start_all_stdout.open("ab")
        stderr_fh = self.start_all_stderr.open("ab")
        try:
            self.proc = subprocess.Popen(
                [sys.executable, "start_all.py"],
                cwd=str(ROOT),
                env=env,
                stdout=stdout_fh,
                stderr=stderr_fh,
            )
        finally:
            stdout_fh.close()
            stderr_fh.close()

        return {"ok": True, "pid": int(self.proc.pid), "runtime_dir": str(self.runtime_dir)}

    def wait_for_boot(self, timeout_s: float) -> dict[str, Any]:
        deadline = time.monotonic() + max(1.0, float(timeout_s))
        operator: dict[str, Any] = {}
        dashboard: dict[str, Any] = {}
        runtime_pid = 0
        while time.monotonic() < deadline:
            if self.proc is not None and self.proc.poll() is not None:
                return {
                    "ok": False,
                    "error": "start_all_exited_early",
                    "returncode": self.proc.returncode,
                    "operator": operator,
                    "dashboard": dashboard,
                }
            operator = self.http.get(f"{self.operator_base}/api/operator/ping", timeout_s=2.0)
            if _status(operator) == 200:
                dashboard = self.http.get(
                    f"{self.dashboard_base}/api/health",
                    token=self.dashboard_token,
                    timeout_s=3.0,
                )
                runtime_pid = self.runtime_pid()
                if _status(dashboard) == 200 and runtime_pid > 0:
                    return {
                        "ok": True,
                        "operator": {"status": _status(operator), "ok": bool(_body(operator).get("ok"))},
                        "dashboard": {"status": _status(dashboard), "ok": _body(dashboard).get("ok")},
                        "runtime_pid": int(runtime_pid),
                    }
            time.sleep(0.5)
        return {
            "ok": False,
            "error": "safe_boot_timeout",
            "operator": {"status": _status(operator), "body": _body(operator)},
            "dashboard": {"status": _status(dashboard), "body": _body(dashboard)},
            "runtime_pid": int(runtime_pid),
        }

    def runtime_pid(self) -> int:
        try:
            raw = self.runtime_pid_path.read_text(encoding="utf-8")
            parsed = json.loads(raw)
            pid = int(parsed.get("pid") if isinstance(parsed, dict) else raw)
            return pid if _pid_running(pid) else 0
        except Exception:
            return 0

    def log_text(self) -> str:
        paths = [
            self.start_all_stdout,
            self.start_all_stderr,
            self.runtime_dir / "log" / "operator.stdout.log",
            self.runtime_dir / "log" / "operator.stderr.log",
            self.runtime_dir / "log" / "runtime.log",
            self.runtime_dir / "log" / "engine_stderr.log",
        ]
        chunks: list[str] = []
        for path in paths:
            try:
                chunks.append(f"\n--- {path.name} ---\n")
                chunks.append(path.read_text(encoding="utf-8", errors="replace")[-20000:])
            except OSError:
                continue
        return "".join(chunks)

    def shutdown_runtime_sigterm(self, timeout_s: float) -> dict[str, Any]:
        pid = self.runtime_pid()
        if pid <= 0:
            return {"ok": False, "error": "runtime_pid_unavailable", "killed": False, "elapsed_s": 0.0}
        started = time.monotonic()
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            return {"ok": True, "pid": int(pid), "already_exited": True, "killed": False, "elapsed_s": 0.0}
        except Exception as exc:
            return {
                "ok": False,
                "pid": int(pid),
                "error": f"{type(exc).__name__}:{exc}",
                "killed": False,
                "elapsed_s": 0.0,
            }

        deadline = started + max(0.5, float(timeout_s))
        while time.monotonic() < deadline:
            if not _pid_running(pid):
                return {
                    "ok": True,
                    "pid": int(pid),
                    "signal": "SIGTERM",
                    "killed": False,
                    "elapsed_s": round(time.monotonic() - started, 3),
                }
            time.sleep(0.25)

        try:
            os.kill(pid, signal.SIGKILL)
        except Exception:  # no-op-guard: allow - process may already have exited before SIGKILL.
            pass
        return {
            "ok": False,
            "pid": int(pid),
            "signal": "SIGTERM",
            "killed": True,
            "elapsed_s": round(time.monotonic() - started, 3),
            "error": "runtime_shutdown_timeout",
        }

    def stop_launcher(self, timeout_s: float = 15.0) -> dict[str, Any]:
        return _terminate_process(self.proc, name="start_all", timeout_s=float(timeout_s))

    def listeners_open(self) -> dict[str, bool]:
        return {
            "dashboard_port_open": _port_open("127.0.0.1", int(self.args.dashboard_port)),
            "operator_port_open": _port_open("127.0.0.1", int(self.args.operator_port)),
        }


class ProductionReadinessGate:
    def __init__(
        self,
        *,
        dashboard_base: str,
        operator_base: str,
        dashboard_token: str,
        operator_token: str,
        job_name: str,
        http_client: Any,
        process: Any,
        request_timeout_s: float = 10.0,
        boot_timeout_s: float = 180.0,
        shutdown_timeout_s: float = 30.0,
    ) -> None:
        self.dashboard_base = dashboard_base.rstrip("/")
        self.operator_base = operator_base.rstrip("/")
        self.dashboard_token = dashboard_token
        self.operator_token = operator_token
        self.job_name = str(job_name or DEFAULT_JOB_NAME)
        self.http = http_client
        self.process = process
        self.request_timeout_s = float(request_timeout_s)
        self.boot_timeout_s = float(boot_timeout_s)
        self.shutdown_timeout_s = float(shutdown_timeout_s)
        self.checks: list[Check] = []
        self._last_jobs_catalog: dict[str, Any] = {}
        self._last_db_health: dict[str, Any] = {}

    def _record(self, check_id: str, ok: bool, summary: str, observed: dict[str, Any] | None = None) -> None:
        self.checks.append(Check(id=str(check_id), ok=bool(ok), summary=str(summary), observed=dict(observed or {})))

    def _get_dashboard(self, path: str) -> dict[str, Any]:
        return self.http.get(
            f"{self.dashboard_base}{path}",
            token=self.dashboard_token,
            timeout_s=self.request_timeout_s,
        )

    def _get_operator(self, path: str) -> dict[str, Any]:
        return self.http.get(
            f"{self.operator_base}{path}",
            operator_token=self.operator_token,
            timeout_s=self.request_timeout_s,
        )

    def _post_dashboard(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        return self.http.post(
            f"{self.dashboard_base}{path}",
            token=self.dashboard_token,
            timeout_s=self.request_timeout_s,
            body=dict(body or {}),
        )

    def _check_boot(self) -> None:
        start = self.process.start()
        boot = self.process.wait_for_boot(self.boot_timeout_s) if bool(start.get("ok")) else start
        self.dashboard_token = str(getattr(self.process, "dashboard_token", self.dashboard_token) or "")
        self.operator_token = str(getattr(self.process, "operator_token", self.operator_token) or "")
        operator = self._get_operator("/api/operator/ping") if bool(boot.get("ok")) else {}
        health = self._get_dashboard("/api/health") if bool(boot.get("ok")) else {}
        ok = bool(start.get("ok")) and bool(boot.get("ok")) and _status(operator) == 200 and _status(health) == 200
        self._record(
            "start_all_ports",
            ok,
            "start_all.py boot exposes operator :4001 and dashboard :8000 safe endpoints",
            {
                "start": start,
                "boot": boot,
                "operator_status": _status(operator),
                "dashboard_health_status": _status(health),
            },
        )

        crash_lines = _credential_crash_lines(self.process.log_text())
        self._record(
            "credential_crash_free",
            len(crash_lines) == 0,
            "safe boot has zero credential-related startup crashes",
            {"credential_crash_count": len(crash_lines), "matches": crash_lines[:10]},
        )

    def _check_safety_gates(self) -> None:
        kill = self._get_dashboard("/api/system/kill_switches")
        kill_body = _body(kill)
        kill_switches = kill_body.get("kill_switches")
        kill_snapshot = kill_switches if isinstance(kill_switches, dict) else kill_body.get("data")
        self._record(
            "kill_switches_available",
            _status(kill) == 200 and isinstance(kill_snapshot, dict),
            "/api/system/kill_switches returns a populated safety snapshot",
            {
                "status": _status(kill),
                "keys": sorted(list((kill_snapshot or {}).keys())) if isinstance(kill_snapshot, dict) else [],
            },
        )

        broker = self._get_dashboard("/api/broker/config")
        broker_body = _body(broker)
        config = broker_body.get("config") if isinstance(broker_body.get("config"), dict) else {}
        active_broker = str((config or {}).get("active_broker") or "").strip().lower()
        paper_live_mode = str((config or {}).get("paper_live_mode") or "").strip().lower()
        self._record(
            "broker_sim",
            _status(broker) == 200 and active_broker == "sim" and paper_live_mode != "live",
            "/api/broker/config pins the active broker to sim",
            {"status": _status(broker), "active_broker": active_broker, "paper_live_mode": paper_live_mode},
        )

        barrier = self._get_dashboard("/api/execution/barrier")
        barrier_body = _body(barrier)
        execution_barrier = barrier_body.get("execution_barrier")
        gate = execution_barrier if isinstance(execution_barrier, dict) else barrier_body
        mode = str((gate or {}).get("mode") or barrier_body.get("mode") or "").strip().lower()
        real_trading_allowed = bool((gate or {}).get("real_trading_allowed", barrier_body.get("real_trading_allowed", False)))
        allowed = bool((gate or {}).get("allowed", barrier_body.get("allowed", False)))
        self._record(
            "execution_barrier_safe",
            _status(barrier) == 200 and mode == "safe" and not real_trading_allowed and not allowed,
            "/api/execution/barrier reports safe mode and no real trading permission",
            {
                "status": _status(barrier),
                "mode": mode,
                "allowed": allowed,
                "real_trading_allowed": real_trading_allowed,
                "reason": str((gate or {}).get("reason") or barrier_body.get("reason") or ""),
            },
        )

        readiness = self._get_dashboard("/api/operator/readiness_evidence")
        readiness_body = _body(readiness)
        target_mode = str(readiness_body.get("mode") or "").strip().lower()
        target_execution_mode = str(readiness_body.get("execution_mode") or "").strip().lower()
        target_broker = str(readiness_body.get("target_broker") or "").strip().lower()
        self._record(
            "readiness_target_safe_sim",
            _status(readiness) == 200 and target_mode == "safe" and target_execution_mode == "safe" and target_broker == "sim",
            "/api/operator/readiness_evidence targets sim/safe readiness",
            {
                "status": _status(readiness),
                "mode": target_mode,
                "execution_mode": target_execution_mode,
                "target_broker": target_broker,
                "summary": readiness_body.get("summary") if isinstance(readiness_body.get("summary"), dict) else {},
            },
        )

    def _check_db_health(self, *, record: bool = True) -> None:
        health = self._get_dashboard("/api/db/health")
        body = _body(health)
        self._last_db_health = body
        if record:
            self._record(
                "db_health_ok",
                _status(health) == 200 and body.get("ok") is True and str(body.get("liveness") or "") == "ok",
                "/api/db/health returns HTTP 200 with ok=true",
                {
                    "status": _status(health),
                    "ok": body.get("ok"),
                    "liveness": body.get("liveness"),
                    "error": body.get("error"),
                },
            )

    def _wait_for_job_heartbeat(self, started_ms: int) -> dict[str, Any]:
        deadline = time.monotonic() + max(1.0, min(30.0, self.request_timeout_s * 3.0))
        last: dict[str, Any] = {}
        while time.monotonic() < deadline:
            response = self._get_dashboard("/api/jobs/catalog")
            body = _body(response)
            last = {"response": response, "body": body, "job": _find_job(body, self.job_name)}
            job = dict(last.get("job") or {})
            hb_ts_ms = int(job.get("heartbeat_ts_ms") or 0)
            if bool(job.get("running")) and hb_ts_ms > 0 and hb_ts_ms >= int(started_ms - 1000):
                return last
            time.sleep(0.5)
        return last

    def _check_write_job(self) -> None:
        started_ms = _now_ms()
        start_response = self._post_dashboard(
            "/api/jobs/start",
            {
                "name": self.job_name,
                "confirmation": "JOB_ACTION",
                "consequence_ack": True,
                "actor": "production_readiness_gate",
                "reason": "go_live_readiness_gate",
            },
        )
        start_body = _body(start_response)
        start_ok = _status(start_response) == 200 and bool(start_body.get("ok", True))
        self._record(
            "write_job_started",
            start_ok,
            "a non-execution write job starts through /api/jobs/start",
            {"status": _status(start_response), "body": start_body, "job": self.job_name},
        )

        observed = self._wait_for_job_heartbeat(started_ms)
        catalog_body = dict(observed.get("body") or {})
        job = dict(observed.get("job") or {})
        self._last_jobs_catalog = catalog_body
        heartbeat_ts_ms = int(job.get("heartbeat_ts_ms") or 0)
        heartbeat_ok = bool(job.get("running")) and heartbeat_ts_ms >= int(started_ms - 1000)
        self._record(
            "write_job_heartbeat",
            heartbeat_ok,
            "started write job is running with a persisted heartbeat",
            {
                "job": self.job_name,
                "running": bool(job.get("running")),
                "heartbeat_ts_ms": heartbeat_ts_ms,
                "heartbeat_age_s": job.get("heartbeat_age_s"),
                "heartbeat_missing": job.get("heartbeat_missing"),
                "heartbeat_source": job.get("heartbeat_source"),
                "status": job.get("status"),
            },
        )

        log_response = self._get_dashboard(f"/api/jobs/log?name={self.job_name}&tail=400")
        log_body = _body(log_response)
        log_text = json.dumps(log_body, sort_keys=True, default=str) + "\n" + self.process.log_text()
        set_local_count = len(re.findall(re.escape(SET_LOCAL_PARAMETER_ERROR), log_text))
        self._record(
            "write_job_no_set_local_parameter_errors",
            set_local_count == 0,
            "write-job path has zero SET LOCAL $1 errors",
            {"set_local_parameter_error_count": int(set_local_count), "job_log_status": _status(log_response)},
        )

    def _check_terminal_order_block(self) -> None:
        response = self._post_dashboard(
            "/api/terminal/order",
            {
                "symbol": "SPY",
                "side": "BUY",
                "qty": 1,
                "confirmation": "TRADE",
                "consequence_ack": True,
                "actor": "production_readiness_gate",
                "reason": "go_live_readiness_gate_block_probe",
            },
        )
        body = _body(response)
        gate = body.get("gate") if isinstance(body.get("gate"), dict) else {}
        self._record(
            "terminal_order_blocked",
            (
                _status(response) == 403
                and body.get("ok") is False
                and str(body.get("reason_code") or "") == DISABLE_LIVE_EXECUTION_REASON
                and str((gate or {}).get("mode") or "").strip().lower() == "safe"
                and bool((gate or {}).get("real_trading_allowed", True)) is False
            ),
            "/api/terminal/order is blocked with disable_live_execution_env in safe mode",
            {
                "status": _status(response),
                "error": body.get("error"),
                "reason_code": body.get("reason_code"),
                "gate": gate,
            },
        )

    def _check_persistence_readback(self) -> None:
        row_counts = self._last_db_health.get("row_counts") if isinstance(self._last_db_health.get("row_counts"), dict) else {}
        job = _find_job(self._last_jobs_catalog, self.job_name)
        count = row_counts.get("job_heartbeats")
        count_ok = count is None or int(count or 0) > 0
        self._record(
            "persistence_readback",
            count_ok and int(job.get("heartbeat_ts_ms") or 0) > 0,
            "persistence read-back sees the write-job heartbeat through API state",
            {
                "job": self.job_name,
                "job_heartbeats_row_count": count,
                "heartbeat_ts_ms": job.get("heartbeat_ts_ms"),
                "heartbeat_source": job.get("heartbeat_source"),
            },
        )

    def _shutdown_checks(self) -> dict[str, Any]:
        shutdown = self.process.shutdown_runtime_sigterm(self.shutdown_timeout_s)
        ok = (
            bool(shutdown.get("ok"))
            and not bool(shutdown.get("killed"))
            and float(shutdown.get("elapsed_s") or 0.0) <= self.shutdown_timeout_s
        )
        self._record(
            "bounded_sigterm_shutdown",
            ok,
            "engine exits after SIGTERM within the bounded deadline and without SIGKILL",
            shutdown,
        )
        launcher = self.process.stop_launcher(timeout_s=15.0)
        time.sleep(0.5)
        listeners = self.process.listeners_open()
        listeners_ok = not bool(listeners.get("dashboard_port_open")) and not bool(listeners.get("operator_port_open"))
        self._record(
            "listeners_closed",
            listeners_ok,
            "dashboard and operator listeners are closed after bounded shutdown",
            {"launcher": launcher, **listeners},
        )
        return {"runtime": shutdown, "launcher": launcher, "listeners": listeners}

    def run(self) -> dict[str, Any]:
        shutdown: dict[str, Any] = {}
        started_at_ms = _now_ms()
        try:
            self._check_boot()
            self._check_safety_gates()
            self._check_db_health()
            self._check_write_job()
            self._check_terminal_order_block()
            self._check_db_health(record=False)
            self._check_persistence_readback()
        finally:
            shutdown = self._shutdown_checks()

        check_dicts = [check.to_dict() for check in self.checks]
        failed = [item["id"] for item in check_dicts if not item["ok"]]
        return {
            "ok": len(failed) == 0,
            "gate": "production_readiness_gate",
            "mode": "safe",
            "target_broker": "sim",
            "started_ts_ms": int(started_at_ms),
            "completed_ts_ms": _now_ms(),
            "checks": check_dicts,
            "failed_invariants": failed,
            "shutdown": shutdown,
        }


def exit_code(report: dict[str, Any]) -> int:
    return 0 if bool(report.get("ok")) else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-env", default=str(ROOT / ".env.codex-sim-paper.bak"))
    parser.add_argument("--runtime-dir", default=str(ROOT / "var" / "tmp" / "production_readiness_gate"))
    parser.add_argument("--dashboard-port", type=int, default=8000)
    parser.add_argument("--operator-port", type=int, default=4001)
    parser.add_argument("--job-name", default=DEFAULT_JOB_NAME)
    parser.add_argument("--boot-timeout-s", type=float, default=180.0)
    parser.add_argument("--request-timeout-s", type=float, default=10.0)
    parser.add_argument("--shutdown-timeout-s", type=float, default=30.0)
    parser.add_argument("--runtime-shutdown-deadline-s", type=float, default=20.0)
    parser.add_argument("--compact", action="store_true", help="Emit single-line JSON.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    http_client = JsonHttpClient()
    process = StartAllSafeBoot(args, http_client)
    gate = ProductionReadinessGate(
        dashboard_base=f"http://127.0.0.1:{int(args.dashboard_port)}",
        operator_base=f"http://127.0.0.1:{int(args.operator_port)}",
        dashboard_token="",
        operator_token="",
        job_name=str(args.job_name),
        http_client=http_client,
        process=process,
        request_timeout_s=float(args.request_timeout_s),
        boot_timeout_s=float(args.boot_timeout_s),
        shutdown_timeout_s=float(args.shutdown_timeout_s),
    )
    report = gate.run()
    print(json.dumps(report, indent=None if args.compact else 2, sort_keys=True, default=str))
    return exit_code(report)


if __name__ == "__main__":
    raise SystemExit(main())
