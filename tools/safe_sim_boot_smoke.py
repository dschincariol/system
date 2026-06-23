from __future__ import annotations

"""Prepare and smoke-test a local safe/sim runtime boot.

The script is intentionally conservative: it derives a runtime env from
``.env.codex-sim-paper.bak``, moves inline secret-shaped values into local
``*_FILE`` references, forces safe execution guards, starts the dashboard and
operator sidecar, checks the safety-gate endpoints, and shuts everything down.
It never prints secret values.
"""

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SAFETY_ENDPOINTS = (
    "/api/system/kill_switches",
    "/api/broker/config",
    "/api/execution/barrier",
    "/api/operator/readiness_evidence",
)

INLINE_SECRET_KEYS = {
    "DASHBOARD_API_TOKEN",
    "OPERATOR_API_TOKEN",
    "DATA_SOURCE_MASTER_KEY",
    "TRADING_MASTER_KEY",
    "APP_MASTER_KEY",
    "OBJECT_STORE_ACCESS_KEY",
    "OBJECT_STORE_SECRET_KEY",
}

PASSWORD_FILE_KEYS = {
    "TS_PG_DSN": "TS_PG_PASSWORD_FILE",
    "TIMESCALE_DSN": "TIMESCALE_PASSWORD_FILE",
    "TIMESCALE_PRICES_DSN": "TIMESCALE_PASSWORD_FILE",
    "REDIS_URL": "REDIS_PASSWORD_FILE",
}

SAFETY_OVERRIDES = {
    "ENV": "dev",
    "PROD_LOCK": "0",
    "ENGINE_MODE": "safe",
    "EXECUTION_MODE": "safe",
    "OPERATOR_MODE": "safe",
    "DISABLE_LIVE_EXECUTION": "1",
    "KILL_SWITCH_GLOBAL": "1",
    "LIVE_TRADING_CONFIRM": "",
    "LIVE_TRADING_REQUIRE_CONFIRMATION": "1",
    "LIVE_TRADING_REQUIRE_DASHBOARD_API_TOKEN": "1",
    "BROKER": "sim",
    "BROKER_NAME": "sim",
    "START_INGESTION_WITH_SERVER": "0",
    "AUTO_PIPELINE": "0",
    "AUTO_PIPELINE_INCLUDE_EXECUTION": "0",
    "AUTO_BOOT_DAEMONS": "0",
    "OPEN_DASHBOARD_BROWSER_ON_START": "0",
    "TRADING_STARTUP_HEALTH_ASYNC_BIND": "1",
    "TS_STORAGE_BACKEND": "sqlite",
    "TIMESCALE_ENABLED": "0",
    "TIMESCALE_PRICES_ENABLED": "0",
    "TIMESCALE_TELEMETRY_MIRROR_ENABLED": "0",
    "TIMESCALE_TELEMETRY_REQUIRE_HEALTHY_MIRROR": "0",
    "TIMESCALE_TELEMETRY_REQUIRE_HEALTHY_TIMESCALE": "0",
    "TIMESCALE_TELEMETRY_VALIDATION_ENABLED": "0",
    "PRICE_MIGRATION_VALIDATION_ENABLED": "0",
    "PRICE_READ_BACKEND": "sqlite",
    "PRICE_READ_REQUIRE_VALIDATION": "0",
    "TELEMETRY_READ_BACKEND": "sqlite",
    "TELEMETRY_READ_REQUIRE_VALIDATION": "0",
    "PREFLIGHT_REQUIRE_TIMESCALE": "0",
    "PREFLIGHT_REQUIRE_REDIS": "0",
    "PREFLIGHT_REQUIRE_OBJECT_STORAGE": "0",
    "POLYGON_REST_ENABLED": "0",
    "POLYGON_WS_ENABLED": "0",
    "TRADIER_ENABLED": "0",
    "IBKR_ENABLED": "0",
    "CCXT_ENABLED": "0",
    "YFINANCE_ENABLED": "0",
    "PYTHONUNBUFFERED": "1",
}


@dataclass(frozen=True)
class PreparedEnv:
    env: dict[str, str]
    env_file: Path
    secrets_dir: Path
    metadata: dict[str, Any]


def _parse_env_file(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    if not path.exists():
        raise FileNotFoundError(str(path))
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        key = key.strip()
        if key.startswith("export "):
            key = key[len("export ") :].strip()
        if not key:
            continue
        value = value.strip()
        if (
            (value.startswith('"') and value.endswith('"'))
            or (value.startswith("'") and value.endswith("'"))
        ):
            value = value[1:-1]
        env[key] = value
    return env


def _write_secret(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(str(value).strip() + "\n")
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def _env_quote(value: str) -> str:
    text = str(value)
    if (
        text == ""
        or any(ch.isspace() for ch in text)
        or any(ch in text for ch in ("'", '"', "#", "$", "`", "\\"))
    ):
        return json.dumps(text)
    return text


def _write_env_file(path: Path, env: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"{key}={_env_quote(value)}" for key, value in sorted(env.items())]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _file_env_for_key(key: str) -> str:
    from engine.runtime.secret_sources import SECRET_ENV_SPEC_BY_KEY

    spec = SECRET_ENV_SPEC_BY_KEY.get(str(key))
    if spec and spec.file_envs:
        return str(spec.file_envs[0])
    return f"{key}_FILE"


def _strip_url_password(value: str) -> tuple[str, str]:
    try:
        parsed = urlsplit(str(value or ""))
    except ValueError:
        return str(value or ""), ""
    if not parsed.scheme or parsed.password is None:
        return str(value or ""), ""
    host = parsed.hostname or ""
    if parsed.port:
        host = f"{host}:{parsed.port}"
    userinfo = parsed.username or ""
    netloc = f"{userinfo}@{host}" if userinfo else host
    sanitized = urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))
    return sanitized, str(parsed.password)


def _move_inline_secrets_to_files(env: dict[str, str], secrets_dir: Path) -> dict[str, Any]:
    moved: list[dict[str, str]] = []
    for key in sorted(INLINE_SECRET_KEYS):
        value = str(env.get(key) or "").strip()
        if not value:
            continue
        file_env = _file_env_for_key(key)
        target = secrets_dir / key.lower()
        _write_secret(target, value)
        env.pop(key, None)
        env[file_env] = str(target)
        moved.append({"key": key, "file_env": file_env})

    password_files: dict[str, Path] = {}
    for key, file_env in PASSWORD_FILE_KEYS.items():
        value = str(env.get(key) or "").strip()
        if not value:
            continue
        sanitized, password = _strip_url_password(value)
        if not password:
            continue
        target = password_files.setdefault(file_env, secrets_dir / file_env.lower())
        _write_secret(target, password)
        env[key] = sanitized
        env[file_env] = str(target)
        moved.append({"key": key, "file_env": file_env})
    return {"moved_inline_secret_keys": moved}


def prepare_safe_sim_env(
    *,
    base_env_path: Path,
    runtime_dir: Path,
    dashboard_port: int,
    operator_port: int,
) -> PreparedEnv:
    env = _parse_env_file(base_env_path)
    runtime_dir = runtime_dir.resolve()
    secrets_dir = runtime_dir / "secrets"
    log_dir = runtime_dir / "log"
    data_dir = runtime_dir / "db"
    for path in (runtime_dir, secrets_dir, log_dir, data_dir):
        path.mkdir(parents=True, exist_ok=True)

    metadata = _move_inline_secrets_to_files(env, secrets_dir)
    if not str(env.get("DASHBOARD_API_TOKEN_FILE") or "").strip():
        token_path = secrets_dir / "dashboard_api_token"
        _write_secret(token_path, "safe-sim-dashboard-token-0000000000000000")
        env["DASHBOARD_API_TOKEN_FILE"] = str(token_path)
    env.pop("DASHBOARD_API_TOKEN", None)

    if not str(env.get("OPERATOR_API_TOKEN_FILE") or "").strip():
        token_path = secrets_dir / "operator_api_token"
        _write_secret(token_path, "safe-sim-operator-token-0000000000000000")
        env["OPERATOR_API_TOKEN_FILE"] = str(token_path)
    env.pop("OPERATOR_API_TOKEN", None)

    env.update(SAFETY_OVERRIDES)
    env.update(
        {
            "DASHBOARD_HOST": "127.0.0.1",
            "DASHBOARD_PORT": str(int(dashboard_port)),
            "OPERATOR_BIND_HOST": "127.0.0.1",
            "OPERATOR_PORT": str(int(operator_port)),
            "DASHBOARD_BASE": f"http://127.0.0.1:{int(dashboard_port)}",
            "TRADING_LOGS": str(log_dir),
            "TRADING_DATA": str(data_dir),
            "DB_PATH": str(data_dir / "trading.db"),
            "TRADING_SECRET_POLICY_REPO_ROOT": str(runtime_dir),
            "OPERATOR_DATA_DIR": str(runtime_dir / "operator"),
            "OPERATOR_ENV_PATH": str(runtime_dir / ".env.safe-sim"),
            "OPERATOR_AUTO_START": "0",
            "OPERATOR_DISABLE_INTERNAL_ENGINE_START": "1",
        }
    )
    env_file = runtime_dir / ".env.safe-sim"
    _write_env_file(env_file, env)
    metadata.update(
        {
            "env_file": str(env_file),
            "secrets_dir": str(secrets_dir),
            "dashboard_token_source": "DASHBOARD_API_TOKEN_FILE",
            "operator_token_source": "OPERATOR_API_TOKEN_FILE",
        }
    )
    return PreparedEnv(env=env, env_file=env_file, secrets_dir=secrets_dir, metadata=metadata)


def _read_token_file(path: str) -> str:
    if not str(path or "").strip():
        return ""
    return Path(path).read_text(encoding="utf-8").strip()


def _http_request_json(
    url: str,
    *,
    method: str = "GET",
    token: str = "",
    operator_token: str = "",
    timeout_s: float = 5.0,
    body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    headers = {"Accept": "application/json"}
    payload = None
    if body is not None:
        payload = json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if token:
        headers["X-API-Token"] = token
    if operator_token:
        headers["X-Operator-Token"] = operator_token
    req = urllib.request.Request(url, data=payload, headers=headers, method=str(method or "GET").upper())
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as response:
            raw = response.read().decode("utf-8", errors="replace")
            body = json.loads(raw) if raw else {}
            return {"ok": True, "status": int(response.status), "body": body}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            body = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            body = {"raw": raw[:200]}
        return {"ok": False, "status": int(exc.code), "body": body}
    except Exception as exc:
        return {"ok": False, "status": 0, "error": f"{type(exc).__name__}:{exc}", "body": {}}


def _http_json(url: str, *, token: str = "", operator_token: str = "", timeout_s: float = 5.0) -> dict[str, Any]:
    return _http_request_json(
        url,
        method="GET",
        token=token,
        operator_token=operator_token,
        timeout_s=timeout_s,
    )


def _http_post_json(url: str, *, token: str = "", timeout_s: float = 5.0, body: dict[str, Any] | None = None) -> dict[str, Any]:
    return _http_request_json(url, method="POST", token=token, timeout_s=timeout_s, body=body or {})


def _port_open(host: str, port: int, timeout_s: float = 0.25) -> bool:
    try:
        with socket.create_connection((host, int(port)), timeout=timeout_s):
            return True
    except OSError:
        return False


def _wait_for_endpoint(url: str, *, token: str, timeout_s: float) -> dict[str, Any]:
    deadline = time.monotonic() + max(1.0, float(timeout_s))
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        last = _http_json(url, token=token, timeout_s=3.0)
        if int(last.get("status") or 0) in {200, 401, 403}:
            return last
        time.sleep(0.5)
    return last or {"ok": False, "status": 0, "error": "timeout", "body": {}}


def _terminate_process(proc: subprocess.Popen | None, *, name: str, timeout_s: float = 10.0) -> dict[str, Any]:
    if proc is None:
        return {"name": name, "started": False}
    if proc.poll() is not None:
        return {"name": name, "started": True, "returncode": proc.returncode}
    try:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=timeout_s)
        return {"name": name, "started": True, "returncode": proc.returncode, "terminated": True}
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)
        return {"name": name, "started": True, "returncode": proc.returncode, "killed": True}


def _request_dashboard_shutdown(dashboard_base: str, *, token: str, timeout_s: float = 10.0) -> dict[str, Any]:
    return _http_post_json(
        f"{dashboard_base}/api/server/shutdown",
        token=token,
        timeout_s=timeout_s,
        body={"reason": "safe_sim_boot_smoke"},
    )


def _spawn(cmd: list[str], *, env: dict[str, str], cwd: Path, stdout_path: Path, stderr_path: Path) -> subprocess.Popen:
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stdout_fh = stdout_path.open("ab")
    stderr_fh = stderr_path.open("ab")
    try:
        return subprocess.Popen(cmd, cwd=str(cwd), env=env, stdout=stdout_fh, stderr=stderr_fh)
    finally:
        stdout_fh.close()
        stderr_fh.close()


def _observe_serve_forever(stdout_path: Path) -> bool:
    try:
        text = stdout_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False
    return "[dashboard_server] serve_forever_enter" in text


def _unlink_if_exists(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return
    except OSError:
        return


def _gate_status(endpoint: str, body: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(body, dict):
        return {"populated": False, "safe": False, "error": "non_object_payload"}
    if endpoint == "/api/system/kill_switches":
        snapshot = body.get("kill_switches") if isinstance(body.get("kill_switches"), dict) else body.get("data")
        return {
            "populated": isinstance(snapshot, dict),
            "safe": isinstance(snapshot, dict),
            "active_rows": len(list((snapshot or {}).get("state") or [])) if isinstance(snapshot, dict) else 0,
        }
    if endpoint == "/api/broker/config":
        config = body.get("config") if isinstance(body.get("config"), dict) else {}
        active_broker = str((config or {}).get("active_broker") or "").strip().lower()
        paper_live_mode = str((config or {}).get("paper_live_mode") or "").strip().lower()
        return {
            "populated": bool(config),
            "safe": active_broker == "sim" and paper_live_mode != "live",
            "active_broker": active_broker,
            "paper_live_mode": paper_live_mode,
            "secrets_masked": bool((config or {}).get("secrets_masked")),
        }
    if endpoint == "/api/execution/barrier":
        barrier = body.get("execution_barrier") if isinstance(body.get("execution_barrier"), dict) else {}
        allowed = bool(body.get("allowed") or (barrier or {}).get("allowed"))
        reasons = list(body.get("reasons") or [])
        reason = str(body.get("reason") or (barrier or {}).get("reason") or "")
        return {
            "populated": bool(barrier),
            "safe": not allowed,
            "allowed": allowed,
            "reason": reason,
            "reason_count": len(reasons),
        }
    if endpoint == "/api/operator/readiness_evidence":
        items = list(body.get("items") or [])
        summary = body.get("summary") if isinstance(body.get("summary"), dict) else {}
        return {
            "populated": bool(items) and bool(summary),
            "safe": bool(items) and bool(summary),
            "status": str(body.get("status") or ""),
            "item_count": len(items),
            "blocking": int((summary or {}).get("blocking") or 0),
            "target_broker": str(body.get("target_broker") or ""),
        }
    return {"populated": bool(body), "safe": bool(body)}


def _summarize_gate_response(endpoint: str, response: dict[str, Any]) -> dict[str, Any]:
    status = int(response.get("status") or 0)
    body = response.get("body") if isinstance(response.get("body"), dict) else {}
    gate = _gate_status(endpoint, body)
    return {
        "status": status,
        "http_ok": status == 200,
        "payload_ok": body.get("ok") if isinstance(body, dict) else None,
        "error": body.get("error") or response.get("error") if isinstance(body, dict) else response.get("error"),
        **gate,
    }


def run_smoke(args: argparse.Namespace) -> int:
    runtime_dir = Path(args.runtime_dir).resolve()
    prepared = prepare_safe_sim_env(
        base_env_path=Path(args.base_env).resolve(),
        runtime_dir=runtime_dir,
        dashboard_port=int(args.dashboard_port),
        operator_port=int(args.operator_port),
    )
    if args.prepare_only:
        print(json.dumps({"ok": True, "prepared": prepared.metadata}, sort_keys=True))
        return 0

    env = dict(os.environ)
    env.update(prepared.env)
    env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    dashboard_token = _read_token_file(prepared.env["DASHBOARD_API_TOKEN_FILE"])
    operator_token = _read_token_file(prepared.env["OPERATOR_API_TOKEN_FILE"])
    dashboard_base = f"http://127.0.0.1:{int(args.dashboard_port)}"
    operator_base = f"http://127.0.0.1:{int(args.operator_port)}"
    runtime_stdout = runtime_dir / "log" / "safe_sim_runtime.stdout.log"
    runtime_stderr = runtime_dir / "log" / "safe_sim_runtime.stderr.log"
    operator_stdout = runtime_dir / "log" / "safe_sim_operator.stdout.log"
    operator_stderr = runtime_dir / "log" / "safe_sim_operator.stderr.log"
    for log_path in (runtime_stdout, runtime_stderr, operator_stdout, operator_stderr):
        _unlink_if_exists(log_path)

    summary: dict[str, Any] = {
        "ok": False,
        "prepared": prepared.metadata,
        "safety_env": {key: prepared.env.get(key, "") for key in sorted(SAFETY_OVERRIDES)},
        "dashboard": {},
        "operator": {},
        "shutdown": {},
    }

    if _port_open("127.0.0.1", int(args.dashboard_port)) or _port_open("127.0.0.1", int(args.operator_port)):
        summary["error"] = "port_already_in_use"
        print(json.dumps(summary, sort_keys=True))
        return 2

    runtime_proc: subprocess.Popen | None = None
    operator_proc: subprocess.Popen | None = None
    exit_code = 1

    class _SmokeFailure(RuntimeError):
        def __init__(self, code: int, reason: str) -> None:
            super().__init__(reason)
            self.code = int(code)
            self.reason = str(reason)

    def _fail(code: int, reason: str) -> None:
        summary["error"] = str(reason)
        raise _SmokeFailure(code, reason)

    try:
        runtime_proc = _spawn(
            [sys.executable, "start_system.py", "safe"],
            env=env,
            cwd=ROOT,
            stdout_path=runtime_stdout,
            stderr_path=runtime_stderr,
        )
        health = _wait_for_endpoint(f"{dashboard_base}/api/health", token=dashboard_token, timeout_s=float(args.timeout_s))
        health_body = health.get("body") if isinstance(health.get("body"), dict) else {}
        summary["dashboard"]["health"] = {
            "status": health.get("status"),
            "ok": health_body.get("ok"),
            "state": str(health_body.get("state") or health_body.get("status") or ""),
        }
        if int(health.get("status") or 0) != 200:
            _fail(1, "dashboard_health_unreachable")

        dashboard_results: dict[str, Any] = {}
        for endpoint in SAFETY_ENDPOINTS:
            response = _http_json(f"{dashboard_base}{endpoint}", token=dashboard_token, timeout_s=float(args.request_timeout_s))
            dashboard_results[endpoint] = _summarize_gate_response(endpoint, response)
        summary["dashboard"]["safety_endpoints"] = dashboard_results
        if any(not bool(item.get("http_ok")) or not bool(item.get("populated")) or not bool(item.get("safe")) for item in dashboard_results.values()):
            _fail(1, "dashboard_safety_endpoint_failed")

        if not args.skip_operator:
            if not (ROOT / "node_modules" / "express" / "package.json").exists():
                _fail(1, "operator_node_modules_missing")
            operator_proc = _spawn(
                ["node", "boot/operator_server.js"],
                env=env,
                cwd=ROOT,
                stdout_path=operator_stdout,
                stderr_path=operator_stderr,
            )
            ping = _wait_for_endpoint(
                f"{operator_base}/api/operator/ping",
                token="",
                timeout_s=float(args.timeout_s),
            )
            summary["operator"]["ping"] = {"status": ping.get("status"), "ok": bool(ping.get("body", {}).get("ok"))}
            if int(ping.get("status") or 0) != 200:
                _fail(1, "operator_ping_unreachable")
            operator_results: dict[str, Any] = {}
            for endpoint in SAFETY_ENDPOINTS:
                response = _http_json(
                    f"{operator_base}{endpoint}",
                    operator_token=operator_token,
                    timeout_s=float(args.request_timeout_s),
                )
                operator_results[endpoint] = _summarize_gate_response(endpoint, response)
            summary["operator"]["safety_endpoints"] = operator_results
            if any(not bool(item.get("http_ok")) or not bool(item.get("populated")) or not bool(item.get("safe")) for item in operator_results.values()):
                _fail(1, "operator_safety_endpoint_failed")

        summary["dashboard"]["serve_forever_observed"] = _observe_serve_forever(runtime_stdout)
        if not bool(summary["dashboard"]["serve_forever_observed"]):
            _fail(1, "dashboard_serve_forever_not_observed")
        summary["ok"] = True
        exit_code = 0
    except _SmokeFailure as exc:
        exit_code = int(exc.code)
    finally:
        summary["shutdown"]["operator"] = _terminate_process(operator_proc, name="operator")
        if runtime_proc is not None and runtime_proc.poll() is None:
            summary["shutdown"]["dashboard_request"] = _request_dashboard_shutdown(
                dashboard_base,
                token=dashboard_token,
                timeout_s=float(args.shutdown_timeout_s),
            )
            try:
                runtime_proc.wait(timeout=float(args.shutdown_timeout_s))
                summary["shutdown"]["runtime"] = {
                    "name": "runtime",
                    "started": True,
                    "returncode": runtime_proc.returncode,
                    "dashboard_shutdown_requested": True,
                }
            except subprocess.TimeoutExpired:
                summary["shutdown"]["runtime"] = _terminate_process(
                    runtime_proc,
                    name="runtime",
                    timeout_s=float(args.shutdown_timeout_s),
                )
        else:
            summary["shutdown"]["runtime"] = _terminate_process(runtime_proc, name="runtime")
        time.sleep(0.5)
        summary["shutdown"]["dashboard_port_open"] = _port_open("127.0.0.1", int(args.dashboard_port))
        summary["shutdown"]["operator_port_open"] = _port_open("127.0.0.1", int(args.operator_port))
        runtime_shutdown = summary["shutdown"].get("runtime") if isinstance(summary["shutdown"].get("runtime"), dict) else {}
        if summary.get("ok") and bool((runtime_shutdown or {}).get("killed")):
            summary["ok"] = False
            summary["error"] = "runtime_required_sigkill"
            exit_code = 1
        if summary.get("ok") and (
            summary["shutdown"]["dashboard_port_open"] or summary["shutdown"]["operator_port_open"]
        ):
            summary["ok"] = False
            summary["error"] = "listener_left_open_after_shutdown"
            exit_code = 1
        print(json.dumps(summary, sort_keys=True))
    return int(exit_code)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-env", default=str(ROOT / ".env.codex-sim-paper.bak"))
    parser.add_argument("--runtime-dir", default=str(ROOT / "var" / "tmp" / "safe_sim_boot"))
    parser.add_argument("--dashboard-port", type=int, default=8000)
    parser.add_argument("--operator-port", type=int, default=4001)
    parser.add_argument("--timeout-s", type=float, default=120.0)
    parser.add_argument("--request-timeout-s", type=float, default=10.0)
    parser.add_argument("--shutdown-timeout-s", type=float, default=30.0)
    parser.add_argument("--skip-operator", action="store_true")
    parser.add_argument("--prepare-only", action="store_true")
    return parser


def main() -> int:
    return int(run_smoke(build_parser().parse_args()) or 0)


if __name__ == "__main__":
    raise SystemExit(main())
