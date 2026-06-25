from __future__ import annotations

import base64
import importlib
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

pytestmark = [pytest.mark.integration, pytest.mark.safety_critical]

_DASHBOARD_TOKEN = "paper-boot-token-1234567890"
_LIVE_PROFILE_ACK = "I_UNDERSTAND_OFFLINE_TRAINING_IN_LIVE_PROFILE"
_VALID_DATA_SOURCE_MASTER_KEY = base64.b64encode(bytes(range(32))).decode("ascii")

_CLEAR_ENV_KEYS = (
    "ALPACA_BASE_URL",
    "ALPACA_KEY_ID",
    "ALPACA_SECRET_KEY",
    "BROKER_BASE_URL",
    "ENGINE_RUNTIME_MODE",
    "EXTERNAL_BROKER",
    "IBKR_CLIENT_ID",
    "IBKR_HOST",
    "IBKR_PORT",
    "INTENDED_LIVE_BROKER",
    "LIVE_EXECUTION_BROKER",
    "LIVE_EXECUTION_ENABLED",
    "MODE",
    "PG_DSN",
    "POLYGON_API_KEY",
    "REDIS_CACHE_URL",
    "REDIS_URL",
    "TIMESCALE_DATABASE_URL",
    "TIMESCALE_DSN",
    "TIMESCALE_URL",
    "LIVE_CACHE_BACKEND",
    "LIVE_CACHE_REDIS_URL",
    "TS_PG_DSN",
    "TS_REDIS_URL",
)


def _reserve_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _write_secret(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(value), encoding="utf-8")
    path.chmod(0o600)


def _paper_boot_env(tmp_path: Path, *, port: int) -> dict[str, str]:
    runtime = tmp_path / "paper_boot_runtime"
    data = runtime / "data"
    logs = runtime / "logs"
    home = runtime / "home"
    for path in (data, logs, home):
        path.mkdir(parents=True, exist_ok=True)

    token_file = runtime / "secrets" / "dashboard_api_token"
    master_key_file = runtime / "secrets" / "data_source_master_key"
    env_file = runtime / ".env.paper-boot"
    _write_secret(token_file, _DASHBOARD_TOKEN)
    _write_secret(master_key_file, _VALID_DATA_SOURCE_MASTER_KEY)
    env_file.write_text("# hermetic paper boot test env\n", encoding="utf-8")

    return {
        "ALLOW_TRAINING": "0",
        "ALPACA_BASE_URL": "",
        "ALPACA_KEY_ID": "",
        "ALPACA_SECRET_KEY": "",
        "ALPACA_TRADE_UPDATES_WS_ENABLED": "0",
        "APP_ENV": "test",
        "AUTO_BOOT_DAEMONS": "0",
        "AUTO_BOOT_TARGETS": "",
        "BROKER": "sim",
        "BROKER_CHUNK_PCT": "1.0",
        "BROKER_FAILOVER": "sim",
        "BROKER_FEE_BPS": "0",
        "BROKER_LATENCY_MS": "0",
        "BROKER_LATENCY_SLEEP": "0",
        "BROKER_NAME": "sim",
        "BROKER_ROUTER_RETRY_ATTEMPTS": "1",
        "BROKER_ROUTER_RETRY_BASE_S": "0",
        "BROKER_ROUTER_RETRY_MAX_S": "0",
        "BROKER_SLIPPAGE_BPS": "0",
        "BROKER_SPREAD_BPS": "0",
        "BROKER_START_CASH": "100000",
        "BROKER_START_EQUITY": "100000",
        "CAPITAL_AWARE_KILL_SWITCH": "0",
        "DATA_DIR": str(data),
        "DATA_SOURCE_MASTER_KEY_FILE": str(master_key_file),
        "DASHBOARD_API_TOKEN_FILE": str(token_file),
        "DASHBOARD_HOST": "127.0.0.1",
        "DASHBOARD_PORT": str(int(port)),
        "API_HEALTH_CACHE_TTL_S": "0",
        "API_SYSTEM_SNAPSHOT_CACHE_TTL_S": "0",
        "DASHBOARD_STORAGE_REQUEST_TIMEOUT_S": "0.5",
        "DB_PATH": str(runtime / "paper_boot.sqlite"),
        "DISABLE_LIVE_EXECUTION": "1",
        "ENGINE_MODE": "paper",
        "ENGINE_SUPERVISED": "0",
        "ENV": "test",
        "EPE_EQUITY_SESSION_ENFORCE": "0",
        "EPE_STRICT_SIGNAL_TS": "0",
        "EXECUTION_MAX_SIGNAL_AGE_S": "3600",
        "EXECUTION_MODE": "paper",
        "EXECUTION_PRELIVE_RECONCILE": "1",
        "FEATURE_STORE_ENABLED": "0",
        "FEATURE_STORE_INIT_ON_STARTUP": "0",
        "HEALTH_PRICES_MAX_AGE_S": "300",
        "HOME": str(home),
        "IBKR_CLIENT_ID": "",
        "IBKR_HOST": "",
        "IBKR_PORT": "",
        "INTENDED_LIVE_BROKER": "sim",
        "KILL_SWITCH": "0",
        "KILL_SWITCH_GLOBAL": "0",
        "KILL_SWITCH_MODEL_MAX_CONSECUTIVE_LOSSES": "0",
        "KILL_SWITCH_MODEL_MAX_DRAWDOWN": "0",
        "KILL_SWITCH_REQUIRE_FRESH_DATA": "0",
        "KILL_SWITCH_REQUIRE_FRESH_JOBS": "0",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "LIVE_CACHE_BACKEND": "memory",
        "LIVE_CACHE_REDIS_URL": "",
        "LIVE_BROKER": "sim",
        "LIVE_TRADING_CONFIRM": "",
        "LIVE_TRADING_REQUIRE_CONFIRMATION": "1",
        "LOG_DIR": str(logs),
        "MODEL_AWARE_KILL_SWITCH": "0",
        "MODE": "paper",
        "NO_PROXY": "127.0.0.1,localhost",
        "NODE_ENV": "test",
        "OFFLINE_TRAINING_LIVE_PROFILE_ACK": _LIVE_PROFILE_ACK,
        "OFFLINE_TRAINING_LIVE_PROFILE_OWNER": "paper-boot-test",
        "OFFLINE_TRAINING_LIVE_PROFILE_REASON": "boot-level paper sim fill regression test",
        "OPEN_DASHBOARD_BROWSER_ON_START": "0",
        "OPERATOR_MODE": "paper",
        "PATH": os.environ.get("PATH", ""),
        "PREFLIGHT_ENABLE": "0",
        "PRICE_READ_BACKEND": "sqlite",
        "PROD_LOCK": "0",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONPATH": str(REPO_ROOT),
        "REDIS_CACHE_URL": "",
        "REDIS_URL": "",
        "RUNTIME_SHUTDOWN_HARD_DEADLINE_S": "5",
        "RUNTIME_WORKLOAD_PROFILE": "live",
        "TERMINAL_DUPLICATE_WINDOW_MS": "0",
        "TERMINAL_MAX_NOTIONAL": "1000000",
        "TERMINAL_MAX_QTY": "10",
        "TERMINAL_PRICE_MAX_AGE_MS": "600000",
        "TELEMETRY_READ_BACKEND": "sqlite",
        "TIMESCALE_ENABLED": "0",
        "TRADING_DATA": str(data),
        "TRADING_ENV_FILE": str(env_file),
        "TRADING_IMPORT_SMOKE_IMPORT_JOBS": "0",
        "TRADING_LOGS": str(logs),
        "TRADING_SECRET_POLICY_REPO_ROOT": str(runtime),
        "TRADING_SKIP_RUNTIME_GRAPH_CHECK": "1",
        "TRADING_SKIP_STALE_INGESTION_CLEANUP": "1",
        "TRADING_STARTUP_HEALTH_ASYNC_BIND": "1",
        "TRADING_STARTUP_HEALTH_POLL_S": "0.5",
        "TRADING_STARTUP_HEALTH_TIMEOUT_S": "60",
        "TRADING_VALIDATION_TIMEOUT_S": "30",
        "TRADING_KILL_SWITCH": "0",
        "TRADING_UNIT_TEST_SCHEMA_FAST": "1",
        "TS_API_ALLOW_LOCALHOST_MUTATIONS_WITHOUT_TOKEN": "0",
        "TS_ENV": "test",
        "TS_PG_SCHEMA_PER_DB_PATH": "1",
        "TS_REDIS_CIRCUIT_COOLDOWN_S": "300",
        "TS_REDIS_CIRCUIT_FAILURES": "1",
        "TS_REDIS_CONNECT_TIMEOUT_S": "0.05",
        "TS_REDIS_KEY_PREFIX": f"paper_boot_{tmp_path.name}",
        "TS_REDIS_SOCKET_TIMEOUT_S": "0.05",
        "TS_REDIS_URL": "redis://127.0.0.1:1/15",
        "TS_STORAGE_BACKEND": "sqlite",
    }


def _http_json(
    base_url: str,
    path: str,
    *,
    token: str = "",
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    timeout_s: float = 5.0,
) -> tuple[int, dict[str, Any]]:
    data = None
    headers = {"Accept": "application/json"}
    if token:
        headers["X-API-Token"] = token
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = Request(f"{base_url}{path}", data=data, headers=headers, method=str(method).upper())
    try:
        with urlopen(req, timeout=float(timeout_s)) as resp:
            raw = resp.read().decode("utf-8")
            status = int(resp.status)
    except HTTPError as exc:
        raw = exc.read().decode("utf-8")
        status = int(exc.code)
    body = json.loads(raw or "{}")
    return status, body


def _tail(path: Path, *, max_chars: int = 4000) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")[-max_chars:]
    except Exception as exc:
        return f"<unreadable {path}: {type(exc).__name__}: {exc}>"


def _process_failure(proc: subprocess.Popen[str], stdout_path: Path, stderr_path: Path) -> str:
    return (
        f"returncode={proc.poll()}\n"
        f"stdout_tail:\n{_tail(stdout_path)}\n"
        f"stderr_tail:\n{_tail(stderr_path)}"
    )


def _start_paper_boot(env: dict[str, str], tmp_path: Path) -> tuple[subprocess.Popen[str], Path, Path]:
    stdout_path = tmp_path / "start_system.stdout.log"
    stderr_path = tmp_path / "start_system.stderr.log"
    stdout = stdout_path.open("w", encoding="utf-8")
    stderr = stderr_path.open("w", encoding="utf-8")
    try:
        proc = subprocess.Popen(
            [sys.executable, "-u", "start_system.py", "paper"],
            cwd=str(REPO_ROOT),
            env=dict(env),
            stdout=stdout,
            stderr=stderr,
            text=True,
        )
    finally:
        stdout.close()
        stderr.close()
    return proc, stdout_path, stderr_path


def _wait_for_dashboard(
    proc: subprocess.Popen[str],
    base_url: str,
    stdout_path: Path,
    stderr_path: Path,
    *,
    timeout_s: float = 35.0,
) -> None:
    deadline = time.monotonic() + float(timeout_s)
    last_error = ""
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise AssertionError("paper boot exited before dashboard bind\n" + _process_failure(proc, stdout_path, stderr_path))
        try:
            status, body = _http_json(base_url, "/api/health", timeout_s=1.0)
            if status == 200 and isinstance(body, dict):
                return
            last_error = f"status={status} body={body}"
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        time.sleep(0.25)
    raise AssertionError(
        f"dashboard did not bind within {timeout_s:.1f}s; last_error={last_error}\n"
        + _process_failure(proc, stdout_path, stderr_path)
    )


def _stop_paper_boot(proc: subprocess.Popen[str], stdout_path: Path, stderr_path: Path) -> dict[str, Any]:
    if proc.poll() is not None:
        return {"ok": False, "returncode": proc.returncode, "reason": "process_exited_before_shutdown"}
    proc.terminate()
    try:
        proc.wait(timeout=12)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)
        return {
            "ok": False,
            "returncode": proc.returncode,
            "reason": "shutdown_timeout",
            "stdout_tail": _tail(stdout_path),
            "stderr_tail": _tail(stderr_path),
        }
    return {
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "reason": "terminated",
        "stdout_tail": _tail(stdout_path, max_chars=1200),
        "stderr_tail": _tail(stderr_path, max_chars=1200),
    }


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def _seed_price_and_live_state(env: dict[str, str]) -> None:
    (
        storage,
        broker_sim,
        execution_ledger,
        trade_attribution_ledger,
        lifecycle_state,
        execution_mode,
        runtime_meta,
    ) = _reload_modules(
        "engine.runtime.storage",
        "engine.execution.broker_sim",
        "engine.execution.execution_ledger",
        "engine.execution.trade_attribution_ledger",
        "engine.runtime.lifecycle_state",
        "engine.cache.wrappers.execution_mode",
        "engine.runtime.runtime_meta",
    )
    storage.init_db()
    broker_sim.init_broker_db()
    execution_ledger.init_execution_ledger()
    trade_attribution_ledger.ensure_trade_attribution_ready()
    execution_mode.set_execution_mode("paper", actor="paper_boot_test", reason="boot_paper_sim_fill", armed=0)

    now_ms = int(time.time() * 1000)
    runtime_meta.meta_set(
        "ingestion_state",
        json.dumps(
            {
                "running": True,
                "pid": os.getpid(),
                "provider_status": "running",
                "last_event_ts_ms": now_ms,
                "lag_ms": 0,
                "market_state": {
                    "status": "running",
                    "last_price_ts_ms": now_ms,
                    "last_ts_ms": now_ms,
                    "updated_ts_ms": now_ms,
                    "price_age_ms": 0,
                    "healthy_providers": 1,
                    "providers": {"paper_boot_seed": {"ok": True, "last_ts_ms": now_ms, "age_ms": 0}},
                },
                "source_health": {
                    "ok": True,
                    "critical_ok": True,
                    "degraded": False,
                    "runtime_reason_codes": [],
                    "advisory_reason_codes": [],
                    "stale_critical_sources": [],
                    "stale_sources": [],
                    "failed_sources": [],
                },
                "children": {},
                "last_error": "",
                "ts_ms": now_ms,
            },
            separators=(",", ":"),
            sort_keys=True,
        ),
    )
    runtime_meta.meta_set("first_price_ts_ms", str(now_ms))
    runtime_meta.meta_set("lifecycle_prev_state", str(lifecycle_state.WARMING_UP))
    runtime_meta.meta_set("lifecycle_state", str(lifecycle_state.LIVE))
    runtime_meta.meta_set("lifecycle_detail", "paper_boot_test_price_seeded")
    runtime_meta.meta_set("lifecycle_updated_ts_ms", str(now_ms))

    con = storage.connect()
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS prices (
              ts_ms INTEGER NOT NULL,
              symbol TEXT NOT NULL,
              price REAL,
              px REAL,
              source TEXT,
              PRIMARY KEY(symbol, ts_ms)
            )
            """
        )
        con.execute(
            "INSERT OR REPLACE INTO prices(ts_ms, symbol, price, px, source) VALUES (?,?,?,?,?)",
            (now_ms, "AAPL", 100.0, 100.0, "paper_boot_test"),
        )
        con.commit()
    finally:
        con.close()

    lifecycle_state.set_state(lifecycle_state.LIVE, "paper_boot_test_price_seeded")
    os.environ["DB_PATH"] = str(env["DB_PATH"])


def _wait_for_paper_barrier(
    proc: subprocess.Popen[str],
    base_url: str,
    stdout_path: Path,
    stderr_path: Path,
    *,
    timeout_s: float = 12.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + float(timeout_s)
    last_body: dict[str, Any] = {}
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise AssertionError("paper boot exited while waiting for barrier\n" + _process_failure(proc, stdout_path, stderr_path))
        status, body = _http_json(base_url, "/api/execution/barrier", token=_DASHBOARD_TOKEN, timeout_s=2.0)
        last_body = body
        barrier = dict(body.get("execution_barrier") or {})
        if (
            status == 200
            and str(barrier.get("mode") or body.get("mode") or "") == "paper"
            and bool(barrier.get("allow_simulation")) is True
            and bool(barrier.get("real_trading_allowed")) is False
        ):
            return body
        time.sleep(0.25)
    raise AssertionError(f"paper barrier did not become simulation-ready: {last_body}")


def _install_live_import_canary(tmp_path: Path, env: dict[str, str]) -> tuple[dict[str, str], Path]:
    canary_dir = tmp_path / "live_import_canary"
    canary_dir.mkdir(parents=True, exist_ok=True)
    canary_path = canary_dir / "live_adapter_imports.txt"
    (canary_dir / "sitecustomize.py").write_text(
        """
from __future__ import annotations

import importlib.abc
import os
from pathlib import Path

_TARGETS = {"engine.execution.broker_alpaca_rest", "engine.execution.broker_ibkr_gateway"}
_CANARY_PATH = Path(os.environ.get("PAPER_BOOT_LIVE_IMPORT_CANARY_PATH", ""))


class _LiveAdapterImportBlocker(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in _TARGETS:
            if str(_CANARY_PATH):
                existing = _CANARY_PATH.read_text(encoding="utf-8") if _CANARY_PATH.exists() else ""
                _CANARY_PATH.write_text(existing + fullname + "\\n", encoding="utf-8")
            raise ImportError(f"live adapter import blocked during paper boot test: {fullname}")
        return None


import sys

sys.meta_path.insert(0, _LiveAdapterImportBlocker())
""".lstrip(),
        encoding="utf-8",
    )
    job_env = dict(env)
    job_env["PAPER_BOOT_LIVE_IMPORT_CANARY_PATH"] = str(canary_path)
    job_env["PYTHONPATH"] = f"{canary_dir}{os.pathsep}{REPO_ROOT}"
    return job_env, canary_path


def _run_job_module(
    module_name: str,
    env: dict[str, str],
    tmp_path: Path,
    *,
    timeout_s: float = 60.0,
) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(
        [sys.executable, "-m", module_name],
        cwd=str(REPO_ROOT),
        env=dict(env),
        text=True,
        capture_output=True,
        timeout=float(timeout_s),
    )
    safe_name = module_name.rsplit(".", 1)[-1]
    (tmp_path / f"{safe_name}.stdout.log").write_text(proc.stdout, encoding="utf-8")
    (tmp_path / f"{safe_name}.stderr.log").write_text(proc.stderr, encoding="utf-8")
    return proc


def _json_lines(raw: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in str(raw or "").splitlines():
        text = line.strip()
        if not text.startswith("{"):
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _assert_redis_isolation(env: dict[str, str]) -> None:
    parsed = urlparse(str(env.get("TS_REDIS_URL") or ""))
    assert parsed.scheme == "redis"
    assert parsed.hostname == "127.0.0.1"
    assert parsed.port == 1
    assert str(env.get("LIVE_CACHE_BACKEND") or "") == "memory"
    assert not str(env.get("LIVE_CACHE_REDIS_URL") or "").strip()


def _run_execution_jobs(env: dict[str, str], tmp_path: Path) -> None:
    job_env, canary_path = _install_live_import_canary(tmp_path, env)

    broker_proc = _run_job_module("engine.execution.jobs.broker_apply_orders", job_env, tmp_path)
    assert broker_proc.returncode == 0, (
        f"broker_apply_orders rc={broker_proc.returncode}\n"
        f"stdout:\n{broker_proc.stdout}\n"
        f"stderr:\n{broker_proc.stderr}"
    )
    assert '"mode":"paper"' in broker_proc.stdout
    assert '"broker":"sim"' in broker_proc.stdout
    broker_payloads = _json_lines(broker_proc.stdout)
    applied = False
    for payload in broker_payloads:
        result = dict(payload.get("result") or {})
        fills_written = int(
            payload.get("fills_written")
            or payload.get("filled")
            or payload.get("filled_count")
            or result.get("fills_written")
            or 0
        )
        if (
            str(payload.get("status") or "") in {"ok", "executed"}
            and str(payload.get("broker") or "") == "sim"
            and str(result.get("status") or "") in {"applied", "executed"}
            and fills_written >= 1
        ):
            applied = True
            break
    assert applied, broker_proc.stdout

    attrib_proc = _run_job_module("engine.execution.jobs.execution_poll_and_attrib", job_env, tmp_path)
    assert attrib_proc.returncode == 0, (
        f"execution_poll_and_attrib rc={attrib_proc.returncode}\n"
        f"stdout:\n{attrib_proc.stdout}\n"
        f"stderr:\n{attrib_proc.stderr}"
    )
    assert not canary_path.exists(), canary_path.read_text(encoding="utf-8") if canary_path.exists() else ""


def _assert_persisted_fill_and_attribution() -> None:
    (storage,) = _reload_modules("engine.runtime.storage")
    con = storage.connect(readonly=True)
    try:
        broker_fills = int(con.execute("SELECT COUNT(*) FROM broker_fills WHERE symbol='AAPL'").fetchone()[0] or 0)
        execution_fills = int(con.execute("SELECT COUNT(*) FROM execution_fills WHERE symbol='AAPL'").fetchone()[0] or 0)
        pnl_attribution = int(con.execute("SELECT COUNT(*) FROM pnl_attribution WHERE symbol='AAPL'").fetchone()[0] or 0)
        trade_attribution = int(
            con.execute("SELECT COUNT(*) FROM trade_attribution_ledger WHERE symbol='AAPL'").fetchone()[0] or 0
        )
    finally:
        con.close()
        storage.close_pooled_connections()

    assert broker_fills >= 1
    assert execution_fills >= 1
    assert pnl_attribution >= 1
    assert trade_attribution >= 1


def test_paper_mode_boot_terminal_order_sim_fill_and_attribution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    port = _reserve_free_port()
    env = _paper_boot_env(tmp_path, port=port)
    _assert_redis_isolation(env)
    for key in _CLEAR_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    proc, stdout_path, stderr_path = _start_paper_boot(env, tmp_path)
    shutdown: dict[str, Any] | None = None
    try:
        base_url = f"http://127.0.0.1:{port}"
        _wait_for_dashboard(proc, base_url, stdout_path, stderr_path)
        _seed_price_and_live_state(env)

        broker_router = importlib.reload(importlib.import_module("engine.execution.broker_router"))
        assert broker_router.effective_broker_chain() == ["sim"]
        paper_contract = broker_router._paper_sim_broker_contract(["sim"])
        assert paper_contract["ok"] is True
        assert paper_contract["live_adapter_import_reachable"] is False

        barrier_body = _wait_for_paper_barrier(proc, base_url, stdout_path, stderr_path)
        barrier = dict(barrier_body.get("execution_barrier") or {})
        assert barrier["mode"] == "paper"
        assert barrier["allow_simulation"] is True
        assert barrier["real_trading_allowed"] is False

        status, body = _http_json(
            base_url,
            "/api/terminal/order",
            token=_DASHBOARD_TOKEN,
            method="POST",
            payload={
                "symbol": "AAPL",
                "side": "BUY",
                "qty": 1,
                "confirmation": "TRADE",
                "consequence_ack": True,
                "actor": "paper_boot_test",
                "source": "pytest",
            },
            timeout_s=5,
        )
        assert status == 200, body
        assert body["ok"] is True
        assert body["symbol"] == "AAPL"

        _seed_price_and_live_state(env)
        _run_execution_jobs(env, tmp_path)
        _assert_persisted_fill_and_attribution()
    finally:
        shutdown = _stop_paper_boot(proc, stdout_path, stderr_path)

    assert shutdown["ok"] is True, shutdown
